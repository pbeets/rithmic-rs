use tracing::{debug, error, info};

use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    ConnectStrategy,
    api::receiver_api::RithmicResponse,
    config::{LoginConfig, RithmicConfig},
    error::RithmicError,
    plants::{
        await_all_responses, await_first_response,
        core::{PlantActor, PlantCore, SelectResult},
    },
    request_handler::PendingReplay,
    rti::{
        messages::RithmicMessage, request_login::SysInfraType, request_tick_bar_update,
        request_time_bar_replay::BarType, request_time_bar_update,
    },
    types::{TickBarReplayRequest, TimeBarReplayRequest, VolumeProfileMinuteBarsRequest},
};

/// Default subscription channel capacity.
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 10_000;

pub(crate) enum HistoryPlantCommand {
    Close,
    Abort,
    GetSystemInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Login {
        config: LoginConfig,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Replay {
        query: ReplayQuery,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ResumeBars {
        request_key: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeTimeBarUpdates {
        symbol: String,
        exchange: String,
        bar_type: request_time_bar_update::BarType,
        bar_type_period: i32,
        request: request_time_bar_update::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeTickBarUpdates {
        symbol: String,
        exchange: String,
        bar_type: request_tick_bar_update::BarType,
        bar_sub_type: request_tick_bar_update::BarSubType,
        bar_type_specifier: String,
        request: request_tick_bar_update::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
}

/// The request behind a [`HistoryPlantCommand::Replay`].
pub(crate) enum ReplayQuery {
    Time(TimeBarReplayRequest),
    Tick(TickBarReplayRequest),
    Volume(VolumeProfileMinuteBarsRequest),
}

/// Historical market data from Rithmic: past ticks and past bars.
///
/// Connect once, log in, then ask for whatever window of history you need. The
/// plant runs on its own background task; you talk to it through a
/// [`RithmicHistoryPlantHandle`], which is cheap to clone and safe to share
/// between tasks.
///
/// # Getting data out
///
/// Every loader returns a `Vec<RithmicResponse>`. Each entry wraps a
/// [`RithmicMessage`], so you match on it to get at the numbers:
///
/// ```no_run
/// # use rithmic_rs::{RithmicResponse, rti::messages::RithmicMessage};
/// # fn demo(ticks: Vec<RithmicResponse>) {
/// for response in &ticks {
///     if let RithmicMessage::ResponseTickBarReplay(tick) = &response.message {
///         println!("{:?} @ {:?}", tick.close_price, tick.data_bar_ssboe);
///     }
/// }
/// # }
/// ```
///
/// Three things to know about the shape of that `Vec`:
///
/// - **The last entry is an end marker, not data.** Rithmic closes every replay
///   with a response that carries no bar. Matching on the message type as above
///   skips it; counting `responses.len()` does not, so subtract one if you want
///   a record count. If the server ended the replay early, the call still
///   returns `Ok` and this entry has [`error`](RithmicResponse::error) set.
/// - **Times are Unix seconds as `i32`,** both going in and coming back. This
///   is Rithmic's own type and it overflows in 2038. Daily and weekly time bars
///   are the exception: they use `YYYYMMDD` dates; see
///   [`load_time_bars`](RithmicHistoryPlantHandle::load_time_bars).
/// - **Tick bars carry two timestamps.** `data_bar_ssboe` and `data_bar_usecs`
///   are two-element arrays holding the bar's open and close: index 0 is when
///   the bar started, index 1 is when it ended. For one-tick bars both describe
///   the same trade. See [`load_ticks`](RithmicHistoryPlantHandle::load_ticks)
///   for a quirk in the first record's open.
///
/// # Which loader do I want?
///
/// | You want | Use | Records |
/// |---|---|---|
/// | Individual trades | [`load_ticks`] / [`load_ticks_all`] | one per trade |
/// | Bars of N trades | [`load_tick_bars`] / [`load_tick_bars_all`] | one per N trades |
/// | Bars of a fixed duration | [`load_time_bars`] / [`load_time_bars_all`] | one per interval |
/// | Volume traded at each price | [`load_volume_profile_minute_bars`] | one per minute |
/// | Tick bars from a [`TickBarReplayRequest`] | [`load_tick_bar_replay`] | one per N trades |
/// | Time bars from a [`TimeBarReplayRequest`] | [`load_time_bar_replay`] | one per interval |
///
/// The struct forms take every field of the request, including
/// `user_max_count`, and send it as given: set `resume_bars` yourself to lift
/// the 10,000-record cap.
///
/// # Limits on one replay
///
/// - **10,000 records.** The plain methods stop there without saying so. The
///   `_all` methods lift the cap; prefer them.
/// - **About four seconds of streaming.** The server then cuts the reply short
///   and the plant asks it to continue, so you still get the whole window.
///   Time bar replays can occasionally stop here without warning, so check the
///   last bar reaches the end of your window. See [`load_ticks_all`].
///
/// [`load_ticks`]: RithmicHistoryPlantHandle::load_ticks
/// [`load_ticks_all`]: RithmicHistoryPlantHandle::load_ticks_all
/// [`load_tick_bars`]: RithmicHistoryPlantHandle::load_tick_bars
/// [`load_tick_bars_all`]: RithmicHistoryPlantHandle::load_tick_bars_all
/// [`load_time_bars`]: RithmicHistoryPlantHandle::load_time_bars
/// [`load_time_bars_all`]: RithmicHistoryPlantHandle::load_time_bars_all
/// [`load_volume_profile_minute_bars`]: RithmicHistoryPlantHandle::load_volume_profile_minute_bars
/// [`load_tick_bar_replay`]: RithmicHistoryPlantHandle::load_tick_bar_replay
/// [`load_time_bar_replay`]: RithmicHistoryPlantHandle::load_time_bar_replay
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant,
///     rti::messages::RithmicMessage,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Credentials come from the environment; see examples/.env.blank.
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///
///     let plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
///     let handle = plant.get_handle();
///     handle.login().await?;
///
///     let now = std::time::SystemTime::now()
///         .duration_since(std::time::UNIX_EPOCH)?
///         .as_secs() as i32;
///
///     // Every trade in the last hour, however many that is.
///     let ticks = handle
///         .load_ticks_all("ESU6".to_string(), "CME".to_string(), now - 3600, now)
///         .await?;
///
///     for response in &ticks {
///         if let RithmicMessage::ResponseTickBarReplay(tick) = &response.message {
///             println!("{:?}", tick.close_price);
///         }
///     }
///
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
///
/// # Runnable examples
///
/// - [`load_historical_ticks.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_ticks.rs)
///   — load a window of trades
/// - [`backfill.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/backfill.rs)
///   — backfill large windows and check you got all of them
/// - [`load_historical_bars.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_bars.rs)
///   — load five-minute bars
/// - [`reconnect.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/reconnect.rs)
///   — surviving a dropped connection
/// - [`.env.blank`](https://github.com/pbeets/rithmic-rs/blob/main/examples/.env.blank)
///   — the credentials the examples expect
#[derive(Debug)]
pub struct RithmicHistoryPlant {
    pub(crate) connection_handle: JoinHandle<()>,
    sender: mpsc::Sender<HistoryPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicHistoryPlant {
    /// Create a new History Plant connection to access historical market data.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy; see [`ConnectStrategy`]
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicHistoryPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] when
    /// its one attempt fails. `Retry` and `AlternateWithRetry` return it only
    /// once the config's
    /// [`retry_timeout`](crate::RithmicConfigBuilder::retry_timeout)
    /// passes, with the attempt count and the timeout in the message. Without
    /// one they retry until they connect, so this call can block
    /// indefinitely if the server is unreachable.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicHistoryPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<HistoryPlantCommand>(32);
        let capacity = config
            .subscription_capacity
            .unwrap_or(DEFAULT_SUBSCRIPTION_CAPACITY);
        let (sub_tx, _sub_rx) = broadcast::channel::<RithmicResponse>(capacity);
        let mut history_plant = HistoryPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            history_plant.run().await;
        });

        Ok(RithmicHistoryPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicHistoryPlant {
    /// Wait for the plant's background connection task to finish.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to interact with the history plant.
    ///
    /// The handle provides methods to load historical ticks, time bars, and subscribe to bar updates.
    /// Multiple handles can be created from the same plant.
    pub fn get_handle(&self) -> RithmicHistoryPlantHandle {
        RithmicHistoryPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}

#[derive(Debug)]
struct HistoryPlant {
    core: PlantCore,
    request_receiver: mpsc::Receiver<HistoryPlantCommand>,
}

impl HistoryPlant {
    async fn new(
        request_receiver: mpsc::Receiver<HistoryPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<HistoryPlant, RithmicError> {
        let core = PlantCore::new(subscription_sender, config, strategy, "history_plant").await?;

        Ok(HistoryPlant {
            core,
            request_receiver,
        })
    }
}

impl PlantActor for HistoryPlant {
    type Command = HistoryPlantCommand;

    async fn run(&mut self) {
        loop {
            self.core.request_handler.release_abandoned_replays();
            let result = self.core.next_event(&mut self.request_receiver).await;

            let stop = match result {
                SelectResult::HeartbeatFired => self.core.send_heartbeat().await,
                SelectResult::PingFired => self.core.send_ping().await,
                SelectResult::PingTimeout => self.core.handle_ping_timeout(),
                SelectResult::Command(cmd) => {
                    if matches!(cmd, HistoryPlantCommand::Abort) {
                        self.core.handle_abort()
                    } else {
                        self.handle_command(cmd).await;

                        false
                    }
                }
                SelectResult::RithmicMessage(msg) => self.core.handle_rithmic_message(msg).await,
                SelectResult::StreamClosed => self.core.handle_stream_closed(),
            };

            if stop {
                break;
            }
        }
    }

    async fn handle_command(&mut self, command: HistoryPlantCommand) {
        // Disconnect race guard — see `TickerPlant::handle_command`.
        if self.core.close_requested
            && !matches!(
                command,
                HistoryPlantCommand::Close | HistoryPlantCommand::Abort
            )
        {
            debug!("history_plant: dropping a command queued after close was requested");

            return;
        }

        match command {
            HistoryPlantCommand::Close => {
                self.core.handle_close().await;
            }
            HistoryPlantCommand::GetSystemInfo { response_sender } => {
                self.core.handle_get_system_info(response_sender).await;
            }
            HistoryPlantCommand::Login {
                config,
                response_sender,
            } => {
                self.core
                    .handle_login(config, SysInfraType::HistoryPlant, response_sender)
                    .await;
            }
            HistoryPlantCommand::Logout { response_sender } => {
                self.core.handle_logout(response_sender).await;
            }
            HistoryPlantCommand::Replay {
                query,
                response_sender,
            } => {
                let (buf, id) = match query {
                    ReplayQuery::Time(query) => {
                        self.core.rithmic_sender_api.request_time_bar_replay(&query)
                    }
                    ReplayQuery::Tick(query) => {
                        self.core.rithmic_sender_api.request_tick_bar_replay(&query)
                    }
                    ReplayQuery::Volume(query) => self
                        .core
                        .rithmic_sender_api
                        .request_volume_profile_minute_bars(&query),
                };

                self.core
                    .register_replay_and_send(buf, id, PendingReplay::new(response_sender))
                    .await;
            }
            HistoryPlantCommand::ResumeBars {
                request_key,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_resume_bars(&request_key);

                self.core.register_and_send(buf, id, response_sender).await;
            }
            HistoryPlantCommand::SubscribeTimeBarUpdates {
                symbol,
                exchange,
                bar_type,
                bar_type_period,
                request,
                response_sender,
            } => {
                let (buf, id) = self.core.rithmic_sender_api.request_time_bar_update(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_type_period,
                    request,
                );

                self.core.register_and_send(buf, id, response_sender).await;
            }
            HistoryPlantCommand::SubscribeTickBarUpdates {
                symbol,
                exchange,
                bar_type,
                bar_sub_type,
                bar_type_specifier,
                request,
                response_sender,
            } => {
                let (buf, id) = self.core.rithmic_sender_api.request_tick_bar_update(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_sub_type,
                    &bar_type_specifier,
                    request,
                );

                self.core.register_and_send(buf, id, response_sender).await;
            }
            HistoryPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
    }
}

/// The way you talk to a [`RithmicHistoryPlant`].
///
/// Get one from [`RithmicHistoryPlant::get_handle`], call
/// [`login`](Self::login), then use the `load_*` methods to pull history. The
/// handle is cheap to clone and can be shared across tasks; every clone talks to
/// the same connection.
///
/// Live bar subscriptions are different from the loaders: `subscribe_*` returns
/// only an acknowledgement, and the bars arrive on
/// [`subscription_receiver`](Self::subscription_receiver).
///
/// See [`RithmicHistoryPlant`] for what the responses look like and which loader
/// to reach for.
pub struct RithmicHistoryPlantHandle {
    sender: mpsc::Sender<HistoryPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,

    /// Receiver for historical data responses.
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl std::fmt::Debug for RithmicHistoryPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicHistoryPlantHandle")
            .field("sender", &self.sender)
            .field("subscription_sender", &self.subscription_sender)
            .finish_non_exhaustive()
    }
}

impl RithmicHistoryPlantHandle {
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
    pub async fn get_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::GetSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Log in to the Rithmic History plant
    ///
    /// This must be called before requesting historical data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic History plant with custom configuration
    ///
    /// This must be called before requesting historical data.
    ///
    /// # Arguments
    /// * `config` - Login configuration options. See [`LoginConfig`] for details.
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login_with_config(
        &self,
        config: LoginConfig,
    ) -> Result<RithmicResponse, RithmicError> {
        info!("history_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();
        let mut config = config;

        config.aggregated_quotes = None;

        let command = HistoryPlantCommand::Login {
            config,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = await_first_response(rx).await?;

        if let Some(err) = response.error.clone() {
            error!("history_plant: login failed {:?}", err);

            return Err(err);
        }

        // The actor marks itself logged in and adopts the server's heartbeat
        // period when it sees this reply, so nothing here needs to reach it.
        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(session_id) = &resp.unique_user_id {
                info!("history_plant: session id: {}", session_id);
            }
        }

        info!("history_plant: logged in");

        Ok(response)
    }

    /// Disconnect from the Rithmic History plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        // Held rather than propagated here so that `Close` is queued either way —
        // see `RithmicOrderPlantHandle::disconnect`.
        let outcome = rx.await.map_err(|_| RithmicError::ConnectionClosed);
        let _ = self.sender.send(HistoryPlantCommand::Close).await;

        let response = outcome??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;

        Ok(response)
    }

    /// Immediately shut down the history plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and a graceful `disconnect()`
    /// would not get through.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(HistoryPlantCommand::Abort);
    }

    /// Load individual trades for a symbol over a time window.
    ///
    /// Each response is one trade. This returns **at most 10,000 trades** — the
    /// limit Rithmic puts on a single replay — and gives no sign when it has cut
    /// the result short. For a window that may hold more, use
    /// [`load_ticks_all`](Self::load_ticks_all).
    ///
    /// # A quirk worth knowing
    ///
    /// Rithmic stamps the **first** record's open time with the second you asked
    /// for, at microsecond 0, rather than the trade's own time. Since the request
    /// is second-granular, that open can read up to a second early. The close
    /// time (index 1 of `data_bar_ssboe` / `data_bar_usecs`) is always the real
    /// trade time, so prefer it if you are ordering or bucketing trades. The
    /// crate passes the values through untouched; what to do about the open is
    /// yours to decide.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol, e.g. `"ESU6"`
    /// * `exchange` - The exchange code, e.g. `"CME"`
    /// * `start_time_sec` - Window start, Unix seconds
    /// * `end_time_sec` - Window end, Unix seconds
    ///
    /// # Returns
    /// One response per trade, followed by an end marker carrying no data.
    ///
    /// # Example
    /// See [`load_historical_ticks.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_ticks.rs).
    pub async fn load_ticks(
        &self,
        symbol: String,
        exchange: String,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.load_tick_bars(symbol, exchange, 1, start_time_sec, end_time_sec)
            .await
    }

    /// Load bars that each aggregate a fixed number of trades.
    ///
    /// `bar_length = 5` gives one bar per five trades. `bar_length = 1` gives one
    /// bar per trade, which is what [`load_ticks`](Self::load_ticks) is.
    ///
    /// Returns **at most 10,000 bars**, with no sign when the result was cut
    /// short. Use [`load_tick_bars_all`](Self::load_tick_bars_all) for the whole
    /// window.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol, e.g. `"ESU6"`
    /// * `exchange` - The exchange code, e.g. `"CME"`
    /// * `bar_length` - Trades per bar, at least 1
    /// * `start_time_sec` - Window start, Unix seconds
    /// * `end_time_sec` - Window end, Unix seconds
    ///
    /// # Returns
    /// One response per bar, followed by an end marker carrying no data.
    ///
    /// # Errors
    /// * [`RithmicError::InvalidArgument`] if the symbol or exchange is empty,
    ///   `bar_length` is 0, either timestamp is not positive, or the window ends
    ///   before it starts. Nothing is sent.
    /// * [`RithmicError::ConnectionClosed`] if the history plant has shut down.
    pub async fn load_tick_bars(
        &self,
        symbol: String,
        exchange: String,
        bar_length: u32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.load_tick_bar_replay(
            TickBarReplayRequest::new()
                .symbol(symbol)
                .exchange(exchange)
                .bar_length(bar_length)
                .start_time_sec(start_time_sec)
                .end_time_sec(end_time_sec),
        )
        .await
    }

    /// Load tick bars from a [`TickBarReplayRequest`] you build yourself.
    ///
    /// The positional tick loaders all end up here. Use this form to reach
    /// fields they do not expose, such as
    /// [`user_max_count`](TickBarReplayRequest::user_max_count) or a raw
    /// [`bar_type_specifier`](TickBarReplayRequest::bar_type_specifier). The
    /// request is sent exactly as given.
    ///
    /// Without [`.resume_bars(true)`](TickBarReplayRequest::resume_bars), the
    /// server caps the reply at 10,000 records and gives no sign it did. The
    /// `_all` loaders set that flag for you. See
    /// [`load_ticks_all`](Self::load_ticks_all) for how truncated replies are
    /// continued.
    ///
    /// The window here is always Unix seconds. Only daily and weekly time bars
    /// take `YYYYMMDD` dates; see [`load_time_bar_replay`](Self::load_time_bar_replay).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use rithmic_rs::{RithmicHistoryPlantHandle, TickBarReplayRequest};
    /// # async fn example(handle: RithmicHistoryPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let request = TickBarReplayRequest::new()
    ///     .symbol("ESU6")
    ///     .exchange("CME")
    ///     .bar_length(5)
    ///     .start_time_sec(1_750_000_000)
    ///     .end_time_sec(1_750_003_600)
    ///     .resume_bars(true)
    ///     .user_max_count(50_000);
    ///
    /// let bars = handle.load_tick_bar_replay(request).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Returns
    /// One response per bar, followed by an end marker carrying no data.
    ///
    /// # Errors
    /// * [`RithmicError::InvalidArgument`] if the request fails
    ///   [`validate`](TickBarReplayRequest::validate). Nothing is sent.
    /// * [`RithmicError::ConnectionClosed`] if the history plant has shut down.
    pub async fn load_tick_bar_replay(
        &self,
        request: TickBarReplayRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        request.validate()?;

        self.replay(ReplayQuery::Tick(request)).await
    }

    /// Load every trade in the window, however many there are.
    ///
    /// Same as [`load_ticks`](Self::load_ticks) but without the 10,000 record
    /// limit, so you get the whole window in one call. This is usually what you
    /// want: an hour of a liquid contract runs well past 10,000 trades, and the
    /// capped version would silently hand you only the beginning of it.
    ///
    /// # How it works
    ///
    /// A normal replay stops at 10,000 records and does not say so — the closing
    /// response looks the same whether it was cut short or not. Setting Rithmic's
    /// `resume_bars` flag on the request lifts that limit, and the server sends
    /// the rest on the same request. There is no paging and no second call.
    ///
    /// # Truncation
    ///
    /// The server also stops streaming a reply after about four seconds and
    /// sends a truncation notice. The plant asks it to continue, so this call
    /// still returns the whole window; each continuation adds about four
    /// seconds. If the server refuses to continue, this returns
    /// [`RithmicError::RequestRejected`] rather than a partial window. If it ends
    /// the replay early any other way, this returns `Ok` and the last frame has
    /// [`error`](RithmicResponse::error) set.
    ///
    /// Time bar replays have also been seen to stop early with no notice (a
    /// 60-day window of one-minute bars came back 7.5 days short). Check that
    /// the last record reaches the end of your window, and request the rest if
    /// not.
    ///
    /// A very large window may get no reply at all, so wrap the call in a
    /// timeout of your own.
    ///
    /// This is observed behaviour, not documented by Rithmic, and may change.
    ///
    /// # Cost
    ///
    /// The whole window is collected in memory before it returns. A full 23-hour
    /// ES session runs to hundreds of thousands of records, so ask for the window
    /// you actually need rather than a day at a time.
    ///
    /// The first record's open time carries the same quirk described on
    /// [`load_ticks`](Self::load_ticks).
    ///
    /// # Example
    /// See [`load_historical_ticks.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_ticks.rs).
    pub async fn load_ticks_all(
        &self,
        symbol: String,
        exchange: String,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.load_tick_bars_all(symbol, exchange, 1, start_time_sec, end_time_sec)
            .await
    }

    /// Load every fixed-trade-count bar in the window, however many there are.
    ///
    /// The uncapped form of [`load_tick_bars`](Self::load_tick_bars). See
    /// [`load_ticks_all`](Self::load_ticks_all) for how the cap is lifted and
    /// what it costs in memory.
    ///
    /// # Errors
    /// * [`RithmicError::InvalidArgument`] if the symbol or exchange is empty,
    ///   `bar_length` is 0, either timestamp is not positive, or the window ends
    ///   before it starts. Nothing is sent.
    pub async fn load_tick_bars_all(
        &self,
        symbol: String,
        exchange: String,
        bar_length: u32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.load_tick_bar_replay(
            TickBarReplayRequest::new()
                .symbol(symbol)
                .exchange(exchange)
                .bar_length(bar_length)
                .start_time_sec(start_time_sec)
                .end_time_sec(end_time_sec)
                .resume_bars(true),
        )
        .await
    }

    /// Load every time bar in the window, however many there are.
    ///
    /// The uncapped form of [`load_time_bars`](Self::load_time_bars). One-second
    /// bars pass 10,000 in under three hours, so this is the one you usually
    /// want. See [`load_ticks_all`](Self::load_ticks_all) for how the cap is
    /// lifted, the server's other limits, and the memory a window costs.
    ///
    /// Daily and weekly bars use `YYYYMMDD` dates, not Unix seconds; see
    /// [`load_time_bars`](Self::load_time_bars).
    ///
    /// # Example
    /// See [`load_historical_bars.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_bars.rs).
    pub async fn load_time_bars_all(
        &self,
        symbol: String,
        exchange: String,
        bar_type: BarType,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.load_time_bar_replay(
            TimeBarReplayRequest::new()
                .symbol(symbol)
                .exchange(exchange)
                .bar_type(bar_type)
                .bar_type_period(bar_type_period)
                .start_time_sec(start_time_sec)
                .end_time_sec(end_time_sec)
                .resume_bars(true),
        )
        .await
    }

    /// Load bars covering a fixed span of time each.
    ///
    /// `bar_type` picks the unit — second, minute, day or week — and
    /// `bar_type_period` how many of them per bar. `MinuteBar` with a period of
    /// 5 gives five-minute bars.
    ///
    /// Each bar carries a `marker`, plus its open, high, low, close, volume and
    /// trade count. For second and minute bars the `marker` is the time the bar
    /// **closed**, in Unix seconds.
    ///
    /// Daily and weekly bars use `YYYYMMDD` dates instead (e.g. `20260914`), for
    /// both the window and the `marker`, despite the `_sec` argument names.
    /// Unix seconds there return an empty reply, not an error.
    ///
    /// Returns **at most 10,000 bars**, with no sign when the result was cut
    /// short. Use [`load_time_bars_all`](Self::load_time_bars_all) for the whole
    /// window.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol, e.g. `"ESU6"`
    /// * `exchange` - The exchange code, e.g. `"CME"`
    /// * `bar_type` - `SecondBar`, `MinuteBar`, `DailyBar` or `WeeklyBar`
    /// * `bar_type_period` - How many of those units per bar
    /// * `start_time_sec` - Window start: Unix seconds, or `YYYYMMDD` for daily
    ///   and weekly bars
    /// * `end_time_sec` - Window end, in the same form as the start
    ///
    /// # Returns
    /// One response per bar, followed by an end marker carrying no data.
    ///
    /// # Example
    /// See [`load_historical_bars.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_bars.rs).
    pub async fn load_time_bars(
        &self,
        symbol: String,
        exchange: String,
        bar_type: BarType,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.load_time_bar_replay(
            TimeBarReplayRequest::new()
                .symbol(symbol)
                .exchange(exchange)
                .bar_type(bar_type)
                .bar_type_period(bar_type_period)
                .start_time_sec(start_time_sec)
                .end_time_sec(end_time_sec),
        )
        .await
    }

    /// Load time bars from a [`TimeBarReplayRequest`] you build yourself.
    ///
    /// The positional time bar loaders all end up here. Use this form to reach
    /// fields they do not expose, such as
    /// [`user_max_count`](TimeBarReplayRequest::user_max_count). The request is
    /// sent exactly as given.
    ///
    /// Without [`.resume_bars(true)`](TimeBarReplayRequest::resume_bars), the
    /// server caps the reply at 10,000 records and gives no sign it did. The
    /// `_all` loaders set that flag for you. See
    /// [`load_ticks_all`](Self::load_ticks_all) for how truncated replies are
    /// continued.
    ///
    /// For daily and weekly bars the window is `YYYYMMDD` dates (e.g.
    /// `20260914`), not Unix seconds; see
    /// [`load_time_bars`](Self::load_time_bars).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use rithmic_rs::{RithmicHistoryPlantHandle, TimeBarReplayRequest, TimeBarType};
    /// # async fn example(handle: RithmicHistoryPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let request = TimeBarReplayRequest::new()
    ///     .symbol("ESU6")
    ///     .exchange("CME")
    ///     .bar_type(TimeBarType::MinuteBar)
    ///     .bar_type_period(1)
    ///     .start_time_sec(1_750_000_000)
    ///     .end_time_sec(1_750_086_400)
    ///     .resume_bars(true)
    ///     .user_max_count(20_000);
    ///
    /// let bars = handle.load_time_bar_replay(request).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also [`load_historical_bars.rs`](https://github.com/pbeets/rithmic-rs/blob/main/examples/load_historical_bars.rs).
    ///
    /// # Returns
    /// One response per bar, followed by an end marker carrying no data.
    ///
    /// # Errors
    /// * [`RithmicError::InvalidArgument`] if the request fails
    ///   [`validate`](TimeBarReplayRequest::validate). Nothing is sent.
    /// * [`RithmicError::ConnectionClosed`] if the history plant has shut down.
    pub async fn load_time_bar_replay(
        &self,
        request: TimeBarReplayRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        request.validate()?;

        self.replay(ReplayQuery::Time(request)).await
    }

    /// Send a replay request and wait for the whole reply. The plant continues
    /// the replay if the server cuts it short.
    async fn replay(&self, query: ReplayQuery) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel();

        let command = HistoryPlantCommand::Replay {
            query,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Load minute bars that break volume down by price.
    ///
    /// Each bar reports how much traded at each price during that minute, rather
    /// than a single volume figure — useful for building a volume profile. Build
    /// the `request` with [`VolumeProfileMinuteBarsRequest`].
    ///
    /// # Returns
    /// One response per minute, followed by an end marker carrying no data.
    ///
    /// # Truncation
    ///
    /// Large windows are cut short and continued automatically, as described on
    /// [`load_ticks_all`](Self::load_ticks_all).
    ///
    /// An empty reply does not always mean nothing traded; it has been seen for
    /// windows that had data moments before. Retry before relying on it.
    pub async fn load_volume_profile_minute_bars(
        &self,
        request: VolumeProfileMinuteBarsRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        self.replay(ReplayQuery::Volume(request)).await
    }

    /// Deprecated: the plant resumes truncated replays itself, as described on
    /// [`load_ticks_all`](Self::load_ticks_all), so there is no need to call this.
    ///
    /// Ask the server to continue a replay it cut short.
    ///
    /// The `load_*` methods do this automatically and never return the notice
    /// that carries the key, so there is normally nothing to pass here; it is
    /// kept for compatibility. Called by hand, it returns only the server's
    /// acknowledgement, and the rest of the replay is discarded.
    ///
    /// Not the same as the `resume_bars` request flag, which lifts the
    /// 10,000-record cap.
    ///
    /// # Arguments
    /// * `request_key` - The key from the server's truncation notice
    ///
    /// # Returns
    /// The server's acknowledgement, `ResponseResumeBars`.
    #[deprecated(
        since = "3.2.0",
        note = "the plant resumes truncated replays itself; the continuation this requests is counted, not delivered"
    )]
    pub async fn resume_bars(
        &self,
        request_key: String,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::ResumeBars {
            request_key,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Start or stop a live feed of time bars as they complete.
    ///
    /// Unlike the loaders, this does not return the bars. It returns the
    /// server's acknowledgement, and the bars themselves then arrive on
    /// [`subscription_receiver`](Self::subscription_receiver) as they close.
    /// Pass `Request::Unsubscribe` to stop.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol, e.g. `"ESU6"`
    /// * `exchange` - The exchange code, e.g. `"CME"`
    /// * `bar_type` - `SecondBar`, `MinuteBar`, `DailyBar` or `WeeklyBar`
    /// * `bar_type_period` - How many of those units per bar
    /// * `request` - `Subscribe` or `Unsubscribe`
    pub async fn subscribe_time_bar_updates(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: request_time_bar_update::BarType,
        bar_type_period: i32,
        request: request_time_bar_update::Request,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::SubscribeTimeBarUpdates {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            bar_type,
            bar_type_period,
            request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Start or stop a live feed of tick bars as they complete.
    ///
    /// Works like [`subscribe_time_bar_updates`](Self::subscribe_time_bar_updates):
    /// the acknowledgement comes back from this call, the bars arrive on
    /// [`subscription_receiver`](Self::subscription_receiver).
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol, e.g. `"ESU6"`
    /// * `exchange` - The exchange code, e.g. `"CME"`
    /// * `bar_type` - The kind of tick bar
    /// * `bar_sub_type` - Regular or custom aggregation
    /// * `bar_type_specifier` - Trades per bar, as a string, e.g. `"1"`
    /// * `request` - `Subscribe` or `Unsubscribe`
    pub async fn subscribe_tick_bar_updates(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: request_tick_bar_update::BarType,
        bar_sub_type: request_tick_bar_update::BarSubType,
        bar_type_specifier: &str,
        request: request_tick_bar_update::Request,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::SubscribeTickBarUpdates {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            bar_type,
            bar_sub_type,
            bar_type_specifier: bar_type_specifier.to_string(),
            request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }
}

impl Clone for RithmicHistoryPlantHandle {
    fn clone(&self) -> Self {
        RithmicHistoryPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
