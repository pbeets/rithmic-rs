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
    rti::{
        messages::RithmicMessage, request_login::SysInfraType, request_tick_bar_update,
        request_time_bar_replay::BarType, request_time_bar_update,
    },
    types::{TickBarReplayRequest, TimeBarReplayRequest, VolumeProfileMinuteBarsRequest},
};

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
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UpdateHeartbeat {
        seconds: u64,
    },
    LoadTicks {
        request: TickBarReplayRequest,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    LoadTimeBars {
        request: TimeBarReplayRequest,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    LoadVolumeProfileMinuteBars {
        request: VolumeProfileMinuteBarsRequest,
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
///   a record count.
/// - **Times are Unix seconds as `i32`,** both going in and coming back. This is
///   Rithmic's own type and it overflows in 2038.
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
///
/// The plain methods return at most 10,000 records, because that is where
/// Rithmic cuts a replay off. The `_all` methods lift that cap and return the
/// whole window. Prefer an `_all` method unless you specifically want a bounded
/// result — see [`load_ticks_all`] for why, and for the memory that costs.
///
/// [`load_ticks`]: RithmicHistoryPlantHandle::load_ticks
/// [`load_ticks_all`]: RithmicHistoryPlantHandle::load_ticks_all
/// [`load_tick_bars`]: RithmicHistoryPlantHandle::load_tick_bars
/// [`load_tick_bars_all`]: RithmicHistoryPlantHandle::load_tick_bars_all
/// [`load_time_bars`]: RithmicHistoryPlantHandle::load_time_bars
/// [`load_time_bars_all`]: RithmicHistoryPlantHandle::load_time_bars_all
/// [`load_volume_profile_minute_bars`]: RithmicHistoryPlantHandle::load_volume_profile_minute_bars
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
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicHistoryPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] only.
    /// `Retry` and `AlternateWithRetry` never return an error — they retry until
    /// they connect, so this call can block indefinitely if the server is
    /// unreachable. Wrap it in `tokio::time::timeout` if you need a deadline.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicHistoryPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<HistoryPlantCommand>(32);
        let (sub_tx, _sub_rx) = broadcast::channel::<RithmicResponse>(20_000);
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
                HistoryPlantCommand::Close
                    | HistoryPlantCommand::SetLogin
                    | HistoryPlantCommand::UpdateHeartbeat { .. }
                    | HistoryPlantCommand::Abort
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
            HistoryPlantCommand::SetLogin => {
                self.core.handle_set_login();
            }
            HistoryPlantCommand::Logout { response_sender } => {
                self.core.handle_logout(response_sender).await;
            }
            HistoryPlantCommand::UpdateHeartbeat { seconds } => {
                self.core.handle_update_heartbeat(seconds);
            }
            HistoryPlantCommand::LoadTicks {
                request,
                response_sender,
            } => {
                let (tick_bar_replay_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_tick_bar_replay(&request);

                self.core
                    .register_and_send(tick_bar_replay_buf, id, response_sender)
                    .await;
            }
            HistoryPlantCommand::LoadTimeBars {
                request,
                response_sender,
            } => {
                let (time_bar_replay_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_time_bar_replay(&request);

                self.core
                    .register_and_send(time_bar_replay_buf, id, response_sender)
                    .await;
            }
            HistoryPlantCommand::LoadVolumeProfileMinuteBars {
                request,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_volume_profile_minute_bars(&request);

                self.core.register_and_send(buf, id, response_sender).await;
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

        let _ = self.sender.send(HistoryPlantCommand::SetLogin).await;

        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(hb) = resp.heartbeat_interval {
                let secs = hb as u64;
                self.update_heartbeat(secs).await;
            }

            if let Some(session_id) = &resp.unique_user_id {
                info!("history_plant: session id: {}", session_id);
            }
        }

        info!("history_plant: logged in");

        Ok(response)
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = HistoryPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
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
        self.tick_bar_replay(
            TickBarReplayRequest::new()
                .symbol(symbol)
                .exchange(exchange)
                .bar_length(bar_length)
                .start_time_sec(start_time_sec)
                .end_time_sec(end_time_sec),
        )
        .await
    }

    /// One tick bar replay request.
    async fn tick_bar_replay(
        &self,
        request: TickBarReplayRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        request.validate()?;

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::LoadTicks {
            request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
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
        self.tick_bar_replay(
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
    /// lifted and what it costs in memory.
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
        self.time_bar_replay(
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
    /// Each bar carries a `marker`, which is the time the bar **closed**, plus
    /// its open, high, low, close, volume and trade count.
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
    /// * `start_time_sec` - Window start, Unix seconds
    /// * `end_time_sec` - Window end, Unix seconds
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
        self.time_bar_replay(
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

    /// One time bar replay request.
    async fn time_bar_replay(
        &self,
        request: TimeBarReplayRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        request.validate()?;

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::LoadTimeBars {
            request,
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
    pub async fn load_volume_profile_minute_bars(
        &self,
        request: VolumeProfileMinuteBarsRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::LoadVolumeProfileMinuteBars {
            request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Resume a bars request from a previous response's `request_key`.
    ///
    /// Rithmic's release notes introduce `RequestResumeBars` as the way to pull
    /// the chunks a truncated replay left out, but the server has not been seen
    /// to hand out a `request_key` to call it with — see
    /// [`RithmicResponse::resume_key`]. Setting `resume_bars` on the replay
    /// request is what actually lifts the cap, which is what
    /// [`load_ticks_all`](Self::load_ticks_all) does. This stays for a server
    /// that does send a key.
    ///
    /// # Arguments
    /// * `request_key` - The `request_key` carried on the previous response
    ///
    /// # Returns
    /// The remaining bar data responses or an error message
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
