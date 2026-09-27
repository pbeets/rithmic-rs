use std::convert::Infallible;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info};

use crate::{
    ConnectStrategy,
    api::receiver_api::RithmicResponse,
    config::{LoginConfig, RithmicConfig},
    error::RithmicError,
    plants::{
        actor::Plant,
        await_all_responses, await_first_response,
        kind::{Cx, PlantCommand, PlantKind},
    },
    request_handler::RequestResult,
    rti::{
        messages::RithmicMessage,
        request_depth_by_order_updates,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
        request_market_data_update_by_underlying, request_search_symbols,
    },
};

/// Default subscription channel capacity.
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 10_000;

/// What a [`RithmicTickerPlantHandle`] asks the plant's task to do.
pub(crate) enum TickerPlantCommand {
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
    Subscribe {
        symbol: String,
        exchange: String,
        fields: Vec<UpdateBits>,
        request_type: Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeOrderBook {
        symbol: String,
        exchange: String,
        request_type: request_depth_by_order_updates::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    RequestDepthByOrderSnapshot {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SearchSymbols {
        search_text: String,
        exchange: Option<String>,
        product_code: Option<String>,
        instrument_type: Option<request_search_symbols::InstrumentType>,
        pattern: Option<request_search_symbols::Pattern>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ListExchangePermissions {
        user: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetInstrumentByUnderlying {
        underlying_symbol: String,
        exchange: String,
        expiration_date: Option<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeByUnderlying {
        underlying_symbol: String,
        exchange: String,
        expiration_date: Option<String>,
        fields: Vec<request_market_data_update_by_underlying::UpdateBits>,
        request_type: request_market_data_update_by_underlying::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetTickSizeTypeTable {
        tick_size_type: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetProductCodes {
        exchange: Option<String>,
        give_toi_products_only: Option<bool>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetVolumeAtPrice {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetAuxilliaryReferenceData {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetReferenceData {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetFrontMonthContract {
        symbol: String,
        exchange: String,
        need_updates: bool,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetSystemGatewayInfo {
        system_name: Option<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
}

/// Real-time market data and instrument reference data from Rithmic.
///
/// The feeds cover last trades, best bid and offer (BBO), order book depth,
/// market mode, session prices, quote statistics, indicator prices, open
/// interest, end-of-day prices, price limits and margin rates. See the
/// `subscribe_*` methods on [`RithmicTickerPlantHandle`].
///
/// The plant runs on its own background task. [`connect`](Self::connect)
/// only opens the connection; log in through a handle before anything else.
///
/// # Connection health
///
/// The subscription receiver also carries connection events, each with
/// [`error`](RithmicResponse::error) set:
/// - [`RithmicMessage::HeartbeatTimeout`] when a WebSocket ping or heartbeat
///   times out or cannot be sent. The plant has stopped.
/// - [`RithmicMessage::ConnectionError`] when the connection drops or a
///   write times out, or after [`abort`](RithmicTickerPlantHandle::abort).
/// - [`RithmicMessage::ForcedLogout`] when the server ends the session. A
///   `ConnectionError` follows it and the plant stops.
///
/// For all of these, [`RithmicError::is_connection_issue`] returns true. The
/// plant does not reconnect: connect a new one and subscribe again.
///
/// The plant heartbeats on its own and drops the replies. A heartbeat the
/// server refuses also arrives as `HeartbeatTimeout`, but its error is the
/// refusal, and `is_connection_issue` returns false for it.
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant,
///     rti::messages::RithmicMessage,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Load configuration from environment
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///
///     // Connect to the ticker plant
///     let ticker_plant =
///         RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
///
///     let mut handle = ticker_plant.get_handle();
///
///     // Login to the ticker plant
///     handle.login().await?;
///
///     // Subscribe to market data for a symbol
///     handle.subscribe("ESZ6", "CME").await?;
///
///     // Process incoming updates
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 // Check for connection errors
///                 if let Some(err) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, err);
///
///                     if err.is_connection_issue() {
///                         eprintln!(
///                             "Connection health issue - reconnection needed"
///                         );
///
///                         break;
///                     }
///
///                     continue;
///                 }
///
///                 match update.message {
///                     RithmicMessage::LastTrade(trade) => {
///                         println!("Trade: {:?}", trade);
///                     }
///
///                     RithmicMessage::BestBidOffer(bbo) => {
///                         println!("BBO: {:?}", bbo);
///                     }
///
///                     _ => {}
///                 }
///             }
///
///             Err(e) => {
///                 eprintln!("Channel error: {}", e);
///                 break;
///             }
///         }
///     }
///
///     // Cleanup
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct RithmicTickerPlant {
    pub(crate) connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicTickerPlant {
    /// Connect to the Rithmic Ticker Plant to access real-time market data.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration with credentials and server URLs
    /// * `strategy` - Connection strategy; see [`ConnectStrategy`]
    ///
    /// # Returns
    /// The connected plant, not yet logged in. Log in through
    /// [`get_handle`](Self::get_handle) before making requests.
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] when
    /// its one attempt fails. `Retry` and `AlternateWithRetry` return it only
    /// once the config's
    /// [`retry_timeout`](crate::RithmicConfigBuilder::retry_timeout)
    /// passes, with the attempt count and the timeout in the message. Without
    /// one they retry until they connect, so this call can block
    /// indefinitely if the server is unreachable.
    ///
    /// # Example
    /// ```no_run
    /// use rithmic_rs::{
    ///     RithmicConfig, RithmicEnv, RithmicTickerPlant, ConnectStrategy,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    ///
    ///     let ticker_plant =
    ///         RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicTickerPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<TickerPlantCommand>(64);
        let capacity = config
            .subscription_capacity
            .unwrap_or(DEFAULT_SUBSCRIPTION_CAPACITY);
        let (sub_tx, _sub_rx) = broadcast::channel(capacity);
        let mut ticker_plant =
            Plant::new(TickerPlant, req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            ticker_plant.run().await;
        });

        Ok(RithmicTickerPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicTickerPlant {
    /// Wait for the plant's background task to finish.
    ///
    /// It finishes after a disconnect or abort, or once the connection is
    /// lost. Until then this waits.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to log in, subscribe and make requests.
    ///
    /// Every handle talks to the same connection. Each gets its own
    /// [`subscription_receiver`](RithmicTickerPlantHandle::subscription_receiver),
    /// which sees every update sent after the handle was made.
    pub fn get_handle(&self) -> RithmicTickerPlantHandle {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

/// The ticker plant's commands. It loads nothing after login.
#[derive(Debug, Default)]
struct TickerPlant;

impl PlantKind for TickerPlant {
    type Command = TickerPlantCommand;
    type Tag = Infallible;

    const SOURCE: &'static str = "ticker_plant";
    const INFRA: SysInfraType = SysInfraType::TickerPlant;

    fn shared(command: TickerPlantCommand) -> Result<PlantCommand, TickerPlantCommand> {
        match command {
            TickerPlantCommand::Close => Ok(PlantCommand::Close),
            TickerPlantCommand::Abort => Ok(PlantCommand::Abort),
            TickerPlantCommand::GetSystemInfo { response_sender } => {
                Ok(PlantCommand::GetSystemInfo { response_sender })
            }
            TickerPlantCommand::Login {
                config,
                response_sender,
            } => Ok(PlantCommand::Login {
                config,
                response_sender,
            }),
            TickerPlantCommand::Logout { response_sender } => {
                Ok(PlantCommand::Logout { response_sender })
            }
            command => Err(command),
        }
    }

    fn on_command(&mut self, command: TickerPlantCommand, cx: &mut Cx<'_, Infallible>) {
        match command {
            TickerPlantCommand::Subscribe {
                symbol,
                exchange,
                fields,
                request_type,
                response_sender,
            } => cx.send_for(
                |api| api.request_market_data_update(&symbol, &exchange, fields, request_type),
                response_sender,
            ),
            TickerPlantCommand::SubscribeOrderBook {
                symbol,
                exchange,
                request_type,
                response_sender,
            } => cx.send_for(
                |api| api.request_depth_by_order_updates(&symbol, &exchange, request_type),
                response_sender,
            ),
            TickerPlantCommand::RequestDepthByOrderSnapshot {
                symbol,
                exchange,
                response_sender,
            } => cx.send_for(
                |api| api.request_depth_by_order_snapshot(&symbol, &exchange),
                response_sender,
            ),
            TickerPlantCommand::SearchSymbols {
                search_text,
                exchange,
                product_code,
                instrument_type,
                pattern,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_search_symbols(
                        &search_text,
                        exchange.as_deref(),
                        product_code.as_deref(),
                        instrument_type,
                        pattern,
                    )
                },
                response_sender,
            ),
            TickerPlantCommand::ListExchangePermissions {
                user,
                response_sender,
            } => cx.send_for(
                |api| api.request_list_exchange_permissions(&user),
                response_sender,
            ),
            TickerPlantCommand::GetInstrumentByUnderlying {
                underlying_symbol,
                exchange,
                expiration_date,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_get_instrument_by_underlying(
                        &underlying_symbol,
                        &exchange,
                        expiration_date.as_deref(),
                    )
                },
                response_sender,
            ),
            TickerPlantCommand::SubscribeByUnderlying {
                underlying_symbol,
                exchange,
                expiration_date,
                fields,
                request_type,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_market_data_update_by_underlying(
                        &underlying_symbol,
                        &exchange,
                        expiration_date.as_deref(),
                        fields,
                        request_type,
                    )
                },
                response_sender,
            ),
            TickerPlantCommand::GetTickSizeTypeTable {
                tick_size_type,
                response_sender,
            } => cx.send_for(
                |api| api.request_give_tick_size_type_table(&tick_size_type),
                response_sender,
            ),
            TickerPlantCommand::GetProductCodes {
                exchange,
                give_toi_products_only,
                response_sender,
            } => cx.send_for(
                |api| api.request_product_codes(exchange.as_deref(), give_toi_products_only),
                response_sender,
            ),
            TickerPlantCommand::GetVolumeAtPrice {
                symbol,
                exchange,
                response_sender,
            } => cx.send_for(
                |api| api.request_get_volume_at_price(&symbol, &exchange),
                response_sender,
            ),
            TickerPlantCommand::GetAuxilliaryReferenceData {
                symbol,
                exchange,
                response_sender,
            } => cx.send_for(
                |api| api.request_auxilliary_reference_data(&symbol, &exchange),
                response_sender,
            ),
            TickerPlantCommand::GetReferenceData {
                symbol,
                exchange,
                response_sender,
            } => cx.send_for(
                |api| api.request_reference_data(&symbol, &exchange),
                response_sender,
            ),
            TickerPlantCommand::GetFrontMonthContract {
                symbol,
                exchange,
                need_updates,
                response_sender,
            } => cx.send_for(
                |api| api.request_front_month_contract(&symbol, &exchange, need_updates),
                response_sender,
            ),
            TickerPlantCommand::GetSystemGatewayInfo {
                system_name,
                response_sender,
            } => cx.send_for(
                |api| api.request_rithmic_system_gateway_info(system_name.as_deref()),
                response_sender,
            ),
            TickerPlantCommand::Close
            | TickerPlantCommand::Abort
            | TickerPlantCommand::GetSystemInfo { .. }
            | TickerPlantCommand::Login { .. }
            | TickerPlantCommand::Logout { .. } => {
                unreachable!("the plant handles the commands every plant shares")
            }
        }
    }

    fn on_reply(&mut self, tag: Infallible, _reply: RequestResult) {
        match tag {}
    }
}

/// Handle for sending commands to a [`RithmicTickerPlant`] and receiving market data updates.
///
/// Obtained from [`RithmicTickerPlant::get_handle`]. Use the methods on this handle to
/// log in, subscribe to symbols, and request reference data. Real-time updates arrive
/// on [`subscription_receiver`](Self::subscription_receiver). Cloning a handle gives
/// the clone a fresh receiver.
///
/// # Replies and refusals
///
/// Each method waits for the server's reply. A refusal comes back as `Ok`
/// with [`RithmicResponse::error`] set, usually to
/// [`RithmicError::RequestRejected`], so check it. Only
/// [`login`](Self::login) turns a refusal into `Err`.
///
/// `Err` means no reply came: [`RithmicError::ConnectionClosed`] once the
/// plant has stopped or is disconnecting, or [`RithmicError::SendFailed`]
/// if the request could not be written.
///
/// # Updates
///
/// A `subscribe_*` call returns only the acknowledgement. The data arrives
/// on `subscription_receiver`, mixed with the connection events described on
/// [`RithmicTickerPlant`]. Subscriptions end with the connection; after a
/// reconnect, subscribe again.
pub struct RithmicTickerPlantHandle {
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,

    /// Market data updates and connection events.
    ///
    /// A [`broadcast`] receiver: if you fall more than the
    /// [`subscription_capacity`](crate::RithmicConfigBuilder::subscription_capacity)
    /// behind (10,000 by default), `recv` returns `RecvError::Lagged` and the
    /// skipped updates are gone.
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl std::fmt::Debug for RithmicTickerPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicTickerPlantHandle")
            .field("sender", &self.sender)
            .field("subscription_sender", &self.subscription_sender)
            .finish_non_exhaustive()
    }
}

impl RithmicTickerPlantHandle {
    /// Ask the server which Rithmic systems it offers.
    ///
    /// The reply is a [`RithmicMessage::ResponseRithmicSystemInfo`] listing
    /// the system names and whether each supports aggregated quotes.
    pub async fn get_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = await_first_response(rx).await?;

        Ok(response)
    }

    /// Log in to the Rithmic ticker plant
    ///
    /// This must be called before subscribing to any market data.
    /// Defaults to tick-by-tick (non-aggregated) quotes.
    ///
    /// To customize login options, use [`login_with_config`](Self::login_with_config).
    ///
    /// The plant logs in once per connection. A call with the same config made
    /// while that login is in progress waits for it, and one made after it
    /// returns its response at once. Neither sends anything.
    ///
    /// # Returns
    /// The login response, once the server accepts the login.
    ///
    /// # Errors
    /// * The error the server's refusal carries, usually
    ///   [`RithmicError::RequestRejected`]. You can log in again.
    /// * [`RithmicError::LoginConflict`] if this plant is logging in, or is
    ///   logged in, with a different [`LoginConfig`].
    /// * [`RithmicError::ConnectionClosed`] if the plant disconnects before
    ///   the login is done, or has disconnected.
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic ticker plant with custom configuration
    ///
    /// This must be called before subscribing to any market data. Leaving
    /// `aggregated_quotes` unset means tick-by-tick quotes, the same config as
    /// [`login`](Self::login).
    ///
    /// # Arguments
    /// * `config` - Login configuration options. See [`LoginConfig`] for details.
    ///
    /// # Returns
    /// The login response, once the server accepts the login.
    ///
    /// # Errors
    /// As for [`login`](Self::login). [`RithmicError::LoginConflict`] means
    /// this plant logged in, or is logging in, with a config other than
    /// `config`.
    pub async fn login_with_config(
        &self,
        config: LoginConfig,
    ) -> Result<RithmicResponse, RithmicError> {
        info!("ticker_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        // Default aggregated_quotes to false for ticker plant
        let mut config = config;

        if config.aggregated_quotes.is_none() {
            config.aggregated_quotes = Some(false);
        }

        let command = TickerPlantCommand::Login {
            config,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = await_first_response(rx).await?;

        if let Some(err) = response.error.clone() {
            error!("ticker_plant: login failed {:?}", err);

            return Err(err);
        }

        // The actor owns the session: it heartbeats before this reply reaches
        // us, whether or not anyone is still waiting for it.
        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(session_id) = &resp.unique_user_id {
                info!("ticker_plant: session id: {}", session_id);
            }
        }

        info!("ticker_plant: logged in");

        Ok(response)
    }

    /// Log out and close the connection.
    ///
    /// Waits for the logout reply, then closes the WebSocket whether or not
    /// the logout succeeded. Requests still waiting, and anything sent later
    /// from any handle, fail with [`RithmicError::ConnectionClosed`]. Use
    /// [`RithmicTickerPlant::await_shutdown`] to wait for the plant to stop.
    ///
    /// # Returns
    /// The logout reply, or [`RithmicError::ConnectionClosed`] if the plant
    /// had already stopped.
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        // Held rather than propagated here so that `Close` is queued either way —
        // see `RithmicOrderPlantHandle::disconnect`.
        let outcome = rx.await.map_err(|_| RithmicError::ConnectionClosed);
        let _ = self.sender.send(TickerPlantCommand::Close).await;

        outcome??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Immediately shut down the ticker plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and a graceful
    /// [`disconnect`](Self::disconnect) would not get through. Waiting
    /// requests fail with [`RithmicError::ConnectionClosed`], and the
    /// subscription channel gets a [`RithmicMessage::ConnectionError`].
    ///
    /// Does not wait. Safe to call if the plant has already stopped. If the
    /// plant's command queue is full, the abort is dropped.
    pub fn abort(&self) {
        let _ = self.sender.try_send(TickerPlantCommand::Abort);
    }

    /// Subscribe to last trades and best bid/offer for a symbol.
    ///
    /// Updates arrive on `subscription_receiver` as
    /// [`RithmicMessage::LastTrade`] and [`RithmicMessage::BestBidOffer`].
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn subscribe(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            Request::Subscribe,
        )
        .await
    }

    /// Subscribe to order book depth-by-order updates for a specific symbol
    ///
    /// Updates arrive on `subscription_receiver` as [`RithmicMessage::DepthByOrder`]
    /// and [`RithmicMessage::DepthByOrderEndEvent`]. For the current book, call
    /// [`get_depth_by_order_snapshot`](Self::get_depth_by_order_snapshot).
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn subscribe_depth_by_order_update(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::SubscribeOrderBook {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            request_type: request_depth_by_order_updates::Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Unsubscribe from the last trades and best bid/offer that
    /// [`subscribe`](Self::subscribe) asked for.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            Request::Unsubscribe,
        )
        .await
    }

    /// Unsubscribe from order book depth-by-order updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_depth_by_order_update(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::SubscribeOrderBook {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            request_type: request_depth_by_order_updates::Request::Unsubscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Send a market data (template 100) request for the given update bits.
    async fn request_market_data_update(
        &self,
        symbol: &str,
        exchange: &str,
        fields: Vec<UpdateBits>,
        request_type: Request,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::Subscribe {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            fields,
            request_type,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Subscribe to instrument status updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, market mode changes arrive as [`RithmicMessage::MarketMode`]
    /// on `subscription_receiver`.
    pub async fn subscribe_instrument_status(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::MarketMode],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from instrument status updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_instrument_status(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::MarketMode],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to level-1 order book summary updates for a specific symbol.
    ///
    /// This uses `request_market_data_update` (proto 100) with `UpdateBits::OrderBook`
    /// and delivers aggregated bid/ask summary ticks. It is distinct from
    /// [`subscribe_depth_by_order_update`](Self::subscribe_depth_by_order_update), which uses
    /// `request_depth_by_order_updates` (proto 104) for full depth-by-order streaming.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, aggregated bid/ask updates arrive as [`RithmicMessage::OrderBook`]
    /// on `subscription_receiver`.
    pub async fn subscribe_order_book_summary(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::OrderBook],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from level-1 order book summary updates for a specific symbol.
    ///
    /// This reverses [`subscribe_order_book_summary`](Self::subscribe_order_book_summary).
    /// Use [`unsubscribe_depth_by_order_update`](Self::unsubscribe_depth_by_order_update) to stop the
    /// dedicated depth-by-order stream instead.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_order_book_summary(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::OrderBook],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to session price updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, intraday high, low, and open price updates arrive as
    /// [`RithmicMessage::TradeStatistics`] on `subscription_receiver`.
    pub async fn subscribe_session_prices(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::Open, UpdateBits::HighLow],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from session price updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_session_prices(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::Open, UpdateBits::HighLow],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to quote statistics updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, high bid / low ask updates arrive as
    /// [`RithmicMessage::QuoteStatistics`] on `subscription_receiver`.
    pub async fn subscribe_quote_statistics(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::HighBidLowAsk],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from quote statistics updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_quote_statistics(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::HighBidLowAsk],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to indicator price updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, opening and closing indicator prices arrive as
    /// [`RithmicMessage::IndicatorPrices`] on `subscription_receiver`.
    pub async fn subscribe_indicator_prices(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::OpeningIndicator, UpdateBits::ClosingIndicator],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from indicator price updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_indicator_prices(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::OpeningIndicator, UpdateBits::ClosingIndicator],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to open interest updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, open interest updates arrive as [`RithmicMessage::OpenInterest`]
    /// on `subscription_receiver`.
    pub async fn subscribe_open_interest(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::OpenInterest],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from open interest updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_open_interest(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::OpenInterest],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to end-of-day price updates for a specific symbol.
    ///
    /// Sends the `Close`, `Settlement`, `ProjectedSettlement`, and
    /// `AdjustedClose` update bits.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// Updates arrive as [`RithmicMessage::EndOfDayPrices`] on
    /// `subscription_receiver`.
    pub async fn subscribe_end_of_day_prices(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![
                UpdateBits::Close,
                UpdateBits::Settlement,
                UpdateBits::ProjectedSettlement,
                UpdateBits::AdjustedClose,
            ],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from end-of-day price updates for a specific symbol.
    ///
    /// This reverses [`subscribe_end_of_day_prices`](Self::subscribe_end_of_day_prices).
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_end_of_day_prices(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![
                UpdateBits::Close,
                UpdateBits::Settlement,
                UpdateBits::ProjectedSettlement,
                UpdateBits::AdjustedClose,
            ],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to order price limit updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, high and low price limit updates arrive as
    /// [`RithmicMessage::OrderPriceLimits`] on `subscription_receiver`.
    pub async fn subscribe_order_price_limits(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::HighPriceLimit, UpdateBits::LowPriceLimit],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from order price limit updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_order_price_limits(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::HighPriceLimit, UpdateBits::LowPriceLimit],
            Request::Unsubscribe,
        )
        .await
    }

    /// Subscribe to symbol margin rate updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    ///
    /// # Updates
    /// After subscribing, margin rate updates arrive as [`RithmicMessage::SymbolMarginRate`]
    /// on `subscription_receiver`.
    pub async fn subscribe_symbol_margin_rate(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::MarginRate],
            Request::Subscribe,
        )
        .await
    }

    /// Unsubscribe from symbol margin rate updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn unsubscribe_symbol_margin_rate(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        self.request_market_data_update(
            symbol,
            exchange,
            vec![UpdateBits::MarginRate],
            Request::Unsubscribe,
        )
        .await
    }

    /// Get the current depth-by-order book for a symbol.
    ///
    /// Rithmic sends the book as several frames, one price level each, and
    /// this waits for all of them.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// Every frame of the reply, each a
    /// [`RithmicMessage::ResponseDepthByOrderSnapshot`]. A refusal is a single
    /// frame with [`error`](RithmicResponse::error) set.
    pub async fn get_depth_by_order_snapshot(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::RequestDepthByOrderSnapshot {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Search for instruments by text.
    ///
    /// # Arguments
    /// * `search_text` - The text to search for
    /// * `exchange` - Only search this exchange
    /// * `product_code` - Only search this product, e.g. `"ES"`
    /// * `instrument_type` - Only return this type, e.g. `Future`
    /// * `pattern` - `Equals` or `Contains`, or `None` to send none
    ///
    /// # Returns
    /// Every frame of the reply, each a [`RithmicMessage::ResponseSearchSymbols`].
    pub async fn search_symbols(
        &self,
        search_text: &str,
        exchange: Option<&str>,
        product_code: Option<&str>,
        instrument_type: Option<request_search_symbols::InstrumentType>,
        pattern: Option<request_search_symbols::Pattern>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::SearchSymbols {
            search_text: search_text.to_string(),
            exchange: exchange.map(|e| e.to_string()),
            product_code: product_code.map(|p| p.to_string()),
            instrument_type,
            pattern,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List the exchanges a user may get market data for.
    ///
    /// Each frame names one exchange and the user's level 1 (top of book) and
    /// level 2 (order book) market data permissions there.
    ///
    /// # Arguments
    /// * `user` - The Rithmic user name to look up
    ///
    /// # Returns
    /// Every frame of the reply, each a
    /// [`RithmicMessage::ResponseListExchangePermissions`].
    pub async fn list_exchange_permissions(
        &self,
        user: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::ListExchangePermissions {
            user: user.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List the instruments that have the given underlying.
    ///
    /// # Arguments
    /// * `underlying_symbol` - The underlying symbol (e.g., "ES" for E-mini S&P 500)
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `expiration_date` - Only return instruments with this expiration
    ///
    /// # Returns
    /// Every frame of the reply.
    pub async fn get_instrument_by_underlying(
        &self,
        underlying_symbol: &str,
        exchange: &str,
        expiration_date: Option<&str>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetInstrumentByUnderlying {
            underlying_symbol: underlying_symbol.to_string(),
            exchange: exchange.to_string(),
            expiration_date: expiration_date.map(|d| d.to_string()),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Subscribe to, or unsubscribe from, market data for every instrument of
    /// an underlying.
    ///
    /// Updates arrive on `subscription_receiver`, as for the per-symbol
    /// `subscribe_*` methods.
    ///
    /// # Arguments
    /// * `underlying_symbol` - The underlying symbol (e.g., "ES")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `expiration_date` - Only instruments with this expiration
    /// * `fields` - The update kinds to turn on or off; they are sent as one bit mask
    /// * `request_type` - `Subscribe` or `Unsubscribe`
    ///
    /// # Returns
    /// The server's acknowledgement. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn subscribe_by_underlying(
        &self,
        underlying_symbol: &str,
        exchange: &str,
        expiration_date: Option<&str>,
        fields: Vec<request_market_data_update_by_underlying::UpdateBits>,
        request_type: request_market_data_update_by_underlying::Request,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::SubscribeByUnderlying {
            underlying_symbol: underlying_symbol.to_string(),
            exchange: exchange.to_string(),
            expiration_date: expiration_date.map(|d| d.to_string()),
            fields,
            request_type,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get the tick size table for a tick size type.
    ///
    /// # Arguments
    /// * `tick_size_type` - The tick size type, as named in the instrument's
    ///   reference data
    ///
    /// # Returns
    /// Every frame of the reply, each a
    /// [`RithmicMessage::ResponseGiveTickSizeTypeTable`].
    pub async fn get_tick_size_type_table(
        &self,
        tick_size_type: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetTickSizeTypeTable {
            tick_size_type: tick_size_type.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List product codes, such as `"ES"` or `"NQ"`.
    ///
    /// # Arguments
    /// * `exchange` - Only list this exchange's products
    /// * `give_toi_products_only` - Sets Rithmic's `give_toi_products_only` flag
    ///
    /// # Returns
    /// Every frame of the reply, each a [`RithmicMessage::ResponseProductCodes`].
    pub async fn get_product_codes(
        &self,
        exchange: Option<&str>,
        give_toi_products_only: Option<bool>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetProductCodes {
            exchange: exchange.map(|e| e.to_string()),
            give_toi_products_only,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Get how much has traded at each price for a symbol.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// Every frame of the reply, each a [`RithmicMessage::ResponseGetVolumeAtPrice`]
    /// holding a list of prices and the volume at each.
    pub async fn get_volume_at_price(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetVolumeAtPrice {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Get a contract's calendar and settlement details.
    ///
    /// The reply holds dates such as first and last trading, notice and
    /// delivery, plus the settlement method and unit of measure.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A [`RithmicMessage::ResponseAuxilliaryReferenceData`].
    pub async fn get_auxilliary_reference_data(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetAuxilliaryReferenceData {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get reference data for a symbol
    ///
    /// Returns detailed information about a trading instrument including
    /// tick size, point value, trading hours, and other specifications.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESZ6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A [`RithmicMessage::ResponseReferenceData`].
    pub async fn get_reference_data(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetReferenceData {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get the current front month contract for a product.
    ///
    /// The contract's symbol is in the reply's `trading_symbol`.
    ///
    /// # Arguments
    /// * `symbol` - The product symbol (e.g., "ES" for E-mini S&P 500)
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `need_updates` - If true, [`RithmicMessage::FrontMonthContractUpdate`]
    ///   messages arrive on `subscription_receiver` when the front month changes
    ///
    /// # Returns
    /// A [`RithmicMessage::ResponseFrontMonthContract`].
    pub async fn get_front_month_contract(
        &self,
        symbol: &str,
        exchange: &str,
        need_updates: bool,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetFrontMonthContract {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            need_updates,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// List the gateways of a Rithmic system.
    ///
    /// # Arguments
    /// * `system_name` - The system to ask about, or `None` to send none
    ///
    /// # Returns
    /// A [`RithmicMessage::ResponseRithmicSystemGatewayInfo`] with the
    /// gateway names and URIs.
    pub async fn get_system_gateway_info(
        &self,
        system_name: Option<&str>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::GetSystemGatewayInfo {
            system_name: system_name.map(|s| s.to_string()),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }
}

impl Clone for RithmicTickerPlantHandle {
    fn clone(&self) -> Self {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
