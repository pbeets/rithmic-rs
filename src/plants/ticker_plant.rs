use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info};

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
        messages::RithmicMessage,
        request_depth_by_order_updates,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
        request_market_data_update_by_underlying, request_search_symbols,
    },
};

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
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UpdateHeartbeat {
        seconds: u64,
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

/// The RithmicTickerPlant provides access to real-time market data.
///
/// Market data updates include last trades, best bid and offer (BBO), order
/// book depth, market mode, session prices, quote statistics, indicator
/// prices, open interest, end-of-day prices, price limits, and margin rates —
/// see the `subscribe_*` methods on [`RithmicTickerPlantHandle`].
///
/// # Connection Health Monitoring
///
/// The subscription receiver also provides connection health events:
/// - **WebSocket ping/pong timeouts**: primary dead-connection signal (auto-detected)
/// - **Heartbeat errors**: forwarded as `HeartbeatTimeout`
/// - **Forced logout events**: session terminated by the server
///
/// **Note:** Heartbeat requests are sent automatically for protocol compliance,
/// but successful responses are dropped. Only heartbeat errors are forwarded as
/// `HeartbeatTimeout` messages.
///
/// # Example: Basic Usage
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
///     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
///     let mut handle = ticker_plant.get_handle();
///
///     // Login to the ticker plant
///     handle.login().await?;
///
///     // Subscribe to market data for a symbol
///     handle.subscribe("ESH6", "CME").await?;
///
///     // Process incoming updates
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 // Check for connection errors
///                 if let Some(err) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, err);
///                     if err.is_connection_issue() {
///                         eprintln!("Connection health issue - reconnection needed");
///                         break;
///                     }
///                     continue;
///                 }
///
///                 match update.message {
///                     RithmicMessage::LastTrade(trade) => {
///                         println!("Trade: {:?}", trade);
///                     }
///                     RithmicMessage::BestBidOffer(bbo) => {
///                         println!("BBO: {:?}", bbo);
///                     }
///                     RithmicMessage::ForcedLogout(logout) => {
///                         eprintln!("Forced logout: {:?}", logout);
///                         break; // Must reconnect
///                     }
///                     _ => {}
///                 }
///             }
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
///
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
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicTickerPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] only.
    /// `Retry` and `AlternateWithRetry` never return an error — they retry until
    /// they connect, so this call can block indefinitely if the server is
    /// unreachable. Wrap it in `tokio::time::timeout` if you need a deadline.
    ///
    /// # Example
    /// ```no_run
    /// use rithmic_rs::{RithmicConfig, RithmicEnv, RithmicTickerPlant, ConnectStrategy};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    ///     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicTickerPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<TickerPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);
        let mut ticker_plant = TickerPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

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
    /// Wait for the plant's background connection task to finish.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to interact with the ticker plant.
    ///
    /// The handle provides methods to subscribe to market data and receive updates.
    /// Multiple handles can be created from the same plant.
    pub fn get_handle(&self) -> RithmicTickerPlantHandle {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
struct TickerPlant {
    core: PlantCore,
    request_receiver: mpsc::Receiver<TickerPlantCommand>,
}

impl TickerPlant {
    async fn new(
        request_receiver: mpsc::Receiver<TickerPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<TickerPlant, RithmicError> {
        let core = PlantCore::new(subscription_sender, config, strategy, "ticker_plant").await?;

        Ok(TickerPlant {
            core,
            request_receiver,
        })
    }
}

impl PlantActor for TickerPlant {
    type Command = TickerPlantCommand;

    /// Execute the ticker plant actor loop.
    async fn run(&mut self) {
        loop {
            let result = self.core.next_event(&mut self.request_receiver).await;
            let stop = match result {
                SelectResult::HeartbeatFired => self.core.send_heartbeat().await,
                SelectResult::PingFired => self.core.send_ping().await,
                SelectResult::PingTimeout => self.core.handle_ping_timeout(),
                SelectResult::Command(cmd) => {
                    if matches!(cmd, TickerPlantCommand::Abort) {
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

    async fn handle_command(&mut self, command: TickerPlantCommand) {
        // Drop a request queued after `close_requested`; handles report the dropped
        // responder as `ConnectionClosed`. The listed variants carry none and must
        // still run — `Close` has to reach `handle_close()`. All four plants alike.
        if self.core.close_requested
            && !matches!(
                command,
                TickerPlantCommand::Close
                    | TickerPlantCommand::SetLogin
                    | TickerPlantCommand::UpdateHeartbeat { .. }
                    | TickerPlantCommand::Abort
            )
        {
            debug!("ticker_plant: dropping a command queued after close was requested");

            return;
        }

        match command {
            TickerPlantCommand::Close => {
                self.core.handle_close().await;
            }
            TickerPlantCommand::GetSystemInfo { response_sender } => {
                self.core.handle_get_system_info(response_sender).await;
            }
            TickerPlantCommand::Login {
                config,
                response_sender,
            } => {
                self.core
                    .handle_login(config, SysInfraType::TickerPlant, response_sender)
                    .await;
            }
            TickerPlantCommand::SetLogin => {
                self.core.handle_set_login();
            }
            TickerPlantCommand::Logout { response_sender } => {
                self.core.handle_logout(response_sender).await;
            }
            TickerPlantCommand::UpdateHeartbeat { seconds } => {
                self.core.handle_update_heartbeat(seconds);
            }
            TickerPlantCommand::Subscribe {
                symbol,
                exchange,
                fields,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.core.rithmic_sender_api.request_market_data_update(
                    &symbol,
                    &exchange,
                    fields,
                    request_type,
                );

                self.core
                    .register_and_send(sub_buf, id, response_sender)
                    .await;
            }
            TickerPlantCommand::SubscribeOrderBook {
                symbol,
                exchange,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.core.rithmic_sender_api.request_depth_by_order_updates(
                    &symbol,
                    &exchange,
                    request_type,
                );

                self.core
                    .register_and_send(sub_buf, id, response_sender)
                    .await;
            }
            TickerPlantCommand::RequestDepthByOrderSnapshot {
                symbol,
                exchange,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_depth_by_order_snapshot(&symbol, &exchange);

                self.core
                    .register_and_send(snapshot_buf, id, response_sender)
                    .await;
            }
            TickerPlantCommand::SearchSymbols {
                search_text,
                exchange,
                product_code,
                instrument_type,
                pattern,
                response_sender,
            } => {
                let (search_buf, id) = self.core.rithmic_sender_api.request_search_symbols(
                    &search_text,
                    exchange.as_deref(),
                    product_code.as_deref(),
                    instrument_type,
                    pattern,
                );

                self.core
                    .register_and_send(search_buf, id, response_sender)
                    .await;
            }
            TickerPlantCommand::ListExchangePermissions {
                user,
                response_sender,
            } => {
                let (list_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_list_exchange_permissions(&user);

                self.core
                    .register_and_send(list_buf, id, response_sender)
                    .await;
            }
            TickerPlantCommand::GetInstrumentByUnderlying {
                underlying_symbol,
                exchange,
                expiration_date,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_get_instrument_by_underlying(
                        &underlying_symbol,
                        &exchange,
                        expiration_date.as_deref(),
                    );

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::SubscribeByUnderlying {
                underlying_symbol,
                exchange,
                expiration_date,
                fields,
                request_type,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_market_data_update_by_underlying(
                        &underlying_symbol,
                        &exchange,
                        expiration_date.as_deref(),
                        fields,
                        request_type,
                    );

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetTickSizeTypeTable {
                tick_size_type,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_give_tick_size_type_table(&tick_size_type);

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetProductCodes {
                exchange,
                give_toi_products_only,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_product_codes(exchange.as_deref(), give_toi_products_only);

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetVolumeAtPrice {
                symbol,
                exchange,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_get_volume_at_price(&symbol, &exchange);

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetAuxilliaryReferenceData {
                symbol,
                exchange,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_auxilliary_reference_data(&symbol, &exchange);

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetReferenceData {
                symbol,
                exchange,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_reference_data(&symbol, &exchange);

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetFrontMonthContract {
                symbol,
                exchange,
                need_updates,
                response_sender,
            } => {
                let (buf, id) = self.core.rithmic_sender_api.request_front_month_contract(
                    &symbol,
                    &exchange,
                    need_updates,
                );

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::GetSystemGatewayInfo {
                system_name,
                response_sender,
            } => {
                let (buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_rithmic_system_gateway_info(system_name.as_deref());

                self.core.register_and_send(buf, id, response_sender).await;
            }
            TickerPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
    }
}

/// Handle for sending commands to a [`RithmicTickerPlant`] and receiving market data updates.
///
/// Obtained from [`RithmicTickerPlant::get_handle`]. Use the methods on this handle to
/// log in, subscribe to symbols, and request reference data. Real-time updates arrive
/// on [`subscription_receiver`](Self::subscription_receiver).
pub struct RithmicTickerPlantHandle {
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,

    /// Receiver for real-time subscription updates (market data, depth, etc.).
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
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
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
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic ticker plant with custom configuration
    ///
    /// This must be called before subscribing to any market data.
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

        let _ = self.sender.send(TickerPlantCommand::SetLogin).await;

        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(hb) = resp.heartbeat_interval {
                let secs = hb as u64;
                self.update_heartbeat(secs).await;
            }

            if let Some(session_id) = &resp.unique_user_id {
                info!("ticker_plant: session id: {}", session_id);
            }
        }

        info!("ticker_plant: logged in");

        Ok(response)
    }

    /// Disconnect from the Rithmic ticker plant
    ///
    /// # Returns
    /// The logout response or an error message
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
    /// Use when the connection is known to be dead and a graceful `disconnect()`
    /// would not get through.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(TickerPlantCommand::Abort);
    }

    /// Subscribe to market data for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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

    /// Unsubscribe from market data for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
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

    /// Request a snapshot of the order book depth-by-order for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A vector of responses containing the order book snapshot data or an error message
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

    async fn update_heartbeat(&self, seconds: u64) {
        let command = TickerPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Search for symbols based on search criteria
    ///
    /// # Arguments
    /// * `search_text` - The text to search for in symbols
    /// * `exchange` - Optional exchange filter
    /// * `product_code` - Optional product code filter
    /// * `instrument_type` - Optional instrument type filter
    /// * `pattern` - Optional search pattern mode
    ///
    /// # Returns
    /// A vector of responses containing matching symbols or an error message
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

    /// List exchange permissions for the specified user
    ///
    /// # Arguments
    /// * `user` - The username to query exchange permissions for
    ///
    /// # Returns
    /// A vector of responses containing exchange information or an error message
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

    /// Get instruments by underlying symbol
    ///
    /// # Arguments
    /// * `underlying_symbol` - The underlying symbol (e.g., "ES" for E-mini S&P 500)
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `expiration_date` - Optional expiration date filter
    ///
    /// # Returns
    /// A vector of responses containing instrument information or an error message
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

    /// Subscribe to market data for all instruments of an underlying
    ///
    /// # Arguments
    /// * `underlying_symbol` - The underlying symbol (e.g., "ES")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `expiration_date` - Optional expiration date filter
    /// * `fields` - Market data fields to subscribe to
    /// * `request_type` - Subscribe or Unsubscribe
    ///
    /// # Returns
    /// The subscription response or an error message
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

    /// Get tick size type table
    ///
    /// # Arguments
    /// * `tick_size_type` - The tick size type identifier
    ///
    /// # Returns
    /// The tick size table response or an error message
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

    /// Get product codes
    ///
    /// # Arguments
    /// * `exchange` - Optional exchange filter
    /// * `give_toi_products_only` - If true, only return Time of Interest products
    ///
    /// # Returns
    /// A vector of responses containing product codes or an error message
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

    /// Get volume at price data
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The volume at price response or an error message
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

    /// Get auxiliary reference data for a symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The auxiliary reference data response or an error message
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
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The reference data response or an error message
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

    /// Get front month contract
    ///
    /// Returns the current front month contract for a given product.
    ///
    /// # Arguments
    /// * `symbol` - The product symbol (e.g., "ES" for E-mini S&P 500)
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `need_updates` - Whether to receive updates when front month changes
    ///
    /// # Returns
    /// The front month contract response or an error message
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

    /// Get Rithmic system gateway info
    ///
    /// # Arguments
    /// * `system_name` - Optional system name to get info for
    ///
    /// # Returns
    /// The gateway info response or an error message
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
