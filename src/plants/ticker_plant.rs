use tracing::{error, info, warn};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        rithmic_command_types::LoginConfig,
        sender_api::RithmicSenderApi,
    },
    config::RithmicConfig,
    error::RithmicError,
    ping_manager::PingManager,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{
        messages::RithmicMessage,
        request_depth_by_order_updates,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
        request_market_data_update_by_underlying, request_search_symbols,
    },
    ws::{
        HEARTBEAT_SECS, PING_TIMEOUT_SECS, PlantActor, connect_with_strategy,
        get_heartbeat_interval, get_ping_interval,
    },
};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    time::{Interval, sleep_until},
};

pub(crate) enum TickerPlantCommand {
    Close,
    Abort,
    ListSystemInfo {
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
    SendHeartbeat,
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
    ListExchanges {
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
/// Currently the following market data updates are supported:
/// - Last trades
/// - Best bid and offer (BBO)
/// - Order book depth-by-order updates
///
/// # Connection Health Monitoring
///
/// The subscription receiver provides connection health events:
/// - **WebSocket ping/pong timeouts**: Primary indicator of dead connections (auto-detected)
/// - **Heartbeat errors**: Only forwarded when Rithmic server returns an error (rare)
/// - **Forced logout events**: Server-initiated disconnections requiring reconnection
/// - **Market data updates**: Real-time trade and quote data
///
/// **Note:** Heartbeat requests are sent automatically to comply with Rithmic's protocol,
/// but successful responses are silently dropped. Only heartbeat errors from the server
/// are forwarded as `HeartbeatTimeout` messages. The primary keep-alive mechanism is
/// WebSocket ping/pong, which reliably detects dead connections 24/7.
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
///                 if let Some(error) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, error);
///
///                     // Ping timeout or heartbeat error - connection may be dead
///                     if matches!(update.message, RithmicMessage::HeartbeatTimeout) {
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
    /// * `strategy` - Connection strategy (Simple or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicTickerPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Unable to establish WebSocket connection to the server
    /// - Network timeout occurs
    /// - Server rejects the connection
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
    config: RithmicConfig,
    interval: Interval,
    logged_in: bool,
    ping_interval: Interval,
    ping_manager: PingManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<TickerPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl TickerPlant {
    async fn new(
        request_receiver: mpsc::Receiver<TickerPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<TickerPlant, RithmicError> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy)
            .await
            .map_err(|e| RithmicError::ConnectionFailed(e.to_string()))?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "ticker_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(TickerPlant {
            config: config.clone(),
            interval,
            ping_interval,
            logged_in: false,
            ping_manager,
            request_handler: RithmicRequestHandler::new(),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender_api,
            rithmic_sender,
            subscription_sender,
        })
    }
}

impl TickerPlant {
    async fn send_or_fail(&mut self, msg: Message, request_id: &str) {
        if self.rithmic_sender.send(msg).await.is_err() {
            error!(
                "ticker_plant: WebSocket send failed for request {}",
                request_id
            );

            self.request_handler
                .fail_request(request_id, RithmicError::SendFailed);
        }
    }
}

impl PlantActor for TickerPlant {
    type Command = TickerPlantCommand;

    /// Execute the ticker plant in its own thread
    /// We will listen for messages from request_receiver and forward them to Rithmic
    /// while also listening for messages from Rithmic and forwarding them to subscription_sender
    /// or request handler
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(TickerPlantCommand::SendHeartbeat).await;
                    }
                }
                _ = self.ping_interval.tick() => {
                    self.ping_manager.sent();
                    let _ = self.rithmic_sender.send(Message::Ping(vec![].into())).await;
                }
                _ = async {
                    if let Some(timeout_at) = self.ping_manager.next_timeout_at() {
                        sleep_until(timeout_at).await
                    } else {
                        std::future::pending::<()>().await
                    }
                } => {
                    if self.ping_manager.check_timeout() {
                        error!("WebSocket ping timed out - connection appears dead");

                        let error_response = RithmicResponse {
                            request_id: "websocket_ping_timeout".to_string(),
                            message: RithmicMessage::HeartbeatTimeout,
                            is_update: true,
                            has_more: false,
                            multi_response: false,
                            error: Some("WebSocket ping timeout - connection dead".to_string()),
                            source: self.rithmic_receiver_api.source.clone(),
                        };
                        let _ = self.subscription_sender.send(error_response);
                        break;
                    }
                }
                Some(message) = self.request_receiver.recv() => {
                    if matches!(message, TickerPlantCommand::Abort) {
                        info!("ticker_plant: abort requested, shutting down immediately");

                        let error_response = RithmicResponse {
                            request_id: "".to_string(),
                            message: RithmicMessage::ConnectionError,
                            is_update: true,
                            has_more: false,
                            multi_response: false,
                            error: Some("Plant aborted".to_string()),
                            source: self.rithmic_receiver_api.source.clone(),
                        };

                        let _ = self.subscription_sender.send(error_response);

                        self.request_handler.drain_and_drop();

                        break;
                    }
                    self.handle_command(message).await;
                }
                Some(message) = self.rithmic_reader.next() => {
                    let stop = self.handle_rithmic_message(message).await;

                    if stop {
                        break;
                    }
                }
                else => { break }
            }
        }
    }

    async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> bool {
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                info!("ticker_plant received close frame: {:?}", frame);

                stop = true;
            }
            Ok(Message::Pong(_)) => {
                self.ping_manager.received();
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    // Handle heartbeat responses: only forward if they contain an error
                    if matches!(response.message, RithmicMessage::ResponseHeartbeat(_)) {
                        if let Some(error) = response.error {
                            let error_response = RithmicResponse {
                                request_id: response.request_id,
                                message: RithmicMessage::HeartbeatTimeout,
                                is_update: true,
                                has_more: false,
                                multi_response: false,
                                error: Some(error),
                                source: self.rithmic_receiver_api.source.clone(),
                            };

                            let _ = self.subscription_sender.send(error_response);
                        }

                        // Always drop heartbeat responses (successful or error)
                        return false;
                    }

                    if response.is_update {
                        match self.subscription_sender.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("ticker_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!(
                        "ticker_plant: error response from server: {:?}",
                        err_response
                    );

                    if err_response.is_update {
                        let _ = self.subscription_sender.send(err_response);
                    } else {
                        self.request_handler.handle_response(err_response);
                    }
                }
            },
            Err(Error::ConnectionClosed) => {
                error!("ticker_plant: connection closed");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket connection closed".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::AlreadyClosed) => {
                error!("ticker_plant: connection already closed");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket connection already closed".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Io(ref io_err)) => {
                error!("ticker_plant: I/O error: {}", io_err);

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some(format!("WebSocket I/O error: {}", io_err)),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                error!("ticker_plant: connection reset without closing handshake");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket connection reset without closing handshake".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {
                error!("ticker_plant: attempted to send after closing");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket attempted to send after closing".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ReceivedAfterClosing)) => {
                error!("ticker_plant: received data after closing");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket received data after closing".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            _ => {
                warn!("ticker_plant received unknown message {:?}", message);
            }
        }

        stop
    }

    async fn handle_command(&mut self, command: TickerPlantCommand) {
        match command {
            TickerPlantCommand::Close => {
                let _ = self.rithmic_sender.send(Message::Close(None)).await;
            }
            TickerPlantCommand::ListSystemInfo { response_sender } => {
                let (list_system_info_buf, id) =
                    self.rithmic_sender_api.request_rithmic_system_info();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(list_system_info_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::Login {
                config,
                response_sender,
            } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::TickerPlant,
                    &self.config.user,
                    &self.config.password,
                    &config,
                );

                info!("ticker_plant: sending login request {}", id);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(login_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            TickerPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(logout_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::SendHeartbeat => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf.into()))
                    .await;
            }
            TickerPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            TickerPlantCommand::Subscribe {
                symbol,
                exchange,
                fields,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_market_data_update(
                    &symbol,
                    &exchange,
                    fields,
                    request_type,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(sub_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::SubscribeOrderBook {
                symbol,
                exchange,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_depth_by_order_update(
                    &symbol,
                    &exchange,
                    request_type,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(sub_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::RequestDepthByOrderSnapshot {
                symbol,
                exchange,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .rithmic_sender_api
                    .request_depth_by_order_snapshot(&symbol, &exchange);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(snapshot_buf.into()), &request_id)
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
                let (search_buf, id) = self.rithmic_sender_api.request_search_symbols(
                    &search_text,
                    exchange.as_deref(),
                    product_code.as_deref(),
                    instrument_type,
                    pattern,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(search_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::ListExchanges {
                user,
                response_sender,
            } => {
                let (list_buf, id) = self.rithmic_sender_api.request_list_exchanges(&user);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(list_buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetInstrumentByUnderlying {
                underlying_symbol,
                exchange,
                expiration_date,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_get_instrument_by_underlying(
                        &underlying_symbol,
                        &exchange,
                        expiration_date.as_deref(),
                    );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
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
                    .rithmic_sender_api
                    .request_market_data_update_by_underlying(
                        &underlying_symbol,
                        &exchange,
                        expiration_date.as_deref(),
                        fields,
                        request_type,
                    );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetTickSizeTypeTable {
                tick_size_type,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_give_tick_size_type_table(&tick_size_type);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetProductCodes {
                exchange,
                give_toi_products_only,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_product_codes(exchange.as_deref(), give_toi_products_only);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetVolumeAtPrice {
                symbol,
                exchange,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_get_volume_at_price(&symbol, &exchange);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetAuxilliaryReferenceData {
                symbol,
                exchange,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_auxilliary_reference_data(&symbol, &exchange);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetReferenceData {
                symbol,
                exchange,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_reference_data(&symbol, &exchange);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetFrontMonthContract {
                symbol,
                exchange,
                need_updates,
                response_sender,
            } => {
                let (buf, id) = self.rithmic_sender_api.request_front_month_contract(
                    &symbol,
                    &exchange,
                    need_updates,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::GetSystemGatewayInfo {
                system_name,
                response_sender,
            } => {
                let (buf, id) = self
                    .rithmic_sender_api
                    .request_rithmic_system_gateway_info(system_name.as_deref());

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            TickerPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
    }
}

/// Handle for sending commands to a [`RithmicTickerPlant`] and receiving market data updates.
///
/// Obtained from [`RithmicTickerPlant::connect()`]. Use the methods on this handle to
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
    pub async fn list_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx
            .await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;

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
        let response = rx
            .await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;

        if let Some(err) = response.error {
            error!("ticker_plant: login failed {:?}", err);
            Err(RithmicError::ServerError(err))
        } else {
            let _ = self.sender.send(TickerPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("ticker_plant: session id: {}", session_id);
                }
            }

            info!("ticker_plant: logged in");

            Ok(response)
        }
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
        let r = rx.await.map_err(|_| RithmicError::ConnectionClosed)??;
        let _ = self.sender.send(TickerPlantCommand::Close).await;
        let response = r.into_iter().next().ok_or(RithmicError::EmptyResponse)?;

        let _ = self.subscription_sender.send(response.clone());

        Ok(response)
    }

    /// Immediately shut down the ticker plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and `disconnect()` would hang.
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
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::Subscribe {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            fields: vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            request_type: Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Subscribe to order book depth-by-order updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_book(
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::Subscribe {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            fields: vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            request_type: Request::Unsubscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Unsubscribe from order book depth-by-order updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The unsubscription response or an error message
    pub async fn unsubscribe_order_book(
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Subscribe to instrument status updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// [`subscribe_order_book`](Self::subscribe_order_book), which uses
    /// `request_depth_by_order_updates` (proto 104) for full depth-by-order streaming.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// Use [`unsubscribe_order_book`](Self::unsubscribe_order_book) to stop the
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
    /// Subscribes to `Close`, `Settlement`, `ProjectedSettlement`, and `AdjustedClose`.
    /// The first three are delivered as real-time updates throughout the session.
    /// `AdjustedClose` is a reference field and will not fire as a real-time callback;
    /// it is included for end-of-session reconciliation use cases where its value is
    /// populated after market close.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
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
    /// This reverses [`subscribe_end_of_day_prices`](Self::subscribe_end_of_day_prices),
    /// including the non-streaming `AdjustedClose` reference field.
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
    pub async fn request_depth_by_order_snapshot(
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// List exchanges available to the specified user
    ///
    /// # Arguments
    /// * `user` - The username to query exchange permissions for
    ///
    /// # Returns
    /// A vector of responses containing exchange information or an error message
    pub async fn list_exchanges(&self, user: &str) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = TickerPlantCommand::ListExchanges {
            user: user.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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
