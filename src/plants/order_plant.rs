use std::{sync::Arc, time::Duration};

use tracing::{error, info, warn};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        rithmic_command_types::{
            LoginConfig, RithmicAdvancedBracketOrder, RithmicBracketOrder, RithmicCancelOrder,
            RithmicModifyOrder, RithmicOcoOrderLeg, RithmicOrder,
        },
        sender_api::RithmicSenderApi,
    },
    config::{RithmicAccount, RithmicConfig},
    error::RithmicError,
    ping_manager::PingManager,
    plants::subscription::SubscriptionFilter,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{messages::RithmicMessage, request_easy_to_borrow_list, request_login::SysInfraType},
    ws::{
        HEARTBEAT_SECS, PING_TIMEOUT_SECS, PlantActor, SEND_TIMEOUT_SECS, WebSocketSendError,
        connect_with_strategy, get_heartbeat_interval, get_ping_interval, send_with_timeout,
    },
};

use futures_util::{
    StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::{Interval, sleep_until},
};

pub(crate) enum OrderPlantCommand {
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
    UpdateHeartbeat {
        seconds: u64,
    },
    AccountList {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeOrderUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeBracketUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceBracketOrder {
        bracket_order: RithmicBracketOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceAdvancedBracketOrder {
        bracket_order: RithmicAdvancedBracketOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyOrder {
        order: RithmicModifyOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyStop {
        order_id: String,
        ticks: i32,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyProfit {
        order_id: String,
        ticks: i32,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    CancelOrder {
        order_id: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrders {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    CancelAllOrders {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetAccountRmsInfo {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetProductRmsInfo {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetTradeRoutes {
        subscribe_for_updates: bool,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistoryDates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistorySummary {
        date: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistoryDetail {
        basket_id: String,
        date: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistory {
        basket_id: Option<String>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceOrder {
        order: RithmicOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceOcoOrder {
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowBrackets {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowBracketStops {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ExitPosition {
        symbol: String,
        exchange: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    LinkOrders {
        basket_ids: Vec<String>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetEasyToBorrowList {
        request_type: request_easy_to_borrow_list::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyOrderReferenceData {
        basket_id: String,
        user_tag: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetOrderSessionConfig {
        should_defer_request: Option<bool>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ReplayExecutions {
        start_index_sec: i32,
        finish_index_sec: i32,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeAccountRmsUpdates {
        subscribe: bool,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetLoginInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    // Agreement-related commands
    ListUnacceptedAgreements {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ListAcceptedAgreements {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    AcceptAgreement {
        agreement_id: String,
        market_data_usage_capacity: Option<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowAgreement {
        agreement_id: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SetRithmicMrktDataSelfCertStatus {
        agreement_id: String,
        market_data_usage_capacity: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ListExchangePermissions {
        user: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
}

/// The RithmicOrderPlant provides functionality to manage trading orders through the Rithmic API.
///
/// It allows applications to:
/// - Place, modify and cancel orders
/// - Work with bracket orders (entry orders with profit targets and stop losses)
/// - Receive real-time order status updates
/// - Track positions and execution reports
///
/// # Connection Health Monitoring
///
/// The subscription receiver provides connection health events:
/// - **WebSocket ping/pong timeouts**: Primary indicator of dead connections (auto-detected)
/// - **Heartbeat errors**: Only forwarded when Rithmic server returns an error (rare)
/// - **Forced logout events**: Server-initiated disconnections requiring reconnection
/// - **Order notifications**: Real-time order fills, cancellations, and status changes
///
/// **Note:** Heartbeat requests are sent automatically for protocol compliance,
/// but successful responses are silently dropped. Only heartbeat errors from the server
/// are forwarded as `HeartbeatTimeout` messages.
///
/// # Example: Basic Usage
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicAccount, RithmicConfig, RithmicEnv, ConnectStrategy, RithmicOrderPlant,
///     RithmicBracketOrder, BracketTransactionType, BracketDuration, BracketPriceType,
///     rti::messages::RithmicMessage,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///     let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
///
///     let order_plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
///     let mut handle = order_plant.get_handle(&account);
///
///     handle.login().await?;
///     handle.subscribe_order_updates().await?;
///     handle.subscribe_bracket_updates().await?;
///
///     // Place a bracket order
///     let bracket_order = RithmicBracketOrder {
///         action: BracketTransactionType::Buy,
///         duration: BracketDuration::Day,
///         exchange: "CME".to_string(),
///         localid: "order1".to_string(),
///         price_type: BracketPriceType::Limit,
///         price: Some(4500.00),
///         profit_ticks: 8,
///         quantity: 1,
///         stop_ticks: 4,
///         symbol: "ESH6".to_string(),
///     };
///
///     handle.place_bracket_order(bracket_order).await?;
///
///     // Monitor order updates with error handling
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 // Check for errors on all messages
///                 if let Some(error) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, error);
///                 }
///
///                 // Handle connection health issues
///                 match update.message {
///                     RithmicMessage::HeartbeatTimeout => {
///                         eprintln!("Connection timeout - reconnection needed");
///                         break;
///                     }
///                     RithmicMessage::ForcedLogout(_) => {
///                         eprintln!("Forced logout - reconnection needed");
///                         break;
///                     }
///                     RithmicMessage::ConnectionError => {
///                         eprintln!("Connection error - reconnection needed");
///                         break;
///                     }
///                     RithmicMessage::RithmicOrderNotification(order) => {
///                         println!("Order notification: {:?}", order);
///                     }
///                     RithmicMessage::ExchangeOrderNotification(order) => {
///                         println!("Exchange notification: {:?}", order);
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
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct RithmicOrderPlant {
    pub(crate) connection_handle: JoinHandle<()>,
    sender: mpsc::Sender<OrderPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicOrderPlant {
    /// Create a new Order Plant connection
    ///
    /// Create a new Order Plant connection to manage trading orders.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicOrderPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if unable to establish WebSocket connection to the server.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicOrderPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<OrderPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut order_plant = OrderPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            order_plant.run().await;
        });

        Ok(RithmicOrderPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicOrderPlant {
    /// Wait for the plant's background connection task to finish.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to interact with the order plant.
    ///
    /// The handle provides methods to place orders, subscribe to updates, and manage positions.
    /// Multiple handles can be created from the same plant for different accounts.
    pub fn get_handle(&self, account: &RithmicAccount) -> RithmicOrderPlantHandle {
        let account = Arc::new(account.clone());

        RithmicOrderPlantHandle {
            account: Arc::clone(&account),
            sender: self.sender.clone(),
            subscription_receiver: SubscriptionFilter::new(
                Arc::clone(&account),
                self.subscription_sender.subscribe(),
            ),
        }
    }
}

struct OrderPlant {
    config: RithmicConfig,
    // Distinguishes an intentional local shutdown from an unexpected peer close.
    close_requested: bool,
    interval: Interval,
    logged_in: bool,
    ping_interval: Interval,
    ping_manager: PingManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<OrderPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl OrderPlant {
    pub async fn new(
        request_receiver: mpsc::Receiver<OrderPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<OrderPlant, RithmicError> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy)
            .await
            .map_err(|e| RithmicError::ConnectionFailed(e.to_string()))?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "order_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(OrderPlant {
            config: config.clone(),
            close_requested: false,
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

    fn emit_connection_health_event(
        &self,
        request_id: &str,
        message: RithmicMessage,
        error_message: impl Into<String>,
    ) {
        let error_response = RithmicResponse {
            request_id: request_id.to_string(),
            message,
            is_update: true,
            has_more: false,
            multi_response: false,
            error: Some(error_message.into()),
            source: self.rithmic_receiver_api.source.clone(),
        };

        let _ = self.subscription_sender.send(error_response);
    }

    fn fail_connection_and_drain(
        &mut self,
        request_id: &str,
        message: RithmicMessage,
        error_message: impl Into<String>,
    ) {
        self.emit_connection_health_event(request_id, message, error_message);
        self.request_handler.drain_and_drop();
    }

    async fn send_or_fail(&mut self, msg: Message, request_id: &str) {
        match send_with_timeout(
            &mut self.rithmic_sender,
            msg,
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => {}
            Err(WebSocketSendError::Transport(error)) => {
                error!(
                    "order_plant: WebSocket send failed for request {}: {}",
                    request_id, error
                );
                self.request_handler
                    .fail_request(request_id, RithmicError::SendFailed);
            }
            Err(WebSocketSendError::Timeout) => {
                error!(
                    "order_plant: WebSocket send timed out for request {}",
                    request_id
                );
                self.request_handler
                    .fail_request(request_id, RithmicError::SendFailed);
            }
        }
    }

    async fn send_ping(&mut self) -> bool {
        self.ping_manager.sent();

        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Ping(vec![].into()),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => false,
            Err(WebSocketSendError::Transport(error)) => {
                error!("order_plant: WebSocket ping send failed: {}", error);
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    format!("WebSocket ping send failed: {error}"),
                );
                true
            }
            Err(WebSocketSendError::Timeout) => {
                error!("order_plant: WebSocket ping send timed out");
                self.fail_connection_and_drain(
                    "websocket_ping_timeout",
                    RithmicMessage::HeartbeatTimeout,
                    "WebSocket ping send timed out - connection dead",
                );
                true
            }
        }
    }

    async fn send_heartbeat(&mut self) -> bool {
        let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Binary(heartbeat_buf.into()),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => false,
            Err(WebSocketSendError::Transport(error)) => {
                error!("order_plant: heartbeat send failed: {}", error);
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    format!("Heartbeat send failed: {error}"),
                );
                true
            }
            Err(WebSocketSendError::Timeout) => {
                error!("order_plant: heartbeat send timed out");
                self.fail_connection_and_drain(
                    "heartbeat_send_timeout",
                    RithmicMessage::HeartbeatTimeout,
                    "Heartbeat send timed out - connection dead",
                );
                true
            }
        }
    }

    async fn send_close_best_effort(&mut self) {
        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Close(None),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => {}
            Err(WebSocketSendError::Transport(error)) => {
                warn!("order_plant: close send failed: {}", error);
            }
            Err(WebSocketSendError::Timeout) => {
                warn!("order_plant: close send timed out");
            }
        }
    }
}

impl PlantActor for OrderPlant {
    type Command = OrderPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in && !self.close_requested && self.send_heartbeat().await {
                        break;
                    }
                }
                _ = self.ping_interval.tick() => {
                    if !self.close_requested && self.send_ping().await {
                        break;
                    }
                }
                _ = async {
                    if let Some(timeout_at) = self.ping_manager.next_timeout_at() {
                        sleep_until(timeout_at).await
                    } else {
                        std::future::pending::<()>().await
                    }
                } => {
                    if self.ping_manager.check_timeout() {
                        if self.close_requested {
                            self.request_handler.drain_and_drop();
                        } else {
                            error!("WebSocket ping timed out - connection appears dead");
                            self.fail_connection_and_drain(
                                "websocket_ping_timeout",
                                RithmicMessage::HeartbeatTimeout,
                                "WebSocket ping timeout - connection dead",
                            );
                        }
                        break;
                    }
                }
                Some(message) = self.request_receiver.recv() => {
                    if matches!(message, OrderPlantCommand::Abort) {
                        info!("order_plant: abort requested, shutting down immediately");
                        self.fail_connection_and_drain(
                            "",
                            RithmicMessage::ConnectionError,
                            "Plant aborted",
                        );
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
                else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> bool {
        let mut stop: bool = false;

        match message {
            Ok(Message::Close(frame)) => {
                info!("order_plant: Received close frame: {:?}", frame);
                if self.close_requested {
                    self.request_handler.drain_and_drop();
                } else {
                    self.fail_connection_and_drain(
                        "",
                        RithmicMessage::ConnectionError,
                        format!("WebSocket close frame received: {:?}", frame),
                    );
                }
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
                                warn!("order_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!(
                        "order_plant: error response from server: {:?}",
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
                error!("order_plant: connection closed");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket connection closed",
                );
                stop = true;
            }
            Err(Error::AlreadyClosed) => {
                error!("order_plant: connection already closed");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket connection already closed",
                );
                stop = true;
            }
            Err(Error::Io(ref io_err)) => {
                error!("order_plant: I/O error: {}", io_err);
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    format!("WebSocket I/O error: {}", io_err),
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                error!("order_plant: connection reset without closing handshake");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket connection reset without closing handshake",
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {
                error!("order_plant: attempted to send after closing");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket attempted to send after closing",
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ReceivedAfterClosing)) => {
                error!("order_plant: received data after closing");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket received data after closing",
                );
                stop = true;
            }
            _ => {
                warn!("order_plant: Unhandled message: {:?}", message);
            }
        }

        stop
    }

    async fn handle_command(&mut self, command: OrderPlantCommand) {
        match command {
            OrderPlantCommand::Close => {
                self.close_requested = true;
                self.send_close_best_effort().await;
            }
            OrderPlantCommand::ListSystemInfo { response_sender } => {
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
            OrderPlantCommand::Login {
                config,
                response_sender,
            } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::OrderPlant,
                    &self.config.user,
                    &self.config.password,
                    &config,
                );

                info!("order_plant: sending login request {}", id);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(login_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            OrderPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(logout_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            OrderPlantCommand::AccountList { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_account_list();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::SubscribeOrderUpdates {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_for_order_updates(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::SubscribeBracketUpdates {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_to_bracket_updates(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_bracket_order(bracket_order, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::PlaceAdvancedBracketOrder {
                bracket_order,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_advanced_bracket_order(bracket_order, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ModifyOrder {
                order,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_modify_order(
                    &order.id,
                    &order.exchange,
                    &order.symbol,
                    order.qty,
                    order.price,
                    order.price_type,
                    &account,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::CancelOrder {
                order_id,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_cancel_order(&order_id, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ModifyStop {
                order_id,
                ticks,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_stop_bracket_level(&order_id, ticks, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ModifyProfit {
                order_id,
                ticks,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_target_bracket_level(&order_id, ticks, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowOrders {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_orders(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::CancelAllOrders {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_cancel_all_orders(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::GetAccountRmsInfo {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_account_rms_info(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::GetProductRmsInfo {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_product_rms_info(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::GetTradeRoutes {
                subscribe_for_updates,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_trade_routes(subscribe_for_updates);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistoryDates { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_order_history_dates();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistorySummary {
                date,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_order_history_summary(&date, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistoryDetail {
                basket_id,
                date,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_order_history_detail(&basket_id, &date, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistory {
                basket_id,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_order_history(basket_id.as_deref(), &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::PlaceOrder {
                order,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_order(&order, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::PlaceOcoOrder {
                order1,
                order2,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_oco_order(order1, order2, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowBrackets {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_brackets(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowBracketStops {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_bracket_stops(&account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ExitPosition {
                symbol,
                exchange,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_exit_position(&symbol, &exchange, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::LinkOrders {
                basket_ids,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_link_orders(basket_ids, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::GetEasyToBorrowList {
                request_type,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_easy_to_borrow_list(request_type);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ModifyOrderReferenceData {
                basket_id,
                user_tag,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_modify_order_reference_data(&basket_id, &user_tag, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::GetOrderSessionConfig {
                should_defer_request,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_order_session_config(should_defer_request);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ReplayExecutions {
                start_index_sec,
                finish_index_sec,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_replay_executions(
                    start_index_sec,
                    finish_index_sec,
                    &account,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::SubscribeAccountRmsUpdates {
                subscribe,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_account_rms_updates(subscribe, &account);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::GetLoginInfo { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_login_info();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ListUnacceptedAgreements { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_list_unaccepted_agreements();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ListAcceptedAgreements { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_list_accepted_agreements();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::AcceptAgreement {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_accept_agreement(&agreement_id, market_data_usage_capacity.as_deref());

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ShowAgreement {
                agreement_id,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_agreement(&agreement_id);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_set_rithmic_mrkt_data_self_cert_status(
                        &agreement_id,
                        &market_data_usage_capacity,
                    );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::ListExchangePermissions {
                user,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_list_exchanges(&user);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(req_buf.into()), &request_id)
                    .await;
            }
            OrderPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        };
    }
}

/// Handle for sending commands to a [`RithmicOrderPlant`] and receiving order updates.
///
/// Obtained from [`RithmicOrderPlant::connect()`]. Use the methods on this handle to
/// log in, place/modify/cancel orders, and query account information. Real-time order
/// updates arrive on [`subscription_receiver`](Self::subscription_receiver).
pub struct RithmicOrderPlantHandle {
    account: Arc<RithmicAccount>,
    sender: mpsc::Sender<OrderPlantCommand>,
    /// Receiver for real-time order updates and responses.
    pub subscription_receiver: SubscriptionFilter,
}

impl std::fmt::Debug for RithmicOrderPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicOrderPlantHandle")
            .field("account", &self.account)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

impl RithmicOrderPlantHandle {
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
    pub async fn list_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Log in to the Rithmic Order plant
    ///
    /// This must be called before sending orders or subscriptions
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic Order plant with custom configuration
    ///
    /// This must be called before sending orders or subscriptions.
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
        info!("order_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let mut config = config;
        config.aggregated_quotes = None;

        let command = OrderPlantCommand::Login {
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
            error!("order_plant: login failed {:?}", err);
            Err(RithmicError::ServerError(err))
        } else {
            let _ = self.sender.send(OrderPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("order_plant: session id: {}", session_id);
                }
            }

            info!("order_plant: logged in");

            Ok(response)
        }
    }

    /// Disconnect from the Rithmic Order plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let r = rx.await.map_err(|_| RithmicError::ConnectionClosed)??;
        let _ = self.sender.send(OrderPlantCommand::Close).await;

        r.into_iter().next().ok_or(RithmicError::EmptyResponse)
    }

    /// Immediately shut down the order plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and `disconnect()` would hang.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(OrderPlantCommand::Abort);
    }

    /// Get a list of available trading accounts
    ///
    /// # Returns
    /// A vector of account list responses or an error message
    pub async fn get_account_list(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::AccountList {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Subscribe to order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeOrderUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Subscribe to bracket order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_bracket_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeBracketUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Place a bracket order (entry order with profit target and stop loss)
    ///
    /// # Arguments
    /// * `bracket_order` - The bracket order parameters
    ///
    /// # Returns
    /// The order placement responses or an error message
    pub async fn place_bracket_order(
        &self,
        bracket_order: RithmicBracketOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceBracketOrder {
            bracket_order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place an advanced bracket order using the full raw `RequestBracketOrder`
    /// request surface currently modeled by this crate.
    ///
    /// # Arguments
    /// * `bracket_order` - The advanced bracket order parameters
    ///
    /// # Returns
    /// The order placement responses or an error message
    pub async fn place_advanced_bracket_order(
        &self,
        bracket_order: RithmicAdvancedBracketOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceAdvancedBracketOrder {
            bracket_order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Modify an existing order
    ///
    /// # Arguments
    /// * `order` - The order parameters to modify
    ///
    /// # Returns
    /// A vector of order modification responses or an error message
    pub async fn modify_order(
        &self,
        order: RithmicModifyOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyOrder {
            order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Cancel an order
    ///
    /// # Arguments
    /// * `order` - The cancel order parameters
    ///
    /// # Returns
    /// A vector of cancellation responses or an error message
    pub async fn cancel_order(
        &self,
        order: RithmicCancelOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::CancelOrder {
            order_id: order.id,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Adjust the profit target level of a bracket order
    ///
    /// # Arguments
    /// * `id` - The order ID
    /// * `ticks` - Number of ticks to adjust the profit target
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_profit(
        &self,
        id: &str,
        ticks: i32,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyProfit {
            order_id: id.to_string(),
            ticks,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Adjust the stop loss level of a bracket order
    ///
    /// # Arguments
    /// * `id` - The order ID
    /// * `ticks` - Number of ticks to adjust the stop loss
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_stop(&self, id: &str, ticks: i32) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyStop {
            order_id: id.to_string(),
            ticks,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Request a list of all open orders
    ///
    /// # Returns
    /// The order list response or an error message
    pub async fn show_orders(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrders {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = OrderPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Cancel all open orders
    ///
    /// # Returns
    /// The cancellation response or an error message
    pub async fn cancel_all_orders(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::CancelAllOrders {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get account RMS (Risk Management System) information
    ///
    /// # Returns
    /// A vector of RMS info responses or an error message
    pub async fn get_account_rms_info(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetAccountRmsInfo {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get product RMS (Risk Management System) information
    ///
    /// # Returns
    /// A vector of product RMS info responses or an error message
    pub async fn get_product_rms_info(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetProductRmsInfo {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get available trade routes
    ///
    /// # Arguments
    /// * `subscribe_for_updates` - Whether to receive updates when routes change
    ///
    /// # Returns
    /// The list of trade routes or an error message
    pub async fn get_trade_routes(
        &self,
        subscribe_for_updates: bool,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetTradeRoutes {
            subscribe_for_updates,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get dates for which order history is available
    ///
    /// # Returns
    /// The list of available dates or an error message
    pub async fn show_order_history_dates(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistoryDates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get order history summary for a specific date
    ///
    /// # Arguments
    /// * `date` - Date in YYYYMMDD format (e.g., "20250122")
    ///
    /// # Returns
    /// The list of order summaries or an error message
    pub async fn show_order_history_summary(
        &self,
        date: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistorySummary {
            date: date.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get detailed order history for a specific order
    ///
    /// # Arguments
    /// * `basket_id` - Order/basket identifier
    /// * `date` - Date in YYYYMMDD format
    ///
    /// # Returns
    /// The detailed order history response or an error message
    pub async fn show_order_history_detail(
        &self,
        basket_id: &str,
        date: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistoryDetail {
            basket_id: basket_id.to_string(),
            date: date.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get general order history
    ///
    /// # Arguments
    /// * `basket_id` - Optional order/basket identifier filter
    ///
    /// # Returns
    /// The list of order history entries or an error message
    pub async fn show_order_history(
        &self,
        basket_id: Option<&str>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistory {
            basket_id: basket_id.map(|s| s.to_string()),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place a new order using [`RithmicOrder`]
    ///
    /// This is the preferred method for placing standalone orders. It supports
    /// all order types including those with trigger prices (stop orders) and
    /// trailing stops.
    ///
    /// For orders with automatic profit targets and stop losses, use
    /// [`place_bracket_order`](Self::place_bracket_order) instead.
    ///
    /// # Arguments
    /// * `order` - The order parameters
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rithmic_rs::{RithmicOrder, NewOrderTransactionType, NewOrderPriceType};
    ///
    /// let order = RithmicOrder {
    ///     symbol: "ESH6".to_string(),
    ///     exchange: "CME".to_string(),
    ///     quantity: 1,
    ///     price: 5000.0,
    ///     transaction_type: NewOrderTransactionType::Buy,
    ///     price_type: NewOrderPriceType::Limit,
    ///     user_tag: "my-order".to_string(),
    ///     ..Default::default()
    /// };
    /// handle.place_order(order).await?;
    /// ```
    pub async fn place_order(
        &self,
        order: RithmicOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceOrder {
            order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place an OCO (One Cancels Other) order pair
    ///
    /// When one order is filled, the other is automatically cancelled.
    ///
    /// # Arguments
    /// * `order1` - First order leg
    /// * `order2` - Second order leg
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    pub async fn place_oco_order(
        &self,
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceOcoOrder {
            order1,
            order2,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Show all active bracket orders
    ///
    /// # Returns
    /// A vector of responses containing bracket order information or an error message
    pub async fn show_brackets(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowBrackets {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Show all active bracket stop orders
    ///
    /// # Returns
    /// A vector of responses containing bracket stop information or an error message
    pub async fn show_bracket_stops(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowBracketStops {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Exit an entire position for a given symbol
    ///
    /// This closes all open positions for the specified symbol/exchange.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A vector of exit position responses or an error message
    pub async fn exit_position(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ExitPosition {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Link multiple orders together
    ///
    /// When one linked order is cancelled, all linked orders are cancelled.
    ///
    /// # Arguments
    /// * `basket_ids` - Vector of basket IDs to link together
    ///
    /// # Returns
    /// The link orders response or an error message
    pub async fn link_orders(
        &self,
        basket_ids: Vec<String>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::LinkOrders {
            basket_ids,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get the easy-to-borrow list for short selling
    ///
    /// # Arguments
    /// * `request_type` - Subscribe or Unsubscribe from updates
    ///
    /// # Returns
    /// A vector of responses containing easy-to-borrow securities or an error message
    pub async fn get_easy_to_borrow_list(
        &self,
        request_type: request_easy_to_borrow_list::Request,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetEasyToBorrowList {
            request_type,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Modify order reference data (user tag)
    ///
    /// # Arguments
    /// * `basket_id` - The order/basket identifier
    /// * `user_tag` - New user tag to set on the order
    ///
    /// # Returns
    /// The modification response or an error message
    pub async fn modify_order_reference_data(
        &self,
        basket_id: &str,
        user_tag: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyOrderReferenceData {
            basket_id: basket_id.to_string(),
            user_tag: user_tag.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get or set order session configuration
    ///
    /// # Arguments
    /// * `should_defer_request` - If true, defers requests until server loads reference data
    ///
    /// # Returns
    /// The session config response or an error message
    pub async fn get_order_session_config(
        &self,
        should_defer_request: Option<bool>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetOrderSessionConfig {
            should_defer_request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Replay historical executions
    ///
    /// # Arguments
    /// * `start_index_sec` - Start time in unix seconds
    /// * `finish_index_sec` - End time in unix seconds
    ///
    /// # Returns
    /// A vector of execution responses or an error message
    pub async fn replay_executions(
        &self,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ReplayExecutions {
            start_index_sec,
            finish_index_sec,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Subscribe to account RMS updates
    ///
    /// # Arguments
    /// * `subscribe` - true to subscribe, false to unsubscribe
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_account_rms_updates(
        &self,
        subscribe: bool,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeAccountRmsUpdates {
            subscribe,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get login information for the current session
    ///
    /// # Returns
    /// The login info response or an error message
    pub async fn get_login_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetLoginInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// List unaccepted agreements
    ///
    /// Returns a list of market data agreements that have not yet been accepted.
    ///
    /// # Returns
    /// A vector of unaccepted agreement responses or an error message
    pub async fn list_unaccepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListUnacceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// List accepted agreements
    ///
    /// Returns a list of market data agreements that have been accepted.
    ///
    /// # Returns
    /// A vector of accepted agreement responses or an error message
    pub async fn list_accepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListAcceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Accept a market data agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to accept
    /// * `market_data_usage_capacity` - Optional capacity indicator (e.g., "Professional", "Non-Professional")
    ///
    /// # Returns
    /// The acceptance response or an error message
    pub async fn accept_agreement(
        &self,
        agreement_id: &str,
        market_data_usage_capacity: Option<&str>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::AcceptAgreement {
            agreement_id: agreement_id.to_string(),
            market_data_usage_capacity: market_data_usage_capacity.map(|s| s.to_string()),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Show details of an agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to display
    ///
    /// # Returns
    /// A vector of agreement details responses or an error message
    pub async fn show_agreement(
        &self,
        agreement_id: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowAgreement {
            agreement_id: agreement_id.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Set Rithmic market data self-certification status
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement
    /// * `market_data_usage_capacity` - The usage capacity (e.g., "Professional", "Non-Professional")
    ///
    /// # Returns
    /// The response or an error message
    pub async fn set_rithmic_mrkt_data_self_cert_status(
        &self,
        agreement_id: &str,
        market_data_usage_capacity: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
            agreement_id: agreement_id.to_string(),
            market_data_usage_capacity: market_data_usage_capacity.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// List exchange permissions for a user
    ///
    /// Returns the exchanges the user has permission to trade on, along with
    /// their entitlement status for each exchange.
    ///
    /// # Arguments
    /// * `user` - The username to query exchange permissions for
    ///
    /// # Returns
    /// A vector of responses containing exchange permission information or an error message
    pub async fn list_exchange_permissions(
        &self,
        user: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListExchangePermissions {
            user: user.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }
}
