use std::{sync::Arc, time::Duration};

use tracing::{error, info, warn};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        rithmic_command_types::LoginConfig,
        sender_api::RithmicSenderApi,
    },
    config::{RithmicAccount, RithmicConfig},
    error::RithmicError,
    ping_manager::PingManager,
    plants::subscription::SubscriptionFilter,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{messages::RithmicMessage, request_login::SysInfraType, request_pn_l_position_updates},
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
    time::{Interval, sleep_until},
};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

pub(crate) enum PnlPlantCommand {
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
    PnlPositionSnapshots {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UpdateHeartbeat {
        seconds: u64,
    },
    SubscribePnlUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UnsubscribePnlUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
}

/// The RithmicPnlPlant provides access to profit and loss (PnL) information through the Rithmic API.
///
/// It allows applications to:
/// - Retrieve current PnL information for positions
/// - Subscribe to real-time PnL updates
/// - Track position changes and risk metrics
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicAccount, RithmicConfig, RithmicEnv, ConnectStrategy, RithmicPnlPlant,
///     rti::messages::RithmicMessage,
/// };
/// use tokio::time::{sleep, Duration};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Step 1: Create connection configuration
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///     let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
///
///     // Step 2: Connect to the PnL plant
///     let pnl_plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Retry).await?;
///
///     // Step 3: Get a handle to interact with the plant
///     let mut handle = pnl_plant.get_handle(&account);
///
///     // Step 4: Login to the PnL plant
///     handle.login().await?;
///
///     // Step 5: Get a current snapshot of all PnL positions
///     let snapshots = handle.pnl_position_snapshots().await?;
///     println!("PnL position snapshot: {:?}", snapshots);
///
///     // Step 6: Subscribe to ongoing PnL updates
///     handle.subscribe_pnl_updates().await?;
///
///     // Step 7: Process real-time PnL updates
///     for _ in 0..5 {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 match update.message {
///                     RithmicMessage::AccountPnLPositionUpdate(_) => {}
///                     RithmicMessage::InstrumentPnLPositionUpdate(_) => {}
///                     _ => {}
///                 }
///             },
///             Err(e) => println!("Error receiving update: {}", e),
///         }
///     }
///
///     // Step 8: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct RithmicPnlPlant {
    pub(crate) connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<PnlPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicPnlPlant {
    /// Create a new PnL Plant connection to access profit and loss information.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicPnlPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if unable to establish WebSocket connection to the server.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicPnlPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<PnlPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut pnl_plant = PnlPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            pnl_plant.run().await;
        });

        Ok(RithmicPnlPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicPnlPlant {
    /// Wait for the plant's background connection task to finish.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to interact with the PnL plant.
    ///
    /// The handle provides methods to subscribe to PnL updates and retrieve position snapshots.
    /// Multiple handles can be created from the same plant for different accounts.
    pub fn get_handle(&self, account: &RithmicAccount) -> RithmicPnlPlantHandle {
        let account = Arc::new(account.clone());
        let account_for_filter = Arc::clone(&account);

        RithmicPnlPlantHandle {
            account,
            sender: self.sender.clone(),
            subscription_receiver: SubscriptionFilter::new(
                account_for_filter,
                self.subscription_sender.subscribe(),
            ),
        }
    }
}

#[derive(Debug)]
struct PnlPlant {
    config: RithmicConfig,
    // Distinguishes an intentional local shutdown from an unexpected peer close.
    close_requested: bool,
    interval: Interval,
    logged_in: bool,
    ping_interval: Interval,
    ping_manager: PingManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<PnlPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl PnlPlant {
    async fn new(
        request_receiver: mpsc::Receiver<PnlPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<PnlPlant, RithmicError> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy)
            .await
            .map_err(|e| RithmicError::ConnectionFailed(e.to_string()))?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "pnl_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(PnlPlant {
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
}

impl PnlPlant {
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
                    "pnl_plant: WebSocket send failed for request {}: {}",
                    request_id, error
                );
                self.request_handler
                    .fail_request(request_id, RithmicError::SendFailed);
            }
            Err(WebSocketSendError::Timeout) => {
                error!(
                    "pnl_plant: WebSocket send timed out for request {}",
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
                error!("pnl_plant: WebSocket ping send failed: {}", error);
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    format!("WebSocket ping send failed: {error}"),
                );
                true
            }
            Err(WebSocketSendError::Timeout) => {
                error!("pnl_plant: WebSocket ping send timed out");
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
                error!("pnl_plant: heartbeat send failed: {}", error);
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    format!("Heartbeat send failed: {error}"),
                );
                true
            }
            Err(WebSocketSendError::Timeout) => {
                error!("pnl_plant: heartbeat send timed out");
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
                warn!("pnl_plant: close send failed: {}", error);
            }
            Err(WebSocketSendError::Timeout) => {
                warn!("pnl_plant: close send timed out");
            }
        }
    }
}

impl PlantActor for PnlPlant {
    type Command = PnlPlantCommand;

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
                    if matches!(message, PnlPlantCommand::Abort) {
                        info!("pnl_plant: abort requested, shutting down immediately");
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
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                info!("pnl_plant: Received close frame: {:?}", frame);
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
                                warn!("pnl_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!("pnl_plant: error response from server: {:?}", err_response);

                    if err_response.is_update {
                        let _ = self.subscription_sender.send(err_response);
                    } else {
                        self.request_handler.handle_response(err_response);
                    }
                }
            },
            Err(Error::ConnectionClosed) => {
                error!("pnl_plant: connection closed");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket connection closed",
                );
                stop = true;
            }
            Err(Error::AlreadyClosed) => {
                error!("pnl_plant: connection already closed");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket connection already closed",
                );
                stop = true;
            }
            Err(Error::Io(ref io_err)) => {
                error!("pnl_plant: I/O error: {}", io_err);
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    format!("WebSocket I/O error: {}", io_err),
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                error!("pnl_plant: connection reset without closing handshake");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket connection reset without closing handshake",
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {
                error!("pnl_plant: attempted to send after closing");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket attempted to send after closing",
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ReceivedAfterClosing)) => {
                error!("pnl_plant: received data after closing");
                self.fail_connection_and_drain(
                    "",
                    RithmicMessage::ConnectionError,
                    "WebSocket received data after closing",
                );
                stop = true;
            }
            _ => {
                warn!("pnl_plant: Unhandled message: {:?}", message);
            }
        }

        stop
    }

    async fn handle_command(&mut self, command: PnlPlantCommand) {
        match command {
            PnlPlantCommand::Close => {
                self.close_requested = true;
                self.send_close_best_effort().await;
            }
            PnlPlantCommand::ListSystemInfo { response_sender } => {
                let (list_system_info_buf, id) =
                    self.rithmic_sender_api.request_rithmic_system_info();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(list_system_info_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::Login {
                config,
                response_sender,
            } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::PnlPlant,
                    &self.config.user,
                    &self.config.password,
                    &config,
                );

                info!("pnl_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(login_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            PnlPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(logout_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            PnlPlantCommand::SubscribePnlUpdates {
                account,
                response_sender,
            } => {
                let (subscribe_buf, id) = self.rithmic_sender_api.request_pnl_position_updates(
                    request_pn_l_position_updates::Request::Subscribe,
                    &account,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(subscribe_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::PnlPositionSnapshots {
                account,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .rithmic_sender_api
                    .request_pnl_position_snapshot(&account);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(snapshot_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::UnsubscribePnlUpdates {
                account,
                response_sender,
            } => {
                let (unsubscribe_buf, id) = self.rithmic_sender_api.request_pnl_position_updates(
                    request_pn_l_position_updates::Request::Unsubscribe,
                    &account,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(unsubscribe_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
    }
}

/// Handle for sending commands to a [`RithmicPnlPlant`] and receiving P&L updates.
///
/// Obtained from [`RithmicPnlPlant::connect()`]. Use the methods on this handle to
/// log in and subscribe to real-time P&L and position updates. Updates arrive on
/// [`subscription_receiver`](Self::subscription_receiver).
pub struct RithmicPnlPlantHandle {
    account: Arc<RithmicAccount>,
    sender: mpsc::Sender<PnlPlantCommand>,
    /// Receiver for real-time P&L and position updates.
    pub subscription_receiver: SubscriptionFilter,
}

impl std::fmt::Debug for RithmicPnlPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicPnlPlantHandle")
            .field("account", &self.account)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

impl RithmicPnlPlantHandle {
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
    pub async fn list_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Log in to the Rithmic PnL plant
    ///
    /// This must be called before subscribing to any PnL data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic PnL plant with custom configuration
    ///
    /// This must be called before subscribing to any PnL data.
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
        info!("pnl_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let mut config = config;
        config.aggregated_quotes = None;

        let command = PnlPlantCommand::Login {
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
            error!("pnl_plant: login failed {:?}", err);
            Err(RithmicError::ServerError(err))
        } else {
            let _ = self.sender.send(PnlPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("pnl_plant: session id: {}", session_id);
                }
            }

            info!("pnl_plant: logged in");

            Ok(response)
        }
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = PnlPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Disconnect from the Rithmic PnL plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let r = rx.await.map_err(|_| RithmicError::ConnectionClosed)??;
        let _ = self.sender.send(PnlPlantCommand::Close).await;

        r.into_iter().next().ok_or(RithmicError::EmptyResponse)
    }

    /// Immediately shut down the PnL plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and `disconnect()` would hang.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(PnlPlantCommand::Abort);
    }

    /// Subscribe to PnL updates for all positions
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_pnl_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::SubscribePnlUpdates {
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

    /// Request a snapshot of all current position PnL data
    ///
    /// # Returns
    /// The position snapshot response or an error message
    pub async fn pnl_position_snapshots(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::PnlPositionSnapshots {
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

    /// Unsubscribe from PnL updates
    ///
    /// # Returns
    /// The unsubscription response or an error message
    pub async fn unsubscribe_pnl_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::UnsubscribePnlUpdates {
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
}
