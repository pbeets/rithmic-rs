use tracing::{error, info, warn};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

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
        messages::RithmicMessage, request_login::SysInfraType, request_tick_bar_update,
        request_time_bar_replay::BarType, request_time_bar_update,
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

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::{Interval, sleep_until},
};

pub(crate) enum HistoryPlantCommand {
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
    LoadTicks {
        bar_type_specifier: String,
        end_time_sec: i32,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
        start_time_sec: i32,
        symbol: String,
    },
    LoadTimeBars {
        bar_type: BarType,
        bar_type_period: i32,
        end_time_sec: i32,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
        start_time_sec: i32,
        symbol: String,
    },
    // New commands for additional historical data functionality
    LoadVolumeProfileMinuteBars {
        symbol: String,
        exchange: String,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
        user_max_count: Option<i32>,
        resume_bars: Option<bool>,
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

/// The RithmicHistoryPlant provides access to historical market data through the Rithmic API.
///
/// It allows applications to retrieve historical tick data and time bar data for specific instruments and time ranges
/// from Rithmic's history database.
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicConfig, RithmicEnv, ConnectStrategy, RithmicHistoryPlant,
/// };
/// use tokio::time::{sleep, Duration};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Step 1: Create connection configuration
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///
///     // Step 2: Connect to the history plant
///     let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
///
///     // Step 3: Get a handle to interact with the plant
///     let mut handle = history_plant.get_handle();
///
///     // Step 4: Login to the history plant
///     handle.login().await?;
///
///     // Step 5: Load historical tick data
///     let now = std::time::SystemTime::now()
///         .duration_since(std::time::UNIX_EPOCH)
///         .unwrap()
///         .as_secs() as i32;
///
///     // Get the last hour of data
///     let one_hour_ago = now - 3600;
///
///     let ticks = handle.load_ticks(
///         "ESH6".to_string(),
///         "CME".to_string(),
///         one_hour_ago,
///         now,
///     ).await?;
///
///     println!("Received {} tick responses", ticks.len());
///
///     // Step 6: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
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
    /// Returns an error if unable to establish WebSocket connection to the server.
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
    config: RithmicConfig,
    interval: Interval,
    logged_in: bool,
    ping_interval: Interval,
    ping_manager: PingManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<HistoryPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl HistoryPlant {
    pub async fn new(
        request_receiver: mpsc::Receiver<HistoryPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<HistoryPlant, RithmicError> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy)
            .await
            .map_err(|e| RithmicError::ConnectionFailed(e.to_string()))?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "history_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(HistoryPlant {
            config: config.clone(),
            interval,
            ping_interval,
            logged_in: false,
            ping_manager,
            request_handler: RithmicRequestHandler::new(),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender,
            rithmic_sender_api,
            subscription_sender,
        })
    }
}

impl HistoryPlant {
    async fn send_or_fail(&mut self, msg: Message, request_id: &str) {
        if self.rithmic_sender.send(msg).await.is_err() {
            error!(
                "history_plant: WebSocket send failed for request {}",
                request_id
            );

            self.request_handler
                .fail_request(request_id, RithmicError::SendFailed);
        }
    }
}

impl PlantActor for HistoryPlant {
    type Command = HistoryPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
              _ = self.interval.tick() => {
                if self.logged_in {
                    self.handle_command(HistoryPlantCommand::SendHeartbeat).await;
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
                if matches!(message, HistoryPlantCommand::Abort) {
                    info!("history_plant: abort requested, shutting down immediately");

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
              else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> bool {
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                info!("history_plant: Received close frame: {:?}", frame);
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
                                warn!("history_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!(
                        "history_plant: error response from server: {:?}",
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
                error!("history_plant: connection closed");

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
                error!("history_plant: connection already closed");

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
                error!("history_plant: I/O error: {}", io_err);

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
                error!("history_plant: connection reset without closing handshake");

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
                error!("history_plant: attempted to send after closing");

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
                error!("history_plant: received data after closing");

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
                warn!("history_plant: Unhandled message {:?}", message);
            }
        }

        stop
    }

    async fn handle_command(&mut self, command: HistoryPlantCommand) {
        match command {
            HistoryPlantCommand::Close => {
                let _ = self.rithmic_sender.send(Message::Close(None)).await;
            }
            HistoryPlantCommand::ListSystemInfo { response_sender } => {
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
            HistoryPlantCommand::Login {
                config,
                response_sender,
            } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::HistoryPlant,
                    &self.config.user,
                    &self.config.password,
                    &config,
                );

                info!("history_plant: sending login request {}", id);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(login_buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            HistoryPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(logout_buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::SendHeartbeat => {
                let (heartbeat_bf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_bf.into()))
                    .await;
            }
            HistoryPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            HistoryPlantCommand::LoadTicks {
                bar_type_specifier,
                exchange,
                symbol,
                start_time_sec,
                end_time_sec,
                response_sender,
            } => {
                let (tick_bar_replay_buf, id) = self.rithmic_sender_api.request_tick_bar_replay(
                    &symbol,
                    &exchange,
                    &bar_type_specifier,
                    start_time_sec,
                    end_time_sec,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(tick_bar_replay_buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::LoadTimeBars {
                bar_type,
                bar_type_period,
                end_time_sec,
                exchange,
                response_sender,
                start_time_sec,
                symbol,
            } => {
                let (time_bar_replay_buf, id) = self.rithmic_sender_api.request_time_bar_replay(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_type_period,
                    start_time_sec,
                    end_time_sec,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(time_bar_replay_buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::LoadVolumeProfileMinuteBars {
                symbol,
                exchange,
                bar_type_period,
                start_time_sec,
                end_time_sec,
                user_max_count,
                resume_bars,
                response_sender,
            } => {
                let (buf, id) = self.rithmic_sender_api.request_volume_profile_minute_bars(
                    &symbol,
                    &exchange,
                    bar_type_period,
                    start_time_sec,
                    end_time_sec,
                    user_max_count,
                    resume_bars,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::ResumeBars {
                request_key,
                response_sender,
            } => {
                let (buf, id) = self.rithmic_sender_api.request_resume_bars(&request_key);

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::SubscribeTimeBarUpdates {
                symbol,
                exchange,
                bar_type,
                bar_type_period,
                request,
                response_sender,
            } => {
                let (buf, id) = self.rithmic_sender_api.request_time_bar_update(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_type_period,
                    request,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
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
                let (buf, id) = self.rithmic_sender_api.request_tick_bar_update(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_sub_type,
                    &bar_type_specifier,
                    request,
                );

                let request_id = id.clone();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.send_or_fail(Message::Binary(buf.into()), &request_id)
                    .await;
            }
            HistoryPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
    }
}

/// Handle for sending commands to a [`RithmicHistoryPlant`] and receiving historical data.
///
/// Obtained from [`RithmicHistoryPlant::connect()`]. Use the methods on this handle to
/// log in and request historical tick, time-bar, or volume data. Streamed responses
/// arrive on [`subscription_receiver`](Self::subscription_receiver).
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
    pub async fn list_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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
        info!("history_plant: logging in ");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let mut config = config;
        config.aggregated_quotes = None;

        let command = HistoryPlantCommand::Login {
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
            error!("history_plant: login failed {:?}", err);
            Err(RithmicError::ServerError(err))
        } else {
            let _ = self.sender.send(HistoryPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("history_plant: session id: {}", session_id);
                }
            }

            info!("history_plant: logged in");

            Ok(response)
        }
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
        let response = rx
            .await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;
        let _ = self.sender.send(HistoryPlantCommand::Close).await;

        Ok(response)
    }

    /// Immediately shut down the history plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and `disconnect()` would hang.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(HistoryPlantCommand::Abort);
    }

    /// Load historical tick data for a specific symbol and time range.
    ///
    /// This is a convenience wrapper around [`load_tick_bars`](Self::load_tick_bars) with
    /// `bar_length = 1`, so each response contains a single tick.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., `"ESH6"`)
    /// * `exchange` - The exchange code (e.g., `"CME"`)
    /// * `start_time_sec` - Start time as a Unix timestamp (seconds)
    /// * `end_time_sec` - End time as a Unix timestamp (seconds)
    ///
    /// # Returns
    /// The historical tick data responses, or a [`RithmicError`] on failure.
    ///
    /// # Note
    ///
    /// Large requests may be truncated by the server. If the response contains a
    /// round number of bars (e.g., 10 000) or does not cover the full time range,
    /// use the `request_key` from the response with `request_resume_bars` on the
    /// sender API to fetch the remaining data.
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

    /// Load historical tick bar data for a specific symbol and time range.
    ///
    /// Each response contains a bar that aggregates `bar_length` ticks. For
    /// example, `bar_length = 5` returns 5-tick bars.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., `"ESH6"`)
    /// * `exchange` - The exchange code (e.g., `"CME"`)
    /// * `bar_length` - Number of ticks per bar (must be &ge; 1)
    /// * `start_time_sec` - Start time as a Unix timestamp (seconds)
    /// * `end_time_sec` - End time as a Unix timestamp (seconds)
    ///
    /// # Returns
    /// The historical tick bar data responses, or a [`RithmicError`] on failure.
    ///
    /// # Errors
    /// * [`RithmicError::InvalidArgument`] if `bar_length` is 0.
    /// * [`RithmicError::ConnectionClosed`] if the history plant has shut down.
    ///
    /// # Note
    ///
    /// Large requests may be truncated by the server. If the response contains a
    /// round number of bars (e.g., 10 000) or does not cover the full time range,
    /// use the `request_key` from the response with `request_resume_bars` on the
    /// sender API to fetch the remaining data.
    pub async fn load_tick_bars(
        &self,
        symbol: String,
        exchange: String,
        bar_length: u32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        if bar_length == 0 {
            return Err(RithmicError::InvalidArgument(
                "bar_length must be at least 1".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::LoadTicks {
            bar_type_specifier: bar_length.to_string(),
            exchange,
            symbol,
            start_time_sec,
            end_time_sec,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Load historical time bar data for a specific symbol and time range
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type` - The type of time bar (SecondBar, MinuteBar, DailyBar, WeeklyBar)
    /// * `bar_type_period` - The period for the bar type (e.g., 1 for 1-minute bars, 5 for 5-minute bars)
    /// * `start_time_sec` - Start time in Unix timestamp (seconds)
    /// * `end_time_sec` - End time in Unix timestamp (seconds)
    ///
    /// # Returns
    /// The historical time bar data responses or an error message
    pub async fn load_time_bars(
        &self,
        symbol: String,
        exchange: String,
        bar_type: BarType,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::LoadTimeBars {
            bar_type,
            bar_type_period,
            end_time_sec,
            exchange,
            response_sender: tx,
            start_time_sec,
            symbol,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Load volume profile minute bars
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type_period` - The period for the bars
    /// * `start_time_sec` - Start time in Unix timestamp (seconds)
    /// * `end_time_sec` - End time in Unix timestamp (seconds)
    /// * `user_max_count` - Optional maximum number of bars to return
    /// * `resume_bars` - Whether to resume from a previous request
    ///
    /// # Returns
    /// The volume profile minute bar responses or an error message
    #[allow(clippy::too_many_arguments)]
    pub async fn load_volume_profile_minute_bars(
        &self,
        symbol: String,
        exchange: String,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
        user_max_count: Option<i32>,
        resume_bars: Option<bool>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = HistoryPlantCommand::LoadVolumeProfileMinuteBars {
            symbol,
            exchange,
            bar_type_period,
            start_time_sec,
            end_time_sec,
            user_max_count,
            resume_bars,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Resume a previously truncated bars request
    ///
    /// Use this when a bars request was truncated due to data limits.
    ///
    /// # Arguments
    /// * `request_key` - The request key from the previous truncated response
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Subscribe to live time bar updates
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type` - The type of time bar (SecondBar, MinuteBar, DailyBar, WeeklyBar)
    /// * `bar_type_period` - The period for the bar type (e.g., 1 for 1-minute bars)
    /// * `request` - Subscribe or Unsubscribe
    ///
    /// # Returns
    /// The subscription response or an error message
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Subscribe to live tick bar updates
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type` - The type of tick bar
    /// * `bar_sub_type` - Sub-type of the bar
    /// * `bar_type_specifier` - Specifier for the bar (e.g., "1" for 1-tick bars)
    /// * `request` - Subscribe or Unsubscribe
    ///
    /// # Returns
    /// The subscription response or an error message
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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
