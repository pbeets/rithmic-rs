use std::time::Duration;

use tracing::{error, info, warn};

use futures_util::{
    Sink, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, oneshot},
    time::Interval,
};

use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
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
    rti::{messages::RithmicMessage, request_login::SysInfraType},
    ws::{
        PING_TIMEOUT_SECS, SEND_TIMEOUT_SECS, WebSocketSendError, connect_with_strategy,
        get_heartbeat_interval, get_ping_interval, send_with_timeout,
    },
};

pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type WsSink = SplitSink<WsStream, Message>;
pub(crate) type WsReader = SplitStream<WsStream>;

/// Result of a single iteration of the plant's `select!` loop.
pub(crate) enum SelectResult<C> {
    HeartbeatFired,
    PingFired,
    PingTimeout,
    Command(C),
    RithmicMessage(Result<Message, Error>),
    /// The WebSocket reader stream returned `None` (clean EOF from the peer).
    StreamClosed,
}

/// Shared infrastructure for all Rithmic plant actors.
///
/// Holds the WebSocket connection, heartbeat/ping timers, request handler,
/// and sender/receiver APIs that every plant uses. Each plant wraps a
/// `PlantCore` plus its own command receiver.
///
/// The type parameter `S` is the WebSocket sink type. It defaults to [`WsSink`]
/// (the concrete split-sink from a real TLS connection) but can be replaced
/// with a mock sink in tests.
#[derive(Debug)]
pub(crate) struct PlantCore<S = WsSink> {
    pub(crate) config: RithmicConfig,
    pub(crate) close_requested: bool,
    pub(crate) interval: Interval,
    pub(crate) logged_in: bool,
    pub(crate) ping_interval: Interval,
    pub(crate) ping_manager: PingManager,
    pub(crate) request_handler: RithmicRequestHandler,
    pub(crate) rithmic_reader: WsReader,
    pub(crate) rithmic_receiver_api: RithmicReceiverApi,
    pub(crate) rithmic_sender: S,
    pub(crate) rithmic_sender_api: RithmicSenderApi,
    pub(crate) subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl PlantCore<WsSink> {
    pub(crate) async fn new(
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
        source: &'static str,
    ) -> Result<PlantCore, RithmicError> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy)
            .await
            .map_err(|e| RithmicError::ConnectionFailed(e.to_string()))?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();
        let rithmic_sender_api = RithmicSenderApi::new(config);

        let rithmic_receiver_api = RithmicReceiverApi {
            source: source.to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(PlantCore {
            config: config.clone(),
            close_requested: false,
            interval,
            logged_in: false,
            ping_interval,
            ping_manager,
            request_handler: RithmicRequestHandler::new(),
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender,
            rithmic_sender_api,
            subscription_sender,
        })
    }
}

impl<S> PlantCore<S>
where
    S: Sink<Message, Error = Error> + Unpin,
{
    pub(crate) fn emit_connection_health_event(&self, request_id: &str, error: RithmicError) {
        let message = error.as_connection_message();

        let error_response = RithmicResponse {
            request_id: request_id.to_string(),
            message,
            is_update: true,
            has_more: false,
            multi_response: false,
            error: Some(error),
            source: self.rithmic_receiver_api.source.clone(),
        };

        let _ = self.subscription_sender.send(error_response);
    }

    pub(crate) fn fail_connection_and_drain(&mut self, request_id: &str, error: RithmicError) {
        self.emit_connection_health_event(request_id, error);
        self.request_handler.drain_and_drop();
    }

    pub(crate) async fn send_or_fail(&mut self, msg: Message, request_id: &str) {
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
                    "{}: WebSocket send failed for request {}: {}",
                    self.rithmic_receiver_api.source, request_id, error
                );
                // Fail only this request. Transport errors from the sink surface
                // promptly through the reader (e.g. Error::ConnectionClosed),
                // which drains remaining requests and emits the connection-health
                // event from a path that can stop the actor loop.
                self.request_handler
                    .fail_request(request_id, RithmicError::SendFailed);
            }
            Err(WebSocketSendError::Timeout) => {
                error!(
                    "{}: WebSocket send timed out for request {} — sink poisoned",
                    self.rithmic_receiver_api.source, request_id
                );
                // send_with_timeout's contract requires the sink be treated as
                // poisoned after a Timeout (the message may still be buffered).
                // A half-open TCP connection may not surface through the reader,
                // so drain all pending requests and broadcast ConnectionError
                // now rather than letting subsequent sends pile into a dead sink.
                // The actor loop will stop when the next ping fires (within
                // PING_INTERVAL_SECS). Heartbeat does not stop it because
                // send_heartbeat returns early when logged_in=false.
                self.fail_connection_and_drain(
                    request_id,
                    RithmicError::ConnectionFailed(
                        "WebSocket send timed out — sink poisoned".to_string(),
                    ),
                );
            }
        }
    }

    /// Send a WebSocket ping frame. Returns `true` if the actor should stop.
    ///
    /// Skips (returns `false`) when a close has already been requested.
    pub(crate) async fn send_ping(&mut self) -> bool {
        if self.close_requested {
            return false;
        }

        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Ping(vec![].into()),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => {
                self.ping_manager.sent();

                false
            }
            Err(WebSocketSendError::Transport(error)) => {
                error!(
                    "{}: WebSocket ping send failed — connection dead: {}",
                    self.rithmic_receiver_api.source, error
                );
                // Dead link: surface as HeartbeatTimeout so reconnect callers
                // see the same signal as a true ping timeout.
                self.fail_connection_and_drain(
                    "websocket_ping_send_failed",
                    RithmicError::HeartbeatTimeout,
                );

                true
            }
            Err(WebSocketSendError::Timeout) => {
                error!(
                    "{}: WebSocket ping send timed out",
                    self.rithmic_receiver_api.source
                );
                self.fail_connection_and_drain(
                    "websocket_ping_timeout",
                    RithmicError::HeartbeatTimeout,
                );

                true
            }
        }
    }

    /// Send a Rithmic heartbeat message. Returns `true` if the actor should stop.
    ///
    /// Skips (returns `false`) when not yet logged in or a close has been requested.
    pub(crate) async fn send_heartbeat(&mut self) -> bool {
        if !self.logged_in || self.close_requested {
            return false;
        }

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
                error!(
                    "{}: heartbeat send failed — connection dead: {}",
                    self.rithmic_receiver_api.source, error
                );
                // Dead link: surface as HeartbeatTimeout (same signal as a
                // true heartbeat timeout).
                self.fail_connection_and_drain(
                    "heartbeat_send_failed",
                    RithmicError::HeartbeatTimeout,
                );

                true
            }
            Err(WebSocketSendError::Timeout) => {
                error!(
                    "{}: heartbeat send timed out",
                    self.rithmic_receiver_api.source
                );
                self.fail_connection_and_drain(
                    "heartbeat_send_timeout",
                    RithmicError::HeartbeatTimeout,
                );

                true
            }
        }
    }

    pub(crate) async fn send_close_best_effort(&mut self) {
        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Close(None),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => {}
            Err(WebSocketSendError::Transport(error)) => {
                warn!(
                    "{}: close send failed: {}",
                    self.rithmic_receiver_api.source, error
                );
            }
            Err(WebSocketSendError::Timeout) => {
                warn!("{}: close send timed out", self.rithmic_receiver_api.source);
            }
        }
    }

    /// Route a decoded or decode-failed response into the subscription
    /// broadcast vs the per-request responder, with the heartbeat special case
    /// that synthesizes a `HeartbeatTimeout` subscription update for errors
    /// while still resolving any registered oneshot with the original frame.
    fn forward_response(&mut self, source: &str, response: RithmicResponse) {
        // Heartbeat: synthesize HeartbeatTimeout for errors (broadcast on
        // subscription channel), but ALWAYS call handle_response with the
        // original ResponseHeartbeat message so any registered oneshot
        // responder is still resolved. Passing the synthetic HeartbeatTimeout
        // to handle_response would mis-route it (handle_response dispatches on
        // message type).
        if matches!(response.message, RithmicMessage::ResponseHeartbeat(_)) {
            if response.error.is_some() {
                let synthetic = RithmicResponse {
                    request_id: response.request_id.clone(),
                    message: RithmicMessage::HeartbeatTimeout,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: response.error.clone(),
                    source: self.rithmic_receiver_api.source.clone(),
                };

                let _ = self.subscription_sender.send(synthetic);
            }

            self.request_handler.handle_response(response);

            return;
        }

        if response.is_update {
            if let Err(e) = self.subscription_sender.send(response) {
                warn!("{}: no active subscribers: {:?}", source, e);
            }
        } else {
            self.request_handler.handle_response(response);
        }
    }

    /// Handle a raw WebSocket message. Returns `true` if the actor should stop.
    pub(crate) async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> bool {
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                let source = self.rithmic_receiver_api.source.clone();
                info!("{}: received close frame: {:?}", source, frame);

                if self.close_requested {
                    self.request_handler.drain_and_drop();
                } else {
                    self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
                }

                stop = true;
            }
            Ok(Message::Pong(_)) => {
                self.ping_manager.received();
            }
            Ok(Message::Binary(data)) => {
                let source = self.rithmic_receiver_api.source.clone();

                match self.rithmic_receiver_api.buf_to_message(data) {
                    Ok(response) => self.forward_response(&source, response),
                    Err(err_response) => {
                        error!("{}: decode failure: {:?}", source, err_response);
                        self.forward_response(&source, err_response);
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                // RFC 6455 §5.5.3: a Ping must be answered with a Pong carrying
                // the same payload.  With a split sink/stream the tungstenite
                // internal write buffer is only flushed when the sink side is
                // polled, so we send the Pong explicitly to guarantee delivery.
                let source = self.rithmic_receiver_api.source.clone();
                match send_with_timeout(
                    &mut self.rithmic_sender,
                    Message::Pong(data),
                    Duration::from_secs(SEND_TIMEOUT_SECS),
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        // Surfaced as ConnectionError (not HeartbeatTimeout): a
                        // pong is a reply to a server-initiated ping, not part
                        // of our own heartbeat lifecycle. ping/heartbeat send
                        // failures use HeartbeatTimeout because they share a
                        // timeout semantics with a true heartbeat timeout.
                        // Both satisfy is_connection_issue() so reconnect
                        // callers see the same signal either way.
                        warn!("{}: failed to send pong: {:?}", source, e);
                        self.fail_connection_and_drain(
                            "",
                            RithmicError::ConnectionFailed(
                                "Failed to send pong — sink dead".to_string(),
                            ),
                        );
                        stop = true;
                    }
                }
            }
            Err(Error::ConnectionClosed) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: connection closed", source);
                self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
                stop = true;
            }
            Err(Error::AlreadyClosed) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: connection already closed", source);
                self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
                stop = true;
            }
            Err(Error::Io(ref io_err)) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: I/O error: {}", source, io_err);
                self.fail_connection_and_drain(
                    "",
                    RithmicError::ConnectionFailed(format!("WebSocket I/O error: {}", io_err)),
                );
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: connection reset without closing handshake", source);
                self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: attempted to send after closing", source);
                self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ReceivedAfterClosing)) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: received data after closing", source);
                self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
                stop = true;
            }
            Err(e) => {
                let source = self.rithmic_receiver_api.source.clone();
                error!("{}: unhandled WebSocket error, closing: {}", source, e);
                self.fail_connection_and_drain(
                    "",
                    RithmicError::ConnectionFailed(format!("WebSocket error: {e}")),
                );
                stop = true;
            }
            Ok(_) => {
                warn!(
                    "{}: received unhandled message type",
                    self.rithmic_receiver_api.source
                );
            }
        }

        stop
    }

    /// Handle a clean EOF on the WebSocket reader stream. Returns `true` (stop).
    ///
    /// A `None` from `reader.next()` means the peer closed the TCP connection
    /// without sending a WebSocket Close frame — treat it as an unexpected drop.
    pub(crate) fn handle_stream_closed(&mut self) -> bool {
        let source = &self.rithmic_receiver_api.source;
        error!("{}: WebSocket stream closed unexpectedly (EOF)", source);
        self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
        true
    }
}

impl<S> PlantCore<S>
where
    S: Sink<Message, Error = Error> + Unpin,
{
    pub(crate) async fn handle_close(&mut self) {
        self.close_requested = true;
        // Drain pending requests immediately so callers are not left waiting for
        // a server close-echo that may never arrive (e.g. on network drop).
        self.request_handler.drain_and_drop();
        self.send_close_best_effort().await;
    }

    pub(crate) async fn handle_list_system_info(
        &mut self,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    ) {
        let (list_system_info_buf, id) = self.rithmic_sender_api.request_rithmic_system_info();

        self.request_handler.register_request(RithmicRequest {
            request_id: id.clone(),
            responder: response_sender,
        });

        self.send_or_fail(Message::Binary(list_system_info_buf.into()), &id)
            .await;
    }

    pub(crate) async fn handle_login(
        &mut self,
        config: LoginConfig,
        sys_infra_type: SysInfraType,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    ) {
        let (login_buf, id) = self.rithmic_sender_api.request_login(
            &self.config.system_name,
            sys_infra_type,
            &self.config.user,
            &self.config.password,
            &config,
        );

        info!(
            "{}: sending login request {}",
            self.rithmic_receiver_api.source, id
        );

        self.request_handler.register_request(RithmicRequest {
            request_id: id.clone(),
            responder: response_sender,
        });

        self.send_or_fail(Message::Binary(login_buf.into()), &id)
            .await;
    }

    pub(crate) fn handle_set_login(&mut self) {
        self.logged_in = true;
    }

    pub(crate) async fn handle_logout(
        &mut self,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    ) {
        // Flip `close_requested` before handing control to any later async step.
        // The actor processes commands sequentially, so any command queued by a
        // cloned handle *after* we dequeued `Logout` will find `close_requested`
        // set and be rejected by `handle_command`'s guard. This closes the
        // disconnect race where a concurrent `subscribe()` could slip between
        // the Logout oneshot resolving and the Close command being sent.
        self.close_requested = true;

        let (logout_buf, id) = self.rithmic_sender_api.request_logout();

        self.request_handler.register_request(RithmicRequest {
            request_id: id.clone(),
            responder: response_sender,
        });

        self.send_or_fail(Message::Binary(logout_buf.into()), &id)
            .await;
    }

    pub(crate) fn handle_update_heartbeat(&mut self, seconds: u64) {
        self.interval = get_heartbeat_interval(Some(seconds));
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_util::StreamExt;
    use tokio::sync::{broadcast, oneshot};
    use tokio_tungstenite::tungstenite::{Error, Message, error::ProtocolError};

    use super::*;
    use crate::{
        api::{receiver_api::RithmicResponse, sender_api::RithmicSenderApi},
        config::{RithmicConfig, RithmicEnv},
        error::RithmicError,
        ping_manager::PingManager,
        request_handler::{RithmicRequest, RithmicRequestHandler},
        rti::messages::RithmicMessage,
        ws::{PING_TIMEOUT_SECS, get_heartbeat_interval, get_ping_interval},
    };

    enum MockSinkBehavior {
        Ready,
        Error,
        Pending,
    }

    struct MockMessageSink {
        behavior: MockSinkBehavior,
        pub sent_messages: Vec<Message>,
    }

    impl MockMessageSink {
        fn ready() -> Self {
            Self {
                behavior: MockSinkBehavior::Ready,
                sent_messages: Vec::new(),
            }
        }

        fn error() -> Self {
            Self {
                behavior: MockSinkBehavior::Error,
                sent_messages: Vec::new(),
            }
        }

        fn pending() -> Self {
            Self {
                behavior: MockSinkBehavior::Pending,
                sent_messages: Vec::new(),
            }
        }
    }

    impl std::fmt::Debug for MockMessageSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockMessageSink").finish()
        }
    }

    impl Sink<Message> for MockMessageSink {
        type Error = Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.behavior {
                MockSinkBehavior::Ready => Poll::Ready(Ok(())),
                MockSinkBehavior::Error => Poll::Ready(Err(Error::ConnectionClosed)),
                MockSinkBehavior::Pending => Poll::Pending,
            }
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.get_mut().sent_messages.push(item);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.behavior {
                MockSinkBehavior::Ready => Poll::Ready(Ok(())),
                MockSinkBehavior::Error => Poll::Ready(Err(Error::ConnectionClosed)),
                MockSinkBehavior::Pending => Poll::Pending,
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.behavior {
                MockSinkBehavior::Ready => Poll::Ready(Ok(())),
                MockSinkBehavior::Error => Poll::Ready(Err(Error::ConnectionClosed)),
                MockSinkBehavior::Pending => Poll::Pending,
            }
        }
    }

    fn test_config() -> RithmicConfig {
        RithmicConfig::builder(RithmicEnv::Demo)
            .user("test_user")
            .password("test_password")
            .url("ws://localhost:9999")
            .beta_url("ws://localhost:9998")
            .app_name("test_app")
            .app_version("1.0")
            .build()
            .unwrap()
    }

    /// Create a real-but-dormant `WsReader` by establishing a local WebSocket
    /// connection so the type is satisfied. The reader is never actually polled
    /// in any of the tests below.
    async fn make_dormant_ws_reader() -> WsReader {
        use tokio::net::{TcpListener, TcpStream};
        use tokio_tungstenite::tungstenite::protocol::Role;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client_tcp, server_result) =
            tokio::join!(TcpStream::connect(addr), async { listener.accept().await });

        let client_tcp = client_tcp.unwrap();
        let (server_tcp, _) = server_result.unwrap();

        // Wrap both sides in MaybeTlsStream::Plain so the type matches WsStream.
        let server_stream = MaybeTlsStream::Plain(server_tcp);

        // Build a raw WebSocket on the server side (no HTTP upgrade needed for
        // our purposes — we only need the type, not actual messages).
        let server_ws = WebSocketStream::from_raw_socket(server_stream, Role::Server, None).await;

        // Drop the client TCP so the server stream sits idle; split and return
        // only the reader half.
        drop(client_tcp);
        let (_, reader) = server_ws.split();
        reader
    }

    fn make_test_core(
        sink: MockMessageSink,
        rithmic_reader: WsReader,
    ) -> (
        PlantCore<MockMessageSink>,
        broadcast::Receiver<RithmicResponse>,
    ) {
        let config = test_config();
        let (sub_tx, sub_rx) = broadcast::channel(16);
        let rithmic_sender_api = RithmicSenderApi::new(&config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let core = PlantCore {
            config,
            close_requested: false,
            interval: get_heartbeat_interval(None),
            logged_in: false,
            ping_interval: get_ping_interval(None),
            ping_manager: PingManager::new(PING_TIMEOUT_SECS),
            request_handler: RithmicRequestHandler::new(),
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender: sink,
            rithmic_sender_api,
            subscription_sender: sub_tx,
        };

        (core, sub_rx)
    }

    fn register_request(
        core: &mut PlantCore<MockMessageSink>,
        id: &str,
    ) -> oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>> {
        let (tx, rx) = oneshot::channel();
        core.request_handler.register_request(RithmicRequest {
            request_id: id.to_string(),
            responder: tx,
        });
        rx
    }

    #[tokio::test]
    async fn fail_connection_and_drain_broadcasts_and_drains_pending() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let mut rx1 = register_request(&mut core, "req-1");

        core.fail_connection_and_drain("", RithmicError::ProtocolError("test error".to_string()));

        // Subscription broadcast received the event
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
        assert!(matches!(
            &broadcast_msg.error,
            Some(RithmicError::ProtocolError(s)) if s == "test error"
        ));

        // Pending request was drained with ConnectionClosed
        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn fail_connection_and_drain_with_no_pending_requests() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        core.fail_connection_and_drain("", RithmicError::ProtocolError("no requests".to_string()));

        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn send_or_fail_transport_error_fails_only_that_request() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, _sub_rx) = make_test_core(MockMessageSink::error(), reader);
        let mut rx1 = register_request(&mut core, "req-1");
        let mut rx2 = register_request(&mut core, "req-2");

        core.send_or_fail(Message::Ping(vec![].into()), "req-1")
            .await;

        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::SendFailed)));

        assert!(matches!(
            rx2.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn send_or_fail_timeout_drains_all_pending_and_broadcasts() {
        // send_with_timeout's contract poisons the sink on any non-Ok return.
        // A half-open TCP connection may not surface through the reader, so
        // send_or_fail must drain ALL pending requests and broadcast a
        // ConnectionError on Timeout, not just fail the one request.
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::pending(), reader);
        let mut rx1 = register_request(&mut core, "req-1");
        let mut rx2 = register_request(&mut core, "req-2");

        tokio::time::pause();
        let fut = core.send_or_fail(Message::Ping(vec![].into()), "req-1");
        tokio::time::advance(std::time::Duration::from_secs(SEND_TIMEOUT_SECS + 1)).await;
        fut.await;

        // Both pending requests drained with ConnectionClosed.
        assert!(matches!(
            rx1.try_recv().unwrap(),
            Err(RithmicError::ConnectionClosed)
        ));
        assert!(matches!(
            rx2.try_recv().unwrap(),
            Err(RithmicError::ConnectionClosed)
        ));

        // Subscribers saw a ConnectionError, not a HeartbeatTimeout — the
        // reviewer's note on the pong/ping asymmetry covers why this path uses
        // ConnectionError (the sink, not the heartbeat, is what failed).
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(
            matches!(broadcast_msg.message, RithmicMessage::ConnectionError),
            "send_or_fail timeout should broadcast ConnectionError, got {:?}",
            broadcast_msg.message
        );
        assert!(
            broadcast_msg
                .error
                .as_ref()
                .expect("error should be set")
                .is_connection_issue()
        );
    }

    #[tokio::test]
    async fn send_ping_skips_when_close_requested() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, _sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        core.close_requested = true;
        let stop = core.send_ping().await;

        assert!(
            !stop,
            "send_ping should return false when close is requested"
        );
        assert!(
            core.ping_manager.next_timeout_at().is_none(),
            "no ping should have been registered"
        );
    }

    #[tokio::test]
    async fn send_ping_success_marks_ping_manager() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, _sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let stop = core.send_ping().await;

        assert!(!stop, "send_ping should return false on success");
        assert!(
            core.ping_manager.next_timeout_at().is_some(),
            "ping_manager should track the pending ping"
        );
    }

    #[tokio::test]
    async fn ping_send_transport_failure_broadcasts_heartbeat_timeout() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::error(), reader);
        let stop = core.send_ping().await;

        assert!(stop, "send_ping should return true on transport error");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(
            matches!(broadcast_msg.message, RithmicMessage::HeartbeatTimeout),
            "ping send transport failure should surface as HeartbeatTimeout, got {:?}",
            broadcast_msg.message
        );
        // Still satisfies is_connection_issue() for reconnect-driving callers.
        assert!(
            broadcast_msg
                .error
                .as_ref()
                .expect("error should be set")
                .is_connection_issue()
        );
    }

    #[tokio::test]
    async fn send_ping_timeout_stops_and_broadcasts_heartbeat_timeout() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::pending(), reader);

        tokio::time::pause();
        let fut = core.send_ping();
        tokio::time::advance(std::time::Duration::from_secs(SEND_TIMEOUT_SECS + 1)).await;
        let stop = fut.await;

        assert!(stop, "send_ping should return true on timeout");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::HeartbeatTimeout
        ));
    }

    #[tokio::test]
    async fn send_heartbeat_skips_when_not_logged_in() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        // logged_in defaults to false
        let stop = core.send_heartbeat().await;

        assert!(
            !stop,
            "send_heartbeat should return false when not logged in"
        );
        assert!(
            sub_rx.try_recv().is_err(),
            "no broadcast should have been sent"
        );
    }

    #[tokio::test]
    async fn send_heartbeat_skips_when_close_requested() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        core.logged_in = true;
        core.close_requested = true;
        let stop = core.send_heartbeat().await;

        assert!(
            !stop,
            "send_heartbeat should return false when close is requested"
        );
        assert!(
            sub_rx.try_recv().is_err(),
            "no broadcast should have been sent"
        );
    }

    #[tokio::test]
    async fn heartbeat_send_transport_failure_broadcasts_heartbeat_timeout() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::error(), reader);

        core.logged_in = true;
        let stop = core.send_heartbeat().await;

        assert!(stop, "send_heartbeat should return true on transport error");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(
            matches!(broadcast_msg.message, RithmicMessage::HeartbeatTimeout),
            "heartbeat send transport failure should surface as HeartbeatTimeout, got {:?}",
            broadcast_msg.message
        );
        // Still satisfies is_connection_issue() for reconnect-driving callers.
        assert!(
            broadcast_msg
                .error
                .as_ref()
                .expect("error should be set")
                .is_connection_issue()
        );
    }

    #[tokio::test]
    async fn send_heartbeat_timeout_stops_and_broadcasts_heartbeat_timeout() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::pending(), reader);

        core.logged_in = true;

        tokio::time::pause();
        let fut = core.send_heartbeat();
        tokio::time::advance(std::time::Duration::from_secs(SEND_TIMEOUT_SECS + 1)).await;
        let stop = fut.await;

        assert!(stop, "send_heartbeat should return true on timeout");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::HeartbeatTimeout
        ));
    }

    #[tokio::test]
    async fn handle_rithmic_message_close_with_close_requested_drains_silently() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        core.close_requested = true;
        let mut rx1 = register_request(&mut core, "req-1");
        let stop = core.handle_rithmic_message(Ok(Message::Close(None))).await;

        assert!(stop, "should stop when close frame received");
        // Oneshot drained with ConnectionClosed
        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::ConnectionClosed)));
        // Broadcast should be EMPTY (silent drain)
        assert!(
            sub_rx.try_recv().is_err(),
            "no broadcast should be sent on clean close"
        );
    }

    #[tokio::test]
    async fn handle_rithmic_message_close_without_close_requested_emits_and_drains() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        // close_requested defaults to false
        let mut rx1 = register_request(&mut core, "req-1");
        let stop = core.handle_rithmic_message(Ok(Message::Close(None))).await;

        assert!(stop, "should stop when unexpected close frame received");
        // Oneshot drained with ConnectionClosed
        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::ConnectionClosed)));
        // Broadcast should contain ConnectionError
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn handle_logout_sets_close_requested_before_sending() {
        // Regression guard for the disconnect race: `close_requested` must be
        // set as soon as the Logout command is dequeued so that any request
        // enqueued by a cloned handle after Logout is rejected by the
        // `handle_command` guard instead of hitting Rithmic after logout.
        let reader = make_dormant_ws_reader().await;
        let (mut core, _sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        assert!(
            !core.close_requested,
            "fresh core starts with close_requested=false"
        );

        let (tx, _rx) = oneshot::channel();
        core.handle_logout(tx).await;

        assert!(
            core.close_requested,
            "handle_logout must flip close_requested=true to close the disconnect race"
        );
    }

    #[tokio::test]
    async fn handle_rithmic_message_pong_clears_ping_manager() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, _sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        // Register a pending ping
        core.ping_manager.sent();
        assert!(core.ping_manager.next_timeout_at().is_some());

        let stop = core
            .handle_rithmic_message(Ok(Message::Pong(vec![].into())))
            .await;

        assert!(!stop, "pong should not stop the actor");
        assert!(
            core.ping_manager.next_timeout_at().is_none(),
            "ping_manager should be cleared after pong"
        );
    }

    #[tokio::test]
    async fn handle_rithmic_message_ping_with_ready_sink_sends_pong() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, _sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        let stop = core
            .handle_rithmic_message(Ok(Message::Ping(b"hello".as_ref().into())))
            .await;

        assert!(!stop, "ping with working sink should not stop actor");
        // Verify a Pong was sent
        let last_sent = core.rithmic_sender.sent_messages.last().unwrap();
        assert!(matches!(last_sent, Message::Pong(_)));
    }

    #[tokio::test]
    async fn handle_rithmic_message_ping_with_failing_sink_stops_actor() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::error(), reader);

        let stop = core
            .handle_rithmic_message(Ok(Message::Ping(b"hello".as_ref().into())))
            .await;

        assert!(stop, "ping with failing sink should stop actor");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn handle_rithmic_message_connection_closed_error_stops_actor() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        let stop = core
            .handle_rithmic_message(Err(Error::ConnectionClosed))
            .await;

        assert!(stop, "ConnectionClosed error should stop actor");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn handle_rithmic_message_already_closed_stops_actor() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let stop = core.handle_rithmic_message(Err(Error::AlreadyClosed)).await;

        assert!(stop, "AlreadyClosed error should stop actor");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn handle_rithmic_message_protocol_reset_stops_actor() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        let stop = core
            .handle_rithmic_message(Err(Error::Protocol(
                ProtocolError::ResetWithoutClosingHandshake,
            )))
            .await;

        assert!(stop, "protocol reset should stop actor");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn non_heartbeat_transport_error_still_broadcasts_connection_error() {
        // Guard against over-application of the HeartbeatTimeout relabel —
        // only ping/heartbeat SEND transport failures become HeartbeatTimeout;
        // reader-side transport errors must remain ConnectionError.
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);

        let stop = core
            .handle_rithmic_message(Err(Error::ConnectionClosed))
            .await;

        assert!(stop);
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    #[tokio::test]
    async fn rp_code_error_in_request_response_does_not_broadcast_connection_issue() {
        // Protocol rejection must route to the request handler (via oneshot),
        // not the subscription broadcast, and must not drain other pending
        // requests or trip a connection-issue event.
        use crate::rti::ResponseLogin;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let mut rx1 = register_request(&mut core, "req-1");

        let resp = ResponseLogin {
            template_id: 11,
            user_msg: vec!["req-1".to_string()],
            rp_code: vec!["3".to_string(), "some rejection".to_string()],
            ..ResponseLogin::default()
        };
        let mut payload = Vec::new();
        resp.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        // Second pending request: verifies the pool is not drained on rejection.
        let mut rx2 = register_request(&mut core, "req-2");

        let stop = core
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(!stop, "protocol rejection must not stop the actor");
        assert!(
            sub_rx.try_recv().is_err(),
            "protocol rejection must not broadcast a connection issue"
        );

        let result = rx1.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(
            &result[0].error,
            Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("some rejection")
        ));

        assert!(matches!(
            rx2.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn handle_stream_closed_stops_and_emits_connection_error() {
        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let mut rx1 = register_request(&mut core, "req-1");
        let stop = core.handle_stream_closed();

        assert!(stop, "handle_stream_closed should return true");
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::ConnectionClosed)));
    }

    /// Heartbeat success with a registered oneshot must resolve the oneshot
    /// with the original `ResponseHeartbeat` frame and must NOT broadcast any
    /// subscription update (no synthetic `HeartbeatTimeout`).
    #[tokio::test]
    async fn heartbeat_response_with_registered_oneshot_resolves_oneshot() {
        use crate::rti::ResponseHeartbeat;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut core, "hb-1");

        let resp = ResponseHeartbeat {
            template_id: 19,
            user_msg: vec!["hb-1".to_string()],
            ..ResponseHeartbeat::default()
        };
        let mut payload = Vec::new();
        resp.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = core
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(!stop, "healthy heartbeat must not stop the actor");
        assert!(
            sub_rx.try_recv().is_err(),
            "healthy heartbeat must not broadcast any subscription update"
        );

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0].message,
            RithmicMessage::ResponseHeartbeat(_)
        ));
        assert!(result[0].error.is_none());
    }

    /// Heartbeat with a populated `error` (e.g. rp_code rejection) must BOTH
    /// broadcast a synthetic `HeartbeatTimeout` update AND resolve any
    /// registered oneshot with the original `ResponseHeartbeat` frame.
    #[tokio::test]
    async fn heartbeat_response_error_broadcasts_timeout_and_resolves_oneshot() {
        use crate::rti::ResponseHeartbeat;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut core, "hb-err");

        let resp = ResponseHeartbeat {
            template_id: 19,
            user_msg: vec!["hb-err".to_string()],
            rp_code: vec!["3".to_string(), "heartbeat rejected".to_string()],
            ..ResponseHeartbeat::default()
        };
        let mut payload = Vec::new();
        resp.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = core
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(!stop, "heartbeat rejection must not stop the actor");

        // Synthetic HeartbeatTimeout broadcast on the subscription channel.
        let broadcast_msg = sub_rx.try_recv().unwrap();
        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::HeartbeatTimeout
        ));
        assert!(matches!(
            &broadcast_msg.error,
            Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("heartbeat rejected")
        ));

        // Oneshot still resolves with the original ResponseHeartbeat frame so
        // callers awaiting a ping/heartbeat request don't hang.
        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0].message,
            RithmicMessage::ResponseHeartbeat(_)
        ));
        assert!(matches!(
            &result[0].error,
            Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("heartbeat rejected")
        ));
    }

    /// Multi-part request flow: an intermediate frame (has_more = true) is
    /// accumulated on the responder; the terminal frame arrives as a rejection
    /// and MUST flush both frames to the oneshot without broadcasting on the
    /// subscription channel.
    #[tokio::test]
    async fn multipart_terminal_rejection_flushes_accumulated_frames() {
        use crate::rti::ResponseSearchSymbols;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut core, mut sub_rx) = make_test_core(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut core, "multi-1");

        // Intermediate frame: rq_handler_rp_code = ["0"] → has_more = true,
        // rp_code empty → no error.
        let intermediate = ResponseSearchSymbols {
            template_id: 110,
            user_msg: vec!["multi-1".to_string()],
            rq_handler_rp_code: vec!["0".to_string()],
            ..ResponseSearchSymbols::default()
        };
        let mut payload = Vec::new();
        intermediate.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = core
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;
        assert!(!stop);
        assert!(
            sub_rx.try_recv().is_err(),
            "intermediate multi-response frame must not broadcast"
        );

        // Terminal frame: no rq_handler_rp_code (has_more = false), rp_code
        // carries a rejection.
        let terminal = ResponseSearchSymbols {
            template_id: 110,
            user_msg: vec!["multi-1".to_string()],
            rp_code: vec!["5".to_string(), "bad".to_string()],
            ..ResponseSearchSymbols::default()
        };
        let mut payload = Vec::new();
        terminal.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = core
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;
        assert!(!stop);
        assert!(
            sub_rx.try_recv().is_err(),
            "terminal multi-response rejection must not broadcast"
        );

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 2, "both accumulated frames must be flushed");
        assert!(result[0].error.is_none());
        assert!(matches!(
            &result[1].error,
            Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("bad")
        ));
    }
}
