//! The loop that owns a plant's WebSocket and carries out what its
//! [`PlantCore`] asks for. The [`core`](crate::plants::core) module docs walk
//! a request from a handle call to the wire and back.

use std::{collections::VecDeque, time::Duration};
use tracing::{debug, error, info, warn};

use futures_util::{
    Sink, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    time::Interval,
};

use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

use crate::{
    ConnectStrategy,
    api::receiver_api::{RithmicReceiverApi, RithmicResponse},
    config::RithmicConfig,
    error::RithmicError,
    ping_manager::PingManager,
    plants::{
        core::{Effect, Event, PlantCore},
        kind::PlantKind,
    },
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

/// The actor every Rithmic plant runs on.
///
/// The I/O around a [`PlantCore`]: the WebSocket connection, the command
/// receiver, the heartbeat and ping timers and the subscription broadcast. Its
/// loop is the only code that awaits. It turns what it reads into an
/// [`Event`], hands it to the core, and carries out the [`Effect`]s it gets
/// back, feeding how each write went back to the core. Transport details stay
/// here: pongs to the server's pings, and send timeouts.
///
/// `S` is the WebSocket sink. It defaults to [`WsSink`], the write half of a
/// real connection, and tests swap in a mock.
#[derive(Debug)]
pub(crate) struct Plant<K: PlantKind, S = WsSink> {
    pub(crate) core: PlantCore<K>,
    pub(crate) interval: Interval,
    pub(crate) ping_interval: Interval,
    pub(crate) ping_manager: PingManager,
    pub(crate) request_receiver: mpsc::Receiver<K::Command>,
    pub(crate) rithmic_reader: WsReader,
    pub(crate) rithmic_receiver_api: RithmicReceiverApi,
    pub(crate) rithmic_sender: S,
    pub(crate) subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl<K: PlantKind> Plant<K> {
    /// Connect a plant of kind `kind`, taking commands from `request_receiver`.
    pub(crate) async fn new(
        kind: K,
        request_receiver: mpsc::Receiver<K::Command>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<Plant<K>, RithmicError> {
        let ws_stream = connect_with_strategy(
            &config.url,
            &config.beta_url,
            strategy,
            config.retry_timeout,
        )
        .await
        .map_err(|e| RithmicError::ConnectionFailed(e.to_string()))?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        Ok(Plant::with_connection(
            PlantCore::new(kind, config),
            request_receiver,
            subscription_sender,
            rithmic_sender,
            rithmic_reader,
        ))
    }
}

impl<K, S> Plant<K, S>
where
    K: PlantKind,
    S: Sink<Message, Error = Error> + Unpin,
{
    /// The actor for `core`, over a connection that is already open.
    pub(crate) fn with_connection(
        core: PlantCore<K>,
        request_receiver: mpsc::Receiver<K::Command>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        rithmic_sender: S,
        rithmic_reader: WsReader,
    ) -> Self {
        Plant {
            core,
            interval: get_heartbeat_interval(None),
            ping_interval: get_ping_interval(),
            ping_manager: PingManager::new(PING_TIMEOUT_SECS),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api: RithmicReceiverApi {
                source: K::SOURCE.to_string(),
            },
            rithmic_sender,
            subscription_sender,
        }
    }

    /// Run the actor until the connection ends or it is aborted.
    pub(crate) async fn run(&mut self) {
        loop {
            let stop = match self.next_event().await {
                SelectResult::HeartbeatFired => self.handle(Event::HeartbeatDue).await,
                SelectResult::PingFired => self.handle(Event::PingDue).await,
                SelectResult::PingTimeout => self.handle(Event::PingTimedOut).await,
                SelectResult::Command(command) => self.handle(Event::Command(command)).await,
                SelectResult::RithmicMessage(msg) => self.handle_rithmic_message(msg).await,
                SelectResult::StreamClosed => self.handle(Event::StreamEnded).await,
            };

            if stop {
                break;
            }
        }
    }

    /// Hand `event` to the core and carry out what it asks. Returns `true` if
    /// the actor should stop.
    pub(crate) async fn handle(&mut self, event: Event<K::Command>) -> bool {
        let effects = self.core.on_event(event);

        self.perform(effects).await
    }

    /// Carry out `effects` in order. How a write went goes back to the core,
    /// and whatever it asks for then is done before the rest. Returns `true`
    /// if the actor should stop.
    async fn perform(&mut self, effects: Vec<Effect>) -> bool {
        let mut effects = VecDeque::from(effects);
        let mut stop = false;

        while let Some(effect) = effects.pop_front() {
            let outcome = match effect {
                Effect::Send { id, frame } => Some(self.send_request(id, frame).await),
                Effect::Heartbeat(frame) => self.send_heartbeat(frame).await,
                Effect::Ping => self.send_ping().await,

                Effect::SetHeartbeat(period) => {
                    self.interval = get_heartbeat_interval(Some(period.as_secs()));

                    None
                }

                Effect::Forward(response) => {
                    // Updates arrive many times a second: with nobody
                    // listening, say so briefly instead of dumping each one.
                    if self.subscription_sender.send(response).is_err() {
                        debug!(
                            "{}: no active subscribers, update dropped",
                            self.rithmic_receiver_api.source
                        );
                    }

                    None
                }

                Effect::Broadcast(response) => {
                    let _ = self.subscription_sender.send(response);

                    None
                }

                Effect::SendClose => {
                    self.send_close_best_effort().await;

                    None
                }

                Effect::Stop => {
                    stop = true;

                    None
                }
            };

            if let Some(event) = outcome {
                // A timed-out write poisons the sink, and the core has already
                // failed every pending request: writing the rest would only
                // block the loop for another timeout each.
                if matches!(event, Event::SendTimedOut(_)) {
                    effects.retain(|effect| !matches!(effect, Effect::Send { .. }));
                }

                for effect in self.core.on_event(event).into_iter().rev() {
                    effects.push_front(effect);
                }
            }
        }

        stop
    }

    /// Write request `id`, and say how it went.
    async fn send_request(&mut self, id: String, frame: Vec<u8>) -> Event<K::Command> {
        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Binary(frame.into()),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => Event::Sent(id),

            Err(WebSocketSendError::Transport(error)) => {
                error!(
                    "{}: WebSocket send failed for request {}: {}",
                    self.rithmic_receiver_api.source, id, error
                );

                Event::SendFailed(id)
            }

            Err(WebSocketSendError::Timeout) => {
                error!(
                    "{}: WebSocket send timed out for request {} — sink poisoned",
                    self.rithmic_receiver_api.source, id
                );

                Event::SendTimedOut(id)
            }
        }
    }

    /// Await the next thing the actor must react to.
    pub(crate) async fn next_event(&mut self) -> SelectResult<K::Command> {
        let interval = &mut self.interval;
        let ping_interval = &mut self.ping_interval;
        let ping_manager = &mut self.ping_manager;
        let receiver = &mut self.request_receiver;
        let reader = &mut self.rithmic_reader;

        tokio::select! {
            _ = interval.tick()      => SelectResult::HeartbeatFired,
            _ = ping_interval.tick() => SelectResult::PingFired,
            _ = ping_manager.timed_out() => SelectResult::PingTimeout,
            Some(cmd) = receiver.recv() => SelectResult::Command(cmd),
            msg = reader.next() => match msg {
                Some(m) => SelectResult::RithmicMessage(m),
                None => SelectResult::StreamClosed,
            },
        }
    }

    /// Send a WebSocket ping frame. A failed write means the connection is
    /// dead.
    async fn send_ping(&mut self) -> Option<Event<K::Command>> {
        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Ping(vec![].into()),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => {
                self.ping_manager.sent();

                None
            }

            Err(WebSocketSendError::Transport(error)) => {
                error!(
                    "{}: WebSocket ping send failed — connection dead: {}",
                    self.rithmic_receiver_api.source, error
                );

                // Dead link: surface as HeartbeatTimeout so reconnect callers
                // see the same signal as a true ping timeout.
                Some(Event::ConnectionLost {
                    id: "websocket_ping_send_failed",
                    error: RithmicError::HeartbeatTimeout,
                })
            }

            Err(WebSocketSendError::Timeout) => {
                error!(
                    "{}: WebSocket ping send timed out",
                    self.rithmic_receiver_api.source
                );

                Some(Event::ConnectionLost {
                    id: "websocket_ping_timeout",
                    error: RithmicError::HeartbeatTimeout,
                })
            }
        }
    }

    /// Send a Rithmic heartbeat message. A failed write means the connection
    /// is dead.
    async fn send_heartbeat(&mut self, frame: Vec<u8>) -> Option<Event<K::Command>> {
        match send_with_timeout(
            &mut self.rithmic_sender,
            Message::Binary(frame.into()),
            Duration::from_secs(SEND_TIMEOUT_SECS),
        )
        .await
        {
            Ok(()) => None,

            Err(WebSocketSendError::Transport(error)) => {
                error!(
                    "{}: heartbeat send failed — connection dead: {}",
                    self.rithmic_receiver_api.source, error
                );

                // Dead link: surface as HeartbeatTimeout (same signal as a
                // true heartbeat timeout).
                Some(Event::ConnectionLost {
                    id: "heartbeat_send_failed",
                    error: RithmicError::HeartbeatTimeout,
                })
            }

            Err(WebSocketSendError::Timeout) => {
                error!(
                    "{}: heartbeat send timed out",
                    self.rithmic_receiver_api.source
                );

                Some(Event::ConnectionLost {
                    id: "heartbeat_send_timeout",
                    error: RithmicError::HeartbeatTimeout,
                })
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

    /// Handle a raw WebSocket message. Returns `true` if the actor should stop.
    pub(crate) async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> bool {
        match message {
            Ok(Message::Close(frame)) => {
                info!(
                    "{}: received close frame: {:?}",
                    self.rithmic_receiver_api.source, frame
                );

                self.handle(Event::CloseReceived).await
            }

            Ok(Message::Pong(_)) => {
                self.ping_manager.received();

                false
            }

            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => self.handle(Event::Frame(response)).await,

                Err(err_response) => {
                    error!(
                        "{}: decode failure: {:?}",
                        self.rithmic_receiver_api.source, err_response
                    );

                    self.handle(Event::Frame(err_response)).await
                }
            },

            Ok(Message::Ping(data)) => {
                // Answer with a Pong carrying the same payload. With a split
                // stream, tungstenite only flushes its own pong when the sink
                // is polled, so send it here.
                match send_with_timeout(
                    &mut self.rithmic_sender,
                    Message::Pong(data),
                    Duration::from_secs(SEND_TIMEOUT_SECS),
                )
                .await
                {
                    Ok(()) => false,

                    Err(e) => {
                        // A ConnectionError, not HeartbeatTimeout: a pong answers
                        // the server's ping, not ours. Both pass
                        // is_connection_issue(), so reconnect logic sees either.
                        warn!(
                            "{}: failed to send pong: {:?}",
                            self.rithmic_receiver_api.source, e
                        );

                        self.handle(Event::ConnectionLost {
                            id: "",
                            error: RithmicError::ConnectionFailed(
                                "Failed to send pong — sink dead".to_string(),
                            ),
                        })
                        .await
                    }
                }
            }

            Err(
                e @ (Error::ConnectionClosed
                | Error::AlreadyClosed
                | Error::Protocol(
                    ProtocolError::ResetWithoutClosingHandshake
                    | ProtocolError::SendAfterClosing
                    | ProtocolError::ReceivedAfterClosing,
                )),
            ) => {
                error!(
                    "{}: connection closed: {}",
                    self.rithmic_receiver_api.source, e
                );

                self.handle(Event::ConnectionLost {
                    id: "",
                    error: RithmicError::ConnectionClosed,
                })
                .await
            }

            Err(Error::Io(ref io_err)) => {
                error!(
                    "{}: I/O error: {}",
                    self.rithmic_receiver_api.source, io_err
                );

                self.handle(Event::ConnectionLost {
                    id: "",
                    error: RithmicError::ConnectionFailed(format!(
                        "WebSocket I/O error: {}",
                        io_err
                    )),
                })
                .await
            }

            Err(e) => {
                error!(
                    "{}: unhandled WebSocket error, closing: {}",
                    self.rithmic_receiver_api.source, e
                );

                self.handle(Event::ConnectionLost {
                    id: "",
                    error: RithmicError::ConnectionFailed(format!("WebSocket error: {e}")),
                })
                .await
            }

            Ok(_) => {
                warn!(
                    "{}: received unhandled message type",
                    self.rithmic_receiver_api.source
                );

                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use tokio::sync::{broadcast, oneshot};
    use tokio_tungstenite::tungstenite::{Error, Message, error::ProtocolError};

    use super::*;

    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use crate::{
        api::receiver_api::RithmicResponse,
        config::{LoginConfig, RithmicEnv},
        error::RithmicError,
        plants::{
            kind::PlantCommand,
            session::Session,
            tag::Tag,
            test_support::{self, Bare},
        },
        request_handler::RequestResult,
        rti::messages::RithmicMessage,
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

    /// A reader whose peer stays connected, so polling it stays pending instead
    /// of yielding EOF. The returned socket must be held for the test's life.
    async fn make_open_ws_reader() -> (WsReader, TcpStream) {
        use tokio::net::TcpListener;
        use tokio_tungstenite::tungstenite::protocol::Role;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client_tcp, server_result) =
            tokio::join!(TcpStream::connect(addr), async { listener.accept().await });

        let client_tcp = client_tcp.unwrap();
        let (server_tcp, _) = server_result.unwrap();

        let server_ws =
            WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(server_tcp), Role::Server, None)
                .await;

        let (_, reader) = server_ws.split();

        (reader, client_tcp)
    }

    fn make_test_plant(
        sink: MockMessageSink,
        rithmic_reader: WsReader,
    ) -> (
        Plant<Bare, MockMessageSink>,
        broadcast::Receiver<RithmicResponse>,
    ) {
        let (sub_tx, sub_rx) = broadcast::channel(16);
        // No command is ever sent, so the receiver's sender can go.
        let (_, request_receiver) = mpsc::channel(1);

        let plant = Plant::with_connection(
            test_support::plant_core(),
            request_receiver,
            sub_tx,
            sink,
            rithmic_reader,
        );

        (plant, sub_rx)
    }

    fn register_request(
        plant: &mut Plant<Bare, MockMessageSink>,
        id: &str,
    ) -> oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>> {
        let (tx, rx) = oneshot::channel();

        plant
            .core
            .request_handler
            .register_request(id.to_string(), Tag::Caller(tx));

        rx
    }

    /// A write of request `id`, as the core asks for one.
    fn send(id: &str) -> Effect {
        Effect::Send {
            id: id.to_string(),
            frame: Vec::new(),
        }
    }

    /// A truncation notice for a pending replay puts `RequestResumeBars`
    /// on the wire with the notice's key, the caller keeps waiting, and the
    /// venue's real end marker resolves the reply.
    #[tokio::test]
    async fn a_truncation_notice_sends_a_resume_request_on_the_wire() {
        use crate::rti::{RequestResumeBars, ResponseVolumeProfileMinuteBars};
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = plant.core.request_handler.register_test_replay("vp-1");

        let frame_of = |message: &ResponseVolumeProfileMinuteBars| {
            let mut payload = Vec::new();
            message.encode(&mut payload).unwrap();
            let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
            framed.extend(payload);
            framed
        };

        let part = ResponseVolumeProfileMinuteBars {
            template_id: 209,
            user_msg: vec!["vp-1".to_string()],
            rq_handler_rp_code: vec!["0".to_string()],
            marker: Some(1_788_732_060),
            ..Default::default()
        };

        plant
            .handle_rithmic_message(Ok(Message::Binary(frame_of(&part).into())))
            .await;

        let notice = ResponseVolumeProfileMinuteBars {
            template_id: 209,
            user_msg: vec!["vp-1".to_string()],
            request_key: Some("0".to_string()),
            ..Default::default()
        };

        plant
            .handle_rithmic_message(Ok(Message::Binary(frame_of(&notice).into())))
            .await;

        assert!(rx.try_recv().is_err(), "the caller keeps waiting");

        let sent = plant
            .rithmic_sender
            .sent_messages
            .last()
            .expect("the resume request was sent");

        let Message::Binary(bytes) = sent else {
            panic!("a binary frame was expected, got {sent:?}");
        };

        let resume = RequestResumeBars::decode(&bytes[4..]).unwrap();
        assert_eq!(resume.template_id, 210);
        assert_eq!(resume.request_key.as_deref(), Some("0"));

        let end = ResponseVolumeProfileMinuteBars {
            template_id: 209,
            user_msg: vec!["vp-1".to_string()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        };

        plant
            .handle_rithmic_message(Ok(Message::Binary(frame_of(&end).into())))
            .await;

        let reply = rx.try_recv().unwrap().unwrap();

        assert_eq!(
            reply.len(),
            2,
            "the part and the end marker; the notice is not delivered"
        );

        assert!(!reply[0].is_truncated() && !reply[1].is_truncated());
    }

    /// A template-11 login reply for request `id`, framed as it arrives off
    /// the wire.
    fn login_reply_frame(id: &str, rp_code: &[&str]) -> Message {
        use crate::rti::ResponseLogin;
        use prost::Message as _;

        let reply = ResponseLogin {
            template_id: 11,
            user_msg: vec![id.to_string()],
            rp_code: rp_code.iter().map(|code| code.to_string()).collect(),
            heartbeat_interval: Some(30.0),
            ..Default::default()
        };

        let mut payload = Vec::new();
        reply.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        Message::Binary(framed.into())
    }

    /// Queue a login with `config`, returning its reply receiver.
    async fn login(
        plant: &mut Plant<Bare, MockMessageSink>,
        config: LoginConfig,
    ) -> oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>> {
        let (tx, rx) = oneshot::channel();
        plant
            .handle(Event::Command(PlantCommand::Login {
                config,
                response_sender: tx,
            }))
            .await;

        rx
    }

    /// The ids of the login requests the plant has written so far.
    fn sent_login_ids(plant: &Plant<Bare, MockMessageSink>) -> Vec<String> {
        use crate::rti::RequestLogin;
        use prost::Message as _;

        plant
            .rithmic_sender
            .sent_messages
            .iter()
            .filter_map(|message| match message {
                Message::Binary(bytes) => RequestLogin::decode(&bytes[4..])
                    .ok()
                    .filter(|request| request.template_id == 10),
                _ => None,
            })
            .map(|request| request.user_msg[0].clone())
            .collect()
    }

    #[tokio::test]
    async fn an_accepted_login_reply_logs_the_actor_in_on_the_server_heartbeat() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = login(&mut plant, LoginConfig::default()).await;
        let id = sent_login_ids(&plant).remove(0);

        plant
            .handle_rithmic_message(Ok(login_reply_frame(&id, &["0"])))
            .await;

        assert!(matches!(plant.core.session, Session::Ready { .. }));
        assert_eq!(plant.interval.period(), Duration::from_secs(30));

        let reply = rx.try_recv().unwrap().unwrap();
        assert!(matches!(reply[0].message, RithmicMessage::ResponseLogin(_)));
    }

    #[tokio::test]
    async fn a_replay_whose_write_fails_is_not_marked_sent() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::error(), reader);
        let mut rx = plant.core.request_handler.register_test_replay("failed");

        plant.perform(vec![send("failed")]).await;

        assert_eq!(rx.try_recv().unwrap(), Err(RithmicError::SendFailed));

        assert!(
            !plant.core.request_handler.expects_late_frames("failed"),
            "the server never saw a write that failed"
        );
    }

    #[tokio::test]
    async fn a_failed_request_write_fails_only_that_request() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::error(), reader);
        let mut rx1 = register_request(&mut plant, "req-1");
        let mut rx2 = register_request(&mut plant, "req-2");

        plant.perform(vec![send("req-1")]).await;

        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::SendFailed)));

        assert!(matches!(
            rx2.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    /// Once a write times out, the writes queued behind it are dropped: their
    /// requests are already failed, and each would block for another timeout
    /// on the poisoned sink.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_write_drops_the_writes_queued_behind_it() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::pending(), reader);
        let mut rx1 = register_request(&mut plant, "req-1");
        let mut rx2 = register_request(&mut plant, "req-2");

        let start = tokio::time::Instant::now();
        plant.perform(vec![send("req-1"), send("req-2")]).await;

        assert!(
            start.elapsed() < std::time::Duration::from_secs(2 * SEND_TIMEOUT_SECS),
            "only the first write may wait out the timeout"
        );

        assert_eq!(rx1.try_recv().unwrap(), Err(RithmicError::ConnectionClosed));
        assert_eq!(rx2.try_recv().unwrap(), Err(RithmicError::ConnectionClosed));

        let broadcast = sub_rx.try_recv().expect("the timeout is broadcast");
        assert!(matches!(broadcast.message, RithmicMessage::ConnectionError));
        assert!(broadcast.error.unwrap().is_connection_issue());
        assert!(sub_rx.try_recv().is_err(), "the timeout is broadcast once");
    }

    #[tokio::test]
    async fn send_ping_skips_when_close_requested() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        plant.core.session = Session::Closing;
        let stop = plant.handle(Event::PingDue).await;

        assert!(
            !stop,
            "send_ping should return false when close is requested"
        );

        assert!(
            plant.ping_manager.next_timeout_at().is_none(),
            "no ping should have been registered"
        );
    }

    #[tokio::test]
    async fn send_ping_success_marks_ping_manager() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let stop = plant.handle(Event::PingDue).await;

        assert!(!stop, "send_ping should return false on success");

        assert!(
            plant.ping_manager.next_timeout_at().is_some(),
            "ping_manager should track the pending ping"
        );
    }

    #[tokio::test]
    async fn ping_send_transport_failure_broadcasts_heartbeat_timeout() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::error(), reader);
        let stop = plant.handle(Event::PingDue).await;

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
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::pending(), reader);

        tokio::time::pause();
        let fut = plant.handle(Event::PingDue);
        tokio::time::advance(std::time::Duration::from_secs(SEND_TIMEOUT_SECS + 1)).await;
        let stop = fut.await;

        assert!(stop, "send_ping should return true on timeout");
        let broadcast_msg = sub_rx.try_recv().unwrap();

        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::HeartbeatTimeout
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn next_event_never_fails_a_request_that_is_still_waiting() {
        // Five heartbeat and five ping ticks, 300s of simulated time, handled
        // as `run` handles them. Only a reply, a failure or a disconnect may
        // resolve the request.
        let (reader, _peer) = make_open_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut plant, "req-1");

        for _ in 0..10 {
            let stop = match plant.next_event().await {
                SelectResult::HeartbeatFired => plant.handle(Event::HeartbeatDue).await,

                SelectResult::PingFired => {
                    let stop = plant.handle(Event::PingDue).await;

                    // The server answers every ping.
                    let pong = Ok(Message::Pong(vec![].into()));

                    plant.handle_rithmic_message(pong).await || stop
                }

                SelectResult::PingTimeout => plant.handle(Event::PingTimedOut).await,
                _ => panic!("only timers fire: no command is sent and the peer is silent"),
            };

            assert!(!stop, "the loop keeps running");
            assert!(rx.try_recv().is_err(), "the request must still be waiting");
        }
    }

    #[tokio::test]
    async fn heartbeat_send_transport_failure_broadcasts_heartbeat_timeout() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::error(), reader);

        plant.core.session = test_support::logged_in_session();
        let stop = plant.handle(Event::HeartbeatDue).await;

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
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::pending(), reader);

        plant.core.session = test_support::logged_in_session();

        tokio::time::pause();
        let fut = plant.handle(Event::HeartbeatDue);
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
    async fn a_close_command_writes_the_close_frame() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        let stop = plant.handle(Event::Command(PlantCommand::Close)).await;

        assert!(!stop, "the loop waits for the server's close echo");

        assert!(matches!(
            plant.rithmic_sender.sent_messages.as_slice(),
            [Message::Close(None)]
        ));
    }

    /// The close is best effort: a write that fails or times out is logged,
    /// and the loop still waits for the echo or the ping timeout.
    #[tokio::test(start_paused = true)]
    async fn a_close_write_that_fails_or_times_out_only_logs() {
        for sink in [MockMessageSink::error(), MockMessageSink::pending()] {
            let reader = make_dormant_ws_reader().await;
            let (mut plant, mut sub_rx) = make_test_plant(sink, reader);

            let stop = plant.handle(Event::Command(PlantCommand::Close)).await;

            assert!(!stop);
            assert!(sub_rx.try_recv().is_err(), "nothing is broadcast");
        }
    }

    #[tokio::test]
    async fn handle_rithmic_message_close_with_close_requested_drains_silently() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        plant.core.session = Session::Closing;
        let mut rx1 = register_request(&mut plant, "req-1");
        let stop = plant.handle_rithmic_message(Ok(Message::Close(None))).await;

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
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        // The session starts Connected, so no close was requested.
        let mut rx1 = register_request(&mut plant, "req-1");
        let stop = plant.handle_rithmic_message(Ok(Message::Close(None))).await;

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
    async fn handle_rithmic_message_pong_clears_ping_manager() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        // Register a pending ping
        plant.ping_manager.sent();
        assert!(plant.ping_manager.next_timeout_at().is_some());

        let stop = plant
            .handle_rithmic_message(Ok(Message::Pong(vec![].into())))
            .await;

        assert!(!stop, "pong should not stop the actor");

        assert!(
            plant.ping_manager.next_timeout_at().is_none(),
            "ping_manager should be cleared after pong"
        );
    }

    #[tokio::test]
    async fn handle_rithmic_message_ping_with_ready_sink_sends_pong() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, _sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        let stop = plant
            .handle_rithmic_message(Ok(Message::Ping(b"hello".as_ref().into())))
            .await;

        assert!(!stop, "ping with working sink should not stop actor");
        // Verify a Pong was sent
        let last_sent = plant.rithmic_sender.sent_messages.last().unwrap();
        assert!(matches!(last_sent, Message::Pong(_)));
    }

    #[tokio::test]
    async fn handle_rithmic_message_ping_with_failing_sink_stops_actor() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::error(), reader);

        let stop = plant
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
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        let stop = plant
            .handle_rithmic_message(Err(Error::ConnectionClosed))
            .await;

        assert!(stop, "ConnectionClosed error should stop actor");
        let broadcast_msg = sub_rx.try_recv().unwrap();

        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));
    }

    /// Hand the actor `error` as the reader returned it, with one request
    /// pending. Returns whether it stopped, the error subscribers were sent
    /// and the request's reply.
    async fn read_error(error: Error) -> (bool, Option<RithmicError>, RequestResult) {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut plant, "req-1");

        let stop = plant.handle_rithmic_message(Err(error)).await;

        let broadcast = sub_rx.try_recv().expect("the error is broadcast");
        assert!(matches!(broadcast.message, RithmicMessage::ConnectionError));

        (stop, broadcast.error, rx.try_recv().unwrap())
    }

    #[tokio::test]
    async fn a_reader_already_closed_error_reports_connection_closed() {
        let (stop, error, reply) = read_error(Error::AlreadyClosed).await;

        assert!(stop);
        assert_eq!(error, Some(RithmicError::ConnectionClosed));
        assert_eq!(reply, Err(RithmicError::ConnectionClosed));
    }

    #[tokio::test]
    async fn a_reader_protocol_reset_reports_connection_closed() {
        let reset = Error::Protocol(ProtocolError::ResetWithoutClosingHandshake);
        let (stop, error, reply) = read_error(reset).await;

        assert!(stop);
        assert_eq!(error, Some(RithmicError::ConnectionClosed));
        assert_eq!(reply, Err(RithmicError::ConnectionClosed));
    }

    #[tokio::test]
    async fn a_reader_io_error_reports_connection_failed() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
        let (stop, error, reply) = read_error(Error::Io(io)).await;

        assert!(stop);

        assert_eq!(
            error,
            Some(RithmicError::ConnectionFailed(
                "WebSocket I/O error: reset by peer".to_string()
            ))
        );

        assert_eq!(reply, Err(RithmicError::ConnectionClosed));
    }

    /// Any reader error without an arm of its own still ends the connection.
    #[tokio::test]
    async fn an_unexpected_reader_error_reports_connection_failed() {
        let unexpected = Error::Protocol(ProtocolError::HandshakeIncomplete);
        let (stop, error, reply) = read_error(unexpected).await;

        assert!(stop);

        assert!(
            matches!(&error, Some(RithmicError::ConnectionFailed(message))
                if message.starts_with("WebSocket error: ")),
            "got {error:?}"
        );

        assert_eq!(reply, Err(RithmicError::ConnectionClosed));
    }

    #[tokio::test]
    async fn rp_code_error_in_request_response_does_not_broadcast_connection_issue() {
        // Protocol rejection must route to the request handler (via oneshot),
        // not the subscription broadcast, and must not drain other pending
        // requests or trip a connection-issue event.
        use crate::rti::ResponseLogin;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx1 = register_request(&mut plant, "req-1");

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
        let mut rx2 = register_request(&mut plant, "req-2");

        let stop = plant
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
    async fn a_stream_end_stops_and_emits_connection_error() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx1 = register_request(&mut plant, "req-1");
        let stop = plant.handle(Event::StreamEnded).await;

        assert!(stop, "a stream end should stop the actor");
        let broadcast_msg = sub_rx.try_recv().unwrap();

        assert!(matches!(
            broadcast_msg.message,
            RithmicMessage::ConnectionError
        ));

        let result = rx1.try_recv().unwrap();
        assert!(matches!(result, Err(RithmicError::ConnectionClosed)));
    }

    /// A server-sent `ForcedLogout` (template 77) ends the session: the actor
    /// loop stops, the session is closed, pending requests resolve with an
    /// error, and subscribers get the frame followed by the `ConnectionError`
    /// event every stopping path emits.
    #[tokio::test]
    async fn forced_logout_stops_actor_and_emits_frame_then_connection_error() {
        use crate::rti::ForcedLogout;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut plant, "req-1");

        let mut payload = Vec::new();
        ForcedLogout { template_id: 77 }
            .encode(&mut payload)
            .unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = plant
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(stop, "forced logout must stop the actor loop");

        assert!(
            matches!(plant.core.session, Session::Closed),
            "forced logout must close the session"
        );

        let result = rx
            .try_recv()
            .expect("pending request must be resolved, not left hanging");
        assert!(matches!(result, Err(RithmicError::ConnectionClosed)));

        let frame_event = sub_rx.try_recv().unwrap();

        assert!(
            matches!(frame_event.message, RithmicMessage::ForcedLogout(_)),
            "the ForcedLogout frame must arrive first, got {:?}",
            frame_event.message
        );

        assert!(
            frame_event
                .error
                .as_ref()
                .expect("error should be set")
                .is_connection_issue(),
            "reconnect-driving callers must see a connection issue"
        );

        let lifecycle_event = sub_rx
            .try_recv()
            .expect("forced logout must emit the actor-lifecycle event every stopping path emits");

        assert!(
            matches!(lifecycle_event.message, RithmicMessage::ConnectionError),
            "the lifecycle event must follow the frame, got {:?}",
            lifecycle_event.message
        );
    }

    /// Encode a server-sent `RequestHeartbeat` (template 18) as a
    /// length-prefixed frame carrying `user_msg` as its correlation token.
    fn inbound_heartbeat_frame(user_msg: &str) -> Vec<u8> {
        use crate::rti::RequestHeartbeat;
        use prost::Message as _;

        let req = RequestHeartbeat {
            template_id: 18,
            user_msg: vec![user_msg.to_string()],
            ..RequestHeartbeat::default()
        };

        let mut payload = Vec::new();
        req.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);
        framed
    }

    /// A server-sent heartbeat reaches subscribers and is never answered, and
    /// its `user_msg` never resolves a pending request: that token is the
    /// server's, and the ids this client hands out are small integers, so the
    /// two can collide.
    #[tokio::test]
    async fn inbound_heartbeat_is_broadcast_but_neither_routed_nor_answered() {
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        // The probe's user_msg is deliberately the same string as the pending
        // request id registered here.
        let mut rx = register_request(&mut plant, "1");

        let stop = plant
            .handle_rithmic_message(Ok(Message::Binary(inbound_heartbeat_frame("1").into())))
            .await;

        assert!(!stop, "a heartbeat frame must not stop the actor");

        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "an inbound heartbeat must not resolve a pending request"
        );

        let broadcast_msg = sub_rx.try_recv().unwrap();

        assert!(
            matches!(broadcast_msg.message, RithmicMessage::RequestHeartbeat(_)),
            "the frame must reach subscribers as RequestHeartbeat, got {:?}",
            broadcast_msg.message
        );

        assert!(
            broadcast_msg.request_id.is_empty(),
            "the server's token must not be surfaced as a request id"
        );

        assert!(
            plant.rithmic_sender.sent_messages.is_empty(),
            "an inbound heartbeat must not be answered, got {:?}",
            plant.rithmic_sender.sent_messages
        );
    }

    /// The core never registers its heartbeats, so a heartbeat reply must not
    /// resolve a request that happens to share its id. Only a failed one is
    /// broadcast, as `HeartbeatTimeout`.
    #[tokio::test]
    async fn a_heartbeat_reply_resolves_no_request_and_only_a_failure_is_broadcast() {
        use crate::rti::ResponseHeartbeat;
        use prost::Message as _;

        for rp_code in [
            vec![],
            vec!["3".to_string(), "heartbeat rejected".to_string()],
        ] {
            let failed = !rp_code.is_empty();
            let reader = make_dormant_ws_reader().await;
            let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
            let mut rx = register_request(&mut plant, "hb-1");

            let resp = ResponseHeartbeat {
                template_id: 19,
                user_msg: vec!["hb-1".to_string()],
                rp_code,
                ..ResponseHeartbeat::default()
            };

            let mut payload = Vec::new();
            resp.encode(&mut payload).unwrap();
            let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
            framed.extend(payload);

            let stop = plant
                .handle_rithmic_message(Ok(Message::Binary(framed.into())))
                .await;

            assert!(!stop, "a heartbeat reply must not stop the actor");

            assert!(
                matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "a heartbeat reply must not resolve a pending request"
            );

            if failed {
                let broadcast_msg = sub_rx.try_recv().unwrap();

                assert!(matches!(
                    broadcast_msg.message,
                    RithmicMessage::HeartbeatTimeout
                ));

                assert!(matches!(
                    &broadcast_msg.error,
                    Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("heartbeat rejected")
                ));
            } else {
                assert!(
                    sub_rx.try_recv().is_err(),
                    "a healthy heartbeat must not be broadcast"
                );
            }
        }
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
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut plant, "multi-1");

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

        let stop = plant
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

        let stop = plant
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

    #[tokio::test]
    async fn unsolicited_reject_is_dropped() {
        // A Reject that echoes no user_msg decodes with an empty request id,
        // so nothing is waiting on it and it goes no further.
        //
        // The responder registered under that empty id is what makes the drop
        // observable: with `is_update` false, an undropped reject reaches the
        // request handler, which correlates on request id and would resolve
        // it. A real request id is a counter and never empty, so nothing else
        // can claim this responder.
        use crate::rti::Reject;
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut plant, "");

        let reject = Reject {
            template_id: 75,
            user_msg: vec![],
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
        };

        let mut payload = Vec::new();
        reject.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = plant
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(!stop, "an unsolicited reject must not stop the actor");

        assert!(
            sub_rx.try_recv().is_err(),
            "an unsolicited reject must not reach the subscription channel"
        );

        assert!(
            rx.try_recv().is_err(),
            "an unsolicited reject must not reach the request handler"
        );
    }

    /// Template 11 (`ResponseLogin`) with `template_version`'s wire type
    /// flipped to a varint. The envelope and `user_msg` stay readable.
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct MalformedResponseLogin {
        #[prost(int32, required, tag = "154467")]
        template_id: i32,
        #[prost(string, repeated, tag = "132760")]
        user_msg: Vec<String>,
        #[prost(int32, optional, tag = "153634")]
        template_version: Option<i32>,
    }

    #[tokio::test]
    async fn uncorrelatable_decode_failure_reaches_the_subscription_channel() {
        // A length-delimited field overrunning the buffer leaves no readable
        // user_msg, so there is nothing to correlate on.
        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);

        let mut framed = 2u32.to_be_bytes().to_vec();
        framed.extend_from_slice(&[0x0a, 0x05]);

        let stop = plant
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(!stop, "a decode failure must not stop the actor");

        let broadcast_msg = sub_rx
            .try_recv()
            .expect("decode failure must reach the subscription channel");

        assert!(matches!(broadcast_msg.message, RithmicMessage::Unknown));
        assert_eq!(broadcast_msg.request_id, "");

        assert!(matches!(
            &broadcast_msg.error,
            Some(RithmicError::ProtocolError(_))
        ));
    }

    #[tokio::test]
    async fn correlatable_decode_failure_resolves_the_waiting_request() {
        use prost::Message as _;

        let reader = make_dormant_ws_reader().await;
        let (mut plant, mut sub_rx) = make_test_plant(MockMessageSink::ready(), reader);
        let mut rx = register_request(&mut plant, "req-1");

        let body = MalformedResponseLogin {
            template_id: 11,
            user_msg: vec!["req-1".to_string()],
            template_version: Some(1),
        };

        let mut payload = Vec::new();
        body.encode(&mut payload).unwrap();
        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        let stop = plant
            .handle_rithmic_message(Ok(Message::Binary(framed.into())))
            .await;

        assert!(!stop, "a decode failure must not stop the actor");

        assert!(
            sub_rx.try_recv().is_err(),
            "a correlated decode failure must not broadcast"
        );

        let result = rx
            .try_recv()
            .expect("the waiting request must be resolved")
            .expect("the frame resolves the oneshot rather than failing it");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].request_id, "req-1");
        assert!(matches!(result[0].message, RithmicMessage::Unknown));

        assert!(matches!(
            &result[0].error,
            Some(RithmicError::ProtocolError(_))
        ));
    }

    /// A retry that runs out of time surfaces as `ConnectionFailed`, like a
    /// failed `Simple` attempt.
    #[tokio::test(start_paused = true)]
    async fn a_passed_retry_timeout_is_reported_as_connection_failed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://127.0.0.1:{}", listener.local_addr().unwrap().port());
        drop(listener);

        let config = RithmicConfig::builder(RithmicEnv::Demo)
            .user("test_user")
            .password("test_password")
            .url(url.clone())
            .beta_url(url)
            .app_name("test_app")
            .app_version("1.0")
            .retry_timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap();

        let (subscription_sender, _) = broadcast::channel(4);
        let (_, request_receiver) = mpsc::channel(1);

        let err = Plant::new(
            Bare,
            request_receiver,
            subscription_sender,
            &config,
            ConnectStrategy::Retry,
        )
        .await
        .expect_err("nothing listens, so the deadline must end the retry");

        match err {
            RithmicError::ConnectionFailed(message) => assert!(
                message.contains("gave up connecting after"),
                "unexpected message: {message}"
            ),
            other => panic!("expected ConnectionFailed, got {other:?}"),
        }
    }
}
