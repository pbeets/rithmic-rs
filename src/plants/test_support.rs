//! Scaffolding shared by the plant actor tests. Compiled only under `cfg(test)`.

use futures_util::StreamExt;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::protocol::Role};

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, oneshot},
};

use crate::{
    api::receiver_api::{RithmicReceiverApi, RithmicResponse},
    config::{LoginConfig, RithmicAccount, RithmicConfig, RithmicEnv},
    error::RithmicError,
    plants::{
        actor::Plant,
        core::{Event, PlantCore},
        kind::{Cx, PlantCommand, PlantKind},
        session::Session,
    },
    request_handler::RequestResult,
    rti::{ResponseLogin, messages::RithmicMessage, request_login::SysInfraType},
};

const WIRE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const WIRE_SILENCE_WINDOW: Duration = Duration::from_millis(200);

/// The response channel every request-bearing plant command carries.
pub(crate) use crate::request_handler::Responder;

/// A plant with nothing of its own: every command it takes is one every plant
/// shares, and it loads nothing after login.
#[derive(Debug, Default)]
pub(crate) struct Bare;

impl PlantKind for Bare {
    type Command = PlantCommand;
    type Tag = Infallible;

    const SOURCE: &'static str = "test";
    const INFRA: SysInfraType = SysInfraType::TickerPlant;

    fn shared(command: PlantCommand) -> Result<PlantCommand, PlantCommand> {
        Ok(command)
    }

    fn on_command(&mut self, _command: PlantCommand, _cx: &mut Cx<'_, Infallible>) {
        unreachable!("every command a bare plant takes is shared")
    }

    fn on_reply(&mut self, tag: Infallible, _reply: RequestResult) {
        match tag {}
    }
}

pub(crate) fn test_config() -> RithmicConfig {
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

/// A plant core of kind `K`, connected and not logged in.
pub(crate) fn plant_core<K: PlantKind + Default>() -> PlantCore<K> {
    PlantCore::new(K::default(), &test_config())
}

/// What a caller waiting on `rx` has been told so far, read the way every
/// handle reads it: a dropped responder is `ConnectionClosed`. `None` while it
/// is still waiting.
pub(crate) fn answer(
    rx: &mut oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>>,
) -> Option<RequestResult> {
    match rx.try_recv() {
        Ok(reply) => Some(reply),
        Err(oneshot::error::TryRecvError::Closed) => Some(Err(RithmicError::ConnectionClosed)),
        Err(oneshot::error::TryRecvError::Empty) => None,
    }
}

/// `message` as the plant reads it off the wire.
pub(crate) fn frame(message: &impl prost::Message) -> RithmicResponse {
    let payload = message.encode_to_vec();
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend(payload);

    let receiver_api = RithmicReceiverApi {
        source: "test".to_string(),
    };

    receiver_api
        .buf_to_message(framed.into())
        .expect("a test frame decodes")
}

pub(crate) fn test_account() -> Arc<RithmicAccount> {
    Arc::new(RithmicAccount::new("FCM_A", "IB_A", "ACCOUNT_A"))
}

/// A session logged in with the default [`LoginConfig`], as a finished
/// `login()` leaves it.
pub(crate) fn logged_in_session() -> Session {
    Session::Ready {
        config: LoginConfig::default(),
        login: RithmicResponse {
            request_id: "1".to_string(),
            message: RithmicMessage::ResponseLogin(ResponseLogin {
                template_id: 11,
                user_msg: vec!["1".to_string()],
                rp_code: vec!["0".to_string()],
                ..ResponseLogin::default()
            }),
            is_update: false,
            has_more: false,
            multi_response: false,
            error: None,
            source: "test".to_string(),
        },
    }
}

/// A logged-in plant actor writing to the server half of a live loopback
/// connection, returned with its command sender and the client half so a test
/// can watch the wire.
///
/// A test that needs the plant as `connect` leaves it sets its core's
/// `session` to [`Session::Connected`].
pub(crate) async fn plant_with_wire<K: PlantKind + Default>()
-> (Plant<K>, mpsc::Sender<K::Command>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, server) =
        tokio::join!(TcpStream::connect(addr), async { listener.accept().await });
    let client = client.unwrap();
    let (server, _) = server.unwrap();

    let server_ws =
        WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(server), Role::Server, None).await;
    let (rithmic_sender, rithmic_reader) = server_ws.split();

    let (command_sender, request_receiver) = mpsc::channel(4);
    let (subscription_sender, _sub_rx) = broadcast::channel(16);

    let mut core = plant_core::<K>();
    core.session = logged_in_session();

    let plant = Plant::with_connection(
        core,
        request_receiver,
        subscription_sender,
        rithmic_sender,
        rithmic_reader,
    );

    (plant, command_sender, client)
}

/// `Close` carries no responder and must still send the close frame.
pub(crate) async fn assert_close_still_sent<K: PlantKind>(
    plant: &mut Plant<K>,
    close: K::Command,
    client: &mut TcpStream,
) {
    plant.handle(Event::Command(close)).await;

    assert_wire_wrote(client, "Close must still send the WebSocket Close frame").await;
}

/// Fails the `Logout` a `disconnect()` queued, then asserts it still queues
/// `Close`. Without that `Close` the actor is left with its session already
/// closing: no heartbeats, every later command dropped, pending requests
/// never drained.
pub(crate) async fn assert_close_follows_failed_logout<C>(
    command_receiver: &mut mpsc::Receiver<C>,
    logout_responder: impl FnOnce(C) -> Option<Responder>,
    is_close: impl FnOnce(&C) -> bool,
) {
    let logout = command_receiver
        .recv()
        .await
        .expect("disconnect must queue a command");
    let responder = logout_responder(logout).expect("disconnect must queue Logout first");
    let _ = responder.send(Err(RithmicError::SendFailed));

    let next = command_receiver
        .recv()
        .await
        .expect("disconnect must queue Close even when logout fails");

    assert!(
        is_close(&next),
        "disconnect must send Close even when logout fails"
    );
}

/// Fails if anything is written to `client` within the silence window.
pub(crate) async fn assert_wire_silent(client: &mut TcpStream) {
    let mut buf = [0u8; 128];
    let read = tokio::time::timeout(WIRE_SILENCE_WINDOW, client.read(&mut buf)).await;

    assert!(
        read.is_err(),
        "a command reached the wire that should have been refused locally: {:?}",
        read.map(|r| r.map(|n| &buf[..n]))
    );
}

/// Fails if nothing is written to `client` before the write timeout.
pub(crate) async fn assert_wire_wrote(client: &mut TcpStream, expectation: &str) {
    let mut buf = [0u8; 128];
    let read = tokio::time::timeout(WIRE_WRITE_TIMEOUT, client.read(&mut buf)).await;

    assert!(matches!(read, Ok(Ok(n)) if n > 0), "{expectation}");
}

/// Reads one frame the plant wrote and returns the protobuf inside it, so a test can
/// assert on the request itself rather than just on bytes having moved.
pub(crate) async fn read_wire_request(client: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 2];
    tokio::time::timeout(WIRE_WRITE_TIMEOUT, client.read_exact(&mut header))
        .await
        .expect("timed out waiting for the request to reach the wire")
        .expect("the connection closed before the request arrived");

    assert_eq!(header[0], 0x82, "expected one final binary frame");
    assert_eq!(header[1] & 0x80, 0, "a server frame must not be masked");

    let len = match header[1] & 0x7f {
        126 => {
            let mut extended = [0u8; 2];
            client.read_exact(&mut extended).await.unwrap();
            u16::from_be_bytes(extended) as usize
        }
        127 => panic!("a request larger than 64 KiB is not something a plant sends"),
        len => len as usize,
    };

    let mut payload = vec![0u8; len];
    client.read_exact(&mut payload).await.unwrap();

    assert!(payload.len() >= 4, "a request carries a length header");

    payload.split_off(4)
}

/// Writes one protobuf response into the plant: length header, then a masked
/// binary frame, since the plant's transport holds the server role.
pub(crate) async fn write_wire_response(client: &mut TcpStream, message: &impl prost::Message) {
    use tokio::io::AsyncWriteExt;

    let payload = message.encode_to_vec();
    let mut body = (payload.len() as u32).to_be_bytes().to_vec();
    body.extend(payload);

    let mut frame = vec![0x82];
    match body.len() {
        len if len < 126 => frame.push(0x80 | len as u8),
        len if len <= u16::MAX as usize => {
            frame.push(0x80 | 126);
            frame.extend((len as u16).to_be_bytes());
        }
        _ => panic!("a test response larger than 64 KiB is not something this helper frames"),
    }
    // An all-zero masking key, so the masked payload is the payload itself.
    frame.extend([0u8; 4]);
    frame.extend(body);

    tokio::time::timeout(WIRE_WRITE_TIMEOUT, client.write_all(&frame))
        .await
        .expect("timed out writing the response to the wire")
        .expect("the connection closed before the response was written");
}

/// Resolves a response channel the way every plant handle method does: a
/// dropped responder becomes `ConnectionClosed`. Fails rather than hangs.
pub(crate) async fn awaited_caller_outcome(
    rx: oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>>,
) -> Result<Vec<RithmicResponse>, RithmicError> {
    tokio::time::timeout(WIRE_WRITE_TIMEOUT, async {
        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    })
    .await
    .expect("the caller must be given an answer, not left waiting")
}
