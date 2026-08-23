//! Scaffolding shared by the plant actor tests. Compiled only under `cfg(test)`.

use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::protocol::Role};

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, oneshot},
};

use crate::{
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    config::{RithmicAccount, RithmicConfig, RithmicEnv},
    error::RithmicError,
    ping_manager::PingManager,
    plants::core::{PlantActor, PlantCore},
    request_handler::RithmicRequestHandler,
    ws::{PING_TIMEOUT_SECS, get_heartbeat_interval, get_ping_interval},
};

const WIRE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const WIRE_SILENCE_WINDOW: Duration = Duration::from_millis(200);

/// The response channel every request-bearing plant command carries.
pub(crate) type Responder = oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>;

pub(crate) fn test_account() -> Arc<RithmicAccount> {
    Arc::new(RithmicAccount::new("FCM_A", "IB_A", "ACCOUNT_A"))
}

/// A logged-in `PlantCore` writing to the server half of a live loopback
/// connection, returned with the client half so a test can watch the wire.
pub(crate) async fn core_with_wire(source: &str) -> (PlantCore, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, server) =
        tokio::join!(TcpStream::connect(addr), async { listener.accept().await });
    let client = client.unwrap();
    let (server, _) = server.unwrap();

    let server_ws =
        WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(server), Role::Server, None).await;
    let (rithmic_sender, rithmic_reader) = server_ws.split();

    let config = RithmicConfig::builder(RithmicEnv::Demo)
        .user("test_user")
        .password("test_password")
        .url("ws://localhost:9999")
        .beta_url("ws://localhost:9998")
        .app_name("test_app")
        .app_version("1.0")
        .build()
        .unwrap();

    let (subscription_sender, _sub_rx) = broadcast::channel(16);
    let rithmic_sender_api = RithmicSenderApi::new(&config);

    let request_handler = RithmicRequestHandler::new();

    let core = PlantCore {
        config,
        close_requested: false,
        interval: get_heartbeat_interval(None),
        logged_in: true,
        ping_interval: get_ping_interval(),
        ping_manager: PingManager::new(PING_TIMEOUT_SECS),
        request_handler,
        rithmic_reader,
        rithmic_receiver_api: RithmicReceiverApi {
            source: source.to_string(),
        },
        rithmic_sender,
        rithmic_sender_api,
        subscription_sender,
    };

    (core, client)
}

/// A plant actor built on `core_with_wire`, returned with its command sender and
/// the client half of the socket.
pub(crate) async fn plant_with_wire<P, C>(
    source: &str,
    build: impl FnOnce(PlantCore, mpsc::Receiver<C>) -> P,
) -> (P, mpsc::Sender<C>, TcpStream) {
    let (core, client) = core_with_wire(source).await;
    let (command_sender, request_receiver) = mpsc::channel(4);

    (build(core, request_receiver), command_sender, client)
}

/// Feeds a request to a plant whose close is already requested: it must put no
/// bytes on the wire, and its caller must be answered `ConnectionClosed`.
pub(crate) async fn assert_rejected_after_close<P: PlantActor>(
    plant: &mut P,
    client: &mut TcpStream,
    build: impl FnOnce(Responder) -> P::Command,
) {
    let (tx, rx) = oneshot::channel();
    plant.handle_command(build(tx)).await;

    assert_wire_silent(client).await;
    assert!(matches!(
        awaited_caller_outcome(rx).await,
        Err(RithmicError::ConnectionClosed)
    ));
}

/// `Close` carries no responder and must still reach `handle_close()`.
pub(crate) async fn assert_close_still_sent<P: PlantActor>(
    plant: &mut P,
    close: P::Command,
    client: &mut TcpStream,
) {
    plant.handle_command(close).await;

    assert_wire_wrote(client, "Close must still send the WebSocket Close frame").await;
}

/// Fails the `Logout` a `disconnect()` queued, then asserts it still queues
/// `Close`. Without that `Close` the actor is left with `close_requested`
/// already set: no heartbeats, every later command dropped, pending requests
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

/// Positive control — the same request on an open connection does reach the
/// wire, so a silent wire above is evidence rather than a blind harness.
pub(crate) async fn assert_sent_while_open<P: PlantActor>(
    plant: &mut P,
    client: &mut TcpStream,
    build: impl FnOnce(Responder) -> P::Command,
) {
    let (tx, _rx) = oneshot::channel();
    plant.handle_command(build(tx)).await;

    assert_wire_wrote(
        client,
        "an open connection must still serialize the request",
    )
    .await;
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
