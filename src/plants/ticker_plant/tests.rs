use super::*;
use prost::Message as _;
use tokio::net::TcpStream;

use crate::{
    plants::{
        core::Event,
        session::Session,
        test_support::{
            self, Responder, assert_close_still_sent, assert_wire_silent, read_wire_request,
            write_wire_response,
        },
    },
    rti::{
        RequestMarketDataUpdate,
        request_market_data_update::{Request, UpdateBits},
    },
};

async fn plant_with_wire() -> (
    Plant<TickerPlant>,
    mpsc::Sender<TickerPlantCommand>,
    TcpStream,
) {
    test_support::plant_with_wire().await
}

fn subscribe(response_sender: Responder) -> TickerPlantCommand {
    TickerPlantCommand::Subscribe {
        symbol: "ESZ6".to_string(),
        exchange: "CME".to_string(),
        fields: vec![UpdateBits::LastTrade],
        request_type: Request::Subscribe,
        response_sender,
    }
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    assert_close_still_sent(&mut plant, TickerPlantCommand::Close, &mut client).await;
}

#[tokio::test]
async fn subscribe_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    let subscription_sender = plant.subscription_sender.clone();

    let handle = RithmicTickerPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };

    let actor = tokio::spawn(async move { plant.run().await });

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.subscribe("ESZ6", "CME"),
    )
    .await
    .expect("subscribe must be answered, not left waiting")
    .expect_err("subscribe must fail once close was requested");

    assert!(matches!(err, RithmicError::ConnectionClosed));
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

/// Also the control for the silent wire above: an open plant does write.
#[tokio::test]
async fn subscribe_sends_the_symbol_and_fields_while_the_connection_is_open() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;

    plant
        .handle(Event::Command(subscribe(oneshot::channel().0)))
        .await;

    let request =
        RequestMarketDataUpdate::decode(read_wire_request(&mut client).await.as_slice()).unwrap();

    assert_eq!(request.template_id, 100);
    assert_eq!(request.symbol.as_deref(), Some("ESZ6"));
    assert_eq!(request.exchange.as_deref(), Some("CME"));
    assert_eq!(request.request, Some(Request::Subscribe as i32));
    assert_eq!(request.update_bits, Some(UpdateBits::LastTrade as u32));
}

fn test_handle() -> (RithmicTickerPlantHandle, mpsc::Receiver<TickerPlantCommand>) {
    let (sender, command_receiver) = mpsc::channel(4);
    let (subscription_sender, subscription_receiver) = broadcast::channel(4);

    let handle = RithmicTickerPlantHandle {
        sender,
        subscription_receiver,
        subscription_sender,
    };

    (handle, command_receiver)
}

#[tokio::test]
async fn disconnect_sends_close_even_when_logout_fails() {
    let (handle, mut command_receiver) = test_handle();
    let call = tokio::spawn(async move { handle.disconnect().await });

    test_support::assert_close_follows_failed_logout(
        &mut command_receiver,
        |command| match command {
            TickerPlantCommand::Logout { response_sender } => Some(response_sender),
            _ => None,
        },
        |command| matches!(command, TickerPlantCommand::Close),
    )
    .await;

    assert!(matches!(
        call.await.expect("call task panicked"),
        Err(RithmicError::SendFailed)
    ));
}

/// A caller that gives up on `login()` — say under `tokio::time::timeout` —
/// once the request is on the wire must still leave a session that heartbeats:
/// the actor learns it is logged in from the reply itself.
#[tokio::test]
async fn a_login_whose_caller_stops_waiting_still_heartbeats() {
    use crate::rti::{RequestHeartbeat, RequestLogin, ResponseLogin};

    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Connected;

    let subscription_sender = plant.subscription_sender.clone();

    let handle = RithmicTickerPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };

    let actor = tokio::spawn(async move { plant.run().await });

    let request = {
        let login = handle.login();
        tokio::pin!(login);

        tokio::select! {
            _ = &mut login => panic!("login cannot finish before it is answered"),
            request = read_wire_request(&mut client) => request,
        }
        // The login future is dropped here, before the reply is written.
    };

    let request = RequestLogin::decode(request.as_slice()).unwrap();
    assert_eq!(request.template_id, 10);

    write_wire_response(
        &mut client,
        &ResponseLogin {
            template_id: 11,
            user_msg: request.user_msg,
            rp_code: vec!["0".to_string()],
            heartbeat_interval: Some(1.0),
            ..Default::default()
        },
    )
    .await;

    let heartbeat = RequestHeartbeat::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the actor must heartbeat once logged in");

    assert_eq!(heartbeat.template_id, 18);

    handle.abort();
    let _ = actor.await;
}
