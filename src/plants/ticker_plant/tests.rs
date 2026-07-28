use tokio::net::TcpStream;

use super::*;
use crate::{
    plants::test_support::{
        self, Responder, assert_close_still_sent, assert_rejected_after_close,
        assert_sent_while_open, assert_wire_silent,
    },
    rti::request_market_data_update::{Request, UpdateBits},
};

async fn plant_with_wire() -> (TickerPlant, mpsc::Sender<TickerPlantCommand>, TcpStream) {
    test_support::plant_with_wire("ticker_plant", |core, request_receiver| TickerPlant {
        core,
        request_receiver,
    })
    .await
}

fn subscribe(response_sender: Responder) -> TickerPlantCommand {
    TickerPlantCommand::Subscribe {
        symbol: "ESH6".to_string(),
        exchange: "CME".to_string(),
        fields: vec![UpdateBits::LastTrade],
        request_type: Request::Subscribe,
        response_sender,
    }
}

#[tokio::test]
async fn subscribe_after_close_requested_is_not_sent() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_rejected_after_close(&mut plant, &mut client, subscribe).await;
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_close_still_sent(&mut plant, TickerPlantCommand::Close, &mut client).await;
}

/// The same contract end to end through the public handle.
#[tokio::test]
async fn subscribe_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicTickerPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };

    let actor = tokio::spawn(async move { plant.run().await });

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.subscribe("ESH6", "CME"),
    )
    .await
    .expect("subscribe must be answered, not left waiting")
    .expect_err("subscribe must fail once close was requested");

    assert!(matches!(err, RithmicError::ConnectionClosed));
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn subscribe_is_sent_while_the_connection_is_open() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;

    assert_sent_while_open(&mut plant, &mut client, subscribe).await;
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
