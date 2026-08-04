use tokio::net::TcpStream;

use super::*;
use crate::plants::test_support::{
    self, Responder, assert_close_still_sent, assert_rejected_after_close, assert_sent_while_open,
    assert_wire_silent, test_account,
};

async fn plant_with_wire() -> (PnlPlant, mpsc::Sender<PnlPlantCommand>, TcpStream) {
    test_support::plant_with_wire("pnl_plant", |core, request_receiver| PnlPlant {
        core,
        request_receiver,
    })
    .await
}

fn subscribe_pnl_updates(response_sender: Responder) -> PnlPlantCommand {
    PnlPlantCommand::SubscribePnlUpdates {
        account: test_account(),
        response_sender,
    }
}

fn position_snapshots(response_sender: Responder) -> PnlPlantCommand {
    PnlPlantCommand::GetPnlPositionSnapshot {
        account: test_account(),
        response_sender,
    }
}

#[tokio::test]
async fn subscribe_after_close_requested_is_not_sent() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_rejected_after_close(&mut plant, &mut client, subscribe_pnl_updates).await;
}

#[tokio::test]
async fn position_snapshots_after_close_requested_is_not_sent() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_rejected_after_close(&mut plant, &mut client, position_snapshots).await;
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_close_still_sent(&mut plant, PnlPlantCommand::Close, &mut client).await;
}

/// The same contract end to end through the public handle.
#[tokio::test]
async fn subscribe_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    let account = test_account();
    let handle = RithmicPnlPlantHandle {
        account: Arc::clone(&account),
        sender: command_sender,
        subscription_receiver: SubscriptionFilter::new(
            account,
            plant.core.subscription_sender.subscribe(),
        ),
    };

    let actor = tokio::spawn(async move { plant.run().await });

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.subscribe_pnl_updates(),
    )
    .await
    .expect("subscribe_pnl_updates must be answered, not left waiting")
    .expect_err("subscribe_pnl_updates must fail once close was requested");

    assert!(matches!(err, RithmicError::ConnectionClosed));
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn subscribe_is_sent_while_the_connection_is_open() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;

    assert_sent_while_open(&mut plant, &mut client, subscribe_pnl_updates).await;
}

fn test_handle() -> (RithmicPnlPlantHandle, mpsc::Receiver<PnlPlantCommand>) {
    let account = test_account();
    let (sender, command_receiver) = mpsc::channel(4);
    let (_, subscription_receiver) = broadcast::channel(4);

    let handle = RithmicPnlPlantHandle {
        account: Arc::clone(&account),
        sender,
        subscription_receiver: SubscriptionFilter::new(account, subscription_receiver),
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
            PnlPlantCommand::Logout { response_sender } => Some(response_sender),
            _ => None,
        },
        |command| matches!(command, PnlPlantCommand::Close),
    )
    .await;

    assert!(matches!(
        call.await.expect("call task panicked"),
        Err(RithmicError::SendFailed)
    ));
}
