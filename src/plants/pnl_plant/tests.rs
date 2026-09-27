use prost::Message as _;
use tokio::net::TcpStream;

use super::*;
use crate::{
    plants::{
        core::Event,
        session::Session,
        test_support::{
            self, Responder, assert_close_still_sent, assert_wire_silent, read_wire_request,
            test_account,
        },
    },
    rti::{RequestPnLPositionUpdates, request_pn_l_position_updates},
};

async fn plant_with_wire() -> (Plant<PnlPlant>, mpsc::Sender<PnlPlantCommand>, TcpStream) {
    test_support::plant_with_wire().await
}

fn subscribe_pnl_updates(response_sender: Responder) -> PnlPlantCommand {
    PnlPlantCommand::SubscribePnlUpdates {
        account: test_account(),
        response_sender,
    }
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    assert_close_still_sent(&mut plant, PnlPlantCommand::Close, &mut client).await;
}

#[tokio::test]
async fn subscribe_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    let account = test_account();
    let handle = RithmicPnlPlantHandle {
        account: Arc::clone(&account),
        sender: command_sender,
        subscription_receiver: SubscriptionFilter::new(
            account,
            plant.subscription_sender.subscribe(),
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

/// Also the control for the silent wire above: an open plant does write.
#[tokio::test]
async fn subscribe_sends_the_account_while_the_connection_is_open() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant
        .handle(Event::Command(subscribe_pnl_updates(oneshot::channel().0)))
        .await;

    let request =
        RequestPnLPositionUpdates::decode(read_wire_request(&mut client).await.as_slice()).unwrap();
    assert_eq!(request.template_id, 400);
    assert_eq!(
        request.request,
        Some(request_pn_l_position_updates::Request::Subscribe as i32)
    );
    assert_eq!(request.fcm_id.as_deref(), Some("FCM_A"));
    assert_eq!(request.ib_id.as_deref(), Some("IB_A"));
    assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_A"));
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

#[tokio::test]
async fn subscribe_all_retains_every_account() {
    let (sender, _rx) = mpsc::channel(4);
    let (subscription_sender, _) = broadcast::channel(4);
    let plant = RithmicPnlPlant {
        sender,
        subscription_sender,
        connection_handle: tokio::spawn(async {}),
    };
    let mut receiver = plant.subscribe_all();
    for account in ["account-a", "account-b"] {
        let update = crate::rti::InstrumentPnLPositionUpdate {
            account_id: Some(account.into()),
            ..Default::default()
        };
        plant
            .subscription_sender
            .send(RithmicResponse {
                request_id: String::new(),
                source: "pnl_plant".into(),
                message: RithmicMessage::InstrumentPnLPositionUpdate(update),
                is_update: true,
                has_more: false,
                multi_response: false,
                error: None,
            })
            .unwrap();
        let RithmicMessage::InstrumentPnLPositionUpdate(update) =
            receiver.recv().await.unwrap().message
        else {
            panic!("missing instrument update")
        };
        assert_eq!(update.account_id.as_deref(), Some(account));
    }
    plant.connection_handle.await.unwrap();
}
