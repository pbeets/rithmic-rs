use tokio::net::TcpStream;

use super::*;
use crate::{
    api::rithmic_command_types::RithmicOcoOrderLeg,
    plants::test_support::{
        self, Responder, assert_close_still_sent, assert_rejected_after_close,
        assert_sent_while_open, assert_wire_silent, test_account,
    },
};

fn test_handle() -> (RithmicOrderPlantHandle, mpsc::Receiver<OrderPlantCommand>) {
    let account = test_account();
    let (sender, command_receiver) = mpsc::channel(4);
    let (_, subscription_receiver) = broadcast::channel(4);

    let handle = RithmicOrderPlantHandle {
        account: account.clone(),
        sender,
        subscription_receiver: SubscriptionFilter::new(account, subscription_receiver),
    };

    (handle, command_receiver)
}

fn leg(tag: &str) -> RithmicOcoOrderLeg {
    RithmicOcoOrderLeg {
        symbol: "ESM6".to_string(),
        exchange: "CME".to_string(),
        quantity: 1,
        price: 5000.0,
        trigger_price: None,
        transaction_type: crate::rti::request_oco_order::TransactionType::Buy,
        duration: crate::rti::request_oco_order::Duration::Day,
        price_type: crate::rti::request_oco_order::PriceType::Limit,
        user_tag: tag.to_string(),
        trailing_stop: None,
    }
}

async fn plant_with_wire() -> (OrderPlant, mpsc::Sender<OrderPlantCommand>, TcpStream) {
    test_support::plant_with_wire("order_plant", |core, request_receiver| OrderPlant {
        core,
        request_receiver,
    })
    .await
}

fn place_order(response_sender: Responder) -> OrderPlantCommand {
    OrderPlantCommand::PlaceOrder {
        order: RithmicOrder::default(),
        account: test_account(),
        response_sender,
    }
}

fn cancel_order(response_sender: Responder) -> OrderPlantCommand {
    OrderPlantCommand::CancelOrder {
        order_id: "basket-1".to_string(),
        account: test_account(),
        response_sender,
    }
}

#[tokio::test]
async fn place_oco_order_multi_rejects_fewer_than_two_legs() {
    for legs in [vec![], vec![leg("only")]] {
        let (handle, mut command_receiver) = test_handle();

        // No actor is running, so without the guard this parks forever; the
        // timeout turns that into a failure rather than a hung suite.
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.place_oco_order_multi(legs),
        )
        .await
        .expect("must be rejected without reaching the actor")
        .expect_err("fewer than two legs must be rejected");

        assert!(matches!(err, RithmicError::InvalidArgument(_)));
        // Rejected before reaching the actor, so nothing was queued.
        assert!(command_receiver.try_recv().is_err());
    }
}

#[tokio::test]
async fn place_oco_order_multi_forwards_two_or_more_legs() {
    let (handle, mut command_receiver) = test_handle();

    // The call parks on its response channel until the actor answers, so it
    // has to run alongside the receive below rather than before it.
    let call = tokio::spawn(async move {
        handle
            .place_oco_order_multi(vec![leg("a"), leg("b"), leg("c")])
            .await
    });

    match command_receiver.recv().await {
        Some(OrderPlantCommand::PlaceOcoOrderMulti { legs, .. }) => {
            assert_eq!(legs.len(), 3);
            assert_eq!(legs[2].user_tag, "c");
            // Dropping the command drops the responder, which unparks the call.
        }
        _ => panic!("expected PlaceOcoOrderMulti to be queued"),
    }

    assert!(matches!(
        call.await.expect("call task panicked"),
        Err(RithmicError::ConnectionClosed)
    ));
}

#[tokio::test]
async fn place_order_after_close_requested_is_not_sent() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_rejected_after_close(&mut plant, &mut client, place_order).await;
}

#[tokio::test]
async fn cancel_order_after_close_requested_is_not_sent() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_rejected_after_close(&mut plant, &mut client, cancel_order).await;
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_close_still_sent(&mut plant, OrderPlantCommand::Close, &mut client).await;
}

/// The contract that matters: the order must not go live at the exchange while
/// its caller records a failure.
#[tokio::test]
async fn place_order_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    let account = test_account();
    let handle = RithmicOrderPlantHandle {
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
        handle.place_order(RithmicOrder::default()),
    )
    .await
    .expect("place_order must be answered, not left waiting")
    .expect_err("place_order must fail once close was requested");

    assert!(matches!(err, RithmicError::ConnectionClosed));
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn place_order_is_sent_while_the_connection_is_open() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;

    assert_sent_while_open(&mut plant, &mut client, place_order).await;
}

#[tokio::test]
async fn disconnect_sends_close_even_when_logout_fails() {
    let (handle, mut command_receiver) = test_handle();
    let call = tokio::spawn(async move { handle.disconnect().await });

    test_support::assert_close_follows_failed_logout(
        &mut command_receiver,
        |command| match command {
            OrderPlantCommand::Logout { response_sender } => Some(response_sender),
            _ => None,
        },
        |command| matches!(command, OrderPlantCommand::Close),
    )
    .await;

    assert!(matches!(
        call.await.expect("call task panicked"),
        Err(RithmicError::SendFailed)
    ));
}
