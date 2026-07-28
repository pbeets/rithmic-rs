use tokio::net::TcpStream;

use super::*;
use crate::{
    RithmicRequestError,
    api::{
        rithmic_command_types::{RithmicBracketOrder, RithmicOcoOrderLeg},
        sender_api::LoginUserType,
    },
    plants::test_support::{
        self, Responder, assert_close_still_sent, assert_rejected_after_close,
        assert_sent_while_open, assert_update_routed_to_subscribers, assert_wire_silent,
        read_wire_request, test_account,
    },
    rti::{
        AccountListUpdates, AccountRmsUpdates, BracketUpdates, ExchangeOrderNotification,
        RithmicOrderNotification, TradeRoute, UpdateEasyToBorrowList,
    },
};

fn test_handle() -> (RithmicOrderPlantHandle, mpsc::Receiver<OrderPlantCommand>) {
    let account = test_account();
    let (sender, command_receiver) = mpsc::channel(4);
    let (_, subscription_receiver) = broadcast::channel(4);

    let handle = RithmicOrderPlantHandle {
        account: account.clone(),
        login_scope: Arc::new(OnceLock::new()),
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
        login_scope: Arc::new(OnceLock::new()),
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
        login_scope: Arc::new(OnceLock::new()),
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

fn login_response() -> RithmicResponse {
    RithmicResponse {
        request_id: "1".to_string(),
        message: RithmicMessage::ResponseLogin(crate::rti::ResponseLogin {
            template_id: 11,
            rp_code: vec!["0".to_string()],
            ..crate::rti::ResponseLogin::default()
        }),
        is_update: false,
        has_more: false,
        multi_response: false,
        error: None,
        source: "order_plant".to_string(),
    }
}

/// Answer `Login` and `SetLogin`, leaving the next command for the caller.
async fn drive_login_to_login_info(command_receiver: &mut mpsc::Receiver<OrderPlantCommand>) {
    match next_command(command_receiver).await {
        OrderPlantCommand::Login {
            response_sender, ..
        } => {
            let _ = response_sender.send(Ok(vec![login_response()]));
        }
        _ => panic!("expected Login first"),
    }

    match next_command(command_receiver).await {
        OrderPlantCommand::SetLogin => {}
        _ => panic!("expected SetLogin"),
    }
}

/// An IB login. Its FCM and IB differ from the account's so a test can tell which
/// one reached the wire.
fn login_info() -> crate::rti::ResponseLoginInfo {
    crate::rti::ResponseLoginInfo {
        template_id: 301,
        fcm_id: Some("FCM_LOGIN".to_string()),
        ib_id: Some("IB_LOGIN".to_string()),
        user_type: Some(crate::rti::response_login_info::UserType::Ib.into()),
        ..crate::rti::ResponseLoginInfo::default()
    }
}

/// A template-301 response as the receiver builds it: a server rejection arrives as
/// a payload with `error` set, not as `Err`.
fn login_info_response(error: Option<RithmicError>) -> RithmicResponse {
    RithmicResponse {
        request_id: "2".to_string(),
        message: RithmicMessage::ResponseLoginInfo(login_info()),
        is_update: false,
        has_more: false,
        multi_response: false,
        error,
        source: "order_plant".to_string(),
    }
}

/// Take the next command, failing rather than hanging if none arrives.
async fn next_command(
    command_receiver: &mut mpsc::Receiver<OrderPlantCommand>,
) -> OrderPlantCommand {
    tokio::time::timeout(std::time::Duration::from_secs(5), command_receiver.recv())
        .await
        .expect("timed out waiting for a command")
        .expect("command channel closed")
}

/// Answer the `GetLoginInfo` that `login` issues with `response`.
async fn answer_login_info(
    command_receiver: &mut mpsc::Receiver<OrderPlantCommand>,
    response: Result<Vec<RithmicResponse>, RithmicError>,
) {
    match next_command(command_receiver).await {
        OrderPlantCommand::GetLoginInfo { response_sender } => {
            let _ = response_sender.send(response);
        }
        _ => panic!("login must fetch the login info that scopes get_account_list"),
    }
}

/// Neither failure shape may fail the login or leave a scope behind.
#[tokio::test]
async fn login_succeeds_and_stays_unscoped_when_the_login_info_fails() {
    for response in [
        Ok(vec![login_info_response(Some(
            RithmicError::RequestRejected(RithmicRequestError {
                rp_code: vec!["5".to_string(), "denied".to_string()],
                code: Some("5".to_string()),
                message: Some("denied".to_string()),
            }),
        ))]),
        Err(RithmicError::EmptyResponse),
    ] {
        let (handle, mut command_receiver) = test_handle();

        let driver = async {
            drive_login_to_login_info(&mut command_receiver).await;
            answer_login_info(&mut command_receiver, response).await;
        };

        let (login, ()) = tokio::join!(handle.login(), driver);
        assert!(login.is_ok(), "a failed login info must not fail the login");

        assert!(
            handle.login_scope.get().is_none(),
            "a failed login info must not scope"
        );
    }
}

/// The scope belongs to the connection, not to the handle that logged in — the
/// README's multi-account flow takes a further handle per account after logging in
/// on one, and those must be scoped too.
#[tokio::test]
async fn every_handle_from_one_plant_shares_the_login_scope() {
    let (sender, mut command_receiver) = mpsc::channel(4);
    let (subscription_sender, _keep_open) = broadcast::channel(4);

    let plant = RithmicOrderPlant {
        connection_handle: tokio::spawn(async {}),
        sender,
        subscription_sender,
        login_scope: Arc::new(OnceLock::new()),
    };

    let logs_in = plant.get_handle(&test_account());
    let never_logs_in = plant.get_handle(&RithmicAccount::new("FCM_B", "IB_B", "ACCOUNT_B"));

    let driver = async {
        drive_login_to_login_info(&mut command_receiver).await;
        answer_login_info(&mut command_receiver, Ok(vec![login_info_response(None)])).await;
    };

    let (login, ()) = tokio::join!(logs_in.login(), driver);
    assert!(login.is_ok());

    let scope = never_logs_in
        .login_scope
        .get()
        .expect("a handle that never logged in must still be scoped");

    assert_eq!(scope.fcm_id.as_deref(), Some("FCM_LOGIN"));
    assert_eq!(scope.ib_id.as_deref(), Some("IB_LOGIN"));
    assert_eq!(scope.user_type, LoginUserType::Ib);
}

/// A plant actor whose connection has logged in, as `login()` would leave it.
async fn scoped_plant_with_wire() -> (OrderPlant, mpsc::Sender<OrderPlantCommand>, TcpStream) {
    let (plant, sender, client) = plant_with_wire().await;

    plant
        .login_scope
        .set(LoginScope::from_login_info(&login_info()).expect("an IB login is expressible"))
        .expect("a fresh cell is empty");

    (plant, sender, client)
}

/// Feeds one command to the actor and decodes the request it put on the wire.
async fn sent_request<M: prost::Message + Default>(
    plant: &mut OrderPlant,
    client: &mut TcpStream,
    build: impl FnOnce(Responder) -> OrderPlantCommand,
) -> M {
    let (response_sender, _rx) = oneshot::channel();
    plant.handle_command(build(response_sender)).await;

    M::decode(&*read_wire_request(client).await).expect("the actor serialized this request")
}

fn bracket_order() -> RithmicBracketOrder {
    RithmicBracketOrder {
        action: crate::rti::request_bracket_order::TransactionType::Buy,
        duration: crate::rti::request_bracket_order::Duration::Day,
        exchange: "CME".to_string(),
        localid: "bracket-1".to_string(),
        price_type: crate::rti::request_bracket_order::PriceType::Limit,
        price: Some(5000.0),
        profit_ticks: 20,
        quantity: 1,
        stop_ticks: 10,
        symbol: "ESM6".to_string(),
    }
}

/// 302 and 304 name no account, so the login scopes them outright; 330 and 346 name
/// one and take only the user type.
#[tokio::test]
async fn a_logged_in_actor_scopes_every_request_that_carries_a_user_type() {
    let (mut plant, _sender, mut client) = scoped_plant_with_wire().await;

    let account_list: crate::rti::RequestAccountList =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::AccountList { response_sender }
        })
        .await;

    assert_eq!(account_list.fcm_id.as_deref(), Some("FCM_LOGIN"));
    assert_eq!(account_list.ib_id.as_deref(), Some("IB_LOGIN"));
    assert_eq!(
        account_list.user_type,
        Some(crate::rti::request_account_list::UserType::Ib.into())
    );

    let rms_info: crate::rti::RequestAccountRmsInfo =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::GetAccountRmsInfo {
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(rms_info.fcm_id.as_deref(), Some("FCM_LOGIN"));
    assert_eq!(rms_info.ib_id.as_deref(), Some("IB_LOGIN"));
    assert_eq!(
        rms_info.user_type,
        Some(crate::rti::request_account_rms_info::UserType::Ib.into())
    );

    let bracket: crate::rti::RequestBracketOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order: bracket_order(),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(bracket.fcm_id.as_deref(), Some("FCM_A"));
    assert_eq!(bracket.account_id.as_deref(), Some("ACCOUNT_A"));
    assert_eq!(
        bracket.user_type,
        Some(crate::rti::request_bracket_order::UserType::Ib.into())
    );

    let cancel_all: crate::rti::RequestCancelAllOrders =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::CancelAllOrders {
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(cancel_all.account_id.as_deref(), Some("ACCOUNT_A"));
    assert_eq!(
        cancel_all.user_type,
        Some(crate::rti::request_cancel_all_orders::UserType::Ib.into())
    );
}

/// Every template the order plant receives unsolicited, i.e. every one the
/// receiver API marks `is_update`, belongs on the subscription broadcast and
/// must never reach the request handler.
mod update_routing {
    use super::*;

    /// Template 350 — trade route availability.
    #[tokio::test]
    async fn trade_route_reaches_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            TradeRoute {
                template_id: 350,
                exchange: Some("CME".to_string()),
                trade_route: Some("globex".to_string()),
                ..TradeRoute::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::TradeRoute(update)
                        if update.trade_route.as_deref() == Some("globex")
                )
            },
        )
        .await;
    }

    /// Template 351 — Rithmic-side order notification.
    #[tokio::test]
    async fn rithmic_order_notification_reaches_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            RithmicOrderNotification {
                template_id: 351,
                account_id: Some("ACCOUNT_A".to_string()),
                basket_id: Some("basket-1".to_string()),
                ..RithmicOrderNotification::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::RithmicOrderNotification(update)
                        if update.basket_id.as_deref() == Some("basket-1")
                )
            },
        )
        .await;
    }

    /// Template 352 — exchange-side order notification.
    #[tokio::test]
    async fn exchange_order_notification_reaches_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            ExchangeOrderNotification {
                template_id: 352,
                account_id: Some("ACCOUNT_A".to_string()),
                exchange_order_id: Some("exch-1".to_string()),
                ..ExchangeOrderNotification::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::ExchangeOrderNotification(update)
                        if update.exchange_order_id.as_deref() == Some("exch-1")
                )
            },
        )
        .await;
    }

    /// Template 353 — bracket target/stop updates.
    #[tokio::test]
    async fn bracket_updates_reach_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            BracketUpdates {
                template_id: 353,
                account_id: Some("ACCOUNT_A".to_string()),
                target_ticks: Some(8),
                ..BracketUpdates::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::BracketUpdates(update) if update.target_ticks == Some(8)
                )
            },
        )
        .await;
    }

    /// Template 354 — account list updates.
    #[tokio::test]
    async fn account_list_updates_reach_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            AccountListUpdates {
                template_id: 354,
                account_id: Some("ACCOUNT_A".to_string()),
                ..AccountListUpdates::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::AccountListUpdates(update)
                        if update.account_id.as_deref() == Some("ACCOUNT_A")
                )
            },
        )
        .await;
    }

    /// Template 355 — easy-to-borrow list updates.
    #[tokio::test]
    async fn update_easy_to_borrow_list_reaches_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            UpdateEasyToBorrowList {
                template_id: 355,
                symbol: Some("ESM6".to_string()),
                ..UpdateEasyToBorrowList::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::UpdateEasyToBorrowList(update)
                        if update.symbol.as_deref() == Some("ESM6")
                )
            },
        )
        .await;
    }

    /// Template 356 — account RMS updates, including auto-liquidation.
    #[tokio::test]
    async fn account_rms_updates_reach_subscribers() {
        assert_update_routed_to_subscribers(
            "order_plant",
            AccountRmsUpdates {
                template_id: 356,
                account_id: Some("ACCOUNT_A".to_string()),
                auto_liq_threshold_current_value: Some("1000".to_string()),
                ..AccountRmsUpdates::default()
            },
            |message| {
                matches!(
                    message,
                    RithmicMessage::AccountRmsUpdates(update)
                        if update.auto_liq_threshold_current_value.as_deref() == Some("1000")
                )
            },
        )
        .await;
    }
}
