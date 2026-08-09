use tokio::net::TcpStream;

use super::*;
use crate::{
    RithmicRequestError,
    api::{
        commands::{RithmicBracketOrder, RithmicOcoOrderLeg},
        sender_api::LoginUserType,
    },
    plants::test_support::{
        self, Responder, assert_close_still_sent, assert_rejected_after_close,
        assert_sent_while_open, assert_wire_silent, awaited_caller_outcome, read_wire_request,
        test_account,
    },
    types::{ManualOrAutoEntry, OrderSide, OrderType, TimeInForce},
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

fn adjustment(id: &str, ticks: i32, level: Option<i32>) -> RithmicBracketLevelAdjustment {
    RithmicBracketLevelAdjustment {
        id: id.to_string(),
        ticks,
        level,
    }
}

fn leg(tag: &str) -> RithmicOcoOrderLeg {
    RithmicOcoOrderLeg {
        manual_or_auto: ManualOrAutoEntry::Auto,
        symbol: "ESM6".to_string(),
        exchange: "CME".to_string(),
        quantity: 1,
        price: Some(5000.0),
        trigger_price: None,
        transaction_type: OrderSide::Buy,
        duration: TimeInForce::Day,
        price_type: OrderType::Limit,
        user_tag: tag.to_string(),
        trailing_stop: None,
        trade_route: None,
        ..Default::default()
    }
}

async fn plant_with_wire() -> (OrderPlant, mpsc::Sender<OrderPlantCommand>, TcpStream) {
    test_support::plant_with_wire("order_plant", |core, request_receiver| OrderPlant {
        core,
        request_receiver,
        login_scope: Arc::new(OnceLock::new()),
        trade_routes: TradeRouteCache::default(),
    })
    .await
}

/// Carries an explicit route, so a silent wire below is the close guard rather
/// than an unroutable order.
fn place_order(response_sender: Responder) -> OrderPlantCommand {
    OrderPlantCommand::PlaceOrder {
        order: RithmicOrder {
            trade_route: Some("globex".to_string()),
            ..RithmicOrder::default()
        },
        account: test_account(),
        response_sender,
    }
}

fn cancel_order(response_sender: Responder) -> OrderPlantCommand {
    OrderPlantCommand::CancelOrder {
        order: RithmicCancelOrder::new()
            .id("basket-1")
            .build()
            .expect("valid cancellation"),
        account: test_account(),
        response_sender,
    }
}

#[tokio::test]
async fn place_oco_order_rejects_fewer_than_two_legs() {
    for legs in [vec![], vec![leg("only")]] {
        let (handle, mut command_receiver) = test_handle();

        // No actor is running, so without the guard this parks forever; the
        // timeout turns that into a failure rather than a hung suite.
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.place_oco_order(RithmicOcoOrder {
                legs,
                ..Default::default()
            }),
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
async fn show_fill_history_rejects_a_record_cap_rithmic_would_refuse() {
    for count in [10_001, -1] {
        let (handle, mut command_receiver) = test_handle();

        // No actor is running, so without the guard this parks forever; the
        // timeout turns that into a failure rather than a hung suite.
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.show_fill_history(
                crate::types::FillHistoryRange::Ssboe {
                    start: 0,
                    finish: 1,
                },
                Some(count),
            ),
        )
        .await
        .expect("must be rejected without reaching the actor")
        .expect_err("an out-of-range record cap must be rejected");

        assert!(matches!(err, RithmicError::InvalidArgument(_)));
        // Rejected before reaching the actor, so nothing was queued.
        assert!(command_receiver.try_recv().is_err());
    }
}

#[tokio::test]
async fn place_oco_order_forwards_two_or_more_legs() {
    let (handle, mut command_receiver) = test_handle();

    // The call parks on its response channel until the actor answers, so it
    // has to run alongside the receive below rather than before it.
    let call = tokio::spawn(async move {
        handle
            .place_oco_order(
                RithmicOcoOrder::new()
                    .legs([leg("a"), leg("b"), leg("c")])
                    .build()
                    .expect("valid oco group"),
            )
            .await
    });

    match command_receiver.recv().await {
        Some(OrderPlantCommand::PlaceOcoOrder { order, .. }) => {
            assert_eq!(order.legs.len(), 3);
            assert_eq!(order.legs[2].user_tag, "c");
            // Dropping the command drops the responder, which unparks the call.
        }
        _ => panic!("expected PlaceOcoOrder to be queued"),
    }

    assert!(matches!(
        call.await.expect("call task panicked"),
        Err(RithmicError::ConnectionClosed)
    ));
}

/// Both calls park on their response channels, so they run alongside the
/// receives below. Dropping each command drops its responder and unparks one.
#[tokio::test]
async fn adjust_target_and_stop_forward_the_bracket_level() {
    let (handle, mut command_receiver) = test_handle();

    let call = tokio::spawn(async move {
        let _ = handle
            .adjust_target(adjustment("basket-1", 16, Some(2)))
            .await;
        let _ = handle.adjust_stop(adjustment("basket-2", 8, None)).await;
    });

    match command_receiver.recv().await {
        Some(OrderPlantCommand::ModifyTarget { adjustment, .. }) => {
            assert_eq!(adjustment.id, "basket-1");
            assert_eq!(adjustment.ticks, 16);
            assert_eq!(adjustment.level, Some(2));
        }
        _ => panic!("expected ModifyTarget to be queued"),
    }

    match command_receiver.recv().await {
        Some(OrderPlantCommand::ModifyStop { adjustment, .. }) => {
            assert_eq!(adjustment.id, "basket-2");
            assert_eq!(adjustment.ticks, 8);
            assert_eq!(adjustment.level, None);
        }
        _ => panic!("expected ModifyStop to be queued"),
    }

    call.await.expect("call task panicked");
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
        handle.place_order(
            RithmicOrder::new()
                .symbol("ESH6")
                .exchange("CME")
                .quantity(1)
                .price_type(OrderType::Market),
        ),
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

/// Answer the `GetTradeRoutes` that `login` issues, returning whether it asked to
/// stay subscribed to later updates.
async fn answer_trade_routes(
    command_receiver: &mut mpsc::Receiver<OrderPlantCommand>,
    response: Result<Vec<RithmicResponse>, RithmicError>,
) -> bool {
    match next_command(command_receiver).await {
        OrderPlantCommand::GetTradeRoutes {
            subscribe_for_updates,
            response_sender,
        } => {
            let _ = response_sender.send(response);

            subscribe_for_updates
        }
        _ => panic!("login must fetch the trade routes that orders are routed on"),
    }
}

fn trade_route_response(exchange: &str, trade_route: &str) -> RithmicResponse {
    RithmicResponse {
        request_id: "3".to_string(),
        message: RithmicMessage::ResponseTradeRoutes(crate::rti::ResponseTradeRoutes {
            template_id: 311,
            exchange: Some(exchange.to_string()),
            trade_route: Some(trade_route.to_string()),
            ..Default::default()
        }),
        is_update: false,
        has_more: false,
        multi_response: true,
        error: None,
        source: "order_plant".to_string(),
    }
}

/// Routes are what orders are placed on, so a login must hand the plant the
/// snapshot it read. Only `record_trade_route` fills the cache after that.
#[tokio::test]
async fn login_hands_the_trade_routes_it_read_to_the_plant() {
    let (handle, mut command_receiver) = test_handle();

    let driver = async {
        drive_login_to_login_info(&mut command_receiver).await;
        answer_login_info(&mut command_receiver, Ok(vec![login_info_response(None)])).await;

        let subscribed = answer_trade_routes(
            &mut command_receiver,
            Ok(vec![trade_route_response("CME", "globex")]),
        )
        .await;

        let recorded = match next_command(&mut command_receiver).await {
            OrderPlantCommand::RecordTradeRoutes(responses) => responses,
            _ => panic!("login must hand the routes it read to the plant"),
        };

        (subscribed, recorded)
    };

    let (login, (subscribed, recorded)) = tokio::join!(handle.login(), driver);

    assert!(login.is_ok());
    assert!(
        subscribed,
        "a route the server changes later must at least reach subscribers"
    );
    assert_eq!(recorded.len(), 1, "the route read has to reach the plant");

    match &recorded[0].message {
        RithmicMessage::ResponseTradeRoutes(route) => {
            assert_eq!(route.exchange.as_deref(), Some("CME"));
            assert_eq!(route.trade_route.as_deref(), Some("globex"));
        }
        _ => panic!("expected the ResponseTradeRoutes frame login read"),
    }
}

/// An unavailable route request must not fail a login that already succeeded — the
/// orders that follow fail individually with `NoTradeRoute` instead.
#[tokio::test]
async fn login_succeeds_when_the_trade_routes_are_unavailable() {
    let (handle, mut command_receiver) = test_handle();

    let driver = async {
        drive_login_to_login_info(&mut command_receiver).await;
        answer_login_info(&mut command_receiver, Ok(vec![login_info_response(None)])).await;
        answer_trade_routes(&mut command_receiver, Err(RithmicError::EmptyResponse)).await;
    };

    let (login, ()) = tokio::join!(handle.login(), driver);

    assert!(
        login.is_ok(),
        "failing trade routes must not fail the login"
    );
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
            answer_trade_routes(&mut command_receiver, Ok(vec![])).await;
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
        answer_trade_routes(&mut command_receiver, Ok(vec![])).await;
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

/// Record the route the server would have published for `exchange`, as a login on
/// this connection would have left it.
fn cache_route(plant: &mut OrderPlant, exchange: &str, trade_route: &str) {
    plant
        .trade_routes
        .record(Some(exchange), Some(trade_route), None);
}

/// A plant actor whose connection has logged in, as `login()` would leave it.
async fn scoped_plant_with_wire() -> (OrderPlant, mpsc::Sender<OrderPlantCommand>, TcpStream) {
    let (mut plant, sender, client) = plant_with_wire().await;

    cache_route(&mut plant, "CME", "globex");

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
    RithmicBracketOrder::new()
        .symbol("ESM6")
        .exchange("CME")
        .quantity(1)
        .action(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .duration(TimeInForce::Day)
        .price(5000.0)
        .target(20)
        .stop(10)
        .localid("bracket-1")
        .build()
        .expect("valid bracket")
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
                bracket_order: Box::new(bracket_order()),
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
                command: RithmicCancelAllOrders::default(),
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

/// The hop the handle-level test cannot see. Its two arms are adjacent
/// near-copies, so every value differs: a crossed arm fails rather than passes.
#[tokio::test]
async fn bracket_level_commands_carry_their_level_to_the_wire() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;

    let target: crate::rti::RequestUpdateTargetBracketLevel =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::ModifyTarget {
                adjustment: adjustment("basket-1", 16, Some(2)),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(target.basket_id.as_deref(), Some("basket-1"));
    assert_eq!(target.target_ticks, Some(16));
    assert_eq!(target.level, Some(2));

    let stop: crate::rti::RequestUpdateStopBracketLevel =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::ModifyStop {
                adjustment: adjustment("basket-2", 8, Some(3)),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(stop.basket_id.as_deref(), Some("basket-2"));
    assert_eq!(stop.stop_ticks, Some(8));
    assert_eq!(stop.level, Some(3));
}

/// A bracket that names its own exchange, and optionally its own route.
fn bracket_order_on(exchange: &str, trade_route: Option<&str>) -> RithmicBracketOrder {
    let mut order = RithmicBracketOrder::new()
        .symbol("ESM6")
        .exchange(exchange)
        .quantity(1)
        .action(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(5000.0)
        .localid("advanced-1");
    if let Some(trade_route) = trade_route {
        order = order.trade_route(trade_route);
    }
    order.build().expect("valid bracket")
}

/// An OCO group straight from its legs; the builder's two-leg minimum is
/// asserted at the handle, and these tests drive the actor directly.
fn oco_group(legs: Vec<RithmicOcoOrderLeg>) -> RithmicOcoOrder {
    RithmicOcoOrder {
        legs,
        ..Default::default()
    }
}

fn leg_on(exchange: &str, trade_route: Option<&str>) -> RithmicOcoOrderLeg {
    RithmicOcoOrderLeg {
        exchange: exchange.to_string(),
        trade_route: trade_route.map(str::to_string),
        ..leg("oco")
    }
}

/// Every order command must reach the wire on the route cached for its own
/// exchange — a route crossed between exchanges is an order the venue rejects.
#[tokio::test]
async fn every_order_command_sends_the_route_cached_for_its_exchange() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");
    cache_route(&mut plant, "NYMEX", "nymex-route");

    let order: crate::rti::RequestNewOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOrder {
                order: RithmicOrder {
                    exchange: "CME".to_string(),
                    ..RithmicOrder::default()
                },
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(order.trade_route.as_deref(), Some("globex"));

    let bracket: crate::rti::RequestBracketOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order: Box::new(bracket_order()),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(bracket.trade_route.as_deref(), Some("globex"));

    let advanced: crate::rti::RequestBracketOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order: Box::new(bracket_order_on("NYMEX", None)),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(advanced.trade_route.as_deref(), Some("nymex-route"));

    // 350's `trade_route` is repeated and index-aligned with the legs, so an OCO
    // spanning exchanges must send one route per leg in the legs' own order.
    let oco: crate::rti::RequestOcoOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOcoOrder {
                order: oco_group(vec![leg_on("NYMEX", None), leg_on("CME", None)]),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(oco.trade_route, vec!["nymex-route", "globex"]);

    let oco_multi: crate::rti::RequestOcoOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOcoOrder {
                order: oco_group(vec![
                    leg_on("CME", None),
                    leg_on("NYMEX", None),
                    leg_on("CME", None),
                ]),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(
        oco_multi.trade_route,
        vec!["globex", "nymex-route", "globex"]
    );
}

/// The per-order route wins over the cache, and works with nothing cached at all —
/// which is how an unlisted route stays reachable.
#[tokio::test]
async fn a_per_order_route_overrides_the_cached_one() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");

    let order: crate::rti::RequestNewOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOrder {
                order: RithmicOrder {
                    exchange: "CME".to_string(),
                    trade_route: Some("my-route".to_string()),
                    ..RithmicOrder::default()
                },
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(order.trade_route.as_deref(), Some("my-route"));

    let advanced: crate::rti::RequestBracketOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order: Box::new(bracket_order_on("CBOT", Some("cbot-route"))),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(advanced.trade_route.as_deref(), Some("cbot-route"));

    let oco: crate::rti::RequestOcoOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOcoOrder {
                order: oco_group(vec![leg_on("CME", Some("leg-route")), leg_on("CME", None)]),
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(oco.trade_route, vec!["leg-route", "globex"]);
}

/// An order with no route must be refused here rather than sent for the server to
/// reject, and an OCO must fail whole: a partial group is not the order asked for.
#[tokio::test]
async fn an_unroutable_order_is_refused_before_the_wire() {
    let commands: Vec<Box<dyn FnOnce(Responder) -> OrderPlantCommand>> = vec![
        Box::new(|response_sender| OrderPlantCommand::PlaceOrder {
            order: RithmicOrder {
                exchange: "CBOT".to_string(),
                ..RithmicOrder::default()
            },
            account: test_account(),
            response_sender,
        }),
        Box::new(|response_sender| OrderPlantCommand::PlaceBracketOrder {
            bracket_order: Box::new(bracket_order()),
            account: test_account(),
            response_sender,
        }),
        Box::new(|response_sender| OrderPlantCommand::PlaceBracketOrder {
            bracket_order: Box::new(bracket_order_on("CBOT", None)),
            account: test_account(),
            response_sender,
        }),
        // Second leg only: the first resolves, so the whole group must still fail.
        Box::new(|response_sender| OrderPlantCommand::PlaceOcoOrder {
            order: oco_group(vec![leg_on("CME", Some("leg-route")), leg_on("CBOT", None)]),
            account: test_account(),
            response_sender,
        }),
    ];

    for build in commands {
        let (mut plant, _sender, mut client) = plant_with_wire().await;

        let (response_sender, rx) = oneshot::channel();
        plant.handle_command(build(response_sender)).await;

        assert!(matches!(
            awaited_caller_outcome(rx).await,
            Err(RithmicError::NoTradeRoute { .. })
        ));
        assert_wire_silent(&mut client).await;
    }
}

/// The preflight check answers from the cache and sends nothing, so it is safe to
/// call before trading opens.
#[tokio::test]
async fn trade_route_for_reports_the_route_an_order_would_take() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;

    cache_route(&mut plant, "CME", "globex");

    let (response_sender, rx) = oneshot::channel();

    plant
        .handle_command(OrderPlantCommand::TradeRouteFor {
            exchange: "CME".to_string(),
            response_sender,
        })
        .await;

    assert_eq!(
        rx.await
            .expect("the caller must be answered")
            .expect("CME is cached"),
        "globex"
    );
    assert_wire_silent(&mut client).await;
}

/// The server moves a route mid-session and a subscriber hands it back, so the
/// orders that follow have to take the new one. Applying it sends nothing.
#[tokio::test]
async fn a_route_update_handed_back_moves_where_orders_go() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;

    cache_route(&mut plant, "CME", "globex");

    plant
        .handle_command(OrderPlantCommand::RecordTradeRouteUpdate(Box::new(
            crate::rti::TradeRoute {
                template_id: 350,
                exchange: Some("CME".to_string()),
                trade_route: Some("globex-2".to_string()),
                is_default: Some(true),
                ..Default::default()
            },
        )))
        .await;

    let (response_sender, rx) = oneshot::channel();

    plant
        .handle_command(OrderPlantCommand::TradeRouteFor {
            exchange: "CME".to_string(),
            response_sender,
        })
        .await;

    assert_eq!(
        rx.await
            .expect("the caller must be answered")
            .expect("CME is cached"),
        "globex-2"
    );
    assert_wire_silent(&mut client).await;
}

/// An exchange with no route fails the preflight the same way placing the order
/// would, so a caller can gate on it.
#[tokio::test]
async fn trade_route_for_fails_where_an_order_would() {
    let (mut plant, _sender, _client) = plant_with_wire().await;

    cache_route(&mut plant, "CME", "globex");

    let (response_sender, rx) = oneshot::channel();

    plant
        .handle_command(OrderPlantCommand::TradeRouteFor {
            exchange: "CBOT".to_string(),
            response_sender,
        })
        .await;

    assert!(matches!(
        rx.await.expect("the caller must be answered"),
        Err(RithmicError::NoTradeRoute { .. })
    ));
}

/// The command login queues once it has read the routes off the wire. Applying
/// it is what fills the cache orders resolve against.
#[tokio::test]
async fn record_trade_routes_populates_the_cache() {
    let (mut plant, _sender, _client) = plant_with_wire().await;

    plant
        .handle_command(OrderPlantCommand::RecordTradeRoutes(vec![
            trade_route_response("CME", "globex"),
        ]))
        .await;

    assert_eq!(plant.trade_routes.resolve(None, "CME").unwrap(), "globex");
}

/// A rejected route request must not fail the login, and must not leave orders
/// a route to send on.
#[tokio::test]
async fn login_does_not_cache_a_rejected_trade_route() {
    let (handle, mut command_receiver) = test_handle();

    let driver = async {
        drive_login_to_login_info(&mut command_receiver).await;
        answer_login_info(&mut command_receiver, Ok(vec![login_info_response(None)])).await;

        answer_trade_routes(
            &mut command_receiver,
            Ok(vec![RithmicResponse {
                error: Some(RithmicError::ProtocolError("denied".to_string())),
                ..trade_route_response("CME", "globex")
            }]),
        )
        .await;

        match next_command(&mut command_receiver).await {
            OrderPlantCommand::RecordTradeRoutes(responses) => responses,
            _ => panic!("login must hand the routes it read to the plant"),
        }
    };

    let (login, recorded) = tokio::join!(handle.login(), driver);

    assert!(
        login.is_ok(),
        "a rejected trade route must not fail the login"
    );

    let (mut plant, _sender, _client) = plant_with_wire().await;
    plant
        .handle_command(OrderPlantCommand::RecordTradeRoutes(recorded))
        .await;

    assert!(matches!(
        plant.trade_routes.resolve(None, "CME"),
        Err(RithmicError::NoTradeRoute { .. })
    ));
}

/// Querying the server's routes is a read: it must not change what an order
/// placed right now would route on.
#[tokio::test]
async fn get_trade_routes_does_not_touch_the_cache() {
    let (mut plant, _sender, _client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");

    let (response_sender, _rx) = oneshot::channel();

    plant
        .handle_command(OrderPlantCommand::GetTradeRoutes {
            subscribe_for_updates: true,
            response_sender,
        })
        .await;

    assert_eq!(plant.trade_routes.resolve(None, "CME").unwrap(), "globex");
}

/// The handle-level wrapper: it has to ask the actor and hand back whatever
/// the actor answers, not just queue the command.
#[tokio::test]
async fn trade_route_for_asks_the_actor_and_returns_its_answer() {
    let (handle, mut command_receiver) = test_handle();

    let call = tokio::spawn(async move { handle.trade_route_for("CME").await });

    match command_receiver.recv().await {
        Some(OrderPlantCommand::TradeRouteFor {
            exchange,
            response_sender,
        }) => {
            assert_eq!(exchange, "CME");
            let _ = response_sender.send(Ok("globex".to_string()));
        }
        _ => panic!("expected TradeRouteFor to be queued"),
    }

    assert_eq!(call.await.expect("call task panicked").unwrap(), "globex");
}

#[tokio::test]
async fn trade_route_for_reports_connection_closed_when_the_plant_is_gone() {
    let (handle, command_receiver) = test_handle();
    drop(command_receiver);

    let err = handle
        .trade_route_for("CME")
        .await
        .expect_err("the plant is gone");

    assert!(matches!(err, RithmicError::ConnectionClosed));
}

/// The handle-level wrapper: it has to hand the update to the actor rather
/// than applying it itself.
#[tokio::test]
async fn record_trade_route_forwards_the_update_to_the_actor() {
    let (handle, mut command_receiver) = test_handle();

    let update = crate::rti::TradeRoute {
        template_id: 350,
        exchange: Some("CME".to_string()),
        trade_route: Some("moved".to_string()),
        is_default: Some(true),
        ..Default::default()
    };

    let call = tokio::spawn({
        let update = update.clone();
        async move { handle.record_trade_route(&update).await }
    });

    match command_receiver.recv().await {
        Some(OrderPlantCommand::RecordTradeRouteUpdate(recorded)) => {
            assert_eq!(recorded.exchange.as_deref(), Some("CME"));
            assert_eq!(recorded.trade_route.as_deref(), Some("moved"));
        }
        _ => panic!("expected RecordTradeRouteUpdate to be queued"),
    }

    call.await.expect("call task panicked").unwrap();
}

#[tokio::test]
async fn record_trade_route_reports_connection_closed_when_the_plant_is_gone() {
    let (handle, command_receiver) = test_handle();
    drop(command_receiver);

    let err = handle
        .record_trade_route(&crate::rti::TradeRoute::default())
        .await
        .expect_err("the plant is gone");

    assert!(matches!(err, RithmicError::ConnectionClosed));
}

#[tokio::test]
async fn cancel_all_orders_encodes_auto_placement_by_default() {
    // The Manual -> Auto change lives in the command's default, so this drives
    // an unconfigured command through the handle and then decodes what that
    // same command puts on the wire.
    let (handle, mut command_receiver) = test_handle();
    let call = tokio::spawn(async move {
        handle
            .cancel_all_orders(RithmicCancelAllOrders::default())
            .await
    });

    let command = command_receiver
        .recv()
        .await
        .expect("cancel_all_orders must queue a command");

    let OrderPlantCommand::CancelAllOrders {
        command: queued, ..
    } = &command
    else {
        panic!("expected CancelAllOrders to be queued");
    };
    assert_eq!(
        queued.manual_or_auto,
        ManualOrAutoEntry::Auto,
        "cancel_all_orders() must attribute to Auto like every other order call"
    );
    let queued = queued.clone();

    drop(command);
    let _ = call.await;

    let (mut plant, _sender, mut client) = plant_with_wire().await;
    let request: crate::rti::RequestCancelAllOrders =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::CancelAllOrders {
                command: queued,
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(
        request.manual_or_auto,
        Some(crate::rti::request_cancel_all_orders::OrderPlacement::Auto as i32)
    );
}

/// Validation is the caller's to run: the plant encodes and sends what it is
/// given. Rithmic is the authority on what it accepts, so an unpriced limit
/// order goes out and comes back rejected rather than being refused locally.
#[tokio::test]
async fn an_unpriced_limit_order_is_still_sent() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");

    let request: crate::rti::RequestNewOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOrder {
                order: RithmicOrder {
                    exchange: "CME".to_string(),
                    price_type: OrderType::Limit,
                    price: None,
                    ..RithmicOrder::default()
                },
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(
        request.price, None,
        "an unset price must be omitted, not sent as zero"
    );
}

/// A market order carries no price by design — Rithmic's own reference client
/// places one without ever setting the field.
#[tokio::test]
async fn a_market_order_omits_price_on_the_wire() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");

    let request: crate::rti::RequestNewOrder =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::PlaceOrder {
                order: RithmicOrder {
                    exchange: "CME".to_string(),
                    price_type: OrderType::Market,
                    price: None,
                    ..RithmicOrder::default()
                },
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(request.price, None);
}

/// The `Auto` attribution for an exit lives in the command's default, and the
/// sender now always states a placement. So this drives an unconfigured command
/// through the real handle and decodes the transmitted frame — asserting on the
/// builder alone could not detect the default moving.
#[tokio::test]
async fn exit_position_encodes_auto_placement_by_default() {
    let (handle, mut command_receiver) = test_handle();
    let call = tokio::spawn(async move {
        handle
            .exit_position(
                RithmicExitPosition::new()
                    .symbol("ESM6")
                    .exchange("CME")
                    .build()
                    .expect("valid exit"),
            )
            .await
    });

    let command = command_receiver
        .recv()
        .await
        .expect("exit_position must queue a command");

    let OrderPlantCommand::ExitPosition {
        command: queued, ..
    } = &command
    else {
        panic!("expected ExitPosition to be queued");
    };
    assert_eq!(
        queued.manual_or_auto,
        ManualOrAutoEntry::Auto,
        "exit_position() must attribute to Auto like every other order call"
    );
    let queued = queued.clone();

    drop(command);
    let _ = call.await;

    let (mut plant, _sender, mut client) = plant_with_wire().await;
    let request: crate::rti::RequestExitPosition =
        sent_request(&mut plant, &mut client, |response_sender| {
            OrderPlantCommand::ExitPosition {
                command: queued,
                account: test_account(),
                response_sender,
            }
        })
        .await;

    assert_eq!(
        request.manual_or_auto,
        Some(crate::rti::request_exit_position::OrderPlacement::Auto as i32)
    );
}
