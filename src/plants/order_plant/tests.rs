use super::*;
use prost::Message as _;
use tokio::net::TcpStream;

use crate::{
    api::commands::{RithmicBracketOrder, RithmicOcoOrderLeg},
    plants::{
        core::{Effect, Event, PlantCore},
        session::Session,
        test_support::{
            self, Responder, answer, assert_close_still_sent, assert_wire_silent,
            awaited_caller_outcome, frame, read_wire_request, test_account, write_wire_response,
        },
    },
    types::{ManualOrAutoEntry, OrderSide, OrderType, TimeInForce},
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
        symbol: "ESZ6".to_string(),
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

async fn plant_with_wire() -> (
    Plant<OrderPlant>,
    mpsc::Sender<OrderPlantCommand>,
    TcpStream,
) {
    test_support::plant_with_wire().await
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
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    assert_close_still_sent(&mut plant, OrderPlantCommand::Close, &mut client).await;
}

/// The contract that matters: the order must not go live at the exchange while
/// its caller records a failure.
#[tokio::test]
async fn place_order_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    let account = test_account();
    let handle = RithmicOrderPlantHandle {
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
        handle.place_order(
            RithmicOrder::new()
                .symbol("ESZ6")
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

/// A running actor on a live wire, as `connect` leaves it before any login,
/// returned with the client half of the socket.
async fn running_plant() -> (RithmicOrderPlant, TcpStream) {
    let (mut plant, sender, client) = plant_with_wire().await;
    plant.core.session = Session::Connected;

    let subscription_sender = plant.subscription_sender.clone();
    let connection_handle = tokio::spawn(async move { plant.run().await });

    let plant = RithmicOrderPlant {
        connection_handle,
        sender,
        subscription_sender,
    };

    (plant, client)
}

/// Reads the next request off the wire, asserting its template.
async fn read_request<M: prost::Message + Default>(
    client: &mut TcpStream,
    template_id: i32,
    expectation: &str,
) -> M {
    let payload = read_wire_request(client).await;
    let sent = crate::rti::MessageType::decode(&*payload)
        .expect("every request carries a template id")
        .template_id;

    assert_eq!(sent, template_id, "{expectation}");

    M::decode(&*payload).expect("the actor serialized this request")
}

/// A login reply echoing `user_msg`, accepted when `rp_code` is `["0"]`.
fn login_reply(user_msg: Vec<String>, rp_code: &[&str]) -> crate::rti::ResponseLogin {
    crate::rti::ResponseLogin {
        template_id: 11,
        user_msg,
        rp_code: rp_code.iter().map(|code| code.to_string()).collect(),
        unique_user_id: Some("session-1".to_string()),
        ..Default::default()
    }
}

async fn read_login_request(client: &mut TcpStream) -> crate::rti::RequestLogin {
    read_request(client, 10, "login must send the login request").await
}

/// Reads the login request off the wire and answers it with `rp_code`.
async fn answer_login(client: &mut TcpStream, rp_code: &[&str]) {
    let login = read_login_request(client).await;

    write_wire_response(client, &login_reply(login.user_msg, rp_code)).await;
}

/// The two requests the actor sends once its login is accepted.
struct PostLoginRequests {
    login_info: crate::rti::RequestLoginInfo,
    trade_routes: crate::rti::RequestTradeRoutes,
}

async fn read_post_login_requests(client: &mut TcpStream) -> PostLoginRequests {
    let login_info = read_request(
        client,
        300,
        "the plant must fetch the login info that scopes get_account_list",
    )
    .await;
    let trade_routes = read_request(
        client,
        310,
        "the plant must fetch the trade routes that orders are routed on",
    )
    .await;

    PostLoginRequests {
        login_info,
        trade_routes,
    }
}

fn rejected() -> Vec<String> {
    vec!["5".to_string(), "denied".to_string()]
}

/// The login info the server returns for an accepted login, with `fcm_id`.
async fn answer_login_info_with(client: &mut TcpStream, user_msg: &[String], fcm_id: &str) {
    let reply = crate::rti::ResponseLoginInfo {
        user_msg: user_msg.to_vec(),
        rp_code: vec!["0".to_string()],
        fcm_id: Some(fcm_id.to_string()),
        ..login_info()
    };

    write_wire_response(client, &reply).await;
}

async fn answer_login_info(client: &mut TcpStream, request: &crate::rti::RequestLoginInfo) {
    answer_login_info_with(client, &request.user_msg, "FCM_LOGIN").await;
}

/// A server refusing the request outright, echoing its id.
async fn reject(client: &mut TcpStream, user_msg: &[String]) {
    let reply = crate::rti::Reject {
        template_id: 75,
        user_msg: user_msg.to_vec(),
        rp_code: rejected(),
    };

    write_wire_response(client, &reply).await;
}

/// One route per exchange, then the frame that ends the list.
async fn answer_trade_routes(
    client: &mut TcpStream,
    request: &crate::rti::RequestTradeRoutes,
    routes: &[(&str, &str)],
) {
    for (exchange, trade_route) in routes {
        let part = crate::rti::ResponseTradeRoutes {
            template_id: 311,
            user_msg: request.user_msg.clone(),
            rq_handler_rp_code: vec!["0".to_string()],
            exchange: Some(exchange.to_string()),
            trade_route: Some(trade_route.to_string()),
            is_default: Some(true),
            ..Default::default()
        };

        write_wire_response(client, &part).await;
    }

    let end = crate::rti::ResponseTradeRoutes {
        template_id: 311,
        user_msg: request.user_msg.clone(),
        rp_code: vec!["0".to_string()],
        ..Default::default()
    };

    write_wire_response(client, &end).await;
}

/// Plays the server through a whole login: accepted, with the login info and
/// a CME route.
async fn serve_login(client: &mut TcpStream) {
    answer_login(client, &["0"]).await;

    let requests = read_post_login_requests(client).await;
    answer_login_info(client, &requests.login_info).await;
    answer_trade_routes(client, &requests.trade_routes, &[("CME", "globex")]).await;
}

/// The account list request `handle` puts on the wire, which shows how the
/// plant is scoped. The call itself is left unanswered.
async fn account_list_request(
    handle: &RithmicOrderPlantHandle,
    client: &mut TcpStream,
) -> crate::rti::RequestAccountList {
    let call = handle.get_account_list();
    tokio::pin!(call);

    tokio::select! {
        _ = &mut call => panic!("the account list cannot return before it is answered"),
        request = read_request(client, 302, "get_account_list must send its request") => request,
    }
}

/// Fails rather than hangs if `call` is still waiting after five seconds.
async fn answered<T>(call: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(5), call)
        .await
        .expect("the call must be answered, not left waiting")
        .expect("call task panicked")
}

/// Routes are what orders are placed on, so the plant a login leaves behind must
/// hold the snapshot the login read. Only `record_trade_route` fills the cache
/// after that.
#[tokio::test]
async fn login_hands_the_trade_routes_it_read_to_the_plant() {
    let (plant, mut client) = running_plant().await;
    let handle = plant.get_handle(&test_account());

    let server = async {
        answer_login(&mut client, &["0"]).await;

        let requests = read_post_login_requests(&mut client).await;
        answer_login_info(&mut client, &requests.login_info).await;
        answer_trade_routes(&mut client, &requests.trade_routes, &[("CME", "globex")]).await;

        requests.trade_routes.subscribe_for_updates
    };

    let (login, subscribed) = tokio::join!(handle.login(), server);

    assert!(login.is_ok());
    assert_eq!(
        subscribed,
        Some(true),
        "a route the server changes later must at least reach subscribers"
    );
    assert_eq!(
        handle.trade_route_for("CME").await.unwrap(),
        "globex",
        "the route read has to reach the plant"
    );
}

/// Neither failure shape may fail the login or leave a scope behind.
#[tokio::test]
async fn login_succeeds_and_stays_unscoped_when_the_login_info_fails() {
    for refused_outright in [false, true] {
        let (plant, mut client) = running_plant().await;
        let handle = plant.get_handle(&test_account());

        let server = async {
            answer_login(&mut client, &["0"]).await;

            let requests = read_post_login_requests(&mut client).await;

            if refused_outright {
                reject(&mut client, &requests.login_info.user_msg).await;
            } else {
                let reply = crate::rti::ResponseLoginInfo {
                    user_msg: requests.login_info.user_msg.clone(),
                    rp_code: rejected(),
                    ..login_info()
                };

                write_wire_response(&mut client, &reply).await;
            }

            answer_trade_routes(&mut client, &requests.trade_routes, &[]).await;
        };

        let (login, ()) = tokio::join!(handle.login(), server);
        assert!(login.is_ok(), "a failed login info must not fail the login");

        let account_list = account_list_request(&handle, &mut client).await;
        assert_eq!(
            account_list.fcm_id, None,
            "a failed login info must not scope"
        );
    }
}

/// The scope belongs to the connection, not to the handle that logged in — the
/// README's multi-account flow takes a further handle per account after logging in
/// on one, and those must be scoped too.
#[tokio::test]
async fn every_handle_from_one_plant_shares_the_login_scope() {
    let (plant, mut client) = running_plant().await;

    let logs_in = plant.get_handle(&test_account());
    let never_logs_in = plant.get_handle(&RithmicAccount::new("FCM_B", "IB_B", "ACCOUNT_B"));

    let (login, ()) = tokio::join!(logs_in.login(), serve_login(&mut client));
    assert!(login.is_ok());

    let scope = account_list_request(&never_logs_in, &mut client).await;

    assert_eq!(scope.fcm_id.as_deref(), Some("FCM_LOGIN"));
    assert_eq!(scope.ib_id.as_deref(), Some("IB_LOGIN"));
    assert_eq!(
        scope.user_type,
        Some(crate::rti::request_account_list::UserType::Ib.into())
    );
}

/// The steps follow an accepted login reply, not anything else the plant sees.
#[tokio::test]
async fn a_refused_login_asks_for_neither_login_info_nor_routes() {
    let (plant, mut client) = running_plant().await;
    let handle = plant.get_handle(&test_account());

    // A turn that is not a login reply.
    assert!(handle.trade_route_for("CME").await.is_err());

    let (login, ()) = tokio::join!(handle.login(), answer_login(&mut client, &["7", "bad"]));
    assert!(matches!(login, Err(RithmicError::RequestRejected(_))));

    assert_wire_silent(&mut client).await;
}

/// Logins that overlap share the one request, and all of them get its reply.
#[tokio::test]
async fn concurrent_logins_send_one_login_request() {
    let (plant, mut client) = running_plant().await;
    let first = plant.get_handle(&test_account());
    let second = plant.get_handle(&RithmicAccount::new("FCM_B", "IB_B", "ACCOUNT_B"));

    let server = async {
        let login = read_login_request(&mut client).await;

        // Both logins are queued by now; a second request would show here.
        assert_wire_silent(&mut client).await;
        write_wire_response(&mut client, &login_reply(login.user_msg, &["0"])).await;

        let requests = read_post_login_requests(&mut client).await;
        answer_login_info(&mut client, &requests.login_info).await;
        answer_trade_routes(&mut client, &requests.trade_routes, &[("CME", "globex")]).await;
    };

    let (first_login, second_login, ()) = tokio::join!(first.login(), second.login(), server);

    let first_login = first_login.expect("the first login must succeed");
    let second_login = second_login.expect("the joined login must succeed");
    assert_eq!(first_login.request_id, second_login.request_id);

    assert_wire_silent(&mut client).await;
}

/// A plant that is logged in answers `login()` with the reply it kept.
#[tokio::test]
async fn a_login_on_a_logged_in_plant_returns_the_kept_reply_without_sending() {
    let (plant, mut client) = running_plant().await;
    let handle = plant.get_handle(&test_account());

    let (first, ()) = tokio::join!(handle.login(), serve_login(&mut client));
    let first = first.expect("the login must succeed");

    // Normalised as the plant sends it: the order plant ignores this field.
    let again = handle
        .login_with_config(LoginConfig {
            aggregated_quotes: Some(true),
            ..LoginConfig::default()
        })
        .await
        .expect("a login with the same config must succeed");

    assert_eq!(again, first);
    assert_wire_silent(&mut client).await;
}

/// `disconnect()` fails a login in flight at once, whether it waits on the
/// login reply or on the login info and routes, rather than leaving it for
/// replies that may never come.
#[tokio::test]
async fn disconnect_fails_a_login_in_flight_at_once() {
    for reply_accepted in [false, true] {
        let (plant, mut client) = running_plant().await;
        let handle = plant.get_handle(&test_account());

        let login = tokio::spawn({
            let handle = handle.clone();
            async move { handle.login().await }
        });

        let request = read_login_request(&mut client).await;
        if reply_accepted {
            write_wire_response(&mut client, &login_reply(request.user_msg, &["0"])).await;
            read_post_login_requests(&mut client).await;
        }

        let disconnect = tokio::spawn({
            let handle = handle.clone();
            async move { handle.disconnect().await }
        });
        let _: crate::rti::RequestLogout =
            read_request(&mut client, 12, "disconnect must send the logout").await;

        // The logout is not answered, so only the disconnect itself can end it.
        assert_eq!(answered(login).await, Err(RithmicError::ConnectionClosed));

        handle.abort();
        disconnect.abort();
    }
}

/// A login in flight fails when the plant is aborted or the server hangs up.
#[tokio::test]
async fn a_login_in_flight_fails_when_the_connection_ends() {
    for server_hangs_up in [false, true] {
        let (plant, mut client) = running_plant().await;
        let handle = plant.get_handle(&test_account());

        let login = tokio::spawn({
            let handle = handle.clone();
            async move { handle.login().await }
        });
        read_login_request(&mut client).await;

        if server_hangs_up {
            drop(client);
        } else {
            handle.abort();
        }

        assert_eq!(answered(login).await, Err(RithmicError::ConnectionClosed));
    }
}

/// Asking for the login info by hand returns it, and scopes a plant whose own
/// login info failed. It never replaces a scope already set.
#[tokio::test]
async fn get_login_info_scopes_only_a_plant_without_a_scope() {
    let (plant, mut client) = running_plant().await;
    let handle = plant.get_handle(&test_account());

    let server = async {
        answer_login(&mut client, &["0"]).await;

        let requests = read_post_login_requests(&mut client).await;
        reject(&mut client, &requests.login_info.user_msg).await;
        answer_trade_routes(&mut client, &requests.trade_routes, &[]).await;
    };
    let (login, ()) = tokio::join!(handle.login(), server);
    assert!(login.is_ok());

    for (fcm_id, scoped) in [("FCM_LOGIN", "FCM_LOGIN"), ("FCM_LATER", "FCM_LOGIN")] {
        let server = async {
            let request: crate::rti::RequestLoginInfo =
                read_request(&mut client, 300, "get_login_info must send its request").await;
            answer_login_info_with(&mut client, &request.user_msg, fcm_id).await;
        };
        let (info, ()) = tokio::join!(handle.get_login_info(), server);

        match info.expect("the caller gets the login info").message {
            RithmicMessage::ResponseLoginInfo(info) => {
                assert_eq!(info.fcm_id.as_deref(), Some(fcm_id));
            }
            other => panic!("expected the login info, got {other:?}"),
        }

        let account_list = account_list_request(&handle, &mut client).await;
        assert_eq!(account_list.fcm_id.as_deref(), Some(scoped));
    }
}

/// The order plant's core, with no socket, as `connect` leaves it.
fn order_core() -> PlantCore<OrderPlant> {
    test_support::plant_core()
}

/// The ids of the requests `effects` puts on the wire.
fn sent_ids(effects: &[Effect]) -> Vec<String> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Send { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect()
}

/// Logs `core` in and has the server accept it. Returns the login's reply
/// receiver and the ids of the login info and trade routes requests that
/// followed.
fn accepted_login(
    core: &mut PlantCore<OrderPlant>,
) -> (
    oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>>,
    String,
    String,
) {
    let (response_sender, rx) = oneshot::channel();
    let effects = core.on_event(Event::Command(OrderPlantCommand::Login {
        config: LoginConfig::default(),
        response_sender,
    }));
    let login_id = sent_ids(&effects).remove(0);

    let effects = core.on_event(Event::Frame(frame(&login_reply(vec![login_id], &["0"]))));
    let ids = sent_ids(&effects);
    assert_eq!(
        ids.len(),
        2,
        "an accepted login loads the login info and the trade routes"
    );

    (rx, ids[0].clone(), ids[1].clone())
}

/// The login info reply to request `id`, with `fcm_id`.
fn login_info_frame(id: &str, fcm_id: &str) -> RithmicResponse {
    frame(&crate::rti::ResponseLoginInfo {
        user_msg: vec![id.to_string()],
        rp_code: vec!["0".to_string()],
        fcm_id: Some(fcm_id.to_string()),
        ..login_info()
    })
}

/// The trade routes reply to request `id`: one route for `exchange`, then the
/// frame that ends the list.
fn trade_route_frames(id: &str, exchange: &str, trade_route: &str) -> [RithmicResponse; 2] {
    [
        frame(&crate::rti::ResponseTradeRoutes {
            template_id: 311,
            user_msg: vec![id.to_string()],
            rq_handler_rp_code: vec!["0".to_string()],
            exchange: Some(exchange.to_string()),
            trade_route: Some(trade_route.to_string()),
            is_default: Some(true),
            ..Default::default()
        }),
        frame(&crate::rti::ResponseTradeRoutes {
            template_id: 311,
            user_msg: vec![id.to_string()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        }),
    ]
}

/// The login a caller waits on is answered by the event that settles the last
/// of the login info and the trade routes, in either order, and not before.
#[test]
fn the_core_answers_a_login_once_the_login_info_and_trade_routes_are_answered() {
    for login_info_first in [true, false] {
        let mut core = order_core();
        let (mut rx, login_info_id, trade_routes_id) = accepted_login(&mut core);
        assert_eq!(answer(&mut rx), None, "nothing is loaded yet");

        let [route, end] = trade_route_frames(&trade_routes_id, "CME", "globex");
        let mut frames = vec![login_info_frame(&login_info_id, "FCM_LOGIN"), route, end];
        if !login_info_first {
            frames.rotate_left(1);
        }

        let last = frames.pop().unwrap();
        for frame in frames {
            core.on_event(Event::Frame(frame));
            assert_eq!(answer(&mut rx), None, "a load is still outstanding");
        }

        core.on_event(Event::Frame(last));
        assert!(matches!(answer(&mut rx), Some(Ok(_))));
        assert!(matches!(core.session, Session::Ready { .. }));
    }
}

/// A load that fails is settled too: the login still succeeds, unscoped and
/// with no routes.
#[test]
fn the_core_answers_a_login_whose_login_info_and_trade_routes_failed() {
    let mut core = order_core();
    let (mut rx, login_info_id, trade_routes_id) = accepted_login(&mut core);

    core.on_event(Event::SendFailed(login_info_id));
    assert_eq!(answer(&mut rx), None, "the trade routes are outstanding");

    core.on_event(Event::Frame(frame(&crate::rti::Reject {
        template_id: 75,
        user_msg: vec![trade_routes_id],
        rp_code: rejected(),
    })));

    let login = answer(&mut rx).expect("both loads are settled");
    assert!(matches!(login, Ok(frames) if frames[0].error.is_none()));
    assert!(core.kind.login_scope.is_none());
    assert!(core.kind.trade_routes.resolve(None, "CME").is_err());
}

/// A write that times out while the login info and trade routes load poisons
/// the sink, so the login fails rather than completing on a dead connection.
#[test]
fn the_core_fails_a_preparing_login_whose_write_timed_out() {
    let mut core = order_core();
    let (mut rx, login_info_id, _trade_routes_id) = accepted_login(&mut core);

    core.on_event(Event::SendTimedOut(login_info_id));

    assert_eq!(answer(&mut rx), Some(Err(RithmicError::ConnectionClosed)));
    assert!(matches!(core.session, Session::Connected));
}

/// A caller that stops waiting once the login reply is in changes nothing:
/// the plant still loads its scope and routes and finishes the login.
#[test]
fn the_core_loads_the_scope_and_routes_for_a_login_nobody_waits_for() {
    let mut core = order_core();
    let (rx, login_info_id, trade_routes_id) = accepted_login(&mut core);
    drop(rx);

    core.on_event(Event::Frame(login_info_frame(&login_info_id, "FCM_LOGIN")));
    for part in trade_route_frames(&trade_routes_id, "CME", "globex") {
        core.on_event(Event::Frame(part));
    }

    let scope = core.kind.login_scope.as_ref().expect("the login scoped it");
    assert_eq!(scope.fcm_id.as_deref(), Some("FCM_LOGIN"));
    assert_eq!(
        core.kind.trade_routes.resolve(None, "CME").unwrap(),
        "globex"
    );
    assert!(matches!(core.session, Session::Ready { .. }));
}

/// The scope and routes are the plant's own. A login loads them and
/// `record_trade_route` updates a route; a caller's own `get_login_info` or
/// `get_trade_routes` reads them without replacing either.
#[test]
fn only_the_plant_writes_its_scope_and_routes() {
    let mut core = order_core();
    let (_rx, login_info_id, trade_routes_id) = accepted_login(&mut core);
    core.on_event(Event::Frame(login_info_frame(&login_info_id, "FCM_LOGIN")));
    for part in trade_route_frames(&trade_routes_id, "CME", "globex") {
        core.on_event(Event::Frame(part));
    }

    let (response_sender, mut info) = oneshot::channel();
    let effects = core.on_event(Event::Command(OrderPlantCommand::GetLoginInfo {
        response_sender,
    }));
    let id = sent_ids(&effects).remove(0);
    core.on_event(Event::Frame(login_info_frame(&id, "FCM_LATER")));
    assert!(matches!(answer(&mut info), Some(Ok(_))));

    let (response_sender, mut routes) = oneshot::channel();
    let effects = core.on_event(Event::Command(OrderPlantCommand::GetTradeRoutes {
        subscribe_for_updates: false,
        response_sender,
    }));
    let id = sent_ids(&effects).remove(0);
    for part in trade_route_frames(&id, "CME", "other") {
        core.on_event(Event::Frame(part));
    }
    assert!(matches!(answer(&mut routes), Some(Ok(_))));

    let scope = core.kind.login_scope.as_ref().unwrap();
    assert_eq!(scope.fcm_id.as_deref(), Some("FCM_LOGIN"));
    assert_eq!(
        core.kind.trade_routes.resolve(None, "CME").unwrap(),
        "globex"
    );

    let effects = core.on_event(Event::Command(OrderPlantCommand::RecordTradeRouteUpdate(
        Box::new(crate::rti::TradeRoute {
            template_id: 350,
            exchange: Some("CME".to_string()),
            trade_route: Some("globex-2".to_string()),
            is_default: Some(true),
            ..Default::default()
        }),
    )));
    assert!(effects.is_empty(), "applying a route update sends nothing");
    assert_eq!(
        core.kind.trade_routes.resolve(None, "CME").unwrap(),
        "globex-2"
    );
}

/// Record the route the server would have published for `exchange`, as a login on
/// this connection would have left it.
fn cache_route(plant: &mut Plant<OrderPlant>, exchange: &str, trade_route: &str) {
    plant
        .core
        .kind
        .trade_routes
        .record(Some(exchange), Some(trade_route), None);
}

/// A plant actor whose connection has logged in, as `login()` would leave it.
async fn scoped_plant_with_wire() -> (
    Plant<OrderPlant>,
    mpsc::Sender<OrderPlantCommand>,
    TcpStream,
) {
    let (mut plant, sender, client) = plant_with_wire().await;

    cache_route(&mut plant, "CME", "globex");

    plant.core.kind.login_scope =
        Some(LoginScope::from_login_info(&login_info()).expect("an IB login is expressible"));

    (plant, sender, client)
}

/// Feeds one command to the actor and decodes the request it put on the wire.
async fn sent_request<M: prost::Message + Default>(
    plant: &mut Plant<OrderPlant>,
    client: &mut TcpStream,
    build: impl FnOnce(Responder) -> OrderPlantCommand,
) -> M {
    let (response_sender, _rx) = oneshot::channel();
    plant.handle(Event::Command(build(response_sender))).await;

    M::decode(&*read_wire_request(client).await).expect("the actor serialized this request")
}

fn bracket_order() -> RithmicBracketOrder {
    RithmicBracketOrder::new()
        .symbol("ESZ6")
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
        .symbol("ESZ6")
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
        plant.handle(Event::Command(build(response_sender))).await;

        assert!(matches!(
            awaited_caller_outcome(rx).await,
            Err(RithmicError::NoTradeRoute { .. })
        ));
        assert_wire_silent(&mut client).await;
    }
}

/// The preflight answers from the cache and sends nothing, so it is safe to call
/// before trading opens. An exchange with no route fails as its order would.
#[tokio::test]
async fn trade_route_for_answers_from_the_cache_without_sending() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");

    for (exchange, expected) in [("CME", Some("globex")), ("CBOT", None)] {
        let (response_sender, rx) = oneshot::channel();
        plant
            .handle(Event::Command(OrderPlantCommand::TradeRouteFor {
                exchange: exchange.to_string(),
                response_sender,
            }))
            .await;

        match (awaited_route(rx).await, expected) {
            (Ok(route), Some(expected)) => assert_eq!(route, expected),
            (Err(RithmicError::NoTradeRoute { .. }), None) => {}
            (other, _) => panic!("{exchange}: unexpected answer {other:?}"),
        }
    }

    assert_wire_silent(&mut client).await;
}

async fn awaited_route(
    rx: oneshot::Receiver<Result<String, RithmicError>>,
) -> Result<String, RithmicError> {
    rx.await.expect("the caller must be answered")
}

/// A route request the server refuses, outright or with an error code, must not
/// fail the login, and must not leave orders a route to send on.
#[tokio::test]
async fn login_succeeds_with_no_route_when_the_trade_routes_are_refused() {
    for refused_outright in [false, true] {
        let (plant, mut client) = running_plant().await;
        let handle = plant.get_handle(&test_account());

        let server = async {
            answer_login(&mut client, &["0"]).await;

            let requests = read_post_login_requests(&mut client).await;
            answer_login_info(&mut client, &requests.login_info).await;

            if refused_outright {
                reject(&mut client, &requests.trade_routes.user_msg).await;
            } else {
                let refused = crate::rti::ResponseTradeRoutes {
                    template_id: 311,
                    user_msg: requests.trade_routes.user_msg.clone(),
                    rp_code: rejected(),
                    exchange: Some("CME".to_string()),
                    trade_route: Some("globex".to_string()),
                    ..Default::default()
                };

                write_wire_response(&mut client, &refused).await;
            }
        };

        let (login, ()) = tokio::join!(handle.login(), server);

        assert!(
            login.is_ok(),
            "a refused trade route must not fail the login"
        );
        assert!(matches!(
            handle.trade_route_for("CME").await,
            Err(RithmicError::NoTradeRoute { .. })
        ));
    }
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

/// The plant sends what it is given and Rithmic judges it: an unpriced limit
/// order goes out rather than being refused here. A market order has no price
/// by design. Either way an unset price is omitted, not sent as zero.
#[tokio::test]
async fn an_unset_price_is_omitted_on_the_wire() {
    let (mut plant, _sender, mut client) = plant_with_wire().await;
    cache_route(&mut plant, "CME", "globex");

    for price_type in [OrderType::Limit, OrderType::Market] {
        let request: crate::rti::RequestNewOrder =
            sent_request(&mut plant, &mut client, |response_sender| {
                OrderPlantCommand::PlaceOrder {
                    order: RithmicOrder {
                        exchange: "CME".to_string(),
                        price_type,
                        price: None,
                        ..RithmicOrder::default()
                    },
                    account: test_account(),
                    response_sender,
                }
            })
            .await;

        assert_eq!(request.price, None, "{price_type:?}");
    }
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
                    .symbol("ESZ6")
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

#[tokio::test]
async fn subscribe_all_retains_every_account() {
    let (sender, _rx) = mpsc::channel(4);
    let (subscription_sender, _) = broadcast::channel(4);
    let plant = RithmicOrderPlant {
        sender,
        subscription_sender,
        connection_handle: tokio::spawn(async {}),
    };
    let mut receiver = plant.subscribe_all();
    for account in ["account-a", "account-b"] {
        let update = crate::rti::AccountPnLPositionUpdate {
            account_id: Some(account.into()),
            ..Default::default()
        };
        plant
            .subscription_sender
            .send(RithmicResponse {
                request_id: String::new(),
                source: "order_plant".into(),
                message: RithmicMessage::AccountPnLPositionUpdate(update),
                is_update: true,
                has_more: false,
                multi_response: false,
                error: None,
            })
            .unwrap();
        let RithmicMessage::AccountPnLPositionUpdate(update) =
            receiver.recv().await.unwrap().message
        else {
            panic!("missing account update")
        };
        assert_eq!(update.account_id.as_deref(), Some(account));
    }
    plant.connection_handle.await.unwrap();
}
