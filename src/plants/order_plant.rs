use std::sync::Arc;
use tracing::{error, info, warn};

use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    ConnectStrategy,
    api::{
        commands::{
            RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders,
            RithmicCancelOrder, RithmicExitPosition, RithmicLinkOrders, RithmicModifyOrder,
            RithmicModifyOrderReferenceData, RithmicOcoOrder, RithmicOrder,
        },
        receiver_api::RithmicResponse,
        sender_api::LoginScope,
    },
    config::{LoginConfig, RithmicAccount, RithmicConfig},
    error::RithmicError,
    plants::{
        actor::Plant,
        await_all_responses, await_first_response,
        kind::{Cx, PlantCommand, PlantKind},
        subscription::SubscriptionFilter,
        tag::answer_caller,
        trade_routes::TradeRouteCache,
    },
    request_handler::{RequestResult, Responder},
    rti::{TradeRoute, messages::RithmicMessage, request_login::SysInfraType},
    types::{EasyToBorrowRequest, FillHistoryRange, RmsUpdateBits},
};

/// Subscription channel capacity used when the config sets none.
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 10_000;

/// What a handle asks the order plant actor to do. Most variants send one
/// request and answer `response_sender` with its reply.
pub(crate) enum OrderPlantCommand {
    Close,
    Abort,
    GetSystemInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Login {
        config: LoginConfig,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    AccountList {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeOrderUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeBracketUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceBracketOrder {
        bracket_order: Box<RithmicBracketOrder>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyOrder {
        order: RithmicModifyOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyStop {
        adjustment: RithmicBracketLevelAdjustment,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyTarget {
        adjustment: RithmicBracketLevelAdjustment,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    CancelOrder {
        order: RithmicCancelOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrders {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    CancelAllOrders {
        command: RithmicCancelAllOrders,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetAccountRmsInfo {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetProductRmsInfo {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetTradeRoutes {
        subscribe_for_updates: bool,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    /// Apply a `TradeRoute` (350) update to the route cache. Sends nothing.
    RecordTradeRouteUpdate(Box<TradeRoute>),
    /// Look up the cached route for `exchange`. Sends nothing.
    TradeRouteFor {
        exchange: String,
        response_sender: oneshot::Sender<Result<String, RithmicError>>,
    },
    ShowOrderHistoryDates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistorySummary {
        date: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistoryDetail {
        basket_id: String,
        date: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrderHistory {
        basket_id: Option<String>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceOrder {
        order: RithmicOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceOcoOrder {
        order: RithmicOcoOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowBrackets {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowBracketStops {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ExitPosition {
        command: RithmicExitPosition,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    LinkOrders {
        command: RithmicLinkOrders,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetEasyToBorrowList {
        request_type: EasyToBorrowRequest,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyOrderReferenceData {
        command: RithmicModifyOrderReferenceData,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetOrderSessionConfig {
        should_defer_request: Option<bool>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ReplayExecutions {
        start_index_sec: i32,
        finish_index_sec: i32,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetUserInfo {
        user: Option<String>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowFillHistory {
        range: FillHistoryRange,
        max_record_count: Option<i32>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribeAccountRmsUpdates {
        subscribe: bool,
        update_bits: Vec<RmsUpdateBits>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetLoginInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    // Agreement-related commands
    ListUnacceptedAgreements {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ListAcceptedAgreements {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    AcceptAgreement {
        agreement_id: String,
        market_data_usage_capacity: Option<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowAgreement {
        agreement_id: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SetRithmicMrktDataSelfCertStatus {
        agreement_id: String,
        market_data_usage_capacity: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ListExchangePermissions {
        user: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
}

/// A connection to Rithmic's order plant, for placing and managing orders.
///
/// Through a [`RithmicOrderPlantHandle`] you can:
/// - Place, modify and cancel orders, including brackets and OCO groups
/// - Receive order status updates and fills
/// - Query order and fill history, RMS limits, and trade routes
/// - List and accept market data agreements
///
/// One plant is one WebSocket connection and one login. Get a handle per
/// account with [`get_handle`](Self::get_handle).
///
/// # Trade routes
///
/// Orders go out on the route the exchange publishes for your account, which
/// the plant loads at [`login`](RithmicOrderPlantHandle::login) and keeps
/// current. Set `.trade_route(..)` on a command to override it. With no route,
/// nothing is sent and the call returns [`RithmicError::NoTradeRoute`]; check
/// one with [`trade_route_for`](RithmicOrderPlantHandle::trade_route_for).
///
/// # Connection Health Monitoring
///
/// The subscription receiver carries order notifications (fills,
/// cancellations, and status changes) as well as connection health events:
/// - **WebSocket ping/pong timeouts**: primary dead-connection signal, sent as `HeartbeatTimeout`
/// - **Heartbeat errors**: forwarded as `HeartbeatTimeout`
/// - **Forced logout events**: session terminated by the server
/// - **Unexpected disconnects**: sent as `ConnectionError`
///
/// **Note:** Heartbeat requests are sent automatically for protocol compliance,
/// but successful responses are silently dropped. Only heartbeat errors from the server
/// are forwarded as `HeartbeatTimeout` messages.
///
/// # Example: Basic Usage
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicAccount, RithmicConfig, RithmicEnv, ConnectStrategy, RithmicOrderPlant,
///     api::{OrderSide, OrderType, RithmicBracketOrder},
///     rti::messages::RithmicMessage,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///     let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
///
///     let order_plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
///     let mut handle = order_plant.get_handle(&account);
///
///     handle.login().await?;
///     handle.subscribe_order_updates().await?;
///     handle.subscribe_bracket_updates().await?;
///
///     // Place a bracket order
///     let bracket_order =
///         RithmicBracketOrder::new()
///             .symbol("ESZ6")
///             .exchange("CME")
///             .quantity(1)
///             .action(OrderSide::Buy)
///             .price_type(OrderType::Limit)
///             .price(4500.00)
///             .target(8)
///             .stop(4)
///             .localid("order1")
///             .build()?;
///
///     handle.place_bracket_order(bracket_order).await?;
///
///     // Monitor order updates with error handling
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 // Check for errors on all messages
///                 if let Some(err) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, err);
///                     if err.is_connection_issue() {
///                         eprintln!("Connection health issue - reconnection needed");
///                         break;
///                     }
///                     continue;
///                 }
///
///                 match update.message {
///                     RithmicMessage::RithmicOrderNotification(order) => {
///                         println!("Order notification: {:?}", order);
///                     }
///
///                     RithmicMessage::ExchangeOrderNotification(order) => {
///                         println!("Exchange notification: {:?}", order);
///                     }
///
///                     _ => {}
///                 }
///             }
///
///             Err(e) => {
///                 eprintln!("Channel error: {}", e);
///
///                 break;
///             }
///         }
///     }
///
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct RithmicOrderPlant {
    pub(crate) connection_handle: JoinHandle<()>,
    sender: mpsc::Sender<OrderPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicOrderPlant {
    /// Open a WebSocket connection to the order plant.
    ///
    /// This only connects. Call [`RithmicOrderPlantHandle::login`] on a handle
    /// before sending anything else.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy; see [`ConnectStrategy`]
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] when
    /// its one attempt fails. `Retry` and `AlternateWithRetry` return it only
    /// once the config's
    /// [`retry_timeout`](crate::RithmicConfigBuilder::retry_timeout)
    /// passes, with the attempt count and the timeout in the message. Without
    /// one they retry until they connect, so this call can block
    /// indefinitely if the server is unreachable.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicOrderPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<OrderPlantCommand>(64);
        let capacity = config
            .subscription_capacity
            .unwrap_or(DEFAULT_SUBSCRIPTION_CAPACITY);
        let (sub_tx, _sub_rx) = broadcast::channel(capacity);
        let mut order_plant = Plant::new(
            OrderPlant::default(),
            req_rx,
            sub_tx.clone(),
            config,
            strategy,
        )
        .await?;

        let connection_handle = tokio::spawn(async move {
            order_plant.run().await;
        });

        Ok(RithmicOrderPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicOrderPlant {
    /// Wait for the plant's background connection task to finish.
    ///
    /// The task ends after [`disconnect`](RithmicOrderPlantHandle::disconnect),
    /// [`abort`](RithmicOrderPlantHandle::abort), or when the connection drops.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle that sends commands for `account`.
    ///
    /// You can create several handles on one plant, one per account. They
    /// share the connection and the login. Each handle's
    /// [`subscription_receiver`](RithmicOrderPlantHandle::subscription_receiver)
    /// only sees updates sent after the handle was created.
    pub fn get_handle(&self, account: &RithmicAccount) -> RithmicOrderPlantHandle {
        let account = Arc::new(account.clone());
        let account_for_filter = Arc::clone(&account);

        RithmicOrderPlantHandle {
            account,
            sender: self.sender.clone(),
            subscription_receiver: SubscriptionFilter::new(
                account_for_filter,
                self.subscription_sender.subscribe(),
            ),
        }
    }

    /// Subscribe to this plant's updates for every account, unfiltered.
    ///
    /// Unlike the handle from [`Self::get_handle`], which only yields updates
    /// for its own account, this receiver yields updates for every account on
    /// the login, plus the connection health events.
    pub fn subscribe_all(&self) -> broadcast::Receiver<RithmicResponse> {
        self.subscription_sender.subscribe()
    }
}

/// A request the order plant sends for itself.
#[derive(Debug)]
enum OrderTag {
    /// The login info, which scopes later requests. `caller` is the handle
    /// that asked for it, or `None` when the plant loads it after login.
    LoginInfo { caller: Option<Responder> },
    /// The trade routes the plant loads after login, which orders go out on.
    TradeRoutes,
}

/// The order plant's commands, and what it loads after login and keeps for
/// the connection. Only the actor writes it.
#[derive(Debug, Default)]
struct OrderPlant {
    /// Scopes the requests that carry a user type. Set by the first login
    /// info that has one.
    login_scope: Option<LoginScope>,
    /// The routes orders go out on.
    trade_routes: TradeRouteCache,
    /// The login info loaded after login is still outstanding.
    loading_login_info: bool,
    /// The trade routes loaded after login are still outstanding.
    loading_trade_routes: bool,
}

impl OrderPlant {
    /// Scope later requests with the login info in `reply`, unless a scope is
    /// already set. A rejected response has no usable identity in it.
    fn record_login_info(&mut self, reply: &RequestResult) {
        if self.login_scope.is_some() {
            return;
        }

        // A `match` rather than a let-chain: those need Rust 1.88 and the MSRV
        // is 1.85.
        if let Ok(frames) = reply {
            if let Some(response) = frames.first() {
                match &response.message {
                    RithmicMessage::ResponseLoginInfo(info) if response.error.is_none() => {
                        self.login_scope = LoginScope::from_login_info(info);
                    }
                    _ => {}
                }
            }
        }
    }

    /// Fill the route cache from the reply to the routes loaded after login.
    /// A rejected frame is never cached.
    fn record_trade_routes(&mut self, responses: &[RithmicResponse]) {
        let loaded = responses
            .iter()
            .filter(|response| self.trade_routes.record_response(response))
            .count();

        match loaded {
            0 => {
                error!("order_plant: no trade routes published, orders will fail with NoTradeRoute")
            }
            loaded => info!("order_plant: {} trade routes loaded", loaded),
        }
    }
}

impl PlantKind for OrderPlant {
    type Command = OrderPlantCommand;
    type Tag = OrderTag;

    const SOURCE: &'static str = "order_plant";
    const INFRA: SysInfraType = SysInfraType::OrderPlant;

    fn shared(command: OrderPlantCommand) -> Result<PlantCommand, OrderPlantCommand> {
        match command {
            OrderPlantCommand::Close => Ok(PlantCommand::Close),
            OrderPlantCommand::Abort => Ok(PlantCommand::Abort),
            OrderPlantCommand::GetSystemInfo { response_sender } => {
                Ok(PlantCommand::GetSystemInfo { response_sender })
            }
            OrderPlantCommand::Login {
                config,
                response_sender,
            } => Ok(PlantCommand::Login {
                config,
                response_sender,
            }),
            OrderPlantCommand::Logout { response_sender } => {
                Ok(PlantCommand::Logout { response_sender })
            }
            command => Err(command),
        }
    }

    /// Load the login info and the routes orders are sent on. The route
    /// request subscribes, so updates reach subscribers, but only
    /// [`RithmicOrderPlantHandle::record_trade_route`] applies one.
    fn after_login(&mut self, cx: &mut Cx<'_, OrderTag>) {
        self.loading_login_info = true;
        self.loading_trade_routes = true;

        cx.send(
            |api| api.request_login_info(),
            OrderTag::LoginInfo { caller: None },
        );
        cx.send(|api| api.request_trade_routes(true), OrderTag::TradeRoutes);
    }

    fn is_ready(&self) -> bool {
        !self.loading_login_info && !self.loading_trade_routes
    }

    fn on_command(&mut self, command: OrderPlantCommand, cx: &mut Cx<'_, OrderTag>) {
        match command {
            OrderPlantCommand::AccountList { response_sender } => {
                // Warn here too, not just at login: this is where the wider list
                // comes back.
                if self.login_scope.is_none() {
                    warn!("order_plant: no login info retained, listing accounts unscoped");
                }

                cx.send_for(
                    |api| api.request_account_list(self.login_scope.as_ref()),
                    response_sender,
                );
            }
            OrderPlantCommand::SubscribeOrderUpdates {
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_subscribe_for_order_updates(&account),
                response_sender,
            ),
            OrderPlantCommand::SubscribeBracketUpdates {
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_subscribe_to_bracket_updates(&account),
                response_sender,
            ),
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order,
                account,
                response_sender,
            } => {
                let trade_route = match self.trade_routes.resolve(
                    bracket_order.trade_route.as_deref(),
                    &bracket_order.exchange,
                ) {
                    Ok(trade_route) => trade_route,
                    Err(err) => {
                        let _ = response_sender.send(Err(err));
                        return;
                    }
                };

                cx.send_for(
                    |api| {
                        api.request_bracket_order(
                            *bracket_order,
                            &account,
                            self.login_scope.as_ref(),
                            &trade_route,
                        )
                    },
                    response_sender,
                );
            }
            OrderPlantCommand::ModifyOrder {
                order,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_modify_order(&order, &account),
                response_sender,
            ),
            OrderPlantCommand::CancelOrder {
                order,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_cancel_order(&order, &account),
                response_sender,
            ),
            OrderPlantCommand::ModifyStop {
                adjustment,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_update_stop_bracket_level(&adjustment, &account),
                response_sender,
            ),
            OrderPlantCommand::ModifyTarget {
                adjustment,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_update_target_bracket_level(&adjustment, &account),
                response_sender,
            ),
            OrderPlantCommand::ShowOrders {
                account,
                response_sender,
            } => cx.send_for(|api| api.request_show_orders(&account), response_sender),
            OrderPlantCommand::CancelAllOrders {
                command,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_cancel_all_orders(&command, &account, self.login_scope.as_ref()),
                response_sender,
            ),
            OrderPlantCommand::GetAccountRmsInfo {
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_account_rms_info(&account, self.login_scope.as_ref()),
                response_sender,
            ),
            OrderPlantCommand::GetProductRmsInfo {
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_product_rms_info(&account),
                response_sender,
            ),
            OrderPlantCommand::GetTradeRoutes {
                subscribe_for_updates,
                response_sender,
            } => cx.send_for(
                |api| api.request_trade_routes(subscribe_for_updates),
                response_sender,
            ),
            OrderPlantCommand::RecordTradeRouteUpdate(update) => {
                self.trade_routes.record_update(&update);
            }
            OrderPlantCommand::TradeRouteFor {
                exchange,
                response_sender,
            } => {
                let _ = response_sender.send(self.trade_routes.resolve(None, &exchange));
            }
            OrderPlantCommand::ShowOrderHistoryDates { response_sender } => cx.send_for(
                |api| api.request_show_order_history_dates(),
                response_sender,
            ),
            OrderPlantCommand::ShowOrderHistorySummary {
                date,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_order_history_summary(&date, &account),
                response_sender,
            ),
            OrderPlantCommand::ShowOrderHistoryDetail {
                basket_id,
                date,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_order_history_detail(&basket_id, &date, &account),
                response_sender,
            ),
            OrderPlantCommand::ShowOrderHistory {
                basket_id,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_order_history(basket_id.as_deref(), &account),
                response_sender,
            ),
            OrderPlantCommand::PlaceOrder {
                order,
                account,
                response_sender,
            } => {
                let trade_route = match self
                    .trade_routes
                    .resolve(order.trade_route.as_deref(), &order.exchange)
                {
                    Ok(trade_route) => trade_route,
                    Err(err) => {
                        let _ = response_sender.send(Err(err));
                        return;
                    }
                };

                cx.send_for(
                    |api| api.request_order(&order, &account, &trade_route),
                    response_sender,
                );
            }
            OrderPlantCommand::PlaceOcoOrder {
                order,
                account,
                response_sender,
            } => {
                let timing = order.cancel_timing();

                let legs = match self.trade_routes.resolve_legs(order.legs) {
                    Ok(legs) => legs,
                    Err(err) => {
                        let _ = response_sender.send(Err(err));
                        return;
                    }
                };

                cx.try_send_for(
                    |api| api.request_oco_order(legs, timing, &account),
                    response_sender,
                );
            }
            OrderPlantCommand::ShowBrackets {
                account,
                response_sender,
            } => cx.send_for(|api| api.request_show_brackets(&account), response_sender),
            OrderPlantCommand::ShowBracketStops {
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_bracket_stops(&account),
                response_sender,
            ),
            OrderPlantCommand::ExitPosition {
                command,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_exit_position(&command, &account),
                response_sender,
            ),
            OrderPlantCommand::LinkOrders {
                command,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_link_orders(command, &account),
                response_sender,
            ),
            OrderPlantCommand::GetEasyToBorrowList {
                request_type,
                response_sender,
            } => cx.send_for(
                |api| api.request_easy_to_borrow_list(request_type),
                response_sender,
            ),
            OrderPlantCommand::ModifyOrderReferenceData {
                command,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_modify_order_reference_data(&command, &account),
                response_sender,
            ),
            OrderPlantCommand::GetOrderSessionConfig {
                should_defer_request,
                response_sender,
            } => cx.send_for(
                |api| api.request_order_session_config(should_defer_request),
                response_sender,
            ),
            OrderPlantCommand::ReplayExecutions {
                start_index_sec,
                finish_index_sec,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_replay_executions(start_index_sec, finish_index_sec, &account),
                response_sender,
            ),
            OrderPlantCommand::GetUserInfo {
                user,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_get_user_info(user.as_deref(), &account),
                response_sender,
            ),
            OrderPlantCommand::ShowFillHistory {
                range,
                max_record_count,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_fill_history(range, max_record_count, &account),
                response_sender,
            ),
            OrderPlantCommand::SubscribeAccountRmsUpdates {
                subscribe,
                update_bits,
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_account_rms_updates(subscribe, update_bits, &account),
                response_sender,
            ),
            OrderPlantCommand::GetLoginInfo { response_sender } => cx.send(
                |api| api.request_login_info(),
                OrderTag::LoginInfo {
                    caller: Some(response_sender),
                },
            ),
            OrderPlantCommand::ListUnacceptedAgreements { response_sender } => cx.send_for(
                |api| api.request_list_unaccepted_agreements(),
                response_sender,
            ),
            OrderPlantCommand::ListAcceptedAgreements { response_sender } => cx.send_for(
                |api| api.request_list_accepted_agreements(),
                response_sender,
            ),
            OrderPlantCommand::AcceptAgreement {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_accept_agreement(
                        &agreement_id,
                        market_data_usage_capacity.as_deref(),
                    )
                },
                response_sender,
            ),
            OrderPlantCommand::ShowAgreement {
                agreement_id,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_agreement(&agreement_id),
                response_sender,
            ),
            OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_set_rithmic_mrkt_data_self_cert_status(
                        &agreement_id,
                        &market_data_usage_capacity,
                    )
                },
                response_sender,
            ),
            OrderPlantCommand::ListExchangePermissions {
                user,
                response_sender,
            } => cx.send_for(
                |api| api.request_list_exchange_permissions(&user),
                response_sender,
            ),
            OrderPlantCommand::Close
            | OrderPlantCommand::Abort
            | OrderPlantCommand::GetSystemInfo { .. }
            | OrderPlantCommand::Login { .. }
            | OrderPlantCommand::Logout { .. } => {
                unreachable!("the plant handles the commands every plant shares")
            }
        }
    }

    /// A failure here is only logged: the login already succeeded, so it
    /// leaves later requests unscoped, or orders failing with
    /// [`RithmicError::NoTradeRoute`], rather than failing the connection.
    fn on_reply(&mut self, tag: OrderTag, reply: RequestResult) {
        match tag {
            OrderTag::LoginInfo { caller } => {
                self.record_login_info(&reply);

                match caller {
                    Some(caller) => answer_caller(caller, reply),
                    None => {
                        self.loading_login_info = false;

                        match reply.as_ref().map(|frames| frames.first()) {
                            Ok(Some(response)) => {
                                if let Some(err) = &response.error {
                                    warn!(
                                        "order_plant: login info rejected, account list will be unscoped: {:?}",
                                        err
                                    );
                                }
                            }
                            Ok(None) => warn!(
                                "order_plant: login info unavailable, account list will be unscoped: {:?}",
                                RithmicError::EmptyResponse
                            ),
                            Err(err) => warn!(
                                "order_plant: login info unavailable, account list will be unscoped: {:?}",
                                err
                            ),
                        }
                    }
                }
            }
            OrderTag::TradeRoutes => {
                self.loading_trade_routes = false;

                match reply {
                    Ok(responses) => {
                        for rejection in responses.iter().filter_map(|resp| resp.error.as_ref()) {
                            error!(
                                "order_plant: trade route request rejected, orders will fail: {}",
                                rejection
                            );
                        }

                        self.record_trade_routes(&responses);
                    }
                    Err(err) => error!(
                        "order_plant: trade routes unavailable, orders will fail: {}",
                        err
                    ),
                }
            }
        }
    }
}

/// Handle for sending commands to a [`RithmicOrderPlant`] and receiving order updates.
///
/// Obtained from [`RithmicOrderPlant::get_handle`], one per account. Use the methods
/// on this handle to log in, place/modify/cancel orders, and query account
/// information. Real-time order updates arrive on
/// [`subscription_receiver`](Self::subscription_receiver).
///
/// # What the methods return
///
/// A request the server turns down still comes back as `Ok`. Check
/// [`RithmicResponse::error`] on each response. [`login`](Self::login) is the
/// exception and returns the refusal as `Err`.
///
/// Methods that return a `Vec` collect every frame of a multi-part reply. The
/// last frame ends the reply and usually carries no data of its own.
///
/// `Err` means no reply: [`RithmicError::ConnectionClosed`] when the plant
/// shut down or disconnected first, [`RithmicError::SendFailed`] when the
/// request could not be written, and [`RithmicError::EmptyResponse`] when a
/// single-response method got no frames.
///
/// Many requests only get an acknowledgement back. What they ask for, such
/// as order status or cancels, arrives later on the subscription receiver.
pub struct RithmicOrderPlantHandle {
    account: Arc<RithmicAccount>,
    sender: mpsc::Sender<OrderPlantCommand>,
    /// Updates for this handle's account, plus connection health events and
    /// updates that name no account, such as `TradeRoute`.
    pub subscription_receiver: SubscriptionFilter,
}

impl std::fmt::Debug for RithmicOrderPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicOrderPlantHandle")
            .field("account", &self.account)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

impl RithmicOrderPlantHandle {
    /// List the Rithmic systems (such as `Rithmic Paper Trading`) this server
    /// offers, and whether each has aggregated quotes. Does not need a login.
    pub async fn get_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Log in to the Rithmic Order plant
    ///
    /// This must be called before sending orders or subscriptions.
    ///
    /// Once the server accepts the login, the plant loads the login info, which
    /// scopes later requests, and the trade routes orders are sent on. It does
    /// so even if you stop waiting for this call. A failure to load either is
    /// only logged and the login still succeeds. Without the login info,
    /// requests such as [`get_account_list`](Self::get_account_list) go out
    /// unscoped. Without routes, orders fail with [`RithmicError::NoTradeRoute`].
    ///
    /// The plant logs in once per connection. A call with the same config made
    /// while that login is in progress waits for it, and one made after it
    /// returns its response at once. Neither sends anything.
    ///
    /// # Returns
    /// The login response, once the login info and trade routes are recorded
    /// or have failed.
    ///
    /// # Errors
    /// * The error the server's refusal carries, usually
    ///   [`RithmicError::RequestRejected`]. You can log in again.
    /// * [`RithmicError::LoginConflict`] if this plant is logging in, or is
    ///   logged in, with a different [`LoginConfig`].
    /// * [`RithmicError::ConnectionClosed`] if the plant disconnects before
    ///   the login is done, or has disconnected.
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic Order plant with custom configuration
    ///
    /// This must be called before sending orders or subscriptions. It loads
    /// the login info and trade routes, like [`login`](Self::login).
    /// `aggregated_quotes` does not apply to this plant and is ignored.
    ///
    /// # Arguments
    /// * `config` - Login configuration options. See [`LoginConfig`] for details.
    ///
    /// # Returns
    /// The login response, once the login info and trade routes are recorded
    /// or have failed.
    ///
    /// # Errors
    /// As for [`login`](Self::login). [`RithmicError::LoginConflict`] means
    /// this plant logged in, or is logging in, with a config other than
    /// `config`.
    pub async fn login_with_config(
        &self,
        config: LoginConfig,
    ) -> Result<RithmicResponse, RithmicError> {
        info!("order_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();
        let mut config = config;

        config.aggregated_quotes = None;

        let command = OrderPlantCommand::Login {
            config,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = await_first_response(rx).await?;

        if let Some(err) = response.error.clone() {
            error!("order_plant: login failed {:?}", err);

            return Err(err);
        }

        // The actor owns the session: it heartbeats, and has loaded the login
        // info and trade routes, before this reply reaches us.
        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(session_id) = &resp.unique_user_id {
                info!("order_plant: session id: {}", session_id);
            }
        }

        info!("order_plant: logged in");

        Ok(response)
    }

    /// Log out, then close the connection.
    ///
    /// The connection is closed even if the logout fails or gets no reply.
    /// Pending requests fail with [`RithmicError::ConnectionClosed`], and so
    /// do commands sent after this from any handle on the plant.
    ///
    /// # Returns
    /// The logout response. A server refusal is on its `error` field.
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        // Held rather than propagated here so that `Close` is queued either way:
        // the logout has already closed the session, so an actor that
        // never receives `Close` stops sending heartbeats, drops every later
        // command, and never drains its pending requests.
        let outcome = rx.await.map_err(|_| RithmicError::ConnectionClosed);
        let _ = self.sender.send(OrderPlantCommand::Close).await;

        outcome??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Immediately shut down the order plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and a graceful `disconnect()`
    /// would not get through. Pending requests fail with
    /// [`RithmicError::ConnectionClosed`] and the subscription channel receives a
    /// `ConnectionError` notification. Safe to call if the actor is already dead.
    ///
    /// The abort is queued behind commands already sent. If that queue is
    /// full, the abort is dropped without notice.
    pub fn abort(&self) {
        let _ = self.sender.try_send(OrderPlantCommand::Abort);
    }

    /// List the accounts this login can trade, one response per account.
    ///
    /// The request is scoped by the login info [`Self::login`] loads. If that
    /// load failed, it goes out unscoped and may list more accounts.
    pub async fn get_account_list(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::AccountList {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Subscribe to order status updates for this handle's account.
    ///
    /// Updates arrive on [`subscription_receiver`](Self::subscription_receiver) as
    /// [`RithmicOrderNotification`] and [`ExchangeOrderNotification`]. Requires
    /// [`login`](Self::login) first. Bracket-specific updates need
    /// [`subscribe_bracket_updates`](Self::subscribe_bracket_updates) as well.
    ///
    /// ```no_run
    /// # use rithmic_rs::{RithmicOrderPlantHandle, rti::messages::RithmicMessage};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// handle.subscribe_order_updates().await?;
    /// let mut updates = handle.subscription_receiver.resubscribe();
    ///
    /// while let Ok(response) = updates.recv().await {
    ///     if let RithmicMessage::ExchangeOrderNotification(order) = &response.message {
    ///         println!("{:?} filled {:?}", order.status, order.fill_size);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`RithmicOrderNotification`]: crate::rti::messages::RithmicMessage::RithmicOrderNotification
    /// [`ExchangeOrderNotification`]: crate::rti::messages::RithmicMessage::ExchangeOrderNotification
    pub async fn subscribe_order_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeOrderUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Subscribe to bracket updates for this handle's account.
    ///
    /// Updates arrive on [`subscription_receiver`](Self::subscription_receiver)
    /// as [`RithmicMessage::BracketUpdates`]. Order status for the bracket's
    /// legs needs [`subscribe_order_updates`](Self::subscribe_order_updates).
    pub async fn subscribe_bracket_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeBracketUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Place a bracket order — entry with linked profit target and stop loss.
    ///
    /// Build the order with [`RithmicBracketOrder::build`], which validates it. This
    /// method does not re-validate: an order assembled without `build()` goes to the
    /// exchange as-is.
    ///
    /// `Ok` means the request was sent, not that it was accepted — check `error` on
    /// each response.
    ///
    /// ```no_run
    /// # use rithmic_rs::{OrderSide, OrderType, RithmicBracketOrder, RithmicOrderPlantHandle};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let order = RithmicBracketOrder::new()
    ///     .symbol("ESZ6")
    ///     .exchange("CME")
    ///     .quantity(1)
    ///     .action(OrderSide::Buy)
    ///     .price_type(OrderType::Limit)
    ///     .price(5000.0)
    ///     .target(20)
    ///     .stop(10)
    ///     .localid("my-order-1")
    ///     .build()?;
    ///
    /// for response in handle.place_bracket_order(order).await? {
    ///     if let Some(err) = &response.error {
    ///         eprintln!("rejected: {err}");
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// * [`RithmicError::NoTradeRoute`] if no route covers the order's exchange and
    ///   the order named none. Nothing is sent.
    /// * [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn place_bracket_order(
        &self,
        bracket_order: RithmicBracketOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceBracketOrder {
            bracket_order: Box::new(bracket_order),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Modify a working order's price, quantity or type.
    ///
    /// A modify restates the order, so see [`RithmicModifyOrder`] for which
    /// fields to carry over unchanged.
    ///
    /// Like [`cancel_order`](Self::cancel_order), the result describes the
    /// request. The order's new state arrives as order notifications on the
    /// subscription receiver.
    pub async fn modify_order(
        &self,
        order: RithmicModifyOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyOrder {
            order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Cancel one working order by its basket id.
    ///
    /// Resolves when the final frame of the response sequence arrives. That
    /// result describes the request, not the order. Order state arrives
    /// separately as [`RithmicOrderNotification`] updates on the subscription
    /// stream; those carry an empty `request_id` and are broadcast to
    /// subscribers, so they never resolve this call.
    ///
    /// [`RithmicOrderNotification`]: crate::rti::messages::RithmicMessage::RithmicOrderNotification
    ///
    /// ```no_run
    /// # use rithmic_rs::{RithmicCancelOrder, RithmicOrderPlantHandle};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// // "123456" is the basket_id from the order notification.
    /// let cancel = RithmicCancelOrder::new().id("123456").build()?;
    /// handle.cancel_order(cancel).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn cancel_order(
        &self,
        order: RithmicCancelOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::CancelOrder {
            order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Move the profit target of a bracket to a new distance in ticks.
    ///
    /// # Arguments
    /// * `adjustment` - The bracket, the new tick distance, and the leg to adjust
    pub async fn adjust_target(
        &self,
        adjustment: RithmicBracketLevelAdjustment,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyTarget {
            adjustment,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Move the stop loss of a bracket to a new distance in ticks.
    ///
    /// # Arguments
    /// * `adjustment` - The bracket, the new tick distance, and the leg to adjust
    pub async fn adjust_stop(
        &self,
        adjustment: RithmicBracketLevelAdjustment,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyStop {
            adjustment,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Ask Rithmic to replay the account's open orders onto the update stream.
    ///
    /// The returned `RithmicResponse` is only an acknowledgement —
    /// `ResponseShowOrders` carries a response code and nothing else. Each open
    /// order arrives separately as a
    /// [`RithmicMessage::RithmicOrderNotification`] or
    /// [`RithmicMessage::ExchangeOrderNotification`] on the subscription stream,
    /// so subscribe before calling this or the orders are missed.
    ///
    /// The crate surfaces no end-of-list signal, so the replayed orders are
    /// indistinguishable from live activity on the stream.
    ///
    /// ```no_run
    /// # use rithmic_rs::{rti::messages::RithmicMessage, RithmicOrderPlantHandle};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut updates = handle.subscription_receiver.resubscribe();
    /// handle.show_orders().await?;
    ///
    /// while let Ok(response) = updates.recv().await {
    ///     if let RithmicMessage::RithmicOrderNotification(order) = response.message {
    ///         println!("{:?} {:?}", order.symbol, order.status);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_orders(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrders {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Cancel every working order on this handle's account.
    ///
    /// The response only acknowledges the request. Each cancel arrives as an
    /// order notification on the subscription receiver.
    pub async fn cancel_all_orders(
        &self,
        command: RithmicCancelAllOrders,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::CancelAllOrders {
            command,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get account RMS (Risk Management System) limits, one response per account.
    ///
    /// Template 304 names no account, so like [`get_account_list`](Self::get_account_list)
    /// this covers every account the login reaches, not just this handle's.
    pub async fn get_account_rms_info(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetAccountRmsInfo {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Get this account's per-product RMS (Risk Management System) limits,
    /// one response per product.
    pub async fn get_product_rms_info(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetProductRmsInfo {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List the trade routes the server publishes, one response per route.
    ///
    /// This only reports the routes. It does not change the routes orders go
    /// out on; see [`record_trade_route`](Self::record_trade_route) for that.
    /// [`login`](Self::login) already subscribes to route updates.
    ///
    /// # Arguments
    /// * `subscribe_for_updates` - Whether to receive `TradeRoute` updates when routes change
    pub async fn get_trade_routes(
        &self,
        subscribe_for_updates: bool,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetTradeRoutes {
            subscribe_for_updates,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Apply a `TradeRoute` update to the routes orders go out on.
    ///
    /// [`login`](Self::login) subscribes, so updates arrive on
    /// [`subscription_receiver`](Self::subscription_receiver); applying them is up
    /// to you. The update replaces the route held for its exchange.
    ///
    /// This returns once the update is queued, not applied. Orders sent after
    /// it from the same task still go out on the new route.
    ///
    /// ```no_run
    /// # use rithmic_rs::{RithmicOrderPlantHandle, rti::messages::RithmicMessage};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut updates = handle.subscription_receiver.resubscribe();
    ///
    /// while let Ok(response) = updates.recv().await {
    ///     if let RithmicMessage::TradeRoute(update) = &response.message {
    ///         handle.record_trade_route(update).await?;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn record_trade_route(&self, update: &TradeRoute) -> Result<(), RithmicError> {
        self.sender
            .send(OrderPlantCommand::RecordTradeRouteUpdate(Box::new(
                update.clone(),
            )))
            .await
            .map_err(|_| RithmicError::ConnectionClosed)
    }

    /// The route an order for `exchange` would go out on right now, without sending
    /// anything. Call it after [`login`](Self::login) to check your venues are routable.
    ///
    /// # Arguments
    /// * `exchange` - The exchange to look up, as it appears on your orders
    ///
    /// # Errors
    /// * [`RithmicError::NoTradeRoute`] if no route is held for `exchange`. It
    ///   lists the exchanges that do have one.
    /// * [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn trade_route_for(&self, exchange: &str) -> Result<String, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<String, RithmicError>>();

        let command = OrderPlantCommand::TradeRouteFor {
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// List the dates (YYYYMMDD) that have order history, for use with
    /// [`show_order_history_summary`](Self::show_order_history_summary).
    pub async fn show_order_history_dates(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistoryDates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Ask for a summary of this account's orders on one date.
    ///
    /// The response only acknowledges the request and has no order fields.
    /// Watch [`subscription_receiver`](Self::subscription_receiver) for the
    /// orders the server sends back.
    ///
    /// # Arguments
    /// * `date` - Date in YYYYMMDD format (e.g., "20250122")
    pub async fn show_order_history_summary(
        &self,
        date: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistorySummary {
            date: date.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Ask for the full history of one order on one date.
    ///
    /// The response only acknowledges the request and has no order fields.
    /// Watch [`subscription_receiver`](Self::subscription_receiver) for the
    /// orders the server sends back.
    ///
    /// # Arguments
    /// * `basket_id` - Order/basket identifier
    /// * `date` - Date in YYYYMMDD format
    pub async fn show_order_history_detail(
        &self,
        basket_id: &str,
        date: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistoryDetail {
            basket_id: basket_id.to_string(),
            date: date.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Ask for this account's order history, optionally for one basket.
    ///
    /// The response only acknowledges the request and has no order fields.
    /// Watch [`subscription_receiver`](Self::subscription_receiver) for the
    /// orders the server sends back.
    ///
    /// # Arguments
    /// * `basket_id` - Limit the history to this order. `None` asks for all.
    pub async fn show_order_history(
        &self,
        basket_id: Option<&str>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistory {
            basket_id: basket_id.map(|s| s.to_string()),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Place a single order built with [`RithmicOrder`].
    ///
    /// Supports every order type, including stop orders with a trigger price
    /// and trailing stops. For an entry with an attached profit target and
    /// stop loss, use [`place_bracket_order`](Self::place_bracket_order).
    ///
    /// The order goes out on the route held for its exchange, unless it names
    /// its own `trade_route`.
    ///
    /// Build the order with [`RithmicOrder::build`], which validates it. This method
    /// does not re-validate: an order assembled without `build()` goes to the
    /// exchange as-is.
    ///
    /// `Ok` means the request was sent, not that it was accepted — check `error` on
    /// each response.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use rithmic_rs::{OrderSide, OrderType, RithmicOrder, RithmicOrderPlantHandle};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let order = RithmicOrder::new()
    ///     .symbol("ESZ6")
    ///     .exchange("CME")
    ///     .quantity(1)
    ///     .transaction_type(OrderSide::Buy)
    ///     .price_type(OrderType::Limit)
    ///     .price(5000.0)
    ///     .user_tag("my-order")
    ///     .build()?;
    ///
    /// for response in handle.place_order(order).await? {
    ///     if let Some(err) = &response.error {
    ///         eprintln!("rejected: {err}");
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// * [`RithmicError::NoTradeRoute`] if no route covers the order's exchange and
    ///   the order named none. Nothing is sent.
    /// * [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn place_order(
        &self,
        order: RithmicOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceOrder {
            order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Place an OCO (One Cancels Other) order.
    ///
    /// When one leg is filled, the others are automatically cancelled. See
    /// [`RithmicOcoOrder`] for building the legs. Each leg goes out on the
    /// route for its own exchange, so a group can span exchanges.
    ///
    /// ```no_run
    /// # use rithmic_rs::{OrderSide, OrderType, RithmicOcoOrder, RithmicOcoOrderLeg, RithmicOrderPlantHandle};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let take_profit = RithmicOcoOrderLeg::new()
    ///     .symbol("ESZ6")
    ///     .exchange("CME")
    ///     .quantity(1)
    ///     .transaction_type(OrderSide::Sell)
    ///     .price_type(OrderType::Limit)
    ///     .price(5020.0)
    ///     .build()?;
    /// let stop_loss = RithmicOcoOrderLeg::new()
    ///     .symbol("ESZ6")
    ///     .exchange("CME")
    ///     .quantity(1)
    ///     .transaction_type(OrderSide::Sell)
    ///     .price_type(OrderType::StopMarket)
    ///     .trigger_price(4980.0)
    ///     .build()?;
    ///
    /// let order = RithmicOcoOrder::new().legs([take_profit, stop_loss]).build()?;
    /// handle.place_oco_order(order).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// * [`RithmicError::InvalidArgument`] if the group has fewer than two legs —
    ///   [`RithmicOcoOrder::build`] does not check the count, this does. Also if
    ///   a leg's price type cannot be sent in an OCO request.
    /// * [`RithmicError::NoTradeRoute`] if no route covers a leg's exchange.
    ///   Nothing is sent for any leg.
    /// * [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn place_oco_order(
        &self,
        order: RithmicOcoOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        // `legs()` is variadic and `validate()` deliberately ignores the count,
        // so a short group is only caught here. A lone leg has nothing to be
        // cancelled against, so it is rejected rather than sent.
        if order.legs.len() < 2 {
            return Err(RithmicError::InvalidArgument(
                "OCO order requires at least 2 legs".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceOcoOrder {
            order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List this account's active brackets, one response per bracket, with
    /// their target quantities and ticks.
    pub async fn show_brackets(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowBrackets {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List the stop legs of this account's active brackets, one response per
    /// stop.
    pub async fn show_bracket_stops(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowBracketStops {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Flatten a position — one instrument, or the whole account.
    ///
    /// The command's symbol and exchange select one instrument; with neither
    /// set, every open position on the account is exited.
    ///
    /// Resolves when the final frame of the response sequence arrives. That
    /// result describes the request, not the resulting orders.
    ///
    /// ```no_run
    /// # use rithmic_rs::{RithmicExitPosition, RithmicOrderPlantHandle};
    /// # async fn example(handle: RithmicOrderPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// // One instrument.
    /// let one = RithmicExitPosition::new().symbol("ESZ6").exchange("CME").build()?;
    /// handle.exit_position(one).await?;
    ///
    /// // Every open position on the account.
    /// handle.exit_position(RithmicExitPosition::new().build()?).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn exit_position(
        &self,
        command: RithmicExitPosition,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ExitPosition {
            command,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Link working orders on this account by basket id (template 344).
    ///
    /// The response only acknowledges the request.
    ///
    /// # Arguments
    /// * `command` - The basket IDs to link together
    pub async fn link_orders(
        &self,
        command: RithmicLinkOrders,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::LinkOrders {
            command,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get the easy-to-borrow list for short selling, one response per symbol.
    ///
    /// With [`EasyToBorrowRequest::Subscribe`], later changes arrive on the
    /// subscription receiver as `UpdateEasyToBorrowList`.
    ///
    /// # Arguments
    /// * `request_type` - Subscribe to the list, or unsubscribe from updates
    pub async fn get_easy_to_borrow_list(
        &self,
        request_type: EasyToBorrowRequest,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetEasyToBorrowList {
            request_type,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Change the user tag on a working order.
    ///
    /// # Arguments
    /// * `command` - The basket to retag and the new tag
    pub async fn modify_order_reference_data(
        &self,
        command: RithmicModifyOrderReferenceData,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyOrderReferenceData {
            command,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Set the order session config (template 3502).
    ///
    /// Despite the name, nothing is read back: the response carries only a
    /// response code.
    ///
    /// # Arguments
    /// * `should_defer_request` - If true, the server defers requests while it
    ///   loads reference data it does not have. `None` leaves the field off.
    pub async fn get_order_session_config(
        &self,
        should_defer_request: Option<bool>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetOrderSessionConfig {
            should_defer_request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Ask the server to replay this account's executions in a time window.
    ///
    /// The response only acknowledges the request and has no execution
    /// fields. Watch [`subscription_receiver`](Self::subscription_receiver)
    /// for what the server sends back.
    ///
    /// # Arguments
    /// * `start_index_sec` - Start time in unix seconds
    /// * `finish_index_sec` - End time in unix seconds
    pub async fn replay_executions(
        &self,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ReplayExecutions {
            start_index_sec,
            finish_index_sec,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Look up a user's profile: name, contact details, entitlement status,
    /// and session limits.
    ///
    /// # Arguments
    /// * `user` - The user to look up. `None` asks about the logged-in user.
    pub async fn get_user_info(
        &self,
        user: Option<&str>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetUserInfo {
            user: user.map(str::to_string),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Request the account's fill history, one response per fill.
    ///
    /// # Arguments
    /// * `range` - The window to report on
    /// * `max_record_count` - Cap on the number of fills returned, at most
    ///   10,000. `None` leaves the cap to the server.
    ///
    /// # Errors
    /// [`RithmicError::InvalidArgument`] when `max_record_count` is outside
    /// 0..=10,000 — Rithmic rejects a cap above 10,000. Nothing is sent.
    pub async fn show_fill_history(
        &self,
        range: FillHistoryRange,
        max_record_count: Option<i32>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        if let Some(count) = max_record_count.filter(|count| !(0..=10_000).contains(count)) {
            return Err(RithmicError::InvalidArgument(format!(
                "max_record_count must be between 0 and 10,000, got {count}"
            )));
        }

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowFillHistory {
            range,
            max_record_count,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Subscribe to, or unsubscribe from, RMS updates for this account.
    ///
    /// Updates arrive on the subscription receiver as
    /// [`RithmicMessage::AccountRmsUpdates`].
    ///
    /// # Arguments
    /// * `subscribe` - true to subscribe, false to unsubscribe
    /// * `update_bits` - which RMS fields to stream. Passing
    ///   `vec![RmsUpdateBits::AutoLiqThresholdCurrentValue]` streams
    ///   `auto_liq_threshold_current_value`; an empty `Vec` leaves the field
    ///   off the request.
    pub async fn subscribe_account_rms_updates(
        &self,
        subscribe: bool,
        update_bits: Vec<RmsUpdateBits>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeAccountRmsUpdates {
            subscribe,
            update_bits,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get the login info for this session: the user, their name, FCM and IB
    /// ids, and user type.
    ///
    /// [`Self::login`] already loads this, and the first successful load
    /// scopes later requests. A call here returns the response, and only
    /// scopes the plant if nothing has scoped it yet.
    pub async fn get_login_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetLoginInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// List the agreements this user has not accepted yet, one response per
    /// agreement.
    pub async fn list_unaccepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListUnacceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List the agreements this user has accepted, one response per agreement.
    pub async fn list_accepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListAcceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Accept a market data agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to accept
    /// * `market_data_usage_capacity` - Optional capacity indicator (e.g., "Professional", "Non-Professional")
    pub async fn accept_agreement(
        &self,
        agreement_id: &str,
        market_data_usage_capacity: Option<&str>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::AcceptAgreement {
            agreement_id: agreement_id.to_string(),
            market_data_usage_capacity: market_data_usage_capacity.map(|s| s.to_string()),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Get the text and details of one agreement.
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to display
    pub async fn show_agreement(
        &self,
        agreement_id: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowAgreement {
            agreement_id: agreement_id.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Set Rithmic market data self-certification status
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement
    /// * `market_data_usage_capacity` - The usage capacity (e.g., "Professional", "Non-Professional")
    pub async fn set_rithmic_mrkt_data_self_cert_status(
        &self,
        agreement_id: &str,
        market_data_usage_capacity: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
            agreement_id: agreement_id.to_string(),
            market_data_usage_capacity: market_data_usage_capacity.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// List exchange permissions for a user
    ///
    /// Returns the exchanges the user has permission to trade on, along with
    /// their entitlement status for each exchange.
    ///
    /// # Arguments
    /// * `user` - The username to query exchange permissions for
    pub async fn list_exchange_permissions(
        &self,
        user: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListExchangePermissions {
            user: user.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }
}

/// The clone shares the connection and account. Its subscription receiver
/// starts at the current stream position, not where the original is.
impl Clone for RithmicOrderPlantHandle {
    fn clone(&self) -> Self {
        RithmicOrderPlantHandle {
            account: Arc::clone(&self.account),
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_receiver.resubscribe(),
        }
    }
}

#[cfg(test)]
mod tests;
