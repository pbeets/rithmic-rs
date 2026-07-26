use std::sync::{Arc, OnceLock};

use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::RithmicResponse,
        rithmic_command_types::{
            LoginConfig, RithmicAdvancedBracketOrder, RithmicBracketLevelAdjustment,
            RithmicBracketOrder, RithmicCancelOrder, RithmicModifyOrder, RithmicOcoOrderLeg,
            RithmicOrder,
        },
        sender_api::LoginScope,
    },
    config::{RithmicAccount, RithmicConfig},
    error::RithmicError,
    plants::{
        core::{PlantCore, SelectResult},
        subscription::SubscriptionFilter,
        trade_routes::TradeRouteCache,
    },
    request_handler::RithmicRequest,
    rti::{
        TradeRoute, messages::RithmicMessage, request_account_rms_updates,
        request_easy_to_borrow_list, request_login::SysInfraType,
    },
    ws::PlantActor,
};

use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::sleep_until,
};

pub(crate) enum OrderPlantCommand {
    Close,
    Abort,
    ListSystemInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Login {
        config: LoginConfig,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UpdateHeartbeat {
        seconds: u64,
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
        bracket_order: RithmicBracketOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    PlaceAdvancedBracketOrder {
        bracket_order: Box<RithmicAdvancedBracketOrder>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyOrder {
        order: RithmicModifyOrder,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyStop {
        basket_id: String,
        ticks: i32,
        level: Option<i32>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyProfit {
        basket_id: String,
        ticks: i32,
        level: Option<i32>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    CancelOrder {
        order_id: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ShowOrders {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    CancelAllOrders {
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
    RecordTradeRoutes(Vec<RithmicResponse>),
    RecordTradeRouteUpdate(Box<TradeRoute>),
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
    PlaceOcoOrderMulti {
        legs: Vec<RithmicOcoOrderLeg>,
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
        symbol: String,
        exchange: String,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    LinkOrders {
        basket_ids: Vec<String>,
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetEasyToBorrowList {
        request_type: request_easy_to_borrow_list::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    ModifyOrderReferenceData {
        basket_id: String,
        user_tag: String,
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
    SubscribeAccountRmsUpdates {
        subscribe: bool,
        update_bits: Vec<request_account_rms_updates::UpdateBits>,
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

/// The RithmicOrderPlant provides functionality to manage trading orders through the Rithmic API.
///
/// It allows applications to:
/// - Place, modify and cancel orders
/// - Work with bracket orders (entry orders with profit targets and stop losses)
/// - Receive real-time order status updates
/// - Track positions and execution reports
///
/// # Connection Health Monitoring
///
/// The subscription receiver provides connection health events:
/// - **WebSocket ping/pong timeouts**: primary dead-connection signal (auto-detected)
/// - **Heartbeat errors**: forwarded as `HeartbeatTimeout`
/// - **Forced logout events**: session terminated by the server
/// - **Order notifications**: real-time order fills, cancellations, and status changes
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
///     RithmicBracketOrder, BracketTransactionType, BracketDuration, BracketPriceType,
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
///     let bracket_order = RithmicBracketOrder {
///         action: BracketTransactionType::Buy,
///         duration: BracketDuration::Day,
///         exchange: "CME".to_string(),
///         localid: "order1".to_string(),
///         price_type: BracketPriceType::Limit,
///         price: Some(4500.00),
///         profit_ticks: 8,
///         quantity: 1,
///         stop_ticks: 4,
///         symbol: "ESH6".to_string(),
///     };
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
    /// Shared with the actor and every handle, so one login scopes them all.
    login_scope: Arc<OnceLock<LoginScope>>,
}

impl RithmicOrderPlant {
    /// Create a new Order Plant connection
    ///
    /// Create a new Order Plant connection to manage trading orders.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicOrderPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if unable to establish WebSocket connection to the server.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicOrderPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<OrderPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);
        let login_scope = Arc::new(OnceLock::new());
        let mut order_plant = OrderPlant::new(
            req_rx,
            sub_tx.clone(),
            config,
            strategy,
            Arc::clone(&login_scope),
        )
        .await?;

        let connection_handle = tokio::spawn(async move {
            order_plant.run().await;
        });

        Ok(RithmicOrderPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
            login_scope,
        })
    }
}

impl RithmicOrderPlant {
    /// Wait for the plant's background connection task to finish.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to interact with the order plant.
    ///
    /// The handle provides methods to place orders, subscribe to updates, and manage positions.
    /// Multiple handles can be created from the same plant for different accounts.
    pub fn get_handle(&self, account: &RithmicAccount) -> RithmicOrderPlantHandle {
        let account = Arc::new(account.clone());
        let account_for_filter = Arc::clone(&account);

        RithmicOrderPlantHandle {
            account,
            login_scope: Arc::clone(&self.login_scope),
            sender: self.sender.clone(),
            subscription_receiver: SubscriptionFilter::new(
                account_for_filter,
                self.subscription_sender.subscribe(),
            ),
        }
    }
}

#[derive(Debug)]
struct OrderPlant {
    core: PlantCore,
    request_receiver: mpsc::Receiver<OrderPlantCommand>,
    login_scope: Arc<OnceLock<LoginScope>>,
    trade_routes: TradeRouteCache,
}

impl OrderPlant {
    async fn new(
        request_receiver: mpsc::Receiver<OrderPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
        login_scope: Arc<OnceLock<LoginScope>>,
    ) -> Result<OrderPlant, RithmicError> {
        let core = PlantCore::new(subscription_sender, config, strategy, "order_plant").await?;

        Ok(OrderPlant {
            core,
            request_receiver,
            login_scope,
            trade_routes: TradeRouteCache::default(),
        })
    }
}

impl PlantActor for OrderPlant {
    type Command = OrderPlantCommand;

    async fn run(&mut self) {
        loop {
            let result = {
                let interval = &mut self.core.interval;
                let ping_interval = &mut self.core.ping_interval;
                let ping_manager = &mut self.core.ping_manager;
                let reader = &mut self.core.rithmic_reader;
                let receiver = &mut self.request_receiver;

                tokio::select! {
                    _ = interval.tick()      => SelectResult::HeartbeatFired,
                    _ = ping_interval.tick() => SelectResult::PingFired,
                    _ = async {
                        if let Some(t) = ping_manager.next_timeout_at() {
                            sleep_until(t).await
                        } else {
                            std::future::pending::<()>().await
                        }
                    } => SelectResult::PingTimeout,
                    Some(cmd) = receiver.recv() => SelectResult::Command(cmd),
                    msg = reader.next() => match msg {
                        Some(m) => SelectResult::RithmicMessage(m),
                        None => SelectResult::StreamClosed,
                    },
                }
            };

            let stop = match result {
                SelectResult::HeartbeatFired => self.core.send_heartbeat().await,
                SelectResult::PingFired => self.core.send_ping().await,
                SelectResult::PingTimeout => {
                    if self.core.ping_manager.check_timeout() {
                        if self.core.close_requested {
                            warn!(
                                "order_plant: ping timed out while waiting for server close echo — terminating"
                            );

                            self.core.request_handler.drain_and_drop();
                        } else {
                            self.core.fail_connection_and_drain(
                                "websocket_ping_timeout",
                                RithmicError::HeartbeatTimeout,
                            );
                        }
                        true
                    } else {
                        false
                    }
                }
                SelectResult::Command(cmd) => {
                    if matches!(cmd, OrderPlantCommand::Abort) {
                        info!("order_plant: abort requested, shutting down immediately");

                        self.core
                            .fail_connection_and_drain("", RithmicError::ConnectionClosed);
                        true
                    } else {
                        self.handle_command(cmd).await;
                        false
                    }
                }
                SelectResult::RithmicMessage(msg) => self.core.handle_rithmic_message(msg).await,
                SelectResult::StreamClosed => self.core.handle_stream_closed(),
            };

            if stop {
                break;
            }
        }
    }

    async fn handle_command(&mut self, command: OrderPlantCommand) {
        // Disconnect race guard — see `TickerPlant::handle_command`.
        if self.core.close_requested
            && !matches!(
                command,
                OrderPlantCommand::Close
                    | OrderPlantCommand::SetLogin
                    | OrderPlantCommand::UpdateHeartbeat { .. }
                    | OrderPlantCommand::Abort
            )
        {
            debug!("order_plant: dropping a command queued after close was requested");

            return;
        }

        match command {
            OrderPlantCommand::Close => {
                self.core.handle_close().await;
            }
            OrderPlantCommand::ListSystemInfo { response_sender } => {
                self.core.handle_list_system_info(response_sender).await;
            }
            OrderPlantCommand::Login {
                config,
                response_sender,
            } => {
                self.core
                    .handle_login(config, SysInfraType::OrderPlant, response_sender)
                    .await;
            }
            OrderPlantCommand::SetLogin => {
                self.core.handle_set_login();
            }
            OrderPlantCommand::Logout { response_sender } => {
                self.core.handle_logout(response_sender).await;
            }
            OrderPlantCommand::UpdateHeartbeat { seconds } => {
                self.core.handle_update_heartbeat(seconds);
            }
            OrderPlantCommand::AccountList { response_sender } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_account_list(self.login_scope.get());

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::SubscribeOrderUpdates {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_subscribe_for_order_updates(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::SubscribeBracketUpdates {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_subscribe_to_bracket_updates(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order,
                account,
                response_sender,
            } => {
                let trade_route = match self.trade_routes.resolve(None, &bracket_order.exchange) {
                    Ok(trade_route) => trade_route,
                    Err(err) => {
                        let _ = response_sender.send(Err(err));
                        return;
                    }
                };

                let (req_buf, id) = self.core.rithmic_sender_api.request_bracket_order(
                    bracket_order,
                    &account,
                    self.login_scope.get(),
                    &trade_route,
                );

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::PlaceAdvancedBracketOrder {
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

                let (req_buf, id) = self.core.rithmic_sender_api.request_advanced_bracket_order(
                    *bracket_order,
                    &account,
                    self.login_scope.get(),
                    &trade_route,
                );

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ModifyOrder {
                order,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_modify_order(
                    &order.id,
                    &order.exchange,
                    &order.symbol,
                    order.qty,
                    order.price,
                    order.price_type,
                    order.trigger_price,
                    &account,
                );

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::CancelOrder {
                order_id,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_cancel_order(&order_id, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ModifyStop {
                basket_id,
                ticks,
                level,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_update_stop_bracket_level(&basket_id, ticks, level, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ModifyProfit {
                basket_id,
                ticks,
                level,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_update_target_bracket_level(&basket_id, ticks, level, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowOrders {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_show_orders(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::CancelAllOrders {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_cancel_all_orders(&account, self.login_scope.get());

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::GetAccountRmsInfo {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_account_rms_info(&account, self.login_scope.get());

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::GetProductRmsInfo {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_product_rms_info(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::GetTradeRoutes {
                subscribe_for_updates,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_trade_routes(subscribe_for_updates);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::RecordTradeRoutes(responses) => {
                let loaded = responses
                    .iter()
                    .filter(|response| self.trade_routes.record_response(response))
                    .count();

                match loaded {
                    0 => error!(
                        "order_plant: no trade routes published, orders will fail with NoTradeRoute"
                    ),
                    loaded => info!("order_plant: {} trade routes loaded", loaded),
                }
            }
            OrderPlantCommand::RecordTradeRouteUpdate(update) => {
                self.trade_routes.record_update(&update);
            }
            OrderPlantCommand::TradeRouteFor {
                exchange,
                response_sender,
            } => {
                let _ = response_sender.send(self.trade_routes.resolve(None, &exchange));
            }
            OrderPlantCommand::ShowOrderHistoryDates { response_sender } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_show_order_history_dates();

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistorySummary {
                date,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_show_order_history_summary(&date, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistoryDetail {
                basket_id,
                date,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_show_order_history_detail(&basket_id, &date, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowOrderHistory {
                basket_id,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_show_order_history(basket_id.as_deref(), &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
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

                let (req_buf, id) =
                    self.core
                        .rithmic_sender_api
                        .request_order(&order, &account, &trade_route);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::PlaceOcoOrderMulti {
                legs,
                account,
                response_sender,
            } => {
                let legs = match self.trade_routes.resolve_legs(legs) {
                    Ok(legs) => legs,
                    Err(err) => {
                        let _ = response_sender.send(Err(err));
                        return;
                    }
                };

                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_oco_order_multi(legs, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowBrackets {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_show_brackets(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowBracketStops {
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_show_bracket_stops(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ExitPosition {
                symbol,
                exchange,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_exit_position(&symbol, &exchange, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::LinkOrders {
                basket_ids,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_link_orders(basket_ids, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::GetEasyToBorrowList {
                request_type,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_easy_to_borrow_list(request_type);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ModifyOrderReferenceData {
                basket_id,
                user_tag,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_modify_order_reference_data(&basket_id, &user_tag, &account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::GetOrderSessionConfig {
                should_defer_request,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_order_session_config(should_defer_request);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ReplayExecutions {
                start_index_sec,
                finish_index_sec,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_replay_executions(
                    start_index_sec,
                    finish_index_sec,
                    &account,
                );

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::SubscribeAccountRmsUpdates {
                subscribe,
                update_bits,
                account,
                response_sender,
            } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_account_rms_updates(
                    subscribe,
                    update_bits,
                    &account,
                );

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::GetLoginInfo { response_sender } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_login_info();

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ListUnacceptedAgreements { response_sender } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_list_unaccepted_agreements();

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ListAcceptedAgreements { response_sender } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_list_accepted_agreements();

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::AcceptAgreement {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_accept_agreement(&agreement_id, market_data_usage_capacity.as_deref());

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ShowAgreement {
                agreement_id,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_show_agreement(&agreement_id);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_set_rithmic_mrkt_data_self_cert_status(
                        &agreement_id,
                        &market_data_usage_capacity,
                    );

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::ListExchangePermissions {
                user,
                response_sender,
            } => {
                let (req_buf, id) = self.core.rithmic_sender_api.request_list_exchanges(&user);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(req_buf.into()), &id)
                    .await;
            }
            OrderPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
    }
}

/// Handle for sending commands to a [`RithmicOrderPlant`] and receiving order updates.
///
/// Obtained from [`RithmicOrderPlant::connect()`]. Use the methods on this handle to
/// log in, place/modify/cancel orders, and query account information. Real-time order
/// updates arrive on [`subscription_receiver`](Self::subscription_receiver).
pub struct RithmicOrderPlantHandle {
    account: Arc<RithmicAccount>,
    /// Set by the first successful login on any handle from this plant.
    login_scope: Arc<OnceLock<LoginScope>>,
    sender: mpsc::Sender<OrderPlantCommand>,
    /// Receiver for real-time order updates and responses.
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
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
    pub async fn list_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Log in to the Rithmic Order plant
    ///
    /// This must be called before sending orders or subscriptions.
    ///
    /// Also loads the trade routes orders are sent on. If that fails the login still
    /// succeeds, and orders fail with [`RithmicError::NoTradeRoute`].
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic Order plant with custom configuration
    ///
    /// This must be called before sending orders or subscriptions.
    ///
    /// Loads trade routes on success, like [`login`](Self::login).
    ///
    /// # Arguments
    /// * `config` - Login configuration options. See [`LoginConfig`] for details.
    ///
    /// # Returns
    /// The login response or an error message
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
        let response = rx
            .await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;

        if let Some(err) = response.error.clone() {
            error!("order_plant: login failed {:?}", err);

            return Err(err);
        }

        let _ = self.sender.send(OrderPlantCommand::SetLogin).await;

        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(hb) = resp.heartbeat_interval {
                let secs = hb as u64;
                self.update_heartbeat(secs).await;
            }

            if let Some(session_id) = &resp.unique_user_id {
                info!("order_plant: session id: {}", session_id);
            }
        }

        // Non-fatal: the login already succeeded, so failing to get the scope just
        // leaves later requests unscoped rather than failing the connection.
        match self.get_login_info().await {
            Ok(response) => {
                if let Some(err) = &response.error {
                    warn!(
                        "order_plant: login info rejected, account list will be unscoped: {:?}",
                        err
                    );
                }
            }
            Err(err) => warn!(
                "order_plant: login info unavailable, account list will be unscoped: {:?}",
                err
            ),
        }

        self.prime_trade_routes().await;

        info!("order_plant: logged in");

        Ok(response)
    }

    /// Load the routes orders are sent on, once, and hand them to the plant.
    ///
    /// This is the snapshot orders route from for the life of the connection.
    /// It subscribes, so updates reach the subscription channel, but only
    /// [`record_trade_route`](Self::record_trade_route) applies one.
    ///
    /// A failure here is only logged: you get [`RithmicError::NoTradeRoute`]
    /// when placing an order, rather than a bad route.
    async fn prime_trade_routes(&self) {
        match self.get_trade_routes(true).await {
            Ok(responses) => {
                for rejection in responses.iter().filter_map(|resp| resp.error.as_ref()) {
                    error!(
                        "order_plant: trade route request rejected, orders will fail: {}",
                        rejection
                    );
                }

                // Queued on the same channel orders are, so an order placed the
                // moment `connect` returns is still handled after this.
                let _ = self
                    .sender
                    .send(OrderPlantCommand::RecordTradeRoutes(responses))
                    .await;
            }
            Err(err) => error!(
                "order_plant: trade routes unavailable, orders will fail: {}",
                err
            ),
        }
    }

    /// Disconnect from the Rithmic Order plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        // Held rather than propagated here so that `Close` is queued either way:
        // `handle_logout` has already set `close_requested`, so an actor that
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
    /// Use when the connection is known to be dead and `disconnect()` would hang.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(OrderPlantCommand::Abort);
    }

    /// Get a list of available trading accounts
    ///
    /// Returns the accounts the login covers. Unscoped, and so possibly wider, if
    /// [`Self::login`] could not retrieve the login info.
    ///
    /// # Returns
    /// A vector of account list responses or an error message
    pub async fn get_account_list(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        // Warn here too, not just at login: this is where the wider list comes back.
        if self.login_scope.get().is_none() {
            warn!("order_plant: no login info retained, listing accounts unscoped");
        }

        let command = OrderPlantCommand::AccountList {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Subscribe to order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeOrderUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Subscribe to bracket order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_bracket_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeBracketUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Place a bracket order (entry order with profit target and stop loss)
    ///
    /// # Arguments
    /// * `bracket_order` - The bracket order parameters
    ///
    /// # Returns
    /// The order placement responses or an error message
    pub async fn place_bracket_order(
        &self,
        bracket_order: RithmicBracketOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceBracketOrder {
            bracket_order,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place an advanced bracket order using the full raw `RequestBracketOrder`
    /// request surface currently modeled by this crate.
    ///
    /// # Arguments
    /// * `bracket_order` - The advanced bracket order parameters
    ///
    /// # Returns
    /// The order placement responses or an error message
    pub async fn place_advanced_bracket_order(
        &self,
        bracket_order: RithmicAdvancedBracketOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceAdvancedBracketOrder {
            bracket_order: Box::new(bracket_order),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Modify an existing order
    ///
    /// # Arguments
    /// * `order` - The order parameters to modify
    ///
    /// # Returns
    /// A vector of order modification responses or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Cancel an order
    ///
    /// Resolves when the final frame of the response sequence arrives. That
    /// result describes the request, not the order. Order state arrives
    /// separately as [`RithmicOrderNotification`] updates on the subscription
    /// stream; those carry an empty `request_id` and are broadcast to
    /// subscribers, so they never resolve this call.
    ///
    /// [`RithmicOrderNotification`]: crate::rti::messages::RithmicMessage::RithmicOrderNotification
    ///
    /// # Arguments
    /// * `order` - The cancel order parameters
    ///
    /// # Returns
    /// A vector of cancellation responses or an error message
    pub async fn cancel_order(
        &self,
        order: RithmicCancelOrder,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::CancelOrder {
            order_id: order.id,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Adjust the profit target level of a bracket order
    ///
    /// # Arguments
    /// * `adjustment` - The bracket, the new tick distance, and the leg to adjust
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_profit(
        &self,
        adjustment: RithmicBracketLevelAdjustment,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyProfit {
            basket_id: adjustment.id,
            ticks: adjustment.ticks,
            level: adjustment.level,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Adjust the stop loss level of a bracket order
    ///
    /// # Arguments
    /// * `adjustment` - The bracket, the new tick distance, and the leg to adjust
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_stop(
        &self,
        adjustment: RithmicBracketLevelAdjustment,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyStop {
            basket_id: adjustment.id,
            ticks: adjustment.ticks,
            level: adjustment.level,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Request a list of all open orders
    ///
    /// The response is a `ResponseShowOrders`, which has no field for the
    /// orders themselves.
    ///
    /// # Returns
    /// The order list response or an error message
    pub async fn show_orders(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrders {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = OrderPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Cancel all open orders
    ///
    /// # Returns
    /// The cancellation response or an error message
    pub async fn cancel_all_orders(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::CancelAllOrders {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get account RMS (Risk Management System) information
    ///
    /// Template 304 names no account, so like [`get_account_list`](Self::get_account_list)
    /// this covers every account the login reaches, not just this handle's.
    ///
    /// # Returns
    /// A vector of RMS info responses or an error message
    pub async fn get_account_rms_info(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetAccountRmsInfo {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get product RMS (Risk Management System) information
    ///
    /// # Returns
    /// A vector of product RMS info responses or an error message
    pub async fn get_product_rms_info(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetProductRmsInfo {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get available trade routes
    ///
    /// # Arguments
    /// * `subscribe_for_updates` - Whether to receive updates when routes change
    ///
    /// # Returns
    /// The list of trade routes or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Apply a `TradeRoute` update to the routes orders go out on.
    ///
    /// [`login`](Self::login) subscribes, so updates arrive on
    /// [`subscription_receiver`](Self::subscription_receiver); applying them is up
    /// to you.
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
    /// Fails with [`RithmicError::NoTradeRoute`] where an order would, so an `Ok`
    /// here means an order that names no route of its own takes this one.
    ///
    /// # Arguments
    /// * `exchange` - The exchange to look up, as it appears on your orders
    ///
    /// # Returns
    /// The route name, or an error naming what is cached instead
    pub async fn trade_route_for(&self, exchange: &str) -> Result<String, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<String, RithmicError>>();

        let command = OrderPlantCommand::TradeRouteFor {
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get dates for which order history is available
    ///
    /// # Returns
    /// The list of available dates or an error message
    pub async fn show_order_history_dates(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowOrderHistoryDates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get order history summary for a specific date
    ///
    /// # Arguments
    /// * `date` - Date in YYYYMMDD format (e.g., "20250122")
    ///
    /// # Returns
    /// The list of order summaries or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Get detailed order history for a specific order
    ///
    /// # Arguments
    /// * `basket_id` - Order/basket identifier
    /// * `date` - Date in YYYYMMDD format
    ///
    /// # Returns
    /// The detailed order history response or an error message
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get general order history
    ///
    /// # Arguments
    /// * `basket_id` - Optional order/basket identifier filter
    ///
    /// # Returns
    /// The list of order history entries or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place a new order using [`RithmicOrder`]
    ///
    /// This is the preferred method for placing standalone orders. It supports
    /// all order types including those with trigger prices (stop orders) and
    /// trailing stops.
    ///
    /// For orders with automatic profit targets and stop losses, use
    /// [`place_bracket_order`](Self::place_bracket_order) instead.
    ///
    /// # Arguments
    /// * `order` - The order parameters
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rithmic_rs::{RithmicOrder, NewOrderTransactionType, NewOrderPriceType};
    ///
    /// let order = RithmicOrder {
    ///     symbol: "ESH6".to_string(),
    ///     exchange: "CME".to_string(),
    ///     quantity: 1,
    ///     price: 5000.0,
    ///     transaction_type: NewOrderTransactionType::Buy,
    ///     price_type: NewOrderPriceType::Limit,
    ///     user_tag: "my-order".to_string(),
    ///     ..Default::default()
    /// };
    /// handle.place_order(order).await?;
    /// ```
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place an OCO (One Cancels Other) order pair
    ///
    /// This wrapper submits exactly two legs.
    ///
    /// # Arguments
    /// * `order1` - First order leg
    /// * `order2` - Second order leg
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    pub async fn place_oco_order(
        &self,
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceOcoOrderMulti {
            legs: vec![order1, order2],
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Place a multi-leg OCO (One Cancels Other) order
    ///
    /// When one leg is filled, the others are automatically cancelled. Requires
    /// at least two legs.
    ///
    /// # Arguments
    /// * `legs` - The order legs (at least two)
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    ///
    /// # Errors
    /// Returns [`RithmicError::InvalidArgument`] if fewer than two legs are supplied.
    pub async fn place_oco_order_multi(
        &self,
        legs: Vec<RithmicOcoOrderLeg>,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        if legs.len() < 2 {
            return Err(RithmicError::InvalidArgument(
                "OCO order requires at least 2 legs".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::PlaceOcoOrderMulti {
            legs,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Show all active bracket orders
    ///
    /// # Returns
    /// A vector of responses containing bracket order information or an error message
    pub async fn show_brackets(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowBrackets {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Show all active bracket stop orders
    ///
    /// # Returns
    /// A vector of responses containing bracket stop information or an error message
    pub async fn show_bracket_stops(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ShowBracketStops {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Exit an entire position for a given symbol
    ///
    /// This closes all open positions for the specified symbol/exchange.
    ///
    /// Resolves when the final frame of the response sequence arrives. That
    /// result describes the request, not the resulting orders.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A vector of exit position responses or an error message
    pub async fn exit_position(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ExitPosition {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Link multiple orders together
    ///
    /// # Arguments
    /// * `basket_ids` - Vector of basket IDs to link together
    ///
    /// # Returns
    /// The link orders response or an error message
    pub async fn link_orders(
        &self,
        basket_ids: Vec<String>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::LinkOrders {
            basket_ids,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get the easy-to-borrow list for short selling
    ///
    /// # Arguments
    /// * `request_type` - Subscribe or Unsubscribe from updates
    ///
    /// # Returns
    /// A vector of responses containing easy-to-borrow securities or an error message
    pub async fn get_easy_to_borrow_list(
        &self,
        request_type: request_easy_to_borrow_list::Request,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetEasyToBorrowList {
            request_type,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Modify order reference data (user tag)
    ///
    /// # Arguments
    /// * `basket_id` - The order/basket identifier
    /// * `user_tag` - New user tag to set on the order
    ///
    /// # Returns
    /// The modification response or an error message
    pub async fn modify_order_reference_data(
        &self,
        basket_id: &str,
        user_tag: &str,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ModifyOrderReferenceData {
            basket_id: basket_id.to_string(),
            user_tag: user_tag.to_string(),
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get or set order session configuration
    ///
    /// # Arguments
    /// * `should_defer_request` - If true, defers requests until server loads reference data
    ///
    /// # Returns
    /// The session config response or an error message
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Replay historical executions
    ///
    /// # Arguments
    /// * `start_index_sec` - Start time in unix seconds
    /// * `finish_index_sec` - End time in unix seconds
    ///
    /// # Returns
    /// A vector of execution responses or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Subscribe to account RMS updates
    ///
    /// # Arguments
    /// * `subscribe` - true to subscribe, false to unsubscribe
    /// * `update_bits` - which RMS fields to stream. Passing
    ///   `vec![RmsUpdateBits::AutoLiqThresholdCurrentValue]` streams
    ///   `auto_liq_threshold_current_value`; an empty `Vec` leaves the field
    ///   off the request.
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_account_rms_updates(
        &self,
        subscribe: bool,
        update_bits: Vec<request_account_rms_updates::UpdateBits>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::SubscribeAccountRmsUpdates {
            subscribe,
            update_bits,
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Get login information for the current session
    ///
    /// [`Self::login`] already calls this once and the first success is what scopes
    /// later requests, so calling it again returns the response but changes nothing.
    ///
    /// # Returns
    /// The login info response or an error message
    pub async fn get_login_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::GetLoginInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        let response = rx
            .await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;

        // A rejected response has no usable identity in it. (A `match` rather than a
        // let-chain: those need Rust 1.88 and the MSRV is 1.85.)
        let scope = match &response.message {
            RithmicMessage::ResponseLoginInfo(info) if response.error.is_none() => {
                LoginScope::from_login_info(info)
            }
            _ => None,
        };

        if let Some(scope) = scope {
            let _ = self.login_scope.set(scope);
        }

        Ok(response)
    }

    /// List unaccepted agreements
    ///
    /// Returns a list of market data agreements that have not yet been accepted.
    ///
    /// # Returns
    /// A vector of unaccepted agreement responses or an error message
    pub async fn list_unaccepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListUnacceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// List accepted agreements
    ///
    /// Returns a list of market data agreements that have been accepted.
    ///
    /// # Returns
    /// A vector of accepted agreement responses or an error message
    pub async fn list_accepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = OrderPlantCommand::ListAcceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Accept a market data agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to accept
    /// * `market_data_usage_capacity` - Optional capacity indicator (e.g., "Professional", "Non-Professional")
    ///
    /// # Returns
    /// The acceptance response or an error message
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Show details of an agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to display
    ///
    /// # Returns
    /// A vector of agreement details responses or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }

    /// Set Rithmic market data self-certification status
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement
    /// * `market_data_usage_capacity` - The usage capacity (e.g., "Professional", "Non-Professional")
    ///
    /// # Returns
    /// The response or an error message
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// List exchange permissions for a user
    ///
    /// Returns the exchanges the user has permission to trade on, along with
    /// their entitlement status for each exchange.
    ///
    /// # Arguments
    /// * `user` - The username to query exchange permissions for
    ///
    /// # Returns
    /// A vector of responses containing exchange permission information or an error message
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

        rx.await.map_err(|_| RithmicError::ConnectionClosed)?
    }
}

#[cfg(test)]
mod tests;
