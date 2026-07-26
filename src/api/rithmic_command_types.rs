use crate::rti::{
    request_bracket_order, request_modify_order, request_new_order, request_oco_order,
};

/// Optional configuration for plant login requests.
///
/// All fields default to `None`, meaning the library's defaults are used.
/// Use [`Default::default()`] for standard login behavior.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::LoginConfig;
///
/// // Tick-by-tick quotes (default)
/// handle.login().await?;
///
/// // Aggregated quotes
/// handle.login_with_config(LoginConfig {
///     aggregated_quotes: Some(true),
///     ..Default::default()
/// }).await?;
/// ```
#[derive(Debug, Clone, Default)]
#[allow(missing_docs)]
pub struct LoginConfig {
    /// Only applicable to the ticker plant.
    pub aggregated_quotes: Option<bool>,
    pub mac_addr: Option<Vec<String>>,
    pub os_version: Option<String>,
    pub os_platform: Option<String>,
}

/// One leg of an OCO (One-Cancels-Other) order pair.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{RithmicOcoOrderLeg, OcoTransactionType, OcoDuration, OcoPriceType};
///
/// let take_profit = RithmicOcoOrderLeg {
///     symbol: "ESH6".to_string(),
///     exchange: "CME".to_string(),
///     quantity: 1,
///     price: 5020.0,
///     trigger_price: None,
///     transaction_type: OcoTransactionType::Sell,
///     duration: OcoDuration::Day,
///     price_type: OcoPriceType::Limit,
///     user_tag: "take-profit".to_string(),
///     trailing_stop: None,
///     trade_route: None,
/// };
///
/// let stop_loss = RithmicOcoOrderLeg {
///     symbol: "ESH6".to_string(),
///     exchange: "CME".to_string(),
///     quantity: 1,
///     price: 4980.0,
///     trigger_price: Some(4980.0),
///     transaction_type: OcoTransactionType::Sell,
///     duration: OcoDuration::Day,
///     price_type: OcoPriceType::StopMarket,
///     user_tag: "stop-loss".to_string(),
///     trailing_stop: None,
///     trade_route: None,
/// };
///
/// handle.place_oco_order(take_profit, stop_loss).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicOcoOrderLeg {
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Order price
    pub price: f64,
    /// Trigger price for stop orders (None for limit/market)
    pub trigger_price: Option<f64>,
    /// Buy or Sell
    pub transaction_type: request_oco_order::TransactionType,
    /// Order duration
    pub duration: request_oco_order::Duration,
    /// Order type
    pub price_type: request_oco_order::PriceType,
    /// Your identifier for this order
    pub user_tag: String,
    /// Optional trailing stop configuration for this leg
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for this
    /// leg's exchange.
    pub trade_route: Option<String>,
}

/// Entry order with linked profit target and stop loss orders.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{RithmicBracketOrder, BracketTransactionType, BracketDuration, BracketPriceType};
///
/// let order = RithmicBracketOrder {
///     symbol: "ESH6".to_string(),
///     exchange: "CME".to_string(),
///     action: BracketTransactionType::Buy,
///     quantity: 1,
///     price_type: BracketPriceType::Limit,
///     price: Some(5000.0),
///     duration: BracketDuration::Day,
///     profit_ticks: 20,  // 20 ticks above entry
///     stop_ticks: 10,    // 10 ticks below entry
///     localid: "my-order-1".to_string(),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RithmicBracketOrder {
    /// Buy or Sell
    pub action: request_bracket_order::TransactionType,
    /// Order duration
    pub duration: request_bracket_order::Duration,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Your identifier for tracking this order
    pub localid: String,
    /// Order type
    pub price_type: request_bracket_order::PriceType,
    /// Limit price (required for Limit orders)
    pub price: Option<f64>,
    /// Profit target distance in ticks from entry
    pub profit_ticks: i32,
    /// Number of contracts
    pub quantity: i32,
    /// Stop loss distance in ticks from entry
    pub stop_ticks: i32,
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
}

/// Conditional trigger for advanced bracket order entry.
///
/// This maps directly to the `if_touched_*` fields on `RequestBracketOrder`.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{BracketCondition, BracketPriceField, RithmicIfTouchedTrigger};
///
/// let trigger = RithmicIfTouchedTrigger {
///     symbol: "NQM6".to_string(),
///     exchange: "CME".to_string(),
///     condition: BracketCondition::GreaterThanEqualTo,
///     price_field: BracketPriceField::TradePrice,
///     price: 18250.5,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RithmicIfTouchedTrigger {
    /// Trading symbol to monitor for the condition.
    pub symbol: String,
    /// Exchange for the monitored symbol.
    pub exchange: String,
    /// Comparison operator for the trigger.
    pub condition: request_bracket_order::Condition,
    /// Price field to evaluate.
    pub price_field: request_bracket_order::PriceField,
    /// Threshold price for the condition.
    pub price: f64,
}

/// Richer bracket order request that maps directly to `RequestBracketOrder`.
///
/// This type exposes the full raw venue-native request surface currently
/// available through the protobuf schema, including triggered entry, break-even,
/// trailing-stop, timed release/cancel fields, and if-touched entry conditions.
///
/// Callers are responsible for providing a coherent combination of
/// `bracket_type`, `target_*`, and `stop_*` fields for the shape they want
/// Rithmic to create.
///
/// # Example
///
/// This struct is `#[non_exhaustive]`, so downstream crates cannot build it with
/// a struct expression — including functional-update (`..Default::default()`)
/// syntax. Start from [`Default`] and assign fields:
///
/// ```ignore
/// use rithmic_rs::{
///     BracketCondition, BracketDuration, BracketPriceField, BracketPriceType,
///     BracketTransactionType, BracketType, RithmicAdvancedBracketOrder,
///     RithmicIfTouchedTrigger,
/// };
///
/// let mut order = RithmicAdvancedBracketOrder::default();
///
/// order.action = BracketTransactionType::Buy;
/// order.duration = BracketDuration::Gtc;
/// order.exchange = "CME".to_string();
/// order.localid = "advanced-bracket-1".to_string();
/// order.price_type = BracketPriceType::StopLimit;
/// order.price = Some(5000.25);
/// order.trigger_price = Some(4999.75);
/// order.quantity = 3;
/// order.symbol = "ESM6".to_string();
///
/// order.bracket_type = BracketType::TargetAndStop;
/// order.target_quantity = vec![2, 1];
/// order.target_ticks = vec![16, 24];
/// order.stop_quantity = vec![3];
/// order.stop_ticks = vec![8];
///
/// order.if_touched = Some(RithmicIfTouchedTrigger {
///     symbol: "NQM6".to_string(),
///     exchange: "CME".to_string(),
///     condition: BracketCondition::GreaterThanEqualTo,
///     price_field: BracketPriceField::TradePrice,
///     price: 18250.5,
/// });
///
/// order.break_even_ticks = Some(2);
/// order.break_even_trigger_ticks = Some(10);
/// order.trailing_stop_trigger_ticks = Some(12);
/// order.target_market_order_if_touched = Some(true);
/// order.stop_market_on_reject = Some(true);
/// order.release_at_ssboe = Some(35900);
/// order.cancel_after_secs = Some(120);
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RithmicAdvancedBracketOrder {
    /// Buy or Sell.
    pub action: request_bracket_order::TransactionType,
    /// Order duration.
    pub duration: request_bracket_order::Duration,
    /// Exchange code (e.g., "CME").
    pub exchange: String,
    /// Your identifier for tracking this order.
    pub localid: String,
    /// Order type.
    pub price_type: request_bracket_order::PriceType,
    /// Entry price when required by the price type.
    pub price: Option<f64>,
    /// Trigger price for stop and if-touched entry types.
    pub trigger_price: Option<f64>,
    /// Entry order size (number of contracts).
    ///
    /// For a coherent bracket, this should equal the sum of
    /// `target_quantity` across all target legs. The crate does not validate
    /// this invariant.
    pub quantity: i32,
    /// Trading symbol (e.g., "ESH6").
    pub symbol: String,
    /// Rithmic bracket shape.
    pub bracket_type: request_bracket_order::BracketType,
    /// Exit target quantities, one value per target leg.
    pub target_quantity: Vec<i32>,
    /// Exit target distances in ticks.
    pub target_ticks: Vec<i32>,
    /// Exit stop quantities.
    pub stop_quantity: Vec<i32>,
    /// Exit stop distances in ticks.
    pub stop_ticks: Vec<i32>,
    /// Optional if-touched trigger settings.
    pub if_touched: Option<RithmicIfTouchedTrigger>,
    /// Move stop to break-even by this many ticks.
    pub break_even_ticks: Option<i32>,
    /// Trigger break-even once the position reaches this many ticks.
    pub break_even_trigger_ticks: Option<i32>,
    /// Enable a trailing stop after this many ticks.
    pub trailing_stop_trigger_ticks: Option<i32>,
    /// Use last trade instead of bid/offer for trailing stop tracking.
    pub trailing_stop_by_last_trade_price: Option<bool>,
    /// Convert target to MIT once touched.
    pub target_market_order_if_touched: Option<bool>,
    /// Convert stop to market if the current stop order is rejected.
    pub stop_market_on_reject: Option<bool>,
    /// Convert target to market at this second-since-beginning-of-epoch value.
    pub target_market_at_ssboe: Option<i32>,
    /// Microsecond component for `target_market_at_ssboe`.
    pub target_market_at_usecs: Option<i32>,
    /// Convert stop to market at this second-since-beginning-of-epoch value.
    pub stop_market_at_ssboe: Option<i32>,
    /// Microsecond component for `stop_market_at_ssboe`.
    pub stop_market_at_usecs: Option<i32>,
    /// Convert target to market after this many seconds.
    pub target_market_order_after_secs: Option<i32>,
    /// Release order at this second-since-beginning-of-epoch value.
    pub release_at_ssboe: Option<i32>,
    /// Microsecond component for `release_at_ssboe`.
    pub release_at_usecs: Option<i32>,
    /// Cancel order at this second-since-beginning-of-epoch value.
    pub cancel_at_ssboe: Option<i32>,
    /// Microsecond component for `cancel_at_ssboe`.
    pub cancel_at_usecs: Option<i32>,
    /// Cancel order after this many seconds.
    pub cancel_after_secs: Option<i32>,
    /// Route to send on. `None` uses the route the server published for `exchange`.
    pub trade_route: Option<String>,
}

impl Default for RithmicAdvancedBracketOrder {
    fn default() -> Self {
        Self {
            action: request_bracket_order::TransactionType::Buy,
            duration: request_bracket_order::Duration::Day,
            exchange: String::new(),
            localid: String::new(),
            price_type: request_bracket_order::PriceType::Limit,
            price: None,
            trigger_price: None,
            quantity: 0,
            symbol: String::new(),
            bracket_type: request_bracket_order::BracketType::TargetAndStopStatic,
            target_quantity: Vec::new(),
            target_ticks: Vec::new(),
            stop_quantity: Vec::new(),
            stop_ticks: Vec::new(),
            if_touched: None,
            break_even_ticks: None,
            break_even_trigger_ticks: None,
            trailing_stop_trigger_ticks: None,
            trailing_stop_by_last_trade_price: None,
            target_market_order_if_touched: None,
            stop_market_on_reject: None,
            target_market_at_ssboe: None,
            target_market_at_usecs: None,
            stop_market_at_ssboe: None,
            stop_market_at_usecs: None,
            target_market_order_after_secs: None,
            release_at_ssboe: None,
            release_at_usecs: None,
            cancel_at_ssboe: None,
            cancel_at_usecs: None,
            cancel_after_secs: None,
            trade_route: None,
        }
    }
}

impl From<RithmicBracketOrder> for RithmicAdvancedBracketOrder {
    fn from(value: RithmicBracketOrder) -> Self {
        Self {
            action: value.action,
            duration: value.duration,
            exchange: value.exchange,
            localid: value.localid,
            price_type: value.price_type,
            price: value.price,
            trigger_price: None,
            quantity: value.quantity,
            symbol: value.symbol,
            bracket_type: request_bracket_order::BracketType::TargetAndStopStatic,
            target_quantity: vec![value.quantity],
            target_ticks: vec![value.profit_ticks],
            stop_quantity: vec![value.quantity],
            stop_ticks: vec![value.stop_ticks],
            if_touched: None,
            break_even_ticks: None,
            break_even_trigger_ticks: None,
            trailing_stop_trigger_ticks: None,
            trailing_stop_by_last_trade_price: None,
            target_market_order_if_touched: None,
            stop_market_on_reject: None,
            target_market_at_ssboe: None,
            target_market_at_usecs: None,
            stop_market_at_ssboe: None,
            stop_market_at_usecs: None,
            target_market_order_after_secs: None,
            release_at_ssboe: None,
            release_at_usecs: None,
            cancel_at_ssboe: None,
            cancel_at_usecs: None,
            cancel_after_secs: None,
            trade_route: None,
        }
    }
}

/// Modify an existing order's price, quantity, or type.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{RithmicModifyOrder, ModifyPriceType};
///
/// let modification = RithmicModifyOrder {
///     id: "123456".to_string(),  // basket_id from order notification
///     symbol: "ESH6".to_string(),
///     exchange: "CME".to_string(),
///     qty: 2,
///     price: 5005.0,
///     price_type: ModifyPriceType::Limit,
///     trigger_price: None,
/// };
/// handle.modify_order(modification).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicModifyOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// Exchange code
    pub exchange: String,
    /// Trading symbol
    pub symbol: String,
    /// New quantity
    pub qty: i32,
    /// New price
    pub price: f64,
    /// Order type
    pub price_type: request_modify_order::PriceType,
    /// Separate trigger price for StopLimit/StopMarket modifies. When `None`, the trigger defaults to `price` for stop order types.
    pub trigger_price: Option<f64>,
}

/// Cancel an existing order.
///
/// # Example
///
/// ```ignore
/// let cancel = RithmicCancelOrder {
///     id: "123456".to_string(),  // basket_id from order notification
/// };
/// handle.cancel_order(cancel).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicCancelOrder {
    /// The `basket_id` from the order notification
    pub id: String,
}

/// Adjust one leg of a bracket's profit target or stop loss.
///
/// The same shape serves `adjust_profit` and `adjust_stop`.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::RithmicBracketLevelAdjustment;
///
/// let adjustment = RithmicBracketLevelAdjustment {
///     id: "123456".to_string(),  // basket_id from order notification
///     ticks: 16,
///     level: Some(2),
/// };
/// handle.adjust_profit(adjustment).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicBracketLevelAdjustment {
    /// The `basket_id` from the order notification
    pub id: String,
    /// The new distance in ticks
    pub ticks: i32,
    /// Which bracket leg to adjust, in the order the legs were placed (see
    /// [`RithmicAdvancedBracketOrder`]). Sent verbatim; the crate defines no
    /// numbering. `None` omits the field.
    pub level: Option<i32>,
}

/// Configuration for trailing stop orders.
///
/// Used both by [`RithmicOrder::trailing_stop`] for a standalone order and by
/// [`RithmicOcoOrderLeg::trailing_stop`] for a single leg of an OCO group.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::TrailingStop;
///
/// let trailing = TrailingStop { trail_by_ticks: 20, trail_by_price_id: 1 };
/// ```
#[derive(Debug, Clone, Default)]
pub struct TrailingStop {
    /// Number of ticks to trail behind the market price
    pub trail_by_ticks: i32,
    /// Rithmic price-id to trail against. Rithmic rejects a trailing stop with
    /// rp_code 1112 when this is unset.
    pub trail_by_price_id: i32,
}

/// A standalone order (not a bracket order).
///
/// Use this struct with `RithmicOrderPlantHandle::place_order()` to submit
/// orders with advanced features like trigger prices and trailing stops.
///
/// For orders with automatic profit targets and stop losses, use
/// [`RithmicBracketOrder`] instead.
///
/// # Example: Simple Limit Order
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
///     user_tag: "my-order-1".to_string(),
///     ..Default::default()
/// };
/// handle.place_order(order).await?;
/// ```
///
/// # Example: Stop-Limit Order with Trigger Price
///
/// ```ignore
/// use rithmic_rs::{RithmicOrder, NewOrderTransactionType, NewOrderPriceType};
///
/// let order = RithmicOrder {
///     symbol: "ESH6".to_string(),
///     exchange: "CME".to_string(),
///     quantity: 1,
///     price: 4980.0,                // Limit price after trigger
///     trigger_price: Some(4985.0),  // Stop triggers at this price
///     transaction_type: NewOrderTransactionType::Sell,
///     price_type: NewOrderPriceType::StopLimit,
///     user_tag: "stop-order".to_string(),
///     ..Default::default()
/// };
/// ```
///
/// # Example: Trailing Stop Order
///
/// ```ignore
/// use rithmic_rs::{RithmicOrder, NewOrderTransactionType, NewOrderPriceType, TrailingStop};
///
/// let order = RithmicOrder {
///     symbol: "ESH6".to_string(),
///     exchange: "CME".to_string(),
///     quantity: 1,
///     price: 0.0,  // Not used for trailing stops
///     transaction_type: NewOrderTransactionType::Sell,
///     price_type: NewOrderPriceType::StopMarket,
///     trailing_stop: Some(TrailingStop { trail_by_ticks: 20, trail_by_price_id: 1 }),
///     user_tag: "trailing-stop".to_string(),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RithmicOrder {
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Order price (ignored for market orders)
    pub price: f64,
    /// Buy or Sell
    pub transaction_type: request_new_order::TransactionType,
    /// Order type (Limit, Market, StopLimit, StopMarket, etc.)
    pub price_type: request_new_order::PriceType,
    /// Your identifier for tracking this order
    pub user_tag: String,
    /// Order duration (defaults to Day if None)
    pub duration: Option<request_new_order::Duration>,
    /// Trigger price for stop orders (StopLimit, StopMarket, etc.)
    ///
    /// Required for stop orders; ignored for limit/market orders.
    pub trigger_price: Option<f64>,
    /// Trailing stop configuration
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for `exchange`.
    pub trade_route: Option<String>,
}

impl Default for RithmicOrder {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            exchange: String::new(),
            quantity: 0,
            price: 0.0,
            transaction_type: request_new_order::TransactionType::Buy,
            price_type: request_new_order::PriceType::Limit,
            user_tag: String::new(),
            duration: None,
            trigger_price: None,
            trailing_stop: None,
            trade_route: None,
        }
    }
}
