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
/// When one leg fills, the other is automatically canceled.
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
}

/// Entry order with linked profit target and stop loss orders.
///
/// When the entry fills, the system creates the profit target and stop loss
/// orders automatically.
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

/// Configuration for trailing stop orders.
///
/// When a trailing stop is configured, the stop price follows the market
/// by the specified number of ticks.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::TrailingStop;
///
/// let trailing = TrailingStop { trail_by_ticks: 20 };
/// ```
#[derive(Debug, Clone, Default)]
pub struct TrailingStop {
    /// Number of ticks to trail behind the market price
    pub trail_by_ticks: i32,
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
///     trailing_stop: Some(TrailingStop { trail_by_ticks: 20 }),
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
        }
    }
}
