use crate::error::RithmicError;
use crate::types::{
    BracketType, OrderCondition, OrderPlacement, OrderPriceField, OrderSide, OrderType, TimeInForce,
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
/// let mut config = LoginConfig::default();
/// config.aggregated_quotes = Some(true);
/// handle.login_with_config(config).await?;
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let config = rithmic_rs::LoginConfig { aggregated_quotes: Some(true), ..Default::default() };
/// ```
#[derive(Debug, Clone, Default)]
#[allow(missing_docs)]
#[non_exhaustive]
pub struct LoginConfig {
    /// Only applicable to the ticker plant.
    pub aggregated_quotes: Option<bool>,
    pub mac_addr: Option<Vec<String>>,
    pub os_version: Option<String>,
    pub os_platform: Option<String>,
}

/// One leg of an OCO (One-Cancels-Other) order group.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOcoOrderLeg};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let take_profit = RithmicOcoOrderLeg::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::Limit)
///     .price(5020.0)
///     .user_tag("take-profit")
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let leg = rithmic_rs::RithmicOcoOrderLeg { quantity: 1, ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicOcoOrderLeg {
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Order price for this leg, omitted from the request when every leg in the
    /// group is `None`.
    ///
    /// `price` is index-aligned with the other repeated per-leg fields, so a
    /// group where at least one leg is priced sends `0.0` for the legs that are
    /// not.
    pub price: Option<f64>,
    /// Trigger price for stop orders (None for limit/market)
    ///
    /// Index-aligned like [`Self::price`]: omitted from the request when no leg
    /// in the group has one, and zero-filled for the legs without one when any
    /// leg does.
    pub trigger_price: Option<f64>,
    /// Buy or Sell
    pub transaction_type: OrderSide,
    /// Order duration
    pub duration: TimeInForce,
    /// Order type. `RequestOcoOrder` has no if-touched price types, so
    /// [`OrderType::MarketIfTouched`] and [`OrderType::LimitIfTouched`] are
    /// rejected here.
    pub price_type: OrderType,
    /// Your identifier for this order
    pub user_tag: String,
    /// Optional trailing stop configuration for this leg
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for this
    /// leg's exchange.
    pub trade_route: Option<String>,
    /// How this leg is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
}

impl RithmicOcoOrderLeg {
    /// Check this leg carries the prices its [`Self::price_type`] requires.
    ///
    /// # Errors
    ///
    /// [`RithmicError::InvalidArgument`] naming the missing or unsupported field.
    pub fn validate(&self) -> Result<(), RithmicError> {
        let order_type = self.price_type.as_str_name();

        let (needs_price, needs_trigger) = match self.price_type {
            OrderType::Market => (false, false),
            OrderType::Limit => (true, false),
            OrderType::StopMarket => (false, true),
            OrderType::StopLimit => (true, true),
            OrderType::MarketIfTouched | OrderType::LimitIfTouched => {
                return Err(RithmicError::InvalidArgument(format!(
                    "price_type {order_type} is not available on an OCO leg"
                )));
            }
        };

        if needs_price && self.price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "price is required for a {order_type} order"
            )));
        }

        if needs_trigger && self.trigger_price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "trigger_price is required for a {order_type} order"
            )));
        }

        Ok(())
    }
}

/// A group of OCO legs: when one fills, the others are cancelled.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOcoOrder, RithmicOcoOrderLeg};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let take_profit = RithmicOcoOrderLeg::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::Limit)
///     .price(5020.0)
///     .build()?;
/// let stop_loss = RithmicOcoOrderLeg::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::StopMarket)
///     .trigger_price(4980.0)
///     .build()?;
///
/// let order = RithmicOcoOrder::new().legs([take_profit, stop_loss]).build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let order = rithmic_rs::RithmicOcoOrder { legs: Vec::new(), ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicOcoOrder {
    /// The legs of the group, in the order they are sent.
    pub legs: Vec<RithmicOcoOrderLeg>,
}

impl RithmicOcoOrder {
    /// Check each leg validates.
    ///
    /// # Errors
    ///
    /// [`RithmicError::InvalidArgument`] naming the first offending leg's problem.
    pub fn validate(&self) -> Result<(), RithmicError> {
        for leg in &self.legs {
            leg.validate()?;
        }

        Ok(())
    }
}

/// Entry order with linked profit target and stop loss orders.
///
/// Maps directly to `RequestBracketOrder`, so it carries the full venue-native
/// surface: multiple target and stop legs, triggered entry, break-even, trailing
/// stop management, and timed release/cancel.
///
/// # Example: one target, one stop
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicBracketOrder};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let order = RithmicBracketOrder::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .action(OrderSide::Buy)
///     .price_type(OrderType::Limit)
///     .price(5000.0)
///     .target(20)
///     .stop(10)
///     .localid("my-order-1")
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// # Example: staggered targets
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicBracketOrder};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let order = RithmicBracketOrder::new()
///     .symbol("ESM6")
///     .exchange("CME")
///     .quantity(3)
///     .action(OrderSide::Buy)
///     .price_type(OrderType::StopLimit)
///     .price(5000.25)
///     .trigger_price(4999.75)
///     .targets([(2, 16), (1, 24)])
///     .stops([(3, 8)])
///     .break_even_ticks(2)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream — not even with functional-update syntax:
///
/// ```compile_fail
/// let order = rithmic_rs::RithmicBracketOrder { quantity: 1, ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicBracketOrder {
    /// Buy or Sell.
    pub action: OrderSide,
    /// Order duration.
    pub duration: TimeInForce,
    /// Exchange code (e.g., "CME").
    pub exchange: String,
    /// Your identifier for tracking this order.
    pub localid: String,
    /// Order type.
    pub price_type: OrderType,
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
    /// Rithmic bracket shape. `None` means "derive it from the legs supplied";
    /// the builder resolves it, so a built order always carries `Some`.
    pub bracket_type: Option<BracketType>,
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
    /// How this order is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
}

impl RithmicBracketOrder {
    /// Check the entry leg carries the prices its [`Self::price_type`] requires.
    ///
    /// The exit legs are not checked: Rithmic's own limits on multi-level
    /// brackets are undocumented, so the crate sends what the caller asked for
    /// rather than refusing orders the server might have accepted.
    ///
    /// # Errors
    ///
    /// [`RithmicError::InvalidArgument`] naming the missing price field.
    pub fn validate(&self) -> Result<(), RithmicError> {
        let (needs_price, needs_trigger) = match self.price_type {
            OrderType::Market => (false, false),
            OrderType::Limit => (true, false),
            OrderType::StopMarket | OrderType::MarketIfTouched => (false, true),
            OrderType::StopLimit | OrderType::LimitIfTouched => (true, true),
        };

        let order_type = self.price_type.as_str_name();

        if needs_price && self.price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "price is required for a {order_type} order"
            )));
        }

        if needs_trigger && self.trigger_price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "trigger_price is required for a {order_type} order"
            )));
        }

        Ok(())
    }
}

/// Conditional trigger that releases an order once a price is touched.
///
/// Maps to the `if_touched_*` fields on `RequestNewOrder` and
/// `RequestBracketOrder`, which are field-identical.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderCondition, OrderPriceField, RithmicIfTouchedTrigger};
///
/// let trigger = RithmicIfTouchedTrigger::new(
///     "NQM6",
///     "CME",
///     OrderCondition::GreaterThanEqualTo,
///     OrderPriceField::TradePrice,
///     18250.5,
/// );
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// use rithmic_rs::{OrderCondition, OrderPriceField, RithmicIfTouchedTrigger};
///
/// let trigger = RithmicIfTouchedTrigger {
///     price: 18250.5,
///     ..RithmicIfTouchedTrigger::new(
///         "NQM6", "CME", OrderCondition::EqualTo, OrderPriceField::TradePrice, 1.0,
///     )
/// };
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RithmicIfTouchedTrigger {
    /// Trading symbol to monitor for the condition.
    pub symbol: String,
    /// Exchange for the monitored symbol.
    pub exchange: String,
    /// Comparison operator for the trigger.
    pub condition: OrderCondition,
    /// Price field to evaluate.
    pub price_field: OrderPriceField,
    /// Threshold price for the condition.
    pub price: f64,
}

impl RithmicIfTouchedTrigger {
    /// Build a trigger. Every field is required, so there is no builder.
    pub fn new(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        condition: OrderCondition,
        price_field: OrderPriceField,
        price: f64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            exchange: exchange.into(),
            condition,
            price_field,
            price,
        }
    }
}

/// Modify an existing order's price, quantity, or type.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderType, RithmicModifyOrder};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// // "123456" is the basket_id from the order notification.
/// let modification = RithmicModifyOrder::new()
///     .id("123456")
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(2)
///     .price(5005.0)
///     .price_type(OrderType::Limit)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let modification = rithmic_rs::RithmicModifyOrder { quantity: 2, ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicModifyOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// Exchange code
    pub exchange: String,
    /// Trading symbol
    pub symbol: String,
    /// New quantity
    pub quantity: i32,
    /// New price, always sent.
    ///
    /// Unlike [`RithmicOrder::price`] this is not an `Option`, because a modify
    /// restates the order rather than patching it: template 314 carries symbol,
    /// exchange, quantity, price and price type together and replaces the
    /// resting order with what it describes. Omitting the price would not leave
    /// it alone.
    ///
    /// So when changing only the quantity, set this to the order's *current*
    /// price from its notification. Rithmic's reference Python client does the
    /// same, falling back to the live order's price whenever the caller does
    /// not supply a new one.
    pub price: f64,
    /// Order type
    pub price_type: OrderType,
    /// Separate trigger price for StopLimit/StopMarket modifies. When `None`, the trigger defaults to `price` for stop order types.
    pub trigger_price: Option<f64>,
    /// How this modification is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
}

impl RithmicModifyOrder {
    /// Always succeeds.
    ///
    /// There is no price rule here: [`Self::price`] is always present, and the
    /// sender already falls back to it for a stop modify's trigger.
    ///
    /// # Errors
    ///
    /// Never returns an error; the signature matches the other command types.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Cancel an existing order.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicCancelOrder;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// // "123456" is the basket_id from the order notification.
/// let cancel = RithmicCancelOrder::new().id("123456").build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let cancel = rithmic_rs::RithmicCancelOrder { id: "123456".to_string(), ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicCancelOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// How this cancellation is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
}

impl RithmicCancelOrder {
    /// Always succeeds.
    ///
    /// # Errors
    ///
    /// Never returns an error; the signature matches the other command types.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Cancel every working order on the account.
///
/// [`Self::new`] is the whole command for the common case; set
/// [`Self::manual_or_auto`] to attribute it to a person instead.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderPlacement, RithmicCancelAllOrders};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let auto = RithmicCancelAllOrders::new().build()?;
/// let manual = RithmicCancelAllOrders::new()
///     .manual_or_auto(OrderPlacement::Manual)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let command = rithmic_rs::RithmicCancelAllOrders { ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicCancelAllOrders {
    /// How this cancellation is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
}

impl RithmicCancelAllOrders {
    /// Always `Ok`; the command carries nothing to check.
    ///
    /// # Errors
    ///
    /// Never.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Flatten the position in one instrument.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicExitPosition;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let command = RithmicExitPosition::new()
///     .symbol("ESM6")
///     .exchange("CME")
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let command = rithmic_rs::RithmicExitPosition { symbol: "ESM6".to_string(), ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicExitPosition {
    /// Trading symbol (e.g., "ESM6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// How this exit is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
}

impl RithmicExitPosition {
    /// Always succeeds.
    ///
    /// # Errors
    ///
    /// Never returns an error; the signature matches the other command types.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Adjust one leg of a bracket's profit target or stop loss.
///
/// The same shape serves `adjust_target` and `adjust_stop`.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicBracketLevelAdjustment;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// // "123456" is the basket_id from the order notification.
/// let adjustment = RithmicBracketLevelAdjustment::new()
///     .id("123456")
///     .ticks(16)
///     .level(2)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let adjustment = rithmic_rs::RithmicBracketLevelAdjustment { ticks: 16, ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicBracketLevelAdjustment {
    /// The `basket_id` from the order notification
    pub id: String,
    /// The new distance in ticks
    pub ticks: i32,
    /// Which bracket leg to adjust, in the order the legs were placed (see
    /// [`RithmicBracketOrder`]). Sent verbatim; the crate defines no numbering.
    /// `None` omits the field.
    pub level: Option<i32>,
}

impl RithmicBracketLevelAdjustment {
    /// Always succeeds.
    ///
    /// `ticks` is not checked: the crate defines no numbering and sends what it
    /// is given.
    ///
    /// # Errors
    ///
    /// Never returns an error; the signature matches the other command types.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Link working orders together so the server treats them as one group.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicLinkOrders;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let command = RithmicLinkOrders::new()
///     .basket_ids(["123456", "123457"])
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let command = rithmic_rs::RithmicLinkOrders { basket_ids: Vec::new(), ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicLinkOrders {
    /// The `basket_id`s to link, from the order notifications.
    pub basket_ids: Vec<String>,
}

impl RithmicLinkOrders {
    /// Always succeeds.
    ///
    /// # Errors
    ///
    /// Never returns an error; the signature matches the other command types.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Change the `user_tag` reported on an order's subsequent notifications.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicModifyOrderReferenceData;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let command = RithmicModifyOrderReferenceData::new()
///     .basket_id("123456")
///     .user_tag("new-tag")
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let command = rithmic_rs::RithmicModifyOrderReferenceData {
///     user_tag: "new-tag".to_string(),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicModifyOrderReferenceData {
    /// The `basket_id` from the order notification.
    pub basket_id: String,
    /// The new tag. Empty is how a tag is cleared, so it is sent as given.
    pub user_tag: String,
}

impl RithmicModifyOrderReferenceData {
    /// Always succeeds.
    ///
    /// `user_tag` is not checked: an empty tag is how a tag is cleared.
    ///
    /// # Errors
    ///
    /// Never returns an error; the signature matches the other command types.
    pub fn validate(&self) -> Result<(), RithmicError> {
        Ok(())
    }
}

/// Configuration for trailing stop orders.
///
/// Used both by [`RithmicOrder::trailing_stop`] for a standalone order and by
/// [`RithmicOcoOrderLeg::trailing_stop`] for a single leg of an OCO group.
///
/// # Example
///
/// ```
/// use rithmic_rs::TrailingStop;
///
/// let trailing = TrailingStop::new(20, 1);
/// ```
///
/// There is deliberately no [`Default`]. Both fields are required and neither
/// has a meaningful zero: a `trail_by_price_id` of `0` is the unset value
/// Rithmic rejects with rp_code 1112, so a defaulted `TrailingStop` would be a
/// guaranteed rejection rather than a starting point.
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let trailing = rithmic_rs::TrailingStop { trail_by_ticks: 20, trail_by_price_id: 1 };
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TrailingStop {
    /// Number of ticks to trail behind the market price
    pub trail_by_ticks: i32,
    /// Rithmic price-id to trail against. Rithmic rejects a trailing stop with
    /// rp_code 1112 when this is unset.
    pub trail_by_price_id: i32,
}

impl TrailingStop {
    /// Build a trailing stop from its two required fields.
    pub fn new(trail_by_ticks: i32, trail_by_price_id: i32) -> Self {
        Self {
            trail_by_ticks,
            trail_by_price_id,
        }
    }
}

/// A standalone order (not a bracket order).
///
/// For orders with automatic profit targets and stop losses, use
/// [`RithmicBracketOrder`] instead.
///
/// This struct carries every field [`RequestNewOrder`](crate::rti::RequestNewOrder)
/// accepts, most of which a given order does not use.
///
/// # Example: limit order
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOrder};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let order = RithmicOrder::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Buy)
///     .price_type(OrderType::Limit)
///     .price(5000.0)
///     .user_tag("my-order-1")
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// # Example: market order
///
/// A market order has no price. Leaving `price` unset omits the field rather
/// than pricing the order at zero.
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOrder};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let order = RithmicOrder::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Buy)
///     .price_type(OrderType::Market)
///     .user_tag("market-order")
///     .build()?;
///
/// assert_eq!(order.price, None);
/// # Ok(())
/// # }
/// ```
///
/// # Example: stop-limit with a trailing stop
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOrder};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let order = RithmicOrder::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::StopLimit)
///     .price(4980.0)
///     .trigger_price(4985.0)
///     .trailing_stop_by(20, 1)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// This type is `#[non_exhaustive]`, so a struct expression does not compile
/// downstream:
///
/// ```compile_fail
/// let order = rithmic_rs::RithmicOrder { quantity: 1, ..Default::default() };
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicOrder {
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Order price, omitted from the request when `None`.
    ///
    /// A market order has no price, and `price = 0.0` is a price rather than
    /// the absence of one, so this is an `Option` rather than a value the
    /// builder has to guess the meaning of.
    pub price: Option<f64>,
    /// Buy or Sell
    pub transaction_type: OrderSide,
    /// Order type (Limit, Market, StopLimit, StopMarket, etc.)
    pub price_type: OrderType,
    /// Your identifier for tracking this order
    pub user_tag: String,
    /// Order duration (defaults to Day if None)
    pub duration: Option<TimeInForce>,
    /// Trigger price for stop orders (StopLimit, StopMarket, etc.)
    ///
    /// Required for stop orders; ignored for limit/market orders.
    pub trigger_price: Option<f64>,
    /// Trailing stop configuration
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for `exchange`.
    pub trade_route: Option<String>,
    /// How this order is attributed to its originator.
    pub manual_or_auto: OrderPlacement,
    /// Originating window name reported to Rithmic.
    pub window_name: Option<String>,
    /// Release the order at this second-since-beginning-of-epoch value.
    pub release_at_ssboe: Option<i32>,
    /// Microsecond component for [`Self::release_at_ssboe`].
    pub release_at_usecs: Option<i32>,
    /// Cancel the order at this second-since-beginning-of-epoch value.
    pub cancel_at_ssboe: Option<i32>,
    /// Microsecond component for [`Self::cancel_at_ssboe`].
    pub cancel_at_usecs: Option<i32>,
    /// Cancel the order after this many seconds.
    pub cancel_after_secs: Option<i32>,
    /// Conditional trigger that releases this order once touched.
    pub if_touched: Option<RithmicIfTouchedTrigger>,
}

impl RithmicOrder {
    /// Check this order carries the prices its [`Self::price_type`] requires.
    ///
    /// Not called on the send path — placing an order sends what you give it.
    ///
    /// # Errors
    ///
    /// [`RithmicError::InvalidArgument`] naming the missing field and the order
    /// type that requires it.
    pub fn validate(&self) -> Result<(), RithmicError> {
        let (needs_price, needs_trigger) = match self.price_type {
            OrderType::Market => (false, false),
            OrderType::Limit => (true, false),
            OrderType::StopMarket | OrderType::MarketIfTouched => (false, true),
            OrderType::StopLimit | OrderType::LimitIfTouched => (true, true),
        };

        let order_type = self.price_type.as_str_name();

        if needs_price && self.price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "price is required for a {order_type} order"
            )));
        }

        if needs_trigger && self.trigger_price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "trigger_price is required for a {order_type} order"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every order command type defaults its origination to `Auto`, which is
    /// only true because [`OrderPlacement`] is crate-owned — the generated
    /// `OrderPlacement` enums have no `Default` at all.
    #[test]
    fn order_command_types_default_to_auto_placement() {
        assert_eq!(RithmicOrder::default().manual_or_auto, OrderPlacement::Auto);
        assert_eq!(
            RithmicOcoOrderLeg::default().manual_or_auto,
            OrderPlacement::Auto
        );
        assert_eq!(
            RithmicBracketOrder::default().manual_or_auto,
            OrderPlacement::Auto
        );
        assert_eq!(
            RithmicModifyOrder::default().manual_or_auto,
            OrderPlacement::Auto
        );
        assert_eq!(
            RithmicCancelOrder::default().manual_or_auto,
            OrderPlacement::Auto
        );
        assert_eq!(
            RithmicCancelAllOrders::default().manual_or_auto,
            OrderPlacement::Auto
        );
        assert_eq!(
            RithmicExitPosition::default().manual_or_auto,
            OrderPlacement::Auto
        );
    }

    #[test]
    fn a_market_order_validates_without_a_price() {
        let order = RithmicOrder {
            price_type: OrderType::Market,
            ..Default::default()
        };

        assert!(order.validate().is_ok());
    }

    #[test]
    fn a_limit_order_needs_a_price() {
        let mut order = RithmicOrder {
            price_type: OrderType::Limit,
            ..Default::default()
        };

        let err = order.validate().unwrap_err().to_string();
        assert!(err.contains("price is required"), "{err}");

        order.price = Some(5000.0);
        assert!(order.validate().is_ok());
    }

    #[test]
    fn a_stop_market_order_needs_a_trigger_but_no_price() {
        let mut order = RithmicOrder {
            price_type: OrderType::StopMarket,
            ..Default::default()
        };

        let err = order.validate().unwrap_err().to_string();
        assert!(err.contains("trigger_price is required"), "{err}");

        order.trigger_price = Some(4985.0);
        assert!(order.validate().is_ok());
    }

    #[test]
    fn a_stop_limit_order_needs_both() {
        let mut order = RithmicOrder {
            price_type: OrderType::StopLimit,
            price: Some(4980.0),
            ..Default::default()
        };

        assert!(order.validate().is_err());

        order.trigger_price = Some(4985.0);
        assert!(order.validate().is_ok());
    }

    /// `validate()` is the price rule and nothing else — a default order carries
    /// no instrument and no size, and that is the server's call to make.
    #[test]
    fn an_order_does_not_check_its_instrument_or_size() {
        let order = RithmicOrder {
            price_type: OrderType::Market,
            ..Default::default()
        };

        assert!(order.symbol.is_empty());
        assert_eq!(order.quantity, 0);
        assert!(order.validate().is_ok());
    }

    #[test]
    fn an_oco_leg_validates_on_the_same_rules() {
        let mut leg = RithmicOcoOrderLeg {
            price_type: OrderType::Limit,
            ..Default::default()
        };

        assert!(leg.validate().is_err());

        leg.price = Some(5000.0);
        assert!(leg.validate().is_ok());
    }

    /// The one place a crate-owned enum is wider than the message it targets.
    #[test]
    fn an_oco_leg_rejects_the_if_touched_price_types() {
        let leg = RithmicOcoOrderLeg {
            price_type: OrderType::LimitIfTouched,
            price: Some(5000.0),
            trigger_price: Some(5000.0),
            ..Default::default()
        };

        let err = leg.validate().unwrap_err().to_string();
        assert!(err.contains("LIMIT_IF_TOUCHED"), "{err}");
        assert!(err.contains("is not available on an OCO leg"), "{err}");
    }

    /// The group checks its legs, not how many of them there are.
    #[test]
    fn an_oco_order_validates_each_leg_but_not_the_count() {
        let ok = RithmicOcoOrderLeg {
            price_type: OrderType::Market,
            ..Default::default()
        };

        assert!(RithmicOcoOrder { legs: Vec::new() }.validate().is_ok());
        assert!(
            RithmicOcoOrder {
                legs: vec![ok.clone()]
            }
            .validate()
            .is_ok()
        );

        let bad = RithmicOcoOrderLeg {
            price_type: OrderType::Limit,
            ..Default::default()
        };
        assert!(
            RithmicOcoOrder {
                legs: vec![ok, bad]
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn a_bracket_validates_its_entry_leg() {
        let mut order = RithmicBracketOrder {
            price_type: OrderType::Limit,
            ..Default::default()
        };

        assert!(order.validate().is_err());

        order.price = Some(5000.0);
        assert!(order.validate().is_ok());
    }

    /// Nothing about the exit legs is checked. Rithmic's limits on multi-level
    /// brackets are undocumented and no other client implements them, so the
    /// crate would only be guessing at which shapes the server rejects.
    #[test]
    fn a_bracket_does_not_check_its_exit_legs() {
        // Mismatched vector lengths.
        let ragged = RithmicBracketOrder {
            price_type: OrderType::Market,
            target_quantity: vec![1],
            target_ticks: vec![16, 24],
            ..Default::default()
        };
        assert!(ragged.validate().is_ok());

        // No exit legs at all.
        let bare = RithmicBracketOrder {
            price_type: OrderType::Market,
            ..Default::default()
        };
        assert!(bare.validate().is_ok());

        // A bracket_type that disagrees with the legs supplied.
        let mismatched = RithmicBracketOrder {
            price_type: OrderType::Market,
            bracket_type: Some(BracketType::TargetOnly),
            stop_quantity: vec![1],
            stop_ticks: vec![10],
            ..Default::default()
        };
        assert!(mismatched.validate().is_ok());
    }

    /// The message names the protobuf type the caller set, not a Rust-side
    /// paraphrase, so it lines up with what Rithmic's docs call the order type.
    #[test]
    fn the_error_names_the_order_type() {
        let order = RithmicOrder {
            price_type: OrderType::LimitIfTouched,
            ..Default::default()
        };

        let err = order.validate().unwrap_err().to_string();
        assert!(err.contains("LIMIT_IF_TOUCHED"), "{err}");
    }

    /// A modify has no price rule to apply, so nothing is checked.
    #[test]
    fn a_modify_validates_unconditionally() {
        assert!(RithmicModifyOrder::default().validate().is_ok());
    }

    /// A stop modify without a trigger stays valid — the sender falls back to
    /// `price`, so a price rule here would reject something that works today.
    #[test]
    fn a_stop_modify_does_not_need_a_trigger_price() {
        let modify = RithmicModifyOrder {
            id: "123456".to_string(),
            symbol: "ESH6".to_string(),
            exchange: "CME".to_string(),
            quantity: 2,
            price: 5005.0,
            price_type: OrderType::StopMarket,
            ..Default::default()
        };

        assert!(modify.validate().is_ok());
    }

    /// The id-carrying commands have no price rule, so they check nothing.
    #[test]
    fn the_id_carrying_commands_validate_unconditionally() {
        assert!(RithmicCancelOrder::default().validate().is_ok());
        assert!(RithmicCancelAllOrders::default().validate().is_ok());
        assert!(RithmicExitPosition::default().validate().is_ok());
        assert!(RithmicBracketLevelAdjustment::default().validate().is_ok());
        assert!(RithmicLinkOrders::default().validate().is_ok());
        assert!(
            RithmicModifyOrderReferenceData::default()
                .validate()
                .is_ok()
        );
    }
}
