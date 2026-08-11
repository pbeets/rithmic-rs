//! A standalone order.

use super::triggers::{RithmicIfTouchedTrigger, TrailingStop};
use super::validate_instrument;

use crate::{
    error::RithmicError,
    types::{ManualOrAutoEntry, OrderSide, OrderType, TimeInForce},
};

/// A standalone order (not a bracket order).
///
/// For orders with automatic profit targets and stop losses, use
/// [`RithmicBracketOrder`](crate::RithmicBracketOrder) instead.
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
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "an order does nothing until passed to a plant handle"]
pub struct RithmicOrder {
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Order price. A market order does not need one.
    pub price: Option<f64>,
    /// Buy or Sell
    pub transaction_type: OrderSide,
    /// Order type (Limit, Market, StopLimit, StopMarket, etc.)
    pub price_type: OrderType,
    /// Your identifier for tracking this order
    pub user_tag: String,
    /// Order duration
    pub duration: TimeInForce,
    /// Trigger price. Only a stop or if-touched order needs one.
    pub trigger_price: Option<f64>,
    /// Trailing stop configuration
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for `exchange`.
    pub trade_route: Option<String>,
    /// Whether the order was placed by a human or automatically.
    pub manual_or_auto: ManualOrAutoEntry,
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
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Instrument symbol.
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    /// Exchange the instrument trades on.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Number of contracts.
    pub fn quantity(mut self, quantity: i32) -> Self {
        self.quantity = quantity;
        self
    }

    /// Buy or sell.
    pub fn transaction_type(mut self, transaction_type: OrderSide) -> Self {
        self.transaction_type = transaction_type;
        self
    }

    /// Market, limit, stop, or if-touched.
    pub fn price_type(mut self, price_type: OrderType) -> Self {
        self.price_type = price_type;
        self
    }

    /// Order price.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Trigger price for stop and if-touched order types.
    pub fn trigger_price(mut self, trigger_price: f64) -> Self {
        self.trigger_price = Some(trigger_price);
        self
    }

    /// Your identifier for this order.
    pub fn user_tag(mut self, user_tag: impl Into<String>) -> Self {
        self.user_tag = user_tag.into();
        self
    }

    /// How long the order stays working.
    pub fn duration(mut self, duration: TimeInForce) -> Self {
        self.duration = duration;
        self
    }

    /// Trailing stop configuration.
    pub fn trailing_stop(mut self, trailing_stop: TrailingStop) -> Self {
        self.trailing_stop = Some(trailing_stop);
        self
    }

    /// Trail by `trail_by_ticks` against Rithmic's `trail_by_price_id`.
    pub fn trailing_stop_by(self, trail_by_ticks: i32, trail_by_price_id: i32) -> Self {
        self.trailing_stop(
            TrailingStop::new()
                .trail_by_ticks(trail_by_ticks)
                .trail_by_price_id(trail_by_price_id),
        )
    }

    /// Route to send on, overriding the route published for the exchange.
    pub fn trade_route(mut self, trade_route: impl Into<String>) -> Self {
        self.trade_route = Some(trade_route.into());
        self
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: ManualOrAutoEntry) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Window name to report this order under.
    pub fn window_name(mut self, window_name: impl Into<String>) -> Self {
        self.window_name = Some(window_name.into());
        self
    }

    /// Release the order at this second-since-beginning-of-epoch value.
    pub fn release_at_ssboe(mut self, ssboe: i32) -> Self {
        self.release_at_ssboe = Some(ssboe);
        self
    }

    /// Microsecond component of the release time.
    pub fn release_at_usecs(mut self, usecs: i32) -> Self {
        self.release_at_usecs = Some(usecs);
        self
    }

    /// Set both halves of the release time.
    pub fn release_at(self, ssboe: i32, usecs: i32) -> Self {
        self.release_at_ssboe(ssboe).release_at_usecs(usecs)
    }

    /// Cancel the order at this second-since-beginning-of-epoch value.
    pub fn cancel_at_ssboe(mut self, ssboe: i32) -> Self {
        self.cancel_at_ssboe = Some(ssboe);
        self
    }

    /// Microsecond component of the cancel time.
    pub fn cancel_at_usecs(mut self, usecs: i32) -> Self {
        self.cancel_at_usecs = Some(usecs);
        self
    }

    /// Set both halves of the cancel time.
    pub fn cancel_at(self, ssboe: i32, usecs: i32) -> Self {
        self.cancel_at_ssboe(ssboe).cancel_at_usecs(usecs)
    }

    /// Cancel the order after this many seconds.
    pub fn cancel_after_secs(mut self, secs: i32) -> Self {
        self.cancel_after_secs = Some(secs);
        self
    }

    /// Conditional trigger that releases this order once touched.
    pub fn if_touched(mut self, if_touched: RithmicIfTouchedTrigger) -> Self {
        self.if_touched = Some(if_touched);
        self
    }

    /// Check the order names an instrument (symbol, exchange, a positive
    /// quantity) and carries the prices its [`Self::price_type`] requires:
    /// `Limit`, `StopLimit` and `LimitIfTouched` need [`Self::price`];
    /// `StopMarket`, `StopLimit`, `MarketIfTouched` and `LimitIfTouched` need
    /// [`Self::trigger_price`]. `Market` needs neither. An embedded
    /// [`TrailingStop`] or [`RithmicIfTouchedTrigger`] is deliberately not
    /// re-validated — `build()` on those types is the opt-in strict path.
    pub fn validate(&self) -> Result<(), RithmicError> {
        validate_instrument(&self.symbol, &self.exchange, self.quantity)?;

        super::require_prices(self.price_type, self.price, self.trigger_price)
    }

    /// Requires an instrument and the prices the price type needs.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order() -> RithmicOrder {
        RithmicOrder::new()
            .symbol("ESH6")
            .exchange("CME")
            .quantity(1)
            .transaction_type(OrderSide::Buy)
            .price_type(OrderType::Limit)
    }

    #[test]
    fn a_market_order_validates_without_a_price() {
        let order = RithmicOrder {
            price_type: OrderType::Market,
            ..order()
        };

        assert!(order.validate().is_ok());
    }

    #[test]
    fn a_limit_order_needs_a_price() {
        let mut order = RithmicOrder {
            price_type: OrderType::Limit,
            ..order()
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
            ..order()
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
            ..order()
        };

        assert!(order.validate().is_err());

        order.trigger_price = Some(4985.0);
        assert!(order.validate().is_ok());
    }

    /// The message names the protobuf type the caller set, not a Rust-side
    /// paraphrase, so it lines up with what Rithmic's docs call the order type.
    #[test]
    fn the_error_names_the_order_type() {
        let order = RithmicOrder {
            price_type: OrderType::LimitIfTouched,
            ..order()
        };

        let err = order.validate().unwrap_err().to_string();
        assert!(err.contains("LIMIT_IF_TOUCHED"), "{err}");
    }

    /// Only the setters that take more than one argument are worth asserting:
    /// each pair is same-typed, so a swapped argument compiles and would put the
    /// microseconds in the seconds field.
    #[test]
    fn the_paired_setters_assign_their_arguments_in_order() {
        let order = order()
            .price(4980.0)
            .trailing_stop_by(20, 1)
            .release_at(35900, 500)
            .cancel_at(36000, 250)
            .build()
            .unwrap();

        let trailing = order.trailing_stop.unwrap();
        assert_eq!(trailing.trail_by_ticks, 20);
        assert_eq!(trailing.trail_by_price_id, 1);
        assert_eq!(order.release_at_ssboe, Some(35900));
        assert_eq!(order.release_at_usecs, Some(500));
        assert_eq!(order.cancel_at_ssboe, Some(36000));
        assert_eq!(order.cancel_at_usecs, Some(250));
    }
    #[test]
    fn an_order_requires_its_identity() {
        assert!(order().symbol("").price(5000.0).build().is_err());
        assert!(order().exchange("").price(5000.0).build().is_err());
        assert!(order().quantity(0).price(5000.0).build().is_err());
        assert!(order().price(5000.0).build().is_ok());
    }
}
