//! Bracket entry orders and the adjustment that moves one of their exit legs.

use super::trailing::RithmicIfTouchedTrigger;
use crate::{
    error::RithmicError,
    types::{BracketType, OrderOrigin, OrderSide, OrderType, TimeInForce},
};

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
    /// Entry price. A market entry does not need one.
    pub price: Option<f64>,
    /// Trigger price. Only a stop or if-touched entry needs one.
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
    /// [`Self::build`] resolves it from the target and stop legs, and leaves it
    /// unset when there are none.
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
    /// Whether the order was placed by a human or automatically.
    pub manual_or_auto: OrderOrigin,
}

/// The exit-leg setters come in singular and plural. Singular sets one leg
/// sized to the entry quantity; plural takes explicit `(quantity, ticks)`
/// pairs.
///
/// ```
/// use rithmic_rs::RithmicBracketOrder;
///
/// let sized = RithmicBracketOrder::new().quantity(2).target(8).stop(4);
/// let explicit = RithmicBracketOrder::new()
///     .targets([(1, 8), (1, 16)])
///     .stops([(2, 4)]);
/// ```
impl RithmicBracketOrder {
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

    /// Number of contracts on the entry.
    ///
    /// Set this before [`Self::target`] or [`Self::stop`], which size their
    /// leg to whatever the quantity is when they are called.
    pub fn quantity(mut self, quantity: i32) -> Self {
        self.quantity = quantity;
        self
    }

    /// Buy or sell on the entry.
    pub fn action(mut self, action: OrderSide) -> Self {
        self.action = action;
        self
    }

    /// Market, limit, stop, or if-touched entry.
    pub fn price_type(mut self, price_type: OrderType) -> Self {
        self.price_type = price_type;
        self
    }

    /// How long the entry stays working.
    pub fn duration(mut self, duration: TimeInForce) -> Self {
        self.duration = duration;
        self
    }

    /// Your identifier for tracking this order.
    pub fn localid(mut self, localid: impl Into<String>) -> Self {
        self.localid = localid.into();
        self
    }

    /// Entry price.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Trigger price for stop and if-touched entry types.
    pub fn trigger_price(mut self, trigger_price: f64) -> Self {
        self.trigger_price = Some(trigger_price);
        self
    }

    /// Bracket shape, overriding what `build()` would derive from the legs.
    pub fn bracket_type(mut self, bracket_type: BracketType) -> Self {
        self.bracket_type = Some(bracket_type);
        self
    }

    /// One target leg at this tick distance, sized to the entry quantity.
    ///
    /// Reads [`Self::quantity`] as it stands right now, so set the quantity
    /// first — otherwise the leg is sized to 0.
    pub fn target(mut self, ticks: i32) -> Self {
        self.target_quantity = vec![self.quantity];
        self.target_ticks = vec![ticks];
        self
    }

    /// One stop leg at this tick distance, sized to the entry quantity.
    ///
    /// Reads [`Self::quantity`] as it stands right now, so set the quantity
    /// first — otherwise the leg is sized to 0.
    pub fn stop(mut self, ticks: i32) -> Self {
        self.stop_quantity = vec![self.quantity];
        self.stop_ticks = vec![ticks];
        self
    }

    /// Target legs as `(quantity, ticks)` pairs, replacing any already set.
    pub fn targets(mut self, legs: impl IntoIterator<Item = (i32, i32)>) -> Self {
        let (quantities, ticks): (Vec<i32>, Vec<i32>) = legs.into_iter().unzip();
        self.target_quantity = quantities;
        self.target_ticks = ticks;
        self
    }

    /// Stop legs as `(quantity, ticks)` pairs, replacing any already set.
    pub fn stops(mut self, legs: impl IntoIterator<Item = (i32, i32)>) -> Self {
        let (quantities, ticks): (Vec<i32>, Vec<i32>) = legs.into_iter().unzip();
        self.stop_quantity = quantities;
        self.stop_ticks = ticks;
        self
    }

    /// Conditional trigger that releases the entry once touched.
    pub fn if_touched(mut self, if_touched: RithmicIfTouchedTrigger) -> Self {
        self.if_touched = Some(if_touched);
        self
    }

    /// Move the stop to break-even by this many ticks.
    pub fn break_even_ticks(mut self, ticks: i32) -> Self {
        self.break_even_ticks = Some(ticks);
        self
    }

    /// Trigger break-even once the position reaches this many ticks.
    pub fn break_even_trigger_ticks(mut self, ticks: i32) -> Self {
        self.break_even_trigger_ticks = Some(ticks);
        self
    }

    /// Enable a trailing stop after this many ticks.
    pub fn trailing_stop_trigger_ticks(mut self, ticks: i32) -> Self {
        self.trailing_stop_trigger_ticks = Some(ticks);
        self
    }

    /// Track the trailing stop against the last trade instead of bid/offer.
    pub fn trailing_stop_by_last_trade_price(mut self, by_last_trade_price: bool) -> Self {
        self.trailing_stop_by_last_trade_price = Some(by_last_trade_price);
        self
    }

    /// Convert the target to market-if-touched once touched.
    pub fn target_market_order_if_touched(mut self, market_if_touched: bool) -> Self {
        self.target_market_order_if_touched = Some(market_if_touched);
        self
    }

    /// Convert the stop to market if the resting stop order is rejected.
    pub fn stop_market_on_reject(mut self, market_on_reject: bool) -> Self {
        self.stop_market_on_reject = Some(market_on_reject);
        self
    }

    /// Convert the target to market at this second-since-beginning-of-epoch value.
    pub fn target_market_at_ssboe(mut self, ssboe: i32) -> Self {
        self.target_market_at_ssboe = Some(ssboe);
        self
    }

    /// Microsecond component of the target's market-conversion time.
    pub fn target_market_at_usecs(mut self, usecs: i32) -> Self {
        self.target_market_at_usecs = Some(usecs);
        self
    }

    /// Set both halves of the target's market-conversion time.
    pub fn target_market_at(self, ssboe: i32, usecs: i32) -> Self {
        self.target_market_at_ssboe(ssboe)
            .target_market_at_usecs(usecs)
    }

    /// Convert the stop to market at this second-since-beginning-of-epoch value.
    pub fn stop_market_at_ssboe(mut self, ssboe: i32) -> Self {
        self.stop_market_at_ssboe = Some(ssboe);
        self
    }

    /// Microsecond component of the stop's market-conversion time.
    pub fn stop_market_at_usecs(mut self, usecs: i32) -> Self {
        self.stop_market_at_usecs = Some(usecs);
        self
    }

    /// Set both halves of the stop's market-conversion time.
    pub fn stop_market_at(self, ssboe: i32, usecs: i32) -> Self {
        self.stop_market_at_ssboe(ssboe).stop_market_at_usecs(usecs)
    }

    /// Convert the target to market after this many seconds.
    pub fn target_market_order_after_secs(mut self, secs: i32) -> Self {
        self.target_market_order_after_secs = Some(secs);
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

    /// Route to send on, overriding the route published for the exchange.
    pub fn trade_route(mut self, trade_route: impl Into<String>) -> Self {
        self.trade_route = Some(trade_route.into());
        self
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderOrigin) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Check the entry carries the prices its [`Self::price_type`] requires:
    /// `Limit`, `StopLimit` and `LimitIfTouched` need [`Self::price`];
    /// `StopMarket`, `StopLimit`, `MarketIfTouched` and `LimitIfTouched` need
    /// [`Self::trigger_price`]. `Market` needs neither. The exit legs are not
    /// checked.
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

    /// Validate and return the order, deriving an unset [`Self::bracket_type`]
    /// from the exit legs supplied.
    pub fn build(mut self) -> Result<Self, RithmicError> {
        self.validate()?;

        if self.bracket_type.is_none() {
            let has_targets = !self.target_ticks.is_empty();
            let has_stops = !self.stop_ticks.is_empty();

            self.bracket_type = match (has_targets, has_stops) {
                (true, true) => Some(BracketType::TargetAndStopStatic),
                (true, false) => Some(BracketType::TargetOnlyStatic),
                (false, true) => Some(BracketType::StopOnlyStatic),
                // No exit legs to describe, so invent no shape.
                (false, false) => None,
            };
        }

        Ok(self)
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
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicBracketLevelAdjustment {
    /// The `basket_id` from the order notification
    pub id: String,
    /// The new distance in ticks
    pub ticks: i32,
    /// Which bracket leg to adjust — a target leg via `adjust_target`, a stop
    /// leg via `adjust_stop`.
    pub level: Option<i32>,
}

impl RithmicBracketLevelAdjustment {
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `basket_id` of the bracket to adjust.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// The new distance in ticks.
    pub fn ticks(mut self, ticks: i32) -> Self {
        self.ticks = ticks;
        self
    }

    /// Which bracket leg to adjust — a target leg via `adjust_target`, a stop
    /// leg via `adjust_stop`.
    pub fn level(mut self, level: i32) -> Self {
        self.level = Some(level);
        self
    }

    /// Return the adjustment.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bracket(quantity: i32, price_type: OrderType) -> RithmicBracketOrder {
        RithmicBracketOrder::new()
            .symbol("ESH6")
            .exchange("CME")
            .quantity(quantity)
            .action(OrderSide::Buy)
            .price_type(price_type)
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

    /// The ergonomic one-target/one-stop path has to produce exactly what the
    /// explicit vectors produce, including the `Static` shape the crate has
    /// always sent for a simple bracket.
    #[test]
    fn the_bracket_sugar_matches_the_explicit_vectors() {
        let sugar = bracket(2, OrderType::Limit)
            .price(5000.0)
            .target(20)
            .stop(10)
            .build()
            .unwrap();

        let explicit = bracket(2, OrderType::Limit)
            .price(5000.0)
            .targets([(2, 20)])
            .stops([(2, 10)])
            .build()
            .unwrap();

        assert_eq!(sugar.target_quantity, explicit.target_quantity);
        assert_eq!(sugar.target_ticks, explicit.target_ticks);
        assert_eq!(sugar.stop_quantity, explicit.stop_quantity);
        assert_eq!(sugar.stop_ticks, explicit.stop_ticks);
        assert_eq!(sugar.bracket_type, explicit.bracket_type);
        assert_eq!(
            sugar.bracket_type,
            Some(BracketType::TargetAndStopStatic),
            "the simple path must stay byte-identical to what it sent before"
        );
    }

    /// The sizing reads `quantity` where it stands, so the setter order that
    /// looks equivalent is not.
    #[test]
    fn the_bracket_sugar_sizes_its_leg_to_the_quantity_set_so_far() {
        let after = RithmicBracketOrder::new()
            .price_type(OrderType::Market)
            .quantity(3)
            .target(20)
            .build()
            .unwrap();
        assert_eq!(after.target_quantity, vec![3]);

        let before = RithmicBracketOrder::new()
            .price_type(OrderType::Market)
            .target(20)
            .quantity(3)
            .build()
            .unwrap();
        assert_eq!(
            before.target_quantity,
            vec![0],
            "quantity set after the leg cannot reach back and resize it"
        );
    }

    #[test]
    fn the_bracket_derives_the_shape_from_the_legs() {
        let target_only = bracket(1, OrderType::Market).target(20).build().unwrap();
        assert_eq!(
            target_only.bracket_type,
            Some(BracketType::TargetOnlyStatic)
        );

        let stop_only = bracket(1, OrderType::Market).stop(10).build().unwrap();
        assert_eq!(stop_only.bracket_type, Some(BracketType::StopOnlyStatic));

        let explicit = bracket(1, OrderType::Market)
            .stop(10)
            .bracket_type(BracketType::StopOnly)
            .build()
            .unwrap();
        assert_eq!(explicit.bracket_type, Some(BracketType::StopOnly));
    }

    #[test]
    fn a_bracket_rejects_what_validate_rejects() {
        let err = bracket(1, OrderType::Limit)
            .target(20)
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("price is required"), "{err}");
    }

    /// With no exit legs there is no shape to derive, so `bracket_type` is left
    /// unset rather than guessed at.
    #[test]
    fn a_bracket_leaves_the_shape_unset_when_there_are_no_exit_legs() {
        assert_eq!(
            bracket(1, OrderType::Market).build().unwrap().bracket_type,
            None
        );
    }
}
