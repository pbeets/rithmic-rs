//! The chained setters every order command type carries.
//!
//! Every command is built the same way: `T::new()` starts from the command's
//! defaults, a chained setter covers each field, and `build()` runs
//! `validate()` before handing back the command.
//!
//! ```
//! use rithmic_rs::{OrderSide, OrderType, RithmicOrder};
//!
//! let order = RithmicOrder::new()
//!     .symbol("ESH6")
//!     .exchange("CME")
//!     .quantity(1)
//!     .transaction_type(OrderSide::Buy)
//!     .price_type(OrderType::Limit)
//!     .price(5000.0)
//!     .build()?;
//! # Ok::<(), rithmic_rs::RithmicError>(())
//! ```
//!
//! There is no separate builder type: the command is its own builder, and the
//! setters are sugar over the public fields. The command types are
//! `#[non_exhaustive]`, so a downstream crate cannot write `T { .. }` or
//! `..Default::default()`. `T::new()` is the replacement for both — it is the
//! same starting point `Default` would have given you.

use crate::api::rithmic_command_types::{
    RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders, RithmicCancelOrder,
    RithmicExitPosition, RithmicIfTouchedTrigger, RithmicLinkOrders, RithmicModifyOrder,
    RithmicModifyOrderReferenceData, RithmicOcoOrder, RithmicOcoOrderLeg, RithmicOrder,
    TrailingStop,
};
use crate::error::RithmicError;
use crate::types::{BracketType, OrderPlacement, OrderSide, OrderType, TimeInForce};

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
        self.duration = Some(duration);
        self
    }

    /// Trailing stop configuration.
    pub fn trailing_stop(mut self, trailing_stop: TrailingStop) -> Self {
        self.trailing_stop = Some(trailing_stop);
        self
    }

    /// Trail by `trail_by_ticks` against Rithmic's `trail_by_price_id`.
    pub fn trailing_stop_by(self, trail_by_ticks: i32, trail_by_price_id: i32) -> Self {
        self.trailing_stop(TrailingStop::new(trail_by_ticks, trail_by_price_id))
    }

    /// Route to send on, overriding the route published for the exchange.
    pub fn trade_route(mut self, trade_route: impl Into<String>) -> Self {
        self.trade_route = Some(trade_route.into());
        self
    }

    /// How this order is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Originating window name reported to Rithmic.
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

    /// Validate and return the order.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicOrder::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
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

    /// How this order is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Validate, resolve the bracket shape, and return the order.
    ///
    /// An unset [`RithmicBracketOrder::bracket_type`] is derived from the legs
    /// supplied, using the `Static` family. Building is the only place that
    /// derivation happens, so a command assembled field by field keeps whatever
    /// shape it was given.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicBracketOrder::validate`] rejects.
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

impl RithmicOcoOrderLeg {
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

    /// Number of contracts on this leg.
    pub fn quantity(mut self, quantity: i32) -> Self {
        self.quantity = quantity;
        self
    }

    /// Buy or sell.
    pub fn transaction_type(mut self, transaction_type: OrderSide) -> Self {
        self.transaction_type = transaction_type;
        self
    }

    /// Market, limit, or stop. The if-touched types are not available here.
    pub fn price_type(mut self, price_type: OrderType) -> Self {
        self.price_type = price_type;
        self
    }

    /// Leg price.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Trigger price for stop order types.
    pub fn trigger_price(mut self, trigger_price: f64) -> Self {
        self.trigger_price = Some(trigger_price);
        self
    }

    /// How long the leg stays working.
    pub fn duration(mut self, duration: TimeInForce) -> Self {
        self.duration = duration;
        self
    }

    /// Your identifier for this leg.
    pub fn user_tag(mut self, user_tag: impl Into<String>) -> Self {
        self.user_tag = user_tag.into();
        self
    }

    /// Trailing stop configuration for this leg.
    pub fn trailing_stop(mut self, trailing_stop: TrailingStop) -> Self {
        self.trailing_stop = Some(trailing_stop);
        self
    }

    /// Trail by `trail_by_ticks` against Rithmic's `trail_by_price_id`.
    pub fn trailing_stop_by(self, trail_by_ticks: i32, trail_by_price_id: i32) -> Self {
        self.trailing_stop(TrailingStop::new(trail_by_ticks, trail_by_price_id))
    }

    /// Route to send on, overriding the route published for the exchange.
    pub fn trade_route(mut self, trade_route: impl Into<String>) -> Self {
        self.trade_route = Some(trade_route.into());
        self
    }

    /// How this leg is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Validate and return the leg.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicOcoOrderLeg::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicOcoOrder {
    /// Start from the defaults, with no legs.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one more leg.
    pub fn leg(mut self, leg: RithmicOcoOrderLeg) -> Self {
        self.legs.push(leg);
        self
    }

    /// Append several more legs.
    pub fn legs(mut self, legs: impl IntoIterator<Item = RithmicOcoOrderLeg>) -> Self {
        self.legs.extend(legs);
        self
    }

    /// Validate and return the group.
    ///
    /// Rithmic has nothing to cancel against a group of one, so the handle
    /// refuses a group shorter than two legs; the count is not checked here.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicOcoOrder::validate`] rejects, including each leg's own
    /// rules.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicModifyOrder {
    /// Start from the defaults.
    ///
    /// A modify restates the order rather than patching it, so every field that
    /// describes the resulting order has to be set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `basket_id` of the order being modified.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
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

    /// The order's size after the modification.
    pub fn quantity(mut self, quantity: i32) -> Self {
        self.quantity = quantity;
        self
    }

    /// The order's price after the modification.
    pub fn price(mut self, price: f64) -> Self {
        self.price = price;
        self
    }

    /// The order's type after the modification.
    pub fn price_type(mut self, price_type: OrderType) -> Self {
        self.price_type = price_type;
        self
    }

    /// Separate trigger price. Unset, stop types fall back to `price`.
    pub fn trigger_price(mut self, trigger_price: f64) -> Self {
        self.trigger_price = Some(trigger_price);
        self
    }

    /// How this modification is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Validate and return the modification.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicModifyOrder::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicCancelOrder {
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `basket_id` of the order to cancel.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// How this cancellation is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Validate and return the cancellation.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicCancelOrder::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicCancelAllOrders {
    /// Start from the defaults, which attribute the cancellation to `Auto`.
    pub fn new() -> Self {
        Self::default()
    }

    /// How this cancellation is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Return the command.
    ///
    /// # Errors
    ///
    /// Never — the command carries nothing to check.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicExitPosition {
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Instrument symbol of the position to exit.
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    /// Exchange the instrument trades on.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// How this exit is attributed to its originator.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderPlacement) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Validate and return the command.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicExitPosition::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
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

    /// Which bracket leg to adjust. Unset, the field is omitted.
    pub fn level(mut self, level: i32) -> Self {
        self.level = Some(level);
        self
    }

    /// Validate and return the adjustment.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicBracketLevelAdjustment::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicLinkOrders {
    /// Start from the defaults, with no baskets to link.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one more `basket_id`.
    pub fn basket_id(mut self, basket_id: impl Into<String>) -> Self {
        self.basket_ids.push(basket_id.into());
        self
    }

    /// Append several more `basket_id`s.
    pub fn basket_ids(mut self, basket_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.basket_ids
            .extend(basket_ids.into_iter().map(Into::into));
        self
    }

    /// Validate and return the command.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicLinkOrders::validate`] rejects.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

impl RithmicModifyOrderReferenceData {
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `basket_id` of the order to retag.
    pub fn basket_id(mut self, basket_id: impl Into<String>) -> Self {
        self.basket_id = basket_id.into();
        self
    }

    /// The new tag. Empty clears the tag.
    pub fn user_tag(mut self, user_tag: impl Into<String>) -> Self {
        self.user_tag = user_tag.into();
        self
    }

    /// Validate and return the command.
    ///
    /// # Errors
    ///
    /// Whatever [`RithmicModifyOrderReferenceData::validate`] rejects.
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

    fn bracket(quantity: i32, price_type: OrderType) -> RithmicBracketOrder {
        RithmicBracketOrder::new()
            .symbol("ESH6")
            .exchange("CME")
            .quantity(quantity)
            .action(OrderSide::Buy)
            .price_type(price_type)
    }

    fn leg() -> RithmicOcoOrderLeg {
        RithmicOcoOrderLeg::new()
            .symbol("ESH6")
            .exchange("CME")
            .quantity(1)
            .transaction_type(OrderSide::Sell)
            .price_type(OrderType::Market)
    }

    #[test]
    fn an_order_rejects_what_validate_rejects() {
        assert!(order().build().is_err());
        assert!(order().price(5000.0).build().is_ok());
    }

    /// `build()` runs `validate()`, which is a price rule only — an empty
    /// instrument or a zero size is left for the server to reject.
    #[test]
    fn an_order_passes_through_an_empty_instrument_and_a_zero_size() {
        assert!(
            RithmicOrder::new()
                .price_type(OrderType::Market)
                .build()
                .is_ok()
        );
    }

    /// `new()` is the starting point `Default` would have given you.
    #[test]
    fn new_matches_default() {
        assert_eq!(RithmicOrder::new(), RithmicOrder::default());
        assert_eq!(RithmicBracketOrder::new(), RithmicBracketOrder::default());
        assert_eq!(RithmicOcoOrder::new(), RithmicOcoOrder::default());
        assert_eq!(
            RithmicCancelAllOrders::new(),
            RithmicCancelAllOrders::default()
        );
    }

    #[test]
    fn an_order_sets_every_optional_field() {
        let order = order()
            .transaction_type(OrderSide::Sell)
            .price_type(OrderType::StopLimit)
            .price(4980.0)
            .trigger_price(4985.0)
            .user_tag("stop-order")
            .duration(TimeInForce::Gtc)
            .trailing_stop_by(20, 1)
            .trade_route("route-1")
            .manual_or_auto(OrderPlacement::Manual)
            .window_name("chart")
            .release_at(35900, 500)
            .cancel_at(36000, 250)
            .cancel_after_secs(120)
            .build()
            .unwrap();

        assert_eq!(order.duration, Some(TimeInForce::Gtc));
        assert_eq!(order.manual_or_auto, OrderPlacement::Manual);
        assert_eq!(order.trailing_stop.unwrap().trail_by_price_id, 1);
        assert_eq!(order.release_at_ssboe, Some(35900));
        assert_eq!(order.release_at_usecs, Some(500));
        assert_eq!(order.cancel_at_ssboe, Some(36000));
        assert_eq!(order.cancel_at_usecs, Some(250));
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

    #[test]
    fn an_oco_leg_rejects_the_if_touched_price_types() {
        let err = leg()
            .price_type(OrderType::MarketIfTouched)
            .trigger_price(4980.0)
            .build()
            .unwrap_err()
            .to_string();

        assert!(err.contains("is not available on an OCO leg"), "{err}");
    }

    #[test]
    fn an_oco_order_collects_its_legs() {
        let tagged = |tag: &str| leg().user_tag(tag).build().unwrap();

        let order = RithmicOcoOrder::new()
            .legs([tagged("a"), tagged("b")])
            .leg(tagged("c"))
            .legs([tagged("d"), tagged("e")])
            .build()
            .unwrap();

        assert_eq!(order.legs.len(), 5);
    }

    #[test]
    fn an_oco_order_rejects_an_invalid_leg() {
        let good = leg().build().unwrap();
        let bad = leg().price_type(OrderType::MarketIfTouched);

        assert!(RithmicOcoOrder::new().legs([good, bad]).build().is_err());
    }

    /// None of these carry a price rule, so `build()` only assembles them.
    #[test]
    fn the_id_carrying_commands_accept_an_empty_id() {
        assert!(RithmicCancelOrder::new().build().is_ok());
        assert!(
            RithmicBracketLevelAdjustment::new()
                .ticks(16)
                .level(2)
                .build()
                .is_ok()
        );
        assert!(
            RithmicModifyOrderReferenceData::new()
                .user_tag("tag")
                .build()
                .is_ok()
        );
        assert!(RithmicExitPosition::new().build().is_ok());
        assert!(RithmicLinkOrders::new().basket_id("123456").build().is_ok());
        assert!(
            RithmicModifyOrder::new()
                .price(5005.0)
                .price_type(OrderType::Limit)
                .build()
                .is_ok()
        );
    }

    #[test]
    fn a_link_command_collects_its_ids() {
        let command = RithmicLinkOrders::new()
            .basket_ids(["123456"])
            .basket_id("123457")
            .build()
            .unwrap();

        assert_eq!(command.basket_ids, ["123456", "123457"]);
    }

    #[test]
    fn cancel_all_defaults_to_auto_and_takes_a_placement() {
        assert_eq!(
            RithmicCancelAllOrders::new()
                .build()
                .unwrap()
                .manual_or_auto,
            OrderPlacement::Auto
        );
        assert_eq!(
            RithmicCancelAllOrders::new()
                .manual_or_auto(OrderPlacement::Manual)
                .build()
                .unwrap()
                .manual_or_auto,
            OrderPlacement::Manual
        );
    }
}
