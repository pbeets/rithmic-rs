//! Modifying a working order: its terms, and the tag it reports under.

use super::triggers::RithmicIfTouchedTrigger;
use super::validate_instrument;

use crate::{
    error::RithmicError,
    types::{ManualOrAutoEntry, OrderType},
};

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
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a modification does nothing until passed to a plant handle"]
pub struct RithmicModifyOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// Exchange code
    pub exchange: String,
    /// Trading symbol
    pub symbol: String,
    /// New quantity
    pub quantity: i32,
    /// New price, omitted from the request when unset. A modify restates the
    /// order, so set this to the order's current price when only the quantity
    /// is changing.
    pub price: Option<f64>,
    /// Order type
    pub price_type: OrderType,
    /// Trigger price. Left unset, the four triggering price types — the stop and
    /// if-touched pairs — send `price` in its place.
    pub trigger_price: Option<f64>,
    /// Whether the modification was made by a human or automatically.
    pub manual_or_auto: ManualOrAutoEntry,
    /// Originating window name reported to Rithmic.
    pub window_name: Option<String>,
    /// Ticks to trail behind the market price.
    ///
    /// A bare distance, not a [`TrailingStop`](crate::TrailingStop) — a modify
    /// takes no price-id.
    pub trail_by_ticks: Option<i32>,
    /// Conditional trigger on the resulting order.
    pub if_touched: Option<RithmicIfTouchedTrigger>,
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
        self.price = Some(price);
        self
    }

    /// The order's type after the modification.
    pub fn price_type(mut self, price_type: OrderType) -> Self {
        self.price_type = price_type;
        self
    }

    /// Trigger price distinct from the limit price. Left unset, the triggering
    /// price types send `price` in its place.
    pub fn trigger_price(mut self, trigger_price: f64) -> Self {
        self.trigger_price = Some(trigger_price);
        self
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: ManualOrAutoEntry) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Window name to report this modification under.
    pub fn window_name(mut self, window_name: impl Into<String>) -> Self {
        self.window_name = Some(window_name.into());
        self
    }

    /// Trail the resulting order this many ticks behind the market price.
    pub fn trail_by_ticks(mut self, trail_by_ticks: i32) -> Self {
        self.trail_by_ticks = Some(trail_by_ticks);
        self
    }

    /// Attach a conditional trigger to the resulting order.
    pub fn if_touched(mut self, if_touched: RithmicIfTouchedTrigger) -> Self {
        self.if_touched = Some(if_touched);
        self
    }

    /// Check the modification carries the prices its [`Self::price_type`]
    /// requires: `Limit`, `StopLimit` and `LimitIfTouched` need [`Self::price`];
    /// `StopMarket`, `StopLimit`, `MarketIfTouched` and `LimitIfTouched` need a
    /// trigger, which is [`Self::trigger_price`] or the [`Self::price`] that
    /// stands in for it. `Market` needs neither.
    pub fn validate(&self) -> Result<(), RithmicError> {
        if self.id.is_empty() {
            return Err(RithmicError::InvalidArgument(
                "a modify requires the basket_id of the order it restates".to_string(),
            ));
        }

        validate_instrument(&self.symbol, &self.exchange, self.quantity)?;

        let (needs_price, needs_trigger) = super::price_requirements(self.price_type);

        let order_type = self.price_type.as_str_name();

        if needs_price && self.price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "price is required for a {order_type} order"
            )));
        }

        if needs_trigger && self.trigger_price.is_none() && self.price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "trigger_price, or a price to stand in for it, is required for a {order_type} order"
            )));
        }

        Ok(())
    }

    /// Requires the basket_id, the instrument, and the prices the price type
    /// needs.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
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
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a command does nothing until passed to a plant handle"]
pub struct RithmicModifyOrderReferenceData {
    /// The `basket_id` from the order notification.
    pub basket_id: String,
    /// The new tag. Empty is how a tag is cleared, so it is sent as given.
    pub user_tag: String,
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

    /// Requires the basket_id; the tag itself may be empty.
    pub fn validate(&self) -> Result<(), RithmicError> {
        if self.basket_id.is_empty() {
            return Err(RithmicError::InvalidArgument(
                "a retag requires the basket_id of the order it retags".to_string(),
            ));
        }
        Ok(())
    }

    /// Requires the basket_id; the tag itself may be empty.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modify(price_type: OrderType) -> RithmicModifyOrder {
        RithmicModifyOrder::new()
            .id("b")
            .symbol("ESM6")
            .exchange("CME")
            .quantity(1)
            .price_type(price_type)
    }

    /// The table is `RithmicOrder::validate`'s, except that a triggering type
    /// accepts `price` standing in for the trigger — a modify restates the
    /// order, and moving a stop by its price alone predates `trigger_price`.
    #[test]
    fn a_modify_requires_the_prices_its_type_needs() {
        assert!(modify(OrderType::Market).build().is_ok());

        assert!(modify(OrderType::Limit).build().is_err());
        assert!(modify(OrderType::Limit).price(5000.0).build().is_ok());

        // Neither a trigger nor a price to stand in for it.
        assert!(modify(OrderType::StopMarket).build().is_err());
        assert!(modify(OrderType::StopMarket).price(5000.0).build().is_ok());
        assert!(
            modify(OrderType::StopMarket)
                .trigger_price(5000.0)
                .build()
                .is_ok()
        );

        // The limit price cannot be stood in for.
        assert!(
            modify(OrderType::StopLimit)
                .trigger_price(4999.0)
                .build()
                .is_err()
        );
        assert!(modify(OrderType::StopLimit).price(5000.0).build().is_ok());

        assert!(modify(OrderType::MarketIfTouched).build().is_err());
        assert!(
            modify(OrderType::LimitIfTouched)
                .price(5000.0)
                .build()
                .is_ok()
        );
    }
    #[test]
    fn a_modify_requires_the_basket_id_and_instrument() {
        assert!(modify(OrderType::Market).id("").build().is_err());
        assert!(modify(OrderType::Market).symbol("").build().is_err());
        assert!(modify(OrderType::Market).quantity(0).build().is_err());
    }

    #[test]
    fn a_retag_requires_the_basket_id_but_takes_an_empty_tag() {
        assert!(RithmicModifyOrderReferenceData::new().build().is_err());
        assert!(
            RithmicModifyOrderReferenceData::new()
                .basket_id("b")
                .user_tag("")
                .build()
                .is_ok()
        );
    }
}
