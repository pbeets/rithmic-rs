//! Modifying a working order: its terms, and the tag it reports under.

use super::triggers::RithmicIfTouchedTrigger;
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
    /// New price. A modify restates the order, so set this to the order's
    /// current price when only the quantity is changing.
    pub price: f64,
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
    /// A bare distance rather than a [`TrailingStop`](crate::TrailingStop):
    /// template version 5.28 added `trailing_stop` and `trail_by_ticks` to
    /// `RequestModifyOrder`, but `trail_by_price_id` only to `RequestNewOrder`,
    /// `RequestBracketOrder` and `RequestOCOOrder`. There is no price-id field
    /// here to set.
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
        self.price = price;
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

    /// Return the modification.
    pub fn build(self) -> Result<Self, RithmicError> {
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
#[non_exhaustive]
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

    /// Return the command.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
    }
}
