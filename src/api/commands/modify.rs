//! Modifying a working order: its terms, and the tag it reports under.

use crate::error::RithmicError;
use crate::types::{OrderOrigin, OrderType};

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
    /// Trigger price. Unset, a stop modify falls back to `price`.
    pub trigger_price: Option<f64>,
    /// Whether the modification was made by a human or automatically.
    pub manual_or_auto: OrderOrigin,
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

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderOrigin) -> Self {
        self.manual_or_auto = manual_or_auto;
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
