//! Cancelling one working order, or every one on the account.

use crate::error::RithmicError;
use crate::types::OrderOrigin;

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
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicCancelOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// Whether the cancellation was made by a human or automatically.
    pub manual_or_auto: OrderOrigin,
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

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderOrigin) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Return the cancellation.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
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
/// use rithmic_rs::{OrderOrigin, RithmicCancelAllOrders};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let auto = RithmicCancelAllOrders::new().build()?;
/// let manual = RithmicCancelAllOrders::new()
///     .manual_or_auto(OrderOrigin::Manual)
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicCancelAllOrders {
    /// Whether the cancellation was made by a human or automatically.
    pub manual_or_auto: OrderOrigin,
}

impl RithmicCancelAllOrders {
    /// Start from the defaults, which attribute the cancellation to `Auto`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderOrigin) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Return the command.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_all_defaults_to_auto_and_takes_a_placement() {
        assert_eq!(
            RithmicCancelAllOrders::new()
                .build()
                .unwrap()
                .manual_or_auto,
            OrderOrigin::Auto
        );
        assert_eq!(
            RithmicCancelAllOrders::new()
                .manual_or_auto(OrderOrigin::Manual)
                .build()
                .unwrap()
                .manual_or_auto,
            OrderOrigin::Manual
        );
    }
}
