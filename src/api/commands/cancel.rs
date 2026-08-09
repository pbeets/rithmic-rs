//! Cancelling one working order, or every one on the account.

use crate::{error::RithmicError, types::ManualOrAutoEntry};

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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a cancellation does nothing until passed to a plant handle"]
pub struct RithmicCancelOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// Whether the cancellation was made by a human or automatically.
    pub manual_or_auto: ManualOrAutoEntry,
    /// Originating window name reported to Rithmic.
    pub window_name: Option<String>,
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
    pub fn manual_or_auto(mut self, manual_or_auto: ManualOrAutoEntry) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Window name to report this cancellation under.
    pub fn window_name(mut self, window_name: impl Into<String>) -> Self {
        self.window_name = Some(window_name.into());
        self
    }

    /// Requires the basket_id of the order to cancel.
    pub fn validate(&self) -> Result<(), RithmicError> {
        if self.id.is_empty() {
            return Err(RithmicError::InvalidArgument(
                "a cancel requires the basket_id of the order it cancels".to_string(),
            ));
        }
        Ok(())
    }

    /// Requires the basket_id of the order to cancel.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
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
/// use rithmic_rs::{ManualOrAutoEntry, RithmicCancelAllOrders};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let auto = RithmicCancelAllOrders::new().build()?;
/// let manual = RithmicCancelAllOrders::new()
///     .manual_or_auto(ManualOrAutoEntry::Manual)
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a cancellation does nothing until passed to a plant handle"]
pub struct RithmicCancelAllOrders {
    /// Whether the cancellation was made by a human or automatically.
    pub manual_or_auto: ManualOrAutoEntry,
}

impl RithmicCancelAllOrders {
    /// Start from the defaults, which attribute the cancellation to `Auto`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: ManualOrAutoEntry) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Requires nothing — the command names no order.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cancel_requires_the_basket_id() {
        assert!(RithmicCancelOrder::new().build().is_err());
        assert!(RithmicCancelOrder::new().id("123456").build().is_ok());
        // Cancel-all names no order, so it has nothing to require.
        assert!(RithmicCancelAllOrders::new().build().is_ok());
    }
}
