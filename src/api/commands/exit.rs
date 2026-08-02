//! Flattening a position, and linking working orders into one group.

use crate::{error::RithmicError, types::OrderOrigin};

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
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicExitPosition {
    /// Trading symbol (e.g., "ESM6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Whether the exit was made by a human or automatically.
    pub manual_or_auto: OrderOrigin,
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
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicLinkOrders {
    /// The `basket_id`s to link, from the order notifications.
    pub basket_ids: Vec<String>,
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

    /// Return the command.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_link_command_collects_its_ids() {
        let command = RithmicLinkOrders::new()
            .basket_ids(["123456"])
            .basket_id("123457")
            .build()
            .unwrap();

        assert_eq!(command.basket_ids, ["123456", "123457"]);
    }
}
