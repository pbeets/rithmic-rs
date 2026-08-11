//! The trigger conditions an order embeds: trailing stops and if-touched
//! triggers.

use crate::{
    error::RithmicError,
    types::{OrderCondition, OrderPriceField},
};

/// Configuration for trailing stop orders.
///
/// Used both by [`RithmicOrder::trailing_stop`](crate::RithmicOrder::trailing_stop)
/// for a standalone order and by
/// [`RithmicOcoOrderLeg::trailing_stop`](crate::RithmicOcoOrderLeg::trailing_stop)
/// for a single leg of an OCO group.
///
/// # Example
///
/// ```
/// use rithmic_rs::TrailingStop;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let trailing = TrailingStop::new()
///     .trail_by_ticks(20)
///     .trail_by_price_id(1)
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a trailing stop does nothing until attached to an order"]
pub struct TrailingStop {
    /// Number of ticks to trail behind the market price
    pub trail_by_ticks: i32,
    /// Rithmic price-id to trail against. `build()` requires a non-zero id.
    pub trail_by_price_id: i32,
}

impl TrailingStop {
    /// Start an empty trailing stop.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            trail_by_ticks: 0,
            trail_by_price_id: 0,
        }
    }

    /// Number of ticks to trail behind the market price.
    pub fn trail_by_ticks(mut self, trail_by_ticks: i32) -> Self {
        self.trail_by_ticks = trail_by_ticks;
        self
    }

    /// Rithmic price-id to trail against.
    pub fn trail_by_price_id(mut self, trail_by_price_id: i32) -> Self {
        self.trail_by_price_id = trail_by_price_id;
        self
    }

    /// Requires both fields.
    pub fn build(self) -> Result<Self, RithmicError> {
        if self.trail_by_ticks < 1 {
            return Err(RithmicError::InvalidArgument(
                "trail_by_ticks must be at least 1".to_string(),
            ));
        }
        if self.trail_by_price_id < 1 {
            return Err(RithmicError::InvalidArgument(
                "trail_by_price_id must be at least 1".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Conditional trigger that releases an order once a price is touched.
///
/// Maps to the `if_touched_*` fields on `RequestNewOrder`,
/// `RequestBracketOrder` and `RequestModifyOrder`, which are field-identical.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderCondition, OrderPriceField, RithmicIfTouchedTrigger};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let trigger = RithmicIfTouchedTrigger::new()
///     .symbol("NQM6")
///     .exchange("CME")
///     .condition(OrderCondition::GreaterThanEqualTo)
///     .price_field(OrderPriceField::TradePrice)
///     .price(18250.5)
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a trigger does nothing until attached to an order"]
pub struct RithmicIfTouchedTrigger {
    /// Trading symbol to monitor for the condition.
    pub symbol: String,
    /// Exchange for the monitored symbol.
    pub exchange: String,
    /// Comparison operator for the trigger.
    pub condition: OrderCondition,
    /// Price field to evaluate.
    pub price_field: OrderPriceField,
    /// Threshold price for the condition. Left off the wire when unset.
    pub price: Option<f64>,
}

impl RithmicIfTouchedTrigger {
    /// Start an empty trigger.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            symbol: String::new(),
            exchange: String::new(),
            condition: OrderCondition::GreaterThanEqualTo,
            price_field: OrderPriceField::TradePrice,
            price: None,
        }
    }

    /// Trading symbol to monitor for the condition.
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    /// Exchange for the monitored symbol.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Comparison operator for the trigger.
    pub fn condition(mut self, condition: OrderCondition) -> Self {
        self.condition = condition;
        self
    }

    /// Price field to evaluate.
    pub fn price_field(mut self, price_field: OrderPriceField) -> Self {
        self.price_field = price_field;
        self
    }

    /// Threshold price for the condition.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Requires a symbol, an exchange and a price.
    pub fn build(self) -> Result<Self, RithmicError> {
        if self.symbol.is_empty() {
            return Err(RithmicError::InvalidArgument(
                "an if-touched trigger requires a symbol".to_string(),
            ));
        }

        if self.exchange.is_empty() {
            return Err(RithmicError::InvalidArgument(
                "an if-touched trigger requires an exchange".to_string(),
            ));
        }

        if self.price.is_none() {
            return Err(RithmicError::InvalidArgument(
                "an if-touched trigger requires a price; unset would otherwise \
                 release the order immediately under the default condition"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_stop_requires_both_fields() {
        assert!(TrailingStop::new().build().is_err());
        assert!(TrailingStop::new().trail_by_ticks(20).build().is_err());
        assert!(TrailingStop::new().trail_by_price_id(1).build().is_err());
        assert!(
            TrailingStop::new()
                .trail_by_ticks(20)
                .trail_by_price_id(1)
                .build()
                .is_ok()
        );
    }

    #[test]
    fn an_if_touched_trigger_requires_symbol_exchange_and_price() {
        let full = RithmicIfTouchedTrigger::new()
            .symbol("NQM6")
            .exchange("CME")
            .price(18250.5);
        assert!(full.clone().build().is_ok());

        assert!(full.clone().symbol("").build().is_err());
        assert!(full.exchange("").build().is_err());

        let err = RithmicIfTouchedTrigger::new()
            .symbol("NQM6")
            .exchange("CME")
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a price"), "{err}");
    }
}
