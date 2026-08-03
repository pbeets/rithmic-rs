//! The trigger conditions an order embeds: trailing stops and if-touched
//! triggers.
//!
//! Neither takes chained setters: their `new()` is positional and every field
//! is required.

use crate::types::{OrderCondition, OrderPriceField};

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
///
/// let trailing = TrailingStop::new(20, 1);
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TrailingStop {
    /// Number of ticks to trail behind the market price
    pub trail_by_ticks: i32,
    /// Rithmic price-id to trail against. Rithmic rejects a trailing stop with
    /// rp_code 1112 when this is unset.
    pub trail_by_price_id: i32,
}

impl TrailingStop {
    /// Build a trailing stop from its two required fields.
    pub fn new(trail_by_ticks: i32, trail_by_price_id: i32) -> Self {
        Self {
            trail_by_ticks,
            trail_by_price_id,
        }
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
///
/// let trigger = RithmicIfTouchedTrigger::new(
///     "NQM6",
///     "CME",
///     OrderCondition::GreaterThanEqualTo,
///     OrderPriceField::TradePrice,
///     18250.5,
/// );
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RithmicIfTouchedTrigger {
    /// Trading symbol to monitor for the condition.
    pub symbol: String,
    /// Exchange for the monitored symbol.
    pub exchange: String,
    /// Comparison operator for the trigger.
    pub condition: OrderCondition,
    /// Price field to evaluate.
    pub price_field: OrderPriceField,
    /// Threshold price for the condition.
    pub price: f64,
}

impl RithmicIfTouchedTrigger {
    /// Build a trigger. Every field is required, so there is no builder.
    pub fn new(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        condition: OrderCondition,
        price_field: OrderPriceField,
        price: f64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            exchange: exchange.into(),
            condition,
            price_field,
            price,
        }
    }
}
