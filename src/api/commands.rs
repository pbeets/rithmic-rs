//! The order commands, one module per command type.
//!
//! Every command is built the same way: `T::new()` starts from the command's
//! defaults, a chained setter covers each field, and `build()` hands the command
//! back. Where the command type has a `validate()`, `build()` runs it first.
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

pub(crate) mod bracket;
pub(crate) mod cancel;
pub(crate) mod exit;
pub(crate) mod link;
pub(crate) mod modify;
pub(crate) mod oco;
pub(crate) mod order;
pub(crate) mod triggers;

use crate::{error::RithmicError, types::OrderType};

pub use bracket::{RithmicBracketLevelAdjustment, RithmicBracketOrder};
pub use cancel::{RithmicCancelAllOrders, RithmicCancelOrder};
pub use exit::RithmicExitPosition;
pub use link::RithmicLinkOrders;
pub use modify::{RithmicModifyOrder, RithmicModifyOrderReferenceData};
pub use oco::{RithmicOcoOrder, RithmicOcoOrderLeg};
pub use order::RithmicOrder;
pub use triggers::{RithmicIfTouchedTrigger, TrailingStop};

pub(crate) fn validate_instrument(
    symbol: &str,
    exchange: &str,
    quantity: i32,
) -> Result<(), RithmicError> {
    if symbol.is_empty() {
        return Err(RithmicError::InvalidArgument(
            "a symbol is required".to_string(),
        ));
    }

    if exchange.is_empty() {
        return Err(RithmicError::InvalidArgument(
            "an exchange is required".to_string(),
        ));
    }

    if quantity < 1 {
        return Err(RithmicError::InvalidArgument(format!(
            "quantity must be at least 1, got {quantity}"
        )));
    }
    Ok(())
}

/// Which prices an order of this type must carry: `Limit`, `StopLimit` and
/// `LimitIfTouched` need a price; `StopMarket`, `StopLimit`, `MarketIfTouched`
/// and `LimitIfTouched` need a trigger price. `Market` needs neither.
pub(crate) fn price_requirements(price_type: OrderType) -> (bool, bool) {
    match price_type {
        OrderType::Market => (false, false),
        OrderType::Limit => (true, false),
        OrderType::StopMarket | OrderType::MarketIfTouched => (false, true),
        OrderType::StopLimit | OrderType::LimitIfTouched => (true, true),
    }
}

/// Check `price` and `trigger_price` against [`price_requirements`], naming the
/// missing field in the error.
pub(crate) fn require_prices(
    price_type: OrderType,
    price: Option<f64>,
    trigger_price: Option<f64>,
) -> Result<(), RithmicError> {
    let (needs_price, needs_trigger) = price_requirements(price_type);
    let order_type = price_type.as_str_name();

    if needs_price && price.is_none() {
        return Err(RithmicError::InvalidArgument(format!(
            "price is required for a {order_type} order"
        )));
    }

    if needs_trigger && trigger_price.is_none() {
        return Err(RithmicError::InvalidArgument(format!(
            "trigger_price is required for a {order_type} order"
        )));
    }

    Ok(())
}
