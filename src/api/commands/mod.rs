//! The order commands, one module per command type.
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

pub(crate) mod bracket;
pub(crate) mod cancel;
pub(crate) mod exit;
pub(crate) mod modify;
pub(crate) mod oco;
pub(crate) mod order;
pub(crate) mod trailing;

pub use bracket::{RithmicBracketLevelAdjustment, RithmicBracketOrder};
pub use cancel::{RithmicCancelAllOrders, RithmicCancelOrder};
pub use exit::{RithmicExitPosition, RithmicLinkOrders};
pub use modify::{RithmicModifyOrder, RithmicModifyOrderReferenceData};
pub use oco::{RithmicOcoOrder, RithmicOcoOrderLeg};
pub use order::RithmicOrder;
pub use trailing::{RithmicIfTouchedTrigger, TrailingStop};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OrderOrigin;

    /// Every order command type defaults its origination to `Auto`, which is
    /// only true because [`OrderOrigin`] is crate-owned — the generated
    /// `OrderPlacement` enums have no `Default` at all.
    #[test]
    fn order_command_types_default_to_auto_placement() {
        assert_eq!(RithmicOrder::default().manual_or_auto, OrderOrigin::Auto);
        assert_eq!(
            RithmicOcoOrderLeg::default().manual_or_auto,
            OrderOrigin::Auto
        );
        assert_eq!(
            RithmicBracketOrder::default().manual_or_auto,
            OrderOrigin::Auto
        );
        assert_eq!(
            RithmicModifyOrder::default().manual_or_auto,
            OrderOrigin::Auto
        );
        assert_eq!(
            RithmicCancelOrder::default().manual_or_auto,
            OrderOrigin::Auto
        );
        assert_eq!(
            RithmicCancelAllOrders::default().manual_or_auto,
            OrderOrigin::Auto
        );
        assert_eq!(
            RithmicExitPosition::default().manual_or_auto,
            OrderOrigin::Auto
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
                .price_type(crate::types::OrderType::Limit)
                .build()
                .is_ok()
        );
    }
}
