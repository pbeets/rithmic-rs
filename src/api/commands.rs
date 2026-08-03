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

pub use bracket::{RithmicBracketLevelAdjustment, RithmicBracketOrder};
pub use cancel::{RithmicCancelAllOrders, RithmicCancelOrder};
pub use exit::RithmicExitPosition;
pub use link::RithmicLinkOrders;
pub use modify::{RithmicModifyOrder, RithmicModifyOrderReferenceData};
pub use oco::{RithmicOcoOrder, RithmicOcoOrderLeg};
pub use order::RithmicOrder;
pub use triggers::{RithmicIfTouchedTrigger, TrailingStop};
