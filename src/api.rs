//! Low-level API types for Rithmic communication.
//!
//! The order command types, and [`RithmicResponse`], the wrapper every message
//! from a plant arrives in. Most callers reach these through the plant handles
//! rather than directly.

pub(crate) mod commands;
pub(crate) mod receiver_api;
pub(crate) mod response;
pub(crate) mod rp_code;
pub(crate) mod sender_api;

// Re-export commonly used types
pub use crate::config::{LoginConfig, RithmicAccount};
pub use response::RithmicResponse;

pub use commands::{
    RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders, RithmicCancelOrder,
    RithmicExitPosition, RithmicIfTouchedTrigger, RithmicLinkOrders, RithmicModifyOrder,
    RithmicModifyOrderReferenceData, RithmicOcoOrder, RithmicOcoOrderLeg, RithmicOrder,
    TrailingStop,
};

// Re-export the crate-owned order enums so `api::ManualOrAutoEntry` also resolves
pub use crate::types::{
    BracketOperationType, BracketType, EasyToBorrowRequest, FillHistoryRange, ManualOrAutoEntry,
    OrderCondition, OrderPriceField, OrderSide, OrderType, RmsUpdateBits, TimeInForce,
};
