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
pub use receiver_api::RithmicResponse;

pub use commands::{
    RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders, RithmicCancelOrder,
    RithmicExitPosition, RithmicIfTouchedTrigger, RithmicLinkOrders, RithmicModifyOrder,
    RithmicModifyOrderReferenceData, RithmicOcoOrder, RithmicOcoOrderLeg, RithmicOrder,
    TrailingStop,
};

// Re-export the crate-owned order enums so `api::ManualOrAutoEntry` also resolves
pub use crate::types::{
    BracketType, ManualOrAutoEntry, OrderCondition, OrderPriceField, OrderSide, OrderType,
    TimeInForce,
};

// Re-export easy-to-borrow list request type
pub use crate::rti::request_easy_to_borrow_list::Request as EasyToBorrowRequest;

// Re-export account RMS update selector
pub use crate::rti::request_account_rms_updates::UpdateBits as RmsUpdateBits;
