//! Low-level API types for Rithmic communication.
//!
//! This module provides command types for order operations and the response wrapper
//! used by all plant modules. Most users will interact with these types through
//! the high-level plant APIs rather than directly.
//!
//! # Order Types
//!
//! - [`RithmicOrder`]: A standalone order
//! - [`RithmicBracketOrder`]: Entry order with profit target and stop loss
//! - [`RithmicOcoOrder`]: A group of One-Cancels-Other legs
//! - [`RithmicModifyOrder`]: Modify an existing order's price/quantity
//! - [`RithmicCancelOrder`]: Cancel an order by ID
//!
//! # Response Type
//!
//! - [`RithmicResponse`]: Wrapper for all messages from Rithmic plants

pub(crate) mod command_builders;
pub(crate) mod receiver_api;
pub(crate) mod response;
pub(crate) mod rithmic_command_types;
pub(crate) mod rp_code;
pub(crate) mod sender_api;

// Re-export commonly used types
pub use crate::config::RithmicAccount;
pub use receiver_api::RithmicResponse;

pub use rithmic_command_types::{
    LoginConfig, RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders,
    RithmicCancelOrder, RithmicExitPosition, RithmicIfTouchedTrigger, RithmicLinkOrders,
    RithmicModifyOrder, RithmicModifyOrderReferenceData, RithmicOcoOrder, RithmicOcoOrderLeg,
    RithmicOrder, TrailingStop,
};

// Re-export the crate-owned order enums so `api::OrderPlacement` also resolves
pub use crate::types::{
    BracketType, OrderCondition, OrderPlacement, OrderPriceField, OrderSide, OrderType, TimeInForce,
};

// Re-export easy-to-borrow list request type
pub use crate::rti::request_easy_to_borrow_list::Request as EasyToBorrowRequest;

// Re-export account RMS update selector
pub use crate::rti::request_account_rms_updates::UpdateBits as RmsUpdateBits;
