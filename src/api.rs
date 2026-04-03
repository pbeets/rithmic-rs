//! Low-level API types for Rithmic communication.
//!
//! This module provides command types for order operations and the response wrapper
//! used by all plant modules. Most users will interact with these types through
//! the high-level plant APIs rather than directly.
//!
//! # Order Types
//!
//! - [`RithmicBracketOrder`]: Entry order with profit target and stop loss
//! - [`RithmicOcoOrderLeg`]: One leg of a One-Cancels-Other order pair
//! - [`RithmicModifyOrder`]: Modify an existing order's price/quantity
//! - [`RithmicCancelOrder`]: Cancel an order by ID
//!
//! # Response Type
//!
//! - [`RithmicResponse`]: Wrapper for all messages from Rithmic plants

pub(crate) mod receiver_api;
pub(crate) mod rithmic_command_types;
pub(crate) mod sender_api;

// Re-export commonly used types
pub use receiver_api::RithmicResponse;

pub use rithmic_command_types::{
    LoginConfig, RithmicAccount, RithmicAdvancedBracketOrder, RithmicBracketOrder,
    RithmicCancelOrder, RithmicIfTouchedTrigger, RithmicModifyOrder, RithmicOcoOrderLeg,
    RithmicOrder, TrailingStop,
};

// Re-export bracket order enums
pub use crate::rti::request_bracket_order::{
    BracketType, Condition as BracketCondition, Duration as BracketDuration,
    PriceField as BracketPriceField, PriceType as BracketPriceType,
    TransactionType as BracketTransactionType,
};

// Re-export OCO order enums
pub use crate::rti::request_oco_order::{
    Duration as OcoDuration, PriceType as OcoPriceType, TransactionType as OcoTransactionType,
};

// Re-export new order enums for RithmicOrder fields
pub use crate::rti::request_new_order::{
    Duration as NewOrderDuration, PriceType as NewOrderPriceType,
    TransactionType as NewOrderTransactionType,
};

// Re-export modify order enums
pub use crate::rti::request_modify_order::PriceType as ModifyPriceType;

// Re-export easy-to-borrow list request type
pub use crate::rti::request_easy_to_borrow_list::Request as EasyToBorrowRequest;
