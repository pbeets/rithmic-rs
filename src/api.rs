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
pub(crate) mod response;
pub(crate) mod rithmic_command_types;
pub(crate) mod rp_code;
pub(crate) mod sender_api;

// Re-export commonly used types
pub use crate::config::RithmicAccount;
pub use receiver_api::RithmicResponse;

pub use rithmic_command_types::{
    LoginConfig, RithmicAdvancedBracketOrder, RithmicBracketLevelAdjustment, RithmicBracketOrder,
    RithmicCancelOrder, RithmicIfTouchedTrigger, RithmicModifyOrder, RithmicOcoOrderLeg,
    RithmicOrder, RithmicOrderIfTouchedTrigger, TrailingStop,
};

// Re-export bracket order enums
pub use crate::rti::request_bracket_order::{
    BracketType, Condition as BracketCondition, Duration as BracketDuration,
    OrderPlacement as BracketOrderPlacement, PriceField as BracketPriceField,
    PriceType as BracketPriceType, TransactionType as BracketTransactionType,
};

// Re-export OCO order enums
pub use crate::rti::request_oco_order::{
    Duration as OcoDuration, OrderPlacement as OcoOrderPlacement, PriceType as OcoPriceType,
    TransactionType as OcoTransactionType,
};

// Re-export new order enums for RithmicOrder fields
pub use crate::rti::request_new_order::{
    Condition as NewOrderCondition, Duration as NewOrderDuration,
    OrderPlacement as NewOrderPlacement, PriceField as NewOrderPriceField,
    PriceType as NewOrderPriceType, TransactionType as NewOrderTransactionType,
};

// Re-export modify order enums
pub use crate::rti::request_modify_order::{
    OrderPlacement as ModifyOrderPlacement, PriceType as ModifyPriceType,
};

// Re-export cancel order origination selectors
pub use crate::rti::request_cancel_all_orders::OrderPlacement as CancelAllOrderPlacement;
pub use crate::rti::request_cancel_order::OrderPlacement as CancelOrderPlacement;

// Re-export exit position origination selector
pub use crate::rti::request_exit_position::OrderPlacement as ExitPositionPlacement;

// Re-export easy-to-borrow list request type
pub use crate::rti::request_easy_to_borrow_list::Request as EasyToBorrowRequest;

// Re-export account RMS update selector
pub use crate::rti::request_account_rms_updates::UpdateBits as RmsUpdateBits;
