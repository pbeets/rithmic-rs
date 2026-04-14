//! # Rithmic plants
//!
//! Rithmic divides its API into several services called "plants", each handling
//! a specific aspect of trading functionality.
//!
//! This module contains implementations for each Rithmic API plant:
//!
//! - **TickerPlant**: Realtime market data subscription (price quotes, trades, etc.)
//! - **OrderPlant**: Order placement and management
//! - **PnlPlant**: Position and profit/loss tracking
//! - **HistoryPlant**: Historical data retrieval

pub(crate) mod core;
/// Access to historical market data
pub mod history_plant;
/// Order entry and management
pub mod order_plant;
/// Position and P&L tracking
pub mod pnl_plant;
/// Account-scoped subscription helpers for shared order/PnL plants
pub mod subscription;
/// Real-time market data subscription
pub mod ticker_plant;
