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

use tokio::sync::oneshot;

use crate::{api::receiver_api::RithmicResponse, error::RithmicError};

pub(crate) mod core;
/// Access to historical market data
pub mod history_plant;
/// Order entry and management
pub mod order_plant;
/// Position and P&L tracking
pub mod pnl_plant;
/// Account-scoped subscription helpers for shared order/PnL plants
pub mod subscription;
#[cfg(test)]
pub(crate) mod test_support;
/// Real-time market data subscription
pub mod ticker_plant;
pub(crate) mod trade_routes;

/// Await a plant actor's reply and return the first (usually only) response.
///
/// A dropped responder means the actor stopped before answering, which handles
/// surface as [`RithmicError::ConnectionClosed`].
pub(crate) async fn await_first_response(
    rx: oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>>,
) -> Result<RithmicResponse, RithmicError> {
    await_all_responses(rx)
        .await?
        .into_iter()
        .next()
        .ok_or(RithmicError::EmptyResponse)
}

/// Await a plant actor's reply and return every accumulated response.
pub(crate) async fn await_all_responses(
    rx: oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>>,
) -> Result<Vec<RithmicResponse>, RithmicError> {
    rx.await.map_err(|_| RithmicError::ConnectionClosed)?
}
