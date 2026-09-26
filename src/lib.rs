#![warn(missing_docs)]

//! # rithmic-rs
//!
//! `rithmic-rs` is a Rust client library for the Rithmic R | Protocol API.
//!
//! ## Features
//!
//! - Stream real-time market data (trades, quotes, order book depth)
//! - Submit and manage orders (bracket orders, modifications, cancellations)
//! - Access historical market data (ticks and time bars)
//! - Manage risk and track positions and P&L
//! - Connection health monitoring with heartbeat and forced logout handling
//!
//! ## Quick Start
//!
//! ```no_run
//! use rithmic_rs::{
//!     RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant,
//!     rti::messages::RithmicMessage,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Load configuration from environment variables
//!     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!
//!     // Connect with Retry strategy (recommended default)
//!     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
//!     let mut handle = ticker_plant.get_handle();
//!
//!     // Login and subscribe to market data
//!     handle.login().await?;
//!     handle.subscribe("ESM6", "CME").await?;
//!
//!     // Process real-time updates
//!     loop {
//!         match handle.subscription_receiver.recv().await {
//!             Ok(update) => {
//!                 // Check for connection health issues
//!                 if let Some(err) = &update.error {
//!                     eprintln!("Error: {}", err);
//!                     if err.is_connection_issue() { break; }
//!                     continue;
//!                 }
//!
//!                 // Process market data
//!                 match update.message {
//!                     RithmicMessage::LastTrade(trade) => {
//!                         println!("Trade: {:?}", trade);
//!                     }
//!                     RithmicMessage::BestBidOffer(bbo) => {
//!                         println!("BBO: {:?}", bbo);
//!                     }
//!                     _ => {}
//!                 }
//!             }
//!             Err(e) => {
//!                 eprintln!("Channel error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Connection Strategies
//!
//! The library provides three connection strategies:
//!
//! - [`ConnectStrategy::Simple`]: Single connection attempt, fast-fail
//! - [`ConnectStrategy::Retry`]: Indefinite retries with linear backoff — 500 ms more per attempt, capped at 60s, jittered ±50% (recommended default)
//! - [`ConnectStrategy::AlternateWithRetry`]: Alternates between primary and beta URLs
//!
//! A graceful `disconnect().await` logs out first and then closes the WebSocket.
//! See [Error Handling](#error-handling) for how that differs from an
//! unexpected drop.
//!
//! ## Configuration
//!
//! Use [`RithmicConfig`] for modern, ergonomic configuration:
//!
//! ```no_run
//! use rithmic_rs::{RithmicAccount, RithmicConfig, RithmicEnv};
//!
//! fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // From environment variables
//!     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!     let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
//!
//!     // Or using builder pattern
//!     let config = RithmicConfig::builder(RithmicEnv::Demo)
//!         .user("your_user".to_string())
//!         .password("your_password".to_string())
//!         .system_name("Rithmic Paper Trading".to_string())
//!         .app_name("your_app_name".to_string())
//!         .app_version("1".to_string())
//!         .build()?;
//!
//!     let account = RithmicAccount::new("your_fcm", "your_ib", "your_account");
//!     let _ = (config, account);
//!     Ok(())
//! }
//! ```
//!
//! ## Error Handling
//!
//! An error reaches you in one of two places: the call you made, or the
//! subscription channel. Which one it is tells you what to do about it.
//!
//! `examples/error_handling.rs` in the repository is this section as one
//! runnable file, if you would rather read code.
//!
//! ### From a call
//!
//! Handle methods return [`Result<_, RithmicError>`], but `Ok` does not mean
//! success. A request the server turned down still comes back as `Ok`, with the
//! reason in `resp.error`. Code that checks only for `Err` will read it as
//! having worked. `login` is the exception — a rejected login is an `Err`.
//!
//! ```ignore
//! use rithmic_rs::RithmicError;
//!
//! match handle.subscribe("ESM6", "CME").await {
//!     Ok(resp) => match &resp.error {
//!         Some(err) => eprintln!("Server rejected: {err}"),
//!         None => { /* success */ }
//!     },
//!     Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
//!         handle.abort();
//!         // reconnect — see examples/reconnect.rs
//!     }
//!     Err(e) => eprintln!("{e}"),
//! }
//!
//! if let Err(RithmicError::RequestRejected(err)) = handle.login().await {
//!     eprintln!(
//!         "Login rejected: code={} msg={}",
//!         err.code.as_deref().unwrap_or("?"),
//!         err.message.as_deref().unwrap_or(""),
//!     );
//! }
//! ```
//!
//! Two errors turn up in `resp.error`.
//! [`RequestRejected`](RithmicError::RequestRejected) is the server saying no,
//! with its code and message split out so you can branch on the code.
//! [`ProtocolError`](RithmicError::ProtocolError) means the response arrived but
//! would not decode — usually Rithmic's schema has moved ahead of this crate, so
//! retrying will not help and it is worth filing.
//!
//! An `Err` means you never got an answer at all:
//!
//! - [`InvalidArgument`](RithmicError::InvalidArgument) — your arguments.
//!   Nothing was sent. Fix them and call again.
//! - [`NoTradeRoute`](RithmicError::NoTradeRoute) — no route for the order's
//!   exchange. Nothing was sent. Set the order's `trade_route`, or check the
//!   exchange with `trade_route_for` before you trade.
//! - [`SendFailed`](RithmicError::SendFailed) — the send failed. Only this
//!   request fails and the plant is still up, but the connection is usually on
//!   its way out; expect a `ConnectionError` to follow. Treat it as a
//!   connection problem rather than retrying in a loop.
//! - [`ConnectionClosed`](RithmicError::ConnectionClosed) — the plant is gone.
//!   Reconnect; calling again will not work.
//!
//! When a connection drops, everything in flight fails with `ConnectionClosed`
//! whatever the real cause was. The cause goes out on the subscription channel,
//! so look there if you need to tell a heartbeat timeout from a dead socket.
//!
//! The crate does not time out requests. A request finishes when Rithmic
//! answers it or the connection drops. If you need timeout handling, see
//! `examples/request_timeout.rs`. Timing out on your side does not cancel
//! anything on Rithmic's, so reconcile an order rather than re-sending it.
//!
//! ([`ConnectionFailed`](RithmicError::ConnectionFailed) comes from `connect()`
//! rather than a handle method, and only under [`ConnectStrategy::Simple`] —
//! the retrying strategies keep trying instead of handing you an error.
//! [`EmptyResponse`](RithmicError::EmptyResponse) is a defensive case you should
//! not see.)
//!
//! ### From the subscription channel
//!
//! Updates normally arrive with `error: None`. Five messages want a decision
//! from you:
//!
//! | Message | What to do |
//! |---|---|
//! | `ConnectionError` | Reconnect. The plant is stopping or already stopped. |
//! | `HeartbeatTimeout` | Reconnect — unless `error` holds a `RequestRejected`, which means the server rejected a heartbeat and the connection is fine. |
//! | `ForcedLogout` | The server ended your session. A `ConnectionError` follows, so expect two events. |
//! | `UnknownTemplate` | Nothing, unless you want to. A template this crate has no mapping for, raw payload attached. Not an error. |
//! | `Unknown` | A frame that would not decode. Log it and carry on. |
//!
//! [`RithmicError::is_connection_issue`] is the shortcut: true means reconnect,
//! false means the connection is fine and something about the data or the
//! request was not. Do not reconnect on `ProtocolError` or `RequestRejected` —
//! neither says anything about connection health, and you will only churn.
//!
//! This is a broadcast channel, so anything sent while you hold no receiver is
//! gone. Keep it for as long as the plant lives.
//!
//! ### When a plant stops
//!
//! Only transport failure takes one down: a broken socket, a keep-alive
//! timeout, a forced logout, or your own `abort()`. You get the matching
//! connection-health event and every pending call fails with `ConnectionClosed`.
//!
//! Bad data never does. An undecodable frame, an unmapped template, a rejected
//! request — the plant keeps running and your other in-flight requests are
//! untouched. A decode failure usually comes back from the call it belongs to,
//! and arrives as `Unknown` when the frame names no request. A frame too
//! damaged to carry a template id at all is logged and dropped.
//!
//! `disconnect().await` is the clean shutdown and emits none of those events.
//! Pending calls still fail with `ConnectionClosed`.
//!
//! `RithmicError` implements [`std::error::Error`], so `?` works in functions
//! returning `Box<dyn Error>`.
//!
//! ## Feature Flags
//!
//! | Flag | Default | Description |
//! |------|---------|-------------|
//! | `serde` | off | Adds `Serialize`/`Deserialize` derives on the config types (`RithmicEnv`, `RithmicAccount`), the trading enums (`OrderSide`, `OrderType`, `TimeInForce`, `ManualOrAutoEntry`, `OrderCondition`, `OrderPriceField`, `BracketType`, `BracketOperationType`, `FillHistoryRange`, `EasyToBorrowRequest`, `RmsUpdateBits`, `OrderStatus`), every order command type (`RithmicOrder`, `RithmicBracketOrder`, `RithmicOcoOrder` and its legs, `RithmicModifyOrder`, the cancel/exit/link/retag/adjustment commands), the triggers (`TrailingStop`, `RithmicIfTouchedTrigger`) and the history request types (`VolumeProfileMinuteBarsRequest`, `TickBarReplayRequest`) |
//!
//! **TLS backend:** The crate uses `native-tls` (via `tokio-tungstenite`) for all
//! WebSocket connections. There is currently no `rustls` option.
//!
//! ## Module Organization
//!
//! - [`plants`]: Specialized clients for different data types (ticker, order, P&L, history)
//! - [`config`]: Configuration API for connecting to Rithmic
//! - [`error`]: Typed error enum for plant handle methods
//! - [`api`]: Low-level API interfaces for sending and receiving messages
//! - [`types`]: High-level trading enums (order side, type, time-in-force, …)
//! - [`rti`]: Protocol message definitions
//! - [`util`]: Utility types and helpers (timestamps, order status, instrument info)

pub mod api;

/// Configuration API for connecting to Rithmic
pub mod config;

/// Error types for plant handle methods.
pub mod error;

mod ping_manager;

/// Specialized clients ("plants") for different Rithmic services.
///
/// Each plant connects to a specific Rithmic infrastructure component:
///
/// - [`ticker_plant`](plants::ticker_plant): Real-time market data (trades, quotes, order book)
/// - [`order_plant`](plants::order_plant): Order entry and management
/// - [`history_plant`](plants::history_plant): Historical tick and bar data
/// - [`pnl_plant`](plants::pnl_plant): Position and P&L tracking
///
/// Plants run as independent async tasks using the actor pattern, communicating
/// via tokio channels. This allows running multiple plants concurrently and
/// reconnecting them independently.
pub mod plants;

mod request_handler;

/// Rithmic protocol message definitions (protobuf-generated).
///
/// This module contains the protocol buffer message types used by the Rithmic API.
/// The main type you'll interact with is [`rti::messages::RithmicMessage`], an enum
/// covering all message types including market data, order notifications, and
/// connection health events.
#[allow(missing_docs)]
pub mod rti;

/// High-level trading types with optional serde support.
pub mod types;

/// Utility types for working with Rithmic data.
pub mod util;

mod ws;

/// The `prost` these types are generated against, so downstream decoding uses
/// the same version.
pub use prost;

// Re-export plant types for easier access
pub use plants::history_plant::{RithmicHistoryPlant, RithmicHistoryPlantHandle};
pub use plants::order_plant::{RithmicOrderPlant, RithmicOrderPlantHandle};
pub use plants::pnl_plant::{RithmicPnlPlant, RithmicPnlPlantHandle};
pub use plants::subscription::SubscriptionFilter;
pub use plants::ticker_plant::{RithmicTickerPlant, RithmicTickerPlantHandle};

// Re-export modern configuration types for convenience
pub use config::{ConfigError, RithmicAccount, RithmicConfig, RithmicConfigBuilder, RithmicEnv};
#[allow(deprecated)]
pub use request_handler::DEFAULT_REQUEST_TIMEOUT;

// Re-export error types
pub use error::{RithmicError, RithmicRequestError};

// Re-export connection strategy
pub use ws::ConnectStrategy;

// Re-export API types
pub use api::{
    LoginConfig, RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders,
    RithmicCancelOrder, RithmicExitPosition, RithmicIfTouchedTrigger, RithmicLinkOrders,
    RithmicModifyOrder, RithmicModifyOrderReferenceData, RithmicOcoOrder, RithmicOcoOrderLeg,
    RithmicOrder, RithmicResponse, TrailingStop,
};

// Re-export utility types for convenience
pub use util::{
    InstrumentInfo, InstrumentInfoError, OrderStatus, UnknownTemplateMessage,
    rithmic_to_unix_nanos, rithmic_to_unix_nanos_precise,
};

// Re-export high-level trading types
pub use types::{
    BracketOperationType, BracketType, EasyToBorrowRequest, FillHistoryRange, ManualOrAutoEntry,
    OrderCondition, OrderPriceField, OrderSide, OrderType, ParseOrderSideError,
    ParseOrderTypeError, ParseTimeInForceError, RmsUpdateBits, TickBarReplayRequest,
    TimeBarReplayRequest, TimeBarType, TimeInForce, VolumeProfileMinuteBarsRequest,
};
