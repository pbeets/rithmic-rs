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
//! - [`ConnectStrategy::Retry`]: Indefinite retries with exponential backoff capped at 60s (recommended default)
//! - [`ConnectStrategy::AlternateWithRetry`]: Alternates between primary and beta URLs
//!
//! A graceful `disconnect().await` logs out first and then closes the WebSocket.
//! On a healthy connection, that shutdown path does not emit synthetic
//! `HeartbeatTimeout` or `ConnectionError` subscription updates; reserve those
//! for unexpected connection-health failures and reconnect logic.
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
//! All plant handle methods return [`Result<_, RithmicError>`]. The [`RithmicError`] enum
//! lets you programmatically distinguish error kinds:
//!
//! ```ignore
//! use rithmic_rs::RithmicError;
//!
//! match handle.subscribe("ESM6", "CME").await {
//!     Ok(resp) => { /* success */ }
//!     Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
//!         handle.abort();
//!         // reconnect — see examples/reconnect.rs
//!     }
//!     Err(RithmicError::RequestRejected(err)) => {
//!         eprintln!(
//!             "Server rejected: code={} msg={}",
//!             err.code.as_deref().unwrap_or("?"),
//!             err.message.as_deref().unwrap_or(""),
//!         );
//!     }
//!     Err(RithmicError::ProtocolError(msg)) => {
//!         eprintln!("Protocol error: {msg}");
//!     }
//!     Err(e) => eprintln!("{e}"),
//! }
//! ```
//!
//! For inspecting a `RithmicResponse` directly, match on `response.error` — it
//! is `Option<RithmicError>` with `RequestRejected` for rp_code rejections and
//! `ProtocolError` for other non-transport failures. Use
//! [`RithmicError::is_connection_issue`] to distinguish transport-level events
//! that warrant reconnection. The raw rp_code payload is available via
//! `response.rp_code()`, `response.rp_code_num()`, and `response.rp_code_text()`.
//!
//! A graceful `disconnect().await` is separate from that reconnect path: it
//! shuts the plant down without sending synthetic `HeartbeatTimeout` or
//! `ConnectionError` updates to subscribers when the close handshake succeeds.
//!
//! `RithmicError` implements [`std::error::Error`], so `?` works in functions
//! returning `Box<dyn Error>`.
//!
//! ## Feature Flags
//!
//! | Flag | Default | Description |
//! |------|---------|-------------|
//! | `serde` | off | Adds `Serialize`/`Deserialize` derives on trading types (`RithmicEnv`, `OrderSide`, `OrderType`, `TimeInForce`, `OrderStatus`, `RithmicOrder`, `TrailingStop`) |
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
//! - [`rti`]: Protocol message definitions
//! - [`ws`]: WebSocket connectivity and connection strategies
//! - [`util`]: Utility types and helpers (timestamps, order status, instrument info)

/// Low-level API types for Rithmic communication.
///
/// This module provides the command types and response structures used internally
/// by the plant modules. Most users should use the high-level plant APIs instead.
///
/// Re-exports include order types ([`RithmicBracketOrder`], [`RithmicModifyOrder`], etc.)
/// and their associated enums for transaction types, durations, and price types.
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

/// WebSocket connectivity layer
pub mod ws;

// Re-export plant types for easier access
pub use plants::history_plant::{RithmicHistoryPlant, RithmicHistoryPlantHandle};
pub use plants::order_plant::{RithmicOrderPlant, RithmicOrderPlantHandle};
pub use plants::pnl_plant::{RithmicPnlPlant, RithmicPnlPlantHandle};
pub use plants::ticker_plant::{RithmicTickerPlant, RithmicTickerPlantHandle};

// Re-export modern configuration types for convenience
pub use config::{ConfigError, RithmicAccount, RithmicConfig, RithmicConfigBuilder, RithmicEnv};

// Re-export error types
pub use error::{RithmicError, RithmicRequestError};

// Re-export connection strategy
pub use ws::ConnectStrategy;

// Re-export API types
pub use api::{
    BracketCondition, BracketDuration, BracketPriceField, BracketPriceType, BracketTransactionType,
    BracketType, EasyToBorrowRequest, LoginConfig, ModifyPriceType, NewOrderDuration,
    NewOrderPriceType, NewOrderTransactionType, OcoDuration, OcoPriceType, OcoTransactionType,
    RithmicAdvancedBracketOrder, RithmicBracketOrder, RithmicCancelOrder, RithmicIfTouchedTrigger,
    RithmicModifyOrder, RithmicOcoOrderLeg, RithmicOrder, RithmicResponse, TrailingStop,
};

// Re-export utility types for convenience
pub use util::{
    InstrumentInfo, InstrumentInfoError, OrderStatus, rithmic_to_unix_nanos,
    rithmic_to_unix_nanos_precise,
};

// Re-export high-level trading types
pub use types::{
    OrderSide, OrderType, ParseOrderSideError, ParseOrderTypeError, ParseTimeInForceError,
    TimeInForce,
};
