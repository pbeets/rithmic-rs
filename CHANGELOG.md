# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking Changes (Unreleased pre-2.0.0 API churn)
- **`RithmicResponse::rp_code_error` field removed.** Use `response.error`
  directly, or the new rp_code accessors
  (`rp_code()`, `rp_code_num()`, `rp_code_text()`) for the raw payload.
- **`RithmicRequestError` shape changed.** `code: String` → `code: Option<String>`;
  `message: String` → `message: Option<String>` (symmetric with `code`;
  single-element rp_codes like `["5"]` now produce `message = None`);
  new `rp_code: Vec<String>` field preserves the full raw payload; struct is
  now `#[non_exhaustive]`. Accesses via `err.code` / `err.message` must update
  to `err.code.as_deref().unwrap_or("?")` and
  `err.message.as_deref().unwrap_or("")`.
- **`buf_to_message` no longer returns `Err(RithmicResponse)` for rp_code
  rejections.** Protocol-level outcomes now always come out as `Ok(response)`
  with `response.error` populated.
  `Err(RithmicResponse)` now exclusively means decode failure.
- **`RithmicError::ServerError(String)` removed.** Replaced by two variants
  that preserve the server/transport distinction. `RithmicError` now derives
  `PartialEq`, and `source()` returns the inner `RithmicRequestError` for
  `RequestRejected`.

#### Migrating from `RithmicError::ServerError`
Before (≤ 1.x):
```rust
match handle.subscribe("ESH6", "CME").await {
    Ok(_) => { /* ... */ }
    Err(RithmicError::ServerError(msg)) => {
        eprintln!("server error: {msg}");
        // unclear whether this is a rejection or a decode failure —
        // callers often used the message text to guess
    }
    Err(e) => eprintln!("{e}"),
}
```

After:
```rust
match handle.subscribe("ESH6", "CME").await {
    Ok(_) => { /* ... */ }
    Err(RithmicError::RequestRejected(err)) => {
        // Structured rp_code rejection — do NOT reconnect.
        eprintln!(
            "rejected code={} msg={}",
            err.code.as_deref().unwrap_or("?"),
            err.message.as_deref().unwrap_or(""),
        );
    }
    Err(RithmicError::ProtocolError(msg)) => {
        // Decode or non-rp_code protocol failure — do NOT reconnect.
        eprintln!("protocol error: {msg}");
    }
    Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
        // Transport failure — reconnect.
    }
    Err(e) => eprintln!("{e}"),
}
```

### Added
- **`RithmicError::ProtocolError(String)`** — non-transport failures that don't
  carry `rp_code` (decode errors, missing response).
- **`RithmicResponse::rp_code() -> Option<&[String]>`** — raw payload slice.
- **`RithmicResponse::rp_code_num() -> Option<&str>`** — numeric code (first element).
- **`RithmicResponse::rp_code_text() -> Option<&str>`** — human message (second element).
- **Internal `rp_code_response_variants!` macro** enumerating every
  `RithmicMessage` variant whose inner proto carries `rp_code`. Keep in sync
  when new `Response*` templates are added.

### Changed
- **Plant login helpers simplified** — all four plants check `response.error` directly.
- **Ping/heartbeat SEND transport failures** broadcast as
  `RithmicMessage::HeartbeatTimeout` (same signal as a true heartbeat
  timeout) instead of `ConnectionError`.
- **`send_or_fail` timeout now drains all pending requests** and broadcasts
  `ConnectionError` before the next ping/heartbeat stops the actor.
  Previously only the single failing request was notified; remaining pending
  oneshots could hang on a half-open TCP connection since the poisoned sink
  is not guaranteed to surface through the reader.
- **`classify_rp_code` accepts `["0", <trailing>]` as success.** Per §2.1.b
  of the Rithmic R|Protocol Reference Guide, `rp_code[0] == "0"` is the
  authoritative success signal regardless of whether the server appends a
  trailing annotation. Previously `["0", "ok"]` was mis-classified as a
  rejection.
- **`has_multiple` (multipart framing) now keys on presence, not value.**
  Per §3 of the Reference Guide, the presence of `rq_handler_rp_code`
  signals "more frames follow"; the value inside is not the multipart
  signal. Previously keying on `[0] == "0"` silently truncated multipart
  responses whose intermediate frames carried a non-`"0"` value.
- **`examples/reconnect.rs` handles broadcast `RecvError::Lagged`
  explicitly** — a slow consumer that drops a connection-health frame
  through buffer wrap now logs and reconnects instead of silently exiting
  the read loop.
- **Unknown `template_id` responses route as subscription updates**
  (`is_update: true`) instead of going to the request handler. Previously
  they surfaced as "no responder found" noise on the per-request path.
  Subscribers now observe `RithmicMessage::Unknown` frames with a populated
  `error` describing the unknown `template_id`.

### Known behaviors
- `RithmicMessage::ForcedLogout` surfaces via subscription updates and
  `is_connection_issue()` returns `true` for it.
- A protobuf decode failure on a `ResponseHeartbeat` frame is routed to the
  subscription channel as a generic update (not a synthetic
  `HeartbeatTimeout`). Unchanged from prior behavior.

## [2.0.0]

### Breaking Changes

- **`RithmicConfig` no longer includes `account_id`, `fcm_id`, or `ib_id`** — those fields moved to `RithmicAccount`
- **`RithmicOrderPlant::get_handle()` and `RithmicPnlPlant::get_handle()` now require `&RithmicAccount`**
  - Create a `RithmicAccount` with `RithmicAccount::from_env(env)` or build one directly
  - For multi-account workflows, create one `RithmicAccount` per account and call `get_handle(&account)` for each
- **`subscription_receiver` on order and PnL handles is now `SubscriptionFilter`** instead of `broadcast::Receiver<RithmicResponse>`

### Added

- **`RithmicAccount`** — account-scoped type for order and PnL operations
  - `RithmicAccount::from_env(RithmicEnv)` loads `RITHMIC_<ENV>_ACCOUNT_ID`, `FCM_ID`, `IB_ID`
- **`load_tick_bars(symbol, exchange, bar_length, start_time_sec, end_time_sec)`** on `RithmicHistoryPlantHandle`
  - Fetches historical N-tick bars (e.g., 5-tick, 10-tick) for a symbol
  - `bar_length` controls the number of ticks aggregated into each bar
  - Returns `RithmicError::InvalidArgument` when `bar_length` is 0
- **`RithmicError::InvalidArgument(String)`** variant for rejecting invalid caller-supplied arguments before a request is sent
- **`RithmicAdvancedBracketOrder`** — full raw bracket order request exposing all venue-native fields
  - Supports triggered entry, break-even, trailing-stop, timed release/cancel, and if-touched entry conditions
  - Re-exported from crate root
- **`RithmicIfTouchedTrigger`** — conditional trigger for advanced bracket order entry (`if_touched_*` fields)
  - Re-exported from crate root
- **New bracket order enums** re-exported from crate root: `BracketType`, `BracketCondition`, `BracketPriceField`
- **Semantic ticker market-data subscription helpers** on `RithmicTickerPlantHandle` (all accept `symbol, exchange`):
  - `subscribe_instrument_status` / `unsubscribe_instrument_status` — market mode updates
  - `subscribe_order_book_summary` / `unsubscribe_order_book_summary` — aggregated bid/ask summary (proto 100, distinct from depth-by-order)
  - `subscribe_session_prices` / `unsubscribe_session_prices` — high/low/open trade statistics
  - `subscribe_quote_statistics` / `unsubscribe_quote_statistics` — quote-related statistics
  - `subscribe_indicator_prices` / `unsubscribe_indicator_prices` — settlement and projected settlement prices
  - `subscribe_open_interest` / `unsubscribe_open_interest` — open interest updates
  - `subscribe_end_of_day_prices` / `unsubscribe_end_of_day_prices` — end-of-day price data
  - `subscribe_order_price_limits` / `unsubscribe_order_price_limits` — high/low price limits
  - `subscribe_symbol_margin_rate` / `unsubscribe_symbol_margin_rate` — margin rate updates

### Changed

- **`RithmicError::SendFailed`** now also covers send timeouts — all plant WebSocket sends are bounded to 10 seconds; a hung sink surfaces as `SendFailed` rather than blocking the actor indefinitely
- **`load_ticks`** now delegates to `load_tick_bars` with `bar_length = 1` — no behavioral change for existing callers
- **`request_tick_bar_replay`** on `RithmicSenderApi` now accepts a `bar_type_specifier` parameter instead of hard-coding `"1"`

### Fixed

- **`rp_code = ["7", "no data"]`** is now treated as a successful empty result (not an error) across all list/replay/search responses — previously this caused methods like `replay_executions` to return `ServerError("no data")` when the query matched zero records

## [1.0.0]

### Breaking Changes

#### Typed Error Handling
- **`RithmicError`** enum replaces `String` errors across the entire API
  - `ConnectionFailed` — WebSocket connection could not be established
  - `ConnectionClosed` — plant's WebSocket connection is gone
  - `SendFailed` — WebSocket send failed after the request was registered
  - `EmptyResponse` — server returned empty response where at least one was expected
  - `ServerError(String)` — protocol-level rejection from Rithmic
- **`connect()`** on all plants now returns `Result<Plant, RithmicError>` instead of `Result<Plant, Box<dyn std::error::Error>>`
- All plant handle methods now return `Result<_, RithmicError>` instead of `Result<_, String>`

#### API Renames
- **`RithmicBracketOrder::qty`** renamed to **`quantity`** for clarity and consistency

#### Visibility Changes
- **`connection_handle`** on all plant structs is now `pub(crate)` (was `pub`)
  - Use the new **`await_shutdown()`** method instead to wait for the plant to stop

#### Dependency Changes
- **Prost** upgraded from `0.13` to `0.14` — if you depend on generated protobuf types, this is a breaking change
- **`async-trait`** removed — all async traits now use native Rust async trait support (requires Rust 1.85+)

#### Removed
- **`place_new_order()`** removed from `RithmicOrderPlantHandle` — use `place_order(RithmicOrder)` instead
- Protobuf codegen removed from build script — moved to standalone example binary

### Added

- **`LoginConfig`** struct for advanced login options (`aggregated_quotes`, `mac_addr`, `os_version`, `os_platform`)
- **`login_with_config(LoginConfig)`** method on all plant handles for customized login
- **`await_shutdown()`** method on all plant structs to wait for clean shutdown
- **`RithmicConfigBuilder`** re-exported from crate root
- **`InstrumentInfoError`** re-exported from crate root
- **`#[non_exhaustive]`** on `RithmicResponse`, `RithmicMessage`, `RithmicError`, `RithmicOrder`, `TrailingStop`, `ConnectStrategy`, `OrderStatus`, and `ConfigError` for forward compatibility
- **`Debug`** impl on all plant structs and plant handle structs
- **`RithmicConfig`** `Debug` output now redacts the `password` field

### Changed

- Set MSRV (minimum supported Rust version) to **1.85**
- Relaxed `futures-util` version constraint from `0.3.32` to `0.3`
- Replaced `serial_test` + unsafe `env::set_var` in tests with `temp-env` crate
- Added `#![warn(missing_docs)]` to enforce documentation coverage
- Feature flags section added to crate-level documentation

## [0.7.2] - 2026-02-07

### Added

#### New Order API
- **`RithmicOrder`**: New struct for placing standalone orders with advanced features
  - Supports trigger prices for stop orders (StopLimit, StopMarket)
  - Supports trailing stops via `TrailingStop` configuration
  - Ergonomic API using `Default` trait for optional fields
  - Comprehensive documentation with examples
- **`TrailingStop`**: Configuration struct for trailing stop orders
  - `trail_by_ticks`: Number of ticks to trail behind market price
- **`place_order(RithmicOrder)`**: New method on `RithmicOrderPlantHandle`
  - Preferred method for placing standalone orders
  - Supports all order types including stop orders and trailing stops

#### Ticker Plant Unsubscribe Methods
- **`unsubscribe(symbol, exchange)`**: Unsubscribe from market data for a symbol
- **`unsubscribe_order_book(symbol, exchange)`**: Unsubscribe from order book depth-by-order updates

#### Serde-Compatible Order Types
- **`OrderSide`**, **`OrderType`**, **`TimeInForce`**: New enums with optional serde support
  - Flexible parsing via `FromStr` (e.g., `"buy"`, `"BUY"`, `"B"` all parse to `OrderSide::Buy`)
  - `From` impls for conversion to protobuf request types
- **`OrderStatus::Expired`**: New variant added to the `OrderStatus` enum
- `OrderStatus` now supports optional serde serialization/deserialization

### Removed
- **`place_new_order()`**: Replaced by `place_order(RithmicOrder)` which supports trigger prices and trailing stops

## [0.7.1] - 2026-01-23

### Added

#### New Utility Module (`util`)
- **`InstrumentInfo`**: Parsed instrument reference data from Rithmic
  - Converts `ResponseReferenceData` to a structured type via `TryFrom`
  - `price_precision()`: Calculate decimal places based on tick size
  - `size_precision()`: Returns 0 for futures (whole contracts)
  - Fields include: symbol, exchange, name, tick_size, point_value, is_tradable, and more
- **`OrderStatus`**: Order status enum with helper methods
  - Parses case-insensitively with common variations ("filled" → Complete, "canceled" → Cancelled)
  - `is_terminal()`: Returns true for Complete, Cancelled, Rejected
  - `is_active()`: Returns true for Open, Pending, Partial
  - Implements `FromStr`, `Display`, `Default` (Unknown)
- **`rithmic_to_unix_nanos(ssboe, usecs)`**: Convert Rithmic timestamps to Unix nanoseconds
- **`rithmic_to_unix_nanos_precise(ssboe, usecs, nsecs)`**: Convert with optional nanosecond precision

#### RithmicResponse Helper Methods
- **`is_error()`**: Returns true if response has an error or connection issue
- **`is_connection_issue()`**: Returns true for ConnectionError, HeartbeatTimeout, ForcedLogout
- **`is_market_data()`**: Returns true for BestBidOffer, LastTrade, DepthByOrder, OrderBook, etc.

#### Optional Serde Support
- Added `serde` feature flag for serialization/deserialization support
- `RithmicEnv` derives `Serialize`/`Deserialize` when enabled with lowercase rename
- Enable with: `rithmic-rs = { version = "1.0", features = ["serde"] }`

#### New Example
- **`bracket_order.rs`**: Demonstrates placing bracket orders with typed enums

#### CI/CD
- Added GitHub Actions CI workflow for automated testing

### Fixed

#### Error Handling Improvements
- Replaced `.unwrap()` panics with proper error handling in all plant handles
  - `RithmicTickerPlantHandle`: `subscribe`, `unsubscribe`, `get_front_month_contract`, and other methods now handle channel send failures gracefully
  - `RithmicOrderPlantHandle`: `place_bracket_order`, `modify_order`, `cancel_order`, and other methods now handle channel send failures gracefully
  - `RithmicHistoryPlantHandle`: `load_time_bars`, `load_ticks`, and other methods now handle channel send failures gracefully
  - `RithmicPnlPlantHandle`: `subscribe_pnl_updates`, `pnl_position_snapshots`, and other methods now handle channel send failures gracefully

#### Code Quality
- Addressed clippy lints in util module
- Cleaned up util module documentation

## [0.7.0] - 2026-01-08

### Breaking Changes

#### Order Types Now Use Enums Instead of Raw Integers
- **`RithmicBracketOrder`**: Field types and names changed
  - `action: i32` → `action: BracketTransactionType` (enum)
  - `ordertype: i32` → `price_type: BracketPriceType` (enum, **renamed**)
  - `duration: i32` → `duration: BracketDuration` (enum)
- **`RithmicModifyOrder`**: Field type changed
  - `ordertype: i32` → `price_type: ModifyPriceType` (enum, **renamed**)

**Migration example:**
```rust
// Old (0.6.x)
let order = RithmicBracketOrder {
    action: 1,      // Buy
    ordertype: 1,   // Limit
    duration: 2,    // Day
    // ...
};

// New
use rithmic_rs::{BracketTransactionType, BracketPriceType, BracketDuration};
let order = RithmicBracketOrder {
    action: BracketTransactionType::Buy,
    price_type: BracketPriceType::Limit,
    duration: BracketDuration::Day,
    // ...
};
```

### Added

#### Cleaner Public API
- All order-related types and enums now re-exported from crate root:
  - `RithmicBracketOrder`, `RithmicModifyOrder`, `RithmicCancelOrder`, `RithmicOcoOrderLeg`
  - `BracketTransactionType`, `BracketDuration`, `BracketPriceType`
  - `ModifyPriceType`
  - `RithmicResponse`, `RithmicStream`
- Internal implementation details hidden with `pub(crate)` visibility
- Users can now import all types from `rithmic_rs::*` instead of deep module paths

#### Improved Documentation
- Added comprehensive doc comments and examples for all order types
- Simplified `ConnectionError` and `HeartbeatTimeout` documentation
- Added module-level documentation for `api`, `plants`, and `rti` modules
- Added `.env.blank` reference to `RithmicConfig::from_env()` docs
- Streamlined README with clearer quick start and architecture sections

#### Reorganized Examples
- Added `ticker.rs`: Market data subscription and symbol discovery
- Added `pnl.rs`: P&L monitoring example
- Added `reconnect.rs`: Reconnection handling with subscription tracking
- Removed `market_data.rs` (replaced by `ticker.rs`)

### Removed
- Removed unused `HEARTBEAT_TIMEOUT_SECS` constant (dead code from removed HeartbeatManager)

## [0.6.2] - 2025-12-20

### Added

#### New Sender API Methods

##### Ticker Plant
- `request_rithmic_system_gateway_info()`: Get gateway-specific information
- `request_get_instrument_by_underlying()`: Get all instruments for an underlying symbol
- `request_market_data_update_by_underlying()`: Subscribe to market data by underlying
- `request_give_tick_size_type_table()`: Get tick size table for a tick size type
- `request_product_codes()`: Get available product codes for an exchange
- `request_get_volume_at_price()`: Get volume profile for a symbol
- `request_auxilliary_reference_data()`: Get additional reference data for a symbol
- `request_volume_profile_minute_bars()`: Get minute bars with volume profile
- `request_resume_bars()`: Resume a truncated bars request
- `request_depth_by_order_snapshot()`: Get depth by order snapshot
- `request_depth_by_order_update()`: Subscribe to depth by order updates

##### Order Plant
- `request_login_info()`: Get current login session information
- `request_oco_order()`: Place OCO (One Cancels Other) order pairs
- `request_link_orders()`: Link multiple orders together
- `request_easy_to_borrow_list()`: Get easy-to-borrow list for short selling
- `request_modify_order_reference_data()`: Update user tag on existing order
- `request_order_session_config()`: Get/set order session configuration
- `request_replay_executions()`: Replay historical execution data

##### Repository Plant (Agreements)
- `request_list_unaccepted_agreements()`: List agreements not yet accepted
- `request_list_accepted_agreements()`: List already accepted agreements
- `request_accept_agreement()`: Accept a specific agreement
- `request_show_agreement()`: Get full agreement details
- `request_set_rithmic_mrkt_data_self_cert_status()`: Set market data self-certification status

#### API Ergonomics
- Re-exported `RithmicOcoOrderLeg` and related OCO order enums from `api` module:
  - `OcoTransactionType`: Buy/Sell transaction type
  - `OcoDuration`: Day/GTC/IOC/FOK duration
  - `OcoPriceType`: Limit/Market/StopLimit/StopMarket price type
- Changed `RithmicOcoOrderLeg.trigger_price` from `f64` to `Option<f64>` since it's only required for stop orders

#### New Market Data Messages (Ticker Plant)
- `TradeStatistics`: High/low/open price statistics
- `QuoteStatistics`: Quote-related statistics  
- `IndicatorPrices`: Settlement, projected settlement prices
- `EndOfDayPrices`: End of day price data
- `MarketMode`: Market trading mode updates
- `OpenInterest`: Open interest updates
- `FrontMonthContractUpdate`: Front month contract changes
- `DepthByOrderEndEvent`: Depth by order stream end marker
- `SymbolMarginRate`: Symbol margin rate updates
- `OrderPriceLimits`: Price limit updates

#### New Order Plant Messages
- `UserAccountUpdate`: Account permission/access changes
- `AccountListUpdates`: Account list change notifications
- `AccountRmsUpdates`: Real-time RMS limit updates

#### New RithmicMessage Variants
- `ResponseReferenceData`: Symbol reference data
- `ResponseFrontMonthContract`: Front month contract info
- `ResponseTimeBarUpdate`: Time bar subscription confirmation
- `ResponseTickBarUpdate`: Tick bar subscription confirmation
- `ResponseAccountRmsUpdates`: RMS updates subscription confirmation

### Fixed
- Fixed clippy warning: use `is_multiple_of()` instead of modulo check in connection retry logic

## [0.6.1] - 2025-11-24

> **⚠️ Breaking Change:** Environment variable names have changed. See migration guide below.

### Breaking Changes

#### Environment Variable Structure
- **Environment-specific configuration variables** for better multi-environment support
  - All configuration variables now include environment prefix (DEMO, LIVE, TEST)
  - Account variables: `RITHMIC_<ENV>_ACCOUNT_ID`, `RITHMIC_<ENV>_FCM_ID`, `RITHMIC_<ENV>_IB_ID`
  - Connection variables: `RITHMIC_<ENV>_URL`, `RITHMIC_<ENV>_ALT_URL`
  - User credentials: `RITHMIC_<ENV>_USER`, `RITHMIC_<ENV>_PW`
  - Enables separate configurations for each environment
  - Example: `RITHMIC_DEMO_ACCOUNT_ID`, `RITHMIC_LIVE_ACCOUNT_ID`, `RITHMIC_TEST_ACCOUNT_ID`

#### Migration from Previous Versions
**Old variable names (no longer supported):**
- `RITHMIC_ACCOUNT_ID` → `RITHMIC_<ENV>_ACCOUNT_ID`
- `FCM_ID` → `RITHMIC_<ENV>_FCM_ID`
- `IB_ID` → `RITHMIC_<ENV>_IB_ID`

**Example for Demo environment:**
```bash
# Old (0.6.0 and earlier)
RITHMIC_ACCOUNT_ID=account123
FCM_ID=fcm123
IB_ID=ib123
RITHMIC_DEMO_USER=user
RITHMIC_DEMO_PW=pass

# New (0.6.1)
RITHMIC_DEMO_ACCOUNT_ID=account123
RITHMIC_DEMO_FCM_ID=fcm123
RITHMIC_DEMO_IB_ID=ib123
RITHMIC_DEMO_USER=user
RITHMIC_DEMO_PW=pass
RITHMIC_DEMO_URL=<provided_by_rithmic>
RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>
```

See `examples/.env.blank` for complete template with all required variables.

### Fixed
- Fixed rustfmt compliance issues with long error messages
- Fixed clippy warning: use `.first()` instead of `.get(0)` for idiomatic array access

## [0.6.0] - 2025-11-23

### Breaking Changes

- **Removed `connection_info` module** - deprecated types removed (use `RithmicConfig` instead)
- **Removed `RithmicConfig::from_dotenv()` method** - consumers call `dotenvy::dotenv()` themselves
- **Removed `return_heartbeat_response()` method** from all plant handles
- **Updated to `dotenvy` crate** - moved to dev-dependencies (from deprecated `dotenv`)

### Changed

- **Connection health monitoring** now fully automatic via WebSocket ping/pong
  - Heartbeats sent automatically for protocol compliance
  - Successful responses silently dropped
  - Errors delivered as `HeartbeatTimeout` messages
- **Environment variable loading** now consumer-controlled
  - Library no longer forces approach for loading env vars
  - Examples demonstrate using `dotenvy`, but any method works
- **Reduced code complexity** - removed 500+ lines of deprecated code

### Documentation

- Removed dotenv/`.env` references from library docs (examples still show usage)
- Updated README with clearer examples and breaking changes summary

## [0.5.3] - 2025-11-22

### Added

#### Order Management APIs
- **New `cancel_all_orders()` method** on `RithmicOrderPlantHandle`
  - Cancels all active orders across all symbols and exchanges for the account
  - Returns cancellation confirmation response
- **New order history methods** on `RithmicOrderPlantHandle`
  - `show_order_history_dates()`: Get dates for which order history is available
  - `show_order_history_summary(date)`: Get order summary for a specific date (YYYYMMDD format)
  - `show_order_history_detail(basket_id, date)`: Get detailed history for a specific order
  - `show_order_history(basket_id)`: Get general order history with optional basket_id filter
  - Enables comprehensive order audit trails and historical analysis

#### Risk Management APIs
- **New RMS information methods** on `RithmicOrderPlantHandle`
  - `get_account_rms_info()`: Retrieve account-level risk management limits and settings
  - `get_product_rms_info()`: Retrieve product-specific risk management limits
  - `get_trade_routes(subscribe_for_updates)`: Get available trade routes with optional update subscription
  - Critical for monitoring trading limits and route availability

#### Symbol Search and Discovery APIs
- **New `search_symbols()` method** on `RithmicTickerPlantHandle`
  - Search for symbols by text pattern with optional filters
  - Supports filtering by exchange, product code, and instrument type
  - Configurable search pattern (EQUALS or CONTAINS)
  - Returns list of matching symbols for dynamic symbol discovery
- **New `list_exchanges()` method** on `RithmicTickerPlantHandle`
  - Lists exchanges available to the specified user
  - Useful for determining trading permissions

#### Protocol Message Support
- **New `TradeRoute` message type** added to `RithmicMessage` enum
  - Handles template ID 310 for trade route information
  - Delivered as update message (`is_update: true`)
  - Supports trade route subscription updates

#### Sender API Methods
- Added 10 new request methods to `RithmicSenderApi`:
  - `request_cancel_all_orders()`: Template 346
  - `request_account_rms_info()`: Template 304
  - `request_product_rms_info()`: Template 306
  - `request_trade_routes(subscribe_for_updates)`: Template 310
  - `request_search_symbols(...)`: Template 109 with extensive search filters
  - `request_list_exchanges(user)`: Template 342
  - `request_show_order_history_dates()`: Template 318
  - `request_show_order_history_summary(date)`: Template 324
  - `request_show_order_history_detail(basket_id, date)`: Template 326
  - `request_show_order_history(basket_id)`: Template 322

### Changed

#### Internal Improvements
- Extended `OrderPlantCommand` enum with 8 new command variants for order history and RMS operations
- Extended `TickerPlantCommand` enum with 2 new command variants for symbol search and exchange listing
- Updated receiver API to handle TradeRoute message type (template ID 310)
- Added new imports for request types: `RequestCancelAllOrders`, `RequestAccountRmsInfo`, `RequestProductRmsInfo`, `RequestSearchSymbols`, `RequestTradeRoutes`, and order history request types

### Known Issues

#### Error Handling
- New TradeRoute message handler uses `.unwrap()` on protobuf decode (line 438 in receiver_api.rs)
- New plant handle methods use multiple `.unwrap()` calls that could panic on channel failures
- These follow existing patterns in the codebase but should be addressed in future releases
- Users should be aware that malformed messages or actor failures may cause panics

## [0.5.2] - 2025-11-20

### Added

#### Optional Heartbeat Response Handling
- **New `return_heartbeat_response()` method** on all plant handles (ticker, order, pnl, history)
  - Controls whether heartbeat responses are delivered through subscription channel
  - Default behavior: heartbeats use request/response pattern (not sent to channel)
  - Call `handle.return_heartbeat_response(true)` to enable heartbeat monitoring
  - Useful for explicit connection health monitoring during trading hours
  - Can be disabled during off-market hours to avoid false alarms

#### Heartbeat Timeout Detection
- **New `HeartbeatManager`** for tracking heartbeat response timeouts
  - Monitors pending heartbeats when responses are expected
  - Detects timeouts after 30 seconds (configurable via `HEARTBEAT_TIMEOUT_SECS`)
  - Integrated into all plant actors (ticker, order, pnl, history)
  - Non-blocking implementation using tokio `sleep_until()` with efficient select! loop integration
- **New `RithmicMessage::HeartbeatTimeout` variant** for timeout notifications
  - Sent as an update message when heartbeat response does not arrive within timeout period
  - Includes error context: "Heartbeat response timeout"
  - Only active when heartbeat responses are expected (`return_heartbeat_response(false)`)
  - Helps detect connection degradation without requiring manual timeout tracking
  - Comprehensive documentation with usage examples
- **Timeout constant `HEARTBEAT_TIMEOUT_SECS`** in `ws.rs`
  - Set to 30 seconds (half the 60-second heartbeat interval)
  - Provides balance between detecting issues and avoiding false positives

### Changed

#### Internal Refactoring
- Renamed internal field `ignore_heartbeat_response` to `expect_heartbeat_response` in all plants
  - Improves code clarity with explicit naming and positive boolean logic
  - Added documentation explaining the setting's purpose and when to use it
  - No API changes - public interface remains the same

#### Heartbeat Response Delivery
- **Reverted heartbeat behavior to request/response pattern** (no longer sent through subscription channel by default)
  - Heartbeats sent automatically on interval but responses not delivered to subscription channel
  - Previous behavior (0.5.0): All heartbeat responses delivered through subscription channel as updates
  - New behavior: Heartbeat responses only delivered if explicitly enabled via `return_heartbeat_response(true)`
  - Reduces noise in subscription channel for applications that don't need heartbeat monitoring
  - Provides flexibility: enable during trading hours, disable during off-hours
- **Internal improvements** to `request_handler.rs`
  - Now handles heartbeat responses when callbacks are registered
  - Refactored response sending into helper method for better error handling
  - Improved logging for failed response deliveries

### Fixed

#### Heartbeat Response Handling
- Fixed ResponseHeartbeat request_id extraction in `src/api/receiver_api.rs`
  - Now correctly extracts request_id from `user_msg[0]` instead of using empty string
  - Enables proper matching of heartbeat responses to pending requests in timeout detection
- Fixed ResponseHeartbeat routing in all plants
  - Successful heartbeat responses are never delivered to subscription channel (silent when connection is healthy)
  - When `expect_heartbeat_response = true`, only `HeartbeatTimeout` messages are sent on failure
  - Purpose: connection health verification - report only when heartbeat fails, not when it succeeds

#### Code Quality
- Fixed clippy warning `tabs_in_doc_comments` in `src/rti.rs`
  - Replaced tab character with spaces in documentation comment

## [0.5.1]

### Added

#### Connection Error Handling
- **New `RithmicMessage::ConnectionError` variant** for WebSocket connection failures
  - Provides unified error handling for all connection-related failures
  - Enables consumers to implement reconnection logic via pattern matching
  - Includes comprehensive documentation with examples
- **Comprehensive WebSocket error detection** across all plants (ticker, order, pnl, history):
  - `ConnectionClosed`: Normal WebSocket closure
  - `AlreadyClosed`: Attempted use of closed connection
  - `Io` errors: Network/socket I/O failures (connection lost, timeout)
  - `ResetWithoutClosingHandshake`: Connection reset without proper WebSocket close
  - `SendAfterClosing`: Attempted to send data after closing frame sent
  - `ReceivedAfterClosing`: Received data after closing frame sent
- **Automatic error notifications** sent through subscription channel when connection fails
  - `RithmicResponse` with `message: ConnectionError` and `is_update: true`
  - `error` field contains specific error description
  - `source` field identifies which plant failed
  - Enables consumers to detect and handle connection failures in real-time

#### Documentation
- Added comprehensive documentation to `RithmicMessage::ConnectionError`
  - Lists all handled error types
  - Step-by-step guidance for handling connection errors
  - Complete code examples showing pattern matching
  - Notes on behavioral details and channel lifecycle
- Added detailed documentation to `RithmicResponse` struct
  - Explains error handling for both protocol and connection errors
  - Examples showing how to handle different error scenarios
  - Cross-references to related documentation

### Changed
- **Improved logging consistency**: Changed `ConnectionClosed` log level from `info!` to `error!` across all plants
  - Ensures all connection termination events are logged at error level
  - Makes connection issues more visible in production logs
- Replace `event!` macro with specific logging macros (`info!`, `error!`, `warn!`) across library code for better code clarity and idiomatic Rust logging
  - Updated: all plant files, `src/api/receiver_api.rs`, `src/request_handler.rs`

### Fixed
- **Connection error handling**: Plants now properly stop and notify consumers on all WebSocket connection failures
  - Previously, most connection errors fell through to catch-all warning and left plants in undefined state
  - Now all connection errors trigger clean shutdown with error notification
  - Prevents resource leaks and zombie plant instances

## [0.5.0]

### Breaking Changes

#### Connection API Changes
- **Plant constructors renamed**: `new()` → `connect()` across all plants
- **Return type changed**: `connect()` now returns `Result<Plant, Box<dyn std::error::Error>>`
- **Required parameter**: All plants now require a `ConnectStrategy` parameter
- Enables proper error handling instead of panics and explicit connection strategy selection

#### Configuration API Changes
- **New unified configuration**: `RithmicConfig` replaces separate account/connection info types
  - Old types (`AccountInfo`, `RithmicConnectionInfo`, `RithmicConnectionSystem`) are deprecated
  - Migration path provided via `From`/`TryFrom` trait implementations
- **Environment handling**: `RithmicEnv` replaces `RithmicConnectionSystem`
  - More idiomatic enum naming
  - Better integration with configuration builder

#### Error Handling Changes
- **Heartbeat error visibility**: Heartbeat responses now delivered through subscription channel
  - `ResponseHeartbeat` changed from `is_update: false` → `is_update: true`
  - Consumers must check `error` field on heartbeat responses to detect connection issues
  - Breaking for applications that assumed heartbeats wouldn't appear in subscriptions
- **Forced logout events**: Now delivered through subscription channel for visibility
  - `ForcedLogout` changed from `is_update: false` → `is_update: true`
  - Applications must handle forced logout events to implement reconnection logic
- **No more panics**: Error responses from server no longer panic, sent to subscription channel instead

### Added

#### Connection Strategies
- New `ConnectStrategy` enum with three modes:
  - **`Simple`**: Single connection attempt (recommended default, fast-fail)
  - **`Retry`**: Indefinite retries with exponential backoff on same URL
  - **`AlternateWithRetry`**: Alternates between primary and beta URLs with retries
- Retry strategies now retry indefinitely instead of limiting to 15 attempts
- Maximum backoff capped at 60 seconds to ensure at most one login attempt per minute
- Prevents excessive load on Rithmic servers during extended outages

#### Unified Configuration API
- `RithmicConfig`: Modern, ergonomic configuration type combining account and connection fields
- `RithmicEnv`: Environment selection enum (Demo, Live, Test)
- `ConfigError`: Type-safe error handling for configuration operations
- `from_env()`: Load configuration from environment variables with proper error handling
- `from_dotenv()`: Load configuration from .env file (requires `dotenv` feature)
- `RithmicConfigBuilder`: Builder pattern for programmatic configuration
- Comprehensive unit tests (15 tests) covering all configuration scenarios

#### Connection Health Monitoring
- Heartbeat responses now include error information in subscription channel
- Forced logout events delivered through subscription channel
- Applications can monitor connection health in real-time
- Examples added showing proper heartbeat timeout tracking

#### Documentation
- Comprehensive documentation for connection strategies
- Connection timeout and retry behavior documented
- Migration guide for deprecated types in `connection_info` module
- Real-world examples showing proper error handling and connection monitoring
- Examples updated to demonstrate new unified configuration API

### Fixed

#### Critical Panic Fixes
- Fixed panic on unknown message types by adding proper error handling (#3)
  - Unknown message types now logged and gracefully handled
  - Added `UnknownMessage` variant to handle unexpected protocol messages
- Fixed panic on error responses in ticker plant (#2)
  - Error responses from `buf_to_message()` now handled gracefully
  - Errors sent through subscription channel for consumer handling
- Fixed panic on heartbeat errors across all plants
  - Broadcast send errors now handled gracefully instead of unwrapping
  - No more crashes on channel receiver drops

#### Consistency Fixes
- Fixed inconsistent heartbeat logic across plants (#9)
  - All plants (ticker, order, pnl, history) now only send heartbeats after login
  - Prevents protocol violations from pre-login heartbeats
  - Unified behavior across all plant implementations
- Fixed MessageType decode unwrap with proper error handling (#4)
  - Removed `.unwrap()` calls in message decoding
  - Proper error propagation through Result types

#### Code Quality
- Removed `#[allow(dead_code)]` annotations from valid public API methods (#11)
  - `request_new_order`, `request_exit_position`, `request_show_brackets`, `request_show_bracket_stops`
  - Added comprehensive documentation for these public API methods
  - Improved library API clarity

### Deprecated

The following types are deprecated and will be removed in a future version:
- `AccountInfo` - Use `RithmicConfig` instead
- `RithmicConnectionInfo` - Use `RithmicConfig` instead
- `RithmicConnectionSystem` - Use `RithmicEnv` instead
- `get_config()` function - Use `RithmicConfig::from_env()` or builder pattern

Migration helpers provided via trait implementations maintain backward compatibility.

### Changed

#### API Consistency
- Unified error handling pattern across all plants
  - Consistent routing based on `is_update` flag
  - Simplified message handling logic
  - No panics in production code

#### Internal Improvements
- Updated `RithmicSenderApi` to use `RithmicConfig` and `RithmicEnv`
- Simplified routing logic using `is_update` flag instead of message type checks
- Improved type safety by replacing panics with proper error types

## [0.4.2] - 2025-11-15

Previous stable release. See git history for earlier changes.

---

## Version History Summary

- **2.0.0**: Breaking changes - `RithmicAccount` split from `RithmicConfig`, account-scoped `get_handle()`, `SubscriptionFilter`; advanced bracket orders, semantic ticker subscriptions, bounded WebSocket sends
- **1.0.0**: Breaking changes - typed `RithmicError` enum, prost 0.14, async-trait removed, `LoginConfig` for advanced login, `await_shutdown()`, non_exhaustive annotations, MSRV 1.85
- **0.7.2** (2026-02-07): New RithmicOrder API with trigger prices and trailing stops, ticker plant unsubscribe methods, serde-compatible order types
- **0.7.1** (2026-01-23): New utility module (InstrumentInfo, OrderStatus, timestamp helpers), RithmicResponse helper methods, optional serde support, improved error handling
- **0.7.0** (2026-01-08): Breaking changes - Order types now use enums instead of raw integers, cleaner public API exports
- **0.6.2** (2025-12-20): Expanded plant handle APIs, additional message types, OCO order support, and new sender methods
- **0.6.1** (2025-11-24): Environment-specific configuration variables
- **0.6.0** (2025-11-23): Major breaking changes - Removed deprecated code, simplified heartbeat handling, updated to dotenvy
- **0.5.3** (2025-11-22): API expansion - Order history, RMS info, symbol search, trade routes, cancel all orders
- **0.5.2** (2025-11-20): Heartbeat improvements - Optional heartbeat response handling, heartbeat timeout detection, internal refactoring
- **0.5.1** (2025-11-18): Connection error handling improvements - ConnectionError variant, comprehensive WebSocket error detection, automatic error notifications
- **0.5.0** (2025-11-16): Major stability and API improvements - Connection strategies, unified config, panic fixes, connection health monitoring
- **0.4.2** (2025-11-15): Previous stable release

[Unreleased]: https://github.com/pbeets/rithmic-rs/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/pbeets/rithmic-rs/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/pbeets/rithmic-rs/compare/v0.7.2...v1.0.0
[0.7.2]: https://github.com/pbeets/rithmic-rs/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/pbeets/rithmic-rs/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/pbeets/rithmic-rs/compare/v0.6.2...v0.7.0
[0.6.2]: https://github.com/pbeets/rithmic-rs/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/pbeets/rithmic-rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/pbeets/rithmic-rs/compare/v0.5.3...v0.6.0
[0.5.3]: https://github.com/pbeets/rithmic-rs/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/pbeets/rithmic-rs/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/pbeets/rithmic-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/pbeets/rithmic-rs/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/pbeets/rithmic-rs/releases/tag/v0.4.2
