# Rust Rithmic R | Protocol API client

[![Crates.io](https://img.shields.io/crates/v/rithmic-rs.svg)](https://crates.io/crates/rithmic-rs)
[![docs.rs](https://img.shields.io/docsrs/rithmic-rs)](https://docs.rs/rithmic-rs)
[![CI](https://github.com/pbeets/rithmic-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/pbeets/rithmic-rs/actions)
[![License](https://img.shields.io/crates/l/rithmic-rs)](LICENSE-MIT)

[Official Rithmic API](https://www.rithmic.com/apis)

Unofficial rust client for connecting to Rithmic's R | Protocol API.

Supported version: **0.89.0.0** (template 5.42)

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rithmic-rs = "3.1.0"
tokio = { version = "1", features = ["full"] }
```

Set your environment variables:

```sh
RITHMIC_APP_NAME=your_app_name
RITHMIC_APP_VERSION=1

RITHMIC_DEMO_USER=your_username
RITHMIC_DEMO_PW=your_password
RITHMIC_DEMO_URL=<provided_by_rithmic>
RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>

# Required for order and PnL requests
RITHMIC_DEMO_ACCOUNT_ID=your_account_id
RITHMIC_DEMO_FCM_ID=your_fcm_id
RITHMIC_DEMO_IB_ID=your_ib_id

# Optional: Rithmic system name to log in to (default "Rithmic Paper Trading"
# on Demo, "Rithmic 01" on Live). Set e.g. RITHMIC_LIVE_SYSTEM_NAME to select
# another provider on Live.
# RITHMIC_DEMO_SYSTEM_NAME=Rithmic Paper Trading

# See examples/.env.blank for Live and Test
```

`RithmicConfig` contains connection and login details. `RithmicAccount` is separate and
identifies which trading account to use for order and PnL requests. Login is user-scoped;
account fields are sent with account-scoped requests, not during the initial sign-in.

Stream live market data:

```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?; // for live RithmicEnv::Live
    let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = plant.get_handle();

    handle.login().await?;
    handle.subscribe("ESM6", "CME").await?; // Update to current front-month ES contract

    while let Ok(update) = handle.subscription_receiver.recv().await {
        println!("{:?}", update.message);
    }

    Ok(())
}
```

Nine runnable examples cover the rest — order routing, historical data, error
handling and reconnection. See [Examples](#examples).

## Migrating from 2.x

3.0.0 reshapes the order surface. **[MIGRATING.md](MIGRATING.md)** works through
it section by section with before/after code — section 1 alone resolves most of
the compiler errors.

The headline changes:

| Change | What it means for you |
|---|---|
| Commands are built with `new()` + setters | `..Default::default()` no longer compiles; `build()` validates and returns a `Result` |
| Fourteen generated enums replaced by crate-owned ones | `OrderSide`, `OrderType`, `TimeInForce` and friends; variant names are unchanged, but matches now need a `_` arm |
| Every order call takes a command struct | `cancel_all_orders`, `exit_position`, `adjust_target`/`adjust_stop` no longer take loose arguments |
| Prices are `Option<f64>` | Wrap in `Some(..)`; pass `None` for market orders |
| Seven handle methods renamed | Each is now named after the request it sends |
| `RithmicConfig` gains fields | Build it through `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)` rather than a struct literal |

The compiler finds all of the above. **It will not find these**, which compile
untouched and change what reaches the exchange:

- Orders route off the exchange's published trade route instead of a hardcoded
  `"globex"`/`"simulator"`. No route and no override means the order is **not
  sent** — you get `RithmicError::NoTradeRoute`.
- `cancel_all_orders` is now attributed `Auto`, not `Manual`.
- A target-only bracket now sends `TARGET_ONLY_STATIC` rather than
  `TargetAndStopStatic`.
- `get_account_list` and `get_account_rms_info` may return fewer accounts, now
  that requests carry the real login info.
- Market orders no longer go out priced at `0.0` — an unset price is omitted.
- An empty `user_tag` echoes back as `None`, not `Some("")`.

Go through that list before the first run against a live account.
[Section 10](MIGRATING.md#10-behavior-changes-that-need-no-code-change) explains
each one and what to set to keep the old behavior.

One more worth acting on even though nothing breaks: 2.x replays stopped at
10,000 records and gave no sign they had. Switch to the `_all` loaders — see
[History Plant](#history-plant).

## Architecture

This library uses the actor pattern where each Rithmic service runs independently as its own tokio task. All communication happens through tokio channels.

- **`RithmicTickerPlant`** - Real-time market data (trades, quotes, order book)
- **`RithmicOrderPlant`** - Order entry and management
- **`RithmicHistoryPlant`** - Historical tick and bar data
- **`RithmicPnlPlant`** - Position and P&L tracking

### Ticker Plant

```rust
// Subscribe to real-time quotes
handle.subscribe("ESM6", "CME").await?;

// Unsubscribe when done
handle.unsubscribe("ESM6", "CME").await?;

// Additional market data subscriptions
handle.subscribe_instrument_status("ESM6", "CME").await?;
handle.subscribe_open_interest("ESM6", "CME").await?;
handle.subscribe_session_prices("ESM6", "CME").await?;
handle.subscribe_order_price_limits("ESM6", "CME").await?;

// Symbol discovery
let symbols = handle.search_symbols("ES", Some("CME"), None, None, None).await?;
let front_month = handle.get_front_month_contract("ES", "CME", false).await?;
```

### Order Plant

```rust
use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicBracketOrder, RithmicCancelOrder,
    RithmicConfig, RithmicEnv, RithmicExitPosition, RithmicOcoOrder, RithmicOcoOrderLeg,
    RithmicOrder, RithmicOrderPlant,
};

let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
let handle = plant.get_handle(&account);

handle.login().await?;
handle.subscribe_order_updates().await?;

// Every order command starts from `::new()`, which takes no arguments, and is
// filled in by setters named after the fields they set. `build()` validates and
// returns a `Result`.
let order = RithmicOrder::new()
    .symbol("ESM6")
    .exchange("CME")
    .quantity(1)
    .transaction_type(OrderSide::Buy)
    .price_type(OrderType::Limit)
    .price(5000.0)
    .user_tag("my-order")
    .build()?;

handle.place_order(order).await?;

// Cancel by the `basket_id` carried on the order notification
handle.cancel_order(RithmicCancelOrder::new().id(basket_id).build()?).await?;

// Flatten by instrument. With no symbol/exchange set it flattens the whole account.
handle.exit_position(RithmicExitPosition::new().symbol("ESM6").exchange("CME").build()?).await?;
```

Order state arrives on the subscription stream as `RithmicOrderNotification`
updates, not in the response to the call.

The handle also covers account queries: `show_fill_history` returns the
account's fills over a time or trade-date window, and `get_user_info` returns
the login's profile, entitlements, and session limits (both new in 0.89).

For multi-account workflows, create one `RithmicAccount` per account and call
`get_handle(&account)` for each handle you need.

#### Bracket and OCO orders

Brackets build the same way. `.target(n)`/`.stop(n)` size their leg from the
entry quantity at the moment they are called, so set `.quantity()` first.

```rust
let bracket = RithmicBracketOrder::new()
    .symbol("ESM6")
    .exchange("CME")
    .quantity(1)
    .action(OrderSide::Buy)
    .price_type(OrderType::Limit)
    .price(5000.0)
    .target(20) // take profit 20 ticks above entry
    .stop(10)   // stop loss 10 ticks below entry
    .build()?;

handle.place_bracket_order(bracket).await?;
```

Leave `bracket_type` unset and `build()` derives it from the legs you supplied.
Use `.targets(..)`/`.stops(..)` with explicit `(quantity, ticks)` pairs for
multi-leg brackets, then `adjust_target`/`adjust_stop` to move a level.

An OCO order carries two or more legs, and the first to fill cancels the rest:

```rust
let oco = RithmicOcoOrder::new()
    .leg(RithmicOcoOrderLeg::new()
        .symbol("ESM6").exchange("CME").quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(4990.0)
        .build()?)
    .leg(RithmicOcoOrderLeg::new()
        .symbol("ESM6").exchange("CME").quantity(1)
        .transaction_type(OrderSide::Sell)
        .price_type(OrderType::Limit)
        .price(5010.0)
        .build()?)
    .build()?;

handle.place_oco_order(oco).await?;
```

#### Trade routes

Orders route off the trade route the exchange publishes, read once at `login()`.
Check one with `trade_route_for("CME")`, or set `.trade_route(..)` on the command
to override it. If no route exists and the command sets none, the order is **not
sent** and you get `RithmicError::NoTradeRoute`. See
[`examples/trade_routes.rs`](examples/trade_routes.rs).

### History Plant

```rust
use rithmic_rs::TimeBarType;

let symbol = "ESM6".to_string(); // Update to current front-month ES contract
let exchange = "CME".to_string();

// start / end are i32 unix seconds
let bars = handle
    .load_time_bars_all(symbol.clone(), exchange.clone(), TimeBarType::MinuteBar, 5, start, end)
    .await?;
let ticks = handle
    .load_ticks_all(symbol.clone(), exchange.clone(), start, end)
    .await?;

// Bars aggregating a fixed number of trades (e.g. one bar per 5 trades)
let tick_bars = handle.load_tick_bars_all(symbol, exchange, 5, start, end).await?;
```

**Use the `_all` loaders.** Rithmic caps a replay at 10,000 records and gives no
sign that it did — the closing response of a truncated replay is identical to a
complete one's. The `_all` variants set `resume_bars`, which lifts the cap, so
one request covers the window. `load_ticks`, `load_tick_bars` and
`load_time_bars` leave the cap in place; reach for them only when you want at
most 10,000 records.

The whole window is buffered before the call returns. A full 23-hour ES session
runs to hundreds of thousands of records, so ask for the window you need rather
than a day at a time.

Volume profile bars take a request struct:

```rust
use rithmic_rs::VolumeProfileMinuteBarsRequest;

let request = VolumeProfileMinuteBarsRequest::new()
    .symbol("ESM6")
    .exchange("CME")
    .bar_type_period(5)
    .start_time_sec(start)
    .end_time_sec(end)
    .build()?;

let bars = handle.load_volume_profile_minute_bars(request).await?;
```

All of these validate before sending: an empty symbol or exchange, a bar length
below 1, a non-positive timestamp, or a window that ends before it starts comes
back as `RithmicError::InvalidArgument` with no round trip.

Live bars stream through `subscribe_time_bar_updates` and
`subscribe_tick_bar_updates`.

### PnL Plant

```rust
use rithmic_rs::{
    ConnectStrategy, RithmicAccount, RithmicConfig, RithmicEnv, RithmicPnlPlant,
};

let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
let plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Retry).await?;
let handle = plant.get_handle(&account);
handle.login().await?;

// Monitor P&L
handle.subscribe_pnl_updates().await?;
let snapshot = handle.get_pnl_position_snapshot().await?;
```

## Error Handling

```rust
use rithmic_rs::RithmicError;

match handle.subscribe("ESM6", "CME").await {
    Ok(resp) => match &resp.error {
        Some(err) => eprintln!("Server rejected: {}", err),
        None => { /* success */ }
    },
    Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
        handle.abort();
        // reconnect — see examples/reconnect.rs
    }
    Err(e) => eprintln!("{}", e),
}

if let Err(RithmicError::RequestRejected(err)) = handle.login().await {
    eprintln!(
        "Login rejected: code={} msg={}",
        err.code.as_deref().unwrap_or("?"),
        err.message.as_deref().unwrap_or(""),
    );
}
```

When inspecting a `RithmicResponse` directly (for example, entries from a
subscription broadcast), match on `response.error` — it is `Option<RithmicError>`.
Use `RithmicError::is_connection_issue` to distinguish transport failures from
requests the server turned down.

`RithmicError` implements `std::error::Error`, so `?` works in functions returning `Box<dyn Error>`.

[`examples/error_handling.rs`](examples/error_handling.rs) walks through every
error the crate can hand you — from a call and from the subscription channel —
in one runnable file. The crate docs cover the same ground in
[Error Handling](https://docs.rs/rithmic-rs/latest/rithmic_rs/#error-handling).

## Connection Strategies

Three strategies for initial connection:

- **`Simple`**: Single attempt, fast-fail
- **`Retry`**: Linear backoff (500 ms more per attempt, capped at 60 seconds, jittered ±50%) (recommended default)
- **`AlternateWithRetry`**: Alternates between primary and alt URLs

### Reconnection

If you need to handle disconnections and automatically reconnect, you must implement your own reconnection loop. See [`examples/reconnect.rs`](examples/reconnect.rs) for a complete example that tracks subscriptions and re-subscribes after reconnect.

## Feature Flags

| Flag | Default | What it adds |
|---|---|---|
| `serde` | off | `Serialize`/`Deserialize` on the config types, the trading enums, every order command and the history request types — enough to persist and replay a command |

The crate uses `native-tls` (via `tokio-tungstenite`) for all WebSocket
connections. There is no `rustls` option.

MSRV is 1.85 (edition 2024).

## Examples

Every example is runnable against a Demo account once `.env` is filled in from
[`examples/.env.blank`](examples/.env.blank).

| Example | Shows |
|---|---|
| [`connect.rs`](examples/connect.rs) | Connect, log in, disconnect |
| [`ticker.rs`](examples/ticker.rs) | Streaming quotes and trades |
| [`bracket_order.rs`](examples/bracket_order.rs) | Placing a bracket and reading order notifications |
| [`trade_routes.rs`](examples/trade_routes.rs) | Inspecting the routes orders will take |
| [`load_historical_bars.rs`](examples/load_historical_bars.rs) | Time bar replay |
| [`load_historical_ticks.rs`](examples/load_historical_ticks.rs) | Tick replay |
| [`pnl.rs`](examples/pnl.rs) | Position and P&L updates |
| [`error_handling.rs`](examples/error_handling.rs) | Every error the crate can hand you, in one file |
| [`reconnect.rs`](examples/reconnect.rs) | A reconnection loop that restores subscriptions |

```sh
cargo run --example ticker
```

## Version History

[CHANGELOG.md](CHANGELOG.md) has the full history. Coming from 2.x, start at
[Migrating from 2.x](#migrating-from-2x).

## Contribution

Contributions encouraged and welcomed!

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as below, without any additional terms or conditions.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
