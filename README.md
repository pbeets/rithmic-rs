# Rust Rithmic R | Protocol API client

[![Crates.io](https://img.shields.io/crates/v/rithmic-rs.svg)](https://crates.io/crates/rithmic-rs)
[![docs.rs](https://img.shields.io/docsrs/rithmic-rs)](https://docs.rs/rithmic-rs)
[![CI](https://github.com/pbeets/rithmic-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/pbeets/rithmic-rs/actions)
[![License](https://img.shields.io/crates/l/rithmic-rs)](LICENSE-MIT)

[Official Rithmic API](https://www.rithmic.com/apis)

Unofficial rust client for connecting to Rithmic's R | Protocol API.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rithmic-rs = "2.0.0"
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

# See examples/.env.blank for Live
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

See [`examples/`](examples/) for more usage patterns including [reconnection handling](examples/reconnect.rs) and historical data loading.

## Architecture

This library uses the actor pattern where each Rithmic service runs independently in its own thread. All communication happens through tokio channels.

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
    ConnectStrategy, NewOrderPriceType, NewOrderTransactionType, RithmicAccount,
    RithmicConfig, RithmicEnv, RithmicOrder, RithmicOrderPlant,
};

let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
let mut handle = plant.get_handle(&account);
handle.login().await?;

// Place orders using the RithmicOrder API
let order = RithmicOrder {
    symbol: "ESM6".to_string(),
    exchange: "CME".to_string(),
    quantity: 1,
    price: 5000.0,
    transaction_type: NewOrderTransactionType::Buy,
    price_type: NewOrderPriceType::Limit,
    user_tag: "my-order".to_string(),
    ..Default::default()
};
handle.place_order(order).await?;

// Bracket orders, OCO orders, advanced bracket orders
handle.place_bracket_order(...).await?;
handle.place_advanced_bracket_order(advanced_order).await?;

// Manage positions
handle.cancel_order(order_id).await?;
handle.exit_position("ESM6", "CME").await?;
```

For multi-account workflows, create one [`RithmicAccount`] per account and call
`get_handle(&account)` for each handle you need.

### History Plant

```rust
// Load historical data (bar_type, period, start_time, end_time as i32 unix seconds)
let bars = handle.load_time_bars("ESM6", "CME", BarType::MinuteBar, 5, start, end).await?;
let ticks = handle.load_ticks("ESM6", "CME", start, end).await?;

// Load N-tick bars (e.g., 5-tick bars)
let tick_bars = handle.load_tick_bars("ESM6", "CME", 5, start, end).await?;
```

### PnL Plant

```rust
use rithmic_rs::{
    ConnectStrategy, RithmicAccount, RithmicConfig, RithmicEnv, RithmicPnlPlant,
};

let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
let plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Retry).await?;
let mut handle = plant.get_handle(&account);
handle.login().await?;

// Monitor P&L
handle.subscribe_pnl_updates().await?;
let snapshot = handle.pnl_position_snapshots().await?;
```

## Error Handling

All plant handle methods return `Result<_, RithmicError>` with typed variants you can match on for recovery decisions:

```rust
use rithmic_rs::RithmicError;

match handle.subscribe("ESM6", "CME").await {
    Ok(resp) => { /* success */ }
    Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
        handle.abort();
        // reconnect — see examples/reconnect.rs
    }
    Err(RithmicError::InvalidArgument(msg)) => eprintln!("Bad argument: {}", msg),
    Err(RithmicError::ServerError(msg)) => eprintln!("Server rejected: {}", msg),
    Err(e) => eprintln!("{}", e),
}
```

`RithmicError` implements `std::error::Error`, so `?` works in functions returning `Box<dyn Error>`.

## Connection Strategies

Three strategies for initial connection:

- **`Simple`**: Single attempt, fast-fail
- **`Retry`**: Exponential backoff, capped at 60 seconds (recommended default)
- **`AlternateWithRetry`**: Alternates between primary and alt URLs

### Reconnection

If you need to handle disconnections and automatically reconnect, you must implement your own reconnection loop. See [`examples/reconnect.rs`](examples/reconnect.rs) for a complete example that tracks subscriptions and re-subscribes after reconnect.

## Upgrading

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Contribution

Contributions encouraged and welcomed!

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as below, without any additional terms or conditions.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
