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
rithmic-rs = "0.7.1"
```

Set your environment variables:

```sh
RITHMIC_DEMO_ACCOUNT_ID=your_account_id
RITHMIC_DEMO_FCM_ID=your_fcm_id
RITHMIC_DEMO_IB_ID=your_ib_id
RITHMIC_DEMO_USER=your_username
RITHMIC_DEMO_PW=your_password
RITHMIC_DEMO_URL=<provided_by_rithmic>
RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>

# See examples/.env.blank for Live
```

Stream live market data:

```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?; // for live RithmicEnv::Live
    let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
    let mut handle = plant.get_handle();

    handle.login().await?;
    handle.subscribe("ESU5", "CME").await?;

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
handle.subscribe("ESU5", "CME").await?;

// Symbol discovery
let symbols = handle.search_symbols("ES", Some("CME"), None, None, None).await?;
let front_month = handle.get_front_month_contract("ES", "CME", false).await?;
```

### Order Plant

```rust
// Place orders
handle.place_limit_order("ESU5", "CME", Side::Buy, 5000.0, 1).await?;
handle.place_bracket_order(...).await?;

// Manage positions
handle.cancel_order(order_id).await?;
handle.exit_position("ESU5", "CME").await?;
```

### History Plant

```rust
// Load historical data (bar_type, period, start_time, end_time as i32 unix seconds)
let bars = handle.load_time_bars("ESU5", "CME", BarType::MinuteBar, 5, start, end).await?;
let ticks = handle.load_ticks("ESU5", "CME", start, end).await?;
```

### PnL Plant

```rust
// Monitor P&L
handle.subscribe_pnl_updates().await?;
let snapshot = handle.pnl_position_snapshots().await?;
```

## Connection Strategies

Three strategies for initial connection:
- **`Simple`**: Single attempt, fast-fail (recommended default)
- **`Retry`**: Exponential backoff, capped at 60 seconds
- **`AlternateWithRetry`**: Alternates between primary and alt URLs

### Reconnection

If you need to handle disconnections and automatically reconnect, you must implement your own reconnection loop. See [`examples/reconnect.rs`](examples/reconnect.rs) for a complete example that tracks subscriptions and re-subscribes after reconnect.

## Upgrading

See [CHANGELOG.md](CHANGELOG.md) for version history and [MIGRATION_0.6.0.md](MIGRATION_0.6.0.md) for migration guides.

## Contribution

Contributions encouraged and welcomed!

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as below, without any additional terms or conditions.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
