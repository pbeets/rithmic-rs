# Rust Rithmic R | Protocol API client

Unofficial rust client for connecting to Rithmic's R | Protocol API.

[![Crates.io](https://img.shields.io/crates/v/rithmic-rs.svg)](https://crates.io/crates/rithmic-rs)
[![Documentation](https://docs.rs/rithmic-rs/badge.svg)](https://docs.rs/rithmic-rs)

[Documentation](https://docs.rs/rithmic-rs/latest/rithmic_rs/) | [Rithmic APIs](https://www.rithmic.com/apis)

Not all functionality has been implemented, but this is currently being used to trade live capital through Rithmic.

Only `order_plant`, `ticker_plant`, `pnl_plant`, `history_plant` are provided. Each plant uses the actor pattern so you'll want to start a plant, and communicate / call commands with it using it's handle. The crate is setup to be used with tokio channels.

The `history_plant` supports loading both historical tick data and time bar data (1-second, 1-minute, 5-minute, daily, and weekly bars).

## Installation

You can install it from crates.io

```
$ cargo add rithmic-rs
```

Or manually add it to your `Cargo.toml` file.

```
[dependencies]
rithmic-rs = "0.6.1"
```

## Usage

### Configuration

Rithmic supports three types of account environments: `RithmicEnv::Demo` for paper trading, `RithmicEnv::Live` for funded accounts, and `RithmicEnv::Test` for the test environment before app approval.

Configure your connection using the builder pattern:

```rust
use rithmic_rs::{RithmicConfig, RithmicEnv};

let config = RithmicConfig::builder()
    .user("your_username".to_string())
    .password("your_password".to_string())
    .system_name("Rithmic Paper Trading".to_string())
    .env(RithmicEnv::Demo)
    .build()?;
```

Alternatively, you can load from environment variables using `from_env()`:

```rust
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
```

Required environment variables for `from_env()`:
```sh
# For Demo environment
RITHMIC_DEMO_ACCOUNT_ID=your_account_id
RITHMIC_DEMO_FCM_ID=your_fcm_id
RITHMIC_DEMO_IB_ID=your_ib_id
RITHMIC_DEMO_USER=your_username
RITHMIC_DEMO_PW=your_password
RITHMIC_DEMO_URL=<provided_by_rithmic>
RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>

# For Live environment
RITHMIC_LIVE_ACCOUNT_ID=your_account_id
RITHMIC_LIVE_FCM_ID=your_fcm_id
RITHMIC_LIVE_IB_ID=your_ib_id
RITHMIC_LIVE_USER=your_username
RITHMIC_LIVE_PW=your_password
RITHMIC_LIVE_URL=<provided_by_rithmic>
RITHMIC_LIVE_ALT_URL=<provided_by_rithmic>

# For Test environment
RITHMIC_TEST_ACCOUNT_ID=your_account_id
RITHMIC_TEST_FCM_ID=your_fcm_id
RITHMIC_TEST_IB_ID=your_ib_id
RITHMIC_TEST_USER=your_username
RITHMIC_TEST_PW=your_password
RITHMIC_TEST_URL=<provided_by_rithmic>
RITHMIC_TEST_ALT_URL=<provided_by_rithmic>
```

See `examples/.env.blank` for a template with all required variables and connection URLs.

### Connection Strategies

The library provides three connection strategies:
- **`Simple`**: Single connection attempt (recommended default, fast-fail)
- **`Retry`**: Indefinite retries with exponential backoff capped at 60 seconds
- **`AlternateWithRetry`**: Alternates between primary and beta URLs with retries

### Quick Start

To use this crate, create a configuration and connect to a plant with your chosen strategy. Each plant uses the actor pattern and spawns a task that listens to commands via a handle. Plants like the ticker plant also include a broadcast channel for real-time updates.

```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};

async fn stream_live_ticks() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    // Connect with retry strategy (indefinite retries with 60s max backoff)
    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = ticker_plant.get_handle();

    // Login and subscribe
    handle.login().await?;
    handle.subscribe("ESU5", "CME").await?;

    // Process real-time updates
    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                // Check for errors on all messages
                if let Some(error) = &update.error {
                    eprintln!("Error from {}: {}", update.source, error);
                }

                // Handle connection health issues
                match update.message {
                    RithmicMessage::HeartbeatTimeout => {
                        eprintln!("Connection timeout - must reconnect");
                        break;
                    }
                    RithmicMessage::ForcedLogout(_) => {
                        eprintln!("Forced logout - must reconnect");
                        break;
                    }
                    RithmicMessage::ConnectionError => {
                        eprintln!("Connection error - must reconnect");
                        break;
                    }
                    _ => {}
                }

                // Process market data
                match update.message {
                    RithmicMessage::LastTrade(trade) => {
                        println!("Trade: {} @ {}", trade.trade_size.unwrap(), trade.trade_price.unwrap());
                    }
                    RithmicMessage::BestBidOffer(bbo) => {
                        println!("BBO: {}@{} / {}@{}",
                            bbo.bid_size.unwrap(), bbo.bid_price.unwrap(),
                            bbo.ask_price.unwrap(), bbo.ask_size.unwrap());
                    }
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("Channel error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
```

## Examples

The repository includes several examples to help you get started. Examples use environment variables for configuration - set the required variables (listed above) in your environment or use a `.env` file.

### Basic Connection
```bash
cargo run --example connect
```

### Market Data
```bash
# Stream live market data
cargo run --example market_data
```

### Historical Data
```bash
# Load historical tick data
cargo run --example load_historical_ticks

# Load historical time bars (1-minute, 5-minute, daily)
cargo run --example load_historical_bars
```

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
