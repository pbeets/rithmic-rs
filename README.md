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
use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant,
    rti::messages::RithmicMessage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = plant.get_handle();

    handle.login().await?;
    handle.subscribe("ESZ6", "CME").await?; // use the current front month

    while let Ok(update) = handle.subscription_receiver.recv().await {
        match update.message {
            RithmicMessage::LastTrade(t) => println!("trade {:?} @ {:?}", t.trade_size, t.trade_price),
            RithmicMessage::BestBidOffer(q) => println!("bid {:?} ask {:?}", q.bid_price, q.ask_price),
            _ => {}
        }
    }

    Ok(())
}
```

The [examples](examples/README.md) cover order routing, historical data, error handling
and reconnection.

### Connection strategies

`connect` takes one of three strategies:

| Strategy | Behavior |
|---|---|
| `Simple` | One attempt. Fails at once with `RithmicError::ConnectionFailed`. |
| `Retry` | Retries `url` with backoff: 500 ms more per attempt, capped at 60 s, jittered ±50%. The recommended default. |
| `AlternateWithRetry` | Like `Retry`, but alternates between `url` and `beta_url`. |

The retrying strategies keep trying forever unless you set `retry_timeout` on
the config builder. Once connected, the crate does not reconnect for you: see
[`examples/reconnect.rs`](examples/reconnect.rs) for a loop that restores
subscriptions.

## Architecture

This library uses the actor pattern where each Rithmic service runs independently as its own tokio task. All communication happens through tokio channels.

- [**`RithmicTickerPlant`**](#ticker-plant) - Real-time market data (trades, quotes, order book)
- [**`RithmicOrderPlant`**](#order-plant) - Order entry and management
- [**`RithmicHistoryPlant`**](#history-plant) - Historical tick and bar data
- [**`RithmicPnlPlant`**](#pnl-plant) - Position and P&L tracking

> [!NOTE]
> Live updates arrive on each handle's `subscription_receiver`, which holds 10,000
> messages by default. A reader that falls further behind misses messages and gets
> `RecvError::Lagged`. Change the size with `subscription_capacity` on the config
> builder; the [crate docs](https://docs.rs/rithmic-rs/latest/rithmic_rs/#subscription-channels)
> have details.

### Ticker Plant

```rust
// Subscribe to real-time quotes
handle.subscribe("ESZ6", "CME").await?;

// Unsubscribe when done
handle.unsubscribe("ESZ6", "CME").await?;

// Additional market data subscriptions
handle.subscribe_instrument_status("ESZ6", "CME").await?;
handle.subscribe_open_interest("ESZ6", "CME").await?;
handle.subscribe_session_prices("ESZ6", "CME").await?;
handle.subscribe_order_price_limits("ESZ6", "CME").await?;

// Symbol discovery
let symbols = handle.search_symbols("ES", Some("CME"), None, None, None).await?;
let front_month = handle.get_front_month_contract("ES", "CME", false).await?;
```

### Order Plant

```rust
use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicBracketOrder, RithmicConfig,
    RithmicEnv, RithmicOrder, RithmicOrderPlant,
};

let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
let handle = plant.get_handle(&account);

handle.login().await?;
handle.subscribe_order_updates().await?;

let order = RithmicOrder::new()
    .symbol("ESZ6")
    .exchange("CME")
    .quantity(1)
    .transaction_type(OrderSide::Buy)
    .price_type(OrderType::Limit)
    .price(5000.0)
    .build()?;
handle.place_order(order).await?;

// Set quantity before target/stop: they size their legs from it.
let bracket = RithmicBracketOrder::new()
    .symbol("ESZ6")
    .exchange("CME")
    .quantity(1)
    .action(OrderSide::Buy)
    .price_type(OrderType::Limit)
    .price(5000.0)
    .target(20)
    .stop(10)
    .build()?;
handle.place_bracket_order(bracket).await?;
```

Fills and status changes arrive on `handle.subscription_receiver`, not in the
reply to the call. Cancels, OCO orders, trade routes and account queries are in
the [`RithmicOrderPlant` docs](https://docs.rs/rithmic-rs/latest/rithmic_rs/plants/order_plant/struct.RithmicOrderPlant.html).

### History Plant

```rust
use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant, TimeBarType};

let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
let plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
let handle = plant.get_handle();
handle.login().await?;

// Unix seconds as i32. Daily and weekly bars take YYYYMMDD dates instead.
let end = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() as i32;
let start = end - 3600;

let (symbol, exchange) = ("ESZ6".to_string(), "CME".to_string());
let bars = handle
    .load_time_bars_all(symbol.clone(), exchange.clone(), TimeBarType::MinuteBar, 5, start, end)
    .await?;
let ticks = handle.load_ticks_all(symbol, exchange, start, end).await?;
```

- Use the `_all` loaders. The plain ones stop at 10,000 records without saying so.
- Requests never time out on their own. Wrap large loads in `tokio::time::timeout`.

Other loaders, replay limits and how to backfill large windows are in the
[`RithmicHistoryPlant` docs](https://docs.rs/rithmic-rs/latest/rithmic_rs/plants/history_plant/struct.RithmicHistoryPlant.html).

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

// Both calls return only an acknowledgement; positions arrive on the receiver.
handle.get_pnl_position_snapshot().await?;
handle.subscribe_pnl_updates().await?;
```

## Migrating from 2.x

3.0 reworked the order API. [MIGRATING.md](MIGRATING.md) has before/after code for every change. The ones that matter most:

- Order commands are built with `::new()`, setters and `build()`, and every order call takes one.
- Orders use the exchange's published trade route. With no route, nothing is sent and you get `RithmicError::NoTradeRoute`.
- Plain history loaders stop at 10,000 records without saying so. Use the `_all` loaders.

## Error Handling

A request the server turns down still returns `Ok`, with the reason in
`resp.error`. `Err` means no answer came back: bad arguments, no trade route,
or a dropped connection.

```rust
match handle.subscribe("ESZ6", "CME").await {
    Ok(resp) => match &resp.error {
        Some(err) => eprintln!("rejected: {err}"),
        None => println!("subscribed"),
    },
    Err(e) if e.is_connection_issue() => { /* reconnect, see examples/reconnect.rs */ }
    Err(e) => eprintln!("{e}"),
}
```

A rejected `login()` and a history replay the server refuses to continue
return `Err(RithmicError::RequestRejected)` instead. Connection drops, order rejections and
dropped updates arrive on `subscription_receiver`.
[`examples/error_handling.rs`](examples/error_handling.rs) covers each case,
and the [crate docs](https://docs.rs/rithmic-rs/latest/rithmic_rs/#error-handling)
explain what to do about them.

## Feature Flags

| Flag | Default | What it adds |
|---|---|---|
| `serde` | off | `Serialize`/`Deserialize` on the config types, the trading enums, every order command and the history request types — enough to persist and replay a command |

The crate uses `native-tls` (via `tokio-tungstenite`) for all WebSocket
connections. There is no `rustls` option.

MSRV is 1.85 (edition 2024).

## Examples

[`examples/`](examples/) has runnable programs for streaming, orders, history,
error handling and reconnection. [`examples/README.md`](examples/README.md)
covers setup and what each one does.

```sh
cp examples/.env.blank .env   # then fill in your Demo credentials
cargo run --example connect
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
