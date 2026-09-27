//! Loads every trade in a 23-hour window with `load_ticks_all`.
//!
//! Run with: cargo run --example load_historical_ticks
//! Env: SYMBOL, EXCHANGE, START_TIME (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant};
use tracing::{info, warn};

const ENV: RithmicEnv = RithmicEnv::Demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(ENV)?;
    let exchange = common::exchange();
    let symbol = common::symbol();

    let start_time = common::start_time();
    let end_time = start_time + 23 * 60 * 60;

    let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = history_plant.get_handle();
    handle.login().await?;

    info!("Loading ticks for {symbol} from {start_time} to {end_time}");

    // `load_ticks_all` sets `resume_bars`, which lifts the server's 10,000
    // record cap, so the whole window arrives in one request. `load_ticks`
    // leaves the cap in place and returns at most 10,000 records.
    let responses = handle
        .load_ticks_all(symbol, exchange, start_time, end_time)
        .await?;

    let Some((end, ticks)) = responses.split_last() else {
        warn!("empty reply");
        return Ok(());
    };

    // A replay the server ends early still returns `Ok`; the reason is on the end marker.
    if let Some(e) = &end.error {
        warn!("server ended the replay early: {e}");
    }

    info!("Received {} ticks", ticks.len());

    for tick in ticks.iter().take(5) {
        info!("Tick: {:?}", tick.message);
    }

    handle.disconnect().await?;
    Ok(())
}
