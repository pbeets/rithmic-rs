//! Example: Load historical tick data
//!
//! Run with: cargo run --example load_historical_ticks
//!
//! Optional env vars: SYMBOL (default: the ES front month), EXCHANGE (default:
//! CME), START_TIME (unix seconds; default: 00:00 UTC on the last weekday).

use std::{env, time::SystemTime};
use tracing::{info, warn};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant, RithmicTickerPlant,
    rti::messages::RithmicMessage,
};

const DAY: i64 = 24 * 60 * 60;

/// 00:00 UTC on the most recent weekday before today, so the 23-hour window
/// falls inside a CME session (Sunday 22:00 to Friday 21:00 UTC).
fn default_start_time() -> i32 {
    // Rithmic uses i32 timestamps, which overflow in 2038.
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let mut day = now / DAY - 1;

    // 1970-01-01 was a Thursday, so (day + 3) % 7 is 0 on Monday.
    while (day + 3) % 7 >= 5 {
        day -= 1;
    }

    i32::try_from(day * DAY).unwrap_or(0)
}

/// The `SYMBOL` env var, or the front month of `ES` from the ticker plant.
async fn symbol_or_front_month(
    config: &RithmicConfig,
    exchange: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(symbol) = env::var("SYMBOL") {
        return Ok(symbol);
    }

    let ticker = RithmicTickerPlant::connect(config, ConnectStrategy::Retry).await?;
    let handle = ticker.get_handle();
    handle.login().await?;
    let response = handle
        .get_front_month_contract("ES", exchange, false)
        .await?;
    handle.disconnect().await?;

    match &response.message {
        RithmicMessage::ResponseFrontMonthContract(fm) => fm.trading_symbol.clone(),
        _ => None,
    }
    .ok_or_else(|| format!("no front month for ES on {exchange}").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string());
    let start_time: i32 = env::var("START_TIME")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(default_start_time);
    let end_time = start_time + (23 * 60 * 60);

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let symbol = symbol_or_front_month(&config, &exchange).await?;
    let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = history_plant.get_handle();
    handle.login().await?;

    info!(
        "Loading ticks for {} from {} to {}",
        symbol, start_time, end_time
    );

    // `load_ticks_all` sets `resume_bars`, which lifts the server's 10,000
    // record cap, so the whole window arrives in one request. `load_ticks`
    // leaves the cap in place and returns at most 10,000 records.
    let ticks = handle
        .load_ticks_all(symbol, exchange, start_time, end_time)
        .await?;

    // A replay the server ends early still returns `Ok`; the reason is on the
    // last frame.
    if let Some(error) = ticks.last().and_then(|r| r.error.as_ref()) {
        warn!("The server ended the replay early: {error}");
    }

    // Every replay ends with a marker frame that carries no tick, so
    // `ticks.len()` would count one too many.
    let ticks: Vec<_> = ticks
        .iter()
        .filter_map(|r| match &r.message {
            RithmicMessage::ResponseTickBarReplay(tick) if !tick.data_bar_ssboe.is_empty() => {
                Some(tick)
            }
            _ => None,
        })
        .collect();

    info!("Received {} ticks", ticks.len());

    for tick in ticks.iter().take(5) {
        info!("Tick: {:?}", tick);
    }

    handle.disconnect().await?;
    Ok(())
}
