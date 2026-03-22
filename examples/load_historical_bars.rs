//! Example: Load historical time bars
//!
//! Run with: cargo run --example load_historical_bars
//!
//! Optional env vars: SYMBOL, EXCHANGE, START_TIME (unix seconds)

use std::{env, time::SystemTime};
use tracing::info;

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant,
    rti::{messages::RithmicMessage, request_time_bar_replay::BarType},
};

fn default_start_time() -> i32 {
    // Note: Rithmic API uses i32 timestamps. This will overflow in 2038.
    // We use try_into() to safely convert and fall back to a recent timestamp if needed.
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|d| i32::try_from(d.as_secs()).ok())
        .map(|s| s - (24 * 60 * 60))
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let symbol = env::var("SYMBOL").unwrap_or_else(|_| "ESM6".to_string());
    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string());
    let start_time: i32 = env::var("START_TIME")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(default_start_time);
    let end_time = start_time + (23 * 60 * 60);

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = history_plant.get_handle();
    handle.login().await?;

    info!(
        "Loading 5-minute bars for {} from {} to {}",
        symbol, start_time, end_time
    );

    let bars = handle
        .load_time_bars(
            symbol,
            exchange,
            BarType::MinuteBar,
            5,
            start_time,
            end_time,
        )
        .await?;

    info!("Received {} bars", bars.len());

    for r in bars.iter().take(5) {
        if let RithmicMessage::ResponseTimeBarReplay(bar) = &r.message {
            info!("Bar: {:?}", bar);
        }
    }

    handle.disconnect().await?;
    Ok(())
}
