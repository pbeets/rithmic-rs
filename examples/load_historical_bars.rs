//! Example: Load historical time bars
//!
//! Loads the same window twice: with the positional `load_time_bars_all`, and
//! with `load_time_bar_replay` and a `TimeBarReplayRequest` you build, which
//! reaches fields such as `user_max_count`.
//!
//! Run with: cargo run --example load_historical_bars
//!
//! Optional env vars: SYMBOL, EXCHANGE, START_TIME (unix seconds)

use std::{env, time::SystemTime};
use tracing::info;

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant, TimeBarReplayRequest,
    TimeBarType, rti::messages::RithmicMessage,
};

fn default_start_time() -> i32 {
    // Note: Rithmic API uses i32 timestamps. This will overflow in 2038.
    // Falls back to 0 (the epoch) if the clock is unavailable or the seconds
    // no longer fit in i32.
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

    let symbol = env::var("SYMBOL").unwrap_or_else(|_| "ESU6".to_string());
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

    // `load_time_bars_all` sets `resume_bars`, which lifts the server's 10,000
    // record cap, so the whole window arrives in one request. `load_time_bars`
    // leaves the cap in place.
    let bars = handle
        .load_time_bars_all(
            symbol.clone(),
            exchange.clone(),
            TimeBarType::MinuteBar,
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

    // The struct form sends the request exactly as built. Unlike the `_all`
    // loaders it does not set `resume_bars` for you, so set it to lift the
    // 10,000 record cap. `user_max_count` caps the reply at your own limit.
    let request = TimeBarReplayRequest::new()
        .symbol(symbol)
        .exchange(exchange)
        .bar_type(TimeBarType::MinuteBar)
        .bar_type_period(1)
        .start_time_sec(start_time)
        .end_time_sec(end_time)
        .resume_bars(true)
        .user_max_count(100);

    info!("Loading the first 100 one-minute bars of the same window");

    let minute_bars = handle.load_time_bar_replay(request).await?;

    // The last response is the replay's end marker, not a bar.
    info!("Received {} bars", minute_bars.len().saturating_sub(1));

    for r in minute_bars.iter().take(5) {
        if let RithmicMessage::ResponseTimeBarReplay(bar) = &r.message {
            info!("Bar: {:?}", bar);
        }
    }

    handle.disconnect().await?;
    Ok(())
}
