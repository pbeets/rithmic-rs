//! Example: Load historical time bars
//!
//! Loads the same window twice: with the positional `load_time_bars_all`, and
//! with `load_time_bar_replay` and a `TimeBarReplayRequest` you build, which
//! reaches fields such as `user_max_count`.
//!
//! Run with: cargo run --example load_historical_bars
//!
//! Optional env vars: SYMBOL (default: the ES front month), EXCHANGE (default:
//! CME), START_TIME (unix seconds; default: 00:00 UTC on the last weekday).

use std::{env, time::SystemTime};
use tracing::{info, warn};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant, RithmicResponse,
    RithmicTickerPlant, TimeBarReplayRequest, TimeBarType,
    rti::{ResponseTimeBarReplay, messages::RithmicMessage},
};

/// The bars in a reply. Every replay ends with a marker frame that carries no
/// bar, so counting `responses.len()` is one too many.
fn bars_only(responses: &[RithmicResponse]) -> Vec<&ResponseTimeBarReplay> {
    responses
        .iter()
        .filter_map(|r| match &r.message {
            RithmicMessage::ResponseTimeBarReplay(bar) if bar.marker.is_some() => Some(bar),
            _ => None,
        })
        .collect()
}

/// A replay the server ends early still returns `Ok`; the reason is on the
/// last frame.
fn log_early_end(responses: &[RithmicResponse]) {
    if let Some(error) = responses.last().and_then(|r| r.error.as_ref()) {
        warn!("The server ended the replay early: {error}");
    }
}

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
    log_early_end(&bars);
    let bars = bars_only(&bars);

    info!("Received {} bars", bars.len());

    for bar in bars.iter().take(5) {
        info!("Bar: {:?}", bar);
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
    log_early_end(&minute_bars);
    let minute_bars = bars_only(&minute_bars);

    info!("Received {} bars", minute_bars.len());

    for bar in minute_bars.iter().take(5) {
        info!("Bar: {:?}", bar);
    }

    handle.disconnect().await?;
    Ok(())
}
