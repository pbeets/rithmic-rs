//! Loads the same window of time bars twice: with the positional
//! `load_time_bars_all`, and with `load_time_bar_replay` and a
//! `TimeBarReplayRequest` you build, which reaches fields such as `user_max_count`.
//!
//! Run with: cargo run --example load_historical_bars
//! Env: SYMBOL, EXCHANGE, START_TIME (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tracing::{info, warn};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant, RithmicResponse,
    TimeBarReplayRequest, TimeBarType,
};

const ENV: RithmicEnv = RithmicEnv::Demo;

fn log_bars(responses: &[RithmicResponse]) {
    let Some((end, bars)) = responses.split_last() else {
        warn!("empty reply");
        return;
    };

    // A replay the server ends early still returns `Ok`; the reason is on the end marker.
    if let Some(e) = &end.error {
        warn!("server ended the replay early: {e}");
    }

    info!("Received {} bars", bars.len());

    for bar in bars.iter().take(5) {
        info!("Bar: {:?}", bar.message);
    }
}

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

    info!("Loading 5-minute bars for {symbol} from {start_time} to {end_time}");

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

    log_bars(&bars);

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
    log_bars(&minute_bars);

    handle.disconnect().await?;
    Ok(())
}
