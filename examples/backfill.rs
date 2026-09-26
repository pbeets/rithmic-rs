//! Example: backfill history and check that you got all of it
//!
//! Loads several windows for the front-month contract and prints one line per
//! check: rows, time taken, the last timestamp against the end of the window,
//! and whether the server ended the replay early. When the server cuts a large
//! reply short, the plant's INFO log shows it asking the server to continue.
//!
//! Run with: cargo run --release --example backfill
//!
//! Optional env vars: RITHMIC_ENV (demo, live or test; default demo), PRODUCT
//! (default MNQ), EXCHANGE (default CME), SYMBOL (a contract to use instead of
//! the front month), TIMEOUT_SECS (per check, default 300)

use std::env;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};
use tracing::info;

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicError, RithmicHistoryPlant, RithmicResponse,
    RithmicTickerPlant, TimeBarType, VolumeProfileMinuteBarsRequest, rti::messages::RithmicMessage,
};

const DAY: i32 = 24 * 60 * 60;

/// Unix seconds now. Rithmic uses `i32`, which overflows in 2038.
fn now_secs() -> i32 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|d| i32::try_from(d.as_secs()).ok())
        .unwrap_or(0)
}

/// The date of Unix time `secs` as `YYYYMMDD`, the form daily and weekly bars
/// use for their window.
fn yyyymmdd(secs: i32) -> i32 {
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = i64::from(secs).div_euclid(i64::from(DAY)) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);

    (year * 10_000 + month * 100 + day) as i32
}

/// The time a data frame covers: a bar's `marker`, or a tick's close time.
/// `None` for the end marker and anything else without data.
fn data_time(response: &RithmicResponse) -> Option<i32> {
    match &response.message {
        RithmicMessage::ResponseTimeBarReplay(bar) => bar.marker,
        RithmicMessage::ResponseVolumeProfileMinuteBars(bar) => bar.marker,
        RithmicMessage::ResponseTickBarReplay(tick) => tick.data_bar_ssboe.last().copied(),
        _ => None,
    }
}

/// How a window's end and its bars' times are written.
#[derive(Clone, Copy)]
enum Times {
    UnixSeconds,
    Dates,
}

/// Run one load with a deadline, print a verdict line, and return the time of
/// its last data frame.
async fn check(
    name: &str,
    window_end: i32,
    times: Times,
    timeout: Duration,
    load: impl Future<Output = Result<Vec<RithmicResponse>, RithmicError>>,
) -> Option<i32> {
    let started = Instant::now();
    let result = tokio::time::timeout(timeout, load).await;
    let elapsed = started.elapsed().as_secs_f64();

    let responses = match result {
        Err(_) => {
            info!("FAIL  {name}: no reply within {timeout:?}");
            return None;
        }
        Ok(Err(error)) => {
            info!("FAIL  {name}: {error} after {elapsed:.1}s");
            return None;
        }
        Ok(Ok(responses)) => responses,
    };

    let rows = responses.iter().filter(|r| data_time(r).is_some()).count();
    let last = responses.iter().rev().find_map(data_time);
    let ended_early = responses.last().and_then(|r| r.error.as_ref());

    let coverage = match (last, times) {
        (None, _) => "no data".to_string(),
        (Some(last), Times::UnixSeconds) => {
            let short_by = f64::from(window_end - last) / 3_600.0;
            format!("last data {short_by:.1}h before the window end")
        }
        (Some(last), Times::Dates) => format!("last bar {last}, window ends {window_end}"),
    };

    match ended_early {
        Some(error) => info!(
            "CHECK {name}: {rows} rows in {elapsed:.1}s, {coverage}; the server ended it early: {error}"
        ),
        None => info!("OK    {name}: {rows} rows in {elapsed:.1}s, {coverage}"),
    }

    last
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let rithmic_env: RithmicEnv = env::var("RITHMIC_ENV")
        .ok()
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(RithmicEnv::Demo);
    let config = RithmicConfig::from_env(rithmic_env)?;

    let product = env::var("PRODUCT").unwrap_or_else(|_| "MNQ".to_string());
    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string());
    let timeout = Duration::from_secs(
        env::var("TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300),
    );

    // Contracts roll, so ask the ticker plant for the front month unless a
    // contract was given.
    let symbol = match env::var("SYMBOL") {
        Ok(symbol) => symbol,
        Err(_) => {
            let ticker = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
            let handle = ticker.get_handle();
            handle.login().await?;

            let response = handle
                .get_front_month_contract(&product, &exchange, false)
                .await?;
            handle.disconnect().await?;

            match &response.message {
                RithmicMessage::ResponseFrontMonthContract(fm) => fm.trading_symbol.clone(),
                _ => None,
            }
            .ok_or_else(|| format!("no front month for {product} on {exchange}; set SYMBOL"))?
        }
    };

    let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Retry).await?;
    let history = history_plant.get_handle();
    history.login().await?;

    let now = now_secs();
    info!("Backfilling {symbol} on {exchange}");

    // 1. Large enough to pass the 10,000-record cap.
    let last_minute = check(
        "30 days of 1-minute bars",
        now,
        Times::UnixSeconds,
        timeout,
        history.load_time_bars_all(
            symbol.clone(),
            exchange.clone(),
            TimeBarType::MinuteBar,
            1,
            now - 30 * DAY,
            now,
        ),
    )
    .await;

    // 2. Large enough that the server cuts the reply short; the log above each
    //    verdict shows the plant asking it to continue.
    let volume_profile = |days: i32| {
        VolumeProfileMinuteBarsRequest::new()
            .symbol(&symbol)
            .exchange(&exchange)
            .bar_type_period(1)
            .start_time_sec(now - days * DAY)
            .end_time_sec(now)
            .resume_bars(true)
    };
    check(
        "7 days of 1-minute volume profile",
        now,
        Times::UnixSeconds,
        timeout,
        history.load_volume_profile_minute_bars(volume_profile(7)),
    )
    .await;

    // 3. Every trade in the 4 hours up to the last minute bar, so the window
    //    has trading in it even on a weekend.
    let tick_end = last_minute.unwrap_or(now);
    check(
        "4 hours of ticks",
        tick_end,
        Times::UnixSeconds,
        timeout,
        history.load_ticks_all(
            symbol.clone(),
            exchange.clone(),
            tick_end - 4 * 60 * 60,
            tick_end,
        ),
    )
    .await;

    // 4. Daily bars take their window as YYYYMMDD dates, not Unix seconds.
    let (from, to) = (yyyymmdd(now - 100 * DAY), yyyymmdd(now));
    check(
        "100 days of daily bars",
        to,
        Times::Dates,
        timeout,
        history.load_time_bars(
            symbol.clone(),
            exchange.clone(),
            TimeBarType::DailyBar,
            1,
            from,
            to,
        ),
    )
    .await;

    // 5. Giving up on a replay leaves the plant ready for the next request.
    let dropped = tokio::time::timeout(
        Duration::from_secs(2),
        history.load_volume_profile_minute_bars(volume_profile(7)),
    )
    .await;
    info!(
        "      dropped a replay after 2s: {}",
        if dropped.is_err() {
            "gave up mid-replay"
        } else {
            "it finished first, so nothing was dropped"
        }
    );
    check(
        "a request after the dropped replay",
        now,
        Times::UnixSeconds,
        timeout,
        history.load_time_bars(
            symbol.clone(),
            exchange.clone(),
            TimeBarType::MinuteBar,
            1,
            now - DAY,
            now,
        ),
    )
    .await;

    history.disconnect().await?;
    Ok(())
}
