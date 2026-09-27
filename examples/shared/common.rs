//! Helpers shared by the examples. Cargo does not build this directory as an example.

#![allow(dead_code)]

use rithmic_rs::{RithmicTickerPlantHandle, rti::messages::RithmicMessage};
use std::{env, time::SystemTime};

/// `EXCHANGE`, or `CME`.
pub fn exchange() -> String {
    env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string())
}

/// The contract the examples trade when `SYMBOL` is not set. Only the ticker
/// plant can look up a front month; move this on when ESZ6 expires (2026-12-18).
pub const DEFAULT_SYMBOL: &str = "ESZ6";

/// `SYMBOL`, or [`DEFAULT_SYMBOL`]. For examples that never open a ticker plant.
pub fn symbol() -> String {
    env::var("SYMBOL").unwrap_or_else(|_| DEFAULT_SYMBOL.to_string())
}

/// `SYMBOL`, or the front month of `PRODUCT` (default `ES`), looked up on a
/// ticker plant you have already logged in to.
pub async fn front_month(
    handle: &RithmicTickerPlantHandle,
    exchange: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(symbol) = env::var("SYMBOL") {
        return Ok(symbol);
    }

    let product = env::var("PRODUCT").unwrap_or_else(|_| "ES".to_string());

    let response = handle
        .get_front_month_contract(&product, exchange, false)
        .await?;

    match &response.message {
        RithmicMessage::ResponseFrontMonthContract(fm) => fm.trading_symbol.clone(),
        _ => None,
    }
    .ok_or_else(|| format!("no front month for {product} on {exchange}").into())
}

const DAY: i64 = 24 * 60 * 60;

/// `START_TIME`, or 00:00 UTC on the last weekday before today, so a 23-hour
/// window falls inside a CME session (Sunday 22:00 to Friday 21:00 UTC).
pub fn start_time() -> i32 {
    if let Some(start) = env::var("START_TIME").ok().and_then(|s| s.parse().ok()) {
        return start;
    }

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
