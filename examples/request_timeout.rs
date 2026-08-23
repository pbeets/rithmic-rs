//! Example: giving a request a deadline of your own.
//!
//! The crate does not time out requests — a call is resolved when Rithmic
//! answers it or when the connection drops. Wrap it if you want a ceiling.
//!
//! Run with: cargo run --example request_timeout

use std::time::Duration;

use tracing::{error, info, warn};

use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle();

    handle.login().await?;

    // Three outcomes, not two: the wrapper adds one the call itself cannot
    // return.
    let request = handle.get_front_month_contract("ES", "CME", false);

    match tokio::time::timeout(Duration::from_secs(10), request).await {
        Ok(Ok(resp)) => info!("front month: {:?}", resp.message),
        Ok(Err(e)) => error!("front month: {e}"),
        // Nothing was cancelled. The request is still registered, and if the
        // reply arrives it is dropped, because this future owned the receiver.
        Err(_elapsed) => warn!("front month: gave up waiting"),
    }

    // Pick the number per call. A quote lookup that is slow is worth
    // abandoning; a login is worth waiting on.
    let slow = handle.get_front_month_contract("NQ", "CME", false);

    if let Ok(Ok(resp)) = tokio::time::timeout(Duration::from_millis(50), slow).await {
        info!("front month: {:?}", resp.message);
    } else {
        warn!("front month: not worth 50ms");
    }

    // On a write, expiry means the outcome is unknown rather than failed:
    // Rithmic never learned you gave up, so the order may well be working.
    // Reconcile it against the order plant instead of sending it again.

    handle.disconnect().await?;
    Ok(())
}
