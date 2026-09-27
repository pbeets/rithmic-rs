//! Setup check: connect to the ticker plant and log in. If this works, your
//! credentials and environment are right.
//!
//! Run with: cargo run --example connect

use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant};
use tracing::info;

const ENV: RithmicEnv = RithmicEnv::Demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(ENV)?;

    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = ticker_plant.get_handle();

    let resp = handle.login().await?;
    info!("Login response: {:#?}", resp);

    handle.disconnect().await?;
    info!("Disconnected from Rithmic");
    Ok(())
}
