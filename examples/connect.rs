//! Example: Connect to the RithmicTickerPlant
use tracing::info;

use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Create configuration from environment variables
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    tracing_subscriber::fmt().init();

    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let ticker_plant_handle = ticker_plant.get_handle();

    let resp = ticker_plant_handle.login().await?;

    info!("Login response: {:#?}", resp);

    ticker_plant_handle.disconnect().await?;

    info!("Disconnected from Rithmic");

    Ok(())
}
