//! Example: Ticker plant - symbol discovery, reference data, and market data
//!
//! Run with: cargo run --example ticker

use std::env;
use tracing::info;

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant, rti::messages::RithmicMessage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = ticker_plant.get_handle();
    handle.login().await?;

    let product = env::var("PRODUCT").unwrap_or_else(|_| "ES".to_string());
    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string());

    // Search for symbols
    let symbols = handle
        .search_symbols(&product, Some(&exchange), None, None, None)
        .await?;
    info!("Found {} symbols for {}", symbols.len(), product);

    // Get the front month contract
    let front_month = handle
        .get_front_month_contract(&product, &exchange, false)
        .await?;
    info!("Front month: {:?}", front_month);

    // Subscribe to market data
    let symbol = env::var("SYMBOL").unwrap_or_else(|_| format!("{}M6", product));
    handle.subscribe(&symbol, &exchange).await?;

    let mut count = 0;
    while count < 10 {
        if let Ok(update) = handle.subscription_receiver.recv().await {
            match &update.message {
                RithmicMessage::LastTrade(t) => {
                    info!(
                        "Trade: {} @ {}",
                        t.trade_size.unwrap_or(0),
                        t.trade_price.unwrap_or(0.0)
                    );
                    count += 1;
                }
                RithmicMessage::BestBidOffer(b) => {
                    info!(
                        "BBO: {}x{} / {}x{}",
                        b.bid_size.unwrap_or(0),
                        b.bid_price.unwrap_or(0.0),
                        b.ask_price.unwrap_or(0.0),
                        b.ask_size.unwrap_or(0)
                    );
                    count += 1;
                }
                _ => {}
            }
        }
    }

    handle.disconnect().await?;
    Ok(())
}
