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

    // Get the front month contract and subscribe to it, so the example stays
    // valid as contracts roll. SYMBOL overrides the discovery when set.
    let front_month = handle
        .get_front_month_contract(&product, &exchange, false)
        .await?;
    info!("Front month: {:?}", front_month);

    let discovered = match &front_month.message {
        RithmicMessage::ResponseFrontMonthContract(fm) => fm.trading_symbol.clone(),
        _ => None,
    };

    // Subscribe to market data. A server rejection comes back as `Ok` with
    // `error` set, so check it — `?` alone only catches transport failures.
    let symbol = env::var("SYMBOL")
        .ok()
        .or(discovered)
        .ok_or("no front month contract found — set SYMBOL")?;

    let resp = handle.subscribe(&symbol, &exchange).await?;
    if let Some(err) = &resp.error {
        return Err(format!("subscribe rejected: {err}").into());
    }

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
