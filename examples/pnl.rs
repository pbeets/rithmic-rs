//! Example: Monitor P&L updates in real-time
//!
//! Run with: cargo run --example pnl

use tracing::info;

use rithmic_rs::{
    ConnectStrategy, RithmicAccount, RithmicConfig, RithmicEnv, RithmicPnlPlant,
    rti::messages::RithmicMessage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
    let pnl_plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = pnl_plant.get_handle(&account);
    handle.login().await?;

    // Get initial snapshot
    let snapshot = handle.get_pnl_position_snapshot().await?;
    info!("Position snapshot: {:?}", snapshot);

    // Subscribe to real-time updates
    handle.subscribe_pnl_updates().await?;

    let mut count = 0;
    while count < 20 {
        if let Ok(update) = handle.subscription_receiver.recv().await {
            match &update.message {
                RithmicMessage::AccountPnLPositionUpdate(pnl) => {
                    info!(
                        "Account: balance={} open_pnl={} net_qty={}",
                        pnl.account_balance.as_deref().unwrap_or("?"),
                        pnl.open_position_pnl.as_deref().unwrap_or("?"),
                        pnl.net_quantity.unwrap_or(0)
                    );
                    count += 1;
                }
                RithmicMessage::InstrumentPnLPositionUpdate(pnl) => {
                    info!(
                        "Instrument: {} day_pnl={:.2} qty={}",
                        pnl.symbol.as_deref().unwrap_or("?"),
                        pnl.day_pnl.unwrap_or(0.0),
                        pnl.open_position_quantity.unwrap_or(0)
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
