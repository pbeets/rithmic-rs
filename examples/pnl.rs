//! Take a P&L snapshot for the account, then stream P&L updates.
//!
//! Run with: cargo run --example pnl

use tracing::{error, info, warn};

use tokio::{
    sync::broadcast::error::RecvError,
    time::{Duration, Instant, timeout_at},
};

use rithmic_rs::{
    ConnectStrategy, RithmicAccount, RithmicConfig, RithmicEnv, RithmicError, RithmicPnlPlant,
    rti::messages::RithmicMessage,
};

const ENV: RithmicEnv = RithmicEnv::Demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(ENV)?;
    let account = RithmicAccount::from_env(ENV)?;

    let pnl_plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = pnl_plant.get_handle(&account);
    handle.login().await?;

    let snapshot = handle.get_pnl_position_snapshot().await?;
    info!("Position snapshot: {:?}", snapshot);

    let resp = handle.subscribe_pnl_updates().await?;

    if let Some(err) = &resp.error {
        return Err(format!("P&L subscribe rejected: {err}").into());
    }

    // An account with no positions and no fills may send nothing, so an empty run is normal.
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let update = match timeout_at(deadline, handle.subscription_receiver.recv()).await {
            Err(_) => break,
            Ok(Ok(update)) => update,

            // Lagged may have swallowed a ConnectionError; the deadline keeps this loop from hanging.
            Ok(Err(RecvError::Lagged(n))) => {
                warn!("missed {n} updates");
                continue;
            }

            Ok(Err(RecvError::Closed)) => break,
        };

        match &update.message {
            RithmicMessage::AccountPnLPositionUpdate(pnl) => info!(
                "Account: balance={} open_pnl={} net_qty={}",
                pnl.account_balance.as_deref().unwrap_or("?"),
                pnl.open_position_pnl.as_deref().unwrap_or("?"),
                pnl.net_quantity.unwrap_or(0)
            ),

            RithmicMessage::InstrumentPnLPositionUpdate(pnl) => info!(
                "Instrument: {} day_pnl={:.2} qty={}",
                pnl.symbol.as_deref().unwrap_or("?"),
                pnl.day_pnl.unwrap_or(0.0),
                pnl.open_position_quantity.unwrap_or(0)
            ),

            // A rejected heartbeat leaves the connection up; any other timeout ends it.
            RithmicMessage::HeartbeatTimeout
                if matches!(update.error, Some(RithmicError::RequestRejected(_))) =>
            {
                warn!("server rejected a heartbeat, connection is fine");
            }

            RithmicMessage::ConnectionError | RithmicMessage::HeartbeatTimeout => {
                error!("connection lost: {:?}", update.error);
                break;
            }

            _ => {}
        }
    }

    handle.disconnect().await?;
    Ok(())
}
