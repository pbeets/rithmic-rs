//! Stream trades and best bid/offer for one contract from the ticker plant.
//!
//! Run with: cargo run --example ticker
//! Env: SYMBOL, PRODUCT, EXCHANGE (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tracing::{error, info, warn};

use tokio::{
    sync::broadcast::error::RecvError,
    time::{Duration, Instant, timeout_at},
};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicError, RithmicTickerPlant,
    rti::messages::RithmicMessage,
};

const ENV: RithmicEnv = RithmicEnv::Demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(ENV)?;
    let exchange = common::exchange();

    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = ticker_plant.get_handle();
    handle.login().await?;

    let symbol = common::front_month(&handle, &exchange).await?;

    // A server rejection comes back as `Ok` with `error` set, so check it;
    // `?` alone only catches transport failures.
    let resp = handle.subscribe(&symbol, &exchange).await?;

    if let Some(err) = &resp.error {
        return Err(format!("subscribe rejected: {err}").into());
    }

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
            RithmicMessage::LastTrade(t) => info!(
                "Trade: {} @ {}",
                t.trade_size.unwrap_or(0),
                t.trade_price.unwrap_or(0.0)
            ),

            RithmicMessage::BestBidOffer(b) => info!(
                "BBO: {}x{} / {}x{}",
                b.bid_size.unwrap_or(0),
                b.bid_price.unwrap_or(0.0),
                b.ask_price.unwrap_or(0.0),
                b.ask_size.unwrap_or(0)
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
