//! Keep a ticker subscription alive across dropped connections: reconnect with
//! exponential backoff, re-subscribe, and exit only when login is refused.
//! Runs until Ctrl-C or a fatal login error.
//!
//! Run with: cargo run --example reconnect
//! Env: SYMBOL, EXCHANGE (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tokio::{sync::broadcast::error::RecvError, time::sleep};
use tracing::{error, info, warn};

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicError, RithmicTickerPlant,
    RithmicTickerPlantHandle, rti::messages::RithmicMessage,
};

const ENV: RithmicEnv = RithmicEnv::Demo;

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
const STABLE_SESSION_THRESHOLD: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(ENV)?;
    let exchange = common::exchange();
    let symbol = common::symbol();

    let subscriptions: HashSet<(String, String)> = HashSet::from([(symbol, exchange)]);
    let mut backoff = BACKOFF_MIN;

    loop {
        let plant = match RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await {
            Ok(p) => p,

            Err(e) => {
                error!("Connect failed: {e}");
                sleep_with_backoff(&mut backoff).await;
                continue;
            }
        };

        let mut handle = plant.get_handle();

        if let Err(e) = handle.login().await {
            if e.is_connection_issue() {
                warn!("Login failed (connection issue): {e}");
                shutdown_plant(&handle, plant).await;
                sleep_with_backoff(&mut backoff).await;
                continue;
            }

            // Retrying a refused login with the same credentials will not help.
            error!("Login failed (fatal): {e}");
            shutdown_plant(&handle, plant).await;
            return Err(e.into());
        }

        let session_started = Instant::now();
        let mut received_data = false;
        let mut connection_lost = false;

        for (symbol, exchange) in &subscriptions {
            match handle.subscribe(symbol, exchange).await {
                Ok(resp) => match &resp.error {
                    Some(err) => {
                        warn!("Subscribe rejected for {symbol}/{exchange}: {err}, skipping")
                    }
                    None => info!("Subscribed to {symbol} on {exchange}"),
                },

                Err(e) if e.is_connection_issue() => {
                    warn!("Subscribe failed (connection lost): {e}");
                    connection_lost = true;
                    break;
                }

                Err(e) => warn!("Subscribe error for {symbol}/{exchange}: {e}"),
            }
        }

        if connection_lost {
            shutdown_plant(&handle, plant).await;
            sleep_with_backoff(&mut backoff).await;
            continue;
        }

        loop {
            match handle.subscription_receiver.recv().await {
                Ok(update) => match &update.message {
                    RithmicMessage::HeartbeatTimeout
                        if matches!(update.error, Some(RithmicError::RequestRejected(_))) =>
                    {
                        warn!("Heartbeat rejected (connection fine): {:?}", update.error);
                    }

                    RithmicMessage::HeartbeatTimeout
                    | RithmicMessage::ForcedLogout(_)
                    | RithmicMessage::ConnectionError => {
                        warn!("Session lost ({:?}), reconnecting", update.message);
                        break;
                    }

                    RithmicMessage::LastTrade(t) => {
                        received_data = true;

                        info!(
                            "Trade: {} @ {}",
                            t.trade_size.unwrap_or(0),
                            t.trade_price.unwrap_or(0.0)
                        );
                    }

                    _ => {}
                },

                // The missed updates may include the ConnectionError, and the channel
                // stays open while we hold the plant, so reconnect rather than wait.
                Err(RecvError::Lagged(n)) => {
                    warn!("Missed {n} updates, reconnecting");
                    break;
                }

                Err(RecvError::Closed) => {
                    warn!("Subscription channel closed, reconnecting");
                    break;
                }
            }
        }

        if received_data && session_started.elapsed() >= STABLE_SESSION_THRESHOLD {
            backoff = BACKOFF_MIN;
        }

        shutdown_plant(&handle, plant).await;
    }
}

/// Dropping the plant does not stop its actor, so abort it and wait, or the old
/// session stays open next to the new one on the same credentials.
async fn shutdown_plant(handle: &RithmicTickerPlantHandle, plant: RithmicTickerPlant) {
    handle.abort();
    let _ = plant.await_shutdown().await;
}

/// Sleep for the current backoff, then double it (capped at BACKOFF_MAX).
async fn sleep_with_backoff(backoff: &mut Duration) {
    info!("Reconnecting in {:?}", backoff);
    sleep(*backoff).await;
    *backoff = (*backoff * 2).min(BACKOFF_MAX);
}
