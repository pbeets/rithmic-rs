//! Example: persistent connection with automatic reconnection and re-subscription.
//!
//! Sketches a production-shaped connection supervisor:
//!
//! - Transport failures reconnect with exponential backoff (capped at 60s).
//! - `RequestRejected` on **login** is terminal (bad credentials / entitlements) —
//!   retrying risks account lockout, so we log, disconnect, and exit.
//! - `RequestRejected` on **subscribe** is per-symbol (e.g. unknown instrument) —
//!   log it and keep going with the rest of the session.
//! - Backoff resets after a session has produced real data for long enough to be
//!   considered healthy, so a long-lived connection that drops doesn't inherit
//!   a large backoff from earlier failures.
//!
//! A real app would also wire `tokio::signal::ctrl_c()` (requires the `signal`
//! feature on tokio) into the supervisor and call `disconnect().await` on shutdown.
//!
//! Run with: `cargo run --example reconnect`

use std::{
    collections::HashSet,
    env,
    time::{Duration, SystemTime},
};

use tokio::{sync::broadcast::error::RecvError, time::sleep};
use tracing::{error, info, warn};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicError, RithmicTickerPlant,
    rti::messages::RithmicMessage,
};

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
const STABLE_SESSION_THRESHOLD: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let mut subscriptions: HashSet<(String, String)> = HashSet::new();
    let symbol = env::var("SYMBOL").unwrap_or_else(|_| "ESM6".to_string());
    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string());

    subscriptions.insert((symbol, exchange));

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
            match &e {
                RithmicError::ConnectionClosed | RithmicError::SendFailed => {
                    warn!("Login failed (connection issue): {e}");

                    shutdown_plant(&handle, plant).await;
                    sleep_with_backoff(&mut backoff).await;

                    continue;
                }
                RithmicError::RequestRejected(err) => {
                    let code = err.code.as_deref().unwrap_or("?");
                    let msg = err.message.as_deref().unwrap_or("");

                    error!(
                        "Login rejected by server (fatal): code={} msg={}",
                        code, msg
                    );

                    let _ = handle.disconnect().await;
                    let _ = plant.await_shutdown().await;

                    return Err(format!("login rejected: {code} / {msg}").into());
                }
                RithmicError::ProtocolError(msg) => {
                    error!("Login protocol error (fatal): {msg}");

                    let _ = handle.disconnect().await;
                    let _ = plant.await_shutdown().await;

                    return Err(format!("login protocol error: {msg}").into());
                }
                _ => {
                    error!("Login failed: {e}");

                    shutdown_plant(&handle, plant).await;
                    sleep_with_backoff(&mut backoff).await;

                    continue;
                }
            }
        }

        let session_started = SystemTime::now();
        let mut received_data = false;
        let mut connection_lost = false;

        for (symbol, exchange) in &subscriptions {
            match handle.subscribe(symbol, exchange).await {
                Ok(_) => info!("Subscribed to {symbol} on {exchange}"),
                Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
                    warn!("Subscribe failed (connection lost), reconnecting…");

                    connection_lost = true;

                    break;
                }
                Err(RithmicError::RequestRejected(err)) => {
                    warn!(
                        "Subscribe rejected for {symbol}/{exchange}: code={} msg={} — skipping",
                        err.code.as_deref().unwrap_or("?"),
                        err.message.as_deref().unwrap_or(""),
                    );
                }
                Err(e) => warn!("Subscribe error for {symbol}/{exchange}: {e}"),
            }
        }

        if connection_lost {
            shutdown_plant(&handle, plant).await;
            sleep_with_backoff(&mut backoff).await;

            continue;
        }

        // A clean `handle.disconnect().await` on a healthy connection does NOT
        // emit HeartbeatTimeout/ConnectionError, so a shutdown path using it
        // won't trip the reconnect branch below.
        //
        // We handle broadcast RecvError explicitly: `Lagged` means the consumer
        // is falling behind and may have dropped the connection-lost signal
        // through buffer wrap — treat as reconnect-worthy rather than silently
        // exiting. `Closed` means the plant actor is gone, so we also reconnect.
        loop {
            match handle.subscription_receiver.recv().await {
                Ok(update) => match &update.message {
                    RithmicMessage::HeartbeatTimeout
                    | RithmicMessage::ForcedLogout(_)
                    | RithmicMessage::ConnectionError => {
                        warn!("Session lost ({:?}), reconnecting…", update.message);

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
                Err(RecvError::Lagged(skipped)) => {
                    warn!(
                        "Subscription lagged ({} messages dropped) — reconnecting to \
                         resync; a connection-health frame may have been lost",
                        skipped
                    );

                    break;
                }
                Err(RecvError::Closed) => {
                    warn!("Subscription channel closed — reconnecting");

                    break;
                }
            }
        }

        let uptime = session_started.elapsed().unwrap_or_default();

        if received_data && uptime >= STABLE_SESSION_THRESHOLD {
            backoff = BACKOFF_MIN;
        }

        // Tokio's `JoinHandle::drop` detaches the task — it does NOT abort it.
        // Without an explicit abort + await_shutdown, the old actor would stay
        // alive alongside the next connection, producing a short-lived dual
        // session against the same credentials.
        shutdown_plant(&handle, plant).await;
    }
}

/// Abort the plant's background actor and await its join handle so we don't
/// leave an orphaned task holding a TCP/WebSocket session while the next
/// reconnect attempt opens a new one.
async fn shutdown_plant(handle: &rithmic_rs::RithmicTickerPlantHandle, plant: RithmicTickerPlant) {
    handle.abort();

    let _ = plant.await_shutdown().await;
}

/// Sleep for the current backoff, then double it (capped at BACKOFF_MAX).
async fn sleep_with_backoff(backoff: &mut Duration) {
    info!("Reconnecting in {:?}…", backoff);

    sleep(*backoff).await;

    *backoff = (*backoff * 2).min(BACKOFF_MAX);
}
