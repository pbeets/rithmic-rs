//! Example: every kind of error the crate can hand you, and what to do with it.
//!
//! Run with: cargo run --example error_handling

use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicError, RithmicTickerPlant,
    rti::messages::RithmicMessage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    // Simple gives up after one try and returns ConnectionFailed. Retry keeps
    // going instead, so it never hands you that error.
    let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = plant.get_handle();

    // Login is the one call where a server rejection comes back as Err.
    if let Err(RithmicError::RequestRejected(err)) = handle.login().await {
        error!(
            "login rejected: code={} msg={}",
            err.code.as_deref().unwrap_or("?"),
            err.message.as_deref().unwrap_or("")
        );
        return Ok(());
    }

    // Everywhere else, Ok doesn't mean it worked — check resp.error.
    match handle.subscribe("ESU6", "CME").await {
        Ok(resp) => match resp.error {
            Some(RithmicError::RequestRejected(err)) => warn!(
                "subscribe rejected: {}",
                err.message.as_deref().unwrap_or("")
            ),
            // The response arrived but wouldn't decode. Retrying won't help;
            // it usually means Rithmic's schema moved ahead of this crate.
            Some(RithmicError::ProtocolError(e)) => error!("subscribe didn't decode: {e}"),
            Some(e) => error!("subscribe: {e}"),
            None => info!("subscribed"),
        },
        // Nothing was sent, so fix the arguments and call again.
        Err(RithmicError::InvalidArgument(e)) => error!("bad arguments: {e}"),
        // Either way the connection is on its way out. Don't retry in a loop.
        Err(e @ (RithmicError::SendFailed | RithmicError::ConnectionClosed)) => {
            error!("connection: {e}");
            handle.abort();
            return Ok(());
        }
        Err(e) => error!("subscribe: {e}"),
    }

    match handle.get_front_month_contract("ES", "CME", false).await {
        Ok(resp) => info!("front month: {:?}", resp.message),
        Err(e) => error!("front month: {e}"),
    }

    loop {
        let update = match handle.subscription_receiver.recv().await {
            Ok(update) => update,
            // You fell behind the broadcast channel and lost n updates.
            Err(RecvError::Lagged(n)) => {
                warn!("dropped {n} updates");
                continue;
            }
            Err(RecvError::Closed) => break,
        };

        // is_connection_issue is the shortcut if you don't care which one it was.
        if update
            .error
            .as_ref()
            .is_some_and(RithmicError::is_connection_issue)
        {
            error!("reconnect: {:?}", update.error);
        }

        match &update.message {
            // The plant is stopping. See examples/reconnect.rs for the loop.
            RithmicMessage::ConnectionError => {
                error!("connection lost: {:?}", update.error);
                break;
            }
            // Usually dead too — unless the server just turned a heartbeat down.
            RithmicMessage::HeartbeatTimeout => {
                if matches!(update.error, Some(RithmicError::RequestRejected(_))) {
                    warn!("server rejected a heartbeat, connection is fine");
                } else {
                    error!("heartbeat timeout");
                    break;
                }
            }
            // Session ended server-side. A ConnectionError follows.
            RithmicMessage::ForcedLogout(_) => warn!("forced logout: {:?}", update.error),
            // A template this crate has no mapping for. Not an error — log it,
            // archive it, or decode it yourself with decode_as.
            RithmicMessage::UnknownTemplate(msg) => {
                info!(
                    "unmapped template {}: {} bytes",
                    msg.template_id,
                    msg.payload.len()
                )
            }
            // A frame that wouldn't decode and named no request to fail.
            RithmicMessage::Unknown => error!("undecodable frame: {:?}", update.error),
            _ => {}
        }
    }

    handle.disconnect().await?;
    Ok(())
}
