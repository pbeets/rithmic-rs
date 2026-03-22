//! Example: Place a bracket order (entry with profit target and stop loss)
//!
//! A bracket order consists of:
//! - Entry order: your initial position (Buy or Sell)
//! - Profit target: limit order to take profits at a specified tick distance
//! - Stop loss: stop order to limit losses at a specified tick distance
//!
//! When the entry fills, Rithmic automatically creates and links the profit and stop orders.
//! When either the profit or stop fills, the other is automatically canceled.
//!
//! Run with: cargo run --example bracket_order

use tokio::sync::broadcast;
use tracing::info;

use rithmic_rs::{
    BracketDuration, BracketPriceType, BracketTransactionType, ConnectStrategy,
    RithmicBracketOrder, RithmicConfig, RithmicEnv, RithmicOrderPlant, RithmicResponse,
    rti::messages::RithmicMessage,
};

/// Spawns a task to listen for order notifications
fn spawn_order_listener(mut receiver: broadcast::Receiver<RithmicResponse>) {
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(update) => {
                    if let Some(error) = &update.error {
                        info!("Connection error: {}", error);
                        break;
                    }

                    match &update.message {
                        RithmicMessage::HeartbeatTimeout
                        | RithmicMessage::ForcedLogout(_)
                        | RithmicMessage::ConnectionError => {
                            info!("Connection lost");
                            break;
                        }
                        RithmicMessage::RithmicOrderNotification(notif) => {
                            info!(
                                "Rithmic order: status={:?} symbol={} qty={} price={:?} basket_id={}",
                                notif.status,
                                notif.symbol.as_deref().unwrap_or("?"),
                                notif.quantity.unwrap_or(0),
                                notif.price,
                                notif.basket_id.as_deref().unwrap_or("?")
                            );
                        }
                        RithmicMessage::ExchangeOrderNotification(notif) => {
                            info!(
                                "Exchange order: status={:?} symbol={} filled_qty={} avg_price={:?}",
                                notif.status.as_deref().unwrap_or("?"),
                                notif.symbol.as_deref().unwrap_or("?"),
                                notif.total_fill_size.unwrap_or(0),
                                notif.avg_fill_price
                            );
                        }
                        RithmicMessage::BracketUpdates(bracket) => {
                            info!(
                                "Bracket update: basket_id={} target_ticks={:?} stop_ticks={:?}",
                                bracket.basket_id.as_deref().unwrap_or("?"),
                                bracket.target_ticks,
                                bracket.stop_ticks
                            );
                        }
                        _ => {}
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("Subscription channel closed");
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    info!("Listener lagged, missed {} messages", n);
                }
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    // Connect to the order plant (use Demo for paper trading)
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let order_plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = order_plant.get_handle();

    // Login to the order plant
    handle.login().await?;
    info!("Logged in to order plant");

    // Subscribe to order and bracket updates
    handle.subscribe_order_updates().await?;
    handle.subscribe_bracket_updates().await?;
    info!("Subscribed to order and bracket updates");

    // Spawn listener task before placing orders (fire-and-forget, runs until disconnect)
    // Use resubscribe() to get a new receiver from the same broadcast channel
    let listener_receiver = handle.subscription_receiver.resubscribe();
    spawn_order_listener(listener_receiver);

    // Define the bracket order
    // Note: Update symbol to a valid front-month contract for your use case
    let bracket_order = RithmicBracketOrder {
        action: BracketTransactionType::Buy,
        duration: BracketDuration::Day,
        exchange: "CME".to_string(),
        localid: "example-bracket-1".to_string(),
        price_type: BracketPriceType::Limit,
        price: Some(5000.00), // Entry limit price
        profit_ticks: 20,     // Take profit 20 ticks above entry
        stop_ticks: 10,       // Stop loss 10 ticks below entry
        quantity: 1,
        symbol: "ESM6".to_string(), // Update to current front-month ES contract
    };

    info!("Placing bracket order: {:?}", bracket_order);

    // Place the bracket order
    let responses = handle.place_bracket_order(bracket_order).await?;
    for resp in &responses {
        info!("Order response: {:?}", resp);
    }

    // Keep the main task alive to receive notifications
    // In a real application, you'd have other logic here or wait for a shutdown signal
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    handle.disconnect().await?;
    info!("Disconnected from order plant");

    Ok(())
}
