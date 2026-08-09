//! Example: Route orders by exchange
//!
//! Orders go out on the route the server publishes for their exchange. `login()`
//! reads those routes once and orders use that snapshot, so there are three
//! things you may want to do:
//!
//! - Check a venue is routable before you trade, with `trade_route_for`.
//! - Apply a route the server moves mid-session, with `record_trade_route`.
//! - Send an order on a route of your own, with `RithmicOrder::trade_route`.
//!
//! Run with: cargo run --example trade_routes

use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};

use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicConfig, RithmicEnv, RithmicOrder,
    RithmicOrderPlantHandle, rti::messages::RithmicMessage,
};

/// Applies route changes as the server publishes them.
///
/// Updates arrive on the subscription channel but are not applied for you: until
/// you hand one back, orders keep the route `login()` read.
fn spawn_route_listener(handle: RithmicOrderPlantHandle) {
    let mut receiver = handle.subscription_receiver.resubscribe();

    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(response) => {
                    if let RithmicMessage::TradeRoute(update) = &response.message {
                        info!(
                            "Route update: {} -> {:?}",
                            update.exchange.as_deref().unwrap_or("?"),
                            update.trade_route.as_deref().unwrap_or("?"),
                        );

                        // Fails only once the plant is gone, so stop listening.
                        if handle.record_trade_route(update).await.is_err() {
                            break;
                        }
                    }
                }
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(n)) => warn!("Listener lagged, missed {} messages", n),
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
    let order_plant =
        rithmic_rs::RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = order_plant.get_handle(&account);

    // Login loads the routes, so nothing below works before this.
    handle.login().await?;

    // The listener needs its own handle; every handle from this plant shares the
    // one login and the one set of routes.
    spawn_route_listener(order_plant.get_handle(&account));

    // Check the venues you trade are routable. This sends nothing and fails
    // exactly where an order for that exchange would.
    for exchange in ["CME", "CBOT", "NYMEX"] {
        match handle.trade_route_for(exchange).await {
            Ok(route) => info!("{} orders go out on {}", exchange, route),
            Err(err) => warn!("{} is not routable: {}", exchange, err),
        }
    }

    // Placing an order picks the route for its exchange. An exchange with no
    // route fails with `RithmicError::NoTradeRoute` and nothing is sent.
    // Leave `trade_route` unset to use the route for the exchange; call
    // `.trade_route(..)` to send on a route of your own, including one the
    // server never published.
    let order = RithmicOrder::new()
        .symbol("ESU6")
        .exchange("CME")
        .quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(5000.0)
        .user_tag("example-routed")
        .build()?;

    match handle.place_order(order).await {
        Ok(responses) => {
            for response in &responses {
                info!("Order response: {:?}", response);
            }
        }
        Err(err) => warn!("Order refused: {}", err),
    }

    handle.disconnect().await?;

    Ok(())
}
