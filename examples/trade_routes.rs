//! Route orders by exchange. `login()` reads the server's routes once, and
//! orders use that snapshot until you apply a change with `record_trade_route`.
//!
//! Run with: cargo run --example trade_routes
//! Env: SYMBOL, EXCHANGE (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tracing::{info, warn};

use tokio::{
    sync::broadcast::error::RecvError,
    time::{Duration, Instant, timeout_at},
};

use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicCancelOrder, RithmicConfig,
    RithmicEnv, RithmicOrder, RithmicOrderPlant, rti::messages::RithmicMessage,
};

const ENV: RithmicEnv = RithmicEnv::Demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(ENV)?;
    let account = RithmicAccount::from_env(ENV)?;
    let exchange = common::exchange();
    let symbol = common::symbol();

    let order_plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
    let mut handle = order_plant.get_handle(&account);
    handle.login().await?;

    // Sends nothing, and fails exactly where an order for that exchange would.
    for venue in ["CME", "CBOT", "NYMEX"] {
        match handle.trade_route_for(venue).await {
            Ok(route) => info!("{venue} orders go out on {route}"),
            Err(e) => warn!("{venue} is not routable: {e}"),
        }
    }

    // A buy far below the market rests and never fills; 4000 suits ES only.
    let order = RithmicOrder::new()
        .symbol(&symbol)
        .exchange(&exchange)
        .quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(4000.0)
        .user_tag("example-routed")
        // .trade_route("MY_ROUTE") // send on your own route, even one the server never published
        .build()?;

    // With no route for the exchange, this fails with `NoTradeRoute` and sends nothing.
    let responses = handle.place_order(order).await?;

    if let Some(e) = responses.iter().find_map(|r| r.error.as_ref()) {
        warn!("order refused: {e}");
    }

    let basket_id = responses.iter().find_map(|r| match &r.message {
        RithmicMessage::ResponseNewOrder(r) => r.basket_id.clone(),
        _ => None,
    });

    let deadline = Instant::now() + Duration::from_secs(10);

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

        if update
            .error
            .as_ref()
            .is_some_and(|e| e.is_connection_issue())
        {
            warn!("connection lost: {:?}", update.error);
            break;
        }

        // Route changes arrive here but orders ignore them until you record them.
        if let RithmicMessage::TradeRoute(route) = &update.message {
            info!(
                "route update: {:?} -> {:?}",
                route.exchange, route.trade_route
            );

            handle.record_trade_route(route).await?;
        }
    }

    if let Some(id) = basket_id {
        let reply = handle
            .cancel_order(RithmicCancelOrder::new().id(id).build()?)
            .await?;

        if let Some(e) = reply.iter().find_map(|r| r.error.as_ref()) {
            warn!("cancel refused: {e}");
        }
    }

    handle.disconnect().await?;
    Ok(())
}
