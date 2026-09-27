//! Place a bracket order: a limit entry that, once filled, gets a profit target
//! and a stop loss. The entry rests below the market and is cancelled at the end.
//!
//! Run with: cargo run --example bracket_order
//! Env: SYMBOL, EXCHANGE (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tracing::{info, warn};

use tokio::{
    sync::broadcast::error::RecvError,
    time::{Duration, Instant, timeout_at},
};

use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicBracketOrder, RithmicCancelOrder,
    RithmicConfig, RithmicEnv, RithmicOrderPlant, TimeInForce, rti::messages::RithmicMessage,
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

    // Subscribe before placing, or the first notifications are gone.
    for reply in [
        handle.subscribe_order_updates().await?,
        handle.subscribe_bracket_updates().await?,
    ] {
        if let Some(e) = &reply.error {
            warn!("subscribe refused: {e}");
        }
    }

    // A buy far below the market rests and never fills. 4000 suits ES;
    // other products need their own price.
    let order = RithmicBracketOrder::new()
        .symbol(&symbol)
        .exchange(&exchange)
        .quantity(1)
        .action(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .duration(TimeInForce::Day)
        .localid("example-bracket-1")
        .price(4000.0)
        .target(20) // ticks in your favour
        .stop(10) // ticks against you
        .build()?;

    let responses = handle.place_bracket_order(order).await?;

    if let Some(e) = responses.iter().find_map(|r| r.error.as_ref()) {
        warn!("bracket refused: {e}");
    }

    let basket_id = responses.iter().find_map(|r| match &r.message {
        RithmicMessage::ResponseBracketOrder(r) => r.basket_id.clone(),
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

        // A refused heartbeat also carries an error, but the connection is fine.
        if let Some(e) = &update.error {
            warn!("update error: {e}");
            if e.is_connection_issue() {
                break;
            }
        }

        match &update.message {
            RithmicMessage::RithmicOrderNotification(n) => info!(
                "order: status={:?} basket_id={:?} price={:?}",
                n.status, n.basket_id, n.price
            ),
            RithmicMessage::ExchangeOrderNotification(n) => info!(
                "exchange: status={:?} filled={:?} avg_price={:?}",
                n.status, n.total_fill_size, n.avg_fill_price
            ),
            RithmicMessage::BracketUpdates(b) => info!(
                "bracket: basket_id={:?} target_ticks={:?} stop_ticks={:?}",
                b.basket_id, b.target_ticks, b.stop_ticks
            ),
            _ => {}
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
