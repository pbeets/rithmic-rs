//! Put your own timeout on requests. The crate never times one out, and what a
//! timeout means depends on whether the request changed anything on the server.
//!
//! Run with: cargo run --example request_timeout
//! Env: SYMBOL, EXCHANGE (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tracing::{info, warn};

use tokio::{
    sync::broadcast::error::RecvError,
    time::{Duration, Instant, timeout, timeout_at},
};

use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicCancelOrder, RithmicConfig,
    RithmicEnv, RithmicError, RithmicOrder, RithmicOrderPlant, RithmicOrderPlantHandle,
    rti::messages::RithmicMessage,
};

const ENV: RithmicEnv = RithmicEnv::Demo;
const TAG: &str = "example-timeout-1";

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

    if let Some(e) = handle.subscribe_order_updates().await?.error {
        warn!("order updates refused: {e}");
    }

    // A read changes nothing on the server, so on a timeout just ask again.
    for attempt in 1..=3 {
        match timeout(Duration::from_secs(5), handle.get_account_rms_info()).await {
            Ok(reply) => {
                match reply?.iter().find_map(|r| r.error.as_ref()) {
                    Some(e) => warn!("rms info refused: {e}"),
                    None => info!("rms info received"),
                }

                break;
            }
            Err(_) => warn!("rms info: no reply within 5s (attempt {attempt})"),
        }
    }

    // A buy far below the market rests and never fills. 4000 suits ES;
    // other products need their own price.
    let order = RithmicOrder::new()
        .symbol(&symbol)
        .exchange(&exchange)
        .quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(4000.0)
        .user_tag(TAG)
        .build()?;

    // A timeout here does not mean the order failed: it may have gone out and
    // be working. Never resend blindly; ask the server what it has first.
    let basket_id = match timeout(Duration::from_secs(5), handle.place_order(order)).await {
        Ok(reply) => {
            let responses = reply?;

            if let Some(e) = responses.iter().find_map(|r| r.error.as_ref()) {
                warn!("order refused: {e}");
            }

            responses.iter().find_map(|r| match &r.message {
                RithmicMessage::ResponseNewOrder(r) => r.basket_id.clone(),
                _ => None,
            })
        }
        Err(_) => {
            warn!("no reply within 5s, looking the order up");
            find_order(&mut handle, TAG).await?
        }
    };

    match basket_id {
        Some(id) => {
            let reply = handle
                .cancel_order(RithmicCancelOrder::new().id(id).build()?)
                .await?;

            if let Some(e) = reply.iter().find_map(|r| r.error.as_ref()) {
                warn!("cancel refused: {e}");
            }
        }
        // Refused, or not seen in 10s. A slow order can still turn up, so check
        // again (show_orders or order history) before sending a replacement.
        None => warn!("no working order found"),
    }

    handle.disconnect().await?;
    Ok(())
}

/// The basket_id of the order sent with `user_tag`, if the server has it.
async fn find_order(
    handle: &mut RithmicOrderPlantHandle,
    user_tag: &str,
) -> Result<Option<String>, RithmicError> {
    // show_orders replays open orders as notifications on the subscription
    // channel. The order's own notifications may already be waiting there too.
    if let Some(e) = handle.show_orders().await?.error {
        warn!("show_orders refused: {e}");
    }

    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let update = match timeout_at(deadline, handle.subscription_receiver.recv()).await {
            Err(_) => return Ok(None),
            Ok(Ok(update)) => update,
            // Lagged may have swallowed a ConnectionError; the deadline keeps this loop from hanging.
            Ok(Err(RecvError::Lagged(n))) => {
                warn!("missed {n} updates");
                continue;
            }
            Ok(Err(RecvError::Closed)) => return Err(RithmicError::ConnectionClosed),
        };

        if let Some(e) = update.error.filter(|e| e.is_connection_issue()) {
            return Err(e);
        }

        if let RithmicMessage::RithmicOrderNotification(n) = update.message {
            if n.user_tag.as_deref() == Some(user_tag) {
                info!(
                    "found it: status={:?} basket_id={:?}",
                    n.status, n.basket_id
                );

                return Ok(n.basket_id);
            }
        }
    }
}
