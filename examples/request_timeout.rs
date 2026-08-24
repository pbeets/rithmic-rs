//! Example: adding your own timeout to a request.
//!
//! The crate does not time out requests. This example shows how to add one.
//!
//! Run with: cargo run --example request_timeout

use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicBracketOrder, RithmicConfig,
    RithmicEnv, RithmicError, RithmicOrderPlant, RithmicResponse, TimeInForce,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let account = RithmicAccount::from_env(RithmicEnv::Demo)?;

    let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle(&account);

    handle.login().await?;
    handle.subscribe_order_updates().await?;
    handle.subscribe_bracket_updates().await?;

    let request = handle.get_account_rms_info();

    match tokio::time::timeout(Duration::from_secs(10), request).await {
        Ok(Ok(responses)) => info!("rms info: {} frames", responses.len()),
        Ok(Err(e)) => error!("rms info: {e}"),
        Err(_elapsed) => warn!("rms info: gave up; the late reply is discarded"),
    }

    let worker = handle.clone();
    let order = build_order("example-timeout-1")?;
    let mut task = tokio::spawn(async move { worker.place_bracket_order(order).await });

    match tokio::time::timeout(Duration::from_secs(30), &mut task).await {
        Ok(joined) => match joined? {
            Ok(responses) => info!("bracket acked in time: {} frames", responses.len()),
            Err(e) => error!("bracket rejected: {e}"),
        },
        Err(_elapsed) => {
            warn!("bracket: gave up waiting; the task still holds the reply");

            match task.await? {
                Ok(responses) => info!("bracket acked late: {} frames", responses.len()),
                Err(e) => error!("bracket failed: {e}"),
            }
        }
    }

    let (outcomes_tx, mut outcomes) = mpsc::channel::<(String, Outcome)>(64);

    for tag in ["example-timeout-2", "example-timeout-3"] {
        let worker = handle.clone();
        let report = outcomes_tx.clone();
        let order = build_order(tag)?;
        let tag = tag.to_string();

        tokio::spawn(async move {
            let outcome = match worker.place_bracket_order(order).await {
                Ok(responses) => Outcome::Acked(responses),
                Err(e) => Outcome::Failed(e),
            };
            let _ = report.send((tag, outcome)).await;
        });
    }

    drop(outcomes_tx);

    while let Some((tag, outcome)) = outcomes.recv().await {
        match outcome {
            Outcome::Acked(responses) => info!("{tag}: acked, {} frames", responses.len()),
            Outcome::Failed(e) => error!("{tag}: {e}"),
        }
    }

    handle.disconnect().await?;
    Ok(())
}

enum Outcome {
    Acked(Vec<RithmicResponse>),
    Failed(RithmicError),
}

fn build_order(localid: &str) -> Result<RithmicBracketOrder, Box<dyn std::error::Error>> {
    Ok(RithmicBracketOrder::new()
        .symbol("ESU6")
        .exchange("CME")
        .quantity(1)
        .action(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .duration(TimeInForce::Day)
        .localid(localid)
        .price(5000.00)
        .target(20)
        .stop(10)
        .build()?)
}
