//! Example: the errors you will run into, and what to do about each.
//!
//! Each function below covers one situation. `orders` needs the account IDs in
//! `.env` and is skipped without them; it sends no order to the exchange.
//!
//! Run with: cargo run --example error_handling
//! Env: SYMBOL, PRODUCT, EXCHANGE, START_TIME (see examples/README.md)

#[path = "shared/common.rs"]
mod common;

use tracing::{error, info, warn};

use tokio::{
    sync::broadcast::{Receiver, error::RecvError},
    time::{Duration, Instant, timeout, timeout_at},
};

use rithmic_rs::{
    ConnectStrategy, OrderSide, OrderType, RithmicAccount, RithmicConfig, RithmicConfigBuilder,
    RithmicEnv, RithmicError, RithmicHistoryPlant, RithmicOrder, RithmicOrderPlant,
    RithmicResponse, RithmicTickerPlant, RithmicTickerPlantHandle,
    rti::{exchange_order_notification::NotifyType, messages::RithmicMessage},
};

const ENV: RithmicEnv = RithmicEnv::Demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    // Without a retry timeout, `Retry` keeps trying forever and never hands
    // you `ConnectionFailed`.
    let config = RithmicConfigBuilder::from_env(ENV)?
        .retry_timeout(Duration::from_secs(60))
        .build()?;

    let exchange = common::exchange();

    let plant = match RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await {
        Ok(plant) => plant,
        // Wrong URL, no network, or the gateway turned us away for a minute.
        Err(e) => {
            error!("connect: {e}");
            return Ok(());
        }
    };

    let mut handle = plant.get_handle();

    if !login(&handle).await {
        return Ok(());
    }

    let symbol = common::front_month(&handle, &exchange).await?;

    if !requests(&handle, &symbol, &exchange).await {
        handle.abort();
        return Ok(());
    }

    caller_timeout(&handle).await;
    history(&config, &symbol, &exchange).await;
    orders(&config, &symbol, &exchange).await;
    stream(&mut handle.subscription_receiver, Duration::from_secs(30)).await;

    if let Err(e) = handle.disconnect().await {
        warn!("disconnect: {e}");
    }

    Ok(())
}

/// Unlike most calls, `login` returns the server's refusal as `Err`.
async fn login(handle: &RithmicTickerPlantHandle) -> bool {
    match handle.login().await {
        Ok(_) => true,
        // Wrong credentials, app name or system name. Retrying won't help.
        Err(RithmicError::RequestRejected(err)) => {
            error!(
                "login rejected: code={} msg={}",
                err.code.as_deref().unwrap_or("?"),
                err.message.as_deref().unwrap_or("")
            );

            false
        }
        // A second login with a different LoginConfig. Reconnect to change it.
        Err(RithmicError::LoginConflict) => {
            error!("login: this plant is already logged in with another config");
            false
        }
        Err(e) => {
            error!("login: {e}");
            false
        }
    }
}

/// For most calls `Ok` does not mean it worked: check `resp.error`.
/// Returns false when the connection is going and the caller should stop.
async fn requests(handle: &RithmicTickerPlantHandle, symbol: &str, exchange: &str) -> bool {
    match handle.subscribe(symbol, exchange).await {
        Ok(resp) => match resp.error {
            // Branch on the code rather than parsing the message.
            Some(RithmicError::RequestRejected(err)) => {
                warn!("subscribe rejected: {err}")
            }
            // The reply would not decode. Usually Rithmic's schema moved ahead
            // of this crate, so retrying won't help.
            Some(RithmicError::ProtocolError(e)) => error!("subscribe didn't decode: {e}"),
            Some(e) => error!("subscribe: {e}"),
            None => info!("subscribed"),
        },
        // Nothing was sent. Fix the arguments and call again.
        Err(RithmicError::InvalidArgument(e)) => error!("bad arguments: {e}"),
        // The connection is on its way out; the cause arrives on the
        // subscription channel. Reconnect rather than retrying the call.
        Err(e) if e.is_connection_issue() => {
            error!("subscribe: {e}");
            return false;
        }
        Err(e) => error!("subscribe: {e}"),
    }

    true
}

/// The crate never times out a request. Add your own where you need one.
async fn caller_timeout(handle: &RithmicTickerPlantHandle) {
    let request = handle.get_front_month_contract("ES", "CME", false);

    match timeout(Duration::from_secs(10), request).await {
        Ok(Ok(resp)) => match &resp.error {
            Some(e) => warn!("front month: {e}"),
            None => info!("front month: {:?}", resp.message),
        },
        Ok(Err(e)) => error!("front month: {e}"),
        // The server may still answer; the late reply is dropped. For orders,
        // check order status instead of sending again (see request_timeout.rs).
        Err(_) => warn!("front month: no reply within 10s"),
    }
}

/// History loads return `Err` for a refused replay, but an early end is `Ok`.
async fn history(config: &RithmicConfig, symbol: &str, exchange: &str) {
    let plant = match RithmicHistoryPlant::connect(config, ConnectStrategy::Simple).await {
        Ok(plant) => plant,
        Err(e) => return error!("history connect: {e}"),
    };

    let handle = plant.get_handle();

    if let Err(e) = handle.login().await {
        return error!("history login: {e}");
    }

    // An hour of a weekday session; "the last hour" is empty on weekends.
    let start = common::start_time();

    match handle
        .load_ticks_all(symbol.into(), exchange.into(), start, start + 3600)
        .await
    {
        // The last frame is an end marker. If it carries an error, the server
        // ended the replay early and you have only part of the window.
        Ok(frames) => match frames.split_last() {
            Some((end, ticks)) => match &end.error {
                Some(e) => warn!("ticks: partial window, {} records: {e}", ticks.len()),
                None => info!("ticks: {} records", ticks.len()),
            },
            None => warn!("ticks: empty reply"),
        },
        // The server refused to continue a replay it cut short. Ask for a
        // smaller window.
        Err(RithmicError::RequestRejected(e)) => warn!("ticks refused: {e}"),
        // Empty symbol, a window that ends before it starts, and so on.
        Err(RithmicError::InvalidArgument(e)) => error!("ticks: bad arguments: {e}"),
        Err(e) => error!("ticks: {e}"),
    }

    let _ = handle.disconnect().await;
}

/// Orders can fail at `build()`, before sending, at Rithmic, or at the exchange.
async fn orders(config: &RithmicConfig, symbol: &str, exchange: &str) {
    let Ok(account) = RithmicAccount::from_env(ENV) else {
        return info!("orders: no account IDs in .env, skipping");
    };

    // build() checks the command before anything is sent. A limit order needs a price.
    let unpriced = RithmicOrder::new()
        .symbol(symbol)
        .exchange(exchange)
        .quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .build();

    if let Err(e) = unpriced {
        info!("build: {e}");
    }

    let plant = match RithmicOrderPlant::connect(config, ConnectStrategy::Simple).await {
        Ok(plant) => plant,
        Err(e) => return error!("order connect: {e}"),
    };

    let handle = plant.get_handle(&account);

    if let Err(e) = handle.login().await {
        return error!("order login: {e}");
    }

    // An exchange with no trade route: the plant refuses it without sending.
    let Ok(order) = RithmicOrder::new()
        .symbol(symbol)
        .exchange("NOSUCH")
        .quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(5000.0)
        .build()
    else {
        return;
    };

    match handle.place_order(order).await {
        Err(RithmicError::NoTradeRoute {
            exchange, cached, ..
        }) => info!("no route for {exchange}; routed exchanges: {cached:?}"),
        // Rithmic refused it before it reached the exchange, e.g. a risk limit.
        Ok(responses) => {
            if let Some(e) = responses.iter().find_map(|r| r.error.as_ref()) {
                warn!("order refused: {e}");
            }
        }
        Err(e) => error!("place_order: {e}"),
    }

    let _ = handle.disconnect().await;
}

/// Connection trouble and dropped updates arrive on the subscription channel.
async fn stream(receiver: &mut Receiver<RithmicResponse>, watch_for: Duration) {
    let deadline = Instant::now() + watch_for;

    loop {
        let update = match timeout_at(deadline, receiver.recv()).await {
            Err(_) => break,
            Ok(Ok(update)) => update,
            // You fell behind and lost n updates, maybe a ConnectionError too. The
            // deadline stops this loop hanging; a long-running loop should
            // reconnect instead (see reconnect.rs).
            Ok(Err(RecvError::Lagged(n))) => {
                warn!("dropped {n} updates");
                continue;
            }
            Ok(Err(RecvError::Closed)) => break,
        };

        match &update.message {
            // The plant is stopping. See reconnect.rs for the loop.
            RithmicMessage::ConnectionError => {
                error!("connection lost: {:?}", update.error);
                break;
            }
            // Usually dead too, unless the server just turned a heartbeat down.
            RithmicMessage::HeartbeatTimeout => {
                if matches!(update.error, Some(RithmicError::RequestRejected(_))) {
                    warn!("server rejected a heartbeat, connection is fine");
                } else {
                    error!("heartbeat timeout");
                    break;
                }
            }
            // The server ended the session. A ConnectionError follows.
            RithmicMessage::ForcedLogout(_) => warn!("forced logout: {:?}", update.error),
            // A template this crate has no mapping for. Not an error: log it,
            // archive it, or decode it yourself with decode_as.
            RithmicMessage::UnknownTemplate(msg) => info!(
                "unmapped template {}: {} bytes",
                msg.template_id,
                msg.payload.len()
            ),
            // A frame that wouldn't decode and named no request to fail.
            RithmicMessage::Unknown => error!("undecodable frame: {:?}", update.error),
            // An order the exchange rejects still returned Ok from place_order.
            // On an order plant receiver, the rejection arrives here.
            RithmicMessage::ExchangeOrderNotification(n)
                if n.notify_type == Some(NotifyType::Reject as i32) =>
            {
                warn!(
                    "order {} rejected: {}",
                    n.basket_id.as_deref().unwrap_or("?"),
                    n.text.as_deref().unwrap_or("no reason given")
                );
            }
            _ => {}
        }
    }
}
