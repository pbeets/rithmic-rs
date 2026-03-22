use std::time::Duration;
use tracing::{info, warn};

use tokio::{
    net::TcpStream,
    time::{Instant, Interval, interval_at, sleep, timeout},
};

use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error, Message},
};

/// Number of seconds between heartbeats sent to the server.
pub(crate) const HEARTBEAT_SECS: u64 = 60;

/// Number of seconds between WebSocket ping frames sent to detect dead connections.
pub(crate) const PING_INTERVAL_SECS: u64 = 60;

/// Timeout in seconds for WebSocket pong response.
pub(crate) const PING_TIMEOUT_SECS: u64 = 50;

/// Connection attempt timeout in seconds.
const CONNECT_TIMEOUT_SECS: u64 = 2;

/// Base backoff in milliseconds multiplied by the attempt number.
const BACKOFF_MS_BASE: u64 = 500;

/// Maximum backoff duration in seconds (rate limit for login attempts).
const MAX_BACKOFF_SECS: u64 = 60;

/// Connection strategy for connecting to Rithmic servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectStrategy {
    /// Single connection attempt. Fast-fail, no retries.
    Simple,
    /// Retry same URL indefinitely with exponential backoff (capped at 60s). Recommended for most users.
    Retry,
    /// Alternates between primary and beta URLs indefinitely. Useful when main server has issues.
    AlternateWithRetry,
}

pub(crate) trait PlantActor {
    type Command;

    async fn run(&mut self);
    async fn handle_command(&mut self, command: Self::Command);
    async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> bool;
}

pub(crate) fn get_heartbeat_interval(override_secs: Option<u64>) -> Interval {
    let secs = override_secs.unwrap_or(HEARTBEAT_SECS);
    let heartbeat_interval = Duration::from_secs(secs);
    let start_offset = Instant::now() + heartbeat_interval;

    interval_at(start_offset, heartbeat_interval)
}

/// Creates an interval for sending WebSocket pings.
///
/// Returns an interval starting after the first ping period elapses.
pub(crate) fn get_ping_interval(override_secs: Option<u64>) -> Interval {
    let secs = override_secs.unwrap_or(PING_INTERVAL_SECS);
    let ping_interval = Duration::from_secs(secs);
    let start_offset = Instant::now() + ping_interval;

    interval_at(start_offset, ping_interval)
}

/// Connect to a single URL without retry.
///
/// # Arguments
/// * `url` - WebSocket URL to connect to
///
/// # Returns
/// WebSocketStream on success, error on failure.
async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    info!("Connecting to {}", url);

    let (ws_stream, _) = connect_async_with_config(url, None, true).await?;

    info!("Successfully connected to {}", url);

    Ok(ws_stream)
}

/// Connect to a single URL with indefinite retry and exponential backoff.
///
/// Retries indefinitely with exponential backoff capped at 60 seconds.
/// This ensures at most one connection attempt per minute after initial ramp-up.
///
/// # Arguments
/// * `url` - WebSocket URL to connect to
///
/// # Returns
/// WebSocketStream on success (never returns error as it retries indefinitely).
async fn connect_with_retry_single_url(
    url: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let mut attempt: u64 = 1;

    loop {
        info!("Attempt {}: connecting to {}", attempt, url);

        match timeout(
            Duration::from_secs(CONNECT_TIMEOUT_SECS),
            connect_async_with_config(url, None, true),
        )
        .await
        {
            Ok(Ok((ws_stream, _))) => {
                info!("Successfully connected to {}", url);
                return Ok(ws_stream);
            }
            Ok(Err(e)) => warn!("connect_async failed for {}: {:?}", url, e),
            Err(e) => warn!("connect_async to {} timed out: {:?}", url, e),
        }

        let backoff_ms: u64 = BACKOFF_MS_BASE.saturating_mul(attempt);
        let backoff_duration =
            Duration::from_millis(backoff_ms).min(Duration::from_secs(MAX_BACKOFF_SECS));

        info!("Backing off for {:?} before retry", backoff_duration);

        sleep(backoff_duration).await;
        attempt += 1;
    }
}

/// Alternate between primary and beta URLs with indefinite retry.
///
/// Retries indefinitely, alternating between primary and beta URLs.
/// Use when main server has issues. Exponential backoff capped at 60 seconds.
///
/// # Arguments
/// * `primary_url` - Primary WebSocket URL
/// * `secondary_url` - Beta WebSocket URL (used after first failure)
///
/// # Returns
/// WebSocketStream on success (never returns error as it retries indefinitely).
async fn connect_with_retry(
    primary_url: &str,
    secondary_url: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let mut attempt: u64 = 1;

    loop {
        let selected_url = if attempt % 2 == 0 {
            secondary_url
        } else {
            primary_url
        };

        info!("Attempt {}: connecting to {}", attempt, selected_url);

        match timeout(
            Duration::from_secs(CONNECT_TIMEOUT_SECS),
            connect_async_with_config(selected_url, None, true),
        )
        .await
        {
            Ok(Ok((ws_stream, _))) => return Ok(ws_stream),
            Ok(Err(e)) => warn!("connect_async failed for {}: {:?}", selected_url, e),
            Err(e) => warn!("connect_async to {} timed out: {:?}", selected_url, e),
        }

        let backoff_ms: u64 = BACKOFF_MS_BASE.saturating_mul(attempt);
        let backoff_duration =
            Duration::from_millis(backoff_ms).min(Duration::from_secs(MAX_BACKOFF_SECS));

        info!("Backing off for {:?} before retry", backoff_duration);

        sleep(backoff_duration).await;
        attempt += 1;
    }
}

/// Connect using the specified strategy.
///
/// # Arguments
/// * `primary_url` - Primary WebSocket URL
/// * `beta_url` - Beta WebSocket URL (only used for AlternateWithRetry)
/// * `strategy` - Connection strategy to use
///
/// # Returns
/// WebSocketStream on success, error on failure.
pub(crate) async fn connect_with_strategy(
    primary_url: &str,
    beta_url: &str,
    strategy: ConnectStrategy,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    match strategy {
        ConnectStrategy::Simple => connect(primary_url).await,
        ConnectStrategy::Retry => connect_with_retry_single_url(primary_url).await,
        ConnectStrategy::AlternateWithRetry => connect_with_retry(primary_url, beta_url).await,
    }
}
