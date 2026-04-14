use std::time::Duration;
use tracing::{info, warn};

use futures_util::{Sink, SinkExt};

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

/// Timeout in seconds for any actor-owned WebSocket write.
pub(crate) const SEND_TIMEOUT_SECS: u64 = 10;

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
}

/// Error returned when a bounded WebSocket send does not complete.
#[derive(Debug)]
pub(crate) enum WebSocketSendError {
    /// The underlying sink returned an error before the timeout elapsed.
    Transport(Error),
    /// The send future did not complete within the configured timeout.
    Timeout,
}

/// Sends a WebSocket message with a hard timeout.
///
/// This prevents actor loop branches from hanging indefinitely on half-open
/// connections where the TCP write side no longer makes progress.
///
/// # Cancellation safety
///
/// This function is not cancel-safe with respect to the underlying sink. If the
/// timeout fires while the sink is flushing, the message may already be buffered
/// inside the WebSocket stream even though the future returned `Timeout`.
/// Callers must treat the sink as poisoned after any non-`Ok` return and avoid
/// reusing it.
pub(crate) async fn send_with_timeout<S>(
    sink: &mut S,
    msg: Message,
    timeout_duration: Duration,
) -> Result<(), WebSocketSendError>
where
    S: Sink<Message, Error = Error> + Unpin,
{
    match timeout(timeout_duration, sink.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(WebSocketSendError::Transport(error)),
        Err(_) => Err(WebSocketSendError::Timeout),
    }
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

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use super::*;

    enum MockSinkBehavior {
        Ready,
        Error,
        Pending,
    }

    struct MockMessageSink {
        behavior: MockSinkBehavior,
        sent_messages: Vec<Message>,
    }

    impl MockMessageSink {
        fn ready() -> Self {
            Self {
                behavior: MockSinkBehavior::Ready,
                sent_messages: Vec::new(),
            }
        }

        fn error() -> Self {
            Self {
                behavior: MockSinkBehavior::Error,
                sent_messages: Vec::new(),
            }
        }

        fn pending() -> Self {
            Self {
                behavior: MockSinkBehavior::Pending,
                sent_messages: Vec::new(),
            }
        }
    }

    impl Sink<Message> for MockMessageSink {
        type Error = Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.behavior {
                MockSinkBehavior::Ready => Poll::Ready(Ok(())),
                MockSinkBehavior::Error => Poll::Ready(Err(Error::ConnectionClosed)),
                MockSinkBehavior::Pending => Poll::Pending,
            }
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.get_mut().sent_messages.push(item);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.behavior {
                MockSinkBehavior::Ready => Poll::Ready(Ok(())),
                MockSinkBehavior::Error => Poll::Ready(Err(Error::ConnectionClosed)),
                MockSinkBehavior::Pending => Poll::Pending,
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.behavior {
                MockSinkBehavior::Ready => Poll::Ready(Ok(())),
                MockSinkBehavior::Error => Poll::Ready(Err(Error::ConnectionClosed)),
                MockSinkBehavior::Pending => Poll::Pending,
            }
        }
    }

    #[tokio::test]
    async fn send_with_timeout_succeeds_for_ready_sink() {
        let mut sink = MockMessageSink::ready();

        let result = send_with_timeout(
            &mut sink,
            Message::Ping(Vec::new().into()),
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(sink.sent_messages.len(), 1);
    }

    #[tokio::test]
    async fn send_with_timeout_returns_transport_error() {
        let mut sink = MockMessageSink::error();

        let result = send_with_timeout(
            &mut sink,
            Message::Ping(Vec::new().into()),
            Duration::from_millis(10),
        )
        .await;

        assert!(matches!(
            result,
            Err(WebSocketSendError::Transport(Error::ConnectionClosed))
        ));
    }

    #[tokio::test]
    async fn send_with_timeout_returns_timeout_for_stuck_sink() {
        let mut sink = MockMessageSink::pending();

        let result = send_with_timeout(
            &mut sink,
            Message::Ping(Vec::new().into()),
            Duration::from_millis(10),
        )
        .await;

        assert!(matches!(result, Err(WebSocketSendError::Timeout)));
    }
}
