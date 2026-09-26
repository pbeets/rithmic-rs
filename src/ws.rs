use futures_util::{Sink, SinkExt};
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

/// Number of seconds between heartbeats sent to the server when the login
/// response carries no interval of its own.
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

/// Maximum backoff duration in seconds (rate limit for connection attempts).
const MAX_BACKOFF_SECS: u64 = 60;

/// Connection strategy for connecting to Rithmic servers.
///
/// The retrying strategies try indefinitely unless
/// [`RithmicConfigBuilder::retry_timeout`](crate::RithmicConfigBuilder::retry_timeout)
/// sets a limit, after which `connect` returns
/// [`RithmicError::ConnectionFailed`](crate::RithmicError::ConnectionFailed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectStrategy {
    /// Single connection attempt. Fast-fail, no retries.
    Simple,
    /// Retry same URL indefinitely with linear backoff (500 ms more per attempt, capped at 60s, jittered ±50%). Recommended for most users.
    Retry,
    /// Alternates between primary and beta URLs indefinitely. Useful when main server has issues.
    AlternateWithRetry,
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

/// Creates an interval for sending heartbeats.
///
/// `override_secs` is the period the server asked for in its login response.
/// A period of 0 falls back to [`HEARTBEAT_SECS`], since `interval_at` panics
/// on a zero period.
pub(crate) fn get_heartbeat_interval(override_secs: Option<u64>) -> Interval {
    let secs = override_secs
        .filter(|secs| *secs > 0)
        .unwrap_or(HEARTBEAT_SECS);
    let heartbeat_interval = Duration::from_secs(secs);
    let start_offset = Instant::now() + heartbeat_interval;

    interval_at(start_offset, heartbeat_interval)
}

/// Creates an interval for sending WebSocket pings.
///
/// Returns an interval starting after the first ping period elapses.
pub(crate) fn get_ping_interval() -> Interval {
    let ping_interval = Duration::from_secs(PING_INTERVAL_SECS);
    let start_offset = Instant::now() + ping_interval;

    interval_at(start_offset, ping_interval)
}

/// Connect to a single URL without retry.
///
/// Bounded by [`CONNECT_TIMEOUT_SECS`] so `Simple` fast-fails instead of
/// hanging for the OS TCP timeout.
///
/// # Arguments
/// * `url` - WebSocket URL to connect to
///
/// # Returns
/// WebSocketStream on success, error on failure or timeout.
async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    info!("Connecting to {}", url);

    let (ws_stream, _) = timeout(
        Duration::from_secs(CONNECT_TIMEOUT_SECS),
        connect_async_with_config(url, None, true),
    )
    .await
    .map_err(|_| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connection attempt timed out",
        ))
    })??;

    info!("Successfully connected to {}", url);

    Ok(ws_stream)
}

/// Scale a delay by a factor in [0.5, 1.5), seeded from the clock's
/// sub-second nanos — enough spread to break reconnect lockstep without
/// pulling in a rand dependency.
fn jittered(ms: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(512);

    ms / 2 + ms * (nanos % 1024) / 1024
}

/// The backoff after failed attempt `attempt`, before jitter.
fn backoff_ms(attempt: u64) -> u64 {
    BACKOFF_MS_BASE
        .saturating_mul(attempt)
        .min(MAX_BACKOFF_SECS * 1000)
}

/// Why a deadline-bounded retry gave up. Carried inside the
/// `ErrorKind::TimedOut` I/O error that [`connect_with_retry`] returns.
#[derive(Debug)]
struct RetryDeadlineExceeded {
    attempts: u64,
    within: Duration,
}

impl std::fmt::Display for RetryDeadlineExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gave up connecting after {} attempt{} within {:?}",
            self.attempts,
            if self.attempts == 1 { "" } else { "s" },
            self.within
        )
    }
}

impl std::error::Error for RetryDeadlineExceeded {}

/// Connect with retry and linear backoff — 500 ms more per attempt, capped
/// at [`MAX_BACKOFF_SECS`] and then jittered by ±50%, so the spread
/// survives a long outage (delays range 30–90 s at the cap).
///
/// The jitter keeps plants that lost the same connection from retrying in
/// lockstep against a recovering server.
///
/// `urls` is cycled by attempt number: pass one URL to retry it, or
/// primary + beta to alternate between them.
///
/// With no `deadline`, never returns until a connection succeeds. With one,
/// each attempt's timeout is capped at the time remaining, and once the
/// next backoff would run past the deadline this returns an
/// `ErrorKind::TimedOut` I/O error naming the attempt count instead of
/// sleeping.
async fn connect_with_retry(
    urls: &[&str],
    deadline: Option<Instant>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let started = Instant::now();
    let mut attempt: u64 = 1;

    loop {
        let url = urls[(attempt - 1) as usize % urls.len()];

        info!("Attempt {}: connecting to {}", attempt, url);

        let attempt_timeout = match deadline {
            Some(deadline) => Duration::from_secs(CONNECT_TIMEOUT_SECS)
                .min(deadline.saturating_duration_since(Instant::now())),
            None => Duration::from_secs(CONNECT_TIMEOUT_SECS),
        };

        match timeout(attempt_timeout, connect_async_with_config(url, None, true)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Successfully connected to {}", url);
                return Ok(ws_stream);
            }
            Ok(Err(e)) => warn!("connect_async failed for {}: {:?}", url, e),
            Err(e) => warn!("connect_async to {} timed out: {:?}", url, e),
        }

        let backoff_duration = Duration::from_millis(jittered(backoff_ms(attempt)));

        if let Some(deadline) = deadline {
            if Instant::now() + backoff_duration >= deadline {
                let reason = RetryDeadlineExceeded {
                    attempts: attempt,
                    // Rounded to the millisecond: `started` is read a moment
                    // after the caller set the deadline.
                    within: Duration::from_millis(
                        (deadline.saturating_duration_since(started) + Duration::from_micros(500))
                            .as_millis() as u64,
                    ),
                };

                warn!("{}", reason);

                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    reason,
                )));
            }
        }

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
/// * `deadline` - How long the retrying strategies keep trying; `None` for
///   no limit. `Simple` makes its one attempt and ignores it.
///
/// # Returns
/// WebSocketStream on success. `Simple` errors when its one attempt fails;
/// `Retry` and `AlternateWithRetry` error only once a `deadline` passes, and
/// without one keep trying until they connect.
pub(crate) async fn connect_with_strategy(
    primary_url: &str,
    beta_url: &str,
    strategy: ConnectStrategy,
    deadline: Option<Duration>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    // A duration too large to add is as good as no deadline.
    let deadline = deadline.and_then(|duration| Instant::now().checked_add(duration));

    match strategy {
        ConnectStrategy::Simple => connect(primary_url).await,
        ConnectStrategy::Retry => connect_with_retry(&[primary_url], deadline).await,
        ConnectStrategy::AlternateWithRetry => {
            connect_with_retry(&[primary_url, beta_url], deadline).await
        }
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

    #[tokio::test]
    async fn get_heartbeat_interval_uses_the_server_period() {
        assert_eq!(
            get_heartbeat_interval(Some(30)).period(),
            Duration::from_secs(30)
        );
        assert_eq!(
            get_heartbeat_interval(Some(120)).period(),
            Duration::from_secs(120)
        );
    }

    #[tokio::test]
    async fn get_heartbeat_interval_falls_back_to_the_default() {
        let default = Duration::from_secs(HEARTBEAT_SECS);

        assert_eq!(get_heartbeat_interval(None).period(), default);
        assert_eq!(get_heartbeat_interval(Some(0)).period(), default);
    }

    /// A local URL nothing listens on, so every connect is refused at once.
    async fn refusing_url() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let port = listener.local_addr().expect("local address").port();
        drop(listener);

        format!("ws://127.0.0.1:{port}")
    }

    /// The attempt count a deadline-bounded retry reported when it gave up.
    fn attempts_of(error: &Error) -> u64 {
        let Error::Io(io) = error else {
            panic!("expected an I/O error, got {error:?}");
        };
        assert_eq!(io.kind(), std::io::ErrorKind::TimedOut);

        io.get_ref()
            .and_then(|inner| inner.downcast_ref::<RetryDeadlineExceeded>())
            .expect("the error must say why the retry gave up")
            .attempts
    }

    #[tokio::test(start_paused = true)]
    async fn retry_gives_up_at_the_deadline() {
        let url = refusing_url().await;
        let limit = Duration::from_secs(10);
        let started = Instant::now();

        let error = connect_with_strategy(&url, &url, ConnectStrategy::Retry, Some(limit))
            .await
            .expect_err("nothing listens, so the deadline must end the retry");

        let elapsed = started.elapsed();
        let attempts = attempts_of(&error);
        // The retry stops when the next backoff would cross the deadline, so
        // it can stop short by at most that backoff at its jittered maximum.
        let last_backoff = Duration::from_millis(backoff_ms(attempts) * 3 / 2);

        assert!(
            elapsed >= limit.saturating_sub(last_backoff),
            "gave up after {elapsed:?}, more than one backoff ({last_backoff:?}) early"
        );
        assert!(
            elapsed <= limit + Duration::from_secs(CONNECT_TIMEOUT_SECS),
            "gave up after {elapsed:?}, past the deadline plus one connect timeout"
        );
        assert!(
            error.to_string().contains(&format!("{attempts} attempts")),
            "the message must give the attempt count: {error}"
        );
        assert!(
            error.to_string().contains("10s"),
            "the message must give the duration: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_with_a_deadline_shorter_than_the_first_backoff_makes_one_attempt() {
        let url = refusing_url().await;
        // Below the first backoff's jittered minimum of half BACKOFF_MS_BASE.
        let limit = Duration::from_millis(BACKOFF_MS_BASE / 4);

        let error = connect_with_strategy(&url, &url, ConnectStrategy::Retry, Some(limit))
            .await
            .expect_err("nothing listens, so the deadline must end the retry");

        assert_eq!(attempts_of(&error), 1);
        assert!(
            error.to_string().contains("after 1 attempt within"),
            "the message must give the attempt count: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn alternate_with_retry_gives_up_at_the_deadline() {
        let primary = refusing_url().await;
        let beta = refusing_url().await;
        let limit = Duration::from_secs(5);
        let started = Instant::now();

        let error = connect_with_strategy(
            &primary,
            &beta,
            ConnectStrategy::AlternateWithRetry,
            Some(limit),
        )
        .await
        .expect_err("nothing listens, so the deadline must end the retry");

        assert!(attempts_of(&error) >= 1);
        assert!(started.elapsed() <= limit + Duration::from_secs(CONNECT_TIMEOUT_SECS));
    }
}
