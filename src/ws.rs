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

/// Default limit on one connection attempt. Australia to Chicago has run
/// past the old 2 seconds (issue #124); 5 also leaves room for a lost packet.
pub(crate) const DEFAULT_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Base backoff in milliseconds multiplied by the attempt number.
const BACKOFF_MS_BASE: u64 = 500;

/// Maximum backoff duration in seconds (rate limit for connection attempts).
const MAX_BACKOFF_SECS: u64 = 60;

/// How a plant's `connect` tries to reach the server.
///
/// Each attempt times out after
/// [`RithmicConfigBuilder::connect_attempt_timeout`](crate::RithmicConfigBuilder::connect_attempt_timeout),
/// 5 seconds by default. The retrying strategies wait between attempts:
/// 500 ms more per failed attempt, capped at 60 s, then jittered by ±50% so
/// plants that dropped together do not retry in step.
///
/// The retrying strategies try indefinitely unless
/// [`RithmicConfigBuilder::connect_total_timeout`](crate::RithmicConfigBuilder::connect_total_timeout)
/// sets a limit, after which `connect` returns
/// [`RithmicError::ConnectionFailed`](crate::RithmicError::ConnectionFailed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectStrategy {
    /// One attempt on [`RithmicConfig::url`](crate::RithmicConfig::url). On
    /// failure `connect` returns
    /// [`RithmicError::ConnectionFailed`](crate::RithmicError::ConnectionFailed)
    /// at once.
    Simple,
    /// Retry [`RithmicConfig::url`](crate::RithmicConfig::url) with backoff.
    /// The recommended default.
    Retry,
    /// Like `Retry`, but alternate between
    /// [`RithmicConfig::url`](crate::RithmicConfig::url) and
    /// [`RithmicConfig::beta_url`](crate::RithmicConfig::beta_url), starting
    /// with `url`. Useful when the main server has issues.
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

/// Send a WebSocket message, giving up after `timeout_duration` so a
/// half-open connection cannot hang the actor loop.
///
/// On `Timeout` the message may still sit in the sink's buffer, so treat the
/// sink as poisoned and do not write to it again.
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

/// Interval for WebSocket pings, every [`PING_INTERVAL_SECS`]. The first tick
/// is one period from now.
pub(crate) fn get_ping_interval() -> Interval {
    let ping_interval = Duration::from_secs(PING_INTERVAL_SECS);
    let start_offset = Instant::now() + ping_interval;

    interval_at(start_offset, ping_interval)
}

/// `attempt_timeout`, cut to the time left before `deadline`.
fn time_for_attempt(attempt_timeout: Duration, deadline: Option<Instant>) -> Duration {
    match deadline {
        Some(deadline) => attempt_timeout.min(deadline.saturating_duration_since(Instant::now())),
        None => attempt_timeout,
    }
}

/// One connection attempt, bounded by `attempt_timeout` so `Simple` fails
/// fast instead of waiting out the OS TCP timeout.
async fn connect(
    url: &str,
    attempt_timeout: Duration,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    info!("Connecting to {}", url);

    let (ws_stream, _) = timeout(attempt_timeout, connect_async_with_config(url, None, true))
        .await
        .map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connection attempt timed out after {attempt_timeout:?}"),
            ))
        })??;

    info!("Successfully connected to {}", url);

    Ok(ws_stream)
}

/// Scale a delay by a factor in [0.5, 1.5), seeded from the clock's
/// sub-second nanos. That is enough spread to break reconnect lockstep without
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

/// Retry until connected, cycling through `urls`, with the jittered linear
/// backoff described on [`ConnectStrategy`] (30-90 s once capped).
///
/// With a `deadline`, each attempt is cut to the time left, and it returns a
/// `TimedOut` I/O error holding a [`RetryDeadlineExceeded`] instead of
/// sleeping past it.
async fn connect_with_retry(
    urls: &[&str],
    attempt_timeout: Duration,
    deadline: Option<Instant>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let started = Instant::now();
    let mut attempt: u64 = 1;

    loop {
        let url = urls[(attempt - 1) as usize % urls.len()];

        info!("Attempt {}: connecting to {}", attempt, url);

        let this_attempt = time_for_attempt(attempt_timeout, deadline);

        match timeout(this_attempt, connect_async_with_config(url, None, true)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Successfully connected to {}", url);
                return Ok(ws_stream);
            }

            Ok(Err(e)) => warn!("connect_async failed for {}: {:?}", url, e),
            Err(_) => warn!(
                "connect_async to {} timed out after {:?}",
                url, this_attempt
            ),
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

/// Connect using `strategy`, giving each attempt up to `attempt_timeout` and
/// all of them together up to `total_timeout` (`None` means no limit).
/// `Simple` errors when its one attempt fails, the others once the total passes.
pub(crate) async fn connect_with_strategy(
    primary_url: &str,
    beta_url: &str,
    strategy: ConnectStrategy,
    attempt_timeout: Duration,
    total_timeout: Option<Duration>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    // A duration too large to add is as good as no deadline.
    let deadline = total_timeout.and_then(|duration| Instant::now().checked_add(duration));

    match strategy {
        ConnectStrategy::Simple => {
            connect(primary_url, time_for_attempt(attempt_timeout, deadline)).await
        }

        ConnectStrategy::Retry => {
            connect_with_retry(&[primary_url], attempt_timeout, deadline).await
        }

        ConnectStrategy::AlternateWithRetry => {
            connect_with_retry(&[primary_url, beta_url], attempt_timeout, deadline).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

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

        let error = connect_with_strategy(
            &url,
            &url,
            ConnectStrategy::Retry,
            DEFAULT_CONNECT_ATTEMPT_TIMEOUT,
            Some(limit),
        )
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
            elapsed <= limit,
            "gave up after {elapsed:?}, past the deadline"
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

        let error = connect_with_strategy(
            &url,
            &url,
            ConnectStrategy::Retry,
            DEFAULT_CONNECT_ATTEMPT_TIMEOUT,
            Some(limit),
        )
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
            DEFAULT_CONNECT_ATTEMPT_TIMEOUT,
            Some(limit),
        )
        .await
        .expect_err("nothing listens, so the deadline must end the retry");

        assert!(attempts_of(&error) >= 1);
        assert!(started.elapsed() <= limit);
    }

    /// A local URL that accepts TCP but never answers the WebSocket upgrade,
    /// so every attempt runs until its timeout. Keep the listener alive.
    async fn silent_url() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let port = listener.local_addr().expect("local address").port();

        (listener, format!("ws://127.0.0.1:{port}"))
    }

    #[tokio::test(start_paused = true)]
    async fn simple_waits_the_configured_attempt_timeout() {
        let (_listener, url) = silent_url().await;
        let attempt = Duration::from_secs(7);
        let started = Instant::now();

        let error = connect_with_strategy(&url, &url, ConnectStrategy::Simple, attempt, None)
            .await
            .expect_err("the server never answers, so the attempt must time out");

        assert_eq!(started.elapsed(), attempt);
        assert!(error.to_string().contains("timed out after 7s"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_gives_each_attempt_the_configured_timeout() {
        let (_listener, url) = silent_url().await;
        let attempt = Duration::from_secs(7);

        // With the total equal to one attempt, a shorter attempt would leave
        // time for a second one.
        let error =
            connect_with_strategy(&url, &url, ConnectStrategy::Retry, attempt, Some(attempt))
                .await
                .expect_err("the server never answers, so the deadline must end the retry");

        assert_eq!(attempts_of(&error), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_total_timeout_cuts_a_simple_attempt() {
        let (_listener, url) = silent_url().await;
        let total = Duration::from_secs(1);
        let started = Instant::now();

        connect_with_strategy(
            &url,
            &url,
            ConnectStrategy::Simple,
            DEFAULT_CONNECT_ATTEMPT_TIMEOUT,
            Some(total),
        )
        .await
        .expect_err("the server never answers, so the attempt must time out");

        assert_eq!(started.elapsed(), total);
    }
}
