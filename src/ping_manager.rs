use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;

/// Manages WebSocket ping/pong timeout detection for plant actors.
///
/// Tracks pending ping frames and detects when pong responses don't arrive within
/// the configured timeout. Provides a secondary layer of connection health monitoring
/// alongside application-level heartbeats.
///
/// # Behavior
///
/// - Tracks one pending ping at a time
/// - New ping sent before pong received: replaces pending ping, logs warning
/// - Any pong clears pending state (WebSocket protocol guarantees correlation)
/// - Timeout indicates dead connection
///
/// # Configuration
///
/// Default: 60s ping interval, 50s pong timeout
#[derive(Debug)]
pub struct PingManager {
    /// Pending ping waiting for pong response
    pending: Option<Instant>,
    /// Timeout duration
    timeout: Duration,
}

impl PingManager {
    /// Creates a new ping manager with the given timeout in seconds.
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            pending: None,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Registers that a WebSocket ping was sent.
    ///
    /// If a ping is already pending, replaces it and logs a warning.
    pub fn sent(&mut self) {
        if self.pending.replace(Instant::now()).is_some() {
            warn!("Sent new ping before receiving pong for previous ping");
        }
    }

    /// Registers that a pong response was received.
    ///
    /// Clears pending state. WebSocket protocol guarantees pongs echo pings,
    /// so any pong corresponds to our most recent ping.
    pub fn received(&mut self) {
        self.pending = None;
    }

    /// Checks if the pending ping has timed out.
    ///
    /// Returns `true` and clears pending state if timeout exceeded.
    /// Call when the instant from `next_timeout_at()` is reached.
    pub fn check_timeout(&mut self) -> bool {
        if let Some(sent_at) = self.pending {
            if sent_at.elapsed() > self.timeout {
                self.pending = None;
                return true;
            }
        }
        false
    }

    /// Returns the instant when the pending ping will timeout, if any.
    ///
    /// Use with `tokio::time::sleep_until()` in a select! loop.
    /// Returns `None` if no ping is pending.
    pub fn next_timeout_at(&self) -> Option<Instant> {
        self.pending.map(|sent_at| sent_at + self.timeout)
    }
}
