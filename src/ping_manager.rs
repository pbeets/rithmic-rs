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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn new_has_no_pending() {
        let mut mgr = PingManager::new(60);
        assert!(mgr.next_timeout_at().is_none());
        assert!(!mgr.check_timeout());
    }

    #[test]
    fn sent_marks_pending() {
        let mut mgr = PingManager::new(60);
        mgr.sent();
        assert!(mgr.next_timeout_at().is_some());
    }

    #[test]
    fn received_clears_pending() {
        let mut mgr = PingManager::new(60);
        mgr.sent();
        mgr.received();
        assert!(mgr.next_timeout_at().is_none());
        assert!(!mgr.check_timeout());
    }

    #[test]
    fn check_timeout_returns_false_before_deadline() {
        let mut mgr = PingManager::new(60);
        mgr.sent();
        // Called immediately — timeout has not elapsed yet.
        assert!(!mgr.check_timeout());
    }

    #[test]
    fn check_timeout_false_when_no_pending() {
        let mut mgr = PingManager::new(60);
        assert!(!mgr.check_timeout());
    }

    #[test]
    fn sent_twice_replaces_pending() {
        let mut mgr = PingManager::new(60);
        mgr.sent();
        mgr.sent(); // should not panic, just log a warning
        assert!(mgr.next_timeout_at().is_some());
    }

    #[tokio::test]
    async fn check_timeout_returns_true_after_deadline() {
        tokio::time::pause();
        let mut mgr = PingManager::new(1);
        mgr.sent();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(mgr.check_timeout());
    }

    #[tokio::test]
    async fn check_timeout_clears_pending_after_trigger() {
        tokio::time::pause();
        let mut mgr = PingManager::new(1);
        mgr.sent();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(mgr.check_timeout());
        // State must be cleared — no double-trigger.
        assert!(mgr.next_timeout_at().is_none());
        assert!(!mgr.check_timeout());
    }

    #[tokio::test]
    async fn next_timeout_at_is_sent_at_plus_timeout() {
        tokio::time::pause();
        let timeout_secs = 30_u64;
        let mut mgr = PingManager::new(timeout_secs);
        let before = Instant::now();
        mgr.sent();
        let deadline = mgr.next_timeout_at().expect("should have a deadline");
        let expected = before + Duration::from_secs(timeout_secs);
        // Allow a tiny delta (1 ms) for any sub-millisecond clock granularity.
        let delta = if deadline >= expected {
            deadline - expected
        } else {
            expected - deadline
        };
        assert!(
            delta <= Duration::from_millis(1),
            "deadline delta too large: {delta:?}"
        );
    }
}
