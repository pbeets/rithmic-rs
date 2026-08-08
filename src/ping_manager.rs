use std::time::Duration;
use tokio::time::{Instant, sleep_until};
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
/// The timeout is supplied by the caller; see [`crate::ws::PING_INTERVAL_SECS`]
/// and [`crate::ws::PING_TIMEOUT_SECS`] for the values the plants use.
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

    /// Resolves once the pending ping has gone unanswered for the timeout.
    ///
    /// Never resolves while no ping is pending, so it can sit in a `select!` arm.
    /// Clears the pending ping before returning, so a timeout is reported once.
    pub async fn timed_out(&mut self) {
        match self.pending {
            Some(sent_at) => {
                sleep_until(sent_at + self.timeout).await;
                self.pending = None;
            }
            None => std::future::pending().await,
        }
    }

    /// Returns the instant when the pending ping will timeout, if any.
    #[cfg(test)]
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
        let mgr = PingManager::new(60);
        assert!(mgr.next_timeout_at().is_none());
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
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_waits_for_the_timeout() {
        let mut mgr = PingManager::new(60);
        mgr.sent();

        let started = Instant::now();
        mgr.timed_out().await;

        assert_eq!(Instant::now() - started, Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_never_resolves_without_a_pending_ping() {
        let mut mgr = PingManager::new(60);

        tokio::select! {
            _ = mgr.timed_out() => panic!("resolved with no ping pending"),
            _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
        }
    }

    #[test]
    fn sent_twice_replaces_pending() {
        let mut mgr = PingManager::new(60);
        mgr.sent();
        mgr.sent(); // should not panic, just log a warning
        assert!(mgr.next_timeout_at().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_clears_pending_so_it_reports_once() {
        let mut mgr = PingManager::new(1);
        mgr.sent();

        mgr.timed_out().await;

        assert!(mgr.next_timeout_at().is_none());

        // A second wait must not resolve off the ping already reported.
        tokio::select! {
            _ = mgr.timed_out() => panic!("reported the same ping twice"),
            _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
        }
    }
}
