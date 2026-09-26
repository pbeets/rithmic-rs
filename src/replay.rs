//! History replays you can watch and cancel. See [`ReplayHandle`].

use crate::{RithmicError, RithmicResponse, plants::history_plant::HistoryPlantCommand};

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};

/// A snapshot of a replay's progress, from [`ReplayHandle::subscribe_progress`].
///
/// Use it to spot a replay that has stalled. Only this replay's own data moves
/// it forward; heartbeats and other requests do not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplayProgress {
    /// When the request was written to the socket. `None` while it is queued.
    pub sent_at: Option<Instant>,
    /// When data last arrived, or the request was sent or continued.
    pub last_progress_at: Option<Instant>,
    /// Frames received that carry data.
    pub data_frames: u64,
    /// How many times the server cut the reply short and the plant asked it to
    /// continue.
    pub continuations: u64,
}

/// Why a replay ended.
///
/// Only [`Complete`](Self::Complete) means you have the whole window. For any
/// other variant, [`ReplayOutcome::responses`] holds only its start.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplayEnd {
    /// The server sent the whole window. An empty window ends here too, with
    /// no data frames.
    Complete,
    /// The server stopped before the end of the window (`rp_code` `"12"`).
    Truncated,
    /// The server refused the replay, or refused to continue it.
    Refused(RithmicError),
    /// The connection failed, or the server's reply could not be read.
    Failed(RithmicError),
    /// You cancelled the replay. The plant is unaffected.
    Cancelled,
}

/// A finished replay: every frame received, and why it ended.
#[derive(Debug)]
#[non_exhaustive]
pub struct ReplayOutcome {
    /// Every frame received, in order.
    pub responses: Vec<RithmicResponse>,
    /// Why the replay ended.
    pub end: ReplayEnd,
}

/// A running history replay.
///
/// Returned by [`start_time_bar_replay`], [`start_tick_bar_replay`] and
/// [`start_volume_profile_minute_bars`]. Unlike the `load_*` methods, a handle
/// lets you:
///
/// - watch progress with [`subscribe_progress`](Self::subscribe_progress), to
///   stop a replay that has stalled;
/// - see whether you got the whole window, from the [`ReplayEnd`] that
///   [`result`](Self::result) returns;
/// - cancel just this replay with [`cancel`](Self::cancel).
///
/// Dropping the handle cancels the replay.
///
/// # Example
///
/// Cancel a replay if no data arrives for 30 seconds:
///
/// ```no_run
/// use std::time::Duration;
/// use rithmic_rs::{ReplayEnd, RithmicHistoryPlantHandle, TimeBarReplayRequest, TimeBarType};
///
/// # async fn demo(handle: RithmicHistoryPlantHandle) -> Result<(), Box<dyn std::error::Error>> {
/// let request = TimeBarReplayRequest::new()
///     .symbol("ESU6")
///     .exchange("CME")
///     .bar_type(TimeBarType::MinuteBar)
///     .bar_type_period(1)
///     .start_time_sec(1_750_000_000)
///     .end_time_sec(1_750_086_400);
///
/// let mut replay = handle.start_time_bar_replay(request).await?;
/// let mut progress = replay.subscribe_progress();
///
/// let outcome = loop {
///     tokio::select! {
///         outcome = replay.result() => break outcome?,
///
///         changed = progress.changed() => {
///             // An error means the replay has ended and its result is ready.
///             if changed.is_err() {
///                 break replay.result().await?;
///             }
///         }
///
///         _ = tokio::time::sleep(Duration::from_secs(30)) => {
///             replay.cancel().await?;
///             break replay.result().await?;
///         }
///     }
/// };
///
/// if outcome.end != ReplayEnd::Complete {
///     println!("only part of the window: {:?}", outcome.end);
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`start_time_bar_replay`]: crate::RithmicHistoryPlantHandle::start_time_bar_replay
/// [`start_tick_bar_replay`]: crate::RithmicHistoryPlantHandle::start_tick_bar_replay
/// [`start_volume_profile_minute_bars`]: crate::RithmicHistoryPlantHandle::start_volume_profile_minute_bars
#[derive(Debug)]
#[must_use = "dropping the handle cancels the replay"]
pub struct ReplayHandle {
    result: Option<oneshot::Receiver<ReplayOutcome>>,
    progress: watch::Receiver<ReplayProgress>,
    sender: mpsc::Sender<HistoryPlantCommand>,
    control: Arc<ReplayControl>,
}

impl ReplayHandle {
    /// A receiver that updates as the replay makes progress. It holds only the
    /// latest snapshot, not every change.
    pub fn subscribe_progress(&self) -> watch::Receiver<ReplayProgress> {
        self.progress.clone()
    }

    /// Wait for the replay to end, and take its frames.
    ///
    /// Safe to use in `tokio::select!`: if the future is dropped, call it again.
    ///
    /// # Errors
    ///
    /// - [`RithmicError::ConnectionClosed`] if the plant shut down first.
    /// - [`RithmicError::InvalidArgument`] if the result was already taken.
    pub async fn result(&mut self) -> Result<ReplayOutcome, RithmicError> {
        let Some(receiver) = self.result.as_mut() else {
            return Err(RithmicError::InvalidArgument(
                "replay result already consumed".into(),
            ));
        };

        let result = receiver.await.map_err(|_| RithmicError::ConnectionClosed);
        self.result = None;

        result
    }

    /// Cancel this replay. Other requests and the connection are unaffected.
    ///
    /// Afterwards, [`result`](Self::result) returns the frames received so far
    /// with [`ReplayEnd::Cancelled`], or the real outcome if the replay had
    /// already finished.
    ///
    /// The replay stops at once, but this call waits for the plant to confirm,
    /// which can take a moment if the plant is busy.
    ///
    /// # Errors
    ///
    /// [`RithmicError::ConnectionClosed`] if the plant has shut down.
    pub async fn cancel(&self) -> Result<(), RithmicError> {
        self.control.cancel();

        let (tx, rx) = oneshot::channel();
        self.sender
            .send(HistoryPlantCommand::CancelReplay {
                control: self.control.clone(),
                acknowledged: Some(tx),
            })
            .await
            .map_err(|_| RithmicError::ConnectionClosed)?;

        rx.await.map_err(|_| RithmicError::ConnectionClosed)
    }
}

impl Drop for ReplayHandle {
    fn drop(&mut self) {
        if self.result.is_some() {
            self.control.cancel();
            // If the queue is full the command is dropped, but the plant also
            // checks the flag on every loop turn.
            let _ = self.sender.try_send(HistoryPlantCommand::CancelReplay {
                control: self.control.clone(),
                acknowledged: None,
            });
        }
    }
}

/// The cancel flag shared by a [`ReplayHandle`] and the plant. The plant checks
/// it on every loop turn.
#[derive(Debug, Default)]
pub(crate) struct ReplayControl(AtomicBool);

impl ReplayControl {
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// The plant's side of a [`ReplayHandle`].
#[derive(Debug)]
pub(crate) struct ReplayRequest {
    pub(crate) control: Arc<ReplayControl>,
    pub(crate) responder: oneshot::Sender<ReplayOutcome>,
    pub(crate) progress: watch::Sender<ReplayProgress>,
    pub(crate) responses: Vec<RithmicResponse>,
    pub(crate) continuation_keys: HashSet<String>,
}

impl ReplayRequest {
    pub(crate) fn new(sender: mpsc::Sender<HistoryPlantCommand>) -> (ReplayHandle, Self) {
        let (tx, rx) = oneshot::channel();
        let (progress_tx, progress_rx) = watch::channel(ReplayProgress::default());
        let control = Arc::new(ReplayControl::default());

        (
            ReplayHandle {
                result: Some(rx),
                progress: progress_rx,
                sender,
                control: control.clone(),
            },
            Self {
                control,
                responder: tx,
                progress: progress_tx,
                responses: Vec::new(),
                continuation_keys: HashSet::new(),
            },
        )
    }

    /// Whether the handle cancelled the replay or was dropped.
    pub(crate) fn cancelled(&self) -> bool {
        self.control.cancelled() || self.responder.is_closed()
    }

    /// Whether the request reached the socket, so the server may still be
    /// sending frames for it.
    pub(crate) fn was_sent(&self) -> bool {
        self.progress.borrow().sent_at.is_some()
    }

    /// Update the counters and set `last_progress_at` to now.
    pub(crate) fn record_progress(&self, update: impl FnOnce(&mut ReplayProgress)) {
        self.progress.send_modify(|progress| {
            update(progress);
            progress.last_progress_at = Some(Instant::now());
        });
    }

    /// Send the frames and outcome to the handle.
    pub(crate) fn finish(self, end: ReplayEnd) {
        let _ = self.responder.send(ReplayOutcome {
            responses: self.responses,
            end,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt as _;

    #[tokio::test]
    async fn dropping_a_result_wait_does_not_cancel_or_lose_the_single_owned_reply() {
        let (sender, _receiver) = mpsc::channel(4);
        let (mut handle, request) = ReplayRequest::new(sender);
        assert!(handle.result().now_or_never().is_none());
        assert!(!request.cancelled());
        request.finish(ReplayEnd::Complete);
        assert_eq!(handle.result().await.unwrap().end, ReplayEnd::Complete);
        assert!(matches!(
            handle.result().await,
            Err(RithmicError::InvalidArgument(_))
        ));
    }
}
