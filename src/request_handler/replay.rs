use super::{RequestResult, RequestTag, Resume, RithmicRequestHandler};
use std::collections::HashSet;
use tokio::sync::oneshot;
use tracing::info;

use crate::{RithmicError, RithmicResponse, rti::messages::RithmicMessage};

/// A history replay the plant is collecting for a caller.
///
/// Unlike other multi-part replies, a replay can be cut short by the server
/// and continued on the same request id, so it keeps its own frames and the
/// resume keys it has used.
#[derive(Debug)]
pub(crate) struct PendingReplay {
    responder: oneshot::Sender<RequestResult>,
    responses: Vec<RithmicResponse>,
    /// Resume keys used since the last data frame. The server can hand out
    /// the same key again after new data.
    used_keys: HashSet<String>,
    /// Whether the request reached the socket, so the server may still be
    /// sending frames for it after it is released.
    sent: bool,
}

impl PendingReplay {
    pub(crate) fn new(responder: oneshot::Sender<RequestResult>) -> Self {
        Self {
            responder,
            responses: Vec::new(),
            used_keys: HashSet::new(),
            sent: false,
        }
    }

    /// Whether the caller stopped waiting (it dropped its future).
    fn caller_stopped_waiting(&self) -> bool {
        self.responder.is_closed()
    }

    fn finish(self, reply: RequestResult) {
        let _ = self.responder.send(reply);
    }
}

/// Whether this frame carries replay data (a `marker`, or tick bar
/// timestamps).
fn carries_replay_data(response: &RithmicResponse) -> bool {
    match &response.message {
        RithmicMessage::ResponseTimeBarReplay(m) => m.marker.is_some(),
        RithmicMessage::ResponseTickBarReplay(m) => !m.data_bar_ssboe.is_empty(),
        RithmicMessage::ResponseVolumeProfileMinuteBars(m) => m.marker.is_some(),
        _ => false,
    }
}

impl<T: RequestTag> RithmicRequestHandler<T> {
    /// Track a replay. Returns `false` if the caller stopped waiting while the
    /// request was queued, in which case nothing should be sent.
    pub(crate) fn register_replay(&mut self, id: String, replay: PendingReplay) -> bool {
        if replay.caller_stopped_waiting() {
            return false;
        }

        self.replay_map.insert(id, replay);

        true
    }

    /// Register a replay for a test, and return its reply channel.
    #[cfg(test)]
    pub(crate) fn register_test_replay(&mut self, id: &str) -> oneshot::Receiver<RequestResult> {
        let (tx, rx) = oneshot::channel();
        assert!(self.register_replay(id.to_string(), PendingReplay::new(tx)));
        rx
    }

    /// Whether the caller of replay `id` is still waiting. Releases the replay
    /// if not.
    pub(crate) fn replay_waiting(&mut self, id: &str) -> bool {
        let abandoned = self
            .replay_map
            .get(id)
            .is_some_and(PendingReplay::caller_stopped_waiting);

        if abandoned {
            self.release_abandoned_replays();
        }

        !abandoned
    }

    /// Record that replay `id` reached the socket.
    pub(crate) fn mark_sent(&mut self, id: &str) {
        if let Some(replay) = self.replay_map.get_mut(id) {
            replay.sent = true;
        }
    }

    /// Drop every replay whose caller stopped waiting, and count whatever the
    /// server still sends for it. Called as each event reaches the plant.
    ///
    /// A replay whose write has not been reported yet is left alone: how that
    /// write went decides what happens to it.
    pub(crate) fn release_abandoned_replays(&mut self) {
        let abandoned: Vec<_> = self
            .replay_map
            .iter()
            .filter(|(_, replay)| replay.sent && replay.caller_stopped_waiting())
            .map(|(id, _)| id.clone())
            .collect();

        for id in abandoned {
            let Some(replay) = self.replay_map.remove(&id) else {
                continue;
            };

            info!(
                "request_id {}: the caller stopped waiting after {} parts; the rest of this reply \
                 is counted, not kept",
                id,
                replay.responses.len()
            );

            self.forget_resumes_of(&id);
            self.expect_late_frames(id);
        }
    }

    /// Forget any in-flight resume of replay `id`, so a late acknowledgement
    /// is not mistaken for one that matters.
    fn forget_resumes_of(&mut self, id: &str) {
        self.resumes.retain(|_, replay| replay != id);
    }

    /// Fail a replay with `error`. Returns `false` if `request_id` is not a
    /// replay.
    pub(super) fn fail_replay(&mut self, request_id: &str, error: RithmicError) -> bool {
        let Some(replay) = self.replay_map.remove(request_id) else {
            return false;
        };

        // Only a request the server saw can still be streaming.
        if replay.sent {
            self.expect_late_frames(request_id);
        }

        self.forget_resumes_of(request_id);
        replay.finish(Err(error));

        true
    }

    /// End a replay because the server refused to continue it. Returns how
    /// many parts it had, or `None` if nothing was waiting on it.
    pub(super) fn refuse_replay(
        &mut self,
        request_id: String,
        error: RithmicError,
    ) -> Option<usize> {
        let replay = self.replay_map.remove(&request_id)?;

        let parts = replay.responses.len();
        self.forget_resumes_of(&request_id);
        replay.finish(Err(error));
        self.expect_late_frames(request_id);

        Some(parts)
    }

    /// Fail every replay because the connection is gone.
    pub(super) fn drain_replays(&mut self) {
        for (_, replay) in self.replay_map.drain() {
            replay.finish(Err(RithmicError::ConnectionClosed));
        }
    }

    /// Route a frame for a replay. Returns a [`Resume`] if the server cut the
    /// reply short and should be asked to continue.
    pub(super) fn handle_replay_response(&mut self, response: RithmicResponse) -> Option<Resume> {
        let id = response.request_id.clone();
        let replay = self.replay_map.get_mut(&id)?;

        if response.is_truncated() {
            let key = response.resume_key()?.to_owned();

            // Ignore a key already used since the last data.
            if !replay.used_keys.insert(key.clone()) {
                return None;
            }

            info!(
                "request_id {}: the venue truncated this reply after {} parts (request_key {:?}); \
                 asking it to resume",
                id,
                replay.responses.len(),
                key
            );

            return Some(Resume {
                request_id: id,
                key,
            });
        }

        if carries_replay_data(&response) && response.error.is_none() {
            // The server can reuse a key after new data, so forget used keys.
            replay.used_keys.clear();
        }

        if response.has_more {
            replay.responses.push(response);
            return None;
        }

        // The server ended the reply, whatever its code says, so the caller
        // gets every frame. Without a code, more may still follow.
        let final_from_server = response.rp_code().is_some_and(|code| !code.is_empty());
        let mut replay = self.replay_map.remove(&id)?;
        self.forget_resumes_of(&id);

        replay.responses.push(response);
        let responses = std::mem::take(&mut replay.responses);
        replay.finish(Ok(responses));

        if !final_from_server {
            self.expect_late_frames(id);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::plants::tag::Tag;

    use crate::rti::{ResponseResumeBars, ResponseTimeBarReplay};

    fn frame(id: &str, marker: Option<i32>, code: &[&str], key: Option<&str>) -> RithmicResponse {
        RithmicResponse {
            request_id: id.into(),
            message: RithmicMessage::ResponseTimeBarReplay(ResponseTimeBarReplay {
                marker,
                rp_code: code.iter().map(|s| (*s).into()).collect(),
                request_key: key.map(Into::into),
                ..Default::default()
            }),
            is_update: false,
            has_more: marker.is_some(),
            multi_response: true,
            error: None,
            source: "test".into(),
        }
    }

    fn ack(id: &str, error: Option<RithmicError>) -> RithmicResponse {
        let mut response = frame(id, None, &[], None);
        response.message = RithmicMessage::ResponseResumeBars(ResponseResumeBars {
            rp_code: vec![if error.is_some() { "7" } else { "0" }.into()],
            ..Default::default()
        });
        response.error = error;
        response
    }

    #[test]
    fn a_reused_continuation_key_resumes_again_after_new_replay_data() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("original");
        let _other = handler.register_test_replay("other");
        handler.mark_sent("original");

        handler.route(frame("original", Some(1), &[], None));
        let first = handler
            .route(frame("original", None, &[], Some("0")))
            .expect("first cut asks to resume");
        handler.register_resume("resume-one".into(), first.request_id);
        handler.route(ack("resume-one", None));

        handler.route(frame("other", Some(1), &[], None));
        assert!(
            handler
                .route(frame("original", None, &[], Some("0")))
                .is_none(),
            "an acknowledgement or another replay cannot rearm the key"
        );

        handler.route(frame("original", Some(2), &[], None));
        let continued = handler
            .route(frame("original", None, &[], Some("0")))
            .expect("the venue reuses the same key after another chunk of data");
        assert_eq!(continued.request_id, "original");
        assert_eq!(continued.key, "0");
        assert!(
            handler
                .route(frame("original", None, &[], Some("0")))
                .is_none(),
            "the repeated notice stays inert until data advances again"
        );

        handler.route(frame("original", None, &["0"], None));
        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(
            reply.len(),
            3,
            "two data frames and the end; notices are left out"
        );
    }

    #[test]
    fn every_reply_the_server_ends_is_returned_whole() {
        let rejected = crate::api::rp_code::classify_rp_code_error(&["5".into(), "denied".into()]);
        for (code, error, late) in [
            (&["0"][..], None, false),
            (&["12", "output inhibited"][..], None, false),
            (&["5", "denied"][..], rejected, false),
            // No code: not proof the server has finished.
            (&[][..], None, true),
        ] {
            let mut handler = RithmicRequestHandler::<Tag>::new();
            let mut rx = handler.register_test_replay("original");

            handler.route(frame("original", Some(1), &[], None));
            let mut last = frame("original", None, code, None);
            last.error = error.clone();
            handler.route(last);

            let reply = rx.try_recv().unwrap().unwrap();
            assert_eq!(reply.len(), 2, "{code:?}");
            assert_eq!(reply[1].error, error, "{code:?}");
            assert_eq!(
                handler.late_continuations.contains_key("original"),
                late,
                "{code:?}"
            );
            assert!(handler.replay_map.is_empty());
        }
    }

    #[test]
    fn an_empty_window_is_one_final_frame() {
        for code in [
            &["7", "no data"][..],
            &["7", "an error occurred while parsing data."][..],
        ] {
            let mut handler = RithmicRequestHandler::<Tag>::new();
            let mut rx = handler.register_test_replay("original");

            let mut last = frame("original", None, code, None);
            // As the decoder classifies it.
            last.error = crate::api::rp_code::classify_rp_code_error(last.rp_code().unwrap());
            handler.route(last);

            let reply = rx.try_recv().unwrap().unwrap();
            assert_eq!(reply.len(), 1, "{code:?}");
        }
    }

    #[test]
    fn a_decode_failure_mid_replay_keeps_the_earlier_frames() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("original");

        handler.route(frame("original", Some(1), &[], None));
        handler.route(RithmicResponse {
            request_id: "original".into(),
            message: RithmicMessage::Unknown,
            is_update: false,
            has_more: false,
            multi_response: false,
            error: Some(RithmicError::ProtocolError("bad frame".into())),
            source: "test".into(),
        });

        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(reply.len(), 2, "the data frame is not dropped");
        assert!(reply[1].error.is_some());
        assert!(
            handler.late_continuations.contains_key("original"),
            "a decode failure is not proof the server has finished"
        );
    }

    #[test]
    fn a_refused_resume_returns_the_refusal_not_the_frames_so_far() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("original");

        handler.route(frame("original", Some(1), &[], None));
        handler.route(frame("original", None, &[], Some("key")));
        handler.register_resume("resume".into(), "original".into());

        let error = RithmicError::ProtocolError("refused".into());
        handler.route(ack("resume", Some(error.clone())));

        assert_eq!(rx.try_recv().unwrap(), Err(error));
        assert!(handler.replay_map.is_empty());
        assert!(handler.late_continuations.contains_key("original"));
    }

    #[test]
    fn an_abandoned_replay_is_released_and_its_late_frames_are_counted() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let rx = handler.register_test_replay("original");
        let _other = handler.register_test_replay("other");
        handler.mark_sent("original");

        handler.route(frame("original", Some(1), &[], None));
        handler.route(frame("original", None, &[], Some("key")));
        handler.register_resume("resume".into(), "original".into());

        drop(rx);
        handler.release_abandoned_replays();
        assert!(!handler.replay_map.contains_key("original"));
        assert!(handler.replay_map.contains_key("other"));
        assert!(handler.late_continuations.contains_key("original"));

        assert!(
            handler
                .route(frame("original", None, &[], Some("late")))
                .is_none(),
            "an abandoned replay is never continued"
        );
        handler.route(frame("original", None, &[], None));
        assert!(
            handler.late_continuations.contains_key("original"),
            "a frame without a code is not the server's end"
        );

        handler.route(frame("original", Some(2), &[], None));
        assert_eq!(handler.late_continuations.get("original"), Some(&1));

        handler.route(ack("resume", None));
        assert!(handler.resumes.is_empty());

        handler.route(frame("original", None, &["12"], None));
        assert!(handler.late_continuations.is_empty());
    }

    #[test]
    fn an_abandoned_request_is_not_admitted_and_an_unsent_one_expects_no_late_frames() {
        let mut handler = RithmicRequestHandler::<Tag>::new();

        let (tx, rx) = oneshot::channel();
        drop(rx);
        assert!(!handler.register_replay("unsent".into(), PendingReplay::new(tx)));
        assert!(handler.replay_map.is_empty());

        let mut failed = handler.register_test_replay("failed");
        handler.fail_request("failed", RithmicError::SendFailed);
        assert_eq!(failed.try_recv().unwrap(), Err(RithmicError::SendFailed));
        assert!(
            !handler.late_continuations.contains_key("failed"),
            "the server never saw a request that failed to send"
        );

        let mut sent = handler.register_test_replay("sent");
        handler.mark_sent("sent");
        handler.fail_request("sent", RithmicError::SendFailed);
        assert_eq!(sent.try_recv().unwrap(), Err(RithmicError::SendFailed));
        assert_eq!(
            handler.late_continuations.get("sent"),
            Some(&0),
            "a request the server saw can still be streaming"
        );
    }

    /// A replay whose caller leaves before its write is reported is kept until
    /// the write is, so a replay the server did see still expects its late
    /// frames.
    #[test]
    fn an_abandoned_replay_waits_for_its_write_to_be_reported() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        drop(handler.register_test_replay("replay"));

        handler.release_abandoned_replays();
        assert!(
            handler.replay_map.contains_key("replay"),
            "its write is not reported yet"
        );

        handler.mark_sent("replay");
        handler.release_abandoned_replays();
        assert!(handler.replay_map.is_empty());
        assert_eq!(handler.late_continuations.get("replay"), Some(&0));
    }

    #[test]
    fn a_dropped_connection_fails_every_replay() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("original");

        handler.route(frame("original", Some(1), &[], None));
        handler.register_resume("resume".into(), "original".into());
        handler.drain_and_drop();

        assert_eq!(rx.try_recv().unwrap(), Err(RithmicError::ConnectionClosed));
        assert!(handler.replay_map.is_empty());
        assert!(handler.resumes.is_empty());
    }
}
