use tokio::sync::oneshot;
use tracing::{error, info, warn};

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    time::Duration,
};

use crate::{
    api::{receiver_api::RithmicResponse, rp_code::response_rp_code_info},
    error::RithmicError,
    replay::{ReplayEnd, ReplayRequest},
    rti::messages::RithmicMessage,
};

mod replay;

/// No longer used. The library does not time out requests; wrap the call in
/// [`tokio::time::timeout`] to set a deadline of your own. Removed in 4.0.0.
#[deprecated(
    since = "3.1.0",
    note = "the library no longer times out requests; wrap the call in tokio::time::timeout"
)]
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct RithmicRequest {
    pub request_id: String,
    pub responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
}

type Responder = oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>;

/// Tells the plant to send `RequestResumeBars` with `key`, so the server
/// continues the reply for `request_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resume {
    /// The replay whose reply is to be continued.
    pub(crate) request_id: String,
    /// The `request_key` the notice carried.
    pub(crate) key: String,
}

/// Matches Rithmic responses to the callers waiting on them.
///
/// A registered request is resolved by a response carrying its id, by
/// [`Self::fail_request`], or by [`Self::drain_and_drop`] on disconnect. It is
/// never failed on a clock: the caller owns its own deadline.
#[derive(Debug)]
pub struct RithmicRequestHandler {
    handle_map: HashMap<String, Responder>,
    replay_map: HashMap<String, ReplayRequest>,
    response_vec_map: HashMap<String, Vec<RithmicResponse>>,

    /// Whether `load_*` replays are continued after a truncation notice.
    resume_truncated: bool,

    /// Frames still arriving for requests nothing is waiting on, counted by
    /// request id so they are logged once rather than one line each.
    ///
    /// The server can keep sending after a truncation, a cancel or a caller
    /// timeout, and may send its final response (`rp_code` `"12"`) over a
    /// minute later. That final response removes the entry.
    late_continuations: HashMap<String, u64>,

    /// In-flight `RequestResumeBars` ids, mapped to the replay each continues.
    resumes: HashMap<String, String>,

    /// Resume keys each `load_*` replay has used since its last data frame,
    /// so a repeated notice does not trigger a second resume.
    legacy_continuation_keys: HashMap<String, HashSet<String>>,
}

impl Default for RithmicRequestHandler {
    fn default() -> Self {
        Self {
            handle_map: HashMap::new(),
            replay_map: HashMap::new(),
            response_vec_map: HashMap::new(),
            resume_truncated: true,
            late_continuations: HashMap::new(),
            resumes: HashMap::new(),
            legacy_continuation_keys: HashMap::new(),
        }
    }
}

impl RithmicRequestHandler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Choose what a truncation notice does to a pending replay: resume it
    /// (`true`, the default) or resolve the reply with the notice as its
    /// last frame, for a caller that pages replays itself. See
    /// [`RithmicResponse::is_truncated`].
    pub fn set_resume_truncated(&mut self, resume: bool) {
        self.resume_truncated = resume;
    }

    /// Register a request. It waits until a response carries its id, until it
    /// is failed, or until the connection drops.
    pub fn register_request(&mut self, request: RithmicRequest) {
        self.handle_map
            .insert(request.request_id, request.responder);
    }

    /// Record that `resume_id` is the `RequestResumeBars` sent to continue
    /// `request_id`'s reply, so its acknowledgement is matched to that reply.
    pub(crate) fn register_resume(&mut self, resume_id: String, request_id: String) {
        self.resumes.insert(resume_id, request_id);
    }

    /// Hand the reply to its caller, or log one line if it stopped waiting.
    fn send_to_responder(
        &self,
        responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
        responses: Vec<RithmicResponse>,
    ) {
        if let Err(unsent) = responder.send(Ok(responses)) {
            let frames = unsent.as_ref().map(Vec::len).unwrap_or(0);
            let last = unsent.as_ref().ok().and_then(|frames| frames.last());
            let request_id = last.map(|frame| frame.request_id.as_str()).unwrap_or("");
            let rp_code = last.and_then(RithmicResponse::rp_code).unwrap_or(&[]);

            info!(
                "request_id {}: the caller stopped waiting before the reply arrived; {} frames \
                 dropped, final rp_code {:?}",
                request_id, frames, rp_code
            );
        }
    }

    /// Remove a pending request and send an error through its oneshot channel.
    ///
    /// Also removes any partially-accumulated multi-part responses for the same
    /// request ID so that `response_vec_map` does not retain stale data.
    ///
    /// Returns `true` if the request was found and the error was sent.
    pub fn fail_request(&mut self, request_id: &str, error: RithmicError) -> bool {
        self.legacy_continuation_keys.remove(request_id);
        if let Some(request) = self.replay_map.remove(request_id) {
            // Only a request the venue saw can still be streaming for an id
            // nothing is waiting on; one that failed to send cannot.
            if request.was_sent() {
                self.expect_late_frames(request_id);
            }
            request.finish(ReplayEnd::Failed(error));
            return true;
        }
        self.response_vec_map.remove(request_id);
        self.resumes.retain(|_, replay| replay != request_id);
        if let Some(responder) = self.handle_map.remove(request_id) {
            let _ = responder.send(Err(error));
            true
        } else {
            false
        }
    }

    /// Route one response. Returns the resume the plant must send when the
    /// response is the notice that closes a truncated replay somebody is
    /// still waiting on; `None` otherwise.
    pub(crate) fn handle_response(&mut self, response: RithmicResponse) -> Option<Resume> {
        self.release_cancelled_replays();

        if self.replay_map.contains_key(&response.request_id) {
            return self.handle_replay_response(response);
        }

        match response.message {
            RithmicMessage::ResponseHeartbeat(_) => {
                if let Some(responder) = self.handle_map.remove(&response.request_id) {
                    self.send_to_responder(responder, vec![response]);
                }

                None
            }

            RithmicMessage::ResponseResumeBars(_)
                if self.resumes.contains_key(&response.request_id) =>
            {
                self.handle_resume_ack(response);

                None
            }

            _ if !response.multi_response => {
                self.resolve_single_frame(response);

                None
            }

            _ if response.has_more => {
                self.collect_part(response);

                None
            }

            _ if response.is_truncated()
                && self.resume_truncated
                && self.is_waiting(&response.request_id) =>
            {
                self.continue_truncated(response)
            }

            _ => {
                self.resolve_multi_part(response);

                None
            }
        }
    }

    /// Whether a caller is still waiting on `request_id`.
    fn is_waiting(&self, request_id: &str) -> bool {
        self.handle_map
            .get(request_id)
            .is_some_and(|responder| !responder.is_closed())
    }

    /// A reply that arrives as a single frame.
    fn resolve_single_frame(&mut self, response: RithmicResponse) {
        // A decode failure can end a multi-part reply early; drop its parts.
        self.response_vec_map.remove(&response.request_id);

        match self.handle_map.remove(&response.request_id) {
            Some(responder) => self.send_to_responder(responder, vec![response]),
            None => self.report_unmatched_terminal(&response),
        }
    }

    /// One part of a multi-part reply, with more to follow.
    fn collect_part(&mut self, response: RithmicResponse) {
        if replay::carries_replay_data(&response) && response.error.is_none() {
            // The server can reuse a key after new data, so forget used keys.
            self.legacy_continuation_keys.remove(&response.request_id);
        }

        // Keep parts only while the caller is waiting; otherwise count them.
        match self.handle_map.get(&response.request_id) {
            Some(responder) if !responder.is_closed() => {
                self.response_vec_map
                    .entry(response.request_id.clone())
                    .or_default()
                    .push(response);
            }

            Some(_) => {
                self.release_abandoned(&response.request_id);
                self.count_late_part(&response.request_id);
            }

            None => self.count_late_part(&response.request_id),
        }
    }

    /// A truncation notice for a reply the caller is still waiting on. Returns
    /// the [`Resume`] to send, or `None` if this key was already used since the
    /// last data. The notice itself is not added to the reply.
    fn continue_truncated(&mut self, notice: RithmicResponse) -> Option<Resume> {
        let key = notice.resume_key().unwrap_or_default().to_owned();
        let first_use = self
            .legacy_continuation_keys
            .entry(notice.request_id.clone())
            .or_default()
            .insert(key.clone());

        if !first_use {
            return None;
        }

        let parts = self
            .response_vec_map
            .get(&notice.request_id)
            .map_or(0, Vec::len);

        info!(
            "request_id {}: the venue truncated this reply after {} parts (request_key {:?}); \
             asking it to resume",
            notice.request_id, parts, key
        );

        Some(Resume {
            request_id: notice.request_id,
            key,
        })
    }

    /// The last frame of a multi-part reply, or a truncation notice that is
    /// not being continued. Hands the collected parts to the caller.
    fn resolve_multi_part(&mut self, response: RithmicResponse) {
        let Some(responder) = self.handle_map.remove(&response.request_id) else {
            self.report_unmatched_terminal(&response);
            return;
        };

        let id = response.request_id.clone();
        self.legacy_continuation_keys.remove(&id);

        let truncated = response.is_truncated();
        let mut reply = self.response_vec_map.remove(&id).unwrap_or_default();
        reply.push(response);

        if truncated {
            // Not continuing, so count whatever the server still sends.
            self.note_truncation(&id, &reply);
        }

        self.send_to_responder(responder, reply);
    }

    /// Handle the answer to a `RequestResumeBars`. A refusal fails the replay,
    /// so partial data is never returned as complete.
    fn handle_resume_ack(&mut self, ack: RithmicResponse) {
        let Some(replay) = self.resumes.remove(&ack.request_id) else {
            return;
        };

        if let Some(error) = ack.error.clone() {
            if let Some(mut request) = self.replay_map.remove(&replay) {
                request.responses.push(ack);
                request.finish(ReplayEnd::Refused(error));
                self.expect_late_frames(replay);
            } else if let Some(responder) = self.handle_map.remove(&replay) {
                let parts = self.response_vec_map.remove(&replay).unwrap_or_default();
                self.legacy_continuation_keys.remove(&replay);

                warn!(
                    "request_id {}: the venue refused to resume request_id {} ({}); {} parts are incomplete",
                    ack.request_id,
                    replay,
                    error,
                    parts.len()
                );

                let _ = responder.send(Err(error));
                self.expect_late_frames(replay);
            }
        } else {
            if ack.rp_code_num() == Some("0") {
                if let Some(request) = self.replay_map.get(&replay) {
                    request.record_progress(|_| {});
                }
            }

            info!(
                "request_id {}: the venue acknowledged the resume of request_id {}",
                ack.request_id, replay
            );
        }
    }

    /// Start counting frames the server may still send for `request_id`.
    fn expect_late_frames(&mut self, request_id: impl Into<String>) {
        self.late_continuations.insert(request_id.into(), 0);
    }

    /// Log that a reply is being returned incomplete, and count what follows.
    fn note_truncation(&mut self, request_id: &str, reply: &[RithmicResponse]) {
        let key = reply
            .last()
            .and_then(RithmicResponse::resume_key)
            .unwrap_or("");
        let parts = reply.len().saturating_sub(1);
        self.expect_late_frames(request_id);

        info!(
            "request_id {}: the venue truncated this reply after {} parts (request_key {:?}); \
             the rest of the window was not delivered and what the venue still sends for it is \
             counted",
            request_id, parts, key
        );
    }

    /// Drop a request whose caller stopped waiting, and count the rest of its
    /// reply.
    fn release_abandoned(&mut self, request_id: &str) {
        self.handle_map.remove(request_id);
        self.legacy_continuation_keys.remove(request_id);
        let parts = self
            .response_vec_map
            .remove(request_id)
            .map_or(0, |p| p.len());
        self.expect_late_frames(request_id);

        info!(
            "request_id {}: the caller stopped waiting after {} parts; the rest of this reply \
             is counted, not kept",
            request_id, parts
        );
    }

    /// Count a part for an id nothing is waiting on. Only the first is logged.
    fn count_late_part(&mut self, request_id: &str) {
        match self.late_continuations.entry(request_id.to_string()) {
            Entry::Vacant(slot) => {
                slot.insert(1);

                info!(
                    "request_id {}: parts are arriving after the reply was resolved; counting them",
                    request_id
                );
            }
            Entry::Occupied(mut parts) => *parts.get_mut() += 1,
        }
    }

    /// Handle a final frame nothing is waiting for. Expected leftovers (late
    /// frames, duplicate acknowledgements) are logged at INFO; anything else
    /// at ERROR.
    fn report_unmatched_terminal(&mut self, response: &RithmicResponse) {
        if response.is_truncated() {
            // A cut is not the remote end, including after local cancellation.
            self.late_continuations
                .entry(response.request_id.clone())
                .or_insert(0);
            return;
        }
        let rp_code = response.rp_code().unwrap_or(&[]);
        if matches!(response.message, RithmicMessage::ResponseResumeBars(_)) {
            info!(
                "request_id {}: a resume acknowledgement nothing is waiting on, rp_code {:?}",
                response.request_id, rp_code
            );
            return;
        }
        let replay_or_decode_failure = matches!(
            response.message,
            RithmicMessage::ResponseTimeBarReplay(_)
                | RithmicMessage::ResponseTickBarReplay(_)
                | RithmicMessage::ResponseVolumeProfileMinuteBars(_)
        ) || response.error.is_some();
        if rp_code.is_empty()
            && replay_or_decode_failure
            && self.late_continuations.contains_key(&response.request_id)
        {
            // A malformed dataless frame or correlated decode failure supplies
            // no evidence that the server stopped streaming this request.
            return;
        }
        match self.late_continuations.remove(&response.request_id) {
            Some(parts) => info!(
                "request_id {}: the venue kept streaming after nothing was waiting: {} more \
                 parts, then a final response with rp_code {:?}",
                response.request_id, parts, rp_code
            ),
            None => error!(
                "request_id {}: no caller waiting; message {}, rp_code {:?}",
                response.request_id,
                response_rp_code_info(&response.message).map_or("Unknown", |(name, _)| name),
                rp_code
            ),
        }
    }

    /// Send [`RithmicError::ConnectionClosed`] to all pending request responders, then clear
    /// internal state.
    ///
    /// Call this during an unclean shutdown (e.g., abort) to unblock any tasks that are
    /// waiting for a response that will never arrive.
    pub fn drain_and_drop(&mut self) {
        for (_, request) in self.replay_map.drain() {
            request.finish(ReplayEnd::Failed(RithmicError::ConnectionClosed));
        }
        for (_, responder) in self.handle_map.drain() {
            let _ = responder.send(Err(RithmicError::ConnectionClosed));
        }
        self.response_vec_map.clear();
        self.late_continuations.clear();
        self.resumes.clear();
        self.legacy_continuation_keys.clear();
    }
}

/// Capture of emitted log lines, so a test can assert a diagnostic really
/// reaches production logs instead of trusting that the call is there.
#[cfg(test)]
pub(crate) mod log_capture {
    use std::{cell::RefCell, io, sync::OnceLock};

    use tracing_subscriber::fmt::MakeWriter;

    thread_local! {
        /// `Some` only while this thread is inside `capture`; events emitted on
        /// any other thread are written nowhere.
        static BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    }

    struct Writer;

    impl io::Write for Writer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            BUFFER.with_borrow_mut(|buffer| {
                if let Some(buffer) = buffer {
                    buffer.extend_from_slice(buf);
                }
            });

            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct PerThread;

    impl<'a> MakeWriter<'a> for PerThread {
        type Writer = Writer;

        fn make_writer(&'a self) -> Writer {
            Writer
        }
    }

    /// Run `f` and return what it logged. Capped at INFO, because a diagnostic
    /// that only appears at debug is filtered out in production. The subscriber
    /// is global: `tracing` caches a callsite's interest on first resolve.
    pub(crate) fn capture<T>(f: impl FnOnce() -> T) -> (T, String) {
        static INSTALLED: OnceLock<()> = OnceLock::new();

        INSTALLED.get_or_init(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(PerThread)
                .with_max_level(tracing::Level::INFO)
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("nothing else installs a global subscriber in the test binary");
        });

        BUFFER.set(Some(Vec::new()));
        let out = f();
        let logged = BUFFER.take().unwrap_or_default();

        (out, String::from_utf8(logged).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::rti::{
        ResponseHeartbeat, ResponseLogin, ResponseReferenceData, ResponseResumeBars,
        ResponseVolumeProfileMinuteBars,
    };

    fn make_response(id: &str, message: RithmicMessage) -> RithmicResponse {
        RithmicResponse {
            request_id: id.to_string(),
            message,
            is_update: false,
            has_more: false,
            multi_response: false,
            error: None,
            source: "test".to_string(),
        }
    }

    fn login_message() -> RithmicMessage {
        RithmicMessage::ResponseLogin(ResponseLogin::default())
    }

    fn heartbeat_message() -> RithmicMessage {
        RithmicMessage::ResponseHeartbeat(ResponseHeartbeat::default())
    }

    fn ref_data_message() -> RithmicMessage {
        RithmicMessage::ResponseReferenceData(ResponseReferenceData::default())
    }

    /// The message the history plant answers template 208 with. An empty
    /// `rp_code` is what a data part carries; `["12", "output inhibited"]` is
    /// what the venue ends an over-budget window with.
    fn volume_profile_message(rp_code: &[&str]) -> RithmicMessage {
        RithmicMessage::ResponseVolumeProfileMinuteBars(ResponseVolumeProfileMinuteBars {
            rp_code: rp_code.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        })
    }

    /// The notice that closes a truncated replay: a `request_key`, no
    /// response code, no data.
    fn truncation_notice(key: &str) -> RithmicMessage {
        RithmicMessage::ResponseVolumeProfileMinuteBars(ResponseVolumeProfileMinuteBars {
            request_key: Some(key.to_string()),
            ..Default::default()
        })
    }

    /// A part that carries replay data: a marker is what the venue sets on a
    /// volume-profile frame that does.
    fn marker_part(id: &str) -> RithmicResponse {
        part(
            id,
            RithmicMessage::ResponseVolumeProfileMinuteBars(ResponseVolumeProfileMinuteBars {
                marker: Some(60),
                ..Default::default()
            }),
        )
    }

    /// A data part of a multi-part reply: `has_more` is set, so the venue is
    /// saying more is coming.
    fn part(id: &str, message: RithmicMessage) -> RithmicResponse {
        let mut response = make_response(id, message);
        response.multi_response = true;
        response.has_more = true;
        response
    }

    /// The frame that ends a multi-part reply.
    fn terminal(id: &str, message: RithmicMessage) -> RithmicResponse {
        let mut response = make_response(id, message);
        response.multi_response = true;
        response
    }

    // =========================================================================
    // Single response
    // =========================================================================

    #[test]
    fn single_response_delivered_to_responder() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "1".to_string(),
            responder: tx,
        });

        handler.handle_response(make_response("1", login_message()));

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].request_id, "1");
    }

    #[test]
    fn single_response_removes_request_from_handler() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "1".to_string(),
            responder: tx,
        });

        handler.handle_response(make_response("1", login_message()));
        let _ = rx.try_recv().unwrap();

        // A second response for the same ID should not panic (just logs error)
        handler.handle_response(make_response("1", login_message()));
    }

    // =========================================================================
    // Multi-part responses
    // =========================================================================

    #[test]
    fn multi_response_collects_all_parts() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "2".to_string(),
            responder: tx,
        });

        // Two intermediate responses with has_more = true
        for _ in 0..2 {
            let mut resp = make_response("2", ref_data_message());
            resp.multi_response = true;
            resp.has_more = true;
            handler.handle_response(resp);
        }

        // Final response with has_more = false
        let mut final_resp = make_response("2", ref_data_message());
        final_resp.multi_response = true;
        final_resp.has_more = false;
        handler.handle_response(final_resp);

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn multi_response_single_message_no_has_more() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "3".to_string(),
            responder: tx,
        });

        // multi_response = true but has_more = false (single-item multi-response)
        let mut resp = make_response("3", ref_data_message());
        resp.multi_response = true;
        resp.has_more = false;
        handler.handle_response(resp);

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
    }

    // =========================================================================
    // Heartbeat responses
    // =========================================================================

    #[test]
    fn heartbeat_delivered_when_responder_registered() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "hb".to_string(),
            responder: tx,
        });

        handler.handle_response(make_response("hb", heartbeat_message()));

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn heartbeat_without_responder_does_not_panic() {
        let mut handler = RithmicRequestHandler::new();
        // No responder registered — should silently ignore
        handler.handle_response(make_response("hb", heartbeat_message()));
    }

    // =========================================================================
    // fail_request
    // =========================================================================

    #[test]
    fn fail_request_sends_error_and_returns_true() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "fail".to_string(),
            responder: tx,
        });

        assert!(handler.fail_request("fail", RithmicError::SendFailed));

        let result = rx.try_recv().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn fail_request_returns_false_for_unknown_id() {
        let mut handler = RithmicRequestHandler::new();
        assert!(!handler.fail_request("unknown", RithmicError::SendFailed));
    }

    // =========================================================================
    // drain_and_drop
    // =========================================================================

    #[test]
    fn drain_and_drop_sends_connection_closed_to_all_pending() {
        let mut handler = RithmicRequestHandler::new();
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "a".to_string(),
            responder: tx1,
        });

        handler.register_request(RithmicRequest {
            request_id: "b".to_string(),
            responder: tx2,
        });

        handler.drain_and_drop();

        for mut rx in [rx1, rx2] {
            let err = rx.try_recv().unwrap().unwrap_err();
            assert!(matches!(err, RithmicError::ConnectionClosed));
        }
    }

    #[test]
    fn drain_and_drop_clears_partial_multi_responses() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, _rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "m".to_string(),
            responder: tx,
        });

        // Accumulate a partial multi-response
        let mut resp = make_response("m", ref_data_message());
        resp.multi_response = true;
        resp.has_more = true;
        handler.handle_response(resp);

        handler.drain_and_drop();

        assert!(handler.response_vec_map.is_empty());

        // After drain, a new request with the same ID should work cleanly.
        let (tx2, mut rx2) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "m".to_string(),
            responder: tx2,
        });

        // Probe with a terminal multi-part response: only that branch merges
        // `response_vec_map`, so only it can observe a leftover part.
        let mut probe = make_response("m", ref_data_message());
        probe.multi_response = true;
        probe.has_more = false;
        handler.handle_response(probe);

        let result = rx2.try_recv().unwrap().unwrap();
        assert_eq!(
            result.len(),
            1,
            "a stale partial part must not be merged into a later response"
        );
    }

    // =========================================================================
    // Requests with no caller waiting
    // =========================================================================

    fn register(
        handler: &mut RithmicRequestHandler,
        id: &str,
    ) -> oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>> {
        let (tx, rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: id.to_string(),
            responder: tx,
        });

        rx
    }

    #[test]
    fn a_failed_request_clears_its_partial_multi_response() {
        let mut handler = RithmicRequestHandler::new();
        let rx = register(&mut handler, "m");

        let mut part = make_response("m", ref_data_message());
        part.multi_response = true;
        part.has_more = true;
        handler.handle_response(part);

        handler.fail_request("m", RithmicError::ConnectionClosed);
        drop(rx);

        assert!(handler.response_vec_map.is_empty());

        // A new request reusing the id must not inherit the stale part. Only a
        // terminal multi-part response merges the map, so probe with one.
        let mut rx2 = register(&mut handler, "m");

        let mut probe = make_response("m", ref_data_message());
        probe.multi_response = true;
        probe.has_more = false;
        handler.handle_response(probe);

        assert_eq!(rx2.try_recv().unwrap().unwrap().len(), 1);
    }

    #[test]
    fn parts_arriving_after_a_failure_do_not_re_create_the_partial_buffer() {
        let mut handler = RithmicRequestHandler::new();
        let rx = register(&mut handler, "42");

        let mut first = make_response("42", ref_data_message());
        first.multi_response = true;
        first.has_more = true;
        handler.handle_response(first);

        handler.fail_request("42", RithmicError::ConnectionClosed);
        drop(rx);

        // The server resumes streaming the request that was just failed.
        for _ in 0..3 {
            let mut late = make_response("42", ref_data_message());
            late.multi_response = true;
            late.has_more = true;
            handler.handle_response(late);
        }

        assert!(
            handler.response_vec_map.is_empty(),
            "parts for an unregistered id must not re-create the buffer"
        );
        assert_eq!(
            handler.late_continuations.get("42"),
            Some(&3),
            "the parts are counted, not accumulated"
        );

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(terminal("42", ref_data_message()));
        });

        assert!(handler.response_vec_map.is_empty());
        assert!(
            !handler.late_continuations.contains_key("42"),
            "the terminal frame ends the continuation"
        );
        assert!(logged.contains("3 more parts"), "{logged}");
        assert!(!logged.contains("ERROR"), "{logged}");
    }

    #[test]
    fn parts_for_a_never_registered_id_do_not_accumulate() {
        let mut handler = RithmicRequestHandler::new();

        for _ in 0..3 {
            let mut part = make_response("ghost", ref_data_message());
            part.multi_response = true;
            part.has_more = true;
            handler.handle_response(part);
        }

        assert!(
            handler.response_vec_map.is_empty(),
            "parts for an id that was never registered must not accumulate"
        );
    }

    #[test]
    fn a_part_whose_request_is_gone_is_counted_without_a_warning() {
        let mut handler = RithmicRequestHandler::new();

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(part("gone", ref_data_message()));
        });

        assert_eq!(logged.lines().count(), 1, "{logged}");
        assert!(logged.contains("INFO"), "{logged}");
        assert!(
            logged.contains("request_id gone: parts are arriving after the reply was resolved"),
            "{logged}"
        );
        assert_eq!(handler.late_continuations.get("gone"), Some(&1));
    }

    // =========================================================================
    // The venue continues after its own end marker
    //
    // Observed 2026-09-12 on RequestVolumeProfileMinuteBars (template 208): the
    // history plant sent thousands of parts, then a dataless end marker, then
    // ~170 more parts for the same request id, then — 70-85s later — a final
    // response with rp_code ["12", "output inhibited"].
    // =========================================================================

    #[test]
    fn late_parts_for_a_resolved_id_are_counted_and_produce_no_warning() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "208");

        // The end marker resolves the request and removes the responder.
        handler.handle_response(terminal("208", volume_profile_message(&["0"])));
        assert_eq!(rx.try_recv().unwrap().unwrap().len(), 1);

        let (_, logged) = log_capture::capture(|| {
            for _ in 0..170 {
                handler.handle_response(part("208", volume_profile_message(&[])));
            }
        });

        assert_eq!(
            logged.lines().count(),
            1,
            "170 parts put one line in the log, not 170: {logged}"
        );
        assert!(!logged.contains("WARN"), "{logged}");
        assert!(
            logged.contains("request_id 208: parts are arriving after the reply was resolved"),
            "{logged}"
        );
        assert_eq!(handler.late_continuations.get("208"), Some(&170));
        assert!(
            handler.response_vec_map.is_empty(),
            "a late part is counted, never accumulated"
        );
    }

    #[test]
    fn a_late_terminal_reports_the_continuation_once_at_info_and_clears_it() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "208");

        handler.handle_response(terminal("208", volume_profile_message(&["0"])));
        let _ = rx.try_recv();

        for _ in 0..170 {
            handler.handle_response(part("208", volume_profile_message(&[])));
        }

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(terminal(
                "208",
                volume_profile_message(&["12", "output inhibited"]),
            ));
        });

        assert_eq!(logged.lines().count(), 1, "{logged}");
        assert!(logged.contains("INFO"), "{logged}");
        assert!(logged.contains("request_id 208"), "{logged}");
        assert!(logged.contains("170 more parts"), "{logged}");
        assert!(logged.contains("output inhibited"), "{logged}");
        assert!(logged.contains("\"12\""), "{logged}");
        assert!(!logged.contains("RithmicResponse {"), "{logged}");
        assert!(
            !handler.late_continuations.contains_key("208"),
            "the final response ends the continuation"
        );

        // With the entry gone, a further terminal for the same id is once again
        // the genuinely unexpected case.
        let (_, second) = log_capture::capture(|| {
            handler.handle_response(terminal(
                "208",
                volume_profile_message(&["12", "output inhibited"]),
            ));
        });

        assert!(second.contains("ERROR"), "{second}");
        assert!(!second.contains("more parts"), "{second}");
    }

    #[test]
    fn a_terminal_that_was_never_a_continuation_is_named_not_dumped() {
        let mut handler = RithmicRequestHandler::new();

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(make_response("ghost", login_message()));
        });

        assert_eq!(logged.lines().count(), 1, "{logged}");
        assert!(logged.contains("ERROR"), "{logged}");
        assert!(logged.contains("request_id ghost"), "{logged}");
        assert!(
            logged.contains("ResponseLogin"),
            "the line names the message variant: {logged}"
        );
        assert!(
            !logged.contains("RithmicResponse {"),
            "the line must not dump the response: {logged}"
        );
    }

    #[test]
    fn drain_and_drop_clears_late_continuations() {
        let mut handler = RithmicRequestHandler::new();

        handler.handle_response(part("208", volume_profile_message(&[])));
        assert!(handler.late_continuations.contains_key("208"));

        handler.drain_and_drop();

        assert!(handler.late_continuations.is_empty());
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn response_for_unregistered_id_does_not_panic() {
        let mut handler = RithmicRequestHandler::new();

        handler.handle_response(make_response("ghost", login_message()));
    }

    /// The caller gave up mid-reply — its own deadline elapsed and it dropped
    /// its receiver — while the venue is still streaming. The next part frees the responder and the parts held for
    /// nobody, says so once, and the rest of the reply is counted as a late
    /// continuation like any other.
    #[test]
    fn a_caller_that_stopped_waiting_mid_reply_frees_the_buffer_and_counts_the_rest_as_late() {
        let mut handler = RithmicRequestHandler::new();
        let rx = register(&mut handler, "9");
        handler.handle_response(part("9", volume_profile_message(&[])));
        handler.handle_response(part("9", volume_profile_message(&[])));
        drop(rx);

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(part("9", volume_profile_message(&[])));
        });

        assert!(
            !handler.handle_map.contains_key("9"),
            "the responder is released"
        );
        assert!(
            handler.response_vec_map.is_empty(),
            "parts held for nobody are freed"
        );
        assert_eq!(handler.late_continuations.get("9"), Some(&1));
        assert_eq!(logged.lines().count(), 1, "{logged}");
        assert!(logged.contains("INFO"), "{logged}");
        assert!(
            logged.contains("request_id 9: the caller stopped waiting after 2 parts"),
            "{logged}"
        );
        assert!(!logged.contains("RithmicResponse {"), "{logged}");

        let (_, ended) = log_capture::capture(|| {
            handler.handle_response(terminal(
                "9",
                volume_profile_message(&["12", "output inhibited"]),
            ));
        });
        assert!(ended.contains("1 more part"), "{ended}");
        assert!(!ended.contains("ERROR"), "{ended}");
        assert!(handler.late_continuations.is_empty());
    }

    /// The caller gave up and the very next frame is the terminal: the
    /// reply has nowhere to go, and that is said in one line with the frame
    /// count and the venue's code — never as a dump of every frame.
    #[test]
    fn a_reply_for_a_caller_that_stopped_waiting_is_one_line_not_a_dump() {
        let mut handler = RithmicRequestHandler::new();
        let rx = register(&mut handler, "9");
        handler.handle_response(part("9", volume_profile_message(&[])));
        handler.handle_response(part("9", volume_profile_message(&[])));
        drop(rx);

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(terminal("9", volume_profile_message(&["0"])));
        });

        assert!(handler.handle_map.is_empty() && handler.response_vec_map.is_empty());
        assert_eq!(logged.lines().count(), 1, "{logged}");
        assert!(!logged.contains("ERROR"), "{logged}");
        assert!(
            logged.contains(
                "request_id 9: the caller stopped waiting before the reply arrived; 3 frames dropped"
            ),
            "{logged}"
        );
        assert!(logged.contains("rp_code [\"0\"]"), "{logged}");
        assert!(!logged.contains("RithmicResponse {"), "{logged}");
    }

    #[test]
    fn dropped_receiver_does_not_panic() {
        let mut handler = RithmicRequestHandler::new();
        let (tx, rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "drop".to_string(),
            responder: tx,
        });

        drop(rx);
        // Sending to a dropped receiver should not panic (just logs error)
        handler.handle_response(make_response("drop", login_message()));
    }

    #[test]
    fn single_response_clears_partial_multi_responses_for_the_same_id() {
        // Mirrors a decode failure landing mid multi-part response.
        let mut handler = RithmicRequestHandler::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "m".to_string(),
            responder: tx,
        });

        let mut partial = make_response("m", ref_data_message());
        partial.multi_response = true;
        partial.has_more = true;
        handler.handle_response(partial);

        let mut failure = make_response("m", RithmicMessage::Unknown);
        failure.error = Some(crate::error::RithmicError::ProtocolError("bad".to_string()));
        handler.handle_response(failure);

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1, "only the terminating frame is delivered");
        assert!(matches!(result[0].message, RithmicMessage::Unknown));

        let (tx2, mut rx2) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "m".to_string(),
            responder: tx2,
        });

        let mut terminal = make_response("m", login_message());
        terminal.multi_response = true;
        handler.handle_response(terminal);

        let result = rx2.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1, "no stale part may be prepended");
        assert!(matches!(
            result[0].message,
            RithmicMessage::ResponseLogin(_)
        ));
    }
    // =========================================================================
    // Truncated replays
    // =========================================================================

    /// The venue closes an over-budget replay with a truncation notice while
    /// the caller is waiting: the caller keeps waiting, the parts stay, the
    /// plant is told to resume with the key, the acknowledgement is consumed,
    /// the continuation joins the parts, and the venue's real end marker
    /// resolves the whole reply — without the notice in it.
    #[test]
    fn a_truncation_notice_keeps_the_caller_waiting_and_asks_to_resume() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "7");

        assert_eq!(
            handler.handle_response(part("7", volume_profile_message(&[]))),
            None
        );
        assert_eq!(
            handler.handle_response(part("7", volume_profile_message(&[]))),
            None
        );

        let (resume, logged) =
            log_capture::capture(|| handler.handle_response(terminal("7", truncation_notice("0"))));
        assert_eq!(
            resume,
            Some(Resume {
                request_id: "7".to_string(),
                key: "0".to_string(),
            })
        );
        assert!(
            logged.contains(
                "request_id 7: the venue truncated this reply after 2 parts (request_key \"0\"); \
                 asking it to resume"
            ),
            "{logged}"
        );
        assert!(rx.try_recv().is_err(), "the caller keeps waiting");

        handler.register_resume("9".to_string(), "7".to_string());
        let (ack, logged) = log_capture::capture(|| {
            handler.handle_response(terminal(
                "9",
                RithmicMessage::ResponseResumeBars(ResponseResumeBars {
                    rp_code: vec!["0".to_string()],
                    ..Default::default()
                }),
            ))
        });
        assert_eq!(ack, None);
        assert!(
            logged.contains("acknowledged the resume of request_id 7"),
            "{logged}"
        );
        assert!(!logged.contains("no caller waiting"), "{logged}");

        handler.handle_response(part("7", volume_profile_message(&[])));
        handler.handle_response(terminal("7", volume_profile_message(&["0"])));

        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(
            reply.len(),
            4,
            "three parts and the end marker; the notice is not data"
        );
        assert!(reply.iter().all(|frame| !frame.is_truncated()));
        assert!(reply[3].rp_code() == Some(&["0".to_string()][..]));
        assert!(handler.late_continuations.is_empty());
        assert!(handler.resumes.is_empty());
        assert!(handler.legacy_continuation_keys.is_empty());
    }

    /// A resume key the venue repeats without intervening data is not asked
    /// for again; data for the reply re-arms it, because the venue reuses a
    /// key across cuts of the same replay. The scoped replays keep the same
    /// rule on each `ReplayRequest`.
    #[test]
    fn a_repeated_resume_key_without_new_data_is_not_asked_for_again() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "7");

        handler.handle_response(part("7", volume_profile_message(&[])));
        assert_eq!(
            handler.handle_response(terminal("7", truncation_notice("0"))),
            Some(Resume {
                request_id: "7".to_string(),
                key: "0".to_string(),
            })
        );

        let (resume, logged) =
            log_capture::capture(|| handler.handle_response(terminal("7", truncation_notice("0"))));
        assert_eq!(resume, None, "a repeated key is not asked for again");
        assert!(!logged.contains("asking it to resume"), "{logged}");
        assert!(rx.try_recv().is_err(), "the caller keeps waiting");

        handler.handle_response(marker_part("7"));
        assert_eq!(
            handler.handle_response(terminal("7", truncation_notice("0"))),
            Some(Resume {
                request_id: "7".to_string(),
                key: "0".to_string(),
            }),
            "data re-arms the key the venue reuses"
        );

        handler.handle_response(terminal("7", volume_profile_message(&["0"])));
        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(
            reply.len(),
            3,
            "two parts and the end marker; the notices are not data"
        );
        assert!(reply.iter().all(|frame| !frame.is_truncated()));
        assert!(handler.legacy_continuation_keys.is_empty());
        assert!(handler.resumes.is_empty());
    }

    /// The venue can acknowledge a resume twice: the first acknowledgement
    /// consumes the correlation, so the second finds no caller. One line at
    /// INFO, not an error for a reply nobody is missing.
    #[test]
    fn a_duplicate_resume_acknowledgement_is_counted_not_an_error() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "7");

        handler.handle_response(part("7", volume_profile_message(&[])));
        let resume = handler
            .handle_response(terminal("7", truncation_notice("0")))
            .expect("a pending truncated reply asks to resume");
        handler.register_resume("9".to_string(), resume.request_id);

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(terminal(
                "9",
                RithmicMessage::ResponseResumeBars(ResponseResumeBars {
                    rp_code: vec!["0".to_string()],
                    ..Default::default()
                }),
            ))
        });
        assert!(
            logged.contains("acknowledged the resume of request_id 7"),
            "{logged}"
        );

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(terminal(
                "9",
                RithmicMessage::ResponseResumeBars(ResponseResumeBars {
                    rp_code: vec!["0".to_string()],
                    ..Default::default()
                }),
            ))
        });
        assert!(
            logged.contains("request_id 9: a resume acknowledgement nothing is waiting on"),
            "{logged}"
        );
        assert!(!logged.contains("no caller waiting"), "{logged}");
        assert!(!logged.contains("ERROR"), "{logged}");

        handler.handle_response(terminal("7", volume_profile_message(&["0"])));
        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(reply.len(), 2, "one part and the end marker");
        assert!(handler.resumes.is_empty());
    }

    /// A venue that refuses the resume ends the wait: the caller gets the
    /// refusal as an error, never the prefix as a successful complete reply.
    #[test]
    fn a_refused_resume_never_reports_a_complete_prefix() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "7");

        handler.handle_response(part("7", volume_profile_message(&[])));
        let resume = handler
            .handle_response(terminal("7", truncation_notice("0")))
            .expect("a pending truncated reply asks to resume");
        handler.register_resume("9".to_string(), resume.request_id);

        let mut refusal = terminal(
            "9",
            RithmicMessage::ResponseResumeBars(ResponseResumeBars {
                rp_code: vec!["7".to_string(), "no data".to_string()],
                ..Default::default()
            }),
        );
        refusal.error = Some(RithmicError::ProtocolError("refused".to_string()));
        let (_, logged) = log_capture::capture(|| handler.handle_response(refusal));

        let reply = rx.try_recv().unwrap();
        assert_eq!(
            reply,
            Err(RithmicError::ProtocolError("refused".to_string()))
        );
        assert!(
            logged.contains("refused to resume request_id 7") && logged.contains("1 parts"),
            "{logged}"
        );
        assert!(handler.resumes.is_empty());
        assert!(handler.legacy_continuation_keys.is_empty());
    }

    /// A truncation notice for a caller that stopped waiting is not resumed:
    /// nobody would get the continuation. The reply is released, the cut is
    /// said once, and what the venue still sends for the id is counted.
    #[test]
    fn a_truncation_notice_for_a_caller_that_stopped_waiting_is_counted_not_resumed() {
        let mut handler = RithmicRequestHandler::new();
        let rx = register(&mut handler, "7");

        handler.handle_response(part("7", volume_profile_message(&[])));
        drop(rx);

        let (resume, logged) =
            log_capture::capture(|| handler.handle_response(terminal("7", truncation_notice("0"))));
        assert_eq!(resume, None);
        assert!(
            logged.contains("the venue truncated this reply after 1 parts"),
            "{logged}"
        );

        let (_, logged) = log_capture::capture(|| {
            for _ in 0..3 {
                handler.handle_response(part("7", volume_profile_message(&[])));
            }
            handler.handle_response(terminal(
                "7",
                volume_profile_message(&["12", "output inhibited"]),
            ));
        });
        assert!(!logged.contains("parts are arriving"), "{logged}");
        assert!(
            logged.contains("3 more parts") && logged.contains("output inhibited"),
            "{logged}"
        );
        assert!(handler.late_continuations.is_empty());
    }

    /// With resumption off — a caller that pages replays itself — the notice
    /// resolves the reply as its last frame, the cut is said once, and what
    /// the venue still sends for the id is counted, not resumed.
    #[test]
    fn a_truncation_notice_resolves_the_reply_when_resumption_is_off() {
        let mut handler = RithmicRequestHandler::new();
        handler.set_resume_truncated(false);
        let mut rx = register(&mut handler, "7");

        handler.handle_response(part("7", volume_profile_message(&[])));
        handler.handle_response(part("7", volume_profile_message(&[])));
        let (resume, logged) =
            log_capture::capture(|| handler.handle_response(terminal("7", truncation_notice("0"))));
        assert_eq!(resume, None, "no resume is asked for");

        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(reply.len(), 3, "the parts and the notice are delivered");
        assert!(reply[2].is_truncated());
        assert!(
            logged.contains("the venue truncated this reply after 2 parts"),
            "{logged}"
        );

        let (_, logged) = log_capture::capture(|| {
            for _ in 0..3 {
                handler.handle_response(part("7", volume_profile_message(&[])));
            }
            handler.handle_response(terminal(
                "7",
                volume_profile_message(&["12", "output inhibited"]),
            ));
        });
        assert!(!logged.contains("parts are arriving"), "{logged}");
        assert!(
            logged.contains("3 more parts") && logged.contains("output inhibited"),
            "{logged}"
        );
        assert!(handler.late_continuations.is_empty());
    }

    /// A complete replay's end marker carries `rp_code` `["0"]` and no key:
    /// it is not a truncation and opens no continuation.
    #[test]
    fn a_complete_replay_opens_no_continuation() {
        let mut handler = RithmicRequestHandler::new();
        let mut rx = register(&mut handler, "8");

        let (_, logged) = log_capture::capture(|| {
            handler.handle_response(part("8", volume_profile_message(&[])));
            handler.handle_response(terminal("8", volume_profile_message(&["0"])));
        });

        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(reply.len(), 2);
        assert!(!reply[1].is_truncated());
        assert!(!logged.contains("truncated"), "{logged}");
        assert!(handler.late_continuations.is_empty());
    }
}
