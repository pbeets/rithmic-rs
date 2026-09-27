use tokio::sync::oneshot;
use tracing::{error, info, warn};

use std::{
    collections::{HashMap, hash_map::Entry},
    time::Duration,
};

use crate::{
    api::{receiver_api::RithmicResponse, rp_code::response_rp_code_info},
    error::RithmicError,
    rti::messages::RithmicMessage,
};

mod replay;

pub(crate) use replay::PendingReplay;

/// No longer used. The library does not time out requests; wrap the call in
/// [`tokio::time::timeout`] to set a deadline of your own. Removed in 4.0.0.
#[deprecated(
    since = "3.1.0",
    note = "the library no longer times out requests; wrap the call in tokio::time::timeout"
)]
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// What a request got back: every frame of its reply, or the error that
/// ended it. The error is often ours, such as `ConnectionClosed`.
pub(crate) type RequestResult = Result<Vec<RithmicResponse>, RithmicError>;

/// The channel a handle method waits on for its reply.
pub(crate) type Responder = oneshot::Sender<RequestResult>;

/// What the handler keeps for a request until its reply completes.
pub(crate) trait RequestTag {
    /// Whether the caller waiting on the reply dropped its future, so the
    /// reply's parts need not be kept. Never true for a request the plant sent
    /// for itself.
    fn caller_stopped_waiting(&self) -> bool;
}

/// Tells the plant to send `RequestResumeBars` with `key`, so the server
/// continues the reply for `request_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resume {
    /// The replay whose reply is to be continued.
    pub(crate) request_id: String,
    /// The `request_key` the notice carried.
    pub(crate) key: String,
}

/// What the plant must do with a response the handler has routed.
#[derive(Debug)]
pub(crate) enum Routed<T> {
    /// A reply completed: answer whatever `T` stands for.
    Reply(T, RequestResult),
    /// The server cut a replay short: ask it to continue.
    Resume(Resume),
}

/// Matches Rithmic responses to the requests waiting on them.
///
/// A registered request is resolved by a response carrying its id, by
/// [`Self::fail_request`], or by [`Self::drain_and_drop`] on disconnect, and
/// dropped once its caller stops waiting. It is never failed on a clock: the
/// caller owns its own deadline.
///
/// The handler keeps a tag `T` for each request and hands it back with the
/// completed reply, so the plant decides who gets it.
#[derive(Debug)]
pub(crate) struct RithmicRequestHandler<T> {
    handle_map: HashMap<String, T>,
    response_vec_map: HashMap<String, Vec<RithmicResponse>>,

    /// History replays, which the server can cut short and continue on the
    /// same request id.
    replay_map: HashMap<String, PendingReplay>,

    /// Frames still arriving for requests nothing is waiting on, counted by
    /// request id so they are logged once rather than one line each.
    ///
    /// The server can keep sending after a truncation, a cancel or a caller
    /// timeout, and may send its final response (`rp_code` `"12"`) over a
    /// minute later. That final response removes the entry.
    late_continuations: HashMap<String, u64>,

    /// In-flight `RequestResumeBars` ids, mapped to the replay each continues.
    resumes: HashMap<String, String>,
}

impl<T: RequestTag> RithmicRequestHandler<T> {
    pub(crate) fn new() -> Self {
        Self {
            handle_map: HashMap::new(),
            response_vec_map: HashMap::new(),
            replay_map: HashMap::new(),
            late_continuations: HashMap::new(),
            resumes: HashMap::new(),
        }
    }

    /// Register a request. It waits until a response carries its id, until it
    /// is failed, or until the connection drops.
    pub(crate) fn register_request(&mut self, request_id: String, tag: T) {
        self.handle_map.insert(request_id, tag);
    }

    /// Record that `resume_id` is the `RequestResumeBars` sent to continue
    /// `request_id`'s reply, so its acknowledgement is matched to that reply.
    pub(crate) fn register_resume(&mut self, resume_id: String, request_id: String) {
        self.resumes.insert(resume_id, request_id);
    }

    /// Remove a pending request, and any parts collected for it, and fail it
    /// with `error`.
    ///
    /// A replay is failed here. Any other request is handed back with its
    /// failed reply for the plant to answer. Returns `None` for a replay or an
    /// unknown id.
    pub(crate) fn fail_request(
        &mut self,
        request_id: &str,
        error: RithmicError,
    ) -> Option<(T, RequestResult)> {
        if self.fail_replay(request_id, error.clone()) {
            return None;
        }

        self.response_vec_map.remove(request_id);

        self.handle_map
            .remove(request_id)
            .map(|tag| (tag, Err(error)))
    }

    /// Route one response. Returns the completed reply for the plant to
    /// answer, or the resume it must send when the server cut a replay short;
    /// `None` otherwise.
    pub(crate) fn handle_response(&mut self, response: RithmicResponse) -> Option<Routed<T>> {
        self.release_abandoned_replays();

        if self.replay_map.contains_key(&response.request_id) {
            return self.handle_replay_response(response).map(Routed::Resume);
        }

        match response.message {
            RithmicMessage::ResponseResumeBars(_)
                if self.resumes.contains_key(&response.request_id) =>
            {
                self.handle_resume_ack(response);

                None
            }

            _ if !response.multi_response => self.resolve_single_frame(response),

            _ if response.has_more => {
                self.collect_part(response);

                None
            }

            _ => self.resolve_multi_part(response),
        }
    }

    /// A reply that arrives as a single frame.
    fn resolve_single_frame(&mut self, response: RithmicResponse) -> Option<Routed<T>> {
        // A decode failure can end a multi-part reply early; drop its parts.
        self.response_vec_map.remove(&response.request_id);

        match self.handle_map.remove(&response.request_id) {
            Some(tag) => Some(Routed::Reply(tag, Ok(vec![response]))),
            None => {
                self.report_unmatched_terminal(&response);

                None
            }
        }
    }

    /// One part of a multi-part reply, with more to follow.
    fn collect_part(&mut self, response: RithmicResponse) {
        // Keep parts only while the caller is waiting; otherwise count them.
        match self.handle_map.get(&response.request_id) {
            Some(tag) if !tag.caller_stopped_waiting() => {
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

    /// The last frame of a multi-part reply. Hands back the collected parts.
    fn resolve_multi_part(&mut self, response: RithmicResponse) -> Option<Routed<T>> {
        let Some(tag) = self.handle_map.remove(&response.request_id) else {
            self.report_unmatched_terminal(&response);
            return None;
        };

        let mut reply = self
            .response_vec_map
            .remove(&response.request_id)
            .unwrap_or_default();
        reply.push(response);

        Some(Routed::Reply(tag, Ok(reply)))
    }

    /// Handle the answer to a `RequestResumeBars`. A refusal fails the replay,
    /// so partial data is never returned as complete.
    fn handle_resume_ack(&mut self, ack: RithmicResponse) {
        let Some(replay) = self.resumes.remove(&ack.request_id) else {
            return;
        };

        let Some(error) = ack.error else {
            info!(
                "request_id {}: the venue acknowledged the resume of request_id {}",
                ack.request_id, replay
            );
            return;
        };

        if let Some(parts) = self.refuse_replay(replay.clone(), error.clone()) {
            warn!(
                "request_id {}: the venue refused to resume request_id {} ({}); {} parts are \
                 incomplete",
                ack.request_id, replay, error, parts
            );
        }
    }

    /// Start counting frames the server may still send for `request_id`.
    fn expect_late_frames(&mut self, request_id: impl Into<String>) {
        self.late_continuations.insert(request_id.into(), 0);
    }

    /// Whether any `RequestResumeBars` is awaiting its acknowledgement.
    #[cfg(test)]
    pub(crate) fn resuming(&self) -> bool {
        !self.resumes.is_empty()
    }

    /// Whether frames that arrive for `request_id` are being counted.
    #[cfg(test)]
    pub(crate) fn expects_late_frames(&self, request_id: &str) -> bool {
        self.late_continuations.contains_key(request_id)
    }

    /// Drop a request whose caller stopped waiting, and count the rest of its
    /// reply.
    fn release_abandoned(&mut self, request_id: &str) {
        self.handle_map.remove(request_id);
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

    /// Fail every replay with [`RithmicError::ConnectionClosed`], clear
    /// internal state, and hand back the tag of every other pending request
    /// for the plant to fail the same way.
    ///
    /// The plant calls this once no reply can be trusted to arrive: on a
    /// close, an abort, a lost connection, or a timed-out write.
    pub(crate) fn drain_and_drop(&mut self) -> Vec<T> {
        self.drain_replays();

        self.response_vec_map.clear();
        self.late_continuations.clear();
        self.resumes.clear();

        self.handle_map.drain().map(|(_, tag)| tag).collect()
    }
}

#[cfg(test)]
impl RithmicRequestHandler<crate::plants::tag::Tag> {
    /// Route one response and answer its caller, as the plant does. Returns
    /// the resume the plant would send.
    pub(crate) fn route(&mut self, response: RithmicResponse) -> Option<Resume> {
        use crate::plants::tag::{Tag, answer_caller};

        match self.handle_response(response)? {
            Routed::Reply(Tag::Caller(responder), reply) => {
                answer_caller(responder, reply);

                None
            }
            Routed::Reply(tag, _) => panic!("these tests register only callers, got {tag:?}"),
            Routed::Resume(resume) => Some(resume),
        }
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

    use crate::plants::tag::{Tag, answer_caller};

    use crate::rti::{
        ResponseLogin, ResponseReferenceData, ResponseResumeBars, ResponseVolumeProfileMinuteBars,
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
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request("1".to_string(), Tag::Caller(tx));

        handler.route(make_response("1", login_message()));

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].request_id, "1");
    }

    // =========================================================================
    // Multi-part responses
    // =========================================================================

    #[test]
    fn multi_response_collects_all_parts() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request("2".to_string(), Tag::Caller(tx));

        // Two intermediate responses with has_more = true
        for _ in 0..2 {
            let mut resp = make_response("2", ref_data_message());
            resp.multi_response = true;
            resp.has_more = true;
            handler.route(resp);
        }

        // Final response with has_more = false
        let mut final_resp = make_response("2", ref_data_message());
        final_resp.multi_response = true;
        final_resp.has_more = false;
        handler.route(final_resp);

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn multi_response_single_message_no_has_more() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request("3".to_string(), Tag::Caller(tx));

        // multi_response = true but has_more = false (single-item multi-response)
        let mut resp = make_response("3", ref_data_message());
        resp.multi_response = true;
        resp.has_more = false;
        handler.route(resp);

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
    }

    // =========================================================================
    // fail_request
    // =========================================================================

    #[test]
    fn fail_request_hands_back_the_caller_with_the_error() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request("fail".to_string(), Tag::Caller(tx));

        let Some((Tag::Caller(responder), reply)) =
            handler.fail_request("fail", RithmicError::SendFailed)
        else {
            panic!("the pending caller is handed back");
        };
        answer_caller(responder, reply);

        let result = rx.try_recv().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn fail_request_returns_none_for_unknown_id() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        assert!(
            handler
                .fail_request("unknown", RithmicError::SendFailed)
                .is_none()
        );
    }

    // =========================================================================
    // drain_and_drop
    // =========================================================================

    #[test]
    fn drain_and_drop_hands_back_every_pending_request() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        handler.register_request("a".to_string(), Tag::Caller(tx1));

        handler.register_request("b".to_string(), Tag::Caller(tx2));

        for tag in handler.drain_and_drop() {
            let Tag::Caller(responder) = tag else {
                panic!("only callers were registered, got {tag:?}");
            };
            answer_caller(responder, Err(RithmicError::ConnectionClosed));
        }

        for mut rx in [rx1, rx2] {
            let err = rx.try_recv().unwrap().unwrap_err();
            assert!(matches!(err, RithmicError::ConnectionClosed));
        }
    }

    #[test]
    fn drain_and_drop_clears_partial_multi_responses() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx, _rx) = oneshot::channel();

        handler.register_request("m".to_string(), Tag::Caller(tx));

        // Accumulate a partial multi-response
        let mut resp = make_response("m", ref_data_message());
        resp.multi_response = true;
        resp.has_more = true;
        handler.route(resp);

        handler.drain_and_drop();

        assert!(handler.response_vec_map.is_empty());

        // After drain, a new request with the same ID should work cleanly.
        let (tx2, mut rx2) = oneshot::channel();

        handler.register_request("m".to_string(), Tag::Caller(tx2));

        // Probe with a terminal multi-part response: only that branch merges
        // `response_vec_map`, so only it can observe a leftover part.
        let mut probe = make_response("m", ref_data_message());
        probe.multi_response = true;
        probe.has_more = false;
        handler.route(probe);

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
        handler: &mut RithmicRequestHandler<Tag>,
        id: &str,
    ) -> oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>> {
        let (tx, rx) = oneshot::channel();

        handler.register_request(id.to_string(), Tag::Caller(tx));

        rx
    }

    #[test]
    fn a_failed_request_clears_its_partial_multi_response() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let rx = register(&mut handler, "m");

        let mut part = make_response("m", ref_data_message());
        part.multi_response = true;
        part.has_more = true;
        handler.route(part);

        handler.fail_request("m", RithmicError::ConnectionClosed);
        drop(rx);

        assert!(handler.response_vec_map.is_empty());

        // A new request reusing the id must not inherit the stale part. Only a
        // terminal multi-part response merges the map, so probe with one.
        let mut rx2 = register(&mut handler, "m");

        let mut probe = make_response("m", ref_data_message());
        probe.multi_response = true;
        probe.has_more = false;
        handler.route(probe);

        assert_eq!(rx2.try_recv().unwrap().unwrap().len(), 1);
    }

    #[test]
    fn parts_arriving_after_a_failure_do_not_re_create_the_partial_buffer() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let rx = register(&mut handler, "42");

        let mut first = make_response("42", ref_data_message());
        first.multi_response = true;
        first.has_more = true;
        handler.route(first);

        handler.fail_request("42", RithmicError::ConnectionClosed);
        drop(rx);

        // The server resumes streaming the request that was just failed.
        for _ in 0..3 {
            let mut late = make_response("42", ref_data_message());
            late.multi_response = true;
            late.has_more = true;
            handler.route(late);
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
            handler.route(terminal("42", ref_data_message()));
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
    fn a_part_whose_request_is_gone_is_counted_without_a_warning() {
        let mut handler = RithmicRequestHandler::<Tag>::new();

        let (_, logged) = log_capture::capture(|| {
            for _ in 0..3 {
                handler.route(part("gone", ref_data_message()));
            }
        });

        assert!(
            handler.response_vec_map.is_empty(),
            "the parts are not kept"
        );
        assert_eq!(logged.lines().count(), 1, "{logged}");
        assert!(logged.contains("INFO"), "{logged}");
        assert!(
            logged.contains("request_id gone: parts are arriving after the reply was resolved"),
            "{logged}"
        );
        assert_eq!(handler.late_continuations.get("gone"), Some(&3));
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
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = register(&mut handler, "208");

        // The end marker resolves the request and removes the responder.
        handler.route(terminal("208", volume_profile_message(&["0"])));
        assert_eq!(rx.try_recv().unwrap().unwrap().len(), 1);

        let (_, logged) = log_capture::capture(|| {
            for _ in 0..170 {
                handler.route(part("208", volume_profile_message(&[])));
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
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = register(&mut handler, "208");

        handler.route(terminal("208", volume_profile_message(&["0"])));
        let _ = rx.try_recv();

        for _ in 0..170 {
            handler.route(part("208", volume_profile_message(&[])));
        }

        let (_, logged) = log_capture::capture(|| {
            handler.route(terminal(
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
            handler.route(terminal(
                "208",
                volume_profile_message(&["12", "output inhibited"]),
            ));
        });

        assert!(second.contains("ERROR"), "{second}");
        assert!(!second.contains("more parts"), "{second}");
    }

    #[test]
    fn a_terminal_that_was_never_a_continuation_is_named_not_dumped() {
        let mut handler = RithmicRequestHandler::<Tag>::new();

        let (_, logged) = log_capture::capture(|| {
            handler.route(make_response("ghost", login_message()));
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
        let mut handler = RithmicRequestHandler::<Tag>::new();

        handler.route(part("208", volume_profile_message(&[])));
        assert!(handler.late_continuations.contains_key("208"));

        handler.drain_and_drop();

        assert!(handler.late_continuations.is_empty());
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    /// The caller gave up mid-reply — its own deadline elapsed and it dropped
    /// its receiver — while the venue is still streaming. The next part frees the responder and the parts held for
    /// nobody, says so once, and the rest of the reply is counted as a late
    /// continuation like any other.
    #[test]
    fn a_caller_that_stopped_waiting_mid_reply_frees_the_buffer_and_counts_the_rest_as_late() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let rx = register(&mut handler, "9");
        handler.route(part("9", volume_profile_message(&[])));
        handler.route(part("9", volume_profile_message(&[])));
        drop(rx);

        let (_, logged) = log_capture::capture(|| {
            handler.route(part("9", volume_profile_message(&[])));
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
            handler.route(terminal(
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
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let rx = register(&mut handler, "9");
        handler.route(part("9", volume_profile_message(&[])));
        handler.route(part("9", volume_profile_message(&[])));
        drop(rx);

        let (_, logged) = log_capture::capture(|| {
            handler.route(terminal("9", volume_profile_message(&["0"])));
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
    fn single_response_clears_partial_multi_responses_for_the_same_id() {
        // Mirrors a decode failure landing mid multi-part response.
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let (tx, mut rx) = oneshot::channel();

        handler.register_request("m".to_string(), Tag::Caller(tx));

        let mut partial = make_response("m", ref_data_message());
        partial.multi_response = true;
        partial.has_more = true;
        handler.route(partial);

        let mut failure = make_response("m", RithmicMessage::Unknown);
        failure.error = Some(crate::error::RithmicError::ProtocolError("bad".to_string()));
        handler.route(failure);

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1, "only the terminating frame is delivered");
        assert!(matches!(result[0].message, RithmicMessage::Unknown));

        let (tx2, mut rx2) = oneshot::channel();

        handler.register_request("m".to_string(), Tag::Caller(tx2));

        let mut terminal = make_response("m", login_message());
        terminal.multi_response = true;
        handler.route(terminal);

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
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("7");

        assert_eq!(handler.route(part("7", volume_profile_message(&[]))), None);
        assert_eq!(handler.route(part("7", volume_profile_message(&[]))), None);

        let (resume, logged) =
            log_capture::capture(|| handler.route(terminal("7", truncation_notice("0"))));
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
            handler.route(terminal(
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

        handler.route(part("7", volume_profile_message(&[])));
        handler.route(terminal("7", volume_profile_message(&["0"])));

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
        assert!(handler.replay_map.is_empty());
    }

    /// A resume key the venue repeats without intervening data is not asked
    /// for again; data for the reply re-arms it, because the venue reuses a
    /// key across cuts of the same replay.
    #[test]
    fn a_repeated_resume_key_without_new_data_is_not_asked_for_again() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("7");

        handler.route(part("7", volume_profile_message(&[])));
        assert_eq!(
            handler.route(terminal("7", truncation_notice("0"))),
            Some(Resume {
                request_id: "7".to_string(),
                key: "0".to_string(),
            })
        );

        let (resume, logged) =
            log_capture::capture(|| handler.route(terminal("7", truncation_notice("0"))));
        assert_eq!(resume, None, "a repeated key is not asked for again");
        assert!(!logged.contains("asking it to resume"), "{logged}");
        assert!(rx.try_recv().is_err(), "the caller keeps waiting");

        handler.route(marker_part("7"));
        assert_eq!(
            handler.route(terminal("7", truncation_notice("0"))),
            Some(Resume {
                request_id: "7".to_string(),
                key: "0".to_string(),
            }),
            "data re-arms the key the venue reuses"
        );

        handler.route(terminal("7", volume_profile_message(&["0"])));
        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(
            reply.len(),
            3,
            "two parts and the end marker; the notices are not data"
        );
        assert!(reply.iter().all(|frame| !frame.is_truncated()));
        assert!(handler.replay_map.is_empty());
        assert!(handler.resumes.is_empty());
    }

    /// The venue can acknowledge a resume twice: the first acknowledgement
    /// consumes the correlation, so the second finds no caller. One line at
    /// INFO, not an error for a reply nobody is missing.
    #[test]
    fn a_duplicate_resume_acknowledgement_is_counted_not_an_error() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("7");

        handler.route(part("7", volume_profile_message(&[])));
        let resume = handler
            .route(terminal("7", truncation_notice("0")))
            .expect("a pending truncated reply asks to resume");
        handler.register_resume("9".to_string(), resume.request_id);

        let (_, logged) = log_capture::capture(|| {
            handler.route(terminal(
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
            handler.route(terminal(
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

        handler.route(terminal("7", volume_profile_message(&["0"])));
        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(reply.len(), 2, "one part and the end marker");
        assert!(handler.resumes.is_empty());
    }

    /// A venue that refuses the resume ends the wait: the caller gets the
    /// refusal as an error, never the prefix as a successful complete reply.
    #[test]
    fn a_refused_resume_never_reports_a_complete_prefix() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("7");

        handler.route(part("7", volume_profile_message(&[])));
        let resume = handler
            .route(terminal("7", truncation_notice("0")))
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
        let (_, logged) = log_capture::capture(|| handler.route(refusal));

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
        assert!(handler.replay_map.is_empty());
    }

    /// A truncation notice for a caller that stopped waiting is not resumed:
    /// nobody would get the continuation. The replay is released, that is said
    /// once, and what the venue still sends for the id is counted.
    #[test]
    fn a_truncation_notice_for_a_caller_that_stopped_waiting_is_counted_not_resumed() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let rx = handler.register_test_replay("7");
        handler.mark_sent("7");

        handler.route(part("7", volume_profile_message(&[])));
        drop(rx);

        let (resume, logged) =
            log_capture::capture(|| handler.route(terminal("7", truncation_notice("0"))));
        assert_eq!(resume, None);
        assert!(
            logged.contains("request_id 7: the caller stopped waiting after 1 parts"),
            "{logged}"
        );
        assert!(handler.replay_map.is_empty());

        let (_, logged) = log_capture::capture(|| {
            for _ in 0..3 {
                handler.route(part("7", volume_profile_message(&[])));
            }
            handler.route(terminal(
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

    /// A resume refusal that arrives after its replay already ended is not a
    /// warning: nothing is missing a reply.
    #[test]
    fn a_refusal_after_the_replay_ended_is_not_a_warning() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("7");

        handler.route(part("7", volume_profile_message(&[])));
        let resume = handler
            .route(terminal("7", truncation_notice("0")))
            .expect("a pending truncated reply asks to resume");
        handler.register_resume("9".to_string(), resume.request_id);
        handler.route(terminal("7", volume_profile_message(&["0"])));
        assert_eq!(rx.try_recv().unwrap().unwrap().len(), 2);
        assert!(
            handler.resumes.is_empty(),
            "the replay's resume is forgotten"
        );

        let mut refusal = terminal(
            "9",
            RithmicMessage::ResponseResumeBars(ResponseResumeBars {
                rp_code: vec!["5".to_string(), "late".to_string()],
                ..Default::default()
            }),
        );
        refusal.error = Some(RithmicError::ProtocolError("late".to_string()));
        let (_, logged) = log_capture::capture(|| handler.route(refusal));

        assert!(!logged.contains("WARN"), "{logged}");
        assert!(!logged.contains("ERROR"), "{logged}");
    }

    /// A complete replay's end marker carries `rp_code` `["0"]` and no key:
    /// it is not a truncation and opens no continuation.
    #[test]
    fn a_complete_replay_opens_no_continuation() {
        let mut handler = RithmicRequestHandler::<Tag>::new();
        let mut rx = handler.register_test_replay("8");

        let (_, logged) = log_capture::capture(|| {
            handler.route(part("8", volume_profile_message(&[])));
            handler.route(terminal("8", volume_profile_message(&["0"])));
        });

        let reply = rx.try_recv().unwrap().unwrap();
        assert_eq!(reply.len(), 2);
        assert!(!reply[1].is_truncated());
        assert!(!logged.contains("truncated"), "{logged}");
        assert!(handler.late_continuations.is_empty());
    }
}
