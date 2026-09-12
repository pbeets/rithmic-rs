use std::{collections::HashMap, time::Duration};
use tracing::{error, info, trace};

use tokio::sync::oneshot;

use crate::{
    api::{receiver_api::RithmicResponse, rp_code::response_rp_code_info},
    error::RithmicError,
    rti::messages::RithmicMessage,
};

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

/// Matches Rithmic responses to the callers waiting on them.
///
/// A registered request is resolved by a response carrying its id, by
/// [`Self::fail_request`], or by [`Self::drain_and_drop`] on disconnect. It is
/// never failed on a clock: the caller owns its own deadline.
#[derive(Debug, Default)]
pub struct RithmicRequestHandler {
    handle_map: HashMap<String, Responder>,
    response_vec_map: HashMap<String, Vec<RithmicResponse>>,

    /// Requests the venue is still streaming after the frame that resolved
    /// them, counted rather than kept.
    ///
    /// The history plant ends an over-budget window with a dataless end
    /// marker, sends a further burst of parts for the same request id, and
    /// then — up to a minute and a half later — a final response carrying
    /// `rp_code` `["12", "output inhibited"]`. That is the venue's own
    /// protocol, so the parts are counted here and reported once, when that
    /// final response arrives, instead of one warning per frame.
    ///
    /// An id is inserted by a part that finds no responder and removed by the
    /// frame that ends the continuation, so the map holds only the ids a
    /// continuation is in progress on. It is bounded by the venue's behaviour,
    /// not by a constant, and carries no size cap: each entry is an id and a
    /// counter, a continuation the venue never terminates leaves exactly one,
    /// and [`Self::drain_and_drop`] clears the map when the connection goes.
    late_continuations: HashMap<String, u64>,
}

impl RithmicRequestHandler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a request. It waits until a response carries its id, until it
    /// is failed, or until the connection drops.
    pub fn register_request(&mut self, request: RithmicRequest) {
        self.handle_map
            .insert(request.request_id, request.responder);
    }

    /// Hand the reply to its caller. A caller that stopped waiting — the
    /// adapter's per-page deadline elapsed and dropped its receiver — has
    /// nowhere for the reply to go; that is said in one line with the frame
    /// count and the venue's code, never as a dump of every frame.
    fn send_to_responder(
        &self,
        responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
        responses: Vec<RithmicResponse>,
    ) {
        if let Err(unsent) = responder.send(Ok(responses)) {
            let frames = unsent.as_ref().map(Vec::len).unwrap_or(0);
            let last = unsent.as_ref().ok().and_then(|frames| frames.last());
            let request_id = last.map(|frame| frame.request_id.as_str()).unwrap_or("");
            let rp_code = last.and_then(RithmicResponse::rp_code);

            info!(
                "request {}: the caller stopped waiting before the reply arrived; {} frames \
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
        self.response_vec_map.remove(request_id);
        if let Some(responder) = self.handle_map.remove(request_id) {
            let _ = responder.send(Err(error));
            true
        } else {
            false
        }
    }

    pub fn handle_response(&mut self, response: RithmicResponse) {
        match response.message {
            RithmicMessage::ResponseHeartbeat(_) => {
                // Handle heartbeat response if a callback is registered
                if let Some(responder) = self.handle_map.remove(&response.request_id) {
                    self.send_to_responder(responder, vec![response]);
                }
            }
            _ => {
                if !response.multi_response {
                    // Clear any parts already accumulated under this id: a
                    // decode failure correlated by user_msg can end a
                    // multi-part response early and lands here.
                    self.response_vec_map.remove(&response.request_id);

                    if let Some(responder) = self.handle_map.remove(&response.request_id) {
                        self.send_to_responder(responder, vec![response]);
                    } else {
                        self.report_unmatched_terminal(&response);
                    }
                } else {
                    // If response has more, we store it in a vector and wait for more messages
                    if response.has_more {
                        // Accumulate only while a caller is still waiting: parts
                        // for a gone id would re-create an entry nothing removes,
                        // and parts for a caller that dropped its receiver would
                        // be held for nobody until the terminal frame.
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
                    } else if let Some(responder) = self.handle_map.remove(&response.request_id) {
                        let response_vec = match self.response_vec_map.remove(&response.request_id)
                        {
                            Some(mut vec) => {
                                vec.push(response);
                                vec
                            }
                            None => {
                                vec![response]
                            }
                        };
                        self.send_to_responder(responder, response_vec);
                    } else {
                        self.report_unmatched_terminal(&response);
                    }
                }
            }
        }
    }

    /// Release a request whose caller stopped waiting mid-reply: drop the
    /// responder and the parts held for it, and say so once. The rest of the
    /// reply is then a late continuation like any other.
    fn release_abandoned(&mut self, request_id: &str) {
        self.handle_map.remove(request_id);
        let parts = self
            .response_vec_map
            .remove(request_id)
            .map(|parts| parts.len())
            .unwrap_or(0);

        info!(
            "request {}: the caller stopped waiting after {} parts; the rest of this reply is \
             counted, not kept",
            request_id, parts
        );
    }

    /// Record a part that arrived for an id nothing is waiting on.
    ///
    /// The part itself is not kept — the caller has already been given its
    /// reply, so there is nowhere to deliver it — and it is not warned about:
    /// a venue that continues past its own end marker is doing something the
    /// protocol allows, and it does it hundreds of times per request.
    fn count_late_part(&mut self, request_id: &str) {
        let parts = self
            .late_continuations
            .entry(request_id.to_string())
            .or_default();

        *parts += 1;

        trace!(
            "request {}: part {} arrived after the reply was resolved",
            request_id, parts
        );
    }

    /// Report a terminal frame that found no caller waiting.
    ///
    /// If the id has a late continuation open, this frame ends it: it is the
    /// venue finishing what it started, so it is reported once at INFO with
    /// how many parts it sent and how it ended. Otherwise nothing explains the
    /// frame and it stays an error — one line naming the request, the message
    /// and its rp_code, never a dump of the whole response.
    fn report_unmatched_terminal(&mut self, response: &RithmicResponse) {
        match self.late_continuations.remove(&response.request_id) {
            Some(parts) => info!(
                "request {}: the venue kept streaming after nothing was waiting: {} more parts, \
                 then a final response with rp_code {:?}",
                response.request_id,
                parts,
                response.rp_code()
            ),
            None => error!(
                "request {}: no caller waiting; message {}, rp_code {:?}",
                response.request_id,
                message_name(&response.message),
                response.rp_code()
            ),
        }
    }

    /// Send [`RithmicError::ConnectionClosed`] to all pending request responders, then clear
    /// internal state.
    ///
    /// Call this during an unclean shutdown (e.g., abort) to unblock any tasks that are
    /// waiting for a response that will never arrive.
    pub fn drain_and_drop(&mut self) {
        for (_, responder) in self.handle_map.drain() {
            let _ = responder.send(Err(RithmicError::ConnectionClosed));
        }
        self.response_vec_map.clear();
        self.late_continuations.clear();
    }
}

/// Name a message for a log line, without formatting its payload.
///
/// Every message that can answer a request carries an rp_code, and
/// [`response_rp_code_info`] returns that variant's name alongside it. The
/// frames the library synthesises itself are named here. What is left are the
/// subscription updates, which are broadcast rather than routed to a
/// responder, so none of them reaches this function.
fn message_name(message: &RithmicMessage) -> &'static str {
    if let Some((name, _)) = response_rp_code_info(message) {
        return name;
    }

    match message {
        RithmicMessage::Unknown => "Unknown",
        RithmicMessage::UnknownTemplate(_) => "UnknownTemplate",
        RithmicMessage::ConnectionError => "ConnectionError",
        RithmicMessage::HeartbeatTimeout => "HeartbeatTimeout",
        _ => "<not a request reply>",
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
        ResponseHeartbeat, ResponseLogin, ResponseReferenceData, ResponseVolumeProfileMinuteBars,
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

        assert!(!logged.contains("Dropping part"), "{logged}");
        assert!(!logged.contains("no request waiting"), "{logged}");
        assert!(
            logged.is_empty(),
            "a continuation part is venue behaviour, not a warning: {logged}"
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

        assert!(!logged.contains("Dropping part"), "{logged}");
        assert!(!logged.contains("no request waiting"), "{logged}");
        assert!(
            logged.is_empty(),
            "170 parts must not put 170 lines in the log: {logged}"
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
        assert!(logged.contains("request 208"), "{logged}");
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
        assert!(logged.contains("request ghost"), "{logged}");
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

    /// The caller gave up mid-reply — the adapter's per-page deadline
    /// elapsed and dropped its receiver — while the venue is still
    /// streaming. The next part frees the responder and the parts held for
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
            logged.contains("request 9: the caller stopped waiting after 2 parts"),
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
                "request 9: the caller stopped waiting before the reply arrived; 3 frames dropped"
            ),
            "{logged}"
        );
        assert!(logged.contains("rp_code Some([\"0\"])"), "{logged}");
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
}
