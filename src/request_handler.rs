use std::{collections::HashMap, time::Duration};
use tracing::{error, warn};

use tokio::{
    sync::oneshot,
    time::{Instant, sleep_until},
};

use crate::{
    api::receiver_api::RithmicResponse, error::RithmicError, rti::messages::RithmicMessage,
};

/// Default time a request may go without progress before it is failed with
/// [`RithmicError::RequestTimeout`].
///
/// Set [`RithmicConfigBuilder::request_timeout`](crate::RithmicConfigBuilder::request_timeout)
/// to change it for a connection.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Flag a response as having no caller waiting on it any more. Only `orphaned`
/// changes: `has_more` and `multi_response` are left alone so a consumer can
/// still see the reply was truncated.
fn mark_orphaned(mut response: RithmicResponse) -> RithmicResponse {
    response.orphaned = true;
    response
}

#[derive(Debug)]
pub struct RithmicRequest {
    pub request_id: String,
    pub responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
}

/// A registered request awaiting its response, and the instant it times out.
#[derive(Debug)]
struct PendingRequest {
    responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    timeout_at: Instant,
}

/// Matches Rithmic responses to the callers waiting on them.
///
/// Each registered request times out, so a response that never arrives does
/// not park its caller forever. See [`Self::fail_timed_out`].
#[derive(Debug)]
pub struct RithmicRequestHandler {
    handle_map: HashMap<String, PendingRequest>,
    response_vec_map: HashMap<String, Vec<RithmicResponse>>,
    request_timeout: Duration,
}

impl RithmicRequestHandler {
    /// Create a handler that fails requests after `request_timeout` of silence.
    ///
    /// A zero `request_timeout` selects [`DEFAULT_REQUEST_TIMEOUT`], so a
    /// zeroed-out config cannot fail every request on arrival.
    pub fn new(request_timeout: Duration) -> Self {
        let request_timeout = if request_timeout.is_zero() {
            DEFAULT_REQUEST_TIMEOUT
        } else {
            request_timeout
        };

        Self {
            handle_map: HashMap::new(),
            response_vec_map: HashMap::new(),
            request_timeout,
        }
    }

    /// Register a request; it times out `request_timeout` from now.
    pub fn register_request(&mut self, request: RithmicRequest) {
        self.handle_map.insert(
            request.request_id,
            PendingRequest {
                responder: request.responder,
                timeout_at: Instant::now() + self.request_timeout,
            },
        );
    }

    /// Fail requests as they time out, returning the parts they had accumulated,
    /// flagged [`RithmicResponse::orphaned`], for the caller to broadcast. The
    /// responder still gets an error; a timed-out request stays failed.
    ///
    /// Cancel-safe: the timeouts live in `self`, so dropping this loses nothing.
    pub async fn fail_timed_out(&mut self) -> Vec<RithmicResponse> {
        loop {
            let Some(soonest) = self.handle_map.values().map(|p| p.timeout_at).min() else {
                std::future::pending::<()>().await;
                continue;
            };

            sleep_until(soonest).await;

            let timed_out = self
                .handle_map
                .iter()
                .filter(|(_, pending)| pending.timeout_at <= soonest)
                .map(|(request_id, _)| request_id.clone())
                .collect::<Vec<_>>();

            let mut orphans = Vec::new();

            for request_id in &timed_out {
                warn!(
                    "No response for request_id {} within {:?}: failing it as timed out",
                    request_id, self.request_timeout
                );

                orphans.extend(self.fail_request(request_id, RithmicError::RequestTimeout));
            }

            return orphans;
        }
    }

    fn send_to_responder(
        &self,
        responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
        responses: Vec<RithmicResponse>,
    ) {
        if let Err(e) = responder.send(Ok(responses)) {
            let request_id = e
                .as_ref()
                .ok()
                .and_then(|r| r.first())
                .map(|r| r.request_id.as_str())
                .unwrap_or("");
            error!(
                "Failed to send response: receiver dropped for request_id {}: {:#?}",
                request_id, e
            );
        }
    }

    /// Remove a pending request and send an error through its oneshot channel.
    ///
    /// Returns the parts already accumulated under that id, flagged
    /// [`RithmicResponse::orphaned`], for the caller to broadcast; dropping them
    /// would lose the part of a reply that arrived inside the deadline.
    pub fn fail_request(&mut self, request_id: &str, error: RithmicError) -> Vec<RithmicResponse> {
        let orphans = self.take_orphaned_parts(request_id);

        if let Some(pending) = self.handle_map.remove(request_id) {
            let _ = pending.responder.send(Err(error));
        }

        orphans
    }

    /// Take the parts accumulated under `request_id`, flagged orphaned.
    fn take_orphaned_parts(&mut self, request_id: &str) -> Vec<RithmicResponse> {
        let Some(parts) = self.response_vec_map.remove(request_id) else {
            return Vec::new();
        };

        warn!(
            "Flushing {} buffered part(s) for failed request_id {} to the orphan broadcast",
            parts.len(),
            request_id
        );

        parts.into_iter().map(mark_orphaned).collect()
    }

    /// Deliver a response to the caller waiting on its request id. Returns what
    /// no longer has a caller — a late reply, or parts a single frame ended
    /// early — flagged [`RithmicResponse::orphaned`] for the caller to broadcast.
    pub fn handle_response(&mut self, response: RithmicResponse) -> Vec<RithmicResponse> {
        match response.message {
            RithmicMessage::ResponseHeartbeat(_) => {
                // Handle heartbeat response if a callback is registered
                if let Some(pending) = self.handle_map.remove(&response.request_id) {
                    self.send_to_responder(pending.responder, vec![response]);
                }
            }
            _ => {
                if !response.multi_response {
                    let mut orphans = Vec::new();

                    // A single frame can end a multi-part reply early — a decode
                    // failure correlated by user_msg, say. The responder below
                    // gets that frame only, so the parts already in would be lost.
                    if let Some(discarded) = self.response_vec_map.remove(&response.request_id) {
                        warn!(
                            "Routing {} buffered part(s) for request_id {} to the subscription \
                             broadcast as orphans: a single-frame response (template {:?}) ended \
                             the multi-part reply early",
                            discarded.len(),
                            response.request_id,
                            response.message.template_id()
                        );

                        orphans.extend(discarded.into_iter().map(mark_orphaned));
                    }

                    if let Some(pending) = self.handle_map.remove(&response.request_id) {
                        self.send_to_responder(pending.responder, vec![response]);
                    } else {
                        error!("No responder found for response: {:#?}", response);
                        orphans.push(mark_orphaned(response));
                    }

                    return orphans;
                } else {
                    // If response has more, we store it in a vector and wait for more messages
                    if response.has_more {
                        // Accumulate only while a responder is waiting: parts for
                        // a gone id would re-create an entry nothing removes.
                        if let Some(pending) = self.handle_map.get_mut(&response.request_id) {
                            // The part is progress, so the timeout restarts.
                            pending.timeout_at = Instant::now() + self.request_timeout;

                            self.response_vec_map
                                .entry(response.request_id.clone())
                                .or_default()
                                .push(response);
                        } else {
                            warn!(
                                "No responder found for part of request_id {}: routing it as orphaned",
                                response.request_id
                            );

                            return vec![mark_orphaned(response)];
                        }
                    } else if let Some(pending) = self.handle_map.remove(&response.request_id) {
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
                        self.send_to_responder(pending.responder, response_vec);
                    } else {
                        error!("No responder found for response: {:#?}", response);

                        return vec![mark_orphaned(response)];
                    }
                }
            }
        }

        Vec::new()
    }

    /// Send [`RithmicError::ConnectionClosed`] to all pending request responders, then clear
    /// internal state.
    ///
    /// Call this during an unclean shutdown (e.g., abort) to unblock any tasks that are
    /// waiting for a response that will never arrive.
    ///
    /// Returns any accumulated parts, flagged [`RithmicResponse::orphaned`], for
    /// the caller to broadcast. Parts keep their arrival order within a request;
    /// order between requests is arbitrary and means nothing.
    pub fn drain_and_drop(&mut self) -> Vec<RithmicResponse> {
        for (_, pending) in self.handle_map.drain() {
            let _ = pending.responder.send(Err(RithmicError::ConnectionClosed));
        }

        let mut orphans = Vec::new();

        for (request_id, parts) in self.response_vec_map.drain() {
            warn!(
                "Connection drained: flushing {} buffered part(s) for request_id {} to the \
                 orphan broadcast",
                parts.len(),
                request_id
            );

            orphans.extend(parts.into_iter().map(mark_orphaned));
        }

        orphans
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
    use crate::rti::{ResponseHeartbeat, ResponseLogin, ResponseReferenceData};

    fn make_response(id: &str, message: RithmicMessage) -> RithmicResponse {
        RithmicResponse {
            request_id: id.to_string(),
            message,
            is_update: false,
            has_more: false,
            multi_response: false,
            error: None,
            source: "test".to_string(),
            orphaned: false,
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

    // =========================================================================
    // Single response
    // =========================================================================

    #[test]
    fn single_response_delivered_to_responder() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        // No responder registered — should silently ignore
        handler.handle_response(make_response("hb", heartbeat_message()));
    }

    // =========================================================================
    // fail_request
    // =========================================================================

    #[test]
    fn fail_request_sends_error_to_the_responder() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let (tx, mut rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "fail".to_string(),
            responder: tx,
        });

        assert!(
            handler
                .fail_request("fail", RithmicError::SendFailed)
                .is_empty()
        );

        let result = rx.try_recv().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn fail_request_for_an_unknown_id_flushes_nothing() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);

        assert!(
            handler
                .fail_request("unknown", RithmicError::SendFailed)
                .is_empty()
        );
    }

    #[test]
    fn fail_request_flushes_accumulated_parts_as_orphans() {
        // The part that carries the payload of a multi-frame reply — a
        // bracket's basket id, say — usually arrives before the terminal one.
        // Failing the request used to destroy it.
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let _rx = register(&mut handler, "bracket");

        for _ in 0..2 {
            handler.handle_response(intermediate_part("bracket"));
        }

        let flushed = handler.fail_request("bracket", RithmicError::RequestTimeout);

        assert_eq!(flushed.len(), 2, "both parts must survive the failure");
        assert!(flushed.iter().all(|part| part.orphaned));
        assert!(
            flushed
                .iter()
                .all(|part| part.has_more && part.multi_response),
            "flags must be left alone so a consumer can see the reply was truncated"
        );
        assert!(handler.response_vec_map.is_empty());
    }

    #[test]
    fn fail_request_flushes_nothing_when_no_parts_accumulated() {
        // The common case. An empty vec means the caller broadcasts nothing,
        // rather than putting a placeholder on the wire.
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let _rx = register(&mut handler, "quiet");

        assert!(
            handler
                .fail_request("quiet", RithmicError::RequestTimeout)
                .is_empty()
        );
    }

    // =========================================================================
    // Orphaned responses
    // =========================================================================

    #[test]
    fn a_response_after_fail_request_is_returned_as_orphaned() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let rx = register(&mut handler, "late");

        handler.fail_request("late", RithmicError::RequestTimeout);
        drop(rx);

        let orphans = handler.handle_response(make_response("late", login_message()));

        assert_eq!(
            orphans.len(),
            1,
            "a reply for a failed request must be handed back, not dropped"
        );

        let orphan = &orphans[0];

        assert!(orphan.orphaned);
        assert_eq!(orphan.request_id, "late");
    }

    #[test]
    fn an_intermediate_part_after_fail_request_is_returned_as_orphaned() {
        // The part carrying the interesting payload of a multi-part reply is
        // usually not the terminal one, and this arm used to drop it silently.
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let rx = register(&mut handler, "stream");

        handler.fail_request("stream", RithmicError::RequestTimeout);
        drop(rx);

        let mut part = make_response("stream", ref_data_message());
        part.multi_response = true;
        part.has_more = true;

        let orphans = handler.handle_response(part);

        assert_eq!(
            orphans.len(),
            1,
            "an intermediate part for a failed request must be handed back"
        );

        let orphan = &orphans[0];

        assert!(orphan.orphaned);
        assert!(orphan.has_more);
        assert!(
            handler.response_vec_map.is_empty(),
            "routing the part must not re-create the partial buffer"
        );
    }

    #[test]
    fn a_terminal_part_after_fail_request_is_returned_as_orphaned() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let rx = register(&mut handler, "stream");

        handler.fail_request("stream", RithmicError::RequestTimeout);
        drop(rx);

        let mut terminal = make_response("stream", ref_data_message());
        terminal.multi_response = true;
        terminal.has_more = false;

        let orphans = handler.handle_response(terminal);

        assert_eq!(
            orphans.len(),
            1,
            "a terminal part for a failed request must be handed back"
        );
        assert!(orphans[0].orphaned);
    }

    #[test]
    fn a_delivered_response_is_not_orphaned() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let mut rx = register(&mut handler, "ok");

        assert!(
            handler
                .handle_response(make_response("ok", login_message()))
                .is_empty()
        );

        let delivered = rx.try_recv().unwrap().unwrap();
        assert!(!delivered[0].orphaned);
    }

    // =========================================================================
    // drain_and_drop
    // =========================================================================

    #[test]
    fn drain_and_drop_sends_connection_closed_to_all_pending() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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

    #[test]
    fn drain_and_drop_flushes_accumulated_parts_as_orphans() {
        // A disconnect destroys partial replies the same way a timeout does,
        // and is the more common way to hit it.
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let _rx = register(&mut handler, "bracket");

        handler.handle_response(intermediate_part_named("bracket", "first"));
        handler.handle_response(intermediate_part_named("bracket", "second"));

        let flushed = handler.drain_and_drop();

        assert_eq!(
            flushed.iter().map(symbol_of).collect::<Vec<_>>(),
            ["first", "second"],
            "arrival order within a request is preserved"
        );
        assert!(
            flushed
                .iter()
                .all(|part| part.orphaned && part.has_more && part.multi_response)
        );
        assert!(handler.response_vec_map.is_empty());
    }

    #[test]
    fn drain_and_drop_flushes_nothing_when_no_parts_accumulated() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let _rx = register(&mut handler, "quiet");

        assert!(handler.drain_and_drop().is_empty());
    }

    // =========================================================================
    // fail_timed_out
    // =========================================================================

    fn timeout_handler() -> RithmicRequestHandler {
        RithmicRequestHandler::new(Duration::from_secs(30))
    }

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

    /// A non-terminal frame of a multi-part reply.
    fn intermediate_part(id: &str) -> RithmicResponse {
        let mut part = make_response(id, ref_data_message());
        part.multi_response = true;
        part.has_more = true;
        part
    }

    /// An intermediate part tagged with `symbol`, so a test can tell frames of
    /// the same request apart by arrival order.
    fn intermediate_part_named(id: &str, symbol: &str) -> RithmicResponse {
        let mut part = intermediate_part(id);

        part.message = RithmicMessage::ResponseReferenceData(ResponseReferenceData {
            symbol: Some(symbol.to_string()),
            ..Default::default()
        });

        part
    }

    fn symbol_of(response: &RithmicResponse) -> String {
        match &response.message {
            RithmicMessage::ResponseReferenceData(data) => data.symbol.clone().unwrap_or_default(),
            other => panic!("expected reference data, got {other:?}"),
        }
    }

    /// Run the timeout loop until it fires, then read the error it sent `rx`.
    /// Time is paused, so the elapsed figure is exact. Flushed parts are
    /// discarded; tests that care about them call `fail_timed_out` directly.
    async fn time_out_until(
        handler: &mut RithmicRequestHandler,
        mut rx: oneshot::Receiver<Result<Vec<RithmicResponse>, RithmicError>>,
    ) -> (RithmicError, Duration) {
        let started = Instant::now();

        handler.fail_timed_out().await;

        let err = rx.try_recv().unwrap().unwrap_err();

        (err, Instant::now() - started)
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_timeout_falls_back_to_the_default() {
        // A config built by hand can carry Duration::ZERO; that must mean
        // "default", not "fail every request on arrival".
        let mut handler = RithmicRequestHandler::new(Duration::ZERO);
        let rx = register(&mut handler, "zero");

        let (err, elapsed) = time_out_until(&mut handler, rx).await;

        assert_eq!(err, RithmicError::RequestTimeout);
        assert_eq!(elapsed, DEFAULT_REQUEST_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_request_fails_exactly_at_its_timeout() {
        let mut handler = timeout_handler();
        let rx = register(&mut handler, "late");

        let (err, elapsed) = time_out_until(&mut handler, rx).await;

        assert_eq!(err, RithmicError::RequestTimeout);
        assert_eq!(elapsed, Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn the_request_that_times_out_soonest_fails_first() {
        let mut handler = timeout_handler();
        let soon = register(&mut handler, "soon");

        tokio::time::advance(Duration::from_secs(10)).await;

        let late = register(&mut handler, "late");

        let (_, elapsed) = time_out_until(&mut handler, soon).await;
        assert_eq!(elapsed, Duration::from_secs(20), "registered 10s earlier");

        let (_, elapsed) = time_out_until(&mut handler, late).await;
        assert_eq!(elapsed, Duration::from_secs(10), "10s behind the first");
    }

    #[tokio::test(start_paused = true)]
    async fn every_request_due_at_the_same_time_fails_together() {
        let mut handler = timeout_handler();
        let mut receivers: Vec<_> = (0..5)
            .map(|i| register(&mut handler, &format!("req-{i}")))
            .collect();

        let last = receivers.pop().unwrap();
        let (_, elapsed) = time_out_until(&mut handler, last).await;

        assert_eq!(elapsed, Duration::from_secs(30));
        assert!(
            handler.handle_map.is_empty(),
            "one pass, not one per request"
        );

        for mut rx in receivers {
            assert_eq!(
                rx.try_recv().unwrap().unwrap_err(),
                RithmicError::RequestTimeout
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_drained_request_is_not_timed_out_afterwards() {
        let mut handler = timeout_handler();
        let mut rx = register(&mut handler, "req-1");

        handler.drain_and_drop();

        assert_eq!(
            rx.try_recv().unwrap().unwrap_err(),
            RithmicError::ConnectionClosed
        );

        // Nothing left to time out, so the loop parks instead of firing.
        tokio::select! {
            _ = handler.fail_timed_out() => panic!("fired on a drained request"),
            _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_response_just_before_the_timeout_still_wins() {
        let mut handler = timeout_handler();
        let mut rx = register(&mut handler, "close-call");

        tokio::time::advance(Duration::from_secs(29)).await;
        handler.handle_response(make_response("close-call", login_message()));

        assert_eq!(rx.try_recv().unwrap().unwrap().len(), 1);

        tokio::select! {
            _ = handler.fail_timed_out() => panic!("fired on an answered request"),
            _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_request_is_removed() {
        let mut handler = timeout_handler();
        let rx = register(&mut handler, "late");

        time_out_until(&mut handler, rx).await;

        assert!(handler.handle_map.is_empty());
        assert!(
            handler
                .fail_request("late", RithmicError::SendFailed)
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_answered_in_time_is_never_failed() {
        let mut handler = timeout_handler();
        let mut rx = register(&mut handler, "ok");

        handler.handle_response(make_response("ok", login_message()));

        assert_eq!(rx.try_recv().unwrap().unwrap().len(), 1);

        // Nothing registered, so the loop parks rather than firing.
        tokio::select! {
            _ = handler.fail_timed_out() => unreachable!(),
            _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_multi_response_part_restarts_the_timeout() {
        let mut handler = timeout_handler();
        let rx = register(&mut handler, "stream");

        // A part arrives 20s in, moving the timeout to 20s + 30s.
        tokio::time::advance(Duration::from_secs(20)).await;

        let mut part = make_response("stream", ref_data_message());
        part.multi_response = true;
        part.has_more = true;
        handler.handle_response(part);

        let (err, elapsed) = time_out_until(&mut handler, rx).await;

        assert_eq!(err, RithmicError::RequestTimeout);
        assert_eq!(elapsed, Duration::from_secs(30), "measured from the part");
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_clears_partial_multi_responses() {
        let mut handler = timeout_handler();
        let rx = register(&mut handler, "m");

        let mut part = make_response("m", ref_data_message());
        part.multi_response = true;
        part.has_more = true;
        handler.handle_response(part);

        time_out_until(&mut handler, rx).await;

        assert!(handler.response_vec_map.is_empty());

        // A new request reusing the ID must not inherit the stale part. Only a
        // terminal multi-part response merges the map, so probe with one.
        let mut rx2 = register(&mut handler, "m");

        let mut probe = make_response("m", ref_data_message());
        probe.multi_response = true;
        probe.has_more = false;
        handler.handle_response(probe);

        assert_eq!(rx2.try_recv().unwrap().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_flushes_accumulated_parts_as_orphans() {
        let mut handler = timeout_handler();
        let _rx = register(&mut handler, "bracket");

        handler.handle_response(intermediate_part("bracket"));
        handler.handle_response(intermediate_part("bracket"));

        let flushed = handler.fail_timed_out().await;

        assert_eq!(flushed.len(), 2, "both parts must survive the timeout");
        assert!(
            flushed
                .iter()
                .all(|part| part.orphaned && part.has_more && part.multi_response)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_with_no_accumulated_parts_flushes_nothing() {
        // The common case: an empty vec means the caller broadcasts nothing,
        // rather than putting a placeholder on the wire.
        let mut handler = timeout_handler();
        let _rx = register(&mut handler, "quiet");

        assert!(handler.fail_timed_out().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn flushed_parts_come_before_the_late_terminal_part() {
        // The incident shape: the parts carrying the payload land inside the
        // deadline and the terminal frame lands after it. A consumer must see
        // all of them, in arrival order, flagged orphaned.
        let mut handler = timeout_handler();
        let _rx = register(&mut handler, "bracket");

        handler.handle_response(intermediate_part_named("bracket", "first"));
        handler.handle_response(intermediate_part_named("bracket", "second"));

        let mut broadcast = handler.fail_timed_out().await;

        let mut terminal = intermediate_part_named("bracket", "terminal");
        terminal.has_more = false;

        broadcast.extend(handler.handle_response(terminal));

        assert_eq!(
            broadcast.iter().map(symbol_of).collect::<Vec<_>>(),
            ["first", "second", "terminal"]
        );
        assert!(broadcast.iter().all(|part| part.orphaned));
    }

    #[tokio::test(start_paused = true)]
    async fn parts_arriving_after_a_timeout_do_not_re_create_the_partial_buffer() {
        let mut handler = timeout_handler();
        let rx = register(&mut handler, "42");

        let mut first = make_response("42", ref_data_message());
        first.multi_response = true;
        first.has_more = true;
        handler.handle_response(first);

        time_out_until(&mut handler, rx).await;

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

        // The terminal part finds no responder and must not leave one behind.
        let mut terminal = make_response("42", ref_data_message());
        terminal.multi_response = true;
        terminal.has_more = false;
        handler.handle_response(terminal);

        assert!(handler.response_vec_map.is_empty());
    }

    #[test]
    fn parts_for_a_never_registered_id_do_not_accumulate() {
        let mut handler = timeout_handler();

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

    #[tokio::test(start_paused = true)]
    async fn a_caller_that_walked_away_does_not_stall_the_loop() {
        let mut handler = timeout_handler();
        let (tx, rx) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "gone".to_string(),
            responder: tx,
        });

        // The caller gave up and dropped its receiver, so nothing observes the
        // failure. The entry must still be removed rather than retried forever.
        drop(rx);

        let live = register(&mut handler, "live");
        let (_, elapsed) = time_out_until(&mut handler, live).await;

        assert_eq!(elapsed, Duration::from_secs(30));
        assert!(handler.handle_map.is_empty());
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn response_for_unregistered_id_does_not_panic() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);

        handler.handle_response(make_response("ghost", login_message()));
    }

    #[test]
    fn dropped_receiver_does_not_panic() {
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
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
        let orphans = handler.handle_response(failure);

        assert_eq!(
            orphans.len(),
            1,
            "the buffered part must come back for the orphan broadcast, not be destroyed"
        );
        assert!(orphans[0].orphaned);

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

    #[test]
    fn a_single_frame_orphans_the_buffered_parts_and_still_answers_the_caller() {
        // The responder is owed exactly the frame that ended the reply. The
        // parts that arrived before it have nowhere else to go, so they come
        // back for the subscription broadcast instead of being destroyed.
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let mut rx = register(&mut handler, "bracket");

        handler.handle_response(intermediate_part_named("bracket", "first"));
        handler.handle_response(intermediate_part_named("bracket", "second"));

        let orphans = handler.handle_response(make_response("bracket", login_message()));

        let delivered = rx.try_recv().unwrap().unwrap();
        assert_eq!(
            delivered.len(),
            1,
            "the responder gets the single frame only"
        );
        assert!(matches!(
            delivered[0].message,
            RithmicMessage::ResponseLogin(_)
        ));
        assert!(!delivered[0].orphaned);

        assert_eq!(
            orphans.iter().map(symbol_of).collect::<Vec<_>>(),
            ["first", "second"],
            "both buffered parts come back, in arrival order"
        );
        assert!(orphans.iter().all(|part| part.orphaned));
        assert!(handler.response_vec_map.is_empty());
    }

    #[test]
    fn a_single_frame_with_no_responder_orphans_the_parts_before_the_frame() {
        // No production path leaves parts buffered with no responder today, so
        // the state is built by hand. What is pinned is the order: a consumer
        // must see the parts before the frame that ended them.
        let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
        let _rx = register(&mut handler, "bracket");

        handler.handle_response(intermediate_part_named("bracket", "first"));
        handler.handle_response(intermediate_part_named("bracket", "second"));

        handler
            .handle_map
            .remove("bracket")
            .expect("the request was registered");

        // Single-frame: the arm under test is the one that ends a multi-part
        // reply early.
        let mut frame = intermediate_part_named("bracket", "frame");
        frame.multi_response = false;
        frame.has_more = false;

        let orphans = handler.handle_response(frame);

        assert_eq!(
            orphans.iter().map(symbol_of).collect::<Vec<_>>(),
            ["first", "second", "frame"]
        );
        assert!(orphans.iter().all(|part| part.orphaned));
        assert!(handler.response_vec_map.is_empty());
    }

    // =========================================================================
    // Diagnostics
    //
    // A request that buffered parts and then failed must not read in the logs
    // like one that was never answered. Every assertion runs under an
    // INFO-capped subscriber, so a debug-only line fails the test.
    // =========================================================================

    fn part_with_template(id: &str, template_id: i32) -> RithmicResponse {
        let mut part = intermediate_part(id);

        part.message = RithmicMessage::ResponseReferenceData(ResponseReferenceData {
            template_id,
            ..Default::default()
        });

        part
    }

    #[test]
    fn flushing_at_fail_time_is_logged_with_the_request_id() {
        let (flushed, logged) = log_capture::capture(|| {
            let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
            let _rx = register(&mut handler, "1243");

            handler.handle_response(part_with_template("1243", 15));

            handler.fail_request("1243", RithmicError::RequestTimeout)
        });

        assert_eq!(flushed.len(), 1);
        assert!(
            logged.contains("Flushing 1 buffered part(s) for failed request_id 1243"),
            "{logged}"
        );
    }

    #[test]
    fn drain_and_drop_logs_the_flush_for_each_request() {
        let (flushed, logged) = log_capture::capture(|| {
            let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
            let _a = register(&mut handler, "1243");
            let _b = register(&mut handler, "1244");

            handler.handle_response(part_with_template("1243", 15));
            handler.handle_response(part_with_template("1244", 15));

            handler.drain_and_drop()
        });

        assert_eq!(flushed.len(), 2);

        for request_id in ["1243", "1244"] {
            assert!(
                logged.contains(&format!(
                    "flushing 1 buffered part(s) for request_id {request_id}"
                )),
                "every drained request must be named: {logged}"
            );
        }
    }

    #[test]
    fn routing_parts_on_a_single_frame_response_is_logged() {
        // A rejection landing mid-stream ends the reply early. The parts that
        // did arrive are routed to the orphan broadcast; left silent, that
        // reads exactly like an ack that never came.
        let (orphans, logged) = log_capture::capture(|| {
            let mut handler = RithmicRequestHandler::new(DEFAULT_REQUEST_TIMEOUT);
            let _rx = register(&mut handler, "1243");

            handler.handle_response(part_with_template("1243", 15));
            handler.handle_response(make_response("1243", login_message()))
        });

        assert_eq!(orphans.len(), 1);
        assert!(
            logged.contains(
                "Routing 1 buffered part(s) for request_id 1243 to the subscription broadcast \
                 as orphans"
            ),
            "{logged}"
        );
    }
}
