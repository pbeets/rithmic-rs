use std::collections::HashMap;
use tokio::sync::oneshot;
use tracing::error;

use crate::{
    api::receiver_api::RithmicResponse, error::RithmicError, rti::messages::RithmicMessage,
};

#[derive(Debug)]
pub struct RithmicRequest {
    pub request_id: String,
    pub responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
}

#[derive(Debug)]
pub struct RithmicRequestHandler {
    handle_map: HashMap<String, oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>>,
    response_vec_map: HashMap<String, Vec<RithmicResponse>>,
}

impl RithmicRequestHandler {
    pub fn new() -> Self {
        Self {
            handle_map: HashMap::new(),
            response_vec_map: HashMap::new(),
        }
    }

    pub fn register_request(&mut self, request: RithmicRequest) {
        self.handle_map
            .insert(request.request_id, request.responder);
    }

    fn send_to_responder(
        &self,
        responder: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
        responses: Vec<RithmicResponse>,
        request_id: &str,
    ) {
        if let Err(e) = responder.send(Ok(responses)) {
            error!(
                "Failed to send response: receiver dropped for request_id {}: {:#?}",
                request_id, e
            );
        }
    }

    /// Remove a pending request and send an error through its oneshot channel.
    ///
    /// Returns `true` if the request was found and the error was sent.
    pub fn fail_request(&mut self, request_id: &str, error: RithmicError) -> bool {
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
                    let request_id = response.request_id.clone();
                    self.send_to_responder(responder, vec![response], &request_id);
                }
            }
            _ => {
                if !response.multi_response {
                    if let Some(responder) = self.handle_map.remove(&response.request_id) {
                        let request_id = response.request_id.clone();
                        self.send_to_responder(responder, vec![response], &request_id);
                    } else {
                        error!("No responder found for response: {:#?}", response);
                    }
                } else {
                    // If response has more, we store it in a vector and wait for more messages
                    if response.has_more {
                        self.response_vec_map
                            .entry(response.request_id.clone())
                            .or_default()
                            .push(response);
                    } else if let Some(responder) = self.handle_map.remove(&response.request_id) {
                        let request_id = response.request_id.clone();
                        let response_vec = match self.response_vec_map.remove(&request_id) {
                            Some(mut vec) => {
                                vec.push(response);
                                vec
                            }
                            None => {
                                vec![response]
                            }
                        };
                        self.send_to_responder(responder, response_vec, &request_id);
                    } else {
                        error!("No responder found for response: {:#?}", response);
                    }
                }
            }
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
    }
}

impl Default for RithmicRequestHandler {
    fn default() -> Self {
        Self::new()
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

        // After drain, a new request with the same ID should work cleanly
        let (tx2, mut rx2) = oneshot::channel();

        handler.register_request(RithmicRequest {
            request_id: "m".to_string(),
            responder: tx2,
        });

        handler.handle_response(make_response("m", login_message()));

        let result = rx2.try_recv().unwrap().unwrap();
        assert_eq!(result.len(), 1);
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn response_for_unregistered_id_does_not_panic() {
        let mut handler = RithmicRequestHandler::new();

        handler.handle_response(make_response("ghost", login_message()));
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
}
