use std::convert::Infallible;
use tracing::info;

use crate::{
    api::receiver_api::RithmicResponse,
    request_handler::{RequestResult, RequestTag, Responder},
};

/// What a plant keeps for each request it sends, so it knows what to do with
/// the reply. `K` is what the plant keeps for requests it sends for itself.
#[derive(Debug)]
pub(crate) enum Tag<K = Infallible> {
    /// A handle method waiting on the reply.
    Caller(Responder),
    /// The session's login request. Its callers wait in the session, not here.
    Login,
    /// A request the plant sent for itself, whose reply it acts on.
    Kind(K),
}

impl<K> RequestTag for Tag<K> {
    fn caller_stopped_waiting(&self) -> bool {
        match self {
            Tag::Caller(responder) => responder.is_closed(),
            // The plant acts on these replies whoever else stopped waiting.
            Tag::Login | Tag::Kind(_) => false,
        }
    }
}

/// Hand a reply to its caller, or log one line if it stopped waiting.
pub(crate) fn answer_caller(responder: Responder, reply: RequestResult) {
    // A failure has nothing to report once the caller is gone.
    if let Err(Ok(frames)) = responder.send(reply) {
        let last = frames.last();
        let request_id = last.map(|frame| frame.request_id.as_str()).unwrap_or("");
        let rp_code = last.and_then(RithmicResponse::rp_code).unwrap_or(&[]);

        info!(
            "request_id {}: the caller stopped waiting before the reply arrived; {} frames \
             dropped, final rp_code {:?}",
            request_id,
            frames.len(),
            rp_code
        );
    }
}
