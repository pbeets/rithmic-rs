use tracing::info;

use crate::{
    api::receiver_api::RithmicResponse,
    request_handler::{Reply, RequestTag, Responder},
};

/// What a plant keeps for each request it sends, so it knows what to do with
/// the reply.
#[derive(Debug)]
pub(crate) enum Tag {
    /// A handle method waiting on the reply.
    Caller(Responder),
}

impl RequestTag for Tag {
    fn abandoned(&self) -> bool {
        match self {
            Tag::Caller(responder) => responder.is_closed(),
        }
    }
}

/// Hand a reply to its caller, or log one line if it stopped waiting.
pub(crate) fn answer_caller(responder: Responder, reply: Reply) {
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
