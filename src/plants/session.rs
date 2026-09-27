use std::mem;

use crate::{
    api::receiver_api::RithmicResponse,
    config::LoginConfig,
    error::RithmicError,
    request_handler::{RequestResult, Responder},
};

/// Where a plant's login stands. The plant's core owns it, so what a session
/// does never depends on whether a `login()` caller is still waiting.
#[derive(Debug)]
pub(crate) enum Session {
    /// Connected and not logged in.
    Connected,
    /// The login request is on the wire.
    LoggingIn {
        config: LoginConfig,
        requesters: Vec<Responder>,
    },
    /// Logged in. The plant is loading what it needs before a login is done.
    Preparing {
        config: LoginConfig,
        login: RithmicResponse,
        requesters: Vec<Responder>,
    },
    /// Logged in and loaded. A later login with the same config gets `login`.
    Ready {
        config: LoginConfig,
        login: RithmicResponse,
    },
    /// A logout was requested. Only the logout and the close frame go out.
    Closing,
    /// A close was requested, or the connection ended.
    Closed,
}

impl Session {
    /// Whether a close was requested, so nothing but the close goes out.
    pub(crate) fn is_closing(&self) -> bool {
        matches!(self, Session::Closing | Session::Closed)
    }

    /// Whether the session is logged in, so it must heartbeat.
    pub(crate) fn heartbeats(&self) -> bool {
        matches!(self, Session::Preparing { .. } | Session::Ready { .. })
    }

    /// Move to `next`, failing every login still waiting with
    /// [`RithmicError::ConnectionClosed`].
    pub(crate) fn close(&mut self, next: Session) {
        for requester in mem::replace(self, next).into_requesters() {
            let _ = requester.send(Err(RithmicError::ConnectionClosed));
        }
    }

    fn into_requesters(self) -> Vec<Responder> {
        match self {
            Session::LoggingIn { requesters, .. } | Session::Preparing { requesters, .. } => {
                requesters
            }
            _ => Vec::new(),
        }
    }
}

/// Answer every login requester with the same reply.
pub(crate) fn answer_requesters(requesters: Vec<Responder>, reply: &RequestResult) {
    // A requester that stopped waiting changes nothing.
    for requester in requesters {
        let _ = requester.send(reply.clone());
    }
}
