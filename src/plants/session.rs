use std::{convert::Infallible, fmt, mem};

use crate::{
    api::{receiver_api::RithmicResponse, sender_api::RithmicSenderApi},
    config::LoginConfig,
    error::RithmicError,
    request_handler::{Reply, Responder},
};

/// Where a plant's login stands. The actor owns it, so what a session does
/// never depends on whether a `login()` caller is still waiting.
#[derive(Debug)]
pub(crate) enum Session {
    /// Connected and not logged in.
    Connected,
    /// The login request is on the wire.
    LoggingIn {
        config: LoginConfig,
        waiters: Vec<Responder>,
    },
    /// Logged in. The plant is loading what it needs before a login is done.
    Preparing {
        config: LoginConfig,
        login: RithmicResponse,
        waiters: Vec<Responder>,
    },
    /// Logged in and loaded. A later login with the same config gets `login`.
    Ready {
        config: LoginConfig,
        login: RithmicResponse,
    },
    /// A logout was requested. Only the logout and the close frame go out.
    Closing,
    /// The connection is closed, or its close frame has been sent.
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
        for waiter in mem::replace(self, next).into_waiters() {
            let _ = waiter.send(Err(RithmicError::ConnectionClosed));
        }
    }

    fn into_waiters(self) -> Vec<Responder> {
        match self {
            Session::LoggingIn { waiters, .. } | Session::Preparing { waiters, .. } => waiters,
            _ => Vec::new(),
        }
    }
}

/// Answer every login waiter with the same reply.
pub(crate) fn answer_waiters(waiters: Vec<Responder>, reply: &Reply) {
    // A waiter that stopped waiting changes nothing.
    for waiter in waiters {
        let _ = waiter.send(reply.clone());
    }
}

/// What a plant does for itself around the session every plant shares: the
/// requests it sends once logged in, and what it does with their replies.
pub(crate) trait PlantKind {
    /// What the plant keeps for a request whose reply it acts on itself.
    type Tag: fmt::Debug;

    /// The requests to send once a login is accepted. The login is done once
    /// [`Self::is_ready`] holds.
    fn after_login(&mut self, _api: &mut RithmicSenderApi) -> Vec<(Vec<u8>, String, Self::Tag)> {
        Vec::new()
    }

    /// Whether everything [`Self::after_login`] sent has been answered or has
    /// failed.
    fn is_ready(&self) -> bool {
        true
    }

    /// Act on the reply to a request tagged `tag`, or on its failure.
    fn on_reply(&mut self, tag: Self::Tag, reply: Reply);
}

/// A plant with nothing of its own to load or answer.
impl PlantKind for () {
    type Tag = Infallible;

    fn on_reply(&mut self, tag: Infallible, _reply: Reply) {
        match tag {}
    }
}
