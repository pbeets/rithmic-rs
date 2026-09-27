use std::fmt;

use crate::{
    api::sender_api::RithmicSenderApi,
    config::LoginConfig,
    error::RithmicError,
    plants::tag::Tag,
    request_handler::{PendingReplay, RequestResult, Responder},
    rti::request_login::SysInfraType,
};

/// The commands every plant takes, and handles the same way.
#[derive(Debug)]
pub(crate) enum PlantCommand {
    /// Fail pending requests and send the close frame. The loop keeps running
    /// until the connection ends, usually on the server's close echo.
    Close,
    /// Fail pending requests and stop the loop at once, sending nothing.
    Abort,
    GetSystemInfo {
        response_sender: Responder,
    },
    Login {
        config: LoginConfig,
        response_sender: Responder,
    },
    Logout {
        response_sender: Responder,
    },
}

/// What makes one plant differ from another. The core every plant runs on,
/// [`PlantCore`](crate::plants::core::PlantCore), owns the session, heartbeats
/// and the close guard, and hands the rest to its kind.
pub(crate) trait PlantKind {
    /// The commands the plant's handles send it.
    type Command;

    /// What the plant keeps for a request whose reply it acts on itself.
    type Tag: fmt::Debug;

    /// The plant's name in logs and on every response it emits.
    const SOURCE: &'static str;

    /// The plant a login asks for.
    const INFRA: SysInfraType;

    /// Take out a command every plant shares. Any other comes back.
    fn shared(command: Self::Command) -> Result<PlantCommand, Self::Command>;

    /// Queue what to load once a login is accepted. The login is done once
    /// [`Self::is_ready`] holds.
    fn after_login(&mut self, _cx: &mut Cx<'_, Self::Tag>) {}

    /// Whether everything [`Self::after_login`] sent has been answered or has
    /// failed.
    fn is_ready(&self) -> bool {
        true
    }

    /// Act on a command of the plant's own. Only runs while no close is
    /// requested.
    fn on_command(&mut self, command: Self::Command, cx: &mut Cx<'_, Self::Tag>);

    /// Act on the reply to a request tagged `tag`, or on its failure.
    fn on_reply(&mut self, tag: Self::Tag, reply: RequestResult);
}

/// A request a plant queued through [`Cx`], for the core to send.
pub(crate) enum Outgoing<T> {
    /// Register `tag` under `id`, then send `buf`.
    Request {
        buf: Vec<u8>,
        id: String,
        tag: Tag<T>,
    },
    /// Register the replay under `id`, then send `buf` unless its caller
    /// stopped waiting.
    Replay {
        buf: Vec<u8>,
        id: String,
        replay: PendingReplay,
    },
}

/// What a plant's own code may touch: it builds requests through the sender
/// API and queues them. The core registers them and has them sent, in order,
/// once the plant returns. The core only builds one while no close is
/// requested: its command guard drops every command after a close.
pub(crate) struct Cx<'a, T> {
    api: &'a mut RithmicSenderApi,
    outgoing: Vec<Outgoing<T>>,
}

impl<'a, T> Cx<'a, T> {
    /// A context that queues requests built through `api`.
    pub(crate) fn new(api: &'a mut RithmicSenderApi) -> Self {
        Cx {
            api,
            outgoing: Vec::new(),
        }
    }

    /// Send the request `build` makes, and hand its reply to the plant with
    /// `tag`.
    pub(crate) fn send(
        &mut self,
        build: impl FnOnce(&mut RithmicSenderApi) -> (Vec<u8>, String),
        tag: T,
    ) {
        self.queue(build, Tag::Kind(tag));
    }

    /// Send the request `build` makes, and hand its reply to `responder`.
    pub(crate) fn send_for(
        &mut self,
        build: impl FnOnce(&mut RithmicSenderApi) -> (Vec<u8>, String),
        responder: Responder,
    ) {
        self.queue(build, Tag::Caller(responder));
    }

    /// Like [`Self::send_for`], for a request that can fail to build. The
    /// error goes to `responder` and nothing is sent.
    pub(crate) fn try_send_for(
        &mut self,
        build: impl FnOnce(&mut RithmicSenderApi) -> Result<(Vec<u8>, String), RithmicError>,
        responder: Responder,
    ) {
        match build(self.api) {
            Ok((buf, id)) => self.outgoing.push(Outgoing::Request {
                buf,
                id,
                tag: Tag::Caller(responder),
            }),
            Err(err) => {
                let _ = responder.send(Err(err));
            }
        }
    }

    /// Send the history replay `build` makes, collecting its reply in `replay`.
    pub(crate) fn send_replay(
        &mut self,
        build: impl FnOnce(&mut RithmicSenderApi) -> (Vec<u8>, String),
        replay: PendingReplay,
    ) {
        let (buf, id) = build(self.api);

        self.outgoing.push(Outgoing::Replay { buf, id, replay });
    }

    /// The requests queued so far, in order.
    pub(crate) fn into_outgoing(self) -> Vec<Outgoing<T>> {
        self.outgoing
    }

    /// Build the request and queue it under `tag`.
    fn queue(
        &mut self,
        build: impl FnOnce(&mut RithmicSenderApi) -> (Vec<u8>, String),
        tag: Tag<T>,
    ) {
        let (buf, id) = build(self.api);

        self.outgoing.push(Outgoing::Request { buf, id, tag });
    }
}
