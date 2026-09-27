//! What every plant does, with no I/O: events in, effects out.
//!
//! How a request travels, taking
//! [`RithmicPnlPlantHandle::get_system_info`](crate::RithmicPnlPlantHandle::get_system_info):
//!
//! 1. The handle puts the sender half of a oneshot in a command, sends the
//!    command down the plant's mpsc channel, and awaits the oneshot.
//! 2. The actor loop, [`Plant::run`](crate::plants::actor::Plant::run), reads
//!    the command and hands it to [`PlantCore::on_event`] as [`Event::Command`].
//! 3. The core builds the request, which gets a new request id, registers a
//!    [`Tag`] under that id in the request handler, and returns
//!    [`Effect::Send`]. A plant's own commands go to
//!    [`PlantKind::on_command`], which queues requests on a [`Cx`] for the
//!    core to register and send the same way.
//! 4. The actor writes the frame and reports how it went as [`Event::Sent`],
//!    [`Event::SendFailed`] or [`Event::SendTimedOut`].
//! 5. The reply arrives. The actor decodes it and hands it in as
//!    [`Event::Frame`]. The request handler matches it to its id and collects
//!    the parts of a multi-part reply until the last one.
//! 6. The handler returns the tag with the whole reply, and the core acts on
//!    it: [`Tag::Caller`] gets the reply on its oneshot, [`Tag::Login`] moves
//!    the [`Session`], and [`Tag::Kind`] goes to [`PlantKind::on_reply`].
//!
//! Updates, the frames the server sends unasked, skip the request handler and
//! go to subscribers as [`Effect::Forward`]. History replays are tracked apart
//! as a [`PendingReplay`](crate::request_handler::PendingReplay), since the
//! server can cut them short and continue them.

use std::{mem, time::Duration};
use tracing::{debug, error, info, warn};

use crate::{
    api::{receiver_api::RithmicResponse, sender_api::RithmicSenderApi},
    config::{LoginConfig, RithmicConfig},
    error::RithmicError,
    plants::{
        kind::{Cx, Outgoing, PlantCommand, PlantKind},
        session::{Session, answer_requesters},
        tag::{Tag, answer_caller},
    },
    request_handler::{RequestResult, Responder, Resume, RithmicRequestHandler, Routed},
    rti::messages::RithmicMessage,
};

/// Something a plant reacts to. The I/O loop reads these off the socket, its
/// timers and its command channel, and reports how each write went.
// A frame is much larger than the other variants, but an event is handled as
// soon as it is made and never stored, so boxing it would only add an
// allocation per frame.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Event<C> {
    /// A command from a handle.
    Command(C),
    /// A frame from the server, or one that failed to decode.
    Frame(RithmicResponse),
    /// It is time to heartbeat.
    HeartbeatDue,
    /// It is time to ping.
    PingDue,
    /// The last ping went unanswered.
    PingTimedOut,
    /// The server sent its close frame.
    CloseReceived,
    /// The connection ended without a close frame.
    StreamEnded,
    /// Request `id` reached the socket.
    Sent(String),
    /// The write of request `id` failed. The connection may still be up.
    SendFailed(String),
    /// The write of request `id` timed out, so the sink is poisoned.
    SendTimedOut(String),
    /// The connection is dead. `error` is reported to subscribers under `id`.
    ConnectionLost {
        id: &'static str,
        error: RithmicError,
    },
}

/// Something the I/O loop does for the plant, in the order the core gives.
#[derive(Debug)]
pub(crate) enum Effect {
    /// Write request `id`, and report how it went with [`Event::Sent`],
    /// [`Event::SendFailed`] or [`Event::SendTimedOut`].
    Send { id: String, frame: Vec<u8> },
    /// Write a heartbeat. Nothing waits on its reply.
    Heartbeat(Vec<u8>),
    /// Write a WebSocket ping.
    Ping,
    /// Heartbeat on this period from now on.
    SetHeartbeat(Duration),
    /// Hand an update from the server to subscribers.
    Forward(RithmicResponse),
    /// Tell subscribers what happened to the connection.
    Broadcast(RithmicResponse),
    /// Write the close frame, if the socket takes it.
    SendClose,
    /// Stop the loop.
    Stop,
}

/// What every plant does, without the I/O.
///
/// Holds the login session, the request handler and the plant's own state,
/// and turns each [`Event`] into the [`Effect`]s the I/O loop carries out. It
/// never awaits and never touches the socket or a clock. Callers waiting on a
/// oneshot are answered here, since that cannot block.
#[derive(Debug)]
pub(crate) struct PlantCore<K: PlantKind> {
    pub(crate) config: RithmicConfig,
    pub(crate) kind: K,
    pub(crate) request_handler: RithmicRequestHandler<Tag<K::Tag>>,
    pub(crate) sender_api: RithmicSenderApi,
    pub(crate) session: Session,
    effects: Vec<Effect>,
}

impl<K: PlantKind> PlantCore<K> {
    /// A plant of kind `kind`, connected and not logged in.
    pub(crate) fn new(kind: K, config: &RithmicConfig) -> Self {
        PlantCore {
            config: config.clone(),
            kind,
            request_handler: RithmicRequestHandler::new(),
            sender_api: RithmicSenderApi::new(config),
            session: Session::Connected,
            effects: Vec::new(),
        }
    }

    /// React to `event`, and return what the I/O loop must do about it.
    pub(crate) fn on_event(&mut self, event: Event<K::Command>) -> Vec<Effect> {
        // Let go of any replay whose caller stopped waiting since the last
        // event, so its late frames are counted rather than kept.
        self.request_handler.release_abandoned_replays();

        match event {
            Event::Command(command) => self.on_command(command),
            Event::Frame(response) => self.on_frame(response),
            Event::HeartbeatDue => self.heartbeat(),
            Event::PingDue => {
                if !self.close_requested() {
                    self.effects.push(Effect::Ping);
                }
            }
            Event::PingTimedOut => self.on_ping_timeout(),
            Event::CloseReceived => self.on_close_received(),
            Event::StreamEnded => self.on_stream_ended(),
            Event::Sent(id) => self.request_handler.mark_sent(&id),
            Event::SendFailed(id) => self.on_send_failed(&id),
            Event::SendTimedOut(id) => self.on_send_timed_out(&id),
            Event::ConnectionLost { id, error } => {
                self.fail_connection_and_drain(id, error);
                self.effects.push(Effect::Stop);
            }
        }

        mem::take(&mut self.effects)
    }

    /// Whether a close was requested, so only the close may still go out.
    pub(crate) fn close_requested(&self) -> bool {
        self.session.is_closing()
    }

    /// Act on a command from a handle.
    fn on_command(&mut self, command: K::Command) {
        let command = K::shared(command);

        // Drop a request queued after a close was requested; handles report
        // the dropped responder as `ConnectionClosed`. `Close` and `Abort`
        // carry none and must still run: `Close` has to send the close frame.
        if self.close_requested()
            && !matches!(command, Ok(PlantCommand::Close | PlantCommand::Abort))
        {
            debug!(
                "{}: dropping a command queued after close was requested",
                K::SOURCE
            );

            return;
        }

        match command {
            Ok(PlantCommand::Close) => self.close(),
            Ok(PlantCommand::Abort) => self.abort(),
            Ok(PlantCommand::GetSystemInfo { response_sender }) => {
                let (buf, id) = self.sender_api.request_rithmic_system_info();
                self.register_and_send(buf, id, Tag::Caller(response_sender));
            }
            Ok(PlantCommand::Login {
                config,
                response_sender,
            }) => self.login(config, response_sender),
            Ok(PlantCommand::Logout { response_sender }) => self.logout(response_sender),
            Err(command) => {
                let mut cx = Cx::new(&mut self.sender_api);
                self.kind.on_command(command, &mut cx);
                let outgoing = cx.into_outgoing();

                self.send_outgoing(outgoing);
            }
        }
    }

    /// Send what the plant queued through a [`Cx`], in order.
    fn send_outgoing(&mut self, outgoing: Vec<Outgoing<K::Tag>>) {
        for request in outgoing {
            match request {
                Outgoing::Request { buf, id, tag } => self.register_and_send(buf, id, tag),
                Outgoing::Replay { buf, id, replay } => {
                    // Nothing is sent for a replay whose caller stopped
                    // waiting while it was queued.
                    if self.request_handler.register_replay(id.clone(), replay) {
                        self.effects.push(Effect::Send { id, frame: buf });
                    }
                }
            }
        }
    }

    /// Register `tag` under `id`, then send `buf`.
    fn register_and_send(&mut self, buf: Vec<u8>, id: String, tag: Tag<K::Tag>) {
        self.request_handler.register_request(id.clone(), tag);
        self.effects.push(Effect::Send { id, frame: buf });
    }

    fn emit_connection_health_event(&mut self, request_id: &str, error: RithmicError) {
        let message = error.as_connection_message();

        self.effects.push(Effect::Broadcast(RithmicResponse {
            request_id: request_id.to_string(),
            message,
            is_update: true,
            has_more: false,
            multi_response: false,
            error: Some(error),
            source: K::SOURCE.to_string(),
        }));
    }

    fn fail_connection_and_drain(&mut self, request_id: &str, error: RithmicError) {
        self.emit_connection_health_event(request_id, error);
        self.drain_requests();
    }

    /// End the session and fail every pending request, and every login still
    /// waiting, with [`RithmicError::ConnectionClosed`].
    fn drain_requests(&mut self) {
        self.session.close(Session::Closed);
        self.fail_pending_requests();
    }

    /// Fail every pending request with [`RithmicError::ConnectionClosed`].
    fn fail_pending_requests(&mut self) {
        for tag in self.request_handler.drain_and_drop() {
            self.dispatch(tag, Err(RithmicError::ConnectionClosed));
        }
    }

    /// Act on a finished request, answered or failed, according to its tag.
    ///
    /// [`Tag::Login`] gets here only when the login request failed:
    /// [`Self::route_reply`] hands every login reply to [`Self::on_login_reply`].
    fn dispatch(&mut self, tag: Tag<K::Tag>, reply: RequestResult) {
        match tag {
            Tag::Caller(responder) => answer_caller(responder, reply),
            Tag::Login => self.login_failed(reply),
            Tag::Kind(tag) => {
                self.kind.on_reply(tag, reply);
                self.check_ready();
            }
        }
    }

    /// Fail only the request whose write failed. A transport error from the
    /// sink surfaces promptly through the reader (e.g. `ConnectionClosed`),
    /// which ends the connection and fails everything else.
    fn on_send_failed(&mut self, request_id: &str) {
        if let Some((tag, reply)) = self
            .request_handler
            .fail_request(request_id, RithmicError::SendFailed)
        {
            self.dispatch(tag, reply);
        }
    }

    /// A write that timed out may still sit in the sink, so treat it as
    /// poisoned: broadcast `ConnectionError` and fail every pending request
    /// now, since a half-open TCP connection may never show up on the reader.
    ///
    /// The session is not closed: the loop stops when the next ping fails to
    /// go out, and a closed session would skip that ping. A preparing login
    /// fails first, so its loads failing below cannot complete it.
    fn on_send_timed_out(&mut self, request_id: &str) {
        self.emit_connection_health_event(
            request_id,
            RithmicError::ConnectionFailed("WebSocket send timed out — sink poisoned".to_string()),
        );

        if matches!(self.session, Session::Preparing { .. }) {
            self.session.close(Session::Connected);
        }

        self.fail_pending_requests();
    }

    /// Heartbeat while the session is logged in, preparing or ready. Nothing
    /// goes out before the login is accepted or once a close is requested.
    fn heartbeat(&mut self) {
        if !self.session.heartbeats() {
            return;
        }

        let (buf, _id) = self.sender_api.request_heartbeat();

        self.effects.push(Effect::Heartbeat(buf));
    }

    /// Act on a frame from the server. A `ForcedLogout` ends the connection
    /// once it has reached subscribers.
    fn on_frame(&mut self, response: RithmicResponse) {
        let forced_logout = matches!(response.message, RithmicMessage::ForcedLogout(_));

        self.forward_response(response);

        if forced_logout {
            self.on_forced_logout();
        }
    }

    /// Send a response where it belongs: updates to subscribers, replies to
    /// the request they answer. A frame that failed to decode takes the same
    /// paths. Heartbeat replies never reach subscribers as they are.
    fn forward_response(&mut self, response: RithmicResponse) {
        // Only a failed heartbeat is broadcast, as `HeartbeatTimeout`. The
        // frame is still routed as a reply, though the core registers no
        // request for its own heartbeats, so nothing is waiting on it.
        if matches!(response.message, RithmicMessage::ResponseHeartbeat(_)) {
            if response.error.is_some() {
                self.effects.push(Effect::Broadcast(RithmicResponse {
                    request_id: response.request_id.clone(),
                    message: RithmicMessage::HeartbeatTimeout,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: response.error.clone(),
                    source: K::SOURCE.to_string(),
                }));
            }

            self.route_reply(response);

            return;
        }

        // An unsolicited reject echoes no request id, so nothing is waiting on it.
        if response.request_id.is_empty() && matches!(response.message, RithmicMessage::Reject(_)) {
            return;
        }

        if response.is_update {
            self.effects.push(Effect::Forward(response));
        } else {
            self.route_reply(response);
        }
    }

    /// Match a reply to its request, and dispatch it once it is complete.
    fn route_reply(&mut self, response: RithmicResponse) {
        match self.request_handler.handle_response(response) {
            Some(Routed::Reply(Tag::Login, reply)) => self.on_login_reply(reply),
            Some(Routed::Reply(tag, reply)) => self.dispatch(tag, reply),
            Some(Routed::Resume(resume)) => self.resume_truncated_replay(resume),
            None => {}
        }
    }

    /// Act on the reply to the session's login request.
    fn on_login_reply(&mut self, reply: RequestResult) {
        let accepted = match &reply {
            Ok(frames) => frames.first().filter(|frame| frame.error.is_none()),
            Err(_) => None,
        };

        match accepted.cloned() {
            Some(login) => self.login_accepted(login),
            None => self.login_failed(reply),
        }
    }

    /// Start heartbeating, on the period the server asked for when it named
    /// one, and send what the plant loads before the login is done.
    fn login_accepted(&mut self, login: RithmicResponse) {
        let (config, requesters) = match mem::replace(&mut self.session, Session::Connected) {
            Session::LoggingIn { config, requesters } => (config, requesters),
            // A close was requested while the login was on the wire.
            other => {
                self.session = other;

                return;
            }
        };

        if let RithmicMessage::ResponseLogin(resp) = &login.message {
            if let Some(hb) = resp.heartbeat_interval {
                if hb > 0.0 {
                    self.effects
                        .push(Effect::SetHeartbeat(Duration::from_secs(hb as u64)));
                }
            }
        }

        self.session = Session::Preparing {
            config,
            login,
            requesters,
        };

        let mut cx = Cx::new(&mut self.sender_api);
        self.kind.after_login(&mut cx);
        let outgoing = cx.into_outgoing();

        self.send_outgoing(outgoing);
        self.check_ready();
    }

    /// Answer every login requester with a login that did not succeed, exactly as
    /// it came back, and return to `Connected` so a later login can try again.
    fn login_failed(&mut self, reply: RequestResult) {
        match mem::replace(&mut self.session, Session::Connected) {
            Session::LoggingIn { requesters, .. } => answer_requesters(requesters, &reply),
            // A close was requested while the login was on the wire, and its
            // requesters were answered then.
            other => self.session = other,
        }
    }

    /// Finish a login once the plant has loaded what it needs.
    fn check_ready(&mut self) {
        if !self.kind.is_ready() {
            return;
        }

        match mem::replace(&mut self.session, Session::Connected) {
            Session::Preparing {
                config,
                login,
                requesters,
            } => {
                answer_requesters(requesters, &Ok(vec![login.clone()]));

                self.session = Session::Ready { config, login };
            }
            other => self.session = other,
        }
    }

    /// Continue a replay the venue truncated: send `RequestResumeBars` with
    /// the key its notice carried. The venue acknowledges on the resume's own
    /// id and streams the rest on the replay's id. The write is reported under
    /// the replay's id, so a failed write fails the replay's caller.
    fn resume_truncated_replay(&mut self, resume: Resume) {
        // Nobody would get the rest of a replay whose caller stopped waiting.
        if !self.request_handler.replay_waiting(&resume.request_id) {
            return;
        }

        let (buf, resume_id) = self.sender_api.request_resume_bars(&resume.key);
        self.request_handler
            .register_resume(resume_id, resume.request_id.clone());

        self.effects.push(Effect::Send {
            id: resume.request_id,
            frame: buf,
        });
    }

    /// End the session after a server-sent `ForcedLogout` (template 77).
    ///
    /// The frame itself has already gone to subscribers, which is what
    /// distinguishes this from an ordinary disconnect; the `ConnectionError`
    /// lifecycle event follows it, since stopping the loop means no later
    /// path emits one.
    fn on_forced_logout(&mut self) {
        error!("{}: server sent a forced logout — stopping", K::SOURCE);
        // Drain first: the loop is about to stop, so nothing else will resolve
        // these. Draining also closes the session.
        self.drain_requests();
        self.emit_connection_health_event("", RithmicError::ConnectionClosed);
        self.effects.push(Effect::Stop);
    }

    /// The server's close frame ends the connection. After a requested close
    /// it is the expected echo, so nothing is broadcast.
    fn on_close_received(&mut self) {
        if self.close_requested() {
            self.drain_requests();
        } else {
            self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
        }

        self.effects.push(Effect::Stop);
    }

    /// The peer closed the TCP connection without sending a WebSocket close
    /// frame: an unexpected drop.
    fn on_stream_ended(&mut self) {
        error!("{}: WebSocket stream closed unexpectedly (EOF)", K::SOURCE);
        self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
        self.effects.push(Effect::Stop);
    }

    /// After a requested close a ping timeout is the expected way out (the
    /// server close echo may never arrive); otherwise it is a dead connection.
    fn on_ping_timeout(&mut self) {
        if self.close_requested() {
            warn!(
                "{}: ping timed out while waiting for server close echo — terminating",
                K::SOURCE
            );
            self.drain_requests();
        } else {
            self.fail_connection_and_drain(
                "websocket_ping_timeout",
                RithmicError::HeartbeatTimeout,
            );
        }

        self.effects.push(Effect::Stop);
    }

    /// Shut the plant down at once, without a logout.
    fn abort(&mut self) {
        info!("{}: abort requested, shutting down immediately", K::SOURCE);
        self.fail_connection_and_drain("", RithmicError::ConnectionClosed);
        self.effects.push(Effect::Stop);
    }

    fn close(&mut self) {
        // Close the session and drain pending requests immediately so callers
        // are not left waiting for a server close-echo that may never arrive
        // (e.g. on network drop).
        self.drain_requests();
        self.effects.push(Effect::SendClose);
    }

    /// Log the session in with `config`, or join or answer the login it has.
    ///
    /// The first login sends the request; the session, not the request,
    /// holds its callers. A login with the session's config joins one in
    /// progress, or gets the kept reply once it is done. One with another
    /// config gets [`RithmicError::LoginConflict`]. Neither sends anything.
    fn login(&mut self, config: LoginConfig, response_sender: Responder) {
        match &mut self.session {
            Session::Connected => {}
            Session::LoggingIn {
                config: current,
                requesters,
            }
            | Session::Preparing {
                config: current,
                requesters,
                ..
            } => {
                if *current == config {
                    requesters.push(response_sender);
                } else {
                    let _ = response_sender.send(Err(RithmicError::LoginConflict));
                }

                return;
            }
            Session::Ready {
                config: current,
                login,
            } => {
                let reply = if *current == config {
                    Ok(vec![login.clone()])
                } else {
                    Err(RithmicError::LoginConflict)
                };
                let _ = response_sender.send(reply);

                return;
            }
            // `on_command` drops every login once a close is requested, so
            // this never runs. It answers anyway rather than panic the actor.
            Session::Closing | Session::Closed => {
                debug_assert!(false, "a login got past the close guard");
                let _ = response_sender.send(Err(RithmicError::ConnectionClosed));

                return;
            }
        }

        let (login_buf, id) = self.sender_api.request_login(
            &self.config.system_name,
            K::INFRA,
            &self.config.user,
            &self.config.password,
            &config,
        );

        info!("{}: sending login request {}", K::SOURCE, id);

        // Set before the send, so a failed write finds the requester to answer.
        self.session = Session::LoggingIn {
            config,
            requesters: vec![response_sender],
        };

        self.register_and_send(login_buf, id, Tag::Login);
    }

    fn logout(&mut self, response_sender: Responder) {
        // Close the session before the logout goes out: the core handles
        // commands one at a time, so anything a cloned handle queues after
        // `Logout` finds it closing and is dropped by the guard in
        // `on_command`. A login still in flight fails now.
        self.session.close(Session::Closing);

        let (logout_buf, id) = self.sender_api.request_logout();
        self.register_and_send(logout_buf, id, Tag::Caller(response_sender));
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        plants::test_support::{self, Bare, answer, frame},
        rti::{
            ForcedLogout, ResponseLogin, ResponseRithmicSystemInfo, ResponseVolumeProfileMinuteBars,
        },
    };

    type ReplyRx = oneshot::Receiver<RequestResult>;

    fn bare() -> PlantCore<Bare> {
        test_support::plant_core()
    }

    /// Queue a login with `config`, returning its reply receiver and what the
    /// core asks the I/O loop to do.
    fn login(core: &mut PlantCore<Bare>, config: LoginConfig) -> (ReplyRx, Vec<Effect>) {
        let (tx, rx) = oneshot::channel();
        let effects = core.on_event(Event::Command(PlantCommand::Login {
            config,
            response_sender: tx,
        }));

        (rx, effects)
    }

    /// A core whose login request is on the wire, with its reply receiver and
    /// the request's id.
    fn logging_in() -> (PlantCore<Bare>, ReplyRx, String) {
        let mut core = bare();
        let (rx, effects) = login(&mut core, LoginConfig::default());
        let id = sent(&effects).remove(0);

        (core, rx, id)
    }

    /// The ids of the requests `effects` puts on the wire.
    fn sent(effects: &[Effect]) -> Vec<String> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Send { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    fn heartbeats(effects: &[Effect]) -> bool {
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::Heartbeat(_)))
    }

    fn stops(effects: &[Effect]) -> bool {
        matches!(effects.last(), Some(Effect::Stop))
    }

    /// A template-11 login reply for request `id`, asking for a 30 second
    /// heartbeat.
    fn login_reply(id: &str, rp_code: &[&str]) -> RithmicResponse {
        frame(&ResponseLogin {
            template_id: 11,
            user_msg: vec![id.to_string()],
            rp_code: rp_code.iter().map(|code| code.to_string()).collect(),
            heartbeat_interval: Some(30.0),
            ..Default::default()
        })
    }

    fn other_config() -> LoginConfig {
        LoginConfig {
            os_version: Some("other".to_string()),
            ..LoginConfig::default()
        }
    }

    /// Ask for the system info, returning its reply receiver and its request id.
    fn system_info(core: &mut PlantCore<Bare>) -> (ReplyRx, String) {
        let (tx, rx) = oneshot::channel();
        let effects = core.on_event(Event::Command(PlantCommand::GetSystemInfo {
            response_sender: tx,
        }));

        (rx, sent(&effects).remove(0))
    }

    #[test]
    fn an_accepted_login_reply_heartbeats_on_the_server_period_and_answers_the_login() {
        let (mut core, mut rx, id) = logging_in();

        let effects = core.on_event(Event::Frame(login_reply(&id, &["0"])));

        assert!(matches!(core.session, Session::Ready { .. }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::SetHeartbeat(period)] if *period == Duration::from_secs(30)
        ));

        let reply = answer(&mut rx).unwrap().unwrap();
        assert!(matches!(reply[0].message, RithmicMessage::ResponseLogin(_)));
    }

    #[test]
    fn a_rejected_login_reply_leaves_the_actor_logged_out() {
        let (mut core, mut rx, id) = logging_in();

        let effects = core.on_event(Event::Frame(login_reply(&id, &["7", "bad"])));

        assert!(matches!(core.session, Session::Connected));
        assert!(effects.is_empty(), "no heartbeat period is adopted");

        let reply = answer(&mut rx).unwrap().unwrap();
        assert!(
            reply[0].error.is_some(),
            "the caller still gets the rejection"
        );
    }

    /// The caller stopped waiting before the reply arrived: the session must
    /// still be logged in, or it never heartbeats and Rithmic drops it.
    #[test]
    fn an_accepted_login_reply_nobody_waits_for_still_logs_the_actor_in() {
        let (mut core, rx, id) = logging_in();
        drop(rx);

        let effects = core.on_event(Event::Frame(login_reply(&id, &["0"])));

        assert!(matches!(core.session, Session::Ready { .. }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::SetHeartbeat(period)] if *period == Duration::from_secs(30)
        ));
        assert!(heartbeats(&core.on_event(Event::HeartbeatDue)));
    }

    /// Only the session's own login request logs it in. A login reply that
    /// matches no request is dropped like any other unmatched reply.
    #[test]
    fn a_login_reply_that_matches_no_request_is_ignored() {
        let mut core = bare();

        let effects = core.on_event(Event::Frame(login_reply("login-1", &["0"])));

        assert!(matches!(core.session, Session::Connected));
        assert!(
            effects.is_empty(),
            "a reply is not an update, and adopts no heartbeat period"
        );
    }

    /// Concurrent logins share one request, and a login after it is done gets
    /// the kept reply. Neither puts anything else on the wire.
    #[test]
    fn one_login_request_answers_every_login_with_the_same_config() {
        let (mut core, mut first, id) = logging_in();
        let (mut second, effects) = login(&mut core, LoginConfig::default());
        assert!(
            effects.is_empty(),
            "a login in progress is joined, not repeated"
        );

        let effects = core.on_event(Event::Frame(login_reply(&id, &["0"])));
        assert!(sent(&effects).is_empty());

        let (mut later, effects) = login(&mut core, LoginConfig::default());
        assert!(effects.is_empty(), "a done login is answered, not repeated");

        for rx in [&mut first, &mut second, &mut later] {
            let reply = answer(rx).unwrap().unwrap();
            assert_eq!(reply[0].request_id, id);
            assert!(reply[0].error.is_none());
        }
    }

    /// A login with another config conflicts with the session's, whether its
    /// login is still on the wire or done, and sends nothing.
    #[test]
    fn a_login_with_a_different_config_conflicts_and_sends_nothing() {
        let (mut core, mut first, id) = logging_in();

        let (mut in_progress, effects) = login(&mut core, other_config());
        assert!(effects.is_empty());
        assert_eq!(
            answer(&mut in_progress),
            Some(Err(RithmicError::LoginConflict))
        );

        core.on_event(Event::Frame(login_reply(&id, &["0"])));
        assert!(matches!(answer(&mut first), Some(Ok(_))));

        let (mut done, effects) = login(&mut core, other_config());
        assert!(effects.is_empty());
        assert_eq!(answer(&mut done), Some(Err(RithmicError::LoginConflict)));

        assert!(matches!(core.session, Session::Ready { .. }));
    }

    /// A refused login leaves the plant free to try again, with any config.
    #[test]
    fn a_login_after_a_refused_one_sends_a_new_request() {
        let (mut core, _refused, id) = logging_in();

        core.on_event(Event::Frame(login_reply(&id, &["7", "bad"])));
        let (_retry, effects) = login(&mut core, other_config());

        assert_eq!(sent(&effects).len(), 1);
        assert!(matches!(core.session, Session::LoggingIn { .. }));
    }

    /// A logout, a close, an abort, or a dropped connection fails a login
    /// still in flight at once, rather than leaving it for a reply that may
    /// never come.
    #[test]
    fn closing_the_session_fails_a_login_in_flight() {
        for close in ["logout", "close", "abort", "stream end"] {
            let (mut core, mut rx, id) = logging_in();

            let event = match close {
                "logout" => Event::Command(PlantCommand::Logout {
                    response_sender: oneshot::channel().0,
                }),
                "close" => Event::Command(PlantCommand::Close),
                "abort" => Event::Command(PlantCommand::Abort),
                _ => Event::StreamEnded,
            };
            let effects = core.on_event(event);
            assert_eq!(
                stops(&effects),
                matches!(close, "abort" | "stream end"),
                "whether {close} stops the loop"
            );

            assert_eq!(
                answer(&mut rx),
                Some(Err(RithmicError::ConnectionClosed)),
                "{close} must fail the login"
            );

            // The reply that arrives anyway must not reopen the session.
            core.on_event(Event::Frame(login_reply(&id, &["0"])));
            assert!(
                core.close_requested(),
                "{close} must keep the session closed"
            );
        }
    }

    /// Whatever happens to the session, a login is answered. Only an
    /// accepted reply answers it with an accepted login.
    #[test]
    fn a_login_is_answered_on_every_path() {
        let paths = [
            "accepted",
            "rejected",
            "write failed",
            "write timed out",
            "logout",
            "close",
            "abort",
            "close frame",
            "stream end",
            "ping timeout",
            "connection lost",
            "forced logout",
        ];

        for path in paths {
            let (mut core, mut rx, id) = logging_in();

            let event = match path {
                "accepted" => Event::Frame(login_reply(&id, &["0"])),
                "rejected" => Event::Frame(login_reply(&id, &["7", "bad"])),
                "write failed" => Event::SendFailed(id.clone()),
                "write timed out" => Event::SendTimedOut(id.clone()),
                "logout" => Event::Command(PlantCommand::Logout {
                    response_sender: oneshot::channel().0,
                }),
                "close" => Event::Command(PlantCommand::Close),
                "abort" => Event::Command(PlantCommand::Abort),
                "close frame" => Event::CloseReceived,
                "stream end" => Event::StreamEnded,
                "ping timeout" => Event::PingTimedOut,
                "connection lost" => Event::ConnectionLost {
                    id: "",
                    error: RithmicError::ConnectionClosed,
                },
                _ => Event::Frame(frame(&ForcedLogout { template_id: 77 })),
            };
            core.on_event(event);

            let reply = answer(&mut rx).unwrap_or_else(|| panic!("{path} must answer the login"));
            let accepted = matches!(&reply, Ok(frames) if frames[0].error.is_none());
            assert_eq!(accepted, path == "accepted", "{path}: {reply:?}");
        }
    }

    /// Heartbeats go out once the login is accepted and until a close is
    /// requested, and never otherwise.
    #[test]
    fn heartbeats_go_out_only_while_preparing_or_ready() {
        let Session::Ready { config, login } = test_support::logged_in_session() else {
            unreachable!("a logged-in session is ready");
        };
        let sessions = [
            (Session::Connected, false),
            (
                Session::LoggingIn {
                    config: config.clone(),
                    requesters: Vec::new(),
                },
                false,
            ),
            (
                Session::Preparing {
                    config: config.clone(),
                    login: login.clone(),
                    requesters: Vec::new(),
                },
                true,
            ),
            (Session::Ready { config, login }, true),
            (Session::Closing, false),
            (Session::Closed, false),
        ];

        for (session, expected) in sessions {
            let mut core = bare();
            let name = format!("{session:?}");
            core.session = session;

            let effects = core.on_event(Event::HeartbeatDue);

            assert_eq!(heartbeats(&effects), expected, "heartbeat sent in {name}");
            assert!(!stops(&effects));
        }
    }

    /// Regression guard for the disconnect race: the session must be closing
    /// as soon as the logout is taken, so anything a cloned handle queues
    /// after it is refused rather than reaching Rithmic after the logout.
    #[test]
    fn a_logout_closes_the_session_before_it_is_sent() {
        let mut core = bare();
        assert!(
            !core.close_requested(),
            "a fresh core has no close requested"
        );

        let effects = core.on_event(Event::Command(PlantCommand::Logout {
            response_sender: oneshot::channel().0,
        }));

        assert!(matches!(core.session, Session::Closing));
        assert_eq!(sent(&effects).len(), 1, "the logout itself goes out");
    }

    /// Once a logout is taken, only the close goes out after it. Every
    /// other command is answered `ConnectionClosed` without being sent.
    #[test]
    fn after_a_logout_only_the_close_goes_out() {
        let mut core = bare();
        core.session = test_support::logged_in_session();
        core.on_event(Event::Command(PlantCommand::Logout {
            response_sender: oneshot::channel().0,
        }));

        let (tx, mut info) = oneshot::channel();
        let effects = core.on_event(Event::Command(PlantCommand::GetSystemInfo {
            response_sender: tx,
        }));
        assert!(effects.is_empty());
        assert_eq!(answer(&mut info), Some(Err(RithmicError::ConnectionClosed)));

        let (mut relogin, effects) = login(&mut core, LoginConfig::default());
        assert!(effects.is_empty());
        assert_eq!(
            answer(&mut relogin),
            Some(Err(RithmicError::ConnectionClosed))
        );

        assert!(core.on_event(Event::HeartbeatDue).is_empty());
        assert!(core.on_event(Event::PingDue).is_empty());

        let effects = core.on_event(Event::Command(PlantCommand::Close));
        assert!(matches!(effects.as_slice(), [Effect::SendClose]));
    }

    /// A reply is answered by the event that carries it. Nothing waits
    /// for a later turn of the loop.
    #[test]
    fn a_reply_is_answered_in_the_event_it_arrives_in() {
        let mut core = bare();
        let (mut rx, id) = system_info(&mut core);
        assert_eq!(answer(&mut rx), None);

        core.on_event(Event::Frame(frame(&ResponseRithmicSystemInfo {
            template_id: 17,
            user_msg: vec![id.clone()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })));

        let reply = answer(&mut rx).unwrap().unwrap();
        assert_eq!(reply[0].request_id, id);
    }

    /// A failed write fails only the request it carried.
    #[test]
    fn a_failed_write_fails_only_its_request() {
        let mut core = bare();
        let (mut failed, id) = system_info(&mut core);
        let (mut other, _) = system_info(&mut core);

        let effects = core.on_event(Event::SendFailed(id));

        assert!(effects.is_empty());
        assert_eq!(answer(&mut failed), Some(Err(RithmicError::SendFailed)));
        assert_eq!(answer(&mut other), None);
    }

    /// The broadcast goes out whether or not anything was pending.
    #[test]
    fn a_lost_connection_broadcasts_and_fails_every_pending_request() {
        for pending in [true, false] {
            let mut core = bare();
            let mut rx = pending.then(|| system_info(&mut core).0);

            let effects = core.on_event(Event::ConnectionLost {
                id: "",
                error: RithmicError::ProtocolError("test error".to_string()),
            });

            match effects.as_slice() {
                [Effect::Broadcast(event), Effect::Stop] => {
                    assert!(matches!(event.message, RithmicMessage::ConnectionError));
                    assert!(matches!(
                        &event.error,
                        Some(RithmicError::ProtocolError(s)) if s == "test error"
                    ));
                }
                other => panic!("expected a broadcast, then stop; got {other:?}"),
            }

            if let Some(rx) = &mut rx {
                assert_eq!(answer(rx), Some(Err(RithmicError::ConnectionClosed)));
            }
        }
    }

    #[test]
    fn an_unanswered_ping_stops_and_broadcasts_heartbeat_timeout() {
        let mut core = bare();
        let (mut rx, _) = system_info(&mut core);

        let effects = core.on_event(Event::PingTimedOut);

        match effects.as_slice() {
            [Effect::Broadcast(event), Effect::Stop] => {
                assert!(matches!(event.message, RithmicMessage::HeartbeatTimeout));
                assert_eq!(event.error, Some(RithmicError::HeartbeatTimeout));
            }
            other => panic!("expected a broadcast, then stop; got {other:?}"),
        }
        assert_eq!(answer(&mut rx), Some(Err(RithmicError::ConnectionClosed)));
    }

    /// After a close, an unanswered ping is how the plant gives up on the
    /// server's close echo, so nothing is broadcast.
    #[test]
    fn an_unanswered_ping_while_closing_stops_without_a_broadcast() {
        let mut core = bare();
        let (mut rx, _) = system_info(&mut core);
        core.session = Session::Closing;

        let effects = core.on_event(Event::PingTimedOut);

        assert!(matches!(effects.as_slice(), [Effect::Stop]));
        assert_eq!(answer(&mut rx), Some(Err(RithmicError::ConnectionClosed)));
    }

    /// A replay whose caller left before its write was reported is still
    /// held, but a truncation notice for it asks the venue for nothing.
    #[test]
    fn a_truncated_replay_whose_caller_left_before_its_write_was_reported_is_not_resumed() {
        let mut core = bare();
        drop(core.request_handler.register_test_replay("vp-1"));

        let effects = core.on_event(Event::Frame(frame(&ResponseVolumeProfileMinuteBars {
            template_id: 209,
            user_msg: vec!["vp-1".to_string()],
            request_key: Some("0".to_string()),
            ..Default::default()
        })));

        assert!(sent(&effects).is_empty(), "no RequestResumeBars goes out");
        assert!(!core.request_handler.resuming());
    }
}
