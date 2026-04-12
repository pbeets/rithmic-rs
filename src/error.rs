use std::fmt;

/// Typed errors returned by all plant handle methods.
///
/// ```ignore
/// match handle.subscribe("ESH6", "CME").await {
///     Ok(resp) => { /* success */ }
///     Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
///         handle.abort();
///         // reconnect — see examples/reconnect.rs
///     }
///     Err(RithmicError::InvalidArgument(msg)) => eprintln!("bad input: {msg}"),
///     Err(RithmicError::ServerError(msg)) => eprintln!("rejected: {msg}"),
///     Err(e) => eprintln!("{e}"),
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RithmicError {
    /// WebSocket connection could not be established.
    ConnectionFailed(String),
    /// The plant's WebSocket connection is gone; pending requests will never complete.
    ConnectionClosed,
    /// WebSocket send failed or timed out after the request was registered.
    ///
    /// Treat this as a connection-health failure for the current request path and
    /// trigger your reconnection logic if the request is required for continued
    /// trading. This error alone does not prove that the actor has already shut
    /// down; keep-alive failure detection can still emit a synthetic
    /// [`crate::rti::messages::RithmicMessage::HeartbeatTimeout`] or
    /// [`crate::rti::messages::RithmicMessage::ConnectionError`] update if the
    /// connection is actually dead. A successful `disconnect()` on a plant
    /// handle does not emit those synthetic health events.
    SendFailed,
    /// Server returned an empty response where at least one was expected.
    EmptyResponse,
    /// Protocol-level rejection from Rithmic (the `rp_code` text).
    ServerError(String),
    /// A caller-supplied argument is invalid (the message describes which argument
    /// and why).
    InvalidArgument(String),
}

impl fmt::Display for RithmicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RithmicError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            RithmicError::ConnectionClosed => write!(f, "connection closed"),
            RithmicError::SendFailed => write!(f, "WebSocket send failed or timed out"),
            RithmicError::EmptyResponse => write!(f, "empty response"),
            RithmicError::ServerError(msg) => write!(f, "server error: {msg}"),
            RithmicError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for RithmicError {}
