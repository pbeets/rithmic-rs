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
    /// WebSocket send failed after the request was registered.
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
            RithmicError::SendFailed => write!(f, "WebSocket send failed"),
            RithmicError::EmptyResponse => write!(f, "empty response"),
            RithmicError::ServerError(msg) => write!(f, "server error: {msg}"),
            RithmicError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for RithmicError {}
