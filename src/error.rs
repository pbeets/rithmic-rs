use std::fmt;

/// Structured server-side rejection preserving both the Rithmic `rp_code`
/// numeric code and the human-readable message.
///
/// Rithmic returns request-level errors as a tuple `rp_code = [code, message]`;
/// this struct keeps both pieces accessible so callers can branch on the
/// numeric code (e.g. `"1039"` for "FCM Id field is not received") without
/// parsing the string. The raw payload is preserved on [`Self::rp_code`] so
/// consumers see exactly what the wire carried.
///
/// A populated `RithmicRequestError` is a **protocol-level** outcome — not a
/// transport/connection failure. Receiving one must NOT trigger reconnection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RithmicRequestError {
    /// Raw rp_code payload exactly as received from Rithmic.
    pub rp_code: Vec<String>,
    /// First rp_code element when present.
    pub code: Option<String>,
    /// Second rp_code element when present; otherwise empty.
    pub message: String,
}

impl fmt::Display for RithmicRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code.as_deref() {
            Some(code) if !code.is_empty() => write!(f, "[{code}] {}", self.message),
            _ => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for RithmicRequestError {}

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
///     Err(RithmicError::RequestRejected(err)) => {
///         eprintln!(
///             "rejected code={} msg={}",
///             err.code.as_deref().unwrap_or("?"),
///             err.message
///         );
///     }
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
    /// Structured protocol-level rejection preserving the Rithmic `rp_code`
    /// tuple. Not a reconnect signal — request-level only.
    RequestRejected(RithmicRequestError),
    /// Non-transport, non-rp_code response failure (e.g. decode failures or
    /// other protocol-level outcomes that don't carry `rp_code`). Not a
    /// reconnect signal.
    ProtocolError(String),
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
            RithmicError::RequestRejected(err) => write!(f, "request rejected: {err}"),
            RithmicError::ProtocolError(msg) => write!(f, "protocol error: {msg}"),
            RithmicError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for RithmicError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_error_display_formats_code_and_message() {
        let err = RithmicRequestError {
            rp_code: vec![
                "1039".to_string(),
                "FCM Id field is not received.".to_string(),
            ],
            code: Some("1039".to_string()),
            message: "FCM Id field is not received.".to_string(),
        };
        assert_eq!(err.to_string(), "[1039] FCM Id field is not received.");
    }

    #[test]
    fn request_error_display_without_code_uses_message_only() {
        let err = RithmicRequestError {
            rp_code: vec![],
            code: None,
            message: "something happened".to_string(),
        };
        assert_eq!(err.to_string(), "something happened");
    }

    #[test]
    fn request_error_equality() {
        let a = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            code: Some("3".to_string()),
            message: "bad request".to_string(),
        };
        let b = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            code: Some("3".to_string()),
            message: "bad request".to_string(),
        };
        let c = RithmicRequestError {
            rp_code: vec!["4".to_string(), "bad request".to_string()],
            code: Some("4".to_string()),
            message: "bad request".to_string(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn plant_rejection_mapping_produces_request_rejected() {
        // Plant login helpers call `response.request_error()`; for an rp_code
        // rejection the new mapping yields `RithmicError::RequestRejected`
        // carrying the full structured payload.
        let err = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            code: Some("3".to_string()),
            message: "bad request".to_string(),
        };

        let mapped = RithmicError::RequestRejected(err.clone());

        match mapped {
            RithmicError::RequestRejected(inner) => {
                assert_eq!(inner, err);
                assert_eq!(inner.code.as_deref(), Some("3"));
                assert_eq!(inner.message, "bad request");
                assert_eq!(
                    inner.rp_code,
                    vec!["3".to_string(), "bad request".to_string()]
                );
            }
            other => panic!("expected RequestRejected, got {other:?}"),
        }

        // Display for the RithmicError wrapper prefixes "request rejected: "
        // and delegates to `RithmicRequestError::Display`.
        let display = RithmicError::RequestRejected(err).to_string();
        assert_eq!(display, "request rejected: [3] bad request");
    }

    #[test]
    fn rithmic_error_request_rejected_display_delegates() {
        let err = RithmicError::RequestRejected(RithmicRequestError {
            rp_code: vec![
                "7".to_string(),
                "an error occurred while parsing data.".to_string(),
            ],
            code: Some("7".to_string()),
            message: "an error occurred while parsing data.".to_string(),
        });
        assert_eq!(
            err.to_string(),
            "request rejected: [7] an error occurred while parsing data."
        );
    }

    #[test]
    fn rithmic_error_protocol_error_display() {
        let err = RithmicError::ProtocolError("decode failed".to_string());
        assert_eq!(err.to_string(), "protocol error: decode failed");
    }
}
