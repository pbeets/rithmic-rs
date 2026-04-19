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
    /// Second rp_code element when present.
    ///
    /// `None` when the server emitted a single-element rp_code (e.g. `["5"]`).
    /// Symmetric with [`Self::code`].
    pub message: Option<String>,
}

/// Filter ASCII/Unicode control characters from server-supplied strings before
/// they reach a log sink or terminal. Protects against log injection (newlines,
/// `\r`) and ANSI-escape attacks when the Rithmic wire payload is rendered via
/// `Display`.
fn sanitize_for_display(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).collect()
}

impl fmt::Display for RithmicRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = self.message.as_deref().map(sanitize_for_display);

        match self.code.as_deref() {
            Some(code) if !code.is_empty() => {
                let code = sanitize_for_display(code);

                match message {
                    Some(m) if !m.is_empty() => write!(f, "[{code}] {m}"),
                    _ => write!(f, "[{code}]"),
                }
            }
            _ => write!(f, "{}", message.unwrap_or_default()),
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
///             err.message.as_deref().unwrap_or(""),
///         );
///     }
///     Err(e) => eprintln!("{e}"),
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RithmicError {
    /// WebSocket connection could not be established.
    ConnectionFailed(String),
    /// The plant's WebSocket connection is gone; pending requests will never complete.
    ConnectionClosed,
    /// WebSocket send failed or timed out after the request was registered.
    ///
    /// Treat as a connection-health failure. This error alone does not prove the
    /// actor has shut down; keep-alive failure detection can still emit
    /// [`crate::rti::messages::RithmicMessage::HeartbeatTimeout`] or
    /// [`crate::rti::messages::RithmicMessage::ConnectionError`] if the
    /// connection is actually dead.
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
    /// Keep-alive detected the connection is dead.
    HeartbeatTimeout,
    /// Server terminated the session with a reason string.
    ForcedLogout(String),
}

impl RithmicError {
    /// Returns true when this error reflects a transport/connection-health failure
    /// rather than a protocol-level rejection.
    pub fn is_connection_issue(&self) -> bool {
        matches!(
            self,
            Self::ConnectionFailed(_)
                | Self::ConnectionClosed
                | Self::SendFailed
                | Self::HeartbeatTimeout
                | Self::ForcedLogout(_)
        )
    }

    /// Maps this error to the synthetic subscription [`RithmicMessage`] that a
    /// connection-health broadcast should carry. `HeartbeatTimeout` preserves
    /// the keep-alive signal; every other variant surfaces as `ConnectionError`.
    ///
    /// [`RithmicMessage`]: crate::rti::messages::RithmicMessage
    pub fn as_connection_message(&self) -> crate::rti::messages::RithmicMessage {
        match self {
            Self::HeartbeatTimeout => crate::rti::messages::RithmicMessage::HeartbeatTimeout,
            _ => crate::rti::messages::RithmicMessage::ConnectionError,
        }
    }
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
            RithmicError::HeartbeatTimeout => write!(f, "heartbeat timeout"),
            RithmicError::ForcedLogout(reason) => {
                write!(f, "forced logout: {}", sanitize_for_display(reason))
            }
        }
    }
}

impl std::error::Error for RithmicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RithmicError::RequestRejected(inner) => Some(inner),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn request_error_display_formats_code_and_message() {
        let err = RithmicRequestError {
            rp_code: vec![
                "1039".to_string(),
                "FCM Id field is not received.".to_string(),
            ],
            code: Some("1039".to_string()),
            message: Some("FCM Id field is not received.".to_string()),
        };

        assert_eq!(err.to_string(), "[1039] FCM Id field is not received.");
    }

    #[test]
    fn request_error_display_without_code_uses_message_only() {
        let err = RithmicRequestError {
            rp_code: vec![],
            code: None,
            message: Some("something happened".to_string()),
        };

        assert_eq!(err.to_string(), "something happened");
    }

    #[test]
    fn request_error_display_single_element_omits_trailing_slash() {
        // rp_code = ["5"] produces code=Some("5"), message=None.
        // Display renders "[5]" rather than "[5] ".
        let err = RithmicRequestError {
            rp_code: vec!["5".to_string()],
            code: Some("5".to_string()),
            message: None,
        };

        assert_eq!(err.to_string(), "[5]");
    }

    #[test]
    fn request_error_display_sanitizes_control_chars() {
        // A malicious or malformed server message must not leak newlines
        // (log-injection) or ANSI escapes (terminal-control) into `Display`.
        // The sanitizer strips control characters — the ESC byte of an ANSI
        // sequence is removed, which breaks the escape and prevents terminal
        // interpretation (even though the printable `[31m` text remains).
        let err = RithmicRequestError {
            rp_code: vec![
                "3\n".to_string(),
                "bad\x1b[31mredinjection\r\ndropped".to_string(),
            ],
            code: Some("3\n".to_string()),
            message: Some("bad\x1b[31mredinjection\r\ndropped".to_string()),
        };

        assert_eq!(err.to_string(), "[3] bad[31mredinjectiondropped");
    }

    #[test]
    fn request_error_equality() {
        let a = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            code: Some("3".to_string()),
            message: Some("bad request".to_string()),
        };

        let b = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            code: Some("3".to_string()),
            message: Some("bad request".to_string()),
        };

        let c = RithmicRequestError {
            rp_code: vec!["4".to_string(), "bad request".to_string()],
            code: Some("4".to_string()),
            message: Some("bad request".to_string()),
        };

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn rithmic_error_equality_for_unit_variants() {
        // `PartialEq` on `RithmicError` lets consumers write
        // `assert_eq!(result, Err(RithmicError::ConnectionClosed))` in tests.
        assert_eq!(
            RithmicError::ConnectionClosed,
            RithmicError::ConnectionClosed
        );
        assert_ne!(RithmicError::ConnectionClosed, RithmicError::SendFailed);
    }

    #[test]
    fn rithmic_error_source_chain_exposes_inner_request_error() {
        // `anyhow`/`eyre` and stdlib chain walkers rely on `source()`.

        let inner = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad".to_string()],
            code: Some("3".to_string()),
            message: Some("bad".to_string()),
        };

        let err = RithmicError::RequestRejected(inner.clone());
        let src = err
            .source()
            .expect("source should be Some for RequestRejected");

        assert_eq!(src.to_string(), inner.to_string());

        assert!(
            RithmicError::ConnectionClosed.source().is_none(),
            "unit variants should have no source"
        );
    }

    #[test]
    fn plant_rejection_mapping_produces_request_rejected() {
        // For an rp_code rejection, `response.error` is populated with
        // `RithmicError::RequestRejected` carrying the full structured payload.
        let err = RithmicRequestError {
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            code: Some("3".to_string()),
            message: Some("bad request".to_string()),
        };

        let mapped = RithmicError::RequestRejected(err.clone());

        match mapped {
            RithmicError::RequestRejected(inner) => {
                assert_eq!(inner, err);
                assert_eq!(inner.code.as_deref(), Some("3"));
                assert_eq!(inner.message.as_deref(), Some("bad request"));
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
            message: Some("an error occurred while parsing data.".to_string()),
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

    #[test]
    fn heartbeat_timeout_display() {
        assert_eq!(
            RithmicError::HeartbeatTimeout.to_string(),
            "heartbeat timeout"
        );
    }

    #[test]
    fn forced_logout_display() {
        assert_eq!(
            RithmicError::ForcedLogout("srv reason".into()).to_string(),
            "forced logout: srv reason"
        );
    }

    #[test]
    fn forced_logout_sanitizes_control_chars() {
        let err = RithmicError::ForcedLogout("bad\nreason".into());
        assert_eq!(err.to_string(), "forced logout: badreason");
    }

    #[test]
    fn is_connection_issue_true_for_transport_variants() {
        assert!(RithmicError::ConnectionFailed("x".into()).is_connection_issue());
        assert!(RithmicError::ConnectionClosed.is_connection_issue());
        assert!(RithmicError::SendFailed.is_connection_issue());
        assert!(RithmicError::HeartbeatTimeout.is_connection_issue());
        assert!(RithmicError::ForcedLogout("x".into()).is_connection_issue());
    }

    #[test]
    fn is_connection_issue_false_for_protocol_variants() {
        let req = RithmicRequestError {
            rp_code: vec!["3".into(), "x".into()],
            code: Some("3".into()),
            message: Some("x".into()),
        };
        assert!(!RithmicError::RequestRejected(req).is_connection_issue());
        assert!(!RithmicError::ProtocolError("x".into()).is_connection_issue());
        assert!(!RithmicError::InvalidArgument("x".into()).is_connection_issue());
        assert!(!RithmicError::EmptyResponse.is_connection_issue());
    }

    #[test]
    fn as_connection_message_heartbeat_timeout() {
        assert!(matches!(
            RithmicError::HeartbeatTimeout.as_connection_message(),
            crate::rti::messages::RithmicMessage::HeartbeatTimeout
        ));
    }

    #[test]
    fn as_connection_message_connection_failed() {
        assert!(matches!(
            RithmicError::ConnectionFailed("x".into()).as_connection_message(),
            crate::rti::messages::RithmicMessage::ConnectionError
        ));
    }
}
