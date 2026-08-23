use std::fmt;

/// A request the server turned down, carrying the numeric code and the
/// human-readable message separately so callers can branch on the code without
/// parsing the message text.
///
/// This is a request-level outcome, not a connection failure. Receiving one
/// does not mean the connection is unhealthy, so it is not a reason to
/// reconnect.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RithmicRequestError {
    /// The response code exactly as received, before it is split into
    /// [`Self::code`] and [`Self::message`].
    pub rp_code: Vec<String>,
    /// Numeric code, when present.
    pub code: Option<String>,
    /// Human-readable message, when present.
    ///
    /// `None` when the response carried a code with no message, or no
    /// `rp_code` at all. Symmetric with [`Self::code`].
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
/// There are three outcomes to handle, not two:
///
/// - `Ok(resp)` with `resp.error == None` — the request succeeded.
/// - `Ok(resp)` with `resp.error == Some(..)` — the request reached the server
///   and the server turned it down.
/// - `Err(..)` — the request could not be completed: an argument was invalid,
///   the connection dropped, or no response came back.
///
/// The second case is the one that catches people out: a request the server
/// turned down still returns `Ok`. Code that only checks for `Err` will treat
/// it as a success. Check [`RithmicResponse::error`] to tell the first two
/// apart.
///
/// `login` is the one call that returns it as
/// `Err(`[`RequestRejected`](Self::RequestRejected)`)` instead — both cases are
/// shown below.
///
/// For which of these arrive on the subscription channel instead, and which
/// stop a plant, see the crate-level [Error Handling](crate#error-handling)
/// section.
///
/// [`RithmicResponse::error`]: crate::api::response::RithmicResponse::error
///
/// ```ignore
/// // A `subscribe` the server turns down arrives as `Ok` with `error` set.
/// match handle.subscribe("ESH6", "CME").await {
///     Ok(resp) => match &resp.error {
///         Some(err) => eprintln!("rejected: {err}"),
///         None => { /* success */ }
///     },
///     Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
///         handle.abort();
///         // reconnect — see examples/reconnect.rs
///     }
///     Err(e) => eprintln!("{e}"),
/// }
///
/// // A `login` the server turns down arrives as `Err`.
/// if let Err(RithmicError::RequestRejected(err)) = handle.login().await {
///     eprintln!(
///         "login rejected code={} msg={}",
///         err.code.as_deref().unwrap_or("?"),
///         err.message.as_deref().unwrap_or(""),
///     );
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
    /// No longer produced. The library does not time out requests; a caller
    /// that wants a deadline wraps the call in [`tokio::time::timeout`], which
    /// reports expiry through its own `Elapsed` rather than this variant.
    /// Removed in 4.0.0.
    #[deprecated(
        since = "3.1.0",
        note = "the library no longer times out requests; wrap the call in tokio::time::timeout"
    )]
    RequestTimeout,
    /// The server turned the request down, with the code and message it gave.
    /// Request-level only — not a reason to reconnect.
    RequestRejected(RithmicRequestError),
    /// A response arrived but could not be turned into a result — a decode
    /// failure, or a failure the server reported without a code. Not a reason
    /// to reconnect.
    ///
    /// An unrecognized `template_id` does not produce this error; it arrives as
    /// [`RithmicMessage::UnknownTemplate`](crate::rti::messages::RithmicMessage::UnknownTemplate).
    ProtocolError(String),
    /// A caller-supplied argument is invalid (the message describes which argument
    /// and why).
    InvalidArgument(String),
    /// No route for the order's exchange and the order named none, so nothing was sent.
    #[non_exhaustive]
    NoTradeRoute {
        /// The exchange the order named.
        exchange: String,
        /// The exchanges that do have a route.
        cached: Vec<String>,
    },
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
    #[allow(deprecated)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RithmicError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            RithmicError::ConnectionClosed => write!(f, "connection closed"),
            RithmicError::SendFailed => write!(f, "WebSocket send failed or timed out"),
            RithmicError::EmptyResponse => write!(f, "empty response"),
            RithmicError::RequestTimeout => write!(f, "request timed out"),
            RithmicError::RequestRejected(err) => {
                let detail = err.to_string();

                if detail.is_empty() {
                    write!(f, "request rejected")
                } else {
                    write!(f, "request rejected: {detail}")
                }
            }
            RithmicError::ProtocolError(msg) => write!(f, "protocol error: {msg}"),
            RithmicError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            RithmicError::NoTradeRoute { exchange, cached } => {
                write!(
                    f,
                    "no trade route for exchange {}",
                    sanitize_for_display(exchange),
                )?;

                match cached.is_empty() {
                    true => write!(f, "; no routes cached"),
                    false => {
                        let cached: Vec<String> =
                            cached.iter().map(|key| sanitize_for_display(key)).collect();

                        write!(f, "; cached: {}", cached.join(", "))
                    }
                }
            }
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
#[allow(deprecated)]
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
    fn rithmic_error_request_rejected_display_omits_the_separator_when_empty() {
        // A Reject carrying no rp_code leaves both fields `None`, so the inner
        // error renders as an empty string.
        let err = RithmicError::RequestRejected(RithmicRequestError {
            rp_code: vec![],
            code: None,
            message: None,
        });

        assert_eq!(err.to_string(), "request rejected");
    }

    #[test]
    fn rithmic_error_protocol_error_display() {
        let err = RithmicError::ProtocolError("decode failed".to_string());

        assert_eq!(err.to_string(), "protocol error: decode failed");
    }

    #[test]
    fn request_timeout_display() {
        assert_eq!(
            RithmicError::RequestTimeout.to_string(),
            "request timed out"
        );
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
        assert!(
            !RithmicError::NoTradeRoute {
                exchange: "CBOT".into(),
                cached: vec![],
            }
            .is_connection_issue()
        );
    }

    #[test]
    fn no_trade_route_display_lists_what_is_cached() {
        let err = RithmicError::NoTradeRoute {
            exchange: "CBOT".into(),
            cached: vec!["CME".into(), "NYMEX".into()],
        };

        assert_eq!(
            err.to_string(),
            "no trade route for exchange CBOT; cached: CME, NYMEX"
        );

        let err = RithmicError::NoTradeRoute {
            exchange: "CBOT".into(),
            cached: vec![],
        };

        assert_eq!(
            err.to_string(),
            "no trade route for exchange CBOT; no routes cached"
        );
    }

    #[test]
    fn no_trade_route_display_sanitizes_control_chars() {
        // The exchange and the cached names both come off the wire, so both go
        // through the sanitizer.
        let err = RithmicError::NoTradeRoute {
            exchange: "CB\rOT".into(),
            cached: vec!["C\x1b[31mME".into()],
        };

        assert_eq!(
            err.to_string(),
            "no trade route for exchange CBOT; cached: C[31mME"
        );
    }

    #[test]
    fn request_timeout_is_not_a_connection_issue() {
        // Otherwise callers that reconnect on `is_connection_issue()` would tear
        // down a live session, and its subscriptions, over one lost request.
        assert!(!RithmicError::RequestTimeout.is_connection_issue());
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
