use crate::rti::messages::RithmicMessage;

/// Classified outcome of a Rithmic `rp_code` tuple.
///
/// `rp_code` is a protocol-level response code, not a transport signal. Any
/// non-success classification here represents a request-level result and has
/// no bearing on WebSocket/connection health.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RpCodeClassification {
    /// Request succeeded (rp_code is empty or `["0"]`).
    Success,
    /// Benign empty result — currently only `["7", "no data"]` (case-insensitive).
    KnownBenignEmpty,
    /// Protocol-level rejection (rp_code reports a non-zero failure code).
    RequestRejected(crate::error::RithmicRequestError),
}

impl RpCodeClassification {
    /// Converts the classification into an optional structured error.
    /// `Success` / `KnownBenignEmpty` yield `None`; `RequestRejected` yields
    /// `Some(RithmicError::RequestRejected(..))` preserving the full rp_code
    /// payload (including the `None` message for single-element rp_codes).
    pub(crate) fn into_error(self) -> Option<crate::error::RithmicError> {
        match self {
            Self::Success | Self::KnownBenignEmpty => None,
            Self::RequestRejected(err) => Some(crate::error::RithmicError::RequestRejected(err)),
        }
    }
}

// INVARIANT: every variant in this list must have an rp_code field on its
// inner proto. If you add a Response* variant to RithmicMessage whose
// proto carries rp_code, add it here AND add a decode-time
// `classify_rp_code_error(&resp.rp_code)` call in the matching decoder arm.
macro_rules! rp_code_response_variants {
    ($macro:ident) => {
        $macro! {
            Reject,
            ResponseAcceptAgreement,
            ResponseAccountList,
            ResponseAccountRmsInfo,
            ResponseAccountRmsUpdates,
            ResponseAuxilliaryReferenceData,
            ResponseBracketOrder,
            ResponseCancelAllOrders,
            ResponseCancelOrder,
            ResponseDepthByOrderSnapshot,
            ResponseDepthByOrderUpdates,
            ResponseEasyToBorrowList,
            ResponseExitPosition,
            ResponseFrontMonthContract,
            ResponseGetInstrumentByUnderlying,
            ResponseGetInstrumentByUnderlyingKeys,
            ResponseGetVolumeAtPrice,
            ResponseGiveTickSizeTypeTable,
            ResponseHeartbeat,
            ResponseLinkOrders,
            ResponseListAcceptedAgreements,
            ResponseListExchangePermissions,
            ResponseListUnacceptedAgreements,
            ResponseLogin,
            ResponseLoginInfo,
            ResponseLogout,
            ResponseMarketDataUpdate,
            ResponseMarketDataUpdateByUnderlying,
            ResponseModifyOrder,
            ResponseModifyOrderReferenceData,
            ResponseNewOrder,
            ResponseOcoOrder,
            ResponseOrderSessionConfig,
            ResponsePnLPositionSnapshot,
            ResponsePnLPositionUpdates,
            ResponseProductCodes,
            ResponseProductRmsInfo,
            ResponseReferenceData,
            ResponseReplayExecutions,
            ResponseResumeBars,
            ResponseRithmicSystemGatewayInfo,
            ResponseRithmicSystemInfo,
            ResponseSearchSymbols,
            ResponseSetRithmicMrktDataSelfCertStatus,
            ResponseShowAgreement,
            ResponseShowBracketStops,
            ResponseShowBrackets,
            ResponseShowOrderHistory,
            ResponseShowOrderHistoryDates,
            ResponseShowOrderHistoryDetail,
            ResponseShowOrderHistorySummary,
            ResponseShowOrders,
            ResponseSubscribeForOrderUpdates,
            ResponseSubscribeToBracketUpdates,
            ResponseTickBarReplay,
            ResponseTickBarUpdate,
            ResponseTimeBarReplay,
            ResponseTimeBarUpdate,
            ResponseTradeRoutes,
            ResponseUpdateStopBracketLevel,
            ResponseUpdateTargetBracketLevel,
            ResponseVolumeProfileMinuteBars,
        }
    };
}

macro_rules! define_response_rp_code_info {
    ($($variant:ident),* $(,)?) => {
        pub(crate) fn response_rp_code_info(message: &RithmicMessage) -> Option<(&'static str, &[String])> {
            match message {
                $(RithmicMessage::$variant(resp) => {
                    Some((stringify!($variant), resp.rp_code.as_slice()))
                })*
                _ => None,
            }
        }
    };
}

rp_code_response_variants!(define_response_rp_code_info);

pub(crate) fn response_rp_code_slice(message: &RithmicMessage) -> Option<&[String]> {
    response_rp_code_info(message).map(|(_, rp_code)| rp_code)
}

// Single extension point for benign `rp_code` normalizations. Any new mapping
// MUST match exactly on both code AND message and ship with a captured-fixture
// decode test — e.g. `["7", "an error occurred while parsing data."]` shares
// code "7" but is a real error.
pub(crate) fn classify_rp_code(rp_code: &[String]) -> RpCodeClassification {
    // Per §2.1.b, `rp_code[0] == "0"` is the authoritative success signal.
    // Empty rp_code = success (defensive — multipart intermediates don't carry
    // rp_code and short-circuit earlier, but this covers any edge case).
    if rp_code.is_empty() || rp_code[0] == "0" {
        return RpCodeClassification::Success;
    }

    if let (Some(code), Some(msg)) = (rp_code.first(), rp_code.get(1)) {
        if code == "7" && msg.eq_ignore_ascii_case("no data") {
            return RpCodeClassification::KnownBenignEmpty;
        }
    }

    let code = rp_code.first().cloned();
    // `message` is strictly the second element, else `None`. Symmetric with
    // `code`. Single-element rp_codes (e.g. `["5"]`) therefore produce
    // `message = None`; consumers see no spurious empty string.
    let message = rp_code.get(1).cloned();

    RpCodeClassification::RequestRejected(crate::error::RithmicRequestError {
        rp_code: rp_code.to_vec(),
        code,
        message,
    })
}

pub(crate) fn classify_rp_code_error(rp_code: &[String]) -> Option<crate::error::RithmicError> {
    classify_rp_code(rp_code).into_error()
}

#[cfg(test)]
mod tests {
    use crate::error::{RithmicError, RithmicRequestError};

    use crate::rti::{
        Reject, ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo,
        ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder,
        ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot,
        ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition,
        ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying,
        ResponseGetInstrumentByUnderlyingKeys, ResponseGetVolumeAtPrice,
        ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders,
        ResponseListAcceptedAgreements, ResponseListExchangePermissions,
        ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout,
        ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder,
        ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder,
        ResponseOrderSessionConfig, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates,
        ResponseProductCodes, ResponseProductRmsInfo, ResponseReferenceData,
        ResponseReplayExecutions, ResponseResumeBars, ResponseRithmicSystemGatewayInfo,
        ResponseRithmicSystemInfo, ResponseSearchSymbols, ResponseSetRithmicMrktDataSelfCertStatus,
        ResponseShowAgreement, ResponseShowBracketStops, ResponseShowBrackets,
        ResponseShowOrderHistory, ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail,
        ResponseShowOrderHistorySummary, ResponseShowOrders, ResponseSubscribeForOrderUpdates,
        ResponseSubscribeToBracketUpdates, ResponseTickBarReplay, ResponseTickBarUpdate,
        ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes,
        ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel,
        ResponseVolumeProfileMinuteBars, messages::RithmicMessage,
    };

    use super::*;

    // =========================================================================
    // classify_rp_code_error() unit tests
    // =========================================================================

    #[test]
    fn classify_rp_code_error_returns_none_for_empty_rp_code() {
        assert_eq!(classify_rp_code_error(&[]), None);
    }

    #[test]
    fn classify_rp_code_error_returns_none_for_zero_rp_code() {
        assert_eq!(classify_rp_code_error(&["0".to_string()]), None);
    }

    #[test]
    fn classify_rp_code_error_returns_none_for_no_data_rp_code() {
        // rp_code = ["7", "no data"] means "successful query, zero results" across all
        // Rithmic list/replay/search responses — must not be treated as an error.
        let rp_code = vec!["7".to_string(), "no data".to_string()];

        assert_eq!(classify_rp_code_error(&rp_code), None);
    }

    #[test]
    fn classify_rp_code_error_returns_none_for_no_data_case_insensitive() {
        let rp_code = vec!["7".to_string(), "No Data".to_string()];

        assert_eq!(classify_rp_code_error(&rp_code), None);
    }

    #[test]
    fn classify_rp_code_error_returns_some_for_other_code_7_messages() {
        // code "7" with a different message is still an error
        let rp_code = vec!["7".to_string(), "permission denied".to_string()];

        assert!(classify_rp_code_error(&rp_code).is_some());
    }

    #[test]
    fn classify_rp_code_error_returns_request_rejected_for_code_7_with_error() {
        let rp_code = vec!["7".to_string(), "permission denied".to_string()];

        assert_eq!(
            classify_rp_code_error(&rp_code),
            Some(RithmicError::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("7".to_string()),
                message: Some("permission denied".to_string()),
            }))
        );
    }

    #[test]
    fn classify_rp_code_error_returns_request_rejected_for_non_zero_non_7_code() {
        let rp_code = vec!["3".to_string(), "bad request".to_string()];

        assert_eq!(
            classify_rp_code_error(&rp_code),
            Some(RithmicError::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("3".to_string()),
                message: Some("bad request".to_string()),
            }))
        );
    }

    #[test]
    fn single_element_rp_code_produces_request_rejected_with_none_message() {
        // Single-element rp_codes (e.g. `["5"]`) must produce
        // `RequestRejected` with `message: None` — NOT `Some("")`. The
        // distinction matters for Display (renders as `[5]`, not `[5] `).
        let rp_code = vec!["5".to_string()];

        match classify_rp_code_error(&rp_code) {
            Some(RithmicError::RequestRejected(err)) => {
                assert!(
                    err.message.is_none(),
                    "expected message = None, got {:?}",
                    err.message
                );
            }
            other => panic!("expected Some(RequestRejected(..)), got {other:?}"),
        }
    }

    // =========================================================================
    // classify_rp_code unit tests
    // =========================================================================

    #[test]
    fn classify_rp_code_empty_is_success() {
        assert_eq!(classify_rp_code(&[]), RpCodeClassification::Success);
    }

    #[test]
    fn classify_rp_code_zero_is_success() {
        assert_eq!(
            classify_rp_code(&["0".to_string()]),
            RpCodeClassification::Success
        );
    }

    #[test]
    fn classify_rp_code_zero_with_trailing_annotation_is_success() {
        // Per §2.1.b of the Rithmic Reference Guide, rp_code[0] == "0" is the
        // authoritative success signal. A server that annotates success with
        // a trailing message (e.g. ["0", "ok"] or ["0", ""]) must not be
        // silently reclassified as a rejection.
        assert_eq!(
            classify_rp_code(&["0".to_string(), "ok".to_string()]),
            RpCodeClassification::Success
        );
        assert_eq!(
            classify_rp_code(&["0".to_string(), String::new()]),
            RpCodeClassification::Success
        );
    }

    #[test]
    fn classify_rp_code_seven_no_data_lowercase_is_known_benign_empty() {
        assert_eq!(
            classify_rp_code(&["7".to_string(), "no data".to_string()]),
            RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_seven_no_data_mixed_case_is_known_benign_empty() {
        assert_eq!(
            classify_rp_code(&["7".to_string(), "No Data".to_string()]),
            RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_seven_no_data_upper_is_known_benign_empty() {
        assert_eq!(
            classify_rp_code(&["7".to_string(), "NO DATA".to_string()]),
            RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_seven_other_msg_is_request_rejected() {
        let rp_code = vec!["7".to_string(), "permission denied".to_string()];

        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("7".to_string()),
                message: Some("permission denied".to_string()),
            })
        );
    }

    #[test]
    fn classify_rp_code_non_zero_two_fields_is_request_rejected() {
        let rp_code = vec!["3".to_string(), "bad request".to_string()];

        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("3".to_string()),
                message: Some("bad request".to_string()),
            })
        );
    }

    #[test]
    fn classify_rp_code_single_non_zero_has_none_message() {
        // When only a single element is provided, the classifier stores it as
        // `code: Some(..)` with `message: None`. Display renders `[5]`.
        let rp_code = vec!["5".to_string()];

        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("5".to_string()),
                message: None,
            })
        );
    }

    #[test]
    fn classify_rp_code_seven_parse_error_is_request_rejected_not_benign_empty() {
        // Captured evidence: ResponseOrderSessionConfig can return
        // rp_code = ["7", "an error occurred while parsing data."]. This shares
        // the benign-empty code ("7") but is NOT a no-data marker — the
        // classifier must match exactly on message, not just code.
        let rp_code = vec![
            "7".to_string(),
            "an error occurred while parsing data.".to_string(),
        ];

        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("7".to_string()),
                message: Some("an error occurred while parsing data.".to_string()),
            })
        );
    }

    #[test]
    fn response_rp_code_info_returns_variant_name_and_payload() {
        let message = RithmicMessage::ResponseSearchSymbols(ResponseSearchSymbols {
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
            ..ResponseSearchSymbols::default()
        });

        let (template_name, rp_code) =
            response_rp_code_info(&message).expect("response should expose rp_code");

        assert_eq!(template_name, "ResponseSearchSymbols");
        assert_eq!(rp_code, &["5".to_string(), "permission denied".to_string()]);
    }

    // Symmetric with the `define_response_rp_code_info` expansion — driven off
    // the same `rp_code_response_variants!` list, so removing a variant from
    // the macro without updating this test is a compile error, and any listed
    // variant whose inner proto lacks the expected shape fails the assertion.
    macro_rules! define_rp_code_info_exhaustiveness_test {
        ($($variant:ident),* $(,)?) => {
            #[test]
            fn response_rp_code_info_covers_every_listed_variant() {
                $(
                    let msg = RithmicMessage::$variant($variant::default());
                    let (name, rp_code) = response_rp_code_info(&msg)
                        .unwrap_or_else(|| panic!(
                            "response_rp_code_info returned None for listed variant {}",
                            stringify!($variant),
                        ));
                    assert_eq!(name, stringify!($variant));
                    assert!(rp_code.is_empty(), "default rp_code should be empty");
                )*
            }
        };
    }
    rp_code_response_variants!(define_rp_code_info_exhaustiveness_test);
}
