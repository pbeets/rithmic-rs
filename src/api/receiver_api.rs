use prost::{Message, bytes::Bytes};
use tracing::error;

use crate::rti::{
    AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates,
    DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, ForcedLogout,
    FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode,
    MessageType, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, Reject,
    ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo,
    ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder,
    ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot,
    ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition,
    ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying,
    ResponseGetInstrumentByUnderlyingKeys, ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable,
    ResponseHeartbeat, ResponseLinkOrders, ResponseListAcceptedAgreements,
    ResponseListExchangePermissions, ResponseListUnacceptedAgreements, ResponseLogin,
    ResponseLoginInfo, ResponseLogout, ResponseMarketDataUpdate,
    ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder, ResponseModifyOrderReferenceData,
    ResponseNewOrder, ResponseOcoOrder, ResponseOrderSessionConfig, ResponsePnLPositionSnapshot,
    ResponsePnLPositionUpdates, ResponseProductCodes, ResponseProductRmsInfo,
    ResponseReferenceData, ResponseReplayExecutions, ResponseResumeBars,
    ResponseRithmicSystemGatewayInfo, ResponseRithmicSystemInfo, ResponseSearchSymbols,
    ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement, ResponseShowBracketStops,
    ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates,
    ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders,
    ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay,
    ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes,
    ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel,
    ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar,
    TradeRoute, TradeStatistics, UpdateEasyToBorrowList, UserAccountUpdate,
    messages::RithmicMessage,
};

/// Response from a Rithmic plant, either from a request or a subscription update.
///
/// This structure wraps all messages received from Rithmic plants, including both
/// request-response messages and subscription updates (like market data, order updates, etc.).
///
/// ## Fields
///
/// - `request_id`: Unique identifier for matching responses to requests. Empty for updates.
/// - `message`: The actual Rithmic message data (see [`RithmicMessage`])
/// - `is_update`: `true` if this is a subscription update, `false` if it's a request response
/// - `has_more`: `true` if more responses are coming for this request
/// - `multi_response`: `true` if this request type can return multiple responses
/// - `error`: Error message if the operation failed or a connection error occurred
/// - `source`: Name of the plant that sent this response (e.g., "ticker_plant", "order_plant")
///
/// ## Error Handling
///
/// The `error` field is populated in two scenarios:
///
/// ### 1. Rithmic Protocol Errors
/// When Rithmic rejects a request or encounters an error, the response will have:
/// - `error: Some("error description from Rithmic")`
/// - `message`: Usually [`RithmicMessage::Reject`]
///
/// ### 2. Connection Errors
/// When a plant's WebSocket connection fails, you'll receive:
/// - `message: RithmicMessage::ConnectionError`
/// - `error: Some("WebSocket error description")`
/// - `is_update: true` (routed to subscription channel)
/// - The plant has stopped and the channel will close
///
/// See [`RithmicMessage::ConnectionError`] for detailed error handling guidance.
///
/// ## Reconnect Guidance
///
/// A populated `error` is a **protocol-level** outcome and is NOT a reconnect
/// signal. Use [`RithmicResponse::request_error`] / [`RithmicResponse::rp_code`]
/// to inspect the classified outcome without treating it as a transport failure.
/// Reconnect only on [`RithmicResponse::is_connection_issue`] (the
/// authoritative subscription-stream signal) or the transport
/// [`RithmicError`](crate::error::RithmicError) variants (`ConnectionFailed`,
/// `ConnectionClosed`, `SendFailed`). The only benign-empty `rp_code`
/// normalization is `["7", "no data"]` (case-insensitive).
///
/// ## Example: Handling Errors
///
/// `response.error` is display-only — prefer the typed accessors below so a
/// single-element rp_code (e.g. `["5"]`) doesn't surface as an empty string.
///
/// ```no_run
/// # use rithmic_rs::RithmicResponse;
/// # use rithmic_rs::rti::messages::RithmicMessage;
/// # fn handle_response(response: RithmicResponse) {
/// // Connection-level signals come from is_connection_issue(); the `error`
/// // field on these frames carries the transport failure description.
/// if response.is_connection_issue() {
///     eprintln!(
///         "Connection issue ({:?}) from {}: {}",
///         response.message,
///         response.source,
///         response.error.as_deref().unwrap_or("")
///     );
///     // Implement reconnection logic
///     return;
/// }
///
/// // Protocol-level request rejections classify through request_error() —
/// // this preserves the full rp_code payload and avoids the empty-string
/// // footgun when a rejection carries only a code without a trailing message.
/// if let Some(err) = response.request_error() {
///     eprintln!("Request rejected from {}: {}", response.source, err);
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub struct RithmicResponse {
    pub request_id: String,
    pub message: RithmicMessage,
    pub is_update: bool,
    pub has_more: bool,
    pub multi_response: bool,

    /// Display-only view of a protocol-level rejection or non-transport
    /// failure. For typed access use [`RithmicResponse::request_error`]
    /// — it classifies rp_code rejections as
    /// [`RithmicError::RequestRejected`](crate::error::RithmicError::RequestRejected)
    /// and non-rp_code failures as
    /// [`RithmicError::ProtocolError`](crate::error::RithmicError::ProtocolError).
    /// Raw rp_code payloads are exposed via [`RithmicResponse::rp_code`] /
    /// [`RithmicResponse::rp_code_first`] / [`RithmicResponse::rp_code_text`].
    ///
    /// `Some("")` is possible: a single-element rp_code (e.g. `["5"]`) has no
    /// trailing message and renders as an empty display string. Don't branch
    /// on `error.is_some()` — use [`RithmicResponse::request_error`].
    pub error: Option<String>,
    pub source: String,
}

impl RithmicResponse {
    /// Returns true if this response represents any error (protocol-level
    /// rejection OR connection health issue). For reconnect decisions use
    /// [`RithmicResponse::is_connection_issue`] instead — a populated `error`
    /// alone is request-level, not a reconnect signal.
    pub fn is_error(&self) -> bool {
        self.error.is_some() || self.is_connection_issue()
    }

    /// Authoritative reconnect signal on the subscription stream: true for
    /// `ConnectionError`, `HeartbeatTimeout` (includes ping/heartbeat send
    /// failures), or `ForcedLogout`. Protocol-level rejections (populated
    /// `error` / rp_code rejections surfaced via
    /// [`RithmicResponse::request_error`]) do NOT set this.
    pub fn is_connection_issue(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::ConnectionError
                | RithmicMessage::HeartbeatTimeout
                | RithmicMessage::ForcedLogout(_)
        )
    }

    /// Full raw rp_code payload as received. `None` for message variants that
    /// don't carry rp_code (updates, ConnectionError, HeartbeatTimeout, etc.).
    pub fn rp_code(&self) -> Option<&[String]> {
        response_rp_code_info(&self.message).map(|(_, rp_code)| rp_code)
    }

    /// First element of rp_code (the numeric code), if present.
    pub fn rp_code_first(&self) -> Option<&str> {
        self.rp_code().and_then(|c| c.first().map(String::as_str))
    }

    /// Second element of rp_code (the human message), if present.
    pub fn rp_code_text(&self) -> Option<&str> {
        self.rp_code().and_then(|c| c.get(1).map(String::as_str))
    }

    /// Structured `rp_code` rejection. `pub(crate)` — downstream consumers
    /// should use [`RithmicResponse::request_error`], which correctly
    /// classifies non-rp_code failures as `ProtocolError` instead of silently
    /// dropping them.
    pub(crate) fn request_rejection(&self) -> Option<crate::error::RithmicRequestError> {
        if self.is_connection_issue() {
            return None;
        }

        match self.rp_code().map(classify_rp_code) {
            Some(RpCodeClassification::RequestRejected(err)) => Some(err),
            _ => None,
        }
    }

    /// Maps non-transport response failures into a typed
    /// [`RithmicError`](crate::error::RithmicError).
    /// - rp_code rejections → `RithmicError::RequestRejected`
    /// - populated `error` without rp_code → `RithmicError::ProtocolError`
    /// - transport-health events → `None` (reconnect signals, not request errors)
    pub fn request_error(&self) -> Option<crate::error::RithmicError> {
        if let Some(err) = self.request_rejection() {
            return Some(crate::error::RithmicError::RequestRejected(err));
        }

        if self.is_connection_issue() {
            return None;
        }

        self.error
            .clone()
            .map(crate::error::RithmicError::ProtocolError)
    }

    /// Returns true if this response contains market data.
    ///
    /// Market data messages include:
    /// - `BestBidOffer`: Top-of-book quotes
    /// - `LastTrade`: Trade executions
    /// - `DepthByOrder`: Order book depth updates
    /// - `DepthByOrderEndEvent`: End of depth snapshot marker
    /// - `OrderBook`: Aggregated order book
    ///
    /// # Example
    /// ```ignore
    /// if response.is_market_data() {
    ///     // Process market data update
    /// }
    /// ```
    pub fn is_market_data(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::BestBidOffer(_)
                | RithmicMessage::LastTrade(_)
                | RithmicMessage::DepthByOrder(_)
                | RithmicMessage::DepthByOrderEndEvent(_)
                | RithmicMessage::OrderBook(_)
        )
    }

    /// Returns true if this response is an order update notification.
    ///
    /// Order update messages include:
    /// - `RithmicOrderNotification`: Order status updates from Rithmic
    /// - `ExchangeOrderNotification`: Order status updates from exchange
    /// - `BracketUpdates`: Bracket order updates
    ///
    /// # Example
    /// ```ignore
    /// if response.is_order_update() {
    ///     // Process order status change
    /// }
    /// ```
    pub fn is_order_update(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::RithmicOrderNotification(_)
                | RithmicMessage::ExchangeOrderNotification(_)
                | RithmicMessage::BracketUpdates(_)
        )
    }

    /// Returns true if this response is a P&L or position update.
    ///
    /// P&L update messages include:
    /// - `AccountPnLPositionUpdate`: Account-level P&L updates
    /// - `InstrumentPnLPositionUpdate`: Per-instrument P&L updates
    ///
    /// # Example
    /// ```ignore
    /// if response.is_pnl_update() {
    ///     // Update position tracking
    /// }
    /// ```
    pub fn is_pnl_update(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::AccountPnLPositionUpdate(_)
                | RithmicMessage::InstrumentPnLPositionUpdate(_)
        )
    }
}

#[derive(Debug)]
pub(crate) struct RithmicReceiverApi {
    pub(crate) source: String,
}

impl RithmicReceiverApi {
    // Large Result size (~1296 bytes) due to RithmicMessage enum, but acceptable since
    // the Result is immediately matched and not passed through deep call stacks.
    #[allow(clippy::result_large_err)]
    pub(crate) fn buf_to_message(&self, data: Bytes) -> Result<RithmicResponse, RithmicResponse> {
        if data.len() < 4 {
            error!("Received message too short: {} bytes", data.len());

            return Err(RithmicResponse {
                request_id: "".to_string(),
                message: RithmicMessage::Unknown,
                is_update: false,
                has_more: false,
                multi_response: false,
                error: Some(format!("Message too short: {} bytes", data.len())),
                source: self.source.clone(),
            });
        }

        let payload = &data[4..];

        let parsed_message = match MessageType::decode(payload) {
            Ok(msg) => msg,
            Err(e) => {
                error!(
                    "Failed to decode MessageType: {} - data_size: {} bytes",
                    e,
                    data.len()
                );

                return Err(RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::Unknown,
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: Some(format!("Failed to decode message: {}", e)),
                    source: self.source.clone(),
                });
            }
        };

        let response = match parsed_message.template_id {
            11 => {
                let resp = ResponseLogin::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLogin(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            13 => {
                let resp = ResponseLogout::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLogout(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            15 => {
                let resp = ResponseReferenceData::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseReferenceData(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            17 => {
                let resp = ResponseRithmicSystemInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseRithmicSystemInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            19 => {
                let resp = ResponseHeartbeat::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseHeartbeat(resp),
                    is_update: true, // Heartbeats are connection health events - route to subscription channel
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            21 => {
                let resp = ResponseRithmicSystemGatewayInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseRithmicSystemGatewayInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            75 => {
                let resp =
                    Reject::decode(payload).map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::Reject(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            76 => {
                let resp = UserAccountUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::UserAccountUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            77 => {
                let resp = ForcedLogout::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ForcedLogout(resp),
                    is_update: true, // Forced logout is a connection health event - route to subscription channel
                    has_more: false,
                    multi_response: false,
                    error: Some("forced logout from server".to_string()),
                    source: self.source.clone(),
                }
            }
            101 => {
                let resp = ResponseMarketDataUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseMarketDataUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            103 => {
                let resp = ResponseGetInstrumentByUnderlying::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetInstrumentByUnderlying(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            104 => {
                let resp = ResponseGetInstrumentByUnderlyingKeys::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetInstrumentByUnderlyingKeys(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            106 => {
                let resp = ResponseMarketDataUpdateByUnderlying::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseMarketDataUpdateByUnderlying(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            108 => {
                let resp = ResponseGiveTickSizeTypeTable::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGiveTickSizeTypeTable(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            110 => {
                let resp = ResponseSearchSymbols::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSearchSymbols(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            112 => {
                let resp = ResponseProductCodes::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseProductCodes(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            114 => {
                let resp = ResponseFrontMonthContract::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseFrontMonthContract(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            116 => {
                let resp = ResponseDepthByOrderSnapshot::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseDepthByOrderSnapshot(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            118 => {
                let resp = ResponseDepthByOrderUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseDepthByOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            120 => {
                let resp = ResponseGetVolumeAtPrice::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetVolumeAtPrice(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            122 => {
                let resp = ResponseAuxilliaryReferenceData::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAuxilliaryReferenceData(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            150 => {
                let resp =
                    LastTrade::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::LastTrade(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            151 => {
                let resp = BestBidOffer::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::BestBidOffer(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            152 => {
                let resp = TradeStatistics::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TradeStatistics(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            153 => {
                let resp = QuoteStatistics::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::QuoteStatistics(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            154 => {
                let resp = IndicatorPrices::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::IndicatorPrices(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            155 => {
                let resp = EndOfDayPrices::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::EndOfDayPrices(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            156 => {
                let resp =
                    OrderBook::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::OrderBook(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            157 => {
                let resp =
                    MarketMode::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::MarketMode(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            158 => {
                let resp = OpenInterest::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::OpenInterest(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            159 => {
                let resp = FrontMonthContractUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::FrontMonthContractUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            160 => {
                let resp = DepthByOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::DepthByOrder(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            161 => {
                let resp = DepthByOrderEndEvent::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::DepthByOrderEndEvent(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            162 => {
                let resp = SymbolMarginRate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::SymbolMarginRate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            163 => {
                let resp = OrderPriceLimits::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::OrderPriceLimits(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            201 => {
                let resp = ResponseTimeBarUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTimeBarUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            203 => {
                let resp = ResponseTimeBarReplay::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTimeBarReplay(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            205 => {
                let resp = ResponseTickBarUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTickBarUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            207 => {
                let resp = ResponseTickBarReplay::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTickBarReplay(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            209 => {
                let resp = ResponseVolumeProfileMinuteBars::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseVolumeProfileMinuteBars(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            211 => {
                let resp = ResponseResumeBars::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseResumeBars(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            250 => {
                let resp =
                    TimeBar::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TimeBar(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            251 => {
                let resp =
                    TickBar::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TickBar(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            301 => {
                let resp = ResponseLoginInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLoginInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            303 => {
                let resp = ResponseAccountList::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAccountList(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            305 => {
                let resp = ResponseAccountRmsInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAccountRmsInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            307 => {
                let resp = ResponseProductRmsInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseProductRmsInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            309 => {
                let resp = ResponseSubscribeForOrderUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSubscribeForOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            311 => {
                let resp = ResponseTradeRoutes::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTradeRoutes(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            313 => {
                let resp = ResponseNewOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseNewOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            315 => {
                let resp = ResponseModifyOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseModifyOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            317 => {
                let resp = ResponseCancelOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseCancelOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            319 => {
                let resp = ResponseShowOrderHistoryDates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistoryDates(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            321 => {
                let resp = ResponseShowOrders::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            323 => {
                let resp = ResponseShowOrderHistory::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistory(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            325 => {
                let resp = ResponseShowOrderHistorySummary::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistorySummary(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            327 => {
                let resp = ResponseShowOrderHistoryDetail::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistoryDetail(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            329 => {
                let resp = ResponseOcoOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseOcoOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            331 => {
                let resp = ResponseBracketOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseBracketOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            333 => {
                let resp = ResponseUpdateTargetBracketLevel::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseUpdateTargetBracketLevel(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            335 => {
                let resp = ResponseUpdateStopBracketLevel::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseUpdateStopBracketLevel(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            337 => {
                let resp = ResponseSubscribeToBracketUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSubscribeToBracketUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            339 => {
                let resp = ResponseShowBrackets::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowBrackets(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            341 => {
                let resp = ResponseShowBracketStops::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowBracketStops(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            343 => {
                let resp = ResponseListExchangePermissions::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseListExchangePermissions(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            345 => {
                let resp = ResponseLinkOrders::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLinkOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            347 => {
                let resp = ResponseCancelAllOrders::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseCancelAllOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            349 => {
                let resp = ResponseEasyToBorrowList::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseEasyToBorrowList(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            350 => {
                let resp =
                    TradeRoute::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TradeRoute(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            351 => {
                let resp = RithmicOrderNotification::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::RithmicOrderNotification(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            352 => {
                let resp = ExchangeOrderNotification::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ExchangeOrderNotification(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            353 => {
                let resp = BracketUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::BracketUpdates(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            354 => {
                let resp = AccountListUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::AccountListUpdates(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            355 => {
                let resp = UpdateEasyToBorrowList::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::UpdateEasyToBorrowList(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            356 => {
                let resp = AccountRmsUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::AccountRmsUpdates(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            401 => {
                let resp = ResponsePnLPositionUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponsePnLPositionUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            403 => {
                let resp = ResponsePnLPositionSnapshot::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponsePnLPositionSnapshot(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            450 => {
                let resp = InstrumentPnLPositionUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::InstrumentPnLPositionUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            451 => {
                let resp = AccountPnLPositionUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::AccountPnLPositionUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            501 => {
                let resp = ResponseListUnacceptedAgreements::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseListUnacceptedAgreements(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            503 => {
                let resp = ResponseListAcceptedAgreements::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseListAcceptedAgreements(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            505 => {
                let resp = ResponseAcceptAgreement::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAcceptAgreement(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            507 => {
                let resp = ResponseShowAgreement::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowAgreement(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            509 => {
                let resp = ResponseSetRithmicMrktDataSelfCertStatus::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSetRithmicMrktDataSelfCertStatus(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3501 => {
                let resp = ResponseModifyOrderReferenceData::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseModifyOrderReferenceData(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3503 => {
                let resp = ResponseOrderSessionConfig::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseOrderSessionConfig(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3505 => {
                let resp = ResponseExitPosition::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseExitPosition(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            3507 => {
                let resp = ResponseReplayExecutions::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseReplayExecutions(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3509 => {
                let resp = ResponseAccountRmsUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAccountRmsUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            _ => {
                error!(
                    "Unknown message type received - template_id: {}, data_size: {} bytes",
                    parsed_message.template_id,
                    data.len()
                );

                // Unknown templates are unsolicited; route as an update so we
                // don't spam "no responder found" via the request handler.
                return Err(RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::Unknown,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some(format!(
                        "Unknown message type: template_id={}",
                        parsed_message.template_id
                    )),
                    source: self.source.clone(),
                });
            }
        };

        Ok(response)
    }
}

// Per the Rithmic R|Protocol Reference Guide (§3 "Responses From Server"):
// a response message carries either `rq_hndlr_rp_code` OR `rp_code`, never
// both. The *presence* of `rq_hndlr_rp_code` means more frames follow;
// `rp_code` marks the terminal frame. The value inside `rq_handler_rp_code`
// is not the multipart signal — presence is. Keying on `[0] == "0"` silently
// truncates multipart responses whose intermediate frames carry a non-"0"
// status.
//
// proto3 `repeated string` has no "absent" vs "empty" distinction on the wire,
// so "presence" is equivalent to "non-empty".
fn has_multiple(rq_handler_rp_code: &[String]) -> bool {
    !rq_handler_rp_code.is_empty()
}

/// Classified outcome of a Rithmic `rp_code` tuple.
///
/// `rp_code` is a protocol-level response code, not a transport signal. Any
/// non-success classification here represents a request-level result and has
/// no bearing on WebSocket/connection health.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RpCodeClassification {
    /// Request succeeded (rp_code is empty or `["0"]`).
    Success,
    /// Benign empty result — currently only `["7", "no data"]` (case-insensitive).
    KnownBenignEmpty,
    /// Protocol-level rejection (rp_code reports a non-zero failure code).
    RequestRejected(crate::error::RithmicRequestError),
}

impl RpCodeClassification {
    /// Returns the human-readable rejection message, or `None` for
    /// `Success` / `KnownBenignEmpty`.
    ///
    /// For a single-element `rp_code` like `["5"]` the inner
    /// `message` is `None`; callers of the legacy [`RithmicResponse::error`]
    /// field expect a `Some(_)` to gate on "is this a rejection", so we map
    /// that case to `Some(String::new())` here. New code should use
    /// [`RithmicResponse::request_error`] which exposes the typed `Option`.
    fn error_message(&self) -> Option<String> {
        match self {
            Self::Success | Self::KnownBenignEmpty => None,
            Self::RequestRejected(err) => Some(err.message.clone().unwrap_or_default()),
        }
    }
}

// INVARIANT: every variant in this list must have an rp_code field on its
// inner proto. If you add a Response* variant to RithmicMessage whose
// proto carries rp_code, add it here AND add a decode-time
// `get_error(&resp.rp_code)` call in the matching decoder arm.
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
        fn response_rp_code_info(message: &RithmicMessage) -> Option<(&'static str, &[String])> {
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

// Single extension point for benign `rp_code` normalizations. Any new mapping
// MUST match exactly on both code AND message and ship with a captured-fixture
// decode test — e.g. `["7", "an error occurred while parsing data."]` shares
// code "7" but is a real error.
fn classify_rp_code(rp_code: &[String]) -> RpCodeClassification {
    // Per §2.1.b of the Rithmic Reference Guide, `rp_code[0] == "0"` is the
    // authoritative "success" signal regardless of whether a trailing message
    // is present. `[]` is also success (e.g. an intermediate multipart frame
    // that doesn't carry rp_code at all wouldn't reach here anyway, but be
    // conservative).
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

fn get_error(rp_code: &[String]) -> Option<String> {
    classify_rp_code(rp_code).error_message()
}

fn decode_error(source: &str, e: prost::DecodeError, is_update: bool) -> RithmicResponse {
    error!("Failed to decode protobuf message: {}", e);

    RithmicResponse {
        request_id: "".to_string(),
        message: RithmicMessage::Unknown,
        is_update,
        has_more: false,
        multi_response: false,
        error: Some(format!("Failed to decode message: {}", e)),
        source: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test response with a specific message type
    fn make_response(message: RithmicMessage) -> RithmicResponse {
        RithmicResponse {
            request_id: String::new(),
            message,
            is_update: false,
            has_more: false,
            multi_response: false,
            error: None,
            source: "test".to_string(),
        }
    }

    fn make_response_with_error(message: RithmicMessage, error: &str) -> RithmicResponse {
        RithmicResponse {
            error: Some(error.to_string()),
            ..make_response(message)
        }
    }

    fn encode_with_header<T: Message>(message: &T) -> Bytes {
        let mut payload = Vec::new();

        message.encode(&mut payload).unwrap();

        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();

        framed.extend(payload);

        Bytes::from(framed)
    }

    fn decode_with_api<T: Message>(message: &T) -> RithmicResponse {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        api.buf_to_message(encode_with_header(message)).unwrap()
    }

    // =========================================================================
    // is_error() tests
    // =========================================================================

    #[test]
    fn is_error_true_when_error_field_set() {
        // Even with a normal message, if error field is set, is_error should be true
        let response = make_response_with_error(
            RithmicMessage::ResponseHeartbeat(ResponseHeartbeat::default()),
            "some error",
        );

        assert!(response.is_error());
    }

    #[test]
    fn is_error_true_for_connection_issues_without_error_field() {
        // Connection issues should be errors even without error field set
        let response = make_response(RithmicMessage::ConnectionError);

        assert!(response.is_error());
        assert!(response.error.is_none()); // Verify error field is not set
    }

    #[test]
    fn is_error_false_for_normal_response() {
        let response = make_response(RithmicMessage::ResponseHeartbeat(
            ResponseHeartbeat::default(),
        ));

        assert!(!response.is_error());
    }

    // =========================================================================
    // is_connection_issue() tests
    // =========================================================================

    #[test]
    fn is_connection_issue_detects_all_connection_error_types() {
        // Test all three connection issue types
        let connection_error = make_response(RithmicMessage::ConnectionError);
        let heartbeat_timeout = make_response(RithmicMessage::HeartbeatTimeout);
        let forced_logout = make_response(RithmicMessage::ForcedLogout(ForcedLogout::default()));

        assert!(connection_error.is_connection_issue());
        assert!(heartbeat_timeout.is_connection_issue());
        assert!(forced_logout.is_connection_issue());
    }

    #[test]
    fn is_connection_issue_false_for_reject() {
        // Reject is an error but NOT a connection issue
        let response = make_response(RithmicMessage::Reject(Reject::default()));

        assert!(!response.is_connection_issue());
    }

    #[test]
    fn reject_decodes_as_non_update() {
        let response = decode_with_api(&Reject {
            template_id: 75,
            ..Reject::default()
        });

        assert!(matches!(response.message, RithmicMessage::Reject(_)));
        assert!(!response.is_update);
    }

    #[test]
    fn trade_route_decodes_as_update() {
        let response = decode_with_api(&TradeRoute {
            template_id: 350,
            ..TradeRoute::default()
        });

        assert!(matches!(response.message, RithmicMessage::TradeRoute(_)));
        assert!(response.is_update);
    }

    #[test]
    fn update_easy_to_borrow_list_decodes_as_update() {
        let response = decode_with_api(&UpdateEasyToBorrowList {
            template_id: 355,
            ..UpdateEasyToBorrowList::default()
        });

        assert!(matches!(
            response.message,
            RithmicMessage::UpdateEasyToBorrowList(_)
        ));
        assert!(response.is_update);
    }

    // =========================================================================
    // is_market_data() tests
    // =========================================================================

    #[test]
    fn is_market_data_true_for_market_data_types() {
        let bbo = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));
        let trade = make_response(RithmicMessage::LastTrade(LastTrade::default()));
        let depth = make_response(RithmicMessage::DepthByOrder(DepthByOrder::default()));
        let depth_end = make_response(RithmicMessage::DepthByOrderEndEvent(
            DepthByOrderEndEvent::default(),
        ));
        let orderbook = make_response(RithmicMessage::OrderBook(OrderBook::default()));

        assert!(bbo.is_market_data());
        assert!(trade.is_market_data());
        assert!(depth.is_market_data());
        assert!(depth_end.is_market_data());
        assert!(orderbook.is_market_data());
    }

    #[test]
    fn is_market_data_false_for_order_notifications() {
        // Order notifications are NOT market data
        let response = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));

        assert!(!response.is_market_data());
    }

    // =========================================================================
    // is_order_update() tests
    // =========================================================================

    #[test]
    fn is_order_update_true_for_order_notification_types() {
        let rithmic_notif = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));
        let exchange_notif = make_response(RithmicMessage::ExchangeOrderNotification(
            ExchangeOrderNotification::default(),
        ));
        let bracket = make_response(RithmicMessage::BracketUpdates(BracketUpdates::default()));

        assert!(rithmic_notif.is_order_update());
        assert!(exchange_notif.is_order_update());
        assert!(bracket.is_order_update());
    }

    #[test]
    fn is_order_update_false_for_market_data() {
        // Market data is NOT an order update
        let response = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));

        assert!(!response.is_order_update());
    }

    // =========================================================================
    // is_pnl_update() tests
    // =========================================================================

    #[test]
    fn is_pnl_update_true_for_pnl_types() {
        let account_pnl = make_response(RithmicMessage::AccountPnLPositionUpdate(
            AccountPnLPositionUpdate::default(),
        ));
        let instrument_pnl = make_response(RithmicMessage::InstrumentPnLPositionUpdate(
            InstrumentPnLPositionUpdate::default(),
        ));

        assert!(account_pnl.is_pnl_update());
        assert!(instrument_pnl.is_pnl_update());
    }

    #[test]
    fn is_pnl_update_false_for_order_updates() {
        // Order updates are NOT P&L updates
        let response = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));

        assert!(!response.is_pnl_update());
    }

    // =========================================================================
    // get_error() / has_multiple() unit tests
    // =========================================================================

    #[test]
    fn get_error_returns_none_for_empty_rp_code() {
        assert_eq!(super::get_error(&[]), None);
    }

    #[test]
    fn get_error_returns_none_for_zero_rp_code() {
        assert_eq!(super::get_error(&["0".to_string()]), None);
    }

    #[test]
    fn get_error_returns_none_for_no_data_rp_code() {
        // rp_code = ["7", "no data"] means "successful query, zero results" across all
        // Rithmic list/replay/search responses — must not be treated as an error.
        let rp_code = vec!["7".to_string(), "no data".to_string()];

        assert_eq!(get_error(&rp_code), None);
    }

    #[test]
    fn get_error_returns_none_for_no_data_case_insensitive() {
        let rp_code = vec!["7".to_string(), "No Data".to_string()];

        assert_eq!(get_error(&rp_code), None);
    }

    #[test]
    fn get_error_returns_some_for_other_code_7_messages() {
        // code "7" with a different message is still an error
        let rp_code = vec!["7".to_string(), "permission denied".to_string()];

        assert!(get_error(&rp_code).is_some());
    }

    #[test]
    fn get_error_returns_message_only_for_code_7_with_error() {
        // New shape: `get_error` returns the human message (second rp_code
        // element), matching `RpCodeClassification::error_message()`.
        assert_eq!(
            super::get_error(&["7".to_string(), "permission denied".to_string()]),
            Some("permission denied".to_string())
        );
    }

    #[test]
    fn get_error_returns_second_element_for_non_zero_non_7_code() {
        assert_eq!(
            super::get_error(&["3".to_string(), "bad request".to_string()]),
            Some("bad request".to_string())
        );
    }

    #[test]
    fn get_error_returns_empty_string_when_no_second_element() {
        // Single-element rp_codes (e.g. `["5"]`) produce `RequestRejected`
        // with an empty `message`. `error_message()` forwards that as
        // `Some(String::new())`; Display renders the structured form as `[5]`.
        assert_eq!(super::get_error(&["5".to_string()]), Some(String::new()));
    }

    // Per §3 of the Rithmic Reference Guide, presence of `rq_hndlr_rp_code`
    // (not any particular value) signals "more frames follow". Our has_multiple
    // mirrors that: any non-empty slice means more frames follow; an empty
    // slice means the field wasn't populated (terminal frame, rp_code is what
    // gets inspected instead).

    #[test]
    fn has_multiple_true_for_zero_only() {
        assert!(super::has_multiple(&["0".to_string()]));
    }

    #[test]
    fn has_multiple_true_for_non_zero_code() {
        // Intermediate frames may carry richer status values here; presence
        // alone means "more frames follow".
        assert!(super::has_multiple(&["7".to_string()]));
    }

    #[test]
    fn has_multiple_true_for_any_present_payload() {
        assert!(super::has_multiple(&["1".to_string(), "0".to_string()]));
    }

    #[test]
    fn has_multiple_false_for_empty() {
        assert!(!super::has_multiple(&[]));
    }

    // =========================================================================
    // classify_rp_code unit tests
    // =========================================================================

    #[test]
    fn classify_rp_code_empty_is_success() {
        assert_eq!(
            super::classify_rp_code(&[]),
            super::RpCodeClassification::Success
        );
    }

    #[test]
    fn classify_rp_code_zero_is_success() {
        assert_eq!(
            super::classify_rp_code(&["0".to_string()]),
            super::RpCodeClassification::Success
        );
    }

    #[test]
    fn classify_rp_code_zero_with_trailing_annotation_is_success() {
        // Per §2.1.b of the Rithmic Reference Guide, rp_code[0] == "0" is the
        // authoritative success signal. A server that annotates success with
        // a trailing message (e.g. ["0", "ok"] or ["0", ""]) must not be
        // silently reclassified as a rejection.
        assert_eq!(
            super::classify_rp_code(&["0".to_string(), "ok".to_string()]),
            super::RpCodeClassification::Success
        );
        assert_eq!(
            super::classify_rp_code(&["0".to_string(), String::new()]),
            super::RpCodeClassification::Success
        );
    }

    #[test]
    fn classify_rp_code_seven_no_data_lowercase_is_known_benign_empty() {
        assert_eq!(
            super::classify_rp_code(&["7".to_string(), "no data".to_string()]),
            super::RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_seven_no_data_mixed_case_is_known_benign_empty() {
        assert_eq!(
            super::classify_rp_code(&["7".to_string(), "No Data".to_string()]),
            super::RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_seven_no_data_upper_is_known_benign_empty() {
        assert_eq!(
            super::classify_rp_code(&["7".to_string(), "NO DATA".to_string()]),
            super::RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_seven_other_msg_is_request_rejected() {
        use crate::error::RithmicRequestError;
        let rp_code = vec!["7".to_string(), "permission denied".to_string()];

        assert_eq!(
            super::classify_rp_code(&rp_code),
            super::RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("7".to_string()),
                message: Some("permission denied".to_string()),
            })
        );
    }

    #[test]
    fn classify_rp_code_non_zero_two_fields_is_request_rejected() {
        use crate::error::RithmicRequestError;
        let rp_code = vec!["3".to_string(), "bad request".to_string()];

        assert_eq!(
            super::classify_rp_code(&rp_code),
            super::RpCodeClassification::RequestRejected(RithmicRequestError {
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
        use crate::error::RithmicRequestError;
        let rp_code = vec!["5".to_string()];

        assert_eq!(
            super::classify_rp_code(&rp_code),
            super::RpCodeClassification::RequestRejected(RithmicRequestError {
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
        use crate::error::RithmicRequestError;
        let rp_code = vec![
            "7".to_string(),
            "an error occurred while parsing data.".to_string(),
        ];

        assert_eq!(
            super::classify_rp_code(&rp_code),
            super::RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("7".to_string()),
                message: Some("an error occurred while parsing data.".to_string()),
            })
        );
    }

    #[test]
    fn list_accounts_no_data_decodes_as_ok() {
        // rp_code = ["7", "no data"] on a ResponseAccountList (list-style response)
        // should produce Ok with no error, confirming the allowlist normalization
        // flows end-to-end for list responses as well as replay responses.
        use crate::rti::ResponseAccountList;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseAccountList {
            template_id: 303,
            user_msg: vec!["req-1".to_string()],
            rq_handler_rp_code: vec![],
            rp_code: vec!["7".to_string(), "no data".to_string()],
            ..ResponseAccountList::default()
        }));

        assert!(
            result.is_ok(),
            "expected Ok but got Err: {:?}",
            result.err()
        );
        let response = result.unwrap();

        assert_eq!(response.error, None);
        assert!(!response.is_error());
        assert!(!response.is_connection_issue());
    }

    #[test]
    fn response_login_rejection_decodes_with_structured_error() {
        // Structured rejection must be exposed via `request_rejection()` alongside
        // the legacy `error: Option<String>` for protocol-level rejections.
        use crate::error::RithmicRequestError;
        use crate::rti::ResponseLogin;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseLogin {
            template_id: 11,
            user_msg: vec!["req-1".to_string()],
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            ..ResponseLogin::default()
        }));
        let response = match result {
            Ok(r) => r,
            Err(r) => r,
        };

        assert_eq!(response.error.as_deref(), Some("bad request"));
        assert_eq!(
            response.request_rejection(),
            Some(RithmicRequestError {
                rp_code: vec!["3".to_string(), "bad request".to_string()],
                code: Some("3".to_string()),
                message: Some("bad request".to_string()),
            })
        );
    }

    #[test]
    fn response_order_session_config_parse_error_decodes_with_structured_error() {
        // Captured fixture: rp_code = ["7", "an error occurred while parsing data."]
        // must decode as a RequestRejected with the full rp_code payload
        // preserved. It MUST NOT be swallowed as KnownBenignEmpty.
        use crate::error::RithmicRequestError;
        use crate::rti::ResponseOrderSessionConfig;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseOrderSessionConfig {
            template_id: 3503,
            user_msg: vec!["req-1".to_string()],
            rp_code: vec![
                "7".to_string(),
                "an error occurred while parsing data.".to_string(),
            ],
        }));
        let response = match result {
            Ok(r) => r,
            Err(r) => r,
        };

        assert_eq!(
            response.error.as_deref(),
            Some("an error occurred while parsing data.")
        );
        assert_eq!(
            response.request_rejection(),
            Some(RithmicRequestError {
                rp_code: vec![
                    "7".to_string(),
                    "an error occurred while parsing data.".to_string(),
                ],
                code: Some("7".to_string()),
                message: Some("an error occurred while parsing data.".to_string()),
            })
        );
    }

    #[test]
    fn response_login_rejection_decodes_with_error() {
        // Protocol rejection populates `error` and `is_error()` but must NOT
        // trip `is_connection_issue()` — that would mis-drive reconnection.
        use crate::rti::ResponseLogin;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseLogin {
            template_id: 11,
            user_msg: vec!["req-1".to_string()],
            rp_code: vec!["3".to_string(), "bad request".to_string()],
            ..ResponseLogin::default()
        }));
        let response = match result {
            Ok(r) => r,
            Err(r) => r,
        };

        assert_eq!(response.error.as_deref(), Some("bad request"));
        assert!(response.is_error());
        assert!(!response.is_connection_issue());
    }

    #[test]
    fn replay_no_data_decodes_as_ok() {
        // rp_code = ["7", "no data"] on a ResponseReplayExecutions should produce Ok,
        // confirming the fix flows end-to-end through buf_to_message.
        use crate::rti::ResponseReplayExecutions;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseReplayExecutions {
            template_id: 3507,
            user_msg: vec!["req-1".to_string()],
            rp_code: vec!["7".to_string(), "no data".to_string()],
        }));

        assert!(
            result.is_ok(),
            "expected Ok but got Err: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap().error, None);
    }

    // =========================================================================
    // PR 60 ported tests: typed rejection surface and macro-driven rp_code info
    // =========================================================================

    #[test]
    fn reject_with_non_zero_rp_code_decodes_as_ok_with_error() {
        // rp_code-carrying responses must reach Ok(_) with `error` populated;
        // `buf_to_message` no longer returns `Err(_)` for rp_code rejections.
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&Reject {
            template_id: 75,
            user_msg: vec!["req-2".to_string()],
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
        }));

        let response = result.expect("reject with rp_code error should still decode");

        assert!(matches!(response.message, RithmicMessage::Reject(_)));
        assert_eq!(response.error.as_deref(), Some("permission denied"));
        assert!(!response.is_connection_issue());
    }

    #[test]
    fn response_request_error_maps_code_6_to_request_rejected_with_full_text() {
        use crate::error::{RithmicError, RithmicRequestError};
        use crate::rti::ResponseListAcceptedAgreements;
        let response = decode_with_api(&ResponseListAcceptedAgreements {
            template_id: 503,
            user_msg: vec!["req-4".to_string()],
            rp_code: vec!["6".to_string(), "agreement already signed".to_string()],
            ..ResponseListAcceptedAgreements::default()
        });

        assert_eq!(response.rp_code_first(), Some("6"));
        assert_eq!(response.rp_code_text(), Some("agreement already signed"));
        assert!(matches!(
            response.request_error(),
            Some(RithmicError::RequestRejected(RithmicRequestError { rp_code, code, message }))
                if rp_code == vec!["6".to_string(), "agreement already signed".to_string()]
                    && code.as_deref() == Some("6")
                    && message.as_deref() == Some("agreement already signed")
        ));
    }

    #[test]
    fn search_symbols_multipart_uses_rq_handler_field_presence_not_value() {
        // Per §3 of the Rithmic Reference Guide, presence of `rq_handler_rp_code`
        // on an intermediate multipart frame means "more frames follow",
        // regardless of the value inside. The terminal frame carries `rp_code`
        // instead (the two fields are mutually exclusive on the wire).
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        // Intermediate frame with a non-"0" rq_handler_rp_code — previously
        // dropped by has_multiple's `[0] == "0"` gate, which would truncate
        // legitimate multipart responses.
        let intermediate = api
            .buf_to_message(encode_with_header(&ResponseSearchSymbols {
                template_id: 110,
                user_msg: vec!["multi-1".to_string()],
                rq_handler_rp_code: vec!["7".to_string()],
                ..ResponseSearchSymbols::default()
            }))
            .expect("intermediate multi-response frame should decode");

        assert!(
            intermediate.has_more,
            "presence of rq_handler_rp_code must mark has_more=true regardless of value"
        );
        assert!(intermediate.multi_response);
        assert!(intermediate.error.is_none());

        // Terminal frame: no rq_handler_rp_code, rp_code set to success.
        let terminal = api
            .buf_to_message(encode_with_header(&ResponseSearchSymbols {
                template_id: 110,
                user_msg: vec!["multi-1".to_string()],
                rp_code: vec!["0".to_string()],
                ..ResponseSearchSymbols::default()
            }))
            .expect("terminal multi-response frame should decode");

        assert!(!terminal.has_more);
        assert!(terminal.multi_response);
        assert!(terminal.error.is_none());
    }

    #[test]
    fn response_rp_code_info_returns_variant_name_and_payload() {
        let message = RithmicMessage::ResponseSearchSymbols(ResponseSearchSymbols {
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
            ..ResponseSearchSymbols::default()
        });

        let (template_name, rp_code) =
            super::response_rp_code_info(&message).expect("response should expose rp_code");

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
                    let (name, rp_code) = super::response_rp_code_info(&msg)
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

    // =========================================================================
    // Mutual exclusivity tests - verify categories don't overlap unexpectedly
    // =========================================================================

    #[test]
    fn categories_are_mutually_exclusive() {
        // Market data should not be flagged as order update or pnl
        let market_data = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));

        assert!(market_data.is_market_data());
        assert!(!market_data.is_order_update());
        assert!(!market_data.is_pnl_update());
        assert!(!market_data.is_connection_issue());

        // Order update should not be flagged as market data or pnl
        let order = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));

        assert!(order.is_order_update());
        assert!(!order.is_market_data());
        assert!(!order.is_pnl_update());
        assert!(!order.is_connection_issue());

        // PnL should not be flagged as market data or order update
        let pnl = make_response(RithmicMessage::AccountPnLPositionUpdate(
            AccountPnLPositionUpdate::default(),
        ));

        assert!(pnl.is_pnl_update());
        assert!(!pnl.is_market_data());
        assert!(!pnl.is_order_update());
        assert!(!pnl.is_connection_issue());

        // Connection issue should not be in any other category
        let conn_err = make_response(RithmicMessage::ConnectionError);

        assert!(conn_err.is_connection_issue());
        assert!(!conn_err.is_market_data());
        assert!(!conn_err.is_order_update());
        assert!(!conn_err.is_pnl_update());
    }
}
