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
/// ## Example: Handling Errors
///
/// ```no_run
/// # use rithmic_rs::RithmicResponse;
/// # use rithmic_rs::rti::messages::RithmicMessage;
/// # fn handle_response(response: RithmicResponse) {
/// match response.message {
///     RithmicMessage::ConnectionError => {
///         // WebSocket connection failed
///         eprintln!(
///             "Connection error from {}: {}",
///             response.source,
///             response.error.as_ref().unwrap()
///         );
///         // Implement reconnection logic
///     }
///     RithmicMessage::Reject(reject) => {
///         // Rithmic rejected a request
///         eprintln!(
///             "Request rejected: {}",
///             response.error.as_ref().unwrap_or(&"Unknown".to_string())
///         );
///     }
///     _ => {
///         // Check error field even for successful-looking messages
///         if let Some(err) = response.error {
///             eprintln!("Error in {}: {}", response.source, err);
///         }
///     }
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
    pub error: Option<String>,
    pub source: String,
}

impl RithmicResponse {
    /// Returns true if this response represents an error condition.
    ///
    /// This checks both:
    /// - The `error` field being set (Rithmic protocol errors)
    /// - Connection issues (WebSocket errors, heartbeat timeouts, forced logout)
    ///
    /// # Example
    /// ```ignore
    /// if response.is_error() {
    ///     eprintln!("Error: {:?}", response.error);
    /// }
    /// ```
    pub fn is_error(&self) -> bool {
        self.error.is_some() || self.is_connection_issue()
    }

    /// Returns true if this response indicates a connection health issue.
    ///
    /// Connection issues include:
    /// - `ConnectionError`: WebSocket connection failed
    /// - `HeartbeatTimeout`: Connection appears dead
    /// - `ForcedLogout`: Server forcibly logged out the client
    ///
    /// These conditions typically require reconnection logic.
    ///
    /// # Example
    /// ```ignore
    /// if response.is_connection_issue() {
    ///     // Trigger reconnection
    /// }
    /// ```
    pub fn is_connection_issue(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::ConnectionError
                | RithmicMessage::HeartbeatTimeout
                | RithmicMessage::ForcedLogout(_)
        )
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

                return Err(RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::Unknown,
                    is_update: false,
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

        // Handle errors
        if let Some(error) = check_message_error(&response) {
            error!("receiver_api: error {:#?} {:?}", response, error);

            return Err(response);
        }

        Ok(response)
    }
}

fn has_multiple(rq_handler_rp_code: &[String]) -> bool {
    !rq_handler_rp_code.is_empty() && rq_handler_rp_code[0] == "0"
}

fn get_error(rp_code: &[String]) -> Option<String> {
    if (rp_code.len() == 1 && rp_code[0] == "0") || rp_code.is_empty() {
        return None;
    }

    // Rithmic uses rp_code = ["7", "no data"] to signal "successful query, zero results"
    // across all list/replay/search responses. Treat it as a normal empty outcome, not an error.
    if let (Some(code), Some(msg)) = (rp_code.first(), rp_code.get(1)) {
        if code == "7" && msg.eq_ignore_ascii_case("no data") {
            return None;
        }
    }

    let msg = rp_code
        .get(1)
        .cloned()
        .unwrap_or_else(|| rp_code[0].clone());

    Some(msg)
}

fn check_message_error(message: &RithmicResponse) -> Option<String> {
    message.error.as_ref().map(|e| e.to_string())
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
    fn get_error_returns_some_for_code_7_with_error() {
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
    fn get_error_returns_first_element_when_no_second() {
        assert_eq!(super::get_error(&["5".to_string()]), Some("5".to_string()));
    }

    #[test]
    fn has_multiple_returns_true_for_zero_code() {
        assert!(super::has_multiple(&["0".to_string()]));
    }

    #[test]
    fn has_multiple_returns_false_for_empty() {
        assert!(!super::has_multiple(&[]));
    }

    #[test]
    fn has_multiple_returns_false_for_non_zero_code() {
        assert!(!super::has_multiple(&["7".to_string()]));
    }

    #[test]
    fn has_multiple_returns_false_for_zero_as_second_element() {
        assert!(!super::has_multiple(&["1".to_string(), "0".to_string()]));
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
