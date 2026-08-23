pub use super::response::RithmicResponse;
use super::rp_code::{classify_rp_code_error, reject_error};
use prost::{Message, bytes::Bytes};
use tracing::{error, warn};

use crate::{
    error::RithmicError,
    rti::{
        AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates, DepthByOrder,
        DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, ForcedLogout,
        FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade,
        MarketMode, MessageType, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics,
        Reject, RequestHeartbeat, ResponseAcceptAgreement, ResponseAccountList,
        ResponseAccountRmsInfo, ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData,
        ResponseBracketOrder, ResponseCancelAllOrders, ResponseCancelOrder,
        ResponseDepthByOrderSnapshot, ResponseDepthByOrderUpdates, ResponseEasyToBorrowList,
        ResponseExitPosition, ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying,
        ResponseGetInstrumentByUnderlyingKeys, ResponseGetUserInfo, ResponseGetVolumeAtPrice,
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
        ResponseShowFillHistory, ResponseShowOrderHistory, ResponseShowOrderHistoryDates,
        ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders,
        ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay,
        ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes,
        ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel,
        ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar,
        TimeBar, TradeRoute, TradeStatistics, UpdateEasyToBorrowList, UserAccountUpdate,
        UserInfoUpdate, messages::RithmicMessage,
    },
    util::unknown_message::UnknownTemplateMessage,
};

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
                error: Some(RithmicError::ProtocolError(format!(
                    "Message too short: {} bytes",
                    data.len()
                ))),
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

                // `template_id` is the envelope's only field, so wire-type
                // drift there still leaves the echoed `user_msg` readable.
                return Err(route_decode_failure(
                    payload,
                    RithmicResponse {
                        request_id: "".to_string(),
                        message: RithmicMessage::Unknown,
                        is_update: false,
                        has_more: false,
                        multi_response: false,
                        error: Some(RithmicError::ProtocolError(format!(
                            "Failed to decode message: {}",
                            e
                        ))),
                        source: self.source.clone(),
                    },
                ));
            }
        };

        if parsed_message.template_id <= 0 {
            error!("{}: frame carries no template_id", self.source);

            return Err(RithmicResponse {
                request_id: "".to_string(),
                message: RithmicMessage::Unknown,
                is_update: false,
                has_more: false,
                multi_response: false,
                error: Some(RithmicError::ProtocolError(
                    "Frame carries no template_id".to_string(),
                )),
                source: self.source.clone(),
            });
        }

        self.decode_body(data.slice(4..), parsed_message.template_id)
            .map_err(|response| {
                let response = route_decode_failure(payload, response);

                error!(
                    "{}: template {} failed to decode: {:?}; recovered request_id {:?}, routing it {}",
                    self.source,
                    parsed_message.template_id,
                    response.error,
                    response.request_id,
                    if response.is_update {
                        "onto the subscription"
                    } else {
                        "to the request waiting on it"
                    }
                );

                response
            })
    }

    /// Decode the body against the message type its `template_id` selects.
    ///
    /// Returns `Err` with `request_id: ""`; [`route_decode_failure`] fills in
    /// `request_id` and `is_update` before the caller sees the response.
    #[allow(clippy::result_large_err)]
    fn decode_body(
        &self,
        payload: Bytes,
        template_id: i32,
    ) -> Result<RithmicResponse, RithmicResponse> {
        let response = match template_id {
            11 => {
                let resp = ResponseLogin::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
            18 => {
                let resp = RequestHeartbeat::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    // The frame's user_msg is the server's own token, not an id
                    // this client handed out, so it is not surfaced as one.
                    request_id: "".to_string(),
                    message: RithmicMessage::RequestHeartbeat(resp),
                    is_update: true, // Server-initiated keep-alive - route to subscription channel
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            19 => {
                let resp = ResponseHeartbeat::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = Some(reject_error(&resp.rp_code));
                let request_id = resp.user_msg.first().cloned().unwrap_or_default();

                if request_id.is_empty() {
                    // A request-correlated reject reaches its caller as
                    // `resp.error`. This one has no caller to reach and is
                    // dropped in `forward_response`, so the log is the only
                    // record of it. `Reject` carries nothing but `template_id`,
                    // `user_msg` and `rp_code`, and `user_msg` is empty here.
                    warn!(
                        "{}: unsolicited reject, rp_code {:?}",
                        self.source, resp.rp_code
                    );
                }

                RithmicResponse {
                    request_id,
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
                    error: Some(RithmicError::ForcedLogout(
                        "forced logout from server".to_string(),
                    )),
                    source: self.source.clone(),
                }
            }
            101 => {
                let resp = ResponseMarketDataUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseDepthByOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            120 => {
                let resp = ResponseGetVolumeAtPrice::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
            357 => {
                let resp = UserInfoUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::UserInfoUpdate(resp),
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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
                let error = classify_rp_code_error(&resp.rp_code);

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
            3511 => {
                let resp = ResponseGetUserInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = classify_rp_code_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetUserInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            3513 => {
                let resp = ResponseShowFillHistory::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = classify_rp_code_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowFillHistory(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            _ => {
                // Not a recognized message template.
                let unknown = UnknownTemplateMessage {
                    template_id,
                    payload,
                };

                // Payload stays out of the log: it may carry account and order
                // ids, and the caller receives the frame to log as it sees fit.
                warn!(
                    "{}: unmapped Rithmic template {} ({} bytes)",
                    self.source,
                    unknown.template_id,
                    unknown.payload.len()
                );

                // No request_id can be extracted without a schema, so route as
                // an update rather than letting the request handler log
                // "no responder found".
                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::UnknownTemplate(unknown),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
        };

        Ok(response)
    }
}

// The *presence* of `rq_handler_rp_code` is the multipart signal, not the value
// inside it: keying on `[0] == "0"` truncates multipart responses whose
// intermediate frames carry a non-"0" status.
//
// A `repeated string` has no "absent" vs "empty" distinction on the wire, so
// "presence" is equivalent to "non-empty".
fn has_multiple(rq_handler_rp_code: &[String]) -> bool {
    !rq_handler_rp_code.is_empty()
}

fn decode_error(source: &str, e: prost::DecodeError, is_update: bool) -> RithmicResponse {
    RithmicResponse {
        request_id: "".to_string(),
        message: RithmicMessage::Unknown,
        is_update,
        has_more: false,
        multi_response: false,
        error: Some(RithmicError::ProtocolError(format!(
            "Failed to decode message: {}",
            e
        ))),
        source: source.to_string(),
    }
}

/// Reads the echoed `user_msg` back off a body that failed to decode.
///
/// prost skips tags a type doesn't declare, so this one-field struct decodes
/// where the real message type won't. Tag 132760 is `user_msg` everywhere in
/// [`crate::rti`] and nothing else uses it — recheck if `rti.rs` is regenerated.
#[derive(Clone, PartialEq, ::prost::Message)]
struct UserMsgProbe {
    #[prost(string, repeated, tag = "132760")]
    user_msg: Vec<String>,
}

/// Settle `request_id` and `is_update` on a decode failure.
///
/// A response already marked an update routes correctly. One built for a
/// request carries an empty `request_id`, matches no responder, and would be
/// dropped along with its `ProtocolError` — so recover the echoed id where
/// there is one, and route it as an update where there is not.
fn route_decode_failure(payload: &[u8], mut response: RithmicResponse) -> RithmicResponse {
    if response.is_update || !response.request_id.is_empty() {
        return response;
    }

    response.request_id = UserMsgProbe::decode(payload)
        .ok()
        .and_then(|probe| probe.user_msg.into_iter().next())
        .unwrap_or_default();
    response.is_update = response.request_id.is_empty();

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{RithmicError, RithmicRequestError};
    use crate::rti::{
        Reject, ResponseAccountList, ResponseGetUserInfo, ResponseListAcceptedAgreements,
        ResponseLogin, ResponseOrderSessionConfig, ResponseReplayExecutions, ResponseSearchSymbols,
        ResponseShowFillHistory, RithmicOrderNotification, TradeRoute, UpdateEasyToBorrowList,
        UserInfoUpdate, messages::RithmicMessage,
    };
    use prost::{Message, bytes::Bytes};

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

    #[test]
    fn unsolicited_reject_decodes_without_a_request_id() {
        // No echoed `user_msg` means no request id to correlate against, and
        // the rejection is not a subscription update either. `forward_response`
        // drops it; decoding it must not fail or panic.
        let response = decode_with_api(&Reject {
            template_id: 75,
            user_msg: vec![],
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
        });

        assert!(matches!(response.message, RithmicMessage::Reject(_)));
        assert!(!response.is_update);
        assert_eq!(response.request_id, "");
        assert_eq!(
            response.error,
            Some(RithmicError::RequestRejected(RithmicRequestError {
                rp_code: vec!["5".to_string(), "permission denied".to_string()],
                code: Some("5".to_string()),
                message: Some("permission denied".to_string()),
            }))
        );
    }

    #[test]
    fn reject_with_user_msg_decodes_as_request_correlated() {
        let response = decode_with_api(&Reject {
            template_id: 75,
            user_msg: vec!["req-9".to_string()],
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
        });

        assert!(matches!(response.message, RithmicMessage::Reject(_)));
        assert!(!response.is_update);
        assert_eq!(response.request_id, "req-9");
    }

    #[test]
    fn reject_carrying_a_response_success_rp_code_still_carries_an_error() {
        // The rp_code a `Response*` decodes as a success, on a request-
        // correlated `Reject`: the caller's oneshot resolves with `error` set,
        // so the documented `resp.error` check sees the rejection. The rp_code
        // reaches the caller unread, to interpret as it sees fit.
        let response = decode_with_api(&Reject {
            template_id: 75,
            user_msg: vec!["req-9".to_string()],
            rp_code: vec!["0".to_string()],
        });

        assert!(!response.is_update);
        assert_eq!(
            response.error,
            Some(RithmicError::RequestRejected(RithmicRequestError {
                rp_code: vec!["0".to_string()],
                code: Some("0".to_string()),
                message: None,
            }))
        );
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

    #[test]
    fn unmapped_template_decodes_as_update_without_error() {
        // 358 has no decoder here, so the frame must survive as an update
        // rather than an error.
        let api = RithmicReceiverApi {
            source: "order_plant".to_string(),
        };
        let notification = RithmicOrderNotification {
            template_id: 358,
            basket_id: Some("9214-2".to_string()),
            symbol: Some("MESU6".to_string()),
            ..RithmicOrderNotification::default()
        };
        let result = api.buf_to_message(encode_with_header(&notification));

        let response = result.expect("unmapped templates are not decode failures");

        assert!(response.error.is_none());
        assert!(response.is_update);
        assert_eq!(response.request_id, "");

        let RithmicMessage::UnknownTemplate(frame) = &response.message else {
            panic!("expected UnknownTemplate, got {:?}", response.message);
        };

        assert_eq!(frame.template_id, 358);

        // Byte-identical to what was framed, length prefix aside.
        assert_eq!(frame.payload, Bytes::from(notification.encode_to_vec()));
    }

    #[test]
    fn frame_without_a_template_id_stays_an_error() {
        // prost decodes a missing proto2 `required` int32 as 0, so this must
        // not be mistaken for a template we simply don't map.
        let api = RithmicReceiverApi {
            source: "order_plant".to_string(),
        };

        let mut framed = 2u32.to_be_bytes().to_vec();
        framed.extend_from_slice(&[0x08, 0x01]); // field 1 — not in MessageType

        let response = api
            .buf_to_message(Bytes::from(framed))
            .expect_err("a frame with no template_id is malformed");

        assert!(matches!(response.message, RithmicMessage::Unknown));
        assert!(matches!(
            response.error,
            Some(RithmicError::ProtocolError(_))
        ));
        assert!(!response.is_update);
    }

    /// A body whose `template_id` and `user_msg` are readable but whose fourth
    /// field contradicts the message type `template_id` selects.
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct MalformedBody {
        #[prost(int32, required, tag = "154467")]
        template_id: i32,
        #[prost(string, repeated, tag = "132760")]
        user_msg: Vec<String>,
        /// `ResponseLogin::template_version` — a string there, a varint here.
        #[prost(int32, optional, tag = "153634")]
        template_version: Option<i32>,
        /// `LastTrade::symbol` — a string there, a varint here.
        #[prost(int32, optional, tag = "110100")]
        symbol: Option<i32>,
    }

    fn malformed_response_login(user_msg: &[&str]) -> MalformedBody {
        MalformedBody {
            template_id: 11,
            user_msg: user_msg.iter().map(|m| m.to_string()).collect(),
            template_version: Some(1),
            symbol: None,
        }
    }

    /// `template_id` as a string rather than a varint, so the envelope itself
    /// fails to decode while the echoed `user_msg` stays readable.
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct DivergedEnvelope {
        #[prost(string, optional, tag = "154467")]
        template_id: Option<String>,
        #[prost(string, repeated, tag = "132760")]
        user_msg: Vec<String>,
    }

    #[test]
    fn envelope_wire_type_divergence_correlates_on_echoed_user_msg() {
        // The realistic case: schema drift on `template_id`, not a corrupt
        // frame. It takes the envelope down but leaves the echoed id readable.
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let response = api
            .buf_to_message(encode_with_header(&DivergedEnvelope {
                template_id: Some("11".to_string()),
                user_msg: vec!["req-9".to_string()],
            }))
            .expect_err("an envelope that fails to decode is a decode failure");

        assert!(matches!(
            response.error,
            Some(RithmicError::ProtocolError(_))
        ));
        assert!(!response.is_update);
        assert_eq!(response.request_id, "req-9");
    }

    #[test]
    fn response_decode_failure_correlates_on_echoed_user_msg() {
        // Template 11 is ResponseLogin; the body then contradicts its schema.
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let response = api
            .buf_to_message(encode_with_header(&malformed_response_login(&["req-7"])))
            .expect_err("a body contradicting ResponseLogin must not decode");

        assert!(matches!(response.message, RithmicMessage::Unknown));
        assert!(matches!(
            response.error,
            Some(RithmicError::ProtocolError(_))
        ));
        assert!(!response.is_update);
        assert_eq!(response.request_id, "req-7");
    }

    #[test]
    fn a_correlated_decode_failure_names_the_request_in_the_log() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let (_, logged) = crate::request_handler::log_capture::capture(|| {
            let _ = api.buf_to_message(encode_with_header(&malformed_response_login(&["req-7"])));
        });

        assert!(logged.contains("template 11 failed to decode"), "{logged}");
        assert!(
            logged.contains(r#"recovered request_id "req-7""#),
            "{logged}"
        );
        assert!(
            logged.contains("routing it to the request waiting on it"),
            "{logged}"
        );
    }

    #[test]
    fn an_uncorrelatable_decode_failure_says_so_in_the_log() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let (_, logged) = crate::request_handler::log_capture::capture(|| {
            let _ = api.buf_to_message(encode_with_header(&malformed_response_login(&[])));
        });

        assert!(logged.contains("template 11 failed to decode"), "{logged}");
        assert!(logged.contains(r#"recovered request_id """#), "{logged}");
        assert!(
            logged.contains("routing it onto the subscription"),
            "{logged}"
        );
    }

    #[test]
    fn response_decode_failure_without_user_msg_routes_as_update() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let response = api
            .buf_to_message(encode_with_header(&malformed_response_login(&[])))
            .expect_err("a body contradicting ResponseLogin must not decode");

        assert!(matches!(response.message, RithmicMessage::Unknown));
        assert!(matches!(
            response.error,
            Some(RithmicError::ProtocolError(_))
        ));
        assert!(response.is_update);
        assert_eq!(response.request_id, "");
    }

    #[test]
    fn update_arm_decode_failure_stays_an_update() {
        // Template 150 is a subscription update. Its decode failure already
        // reaches the broadcast channel, so an echoed user_msg must not divert
        // it to the request handler.
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let response = api
            .buf_to_message(encode_with_header(&MalformedBody {
                template_id: 150,
                user_msg: vec!["req-7".to_string()],
                template_version: None,
                symbol: Some(1),
            }))
            .expect_err("a body contradicting LastTrade must not decode");

        assert!(matches!(response.message, RithmicMessage::Unknown));
        assert!(response.is_update);
        assert_eq!(response.request_id, "");
    }

    // =========================================================================
    // has_multiple() unit tests
    // =========================================================================

    // has_multiple keys on presence, not value: any non-empty slice means more
    // frames follow; an empty slice means the field wasn't populated.

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

    #[test]
    fn list_accounts_no_data_decodes_as_ok() {
        // rp_code = ["7", "no data"] on a ResponseAccountList (list-style response)
        // should produce Ok with no error, confirming the allowlist normalization
        // flows end-to-end for list responses as well as replay responses.
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
    }

    #[test]
    fn response_login_rejection_decodes_with_structured_error() {
        // Structured rejection must be exposed via `request_rejection()` alongside
        // the legacy `error: Option<String>` for protocol-level rejections.
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

        assert_eq!(
            response.error,
            Some(RithmicError::RequestRejected(RithmicRequestError {
                rp_code: vec!["3".to_string(), "bad request".to_string()],
                code: Some("3".to_string()),
                message: Some("bad request".to_string()),
            }))
        );
    }

    #[test]
    fn response_order_session_config_parse_error_decodes_with_structured_error() {
        // Captured fixture: rp_code = ["7", "an error occurred while parsing data."]
        // must decode as a RequestRejected with the full rp_code payload
        // preserved. It MUST NOT be swallowed as KnownBenignEmpty.
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
            response.error,
            Some(RithmicError::RequestRejected(RithmicRequestError {
                rp_code: vec![
                    "7".to_string(),
                    "an error occurred while parsing data.".to_string(),
                ],
                code: Some("7".to_string()),
                message: Some("an error occurred while parsing data.".to_string()),
            }))
        );
    }

    #[test]
    fn response_login_rejection_decodes_with_error() {
        // Protocol rejection populates `error` and `is_error()` but must NOT
        // trip `is_connection_issue()` — that would mis-drive reconnection.
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

        assert!(matches!(
            &response.error,
            Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("bad request")
        ));
        assert!(
            !response
                .error
                .as_ref()
                .expect("error should be set")
                .is_connection_issue()
        );
    }

    #[test]
    fn replay_no_data_decodes_as_ok() {
        // rp_code = ["7", "no data"] on a ResponseReplayExecutions should produce Ok,
        // confirming the fix flows end-to-end through buf_to_message.
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
    // Typed rejection surface and macro-driven rp_code info
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
        assert!(matches!(
            &response.error,
            Some(RithmicError::RequestRejected(e)) if e.message.as_deref() == Some("permission denied")
        ));
        assert!(
            !response
                .error
                .as_ref()
                .expect("error should be set")
                .is_connection_issue()
        );
    }

    #[test]
    fn response_request_error_maps_code_6_to_request_rejected_with_full_text() {
        let response = decode_with_api(&ResponseListAcceptedAgreements {
            template_id: 503,
            user_msg: vec!["req-4".to_string()],
            rp_code: vec!["6".to_string(), "agreement already signed".to_string()],
            ..ResponseListAcceptedAgreements::default()
        });

        assert_eq!(response.rp_code_num(), Some("6"));
        assert_eq!(response.rp_code_text(), Some("agreement already signed"));
        assert!(matches!(
            &response.error,
            Some(RithmicError::RequestRejected(RithmicRequestError { rp_code, code, message }))
                if *rp_code == vec!["6".to_string(), "agreement already signed".to_string()]
                    && code.as_deref() == Some("6")
                    && message.as_deref() == Some("agreement already signed")
        ));
    }

    #[test]
    fn search_symbols_multipart_uses_rq_handler_field_presence_not_value() {
        // Presence of `rq_handler_rp_code` marks an intermediate frame,
        // regardless of the value inside.
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
    fn user_info_update_decodes_as_update() {
        let response = decode_with_api(&UserInfoUpdate {
            template_id: 357,
            ..UserInfoUpdate::default()
        });

        assert!(matches!(
            response.message,
            RithmicMessage::UserInfoUpdate(_)
        ));
        assert!(response.is_update);
        assert_eq!(response.request_id, "");
    }

    #[test]
    fn get_user_info_response_correlates_by_user_msg() {
        let response = decode_with_api(&ResponseGetUserInfo {
            template_id: 3511,
            user_msg: vec!["req-42".to_string()],
            rp_code: vec!["0".to_string()],
            ..ResponseGetUserInfo::default()
        });

        assert!(matches!(
            response.message,
            RithmicMessage::ResponseGetUserInfo(_)
        ));
        assert!(!response.is_update);
        assert!(response.multi_response);
        assert!(!response.has_more);
        assert_eq!(response.request_id, "req-42");
        assert!(response.error.is_none());
    }

    #[test]
    fn show_fill_history_streams_fills_until_the_terminal_frame() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        // One frame per fill, marked intermediate by rq_handler_rp_code.
        let fill = api
            .buf_to_message(encode_with_header(&ResponseShowFillHistory {
                template_id: 3513,
                user_msg: vec!["req-7".to_string()],
                rq_handler_rp_code: vec!["0".to_string()],
                ..ResponseShowFillHistory::default()
            }))
            .expect("fill frame should decode");

        assert!(matches!(
            fill.message,
            RithmicMessage::ResponseShowFillHistory(_)
        ));
        assert!(fill.has_more);
        assert!(fill.multi_response);
        assert_eq!(fill.request_id, "req-7");

        let terminal = api
            .buf_to_message(encode_with_header(&ResponseShowFillHistory {
                template_id: 3513,
                user_msg: vec!["req-7".to_string()],
                rp_code: vec!["0".to_string()],
                ..ResponseShowFillHistory::default()
            }))
            .expect("terminal frame should decode");

        assert!(!terminal.has_more);
        assert!(terminal.multi_response);
        assert!(terminal.error.is_none());
    }
}
