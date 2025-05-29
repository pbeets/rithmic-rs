use prost::{Message, bytes::Bytes};
use std::io::Cursor;
use tracing::{Level, event};

use crate::rti::{AccountPnLPositionUpdate, BestBidOffer, BracketUpdates, ExchangeOrderNotification, ForcedLogout, InstrumentPnLPositionUpdate, LastTrade, MessageType, Reject, ResponseAccountList, ResponseAccountRmsInfo, ResponseBracketOrder, ResponseCancelAllOrders, ResponseCancelOrder, ResponseExitPosition, ResponseHeartbeat, ResponseLogin, ResponseLogout, ResponseMarketDataUpdate, ResponseModifyOrder, ResponseNewOrder, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseProductRmsInfo, ResponseRithmicSystemInfo, ResponseSearchSymbols, ResponseShowBracketStops, ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders, ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay, ResponseTimeBarReplay, ResponseTradeRoutes, ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel, RithmicOrderNotification, TickBar, TimeBar, messages::RithmicMessage, OrderBook, ResponseDepthByOrderUpdates, DepthByOrder, ResponseDepthByOrderSnapshot};

#[derive(Debug, Clone)]
pub struct RithmicResponse {
    pub request_id: String,
    pub message: RithmicMessage,
    pub is_update: bool,
    pub has_more: bool,
    pub multi_response: bool,
    pub error: Option<String>,
    pub source: String,
}

#[derive(Debug)]
pub struct RithmicReceiverApi {
    pub source: String,
}

impl RithmicReceiverApi {
    pub fn buf_to_message(&self, data: Bytes) -> Result<RithmicResponse, RithmicResponse> {
        let parsed_message = MessageType::decode(&mut Cursor::new(&data[4..]));

        let response = match parsed_message.clone().unwrap().template_id {
            11 => {
                let resp = ResponseLogin::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseLogin(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            13 => {
                let resp = ResponseLogout::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseLogout(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            17 => {
                let resp = ResponseRithmicSystemInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseRithmicSystemInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            19 => {
                let resp = ResponseHeartbeat::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ResponseHeartbeat(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            75 => {
                let resp = Reject::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::Reject(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            77 => {
                let resp = ForcedLogout::decode(&mut Cursor::new(&data[4..])).unwrap();

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ForcedLogout(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: Some("forced logout from server".to_string()),
                    source: self.source.clone(),
                }
            }
            101 => {
                let resp = ResponseMarketDataUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseMarketDataUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            110 => {
                let resp = ResponseSearchSymbols::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseSearchSymbols(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            116 => {
                let resp = ResponseDepthByOrderSnapshot::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseDepthByOrderSnapshot(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            118 => {
                let resp = ResponseDepthByOrderUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseDepthByOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            150 => {
                let resp = LastTrade::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = BestBidOffer::decode(&mut Cursor::new(&data[4..])).unwrap();

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
            156 => {
                let resp = OrderBook::decode(&mut Cursor::new(&data[4..])).unwrap();

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
            160 => {
                let resp = DepthByOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                
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
            203 => {
                let resp = ResponseTimeBarReplay::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseTimeBarReplay(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            207 => {
                let resp = ResponseTickBarReplay::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseTickBarReplay(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            250 => {
                let resp = TimeBar::decode(&mut Cursor::new(&data[4..])).unwrap();

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TimeBar(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            251 => {
                let resp = TickBar::decode(&mut Cursor::new(&data[4..])).unwrap();

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TickBar(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            303 => {
                let resp = ResponseAccountList::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseAccountList(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            305 => {
                let resp = ResponseAccountRmsInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseAccountRmsInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            307 => {
                let resp = ResponseProductRmsInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseProductRmsInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            309 => {
                let resp =
                    ResponseSubscribeForOrderUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseSubscribeForOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            311 => {
                let resp = ResponseTradeRoutes::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseTradeRoutes(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            313 => {
                let resp = ResponseNewOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseNewOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            315 => {
                let resp = ResponseModifyOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseModifyOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            317 => {
                let resp = ResponseCancelOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseCancelOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            319 => {
                let resp =
                    ResponseShowOrderHistoryDates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowOrderHistoryDates(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            321 => {
                let resp = ResponseShowOrders::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            323 => {
                let resp = ResponseShowOrderHistory::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowOrderHistory(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            325 => {
                let resp =
                    ResponseShowOrderHistorySummary::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowOrderHistorySummary(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            327 => {
                let resp =
                    ResponseShowOrderHistoryDetail::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowOrderHistoryDetail(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            331 => {
                let resp = ResponseBracketOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseBracketOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            333 => {
                let resp =
                    ResponseUpdateTargetBracketLevel::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseUpdateTargetBracketLevel(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            335 => {
                let resp =
                    ResponseUpdateStopBracketLevel::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseUpdateStopBracketLevel(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            337 => {
                let resp = ResponseSubscribeToBracketUpdates::decode(&mut Cursor::new(&data[4..]))
                    .unwrap();
                let error = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseSubscribeToBracketUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            339 => {
                let resp = ResponseShowBrackets::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowBrackets(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error: err,
                    source: self.source.clone(),
                }
            }
            341 => {
                let resp = ResponseShowBracketStops::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseShowBracketStops(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error: err,
                    source: self.source.clone(),
                }
            }
            347 => {
                let resp = ResponseCancelAllOrders::decode(&mut Cursor::new(&data[4..])).unwrap();
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseCancelAllOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: err,
                    source: self.source.clone(),
                }
            }
            351 => {
                let resp = RithmicOrderNotification::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = ExchangeOrderNotification::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = BracketUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();

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
            3505 => {
                let resp = ResponseExitPosition::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponseExitPosition(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error: err,
                    source: self.source.clone(),
                }
            }
            401 => {
                let resp =
                    ResponsePnLPositionUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponsePnLPositionUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: err,
                    source: self.source.clone(),
                }
            }
            403 => {
                let resp =
                    ResponsePnLPositionSnapshot::decode(&mut Cursor::new(&data[4..])).unwrap();
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg[0].clone(),
                    message: RithmicMessage::ResponsePnLPositionSnapshot(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: err,
                    source: self.source.clone(),
                }
            }
            450 => {
                let resp =
                    InstrumentPnLPositionUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = AccountPnLPositionUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
            _ => {
                panic!("Unknown message type: {:#01x?}", parsed_message)
            }
        };

        // Handle errors
        let err = self.check_message_error(&response);

        if let Some(error) = err {
            event!(
                Level::ERROR,
                "receiver_api: error {:#?} {:?}",
                response,
                error
            );

            return Err(response);
        }

        Ok(response)
    }

    fn has_multiple(&self, rq_handler_rp_code: &[String]) -> bool {
        rq_handler_rp_code.len() == 1 && rq_handler_rp_code[0] == "0"
    }

    fn get_error(&self, rp_code: &Vec<String>) -> Option<String> {
        if (rp_code.len() == 1 && rp_code[0] == "0") || (rp_code.is_empty()) {
            None
        } else {
            event!(Level::ERROR, "receiver_api: error {:#?}", rp_code);

            Some(rp_code[1].clone())
        }
    }

    fn check_message_error(&self, message: &RithmicResponse) -> Option<String> {
        message.error.as_ref().map(|e| e.to_string())
    }
}
