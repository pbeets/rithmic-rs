use crate::util::unknown_message::UnknownTemplateMessage;

use super::{
    AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates, DepthByOrder,
    DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, ForcedLogout,
    FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode,
    OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, Reject, RequestHeartbeat,
    ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo,
    ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder,
    ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot,
    ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition,
    ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying,
    ResponseGetInstrumentByUnderlyingKeys, ResponseGetUserInfo, ResponseGetVolumeAtPrice,
    ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders,
    ResponseListAcceptedAgreements, ResponseListExchangePermissions,
    ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout,
    ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder,
    ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder,
    ResponseOrderSessionConfig, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates,
    ResponseProductCodes, ResponseProductRmsInfo, ResponseReferenceData, ResponseReplayExecutions,
    ResponseResumeBars, ResponseRithmicSystemGatewayInfo, ResponseRithmicSystemInfo,
    ResponseSearchSymbols, ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement,
    ResponseShowBracketStops, ResponseShowBrackets, ResponseShowFillHistory,
    ResponseShowOrderHistory, ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail,
    ResponseShowOrderHistorySummary, ResponseShowOrders, ResponseSubscribeForOrderUpdates,
    ResponseSubscribeToBracketUpdates, ResponseTickBarReplay, ResponseTickBarUpdate,
    ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes,
    ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel,
    ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar,
    TradeRoute, TradeStatistics, UpdateEasyToBorrowList, UserAccountUpdate, UserInfoUpdate,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RithmicMessage {
    AccountPnLPositionUpdate(AccountPnLPositionUpdate),
    AccountRmsUpdates(AccountRmsUpdates),
    BestBidOffer(BestBidOffer),
    BracketUpdates(BracketUpdates),
    DepthByOrder(DepthByOrder),
    DepthByOrderEndEvent(DepthByOrderEndEvent),
    EndOfDayPrices(EndOfDayPrices),
    ExchangeOrderNotification(ExchangeOrderNotification),
    ForcedLogout(ForcedLogout),
    FrontMonthContractUpdate(FrontMonthContractUpdate),
    IndicatorPrices(IndicatorPrices),
    InstrumentPnLPositionUpdate(InstrumentPnLPositionUpdate),
    LastTrade(LastTrade),
    MarketMode(MarketMode),
    OpenInterest(OpenInterest),
    OrderBook(OrderBook),
    OrderPriceLimits(OrderPriceLimits),
    QuoteStatistics(QuoteStatistics),
    Reject(Reject),

    /// A keep-alive frame (template 18) sent by the server. Not replied to.
    RequestHeartbeat(RequestHeartbeat),
    ResponseAcceptAgreement(ResponseAcceptAgreement),
    ResponseAccountList(ResponseAccountList),
    ResponseAccountRmsInfo(ResponseAccountRmsInfo),
    ResponseAccountRmsUpdates(ResponseAccountRmsUpdates),
    ResponseAuxilliaryReferenceData(ResponseAuxilliaryReferenceData),
    ResponseBracketOrder(ResponseBracketOrder),
    ResponseCancelAllOrders(ResponseCancelAllOrders),
    ResponseCancelOrder(ResponseCancelOrder),
    ResponseDepthByOrderSnapshot(ResponseDepthByOrderSnapshot),
    ResponseDepthByOrderUpdates(ResponseDepthByOrderUpdates),
    ResponseEasyToBorrowList(ResponseEasyToBorrowList),
    ResponseExitPosition(ResponseExitPosition),
    ResponseFrontMonthContract(ResponseFrontMonthContract),
    ResponseGetInstrumentByUnderlying(ResponseGetInstrumentByUnderlying),
    ResponseGetInstrumentByUnderlyingKeys(ResponseGetInstrumentByUnderlyingKeys),
    ResponseGetUserInfo(ResponseGetUserInfo),
    ResponseGetVolumeAtPrice(ResponseGetVolumeAtPrice),
    ResponseGiveTickSizeTypeTable(ResponseGiveTickSizeTypeTable),
    ResponseHeartbeat(ResponseHeartbeat),
    ResponseLinkOrders(ResponseLinkOrders),
    ResponseListAcceptedAgreements(ResponseListAcceptedAgreements),
    ResponseListExchangePermissions(ResponseListExchangePermissions),
    ResponseListUnacceptedAgreements(ResponseListUnacceptedAgreements),
    ResponseLogin(ResponseLogin),
    ResponseLoginInfo(ResponseLoginInfo),
    ResponseLogout(ResponseLogout),
    ResponseMarketDataUpdate(ResponseMarketDataUpdate),
    ResponseMarketDataUpdateByUnderlying(ResponseMarketDataUpdateByUnderlying),
    ResponseModifyOrder(ResponseModifyOrder),
    ResponseModifyOrderReferenceData(ResponseModifyOrderReferenceData),
    ResponseNewOrder(ResponseNewOrder),
    ResponseOcoOrder(ResponseOcoOrder),
    ResponseOrderSessionConfig(ResponseOrderSessionConfig),
    ResponsePnLPositionSnapshot(ResponsePnLPositionSnapshot),
    ResponsePnLPositionUpdates(ResponsePnLPositionUpdates),
    ResponseProductCodes(ResponseProductCodes),
    ResponseProductRmsInfo(ResponseProductRmsInfo),
    ResponseReferenceData(ResponseReferenceData),
    ResponseReplayExecutions(ResponseReplayExecutions),
    ResponseResumeBars(ResponseResumeBars),
    ResponseRithmicSystemGatewayInfo(ResponseRithmicSystemGatewayInfo),
    ResponseRithmicSystemInfo(ResponseRithmicSystemInfo),
    ResponseSearchSymbols(ResponseSearchSymbols),
    ResponseSetRithmicMrktDataSelfCertStatus(ResponseSetRithmicMrktDataSelfCertStatus),
    ResponseShowAgreement(ResponseShowAgreement),
    ResponseShowBrackets(ResponseShowBrackets),
    ResponseShowBracketStops(ResponseShowBracketStops),
    ResponseShowFillHistory(ResponseShowFillHistory),
    ResponseShowOrderHistory(ResponseShowOrderHistory),
    ResponseShowOrderHistoryDates(ResponseShowOrderHistoryDates),
    ResponseShowOrderHistoryDetail(ResponseShowOrderHistoryDetail),
    ResponseShowOrderHistorySummary(ResponseShowOrderHistorySummary),
    ResponseShowOrders(ResponseShowOrders),
    ResponseSubscribeForOrderUpdates(ResponseSubscribeForOrderUpdates),
    ResponseSubscribeToBracketUpdates(ResponseSubscribeToBracketUpdates),
    ResponseTickBarReplay(ResponseTickBarReplay),
    ResponseTickBarUpdate(ResponseTickBarUpdate),
    ResponseTimeBarReplay(ResponseTimeBarReplay),
    ResponseTimeBarUpdate(ResponseTimeBarUpdate),
    ResponseTradeRoutes(ResponseTradeRoutes),
    ResponseUpdateStopBracketLevel(ResponseUpdateStopBracketLevel),
    ResponseUpdateTargetBracketLevel(ResponseUpdateTargetBracketLevel),
    ResponseVolumeProfileMinuteBars(ResponseVolumeProfileMinuteBars),
    RithmicOrderNotification(RithmicOrderNotification),
    SymbolMarginRate(SymbolMarginRate),
    TickBar(TickBar),
    TimeBar(TimeBar),
    TradeRoute(TradeRoute),
    TradeStatistics(TradeStatistics),
    UpdateEasyToBorrowList(UpdateEasyToBorrowList),
    UserAccountUpdate(UserAccountUpdate),
    UserInfoUpdate(UserInfoUpdate),

    /// The WebSocket connection failed unexpectedly.
    ///
    /// *Note: This is a synthetic message from rithmic-rs, not from Rithmic servers.*
    ///
    /// This means the network connection to Rithmic was lost (e.g., internet dropped,
    /// the peer sent a WebSocket close frame, a write failed, or a network timeout).
    /// The plant has stopped and you'll need to reconnect.
    ///
    /// # Example
    ///
    /// ```ignore
    /// match update.message {
    ///     RithmicMessage::ConnectionError => {
    ///         tracing::error!("Connection lost: {:?}", update.error);
    ///         // Trigger your reconnection logic
    ///     }
    ///     _ => {}
    /// }
    /// ```
    ConnectionError,

    /// The connection appears to be dead (no response to keep-alive pings).
    ///
    /// *Note: This is a synthetic message from rithmic-rs, not from Rithmic servers.*
    ///
    /// This is sent when the library's internal health checks detect the connection
    /// is unresponsive. The plant will stop after sending this message.
    ///
    /// This typically happens when:
    /// - Network connectivity is lost but the socket hasn't closed yet
    /// - The Rithmic server is overloaded or unresponsive
    /// - A firewall or proxy silently dropped the connection
    ///
    /// # Example
    ///
    /// ```ignore
    /// match update.message {
    ///     RithmicMessage::HeartbeatTimeout => {
    ///         tracing::warn!("Connection unresponsive: {:?}", update.error);
    ///         // Trigger your reconnection logic
    ///     }
    ///     _ => {}
    /// }
    /// ```
    HeartbeatTimeout,

    /// A frame whose `template_id` has no message definition in this crate.
    ///
    /// The header parsed and carried a `template_id`, but no decoder is
    /// registered for it, so the body is delivered as received with
    /// `error: None` rather than as a decode failure. It can be logged,
    /// archived, or decoded by the caller.
    ///
    /// The library logs only the template id and size; the payload may carry
    /// account and order ids, so what to log is left to the caller.
    ///
    /// [`UnknownTemplateMessage`] carries the handling API and an example of
    /// logging and decoding one.
    UnknownTemplate(UnknownTemplateMessage),

    /// A frame that could not be decoded.
    ///
    /// *Note: This is a synthetic message from rithmic-rs, not from Rithmic servers.*
    ///
    /// Always accompanied by a `ProtocolError`. A `template_id` this crate
    /// doesn't map arrives as [`UnknownTemplate`](Self::UnknownTemplate).
    ///
    /// Usually comes back from the call it belongs to; when the frame names no
    /// request, it arrives on `subscription_receiver` instead.
    Unknown,
}

impl RithmicMessage {
    /// The Rithmic `template_id` of the frame this message came from, for logs.
    /// `None` for the synthetic variants this crate makes itself and that never
    /// came off the wire: `ConnectionError`, `HeartbeatTimeout` and `Unknown`.
    pub fn template_id(&self) -> Option<i32> {
        match self {
            Self::AccountPnLPositionUpdate(m) => Some(m.template_id),
            Self::AccountRmsUpdates(m) => Some(m.template_id),
            Self::BestBidOffer(m) => Some(m.template_id),
            Self::BracketUpdates(m) => Some(m.template_id),
            Self::DepthByOrder(m) => Some(m.template_id),
            Self::DepthByOrderEndEvent(m) => Some(m.template_id),
            Self::EndOfDayPrices(m) => Some(m.template_id),
            Self::ExchangeOrderNotification(m) => Some(m.template_id),
            Self::ForcedLogout(m) => Some(m.template_id),
            Self::FrontMonthContractUpdate(m) => Some(m.template_id),
            Self::IndicatorPrices(m) => Some(m.template_id),
            Self::InstrumentPnLPositionUpdate(m) => Some(m.template_id),
            Self::LastTrade(m) => Some(m.template_id),
            Self::MarketMode(m) => Some(m.template_id),
            Self::OpenInterest(m) => Some(m.template_id),
            Self::OrderBook(m) => Some(m.template_id),
            Self::OrderPriceLimits(m) => Some(m.template_id),
            Self::QuoteStatistics(m) => Some(m.template_id),
            Self::Reject(m) => Some(m.template_id),
            Self::RequestHeartbeat(m) => Some(m.template_id),
            Self::ResponseAcceptAgreement(m) => Some(m.template_id),
            Self::ResponseAccountList(m) => Some(m.template_id),
            Self::ResponseAccountRmsInfo(m) => Some(m.template_id),
            Self::ResponseAccountRmsUpdates(m) => Some(m.template_id),
            Self::ResponseAuxilliaryReferenceData(m) => Some(m.template_id),
            Self::ResponseBracketOrder(m) => Some(m.template_id),
            Self::ResponseCancelAllOrders(m) => Some(m.template_id),
            Self::ResponseCancelOrder(m) => Some(m.template_id),
            Self::ResponseDepthByOrderSnapshot(m) => Some(m.template_id),
            Self::ResponseDepthByOrderUpdates(m) => Some(m.template_id),
            Self::ResponseEasyToBorrowList(m) => Some(m.template_id),
            Self::ResponseExitPosition(m) => Some(m.template_id),
            Self::ResponseFrontMonthContract(m) => Some(m.template_id),
            Self::ResponseGetInstrumentByUnderlying(m) => Some(m.template_id),
            Self::ResponseGetInstrumentByUnderlyingKeys(m) => Some(m.template_id),
            Self::ResponseGetUserInfo(m) => Some(m.template_id),
            Self::ResponseGetVolumeAtPrice(m) => Some(m.template_id),
            Self::ResponseGiveTickSizeTypeTable(m) => Some(m.template_id),
            Self::ResponseHeartbeat(m) => Some(m.template_id),
            Self::ResponseLinkOrders(m) => Some(m.template_id),
            Self::ResponseListAcceptedAgreements(m) => Some(m.template_id),
            Self::ResponseListExchangePermissions(m) => Some(m.template_id),
            Self::ResponseListUnacceptedAgreements(m) => Some(m.template_id),
            Self::ResponseLogin(m) => Some(m.template_id),
            Self::ResponseLoginInfo(m) => Some(m.template_id),
            Self::ResponseLogout(m) => Some(m.template_id),
            Self::ResponseMarketDataUpdate(m) => Some(m.template_id),
            Self::ResponseMarketDataUpdateByUnderlying(m) => Some(m.template_id),
            Self::ResponseModifyOrder(m) => Some(m.template_id),
            Self::ResponseModifyOrderReferenceData(m) => Some(m.template_id),
            Self::ResponseNewOrder(m) => Some(m.template_id),
            Self::ResponseOcoOrder(m) => Some(m.template_id),
            Self::ResponseOrderSessionConfig(m) => Some(m.template_id),
            Self::ResponsePnLPositionSnapshot(m) => Some(m.template_id),
            Self::ResponsePnLPositionUpdates(m) => Some(m.template_id),
            Self::ResponseProductCodes(m) => Some(m.template_id),
            Self::ResponseProductRmsInfo(m) => Some(m.template_id),
            Self::ResponseReferenceData(m) => Some(m.template_id),
            Self::ResponseReplayExecutions(m) => Some(m.template_id),
            Self::ResponseResumeBars(m) => Some(m.template_id),
            Self::ResponseRithmicSystemGatewayInfo(m) => Some(m.template_id),
            Self::ResponseRithmicSystemInfo(m) => Some(m.template_id),
            Self::ResponseSearchSymbols(m) => Some(m.template_id),
            Self::ResponseSetRithmicMrktDataSelfCertStatus(m) => Some(m.template_id),
            Self::ResponseShowAgreement(m) => Some(m.template_id),
            Self::ResponseShowBrackets(m) => Some(m.template_id),
            Self::ResponseShowBracketStops(m) => Some(m.template_id),
            Self::ResponseShowFillHistory(m) => Some(m.template_id),
            Self::ResponseShowOrderHistory(m) => Some(m.template_id),
            Self::ResponseShowOrderHistoryDates(m) => Some(m.template_id),
            Self::ResponseShowOrderHistoryDetail(m) => Some(m.template_id),
            Self::ResponseShowOrderHistorySummary(m) => Some(m.template_id),
            Self::ResponseShowOrders(m) => Some(m.template_id),
            Self::ResponseSubscribeForOrderUpdates(m) => Some(m.template_id),
            Self::ResponseSubscribeToBracketUpdates(m) => Some(m.template_id),
            Self::ResponseTickBarReplay(m) => Some(m.template_id),
            Self::ResponseTickBarUpdate(m) => Some(m.template_id),
            Self::ResponseTimeBarReplay(m) => Some(m.template_id),
            Self::ResponseTimeBarUpdate(m) => Some(m.template_id),
            Self::ResponseTradeRoutes(m) => Some(m.template_id),
            Self::ResponseUpdateStopBracketLevel(m) => Some(m.template_id),
            Self::ResponseUpdateTargetBracketLevel(m) => Some(m.template_id),
            Self::ResponseVolumeProfileMinuteBars(m) => Some(m.template_id),
            Self::RithmicOrderNotification(m) => Some(m.template_id),
            Self::SymbolMarginRate(m) => Some(m.template_id),
            Self::TickBar(m) => Some(m.template_id),
            Self::TimeBar(m) => Some(m.template_id),
            Self::TradeRoute(m) => Some(m.template_id),
            Self::TradeStatistics(m) => Some(m.template_id),
            Self::UpdateEasyToBorrowList(m) => Some(m.template_id),
            Self::UserAccountUpdate(m) => Some(m.template_id),
            Self::UserInfoUpdate(m) => Some(m.template_id),
            Self::UnknownTemplate(m) => Some(m.template_id),
            Self::ConnectionError | Self::HeartbeatTimeout | Self::Unknown => None,
        }
    }
}
