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
