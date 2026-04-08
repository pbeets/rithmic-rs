use super::{
    AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates,
    DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, ForcedLogout,
    FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode,
    OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, Reject, ResponseAcceptAgreement,
    ResponseAccountList, ResponseAccountRmsInfo, ResponseAccountRmsUpdates,
    ResponseAuxilliaryReferenceData, ResponseBracketOrder, ResponseCancelAllOrders,
    ResponseCancelOrder, ResponseDepthByOrderSnapshot, ResponseDepthByOrderUpdates,
    ResponseEasyToBorrowList, ResponseExitPosition, ResponseFrontMonthContract,
    ResponseGetInstrumentByUnderlying, ResponseGetInstrumentByUnderlyingKeys,
    ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders,
    ResponseListAcceptedAgreements, ResponseListExchangePermissions,
    ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout,
    ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder,
    ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder,
    ResponseOrderSessionConfig, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates,
    ResponseProductCodes, ResponseProductRmsInfo, ResponseReferenceData, ResponseReplayExecutions,
    ResponseResumeBars, ResponseRithmicSystemGatewayInfo, ResponseRithmicSystemInfo,
    ResponseSearchSymbols, ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement,
    ResponseShowBracketStops, ResponseShowBrackets, ResponseShowOrderHistory,
    ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary,
    ResponseShowOrders, ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates,
    ResponseTickBarReplay, ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate,
    ResponseTradeRoutes, ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel,
    ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar,
    TradeRoute, TradeStatistics, UpdateEasyToBorrowList, UserAccountUpdate,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RithmicMessage {
    AccountListUpdates(AccountListUpdates),
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
    ///         log::error!("Connection lost: {:?}", update.error);
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
    ///         log::warn!("Connection unresponsive: {:?}", update.error);
    ///         // Trigger your reconnection logic
    ///     }
    ///     _ => {}
    /// }
    /// ```
    HeartbeatTimeout,

    /// A message type that this library doesn't recognize.
    ///
    /// *Note: This is a synthetic message from rithmic-rs, not from Rithmic servers.*
    ///
    /// This shouldn't happen in normal usage. If you see this, it may indicate
    /// a newer Rithmic protocol version with message types not yet supported.
    Unknown,
}
