use crate::error::RithmicError;
use crate::rti::messages::RithmicMessage;

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
/// - `error`: Typed error if the operation failed or a connection error occurred
/// - `source`: Name of the plant that sent this response (e.g., "ticker_plant", "order_plant")
///
/// ## Error Handling
///
/// The `error` field is `Option<RithmicError>`. Use
/// [`RithmicError::is_connection_issue`] to distinguish transport/connection
/// failures (reconnect signal) from protocol-level request rejections.
///
/// ## Example: Handling Errors
///
/// ```no_run
/// # use rithmic_rs::RithmicResponse;
/// # fn handle_response(response: RithmicResponse) {
/// if let Some(err) = &response.error {
///     if err.is_connection_issue() {
///         // reconnect
///         return;
///     }
///     eprintln!("Request error from {}: {}", response.source, err);
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

    /// Typed error when the operation failed or a connection-level event
    /// occurred. Use [`RithmicError::is_connection_issue`] to distinguish
    /// transport failures (reconnect signal) from protocol-level request
    /// rejections.
    pub error: Option<RithmicError>,
    pub source: String,
}

impl RithmicResponse {
    /// Full raw rp_code payload as received. `None` for message variants that
    /// don't carry rp_code (updates, ConnectionError, HeartbeatTimeout, etc.).
    pub fn rp_code(&self) -> Option<&[String]> {
        super::rp_code::response_rp_code_slice(&self.message)
    }

    /// Numeric portion of rp_code, if present.
    pub fn rp_code_num(&self) -> Option<&str> {
        self.rp_code().and_then(|c| c.first().map(String::as_str))
    }

    /// Second element of rp_code (the human message), if present.
    pub fn rp_code_text(&self) -> Option<&str> {
        self.rp_code().and_then(|c| c.get(1).map(String::as_str))
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::rti::{
        AccountPnLPositionUpdate, BestBidOffer, BracketUpdates, DepthByOrder, DepthByOrderEndEvent,
        ExchangeOrderNotification, ForcedLogout, InstrumentPnLPositionUpdate, LastTrade, OrderBook,
        RithmicOrderNotification, messages::RithmicMessage,
    };

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
    // Mutual exclusivity tests - verify categories don't overlap unexpectedly
    // =========================================================================

    #[test]
    fn categories_are_mutually_exclusive() {
        // Market data should not be flagged as order update or pnl
        let market_data = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));

        assert!(market_data.is_market_data());
        assert!(!market_data.is_order_update());
        assert!(!market_data.is_pnl_update());

        // Order update should not be flagged as market data or pnl
        let order = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));

        assert!(order.is_order_update());
        assert!(!order.is_market_data());
        assert!(!order.is_pnl_update());

        // PnL should not be flagged as market data or order update
        let pnl = make_response(RithmicMessage::AccountPnLPositionUpdate(
            AccountPnLPositionUpdate::default(),
        ));

        assert!(pnl.is_pnl_update());
        assert!(!pnl.is_market_data());
        assert!(!pnl.is_order_update());

        // Connection-error message should not be in any content category
        let conn_err = make_response(RithmicMessage::ConnectionError);

        assert!(!conn_err.is_market_data());
        assert!(!conn_err.is_order_update());
        assert!(!conn_err.is_pnl_update());
    }

    // =========================================================================
    // error field typing tests
    // =========================================================================

    #[test]
    fn error_field_accepts_typed_rithmic_error() {
        let mut response = make_response(RithmicMessage::ConnectionError);
        response.error = Some(RithmicError::ConnectionClosed);

        match &response.error {
            Some(err) => {
                assert!(err.is_connection_issue());
                assert_eq!(err, &RithmicError::ConnectionClosed);
            }
            None => panic!("expected Some(RithmicError)"),
        }
    }

    #[test]
    fn error_field_forced_logout_is_connection_issue() {
        let mut response = make_response(RithmicMessage::ForcedLogout(ForcedLogout::default()));
        response.error = Some(RithmicError::ForcedLogout("server shutdown".into()));

        let err = response.error.as_ref().expect("error should be set");
        assert!(err.is_connection_issue());
    }

    #[test]
    fn error_field_protocol_error_is_not_connection_issue() {
        let mut response = make_response(RithmicMessage::ConnectionError);
        response.error = Some(RithmicError::ProtocolError("decode failed".into()));

        let err = response.error.as_ref().expect("error should be set");
        assert!(!err.is_connection_issue());
    }
}
