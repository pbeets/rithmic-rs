use crate::{error::RithmicError, rti::messages::RithmicMessage};

/// One message from a Rithmic plant: a reply to a request, or an update on a
/// plant's subscription channel.
///
/// # Telling frames apart
///
/// - **Error**: `error` is `Some`. [`RithmicError::is_connection_issue`] is
///   true for a transport failure, which means reconnect. It is false for a
///   rejected request or a frame that would not decode.
/// - **Update**: `is_update` is true. Everything on the subscription channel
///   is an update, including the connection events `ConnectionError`,
///   `HeartbeatTimeout` and `ForcedLogout`.
/// - **Reply**: `is_update` is false and `request_id` names the request. A
///   list or replay request can answer in several frames (`multi_response`);
///   every frame but the last has `has_more` set.
///
/// Most handle methods return `Ok` even when the server rejects the request,
/// so check `error` on the reply too. See [`RithmicError`] for which calls
/// return a rejection as `Err` instead.
///
/// # Examples
///
/// Reading a subscription channel:
///
/// ```no_run
/// # use rithmic_rs::RithmicResponse;
/// # use tokio::sync::broadcast;
/// # async fn run(mut updates: broadcast::Receiver<RithmicResponse>) {
/// while let Ok(resp) = updates.recv().await {
///     if let Some(err) = &resp.error {
///         if err.is_connection_issue() {
///             break; // reconnect
///         }
///
///         eprintln!("{}: {err}", resp.source);
///         continue;
///     }
///
///     if resp.is_market_data() {
///         // quotes, trades, depth
///     } else if resp.is_order_update() {
///         // order status
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RithmicResponse {
    /// Id of the request this frame answers, echoed back by the server. Empty
    /// on updates the server pushes. A connection event may carry the id of
    /// the request whose failure raised it.
    pub request_id: String,
    /// The decoded message. Match on it to get the payload.
    pub message: RithmicMessage,
    /// `true` for frames sent to the subscription channel: streaming data,
    /// server notices and connection events. `false` for replies to a request.
    pub is_update: bool,
    /// `true` on every frame of a multi-frame reply except the last.
    pub has_more: bool,
    /// `true` if this frame belongs to a reply that can span several frames,
    /// such as a symbol search or a bar replay.
    pub multi_response: bool,

    /// Why the request or connection failed, if it did.
    ///
    /// Holds [`RequestRejected`](RithmicError::RequestRejected) when the
    /// server said no, [`ProtocolError`](RithmicError::ProtocolError) when the
    /// frame would not decode, and a connection error on connection events.
    /// An `rp_code` of `["7", "no data"]` counts as an empty result, not an
    /// error.
    pub error: Option<RithmicError>,
    /// Plant that produced the frame: `"ticker_plant"`, `"order_plant"`,
    /// `"history_plant"` or `"pnl_plant"`.
    pub source: String,
}

impl RithmicResponse {
    /// The `rp_code` exactly as the server sent it.
    ///
    /// `None` for messages with no `rp_code` field, such as streaming updates
    /// and connection events. Data frames of a multi-frame reply normally give
    /// an empty slice, with the code on the last frame.
    pub fn rp_code(&self) -> Option<&[String]> {
        super::rp_code::response_rp_code_slice(&self.message)
    }

    /// First element of `rp_code`: `"0"` on success, else the error code.
    pub fn rp_code_num(&self) -> Option<&str> {
        self.rp_code().and_then(|c| c.first().map(String::as_str))
    }

    /// Second element of `rp_code`: the server's text, if it sent any.
    pub fn rp_code_text(&self) -> Option<&str> {
        self.rp_code().and_then(|c| c.get(1).map(String::as_str))
    }

    /// The key for continuing a replay the server cut short.
    ///
    /// Only the server's truncation notice carries one. The `load_*` methods
    /// use those notices to continue the replay and leave them out of the
    /// result, so frames they return always give `None`. Kept for
    /// compatibility.
    pub fn resume_key(&self) -> Option<&str> {
        let key = match &self.message {
            RithmicMessage::ResponseTickBarReplay(m) => m.request_key.as_deref(),
            RithmicMessage::ResponseTimeBarReplay(m) => m.request_key.as_deref(),
            RithmicMessage::ResponseVolumeProfileMinuteBars(m) => m.request_key.as_deref(),
            _ => None,
        };

        key.filter(|k| !k.is_empty())
    }

    /// `true` if this frame is the server's notice that it cut a replay short:
    /// a [`resume_key`](Self::resume_key), no response code and no data.
    pub(crate) fn is_truncated(&self) -> bool {
        self.multi_response
            && !self.has_more
            && self.rp_code().is_none_or(|code| code.is_empty())
            && self.resume_key().is_some()
    }

    /// `true` for `BestBidOffer`, `LastTrade`, `DepthByOrder`,
    /// `DepthByOrderEndEvent` and `OrderBook`.
    ///
    /// Other ticker plant updates, such as `TradeStatistics` or `MarketMode`,
    /// give `false`.
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

    /// `true` for `RithmicOrderNotification`, `ExchangeOrderNotification` and
    /// `BracketUpdates`.
    pub fn is_order_update(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::RithmicOrderNotification(_)
                | RithmicMessage::ExchangeOrderNotification(_)
                | RithmicMessage::BracketUpdates(_)
        )
    }

    /// `true` for `AccountPnLPositionUpdate` and `InstrumentPnLPositionUpdate`.
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
        ExchangeOrderNotification, InstrumentPnLPositionUpdate, LastTrade, OrderBook,
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
}
