use super::rithmic_command_types::{
    LoginConfig, RithmicBracketOrder, RithmicOcoOrderLeg, RithmicOrder,
};
use prost::Message;

use crate::{
    config::{RithmicConfig, RithmicEnv},
    rti::{
        RequestAcceptAgreement, RequestAccountList, RequestAccountRmsInfo,
        RequestAccountRmsUpdates, RequestAuxilliaryReferenceData, RequestBracketOrder,
        RequestCancelAllOrders, RequestCancelOrder, RequestDepthByOrderSnapshot,
        RequestDepthByOrderUpdates, RequestEasyToBorrowList, RequestExitPosition,
        RequestFrontMonthContract, RequestGetInstrumentByUnderlying, RequestGetVolumeAtPrice,
        RequestGiveTickSizeTypeTable, RequestHeartbeat, RequestLinkOrders,
        RequestListAcceptedAgreements, RequestListExchangePermissions,
        RequestListUnacceptedAgreements, RequestLogin, RequestLoginInfo, RequestLogout,
        RequestMarketDataUpdate, RequestMarketDataUpdateByUnderlying, RequestModifyOrder,
        RequestModifyOrderReferenceData, RequestNewOrder, RequestOcoOrder,
        RequestOrderSessionConfig, RequestPnLPositionSnapshot, RequestPnLPositionUpdates,
        RequestProductCodes, RequestProductRmsInfo, RequestReferenceData, RequestReplayExecutions,
        RequestResumeBars, RequestRithmicSystemGatewayInfo, RequestRithmicSystemInfo,
        RequestSearchSymbols, RequestSetRithmicMrktDataSelfCertStatus, RequestShowAgreement,
        RequestShowBracketStops, RequestShowBrackets, RequestShowOrderHistory,
        RequestShowOrderHistoryDates, RequestShowOrderHistoryDetail,
        RequestShowOrderHistorySummary, RequestShowOrders, RequestSubscribeForOrderUpdates,
        RequestSubscribeToBracketUpdates, RequestTickBarReplay, RequestTickBarUpdate,
        RequestTimeBarReplay, RequestTimeBarUpdate, RequestTradeRoutes,
        RequestUpdateStopBracketLevel, RequestUpdateTargetBracketLevel,
        RequestVolumeProfileMinuteBars,
        request_account_list::UserType,
        request_bracket_order, request_cancel_all_orders, request_depth_by_order_updates,
        request_easy_to_borrow_list,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
        request_market_data_update_by_underlying, request_modify_order, request_oco_order,
        request_pn_l_position_updates, request_search_symbols,
        request_tick_bar_replay::{BarSubType, BarType, Direction, TimeOrder},
        request_tick_bar_update, request_time_bar_replay, request_time_bar_update,
    },
};

pub(crate) const TRADE_ROUTE_LIVE: &str = "globex";
pub(crate) const TRADE_ROUTE_DEMO: &str = "simulator";
pub(crate) const USER_TYPE: i32 = 3;

#[derive(Debug, Clone)]
pub(crate) struct RithmicSenderApi {
    account_id: String,
    app_name: String,
    app_version: String,
    env: RithmicEnv,
    fcm_id: String,
    ib_id: String,
    message_id_counter: u64,
}

impl RithmicSenderApi {
    pub(crate) fn new(config: &RithmicConfig) -> Self {
        RithmicSenderApi {
            account_id: config.account_id.clone(),
            app_name: config.app_name.clone(),
            app_version: config.app_version.clone(),
            env: config.env,
            fcm_id: config.fcm_id.clone(),
            ib_id: config.ib_id.clone(),
            message_id_counter: 0,
        }
    }

    fn get_next_message_id(&mut self) -> String {
        self.message_id_counter += 1;
        self.message_id_counter.to_string()
    }

    fn request_to_buf(&self, req: impl Message, id: String) -> (Vec<u8>, String) {
        let mut buf = Vec::new();
        let len = req.encoded_len() as u32;
        let header = len.to_be_bytes();

        buf.reserve((len + 4) as usize);
        req.encode(&mut buf).unwrap();
        buf.splice(0..0, header.iter().cloned());

        (buf, id)
    }

    pub fn request_rithmic_system_info(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestRithmicSystemInfo {
            template_id: 16,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_login(
        &mut self,
        system_name: &str,
        infra_type: SysInfraType,
        user: &str,
        password: &str,
        config: &LoginConfig,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestLogin {
            template_id: 10,
            template_version: Some("5.30".into()),
            user: Some(user.to_string()),
            password: Some(password.to_string()),
            app_name: Some(self.app_name.clone()),
            app_version: Some(self.app_version.clone()),
            system_name: Some(system_name.to_string()),
            infra_type: Some(infra_type.into()),
            user_msg: vec![id.clone()],
            aggregated_quotes: config.aggregated_quotes,
            mac_addr: config.mac_addr.clone().unwrap_or_default(),
            os_version: config.os_version.clone(),
            os_platform: config.os_platform.clone(),
        };

        self.request_to_buf(req, id)
    }

    pub fn request_logout(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestLogout {
            template_id: 12,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_heartbeat(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestHeartbeat {
            template_id: 18,
            user_msg: vec![id.clone()],
            ..RequestHeartbeat::default()
        };

        self.request_to_buf(req, id)
    }

    /// Request Rithmic system gateway information
    ///
    /// Returns gateway-specific information for a Rithmic system.
    ///
    /// # Arguments
    /// * `system_name` - Optional system name to get info for
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_rithmic_system_gateway_info(
        &mut self,
        system_name: Option<&str>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestRithmicSystemGatewayInfo {
            template_id: 20,
            user_msg: vec![id.clone()],
            system_name: system_name.map(|s| s.to_string()),
        };

        self.request_to_buf(req, id)
    }

    pub fn request_market_data_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        fields: Vec<UpdateBits>,
        request_type: Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let mut req = RequestMarketDataUpdate {
            template_id: 100,
            user_msg: vec![id.clone()],
            ..RequestMarketDataUpdate::default()
        };

        let mut bits = 0;

        for field in fields {
            bits |= field as u32;
        }

        req.symbol = Some(symbol.into());
        req.exchange = Some(exchange.into());
        req.request = Some(request_type.into());
        req.update_bits = Some(bits);

        self.request_to_buf(req, id)
    }

    /// Request instruments by underlying symbol
    ///
    /// Returns all instruments (options, futures) for a given underlying symbol.
    ///
    /// # Arguments
    /// * `underlying_symbol` - The underlying symbol (e.g., "ES" for E-mini S&P 500)
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `expiration_date` - Optional expiration date filter
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_get_instrument_by_underlying(
        &mut self,
        underlying_symbol: &str,
        exchange: &str,
        expiration_date: Option<&str>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestGetInstrumentByUnderlying {
            template_id: 102,
            user_msg: vec![id.clone()],
            underlying_symbol: Some(underlying_symbol.to_string()),
            exchange: Some(exchange.to_string()),
            expiration_date: expiration_date.map(|d| d.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Subscribe to or unsubscribe from market data updates by underlying
    ///
    /// Similar to request_market_data_update but subscribes to all instruments
    /// for a given underlying symbol.
    ///
    /// # Arguments
    /// * `underlying_symbol` - The underlying symbol (e.g., "ES")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `expiration_date` - Optional expiration date filter
    /// * `fields` - The market data fields to subscribe to
    /// * `request_type` - Subscribe or Unsubscribe
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_market_data_update_by_underlying(
        &mut self,
        underlying_symbol: &str,
        exchange: &str,
        expiration_date: Option<&str>,
        fields: Vec<request_market_data_update_by_underlying::UpdateBits>,
        request_type: request_market_data_update_by_underlying::Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let mut bits = 0;

        for field in fields {
            bits |= field as u32;
        }

        let req = RequestMarketDataUpdateByUnderlying {
            template_id: 105,
            user_msg: vec![id.clone()],
            underlying_symbol: Some(underlying_symbol.to_string()),
            exchange: Some(exchange.to_string()),
            expiration_date: expiration_date.map(|d| d.to_string()),
            request: Some(request_type.into()),
            update_bits: Some(bits),
        };

        self.request_to_buf(req, id)
    }

    /// Request tick size type table
    ///
    /// Returns the tick size table for a given tick size type.
    ///
    /// # Arguments
    /// * `tick_size_type` - The tick size type identifier
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_give_tick_size_type_table(&mut self, tick_size_type: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestGiveTickSizeTypeTable {
            template_id: 107,
            user_msg: vec![id.clone()],
            tick_size_type: Some(tick_size_type.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request product codes
    ///
    /// Returns available product codes for an exchange.
    ///
    /// # Arguments
    /// * `exchange` - Optional exchange filter (e.g., "CME")
    /// * `give_toi_products_only` - If true, only return Time of Interest products
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_product_codes(
        &mut self,
        exchange: Option<&str>,
        give_toi_products_only: Option<bool>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestProductCodes {
            template_id: 111,
            user_msg: vec![id.clone()],
            exchange: exchange.map(|e| e.to_string()),
            give_toi_products_only,
        };

        self.request_to_buf(req, id)
    }

    /// Request volume at price data
    ///
    /// Returns the volume profile (volume at each price level) for a symbol.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_get_volume_at_price(
        &mut self,
        symbol: &str,
        exchange: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestGetVolumeAtPrice {
            template_id: 119,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request auxiliary reference data
    ///
    /// Returns additional reference data for a symbol.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_auxilliary_reference_data(
        &mut self,
        symbol: &str,
        exchange: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAuxilliaryReferenceData {
            template_id: 121,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request login information for the current session
    ///
    /// Returns information about the current login session on the Order Plant.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_login_info(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestLoginInfo {
            template_id: 300,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_account_list(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAccountList {
            template_id: 302,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            user_type: Some(UserType::Trader.into()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_for_order_updates(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestSubscribeForOrderUpdates {
            template_id: 308,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_to_bracket_updates(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestSubscribeToBracketUpdates {
            template_id: 336,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Send a new order request using [`RithmicOrder`].
    ///
    /// This is the preferred method for placing orders as it supports
    /// advanced features like trigger prices and trailing stops.
    ///
    /// # Arguments
    /// * `order` - The order parameters
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_order(&mut self, order: &RithmicOrder) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let trade_route = match self.env {
            RithmicEnv::Live => TRADE_ROUTE_LIVE,
            RithmicEnv::Demo | RithmicEnv::Test => TRADE_ROUTE_DEMO,
        };

        let req = RequestNewOrder {
            template_id: 312,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(order.exchange.clone()),
            symbol: Some(order.symbol.clone()),
            quantity: Some(order.quantity),
            price: Some(order.price),
            transaction_type: Some(order.transaction_type.into()),
            price_type: Some(order.price_type.into()),
            manual_or_auto: Some(2),
            duration: order.duration.map(|d| d.into()).or(Some(1)),
            user_msg: vec![id.clone()],
            user_tag: Some(order.user_tag.clone()),
            trigger_price: order.trigger_price,
            trailing_stop: order.trailing_stop.as_ref().map(|_| true),
            trail_by_ticks: order.trailing_stop.as_ref().map(|ts| ts.trail_by_ticks),
            ..RequestNewOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_bracket_order(
        &mut self,
        bracket_order: RithmicBracketOrder,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let trade_route = match self.env {
            RithmicEnv::Live => TRADE_ROUTE_LIVE,
            RithmicEnv::Demo | RithmicEnv::Test => TRADE_ROUTE_DEMO, // NOTE: Not sure if this is correct value for test environment
        };

        let req = RequestBracketOrder {
            template_id: 330,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(bracket_order.exchange),
            symbol: Some(bracket_order.symbol),
            user_type: Some(USER_TYPE),
            quantity: Some(bracket_order.quantity),
            transaction_type: Some(bracket_order.action.into()),
            price_type: Some(bracket_order.price_type.into()),
            manual_or_auto: Some(2),
            duration: Some(bracket_order.duration.into()),
            bracket_type: Some(6),
            target_quantity: vec![bracket_order.quantity],
            stop_quantity: vec![bracket_order.quantity],
            target_ticks: vec![bracket_order.profit_ticks],
            stop_ticks: vec![bracket_order.stop_ticks],
            price: if bracket_order.price_type != request_bracket_order::PriceType::Market {
                bracket_order.price
            } else {
                None
            },
            user_msg: vec![id.clone()],
            user_tag: Some(bracket_order.localid),
            ..RequestBracketOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_modify_order(
        &mut self,
        basket_id: &str,
        exchange: &str,
        symbol: &str,
        qty: i32,
        price: f64,
        price_type: request_modify_order::PriceType,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestModifyOrder {
            template_id: 314,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            manual_or_auto: Some(2),
            exchange: Some(exchange.into()),
            symbol: Some(symbol.into()),
            price_type: Some(price_type.into()),
            quantity: Some(qty),
            price: Some(price),
            user_msg: vec![id.clone()],
            trigger_price: match price_type {
                request_modify_order::PriceType::StopLimit
                | request_modify_order::PriceType::StopMarket => Some(price),
                _ => None,
            },
            ..RequestModifyOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_cancel_order(&mut self, basket_id: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestCancelOrder {
            template_id: 316,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            manual_or_auto: Some(2),
            user_msg: vec![id.clone()],
            ..RequestCancelOrder::default()
        };

        self.request_to_buf(req, id)
    }

    /// Request to exit an entire position for a given symbol
    ///
    /// This will close all open positions for the specified symbol/exchange combination
    /// by placing a market order in the opposite direction.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_exit_position(&mut self, symbol: &str, exchange: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestExitPosition {
            template_id: 3504,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            manual_or_auto: Some(2),
            user_msg: vec![id.clone()],
            ..RequestExitPosition::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_update_target_bracket_level(
        &mut self,
        basket_id: &str,
        profit_ticks: i32,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestUpdateTargetBracketLevel {
            template_id: 332,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            target_ticks: Some(profit_ticks),
            user_msg: vec![id.clone()],
            ..RequestUpdateTargetBracketLevel::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_update_stop_bracket_level(
        &mut self,
        basket_id: &str,
        stop_ticks: i32,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestUpdateStopBracketLevel {
            template_id: 334,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            stop_ticks: Some(stop_ticks),
            user_msg: vec![id.clone()],
            ..RequestUpdateStopBracketLevel::default()
        };

        self.request_to_buf(req, id)
    }

    /// Request a list of all active bracket orders
    ///
    /// Returns information about all currently active bracket orders for the account,
    /// including entry orders with their associated profit targets and stop losses.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_brackets(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowBrackets {
            template_id: 338,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request a list of all active bracket stop orders
    ///
    /// Returns information specifically about the stop loss orders associated with
    /// bracket orders. This is useful for monitoring risk management on active positions.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_bracket_stops(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowBracketStops {
            template_id: 340,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_show_orders(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrders {
            template_id: 320,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_updates(
        &mut self,
        action: request_pn_l_position_updates::Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestPnLPositionUpdates {
            template_id: 400,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            request: Some(action.into()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_snapshot(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestPnLPositionSnapshot {
            template_id: 402,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request a replay of tick bar data
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol to request data for
    /// * `exchange` - The exchange of the symbol
    /// * `start_index_sec` - unix seconds
    /// * `finish_index_sec` - unix seconds
    ///
    /// # Returns
    ///
    /// A tuple containing the request buffer and the message id
    ///
    /// # Note
    ///
    /// Large data requests may be truncated by the server. If the response contains
    /// a round number of bars (e.g., 10000) or does not cover the entire requested
    /// time period, use [`request_resume_bars`](Self::request_resume_bars) with the
    /// `request_key` from the response to fetch the remaining data.
    pub fn request_tick_bar_replay(
        &mut self,
        symbol: &str,
        exchange: &str,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTickBarReplay {
            template_id: 206,
            exchange: Some(exchange.to_string()),
            symbol: Some(symbol.to_string()),
            bar_type: Some(BarType::TickBar.into()),
            bar_sub_type: Some(BarSubType::Regular.into()),
            bar_type_specifier: Some("1".to_string()),
            start_index: Some(start_index_sec),
            finish_index: Some(finish_index_sec),
            direction: Some(Direction::First.into()),
            time_order: Some(TimeOrder::Forwards.into()),
            user_msg: vec![id.clone()],
            ..Default::default()
        };

        self.request_to_buf(req, id)
    }

    /// Request a replay of time bar data
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol to request data for
    /// * `exchange` - The exchange of the symbol
    /// * `bar_type` - The type of time bar (SecondBar, MinuteBar, DailyBar, WeeklyBar)
    /// * `bar_type_period` - The period for the bar type (e.g., 1 for 1-minute bars, 5 for 5-minute bars)
    /// * `start_index_sec` - unix seconds
    /// * `finish_index_sec` - unix seconds
    ///
    /// # Returns
    ///
    /// A tuple containing the request buffer and the message id
    ///
    /// # Note
    ///
    /// Large data requests may be truncated by the server. If the response contains
    /// a round number of bars (e.g., 10000) or does not cover the entire requested
    /// time period, use [`request_resume_bars`](Self::request_resume_bars) with the
    /// `request_key` from the response to fetch the remaining data.
    pub fn request_time_bar_replay(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_time_bar_replay::BarType,
        bar_type_period: i32,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTimeBarReplay {
            template_id: 202,
            exchange: Some(exchange.to_string()),
            symbol: Some(symbol.to_string()),
            bar_type: Some(bar_type.into()),
            bar_type_period: Some(bar_type_period),
            start_index: Some(start_index_sec),
            finish_index: Some(finish_index_sec),
            direction: Some(request_time_bar_replay::Direction::First.into()),
            time_order: Some(request_time_bar_replay::TimeOrder::Forwards.into()),
            user_msg: vec![id.clone()],
            ..Default::default()
        };

        self.request_to_buf(req, id)
    }

    /// Request volume profile minute bars
    ///
    /// Returns minute bar data with volume profile information.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type_period` - The period for the bars
    /// * `start_index_sec` - Start time in unix seconds
    /// * `finish_index_sec` - End time in unix seconds
    /// * `user_max_count` - Optional maximum number of bars to return
    /// * `resume_bars` - Whether to resume from a previous request
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    ///
    /// # Note
    ///
    /// Large data requests may be truncated by the server. If the response contains
    /// a round number of bars (e.g., 10000) or does not cover the entire requested
    /// time period, use [`request_resume_bars`](Self::request_resume_bars) with the
    /// `request_key` from the response to fetch the remaining data.
    #[allow(clippy::too_many_arguments)]
    pub fn request_volume_profile_minute_bars(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type_period: i32,
        start_index_sec: i32,
        finish_index_sec: i32,
        user_max_count: Option<i32>,
        resume_bars: Option<bool>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestVolumeProfileMinuteBars {
            template_id: 208,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            bar_type_period: Some(bar_type_period),
            start_index: Some(start_index_sec),
            finish_index: Some(finish_index_sec),
            user_max_count,
            resume_bars,
        };

        self.request_to_buf(req, id)
    }

    /// Request to resume a previously truncated bars request
    ///
    /// Use this when a bars request was truncated due to data limits.
    /// Pass the request_key from the previous response.
    ///
    /// # Arguments
    /// * `request_key` - The request key from the previous truncated response
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_resume_bars(&mut self, request_key: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestResumeBars {
            template_id: 210,
            user_msg: vec![id.clone()],
            request_key: Some(request_key.to_string()),
        };

        self.request_to_buf(req, id)
    }

    pub fn request_depth_by_order_snapshot(
        &mut self,
        symbol: &str,
        exchange: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestDepthByOrderSnapshot {
            template_id: 115,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            depth_price: None,
        };

        self.request_to_buf(req, id)
    }

    pub fn request_depth_by_order_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        request_type: request_depth_by_order_updates::Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestDepthByOrderUpdates {
            template_id: 117,
            user_msg: vec![id.clone()],
            request: Some(request_type.into()),
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            depth_price: None,
        };

        self.request_to_buf(req, id)
    }

    /// Request to cancel all orders for the account
    ///
    /// This will cancel all active orders across all symbols and exchanges for the account.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_cancel_all_orders(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestCancelAllOrders {
            template_id: 346,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_type: Some(USER_TYPE),
            manual_or_auto: Some(request_cancel_all_orders::OrderPlacement::Manual.into()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request account RMS (Risk Management System) information
    ///
    /// Returns risk management limits and settings for the account.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_account_rms_info(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAccountRmsInfo {
            template_id: 304,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            user_type: Some(USER_TYPE),
        };

        self.request_to_buf(req, id)
    }

    /// Request product RMS (Risk Management System) information
    ///
    /// Returns risk management limits for specific products/symbols.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_product_rms_info(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestProductRmsInfo {
            template_id: 306,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
        };

        self.request_to_buf(req, id)
    }

    /// Request list of available trade routes
    ///
    /// Returns the trade routes configured for the user's account.
    ///
    /// # Arguments
    /// * `subscribe_for_updates` - Whether to receive updates when routes change
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_trade_routes(&mut self, subscribe_for_updates: bool) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTradeRoutes {
            template_id: 310,
            user_msg: vec![id.clone()],
            subscribe_for_updates: Some(subscribe_for_updates),
        };

        self.request_to_buf(req, id)
    }

    /// Request to search for symbols matching a pattern
    ///
    /// # Arguments
    /// * `search_text` - Search query string
    /// * `exchange` - Optional exchange filter (e.g., "CME", "COMEX")
    /// * `product_code` - Optional product code filter (e.g., "ES", "SI")
    /// * `instrument_type` - Optional instrument type filter
    /// * `pattern` - Search pattern type (EQUALS or CONTAINS)
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_search_symbols(
        &mut self,
        search_text: &str,
        exchange: Option<&str>,
        product_code: Option<&str>,
        instrument_type: Option<request_search_symbols::InstrumentType>,
        pattern: Option<request_search_symbols::Pattern>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestSearchSymbols {
            template_id: 109,
            user_msg: vec![id.clone()],
            search_text: Some(search_text.to_string()),
            exchange: exchange.map(|e| e.to_string()),
            product_code: product_code.map(|p| p.to_string()),
            instrument_type: instrument_type.map(|i| i.into()),
            pattern: pattern.map(|p| p.into()),
        };

        self.request_to_buf(req, id)
    }

    /// Request list of exchanges available to the user
    ///
    /// Returns the exchanges the user has permission to trade on.
    ///
    /// # Arguments
    /// * `user` - Username for authentication
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_list_exchanges(&mut self, user: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestListExchangePermissions {
            template_id: 342,
            user_msg: vec![id.clone()],
            user: Some(user.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request order history dates
    ///
    /// Returns the dates for which order history is available.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history_dates(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistoryDates {
            template_id: 318,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request order history summary for a specific date
    ///
    /// # Arguments
    /// * `date` - Date in YYYYMMDD format (e.g., "20250122")
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history_summary(&mut self, date: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistorySummary {
            template_id: 324,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            date: Some(date.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request detailed order history for a specific order
    ///
    /// # Arguments
    /// * `basket_id` - Order/basket identifier
    /// * `date` - Date in YYYYMMDD format
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history_detail(
        &mut self,
        basket_id: &str,
        date: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistoryDetail {
            template_id: 326,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.to_string()),
            date: Some(date.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request general order history
    ///
    /// # Arguments
    /// * `basket_id` - Optional order/basket identifier filter
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history(&mut self, basket_id: Option<&str>) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistory {
            template_id: 322,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: basket_id.map(|b| b.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request reference data for a symbol
    ///
    /// Returns detailed information about a trading instrument including
    /// tick size, point value, trading hours, and other symbol specifications.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_reference_data(&mut self, symbol: &str, exchange: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestReferenceData {
            template_id: 14,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request front month contract information
    ///
    /// Returns the current front month contract for a given product.
    /// Optionally subscribe to updates when the front month rolls.
    ///
    /// # Arguments
    /// * `symbol` - The product symbol (e.g., "ES" for E-mini S&P 500)
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `need_updates` - Whether to receive updates when front month changes
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_front_month_contract(
        &mut self,
        symbol: &str,
        exchange: &str,
        need_updates: bool,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestFrontMonthContract {
            template_id: 113,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            need_updates: Some(need_updates),
        };

        self.request_to_buf(req, id)
    }

    /// Subscribe to or unsubscribe from live time bar updates
    ///
    /// Receive real-time time bar (OHLCV) updates for a symbol.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type` - The type of time bar (SecondBar, MinuteBar, DailyBar, WeeklyBar)
    /// * `bar_type_period` - The period for the bar type (e.g., 1 for 1-minute bars)
    /// * `request` - Subscribe or Unsubscribe
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_time_bar_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_time_bar_update::BarType,
        bar_type_period: i32,
        request: request_time_bar_update::Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTimeBarUpdate {
            template_id: 200,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            bar_type: Some(bar_type.into()),
            bar_type_period: Some(bar_type_period),
            request: Some(request.into()),
        };

        self.request_to_buf(req, id)
    }

    /// Subscribe to or unsubscribe from live tick bar updates
    ///
    /// Receive real-time tick bar updates for a symbol.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESH6")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type` - The type of tick bar
    /// * `bar_sub_type` - Sub-type of the bar
    /// * `bar_type_specifier` - Specifier for the bar (e.g., "1" for 1-tick bars)
    /// * `request` - Subscribe or Unsubscribe
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_tick_bar_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_tick_bar_update::BarType,
        bar_sub_type: request_tick_bar_update::BarSubType,
        bar_type_specifier: &str,
        request: request_tick_bar_update::Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTickBarUpdate {
            template_id: 204,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            bar_type: Some(bar_type.into()),
            bar_sub_type: Some(bar_sub_type.into()),
            bar_type_specifier: Some(bar_type_specifier.to_string()),
            request: Some(request.into()),
            ..Default::default()
        };

        self.request_to_buf(req, id)
    }

    /// Subscribe to account RMS (Risk Management System) updates
    ///
    /// Receive real-time updates when account RMS limits change.
    ///
    /// # Arguments
    /// * `subscribe` - true to subscribe, false to unsubscribe
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_account_rms_updates(&mut self, subscribe: bool) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAccountRmsUpdates {
            template_id: 3508,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            request: Some(
                if subscribe {
                    "subscribe"
                } else {
                    "unsubscribe"
                }
                .to_string(),
            ),
            update_bits: None,
        };

        self.request_to_buf(req, id)
    }

    /// Request an OCO (One Cancels Other) order pair
    ///
    /// Places two orders where if one is filled, the other is automatically cancelled.
    /// This is commonly used for bracket-style trading (profit target and stop loss).
    ///
    /// # Arguments
    /// * `order1` - First order leg
    /// * `order2` - Second order leg
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_oco_order(
        &mut self,
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let trade_route = match self.env {
            RithmicEnv::Live => TRADE_ROUTE_LIVE,
            RithmicEnv::Demo | RithmicEnv::Test => TRADE_ROUTE_DEMO,
        };

        let req = RequestOcoOrder {
            template_id: 328,
            user_msg: vec![id.clone()],
            user_tag: vec![order1.user_tag, order2.user_tag],
            window_name: vec![],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            symbol: vec![order1.symbol, order2.symbol],
            exchange: vec![order1.exchange, order2.exchange],
            quantity: vec![order1.quantity, order2.quantity],
            price: vec![order1.price, order2.price],
            trigger_price: vec![
                order1.trigger_price.unwrap_or(0.0),
                order2.trigger_price.unwrap_or(0.0),
            ],
            transaction_type: vec![
                order1.transaction_type.into(),
                order2.transaction_type.into(),
            ],
            duration: vec![order1.duration.into(), order2.duration.into()],
            price_type: vec![order1.price_type.into(), order2.price_type.into()],
            trade_route: vec![trade_route.to_string(), trade_route.to_string()],
            manual_or_auto: vec![
                request_oco_order::OrderPlacement::Auto.into(),
                request_oco_order::OrderPlacement::Auto.into(),
            ],
            trailing_stop: vec![],
            trail_by_ticks: vec![],
            trail_by_price_id: vec![],
            cancel_at_ssboe: None,
            cancel_at_usecs: None,
            cancel_after_secs: None,
        };

        self.request_to_buf(req, id)
    }

    /// Request to link multiple orders together
    ///
    /// Links orders so they are managed as a group. When one order is cancelled,
    /// all linked orders are cancelled.
    ///
    /// # Arguments
    /// * `basket_ids` - Vector of basket IDs to link together
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_link_orders(&mut self, basket_ids: Vec<String>) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let count = basket_ids.len();

        let req = RequestLinkOrders {
            template_id: 344,
            user_msg: vec![id.clone()],
            fcm_id: vec![self.fcm_id.clone(); count],
            ib_id: vec![self.ib_id.clone(); count],
            account_id: vec![self.account_id.clone(); count],
            basket_id: basket_ids,
        };

        self.request_to_buf(req, id)
    }

    /// Request the easy-to-borrow list for short selling
    ///
    /// Returns a list of securities that are readily available for short selling.
    ///
    /// # Arguments
    /// * `request_type` - Subscribe or Unsubscribe from updates
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_easy_to_borrow_list(
        &mut self,
        request_type: request_easy_to_borrow_list::Request,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestEasyToBorrowList {
            template_id: 348,
            user_msg: vec![id.clone()],
            request: Some(request_type.into()),
        };

        self.request_to_buf(req, id)
    }

    /// Modify order reference data (user tag)
    ///
    /// Updates the user-defined reference data on an existing order.
    ///
    /// # Arguments
    /// * `basket_id` - The order/basket identifier
    /// * `user_tag` - New user tag to set on the order
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_modify_order_reference_data(
        &mut self,
        basket_id: &str,
        user_tag: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestModifyOrderReferenceData {
            template_id: 3500,
            user_msg: vec![id.clone()],
            user_tag: Some(user_tag.to_string()),
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request order session configuration
    ///
    /// Gets or sets order session configuration options.
    ///
    /// # Arguments
    /// * `should_defer_request` - If true, defers requests until server loads reference data
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_order_session_config(
        &mut self,
        should_defer_request: Option<bool>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestOrderSessionConfig {
            template_id: 3502,
            user_msg: vec![id.clone()],
            should_defer_request,
        };

        self.request_to_buf(req, id)
    }

    /// Request replay of executions
    ///
    /// Replays historical execution data for the account within a time range.
    ///
    /// # Arguments
    /// * `start_index_sec` - Start time in unix seconds
    /// * `finish_index_sec` - End time in unix seconds
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_replay_executions(
        &mut self,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestReplayExecutions {
            template_id: 3506,
            user_msg: vec![id.clone()],
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            start_index: Some(start_index_sec),
            finish_index: Some(finish_index_sec),
        };

        self.request_to_buf(req, id)
    }

    /// Request list of unaccepted agreements
    ///
    /// Returns agreements that the user has not yet accepted.
    /// These may include market data agreements, exchange agreements, etc.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_list_unaccepted_agreements(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestListUnacceptedAgreements {
            template_id: 500,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request list of accepted agreements
    ///
    /// Returns agreements that the user has already accepted.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_list_accepted_agreements(&mut self) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestListAcceptedAgreements {
            template_id: 502,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Accept an agreement
    ///
    /// Accepts a specific agreement identified by agreement_id.
    ///
    /// # Arguments
    /// * `agreement_id` - The agreement identifier
    /// * `market_data_usage_capacity` - "Professional" or "Non-Professional"
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_accept_agreement(
        &mut self,
        agreement_id: &str,
        market_data_usage_capacity: Option<&str>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAcceptAgreement {
            template_id: 504,
            user_msg: vec![id.clone()],
            agreement_id: Some(agreement_id.to_string()),
            market_data_usage_capacity: market_data_usage_capacity.map(|s| s.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request to show agreement details
    ///
    /// Returns the full text and details of a specific agreement.
    ///
    /// # Arguments
    /// * `agreement_id` - The agreement identifier
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_agreement(&mut self, agreement_id: &str) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowAgreement {
            template_id: 506,
            user_msg: vec![id.clone()],
            agreement_id: Some(agreement_id.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Set Rithmic market data self-certification status
    ///
    /// Sets the user's self-certification status for market data usage
    /// (Professional vs Non-Professional).
    ///
    /// # Arguments
    /// * `agreement_id` - The agreement identifier
    /// * `market_data_usage_capacity` - "Professional" or "Non-Professional"
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_set_rithmic_mrkt_data_self_cert_status(
        &mut self,
        agreement_id: &str,
        market_data_usage_capacity: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestSetRithmicMrktDataSelfCertStatus {
            template_id: 508,
            user_msg: vec![id.clone()],
            agreement_id: Some(agreement_id.to_string()),
            market_data_usage_capacity: Some(market_data_usage_capacity.to_string()),
        };

        self.request_to_buf(req, id)
    }
}
