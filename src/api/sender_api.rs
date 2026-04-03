use super::rithmic_command_types::{
    LoginConfig, RithmicAccount, RithmicAdvancedBracketOrder, RithmicBracketOrder,
    RithmicOcoOrderLeg, RithmicOrder,
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
        request_cancel_all_orders, request_depth_by_order_updates, request_easy_to_borrow_list,
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

struct ModifyOrderRequest<'a> {
    basket_id: &'a str,
    exchange: &'a str,
    symbol: &'a str,
    qty: i32,
    price: f64,
    price_type: request_modify_order::PriceType,
}

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

        req.encode(&mut buf)
            .expect("prost encoding into a Vec<u8> is infallible");

        buf.splice(0..0, header.iter().cloned());

        (buf, id)
    }

    fn account_fields(&self, account: Option<&RithmicAccount>) -> (String, String, String) {
        match account {
            Some(account) => (
                account.fcm_id.clone(),
                account.ib_id.clone(),
                account.account_id.clone(),
            ),
            None => (
                self.fcm_id.clone(),
                self.ib_id.clone(),
                self.account_id.clone(),
            ),
        }
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
        self.request_subscribe_for_order_updates_with_account(None)
    }

    pub fn request_subscribe_for_order_updates_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_subscribe_for_order_updates_with_account(Some(account))
    }

    fn request_subscribe_for_order_updates_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestSubscribeForOrderUpdates {
            template_id: 308,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_to_bracket_updates(&mut self) -> (Vec<u8>, String) {
        self.request_subscribe_to_bracket_updates_with_account(None)
    }

    pub fn request_subscribe_to_bracket_updates_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_subscribe_to_bracket_updates_with_account(Some(account))
    }

    fn request_subscribe_to_bracket_updates_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestSubscribeToBracketUpdates {
            template_id: 336,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_order_with_account(order, None)
    }

    pub fn request_order_for_account(
        &mut self,
        order: &RithmicOrder,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_order_with_account(order, Some(account))
    }

    fn request_order_with_account(
        &mut self,
        order: &RithmicOrder,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let trade_route = match self.env {
            RithmicEnv::Live => TRADE_ROUTE_LIVE,
            RithmicEnv::Demo | RithmicEnv::Test => TRADE_ROUTE_DEMO,
        };

        let req = RequestNewOrder {
            template_id: 312,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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

    pub fn request_advanced_bracket_order(
        &mut self,
        bracket_order: RithmicAdvancedBracketOrder,
    ) -> (Vec<u8>, String) {
        self.request_advanced_bracket_order_with_account(bracket_order, None)
    }

    pub fn request_advanced_bracket_order_for_account(
        &mut self,
        bracket_order: RithmicAdvancedBracketOrder,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_advanced_bracket_order_with_account(bracket_order, Some(account))
    }

    pub fn request_bracket_order(
        &mut self,
        bracket_order: RithmicBracketOrder,
    ) -> (Vec<u8>, String) {
        self.request_advanced_bracket_order(bracket_order.into())
    }

    pub fn request_bracket_order_for_account(
        &mut self,
        bracket_order: RithmicBracketOrder,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_advanced_bracket_order_for_account(bracket_order.into(), account)
    }

    fn request_advanced_bracket_order_with_account(
        &mut self,
        bracket_order: RithmicAdvancedBracketOrder,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let trade_route = match self.env {
            RithmicEnv::Live => TRADE_ROUTE_LIVE,
            RithmicEnv::Demo | RithmicEnv::Test => TRADE_ROUTE_DEMO, // NOTE: Not sure if this is correct value for test environment
        };

        let req = RequestBracketOrder {
            template_id: 330,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            trade_route: Some(trade_route.into()),
            exchange: Some(bracket_order.exchange),
            symbol: Some(bracket_order.symbol),
            user_type: Some(USER_TYPE),
            quantity: Some(bracket_order.quantity),
            transaction_type: Some(bracket_order.action.into()),
            price_type: Some(bracket_order.price_type.into()),
            manual_or_auto: Some(2),
            duration: Some(bracket_order.duration.into()),
            bracket_type: Some(bracket_order.bracket_type.into()),
            break_even_ticks: bracket_order.break_even_ticks,
            break_even_trigger_ticks: bracket_order.break_even_trigger_ticks,
            target_quantity: bracket_order.target_quantity,
            target_ticks: bracket_order.target_ticks,
            stop_quantity: bracket_order.stop_quantity,
            stop_ticks: bracket_order.stop_ticks,
            trailing_stop_trigger_ticks: bracket_order.trailing_stop_trigger_ticks,
            trailing_stop_by_last_trade_price: bracket_order.trailing_stop_by_last_trade_price,
            target_market_order_if_touched: bracket_order.target_market_order_if_touched,
            stop_market_on_reject: bracket_order.stop_market_on_reject,
            target_market_at_ssboe: bracket_order.target_market_at_ssboe,
            target_market_at_usecs: bracket_order.target_market_at_usecs,
            stop_market_at_ssboe: bracket_order.stop_market_at_ssboe,
            stop_market_at_usecs: bracket_order.stop_market_at_usecs,
            target_market_order_after_secs: bracket_order.target_market_order_after_secs,
            release_at_ssboe: bracket_order.release_at_ssboe,
            release_at_usecs: bracket_order.release_at_usecs,
            cancel_at_ssboe: bracket_order.cancel_at_ssboe,
            cancel_at_usecs: bracket_order.cancel_at_usecs,
            cancel_after_secs: bracket_order.cancel_after_secs,
            if_touched_symbol: bracket_order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.symbol.clone()),
            if_touched_exchange: bracket_order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.exchange.clone()),
            if_touched_condition: bracket_order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.condition.into()),
            if_touched_price_field: bracket_order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.price_field.into()),
            if_touched_price: bracket_order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.price),
            price: bracket_order.price,
            trigger_price: bracket_order.trigger_price,
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
        let request = ModifyOrderRequest {
            basket_id,
            exchange,
            symbol,
            qty,
            price,
            price_type,
        };
        self.request_modify_order_with_account(request, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn request_modify_order_for_account(
        &mut self,
        basket_id: &str,
        exchange: &str,
        symbol: &str,
        qty: i32,
        price: f64,
        price_type: request_modify_order::PriceType,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let request = ModifyOrderRequest {
            basket_id,
            exchange,
            symbol,
            qty,
            price,
            price_type,
        };
        self.request_modify_order_with_account(request, Some(account))
    }

    fn request_modify_order_with_account(
        &mut self,
        request: ModifyOrderRequest<'_>,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestModifyOrder {
            template_id: 314,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            basket_id: Some(request.basket_id.into()),
            manual_or_auto: Some(2),
            exchange: Some(request.exchange.into()),
            symbol: Some(request.symbol.into()),
            price_type: Some(request.price_type.into()),
            quantity: Some(request.qty),
            price: Some(request.price),
            user_msg: vec![id.clone()],
            trigger_price: match request.price_type {
                request_modify_order::PriceType::StopLimit
                | request_modify_order::PriceType::StopMarket => Some(request.price),
                _ => None,
            },
            ..RequestModifyOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_cancel_order(&mut self, basket_id: &str) -> (Vec<u8>, String) {
        self.request_cancel_order_with_account(basket_id, None)
    }

    pub fn request_cancel_order_for_account(
        &mut self,
        basket_id: &str,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_cancel_order_with_account(basket_id, Some(account))
    }

    fn request_cancel_order_with_account(
        &mut self,
        basket_id: &str,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestCancelOrder {
            template_id: 316,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_exit_position_with_account(symbol, exchange, None)
    }

    pub fn request_exit_position_for_account(
        &mut self,
        symbol: &str,
        exchange: &str,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_exit_position_with_account(symbol, exchange, Some(account))
    }

    fn request_exit_position_with_account(
        &mut self,
        symbol: &str,
        exchange: &str,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestExitPosition {
            template_id: 3504,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_update_target_bracket_level_with_account(basket_id, profit_ticks, None)
    }

    pub fn request_update_target_bracket_level_for_account(
        &mut self,
        basket_id: &str,
        profit_ticks: i32,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_update_target_bracket_level_with_account(
            basket_id,
            profit_ticks,
            Some(account),
        )
    }

    fn request_update_target_bracket_level_with_account(
        &mut self,
        basket_id: &str,
        profit_ticks: i32,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestUpdateTargetBracketLevel {
            template_id: 332,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_update_stop_bracket_level_with_account(basket_id, stop_ticks, None)
    }

    pub fn request_update_stop_bracket_level_for_account(
        &mut self,
        basket_id: &str,
        stop_ticks: i32,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_update_stop_bracket_level_with_account(basket_id, stop_ticks, Some(account))
    }

    fn request_update_stop_bracket_level_with_account(
        &mut self,
        basket_id: &str,
        stop_ticks: i32,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestUpdateStopBracketLevel {
            template_id: 334,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_show_brackets_with_account(None)
    }

    pub fn request_show_brackets_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_show_brackets_with_account(Some(account))
    }

    fn request_show_brackets_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestShowBrackets {
            template_id: 338,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_show_bracket_stops_with_account(None)
    }

    pub fn request_show_bracket_stops_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_show_bracket_stops_with_account(Some(account))
    }

    fn request_show_bracket_stops_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestShowBracketStops {
            template_id: 340,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_show_orders(&mut self) -> (Vec<u8>, String) {
        self.request_show_orders_with_account(None)
    }

    pub fn request_show_orders_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_show_orders_with_account(Some(account))
    }

    fn request_show_orders_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestShowOrders {
            template_id: 320,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_updates(
        &mut self,
        action: request_pn_l_position_updates::Request,
    ) -> (Vec<u8>, String) {
        self.request_pnl_position_updates_with_account(action, None)
    }

    pub fn request_pnl_position_updates_for_account(
        &mut self,
        action: request_pn_l_position_updates::Request,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_pnl_position_updates_with_account(action, Some(account))
    }

    fn request_pnl_position_updates_with_account(
        &mut self,
        action: request_pn_l_position_updates::Request,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestPnLPositionUpdates {
            template_id: 400,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            request: Some(action.into()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_snapshot(&mut self) -> (Vec<u8>, String) {
        self.request_pnl_position_snapshot_with_account(None)
    }

    pub fn request_pnl_position_snapshot_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_pnl_position_snapshot_with_account(Some(account))
    }

    fn request_pnl_position_snapshot_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestPnLPositionSnapshot {
            template_id: 402,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request a replay of tick bar data.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol to request data for
    /// * `exchange` - The exchange of the symbol
    /// * `bar_type_specifier` - Number of ticks per bar as a string (e.g., `"1"` for
    ///   individual ticks, `"5"` for 5-tick bars)
    /// * `start_index_sec` - Start time as a Unix timestamp (seconds)
    /// * `finish_index_sec` - End time as a Unix timestamp (seconds)
    ///
    /// # Returns
    ///
    /// A tuple containing the request buffer and the message id.
    ///
    /// # Note
    ///
    /// Large data requests may be truncated by the server. If the response contains
    /// a round number of bars (e.g., 10 000) or does not cover the entire requested
    /// time period, use [`request_resume_bars`](Self::request_resume_bars) with the
    /// `request_key` from the response to fetch the remaining data.
    pub fn request_tick_bar_replay(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type_specifier: &str,
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
            bar_type_specifier: Some(bar_type_specifier.to_string()),
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
        self.request_cancel_all_orders_with_account(None)
    }

    pub fn request_cancel_all_orders_for_account(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_cancel_all_orders_with_account(Some(account))
    }

    fn request_cancel_all_orders_with_account(
        &mut self,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestCancelAllOrders {
            template_id: 346,
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_account_rms_updates_with_account(subscribe, None)
    }

    pub fn request_account_rms_updates_for_account(
        &mut self,
        subscribe: bool,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_account_rms_updates_with_account(subscribe, Some(account))
    }

    fn request_account_rms_updates_with_account(
        &mut self,
        subscribe: bool,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestAccountRmsUpdates {
            template_id: 3508,
            user_msg: vec![id.clone()],
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_oco_order_with_account(order1, order2, None)
    }

    pub fn request_oco_order_for_account(
        &mut self,
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_oco_order_with_account(order1, order2, Some(account))
    }

    fn request_oco_order_with_account(
        &mut self,
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let trade_route = match self.env {
            RithmicEnv::Live => TRADE_ROUTE_LIVE,
            RithmicEnv::Demo | RithmicEnv::Test => TRADE_ROUTE_DEMO,
        };

        let req = RequestOcoOrder {
            template_id: 328,
            user_msg: vec![id.clone()],
            user_tag: vec![order1.user_tag, order2.user_tag],
            window_name: vec![],
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_link_orders_with_account(basket_ids, None)
    }

    pub fn request_link_orders_for_account(
        &mut self,
        basket_ids: Vec<String>,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_link_orders_with_account(basket_ids, Some(account))
    }

    fn request_link_orders_with_account(
        &mut self,
        basket_ids: Vec<String>,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let count = basket_ids.len();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestLinkOrders {
            template_id: 344,
            user_msg: vec![id.clone()],
            fcm_id: vec![fcm_id; count],
            ib_id: vec![ib_id; count],
            account_id: vec![account_id; count],
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
        self.request_modify_order_reference_data_with_account(basket_id, user_tag, None)
    }

    pub fn request_modify_order_reference_data_for_account(
        &mut self,
        basket_id: &str,
        user_tag: &str,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_modify_order_reference_data_with_account(basket_id, user_tag, Some(account))
    }

    fn request_modify_order_reference_data_with_account(
        &mut self,
        basket_id: &str,
        user_tag: &str,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestModifyOrderReferenceData {
            template_id: 3500,
            user_msg: vec![id.clone()],
            user_tag: Some(user_tag.to_string()),
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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
        self.request_replay_executions_with_account(start_index_sec, finish_index_sec, None)
    }

    pub fn request_replay_executions_for_account(
        &mut self,
        start_index_sec: i32,
        finish_index_sec: i32,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        self.request_replay_executions_with_account(
            start_index_sec,
            finish_index_sec,
            Some(account),
        )
    }

    fn request_replay_executions_with_account(
        &mut self,
        start_index_sec: i32,
        finish_index_sec: i32,
        account: Option<&RithmicAccount>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let (fcm_id, ib_id, account_id) = self.account_fields(account);

        let req = RequestReplayExecutions {
            template_id: 3506,
            user_msg: vec![id.clone()],
            fcm_id: Some(fcm_id),
            ib_id: Some(ib_id),
            account_id: Some(account_id),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn advanced_bracket() -> RithmicAdvancedBracketOrder {
        RithmicAdvancedBracketOrder {
            action: crate::rti::request_bracket_order::TransactionType::Buy,
            duration: crate::rti::request_bracket_order::Duration::Gtc,
            exchange: "CME".to_string(),
            localid: "advanced-bracket-1".to_string(),
            price_type: crate::rti::request_bracket_order::PriceType::StopLimit,
            price: Some(5000.25),
            trigger_price: Some(4999.75),
            quantity: 3,
            symbol: "ESM6".to_string(),
            bracket_type: crate::rti::request_bracket_order::BracketType::TargetAndStop,
            target_quantity: vec![2, 1],
            target_ticks: vec![16, 24],
            stop_quantity: vec![3],
            stop_ticks: vec![8],
            if_touched: Some(crate::api::RithmicIfTouchedTrigger {
                symbol: "NQM6".to_string(),
                exchange: "CME".to_string(),
                condition: crate::rti::request_bracket_order::Condition::GreaterThanEqualTo,
                price_field: crate::rti::request_bracket_order::PriceField::TradePrice,
                price: 18250.5,
            }),
            break_even_ticks: Some(2),
            break_even_trigger_ticks: Some(10),
            trailing_stop_trigger_ticks: Some(12),
            trailing_stop_by_last_trade_price: Some(true),
            target_market_order_if_touched: Some(true),
            stop_market_on_reject: Some(true),
            target_market_at_ssboe: Some(36000),
            target_market_at_usecs: Some(250000),
            stop_market_at_ssboe: Some(36100),
            stop_market_at_usecs: Some(500000),
            target_market_order_after_secs: Some(30),
            release_at_ssboe: Some(35900),
            release_at_usecs: Some(125000),
            cancel_at_ssboe: Some(37000),
            cancel_at_usecs: Some(750000),
            cancel_after_secs: Some(120),
        }
    }

    fn test_config() -> RithmicConfig {
        RithmicConfig::builder(RithmicEnv::Demo)
            .account_id("ACCOUNT_A")
            .fcm_id("FCM_A")
            .ib_id("IB_A")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .user("user")
            .password("password")
            .app_name("app")
            .app_version("1")
            .system_name("Rithmic Paper Trading")
            .build()
            .expect("valid config")
    }

    fn override_account() -> RithmicAccount {
        RithmicAccount::new("FCM_B", "IB_B", "ACCOUNT_B")
    }

    fn decode_request<T: Message + Default>(buf: &[u8]) -> T {
        T::decode(&buf[4..]).expect("decode request")
    }

    #[test]
    fn order_request_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: 5000.0,
            transaction_type: crate::rti::request_new_order::TransactionType::Buy,
            price_type: crate::rti::request_new_order::PriceType::Limit,
            user_tag: "order-1".to_string(),
            duration: Some(crate::rti::request_new_order::Duration::Day),
            trigger_price: None,
            trailing_stop: None,
        };

        let (buf, _) = api.request_order_for_account(&order, &override_account());
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn pnl_snapshot_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_pnl_position_snapshot_for_account(&override_account());
        let request: RequestPnLPositionSnapshot = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn legacy_request_path_keeps_configured_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_show_orders();
        let request: RequestShowOrders = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_A"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_A"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_A"));
    }

    #[test]
    fn bracket_request_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());
        let bracket = RithmicBracketOrder {
            action: crate::rti::request_bracket_order::TransactionType::Buy,
            duration: crate::rti::request_bracket_order::Duration::Day,
            exchange: "CME".to_string(),
            localid: "bracket-1".to_string(),
            price_type: crate::rti::request_bracket_order::PriceType::Limit,
            price: Some(5000.0),
            profit_ticks: 20,
            quantity: 1,
            stop_ticks: 10,
            symbol: "ESM6".to_string(),
        };

        let (buf, _) = api.request_bracket_order_for_account(bracket, &override_account());
        let request: RequestBracketOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
        assert_eq!(
            request.bracket_type,
            Some(crate::rti::request_bracket_order::BracketType::TargetAndStopStatic as i32)
        );
        assert_eq!(request.target_quantity, vec![1]);
        assert_eq!(request.target_ticks, vec![20]);
        assert_eq!(request.stop_quantity, vec![1]);
        assert_eq!(request.stop_ticks, vec![10]);
        assert_eq!(request.price, Some(5000.0));
        assert_eq!(request.trigger_price, None);
        assert_eq!(request.user_tag.as_deref(), Some("bracket-1"));
    }

    #[test]
    fn advanced_bracket_request_encodes_trigger_and_if_touched_fields() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) =
            api.request_advanced_bracket_order_for_account(advanced_bracket(), &override_account());
        let request: RequestBracketOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
        assert_eq!(request.price, Some(5000.25));
        assert_eq!(request.trigger_price, Some(4999.75));
        assert_eq!(
            request.price_type,
            Some(crate::rti::request_bracket_order::PriceType::StopLimit as i32)
        );
        assert_eq!(
            request.bracket_type,
            Some(crate::rti::request_bracket_order::BracketType::TargetAndStop as i32)
        );
        assert_eq!(request.target_quantity, vec![2, 1]);
        assert_eq!(request.target_ticks, vec![16, 24]);
        assert_eq!(request.stop_quantity, vec![3]);
        assert_eq!(request.stop_ticks, vec![8]);
        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_exchange.as_deref(), Some("CME"));
        assert_eq!(
            request.if_touched_condition,
            Some(crate::rti::request_bracket_order::Condition::GreaterThanEqualTo as i32)
        );
        assert_eq!(
            request.if_touched_price_field,
            Some(crate::rti::request_bracket_order::PriceField::TradePrice as i32)
        );
        assert_eq!(request.if_touched_price, Some(18250.5));
    }

    #[test]
    fn advanced_bracket_request_encodes_management_and_timing_fields() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_advanced_bracket_order(advanced_bracket());
        let request: RequestBracketOrder = decode_request(&buf);

        assert_eq!(request.break_even_ticks, Some(2));
        assert_eq!(request.break_even_trigger_ticks, Some(10));
        assert_eq!(request.trailing_stop_trigger_ticks, Some(12));
        assert_eq!(request.trailing_stop_by_last_trade_price, Some(true));
        assert_eq!(request.target_market_order_if_touched, Some(true));
        assert_eq!(request.stop_market_on_reject, Some(true));
        assert_eq!(request.target_market_at_ssboe, Some(36000));
        assert_eq!(request.target_market_at_usecs, Some(250000));
        assert_eq!(request.stop_market_at_ssboe, Some(36100));
        assert_eq!(request.stop_market_at_usecs, Some(500000));
        assert_eq!(request.target_market_order_after_secs, Some(30));
        assert_eq!(request.release_at_ssboe, Some(35900));
        assert_eq!(request.release_at_usecs, Some(125000));
        assert_eq!(request.cancel_at_ssboe, Some(37000));
        assert_eq!(request.cancel_at_usecs, Some(750000));
        assert_eq!(request.cancel_after_secs, Some(120));
    }

    #[test]
    fn oco_request_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());
        let leg1 = RithmicOcoOrderLeg {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: 5000.0,
            trigger_price: None,
            transaction_type: crate::rti::request_oco_order::TransactionType::Buy,
            duration: crate::rti::request_oco_order::Duration::Day,
            price_type: crate::rti::request_oco_order::PriceType::Limit,
            user_tag: "oco-1".to_string(),
        };
        let leg2 = RithmicOcoOrderLeg {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: 4990.0,
            trigger_price: Some(4990.0),
            transaction_type: crate::rti::request_oco_order::TransactionType::Sell,
            duration: crate::rti::request_oco_order::Duration::Day,
            price_type: crate::rti::request_oco_order::PriceType::StopMarket,
            user_tag: "oco-2".to_string(),
        };

        let (buf, _) = api.request_oco_order_for_account(leg1, leg2, &override_account());
        let request: RequestOcoOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn exit_position_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_exit_position_for_account("ESM6", "CME", &override_account());
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn link_orders_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_link_orders_for_account(
            vec!["basket-1".to_string(), "basket-2".to_string()],
            &override_account(),
        );
        let request: RequestLinkOrders = decode_request(&buf);

        assert_eq!(
            request.fcm_id,
            vec!["FCM_B".to_string(), "FCM_B".to_string()]
        );
        assert_eq!(request.ib_id, vec!["IB_B".to_string(), "IB_B".to_string()]);
        assert_eq!(
            request.account_id,
            vec!["ACCOUNT_B".to_string(), "ACCOUNT_B".to_string()]
        );
    }

    #[test]
    fn modify_order_reference_data_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_modify_order_reference_data_for_account(
            "basket-1",
            "new-tag",
            &override_account(),
        );
        let request: RequestModifyOrderReferenceData = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn account_rms_updates_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_account_rms_updates_for_account(true, &override_account());
        let request: RequestAccountRmsUpdates = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }
}
