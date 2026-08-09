use prost::Message;

use super::commands::{
    RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders, RithmicCancelOrder,
    RithmicExitPosition, RithmicLinkOrders, RithmicModifyOrder, RithmicModifyOrderReferenceData,
    RithmicOcoOrderLeg, RithmicOrder, oco::OcoCancelTiming,
};

use crate::{
    config::{LoginConfig, RithmicAccount, RithmicConfig},
    error::RithmicError,
    rti::{
        RequestAcceptAgreement, RequestAccountList, RequestAccountRmsInfo,
        RequestAccountRmsUpdates, RequestAuxilliaryReferenceData, RequestBracketOrder,
        RequestCancelAllOrders, RequestCancelOrder, RequestDepthByOrderSnapshot,
        RequestDepthByOrderUpdates, RequestEasyToBorrowList, RequestExitPosition,
        RequestFrontMonthContract, RequestGetInstrumentByUnderlying, RequestGetUserInfo,
        RequestGetVolumeAtPrice, RequestGiveTickSizeTypeTable, RequestHeartbeat, RequestLinkOrders,
        RequestListAcceptedAgreements, RequestListExchangePermissions,
        RequestListUnacceptedAgreements, RequestLogin, RequestLoginInfo, RequestLogout,
        RequestMarketDataUpdate, RequestMarketDataUpdateByUnderlying, RequestModifyOrder,
        RequestModifyOrderReferenceData, RequestNewOrder, RequestOcoOrder,
        RequestOrderSessionConfig, RequestPnLPositionSnapshot, RequestPnLPositionUpdates,
        RequestProductCodes, RequestProductRmsInfo, RequestReferenceData, RequestReplayExecutions,
        RequestResumeBars, RequestRithmicSystemGatewayInfo, RequestRithmicSystemInfo,
        RequestSearchSymbols, RequestSetRithmicMrktDataSelfCertStatus, RequestShowAgreement,
        RequestShowBracketStops, RequestShowBrackets, RequestShowFillHistory,
        RequestShowOrderHistory, RequestShowOrderHistoryDates, RequestShowOrderHistoryDetail,
        RequestShowOrderHistorySummary, RequestShowOrders, RequestSubscribeForOrderUpdates,
        RequestSubscribeToBracketUpdates, RequestTickBarReplay, RequestTickBarUpdate,
        RequestTimeBarReplay, RequestTimeBarUpdate, RequestTradeRoutes,
        RequestUpdateStopBracketLevel, RequestUpdateTargetBracketLevel,
        RequestVolumeProfileMinuteBars, ResponseLoginInfo, request_account_list,
        request_account_rms_info, request_account_rms_updates, request_bracket_order,
        request_cancel_all_orders, request_cancel_order, request_depth_by_order_updates,
        request_easy_to_borrow_list, request_exit_position,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
        request_market_data_update_by_underlying, request_modify_order, request_new_order,
        request_oco_order, request_pn_l_position_updates, request_search_symbols,
        request_tick_bar_replay::{BarSubType, BarType, Direction, TimeOrder},
        request_tick_bar_update, request_time_bar_replay, request_time_bar_update,
        response_login_info,
    },
    types::{
        EasyToBorrowRequest, FillHistoryRange, OrderType, RmsUpdateBits, TickBarReplayRequest,
        TimeBarReplayRequest, VolumeProfileMinuteBarsRequest,
    },
};

/// The protocol template version sent on every login.
///
/// It names the `.proto` set in `src/raw-proto/`, which the R | Protocol API
/// 0.89.0.0 change log labels template 5.42. Bump it whenever those protos are
/// regenerated against a newer release.
pub(crate) const TEMPLATE_VERSION: &str = "5.42";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoginUserType {
    Fcm,
    Ib,
    Trader,
}

// Each request proto declares its own `UserType`. List every one here so no request is
// left hardcoding a user type the login never granted.
//
// Mapped by name, not by cast — the numbers agree today, but a cast would keep sending
// the old one if a proto were renumbered.
macro_rules! login_user_type_accessors {
    ($($accessor:ident => $request:ty),+ $(,)?) => {
        impl LoginUserType {
            $(
                fn $accessor(self) -> $request {
                    match self {
                        LoginUserType::Fcm => <$request>::Fcm,
                        LoginUserType::Ib => <$request>::Ib,
                        LoginUserType::Trader => <$request>::Trader,
                    }
                }
            )+
        }
    };
}

login_user_type_accessors! {
    account_list => request_account_list::UserType,
    account_rms_info => request_account_rms_info::UserType,
    bracket_order => request_bracket_order::UserType,
    cancel_all_orders => request_cancel_all_orders::UserType,
}

/// What a login grants, used to scope requests by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoginScope {
    pub(crate) fcm_id: Option<String>,
    pub(crate) ib_id: Option<String>,
    pub(crate) user_type: LoginUserType,
}

impl LoginScope {
    /// The only way to build one, so a scope that exists is always one the requests can
    /// send. `None` for a user type they can't: `Admin`, out of range, or absent.
    ///
    /// `Admin` is missing from the account-list and RMS-info protos entirely, so a scope
    /// only half the requests could use would be worse than none.
    pub(crate) fn from_login_info(info: &ResponseLoginInfo) -> Option<Self> {
        let user_type = match info
            .user_type
            .and_then(|ty| response_login_info::UserType::try_from(ty).ok())?
        {
            response_login_info::UserType::Fcm => LoginUserType::Fcm,
            response_login_info::UserType::Ib => LoginUserType::Ib,
            response_login_info::UserType::Trader => LoginUserType::Trader,
            response_login_info::UserType::Admin => return None,
        };

        Some(LoginScope {
            fcm_id: info.fcm_id.clone(),
            ib_id: info.ib_id.clone(),
            user_type,
        })
    }
}

/// An empty caller-supplied identifier carries no more information than an
/// absent field, so it is left off the wire rather than sent as `""`.
fn omit_if_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// Zero-fill per-leg optional prices into the repeated field that goes on the
/// wire.
///
/// The repeated field is positional — slot `i` prices leg `i` — so once one
/// leg carries a price every leg needs a slot and the ones without take `0.0`.
/// When no leg carries one the field is left out entirely rather than sent as
/// a run of zeroes, which would price every leg at zero.
fn zero_fill_prices(prices: Vec<Option<f64>>) -> Vec<f64> {
    if prices.iter().all(Option::is_none) {
        return vec![];
    }

    prices
        .into_iter()
        .map(|price| price.unwrap_or(0.0))
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct RithmicSenderApi {
    app_name: String,
    app_version: String,
    message_id_counter: u64,
}

impl RithmicSenderApi {
    pub(crate) fn new(config: &RithmicConfig) -> Self {
        RithmicSenderApi {
            app_name: config.app_name.clone(),
            app_version: config.app_version.clone(),
            message_id_counter: 0,
        }
    }

    fn get_next_message_id(&mut self) -> String {
        self.message_id_counter += 1;
        self.message_id_counter.to_string()
    }

    fn request_to_buf(&self, req: impl Message, id: String) -> (Vec<u8>, String) {
        let len = req.encoded_len() as u32;

        let mut buf = Vec::with_capacity((len + 4) as usize);
        buf.extend_from_slice(&len.to_be_bytes());

        req.encode(&mut buf)
            .expect("prost encoding into a Vec<u8> is infallible");

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
            template_version: Some(TEMPLATE_VERSION.into()),
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

    /// Request the accounts visible to the logged-in user
    ///
    /// # Arguments
    /// * `scope` - Narrows the query to the login. `None` sends no ids and `Trader`.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_account_list(&mut self, scope: Option<&LoginScope>) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAccountList {
            template_id: 302,
            fcm_id: scope.and_then(|s| s.fcm_id.clone()),
            ib_id: scope.and_then(|s| s.ib_id.clone()),
            user_type: Some(
                scope
                    .map_or(LoginUserType::Trader, |s| s.user_type)
                    .account_list()
                    .into(),
            ),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_for_order_updates(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestSubscribeForOrderUpdates {
            template_id: 308,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_to_bracket_updates(
        &mut self,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestSubscribeToBracketUpdates {
            template_id: 336,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Build a new order request from a [`RithmicOrder`], including advanced
    /// features like trigger prices and trailing stops.
    ///
    /// The route sent is the `trade_route` argument; `order.trade_route` is one
    /// of the inputs the caller resolved it from and is not read here.
    ///
    /// # Arguments
    /// * `order` - The order parameters
    /// * `account` - The account to place the order for
    /// * `trade_route` - The route to send the order on
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_order(
        &mut self,
        order: &RithmicOrder,
        account: &RithmicAccount,
        trade_route: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestNewOrder {
            template_id: 312,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(order.exchange.clone()),
            symbol: Some(order.symbol.clone()),
            quantity: Some(order.quantity),
            price: order.price,
            transaction_type: Some(
                request_new_order::TransactionType::from(order.transaction_type).into(),
            ),
            price_type: Some(request_new_order::PriceType::from(order.price_type).into()),
            manual_or_auto: Some(
                request_new_order::OrderPlacement::from(order.manual_or_auto).into(),
            ),
            duration: Some(request_new_order::Duration::from(order.duration).into()),
            user_msg: vec![id.clone()],
            user_tag: omit_if_empty(order.user_tag.clone()),
            trigger_price: order.trigger_price,
            trailing_stop: order.trailing_stop.as_ref().map(|_| true),
            trail_by_ticks: order.trailing_stop.as_ref().map(|ts| ts.trail_by_ticks),
            trail_by_price_id: order.trailing_stop.as_ref().map(|ts| ts.trail_by_price_id),
            window_name: order.window_name.clone(),
            release_at_ssboe: order.release_at_ssboe,
            release_at_usecs: order.release_at_usecs,
            cancel_at_ssboe: order.cancel_at_ssboe,
            cancel_at_usecs: order.cancel_at_usecs,
            cancel_after_secs: order.cancel_after_secs,
            if_touched_symbol: order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.symbol.clone()),
            if_touched_exchange: order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.exchange.clone()),
            if_touched_condition: order
                .if_touched
                .as_ref()
                .map(|trigger| request_new_order::Condition::from(trigger.condition).into()),
            if_touched_price_field: order
                .if_touched
                .as_ref()
                .map(|trigger| request_new_order::PriceField::from(trigger.price_field).into()),
            if_touched_price: order.if_touched.as_ref().and_then(|trigger| trigger.price),
        };

        self.request_to_buf(req, id)
    }

    /// Build a bracket order request from a [`RithmicBracketOrder`].
    ///
    /// The route sent is the `trade_route` argument; `bracket_order.trade_route`
    /// is one of the inputs the caller resolved it from and is not read here.
    ///
    /// # Arguments
    /// * `bracket_order` - The bracket order parameters
    /// * `account` - The account to place the order for
    /// * `scope` - Supplies the user type the login granted
    /// * `trade_route` - The route to send the order on
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_bracket_order(
        &mut self,
        bracket_order: RithmicBracketOrder,
        account: &RithmicAccount,
        scope: Option<&LoginScope>,
        trade_route: &str,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestBracketOrder {
            template_id: 330,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(bracket_order.exchange),
            symbol: Some(bracket_order.symbol),
            user_type: Some(
                scope
                    .map_or(LoginUserType::Trader, |s| s.user_type)
                    .bracket_order()
                    .into(),
            ),
            quantity: Some(bracket_order.quantity),
            transaction_type: Some(
                request_bracket_order::TransactionType::from(bracket_order.action).into(),
            ),
            price_type: Some(
                request_bracket_order::PriceType::from(bracket_order.price_type).into(),
            ),
            manual_or_auto: Some(
                request_bracket_order::OrderPlacement::from(bracket_order.manual_or_auto).into(),
            ),
            duration: Some(request_bracket_order::Duration::from(bracket_order.duration).into()),
            // A bracket with no exit legs has no shape to describe, so the field
            // is omitted rather than asserting one the caller never asked for.
            bracket_type: bracket_order
                .bracket_type
                .map(|kind| request_bracket_order::BracketType::from(kind).into()),
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
                .map(|trigger| request_bracket_order::Condition::from(trigger.condition).into()),
            if_touched_price_field: bracket_order
                .if_touched
                .as_ref()
                .map(|trigger| request_bracket_order::PriceField::from(trigger.price_field).into()),
            if_touched_price: bracket_order
                .if_touched
                .as_ref()
                .and_then(|trigger| trigger.price),
            price: bracket_order.price,
            trigger_price: bracket_order.trigger_price,
            user_msg: vec![id.clone()],
            user_tag: omit_if_empty(bracket_order.localid),
            window_name: bracket_order.window_name,
            order_operation_type: bracket_order
                .operation_type
                .map(|op| op.as_str_name().to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Build a request to modify a working order.
    ///
    /// A stop order with no explicit `trigger_price` triggers at its own price.
    ///
    /// # Arguments
    /// * `order` - The modification to apply
    /// * `account` - The account the order belongs to
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_modify_order(
        &mut self,
        order: &RithmicModifyOrder,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let price_type = request_modify_order::PriceType::from(order.price_type);

        let req = RequestModifyOrder {
            template_id: 314,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            basket_id: Some(order.id.clone()),
            manual_or_auto: Some(
                request_modify_order::OrderPlacement::from(order.manual_or_auto).into(),
            ),
            exchange: Some(order.exchange.clone()),
            symbol: Some(order.symbol.clone()),
            price_type: Some(price_type.into()),
            quantity: Some(order.quantity),
            price: order.price,
            user_msg: vec![id.clone()],
            // The same four types `RithmicOrder::validate` demands a trigger for
            // when the order is placed, so a modify to one of them carries a
            // trigger too. The order's own price is the stand-in when the caller
            // named no separate level; `RithmicModifyOrder::validate` guarantees
            // a built command has one or the other.
            trigger_price: order.trigger_price.or(match order.price_type {
                OrderType::StopMarket
                | OrderType::StopLimit
                | OrderType::MarketIfTouched
                | OrderType::LimitIfTouched => order.price,
                OrderType::Market | OrderType::Limit => None,
            }),
            window_name: order.window_name.clone(),
            // `RequestModifyOrder` splits the trailing stop into a flag and a
            // distance, so the flag is derived rather than set by the caller.
            trailing_stop: order.trail_by_ticks.map(|_| true),
            trail_by_ticks: order.trail_by_ticks,
            if_touched_symbol: order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.symbol.clone()),
            if_touched_exchange: order
                .if_touched
                .as_ref()
                .map(|trigger| trigger.exchange.clone()),
            if_touched_condition: order
                .if_touched
                .as_ref()
                .map(|trigger| request_modify_order::Condition::from(trigger.condition).into()),
            if_touched_price_field: order
                .if_touched
                .as_ref()
                .map(|trigger| request_modify_order::PriceField::from(trigger.price_field).into()),
            if_touched_price: order.if_touched.as_ref().and_then(|trigger| trigger.price),
        };

        self.request_to_buf(req, id)
    }

    /// Build a request to cancel a working order.
    ///
    /// # Arguments
    /// * `order` - The order to cancel
    /// * `account` - The account the order belongs to
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_cancel_order(
        &mut self,
        order: &RithmicCancelOrder,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestCancelOrder {
            template_id: 316,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            basket_id: Some(order.id.clone()),
            manual_or_auto: Some(
                request_cancel_order::OrderPlacement::from(order.manual_or_auto).into(),
            ),
            user_msg: vec![id.clone()],
            window_name: order.window_name.clone(),
        };

        self.request_to_buf(req, id)
    }

    /// Request to exit an entire position for a given symbol
    ///
    /// This will close all open positions for the specified symbol/exchange combination
    /// by placing a market order in the opposite direction.
    ///
    /// # Arguments
    /// * `command` - The position to exit and how the exit is attributed
    /// * `account` - The account holding the position
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_exit_position(
        &mut self,
        command: &RithmicExitPosition,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestExitPosition {
            template_id: 3504,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            symbol: command.symbol.clone(),
            exchange: command.exchange.clone(),
            manual_or_auto: Some(
                request_exit_position::OrderPlacement::from(command.manual_or_auto).into(),
            ),
            user_msg: vec![id.clone()],
            window_name: command.window_name.clone(),
            trading_algorithm: command.trading_algorithm.clone(),
        };

        self.request_to_buf(req, id)
    }

    /// Update the profit target level of a bracket
    ///
    /// # Arguments
    /// * `adjustment` - The basket, the new profit target distance in ticks, and
    ///   which leg to adjust. The level is sent verbatim; the crate defines no
    ///   numbering, and `None` omits the field.
    /// * `account` - The account the bracket belongs to
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_update_target_bracket_level(
        &mut self,
        adjustment: &RithmicBracketLevelAdjustment,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestUpdateTargetBracketLevel {
            template_id: 332,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            basket_id: Some(adjustment.id.clone()),
            level: adjustment.level,
            target_ticks: Some(adjustment.ticks),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Update the stop loss level of a bracket
    ///
    /// # Arguments
    /// * `adjustment` - The basket, the new stop loss distance in ticks, and
    ///   which leg to adjust. The level is sent verbatim; the crate defines no
    ///   numbering, and `None` omits the field.
    /// * `account` - The account the bracket belongs to
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_update_stop_bracket_level(
        &mut self,
        adjustment: &RithmicBracketLevelAdjustment,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestUpdateStopBracketLevel {
            template_id: 334,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            basket_id: Some(adjustment.id.clone()),
            level: adjustment.level,
            stop_ticks: Some(adjustment.ticks),
            user_msg: vec![id.clone()],
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
    pub fn request_show_brackets(&mut self, account: &RithmicAccount) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowBrackets {
            template_id: 338,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
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
    pub fn request_show_bracket_stops(&mut self, account: &RithmicAccount) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowBracketStops {
            template_id: 340,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_show_orders(&mut self, account: &RithmicAccount) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrders {
            template_id: 320,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_updates(
        &mut self,
        action: request_pn_l_position_updates::Request,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestPnLPositionUpdates {
            template_id: 400,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            request: Some(action.into()),
            // Off the wire keeps the pre-5.42 behavior: the subscription
            // streams every PnL update, not just RMS-driven ones.
            rms_updates_only: None,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_snapshot(&mut self, account: &RithmicAccount) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestPnLPositionSnapshot {
            template_id: 402,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Build a tick bar replay request.
    ///
    /// # Arguments
    ///
    /// * `request` - The window and bar length to replay. Build it with
    ///   [`TickBarReplayRequest::new`](crate::TickBarReplayRequest::new).
    ///
    /// # Returns
    ///
    /// A tuple containing the request buffer and the message id.
    pub fn request_tick_bar_replay(&mut self, request: &TickBarReplayRequest) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTickBarReplay {
            template_id: 206,
            exchange: Some(request.exchange.clone()),
            symbol: Some(request.symbol.clone()),
            bar_type: Some(BarType::TickBar.into()),
            bar_sub_type: Some(BarSubType::Regular.into()),
            bar_type_specifier: Some(request.bar_type_specifier.clone()),
            start_index: Some(request.start_time_sec),
            finish_index: Some(request.end_time_sec),
            direction: Some(Direction::First.into()),
            time_order: Some(TimeOrder::Forwards.into()),
            user_max_count: request.user_max_count,
            resume_bars: request.resume_bars,
            user_msg: vec![id.clone()],
            ..Default::default()
        };

        self.request_to_buf(req, id)
    }

    /// Build a time bar replay request.
    ///
    /// # Arguments
    ///
    /// * `request` - The window and bar size to replay. Build it with
    ///   [`TimeBarReplayRequest::new`](crate::TimeBarReplayRequest::new).
    ///
    /// # Returns
    ///
    /// A tuple containing the request buffer and the message id.
    pub fn request_time_bar_replay(&mut self, request: &TimeBarReplayRequest) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTimeBarReplay {
            template_id: 202,
            exchange: Some(request.exchange.clone()),
            symbol: Some(request.symbol.clone()),
            bar_type: request.bar_type.map(Into::into),
            bar_type_period: Some(request.bar_type_period),
            start_index: Some(request.start_time_sec),
            finish_index: Some(request.end_time_sec),
            direction: Some(request_time_bar_replay::Direction::First.into()),
            time_order: Some(request_time_bar_replay::TimeOrder::Forwards.into()),
            user_max_count: request.user_max_count,
            resume_bars: request.resume_bars,
            user_msg: vec![id.clone()],
            ..Default::default()
        };

        self.request_to_buf(req, id)
    }

    /// Build a volume profile minute bars request.
    ///
    /// Returns minute bar data with volume profile information.
    ///
    /// # Arguments
    ///
    /// * `request` - The window and bar period to replay. Build it with
    ///   [`VolumeProfileMinuteBarsRequest::new`](crate::VolumeProfileMinuteBarsRequest::new).
    ///
    /// # Returns
    ///
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_volume_profile_minute_bars(
        &mut self,
        request: &VolumeProfileMinuteBarsRequest,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestVolumeProfileMinuteBars {
            template_id: 208,
            user_msg: vec![id.clone()],
            symbol: Some(request.symbol.clone()),
            exchange: Some(request.exchange.clone()),
            bar_type_period: Some(request.bar_type_period),
            start_index: Some(request.start_time_sec),
            finish_index: Some(request.end_time_sec),
            user_max_count: request.user_max_count,
            resume_bars: request.resume_bars,
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

    pub fn request_depth_by_order_updates(
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
    /// # Arguments
    /// * `command` - The cancellation and how it is attributed to its originator
    /// * `account` - The account whose orders are cancelled
    /// * `scope` - Supplies the user type the login granted
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_cancel_all_orders(
        &mut self,
        command: &RithmicCancelAllOrders,
        account: &RithmicAccount,
        scope: Option<&LoginScope>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestCancelAllOrders {
            template_id: 346,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            user_type: Some(
                scope
                    .map_or(LoginUserType::Trader, |s| s.user_type)
                    .cancel_all_orders()
                    .into(),
            ),
            manual_or_auto: Some(
                request_cancel_all_orders::OrderPlacement::from(command.manual_or_auto).into(),
            ),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    /// Request account RMS (Risk Management System) information
    ///
    /// Template 304 has no `account_id` field, so this covers every account the login
    /// reaches rather than one account.
    ///
    /// # Arguments
    /// * `account` - Supplies the ids only when there is no scope.
    /// * `scope` - Narrows the query to the login.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_account_rms_info(
        &mut self,
        account: &RithmicAccount,
        scope: Option<&LoginScope>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestAccountRmsInfo {
            template_id: 304,
            user_msg: vec![id.clone()],
            fcm_id: match scope {
                Some(scope) => scope.fcm_id.clone(),
                None => Some(account.fcm_id.clone()),
            },
            ib_id: match scope {
                Some(scope) => scope.ib_id.clone(),
                None => Some(account.ib_id.clone()),
            },
            user_type: Some(
                scope
                    .map_or(LoginUserType::Trader, |s| s.user_type)
                    .account_rms_info()
                    .into(),
            ),
        };

        self.request_to_buf(req, id)
    }

    /// Request product RMS (Risk Management System) information
    ///
    /// Returns risk management limits for specific products/symbols.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_product_rms_info(&mut self, account: &RithmicAccount) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestProductRmsInfo {
            template_id: 306,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
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
    pub fn request_list_exchange_permissions(&mut self, user: &str) -> (Vec<u8>, String) {
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
    /// * `account` - The account to query
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history_summary(
        &mut self,
        date: &str,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistorySummary {
            template_id: 324,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            date: Some(date.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request detailed order history for a specific order
    ///
    /// # Arguments
    /// * `basket_id` - Order/basket identifier
    /// * `date` - Date in YYYYMMDD format
    /// * `account` - The account to query
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history_detail(
        &mut self,
        basket_id: &str,
        date: &str,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistoryDetail {
            template_id: 326,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            basket_id: Some(basket_id.to_string()),
            date: Some(date.to_string()),
        };

        self.request_to_buf(req, id)
    }

    /// Request general order history
    ///
    /// # Arguments
    /// * `basket_id` - Optional order/basket identifier filter
    /// * `account` - The account to query
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_order_history(
        &mut self,
        basket_id: Option<&str>,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrderHistory {
            template_id: 322,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
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
    /// * `update_bits` - which RMS fields to stream, folded into the `update_bits`
    ///   bitmask. An empty `Vec` leaves the field off the request.
    /// * `account` - The account to subscribe for
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_account_rms_updates(
        &mut self,
        subscribe: bool,
        update_bits: Vec<RmsUpdateBits>,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        // An empty selection leaves the field off the wire entirely; sending an
        // explicit 0 is a different message from omitting the field.
        let bits = if update_bits.is_empty() {
            None
        } else {
            Some(update_bits.into_iter().fold(0i32, |acc, f| {
                acc | request_account_rms_updates::UpdateBits::from(f) as i32
            }))
        };

        let req = RequestAccountRmsUpdates {
            template_id: 3508,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            request: Some(
                if subscribe {
                    "subscribe"
                } else {
                    "unsubscribe"
                }
                .to_string(),
            ),
            update_bits: bits,
        };

        self.request_to_buf(req, id)
    }

    /// Request an OCO (One Cancels Other) order with an arbitrary number of legs
    ///
    /// Builds a single `RequestOcoOrder` (template 328) with every repeated field
    /// populated in a single pass over `legs`. When one leg is filled, the others
    /// are automatically cancelled.
    ///
    /// # Arguments
    /// * `legs` - The order legs, each paired with the resolved trade route for
    ///   that leg's exchange, which keeps the repeated route field index-aligned
    /// * `account` - The account to place the order for
    ///
    /// `price` and `trigger_price` are sent for every leg once any leg carries
    /// one, and omitted entirely when none does.
    ///
    /// # Errors
    /// [`RithmicError::InvalidArgument`] when a leg names a price type template
    /// 328 cannot express.
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_oco_order(
        &mut self,
        legs: Vec<(RithmicOcoOrderLeg, String)>,
        timing: OcoCancelTiming,
        account: &RithmicAccount,
    ) -> Result<(Vec<u8>, String), RithmicError> {
        let id = self.get_next_message_id();

        let mut window_name = Vec::new();
        let mut user_tag = Vec::new();
        let mut symbol = Vec::new();
        let mut exchange = Vec::new();
        let mut quantity = Vec::new();
        let mut price: Vec<Option<f64>> = Vec::new();
        let mut trigger_price: Vec<Option<f64>> = Vec::new();
        let mut transaction_type = Vec::new();
        let mut duration = Vec::new();
        let mut price_type = Vec::new();
        let mut trade_routes = Vec::new();
        let mut manual_or_auto = Vec::new();
        let mut trailing_stop = Vec::new();
        let mut trail_by_ticks = Vec::new();
        let mut trail_by_price_id = Vec::new();

        for (leg, trade_route) in legs {
            window_name.push(leg.window_name.unwrap_or_default());
            user_tag.push(leg.user_tag);
            symbol.push(leg.symbol);
            exchange.push(leg.exchange);
            quantity.push(leg.quantity);
            price.push(leg.price);
            trigger_price.push(leg.trigger_price);
            transaction_type.push(i32::from(request_oco_order::TransactionType::from(
                leg.transaction_type,
            )));
            duration.push(i32::from(request_oco_order::Duration::from(leg.duration)));
            price_type.push(i32::from(request_oco_order::PriceType::try_from(
                leg.price_type,
            )?));
            trade_routes.push(trade_route);
            manual_or_auto.push(i32::from(request_oco_order::OrderPlacement::from(
                leg.manual_or_auto,
            )));
            trailing_stop.push(leg.trailing_stop.is_some());
            trail_by_ticks.push(leg.trailing_stop.as_ref().map_or(0, |ts| ts.trail_by_ticks));
            trail_by_price_id.push(
                leg.trailing_stop
                    .as_ref()
                    .map_or(0, |ts| ts.trail_by_price_id),
            );
        }

        // If any leg trails, all three trailing fields keep one slot per leg so
        // they stay lined up with the legs; if none do, leave the fields out.
        let (trailing_stop, trail_by_ticks, trail_by_price_id) = if trailing_stop.contains(&true) {
            (trailing_stop, trail_by_ticks, trail_by_price_id)
        } else {
            (vec![], vec![], vec![])
        };

        let price = zero_fill_prices(price);
        let trigger_price = zero_fill_prices(trigger_price);

        // Position is what ties a value to a leg, so each vector keeps one slot
        // per leg; leave a field out only when no leg sets it.
        if user_tag.iter().all(String::is_empty) {
            user_tag.clear();
        }
        if window_name.iter().all(String::is_empty) {
            window_name.clear();
        }

        let req = RequestOcoOrder {
            template_id: 328,
            user_msg: vec![id.clone()],
            user_tag,
            window_name,
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            symbol,
            exchange,
            quantity,
            price,
            trigger_price,
            transaction_type,
            duration,
            price_type,
            trade_route: trade_routes,
            manual_or_auto,
            trailing_stop,
            trail_by_ticks,
            trail_by_price_id,
            cancel_at_ssboe: timing.cancel_at_ssboe,
            cancel_at_usecs: timing.cancel_at_usecs,
            cancel_after_secs: timing.cancel_after_secs,
        };

        Ok(self.request_to_buf(req, id))
    }

    /// Request to link multiple orders together
    ///
    /// Links orders together by basket id.
    ///
    /// # Arguments
    /// * `command` - The basket IDs to link together
    /// * `account` - The account the baskets belong to
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_link_orders(
        &mut self,
        command: RithmicLinkOrders,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();
        let basket_ids = command.basket_ids;
        let count = basket_ids.len();

        let req = RequestLinkOrders {
            template_id: 344,
            user_msg: vec![id.clone()],
            fcm_id: vec![account.fcm_id.clone(); count],
            ib_id: vec![account.ib_id.clone(); count],
            account_id: vec![account.account_id.clone(); count],
            basket_id: basket_ids,
        };

        self.request_to_buf(req, id)
    }

    /// Request the easy-to-borrow list for short selling
    ///
    /// # Arguments
    /// * `request_type` - Subscribe or Unsubscribe from updates
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_easy_to_borrow_list(
        &mut self,
        request_type: EasyToBorrowRequest,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestEasyToBorrowList {
            template_id: 348,
            user_msg: vec![id.clone()],
            request: Some(request_easy_to_borrow_list::Request::from(request_type).into()),
        };

        self.request_to_buf(req, id)
    }

    /// Modify order reference data (user tag)
    ///
    /// Updates the user-defined reference data on an existing order.
    ///
    /// # Arguments
    /// * `command` - The basket to retag and the new tag
    /// * `account` - The account the basket belongs to
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_modify_order_reference_data(
        &mut self,
        command: &RithmicModifyOrderReferenceData,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestModifyOrderReferenceData {
            template_id: 3500,
            user_msg: vec![id.clone()],
            user_tag: Some(command.user_tag.clone()),
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            basket_id: Some(command.basket_id.clone()),
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
    /// * `account` - The account to query
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_replay_executions(
        &mut self,
        start_index_sec: i32,
        finish_index_sec: i32,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestReplayExecutions {
            template_id: 3506,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            start_index: Some(start_index_sec),
            finish_index: Some(finish_index_sec),
        };

        self.request_to_buf(req, id)
    }

    /// Request the profile of a user (template 3510).
    ///
    /// # Arguments
    /// * `user` - The user to look up. `None` asks about the logged-in user.
    /// * `account` - Supplies the FCM and IB ids on the request
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_get_user_info(
        &mut self,
        user: Option<&str>,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestGetUserInfo {
            template_id: 3510,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            user: user.map(str::to_string),
        };

        self.request_to_buf(req, id)
    }

    /// Request the fill history of an account (template 3512).
    ///
    /// # Arguments
    /// * `range` - The window to report on
    /// * `max_record_count` - Cap on the number of fills returned. Rithmic
    ///   rejects values above 10,000. `None` leaves the cap to the server.
    /// * `account` - The account to report on
    ///
    /// # Returns
    /// A tuple of (serialized request buffer, request ID)
    pub fn request_show_fill_history(
        &mut self,
        range: FillHistoryRange,
        max_record_count: Option<i32>,
        account: &RithmicAccount,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestShowFillHistory {
            template_id: 3512,
            user_msg: vec![id.clone()],
            fcm_id: Some(account.fcm_id.clone()),
            ib_id: Some(account.ib_id.clone()),
            account_id: Some(account.account_id.clone()),
            index_format: Some(range.index_format().to_string()),
            start_index: Some(range.start()),
            finish_index: Some(range.finish()),
            max_record_count,
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
    use crate::{
        api::commands::{RithmicIfTouchedTrigger, RithmicOcoOrder, TrailingStop},
        config::RithmicEnv,
        types::{
            BracketOperationType, BracketType, ManualOrAutoEntry, OrderCondition, OrderPriceField,
            OrderSide, OrderType, TimeInForce,
        },
    };

    fn test_config() -> RithmicConfig {
        RithmicConfig::builder(RithmicEnv::Demo)
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

    fn default_account() -> RithmicAccount {
        RithmicAccount::new("FCM_A", "IB_A", "ACCOUNT_A")
    }

    fn override_account() -> RithmicAccount {
        RithmicAccount::new("FCM_B", "IB_B", "ACCOUNT_B")
    }

    fn decode_request<T: Message + Default>(buf: &[u8]) -> T {
        T::decode(&buf[4..]).expect("decode request")
    }

    /// A bracket exercising every field the request models, built the way
    /// callers build one.
    fn advanced_bracket() -> RithmicBracketOrder {
        RithmicBracketOrder::new()
            .symbol("ESM6")
            .exchange("CME")
            .quantity(3)
            .action(OrderSide::Buy)
            .price_type(OrderType::StopLimit)
            .duration(TimeInForce::Gtc)
            .localid("advanced-bracket-1")
            .price(5000.25)
            .trigger_price(4999.75)
            .bracket_type(BracketType::TargetAndStop)
            .targets([(2, 16), (1, 24)])
            .stops([(3, 8)])
            .if_touched(
                RithmicIfTouchedTrigger::new()
                    .symbol("NQM6")
                    .exchange("CME")
                    .condition(OrderCondition::GreaterThanEqualTo)
                    .price_field(OrderPriceField::TradePrice)
                    .price(18250.5),
            )
            .break_even_ticks(2)
            .break_even_trigger_ticks(10)
            .trailing_stop_trigger_ticks(12)
            .trailing_stop_by_last_trade_price(true)
            .target_market_order_if_touched(true)
            .stop_market_on_reject(true)
            .target_market_at(36000, 250000)
            .stop_market_at(36100, 500000)
            .target_market_order_after_secs(30)
            .release_at(35900, 125000)
            .cancel_at(37000, 750000)
            .cancel_after_secs(120)
            .operation_type(BracketOperationType::Oca)
            .build()
            .expect("valid bracket")
    }

    #[test]
    fn order_request_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            transaction_type: OrderSide::Buy,
            price_type: OrderType::Limit,
            user_tag: "order-1".to_string(),
            duration: TimeInForce::Day,
            trigger_price: None,
            trailing_stop: None,
            trade_route: None,
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &override_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn pnl_snapshot_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_pnl_position_snapshot(&override_account());
        let request: RequestPnLPositionSnapshot = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn show_orders_request_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_show_orders(&default_account());
        let request: RequestShowOrders = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_A"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_A"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_A"));
    }

    #[test]
    fn bracket_request_retains_static_shape_for_simple_helper() {
        let mut api = RithmicSenderApi::new(&test_config());
        let bracket = RithmicBracketOrder::new()
            .symbol("ESM6")
            .exchange("CME")
            .quantity(1)
            .action(OrderSide::Buy)
            .price_type(OrderType::Limit)
            .price(5000.0)
            .target(20)
            .stop(10)
            .localid("bracket-1")
            .build()
            .expect("valid bracket");

        let (buf, _) = api.request_bracket_order(bracket, &override_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
        assert_eq!(
            request.bracket_type,
            Some(request_bracket_order::BracketType::TargetAndStopStatic as i32)
        );
        assert_eq!(request.target_quantity, vec![1]);
        assert_eq!(request.target_ticks, vec![20]);
        assert_eq!(request.stop_quantity, vec![1]);
        assert_eq!(request.stop_ticks, vec![10]);
        assert_eq!(request.price, Some(5000.0));
        assert_eq!(request.trigger_price, None);
        assert_eq!(request.user_tag.as_deref(), Some("bracket-1"));
        // An unset operation type must stay off the wire: a server-side default
        // is not the same request as an explicit OCA.
        assert_eq!(request.order_operation_type, None);
    }

    #[test]
    fn advanced_bracket_request_sets_account_and_trade_route_fields() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) =
            api.request_bracket_order(advanced_bracket(), &override_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
        assert_eq!(request.trade_route.as_deref(), Some("globex"));
        assert_eq!(request.user_tag.as_deref(), Some("advanced-bracket-1"));
        assert_eq!(request.order_operation_type.as_deref(), Some("OCA"));
    }

    #[test]
    fn get_user_info_request_carries_user_and_account_ids() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_get_user_info(Some("someone_else"), &default_account());
        let request: RequestGetUserInfo = decode_request(&buf);

        assert_eq!(request.template_id, 3510);
        assert_eq!(request.fcm_id.as_deref(), Some("FCM_A"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_A"));
        assert_eq!(request.user.as_deref(), Some("someone_else"));
    }

    #[test]
    fn show_fill_history_request_carries_ssboe_range() {
        let mut api = RithmicSenderApi::new(&test_config());
        let range = FillHistoryRange::Ssboe {
            start: 1_700_000_000,
            finish: 1_700_003_600,
        };

        let (buf, _) = api.request_show_fill_history(range, Some(500), &override_account());
        let request: RequestShowFillHistory = decode_request(&buf);

        assert_eq!(request.template_id, 3512);
        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
        assert_eq!(request.index_format.as_deref(), Some("ssboe"));
        assert_eq!(request.start_index, Some(1_700_000_000));
        assert_eq!(request.finish_index, Some(1_700_003_600));
        assert_eq!(request.max_record_count, Some(500));
    }

    #[test]
    fn show_fill_history_request_carries_trade_date_range() {
        let mut api = RithmicSenderApi::new(&test_config());
        let range = FillHistoryRange::TradeDate {
            start: 20_260_801,
            finish: 20_260_804,
        };

        let (buf, _) = api.request_show_fill_history(range, None, &default_account());
        let request: RequestShowFillHistory = decode_request(&buf);

        assert_eq!(request.index_format.as_deref(), Some("trade_date"));
        assert_eq!(request.start_index, Some(20_260_801));
        assert_eq!(request.finish_index, Some(20_260_804));
        assert_eq!(request.max_record_count, None);
    }

    #[test]
    fn advanced_bracket_request_encodes_trigger_and_if_touched_fields() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) =
            api.request_bracket_order(advanced_bracket(), &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(request.price, Some(5000.25));
        assert_eq!(request.trigger_price, Some(4999.75));
        assert_eq!(
            request.price_type,
            Some(request_bracket_order::PriceType::StopLimit as i32)
        );
        assert_eq!(
            request.bracket_type,
            Some(request_bracket_order::BracketType::TargetAndStop as i32)
        );
        assert_eq!(request.target_quantity, vec![2, 1]);
        assert_eq!(request.target_ticks, vec![16, 24]);
        assert_eq!(request.stop_quantity, vec![3]);
        assert_eq!(request.stop_ticks, vec![8]);
        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_exchange.as_deref(), Some("CME"));
        assert_eq!(
            request.if_touched_condition,
            Some(request_bracket_order::Condition::GreaterThanEqualTo as i32)
        );
        assert_eq!(
            request.if_touched_price_field,
            Some(request_bracket_order::PriceField::TradePrice as i32)
        );
        assert_eq!(request.if_touched_price, Some(18250.5));
    }

    #[test]
    fn advanced_bracket_request_encodes_management_and_timing_fields() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) =
            api.request_bracket_order(advanced_bracket(), &default_account(), None, "globex");
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
            price: Some(5000.0),
            trigger_price: None,
            transaction_type: OrderSide::Buy,
            duration: TimeInForce::Day,
            price_type: OrderType::Limit,
            user_tag: "oco-1".to_string(),
            trailing_stop: None,
            trade_route: None,
            manual_or_auto: ManualOrAutoEntry::Auto,
            ..Default::default()
        };
        let leg2 = RithmicOcoOrderLeg {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(4990.0),
            trigger_price: Some(4990.0),
            transaction_type: OrderSide::Sell,
            duration: TimeInForce::Day,
            price_type: OrderType::StopMarket,
            user_tag: "oco-2".to_string(),
            trailing_stop: None,
            trade_route: None,
            manual_or_auto: ManualOrAutoEntry::Auto,
            ..Default::default()
        };

        let (buf, _) = api
            .request_oco_order(
                vec![(leg1, "globex".to_string()), (leg2, "nymex".to_string())],
                OcoCancelTiming::default(),
                &override_account(),
            )
            .expect("every leg's price type is expressible");
        let request: RequestOcoOrder = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));

        // No leg trails, so the trailing-stop fields stay off the wire rather than
        // carrying a trail_by_price_id of 0.
        assert!(request.trailing_stop.is_empty());
        assert!(request.trail_by_ticks.is_empty());
        assert!(request.trail_by_price_id.is_empty());
    }

    #[test]
    fn exit_position_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_exit_position(
            &RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("CME")
                .build()
                .expect("valid exit"),
            &override_account(),
        );
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn link_orders_override_uses_supplied_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_link_orders(
            RithmicLinkOrders::new()
                .basket_ids(["basket-1", "basket-2"])
                .build()
                .expect("valid link"),
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

        let (buf, _) = api.request_modify_order_reference_data(
            &RithmicModifyOrderReferenceData::new()
                .basket_id("basket-1")
                .user_tag("new-tag")
                .build()
                .expect("valid retag"),
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

        let (buf, _) = api.request_account_rms_updates(true, vec![], &override_account());
        let request: RequestAccountRmsUpdates = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
    }

    #[test]
    fn account_rms_updates_omits_update_bits_when_empty() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_account_rms_updates(true, vec![], &default_account());
        let request: RequestAccountRmsUpdates = decode_request(&buf);
        assert_eq!(request.update_bits, None);

        let (buf, _) = api.request_account_rms_updates(false, vec![], &default_account());
        let request: RequestAccountRmsUpdates = decode_request(&buf);
        assert_eq!(request.update_bits, None);
        assert_eq!(request.request.as_deref(), Some("unsubscribe"));
    }

    #[test]
    fn account_rms_updates_sets_update_bits() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_account_rms_updates(
            true,
            vec![RmsUpdateBits::AutoLiqThresholdCurrentValue],
            &default_account(),
        );
        let request: RequestAccountRmsUpdates = decode_request(&buf);

        assert_eq!(request.update_bits, Some(1));
        assert_eq!(request.request.as_deref(), Some("subscribe"));
    }

    #[test]
    fn easy_to_borrow_list_maps_the_request_enum_by_name() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_easy_to_borrow_list(EasyToBorrowRequest::Subscribe);
        let request: RequestEasyToBorrowList = decode_request(&buf);
        assert_eq!(request.template_id, 348);
        assert_eq!(
            request.request,
            Some(request_easy_to_borrow_list::Request::Subscribe as i32)
        );

        let (buf, _) = api.request_easy_to_borrow_list(EasyToBorrowRequest::Unsubscribe);
        let request: RequestEasyToBorrowList = decode_request(&buf);
        assert_eq!(
            request.request,
            Some(request_easy_to_borrow_list::Request::Unsubscribe as i32)
        );
    }

    #[test]
    fn oco_multi_request_populates_repeated_fields_and_trailing_stops() {
        let mut api = RithmicSenderApi::new(&test_config());

        let leg0 = RithmicOcoOrderLeg {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            trigger_price: None,
            transaction_type: OrderSide::Buy,
            duration: TimeInForce::Day,
            price_type: OrderType::Limit,
            user_tag: "leg-0".to_string(),
            trailing_stop: None,
            trade_route: None,
            manual_or_auto: ManualOrAutoEntry::Auto,
            ..Default::default()
        };
        let leg1 = RithmicOcoOrderLeg {
            symbol: "NQM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 2,
            price: Some(18000.0),
            trigger_price: Some(17990.0),
            transaction_type: OrderSide::Sell,
            duration: TimeInForce::Gtc,
            price_type: OrderType::StopMarket,
            user_tag: "leg-1".to_string(),
            trailing_stop: Some(TrailingStop::new().trail_by_ticks(15).trail_by_price_id(7)),
            trade_route: None,
            manual_or_auto: ManualOrAutoEntry::Auto,
            ..Default::default()
        };
        let leg2 = RithmicOcoOrderLeg {
            symbol: "CLM6".to_string(),
            exchange: "NYMEX".to_string(),
            quantity: 3,
            price: Some(75.0),
            trigger_price: Some(74.5),
            transaction_type: OrderSide::Sell,
            duration: TimeInForce::Day,
            price_type: OrderType::StopMarket,
            user_tag: "leg-2".to_string(),
            trailing_stop: Some(TrailingStop::new().trail_by_ticks(25).trail_by_price_id(9)),
            trade_route: None,
            manual_or_auto: ManualOrAutoEntry::Auto,
            ..Default::default()
        };

        let (buf, _) = api
            .request_oco_order(
                vec![
                    (leg0, "globex".to_string()),
                    (leg1, "globex".to_string()),
                    (leg2, "nymex".to_string()),
                ],
                OcoCancelTiming::default(),
                &default_account(),
            )
            .expect("every leg's price type is expressible");
        let request: RequestOcoOrder = decode_request(&buf);

        assert_eq!(request.symbol.len(), 3);
        assert_eq!(
            request.symbol,
            vec!["ESM6".to_string(), "NQM6".to_string(), "CLM6".to_string()]
        );
        assert_eq!(
            request.exchange,
            vec!["CME".to_string(), "CME".to_string(), "NYMEX".to_string()]
        );
        assert_eq!(request.quantity, vec![1, 2, 3]);
        assert_eq!(request.price, vec![5000.0, 18000.0, 75.0]);
        assert_eq!(request.trigger_price, vec![0.0, 17990.0, 74.5]);
        assert_eq!(
            request.user_tag,
            vec![
                "leg-0".to_string(),
                "leg-1".to_string(),
                "leg-2".to_string()
            ]
        );

        // Every remaining repeated field is asserted too: the two-leg builder was
        // rewritten into this loop, so a per-field slip would otherwise go unseen.
        assert_eq!(
            request.transaction_type,
            vec![
                request_oco_order::TransactionType::Buy as i32,
                request_oco_order::TransactionType::Sell as i32,
                request_oco_order::TransactionType::Sell as i32,
            ]
        );
        assert_eq!(
            request.price_type,
            vec![
                request_oco_order::PriceType::Limit as i32,
                request_oco_order::PriceType::StopMarket as i32,
                request_oco_order::PriceType::StopMarket as i32,
            ]
        );
        assert_eq!(
            request.duration,
            vec![
                request_oco_order::Duration::Day as i32,
                request_oco_order::Duration::Gtc as i32,
                request_oco_order::Duration::Day as i32,
            ]
        );
        assert_eq!(
            request.manual_or_auto,
            vec![request_oco_order::OrderPlacement::Auto as i32; 3]
        );
        assert_eq!(
            request.trade_route,
            vec![
                "globex".to_string(),
                "globex".to_string(),
                "nymex".to_string()
            ]
        );
        // Leg 0 does not trail, so its three slots are filled rather than
        // skipped — a shorter vector would move legs 1 and 2's distances onto
        // the wrong legs.
        assert_eq!(request.trailing_stop, vec![false, true, true]);
        assert_eq!(request.trail_by_ticks, vec![0, 15, 25]);
        assert_eq!(request.trail_by_price_id, vec![0, 7, 9]);
    }

    #[test]
    fn order_request_sets_trail_by_price_id() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(0.0),
            transaction_type: OrderSide::Sell,
            price_type: OrderType::StopMarket,
            user_tag: "trailing-stop".to_string(),
            trigger_price: None,
            trailing_stop: Some(TrailingStop::new().trail_by_ticks(20).trail_by_price_id(3)),
            trade_route: None,
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.trailing_stop, Some(true));
        assert_eq!(request.trail_by_ticks, Some(20));
        assert_eq!(request.trail_by_price_id, Some(3));
    }

    #[test]
    fn modify_order_uses_explicit_trigger_price() {
        let mut api = RithmicSenderApi::new(&test_config());

        let modify = |trigger_price: Option<f64>| {
            let mut modification = RithmicModifyOrder::new()
                .id("b")
                .symbol("ESM6")
                .exchange("CME")
                .quantity(2)
                .price(5005.0)
                .price_type(OrderType::StopLimit);
            if let Some(trigger_price) = trigger_price {
                modification = modification.trigger_price(trigger_price);
            }
            modification.build().expect("valid modification")
        };

        let (buf, _) = api.request_modify_order(&modify(Some(4999.0)), &default_account());
        let request: RequestModifyOrder = decode_request(&buf);

        assert_eq!(request.price, Some(5005.0));
        assert_eq!(request.trigger_price, Some(4999.0));

        let (buf, _) = api.request_modify_order(&modify(None), &default_account());
        let request: RequestModifyOrder = decode_request(&buf);

        assert_eq!(request.trigger_price, Some(5005.0));
    }

    /// The fallback covers the same four types `RithmicOrder::validate` demands
    /// a trigger for, not just the two stop types — `RequestModifyOrder`
    /// declares the two if-touched price types as well.
    #[test]
    fn modify_order_falls_back_to_the_price_for_every_triggering_type() {
        let mut api = RithmicSenderApi::new(&test_config());

        for price_type in [
            OrderType::StopMarket,
            OrderType::StopLimit,
            OrderType::MarketIfTouched,
            OrderType::LimitIfTouched,
        ] {
            let modification = RithmicModifyOrder::new()
                .id("b")
                .symbol("ESM6")
                .exchange("CME")
                .quantity(1)
                .price(5005.0)
                .price_type(price_type)
                .build()
                .expect("valid modification");

            let (buf, _) = api.request_modify_order(&modification, &default_account());
            let request: RequestModifyOrder = decode_request(&buf);

            assert_eq!(
                request.trigger_price,
                Some(5005.0),
                "{price_type} takes a trigger, so the price stands in for it"
            );
        }

        for price_type in [OrderType::Market, OrderType::Limit] {
            let modification = RithmicModifyOrder::new()
                .id("b")
                .symbol("ESM6")
                .exchange("CME")
                .quantity(1)
                .price(5005.0)
                .price_type(price_type)
                .build()
                .expect("valid modification");

            let (buf, _) = api.request_modify_order(&modification, &default_account());
            let request: RequestModifyOrder = decode_request(&buf);

            assert_eq!(
                request.trigger_price, None,
                "{price_type} takes no trigger, so none is invented"
            );
        }
    }

    /// A modify restates the order, so an unset trailing distance has to leave
    /// the flag off rather than sending `trailing_stop` with no distance.
    #[test]
    fn modify_order_derives_the_trailing_stop_flag_from_the_distance() {
        let mut api = RithmicSenderApi::new(&test_config());
        let modification = RithmicModifyOrder::new()
            .id("b")
            .symbol("ESM6")
            .exchange("CME")
            .quantity(1)
            .price(5005.0)
            .price_type(OrderType::StopMarket)
            .build()
            .expect("valid modification");

        let (buf, _) = api.request_modify_order(&modification, &default_account());
        let request: RequestModifyOrder = decode_request(&buf);
        assert_eq!(request.trailing_stop, None);
        assert_eq!(request.trail_by_ticks, None);

        let (buf, _) =
            api.request_modify_order(&modification.clone().trail_by_ticks(20), &default_account());
        let request: RequestModifyOrder = decode_request(&buf);
        assert_eq!(request.trailing_stop, Some(true));
        assert_eq!(request.trail_by_ticks, Some(20));
    }

    #[test]
    fn modify_order_carries_the_window_name_and_if_touched_trigger() {
        let mut api = RithmicSenderApi::new(&test_config());
        let modification = RithmicModifyOrder::new()
            .id("b")
            .symbol("ESM6")
            .exchange("CME")
            .quantity(1)
            .price(5005.0)
            .price_type(OrderType::Limit)
            .window_name("chart")
            .if_touched(
                RithmicIfTouchedTrigger::new()
                    .symbol("NQM6")
                    .exchange("CME")
                    .condition(OrderCondition::GreaterThanEqualTo)
                    .price_field(OrderPriceField::TradePrice)
                    .price(18250.5),
            )
            .build()
            .expect("valid modification");

        let (buf, _) = api.request_modify_order(&modification, &default_account());
        let request: RequestModifyOrder = decode_request(&buf);

        assert_eq!(request.window_name.as_deref(), Some("chart"));
        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_exchange.as_deref(), Some("CME"));
        assert_eq!(
            request.if_touched_condition,
            Some(request_modify_order::Condition::GreaterThanEqualTo as i32)
        );
        assert_eq!(
            request.if_touched_price_field,
            Some(request_modify_order::PriceField::TradePrice as i32)
        );
        assert_eq!(request.if_touched_price, Some(18250.5));
    }

    /// A login granting `user_type`.
    fn test_login_info(user_type: response_login_info::UserType) -> ResponseLoginInfo {
        ResponseLoginInfo {
            template_id: 301,
            fcm_id: Some("FCM_LOGIN".to_string()),
            ib_id: Some("IB_LOGIN".to_string()),
            user_type: Some(user_type.into()),
            ..ResponseLoginInfo::default()
        }
    }

    #[test]
    fn login_scope_maps_each_expressible_user_type() {
        for (from, expected) in [
            (response_login_info::UserType::Fcm, LoginUserType::Fcm),
            (response_login_info::UserType::Ib, LoginUserType::Ib),
            (response_login_info::UserType::Trader, LoginUserType::Trader),
        ] {
            let scope = LoginScope::from_login_info(&test_login_info(from))
                .unwrap_or_else(|| panic!("{from:?} is expressible"));

            assert_eq!(scope.user_type, expected);
            assert_eq!(scope.fcm_id.as_deref(), Some("FCM_LOGIN"));
            assert_eq!(scope.ib_id.as_deref(), Some("IB_LOGIN"));
        }
    }

    #[test]
    fn login_scope_rejects_a_user_type_it_cannot_express() {
        // Carrying the ids under a substituted `Trader` would narrow the query to a
        // scope the login never granted, so there is no partial scope to build.
        for info in [
            test_login_info(response_login_info::UserType::Admin),
            ResponseLoginInfo {
                user_type: Some(99),
                ..test_login_info(response_login_info::UserType::Ib)
            },
            ResponseLoginInfo {
                user_type: None,
                ..test_login_info(response_login_info::UserType::Ib)
            },
        ] {
            assert!(
                LoginScope::from_login_info(&info).is_none(),
                "{:?} must not produce a scope",
                info.user_type
            );
        }
    }

    /// A scope as a successful login would leave it.
    fn test_scope(user_type: LoginUserType) -> LoginScope {
        LoginScope {
            fcm_id: Some("FCM_LOGIN".to_string()),
            ib_id: Some("IB_LOGIN".to_string()),
            user_type,
        }
    }

    #[test]
    fn account_list_carries_the_scope() {
        let mut api = RithmicSenderApi::new(&test_config());

        // Trader is included on purpose: those logins carry ids too, so their bytes
        // change as well.
        for (user_type, expected) in [
            (LoginUserType::Fcm, request_account_list::UserType::Fcm),
            (LoginUserType::Ib, request_account_list::UserType::Ib),
            (
                LoginUserType::Trader,
                request_account_list::UserType::Trader,
            ),
        ] {
            let (buf, _) = api.request_account_list(Some(&test_scope(user_type)));
            let request: RequestAccountList = decode_request(&buf);

            assert_eq!(request.fcm_id.as_deref(), Some("FCM_LOGIN"));
            assert_eq!(request.ib_id.as_deref(), Some("IB_LOGIN"));
            assert_eq!(request.user_type, Some(expected.into()));
        }
    }

    /// 304 has no `account_id`, so the login wins over the account passed in.
    #[test]
    fn account_rms_info_carries_the_scope_over_the_account() {
        let mut api = RithmicSenderApi::new(&test_config());

        for (user_type, expected) in [
            (LoginUserType::Fcm, request_account_rms_info::UserType::Fcm),
            (LoginUserType::Ib, request_account_rms_info::UserType::Ib),
            (
                LoginUserType::Trader,
                request_account_rms_info::UserType::Trader,
            ),
        ] {
            let (buf, _) =
                api.request_account_rms_info(&override_account(), Some(&test_scope(user_type)));
            let request: RequestAccountRmsInfo = decode_request(&buf);

            assert_eq!(request.fcm_id.as_deref(), Some("FCM_LOGIN"));
            assert_eq!(request.ib_id.as_deref(), Some("IB_LOGIN"));
            assert_eq!(request.user_type, Some(expected.into()));
        }
    }

    /// 330 and 346 name an account, so only the user type comes from the scope.
    #[test]
    fn account_requests_take_only_the_user_type_from_the_scope() {
        let mut api = RithmicSenderApi::new(&test_config());

        for (user_type, bracket, cancel_all) in [
            (
                LoginUserType::Fcm,
                request_bracket_order::UserType::Fcm,
                request_cancel_all_orders::UserType::Fcm,
            ),
            (
                LoginUserType::Ib,
                request_bracket_order::UserType::Ib,
                request_cancel_all_orders::UserType::Ib,
            ),
            (
                LoginUserType::Trader,
                request_bracket_order::UserType::Trader,
                request_cancel_all_orders::UserType::Trader,
            ),
        ] {
            let scope = test_scope(user_type);

            let (buf, _) = api.request_bracket_order(
                advanced_bracket(),
                &override_account(),
                Some(&scope),
                "globex",
            );
            let request: RequestBracketOrder = decode_request(&buf);

            assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
            assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
            assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
            assert_eq!(request.user_type, Some(bracket.into()));

            let (buf, _) = api.request_cancel_all_orders(
                &RithmicCancelAllOrders::default(),
                &override_account(),
                Some(&scope),
            );
            let request: RequestCancelAllOrders = decode_request(&buf);

            assert_eq!(request.fcm_id.as_deref(), Some("FCM_B"));
            assert_eq!(request.ib_id.as_deref(), Some("IB_B"));
            assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_B"));
            assert_eq!(request.user_type, Some(cancel_all.into()));
        }
    }

    /// Every field is asserted, not just `level`: the builder now names them all
    /// outright rather than leaning on `Default`.
    #[test]
    fn update_target_bracket_level_carries_requested_level() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_update_target_bracket_level(
            &RithmicBracketLevelAdjustment::new()
                .id("basket-1")
                .ticks(16)
                .level(2)
                .build()
                .expect("valid adjustment"),
            &default_account(),
        );
        let request: RequestUpdateTargetBracketLevel = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_A"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_A"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_A"));
        assert_eq!(request.basket_id.as_deref(), Some("basket-1"));
        assert_eq!(request.target_ticks, Some(16));
        assert_eq!(request.level, Some(2));
    }

    #[test]
    fn update_stop_bracket_level_carries_requested_level() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_update_stop_bracket_level(
            &RithmicBracketLevelAdjustment::new()
                .id("basket-1")
                .ticks(8)
                .level(2)
                .build()
                .expect("valid adjustment"),
            &default_account(),
        );
        let request: RequestUpdateStopBracketLevel = decode_request(&buf);

        assert_eq!(request.fcm_id.as_deref(), Some("FCM_A"));
        assert_eq!(request.ib_id.as_deref(), Some("IB_A"));
        assert_eq!(request.account_id.as_deref(), Some("ACCOUNT_A"));
        assert_eq!(request.basket_id.as_deref(), Some("basket-1"));
        assert_eq!(request.stop_ticks, Some(8));
        assert_eq!(request.level, Some(2));
    }

    /// `level` is proto2 `optional`, so a decoded `None` proves the field never
    /// reached the wire — an explicit zero comes back as `Some(0)`.
    #[test]
    fn bracket_level_requests_omit_level_when_unset() {
        let mut api = RithmicSenderApi::new(&test_config());

        // Built by hand, not through the builder: `.level()` cannot express the
        // unset case the first half of this test is about.
        let adjustment = |ticks, level| RithmicBracketLevelAdjustment {
            id: "basket-1".to_string(),
            ticks,
            level,
        };
        let target = |api: &mut RithmicSenderApi, level| {
            let (buf, _) =
                api.request_update_target_bracket_level(&adjustment(16, level), &default_account());
            decode_request::<RequestUpdateTargetBracketLevel>(&buf).level
        };
        let stop = |api: &mut RithmicSenderApi, level| {
            let (buf, _) =
                api.request_update_stop_bracket_level(&adjustment(8, level), &default_account());
            decode_request::<RequestUpdateStopBracketLevel>(&buf).level
        };

        assert_eq!(target(&mut api, None), None);
        assert_eq!(target(&mut api, Some(0)), Some(0));
        assert_eq!(stop(&mut api, None), None);
        assert_eq!(stop(&mut api, Some(0)), Some(0));
    }

    fn oco_leg_priced(
        tag: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
    ) -> RithmicOcoOrderLeg {
        RithmicOcoOrderLeg {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price,
            trigger_price,
            transaction_type: OrderSide::Buy,
            duration: TimeInForce::Day,
            price_type: OrderType::Market,
            user_tag: tag.to_string(),
            trailing_stop: None,
            trade_route: None,
            manual_or_auto: ManualOrAutoEntry::Auto,
            ..Default::default()
        }
    }

    fn oco_request(legs: Vec<RithmicOcoOrderLeg>) -> RequestOcoOrder {
        oco_request_with_timing(legs, OcoCancelTiming::default())
    }

    fn oco_request_with_timing(
        legs: Vec<RithmicOcoOrderLeg>,
        timing: OcoCancelTiming,
    ) -> RequestOcoOrder {
        let mut api = RithmicSenderApi::new(&test_config());
        let routed = legs
            .into_iter()
            .map(|leg| (leg, "globex".to_string()))
            .collect();
        let (buf, _) = api
            .request_oco_order(routed, timing, &default_account())
            .expect("every leg's price type is expressible");

        decode_request(&buf)
    }

    #[test]
    fn order_request_omits_price_when_the_order_has_none() {
        let mut api = RithmicSenderApi::new(&test_config());
        let mut order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: None,
            transaction_type: OrderSide::Buy,
            price_type: OrderType::Market,
            user_tag: "market-order".to_string(),
            duration: TimeInForce::Day,
            trigger_price: None,
            trailing_stop: None,
            trade_route: None,
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(request.price, None);

        // `price` is a proto2 `optional double`, so an explicit zero is
        // distinguishable on the wire from an absent field.
        order.price = Some(0.0);
        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(request.price, Some(0.0));

        order.price = Some(5000.0);
        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(request.price, Some(5000.0));
    }

    #[test]
    fn oco_request_omits_prices_when_no_leg_carries_one() {
        let request = oco_request(vec![
            oco_leg_priced("a", None, None),
            oco_leg_priced("b", None, None),
        ]);

        assert_eq!(request.price, Vec::<f64>::new());
        assert_eq!(request.trigger_price, Vec::<f64>::new());
        // The rest of the per-leg fields still describe both legs.
        assert_eq!(request.quantity, vec![1, 1]);
    }

    #[test]
    fn oco_request_zero_fills_prices_once_any_leg_carries_one() {
        let request = oco_request(vec![
            oco_leg_priced("a", None, None),
            oco_leg_priced("b", Some(5000.0), None),
            oco_leg_priced("c", None, Some(4990.0)),
        ]);

        assert_eq!(request.price, vec![0.0, 5000.0, 0.0]);
        assert_eq!(request.trigger_price, vec![0.0, 0.0, 4990.0]);
    }

    #[test]
    fn order_request_omits_an_empty_user_tag() {
        let mut api = RithmicSenderApi::new(&test_config());
        let mut order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            transaction_type: OrderSide::Buy,
            price_type: OrderType::Limit,
            user_tag: String::new(),
            duration: TimeInForce::Day,
            trigger_price: None,
            trailing_stop: None,
            trade_route: None,
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(request.user_tag, None);

        order.user_tag = "order-1".to_string();
        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(request.user_tag.as_deref(), Some("order-1"));
    }

    #[test]
    fn bracket_request_omits_an_empty_localid() {
        let mut api = RithmicSenderApi::new(&test_config());

        let mut order = advanced_bracket();
        order.localid = String::new();
        let (buf, _) = api.request_bracket_order(order, &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(request.user_tag, None);

        let (buf, _) =
            api.request_bracket_order(advanced_bracket(), &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(request.user_tag.as_deref(), Some("advanced-bracket-1"));
    }

    #[test]
    fn oco_request_omits_user_tags_when_no_leg_is_tagged() {
        let request = oco_request(vec![
            oco_leg_priced("", Some(5000.0), None),
            oco_leg_priced("", Some(4990.0), None),
        ]);
        assert_eq!(request.user_tag, Vec::<String>::new());

        let request = oco_request(vec![
            oco_leg_priced("", Some(5000.0), None),
            oco_leg_priced("tagged", Some(4990.0), None),
        ]);
        assert_eq!(
            request.user_tag,
            vec![String::new(), "tagged".to_string()],
            "an untagged leg keeps its slot once any leg is tagged"
        );
    }

    #[test]
    fn oco_request_omits_window_names_when_no_leg_names_one() {
        let request = oco_request(vec![
            oco_leg_priced("a", Some(5000.0), None),
            oco_leg_priced("b", Some(4990.0), None),
        ]);
        assert_eq!(request.window_name, Vec::<String>::new());

        let request = oco_request(vec![
            oco_leg_priced("a", Some(5000.0), None),
            oco_leg_priced("b", Some(4990.0), None).window_name("chart"),
        ]);
        assert_eq!(
            request.window_name,
            vec![String::new(), "chart".to_string()],
            "an unnamed leg keeps its slot once any leg is named"
        );
    }

    #[test]
    fn oco_request_carries_the_group_cancel_timing() {
        let legs = || {
            vec![
                oco_leg_priced("a", Some(5000.0), None),
                oco_leg_priced("b", Some(4990.0), None),
            ]
        };

        let request = oco_request(legs());
        assert_eq!(request.cancel_at_ssboe, None);
        assert_eq!(request.cancel_at_usecs, None);
        assert_eq!(request.cancel_after_secs, None);

        let request = oco_request_with_timing(
            legs(),
            RithmicOcoOrder::new()
                .cancel_at(1_700_000_000, 500)
                .cancel_after_secs(120)
                .cancel_timing(),
        );
        assert_eq!(request.cancel_at_ssboe, Some(1_700_000_000));
        assert_eq!(request.cancel_at_usecs, Some(500));
        assert_eq!(request.cancel_after_secs, Some(120));
    }

    #[test]
    fn order_request_carries_the_requested_order_placement() {
        let mut api = RithmicSenderApi::new(&test_config());
        let mut order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price_type: OrderType::Market,
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_new_order::OrderPlacement::Auto as i32)
        );

        order.manual_or_auto = ManualOrAutoEntry::Manual;
        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_new_order::OrderPlacement::Manual as i32)
        );
    }

    #[test]
    fn bracket_request_carries_the_requested_order_placement_and_trader_user_type() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) =
            api.request_bracket_order(advanced_bracket(), &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_bracket_order::OrderPlacement::Auto as i32)
        );
        assert_eq!(
            request.user_type,
            Some(request_bracket_order::UserType::Trader as i32)
        );

        let mut order = advanced_bracket();
        order.manual_or_auto = ManualOrAutoEntry::Manual;
        let (buf, _) = api.request_bracket_order(order, &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_bracket_order::OrderPlacement::Manual as i32)
        );
    }

    #[test]
    fn oco_request_carries_each_legs_order_placement() {
        let mut leg_manual = oco_leg_priced("a", Some(5000.0), None);
        leg_manual.manual_or_auto = ManualOrAutoEntry::Manual;

        let request = oco_request(vec![leg_manual, oco_leg_priced("b", Some(4990.0), None)]);

        assert_eq!(
            request.manual_or_auto,
            vec![
                request_oco_order::OrderPlacement::Manual as i32,
                request_oco_order::OrderPlacement::Auto as i32,
            ]
        );
    }

    #[test]
    fn cancel_all_orders_encodes_the_requested_placement() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_cancel_all_orders(
            &RithmicCancelAllOrders::new().manual_or_auto(ManualOrAutoEntry::Auto),
            &default_account(),
            None,
        );
        let request: RequestCancelAllOrders = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_cancel_all_orders::OrderPlacement::Auto as i32)
        );
        assert_eq!(
            request.user_type,
            Some(request_cancel_all_orders::UserType::Trader as i32)
        );

        let (buf, _) = api.request_cancel_all_orders(
            &RithmicCancelAllOrders::new().manual_or_auto(ManualOrAutoEntry::Manual),
            &default_account(),
            None,
        );
        let request: RequestCancelAllOrders = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_cancel_all_orders::OrderPlacement::Manual as i32)
        );
    }

    #[test]
    fn cancel_and_modify_requests_carry_the_requested_order_placement() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_cancel_order(
            &RithmicCancelOrder::new()
                .id("basket-1")
                .manual_or_auto(ManualOrAutoEntry::Manual)
                .build()
                .expect("valid cancellation"),
            &default_account(),
        );
        let request: RequestCancelOrder = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_cancel_order::OrderPlacement::Manual as i32)
        );

        let (buf, _) = api.request_modify_order(
            &RithmicModifyOrder::new()
                .id("basket-1")
                .symbol("ESM6")
                .exchange("CME")
                .quantity(1)
                .price(5000.0)
                .price_type(OrderType::Limit)
                .manual_or_auto(ManualOrAutoEntry::Manual)
                .build()
                .expect("valid modification"),
            &default_account(),
        );
        let request: RequestModifyOrder = decode_request(&buf);
        assert_eq!(
            request.manual_or_auto,
            Some(request_modify_order::OrderPlacement::Manual as i32)
        );
    }

    #[test]
    fn typed_user_type_matches_the_numeric_value_it_replaced() {
        // The three request modules that carry `user_type` all number Trader 3,
        // which is what the previous shared constant sent. `response_login_info`
        // numbers from Admin = 0 and is not interchangeable with them.
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_account_rms_info(&default_account(), None);
        let request: RequestAccountRmsInfo = decode_request(&buf);
        assert_eq!(request.user_type, Some(3));
        assert_eq!(
            request.user_type,
            Some(request_account_rms_info::UserType::Trader as i32)
        );

        let (buf, _) =
            api.request_bracket_order(advanced_bracket(), &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(request.user_type, Some(3));

        let (buf, _) = api.request_cancel_all_orders(
            &RithmicCancelAllOrders::new().manual_or_auto(ManualOrAutoEntry::Auto),
            &default_account(),
            None,
        );
        let request: RequestCancelAllOrders = decode_request(&buf);
        assert_eq!(request.user_type, Some(3));
    }

    /// `OrderSide` reaches the wire through three hand-written `From` impls, one
    /// per request module. The OCO one is read back by the mixed-group test; these
    /// two are the ones where a swapped arm would send a buy as a sell without
    /// anything else noticing.
    #[test]
    fn the_side_survives_the_trip_to_the_wire_on_both_requests() {
        let mut api = RithmicSenderApi::new(&test_config());

        for (side, expected) in [
            (OrderSide::Buy, request_new_order::TransactionType::Buy),
            (OrderSide::Sell, request_new_order::TransactionType::Sell),
        ] {
            let order = RithmicOrder::new()
                .symbol("ESM6")
                .exchange("CME")
                .quantity(1)
                .transaction_type(side)
                .price_type(OrderType::Market)
                .build()
                .expect("valid order");

            let (buf, _) = api.request_order(&order, &default_account(), "globex");
            let request: RequestNewOrder = decode_request(&buf);
            assert_eq!(request.transaction_type, Some(expected as i32), "{side}");
        }

        for (side, expected) in [
            (OrderSide::Buy, request_bracket_order::TransactionType::Buy),
            (
                OrderSide::Sell,
                request_bracket_order::TransactionType::Sell,
            ),
        ] {
            let bracket = RithmicBracketOrder::new()
                .symbol("ESM6")
                .exchange("CME")
                .quantity(1)
                .action(side)
                .price_type(OrderType::Market)
                .build()
                .expect("valid bracket");

            let (buf, _) = api.request_bracket_order(bracket, &default_account(), None, "globex");
            let request: RequestBracketOrder = decode_request(&buf);
            assert_eq!(request.transaction_type, Some(expected as i32), "{side}");
        }
    }

    #[test]
    fn exit_position_request_encodes_the_requested_placement() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_exit_position(
            &RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("CME")
                .manual_or_auto(ManualOrAutoEntry::Manual)
                .build()
                .expect("valid exit"),
            &default_account(),
        );
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(
            request.manual_or_auto,
            Some(request_exit_position::OrderPlacement::Manual as i32)
        );
    }

    /// With neither symbol nor exchange, both fields stay off the wire — the
    /// absent pair is how template 3504 spells "flatten the whole account".
    #[test]
    fn exit_position_request_omits_the_instrument_for_an_account_wide_exit() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_exit_position(
            &RithmicExitPosition::new().build().expect("valid exit"),
            &default_account(),
        );
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(request.symbol, None);
        assert_eq!(request.exchange, None);

        let (buf, _) = api.request_exit_position(
            &RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("CME")
                .build()
                .expect("valid exit"),
            &default_account(),
        );
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(request.symbol.as_deref(), Some("ESM6"));
        assert_eq!(request.exchange.as_deref(), Some("CME"));
    }

    /// The same omission contract as the new-order path: an unset trigger
    /// price stays off the wire on bracket and modify requests too.
    #[test]
    fn bracket_and_modify_requests_omit_an_unset_if_touched_price() {
        let mut api = RithmicSenderApi::new(&test_config());
        let trigger = crate::api::RithmicIfTouchedTrigger::new()
            .symbol("NQM6")
            .exchange("CME");

        let bracket = RithmicBracketOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            if_touched: Some(trigger.clone()),
            ..RithmicBracketOrder::default()
        };
        let (buf, _) = api.request_bracket_order(bracket, &default_account(), None, "globex");
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_price, None);

        let modification = RithmicModifyOrder::new()
            .id("b")
            .symbol("ESM6")
            .exchange("CME")
            .quantity(1)
            .price(5005.0)
            .price_type(OrderType::Limit)
            .if_touched(trigger);
        let (buf, _) = api.request_modify_order(&modification, &default_account());
        let request: RequestModifyOrder = decode_request(&buf);
        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_price, None);
    }

    /// The attribution is always stated. An exit the caller did not attribute is
    /// `Auto` like every other command, not an omitted field the server fills in.
    #[test]
    fn exit_position_request_always_states_a_placement() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_exit_position(
            &RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("CME")
                .build()
                .expect("valid exit"),
            &default_account(),
        );
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(
            request.manual_or_auto,
            Some(request_exit_position::OrderPlacement::Auto as i32)
        );
    }

    #[test]
    fn exit_position_request_carries_the_window_name_and_trading_algorithm() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_exit_position(
            &RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("CME")
                .window_name("chart")
                .trading_algorithm("mean-reversion")
                .build()
                .expect("valid exit"),
            &default_account(),
        );
        let request: RequestExitPosition = decode_request(&buf);

        assert_eq!(request.window_name.as_deref(), Some("chart"));
        assert_eq!(request.trading_algorithm.as_deref(), Some("mean-reversion"));
    }

    #[test]
    fn cancel_and_bracket_requests_carry_the_window_name() {
        let mut api = RithmicSenderApi::new(&test_config());

        let (buf, _) = api.request_cancel_order(
            &RithmicCancelOrder::new()
                .id("123456")
                .window_name("chart")
                .build()
                .expect("valid cancellation"),
            &default_account(),
        );
        let request: RequestCancelOrder = decode_request(&buf);
        assert_eq!(request.window_name.as_deref(), Some("chart"));

        let (buf, _) = api.request_bracket_order(
            RithmicBracketOrder::new()
                .symbol("ESM6")
                .exchange("CME")
                .quantity(1)
                .price_type(OrderType::Market)
                .window_name("chart")
                .build()
                .expect("valid bracket"),
            &default_account(),
            None,
            "globex",
        );
        let request: RequestBracketOrder = decode_request(&buf);
        assert_eq!(request.window_name.as_deref(), Some("chart"));
    }

    #[test]
    fn order_request_encodes_the_timing_and_window_fields() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            window_name: Some("strategy-1".to_string()),
            release_at_ssboe: Some(35900),
            release_at_usecs: Some(125000),
            cancel_at_ssboe: Some(37000),
            cancel_at_usecs: Some(750000),
            cancel_after_secs: Some(120),
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.window_name.as_deref(), Some("strategy-1"));
        assert_eq!(request.release_at_ssboe, Some(35900));
        assert_eq!(request.release_at_usecs, Some(125000));
        assert_eq!(request.cancel_at_ssboe, Some(37000));
        assert_eq!(request.cancel_at_usecs, Some(750000));
        assert_eq!(request.cancel_after_secs, Some(120));
    }

    #[test]
    fn order_request_encodes_the_if_touched_group() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            if_touched: Some(
                crate::api::RithmicIfTouchedTrigger::new()
                    .symbol("NQM6")
                    .exchange("CME")
                    .condition(OrderCondition::GreaterThanEqualTo)
                    .price_field(OrderPriceField::TradePrice)
                    .price(18250.5),
            ),
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_exchange.as_deref(), Some("CME"));
        assert_eq!(
            request.if_touched_condition,
            Some(request_new_order::Condition::GreaterThanEqualTo as i32)
        );
        assert_eq!(
            request.if_touched_price_field,
            Some(request_new_order::PriceField::TradePrice as i32)
        );
        assert_eq!(request.if_touched_price, Some(18250.5));
    }

    /// A trigger that skipped `build()` and never set a price must omit
    /// `if_touched_price` from the wire — sent as `0.0`, the default
    /// `GreaterThanEqualTo`/`TradePrice` condition would release the order
    /// immediately.
    #[test]
    fn order_request_omits_an_unset_if_touched_price() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            if_touched: Some(
                crate::api::RithmicIfTouchedTrigger::new()
                    .symbol("NQM6")
                    .exchange("CME"),
            ),
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.if_touched_symbol.as_deref(), Some("NQM6"));
        assert_eq!(request.if_touched_price, None);
    }

    #[test]
    fn order_request_omits_the_optional_groups_when_unset() {
        let mut api = RithmicSenderApi::new(&test_config());
        let order = RithmicOrder {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price: Some(5000.0),
            ..RithmicOrder::default()
        };

        let (buf, _) = api.request_order(&order, &default_account(), "globex");
        let request: RequestNewOrder = decode_request(&buf);

        assert_eq!(request.window_name, None);
        assert_eq!(request.release_at_ssboe, None);
        assert_eq!(request.release_at_usecs, None);
        assert_eq!(request.cancel_at_ssboe, None);
        assert_eq!(request.cancel_at_usecs, None);
        assert_eq!(request.cancel_after_secs, None);
        assert_eq!(request.if_touched_symbol, None);
        assert_eq!(request.if_touched_exchange, None);
        assert_eq!(request.if_touched_condition, None);
        assert_eq!(request.if_touched_price_field, None);
        assert_eq!(request.if_touched_price, None);
    }
}
