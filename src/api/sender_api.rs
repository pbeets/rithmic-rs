use prost::Message;

use crate::{
    connection_info::{AccountInfo, RithmicConnectionSystem},
    rti::{
        RequestAccountList, RequestBracketOrder, RequestCancelOrder, RequestExitPosition,
        RequestHeartbeat, RequestLogin, RequestLogout, RequestMarketDataUpdate, RequestModifyOrder,
        RequestNewOrder, RequestPnLPositionSnapshot, RequestPnLPositionUpdates,
        RequestRithmicSystemInfo, RequestShowBracketStops, RequestShowBrackets, RequestShowOrders,
        RequestSubscribeForOrderUpdates, RequestSubscribeToBracketUpdates, RequestTickBarReplay,
        RequestUpdateStopBracketLevel, RequestUpdateTargetBracketLevel,
        request_account_list::UserType,
        request_bracket_order,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
        request_new_order, request_pn_l_position_updates,
        request_tick_bar_replay::{BarSubType, BarType, Direction, TimeOrder},
    },
};
use crate::rti::{RequestDepthByOrderSnapshot, RequestDepthByOrderUpdates};
use super::rithmic_command_types::RithmicBracketOrder;

pub const TRADE_ROUTE_LIVE: &str = "globex";
pub const TRADE_ROUTE_DEMO: &str = "simulator";
pub const USER_TYPE: i32 = 3;

#[derive(Debug, Clone)]
pub struct RithmicSenderApi {
    account_id: String,
    env: RithmicConnectionSystem,
    fcm_id: String,
    ib_id: String,
    message_id_counter: u64,
}

impl RithmicSenderApi {
    pub fn new(account_info: &AccountInfo) -> Self {
        RithmicSenderApi {
            account_id: account_info.account_id.clone(),
            env: account_info.env.clone(),
            fcm_id: account_info.fcm_id.clone(),
            ib_id: account_info.ib_id.clone(),
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
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestLogin {
            template_id: 10,
            template_version: Some("5.30".into()),
            user: Some(user.to_string()),
            password: Some(password.to_string()),
            app_name: Some("pede:pts".to_string()),
            app_version: Some("1".into()),
            system_name: Some(system_name.to_string()),
            infra_type: Some(infra_type.into()),
            user_msg: vec![id.clone()],
            ..RequestLogin::default()
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
        request_type: Request,
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

    #[allow(dead_code)]
    pub fn request_new_order(
        &mut self,
        exchange: &str,
        symbol: &str,
        qty: i32,
        price: f64,
        action: request_new_order::TransactionType,
        ordertype: request_new_order::PriceType,
        localid: &str,

        // optional args
        duration: Option<request_new_order::Duration>,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let trade_route = match self.env {
            RithmicConnectionSystem::Live => TRADE_ROUTE_LIVE,
            RithmicConnectionSystem::Demo => TRADE_ROUTE_DEMO,
            RithmicConnectionSystem::Test => panic!("test environment not supported"),
        };

        let req = RequestNewOrder {
            template_id: 312,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(exchange.into()),
            symbol: Some(symbol.into()),
            quantity: Some(qty),
            price: Some(price),
            transaction_type: Some(action.into()),
            price_type: Some(ordertype.into()),
            manual_or_auto: Some(2),
            duration: if let Some(d) = duration {
                Some(d.into())
            } else {
                Some(1)
            },
            user_msg: vec![id.clone()],
            user_tag: Some(localid.into()),
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
            RithmicConnectionSystem::Live => TRADE_ROUTE_LIVE,
            RithmicConnectionSystem::Demo => TRADE_ROUTE_DEMO,
            RithmicConnectionSystem::Test => panic!("test environment not supported"),
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
            quantity: Some(bracket_order.qty),
            transaction_type: Some(bracket_order.action),
            price_type: Some(bracket_order.ordertype),
            manual_or_auto: Some(2),
            duration: Some(bracket_order.duration),
            bracket_type: Some(6),
            target_quantity: vec![bracket_order.qty],
            stop_quantity: vec![bracket_order.qty],
            target_ticks: vec![bracket_order.profit_ticks],
            stop_ticks: vec![bracket_order.stop_ticks],
            price: if bracket_order.ordertype != request_bracket_order::PriceType::Market as i32 {
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
        ordertype: i32,
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
            price_type: Some(ordertype),
            quantity: Some(qty),
            price: Some(price),
            user_msg: vec![id.clone()],
            trigger_price: match ordertype {
                3 | 4 => Some(price),
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
    /// * `exchange` - The exchange of the symbol
    /// * `symbol` - The symbol to request data for
    /// * `start_index_sec` - unix seconds
    /// * `finish_index_sec` - unix seconds
    ///
    /// # Returns
    ///
    /// A tuple containing the request buffer and the message id
    pub fn request_tick_bar_replay(
        &mut self,
        exchange: String,
        symbol: String,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> (Vec<u8>, String) {
        let id = self.get_next_message_id();

        let req = RequestTickBarReplay {
            template_id: 206,
            exchange: Some(exchange),
            symbol: Some(symbol),
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
}
