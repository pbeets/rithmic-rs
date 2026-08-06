use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::{
    api::{commands::RithmicOcoOrderLeg, receiver_api::RithmicResponse},
    error::RithmicError,
    rti::{TradeRoute, messages::RithmicMessage},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CachedTradeRoute {
    pub(crate) trade_route: String,
    pub(crate) is_default: Option<bool>,
}

/// The routes published on this connection, keyed by exchange.
///
/// `RequestTradeRoutes` (310) carries no account, so every route here belongs to
/// the connection's login and the exchange alone identifies one.
#[derive(Clone, Debug, Default)]
pub(crate) struct TradeRouteCache {
    routes: HashMap<String, CachedTradeRoute>,
}

impl TradeRouteCache {
    /// Take the route a `ResponseTradeRoutes` frame carries, returning whether
    /// it cached anything.
    ///
    /// Rejected frames are skipped, since they describe no route we could send
    /// an order on. Every other template is ignored.
    pub(crate) fn record_response(&mut self, response: &RithmicResponse) -> bool {
        if response.error.is_some() {
            return false;
        }

        let RithmicMessage::ResponseTradeRoutes(route) = &response.message else {
            return false;
        };

        self.record(
            route.exchange.as_deref(),
            route.trade_route.as_deref(),
            route.is_default,
        )
    }

    /// Apply a `TradeRoute` (350) update, replacing any route held for its exchange.
    pub(crate) fn record_update(&mut self, update: &TradeRoute) {
        let (Some(exchange), Some(trade_route)) =
            (update.exchange.as_deref(), update.trade_route.as_deref())
        else {
            return;
        };

        let held = self.routes.insert(
            exchange.to_string(),
            CachedTradeRoute {
                trade_route: trade_route.to_string(),
                is_default: update.is_default,
            },
        );

        if held.is_none_or(|held| held.trade_route != trade_route) {
            info!("order_plant: trade route {exchange} -> {trade_route:?}");
        }
    }

    /// Store one route from login, returning whether it changed anything.
    ///
    /// Login sends a frame per route and two can name the same exchange, so the
    /// one the server marked default wins. Frames missing an exchange or a route
    /// are dropped.
    pub(crate) fn record(
        &mut self,
        exchange: Option<&str>,
        trade_route: Option<&str>,
        is_default: Option<bool>,
    ) -> bool {
        let (Some(exchange), Some(trade_route)) = (exchange, trade_route) else {
            return false;
        };

        let entry = CachedTradeRoute {
            trade_route: trade_route.to_string(),
            is_default,
        };

        let changed = match self.routes.get_mut(exchange) {
            // The route we already hold, so take whatever detail came with it.
            Some(held) if held.trade_route == entry.trade_route => {
                if *held == entry {
                    false
                } else {
                    *held = entry;
                    true
                }
            }
            Some(held) if held.is_default != Some(true) && entry.is_default == Some(true) => {
                *held = entry;
                true
            }
            Some(_) => false,
            None => {
                self.routes.insert(exchange.to_string(), entry);
                true
            }
        };

        if changed {
            info!("order_plant: trade route {exchange} -> {trade_route:?}");
        }

        changed
    }

    /// The route an order for `exchange` is sent on.
    ///
    /// `override_route` is taken as given, so an order can name a route the
    /// server never published. Otherwise this is the route recorded for the
    /// exchange. With neither, the caller gets
    /// [`RithmicError::NoTradeRoute`] and must not send the order.
    pub(crate) fn resolve(
        &self,
        override_route: Option<&str>,
        exchange: &str,
    ) -> Result<String, RithmicError> {
        if let Some(route) = override_route {
            debug!("order_plant: {exchange} order on caller-supplied route {route:?}");

            return Ok(route.to_string());
        }

        if let Some(route) = self.routes.get(exchange) {
            debug!(
                "order_plant: {exchange} order on route {:?}",
                route.trade_route,
            );

            return Ok(route.trade_route.clone());
        }

        let cached = self.exchanges();

        warn!("order_plant: no trade route for {exchange}; routed exchanges: {cached:?}");

        Err(RithmicError::NoTradeRoute {
            exchange: exchange.to_string(),
            cached,
        })
    }

    /// Pair every OCO leg with its own route, so a group spanning exchanges
    /// works. If any leg has no route the whole group fails and nothing is sent.
    pub(crate) fn resolve_legs(
        &self,
        legs: Vec<RithmicOcoOrderLeg>,
    ) -> Result<Vec<(RithmicOcoOrderLeg, String)>, RithmicError> {
        legs.into_iter()
            .map(|leg| {
                let route = self.resolve(leg.trade_route.as_deref(), &leg.exchange)?;

                Ok((leg, route))
            })
            .collect()
    }

    /// The exchanges that hold a route, sorted so errors and log lines read the
    /// same every run.
    fn exchanges(&self) -> Vec<String> {
        let mut exchanges: Vec<String> = self.routes.keys().cloned().collect();

        exchanges.sort();
        exchanges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderSide, OrderType, TimeInForce};

    fn cache_with(exchange: &str, trade_route: &str) -> TradeRouteCache {
        let mut cache = TradeRouteCache::default();

        cache.record(Some(exchange), Some(trade_route), None);
        cache
    }

    /// A `TradeRoute` (350) update naming a route the server moved to.
    fn trade_route_update(exchange: &str, trade_route: &str) -> TradeRoute {
        TradeRoute {
            template_id: 350,
            exchange: Some(exchange.to_string()),
            trade_route: Some(trade_route.to_string()),
            is_default: Some(true),
            ..Default::default()
        }
    }

    /// The route for an exchange, or `None` where an order would be refused.
    fn routed(cache: &TradeRouteCache, exchange: &str) -> Option<String> {
        cache.resolve(None, exchange).ok()
    }

    fn oco_leg(exchange: &str, trade_route: Option<&str>) -> RithmicOcoOrderLeg {
        let mut leg = RithmicOcoOrderLeg::new()
            .symbol("ESM6")
            .exchange(exchange)
            .quantity(1)
            .transaction_type(OrderSide::Buy)
            .price_type(OrderType::Limit)
            .price(5000.0)
            .duration(TimeInForce::Day)
            .user_tag("leg");
        if let Some(trade_route) = trade_route {
            leg = leg.trade_route(trade_route);
        }
        leg.build().expect("valid leg")
    }

    fn response(message: RithmicMessage, error: Option<RithmicError>) -> RithmicResponse {
        RithmicResponse {
            request_id: "1".to_string(),
            message,
            is_update: false,
            has_more: false,
            multi_response: false,
            error,
            source: "order_plant".to_string(),
        }
    }

    #[test]
    fn record_response_takes_the_route_off_a_311_frame() {
        let mut cache = TradeRouteCache::default();

        cache.record_response(&response(
            RithmicMessage::ResponseTradeRoutes(crate::rti::ResponseTradeRoutes {
                template_id: 311,
                exchange: Some("CBOT".to_string()),
                trade_route: Some("cbot-route".to_string()),
                ..Default::default()
            }),
            None,
        ));

        assert_eq!(routed(&cache, "CBOT").as_deref(), Some("cbot-route"));
    }

    /// 350 reaches subscribers, and the login path never takes one: an update
    /// moves a route only where a caller asked for it, through `record_update`.
    #[test]
    fn record_response_ignores_a_350_update() {
        let mut cache = cache_with("CME", "globex");

        cache.record_response(&response(
            RithmicMessage::TradeRoute(trade_route_update("CME", "moved")),
            None,
        ));

        assert_eq!(routed(&cache, "CME").as_deref(), Some("globex"));
    }

    /// What a caller handing back an update buys them: the orders that follow
    /// take the route the server moved to.
    #[test]
    fn record_update_takes_the_route_off_a_350_frame() {
        let mut cache = cache_with("CME", "globex");

        cache.record_update(&trade_route_update("CME", "moved"));

        assert_eq!(routed(&cache, "CME").as_deref(), Some("moved"));
    }

    /// A rejection describes no route, so taking one from it would be worse
    /// than having no route at all.
    #[test]
    fn record_response_skips_a_rejected_frame() {
        let mut cache = TradeRouteCache::default();

        cache.record_response(&response(
            RithmicMessage::ResponseTradeRoutes(crate::rti::ResponseTradeRoutes {
                template_id: 311,
                exchange: Some("CBOT".to_string()),
                trade_route: Some("cbot-route".to_string()),
                ..Default::default()
            }),
            Some(RithmicError::ProtocolError("denied".to_string())),
        ));

        assert_eq!(routed(&cache, "CBOT"), None);
        assert!(cache.exchanges().is_empty());
    }

    #[test]
    fn resolve_returns_the_route_recorded_for_the_exchange() {
        let cache = cache_with("CME", "globex");

        assert_eq!(routed(&cache, "CME").as_deref(), Some("globex"));
        assert_eq!(routed(&cache, "CBOT"), None);
    }

    #[test]
    fn resolve_prefers_the_per_order_override() {
        let cache = cache_with("CME", "globex");

        assert_eq!(cache.resolve(Some("my-route"), "CME").unwrap(), "my-route");
    }

    #[test]
    fn resolve_errors_with_the_exchanges_that_do_have_a_route() {
        // Recorded NYMEX first, so the reported order is the sorted one rather
        // than the order the routes arrived in.
        let mut cache = cache_with("NYMEX", "nymex-route");

        cache.record(Some("CME"), Some("globex"), None);

        let err = cache
            .resolve(None, "CBOT")
            .expect_err("an uncached exchange must not resolve");

        assert_eq!(
            err,
            RithmicError::NoTradeRoute {
                exchange: "CBOT".to_string(),
                cached: vec!["CME".to_string(), "NYMEX".to_string()],
            }
        );
    }

    #[test]
    fn resolve_legs_routes_each_leg_by_its_own_exchange() {
        let mut cache = cache_with("CME", "globex");

        cache.record(Some("NYMEX"), Some("nymex-route"), None);

        let routed = cache
            .resolve_legs(vec![
                oco_leg("CME", None),
                oco_leg("NYMEX", None),
                oco_leg("CME", Some("my-route")),
            ])
            .unwrap();

        let routes: Vec<&str> = routed.iter().map(|(_, route)| route.as_str()).collect();

        assert_eq!(routes, vec!["globex", "nymex-route", "my-route"]);
    }

    #[test]
    fn resolve_legs_fails_the_group_on_an_unroutable_leg() {
        let cache = cache_with("CME", "globex");

        let err = cache
            .resolve_legs(vec![oco_leg("CME", None), oco_leg("CBOT", None)])
            .expect_err("one unroutable leg must fail the whole group");

        assert_eq!(
            err,
            RithmicError::NoTradeRoute {
                exchange: "CBOT".to_string(),
                cached: vec!["CME".to_string()],
            }
        );
    }

    #[test]
    fn a_default_route_displaces_one_the_server_never_marked_default() {
        let mut cache = cache_with("CME", "first-seen");

        assert!(cache.record(Some("CME"), Some("the-default"), Some(true)));
        assert_eq!(routed(&cache, "CME").as_deref(), Some("the-default"));
    }

    #[test]
    fn a_non_default_route_does_not_displace_the_default() {
        let mut cache = TradeRouteCache::default();

        cache.record(Some("CME"), Some("the-default"), Some(true));

        assert!(!cache.record(Some("CME"), Some("another"), None));
        assert_eq!(routed(&cache, "CME").as_deref(), Some("the-default"));
    }

    /// A caller handing back a 350 asked for that route, so it lands even where
    /// a login frame marked the route it displaces default.
    #[test]
    fn record_update_replaces_a_route_marked_default() {
        let mut cache = TradeRouteCache::default();

        cache.record(Some("CME"), Some("the-default"), Some(true));

        cache.record_update(&TradeRoute {
            template_id: 350,
            exchange: Some("CME".to_string()),
            trade_route: Some("moved".to_string()),
            is_default: None,
            ..Default::default()
        });

        assert_eq!(routed(&cache, "CME").as_deref(), Some("moved"));
    }

    #[test]
    fn the_first_route_wins_when_the_server_marks_no_default() {
        let mut cache = cache_with("CME", "first-seen");

        assert!(!cache.record(Some("CME"), Some("second-seen"), None));
        assert_eq!(routed(&cache, "CME").as_deref(), Some("first-seen"));
    }

    #[test]
    fn a_later_frame_updates_the_route_it_names() {
        let mut cache = cache_with("CME", "globex");

        assert!(
            cache.record(Some("CME"), Some("globex"), Some(true)),
            "new detail for the route we hold is a change"
        );
        assert!(
            !cache.record(Some("CME"), Some("globex"), Some(true)),
            "the same frame again is not, so it must not be logged again"
        );
        assert_eq!(routed(&cache, "CME").as_deref(), Some("globex"));
    }

    #[test]
    fn frames_describing_no_route_are_dropped() {
        let mut cache = TradeRouteCache::default();

        assert!(!cache.record(None, Some("globex"), None));
        assert!(!cache.record(Some("CME"), None, None));
        assert_eq!(routed(&cache, "CME"), None);
        assert!(cache.exchanges().is_empty());
    }
}
