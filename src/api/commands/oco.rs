//! OCO (One-Cancels-Other) groups and the legs they hold.

use super::trailing::TrailingStop;
use crate::{
    error::RithmicError,
    types::{OrderOrigin, OrderSide, OrderType, TimeInForce},
};

/// One leg of an OCO (One-Cancels-Other) order group.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOcoOrderLeg};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let take_profit = RithmicOcoOrderLeg::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::Limit)
///     .price(5020.0)
///     .user_tag("take-profit")
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicOcoOrderLeg {
    /// Trading symbol (e.g., "ESH6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Leg price. A market leg does not need one.
    pub price: Option<f64>,
    /// Trigger price. Only a stop leg needs one.
    pub trigger_price: Option<f64>,
    /// Buy or Sell
    pub transaction_type: OrderSide,
    /// Order duration
    pub duration: TimeInForce,
    /// Order type. [`OrderType::MarketIfTouched`] and [`OrderType::LimitIfTouched`] are rejected on an OCO leg.
    pub price_type: OrderType,
    /// Your identifier for this order
    pub user_tag: String,
    /// Optional trailing stop configuration for this leg
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for this
    /// leg's exchange.
    pub trade_route: Option<String>,
    /// Whether the leg was placed by a human or automatically.
    pub manual_or_auto: OrderOrigin,
}

impl RithmicOcoOrderLeg {
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Instrument symbol.
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    /// Exchange the instrument trades on.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Number of contracts on this leg.
    pub fn quantity(mut self, quantity: i32) -> Self {
        self.quantity = quantity;
        self
    }

    /// Buy or sell.
    pub fn transaction_type(mut self, transaction_type: OrderSide) -> Self {
        self.transaction_type = transaction_type;
        self
    }

    /// Market, limit, or stop.
    pub fn price_type(mut self, price_type: OrderType) -> Self {
        self.price_type = price_type;
        self
    }

    /// Leg price.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Trigger price for stop order types.
    pub fn trigger_price(mut self, trigger_price: f64) -> Self {
        self.trigger_price = Some(trigger_price);
        self
    }

    /// How long the leg stays working.
    pub fn duration(mut self, duration: TimeInForce) -> Self {
        self.duration = duration;
        self
    }

    /// Your identifier for this leg.
    pub fn user_tag(mut self, user_tag: impl Into<String>) -> Self {
        self.user_tag = user_tag.into();
        self
    }

    /// Trailing stop configuration for this leg.
    pub fn trailing_stop(mut self, trailing_stop: TrailingStop) -> Self {
        self.trailing_stop = Some(trailing_stop);
        self
    }

    /// Trail by `trail_by_ticks` against Rithmic's `trail_by_price_id`.
    pub fn trailing_stop_by(self, trail_by_ticks: i32, trail_by_price_id: i32) -> Self {
        self.trailing_stop(TrailingStop::new(trail_by_ticks, trail_by_price_id))
    }

    /// Route to send on, overriding the route published for the exchange.
    pub fn trade_route(mut self, trade_route: impl Into<String>) -> Self {
        self.trade_route = Some(trade_route.into());
        self
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: OrderOrigin) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Check the leg carries the prices its [`Self::price_type`] requires:
    /// `Limit` and `StopLimit` need [`Self::price`]; `StopMarket` and
    /// `StopLimit` need [`Self::trigger_price`]. `Market` needs neither.
    pub fn validate(&self) -> Result<(), RithmicError> {
        let order_type = self.price_type.as_str_name();

        let (needs_price, needs_trigger) = match self.price_type {
            OrderType::Market => (false, false),
            OrderType::Limit => (true, false),
            OrderType::StopMarket => (false, true),
            OrderType::StopLimit => (true, true),
            OrderType::MarketIfTouched | OrderType::LimitIfTouched => {
                return Err(RithmicError::InvalidArgument(format!(
                    "price_type {order_type} is not available on an OCO leg"
                )));
            }
        };

        if needs_price && self.price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "price is required for a {order_type} order"
            )));
        }

        if needs_trigger && self.trigger_price.is_none() {
            return Err(RithmicError::InvalidArgument(format!(
                "trigger_price is required for a {order_type} order"
            )));
        }

        Ok(())
    }

    /// Validate and return the leg.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

/// A group of OCO legs: when one fills, the others are cancelled.
///
/// # Example
///
/// ```
/// use rithmic_rs::{OrderSide, OrderType, RithmicOcoOrder, RithmicOcoOrderLeg};
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let take_profit = RithmicOcoOrderLeg::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::Limit)
///     .price(5020.0)
///     .build()?;
/// let stop_loss = RithmicOcoOrderLeg::new()
///     .symbol("ESH6")
///     .exchange("CME")
///     .quantity(1)
///     .transaction_type(OrderSide::Sell)
///     .price_type(OrderType::StopMarket)
///     .trigger_price(4980.0)
///     .build()?;
///
/// let order = RithmicOcoOrder::new().legs([take_profit, stop_loss]).build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicOcoOrder {
    /// The legs of the group, in the order they are sent.
    pub legs: Vec<RithmicOcoOrderLeg>,
}

impl RithmicOcoOrder {
    /// Start from the defaults, with no legs.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one more leg.
    pub fn leg(mut self, leg: RithmicOcoOrderLeg) -> Self {
        self.legs.push(leg);
        self
    }

    /// Append several more legs.
    pub fn legs(mut self, legs: impl IntoIterator<Item = RithmicOcoOrderLeg>) -> Self {
        self.legs.extend(legs);
        self
    }

    /// Check every leg validates.
    pub fn validate(&self) -> Result<(), RithmicError> {
        for leg in &self.legs {
            leg.validate()?;
        }

        Ok(())
    }

    /// Validate and return the group. The leg count is not checked here; the
    /// handle refuses a group shorter than two legs.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg() -> RithmicOcoOrderLeg {
        RithmicOcoOrderLeg::new()
            .symbol("ESH6")
            .exchange("CME")
            .quantity(1)
            .transaction_type(OrderSide::Sell)
            .price_type(OrderType::Market)
    }

    #[test]
    fn an_oco_leg_validates_on_the_same_rules() {
        let mut leg = RithmicOcoOrderLeg {
            price_type: OrderType::Limit,
            ..Default::default()
        };

        assert!(leg.validate().is_err());

        leg.price = Some(5000.0);
        assert!(leg.validate().is_ok());
    }

    /// The one place a crate-owned enum is wider than the message it targets.
    #[test]
    fn an_oco_leg_rejects_the_if_touched_price_types() {
        let leg = RithmicOcoOrderLeg {
            price_type: OrderType::LimitIfTouched,
            price: Some(5000.0),
            trigger_price: Some(5000.0),
            ..Default::default()
        };

        let err = leg.validate().unwrap_err().to_string();
        assert!(err.contains("LIMIT_IF_TOUCHED"), "{err}");
        assert!(err.contains("is not available on an OCO leg"), "{err}");
    }

    #[test]
    fn an_oco_leg_build_rejects_the_if_touched_price_types() {
        let err = leg()
            .price_type(OrderType::MarketIfTouched)
            .trigger_price(4980.0)
            .build()
            .unwrap_err()
            .to_string();

        assert!(err.contains("is not available on an OCO leg"), "{err}");
    }

    /// The group checks its legs, not how many of them there are.
    #[test]
    fn an_oco_order_validates_each_leg_but_not_the_count() {
        let ok = RithmicOcoOrderLeg {
            price_type: OrderType::Market,
            ..Default::default()
        };

        assert!(RithmicOcoOrder { legs: Vec::new() }.validate().is_ok());
        assert!(
            RithmicOcoOrder {
                legs: vec![ok.clone()]
            }
            .validate()
            .is_ok()
        );

        let bad = RithmicOcoOrderLeg {
            price_type: OrderType::Limit,
            ..Default::default()
        };
        assert!(
            RithmicOcoOrder {
                legs: vec![ok, bad]
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn an_oco_order_collects_its_legs() {
        let tagged = |tag: &str| leg().user_tag(tag).build().unwrap();

        let order = RithmicOcoOrder::new()
            .legs([tagged("a"), tagged("b")])
            .leg(tagged("c"))
            .legs([tagged("d"), tagged("e")])
            .build()
            .unwrap();

        assert_eq!(order.legs.len(), 5);
    }

    #[test]
    fn an_oco_order_rejects_an_invalid_leg() {
        let good = leg().build().unwrap();
        let bad = leg().price_type(OrderType::MarketIfTouched);

        assert!(RithmicOcoOrder::new().legs([good, bad]).build().is_err());
    }
}
