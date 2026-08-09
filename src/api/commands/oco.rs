//! OCO (One-Cancels-Other) groups and the legs they hold.

use super::triggers::TrailingStop;
use super::validate_instrument;

use crate::{
    error::RithmicError,
    types::{ManualOrAutoEntry, OrderSide, OrderType, TimeInForce},
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a leg does nothing until added to an OCO group"]
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
    /// Order type. Template 328 declares no if-touched price type, so
    /// [`OrderType::MarketIfTouched`] and [`OrderType::LimitIfTouched`] are
    /// rejected on an OCO leg.
    pub price_type: OrderType,
    /// Your identifier for this order
    pub user_tag: String,
    /// Optional trailing stop configuration for this leg
    pub trailing_stop: Option<TrailingStop>,
    /// Route to send on. `None` uses the route the server published for this
    /// leg's exchange.
    pub trade_route: Option<String>,
    /// Whether the leg was placed by a human or automatically.
    pub manual_or_auto: ManualOrAutoEntry,
    /// Originating window name reported to Rithmic. `window_name` is repeated
    /// on `RequestOcoOrder`, so it is per-leg like the other leg fields.
    pub window_name: Option<String>,
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
        self.trailing_stop(
            TrailingStop::new()
                .trail_by_ticks(trail_by_ticks)
                .trail_by_price_id(trail_by_price_id),
        )
    }

    /// Route to send on, overriding the route published for the exchange.
    pub fn trade_route(mut self, trade_route: impl Into<String>) -> Self {
        self.trade_route = Some(trade_route.into());
        self
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: ManualOrAutoEntry) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Window name to report this leg under.
    pub fn window_name(mut self, window_name: impl Into<String>) -> Self {
        self.window_name = Some(window_name.into());
        self
    }

    /// Requires a symbol, an exchange, a positive quantity, and the prices
    /// the [`Self::price_type`] needs. An OCO leg cannot be if-touched. An
    /// embedded trailing stop is not re-validated.
    pub fn validate(&self) -> Result<(), RithmicError> {
        validate_instrument(&self.symbol, &self.exchange, self.quantity)?;

        if matches!(
            self.price_type,
            OrderType::MarketIfTouched | OrderType::LimitIfTouched
        ) {
            return Err(RithmicError::InvalidArgument(format!(
                "price_type {} is not available on an OCO leg",
                self.price_type.as_str_name()
            )));
        }

        super::require_prices(self.price_type, self.price, self.trigger_price)
    }

    /// Requires an instrument and the prices the price type needs.
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "an order does nothing until passed to a plant handle"]
pub struct RithmicOcoOrder {
    /// The legs of the group, in the order they are sent.
    pub legs: Vec<RithmicOcoOrderLeg>,
    /// Cancel the group at this second-since-beginning-of-epoch value.
    pub cancel_at_ssboe: Option<i32>,
    /// Microsecond component for `cancel_at_ssboe`.
    pub cancel_at_usecs: Option<i32>,
    /// Cancel the group after this many seconds.
    pub cancel_after_secs: Option<i32>,
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

    /// Cancel the group at this second-since-beginning-of-epoch value.
    pub fn cancel_at_ssboe(mut self, ssboe: i32) -> Self {
        self.cancel_at_ssboe = Some(ssboe);
        self
    }

    /// Microsecond component of the cancel time.
    pub fn cancel_at_usecs(mut self, usecs: i32) -> Self {
        self.cancel_at_usecs = Some(usecs);
        self
    }

    /// Set both halves of the cancel time.
    pub fn cancel_at(self, ssboe: i32, usecs: i32) -> Self {
        self.cancel_at_ssboe(ssboe).cancel_at_usecs(usecs)
    }

    /// Cancel the group after this many seconds.
    pub fn cancel_after_secs(mut self, secs: i32) -> Self {
        self.cancel_after_secs = Some(secs);
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

    /// Copy out the group-level timing. The plant hands the legs to the route
    /// cache before it reaches the sender, so the timing has to travel
    /// separately.
    pub(crate) fn cancel_timing(&self) -> OcoCancelTiming {
        OcoCancelTiming {
            cancel_at_ssboe: self.cancel_at_ssboe,
            cancel_at_usecs: self.cancel_at_usecs,
            cancel_after_secs: self.cancel_after_secs,
        }
    }
}

/// The group-level cancel timing on an OCO order, carried on its own so the
/// three same-typed fields cannot be swapped at a call site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OcoCancelTiming {
    /// Cancel the group at this second-since-beginning-of-epoch value.
    pub(crate) cancel_at_ssboe: Option<i32>,
    /// Microsecond component for `cancel_at_ssboe`.
    pub(crate) cancel_at_usecs: Option<i32>,
    /// Cancel the group after this many seconds.
    pub(crate) cancel_after_secs: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(price_type: OrderType) -> RithmicOcoOrderLeg {
        RithmicOcoOrderLeg {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            quantity: 1,
            price_type,
            ..Default::default()
        }
    }

    #[test]
    fn an_oco_leg_validates_on_the_same_rules() {
        let mut leg = leg(OrderType::Limit);

        assert!(leg.validate().is_err());

        leg.price = Some(5000.0);
        assert!(leg.validate().is_ok());
    }

    /// The one place a crate-owned enum is wider than the message it targets.
    #[test]
    fn an_oco_leg_rejects_the_if_touched_price_types() {
        let leg = RithmicOcoOrderLeg {
            price: Some(5000.0),
            trigger_price: Some(5000.0),
            ..leg(OrderType::LimitIfTouched)
        };

        let err = leg.validate().unwrap_err().to_string();
        assert!(err.contains("LIMIT_IF_TOUCHED"), "{err}");
        assert!(err.contains("is not available on an OCO leg"), "{err}");
    }

    /// The group checks its legs, not how many of them there are.
    #[test]
    fn an_oco_order_validates_each_leg_but_not_the_count() {
        let ok = leg(OrderType::Market);

        assert!(RithmicOcoOrder::default().validate().is_ok());
        assert!(
            RithmicOcoOrder {
                legs: vec![ok.clone()],
                ..Default::default()
            }
            .validate()
            .is_ok()
        );

        let bad = leg(OrderType::Limit);
        assert!(
            RithmicOcoOrder {
                legs: vec![ok, bad],
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}
