//! Order enums with serde support and protobuf conversions.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use std::fmt;
use std::str::FromStr;

use crate::error::RithmicError;
use crate::rti::{
    request_bracket_order, request_cancel_all_orders, request_cancel_order, request_exit_position,
    request_modify_order, request_new_order, request_oco_order,
};

/// Buy or sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OrderSide {
    /// Buy side.
    #[default]
    Buy,
    /// Sell side.
    Sell,
}

impl OrderSide {
    /// The protobuf spelling, as `TransactionType::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

/// Error returned when parsing an invalid [`OrderSide`] string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseOrderSideError(String);

impl fmt::Display for ParseOrderSideError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid order side: '{}'", self.0)
    }
}

impl std::error::Error for ParseOrderSideError {}

impl FromStr for OrderSide {
    type Err = ParseOrderSideError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BUY" | "B" => Ok(Self::Buy),
            "SELL" | "S" => Ok(Self::Sell),
            _ => Err(ParseOrderSideError(s.to_string())),
        }
    }
}

impl From<OrderSide> for request_new_order::TransactionType {
    fn from(side: OrderSide) -> Self {
        match side {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

impl From<OrderSide> for request_bracket_order::TransactionType {
    fn from(side: OrderSide) -> Self {
        match side {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

impl From<OrderSide> for request_oco_order::TransactionType {
    fn from(side: OrderSide) -> Self {
        match side {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

/// Order price type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OrderType {
    /// Market order — executes immediately at the best available price.
    Market,
    /// Limit order — executes at the specified price or better.
    #[default]
    Limit,
    /// Stop market order — becomes a market order when the stop price is reached.
    StopMarket,
    /// Stop limit order — becomes a limit order when the stop price is reached.
    StopLimit,
    /// Market order released when the trigger price is touched.
    MarketIfTouched,
    /// Limit order released when the trigger price is touched.
    LimitIfTouched,
}

impl OrderType {
    /// The protobuf spelling, as `PriceType::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Market => "MARKET",
            Self::Limit => "LIMIT",
            Self::StopMarket => "STOP_MARKET",
            Self::StopLimit => "STOP_LIMIT",
            Self::MarketIfTouched => "MARKET_IF_TOUCHED",
            Self::LimitIfTouched => "LIMIT_IF_TOUCHED",
        }
    }
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

/// Error returned when parsing an invalid [`OrderType`] string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseOrderTypeError(String);

impl fmt::Display for ParseOrderTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid order type: '{}'", self.0)
    }
}

impl std::error::Error for ParseOrderTypeError {}

impl FromStr for OrderType {
    type Err = ParseOrderTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "MARKET" | "MKT" => Ok(Self::Market),
            "LIMIT" | "LMT" => Ok(Self::Limit),
            "STOPMARKET" | "STPMKT" | "STOP_MARKET" | "STOP-MARKET" => Ok(Self::StopMarket),
            "STOPLIMIT" | "STPLMT" | "STOP_LIMIT" | "STOP-LIMIT" => Ok(Self::StopLimit),
            "MARKETIFTOUCHED" | "MIT" | "MARKET_IF_TOUCHED" | "MARKET-IF-TOUCHED" => {
                Ok(Self::MarketIfTouched)
            }
            "LIMITIFTOUCHED" | "LIT" | "LIMIT_IF_TOUCHED" | "LIMIT-IF-TOUCHED" => {
                Ok(Self::LimitIfTouched)
            }
            _ => Err(ParseOrderTypeError(s.to_string())),
        }
    }
}

impl From<OrderType> for request_new_order::PriceType {
    fn from(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Market => Self::Market,
            OrderType::Limit => Self::Limit,
            OrderType::StopMarket => Self::StopMarket,
            OrderType::StopLimit => Self::StopLimit,
            OrderType::MarketIfTouched => Self::MarketIfTouched,
            OrderType::LimitIfTouched => Self::LimitIfTouched,
        }
    }
}

impl From<OrderType> for request_modify_order::PriceType {
    fn from(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Market => Self::Market,
            OrderType::Limit => Self::Limit,
            OrderType::StopMarket => Self::StopMarket,
            OrderType::StopLimit => Self::StopLimit,
            OrderType::MarketIfTouched => Self::MarketIfTouched,
            OrderType::LimitIfTouched => Self::LimitIfTouched,
        }
    }
}

impl From<OrderType> for request_bracket_order::PriceType {
    fn from(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Market => Self::Market,
            OrderType::Limit => Self::Limit,
            OrderType::StopMarket => Self::StopMarket,
            OrderType::StopLimit => Self::StopLimit,
            OrderType::MarketIfTouched => Self::MarketIfTouched,
            OrderType::LimitIfTouched => Self::LimitIfTouched,
        }
    }
}

/// An OCO leg cannot be if-touched: template 328 has no such price type, so
/// [`OrderType::MarketIfTouched`] and [`OrderType::LimitIfTouched`] are rejected.
impl TryFrom<OrderType> for request_oco_order::PriceType {
    type Error = RithmicError;

    fn try_from(order_type: OrderType) -> Result<Self, Self::Error> {
        match order_type {
            OrderType::Market => Ok(Self::Market),
            OrderType::Limit => Ok(Self::Limit),
            OrderType::StopMarket => Ok(Self::StopMarket),
            OrderType::StopLimit => Ok(Self::StopLimit),
            OrderType::MarketIfTouched | OrderType::LimitIfTouched => {
                Err(RithmicError::InvalidArgument(format!(
                    "price_type {} is not available on an OCO leg",
                    order_type.as_str_name()
                )))
            }
        }
    }
}

/// How long an order remains active before expiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum TimeInForce {
    /// Good for the current trading day only.
    #[default]
    Day,
    /// Good till cancelled.
    Gtc,
    /// Immediate or cancel — fill what you can, cancel the rest.
    Ioc,
    /// Fill or kill — fill the entire order or cancel it.
    Fok,
}

impl TimeInForce {
    /// The protobuf spelling, as `Duration::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Day => "DAY",
            Self::Gtc => "GTC",
            Self::Ioc => "IOC",
            Self::Fok => "FOK",
        }
    }
}

impl fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

/// Error returned when parsing an invalid [`TimeInForce`] string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTimeInForceError(String);

impl fmt::Display for ParseTimeInForceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid time-in-force: '{}'", self.0)
    }
}

impl std::error::Error for ParseTimeInForceError {}

impl FromStr for TimeInForce {
    type Err = ParseTimeInForceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "DAY" => Ok(Self::Day),
            "GTC" | "GOODTILLCANCELLED" | "GOOD_TILL_CANCELLED" | "GOOD-TILL-CANCELLED" => {
                Ok(Self::Gtc)
            }
            "IOC" | "IMMEDIATEORCANCEL" | "IMMEDIATE_OR_CANCEL" | "IMMEDIATE-OR-CANCEL" => {
                Ok(Self::Ioc)
            }
            "FOK" | "FILLORKILL" | "FILL_OR_KILL" | "FILL-OR-KILL" => Ok(Self::Fok),
            _ => Err(ParseTimeInForceError(s.to_string())),
        }
    }
}

impl From<TimeInForce> for request_new_order::Duration {
    fn from(tif: TimeInForce) -> Self {
        match tif {
            TimeInForce::Day => Self::Day,
            TimeInForce::Gtc => Self::Gtc,
            TimeInForce::Ioc => Self::Ioc,
            TimeInForce::Fok => Self::Fok,
        }
    }
}

impl From<TimeInForce> for request_bracket_order::Duration {
    fn from(tif: TimeInForce) -> Self {
        match tif {
            TimeInForce::Day => Self::Day,
            TimeInForce::Gtc => Self::Gtc,
            TimeInForce::Ioc => Self::Ioc,
            TimeInForce::Fok => Self::Fok,
        }
    }
}

impl From<TimeInForce> for request_oco_order::Duration {
    fn from(tif: TimeInForce) -> Self {
        match tif {
            TimeInForce::Day => Self::Day,
            TimeInForce::Gtc => Self::Gtc,
            TimeInForce::Ioc => Self::Ioc,
            TimeInForce::Fok => Self::Fok,
        }
    }
}

/// Whether an order was placed by a human or automatically. Defaults to `Auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum ManualOrAutoEntry {
    /// A person placed this.
    Manual,
    /// An algorithm placed this.
    #[default]
    Auto,
}

impl ManualOrAutoEntry {
    /// The protobuf spelling, as `OrderPlacement::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Manual => "MANUAL",
            Self::Auto => "AUTO",
        }
    }
}

impl fmt::Display for ManualOrAutoEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

impl From<ManualOrAutoEntry> for request_new_order::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

impl From<ManualOrAutoEntry> for request_bracket_order::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

impl From<ManualOrAutoEntry> for request_oco_order::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

impl From<ManualOrAutoEntry> for request_modify_order::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

impl From<ManualOrAutoEntry> for request_cancel_order::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

impl From<ManualOrAutoEntry> for request_cancel_all_orders::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

impl From<ManualOrAutoEntry> for request_exit_position::OrderPlacement {
    fn from(entry: ManualOrAutoEntry) -> Self {
        match entry {
            ManualOrAutoEntry::Manual => Self::Manual,
            ManualOrAutoEntry::Auto => Self::Auto,
        }
    }
}

/// The shape of a bracket order's exit legs.
///
/// Rithmic does not document the difference between the plain and `Static`
/// variants. Our reading is that the `Static` variants hold their tick distances
/// fixed relative to the entry, while the others let Rithmic manage them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum BracketType {
    /// Stop legs only.
    StopOnly,
    /// Target legs only.
    TargetOnly,
    /// Both target and stop legs.
    TargetAndStop,
    /// Stop legs only, at fixed tick distances.
    StopOnlyStatic,
    /// Target legs only, at fixed tick distances.
    TargetOnlyStatic,
    /// Both target and stop legs, at fixed tick distances.
    TargetAndStopStatic,
}

impl BracketType {
    /// The protobuf spelling, as `BracketType::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::StopOnly => "STOP_ONLY",
            Self::TargetOnly => "TARGET_ONLY",
            Self::TargetAndStop => "TARGET_AND_STOP",
            Self::StopOnlyStatic => "STOP_ONLY_STATIC",
            Self::TargetOnlyStatic => "TARGET_ONLY_STATIC",
            Self::TargetAndStopStatic => "TARGET_AND_STOP_STATIC",
        }
    }
}

impl fmt::Display for BracketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

impl From<BracketType> for request_bracket_order::BracketType {
    fn from(bracket_type: BracketType) -> Self {
        match bracket_type {
            BracketType::StopOnly => Self::StopOnly,
            BracketType::TargetOnly => Self::TargetOnly,
            BracketType::TargetAndStop => Self::TargetAndStop,
            BracketType::StopOnlyStatic => Self::StopOnlyStatic,
            BracketType::TargetOnlyStatic => Self::TargetOnlyStatic,
            BracketType::TargetAndStopStatic => Self::TargetAndStopStatic,
        }
    }
}

/// The `order_operation_type` of a bracket order, added in template
/// version 5.37: which event on one order of the bracket cancels the rest.
///
/// Rithmic documents only the wire spellings — "AFOCCA, FOCCA, CCA, FCA or
/// OCA". The reading on each variant is async_rithmic's annotation of the
/// same field, not Rithmic's own words; Rithmic's C++ SDK declares the same
/// constants without comment.
///
/// Leave [`RithmicBracketOrder::operation_type`] unset unless a specific
/// grouping is wanted — the server then applies its default, and
/// async_rithmic reverted sending `OCA` on every bracket after it broke
/// bracket orders.
///
/// [`RithmicBracketOrder::operation_type`]: crate::RithmicBracketOrder::operation_type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum BracketOperationType {
    /// Sent as `AFOCCA` — read as "all fill or cancel cancels all".
    Afocca,
    /// Sent as `FOCCA` — read as "fill or cancel cancels all".
    Focca,
    /// Sent as `CCA` — read as "cancel cancels all".
    Cca,
    /// Sent as `FCA` — read as "fill cancels all".
    Fca,
    /// Sent as `OCA` — read as "one cancels all", the classic OCO grouping.
    /// The one value Rithmic's C++ SDK declares no constant for.
    Oca,
}

impl BracketOperationType {
    /// The spelling sent on the wire.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Afocca => "AFOCCA",
            Self::Focca => "FOCCA",
            Self::Cca => "CCA",
            Self::Fca => "FCA",
            Self::Oca => "OCA",
        }
    }
}

impl fmt::Display for BracketOperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

/// The window of a fill-history request, in the two index formats
/// template 3512 accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum FillHistoryRange {
    /// Bounds are seconds since the beginning of the epoch.
    Ssboe {
        /// Start of the window, in seconds since the beginning of the epoch.
        start: i32,
        /// End of the window, in seconds since the beginning of the epoch.
        finish: i32,
    },
    /// Bounds are trade dates written as CCYYMMDD, e.g. `20260804`.
    TradeDate {
        /// First trade date of the window, as CCYYMMDD.
        start: i32,
        /// Last trade date of the window, as CCYYMMDD.
        finish: i32,
    },
}

impl FillHistoryRange {
    /// The `index_format` spelling sent on the wire.
    pub fn index_format(&self) -> &'static str {
        match self {
            Self::Ssboe { .. } => "ssboe",
            Self::TradeDate { .. } => "trade_date",
        }
    }

    /// The start of the window, in this range's index format.
    pub fn start(&self) -> i32 {
        match self {
            Self::Ssboe { start, .. } | Self::TradeDate { start, .. } => *start,
        }
    }

    /// The end of the window, in this range's index format.
    pub fn finish(&self) -> i32 {
        match self {
            Self::Ssboe { finish, .. } | Self::TradeDate { finish, .. } => *finish,
        }
    }
}

/// Comparison operator for an if-touched trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OrderCondition {
    /// Fires when the price field equals the threshold.
    EqualTo,
    /// Fires when the price field differs from the threshold.
    NotEqualTo,
    /// Fires when the price field is above the threshold.
    GreaterThan,
    /// Fires when the price field is at or above the threshold.
    GreaterThanEqualTo,
    /// Fires when the price field is below the threshold.
    LesserThan,
    /// Fires when the price field is at or below the threshold.
    LesserThanEqualTo,
}

impl OrderCondition {
    /// The protobuf spelling, as `Condition::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::EqualTo => "EQUAL_TO",
            Self::NotEqualTo => "NOT_EQUAL_TO",
            Self::GreaterThan => "GREATER_THAN",
            Self::GreaterThanEqualTo => "GREATER_THAN_EQUAL_TO",
            Self::LesserThan => "LESSER_THAN",
            Self::LesserThanEqualTo => "LESSER_THAN_EQUAL_TO",
        }
    }
}

impl fmt::Display for OrderCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

impl From<OrderCondition> for request_new_order::Condition {
    fn from(condition: OrderCondition) -> Self {
        match condition {
            OrderCondition::EqualTo => Self::EqualTo,
            OrderCondition::NotEqualTo => Self::NotEqualTo,
            OrderCondition::GreaterThan => Self::GreaterThan,
            OrderCondition::GreaterThanEqualTo => Self::GreaterThanEqualTo,
            OrderCondition::LesserThan => Self::LesserThan,
            OrderCondition::LesserThanEqualTo => Self::LesserThanEqualTo,
        }
    }
}

impl From<OrderCondition> for request_bracket_order::Condition {
    fn from(condition: OrderCondition) -> Self {
        match condition {
            OrderCondition::EqualTo => Self::EqualTo,
            OrderCondition::NotEqualTo => Self::NotEqualTo,
            OrderCondition::GreaterThan => Self::GreaterThan,
            OrderCondition::GreaterThanEqualTo => Self::GreaterThanEqualTo,
            OrderCondition::LesserThan => Self::LesserThan,
            OrderCondition::LesserThanEqualTo => Self::LesserThanEqualTo,
        }
    }
}

impl From<OrderCondition> for request_modify_order::Condition {
    fn from(condition: OrderCondition) -> Self {
        match condition {
            OrderCondition::EqualTo => Self::EqualTo,
            OrderCondition::NotEqualTo => Self::NotEqualTo,
            OrderCondition::GreaterThan => Self::GreaterThan,
            OrderCondition::GreaterThanEqualTo => Self::GreaterThanEqualTo,
            OrderCondition::LesserThan => Self::LesserThan,
            OrderCondition::LesserThanEqualTo => Self::LesserThanEqualTo,
        }
    }
}

/// Which price an if-touched trigger watches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OrderPriceField {
    /// The best bid.
    BidPrice,
    /// The best offer.
    OfferPrice,
    /// The last trade price.
    TradePrice,
    /// The lean price.
    LeanPrice,
}

impl OrderPriceField {
    /// The protobuf spelling, as `PriceField::as_str_name` writes it.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::BidPrice => "BID_PRICE",
            Self::OfferPrice => "OFFER_PRICE",
            Self::TradePrice => "TRADE_PRICE",
            Self::LeanPrice => "LEAN_PRICE",
        }
    }
}

impl fmt::Display for OrderPriceField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

impl From<OrderPriceField> for request_new_order::PriceField {
    fn from(price_field: OrderPriceField) -> Self {
        match price_field {
            OrderPriceField::BidPrice => Self::BidPrice,
            OrderPriceField::OfferPrice => Self::OfferPrice,
            OrderPriceField::TradePrice => Self::TradePrice,
            OrderPriceField::LeanPrice => Self::LeanPrice,
        }
    }
}

impl From<OrderPriceField> for request_bracket_order::PriceField {
    fn from(price_field: OrderPriceField) -> Self {
        match price_field {
            OrderPriceField::BidPrice => Self::BidPrice,
            OrderPriceField::OfferPrice => Self::OfferPrice,
            OrderPriceField::TradePrice => Self::TradePrice,
            OrderPriceField::LeanPrice => Self::LeanPrice,
        }
    }
}

impl From<OrderPriceField> for request_modify_order::PriceField {
    fn from(price_field: OrderPriceField) -> Self {
        match price_field {
            OrderPriceField::BidPrice => Self::BidPrice,
            OrderPriceField::OfferPrice => Self::OfferPrice,
            OrderPriceField::TradePrice => Self::TradePrice,
            OrderPriceField::LeanPrice => Self::LeanPrice,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `RequestOcoOrder` stops at four price types; the if-touched pair has to be
    /// rejected rather than remapped onto something the caller did not ask for.
    #[test]
    fn the_oco_price_type_rejects_the_if_touched_pair() {
        for order_type in [
            OrderType::Market,
            OrderType::Limit,
            OrderType::StopMarket,
            OrderType::StopLimit,
        ] {
            let converted = request_oco_order::PriceType::try_from(order_type).unwrap();
            assert_eq!(converted.as_str_name(), order_type.as_str_name());
        }

        for order_type in [OrderType::MarketIfTouched, OrderType::LimitIfTouched] {
            let err = request_oco_order::PriceType::try_from(order_type)
                .unwrap_err()
                .to_string();
            assert!(err.contains("is not available on an OCO leg"), "{err}");
            assert!(err.contains(order_type.as_str_name()), "{err}");
        }
    }

    #[test]
    fn order_type_round_trips_through_its_string_forms() {
        for order_type in [
            OrderType::Market,
            OrderType::Limit,
            OrderType::StopMarket,
            OrderType::StopLimit,
            OrderType::MarketIfTouched,
            OrderType::LimitIfTouched,
        ] {
            assert_eq!(order_type.to_string(), order_type.as_str_name());
            assert_eq!(
                order_type.to_string().parse::<OrderType>().unwrap(),
                order_type
            );
        }

        assert_eq!(
            "mit".parse::<OrderType>().unwrap(),
            OrderType::MarketIfTouched
        );
        assert_eq!(
            "lit".parse::<OrderType>().unwrap(),
            OrderType::LimitIfTouched
        );
        assert_eq!(
            "market-if-touched".parse::<OrderType>().unwrap(),
            OrderType::MarketIfTouched
        );
        assert_eq!(
            "limit-if-touched".parse::<OrderType>().unwrap(),
            OrderType::LimitIfTouched
        );
    }
}
