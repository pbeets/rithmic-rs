//! Order enums with serde support and protobuf conversions.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use std::fmt;
use std::str::FromStr;

use crate::rti::{
    request_bracket_order, request_modify_order, request_new_order, request_oco_order,
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

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Buy => write!(f, "BUY"),
            Self::Sell => write!(f, "SELL"),
        }
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
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Market => write!(f, "MARKET"),
            Self::Limit => write!(f, "LIMIT"),
            Self::StopMarket => write!(f, "STOP_MARKET"),
            Self::StopLimit => write!(f, "STOP_LIMIT"),
        }
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
        }
    }
}

impl From<OrderType> for request_oco_order::PriceType {
    fn from(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Market => Self::Market,
            OrderType::Limit => Self::Limit,
            OrderType::StopMarket => Self::StopMarket,
            OrderType::StopLimit => Self::StopLimit,
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

impl fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Day => write!(f, "DAY"),
            Self::Gtc => write!(f, "GTC"),
            Self::Ioc => write!(f, "IOC"),
            Self::Fok => write!(f, "FOK"),
        }
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
