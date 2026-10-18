//! Order status types and utilities.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, fmt, str::FromStr};

/// Status string for open orders.
pub const OPEN: &str = "open";
/// Status string for finished orders, filled or cancelled.
pub const COMPLETE: &str = "complete";
/// Status string for cancelled orders.
pub const CANCELLED: &str = "cancelled";
/// Status string for pending orders.
pub const PENDING: &str = "pending";
/// Status string for working conditional orders awaiting their trigger.
pub const TRIGGER_PENDING: &str = "trigger pending";
/// Status string for orders awaiting acknowledgement that they are open.
pub const OPEN_PENDING: &str = "open pending";
/// Status string for orders with a pending modification.
pub const MODIFY_PENDING: &str = "modify pending";
/// Status string for orders with a pending cancellation.
pub const CANCEL_PENDING: &str = "cancel pending";
/// Status string for working orders that have been modified.
pub const MODIFIED: &str = "modified";
/// Status string for modified conditional orders still awaiting their trigger.
pub const MODIFIED_TRIGGER_PENDING: &str = "modified, trigger pending";
/// Status string for rejected orders.
pub const REJECTED: &str = "rejected";
/// Status string for partially filled orders.
pub const PARTIAL: &str = "partial";
/// Status string for expired orders.
pub const EXPIRED: &str = "expired";

/// Order status with helpers for checking terminal/active states.
///
/// Parses case-insensitively and handles common variations like "filled" for Complete
/// and "canceled" (US spelling) for Cancelled. Parsing never fails: a string it
/// does not recognise becomes [`OrderStatus::Unknown`].
///
/// # Example
/// ```
/// use rithmic_rs::OrderStatus;
///
/// let status: OrderStatus = "filled".parse().unwrap();
/// assert_eq!(status, OrderStatus::Complete);
/// assert!(status.is_terminal());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OrderStatus {
    /// Order is open and working in the market.
    Open,
    /// Order is finished. Rithmic reports cancels as `complete` too; the
    /// notification's `completion_reason` is `"F"` for filled, `"C"` for cancelled.
    Complete,
    /// Order has been cancelled.
    Cancelled,
    /// Generic pending status. Rithmic order notifications send the specific
    /// pending variants, such as [`Self::OpenPending`], instead.
    Pending,
    /// Order was rejected by the exchange or risk system.
    Rejected,
    /// Order has been partially filled.
    Partial,
    /// Order has expired.
    Expired,
    /// Unknown or unrecognized status.
    #[default]
    Unknown,
    // Append variants to preserve existing indices in index-based Serde formats.
    /// Conditional order is working and awaiting its trigger.
    TriggerPending,
    /// Order is awaiting acknowledgement that it is open.
    OpenPending,
    /// Order has a pending modification and remains active.
    ModifyPending,
    /// Order has a pending cancellation and remains active until confirmed.
    CancelPending,
    /// Order has been modified and is still working.
    Modified,
    /// Conditional order has been modified and is still awaiting its trigger.
    ModifiedTriggerPending,
}

impl OrderStatus {
    /// Returns true if this is a terminal status (Complete, Cancelled, Rejected, Expired).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Cancelled | Self::Rejected | Self::Expired
        )
    }

    /// Returns true for open, partially filled, modified and pending orders,
    /// including orders awaiting a trigger, modification or cancellation.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Open
                | Self::Pending
                | Self::TriggerPending
                | Self::OpenPending
                | Self::ModifyPending
                | Self::CancelPending
                | Self::Modified
                | Self::ModifiedTriggerPending
                | Self::Partial
        )
    }
}

impl FromStr for OrderStatus {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let status = match s.to_lowercase().as_str() {
            OPEN => Self::Open,
            COMPLETE | "filled" => Self::Complete,
            CANCELLED | "canceled" => Self::Cancelled,
            PENDING => Self::Pending,
            TRIGGER_PENDING => Self::TriggerPending,
            OPEN_PENDING => Self::OpenPending,
            MODIFY_PENDING => Self::ModifyPending,
            CANCEL_PENDING => Self::CancelPending,
            MODIFIED => Self::Modified,
            MODIFIED_TRIGGER_PENDING => Self::ModifiedTriggerPending,
            REJECTED => Self::Rejected,
            PARTIAL | "partially_filled" | "partially filled" => Self::Partial,
            EXPIRED => Self::Expired,
            _ => Self::Unknown,
        };

        Ok(status)
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Open => OPEN,
            Self::Complete => COMPLETE,
            Self::Cancelled => CANCELLED,
            Self::Pending => PENDING,
            Self::TriggerPending => TRIGGER_PENDING,
            Self::OpenPending => OPEN_PENDING,
            Self::ModifyPending => MODIFY_PENDING,
            Self::CancelPending => CANCEL_PENDING,
            Self::Modified => MODIFIED,
            Self::ModifiedTriggerPending => MODIFIED_TRIGGER_PENDING,
            Self::Rejected => REJECTED,
            Self::Partial => PARTIAL,
            Self::Expired => EXPIRED,
            Self::Unknown => "unknown",
        };

        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_working_statuses_remain_active() {
        for (value, expected) in [
            ("trigger pending", OrderStatus::TriggerPending),
            ("open pending", OrderStatus::OpenPending),
            ("modify pending", OrderStatus::ModifyPending),
            ("cancel pending", OrderStatus::CancelPending),
            ("modified", OrderStatus::Modified),
            (
                "modified, trigger pending",
                OrderStatus::ModifiedTriggerPending,
            ),
        ] {
            let status: OrderStatus = value.parse().unwrap();
            assert_eq!(status, expected);
            assert!(status.is_active(), "{value} must remain active");
            assert!(!status.is_terminal(), "{value} must remain nonterminal");
        }
    }

    #[test]
    fn test_parse_case_insensitive() {
        assert_eq!("open".parse::<OrderStatus>().unwrap(), OrderStatus::Open);
        assert_eq!("OPEN".parse::<OrderStatus>().unwrap(), OrderStatus::Open);
        assert_eq!("Open".parse::<OrderStatus>().unwrap(), OrderStatus::Open);
    }

    #[test]
    fn test_parse_variations() {
        assert_eq!(
            "complete".parse::<OrderStatus>().unwrap(),
            OrderStatus::Complete
        );

        assert_eq!(
            "filled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Complete
        );

        assert_eq!(
            "cancelled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );

        assert_eq!(
            "canceled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );

        assert_eq!(
            "partial".parse::<OrderStatus>().unwrap(),
            OrderStatus::Partial
        );

        assert_eq!(
            "partially_filled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Partial
        );
    }

    #[test]
    fn test_is_terminal() {
        assert!(OrderStatus::Complete.is_terminal());
        assert!(OrderStatus::Cancelled.is_terminal());
        assert!(OrderStatus::Rejected.is_terminal());
        assert!(OrderStatus::Expired.is_terminal());
        assert!(!OrderStatus::Open.is_terminal());
        assert!(!OrderStatus::Pending.is_terminal());
        assert!(!OrderStatus::Partial.is_terminal());
        assert!(!OrderStatus::Unknown.is_terminal());
    }

    #[test]
    fn test_is_active() {
        assert!(OrderStatus::Open.is_active());
        assert!(OrderStatus::Pending.is_active());
        assert!(OrderStatus::Partial.is_active());
        assert!(!OrderStatus::Complete.is_active());
        assert!(!OrderStatus::Cancelled.is_active());
        assert!(!OrderStatus::Rejected.is_active());
        assert!(!OrderStatus::Expired.is_active());
        assert!(!OrderStatus::Unknown.is_active());
    }

    #[test]
    fn test_unknown_status() {
        assert_eq!("".parse::<OrderStatus>().unwrap(), OrderStatus::Unknown);

        assert_eq!(
            "foobar".parse::<OrderStatus>().unwrap(),
            OrderStatus::Unknown
        );
    }

    #[test]
    fn test_roundtrip() {
        for status in [
            OrderStatus::Open,
            OrderStatus::Complete,
            OrderStatus::Cancelled,
            OrderStatus::Pending,
            OrderStatus::TriggerPending,
            OrderStatus::OpenPending,
            OrderStatus::ModifyPending,
            OrderStatus::CancelPending,
            OrderStatus::Modified,
            OrderStatus::ModifiedTriggerPending,
            OrderStatus::Rejected,
            OrderStatus::Partial,
            OrderStatus::Expired,
        ] {
            let s = status.to_string();
            let parsed: OrderStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }
}
