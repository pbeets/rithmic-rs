//! Flattening a position.

use crate::{error::RithmicError, types::ManualOrAutoEntry};

/// Flatten the position in one instrument, or every position on the account.
///
/// The symbol and exchange come as a pair: set both to flatten one
/// instrument, set neither to flatten the whole account.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicExitPosition;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let one = RithmicExitPosition::new()
///     .symbol("ESM6")
///     .exchange("CME")
///     .build()?;
/// let all = RithmicExitPosition::new().build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a command does nothing until passed to a plant handle"]
pub struct RithmicExitPosition {
    /// Trading symbol (e.g., "ESM6"). Unset together with `exchange`, the
    /// exit flattens every position on the account.
    pub symbol: Option<String>,
    /// Exchange code (e.g., "CME"). Comes as a pair with `symbol`.
    pub exchange: Option<String>,
    /// Whether the exit was made by a human or automatically.
    pub manual_or_auto: ManualOrAutoEntry,
    /// Originating window name reported to Rithmic.
    pub window_name: Option<String>,
    /// Name of the trading algorithm credited with the exit.
    pub trading_algorithm: Option<String>,
}

impl RithmicExitPosition {
    /// Start from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Instrument symbol of the position to exit.
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }

    /// Exchange the instrument trades on.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = Some(exchange.into());
        self
    }

    /// Whether this was done by a human or automatically.
    pub fn manual_or_auto(mut self, manual_or_auto: ManualOrAutoEntry) -> Self {
        self.manual_or_auto = manual_or_auto;
        self
    }

    /// Window name to report this exit under.
    pub fn window_name(mut self, window_name: impl Into<String>) -> Self {
        self.window_name = Some(window_name.into());
        self
    }

    /// Trading algorithm to credit with this exit.
    pub fn trading_algorithm(mut self, trading_algorithm: impl Into<String>) -> Self {
        self.trading_algorithm = Some(trading_algorithm.into());
        self
    }

    /// Requires the symbol and exchange together, or neither.
    pub fn validate(&self) -> Result<(), RithmicError> {
        match (&self.symbol, &self.exchange) {
            (Some(symbol), _) if symbol.is_empty() => Err(RithmicError::InvalidArgument(
                "the exit symbol must be non-empty; leave both unset to flatten the account"
                    .to_string(),
            )),
            (_, Some(exchange)) if exchange.is_empty() => Err(RithmicError::InvalidArgument(
                "the exit exchange must be non-empty; leave both unset to flatten the account"
                    .to_string(),
            )),
            (Some(_), None) | (None, Some(_)) => Err(RithmicError::InvalidArgument(
                "symbol and exchange come as a pair: set both to flatten one instrument, \
                 neither to flatten the account"
                    .to_string(),
            )),
            _ => Ok(()),
        }
    }

    /// Requires the symbol and exchange together, or neither.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exit_takes_the_instrument_as_a_pair_or_not_at_all() {
        // Neither set: flatten the whole account.
        assert!(RithmicExitPosition::new().build().is_ok());

        // One of the pair alone is refused, as is an empty member.
        assert!(RithmicExitPosition::new().symbol("ESM6").build().is_err());
        assert!(RithmicExitPosition::new().exchange("CME").build().is_err());
        assert!(
            RithmicExitPosition::new()
                .symbol("")
                .exchange("CME")
                .build()
                .is_err()
        );
        assert!(
            RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("")
                .build()
                .is_err()
        );

        assert!(
            RithmicExitPosition::new()
                .symbol("ESM6")
                .exchange("CME")
                .build()
                .is_ok()
        );
    }
}
