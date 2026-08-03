//! Flattening a position.

use crate::{error::RithmicError, types::ManualOrAutoEntry};

/// Flatten the position in one instrument.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicExitPosition;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let command = RithmicExitPosition::new()
///     .symbol("ESM6")
///     .exchange("CME")
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct RithmicExitPosition {
    /// Trading symbol (e.g., "ESM6")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
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
        self.symbol = symbol.into();
        self
    }

    /// Exchange the instrument trades on.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
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

    /// Return the command.
    pub fn build(self) -> Result<Self, RithmicError> {
        Ok(self)
    }
}
