//! Instrument reference data types.

use crate::rti::ResponseReferenceData;

/// Parsed instrument information from Rithmic reference data.
///
/// # Example
/// ```ignore
/// let info = InstrumentInfo::try_from(&response)?;
/// println!("{} on {} - tick size {:?}", info.symbol, info.exchange, info.tick_size);
/// ```
#[derive(Debug, Clone, Default)]
pub struct InstrumentInfo {
    /// Trading symbol (e.g., "ESH4")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Exchange-specific symbol if different from symbol
    pub exchange_symbol: Option<String>,
    /// Human-readable name (e.g., "E-mini S&P 500")
    pub name: Option<String>,
    /// Product code for the instrument family (e.g., "ES")
    pub product_code: Option<String>,
    /// Instrument type (e.g., "Future", "Option")
    pub instrument_type: Option<String>,
    /// Underlying symbol for derivatives
    pub underlying: Option<String>,
    /// Currency code (e.g., "USD")
    pub currency: Option<String>,
    /// Expiration date string (format varies by exchange)
    pub expiration_date: Option<String>,
    /// Minimum price increment
    pub tick_size: Option<f64>,
    /// Dollar value of one point move
    pub point_value: Option<f64>,
    /// Whether the instrument can be traded
    pub is_tradable: bool,
}

impl InstrumentInfo {
    /// Calculate decimal places for price display based on tick size.
    ///
    /// Returns 2 as default if tick_size is not available.
    ///
    /// # Example
    /// ```
    /// use rithmic_rs::InstrumentInfo;
    ///
    /// let mut info = InstrumentInfo::default();
    /// info.tick_size = Some(0.25);  // ES
    /// assert_eq!(info.price_precision(), 2);
    ///
    /// info.tick_size = Some(0.03125);  // ZB (1/32)
    /// assert_eq!(info.price_precision(), 5);
    /// ```
    pub fn price_precision(&self) -> u8 {
        match self.tick_size {
            Some(tick) if tick > 0.0 => {
                let mut precision = 0u8;
                let mut value = tick;

                while value < 1.0 && precision < 10 {
                    value *= 10.0;
                    precision += 1;
                }

                let fractional = value - value.floor();

                if fractional > 0.0001 && precision < 10 {
                    let mut frac = fractional;

                    while frac > 0.0001 && precision < 10 {
                        frac *= 10.0;
                        frac -= frac.floor();
                        precision += 1;
                    }
                }
                precision
            }
            _ => 2,
        }
    }

    /// Size precision (always 0 for futures since they trade in whole contracts).
    pub fn size_precision(&self) -> u8 {
        0
    }
}

/// Error returned when constructing an [`InstrumentInfo`] from reference data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentInfoError {
    /// Human-readable description of what went wrong.
    pub message: String,
}

impl std::fmt::Display for InstrumentInfoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for InstrumentInfoError {}

impl TryFrom<&ResponseReferenceData> for InstrumentInfo {
    type Error = InstrumentInfoError;

    fn try_from(data: &ResponseReferenceData) -> Result<Self, Self::Error> {
        let symbol = data.symbol.clone().ok_or_else(|| InstrumentInfoError {
            message: "missing symbol".to_string(),
        })?;

        let exchange = data.exchange.clone().ok_or_else(|| InstrumentInfoError {
            message: "missing exchange".to_string(),
        })?;

        let is_tradable = data
            .is_tradable
            .as_ref()
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);

        Ok(InstrumentInfo {
            symbol,
            exchange,
            exchange_symbol: data.exchange_symbol.clone(),
            name: data.symbol_name.clone(),
            product_code: data.product_code.clone(),
            instrument_type: data.instrument_type.clone(),
            underlying: data.underlying_symbol.clone(),
            currency: data.currency.clone(),
            expiration_date: data.expiration_date.clone(),
            tick_size: data.min_qprice_change,
            point_value: data.single_point_value,
            is_tradable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_price_precision() {
        let mut info = InstrumentInfo::default();

        info.tick_size = Some(0.25); // ES
        assert_eq!(info.price_precision(), 2);

        info.tick_size = Some(0.01); // CL
        assert_eq!(info.price_precision(), 2);

        info.tick_size = Some(0.03125); // ZB (1/32)
        assert_eq!(info.price_precision(), 5);

        info.tick_size = Some(1.0); // whole number tick
        assert_eq!(info.price_precision(), 0);

        info.tick_size = None; // default
        assert_eq!(info.price_precision(), 2);
    }

    #[test]
    fn test_try_from_missing_symbol() {
        let data = ResponseReferenceData {
            template_id: 15,
            symbol: None,
            exchange: Some("CME".to_string()),
            ..Default::default()
        };

        let result = InstrumentInfo::try_from(&data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "missing symbol");
    }

    #[test]
    fn test_try_from_missing_exchange() {
        let data = ResponseReferenceData {
            template_id: 15,
            symbol: Some("ESH4".to_string()),
            exchange: None,
            ..Default::default()
        };

        let result = InstrumentInfo::try_from(&data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "missing exchange");
    }

    #[test]
    fn test_try_from_success() {
        let data = ResponseReferenceData {
            template_id: 15,
            symbol: Some("ESH4".to_string()),
            exchange: Some("CME".to_string()),
            symbol_name: Some("E-mini S&P 500".to_string()),
            product_code: Some("ES".to_string()),
            instrument_type: Some("Future".to_string()),
            currency: Some("USD".to_string()),
            min_qprice_change: Some(0.25),
            single_point_value: Some(50.0),
            is_tradable: Some("true".to_string()),
            ..Default::default()
        };

        let info = InstrumentInfo::try_from(&data).unwrap();
        assert_eq!(info.symbol, "ESH4");
        assert_eq!(info.exchange, "CME");
        assert_eq!(info.name, Some("E-mini S&P 500".to_string()));
        assert_eq!(info.product_code, Some("ES".to_string()));
        assert_eq!(info.tick_size, Some(0.25));
        assert_eq!(info.point_value, Some(50.0));
        assert!(info.is_tradable);
    }
}
