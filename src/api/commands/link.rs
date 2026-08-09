//! Linking working orders into one group.

use crate::error::RithmicError;

/// Link working orders together so the server treats them as one group.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicLinkOrders;
/// # fn main() -> Result<(), rithmic_rs::RithmicError> {
/// let command = RithmicLinkOrders::new()
///     .basket_ids(["123456", "123457"])
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[must_use = "a command does nothing until passed to a plant handle"]
pub struct RithmicLinkOrders {
    /// The `basket_id`s to link, from the order notifications.
    pub basket_ids: Vec<String>,
}

impl RithmicLinkOrders {
    /// Start from the defaults, with no baskets to link.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one more `basket_id`.
    pub fn basket_id(mut self, basket_id: impl Into<String>) -> Self {
        self.basket_ids.push(basket_id.into());
        self
    }

    /// Append several more `basket_id`s.
    pub fn basket_ids(mut self, basket_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.basket_ids
            .extend(basket_ids.into_iter().map(Into::into));
        self
    }

    /// Requires at least two non-empty basket_ids.
    pub fn validate(&self) -> Result<(), RithmicError> {
        if self.basket_ids.len() < 2 {
            return Err(RithmicError::InvalidArgument(format!(
                "linking needs at least two basket_ids, got {}",
                self.basket_ids.len()
            )));
        }

        if self.basket_ids.iter().any(String::is_empty) {
            return Err(RithmicError::InvalidArgument(
                "every basket_id to link must be non-empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Requires at least two non-empty basket_ids.
    pub fn build(self) -> Result<Self, RithmicError> {
        self.validate()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linking_requires_at_least_two_non_empty_basket_ids() {
        assert!(RithmicLinkOrders::new().build().is_err());
        assert!(
            RithmicLinkOrders::new()
                .basket_id("123456")
                .build()
                .is_err()
        );
        assert!(
            RithmicLinkOrders::new()
                .basket_ids(["123456", ""])
                .build()
                .is_err()
        );
        assert!(
            RithmicLinkOrders::new()
                .basket_ids(["123456", "123457"])
                .build()
                .is_ok()
        );
    }
}
