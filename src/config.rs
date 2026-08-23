//! Configuration for Rithmic connections.
//!
//! This module provides the primary interface for configuring Rithmic connections.
//! [`RithmicConfig`] contains connection and login details, while [`RithmicAccount`]
//! models a concrete trading account identity for order and PnL requests.
//!
//! # Example
//! ```no_run
//! use rithmic_rs::config::{RithmicConfig, RithmicEnv};
//! use rithmic_rs::RithmicAccount;
//!
//! // Simple one-line configuration from environment variables
//! let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!
//! // Or build manually if needed
//! let config = RithmicConfig::builder(RithmicEnv::Demo)
//!     .user("my_user")
//!     .password("my_password")
//!     .app_name("my_app")
//!     .app_version("1")
//!     .build()?;
//!
//! let account = RithmicAccount::new("my_fcm", "my_ib", "my_account");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#[allow(deprecated)]
use crate::request_handler::DEFAULT_REQUEST_TIMEOUT;
use std::{env, fmt, str::FromStr, time::Duration};

/// Trading environment selector.
///
/// Determines which Rithmic environment to connect to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
#[non_exhaustive]
pub enum RithmicEnv {
    /// Rithmic Paper Trading (demo/development) environment.
    #[default]
    Demo,
    /// Rithmic 01 (live/production) environment.
    Live,
    /// Rithmic Test environment.
    Test,
}

impl RithmicEnv {
    /// Environment-variable prefix for this environment's settings
    /// (e.g. `RITHMIC_DEMO` → `RITHMIC_DEMO_USER`).
    fn var_prefix(self) -> &'static str {
        match self {
            RithmicEnv::Demo => "RITHMIC_DEMO",
            RithmicEnv::Live => "RITHMIC_LIVE",
            RithmicEnv::Test => "RITHMIC_TEST",
        }
    }

    /// The Rithmic system name this environment logs in to by default.
    fn default_system_name(self) -> &'static str {
        match self {
            RithmicEnv::Demo => "Rithmic Paper Trading",
            RithmicEnv::Live => "Rithmic 01",
            RithmicEnv::Test => "Rithmic Test",
        }
    }
}

impl fmt::Display for RithmicEnv {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RithmicEnv::Demo => write!(f, "demo"),
            RithmicEnv::Live => write!(f, "live"),
            RithmicEnv::Test => write!(f, "test"),
        }
    }
}

/// Read a required environment variable, mapping absence to
/// [`ConfigError::MissingEnvVar`].
fn require_env(var: &str) -> Result<String, ConfigError> {
    env::var(var).map_err(|_| ConfigError::MissingEnvVar(var.to_string()))
}

impl FromStr for RithmicEnv {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "demo" | "development" => Ok(RithmicEnv::Demo),
            "live" | "production" => Ok(RithmicEnv::Live),
            "test" => Ok(RithmicEnv::Test),
            _ => Err(ConfigError::InvalidEnvironment(s.to_string())),
        }
    }
}

/// Configuration error types.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The environment string could not be parsed.
    InvalidEnvironment(String),
    /// A configuration value was present but invalid.
    #[non_exhaustive]
    InvalidValue {
        /// The variable or field name.
        var: String,
        /// Why the value was rejected.
        reason: String,
    },
    /// A required environment variable was not set.
    MissingEnvVar(String),
    /// A required builder field was not provided.
    MissingField(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConfigError::MissingEnvVar(var) => {
                write!(f, "Missing environment variable: {}", var)
            }
            ConfigError::InvalidEnvironment(env) => {
                write!(f, "Invalid environment: {}", env)
            }
            ConfigError::InvalidValue { var, reason } => {
                write!(f, "Invalid value for {}: {}", var, reason)
            }
            ConfigError::MissingField(field) => {
                write!(f, "Missing required field: {}", field)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Trading account identity for order and PnL requests.
///
/// This type is separate from [`RithmicConfig`] because Rithmic authenticates
/// per user session while order and PnL operations are account-scoped.
///
/// # Example
///
/// ```
/// use rithmic_rs::RithmicAccount;
///
/// let account = RithmicAccount::new("FCM_ID", "IB_ID", "ACCOUNT_ID");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct RithmicAccount {
    /// Trading account identifier.
    pub account_id: String,
    /// Futures Commission Merchant identifier.
    pub fcm_id: String,
    /// Introducing Broker identifier.
    pub ib_id: String,
}

impl RithmicAccount {
    /// Create a typed account identity directly.
    pub fn new(
        fcm_id: impl Into<String>,
        ib_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Self {
        Self {
            account_id: account_id.into(),
            fcm_id: fcm_id.into(),
            ib_id: ib_id.into(),
        }
    }

    /// Create an account identity by loading values from environment variables.
    ///
    /// See [`examples/.env.blank`](https://github.com/pbeets/rithmic-rs/blob/main/examples/.env.blank)
    /// for a template of all required environment variables.
    pub fn from_env(env: RithmicEnv) -> Result<Self, ConfigError> {
        let prefix = env.var_prefix();

        Ok(Self {
            account_id: require_env(&format!("{prefix}_ACCOUNT_ID"))?,
            fcm_id: require_env(&format!("{prefix}_FCM_ID"))?,
            ib_id: require_env(&format!("{prefix}_IB_ID"))?,
        })
    }
}

/// Login overrides. Every field left `None` keeps the default, so `login()` is
/// the whole story unless you need one of these.
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{ConnectStrategy, LoginConfig, RithmicConfig, RithmicEnv, RithmicTickerPlant};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
/// let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
/// let handle = plant.get_handle();
///
/// // Tick-by-tick quotes (default)
/// handle.login().await?;
///
/// // Aggregated quotes
/// let mut login = LoginConfig::default();
/// login.aggregated_quotes = Some(true);
/// handle.login_with_config(login).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct LoginConfig {
    /// Aggregated rather than tick-by-tick quotes. Ticker plant only.
    pub aggregated_quotes: Option<bool>,
    /// MAC addresses reported to Rithmic. None are sent when unset.
    pub mac_addr: Option<Vec<String>>,
    /// OS version reported to Rithmic. Sent empty when unset.
    pub os_version: Option<String>,
    /// OS platform reported to Rithmic. Sent empty when unset.
    pub os_platform: Option<String>,
}

const REQUEST_TIMEOUT_VAR: &str = "RITHMIC_REQUEST_TIMEOUT_SECS";

/// Parse a duration given as a plain decimal count of seconds.
///
/// Stricter than `u64::from_str`, which also takes a sign and leading zeros:
/// only canonical digits pass, so a mangled value cannot slip through as a
/// plausible number.
fn parse_whole_seconds(value: &str) -> Option<u64> {
    let canonical = !value.is_empty()
        && value.bytes().all(|b| b.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'));

    if canonical { value.parse().ok() } else { None }
}

/// Configuration for Rithmic connections.
///
/// This struct contains session-level connection and login details.
///
/// Build one with [`RithmicConfig::from_env`] or [`RithmicConfig::builder`]; to
/// load from the environment and override a field, use
/// [`RithmicConfigBuilder::from_env`].
///
/// ```no_run
/// use rithmic_rs::{RithmicConfigBuilder, RithmicEnv};
///
/// let config = RithmicConfigBuilder::from_env(RithmicEnv::Demo)?
///     .system_name("Rithmic Paper Trading")
///     .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub struct RithmicConfig {
    /// Primary WebSocket URL.
    pub url: String,
    /// Alternative/beta WebSocket URL used by [`ConnectStrategy::AlternateWithRetry`](crate::ConnectStrategy::AlternateWithRetry).
    pub beta_url: String,
    /// Login username.
    pub user: String,
    /// Login password.
    pub password: String,
    /// Rithmic system name (e.g. "Rithmic Paper Trading").
    pub system_name: String,
    /// Target trading environment.
    pub env: RithmicEnv,
    /// Application name registered with Rithmic.
    pub app_name: String,
    /// Application version string.
    pub app_version: String,
    /// No longer used. The library does not time out requests; wrap the call
    /// in [`tokio::time::timeout`] to set a deadline of your own. Still
    /// readable so existing code keeps compiling; removed in 4.0.0.
    #[deprecated(
        since = "3.1.0",
        note = "the library no longer times out requests; wrap the call in tokio::time::timeout"
    )]
    pub request_timeout: Duration,
}

impl fmt::Debug for RithmicConfig {
    #[allow(deprecated)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RithmicConfig")
            .field("url", &self.url)
            .field("beta_url", &self.beta_url)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("system_name", &self.system_name)
            .field("env", &self.env)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl RithmicConfig {
    /// Create a configuration by loading values from environment variables.
    ///
    /// See [`examples/.env.blank`](https://github.com/pbeets/rithmic-rs/blob/main/examples/.env.blank)
    /// for a template of all required environment variables.
    ///
    /// # Required environment variables
    ///
    /// For Demo environment:
    /// - `RITHMIC_DEMO_USER`: Demo username
    /// - `RITHMIC_DEMO_PW`: Demo password
    /// - `RITHMIC_DEMO_URL`: Demo WebSocket URL
    /// - `RITHMIC_DEMO_ALT_URL`: Demo alternative/beta WebSocket URL
    ///
    /// For Live environment:
    /// - `RITHMIC_LIVE_USER`: Live username
    /// - `RITHMIC_LIVE_PW`: Live password
    /// - `RITHMIC_LIVE_URL`: Live WebSocket URL
    /// - `RITHMIC_LIVE_ALT_URL`: Live alternative/beta WebSocket URL
    ///
    /// For Test environment:
    /// - `RITHMIC_TEST_USER`: Test username
    /// - `RITHMIC_TEST_PW`: Test password
    /// - `RITHMIC_TEST_URL`: Test WebSocket URL
    /// - `RITHMIC_TEST_ALT_URL`: Test alternative/beta WebSocket URL
    ///
    /// Shared (all environments):
    /// - `RITHMIC_APP_NAME` (required): Application name registered with Rithmic
    /// - `RITHMIC_APP_VERSION` (required): Application version
    ///
    /// # Optional environment variables
    ///
    /// - `RITHMIC_{DEMO,LIVE,TEST}_SYSTEM_NAME`: Rithmic system name to log in
    ///   to. Defaults to "Rithmic Paper Trading" (Demo), "Rithmic 01" (Live),
    ///   or "Rithmic Test" (Test). Set it on Live to select another provider,
    ///   e.g. Thrive Trading.
    /// - `RITHMIC_REQUEST_TIMEOUT_SECS`: no longer used. Still read and still
    ///   rejected if it is not plain digits, then ignored.
    ///
    /// # Example
    /// ```no_run
    /// use rithmic_rs::config::{RithmicConfig, RithmicEnv};
    /// use rithmic_rs::RithmicAccount;
    ///
    /// // Load from environment variables
    /// let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    /// let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[allow(deprecated)]
    pub fn from_env(env: RithmicEnv) -> Result<Self, ConfigError> {
        let prefix = env.var_prefix();

        let url = require_env(&format!("{prefix}_URL"))?;
        let beta_url = require_env(&format!("{prefix}_ALT_URL"))?;
        let user = require_env(&format!("{prefix}_USER"))?;
        let password = require_env(&format!("{prefix}_PW"))?;

        // The environment's usual system name is only a default: on Live in
        // particular, providers other than Rithmic 01 (Thrive Trading, etc.)
        // are selected by overriding it.
        let system_name = env::var(format!("{prefix}_SYSTEM_NAME"))
            .unwrap_or_else(|_| env.default_system_name().to_string());

        let app_name = require_env("RITHMIC_APP_NAME")?;
        let app_version = require_env("RITHMIC_APP_VERSION")?;

        let request_timeout = match env::var(REQUEST_TIMEOUT_VAR) {
            Err(env::VarError::NotPresent) => DEFAULT_REQUEST_TIMEOUT,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidValue {
                    var: REQUEST_TIMEOUT_VAR.to_string(),
                    reason: "expected whole seconds, got a non-unicode value".to_string(),
                });
            }
            Ok(value) => match parse_whole_seconds(value.trim()) {
                // Zero selects the default, consistent with the builder.
                Some(0) => DEFAULT_REQUEST_TIMEOUT,
                Some(secs) => Duration::from_secs(secs),
                None => {
                    return Err(ConfigError::InvalidValue {
                        var: REQUEST_TIMEOUT_VAR.to_string(),
                        reason: format!("expected whole seconds (digits only), got {value:?}"),
                    });
                }
            },
        };

        Ok(Self {
            url,
            beta_url,
            user,
            password,
            system_name,
            env,
            app_name,
            app_version,
            request_timeout,
        })
    }

    /// Create a builder for programmatic configuration.
    ///
    /// Use this to set configuration values directly in code.
    ///
    /// # Example
    /// ```no_run
    /// use rithmic_rs::config::{RithmicConfig, RithmicEnv};
    ///
    /// let config = RithmicConfig::builder(RithmicEnv::Demo)
    ///     .user("my_user")
    ///     .password("my_password")
    ///     .app_name("my_app")
    ///     .app_version("1")
    ///     .build()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn builder(env: RithmicEnv) -> RithmicConfigBuilder {
        RithmicConfigBuilder::new(env)
    }
}

/// Builder for constructing a RithmicConfig with custom values.
#[must_use = "the builder does nothing until build() is called"]
pub struct RithmicConfigBuilder {
    env: RithmicEnv,
    url: Option<String>,
    beta_url: Option<String>,
    user: Option<String>,
    password: Option<String>,
    system_name: Option<String>,
    app_name: Option<String>,
    app_version: Option<String>,
    request_timeout: Duration,
}

impl RithmicConfigBuilder {
    /// Create a builder pre-filled from the same environment variables
    /// [`RithmicConfig::from_env`] reads, so a single field can be overridden.
    /// Rejects whatever [`RithmicConfig::from_env`] rejects.
    ///
    /// ```no_run
    /// use rithmic_rs::{RithmicConfigBuilder, RithmicEnv};
    ///
    /// let config = RithmicConfigBuilder::from_env(RithmicEnv::Demo)?
    ///     .system_name("Rithmic Paper Trading")
    ///     .build()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[allow(deprecated)]
    pub fn from_env(env: RithmicEnv) -> Result<Self, ConfigError> {
        let config = RithmicConfig::from_env(env)?;

        Ok(Self {
            env: config.env,
            url: Some(config.url),
            beta_url: Some(config.beta_url),
            user: Some(config.user),
            password: Some(config.password),
            system_name: Some(config.system_name),
            app_name: Some(config.app_name),
            app_version: Some(config.app_version),
            request_timeout: config.request_timeout,
        })
    }

    /// Create a new builder for the specified environment.
    #[allow(deprecated)]
    pub fn new(env: RithmicEnv) -> Self {
        let system_name = env.default_system_name().to_string();

        Self {
            env,
            url: None,
            beta_url: None,
            user: None,
            password: None,
            system_name: Some(system_name),
            app_name: None,
            app_version: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Set the WebSocket URL.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set the beta WebSocket URL.
    pub fn beta_url(mut self, beta_url: impl Into<String>) -> Self {
        self.beta_url = Some(beta_url.into());
        self
    }

    /// Set the username.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the system name.
    pub fn system_name(mut self, system_name: impl Into<String>) -> Self {
        self.system_name = Some(system_name.into());
        self
    }

    /// Set the application name registered with Rithmic.
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = Some(app_name.into());
        self
    }

    /// No longer used. The library does not time out requests; wrap the call
    /// in [`tokio::time::timeout`] to set a deadline of your own. The value is
    /// still recorded on the config and still ignored; removed in 4.0.0.
    #[deprecated(
        since = "3.1.0",
        note = "the library no longer times out requests; wrap the call in tokio::time::timeout"
    )]
    #[allow(deprecated)]
    pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = if request_timeout.is_zero() {
            DEFAULT_REQUEST_TIMEOUT
        } else {
            request_timeout
        };
        self
    }

    /// Set the application version string registered with Rithmic.
    pub fn app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = Some(app_version.into());
        self
    }

    /// Build the configuration.
    ///
    /// Returns an error if any required fields are missing.
    #[allow(deprecated)]
    pub fn build(self) -> Result<RithmicConfig, ConfigError> {
        Ok(RithmicConfig {
            env: self.env,
            url: self
                .url
                .ok_or_else(|| ConfigError::MissingField("url".to_string()))?,
            beta_url: self
                .beta_url
                .ok_or_else(|| ConfigError::MissingField("beta_url".to_string()))?,
            user: self
                .user
                .ok_or_else(|| ConfigError::MissingField("user".to_string()))?,
            password: self
                .password
                .ok_or_else(|| ConfigError::MissingField("password".to_string()))?,
            system_name: self
                .system_name
                .ok_or_else(|| ConfigError::MissingField("system_name".to_string()))?,
            app_name: self
                .app_name
                .ok_or_else(|| ConfigError::MissingField("app_name".to_string()))?,
            app_version: self
                .app_version
                .ok_or_else(|| ConfigError::MissingField("app_version".to_string()))?,
            request_timeout: self.request_timeout,
        })
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {

    use super::*;

    fn demo_env_vars() -> Vec<(&'static str, Option<&'static str>)> {
        vec![
            ("RITHMIC_DEMO_ACCOUNT_ID", Some("test_account")),
            ("RITHMIC_DEMO_FCM_ID", Some("test_fcm")),
            ("RITHMIC_DEMO_IB_ID", Some("test_ib")),
            ("RITHMIC_DEMO_USER", Some("demo_user")),
            ("RITHMIC_DEMO_PW", Some("demo_password")),
            ("RITHMIC_DEMO_URL", Some("wss://test-demo.example.com:443")),
            (
                "RITHMIC_DEMO_ALT_URL",
                Some("wss://test-demo-alt.example.com:443"),
            ),
            ("RITHMIC_APP_NAME", Some("test_app")),
            ("RITHMIC_APP_VERSION", Some("1")),
        ]
    }

    fn live_env_vars() -> Vec<(&'static str, Option<&'static str>)> {
        vec![
            ("RITHMIC_LIVE_ACCOUNT_ID", Some("test_account")),
            ("RITHMIC_LIVE_FCM_ID", Some("test_fcm")),
            ("RITHMIC_LIVE_IB_ID", Some("test_ib")),
            ("RITHMIC_LIVE_USER", Some("live_user")),
            ("RITHMIC_LIVE_PW", Some("live_password")),
            ("RITHMIC_LIVE_URL", Some("wss://test-live.example.com:443")),
            (
                "RITHMIC_LIVE_ALT_URL",
                Some("wss://test-live-alt.example.com:443"),
            ),
            ("RITHMIC_APP_NAME", Some("test_app")),
            ("RITHMIC_APP_VERSION", Some("1")),
        ]
    }

    #[test]
    fn test_rithmic_env_display() {
        assert_eq!(RithmicEnv::Demo.to_string(), "demo");
        assert_eq!(RithmicEnv::Live.to_string(), "live");
        assert_eq!(RithmicEnv::Test.to_string(), "test");
    }

    #[test]
    fn test_rithmic_env_from_str() {
        assert_eq!("demo".parse::<RithmicEnv>().unwrap(), RithmicEnv::Demo);
        assert_eq!(
            "development".parse::<RithmicEnv>().unwrap(),
            RithmicEnv::Demo
        );
        assert_eq!("live".parse::<RithmicEnv>().unwrap(), RithmicEnv::Live);
        assert_eq!(
            "production".parse::<RithmicEnv>().unwrap(),
            RithmicEnv::Live
        );
        assert_eq!("test".parse::<RithmicEnv>().unwrap(), RithmicEnv::Test);

        // Test invalid input
        let result = "invalid".parse::<RithmicEnv>();
        assert!(result.is_err());
        if let Err(ConfigError::InvalidEnvironment(env)) = result {
            assert_eq!(env, "invalid");
        } else {
            panic!("Expected InvalidEnvironment error");
        }
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::MissingEnvVar("TEST_VAR".to_string());
        assert_eq!(err.to_string(), "Missing environment variable: TEST_VAR");

        let err = ConfigError::InvalidEnvironment("bad_env".to_string());
        assert_eq!(err.to_string(), "Invalid environment: bad_env");

        let err = ConfigError::InvalidValue {
            var: "TEST".to_string(),
            reason: "too short".to_string(),
        };
        assert_eq!(err.to_string(), "Invalid value for TEST: too short");

        let err = ConfigError::MissingField("field".to_string());
        assert_eq!(err.to_string(), "Missing required field: field");
    }

    #[test]
    fn test_account_from_env_demo_success() {
        temp_env::with_vars(demo_env_vars(), || {
            let account = RithmicAccount::from_env(RithmicEnv::Demo).unwrap();

            assert_eq!(account.account_id, "test_account");
            assert_eq!(account.fcm_id, "test_fcm");
            assert_eq!(account.ib_id, "test_ib");
        });
    }

    #[test]
    fn from_env_reads_the_request_timeout_when_set() {
        let mut vars = demo_env_vars();
        vars.push((REQUEST_TIMEOUT_VAR, Some("5")));

        temp_env::with_vars(vars, || {
            let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();

            assert_eq!(config.request_timeout, Duration::from_secs(5));
        });
    }

    #[test]
    fn from_env_defaults_the_request_timeout_when_unset() {
        let mut vars = demo_env_vars();
        vars.push((REQUEST_TIMEOUT_VAR, None));

        temp_env::with_vars(vars, || {
            let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();

            assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        });
    }

    #[test]
    fn from_env_rejects_an_unusable_request_timeout() {
        for value in ["soon", "+30", "-30", "007", "30.5", "30s", ""] {
            let mut vars = demo_env_vars();
            vars.push((REQUEST_TIMEOUT_VAR, Some(value)));

            temp_env::with_vars(vars, || {
                assert!(
                    matches!(
                        RithmicConfig::from_env(RithmicEnv::Demo),
                        Err(ConfigError::InvalidValue { .. })
                    ),
                    "{value:?} should have been rejected"
                );
            });
        }
    }

    #[test]
    fn from_env_treats_a_zero_request_timeout_as_the_default() {
        let mut vars = demo_env_vars();
        vars.push((REQUEST_TIMEOUT_VAR, Some("0")));

        temp_env::with_vars(vars, || {
            let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();

            assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        });
    }

    #[test]
    fn the_builder_treats_a_zero_request_timeout_as_the_default() {
        let config = RithmicConfig::builder(RithmicEnv::Demo)
            .user("u")
            .password("p")
            .url("ws://localhost:9999")
            .beta_url("ws://localhost:9998")
            .app_name("a")
            .app_version("1")
            .request_timeout(Duration::ZERO)
            .build()
            .unwrap();

        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn test_from_env_demo_success() {
        temp_env::with_vars(demo_env_vars(), || {
            let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();

            assert_eq!(config.user, "demo_user");
            assert_eq!(config.password, "demo_password");
            assert_eq!(config.url, "wss://test-demo.example.com:443");
            assert_eq!(config.beta_url, "wss://test-demo-alt.example.com:443");
            assert_eq!(config.system_name, "Rithmic Paper Trading");
            assert_eq!(config.env, RithmicEnv::Demo);
        });
    }

    #[test]
    fn test_from_env_live_success() {
        temp_env::with_vars(live_env_vars(), || {
            let config = RithmicConfig::from_env(RithmicEnv::Live).unwrap();

            assert_eq!(config.user, "live_user");
            assert_eq!(config.password, "live_password");
            assert_eq!(config.system_name, "Rithmic 01");
            assert_eq!(config.env, RithmicEnv::Live);
        });
    }

    #[test]
    fn from_env_overrides_the_system_name_when_set() {
        let mut vars = live_env_vars();
        vars.push(("RITHMIC_LIVE_SYSTEM_NAME", Some("Thrive Trading")));

        temp_env::with_vars(vars, || {
            let config = RithmicConfig::from_env(RithmicEnv::Live).unwrap();

            assert_eq!(config.system_name, "Thrive Trading");
        });
    }

    #[test]
    fn test_account_from_env_missing_account_id() {
        temp_env::with_vars(
            vec![
                ("RITHMIC_DEMO_ACCOUNT_ID", None::<&str>),
                ("RITHMIC_DEMO_FCM_ID", Some("test_fcm")),
                ("RITHMIC_DEMO_IB_ID", Some("test_ib")),
                ("RITHMIC_DEMO_USER", Some("demo_user")),
                ("RITHMIC_DEMO_PW", Some("demo_password")),
                ("RITHMIC_DEMO_URL", Some("wss://test-demo.example.com:443")),
                (
                    "RITHMIC_DEMO_ALT_URL",
                    Some("wss://test-demo-alt.example.com:443"),
                ),
            ],
            || {
                let result = RithmicAccount::from_env(RithmicEnv::Demo);
                assert!(result.is_err());

                if let Err(ConfigError::MissingEnvVar(var)) = result {
                    assert_eq!(var, "RITHMIC_DEMO_ACCOUNT_ID");
                } else {
                    panic!("Expected MissingEnvVar error");
                }
            },
        );
    }

    #[test]
    fn test_from_env_missing_credentials() {
        temp_env::with_vars(
            vec![
                ("RITHMIC_DEMO_USER", None::<&str>),
                ("RITHMIC_DEMO_PW", None),
                ("RITHMIC_DEMO_URL", Some("wss://test-demo.example.com:443")),
                (
                    "RITHMIC_DEMO_ALT_URL",
                    Some("wss://test-demo-alt.example.com:443"),
                ),
            ],
            || {
                let result = RithmicConfig::from_env(RithmicEnv::Demo);
                assert!(result.is_err());

                if let Err(ConfigError::MissingEnvVar(var)) = result {
                    assert_eq!(var, "RITHMIC_DEMO_USER");
                } else {
                    panic!("Expected MissingEnvVar error");
                }
            },
        );
    }

    #[test]
    fn test_from_env_missing_url() {
        temp_env::with_vars(
            vec![
                ("RITHMIC_DEMO_USER", Some("demo_user")),
                ("RITHMIC_DEMO_PW", Some("demo_password")),
                ("RITHMIC_DEMO_URL", None::<&str>),
                ("RITHMIC_DEMO_ALT_URL", None),
            ],
            || {
                let result = RithmicConfig::from_env(RithmicEnv::Demo);
                assert!(result.is_err());

                if let Err(ConfigError::MissingEnvVar(var)) = result {
                    assert_eq!(var, "RITHMIC_DEMO_URL");
                } else {
                    panic!("Expected MissingEnvVar error");
                }
            },
        );
    }

    #[test]
    fn test_builder_missing_user() {
        let result = RithmicConfig::builder(RithmicEnv::Demo)
            .password("my_password")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .build();

        assert!(result.is_err());
        if let Err(ConfigError::MissingField(field)) = result {
            assert_eq!(field, "user");
        } else {
            panic!("Expected MissingField error");
        }
    }

    #[test]
    fn test_builder_demo_defaults() {
        let builder = RithmicConfigBuilder::new(RithmicEnv::Demo);
        let config = builder
            .user("test")
            .password("test")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        // Builder should set system_name default
        assert_eq!(config.system_name, "Rithmic Paper Trading");
    }

    #[test]
    fn test_builder_live_defaults() {
        let builder = RithmicConfigBuilder::new(RithmicEnv::Live);
        let config = builder
            .user("test")
            .password("test")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        // Builder should set system_name default
        assert_eq!(config.system_name, "Rithmic 01");
    }

    #[test]
    fn test_builder_test_defaults() {
        let builder = RithmicConfigBuilder::new(RithmicEnv::Test);
        let config = builder
            .user("test")
            .password("test")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        // Builder should set system_name default
        assert_eq!(config.system_name, "Rithmic Test");
    }

    #[test]
    fn test_debug_redacts_password() {
        let config = RithmicConfig::builder(RithmicEnv::Demo)
            .user("my_user")
            .password("super_secret_password")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        let debug_output = format!("{:?}", config);
        assert!(
            !debug_output.contains("super_secret_password"),
            "Debug output should not contain the actual password"
        );
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED] for the password"
        );
        // Other fields should still be visible
        assert!(debug_output.contains("my_user"));
    }
}
