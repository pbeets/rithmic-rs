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

use std::{env, fmt, str::FromStr};

/// Trading environment selector.
///
/// Determines which Rithmic environment to connect to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum RithmicEnv {
    /// Rithmic Paper Trading (demo/development) environment.
    #[default]
    Demo,
    /// Rithmic 01 (live/production) environment.
    Live,
    /// Rithmic Test environment.
    Test,
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
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConfigError {
    /// The environment string could not be parsed.
    InvalidEnvironment(String),
    /// A configuration value was present but invalid.
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
/// ```ignore
/// use rithmic_rs::RithmicAccount;
///
/// let account = RithmicAccount::new("FCM_ID", "IB_ID", "ACCOUNT_ID");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
        let (account_id, fcm_id, ib_id) = match &env {
            RithmicEnv::Demo => (
                env::var("RITHMIC_DEMO_ACCOUNT_ID").map_err(|_| {
                    ConfigError::MissingEnvVar("RITHMIC_DEMO_ACCOUNT_ID".to_string())
                })?,
                env::var("RITHMIC_DEMO_FCM_ID")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_DEMO_FCM_ID".to_string()))?,
                env::var("RITHMIC_DEMO_IB_ID")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_DEMO_IB_ID".to_string()))?,
            ),
            RithmicEnv::Live => (
                env::var("RITHMIC_LIVE_ACCOUNT_ID").map_err(|_| {
                    ConfigError::MissingEnvVar("RITHMIC_LIVE_ACCOUNT_ID".to_string())
                })?,
                env::var("RITHMIC_LIVE_FCM_ID")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_LIVE_FCM_ID".to_string()))?,
                env::var("RITHMIC_LIVE_IB_ID")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_LIVE_IB_ID".to_string()))?,
            ),
            RithmicEnv::Test => (
                env::var("RITHMIC_TEST_ACCOUNT_ID").map_err(|_| {
                    ConfigError::MissingEnvVar("RITHMIC_TEST_ACCOUNT_ID".to_string())
                })?,
                env::var("RITHMIC_TEST_FCM_ID")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_TEST_FCM_ID".to_string()))?,
                env::var("RITHMIC_TEST_IB_ID")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_TEST_IB_ID".to_string()))?,
            ),
        };

        Ok(Self {
            account_id,
            fcm_id,
            ib_id,
        })
    }
}

/// Configuration for Rithmic connections.
///
/// This struct contains session-level connection and login details.
#[derive(Clone)]
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
}

impl fmt::Debug for RithmicConfig {
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
    pub fn from_env(env: RithmicEnv) -> Result<Self, ConfigError> {
        let (url, beta_url, user, password, system_name) = match &env {
            RithmicEnv::Demo => (
                env::var("RITHMIC_DEMO_URL")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_DEMO_URL".to_string()))?,
                env::var("RITHMIC_DEMO_ALT_URL")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_DEMO_ALT_URL".to_string()))?,
                env::var("RITHMIC_DEMO_USER")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_DEMO_USER".to_string()))?,
                env::var("RITHMIC_DEMO_PW")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_DEMO_PW".to_string()))?,
                "Rithmic Paper Trading".to_string(),
            ),
            RithmicEnv::Live => (
                env::var("RITHMIC_LIVE_URL")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_LIVE_URL".to_string()))?,
                env::var("RITHMIC_LIVE_ALT_URL")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_LIVE_ALT_URL".to_string()))?,
                env::var("RITHMIC_LIVE_USER")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_LIVE_USER".to_string()))?,
                env::var("RITHMIC_LIVE_PW")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_LIVE_PW".to_string()))?,
                "Rithmic 01".to_string(),
            ),
            RithmicEnv::Test => (
                env::var("RITHMIC_TEST_URL")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_TEST_URL".to_string()))?,
                env::var("RITHMIC_TEST_ALT_URL")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_TEST_ALT_URL".to_string()))?,
                env::var("RITHMIC_TEST_USER")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_TEST_USER".to_string()))?,
                env::var("RITHMIC_TEST_PW")
                    .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_TEST_PW".to_string()))?,
                "Rithmic Test".to_string(),
            ),
        };

        let app_name = env::var("RITHMIC_APP_NAME")
            .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_APP_NAME".to_string()))?;

        let app_version = env::var("RITHMIC_APP_VERSION")
            .map_err(|_| ConfigError::MissingEnvVar("RITHMIC_APP_VERSION".to_string()))?;

        Ok(Self {
            url,
            beta_url,
            user,
            password,
            system_name,
            env,
            app_name,
            app_version,
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
pub struct RithmicConfigBuilder {
    env: Option<RithmicEnv>,
    url: Option<String>,
    beta_url: Option<String>,
    user: Option<String>,
    password: Option<String>,
    system_name: Option<String>,
    app_name: Option<String>,
    app_version: Option<String>,
}

impl RithmicConfigBuilder {
    /// Create a new builder for the specified environment.
    pub fn new(env: RithmicEnv) -> Self {
        // Set system name default based on environment
        let system_name = match &env {
            RithmicEnv::Demo => "Rithmic Paper Trading".to_string(),
            RithmicEnv::Live => "Rithmic 01".to_string(),
            RithmicEnv::Test => "Rithmic Test".to_string(),
        };

        Self {
            env: Some(env),
            url: None,
            beta_url: None,
            user: None,
            password: None,
            system_name: Some(system_name),
            app_name: None,
            app_version: None,
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

    /// Set the application version string.
    pub fn app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = Some(app_version.into());
        self
    }

    /// Build the configuration.
    ///
    /// Returns an error if any required fields are missing.
    pub fn build(self) -> Result<RithmicConfig, ConfigError> {
        Ok(RithmicConfig {
            env: self
                .env
                .ok_or_else(|| ConfigError::MissingField("env".to_string()))?,
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
        })
    }
}

#[cfg(test)]
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
    fn test_account_new_complete() {
        let account = RithmicAccount::new("my_fcm", "my_ib", "my_account");

        assert_eq!(account.account_id, "my_account");
        assert_eq!(account.fcm_id, "my_fcm");
        assert_eq!(account.ib_id, "my_ib");
    }

    #[test]
    fn test_builder_complete() {
        let config = RithmicConfig::builder(RithmicEnv::Demo)
            .user("my_user")
            .password("my_password")
            .url("wss://test.example.com:443")
            .beta_url("wss://test-alt.example.com:443")
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        assert_eq!(config.user, "my_user");
        assert_eq!(config.password, "my_password");
        assert_eq!(config.env, RithmicEnv::Demo);
        assert_eq!(config.url, "wss://test.example.com:443");
        assert_eq!(config.beta_url, "wss://test-alt.example.com:443");
        // Builder should set system_name default
        assert_eq!(config.system_name, "Rithmic Paper Trading");
    }

    #[test]
    fn test_builder_custom_urls() {
        let config = RithmicConfig::builder(RithmicEnv::Demo)
            .user("my_user")
            .password("my_password")
            .url("wss://custom.example.com:443")
            .beta_url("wss://custom-beta.example.com:443")
            .system_name("Custom System")
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        assert_eq!(config.url, "wss://custom.example.com:443");
        assert_eq!(config.beta_url, "wss://custom-beta.example.com:443");
        assert_eq!(config.system_name, "Custom System");
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
    fn test_builder_into_string_conversions() {
        // Test that Into<String> works for builder methods
        let config = RithmicConfig::builder(RithmicEnv::Demo)
            .user(String::from("my_user"))
            .password(String::from("my_password"))
            .url(String::from("wss://test.example.com:443"))
            .beta_url(String::from("wss://test-alt.example.com:443"))
            .app_name("test_app")
            .app_version("1")
            .build()
            .unwrap();

        assert_eq!(config.user, "my_user");
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
