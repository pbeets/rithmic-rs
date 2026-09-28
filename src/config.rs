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
//!     .url("wss://<demo url from Rithmic>")
//!     .beta_url("wss://<demo alt url from Rithmic>")
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

/// Which Rithmic environment a config is for.
///
/// It picks the environment variables [`RithmicConfig::from_env`] and
/// [`RithmicAccount::from_env`] read (`RITHMIC_DEMO_*`, `RITHMIC_LIVE_*`,
/// `RITHMIC_TEST_*`) and the default system name. It does not pick the server:
/// that is the URL you supply.
///
/// Parses from `"demo"` or `"development"`, `"live"` or `"production"`, and
/// `"test"` (lowercase only), and displays as `demo`, `live` or `test`.
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

/// Why a config could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// Parsing a [`RithmicEnv`] from a string failed. Holds the string.
    InvalidEnvironment(String),
    /// A configuration value was present but invalid. Today only
    /// `RITHMIC_REQUEST_TIMEOUT_SECS` is checked this way.
    #[non_exhaustive]
    InvalidValue {
        /// The variable or field name.
        var: String,
        /// Why the value was rejected.
        reason: String,
    },
    /// A required environment variable was not set, or was not valid
    /// Unicode. Holds the variable name.
    MissingEnvVar(String),
    /// [`RithmicConfigBuilder::build`] was called without a required field.
    /// Holds the field name, e.g. `"beta_url"`.
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
    /// Create an account identity. Note the argument order: FCM, IB, then
    /// account.
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

    /// Load the account identity from `RITHMIC_{DEMO,LIVE,TEST}_ACCOUNT_ID`,
    /// `_FCM_ID` and `_IB_ID`, using the prefix for `env`.
    ///
    /// Returns [`ConfigError::MissingEnvVar`] naming the first one not set.
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
/// A plant logs in once per connection. Calling `login_with_config` again with
/// a different config returns [`RithmicError::LoginConflict`](crate::RithmicError::LoginConflict),
/// so pick the config before the first login.
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     ConnectStrategy, LoginConfig, RithmicConfig, RithmicEnv, RithmicTickerPlant,
/// };
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
/// let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
/// let handle = plant.get_handle();
///
/// // Aggregated quotes. For tick-by-tick quotes, call `handle.login()` instead.
/// let mut login = LoginConfig::default();
/// login.aggregated_quotes = Some(true);
/// handle.login_with_config(login).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LoginConfig {
    /// Aggregated rather than tick-by-tick quotes. Unset means tick-by-tick.
    /// Ticker plant only; the other plants ignore it.
    pub aggregated_quotes: Option<bool>,
    /// MAC addresses reported to Rithmic. None are sent when unset.
    pub mac_addr: Option<Vec<String>>,
    /// OS version reported to Rithmic. Left out of the login when unset.
    pub os_version: Option<String>,
    /// OS platform reported to Rithmic. Left out of the login when unset.
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

/// Where to connect and how to log in. One config can be shared by every
/// plant you connect.
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
    /// Primary WebSocket URL, e.g. `wss://...:443`. Rithmic supplies it.
    pub url: String,
    /// Second WebSocket URL, used only by
    /// [`ConnectStrategy::AlternateWithRetry`](crate::ConnectStrategy::AlternateWithRetry).
    /// Still required by [`RithmicConfigBuilder::build`] under the other
    /// strategies.
    pub beta_url: String,
    /// Login username.
    pub user: String,
    /// Login password.
    pub password: String,
    /// Rithmic system to log in to (e.g. "Rithmic Paper Trading"). It must
    /// match a name the server offers; `get_system_info` on any plant handle
    /// lists them.
    pub system_name: String,
    /// The environment this config was built for. Nothing reads it after
    /// building; `url` and `system_name` decide where you connect.
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
    /// Capacity of each plant's subscription broadcast channel, or `None` for
    /// the default of 10,000. Set with
    /// [`RithmicConfigBuilder::subscription_capacity`], which explains what
    /// the capacity costs.
    pub subscription_capacity: Option<usize>,
    /// How long [`ConnectStrategy::Retry`](crate::ConnectStrategy::Retry) and
    /// [`ConnectStrategy::AlternateWithRetry`](crate::ConnectStrategy::AlternateWithRetry)
    /// keep trying before `connect` gives up with
    /// [`RithmicError::ConnectionFailed`](crate::RithmicError::ConnectionFailed).
    /// The limit covers every attempt together, not each one. `None`, the
    /// default, retries until connected. Set it with
    /// [`RithmicConfigBuilder::retry_timeout`].
    pub retry_timeout: Option<Duration>,
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
            .field("subscription_capacity", &self.subscription_capacity)
            .field("retry_timeout", &self.retry_timeout)
            .finish()
    }
}

impl RithmicConfig {
    /// Create a configuration by loading values from environment variables.
    ///
    /// Returns [`ConfigError::MissingEnvVar`] naming the first required
    /// variable not set. See [`examples/.env.blank`](https://github.com/pbeets/rithmic-rs/blob/main/examples/.env.blank)
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
            subscription_capacity: None,
            retry_timeout: None,
        })
    }

    /// Create a builder to set every value in code. Same as
    /// [`RithmicConfigBuilder::new`].
    ///
    /// # Example
    /// ```no_run
    /// use rithmic_rs::config::{RithmicConfig, RithmicEnv};
    ///
    /// let config = RithmicConfig::builder(RithmicEnv::Demo)
    ///     .url("wss://<demo url from Rithmic>")
    ///     .beta_url("wss://<demo alt url from Rithmic>")
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

/// Builder for a [`RithmicConfig`].
///
/// Start empty with [`RithmicConfig::builder`], or pre-filled from the
/// environment with [`RithmicConfigBuilder::from_env`]. Setters can be called
/// in any order; a later call replaces an earlier one.
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
    subscription_capacity: Option<usize>,
    retry_timeout: Option<Duration>,
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
            subscription_capacity: config.subscription_capacity,
            retry_timeout: config.retry_timeout,
        })
    }

    /// Create an empty builder for `env`. Only `system_name` starts filled
    /// in, with the environment's default ("Rithmic Paper Trading",
    /// "Rithmic 01" or "Rithmic Test").
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
            subscription_capacity: None,
            retry_timeout: None,
        }
    }

    /// Set the primary WebSocket URL. Required.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set the second WebSocket URL, the one
    /// [`ConnectStrategy::AlternateWithRetry`](crate::ConnectStrategy::AlternateWithRetry)
    /// alternates to. Required even if you never use that strategy.
    pub fn beta_url(mut self, beta_url: impl Into<String>) -> Self {
        self.beta_url = Some(beta_url.into());
        self
    }

    /// Set the username. Required.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set the password. Required. [`RithmicConfig`]'s `Debug` output hides
    /// it.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the Rithmic system to log in to, overriding the environment's
    /// default. See [`RithmicConfig::system_name`].
    pub fn system_name(mut self, system_name: impl Into<String>) -> Self {
        self.system_name = Some(system_name.into());
        self
    }

    /// Set the application name registered with Rithmic. Required.
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

    /// Give up connecting after `timeout` instead of retrying forever.
    ///
    /// The timeout covers the whole retry loop, every attempt and backoff
    /// together, not a single attempt.
    ///
    /// It bounds only `connect()`. `login()` and requests never time out; wrap
    /// them in `tokio::time::timeout` (see `examples/request_timeout.rs`).
    /// Each `connect()` call starts its own clock.
    ///
    /// Applies to [`ConnectStrategy::Retry`](crate::ConnectStrategy::Retry)
    /// and [`ConnectStrategy::AlternateWithRetry`](crate::ConnectStrategy::AlternateWithRetry),
    /// which keep their usual backoff but cap each attempt's timeout at the
    /// time left and stop before a backoff that would run past the timeout.
    /// At least one attempt is always made. `connect` then returns
    /// [`RithmicError::ConnectionFailed`](crate::RithmicError::ConnectionFailed)
    /// with the attempt count and the timeout in its message.
    /// [`ConnectStrategy::Simple`](crate::ConnectStrategy::Simple) makes one
    /// attempt either way and ignores this.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use rithmic_rs::{
    ///     ConnectStrategy, RithmicConfigBuilder, RithmicEnv, RithmicTickerPlant,
    /// };
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = RithmicConfigBuilder::from_env(RithmicEnv::Demo)?
    ///     .retry_timeout(Duration::from_secs(30))
    ///     .build()?;
    ///
    /// // Retries for up to 30 seconds, then returns ConnectionFailed.
    /// let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = Some(timeout);
        self
    }

    /// Set the application version sent at login. Required.
    pub fn app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = Some(app_version.into());
        self
    }

    /// Set the capacity of each plant's subscription broadcast channel.
    ///
    /// Every plant allocates its channel up front, when it connects. Tokio
    /// rounds the capacity up to the next power of two and allocates every
    /// slot eagerly, and each slot holds a
    /// [`RithmicResponse`](crate::RithmicResponse) of about 1.3 KB. The default
    /// of 10,000 becomes 16,384 slots, about 22 MB per plant. Lower the
    /// capacity to save memory when you run many plants.
    ///
    /// A subscriber that falls more than `capacity` messages behind misses
    /// the oldest ones: its next `recv` returns
    /// [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
    /// with the number skipped. Raise the capacity if your consumer is bursty.
    ///
    /// `0` leaves the plant defaults in place.
    pub fn subscription_capacity(mut self, capacity: usize) -> Self {
        self.subscription_capacity = if capacity == 0 { None } else { Some(capacity) };
        self
    }

    /// Build the configuration.
    ///
    /// Returns [`ConfigError::MissingField`] if `url`, `beta_url`, `user`,
    /// `password`, `app_name` or `app_version` was never set.
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
            subscription_capacity: self.subscription_capacity,
            retry_timeout: self.retry_timeout,
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
    fn test_rithmic_env_from_str() {
        for (env, name) in [
            (RithmicEnv::Demo, "demo"),
            (RithmicEnv::Live, "live"),
            (RithmicEnv::Test, "test"),
        ] {
            assert_eq!(env.to_string(), name);
            assert_eq!(name.parse::<RithmicEnv>().unwrap(), env);
        }

        assert_eq!(
            "development".parse::<RithmicEnv>().unwrap(),
            RithmicEnv::Demo
        );
        assert_eq!(
            "production".parse::<RithmicEnv>().unwrap(),
            RithmicEnv::Live
        );

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

    fn builder_with_required_fields() -> RithmicConfigBuilder {
        RithmicConfig::builder(RithmicEnv::Demo)
            .user("u")
            .password("p")
            .url("ws://localhost:9999")
            .beta_url("ws://localhost:9998")
            .app_name("a")
            .app_version("1")
    }

    #[test]
    fn the_builder_sets_the_subscription_capacity() {
        let config = builder_with_required_fields()
            .subscription_capacity(1_024)
            .build()
            .unwrap();

        assert_eq!(config.subscription_capacity, Some(1_024));
    }

    #[test]
    fn the_builder_treats_a_zero_subscription_capacity_as_unset() {
        let config = builder_with_required_fields()
            .subscription_capacity(0)
            .build()
            .unwrap();

        assert_eq!(config.subscription_capacity, None);
    }

    #[test]
    fn from_env_leaves_the_subscription_capacity_unset() {
        temp_env::with_vars(demo_env_vars(), || {
            let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
            assert_eq!(config.subscription_capacity, None);

            let config = RithmicConfigBuilder::from_env(RithmicEnv::Demo)
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(config.subscription_capacity, None);
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
    fn from_env_reads_the_test_prefix_for_the_test_env() {
        temp_env::with_vars(
            vec![
                ("RITHMIC_TEST_USER", Some("test_user")),
                ("RITHMIC_TEST_PW", Some("test_password")),
                ("RITHMIC_TEST_URL", Some("wss://test-test.example.com:443")),
                (
                    "RITHMIC_TEST_ALT_URL",
                    Some("wss://test-test-alt.example.com:443"),
                ),
                ("RITHMIC_TEST_SYSTEM_NAME", None),
                ("RITHMIC_APP_NAME", Some("test_app")),
                ("RITHMIC_APP_VERSION", Some("1")),
            ],
            || {
                let config = RithmicConfig::from_env(RithmicEnv::Test).unwrap();

                assert_eq!(config.user, "test_user");
                assert_eq!(config.password, "test_password");
                assert_eq!(config.url, "wss://test-test.example.com:443");
                assert_eq!(config.beta_url, "wss://test-test-alt.example.com:443");
                assert_eq!(config.system_name, "Rithmic Test");
                assert_eq!(config.env, RithmicEnv::Test);
            },
        );
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
    fn the_builder_defaults_the_system_name_per_env() {
        for (env, system_name) in [
            (RithmicEnv::Demo, "Rithmic Paper Trading"),
            (RithmicEnv::Live, "Rithmic 01"),
            (RithmicEnv::Test, "Rithmic Test"),
        ] {
            let config = RithmicConfigBuilder::new(env)
                .user("test")
                .password("test")
                .url("wss://test.example.com:443")
                .beta_url("wss://test-alt.example.com:443")
                .app_name("test_app")
                .app_version("1")
                .build()
                .unwrap();

            assert_eq!(config.system_name, system_name, "{env}");
        }
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
