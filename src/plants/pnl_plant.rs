use std::{convert::Infallible, sync::Arc};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info};

use crate::{
    ConnectStrategy,
    api::receiver_api::RithmicResponse,
    config::{LoginConfig, RithmicAccount, RithmicConfig},
    error::RithmicError,
    plants::{
        actor::Plant,
        await_first_response,
        kind::{Cx, PlantCommand, PlantKind},
        subscription::SubscriptionFilter,
    },
    request_handler::RequestResult,
    rti::{messages::RithmicMessage, request_login::SysInfraType, request_pn_l_position_updates},
};

/// Default subscription channel capacity.
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 10_000;

pub(crate) enum PnlPlantCommand {
    Close,
    Abort,
    GetSystemInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Login {
        config: LoginConfig,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    GetPnlPositionSnapshot {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    SubscribePnlUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UnsubscribePnlUpdates {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
}

/// The RithmicPnlPlant provides access to profit and loss (PnL) information through the Rithmic API.
///
/// It allows applications to:
/// - Retrieve current PnL information for positions
/// - Subscribe to real-time PnL updates
/// - Track position changes and risk metrics
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicAccount, RithmicConfig, RithmicEnv, ConnectStrategy, RithmicPnlPlant,
///     rti::messages::RithmicMessage,
/// };
/// use tokio::time::{sleep, Duration};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Step 1: Create connection configuration
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///     let account = RithmicAccount::from_env(RithmicEnv::Demo)?;
///
///     // Step 2: Connect to the PnL plant
///     let pnl_plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Retry).await?;
///
///     // Step 3: Get a handle to interact with the plant
///     let mut handle = pnl_plant.get_handle(&account);
///
///     // Step 4: Login to the PnL plant
///     handle.login().await?;
///
///     // Step 5: Get a current snapshot of all PnL positions
///     let snapshots = handle.get_pnl_position_snapshot().await?;
///     println!("PnL position snapshot: {:?}", snapshots);
///
///     // Step 6: Subscribe to ongoing PnL updates
///     handle.subscribe_pnl_updates().await?;
///
///     // Step 7: Process real-time PnL updates
///     for _ in 0..5 {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 match update.message {
///                     RithmicMessage::AccountPnLPositionUpdate(_) => {}
///                     RithmicMessage::InstrumentPnLPositionUpdate(_) => {}
///                     _ => {}
///                 }
///             },
///             Err(e) => println!("Error receiving update: {}", e),
///         }
///     }
///
///     // Step 8: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct RithmicPnlPlant {
    pub(crate) connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<PnlPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicPnlPlant {
    /// Create a new PnL Plant connection to access profit and loss information.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy; see [`ConnectStrategy`]
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicPnlPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] when
    /// its one attempt fails. `Retry` and `AlternateWithRetry` return it only
    /// once the config's
    /// [`retry_timeout`](crate::RithmicConfigBuilder::retry_timeout)
    /// passes, with the attempt count and the timeout in the message. Without
    /// one they retry until they connect, so this call can block
    /// indefinitely if the server is unreachable.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicPnlPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<PnlPlantCommand>(64);
        let capacity = config
            .subscription_capacity
            .unwrap_or(DEFAULT_SUBSCRIPTION_CAPACITY);
        let (sub_tx, _sub_rx) = broadcast::channel(capacity);
        let mut pnl_plant = Plant::new(PnlPlant, req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            pnl_plant.run().await;
        });

        Ok(RithmicPnlPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicPnlPlant {
    /// Wait for the plant's background connection task to finish.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }

    /// Get a handle to interact with the PnL plant.
    ///
    /// The handle provides methods to subscribe to PnL updates and retrieve position snapshots.
    /// Multiple handles can be created from the same plant for different accounts.
    pub fn get_handle(&self, account: &RithmicAccount) -> RithmicPnlPlantHandle {
        let account = Arc::new(account.clone());
        let account_for_filter = Arc::clone(&account);

        RithmicPnlPlantHandle {
            account,
            sender: self.sender.clone(),
            subscription_receiver: SubscriptionFilter::new(
                account_for_filter,
                self.subscription_sender.subscribe(),
            ),
        }
    }

    /// Subscribe to this plant's updates for every account, unfiltered.
    ///
    /// Unlike the handle from [`Self::get_handle`], which only yields updates
    /// for its own account, this receiver yields updates for every account on
    /// the login.
    pub fn subscribe_all(&self) -> broadcast::Receiver<RithmicResponse> {
        self.subscription_sender.subscribe()
    }
}

/// The PnL plant's commands. It loads nothing after login.
#[derive(Debug, Default)]
struct PnlPlant;

impl PlantKind for PnlPlant {
    type Command = PnlPlantCommand;
    type Tag = Infallible;

    const SOURCE: &'static str = "pnl_plant";
    const INFRA: SysInfraType = SysInfraType::PnlPlant;

    fn shared(command: PnlPlantCommand) -> Result<PlantCommand, PnlPlantCommand> {
        match command {
            PnlPlantCommand::Close => Ok(PlantCommand::Close),
            PnlPlantCommand::Abort => Ok(PlantCommand::Abort),
            PnlPlantCommand::GetSystemInfo { response_sender } => {
                Ok(PlantCommand::GetSystemInfo { response_sender })
            }
            PnlPlantCommand::Login {
                config,
                response_sender,
            } => Ok(PlantCommand::Login {
                config,
                response_sender,
            }),
            PnlPlantCommand::Logout { response_sender } => {
                Ok(PlantCommand::Logout { response_sender })
            }
            command => Err(command),
        }
    }

    fn on_command(&mut self, command: PnlPlantCommand, cx: &mut Cx<'_, Infallible>) {
        match command {
            PnlPlantCommand::SubscribePnlUpdates {
                account,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_pnl_position_updates(
                        request_pn_l_position_updates::Request::Subscribe,
                        &account,
                    )
                },
                response_sender,
            ),
            PnlPlantCommand::GetPnlPositionSnapshot {
                account,
                response_sender,
            } => cx.send_for(
                |api| api.request_pnl_position_snapshot(&account),
                response_sender,
            ),
            PnlPlantCommand::UnsubscribePnlUpdates {
                account,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_pnl_position_updates(
                        request_pn_l_position_updates::Request::Unsubscribe,
                        &account,
                    )
                },
                response_sender,
            ),
            PnlPlantCommand::Close
            | PnlPlantCommand::Abort
            | PnlPlantCommand::GetSystemInfo { .. }
            | PnlPlantCommand::Login { .. }
            | PnlPlantCommand::Logout { .. } => {
                unreachable!("the plant handles the commands every plant shares")
            }
        }
    }

    fn on_reply(&mut self, tag: Infallible, _reply: RequestResult) {
        match tag {}
    }
}

/// Handle for sending commands to a [`RithmicPnlPlant`] and receiving P&L updates.
///
/// Obtained from [`RithmicPnlPlant::get_handle`], one per account. Use the methods on this handle to
/// log in and subscribe to real-time P&L and position updates. Updates arrive on
/// [`subscription_receiver`](Self::subscription_receiver).
pub struct RithmicPnlPlantHandle {
    account: Arc<RithmicAccount>,
    sender: mpsc::Sender<PnlPlantCommand>,
    /// Receiver for real-time P&L and position updates.
    pub subscription_receiver: SubscriptionFilter,
}

impl std::fmt::Debug for RithmicPnlPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicPnlPlantHandle")
            .field("account", &self.account)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

impl RithmicPnlPlantHandle {
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
    pub async fn get_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::GetSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Log in to the Rithmic PnL plant
    ///
    /// This must be called before subscribing to any PnL data.
    ///
    /// The plant logs in once per connection. A call with the same config made
    /// while that login is in progress waits for it, and one made after it
    /// returns its response at once. Neither sends anything.
    ///
    /// # Returns
    /// The login response, once the server accepts the login.
    ///
    /// # Errors
    /// * The error the server's refusal carries, usually
    ///   [`RithmicError::RequestRejected`]. You can log in again.
    /// * [`RithmicError::LoginConflict`] if this plant is logging in, or is
    ///   logged in, with a different [`LoginConfig`].
    /// * [`RithmicError::ConnectionClosed`] if the plant disconnects before
    ///   the login is done, or has disconnected.
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic PnL plant with custom configuration
    ///
    /// This must be called before subscribing to any PnL data.
    /// `aggregated_quotes` does not apply to this plant and is ignored.
    ///
    /// # Arguments
    /// * `config` - Login configuration options. See [`LoginConfig`] for details.
    ///
    /// # Returns
    /// The login response, once the server accepts the login.
    ///
    /// # Errors
    /// As for [`login`](Self::login). [`RithmicError::LoginConflict`] means
    /// this plant logged in, or is logging in, with a config other than
    /// `config`.
    pub async fn login_with_config(
        &self,
        config: LoginConfig,
    ) -> Result<RithmicResponse, RithmicError> {
        info!("pnl_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();
        let mut config = config;

        config.aggregated_quotes = None;

        let command = PnlPlantCommand::Login {
            config,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = await_first_response(rx).await?;

        if let Some(err) = response.error.clone() {
            error!("pnl_plant: login failed {:?}", err);

            return Err(err);
        }

        // The actor owns the session: it heartbeats before this reply reaches
        // us, whether or not anyone is still waiting for it.
        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(session_id) = &resp.unique_user_id {
                info!("pnl_plant: session id: {}", session_id);
            }
        }

        info!("pnl_plant: logged in");

        Ok(response)
    }

    /// Disconnect from the Rithmic PnL plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        // Held rather than propagated here so that `Close` is queued either way —
        // see `RithmicOrderPlantHandle::disconnect`.
        let outcome = rx.await.map_err(|_| RithmicError::ConnectionClosed);
        let _ = self.sender.send(PnlPlantCommand::Close).await;

        outcome??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Immediately shut down the PnL plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and a graceful `disconnect()`
    /// would not get through.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    pub fn abort(&self) {
        let _ = self.sender.try_send(PnlPlantCommand::Abort);
    }

    /// Subscribe to PnL updates for all positions
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_pnl_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::SubscribePnlUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Request a snapshot of all current position PnL data
    ///
    /// # Returns
    /// The position snapshot response or an error message
    pub async fn get_pnl_position_snapshot(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::GetPnlPositionSnapshot {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Unsubscribe from PnL updates
    ///
    /// # Returns
    /// The unsubscription response or an error message
    pub async fn unsubscribe_pnl_updates(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::UnsubscribePnlUpdates {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }
}

impl Clone for RithmicPnlPlantHandle {
    fn clone(&self) -> Self {
        RithmicPnlPlantHandle {
            account: Arc::clone(&self.account),
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_receiver.resubscribe(),
        }
    }
}

#[cfg(test)]
mod tests;
