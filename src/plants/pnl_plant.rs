use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info};

use crate::{
    ConnectStrategy,
    api::receiver_api::RithmicResponse,
    config::{LoginConfig, RithmicAccount, RithmicConfig},
    error::RithmicError,
    plants::{
        await_first_response,
        core::{PlantActor, PlantCore, SelectResult},
        subscription::SubscriptionFilter,
    },
    rti::{messages::RithmicMessage, request_login::SysInfraType, request_pn_l_position_updates},
};

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
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UpdateHeartbeat {
        seconds: u64,
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
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicPnlPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// [`RithmicError::ConnectionFailed`] under [`ConnectStrategy::Simple`] only.
    /// `Retry` and `AlternateWithRetry` never return an error — they retry until
    /// they connect, so this call can block indefinitely if the server is
    /// unreachable. Wrap it in `tokio::time::timeout` if you need a deadline.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicPnlPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<PnlPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);
        let mut pnl_plant = PnlPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

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
}

#[derive(Debug)]
struct PnlPlant {
    core: PlantCore,
    request_receiver: mpsc::Receiver<PnlPlantCommand>,
}

impl PnlPlant {
    async fn new(
        request_receiver: mpsc::Receiver<PnlPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<PnlPlant, RithmicError> {
        let core = PlantCore::new(subscription_sender, config, strategy, "pnl_plant").await?;

        Ok(PnlPlant {
            core,
            request_receiver,
        })
    }
}

impl PlantActor for PnlPlant {
    type Command = PnlPlantCommand;

    async fn run(&mut self) {
        loop {
            let result = self.core.next_event(&mut self.request_receiver).await;
            let stop = match result {
                SelectResult::HeartbeatFired => self.core.send_heartbeat().await,
                SelectResult::PingFired => self.core.send_ping().await,
                SelectResult::PingTimeout => self.core.handle_ping_timeout(),
                SelectResult::Command(cmd) => {
                    if matches!(cmd, PnlPlantCommand::Abort) {
                        self.core.handle_abort()
                    } else {
                        self.handle_command(cmd).await;
                        false
                    }
                }
                SelectResult::RithmicMessage(msg) => self.core.handle_rithmic_message(msg).await,
                SelectResult::StreamClosed => self.core.handle_stream_closed(),
            };

            if stop {
                break;
            }
        }
    }

    async fn handle_command(&mut self, command: PnlPlantCommand) {
        // Disconnect race guard — see `TickerPlant::handle_command`.
        if self.core.close_requested
            && !matches!(
                command,
                PnlPlantCommand::Close
                    | PnlPlantCommand::SetLogin
                    | PnlPlantCommand::UpdateHeartbeat { .. }
                    | PnlPlantCommand::Abort
            )
        {
            debug!("pnl_plant: dropping a command queued after close was requested");

            return;
        }

        match command {
            PnlPlantCommand::Close => {
                self.core.handle_close().await;
            }
            PnlPlantCommand::GetSystemInfo { response_sender } => {
                self.core.handle_get_system_info(response_sender).await;
            }
            PnlPlantCommand::Login {
                config,
                response_sender,
            } => {
                self.core
                    .handle_login(config, SysInfraType::PnlPlant, response_sender)
                    .await;
            }
            PnlPlantCommand::SetLogin => {
                self.core.handle_set_login();
            }
            PnlPlantCommand::Logout { response_sender } => {
                self.core.handle_logout(response_sender).await;
            }
            PnlPlantCommand::UpdateHeartbeat { seconds } => {
                self.core.handle_update_heartbeat(seconds);
            }
            PnlPlantCommand::SubscribePnlUpdates {
                account,
                response_sender,
            } => {
                let (subscribe_buf, id) =
                    self.core.rithmic_sender_api.request_pnl_position_updates(
                        request_pn_l_position_updates::Request::Subscribe,
                        &account,
                    );

                self.core
                    .register_and_send(subscribe_buf, id, response_sender)
                    .await;
            }
            PnlPlantCommand::GetPnlPositionSnapshot {
                account,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_pnl_position_snapshot(&account);

                self.core
                    .register_and_send(snapshot_buf, id, response_sender)
                    .await;
            }
            PnlPlantCommand::UnsubscribePnlUpdates {
                account,
                response_sender,
            } => {
                let (unsubscribe_buf, id) =
                    self.core.rithmic_sender_api.request_pnl_position_updates(
                        request_pn_l_position_updates::Request::Unsubscribe,
                        &account,
                    );

                self.core
                    .register_and_send(unsubscribe_buf, id, response_sender)
                    .await;
            }
            PnlPlantCommand::Abort => {
                unreachable!("Abort is handled in run() before handle_command");
            }
        }
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
    /// This must be called before subscribing to any PnL data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in to the Rithmic PnL plant with custom configuration
    ///
    /// This must be called before subscribing to any PnL data.
    ///
    /// # Arguments
    /// * `config` - Login configuration options. See [`LoginConfig`] for details.
    ///
    /// # Returns
    /// The login response or an error message
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

        let _ = self.sender.send(PnlPlantCommand::SetLogin).await;

        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(hb) = resp.heartbeat_interval {
                let secs = hb as u64;
                self.update_heartbeat(secs).await;
            }

            if let Some(session_id) = &resp.unique_user_id {
                info!("pnl_plant: session id: {}", session_id);
            }
        }

        info!("pnl_plant: logged in");

        Ok(response)
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = PnlPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
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
