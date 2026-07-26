use std::sync::Arc;

use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::{
    ConnectStrategy,
    api::{receiver_api::RithmicResponse, rithmic_command_types::LoginConfig},
    config::{RithmicAccount, RithmicConfig},
    error::RithmicError,
    plants::{
        core::{PlantCore, SelectResult},
        subscription::{InitialReceiver, SubscriptionFilter},
    },
    request_handler::RithmicRequest,
    rti::{messages::RithmicMessage, request_login::SysInfraType, request_pn_l_position_updates},
    ws::{HEARTBEAT_SECS, PlantActor},
};

use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::sleep_until,
};

pub(crate) enum PnlPlantCommand {
    Close,
    Abort,
    ListSystemInfo {
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
    PnlPositionSnapshots {
        account: Arc<RithmicAccount>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, RithmicError>>,
    },
    UpdateHeartbeat {
        seconds: u64,
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
///     let snapshots = handle.pnl_position_snapshots().await?;
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
    initial_receiver: InitialReceiver,
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
    /// Returns an error if unable to establish WebSocket connection to the server.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicPnlPlant, RithmicError> {
        let (req_tx, req_rx) = mpsc::channel::<PnlPlantCommand>(64);
        let (sub_tx, sub_rx) = broadcast::channel(10_000);
        let mut pnl_plant = PnlPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            pnl_plant.run().await;
        });

        Ok(RithmicPnlPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
            initial_receiver: InitialReceiver::new(sub_rx),
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
    ///
    /// # Take every handle before logging in
    ///
    /// Only the first handle carries the backlog broadcast since
    /// [`connect`](Self::connect); every later handle starts at the current end
    /// of the stream, and the first handle filters that backlog down to its own
    /// account. An application tracking several accounts must therefore call
    /// `get_handle` for all of them before `login()`, otherwise the accounts
    /// whose handles are taken later miss every update broadcast up to that
    /// point.
    pub fn get_handle(&self, account: &RithmicAccount) -> RithmicPnlPlantHandle {
        let account = Arc::new(account.clone());
        let account_for_filter = Arc::clone(&account);

        RithmicPnlPlantHandle {
            account,
            sender: self.sender.clone(),
            subscription_receiver: SubscriptionFilter::new(
                account_for_filter,
                self.initial_receiver
                    .take_or_subscribe(&self.subscription_sender),
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
            let result = {
                let interval = &mut self.core.interval;
                let ping_interval = &mut self.core.ping_interval;
                let ping_manager = &mut self.core.ping_manager;
                let reader = &mut self.core.rithmic_reader;
                let receiver = &mut self.request_receiver;
                tokio::select! {
                    _ = interval.tick()      => SelectResult::HeartbeatFired,
                    _ = ping_interval.tick() => SelectResult::PingFired,
                    _ = async {
                        if let Some(t) = ping_manager.next_timeout_at() {
                            sleep_until(t).await
                        } else {
                            std::future::pending::<()>().await
                        }
                    } => SelectResult::PingTimeout,
                    Some(cmd) = receiver.recv() => SelectResult::Command(cmd),
                    msg = reader.next() => match msg {
                        Some(m) => SelectResult::RithmicMessage(m),
                        None => SelectResult::StreamClosed,
                    },
                }
            };
            let stop = match result {
                SelectResult::HeartbeatFired => self.core.send_heartbeat().await,
                SelectResult::PingFired => self.core.send_ping().await,
                SelectResult::PingTimeout => {
                    if self.core.ping_manager.check_timeout() {
                        if self.core.close_requested {
                            warn!(
                                "pnl_plant: ping timed out while waiting for server close echo — terminating"
                            );

                            self.core.request_handler.drain_and_drop();
                        } else {
                            self.core.fail_connection_and_drain(
                                "websocket_ping_timeout",
                                RithmicError::HeartbeatTimeout,
                            );
                        }
                        true
                    } else {
                        false
                    }
                }
                SelectResult::Command(cmd) => {
                    if matches!(cmd, PnlPlantCommand::Abort) {
                        info!("pnl_plant: abort requested, shutting down immediately");

                        self.core
                            .fail_connection_and_drain("", RithmicError::ConnectionClosed);
                        true
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
        match command {
            PnlPlantCommand::Close => {
                self.core.handle_close().await;
            }
            PnlPlantCommand::ListSystemInfo { response_sender } => {
                self.core.handle_list_system_info(response_sender).await;
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

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(subscribe_buf.into()), &id)
                    .await;
            }
            PnlPlantCommand::PnlPositionSnapshots {
                account,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .core
                    .rithmic_sender_api
                    .request_pnl_position_snapshot(&account);

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(snapshot_buf.into()), &id)
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

                self.core.request_handler.register_request(RithmicRequest {
                    request_id: id.clone(),
                    responder: response_sender,
                });

                self.core
                    .send_or_fail(Message::Binary(unsubscribe_buf.into()), &id)
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
/// Obtained from [`RithmicPnlPlant::connect()`]. Use the methods on this handle to
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
    pub async fn list_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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
        let response = rx
            .await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)?;

        if let Some(err) = response.error.clone() {
            error!("pnl_plant: login failed {:?}", err);

            return Err(err);
        }

        let _ = self.sender.send(PnlPlantCommand::SetLogin).await;

        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(hb) = resp.heartbeat_interval {
                let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
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
        let r = rx.await.map_err(|_| RithmicError::ConnectionClosed)??;
        let _ = self.sender.send(PnlPlantCommand::Close).await;

        r.into_iter().next().ok_or(RithmicError::EmptyResponse)
    }

    /// Shut down the PnL plant actor without a graceful logout.
    ///
    /// Use when the connection is known to be dead and `disconnect()` would hang.
    /// All pending request callers will receive an error. The subscription channel
    /// receives a `ConnectionError` notification. Safe to call if the actor is already dead.
    ///
    /// # How long this takes
    ///
    /// The abort is queued on the command channel and the actor takes commands
    /// in order, so it waits behind everything already queued — up to 64
    /// commands, each of which can hold the actor for the WebSocket send
    /// timeout. It does not jump the queue. On a dead link the call is usually
    /// released by the actor stopping itself once a ping goes unanswered, which
    /// drops the receiver, rather than by the queue draining.
    ///
    /// # Cancel safety
    ///
    /// This method is **not** cancel safe. Nothing is queued if the returned
    /// future is dropped before it completes, so an abort raced in `select!` or
    /// wrapped in `timeout` is silently not delivered when the other branch
    /// wins. Poll it to completion.
    pub async fn abort(&self) {
        let _ = self.sender.send(PnlPlantCommand::Abort).await;
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Request a snapshot of all current position PnL data
    ///
    /// # Returns
    /// The position snapshot response or an error message
    pub async fn pnl_position_snapshots(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = PnlPlantCommand::PnlPositionSnapshots {
            account: self.account.clone(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
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

        rx.await
            .map_err(|_| RithmicError::ConnectionClosed)??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health_event(request_id: &str) -> RithmicResponse {
        RithmicResponse {
            request_id: request_id.to_string(),
            message: RithmicMessage::HeartbeatTimeout,
            error: None,
            is_update: true,
            has_more: false,
            multi_response: false,
            source: "pnl_plant".to_string(),
        }
    }

    /// Every message asserted on below is already buffered by the time the
    /// assertion runs, so a wait means it was dropped. The timeout turns that
    /// into a failure instead of a hung suite.
    async fn next_soon(filter: &mut SubscriptionFilter) -> RithmicResponse {
        tokio::time::timeout(std::time::Duration::from_secs(5), filter.recv())
            .await
            .expect("the message must already be buffered")
            .expect("the subscription stream must stay open")
    }

    #[tokio::test]
    async fn first_handle_receives_updates_broadcast_before_it_existed() {
        let (sender, command_receiver) = mpsc::channel(4);
        let (sub_tx, sub_rx) = broadcast::channel(16);

        let plant = RithmicPnlPlant {
            connection_handle: tokio::spawn(async move {
                let _keep_open = command_receiver;
            }),
            sender,
            subscription_sender: sub_tx.clone(),
            initial_receiver: InitialReceiver::new(sub_rx),
        };
        let account = RithmicAccount::new("FCM_A", "IB_A", "ACCOUNT_A");

        // Emitted during the window between connect() and the first get_handle().
        sub_tx
            .send(health_event("early"))
            .expect("send must reach the parked receiver");

        let mut first = plant.get_handle(&account);
        let mut second = plant.get_handle(&account);

        sub_tx.send(health_event("late")).unwrap();

        assert_eq!(
            next_soon(&mut first.subscription_receiver).await.request_id,
            "early"
        );
        assert_eq!(
            next_soon(&mut first.subscription_receiver).await.request_id,
            "late"
        );

        // Handles after the first subscribe at the tail, so "early" is not
        // replayed and the first thing this handle sees is "late".
        assert_eq!(
            next_soon(&mut second.subscription_receiver)
                .await
                .request_id,
            "late"
        );
    }

    #[tokio::test]
    async fn abort_queues_behind_a_full_command_channel() {
        let account = Arc::new(RithmicAccount::new("FCM_A", "IB_A", "ACCOUNT_A"));
        let (sender, mut command_receiver) = mpsc::channel(4);
        let (_sub_tx, sub_rx) = broadcast::channel(4);

        let handle = RithmicPnlPlantHandle {
            account: Arc::clone(&account),
            sender,
            subscription_receiver: SubscriptionFilter::new(account, sub_rx),
        };

        // Fill the command channel so there is no room for another command.
        while handle.sender.try_send(PnlPlantCommand::SetLogin).is_ok() {}

        let abort = handle.abort();
        tokio::pin!(abort);

        // Polled to completion here only if the abort gave up on the full
        // channel; still pending means it is holding out for a slot.
        assert!(
            futures_util::poll!(abort.as_mut()).is_pending(),
            "abort must wait for room rather than give up on a full channel"
        );

        // Free one slot; the abort claims it.
        assert!(matches!(
            command_receiver.recv().await,
            Some(PnlPlantCommand::SetLogin)
        ));
        assert!(futures_util::poll!(abort.as_mut()).is_ready());

        let mut saw_abort = false;
        while let Ok(command) = command_receiver.try_recv() {
            if matches!(command, PnlPlantCommand::Abort) {
                saw_abort = true;
            }
        }

        assert!(saw_abort, "abort must be queued once room appears");
    }
}
