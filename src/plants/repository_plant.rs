//! Optional, user-scoped connection for reviewing and accepting Rithmic agreements.

use std::convert::Infallible;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info};

use crate::{
    ConnectStrategy,
    api::receiver_api::RithmicResponse,
    config::{LoginConfig, RithmicConfig},
    error::RithmicError,
    plants::{
        actor::Plant,
        await_all_responses, await_first_response,
        kind::{Cx, PlantCommand, PlantKind},
    },
    request_handler::{RequestResult, Responder},
    rti::{messages::RithmicMessage, request_login::SysInfraType},
    types::MarketDataUsageCapacity,
};

// This plant has connection events but no high-volume market data feed.
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 64;

/// What a [`RithmicRepositoryPlantHandle`] asks the plant's task to do.
pub(crate) enum RepositoryPlantCommand {
    Close,
    Abort,
    GetSystemInfo {
        response_sender: Responder,
    },
    Login {
        config: LoginConfig,
        response_sender: Responder,
    },
    Logout {
        response_sender: Responder,
    },
    ListUnacceptedAgreements(Responder),
    ListAcceptedAgreements(Responder),
    ShowAgreement {
        agreement_id: String,
        response_sender: Responder,
    },
    AcceptAgreement {
        agreement_id: String,
        capacity: Option<MarketDataUsageCapacity>,
        response_sender: Responder,
    },
    SetMarketDataSelfCertStatus {
        agreement_id: String,
        capacity: MarketDataUsageCapacity,
        response_sender: Responder,
    },
}

/// Rithmic's repository service for first-use account agreements.
///
/// Connect this plant only when you need to review or accept agreements, then
/// disconnect it before opening your trading plants. No other plant connects
/// it automatically. It shares the same configuration and connection strategy
/// as the other plants and needs no trading account identifiers.
///
/// Connecting and logging in never accepts an agreement or changes the user's
/// market data certification. Those actions require explicit handle calls.
/// Agreement methods exist only on this plant, not the order plant.
///
/// ```no_run
/// use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicRepositoryPlant};
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
/// let plant = RithmicRepositoryPlant::connect(&config, ConnectStrategy::Simple).await?;
/// let handle = plant.get_handle();
/// handle.login().await?;
/// let agreements = handle.list_unaccepted_agreements().await?;
/// // Present each agreement to the user before calling accept_agreement.
/// handle.disconnect().await?;
/// plant.await_shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RithmicRepositoryPlant {
    connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<RepositoryPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicRepositoryPlant {
    /// Open an optional repository connection, without logging in.
    ///
    /// # Errors
    /// Returns [`RithmicError::ConnectionFailed`] if the connection strategy
    /// exhausts its attempts. Retrying strategies can wait indefinitely unless
    /// the config sets [`retry_timeout`](crate::RithmicConfigBuilder::retry_timeout).
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<Self, RithmicError> {
        let (sender, receiver) = mpsc::channel(64);
        let capacity = config
            .subscription_capacity
            .unwrap_or(DEFAULT_SUBSCRIPTION_CAPACITY);
        let (subscription_sender, _) = broadcast::channel(capacity);
        let mut plant = Plant::new(
            RepositoryPlant,
            receiver,
            subscription_sender.clone(),
            config,
            strategy,
        )
        .await?;
        let connection_handle = tokio::spawn(async move { plant.run().await });

        Ok(Self {
            connection_handle,
            sender,
            subscription_sender,
        })
    }

    /// Create a handle to the same user session, with its own event receiver.
    pub fn get_handle(&self) -> RithmicRepositoryPlantHandle {
        RithmicRepositoryPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }

    /// Wait for the background task to finish after disconnect, abort or loss
    /// of the connection.
    pub async fn await_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.connection_handle.await
    }
}

#[derive(Debug, Default)]
struct RepositoryPlant;

impl PlantKind for RepositoryPlant {
    type Command = RepositoryPlantCommand;
    type Tag = Infallible;

    const SOURCE: &'static str = "repository_plant";
    const INFRA: SysInfraType = SysInfraType::RepositoryPlant;

    fn shared(command: Self::Command) -> Result<PlantCommand, Self::Command> {
        match command {
            RepositoryPlantCommand::Close => Ok(PlantCommand::Close),
            RepositoryPlantCommand::Abort => Ok(PlantCommand::Abort),

            RepositoryPlantCommand::GetSystemInfo { response_sender } => {
                Ok(PlantCommand::GetSystemInfo { response_sender })
            }

            RepositoryPlantCommand::Login {
                config,
                response_sender,
            } => Ok(PlantCommand::Login {
                config,
                response_sender,
            }),

            RepositoryPlantCommand::Logout { response_sender } => {
                Ok(PlantCommand::Logout { response_sender })
            }

            command => Err(command),
        }
    }

    fn on_command(&mut self, command: Self::Command, cx: &mut Cx<'_, Infallible>) {
        match command {
            RepositoryPlantCommand::ListUnacceptedAgreements(response_sender) => cx.send_for(
                |api| api.request_list_unaccepted_agreements(),
                response_sender,
            ),
            RepositoryPlantCommand::ListAcceptedAgreements(response_sender) => cx.send_for(
                |api| api.request_list_accepted_agreements(),
                response_sender,
            ),
            RepositoryPlantCommand::ShowAgreement {
                agreement_id,
                response_sender,
            } => cx.send_for(
                |api| api.request_show_agreement(&agreement_id),
                response_sender,
            ),
            RepositoryPlantCommand::AcceptAgreement {
                agreement_id,
                capacity,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_accept_agreement(&agreement_id, capacity.map(|c| c.as_str_name()))
                },
                response_sender,
            ),
            RepositoryPlantCommand::SetMarketDataSelfCertStatus {
                agreement_id,
                capacity,
                response_sender,
            } => cx.send_for(
                |api| {
                    api.request_set_rithmic_mrkt_data_self_cert_status(
                        &agreement_id,
                        capacity.as_str_name(),
                    )
                },
                response_sender,
            ),
            RepositoryPlantCommand::Close
            | RepositoryPlantCommand::Abort
            | RepositoryPlantCommand::GetSystemInfo { .. }
            | RepositoryPlantCommand::Login { .. }
            | RepositoryPlantCommand::Logout { .. } => {
                unreachable!("the plant handles the commands every plant shares")
            }
        }
    }

    fn on_reply(&mut self, tag: Infallible, _reply: RequestResult) {
        match tag {}
    }
}

/// Commands and connection events for a [`RithmicRepositoryPlant`].
///
/// Log in before making agreement requests. As on the other plants, a server
/// refusal returns `Ok` with [`RithmicResponse::error`] set; check every frame
/// of a multipart reply. Only login converts a refusal into `Err`.
/// Requests have no built-in deadline; use [`tokio::time::timeout`] if needed.
pub struct RithmicRepositoryPlantHandle {
    sender: mpsc::Sender<RepositoryPlantCommand>,
    /// Connection events, including forced logout and connection loss.
    /// Agreement replies are returned directly by the methods below.
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl std::fmt::Debug for RithmicRepositoryPlantHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicRepositoryPlantHandle")
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

impl Clone for RithmicRepositoryPlantHandle {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_receiver.resubscribe(),
        }
    }
}

impl RithmicRepositoryPlantHandle {
    /// Ask which Rithmic systems the gateway offers. Works before login.
    ///
    /// The reply is a [`RithmicMessage::ResponseRithmicSystemInfo`].
    pub async fn get_system_info(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::GetSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Log in without fetching or accepting agreements.
    ///
    /// Repeated calls with the same config share the login and its reply.
    ///
    /// # Returns
    /// The login response, once the server accepts the login.
    ///
    /// # Errors
    /// * The error the server's refusal carries, usually
    ///   [`RithmicError::RequestRejected`].
    /// * [`RithmicError::LoginConflict`] for a different [`LoginConfig`] than
    ///   the one already logging in or logged in.
    /// * [`RithmicError::ConnectionClosed`] if the plant disconnects first, or
    ///   already has.
    pub async fn login(&self) -> Result<RithmicResponse, RithmicError> {
        self.login_with_config(LoginConfig::default()).await
    }

    /// Log in with device and OS metadata. `aggregated_quotes` does not apply
    /// to this plant and is ignored.
    ///
    /// # Returns
    /// The login response, once the server accepts the login.
    ///
    /// # Errors
    /// As for [`login`](Self::login).
    pub async fn login_with_config(
        &self,
        config: LoginConfig,
    ) -> Result<RithmicResponse, RithmicError> {
        info!("repository_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();
        let mut config = config;

        config.aggregated_quotes = None;

        let command = RepositoryPlantCommand::Login {
            config,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = await_first_response(rx).await?;

        if let Some(err) = response.error.clone() {
            error!("repository_plant: login failed {:?}", err);

            return Err(err);
        }

        if let RithmicMessage::ResponseLogin(resp) = &response.message {
            if let Some(session_id) = &resp.unique_user_id {
                info!("repository_plant: session id: {}", session_id);
            }
        }

        info!("repository_plant: logged in");

        Ok(response)
    }

    /// List unaccepted agreements.
    ///
    /// Titles, IDs and acceptance requests are in
    /// [`RithmicMessage::ResponseListUnacceptedAgreements`].
    ///
    /// # Returns
    /// Every frame of the reply, in order, ending with the terminal one.
    pub async fn list_unaccepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::ListUnacceptedAgreements(tx);

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// List accepted agreements.
    ///
    /// Acceptance status and timestamps are in
    /// [`RithmicMessage::ResponseListAcceptedAgreements`].
    ///
    /// # Returns
    /// Every frame of the reply, in order, ending with the terminal one.
    pub async fn list_accepted_agreements(&self) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::ListAcceptedAgreements(tx);

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Retrieve an agreement for review.
    ///
    /// [`RithmicMessage::ResponseShowAgreement`] carries optional `agreement`
    /// and `agreement_html` byte buffers, left undecoded and unconcatenated.
    ///
    /// # Returns
    /// Every frame of the reply, in order, ending with the terminal one.
    pub async fn show_agreement(
        &self,
        agreement_id: &str,
    ) -> Result<Vec<RithmicResponse>, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::ShowAgreement {
            agreement_id: agreement_id.to_owned(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_all_responses(rx).await
    }

    /// Accept the identified agreement after the user has reviewed it.
    ///
    /// `None` omits market data capacity; provide it when the agreement
    /// requires it.
    ///
    /// # Returns
    /// A [`RithmicMessage::ResponseAcceptAgreement`]. A refusal is `Ok` with
    /// [`error`](RithmicResponse::error) set.
    pub async fn accept_agreement(
        &self,
        agreement_id: &str,
        capacity: Option<MarketDataUsageCapacity>,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::AcceptAgreement {
            agreement_id: agreement_id.to_owned(),
            capacity,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Set the user's market data self-certification for an agreement.
    ///
    /// # Returns
    /// A [`RithmicMessage::ResponseSetRithmicMrktDataSelfCertStatus`]. A
    /// refusal is `Ok` with [`error`](RithmicResponse::error) set.
    pub async fn set_market_data_self_cert_status(
        &self,
        agreement_id: &str,
        capacity: MarketDataUsageCapacity,
    ) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::SetMarketDataSelfCertStatus {
            agreement_id: agreement_id.to_owned(),
            capacity,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        await_first_response(rx).await
    }

    /// Log out and close the connection for every handle, even if logout fails.
    ///
    /// Pending and subsequent requests fail with
    /// [`RithmicError::ConnectionClosed`].
    ///
    /// # Returns
    /// The logout reply, or [`RithmicError::ConnectionClosed`] if the plant
    /// had already stopped.
    pub async fn disconnect(&self) -> Result<RithmicResponse, RithmicError> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, RithmicError>>();

        let command = RepositoryPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        // Held rather than propagated so that `Close` is queued either way.
        let outcome = rx.await.map_err(|_| RithmicError::ConnectionClosed);
        let _ = self.sender.send(RepositoryPlantCommand::Close).await;

        outcome??
            .into_iter()
            .next()
            .ok_or(RithmicError::EmptyResponse)
    }

    /// Stop the actor immediately for every handle. Does not wait or log out.
    ///
    /// Safe to call after shutdown; dropped if the command queue is full.
    pub fn abort(&self) {
        let _ = self.sender.try_send(RepositoryPlantCommand::Abort);
    }
}

#[cfg(test)]
mod tests;
