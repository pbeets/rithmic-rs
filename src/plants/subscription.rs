use crate::{api::RithmicResponse, config::RithmicAccount, rti::messages::RithmicMessage};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Filters a shared plant subscription stream down to a single account.
///
/// Yields only updates tagged with this handle's account. Connection-health
/// events and untagged messages reach every handle.
pub struct SubscriptionFilter {
    account: Arc<RithmicAccount>,
    receiver: broadcast::Receiver<RithmicResponse>,
}

impl SubscriptionFilter {
    pub(crate) fn new(
        account: Arc<RithmicAccount>,
        receiver: broadcast::Receiver<RithmicResponse>,
    ) -> Self {
        Self { account, receiver }
    }

    /// Wait for the next subscription update for this account.
    ///
    /// When `RecvError::Lagged(n)` is returned, `n` counts all skipped messages
    /// on the shared broadcast stream, including messages for other accounts.
    pub async fn recv(&mut self) -> Result<RithmicResponse, broadcast::error::RecvError> {
        loop {
            let response = self.receiver.recv().await?;
            if self.should_forward(&response) {
                return Ok(response);
            }
        }
    }

    /// Create a second receiver starting at the current stream position.
    #[must_use]
    pub fn resubscribe(&self) -> Self {
        Self {
            account: Arc::clone(&self.account),
            receiver: self.receiver.resubscribe(),
        }
    }

    fn should_forward(&self, response: &RithmicResponse) -> bool {
        match response_account_id(response) {
            Some(account_id) => account_id == self.account.account_id,
            None => true,
        }
    }
}

impl std::fmt::Debug for SubscriptionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionFilter")
            .field("account_id", &self.account.account_id)
            .finish_non_exhaustive()
    }
}

fn response_account_id(response: &RithmicResponse) -> Option<&str> {
    match &response.message {
        RithmicMessage::UserAccountUpdate(update) => update.account_id.as_deref(),
        RithmicMessage::AccountRmsUpdates(update) => update.account_id.as_deref(),
        RithmicMessage::BracketUpdates(update) => update.account_id.as_deref(),
        RithmicMessage::RithmicOrderNotification(update) => update.account_id.as_deref(),
        RithmicMessage::ExchangeOrderNotification(update) => update.account_id.as_deref(),
        RithmicMessage::AccountPnLPositionUpdate(update) => update.account_id.as_deref(),
        RithmicMessage::InstrumentPnLPositionUpdate(update) => update.account_id.as_deref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast;

    use super::SubscriptionFilter;
    use crate::{
        api::RithmicResponse,
        config::RithmicAccount,
        rti::{
            AccountPnLPositionUpdate, ResponseAcceptAgreement, TradeRoute, UpdateEasyToBorrowList,
            UserAccountUpdate, messages::RithmicMessage,
        },
    };

    fn account(account_id: &str) -> std::sync::Arc<RithmicAccount> {
        std::sync::Arc::new(RithmicAccount::new("FCM", "IB", account_id))
    }

    fn response(message: RithmicMessage) -> RithmicResponse {
        RithmicResponse {
            request_id: "1".to_string(),
            message,
            error: None,
            is_update: true,
            has_more: false,
            multi_response: false,
            source: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn forwards_matching_account_messages() {
        let (sender, receiver) = broadcast::channel(16);
        let mut filter = SubscriptionFilter::new(account("ACCOUNT_A"), receiver);

        sender
            .send(response(RithmicMessage::UserAccountUpdate(
                UserAccountUpdate {
                    template_id: 0,
                    account_id: Some("ACCOUNT_A".to_string()),
                    ..UserAccountUpdate::default()
                },
            )))
            .unwrap();

        let response = filter.recv().await.unwrap();
        match response.message {
            RithmicMessage::UserAccountUpdate(update) => {
                assert_eq!(update.account_id.as_deref(), Some("ACCOUNT_A"));
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn forwards_trade_route_updates_without_account_id() {
        let (sender, receiver) = broadcast::channel(16);
        let mut filter = SubscriptionFilter::new(account("ACCOUNT_A"), receiver);

        sender
            .send(response(RithmicMessage::TradeRoute(TradeRoute {
                template_id: 350,
                ..TradeRoute::default()
            })))
            .unwrap();

        let response = filter.recv().await.unwrap();
        assert!(matches!(response.message, RithmicMessage::TradeRoute(_)));
    }

    #[tokio::test]
    async fn forwards_update_easy_to_borrow_messages_without_account_id() {
        let (sender, receiver) = broadcast::channel(16);
        let mut filter = SubscriptionFilter::new(account("ACCOUNT_A"), receiver);

        sender
            .send(response(RithmicMessage::UpdateEasyToBorrowList(
                UpdateEasyToBorrowList {
                    template_id: 355,
                    ..UpdateEasyToBorrowList::default()
                },
            )))
            .unwrap();

        let response = filter.recv().await.unwrap();
        assert!(matches!(
            response.message,
            RithmicMessage::UpdateEasyToBorrowList(_)
        ));
    }

    #[tokio::test]
    async fn skips_other_accounts_and_waits_for_matching_update() {
        let (sender, receiver) = broadcast::channel(16);
        let mut filter = SubscriptionFilter::new(account("ACCOUNT_A"), receiver);

        sender
            .send(response(RithmicMessage::AccountPnLPositionUpdate(
                AccountPnLPositionUpdate {
                    template_id: 0,
                    account_id: Some("ACCOUNT_B".to_string()),
                    ..AccountPnLPositionUpdate::default()
                },
            )))
            .unwrap();
        sender
            .send(response(RithmicMessage::ResponseAcceptAgreement(
                ResponseAcceptAgreement::default(),
            )))
            .unwrap();

        let response = filter.recv().await.unwrap();
        assert!(matches!(
            response.message,
            RithmicMessage::ResponseAcceptAgreement(_)
        ));
    }
}
