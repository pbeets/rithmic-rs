use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::broadcast;

use crate::{api::RithmicResponse, config::RithmicAccount, rti::messages::RithmicMessage};

/// Parks the `Receiver` that `broadcast::channel` returns alongside the sender
/// until the first plant handle claims it.
///
/// That receiver starts at position zero and is never read, so it still yields
/// every message the sender has buffered since the plant connected. Handing it
/// to the first `get_handle()` caller makes those messages visible to that
/// handle; later callers get a fresh `subscribe()`, which starts at the channel
/// tail. Parking it also keeps the receiver count at one, so `send` does not
/// fail — and drop the value instead of buffering it — while no handle exists.
///
/// Backlog is bounded by the requested channel capacity rounded up to a power
/// of two: the ring is preallocated at that size and sends overwrite the oldest
/// slot, so a claimed receiver that has fallen further behind than the ring
/// gets `RecvError::Lagged` and resumes at the oldest retained message.
#[derive(Debug)]
pub(crate) struct InitialReceiver {
    receiver: Mutex<Option<broadcast::Receiver<RithmicResponse>>>,
}

impl InitialReceiver {
    pub(crate) fn new(receiver: broadcast::Receiver<RithmicResponse>) -> Self {
        Self {
            receiver: Mutex::new(Some(receiver)),
        }
    }

    /// Claim the parked receiver, or subscribe at the channel tail once it is
    /// already claimed.
    pub(crate) fn take_or_subscribe(
        &self,
        sender: &broadcast::Sender<RithmicResponse>,
    ) -> broadcast::Receiver<RithmicResponse> {
        self.receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| sender.subscribe())
    }
}

/// Filters a shared plant subscription stream down to a single account.
///
/// Order and PnL plants share one upstream connection per login session while
/// updates remain account-specific. This type keeps the familiar `.recv().await`
/// API while forwarding connection-level and other non-account-tagged messages to
/// every handle.
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
        RithmicMessage::AccountListUpdates(update) => update.account_id.as_deref(),
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

    use super::{InitialReceiver, SubscriptionFilter};
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

    fn tagged(request_id: &str) -> RithmicResponse {
        RithmicResponse {
            request_id: request_id.to_string(),
            ..response(RithmicMessage::ResponseAcceptAgreement(
                ResponseAcceptAgreement::default(),
            ))
        }
    }

    // Everything these tests assert on is already buffered when the assertion
    // runs, so they use `try_recv`: a lost message shows up as `Empty` instead
    // of parking the suite on a message that will never arrive.

    #[test]
    fn parked_receiver_replays_messages_sent_before_it_was_claimed() {
        let (sender, receiver) = broadcast::channel(16);
        let parked = InitialReceiver::new(receiver);

        sender.send(tagged("before")).unwrap();

        let mut first = parked.take_or_subscribe(&sender);

        assert_eq!(first.try_recv().unwrap().request_id, "before");
    }

    #[test]
    fn claims_after_the_first_start_at_the_stream_tail() {
        let (sender, receiver) = broadcast::channel(16);
        let parked = InitialReceiver::new(receiver);

        sender.send(tagged("before")).unwrap();

        let mut first = parked.take_or_subscribe(&sender);
        let mut second = parked.take_or_subscribe(&sender);

        sender.send(tagged("after")).unwrap();

        assert_eq!(first.try_recv().unwrap().request_id, "before");
        assert_eq!(first.try_recv().unwrap().request_id, "after");
        // The second claim subscribed at the tail, so "before" is not replayed.
        assert_eq!(second.try_recv().unwrap().request_id, "after");
        assert!(second.try_recv().is_err());
    }

    #[test]
    fn sends_succeed_while_the_receiver_is_still_parked() {
        let (sender, receiver) = broadcast::channel(16);
        let _parked = InitialReceiver::new(receiver);

        assert_eq!(sender.receiver_count(), 1);
        assert!(sender.send(tagged("unclaimed")).is_ok());
    }

    #[test]
    fn backlog_beyond_capacity_lags_the_parked_receiver() {
        // Capacity bounds the backlog: the ring keeps the newest `capacity`
        // messages — rounded up to a power of two, already the case for 2 — and
        // the claimed receiver resumes at the oldest retained one.
        let (sender, receiver) = broadcast::channel(2);
        let parked = InitialReceiver::new(receiver);

        for i in 0..4 {
            sender.send(tagged(&i.to_string())).unwrap();
        }

        let mut first = parked.take_or_subscribe(&sender);

        assert!(matches!(
            first.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(2))
        ));
        assert_eq!(first.try_recv().unwrap().request_id, "2");
        assert_eq!(first.try_recv().unwrap().request_id, "3");
    }
}
