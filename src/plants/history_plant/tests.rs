use prost::Message as _;
use tokio::net::TcpStream;

use super::*;
use crate::{
    plants::test_support::{
        self, Responder, assert_close_still_sent, assert_rejected_after_close,
        assert_sent_while_open, assert_wire_silent, read_wire_request, write_wire_response,
    },
    rti::{
        RequestTickBarReplay, RequestTimeBarReplay, ResponseTickBarReplay, ResponseTimeBarReplay,
    },
};

async fn plant_with_wire() -> (HistoryPlant, mpsc::Sender<HistoryPlantCommand>, TcpStream) {
    test_support::plant_with_wire("history_plant", |core, request_receiver| HistoryPlant {
        core,
        request_receiver,
    })
    .await
}

fn load_ticks(response_sender: Responder) -> HistoryPlantCommand {
    HistoryPlantCommand::LoadTicks {
        request: TickBarReplayRequest::new()
            .symbol("ESH6")
            .exchange("CME")
            .bar_length(1)
            .start_time_sec(1)
            .end_time_sec(1000),
        response_sender,
    }
}

#[tokio::test]
async fn load_ticks_after_close_requested_is_not_sent() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_rejected_after_close(&mut plant, &mut client, load_ticks).await;
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    assert_close_still_sent(&mut plant, HistoryPlantCommand::Close, &mut client).await;
}

/// The same contract end to end through the public handle.
#[tokio::test]
async fn load_ticks_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.close_requested = true;

    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };

    let actor = tokio::spawn(async move { plant.run().await });

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.load_ticks("ESH6".to_string(), "CME".to_string(), 1, 1000),
    )
    .await
    .expect("load_ticks must be answered, not left waiting")
    .expect_err("load_ticks must fail once close was requested");

    assert!(matches!(err, RithmicError::ConnectionClosed));
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn load_ticks_is_sent_while_the_connection_is_open() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;

    assert_sent_while_open(&mut plant, &mut client, load_ticks).await;
}

/// A running plant actor on a live loopback wire, with the handle to drive it.
async fn running_plant_with_handle() -> (
    RithmicHistoryPlantHandle,
    tokio::task::JoinHandle<()>,
    TcpStream,
) {
    let (mut plant, command_sender, client) = plant_with_wire().await;

    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };

    let actor = tokio::spawn(async move { plant.run().await });

    (handle, actor, client)
}

/// An intermediate replay frame carrying one tick at `sec`.`usec`.
fn tick_at(id: &str, sec: i32, usec: i32) -> ResponseTickBarReplay {
    ResponseTickBarReplay {
        template_id: 207,
        user_msg: vec![id.to_string()],
        rq_handler_rp_code: vec!["0".to_string()],
        data_bar_ssboe: vec![sec, sec],
        data_bar_usecs: vec![usec, usec],
        ..Default::default()
    }
}

/// The data-less frame every replay closes with, truncated or not.
fn tick_page_end(id: &str) -> ResponseTickBarReplay {
    ResponseTickBarReplay {
        template_id: 207,
        user_msg: vec![id.to_string()],
        rp_code: vec!["0".to_string()],
        ..Default::default()
    }
}

fn time_bar_at(id: &str, marker: i32) -> ResponseTimeBarReplay {
    ResponseTimeBarReplay {
        template_id: 203,
        user_msg: vec![id.to_string()],
        rq_handler_rp_code: vec!["0".to_string()],
        marker: Some(marker),
        ..Default::default()
    }
}

fn time_bar_replay_end(id: &str) -> ResponseTimeBarReplay {
    ResponseTimeBarReplay {
        template_id: 203,
        user_msg: vec![id.to_string()],
        rp_code: vec!["0".to_string()],
        ..Default::default()
    }
}

/// Read one tick bar replay request and return its `resume_bars` and message id.
async fn read_tick_replay(client: &mut TcpStream) -> (Option<bool>, String) {
    let request = RequestTickBarReplay::decode(read_wire_request(client).await.as_slice())
        .expect("the request must be a tick bar replay");

    assert_eq!(request.template_id, 206);

    (request.resume_bars, request.user_msg[0].clone())
}

#[tokio::test]
async fn load_ticks_all_asks_the_server_to_lift_the_record_cap() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let loader = tokio::spawn(async move {
        handle
            .load_ticks_all("ESH6".to_string(), "CME".to_string(), 1, 1000)
            .await
    });

    // One request only: resume_bars replaces paging, so there is nothing to
    // follow up.
    let (resume_bars, id) = read_tick_replay(&mut client).await;
    assert_eq!(
        resume_bars,
        Some(true),
        "load_ticks_all must set resume_bars, which is what lifts the 10,000 record cap"
    );

    write_wire_response(&mut client, &tick_at(&id, 100, 1)).await;
    write_wire_response(&mut client, &tick_at(&id, 100, 2)).await;
    write_wire_response(&mut client, &tick_at(&id, 200, 5)).await;
    write_wire_response(&mut client, &tick_page_end(&id)).await;

    let responses = loader
        .await
        .expect("the loader must not panic")
        .expect("the load must succeed");

    let ticks: Vec<(i32, i32)> = responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseTickBarReplay(bar) if bar.data_bar_ssboe.len() == 2 => {
                Some((bar.data_bar_ssboe[1], bar.data_bar_usecs[1]))
            }
            _ => None,
        })
        .collect();

    assert_eq!(ticks, vec![(100, 1), (100, 2), (200, 5)]);
    assert_eq!(
        responses.len(),
        4,
        "every record plus the replay's closing frame"
    );

    actor.abort();
}

#[tokio::test]
async fn load_ticks_leaves_the_cap_in_place() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let loader = tokio::spawn(async move {
        handle
            .load_ticks("ESH6".to_string(), "CME".to_string(), 1, 1000)
            .await
    });

    let (resume_bars, id) = read_tick_replay(&mut client).await;
    assert_eq!(
        resume_bars, None,
        "the capped loader must not ask for the cap to be lifted"
    );

    write_wire_response(&mut client, &tick_page_end(&id)).await;
    loader
        .await
        .expect("the loader must not panic")
        .expect("the load must succeed");

    actor.abort();
}

#[tokio::test]
async fn load_time_bars_all_asks_the_server_to_lift_the_record_cap() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let loader = tokio::spawn(async move {
        handle
            .load_time_bars_all(
                "ESH6".to_string(),
                "CME".to_string(),
                BarType::MinuteBar,
                1,
                1,
                1000,
            )
            .await
    });

    let request = RequestTimeBarReplay::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the request must be a time bar replay");
    assert_eq!(request.template_id, 202);
    assert_eq!(
        request.resume_bars,
        Some(true),
        "load_time_bars_all must set resume_bars too"
    );

    let id = request.user_msg[0].clone();
    write_wire_response(&mut client, &time_bar_at(&id, 60)).await;
    write_wire_response(&mut client, &time_bar_at(&id, 120)).await;
    write_wire_response(&mut client, &time_bar_replay_end(&id)).await;

    let responses = loader
        .await
        .expect("the loader must not panic")
        .expect("the load must succeed");

    let markers: Vec<i32> = responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseTimeBarReplay(bar) => bar.marker,
            _ => None,
        })
        .collect();

    assert_eq!(markers, vec![60, 120]);

    actor.abort();
}

#[tokio::test]
async fn load_tick_bars_all_rejects_a_zero_bar_length() {
    let (handle, _command_receiver) = test_handle();

    let err = handle
        .load_tick_bars_all("ESH6".to_string(), "CME".to_string(), 0, 1, 1000)
        .await
        .expect_err("a zero bar length must be refused");

    assert!(matches!(err, RithmicError::InvalidArgument(_)));
}

fn test_handle() -> (
    RithmicHistoryPlantHandle,
    mpsc::Receiver<HistoryPlantCommand>,
) {
    let (sender, command_receiver) = mpsc::channel(4);
    let (subscription_sender, subscription_receiver) = broadcast::channel(4);

    let handle = RithmicHistoryPlantHandle {
        sender,
        subscription_receiver,
        subscription_sender,
    };

    (handle, command_receiver)
}

#[tokio::test]
async fn disconnect_sends_close_even_when_logout_fails() {
    let (handle, mut command_receiver) = test_handle();
    let call = tokio::spawn(async move { handle.disconnect().await });

    test_support::assert_close_follows_failed_logout(
        &mut command_receiver,
        |command| match command {
            HistoryPlantCommand::Logout { response_sender } => Some(response_sender),
            _ => None,
        },
        |command| matches!(command, HistoryPlantCommand::Close),
    )
    .await;

    assert!(matches!(
        call.await.expect("call task panicked"),
        Err(RithmicError::SendFailed)
    ));
}

fn tick_replay_request() -> TickBarReplayRequest {
    TickBarReplayRequest::new()
        .symbol("ESH6")
        .exchange("CME")
        .bar_length(1)
        .start_time_sec(1)
        .end_time_sec(1000)
}

#[tokio::test]
async fn scoped_replay_reports_sent_only_after_actor_writes_and_cancel_preserves_the_session() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };
    let mut replay = handle
        .start_tick_bar_replay(tick_replay_request())
        .await
        .unwrap();
    let mut progress = replay.subscribe_progress();
    assert_eq!(
        progress.borrow().sent_at,
        None,
        "queue admission is not a send"
    );
    let command = plant.request_receiver.recv().await.unwrap();
    plant.handle_command(command).await;
    let (resume_bars, original) = read_tick_replay(&mut client).await;
    assert_eq!(resume_bars, Some(true));
    assert!(progress.borrow().sent_at.is_some());
    let actor = tokio::spawn(async move { plant.run().await });
    write_wire_response(&mut client, &tick_at(&original, 100, 1)).await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while progress.borrow().data_frames == 0 {
            progress.changed().await.unwrap();
        }
    })
    .await
    .expect("the first data frame reaches progress");
    tokio::time::timeout(std::time::Duration::from_secs(2), replay.cancel())
        .await
        .unwrap()
        .unwrap();
    let outcome = scoped_outcome(&mut replay).await;
    assert_eq!(outcome.end, crate::ReplayEnd::Cancelled);
    assert_eq!(outcome.responses.len(), 1);
    // Late data and a cut cannot revive or re-resume the cancelled replay.
    write_wire_response(&mut client, &tick_at(&original, 200, 1)).await;
    write_wire_response(
        &mut client,
        &ResponseTickBarReplay {
            template_id: 207,
            user_msg: vec![original.clone()],
            request_key: Some("late".into()),
            ..Default::default()
        },
    )
    .await;
    write_wire_response(&mut client, &tick_page_end(&original)).await;
    // A second request uses the same healthy plant, without logout or close.
    let mut next = handle
        .start_tick_bar_replay(tick_replay_request())
        .await
        .unwrap();
    let (_, next_id) = read_tick_replay(&mut client).await;
    assert_ne!(original, next_id);
    write_wire_response(&mut client, &tick_page_end(&next_id)).await;
    assert_eq!(
        scoped_outcome(&mut next).await.end,
        crate::ReplayEnd::Complete
    );
    assert_wire_silent(&mut client).await;
    handle.abort();
    actor.await.unwrap();
}

#[tokio::test]
async fn dropping_an_admitted_handle_before_actor_send_keeps_the_wire_silent_even_when_queue_full()
{
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };
    let replay = handle
        .start_tick_bar_replay(tick_replay_request())
        .await
        .unwrap();
    for _ in 0..3 {
        handle
            .sender
            .try_send(HistoryPlantCommand::SetLogin)
            .unwrap();
    }
    assert_eq!(handle.sender.capacity(), 0);
    drop(replay); // Its cancellation command cannot enter the full queue.
    let command = plant.request_receiver.recv().await.unwrap();
    plant.handle_command(command).await;
    assert_wire_silent(&mut client).await;
}

#[tokio::test]
async fn cancelling_before_actor_send_is_acknowledged_and_never_claims_sent() {
    use futures_util::FutureExt as _;
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };
    let mut replay = handle
        .start_tick_bar_replay(tick_replay_request())
        .await
        .unwrap();
    let progress = replay.subscribe_progress();
    // Poll cancellation through admission, then prove the actor answers it.
    let mut cancellation = Box::pin(replay.cancel());
    assert!(cancellation.as_mut().now_or_never().is_none());
    let start = plant.request_receiver.recv().await.unwrap();
    plant.handle_command(start).await;
    let cancel = plant.request_receiver.recv().await.unwrap();
    plant.handle_command(cancel).await;
    cancellation.await.unwrap();
    assert_eq!(
        scoped_outcome(&mut replay).await.end,
        crate::ReplayEnd::Cancelled
    );
    assert_eq!(progress.borrow().sent_at, None);
    assert_wire_silent(&mut client).await;
}

#[tokio::test]
async fn scoped_time_replay_uses_original_correlation_through_native_continuation() {
    use crate::rti::{RequestResumeBars, ResponseResumeBars};
    let (handle, actor, mut client) = running_plant_with_handle().await;
    handle.resume_truncated_replays(false).await;
    let mut replay = handle
        .start_time_bar_replay(
            TimeBarReplayRequest::new()
                .symbol("ESH6")
                .exchange("CME")
                .bar_type(crate::TimeBarType::MinuteBar)
                .bar_type_period(1)
                .start_time_sec(1)
                .end_time_sec(1000),
        )
        .await
        .unwrap();
    let request =
        RequestTimeBarReplay::decode(read_wire_request(&mut client).await.as_slice()).unwrap();
    assert_eq!(request.resume_bars, Some(true));
    let id = &request.user_msg[0];
    write_wire_response(&mut client, &time_bar_at(id, 10)).await;
    write_wire_response(
        &mut client,
        &ResponseTimeBarReplay {
            template_id: 203,
            user_msg: vec![id.clone()],
            request_key: Some("first-cut".into()),
            ..Default::default()
        },
    )
    .await;
    let resume =
        RequestResumeBars::decode(read_wire_request(&mut client).await.as_slice()).unwrap();
    assert_eq!(resume.template_id, 210);
    assert_eq!(resume.request_key.as_deref(), Some("first-cut"));
    assert_ne!(resume.user_msg[0], *id);
    write_wire_response(
        &mut client,
        &ResponseResumeBars {
            template_id: 211,
            user_msg: resume.user_msg,
            rp_code: vec!["0".into()],
            ..Default::default()
        },
    )
    .await;
    write_wire_response(&mut client, &time_bar_at(id, 20)).await;
    write_wire_response(&mut client, &time_bar_replay_end(id)).await;
    let outcome = scoped_outcome(&mut replay).await;
    assert_eq!(outcome.end, crate::ReplayEnd::Complete);
    assert_eq!(outcome.responses.len(), 3);
    assert!(
        outcome
            .responses
            .iter()
            .all(|response| response.request_id == *id)
    );
    let progress = *replay.subscribe_progress().borrow();
    assert_eq!(progress.data_frames, 2);
    assert_eq!(progress.continuations, 1);
    handle.abort();
    actor.await.unwrap();
}

#[tokio::test]
async fn scoped_volume_replay_refusal_cannot_turn_a_prefix_into_success() {
    use crate::rti::{
        RequestResumeBars, RequestVolumeProfileMinuteBars, ResponseResumeBars,
        ResponseVolumeProfileMinuteBars,
    };
    let (handle, actor, mut client) = running_plant_with_handle().await;
    let mut replay = handle
        .start_volume_profile_minute_bars(
            VolumeProfileMinuteBarsRequest::new()
                .symbol("ESH6")
                .exchange("CME")
                .bar_type_period(1)
                .start_time_sec(1)
                .end_time_sec(1000),
        )
        .await
        .unwrap();
    let request =
        RequestVolumeProfileMinuteBars::decode(read_wire_request(&mut client).await.as_slice())
            .unwrap();
    assert_eq!(request.resume_bars, Some(true));
    let id = &request.user_msg[0];
    write_wire_response(
        &mut client,
        &ResponseVolumeProfileMinuteBars {
            template_id: 209,
            user_msg: vec![id.clone()],
            marker: Some(1),
            rq_handler_rp_code: vec!["0".into()],
            ..Default::default()
        },
    )
    .await;
    write_wire_response(
        &mut client,
        &ResponseVolumeProfileMinuteBars {
            template_id: 209,
            user_msg: vec![id.clone()],
            request_key: Some("cut".into()),
            ..Default::default()
        },
    )
    .await;
    let resume =
        RequestResumeBars::decode(read_wire_request(&mut client).await.as_slice()).unwrap();
    write_wire_response(
        &mut client,
        &ResponseResumeBars {
            template_id: 211,
            user_msg: resume.user_msg,
            rp_code: vec!["7".into(), "refused".into()],
            ..Default::default()
        },
    )
    .await;
    let outcome = scoped_outcome(&mut replay).await;
    assert!(matches!(
        outcome.end,
        crate::ReplayEnd::Refused(RithmicError::RequestRejected(_))
    ));
    assert_eq!(outcome.responses.len(), 2);
    assert_eq!(replay.subscribe_progress().borrow().data_frames, 1);
    handle.abort();
    actor.await.unwrap();
}

async fn scoped_outcome(replay: &mut crate::ReplayHandle) -> crate::ReplayOutcome {
    tokio::time::timeout(std::time::Duration::from_secs(2), replay.result())
        .await
        .expect("the mock server or local cancellation supplied a terminal outcome")
        .expect("the actor is alive")
}

#[tokio::test]
async fn dropping_an_active_replay_with_a_full_command_queue_releases_it_without_another_frame() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };
    let replay = handle
        .start_tick_bar_replay(tick_replay_request())
        .await
        .unwrap();
    let mut progress = replay.subscribe_progress();
    let command = plant.request_receiver.recv().await.unwrap();
    plant.handle_command(command).await;
    let (_, id) = read_tick_replay(&mut client).await;
    // Give the actor an actual accumulated part, then fill its control queue.
    let response = crate::api::receiver_api::RithmicResponse {
        request_id: id,
        message: RithmicMessage::ResponseTickBarReplay(tick_at("", 100, 1)),
        is_update: false,
        has_more: true,
        multi_response: true,
        error: None,
        source: "test".into(),
    };
    plant.core.request_handler.handle_response(response);
    for _ in 0..4 {
        handle
            .sender
            .try_send(HistoryPlantCommand::SetLogin)
            .unwrap();
    }
    drop(replay);
    let actor = tokio::spawn(async move { plant.run().await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while progress.changed().await.is_ok() {}
    })
    .await
    .expect("the actor sweep drops the progress writer and payload even without a new frame");
    assert_wire_silent(&mut client).await;
    handle.abort();
    actor.await.unwrap();
}

#[tokio::test]
async fn dropping_a_start_future_waiting_for_admission_never_sends_a_replay() {
    use futures_util::FutureExt as _;
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let subscription_sender = plant.core.subscription_sender.clone();
    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };
    for _ in 0..4 {
        handle
            .sender
            .try_send(HistoryPlantCommand::SetLogin)
            .unwrap();
    }
    let mut start = Box::pin(handle.start_tick_bar_replay(tick_replay_request()));
    assert!(start.as_mut().now_or_never().is_none());
    drop(start);
    for _ in 0..4 {
        let command = plant.request_receiver.recv().await.unwrap();
        plant.handle_command(command).await;
    }
    assert!(plant.request_receiver.try_recv().is_err());
    assert_wire_silent(&mut client).await;
}
