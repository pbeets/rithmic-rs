use super::*;
use prost::Message as _;
use tokio::net::TcpStream;

use crate::{
    plants::{
        core::Event,
        session::Session,
        test_support::{
            self, Responder, assert_close_still_sent, assert_wire_silent, read_wire_request,
            write_wire_response,
        },
    },
    rti::{
        RequestTickBarReplay, RequestTimeBarReplay, ResponseTickBarReplay, ResponseTimeBarReplay,
    },
};

async fn plant_with_wire() -> (
    Plant<HistoryPlant>,
    mpsc::Sender<HistoryPlantCommand>,
    TcpStream,
) {
    test_support::plant_with_wire().await
}

fn load_ticks(response_sender: Responder) -> HistoryPlantCommand {
    HistoryPlantCommand::Replay {
        query: ReplayQuery::Tick(
            TickBarReplayRequest::new()
                .symbol("ESZ6")
                .exchange("CME")
                .bar_length(1)
                .start_time_sec(1)
                .end_time_sec(1000),
        ),
        response_sender,
    }
}

/// A load whose caller has already stopped waiting: the actor takes it off the
/// queue and sends nothing.
fn abandoned_load() -> HistoryPlantCommand {
    load_ticks(oneshot::channel().0)
}

#[tokio::test]
async fn close_still_reaches_the_wire_after_close_requested() {
    let (mut plant, _command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    assert_close_still_sent(&mut plant, HistoryPlantCommand::Close, &mut client).await;
}

#[tokio::test]
async fn load_ticks_through_the_handle_after_close_requested_reports_connection_closed() {
    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    plant.core.session = Session::Closing;

    let subscription_sender = plant.subscription_sender.clone();

    let handle = RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    };

    let actor = tokio::spawn(async move { plant.run().await });

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.load_ticks("ESZ6".to_string(), "CME".to_string(), 1, 1000),
    )
    .await
    .expect("load_ticks must be answered, not left waiting")
    .expect_err("load_ticks must fail once close was requested");

    assert!(matches!(err, RithmicError::ConnectionClosed));
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

/// A running plant actor on a live loopback wire, with the handle to drive it.
async fn running_plant_with_handle() -> (
    RithmicHistoryPlantHandle,
    tokio::task::JoinHandle<()>,
    TcpStream,
) {
    let (mut plant, command_sender, client) = plant_with_wire().await;

    let subscription_sender = plant.subscription_sender.clone();

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
            .load_ticks_all("ESZ6".to_string(), "CME".to_string(), 1, 1000)
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
            .load_ticks("ESZ6".to_string(), "CME".to_string(), 1, 1000)
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
                "ESZ6".to_string(),
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
    let (handle, mut command_receiver) = test_handle();

    // No actor is running, so a missing guard fails the timeout, not the suite.
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.load_tick_bars_all("ESZ6".to_string(), "CME".to_string(), 0, 1, 1000),
    )
    .await
    .expect("must be refused without reaching the actor")
    .expect_err("a zero bar length must be refused");

    assert!(matches!(err, RithmicError::InvalidArgument(_)));
    assert!(command_receiver.try_recv().is_err());
}

#[tokio::test]
async fn load_tick_bar_replay_sends_the_request_as_given() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    // Unset, off and on: the struct form must forward `resume_bars` untouched
    // rather than defaulting it either way.
    for resume_bars in [None, Some(false), Some(true)] {
        let mut request = TickBarReplayRequest::new()
            .symbol("ESZ6")
            .exchange("CME")
            .bar_length(1)
            .start_time_sec(1)
            .end_time_sec(1000)
            .user_max_count(500);

        request.resume_bars = resume_bars;

        let loader = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.load_tick_bar_replay(request).await })
        };

        let sent = RequestTickBarReplay::decode(read_wire_request(&mut client).await.as_slice())
            .expect("the request must be a tick bar replay");

        assert_eq!(sent.template_id, 206);
        assert_eq!(sent.user_max_count, Some(500));
        assert_eq!(sent.resume_bars, resume_bars);

        write_wire_response(&mut client, &tick_page_end(&sent.user_msg[0])).await;
        reply_of(loader).await.expect("the load must succeed");
    }

    actor.abort();
}

#[tokio::test]
async fn load_time_bar_replay_sends_the_request_as_given() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let loader = tokio::spawn(async move {
        handle
            .load_time_bar_replay(
                TimeBarReplayRequest::new()
                    .symbol("ESZ6")
                    .exchange("CME")
                    .bar_type(BarType::DailyBar)
                    .bar_type_period(1)
                    .start_time_sec(20260901)
                    .end_time_sec(20260914)
                    .user_max_count(500),
            )
            .await
    });

    let sent = RequestTimeBarReplay::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the request must be a time bar replay");

    assert_eq!(sent.template_id, 202);
    assert_eq!(sent.user_max_count, Some(500));
    assert_eq!(sent.resume_bars, None);
    assert_eq!(sent.start_index, Some(20260901));
    assert_eq!(sent.finish_index, Some(20260914));

    write_wire_response(&mut client, &time_bar_replay_end(&sent.user_msg[0])).await;
    reply_of(loader).await.expect("the load must succeed");

    actor.abort();
}

#[tokio::test]
async fn load_tick_bar_replay_refuses_an_invalid_request_without_sending_it() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    // No exchange, and a window that ends before it starts.
    let err = handle
        .load_tick_bar_replay(
            TickBarReplayRequest::new()
                .symbol("ESZ6")
                .bar_length(1)
                .start_time_sec(1000)
                .end_time_sec(1)
                .user_max_count(500),
        )
        .await
        .expect_err("an invalid request must be refused");

    assert!(matches!(err, RithmicError::InvalidArgument(_)));
    assert_wire_silent(&mut client).await;

    actor.abort();
}

#[tokio::test]
async fn load_time_bar_replay_refuses_an_invalid_request_without_sending_it() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    // No bar type.
    let err = handle
        .load_time_bar_replay(
            TimeBarReplayRequest::new()
                .symbol("ESZ6")
                .exchange("CME")
                .bar_type_period(1)
                .start_time_sec(1)
                .end_time_sec(1000),
        )
        .await
        .expect_err("an invalid request must be refused");

    assert!(matches!(err, RithmicError::InvalidArgument(_)));
    assert_wire_silent(&mut client).await;

    actor.abort();
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

fn handle_for(
    plant: &Plant<HistoryPlant>,
    command_sender: mpsc::Sender<HistoryPlantCommand>,
) -> RithmicHistoryPlantHandle {
    let subscription_sender = plant.subscription_sender.clone();

    RithmicHistoryPlantHandle {
        sender: command_sender,
        subscription_receiver: subscription_sender.subscribe(),
        subscription_sender,
    }
}

/// Wait for a spawned `load_*` call, with a deadline so a hang fails the test.
async fn reply_of(
    task: tokio::task::JoinHandle<Result<Vec<RithmicResponse>, RithmicError>>,
) -> Result<Vec<RithmicResponse>, RithmicError> {
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("the load call returned")
        .expect("the load task did not panic")
}

#[tokio::test]
async fn a_dropped_load_leaves_the_session_usable() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let load = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .load_ticks_all("ESZ6".into(), "CME".into(), 1, 1000)
                .await
        })
    };

    let (resume_bars, original) = read_tick_replay(&mut client).await;
    assert_eq!(resume_bars, Some(true));

    write_wire_response(&mut client, &tick_at(&original, 100, 1)).await;
    load.abort();
    let _ = load.await;

    // Late data and a late cut cannot revive the dropped replay or trigger a
    // continuation.
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

    // The next request uses the same plant, without logout or reconnect.
    let next = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .load_ticks_all("ESZ6".into(), "CME".into(), 1, 1000)
                .await
        })
    };

    let (_, next_id) = read_tick_replay(&mut client).await;
    assert_ne!(original, next_id);

    write_wire_response(&mut client, &tick_page_end(&next_id)).await;
    assert_eq!(reply_of(next).await.unwrap().len(), 1);
    assert_wire_silent(&mut client).await;

    handle.abort();
    actor.await.unwrap();
}

#[tokio::test]
async fn a_load_dropped_before_the_plant_sends_it_is_never_sent() {
    use futures_util::FutureExt as _;

    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let handle = handle_for(&plant, command_sender);

    // Queue the request, then drop the call before the plant picks it up.
    let mut load = Box::pin(handle.load_ticks_all("ESZ6".into(), "CME".into(), 1, 1000));
    assert!(load.as_mut().now_or_never().is_none());
    drop(load);

    let command = plant.request_receiver.recv().await.unwrap();
    plant.handle(Event::Command(command)).await;

    assert_wire_silent(&mut client).await;
}

#[tokio::test]
async fn load_time_bars_all_continues_a_cut_reply_on_the_original_request() {
    use crate::rti::{RequestResumeBars, ResponseResumeBars};

    let (handle, actor, mut client) = running_plant_with_handle().await;

    let load = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .load_time_bars_all(
                    "ESZ6".into(),
                    "CME".into(),
                    crate::TimeBarType::MinuteBar,
                    1,
                    1,
                    1000,
                )
                .await
        })
    };

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

    let reply = reply_of(load).await.unwrap();

    assert_eq!(
        reply.len(),
        3,
        "two bars and the end; the notice is left out"
    );
    assert!(reply.iter().all(|response| response.request_id == *id));

    handle.abort();
    actor.await.unwrap();
}

#[tokio::test]
async fn a_refused_continuation_fails_the_load() {
    use crate::rti::{
        RequestResumeBars, RequestVolumeProfileMinuteBars, ResponseResumeBars,
        ResponseVolumeProfileMinuteBars,
    };

    let (handle, actor, mut client) = running_plant_with_handle().await;

    let load = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .load_volume_profile_minute_bars(
                    VolumeProfileMinuteBarsRequest::new()
                        .symbol("ESZ6")
                        .exchange("CME")
                        .bar_type_period(1)
                        .start_time_sec(1)
                        .end_time_sec(1000),
                )
                .await
        })
    };

    let request =
        RequestVolumeProfileMinuteBars::decode(read_wire_request(&mut client).await.as_slice())
            .unwrap();

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

    assert!(matches!(
        reply_of(load).await,
        Err(RithmicError::RequestRejected(_))
    ));

    handle.abort();
    actor.await.unwrap();
}

#[tokio::test]
async fn a_load_dropped_mid_replay_is_released_without_another_frame() {
    use futures_util::FutureExt as _;

    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let handle = handle_for(&plant, command_sender);

    let mut load = Box::pin(handle.load_ticks_all("ESZ6".into(), "CME".into(), 1, 1000));
    assert!(load.as_mut().now_or_never().is_none());

    let command = plant.request_receiver.recv().await.unwrap();
    plant.handle(Event::Command(command)).await;
    let (_, id) = read_tick_replay(&mut client).await;

    // Give the replay a collected part, then fill the command queue.
    plant.core.request_handler.handle_response(RithmicResponse {
        request_id: id.clone(),
        message: RithmicMessage::ResponseTickBarReplay(tick_at("", 100, 1)),
        is_update: false,
        has_more: true,
        multi_response: true,
        error: None,
        source: "test".into(),
    });

    for _ in 0..4 {
        handle.sender.try_send(abandoned_load()).unwrap();
    }

    drop(load);

    // One pass of the plant loop works through the queued commands and
    // releases the dropped replay, with no new frame.
    {
        let mut run = std::pin::pin!(plant.run());
        assert!(run.as_mut().now_or_never().is_none());
    }

    assert!(
        plant.core.request_handler.expects_late_frames(&id),
        "the replay was released and its late frames are counted"
    );
    assert_wire_silent(&mut client).await;
}

#[tokio::test]
async fn a_load_dropped_while_waiting_for_room_in_the_queue_is_never_sent() {
    use futures_util::FutureExt as _;

    let (mut plant, command_sender, mut client) = plant_with_wire().await;
    let handle = handle_for(&plant, command_sender);

    for _ in 0..4 {
        handle.sender.try_send(abandoned_load()).unwrap();
    }

    let mut load = Box::pin(handle.load_ticks_all("ESZ6".into(), "CME".into(), 1, 1000));
    assert!(load.as_mut().now_or_never().is_none());
    drop(load);

    for _ in 0..4 {
        let command = plant.request_receiver.recv().await.unwrap();
        plant.handle(Event::Command(command)).await;
    }

    assert!(plant.request_receiver.try_recv().is_err());
    assert_wire_silent(&mut client).await;
}
