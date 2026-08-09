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
