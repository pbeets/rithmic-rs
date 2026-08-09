use prost::Message as _;
use tokio::net::TcpStream;

use super::*;
use crate::{
    plants::test_support::{
        self, Responder, assert_close_still_sent, assert_rejected_after_close,
        assert_sent_while_open, assert_wire_silent, read_wire_request, write_wire_response,
    },
    rti::{
        RequestResumeBars, RequestTickBarReplay, RequestTimeBarReplay, ResponseTickBarReplay,
        ResponseTimeBarReplay,
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
        bar_type_specifier: "1".to_string(),
        end_time_sec: 1000,
        exchange: "CME".to_string(),
        response_sender,
        start_time_sec: 0,
        symbol: "ESH6".to_string(),
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
        handle.load_ticks("ESH6".to_string(), "CME".to_string(), 0, 1000),
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

/// An intermediate replay frame: the presence of `rq_handler_rp_code` marks it.
fn tick_page_bar(id: &str) -> ResponseTickBarReplay {
    ResponseTickBarReplay {
        template_id: 207,
        user_msg: vec![id.to_string()],
        rq_handler_rp_code: vec!["0".to_string()],
        ..Default::default()
    }
}

/// A closing replay frame, truncated when it carries a `request_key`.
fn tick_page_end(id: &str, resume_key: Option<&str>) -> ResponseTickBarReplay {
    ResponseTickBarReplay {
        template_id: 207,
        user_msg: vec![id.to_string()],
        rp_code: vec!["0".to_string()],
        request_key: resume_key.map(String::from),
        ..Default::default()
    }
}

#[tokio::test]
async fn load_ticks_all_follows_the_resume_key_across_pages() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let call = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .load_ticks_all("ESH6".to_string(), "CME".to_string(), 0, 1000, None)
                .await
        }
    });

    // Page 1: the replay request goes out, and its closing frame is truncated.
    let request = RequestTickBarReplay::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the first request must be a tick bar replay");
    assert_eq!(request.template_id, 206);
    let id = request.user_msg[0].clone();

    write_wire_response(&mut client, &tick_page_bar(&id)).await;
    write_wire_response(&mut client, &tick_page_end(&id, Some("key-1"))).await;

    // Page 2: the truncation must be resumed with the key the server handed out.
    let resume = RequestResumeBars::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the second request must be a resume");
    assert_eq!(resume.template_id, 210);
    assert_eq!(resume.request_key.as_deref(), Some("key-1"));
    let id = resume.user_msg[0].clone();

    write_wire_response(&mut client, &tick_page_bar(&id)).await;
    write_wire_response(&mut client, &tick_page_end(&id, None)).await;

    let responses = call
        .await
        .expect("call task panicked")
        .expect("the paginated load must succeed");

    assert_eq!(responses.len(), 4, "both pages must be returned");
    assert_eq!(
        responses[1].resume_key(),
        Some("key-1"),
        "page order must be preserved"
    );
    assert!(
        responses.last().unwrap().resume_key().is_none(),
        "a completed replay leaves no resume key"
    );

    handle.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn load_ticks_all_with_max_pages_stops_and_keeps_the_resume_key() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let call = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .load_ticks_all("ESH6".to_string(), "CME".to_string(), 0, 1000, Some(1))
                .await
        }
    });

    let request = RequestTickBarReplay::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the first request must be a tick bar replay");
    let id = request.user_msg[0].clone();

    write_wire_response(&mut client, &tick_page_bar(&id)).await;
    write_wire_response(&mut client, &tick_page_end(&id, Some("key-1"))).await;

    let responses = call
        .await
        .expect("call task panicked")
        .expect("a capped load still returns the gathered page");

    assert_eq!(responses.len(), 2, "only the first page must be returned");
    assert_eq!(
        responses.last().unwrap().resume_key(),
        Some("key-1"),
        "the cut-short replay must keep its resume key"
    );
    assert_wire_silent(&mut client).await;

    handle.abort();
    let _ = actor.await;
}

#[tokio::test]
async fn load_time_bars_all_follows_the_resume_key_across_pages() {
    let (handle, actor, mut client) = running_plant_with_handle().await;

    let call = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .load_time_bars_all(
                    "ESH6".to_string(),
                    "CME".to_string(),
                    BarType::MinuteBar,
                    1,
                    0,
                    1000,
                    None,
                )
                .await
        }
    });

    let request = RequestTimeBarReplay::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the first request must be a time bar replay");
    assert_eq!(request.template_id, 202);
    let id = request.user_msg[0].clone();

    write_wire_response(
        &mut client,
        &ResponseTimeBarReplay {
            template_id: 203,
            user_msg: vec![id.clone()],
            rp_code: vec!["0".to_string()],
            request_key: Some("key-2".to_string()),
            ..Default::default()
        },
    )
    .await;

    let resume = RequestResumeBars::decode(read_wire_request(&mut client).await.as_slice())
        .expect("the second request must be a resume");
    assert_eq!(resume.request_key.as_deref(), Some("key-2"));
    let id = resume.user_msg[0].clone();

    write_wire_response(
        &mut client,
        &ResponseTimeBarReplay {
            template_id: 203,
            user_msg: vec![id],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        },
    )
    .await;

    let responses = call
        .await
        .expect("call task panicked")
        .expect("the paginated load must succeed");

    assert_eq!(responses.len(), 2, "both pages must be returned");
    assert!(responses.last().unwrap().resume_key().is_none());

    handle.abort();
    let _ = actor.await;
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
