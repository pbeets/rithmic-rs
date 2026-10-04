use super::*;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use crate::{
    plants::{
        core::{Effect, Event},
        session::Session,
        test_support::{self, answer, frame, plant_core},
    },
    rti::{
        RequestAcceptAgreement, RequestListAcceptedAgreements, RequestListUnacceptedAgreements,
        RequestLogin, RequestLogout, RequestSetRithmicMrktDataSelfCertStatus, RequestShowAgreement,
        ResponseAcceptAgreement, ResponseListAcceptedAgreements, ResponseListUnacceptedAgreements,
        ResponseLogin, ResponseLogout, ResponseSetRithmicMrktDataSelfCertStatus,
        ResponseShowAgreement, messages::RithmicMessage,
    },
};

async fn read_request<M: prost::Message + Default>(ws: &mut WebSocketStream<TcpStream>) -> M {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(bytes) => {
                assert_eq!(
                    u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
                    bytes.len() - 4
                );
                return M::decode(&bytes[4..]).unwrap();
            }
            Message::Ping(_) => ws.flush().await.unwrap(),
            message => panic!("expected request, got {message:?}"),
        }
    }
}

async fn send_response(ws: &mut WebSocketStream<TcpStream>, response: &impl prost::Message) {
    let body = response.encode_to_vec();
    let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
    bytes.extend(body);
    ws.send(Message::Binary(bytes.into())).await.unwrap();
}

#[tokio::test]
async fn optional_connection_supports_the_full_agreement_workflow() {
    // Public connect/handle APIs against a local websocket, never a real account.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let config = test_support::test_config();
        let mut config = config;
        config.url = url;

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let login: RequestLogin = read_request(&mut ws).await;
            assert_eq!(login.template_id, 10);
            assert_eq!(login.infra_type, Some(SysInfraType::RepositoryPlant as i32));
            assert_eq!(login.aggregated_quotes, None);
            assert_eq!(login.os_platform.as_deref(), Some("test-platform"));
            send_response(
                &mut ws,
                &ResponseLogin {
                    template_id: 11,
                    user_msg: login.user_msg,
                    rp_code: vec!["0".into()],
                    heartbeat_interval: Some(60.0),
                    ..Default::default()
                },
            )
            .await;

            // No automatic agreement requests or accepts: the next frame is
            // the caller's explicit list request, even after a repeated login.
            let list: RequestListUnacceptedAgreements = read_request(&mut ws).await;
            assert_eq!(list.template_id, 500);
            for id in ["agreement-a", "agreement-b"] {
                send_response(
                    &mut ws,
                    &ResponseListUnacceptedAgreements {
                        template_id: 501,
                        user_msg: list.user_msg.clone(),
                        rq_handler_rp_code: vec!["0".into()],
                        agreement_id: Some(id.into()),
                        agreement_title: Some(format!("Title of {id}")),
                        agreement_acceptance_request: Some("required".into()),
                        ..Default::default()
                    },
                )
                .await;
            }
            send_response(
                &mut ws,
                &ResponseListUnacceptedAgreements {
                    template_id: 501,
                    user_msg: list.user_msg,
                    rp_code: vec!["0".into()],
                    ..Default::default()
                },
            )
            .await;

            let show: RequestShowAgreement = read_request(&mut ws).await;
            assert_eq!(show.template_id, 506);
            assert_eq!(show.agreement_id.as_deref(), Some("agreement-a"));
            for bytes in [vec![0, 255, 1], vec![2, 254, 3]] {
                send_response(
                    &mut ws,
                    &ResponseShowAgreement {
                        template_id: 507,
                        user_msg: show.user_msg.clone(),
                        rq_handler_rp_code: vec!["0".into()],
                        agreement: Some(bytes),
                        agreement_html: Some(b"<p>Agreement</p>".to_vec()),
                        agreement_mandatory_flag: Some("1".into()),
                        agreement_status: Some("unaccepted".into()),
                        ..Default::default()
                    },
                )
                .await;
            }
            send_response(
                &mut ws,
                &ResponseShowAgreement {
                    template_id: 507,
                    user_msg: show.user_msg,
                    rp_code: vec!["0".into()],
                    ..Default::default()
                },
            )
            .await;

            let accept: RequestAcceptAgreement = read_request(&mut ws).await;
            assert_eq!(accept.template_id, 504);
            assert_eq!(accept.agreement_id.as_deref(), Some("agreement-a"));
            assert_eq!(
                accept.market_data_usage_capacity.as_deref(),
                Some("Non-Professional")
            );
            send_response(
                &mut ws,
                &ResponseAcceptAgreement {
                    template_id: 505,
                    user_msg: accept.user_msg,
                    rp_code: vec!["6".into(), "cannot accept".into()],
                },
            )
            .await;

            let cert: RequestSetRithmicMrktDataSelfCertStatus = read_request(&mut ws).await;
            assert_eq!(cert.template_id, 508);
            assert_eq!(cert.agreement_id.as_deref(), Some("agreement-a"));
            assert_eq!(
                cert.market_data_usage_capacity.as_deref(),
                Some("Professional")
            );
            send_response(
                &mut ws,
                &ResponseSetRithmicMrktDataSelfCertStatus {
                    template_id: 509,
                    user_msg: cert.user_msg,
                    rp_code: vec!["0".into()],
                },
            )
            .await;

            let accepted: RequestListAcceptedAgreements = read_request(&mut ws).await;
            assert_eq!(accepted.template_id, 502);
            send_response(
                &mut ws,
                &ResponseListAcceptedAgreements {
                    template_id: 503,
                    user_msg: accepted.user_msg.clone(),
                    rq_handler_rp_code: vec!["0".into()],
                    agreement_id: Some("old-agreement".into()),
                    agreement_acceptance_ssboe: Some(1_700_000_000),
                    agreement_acceptance_status: Some("accepted".into()),
                    ..Default::default()
                },
            )
            .await;
            send_response(
                &mut ws,
                &ResponseListAcceptedAgreements {
                    template_id: 503,
                    user_msg: accepted.user_msg,
                    rp_code: vec!["0".into()],
                    ..Default::default()
                },
            )
            .await;

            let logout: RequestLogout = read_request(&mut ws).await;
            assert_eq!(logout.template_id, 12);
            send_response(
                &mut ws,
                &ResponseLogout {
                    template_id: 13,
                    user_msg: logout.user_msg,
                    rp_code: vec!["0".into()],
                },
            )
            .await;
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            ws.flush().await.unwrap();
        });

        let plant = RithmicRepositoryPlant::connect(&config, ConnectStrategy::Simple)
            .await
            .unwrap();
        let handle = plant.get_handle();
        let login = LoginConfig {
            aggregated_quotes: Some(true),
            os_platform: Some("test-platform".into()),
            ..Default::default()
        };
        let response = handle.login_with_config(login.clone()).await.unwrap();
        assert_eq!(response.source, "repository_plant");
        assert_eq!(
            handle.clone().login_with_config(login).await.unwrap(),
            response
        );
        let listed = handle.list_unaccepted_agreements().await.unwrap();
        assert_eq!(listed.len(), 3);
        assert!(listed[0].has_more);
        assert!(!listed[2].has_more);
        assert!(
            listed
                .iter()
                .all(|r| r.error.is_none() && r.source == "repository_plant")
        );
        let RithmicMessage::ResponseListUnacceptedAgreements(first) = &listed[0].message else {
            panic!()
        };
        assert_eq!(first.agreement_id.as_deref(), Some("agreement-a"));
        assert_eq!(
            first.agreement_acceptance_request.as_deref(),
            Some("required")
        );
        let RithmicMessage::ResponseListUnacceptedAgreements(second) = &listed[1].message else {
            panic!()
        };
        assert_eq!(second.agreement_id.as_deref(), Some("agreement-b"));

        let shown = handle.show_agreement("agreement-a").await.unwrap();
        assert_eq!(shown.len(), 3);
        for (response, expected) in shown[..2].iter().zip([vec![0, 255, 1], vec![2, 254, 3]]) {
            let RithmicMessage::ResponseShowAgreement(part) = &response.message else {
                panic!()
            };
            assert_eq!(part.agreement.as_ref(), Some(&expected));
            assert_eq!(
                part.agreement_html.as_deref(),
                Some(b"<p>Agreement</p>".as_slice())
            );
            assert_eq!(part.agreement_mandatory_flag.as_deref(), Some("1"));
        }
        let refused = handle
            .accept_agreement(
                "agreement-a",
                Some(MarketDataUsageCapacity::NonProfessional),
            )
            .await
            .unwrap();
        assert!(matches!(
            refused.error,
            Some(RithmicError::RequestRejected { .. })
        ));
        assert_eq!(refused.rp_code_text(), Some("cannot accept"));
        let cert = handle
            .set_market_data_self_cert_status("agreement-a", MarketDataUsageCapacity::Professional)
            .await
            .unwrap();
        assert!(cert.error.is_none());
        assert!(matches!(
            cert.message,
            RithmicMessage::ResponseSetRithmicMrktDataSelfCertStatus(_)
        ));
        let accepted = handle.list_accepted_agreements().await.unwrap();
        assert_eq!(accepted.len(), 2);
        let RithmicMessage::ResponseListAcceptedAgreements(first) = &accepted[0].message else {
            panic!()
        };
        assert_eq!(first.agreement_acceptance_ssboe, Some(1_700_000_000));
        assert_eq!(
            first.agreement_acceptance_status.as_deref(),
            Some("accepted")
        );
        handle.disconnect().await.unwrap();
        assert!(matches!(
            handle.list_unaccepted_agreements().await,
            Err(RithmicError::ConnectionClosed)
        ));
        plant.await_shutdown().await.unwrap();
        server.await.unwrap();
    })
    .await
    .expect("agreement workflow timed out");
}

#[tokio::test]
async fn acceptance_can_omit_capacity() {
    let mut core = plant_core::<RepositoryPlant>();
    let (tx, mut rx) = oneshot::channel();
    let effects = core.on_event(Event::Command(RepositoryPlantCommand::AcceptAgreement {
        agreement_id: "terms".into(),
        capacity: None,
        response_sender: tx,
    }));
    let [Effect::Send { frame: bytes, id }] = effects.as_slice() else {
        panic!()
    };
    let request = RequestAcceptAgreement::decode(&bytes[4..]).unwrap();
    assert_eq!(request.agreement_id.as_deref(), Some("terms"));
    assert_eq!(request.market_data_usage_capacity, None);
    core.on_event(Event::Frame(frame(&ResponseAcceptAgreement {
        template_id: 505,
        user_msg: vec![id.clone()],
        rp_code: vec!["0".into()],
    })));
    let reply = answer(&mut rx).unwrap().unwrap();
    assert_eq!(reply.len(), 1);
    assert!(reply[0].error.is_none());
    assert!(matches!(
        reply[0].message,
        RithmicMessage::ResponseAcceptAgreement(_)
    ));
}

#[tokio::test]
async fn agreement_content_and_terminal_refusal_are_both_preserved() {
    let mut core = plant_core::<RepositoryPlant>();
    let (tx, mut rx) = oneshot::channel();
    let effects = core.on_event(Event::Command(RepositoryPlantCommand::ShowAgreement {
        agreement_id: "terms".into(),
        response_sender: tx,
    }));
    let [Effect::Send { id, .. }] = effects.as_slice() else {
        panic!()
    };
    let content = frame(&ResponseShowAgreement {
        template_id: 507,
        user_msg: vec![id.clone()],
        rq_handler_rp_code: vec!["0".into()],
        agreement: Some(vec![0, 255, 1]),
        ..Default::default()
    });
    core.on_event(Event::Frame(content.clone()));
    assert!(
        answer(&mut rx).is_none(),
        "partial content must not complete the request"
    );
    core.on_event(Event::Frame(frame(&ResponseShowAgreement {
        template_id: 507,
        user_msg: vec![id.clone()],
        rp_code: vec!["6".into(), "agreement unavailable".into()],
        ..Default::default()
    })));
    let reply = answer(&mut rx).unwrap().unwrap();
    assert_eq!(reply.len(), 2);
    assert_eq!(reply[0], content);
    assert!(matches!(
        reply[1].error,
        Some(RithmicError::RequestRejected { .. })
    ));
    assert_eq!(reply[1].rp_code_text(), Some("agreement unavailable"));
}

#[tokio::test]
async fn empty_lists_and_terminal_refusals_complete_without_losing_errors() {
    for (code, message) in [("7", "no data"), ("6", "permission denied")] {
        let mut core = plant_core::<RepositoryPlant>();
        let (tx, mut rx) = oneshot::channel();
        let effects = core.on_event(Event::Command(
            RepositoryPlantCommand::ListUnacceptedAgreements(tx),
        ));
        let [Effect::Send { id, .. }] = effects.as_slice() else {
            panic!()
        };
        core.on_event(Event::Frame(frame(&ResponseListUnacceptedAgreements {
            template_id: 501,
            user_msg: vec![id.clone()],
            rp_code: vec![code.into(), message.into()],
            ..Default::default()
        })));
        let reply = answer(&mut rx).unwrap().unwrap();
        assert_eq!(reply.len(), 1);
        assert_eq!(reply[0].error.is_some(), code == "6");
    }
}

fn test_handle() -> (
    RithmicRepositoryPlantHandle,
    mpsc::Receiver<RepositoryPlantCommand>,
) {
    let (sender, receiver) = mpsc::channel(4);
    let (_, subscription_receiver) = broadcast::channel(4);
    (
        RithmicRepositoryPlantHandle {
            sender,
            subscription_receiver,
        },
        receiver,
    )
}

#[tokio::test]
async fn login_refusal_is_an_error() {
    let (handle, mut receiver) = test_handle();
    let call = tokio::spawn(async move { handle.login().await });
    let RepositoryPlantCommand::Shared(PlantCommand::Login {
        response_sender, ..
    }) = receiver.recv().await.unwrap()
    else {
        panic!()
    };
    response_sender
        .send(Ok(vec![frame(&ResponseLogin {
            template_id: 11,
            rp_code: vec!["6".into(), "login refused".into()],
            ..Default::default()
        })]))
        .unwrap();
    assert!(matches!(
        call.await.unwrap(),
        Err(RithmicError::RequestRejected { .. })
    ));
}

#[tokio::test]
async fn disconnect_sends_close_even_when_logout_fails() {
    let (handle, mut receiver) = test_handle();
    let call = tokio::spawn(async move { handle.disconnect().await });
    test_support::assert_close_follows_failed_logout(
        &mut receiver,
        |command| match command {
            RepositoryPlantCommand::Shared(PlantCommand::Logout { response_sender }) => {
                Some(response_sender)
            }
            _ => None,
        },
        |command| matches!(command, RepositoryPlantCommand::Shared(PlantCommand::Close)),
    )
    .await;
    assert!(matches!(call.await.unwrap(), Err(RithmicError::SendFailed)));
}

#[tokio::test]
async fn abort_drains_pending_requests_and_broadcasts_connection_loss() {
    let mut core = plant_core::<RepositoryPlant>();
    let (tx, mut rx) = oneshot::channel();
    core.on_event(Event::Command(RepositoryPlantCommand::ShowAgreement {
        agreement_id: "terms".into(),
        response_sender: tx,
    }));
    let effects = core.on_event(Event::Command(RepositoryPlantCommand::Shared(
        PlantCommand::Abort,
    )));
    assert!(matches!(
        answer(&mut rx),
        Some(Err(RithmicError::ConnectionClosed))
    ));
    assert!(effects.iter().any(|e| matches!(e, Effect::Stop)));
    assert!(effects.iter().any(|e| matches!(e, Effect::Broadcast(r) if r.source == "repository_plant" && matches!(r.message, RithmicMessage::ConnectionError))));
    assert!(matches!(core.session, Session::Closed));
}
