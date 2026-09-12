//! Example: watch every frame of ONE history replay on the raw socket.
//!
//! This bypasses the crate's request handler entirely. It opens the
//! WebSocket itself, logs in, sends one replay request and records the
//! envelope of every frame the server sends back — including any frame that
//! arrives AFTER the reply's end marker — so what the venue does with a
//! large window is on record with no client-side interpretation in the way.
//!
//! The reader does nothing per frame but decode the envelope and count, so a
//! cut it observes is not the reader's pace. `SLOW_MS` adds a per-frame
//! delay to see whether the venue reacts to a slow consumer.
//!
//! Run with (credentials from the environment or a `.env`):
//!
//! ```text
//! RITHMIC_URL=wss://rprotocol.rithmic.com:443 RITHMIC_USER=… RITHMIC_PW=… \
//! RITHMIC_SYSTEM_NAME=… RITHMIC_APP_NAME=… RITHMIC_APP_VERSION=… \
//! KIND=vp SYMBOL=MNQU6 EXCHANGE=CME DAYS_BACK=7 \
//! cargo run --release --example replay_frames
//! ```
//!
//! - `KIND`: `vp` (template 208, one-minute per-price bars), `minute` or
//!   `second` (template 202 with `PERIOD`, default 1), `tick` (template 206,
//!   one-tick bars).
//! - `DAYS_BACK` (default 7) or `START`/`END` in Unix seconds; `END` defaults
//!   to now.
//! - `RESUME_BARS` (default `true`).
//! - `WAIT_SECS` (default 150): how long to keep reading after the first end
//!   marker, so a continuation and a late terminal are seen.
//! - `MAX_SECS` (default 600): how long to wait for a first end marker.
//! - `SLOW_MS` (default 0): per-frame delay, to act as a slow consumer.
//! - `RESUME_KEY=1`: when a frame carries a `request_key` and no response
//!   code (the venue's truncation notice), send `RequestResumeBars` (template
//!   210) with that key as id `probe-2` and record what comes back.
//!   `RESUME_KEY=all` resumes after every notice until a frame with a
//!   response code closes the replay, so the whole protocol is on record.
//! - `SECOND_REQUEST=1`: send a second, small replay (one day of one-minute
//!   bars, id `probe-2`) the moment the first frame without
//!   `rq_handler_rp_code` arrives, as a client that took that frame for the
//!   end of the reply would; what the venue then does to the first reply is
//!   recorded.
//! - `DOTENV`: path of a `.env` to load first. `RITHMIC_USERNAME` /
//!   `RITHMIC_PASSWORD` are accepted as aliases of `RITHMIC_USER` /
//!   `RITHMIC_PW`, and `RITHMIC_SERVER=chicago` (or another Rithmic gateway
//!   name) as an alias of `RITHMIC_URL`.

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use rithmic_rs::rti::{
    RequestHeartbeat, RequestLogin, RequestLogout, RequestResumeBars, RequestTickBarReplay,
    RequestTimeBarReplay, RequestVolumeProfileMinuteBars, request_login::SysInfraType,
    request_tick_bar_replay, request_time_bar_replay,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// The fields every Rithmic response shares, plus the ones a replay frame
/// is stamped with. Rithmic's field numbers are global, so one struct reads
/// the envelope of any template; fields a template lacks decode as empty.
#[derive(Clone, PartialEq, prost::Message)]
struct Envelope {
    #[prost(int32, optional, tag = "154467")]
    template_id: Option<i32>,
    #[prost(string, optional, tag = "132758")]
    request_key: Option<String>,
    #[prost(string, repeated, tag = "132760")]
    user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132764")]
    rq_handler_rp_code: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    rp_code: Vec<String>,
    /// Time bar and per-price replays: the bar's close.
    #[prost(int32, optional, tag = "119100")]
    marker: Option<i32>,
    /// Tick bar replays: [open, close] of the bar.
    #[prost(int32, repeated, packed = "false", tag = "119202")]
    data_bar_ssboe: Vec<i32>,
    /// Login response only.
    #[prost(double, optional, tag = "153633")]
    heartbeat_interval: Option<f64>,
}

impl Envelope {
    /// The second a data frame is stamped with, if it carries data.
    fn stamp(&self) -> Option<i32> {
        self.marker.or_else(|| self.data_bar_ssboe.last().copied())
    }

    /// The Reference Guide's rule (§3): `rq_handler_rp_code` present means
    /// more frames follow; absent, `rp_code` present means the sequence is
    /// over.
    fn is_terminal(&self) -> bool {
        self.rq_handler_rp_code.is_empty()
    }
}

/// One frame the probe saw, kept whole for the record when it matters.
struct Seen {
    at: Duration,
    wall: f64,
    bytes: usize,
    envelope: Envelope,
    hex: String,
}

/// What the probe watched for one request id.
#[derive(Default)]
struct Watch {
    // Before the first terminal.
    frames: u64,
    bytes: u64,
    data_frames: u64,
    first_stamp: Option<i32>,
    min_stamp: Option<i32>,
    max_stamp: Option<i32>,
    dataless: u64,
    first_frame_at: Option<Duration>,
    // Terminals, in order.
    terminals: Vec<Seen>,
    // After the first terminal.
    late_frames: u64,
    late_bytes: u64,
    late_data_frames: u64,
    late_first_at: Option<Duration>,
    late_last_at: Option<Duration>,
    late_min_stamp: Option<i32>,
    late_max_stamp: Option<i32>,
    // Pacing.
    last_at: Option<Duration>,
    max_gap: Duration,
    max_gap_at: Duration,
}

fn frame(req: &impl prost::Message) -> Vec<u8> {
    let len = req.encoded_len() as u32;
    let mut buf = Vec::with_capacity(len as usize + 4);
    buf.extend_from_slice(&len.to_be_bytes());
    req.encode(&mut buf)
        .expect("encoding into a Vec is infallible");
    buf
}

/// Builds a generated request, which is `#[non_exhaustive]` and so cannot be
/// written as a struct literal outside the crate.
fn build<T: Default>(fill: impl FnOnce(&mut T)) -> T {
    let mut value = T::default();
    fill(&mut value);
    value
}

fn wall_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn utc(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    let (h, m, sec) = ((s % 86_400) / 3_600, (s % 3_600) / 60, s % 60);
    let millis = ((secs - secs.floor()) * 1000.0) as u64;
    format!("{h:02}:{m:02}:{sec:02}.{millis:03}Z")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn require(keys: &[&str]) -> Result<String, String> {
    keys.iter()
        .find_map(|k| var(k))
        .ok_or_else(|| format!("set one of {}", keys.join(" / ")))
}

fn gateway(server: &str) -> String {
    match server.to_ascii_lowercase().trim() {
        "chicago" => "wss://rprotocol.rithmic.com:443".to_owned(),
        "sydney" => "wss://rprotocol-au.rithmic.com:443".to_owned(),
        "frankfurt" => "wss://rprotocol-de.rithmic.com:443".to_owned(),
        "test" => "wss://rituz00100.rithmic.com:443".to_owned(),
        raw => raw.to_owned(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = var("DOTENV") {
        dotenvy::from_path(&path).map_err(|e| format!("DOTENV {path}: {e}"))?;
    } else {
        dotenvy::dotenv().ok();
    }

    let url = var("RITHMIC_URL")
        .or_else(|| var("RITHMIC_SERVER").map(|s| gateway(&s)))
        .ok_or("set RITHMIC_URL or RITHMIC_SERVER")?;
    let user = require(&["RITHMIC_USER", "RITHMIC_USERNAME"])?;
    let password = require(&["RITHMIC_PW", "RITHMIC_PASSWORD"])?;
    let system_name = require(&["RITHMIC_SYSTEM_NAME"])?;
    let app_name = require(&["RITHMIC_APP_NAME"])?;
    let app_version = require(&["RITHMIC_APP_VERSION"])?;

    let kind = var("KIND").unwrap_or_else(|| "vp".to_owned());
    let symbol = var("SYMBOL").unwrap_or_else(|| "MNQU6".to_owned());
    let exchange = var("EXCHANGE").unwrap_or_else(|| "CME".to_owned());
    let period: i32 = var("PERIOD").map(|v| v.parse()).transpose()?.unwrap_or(1);
    let resume_bars: bool = var("RESUME_BARS")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(true);
    let wait_secs: u64 = var("WAIT_SECS")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(150);
    let slow_ms: u64 = var("SLOW_MS").map(|v| v.parse()).transpose()?.unwrap_or(0);
    let second_request = var("SECOND_REQUEST").as_deref() == Some("1");
    let resume_key = var("RESUME_KEY");
    let resume_every = resume_key.as_deref() == Some("all");
    let resume_key = resume_every || resume_key.as_deref() == Some("1");
    let now = wall_now() as i64;
    let end: i32 = var("END")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(i32::try_from(now)?);
    let start: i32 = match var("START") {
        Some(v) => v.parse()?,
        None => {
            let days: f64 = var("DAYS_BACK")
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(7.0);
            i32::try_from(i64::from(end) - (days * 86_400.0) as i64)?
        }
    };

    println!(
        "probe: {url} system={system_name} app={app_name}/{app_version} kind={kind} \
         symbol={symbol} exchange={exchange} period={period} start={start} end={end} \
         ({:.1} minutes) resume_bars={resume_bars} wait_secs={wait_secs} slow_ms={slow_ms}",
        f64::from(end - start) / 60.0
    );

    // A gateway name resolves to several nodes; one that accepts TCP and then
    // stalls or closes the handshake is skipped by asking again, which
    // re-resolves and rotates the order.
    let attempts: u32 = var("CONNECT_ATTEMPTS")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(3);
    let mut ws = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await {
            Ok(Ok((stream, _))) => {
                ws = Some(stream);
                break;
            }
            Ok(Err(e)) => println!("connect attempt {attempt}/{attempts} failed: {e}"),
            Err(_) => {
                println!("connect attempt {attempt}/{attempts}: no WebSocket handshake within 10s")
            }
        }
    }
    let ws = ws.ok_or_else(|| format!("{url}: could not connect in {attempts} attempts"))?;
    let (mut sink, mut stream) = ws.split();
    println!("connected at {}", utc(wall_now()));

    // Login: template 10 on the history plant. The reply is template 11 with
    // rp_code ["0"] and the heartbeat interval the server wants.
    let login = build(|r: &mut RequestLogin| {
        r.template_id = 10;
        r.template_version = Some("5.42".to_owned());
        r.user_msg = vec!["login".to_owned()];
        r.user = Some(user);
        r.password = Some(password);
        r.app_name = Some(app_name);
        r.app_version = Some(app_version);
        r.system_name = Some(system_name);
        r.infra_type = Some(SysInfraType::HistoryPlant as i32);
    });
    sink.send(Message::Binary(frame(&login).into())).await?;
    let mut heartbeat_secs = 60u64;
    loop {
        let Some(msg) = stream.next().await else {
            return Err("socket closed before the login reply".into());
        };
        let Message::Binary(data) = msg? else {
            continue;
        };
        let envelope = Envelope::decode(&data[4..])?;
        if envelope.template_id == Some(11) {
            println!(
                "login reply: rp_code={:?} heartbeat_interval={:?}",
                envelope.rp_code, envelope.heartbeat_interval
            );
            if envelope.rp_code.first().map(String::as_str) != Some("0") {
                return Err(format!("login refused: {:?}", envelope.rp_code).into());
            }
            if let Some(hb) = envelope.heartbeat_interval.filter(|hb| *hb >= 1.0) {
                heartbeat_secs = hb as u64;
            }
            break;
        }
        println!(
            "before login reply: template {:?} rp_code={:?}",
            envelope.template_id, envelope.rp_code
        );
    }

    // The one replay request, id "probe".
    const ID: &str = "probe";
    let request = match kind.as_str() {
        "vp" => frame(&build(|r: &mut RequestVolumeProfileMinuteBars| {
            r.template_id = 208;
            r.user_msg = vec![ID.to_owned()];
            r.symbol = Some(symbol.clone());
            r.exchange = Some(exchange.clone());
            r.bar_type_period = Some(period);
            r.start_index = Some(start);
            r.finish_index = Some(end);
            r.resume_bars = Some(resume_bars);
        })),
        "minute" | "second" => frame(&build(|r: &mut RequestTimeBarReplay| {
            r.template_id = 202;
            r.user_msg = vec![ID.to_owned()];
            r.symbol = Some(symbol.clone());
            r.exchange = Some(exchange.clone());
            r.bar_type = Some(if kind == "minute" {
                request_time_bar_replay::BarType::MinuteBar as i32
            } else {
                request_time_bar_replay::BarType::SecondBar as i32
            });
            r.bar_type_period = Some(period);
            r.start_index = Some(start);
            r.finish_index = Some(end);
            r.direction = Some(request_time_bar_replay::Direction::First as i32);
            r.time_order = Some(request_time_bar_replay::TimeOrder::Forwards as i32);
            r.resume_bars = Some(resume_bars);
        })),
        "tick" => frame(&build(|r: &mut RequestTickBarReplay| {
            r.template_id = 206;
            r.user_msg = vec![ID.to_owned()];
            r.symbol = Some(symbol.clone());
            r.exchange = Some(exchange.clone());
            r.bar_type = Some(request_tick_bar_replay::BarType::TickBar as i32);
            r.bar_sub_type = Some(request_tick_bar_replay::BarSubType::Regular as i32);
            r.bar_type_specifier = Some(period.to_string());
            r.start_index = Some(start);
            r.finish_index = Some(end);
            r.direction = Some(request_tick_bar_replay::Direction::First as i32);
            r.time_order = Some(request_tick_bar_replay::TimeOrder::Forwards as i32);
            r.resume_bars = Some(resume_bars);
        })),
        other => return Err(format!("KIND {other}: expected vp, minute, second or tick").into()),
    };

    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_secs.max(1)));
    heartbeat.tick().await; // the first tick is immediate
    let mut heartbeats_sent: u64 = 0;
    let mut others: u64 = 0;
    let mut second_frames: u64 = 0;

    let sent = Instant::now();
    let sent_wall = wall_now();
    sink.send(Message::Binary(request.into())).await?;
    println!("request sent at {} (t=0)", utc(sent_wall));

    let mut watch = Watch::default();
    let deadline_without_terminal = Duration::from_secs(
        var("MAX_SECS")
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(600),
    );
    let mut stop_at: Option<Instant> = None;
    let mut late_terminal_seen = false;

    loop {
        if stop_at.is_some_and(|stop| Instant::now() >= stop) {
            break;
        }
        if watch.terminals.is_empty() && sent.elapsed() > deadline_without_terminal {
            println!("no end marker within {deadline_without_terminal:?}; stopping");
            break;
        }
        let msg = tokio::select! {
            _ = heartbeat.tick() => {
                let hb = build(|r: &mut RequestHeartbeat| {
                    r.template_id = 18;
                    r.user_msg = vec!["hb".to_owned()];
                });
                sink.send(Message::Binary(frame(&hb).into())).await?;
                heartbeats_sent += 1;
                continue;
            }
            msg = stream.next() => msg,
            _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
        };
        let Some(msg) = msg else {
            println!(
                "t={:.3}s socket closed by the server",
                sent.elapsed().as_secs_f64()
            );
            break;
        };
        let data = match msg? {
            Message::Binary(data) => data,
            Message::Close(f) => {
                println!("t={:.3}s close frame: {f:?}", sent.elapsed().as_secs_f64());
                break;
            }
            _ => continue,
        };
        if slow_ms > 0 {
            tokio::time::sleep(Duration::from_millis(slow_ms)).await;
        }
        let at = sent.elapsed();
        let wall = wall_now();
        let envelope = match Envelope::decode(&data[4..]) {
            Ok(e) => e,
            Err(e) => {
                println!(
                    "t={:.3}s undecodable frame ({} bytes): {e}",
                    at.as_secs_f64(),
                    data.len()
                );
                continue;
            }
        };
        let stamp = envelope.stamp();
        if envelope.user_msg.first().map(String::as_str) == Some("probe-2") {
            second_frames += 1;
            if second_frames == 1 || envelope.rq_handler_rp_code.is_empty() {
                println!(
                    "t={:.3}s probe-2 frame #{second_frames}: template {:?} rq={:?} rp={:?} request_key={:?} stamp={:?}",
                    at.as_secs_f64(),
                    envelope.template_id,
                    envelope.rq_handler_rp_code,
                    envelope.rp_code,
                    envelope.request_key,
                    stamp
                );
            }
            continue;
        }
        if envelope.user_msg.first().map(String::as_str) != Some(ID) {
            others += 1;
            if envelope.template_id != Some(19) {
                println!(
                    "t={:.3}s other frame: template {:?} user_msg={:?} rq={:?} rp={:?}",
                    at.as_secs_f64(),
                    envelope.template_id,
                    envelope.user_msg,
                    envelope.rq_handler_rp_code,
                    envelope.rp_code
                );
            }
            continue;
        }

        // Pacing, over the whole life of the id.
        if let Some(last) = watch.last_at {
            let gap = at.saturating_sub(last);
            if gap > watch.max_gap {
                watch.max_gap = gap;
                watch.max_gap_at = at;
            }
        }
        watch.last_at = Some(at);

        let terminal = envelope.is_terminal();
        if watch.terminals.is_empty() {
            watch.frames += 1;
            watch.bytes += data.len() as u64;
            watch.first_frame_at.get_or_insert(at);
            match stamp {
                Some(s) => {
                    watch.data_frames += 1;
                    watch.first_stamp.get_or_insert(s);
                    watch.min_stamp = Some(watch.min_stamp.map_or(s, |m| m.min(s)));
                    watch.max_stamp = Some(watch.max_stamp.map_or(s, |m| m.max(s)));
                }
                None => watch.dataless += 1,
            }
            if watch.frames == 1 || watch.frames % 2_000 == 0 {
                println!(
                    "t={:.3}s frame #{} {} bytes so far, template {:?} rq={:?} rp={:?} stamp={:?}",
                    at.as_secs_f64(),
                    watch.frames,
                    watch.bytes,
                    envelope.template_id,
                    envelope.rq_handler_rp_code,
                    envelope.rp_code,
                    stamp
                );
            }
        } else {
            watch.late_frames += 1;
            watch.late_bytes += data.len() as u64;
            watch.late_first_at.get_or_insert(at);
            watch.late_last_at = Some(at);
            if let Some(s) = stamp {
                watch.late_data_frames += 1;
                watch.late_min_stamp = Some(watch.late_min_stamp.map_or(s, |m| m.min(s)));
                watch.late_max_stamp = Some(watch.late_max_stamp.map_or(s, |m| m.max(s)));
            }
            if watch.late_frames <= 3 || watch.late_frames % 100 == 0 {
                println!(
                    "t={:.3}s LATE frame #{} after the end marker: template {:?} rq={:?} rp={:?} stamp={:?} ({} bytes)",
                    at.as_secs_f64(),
                    watch.late_frames,
                    envelope.template_id,
                    envelope.rq_handler_rp_code,
                    envelope.rp_code,
                    stamp,
                    data.len()
                );
            }
        }

        if terminal {
            let ordinal = watch.terminals.len() + 1;
            println!(
                "t={:.3}s {} TERMINAL #{ordinal}: template {:?} rq_handler_rp_code={:?} rp_code={:?} request_key={:?} stamp={:?} {} bytes hex={}",
                at.as_secs_f64(),
                utc(wall),
                envelope.template_id,
                envelope.rq_handler_rp_code,
                envelope.rp_code,
                envelope.request_key,
                stamp,
                data.len(),
                hex(&data)
            );
            watch.terminals.push(Seen {
                at,
                wall,
                bytes: data.len(),
                envelope: envelope.clone(),
                hex: hex(&data),
            });
            let resume_with = envelope.request_key.clone().filter(|_| {
                (ordinal == 1 || resume_every) && resume_key && envelope.rp_code.is_empty()
            });
            if let Some(key) = resume_with {
                let resume = build(|r: &mut RequestResumeBars| {
                    r.template_id = 210;
                    r.user_msg = vec!["probe-2".to_owned()];
                    r.request_key = Some(key.clone());
                });
                sink.send(Message::Binary(frame(&resume).into())).await?;
                println!(
                    "t={:.3}s RESUME sent (RequestResumeBars request_key={key:?} as probe-2)",
                    sent.elapsed().as_secs_f64()
                );
            }
            if ordinal == 1 && second_request {
                let second = build(|r: &mut RequestTimeBarReplay| {
                    r.template_id = 202;
                    r.user_msg = vec!["probe-2".to_owned()];
                    r.symbol = Some(symbol.clone());
                    r.exchange = Some(exchange.clone());
                    r.bar_type = Some(request_time_bar_replay::BarType::MinuteBar as i32);
                    r.bar_type_period = Some(1);
                    r.start_index = Some(end - 86_400);
                    r.finish_index = Some(end);
                    r.direction = Some(request_time_bar_replay::Direction::First as i32);
                    r.time_order = Some(request_time_bar_replay::TimeOrder::Forwards as i32);
                    r.resume_bars = Some(true);
                });
                sink.send(Message::Binary(frame(&second).into())).await?;
                println!(
                    "t={:.3}s SECOND REQUEST sent (probe-2: one day of one-minute bars)",
                    sent.elapsed().as_secs_f64()
                );
            }
            if resume_every && envelope.rp_code.is_empty() {
                // A notice that was just resumed: keep reading, bounded by
                // the no-reply deadline from here.
                stop_at = Some(Instant::now() + deadline_without_terminal);
            } else if resume_every {
                stop_at = Some(Instant::now() + Duration::from_secs(5));
            } else if ordinal == 1 {
                // A refusal with no data streams nothing more; a marker that
                // closed data may be followed by a continuation, so wait.
                let refused_empty = watch.data_frames == 0
                    && envelope.rp_code.first().map(String::as_str) != Some("0");
                let wait = if refused_empty { 5 } else { wait_secs };
                stop_at = Some(Instant::now() + Duration::from_secs(wait));
            } else if !late_terminal_seen {
                late_terminal_seen = true;
                stop_at = Some(Instant::now() + Duration::from_secs(5));
            }
        }
    }

    // Logout (template 12) and close.
    let logout = build(|r: &mut RequestLogout| {
        r.template_id = 12;
        r.user_msg = vec!["logout".to_owned()];
    });
    let _ = sink.send(Message::Binary(frame(&logout).into())).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(msg)) = stream.next().await {
            let Message::Binary(data) = msg else { continue };
            let Ok(e) = Envelope::decode(&data[4..]) else {
                continue;
            };
            if e.template_id == Some(13) {
                println!("logout reply: rp_code={:?}", e.rp_code);
                break;
            }
        }
    })
    .await;
    let _ = sink.close().await;

    // The record.
    println!();
    println!("===== replay_frames summary =====");
    println!(
        "request: kind={kind} symbol={symbol} exchange={exchange} period={period} window=[{start}, {end}] ({:.1} minutes) resume_bars={resume_bars} sent {}",
        f64::from(end - start) / 60.0,
        utc(sent_wall)
    );
    let first_terminal_at = watch.terminals.first().map(|t| t.at);
    let streamed = first_terminal_at.unwrap_or_else(|| sent.elapsed());
    println!(
        "before the first end marker: {} frames ({} with data, {} dataless), {} bytes, first frame at {:.3}s, streamed {:.3}s = {:.0} frames/s, {:.0} KB/s",
        watch.frames,
        watch.data_frames,
        watch.dataless,
        watch.bytes,
        watch.first_frame_at.map_or(0.0, |d| d.as_secs_f64()),
        streamed.as_secs_f64(),
        watch.data_frames as f64 / streamed.as_secs_f64().max(1e-9),
        watch.bytes as f64 / 1024.0 / streamed.as_secs_f64().max(1e-9)
    );
    println!(
        "data stamps before the first end marker: first={:?} min={:?} max={:?}; window end={end}; shortfall={} minutes",
        watch.first_stamp,
        watch.min_stamp,
        watch.max_stamp,
        watch
            .max_stamp
            .map_or(f64::NAN, |m| f64::from(end - m) / 60.0)
    );
    for (i, t) in watch.terminals.iter().enumerate() {
        println!(
            "terminal #{}: t={:.3}s {} template {:?} rq_handler_rp_code={:?} rp_code={:?} request_key={:?} stamp={:?} {} bytes hex={}",
            i + 1,
            t.at.as_secs_f64(),
            utc(t.wall),
            t.envelope.template_id,
            t.envelope.rq_handler_rp_code,
            t.envelope.rp_code,
            t.envelope.request_key,
            t.envelope.stamp(),
            t.bytes,
            t.hex
        );
    }
    println!(
        "after the first end marker: {} frames ({} with data), {} bytes, from t={:?} to t={:?}, stamps min={:?} max={:?}",
        watch.late_frames,
        watch.late_data_frames,
        watch.late_bytes,
        watch.late_first_at.map(|d| d.as_secs_f64()),
        watch.late_last_at.map(|d| d.as_secs_f64()),
        watch.late_min_stamp,
        watch.late_max_stamp
    );
    println!(
        "largest gap between two frames of this id: {:.3}s, ending at t={:.3}s; heartbeats sent={heartbeats_sent}; other frames={others}; probe-2 frames={second_frames}; total run {:.1}s",
        watch.max_gap.as_secs_f64(),
        watch.max_gap_at.as_secs_f64(),
        sent.elapsed().as_secs_f64()
    );
    Ok(())
}
