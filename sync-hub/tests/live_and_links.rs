//! Live transcription (WebSocket), direct links (`GET /v1/connections`, QR parameters) and the Bluetooth LE channel
//! layer, end to end: a hub on localhost and clients that act like the phone apps.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::{json, Value};
use sync_hub::bluetooth::{BtError, BtSession};
use sync_hub::pinning::PinnedKeyVerifier;
use sync_hub::{DirectLinks, LiveError, LiveReady, LiveSession, LiveStart, LiveText, LiveTranscriber};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, Message};

// MARK: a transcriber that says which second it heard

#[derive(Clone, Default)]
struct Fake {
    unavailable: Arc<AtomicBool>,
    opened: Arc<Mutex<Vec<LiveStart>>>,
    slow: Option<Duration>,
}

struct FakeSession {
    samples: u64,
    said: u64,
    slow: Option<Duration>,
}

impl LiveTranscriber for Fake {
    fn check(&self) -> Result<(), LiveError> {
        if self.unavailable.load(Ordering::SeqCst) {
            Err(LiveError::Unavailable { retry_after_secs: 30, reason: "the computer is recording".into() })
        } else {
            Ok(())
        }
    }

    fn open(&self, start: LiveStart) -> sync_hub::LiveOpening {
        self.opened.lock().unwrap().push(start.clone());
        let slow = self.slow;
        Box::pin(async move {
            let ready = LiveReady { engine: "fake".into(), model: Some("m1".into()), language: start.language };
            Ok((ready, Box::new(FakeSession { samples: 0, said: 0, slow }) as Box<dyn LiveSession>))
        })
    }
}

impl LiveSession for FakeSession {
    fn feed(&mut self, samples: Vec<f32>) -> BoxFuture<'_, Result<Vec<LiveText>, LiveError>> {
        Box::pin(async move {
            if let Some(d) = self.slow {
                tokio::time::sleep(d).await;
            }
            self.samples += samples.len() as u64;
            let mut out = Vec::new();
            while (self.said + 1) * 16_000 <= self.samples {
                out.push(LiveText {
                    start_sec: self.said as f64,
                    end_sec: (self.said + 1) as f64,
                    text: format!("second {}", self.said),
                    is_final: true,
                });
                self.said += 1;
            }
            if self.samples > self.said * 16_000 {
                out.push(LiveText {
                    start_sec: self.said as f64,
                    end_sec: self.samples as f64 / 16_000.0,
                    text: "…".into(),
                    is_final: false,
                });
            }
            Ok(out)
        })
    }

    fn finish(&mut self) -> BoxFuture<'_, Result<Vec<LiveText>, LiveError>> {
        Box::pin(async move {
            let mut out = Vec::new();
            if self.samples > self.said * 16_000 {
                out.push(LiveText {
                    start_sec: self.said as f64,
                    end_sec: self.samples as f64 / 16_000.0,
                    text: format!("rest of second {}", self.said),
                    is_final: true,
                });
            }
            Ok(out)
        })
    }
}

// MARK: helpers

type Ws = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn tls_to(hub: &TestHub) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect(("127.0.0.1", hub.server.port())).await.unwrap();
    let mut config = PinnedKeyVerifier::client_config(hub.hub.fingerprint());
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(rustls::pki_types::ServerName::try_from("hub").unwrap(), tcp)
        .await
        .unwrap()
}

async fn open_live(hub: &TestHub, phone: &Phone, meeting: &str, subprotocol: bool) -> Result<Ws, tungstenite::Error> {
    let tls = tls_to(hub).await;
    let mut request = format!("wss://127.0.0.1:{}/v1/meetings/{meeting}/live", hub.server.port())
        .into_client_request()
        .unwrap();
    let token = phone.token.clone().unwrap();
    request.headers_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    if subprotocol {
        request.headers_mut().insert("sec-websocket-protocol", "salescopilot-live.1".parse().unwrap());
    }
    tokio_tungstenite::client_async(request, tls).await.map(|(ws, _)| ws)
}

/// The HTTP status and problem code of a refused upgrade.
fn refused(result: Result<Ws, tungstenite::Error>) -> (u16, String) {
    match result {
        Err(tungstenite::Error::Http(response)) => {
            let body: Value = response
                .body()
                .as_ref()
                .and_then(|b| serde_json::from_slice(b).ok())
                .unwrap_or(Value::Null);
            let code = body["code"].as_str().unwrap_or_default().to_string();
            (response.status().as_u16(), code)
        }
        Err(e) => panic!("expected an HTTP error, got {e}"),
        Ok(_) => panic!("expected the upgrade to be refused"),
    }
}

async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into())).await.unwrap();
}

fn start_msg(rate: u32, start_sec: f64) -> Value {
    json!({ "type": "start", "audio": { "format": "pcm_s16le", "sampleRate": rate, "channels": 1 }, "startSec": start_sec, "languageCode": "nl" })
}

/// PCM s16le of `seconds` at `rate`.
fn pcm(rate: u32, seconds: f64) -> Vec<u8> {
    let n = (rate as f64 * seconds) as usize;
    (0..n).flat_map(|i| (((i % 100) as i16 - 50) * 300).to_le_bytes()).collect()
}

/// The next JSON message, or the close frame as `{"close": code}`.
async fn next(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next()).await.expect("a message in time");
        match msg {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(Message::Close(frame))) => {
                return json!({ "close": frame.map(|f| u16::from(f.code)).unwrap_or(0) })
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => return json!({ "transport": e.to_string() }),
            None => return json!({ "close": 0 }),
        }
    }
}

async fn recording_meeting(phone: &Phone) -> String {
    let id = new_id();
    let reply = phone.create_meeting(&meeting_doc(&id, &phone.device_id, "recording")).await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    id
}

// MARK: live transcription

#[tokio::test]
async fn hello_lists_live_transcription_only_when_the_app_offers_it() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let hello = phone.get("/v1/hello").await.body;
    assert!(hello.get("features").is_none());
    let meeting = recording_meeting(&phone).await;
    assert_eq!(refused(open_live(&hub, &phone, &meeting, true).await).0, 404);

    hub.hub.set_live_transcriber(Some(Arc::new(Fake::default())));
    let hello = phone.get("/v1/hello").await.body;
    assert_eq!(hello["features"], json!(["live-transcription"]));
    assert_eq!(hello["liveAudio"]["formats"], json!(["pcm_s16le"]));
    let rates: Vec<u64> = hello["liveAudio"]["sampleRates"].as_array().unwrap().iter().map(|r| r.as_u64().unwrap()).collect();
    assert!(rates.contains(&8000) && rates.contains(&16000));
}

#[tokio::test]
async fn a_live_session_transcribes_on_the_meeting_timeline() {
    let hub = start_hub().await;
    let fake = Fake::default();
    hub.hub.set_live_transcriber(Some(Arc::new(fake.clone())));
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let meeting = recording_meeting(&phone).await;

    let mut ws = open_live(&hub, &phone, &meeting, true).await.unwrap();
    // A resumed session: the phone's audio starts at 10 s of meeting time, at 8 kHz over a slow link.
    send(&mut ws, start_msg(8000, 10.0)).await;
    let ready = next(&mut ws).await;
    assert_eq!(ready, json!({ "type": "ready", "engine": "fake", "model": "m1", "languageCode": "nl" }));
    let started = fake.opened.lock().unwrap()[0].clone();
    assert_eq!((started.meeting_id.as_str(), started.device_id.as_str()), (meeting.as_str(), phone.device_id.as_str()));

    // 2.5 s of audio in frames of odd sizes, so samples straddle frames.
    let audio = pcm(8000, 2.5);
    for piece in audio.chunks(3999) {
        ws.send(Message::Binary(piece.to_vec().into())).await.unwrap();
    }
    send(&mut ws, json!({ "type": "stop" })).await;

    let mut finals = Vec::new();
    let mut partial_ids = Vec::new();
    let mut last_progress = 0.0;
    loop {
        let msg = next(&mut ws).await;
        match msg["type"].as_str() {
            Some("segment") if msg["final"] == true => finals.push(msg),
            Some("segment") => partial_ids.push(msg["id"].as_str().unwrap().to_string()),
            Some("progress") => last_progress = msg["processedSec"].as_f64().unwrap(),
            Some("done") => break,
            _ => panic!("unexpected {msg}"),
        }
    }
    let texts: Vec<(&str, f64, f64, &str)> = finals
        .iter()
        .map(|m| (m["id"].as_str().unwrap(), m["startSec"].as_f64().unwrap(), m["endSec"].as_f64().unwrap(), m["text"].as_str().unwrap()))
        .collect();
    assert_eq!(
        texts,
        vec![
            ("live-10000", 10.0, 11.0, "second 0"),
            ("live-11000", 11.0, 12.0, "second 1"),
            ("live-12000", 12.0, 12.5, "rest of second 2"),
        ]
    );
    // Partial results use the id their final segment gets.
    assert!(partial_ids.iter().all(|id| texts.iter().any(|t| t.0 == id)), "{partial_ids:?}");
    assert!((last_progress - 12.5).abs() < 0.01, "{last_progress}");
    assert_eq!(next(&mut ws).await, json!({ "close": 1000 }));
    assert_eq!(hub.hub.live_session_count(), 0);
}

#[tokio::test]
async fn live_sessions_are_refused_with_the_right_codes() {
    let hub = start_hub().await;
    let fake = Fake::default();
    hub.hub.set_live_transcriber(Some(Arc::new(fake.clone())));
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let other = Phone::pair(&hub, "Pixel", Some("own")).await;

    // Unknown meeting, and one outside another phone's scope.
    assert_eq!(refused(open_live(&hub, &phone, &new_id(), true).await), (404, "not_found".into()));
    let meeting = recording_meeting(&phone).await;
    assert_eq!(refused(open_live(&hub, &other, &meeting, true).await), (404, "not_found".into()));
    // A phone that sees every meeting still can't stream audio into another phone's meeting.
    let colleague = Phone::pair(&hub, "iPad", None).await;
    assert_eq!(refused(open_live(&hub, &colleague, &meeting, true).await), (403, "forbidden".into()));

    // An ended meeting.
    let ended = new_id();
    phone.create_meeting(&meeting_doc(&ended, &phone.device_id, "ended")).await;
    assert_eq!(refused(open_live(&hub, &phone, &ended, true).await), (409, "meeting_not_recording".into()));

    // Without the subprotocol.
    assert_eq!(refused(open_live(&hub, &phone, &meeting, false).await).0, 400);

    // The computer is busy.
    fake.unavailable.store(true, Ordering::SeqCst);
    let tls = tls_to(&hub).await;
    let mut request = format!("wss://127.0.0.1:{}/v1/meetings/{meeting}/live", hub.server.port()).into_client_request().unwrap();
    request.headers_mut().insert("authorization", format!("Bearer {}", phone.token.clone().unwrap()).parse().unwrap());
    request.headers_mut().insert("sec-websocket-protocol", "salescopilot-live.1".parse().unwrap());
    match tokio_tungstenite::client_async(request, tls).await {
        Err(tungstenite::Error::Http(response)) => {
            assert_eq!(response.status().as_u16(), 503);
            assert_eq!(response.headers()["retry-after"], "30");
        }
        other => panic!("expected 503, got {:?}", other.map(|_| ())),
    }
    fake.unavailable.store(false, Ordering::SeqCst);

    // Unsupported audio, after the upgrade.
    let mut ws = open_live(&hub, &phone, &meeting, true).await.unwrap();
    send(&mut ws, json!({ "type": "start", "audio": { "format": "pcm_s16le", "sampleRate": 16000, "channels": 2 }, "startSec": 0 })).await;
    assert_eq!(next(&mut ws).await["code"], "unsupported_audio_format");
    assert_eq!(next(&mut ws).await, json!({ "close": 1008 }));

    // Audio before start.
    let mut ws = open_live(&hub, &phone, &meeting, true).await.unwrap();
    ws.send(Message::Binary(pcm(16000, 0.1).into())).await.unwrap();
    assert_eq!(next(&mut ws).await["code"], "bad_request");

    // Deleting the meeting ends its session.
    let mut ws = open_live(&hub, &phone, &meeting, true).await.unwrap();
    send(&mut ws, start_msg(16000, 0.0)).await;
    assert_eq!(next(&mut ws).await["type"], "ready");
    let etag = phone.get(&format!("/v1/meetings/{meeting}")).await.etag();
    let deleted = phone.send(reqwest::Method::DELETE, &format!("/v1/meetings/{meeting}"), &[("if-match", etag.as_str())], None).await;
    assert!(deleted.status.is_success(), "{:?}", deleted.body);
    let mut code = Value::Null;
    for _ in 0..5 {
        let msg = next(&mut ws).await;
        if msg["type"] == "error" {
            code = msg["code"].clone();
            break;
        }
    }
    assert_eq!(code, "meeting_deleted");
}

#[tokio::test]
async fn a_new_session_from_the_same_phone_replaces_the_old_one() {
    let hub = start_hub().await;
    hub.hub.set_live_transcriber(Some(Arc::new(Fake::default())));
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let meeting = recording_meeting(&phone).await;

    let mut first = open_live(&hub, &phone, &meeting, true).await.unwrap();
    send(&mut first, start_msg(16000, 0.0)).await;
    assert_eq!(next(&mut first).await["type"], "ready");

    // The phone moved to another link and resumes.
    let mut second = open_live(&hub, &phone, &meeting, true).await.unwrap();
    assert_eq!(next(&mut first).await["code"], "session_replaced");
    assert_eq!(next(&mut first).await, json!({ "close": 1000 }));
    send(&mut second, start_msg(16000, 4.2)).await;
    assert_eq!(next(&mut second).await["type"], "ready");
    second.send(Message::Binary(pcm(16000, 1.0).into())).await.unwrap();
    let segment = loop {
        let msg = next(&mut second).await;
        if msg["type"] == "segment" && msg["final"] == true {
            break msg;
        }
    };
    assert_eq!((segment["id"].as_str().unwrap(), segment["startSec"].as_f64().unwrap()), ("live-4200", 4.2));
    assert_eq!(hub.hub.live_session_count(), 1);
}

#[tokio::test]
async fn removing_a_phone_on_the_computer_ends_its_session() {
    let hub = start_hub().await;
    hub.hub.set_live_transcriber(Some(Arc::new(Fake::default())));
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let meeting = recording_meeting(&phone).await;
    let mut ws = open_live(&hub, &phone, &meeting, true).await.unwrap();
    send(&mut ws, start_msg(16000, 0.0)).await;
    assert_eq!(next(&mut ws).await["type"], "ready");
    assert!(hub.hub.remove_device(&phone.device_id).await.unwrap());
    assert_eq!(next(&mut ws).await["code"], "device_revoked");
    assert_eq!(next(&mut ws).await, json!({ "close": 1000 }));
    assert_eq!(hub.hub.live_session_count(), 0);
}

#[tokio::test]
async fn a_hub_that_falls_too_far_behind_ends_the_session() {
    let hub = start_hub().await;
    let fake = Fake { slow: Some(Duration::from_secs(3)), ..Fake::default() };
    hub.hub.set_live_transcriber(Some(Arc::new(fake)));
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let meeting = recording_meeting(&phone).await;
    let mut ws = open_live(&hub, &phone, &meeting, true).await.unwrap();
    send(&mut ws, start_msg(16000, 0.0)).await;
    assert_eq!(next(&mut ws).await["type"], "ready");
    // 70 seconds of audio at once.
    for piece in pcm(16000, 70.0).chunks(64 * 1024) {
        if ws.send(Message::Binary(piece.to_vec().into())).await.is_err() {
            break;
        }
    }
    let mut code = Value::Null;
    for _ in 0..10 {
        let msg = next(&mut ws).await;
        if msg["type"] == "error" {
            code = msg["code"].clone();
            break;
        }
    }
    assert_eq!(code, "too_far_behind");
    assert_eq!(next(&mut ws).await, json!({ "close": u16::from(CloseCode::Again) }));
}

// MARK: direct links

#[tokio::test]
async fn connections_and_the_qr_code_list_the_direct_links() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "Pixel", None).await;
    assert_eq!(phone.get("/v1/connections").await.body, json!({ "links": [], "wifiDirect": null }));
    assert!(!hub.hub.start_pairing().qr_uri.contains("&l="));

    hub.hub.set_direct_links(DirectLinks {
        links: vec!["wd".into(), "bt".into()],
        wifi_direct: Some(("DIRECT-SC-7Q2M".into(), "k3ZpWq9rTt".into())),
    });
    assert_eq!(
        phone.get("/v1/connections").await.body,
        json!({ "links": ["wd", "bt"], "wifiDirect": { "networkName": "DIRECT-SC-7Q2M", "passphrase": "k3ZpWq9rTt" } })
    );
    assert!(hub.hub.start_pairing().qr_uri.ends_with("&l=wd,bt&wd=DIRECT-SC-7Q2M&wp=k3ZpWq9rTt"));

    // Only for paired phones.
    let stranger = Phone::new(&hub, "stranger");
    assert_eq!(stranger.get("/v1/connections").await.status, StatusCode::UNAUTHORIZED);
}

// MARK: Bluetooth LE channels

/// The phone's end of the GATT link: turns channel streams into chunks and back, like the phone apps do.
struct PhoneLink {
    writes: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    streams: Arc<Mutex<std::collections::HashMap<u8, tokio::io::WriteHalf<tokio::io::DuplexStream>>>>,
}

/// Connects a [`BtSession`] to a phone-side link with the given ATT MTU; returns the link.
fn connect(hub: &TestHub, mtu: usize) -> (PhoneLink, Arc<Mutex<Option<BtError>>>) {
    let (mut session, mut notifications) = BtSession::new(hub.hub.clone(), "phone-1", mtu).unwrap();
    let (writes, mut to_hub) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let error = Arc::new(Mutex::new(None));
    let err = error.clone();
    // The "radio", phone → hub: every write is at most MTU − 3 bytes.
    tokio::spawn(async move {
        while let Some(chunk) = to_hub.recv().await {
            assert!(chunk.len() <= mtu - 3, "write of {} bytes over MTU {mtu}", chunk.len());
            if let Err(e) = session.received(&chunk) {
                *err.lock().unwrap() = Some(e);
            }
        }
    });
    let streams: Arc<Mutex<std::collections::HashMap<u8, tokio::io::WriteHalf<tokio::io::DuplexStream>>>> = Default::default();
    let to_phone = streams.clone();
    // Hub → phone notifications.
    tokio::spawn(async move {
        while let Some(chunk) = notifications.recv().await {
            assert!(chunk.len() <= mtu - 3, "notification of {} bytes over MTU {mtu}", chunk.len());
            let channel = chunk[0];
            let writer = to_phone.lock().unwrap().remove(&channel);
            if let Some(mut w) = writer {
                if chunk.len() == 1 {
                    let _ = w.shutdown().await;
                } else if w.write_all(&chunk[1..]).await.is_ok() {
                    to_phone.lock().unwrap().insert(channel, w);
                }
                // A write error: the phone's app already closed its end of the channel.
            }
        }
    });
    (PhoneLink { writes, streams }, error)
}

impl PhoneLink {
    /// Opens a channel and returns the phone's byte stream on it.
    fn open(&self, channel: u8, mtu: usize) -> tokio::io::DuplexStream {
        let (app_end, link_end) = tokio::io::duplex(64 * 1024);
        let (mut read, write) = tokio::io::split(link_end);
        self.streams.lock().unwrap().insert(channel, write);
        let writes = self.writes.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; mtu - 4];
            loop {
                let n = read.read(&mut buf).await.unwrap_or(0);
                let mut chunk = vec![channel];
                chunk.extend_from_slice(&buf[..n]);
                let _ = writes.send(chunk);
                if n == 0 {
                    return;
                }
            }
        });
        app_end
    }
}

/// HTTPS over a channel stream: pinned TLS, then one HTTP/1.1 connection.
async fn https_over(
    stream: tokio::io::DuplexStream,
    fingerprint: &str,
) -> hyper::client::conn::http1::SendRequest<http_body_util::Full<bytes::Bytes>> {
    let mut config = PinnedKeyVerifier::client_config(fingerprint);
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(rustls::pki_types::ServerName::try_from("hub").unwrap(), stream)
        .await
        .expect("pinned TLS over the Bluetooth channel");
    let (sender, connection) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls)).await.unwrap();
    tokio::spawn(connection);
    sender
}

async fn get_json(
    sender: &mut hyper::client::conn::http1::SendRequest<http_body_util::Full<bytes::Bytes>>,
    path: &str,
    token: Option<&str>,
) -> (u16, Value) {
    use http_body_util::BodyExt;
    let mut request = hyper::Request::get(path).header("host", "hub");
    if let Some(t) = token {
        request = request.header("authorization", format!("Bearer {t}"));
    }
    let response = sender.send_request(request.body(http_body_util::Full::default()).unwrap()).await.unwrap();
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn the_api_runs_over_bluetooth_channels() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let token = phone.token.clone().unwrap();
    // The smallest ATT MTU: 19 bytes of stream per chunk.
    let mtu = 23;
    let (link, error) = connect(&hub, mtu);

    // Two channels side by side.
    let mut zero = https_over(link.open(0, mtu), hub.hub.fingerprint()).await;
    let mut one = https_over(link.open(1, mtu), hub.hub.fingerprint()).await;
    let (hello, me) = tokio::join!(get_json(&mut zero, "/v1/hello", None), get_json(&mut one, "/v1/devices/me", Some(&token)));
    assert_eq!((hello.0, hello.1["hubId"].as_str()), (200, Some(hub.hub.hub_id())));
    assert_eq!((me.0, me.1["deviceId"].as_str()), (200, Some(phone.device_id.as_str())));
    // Several requests on one channel (keep-alive).
    assert_eq!(get_json(&mut zero, "/v1/connections", Some(&token)).await.0, 200);
    assert_eq!(get_json(&mut zero, "/v1/devices/me", None).await.0, 401);

    // Closing channel 0 and opening it again.
    drop(zero);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut again = https_over(link.open(0, mtu), hub.hub.fingerprint()).await;
    assert_eq!(get_json(&mut again, "/v1/hello", None).await.0, 200);

    // At most eight phones at once.
    let more: Vec<_> = (0..7).map(|i| BtSession::new(hub.hub.clone(), &format!("phone-{}", i + 2), mtu)).collect();
    assert!(more.iter().all(Result::is_ok));
    assert_eq!(BtSession::new(hub.hub.clone(), "phone-10", mtu).err(), Some(BtError::TooManySessions));
    drop(more);
    assert!(BtSession::new(hub.hub.clone(), "phone-10", mtu).is_ok());

    // A phone that sends far more than the hub reads is cut off (the hub reads nothing before the TLS hello).
    let (mut flood, _rx) = BtSession::new(hub.hub.clone(), "flood", mtu).unwrap();
    let junk = vec![3u8; 20];
    let result = (0..20_000).map(|_| flood.received(&junk)).find(|r| r.is_err());
    assert_eq!(result, Some(Err(BtError::Overloaded)));

    // A chunk for a channel that doesn't exist.
    link.writes.send(vec![7, 1, 2, 3]).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*error.lock().unwrap(), Some(BtError::BadChunk));
}

#[tokio::test]
async fn live_transcription_works_over_bluetooth() {
    let hub = start_hub().await;
    hub.hub.set_live_transcriber(Some(Arc::new(Fake::default())));
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let meeting = recording_meeting(&phone).await;
    let mtu = 185;
    let (link, _) = connect(&hub, mtu);

    let stream = link.open(2, mtu);
    let mut config = PinnedKeyVerifier::client_config(hub.hub.fingerprint());
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(rustls::pki_types::ServerName::try_from("hub").unwrap(), stream)
        .await
        .unwrap();
    let mut request = format!("wss://hub/v1/meetings/{meeting}/live").into_client_request().unwrap();
    request.headers_mut().insert("authorization", format!("Bearer {}", phone.token.clone().unwrap()).parse().unwrap());
    request.headers_mut().insert("sec-websocket-protocol", "salescopilot-live.1".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::client_async(request, tls).await.unwrap();
    ws.send(Message::Text(start_msg(8000, 0.0).to_string().into())).await.unwrap();
    let ready = loop {
        if let Some(Ok(Message::Text(t))) = ws.next().await {
            break serde_json::from_str::<Value>(&t).unwrap();
        }
    };
    assert_eq!(ready["type"], "ready");
    ws.send(Message::Binary(pcm(8000, 1.0).into())).await.unwrap();
    ws.send(Message::Text(json!({ "type": "stop" }).to_string().into())).await.unwrap();
    let mut finals = Vec::new();
    while let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["type"] == "segment" && v["final"] == true {
                finals.push(v["text"].as_str().unwrap().to_string());
            }
            if v["type"] == "done" {
                break;
            }
        }
    }
    assert_eq!(finals, vec!["second 0"]);
}
