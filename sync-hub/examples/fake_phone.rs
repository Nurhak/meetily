//! A fake phone for testing phone → computer sync without a phone. It talks to a running hub (the desktop app with
//! Settings → Phones turned on) the way the phone apps do, using the test clients in `tests/common`.
//!
//! ```sh
//! cargo run -p salescopilot-sync-hub --example fake_phone -- pair ABCD-EFGH        # the code shown in "Pair a phone"
//! cargo run -p salescopilot-sync-hub --example fake_phone -- record --say "Hello, this is a test meeting."
//! cargo run -p salescopilot-sync-hub --example fake_phone -- watch <meetingId>     # until the final transcript is in
//! cargo run -p salescopilot-sync-hub --example fake_phone -- changes
//! cargo run -p salescopilot-sync-hub --example fake_phone -- live --say "We should talk about the renewal."
//! ```
//!
//! Pairing by code works like on a phone: the hub's key is taken on first use and the verification number is
//! printed, to compare with the one in the pairing dialog. The token and the pinned key are kept in a state file
//! (`--state`, default `$TMPDIR/salescopilot-fake-phone.json`).

#[path = "../tests/common/mod.rs"]
mod common;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use common::{client, new_id, Phone};
use reqwest::StatusCode;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use serde_json::{json, Value};
use sync_hub::identity::certificate_fingerprint;
use sync_hub::pairing::{format_verification, verification_number};
use sync_hub::probe::{AudioProber, BasicProber};

const USAGE: &str = "\
usage: fake_phone [--state FILE] <command>

commands:
  pair CODE [--host HOST:PORT] [--name NAME]   pair with the code shown in Settings → Phones → Pair a phone
                                               (host defaults to 127.0.0.1:47821)
  record [--title TITLE] [--wav FILE | --say TEXT] [--lang CODE] [--no-draft]
                                               upload an ended meeting with a draft transcript and its recording
                                               (--say makes the WAV with macOS `say`; default: a short sentence)
  watch MEETING_ID [--timeout SECONDS]         wait until the hub has transcribed the meeting, then show the transcript
  show MEETING_ID                              show a meeting and its transcript
  changes [--since CURSOR]                     list the change feed
  live [--wav FILE | --say TEXT] [--lang CODE] [--rate 16000|8000] [--speed X] [--no-upload]
                                               record a meeting with live transcription: stream the audio in real
                                               time (X times faster with --speed), print the computer's transcript
                                               as it comes, then end the meeting and upload the recording so the
                                               final transcript follows (watch it). --rate 8000 is what phones send
                                               over Bluetooth.
  forget                                       delete the state file";

#[derive(serde::Serialize, serde::Deserialize)]
struct State {
    base: String,
    fingerprint: String,
    token: String,
    device_id: String,
    name: String,
}

impl State {
    fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| {
            format!(
                "not paired yet (no {}); run `pair CODE` first",
                path.display()
            )
        })?;
        Ok(serde_json::from_str(&text)?)
    }

    fn phone(&self) -> Phone {
        Phone {
            client: client(&self.fingerprint),
            base: self.base.clone(),
            token: Some(self.token.clone()),
            device_id: self.device_id.clone(),
            name: self.name.clone(),
        }
    }
}

struct Args(Vec<String>);

impl Args {
    /// Removes `--flag VALUE` and returns the value.
    fn option(&mut self, flag: &str) -> Option<String> {
        let i = self.0.iter().position(|a| a == flag)?;
        self.0.remove(i);
        (i < self.0.len()).then(|| self.0.remove(i))
    }

    fn flag(&mut self, flag: &str) -> bool {
        let found = self.0.iter().position(|a| a == flag);
        found.map(|i| self.0.remove(i)).is_some()
    }

    fn next(&mut self) -> Option<String> {
        (!self.0.is_empty()).then(|| self.0.remove(0))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args(std::env::args().skip(1).collect());
    let state_path = args
        .option("--state")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("salescopilot-fake-phone.json"));
    let Some(command) = args.next() else {
        println!("{USAGE}");
        return Ok(());
    };
    match command.as_str() {
        "pair" => {
            let host = args
                .option("--host")
                .unwrap_or_else(|| "127.0.0.1:47821".into());
            let name = args.option("--name").unwrap_or_else(|| "Fake Phone".into());
            let code = args
                .next()
                .context("pair needs the code shown in the app")?;
            pair(&state_path, &host, &name, &code).await
        }
        "record" => {
            let title = args.option("--title");
            let wav = args.option("--wav");
            let say = args.option("--say");
            let lang = args.option("--lang").unwrap_or_else(|| "en".into());
            let draft = !args.flag("--no-draft");
            record(&State::load(&state_path)?, title, wav, say, &lang, draft).await
        }
        "watch" => {
            let timeout = args
                .option("--timeout")
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or(600);
            let id = args.next().context("watch needs a meeting ID")?;
            watch(
                &State::load(&state_path)?,
                &id,
                Duration::from_secs(timeout),
            )
            .await
        }
        "show" => {
            let id = args.next().context("show needs a meeting ID")?;
            show(&State::load(&state_path)?.phone(), &id).await
        }
        "changes" => {
            let since = args.option("--since");
            changes(&State::load(&state_path)?.phone(), since.as_deref()).await
        }
        "live" => {
            let wav = args.option("--wav");
            let say = args.option("--say");
            let lang = args.option("--lang").unwrap_or_else(|| "en".into());
            let rate: u32 = args.option("--rate").map(|r| r.parse()).transpose()?.unwrap_or(16000);
            let speed: f64 = args.option("--speed").map(|r| r.parse()).transpose()?.unwrap_or(1.0);
            let upload = !args.flag("--no-upload");
            live(&State::load(&state_path)?, wav, say, &lang, rate, speed, upload).await
        }
        "forget" => {
            std::fs::remove_file(&state_path).ok();
            println!("Removed {}", state_path.display());
            Ok(())
        }
        _ => bail!("unknown command {command:?}\n\n{USAGE}"),
    }
}

async fn pair(state_path: &Path, host: &str, name: &str, code: &str) -> Result<()> {
    let base = format!("https://{host}");
    let fingerprint = first_use_fingerprint(&base).await?;
    let hello = reqwest_json(client(&fingerprint).get(format!("{base}/v1/hello"))).await?;
    println!(
        "Hub: {} ({})",
        hello["name"].as_str().unwrap_or("?"),
        hello["hubId"].as_str().unwrap_or("?")
    );
    println!("Key fingerprint: {fingerprint}");
    println!(
        "Verification number: {}  (must match the one in the pairing dialog)",
        format_verification(verification_number(&fingerprint, code))
    );

    let mut phone = Phone {
        client: client(&fingerprint),
        base: base.clone(),
        token: None,
        device_id: new_id(),
        name: name.to_string(),
    };
    let reply = phone.pair_with(code, None).await;
    if reply.status != StatusCode::CREATED {
        bail!("pairing failed: {} {}", reply.status, reply.body);
    }
    let state = State {
        base,
        fingerprint,
        token: phone
            .token
            .clone()
            .context("no token in the pairing reply")?,
        device_id: phone.device_id.clone(),
        name: name.to_string(),
    };
    std::fs::write(state_path, serde_json::to_string_pretty(&state)?)?;
    println!(
        "Paired as {name} ({}). State in {}",
        state.device_id,
        state_path.display()
    );
    Ok(())
}

async fn record(
    state: &State,
    title: Option<String>,
    wav: Option<String>,
    say: Option<String>,
    lang: &str,
    draft: bool,
) -> Result<()> {
    let phone = state.phone();
    let tmp = tempfile::tempdir()?;
    let wav_path = wav_file(tmp.path(), wav, say)?;
    let audio =
        std::fs::read(&wav_path).with_context(|| format!("reading {}", wav_path.display()))?;
    let duration = BasicProber
        .probe(&wav_path, "audio/wav")
        .with_context(|| format!("{} is not a WAV the hub accepts", wav_path.display()))?
        .duration_sec;

    let id = new_id();
    let ended = chrono::Utc::now();
    let started = ended - chrono::Duration::milliseconds((duration * 1000.0) as i64);
    let ts = |t: chrono::DateTime<chrono::Utc>| t.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let title = title.unwrap_or_else(|| format!("Fake phone meeting {}", ended.format("%H:%M:%S")));

    let mut meeting = common::meeting_doc(&id, &state.device_id, "ended");
    meeting["title"] = json!(title);
    meeting["updatedAt"] = json!(ts(ended));
    meeting["startedAt"] = json!(ts(started));
    meeting["endedAt"] = json!(ts(ended));
    meeting["timeZone"] = json!("Europe/Amsterdam");
    meeting["languageCode"] = json!(lang);
    meeting["recordedBy"]["deviceName"] = json!(state.name);
    let reply = phone.create_meeting(&meeting).await;
    if reply.status != StatusCode::CREATED {
        bail!(
            "creating the meeting failed: {} {}",
            reply.status,
            reply.body
        );
    }
    println!("Meeting {id} \"{title}\" ({duration:.1} s)");

    if draft {
        let mut transcript = common::transcript_doc(&id, &state.device_id, &new_id());
        transcript["updatedAt"] = json!(ts(ended));
        transcript["producedBy"]["createdAt"] = json!(ts(ended));
        transcript["languageCode"] = json!(lang);
        transcript["segments"] = json!([
            { "id": "s0001", "startSec": 0.0, "endSec": duration, "text": "(draft transcript from the fake phone)" }
        ]);
        let reply = phone.create_transcript(&transcript).await;
        if reply.status != StatusCode::CREATED {
            bail!(
                "creating the draft transcript failed: {} {}",
                reply.status,
                reply.body
            );
        }
        println!("Draft transcript uploaded");
    }

    let reply = phone
        .upload(&id, "audio.wav", "audio/wav", &audio, 1 << 20)
        .await;
    let complete = reply.header("upload-complete").as_deref() == Some("true")
        || reply.body["complete"] == json!(true);
    if !reply.status.is_success() || !complete {
        bail!("upload failed: {} {}", reply.status, reply.body);
    }
    println!("Recording uploaded ({} bytes)", audio.len());
    println!("\nWatch it: cargo run -p salescopilot-sync-hub --example fake_phone -- watch {id}");
    Ok(())
}

/// The WAV to send: the given file, or one made with macOS `say` (16 kHz mono 16-bit).
fn wav_file(dir: &Path, wav: Option<String>, say: Option<String>) -> Result<PathBuf> {
    if let Some(p) = wav {
        return Ok(PathBuf::from(p));
    }
    let text = say.unwrap_or_else(|| {
        "Hello, this is a test meeting from the fake phone. We talked about renewing the contract next month.".into()
    });
    let out = dir.join("audio.wav");
    let status = std::process::Command::new("say")
        .args(["-o"])
        .arg(&out)
        .args(["--file-format=WAVE", "--data-format=LEI16@16000", &text])
        .status()
        .context("running `say` (macOS only; pass --wav FILE elsewhere)")?;
    if !status.success() {
        bail!("`say` failed: {status}");
    }
    Ok(out)
}

/// Sample rate and 16-bit mono PCM samples of a WAV file.
fn wav_pcm(bytes: &[u8]) -> Result<(u32, Vec<i16>)> {
    if bytes.len() < 12 || &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        bail!("not a WAV file");
    }
    let (mut rate, mut channels, mut bits) = (0u32, 0u16, 0u16);
    let mut at = 12;
    while at + 8 <= bytes.len() {
        let id = &bytes[at..at + 4];
        let size = u32::from_le_bytes(bytes[at + 4..at + 8].try_into()?) as usize;
        let body = &bytes[at + 8..(at + 8 + size).min(bytes.len())];
        if id == b"fmt " && body.len() >= 16 {
            channels = u16::from_le_bytes([body[2], body[3]]);
            rate = u32::from_le_bytes(body[4..8].try_into()?);
            bits = u16::from_le_bytes([body[14], body[15]]);
        } else if id == b"data" {
            if channels != 1 || bits != 16 {
                bail!("the WAV must be mono 16-bit (it is {channels} channel(s), {bits} bits)");
            }
            let samples = body.chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])).collect();
            return Ok((rate, samples));
        }
        at += 8 + size + (size & 1);
    }
    bail!("no audio data in the WAV file")
}

/// Records a meeting with live transcription (sync-protocol.md, "Live transcription").
async fn live(
    state: &State,
    wav: Option<String>,
    say: Option<String>,
    lang: &str,
    rate: u32,
    speed: f64,
    upload: bool,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    let phone = state.phone();
    let hello = reqwest_json(phone.client.get(format!("{}/v1/hello", state.base))).await?;
    if !hello["features"].as_array().is_some_and(|f| f.iter().any(|x| x == "live-transcription")) {
        bail!("this computer doesn't offer live transcription (hello: {hello})");
    }
    let tmp = tempfile::tempdir()?;
    let wav_path = wav_file(tmp.path(), wav, say)?;
    let audio = std::fs::read(&wav_path).with_context(|| format!("reading {}", wav_path.display()))?;
    let (file_rate, samples) = wav_pcm(&audio)?;
    // What a phone on Bluetooth sends: 8 kHz, here by dropping every other sample of a 16 kHz file.
    let samples: Vec<i16> = if rate == file_rate {
        samples
    } else if rate * 2 == file_rate {
        samples.iter().step_by(2).copied().collect()
    } else {
        bail!("--rate must be {file_rate} or {} for this file", file_rate / 2);
    };
    let duration = samples.len() as f64 / rate as f64;

    // The meeting, recording.
    let id = new_id();
    let started = chrono::Utc::now();
    let ts = |t: chrono::DateTime<chrono::Utc>| t.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let title = format!("Fake phone live meeting {}", started.format("%H:%M:%S"));
    let mut meeting = common::meeting_doc(&id, &state.device_id, "recording");
    meeting["title"] = json!(title);
    meeting["updatedAt"] = json!(ts(started));
    meeting["startedAt"] = json!(ts(started));
    meeting["languageCode"] = json!(lang);
    meeting["recordedBy"]["deviceName"] = json!(state.name);
    let reply = phone.create_meeting(&meeting).await;
    if reply.status != StatusCode::CREATED {
        bail!("creating the meeting failed: {} {}", reply.status, reply.body);
    }
    let etag = reply.etag();
    println!("Meeting {id} \"{title}\" ({duration:.1} s of audio at {rate} Hz)");

    // The WebSocket, over TLS pinned to the hub's key.
    let authority = state.base.trim_start_matches("https://").to_string();
    let tcp = tokio::net::TcpStream::connect(&authority).await.with_context(|| format!("connecting to {authority}"))?;
    let mut config = sync_hub::pinning::PinnedKeyVerifier::client_config(state.fingerprint.clone());
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("hub")?, tcp)
        .await?;
    let mut request = format!("wss://{authority}/v1/meetings/{id}/live").into_client_request()?;
    request.headers_mut().insert("authorization", format!("Bearer {}", state.token).parse()?);
    request.headers_mut().insert("sec-websocket-protocol", sync_hub::live::SUBPROTOCOL.parse()?);
    let (ws, _) = match tokio_tungstenite::client_async(request, tls).await {
        Ok(ok) => ok,
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            let body = response.body().as_ref().map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
            bail!("the computer refused live transcription: {} {body}", response.status());
        }
        Err(e) => return Err(e.into()),
    };
    let (mut sink, mut stream) = ws.split();
    let start = json!({ "type": "start", "audio": { "format": "pcm_s16le", "sampleRate": rate, "channels": 1 }, "startSec": 0, "languageCode": lang });
    sink.send(Message::Text(start.to_string().into())).await?;

    // Audio in 100 ms frames at the speaking pace, while segments are printed as they arrive.
    let frame = (rate / 10) as usize;
    let pace = Duration::from_secs_f64(0.1 / speed.max(0.01));
    let sender = tokio::spawn(async move {
        let mut tick = tokio::time::interval(pace);
        for chunk in samples.chunks(frame) {
            tick.tick().await;
            let bytes: Vec<u8> = chunk.iter().flat_map(|s| s.to_le_bytes()).collect();
            if sink.send(Message::Binary(bytes.into())).await.is_err() {
                return sink;
            }
        }
        let _ = sink.send(Message::Text(json!({ "type": "stop" }).to_string().into())).await;
        sink
    });
    let mut finals: Vec<(f64, String)> = Vec::new();
    while let Some(msg) = stream.next().await {
        let text = match msg? {
            Message::Text(text) => text,
            Message::Close(frame) => {
                println!("(closed: {frame:?})");
                break;
            }
            _ => continue,
        };
        let v: Value = serde_json::from_str(&text)?;
        match v["type"].as_str() {
            Some("ready") => println!("Live transcription by {} ({})\n", v["engine"], v["model"]),
            Some("segment") => {
                let (start, end) = (v["startSec"].as_f64().unwrap_or(0.0), v["endSec"].as_f64().unwrap_or(0.0));
                let mark = if v["final"] == true { " " } else { "~" };
                println!("{mark}[{start:6.1}–{end:6.1}] {}", v["text"].as_str().unwrap_or(""));
                if v["final"] == true {
                    finals.push((start, v["text"].as_str().unwrap_or("").to_string()));
                }
            }
            Some("progress") => {}
            Some("done") => {
                println!("\nDone: {} segment(s)", finals.len());
                break;
            }
            Some("error") => bail!("live transcription stopped: {} {}", v["code"], v["message"]),
            _ => println!("{v}"),
        }
    }
    drop(sender.await);

    if !upload {
        return Ok(());
    }
    // The meeting ends; the recording goes up and the computer makes the final transcript.
    let ended = chrono::Utc::now();
    meeting["status"] = json!("ended");
    meeting["endedAt"] = json!(ts(ended));
    meeting["updatedAt"] = json!(ts(ended));
    let reply = phone.update_meeting(&meeting, &etag).await;
    if !reply.status.is_success() {
        bail!("ending the meeting failed: {} {}", reply.status, reply.body);
    }
    let reply = phone.upload(&id, "audio.wav", "audio/wav", &audio, 1 << 20).await;
    if !reply.status.is_success() {
        bail!("upload failed: {} {}", reply.status, reply.body);
    }
    println!("Meeting ended and recording uploaded. Final transcript:");
    println!("  cargo run -p salescopilot-sync-hub --example fake_phone -- watch {id}");
    Ok(())
}

async fn watch(state: &State, id: &str, timeout: Duration) -> Result<()> {
    let phone = state.phone();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = String::new();
    loop {
        let meeting = phone.get(&format!("/v1/meetings/{id}")).await;
        if !meeting.status.is_success() {
            bail!("{} {}", meeting.status, meeting.body);
        }
        let hub_state = &meeting.body["hubState"];
        let now = format!(
            "transcription: {} {}{}",
            hub_state["transcription"].as_str().unwrap_or("?"),
            hub_state["transcriptionProgress"],
            hub_state["transcriptionError"]
                .as_str()
                .map(|e| format!(" ({e})"))
                .unwrap_or_default()
        );
        if now != last {
            println!("{now}");
            last = now;
        }
        match hub_state["transcription"].as_str() {
            Some("done") => return show(&phone, id).await,
            Some("failed") | Some("skipped") => {
                bail!("transcription {}", hub_state["transcription"])
            }
            _ => {}
        }
        if tokio::time::Instant::now() > deadline {
            bail!("timed out");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn show(phone: &Phone, id: &str) -> Result<()> {
    let meeting = phone.get(&format!("/v1/meetings/{id}")).await;
    println!(
        "{} {}",
        meeting.status,
        serde_json::to_string_pretty(&meeting.body)?
    );
    let transcript = phone.get(&format!("/v1/meetings/{id}/transcript")).await;
    if transcript.status.is_success() {
        let t = &transcript.body;
        println!(
            "\nTranscript ({}, {} {}):",
            t["status"].as_str().unwrap_or("?"),
            t["producedBy"]["engine"].as_str().unwrap_or("?"),
            t["producedBy"]["model"].as_str().unwrap_or("")
        );
        for s in t["segments"].as_array().into_iter().flatten() {
            println!(
                "  [{:>6.1}–{:>6.1}] {}",
                s["startSec"].as_f64().unwrap_or_default(),
                s["endSec"].as_f64().unwrap_or_default(),
                s["text"].as_str().unwrap_or_default()
            );
        }
    } else {
        println!("\nNo transcript: {} {}", transcript.status, transcript.body);
    }
    Ok(())
}

async fn changes(phone: &Phone, since: Option<&str>) -> Result<()> {
    let mut since = since.map(str::to_string);
    loop {
        let page = phone.changes(since.as_deref()).await;
        if !page.status.is_success() {
            bail!("{} {}", page.status, page.body);
        }
        for c in page.body["changes"].as_array().into_iter().flatten() {
            println!(
                "{}  meeting {}  transcript {}  audio {}  transcription {}{}",
                c["meetingId"].as_str().unwrap_or("?"),
                c["meetingEtag"].as_str().unwrap_or("-"),
                c["transcriptEtag"].as_str().unwrap_or("-"),
                if c["audio"].is_null() { "-" } else { "yes" },
                c["hubState"]["transcription"].as_str().unwrap_or("-"),
                if c["deletedAt"].is_null() {
                    ""
                } else {
                    "  (deleted)"
                },
            );
        }
        since = page.body["cursor"].as_str().map(str::to_string);
        if page.body["hasMore"] != json!(true) {
            println!("cursor: {}", since.as_deref().unwrap_or("-"));
            return Ok(());
        }
    }
}

async fn reqwest_json(req: reqwest::RequestBuilder) -> Result<Value> {
    let res = req.send().await?;
    let status = res.status();
    let body: Value = res.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        bail!("{status} {body}");
    }
    Ok(body)
}

/// Connects once accepting any certificate and returns its key fingerprint (trust on first use, checked by the
/// person through the verification number).
async fn first_use_fingerprint(base: &str) -> Result<String> {
    let verifier = Arc::new(FirstUse::default());
    let config = rustls::ClientConfig::builder_with_provider(verifier.provider.clone())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    let client = reqwest::Client::builder()
        .use_preconfigured_tls(config)
        .timeout(Duration::from_secs(10))
        .build()?;
    client
        .get(format!("{base}/v1/hello"))
        .send()
        .await
        .with_context(|| format!("connecting to {base}"))?;
    let fingerprint = verifier.fingerprint.lock().unwrap().clone();
    fingerprint.context("the hub sent no certificate")
}

#[derive(Debug)]
struct FirstUse {
    fingerprint: Mutex<Option<String>>,
    provider: Arc<CryptoProvider>,
}

impl Default for FirstUse {
    fn default() -> Self {
        Self {
            fingerprint: Mutex::new(None),
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }
}

impl ServerCertVerifier for FirstUse {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fingerprint = certificate_fingerprint(end_entity).map_err(|_| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;
        *self.fingerprint.lock().unwrap() = Some(fingerprint);
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
