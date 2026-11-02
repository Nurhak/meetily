//! Live transcription (sync-protocol.md, "Live transcription"): a phone streams audio over a WebSocket while it
//! records, and gets the computer's transcript back as people talk.
//!
//! The hub handles the protocol (messages, audio format, timeline, resuming, limits); the desktop app plugs in the
//! speech recognition through [`LiveTranscriber`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::future::BoxFuture;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::hub::Hub;

/// WebSocket subprotocol of version 1.
pub const SUBPROTOCOL: &str = "salescopilot-live.1";
/// Sample rates the hub accepts; everything is converted to 16 kHz for the engine.
pub const SAMPLE_RATES: &[u32] = &[8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000];
/// The rate engines get.
pub const ENGINE_RATE: u32 = 16_000;
/// More audio than this waiting to be transcribed ends the session (`too_far_behind`).
pub const MAX_BACKLOG: Duration = Duration::from_secs(60);
/// Largest binary frame (audio) and text frame (control message).
pub const MAX_AUDIO_FRAME: usize = 64 * 1024;
pub const MAX_TEXT_FRAME: usize = 4 * 1024;

const START_TIMEOUT: Duration = Duration::from_secs(15);
/// Meeting times beyond this (about three years) are refused, so segment ids stay within 12 digits.
const MAX_START_SEC: f64 = 1e8;
/// After `stop`, the hub has this long to transcribe what is left (a slow engine with a full backlog).
const FINISH_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SEGMENT_TEXT: usize = 20_000;
const MAX_ERROR_MESSAGE: usize = 1_000;
/// Nothing at all from the phone (audio, messages or pings) for this long closes the session.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const PROGRESS_EVERY: Duration = Duration::from_secs(3);
/// The engine gets at most this much audio per call, so segments keep coming while the phone talks.
const MAX_FEED: usize = 30 * ENGINE_RATE as usize;

/// What the desktop app needs to start a live session.
#[derive(Debug, Clone)]
pub struct LiveStart {
    pub meeting_id: String,
    pub device_id: String,
    /// The meeting language, or `None` to detect it.
    pub language: Option<String>,
}

/// What transcribes, as reported to the phone in `ready`.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveReady {
    /// Same names as `producedBy.engine` in transcript.json (`parakeet`, `whisper`, …).
    pub engine: String,
    pub model: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiveError {
    /// The computer can't transcribe live now (it is recording itself, has no model ready, …).
    Unavailable { retry_after_secs: u64, reason: String },
    Failed(String),
}

impl std::fmt::Display for LiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable { reason, .. } => write!(f, "{reason}"),
            Self::Failed(reason) => write!(f, "{reason}"),
        }
    }
}

/// Transcribed speech. Times are seconds since the first sample of the session.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveText {
    pub start_sec: f64,
    pub end_sec: f64,
    pub text: String,
    /// `false`: a partial result, replaced by a later one with the same start.
    pub is_final: bool,
}

/// What [`LiveTranscriber::open`] returns.
pub type LiveOpening = BoxFuture<'static, Result<(LiveReady, Box<dyn LiveSession>), LiveError>>;

/// Speech recognition for live sessions, provided by the desktop app.
pub trait LiveTranscriber: Send + Sync + 'static {
    /// A quick check before the WebSocket is accepted; `Unavailable` becomes `503 transcriber_unavailable`.
    fn check(&self) -> Result<(), LiveError>;
    /// Starts a session.
    fn open(&self, start: LiveStart) -> LiveOpening;
}

/// One phone's audio stream.
pub trait LiveSession: Send + 'static {
    /// 16 kHz mono samples following the previous ones. Returns the speech finished so far.
    fn feed(&mut self, samples: Vec<f32>) -> BoxFuture<'_, Result<Vec<LiveText>, LiveError>>;
    /// No more audio: transcribe what is left.
    fn finish(&mut self) -> BoxFuture<'_, Result<Vec<LiveText>, LiveError>>;
}

/// Why the hub ends a session from outside it: an error code from the spec.
type EndReason = Option<&'static str>;

/// Open sessions by device, so a new one replaces the old (`session_replaced`) and removing a device ends its
/// session (`device_revoked`).
#[derive(Default)]
pub struct Sessions {
    next: AtomicU64,
    by_device: StdMutex<HashMap<String, (u64, watch::Sender<EndReason>)>>,
}

impl Sessions {
    fn register(&self, device_id: &str) -> (u64, watch::Receiver<EndReason>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(None);
        if let Some((_, old)) = self.by_device.lock().unwrap().insert(device_id.to_string(), (id, tx)) {
            let _ = old.send(Some("session_replaced"));
        }
        (id, rx)
    }

    /// Ends the device's session, if it has one (the device was removed, replaced or unpaired).
    pub fn end_device(&self, device_id: &str) {
        if let Some((_, tx)) = self.by_device.lock().unwrap().remove(device_id) {
            let _ = tx.send(Some("device_revoked"));
        }
    }

    fn unregister(&self, device_id: &str, id: u64) {
        let mut map = self.by_device.lock().unwrap();
        if map.get(device_id).is_some_and(|(current, _)| *current == id) {
            map.remove(device_id);
        }
    }

    /// Number of open sessions.
    pub fn count(&self) -> usize {
        self.by_device.lock().unwrap().len()
    }
}

/// `hello` additions when live transcription is available.
pub fn hello_fields() -> Value {
    json!({
        "features": ["live-transcription"],
        "liveAudio": { "formats": ["pcm_s16le"], "sampleRates": SAMPLE_RATES },
    })
}

/// Converts any accepted rate to 16 kHz by linear interpolation, keeping state between frames.
pub struct Resampler {
    step: f64,
    /// Position of the next output sample, in input samples, relative to `last` (index 0).
    pos: f64,
    last: Option<f32>,
}

impl Resampler {
    pub fn new(input_rate: u32) -> Self {
        Self { step: input_rate as f64 / ENGINE_RATE as f64, pos: 0.0, last: None }
    }

    pub fn process(&mut self, input: &[f32]) -> Vec<f32> {
        if (self.step - 1.0).abs() < f64::EPSILON {
            return input.to_vec();
        }
        if input.is_empty() {
            return Vec::new();
        }
        let mut buf = Vec::with_capacity(input.len() + 1);
        if let Some(last) = self.last {
            buf.push(last);
        }
        buf.extend_from_slice(input);
        let mut out = Vec::with_capacity((input.len() as f64 / self.step) as usize + 2);
        let mut t = self.pos;
        // Interpolate between buf[i] and buf[i + 1]; a position exactly on the last sample waits for the next frame.
        while t < (buf.len() - 1) as f64 {
            let i = t as usize;
            let f = (t - i as f64) as f32;
            out.push(buf[i] * (1.0 - f) + buf[i + 1] * f);
            t += self.step;
        }
        self.pos = t - (buf.len() - 1) as f64;
        self.last = buf.last().copied();
        out
    }
}

impl Resampler {
    /// The samples still owed when the stream ends: the last input sample is held back until the next frame
    /// arrives, so without this a stream would come out a few samples short.
    pub fn flush(&mut self) -> Vec<f32> {
        let mut out = Vec::new();
        if let Some(last) = self.last {
            while self.pos < 1.0 - 1e-9 {
                out.push(last);
                self.pos += self.step;
            }
        }
        out
    }
}

/// Little-endian signed 16-bit PCM to floats in [-1, 1].
pub fn pcm_s16le_to_f32(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]) as f32 / 32768.0)
        .collect()
}

/// `live-` and the start in milliseconds on the meeting timeline.
pub fn segment_id(start_sec: f64) -> String {
    format!("live-{}", (start_sec * 1000.0).round().max(0.0) as u64)
}

fn round_ms(sec: f64) -> f64 {
    (sec * 1000.0).round() / 1000.0
}

struct Start {
    rate: u32,
    start_sec: f64,
    language: Option<String>,
}

enum Problem {
    Close { code: &'static str, message: String, close: u16 },
    /// The phone went away; nothing to send.
    Gone,
}

fn problem(code: &'static str, message: impl Into<String>) -> Problem {
    let close = match code {
        "transcriber_unavailable" | "too_far_behind" => 1013,
        "session_replaced" | "device_revoked" => 1000,
        _ => 1008,
    };
    Problem::Close { code, message: message.into(), close }
}

fn parse_start(text: &str) -> Result<Start, Problem> {
    let v: Value = serde_json::from_str(text).map_err(|e| problem("bad_request", format!("invalid JSON: {e}")))?;
    if v["type"] != "start" {
        return Err(problem("bad_request", "the first message must be start"));
    }
    let audio = &v["audio"];
    if audio["format"] != "pcm_s16le" || audio["channels"] != 1 {
        return Err(problem("unsupported_audio_format", "only mono pcm_s16le"));
    }
    let rate = audio["sampleRate"]
        .as_u64()
        .and_then(|r| u32::try_from(r).ok())
        .filter(|r| SAMPLE_RATES.contains(r))
        .ok_or_else(|| problem("unsupported_audio_format", format!("sample rates: {SAMPLE_RATES:?}")))?;
    let start_sec = v["startSec"]
        .as_f64()
        .filter(|s| s.is_finite() && (0.0..=MAX_START_SEC).contains(s))
        .ok_or_else(|| problem("bad_request", "startSec must be a number of seconds"))?;
    let language = match &v["languageCode"] {
        Value::Null => None,
        Value::String(code) if crate::docs::is_language_code(code) => Some(code.clone()),
        _ => return Err(problem("bad_request", "languageCode must be a BCP 47 tag or null")),
    };
    Ok(Start { rate, start_sec, language })
}

/// Runs one session on an accepted WebSocket.
pub async fn run(hub: Hub, transcriber: Arc<dyn LiveTranscriber>, device_id: String, meeting_id: String, mut socket: WebSocket) {
    let (session_no, mut ended) = hub.live_sessions().register(&device_id);
    let result = session(&hub, transcriber, &device_id, &meeting_id, &mut socket, &mut ended).await;
    hub.live_sessions().unregister(&device_id, session_no);
    match result {
        Ok(()) => {
            let _ = socket.send(Message::Close(Some(CloseFrame { code: 1000, reason: Utf8Bytes::from_static("") }))).await;
        }
        Err(Problem::Close { code, message, close }) => {
            tracing::info!(target: "sync_hub", "live session for {meeting_id} ended: {code} {message}");
            let message: String = message.chars().take(MAX_ERROR_MESSAGE).collect();
            let _ = send_json(&mut socket, json!({ "type": "error", "code": code, "message": message })).await;
            let _ = socket.send(Message::Close(Some(CloseFrame { code: close, reason: Utf8Bytes::from(code) }))).await;
        }
        Err(Problem::Gone) => {}
    }
}

async fn send_json(socket: &mut WebSocket, value: Value) -> Result<(), Problem> {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .map_err(|_| Problem::Gone)
}

/// What the engine worker reports.
enum Worked {
    Texts(Vec<LiveText>, f64),
    Finished(Vec<LiveText>, f64),
    Failed(LiveError),
}

async fn session(
    hub: &Hub,
    transcriber: Arc<dyn LiveTranscriber>,
    device_id: &str,
    meeting_id: &str,
    socket: &mut WebSocket,
    ended: &mut watch::Receiver<EndReason>,
) -> Result<(), Problem> {
    // The start message.
    let start = loop {
        let msg = tokio::time::timeout(START_TIMEOUT, socket.recv())
            .await
            .map_err(|_| problem("bad_request", "no start message"))?;
        match msg {
            Some(Ok(Message::Text(text))) => {
                if text.len() > MAX_TEXT_FRAME {
                    return Err(problem("bad_request", "message too large"));
                }
                break parse_start(&text)?;
            }
            Some(Ok(Message::Binary(_))) => return Err(problem("bad_request", "audio before start")),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return Err(Problem::Gone),
        }
    };

    let (ready, mut engine) = transcriber
        .open(LiveStart { meeting_id: meeting_id.to_string(), device_id: device_id.to_string(), language: start.language.clone() })
        .await
        .map_err(|e| problem("transcriber_unavailable", e.to_string()))?;
    send_json(
        socket,
        json!({ "type": "ready", "engine": ready.engine, "model": ready.model, "languageCode": ready.language }),
    )
    .await?;

    // The engine runs in its own task so audio keeps arriving while it works.
    let (audio_tx, mut audio_rx) = mpsc::unbounded_channel::<Option<Vec<f32>>>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Worked>();
    let pending = Arc::new(AtomicU64::new(0));
    let worker_pending = pending.clone();
    let worker = tokio::spawn(async move {
        let mut fed: u64 = 0;
        while let Some(item) = audio_rx.recv().await {
            let Some(mut samples) = item else {
                let result = engine.finish().await;
                let _ = out_tx.send(match result {
                    Ok(texts) => Worked::Finished(texts, fed as f64 / ENGINE_RATE as f64),
                    Err(e) => Worked::Failed(e),
                });
                return;
            };
            // Take what else is waiting, up to MAX_FEED, so the engine sees larger pieces when it is behind.
            let mut finishing = false;
            while samples.len() < MAX_FEED {
                match audio_rx.try_recv() {
                    Ok(Some(more)) => samples.extend(more),
                    Ok(None) => {
                        finishing = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            let n = samples.len() as u64;
            let result = engine.feed(samples).await;
            worker_pending.fetch_sub(n, Ordering::Relaxed);
            fed += n;
            match result {
                Ok(texts) => {
                    let _ = out_tx.send(Worked::Texts(texts, fed as f64 / ENGINE_RATE as f64));
                }
                Err(e) => {
                    let _ = out_tx.send(Worked::Failed(e));
                    return;
                }
            }
            if finishing {
                let result = engine.finish().await;
                let _ = out_tx.send(match result {
                    Ok(texts) => Worked::Finished(texts, fed as f64 / ENGINE_RATE as f64),
                    Err(e) => Worked::Failed(e),
                });
                return;
            }
        }
    });
    let _abort_worker = AbortOnDrop(worker);

    let mut resampler = Resampler::new(start.rate);
    let max_pending = MAX_BACKLOG.as_secs() * ENGINE_RATE as u64;
    let mut stopping = false;
    let mut stopped_at: Option<Instant> = None;
    let mut last_input = Instant::now();
    let mut processed = start.start_sec;
    let mut reported = -1.0;
    let mut progress = tokio::time::interval(PROGRESS_EVERY);
    progress.tick().await;
    let mut odd_byte: Option<u8> = None;

    loop {
        tokio::select! {
            changed = ended.changed() => {
                let reason = *ended.borrow();
                match (changed.is_ok(), reason) {
                    (true, Some("session_replaced")) => return Err(problem("session_replaced", "a new session for this device started")),
                    (true, Some(code)) => return Err(problem(code, "this device was removed on the computer")),
                    _ => {}
                }
            }
            worked = out_rx.recv() => {
                let (texts, done) = match worked {
                    Some(Worked::Texts(texts, fed)) => { processed = start.start_sec + fed; (texts, false) }
                    Some(Worked::Finished(texts, fed)) => { processed = start.start_sec + fed; (texts, true) }
                    Some(Worked::Failed(e)) => return Err(problem("transcriber_unavailable", e.to_string())),
                    None => return Err(problem("transcriber_unavailable", "the transcriber stopped")),
                };
                for t in texts {
                    let text: String = t.text.trim().chars().take(MAX_SEGMENT_TEXT).collect();
                    let text = text.as_str();
                    if text.is_empty() {
                        continue;
                    }
                    let start_sec = round_ms(start.start_sec + t.start_sec.max(0.0));
                    let end_sec = round_ms((start.start_sec + t.end_sec).max(start_sec));
                    send_json(socket, json!({
                        "type": "segment",
                        "id": segment_id(start_sec),
                        "startSec": start_sec,
                        "endSec": end_sec,
                        "text": text,
                        "final": t.is_final,
                    })).await?;
                }
                if done {
                    send_json(socket, json!({ "type": "progress", "processedSec": round_ms(processed) })).await?;
                    send_json(socket, json!({ "type": "done" })).await?;
                    return Ok(());
                }
            }
            _ = progress.tick() => {
                // While finishing the phone sends nothing more; it has its own, longer deadline.
                match stopped_at {
                    Some(at) if at.elapsed() > FINISH_TIMEOUT => {
                        return Err(problem("transcriber_unavailable", "transcribing the rest took too long"));
                    }
                    None if last_input.elapsed() > IDLE_TIMEOUT => return Err(Problem::Gone),
                    _ => {}
                }
                if hub.meeting_deleted(meeting_id).await {
                    return Err(problem("meeting_deleted", "the meeting was deleted"));
                }
                if processed > reported {
                    reported = processed;
                    send_json(socket, json!({ "type": "progress", "processedSec": round_ms(processed) })).await?;
                }
            }
            msg = socket.recv(), if !stopping => {
                last_input = Instant::now();
                match msg {
                    Some(Ok(Message::Binary(bytes))) => {
                        if bytes.len() > MAX_AUDIO_FRAME {
                            return Err(problem("bad_request", "audio frames are at most 64 KiB"));
                        }
                        // Keep an odd trailing byte for the next frame, so samples never split.
                        let mut data = Vec::with_capacity(bytes.len() + 1);
                        if let Some(b) = odd_byte.take() {
                            data.push(b);
                        }
                        data.extend_from_slice(&bytes);
                        if data.len() % 2 == 1 {
                            odd_byte = data.pop();
                        }
                        let samples = resampler.process(&pcm_s16le_to_f32(&data));
                        if samples.is_empty() {
                            continue;
                        }
                        let n = samples.len() as u64;
                        if pending.fetch_add(n, Ordering::Relaxed) + n > max_pending {
                            return Err(problem("too_far_behind", "more than 60 seconds of audio are waiting"));
                        }
                        if audio_tx.send(Some(samples)).is_err() {
                            return Err(problem("transcriber_unavailable", "the transcriber stopped"));
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > MAX_TEXT_FRAME {
                            return Err(problem("bad_request", "message too large"));
                        }
                        let v: Value = serde_json::from_str(&text)
                            .map_err(|e| problem("bad_request", format!("invalid JSON: {e}")))?;
                        match v["type"].as_str() {
                            Some("stop") => {
                                stopping = true;
                                stopped_at = Some(Instant::now());
                                let tail = resampler.flush();
                                if !tail.is_empty() {
                                    pending.fetch_add(tail.len() as u64, Ordering::Relaxed);
                                    let _ = audio_tx.send(Some(tail));
                                }
                                let _ = audio_tx.send(None);
                            }
                            Some("start") => return Err(problem("bad_request", "the session already started")),
                            // Unknown message types are skipped (newer phones).
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return Err(Problem::Gone),
                }
            }
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resampling_keeps_the_timeline() {
        for rate in SAMPLE_RATES {
            let mut r = Resampler::new(*rate);
            let second: Vec<f32> = (0..*rate).map(|i| i as f32 / *rate as f32).collect();
            // In uneven pieces, as frames arrive.
            let mut out = Vec::new();
            for piece in second.chunks(997) {
                out.extend(r.process(piece));
            }
            out.extend(r.flush());
            assert_eq!(out.len(), ENGINE_RATE as usize, "{rate}");
            // A ramp stays a ramp: sample k at 16 kHz is at time k / 16000.
            for (k, s) in out.iter().enumerate().step_by(1000) {
                assert!((s - k as f32 / ENGINE_RATE as f32).abs() < 1e-3, "{rate}: sample {k} is {s}");
            }
        }
    }

    #[test]
    fn pcm_and_ids() {
        assert_eq!(pcm_s16le_to_f32(&[0x00, 0x80, 0xff, 0x7f, 0, 0]), vec![-1.0, 32767.0 / 32768.0, 0.0]);
        assert_eq!(segment_id(3.9), "live-3900");
        assert_eq!(segment_id(0.0), "live-0");
    }

    #[test]
    fn start_messages() {
        let ok = parse_start(r#"{"type":"start","audio":{"format":"pcm_s16le","sampleRate":8000,"channels":1},"startSec":3.1,"languageCode":"nl"}"#).ok().unwrap();
        assert_eq!((ok.rate, ok.start_sec, ok.language.as_deref()), (8000, 3.1, Some("nl")));
        let code = |text: &str| match parse_start(text) {
            Err(Problem::Close { code, .. }) => code,
            _ => "ok",
        };
        assert_eq!(code(r#"{"type":"start","audio":{"format":"opus","sampleRate":16000,"channels":1},"startSec":0}"#), "unsupported_audio_format");
        assert_eq!(code(r#"{"type":"start","audio":{"format":"pcm_s16le","sampleRate":12345,"channels":1},"startSec":0}"#), "unsupported_audio_format");
        assert_eq!(code(r#"{"type":"start","audio":{"format":"pcm_s16le","sampleRate":16000,"channels":2},"startSec":0}"#), "unsupported_audio_format");
        assert_eq!(code(r#"{"type":"start","audio":{"format":"pcm_s16le","sampleRate":16000,"channels":1},"startSec":-1}"#), "bad_request");
        assert_eq!(code(r#"{"type":"start","audio":{"format":"pcm_s16le","sampleRate":16000,"channels":1},"startSec":0,"languageCode":"Dutch"}"#), "bad_request");
        assert_eq!(code(r#"{"type":"stop"}"#), "bad_request");
        assert_eq!(code(r#"{"type":"start","audio":{"format":"pcm_s16le","sampleRate":16000,"channels":1},"startSec":0,"languageCode":null}"#), "ok");
    }
}
