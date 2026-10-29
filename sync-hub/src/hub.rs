//! The hub: every rule of the sync protocol (spec/sync-protocol.md) lives here; `api.rs` only maps HTTP to it.
//! The desktop app uses the same type for its own side: pairing UI, local meetings, transcription hand-off.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::Duration;

use anyhow::Context;
use axum::http::StatusCode;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use crate::docs::{self, MEETING_HUB_OWNED, TRANSCRIPT_HUB_OWNED};
use crate::error::{ApiError, ApiResult};
use crate::identity::HubIdentity;
use crate::netfilter::NetFilter;
use crate::pairing::{Attempt, PairingInfo, PairingSession};
use crate::probe::{AudioDetails, AudioProber, BasicProber};
use crate::schema::Schemas;
use crate::store::{default_hub_state, DeviceRecord, MeetingRow, Store, TranscriptRow, UploadRow};

pub const DEFAULT_PORT: u16 = 47821;
pub const PROTOCOL_VERSION: u32 = 1;
pub const FORMAT_VERSION: u32 = 1;
pub const MAX_AUDIO_BYTES: i64 = 4 * 1024 * 1024 * 1024;
pub const MAX_UPLOAD_CHUNK: usize = 16 * 1024 * 1024;
const UPLOAD_TTL: chrono::Duration = chrono::Duration::hours(24);
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
pub struct HubConfig {
    /// Where the hub keeps its key, database and audio.
    pub data_dir: PathBuf,
    /// Shown to users and advertised, e.g. "Nurhak's MacBook".
    pub name: String,
    /// `macos` or `windows`.
    pub platform: String,
    pub app_version: String,
    /// Addresses to listen on. The first one's port falls back to a free port when taken; the others reuse it.
    pub listen: Vec<SocketAddr>,
    /// Advertise through DNS-SD.
    pub advertise: bool,
    /// How long deleted meetings stay restorable (Recently deleted).
    pub trash_retention: Duration,
    /// How long tombstones stay in the change feed.
    pub tombstone_retention: Duration,
    /// Connections idle for this long are closed (longer than a 30-second long poll).
    pub idle_timeout: Duration,
    /// TLS handshake and request-header deadline.
    pub handshake_timeout: Duration,
    pub max_connections: usize,
    pub max_connections_per_address: usize,
    /// Unfinished uploads one device may have at a time.
    pub max_unfinished_uploads_per_device: usize,
    /// Free disk space the hub keeps after accepting an upload.
    pub min_free_space: u64,
}

impl HubConfig {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        name: impl Into<String>,
        platform: impl Into<String>,
        app_version: impl Into<String>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            name: name.into(),
            platform: platform.into(),
            app_version: app_version.into(),
            listen: vec![
                SocketAddr::from(([0, 0, 0, 0], DEFAULT_PORT)),
                SocketAddr::from(([0u16; 8], DEFAULT_PORT)),
            ],
            advertise: true,
            trash_retention: Duration::from_secs(30 * 24 * 3600),
            tombstone_retention: Duration::from_secs(90 * 24 * 3600),
            idle_timeout: Duration::from_secs(90),
            handshake_timeout: Duration::from_secs(10),
            max_connections: 128,
            max_connections_per_address: 16,
            max_unfinished_uploads_per_device: 4,
            min_free_space: 1024 * 1024 * 1024,
        }
    }
}

/// What the desktop app hears about.
#[derive(Debug, Clone, PartialEq)]
pub enum HubEvent {
    DevicePaired {
        device_id: String,
    },
    /// meeting.json changed, or the meeting was deleted or restored.
    MeetingChanged {
        meeting_id: String,
    },
    TranscriptChanged {
        meeting_id: String,
    },
    AudioReceived {
        meeting_id: String,
        path: PathBuf,
    },
    /// The hub wants a final transcript: call [`Hub::begin_transcription`], then [`Hub::finish_transcription`].
    TranscriptionWanted {
        meeting_id: String,
    },
}

/// Who is asking: the desktop app itself, or a paired device (identified by its token).
#[derive(Debug, Clone)]
pub enum Caller {
    Hub,
    Device(DeviceRecord),
}

impl Caller {
    fn device_id(&self, hub_id: &str) -> String {
        match self {
            Caller::Hub => hub_id.to_string(),
            Caller::Device(d) => d.device_id.clone(),
        }
    }

    /// The device itself and the devices it replaced.
    fn identities(&self) -> Vec<String> {
        match self {
            Caller::Hub => Vec::new(),
            Caller::Device(d) => std::iter::once(d.device_id.clone())
                .chain(d.replaces.iter().cloned())
                .collect(),
        }
    }

    fn sees(&self, recorded_by: &str) -> bool {
        match self {
            Caller::Hub => true,
            Caller::Device(d) => {
                d.scope != "own" || self.identities().iter().any(|i| i == recorded_by)
            }
        }
    }

    /// The desktop app manages every meeting it holds; a device owns what it (or a device it replaced) recorded.
    fn owns(&self, recorded_by: &str) -> bool {
        match self {
            Caller::Hub => true,
            Caller::Device(_) => self.identities().iter().any(|i| i == recorded_by),
        }
    }

    fn scope_filter(&self) -> Option<Vec<String>> {
        match self {
            Caller::Device(d) if d.scope == "own" => Some(self.identities()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Precondition {
    Create,
    Update(String),
}

impl Precondition {
    /// Exactly one of `If-None-Match: *` and `If-Match` (sync-protocol.md, "Preconditions").
    pub fn from_headers(if_match: Option<&str>, if_none_match: Option<&str>) -> ApiResult<Self> {
        match (if_match, if_none_match) {
            (Some(_), Some(_)) => Err(ApiError::bad_request(
                "send only one of If-Match and If-None-Match",
            )),
            (Some(etag), None) => Ok(Precondition::Update(etag.trim().to_string())),
            (None, Some(v)) if v.trim() == "*" => Ok(Precondition::Create),
            (None, Some(_)) => Err(ApiError::bad_request("If-None-Match must be *")),
            (None, None) => Err(ApiError::precondition_required()),
        }
    }
}

/// Result of a PUT.
#[derive(Debug)]
pub enum Write {
    Created {
        doc: Value,
        etag: String,
    },
    /// Also returned, with the unchanged document, for a PUT that changed nothing.
    Updated {
        doc: Value,
        etag: String,
    },
    /// `412`: the current document, to merge with.
    Conflict {
        doc: Value,
        etag: String,
    },
}

pub struct UploadProgress {
    pub offset: i64,
    pub complete: bool,
}

pub struct AudioFile {
    pub path: PathBuf,
    pub sha256: String,
    pub mime_type: String,
    pub size: u64,
}

/// Handed out by [`Hub::begin_transcription`] and [`Hub::begin_retranscription`].
#[derive(Debug, Clone)]
pub struct TranscriptionTicket {
    pub meeting_id: String,
    /// Path of the audio to transcribe.
    pub audio_path: PathBuf,
    pub language_code: Option<String>,
    /// Transcript revision when transcription began; any change after it means the user edited meanwhile.
    based_on_revision: Option<i64>,
}

#[derive(Debug, PartialEq)]
pub enum FinishOutcome {
    Saved,
    /// The transcript changed while transcribing, so the result was not saved (the user's version stays).
    Skipped,
    /// The meeting was deleted meanwhile; nothing was saved.
    MeetingDeleted,
}

pub fn meeting_etag(revision: i64) -> String {
    format!("\"m-{revision}\"")
}

pub fn transcript_etag(revision: i64) -> String {
    format!("\"t-{revision}\"")
}

pub fn audio_etag(sha256: &str) -> String {
    format!("\"{sha256}\"")
}

pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn random_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.char_indices().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == '-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
}

/// Upload IDs as the hub makes them; anything else is not looked up at all.
pub fn is_upload_id(s: &str) -> bool {
    (8..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn ago(d: Duration) -> String {
    let d = chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX);
    (chrono::Utc::now() - d).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn mime_for_extension(file_name: &str) -> Option<&'static str> {
    match file_name.rsplit('.').next()? {
        "m4a" | "mp4" => Some("audio/mp4"),
        "wav" => Some("audio/wav"),
        "ogg" | "opus" => Some("audio/ogg"),
        "flac" => Some("audio/flac"),
        "mp3" => Some("audio/mpeg"),
        _ => None,
    }
}

const FILE_NAMES: &[&str] = &[
    "audio.m4a",
    "audio.mp4",
    "audio.wav",
    "audio.ogg",
    "audio.opus",
    "audio.flac",
    "audio.mp3",
];

fn transcription_state(row: &MeetingRow) -> &str {
    row.hub_state
        .get("transcription")
        .and_then(Value::as_str)
        .unwrap_or("none")
}

/// Rejects transcripts other apps couldn't merge: duplicate segment or speaker IDs. Drops dangling speaker
/// references and sorts segments (the order the format prescribes).
fn tidy_transcript(doc: &mut Value) -> ApiResult<()> {
    let mut speaker_ids = std::collections::HashSet::new();
    for s in doc
        .get("speakers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if !speaker_ids.insert(
            s.get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        ) {
            return Err(ApiError::bad_request("speaker ids must be unique"));
        }
    }
    let Some(segments) = doc.get_mut("segments").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let mut segment_ids = std::collections::HashSet::new();
    for seg in segments.iter_mut() {
        if !segment_ids.insert(
            seg.get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        ) {
            return Err(ApiError::bad_request("segment ids must be unique"));
        }
        let dangling = seg
            .get("speakerId")
            .and_then(Value::as_str)
            .is_some_and(|id| !speaker_ids.contains(id));
        if dangling {
            seg["speakerId"] = Value::Null;
        }
    }
    let key = |s: &Value| {
        (
            s.get("startSec").and_then(Value::as_f64).unwrap_or(0.0),
            s.get("endSec").and_then(Value::as_f64).unwrap_or(0.0),
            s.get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        )
    };
    segments.sort_by(|a, b| {
        key(a)
            .partial_cmp(&key(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(())
}

#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

struct Inner {
    config: HubConfig,
    identity: HubIdentity,
    store: Store,
    schemas: Arc<Schemas>,
    pairing: StdMutex<Option<PairingSession>>,
    events: mpsc::UnboundedSender<HubEvent>,
    seq: watch::Sender<i64>,
    netfilter: NetFilter,
    /// One lock per upload in progress; entries exist only for uploads in the database.
    upload_locks: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    prober: Arc<dyn AudioProber>,
    listening: RwLock<Vec<SocketAddr>>,
}

impl Hub {
    /// Opens (or creates) the hub's data. Events arrive on the returned receiver.
    pub async fn open(
        config: HubConfig,
    ) -> anyhow::Result<(Hub, mpsc::UnboundedReceiver<HubEvent>)> {
        Self::open_with_prober(config, Arc::new(BasicProber)).await
    }

    pub async fn open_with_prober(
        config: HubConfig,
        prober: Arc<dyn AudioProber>,
    ) -> anyhow::Result<(Hub, mpsc::UnboundedReceiver<HubEvent>)> {
        std::fs::create_dir_all(&config.data_dir)?;
        let identity = HubIdentity::load_or_create(&config.data_dir.join("identity"))?;
        let store = Store::open(&config.data_dir)
            .await
            .context("opening hub database")?;
        let current = store.current_seq().await?;
        let (events, rx) = mpsc::unbounded_channel();
        let hub = Hub {
            inner: Arc::new(Inner {
                config,
                identity,
                store,
                schemas: Arc::new(Schemas::load()?),
                pairing: StdMutex::new(None),
                events,
                seq: watch::Sender::new(current),
                netfilter: NetFilter::new(),
                upload_locks: StdMutex::new(HashMap::new()),
                prober,
                listening: RwLock::new(Vec::new()),
            }),
        };
        hub.recover_transcriptions().await?;
        Ok((hub, rx))
    }

    /// Transcription that was queued or running when the app quit starts again.
    async fn recover_transcriptions(&self) -> anyhow::Result<()> {
        let _g = self.store().lock().await;
        for row in self
            .store()
            .meetings_in_transcription_state(&["queued", "running"])
            .await?
        {
            let mut state = row.hub_state.clone();
            state["transcription"] = json!("none");
            state["transcriptionProgress"] = json!(0);
            self.store().set_hub_state(&row.id, &state, false).await?;
            self.maybe_queue_transcription(&row.id).await?;
        }
        Ok(())
    }

    pub fn hub_id(&self) -> &str {
        &self.inner.identity.hub_id
    }

    pub fn fingerprint(&self) -> &str {
        &self.inner.identity.fingerprint
    }

    pub fn config(&self) -> &HubConfig {
        &self.inner.config
    }

    pub(crate) fn identity(&self) -> &HubIdentity {
        &self.inner.identity
    }

    pub(crate) fn netfilter(&self) -> &NetFilter {
        &self.inner.netfilter
    }

    pub(crate) fn set_listening(&self, addrs: Vec<SocketAddr>) {
        *self.inner.listening.write().unwrap() = addrs;
    }

    fn store(&self) -> &Store {
        &self.inner.store
    }

    fn emit(&self, e: HubEvent) {
        let _ = self.inner.events.send(e);
    }

    fn notify(&self, seq: Option<i64>) {
        let Some(seq) = seq else { return };
        self.inner.seq.send_if_modified(|current| {
            if seq > *current {
                *current = seq;
                true
            } else {
                false
            }
        });
    }

    async fn check_schema(&self, doc: Value, transcript: bool) -> ApiResult<Value> {
        let schemas = self.inner.schemas.clone();
        tokio::task::spawn_blocking(move || {
            let result = if transcript {
                schemas.check_transcript(&doc)
            } else {
                schemas.check_meeting(&doc)
            };
            result.map(|_| doc)
        })
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::bad_request)
    }

    // MARK: hello and pairing

    pub fn hello(&self) -> Value {
        json!({
            "hubId": self.hub_id(),
            "name": self.inner.config.name,
            "platform": self.inner.config.platform,
            "appVersion": self.inner.config.app_version,
            "protocolVersions": [PROTOCOL_VERSION],
            "formatVersions": [FORMAT_VERSION],
        })
    }

    /// Addresses phones can use: the interfaces' addresses with the listening port.
    pub fn reachable_addresses(&self) -> Vec<SocketAddr> {
        let listening = self.inner.listening.read().unwrap().clone();
        let Some(port) = listening.first().map(|a| a.port()) else {
            return Vec::new();
        };
        let mut out: Vec<SocketAddr> = Vec::new();
        for l in &listening {
            if l.ip().is_unspecified() {
                for net in crate::netfilter::local_nets() {
                    let same_family = net.addr.is_ipv4() == l.ip().is_ipv4();
                    if same_family && !net.addr.is_loopback() {
                        out.push(SocketAddr::new(net.addr, port));
                    }
                }
            } else {
                out.push(*l);
            }
        }
        out.sort_by_key(|a| (!a.is_ipv4(), a.to_string()));
        out.dedup();
        out
    }

    /// Starts showing a pairing code (replacing any earlier one).
    pub fn start_pairing(&self) -> PairingInfo {
        let session = PairingSession::start();
        let info = session.info(
            self.hub_id(),
            &self.inner.config.name,
            self.fingerprint(),
            &self.reachable_addresses(),
        );
        *self.inner.pairing.lock().unwrap() = Some(session);
        info
    }

    pub fn cancel_pairing(&self) {
        *self.inner.pairing.lock().unwrap() = None;
    }

    /// `POST /v1/pair`. `presented_token`: the device's current token, required to pair a deviceId that is
    /// already paired (otherwise anyone who completes a pairing could take over another phone's identity).
    pub async fn pair(
        &self,
        peer: IpAddr,
        body: &Value,
        presented_token: Option<&str>,
    ) -> ApiResult<Value> {
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("code is required"))?;
        let device = body
            .get("device")
            .ok_or_else(|| ApiError::bad_request("device is required"))?;
        let device_id = device
            .get("deviceId")
            .and_then(Value::as_str)
            .map(docs::lowercase_uuid)
            .filter(|d| is_uuid(d))
            .ok_or_else(|| ApiError::bad_request("device.deviceId must be a UUID"))?;
        let platform = device
            .get("platform")
            .and_then(Value::as_str)
            .filter(|p| !p.is_empty() && p.len() <= 32)
            .ok_or_else(|| ApiError::bad_request("device.platform is required"))?;
        let name = device
            .get("deviceName")
            .and_then(Value::as_str)
            .map(|s| s.chars().take(200).collect::<String>());
        let app_version = device
            .get("appVersion")
            .and_then(Value::as_str)
            .map(|s| s.chars().take(64).collect::<String>());
        let scope = match body.get("scope").and_then(Value::as_str) {
            None => None,
            Some(s @ ("all" | "own")) => Some(s),
            Some(_) => return Err(ApiError::bad_request("scope must be all or own")),
        };
        if device_id == self.hub_id() {
            return Err(ApiError::bad_request("device.deviceId is the hub's own ID"));
        }
        {
            let mut guard = self.inner.pairing.lock().unwrap();
            let Some(session) = guard.as_mut() else {
                return Err(ApiError::pairing_code_invalid());
            };
            match session.attempt(peer, code) {
                Attempt::Accepted => {}
                Attempt::AddressBlocked => {
                    return Err(ApiError::rate_limited(session.remaining().as_secs().max(1)))
                }
                Attempt::Invalid => {
                    if session.is_expired() {
                        *guard = None;
                    }
                    return Err(ApiError::pairing_code_invalid());
                }
            }
        }
        let _g = self.store().lock().await;
        if let Some(current) = self.store().device_token_hash(&device_id).await? {
            if presented_token.map(token_hash).as_deref() != Some(current.as_str()) {
                // The code stays valid: the right phone can still pair.
                return Err(ApiError::forbidden(
                    "this device is already paired: send its current token, or remove it on the computer first",
                ));
            }
        }
        // The code is used up only now.
        *self.inner.pairing.lock().unwrap() = None;
        let token = random_token(32);
        let record = self
            .store()
            .pair_device(
                &device_id,
                name.as_deref(),
                platform,
                app_version.as_deref(),
                scope,
                &token_hash(&token),
                &docs::now(),
            )
            .await?;
        self.emit(HubEvent::DevicePaired {
            device_id: device_id.clone(),
        });
        Ok(json!({ "token": token, "hub": self.hello(), "device": record.to_json() }))
    }

    /// Resolves a bearer token to its device.
    pub async fn authenticate(&self, token: &str) -> ApiResult<Caller> {
        let hash = token_hash(token);
        if let Some(device) = self.store().device_by_token_hash(&hash).await? {
            self.store()
                .touch_device(&device.device_id, &docs::now())
                .await?;
            return Ok(Caller::Device(device));
        }
        if self.store().is_revoked_token(&hash).await? {
            return Err(ApiError::device_revoked());
        }
        Err(ApiError::unauthorized())
    }

    pub async fn devices(&self) -> anyhow::Result<Vec<DeviceRecord>> {
        self.store().devices().await
    }

    /// Removes a paired device ("Paired devices" in the app). Its token gets `device_revoked`.
    pub async fn remove_device(&self, device_id: &str) -> anyhow::Result<bool> {
        let _g = self.store().lock().await;
        self.store().remove_device(device_id, &docs::now()).await
    }

    /// "This replaces …": the new device sees the old devices' meetings as its own; their tokens are revoked.
    pub async fn replace_devices(
        &self,
        device_id: &str,
        replaced: &[String],
    ) -> anyhow::Result<()> {
        let _g = self.store().lock().await;
        let device = self
            .store()
            .device(device_id)
            .await?
            .context("unknown device")?;
        let mut all = device.replaces.clone();
        for old in replaced {
            if let Some(old_device) = self.store().device(old).await? {
                all.extend(old_device.replaces);
            }
            all.push(old.clone());
            self.store().remove_device(old, &docs::now()).await?;
        }
        all.sort();
        all.dedup();
        all.retain(|d| d != device_id);
        self.store().set_replaces(device_id, &all).await
    }

    pub async fn own_device(&self, caller: &Caller) -> ApiResult<Value> {
        match caller {
            Caller::Device(d) => Ok(self
                .store()
                .device(&d.device_id)
                .await?
                .ok_or_else(ApiError::unauthorized)?
                .to_json()),
            Caller::Hub => Err(ApiError::not_found()),
        }
    }

    pub async fn update_own_device(&self, caller: &Caller, body: &Value) -> ApiResult<Value> {
        let Caller::Device(d) = caller else {
            return Err(ApiError::not_found());
        };
        let obj = body
            .as_object()
            .filter(|o| !o.is_empty())
            .ok_or_else(|| ApiError::bad_request("expected a non-empty object"))?;
        let get = |k: &str, max: usize| -> ApiResult<Option<String>> {
            match obj.get(k) {
                None => Ok(None),
                Some(Value::String(s)) if s.chars().count() <= max => Ok(Some(s.clone())),
                Some(_) => Err(ApiError::bad_request(format!("invalid {k}"))),
            }
        };
        let name = get("deviceName", 200)?;
        let version = get("appVersion", 64)?;
        let scope = get("scope", 3)?;
        if scope.as_deref().is_some_and(|s| s != "all" && s != "own") {
            return Err(ApiError::bad_request("scope must be all or own"));
        }
        self.store()
            .update_device(
                &d.device_id,
                name.as_deref(),
                version.as_deref(),
                scope.as_deref(),
            )
            .await?;
        self.own_device(caller).await
    }

    pub async fn unpair(&self, caller: &Caller) -> ApiResult<()> {
        let Caller::Device(d) = caller else {
            return Err(ApiError::not_found());
        };
        self.remove_device(&d.device_id).await?;
        Ok(())
    }

    // MARK: meetings

    fn meeting_view(row: &MeetingRow) -> Value {
        let mut doc = row.doc.clone();
        docs::set(&mut doc, "hubState", row.hub_state.clone());
        doc
    }

    /// Finds a meeting the caller may see; others are "not found".
    async fn visible_meeting(&self, caller: &Caller, id: &str) -> ApiResult<MeetingRow> {
        let row = self
            .store()
            .meeting(id)
            .await?
            .ok_or_else(ApiError::not_found)?;
        if !caller.sees(&row.recorded_by) {
            return Err(ApiError::not_found());
        }
        Ok(row)
    }

    pub async fn get_meeting(&self, caller: &Caller, id: &str) -> ApiResult<(Value, String)> {
        let row = self
            .visible_meeting(caller, &docs::lowercase_uuid(id))
            .await?;
        Ok((Self::meeting_view(&row), meeting_etag(row.revision)))
    }

    /// `PUT /v1/meetings/{id}`.
    pub async fn put_meeting(
        &self,
        caller: &Caller,
        id: &str,
        mut body: Value,
        pre: Precondition,
    ) -> ApiResult<Write> {
        let id = docs::lowercase_uuid(id);
        if !body.is_object() {
            return Err(ApiError::bad_request("expected a JSON object"));
        }
        docs::lowercase_meeting_ids(&mut body);
        if docs::str_field(&body, "id") != Some(id.as_str()) {
            return Err(ApiError::bad_request("id in the body must match the path"));
        }
        let body = self.check_schema(body, false).await?;
        let caller_id = caller.device_id(self.hub_id());
        let now = docs::now();

        let _g = self.store().lock().await;
        let existing = self.store().meeting(&id).await?;
        if let Some(row) = &existing {
            if !caller.sees(&row.recorded_by) {
                return Err(ApiError::not_found());
            }
            if row.deleted_at.is_some() {
                return Err(ApiError::meeting_deleted());
            }
        }
        match (pre, existing) {
            (Precondition::Create, Some(row)) => Ok(Write::Conflict {
                doc: Self::meeting_view(&row),
                etag: meeting_etag(row.revision),
            }),
            (Precondition::Update(_), None) => {
                Err(ApiError::not_found().detail("the hub doesn't have this meeting: create it"))
            }
            (Precondition::Update(etag), Some(row)) => {
                if etag != meeting_etag(row.revision) {
                    return Ok(Write::Conflict {
                        doc: Self::meeting_view(&row),
                        etag: meeting_etag(row.revision),
                    });
                }
                if docs::same_user_content(&body, &row.doc, MEETING_HUB_OWNED) {
                    return Ok(Write::Updated {
                        doc: Self::meeting_view(&row),
                        etag: meeting_etag(row.revision),
                    });
                }
                let mut doc = body;
                for key in ["recordedBy", "audio", "deletedAt"] {
                    docs::set(
                        &mut doc,
                        key,
                        row.doc.get(key).cloned().unwrap_or(Value::Null),
                    );
                }
                let revision = row.revision + 1;
                stamp(&mut doc, revision, &now, &caller_id);
                let updated = MeetingRow {
                    doc,
                    revision,
                    ..row
                };
                let seq = self.store().save_meeting(&updated, None).await?;
                self.after_meeting_saved(&id, seq).await?;
                let saved = self.store().meeting(&id).await?.expect("just saved");
                Ok(Write::Updated {
                    doc: Self::meeting_view(&saved),
                    etag: meeting_etag(saved.revision),
                })
            }
            (Precondition::Create, None) => {
                let recorded_by = body
                    .pointer("/recordedBy/deviceId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if matches!(caller, Caller::Device(_)) && recorded_by != caller_id {
                    return Err(ApiError::forbidden(
                        "recordedBy.deviceId must be the creating device",
                    ));
                }
                let declared = body.get("audio").filter(|a| a.is_object()).cloned();
                let mut doc = body;
                docs::set(&mut doc, "audio", Value::Null);
                docs::set(&mut doc, "deletedAt", Value::Null);
                stamp(&mut doc, 1, &now, &caller_id);
                let row = MeetingRow {
                    id: id.clone(),
                    doc,
                    revision: 1,
                    recorded_by,
                    deleted_at: None,
                    seq: 0,
                    hub_state: default_hub_state(),
                    declared_audio: declared,
                    purged: false,
                };
                let seq = self.store().save_meeting(&row, None).await?;
                self.after_meeting_saved(&id, seq).await?;
                let saved = self.store().meeting(&id).await?.expect("just saved");
                Ok(Write::Created {
                    doc: Self::meeting_view(&saved),
                    etag: meeting_etag(saved.revision),
                })
            }
        }
    }

    async fn after_meeting_saved(&self, id: &str, seq: i64) -> anyhow::Result<()> {
        self.notify(Some(seq));
        self.emit(HubEvent::MeetingChanged {
            meeting_id: id.to_string(),
        });
        // A meeting can end after its audio arrived (desktop recordings).
        self.maybe_queue_transcription(id).await
    }

    /// `DELETE /v1/meetings/{id}`: recorders may delete only meetings they recorded.
    pub async fn delete_meeting(&self, caller: &Caller, id: &str) -> ApiResult<()> {
        let id = docs::lowercase_uuid(id);
        let _g = self.store().lock().await;
        let row = self.visible_meeting(caller, &id).await?;
        if row.deleted_at.is_some() {
            return Ok(());
        }
        if !caller.owns(&row.recorded_by) {
            return Err(ApiError::forbidden(
                "only the device that recorded a meeting can delete it everywhere",
            ));
        }
        let now = docs::now();
        let mut doc = row.doc.clone();
        docs::set(&mut doc, "deletedAt", json!(now));
        let revision = row.revision + 1;
        stamp(&mut doc, revision, &now, &caller.device_id(self.hub_id()));
        let deleted = MeetingRow {
            doc,
            revision,
            deleted_at: Some(now),
            ..row
        };
        let seq = self.store().save_meeting(&deleted, None).await?;
        self.notify(Some(seq));
        self.emit(HubEvent::MeetingChanged { meeting_id: id });
        Ok(())
    }

    /// Restores a meeting from "Recently deleted" (desktop app only).
    pub async fn restore_meeting(&self, id: &str) -> anyhow::Result<()> {
        let _g = self.store().lock().await;
        let row = self.store().meeting(id).await?.context("unknown meeting")?;
        anyhow::ensure!(row.deleted_at.is_some(), "the meeting isn't deleted");
        anyhow::ensure!(!row.purged, "the meeting is no longer in Recently deleted");
        let now = docs::now();
        let mut doc = row.doc.clone();
        docs::set(&mut doc, "deletedAt", Value::Null);
        let revision = row.revision + 1;
        stamp(&mut doc, revision, &now, self.hub_id());
        let restored = MeetingRow {
            doc,
            revision,
            deleted_at: None,
            ..row
        };
        let seq = self.store().save_meeting(&restored, None).await?;
        self.notify(Some(seq));
        self.emit(HubEvent::MeetingChanged {
            meeting_id: id.to_string(),
        });
        Ok(())
    }

    // MARK: transcripts

    pub async fn get_transcript(
        &self,
        caller: &Caller,
        meeting_id: &str,
    ) -> ApiResult<(Value, String)> {
        let meeting = self
            .visible_meeting(caller, &docs::lowercase_uuid(meeting_id))
            .await?;
        if meeting.deleted_at.is_some() {
            return Err(ApiError::meeting_deleted());
        }
        let t = self
            .store()
            .transcript(&meeting.id)
            .await?
            .ok_or_else(ApiError::not_found)?;
        Ok((t.doc, transcript_etag(t.revision)))
    }

    /// `PUT /v1/meetings/{id}/transcript`.
    pub async fn put_transcript(
        &self,
        caller: &Caller,
        meeting_id: &str,
        mut body: Value,
        pre: Precondition,
    ) -> ApiResult<Write> {
        let meeting_id = docs::lowercase_uuid(meeting_id);
        if !body.is_object() {
            return Err(ApiError::bad_request("expected a JSON object"));
        }
        docs::lowercase_transcript_ids(&mut body);
        if docs::str_field(&body, "meetingId") != Some(meeting_id.as_str()) {
            return Err(ApiError::bad_request(
                "meetingId in the body must match the path",
            ));
        }
        let mut body = self.check_schema(body, true).await?;
        tidy_transcript(&mut body)?;
        let caller_id = caller.device_id(self.hub_id());
        let now = docs::now();

        let _g = self.store().lock().await;
        let meeting = self.visible_meeting(caller, &meeting_id).await?;
        if meeting.deleted_at.is_some() {
            return Err(ApiError::meeting_deleted());
        }
        if docs::str_field(&meeting.doc, "status") != Some("ended") {
            return Err(ApiError::conflict("meeting_not_ended"));
        }
        let existing = self.store().transcript(&meeting_id).await?;
        let row = match (pre, existing) {
            (Precondition::Create, Some(t)) => {
                return Ok(Write::Conflict {
                    doc: t.doc,
                    etag: transcript_etag(t.revision),
                });
            }
            (Precondition::Update(_), None) => {
                return Err(ApiError::not_found()
                    .detail("the hub has no transcript for this meeting: create it"));
            }
            (Precondition::Update(etag), Some(t)) => {
                if etag != transcript_etag(t.revision) {
                    return Ok(Write::Conflict {
                        doc: t.doc,
                        etag: transcript_etag(t.revision),
                    });
                }
                let body_generation = docs::str_field(&body, "generation")
                    .unwrap_or_default()
                    .to_string();
                let new_generation = body_generation != t.generation;
                if new_generation && !matches!(caller, Caller::Hub) {
                    return Err(ApiError::conflict("generation_conflict")
                        .detail("the hub has a different generation of this transcript; take the hub's version"));
                }
                if !new_generation && docs::same_user_content(&body, &t.doc, TRANSCRIPT_HUB_OWNED) {
                    return Ok(Write::Updated {
                        doc: t.doc,
                        etag: transcript_etag(t.revision),
                    });
                }
                let mut doc = body;
                if !new_generation {
                    for key in ["generation", "status", "producedBy"] {
                        docs::set(
                            &mut doc,
                            key,
                            t.doc.get(key).cloned().unwrap_or(Value::Null),
                        );
                    }
                }
                let revision = t.revision + 1;
                stamp(&mut doc, revision, &now, &caller_id);
                let generation = docs::str_field(&doc, "generation")
                    .unwrap_or_default()
                    .to_string();
                TranscriptRow {
                    meeting_id: meeting_id.clone(),
                    doc,
                    revision,
                    generation,
                }
            }
            (Precondition::Create, None) => {
                // Phones make drafts; only the computer makes final transcripts.
                if matches!(caller, Caller::Device(_))
                    && docs::str_field(&body, "status") != Some("draft")
                {
                    return Err(ApiError::bad_request("transcripts from phones are drafts"));
                }
                let mut doc = body;
                stamp(&mut doc, 1, &now, &caller_id);
                let generation = docs::str_field(&doc, "generation")
                    .unwrap_or_default()
                    .to_string();
                TranscriptRow {
                    meeting_id: meeting_id.clone(),
                    doc,
                    revision: 1,
                    generation,
                }
            }
        };
        let seq = self.store().save_transcript(&row, None).await?;
        self.notify(Some(seq));
        self.emit(HubEvent::TranscriptChanged {
            meeting_id: meeting_id.clone(),
        });
        self.maybe_queue_transcription(&meeting_id).await?;
        let etag = transcript_etag(row.revision);
        Ok(if row.revision == 1 {
            Write::Created { doc: row.doc, etag }
        } else {
            Write::Updated { doc: row.doc, etag }
        })
    }

    // MARK: transcription hand-off

    /// Queues transcription when the hub has the audio of an ended meeting, nothing has been tried yet, and the
    /// transcript is missing or an unedited draft (sync-protocol.md, "What the hub does with incoming meetings").
    /// Failed, skipped or finished transcriptions are not retried automatically; the desktop app offers "Transcribe
    /// again" ([`Hub::begin_retranscription`]). Call with the store locked.
    async fn maybe_queue_transcription(&self, meeting_id: &str) -> anyhow::Result<()> {
        let Some(row) = self.store().meeting(meeting_id).await? else {
            return Ok(());
        };
        let audio = row
            .hub_state
            .get("audioReceived")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if row.deleted_at.is_some()
            || !audio
            || docs::str_field(&row.doc, "status") != Some("ended")
            || transcription_state(&row) != "none"
        {
            return Ok(());
        }
        if !self.transcript_replaceable(meeting_id).await? {
            return Ok(());
        }
        let mut hub_state = row.hub_state.clone();
        hub_state["transcription"] = json!("queued");
        hub_state["transcriptionProgress"] = json!(0);
        hub_state["transcriptionError"] = Value::Null;
        let seq = self
            .store()
            .set_hub_state(meeting_id, &hub_state, true)
            .await?;
        self.notify(seq);
        self.emit(HubEvent::TranscriptionWanted {
            meeting_id: meeting_id.to_string(),
        });
        Ok(())
    }

    /// Missing, or a draft nobody edited.
    async fn transcript_replaceable(&self, meeting_id: &str) -> anyhow::Result<bool> {
        Ok(match self.store().transcript(meeting_id).await? {
            None => true,
            Some(t) => {
                docs::str_field(&t.doc, "status") == Some("draft") && !docs::has_edits(&t.doc)
            }
        })
    }

    /// Starts the automatic transcription that [`HubEvent::TranscriptionWanted`] asked for. Returns `None` (and marks
    /// the meeting `skipped`) when the transcript was edited since it was queued: edits are never replaced
    /// automatically.
    pub async fn begin_transcription(
        &self,
        meeting_id: &str,
    ) -> anyhow::Result<Option<TranscriptionTicket>> {
        self.begin(meeting_id, false).await
    }

    /// "Transcribe again", which the user chose knowing their edits will be replaced.
    pub async fn begin_retranscription(
        &self,
        meeting_id: &str,
    ) -> anyhow::Result<TranscriptionTicket> {
        Ok(self
            .begin(meeting_id, true)
            .await?
            .expect("forced transcription always starts"))
    }

    async fn begin(
        &self,
        meeting_id: &str,
        forced: bool,
    ) -> anyhow::Result<Option<TranscriptionTicket>> {
        let _g = self.store().lock().await;
        let row = self
            .store()
            .meeting(meeting_id)
            .await?
            .context("unknown meeting")?;
        anyhow::ensure!(row.deleted_at.is_none(), "the meeting was deleted");
        let file = row
            .doc
            .pointer("/audio/fileName")
            .and_then(Value::as_str)
            .context("the meeting has no audio")?;
        let audio_path = self.store().audio_dir(meeting_id).join(file);
        if !forced && !self.transcript_replaceable(meeting_id).await? {
            self.update_hub_state(&row, |s| s["transcription"] = json!("skipped"), true)
                .await?;
            return Ok(None);
        }
        let based_on_revision = self
            .store()
            .transcript(meeting_id)
            .await?
            .map(|t| t.revision);
        self.update_hub_state(
            &row,
            |s| {
                s["transcription"] = json!("running");
                s["transcriptionProgress"] = json!(0);
                s["transcriptionError"] = Value::Null;
            },
            true,
        )
        .await?;
        Ok(Some(TranscriptionTicket {
            meeting_id: meeting_id.to_string(),
            audio_path,
            language_code: docs::str_field(&row.doc, "languageCode").map(str::to_string),
            based_on_revision,
        }))
    }

    /// Progress for phones that poll `GET /v1/meetings/{id}`; no change-feed entry.
    pub async fn set_transcription_progress(
        &self,
        meeting_id: &str,
        fraction: f64,
    ) -> anyhow::Result<()> {
        let _g = self.store().lock().await;
        let row = self
            .store()
            .meeting(meeting_id)
            .await?
            .context("unknown meeting")?;
        self.update_hub_state(
            &row,
            |s| s["transcriptionProgress"] = json!(fraction.clamp(0.0, 1.0)),
            false,
        )
        .await
    }

    pub async fn fail_transcription(&self, meeting_id: &str, error: &str) -> anyhow::Result<()> {
        let _g = self.store().lock().await;
        let row = self
            .store()
            .meeting(meeting_id)
            .await?
            .context("unknown meeting")?;
        self.mark_failed(&row, error).await
    }

    async fn mark_failed(&self, row: &MeetingRow, error: &str) -> anyhow::Result<()> {
        let error: String = error.chars().take(1000).collect();
        self.update_hub_state(
            row,
            |s| {
                s["transcription"] = json!("failed");
                s["transcriptionError"] = json!(error);
            },
            true,
        )
        .await
    }

    /// Saves the hub's final transcript, unless the transcript changed since transcription began (the user's
    /// version then stays). `doc` is a complete transcript.json; the hub fills in meetingId, a new generation,
    /// revision and the update fields. An invalid `doc` marks the transcription `failed`.
    pub async fn finish_transcription(
        &self,
        ticket: TranscriptionTicket,
        mut doc: Value,
    ) -> anyhow::Result<FinishOutcome> {
        let _g = self.store().lock().await;
        let row = self
            .store()
            .meeting(&ticket.meeting_id)
            .await?
            .context("unknown meeting")?;
        if row.deleted_at.is_some() {
            return Ok(FinishOutcome::MeetingDeleted);
        }
        let current = self.store().transcript(&ticket.meeting_id).await?;
        if current.as_ref().map(|t| t.revision) != ticket.based_on_revision {
            self.update_hub_state(
                &row,
                |s| {
                    s["transcription"] = json!("skipped");
                    s["transcriptionProgress"] = json!(1);
                },
                true,
            )
            .await?;
            return Ok(FinishOutcome::Skipped);
        }
        docs::lowercase_transcript_ids(&mut doc);
        docs::set(&mut doc, "formatVersion", json!(FORMAT_VERSION));
        docs::set(&mut doc, "meetingId", json!(ticket.meeting_id));
        let generation = match docs::str_field(&doc, "generation") {
            Some(g) if is_uuid(g) && current.as_ref().is_none_or(|t| t.generation != g) => {
                g.to_string()
            }
            _ => uuid::Uuid::new_v4().to_string(),
        };
        docs::set(&mut doc, "generation", json!(generation));
        let revision = current.as_ref().map(|t| t.revision + 1).unwrap_or(1);
        stamp(&mut doc, revision, &docs::now(), self.hub_id());
        let checked = match self.check_schema(doc, true).await {
            Ok(mut d) => match tidy_transcript(&mut d) {
                Ok(()) => Ok(d),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        let doc = match checked {
            Ok(d) => d,
            Err(e) => {
                let message = e.detail.clone().unwrap_or_else(|| e.code.to_string());
                self.mark_failed(&row, &format!("invalid transcript: {message}"))
                    .await?;
                anyhow::bail!("invalid transcript: {message}");
            }
        };
        let mut state = row.hub_state.clone();
        state["transcription"] = json!("done");
        state["transcriptionProgress"] = json!(1);
        state["transcriptionError"] = Value::Null;
        let row = TranscriptRow {
            meeting_id: ticket.meeting_id.clone(),
            doc,
            revision,
            generation,
        };
        let seq = self.store().save_transcript(&row, Some(&state)).await?;
        self.notify(Some(seq));
        self.emit(HubEvent::TranscriptChanged {
            meeting_id: ticket.meeting_id,
        });
        Ok(FinishOutcome::Saved)
    }

    async fn update_hub_state(
        &self,
        row: &MeetingRow,
        change: impl FnOnce(&mut Value),
        bump: bool,
    ) -> anyhow::Result<()> {
        let mut state = row.hub_state.clone();
        change(&mut state);
        let seq = self.store().set_hub_state(&row.id, &state, bump).await?;
        self.notify(seq);
        Ok(())
    }

    // MARK: audio

    /// `POST /v1/meetings/{id}/audio/uploads`.
    pub async fn start_upload(
        &self,
        caller: &Caller,
        meeting_id: &str,
        body: &Value,
    ) -> ApiResult<(StatusCode, Value)> {
        let meeting_id = docs::lowercase_uuid(meeting_id);
        let Caller::Device(device) = caller else {
            return Err(ApiError::forbidden(
                "the hub attaches its own audio locally",
            ));
        };
        let file_name = body
            .get("fileName")
            .and_then(Value::as_str)
            .filter(|f| FILE_NAMES.contains(f));
        let mime = body.get("mimeType").and_then(Value::as_str);
        let size = body
            .get("sizeBytes")
            .and_then(Value::as_i64)
            .filter(|s| (1..=MAX_AUDIO_BYTES).contains(s));
        let sha = body.get("sha256").and_then(Value::as_str).filter(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
        let (Some(file_name), Some(mime), Some(size), Some(sha)) = (file_name, mime, size, sha)
        else {
            if body
                .get("sizeBytes")
                .and_then(Value::as_i64)
                .is_some_and(|s| s > MAX_AUDIO_BYTES)
            {
                return Err(ApiError::payload_too_large());
            }
            return Err(ApiError::bad_request(
                "fileName, mimeType, sizeBytes (1 B to 4 GiB) and sha256 (lowercase hex) are required",
            ));
        };
        if mime_for_extension(file_name) != Some(mime) {
            return Err(ApiError::bad_request("mimeType doesn't match fileName"));
        }

        let _g = self.store().lock().await;
        let meeting = self.visible_meeting(caller, &meeting_id).await?;
        if meeting.deleted_at.is_some() {
            return Err(ApiError::meeting_deleted());
        }
        if !caller.owns(&meeting.recorded_by) {
            return Err(ApiError::forbidden(
                "only the device that recorded a meeting uploads its audio",
            ));
        }
        if docs::str_field(&meeting.doc, "status") != Some("ended") {
            return Err(ApiError::conflict("meeting_not_ended"));
        }
        if let Some(existing) = meeting.doc.get("audio").filter(|a| a.is_object()) {
            if existing.get("sha256").and_then(Value::as_str) == Some(sha) {
                return Ok((StatusCode::OK, json!({ "complete": true })));
            }
            return Err(ApiError::conflict("audio_exists"));
        }
        let expires = |u: &UploadRow| -> String {
            chrono::DateTime::parse_from_rfc3339(&u.updated_at)
                .map(|t| {
                    (t.with_timezone(&chrono::Utc) + UPLOAD_TTL)
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                })
                .unwrap_or_default()
        };
        // One unfinished upload per meeting and device: a different file replaces the earlier attempt.
        for u in self
            .store()
            .unfinished_uploads(&device.device_id, Some(&meeting_id))
            .await?
        {
            if u.sha256 == sha && u.size_bytes == size && u.file_name == file_name {
                return Ok((
                    StatusCode::OK,
                    json!({ "complete": false, "uploadId": u.id, "offset": u.offset, "expiresAt": expires(&u) }),
                ));
            }
            self.discard_upload(&u.id).await?;
        }
        let limit = self.inner.config.max_unfinished_uploads_per_device;
        if self
            .store()
            .unfinished_uploads(&device.device_id, None)
            .await?
            .len()
            >= limit
        {
            return Err(ApiError::rate_limited(60)
                .detail(format!("at most {limit} unfinished uploads per device")));
        }
        let free = fs4::available_space(&self.inner.config.data_dir).unwrap_or(u64::MAX);
        if free < size as u64 + self.inner.config.min_free_space {
            return Err(
                ApiError::payload_too_large().detail("not enough free disk space on the computer")
            );
        }
        let upload = UploadRow {
            id: random_token(18),
            meeting_id,
            device_id: device.device_id.clone(),
            file_name: file_name.to_string(),
            mime_type: mime.to_string(),
            size_bytes: size,
            sha256: sha.to_string(),
            offset: 0,
            complete: false,
            updated_at: docs::now(),
        };
        tokio::fs::File::create(self.store().upload_path(&upload.id)).await?;
        self.store().save_upload(&upload).await?;
        Ok((
            StatusCode::CREATED,
            json!({ "complete": false, "uploadId": upload.id, "offset": 0, "expiresAt": expires(&upload) }),
        ))
    }

    async fn caller_upload(&self, caller: &Caller, upload_id: &str) -> ApiResult<UploadRow> {
        if !is_upload_id(upload_id) {
            return Err(ApiError::not_found());
        }
        let upload = self
            .store()
            .upload(upload_id)
            .await?
            .ok_or_else(ApiError::not_found)?;
        match caller {
            Caller::Device(d) if d.device_id == upload.device_id => Ok(upload),
            _ => Err(ApiError::not_found()),
        }
    }

    /// `HEAD /v1/uploads/{id}`.
    pub async fn upload_state(
        &self,
        caller: &Caller,
        upload_id: &str,
    ) -> ApiResult<UploadProgress> {
        let u = self.caller_upload(caller, upload_id).await?;
        Ok(UploadProgress {
            offset: u.offset,
            complete: u.complete,
        })
    }

    fn upload_lock(&self, upload_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.inner
            .upload_locks
            .lock()
            .unwrap()
            .entry(upload_id.to_string())
            .or_default()
            .clone()
    }

    fn forget_upload_lock(&self, upload_id: &str) {
        self.inner.upload_locks.lock().unwrap().remove(upload_id);
    }

    async fn discard_upload(&self, upload_id: &str) -> anyhow::Result<()> {
        self.store().delete_upload(upload_id).await?;
        self.forget_upload_lock(upload_id);
        Ok(())
    }

    /// `PATCH /v1/uploads/{id}`: one request per upload at a time; offsets count durably written bytes.
    pub async fn append_upload(
        &self,
        caller: &Caller,
        upload_id: &str,
        offset: i64,
        bytes: &[u8],
    ) -> ApiResult<UploadProgress> {
        // Only uploads that exist get a lock (unknown IDs must not grow the lock map).
        self.caller_upload(caller, upload_id).await?;
        let lock = self.upload_lock(upload_id);
        let _upload_guard = lock.lock().await;
        // Look again: it may have completed, expired or been replaced while waiting.
        let mut upload = self.caller_upload(caller, upload_id).await?;
        let mismatch = |at: i64| {
            ApiError::conflict("upload_offset_mismatch")
                .header(axum::http::HeaderName::from_static("upload-offset"), at)
        };
        if upload.complete {
            return if offset == upload.size_bytes && bytes.is_empty() {
                Ok(UploadProgress {
                    offset: upload.size_bytes,
                    complete: true,
                })
            } else {
                Err(mismatch(upload.offset))
            };
        }
        if offset != upload.offset {
            return Err(mismatch(upload.offset));
        }
        if offset + bytes.len() as i64 > upload.size_bytes {
            return Err(ApiError::payload_too_large().detail("more bytes than sizeBytes"));
        }
        let path = self.store().upload_path(upload_id);
        if !bytes.is_empty() {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .await?;
            if file.metadata().await?.len() < offset as u64 {
                // The partial file was lost (e.g. disk cleanup): start the upload again.
                drop(file);
                self.discard_upload(upload_id).await?;
                return Err(ApiError::not_found().detail("the upload was lost; start a new one"));
            }
            file.set_len(offset as u64).await?;
            file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
            file.write_all(bytes).await?;
            file.sync_data().await?;
        }
        upload.offset = offset + bytes.len() as i64;
        upload.updated_at = docs::now();

        if upload.offset < upload.size_bytes {
            self.store().save_upload(&upload).await?;
            return Ok(UploadProgress {
                offset: upload.offset,
                complete: false,
            });
        }
        // The last byte arrived. Check it, then attach; failures that a retry can't fix end the upload.
        let target = self
            .store()
            .audio_dir(&upload.meeting_id)
            .join(&upload.file_name);
        let source = if path.exists() {
            path.clone()
        } else {
            target.clone()
        };
        let digest = match sha256_file(&source).await {
            Ok(d) => d,
            Err(_) => {
                self.discard_upload(upload_id).await?;
                return Err(ApiError::not_found().detail("the upload was lost; start a new one"));
            }
        };
        if digest != upload.sha256 {
            self.discard_upload(upload_id).await?;
            return Err(ApiError::checksum_mismatch().detail(format!(
                "received {} bytes with SHA-256 {digest}, expected {}",
                upload.size_bytes, upload.sha256
            )));
        }
        let device_id = caller.device_id(self.hub_id());
        upload.complete = true;
        match self
            .attach_audio(
                &upload.meeting_id,
                &source,
                &upload.file_name,
                &upload.mime_type,
                &upload.sha256,
                &device_id,
                Some(&upload),
            )
            .await
        {
            Ok(()) => {
                self.forget_upload_lock(upload_id);
                Ok(UploadProgress {
                    offset: upload.offset,
                    complete: true,
                })
            }
            Err(e) if e.status.is_client_error() => {
                self.discard_upload(upload_id).await?;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn probe(&self, path: &Path, mime: &str) -> Option<AudioDetails> {
        let prober = self.inner.prober.clone();
        let (path, mime) = (path.to_path_buf(), mime.to_string());
        let job = tokio::task::spawn_blocking(move || prober.probe(&path, &mime));
        match tokio::time::timeout(PROBE_TIMEOUT, job).await {
            Ok(Ok(Ok(details))) => Some(details),
            Ok(Ok(Err(e))) => {
                tracing::warn!(target: "sync_hub", "couldn't read audio details: {e:#}");
                None
            }
            _ => {
                tracing::warn!(target: "sync_hub", "reading audio details took too long");
                None
            }
        }
    }

    /// Moves a verified file into the meeting and records it in meeting.json → `audio` (and the upload as complete,
    /// in the same transaction). Slow work (probing) happens before taking the store lock.
    #[allow(clippy::too_many_arguments)]
    async fn attach_audio(
        &self,
        meeting_id: &str,
        source: &Path,
        file_name: &str,
        mime: &str,
        sha256: &str,
        updated_by: &str,
        upload: Option<&UploadRow>,
    ) -> ApiResult<()> {
        let probed = self.probe(source, mime).await;
        let size = tokio::fs::metadata(source).await?.len();

        let _g = self.store().lock().await;
        let row = self
            .store()
            .meeting(meeting_id)
            .await?
            .ok_or_else(ApiError::not_found)?;
        if row.deleted_at.is_some() {
            return Err(ApiError::meeting_deleted());
        }
        if let Some(existing) = row.doc.get("audio").filter(|a| a.is_object()) {
            if existing.get("sha256").and_then(Value::as_str) != Some(sha256) {
                return Err(ApiError::conflict("audio_exists"));
            }
            if let Some(u) = upload {
                self.store().save_upload(u).await?;
            }
            return Ok(());
        }
        let dir = self.store().audio_dir(meeting_id);
        tokio::fs::create_dir_all(&dir).await?;
        let target = dir.join(file_name);
        if source != target {
            // Same volume (uploads/ and audio/ share the data directory), so this is a rename.
            tokio::fs::rename(source, &target).await?;
        }
        let declared = row.declared_audio.clone().unwrap_or(Value::Null);
        let mut audio = json!({
            "fileName": file_name,
            "mimeType": mime,
            "durationSec": probed.as_ref().map(|p| p.duration_sec)
                .or_else(|| declared.get("durationSec").and_then(Value::as_f64)).unwrap_or(0.0),
            "sizeBytes": size,
            "sha256": sha256,
        });
        let codec = probed.as_ref().and_then(|p| p.codec.clone()).or_else(|| {
            declared
                .get("codec")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
        let rate = probed
            .as_ref()
            .and_then(|p| p.sample_rate)
            .map(i64::from)
            .or_else(|| declared.get("sampleRate").and_then(Value::as_i64));
        let channels = probed
            .as_ref()
            .and_then(|p| p.channels)
            .map(i64::from)
            .or_else(|| declared.get("channels").and_then(Value::as_i64));
        if let Some(c) = codec.filter(|c| !c.is_empty() && c.len() <= 32) {
            audio["codec"] = json!(c);
        }
        if let Some(r) = rate.filter(|r| *r >= 8000) {
            audio["sampleRate"] = json!(r);
        }
        if let Some(c) = channels.filter(|c| (1..=8).contains(c)) {
            audio["channels"] = json!(c);
        }
        let mut doc = row.doc.clone();
        docs::set(&mut doc, "audio", audio);
        let revision = row.revision + 1;
        stamp(&mut doc, revision, &docs::now(), updated_by);
        let mut hub_state = row.hub_state.clone();
        hub_state["audioReceived"] = json!(true);
        let updated = MeetingRow {
            doc,
            revision,
            hub_state,
            ..row
        };
        let seq = match self.store().save_meeting(&updated, upload).await {
            Ok(seq) => seq,
            Err(e) => {
                if source != target {
                    let _ = tokio::fs::rename(&target, source).await;
                }
                return Err(e.into());
            }
        };
        self.notify(Some(seq));
        self.emit(HubEvent::AudioReceived {
            meeting_id: meeting_id.to_string(),
            path: target,
        });
        self.emit(HubEvent::MeetingChanged {
            meeting_id: meeting_id.to_string(),
        });
        self.maybe_queue_transcription(meeting_id).await?;
        Ok(())
    }

    /// For `GET /v1/meetings/{id}/audio`.
    pub async fn audio_file(&self, caller: &Caller, meeting_id: &str) -> ApiResult<AudioFile> {
        let row = self
            .visible_meeting(caller, &docs::lowercase_uuid(meeting_id))
            .await?;
        if row.deleted_at.is_some() {
            return Err(ApiError::meeting_deleted());
        }
        let audio = row
            .doc
            .get("audio")
            .filter(|a| a.is_object())
            .ok_or_else(ApiError::not_found)?;
        let file = audio
            .get("fileName")
            .and_then(Value::as_str)
            .filter(|f| FILE_NAMES.contains(f))
            .ok_or_else(ApiError::not_found)?;
        let path = self.store().audio_dir(&row.id).join(file);
        let size = tokio::fs::metadata(&path)
            .await
            .map_err(|_| ApiError::not_found())?
            .len();
        Ok(AudioFile {
            path,
            sha256: audio
                .get("sha256")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            mime_type: audio
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream")
                .to_string(),
            size,
        })
    }

    // MARK: change feed

    /// `GET /v1/changes`. Cursors are opaque to phones: a number for incremental syncs, and `f<last>.<start>` while
    /// paging through a full sync (which lists only meetings that aren't deleted and never expires mid-way). The last
    /// page of a full sync returns the sequence number from when it started, so changes made during it come again.
    pub async fn changes(
        &self,
        caller: &Caller,
        since: Option<&str>,
        limit: Option<i64>,
        wait: Option<u64>,
    ) -> ApiResult<Value> {
        enum Mode {
            Full { after: i64, started: Option<i64> },
            Incremental(i64),
        }
        let invalid = || ApiError::bad_request("invalid cursor");
        let mode = match since {
            None => Mode::Full {
                after: 0,
                started: None,
            },
            Some(s) if s.starts_with('f') => {
                let (after, started) = s[1..].split_once('.').ok_or_else(invalid)?;
                Mode::Full {
                    after: after.parse().map_err(|_| invalid())?,
                    started: Some(started.parse().map_err(|_| invalid())?),
                }
            }
            Some(s) => Mode::Incremental(
                s.parse::<i64>()
                    .ok()
                    .filter(|v| *v >= 0)
                    .ok_or_else(invalid)?,
            ),
        };
        let limit = limit.unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(ApiError::bad_request("limit must be 1 to 500"));
        }
        let wait = wait.unwrap_or(0);
        if wait > 30 {
            return Err(ApiError::bad_request("wait must be 0 to 30"));
        }
        if let Mode::Incremental(s) = mode {
            if s < self.store().pruned_through().await? {
                return Err(ApiError::cursor_expired());
            }
        }
        let filter = caller.scope_filter();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait);
        let mut seq_rx = self.inner.seq.subscribe();
        loop {
            let (after, live_only) = match mode {
                Mode::Full { after, .. } => (after, true),
                Mode::Incremental(s) => (s, false),
            };
            let (snapshot, rows) = self
                .store()
                .changes(after, live_only, limit + 1, filter.as_deref())
                .await?;
            if rows.is_empty()
                && matches!(mode, Mode::Incremental(_))
                && tokio::time::Instant::now() < deadline
            {
                // Long polling: wait for the next change (or the deadline), then look again.
                let _ = tokio::time::timeout_at(deadline, seq_rx.changed()).await;
                seq_rx.borrow_and_update();
                continue;
            }
            let has_more = rows.len() as i64 > limit;
            let page: Vec<MeetingRow> = rows.into_iter().take(limit as usize).collect();
            let last = page.last().map(|r| r.seq);
            let cursor = match mode {
                Mode::Incremental(_) if has_more => last.unwrap_or(snapshot).to_string(),
                Mode::Incremental(_) => snapshot.to_string(),
                Mode::Full { started, .. } => {
                    let started = started.unwrap_or(snapshot);
                    if has_more {
                        format!("f{}.{started}", last.unwrap_or(0))
                    } else {
                        started.to_string()
                    }
                }
            };
            let ids: Vec<String> = page
                .iter()
                .filter(|r| r.deleted_at.is_none())
                .map(|r| r.id.clone())
                .collect();
            let transcripts = self.store().transcript_revisions(&ids).await?;
            let changes: Vec<Value> = page
                .iter()
                .map(|r| {
                    let deleted = r.deleted_at.is_some();
                    let transcript_etag = match (deleted, transcripts.get(&r.id)) {
                        (false, Some(rev)) => json!(transcript_etag(*rev)),
                        _ => Value::Null,
                    };
                    let audio = match (deleted, r.doc.get("audio").filter(|a| a.is_object())) {
                        (false, Some(a)) => {
                            json!({ "sha256": a["sha256"], "sizeBytes": a["sizeBytes"] })
                        }
                        _ => Value::Null,
                    };
                    json!({
                        "meetingId": r.id,
                        "deleted": deleted,
                        "meetingEtag": meeting_etag(r.revision),
                        "transcriptEtag": transcript_etag,
                        "audio": audio,
                        "hubState": r.hub_state,
                    })
                })
                .collect();
            return Ok(json!({ "changes": changes, "cursor": cursor, "hasMore": has_more }));
        }
    }

    // MARK: the desktop app's own meetings

    /// Creates or replaces a meeting recorded or imported on this computer. No merging: the app is authoritative.
    pub async fn save_local_meeting(&self, mut doc: Value) -> anyhow::Result<Value> {
        docs::lowercase_meeting_ids(&mut doc);
        let id = docs::str_field(&doc, "id")
            .context("meeting.json needs an id")?
            .to_string();
        for _ in 0..3 {
            let pre = match self.store().meeting(&id).await? {
                Some(row) => Precondition::Update(meeting_etag(row.revision)),
                None => Precondition::Create,
            };
            match self
                .put_meeting(&Caller::Hub, &id, doc.clone(), pre)
                .await
                .map_err(api_to_anyhow)?
            {
                Write::Created { doc, .. } | Write::Updated { doc, .. } => return Ok(doc),
                Write::Conflict { .. } => continue,
            }
        }
        anyhow::bail!("meeting {id} kept changing while saving")
    }

    /// Attaches a recording made on this computer (copied into the hub's store).
    pub async fn attach_local_audio(&self, meeting_id: &str, source: &Path) -> anyhow::Result<()> {
        let file_name = source
            .file_name()
            .and_then(|n| n.to_str())
            .context("audio file name")?;
        let ext = file_name
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let target_name = format!("audio.{ext}");
        anyhow::ensure!(
            FILE_NAMES.contains(&target_name.as_str()),
            "unsupported audio type .{ext}"
        );
        let mime = mime_for_extension(&target_name).expect("known extension");
        let staging = self
            .store()
            .upload_path(&format!("local-{}", uuid::Uuid::new_v4()));
        tokio::fs::copy(source, &staging).await?;
        let sha = sha256_file(&staging).await?;
        let result = self
            .attach_audio(
                meeting_id,
                &staging,
                &target_name,
                mime,
                &sha,
                self.hub_id(),
                None,
            )
            .await;
        let _ = tokio::fs::remove_file(&staging).await;
        result.map_err(api_to_anyhow)
    }

    /// Creates or replaces the transcript of a meeting from this computer (a new generation is allowed).
    pub async fn save_local_transcript(&self, mut doc: Value) -> anyhow::Result<Value> {
        docs::lowercase_transcript_ids(&mut doc);
        let id = docs::str_field(&doc, "meetingId")
            .context("transcript.json needs a meetingId")?
            .to_string();
        for _ in 0..3 {
            let pre = match self.store().transcript(&id).await? {
                Some(t) => Precondition::Update(transcript_etag(t.revision)),
                None => Precondition::Create,
            };
            match self
                .put_transcript(&Caller::Hub, &id, doc.clone(), pre)
                .await
                .map_err(api_to_anyhow)?
            {
                Write::Created { doc, .. } | Write::Updated { doc, .. } => return Ok(doc),
                Write::Conflict { .. } => continue,
            }
        }
        anyhow::bail!("transcript of {id} kept changing while saving")
    }

    pub async fn delete_local_meeting(&self, meeting_id: &str) -> anyhow::Result<()> {
        self.delete_meeting(&Caller::Hub, meeting_id)
            .await
            .map_err(api_to_anyhow)
    }

    /// The meeting and transcript as the hub has them (for the desktop app).
    pub async fn local_meeting(
        &self,
        meeting_id: &str,
    ) -> anyhow::Result<Option<(Value, Option<Value>)>> {
        let Some(row) = self.store().meeting(meeting_id).await? else {
            return Ok(None);
        };
        let transcript = self.store().transcript(meeting_id).await?.map(|t| t.doc);
        Ok(Some((Self::meeting_view(&row), transcript)))
    }

    // MARK: maintenance

    /// Empties Recently deleted, forgets old tombstones, and expires uploads idle for 24 hours.
    pub async fn run_maintenance(&self) -> anyhow::Result<()> {
        let config = &self.inner.config;
        {
            let _g = self.store().lock().await;
            for m in self
                .store()
                .meetings_deleted_before(&ago(config.tombstone_retention))
                .await?
            {
                let _ = tokio::fs::remove_dir_all(self.store().audio_dir(&m.id)).await;
                self.store().forget_meeting(&m).await?;
            }
            for m in self
                .store()
                .meetings_deleted_before(&ago(config.trash_retention))
                .await?
            {
                if !m.purged {
                    let _ = tokio::fs::remove_dir_all(self.store().audio_dir(&m.id)).await;
                    self.store().mark_purged(&m.id).await?;
                }
            }
        }
        let cutoff =
            (chrono::Utc::now() - UPLOAD_TTL).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        for u in self.store().uploads_idle_since(&cutoff).await? {
            // Skip uploads a request is working on right now.
            let lock = self.upload_lock(&u.id);
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            self.discard_upload(&u.id).await?;
        }
        Ok(())
    }
}

/// Hub-owned fields of a document being saved.
fn stamp(doc: &mut Value, revision: i64, now: &str, updated_by: &str) {
    docs::set(doc, "formatVersion", json!(FORMAT_VERSION));
    docs::set(doc, "revision", json!(revision));
    docs::set(doc, "updatedAt", json!(now));
    docs::set(doc, "updatedBy", json!(updated_by));
    if let Value::Object(map) = doc {
        map.remove("hubState");
    }
}

fn api_to_anyhow(e: ApiError) -> anyhow::Error {
    anyhow::anyhow!(
        "{} {}{}",
        e.status.as_u16(),
        e.code,
        e.detail.map(|d| format!(": {d}")).unwrap_or_default()
    )
}

pub async fn sha256_file(path: &Path) -> std::io::Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut f = std::fs::File::open(path)?;
        let mut h = Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        Ok(hex::encode(h.finalize()))
    })
    .await
    .map_err(std::io::Error::other)?
}
