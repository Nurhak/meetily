//! Phone sync (Sales Copilot): the computer is the hub, phones pair by QR code and sync meetings and
//! transcripts over the local network, without a cloud. The work happens in the `sync-bridge` crate; this
//! module starts it once the database is ready, lets it use Meetily's retranscription for meetings recorded
//! on phones, and exposes it to the UI as Tauri commands and events.
//!
//! Phones that are recording can also stream their audio here and get Meetily's transcript back as people talk
//! (live transcription, [`MeetilyLive`]), using the same speech detection and engine as Meetily's own recordings.
//!
//! Events for the UI:
//! - `sync-meetings-changed`: meetings were added, changed or removed by sync; refresh the list.
//! - `sync-device-paired` (`{ deviceId }`): a phone just paired; close the pairing dialog.
//! - `sync-live-changed` (`{ sessions }`): a phone started or stopped live transcription.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use log::{error, info};
use serde::Serialize;
use sqlx::SqlitePool;
use sync_bridge::sync_hub::{LiveError, LiveOpening, LiveReady, LiveSession, LiveStart, LiveText, LiveTranscriber};
use sync_bridge::{
    BridgeEvent, DeletedMeetingView, DeviceView, PairingView, ServiceConfig, SyncService, SyncStatus,
    TranscribeError, TranscribeJob, Transcriber,
};
use tauri::{AppHandle, Emitter, Manager, Runtime, State};
use tauri_plugin_store::StoreExt;
use tokio::sync::{mpsc, OnceCell};

use crate::audio::recording_preferences::{get_default_recordings_folder, RecordingPreferences};
use crate::audio::transcription::engine::{get_or_init_transcription_engine, validate_transcription_model_ready};
use crate::audio::transcription::TranscriptionEngine;
use crate::audio::vad::ContinuousVadProcessor;
use crate::state::AppState;

/// Managed from startup; the service is set once the database is ready (on a first launch only after
/// onboarding has created it).
#[derive(Default)]
pub struct SyncState(OnceCell<Arc<SyncService>>);

impl SyncState {
    fn service(&self) -> Result<&Arc<SyncService>, String> {
        self.0.get().ok_or_else(|| "Phone sync is still starting".to_string())
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DevicePairedPayload {
    device_id: String,
}

/// Starts phone sync (when the user turned it on) as soon as Meetily's database is available.
pub fn start<R: Runtime>(app: AppHandle<R>) {
    tauri::async_runtime::spawn(async move {
        let pool = loop {
            if let Some(state) = app.try_state::<AppState>() {
                break state.db_manager.pool().clone();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        let data_dir = match app.path().app_data_dir() {
            Ok(dir) => dir.join("sync"),
            Err(e) => {
                error!("Phone sync: no app data directory: {e}");
                return;
            }
        };

        let (ui_tx, mut ui_rx) = mpsc::unbounded_channel();
        let recordings_app = app.clone();
        let service = Arc::new(SyncService::new(
            ServiceConfig {
                data_dir,
                platform: platform().to_string(),
                app_version: app.package_info().version.to_string(),
                recordings_dir: Arc::new(move || recordings_folder(&recordings_app)),
                live: Some(Arc::new(MeetilyLive { app: app.clone() })),
            },
            pool.clone(),
            Arc::new(MeetilyTranscriber { app: app.clone(), pool }),
            ui_tx,
        ));
        if app.state::<SyncState>().0.set(service.clone()).is_err() {
            return;
        }

        let events_app = app.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(event) = ui_rx.recv().await {
                let sent = match event {
                    BridgeEvent::MeetingsChanged => events_app.emit("sync-meetings-changed", ()),
                    BridgeEvent::DevicePaired { device_id } => {
                        events_app.emit("sync-device-paired", DevicePairedPayload { device_id })
                    }
                };
                if let Err(e) = sent {
                    error!("Phone sync: couldn't notify the UI: {e}");
                }
            }
        });

        service.start_if_enabled().await;
        info!("Phone sync ready (on: {})", service.status().await.running);
    });
}

/// Stops the sync server; called when the app exits.
pub async fn shutdown<R: Runtime>(app: &AppHandle<R>) {
    if let Some(state) = app.try_state::<SyncState>() {
        if let Some(service) = state.0.get() {
            service.shutdown().await;
        }
    }
}

fn platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    }
}

/// The recordings folder from the Recording settings; meetings from phones get their folders there.
fn recordings_folder<R: Runtime>(app: &AppHandle<R>) -> PathBuf {
    app.store("recording_preferences.json")
        .ok()
        .and_then(|store| store.get("preferences"))
        .and_then(|value| serde_json::from_value::<RecordingPreferences>(value).ok())
        .map(|prefs| prefs.save_folder)
        .filter(|folder| !folder.as_os_str().is_empty())
        .unwrap_or_else(get_default_recordings_folder)
}

/// Transcribes meetings recorded on phones (and re-transcribes when a phone asks) with Meetily's own
/// retranscription, using the engine and model chosen in Settings.
struct MeetilyTranscriber<R: Runtime> {
    app: AppHandle<R>,
    pool: SqlitePool,
}

impl<R: Runtime> Transcriber for MeetilyTranscriber<R> {
    fn transcribe(&self, job: TranscribeJob) -> BoxFuture<'static, Result<(), TranscribeError>> {
        let app = self.app.clone();
        let pool = self.pool.clone();
        Box::pin(async move {
            // One transcription at a time, never while recording, and not while phones get live transcription:
            // try again later.
            if crate::audio::recording_commands::is_recording().await
                || crate::audio::retranscription::is_retranscription_in_progress()
                || crate::audio::import::is_import_in_progress()
                || LIVE_SESSIONS.load(Ordering::SeqCst) > 0
            {
                return Err(TranscribeError::Busy);
            }
            let settings: Option<(String, String)> =
                sqlx::query_as("SELECT provider, model FROM transcript_settings WHERE id = '1'")
                    .fetch_optional(&pool)
                    .await
                    .ok()
                    .flatten();
            let (provider, model) = match settings {
                Some((provider, model)) => (provider, (!model.is_empty()).then_some(model)),
                None => ("parakeet".to_string(), None),
            };
            // Parakeet detects the language itself; Whisper takes the meeting's language.
            let language = if provider == "parakeet" { None } else { job.language };
            info!(
                "Phone sync: transcribing meeting {} with {provider} ({model:?})",
                job.meetily_meeting_id
            );
            crate::audio::retranscription::start_retranscription(
                app,
                job.meetily_meeting_id,
                job.folder.to_string_lossy().to_string(),
                language,
                model,
                Some(provider),
            )
            .await
            .map(|_| ())
            .map_err(|e| {
                let message = format!("{e:#}");
                if message.contains("already in progress") {
                    TranscribeError::Busy
                } else {
                    TranscribeError::Failed(message)
                }
            })
        })
    }
}

// MARK: live transcription

/// Live sessions open now; Meetily's own batch transcription of phone meetings waits while there are any.
static LIVE_SESSIONS: AtomicUsize = AtomicUsize::new(0);

/// Live transcription for phones with Meetily's speech detection (Silero VAD) and engine (Parakeet or Whisper,
/// as chosen in Settings → Transcription). Unavailable while this computer records or transcribes another
/// meeting, so the phone keeps its own draft and gets the final transcript after the meeting.
struct MeetilyLive<R: Runtime> {
    app: AppHandle<R>,
}

fn busy_reason() -> Option<&'static str> {
    if crate::audio::recording_commands::is_recording().now_or_never().unwrap_or(true) {
        Some("This computer is recording a meeting")
    } else if crate::audio::retranscription::is_retranscription_in_progress()
        || crate::audio::import::is_import_in_progress()
    {
        Some("This computer is transcribing another meeting")
    } else {
        None
    }
}

impl<R: Runtime> LiveTranscriber for MeetilyLive<R> {
    fn check(&self) -> Result<(), LiveError> {
        match busy_reason() {
            Some(reason) => Err(LiveError::Unavailable { retry_after_secs: 30, reason: reason.into() }),
            None => Ok(()),
        }
    }

    fn open(&self, start: LiveStart) -> LiveOpening {
        let app = self.app.clone();
        Box::pin(async move {
            if let Some(reason) = busy_reason() {
                return Err(LiveError::Unavailable { retry_after_secs: 30, reason: reason.into() });
            }
            validate_transcription_model_ready(&app)
                .await
                .map_err(|reason| LiveError::Unavailable { retry_after_secs: 60, reason })?;
            let engine = get_or_init_transcription_engine(&app)
                .await
                .map_err(|reason| LiveError::Unavailable { retry_after_secs: 60, reason })?;
            let vad = ContinuousVadProcessor::new(16_000, LIVE_REDEMPTION_MS)
                .map_err(|e| LiveError::Failed(format!("speech detection: {e}")))?;
            let (name, language) = match &engine {
                // Parakeet detects the language itself.
                TranscriptionEngine::Parakeet(_) => ("parakeet".to_string(), None),
                TranscriptionEngine::Whisper(_) => (
                    "whisper".to_string(),
                    start
                        .language
                        .as_deref()
                        .map(|tag| tag.split('-').next().unwrap_or(tag).to_string())
                        .or_else(crate::get_language_preference_internal),
                ),
                TranscriptionEngine::Provider(p) => (
                    p.provider_name().to_lowercase(),
                    start.language.clone().or_else(crate::get_language_preference_internal),
                ),
            };
            let ready = LiveReady {
                engine: name,
                model: engine.get_current_model().await,
                // Reported only when it is the meeting's language as the phone gave it.
                language: if language.is_some() { start.language.clone() } else { None },
            };
            let sessions = LIVE_SESSIONS.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = app.emit("sync-live-changed", serde_json::json!({ "sessions": sessions }));
            info!("Phone sync: live transcription for meeting {} with {}", start.meeting_id, ready.engine);
            let session = MeetilyLiveSession { engine, vad, language, app: app.clone(), failures: 0 };
            Ok((ready, Box::new(session) as Box<dyn LiveSession>))
        })
    }
}

/// Another handle on the same engine (the variants hold `Arc`s).
fn same_engine(engine: &TranscriptionEngine) -> TranscriptionEngine {
    match engine {
        TranscriptionEngine::Whisper(w) => TranscriptionEngine::Whisper(w.clone()),
        TranscriptionEngine::Parakeet(p) => TranscriptionEngine::Parakeet(p.clone()),
        TranscriptionEngine::Provider(p) => TranscriptionEngine::Provider(p.clone()),
    }
}

/// Silence that ends a speech segment; Meetily's own live path uses the same.
const LIVE_REDEMPTION_MS: u32 = 500;

struct MeetilyLiveSession<R: Runtime> {
    engine: TranscriptionEngine,
    vad: ContinuousVadProcessor,
    /// For Whisper; Parakeet detects the language.
    language: Option<String>,
    app: AppHandle<R>,
    failures: u32,
}

impl<R: Runtime> Drop for MeetilyLiveSession<R> {
    fn drop(&mut self) {
        let sessions = LIVE_SESSIONS.fetch_sub(1, Ordering::SeqCst).saturating_sub(1);
        let _ = self.app.emit("sync-live-changed", serde_json::json!({ "sessions": sessions }));
    }
}

impl<R: Runtime> MeetilyLiveSession<R> {
    async fn transcribe(&mut self, segments: Vec<crate::audio::vad::SpeechSegment>) -> Result<Vec<LiveText>, LiveError> {
        let mut out = Vec::new();
        for segment in segments {
            if segment.samples.len() < 16_000 / 4 {
                continue;
            }
            // Meetily started recording or transcribing: the engine is its again.
            if let Some(reason) = busy_reason() {
                return Err(LiveError::Unavailable { retry_after_secs: 30, reason: reason.into() });
            }
            // The engines compute on the calling thread; keep that off the async threads the sync server uses.
            let engine = same_engine(&self.engine);
            let language = self.language.clone();
            let handle = tokio::runtime::Handle::current();
            let (start_ms, end_ms) = (segment.start_timestamp_ms, segment.end_timestamp_ms);
            let samples = segment.samples;
            let text = tokio::task::spawn_blocking(move || {
                handle.block_on(async move {
                    match &engine {
                        TranscriptionEngine::Whisper(w) => w.transcribe_audio(samples, language).await.map_err(|e| e.to_string()),
                        TranscriptionEngine::Parakeet(p) => p.transcribe_audio(samples).await.map_err(|e| e.to_string()),
                        TranscriptionEngine::Provider(p) => {
                            p.transcribe(samples, language).await.map(|r| r.text).map_err(|e| e.to_string())
                        }
                    }
                })
            })
            .await
            .unwrap_or_else(|e| Err(format!("transcription task: {e}")));
            let segment = crate::audio::vad::SpeechSegment {
                samples: Vec::new(),
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                confidence: 0.0,
            };
            match text {
                Ok(text) => {
                    self.failures = 0;
                    out.push(LiveText {
                        start_sec: segment.start_timestamp_ms / 1000.0,
                        end_sec: segment.end_timestamp_ms / 1000.0,
                        text: text.trim().to_string(),
                        is_final: true,
                    });
                }
                Err(e) => {
                    // One bad segment shouldn't end the session; a broken engine should.
                    log::warn!("Phone sync: live transcription of a segment failed: {e}");
                    self.failures += 1;
                    if self.failures >= 3 {
                        return Err(LiveError::Failed(e));
                    }
                }
            }
        }
        Ok(out)
    }
}

impl<R: Runtime> LiveSession for MeetilyLiveSession<R> {
    fn feed(&mut self, samples: Vec<f32>) -> BoxFuture<'_, Result<Vec<LiveText>, LiveError>> {
        Box::pin(async move {
            let segments = self
                .vad
                .process_audio(&samples)
                .map_err(|e| LiveError::Failed(format!("speech detection: {e}")))?;
            self.transcribe(segments).await
        })
    }

    fn finish(&mut self) -> BoxFuture<'_, Result<Vec<LiveText>, LiveError>> {
        Box::pin(async move {
            let segments = self.vad.flush().map_err(|e| LiveError::Failed(format!("speech detection: {e}")))?;
            self.transcribe(segments).await
        })
    }
}

fn to_message(e: anyhow::Error) -> String {
    format!("{e:#}")
}

#[tauri::command]
pub async fn sync_status(state: State<'_, SyncState>) -> Result<SyncStatus, String> {
    Ok(state.service()?.status().await)
}

#[tauri::command]
pub async fn sync_set_enabled(state: State<'_, SyncState>, enabled: bool) -> Result<SyncStatus, String> {
    state.service()?.set_enabled(enabled).await.map_err(to_message)
}

#[tauri::command]
pub async fn sync_set_name(state: State<'_, SyncState>, name: String) -> Result<SyncStatus, String> {
    state.service()?.set_name(&name).await.map_err(to_message)
}

#[tauri::command]
pub async fn sync_start_pairing(state: State<'_, SyncState>) -> Result<PairingView, String> {
    state.service()?.start_pairing().await.map_err(to_message)
}

#[tauri::command]
pub async fn sync_cancel_pairing(state: State<'_, SyncState>) -> Result<(), String> {
    state.service()?.cancel_pairing().await;
    Ok(())
}

#[tauri::command]
pub async fn sync_devices(state: State<'_, SyncState>) -> Result<Vec<DeviceView>, String> {
    state.service()?.devices().await.map_err(to_message)
}

#[tauri::command]
pub async fn sync_remove_device(state: State<'_, SyncState>, device_id: String) -> Result<(), String> {
    state.service()?.remove_device(&device_id).await.map_err(to_message)
}

#[tauri::command]
pub async fn sync_recently_deleted(state: State<'_, SyncState>) -> Result<Vec<DeletedMeetingView>, String> {
    state.service()?.recently_deleted().await.map_err(to_message)
}

#[tauri::command]
pub async fn sync_restore_meeting(state: State<'_, SyncState>, meeting_id: String) -> Result<(), String> {
    state.service()?.restore_meeting(&meeting_id).await.map_err(to_message)
}

/// Lets the bridge look at Meetily's meetings now instead of at its next check (e.g. after a recording).
#[tauri::command]
pub async fn sync_meetings_changed(state: State<'_, SyncState>) -> Result<(), String> {
    if let Ok(service) = state.service() {
        service.meetings_changed().await;
    }
    Ok(())
}
