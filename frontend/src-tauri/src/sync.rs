//! Phone sync (Sales Copilot): the computer is the hub, phones pair by QR code and sync meetings and
//! transcripts over the local network, without a cloud. The work happens in the `sync-bridge` crate; this
//! module starts it once the database is ready, lets it use Meetily's retranscription for meetings recorded
//! on phones, and exposes it to the UI as Tauri commands and events.
//!
//! Events for the UI:
//! - `sync-meetings-changed`: meetings were added, changed or removed by sync; refresh the list.
//! - `sync-device-paired` (`{ deviceId }`): a phone just paired; close the pairing dialog.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use log::{error, info};
use serde::Serialize;
use sqlx::SqlitePool;
use sync_bridge::{
    BridgeEvent, DeletedMeetingView, DeviceView, PairingView, ServiceConfig, SyncService, SyncStatus,
    TranscribeError, TranscribeJob, Transcriber,
};
use tauri::{AppHandle, Emitter, Manager, Runtime, State};
use tauri_plugin_store::StoreExt;
use tokio::sync::{mpsc, OnceCell};

use crate::audio::recording_preferences::{get_default_recordings_folder, RecordingPreferences};
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
            // One transcription at a time, and never while recording: try again later.
            if crate::audio::recording_commands::is_recording().await
                || crate::audio::retranscription::is_retranscription_in_progress()
                || crate::audio::import::is_import_in_progress()
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
