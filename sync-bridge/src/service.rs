//! Everything the desktop app needs for phone sync, behind one type: turning it on and off, the computer's name,
//! pairing (with the QR code as SVG), paired devices, Recently deleted. The Tauri commands are thin wrappers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use sync_hub::{Hub, HubConfig, RunningHub};
use tokio::sync::{mpsc, Mutex};

use crate::bridge::{Bridge, BridgeConfig, BridgeEvent, Transcriber};

pub struct ServiceConfig {
    /// The app's data directory for sync (keys, database, phone recordings).
    pub data_dir: PathBuf,
    /// `macos` or `windows`.
    pub platform: String,
    pub app_version: String,
    /// Meetily's recordings folder; meetings from phones get their folders there.
    pub recordings_dir: Arc<dyn Fn() -> PathBuf + Send + Sync>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Settings {
    enabled: bool,
    name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub enabled: bool,
    pub running: bool,
    pub name: String,
    pub hub_id: Option<String>,
    /// Phones pin this key; shown so the user can compare when something looks wrong.
    pub fingerprint: Option<String>,
    pub port: Option<u16>,
    pub addresses: Vec<String>,
    pub paired_devices: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PairingView {
    /// e.g. `7KQ2-M9XD`
    pub code: String,
    /// e.g. `689 088`; equals what the phone shows when pairing without the camera.
    pub verification: String,
    pub qr_uri: String,
    pub qr_svg: String,
    pub expires_in_secs: u64,
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceView {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub app_version: Option<String>,
    pub scope: String,
    pub paired_at: String,
    pub last_seen_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeletedMeetingView {
    pub meeting_id: String,
    pub title: String,
    pub deleted_at: String,
    pub recorded_on: Option<String>,
}

struct Running {
    hub: Hub,
    server: RunningHub,
    bridge: Bridge,
}

struct State {
    settings: Settings,
    running: Option<Running>,
    error: Option<String>,
}

pub struct SyncService {
    config: ServiceConfig,
    meetily: SqlitePool,
    transcriber: Arc<dyn Transcriber>,
    ui: mpsc::UnboundedSender<BridgeEvent>,
    state: Mutex<State>,
}

impl SyncService {
    /// Reads the saved settings; nothing runs until [`SyncService::start_if_enabled`] or
    /// [`SyncService::set_enabled`].
    pub fn new(
        config: ServiceConfig,
        meetily: SqlitePool,
        transcriber: Arc<dyn Transcriber>,
        ui: mpsc::UnboundedSender<BridgeEvent>,
    ) -> Self {
        let settings = std::fs::read(config.data_dir.join("settings.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Settings>(&b).ok())
            .unwrap_or_else(|| Settings { enabled: false, name: default_computer_name() });
        Self {
            config,
            meetily,
            transcriber,
            ui,
            state: Mutex::new(State { settings, running: None, error: None }),
        }
    }

    fn save_settings(&self, settings: &Settings) -> Result<()> {
        std::fs::create_dir_all(&self.config.data_dir)?;
        let path = self.config.data_dir.join("settings.json");
        let tmp = self.config.data_dir.join(".settings.json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(settings)?)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    pub async fn start_if_enabled(&self) {
        let mut state = self.state.lock().await;
        if state.settings.enabled && state.running.is_none() {
            self.start_locked(&mut state).await;
        }
    }

    async fn start_locked(&self, state: &mut State) {
        match self.start_hub(&state.settings.name).await {
            Ok(running) => {
                state.running = Some(running);
                state.error = None;
            }
            Err(e) => {
                tracing::error!(target: "sync_bridge", "phone sync didn't start: {e:#}");
                state.error = Some(format!("{e:#}"));
            }
        }
    }

    async fn start_hub(&self, name: &str) -> Result<Running> {
        let hub_config = HubConfig::new(
            self.config.data_dir.join("hub"),
            name,
            self.config.platform.clone(),
            self.config.app_version.clone(),
        );
        let (hub, events) = Hub::open(hub_config).await.context("opening the sync store")?;
        let server = hub.serve().await.context("starting the sync server")?;
        let bridge = Bridge::start(
            hub.clone(),
            events,
            self.meetily.clone(),
            BridgeConfig {
                state_dir: self.config.data_dir.clone(),
                recordings_dir: self.config.recordings_dir.clone(),
                scan_interval: Duration::from_secs(10),
                busy_retry: Duration::from_secs(60),
                background: true,
            },
            self.transcriber.clone(),
            self.ui.clone(),
        )
        .await?;
        Ok(Running { hub, server, bridge })
    }

    async fn stop_locked(&self, state: &mut State) {
        if let Some(running) = state.running.take() {
            running.bridge.stop();
            running.server.shutdown().await;
            drop(running.hub);
        }
    }

    pub async fn status(&self) -> SyncStatus {
        let state = self.state.lock().await;
        let mut status = SyncStatus {
            enabled: state.settings.enabled,
            running: state.running.is_some(),
            name: state.settings.name.clone(),
            hub_id: None,
            fingerprint: None,
            port: None,
            addresses: Vec::new(),
            paired_devices: 0,
            error: state.error.clone(),
        };
        if let Some(r) = &state.running {
            status.hub_id = Some(r.hub.hub_id().to_string());
            status.fingerprint = Some(r.hub.fingerprint().to_string());
            status.port = Some(r.server.port());
            status.addresses = r.hub.reachable_addresses().iter().map(|a| a.to_string()).collect();
            status.paired_devices = r.hub.devices().await.map(|d| d.len()).unwrap_or(0);
        }
        status
    }

    pub async fn set_enabled(&self, enabled: bool) -> Result<SyncStatus> {
        {
            let mut state = self.state.lock().await;
            state.settings.enabled = enabled;
            self.save_settings(&state.settings)?;
            if enabled && state.running.is_none() {
                self.start_locked(&mut state).await;
            } else if !enabled {
                self.stop_locked(&mut state).await;
                state.error = None;
            }
        }
        Ok(self.status().await)
    }

    /// The name phones show for this computer. Restarts sync when it is running (the name is advertised).
    pub async fn set_name(&self, name: &str) -> Result<SyncStatus> {
        let name: String = name.trim().chars().filter(|c| !c.is_control()).take(63).collect();
        anyhow::ensure!(!name.is_empty(), "the name can't be empty");
        {
            let mut state = self.state.lock().await;
            state.settings.name = name;
            self.save_settings(&state.settings)?;
            if state.running.is_some() {
                self.stop_locked(&mut state).await;
                self.start_locked(&mut state).await;
            }
        }
        Ok(self.status().await)
    }

    async fn hub(&self) -> Result<Hub> {
        let state = self.state.lock().await;
        state.running.as_ref().map(|r| r.hub.clone()).context("phone sync is off")
    }

    pub async fn start_pairing(&self) -> Result<PairingView> {
        let hub = self.hub().await?;
        let info = hub.start_pairing();
        Ok(PairingView {
            qr_svg: qr_svg(&info.qr_uri)?,
            code: info.code,
            verification: info.verification,
            qr_uri: info.qr_uri,
            expires_in_secs: info.expires_in_secs,
            addresses: hub.reachable_addresses().iter().map(|a| a.to_string()).collect(),
        })
    }

    pub async fn cancel_pairing(&self) {
        if let Ok(hub) = self.hub().await {
            hub.cancel_pairing();
        }
    }

    pub async fn devices(&self) -> Result<Vec<DeviceView>> {
        let hub = self.hub().await?;
        Ok(hub
            .devices()
            .await?
            .into_iter()
            .map(|d| DeviceView {
                name: d.device_name.clone().unwrap_or_else(|| d.platform.clone()),
                device_id: d.device_id,
                platform: d.platform,
                app_version: d.app_version,
                scope: d.scope,
                paired_at: d.paired_at,
                last_seen_at: d.last_seen_at,
            })
            .collect())
    }

    pub async fn remove_device(&self, device_id: &str) -> Result<()> {
        self.hub().await?.remove_device(device_id).await?;
        Ok(())
    }

    pub async fn recently_deleted(&self) -> Result<Vec<DeletedMeetingView>> {
        let hub = self.hub().await?;
        Ok(hub
            .recently_deleted()
            .await?
            .iter()
            .map(|m| DeletedMeetingView {
                meeting_id: m["id"].as_str().unwrap_or_default().to_string(),
                title: crate::mapping::display_title(m),
                deleted_at: m["deletedAt"].as_str().unwrap_or_default().to_string(),
                recorded_on: m.pointer("/recordedBy/deviceName").and_then(Value::as_str).map(str::to_string),
            })
            .collect())
    }

    /// Restores a meeting everywhere; it comes back in Meetily and on the phones.
    pub async fn restore_meeting(&self, meeting_id: &str) -> Result<()> {
        self.hub().await?.restore_meeting(meeting_id).await
    }

    /// Checks Meetily's meetings now (e.g. right after a recording was saved).
    pub async fn meetings_changed(&self) {
        let state = self.state.lock().await;
        if let Some(r) = &state.running {
            r.bridge.meetily_changed();
        }
    }

    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        self.stop_locked(&mut state).await;
    }
}

/// The pairing URI as an SVG QR code.
pub fn qr_svg(uri: &str) -> Result<String> {
    let code = qrcode::QrCode::with_error_correction_level(uri.as_bytes(), qrcode::EcLevel::M)?;
    Ok(code
        .render::<qrcode::render::svg::Color>()
        .min_dimensions(240, 240)
        .quiet_zone(true)
        .build())
}

/// What phones call this computer by default: the name the user gave it.
pub fn default_computer_name() -> String {
    let from_command = |cmd: &str, args: &[&str]| {
        std::process::Command::new(cmd)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
    };
    let name = if cfg!(target_os = "macos") {
        from_command("scutil", &["--get", "ComputerName"])
    } else if cfg!(target_os = "windows") {
        std::env::var("COMPUTERNAME").ok()
    } else {
        from_command("hostname", &[])
    };
    name.map(|n| n.chars().take(63).collect()).unwrap_or_else(|| "Sales Copilot".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qr_codes_render_as_svg() {
        let svg = qr_svg("salescopilot://pair?v=1&hid=6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11&n=Nurhak%27s%20MacBook&a=192.168.1.10%3A47821&fp=U8XDRA0Y33XOTCMNDjhDJQqqhc5DhzLBNCl1tHKMtAE&c=7KQ2M9XD").unwrap();
        assert!(svg.contains("<svg") && svg.contains("</svg>"));
        assert!(!default_computer_name().is_empty());
    }
}
