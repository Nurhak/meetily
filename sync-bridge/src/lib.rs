//! Phone sync for the Meetily desktop app (Sales Copilot).
//!
//! [`sync_hub`] is the local-network sync server; this crate connects it to Meetily's own meetings
//! ([`bridge`]) and gives the app one type to drive it ([`SyncService`]). The Tauri commands in
//! `frontend/src-tauri/src/sync.rs` are thin wrappers around [`SyncService`].

pub mod bridge;
pub mod links;
pub mod mapping;
pub mod meetily;
pub mod service;

pub use bridge::{Bridge, BridgeConfig, BridgeEvent, JobOutcome, TranscribeError, TranscribeJob, Transcriber};
pub use service::{DeletedMeetingView, DeviceView, PairingView, ServiceConfig, SyncService, SyncStatus};
pub use sync_hub;
