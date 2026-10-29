# Sales Copilot sync hub

The desktop app's side of the Sales Copilot **local-network sync**: phones (iOS, Android) pair with the computer by QR code and sync meetings, transcripts and recordings with it directly, with no cloud in between. The computer is the **hub**: it keeps every meeting, makes the final transcript with speaker detection, and sends it back to the phones.

The protocol is specified in the [SalesMeetingCopilot](https://github.com/Nurhak/SalesMeetingCopilot) repository under `spec/`: `meeting-format.md`, `sync-protocol.md` and `sync-api.openapi.yaml`. The JSON Schemas are vendored in [`spec/schemas/`](spec/schemas/) (from SalesMeetingCopilot commit `3b0cf71`); copy them again when the spec changes.

## What it does

| Spec section | Where |
|---|---|
| Hub certificate and the public-key fingerprint phones pin | `src/identity.rs` |
| Pairing codes, verification number, attempt limits, QR URI | `src/pairing.rs` |
| Local network only | `src/netfilter.rs` |
| DNS-SD `_salescopilot._tcp` | `src/discovery.rs` |
| Devices and tokens, permissions and scope, revisions and ETags, preconditions, transcripts, resumable uploads, downloads, change feed with long polling, deletion and Recently deleted, the transcription hand-off | `src/hub.rs` |
| HTTP routes and body limits | `src/api.rs` |
| Schema validation with the reader profile (open enums) | `src/schema.rs` |
| SQLite (`hub.db`) and audio files | `src/store.rs` |
| Duration, sample rate and channels of uploaded WAV and M4A | `src/probe.rs` |
| Key pinning for Rust clients (used by the tests) | `src/pinning.rs` |

## Using it from the desktop app

```rust
use sync_hub::{Hub, HubConfig, HubEvent, FinishOutcome};

let config = HubConfig::new(app_data_dir.join("sync"), "Nurhak's MacBook", "macos", env!("CARGO_PKG_VERSION"));
let (hub, mut events) = Hub::open(config).await?;
let server = hub.serve().await?;           // HTTPS on port 47821 (or a free one) + DNS-SD

// "Add phone": show info.qr_uri as a QR code, with info.code and info.verification as text.
let info = hub.start_pairing();

// Meetings recorded on this computer go into the hub so phones can see them.
hub.save_local_meeting(meeting_json).await?;
hub.attach_local_audio(&meeting_id, &recording_path).await?;

while let Some(event) = events.recv().await {
    match event {
        HubEvent::TranscriptionWanted { meeting_id } => {
            let ticket = hub.begin_transcription(&meeting_id).await?;
            // transcribe ticket.audio_path (Parakeet/Whisper + diarization), reporting progress:
            hub.set_transcription_progress(&meeting_id, 0.5).await?;
            let outcome = hub.finish_transcription(ticket, transcript_json).await?;
            // FinishOutcome::Skipped: the user edited the transcript meanwhile; the edits were kept.
        }
        HubEvent::MeetingChanged { .. } | HubEvent::TranscriptChanged { .. } => { /* refresh the UI / Meetily's database */ }
        _ => {}
    }
}
```

Paired devices: `hub.devices()`, `hub.remove_device(id)`, `hub.replace_devices(new, &[old])`. Recently deleted: `hub.restore_meeting(id)`.

## Not done yet (desktop integration)

- Depend on this crate from `frontend/src-tauri`, run the hub in the Tauri app, and add Tauri commands and the UI: **Add phone** (QR code, code, verification number), **Paired devices**, **Recently deleted**, sync on/off.
- Map Meetily's database to the meeting format (meeting IDs `meeting-<uuid>`, `transcripts` rows → segments, `mic`/`system` → `audioSource`) in both directions.
- Feed `TranscriptionWanted` into the existing Parakeet/Whisper pipeline, and plug in the Symphonia-based prober for formats other than WAV and M4A.
- Keep the hub's private key in the Keychain (macOS) and DPAPI (Windows); it is a file readable only by the owner for now (`identity/key.pem`).
- Windows installer: firewall rule for Private and Domain networks; the Public-network prompt.

## Tests

```sh
cd sync-hub
cargo test          # unit tests + end-to-end tests: a hub on localhost and clients that act like phones
cargo clippy --all-targets
```

The end-to-end tests (`tests/sync_e2e.rs`) pin the hub's key like the phones do and cover pairing (including limits and re-pairing), preconditions and conflicts, hub-owned fields, schema leniency, gzip and size limits, transcripts and generations, resumable uploads with checksums, downloads with `Range`/`If-Range`, the change feed with paging, long polling and full sync, scope and permissions, deletion and restore, device replacement, desktop meetings, and the transcription hand-off including edits that must not be overwritten.

This crate has its own Cargo workspace for now (the root workspace excludes it), so it builds without the Tauri app's dependencies.
