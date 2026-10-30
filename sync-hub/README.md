# Sales Copilot sync hub

The desktop app's side of the Sales Copilot **local-network sync**: phones (iOS, Android) pair with the computer by QR code and sync meetings, transcripts and recordings with it directly, with no cloud in between. The computer is the **hub**: it keeps every meeting, makes the final transcript with speaker detection, and sends it back to the phones.

The protocol is specified in the [SalesMeetingCopilot](https://github.com/Nurhak/SalesMeetingCopilot) repository under `spec/`: `meeting-format.md`, `sync-protocol.md` and `sync-api.openapi.yaml`. The JSON Schemas are vendored in [`spec/schemas/`](spec/schemas/) (from SalesMeetingCopilot commit `949e6bf`); copy them again when the spec changes.

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
            // None: the transcript was edited since it was queued; edits are never replaced automatically.
            let Some(ticket) = hub.begin_transcription(&meeting_id).await? else { continue };
            // transcribe ticket.audio_path (Parakeet/Whisper + diarization), reporting progress:
            hub.set_transcription_progress(&meeting_id, 0.5).await?;
            match hub.finish_transcription(ticket, transcript_json).await? {
                FinishOutcome::Saved => {}
                FinishOutcome::Skipped => { /* the user changed the transcript meanwhile; theirs stays */ }
                FinishOutcome::MeetingDeleted => {}
            }
            // On an engine error: hub.fail_transcription(&meeting_id, "…").await?
        }
        HubEvent::MeetingChanged { .. } | HubEvent::TranscriptChanged { .. } => { /* refresh the UI / Meetily's database */ }
        _ => {}
    }
}
```

Paired devices: `hub.devices()`, `hub.remove_device(id)`, `hub.replace_devices(new, &[old])`. Recently deleted: `hub.restore_meeting(id)`. "Transcribe again" (replacing the user's edits, after asking): `hub.begin_retranscription(id)`. Transcription that was queued or running when the app quit is queued again when the hub opens.

## Hardening

- Connections: at most 128 in total and 16 per address (`HubConfig`), closed after 90 s of silence; TLS handshake and request headers must arrive within 10 s; request bodies within 120 s; HTTP/1.1 only.
- Large bodies (transcripts, upload parts) are handled four at a time; schema validation runs off the async threads.
- Uploads: one unfinished upload per meeting and device (a different file replaces the earlier attempt), at most four per device, and only with 1 GiB of disk left after the file. The MP4 reader is bounded (depth, box count, sizes) and runs with a time limit outside the database lock.
- Pairing a deviceId that is already paired needs that device's current token, so nobody can take over another phone's identity.
- Every logical write is one SQLite transaction together with its change-feed sequence number.

## Not done yet

The desktop app runs the hub through [`sync-bridge`](../sync-bridge/README.md), which maps Meetily's meetings to the meeting format in both directions and feeds `TranscriptionWanted` into Meetily's retranscription. Still open:

- Speaker detection for transcripts made on the computer, and the Symphonia-based prober for formats other than WAV and M4A.
- Keep the hub's private key in the Keychain (macOS) and DPAPI (Windows); it is a file readable only by the owner for now (`identity/key.pem`).
- Windows installer: firewall rule for Private and Domain networks; the Public-network prompt.

## Tests

```sh
cargo test -p salescopilot-sync-hub    # unit tests + end-to-end tests: a hub on localhost and clients that act like phones
cargo clippy -p salescopilot-sync-hub --all-targets
```

The end-to-end tests (`tests/sync_e2e.rs`) pin the hub's key like the phones do and cover pairing (including limits and re-pairing), preconditions and conflicts, hub-owned fields, schema leniency, gzip and size limits, transcripts and generations, resumable uploads with checksums, downloads with `Range`/`If-Range`, the change feed with paging, long polling and full sync, scope and permissions, deletion and restore, device replacement, desktop meetings, and the transcription hand-off including edits that must not be overwritten.

The crate is a member of the root workspace; building and testing it doesn't build the Tauri app.
