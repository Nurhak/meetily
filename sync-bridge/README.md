# Sales Copilot sync bridge

Connects the [sync hub](../sync-hub/README.md) to Meetily's own meetings, so the desktop app and the phones see the same meetings and transcripts over the local network, without a cloud. The Tauri app uses it through `frontend/src-tauri/src/sync.rs` (Tauri commands and events) and the **Settings → Phones** tab (`frontend/src/components/PhoneSyncSettings.tsx`).

## What syncs

| Happens | Result |
|---|---|
| A meeting is recorded or imported in Meetily | It goes to the hub with its title, transcript (as a `final` transcript) and recording, once the recording is complete. Phones see it. The recording is hard-linked into the hub's storage when the disk allows it, so it doesn't take space twice. |
| A phone uploads a meeting | It appears in Meetily like an import: a folder in the recordings folder (Settings → Recordings) with the recording and a `metadata.json`, and the phone's draft transcript as rows. |
| The hub wants a final transcript (phone meetings, "transcribe again" on a phone) | Meetily's retranscription makes it with the engine and model from Settings → Transcription. It waits while Meetily is recording, importing or retranscribing something else, and tries again a minute later. |
| A title or transcript changes on one side | It goes to the other side. If both changed since the last sync, the desktop's version wins. Speakers on segments the user edited in Meetily are kept. |
| A meeting is deleted in Meetily | It is deleted on the phones too, and kept in **Recently deleted** for 30 days. |
| A meeting is deleted on a phone | It is removed from Meetily. Restoring it (Settings → Phones → Recently deleted, or on a phone) brings it back everywhere. |

Each meeting is reconciled on its own: both sides are compared with what was last synced (`bridge.db`, [`links.rs`](src/links.rs)), what changed is copied, and the new state is recorded. Hub events and a scan of Meetily's database every 10 seconds decide which meetings to look at. Meetily's code isn't changed for this; the bridge reads and writes the `meetings` and `transcripts` tables the way Meetily does.

| File | |
|---|---|
| [`src/service.rs`](src/service.rs) | `SyncService`: on/off, computer name, pairing with the QR code as SVG, paired phones, Recently deleted. Settings in `settings.json`. |
| [`src/bridge.rs`](src/bridge.rs) | Reconciling, imports, audio, the transcription hand-off (`Transcriber`). |
| [`src/mapping.rs`](src/mapping.rs) | Meetily ⇄ meeting format (IDs, titles, channel, segments, generations). |
| [`src/meetily.rs`](src/meetily.rs) | Meetily's database and recording folders. |
| [`src/links.rs`](src/links.rs) | What was last synced, per meeting. |

## In the desktop app

Data lives in `<app data>/sync/` (`settings.json`, `bridge.db`, `hub/` with the hub's key, database and phone recordings). Sync is off until the user turns it on in Settings → Phones; it starts once Meetily's database is ready (after onboarding on a first launch).

Tauri commands: `sync_status`, `sync_set_enabled { enabled }`, `sync_set_name { name }`, `sync_start_pairing`, `sync_cancel_pairing`, `sync_devices`, `sync_remove_device { deviceId }`, `sync_recently_deleted`, `sync_restore_meeting { meetingId }`, `sync_meetings_changed`.

Events: `sync-meetings-changed` (the sidebar and an open meeting refresh), `sync-device-paired { deviceId }` (the pairing dialog closes).

macOS: `Info.plist` has `NSLocalNetworkUsageDescription` and `NSBonjourServices` (`_salescopilot._tcp`), so macOS asks for Local Network access the first time sync runs. Windows asks to allow the app through the firewall.

## Not done yet

- Speaker detection: Meetily's retranscription has no diarization, so transcripts made here have no speakers.
- Meetily's summaries and notes don't sync (the meeting format has no field for them yet).
- The bridge notices new recordings within 10 seconds; Meetily doesn't call `sync_meetings_changed` after saving a recording yet.

## Tests

```sh
cargo test -p salescopilot-sync-bridge
```

The end-to-end tests (`tests/bridge.rs`) create a Meetily database with Meetily's own migrations, run a hub on localhost and act as a phone over HTTPS: desktop recordings reaching phones (also unfinished ones), phone meetings arriving in Meetily and getting a final transcript, edits both ways (including a phone edit during transcription), deleting and restoring both ways, busy and failed transcription, and turning the service on and off with pairing.
