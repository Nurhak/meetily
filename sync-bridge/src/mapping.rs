//! Converting between Meetily's meetings and the shared meeting format (SalesMeetingCopilot `spec/meeting-format.md`).
//! Pure functions, so the rules are easy to test.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::meetily::{FolderInfo, MeetilyMeeting, MeetilyRow};

pub fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
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

/// Meetily meeting IDs are `meeting-<uuid>`; the shared format uses the UUID.
pub fn meeting_uuid(meetily_id: &str) -> Option<String> {
    meetily_id.strip_prefix("meeting-").filter(|u| is_uuid(u)).map(str::to_ascii_lowercase)
}

pub fn meetily_id(uuid: &str) -> String {
    format!("meeting-{uuid}")
}

/// Meetily row IDs are unique across all meetings; segment IDs only within a transcript. Meetily's own rows
/// (`transcript-<uuid>`) keep their ID; segments from phones get a meeting-specific prefix, so the round trip is stable.
pub fn row_id(uuid: &str, segment_id: &str) -> String {
    if segment_id.starts_with("transcript-") {
        segment_id.to_string()
    } else {
        format!("sync-{uuid}-{segment_id}")
    }
}

pub fn segment_id(uuid: &str, row_id: &str) -> String {
    let id = row_id.strip_prefix(&format!("sync-{uuid}-")).unwrap_or(row_id);
    // Segment IDs are at most 64 safe characters (spec); anything else gets a stable stand-in.
    if !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        id.to_string()
    } else {
        format!("m{}", &hex::encode(Sha256::digest(id.as_bytes()))[..32])
    }
}

pub fn fingerprint_title(title: &str) -> String {
    hex::encode(Sha256::digest(title.as_bytes()))
}

pub fn fingerprint_rows(rows: &[MeetilyRow]) -> String {
    let mut h = Sha256::new();
    for r in rows {
        h.update(r.id.as_bytes());
        h.update([0]);
        h.update(format!("{:?}|{:?}", r.start, r.end).as_bytes());
        h.update([0]);
        h.update(r.text.as_bytes());
        h.update([1]);
    }
    hex::encode(h.finalize())
}

/// The title Meetily shows for a meeting without one ("title null = apps show the start date").
pub fn display_title(meeting: &Value) -> String {
    if let Some(t) = meeting["title"].as_str().map(str::trim).filter(|t| !t.is_empty()) {
        return t.to_string();
    }
    let started = meeting["startedAt"]
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default();
    let device = meeting.pointer("/recordedBy/deviceName").and_then(Value::as_str).unwrap_or("");
    match (device.is_empty(), started.is_empty()) {
        (false, false) => format!("{device}, {started}"),
        (true, false) => format!("Meeting {started}"),
        _ => "Meeting".to_string(),
    }
}

pub fn started_at(meeting: &Value) -> DateTime<Utc> {
    meeting["startedAt"]
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}

/// meeting.json for a meeting recorded or imported in Meetily on this computer.
pub fn desktop_meeting_doc(uuid: &str, m: &MeetilyMeeting, folder: &FolderInfo, rows: &[MeetilyRow], hub_device: &Value) -> Value {
    // Meetily stores the meeting when the recording stops, so created_at is the end unless metadata.json says more.
    let last_speech = rows.iter().filter_map(|r| r.end).fold(0.0f64, f64::max);
    let ended = folder.ended_at.unwrap_or(m.created_at);
    let started = folder.started_at.unwrap_or_else(|| {
        let length = folder.duration_seconds.unwrap_or(last_speech);
        ended - chrono::Duration::milliseconds((length * 1000.0) as i64)
    });
    let (channel, trigger) = if folder.imported { ("file-import", "import") } else if folder.has_system_audio { ("online-call", "manual") } else { ("in-person", "manual") };
    let mut sources = Vec::new();
    if folder.has_microphone || !folder.imported {
        sources.push("microphone");
    }
    if folder.has_system_audio {
        sources.push("system");
    }
    json!({
        "formatVersion": 1,
        "id": uuid,
        "revision": 0,
        "updatedAt": ts(Utc::now()),
        "updatedBy": hub_device["deviceId"],
        "title": m.title,
        "startedAt": ts(started.min(ended)),
        "endedAt": ts(ended),
        "status": "ended",
        "languageCode": null,
        "recordedBy": hub_device,
        "capture": { "channel": channel, "meetingApp": null, "trigger": trigger, "audioSources": sources },
        "audio": null,
        "deletedAt": null
    })
}

/// transcript.json from Meetily's rows.
///
/// With a `base` (the hub's current transcript) that shares segment IDs with the rows, the result is the same
/// generation: unchanged segments keep everything the hub had (speakers, words, …), changed ones are marked `edited`.
/// Otherwise (no base, or Meetily transcribed again and every ID is new) it is a new `final` generation.
pub fn transcript_from_rows(
    uuid: &str,
    rows: &[MeetilyRow],
    base: Option<&Value>,
    hub_device: &Value,
    engine: &str,
    model: Option<&str>,
) -> Value {
    let segments_from_rows: Vec<(String, f64, f64, &str)> = rows
        .iter()
        .map(|r| {
            let start = r.start.unwrap_or(0.0).max(0.0);
            let end = r.end.unwrap_or(start).max(start);
            (segment_id(uuid, &r.id), start, end, r.text.trim())
        })
        .collect();
    let base_segments: Vec<&Value> = base.and_then(|b| b["segments"].as_array()).map(|a| a.iter().collect()).unwrap_or_default();
    let overlaps = segments_from_rows.iter().any(|(id, ..)| base_segments.iter().any(|s| s["id"].as_str() == Some(id)));

    if let (Some(base), true) = (base, overlaps) {
        let mut doc = base.clone();
        let segments: Vec<Value> = segments_from_rows
            .iter()
            .map(|(id, start, end, text)| match base_segments.iter().find(|s| s["id"].as_str() == Some(id)) {
                Some(old) => {
                    let mut seg = (*old).clone();
                    let text_changed = old["text"].as_str() != Some(*text);
                    let times_changed = (old["startSec"].as_f64().unwrap_or(-1.0) - start).abs() > 1e-6
                        || (old["endSec"].as_f64().unwrap_or(-1.0) - end).abs() > 1e-6;
                    if text_changed || times_changed {
                        seg["text"] = json!(text);
                        seg["startSec"] = json!(start);
                        seg["endSec"] = json!(end);
                        seg["edited"] = json!(true);
                        if text_changed {
                            if let Value::Object(map) = &mut seg {
                                map.remove("words");
                            }
                        }
                    }
                    seg
                }
                None => json!({ "id": id, "startSec": start, "endSec": end, "text": text, "edited": true }),
            })
            .collect();
        doc["segments"] = json!(segments);
        return doc;
    }

    json!({
        "formatVersion": 1,
        "meetingId": uuid,
        "generation": uuid::Uuid::new_v4().to_string(),
        "revision": 0,
        "status": "final",
        "updatedAt": ts(Utc::now()),
        "updatedBy": hub_device["deviceId"],
        "producedBy": {
            "device": hub_device,
            "engine": engine,
            "model": model,
            "diarization": null,
            "createdAt": ts(Utc::now())
        },
        "languageCode": base.and_then(|b| b.get("languageCode")).cloned().unwrap_or(Value::Null),
        "speakers": [],
        "segments": segments_from_rows
            .iter()
            .map(|(id, start, end, text)| json!({ "id": id, "startSec": start, "endSec": end, "text": text }))
            .collect::<Vec<_>>()
    })
}

/// Meetily rows for a transcript from the hub. Meetily doesn't show speakers, so the text stays exactly as spoken;
/// speakers stay in the hub's transcript and survive edits made on the desktop (see [`transcript_from_rows`]).
pub fn rows_from_transcript(uuid: &str, transcript: &Value) -> Vec<MeetilyRow> {
    transcript["segments"]
        .as_array()
        .map(|segments| {
            segments
                .iter()
                .filter_map(|s| {
                    let id = s["id"].as_str()?;
                    let start = s["startSec"].as_f64().unwrap_or(0.0);
                    Some(MeetilyRow {
                        id: row_id(uuid, id),
                        text: s["text"].as_str().unwrap_or_default().to_string(),
                        start: Some(start),
                        end: Some(s["endSec"].as_f64().unwrap_or(start)),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11";

    fn device() -> Value {
        json!({ "deviceId": "c4d18e60-7f2b-4a95-b3c1-9e6a0d5f2e17", "deviceName": "MacBook", "platform": "macos" })
    }

    fn row(id: &str, start: f64, end: f64, text: &str) -> MeetilyRow {
        MeetilyRow { id: id.into(), text: text.into(), start: Some(start), end: Some(end) }
    }

    #[test]
    fn ids_round_trip() {
        assert_eq!(meeting_uuid(&format!("meeting-{}", UUID.to_uppercase())).as_deref(), Some(UUID));
        assert_eq!(meeting_uuid("meeting-123"), None);
        assert_eq!(meeting_uuid(UUID), None);
        for seg in ["s0001", "transcript-0b6f3d2a-91c4-4e7a-8f15-3c2d9e8a7b61", "a_b-C"] {
            assert_eq!(segment_id(UUID, &row_id(UUID, seg)), seg);
        }
        let odd = segment_id(UUID, "weird id/with spaces");
        assert!(odd.starts_with('m') && odd.len() == 33);
        assert_eq!(odd, segment_id(UUID, "weird id/with spaces"), "stable");
    }

    #[test]
    fn titles() {
        assert_eq!(display_title(&json!({ "title": "  Acme  " })), "Acme");
        let t = display_title(&json!({ "title": null, "startedAt": "2026-10-29T12:30:05Z", "recordedBy": { "deviceName": "iPhone" } }));
        assert!(t.starts_with("iPhone, 2026-10-29 "), "{t}");
        assert_eq!(display_title(&json!({})), "Meeting");
    }

    #[test]
    fn desktop_meeting_from_recording_and_import() {
        let created = DateTime::parse_from_rfc3339("2026-10-28T09:41:02Z").unwrap().with_timezone(&Utc);
        let m = MeetilyMeeting { id: format!("meeting-{UUID}"), title: "Team call".into(), created_at: created, folder_path: None };
        let rows = vec![row("transcript-a", 2.0, 6.4, "Merhaba"), row("transcript-b", 6.9, 600.0, "Evet")];
        let folder = FolderInfo { has_microphone: true, has_system_audio: true, completed: true, ..Default::default() };
        let doc = desktop_meeting_doc(UUID, &m, &folder, &rows, &device());
        assert_eq!(doc["capture"]["channel"], "online-call");
        assert_eq!(doc["capture"]["audioSources"], json!(["microphone", "system"]));
        assert_eq!(doc["endedAt"], "2026-10-28T09:41:02.000Z");
        assert_eq!(doc["startedAt"], "2026-10-28T09:31:02.000Z", "end minus the last speech");
        assert_eq!(doc["recordedBy"], device());
        let imported = FolderInfo { imported: true, ..Default::default() };
        let doc = desktop_meeting_doc(UUID, &m, &imported, &[], &device());
        assert_eq!((doc["capture"]["channel"].as_str(), doc["capture"]["trigger"].as_str()), (Some("file-import"), Some("import")));
    }

    #[test]
    fn transcript_edits_keep_speakers_and_retranscription_starts_a_generation() {
        let base = json!({
            "formatVersion": 1, "meetingId": UUID, "generation": "8e0a4b1c-2d3f-4a5b-9c6d-7e8f9a0b1c2d", "revision": 3, "status": "final",
            "speakers": [{ "id": "S1", "name": "Ayşe" }],
            "segments": [
                { "id": "s0001", "startSec": 0.5, "endSec": 1.5, "text": "Goedemiddag.", "speakerId": "S1", "words": [{ "text": "Goedemiddag.", "startSec": 0.5, "endSec": 1.5 }] },
                { "id": "s0002", "startSec": 1.6, "endSec": 2.8, "text": "Graag.", "speakerId": "S1" }
            ]
        });
        let rows = rows_from_transcript(UUID, &base);
        assert_eq!(rows[0].id, format!("sync-{UUID}-s0001"));
        // Unchanged rows give back exactly the hub's transcript.
        assert_eq!(transcript_from_rows(UUID, &rows, Some(&base), &device(), "whisper", None), base);
        // An edit marks the segment, drops its word timings and keeps its speaker.
        let mut edited = rows.clone();
        edited[0].text = "Goedemiddag allemaal.".into();
        edited.pop();
        let doc = transcript_from_rows(UUID, &edited, Some(&base), &device(), "whisper", None);
        assert_eq!(doc["generation"], base["generation"]);
        assert_eq!(doc["segments"].as_array().unwrap().len(), 1);
        assert_eq!(doc["segments"][0]["edited"], true);
        assert_eq!(doc["segments"][0]["speakerId"], "S1");
        assert!(doc["segments"][0].get("words").is_none());
        assert_eq!(doc["speakers"], base["speakers"]);
        // Meetily transcribed again (all IDs new): a new final generation.
        let fresh = vec![row("transcript-x", 0.4, 1.6, "Goedemiddag!")];
        let doc = transcript_from_rows(UUID, &fresh, Some(&base), &device(), "parakeet", Some("parakeet-tdt-0.6b-v3"));
        assert_ne!(doc["generation"], base["generation"]);
        assert_eq!((doc["status"].as_str(), doc["producedBy"]["engine"].as_str()), (Some("final"), Some("parakeet")));
        assert_eq!(doc["segments"][0]["id"], "transcript-x");
    }

    #[test]
    fn fingerprints_notice_every_change() {
        let a = vec![row("t1", 0.0, 1.0, "a")];
        let mut b = a.clone();
        assert_eq!(fingerprint_rows(&a), fingerprint_rows(&b));
        b[0].text = "b".into();
        assert_ne!(fingerprint_rows(&a), fingerprint_rows(&b));
        let mut c = a.clone();
        c[0].end = Some(1.5);
        assert_ne!(fingerprint_rows(&a), fingerprint_rows(&c));
    }
}
