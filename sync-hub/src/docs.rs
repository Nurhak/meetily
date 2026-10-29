//! Rules for meeting.json and transcript.json that the hub applies: hub-owned fields, ID case, and the equality
//! used to recognise a PUT that changes nothing (meeting-format.md "Conventions", sync-protocol.md "Revisions").

use serde_json::{Map, Value};

/// Fields of meeting.json the hub owns or that never change after creation.
pub const MEETING_HUB_OWNED: &[&str] = &[
    "formatVersion",
    "id",
    "revision",
    "updatedAt",
    "updatedBy",
    "recordedBy",
    "audio",
    "deletedAt",
    "hubState",
];

/// Fields of transcript.json the hub owns.
pub const TRANSCRIPT_HUB_OWNED: &[&str] = &[
    "formatVersion",
    "meetingId",
    "generation",
    "status",
    "producedBy",
    "revision",
    "updatedAt",
    "updatedBy",
];

pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn lower(v: &mut Value, pointer: &str) {
    if let Some(Value::String(s)) = v.pointer_mut(pointer) {
        if s.chars().any(|c| c.is_ascii_uppercase()) {
            *s = s.to_ascii_lowercase();
        }
    }
}

/// Readers lowercase the UUIDs they receive (Swift writes them in upper case).
pub fn lowercase_meeting_ids(doc: &mut Value) {
    for p in ["/id", "/updatedBy", "/recordedBy/deviceId"] {
        lower(doc, p);
    }
}

pub fn lowercase_transcript_ids(doc: &mut Value) {
    for p in [
        "/meetingId",
        "/generation",
        "/updatedBy",
        "/producedBy/device/deviceId",
    ] {
        lower(doc, p);
    }
    if let Some(Value::Array(speakers)) = doc.get_mut("speakers") {
        for s in speakers {
            lower(s, "/personId");
        }
    }
}

pub fn lowercase_uuid(s: &str) -> String {
    s.to_ascii_lowercase()
}

/// A copy without the given top-level fields.
pub fn without(doc: &Value, fields: &[&str]) -> Value {
    let mut out = doc.clone();
    if let Value::Object(map) = &mut out {
        for f in fields {
            map.remove(*f);
        }
    }
    out
}

/// Normal form for comparing documents as JSON: null and absent are the same, a missing `edited` is false, missing
/// `samples`/`words` are empty, and numbers compare by value (3.1 == 3.10, 1 == 1.0). Key order never matters.
pub fn normalized(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                let drop = val.is_null()
                    || (k == "edited" && val == &Value::Bool(false))
                    || ((k == "samples" || k == "words")
                        && val.as_array().is_some_and(|a| a.is_empty()));
                if !drop {
                    out.insert(k.clone(), normalized(val));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(normalized).collect()),
        Value::Number(n) => n.as_f64().map(Value::from).unwrap_or_else(|| v.clone()),
        other => other.clone(),
    }
}

pub fn same_user_content(a: &Value, b: &Value, hub_owned: &[&str]) -> bool {
    normalized(&without(a, hub_owned)) == normalized(&without(b, hub_owned))
}

/// True when a user changed any segment or speaker (`edited: true`).
pub fn has_edits(transcript: &Value) -> bool {
    let edited = |key: &str| {
        transcript
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .any(|i| i.get("edited").and_then(Value::as_bool).unwrap_or(false))
            })
    };
    edited("segments") || edited("speakers")
}

pub fn str_field<'a>(doc: &'a Value, key: &str) -> Option<&'a str> {
    doc.get(key).and_then(Value::as_str)
}

pub fn set(doc: &mut Value, key: &str, value: Value) {
    if let Value::Object(map) = doc {
        map.insert(key.to_string(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn equality_ignores_order_absent_vs_null_and_number_form() {
        let a = json!({"title": "A", "languageCode": null, "x": 3.1, "segments": [{"id": "s1", "edited": false, "words": []}]});
        let b = json!({"segments": [{"id": "s1"}], "x": 3.10, "title": "A"});
        assert_eq!(normalized(&a), normalized(&b));
        assert_ne!(normalized(&json!({"n": 1})), normalized(&json!({"n": 2})));
        assert_eq!(normalized(&json!({"n": 1})), normalized(&json!({"n": 1.0})));
    }

    #[test]
    fn hub_owned_fields_do_not_count() {
        let a = json!({"id": "x", "revision": 1, "title": "T", "hubState": {"transcription": "running"}});
        let b = json!({"id": "x", "revision": 7, "title": "T", "updatedBy": "y"});
        assert!(same_user_content(&a, &b, MEETING_HUB_OWNED));
        assert!(!same_user_content(
            &a,
            &json!({"title": "U"}),
            MEETING_HUB_OWNED
        ));
    }

    #[test]
    fn edits_and_ids() {
        assert!(!has_edits(
            &json!({"segments": [{"id": "s1"}], "speakers": []})
        ));
        assert!(has_edits(
            &json!({"segments": [], "speakers": [{"id": "S1", "edited": true}]})
        ));
        let mut m =
            json!({"id": "6F1D2C7E-4B8A-4F1E-9C3D-2A5B7E9F0C11", "recordedBy": {"deviceId": "AB"}});
        lowercase_meeting_ids(&mut m);
        assert_eq!(m["id"], "6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11");
        assert_eq!(m["recordedBy"]["deviceId"], "ab");
    }
}
