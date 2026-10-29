//! Validates incoming documents against the spec's JSON Schemas with the *reader profile*: open enums
//! (`x-extensible-enum`) accept unknown values, so documents from newer apps are not rejected
//! (meeting-format.md, "Conventions"). The schemas are vendored from the SalesMeetingCopilot repository (spec/schemas).

use jsonschema::{Draft, Resource, Validator};
use serde_json::Value;

const COMMON: &str = include_str!("../spec/schemas/common.schema.json");
const MEETING: &str = include_str!("../spec/schemas/meeting.schema.json");
const TRANSCRIPT: &str = include_str!("../spec/schemas/transcript.schema.json");

pub struct Schemas {
    meeting: Validator,
    transcript: Validator,
}

impl Schemas {
    pub fn load() -> anyhow::Result<Self> {
        let common = lenient(serde_json::from_str(COMMON)?);
        let common_id = common["$id"].as_str().unwrap_or_default().to_string();
        let build = |text: &str| -> anyhow::Result<Validator> {
            let schema = lenient(serde_json::from_str(text)?);
            jsonschema::options()
                .with_draft(Draft::Draft202012)
                .with_resource(common_id.clone(), Resource::from_contents(common.clone())?)
                .build(&schema)
                .map_err(|e| anyhow::anyhow!("invalid schema: {e}"))
        };
        Ok(Self {
            meeting: build(MEETING)?,
            transcript: build(TRANSCRIPT)?,
        })
    }

    pub fn check_meeting(&self, doc: &Value) -> Result<(), String> {
        check(&self.meeting, doc)
    }

    pub fn check_transcript(&self, doc: &Value) -> Result<(), String> {
        check(&self.transcript, doc)
    }
}

fn check(validator: &Validator, doc: &Value) -> Result<(), String> {
    let errors: Vec<String> = validator
        .iter_errors(doc)
        .take(5)
        .map(|e| format!("{}: {}", e.instance_path, e))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Drops `enum` wherever the schema marks it `x-extensible-enum`.
fn lenient(node: Value) -> Value {
    match node {
        Value::Object(map) => {
            let extensible = map
                .get("x-extensible-enum")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if extensible && k == "enum" {
                    continue;
                }
                out.insert(k, lenient(v));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(lenient).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meeting() -> Value {
        json!({
            "formatVersion": 1, "id": "a9c3e5f7-1b2d-4e6f-8a0b-c2d4e6f8a0b2", "revision": 0,
            "updatedAt": "2026-10-28T09:47:15Z", "updatedBy": "5a2e9c71-0d4f-4b38-a6e2-71f0c3b9d842",
            "startedAt": "2026-10-28T09:00:31Z", "status": "ended",
            "recordedBy": { "deviceId": "5a2e9c71-0d4f-4b38-a6e2-71f0c3b9d842", "platform": "windows" },
            "capture": { "channel": "online-call", "trigger": "meeting-detection", "meetingApp": "teams" }
        })
    }

    #[test]
    fn open_enums_accept_new_values_closed_enums_do_not() {
        let s = Schemas::load().unwrap();
        assert!(s.check_meeting(&meeting()).is_ok());
        let mut newer = meeting();
        newer["capture"]["meetingApp"] = json!("facetime");
        newer["recordedBy"]["platform"] = json!("visionos");
        newer["someFutureField"] = json!({ "x": 1 });
        assert!(s.check_meeting(&newer).is_ok());
        let mut bad = meeting();
        bad["status"] = json!("paused");
        assert!(s.check_meeting(&bad).is_err());
        let mut bad = meeting();
        bad["id"] = json!("not-a-uuid");
        assert!(s.check_meeting(&bad).unwrap_err().contains("/id"));
    }
}
