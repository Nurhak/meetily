//! Test harness: a hub on localhost and clients that act like the phone apps (pinned TLS, bearer tokens).
#![allow(dead_code)]

use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use sync_hub::pinning::PinnedKeyVerifier;
use sync_hub::{Hub, HubConfig, HubEvent, RunningHub};
use tokio::sync::mpsc::UnboundedReceiver;

pub struct TestHub {
    pub hub: Hub,
    pub server: RunningHub,
    pub events: UnboundedReceiver<HubEvent>,
    pub base: String,
    pub dir: tempfile::TempDir,
}

pub async fn start_hub() -> TestHub {
    let dir = tempfile::tempdir().unwrap();
    let mut config = HubConfig::new(dir.path(), "Test MacBook", "macos", "0.0.0-test");
    config.listen = vec!["127.0.0.1:0".parse().unwrap()];
    config.advertise = false;
    let (hub, events) = Hub::open(config).await.unwrap();
    let server = hub.serve().await.unwrap();
    let base = format!("https://127.0.0.1:{}", server.port());
    TestHub {
        hub,
        server,
        events,
        base,
        dir,
    }
}

impl TestHub {
    /// The next event, waiting briefly.
    pub async fn next_event(&mut self) -> Option<HubEvent> {
        tokio::time::timeout(Duration::from_secs(2), self.events.recv())
            .await
            .ok()
            .flatten()
    }

    /// Drains events until `pred` matches one; returns whether it did.
    pub async fn saw(&mut self, pred: impl Fn(&HubEvent) -> bool) -> bool {
        while let Some(e) = self.next_event().await {
            if pred(&e) {
                return true;
            }
        }
        false
    }

    pub fn drain_events(&mut self) -> Vec<HubEvent> {
        let mut out = Vec::new();
        while let Ok(e) = self.events.try_recv() {
            out.push(e);
        }
        out
    }
}

pub struct Reply {
    pub status: StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Value,
    pub bytes: Vec<u8>,
}

impl Reply {
    pub fn code(&self) -> &str {
        self.body
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    pub fn etag(&self) -> String {
        self.header("etag").expect("ETag header")
    }
}

pub struct Phone {
    pub client: reqwest::Client,
    pub base: String,
    pub token: Option<String>,
    pub device_id: String,
    pub name: String,
}

pub fn client(fingerprint: &str) -> reqwest::Client {
    reqwest::Client::builder()
        .use_preconfigured_tls(PinnedKeyVerifier::client_config(fingerprint))
        .timeout(Duration::from_secs(40))
        .build()
        .unwrap()
}

impl Phone {
    pub fn new(hub: &TestHub, name: &str) -> Self {
        Self {
            client: client(hub.hub.fingerprint()),
            base: hub.base.clone(),
            token: None,
            device_id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
        }
    }

    /// Pairs with the hub's current pairing code.
    pub async fn pair(hub: &TestHub, name: &str, scope: Option<&str>) -> Self {
        let mut phone = Self::new(hub, name);
        let info = hub.hub.start_pairing();
        let reply = phone.pair_with(&info.code, scope).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
        phone
    }

    pub async fn pair_with(&mut self, code: &str, scope: Option<&str>) -> Reply {
        let mut body = json!({
            "code": code,
            "device": { "deviceId": self.device_id, "deviceName": self.name, "platform": "ios", "appVersion": "1.4.0" },
        });
        if let Some(s) = scope {
            body["scope"] = json!(s);
        }
        let reply = self.send(Method::POST, "/v1/pair", &[], Some(body)).await;
        if reply.status == StatusCode::CREATED {
            self.token = reply.body["token"].as_str().map(str::to_string);
        }
        reply
    }

    pub async fn send(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> Reply {
        let mut req = self
            .client
            .request(method, format!("{}{}", self.base, path));
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        Self::reply(req.send().await.expect("request")).await
    }

    pub async fn send_bytes(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        bytes: Vec<u8>,
    ) -> Reply {
        let mut req = self
            .client
            .request(method, format!("{}{}", self.base, path))
            .body(bytes);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        Self::reply(req.send().await.expect("request")).await
    }

    async fn reply(res: reqwest::Response) -> Reply {
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.bytes().await.unwrap().to_vec();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Reply {
            status,
            headers,
            body,
            bytes,
        }
    }

    pub async fn get(&self, path: &str) -> Reply {
        self.send(Method::GET, path, &[], None).await
    }

    pub async fn create_meeting(&self, doc: &Value) -> Reply {
        let id = doc["id"].as_str().unwrap();
        self.send(
            Method::PUT,
            &format!("/v1/meetings/{id}"),
            &[("if-none-match", "*")],
            Some(doc.clone()),
        )
        .await
    }

    pub async fn update_meeting(&self, doc: &Value, etag: &str) -> Reply {
        let id = doc["id"].as_str().unwrap();
        self.send(
            Method::PUT,
            &format!("/v1/meetings/{id}"),
            &[("if-match", etag)],
            Some(doc.clone()),
        )
        .await
    }

    pub async fn create_transcript(&self, doc: &Value) -> Reply {
        let id = doc["meetingId"].as_str().unwrap();
        self.send(
            Method::PUT,
            &format!("/v1/meetings/{id}/transcript"),
            &[("if-none-match", "*")],
            Some(doc.clone()),
        )
        .await
    }

    pub async fn update_transcript(&self, doc: &Value, etag: &str) -> Reply {
        let id = doc["meetingId"].as_str().unwrap();
        self.send(
            Method::PUT,
            &format!("/v1/meetings/{id}/transcript"),
            &[("if-match", etag)],
            Some(doc.clone()),
        )
        .await
    }

    /// Uploads a whole file in pieces of `chunk` bytes; returns the last reply.
    pub async fn upload(
        &self,
        meeting_id: &str,
        file_name: &str,
        mime: &str,
        data: &[u8],
        chunk: usize,
    ) -> Reply {
        let start = self
            .send(
                Method::POST,
                &format!("/v1/meetings/{meeting_id}/audio/uploads"),
                &[],
                Some(json!({ "fileName": file_name, "mimeType": mime, "sizeBytes": data.len(), "sha256": sha256(data) })),
            )
            .await;
        if start.body["complete"] == json!(true) || !start.status.is_success() {
            return start;
        }
        let id = start.body["uploadId"].as_str().unwrap().to_string();
        let mut offset = start.body["offset"].as_u64().unwrap() as usize;
        let mut last = start;
        while offset < data.len() {
            let end = (offset + chunk).min(data.len());
            last = self.append(&id, offset, data[offset..end].to_vec()).await;
            assert_eq!(last.status, StatusCode::NO_CONTENT, "{:?}", last.body);
            offset = last.header("upload-offset").unwrap().parse().unwrap();
        }
        last
    }

    pub async fn append(&self, upload_id: &str, offset: usize, bytes: Vec<u8>) -> Reply {
        let offset = offset.to_string();
        self.send_bytes(
            Method::PATCH,
            &format!("/v1/uploads/{upload_id}"),
            &[
                ("upload-offset", &offset),
                ("content-type", "application/octet-stream"),
            ],
            bytes,
        )
        .await
    }

    pub async fn changes(&self, since: Option<&str>) -> Reply {
        match since {
            Some(s) => self.get(&format!("/v1/changes?since={s}")).await,
            None => self.get("/v1/changes").await,
        }
    }
}

pub fn sha256(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

pub fn meeting_doc(id: &str, recorded_by: &str, status: &str) -> Value {
    json!({
        "formatVersion": 1,
        "id": id,
        "revision": 0,
        "updatedAt": "2026-10-29T13:04:10Z",
        "updatedBy": recorded_by,
        "title": "Acme – contractverlenging",
        "startedAt": "2026-10-29T12:30:05Z",
        "endedAt": if status == "ended" { json!("2026-10-29T13:02:51Z") } else { Value::Null },
        "timeZone": "Europe/Amsterdam",
        "status": status,
        "languageCode": "nl",
        "recordedBy": { "deviceId": recorded_by, "deviceName": "Phone", "platform": "ios", "appVersion": "1.4.0" },
        "capture": { "channel": "in-person", "meetingApp": null, "trigger": "manual", "audioSources": ["microphone"] },
        "audio": null,
        "deletedAt": null
    })
}

pub fn transcript_doc(meeting_id: &str, device_id: &str, generation: &str) -> Value {
    json!({
        "formatVersion": 1,
        "meetingId": meeting_id,
        "generation": generation,
        "revision": 0,
        "status": "draft",
        "updatedAt": "2026-10-29T13:04:10Z",
        "updatedBy": device_id,
        "producedBy": {
            "device": { "deviceId": device_id, "platform": "ios" },
            "engine": "apple-speechanalyzer", "model": null, "diarization": null, "createdAt": "2026-10-29T13:04:10Z"
        },
        "languageCode": "nl",
        "speakers": [],
        "segments": [
            { "id": "s0001", "startSec": 0.5, "endSec": 1.5, "text": "Goedemiddag." },
            { "id": "s0002", "startSec": 1.6, "endSec": 2.8, "text": "Graag, we willen het over de verlenging hebben." }
        ]
    })
}

/// A WAV of silence, 16 kHz mono 16-bit.
pub fn wav(seconds: f64) -> Vec<u8> {
    let data = (16000.0 * seconds) as u32 * 2;
    let mut v = Vec::new();
    v.extend(b"RIFF");
    v.extend((36 + data).to_le_bytes());
    v.extend(b"WAVEfmt ");
    v.extend(16u32.to_le_bytes());
    v.extend(1u16.to_le_bytes());
    v.extend(1u16.to_le_bytes());
    v.extend(16000u32.to_le_bytes());
    v.extend(32000u32.to_le_bytes());
    v.extend(2u16.to_le_bytes());
    v.extend(16u16.to_le_bytes());
    v.extend(b"data");
    v.extend(data.to_le_bytes());
    v.extend((0..data).map(|i| (i % 251) as u8));
    v
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
