//! End-to-end: Meetily's real database schema (its migrations), a running sync hub, a client acting as a phone,
//! and a stand-in for Meetily's retranscription.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};
use sync_bridge::sync_hub::pinning::PinnedKeyVerifier;
use sync_bridge::sync_hub::{Hub, HubConfig, RunningHub};
use sync_bridge::{Bridge, BridgeConfig, BridgeEvent, JobOutcome, TranscribeError, TranscribeJob, Transcriber};
use tokio::sync::mpsc;

// MARK: harness

/// What the stand-in for Meetily's retranscription does next.
#[derive(Clone)]
enum Behaviour {
    Write(Vec<(&'static str, f64, f64)>),
    Busy,
    Fail(&'static str),
}

struct FakeTranscriber {
    pool: SqlitePool,
    behaviour: Mutex<Behaviour>,
    calls: Mutex<Vec<(String, PathBuf, Option<String>)>>,
    /// Runs while "transcribing" (e.g. a phone editing the transcript meanwhile).
    during: Mutex<Option<BoxFuture<'static, ()>>>,
}

impl Transcriber for FakeTranscriber {
    fn transcribe(&self, job: TranscribeJob) -> BoxFuture<'static, Result<(), TranscribeError>> {
        self.calls.lock().unwrap().push((job.meetily_meeting_id.clone(), job.folder.clone(), job.language.clone()));
        let behaviour = self.behaviour.lock().unwrap().clone();
        let during = self.during.lock().unwrap().take();
        let pool = self.pool.clone();
        Box::pin(async move {
            if let Some(f) = during {
                f.await;
            }
            match behaviour {
                Behaviour::Busy => Err(TranscribeError::Busy),
                Behaviour::Fail(m) => Err(TranscribeError::Failed(m.to_string())),
                Behaviour::Write(rows) => {
                    // Like Meetily's retranscription: replace the rows with new IDs.
                    sqlx::query("DELETE FROM transcripts WHERE meeting_id = ?")
                        .bind(&job.meetily_meeting_id)
                        .execute(&pool)
                        .await
                        .unwrap();
                    for (text, start, end) in rows {
                        insert_row(&pool, &job.meetily_meeting_id, &format!("transcript-{}", uuid::Uuid::new_v4()), text, start, end).await;
                    }
                    Ok(())
                }
            }
        })
    }
}

struct Env {
    _dir: tempfile::TempDir,
    root: PathBuf,
    meetily: SqlitePool,
    hub: Hub,
    server: RunningHub,
    bridge: Bridge,
    ui: mpsc::UnboundedReceiver<BridgeEvent>,
    transcriber: Arc<FakeTranscriber>,
}

async fn insert_row(pool: &SqlitePool, meeting_id: &str, id: &str, text: &str, start: f64, end: f64) {
    sqlx::query(
        "INSERT INTO transcripts (id, meeting_id, transcript, timestamp, audio_start_time, audio_end_time, duration)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(meeting_id)
    .bind(text)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(start)
    .bind(end)
    .bind(end - start)
    .execute(pool)
    .await
    .unwrap();
}

async fn setup() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    // Meetily's database, created by Meetily's own migrations.
    let meetily = SqlitePoolOptions::new()
        .max_connections(4)
        .connect(&format!("sqlite://{}?mode=rwc", root.join("meeting_minutes.sqlite").display()))
        .await
        .unwrap();
    let migrations = Path::new(env!("CARGO_MANIFEST_DIR")).join("../frontend/src-tauri/migrations");
    sqlx::migrate::Migrator::new(migrations).await.unwrap().run(&meetily).await.unwrap();

    let mut config = HubConfig::new(root.join("sync/hub"), "Test MacBook", "macos", "0.4.1");
    config.listen = vec!["127.0.0.1:0".parse().unwrap()];
    config.advertise = false;
    let (hub, events) = Hub::open(config).await.unwrap();
    let server = hub.serve().await.unwrap();
    let transcriber = Arc::new(FakeTranscriber {
        pool: meetily.clone(),
        behaviour: Mutex::new(Behaviour::Write(vec![("Goedemiddag, welkom.", 0.4, 1.6), ("We hebben het over de verlenging.", 1.7, 3.0)])),
        calls: Mutex::new(Vec::new()),
        during: Mutex::new(None),
    });
    let (ui_tx, ui) = mpsc::unbounded_channel();
    let recordings = root.join("meetily-recordings");
    let bridge = Bridge::start(
        hub.clone(),
        events,
        meetily.clone(),
        BridgeConfig {
            state_dir: root.join("sync"),
            recordings_dir: Arc::new(move || recordings.clone()),
            scan_interval: Duration::from_secs(3600),
            busy_retry: Duration::from_millis(10),
            background: false,
        },
        transcriber.clone(),
        ui_tx,
    )
    .await
    .unwrap();
    Env { _dir: dir, root, meetily, hub, server, bridge, ui, transcriber }
}

impl Env {
    /// A meeting as Meetily's recorder leaves it: folder with audio and completed metadata, then the database rows.
    async fn record_on_desktop(&self, title: &str, rows: &[(&str, f64, f64)], completed: bool) -> (String, PathBuf) {
        let id = format!("meeting-{}", uuid::Uuid::new_v4());
        let folder = self.root.join("meetily-recordings").join(format!("{title}_2026-10-28_09-00"));
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("audio.mp4"), b"not really an mp4, just bytes to share").unwrap();
        let status = if completed { "completed" } else { "recording" };
        std::fs::write(
            folder.join("metadata.json"),
            json!({ "version": "1.0", "meeting_id": null, "meeting_name": title, "created_at": "2026-10-28T09:00:31Z",
                    "completed_at": "2026-10-28T09:41:02Z", "duration_seconds": 2431.5,
                    "devices": { "microphone": "MacBook Pro Microphone", "system_audio": "Teams" },
                    "audio_file": "audio.mp4", "transcript_file": "transcripts.json", "sample_rate": 48000, "status": status })
            .to_string(),
        )
        .unwrap();
        let now = chrono::Utc::now();
        sqlx::query("INSERT INTO meetings (id, title, created_at, updated_at, folder_path) VALUES (?, ?, ?, ?, ?)")
            .bind(&id)
            .bind(title)
            .bind(now)
            .bind(now)
            .bind(folder.to_string_lossy().to_string())
            .execute(&self.meetily)
            .await
            .unwrap();
        for (text, start, end) in rows {
            insert_row(&self.meetily, &id, &format!("transcript-{}", uuid::Uuid::new_v4()), text, *start, *end).await;
        }
        (id, folder)
    }

    async fn meetily_title(&self, id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT title FROM meetings WHERE id = ?").bind(id).fetch_optional(&self.meetily).await.unwrap()
    }

    async fn meetily_folder(&self, id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT folder_path FROM meetings WHERE id = ?").bind(id).fetch_optional(&self.meetily).await.unwrap().flatten()
    }

    async fn meetily_rows(&self, id: &str) -> Vec<(String, String, f64, f64)> {
        sqlx::query("SELECT id, transcript, audio_start_time, audio_end_time FROM transcripts WHERE meeting_id = ? ORDER BY audio_start_time")
            .bind(id)
            .fetch_all(&self.meetily)
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect()
    }

    fn saw_meetings_changed(&mut self) -> bool {
        let mut saw = false;
        while let Ok(e) = self.ui.try_recv() {
            saw |= e == BridgeEvent::MeetingsChanged;
        }
        saw
    }
}

struct Phone {
    client: reqwest::Client,
    base: String,
    token: String,
    device_id: String,
}

impl Phone {
    async fn pair(env: &Env) -> Phone {
        let client = reqwest::Client::builder()
            .use_preconfigured_tls(PinnedKeyVerifier::client_config(env.hub.fingerprint()))
            .build()
            .unwrap();
        let base = format!("https://127.0.0.1:{}", env.server.port());
        let code = env.hub.start_pairing().code;
        let device_id = uuid::Uuid::new_v4().to_string();
        let r: Value = client
            .post(format!("{base}/v1/pair"))
            .json(&json!({ "code": code, "device": { "deviceId": device_id, "deviceName": "Nurhak's iPhone", "platform": "ios" } }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        Phone { client, base, token: r["token"].as_str().unwrap().to_string(), device_id }
    }

    async fn send(&self, method: Method, path: &str, headers: &[(&str, &str)], body: Option<Value>) -> (StatusCode, Option<String>, Value) {
        let mut req = self.client.request(method, format!("{}{}", self.base, path)).bearer_auth(&self.token);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let res = req.send().await.unwrap();
        let status = res.status();
        let etag = res.headers().get("etag").map(|v| v.to_str().unwrap().to_string());
        let body = res.json().await.unwrap_or(Value::Null);
        (status, etag, body)
    }

    async fn get(&self, path: &str) -> (StatusCode, Option<String>, Value) {
        self.send(Method::GET, path, &[], None).await
    }

    /// An in-person meeting recorded on the phone, with its on-device draft transcript and recording.
    async fn record(&self, title: Option<&str>) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let meeting = json!({
            "formatVersion": 1, "id": id, "revision": 0, "updatedAt": "2026-10-29T13:04:10Z", "updatedBy": self.device_id,
            "title": title, "startedAt": "2026-10-29T12:30:05Z", "endedAt": "2026-10-29T13:02:51Z", "status": "ended",
            "languageCode": "nl-NL",
            "recordedBy": { "deviceId": self.device_id, "deviceName": "Nurhak's iPhone", "platform": "ios" },
            "capture": { "channel": "in-person", "trigger": "manual", "audioSources": ["microphone"] }
        });
        let (s, ..) = self.send(Method::PUT, &format!("/v1/meetings/{id}"), &[("if-none-match", "*")], Some(meeting)).await;
        assert_eq!(s, StatusCode::CREATED);
        let draft = json!({
            "formatVersion": 1, "meetingId": id, "generation": uuid::Uuid::new_v4().to_string(), "revision": 0, "status": "draft",
            "updatedAt": "2026-10-29T13:04:10Z", "updatedBy": self.device_id,
            "producedBy": { "device": { "deviceId": self.device_id, "platform": "ios" }, "engine": "apple-speechanalyzer", "createdAt": "2026-10-29T13:04:10Z" },
            "languageCode": "nl", "speakers": [],
            "segments": [
                { "id": "s0001", "startSec": 0.5, "endSec": 1.5, "text": "Goedemiddag." },
                { "id": "s0002", "startSec": 1.6, "endSec": 2.8, "text": "Graag." }
            ]
        });
        let (s, ..) = self.send(Method::PUT, &format!("/v1/meetings/{id}/transcript"), &[("if-none-match", "*")], Some(draft)).await;
        assert_eq!(s, StatusCode::CREATED);
        self.upload(&id, &wav(2.0)).await;
        id
    }

    async fn upload(&self, id: &str, data: &[u8]) {
        let sha = { use sha2::Digest; hex::encode(sha2::Sha256::digest(data)) };
        let (s, _, start) = self
            .send(Method::POST, &format!("/v1/meetings/{id}/audio/uploads"), &[],
                  Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": data.len(), "sha256": sha })))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{start}");
        let upload = start["uploadId"].as_str().unwrap();
        let res = self
            .client
            .patch(format!("{}/v1/uploads/{upload}", self.base))
            .bearer_auth(&self.token)
            .header("upload-offset", "0")
            .header("content-type", "application/octet-stream")
            .body(data.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(res.headers()["upload-complete"], "true");
    }
}

fn wav(seconds: f64) -> Vec<u8> {
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

// MARK: tests

#[tokio::test]
async fn a_desktop_recording_is_shared_with_phones() {
    let env = setup().await;
    let (id, folder) = env.record_on_desktop("Teams call Acme", &[("Merhaba, beni duyabiliyor musunuz?", 2.0, 6.4), ("Evet.", 6.9, 8.0)], true).await;
    env.bridge.sync_all().await.unwrap();

    let uuid = id.strip_prefix("meeting-").unwrap();
    let (meeting, transcript) = env.hub.local_meeting(uuid).await.unwrap().expect("in the hub");
    assert_eq!(meeting["title"], "Teams call Acme");
    assert_eq!(meeting["capture"]["channel"], "online-call");
    assert_eq!(meeting["capture"]["audioSources"], json!(["microphone", "system"]));
    assert_eq!(meeting["startedAt"], "2026-10-28T09:00:31.000Z");
    assert_eq!(meeting["recordedBy"]["deviceId"], env.hub.hub_id());
    assert_eq!(meeting["audio"]["fileName"], "audio.mp4");
    let transcript = transcript.expect("transcript");
    assert_eq!(transcript["status"], "final");
    assert_eq!(transcript["segments"][0]["text"], "Merhaba, beni duyabiliyor musunuz?");
    assert!(transcript["segments"][0]["id"].as_str().unwrap().starts_with("transcript-"));

    // The recording is hard-linked, not copied.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let hub_audio = env.hub.local_audio_path(uuid).await.unwrap().unwrap();
        assert_eq!(std::fs::metadata(&hub_audio).unwrap().ino(), std::fs::metadata(folder.join("audio.mp4")).unwrap().ino());
    }

    // Phones see it, and nothing is transcribed again.
    let phone = Phone::pair(&env).await;
    let (s, _, m) = phone.get(&format!("/v1/meetings/{uuid}")).await;
    assert_eq!((s, m["hubState"]["transcription"].as_str()), (StatusCode::OK, Some("none")));
    assert!(env.bridge.run_transcriptions().await.unwrap().is_empty());
    // Running again changes nothing.
    let rev = m["revision"].clone();
    env.bridge.sync_all().await.unwrap();
    assert_eq!(phone.get(&format!("/v1/meetings/{uuid}")).await.2["revision"], rev);
}

#[tokio::test]
async fn an_unfinished_recording_is_shared_without_audio_until_it_is_complete() {
    let env = setup().await;
    let (id, folder) = env.record_on_desktop("Still saving", &[("Hallo.", 0.0, 1.0)], false).await;
    env.bridge.sync_all().await.unwrap();
    let uuid = id.strip_prefix("meeting-").unwrap();
    assert_eq!(env.hub.local_meeting(uuid).await.unwrap().unwrap().0["audio"], Value::Null);
    let meta = std::fs::read_to_string(folder.join("metadata.json")).unwrap().replace("\"recording\"", "\"completed\"");
    std::fs::write(folder.join("metadata.json"), meta).unwrap();
    env.bridge.sync_all().await.unwrap();
    assert_eq!(env.hub.local_meeting(uuid).await.unwrap().unwrap().0["audio"]["fileName"], "audio.mp4");
}

#[tokio::test]
async fn a_phone_meeting_arrives_in_meetily_and_gets_a_final_transcript() {
    let mut env = setup().await;
    let phone = Phone::pair(&env).await;
    let uuid = phone.record(None).await;
    env.bridge.sync_all().await.unwrap();

    let id = format!("meeting-{uuid}");
    let title = env.meetily_title(&id).await.expect("in Meetily");
    assert!(title.starts_with("Nurhak's iPhone, 2026-10-29 "), "{title}");
    let rows = env.meetily_rows(&id).await;
    assert_eq!(rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(), vec![format!("sync-{uuid}-s0001"), format!("sync-{uuid}-s0002")]);
    assert_eq!(rows[0].1, "Goedemiddag.");
    let folder = PathBuf::from(env.meetily_folder(&id).await.unwrap());
    assert!(folder.starts_with(env.root.join("meetily-recordings")));
    assert_eq!(std::fs::read(folder.join("audio.wav")).unwrap(), wav(2.0));
    let meta: Value = serde_json::from_slice(&std::fs::read(folder.join("metadata.json")).unwrap()).unwrap();
    assert_eq!((meta["status"].as_str(), meta["source"].as_str(), meta["audio_file"].as_str()), (Some("completed"), Some("phone"), Some("audio.wav")));
    assert!(env.saw_meetings_changed());

    // The hub asked for a final transcript: Meetily makes it.
    assert_eq!(env.bridge.run_transcriptions().await.unwrap(), vec![JobOutcome::Done]);
    let calls = env.transcriber.calls.lock().unwrap().clone();
    assert_eq!(calls, vec![(id.clone(), folder.clone(), Some("nl".to_string()))]);
    let (_, _, t) = phone.get(&format!("/v1/meetings/{uuid}/transcript")).await;
    assert_eq!((t["status"].as_str(), t["languageCode"].as_str()), (Some("final"), Some("nl-NL")));
    assert_eq!(t["segments"][0]["text"], "Goedemiddag, welkom.");
    assert_eq!(phone.get(&format!("/v1/meetings/{uuid}")).await.2["hubState"]["transcription"], "done");
    assert_eq!(env.meetily_rows(&id).await[0].1, "Goedemiddag, welkom.");

    // In step: another pass changes nothing on either side.
    let rev = t["revision"].clone();
    env.bridge.sync_all().await.unwrap();
    assert_eq!(phone.get(&format!("/v1/meetings/{uuid}/transcript")).await.2["revision"], rev);
}

#[tokio::test]
async fn titles_and_transcripts_follow_edits_on_either_side() {
    let env = setup().await;
    let phone = Phone::pair(&env).await;
    let uuid = phone.record(Some("Acme")).await;
    env.bridge.sync_all().await.unwrap();
    env.bridge.run_transcriptions().await.unwrap();
    let id = format!("meeting-{uuid}");

    // Renamed on the phone: Meetily follows.
    let (_, etag, mut m) = phone.get(&format!("/v1/meetings/{uuid}")).await;
    m["title"] = json!("Acme – verlenging");
    let (s, ..) = phone.send(Method::PUT, &format!("/v1/meetings/{uuid}"), &[("if-match", &etag.unwrap())], Some(m)).await;
    assert_eq!(s, StatusCode::OK);
    env.bridge.sync_all().await.unwrap();
    assert_eq!(env.meetily_title(&id).await.unwrap(), "Acme – verlenging");

    // Renamed in Meetily: the phone follows.
    sqlx::query("UPDATE meetings SET title = 'Acme renewal 2027' WHERE id = ?").bind(&id).execute(&env.meetily).await.unwrap();
    env.bridge.sync_all().await.unwrap();
    assert_eq!(phone.get(&format!("/v1/meetings/{uuid}")).await.2["title"], "Acme renewal 2027");

    // A speaker named on the phone survives a text correction made in Meetily.
    let (_, etag, mut t) = phone.get(&format!("/v1/meetings/{uuid}/transcript")).await;
    t["speakers"] = json!([{ "id": "S1", "name": "Ayşe", "edited": true }]);
    t["segments"][0]["speakerId"] = json!("S1");
    let (s, ..) = phone.send(Method::PUT, &format!("/v1/meetings/{uuid}/transcript"), &[("if-match", &etag.unwrap())], Some(t)).await;
    assert_eq!(s, StatusCode::OK);
    env.bridge.sync_all().await.unwrap();
    let first = env.meetily_rows(&id).await[0].0.clone();
    sqlx::query("UPDATE transcripts SET transcript = 'Goedemiddag allemaal, welkom.' WHERE id = ?").bind(&first).execute(&env.meetily).await.unwrap();
    env.bridge.sync_all().await.unwrap();
    let (_, _, t) = phone.get(&format!("/v1/meetings/{uuid}/transcript")).await;
    assert_eq!(t["segments"][0]["text"], "Goedemiddag allemaal, welkom.");
    assert_eq!((t["segments"][0]["edited"].as_bool(), t["segments"][0]["speakerId"].as_str()), (Some(true), Some("S1")));
    assert_eq!(t["speakers"][0]["name"], "Ayşe");

    // Corrected on the phone: Meetily follows.
    let (_, etag, mut t) = phone.get(&format!("/v1/meetings/{uuid}/transcript")).await;
    t["segments"][1]["text"] = json!("Het gaat over de verlenging.");
    t["segments"][1]["edited"] = json!(true);
    phone.send(Method::PUT, &format!("/v1/meetings/{uuid}/transcript"), &[("if-match", &etag.unwrap())], Some(t)).await;
    env.bridge.sync_all().await.unwrap();
    assert_eq!(env.meetily_rows(&id).await[1].1, "Het gaat over de verlenging.");
}

#[tokio::test]
async fn deleting_goes_both_ways_and_restoring_brings_meetings_back() {
    let mut env = setup().await;
    let phone = Phone::pair(&env).await;

    // A phone meeting deleted everywhere from the phone leaves Meetily.
    let from_phone = phone.record(Some("From the phone")).await;
    env.bridge.sync_all().await.unwrap();
    let id = format!("meeting-{from_phone}");
    let folder = env.meetily_folder(&id).await.unwrap();
    env.saw_meetings_changed();
    let (s, ..) = phone.send(Method::DELETE, &format!("/v1/meetings/{from_phone}"), &[], None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    env.bridge.sync_all().await.unwrap();
    assert!(env.meetily_title(&id).await.is_none());
    assert!(env.saw_meetings_changed());
    assert!(Path::new(&folder).join("audio.wav").exists(), "the recording folder stays on disk");

    // Restored from Recently deleted: back in Meetily, in the same folder, with its transcript.
    assert_eq!(env.hub.recently_deleted().await.unwrap()[0]["id"], from_phone.as_str());
    env.hub.restore_meeting(&from_phone).await.unwrap();
    env.bridge.sync_all().await.unwrap();
    assert_eq!(env.meetily_title(&id).await.as_deref(), Some("From the phone"));
    assert_eq!(env.meetily_folder(&id).await.unwrap(), folder);
    assert_eq!(env.meetily_rows(&id).await.len(), 2);

    // A meeting deleted in Meetily is deleted everywhere.
    let (desktop, _) = env.record_on_desktop("Desktop call", &[("Hallo.", 0.0, 1.0)], true).await;
    env.bridge.sync_all().await.unwrap();
    let desktop_uuid = desktop.strip_prefix("meeting-").unwrap().to_string();
    for table in ["transcripts", "meetings"] {
        let column = if table == "meetings" { "id" } else { "meeting_id" };
        sqlx::query(&format!("DELETE FROM {table} WHERE {column} = ?")).bind(&desktop).execute(&env.meetily).await.unwrap();
    }
    env.bridge.sync_all().await.unwrap();
    let (s, _, m) = phone.get(&format!("/v1/meetings/{desktop_uuid}")).await;
    assert_eq!(s, StatusCode::OK);
    assert!(m["deletedAt"].is_string());
    // And it stays deleted (no re-import on the next pass).
    env.bridge.sync_all().await.unwrap();
    assert!(env.meetily_title(&desktop).await.is_none());
}

#[tokio::test]
async fn busy_and_failed_transcriptions() {
    let env = setup().await;
    let phone = Phone::pair(&env).await;
    let uuid = phone.record(None).await;
    env.bridge.sync_all().await.unwrap();

    *env.transcriber.behaviour.lock().unwrap() = Behaviour::Busy;
    assert_eq!(env.bridge.run_transcriptions().await.unwrap(), vec![JobOutcome::Busy]);
    assert_eq!(phone.get(&format!("/v1/meetings/{uuid}/transcript")).await.2["status"], "draft");

    *env.transcriber.behaviour.lock().unwrap() = Behaviour::Fail("Retranscription requires Whisper");
    assert_eq!(env.bridge.run_transcriptions().await.unwrap(), vec![JobOutcome::Done]);
    let (_, _, m) = phone.get(&format!("/v1/meetings/{uuid}")).await;
    assert_eq!((m["hubState"]["transcription"].as_str(), m["hubState"]["transcriptionError"].as_str()),
               (Some("failed"), Some("Retranscription requires Whisper")));
    // The phone's draft is still what Meetily shows.
    assert_eq!(env.meetily_rows(&format!("meeting-{uuid}")).await[0].1, "Goedemiddag.");
}

#[tokio::test]
async fn a_phone_edit_during_transcription_wins_in_meetily_too() {
    let env = setup().await;
    let phone = Arc::new(Phone::pair(&env).await);
    let uuid = phone.record(None).await;
    env.bridge.sync_all().await.unwrap();

    // While Meetily transcribes, the user corrects the draft on the phone.
    let p = phone.clone();
    let u = uuid.clone();
    *env.transcriber.during.lock().unwrap() = Some(Box::pin(async move {
        let (_, etag, mut t) = p.get(&format!("/v1/meetings/{u}/transcript")).await;
        t["segments"][0]["text"] = json!("Goedemiddag, Ayşe.");
        t["segments"][0]["edited"] = json!(true);
        let (s, ..) = p.send(Method::PUT, &format!("/v1/meetings/{u}/transcript"), &[("if-match", &etag.unwrap())], Some(t)).await;
        assert_eq!(s, StatusCode::OK);
    }));
    assert_eq!(env.bridge.run_transcriptions().await.unwrap(), vec![JobOutcome::Done]);
    let (_, _, t) = phone.get(&format!("/v1/meetings/{uuid}/transcript")).await;
    assert_eq!((t["status"].as_str(), t["segments"][0]["text"].as_str()), (Some("draft"), Some("Goedemiddag, Ayşe.")));
    let rows = env.meetily_rows(&format!("meeting-{uuid}")).await;
    assert_eq!(rows[0].1, "Goedemiddag, Ayşe.", "Meetily shows the user's version, not the machine's");
    env.bridge.sync_all().await.unwrap();
    assert_eq!(phone.get(&format!("/v1/meetings/{uuid}/transcript")).await.2["segments"][0]["text"], "Goedemiddag, Ayşe.");
}

#[tokio::test]
async fn the_service_turns_sync_on_and_off_and_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let meetily = SqlitePoolOptions::new()
        .connect(&format!("sqlite://{}?mode=rwc", dir.path().join("m.sqlite").display()))
        .await
        .unwrap();
    let migrations = Path::new(env!("CARGO_MANIFEST_DIR")).join("../frontend/src-tauri/migrations");
    sqlx::migrate::Migrator::new(migrations).await.unwrap().run(&meetily).await.unwrap();
    let transcriber = Arc::new(FakeTranscriber {
        pool: meetily.clone(),
        behaviour: Mutex::new(Behaviour::Busy),
        calls: Mutex::new(Vec::new()),
        during: Mutex::new(None),
    });
    let (tx, _rx) = mpsc::unbounded_channel();
    let recordings = dir.path().join("rec");
    let make = || {
        sync_bridge::SyncService::new(
            sync_bridge::ServiceConfig {
                data_dir: dir.path().join("sync"),
                platform: "macos".into(),
                app_version: "0.4.1".into(),
                recordings_dir: Arc::new({ let r = recordings.clone(); move || r.clone() }),
            },
            meetily.clone(),
            transcriber.clone(),
            tx.clone(),
        )
    };
    let service = make();
    let off = service.status().await;
    assert!(!off.enabled && !off.running);
    assert!(service.start_pairing().await.is_err(), "no pairing while off");

    let on = service.set_enabled(true).await.unwrap();
    assert!(on.enabled && on.running && on.port.is_some(), "{on:?}");
    assert_eq!(on.fingerprint.as_ref().map(|f| f.len()), Some(43));
    let pairing = service.start_pairing().await.unwrap();
    assert!(pairing.qr_svg.starts_with("<?xml") || pairing.qr_svg.contains("<svg"));
    assert_eq!(pairing.code.len(), 9);

    let renamed = service.set_name("  Nurhak's MacBook Pro  ").await.unwrap();
    assert_eq!((renamed.name.as_str(), renamed.running), ("Nurhak's MacBook Pro", true));
    assert_eq!(renamed.hub_id, on.hub_id, "same identity after restarting");
    assert!(service.devices().await.unwrap().is_empty());
    assert!(service.recently_deleted().await.unwrap().is_empty());
    service.shutdown().await;

    // Settings survive a restart of the app.
    let again = make();
    let status = again.status().await;
    assert!(status.enabled && !status.running);
    assert_eq!(status.name, "Nurhak's MacBook Pro");
    again.start_if_enabled().await;
    assert!(again.status().await.running);
    assert!(!again.set_enabled(false).await.unwrap().running);
}
