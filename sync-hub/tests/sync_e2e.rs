//! End-to-end: a hub on localhost and clients behaving like the phone apps. Each test follows a rule of
//! spec/sync-protocol.md; the section is named in the test.

mod common;

use std::io::Write as _;
use std::time::Duration;

use common::*;
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use sync_hub::pairing::{format_verification, verification_number};
use sync_hub::{FinishOutcome, HubEvent};

// MARK: transport and pairing

#[tokio::test]
async fn hello_needs_no_token_and_the_key_is_pinned() {
    let hub = start_hub().await;
    let phone = Phone::new(&hub, "iPhone");
    let hello = phone.get("/v1/hello").await;
    assert_eq!(hello.status, StatusCode::OK);
    assert_eq!(hello.body["hubId"], hub.hub.hub_id());
    assert_eq!(hello.body["protocolVersions"], json!([1]));
    assert_eq!(hello.body["platform"], "macos");

    // "Transport and security": a different key means "not my hub".
    let wrong = client("U8XDRA0Y33XOTCMNDjhDJQqqhc5DhzLBNCl1tHKMtAE");
    assert!(wrong
        .get(format!("{}/v1/hello", hub.base))
        .send()
        .await
        .is_err());
}

#[tokio::test]
async fn pairing_with_qr_code_and_by_hand() {
    let mut hub = start_hub().await;
    let mut phone = Phone::new(&hub, "Nurhak's iPhone");

    // No code on screen.
    let r = phone.pair_with("7KQ2-M9XD", None).await;
    assert_eq!(
        (r.status, r.code()),
        (StatusCode::FORBIDDEN, "pairing_code_invalid")
    );
    assert_eq!(
        r.header("content-type").as_deref(),
        Some("application/problem+json")
    );

    let info = hub.hub.start_pairing();
    assert!(info
        .qr_uri
        .starts_with(&format!("salescopilot://pair?v=1&hid={}", hub.hub.hub_id())));
    assert!(info
        .qr_uri
        .contains(&format!("&fp={}", hub.hub.fingerprint())));
    // "Without a camera": the computer shows the number the phone calculates.
    assert_eq!(
        info.verification,
        format_verification(verification_number(hub.hub.fingerprint(), &info.code))
    );

    let r = phone.pair_with("AAAA-AAAA", None).await;
    assert_eq!(r.code(), "pairing_code_invalid");
    // The typed code is canonicalised: lower case, no dash.
    let typed = info.code.to_lowercase().replace('-', "");
    let r = phone.pair_with(&typed, None).await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.body);
    assert_eq!(r.body["token"].as_str().unwrap().len(), 43);
    assert_eq!(r.body["hub"]["hubId"], hub.hub.hub_id());
    assert_eq!(r.body["device"]["scope"], "all");
    assert!(
        hub.saw(|e| matches!(e, HubEvent::DevicePaired { .. }))
            .await
    );

    // One successful pairing per code.
    let mut other = Phone::new(&hub, "Other");
    assert_eq!(
        other.pair_with(&info.code, None).await.code(),
        "pairing_code_invalid"
    );

    let me = phone.get("/v1/devices/me").await;
    assert_eq!(me.body["deviceId"], phone.device_id);
    assert_eq!(me.body["deviceName"], "Nurhak's iPhone");

    // Pairing the same phone again: the old token stops working.
    let old_token = phone.token.clone();
    let info = hub.hub.start_pairing();
    assert_eq!(
        phone.pair_with(&info.code, None).await.status,
        StatusCode::CREATED
    );
    let mut stale = Phone::new(&hub, "stale");
    stale.token = old_token;
    assert_eq!(stale.get("/v1/devices/me").await.code(), "device_revoked");
    assert_eq!(phone.get("/v1/devices/me").await.status, StatusCode::OK);

    // Removed on the computer.
    hub.hub.remove_device(&phone.device_id).await.unwrap();
    assert_eq!(phone.get("/v1/devices/me").await.code(), "device_revoked");
    stale.token = Some("not-a-token".into());
    assert_eq!(stale.get("/v1/devices/me").await.code(), "unauthorized");
    stale.token = None;
    assert_eq!(stale.get("/v1/changes").await.code(), "unauthorized");
}

#[tokio::test]
async fn failed_pairing_attempts_are_limited_per_address() {
    let hub = start_hub().await;
    let info = hub.hub.start_pairing();
    let mut attacker = Phone::new(&hub, "attacker");
    for _ in 0..5 {
        assert_eq!(
            attacker.pair_with("ZZZZ-ZZZZ", None).await.code(),
            "pairing_code_invalid"
        );
    }
    let r = attacker.pair_with(&info.code, None).await;
    assert_eq!(
        (r.status, r.code()),
        (StatusCode::TOO_MANY_REQUESTS, "rate_limited")
    );
    assert!(r.header("retry-after").is_some());
}

#[tokio::test]
async fn devices_me_update_and_unpair() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "Pixel", Some("own")).await;
    let r = phone
        .send(
            Method::PATCH,
            "/v1/devices/me",
            &[],
            Some(json!({ "scope": "all", "appVersion": "2.0" })),
        )
        .await;
    assert_eq!(
        (r.body["scope"].as_str(), r.body["appVersion"].as_str()),
        (Some("all"), Some("2.0"))
    );
    let r = phone
        .send(
            Method::PATCH,
            "/v1/devices/me",
            &[],
            Some(json!({ "scope": "some" })),
        )
        .await;
    assert_eq!(r.code(), "bad_request");
    assert_eq!(
        phone
            .send(Method::DELETE, "/v1/devices/me", &[], None)
            .await
            .status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(phone.get("/v1/devices/me").await.code(), "device_revoked");
}

// MARK: meetings

#[tokio::test]
async fn meeting_create_update_and_preconditions() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    let mut doc = meeting_doc(&id, &phone.device_id, "ended");

    // "Preconditions"
    let r = phone
        .send(
            Method::PUT,
            &format!("/v1/meetings/{id}"),
            &[],
            Some(doc.clone()),
        )
        .await;
    assert_eq!(
        (r.status, r.code()),
        (StatusCode::PRECONDITION_REQUIRED, "precondition_required")
    );

    // "Permissions": a phone creates only meetings it recorded.
    let r = phone
        .create_meeting(&meeting_doc(&id, &new_id(), "ended"))
        .await;
    assert_eq!(r.code(), "forbidden");

    // Hub-owned fields are the hub's; IDs are lowercased (Swift writes upper case).
    doc["revision"] = json!(41);
    doc["updatedBy"] = json!(new_id());
    doc["audio"] = json!({ "fileName": "audio.m4a", "mimeType": "audio/mp4", "durationSec": 12.5, "sizeBytes": 10, "sha256": "a".repeat(64) });
    doc["hubState"] = json!({ "transcription": "done" });
    let mut upper = doc.clone();
    upper["id"] = json!(id.to_uppercase());
    let r = phone
        .send(
            Method::PUT,
            &format!("/v1/meetings/{}", id.to_uppercase()),
            &[("if-none-match", "*")],
            Some(upper),
        )
        .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.body);
    assert_eq!(r.etag(), "\"m-1\"");
    assert_eq!(r.body["id"], id);
    assert_eq!(r.body["revision"], 1);
    assert_eq!(r.body["updatedBy"], phone.device_id);
    assert_eq!(r.body["audio"], Value::Null);
    assert_eq!(r.body["hubState"]["transcription"], "none");

    // Creating again: 412 with the hub's document.
    let r = phone.create_meeting(&doc).await;
    assert_eq!(r.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        r.header("content-type").as_deref(),
        Some("application/json")
    );
    assert_eq!(r.body["revision"], 1);

    doc["title"] = json!("Acme – verlenging 2027");
    let r = phone.update_meeting(&doc, "\"m-1\"").await;
    assert_eq!(
        (r.status, r.etag()),
        (StatusCode::OK, "\"m-2\"".to_string())
    );
    assert_eq!(r.body["title"], "Acme – verlenging 2027");
    // Stale ETag.
    let r = phone.update_meeting(&doc, "\"m-1\"").await;
    assert_eq!(
        (r.status, r.etag()),
        (StatusCode::PRECONDITION_FAILED, "\"m-2\"".to_string())
    );
    // A PUT that changes nothing (null vs absent, key order) keeps the revision.
    let mut same = doc.clone();
    same.as_object_mut().unwrap().remove("timeZone");
    same["timeZone"] = json!("Europe/Amsterdam");
    same.as_object_mut().unwrap().remove("deletedAt");
    let r = phone.update_meeting(&same, "\"m-2\"").await;
    assert_eq!(
        (r.status, r.etag()),
        (StatusCode::OK, "\"m-2\"".to_string())
    );

    // If-Match for a meeting the hub doesn't have: 404, retry as a create.
    let unknown = meeting_doc(&new_id(), &phone.device_id, "ended");
    assert_eq!(
        phone.update_meeting(&unknown, "\"m-3\"").await.code(),
        "not_found"
    );

    // Validation: closed enums and wrong ids are errors; open enums and unknown fields are not.
    let mut bad = meeting_doc(&new_id(), &phone.device_id, "ended");
    bad["status"] = json!("paused");
    assert_eq!(phone.create_meeting(&bad).await.code(), "bad_request");
    let mut newer = meeting_doc(&new_id(), &phone.device_id, "ended");
    newer["capture"]["meetingApp"] = json!("facetime");
    newer["somethingNew"] = json!({ "kept": true });
    let r = phone.create_meeting(&newer).await;
    assert_eq!(r.status, StatusCode::CREATED);
    assert_eq!(
        r.body["somethingNew"]["kept"], true,
        "unknown properties survive"
    );
    let r = phone
        .send(
            Method::PUT,
            &format!("/v1/meetings/{}", new_id()),
            &[("if-none-match", "*")],
            Some(doc.clone()),
        )
        .await;
    assert_eq!(r.code(), "bad_request");

    let r = phone.get(&format!("/v1/meetings/{id}")).await;
    assert_eq!(
        (r.status, r.etag()),
        (StatusCode::OK, "\"m-2\"".to_string())
    );
    assert_eq!(
        phone
            .get(&format!("/v1/meetings/{}", new_id()))
            .await
            .code(),
        "not_found"
    );
}

#[tokio::test]
async fn bodies_may_be_gzipped_and_limits_apply_after_decompression() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let doc = meeting_doc(&new_id(), &phone.device_id, "ended");
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&serde_json::to_vec(&doc).unwrap()).unwrap();
    let r = phone
        .send_bytes(
            Method::PUT,
            &format!("/v1/meetings/{}", doc["id"].as_str().unwrap()),
            &[
                ("if-none-match", "*"),
                ("content-type", "application/json"),
                ("content-encoding", "gzip"),
            ],
            gz.finish().unwrap(),
        )
        .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.body);

    // 64 KB for meeting.json, measured uncompressed: a small gzip of a huge document is refused.
    let mut big = meeting_doc(&new_id(), &phone.device_id, "ended");
    big["padding"] = json!("x".repeat(200_000));
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    gz.write_all(&serde_json::to_vec(&big).unwrap()).unwrap();
    let compressed = gz.finish().unwrap();
    assert!(compressed.len() < 4096);
    let r = phone
        .send_bytes(
            Method::PUT,
            &format!("/v1/meetings/{}", big["id"].as_str().unwrap()),
            &[
                ("if-none-match", "*"),
                ("content-type", "application/json"),
                ("content-encoding", "gzip"),
            ],
            compressed,
        )
        .await;
    assert_eq!(
        (r.status, r.code()),
        (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large")
    );
}

// MARK: transcripts

#[tokio::test]
async fn transcript_rules() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    let mut meeting = meeting_doc(&id, &phone.device_id, "recording");
    let created = phone.create_meeting(&meeting).await;
    let generation = new_id();
    let mut t = transcript_doc(&id, &phone.device_id, &generation);

    // "Endpoints": transcripts and audio only for ended meetings.
    assert_eq!(
        phone.create_transcript(&t).await.code(),
        "meeting_not_ended"
    );
    meeting["status"] = json!("ended");
    meeting["endedAt"] = json!("2026-10-29T13:02:51Z");
    assert_eq!(
        phone.update_meeting(&meeting, &created.etag()).await.status,
        StatusCode::OK
    );

    let r = phone.create_transcript(&t).await;
    assert_eq!(
        (r.status, r.etag()),
        (StatusCode::CREATED, "\"t-1\"".to_string())
    );
    let etag = r.etag();

    t["segments"][1]["text"] = json!("Graag. We willen het over de verlenging hebben.");
    t["segments"][1]["edited"] = json!(true);
    t["status"] = json!("final"); // hub-owned: ignored
    let r = phone.update_transcript(&t, &etag).await;
    assert_eq!(
        (r.status, r.etag()),
        (StatusCode::OK, "\"t-2\"".to_string())
    );
    assert_eq!(r.body["status"], "draft");
    assert_eq!(r.body["segments"][1]["edited"], true);

    // "Merging": a recorder never replaces the generation the hub has.
    let mut other = t.clone();
    other["generation"] = json!(new_id());
    assert_eq!(
        phone.update_transcript(&other, "\"t-2\"").await.code(),
        "generation_conflict"
    );
    assert_eq!(
        phone.update_transcript(&t, "\"t-1\"").await.status,
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        phone.create_transcript(&t).await.status,
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        phone
            .get(&format!("/v1/meetings/{id}/transcript"))
            .await
            .etag(),
        "\"t-2\""
    );
}

// MARK: audio and the transcription hand-off

#[tokio::test]
async fn resumable_upload_download_and_final_transcript() {
    let mut hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let draft = phone
        .create_transcript(&transcript_doc(&id, &phone.device_id, &new_id()))
        .await;
    assert_eq!(draft.status, StatusCode::CREATED);
    hub.drain_events();

    let audio = wav(3.0);
    let sha = sha256(&audio);
    let path = format!("/v1/meetings/{id}/audio/uploads");
    let start = phone
        .send(Method::POST, &path, &[], Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": audio.len(), "sha256": sha })))
        .await;
    assert_eq!(start.status, StatusCode::CREATED, "{:?}", start.body);
    let upload_id = start.body["uploadId"].as_str().unwrap().to_string();
    let half = audio.len() / 2;

    let r = phone.append(&upload_id, 0, audio[..half].to_vec()).await;
    assert_eq!(
        (
            r.status,
            r.header("upload-offset").unwrap(),
            r.header("upload-complete").unwrap()
        ),
        (
            StatusCode::NO_CONTENT,
            half.to_string(),
            "false".to_string()
        )
    );
    // Wrong offset: 409 with where to continue.
    let r = phone.append(&upload_id, 10, audio[10..20].to_vec()).await;
    assert_eq!(
        (r.status, r.code()),
        (StatusCode::CONFLICT, "upload_offset_mismatch")
    );
    assert_eq!(r.header("upload-offset").unwrap(), half.to_string());
    // After a restart: HEAD, or POST again for the same checksum.
    let head = phone
        .send(Method::HEAD, &format!("/v1/uploads/{upload_id}"), &[], None)
        .await;
    assert_eq!(head.header("upload-offset").unwrap(), half.to_string());
    let again = phone
        .send(Method::POST, &path, &[], Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": audio.len(), "sha256": sha })))
        .await;
    assert_eq!(
        (again.status, again.body["uploadId"].as_str()),
        (StatusCode::OK, Some(upload_id.as_str()))
    );
    assert_eq!(again.body["offset"], half);

    let r = phone.append(&upload_id, half, audio[half..].to_vec()).await;
    assert_eq!(r.header("upload-complete").as_deref(), Some("true"));
    let head = phone
        .send(Method::HEAD, &format!("/v1/uploads/{upload_id}"), &[], None)
        .await;
    assert_eq!(head.header("upload-complete").as_deref(), Some("true"));

    // meeting.json → audio is filled in by the hub (probed duration), with a new revision.
    let m = phone.get(&format!("/v1/meetings/{id}")).await;
    assert_eq!(m.etag(), "\"m-2\"");
    assert_eq!(m.body["audio"]["sha256"], sha);
    assert_eq!(m.body["audio"]["durationSec"], 3.0);
    assert_eq!(m.body["audio"]["sampleRate"], 16000);
    assert_eq!(m.body["hubState"]["audioReceived"], true);
    assert_eq!(m.body["hubState"]["transcription"], "queued");
    assert!(
        hub.saw(|e| matches!(e, HubEvent::TranscriptionWanted { meeting_id } if *meeting_id == id))
            .await
    );

    // The same file again is already there; a different one is refused.
    let r = phone
        .upload(&id, "audio.wav", "audio/wav", &audio, 1 << 20)
        .await;
    assert_eq!(
        (r.status, &r.body),
        (StatusCode::OK, &json!({ "complete": true }))
    );
    let r = phone
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    assert_eq!(r.code(), "audio_exists");

    // Download, whole and resumed with If-Range.
    let full = phone.get(&format!("/v1/meetings/{id}/audio")).await;
    assert_eq!(
        (full.status, full.bytes.len()),
        (StatusCode::OK, audio.len())
    );
    assert_eq!(full.header("etag").unwrap(), format!("\"{sha}\""));
    let etag = format!("\"{sha}\"");
    let part = phone
        .send(
            Method::GET,
            &format!("/v1/meetings/{id}/audio"),
            &[("range", "bytes=100-"), ("if-range", &etag)],
            None,
        )
        .await;
    assert_eq!(part.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(part.bytes, audio[100..]);
    let stale = phone
        .send(
            Method::GET,
            &format!("/v1/meetings/{id}/audio"),
            &[("range", "bytes=100-"), ("if-range", "\"other\"")],
            None,
        )
        .await;
    assert_eq!(
        (stale.status, stale.bytes.len()),
        (StatusCode::OK, audio.len())
    );

    // The desktop app transcribes and saves the final transcript.
    let ticket = hub
        .hub
        .begin_transcription(&id)
        .await
        .unwrap()
        .expect("transcription starts");
    assert!(ticket.audio_path.ends_with("audio.wav"));
    assert_eq!(ticket.language_code.as_deref(), Some("nl"));
    hub.hub.set_transcription_progress(&id, 0.5).await.unwrap();
    let running = phone.get(&format!("/v1/meetings/{id}")).await;
    assert_eq!(
        (
            running.body["hubState"]["transcription"].as_str(),
            running.body["hubState"]["transcriptionProgress"].as_f64()
        ),
        (Some("running"), Some(0.5))
    );
    let mut final_doc = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    final_doc["status"] = json!("final");
    final_doc["producedBy"] = json!({
        "device": { "deviceId": hub.hub.hub_id(), "platform": "macos" },
        "engine": "parakeet", "model": "parakeet-tdt-0.6b-v3",
        "diarization": { "engine": "sortformer", "model": null }, "createdAt": "2026-10-29T13:10:02Z"
    });
    final_doc["speakers"] =
        json!([{ "id": "S1", "name": null, "samples": [{ "startSec": 0.5, "endSec": 1.5 }] }]);
    final_doc["segments"][0]["speakerId"] = json!("S1");
    assert_eq!(
        hub.hub
            .finish_transcription(ticket, final_doc)
            .await
            .unwrap(),
        FinishOutcome::Saved
    );

    let t = phone.get(&format!("/v1/meetings/{id}/transcript")).await;
    assert_eq!(
        (t.etag(), t.body["status"].as_str()),
        ("\"t-2\"".to_string(), Some("final"))
    );
    assert_ne!(t.body["generation"], draft.body["generation"]);
    assert_eq!(t.body["updatedBy"], hub.hub.hub_id());
    let feed = phone.changes(None).await;
    let entry = &feed.body["changes"][0];
    assert_eq!(
        (
            entry["transcriptEtag"].as_str(),
            entry["hubState"]["transcription"].as_str()
        ),
        (Some("\"t-2\""), Some("done"))
    );
    assert_eq!(entry["audio"]["sha256"], sha);
}

#[tokio::test]
async fn edits_are_never_replaced_by_the_computer() {
    let mut hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;

    // An edited draft pushed before the audio (the push order in "Recorder sync procedure"): nothing is queued.
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let mut edited = transcript_doc(&id, &phone.device_id, &new_id());
    edited["segments"][0]["edited"] = json!(true);
    phone.create_transcript(&edited).await;
    hub.drain_events();
    phone
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    assert!(
        !hub.saw(|e| matches!(e, HubEvent::TranscriptionWanted { .. }))
            .await
    );
    assert_eq!(
        phone.get(&format!("/v1/meetings/{id}")).await.body["hubState"]["transcription"],
        "none"
    );

    // Edited while the computer was transcribing: the result is not saved.
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let mut draft = transcript_doc(&id, &phone.device_id, &new_id());
    let etag = phone.create_transcript(&draft).await.etag();
    phone
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    let ticket = hub
        .hub
        .begin_transcription(&id)
        .await
        .unwrap()
        .expect("transcription starts");
    draft["segments"][0]["text"] = json!("Goedemiddag allemaal.");
    draft["segments"][0]["edited"] = json!(true);
    assert_eq!(
        phone.update_transcript(&draft, &etag).await.status,
        StatusCode::OK
    );
    let mut result = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    result["status"] = json!("final");
    assert_eq!(
        hub.hub.finish_transcription(ticket, result).await.unwrap(),
        FinishOutcome::Skipped
    );
    let t = phone.get(&format!("/v1/meetings/{id}/transcript")).await;
    assert_eq!(
        (
            t.body["status"].as_str(),
            t.body["segments"][0]["text"].as_str()
        ),
        (Some("draft"), Some("Goedemiddag allemaal."))
    );
    assert_eq!(
        phone.get(&format!("/v1/meetings/{id}")).await.body["hubState"]["transcription"],
        "skipped"
    );
}

#[tokio::test]
async fn a_corrupted_upload_is_discarded() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let audio = wav(1.0);
    let start = phone
        .send(Method::POST, &format!("/v1/meetings/{id}/audio/uploads"), &[],
              Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": audio.len(), "sha256": sha256(b"something else") })))
        .await;
    let upload_id = start.body["uploadId"].as_str().unwrap();
    let r = phone.append(upload_id, 0, audio.clone()).await;
    assert_eq!(
        (r.status, r.code()),
        (StatusCode::UNPROCESSABLE_ENTITY, "checksum_mismatch")
    );
    assert_eq!(
        phone
            .send(Method::HEAD, &format!("/v1/uploads/{upload_id}"), &[], None)
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        phone.get(&format!("/v1/meetings/{id}")).await.body["audio"],
        Value::Null
    );
    // Too many bytes for the declared size.
    let start = phone
        .send(Method::POST, &format!("/v1/meetings/{id}/audio/uploads"), &[],
              Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": 10, "sha256": sha256(&audio[..10]) })))
        .await;
    let r = phone
        .append(
            start.body["uploadId"].as_str().unwrap(),
            0,
            audio[..20].to_vec(),
        )
        .await;
    assert_eq!(r.code(), "payload_too_large");
    // fileName and mimeType must agree.
    let r = phone
        .send(Method::POST, &format!("/v1/meetings/{id}/audio/uploads"), &[],
              Some(json!({ "fileName": "audio.m4a", "mimeType": "audio/wav", "sizeBytes": 10, "sha256": sha256(b"x") })))
        .await;
    assert_eq!(r.code(), "bad_request");
}

// MARK: change feed

#[tokio::test]
async fn change_feed_paging_long_polling_and_full_sync() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let ids: Vec<String> = (0..3).map(|_| new_id()).collect();
    for id in &ids {
        phone
            .create_meeting(&meeting_doc(id, &phone.device_id, "ended"))
            .await;
    }
    let page = phone.get("/v1/changes?limit=2").await;
    assert_eq!(
        (
            page.body["changes"].as_array().unwrap().len(),
            page.body["hasMore"].as_bool()
        ),
        (2, Some(true))
    );
    assert_eq!(page.body["changes"][0]["meetingId"], ids[0].as_str());
    assert_eq!(page.body["changes"][0]["meetingEtag"], "\"m-1\"");
    assert_eq!(page.body["changes"][0]["transcriptEtag"], Value::Null);
    let cursor = page.body["cursor"].as_str().unwrap().to_string();
    let rest = phone.changes(Some(&cursor)).await;
    assert_eq!(
        (
            rest.body["changes"].as_array().unwrap().len(),
            rest.body["hasMore"].as_bool()
        ),
        (1, Some(false))
    );
    let cursor = rest.body["cursor"].as_str().unwrap().to_string();
    let empty = phone.changes(Some(&cursor)).await;
    assert_eq!(empty.body["changes"], json!([]));
    assert_eq!(empty.body["cursor"], cursor.as_str());

    // Long polling wakes up on a change.
    let waiting = {
        let client = phone.client.clone();
        let url = format!("{}/v1/changes?since={cursor}&wait=20", phone.base);
        let token = phone.token.clone().unwrap();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let body: Value = client
                .get(url)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            (started.elapsed(), body)
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut doc = meeting_doc(&ids[1], &phone.device_id, "ended");
    doc["title"] = json!("Renamed");
    phone.update_meeting(&doc, "\"m-1\"").await;
    let (elapsed, body) = waiting.await.unwrap();
    assert!(
        elapsed < Duration::from_secs(10),
        "long poll returned after {elapsed:?}"
    );
    assert_eq!(body["changes"][0]["meetingId"], ids[1].as_str());
    assert_eq!(body["changes"][0]["meetingEtag"], "\"m-2\"");
    let cursor = body["cursor"].as_str().unwrap().to_string();

    // Deletion shows in the incremental feed and is absent from a full sync.
    assert_eq!(
        phone
            .send(
                Method::DELETE,
                &format!("/v1/meetings/{}", ids[0]),
                &[],
                None
            )
            .await
            .status,
        StatusCode::NO_CONTENT
    );
    let inc = phone.changes(Some(&cursor)).await;
    assert_eq!(
        (
            inc.body["changes"][0]["deleted"].as_bool(),
            inc.body["changes"][0]["meetingEtag"].as_str()
        ),
        (Some(true), Some("\"m-2\""))
    );
    let full = phone.changes(None).await;
    let listed: Vec<&str> = full.body["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["meetingId"].as_str().unwrap())
        .collect();
    assert_eq!(listed.len(), 2);
    assert!(!listed.contains(&ids[0].as_str()));

    assert_eq!(
        phone.get("/v1/changes?since=abc").await.code(),
        "bad_request"
    );
    assert_eq!(phone.get("/v1/changes?limit=0").await.code(), "bad_request");
    assert_eq!(phone.get("/v1/changes?wait=31").await.code(), "bad_request");
}

// MARK: permissions, scope, deletion

#[tokio::test]
async fn scope_permissions_and_deletion() {
    let mut hub = start_hub().await;
    let alice = Phone::pair(&hub, "Alice's iPhone", None).await;
    let bob = Phone::pair(&hub, "Bob's Pixel", Some("own")).await;
    let id = new_id();
    phone_meeting_with_transcript(&alice, &id).await;

    // Scope "own": someone else's meeting doesn't exist for Bob.
    assert_eq!(
        bob.get(&format!("/v1/meetings/{id}")).await.code(),
        "not_found"
    );
    assert_eq!(bob.changes(None).await.body["changes"], json!([]));
    let r = bob
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    assert_eq!(r.code(), "not_found");

    // Scope "all": Bob sees it and may correct it, but only Alice uploads its audio or deletes it everywhere.
    bob.send(
        Method::PATCH,
        "/v1/devices/me",
        &[],
        Some(json!({ "scope": "all" })),
    )
    .await;
    let m = bob.get(&format!("/v1/meetings/{id}")).await;
    let mut doc = m.body.clone();
    doc["title"] = json!("Corrected by Bob");
    assert_eq!(
        bob.update_meeting(&doc, &m.etag()).await.status,
        StatusCode::OK
    );
    assert_eq!(
        bob.upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
            .await
            .code(),
        "forbidden"
    );
    assert_eq!(
        bob.send(Method::DELETE, &format!("/v1/meetings/{id}"), &[], None)
            .await
            .code(),
        "forbidden"
    );

    // Alice deletes everywhere: a tombstone, and every write is refused.
    assert_eq!(
        alice
            .send(Method::DELETE, &format!("/v1/meetings/{id}"), &[], None)
            .await
            .status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        alice
            .send(Method::DELETE, &format!("/v1/meetings/{id}"), &[], None)
            .await
            .status,
        StatusCode::NO_CONTENT
    );
    let tomb = alice.get(&format!("/v1/meetings/{id}")).await;
    assert!(tomb.body["deletedAt"].is_string());
    assert_eq!(
        alice
            .get(&format!("/v1/meetings/{id}/transcript"))
            .await
            .code(),
        "meeting_deleted"
    );
    assert_eq!(
        alice.update_meeting(&tomb.body, &tomb.etag()).await.code(),
        "meeting_deleted"
    );
    assert_eq!(
        alice.create_meeting(&tomb.body).await.code(),
        "meeting_deleted"
    );
    assert_eq!(
        alice.get(&format!("/v1/meetings/{id}/audio")).await.code(),
        "meeting_deleted"
    );

    // Restored on the computer (Recently deleted): back in the feed, not deleted.
    let cursor = alice.changes(None).await.body["cursor"]
        .as_str()
        .unwrap()
        .to_string();
    hub.drain_events();
    hub.hub.restore_meeting(&id).await.unwrap();
    let inc = alice.changes(Some(&cursor)).await;
    assert_eq!(
        (
            inc.body["changes"][0]["meetingId"].as_str(),
            inc.body["changes"][0]["deleted"].as_bool()
        ),
        (Some(id.as_str()), Some(false))
    );
    assert!(
        hub.saw(|e| matches!(e, HubEvent::MeetingChanged { meeting_id } if *meeting_id == id))
            .await
    );
}

#[tokio::test]
async fn a_replacing_device_owns_the_old_devices_meetings() {
    let hub = start_hub().await;
    let old = Phone::pair(&hub, "Old iPhone", None).await;
    let id = new_id();
    old.create_meeting(&meeting_doc(&id, &old.device_id, "ended"))
        .await;
    let new = Phone::pair(&hub, "New iPhone", Some("own")).await;
    assert_eq!(
        new.get(&format!("/v1/meetings/{id}")).await.code(),
        "not_found"
    );
    hub.hub
        .replace_devices(&new.device_id, std::slice::from_ref(&old.device_id))
        .await
        .unwrap();
    assert_eq!(old.get("/v1/devices/me").await.code(), "device_revoked");
    assert_eq!(
        new.get(&format!("/v1/meetings/{id}")).await.status,
        StatusCode::OK
    );
    assert_eq!(
        new.get("/v1/devices/me").await.body["replaces"],
        json!([old.device_id])
    );
    assert_eq!(
        new.upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
            .await
            .header("upload-complete")
            .as_deref(),
        Some("true")
    );
}

// MARK: the desktop app's own meetings

#[tokio::test]
async fn desktop_meetings_reach_the_phone() {
    let mut hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    let mut doc = meeting_doc(&id, hub.hub.hub_id(), "ended");
    doc["recordedBy"] =
        json!({ "deviceId": hub.hub.hub_id(), "deviceName": "Test MacBook", "platform": "macos" });
    doc["capture"] = json!({ "channel": "online-call", "meetingApp": "teams", "trigger": "meeting-detection", "audioSources": ["microphone", "system"] });
    hub.hub.save_local_meeting(doc).await.unwrap();
    let recording = hub.dir.path().join("recording.wav");
    std::fs::write(&recording, wav(2.0)).unwrap();
    hub.drain_events();
    hub.hub.attach_local_audio(&id, &recording).await.unwrap();
    assert!(
        hub.saw(|e| matches!(e, HubEvent::TranscriptionWanted { meeting_id } if *meeting_id == id))
            .await
    );
    let ticket = hub
        .hub
        .begin_transcription(&id)
        .await
        .unwrap()
        .expect("transcription starts");
    let mut t = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    t["status"] = json!("final");
    t["speakers"] = json!([{ "id": "S1", "isLocalUser": true }, { "id": "S2" }]);
    t["segments"][0]["speakerId"] = json!("S1");
    t["segments"][0]["audioSource"] = json!("microphone");
    t["segments"][1]["speakerId"] = json!("S2");
    t["segments"][1]["audioSource"] = json!("system");
    assert_eq!(
        hub.hub.finish_transcription(ticket, t).await.unwrap(),
        FinishOutcome::Saved
    );

    // The phone sees the call and names the other speaker; the name comes back to the computer.
    let feed = phone.changes(None).await;
    assert_eq!(feed.body["changes"][0]["meetingId"], id.as_str());
    let t = phone.get(&format!("/v1/meetings/{id}/transcript")).await;
    let mut named = t.body.clone();
    named["speakers"][1]["name"] = json!("Ayşe (Acme)");
    named["speakers"][1]["edited"] = json!(true);
    assert_eq!(
        phone.update_transcript(&named, &t.etag()).await.status,
        StatusCode::OK
    );
    let (_, transcript) = hub.hub.local_meeting(&id).await.unwrap().unwrap();
    assert_eq!(transcript.unwrap()["speakers"][1]["name"], "Ayşe (Acme)");

    // The phone downloads the call audio to play it.
    assert_eq!(
        phone
            .get(&format!("/v1/meetings/{id}/audio"))
            .await
            .bytes
            .len(),
        wav(2.0).len()
    );
    // Only the computer deletes its own meetings.
    assert_eq!(
        phone
            .send(Method::DELETE, &format!("/v1/meetings/{id}"), &[], None)
            .await
            .code(),
        "forbidden"
    );
    hub.hub.delete_local_meeting(&id).await.unwrap();
    assert!(phone.get(&format!("/v1/meetings/{id}")).await.body["deletedAt"].is_string());
}

async fn phone_meeting_with_transcript(phone: &Phone, id: &str) {
    assert_eq!(
        phone
            .create_meeting(&meeting_doc(id, &phone.device_id, "ended"))
            .await
            .status,
        StatusCode::CREATED
    );
    assert_eq!(
        phone
            .create_transcript(&transcript_doc(id, &phone.device_id, &new_id()))
            .await
            .status,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn unknown_routes_are_problems() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let r = phone.get("/v1/nothing").await;
    assert_eq!((r.status, r.code()), (StatusCode::NOT_FOUND, "not_found"));
}

#[tokio::test]
async fn a_taken_port_falls_back_and_advertising_never_blocks_serving() {
    // Something else holds the default port.
    let blocker = std::net::TcpListener::bind(("127.0.0.1", sync_hub::DEFAULT_PORT));
    let dir = tempfile::tempdir().unwrap();
    let mut config = sync_hub::HubConfig::new(dir.path(), "Port test", "windows", "0.0.0-test");
    config.listen = vec![format!("127.0.0.1:{}", sync_hub::DEFAULT_PORT)
        .parse()
        .unwrap()];
    config.advertise = true; // may not work in a sandbox; must not stop the server
    let (hub, _events) = sync_hub::Hub::open(config).await.unwrap();
    let server = hub.serve().await.unwrap();
    if blocker.is_ok() {
        assert_ne!(server.port(), sync_hub::DEFAULT_PORT);
    }
    let client = client(hub.fingerprint());
    let hello: Value = client
        .get(format!("https://127.0.0.1:{}/v1/hello", server.port()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(hello["platform"], "windows");
    // The pairing QR code carries the port actually in use.
    assert!(
        hub.start_pairing()
            .qr_uri
            .contains(&format!("%3A{}", server.port()))
            || hub.reachable_addresses().is_empty()
    );
    server.shutdown().await;
}

// MARK: fixes from the security and protocol review

#[tokio::test]
async fn an_already_paired_device_id_cannot_be_taken_over() {
    let hub = start_hub().await;
    let victim = Phone::pair(&hub, "Victim's iPhone", None).await;
    let info = hub.hub.start_pairing();
    // Someone who completes a pairing with the victim's (non-secret) deviceId but without its token.
    let mut attacker = Phone::new(&hub, "Attacker");
    attacker.device_id = victim.device_id.clone();
    let r = attacker.pair_with(&info.code, None).await;
    assert_eq!((r.status, r.code()), (StatusCode::FORBIDDEN, "forbidden"));
    assert_eq!(
        victim.get("/v1/devices/me").await.status,
        StatusCode::OK,
        "the victim stays paired"
    );
    // The code was not used up: the real phone can still pair with it.
    let mut newcomer = Phone::new(&hub, "Newcomer");
    assert_eq!(
        newcomer.pair_with(&info.code, None).await.status,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn edits_between_queueing_and_starting_are_kept() {
    let mut hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let mut draft = transcript_doc(&id, &phone.device_id, &new_id());
    let etag = phone.create_transcript(&draft).await.etag();
    phone
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    assert!(
        hub.saw(|e| matches!(e, HubEvent::TranscriptionWanted { .. }))
            .await
    );
    // Queued; the user edits before the computer starts.
    draft["segments"][1]["text"] = json!("Corrected.");
    draft["segments"][1]["edited"] = json!(true);
    phone.update_transcript(&draft, &etag).await;
    assert!(hub.hub.begin_transcription(&id).await.unwrap().is_none());
    assert_eq!(
        phone.get(&format!("/v1/meetings/{id}")).await.body["hubState"]["transcription"],
        "skipped"
    );
    assert_eq!(
        phone
            .get(&format!("/v1/meetings/{id}/transcript"))
            .await
            .body["segments"][1]["text"],
        "Corrected."
    );

    // "Transcribe again" (the user chose to replace their edits) still works.
    let ticket = hub.hub.begin_retranscription(&id).await.unwrap();
    let mut result = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    result["status"] = json!("final");
    assert_eq!(
        hub.hub.finish_transcription(ticket, result).await.unwrap(),
        FinishOutcome::Saved
    );
    assert_eq!(
        phone
            .get(&format!("/v1/meetings/{id}/transcript"))
            .await
            .body["status"],
        "final"
    );
}

#[tokio::test]
async fn any_change_during_transcription_wins_even_without_edited_flags() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let mut draft = transcript_doc(&id, &phone.device_id, &new_id());
    let etag = phone.create_transcript(&draft).await.etag();
    phone
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    let ticket = hub.hub.begin_transcription(&id).await.unwrap().unwrap();
    // Deleting a segment sets no `edited` flag, but it is still the user's change.
    draft["segments"].as_array_mut().unwrap().pop();
    assert_eq!(
        phone.update_transcript(&draft, &etag).await.status,
        StatusCode::OK
    );
    let result = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    assert_eq!(
        hub.hub.finish_transcription(ticket, result).await.unwrap(),
        FinishOutcome::Skipped
    );
    assert_eq!(
        phone
            .get(&format!("/v1/meetings/{id}/transcript"))
            .await
            .body["segments"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn invalid_results_and_deleted_meetings_end_transcription_cleanly() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    phone
        .upload(&id, "audio.wav", "audio/wav", &wav(1.0), 1 << 20)
        .await;
    let ticket = hub.hub.begin_transcription(&id).await.unwrap().unwrap();
    let mut bad = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    bad["segments"][1]["id"] = json!("s0001"); // duplicate id
    assert!(hub.hub.finish_transcription(ticket, bad).await.is_err());
    let m = phone.get(&format!("/v1/meetings/{id}")).await;
    assert_eq!(m.body["hubState"]["transcription"], "failed");
    // Not retried automatically by unrelated edits (a title rename).
    let mut doc = m.body.clone();
    doc["title"] = json!("Renamed");
    phone.update_meeting(&doc, &m.etag()).await;
    assert_eq!(
        phone.get(&format!("/v1/meetings/{id}")).await.body["hubState"]["transcription"],
        "failed"
    );

    let ticket = hub.hub.begin_retranscription(&id).await.unwrap();
    phone
        .send(Method::DELETE, &format!("/v1/meetings/{id}"), &[], None)
        .await;
    let result = transcript_doc(&id, hub.hub.hub_id(), &new_id());
    assert_eq!(
        hub.hub.finish_transcription(ticket, result).await.unwrap(),
        FinishOutcome::MeetingDeleted
    );
}

#[tokio::test]
async fn phones_send_drafts_with_unique_ids() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let id = new_id();
    phone
        .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
        .await;
    let mut t = transcript_doc(&id, &phone.device_id, &new_id());
    t["status"] = json!("final");
    assert_eq!(phone.create_transcript(&t).await.code(), "bad_request");
    let mut t = transcript_doc(&id, &phone.device_id, &new_id());
    t["segments"][1]["id"] = json!("s0001");
    assert_eq!(phone.create_transcript(&t).await.code(), "bad_request");
    // Out of order and with a dangling speaker: stored sorted, with the reference dropped.
    let mut t = transcript_doc(&id, &phone.device_id, &new_id());
    t["segments"].as_array_mut().unwrap().reverse();
    t["segments"][0]["speakerId"] = json!("S9");
    let r = phone.create_transcript(&t).await;
    assert_eq!(r.status, StatusCode::CREATED);
    assert_eq!(r.body["segments"][0]["id"], "s0001");
    assert_eq!(r.body["segments"][1]["speakerId"], Value::Null);
}

#[tokio::test]
async fn transcription_queued_when_the_app_quit_starts_again() {
    let dir = tempfile::tempdir().unwrap();
    let id = new_id();
    {
        let mut config = sync_hub::HubConfig::new(dir.path(), "Restart", "macos", "0.0.0-test");
        config.listen = vec!["127.0.0.1:0".parse().unwrap()];
        config.advertise = false;
        let (hub, mut events) = sync_hub::Hub::open(config).await.unwrap();
        let mut doc = meeting_doc(&id, hub.hub_id(), "ended");
        doc["recordedBy"] = json!({ "deviceId": hub.hub_id(), "platform": "macos" });
        hub.save_local_meeting(doc).await.unwrap();
        let wav_path = dir.path().join("r.wav");
        std::fs::write(&wav_path, wav(1.0)).unwrap();
        hub.attach_local_audio(&id, &wav_path).await.unwrap();
        let mut queued = false;
        while let Ok(e) = events.try_recv() {
            queued |= matches!(e, HubEvent::TranscriptionWanted { .. });
        }
        assert!(queued);
        // The app quits before transcribing.
    }
    let mut config = sync_hub::HubConfig::new(dir.path(), "Restart", "macos", "0.0.0-test");
    config.advertise = false;
    let (_hub, mut events) = sync_hub::Hub::open(config).await.unwrap();
    let mut again = false;
    while let Ok(e) = events.try_recv() {
        again |= matches!(&e, HubEvent::TranscriptionWanted { meeting_id } if *meeting_id == id);
    }
    assert!(again, "TranscriptionWanted again after a restart");
}

#[tokio::test]
async fn full_sync_survives_forgotten_tombstones_and_misses_no_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = sync_hub::HubConfig::new(dir.path(), "Full sync", "macos", "0.0.0-test");
    config.listen = vec!["127.0.0.1:0".parse().unwrap()];
    config.advertise = false;
    config.tombstone_retention = Duration::ZERO;
    config.trash_retention = Duration::ZERO;
    let (hub, events) = sync_hub::Hub::open(config).await.unwrap();
    let server = hub.serve().await.unwrap();
    let test = TestHub {
        base: format!("https://127.0.0.1:{}", server.port()),
        hub,
        server,
        events,
        dir,
    };
    let phone = Phone::pair(&test, "iPhone", None).await;
    let ids: Vec<String> = (0..5).map(|_| new_id()).collect();
    for id in &ids {
        phone
            .create_meeting(&meeting_doc(id, &phone.device_id, "ended"))
            .await;
    }
    // A later meeting is deleted and its tombstone forgotten: the pruned point is past the first meetings.
    let gone = new_id();
    phone
        .create_meeting(&meeting_doc(&gone, &phone.device_id, "ended"))
        .await;
    phone
        .send(Method::DELETE, &format!("/v1/meetings/{gone}"), &[], None)
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    test.hub.run_maintenance().await.unwrap();

    // Page through a full sync two at a time; delete a meeting from page 1 while paging.
    let page1 = phone.get("/v1/changes?limit=2").await;
    assert_eq!(page1.body["hasMore"], true);
    let mut cursor = page1.body["cursor"].as_str().unwrap().to_string();
    assert!(
        cursor.starts_with('f'),
        "full-sync continuation cursor: {cursor}"
    );
    phone
        .send(
            Method::DELETE,
            &format!("/v1/meetings/{}", ids[0]),
            &[],
            None,
        )
        .await;
    let mut listed = 2;
    loop {
        let page = phone
            .get(&format!("/v1/changes?limit=2&since={cursor}"))
            .await;
        assert_eq!(page.status, StatusCode::OK, "{:?}", page.body);
        listed += page.body["changes"].as_array().unwrap().len();
        cursor = page.body["cursor"].as_str().unwrap().to_string();
        if page.body["hasMore"] == false {
            break;
        }
    }
    assert_eq!(listed, 5);
    // The final cursor is from when the full sync started, so the deletion during it comes through.
    let after = phone.changes(Some(&cursor)).await;
    assert!(after.body["changes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["meetingId"] == ids[0].as_str() && c["deleted"] == true));
    // An old incremental cursor is expired.
    assert_eq!(phone.changes(Some("1")).await.code(), "cursor_expired");
}

#[tokio::test]
async fn uploads_are_limited_per_device_and_replaced_per_meeting() {
    let hub = start_hub().await;
    let phone = Phone::pair(&hub, "iPhone", None).await;
    let mut started = Vec::new();
    for i in 0..5 {
        let id = new_id();
        phone
            .create_meeting(&meeting_doc(&id, &phone.device_id, "ended"))
            .await;
        let r = phone
            .send(Method::POST, &format!("/v1/meetings/{id}/audio/uploads"), &[],
                  Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": 100, "sha256": sha256(&[i]) })))
            .await;
        started.push((id, r));
    }
    for (_, r) in &started[..4] {
        assert_eq!(r.status, StatusCode::CREATED);
    }
    assert_eq!(started[4].1.code(), "rate_limited");
    // A different file for the same meeting replaces the earlier attempt instead of adding one.
    let (id, first) = &started[0];
    let r = phone
        .send(Method::POST, &format!("/v1/meetings/{id}/audio/uploads"), &[],
              Some(json!({ "fileName": "audio.wav", "mimeType": "audio/wav", "sizeBytes": 100, "sha256": sha256(b"other") })))
        .await;
    assert_eq!(r.status, StatusCode::CREATED);
    let old = first.body["uploadId"].as_str().unwrap();
    assert_eq!(
        phone
            .send(Method::HEAD, &format!("/v1/uploads/{old}"), &[], None)
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    // Unknown and malformed upload IDs are simply not found.
    assert_eq!(
        phone
            .append("not-an-upload-id-at-all", 0, vec![1])
            .await
            .code(),
        "not_found"
    );
    assert_eq!(
        phone.append(&"x".repeat(300), 0, vec![1]).await.code(),
        "not_found"
    );
}

#[tokio::test]
async fn connections_are_limited_and_silent_ones_closed() {
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let mut config = sync_hub::HubConfig::new(dir.path(), "Limits", "macos", "0.0.0-test");
    config.listen = vec!["127.0.0.1:0".parse().unwrap()];
    config.advertise = false;
    config.max_connections_per_address = 3;
    config.handshake_timeout = Duration::from_millis(500);
    config.idle_timeout = Duration::from_secs(1);
    let (hub, _events) = sync_hub::Hub::open(config).await.unwrap();
    let server = hub.serve().await.unwrap();
    let addr = format!("127.0.0.1:{}", server.port());

    // Three connections that never speak TLS fill this address's quota…
    let mut idle = Vec::new();
    for _ in 0..3 {
        idle.push(tokio::net::TcpStream::connect(&addr).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    // …so a fourth is closed at once.
    let mut extra = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), extra.read(&mut buf))
        .await
        .expect("closed quickly");
    assert!(matches!(n, Ok(0) | Err(_)));
    // The silent ones are dropped after the handshake deadline, freeing the quota.
    for mut s in idle {
        let n = tokio::time::timeout(Duration::from_secs(3), s.read(&mut buf))
            .await
            .expect("closed by the hub");
        assert!(matches!(n, Ok(0) | Err(_)));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let c = client(hub.fingerprint());
    assert!(c
        .get(format!("https://{addr}/v1/hello"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    server.shutdown().await;
}
