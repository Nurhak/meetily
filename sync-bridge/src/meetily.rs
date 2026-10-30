//! Reading and writing Meetily's own database (`meetings`, `transcripts`), the way Meetily itself does
//! (see frontend/src-tauri/src/database/repositories and audio/import.rs).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, PartialEq)]
pub struct MeetilyMeeting {
    pub id: String,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub folder_path: Option<String>,
}

/// One row of Meetily's `transcripts` table.
#[derive(Debug, Clone, PartialEq)]
pub struct MeetilyRow {
    pub id: String,
    pub text: String,
    pub start: Option<f64>,
    pub end: Option<f64>,
}

#[derive(Clone)]
pub struct Meetily {
    pool: SqlitePool,
}

impl Meetily {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// A cheap signature of every meeting and its transcript, to notice changes without reading every transcript.
    pub async fn signatures(&self) -> Result<HashMap<String, String>> {
        let rows = sqlx::query(
            "SELECT m.id, m.title, IFNULL(m.folder_path, ''), COUNT(t.id), TOTAL(LENGTH(t.transcript)),
                    TOTAL(t.audio_start_time), TOTAL(t.audio_end_time), IFNULL(MIN(t.id), ''), IFNULL(MAX(t.id), '')
             FROM meetings m LEFT JOIN transcripts t ON t.meeting_id = m.id
             GROUP BY m.id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let id: String = r.get(0);
                let sig = format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}",
                    r.get::<String, _>(1),
                    r.get::<String, _>(2),
                    r.get::<i64, _>(3),
                    r.get::<f64, _>(4),
                    r.get::<f64, _>(5),
                    r.get::<f64, _>(6),
                    r.get::<String, _>(7),
                    r.get::<String, _>(8),
                );
                (id, sig)
            })
            .collect())
    }

    pub async fn meeting(&self, id: &str) -> Result<Option<MeetilyMeeting>> {
        let row = sqlx::query("SELECT id, title, created_at, folder_path FROM meetings WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| MeetilyMeeting {
            id: r.get("id"),
            title: r.get("title"),
            created_at: r.try_get::<DateTime<Utc>, _>("created_at").unwrap_or_else(|_| Utc::now()),
            folder_path: r.get("folder_path"),
        }))
    }

    /// Transcript rows in playback order.
    pub async fn rows(&self, meeting_id: &str) -> Result<Vec<MeetilyRow>> {
        let rows = sqlx::query(
            "SELECT id, transcript, audio_start_time, audio_end_time FROM transcripts WHERE meeting_id = ?
             ORDER BY audio_start_time ASC, id ASC",
        )
        .bind(meeting_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| MeetilyRow {
                id: r.get(0),
                text: r.get(1),
                start: r.get(2),
                end: r.get(3),
            })
            .collect())
    }

    /// Creates a meeting with its transcript, as Meetily's import does.
    pub async fn insert_meeting(&self, meeting: &MeetilyMeeting, rows: &[MeetilyRow]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO meetings (id, title, created_at, updated_at, folder_path) VALUES (?, ?, ?, ?, ?)")
            .bind(&meeting.id)
            .bind(&meeting.title)
            .bind(meeting.created_at)
            .bind(Utc::now())
            .bind(&meeting.folder_path)
            .execute(&mut *tx)
            .await?;
        insert_rows(&mut tx, &meeting.id, meeting.created_at, rows).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn update_title(&self, meeting_id: &str, title: &str) -> Result<()> {
        sqlx::query("UPDATE meetings SET title = ?, updated_at = ? WHERE id = ?")
            .bind(title)
            .bind(Utc::now())
            .bind(meeting_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_folder(&self, meeting_id: &str, folder: &Path) -> Result<()> {
        sqlx::query("UPDATE meetings SET folder_path = ? WHERE id = ?")
            .bind(folder.to_string_lossy().to_string())
            .bind(meeting_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Replaces a meeting's transcript, as Meetily's retranscription does.
    pub async fn replace_rows(&self, meeting_id: &str, started_at: DateTime<Utc>, rows: &[MeetilyRow]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM transcripts WHERE meeting_id = ?")
            .bind(meeting_id)
            .execute(&mut *tx)
            .await?;
        insert_rows(&mut tx, meeting_id, started_at, rows).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Deletes a meeting the way Meetily's own delete does; the recording folder stays on disk.
    pub async fn delete_meeting(&self, meeting_id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for table in ["transcript_chunks", "summary_processes", "transcripts", "meeting_notes"] {
            let sql = format!("DELETE FROM {table} WHERE meeting_id = ?");
            if let Err(e) = sqlx::query(&sql).bind(meeting_id).execute(&mut *tx).await {
                // Older databases may not have every table.
                if !e.to_string().contains("no such table") {
                    return Err(e.into());
                }
            }
        }
        sqlx::query("DELETE FROM meetings WHERE id = ?").bind(meeting_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// The speech-to-text engine and model configured in Meetily, for `producedBy`.
    pub async fn stt_engine(&self) -> (String, Option<String>) {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT provider, model FROM transcript_settings WHERE id = '1'")
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        match row {
            Some((provider, model)) => {
                let engine = match provider.as_str() {
                    "localWhisper" | "whisper" => "whisper".to_string(),
                    other => other.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-').take(64).collect(),
                };
                let model: String = model.chars().take(128).collect();
                (if engine.is_empty() { "meetily".into() } else { engine }, (!model.is_empty()).then_some(model))
            }
            None => ("whisper".to_string(), None),
        }
    }

    /// Meetily's transcription provider (`parakeet`, `localWhisper`, …), for retranscription.
    pub async fn stt_provider(&self) -> Option<String> {
        sqlx::query_scalar("SELECT provider FROM transcript_settings WHERE id = '1'")
            .fetch_optional(&self.pool)
            .await
            .ok()
            .flatten()
    }
}

async fn insert_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    meeting_id: &str,
    started_at: DateTime<Utc>,
    rows: &[MeetilyRow],
) -> Result<()> {
    for row in rows {
        let start = row.start.unwrap_or(0.0);
        let end = row.end.unwrap_or(start);
        // `timestamp` is a wall-clock time in Meetily; use when the words were spoken.
        let spoken_at = started_at + chrono::Duration::milliseconds((start * 1000.0) as i64);
        sqlx::query(
            "INSERT INTO transcripts (id, meeting_id, transcript, timestamp, audio_start_time, audio_end_time, duration)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.id)
        .bind(meeting_id)
        .bind(&row.text)
        .bind(spoken_at.to_rfc3339())
        .bind(start)
        .bind(end)
        .bind((end - start).max(0.0))
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// What a meeting folder's `metadata.json` says (written by Meetily's recorder and import).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FolderInfo {
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_seconds: Option<f64>,
    pub has_microphone: bool,
    pub has_system_audio: bool,
    pub imported: bool,
    /// The recording is finished (status `completed`) and its audio file is final.
    pub completed: bool,
    pub audio_file: Option<PathBuf>,
}

/// Audio file names Meetily looks for, in order (see audio/retranscription.rs `find_audio_file`).
const AUDIO_CANDIDATES: &[&str] = &["audio.mp4", "audio.m4a", "audio.wav", "audio.mp3", "audio.flac", "audio.ogg"];

pub fn folder_info(folder: &Path) -> FolderInfo {
    let mut info = FolderInfo::default();
    let parse_time =
        |v: &Value| v.as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&Utc));
    match std::fs::read(folder.join("metadata.json")).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok()) {
        Some(meta) => {
            info.started_at = parse_time(&meta["created_at"]);
            info.ended_at = parse_time(&meta["completed_at"]);
            info.duration_seconds = meta["duration_seconds"].as_f64();
            info.has_microphone = meta.pointer("/devices/microphone").is_some_and(|v| !v.is_null());
            info.has_system_audio = meta.pointer("/devices/system_audio").is_some_and(|v| !v.is_null());
            info.imported = meta["source"].as_str() == Some("import");
            info.completed = meta["status"].as_str() == Some("completed");
            if let Some(name) = meta["audio_file"].as_str().filter(|n| !n.is_empty() && !n.contains(['/', '\\'])) {
                let path = folder.join(name);
                if path.is_file() {
                    info.audio_file = Some(path);
                }
            }
        }
        None => {
            // Older folders without metadata: treat a recording untouched for a minute as finished.
            info.completed = std::fs::metadata(folder)
                .and_then(|m| m.modified())
                .map(|t| t.elapsed().unwrap_or_default().as_secs() > 60)
                .unwrap_or(false);
        }
    }
    if folder.join(".checkpoints").exists() {
        // Still being merged from checkpoints.
        info.completed = false;
    }
    if info.audio_file.is_none() {
        info.audio_file = AUDIO_CANDIDATES.iter().map(|n| folder.join(n)).find(|p| p.is_file());
    }
    info
}

/// A new folder for a meeting from a phone, named like Meetily's own (`<title>_<YYYY-MM-DD_HH-MM>`).
pub fn new_meeting_folder(base: &Path, title: &str, started_at: DateTime<Utc>) -> Result<PathBuf> {
    let name: String = title
        .chars()
        .map(|c| if c.is_alphanumeric() || c == ' ' || c == '-' || c == '_' { c } else { '_' })
        .collect::<String>()
        .trim()
        .chars()
        .take(60)
        .collect();
    let name = if name.is_empty() { "Meeting".to_string() } else { name };
    let stamp = started_at.with_timezone(&chrono::Local).format("%Y-%m-%d_%H-%M");
    std::fs::create_dir_all(base)?;
    for n in 0..1000 {
        let suffix = if n == 0 { String::new() } else { format!("-{}", n + 1) };
        let folder = base.join(format!("{name}_{stamp}{suffix}"));
        match std::fs::create_dir(&folder) {
            Ok(()) => return Ok(folder),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("no free folder name for {name}")
}

/// Places a copy of `source` at `target` without using extra disk space where possible (hard link).
pub fn link_or_copy(source: &Path, target: &Path) -> Result<()> {
    if target.exists() {
        return Ok(());
    }
    if std::fs::hard_link(source, target).is_err() {
        let tmp = target.with_extension("partial");
        std::fs::copy(source, &tmp)?;
        std::fs::rename(&tmp, target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_info_reads_recorder_and_import_metadata() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("audio.mp4"), b"x").unwrap();
        std::fs::write(
            dir.path().join("metadata.json"),
            r#"{"version":"1.0","created_at":"2026-10-28T09:00:31+00:00","completed_at":"2026-10-28T09:41:02Z",
               "duration_seconds":2431.5,"devices":{"microphone":"MacBook Pro Microphone","system_audio":"System"},
               "audio_file":"audio.mp4","status":"completed"}"#,
        )
        .unwrap();
        let info = folder_info(dir.path());
        assert!(info.completed && info.has_microphone && info.has_system_audio && !info.imported);
        assert_eq!(info.duration_seconds, Some(2431.5));
        assert_eq!(info.audio_file.as_deref(), Some(dir.path().join("audio.mp4").as_path()));
        assert_eq!(info.started_at.unwrap().to_rfc3339(), "2026-10-28T09:00:31+00:00");

        std::fs::create_dir(dir.path().join(".checkpoints")).unwrap();
        assert!(!folder_info(dir.path()).completed, "still merging");

        let import = tempfile::tempdir().unwrap();
        std::fs::write(import.path().join("audio.wav"), b"x").unwrap();
        std::fs::write(
            import.path().join("metadata.json"),
            r#"{"status":"recording","source":"import","audio_file":"../escape.wav"}"#,
        )
        .unwrap();
        let info = folder_info(import.path());
        assert!(info.imported && !info.completed);
        assert_eq!(info.audio_file.as_deref(), Some(import.path().join("audio.wav").as_path()));
    }

    #[test]
    fn new_folders_never_collide_and_names_are_safe() {
        let dir = tempfile::tempdir().unwrap();
        let t = DateTime::parse_from_rfc3339("2026-10-29T12:30:05Z").unwrap().with_timezone(&Utc);
        let a = new_meeting_folder(dir.path(), "Acme / renewal: Q4", t).unwrap();
        let b = new_meeting_folder(dir.path(), "Acme / renewal: Q4", t).unwrap();
        assert_ne!(a, b);
        assert_eq!(a.parent().unwrap(), dir.path());
        assert!(a.file_name().unwrap().to_str().unwrap().starts_with("Acme _ renewal_ Q4_2026-10-29_"));
        let c = new_meeting_folder(dir.path(), "../..", t).unwrap();
        assert_eq!(c.parent().unwrap(), dir.path());
    }
}
