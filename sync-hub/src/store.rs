//! Persistent hub state: SQLite (`hub.db`) for devices, documents, the change sequence and uploads; files for audio.
//!
//! Every read-modify-write goes through [`Store::lock`], so there is one writer at a time; readers use WAL.
//! The change feed is coalesced by construction: each meeting row carries the sequence number of its latest change.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Result;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use tokio::sync::{Mutex, MutexGuard};

pub struct Store {
    pool: SqlitePool,
    dir: PathBuf,
    write: Mutex<()>,
}

#[derive(Debug, Clone)]
pub struct DeviceRecord {
    pub device_id: String,
    pub device_name: Option<String>,
    pub platform: String,
    pub app_version: Option<String>,
    pub scope: String,
    pub paired_at: String,
    pub last_seen_at: Option<String>,
    pub replaces: Vec<String>,
}

impl DeviceRecord {
    pub fn to_json(&self) -> Value {
        let mut v = json!({
            "deviceId": self.device_id,
            "platform": self.platform,
            "scope": self.scope,
            "pairedAt": self.paired_at,
            "lastSeenAt": self.last_seen_at,
            "replaces": self.replaces,
        });
        if let Some(n) = &self.device_name {
            v["deviceName"] = json!(n);
        }
        if let Some(a) = &self.app_version {
            v["appVersion"] = json!(a);
        }
        v
    }
}

#[derive(Debug, Clone)]
pub struct MeetingRow {
    pub id: String,
    pub doc: Value,
    pub revision: i64,
    pub recorded_by: String,
    pub deleted_at: Option<String>,
    pub seq: i64,
    pub hub_state: Value,
    /// `audio` as the recorder described it in meeting.json; only a fallback when probing the file fails.
    pub declared_audio: Option<Value>,
    pub purged: bool,
}

#[derive(Debug, Clone)]
pub struct TranscriptRow {
    pub meeting_id: String,
    pub doc: Value,
    pub revision: i64,
    pub generation: String,
}

#[derive(Debug, Clone)]
pub struct UploadRow {
    pub id: String,
    pub meeting_id: String,
    pub device_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub size_bytes: i64,
    pub sha256: String,
    pub offset: i64,
    pub complete: bool,
    pub updated_at: String,
}

pub fn default_hub_state() -> Value {
    json!({ "audioReceived": false, "transcription": "none", "transcriptionProgress": 0, "transcriptionError": null })
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)",
    "INSERT OR IGNORE INTO counters (name, value) VALUES ('seq', 0), ('pruned_through', 0)",
    "CREATE TABLE IF NOT EXISTS devices (
        device_id TEXT PRIMARY KEY, device_name TEXT, platform TEXT NOT NULL, app_version TEXT,
        scope TEXT NOT NULL, token_hash TEXT UNIQUE, paired_at TEXT NOT NULL, last_seen_at TEXT,
        replaces TEXT NOT NULL DEFAULT '[]')",
    "CREATE TABLE IF NOT EXISTS revoked_tokens (token_hash TEXT PRIMARY KEY, device_id TEXT NOT NULL, revoked_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS meetings (
        id TEXT PRIMARY KEY, doc TEXT NOT NULL, revision INTEGER NOT NULL, recorded_by TEXT NOT NULL,
        deleted_at TEXT, seq INTEGER NOT NULL, hub_state TEXT NOT NULL, declared_audio TEXT,
        purged INTEGER NOT NULL DEFAULT 0)",
    "CREATE INDEX IF NOT EXISTS meetings_seq ON meetings (seq)",
    "CREATE TABLE IF NOT EXISTS transcripts (
        meeting_id TEXT PRIMARY KEY, doc TEXT NOT NULL, revision INTEGER NOT NULL, generation TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS uploads (
        id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL, device_id TEXT NOT NULL, file_name TEXT NOT NULL,
        mime_type TEXT NOT NULL, size_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, offset INTEGER NOT NULL,
        complete INTEGER NOT NULL, updated_at TEXT NOT NULL)",
];

impl Store {
    pub async fn open(dir: &Path) -> Result<Self> {
        for sub in ["audio", "uploads"] {
            std::fs::create_dir_all(dir.join(sub))?;
        }
        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", dir.join("hub.db").display()))?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(10));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        for stmt in SCHEMA {
            sqlx::query(stmt).execute(&pool).await?;
        }
        Ok(Self {
            pool,
            dir: dir.to_path_buf(),
            write: Mutex::new(()),
        })
    }

    /// Hold while reading and then writing, so changes don't interleave.
    pub async fn lock(&self) -> MutexGuard<'_, ()> {
        self.write.lock().await
    }

    pub fn audio_dir(&self, meeting_id: &str) -> PathBuf {
        self.dir.join("audio").join(meeting_id)
    }

    pub fn upload_path(&self, upload_id: &str) -> PathBuf {
        self.dir.join("uploads").join(format!("{upload_id}.part"))
    }

    // MARK: counters

    pub async fn next_seq(&self) -> Result<i64> {
        let row =
            sqlx::query("UPDATE counters SET value = value + 1 WHERE name = 'seq' RETURNING value")
                .fetch_one(&self.pool)
                .await?;
        Ok(row.get(0))
    }

    pub async fn current_seq(&self) -> Result<i64> {
        self.counter("seq").await
    }

    pub async fn pruned_through(&self) -> Result<i64> {
        self.counter("pruned_through").await
    }

    async fn counter(&self, name: &str) -> Result<i64> {
        Ok(sqlx::query("SELECT value FROM counters WHERE name = ?")
            .bind(name)
            .fetch_one(&self.pool)
            .await?
            .get(0))
    }

    // MARK: devices

    fn device_from_row(row: &sqlx::sqlite::SqliteRow) -> DeviceRecord {
        let replaces: String = row.get("replaces");
        DeviceRecord {
            device_id: row.get("device_id"),
            device_name: row.get("device_name"),
            platform: row.get("platform"),
            app_version: row.get("app_version"),
            scope: row.get("scope"),
            paired_at: row.get("paired_at"),
            last_seen_at: row.get("last_seen_at"),
            replaces: serde_json::from_str(&replaces).unwrap_or_default(),
        }
    }

    pub async fn device_by_token_hash(&self, hash: &str) -> Result<Option<DeviceRecord>> {
        let row = sqlx::query("SELECT * FROM devices WHERE token_hash = ?")
            .bind(hash)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(Self::device_from_row))
    }

    pub async fn is_revoked_token(&self, hash: &str) -> Result<bool> {
        Ok(
            sqlx::query("SELECT 1 FROM revoked_tokens WHERE token_hash = ?")
                .bind(hash)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    pub async fn device(&self, device_id: &str) -> Result<Option<DeviceRecord>> {
        let row = sqlx::query("SELECT * FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(Self::device_from_row))
    }

    pub async fn devices(&self) -> Result<Vec<DeviceRecord>> {
        let rows = sqlx::query("SELECT * FROM devices ORDER BY paired_at")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(Self::device_from_row).collect())
    }

    async fn revoke_token(&self, device_id: &str, hash: Option<String>, now: &str) -> Result<()> {
        if let Some(hash) = hash {
            sqlx::query("INSERT OR REPLACE INTO revoked_tokens (token_hash, device_id, revoked_at) VALUES (?, ?, ?)")
                .bind(hash)
                .bind(device_id)
                .bind(now)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// Pairs a device; for a deviceId the hub already knows, the new token replaces the old one.
    #[allow(clippy::too_many_arguments)]
    pub async fn pair_device(
        &self,
        device_id: &str,
        device_name: Option<&str>,
        platform: &str,
        app_version: Option<&str>,
        scope: Option<&str>,
        token_hash: &str,
        now: &str,
    ) -> Result<DeviceRecord> {
        let existing = sqlx::query("SELECT token_hash, scope FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_optional(&self.pool)
            .await?;
        if let Some(row) = &existing {
            self.revoke_token(device_id, row.get("token_hash"), now)
                .await?;
            let scope = scope
                .map(str::to_string)
                .unwrap_or_else(|| row.get("scope"));
            sqlx::query(
                "UPDATE devices SET device_name = ?, platform = ?, app_version = ?, scope = ?, token_hash = ?, paired_at = ?
                 WHERE device_id = ?",
            )
            .bind(device_name)
            .bind(platform)
            .bind(app_version)
            .bind(scope)
            .bind(token_hash)
            .bind(now)
            .bind(device_id)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO devices (device_id, device_name, platform, app_version, scope, token_hash, paired_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(device_id)
            .bind(device_name)
            .bind(platform)
            .bind(app_version)
            .bind(scope.unwrap_or("all"))
            .bind(token_hash)
            .bind(now)
            .execute(&self.pool)
            .await?;
        }
        Ok(self.device(device_id).await?.expect("device just written"))
    }

    pub async fn update_device(
        &self,
        device_id: &str,
        device_name: Option<&str>,
        app_version: Option<&str>,
        scope: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE devices SET device_name = COALESCE(?, device_name), app_version = COALESCE(?, app_version),
             scope = COALESCE(?, scope) WHERE device_id = ?",
        )
        .bind(device_name)
        .bind(app_version)
        .bind(scope)
        .bind(device_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn touch_device(&self, device_id: &str, now: &str) -> Result<()> {
        sqlx::query("UPDATE devices SET last_seen_at = ? WHERE device_id = ?")
            .bind(now)
            .bind(device_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Removes a device; its token answers `device_revoked` from then on.
    pub async fn remove_device(&self, device_id: &str, now: &str) -> Result<bool> {
        let row = sqlx::query("SELECT token_hash FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else { return Ok(false) };
        self.revoke_token(device_id, row.get("token_hash"), now)
            .await?;
        sqlx::query("DELETE FROM devices WHERE device_id = ?")
            .bind(device_id)
            .execute(&self.pool)
            .await?;
        Ok(true)
    }

    pub async fn set_replaces(&self, device_id: &str, replaces: &[String]) -> Result<()> {
        sqlx::query("UPDATE devices SET replaces = ? WHERE device_id = ?")
            .bind(serde_json::to_string(replaces)?)
            .bind(device_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // MARK: meetings

    fn meeting_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<MeetingRow> {
        let doc: String = row.get("doc");
        let hub_state: String = row.get("hub_state");
        let declared: Option<String> = row.get("declared_audio");
        let purged: i64 = row.get("purged");
        Ok(MeetingRow {
            id: row.get("id"),
            doc: serde_json::from_str(&doc)?,
            revision: row.get("revision"),
            recorded_by: row.get("recorded_by"),
            deleted_at: row.get("deleted_at"),
            seq: row.get("seq"),
            hub_state: serde_json::from_str(&hub_state)?,
            declared_audio: declared.map(|d| serde_json::from_str(&d)).transpose()?,
            purged: purged != 0,
        })
    }

    pub async fn meeting(&self, id: &str) -> Result<Option<MeetingRow>> {
        let row = sqlx::query("SELECT * FROM meetings WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(Self::meeting_from_row).transpose()
    }

    /// Inserts or replaces a meeting row and gives it a new change sequence number.
    pub async fn save_meeting(&self, m: &MeetingRow) -> Result<i64> {
        let seq = self.next_seq().await?;
        sqlx::query(
            "INSERT INTO meetings (id, doc, revision, recorded_by, deleted_at, seq, hub_state, declared_audio, purged)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET doc = excluded.doc, revision = excluded.revision,
               recorded_by = excluded.recorded_by, deleted_at = excluded.deleted_at, seq = excluded.seq,
               hub_state = excluded.hub_state, declared_audio = excluded.declared_audio, purged = excluded.purged",
        )
        .bind(&m.id)
        .bind(serde_json::to_string(&m.doc)?)
        .bind(m.revision)
        .bind(&m.recorded_by)
        .bind(&m.deleted_at)
        .bind(seq)
        .bind(serde_json::to_string(&m.hub_state)?)
        .bind(m.declared_audio.as_ref().map(serde_json::to_string).transpose()?)
        .bind(m.purged as i64)
        .execute(&self.pool)
        .await?;
        Ok(seq)
    }

    /// Bumps a meeting's change sequence without changing it (e.g. its transcript changed).
    pub async fn touch_meeting(&self, id: &str) -> Result<i64> {
        let seq = self.next_seq().await?;
        sqlx::query("UPDATE meetings SET seq = ? WHERE id = ?")
            .bind(seq)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(seq)
    }

    /// Updates hubState; `bump` adds a change-feed entry (state transitions, not progress).
    pub async fn set_hub_state(&self, id: &str, state: &Value, bump: bool) -> Result<()> {
        if bump {
            let seq = self.next_seq().await?;
            sqlx::query("UPDATE meetings SET hub_state = ?, seq = ? WHERE id = ?")
                .bind(serde_json::to_string(state)?)
                .bind(seq)
                .bind(id)
                .execute(&self.pool)
                .await?;
        } else {
            sqlx::query("UPDATE meetings SET hub_state = ? WHERE id = ?")
                .bind(serde_json::to_string(state)?)
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// Meetings changed after `since` (or, for a full sync, every meeting that isn't deleted), oldest change first.
    /// `visible_to`: recorder identities for scope `own`; `None` = every meeting.
    pub async fn changes(
        &self,
        since: Option<i64>,
        limit: i64,
        visible_to: Option<&[String]>,
    ) -> Result<Vec<MeetingRow>> {
        let mut sql = String::from("SELECT * FROM meetings WHERE seq > ?");
        if since.is_none() {
            sql.push_str(" AND deleted_at IS NULL");
        }
        if let Some(ids) = visible_to {
            sql.push_str(&format!(
                " AND recorded_by IN ({})",
                vec!["?"; ids.len().max(1)].join(",")
            ));
        }
        sql.push_str(" ORDER BY seq LIMIT ?");
        let mut q = sqlx::query(&sql).bind(since.unwrap_or(0));
        if let Some(ids) = visible_to {
            if ids.is_empty() {
                q = q.bind("");
            }
            for id in ids {
                q = q.bind(id);
            }
        }
        let rows = q.bind(limit).fetch_all(&self.pool).await?;
        rows.iter().map(Self::meeting_from_row).collect()
    }

    pub async fn meetings_deleted_before(&self, cutoff: &str) -> Result<Vec<MeetingRow>> {
        let rows =
            sqlx::query("SELECT * FROM meetings WHERE deleted_at IS NOT NULL AND deleted_at < ?")
                .bind(cutoff)
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(Self::meeting_from_row).collect()
    }

    /// Drops a tombstone for good; cursors from before it get `cursor_expired`.
    pub async fn forget_meeting(&self, m: &MeetingRow) -> Result<()> {
        sqlx::query("UPDATE counters SET value = MAX(value, ?) WHERE name = 'pruned_through'")
            .bind(m.seq)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM transcripts WHERE meeting_id = ?")
            .bind(&m.id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM meetings WHERE id = ?")
            .bind(&m.id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn mark_purged(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE meetings SET purged = 1 WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM transcripts WHERE meeting_id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // MARK: transcripts

    pub async fn transcript(&self, meeting_id: &str) -> Result<Option<TranscriptRow>> {
        let row = sqlx::query("SELECT * FROM transcripts WHERE meeting_id = ?")
            .bind(meeting_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| {
            let doc: String = r.get("doc");
            Ok(TranscriptRow {
                meeting_id: r.get("meeting_id"),
                doc: serde_json::from_str(&doc)?,
                revision: r.get("revision"),
                generation: r.get("generation"),
            })
        })
        .transpose()
    }

    pub async fn transcript_revisions(
        &self,
        meeting_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>> {
        let mut out = std::collections::HashMap::new();
        for chunk in meeting_ids.chunks(200) {
            let sql = format!(
                "SELECT meeting_id, revision FROM transcripts WHERE meeting_id IN ({})",
                vec!["?"; chunk.len()].join(",")
            );
            let mut q = sqlx::query(&sql);
            for id in chunk {
                q = q.bind(id);
            }
            for row in q.fetch_all(&self.pool).await? {
                out.insert(row.get(0), row.get(1));
            }
        }
        Ok(out)
    }

    pub async fn save_transcript(&self, t: &TranscriptRow) -> Result<()> {
        sqlx::query(
            "INSERT INTO transcripts (meeting_id, doc, revision, generation) VALUES (?, ?, ?, ?)
             ON CONFLICT (meeting_id) DO UPDATE SET doc = excluded.doc, revision = excluded.revision,
               generation = excluded.generation",
        )
        .bind(&t.meeting_id)
        .bind(serde_json::to_string(&t.doc)?)
        .bind(t.revision)
        .bind(&t.generation)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // MARK: uploads

    fn upload_from_row(r: &sqlx::sqlite::SqliteRow) -> UploadRow {
        let complete: i64 = r.get("complete");
        UploadRow {
            id: r.get("id"),
            meeting_id: r.get("meeting_id"),
            device_id: r.get("device_id"),
            file_name: r.get("file_name"),
            mime_type: r.get("mime_type"),
            size_bytes: r.get("size_bytes"),
            sha256: r.get("sha256"),
            offset: r.get("offset"),
            complete: complete != 0,
            updated_at: r.get("updated_at"),
        }
    }

    pub async fn upload(&self, id: &str) -> Result<Option<UploadRow>> {
        let row = sqlx::query("SELECT * FROM uploads WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(Self::upload_from_row))
    }

    pub async fn unfinished_upload(
        &self,
        meeting_id: &str,
        sha256: &str,
        device_id: &str,
    ) -> Result<Option<UploadRow>> {
        let row = sqlx::query(
            "SELECT * FROM uploads WHERE meeting_id = ? AND sha256 = ? AND device_id = ? AND complete = 0
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(meeting_id)
        .bind(sha256)
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(Self::upload_from_row))
    }

    pub async fn save_upload(&self, u: &UploadRow) -> Result<()> {
        sqlx::query(
            "INSERT INTO uploads (id, meeting_id, device_id, file_name, mime_type, size_bytes, sha256, offset, complete, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET offset = excluded.offset, complete = excluded.complete,
               updated_at = excluded.updated_at",
        )
        .bind(&u.id)
        .bind(&u.meeting_id)
        .bind(&u.device_id)
        .bind(&u.file_name)
        .bind(&u.mime_type)
        .bind(u.size_bytes)
        .bind(&u.sha256)
        .bind(u.offset)
        .bind(u.complete as i64)
        .bind(&u.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_upload(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM uploads WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        let _ = std::fs::remove_file(self.upload_path(id));
        Ok(())
    }

    pub async fn uploads_idle_since(&self, cutoff: &str) -> Result<Vec<UploadRow>> {
        let rows = sqlx::query("SELECT * FROM uploads WHERE updated_at < ?")
            .bind(cutoff)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(Self::upload_from_row).collect())
    }
}
