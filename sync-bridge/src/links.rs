//! What the bridge last synced for each meeting (`bridge.db` in the sync data directory), so it can tell which side
//! changed since.

use std::path::Path;
use std::str::FromStr;

use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// Recorded or imported in Meetily on this computer.
    Desktop,
    /// Arrived from a phone.
    Phone,
}

impl Origin {
    fn as_str(&self) -> &'static str {
        match self {
            Origin::Desktop => "desktop",
            Origin::Phone => "phone",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Link {
    pub uuid: String,
    pub meetily_id: String,
    pub origin: Origin,
    /// Meetily's title and transcript as last synced.
    pub title_fp: Option<String>,
    pub rows_fp: Option<String>,
    /// The hub's revisions as last synced.
    pub hub_meeting_rev: Option<i64>,
    pub hub_transcript_rev: Option<i64>,
    /// False after the bridge removed the meeting from Meetily because it was deleted everywhere.
    pub meetily_present: bool,
    /// The recording is in place on both sides.
    pub audio_done: bool,
    /// Meetily folder of a meeting from a phone.
    pub folder: Option<String>,
}

#[derive(Clone)]
pub struct Links {
    pool: SqlitePool,
}

impl Links {
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", dir.join("bridge.db").display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new().max_connections(2).connect_with(options).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS links (
                uuid TEXT PRIMARY KEY, meetily_id TEXT NOT NULL UNIQUE, origin TEXT NOT NULL,
                title_fp TEXT, rows_fp TEXT, hub_meeting_rev INTEGER, hub_transcript_rev INTEGER,
                meetily_present INTEGER NOT NULL, audio_done INTEGER NOT NULL, folder TEXT)",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    fn from_row(r: &sqlx::sqlite::SqliteRow) -> Link {
        Link {
            uuid: r.get("uuid"),
            meetily_id: r.get("meetily_id"),
            origin: if r.get::<String, _>("origin") == "phone" { Origin::Phone } else { Origin::Desktop },
            title_fp: r.get("title_fp"),
            rows_fp: r.get("rows_fp"),
            hub_meeting_rev: r.get("hub_meeting_rev"),
            hub_transcript_rev: r.get("hub_transcript_rev"),
            meetily_present: r.get::<i64, _>("meetily_present") != 0,
            audio_done: r.get::<i64, _>("audio_done") != 0,
            folder: r.get("folder"),
        }
    }

    pub async fn get(&self, uuid: &str) -> Result<Option<Link>> {
        let row = sqlx::query("SELECT * FROM links WHERE uuid = ?").bind(uuid).fetch_optional(&self.pool).await?;
        Ok(row.as_ref().map(Self::from_row))
    }

    pub async fn by_meetily_id(&self, meetily_id: &str) -> Result<Option<Link>> {
        let row = sqlx::query("SELECT * FROM links WHERE meetily_id = ?")
            .bind(meetily_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(Self::from_row))
    }

    pub async fn all(&self) -> Result<Vec<Link>> {
        let rows = sqlx::query("SELECT * FROM links").fetch_all(&self.pool).await?;
        Ok(rows.iter().map(Self::from_row).collect())
    }

    pub async fn save(&self, l: &Link) -> Result<()> {
        sqlx::query(
            "INSERT INTO links (uuid, meetily_id, origin, title_fp, rows_fp, hub_meeting_rev, hub_transcript_rev,
                                meetily_present, audio_done, folder)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (uuid) DO UPDATE SET meetily_id = excluded.meetily_id, origin = excluded.origin,
               title_fp = excluded.title_fp, rows_fp = excluded.rows_fp, hub_meeting_rev = excluded.hub_meeting_rev,
               hub_transcript_rev = excluded.hub_transcript_rev, meetily_present = excluded.meetily_present,
               audio_done = excluded.audio_done, folder = excluded.folder",
        )
        .bind(&l.uuid)
        .bind(&l.meetily_id)
        .bind(l.origin.as_str())
        .bind(&l.title_fp)
        .bind(&l.rows_fp)
        .bind(l.hub_meeting_rev)
        .bind(l.hub_transcript_rev)
        .bind(l.meetily_present as i64)
        .bind(l.audio_done as i64)
        .bind(&l.folder)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete(&self, uuid: &str) -> Result<()> {
        sqlx::query("DELETE FROM links WHERE uuid = ?").bind(uuid).execute(&self.pool).await?;
        Ok(())
    }
}
