//! Keeps Meetily's meetings and the sync hub in step.
//!
//! - A meeting recorded or imported in Meetily is added to the hub (title, transcript as a `final` transcript,
//!   recording), so phones see it.
//! - A meeting from a phone is added to Meetily like an import: a folder in the recordings directory with the
//!   recording, and the transcript as rows. When the hub asks for a final transcript, Meetily's own
//!   retranscription makes it (through [`Transcriber`]).
//! - Later changes go whichever way they happened: a title or transcript changed in Meetily goes to the hub (and
//!   on to the phones); one changed on a phone comes into Meetily. If both changed, the desktop's version wins.
//! - Deleting a meeting in Meetily deletes it everywhere (the hub keeps it in Recently deleted); a meeting deleted
//!   everywhere from a phone is removed from Meetily, and comes back if it is restored.
//!
//! Each meeting is reconciled on its own: compare both sides with what was last synced ([`Link`]), copy what
//! changed, record the new state. Hub events and a periodic scan of Meetily's database decide which meetings to look
//! at.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use sync_hub::{FinishOutcome, Hub, HubEvent};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::links::{Link, Links, Origin};
use crate::mapping::{self, fingerprint_rows, fingerprint_title};
use crate::meetily::{self, Meetily, MeetilyMeeting, MeetilyRow};

const MAX_TITLE: usize = 300;
const MAX_SEGMENT_TEXT: usize = 20_000;

#[derive(Clone)]
pub struct BridgeConfig {
    /// Where the bridge keeps `bridge.db`.
    pub state_dir: PathBuf,
    /// Where meetings from phones get their folders: Meetily's recordings folder (asked each time; it can change).
    pub recordings_dir: Arc<dyn Fn() -> PathBuf + Send + Sync>,
    /// How often Meetily's database is checked for changes.
    pub scan_interval: Duration,
    /// How long to wait before trying again when Meetily is busy recording or transcribing.
    pub busy_retry: Duration,
    /// Run the background tasks (off in tests, which call [`Bridge::sync_all`] and [`Bridge::run_transcriptions`]).
    pub background: bool,
}

/// Makes Meetily transcribe the recording in a meeting folder, replacing the meeting's transcript rows
/// (Meetily's retranscription).
pub struct TranscribeJob {
    pub meetily_meeting_id: String,
    pub folder: PathBuf,
    pub language: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TranscribeError {
    /// Meetily is recording or transcribing something else; try again later.
    Busy,
    Failed(String),
}

pub trait Transcriber: Send + Sync + 'static {
    fn transcribe(&self, job: TranscribeJob) -> BoxFuture<'static, Result<(), TranscribeError>>;
}

/// For the desktop app's UI.
#[derive(Debug, Clone, PartialEq)]
pub enum BridgeEvent {
    /// Meetings in Meetily were added, changed or removed by sync: refresh the list.
    MeetingsChanged,
    DevicePaired { device_id: String },
}

/// Which meeting to look at, from either side.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Key {
    Hub(String),
    Meetily(String),
}

#[derive(Debug, PartialEq)]
pub enum JobOutcome {
    Done,
    /// Meetily was busy; the job was put back.
    Busy,
}

pub struct Bridge {
    inner: Arc<Inner>,
    tasks: Vec<JoinHandle<()>>,
}

struct Inner {
    hub: Hub,
    meetily: Meetily,
    links: Links,
    config: BridgeConfig,
    transcriber: Arc<dyn Transcriber>,
    hub_device: Value,
    ui: mpsc::UnboundedSender<BridgeEvent>,
    /// One reconcile or job step at a time.
    work: tokio::sync::Mutex<()>,
    /// Meetings Meetily is transcribing right now; reconciling waits for them.
    transcribing: StdMutex<HashSet<String>>,
    pending: StdMutex<VecDeque<Key>>,
    pending_notify: Notify,
    jobs: StdMutex<VecDeque<String>>,
    jobs_notify: Notify,
    signatures: StdMutex<HashMap<String, String>>,
    /// Hub events when there are no background tasks.
    manual_events: StdMutex<Option<mpsc::UnboundedReceiver<HubEvent>>>,
}

impl Bridge {
    pub async fn start(
        hub: Hub,
        mut hub_events: mpsc::UnboundedReceiver<HubEvent>,
        meetily_pool: SqlitePool,
        config: BridgeConfig,
        transcriber: Arc<dyn Transcriber>,
        ui: mpsc::UnboundedSender<BridgeEvent>,
    ) -> Result<Bridge> {
        let links = Links::open(&config.state_dir).await.context("opening bridge state")?;
        let hub_config = hub.config().clone();
        let hub_device = json!({
            "deviceId": hub.hub_id(),
            "deviceName": hub_config.name,
            "platform": hub_config.platform,
            "appVersion": hub_config.app_version,
        });
        let inner = Arc::new(Inner {
            hub,
            meetily: Meetily::new(meetily_pool),
            links,
            config,
            transcriber,
            hub_device,
            ui,
            work: tokio::sync::Mutex::new(()),
            transcribing: StdMutex::new(HashSet::new()),
            pending: StdMutex::new(VecDeque::new()),
            pending_notify: Notify::new(),
            jobs: StdMutex::new(VecDeque::new()),
            jobs_notify: Notify::new(),
            signatures: StdMutex::new(HashMap::new()),
            manual_events: StdMutex::new(None),
        });
        let mut tasks = Vec::new();
        if inner.config.background {
            let i = inner.clone();
            tasks.push(tokio::spawn(async move {
                // Everything once at startup, then Meetily's database every few seconds.
                if let Err(e) = i.enqueue_everything().await {
                    tracing::warn!(target: "sync_bridge", "startup scan failed: {e:#}");
                }
                let mut tick = tokio::time::interval(i.config.scan_interval);
                loop {
                    tick.tick().await;
                    if let Err(e) = i.scan_meetily().await {
                        tracing::warn!(target: "sync_bridge", "scanning Meetily failed: {e:#}");
                    }
                }
            }));
            let i = inner.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    i.pending_notify.notified().await;
                    i.drain().await;
                }
            }));
            let i = inner.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    i.jobs_notify.notified().await;
                    while let Some(uuid) = i.next_job() {
                        if i.run_job_logged(&uuid).await == JobOutcome::Busy {
                            // Meetily is recording or transcribing: try again later.
                            tokio::time::sleep(i.config.busy_retry).await;
                            i.push_job(uuid);
                            break;
                        }
                    }
                }
            }));
            let i = inner.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(event) = hub_events.recv().await {
                    i.on_hub_event(event);
                }
            }));
        } else {
            // Tests pump hub events themselves (Bridge::sync_all, Bridge::run_transcriptions).
            *inner.manual_events.lock().unwrap() = Some(hub_events);
        }
        Ok(Bridge { inner, tasks })
    }

    pub fn hub(&self) -> &Hub {
        &self.inner.hub
    }

    /// Looks at every meeting on both sides now (and waits for it).
    pub async fn sync_all(&self) -> Result<()> {
        self.inner.pump_events();
        self.inner.enqueue_everything().await?;
        self.inner.drain().await;
        Ok(())
    }

    /// Runs queued transcriptions now (tests; the background task does this on its own).
    pub async fn run_transcriptions(&self) -> Result<Vec<JobOutcome>> {
        self.inner.pump_events();
        let mut outcomes = Vec::new();
        while let Some(uuid) = self.inner.next_job() {
            let outcome = self.inner.run_job_logged(&uuid).await;
            if outcome == JobOutcome::Busy {
                self.inner.push_job(uuid);
                outcomes.push(outcome);
                break;
            }
            outcomes.push(outcome);
        }
        self.inner.pump_events();
        self.inner.drain().await;
        Ok(outcomes)
    }

    /// Checks Meetily's database now instead of waiting for the next scan.
    pub fn meetily_changed(&self) {
        let i = self.inner.clone();
        tokio::spawn(async move {
            if let Err(e) = i.scan_meetily().await {
                tracing::warn!(target: "sync_bridge", "scanning Meetily failed: {e:#}");
            }
        });
    }

    pub fn stop(self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl Inner {
    fn pump_events(&self) {
        let mut guard = self.manual_events.lock().unwrap();
        if let Some(rx) = guard.as_mut() {
            while let Ok(event) = rx.try_recv() {
                self.on_hub_event(event);
            }
        }
    }

    fn on_hub_event(&self, event: HubEvent) {
        match event {
            HubEvent::MeetingChanged { meeting_id }
            | HubEvent::TranscriptChanged { meeting_id }
            | HubEvent::AudioReceived { meeting_id, .. } => self.push(Key::Hub(meeting_id)),
            HubEvent::TranscriptionWanted { meeting_id } => self.push_job(meeting_id),
            HubEvent::DevicePaired { device_id } => {
                let _ = self.ui.send(BridgeEvent::DevicePaired { device_id });
            }
        }
    }

    fn push(&self, key: Key) {
        let mut q = self.pending.lock().unwrap();
        if !q.contains(&key) {
            q.push_back(key);
        }
        drop(q);
        self.pending_notify.notify_one();
    }

    fn push_job(&self, uuid: String) {
        let mut q = self.jobs.lock().unwrap();
        if !q.contains(&uuid) {
            q.push_back(uuid);
        }
        drop(q);
        self.jobs_notify.notify_one();
    }

    fn next_job(&self) -> Option<String> {
        self.jobs.lock().unwrap().pop_front()
    }

    async fn enqueue_everything(&self) -> Result<()> {
        for id in self.hub.meeting_ids().await? {
            self.push(Key::Hub(id));
        }
        for link in self.links.all().await? {
            self.push(Key::Hub(link.uuid));
        }
        self.signatures.lock().unwrap().clear();
        self.scan_meetily().await
    }

    /// Queues Meetily meetings whose signature changed, ones that disappeared, and ones still waiting for audio.
    async fn scan_meetily(&self) -> Result<()> {
        let now = self.meetily.signatures().await?;
        let changed: Vec<String> = {
            let mut known = self.signatures.lock().unwrap();
            let changed = now.iter().filter(|(id, sig)| known.get(*id) != Some(*sig)).map(|(id, _)| id.clone()).collect();
            *known = now.clone();
            changed
        };
        for id in changed {
            self.push(Key::Meetily(id));
        }
        for link in self.links.all().await? {
            let gone = link.meetily_present && !now.contains_key(&link.meetily_id);
            if gone || (link.meetily_present && !link.audio_done) {
                self.push(Key::Hub(link.uuid));
            }
        }
        Ok(())
    }

    async fn drain(&self) {
        loop {
            let Some(key) = self.pending.lock().unwrap().pop_front() else { break };
            if let Err(e) = self.reconcile_key(&key).await {
                tracing::warn!(target: "sync_bridge", "syncing {key:?} failed: {e:#}");
                // Look again on the next scan.
                if let Key::Meetily(id) = &key {
                    self.signatures.lock().unwrap().remove(id);
                }
            }
        }
    }

    fn ui_changed(&self) {
        let _ = self.ui.send(BridgeEvent::MeetingsChanged);
    }

    async fn reconcile_key(&self, key: &Key) -> Result<()> {
        let (uuid, meetily_id) = match key {
            Key::Hub(uuid) => {
                let uuid = uuid.to_ascii_lowercase();
                let meetily_id = match self.links.get(&uuid).await? {
                    Some(l) => l.meetily_id,
                    None => mapping::meetily_id(&uuid),
                };
                (uuid, meetily_id)
            }
            Key::Meetily(id) => {
                let uuid = match self.links.by_meetily_id(id).await? {
                    Some(l) => l.uuid,
                    None => mapping::meeting_uuid(id).unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                };
                (uuid, id.clone())
            }
        };
        if self.transcribing.lock().unwrap().contains(&uuid) {
            // The job looks at it again when it is done.
            return Ok(());
        }
        let _work = self.work.lock().await;
        if self.reconcile(&uuid, &meetily_id).await? {
            self.ui_changed();
        }
        Ok(())
    }

    /// Brings one meeting in step. Returns whether Meetily's side changed.
    async fn reconcile(&self, uuid: &str, meetily_id: &str) -> Result<bool> {
        let link = self.links.get(uuid).await?;
        let meeting = self.meetily.meeting(meetily_id).await?;
        let hub = self.hub.local_meeting(uuid).await?;
        match (meeting, hub) {
            (Some(m), None) => {
                self.export_new(uuid, &m).await?;
                Ok(false)
            }
            (Some(m), Some((hm, ht))) => {
                if hm["deletedAt"].is_string() {
                    let Some(mut link) = link else {
                        tracing::info!(target: "sync_bridge", "{meetily_id} was deleted everywhere before it was linked; leaving it");
                        return Ok(false);
                    };
                    self.meetily.delete_meeting(&m.id).await?;
                    link.meetily_present = false;
                    link.hub_meeting_rev = hm["revision"].as_i64();
                    self.links.save(&link).await?;
                    return Ok(true);
                }
                match link {
                    Some(link) => self.sync_both(link, &m, &hm, ht.as_ref()).await,
                    None => {
                        // Both sides have it but the bridge lost track (e.g. its state was reset): the hub's
                        // version wins.
                        let origin = self.origin_of(&hm);
                        let rows_fp = match &ht {
                            Some(_) => Some(fingerprint_rows(&self.meetily.rows(&m.id).await?)),
                            None => None,
                        };
                        let link = Link {
                            uuid: uuid.to_string(),
                            meetily_id: m.id.clone(),
                            origin,
                            title_fp: Some(fingerprint_title(&m.title)),
                            rows_fp,
                            hub_meeting_rev: None,
                            hub_transcript_rev: None,
                            meetily_present: true,
                            audio_done: false,
                            folder: m.folder_path.clone(),
                        };
                        self.sync_both(link, &m, &hm, ht.as_ref()).await
                    }
                }
            }
            (None, Some((hm, ht))) => {
                if hm["deletedAt"].is_string() {
                    if let Some(mut link) = link.filter(|l| l.meetily_present) {
                        link.meetily_present = false;
                        self.links.save(&link).await?;
                    }
                    return Ok(false);
                }
                match link {
                    Some(mut link) if link.meetily_present => {
                        // Deleted in Meetily on this computer: delete everywhere (restorable on the hub).
                        self.hub.delete_local_meeting(uuid).await?;
                        link.meetily_present = false;
                        link.hub_meeting_rev = self.hub_revisions(uuid).await?.0;
                        self.links.save(&link).await?;
                        Ok(false)
                    }
                    link => {
                        self.import_new(uuid, link, &hm, ht.as_ref()).await?;
                        Ok(true)
                    }
                }
            }
            (None, None) => {
                if link.is_some() {
                    self.links.delete(uuid).await?;
                }
                Ok(false)
            }
        }
    }

    fn origin_of(&self, hub_meeting: &Value) -> Origin {
        if hub_meeting.pointer("/recordedBy/deviceId").and_then(Value::as_str) == Some(self.hub.hub_id()) {
            Origin::Desktop
        } else {
            Origin::Phone
        }
    }

    async fn hub_revisions(&self, uuid: &str) -> Result<(Option<i64>, Option<i64>)> {
        Ok(match self.hub.local_meeting(uuid).await? {
            Some((m, t)) => (m["revision"].as_i64(), t.and_then(|t| t["revision"].as_i64())),
            None => (None, None),
        })
    }

    /// A Meetily meeting the hub doesn't have yet.
    async fn export_new(&self, uuid: &str, m: &MeetilyMeeting) -> Result<()> {
        let folder = m.folder_path.as_deref().map(std::path::Path::new);
        let info = folder.map(meetily::folder_info).unwrap_or_default();
        let rows = self.meetily.rows(&m.id).await?;
        let mut doc = mapping::desktop_meeting_doc(uuid, m, &info, &rows, &self.hub_device);
        doc["title"] = json!(truncate(&m.title, MAX_TITLE));
        self.hub.save_local_meeting(doc).await?;
        if !rows.is_empty() {
            let transcript = self.transcript_doc(uuid, &rows, None).await;
            self.hub.save_local_transcript(transcript).await?;
        }
        let mut link = Link {
            uuid: uuid.to_string(),
            meetily_id: m.id.clone(),
            origin: Origin::Desktop,
            title_fp: Some(fingerprint_title(&m.title)),
            rows_fp: Some(fingerprint_rows(&rows)),
            hub_meeting_rev: None,
            hub_transcript_rev: None,
            meetily_present: true,
            audio_done: false,
            folder: m.folder_path.clone(),
        };
        self.ensure_audio(&mut link, m).await?;
        (link.hub_meeting_rev, link.hub_transcript_rev) = self.hub_revisions(uuid).await?;
        self.links.save(&link).await?;
        tracing::info!(target: "sync_bridge", "shared {} with phones", m.id);
        Ok(())
    }

    /// A hub meeting Meetily doesn't have (from a phone, or restored from Recently deleted).
    async fn import_new(&self, uuid: &str, link: Option<Link>, hm: &Value, ht: Option<&Value>) -> Result<()> {
        let title = mapping::display_title(hm);
        let started = mapping::started_at(hm);
        let folder = match link.as_ref().and_then(|l| l.folder.clone()).map(PathBuf::from).filter(|f| f.is_dir()) {
            Some(f) => f,
            None => meetily::new_meeting_folder(&(self.config.recordings_dir)(), &title, started)?,
        };
        let meetily_id = mapping::meetily_id(uuid);
        let rows = ht.map(|t| mapping::rows_from_transcript(uuid, t)).unwrap_or_default();
        let meeting = MeetilyMeeting {
            id: meetily_id.clone(),
            title: title.clone(),
            created_at: started,
            folder_path: Some(folder.to_string_lossy().to_string()),
        };
        write_folder_metadata(&folder, &meetily_id, &title, hm)?;
        self.meetily.insert_meeting(&meeting, &rows).await?;
        let mut link = Link {
            uuid: uuid.to_string(),
            meetily_id,
            origin: link.map(|l| l.origin).unwrap_or_else(|| self.origin_of(hm)),
            title_fp: Some(fingerprint_title(&title)),
            rows_fp: Some(fingerprint_rows(&rows)),
            hub_meeting_rev: hm["revision"].as_i64(),
            hub_transcript_rev: ht.and_then(|t| t["revision"].as_i64()),
            meetily_present: true,
            audio_done: false,
            folder: Some(folder.to_string_lossy().to_string()),
        };
        self.ensure_audio(&mut link, &meeting).await?;
        self.links.save(&link).await?;
        tracing::info!(target: "sync_bridge", "added {} from {}", link.meetily_id,
            hm.pointer("/recordedBy/deviceName").and_then(serde_json::Value::as_str).unwrap_or("a phone"));
        Ok(())
    }

    /// Both sides have the meeting: copy whatever changed since the last sync.
    async fn sync_both(&self, mut link: Link, m: &MeetilyMeeting, hm: &Value, ht: Option<&Value>) -> Result<bool> {
        let uuid = link.uuid.clone();
        let rows = self.meetily.rows(&m.id).await?;
        let mut meetily_changed = false;

        // Title
        if link.title_fp.as_deref() != Some(fingerprint_title(&m.title).as_str()) {
            if hm["title"].as_str() != Some(m.title.as_str()) {
                let mut doc = hm.clone();
                doc["title"] = json!(truncate(&m.title, MAX_TITLE));
                self.hub.save_local_meeting(doc).await?;
            }
        } else if link.hub_meeting_rev != hm["revision"].as_i64() {
            let title = mapping::display_title(hm);
            if title != m.title {
                self.meetily.update_title(&m.id, &title).await?;
                meetily_changed = true;
            }
        }

        // Transcript
        let hub_transcript_rev = ht.and_then(|t| t["revision"].as_i64());
        if link.rows_fp.as_deref() != Some(fingerprint_rows(&rows).as_str()) {
            if !(rows.is_empty() && ht.is_none()) {
                let doc = self.transcript_doc(&uuid, &rows, ht).await;
                self.hub.save_local_transcript(doc).await?;
            }
        } else if link.hub_transcript_rev != hub_transcript_rev {
            if let Some(t) = ht {
                self.meetily.replace_rows(&m.id, mapping::started_at(hm), &mapping::rows_from_transcript(&uuid, t)).await?;
                meetily_changed = true;
            }
        }

        // Recording
        let current = self.meetily.meeting(&m.id).await?.unwrap_or_else(|| m.clone());
        if self.ensure_audio(&mut link, &current).await? {
            meetily_changed = true;
        }

        // Remember what is in sync now.
        let current = self.meetily.meeting(&m.id).await?.unwrap_or_else(|| m.clone());
        link.title_fp = Some(fingerprint_title(&current.title));
        link.rows_fp = Some(fingerprint_rows(&self.meetily.rows(&m.id).await?));
        (link.hub_meeting_rev, link.hub_transcript_rev) = self.hub_revisions(&uuid).await?;
        link.meetily_present = true;
        self.links.save(&link).await?;
        Ok(meetily_changed)
    }

    /// Puts the recording where the other side needs it. Returns whether Meetily's meeting changed.
    async fn ensure_audio(&self, link: &mut Link, m: &MeetilyMeeting) -> Result<bool> {
        if link.audio_done {
            return Ok(false);
        }
        match link.origin {
            Origin::Desktop => {
                let has_audio = self
                    .hub
                    .local_meeting(&link.uuid)
                    .await?
                    .is_some_and(|(hm, _)| hm["audio"].is_object());
                if has_audio {
                    link.audio_done = true;
                    return Ok(false);
                }
                let Some(folder) = m.folder_path.as_deref() else {
                    link.audio_done = true; // nothing will come
                    return Ok(false);
                };
                let info = meetily::folder_info(std::path::Path::new(folder));
                if !info.completed {
                    return Ok(false); // still being written; the next scan tries again
                }
                match info.audio_file {
                    Some(path) => match self.hub.attach_local_audio(&link.uuid, &path).await {
                        Ok(()) => link.audio_done = true,
                        Err(e) if e.to_string().contains("unsupported audio type") => {
                            tracing::info!(target: "sync_bridge", "not sharing {}: {e}", path.display());
                            link.audio_done = true;
                        }
                        Err(e) => return Err(e),
                    },
                    None => link.audio_done = true,
                }
                Ok(false)
            }
            Origin::Phone => {
                let Some(source) = self.hub.local_audio_path(&link.uuid).await? else {
                    return Ok(false); // not uploaded yet
                };
                let mut changed = false;
                let folder = match m.folder_path.as_deref().map(PathBuf::from).filter(|f| f.is_dir()) {
                    Some(f) => f,
                    None => {
                        let title = m.title.clone();
                        let f = meetily::new_meeting_folder(&(self.config.recordings_dir)(), &title, m.created_at)?;
                        self.meetily.set_folder(&m.id, &f).await?;
                        changed = true;
                        f
                    }
                };
                let name = source.file_name().context("audio file name")?;
                meetily::link_or_copy(&source, &folder.join(name))?;
                set_folder_audio(&folder, &name.to_string_lossy())?;
                link.folder = Some(folder.to_string_lossy().to_string());
                link.audio_done = true;
                Ok(changed)
            }
        }
    }

    async fn transcript_doc(&self, uuid: &str, rows: &[MeetilyRow], base: Option<&Value>) -> Value {
        let (engine, model) = self.meetily.stt_engine().await;
        let rows: Vec<MeetilyRow> = rows
            .iter()
            .map(|r| MeetilyRow { text: truncate(&r.text, MAX_SEGMENT_TEXT), ..r.clone() })
            .collect();
        mapping::transcript_from_rows(uuid, &rows, base, &self.hub_device, &engine, model.as_deref())
    }

    /// [`Inner::run_job`], turning errors into a failed transcription so the hub doesn't wait forever.
    async fn run_job_logged(&self, uuid: &str) -> JobOutcome {
        match self.run_job(uuid).await {
            Ok(outcome) => outcome,
            Err(e) => {
                tracing::warn!(target: "sync_bridge", "transcribing {uuid} failed: {e:#}");
                let _ = self.hub.fail_transcription(uuid, &format!("{e:#}")).await;
                self.push(Key::Hub(uuid.to_string()));
                JobOutcome::Done
            }
        }
    }

    /// Makes the final transcript the hub asked for, with Meetily's retranscription.
    async fn run_job(&self, uuid: &str) -> Result<JobOutcome> {
        // The meeting and its recording must be in Meetily first.
        self.reconcile_key(&Key::Hub(uuid.to_string())).await?;
        let (ticket, meetily_id, folder) = {
            let _work = self.work.lock().await;
            let link = self.links.get(uuid).await?.filter(|l| l.meetily_present);
            let meeting = match &link {
                Some(l) => self.meetily.meeting(&l.meetily_id).await?,
                None => None,
            };
            let folder = meeting.as_ref().and_then(|m| m.folder_path.clone()).map(PathBuf::from);
            let audio = folder.as_deref().and_then(|f| meetily::folder_info(f).audio_file);
            let (Some(meeting), Some(folder), Some(_)) = (meeting, folder, audio) else {
                self.hub.fail_transcription(uuid, "the recording isn't on this computer").await?;
                return Ok(JobOutcome::Done);
            };
            let Some(ticket) = self.hub.begin_transcription(uuid).await? else {
                return Ok(JobOutcome::Done); // edited meanwhile; left alone
            };
            self.transcribing.lock().unwrap().insert(uuid.to_string());
            (ticket, meeting.id, folder)
        };
        let result = self.transcribe_and_finish(uuid, ticket, &meetily_id, folder).await;
        self.transcribing.lock().unwrap().remove(uuid);
        self.push(Key::Hub(uuid.to_string()));
        result
    }

    async fn transcribe_and_finish(
        &self,
        uuid: &str,
        ticket: sync_hub::TranscriptionTicket,
        meetily_id: &str,
        folder: PathBuf,
    ) -> Result<JobOutcome> {
        let job = TranscribeJob {
            meetily_meeting_id: meetily_id.to_string(),
            folder,
            language: ticket.language_code.as_deref().map(|l| l.split('-').next().unwrap_or(l).to_string()),
        };
        let result = self.transcriber.transcribe(job).await;
        let _work = self.work.lock().await;
        match result {
            Err(TranscribeError::Busy) => return Ok(JobOutcome::Busy),
            Err(TranscribeError::Failed(message)) => {
                self.hub.fail_transcription(uuid, &message).await?;
                return Ok(JobOutcome::Done);
            }
            Ok(()) => {}
        }
        let rows = self.meetily.rows(meetily_id).await?;
        let mut doc = self.transcript_doc(uuid, &rows, None).await;
        doc["languageCode"] = ticket.language_code.clone().map(Value::from).unwrap_or(Value::Null);
        match self.hub.finish_transcription(ticket, doc).await? {
            FinishOutcome::Saved => {}
            FinishOutcome::Skipped => {
                // The user changed the transcript meanwhile; theirs stays, in Meetily too.
                if let Some((hm, Some(t))) = self.hub.local_meeting(uuid).await? {
                    let rows = mapping::rows_from_transcript(uuid, &t);
                    self.meetily.replace_rows(meetily_id, mapping::started_at(&hm), &rows).await?;
                }
            }
            FinishOutcome::MeetingDeleted => {}
        }
        if let Some(mut link) = self.links.get(uuid).await? {
            link.rows_fp = Some(fingerprint_rows(&self.meetily.rows(meetily_id).await?));
            (link.hub_meeting_rev, link.hub_transcript_rev) = self.hub_revisions(uuid).await?;
            self.links.save(&link).await?;
        }
        self.ui_changed();
        Ok(JobOutcome::Done)
    }
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect()
    }
}

/// metadata.json for a phone meeting's folder, in Meetily's format, so its retranscription and recovery work.
fn write_folder_metadata(folder: &std::path::Path, meetily_id: &str, title: &str, hm: &Value) -> Result<()> {
    let path = folder.join("metadata.json");
    if path.exists() {
        return Ok(());
    }
    let meta = json!({
        "version": "1.0",
        "meeting_id": meetily_id,
        "meeting_name": title,
        "created_at": hm["startedAt"],
        "completed_at": hm["endedAt"],
        "duration_seconds": hm.pointer("/audio/durationSec").cloned().unwrap_or(Value::Null),
        "audio_file": hm.pointer("/audio/fileName").cloned().unwrap_or(json!("")),
        "transcript_file": "transcripts.json",
        "status": "completed",
        "source": "phone",
        "device": hm.pointer("/recordedBy/deviceName").cloned().unwrap_or(Value::Null),
    });
    let tmp = folder.join(".metadata.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&meta)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

fn set_folder_audio(folder: &std::path::Path, audio_file: &str) -> Result<()> {
    let path = folder.join("metadata.json");
    let Ok(bytes) = std::fs::read(&path) else { return Ok(()) };
    let Ok(mut meta) = serde_json::from_slice::<Value>(&bytes) else { return Ok(()) };
    if meta["audio_file"].as_str() == Some(audio_file) {
        return Ok(());
    }
    meta["audio_file"] = json!(audio_file);
    let tmp = folder.join(".metadata.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&meta)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}
