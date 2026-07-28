use crate::boundary::{BOUNDARY_SCHEMA_VERSION, BoundaryEnvelope, BoundaryEvent};
use crate::capabilities::{CAPABILITY_SCHEMA_VERSION, CapabilityEnvelope, CapabilityEvent};
use crate::error::WorkflowError;
use crate::model::{
    BUDGET_SCHEMA_VERSION, BudgetEnvelope, BudgetEvent, BudgetLedger, EVENT_SCHEMA_VERSION,
    GateDecision, GateRequest, JournalEntry, ReconstructedState, ReservationSummary, RunState,
    RunStatus, SupersedeInfo, WorkflowEvent, WorkflowEventEnvelope,
};
use chrono::Utc;
use fs2::FileExt;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Debug)]
pub struct WorkflowStore {
    root: PathBuf,
    writes: Mutex<()>,
}

impl WorkflowStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            writes: Mutex::new(()),
        }
    }

    pub fn from_environment() -> Self {
        let root = std::env::var_os("SERVITOR_WORKFLOWS_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(default_state_root);
        Self::new(root)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn run_dir(&self, run_id: &str) -> PathBuf {
        self.root.join("runs").join(run_id)
    }
    pub fn script_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("workflow.js")
    }
    pub fn state_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("state.json")
    }
    pub fn journal_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("journal.jsonl")
    }
    /// Path of the versioned append-only event stream. Written only for
    /// `workflow.v2` runs; v1 runs never create this file.
    pub fn events_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("events.jsonl")
    }
    pub fn budget_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("budget.jsonl")
    }
    pub fn boundary_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("boundary.jsonl")
    }
    pub fn capabilities_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("capabilities.jsonl")
    }
    pub fn command_result_path(&self, run_id: &str, key: &str) -> PathBuf {
        self.run_dir(run_id)
            .join("commands")
            .join(key.replace('#', "-"))
            .join("result.json")
    }
    pub fn run_summary_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("run-summary.html")
    }
    pub fn pause_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("pause.request")
    }
    pub fn cancel_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("cancel.request")
    }

    pub fn create_run(&self, state: &RunState, script: &str) -> Result<(), WorkflowError> {
        let dir = self.run_dir(&state.run_id);
        fs::create_dir_all(&dir).map_err(|source| WorkflowError::Write { path: dir, source })?;
        self.write(&self.script_path(&state.run_id), script.as_bytes())?;
        self.save_state(state)
    }

    pub fn load_script(&self, run_id: &str) -> Result<String, WorkflowError> {
        let path = self.script_path(run_id);
        fs::read_to_string(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                WorkflowError::RunNotFound(run_id.to_owned())
            } else {
                WorkflowError::Read { path, source }
            }
        })
    }

    pub fn load_state(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        let path = self.state_path(run_id);
        let text = fs::read_to_string(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                WorkflowError::RunNotFound(run_id.to_owned())
            } else {
                WorkflowError::Read {
                    path: path.clone(),
                    source,
                }
            }
        })?;
        serde_json::from_str(&text).map_err(WorkflowError::Json)
    }

    pub fn save_state(&self, state: &RunState) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("state write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("state write lock poisoned".to_owned()))?;
        let bytes = serde_json::to_vec_pretty(state)?;
        self.write(&self.state_path(&state.run_id), &bytes)
    }

    pub fn update_state<F>(&self, run_id: &str, update: F) -> Result<RunState, WorkflowError>
    where
        F: FnOnce(&mut RunState),
    {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("state write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("state write lock poisoned".to_owned()))?;
        let mut state = self.load_state(run_id)?;
        update(&mut state);
        state.updated_at = chrono::Utc::now();
        let bytes = serde_json::to_vec_pretty(&state)?;
        fs::write(self.state_path(run_id), bytes).map_err(|source| WorkflowError::Write {
            path: self.state_path(run_id),
            source,
        })?;
        Ok(state)
    }

    pub fn append(&self, run_id: &str, entry: &JournalEntry) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("journal write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("journal write lock poisoned".to_owned()))?;
        let path = self.journal_path(run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let mut bytes = serde_json::to_vec(entry)?;
        bytes.push(b'\n');
        let result = file
            .write_all(&bytes)
            .and_then(|_| file.sync_data())
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            });
        let unlock = FileExt::unlock(&file).map_err(|source| WorkflowError::Write { path, source });
        result.and(unlock)
    }

    pub fn journal_index(
        &self,
        run_id: &str,
    ) -> Result<BTreeMap<String, JournalEntry>, WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("journal read lock poisoned".to_owned()))?;
        self.journal_index_unlocked(run_id)
    }

    fn journal_index_unlocked(
        &self,
        run_id: &str,
    ) -> Result<BTreeMap<String, JournalEntry>, WorkflowError> {
        let path = self.journal_path(run_id);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeMap::new());
            }
            Err(source) => return Err(WorkflowError::Read { path, source }),
        };
        let mut index = BTreeMap::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let entry: JournalEntry = serde_json::from_str(line)?;
            index.insert(entry.key.clone(), entry);
        }
        Ok(index)
    }

    /// Persist one secret-safe V2-E boundary observation. The event carries
    /// declared paths and variable *names*, never environment values.
    pub fn append_boundary_event(
        &self,
        run_id: &str,
        event: BoundaryEvent,
    ) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("boundary write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("boundary write lock poisoned".to_owned()))?;
        let path = self.boundary_path(run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let sequence = Self::count_event_lines_in(&mut file, &path)? + 1;
        file.seek(SeekFrom::End(0))
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let envelope = BoundaryEnvelope {
            version: BOUNDARY_SCHEMA_VERSION,
            sequence,
            at: Utc::now(),
            run_id: run_id.to_owned(),
            event,
        };
        let mut bytes = serde_json::to_vec(&envelope)?;
        bytes.push(b'\n');
        let result = file
            .write_all(&bytes)
            .and_then(|_| file.sync_data())
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            });
        let unlock = FileExt::unlock(&file).map_err(|source| WorkflowError::Write { path, source });
        result.and(unlock)
    }

    pub fn read_boundary_events(
        &self,
        run_id: &str,
    ) -> Result<Vec<BoundaryEnvelope>, WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("boundary read lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("boundary read lock poisoned".to_owned()))?;
        let path = self.boundary_path(run_id);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(WorkflowError::Read { path, source }),
        };
        let mut events = Vec::new();
        for (index, line) in text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .enumerate()
        {
            let envelope: BoundaryEnvelope = serde_json::from_str(line)?;
            let expected_sequence = index as u64 + 1;
            if envelope.run_id != run_id {
                return Err(WorkflowError::Invariant(format!(
                    "boundary event run id mismatch: expected {run_id}, got {}",
                    envelope.run_id
                )));
            }
            if envelope.sequence != expected_sequence {
                return Err(WorkflowError::Invariant(format!(
                    "boundary event sequence gap in run {run_id}: expected {expected_sequence}, got {}",
                    envelope.sequence
                )));
            }
            events.push(envelope);
        }
        Ok(events)
    }

    pub fn append_capability_event(
        &self,
        run_id: &str,
        event: CapabilityEvent,
    ) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("capability write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("capability write lock poisoned".to_owned()))?;
        let path = self.capabilities_path(run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let sequence = Self::count_event_lines_in(&mut file, &path)? + 1;
        file.seek(SeekFrom::End(0))
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let envelope = CapabilityEnvelope {
            version: CAPABILITY_SCHEMA_VERSION,
            sequence,
            at: Utc::now(),
            run_id: run_id.to_owned(),
            event,
        };
        let mut bytes = serde_json::to_vec(&envelope)?;
        bytes.push(b'\n');
        let result = file
            .write_all(&bytes)
            .and_then(|_| file.sync_data())
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            });
        let unlock = FileExt::unlock(&file).map_err(|source| WorkflowError::Write { path, source });
        result.and(unlock)
    }

    pub fn read_capability_events(
        &self,
        run_id: &str,
    ) -> Result<Vec<CapabilityEnvelope>, WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("capability read lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("capability read lock poisoned".to_owned()))?;
        let path = self.capabilities_path(run_id);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(WorkflowError::Read { path, source }),
        };
        let mut events = Vec::new();
        for (index, line) in text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .enumerate()
        {
            let envelope: CapabilityEnvelope = serde_json::from_str(line)?;
            let expected_sequence = index as u64 + 1;
            if envelope.run_id != run_id {
                return Err(WorkflowError::Invariant(format!(
                    "capability event run id mismatch: expected {run_id}, got {}",
                    envelope.run_id
                )));
            }
            if envelope.sequence != expected_sequence {
                return Err(WorkflowError::Invariant(format!(
                    "capability event sequence gap in run {run_id}: expected {expected_sequence}, got {}",
                    envelope.sequence
                )));
            }
            events.push(envelope);
        }
        Ok(events)
    }

    /// Append one lifecycle event to `events.jsonl`. The envelope is stamped
    /// with the event schema version, a monotonic per-run sequence, the wall
    /// time, the run id, and the parent run id (foundation only; `None` in
    /// V2-A). The file is append-only and `sync_data`'d so a crash leaves a
    /// clean tail. Returns `Ok(())` even when the run is v1 (no event stream)
    /// so callers can gate on a single v2 check upstream.
    pub fn append_event(
        &self,
        run_id: &str,
        parent_run_id: Option<&str>,
        event: WorkflowEvent,
    ) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("event write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("event write lock poisoned".to_owned()))?;
        let path = self.events_path(run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let sequence = Self::count_event_lines_in(&mut file, &path)? + 1;
        file.seek(SeekFrom::End(0))
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let envelope = WorkflowEventEnvelope {
            version: EVENT_SCHEMA_VERSION,
            sequence,
            at: Utc::now(),
            run_id: run_id.to_owned(),
            parent_run_id: parent_run_id.map(str::to_owned),
            event,
        };
        let mut bytes = serde_json::to_vec(&envelope)?;
        bytes.push(b'\n');
        let result = file
            .write_all(&bytes)
            .and_then(|_| file.sync_data())
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            });
        let unlock = FileExt::unlock(&file).map_err(|source| WorkflowError::Write { path, source });
        result.and(unlock)
    }

    /// Persist a v2 lifecycle event before applying its matching state mutation.
    /// A caller can use this when an event/state pair has a single transition
    /// boundary; reconstruction remains authoritative after a crash between the
    /// two durable writes.
    pub fn transition<F>(
        &self,
        run_id: &str,
        parent_run_id: Option<&str>,
        event: WorkflowEvent,
        update: F,
    ) -> Result<RunState, WorkflowError>
    where
        F: FnOnce(&mut RunState),
    {
        self.transition_many(run_id, parent_run_id, [event], update)
    }

    /// Persist all lifecycle events for a compound transition before applying
    /// its matching state mutation. This preserves the event-first recovery
    /// boundary when one state change has more than one observable event.
    pub fn transition_many<F, I>(
        &self,
        run_id: &str,
        parent_run_id: Option<&str>,
        events: I,
        update: F,
    ) -> Result<RunState, WorkflowError>
    where
        F: FnOnce(&mut RunState),
        I: IntoIterator<Item = WorkflowEvent>,
    {
        for event in events {
            self.append_event(run_id, parent_run_id, event)?;
        }
        self.update_state(run_id, update)
    }

    /// for v1 runs (no `events.jsonl`) and for v2 runs that have not yet
    /// recorded any events.
    pub fn read_events(&self, run_id: &str) -> Result<Vec<WorkflowEventEnvelope>, WorkflowError> {
        let path = self.events_path(run_id);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(source) => return Err(WorkflowError::Read { path, source }),
        };
        let mut out = Vec::new();
        for (expected_sequence, line) in
            (1_u64..).zip(text.lines().filter(|line| !line.trim().is_empty()))
        {
            let envelope: WorkflowEventEnvelope = serde_json::from_str(line)?;
            if envelope.version != EVENT_SCHEMA_VERSION {
                return Err(WorkflowError::Invariant(format!(
                    "unsupported event schema version {} in run {run_id}",
                    envelope.version
                )));
            }
            if envelope.run_id != run_id {
                return Err(WorkflowError::Invariant(format!(
                    "event run id mismatch: expected {run_id}, got {}",
                    envelope.run_id
                )));
            }
            if envelope.sequence != expected_sequence {
                return Err(WorkflowError::Invariant(format!(
                    "event sequence gap in run {run_id}: expected {expected_sequence}, got {}",
                    envelope.sequence
                )));
            }
            out.push(envelope);
        }
        Ok(out)
    }

    fn count_event_lines_in(file: &mut std::fs::File, path: &Path) -> Result<u64, WorkflowError> {
        file.seek(SeekFrom::Start(0))
            .map_err(|source| WorkflowError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let mut text = String::new();
        file.read_to_string(&mut text)
            .map_err(|source| WorkflowError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        Ok(text.lines().filter(|line| !line.trim().is_empty()).count() as u64)
    }

    // ------------------------------------------------------------------
    // V2-B: shared multidimensional budget ledger (budget.jsonl)
    // ------------------------------------------------------------------

    pub fn reserve_budget(
        &self,
        owner_run_id: &str,
        originating_run_id: &str,
        key: String,
        kind: crate::model::CallKind,
        estimate_money: Option<u64>,
    ) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("budget write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("budget write lock poisoned".to_owned()))?;
        let ledger = self.reconstruct_budget_unlocked(owner_run_id)?;
        if ledger.reservations.contains_key(&key) {
            return Ok(());
        }
        if let Some(limit) = ledger.limit_calls {
            let committed = (ledger.used_calls + ledger.held_calls) as usize;
            if committed >= limit {
                return Err(WorkflowError::Invariant(format!(
                    "budget exhausted: {committed} calls committed (limit {limit})"
                )));
            }
        }
        if let (Some(cap), Some(estimate)) = (ledger.limit_money, estimate_money) {
            let committed = ledger.used_money + ledger.held_money;
            if committed.saturating_add(estimate) > cap {
                return Err(WorkflowError::Invariant(format!(
                    "budget exhausted: {committed} money used/held + {estimate} estimate > cap {cap}"
                )));
            }
        }
        self.append_budget_event_locked(
            owner_run_id,
            originating_run_id,
            BudgetEvent::Reserved {
                key,
                kind,
                estimate_money,
            },
        )
    }

    pub fn append_budget_event(
        &self,
        owner_run_id: &str,
        originating_run_id: &str,
        event: BudgetEvent,
    ) -> Result<(), WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("budget write lock poisoned".to_owned()))?;
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("budget write lock poisoned".to_owned()))?;
        self.append_budget_event_locked(owner_run_id, originating_run_id, event)
    }

    fn append_budget_event_locked(
        &self,
        owner_run_id: &str,
        originating_run_id: &str,
        event: BudgetEvent,
    ) -> Result<(), WorkflowError> {
        // A tree has one physical ledger owned by its root. The envelope retains
        // the originating child id for attribution, but capacity is reconstructed
        // from this owner file so sibling children cannot bypass root limits.
        let path = self.budget_path(owner_run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let sequence = Self::count_event_lines_in(&mut file, &path)? + 1;
        file.seek(SeekFrom::End(0))
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let envelope = BudgetEnvelope {
            version: BUDGET_SCHEMA_VERSION,
            sequence,
            at: Utc::now(),
            run_id: originating_run_id.to_owned(),
            owner_run_id: owner_run_id.to_owned(),
            event,
        };
        let mut bytes = serde_json::to_vec(&envelope)?;
        bytes.push(b'\n');
        let result = file
            .write_all(&bytes)
            .and_then(|_| file.sync_data())
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            });
        let unlock = FileExt::unlock(&file).map_err(|source| WorkflowError::Write { path, source });
        result.and(unlock)
    }

    pub fn read_budget_events(&self, run_id: &str) -> Result<Vec<BudgetEnvelope>, WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("budget read lock poisoned".to_owned()))?;
        self.read_budget_events_unlocked(run_id)
    }

    fn read_budget_events_unlocked(
        &self,
        run_id: &str,
    ) -> Result<Vec<BudgetEnvelope>, WorkflowError> {
        let path = self.budget_path(run_id);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(WorkflowError::Read { path, source }),
        };
        let mut out = Vec::new();
        for (line_num, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let envelope: BudgetEnvelope = serde_json::from_str(trimmed).map_err(|source| {
                WorkflowError::Invariant(format!(
                    "budget line {} malformed: {source}",
                    line_num + 1
                ))
            })?;
            if envelope.version != BUDGET_SCHEMA_VERSION {
                return Err(WorkflowError::Invariant(format!(
                    "budget version mismatch: expected {BUDGET_SCHEMA_VERSION}, got {}",
                    envelope.version
                )));
            }
            if envelope.owner_run_id != run_id {
                return Err(WorkflowError::Invariant(format!(
                    "budget run id mismatch: expected {run_id}, got {}",
                    envelope.owner_run_id
                )));
            }
            let expected_sequence = (out.len() + 1) as u64;
            if envelope.sequence != expected_sequence {
                return Err(WorkflowError::Invariant(format!(
                    "budget sequence gap in run {run_id}: expected {expected_sequence}, got {}",
                    envelope.sequence
                )));
            }
            out.push(envelope);
        }
        Ok(out)
    }

    pub fn reconstruct_budget(&self, run_id: &str) -> Result<BudgetLedger, WorkflowError> {
        let _process_guard = process_write_lock()
            .lock()
            .map_err(|_| WorkflowError::Invariant("budget read lock poisoned".to_owned()))?;
        self.reconstruct_budget_unlocked(run_id)
    }

    fn reconstruct_budget_unlocked(&self, run_id: &str) -> Result<BudgetLedger, WorkflowError> {
        let state = self.load_state(run_id)?;
        let events = self.read_budget_events_unlocked(run_id)?;
        let mut ledger = BudgetLedger {
            limit_calls: if state.contract.is_some() {
                Some(state.max_calls)
            } else {
                None
            },
            limit_money: state.money_cap,
            ..Default::default()
        };
        for envelope in events {
            match envelope.event {
                BudgetEvent::Reserved {
                    key,
                    kind,
                    estimate_money,
                } => {
                    let existing = ledger.reservations.get(&key);
                    let already_known = existing.is_some();
                    if !already_known {
                        ledger.held_calls = ledger.held_calls.saturating_add(1);
                        if let Some(est) = estimate_money {
                            ledger.held_money = ledger.held_money.saturating_add(est);
                        }
                    }
                    ledger
                        .reservations
                        .entry(key)
                        .or_insert_with(|| ReservationSummary {
                            kind,
                            estimate_money,
                            actual_money: None,
                            actual_tokens: 0,
                            settled: false,
                            released: false,
                        });
                }
                BudgetEvent::Settled {
                    key,
                    actual_money,
                    actual_tokens,
                } => {
                    if let Some(res) = ledger.reservations.get_mut(&key)
                        && !res.settled
                        && !res.released
                    {
                        res.settled = true;
                        res.actual_money = actual_money;
                        res.actual_tokens = actual_tokens;
                        ledger.used_calls = ledger.used_calls.saturating_add(1);
                        ledger.held_calls = ledger.held_calls.saturating_sub(1);
                        if let Some(held) = res.estimate_money {
                            ledger.held_money = ledger.held_money.saturating_sub(held);
                        }
                        if let Some(money) = actual_money {
                            ledger.used_money = ledger.used_money.saturating_add(money);
                        }
                        ledger.attributed_tokens =
                            ledger.attributed_tokens.saturating_add(actual_tokens);
                    }
                }
                BudgetEvent::Released { key, .. } => {
                    if let Some(res) = ledger.reservations.get_mut(&key)
                        && !res.released
                        && !res.settled
                    {
                        res.released = true;
                        ledger.held_calls = ledger.held_calls.saturating_sub(1);
                        if let Some(held) = res.estimate_money {
                            ledger.held_money = ledger.held_money.saturating_sub(held);
                        }
                    }
                }
            }
        }
        Ok(ledger)
    }

    /// Reconstruct run state purely from persisted artifacts: the versioned
    /// event stream (`events.jsonl`) for lifecycle/phase/gate, the journal
    /// (`journal.jsonl`) for call outcomes and active calls, and the static
    /// identity fields recorded at run creation. V2-A foundation: this never
    /// consults in-memory runtime state.
    pub fn reconstruct_state(&self, run_id: &str) -> Result<ReconstructedState, WorkflowError> {
        let state = self.load_state(run_id)?;
        let events = self.read_events(run_id)?;
        let journal = self.journal_index(run_id)?;

        // Seed from the persisted state so v1 runs (which have no event
        // stream) reconstruct their true status. For v2 runs the `RunStarted`
        // lifecycle event overwrites this immediately, so behavior is unchanged.
        let mut rs = ReconstructedState {
            version: state.version,
            contract: state.contract.clone(),
            run_id: run_id.to_owned(),
            parent_run_id: state.parent_run_id.clone(),
            name: state.name.clone(),
            max_parallel: state.max_parallel,
            max_calls: state.max_calls,
            money_cap: state.money_cap,
            status: state.status.clone(),
            phase: state.phase.clone(),
            active: BTreeMap::new(),
            waiting_gate: state.waiting_gate.clone(),
            supersede: state.supersede.clone(),
            decisions: BTreeMap::new(),
            result: state.result.clone(),
            error: state.error.clone(),
            resume_count: state.resume_count,
            call_count: journal.len(),
        };

        for envelope in events {
            match envelope.event {
                WorkflowEvent::RunStarted {
                    name,
                    max_parallel,
                    max_calls,
                    money_cap,
                    ..
                } => {
                    rs.name = name;
                    rs.max_parallel = max_parallel;
                    rs.max_calls = max_calls;
                    rs.money_cap = money_cap;
                    rs.status = RunStatus::Running;
                }
                WorkflowEvent::RunResumed { resume_count } => {
                    rs.resume_count = resume_count;
                    rs.status = RunStatus::Running;
                }
                WorkflowEvent::PhaseChanged { phase } => rs.phase = Some(phase),
                WorkflowEvent::GateOpened {
                    key,
                    origin_run_id,
                    label,
                    question,
                    expect,
                    current,
                    hint,
                } => {
                    rs.status = RunStatus::WaitingHuman;
                    rs.waiting_gate = Some(GateRequest {
                        key,
                        origin_run_id,
                        label,
                        question,
                        expect,
                        current,
                        hint,
                    });
                }
                WorkflowEvent::GateDecided {
                    key,
                    approved,
                    reason,
                    value,
                } => {
                    rs.waiting_gate = None;
                    rs.decisions.insert(
                        key,
                        GateDecision {
                            approved,
                            reason,
                            decided_at: envelope.at,
                            value,
                        },
                    );
                    rs.status = if approved {
                        RunStatus::Running
                    } else {
                        RunStatus::Failed
                    };
                }
                WorkflowEvent::RunSucceeded { result } => {
                    rs.status = RunStatus::Succeeded;
                    rs.result = result;
                    rs.waiting_gate = None;
                    rs.active.clear();
                }
                WorkflowEvent::RunFailed { error } => {
                    rs.status = RunStatus::Failed;
                    rs.error = Some(error);
                    rs.waiting_gate = None;
                    rs.active.clear();
                }
                WorkflowEvent::RunCancelled { error } => {
                    rs.status = RunStatus::Cancelled;
                    rs.error = Some(error);
                    rs.waiting_gate = None;
                    rs.active.clear();
                }
                WorkflowEvent::RunSuperseded {
                    reason,
                    evidence,
                    new_contract,
                } => {
                    rs.status = RunStatus::Superseded;
                    rs.supersede = Some(SupersedeInfo {
                        reason,
                        evidence,
                        new_contract,
                        decided_at: envelope.at,
                    });
                    rs.waiting_gate = None;
                    rs.active.clear();
                }
                WorkflowEvent::RunPaused => {
                    rs.status = RunStatus::Paused;
                    rs.active.clear();
                }
                WorkflowEvent::RunPausing => rs.status = RunStatus::Pausing,
                WorkflowEvent::RunCancelling { error } => {
                    rs.status = RunStatus::Cancelling;
                    rs.error = Some(error);
                }
            }
        }

        // Active calls: a journal key whose last recorded state is `Submitted`
        // has an in-flight worker. Terminal lifecycle events always win over a
        // stale submitted journal entry left behind by a crash.
        if !rs.status.is_terminal() {
            for (key, entry) in journal {
                if entry.state == crate::model::CallState::Submitted {
                    rs.active.insert(
                        key,
                        crate::model::ActiveCall {
                            kind: entry.kind,
                            label: entry.label,
                            started_at: entry.at,
                        },
                    );
                }
            }
        }

        Ok(rs)
    }

    pub fn request_pause(&self, run_id: &str) -> Result<(), WorkflowError> {
        self.assert_run(run_id)?;
        self.write(&self.pause_path(run_id), b"")
    }
    pub fn clear_pause(&self, run_id: &str) -> Result<(), WorkflowError> {
        match fs::remove_file(self.pause_path(run_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(WorkflowError::Write {
                path: self.pause_path(run_id),
                source,
            }),
        }
    }

    pub fn clear_cancel(&self, run_id: &str) -> Result<(), WorkflowError> {
        match fs::remove_file(self.cancel_path(run_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(WorkflowError::Write {
                path: self.cancel_path(run_id),
                source,
            }),
        }
    }
    pub fn request_cancel(&self, run_id: &str) -> Result<(), WorkflowError> {
        self.assert_run(run_id)?;
        self.write(&self.cancel_path(run_id), b"")
    }
    pub fn pause_requested(&self, run_id: &str) -> bool {
        self.pause_path(run_id).exists()
    }
    pub fn cancel_requested(&self, run_id: &str) -> bool {
        self.cancel_path(run_id).exists()
    }

    pub fn list_run_ids(&self) -> Result<Vec<String>, WorkflowError> {
        let dir = self.root.join("runs");
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|source| WorkflowError::Read {
            path: dir.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| WorkflowError::Read {
                path: dir.clone(),
                source,
            })?;
            if !entry
                .file_type()
                .map_err(|source| WorkflowError::Read {
                    path: dir.clone(),
                    source,
                })?
                .is_dir()
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let modified = entry.metadata().and_then(|meta| meta.modified()).ok();
            entries.push((modified, name));
        }
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.0));
        Ok(entries.into_iter().map(|(_, name)| name).collect())
    }

    pub fn child_run_ids(&self, parent_run_id: &str) -> Result<Vec<String>, WorkflowError> {
        let mut children = Vec::new();
        for run_id in self.list_run_ids()? {
            let state = match self.load_state(&run_id) {
                Ok(state) => state,
                Err(_) => continue,
            };
            if state.parent_run_id.as_deref() == Some(parent_run_id) {
                children.push(run_id);
            }
        }
        children.sort();
        Ok(children)
    }

    fn assert_run(&self, run_id: &str) -> Result<(), WorkflowError> {
        if self.run_dir(run_id).is_dir() {
            Ok(())
        } else {
            Err(WorkflowError::RunNotFound(run_id.to_owned()))
        }
    }
    fn write(&self, path: &Path, bytes: &[u8]) -> Result<(), WorkflowError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| WorkflowError::Write {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::write(path, bytes).map_err(|source| WorkflowError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

fn process_write_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn default_state_root() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(r"D:\AgentWork\state\servitor-workflows")
    } else {
        std::env::temp_dir().join("servitor-workflows")
    }
}
