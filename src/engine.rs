use crate::agent::Transport;
use crate::error::WorkflowError;
use crate::model::{GateDecision, PublicRun, RunState, RunStatus};
use crate::run_summary;
use crate::scheduler::{RuntimeHost, Scheduler};
use crate::script;
use crate::store::WorkflowStore;
use chrono::Utc;
use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

pub struct Engine {
    store: Arc<WorkflowStore>,
    transport: Arc<dyn Transport>,
}

impl Engine {
    pub fn new(store: WorkflowStore, transport: Arc<dyn Transport>) -> Self {
        Self {
            store: Arc::new(store),
            transport,
        }
    }
    pub fn store(&self) -> &WorkflowStore {
        &self.store
    }

    pub fn start(
        &self,
        path: &Path,
        args: Value,
        max_parallel: usize,
        max_calls: usize,
    ) -> Result<RunState, WorkflowError> {
        let state = self.prepare(path, args, max_parallel, max_calls)?;
        self.execute_existing(&state.run_id)
    }

    pub fn prepare(
        &self,
        path: &Path,
        args: Value,
        max_parallel: usize,
        max_calls: usize,
    ) -> Result<RunState, WorkflowError> {
        if max_parallel == 0 || max_calls == 0 {
            return Err(WorkflowError::InvalidOperation(
                "limits must be positive".to_owned(),
            ));
        }
        let script = std::fs::read_to_string(path).map_err(|source| WorkflowError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        validate_script(&script)?;
        let cwd = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|source| WorkflowError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let now = Utc::now();
        let run_id = Uuid::now_v7().to_string();
        let state = RunState {
            version: 1,
            run_id: run_id.clone(),
            name: path
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("workflow")
                .to_owned(),
            cwd,
            status: RunStatus::Running,
            created_at: now,
            updated_at: now,
            args,
            max_parallel,
            max_calls,
            resume_count: 0,
            phase: None,
            active: Default::default(),
            waiting_gate: None,
            supersede: None,
            decisions: Default::default(),
            result: None,
            error: None,
            report: None,
            run_summary: None,
            journal_path: self.store.journal_path(&run_id),
        };
        self.store.create_run(&state, &script)?;
        Ok(state)
    }

    pub fn execute_existing(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        self.store.load_state(run_id)?;
        self.execute(run_id)
    }

    pub fn resume(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        let state = self.store.load_state(run_id)?;
        // Succeeded/Cancelled/Superseded must not re-execute. Failed remains resumable.
        if state.status.blocks_resume_rerun() {
            return self.ensure_terminal_artifacts(state);
        }
        self.store.update_state(run_id, |state| {
            state.resume_count = state.resume_count.saturating_add(1);
        })?;
        self.store.clear_pause(run_id)?;
        self.execute(run_id)
    }

    pub fn get(&self, run_id: &str) -> Result<PublicRun, WorkflowError> {
        let mut state = self.store.load_state(run_id)?;
        self.populate_journal_path(&mut state);
        Ok(PublicRun::from(&state))
    }

    pub fn list(
        &self,
        limit: usize,
        status_filter: Option<&str>,
    ) -> Result<serde_json::Value, WorkflowError> {
        let limit = limit.max(1);
        let ids = self.store.list_run_ids()?;
        let total = ids.len();
        let mut runs = Vec::new();
        let mut truncated = false;
        for run_id in ids {
            let mut state = match self.store.load_state(&run_id) {
                Ok(state) => state,
                Err(_) => continue,
            };
            self.populate_journal_path(&mut state);
            if let Some(filter) = status_filter {
                let status = match state.status {
                    RunStatus::Running => "running",
                    RunStatus::WaitingHuman => "waiting_human",
                    RunStatus::Pausing => "pausing",
                    RunStatus::Paused => "paused",
                    RunStatus::Cancelling => "cancelling",
                    RunStatus::Succeeded => "succeeded",
                    RunStatus::Failed => "failed",
                    RunStatus::Cancelled => "cancelled",
                    RunStatus::Superseded => "superseded",
                };
                if status != filter {
                    continue;
                }
            }
            if runs.len() >= limit {
                truncated = true;
                break;
            }
            runs.push(PublicRun::from(&state));
        }
        Ok(serde_json::json!({
            "runs": runs,
            "count": runs.len(),
            "limit": limit,
            "truncated": truncated,
            "total": total,
        }))
    }

    pub fn approve(
        &self,
        run_id: &str,
        approved: bool,
        reason: String,
        value: Option<Value>,
    ) -> Result<RunState, WorkflowError> {
        if reason.trim().is_empty() && value.is_none() {
            return Err(WorkflowError::InvalidOperation(
                "decision reason or value is required".to_owned(),
            ));
        }
        let state = self.store.load_state(run_id)?;
        if state.status != RunStatus::WaitingHuman {
            return Err(WorkflowError::InvalidOperation(
                "run is not waiting for a gate".to_owned(),
            ));
        }
        let gate = state
            .waiting_gate
            .ok_or_else(|| WorkflowError::Invariant("waiting run has no gate".to_owned()))?;
        self.store.update_state(run_id, |state| {
            state.decisions.insert(
                gate.key.clone(),
                GateDecision {
                    approved,
                    reason: reason.clone(),
                    decided_at: Utc::now(),
                    value,
                },
            );
            state.waiting_gate = None;
            if approved {
                state.status = RunStatus::Running;
            } else {
                state.status = RunStatus::Failed;
                state.error = Some(reason);
            }
        })?;
        if approved {
            self.execute(run_id)
        } else {
            let state = self.store.load_state(run_id)?;
            self.ensure_terminal_artifacts(state)
        }
    }

    pub fn pause(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        self.store.request_pause(run_id)?;
        self.store.update_state(run_id, |state| {
            state.status = if state.active.is_empty() {
                RunStatus::Paused
            } else {
                RunStatus::Pausing
            };
        })
    }

    /// Validate a workflow script without creating a run: same emptiness/meta
    /// checks and the same engine-wrap parse that `start` performs.
    pub fn check(&self, path: &Path) -> Result<Value, WorkflowError> {
        let script = std::fs::read_to_string(path).map_err(|source| WorkflowError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        validate_script(&script)?;
        Ok(serde_json::json!({
            "check": "ok",
            "workflow": path.display().to_string(),
        }))
    }

    pub fn cancel(&self, run_id: &str, reason: String) -> Result<RunState, WorkflowError> {
        self.store.request_cancel(run_id)?;
        let state = self.store.update_state(run_id, |state| {
            state.error = Some(format!("cancelled: {reason}"));
            state.status = if state.active.is_empty() {
                RunStatus::Cancelled
            } else {
                RunStatus::Cancelling
            };
        })?;
        if state.status.is_terminal() {
            self.ensure_terminal_artifacts(state)
        } else {
            Ok(state)
        }
    }

    pub fn supersede(
        &self,
        run_id: &str,
        reason: String,
        evidence: Option<String>,
        new_contract: Option<String>,
    ) -> Result<RunState, WorkflowError> {
        let state = self
            .store
            .mark_superseded(run_id, reason, evidence, new_contract)?;
        self.ensure_terminal_artifacts(state)
    }

    pub fn inspect(&self, run_id: &str) -> Result<Inspection, WorkflowError> {
        Ok(Inspection {
            state: self.store.load_state(run_id)?,
            script_path: self.store.script_path(run_id),
            state_path: self.store.state_path(run_id),
            journal_path: self.store.journal_path(run_id),
            run_summary_path: self.store.run_summary_path(run_id),
        })
    }

    fn execute(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        let initial = self.store.update_state(run_id, |state| {
            state.status = RunStatus::Running;
            state.active.clear();
            state.waiting_gate = None;
            state.result = None;
            state.error = None;
            state.report = None;
            state.run_summary = None;
        })?;
        if self.store.cancel_requested(run_id) {
            let state = self.store.update_state(run_id, |state| {
                state.status = RunStatus::Cancelled;
            })?;
            return self.ensure_terminal_artifacts(state);
        }
        let source = self.store.load_script(run_id)?;
        let runtime = Arc::new(RuntimeHost {
            run_id: run_id.to_owned(),
            cwd: initial.cwd.clone(),
            store: Arc::clone(&self.store),
            transport: Arc::clone(&self.transport),
            scheduler: Scheduler::new(initial.max_parallel),
        });
        let result = script::execute(runtime, &source, &initial.args, initial.max_calls);
        let current = self.store.load_state(run_id)?;
        if current.status == RunStatus::Superseded {
            return self.ensure_terminal_artifacts(current);
        }
        if self.store.cancel_requested(run_id) {
            let state = self.store.update_state(run_id, |state| {
                state.status = RunStatus::Cancelled;
                state.active.clear();
            })?;
            let _ = self.store.clear_cancel(run_id);
            return self.ensure_terminal_artifacts(state);
        }
        if self.store.pause_requested(run_id) {
            let state = self.store.update_state(run_id, |state| {
                state.status = RunStatus::Paused;
                state.active.clear();
            })?;
            self.store.clear_pause(run_id)?;
            return Ok(state);
        }
        if current.status == RunStatus::WaitingHuman {
            return self.refresh_waiting_summary(current);
        }
        let state = match result {
            Ok(value) => match delivery_report(&value) {
                Ok(report) => self.store.update_state(run_id, |state| {
                    state.status = RunStatus::Succeeded;
                    state.result = Some(value);
                    state.error = None;
                    state.report = report;
                    state.active.clear();
                }),
                Err(error) => self.store.update_state(run_id, |state| {
                    state.status = RunStatus::Failed;
                    state.result = Some(value);
                    state.error = Some(error.to_string());
                    state.report = None;
                    state.active.clear();
                }),
            },
            Err(error) => self.store.update_state(run_id, |state| {
                state.status = RunStatus::Failed;
                state.error = Some(error.to_string());
                state.active.clear();
            }),
        }?;
        self.ensure_terminal_artifacts(state)
    }

    fn populate_journal_path(&self, state: &mut RunState) {
        if state.journal_path.as_os_str().is_empty() {
            state.journal_path = self.store.journal_path(&state.run_id);
        }
    }

    fn refresh_waiting_summary(&self, state: RunState) -> Result<RunState, WorkflowError> {
        let path = run_summary::write(&self.store, &state)?;
        self.store
            .update_state(&state.run_id, |state| state.run_summary = Some(path))
    }

    fn ensure_terminal_artifacts(&self, state: RunState) -> Result<RunState, WorkflowError> {
        if !state.status.is_terminal() {
            return Ok(state);
        }
        let report = if state.status == RunStatus::Succeeded {
            match state.result.as_ref().map(delivery_report).transpose() {
                Ok(report) => report.flatten(),
                Err(error) => {
                    return self.store.update_state(&state.run_id, |state| {
                        state.status = RunStatus::Failed;
                        state.error = Some(error.to_string());
                        state.report = None;
                    });
                }
            }
        } else {
            None
        };
        let path = match run_summary::write(&self.store, &state) {
            Ok(path) => path,
            Err(error) => {
                return self.store.update_state(&state.run_id, |state| {
                    state.status = RunStatus::Failed;
                    state.error = Some(format!("run summary generation failed: {error}"));
                    state.run_summary = None;
                });
            }
        };
        if state.report == report && state.run_summary.as_ref() == Some(&path) {
            return Ok(state);
        }
        self.store.update_state(&state.run_id, |state| {
            state.report = report;
            state.run_summary = Some(path);
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Inspection {
    pub state: RunState,
    pub script_path: PathBuf,
    pub state_path: PathBuf,
    pub journal_path: PathBuf,
    pub run_summary_path: PathBuf,
}

fn delivery_report(value: &Value) -> Result<Option<PathBuf>, WorkflowError> {
    let Some(raw) = value.get("report") else {
        return Ok(None);
    };
    let path = raw.as_str().map(PathBuf::from).ok_or_else(|| {
        WorkflowError::InvalidOperation(
            "delivery report must be an absolute path string".to_owned(),
        )
    })?;
    if !path.is_absolute() {
        return Err(WorkflowError::InvalidOperation(
            "delivery report path must be absolute".to_owned(),
        ));
    }
    let metadata = std::fs::metadata(&path).map_err(|_| {
        WorkflowError::InvalidOperation(format!(
            "delivery report does not exist: {}",
            path.display()
        ))
    })?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(WorkflowError::InvalidOperation(format!(
            "delivery report is not a non-empty file: {}",
            path.display()
        )));
    }
    Ok(Some(path))
}

fn validate_script(script: &str) -> Result<(), WorkflowError> {
    if script.trim().is_empty() {
        return Err(WorkflowError::InvalidWorkflow("script is empty".to_owned()));
    }
    if !script.contains("export const meta") {
        return Err(WorkflowError::InvalidWorkflow(
            "script must declare `export const meta`".to_owned(),
        ));
    }
    script::parse_check(script)
}
