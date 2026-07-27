use crate::agent::Transport;
use crate::error::WorkflowError;
use crate::model::{GateDecision, PublicRun, RunState, RunStatus, WorkflowEvent};
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

    fn transition<F>(
        &self,
        run_id: &str,
        event: WorkflowEvent,
        update: F,
    ) -> Result<RunState, WorkflowError>
    where
        F: FnOnce(&mut RunState),
    {
        let state = self.store.load_state(run_id)?;
        if state.contract.as_deref() == Some("workflow.v2") {
            self.store
                .transition(run_id, state.parent_run_id.as_deref(), event, update)
        } else {
            self.store.update_state(run_id, update)
        }
    }

    fn transition_many<F, I>(
        &self,
        run_id: &str,
        events: I,
        update: F,
    ) -> Result<RunState, WorkflowError>
    where
        F: FnOnce(&mut RunState),
        I: IntoIterator<Item = WorkflowEvent>,
    {
        let state = self.store.load_state(run_id)?;
        if state.contract.as_deref() == Some("workflow.v2") {
            self.store
                .transition_many(run_id, state.parent_run_id.as_deref(), events, update)
        } else {
            self.store.update_state(run_id, update)
        }
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
        // New runs must opt into the v2 contract explicitly. Legacy v1 runs are
        // resumed from their persisted state and never pass through prepare().
        let contract = validate_script(&script)?;
        let version = 2;
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
            version,
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
            contract,
            parent_run_id: None,
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
        if state.contract.as_deref() == Some("workflow.v2") {
            self.store.append_event(
                &state.run_id,
                None,
                WorkflowEvent::RunStarted {
                    name: state.name.clone(),
                    args: state.args.clone(),
                    max_parallel: state.max_parallel,
                    max_calls: state.max_calls,
                },
            )?;
        }
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
        self.transition(
            run_id,
            WorkflowEvent::RunResumed {
                resume_count: state.resume_count.saturating_add(1),
            },
            |state| {
                state.resume_count = state.resume_count.saturating_add(1);
            },
        )?;
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
        let decided_event = WorkflowEvent::GateDecided {
            key: gate.key.clone(),
            approved,
            reason: reason.clone(),
            value: value.clone(),
        };
        if approved {
            self.transition(run_id, decided_event, |state| {
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
                state.status = RunStatus::Running;
            })?;
            self.execute(run_id)
        } else {
            let state = self.transition_many(
                run_id,
                [
                    decided_event,
                    WorkflowEvent::RunFailed {
                        error: reason.clone(),
                    },
                ],
                |state| {
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
                    state.status = RunStatus::Failed;
                    state.error = Some(reason);
                },
            )?;
            self.ensure_terminal_artifacts(state)
        }
    }

    pub fn pause(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        self.store.request_pause(run_id)?;
        let state = self.store.load_state(run_id)?;
        if state.active.is_empty() {
            self.transition(run_id, WorkflowEvent::RunPaused, |state| {
                state.status = RunStatus::Paused;
            })
        } else {
            self.transition(run_id, WorkflowEvent::RunPausing, |state| {
                state.status = RunStatus::Pausing;
            })
        }
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
        let state = self.store.load_state(run_id)?;
        let error = format!("cancelled: {reason}");
        let state = if state.active.is_empty() {
            self.transition(
                run_id,
                WorkflowEvent::RunCancelled {
                    error: error.clone(),
                },
                |state| {
                    state.error = Some(error);
                    state.status = RunStatus::Cancelled;
                    state.waiting_gate = None;
                    state.active.clear();
                },
            )?
        } else {
            self.transition(
                run_id,
                WorkflowEvent::RunCancelling {
                    error: error.clone(),
                },
                |state| {
                    state.error = Some(error);
                    state.status = RunStatus::Cancelling;
                },
            )?
        };
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
        if reason.trim().is_empty() {
            return Err(WorkflowError::InvalidOperation(
                "supersede reason is required".to_owned(),
            ));
        }
        self.store.request_cancel(run_id)?;
        let event = WorkflowEvent::RunSuperseded {
            reason: reason.clone(),
            evidence: evidence.clone(),
            new_contract: new_contract.clone(),
        };
        let state = self.transition(run_id, event, |state| {
            state.status = RunStatus::Superseded;
            state.active.clear();
            state.waiting_gate = None;
            state.supersede = Some(crate::model::SupersedeInfo {
                reason,
                evidence,
                new_contract,
                decided_at: Utc::now(),
            });
        })?;
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
        let cancel_requested = self.store.cancel_requested(run_id);
        let initial = self.store.update_state(run_id, |state| {
            state.status = RunStatus::Running;
            state.active.clear();
            state.waiting_gate = None;
            state.result = None;
            if !cancel_requested {
                state.error = None;
            }
            state.report = None;
            state.run_summary = None;
        })?;
        if self.store.cancel_requested(run_id) {
            let error = initial
                .error
                .clone()
                .unwrap_or_else(|| "workflow cancelled".to_owned());
            let state = self.transition(
                run_id,
                WorkflowEvent::RunCancelled {
                    error: error.clone(),
                },
                |state| {
                    state.status = RunStatus::Cancelled;
                    state.error = Some(error);
                    state.active.clear();
                },
            )?;
            let _ = self.store.clear_cancel(run_id);
            return self.ensure_terminal_artifacts(state);
        }
        let source = self.store.load_script(run_id)?;
        let runtime = Arc::new(RuntimeHost {
            run_id: run_id.to_owned(),
            cwd: initial.cwd.clone(),
            store: Arc::clone(&self.store),
            transport: Arc::clone(&self.transport),
            scheduler: Scheduler::new(initial.max_parallel),
            contract: initial.contract.clone(),
            parent_run_id: initial.parent_run_id.clone(),
        });
        let result = script::execute(runtime, &source, &initial.args, initial.max_calls);
        let current = self.store.load_state(run_id)?;
        if current.status == RunStatus::Superseded {
            return self.ensure_terminal_artifacts(current);
        }
        if self.store.cancel_requested(run_id) {
            let state = self.transition(
                run_id,
                WorkflowEvent::RunCancelled {
                    error: current
                        .error
                        .clone()
                        .unwrap_or_else(|| "workflow cancelled".to_owned()),
                },
                |state| {
                    state.status = RunStatus::Cancelled;
                    state.active.clear();
                },
            )?;
            let _ = self.store.clear_cancel(run_id);
            return self.ensure_terminal_artifacts(state);
        }
        if self.store.pause_requested(run_id) {
            let state = self.transition(run_id, WorkflowEvent::RunPaused, |state| {
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
                Ok(report) => self.transition(
                    run_id,
                    WorkflowEvent::RunSucceeded {
                        result: Some(value.clone()),
                    },
                    |state| {
                        state.status = RunStatus::Succeeded;
                        state.result = Some(value);
                        state.error = None;
                        state.report = report;
                        state.active.clear();
                    },
                ),
                Err(error) => self.transition(
                    run_id,
                    WorkflowEvent::RunFailed {
                        error: error.to_string(),
                    },
                    |state| {
                        state.status = RunStatus::Failed;
                        state.result = Some(value);
                        state.error = Some(error.to_string());
                        state.report = None;
                        state.active.clear();
                    },
                ),
            },
            Err(error) => self.transition(
                run_id,
                WorkflowEvent::RunFailed {
                    error: error.to_string(),
                },
                |state| {
                    state.status = RunStatus::Failed;
                    state.error = Some(error.to_string());
                    state.active.clear();
                },
            ),
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
                    let error = error.to_string();
                    return self.transition(
                        &state.run_id,
                        WorkflowEvent::RunFailed {
                            error: error.clone(),
                        },
                        |state| {
                            state.status = RunStatus::Failed;
                            state.error = Some(error);
                            state.report = None;
                        },
                    );
                }
            }
        } else {
            None
        };
        let path = match run_summary::write(&self.store, &state) {
            Ok(path) => path,
            Err(error) => {
                let error = format!("run summary generation failed: {error}");
                return self.transition(
                    &state.run_id,
                    WorkflowEvent::RunFailed {
                        error: error.clone(),
                    },
                    |state| {
                        state.status = RunStatus::Failed;
                        state.error = Some(error);
                        state.run_summary = None;
                    },
                );
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

fn validate_script(script: &str) -> Result<Option<String>, WorkflowError> {
    if script.trim().is_empty() {
        return Err(WorkflowError::InvalidWorkflow("script is empty".to_owned()));
    }
    script::parse_check(script)?;
    // New runs are v2-only. Existing v1 runs bypass this path and keep their
    // persisted script, state, journal, and frozen replay semantics.
    let contract = script::contract_of(script)?;
    if contract.as_deref() != Some("workflow.v2") {
        return Err(WorkflowError::InvalidWorkflow(
            "new workflows must declare `meta.contract: \"workflow.v2\"`".to_owned(),
        ));
    }
    Ok(contract)
}
