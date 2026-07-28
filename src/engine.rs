use crate::agent::Transport;
use crate::boundary::{
    BoundaryEvent, BoundaryPolicy, IsolationLevel, ensure_child_narrows, resolve_policy,
};
use crate::budget::Budget;
use crate::capabilities::{
    CapabilityEvent, CapabilityPolicy, ensure_child_narrows as ensure_child_capabilities_narrow,
    validate_policy as validate_capability_policy,
};
use crate::error::WorkflowError;
use crate::model::{CallState, GateDecision, PublicRun, RunState, RunStatus, WorkflowEvent};
use crate::run_summary;
use crate::scheduler::{RuntimeHost, Scheduler};
use crate::script;
use crate::store::WorkflowStore;
use chrono::Utc;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
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

    pub(crate) fn from_shared(store: Arc<WorkflowStore>, transport: Arc<dyn Transport>) -> Self {
        Self { store, transport }
    }
    pub fn store(&self) -> &WorkflowStore {
        &self.store
    }

    pub(crate) fn prepare_child(
        &self,
        parent_run_id: &str,
        parent_call_key: &str,
        path: &Path,
        args: Value,
    ) -> Result<RunState, WorkflowError> {
        const MAX_WORKFLOW_DEPTH: usize = 16;
        let parent = self.store.load_state(parent_run_id)?;
        let source = std::fs::read_to_string(path).map_err(|source| WorkflowError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let contract = validate_script(&source)?;
        let child_boundary = parse_meta_boundary(&source)?;
        let child_capabilities = parse_meta_capabilities(&source)?;
        ensure_supported_isolation(&source)?;
        let cwd = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|source| WorkflowError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let child_boundary = child_boundary
            .as_ref()
            .map(|policy| resolve_policy(policy, &cwd))
            .transpose()
            .map_err(WorkflowError::InvalidWorkflow)?;
        if let (Some(parent_boundary), Some(child_boundary)) =
            (parent.boundary.as_ref(), child_boundary.as_ref())
        {
            ensure_child_narrows(parent_boundary, child_boundary)
                .map_err(WorkflowError::InvalidWorkflow)?;
        }
        let child_boundary = child_boundary.or_else(|| parent.boundary.clone());
        if let (Some(parent_capabilities), Some(child_capabilities)) =
            (parent.capabilities.as_ref(), child_capabilities.as_ref())
        {
            ensure_child_capabilities_narrow(parent_capabilities, child_capabilities)
                .map_err(WorkflowError::InvalidWorkflow)?;
        }
        let child_capabilities = child_capabilities.or_else(|| parent.capabilities.clone());
        let worktree = parent.worktree.clone();
        let cwd = worktree
            .as_ref()
            .map(|worktree| worktree.path.clone())
            .unwrap_or(cwd);
        let root_run_id = parent
            .root_run_id
            .clone()
            .unwrap_or_else(|| parent.run_id.clone());
        let mut ancestors = vec![parent.run_id.clone()];
        let mut cursor = parent.parent_run_id.clone();
        while let Some(run_id) = cursor {
            let ancestor = self.store.load_state(&run_id)?;
            ancestors.push(ancestor.run_id.clone());
            cursor = ancestor.parent_run_id.clone();
        }
        if ancestors.len() >= MAX_WORKFLOW_DEPTH {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow nesting exceeds maximum depth {MAX_WORKFLOW_DEPTH}"
            )));
        }
        let digest = sha256(&source);
        for ancestor_run_id in &ancestors {
            if sha256(&self.store.load_script(ancestor_run_id)?) == digest {
                return Err(WorkflowError::InvalidWorkflow(
                    "workflow child cycle detected before dispatch".to_owned(),
                ));
            }
        }
        let identity = format!(
            "{root_run_id}\0{parent_run_id}\0{parent_call_key}\0{}\0{}",
            path.canonicalize()
                .map_err(|source| WorkflowError::Read {
                    path: path.to_path_buf(),
                    source
                })?
                .display(),
            serde_json::to_string(&args)?
        );
        let run_id = format!("child-{}", sha256(&identity));
        if let Ok(existing) = self.store.load_state(&run_id) {
            if existing.parent_run_id.as_deref() != Some(parent_run_id)
                || existing.parent_call_key.as_deref() != Some(parent_call_key)
                || existing.root_run_id.as_deref() != Some(&root_run_id)
            {
                return Err(WorkflowError::Invariant(
                    "persisted child identity does not match parent workflow call".to_owned(),
                ));
            }
            return Ok(existing);
        }
        let now = Utc::now();
        let state = RunState {
            version: 2,
            run_id: run_id.clone(),
            name: path
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("workflow-child")
                .to_owned(),
            cwd,
            contract: Some(contract),
            parent_run_id: Some(parent_run_id.to_owned()),
            root_run_id: Some(root_run_id),
            parent_call_key: Some(parent_call_key.to_owned()),
            money_cap: parent.money_cap,
            boundary: child_boundary.clone(),
            capabilities: child_capabilities.clone(),
            worktree,
            status: RunStatus::Running,
            created_at: now,
            updated_at: now,
            args,
            max_parallel: parent.max_parallel,
            max_calls: parent.max_calls,
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
        self.store.create_run(&state, &source)?;
        if let Some(policy) = state.boundary.clone() {
            self.store
                .append_boundary_event(&state.run_id, BoundaryEvent::Declared { policy })?;
        }
        if let Some(policy) = state.boundary.clone() {
            self.store.append_boundary_event(
                parent_run_id,
                BoundaryEvent::ChildDeclared {
                    key: parent_call_key.to_owned(),
                    policy,
                },
            )?;
        }
        if let Some(policy) = state.capabilities.clone() {
            self.store
                .append_capability_event(&state.run_id, CapabilityEvent::Declared { policy })?;
        }
        self.store.append_event(
            &state.run_id,
            Some(parent_run_id),
            WorkflowEvent::RunStarted {
                name: state.name.clone(),
                args: state.args.clone(),
                max_parallel: state.max_parallel,
                max_calls: state.max_calls,
                money_cap: state.money_cap,
            },
        )?;
        Ok(state)
    }

    pub(crate) fn execute_child(
        &self,
        run_id: &str,
        scheduler: Arc<Scheduler>,
    ) -> Result<RunState, WorkflowError> {
        self.execute_with_scheduler(run_id, scheduler)
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
        if crate::script::is_current_contract(state.contract.as_deref()) {
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
        if crate::script::is_current_contract(state.contract.as_deref()) {
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

    /// Parse the workflow script's `meta` block (including `meta.moneyCap`) into
    /// a `RunState` seed.
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
        // Every new run declares the current contract explicitly.
        let contract = validate_script(&script)?;
        let declared_boundary = parse_meta_boundary(&script)?;
        let capabilities = parse_meta_capabilities(&script)?;
        ensure_supported_isolation(&script)?;
        let version = 2;
        let cwd = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|source| WorkflowError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let money_cap = parse_meta_money_cap(&script);
        let mut boundary = declared_boundary
            .as_ref()
            .map(|policy| resolve_policy(policy, &cwd))
            .transpose()
            .map_err(WorkflowError::InvalidWorkflow)?;
        let now = Utc::now();
        let run_id = Uuid::now_v7().to_string();
        let worktree = if boundary
            .as_ref()
            .is_some_and(|policy| policy.isolation == IsolationLevel::Worktree)
        {
            Some(
                crate::isolation::create_worktree(&cwd, self.store.root(), &run_id)
                    .map_err(WorkflowError::InvalidWorkflow)?,
            )
        } else {
            None
        };
        let cwd = worktree
            .as_ref()
            .map(|worktree| worktree.path.clone())
            .unwrap_or(cwd);
        if let Some(boundary) = boundary.as_mut()
            && boundary.isolation == IsolationLevel::Worktree
        {
            boundary.read_paths = vec![cwd.clone()];
            boundary.write_paths = vec![cwd.clone()];
        }
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
            contract: Some(contract),
            parent_run_id: None,
            root_run_id: Some(run_id.clone()),
            parent_call_key: None,
            money_cap,
            boundary,
            capabilities: capabilities.clone(),
            worktree,
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
        if let Some(policy) = state.boundary.clone() {
            self.store
                .append_boundary_event(&state.run_id, BoundaryEvent::Declared { policy })?;
        }
        if let Some(policy) = state.capabilities.clone() {
            self.store
                .append_capability_event(&state.run_id, CapabilityEvent::Declared { policy })?;
        }
        if crate::script::is_current_contract(state.contract.as_deref()) {
            self.store.append_event(
                &state.run_id,
                None,
                WorkflowEvent::RunStarted {
                    name: state.name.clone(),
                    args: state.args.clone(),
                    max_parallel: state.max_parallel,
                    max_calls: state.max_calls,
                    money_cap: state.money_cap,
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
        self.reconcile_budget(run_id, "recovered on resume")?;
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
        let requested = self.store.load_state(run_id)?;
        if let Some(origin_run_id) = requested
            .waiting_gate
            .as_ref()
            .and_then(|gate| gate.origin_run_id.as_deref())
            .filter(|origin| *origin != run_id)
        {
            let state = self.approve(origin_run_id, approved, reason, value)?;
            return if state.status == RunStatus::Succeeded || state.status == RunStatus::Failed {
                self.resume_waiting_ancestors(run_id, origin_run_id)
            } else {
                Ok(state)
            };
        }
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

    fn resume_waiting_ancestors(
        &self,
        requested_run_id: &str,
        origin_run_id: &str,
    ) -> Result<RunState, WorkflowError> {
        let mut chain = Vec::new();
        let mut cursor = origin_run_id.to_owned();
        while cursor != requested_run_id {
            let state = self.store.load_state(&cursor)?;
            let parent_run_id = state.parent_run_id.ok_or_else(|| {
                WorkflowError::Invariant(
                    "bubbled gate origin is not a descendant of the requested run".to_owned(),
                )
            })?;
            chain.push(parent_run_id.clone());
            cursor = parent_run_id;
        }
        let mut resumed = self.store.load_state(origin_run_id)?;
        for run_id in chain {
            resumed = self.resume(&run_id)?;
        }
        Ok(resumed)
    }

    pub fn pause(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        self.propagate_request(run_id, true)?;
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
        parse_meta_boundary(&script)?;
        parse_meta_capabilities(&script)?;
        ensure_supported_isolation(&script)?;
        Ok(serde_json::json!({
            "check": "ok",
            "workflow": path.display().to_string(),
        }))
    }

    pub fn cancel(&self, run_id: &str, reason: String) -> Result<RunState, WorkflowError> {
        self.propagate_request(run_id, false)?;
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
            self.reconcile_budget(run_id, "cancelled")?;
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
        self.propagate_request(run_id, false)?;
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
        self.reconcile_budget(run_id, "superseded")?;
        self.ensure_terminal_artifacts(state)
    }

    pub fn inspect(&self, run_id: &str) -> Result<Inspection, WorkflowError> {
        let state = self.store.load_state(run_id)?;
        let budget = if crate::script::is_current_contract(state.contract.as_deref()) {
            Some(self.store.reconstruct_budget(run_id)?)
        } else {
            None
        };
        let budget_path = crate::script::is_current_contract(state.contract.as_deref())
            .then(|| self.store.budget_path(run_id));
        let boundary_path = state
            .boundary
            .is_some()
            .then(|| self.store.boundary_path(run_id));
        let boundary_events = boundary_path
            .as_ref()
            .map(|_| self.store.read_boundary_events(run_id))
            .transpose()?;
        let capabilities_path = state
            .capabilities
            .is_some()
            .then(|| self.store.capabilities_path(run_id));
        let capability_events = capabilities_path
            .as_ref()
            .map(|_| self.store.read_capability_events(run_id))
            .transpose()?;
        Ok(Inspection {
            state,
            script_path: self.store.script_path(run_id),
            state_path: self.store.state_path(run_id),
            journal_path: self.store.journal_path(run_id),
            budget_path,
            budget,
            boundary_path,
            boundary_events,
            capabilities_path,
            capability_events,
            run_summary_path: self.store.run_summary_path(run_id),
        })
    }

    fn propagate_request(&self, run_id: &str, pause: bool) -> Result<(), WorkflowError> {
        for child_run_id in self.store.child_run_ids(run_id)? {
            self.propagate_request(&child_run_id, pause)?;
            let child = self.store.load_state(&child_run_id)?;
            if child.status.is_terminal() {
                continue;
            }
            if pause {
                self.store.request_pause(&child_run_id)?;
            } else {
                self.store.request_cancel(&child_run_id)?;
            }
        }
        Ok(())
    }

    fn reconcile_budget(&self, run_id: &str, reason: &str) -> Result<(), WorkflowError> {
        let root = self.store.load_state(run_id)?;
        let root_run_id = root
            .root_run_id
            .clone()
            .unwrap_or_else(|| root.run_id.clone());
        for origin_run_id in self.tree_run_ids(&root_run_id)? {
            self.reconcile_budget_for_origin(&origin_run_id, &root_run_id, reason)?;
        }
        Ok(())
    }

    fn tree_run_ids(&self, run_id: &str) -> Result<Vec<String>, WorkflowError> {
        let mut ids = vec![run_id.to_owned()];
        for child in self.store.child_run_ids(run_id)? {
            ids.extend(self.tree_run_ids(&child)?);
        }
        Ok(ids)
    }

    fn reconcile_budget_for_origin(
        &self,
        run_id: &str,
        root_run_id: &str,
        reason: &str,
    ) -> Result<(), WorkflowError> {
        let state = self.store.load_state(run_id)?;
        if !crate::script::is_current_contract(state.contract.as_deref()) {
            return Ok(());
        }
        let budget = Budget::new(
            Arc::clone(&self.store),
            run_id.to_owned(),
            root_run_id.to_owned(),
        );
        let journal = self.store.journal_index(run_id)?;
        let ledger = budget.ledger()?;
        let key_prefix = (run_id != root_run_id).then(|| format!("{run_id}:"));
        let child_prefixes = if run_id == root_run_id {
            self.tree_run_ids(root_run_id)?
                .into_iter()
                .filter(|child_run_id| child_run_id != root_run_id)
                .map(|child_run_id| format!("{child_run_id}:"))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for (key, reservation) in ledger.reservations {
            let local_key = match key_prefix.as_deref() {
                Some(prefix) => match key.strip_prefix(prefix) {
                    Some(local_key) => local_key,
                    None => continue,
                },
                None if !child_prefixes.iter().any(|prefix| key.starts_with(prefix)) => {
                    key.as_str()
                }
                None => continue,
            };
            let is_submitted = journal
                .get(local_key)
                .is_some_and(|entry| entry.state == CallState::Submitted);
            if !reservation.settled && !reservation.released && !is_submitted {
                budget.release(local_key, reason)?;
            }
        }
        Ok(())
    }

    fn execute(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        let state = self.store.load_state(run_id)?;
        self.execute_with_scheduler(run_id, Arc::new(Scheduler::new(state.max_parallel)))
    }

    fn execute_with_scheduler(
        &self,
        run_id: &str,
        scheduler: Arc<Scheduler>,
    ) -> Result<RunState, WorkflowError> {
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
        let root_run_id = initial
            .root_run_id
            .clone()
            .unwrap_or_else(|| initial.run_id.clone());
        let budget = crate::script::is_current_contract(initial.contract.as_deref()).then(|| {
            Budget::new(
                Arc::clone(&self.store),
                run_id.to_owned(),
                root_run_id.clone(),
            )
        });
        let runtime = Arc::new(RuntimeHost {
            run_id: run_id.to_owned(),
            cwd: initial.cwd.clone(),
            store: Arc::clone(&self.store),
            transport: Arc::clone(&self.transport),
            scheduler,
            contract: initial.contract.clone(),
            parent_run_id: initial.parent_run_id.clone(),
            boundary: initial.boundary.clone(),
            capabilities: initial.capabilities.clone(),
            budget,
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
        let state = if state.parent_run_id.is_none()
            && state
                .worktree
                .as_ref()
                .is_some_and(|worktree| !worktree.finalized)
        {
            let worktree = state.worktree.as_ref().expect("checked worktree state");
            crate::isolation::finalize_worktree(&self.store, &state.run_id, worktree)
                .map_err(WorkflowError::InvalidOperation)?;
            self.store.update_state(&state.run_id, |state| {
                if let Some(worktree) = state.worktree.as_mut() {
                    worktree.finalized = true;
                }
            })?
        } else {
            state
        };
        if state.status == RunStatus::Succeeded && state.boundary.is_some() {
            let violations = self
                .store
                .read_boundary_events(&state.run_id)?
                .into_iter()
                .filter_map(|envelope| match envelope.event {
                    BoundaryEvent::Violation { message, .. } => Some(message),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if !violations.is_empty() {
                let error = format!(
                    "boundary violation blocked success: {}",
                    violations.join("; ")
                );
                return self.transition(
                    &state.run_id,
                    WorkflowEvent::RunFailed {
                        error: error.clone(),
                    },
                    |state| {
                        state.status = RunStatus::Failed;
                        state.error = Some(error);
                        state.report = None;
                        state.active.clear();
                    },
                );
            }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<crate::model::BudgetLedger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_events: Option<Vec<crate::boundary::BoundaryEnvelope>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability_events: Option<Vec<crate::capabilities::CapabilityEnvelope>>,
    pub run_summary_path: PathBuf,
}

fn sha256(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
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

/// Parse `meta.moneyCap` from the script's meta block. Returns `None` when
/// the key is absent or the script has no `meta` block.
fn parse_meta_money_cap(script: &str) -> Option<u64> {
    let meta = script::parse_meta(script).ok()??;
    // moneyCap must be a positive integer in cents. Null / absent = unlimited.
    match meta.get("moneyCap")? {
        Value::Number(n) if n.is_u64() && n.as_u64()? > 0 => n.as_u64(),
        _ => None,
    }
}

fn parse_meta_boundary(script: &str) -> Result<Option<BoundaryPolicy>, WorkflowError> {
    let Some(meta) = script::parse_meta(script)? else {
        return Ok(None);
    };
    let Some(value) = meta.get("boundary") else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|error| {
            WorkflowError::InvalidWorkflow(format!("meta.boundary is invalid: {error}"))
        })
}

fn parse_meta_capabilities(script: &str) -> Result<Option<CapabilityPolicy>, WorkflowError> {
    let Some(meta) = script::parse_meta(script)? else {
        return Ok(None);
    };
    let Some(value) = meta.get("capabilities") else {
        return Ok(None);
    };
    let policy: CapabilityPolicy = serde_json::from_value(value.clone()).map_err(|error| {
        WorkflowError::InvalidWorkflow(format!("meta.capabilities is invalid: {error}"))
    })?;
    validate_capability_policy(&policy).map_err(WorkflowError::InvalidWorkflow)?;
    Ok(Some(policy))
}

fn ensure_supported_isolation(script: &str) -> Result<(), WorkflowError> {
    if parse_meta_boundary(script)?
        .is_some_and(|policy| policy.isolation == IsolationLevel::Container)
    {
        return Err(WorkflowError::InvalidWorkflow(
            "meta.boundary.isolation=\"container\" is not implemented on this host; declare \"none\", \"worktree\", or \"process\" instead"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_script(script: &str) -> Result<String, WorkflowError> {
    if script.trim().is_empty() {
        return Err(WorkflowError::InvalidWorkflow("script is empty".to_owned()));
    }
    script::parse_check(script)?;
    // Every new run declares the current contract explicitly.
    let contract = script::contract_of(script)?;
    // Contract validation owns `moneyCap` semantics: absent or null is
    // unlimited; a present value must be a positive integer number of cents.
    let meta = script::parse_meta(script)?;
    if let Some(value) = meta.as_ref().and_then(|meta| meta.get("moneyCap")) {
        match value {
            Value::Null => {}
            Value::Number(number)
                if number.is_u64() && number.as_u64().is_some_and(|cap| cap > 0) => {}
            _ => {
                return Err(WorkflowError::InvalidWorkflow(
                    "meta.moneyCap must be a positive integer number of cents or null".to_owned(),
                ));
            }
        }
    }
    Ok(contract)
}
