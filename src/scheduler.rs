use crate::agent::{AgentCall, AgentOptions, Transport};
use crate::boundary::{
    BoundaryEvent, BoundaryPolicy, NetworkEvidenceSource, ensure_command_policy,
    observed_undeclared_writes, snapshot_git, snapshot_observable_files,
    validate_command_environment,
};
use crate::budget::Budget;
use crate::capabilities::{CapabilityPolicy, ModelChoice, resolve as resolve_capability};
use crate::command::{CommandCall, CommandOptions};
use crate::model::{ActiveCall, CallKind, CallState, JournalEntry, WorkflowEvent};
use crate::store::WorkflowStore;
use chrono::Utc;
use futures_channel::oneshot;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

pub(crate) type JobResult = Result<Value, String>;
type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct Scheduler {
    sender: mpsc::Sender<Job>,
    process_tree: Result<Arc<crate::process_tree::ProcessTree>, String>,
    boundary_snapshot_lock: Arc<Mutex<()>>,
    _workers: Vec<thread::JoinHandle<()>>,
}

impl Scheduler {
    pub fn new(parallelism: usize) -> Self {
        let process_tree = crate::process_tree::ProcessTree::new().map(Arc::new);
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = (0..parallelism.max(1))
            .map(|_| {
                let receiver = Arc::clone(&receiver);
                thread::spawn(move || {
                    loop {
                        let job = match receiver.lock() {
                            Ok(receiver) => receiver.recv(),
                            Err(_) => return,
                        };
                        match job {
                            Ok(job) => job(),
                            Err(_) => return,
                        }
                    }
                })
            })
            .collect();
        Self {
            sender,
            process_tree,
            boundary_snapshot_lock: Arc::new(Mutex::new(())),
            _workers: workers,
        }
    }

    pub(crate) fn process_tree(&self) -> Result<Arc<crate::process_tree::ProcessTree>, String> {
        self.process_tree.clone()
    }

    pub(crate) fn boundary_snapshot_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.boundary_snapshot_lock)
    }

    fn submit<F>(&self, operation: F) -> oneshot::Receiver<JobResult>
    where
        F: FnOnce() -> JobResult + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let job = Box::new(move || {
            let _ = sender.send(operation());
        });
        if let Err(error) = self.sender.send(job) {
            error.0();
        }
        receiver
    }
}

pub struct RuntimeHost {
    pub run_id: String,
    pub cwd: PathBuf,
    pub store: Arc<WorkflowStore>,
    pub transport: Arc<dyn Transport>,
    /// Shared tree scheduler. Child orchestration itself runs outside this pool,
    /// so `maxParallel=1` cannot deadlock when the child invokes a host call.
    pub scheduler: Arc<Scheduler>,
    /// `Some("workflow.v2")` for v2 runs; `None` for v1 runs. Host functions
    /// use this to decide whether to append lifecycle events to the versioned
    /// stream. V2-A only — children inherit via V2-C.
    pub contract: Option<String>,
    pub parent_run_id: Option<String>,
    /// Declared V2-E audit boundary. Enforcement is at host-call boundaries,
    /// not a claim that command processes are sandboxed.
    pub boundary: Option<BoundaryPolicy>,
    /// V2-G provider/model candidates and role contracts.
    pub capabilities: Option<CapabilityPolicy>,
    /// Shared budget handle (V2-B+). `None` for v1 runs.
    pub budget: Option<Budget>,
}

impl RuntimeHost {
    pub fn transition<F>(&self, event: WorkflowEvent, update: F) -> Result<(), String>
    where
        F: FnOnce(&mut crate::model::RunState),
    {
        if self.contract.as_deref() != Some("workflow.v2") {
            self.store
                .update_state(&self.run_id, update)
                .map_err(|error| error.to_string())?;
            return Ok(());
        }
        self.store
            .transition(&self.run_id, self.parent_run_id.as_deref(), event, update)
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn agent(
        &self,
        key: String,
        prompt: String,
        options: AgentOptions,
        phase: Option<String>,
    ) -> oneshot::Receiver<JobResult> {
        let mut call = AgentCall::new(key, prompt, options, phase);
        let active_key = call.key.clone();
        let active_label = call.label.clone();
        let agent_cwd = call.options.cwd.clone();
        let run_id = self.run_id.clone();
        let cwd = self.cwd.clone();
        let store = Arc::clone(&self.store);
        let transport = Arc::clone(&self.transport);
        let boundary = self.boundary.clone();
        let capabilities = self.capabilities.clone();
        let budget = self.budget.clone();
        self.scheduler.submit(move || {
            let result = with_active(
                &store,
                &run_id,
                &active_key,
                CallKind::Agent,
                &active_label,
                || {
                    if let Some(entry) = store
                        .journal_index(&run_id)
                        .map_err(|error| error.to_string())?
                        .get(&active_key)
                        && entry.state == CallState::Succeeded
                    {
                        return Ok(entry.result.clone().unwrap_or(Value::Null));
                    }
                    if let Some(policy) = capabilities.as_ref() {
                        let ModelChoice { agent, model } = resolve_capability(
                            &store,
                            &run_id,
                            &active_key,
                            policy,
                            &call.options,
                        )?;
                        call.options.agent = Some(agent);
                        call.options.model = model;
                    }
                    let resolved_cwd = agent_cwd
                        .as_deref()
                        .filter(|path| path.is_absolute())
                        .map(PathBuf::from)
                        .unwrap_or_else(|| {
                            agent_cwd
                                .as_deref()
                                .map(|path| cwd.join(path))
                                .unwrap_or_else(|| cwd.clone())
                        });
                    audit_agent_boundary(
                        &store,
                        &run_id,
                        &active_key,
                        boundary.as_ref(),
                        &resolved_cwd,
                        true,
                    )?;
                    crate::agent::run(&store, transport.as_ref(), &run_id, &cwd, call)
                },
            );
            settle_budget(budget.as_ref(), &store, &active_key, result)
        })
    }

    pub fn command(
        &self,
        key: String,
        program: String,
        args: Vec<String>,
        options: CommandOptions,
        phase: Option<String>,
    ) -> oneshot::Receiver<JobResult> {
        let call = CommandCall::new(key, program, args, options, phase);
        let active_key = call.key.clone();
        let active_label = call.label.clone();
        let run_id = self.run_id.clone();
        let cwd = self.cwd.clone();
        let store = Arc::clone(&self.store);
        let boundary = self.boundary.clone();
        let budget = self.budget.clone();
        let process_tree = if boundary
            .as_ref()
            .is_some_and(|policy| policy.isolation == crate::boundary::IsolationLevel::Process)
        {
            Some(self.scheduler.process_tree())
        } else {
            None
        };
        let boundary_snapshot_lock = self.scheduler.boundary_snapshot_lock();
        self.scheduler.submit(move || {
            let result = with_active(
                &store,
                &run_id,
                &active_key,
                CallKind::Command,
                &active_label,
                || {
                    let resolved_cwd = call
                        .options
                        .cwd
                        .as_deref()
                        .filter(|path| path.is_absolute())
                        .map(PathBuf::from)
                        .unwrap_or_else(|| {
                            call.options
                                .cwd
                                .as_deref()
                                .map(|path| cwd.join(path))
                                .unwrap_or_else(|| cwd.clone())
                        });
                    if let Some(entry) = store
                        .journal_index(&run_id)
                        .map_err(|error| error.to_string())?
                        .get(&active_key)
                        && entry.state == CallState::Succeeded
                    {
                        return Ok(entry.result.clone().unwrap_or(Value::Null));
                    }
                    audit_command_boundary(
                        &store,
                        &run_id,
                        &active_key,
                        boundary.as_ref(),
                        &call,
                        &resolved_cwd,
                    )?;
                    let redacted_values = boundary
                        .as_ref()
                        .map(|_| call.options.env.values().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    let process_tree = process_tree
                        .as_ref()
                        .map_or_else(|| Ok(None), |process_tree| process_tree.clone().map(Some))?;
                    if boundary.is_none() {
                        return crate::command::run(
                            &store,
                            &run_id,
                            &cwd,
                            call,
                            false,
                            &redacted_values,
                            process_tree.as_deref(),
                        );
                    }
                    let _snapshot_guard = boundary_snapshot_lock
                        .lock()
                        .map_err(|_| "boundary snapshot lock poisoned".to_owned())?;
                    let before_files = snapshot_observable_files(
                        boundary.as_ref().expect("checked boundary presence"),
                    )?;
                    let before_git = snapshot_git(&resolved_cwd);
                    let result = crate::command::run(
                        &store,
                        &run_id,
                        &cwd,
                        call,
                        true,
                        &redacted_values,
                        process_tree.as_deref(),
                    );
                    audit_command_snapshots(
                        &store,
                        &run_id,
                        &active_key,
                        boundary.as_ref().expect("checked boundary presence"),
                        before_files,
                        before_git,
                        &store.run_dir(&run_id),
                    )?;
                    result
                },
            );
            settle_budget(budget.as_ref(), &store, &active_key, result)
        })
    }
}

fn audit_agent_boundary(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    boundary: Option<&BoundaryPolicy>,
    cwd: &std::path::Path,
    network: bool,
) -> JobResult {
    let Some(boundary) = boundary else {
        return Ok(Value::Null);
    };
    if let Err(error) = ensure_command_policy(boundary, cwd, network) {
        record_boundary_violation(store, run_id, key, CallKind::Agent, &error, true)?;
        return Err(error);
    }
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::AgentObserved {
                key: key.to_owned(),
                cwd: cwd.to_path_buf(),
            },
        )
        .map_err(|error| error.to_string())?;
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::NetworkObserved {
                key: key.to_owned(),
                declared: true,
                source: NetworkEvidenceSource::AgentTransport,
            },
        )
        .map_err(|error| error.to_string())?;
    Ok(Value::Null)
}

fn audit_command_boundary(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    boundary: Option<&BoundaryPolicy>,
    call: &CommandCall,
    cwd: &std::path::Path,
) -> JobResult {
    let Some(boundary) = boundary else {
        return Ok(Value::Null);
    };
    let checked = ensure_command_policy(boundary, cwd, call.options.network)
        .and_then(|_| validate_command_environment(boundary, &call.options.env));
    let names = match checked {
        Ok(names) => names,
        Err(error) => {
            record_boundary_violation(store, run_id, key, CallKind::Command, &error, true)?;
            return Err(error);
        }
    };
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::CommandObserved {
                key: key.to_owned(),
                program: call.program.clone(),
                cwd: cwd.to_path_buf(),
                environment: names,
            },
        )
        .map_err(|error| error.to_string())?;
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::NetworkObserved {
                key: key.to_owned(),
                declared: call.options.network,
                source: NetworkEvidenceSource::CommandDeclaration,
            },
        )
        .map_err(|error| error.to_string())?;
    Ok(Value::Null)
}

fn audit_command_snapshots(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    boundary: &BoundaryPolicy,
    before_files: crate::boundary::FileSnapshot,
    before_git: crate::boundary::GitSnapshot,
    run_dir: &std::path::Path,
) -> JobResult {
    let mut after_files = snapshot_observable_files(boundary)?;
    after_files.observed_undeclared_writes =
        observed_undeclared_writes(&before_files, &after_files, boundary)
            .into_iter()
            .filter(|path| !path_is_within(path, run_dir))
            .collect();
    let violation = after_files.observed_undeclared_writes.first().map(|path| {
        format!(
            "observed write outside declared writePaths: {}",
            path.display()
        )
    });
    let after_git = snapshot_git(&before_git.cwd);
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::FileSnapshot {
                key: key.to_owned(),
                before: before_files,
                after: after_files,
            },
        )
        .map_err(|error| error.to_string())?;
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::GitSnapshot {
                key: Some(key.to_owned()),
                before: before_git,
                after: after_git,
            },
        )
        .map_err(|error| error.to_string())?;
    if let Some(message) = violation {
        record_boundary_violation(store, run_id, key, CallKind::Command, &message, false)?;
        return Err(message);
    }
    Ok(Value::Null)
}

fn path_is_within(path: &std::path::Path, root: &std::path::Path) -> bool {
    fn normalized(path: &std::path::Path) -> String {
        path.as_os_str()
            .to_string_lossy()
            .trim_start_matches(r"\\?\")
            .trim_start_matches(r"\??\")
            .to_ascii_lowercase()
    }
    let path_text = normalized(path);
    let root_text = normalized(root);
    if std::path::Path::new(&path_text).starts_with(std::path::Path::new(&root_text)) {
        return true;
    }
    let (Ok(path), Ok(root)) = (path.canonicalize(), root.canonicalize()) else {
        return false;
    };
    let path_text = normalized(&path);
    let root_text = normalized(&root);
    std::path::Path::new(&path_text).starts_with(std::path::Path::new(&root_text))
}

fn record_boundary_violation(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    kind: CallKind,
    message: &str,
    journal_failure: bool,
) -> Result<(), String> {
    if journal_failure {
        store
            .append(
                run_id,
                &JournalEntry {
                    at: Utc::now(),
                    key: key.to_owned(),
                    kind,
                    state: CallState::Failed,
                    label: key.to_owned(),
                    result: None,
                    error: Some(message.to_owned()),
                    transport_run_id: None,
                    child_run_id: None,
                    phase: None,
                    duration_ms: None,
                    usage: None,
                    schema_correction: None,
                },
            )
            .map_err(|error| error.to_string())?;
    }
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::Violation {
                key: Some(key.to_owned()),
                message: message.to_owned(),
            },
        )
        .map_err(|error| error.to_string())
}

fn settle_budget(
    budget: Option<&Budget>,
    store: &WorkflowStore,
    key: &str,
    result: JobResult,
) -> JobResult {
    let Some(budget) = budget else {
        return result;
    };
    if let Err(result_error) = result {
        let succeeded = store
            .journal_index(budget.run_id())
            .ok()
            .and_then(|journal| {
                journal
                    .get(key)
                    .map(|entry| entry.state == CallState::Succeeded)
            })
            .unwrap_or(false);
        if succeeded {
            let actual_tokens = store
                .journal_index(budget.run_id())
                .ok()
                .and_then(|journal| {
                    journal
                        .get(key)
                        .and_then(|entry| usage_tokens(entry.usage.as_ref()))
                })
                .unwrap_or(0);
            return match budget.settle(key, None, actual_tokens) {
                Ok(()) => Err(result_error),
                Err(error) => Err(format!(
                    "{result_error}; budget settlement failed after completed call: {error}"
                )),
            };
        }
        if let Err(error) = budget.release(key, "host call failed") {
            return Err(format!(
                "{result_error}; budget release failed after failed call: {error}"
            ));
        }
        return Err(result_error);
    }
    // V2-B does not price calls yet. Agent token totals are already normalized
    // into the durable journal by `agent::run`, so attribute them at settlement.
    let actual_tokens = store
        .journal_index(budget.run_id())
        .ok()
        .and_then(|journal| {
            journal
                .get(key)
                .and_then(|entry| usage_tokens(entry.usage.as_ref()))
        })
        .unwrap_or(0);
    if let Err(error) = budget.settle(key, None, actual_tokens) {
        return match result {
            Ok(_) => Err(format!(
                "budget settlement failed after successful call: {error}"
            )),
            Err(result_error) => Err(format!("{result_error}; budget settlement failed: {error}")),
        };
    }
    result
}

fn usage_tokens(usage: Option<&Value>) -> Option<u64> {
    let usage = usage?.as_object()?;
    for key in ["total_tokens", "totalTokens", "total"] {
        if let Some(total) = usage.get(key).and_then(Value::as_u64) {
            return Some(total);
        }
    }
    for (input, output) in [
        ("input", "output"),
        ("input_tokens", "output_tokens"),
        ("prompt_tokens", "completion_tokens"),
        ("inputTokens", "outputTokens"),
    ] {
        if let (Some(input), Some(output)) = (
            usage.get(input).and_then(Value::as_u64),
            usage.get(output).and_then(Value::as_u64),
        ) {
            return Some(input.saturating_add(output));
        }
    }
    None
}

fn with_active<F>(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    kind: CallKind,
    label: &str,
    operation: F,
) -> JobResult
where
    F: FnOnce() -> JobResult,
{
    store
        .update_state(run_id, |state| {
            state.active.insert(
                key.to_owned(),
                ActiveCall {
                    kind,
                    label: label.to_owned(),
                    started_at: Utc::now(),
                },
            );
        })
        .map_err(|error| error.to_string())?;
    let result = operation();
    store
        .update_state(run_id, |state| {
            state.active.remove(key);
        })
        .map_err(|error| error.to_string())?;
    result
}

pub fn call_key(kind: &str, input: &Value, occurrence: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(input).unwrap_or_default());
    format!("{:x}#{occurrence}", hasher.finalize())
}
