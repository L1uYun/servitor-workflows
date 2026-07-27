use crate::agent::{AgentCall, AgentOptions, Transport};
use crate::budget::Budget;
use crate::command::{CommandCall, CommandOptions};
use crate::model::{ActiveCall, CallKind, WorkflowEvent};
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
    _workers: Vec<thread::JoinHandle<()>>,
}

impl Scheduler {
    pub fn new(parallelism: usize) -> Self {
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
            _workers: workers,
        }
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
    pub scheduler: Scheduler,
    /// `Some("workflow.v2")` for v2 runs; `None` for v1 runs. Host functions
    /// use this to decide whether to append lifecycle events to the versioned
    /// stream. V2-A only — children inherit via V2-C.
    pub contract: Option<String>,
    pub parent_run_id: Option<String>,
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
        let call = AgentCall::new(key, prompt, options, phase);
        let active_key = call.key.clone();
        let active_label = call.label.clone();
        let run_id = self.run_id.clone();
        let cwd = self.cwd.clone();
        let store = Arc::clone(&self.store);
        let transport = Arc::clone(&self.transport);
        let budget = self.budget.clone();
        self.scheduler.submit(move || {
            let result = with_active(
                &store,
                &run_id,
                &active_key,
                CallKind::Agent,
                &active_label,
                || crate::agent::run(&store, transport.as_ref(), &run_id, &cwd, call),
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
        let budget = self.budget.clone();
        self.scheduler.submit(move || {
            let result = with_active(
                &store,
                &run_id,
                &active_key,
                CallKind::Command,
                &active_label,
                || crate::command::run(&store, &run_id, &cwd, call),
            );
            settle_budget(budget.as_ref(), &store, &active_key, result)
        })
    }
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
    // V2-B does not price calls yet. Agent token totals are already normalized
    // into the durable journal by `agent::run`, so attribute them at settlement.
    let actual_tokens = store
        .journal_index(&budget.run_id())
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
