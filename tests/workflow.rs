use chrono::Utc;
use serde_json::{Value, json};
use servitor::protocol::Diagnostics;
use servitor::{
    Activity, ErrorInfo, Input, Output, RunRecord, RunState as ServitorState, SubmitRequest,
    SubmitResponse,
};
use servitor_workflows::{
    BoundaryEvent, BudgetEvent, CallKind, CallState, CapabilityEvent, Engine, JournalEntry,
    NetworkPolicy, RunState, RunStatus, Transport, WorkflowStore,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct FakeTransport {
    calls: AtomicUsize,
    active_inspections: AtomicUsize,
    peak_inspections: AtomicUsize,
    delay: Duration,
    records: Mutex<BTreeMap<String, RunRecord>>,
    requests: Mutex<Vec<SubmitRequest>>,
    fail_submissions: bool,
    fail_after: Option<usize>,
    loop_rounds: AtomicUsize,
    emit_usage: bool,
}

impl FakeTransport {
    fn new(delay: Duration) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            active_inspections: AtomicUsize::new(0),
            peak_inspections: AtomicUsize::new(0),
            delay,
            records: Mutex::new(BTreeMap::new()),
            requests: Mutex::new(Vec::new()),
            fail_submissions: false,
            fail_after: None,
            loop_rounds: AtomicUsize::new(0),
            emit_usage: true,
        }
    }
    /// Fault injection: a provider that reports no usage diagnostics,
    /// so settlement must still succeed with zero attributed tokens.
    fn without_usage(delay: Duration) -> Self {
        Self {
            emit_usage: false,
            ..Self::new(delay)
        }
    }
    fn failing(delay: Duration) -> Self {
        Self {
            fail_submissions: true,
            ..Self::new(delay)
        }
    }
    fn fail_after(delay: Duration, successful_submissions: usize) -> Self {
        Self {
            fail_after: Some(successful_submissions),
            ..Self::new(delay)
        }
    }
    fn count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    fn peak_inspections(&self) -> usize {
        self.peak_inspections.load(Ordering::SeqCst)
    }
    /// The transport run ids currently held by the provider, in sorted order.
    /// Used by the fault tests to assert which submissions persisted.
    fn record_ids(&self) -> Vec<String> {
        self.records
            .lock()
            .expect("record lock")
            .keys()
            .cloned()
            .collect()
    }
}

impl Transport for FakeTransport {
    fn submit(&self, request: SubmitRequest) -> Result<SubmitResponse, ErrorInfo> {
        let number = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_submissions || self.fail_after.is_some_and(|limit| number > limit) {
            return Err(ErrorInfo::new("provider", "simulated transport failure"));
        }
        self.requests
            .lock()
            .map_err(|_| ErrorInfo::new("lock", "request lock poisoned"))?
            .push(request.clone());
        let run_id = format!("fake-{number}");
        let prompt = match request.input {
            Input::Text { text } => text,
            Input::Image(_) => String::new(),
        };
        let output = if prompt.contains("FENCED_JSON") {
            "done

```json
{\"summary\":\"ok\",\"score\":1}
```"
            .to_owned()
        } else if prompt.contains("INVALID_TWICE") {
            "still-not-json".to_owned()
        } else if prompt.contains("VALID_FIRST") {
            r#"{"ok":true}"#.to_owned()
        } else if prompt.contains("STRICT_RETRY_JSON") {
            if number == 1 {
                r#"{"kind":"ok","score":7,"unexpected":true}"#.to_owned()
            } else {
                r#"{"kind":"ok","score":7}"#.to_owned()
            }
        } else if prompt.contains("RETRY_JSON") {
            if number == 1 {
                "not-json".to_owned()
            } else {
                r#"{"ok":true}"#.to_owned()
            }
        } else if prompt.contains("DISCOVER") {
            r#"{"items":["a","b","c"]}"#.to_owned()
        } else if prompt.contains("NEGOTIATE_PROPOSE") {
            if prompt.contains("ROUND=1") {
                r#"{"proposal":"use canary A","assumptions":["local only"],"confidence":0.6}"#
                    .to_owned()
            } else {
                r#"{"proposal":"use canary A fixed","assumptions":["local only","timeout 30s"],"confidence":0.85}"#
                    .to_owned()
            }
        } else if prompt.contains("NEGOTIATE_REVIEW") {
            if prompt.contains("ROUND=1") {
                r#"{"verdict":"revise","critique":"missing timeout","must_fix":["add timeout bound"]}"#
                    .to_owned()
            } else {
                r#"{"verdict":"accept","critique":"addressed","must_fix":[]}"#.to_owned()
            }
        } else if prompt.contains("NEGOTIATE_SYNTH") {
            r#"{"accepted":true,"decision":"use canary A fixed","rationale":"reviewer accepted after revise","open_issues":[]}"#
                .to_owned()
        } else if prompt.contains("GOALCHAIN_WORKER") && prompt.contains("BADWORKER") {
            // Negative control: the worker lands evidence with one token false,
            // so the mechanical gate (which reads back from disk) must fail.
            r#"{"summary":"claimed done","evidence":"{\"dual-gate\":true,\"boundary-audit\":false}"}"#
                .to_owned()
        } else if prompt.contains("GOALCHAIN_WORKER") {
            r#"{"summary":"delivered bounded objective","evidence":"{\"dual-gate\":true,\"boundary-audit\":true}"}"#
                .to_owned()
        } else if prompt.contains("GOALCHAIN_REVIEW") {
            r#"{"verdict":"approve","critique":"satisfies contract","must_fix":[]}"#.to_owned()
        } else if prompt.contains("GOALCHAIN_SEMANTIC") {
            r#"{"approved":true,"rationale":"meaning holds"}"#.to_owned()
        } else if prompt.contains("JUDGE_CASE") {
            // Judge panel: each independent candidate returns a score.
            r#"{"score":8,"rationale":"candidate approach"}"#.to_owned()
        } else if prompt.contains("JUDGE_SYNTH") {
            r#"{"winner":"beta","rationale":"highest judged score"}"#.to_owned()
        } else if prompt.contains("FIND_NEW") {
            // Loop-until-dry: first discovery finds items, every later
            // round finds nothing new so the loop converges.
            let round = self.loop_rounds.fetch_add(1, Ordering::SeqCst);
            if round == 0 {
                r#"{"items":["x","y"]}"#.to_owned()
            } else {
                r#"{"items":[]}"#.to_owned()
            }
        } else if prompt.contains("VANISH") {
            // Fault: the provider accepts the submission but loses the
            // record before the result is persisted. Handled below by removing
            // the just-inserted record so the first `inspect` fails "missing".
            "ok".to_owned()
        } else if prompt.contains("Review the following stage output") {
            // B2 verify gate: the default-ON LLM review prompt. The FakeTransport
            // returns a pass verdict unless the upstream output contains the
            // "FAIL_ME" marker, in which case it returns a fail verdict.
            if prompt.contains("FAIL_ME") {
                r#"{"pass":false,"reason":"verify rejected"}"#.to_owned()
            } else {
                r#"{"pass":true,"reason":"ok"}"#.to_owned()
            }
        } else if prompt.contains("WORK ") {
            format!(
                "{}-ok",
                prompt
                    .split("WORK ")
                    .nth(1)
                    .unwrap_or_default()
                    .lines()
                    .next()
                    .unwrap_or_default()
            )
        } else {
            "ok".to_owned()
        };
        let now = Utc::now();
        // Model realistic daemon continuation behavior so the default-ON
        // cross-call memory and the "makeup exam" correction retry are
        // observable in tests: when the request carries a continuation, echo
        // it back (same session extended); when it does not, mint a new
        // session id derived from the transport run id. This is what lets
        // `requests[N].continuation` assertions verify threading.
        let continuation = request
            .continuation
            .clone()
            .or_else(|| Some(format!("sess-{run_id}")));
        self.records
            .lock()
            .map_err(|_| ErrorInfo::new("lock", "record lock poisoned"))?
            .insert(
                run_id.clone(),
                RunRecord {
                    version: 1,
                    run_id: run_id.clone(),
                    state: ServitorState::Running,
                    agent: request.agent,
                    model: request.model,
                    created_at: now,
                    started_at: Some(now),
                    finished_at: None,
                    output: Some(Output::Text { text: output }),
                    error: None,
                    continuation,
                    activity: None::<Activity>,
                    diagnostics: Diagnostics::default(),
                },
            );
        if prompt.contains("VANISH") {
            // Fault injection: submitted transport without persisted
            // result — the provider accepted the run but lost the record, so
            // the first `inspect` fails with "missing".
            self.records
                .lock()
                .map_err(|_| ErrorInfo::new("lock", "record lock poisoned"))?
                .remove(&run_id);
        }
        Ok(SubmitResponse {
            run_id,
            state: ServitorState::Accepted,
        })
    }

    fn inspect(&self, run_id: &str) -> Result<RunRecord, ErrorInfo> {
        let active = self.active_inspections.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_inspections.fetch_max(active, Ordering::SeqCst);
        thread::sleep(self.delay);
        self.active_inspections.fetch_sub(1, Ordering::SeqCst);
        let mut records = self
            .records
            .lock()
            .map_err(|_| ErrorInfo::new("lock", "record lock poisoned"))?;
        let record = records
            .get_mut(run_id)
            .ok_or_else(|| ErrorInfo::new("missing", run_id))?;
        if record.state == ServitorState::Running {
            record.state = ServitorState::Succeeded;
            record.finished_at = Some(Utc::now());
            if self.emit_usage {
                record
                    .diagnostics
                    .provider
                    .insert("usage".to_owned(), json!({"input": 100, "output": 20}));
            }
        }
        Ok(record.clone())
    }

    fn cancel(&self, run_id: &str) -> Result<RunRecord, ErrorInfo> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| ErrorInfo::new("lock", "record lock poisoned"))?;
        let record = records
            .get_mut(run_id)
            .ok_or_else(|| ErrorInfo::new("missing", run_id))?;
        record.state = ServitorState::Cancelled;
        record.finished_at = Some(Utc::now());
        record.output = None;
        Ok(record.clone())
    }
}

fn engine(root: &Path, transport: Arc<FakeTransport>) -> Engine {
    Engine::new(WorkflowStore::new(root), transport)
}

fn script(temp: &TempDir, name: &str, body: &str) -> std::path::PathBuf {
    let path = temp.path().join(name);
    fs::write(
        &path,
        format!("export const meta = {{ name: \"test\", contract: \"workflow\" }};\n{body}"),
    )
    .expect("write fixture");
    path
}

fn legacy_state(store: &WorkflowStore, path: &Path, status: RunStatus) -> RunState {
    let now = Utc::now();
    let run_id = "legacy-fixture".to_owned();
    RunState {
        version: 1,
        run_id: run_id.clone(),
        name: "legacy".to_owned(),
        cwd: path.parent().expect("script parent").to_path_buf(),
        contract: None,
        parent_run_id: None,
        root_run_id: None,
        parent_call_key: None,
        money_cap: None,
        boundary: None,
        capabilities: None,
        worktree: None,
        status,
        created_at: now,
        updated_at: now,
        args: Value::Null,
        max_parallel: 1,
        max_calls: 10,
        resume_count: 0,
        phase: None,
        active: BTreeMap::new(),
        waiting_gate: None,
        supersede: None,
        decisions: BTreeMap::new(),
        result: None,
        error: None,
        report: None,
        run_summary: None,
        journal_path: store.journal_path(&run_id),
    }
}

#[test]
fn container_isolation_is_refused_before_run_creation() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-container-refusal");
    let path = temp.path().join("container.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "container",
          contract: "workflow",
          boundary: { isolation: "container" }
        }; return true;"#,
    )
    .expect("write script");
    let runtime = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let error = runtime.check(&path).expect_err("container refusal");
    assert!(error.to_string().contains("not implemented on this host"));
    assert!(!root.join("runs").exists());
}

#[test]
fn child_cannot_weaken_parent_isolation() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-isolation-child");
    fs::write(
        temp.path().join("child.js"),
        r#"export const meta = {
          name: "child",
          contract: "workflow",
          boundary: { isolation: "worktree" }
        }; return true;"#,
    )
    .expect("write child");
    let parent = temp.path().join("parent.js");
    fs::write(
        &parent,
        r#"export const meta = {
          name: "parent",
          contract: "workflow",
          boundary: { isolation: "process" }
        }; return await workflow("child.js");"#,
    )
    .expect("write parent");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&parent, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("child isolation widens parent boundary")
    );
}

#[test]
fn worktree_isolation_writes_patch_and_commit_evidence() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-worktree");
    Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(temp.path())
        .output()
        .expect("git init");
    Command::new("git")
        .args(["config", "user.email", "tests@example.invalid"])
        .current_dir(temp.path())
        .output()
        .expect("configure email");
    Command::new("git")
        .args(["config", "user.name", "Workflow Tests"])
        .current_dir(temp.path())
        .output()
        .expect("configure name");
    fs::write(temp.path().join("seed.txt"), "seed\n").expect("write seed");
    Command::new("git")
        .args(["add", "seed.txt"])
        .current_dir(temp.path())
        .output()
        .expect("stage seed");
    Command::new("git")
        .args(["commit", "-m", "seed"])
        .current_dir(temp.path())
        .output()
        .expect("commit seed");
    let path = temp.path().join("worktree.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "worktree",
          contract: "workflow",
          boundary: { isolation: "worktree" }
        }; return await command("cmd", ["/D", "/C", "echo evidence>>seed.txt"]);"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let store = WorkflowStore::new(&root);
    let run_dir = store.run_dir(&state.run_id);
    assert!(
        fs::read_to_string(run_dir.join("worktree.patch"))
            .expect("patch evidence")
            .contains("seed.txt")
    );
    assert!(
        fs::read_to_string(run_dir.join("worktree.commit.txt"))
            .expect("commit evidence")
            .contains("base_commit:")
    );
    assert!(
        store
            .read_boundary_events(&state.run_id)
            .expect("boundary events")
            .iter()
            .any(|event| matches!(event.event, BoundaryEvent::WorktreeFinalized { .. }))
    );
}

#[test]
fn boundary_metadata_is_checked_and_persisted() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-declared");
    let path = temp.path().join("boundary.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary",
          contract: "workflow",
          boundary: { readPaths: ["."], writePaths: ["./out"], network: "allow", environment: { allow: ["SAFE_VAR"] } }
        }; return { done: true };"#,
    )
    .expect("write script");
    let engine = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)));
    engine.check(&path).expect("check valid boundary");
    let state = engine.start(&path, Value::Null, 1, 1).expect("run");
    let events = WorkflowStore::new(&root)
        .read_boundary_events(&state.run_id)
        .expect("read boundary events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[0].run_id, state.run_id);
    assert!(matches!(events[0].event, BoundaryEvent::Declared { .. }));
}

#[test]
fn check_rejects_invalid_boundary_metadata() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("invalid-boundary.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "invalid",
          contract: "workflow",
          boundary: { readPaths: ["."], unexpected: true }
        }; return true;"#,
    )
    .expect("write script");
    let error = engine(
        &temp.path().join("state-invalid-boundary"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .check(&path)
    .expect_err("reject invalid boundary");
    assert!(error.to_string().contains("meta.boundary is invalid"));
}

#[test]
fn boundary_rejects_undeclared_environment_without_persisting_values() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-env");
    let path = temp.path().join("boundary-env.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-env",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny", environment: { allow: ["SAFE_VAR"] } }
        };
        return await command("cmd", ["/C", "exit 0"], { env: { SAFE_VAR: "safe-value", TOKEN: "secret-value" } });"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("environment variable is not declared: TOKEN")
    );
    let boundary = fs::read_to_string(WorkflowStore::new(&root).boundary_path(&state.run_id))
        .expect("boundary audit");
    assert!(boundary.contains("TOKEN"));
    assert!(!boundary.contains("secret-value"));
    assert!(!boundary.contains("safe-value"));
}

#[test]
fn boundary_rejects_declared_network_when_denied() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-network");
    let path = temp.path().join("boundary-network.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-network",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny" }
        };
        return await command("cmd", ["/C", "exit 0"], { network: true });"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("network policy denies it")
    );
}

#[test]
fn child_boundary_narrows_and_is_audited() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-child-narrow");
    fs::create_dir(temp.path().join("sub")).expect("create child directory");
    fs::write(
        temp.path().join("child-narrow.js"),
        r#"export const meta = {
          name: "child",
          contract: "workflow",
          boundary: { readPaths: ["./sub"], network: "deny", environment: { allow: ["SAFE_VAR"] } }
        }; return true;"#,
    )
    .expect("write child");
    let parent = temp.path().join("parent-narrow.js");
    fs::write(
        &parent,
        r#"export const meta = {
          name: "parent",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "allow", environment: { allow: ["SAFE_VAR", "PARENT_ONLY"] } }
        }; return await workflow("child-narrow.js");"#,
    )
    .expect("write parent");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&parent, Value::Null, 1, 2)
        .expect("run parent");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let store = WorkflowStore::new(&root);
    let child_id = store
        .child_run_ids(&state.run_id)
        .expect("children")
        .into_iter()
        .next()
        .expect("child");
    let child = store.load_state(&child_id).expect("child state");
    assert_eq!(
        child.boundary.as_ref().expect("child boundary").network,
        NetworkPolicy::Deny
    );
    assert!(
        store
            .read_boundary_events(&state.run_id)
            .expect("parent boundary events")
            .iter()
            .any(|event| matches!(event.event, BoundaryEvent::ChildDeclared { .. }))
    );
    assert!(matches!(
        store
            .read_boundary_events(&child_id)
            .expect("child boundary events")
            .first()
            .map(|event| &event.event),
        Some(BoundaryEvent::Declared { .. })
    ));
}

#[test]
fn child_capability_policy_cannot_weaken_parent_requirements() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-capability-child-narrow");
    fs::write(
        temp.path().join("child-capability.js"),
        r#"export const meta = {
          name: "child",
          contract: "workflow",
          capabilities: {
            providers: [{ agent: "claude", model: "claude-opus-5", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 200000 }],
            roles: { reviewer: { requires: ["reasoning"], effort: "medium", contextTokens: 64000 } }
          }
        }; return true;"#,
    )
    .expect("write child");
    let parent = temp.path().join("parent-capability.js");
    fs::write(
        &parent,
        r#"export const meta = {
          name: "parent",
          contract: "workflow",
          capabilities: {
            providers: [{ agent: "claude", model: "claude-opus-5", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 200000 }],
            roles: { reviewer: { requires: ["reasoning"], effort: "high", contextTokens: 100000 } }
          }
        }; return await workflow("child-capability.js");"#,
    )
    .expect("write parent");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&parent, Value::Null, 1, 2)
        .expect("terminal parent");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("weakens parent")
    );
    assert!(
        WorkflowStore::new(&root)
            .child_run_ids(&state.run_id)
            .expect("children")
            .is_empty()
    );
}

#[test]
fn boundary_failure_releases_budget_reservation() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-release");
    let path = temp.path().join("boundary-release.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-release",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny" }
        };
        return await command("cmd", ["/C", "exit 0"], { network: true });"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    let budget = WorkflowStore::new(&root)
        .read_budget_events(&state.run_id)
        .expect("read budget events");
    assert!(matches!(budget[0].event, BudgetEvent::Reserved { .. }));
    assert!(matches!(budget[1].event, BudgetEvent::Released { .. }));
}

#[test]
fn child_boundary_cannot_widen_parent_network_policy() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-child");
    fs::write(
        temp.path().join("child.js"),
        r#"export const meta = {
          name: "child",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "allow" }
        }; return true;"#,
    )
    .expect("write child");
    let parent = temp.path().join("parent.js");
    fs::write(
        &parent,
        r#"export const meta = {
          name: "parent",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny" }
        }; return await workflow("child.js");"#,
    )
    .expect("write parent");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&parent, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("child network policy widens parent boundary")
    );
    assert!(
        WorkflowStore::new(&root)
            .child_run_ids(&state.run_id)
            .expect("children")
            .is_empty()
    );
}

#[test]
fn boundary_command_clears_inherited_environment() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-inheritance");
    let path = temp.path().join("boundary-inheritance.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-inheritance",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny", environment: { allow: ["SAFE_VAR"] } }
        };
        return await command("cmd", ["/D", "/C", "if defined PATH exit /b 7"], { env: { SAFE_VAR: "declared" } });"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
}

#[test]
fn boundary_snapshots_declared_writes_and_git_state() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-snapshot");
    let path = temp.path().join("boundary-snapshot.js");
    fs::create_dir(temp.path().join("allowed")).expect("create allowed output");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-snapshot",
          contract: "workflow",
          boundary: { readPaths: ["."], writePaths: ["./allowed"], network: "deny" }
        };
        return await command("cmd", ["/D", "/C", "echo recorded>allowed\\output.txt"]);"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let events = WorkflowStore::new(&root)
        .read_boundary_events(&state.run_id)
        .expect("boundary audit");
    let file_snapshot = events.iter().find_map(|event| match &event.event {
        BoundaryEvent::FileSnapshot { before, after, .. } => Some((before, after)),
        _ => None,
    });
    let (before, after) = file_snapshot.expect("file snapshot");
    assert!(
        before
            .files
            .iter()
            .all(|entry| !entry.path.ends_with("output.txt"))
    );
    assert!(
        after
            .files
            .iter()
            .any(|entry| entry.path.ends_with("output.txt"))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event.event, BoundaryEvent::GitSnapshot { .. }))
    );
}

#[test]
fn boundary_blocks_observed_write_outside_declared_paths() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-write-violation");
    let path = temp.path().join("boundary-write-violation.js");
    fs::create_dir(temp.path().join("allowed")).expect("create allowed output");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-write-violation",
          contract: "workflow",
          boundary: { readPaths: ["."], writePaths: ["./allowed"], network: "deny" }
        };
        return await command("cmd", ["/D", "/C", "echo blocked>undeclared.txt"]);"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("observed write outside declared writePaths")
    );
    let events = WorkflowStore::new(&root)
        .read_boundary_events(&state.run_id)
        .expect("boundary audit");
    assert!(events.iter().any(|event| matches!(
        &event.event,
        BoundaryEvent::FileSnapshot { after, .. }
            if after.observed_undeclared_writes.iter().any(|path| path.ends_with("undeclared.txt"))
    )));
    let budget = WorkflowStore::new(&root)
        .read_budget_events(&state.run_id)
        .expect("budget audit");
    assert!(matches!(budget[0].event, BudgetEvent::Reserved { .. }));
    assert!(matches!(budget[1].event, BudgetEvent::Settled { .. }));
}

#[test]
fn boundary_blocks_observed_deletion_outside_declared_paths() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-delete-violation");
    let path = temp.path().join("boundary-delete-violation.js");
    fs::write(temp.path().join("undeclared.txt"), "before").expect("write input");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-delete-violation",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny" }
        };
        return await command("cmd", ["/D", "/C", "del undeclared.txt"]);"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    let events = WorkflowStore::new(&root)
        .read_boundary_events(&state.run_id)
        .expect("boundary audit");
    assert!(events.iter().any(|event| matches!(
        &event.event,
        BoundaryEvent::FileSnapshot { after, .. }
            if after.observed_undeclared_writes.iter().any(|path| path.ends_with("undeclared.txt"))
    )));
}

#[test]
fn boundary_resume_does_not_duplicate_completed_call_audit() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-resume");
    let path = temp.path().join("boundary-resume.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-resume",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny" }
        };
        return await command("cmd", ["/D", "/C", "exit 0"]);"#,
    )
    .expect("write script");
    let runtime = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let state = runtime
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let store = WorkflowStore::new(&root);
    let before = store
        .read_boundary_events(&state.run_id)
        .expect("initial audit")
        .len();
    let resumed = runtime.resume(&state.run_id).expect("resume workflow");
    assert_eq!(resumed.status, RunStatus::Succeeded, "{:?}", resumed.error);
    assert_eq!(
        store
            .read_boundary_events(&state.run_id)
            .expect("resumed audit")
            .len(),
        before
    );
}

#[test]
fn boundary_redacts_explicit_environment_values_from_command_journal() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-redaction");
    let path = temp.path().join("boundary-redaction.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-redaction",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny", environment: { allow: ["TOKEN"] } }
        };
        return await command("cmd", ["/D", "/C", "echo %TOKEN%"], { env: { TOKEN: "secret-value" } });"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let journal =
        fs::read_to_string(WorkflowStore::new(&root).journal_path(&state.run_id)).expect("journal");
    assert!(!journal.contains("secret-value"));
    assert!(journal.contains("[REDACTED]"));
}

#[test]
fn boundary_violation_caught_by_script_still_blocks_success() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-boundary-caught-violation");
    let path = temp.path().join("boundary-caught-violation.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "boundary-caught-violation",
          contract: "workflow",
          boundary: { readPaths: ["."], network: "deny", environment: { allow: [] } }
        };
        try {
          await command("cmd", ["/D", "/C", "exit 0"], { env: { TOKEN: "secret-value" } });
        } catch (_) {}
        return { done: true };"#,
    )
    .expect("write script");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 2)
        .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("boundary violation blocked success")
    );
}

#[test]
fn legacy_resume_does_not_create_boundary_audit() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-legacy-boundary");
    let store = WorkflowStore::new(&root);
    let path = temp.path().join("legacy.js");
    fs::write(&path, "return true;").expect("write legacy script");
    let state = legacy_state(&store, &path, RunStatus::Succeeded);
    store
        .create_run(&state, "return true;")
        .expect("persist legacy run");
    let resumed = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .resume(&state.run_id)
        .expect("resume legacy run");
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert!(!store.boundary_path(&state.run_id).exists());
}

#[test]
fn budget_ledger_reserves_and_settles_each_host_call() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-budget-ledger");
    let path = script(
        &temp,
        "budget-ledger.js",
        r#"const result = await command("cmd", ["/C", "exit 0"]); return { result };"#,
    );
    let state = engine(
        &root,
        Arc::new(FakeTransport::new(Duration::from_millis(5))),
    )
    .start(&path, Value::Null, 1, 2)
    .expect("run");
    assert_eq!(state.status, RunStatus::Succeeded);

    let store = WorkflowStore::new(&root);
    let events = store
        .read_budget_events(&state.run_id)
        .expect("read budget");
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].event, BudgetEvent::Reserved { .. }));
    assert!(matches!(events[1].event, BudgetEvent::Settled { .. }));
    let ledger = store.reconstruct_budget(&state.run_id).expect("ledger");
    assert_eq!(ledger.used_calls, 1);
    assert_eq!(ledger.held_calls, 0);
    assert_eq!(ledger.limit_calls, Some(2));
    assert_eq!(ledger.limit_money, None);
}

#[test]
fn meta_money_cap_is_persisted_and_emitted() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-money-cap");
    let path = temp.path().join("money-cap.js");
    fs::write(
        &path,
        r#"export const meta = { name: "cap", contract: "workflow", moneyCap: 123 }; return { done: true };"#,
    )
    .expect("write script");
    let state = engine(
        &root,
        Arc::new(FakeTransport::new(Duration::from_millis(5))),
    )
    .start(&path, Value::Null, 1, 1)
    .expect("run");
    assert_eq!(state.money_cap, Some(123));
    let reconstructed = WorkflowStore::new(&root)
        .reconstruct_state(&state.run_id)
        .expect("reconstruct");
    assert_eq!(reconstructed.money_cap, Some(123));
}
#[test]
fn budget_released_reservation_cannot_settle() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-budget-release");
    let path = script(&temp, "budget-release.js", r#"return { done: true };"#);
    let state = engine(
        &root,
        Arc::new(FakeTransport::new(Duration::from_millis(5))),
    )
    .prepare(&path, Value::Null, 1, 2)
    .expect("prepare");
    let store = WorkflowStore::new(&root);
    store
        .append_budget_event(
            &state.run_id,
            &state.run_id,
            BudgetEvent::Reserved {
                key: "call".to_owned(),
                kind: CallKind::Command,
                estimate_money: None,
            },
        )
        .expect("reserve");
    store
        .append_budget_event(
            &state.run_id,
            &state.run_id,
            BudgetEvent::Released {
                key: "call".to_owned(),
                reason: "cancelled".to_owned(),
            },
        )
        .expect("release");
    store
        .append_budget_event(
            &state.run_id,
            &state.run_id,
            BudgetEvent::Settled {
                key: "call".to_owned(),
                actual_money: None,
                actual_tokens: 0,
            },
        )
        .expect("late settlement");

    let ledger = store.reconstruct_budget(&state.run_id).expect("ledger");
    assert_eq!(ledger.used_calls, 0);
    assert_eq!(ledger.held_calls, 0);
    assert!(ledger.reservations["call"].released);
    assert!(!ledger.reservations["call"].settled);
}

#[test]
fn resume_preserves_submitted_reservation() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-budget-resume");
    let path = script(&temp, "budget-resume.js", r#"return { done: true };"#);
    let state = engine(
        &root,
        Arc::new(FakeTransport::new(Duration::from_millis(5))),
    )
    .prepare(&path, Value::Null, 1, 1)
    .expect("prepare");
    let store = WorkflowStore::new(&root);
    store
        .append_budget_event(
            &state.run_id,
            &state.run_id,
            BudgetEvent::Reserved {
                key: "call".to_owned(),
                kind: CallKind::Agent,
                estimate_money: None,
            },
        )
        .expect("reserve");
    store
        .append(
            &state.run_id,
            &JournalEntry {
                at: Utc::now(),
                key: "call".to_owned(),
                kind: CallKind::Agent,
                state: CallState::Submitted,
                label: "call".to_owned(),
                result: None,
                error: None,
                transport_run_id: Some("fake-1".to_owned()),
                child_run_id: None,
                phase: None,
                duration_ms: None,
                usage: None,
                schema_correction: None,
            },
        )
        .expect("submitted journal entry");

    engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .resume(&state.run_id)
        .expect("resume");
    let ledger = store.reconstruct_budget(&state.run_id).expect("ledger");
    assert_eq!(ledger.held_calls, 1);
    assert_eq!(ledger.used_calls, 0);
    assert!(!ledger.reservations["call"].released);
}

#[test]
fn rejects_invalid_money_cap() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-invalid-money-cap");
    for (name, money_cap) in [("zero", "0"), ("negative", "-1"), ("fraction", "1.5")] {
        let path = temp.path().join(format!("{name}.js"));
        fs::write(
            &path,
            format!(
                "export const meta = {{ name: \"cap\", contract: \"workflow\", moneyCap: {money_cap} }}; return {{}};"
            ),
        )
        .expect("write script");
        let error = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
            .prepare(&path, Value::Null, 1, 1)
            .expect_err("invalid money cap must fail");
        assert!(error.to_string().contains("meta.moneyCap"));
    }
}

#[test]
fn new_runs_require_explicit_contract() {
    let temp = TempDir::new().expect("tempdir");
    let missing = temp.path().join("missing.js");
    fs::write(
        &missing,
        "export const meta = { name: \"legacy\" }; return {};",
    )
    .expect("write missing contract fixture");
    let wrong = temp.path().join("wrong.js");
    fs::write(
        &wrong,
        "export const meta = { name: \"wrong\", contract: \"workflow.v3\" }; return {};",
    )
    .expect("write wrong contract fixture");
    let runtime = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    );

    let missing_error = runtime
        .start(&missing, Value::Null, 1, 10)
        .expect_err("missing contract must fail");
    assert!(missing_error.to_string().contains("workflow"));
    let wrong_error = runtime
        .start(&wrong, Value::Null, 1, 10)
        .expect_err("wrong contract must fail");
    assert!(
        wrong_error
            .to_string()
            .contains("unsupported workflow `contract`")
    );
    assert_eq!(
        fs::read_dir(temp.path().join("state").join("runs"))
            .map(|entries| entries.count())
            .unwrap_or(0),
        0
    );
}

#[test]
fn computed_metadata_initializer_is_rejected() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("computed-metadata.js");
    fs::write(
        &path,
        r#"
        export const meta = makeMeta({ contract: "workflow" });
        return {};
        "#,
    )
    .expect("write computed metadata fixture");

    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("computed metadata must not satisfy the contract");
    assert!(error.to_string().contains("workflow"));
}

#[test]
fn metadata_decoy_before_real_declaration_is_ignored() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("metadata-decoy.js");
    fs::write(
        &path,
        r#"
        // export const meta = { name: "decoy" };
        const text = "export const meta = { contract: 'workflow' };";
        export const meta = { name: "missing-contract" };
        return {};
        "#,
    )
    .expect("write metadata decoy fixture");
    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("decoy metadata must not satisfy the contract");
    assert!(error.to_string().contains("workflow"));
}

#[test]
fn regex_metadata_decoy_is_ignored() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("regex-metadata-decoy.js");
    fs::write(
        &path,
        r#"
        const marker = /export const meta = { contract: "workflow" }/;
        return { accepted: marker.test("meta") };
        "#,
    )
    .expect("write regex metadata decoy fixture");

    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("regex text must not satisfy the contract");
    assert!(error.to_string().contains("workflow"));
}

#[test]
fn nested_metadata_declaration_is_ignored() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("nested-metadata-decoy.js");
    fs::write(
        &path,
        r#"
        function unused() { export const meta = { contract: "workflow" }; }
        return { ok: true };
        "#,
    )
    .expect("write nested metadata decoy fixture");

    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("nested metadata must not satisfy the contract");
    assert!(
        !error.to_string().is_empty(),
        "nested export declaration must be rejected before execution"
    );
}

#[test]
fn metadata_declaration_allows_comments_before_assignment() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("comment-before-assignment.js");
    fs::write(
        &path,
        r#"
        export const meta /* note = { decoy } */ = {
          contract: "workflow",
        };
        return {};
        "#,
    )
    .expect("write commented assignment fixture");

    let state = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("comments before assignment must be supported");
    assert_eq!(state.contract.as_deref(), Some("workflow"));
}

#[test]
fn contract_metadata_accepts_json5_comments() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("commented-meta.js");
    fs::write(
        &path,
        r#"export const meta = {
          // this comment has a closing brace: }
          name: "commented",
          /* and this one has a quote: " */
          contract: "workflow",
        };
        return { ok: true };"#,
    )
    .expect("write commented metadata fixture");

    let state = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("JSON5 comments must not break metadata extraction");
    assert_eq!(state.contract.as_deref(), Some("workflow"));
    assert_eq!(state.status, RunStatus::Succeeded);
}

#[test]
fn legacy_nonterminal_run_resumes_without_rewriting_journal() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let store = WorkflowStore::new(&state_root);
    let path = temp.path().join("legacy.js");
    let source = "export const meta = { name: \"legacy\" }; return { legacy: true };";
    fs::write(&path, source).expect("write legacy fixture");
    let state = legacy_state(&store, &path, RunStatus::Failed);
    store
        .create_run(&state, source)
        .expect("persist legacy run");
    let journal = br#"{"at":"2026-01-01T00:00:00Z","key":"old#0","kind":"command","state":"succeeded","label":"old","result":{"ok":true}}
"#;
    fs::write(store.journal_path(&state.run_id), journal).expect("write frozen journal");
    let before = fs::read(store.journal_path(&state.run_id)).expect("read journal before resume");

    let resumed = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .resume(&state.run_id)
        .expect("resume legacy run");

    assert_eq!(resumed.version, 1);
    assert_eq!(resumed.contract, None);
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(resumed.result, Some(json!({"legacy": true})));
    assert!(!store.events_path(&state.run_id).exists());
    assert_eq!(
        fs::read(store.journal_path(&state.run_id)).expect("read journal after resume"),
        before
    );
}

#[test]
fn events_are_append_only_and_reconstruct_terminal_state() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let path = script(
        &temp,
        "events.js",
        "phase(\"verify\"); return { outcome: \"ok\" };",
    );
    let state = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, json!({"input": 1}), 2, 12)
        .expect("run workflow");
    let store = WorkflowStore::new(&state_root);
    let events = store.read_events(&state.run_id).expect("read events");

    assert_eq!(state.version, 2);
    assert_eq!(state.contract.as_deref(), Some("workflow"));
    assert_eq!(events.len(), 3);
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let reconstructed = store
        .reconstruct_state(&state.run_id)
        .expect("reconstruct state from persisted events");
    assert_eq!(reconstructed.status, state.status);
    assert_eq!(reconstructed.phase, state.phase);
    assert_eq!(reconstructed.result, state.result);
    assert_eq!(reconstructed.max_parallel, state.max_parallel);
    assert_eq!(reconstructed.max_calls, state.max_calls);
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(store.journal_path(&state.run_id))
        .and_then(|mut file| {
            use std::io::Write;
            writeln!(
                file,
                r#"{{"at":"2026-01-01T00:00:00Z","key":"stale#0","kind":"agent","state":"submitted","label":"stale"}}"#
            )
        })
        .expect("append stale submitted journal entry");
    let reconstructed = store
        .reconstruct_state(&state.run_id)
        .expect("reconstruct terminal state with stale journal");
    assert!(
        reconstructed.active.is_empty(),
        "terminal lifecycle event must override stale submitted calls"
    );

    let mut torn_events = fs::read_to_string(store.events_path(&state.run_id))
        .expect("read event stream before tear");
    torn_events.push_str("{\"broken\":\n");
    fs::write(store.events_path(&state.run_id), torn_events).expect("write torn event tail");
    assert!(store.reconstruct_state(&state.run_id).is_err());
}

#[test]
fn gate_rejection_is_recorded_as_terminal_event() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let path = script(
        &temp,
        "gate-reject.js",
        "await gate(\"approve release?\", { label: \"release\" }); return { released: true };",
    );
    let runtime = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let waiting = runtime
        .start(&path, Value::Null, 1, 10)
        .expect("run to gate");
    assert_eq!(waiting.status, RunStatus::WaitingHuman);

    let rejected = runtime
        .approve(&waiting.run_id, false, "not approved".to_owned(), None)
        .expect("reject gate");
    let store = WorkflowStore::new(&state_root);
    let events = store.read_events(&waiting.run_id).expect("read events");
    assert_eq!(events.len(), 4);
    assert!(matches!(
        events[1].event,
        servitor_workflows::WorkflowEvent::GateOpened { .. }
    ));
    assert!(matches!(
        events[2].event,
        servitor_workflows::WorkflowEvent::GateDecided {
            approved: false,
            ..
        }
    ));
    let reconstructed = store
        .reconstruct_state(&waiting.run_id)
        .expect("reconstruct rejected gate");
    assert_eq!(reconstructed.status, rejected.status);
    assert_eq!(reconstructed.error, rejected.error);
}

#[test]
fn cancellation_is_recorded_as_terminal_event() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let path = script(&temp, "cancel.js", "return { unused: true };");
    let runtime = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let prepared = runtime
        .prepare(&path, Value::Null, 1, 10)
        .expect("prepare run");
    let cancelled = runtime
        .cancel(&prepared.run_id, "operator cancelled".to_owned())
        .expect("cancel prepared run");
    let store = WorkflowStore::new(&state_root);
    let events = store.read_events(&prepared.run_id).expect("read events");
    assert!(matches!(
        events.last().expect("terminal event").event,
        servitor_workflows::WorkflowEvent::RunCancelled { .. }
    ));
    let reconstructed = store
        .reconstruct_state(&prepared.run_id)
        .expect("reconstruct cancelled run");
    assert_eq!(reconstructed.status, cancelled.status);
    assert_eq!(reconstructed.error, cancelled.error);
}

#[test]
fn pending_cancellation_reason_survives_resume() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let path = script(&temp, "cancel-resume.js", "return { unused: true };");
    let runtime = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let prepared = runtime
        .prepare(&path, Value::Null, 1, 10)
        .expect("prepare run");
    let state_path = runtime.store().state_path(&prepared.run_id);
    let mut persisted: Value =
        serde_json::from_slice(&fs::read(&state_path).expect("read prepared state"))
            .expect("decode prepared state");
    persisted["active"] = json!({
        "active#0": {
            "kind": "agent",
            "label": "active",
            "started_at": Utc::now(),
        }
    });
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&persisted).expect("encode active state"),
    )
    .expect("seed active call");
    let cancelling = runtime
        .cancel(&prepared.run_id, "operator cancelled".to_owned())
        .expect("request cancellation");
    assert_eq!(cancelling.status, RunStatus::Cancelling);

    let resumed = runtime
        .resume(&prepared.run_id)
        .expect("resume cancellation");
    assert_eq!(resumed.status, RunStatus::Cancelled);
    assert_eq!(
        resumed.error.as_deref(),
        Some("cancelled: operator cancelled")
    );
    let reconstructed = runtime
        .store()
        .reconstruct_state(&prepared.run_id)
        .expect("reconstruct resumed cancellation");
    assert_eq!(reconstructed.error, resumed.error);
}

#[test]
fn pause_resume_and_supersede_reconstruct_from_events() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(250)));
    let path = script(
        &temp,
        "pause-resume.js",
        "phase(\"prepare\"); const result = await agent(\"LONG\"); return { result };",
    );
    let runtime = engine(&root, Arc::clone(&transport));
    let prepared = runtime
        .prepare(&path, Value::Null, 1, 10)
        .expect("prepare run");
    let run_id = prepared.run_id;
    let paused = runtime.pause(&run_id).expect("pause run");
    assert_eq!(paused.status, RunStatus::Paused);

    let resumed = runtime.resume(&run_id).expect("resume run");
    assert_eq!(resumed.status, RunStatus::Succeeded, "{:?}", resumed.error);
    let store = WorkflowStore::new(&root);
    let events = store.read_events(&run_id).expect("read lifecycle events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event.event, servitor_workflows::WorkflowEvent::RunPaused))
    );
    assert!(events.iter().any(|event| matches!(
        event.event,
        servitor_workflows::WorkflowEvent::RunResumed { resume_count: 1 }
    )));
    let reconstructed = store
        .reconstruct_state(&run_id)
        .expect("reconstruct resumed run");
    assert_eq!(reconstructed.status, resumed.status);
    assert_eq!(reconstructed.phase, resumed.phase);
    assert_eq!(reconstructed.resume_count, resumed.resume_count);

    let redirect = runtime
        .prepare(&path, Value::Null, 1, 10)
        .expect("prepare supersede run");
    let superseded = runtime
        .supersede(
            &redirect.run_id,
            "redirected by operator".to_owned(),
            Some("evidence.md".to_owned()),
            Some("next-contract.md".to_owned()),
        )
        .expect("supersede run");
    let reconstructed = store
        .reconstruct_state(&redirect.run_id)
        .expect("reconstruct superseded run");
    assert_eq!(reconstructed.status, superseded.status);
    assert_eq!(
        reconstructed
            .supersede
            .expect("reconstructed supersede")
            .reason,
        "redirected by operator"
    );
}
#[test]
fn dynamic_pipeline_fans_out_and_runs_concurrently() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(200)));
    let path = script(
        &temp,
        "dynamic.js",
        r#"
        const found = await agent("DISCOVER", {
          schema: { type: "object", required: ["items"], properties: { items: { type: "array", items: { type: "string" } } } }
        });
        const results = await pipeline(found.items, item => agent(`WORK ${item}`));
        return { items: found.items, results };
    "#,
    );
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 4, 100)
        .expect("run workflow");
    assert_eq!(state.status, RunStatus::Succeeded);
    assert_eq!(
        state.result,
        Some(json!({"items":["a","b","c"],"results":["a-ok","b-ok","c-ok"]}))
    );
    assert_eq!(transport.count(), 4);
    assert!(
        transport.peak_inspections() >= 3,
        "fan-out was not concurrent"
    );
}

#[test]
fn gate_replay_uses_cached_calls() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let path = script(
        &temp,
        "gate.js",
        r#"
        const before = await agent("BEFORE");
        const decision = await gate("ship it?", { label: "ship" });
        const after = await agent("AFTER");
        return { before, decision, after };
    "#,
    );
    let root = temp.path().join("state");
    let first = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 2, 20)
        .expect("first run");
    assert_eq!(first.status, RunStatus::WaitingHuman);
    assert_eq!(transport.count(), 1);
    let completed = engine(&root, Arc::clone(&transport))
        .approve(&first.run_id, true, "evidence accepted".to_owned(), None)
        .expect("approve");
    assert_eq!(completed.status, RunStatus::Succeeded);
    assert_eq!(transport.count(), 2, "completed call was submitted again");
    assert_eq!(
        completed.result.expect("result")["decision"]["approved"],
        true
    );
}

#[test]
fn invalid_structured_agent_correction_threads_prior_continuation() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-retry-agent");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "retry-agent.js",
        r#"
        return await agent("RETRY_JSON", {
          schema: {
            type: "object",
            required: ["ok"],
            properties: { ok: { type: "boolean" } }
          }
        });
    "#,
    );

    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal state");
    assert_eq!(state.status, RunStatus::Succeeded);
    assert_eq!(state.result, Some(json!({"ok": true})));
    assert_eq!(transport.count(), 2);
    // B1 default-ON threading: first submit is cold; the correction retry
    // threads the failed first attempt's continuation (`sess-fake-1`).
    let requests = transport.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].continuation, None);
    assert_eq!(requests[1].continuation.as_deref(), Some("sess-fake-1"));
}

#[test]
fn structured_agent_corrects_invalid_output_once_and_preserves_options() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-correct-once");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "correct-once.js",
        r#"return await agent("RETRY_JSON", {
          agent: "claude", model: "model-x", cwd: "nested", systemPrompt: "system-x",
          timeoutSeconds: 42, nativeArgs: ["--flag"],
          schema: { type: "object", required: ["ok"], properties: { ok: { type: "boolean" } } }
        });"#,
    );
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("structured workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"ok": true})));
    assert_eq!(transport.count(), 2);
    let requests = transport.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].agent, requests[1].agent);
    assert_eq!(requests[0].model, requests[1].model);
    assert_eq!(requests[0].cwd, requests[1].cwd);
    assert_eq!(requests[0].system_prompt, requests[1].system_prompt);
    assert_eq!(requests[0].timeout_seconds, requests[1].timeout_seconds);
    assert_eq!(requests[0].native_args, requests[1].native_args);
    // B1 default-ON continuation threading + "makeup exam" fix: the first
    // submit is a cold call (no prior session for this resolved agent), so
    // `requests[0].continuation` is None. The correction retry must carry the
    // FAILED first attempt's continuation so the model sees its own prior
    // reasoning instead of a cold re-read. FakeTransport echoes the first
    // attempt's continuation as `sess-fake-1`, and the retry submit must thread
    // that exact value forward.
    assert_eq!(
        requests[0].continuation, None,
        "first submit is cold; no prior session cached for this agent"
    );
    assert_eq!(
        requests[1].continuation.as_deref(),
        Some("sess-fake-1"),
        "correction retry must thread the failed attempt's continuation"
    );
    let prompt = match &requests[1].input {
        Input::Text { text } => text,
        Input::Image(_) => panic!("text correction prompt"),
    };
    assert!(prompt.contains("Original task:\nRETRY_JSON"));
    assert!(prompt.contains("JSON Schema:"));
    assert!(prompt.contains("Validation error:"));
    assert!(prompt.contains("Invalid output excerpt (bounded):\nnot-json"));
    assert!(prompt.contains("Return only corrected JSON"));
}

#[test]
fn structured_agent_corrects_provider_success_that_fails_local_strict_schema() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "strict-correct-once.js",
        r##"return await agent("STRICT_RETRY_JSON", {
          schema: {
            "$defs": {
              "result": {
                "type": "object",
                "required": ["kind", "score"],
                "properties": {
                  "kind": { "const": "ok" },
                  "score": { "type": "integer", "minimum": 0, "maximum": 10 }
                },
                "additionalProperties": false
              }
            },
            "$ref": "#/$defs/result"
          }
        });"##,
    );

    let state = engine(
        &temp.path().join("state-strict-correct"),
        Arc::clone(&transport),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("workflow completes after correction");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"kind":"ok","score":7})));
    assert_eq!(
        transport.count(),
        2,
        "local validation must force one correction"
    );
    let requests = transport.requests.lock().expect("requests");
    // B1 default-ON threading + "makeup exam" fix: first submit cold, retry
    // threads the failed attempt's continuation (`sess-fake-1`).
    assert_eq!(requests[0].continuation, None);
    assert_eq!(requests[1].continuation.as_deref(), Some("sess-fake-1"));
    let correction = match &requests[1].input {
        Input::Text { text } => text,
        Input::Image(_) => panic!("text correction prompt"),
    };
    assert!(correction.contains("unexpected"), "{correction}");
    assert!(correction.contains("not allowed"), "{correction}");
}
#[test]
fn structured_agent_accepts_valid_first_output_without_correction() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "valid-first.js",
        r#"return await agent("VALID_FIRST", { schema: { type: "object", required: ["ok"] } });"#,
    );
    let state = engine(
        &temp.path().join("state-valid-first"),
        Arc::clone(&transport),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded);
    assert_eq!(transport.count(), 1);
}

#[test]
fn exhausted_schema_correction_fails_and_resume_does_not_submit_third_time() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-invalid-twice");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "invalid-twice.js",
        r#"return await agent("INVALID_TWICE", { schema: { type: "object", required: ["ok"] } });"#,
    );
    let failed = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal workflow");
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(transport.count(), 2);
    // B1 default-ON + makeup-exam: first submit cold, correction retry threads
    // the failed attempt's continuation (`sess-fake-1`). Both attempts return
    // invalid JSON, so the run still fails after one correction.
    {
        let requests = transport.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].continuation, None);
        assert_eq!(requests[1].continuation.as_deref(), Some("sess-fake-1"));
    }
    let journal = fs::read_to_string(WorkflowStore::new(&root).journal_path(&failed.run_id))
        .expect("journal");
    let terminal: Value = serde_json::from_str(journal.lines().last().expect("terminal entry"))
        .expect("terminal json");
    assert_eq!(
        terminal["schema_correction"]["transport_run_ids"],
        json!(["fake-1", "fake-2"])
    );
    assert_eq!(
        terminal["schema_correction"]["validation_errors"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&failed.run_id)
        .expect("resume exhausted workflow");
    assert_eq!(resumed.status, RunStatus::Failed);
    assert_eq!(
        transport.count(),
        2,
        "resume submitted a third transport run"
    );
}

#[test]
fn submitted_schema_correction_is_polled_on_resume_without_third_submission() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-submitted-correction");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "submitted-correction.js",
        r#"return await agent("INVALID_TWICE", { schema: { type: "object", required: ["ok"] } });"#,
    );
    let failed = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal workflow");
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(transport.count(), 2);

    let journal_path = WorkflowStore::new(&root).journal_path(&failed.run_id);
    let journal = fs::read_to_string(&journal_path).expect("journal");
    let crash_journal = journal.lines().take(2).collect::<Vec<_>>().join("\n");
    let submitted: Value = serde_json::from_str(
        crash_journal
            .lines()
            .last()
            .expect("correction submission entry"),
    )
    .expect("submitted correction json");
    assert_eq!(submitted["state"], "submitted");
    assert_eq!(submitted["transport_run_id"], "fake-2");
    assert_eq!(submitted["schema_correction"]["attempted"], true);
    assert!(submitted["schema_correction"]["schema_sha256"].is_string());
    fs::write(&journal_path, format!("{crash_journal}\n")).expect("simulate crash journal");

    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&failed.run_id)
        .expect("resume submitted correction");
    assert_eq!(resumed.status, RunStatus::Failed);
    assert_eq!(
        transport.count(),
        2,
        "resume submitted a third transport run"
    );
    let terminal: Value = serde_json::from_str(
        fs::read_to_string(&journal_path)
            .expect("resumed journal")
            .lines()
            .last()
            .expect("terminal correction entry"),
    )
    .expect("terminal correction json");
    assert_eq!(terminal["state"], "failed");
    assert_eq!(
        terminal["schema_correction"]["transport_run_ids"],
        json!(["fake-1", "fake-2"])
    );
    assert_eq!(
        terminal["schema_correction"]["validation_errors"]
            .as_array()
            .expect("validation errors")
            .len(),
        2
    );
}

#[test]
fn transport_submission_failure_has_no_schema_correction_retry() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::failing(Duration::ZERO));
    let path = script(
        &temp,
        "transport-failure.js",
        r#"return await agent("TRANSPORT_FAIL", { schema: { type: "object" } });"#,
    );
    let state = engine(
        &temp.path().join("state-transport-failure"),
        Arc::clone(&transport),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("terminal workflow");
    assert_eq!(state.status, RunStatus::Failed);
    assert_eq!(transport.count(), 1);
}

#[test]
fn correction_submission_failure_is_not_retried_on_resume() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-correction-submit-failure");
    let transport = Arc::new(FakeTransport::fail_after(Duration::ZERO, 1));
    let path = script(
        &temp,
        "correction-submit-failure.js",
        r#"return await agent("INVALID_TWICE", { schema: { type: "object" } });"#,
    );
    let failed = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal workflow");
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(transport.count(), 2);
    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&failed.run_id)
        .expect("resume exhausted workflow");
    assert_eq!(resumed.status, RunStatus::Failed);
    assert_eq!(transport.count(), 2);
}

#[test]
fn command_returns_deterministic_evidence() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "command.js",
        r#"
        const checked = await command("rustc", ["--version"], { timeoutSeconds: 30 });
        return checked;
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("command workflow");
    assert_eq!(state.status, RunStatus::Succeeded);
    let result = state.result.expect("result");
    assert_eq!(result["exitCode"], 0);
    assert!(
        result["stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("rustc")
    );
}

#[test]
fn command_result_is_structured_and_persisted() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "command.js",
        r#"
        const ok = await command("rustc", ["--version"], { timeoutSeconds: 30 });
        return ok;
    "#,
    );
    let state_root = temp.path().join("state-cmdresult");
    let state = engine(&state_root, transport)
        .start(&path, Value::Null, 1, 10)
        .expect("command workflow");
    assert_eq!(state.status, RunStatus::Succeeded);
    let result = state.result.expect("result");
    assert_eq!(result["exitCode"], 0);
    assert_eq!(result["timedOut"], false);
    assert_eq!(result["stdoutTruncated"], false);
    assert_eq!(result["stderrTruncated"], false);
    assert!(result["durationMs"].as_u64().is_some());
    assert_eq!(result["argv"][0].as_str().unwrap_or_default(), "rustc");
    assert!(result["cwd"].as_str().unwrap_or_default().len() > 1);

    let store = WorkflowStore::new(&state_root);
    let run_dir = store.run_dir(&state.run_id).join("commands");
    let mut found = false;
    for entry in fs::read_dir(&run_dir).expect("commands dir") {
        let entry = entry.expect("entry");
        let candidate = entry.path().join("result.json");
        if candidate.exists() {
            let disk: Value =
                serde_json::from_str(&fs::read_to_string(&candidate).expect("read result.json"))
                    .expect("parse result.json");
            assert_eq!(disk["exitCode"], 0);
            assert_eq!(disk["timedOut"], false);
            assert!(
                disk["stdout"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("rustc")
            );
            found = true;
        }
    }
    assert!(found, "expected at least one persisted command result.json");
}

#[test]
fn command_failure_persists_result_and_error() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "command.js",
        r#"
        try {
          await command("rustc", ["--definitely-not-a-real-flag-xyz"], { timeoutSeconds: 30 });
          return { unreachable: true };
        } catch (error) {
          return { caught: String(error) };
        }
    "#,
    );
    let state_root = temp.path().join("state-cmdfail");
    let state = engine(&state_root, transport)
        .start(&path, Value::Null, 1, 10)
        .expect("command workflow");
    assert_eq!(state.status, RunStatus::Succeeded);
    let result = state.result.expect("result");
    assert!(
        result["caught"]
            .as_str()
            .unwrap_or_default()
            .contains("command exited")
    );

    let store = WorkflowStore::new(&state_root);
    let run_dir = store.run_dir(&state.run_id).join("commands");
    let mut found = false;
    for entry in fs::read_dir(&run_dir).expect("commands dir") {
        let entry = entry.expect("entry");
        let candidate = entry.path().join("result.json");
        if candidate.exists() {
            let disk: Value =
                serde_json::from_str(&fs::read_to_string(&candidate).expect("read result.json"))
                    .expect("parse result.json");
            assert_ne!(disk["exitCode"], 0);
            found = true;
        }
    }
    assert!(found, "expected persisted result.json for failed command");
}

#[test]
fn result_report_is_validated_as_a_delivery_artifact() {
    let temp = TempDir::new().expect("tempdir");
    let report = temp.path().join("delivery.html");
    fs::write(&report, "<html>delivery</html>").expect("write delivery report");
    let report_json = serde_json::to_string(&report).expect("serialize report path");
    let path = script(
        &temp,
        "delivery.js",
        &format!(r#"return {{ summary: "done", report: {report_json} }};"#),
    );
    let state = engine(
        &temp.path().join("state-delivery"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("delivery workflow");

    assert_eq!(state.status, RunStatus::Succeeded);
    assert_eq!(state.report.as_deref(), Some(report.as_path()));
    assert!(state.run_summary.is_some());
}

#[test]
fn script_supersede_requests_cancellation_for_active_calls() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-script-supersede");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(400)));
    let path = script(
        &temp,
        "script-supersede.js",
        r#"
            const pending = agent("LONG");
            await supersede({ reason: "contract replaced" });
        "#,
    );

    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("superseded workflow");

    assert_eq!(state.status, RunStatus::Superseded);
    assert!(
        root.join("runs")
            .join(&state.run_id)
            .join("cancel.request")
            .exists(),
        "script-initiated supersede must request active-call cancellation"
    );
}

#[test]
fn missing_delivery_report_fails_the_run() {
    let temp = TempDir::new().expect("tempdir");
    let missing = temp.path().join("missing.html");
    let missing_json = serde_json::to_string(&missing).expect("serialize missing path");
    let path = script(
        &temp,
        "missing-delivery.js",
        &format!(r#"return {{ summary: "done", report: {missing_json} }};"#),
    );
    let state = engine(
        &temp.path().join("state-missing-delivery"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("workflow terminal state");

    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("delivery report does not exist")
    );
    assert!(state.report.is_none());
    assert!(state.run_summary.is_some());

    fs::write(&missing, "<html>recovered</html>").expect("write recovered report");
    let resumed = engine(
        &temp.path().join("state-missing-delivery"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .resume(&state.run_id)
    .expect("resume failed workflow");
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(resumed.resume_count, 1);
    assert_eq!(resumed.report.as_deref(), Some(missing.as_path()));
}

#[cfg(windows)]
#[test]
fn process_isolation_cancellation_terminates_descendants() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-process-isolation");
    let marker = temp.path().join("descendant-marker.txt");
    let escaped_marker = marker.display().to_string().replace('\\', "\\\\");
    let path = temp.path().join("process-isolation.js");
    fs::write(
        &path,
        format!(
            r#"export const meta = {{
              name: "process-isolation",
              contract: "workflow",
              boundary: {{ isolation: "process", readPaths: ["."], writePaths: ["."] }}
            }};
            return await command("pwsh", ["-NoProfile", "-Command", "Start-Process pwsh -ArgumentList '-NoProfile', '-Command', 'Start-Sleep -Milliseconds 750; Set-Content -LiteralPath ''{escaped_marker}'' -Value escaped'; Start-Sleep -Seconds 10"]);"#
        ),
    )
    .expect("write process-isolation fixture");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let runner_root = root.clone();
    let runner_transport = Arc::clone(&transport);
    let path_copy = path.clone();
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&path_copy, Value::Null, 1, 10)
    });
    let run_id = wait_for_active_run(&root);
    engine(&root, Arc::clone(&transport))
        .cancel(&run_id, "test process-tree cleanup".to_owned())
        .expect("cancel process-isolated workflow");
    let terminal = runner.join().expect("join").expect("terminal state");
    assert_eq!(terminal.status, RunStatus::Cancelled);
    thread::sleep(Duration::from_secs(1));
    assert!(
        !marker.exists(),
        "descendant wrote after its process-isolated parent was cancelled"
    );
}

#[test]
fn pause_and_cancel_interrupt_active_calls() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(400)));
    let path = script(&temp, "long.js", "return await agent(\"LONG\");");
    let runner_root = root.clone();
    let runner_transport = Arc::clone(&transport);
    let path_copy = path.clone();
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&path_copy, Value::Null, 1, 10)
    });
    let run_id = wait_for_active_run(&root);
    let paused = engine(&root, Arc::clone(&transport))
        .pause(&run_id)
        .expect("pause");
    assert!(matches!(
        paused.status,
        RunStatus::Pausing | RunStatus::Paused
    ));
    let paused_reconstruction = WorkflowStore::new(&root)
        .reconstruct_state(&run_id)
        .expect("reconstruct pause request");
    assert_eq!(paused_reconstruction.status, paused.status);
    let final_pause = runner.join().expect("join").expect("runner result");
    assert_eq!(final_pause.status, RunStatus::Paused);
    assert!(
        !root
            .join("runs")
            .join(&run_id)
            .join("pause.request")
            .exists(),
        "pause.request must be cleared after pause terminalizes"
    );

    let second_root = temp.path().join("state-2");
    let runner_root = second_root.clone();
    let runner_transport = Arc::clone(&transport);
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&path, Value::Null, 1, 10)
    });
    let second_id = wait_for_active_run(&second_root);
    let cancelling = engine(&second_root, Arc::clone(&transport))
        .cancel(&second_id, "test cancel audit reason".to_owned())
        .expect("cancel");
    assert!(matches!(
        cancelling.status,
        RunStatus::Cancelling | RunStatus::Cancelled
    ));
    let cancelling_reconstruction = WorkflowStore::new(&second_root)
        .reconstruct_state(&second_id)
        .expect("reconstruct cancellation request");
    assert_eq!(cancelling_reconstruction.status, cancelling.status);
    assert_eq!(
        cancelling_reconstruction.error.as_deref(),
        Some("cancelled: test cancel audit reason"),
        "cancellation reason must reconstruct during drain"
    );
    let final_cancel = runner.join().expect("join").expect("runner result");
    assert_eq!(final_cancel.status, RunStatus::Cancelled);
    assert_eq!(
        final_cancel.error.as_deref(),
        Some("cancelled: test cancel audit reason"),
        "cancel reason must survive into terminal state.json"
    );
    assert!(
        !second_root
            .join("runs")
            .join(&second_id)
            .join("cancel.request")
            .exists(),
        "cancel.request must be cleared after cancel terminalizes"
    );
}

#[test]
fn resume_keeps_run_id_replays_journal_and_writes_run_summary() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-resume");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(300)));
    let path = script(
        &temp,
        "resume.js",
        r#"
        const first = await agent("FIRST");
        const second = await agent("SECOND");
        return { first, second };
    "#,
    );
    let runner_root = root.clone();
    let runner_transport = Arc::clone(&transport);
    let path_copy = path.clone();
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&path_copy, Value::Null, 1, 10)
    });
    wait_for_call_count(&transport, 2);
    let run_id = wait_for_active_run(&root);
    engine(&root, Arc::clone(&transport))
        .pause(&run_id)
        .expect("pause");
    let paused = runner.join().expect("join").expect("paused run");
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(paused.run_id, run_id);
    assert_eq!(transport.count(), 2);

    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&run_id)
        .expect("resume");
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(resumed.run_id, run_id);
    assert_eq!(resumed.resume_count, 1);
    assert_eq!(transport.count(), 2, "completed calls were submitted again");
    assert!(resumed.report.is_none());
    let summary = resumed.run_summary.expect("terminal run summary path");
    let html = fs::read_to_string(summary).expect("read run summary");
    assert!(html.contains(&run_id));
    assert!(html.contains("Workflow 运行摘要"));
}

#[test]
fn agent_journal_records_phase_duration_and_usage() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-agent-observability");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let path = script(
        &temp,
        "agent-observability.js",
        r#"phase("collect"); const a = await agent("hello"); return { done: a };"#,
    );
    let state = engine(&root, transport)
        .start(&path, Value::Null, 1, 10)
        .expect("agent workflow");
    let lines = fs::read_to_string(WorkflowStore::new(&root).journal_path(&state.run_id))
        .expect("read journal");
    let entries = lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("journal value"))
        .collect::<Vec<_>>();
    let submitted = entries
        .iter()
        .find(|entry| entry["state"] == "submitted")
        .expect("submitted entry");
    assert_eq!(submitted["phase"], "collect");
    assert!(submitted.get("duration_ms").is_none());
    let succeeded = entries
        .iter()
        .find(|entry| entry["state"] == "succeeded")
        .expect("succeeded entry");
    assert_eq!(succeeded["phase"], "collect");
    assert!(succeeded["duration_ms"].as_u64().is_some());
    assert_eq!(succeeded["usage"], json!({"input": 100, "output": 20}));
}

#[test]
fn command_journal_records_phase_and_duration() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-command-observability");
    let path = script(
        &temp,
        "command-observability.js",
        r#"phase("build"); return await command("rustc", ["--version"]);"#,
    );
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 10)
        .expect("command workflow");
    let lines = fs::read_to_string(WorkflowStore::new(&root).journal_path(&state.run_id))
        .expect("read journal");
    let succeeded = lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("journal value"))
        .find(|entry| entry["state"] == "succeeded")
        .expect("succeeded entry");
    assert_eq!(succeeded["phase"], "build");
    assert!(succeeded["duration_ms"].as_u64().is_some());
    assert!(succeeded.get("usage").is_none());
}

#[test]
fn cancelling_waiting_gate_clears_live_and_reconstructed_gate() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-cancel-waiting");
    let path = script(
        &temp,
        "cancel-waiting.js",
        r#"return await gate("ship it?", { label: "ship" });"#,
    );
    let runtime = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let waiting = runtime.start(&path, Value::Null, 1, 10).expect("start");
    assert_eq!(waiting.status, RunStatus::WaitingHuman);
    assert!(waiting.waiting_gate.is_some());

    let cancelled = runtime
        .cancel(&waiting.run_id, "operator cancelled".to_owned())
        .expect("cancel waiting run");
    assert_eq!(cancelled.status, RunStatus::Cancelled);
    assert!(cancelled.waiting_gate.is_none());
    let reconstructed = runtime
        .store()
        .reconstruct_state(&waiting.run_id)
        .expect("reconstruct cancelled waiting run");
    assert_eq!(reconstructed.waiting_gate, cancelled.waiting_gate);
}

#[test]
fn waiting_human_writes_run_summary() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-waiting-summary");
    let path = script(
        &temp,
        "waiting-summary.js",
        r#"const decision = await gate("ship it?", { label: "ship", expect: "approval", current: { contract: "<script>alert(1)</script>", nested: ["<&>"] }, hint: "check evidence" }); return decision;"#,
    );
    let engine = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let waiting = engine.start(&path, Value::Null, 1, 10).expect("start");
    assert_eq!(waiting.status, RunStatus::WaitingHuman);
    let waiting_summary = waiting.run_summary.expect("waiting summary");
    let html = fs::read_to_string(&waiting_summary).expect("read waiting summary");
    assert!(html.contains("<h2>等待人工审批</h2>"));
    assert!(html.contains("ship it?"));
    assert!(html.contains("<dt>期望</dt><dd>approval</dd>"));
    assert!(html.contains("<dt>当前值</dt><dd><pre>{\n  &quot;contract&quot;: &quot;&lt;script&gt;alert(1)&lt;/script&gt;&quot;,\n  &quot;nested&quot;: [\n    &quot;&lt;&amp;&gt;&quot;\n  ]\n}</pre></dd>"));
    assert!(!html.contains("<script>alert(1)</script>"));
    assert!(html.contains("执行暂停，等待人工闸门审批"));
    let persisted = engine
        .store()
        .load_state(&waiting.run_id)
        .expect("load persisted waiting state");
    assert_eq!(persisted.status, RunStatus::WaitingHuman);
    assert_eq!(persisted.run_summary.as_ref(), Some(&waiting_summary));
    let completed = engine
        .approve(&waiting.run_id, true, "approved".to_owned(), None)
        .expect("approve");
    let html = fs::read_to_string(completed.run_summary.expect("terminal summary"))
        .expect("read terminal summary");
    assert!(!html.contains("<h2>等待人工审批</h2>"));
}

#[test]
fn waiting_human_summary_write_failure_is_propagated_without_changing_state() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-waiting-summary-failure");
    let path = script(
        &temp,
        "waiting-summary-failure.js",
        r#"return await gate("ship it?", { label: "ship" });"#,
    );
    let engine = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let prepared = engine
        .prepare(&path, Value::Null, 1, 10)
        .expect("prepare waiting workflow");
    let summary_path = engine.store().run_summary_path(&prepared.run_id);
    fs::create_dir(&summary_path).expect("block summary file with directory");

    let error = engine
        .execute_existing(&prepared.run_id)
        .expect_err("summary write failure must be returned");
    assert_eq!(error.payload().code, "write_failed");
    assert!(error.to_string().contains("run-summary.html"));
    let persisted = engine
        .store()
        .load_state(&prepared.run_id)
        .expect("load waiting state after failure");
    assert_eq!(persisted.status, RunStatus::WaitingHuman);
    assert!(persisted.waiting_gate.is_some());
    assert!(persisted.run_summary.is_none());
}

#[test]
fn run_summary_has_phase_duration_token_columns_and_chip() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-summary-observability");
    let path = script(
        &temp,
        "summary-observability.js",
        r#"phase("collect"); return await agent("hello");"#,
    );
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 10)
        .expect("agent workflow");
    let html = fs::read_to_string(state.run_summary.expect("summary")).expect("read summary");
    assert!(html.contains("<th>阶段</th>"));
    assert!(html.contains("<th>耗时</th>"));
    assert!(html.contains("<th>Tokens</th>"));
    assert!(html.contains(">collect</td>"));
    assert!(html.contains("<span class=\"chip\">Tokens 120</span>"));

    let command_path = script(
        &temp,
        "summary-no-usage.js",
        r#"return await command("rustc", ["--version"]);"#,
    );
    let command_state = engine(
        &temp.path().join("state-summary-no-usage"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&command_path, Value::Null, 1, 10)
    .expect("command workflow");
    let html =
        fs::read_to_string(command_state.run_summary.expect("summary")).expect("read summary");
    assert!(!html.contains("<span class=\"chip\">Tokens "));
}

#[test]
fn resume_replays_journal_lines_without_new_fields() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-old-journal-resume");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "old-journal-resume.js",
        r#"phase("collect"); const a = await agent("one"); const b = await agent("two"); return { a, b };"#,
    );
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    let journal_path = WorkflowStore::new(&root).journal_path(&state.run_id);
    let stripped = fs::read_to_string(&journal_path)
        .expect("read journal")
        .lines()
        .map(|line| {
            let mut value: Value = serde_json::from_str(line).expect("journal value");
            let object = value.as_object_mut().expect("journal object");
            object.remove("phase");
            object.remove("duration_ms");
            object.remove("usage");
            serde_json::to_string(&value).expect("serialize journal value")
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&journal_path, format!("{stripped}\n")).expect("rewrite old journal");
    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&state.run_id)
        .expect("resume completed run");
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(transport.count(), 2);
}

fn wait_for_active_run(root: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(entries) = fs::read_dir(root.join("runs")) {
            for entry in entries.flatten() {
                let run_id = entry.file_name().to_string_lossy().into_owned();
                if let Ok(state) = WorkflowStore::new(root).load_state(&run_id)
                    && !state.active.is_empty()
                {
                    return run_id;
                }
            }
        }
        assert!(Instant::now() < deadline, "run never became active");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_call_count(transport: &FakeTransport, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while transport.count() < expected {
        assert!(
            Instant::now() < deadline,
            "call count never reached {expected}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn recovers_fenced_json_from_model_prose() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-recover-json");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "recover-json.js",
        r#"
        return await agent("FENCED_JSON", {
          schema: {
            type: "object",
            required: ["summary", "score"],
            properties: {
              summary: { type: "string" },
              score: { type: "number" }
            }
          }
        });
    "#,
    );

    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("recover structured call");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"summary":"ok","score":1})));
    assert_eq!(transport.count(), 1);
}

#[test]
fn workflow_pause_propagates_and_resume_replays_child_tree() {
    let temp = TempDir::new().expect("tempdir");
    let root_dir = temp.path().join("state");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(300)));
    let _grandchild = script(
        &temp,
        "paused-grandchild.js",
        r#"
        return await agent("PAUSED_GRANDCHILD");
    "#,
    );
    let _child = script(
        &temp,
        "paused-child.js",
        r#"
        return await workflow("paused-grandchild.js");
    "#,
    );
    let root = script(
        &temp,
        "paused-root.js",
        r#"
        return await workflow("paused-child.js");
    "#,
    );
    let runner_root = root_dir.clone();
    let runner_transport = Arc::clone(&transport);
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&root, Value::Null, 1, 10)
    });
    wait_for_call_count(&transport, 1);
    let root_run_id = wait_for_root_run(&root_dir);

    let paused = engine(&root_dir, Arc::clone(&transport))
        .pause(&root_run_id)
        .expect("pause root");
    assert!(matches!(
        paused.status,
        RunStatus::Pausing | RunStatus::Paused
    ));
    let paused_root = runner.join().expect("join runner").expect("paused root");
    assert_eq!(
        paused_root.status,
        RunStatus::Paused,
        "{:?}",
        paused_root.error
    );

    let store = WorkflowStore::new(&root_dir);
    let child_id = store
        .child_run_ids(&root_run_id)
        .expect("child ids")
        .remove(0);
    let grandchild_id = store
        .child_run_ids(&child_id)
        .expect("grandchild ids")
        .remove(0);
    assert_eq!(
        store.load_state(&child_id).expect("child").status,
        RunStatus::Paused
    );
    assert_eq!(
        store.load_state(&grandchild_id).expect("grandchild").status,
        RunStatus::Paused
    );

    let resumed = engine(&root_dir, Arc::clone(&transport))
        .resume(&root_run_id)
        .expect("resume root");
    assert_eq!(resumed.status, RunStatus::Succeeded, "{:?}", resumed.error);
    assert_eq!(resumed.result, Some(json!("ok")));
    assert_eq!(
        transport.count(),
        1,
        "paused child call was submitted again"
    );
}

fn wait_for_root_run(root: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(entries) = fs::read_dir(root.join("runs"))
            && let Some(run_id) = entries.flatten().find_map(|entry| {
                let run_id = entry.file_name().to_string_lossy().into_owned();
                WorkflowStore::new(root)
                    .load_state(&run_id)
                    .ok()
                    .filter(|state| state.parent_run_id.is_none())
                    .map(|_| run_id)
            })
        {
            return run_id;
        }
        assert!(Instant::now() < deadline, "root run was not created");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn cancelling_root_propagates_to_active_child() {
    let temp = TempDir::new().expect("tempdir");
    let root_dir = temp.path().join("state");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(300)));
    let _child = script(
        &temp,
        "cancellable-child.js",
        r#"
        return await agent("LONG_CHILD");
    "#,
    );
    let root = script(
        &temp,
        "cancellable-root.js",
        r#"
        return await workflow("cancellable-child.js");
    "#,
    );
    let runner_root = root_dir.clone();
    let runner_transport = Arc::clone(&transport);
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&root, Value::Null, 1, 10)
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let root_run_id = loop {
        if let Ok(entries) = fs::read_dir(root_dir.join("runs"))
            && let Some(run_id) = entries.flatten().find_map(|entry| {
                let run_id = entry.file_name().to_string_lossy().into_owned();
                WorkflowStore::new(&root_dir)
                    .load_state(&run_id)
                    .ok()
                    .filter(|state| state.parent_run_id.is_none())
                    .map(|_| run_id)
            })
        {
            break run_id;
        }
        assert!(Instant::now() < deadline, "root run was not created");
        thread::sleep(Duration::from_millis(20));
    };
    wait_for_call_count(&transport, 1);

    let cancelled = engine(&root_dir, Arc::clone(&transport))
        .cancel(&root_run_id, "cancel child tree".to_owned())
        .expect("cancel root");
    assert!(matches!(
        cancelled.status,
        RunStatus::Cancelling | RunStatus::Cancelled
    ));
    let terminal = runner.join().expect("join runner").expect("runner result");
    assert_eq!(
        terminal.status,
        RunStatus::Cancelled,
        "{:?}",
        terminal.error
    );

    let children = WorkflowStore::new(&root_dir)
        .child_run_ids(&root_run_id)
        .expect("children");
    assert_eq!(children.len(), 1);
    let child = WorkflowStore::new(&root_dir)
        .load_state(&children[0])
        .expect("child state");
    assert_eq!(child.status, RunStatus::Cancelled);
}

#[test]
fn rejected_child_gate_replays_parent_policy() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let _grandchild = script(
        &temp,
        "rejected-gate-grandchild.js",
        r#"
        await gate("continue?", { label: "child gate" });
        return { unreachable: true };
    "#,
    );
    let _child = script(
        &temp,
        "rejected-gate-child.js",
        r#"
        try {
          await workflow("rejected-gate-grandchild.js");
          return { caught: false };
        } catch (error) {
          return { caught: String(error).includes("not approved") };
        }
    "#,
    );
    let root = script(
        &temp,
        "rejected-gate-root.js",
        r#"
        return await workflow("rejected-gate-child.js");
    "#,
    );
    let root_dir = temp.path().join("state");
    let engine = engine(&root_dir, transport);
    let waiting = engine
        .start(&root, Value::Null, 1, 10)
        .expect("reach bubbled gate");
    assert_eq!(waiting.status, RunStatus::WaitingHuman);

    let state = engine
        .approve(&waiting.run_id, false, "not approved".to_owned(), None)
        .expect("reject root gate");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"caught": true})));
}

#[test]
fn workflow_child_gate_bubbles_and_root_approval_resumes_tree() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let _grandchild = script(
        &temp,
        "gate-grandchild.js",
        r#"
        const decision = await gate("continue?", { label: "child gate" });
        return { approved: decision.approved };
    "#,
    );
    let _child = script(
        &temp,
        "gate-child.js",
        r#"
        return await workflow("gate-grandchild.js");
    "#,
    );
    let root = script(
        &temp,
        "gate-root.js",
        r#"
        return await workflow("gate-child.js");
    "#,
    );
    let root_dir = temp.path().join("state");
    let engine = engine(&root_dir, transport);
    let waiting = engine
        .start(&root, Value::Null, 1, 10)
        .expect("reach bubbled gate");
    assert_eq!(
        waiting.status,
        RunStatus::WaitingHuman,
        "{:?}",
        waiting.error
    );
    let gate = waiting.waiting_gate.expect("bubbled gate");
    let origin = gate.origin_run_id.expect("origin run");
    assert_ne!(origin, waiting.run_id);

    let state = engine
        .approve(&waiting.run_id, true, "approved".to_owned(), None)
        .expect("approve root gate");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"approved": true})));
}

#[test]
fn workflow_children_share_root_parallelism_limit() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(100)));
    let _child_a = script(
        &temp,
        "parallel-child-a.js",
        r#"
        return await agent("CHILD_A");
    "#,
    );
    let _child_b = script(
        &temp,
        "parallel-child-b.js",
        r#"
        return await agent("CHILD_B");
    "#,
    );
    let root = script(
        &temp,
        "parallel-root.js",
        r#"
        const results = await parallel([
          () => workflow("parallel-child-a.js"),
          () => workflow("parallel-child-b.js"),
        ]);
        return { results };
    "#,
    );
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&root, Value::Null, 1, 10)
        .expect("run nested parallel workflow");

    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"results":["ok", "ok"]})));
    assert_eq!(transport.count(), 2);
    assert_eq!(
        transport.peak_inspections(),
        1,
        "children bypassed the root maxParallel limit"
    );
}

#[test]
fn workflow_children_share_root_max_calls_limit() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(100)));
    let _first_child = script(
        &temp,
        "budget-child-a.js",
        r#"
        return await agent("BUDGET_CHILD_A");
    "#,
    );
    let _second_child = script(
        &temp,
        "budget-child-b.js",
        r#"
        return await agent("BUDGET_CHILD_B");
    "#,
    );
    let root = script(
        &temp,
        "budget-root.js",
        r#"
        const results = await parallel([
          () => workflow("budget-child-a.js"),
          () => workflow("budget-child-b.js"),
        ]);
        return { results };
    "#,
    );
    let root_dir = temp.path().join("state");
    let state = engine(&root_dir, Arc::clone(&transport))
        .start(&root, Value::Null, 2, 3)
        .expect("run shared budget workflow");

    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(transport.count(), 1, "children bypassed root maxCalls");
    let results = state.result.expect("results")["results"]
        .as_array()
        .expect("result array")
        .clone();
    assert_eq!(results.len(), 2);
    assert_eq!(results.iter().filter(|value| value.is_null()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|value| **value == json!("ok"))
            .count(),
        1
    );
    let ledger = WorkflowStore::new(&root_dir)
        .reconstruct_budget(&state.run_id)
        .expect("root ledger");
    assert_eq!(ledger.used_calls, 3);
}

#[test]
fn workflow_depth_limit_rejects_before_leaf_dispatch() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let names = (0..17)
        .map(|index| format!("depth-{index}.js"))
        .collect::<Vec<_>>();
    for (index, name) in names.iter().enumerate() {
        let body = if let Some(next) = names.get(index + 1) {
            format!(r#"return await workflow("{next}");"#)
        } else {
            "return await agent(\"DEPTH_LEAF\");".to_owned()
        };
        script(&temp, name, &body);
    }

    let root_dir = temp.path().join("state");
    let state = engine(&root_dir, Arc::clone(&transport))
        .start(&temp.path().join(&names[0]), Value::Null, 1, 100)
        .expect("run depth limit workflow");

    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .is_some_and(|error| error.contains("workflow nesting exceeds maximum depth 16")),
        "{:?}",
        state.error
    );
    assert_eq!(transport.count(), 0, "depth rejection dispatched leaf work");
}

#[test]
fn workflow_indirect_cycle_is_rejected_before_leaf_dispatch() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let _child = script(
        &temp,
        "indirect-cycle-child.js",
        r#"
        return await workflow("indirect-cycle-root.js");
    "#,
    );
    let root = script(
        &temp,
        "indirect-cycle-root.js",
        r#"
        return await workflow("indirect-cycle-child.js");
    "#,
    );

    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&root, Value::Null, 1, 10)
        .expect("run indirect cycle workflow");

    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .is_some_and(|error| error.contains("cycle detected before dispatch")),
        "{:?}",
        state.error
    );
    assert_eq!(transport.count(), 0);
}

#[test]
fn workflow_cycle_is_rejected_before_child_dispatch() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let root = script(
        &temp,
        "self-cycle.js",
        r#"
        await workflow("self-cycle.js");
        return { unreachable: true };
    "#,
    );
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&root, Value::Null, 1, 10)
        .expect("run cycle workflow");

    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .is_some_and(|error| error.contains("cycle detected before dispatch")),
        "{:?}",
        state.error
    );
    assert!(
        WorkflowStore::new(temp.path().join("state"))
            .child_run_ids(&state.run_id)
            .expect("children")
            .is_empty()
    );
    assert_eq!(transport.count(), 0);
}

#[test]
fn workflow_child_failure_is_catchable_by_parent_policy() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let _child = script(
        &temp,
        "failing-child.js",
        r#"
        throw new Error("expected child failure");
    "#,
    );
    let root = script(
        &temp,
        "failure-policy-root.js",
        r#"
        try {
          await workflow("failing-child.js");
          return { caught: false };
        } catch (error) {
          return { caught: String(error).includes("expected child failure") };
        }
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&root, Value::Null, 1, 10)
        .expect("run parent failure policy");

    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"caught": true})));
}

#[test]
fn workflow_child_tree_persists_identity_result_budget_and_shared_scheduler() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(50)));
    let _grandchild = script(
        &temp,
        "grandchild.js",
        r#"
        const value = await agent("GRANDCHILD");
        return { value };
    "#,
    );
    let _child = script(
        &temp,
        "child.js",
        r#"
        const child = await workflow("grandchild.js");
        return { child };
    "#,
    );
    let root = script(
        &temp,
        "root.js",
        r#"
        const result = await workflow("child.js");
        return { result };
    "#,
    );
    let root_dir = temp.path().join("state");
    let engine = engine(&root_dir, Arc::clone(&transport));
    let state = engine
        .start(&root, Value::Null, 1, 10)
        .expect("run nested workflow");

    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(
        state.result,
        Some(json!({"result":{"child":{"value":"ok"}}}))
    );
    assert_eq!(transport.count(), 1);

    let store = WorkflowStore::new(&root_dir);
    let root_children = store.child_run_ids(&state.run_id).expect("root children");
    assert_eq!(root_children.len(), 1);
    let child_state = store.load_state(&root_children[0]).expect("child state");
    assert_eq!(
        child_state.parent_run_id.as_deref(),
        Some(state.run_id.as_str())
    );
    assert_eq!(
        child_state.root_run_id.as_deref(),
        Some(state.run_id.as_str())
    );
    assert!(child_state.parent_call_key.is_some());

    let grandchildren = store
        .child_run_ids(&child_state.run_id)
        .expect("grandchildren");
    assert_eq!(grandchildren.len(), 1);
    let grandchild_state = store
        .load_state(&grandchildren[0])
        .expect("grandchild state");
    assert_eq!(
        grandchild_state.parent_run_id.as_deref(),
        Some(child_state.run_id.as_str())
    );
    assert_eq!(
        grandchild_state.root_run_id.as_deref(),
        Some(state.run_id.as_str())
    );
    assert!(grandchild_state.parent_call_key.is_some());
    assert_eq!(grandchild_state.status, RunStatus::Succeeded);

    let ledger = store
        .reconstruct_budget(&state.run_id)
        .expect("root ledger");
    assert_eq!(ledger.used_calls, 3);
    let child_ledger_key = ledger
        .reservations
        .keys()
        .find(|key| key.starts_with(&format!("{}:", child_state.run_id)))
        .expect("child workflow reservation");
    assert!(ledger.reservations[child_ledger_key].settled);
    let grandchild_ledger_key = ledger
        .reservations
        .keys()
        .find(|key| key.starts_with(&format!("{}:", grandchild_state.run_id)))
        .expect("grandchild agent reservation");
    assert!(ledger.reservations[grandchild_ledger_key].settled);

    let root_journal = store.journal_index(&state.run_id).expect("root journal");
    assert!(root_journal.values().any(|entry| {
        entry.kind == CallKind::Workflow
            && entry.child_run_id.as_deref() == Some(child_state.run_id.as_str())
            && entry.state == CallState::Succeeded
    }));
    let child_journal = store
        .journal_index(&child_state.run_id)
        .expect("child journal");
    assert!(child_journal.values().any(|entry| {
        entry.kind == CallKind::Workflow
            && entry.child_run_id.as_deref() == Some(grandchild_state.run_id.as_str())
            && entry.state == CallState::Succeeded
    }));
}

#[test]
fn supersede_marks_terminal_and_records_contract() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "supersede.js",
        r#"
        phase("work");
        const r = await command("rustc", ["--version"], { timeoutSeconds: 30 });
        if (r.exitCode === 0) {
          await supersede({
            reason: "evidence shows direction changed",
            evidence: "results.json",
            newContract: "use non-oracle gold",
          });
        }
        return { unreachable: true };
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("supersede workflow");
    assert_eq!(state.status, RunStatus::Superseded);
    let info = state.supersede.expect("supersede info");
    assert_eq!(info.reason, "evidence shows direction changed");
    assert_eq!(info.evidence.as_deref(), Some("results.json"));
    assert_eq!(info.new_contract.as_deref(), Some("use non-oracle gold"));
}

#[test]
fn supersede_cli_records_state() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "supersede-cli.js",
        r#"
        phase("work");
        await new Promise(() => {});
    "#,
    );
    let engine = engine(&temp.path().join("state"), transport);
    let state = engine.start(&path, Value::Null, 1, 10).expect("start");
    let after = engine
        .supersede(
            &state.run_id,
            "manual redirect".to_owned(),
            Some("ev.md".to_owned()),
            None,
        )
        .expect("supersede");
    assert_eq!(after.status, RunStatus::Superseded);
    let info = after.supersede.expect("info");
    assert_eq!(info.reason, "manual redirect");
    assert_eq!(info.evidence.as_deref(), Some("ev.md"));
}

#[test]
fn supersede_wins_over_late_ok_return() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "supersede-late-ok.js",
        r#"
        phase("work");
        try {
          await supersede({ reason: "redirect" });
        } catch (_) {}
        return { late: "ok" };
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Superseded);
    assert!(state.supersede.is_some());
}

#[test]
fn retry_succeeds_after_transient_failure() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    // attempt 1 fails, attempt 2 succeeds (rustc --version always succeeds,
    // so drive failure via a bad flag on attempt 1 only)
    let path = script(
        &temp,
        "retry.js",
        r#"
        const r = await retry(async (attempt) => {
          if (attempt === 1) {
            const bad = await command("rustc", ["--nope-xyz"], { timeoutSeconds: 30 });
            if (bad.exitCode !== 0) { throw new Error("transient: " + bad.exitCode); }
          }
          return await command("rustc", ["--version"], { timeoutSeconds: 30 });
        }, { maxAttempts: 3, delayMs: 1 });
        return { exitCode: r.exitCode };
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("retry workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result, Some(json!({"exitCode": 0})));
}

#[test]
fn retry_fail_fast_on_non_retryable() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "retry-nr.js",
        r#"
        let calls = 0;
        try {
          await retry(async () => {
            calls++;
            throw new Error("validation: bad input");
          }, { maxAttempts: 3, delayMs: 1, nonRetryable: ["validation"] });
        } catch (e) {
          return { calls, msg: String(e) };
        }
        return { calls, msg: "no-throw" };
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded);
    // fail-fast: only 1 attempt despite maxAttempts=3
    assert_eq!(state.result.as_ref().unwrap()["calls"], json!(1));
}

#[test]
fn gate_returns_injected_value() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "gate-value.js",
        r#"
        const fixed = await gate("give the correct contractPath", {
          expect: "value",
          current: { contractPath: "old.md" },
          hint: "should be under surveys/",
        });
        return { path: fixed.value ? fixed.value.contractPath : fixed.contractPath };
    "#,
    );
    let engine = engine(&temp.path().join("state"), transport);
    let waiting = engine.start(&path, Value::Null, 1, 10).expect("start");
    assert_eq!(waiting.status, RunStatus::WaitingHuman);
    let gate_req = waiting.waiting_gate.expect("gate");
    assert_eq!(gate_req.expect.as_deref(), Some("value"));
    // inject corrected value via approve --value channel
    let resumed = engine
        .approve(
            &waiting.run_id,
            true,
            "corrected".to_owned(),
            Some(json!({"contractPath": "surveys/new.md"})),
        )
        .expect("approve with value");
    assert_eq!(resumed.status, RunStatus::Succeeded, "{:?}", resumed.error);
    assert_eq!(resumed.result, Some(json!({"path": "surveys/new.md"})));
}

#[test]
fn resume_does_not_rerun_superseded_run() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "supersede-resume.js",
        r#"
        phase("work");
        await supersede({ reason: "redirect", newContract: "next.md" });
        return { unreachable: true };
    "#,
    );
    let engine = engine(&temp.path().join("state"), transport);
    let state = engine.start(&path, Value::Null, 1, 10).expect("start");
    assert_eq!(state.status, RunStatus::Superseded);
    let before = state.resume_count;
    let resumed = engine.resume(&state.run_id).expect("resume superseded");
    assert_eq!(resumed.status, RunStatus::Superseded);
    assert_eq!(
        resumed.resume_count, before,
        "resume must not re-execute superseded runs"
    );
}

#[test]
fn max_calls_budget_persists_across_resume() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    // max_calls=1: first agent consumes budget; second must fail.
    let path = script(
        &temp,
        "max-calls.js",
        r#"
        await agent("ONE");
        await agent("TWO");
        return { ok: true };
    "#,
    );
    let engine = engine(&temp.path().join("state"), Arc::clone(&transport));
    let failed = engine
        .start(&path, Value::Null, 1, 1)
        .expect("start with tiny max_calls");
    assert_eq!(failed.status, RunStatus::Failed, "{:?}", failed.error);
    assert!(
        failed.error.as_deref().unwrap_or("").contains("max_calls"),
        "{:?}",
        failed.error
    );
    // Resume must not grant a fresh budget of 1 that lets TWO run.
    let resumed = engine.resume(&failed.run_id).expect("resume");
    assert_eq!(resumed.status, RunStatus::Failed, "{:?}", resumed.error);
    assert_eq!(transport.count(), 1, "second agent must not be submitted");
}

#[test]
fn negotiate_two_body_reaches_accept_after_revise() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let path = temp.path().join("negotiate.js");
    fs::write(
        &path,
        fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/negotiate-2body.workflow.js"),
        )
        .expect("read example"),
    )
    .expect("copy example");
    let state = engine(&temp.path().join("state-negotiate"), Arc::clone(&transport))
        .start(
            &path,
            json!({"topic":"local canary","maxRounds":2,"timeoutSeconds":30}),
            2,
            20,
        )
        .expect("run negotiation");
    assert_eq!(state.status, RunStatus::Succeeded);
    let result = state.result.expect("result");
    assert_eq!(result["protocol"], "negotiate-2body");
    assert_eq!(result["stopReason"], "reviewer_accept");
    assert_eq!(result["rounds"], 2);
    assert_eq!(result["decision"]["accepted"], true);
    assert_eq!(result["decision"]["decision"], "use canary A fixed");
    assert_eq!(transport.count(), 5, "2 propose + 2 review + 1 synth");
}

#[test]
fn date_now_fails_fast_with_actionable_error() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "date-now.js",
        r#"
        const t = Date.now();
        await agent("SHOULD_NOT_RUN");
        return { t };
    "#,
    );
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal state");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Date.now() is nondeterministic and breaks journal-replay resume"),
        "{:?}",
        state.error
    );
    assert_eq!(
        transport.count(),
        0,
        "banned call must throw before any paid submission"
    );
}

#[test]
fn math_random_fails_fast() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(&temp, "math-random.js", "return { r: Math.random() };\n");
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("terminal state");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Math.random() is nondeterministic and breaks journal-replay resume"),
        "{:?}",
        state.error
    );
}

#[test]
fn argless_new_date_throws_and_explicit_date_survives() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "date-guard.js",
        r#"
        let caughtArgless = "";
        try { new Date(); } catch (e) { caughtArgless = String(e); }
        let caughtFn = "";
        try { Date(0); } catch (e) { caughtFn = String(e); }
        let caughtCtor = "";
        try { new (new Date(0)).constructor(); } catch (e) { caughtCtor = String(e); }
        return {
          caughtArgless,
          caughtFn,
          caughtCtor,
          iso: new Date(1700000000000).toISOString(),
          parsed: Date.parse("2020-01-02T03:04:05Z"),
          utc: Date.UTC(2020, 0, 2),
          inst: (new Date(0) instanceof Date),
        };
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let result = state.result.expect("result");
    assert!(
        result["caughtArgless"]
            .as_str()
            .unwrap_or_default()
            .contains("new Date() without arguments"),
        "{:?}",
        result["caughtArgless"]
    );
    assert!(
        result["caughtFn"]
            .as_str()
            .unwrap_or_default()
            .contains("Date() called as a function"),
        "{:?}",
        result["caughtFn"]
    );
    assert!(
        result["caughtCtor"]
            .as_str()
            .unwrap_or_default()
            .contains("without arguments"),
        "constructor escape must stay closed: {:?}",
        result["caughtCtor"]
    );
    assert_eq!(result["iso"], "2023-11-14T22:13:20.000Z");
    assert_eq!(result["parsed"], json!(1577934245000i64));
    assert_eq!(result["utc"], json!(1577923200000i64));
    assert_eq!(result["inst"], true);
}

#[test]
fn capability_routing_preserves_pinned_provider_and_model() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = temp.path().join("pinned-capability.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "pinned capability",
          contract: "workflow",
          capabilities: {
            providers: [
              { agent: "claude", model: "claude-opus-5", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 200000 },
              { agent: "pi", model: "fallback", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 200000 }
            ],
            roles: { maker: { requires: ["reasoning"], effort: "high", contextTokens: 100000 } }
          }
        };
        return await agent("pinned", { agent: "claude", model: "claude-opus-5", role: "maker" });"#,
    )
    .expect("write script");
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let requests = transport.requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].agent, "claude");
    assert_eq!(requests[0].model.as_deref(), Some("claude-opus-5"));
    drop(requests);
    let events = WorkflowStore::new(temp.path().join("state"))
        .read_capability_events(&state.run_id)
        .expect("capability events");
    assert!(matches!(
        events.first().map(|event| &event.event),
        Some(CapabilityEvent::Declared { .. })
    ));
    assert!(
        matches!(events.get(1).map(|event| &event.event), Some(CapabilityEvent::Selected { requested: Some(requested), chosen, .. }) if requested.agent == "claude" && requested.model.as_deref() == Some("claude-opus-5") && chosen == requested)
    );
}

#[test]
fn missing_capability_fails_before_transport_submission() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = temp.path().join("missing-capability.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "missing capability",
          contract: "workflow",
          capabilities: {
            providers: [{ agent: "pi", capabilities: ["text"], maxEffort: "medium", contextTokens: 32000 }],
            roles: { reviewer: { requires: ["vision"], effort: "high", contextTokens: 64000 } }
          }
        };
        return await agent("must not submit", { role: "reviewer" });"#,
    )
    .expect("write script");
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal state");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("missing capability for call")
    );
    assert_eq!(
        transport.count(),
        0,
        "routing failure must precede transport"
    );
}

#[test]
fn automatic_capability_degradation_records_exclusions() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = temp.path().join("degraded-capability.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "degraded capability",
          contract: "workflow",
          capabilities: {
            providers: [
              { agent: "pi", model: "small", capabilities: ["text"], maxEffort: "low", contextTokens: 16000 },
              { agent: "claude", model: "claude-sonnet-5", capabilities: ["text"], maxEffort: "high", contextTokens: 200000 }
            ],
            roles: { analyst: { requires: ["text"], effort: "high", contextTokens: 100000 } }
          }
        };
        return await agent("route automatically", { role: "analyst" });"#,
    )
    .expect("write script");
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let events = WorkflowStore::new(temp.path().join("state"))
        .read_capability_events(&state.run_id)
        .expect("capability events");
    assert!(
        events.iter().any(|event| {
            matches!(&event.event, CapabilityEvent::Selected { requested: None, chosen, excluded, degradation: Some(message), .. } if chosen.agent == "claude" && chosen.model.as_deref() == Some("claude-sonnet-5") && excluded.len() == 1 && message.contains("claude/claude-sonnet-5"))
        })
    );
}

#[test]
fn independent_roles_cannot_share_the_same_model_choice() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = temp.path().join("independent-capability.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "independent capability",
          contract: "workflow",
          capabilities: {
            providers: [{ agent: "claude", model: "claude-opus-5", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 200000 }],
            roles: {
              maker: { requires: ["reasoning"] },
              reviewer: { requires: ["reasoning"], independentFrom: ["maker"] }
            }
          }
        };
        const maker = await agent("make", { role: "maker" });
        return await agent("review", { role: "reviewer" });"#,
    )
    .expect("write script");
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("terminal state");
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("must be independent")
    );
    assert_eq!(
        transport.count(),
        1,
        "reviewer must be rejected before submission"
    );
    let events = WorkflowStore::new(temp.path().join("state"))
        .read_capability_events(&state.run_id)
        .expect("capability events");
    assert!(
        matches!(events.last().map(|event| &event.event), Some(CapabilityEvent::IndependenceViolation { role, conflict_role, .. }) if role == "reviewer" && conflict_role == "maker")
    );
    let resumed = engine(&temp.path().join("state"), Arc::clone(&transport))
        .resume(&state.run_id)
        .expect("resume failed run");
    assert_eq!(resumed.status, RunStatus::Failed);
    assert_eq!(transport.count(), 1, "resume must not submit reviewer");
    let resumed_events = WorkflowStore::new(temp.path().join("state"))
        .read_capability_events(&state.run_id)
        .expect("capability events after resume");
    assert_eq!(
        resumed_events.len(),
        events.len(),
        "resume must not duplicate routing evidence"
    );
}

#[test]
fn retry_wall_time_bookkeeping_works_under_guard() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let path = script(
        &temp,
        "retry-wall.js",
        r#"
        try {
          await retry(async () => { throw new Error("boom"); },
            { maxAttempts: 5, delayMs: 1, wallTimeSeconds: 0 });
        } catch (e) {
          return { msg: String(e) };
        }
        return { msg: "no-throw" };
    "#,
    );
    let state = engine(&temp.path().join("state"), transport)
        .start(&path, Value::Null, 1, 10)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    // Proves the reworked clock path: a broken rework would surface the
    // Date.now guard TypeError here instead of the wall-time message.
    assert!(
        state.result.as_ref().unwrap()["msg"]
            .as_str()
            .unwrap_or_default()
            .contains("retry wall-time exceeded"),
        "{:?}",
        state.result
    );
}

#[test]
fn check_still_passes_scripts_that_textually_mention_banned_apis() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let eng = engine(&temp.path().join("state-check-banned"), transport);
    let path = script(
        &temp,
        "check-banned.js",
        "const t = Date.now();\nreturn { t };\n",
    );
    let value = eng
        .check(&path)
        .expect("guard is runtime-only; parse preflight must stay unaffected");
    assert_eq!(value["check"], "ok");
}

fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_servitor-workflows"))
        .args(args)
        .env("SERVITOR_WORKFLOWS_STATE_ROOT", root)
        .output()
        .expect("run workflow CLI")
}

fn cli_json(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("CLI JSON envelope")
}

#[test]
fn detached_run_reuses_one_record_and_waits_for_success() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-detach");
    let path = script(
        &temp,
        "detached.js",
        r#"const delayed = await command("pwsh", ["-NoProfile", "-Command", "Start-Sleep -Milliseconds 800"], { timeoutSeconds: 10 });
        return { ok: delayed.exitCode === 0 };"#,
    );
    let started = Instant::now();
    let mut process = Command::new(env!("CARGO_BIN_EXE_servitor-workflows"))
        .args(["run", path.to_str().expect("workflow path"), "--detach"])
        .env("SERVITOR_WORKFLOWS_STATE_ROOT", &root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn detach probe");
    let deadline = Instant::now() + Duration::from_secs(5);
    while process.try_wait().expect("poll detach probe").is_none() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        process.try_wait().expect("poll detach probe").is_some(),
        "detach parent did not exit promptly"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "detach took {:?}",
        started.elapsed()
    );
    let detached = cli(&root, &["list", "--limit", "1"]);
    let listed = cli_json(&detached);
    let run_id = listed["data"]["runs"][0]["run_id"]
        .as_str()
        .expect("run id");
    assert!(
        listed["data"]["runs"][0]["journal_path"]
            .as_str()
            .is_some_and(|path| path.ends_with("journal.jsonl"))
    );
    let waited = cli(&root, &["get", run_id, "--wait", "--timeout-seconds", "10"]);
    assert!(waited.status.success(), "{:?}", waited);
    assert_eq!(cli_json(&waited)["data"]["status"], "succeeded");
    assert!(root.join("runs").join(run_id).join("state.json").is_file());
    assert!(
        root.join("runs")
            .join(run_id)
            .join("run-summary.html")
            .is_file()
    );
    assert_eq!(
        fs::read_dir(root.join("runs")).expect("runs dir").count(),
        1,
        "detached child must not create a second run"
    );
}

#[test]
fn wait_reports_human_failure_and_timeout_contracts() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-wait");

    let gate_path = script(&temp, "gate-wait.js", "return await gate(\"ship?\");");
    let gate_run = cli(&root, &["run", gate_path.to_str().unwrap(), "--detach"]);
    let gate_id = cli_json(&gate_run)["data"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let waiting = cli(
        &root,
        &["get", &gate_id, "--wait", "--timeout-seconds", "10"],
    );
    assert_eq!(waiting.status.code(), Some(3));
    assert_eq!(cli_json(&waiting)["error"]["code"], "waiting_human");

    let failed_path = script(
        &temp,
        "failed-wait.js",
        "throw new Error(\"expected failure\");",
    );
    let failed_run = cli(&root, &["run", failed_path.to_str().unwrap(), "--detach"]);
    let failed_id = cli_json(&failed_run)["data"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let failed = cli(
        &root,
        &["get", &failed_id, "--wait", "--timeout-seconds", "10"],
    );
    assert_eq!(failed.status.code(), Some(1));
    let failed_json = cli_json(&failed);
    assert_eq!(failed_json["error"]["code"], "terminal_failed");
    assert!(
        failed_json["error"]["remediation"]
            .as_str()
            .is_some_and(|text| text.contains(&format!("servitor-workflows resume {failed_id}")))
    );
    assert!(failed_json["data"]["journal_path"].is_string());

    let slow_path = script(
        &temp,
        "timeout-wait.js",
        r#"await command("pwsh", ["-NoProfile", "-Command", "Start-Sleep -Seconds 3"], { timeoutSeconds: 10 }); return true;"#,
    );
    let slow_root = temp.path().join("state-timeout");
    let slow_engine = engine(&slow_root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let prepared = slow_engine
        .prepare(&slow_path, Value::Null, 1, 10)
        .expect("prepare running run");
    let timed_out = cli(
        &slow_root,
        &["get", &prepared.run_id, "--wait", "--timeout-seconds", "0"],
    );
    assert_eq!(timed_out.status.code(), Some(4));
    assert_eq!(cli_json(&timed_out)["error"]["code"], "wait_timeout");
}

#[test]
fn check_rejects_syntax_errors_and_module_only_syntax_without_creating_runs() {
    use servitor_workflows::WorkflowError;
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let root = temp.path().join("state-check");
    let eng = engine(&root, Arc::clone(&transport));

    let good = script(&temp, "good.js", "return { done: true };\n");
    let value = eng.check(&good).expect("good script parses");
    assert_eq!(value["check"], "ok");

    for (name, body) in [
        ("assign.js", "1 = 2;\nreturn {};\n"),
        ("importmeta.js", "let x = import.meta.path;\nreturn {};\n"),
        ("decl.js", "const a\n  b = 1;\nreturn {};\n"),
    ] {
        let bad = script(&temp, name, body);
        let err = eng.check(&bad).expect_err("bad script must fail check");
        assert!(
            matches!(err, WorkflowError::InvalidWorkflow(_)),
            "{name}: expected InvalidWorkflow, got {err:?}"
        );
        let err = eng
            .start(&bad, Value::Null, 1, 10)
            .expect_err("start must refuse unparseable script");
        assert!(matches!(err, WorkflowError::InvalidWorkflow(_)));
    }
    let runs = root.join("runs");
    let leftover = std::fs::read_dir(&runs).map(|it| it.count()).unwrap_or(0);
    assert_eq!(
        leftover, 0,
        "refused scripts must not leave run directories"
    );
}

#[test]
fn watch_reconstructs_tree_and_critical_path_from_persisted_events() {
    use servitor_workflows::reconstruct_watch;
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let _grandchild = script(
        &temp,
        "watch-grandchild.js",
        r#"
        const decision = await gate("continue?", { label: "child gate" });
        return { approved: decision.approved };
    "#,
    );
    let _child = script(
        &temp,
        "watch-child.js",
        r#"
        return await workflow("watch-grandchild.js");
    "#,
    );
    let root = script(
        &temp,
        "watch-root.js",
        r#"
        return await workflow("watch-child.js");
    "#,
    );
    let root_dir = temp.path().join("state");
    let eng = engine(&root_dir, Arc::clone(&transport));
    let waiting = eng
        .start(&root, Value::Null, 1, 10)
        .expect("reach bubbled gate");
    assert_eq!(
        waiting.status,
        RunStatus::WaitingHuman,
        "{:?}",
        waiting.error
    );
    let root_run_id = waiting.run_id.clone();

    // A brand-new store over the same root simulates a killed-and-restarted
    // CLI process: no in-memory runtime state survives, only persisted events.
    let restarted = WorkflowStore::new(&root_dir);
    let view = reconstruct_watch(&restarted, &root_run_id).expect("watch reconstructs");

    assert_eq!(view.source, "persisted_events");
    assert_eq!(view.status, RunStatus::WaitingHuman);
    assert_eq!(view.root_run_id, root_run_id);

    // Tree: root -> child -> grandchild, each reconstructed with a category.
    // A bubbled gate leaves every ancestor in WaitingHuman (the gate request
    // is recorded on each, with origin pointing at the grandchild).
    assert_eq!(view.tree.run_id, root_run_id);
    assert_eq!(view.tree.category, "waiting_human");
    assert_eq!(view.tree.children.len(), 1, "root has one child workflow");
    let child = &view.tree.children[0];
    assert_eq!(child.status, RunStatus::WaitingHuman);
    assert_eq!(child.category, "waiting_human");
    assert_eq!(child.children.len(), 1, "child has one grandchild workflow");
    let grandchild = &child.children[0];
    assert_eq!(grandchild.status, RunStatus::WaitingHuman);
    assert_eq!(grandchild.category, "waiting_human");

    // Critical path descends to the deepest non-terminal branch (the gate).
    assert_eq!(view.critical_path.len(), 3);
    assert_eq!(view.critical_path[0], root_run_id);
    assert_eq!(view.critical_path[2], grandchild.run_id);

    // Waiting categories surface every blocked node; the gate decision is
    // routed to its origin (the grandchild), not the bubbled ancestors.
    assert!(
        view.waiting
            .iter()
            .any(|entry| entry.category == "waiting_human" && entry.run_id == grandchild.run_id),
        "gate node must be categorized waiting_human: {:?}",
        view.waiting
    );
    let recovery = view
        .recovery
        .iter()
        .find(|step| step.command.contains("approve"))
        .expect("gate recovery step");
    assert!(
        recovery.command.contains(&grandchild.run_id),
        "approval must target the gate origin: {}",
        recovery.command
    );
}

#[test]
fn watch_after_restart_matches_live_reconstruction_and_jsonl_output() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let _grandchild = script(
        &temp,
        "watch2-grandchild.js",
        r#"
        const decision = await gate("continue?", { label: "child gate" });
        return { approved: decision.approved };
    "#,
    );
    let _child = script(
        &temp,
        "watch2-child.js",
        r#"
        return await workflow("watch2-grandchild.js");
    "#,
    );
    let root = script(
        &temp,
        "watch2-root.js",
        r#"
        return await workflow("watch2-child.js");
    "#,
    );
    let root_dir = temp.path().join("state");
    let eng = engine(&root_dir, Arc::clone(&transport));
    let waiting = eng
        .start(&root, Value::Null, 1, 10)
        .expect("reach bubbled gate");
    let root_run_id = waiting.run_id.clone();

    // Live in-process reconstruction.
    let live =
        servitor_workflows::reconstruct_watch(eng.store(), &root_run_id).expect("live watch");

    // Restarted-process reconstruction via the real CLI binary over the same
    // state root: this is the acceptance test — a fresh process rebuilds the
    // identical tree and critical path exclusively from persisted events.
    let output = cli(&root_dir, &["watch", &root_run_id]);
    assert!(output.status.success(), "watch CLI failed");
    let restarted = cli_json(&output)["data"].clone();

    let live_value = serde_json::to_value(&live).expect("serialize live view");
    assert_eq!(
        restarted["tree"], live_value["tree"],
        "restarted CLI tree must equal live reconstruction"
    );
    assert_eq!(
        restarted["criticalPath"], live_value["criticalPath"],
        "restarted CLI critical path must equal live reconstruction"
    );
    assert_eq!(restarted["source"], "persisted_events");

    // JSONL output streams one envelope per line; the schema contract (ok/meta
    // present) holds even in jsonl mode, and `.data` carries the view.
    let jsonl = cli(&root_dir, &["--output", "jsonl", "watch", &root_run_id]);
    assert!(jsonl.status.success(), "watch --output jsonl failed");
    let lines: Vec<Value> = String::from_utf8_lossy(&jsonl.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line"))
        .collect();
    assert_eq!(lines.len(), 1, "watch emits exactly one JSONL record");
    assert_eq!(
        lines[0]["ok"], true,
        "jsonl record keeps the envelope contract"
    );
    assert_eq!(lines[0]["data"]["tree"], live_value["tree"]);
    assert_eq!(lines[0]["data"]["criticalPath"], live_value["criticalPath"]);
}

#[test]
fn watch_reports_failed_recovery_and_budget_usage() {
    use servitor_workflows::reconstruct_watch;
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let root = script(
        &temp,
        "watch-fail.js",
        r#"
        await agent("WATCH_FAIL");
        throw new Error("boom");
    "#,
    );
    let root_dir = temp.path().join("state");
    let eng = engine(&root_dir, Arc::clone(&transport));
    let failed = eng
        .start(&root, Value::Null, 1, 10)
        .expect("start returns state");
    assert_eq!(failed.status, RunStatus::Failed, "{:?}", failed.error);

    // The run record persists even though execution failed; a restarted
    // process finds it by listing the state root.
    let restarted = WorkflowStore::new(&root_dir);
    let ids = restarted.list_run_ids().expect("list runs");
    assert_eq!(ids.len(), 1);
    let view = reconstruct_watch(&restarted, &ids[0]).expect("watch failed run");

    assert_eq!(view.status, RunStatus::Failed);
    assert_eq!(view.tree.category, "failed");
    assert!(
        view.recovery
            .iter()
            .any(|step| step.command.contains("resume") && step.run_id == ids[0]),
        "failed run must offer a resume recovery step: {:?}",
        view.recovery
    );
    // Budget/usage is reconstructed from budget.jsonl; the agent call settled.
    let budget = view.budget.expect("run has a budget ledger");
    assert!(budget.attributed_tokens > 0, "usage tokens attributed");
}

#[test]
fn watch_reports_true_status_for_legacy_run_without_event_stream() {
    use servitor_workflows::reconstruct_watch;
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-legacy-watch");
    let store = WorkflowStore::new(&root);
    let path = temp.path().join("legacy-watch.js");
    fs::write(&path, "return true;").expect("write legacy script");
    // A legacy run has no events.jsonl; watch must not silently report "running".
    let state = legacy_state(&store, &path, RunStatus::Cancelled);
    store
        .create_run(&state, "return true;")
        .expect("persist legacy run");

    let view = reconstruct_watch(&store, &state.run_id).expect("watch legacy run");
    assert_eq!(
        view.status,
        RunStatus::Cancelled,
        "legacy watch must reflect the persisted status, not a hardcoded running"
    );
    assert_eq!(view.tree.category, "cancelled");
    assert!(view.budget.is_none(), "legacy run has no budget ledger");
}

#[test]
fn watch_rejects_corrupted_parent_child_cycle_instead_of_crashing() {
    use servitor_workflows::reconstruct_watch;
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-cycle-watch");
    let store = WorkflowStore::new(&root);
    let path = temp.path().join("cycle.js");
    fs::write(&path, "return true;").expect("write script");

    // Hand-corrupt two records into a parent/child cycle. The engine can never
    // produce this (depth + digest checks), but persisted state is the trust
    // boundary for watch, so it must error, not overflow the stack.
    let mut a = legacy_state(&store, &path, RunStatus::Running);
    a.run_id = "cycle-a".to_owned();
    a.parent_run_id = Some("cycle-b".to_owned());
    a.journal_path = store.journal_path("cycle-a");
    let mut b = legacy_state(&store, &path, RunStatus::Running);
    b.run_id = "cycle-b".to_owned();
    b.parent_run_id = Some("cycle-a".to_owned());
    b.journal_path = store.journal_path("cycle-b");
    store
        .create_run(&a, "return true;")
        .expect("persist cycle-a");
    store
        .create_run(&b, "return true;")
        .expect("persist cycle-b");

    let err = reconstruct_watch(&store, "cycle-a").expect_err("cycle must be rejected");
    assert!(
        matches!(err, servitor_workflows::WorkflowError::Invariant(_)),
        "expected Invariant cycle error, got {err:?}"
    );
}

#[test]
fn goalchain_delivery_completes_dual_gates_child_review_cost_and_boundary() {
    use servitor_workflows::reconstruct_watch;
    let temp = TempDir::new().expect("tempdir");
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    // Copy the migrated chain + its independent reviewer child into the temp
    // cwd so the relative `workflow("goalchain-review.workflow.js")` resolves.
    fs::copy(
        examples.join("goalchain.workflow.js"),
        temp.path().join("goalchain.workflow.js"),
    )
    .expect("copy chain");
    fs::copy(
        examples.join("goalchain-review.workflow.js"),
        temp.path().join("goalchain-review.workflow.js"),
    )
    .expect("copy reviewer child");
    // Frozen contract carrying the mechanical acceptance identifiers (G15).
    fs::write(
        temp.path().join("contract.md"),
        "# contract\n\nAcceptance identifiers: dual-gate, boundary-audit.\n",
    )
    .expect("write contract");

    let root_dir = temp.path().join("state");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let eng = engine(&root_dir, Arc::clone(&transport));
    let waiting = eng
        .start(
            &temp.path().join("goalchain.workflow.js"),
            json!({
                "contractPath": "contract.md",
                "evidencePath": "./out/evidence.json",
                "mechanicalTokens": ["dual-gate", "boundary-audit"],
                "requireHumanGate": true
            }),
            1,
            20,
        )
        .expect("start goalchain");
    // readiness -> dispatch -> child review -> mechanical gate -> human gate.
    assert_eq!(
        waiting.status,
        RunStatus::WaitingHuman,
        "{:?}",
        waiting.error
    );
    let root_run_id = waiting.run_id.clone();

    // Human acceptance resumes the chain through the semantic gate + writeback.
    let done = eng
        .approve(&root_run_id, true, "accept".to_owned(), None)
        .expect("approve human gate");
    assert_eq!(done.status, RunStatus::Succeeded, "{:?}", done.error);

    // Dual gates: the mechanical gate (identifier tokens read back from disk)
    // and the semantic gate (independent meaning review) both passed.
    let result = done.result.clone().expect("result");
    assert_eq!(result["protocol"], "goalchain");
    assert_eq!(result["semantic"]["approved"], true);
    assert_eq!(result["review"]["verdict"], "approve");
    assert_eq!(result["human"]["approved"], true);
    // The mechanical gate (identifier tokens read back from disk) is surfaced
    // in the result so its pass is observable, not just an unasserted side
    // effect. The negative-control test below proves it can also fail a chain.
    assert_eq!(result["mechanical"]["passed"], true);
    assert_eq!(
        result["mechanical"]["tokens"],
        json!(["dual-gate", "boundary-audit"])
    );

    // Child review (G13): the independent reviewer ran as its own run with its
    // own journal — never self-review in the parent run.
    let store = WorkflowStore::new(&root_dir);
    let children = store.child_run_ids(&root_run_id).expect("children");
    assert_eq!(
        children.len(),
        1,
        "exactly one child workflow (the reviewer)"
    );
    let child = store.load_state(&children[0]).expect("child state");
    assert_eq!(child.parent_run_id.as_deref(), Some(root_run_id.as_str()));
    assert_eq!(child.root_run_id.as_deref(), Some(root_run_id.as_str()));
    assert_eq!(child.status, RunStatus::Succeeded);
    // The reviewer inherited the parent capability policy and used the
    // independent `reviewer` role.
    assert!(
        store
            .read_capability_events(&children[0])
            .expect("child capability events")
            .iter()
            .any(|event| matches!(&event.event, CapabilityEvent::Selected { role, .. } if role.as_deref() == Some("reviewer"))),
        "reviewer child must resolve the independent reviewer role"
    );
    // The reviewer boundary is read-only: writePaths narrowed to empty.
    assert!(
        child
            .boundary
            .as_ref()
            .expect("child boundary")
            .write_paths
            .is_empty(),
        "reviewer must be read-only"
    );

    // Cost attribution: worker + reviewer + semantic = 3 agent calls,
    // all attributed to the shared root ledger with tokens settled. The ledger
    // also counts command/gate/workflow host calls, so assert the agent
    // Cost attribution: the shared root ledger counts EVERY host call,
    // not just agents — 3 agents (worker + reviewer-child + semantic) + 3
    // commands (readiness + write-evidence + read-evidence) + 1 gate (human) +
    // 1 workflow (review child) = 8 settled keys. Pin the exact count so a
    // regression that stops counting command/gate/workflow calls is caught.
    let ledger = store.reconstruct_budget(&root_run_id).expect("root ledger");
    assert_eq!(
        ledger.used_calls, 8,
        "ledger counts all 8 host calls (3 agents + 3 commands + gate + workflow)"
    );
    assert!(ledger.attributed_tokens > 0, "usage tokens attributed");
    assert_eq!(transport.count(), 3, "three transport submissions total");
    // The child reviewer's agent settled against the shared root ledger under a
    // child-prefixed reservation key — proof the child shares the root budget.
    let child_reservation = ledger
        .reservations
        .keys()
        .find(|key| key.starts_with(&format!("{}:", children[0])))
        .expect("child reservation in root ledger");
    assert!(ledger.reservations[child_reservation].settled);

    // Boundary audit: declared policy, child narrowing, and a file
    // snapshot that observed the evidence land inside ./out — no violations.
    let boundary = store
        .read_boundary_events(&root_run_id)
        .expect("boundary events");
    assert!(
        boundary
            .iter()
            .any(|event| matches!(event.event, BoundaryEvent::Declared { .. })),
        "chain declares its boundary"
    );
    assert!(
        boundary
            .iter()
            .any(|event| matches!(event.event, BoundaryEvent::ChildDeclared { .. })),
        "child reviewer boundary is recorded on the parent"
    );
    assert!(
        boundary.iter().any(|event| matches!(
            &event.event,
            BoundaryEvent::FileSnapshot { after, .. }
                if after.files.iter().any(|entry| entry.path.ends_with("evidence.json"))
        )),
        "file snapshot observed the evidence written inside ./out"
    );
    assert!(
        !boundary
            .iter()
            .any(|event| matches!(event.event, BoundaryEvent::Violation { .. })),
        "no boundary violations in a clean delivery"
    );

    // Crash recovery: a restarted process rebuilds the identical tree,
    // status, and budget exclusively from persisted events.
    let restarted = WorkflowStore::new(&root_dir);
    let view = reconstruct_watch(&restarted, &root_run_id).expect("watch reconstructs");
    assert_eq!(view.source, "persisted_events");
    assert_eq!(view.status, RunStatus::Succeeded);
    assert_eq!(view.tree.children.len(), 1, "reviewer child in the tree");
    assert!(view.budget.is_some(), "run reconstructs its budget");
}

// Negative control for the mechanical gate: a worker that claims success but
// lands evidence with a token false must FAIL the chain at the verification
// phase. This is what proves the mechanical half of the dual gate has teeth —
// the positive test above would stay green even if the gate were a no-op, so
// this test pins the failure path. The BADWORKER transport branch returns
// evidence {"dual-gate":true,"boundary-audit":false}.
#[test]
fn goalchain_mechanical_gate_fails_when_landed_evidence_token_is_false() {
    let temp = TempDir::new().expect("tempdir");
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    fs::copy(
        examples.join("goalchain.workflow.js"),
        temp.path().join("goalchain.workflow.js"),
    )
    .expect("copy chain");
    fs::copy(
        examples.join("goalchain-review.workflow.js"),
        temp.path().join("goalchain-review.workflow.js"),
    )
    .expect("copy reviewer child");
    // Contract carries the BADWORKER identifier so readiness passes and the
    // worker prompt selects the BADWORKER transport branch.
    fs::write(
        temp.path().join("contract.md"),
        "# contract\n\nAcceptance identifiers: dual-gate, boundary-audit, BADWORKER.\n",
    )
    .expect("write contract");

    let root_dir = temp.path().join("state");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let eng = engine(&root_dir, Arc::clone(&transport));
    // requireHumanGate=false so the chain reaches the mechanical gate without
    // parking; the gate must fail the run before any human decision.
    let failed = eng
        .start(
            &temp.path().join("goalchain.workflow.js"),
            json!({
                "contractPath": "contract.md",
                "evidencePath": "./out/evidence.json",
                "mechanicalTokens": ["dual-gate", "boundary-audit", "BADWORKER"],
                "requireHumanGate": false
            }),
            1,
            20,
        )
        .expect("start goalchain negative control");
    assert_eq!(failed.status, RunStatus::Failed, "{:?}", failed.result);
    let err = failed.error.expect("failure error");
    assert!(
        err.contains("mechanical gate failed"),
        "expected mechanical gate failure, got: {err}"
    );
}

// ===========================================================================
// Superiority benchmark and fault-injection release gate
//
// Fixed behavioral cases (Claude-native-expressible) and fault-injection cases
// (local-only). Every input is fixed and every assertion is a machine verdict,
// so the whole block is replayable. Cases already proven elsewhere in this
// suite (dynamic fan-out, no-barrier pipeline, schema correction, child
// workflows, budget persistence, human waiting, child degradation, cancellation
// propagation, boundary violation, restart-while-waiting-human, orphan-process
// termination) are not duplicated here; the tests below cover the remaining
// named cases.
// ===========================================================================

#[test]
fn benchmark_judge_panel_scores_independent_candidates_and_synthesizes() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-bench-judge");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(20)));
    let path = script(
        &temp,
        "judge-panel.js",
        r#"
        const schema = { type: "object", required: ["score", "rationale"], properties: { score: { type: "number" }, rationale: { type: "string" } } };
        const candidates = ["alpha", "beta", "gamma"];
        const scored = await parallel(candidates.map(name => () =>
          agent(`JUDGE_CASE ${name}`, { schema }).then(r => ({ name, score: r.score }))));
        const synth = await agent("JUDGE_SYNTH", {
          schema: { type: "object", required: ["winner", "rationale"], properties: { winner: { type: "string" }, rationale: { type: "string" } } }
        });
        return { scored, winner: synth.winner };
    "#,
    );
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 4, 100)
        .expect("judge panel workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(
        state.result,
        Some(json!({
            "scored": [
                {"name": "alpha", "score": 8},
                {"name": "beta", "score": 8},
                {"name": "gamma", "score": 8}
            ],
            "winner": "beta"
        }))
    );
    // 3 independent candidate submissions + 1 synthesis = 4 transport calls.
    assert_eq!(transport.count(), 4);
}

#[test]
fn benchmark_loop_until_dry_converges_when_no_new_items() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-bench-loop");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let path = script(
        &temp,
        "loop-until-dry.js",
        r#"
        const schema = { type: "object", required: ["items"], properties: { items: { type: "array", items: { type: "string" } } } };
        const seen = [];
        let rounds = 0;
        while (true) {
          const found = await agent("FIND_NEW", { schema });
          rounds += 1;
          const fresh = found.items.filter(item => !seen.includes(item));
          if (fresh.length === 0) { break; }
          seen.push(...fresh);
        }
        const processed = await pipeline(seen, item => agent(`WORK ${item}`));
        return { seen, rounds, processed };
    "#,
    );
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 4, 100)
        .expect("loop-until-dry workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    // Round 0 discovers x,y; round 1 discovers nothing new, so the loop dries
    // after exactly 2 discovery rounds, then the pipeline processes x and y.
    assert_eq!(
        state.result,
        Some(json!({"seen": ["x", "y"], "rounds": 2, "processed": ["x-ok", "y-ok"]}))
    );
    // 2 discovery + 2 pipeline work calls.
    assert_eq!(transport.count(), 4);
}

#[test]
fn fault_torn_journal_tail_is_tolerated_on_reconstruction_and_resume() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-torn-journal");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let path = script(
        &temp,
        "torn-journal.js",
        r#"const a = await agent("FIRST"); return { a };"#,
    );
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("run");
    assert_eq!(state.status, RunStatus::Succeeded);

    let store = WorkflowStore::new(&root);
    let journal_path = store.journal_path(&state.run_id);
    let before = store.journal_index(&state.run_id).expect("index before");
    assert!(!before.is_empty(), "agent call journaled");

    // Simulate a host kill mid-append: only the FINAL line is torn.
    let mut text = fs::read_to_string(&journal_path).expect("read journal");
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str("{\"at\":\"2026-01-01T00:00:00Z\",\"key\":\"agent:0\",\"state\":\"subm");
    fs::write(&journal_path, text).expect("write torn journal");

    // Reconstruction drops the torn tail and keeps every complete line.
    let after = store
        .journal_index(&state.run_id)
        .expect("index after torn tail");
    assert_eq!(
        after.len(),
        before.len(),
        "torn tail must be dropped, not counted"
    );
    assert_eq!(after, before, "complete entries survive the torn tail");

    // Roll the run back to a non-terminal status so `resume` actually
    // reconstructs from the journal instead of short-circuiting on a terminal
    // state (a Succeeded run resumes via `ensure_terminal_artifacts` without
    // ever reading the journal). This models a host killed after the agent call
    // settled but before the run wrote its own terminal status.
    store
        .update_state(&state.run_id, |st| {
            st.status = RunStatus::Running;
            st.result = None;
        })
        .expect("roll back to running");

    // Resume over the torn journal succeeds and does not resubmit the call:
    // replay reads the surviving Succeeded agent entry and returns it cached.
    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&state.run_id)
        .expect("resume over torn journal");
    assert_eq!(resumed.status, RunStatus::Succeeded, "{:?}", resumed.error);
    assert_eq!(
        transport.count(),
        1,
        "torn-tail resume resubmitted the call"
    );
}

#[test]
fn fault_mid_journal_corruption_still_errors() {
    // The torn-tail tolerance is specific to the FINAL line. A malformed line
    // anywhere else is genuine corruption and must still fail loudly rather
    // than be silently dropped.
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-mid-corrupt");
    let path = script(&temp, "mid-corrupt.js", r#"return { done: true };"#);
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 10)
        .expect("run");
    let store = WorkflowStore::new(&root);
    let journal_path = store.journal_path(&state.run_id);
    // One corrupt line followed by a valid line: the corrupt line is not the
    // tail, so reconstruction must error.
    fs::write(
        &journal_path,
        "not-json\n{\"at\":\"2026-01-01T00:00:00Z\",\"key\":\"x\",\"kind\":\"agent\",\"state\":\"succeeded\",\"label\":\"x\"}\n",
    )
    .expect("write corrupt journal");
    assert!(
        store.journal_index(&state.run_id).is_err(),
        "mid-journal corruption must not be silently tolerated"
    );
}

#[test]
fn fault_interrupted_state_write_never_leaves_torn_or_temp_state() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-atomic-write");
    let path = script(&temp, "atomic-write.js", r#"return { done: true };"#);
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 10)
        .expect("run");
    let store = WorkflowStore::new(&root);
    let run_dir = store.run_dir(&state.run_id);

    // Many in-place updates: because each write is temp-file + sync + rename
    // (temp created in the target's OWN directory so the rename is same-device
    // and therefore atomic), the on-disk state.json is always one complete
    // document — a kill at any point leaves either the old or the new file,
    // never a torn one. Every intermediate state must read back parseable.
    for i in 0..10 {
        store
            .update_state(&state.run_id, |st| st.phase = Some(format!("phase-{i}")))
            .expect("atomic update");
        let loaded = store.load_state(&state.run_id).expect("load after update");
        assert_eq!(loaded.phase.as_deref(), Some(format!("phase-{i}").as_str()));
    }

    // No temp file survives a successful write. The temp now lives in the run
    // dir itself (the same directory as state.json), so this scan exercises the
    // real temp location — a leaked temp here would be caught, unlike a temp
    // parked in the shared process temp dir.
    let leftovers: Vec<_> = fs::read_dir(&run_dir)
        .expect("read run dir")
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "atomic write leaked a temp file into the run dir: {leftovers:?}"
    );

    // Crash-safety property: an orphaned temp sibling (what a kill between
    // temp-create and rename would leave behind) must never affect the
    // observable target. Plant one, then confirm load_state still returns the
    // complete, current document.
    let orphan = run_dir.join(".state.json.deadbeef.999.tmp");
    fs::write(&orphan, b"{\"truncated\": tr").expect("plant orphan temp");
    let still_complete = store.load_state(&state.run_id).expect("load with orphan");
    assert_eq!(still_complete.phase.as_deref(), Some("phase-9"));
    fs::remove_file(&orphan).expect("clean orphan");
}

#[test]
fn fault_absent_provider_usage_settles_with_zero_tokens() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-no-usage");
    let transport = Arc::new(FakeTransport::without_usage(Duration::from_millis(5)));
    let path = script(
        &temp,
        "no-usage.js",
        r#"const a = await agent("hello"); return { a };"#,
    );
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("run without usage");
    // A provider that reports no usage diagnostics must not break settlement.
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);

    let store = WorkflowStore::new(&root);
    let journal = fs::read_to_string(store.journal_path(&state.run_id)).expect("journal");
    let succeeded = journal
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("journal value"))
        .find(|entry| entry["state"] == "succeeded")
        .expect("succeeded entry");
    assert!(
        succeeded.get("usage").is_none(),
        "no usage diagnostics means no usage field"
    );
    let ledger = store.reconstruct_budget(&state.run_id).expect("ledger");
    assert_eq!(ledger.used_calls, 1, "call still settles");
    assert_eq!(
        ledger.attributed_tokens, 0,
        "absent usage attributes zero tokens"
    );
}

#[test]
fn fault_submitted_transport_without_persisted_result_fails_the_call() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-vanish");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let path = script(&temp, "vanish.js", r#"return await agent("VANISH");"#);
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("run with vanished record");
    // The provider accepted the submission but lost the record before the
    // result was persisted; the first inspect fails "missing" and the call
    // fails fast rather than hanging or silently succeeding.
    assert_eq!(state.status, RunStatus::Failed);
    let err = state.error.expect("failure error");
    assert!(
        err.contains("missing"),
        "expected a missing-record inspect failure, got: {err}"
    );
    assert_eq!(transport.count(), 1, "exactly one submission was made");
    assert!(
        transport.record_ids().is_empty(),
        "the provider holds no persisted record for the run"
    );
}

#[test]
fn fault_unsettled_reservation_never_counts_as_used() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-unsettled");
    let path = script(&temp, "unsettled.js", r#"return { done: true };"#);
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .prepare(&path, Value::Null, 1, 5)
        .expect("prepare");
    let store = WorkflowStore::new(&root);
    // A reservation with no settlement and no release — the ledger state a
    // crash leaves behind for an in-flight call.
    store
        .append_budget_event(
            &state.run_id,
            &state.run_id,
            BudgetEvent::Reserved {
                key: "orphan".to_owned(),
                kind: CallKind::Agent,
                estimate_money: None,
            },
        )
        .expect("reserve");
    let ledger = store.reconstruct_budget(&state.run_id).expect("ledger");
    assert_eq!(ledger.held_calls, 1, "reservation is held");
    assert_eq!(ledger.used_calls, 0, "unsettled reservation is never used");
    assert!(!ledger.reservations["orphan"].settled);
    assert!(!ledger.reservations["orphan"].released);
}

#[test]
fn fault_child_success_before_parent_acknowledgment_is_replayed_on_resume() {
    let temp = TempDir::new().expect("tempdir");
    let root_dir = temp.path().join("state-fault-child-ack");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(5)));
    let _child = script(
        &temp,
        "ack-child.js",
        r#"return await agent("CHILD_WORK");"#,
    );
    let root = script(
        &temp,
        "ack-root.js",
        r#"const result = await workflow("ack-child.js"); return { result };"#,
    );
    let state = engine(&root_dir, Arc::clone(&transport))
        .start(&root, Value::Null, 1, 10)
        .expect("run parent");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let root_run_id = state.run_id.clone();
    assert_eq!(transport.count(), 1);

    let store = WorkflowStore::new(&root_dir);
    let child_id = store
        .child_run_ids(&root_run_id)
        .expect("child ids")
        .remove(0);
    assert_eq!(
        store.load_state(&child_id).expect("child state").status,
        RunStatus::Succeeded
    );

    // Simulate a host kill in the window between the child reaching Succeeded
    // and the parent acknowledging it: truncate the parent journal so the
    // workflow call is recorded only as `submitted` (its acknowledgment entry
    // never landed). The child stays Succeeded on disk.
    let journal_path = store.journal_path(&root_run_id);
    let submitted_line = fs::read_to_string(&journal_path)
        .expect("read parent journal")
        .lines()
        .find(|line| {
            serde_json::from_str::<Value>(line)
                .map(|entry| entry["state"] == "submitted" && entry["kind"] == "workflow")
                .unwrap_or(false)
        })
        .expect("parent journaled the child submission")
        .to_owned();
    fs::write(&journal_path, format!("{submitted_line}\n")).expect("truncate to submitted");
    // Roll the parent's persisted status back to the state it held inside that
    // window: still Running (it never reached its own terminal write), with the
    // child call recorded only as `submitted`. A real host kill leaves exactly
    // this — a non-terminal parent, a succeeded child on disk, an unacknowledged
    // journal entry.
    store
        .update_state(&root_run_id, |state| {
            state.status = RunStatus::Running;
            state.result = None;
            state.error = None;
        })
        .expect("roll parent back to running");
    let truncated = store.journal_index(&root_run_id).expect("truncated index");
    let child_call = truncated
        .values()
        .find(|entry| entry.kind == CallKind::Workflow)
        .expect("workflow call in truncated journal");
    assert_eq!(child_call.state, CallState::Submitted);

    // A killed-and-restarted controller resumes the parent. It sees the child
    // call as submitted, loads the child's persisted Succeeded state (never
    // resubmitting it), acknowledges the result, and completes.
    let resumed = engine(&root_dir, Arc::clone(&transport))
        .resume(&root_run_id)
        .expect("resume parent");
    assert_eq!(resumed.status, RunStatus::Succeeded, "{:?}", resumed.error);
    assert_eq!(resumed.result, Some(json!({"result": "ok"})));
    assert_eq!(
        transport.count(),
        1,
        "the succeeded child call was submitted again on parent resume"
    );
    let acked = store
        .journal_index(&root_run_id)
        .expect("journal after ack");
    let child_call_after = acked
        .values()
        .find(|entry| entry.kind == CallKind::Workflow)
        .expect("child call after ack");
    assert_eq!(child_call_after.state, CallState::Succeeded);
    assert_eq!(child_call_after.result, Some(json!("ok")));
}

#[test]
fn fault_host_kill_mid_run_recovers_via_replay_without_resubmission() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-fault-host-kill");
    let transport = Arc::new(FakeTransport::new(Duration::from_millis(300)));
    let path = script(
        &temp,
        "host-kill.js",
        r#"const a = await agent("FIRST"); const b = await agent("SECOND"); return { a, b };"#,
    );
    let runner_root = root.clone();
    let runner_transport = Arc::clone(&transport);
    let path_copy = path.clone();
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&path_copy, Value::Null, 1, 10)
    });
    // Let both submissions land, then stop the in-memory runtime. `pause` is a
    // COOPERATIVE stand-in for a host death, not a true crash: it persists a
    // clean Paused status rather than losing in-memory state mid-flight. What
    // this test still proves — and what a naive controller cannot — is that a
    // FRESH engine with no shared memory can reconstruct the run purely from
    // persisted journal/state and drive it to completion WITHOUT resubmitting
    // any already-settled call. The torn journal tail appended below exercises
    // the torn-tail tolerance on top of that reconstruction. The
    // no-resubmission assertion is robust regardless of the pause/inspect race
    // because `transport.count()` counts submissions only, not inspections.
    wait_for_call_count(&transport, 2);
    let run_id = wait_for_active_run(&root);
    engine(&root, Arc::clone(&transport))
        .pause(&run_id)
        .expect("pause (stop the in-memory runtime)");
    let killed = runner.join().expect("join").expect("paused run");
    assert_eq!(killed.status, RunStatus::Paused);
    assert_eq!(transport.count(), 2);

    // Append a torn journal tail on top of the kill, then restart fresh: a new
    // engine (no shared memory) must reconstruct from disk and complete.
    let store = WorkflowStore::new(&root);
    let journal_path = store.journal_path(&run_id);
    let mut text = fs::read_to_string(&journal_path).expect("read journal");
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str("{\"at\":\"2026-01-01T00:00:00Z\",\"key\":\"agent:1\",\"sta");
    fs::write(&journal_path, text).expect("write torn tail");

    let restarted = engine(&root, Arc::clone(&transport))
        .resume(&run_id)
        .expect("resume after kill");
    assert_eq!(
        restarted.status,
        RunStatus::Succeeded,
        "{:?}",
        restarted.error
    );
    assert_eq!(restarted.result, Some(json!({"a": "ok", "b": "ok"})));
    assert_eq!(
        transport.count(),
        2,
        "host-kill recovery resubmitted completed calls"
    );
}

// ---------------------------------------------------------------------------
// B2: stage-boundary verify gate for pipeline().
//
// pipeline() now ships default-ON verify between stages: an independent LLM
// reviewer checks the upstream stage output before the downstream stage sees
// it. A stage may opt out via `noVerify: true` or declare a declarative
// assertion that runs FIRST as a cheap machine check; on declarative fail the
// LLM reviewer is NOT invoked. On verify fail the item is marked Rejected
// (NOT null): the reject reason is written to the journal with
// CallKind::Verify + CallState::Rejected so it is auditable and
// distinguishable from a real null/empty output, and the downstream stage
// never receives that item. Other items in the same pipeline still complete.
//
// The degenerate `pipeline(items, worker)` N=1 case has no stage boundary, so
// no verify fires — the existing dynamic_pipeline_fans_out_and_runs_concurrently
// and benchmark_loop_until_dry_converges_when_no_new_items tests pin that
// backward-compat.
//
// FakeTransport review prompt: any prompt containing "Review the following
// stage output" returns a pass verdict unless the upstream output contains
// the "FAIL_ME" marker, in which case it returns a fail verdict. This lets
// tests drive both outcomes from the same transport branch.
// ---------------------------------------------------------------------------

#[test]
fn pipeline_verify_pass_lets_downstream_receive_item() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    // Two stages: stage 0 uppercases, stage 1 appends a suffix. Default-ON
    // verify fires between them. The reviewer returns pass, so stage 1
    // receives the uppercased value and the result is the suffixed form.
    let path = script(
        &temp,
        "verify-pass.js",
        r#"
        const items = ["alpha"];
        const out = await pipeline(items,
          { run: (item) => item.toUpperCase() },
          { run: (prev) => prev + "-ok" }
        );
        return { out };
    "#,
    );
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 4, 100)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(
        state.result,
        Some(json!({ "out": ["ALPHA-ok"] }))
    );
    // One verify LLM call between the two stages (the last stage has no
    // downstream boundary, so no verify fires for it).
    let requests = transport.requests.lock().expect("requests");
    let verify_count = requests
        .iter()
        .filter(|r| match &r.input {
            Input::Text { text } => text.contains("Review the following stage output"),
            _ => false,
        })
        .count();
    assert_eq!(verify_count, 1, "exactly one verify LLM call between stages");
}

#[test]
fn pipeline_verify_fail_marks_item_rejected_and_skips_downstream() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    // Two items, two stages. Stage 0 returns "FAIL_ME" for item "bad" so the
    // verify gate rejects it; item "good" passes verify and flows through to
    // stage 1. The bad item MUST come back as a Rejected marker (not null),
    // the journal MUST record a CallKind::Verify/CallState::Rejected entry
    // with the reject reason, and the good item MUST still complete stage 1.
    let path = script(
        &temp,
        "verify-fail.js",
        r#"
        const items = ["good", "bad"];
        const out = await pipeline(items,
          { run: (item) => item === "bad" ? "FAIL_ME" : item },
          { run: (prev) => prev + "-ok" }
        );
        return {
          good: out[0],
          bad: out[1],
          badIsObject: out[1] != null && typeof out[1] === "object",
          badMarker: out[1] && out[1]["__servitor_rejected__"],
          badReason: out[1] && out[1]["reason"],
        };
    "#,
    );
    let root = temp.path().join("state");
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 8, 100)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let result = state.result.expect("result");
    assert_eq!(result["good"], "good-ok", "good item flows through both stages");
    assert_eq!(result["badIsObject"], true, "rejected item is a Rejected object, not null");
    assert_eq!(result["badMarker"], true, "Rejected marker present");
    assert!(
        result["badReason"]
            .as_str()
            .unwrap_or_default()
            .contains("verify rejected"),
        "reject reason carries the LLM verdict: {:?}",
        result["badReason"]
    );

    // The journal records the reject as CallKind::Verify + CallState::Rejected
    // with the reject reason in the error field — distinguishable from null.
    let store = WorkflowStore::new(&root);
    let journal = fs::read_to_string(store.journal_path(&state.run_id)).expect("journal");
    let rejects: Vec<Value> = journal
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("journal entry"))
        .filter(|entry| entry["kind"] == "verify" && entry["state"] == "rejected")
        .collect();
    assert_eq!(rejects.len(), 1, "exactly one verify-reject entry in the journal");
    assert!(
        rejects[0]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("verify rejected"),
        "journal reject entry carries the reason: {:?}",
        rejects[0]
    );
}

#[test]
fn pipeline_declarative_fast_path_short_circuits_without_llm_reviewer() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    // Two stages; stage 0 carries a declarative assertion that exits 1. The
    // declarative fast-path must short-circuit to reject WITHOUT invoking the
    // LLM reviewer, so transport.count() must be 0 agent calls.
    let path = script(
        &temp,
        "verify-declarative.js",
        r#"
        const items = ["x"];
        const out = await pipeline(items,
          {
            run: (item) => item,
            declarative: { command: "cmd", args: ["/C", "exit 1"] }
          },
          { run: (prev) => prev + "-ok" }
        );
        return {
          marker: out[0] && out[0]["__servitor_rejected__"],
          reason: out[0] && out[0]["reason"],
        };
    "#,
    );
    let state = engine(&temp.path().join("state"), Arc::clone(&transport))
        .start(&path, Value::Null, 8, 100)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let result = state.result.expect("result");
    assert_eq!(result["marker"], true, "declarative fail produces a Rejected marker");
    assert!(
        result["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("declarative assertion failed"),
        "reject reason points at the declarative assertion: {:?}",
        result["reason"]
    );
    // CRITICAL: the LLM reviewer must NOT be invoked when the declarative
    // assertion already failed. No agent prompts at all in this workflow.
    assert_eq!(
        transport.count(),
        0,
        "declarative fast-path must short-circuit before the LLM reviewer"
    );
}

#[test]
fn pipeline_independent_from_violation_in_verify_is_rejected() {
    let temp = TempDir::new().expect("tempdir");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    // A single provider policy where the `reviewer` role declares
    // `independentFrom: ["maker"]`. Stage 0 (the maker) selects the only
    // available provider/model; when the verify gate fires for stage 1 with
    // `role: "reviewer"`, capability resolution MUST reject the same-model
    // independent role before transport submission — the verify call errors
    // out and the item is marked rejected with a verify-agent-failed reason.
    let path = temp.path().join("verify-independence.js");
    fs::write(
        &path,
        r#"export const meta = {
          name: "verify-independence",
          contract: "workflow",
          capabilities: {
            providers: [{ agent: "claude", model: "claude-opus-5", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 200000 }],
            roles: {
              maker: { requires: ["reasoning"] },
              reviewer: { requires: ["reasoning"], independentFrom: ["maker"] }
            }
          }
        };
        const out = await pipeline(["item"],
          { run: (item) => agent(`WORK ${item}`, { role: "maker" }), verify: { role: "reviewer" } },
          { run: (prev) => prev + "-ok" }
        );
        return {
          marker: out[0] && out[0]["__servitor_rejected__"],
          reason: out[0] && out[0]["reason"],
        };"#,
    )
    .expect("write script");
    let root = temp.path().join("state");
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 8, 100)
        .expect("workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let result = state.result.expect("result");
    assert_eq!(result["marker"], true, "independence violation rejects the item");
    assert!(
        result["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("must be independent"),
        "reject reason surfaces the independence violation: {:?}",
        result["reason"]
    );
    // Capability resolution rejects the reviewer before any transport submit
    // for the verify call, so only the maker agent call lands.
    assert_eq!(
        transport.count(),
        1,
        "reviewer must be rejected before transport submission"
    );
    let events = WorkflowStore::new(&root)
        .read_capability_events(&state.run_id)
        .expect("capability events");
    assert!(
        matches!(events.last().map(|event| &event.event), Some(CapabilityEvent::IndependenceViolation { role, conflict_role, .. }) if role == "reviewer" && conflict_role == "maker"),
        "independence violation recorded for reviewer vs maker: {:?}",
        events.last()
    );
}

// ---------------------------------------------------------------------------
// spawn(specs[]) builtin — runtime fan-out into independent child runs.
// ---------------------------------------------------------------------------

#[test]
fn spawn_builtin() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-spawn-builtin");
    let path = script(
        &temp,
        "spawn-builtin.js",
        r#"const child = 'export const meta = { name: "c", contract: "workflow" }; return { i: args.i };';
const specs = [
  { inline: child, args: { i: 1 } },
  { inline: child, args: { i: 2 } },
];
const r = await spawn(specs);
return { count: r.length, ids: r.map(x => x.runId), vals: r.map(x => x.result.i) };"#,
    );
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 20)
        .expect("spawn workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    let result = state.result.expect("result");
    assert_eq!(result["count"], 2);
    let ids = result["ids"].as_array().expect("ids array");
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert_eq!(result["vals"], json!([1, 2]));

    // Two spawn child runs are linked from the parent journal with distinct
    // child_run_id values and a "spawn" call kind.
    let store = WorkflowStore::new(&root);
    let journal = fs::read_to_string(store.journal_path(&state.run_id)).expect("journal");
    let child_ids: Vec<String> = journal
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("journal entry"))
        .filter(|entry| entry["kind"] == "spawn" && entry["state"] == "succeeded")
        .filter_map(|entry| entry["child_run_id"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(child_ids.len(), 2, "two spawn children in parent journal");
    assert_ne!(child_ids[0], child_ids[1]);
}

#[test]
fn spawn_depth_guard() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-spawn-depth");
    // A linear chain of 17 DISTINCT scripts. Each spawns the next by filename;
    // distinct sources keep the cycle detector quiet so MAX_WORKFLOW_DEPTH=16
    // is the only thing that can stop the chain. Preparing the depth-16 child
    // from depth-15 sees 16 ancestors and must reject.
    for n in 0..=16u32 {
        let body = if n == 16 {
            format!("return {{ level: {n} }};")
        } else {
            format!(
                "const r = await spawn([{{ path: \"depth-{}.js\", args: {{}} }}]);\nreturn {{ level: {n}, child: r[0].result }};",
                n + 1
            )
        };
        fs::write(
            temp.path().join(format!("depth-{n}.js")),
            format!(
                "export const meta = {{ name: \"depth-{n}\", contract: \"workflow\" }};\n{body}",
            ),
        )
        .expect("write depth script");
    }
    let path = temp.path().join("depth-0.js");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, Value::Null, 1, 1000)
        .expect("terminal depth workflow");
    assert_eq!(state.status, RunStatus::Failed, "{:?}", state.status);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("maximum depth"),
        "depth guard error: {:?}",
        state.error
    );
}

#[test]
fn spawn_budget_attribution() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-spawn-budget");
    let path = temp.path().join("spawn-budget.js");
    fs::write(
        &path,
        r#"export const meta = { name: "spawn-budget", contract: "workflow" };
const child = 'export const meta = { name: "c", contract: "workflow" }; const a = await agent("hello " + args.label); return { label: args.label, ok: a };';
const specs = [
  { inline: child, args: { label: "x" } },
  { inline: child, args: { label: "y" } },
  { inline: child, args: { label: "z" } },
];
const r = await spawn(specs);
return { count: r.length };"#,
    )
    .expect("write budget workflow");
    let transport = Arc::new(FakeTransport::new(Duration::ZERO));
    let state = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 100)
        .expect("budget workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);

    // Each of the 3 children made one agent call. Those calls must land in the
    // shared root budget ledger (keys prefixed with the child run id), proving
    // child runs count toward the shared max_calls ledger — not just the
    // parent's spawn reservations.
    let store = WorkflowStore::new(&root);
    let ledger = store
        .reconstruct_budget(&state.run_id)
        .expect("reconstruct root ledger");
    let child_call_keys = ledger
        .reservations
        .keys()
        .filter(|key| key.starts_with("child-"))
        .count();
    assert!(
        child_call_keys >= 3,
        "expected >=3 child-attributed reservations in root ledger, got {child_call_keys}: {:?}",
        ledger.reservations.keys().collect::<Vec<_>>()
    );
    assert!(
        ledger.used_calls >= 3,
        "expected >=3 settled calls in root ledger, got {}",
        ledger.used_calls
    );
    assert_eq!(transport.count(), 3, "one provider call per spawned child");
}

#[test]
fn spawn_probe_fanout() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-spawn-probe");
    let example = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("spawn-fanout.workflow.js");
    let path = temp.path().join("spawn-fanout.workflow.js");
    fs::copy(&example, &path).expect("copy example workflow");
    let state = engine(&root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, json!({ "count": 3 }), 1, 20)
        .expect("probe workflow");
    assert_eq!(state.status, RunStatus::Succeeded, "{:?}", state.error);
    assert_eq!(state.result.expect("result")["count"], 3);

    // The probe must produce N>1 child run ids in the parent journal from a
    // single spawn() call (runtime-determined fan-out).
    let store = WorkflowStore::new(&root);
    let journal = fs::read_to_string(store.journal_path(&state.run_id)).expect("journal");
    let child_ids: Vec<String> = journal
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("journal entry"))
        .filter(|entry| entry["kind"] == "spawn")
        .filter_map(|entry| entry["child_run_id"].as_str().map(str::to_owned))
        .collect();
    assert!(
        child_ids.len() > 1,
        "expected N>1 child run ids in parent journal, got {}: {:?}",
        child_ids.len(),
        child_ids
    );
}
