use chrono::Utc;
use serde_json::{Value, json};
use servitor::protocol::Diagnostics;
use servitor::{
    Activity, ErrorInfo, Input, Output, RunRecord, RunState as ServitorState, SubmitRequest,
    SubmitResponse,
};
use servitor_workflows::{
    BudgetEvent, CallKind, CallState, Engine, JournalEntry, RunState, RunStatus, Transport,
    WorkflowStore,
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
                    continuation: None,
                    activity: None::<Activity>,
                    diagnostics: Diagnostics::default(),
                },
            );
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
            record
                .diagnostics
                .provider
                .insert("usage".to_owned(), json!({"input": 100, "output": 20}));
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
        format!("export const meta = {{ name: \"test\", contract: \"workflow.v2\" }};\n{body}"),
    )
    .expect("write fixture");
    path
}

fn legacy_state(store: &WorkflowStore, path: &Path, status: RunStatus) -> RunState {
    let now = Utc::now();
    let run_id = "legacy-v1-fixture".to_owned();
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
fn v2_budget_ledger_reserves_and_settles_each_host_call() {
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
fn v2_meta_money_cap_is_persisted_and_emitted() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-money-cap");
    let path = temp.path().join("money-cap.js");
    fs::write(
        &path,
        r#"export const meta = { name: "cap", contract: "workflow.v2", moneyCap: 123 }; return { done: true };"#,
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
fn v2_budget_released_reservation_cannot_settle() {
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
fn v2_resume_preserves_submitted_reservation() {
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
fn v2_rejects_invalid_money_cap() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("state-invalid-money-cap");
    for (name, money_cap) in [("zero", "0"), ("negative", "-1"), ("fraction", "1.5")] {
        let path = temp.path().join(format!("{name}.js"));
        fs::write(
            &path,
            format!(
                "export const meta = {{ name: \"cap\", contract: \"workflow.v2\", moneyCap: {money_cap} }}; return {{}};"
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
fn new_runs_require_explicit_v2_contract() {
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
        .expect_err("missing v2 contract must fail");
    assert!(missing_error.to_string().contains("workflow.v2"));
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
        export const meta = makeMeta({ contract: "workflow.v2" });
        return {};
        "#,
    )
    .expect("write computed metadata fixture");

    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("computed metadata must not satisfy v2 contract");
    assert!(error.to_string().contains("workflow.v2"));
}

#[test]
fn metadata_decoy_before_real_declaration_is_ignored() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("metadata-decoy.js");
    fs::write(
        &path,
        r#"
        // export const meta = { name: "decoy" };
        const text = "export const meta = { contract: 'workflow.v2' };";
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
    .expect_err("decoy metadata must not satisfy v2 contract");
    assert!(error.to_string().contains("workflow.v2"));
}

#[test]
fn regex_metadata_decoy_is_ignored() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("regex-metadata-decoy.js");
    fs::write(
        &path,
        r#"
        const marker = /export const meta = { contract: "workflow.v2" }/;
        return { accepted: marker.test("meta") };
        "#,
    )
    .expect("write regex metadata decoy fixture");

    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("regex text must not satisfy v2 contract");
    assert!(error.to_string().contains("workflow.v2"));
}

#[test]
fn nested_metadata_declaration_is_ignored() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("nested-metadata-decoy.js");
    fs::write(
        &path,
        r#"
        function unused() { export const meta = { contract: "workflow.v2" }; }
        return { ok: true };
        "#,
    )
    .expect("write nested metadata decoy fixture");

    let error = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect_err("nested metadata must not satisfy v2 contract");
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
          contract: "workflow.v2",
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
    assert_eq!(state.contract.as_deref(), Some("workflow.v2"));
}

#[test]
fn v2_contract_metadata_accepts_json5_comments() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("commented-meta.js");
    fs::write(
        &path,
        r#"export const meta = {
          // this comment has a closing brace: }
          name: "commented",
          /* and this one has a quote: " */
          contract: "workflow.v2",
        };
        return { ok: true };"#,
    )
    .expect("write commented metadata fixture");

    let state = engine(
        &temp.path().join("state"),
        Arc::new(FakeTransport::new(Duration::ZERO)),
    )
    .start(&path, Value::Null, 1, 10)
    .expect("JSON5 comments must not break v2 metadata extraction");
    assert_eq!(state.contract.as_deref(), Some("workflow.v2"));
    assert_eq!(state.status, RunStatus::Succeeded);
}

#[test]
fn legacy_nonterminal_run_resumes_without_rewriting_v1_journal() {
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
    fs::write(store.journal_path(&state.run_id), journal).expect("write frozen v1 journal");
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
fn v2_events_are_append_only_and_reconstruct_terminal_state() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let path = script(
        &temp,
        "events.js",
        "phase(\"verify\"); return { outcome: \"ok\" };",
    );
    let state = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)))
        .start(&path, json!({"input": 1}), 2, 12)
        .expect("run v2 workflow");
    let store = WorkflowStore::new(&state_root);
    let events = store.read_events(&state.run_id).expect("read v2 events");

    assert_eq!(state.version, 2);
    assert_eq!(state.contract.as_deref(), Some("workflow.v2"));
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
fn v2_gate_rejection_is_recorded_as_terminal_event() {
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
fn v2_cancellation_is_recorded_as_terminal_event() {
    let temp = TempDir::new().expect("tempdir");
    let state_root = temp.path().join("state");
    let path = script(&temp, "cancel.js", "return { unused: true };");
    let runtime = engine(&state_root, Arc::new(FakeTransport::new(Duration::ZERO)));
    let prepared = runtime
        .prepare(&path, Value::Null, 1, 10)
        .expect("prepare v2 run");
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
        .expect("prepare v2 run");
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
fn v2_pause_resume_and_supersede_reconstruct_from_events() {
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
        .expect("prepare v2 run");
    let run_id = prepared.run_id;
    let paused = runtime.pause(&run_id).expect("pause v2 run");
    assert_eq!(paused.status, RunStatus::Paused);

    let resumed = runtime.resume(&run_id).expect("resume v2 run");
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
        .expect("prepare v2 supersede run");
    let superseded = runtime
        .supersede(
            &redirect.run_id,
            "redirected by operator".to_owned(),
            Some("evidence.md".to_owned()),
            Some("next-contract.md".to_owned()),
        )
        .expect("supersede v2 run");
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
fn invalid_structured_agent_is_corrected_without_resume() {
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
        if let Ok(entries) = fs::read_dir(root.join("runs")) {
            if let Some(run_id) = entries.flatten().find_map(|entry| {
                let run_id = entry.file_name().to_string_lossy().into_owned();
                WorkflowStore::new(root)
                    .load_state(&run_id)
                    .ok()
                    .filter(|state| state.parent_run_id.is_none())
                    .map(|_| run_id)
            }) {
                return run_id;
            }
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
        if let Ok(entries) = fs::read_dir(root_dir.join("runs")) {
            if let Some(run_id) = entries.flatten().find_map(|entry| {
                let run_id = entry.file_name().to_string_lossy().into_owned();
                WorkflowStore::new(&root_dir)
                    .load_state(&run_id)
                    .ok()
                    .filter(|state| state.parent_run_id.is_none())
                    .map(|_| run_id)
            }) {
                break run_id;
            }
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
        WorkflowStore::new(&temp.path().join("state"))
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
    assert_eq!(result["protocol"], "negotiate-2body.v1");
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
