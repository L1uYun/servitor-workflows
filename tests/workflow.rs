use chrono::Utc;
use serde_json::{Value, json};
use servitor::protocol::Diagnostics;
use servitor::{
    Activity, ErrorInfo, Input, Output, RunRecord, RunState as ServitorState, SubmitRequest,
    SubmitResponse,
};
use servitor_workflows::{Engine, RunStatus, Transport, WorkflowStore};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
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
}

impl FakeTransport {
    fn new(delay: Duration) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            active_inspections: AtomicUsize::new(0),
            peak_inspections: AtomicUsize::new(0),
            delay,
            records: Mutex::new(BTreeMap::new()),
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
        let run_id = format!("fake-{number}");
        let prompt = match request.input {
            Input::Text { text } => text,
            Input::Image(_) => String::new(),
        };
        let output = if prompt.contains("DISCOVER") {
            r#"{"items":["a","b","c"]}"#.to_owned()
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
        format!("export const meta = {{ name: \"test\" }};\n{body}"),
    )
    .expect("write fixture");
    path
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
        .approve(&first.run_id, true, "evidence accepted".to_owned())
        .expect("approve");
    assert_eq!(completed.status, RunStatus::Succeeded);
    assert_eq!(transport.count(), 2, "completed call was submitted again");
    assert_eq!(
        completed.result.expect("result")["decision"]["approved"],
        true
    );
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
    let final_pause = runner.join().expect("join").expect("runner result");
    assert_eq!(final_pause.status, RunStatus::Paused);

    let second_root = temp.path().join("state-2");
    let runner_root = second_root.clone();
    let runner_transport = Arc::clone(&transport);
    let runner = thread::spawn(move || {
        engine(&runner_root, runner_transport).start(&path, Value::Null, 1, 10)
    });
    let second_id = wait_for_active_run(&second_root);
    let cancelling = engine(&second_root, Arc::clone(&transport))
        .cancel(&second_id)
        .expect("cancel");
    assert!(matches!(
        cancelling.status,
        RunStatus::Cancelling | RunStatus::Cancelled
    ));
    let final_cancel = runner.join().expect("join").expect("runner result");
    assert_eq!(final_cancel.status, RunStatus::Cancelled);
}

#[test]
fn resume_keeps_run_id_replays_journal_and_writes_report() {
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
    let report = resumed.report.expect("terminal report path");
    let html = fs::read_to_string(report).expect("read report");
    assert!(html.contains(&run_id));
    assert!(html.contains("Workflow 交付汇报"));
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
