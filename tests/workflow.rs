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
        let output = if prompt.contains("FENCED_JSON") {
            "done

```json
{\"summary\":\"ok\",\"score\":1}
```".to_owned()
        } else if prompt.contains("RETRY_JSON") {
            if number == 1 {
                "not-json".to_owned()
            } else {
                r#"{"ok":true}"#.to_owned()
            }
        } else if prompt.contains("DISCOVER") {
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
fn resume_retries_a_failed_structured_agent_call() {
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

    let failed = engine(&root, Arc::clone(&transport))
        .start(&path, Value::Null, 1, 10)
        .expect("failed terminal state");
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(transport.count(), 1);

    let resumed = engine(&root, Arc::clone(&transport))
        .resume(&failed.run_id)
        .expect("resume structured call");
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(resumed.resume_count, 1);
    assert_eq!(resumed.result, Some(json!({"ok": true})));
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
            let disk: Value = serde_json::from_str(
                &fs::read_to_string(&candidate).expect("read result.json"),
            )
            .expect("parse result.json");
            assert_eq!(disk["exitCode"], 0);
            assert_eq!(disk["timedOut"], false);
            assert!(disk["stdout"].as_str().unwrap_or_default().contains("rustc"));
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
    assert!(result["caught"].as_str().unwrap_or_default().contains("command exited"));

    let store = WorkflowStore::new(&state_root);
    let run_dir = store.run_dir(&state.run_id).join("commands");
    let mut found = false;
    for entry in fs::read_dir(&run_dir).expect("commands dir") {
        let entry = entry.expect("entry");
        let candidate = entry.path().join("result.json");
        if candidate.exists() {
            let disk: Value = serde_json::from_str(
                &fs::read_to_string(&candidate).expect("read result.json"),
            )
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
    let final_pause = runner.join().expect("join").expect("runner result");
    assert_eq!(final_pause.status, RunStatus::Paused);
    assert!(
        !root.join("runs").join(&run_id).join("pause.request").exists(),
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
        .cancel(&second_id)
        .expect("cancel");
    assert!(matches!(
        cancelling.status,
        RunStatus::Cancelling | RunStatus::Cancelled
    ));
    let final_cancel = runner.join().expect("join").expect("runner result");
    assert_eq!(final_cancel.status, RunStatus::Cancelled);
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
        failed
            .error
            .as_deref()
            .unwrap_or("")
            .contains("max_calls"),
        "{:?}",
        failed.error
    );
    // Resume must not grant a fresh budget of 1 that lets TWO run.
    let resumed = engine.resume(&failed.run_id).expect("resume");
    assert_eq!(resumed.status, RunStatus::Failed, "{:?}", resumed.error);
    assert_eq!(transport.count(), 1, "second agent must not be submitted");
}
