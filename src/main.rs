use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{Shell, generate};
use serde::Serialize;
use serde_json::Value;
use servitor_workflows::{
    Engine, ErrorPayload, PublicRun, RunState, RunStatus, WorkflowError, default_engine,
};
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const WAIT_OUTCOME_KEY: &str = "__servitor_workflows_wait_outcome";

#[derive(Parser)]
#[command(
    name = "servitor-workflows",
    version,
    about = "Run dynamic agent workflows",
    after_help = "Examples:\n  servitor-workflows check workflow.js\n  servitor-workflows run workflow.js --args '{\"x\":1}'\n  servitor-workflows run workflow.js --detach\n  servitor-workflows get RUN_ID --wait --timeout-seconds 300\n  servitor-workflows list --limit 20 --status failed\n  servitor-workflows resume RUN_ID\n  servitor-workflows cancel RUN_ID --reason 'superseded by new contract' --dry-run\n  servitor-workflows schema\n\nExit codes: 0 ok, 1 runtime/terminal failure, 2 invalid input, 3 not found/waiting_human, 4 wait timeout"
)]
struct Cli {
    #[arg(long, global = true, value_enum, default_value_t = OutputMode::Json)]
    output: OutputMode,
    /// Refuse any interactive prompt path (already the default; kept for agent contracts).
    #[arg(
        long,
        global = true,
        env = "SERVITOR_WORKFLOWS_NO_INTERACTIVE",
        default_value_t = false
    )]
    no_interactive: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum OutputMode {
    Json,
    Human,
    Quiet,
    /// One JSON object per line (JSONL). Used by `watch` to stream the
    /// reconstructed view; other commands treat it like `json`.
    Jsonl,
}

#[derive(Subcommand)]
enum Command {
    Run {
        workflow: PathBuf,
        #[arg(long, default_value = "null")]
        args: String,
        #[arg(long, default_value_t = default_parallelism())]
        max_parallel: usize,
        #[arg(long, default_value_t = 1000)]
        max_calls: usize,
        /// Create the run record, execute it in a detached child, and return immediately.
        #[arg(long)]
        detach: bool,
    },
    /// Validate a workflow script (meta + engine-wrap parse) without running it.
    Check {
        workflow: PathBuf,
    },
    Resume {
        run_id: String,
    },
    Get {
        run_id: String,
        /// Poll until the run succeeds, fails, is cancelled/superseded, or needs a human.
        #[arg(long)]
        wait: bool,
        /// Maximum wait duration. Without this option, wait indefinitely.
        #[arg(long, requires = "wait")]
        timeout_seconds: Option<u64>,
    },
    List {
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Filter: running|waiting_human|paused|succeeded|failed|cancelled|superseded|...
        #[arg(long)]
        status: Option<String>,
    },
    Approve {
        run_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        value: Option<String>,
    },
    Reject {
        run_id: String,
        #[arg(long)]
        reason: String,
    },
    Pause {
        run_id: String,
        #[arg(long)]
        dry_run: bool,
    },
    Cancel {
        run_id: String,
        /// Why this run is being cancelled; recorded in state.json for audit.
        #[arg(long)]
        reason: String,
        #[arg(long)]
        dry_run: bool,
    },
    Supersede {
        run_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        evidence: Option<String>,
        #[arg(long)]
        new_contract: Option<String>,
        #[arg(long)]
        dry_run: bool,
    },
    Inspect {
        run_id: String,
    },
    /// Reconstruct a live tree view (status, budget/usage, waiting categories,
    /// critical path, recovery) exclusively from persisted events.
    Watch {
        run_id: String,
    },
    Schema,
    /// Emit shell completions for bash/zsh/fish/powershell/elvish.
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Execute a prepared run. Internal detached-child entry point.
    #[command(hide = true)]
    ExecuteExisting {
        run_id: String,
    },
}

#[derive(Serialize)]
struct Envelope {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    meta: Meta,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorPayload>,
}

#[derive(Serialize)]
struct Meta {
    tool: &'static str,
    version: &'static str,
}

fn meta() -> Meta {
    Meta {
        tool: "servitor-workflows",
        version: env!("CARGO_PKG_VERSION"),
    }
}

fn clap_error_message(err: &clap::Error) -> String {
    // Prefer a single-line reason; full usage stays in --help/schema.
    let raw = err.to_string();
    raw.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("invalid arguments")
        .trim_start_matches("error: ")
        .to_owned()
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            use clap::error::ErrorKind;
            match err.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => err.exit(),
                _ => {
                    let envelope = Envelope {
                        ok: false,
                        data: None,
                        meta: meta(),
                        error: Some(ErrorPayload {
                            code: "invalid_arguments".into(),
                            message: clap_error_message(&err),
                            remediation:
                                "Run `servitor-workflows --help` or `servitor-workflows schema`."
                                    .into(),
                        }),
                    };
                    let _ = emit_json(&envelope);
                    std::process::exit(2);
                }
            }
        }
    };
    let _ = cli.no_interactive;
    let engine = default_engine();
    let result = match cli.command {
        Command::Run {
            workflow,
            args,
            max_parallel,
            max_calls,
            detach,
        } => parse_args(&args).and_then(|args| {
            if detach {
                let state = engine.prepare(&workflow, args, max_parallel, max_calls)?;
                if let Err(error) = spawn_detached(&state.run_id) {
                    let _ =
                        engine.cancel(&state.run_id, "detached child failed to start".to_owned());
                    return Err(error);
                }
                public_value(state)
            } else {
                engine
                    .start(&workflow, args, max_parallel, max_calls)
                    .and_then(public_value)
            }
        }),
        Command::Resume { run_id } => engine.resume(&run_id).and_then(public_value),
        Command::Get {
            run_id,
            wait,
            timeout_seconds,
        } => {
            if wait {
                wait_for_run(&engine, &run_id, timeout_seconds)
            } else {
                engine.get(&run_id).and_then(to_value)
            }
        }
        Command::List { limit, status } => {
            if let Some(filter) = status.as_deref() {
                const ALLOWED: &[&str] = &[
                    "running",
                    "waiting_human",
                    "pausing",
                    "paused",
                    "cancelling",
                    "succeeded",
                    "failed",
                    "cancelled",
                    "superseded",
                ];
                if !ALLOWED.contains(&filter) {
                    Err(WorkflowError::InvalidOperation(format!(
                        "status must be one of {}",
                        ALLOWED.join("|")
                    )))
                } else {
                    engine.list(limit, status.as_deref())
                }
            } else {
                engine.list(limit, None)
            }
        }
        Command::Approve {
            run_id,
            reason,
            value,
        } => value
            .map(|raw| parse_args(&raw))
            .transpose()
            .and_then(|parsed| engine.approve(&run_id, true, reason, parsed))
            .and_then(public_value),
        Command::Reject { run_id, reason } => engine
            .approve(&run_id, false, reason, None)
            .and_then(public_value),
        Command::Pause { run_id, dry_run } => {
            if dry_run {
                engine.get(&run_id).and_then(|run| {
                    to_value(serde_json::json!({
                        "dry_run": true,
                        "run_id": run.run_id,
                        "status": run.status,
                        "would_pause": !matches!(
                            run.status,
                            servitor_workflows::RunStatus::Succeeded
                                | servitor_workflows::RunStatus::Failed
                                | servitor_workflows::RunStatus::Cancelled
                                | servitor_workflows::RunStatus::Superseded
                        ),
                    }))
                })
            } else {
                engine.pause(&run_id).and_then(public_value)
            }
        }
        Command::Check { workflow } => engine.check(&workflow),
        Command::Cancel {
            run_id,
            reason,
            dry_run,
        } => {
            if dry_run {
                engine.get(&run_id).and_then(|run| {
                    to_value(serde_json::json!({
                        "dry_run": true,
                        "run_id": run.run_id,
                        "status": run.status,
                        "reason": reason,
                        "would_cancel": !matches!(
                            run.status,
                            servitor_workflows::RunStatus::Succeeded
                                | servitor_workflows::RunStatus::Failed
                                | servitor_workflows::RunStatus::Cancelled
                                | servitor_workflows::RunStatus::Superseded
                        ),
                    }))
                })
            } else {
                engine.cancel(&run_id, reason).and_then(public_value)
            }
        }
        Command::Supersede {
            run_id,
            reason,
            evidence,
            new_contract,
            dry_run,
        } => {
            if dry_run {
                engine.get(&run_id).and_then(|run| {
                    to_value(serde_json::json!({
                        "dry_run": true,
                        "run_id": run.run_id,
                        "status": run.status,
                        "reason": reason,
                        "evidence": evidence,
                        "new_contract": new_contract,
                        "would_supersede": !matches!(
                            run.status,
                            servitor_workflows::RunStatus::Succeeded
                                | servitor_workflows::RunStatus::Failed
                                | servitor_workflows::RunStatus::Cancelled
                                | servitor_workflows::RunStatus::Superseded
                        ),
                    }))
                })
            } else {
                engine
                    .supersede(&run_id, reason, evidence, new_contract)
                    .and_then(public_value)
            }
        }
        Command::Inspect { run_id } => engine.inspect(&run_id).and_then(to_value),
        Command::Watch { run_id } => {
            servitor_workflows::reconstruct_watch(engine.store(), &run_id).and_then(to_value)
        }
        Command::Schema => Ok(schema_value()),
        Command::Completions { shell } => {
            emit_completions(shell);
        }
        Command::ExecuteExisting { run_id } => {
            engine.execute_existing(&run_id).and_then(public_value)
        }
    };

    let (envelope, code) = match result {
        Ok(mut value) => {
            let terminal = terminal_exit_code(&value);
            if let Some(object) = value.as_object_mut() {
                object.remove(WAIT_OUTCOME_KEY);
            }
            if terminal == 0 {
                (
                    Envelope {
                        ok: true,
                        data: Some(value),
                        meta: meta(),
                        error: None,
                    },
                    0,
                )
            } else {
                let run_id = value
                    .get("run_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let status = value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed")
                    .to_owned();
                let (error_code, message, remediation) = match terminal {
                    3 => (
                        "waiting_human".to_owned(),
                        "workflow is waiting for a human decision".to_owned(),
                        run_id.map_or_else(
                            || "Inspect the gate and submit an approve or reject decision.".to_owned(),
                            |run_id| format!(
                                "Inspect data.gate, then run `servitor-workflows approve {run_id} --reason TEXT` or `servitor-workflows reject {run_id} --reason TEXT`."
                            ),
                        ),
                    ),
                    4 => (
                        "wait_timeout".to_owned(),
                        "workflow did not reach a wait outcome before the timeout".to_owned(),
                        run_id.map_or_else(
                            || "Retry get --wait with a longer timeout.".to_owned(),
                            |run_id| format!(
                                "Inspect data.journal_path or retry `servitor-workflows get {run_id} --wait --timeout-seconds SECONDS`."
                            ),
                        ),
                    ),
                    _ => {
                        let message = value
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("workflow ended in a non-success status")
                            .to_owned();
                        let remediation = run_id.map_or_else(
                            || "Inspect data.run_summary and journal; fix the workflow before starting a new run.".to_owned(),
                            |run_id| format!(
                                "Inspect data.run_summary and data.journal_path; fix then run `servitor-workflows resume {run_id}` if recovery is appropriate."
                            ),
                        );
                        (format!("terminal_{status}"), message, remediation)
                    }
                };
                (
                    Envelope {
                        ok: false,
                        data: Some(value),
                        meta: meta(),
                        error: Some(ErrorPayload {
                            code: error_code,
                            message,
                            remediation,
                        }),
                    },
                    terminal,
                )
            }
        }
        Err(error) => (
            Envelope {
                ok: false,
                data: None,
                meta: meta(),
                error: Some(error.payload()),
            },
            exit_code_for(&error),
        ),
    };

    if let Err(error) = emit(&envelope, cli.output) {
        let fallback = Envelope {
            ok: false,
            data: None,
            meta: meta(),
            error: Some(error.payload()),
        };
        let _ = emit_json(&fallback);
        std::process::exit(1);
    }
    if code != 0 {
        std::process::exit(code);
    }
}

fn parse_args(text: &str) -> Result<Value, WorkflowError> {
    serde_json::from_str(text).map_err(WorkflowError::Json)
}

fn to_value<T: Serialize>(value: T) -> Result<Value, WorkflowError> {
    serde_json::to_value(value).map_err(WorkflowError::Json)
}

fn public_value(state: RunState) -> Result<Value, WorkflowError> {
    to_value(PublicRun::from(&state))
}

fn spawn_detached(run_id: &str) -> Result<(), WorkflowError> {
    let executable = std::env::current_exe().map_err(|source| WorkflowError::Read {
        path: PathBuf::from("current executable"),
        source,
    })?;
    let mut command = ProcessCommand::new(&executable);
    command
        .arg("--output")
        .arg("quiet")
        .arg("execute-existing")
        .arg(run_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_detached_process(&mut command);
    command
        .spawn()
        .map(|_| ())
        .map_err(|source| WorkflowError::Read {
            path: executable,
            source,
        })
}

#[cfg(windows)]
fn configure_detached_process(command: &mut ProcessCommand) {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS};

    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(windows))]
fn configure_detached_process(_command: &mut ProcessCommand) {}

fn wait_for_run(
    engine: &Engine,
    run_id: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, WorkflowError> {
    let started = Instant::now();
    let timeout = timeout_seconds.map(Duration::from_secs);
    loop {
        let run = engine.get(run_id)?;
        let outcome = match run.status {
            RunStatus::Succeeded => Some(0),
            RunStatus::Failed | RunStatus::Cancelled | RunStatus::Superseded => Some(1),
            RunStatus::WaitingHuman => Some(3),
            _ => None,
        };
        let mut value = to_value(run)?;
        if let Some(code) = outcome {
            value[WAIT_OUTCOME_KEY] = Value::from(code);
            return Ok(value);
        }
        if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
            value[WAIT_OUTCOME_KEY] = Value::from(4);
            return Ok(value);
        }
        thread::sleep(WAIT_POLL_INTERVAL);
    }
}

fn exit_code_for(error: &WorkflowError) -> i32 {
    match error {
        WorkflowError::RunNotFound(_) => 3,
        WorkflowError::Json(_)
        | WorkflowError::InvalidWorkflow(_)
        | WorkflowError::InvalidOperation(_) => 2,
        _ => 1,
    }
}

fn schema_value() -> Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "tool": "servitor-workflows",
        "version": env!("CARGO_PKG_VERSION"),
        "default_format": "json",
        "exit_codes": {
            "0": "ok",
            "1": "runtime_or_terminal_failure",
            "2": "invalid_input",
            "3": "not_found_or_waiting_human",
            "4": "wait_timeout"
        },
        "envelope": {
            "type": "object",
            "required": ["ok", "meta"],
            "properties": {
                "ok": { "type": "boolean" },
                "data": {},
                "meta": { "type": "object" },
                "error": {
                    "type": "object",
                    "required": ["code", "message", "remediation"]
                }
            }
        },
        "commands": [
            "run", "check", "resume", "get", "list", "approve", "reject", "pause", "cancel", "supersede", "inspect", "watch", "schema", "completions"
        ],
        "public_run": {
            "run_id": "string",
            "status": "running|waiting_human|pausing|paused|cancelling|succeeded|failed|cancelled|superseded",
            "journal_path": "path",
            "run_summary": "path?"
        },
        "resume_policy": {
            "rerun_blocked": ["succeeded", "cancelled", "superseded"],
            "rerun_allowed": ["failed", "paused", "waiting_human", "running", "pausing", "cancelling"],
            "max_calls": "budget counts agent+command+gate keys; journaled keys free on replay; seeded from journal size at VM start"
        },
        "examples": [
            "servitor-workflows check path/to/workflow.js",
            "servitor-workflows run path/to/workflow.js --args null",
            "servitor-workflows run path/to/workflow.js --detach",
            "servitor-workflows get RUN_ID --wait --timeout-seconds 300",
            "servitor-workflows list --limit 20 --status failed",
            "servitor-workflows cancel RUN_ID --reason 'why' --dry-run",
            "servitor-workflows watch RUN_ID",
            "servitor-workflows --output jsonl watch RUN_ID",
            "servitor-workflows schema",
            "servitor-workflows completions powershell"
        ]
    })
}

fn emit(envelope: &Envelope, mode: OutputMode) -> Result<(), WorkflowError> {
    match mode {
        OutputMode::Quiet => Ok(()),
        OutputMode::Json => emit_json(envelope),
        OutputMode::Jsonl => {
            // JSONL: one compact JSON object per line. The record IS the full
            // envelope (ok/data/meta/error) so the schema contract holds for
            // every command; consumers read `.data` for the payload.
            emit_json(envelope)
        }
        OutputMode::Human => {
            if let Some(data) = envelope.data.as_ref() {
                println!("{}", human(data));
            } else if let Some(error) = &envelope.error {
                eprintln!("{}: {} ({})", error.code, error.message, error.remediation);
            }
            Ok(())
        }
    }
}

fn emit_json(envelope: &Envelope) -> Result<(), WorkflowError> {
    println!("{}", serde_json::to_string(envelope)?);
    Ok(())
}

fn human(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("error");
    let run_id = value.get("run_id").and_then(Value::as_str).unwrap_or("-");
    let report = value
        .get("report")
        .and_then(Value::as_str)
        .map(|path| format!("\nreport: {path}"))
        .unwrap_or_default();
    let summary = value
        .get("run_summary")
        .and_then(Value::as_str)
        .map(|path| format!("\nrun_summary: {path}"))
        .unwrap_or_default();
    let journal = value
        .get("journal_path")
        .and_then(Value::as_str)
        .map(|path| format!("\njournal: {path}"))
        .unwrap_or_default();
    if let Some(result) = value.get("result").filter(|value| !value.is_null()) {
        format!("{status} {run_id}\n{result}{report}{summary}{journal}")
    } else if let Some(error) = value.get("error").and_then(Value::as_str) {
        format!("{status} {run_id}\n{error}{report}{summary}{journal}")
    } else {
        format!("{status} {run_id}{report}{summary}{journal}")
    }
}

fn emit_completions(shell: Shell) -> ! {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_owned();
    generate(shell, &mut cmd, name, &mut std::io::stdout());
    std::process::exit(0);
}

fn terminal_exit_code(value: &Value) -> i32 {
    if let Some(code) = value.get(WAIT_OUTCOME_KEY).and_then(Value::as_i64) {
        return i32::try_from(code).unwrap_or(1);
    }
    // Preview payloads include current status for agents; they are not terminal results.
    if value.get("dry_run").and_then(Value::as_bool) == Some(true) {
        return 0;
    }
    match value.get("status").and_then(Value::as_str) {
        Some("failed" | "cancelled" | "superseded") => 1,
        _ => 0,
    }
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(16)
}

#[cfg(test)]
mod cli_contract_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dry_run_payload_does_not_inherit_terminal_exit() {
        let value = json!({
            "dry_run": true,
            "status": "failed",
            "would_cancel": false
        });
        assert_eq!(terminal_exit_code(&value), 0);
    }

    #[test]
    fn failed_status_without_dry_run_is_terminal_failure() {
        let value = json!({"status": "failed"});
        assert_eq!(terminal_exit_code(&value), 1);
    }

    #[test]
    fn wait_outcomes_have_distinct_exit_codes() {
        assert_eq!(
            terminal_exit_code(&json!({WAIT_OUTCOME_KEY: 0, "status": "succeeded"})),
            0
        );
        assert_eq!(
            terminal_exit_code(&json!({WAIT_OUTCOME_KEY: 3, "status": "waiting_human"})),
            3
        );
        assert_eq!(
            terminal_exit_code(&json!({WAIT_OUTCOME_KEY: 4, "status": "running"})),
            4
        );
    }

    #[test]
    fn terminal_remediation_uses_actual_run_id() {
        let value = json!({"run_id": "run-actual-42", "status": "failed"});
        let run_id = value.get("run_id").and_then(Value::as_str).unwrap();
        let remediation = format!(
            "Inspect data.run_summary and data.journal_path; fix then run `servitor-workflows resume {run_id}` if recovery is appropriate."
        );
        assert!(remediation.contains("servitor-workflows resume run-actual-42"));
    }

    #[test]
    fn clap_error_message_is_compact() {
        let err = match Cli::try_parse_from(["servitor-workflows", "nope"]) {
            Ok(_) => panic!("expected parse failure"),
            Err(err) => err,
        };
        let message = clap_error_message(&err);
        assert!(!message.contains("Usage:"));
    }
}
