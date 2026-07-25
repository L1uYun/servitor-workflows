use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::Value;
use servitor_workflows::{ErrorPayload, PublicRun, RunState, WorkflowError, default_engine};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "servitor-workflows",
    version,
    about = "Run dynamic agent workflows",
    after_help = "Examples:\n  servitor-workflows run workflow.js --args '{\"x\":1}'\n  servitor-workflows get RUN_ID\n  servitor-workflows resume RUN_ID\n  servitor-workflows approve RUN_ID --reason ok --value '{\"path\":\"a.md\"}'\n  servitor-workflows schema"
)]
struct Cli {
    #[arg(long, global = true, value_enum, default_value_t = OutputMode::Json)]
    output: OutputMode,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum OutputMode {
    Json,
    Human,
    Quiet,
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
    },
    Resume {
        run_id: String,
    },
    Get {
        run_id: String,
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
    },
    Cancel {
        run_id: String,
    },
    Supersede {
        run_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        evidence: Option<String>,
        #[arg(long)]
        new_contract: Option<String>,
    },
    Inspect {
        run_id: String,
    },
    /// Export agent-native JSON contracts for commands and envelopes.
    Schema,
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

fn main() {
    let cli = Cli::parse();
    let engine = default_engine();
    let result = match cli.command {
        Command::Run {
            workflow,
            args,
            max_parallel,
            max_calls,
        } => parse_args(&args)
            .and_then(|args| engine.start(&workflow, args, max_parallel, max_calls))
            .and_then(public_value),
        Command::Resume { run_id } => engine.resume(&run_id).and_then(public_value),
        Command::Get { run_id } => engine.get(&run_id).and_then(to_value),
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
        Command::Pause { run_id } => engine.pause(&run_id).and_then(public_value),
        Command::Cancel { run_id } => engine.cancel(&run_id).and_then(public_value),
        Command::Supersede {
            run_id,
            reason,
            evidence,
            new_contract,
        } => engine
            .supersede(&run_id, reason, evidence, new_contract)
            .and_then(public_value),
        Command::Inspect { run_id } => engine.inspect(&run_id).and_then(to_value),
        Command::Schema => Ok(schema_value()),
    };

    let envelope = match result {
        Ok(value) => {
            let code = terminal_exit_code(&value);
            if code == 0 {
                Envelope {
                    ok: true,
                    data: Some(value),
                    meta: meta(),
                    error: None,
                }
            } else {
                let status = value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed")
                    .to_owned();
                let message = value
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("workflow ended in a non-success status")
                    .to_owned();
                Envelope {
                    ok: false,
                    data: Some(value),
                    meta: meta(),
                    error: Some(ErrorPayload {
                        code: format!("terminal_{status}"),
                        message,
                        remediation: "Inspect data.run_summary and journal; fix the workflow then start a new run, or resume if status is failed.".into(),
                    }),
                }
            }
        }
        Err(error) => Envelope {
            ok: false,
            data: None,
            meta: meta(),
            error: Some(error.payload()),
        },
    };

    let code = if envelope.ok { 0 } else { 1 };
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

fn schema_value() -> Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "tool": "servitor-workflows",
        "version": env!("CARGO_PKG_VERSION"),
        "default_format": "json",
        "envelope": {
            "type": "object",
            "required": ["ok", "meta"],
            "properties": {
                "ok": { "type": "boolean" },
                "data": {},
                "meta": {
                    "type": "object",
                    "required": ["tool", "version"],
                    "properties": {
                        "tool": { "type": "string" },
                        "version": { "type": "string" }
                    }
                },
                "error": {
                    "type": "object",
                    "required": ["code", "message", "remediation"],
                    "properties": {
                        "code": { "type": "string" },
                        "message": { "type": "string" },
                        "remediation": { "type": "string" }
                    }
                }
            }
        },
        "commands": [
            "run", "resume", "get", "approve", "reject", "pause", "cancel", "supersede", "inspect", "schema"
        ],
        "public_run": {
            "run_id": "string",
            "status": "running|waiting_human|pausing|paused|cancelling|succeeded|failed|cancelled|superseded",
            "phase": "string?",
            "active": "map?",
            "gate": "object?",
            "supersede": "object?",
            "result": "any?",
            "error": "string?",
            "report": "path?",
            "run_summary": "path?"
        },
        "resume_policy": {
            "rerun_blocked": ["succeeded", "cancelled", "superseded"],
            "rerun_allowed": ["failed", "paused", "waiting_human", "running", "pausing", "cancelling"]
        },
        "examples": [
            "servitor-workflows run path/to/workflow.js --args null",
            "servitor-workflows get RUN_ID",
            "servitor-workflows resume RUN_ID",
            "servitor-workflows approve RUN_ID --reason ok --value '{\"x\":1}'",
            "servitor-workflows schema"
        ]
    })
}

fn emit(envelope: &Envelope, mode: OutputMode) -> Result<(), WorkflowError> {
    match mode {
        OutputMode::Quiet => Ok(()),
        OutputMode::Json => emit_json(envelope),
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
    if let Some(result) = value.get("result").filter(|value| !value.is_null()) {
        format!("{status} {run_id}\n{result}{report}{summary}")
    } else if let Some(error) = value.get("error").and_then(Value::as_str) {
        format!("{status} {run_id}\n{error}{report}{summary}")
    } else {
        format!("{status} {run_id}{report}{summary}")
    }
}

fn terminal_exit_code(value: &Value) -> i32 {
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
