use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::Value;
use servitor_workflows::{PublicRun, RunState, default_engine};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "servitor-workflows",
    version,
    about = "Run dynamic agent workflows"
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
        } => {
            let parsed = match value {
                Some(raw) => match serde_json::from_str(&raw) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!("invalid --value JSON: {e}");
                        std::process::exit(1);
                    }
                },
                None => None,
            };
            engine
                .approve(&run_id, true, reason, parsed)
                .and_then(public_value)
        }
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
    };
    let (value, code) = match result {
        Ok(value) => {
            let code = terminal_exit_code(&value);
            (value, code)
        }
        Err(error) => (serde_json::json!({"error": error.to_string()}), 1),
    };
    if let Err(error) = emit(&value, cli.output) {
        eprintln!("{error}");
        std::process::exit(1);
    }
    if code != 0 {
        std::process::exit(code);
    }
}

fn parse_args(text: &str) -> Result<Value, servitor_workflows::WorkflowError> {
    serde_json::from_str(text).map_err(servitor_workflows::WorkflowError::Json)
}

fn to_value<T: Serialize>(value: T) -> Result<Value, servitor_workflows::WorkflowError> {
    serde_json::to_value(value).map_err(servitor_workflows::WorkflowError::Json)
}

fn public_value(state: RunState) -> Result<Value, servitor_workflows::WorkflowError> {
    to_value(PublicRun::from(&state))
}

fn emit(value: &Value, mode: OutputMode) -> Result<(), serde_json::Error> {
    match mode {
        OutputMode::Json => println!("{}", serde_json::to_string(value)?),
        OutputMode::Human => println!("{}", human(value)),
        OutputMode::Quiet => {}
    }
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
    if let Some(result) = value.get("result").filter(|value| !value.is_null()) {
        format!("{status} {run_id}\n{result}{report}")
    } else if let Some(error) = value.get("error").and_then(Value::as_str) {
        format!("{status} {run_id}\n{error}{report}")
    } else {
        format!("{status} {run_id}{report}")
    }
}

fn terminal_exit_code(value: &Value) -> i32 {
    match value.get("status").and_then(Value::as_str) {
        Some("failed" | "cancelled") => 1,
        _ => 0,
    }
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(16)
}
