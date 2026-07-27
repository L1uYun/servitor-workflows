use crate::error::WorkflowError;
use crate::model::{CallKind, CallState, JournalEntry};
use crate::scheduler::JobResult;
use crate::store::WorkflowStore;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const OUTPUT_LIMIT: u64 = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandOptions {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

pub(crate) struct CommandCall {
    pub key: String,
    pub label: String,
    program: String,
    args: Vec<String>,
    options: CommandOptions,
    pub phase: Option<String>,
}

impl CommandCall {
    pub fn new(
        key: String,
        program: String,
        args: Vec<String>,
        options: CommandOptions,
        phase: Option<String>,
    ) -> Self {
        let label = options.label.clone().unwrap_or_else(|| key.clone());
        Self {
            key,
            label,
            program,
            args,
            options,
            phase,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandResult {
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub timed_out: bool,
    pub duration_ms: u64,
}

impl CommandResult {
    fn into_json(self) -> Value {
        serde_json::to_value(self)
            .unwrap_or_else(|_| json!({"error": "command result serialization failed"}))
    }
}

pub(crate) fn run(
    store: &WorkflowStore,
    run_id: &str,
    default_cwd: &Path,
    call: CommandCall,
) -> JobResult {
    let CommandCall {
        key,
        label,
        program,
        args,
        options,
        phase,
    } = call;
    if let Some(entry) = store
        .journal_index(run_id)
        .map_err(|error| error.to_string())?
        .remove(&key)
    {
        match entry.state {
            CallState::Succeeded => return Ok(entry.result.unwrap_or(Value::Null)),
            CallState::Failed | CallState::Submitted | CallState::Cancelled => {}
        }
    }
    let journal = CommandJournal {
        store,
        run_id,
        key: &key,
        label: &label,
        phase: phase.clone(),
    };
    journal.append(CallState::Submitted, None, None, None)?;
    let call_dir = store
        .run_dir(run_id)
        .join("commands")
        .join(key.replace('#', "-"));
    fs::create_dir_all(&call_dir).map_err(|error| error.to_string())?;
    let stdout_path = call_dir.join("stdout.txt");
    let stderr_path = call_dir.join("stderr.txt");
    let stdout = fs::File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = fs::File::create(&stderr_path).map_err(|error| error.to_string())?;
    let resolved_cwd = resolve_cwd(default_cwd, options.cwd.as_deref());
    let argv: Vec<String> = std::iter::once(program.clone())
        .chain(args.iter().cloned())
        .collect();
    let mut command = Command::new(&program);
    command
        .args(&args)
        .envs(&options.env)
        .current_dir(&resolved_cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    hide_window(&mut command);
    let mut child = command.spawn().map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if store.cancel_requested(run_id) || store.pause_requested(run_id) {
            let _ = child.kill();
            let _ = child.wait();
            journal.append(
                CallState::Cancelled,
                None,
                Some("interrupted".to_owned()),
                None,
            )?;
            return Err("workflow interrupted".to_owned());
        }
        if options
            .timeout_seconds
            .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
        {
            timed_out = true;
            let _ = child.kill();
            let status = child.wait().map_err(|error| error.to_string())?;
            break status;
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(POLL_INTERVAL),
            Err(error) => {
                return fail(&journal, error.to_string());
            }
        }
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    let (stdout, stdout_truncated) = read_tail(&stdout_path).map_err(|error| error.to_string())?;
    let (stderr, stderr_truncated) = read_tail(&stderr_path).map_err(|error| error.to_string())?;
    let result = CommandResult {
        argv,
        cwd: resolved_cwd,
        exit_code: status.code(),
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
        timed_out,
        duration_ms,
    };
    let result_json = result.into_json();
    write_command_result(store, run_id, &key, &result_json)?;
    if timed_out {
        return fail_with_result(
            &journal,
            "command timed out".to_owned(),
            result_json,
            Some(duration_ms),
        );
    }
    if status.success() {
        journal.append(
            CallState::Succeeded,
            Some(result_json.clone()),
            None,
            Some(duration_ms),
        )?;
        Ok(result_json)
    } else {
        fail_with_result(
            &journal,
            format!(
                "command exited with {:?}: {}",
                status.code(),
                result_json["stderr"].as_str().unwrap_or_default().trim()
            ),
            result_json,
            Some(duration_ms),
        )
    }
}

fn write_command_result(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    result: &Value,
) -> Result<(), String> {
    let path = store.command_result_path(run_id, key);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(result).map_err(|error| error.to_string())?;
    fs::write(&tmp, bytes).map_err(|error| error.to_string())?;
    fs::rename(&tmp, &path).map_err(|error| error.to_string())?;
    Ok(())
}

fn fail_with_result(
    journal: &CommandJournal<'_>,
    message: String,
    result: Value,
    duration_ms: Option<u64>,
) -> JobResult {
    journal.append(
        CallState::Failed,
        Some(result),
        Some(message.clone()),
        duration_ms,
    )?;
    Err(message)
}

fn fail(journal: &CommandJournal<'_>, message: String) -> JobResult {
    journal.append(CallState::Failed, None, Some(message.clone()), None)?;
    Err(message)
}

struct CommandJournal<'a> {
    store: &'a WorkflowStore,
    run_id: &'a str,
    key: &'a str,
    label: &'a str,
    phase: Option<String>,
}

impl CommandJournal<'_> {
    fn append(
        &self,
        state: CallState,
        result: Option<Value>,
        error: Option<String>,
        duration_ms: Option<u64>,
    ) -> Result<(), String> {
        self.store
            .append(
                self.run_id,
                &JournalEntry {
                    at: Utc::now(),
                    key: self.key.to_owned(),
                    kind: CallKind::Command,
                    state,
                    label: self.label.to_owned(),
                    result,
                    error,
                    transport_run_id: None,
                    phase: self.phase.clone(),
                    duration_ms,
                    usage: None,
                    schema_correction: None,
                },
            )
            .map_err(|error| error.to_string())
    }
}

fn resolve_cwd(default: &Path, requested: Option<&Path>) -> PathBuf {
    match requested {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => default.join(path),
        None => default.to_path_buf(),
    }
}

fn read_tail(path: &Path) -> Result<(String, bool), WorkflowError> {
    let mut file = fs::File::open(path).map_err(|source| WorkflowError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let length = file
        .metadata()
        .map_err(|source| WorkflowError::Read {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    let truncated = length > OUTPUT_LIMIT;
    if truncated {
        file.seek(SeekFrom::Start(length - OUTPUT_LIMIT))
            .map_err(|source| WorkflowError::Read {
                path: path.to_path_buf(),
                source,
            })?;
    }
    let mut bytes = Vec::with_capacity(length.min(OUTPUT_LIMIT) as usize);
    file.read_to_end(&mut bytes)
        .map_err(|source| WorkflowError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    Ok((String::from_utf8_lossy(&bytes).into_owned(), truncated))
}

#[cfg(windows)]
fn hide_window(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
}
#[cfg(not(windows))]
fn hide_window(_command: &mut Command) {}
