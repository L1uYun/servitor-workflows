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
}

impl CommandCall {
    pub fn new(key: String, program: String, args: Vec<String>, options: CommandOptions) -> Self {
        let label = options.label.clone().unwrap_or_else(|| key.clone());
        Self {
            key,
            label,
            program,
            args,
            options,
        }
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
    } = call;
    if let Some(entry) = store
        .journal_index(run_id)
        .map_err(|error| error.to_string())?
        .remove(&key)
    {
        match entry.state {
            CallState::Succeeded => return Ok(entry.result.unwrap_or(Value::Null)),
            CallState::Failed => {
                return Err(entry
                    .error
                    .unwrap_or_else(|| "cached command failure".to_owned()));
            }
            CallState::Submitted | CallState::Cancelled => {}
        }
    }
    append(
        store,
        run_id,
        &key,
        &label,
        CallState::Submitted,
        None,
        None,
    )?;
    let call_dir = store
        .run_dir(run_id)
        .join("commands")
        .join(key.replace('#', "-"));
    fs::create_dir_all(&call_dir).map_err(|error| error.to_string())?;
    let stdout_path = call_dir.join("stdout.txt");
    let stderr_path = call_dir.join("stderr.txt");
    let stdout = fs::File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = fs::File::create(&stderr_path).map_err(|error| error.to_string())?;
    let mut command = Command::new(&program);
    command
        .args(&args)
        .envs(&options.env)
        .current_dir(resolve_cwd(default_cwd, options.cwd.as_deref()))
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    hide_window(&mut command);
    let mut child = command.spawn().map_err(|error| error.to_string())?;
    let started = Instant::now();
    let status = loop {
        if store.cancel_requested(run_id) || store.pause_requested(run_id) {
            let _ = child.kill();
            let _ = child.wait();
            append(
                store,
                run_id,
                &key,
                &label,
                CallState::Cancelled,
                None,
                Some("interrupted".to_owned()),
            )?;
            return Err("workflow interrupted".to_owned());
        }
        if options
            .timeout_seconds
            .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
        {
            let _ = child.kill();
            let _ = child.wait();
            return fail(store, run_id, &key, &label, "command timed out".to_owned());
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(POLL_INTERVAL),
            Err(error) => return fail(store, run_id, &key, &label, error.to_string()),
        }
    };
    let stdout = read_tail(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = read_tail(&stderr_path).map_err(|error| error.to_string())?;
    let result = json!({"exitCode": status.code(), "stdout": stdout, "stderr": stderr});
    if status.success() {
        append(
            store,
            run_id,
            &key,
            &label,
            CallState::Succeeded,
            Some(result.clone()),
            None,
        )?;
        Ok(result)
    } else {
        fail(
            store,
            run_id,
            &key,
            &label,
            format!("command exited with {:?}: {}", status.code(), stderr.trim()),
        )
    }
}

fn fail(store: &WorkflowStore, run_id: &str, key: &str, label: &str, message: String) -> JobResult {
    append(
        store,
        run_id,
        key,
        label,
        CallState::Failed,
        None,
        Some(message.clone()),
    )?;
    Err(message)
}

fn append(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    label: &str,
    state: CallState,
    result: Option<Value>,
    error: Option<String>,
) -> Result<(), String> {
    store
        .append(
            run_id,
            &JournalEntry {
                at: Utc::now(),
                key: key.to_owned(),
                kind: CallKind::Command,
                state,
                label: label.to_owned(),
                result,
                error,
                transport_run_id: None,
            },
        )
        .map_err(|error| error.to_string())
}

fn resolve_cwd(default: &Path, requested: Option<&Path>) -> PathBuf {
    match requested {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => default.join(path),
        None => default.to_path_buf(),
    }
}

fn read_tail(path: &Path) -> Result<String, WorkflowError> {
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
    if length > OUTPUT_LIMIT {
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
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(windows)]
fn hide_window(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
}
#[cfg(not(windows))]
fn hide_window(_command: &mut Command) {}
