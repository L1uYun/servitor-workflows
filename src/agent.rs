use crate::json_extract::{expect_from_schema, extract_json_value};
use crate::model::{CallKind, CallState, JournalEntry};
use crate::scheduler::JobResult;
use crate::store::WorkflowStore;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use servitor::{Input, Output, RunState as ServitorState, SubmitRequest};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(250);

pub trait Transport: Send + Sync {
    fn submit(
        &self,
        request: SubmitRequest,
    ) -> Result<servitor::SubmitResponse, servitor::ErrorInfo>;
    fn inspect(&self, run_id: &str) -> Result<servitor::RunRecord, servitor::ErrorInfo>;
    fn cancel(&self, run_id: &str) -> Result<servitor::RunRecord, servitor::ErrorInfo>;
}

#[derive(Clone, Debug)]
pub struct ServitorTransport(servitor::Client);

impl ServitorTransport {
    pub fn from_environment() -> Self {
        Self(servitor::Client::from_environment())
    }
}

impl Transport for ServitorTransport {
    fn submit(
        &self,
        request: SubmitRequest,
    ) -> Result<servitor::SubmitResponse, servitor::ErrorInfo> {
        self.0.submit(request)
    }
    fn inspect(&self, run_id: &str) -> Result<servitor::RunRecord, servitor::ErrorInfo> {
        self.0.inspect(run_id)
    }
    fn cancel(&self, run_id: &str) -> Result<servitor::RunRecord, servitor::ErrorInfo> {
        self.0.cancel(run_id)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentOptions {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default = "default_agent")]
    pub agent: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub native_args: Vec<String>,
    #[serde(default)]
    pub schema: Option<Value>,
}

fn default_agent() -> String {
    "pi".to_owned()
}

pub(crate) struct AgentCall {
    pub key: String,
    pub label: String,
    prompt: String,
    options: AgentOptions,
}

impl AgentCall {
    pub fn new(key: String, prompt: String, options: AgentOptions) -> Self {
        let label = options.label.clone().unwrap_or_else(|| key.clone());
        Self {
            key,
            label,
            prompt,
            options,
        }
    }
}

pub(crate) fn run(
    store: &WorkflowStore,
    transport: &dyn Transport,
    run_id: &str,
    default_cwd: &Path,
    call: AgentCall,
) -> JobResult {
    let AgentCall {
        key,
        label,
        prompt,
        options,
    } = call;
    let append = |state, result, error, transport_run_id| {
        store
            .append(
                run_id,
                &JournalEntry {
                    at: Utc::now(),
                    key: key.clone(),
                    kind: CallKind::Agent,
                    state,
                    label: label.clone(),
                    result,
                    error,
                    transport_run_id,
                },
            )
            .map_err(|error| error.to_string())
    };
    let existing = store
        .journal_index(run_id)
        .map_err(|error| error.to_string())?
        .remove(&key);
    if let Some(entry) = existing.as_ref() {
        match entry.state {
            CallState::Succeeded => return Ok(entry.result.clone().unwrap_or(Value::Null)),
            CallState::Failed => {
                if let Some(transport_run_id) = entry.transport_run_id.as_deref()
                    && let Ok(Some(result)) =
                        try_recover_structured(transport, transport_run_id, options.schema.as_ref())
                {
                    append(
                        CallState::Succeeded,
                        Some(result.clone()),
                        None,
                        Some(transport_run_id.to_owned()),
                    )?;
                    return Ok(result);
                }
            }
            CallState::Submitted | CallState::Cancelled => {}
        }
    }
    let transport_run_id =
        if let Some(entry) = existing.filter(|entry| entry.state == CallState::Submitted) {
            entry
                .transport_run_id
                .ok_or_else(|| "submitted agent call has no Servitor run id".to_owned())?
        } else {
            let response = transport
                .submit(SubmitRequest {
                    agent: options.agent.clone(),
                    model: options.model.clone(),
                    input: Input::Text {
                        text: structured_prompt(&prompt, options.schema.as_ref()),
                    },
                    cwd: resolve_cwd(default_cwd, options.cwd.as_deref()),
                    system_prompt: options.system_prompt.clone(),
                    continuation: None,
                    timeout_seconds: options.timeout_seconds,
                    native_args: options.native_args.clone(),
                })
                .map_err(|error| format!("{}: {}", error.code, error.message))?;
            append(
                CallState::Submitted,
                None,
                None,
                Some(response.run_id.clone()),
            )?;
            response.run_id
        };

    loop {
        if store.cancel_requested(run_id) || store.pause_requested(run_id) {
            let _ = transport.cancel(&transport_run_id);
            append(
                CallState::Cancelled,
                None,
                Some("interrupted".to_owned()),
                Some(transport_run_id),
            )?;
            return Err("workflow interrupted".to_owned());
        }
        let record = transport
            .inspect(&transport_run_id)
            .map_err(|error| format!("{}: {}", error.code, error.message))?;
        match record.state {
            ServitorState::Accepted | ServitorState::Running => thread::sleep(POLL_INTERVAL),
            ServitorState::Succeeded => {
                let text = output_text(record.output.as_ref());
                match parse_output(&text, options.schema.as_ref()) {
                    Ok(result) => {
                        append(
                            CallState::Succeeded,
                            Some(result.clone()),
                            None,
                            Some(transport_run_id),
                        )?;
                        return Ok(result);
                    }
                    Err(error) => {
                        append(
                            CallState::Failed,
                            None,
                            Some(error.clone()),
                            Some(transport_run_id),
                        )?;
                        return Err(error);
                    }
                }
            }
            ServitorState::Failed | ServitorState::Cancelled => {
                let message = record
                    .error
                    .map(|error| format!("{}: {}", error.code, error.message))
                    .unwrap_or_else(|| format!("Servitor run ended as {:?}", record.state));
                let state = if record.state == ServitorState::Cancelled {
                    CallState::Cancelled
                } else {
                    CallState::Failed
                };
                append(state, None, Some(message.clone()), Some(transport_run_id))?;
                return Err(message);
            }
        }
    }
}

fn structured_prompt(prompt: &str, schema: Option<&Value>) -> String {
    schema.map_or_else(
        || prompt.to_owned(),
        |schema| format!("{prompt}\n\nReturn only valid JSON matching this JSON Schema:\n{schema}"),
    )
}

fn try_recover_structured(
    transport: &dyn Transport,
    transport_run_id: &str,
    schema: Option<&Value>,
) -> Result<Option<Value>, String> {
    let record = transport
        .inspect(transport_run_id)
        .map_err(|error| format!("{}: {}", error.code, error.message))?;
    if record.state != ServitorState::Succeeded {
        return Ok(None);
    }
    let text = output_text(record.output.as_ref());
    match parse_output(&text, schema) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

fn output_text(output: Option<&Output>) -> String {
    match output {
        Some(Output::Text { text }) => text.clone(),
        Some(Output::Image { paths }) => paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join("\n"),
        None => String::new(),
    }
}

fn parse_output(text: &str, schema: Option<&Value>) -> JobResult {
    let Some(schema) = schema else {
        return Ok(Value::String(text.to_owned()));
    };
    let value = extract_json_value(text, expect_from_schema(Some(schema)))
        .map_err(|error| format!("agent output is not JSON: {error}"))?;
    validate_schema(&value, schema, "$")?;
    Ok(value)
}

fn validate_schema(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    if let Some(expected) = schema.get("type").and_then(Value::as_str) {
        let matches = match expected {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            other => return Err(format!("unsupported schema type {other}")),
        };
        if !matches {
            return Err(format!("{path} must be {expected}"));
        }
    }
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{path} must be an object"))?;
        for name in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(name) {
                return Err(format!("{path}.{name} is required"));
            }
        }
    }
    if let (Some(object), Some(properties)) = (
        value.as_object(),
        schema.get("properties").and_then(Value::as_object),
    ) {
        for (name, child_schema) in properties {
            if let Some(child) = object.get(name) {
                validate_schema(child, child_schema, &format!("{path}.{name}"))?;
            }
        }
    }
    if let (Some(items), Some(item_schema)) = (value.as_array(), schema.get("items")) {
        for (index, item) in items.iter().enumerate() {
            validate_schema(item, item_schema, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn resolve_cwd(default: &Path, requested: Option<&Path>) -> PathBuf {
    match requested {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => default.join(path),
        None => default.to_path_buf(),
    }
}
