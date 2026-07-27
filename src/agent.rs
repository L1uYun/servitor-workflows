use crate::json_extract::extract_json_value_for_schema;
use crate::model::{CallKind, CallState, JournalEntry, SchemaCorrectionMetadata};
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
const INVALID_OUTPUT_EXCERPT_MAX_CHARS: usize = 2_000;
type RecoveredStructuredOutput = (Value, Option<u64>, Option<Value>);

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
    pub phase: Option<String>,
}

impl AgentCall {
    pub fn new(key: String, prompt: String, options: AgentOptions, phase: Option<String>) -> Self {
        let label = options.label.clone().unwrap_or_else(|| key.clone());
        Self {
            key,
            label,
            prompt,
            options,
            phase,
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
        phase,
    } = call;
    let append = |state, result, error, transport_run_id, duration_ms, usage, schema_correction| {
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
                    phase: phase.clone(),
                    duration_ms,
                    usage,
                    schema_correction,
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
            CallState::Failed if correction_exhausted(entry) => {
                if let Some(transport_run_id) = entry.transport_run_id.as_deref()
                    && let Ok(Some((result, duration_ms, usage))) =
                        try_recover_structured(transport, transport_run_id, options.schema.as_ref())
                {
                    append(
                        CallState::Succeeded,
                        Some(result.clone()),
                        None,
                        Some(transport_run_id.to_owned()),
                        duration_ms,
                        usage,
                        entry.schema_correction.clone(),
                    )?;
                    return Ok(result);
                }
                return Err(entry
                    .error
                    .clone()
                    .unwrap_or_else(|| "schema correction was already exhausted".to_owned()));
            }
            CallState::Failed | CallState::Submitted | CallState::Cancelled => {}
        }
    }

    let resumed_correction = existing
        .as_ref()
        .filter(|entry| entry.state == CallState::Submitted && correction_exhausted(entry))
        .and_then(|entry| entry.schema_correction.clone());
    let (first_run_id, first_record) = match existing.as_ref() {
        Some(entry) if matches!(entry.state, CallState::Submitted | CallState::Failed) => {
            let transport_run_id = entry
                .transport_run_id
                .clone()
                .ok_or_else(|| "journaled agent call has no Servitor run id".to_owned())?;
            let record = if entry.state == CallState::Failed {
                Some(
                    transport
                        .inspect(&transport_run_id)
                        .map_err(|error| format!("{}: {}", error.code, error.message))?,
                )
            } else {
                None
            };
            (transport_run_id, record)
        }
        _ => {
            let response = submit(
                transport,
                &options,
                default_cwd,
                structured_prompt(&prompt, options.schema.as_ref()),
            )?;
            append(
                CallState::Submitted,
                None,
                None,
                Some(response.run_id.clone()),
                None,
                None,
                None,
            )?;
            (response.run_id, None)
        }
    };

    let first_record = match first_record {
        Some(record) => record,
        None => match wait_for_terminal(store, transport, run_id, &first_run_id) {
            Ok(record) => record,
            Err(error) => {
                append(
                    CallState::Failed,
                    None,
                    Some(error.clone()),
                    Some(first_run_id),
                    None,
                    None,
                    resumed_correction,
                )?;
                return Err(error);
            }
        },
    };
    if first_record.state != ServitorState::Succeeded {
        return finish_transport_failure(&append, first_run_id, first_record, resumed_correction);
    }

    let (first_duration_ms, first_usage) = record_metrics(&first_record);
    if let Some(mut metadata) = resumed_correction {
        return match materialize_output(first_record.output.as_ref(), options.schema.as_ref()) {
            Ok(result) => {
                append(
                    CallState::Succeeded,
                    Some(result.clone()),
                    None,
                    Some(first_run_id),
                    first_duration_ms,
                    first_usage,
                    Some(metadata),
                )?;
                Ok(result)
            }
            Err(second_error) => {
                let first_error = metadata
                    .validation_errors
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "unknown initial schema validation error".to_owned());
                metadata.validation_errors.push(second_error.clone());
                let combined = format!(
                    "schema validation failed after one correction; first: {first_error}; correction: {second_error}"
                );
                append(
                    CallState::Failed,
                    None,
                    Some(combined.clone()),
                    Some(first_run_id),
                    first_duration_ms,
                    first_usage,
                    Some(metadata),
                )?;
                Err(combined)
            }
        };
    }
    match materialize_output(first_record.output.as_ref(), options.schema.as_ref()) {
        Ok(result) => {
            append(
                CallState::Succeeded,
                Some(result.clone()),
                None,
                Some(first_run_id),
                first_duration_ms,
                first_usage,
                None,
            )?;
            Ok(result)
        }
        Err(first_error) if options.schema.is_some() => {
            let invalid_output = output_text(first_record.output.as_ref());
            let correction_prompt = correction_prompt(
                &prompt,
                options.schema.as_ref().expect("schema checked"),
                &first_error,
                &invalid_output,
            );
            let correction = match submit(transport, &options, default_cwd, correction_prompt) {
                Ok(response) => response,
                Err(error) => {
                    let metadata = SchemaCorrectionMetadata {
                        attempted: true,
                        transport_run_ids: vec![first_run_id.clone()],
                        validation_errors: vec![first_error],
                    };
                    append(
                        CallState::Failed,
                        None,
                        Some(error.clone()),
                        Some(first_run_id),
                        first_duration_ms,
                        first_usage,
                        Some(metadata),
                    )?;
                    return Err(error);
                }
            };
            let correction_run_id = correction.run_id;
            let mut metadata = SchemaCorrectionMetadata {
                attempted: true,
                transport_run_ids: vec![first_run_id, correction_run_id.clone()],
                validation_errors: vec![first_error.clone()],
            };
            append(
                CallState::Submitted,
                None,
                Some(first_error.clone()),
                Some(correction_run_id.clone()),
                None,
                None,
                Some(metadata.clone()),
            )?;
            let correction_record =
                match wait_for_terminal(store, transport, run_id, &correction_run_id) {
                    Ok(record) => record,
                    Err(error) => {
                        append(
                            CallState::Failed,
                            None,
                            Some(error.clone()),
                            Some(correction_run_id),
                            None,
                            None,
                            Some(metadata),
                        )?;
                        return Err(error);
                    }
                };
            if correction_record.state != ServitorState::Succeeded {
                return finish_transport_failure(
                    &append,
                    correction_run_id,
                    correction_record,
                    Some(metadata),
                );
            }
            let (duration_ms, usage) = record_metrics(&correction_record);
            match materialize_output(correction_record.output.as_ref(), options.schema.as_ref()) {
                Ok(result) => {
                    append(
                        CallState::Succeeded,
                        Some(result.clone()),
                        None,
                        Some(correction_run_id),
                        duration_ms,
                        usage,
                        Some(metadata),
                    )?;
                    Ok(result)
                }
                Err(second_error) => {
                    metadata.validation_errors.push(second_error.clone());
                    let combined = format!(
                        "schema validation failed after one correction; first: {first_error}; correction: {second_error}"
                    );
                    append(
                        CallState::Failed,
                        None,
                        Some(combined.clone()),
                        Some(correction_run_id),
                        duration_ms,
                        usage,
                        Some(metadata),
                    )?;
                    Err(combined)
                }
            }
        }
        Err(error) => {
            append(
                CallState::Failed,
                None,
                Some(error.clone()),
                Some(first_run_id),
                first_duration_ms,
                first_usage,
                None,
            )?;
            Err(error)
        }
    }
}

fn correction_exhausted(entry: &JournalEntry) -> bool {
    entry
        .schema_correction
        .as_ref()
        .is_some_and(|metadata| metadata.attempted)
}

fn submit(
    transport: &dyn Transport,
    options: &AgentOptions,
    default_cwd: &Path,
    text: String,
) -> Result<servitor::SubmitResponse, String> {
    transport
        .submit(SubmitRequest {
            agent: options.agent.clone(),
            model: options.model.clone(),
            input: Input::Text { text },
            cwd: resolve_cwd(default_cwd, options.cwd.as_deref()),
            system_prompt: options.system_prompt.clone(),
            continuation: None,
            timeout_seconds: options.timeout_seconds,
            native_args: options.native_args.clone(),
        })
        .map_err(|error| format!("{}: {}", error.code, error.message))
}

fn wait_for_terminal(
    store: &WorkflowStore,
    transport: &dyn Transport,
    workflow_run_id: &str,
    transport_run_id: &str,
) -> Result<servitor::RunRecord, String> {
    loop {
        if store.cancel_requested(workflow_run_id) || store.pause_requested(workflow_run_id) {
            let _ = transport.cancel(transport_run_id);
            return Err("workflow interrupted".to_owned());
        }
        let record = transport
            .inspect(transport_run_id)
            .map_err(|error| format!("{}: {}", error.code, error.message))?;
        match record.state {
            ServitorState::Accepted | ServitorState::Running => thread::sleep(POLL_INTERVAL),
            _ => return Ok(record),
        }
    }
}

fn finish_transport_failure<F>(
    append: &F,
    transport_run_id: String,
    record: servitor::RunRecord,
    schema_correction: Option<SchemaCorrectionMetadata>,
) -> JobResult
where
    F: Fn(
        CallState,
        Option<Value>,
        Option<String>,
        Option<String>,
        Option<u64>,
        Option<Value>,
        Option<SchemaCorrectionMetadata>,
    ) -> Result<(), String>,
{
    let (duration_ms, usage) = record_metrics(&record);
    let message = record
        .error
        .map(|error| format!("{}: {}", error.code, error.message))
        .unwrap_or_else(|| format!("Servitor run ended as {:?}", record.state));
    let state = if record.state == ServitorState::Cancelled {
        CallState::Cancelled
    } else {
        CallState::Failed
    };
    append(
        state,
        None,
        Some(message.clone()),
        Some(transport_run_id),
        duration_ms,
        usage,
        schema_correction,
    )?;
    Err(message)
}

fn correction_prompt(prompt: &str, schema: &Value, error: &str, invalid: &str) -> String {
    let excerpt: String = invalid
        .chars()
        .take(INVALID_OUTPUT_EXCERPT_MAX_CHARS)
        .collect();
    format!(
        "Correct the JSON response for the original task below.\n\nOriginal task:\n{prompt}\n\nJSON Schema:\n{schema}\n\nValidation error:\n{error}\n\nInvalid output excerpt (bounded):\n{excerpt}\n\nReturn only corrected JSON matching the schema. Do not include Markdown fences, prose, or explanation."
    )
}

fn structured_prompt(prompt: &str, schema: Option<&Value>) -> String {
    schema.map_or_else(
        || prompt.to_owned(),
        |schema| format!("{prompt}\n\nReturn only valid JSON matching this JSON Schema.\nDo not wrap the JSON in Markdown fences or prose.\n{schema}"),
    )
}

fn try_recover_structured(
    transport: &dyn Transport,
    transport_run_id: &str,
    schema: Option<&Value>,
) -> Result<Option<RecoveredStructuredOutput>, String> {
    let record = transport
        .inspect(transport_run_id)
        .map_err(|error| format!("{}: {}", error.code, error.message))?;
    if record.state != ServitorState::Succeeded {
        return Ok(None);
    }
    let (duration_ms, usage) = record_metrics(&record);
    match materialize_output(record.output.as_ref(), schema) {
        Ok(value) => Ok(Some((value, duration_ms, usage))),
        Err(_) => Ok(None),
    }
}

fn record_metrics(record: &servitor::RunRecord) -> (Option<u64>, Option<Value>) {
    let duration_ms = match (record.started_at, record.finished_at) {
        (Some(start), Some(end)) => u64::try_from((end - start).num_milliseconds()).ok(),
        _ => None,
    };
    let usage = record.diagnostics.provider.get("usage").cloned();
    (duration_ms, usage)
}

fn materialize_output(output: Option<&Output>, schema: Option<&Value>) -> JobResult {
    if matches!(output, Some(Output::Image { .. })) && schema.is_some() {
        return Err(
            "agent produced image output but a JSON schema was requested; use text agent output"
                .to_owned(),
        );
    }
    let text = output_text(output);
    parse_output(&text, schema)
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
    // Schema participates in candidate selection (last schema-valid value),
    // not only as a post-check on the first shape match.
    extract_json_value_for_schema(text, schema)
        .map_err(|error| format!("agent output is not JSON: {error}"))
}

fn resolve_cwd(default: &Path, requested: Option<&Path>) -> PathBuf {
    match requested {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => default.join(path),
        None => default.to_path_buf(),
    }
}

#[cfg(test)]
mod materialize_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn image_output_with_schema_is_rejected() {
        let schema = json!({"type":"object"});
        let output = Output::Image {
            paths: vec![PathBuf::from("x.png")],
        };
        let err = materialize_output(Some(&output), Some(&schema)).unwrap_err();
        assert!(err.contains("image output"), "{err}");
    }

    #[test]
    fn text_json_with_schema_passes() {
        let schema = json!({"type":"object","required":["ok"]});
        let output = Output::Text {
            text: r#"{"ok":true}"#.into(),
        };
        let value = materialize_output(Some(&output), Some(&schema)).unwrap();
        assert_eq!(value["ok"], true);
    }
}
