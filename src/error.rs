use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("run not found: {0}")]
    RunNotFound(String),
    #[error("invalid workflow: {0}")]
    InvalidWorkflow(String),
    #[error("invalid run operation: {0}")]
    InvalidOperation(String),
    #[error("JavaScript error: {0}")]
    JavaScript(String),
    #[error("transport error {code}: {message}")]
    Transport { code: String, message: String },
    #[error("workflow invariant failed: {0}")]
    Invariant(String),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    pub remediation: String,
}

impl WorkflowError {
    pub fn payload(&self) -> ErrorPayload {
        let (code, remediation) = match self {
            Self::Read { .. } => (
                "read_failed",
                "Check the workflow path exists and is readable.",
            ),
            Self::Write { .. } => (
                "write_failed",
                "Ensure SERVITOR_WORKFLOWS_STATE_ROOT is writable.",
            ),
            Self::Json(_) => (
                "invalid_json",
                "Pass valid JSON (for --args/--value) matching `servitor-workflows schema`.",
            ),
            Self::RunNotFound(_) => (
                "run_not_found",
                "Use a run_id returned by `servitor-workflows run`, or inspect state under the workflows state root.",
            ),
            Self::InvalidWorkflow(_) => (
                "invalid_workflow",
                "Workflow must export `export const meta` and only use host primitives (agent/command/gate/...).",
            ),
            Self::InvalidOperation(_) => (
                "invalid_operation",
                "Check run status with `servitor-workflows get RUN_ID` before approve/resume/cancel/supersede.",
            ),
            Self::JavaScript(_) => (
                "javascript_error",
                "Fix the workflow script; inspect journal and run_summary for the failing call.",
            ),
            Self::Transport { .. } => (
                "transport_error",
                "Inspect the nested transport run with `servitor inspect <transport_run_id>` and `servitor doctor`.",
            ),
            Self::Invariant(_) => (
                "invariant",
                "State is inconsistent; inspect the run directory and open a bug if it reproduces.",
            ),
        };
        ErrorPayload {
            code: code.to_owned(),
            message: self.to_string(),
            remediation: remediation.to_owned(),
        }
    }
}

impl From<servitor::ErrorInfo> for WorkflowError {
    fn from(value: servitor::ErrorInfo) -> Self {
        Self::Transport {
            code: value.code,
            message: value.message,
        }
    }
}
