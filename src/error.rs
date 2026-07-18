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

impl From<servitor::ErrorInfo> for WorkflowError {
    fn from(value: servitor::ErrorInfo) -> Self {
        Self::Transport {
            code: value.code,
            message: value.message,
        }
    }
}
