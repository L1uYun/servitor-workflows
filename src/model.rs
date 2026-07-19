use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    WaitingHuman,
    Pausing,
    Paused,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Superseded,
}

impl RunStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::Superseded)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunState {
    pub version: u32,
    pub run_id: String,
    pub name: String,
    pub cwd: PathBuf,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub args: Value,
    pub max_parallel: usize,
    pub max_calls: usize,
    #[serde(default)]
    pub resume_count: u32,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub active: BTreeMap<String, ActiveCall>,
    #[serde(default)]
    pub waiting_gate: Option<GateRequest>,
    #[serde(default)]
    pub supersede: Option<SupersedeInfo>,
    #[serde(default)]
    pub decisions: BTreeMap<String, GateDecision>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub report: Option<PathBuf>,
    #[serde(default)]
    pub run_summary: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupersedeInfo {
    pub reason: String,
    pub evidence: Option<String>,
    pub new_contract: Option<String>,
    pub decided_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveCall {
    pub kind: CallKind,
    pub label: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateRequest {
    pub key: String,
    pub label: String,
    pub question: String,
    #[serde(default)]
    pub expect: Option<String>,
    #[serde(default)]
    pub current: Option<Value>,
    #[serde(default)]
    pub hint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GateDecision {
    pub approved: bool,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CallKind {
    Agent,
    Command,
    Gate,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JournalEntry {
    pub at: DateTime<Utc>,
    pub key: String,
    pub kind: CallKind,
    pub state: CallState,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_run_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CallState {
    Submitted,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct PublicRun {
    pub run_id: String,
    pub status: RunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub active: BTreeMap<String, ActiveCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gate: Option<GateRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supersede: Option<SupersedeInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<PathBuf>,
}

impl From<&RunState> for PublicRun {
    fn from(state: &RunState) -> Self {
        Self {
            run_id: state.run_id.clone(),
            status: state.status.clone(),
            phase: state.phase.clone(),
            active: state.active.clone(),
            gate: state.waiting_gate.clone(),
            supersede: state.supersede.clone(),
            result: state.result.clone(),
            error: state.error.clone(),
            report: state.report.clone(),
        }
    }
}
