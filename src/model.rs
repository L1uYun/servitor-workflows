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
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Superseded
        )
    }

    /// Statuses that must not re-execute on `resume`.
    /// `Failed` stays resumable so journaled recovery can retry intentionally.
    pub fn blocks_resume_rerun(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Cancelled | Self::Superseded)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_correction: Option<SchemaCorrectionMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCorrectionMetadata {
    pub attempted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transport_run_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_errors: Vec<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_summary: Option<PathBuf>,
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
            run_summary: state.run_summary.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn old_journal_entry_deserializes_without_observability_fields() {
        let entry: JournalEntry = serde_json::from_str(
            r#"{"at":"2026-01-01T00:00:00Z","key":"k","kind":"agent","state":"succeeded","label":"l"}"#,
        )
        .expect("deserialize old journal entry");
        assert_eq!(entry.phase, None);
        assert_eq!(entry.duration_ms, None);
        assert_eq!(entry.usage, None);
        assert_eq!(entry.schema_correction, None);
    }

    #[test]
    fn absent_observability_fields_are_not_serialized() {
        let entry = JournalEntry {
            at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            key: "k".to_owned(),
            kind: CallKind::Agent,
            state: CallState::Succeeded,
            label: "l".to_owned(),
            result: None,
            error: None,
            transport_run_id: None,
            phase: None,
            duration_ms: None,
            usage: None,
            schema_correction: None,
        };
        let value = serde_json::to_value(entry).expect("serialize journal entry");
        assert!(value.get("phase").is_none());
        assert!(value.get("duration_ms").is_none());
        assert!(value.get("usage").is_none());
        assert!(value.get("schema_correction").is_none());
    }
}
