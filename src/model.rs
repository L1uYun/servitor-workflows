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
    /// Contract version string. `Some("workflow.v2")` for new v2 runs; `None`
    /// for v1 runs (the frozen compatibility path). Drives whether the
    /// versioned event stream is written.
    #[serde(default)]
    pub contract: Option<String>,
    /// Parent run id for structured-concurrency children. `None` in V2-A;
    /// foundation only, exercised by V2-C.
    #[serde(default)]
    pub parent_run_id: Option<String>,
    /// Stable root of a structured workflow tree. Root runs point to themselves;
    /// v1 records omit it and remain readable.
    #[serde(default)]
    pub root_run_id: Option<String>,
    /// The deterministic parent workflow call which owns this child run.
    #[serde(default)]
    pub parent_call_key: Option<String>,
    #[serde(default)]
    pub money_cap: Option<u64>,
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
    #[serde(default)]
    pub journal_path: PathBuf,
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
    /// Run that owns the actual decision. Ancestors retain this when a child
    /// wait bubbles so approval is routed to the correct leaf.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_run_id: Option<String>,
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
    Workflow,
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
    /// Persisted child identity for `CallKind::Workflow`. Kept separate from a
    /// provider transport id so replay never needs to infer ownership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_run_id: Option<String>,
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
    pub journal_path: PathBuf,
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
            journal_path: state.journal_path.clone(),
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
            child_run_id: None,
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

// ---------------------------------------------------------------------------
// V2-A: versioned append-only event stream + event-to-state reconstruction
// ---------------------------------------------------------------------------

/// Schema version stamped on every `WorkflowEventEnvelope`. Bumped only on a
/// breaking change to the event shape. `workflow.v2` runs emit envelopes with
/// this version; v1 runs never emit events (they keep the frozen journal path).
pub const EVENT_SCHEMA_VERSION: u32 = 2;

/// Lifecycle event recorded to `events.jsonl` for `workflow.v2` runs. Call
/// outcomes remain in `journal.jsonl` (the v1 call-event stream); the lifecycle
/// stream here carries run-level transitions, phases, and gates so a run can
/// be reconstructed without in-memory state. V2-A deliberately records only
/// what the foundation needs; budgets, children, isolation, routing, and watch
/// arrive in later slices.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowEvent {
    RunStarted {
        name: String,
        args: Value,
        max_parallel: usize,
        max_calls: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        money_cap: Option<u64>,
    },
    RunResumed {
        resume_count: u32,
    },
    PhaseChanged {
        phase: String,
    },
    GateOpened {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_run_id: Option<String>,
        label: String,
        question: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
    GateDecided {
        key: String,
        approved: bool,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
    },
    RunSucceeded {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
    },
    RunFailed {
        error: String,
    },
    RunCancelled {
        error: String,
    },
    RunSuperseded {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        evidence: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        new_contract: Option<String>,
    },
    RunPaused,
    /// A pause request is durable while active calls drain. This is distinct
    /// from `RunPaused`, which means execution has stopped and may resume.
    RunPausing,
    /// A cancellation request is durable while active calls drain. This is
    /// distinct from `RunCancelled`, which is the terminal outcome.
    RunCancelling {
        error: String,
    },
}

/// One append-only line of `events.jsonl`. `sequence` is monotonic per run;
/// `parent_run_id` is `None` in V2-A and exists so V2-C can grow the tree
/// without reshaping the envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEventEnvelope {
    pub version: u32,
    pub sequence: u64,
    pub at: DateTime<Utc>,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub event: WorkflowEvent,
}

/// State rebuilt purely from persisted artifacts (`events.jsonl` lifecycle
/// stream + `journal.jsonl` call stream + the static identity fields a run
/// records at creation). Compared against live `RunState` to prove the
/// reconstruction is deterministic for fixed traces.
#[derive(Clone, Debug, PartialEq)]
pub struct ReconstructedState {
    pub version: u32,
    pub contract: Option<String>,
    pub run_id: String,
    pub parent_run_id: Option<String>,
    pub name: String,
    pub max_parallel: usize,
    pub max_calls: usize,
    pub money_cap: Option<u64>,
    pub status: RunStatus,
    pub phase: Option<String>,
    pub active: BTreeMap<String, ActiveCall>,
    pub waiting_gate: Option<GateRequest>,
    pub supersede: Option<SupersedeInfo>,
    pub decisions: BTreeMap<String, GateDecision>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub resume_count: u32,
    pub call_count: usize,
}

// ---------------------------------------------------------------------------
// V2-B: shared multidimensional budget ledger (budget.jsonl per run)
// ---------------------------------------------------------------------------

/// Schema version stamped on every `BudgetEnvelope`.
pub const BUDGET_SCHEMA_VERSION: u32 = 1;

/// Budget events recorded to `budget.jsonl` for `workflow.v2` runs. Each
/// entry is idempotent by call `key` — reserve scans the existing ledger
/// before writing so a crash after reservation never double-charges.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BudgetEvent {
    Reserved {
        key: String,
        kind: CallKind,
        /// Conservative money estimate in cents (1 = 0.01 USD). `None` when
        /// moneyCap is unlimited or the call kind has no cost.
        estimate_money: Option<u64>,
    },
    Settled {
        key: String,
        /// Actual money charge in cents. `None` when moneyCap is unlimited.
        actual_money: Option<u64>,
        /// Token count from provider usage (attributed, never gates).
        actual_tokens: u64,
    },
    Released {
        key: String,
        reason: String,
    },
}

/// One append-only line of `budget.jsonl`. `owner_run_id` is the run whose
/// ledger is charged (in V2-B this equals `run_id`; V2-C children write
/// into their parent's ledger).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BudgetEnvelope {
    pub version: u32,
    pub sequence: u64,
    pub at: DateTime<Utc>,
    pub run_id: String,
    pub owner_run_id: String,
    pub event: BudgetEvent,
}

/// Reconstructed view of a run's budget ledger. `limit_calls`/`limit_money`
/// come from the run contract; the counter fields are derived from the
/// `budget.jsonl` stream.
#[derive(Clone, Debug, PartialEq, Default, Serialize)]
pub struct BudgetLedger {
    pub limit_calls: Option<usize>,
    pub limit_money: Option<u64>,
    pub used_calls: u64,
    pub used_money: u64,
    pub held_calls: u64,
    pub held_money: u64,
    pub attributed_tokens: u64,
    pub reservations: BTreeMap<String, ReservationSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ReservationSummary {
    pub kind: CallKind,
    pub estimate_money: Option<u64>,
    pub actual_money: Option<u64>,
    pub actual_tokens: u64,
    pub settled: bool,
    pub released: bool,
}
