//! Live observability: reconstruct a run tree, budget/usage, waiting
//! categories, critical path, and recovery instructions exclusively from
//! persisted artifacts (`state.json`, `events.jsonl`, `journal.jsonl`,
//! `budget.jsonl`). No in-memory runtime state is consulted, so a fresh
//! process — a killed-and-restarted CLI — rebuilds the identical view.

use crate::model::{BudgetLedger, CallKind, ReconstructedState, RunStatus};
use crate::run_summary::usage_tokens;
use crate::store::WorkflowStore;
use serde::Serialize;

/// Where the view came from. Always persisted artifacts: the defining
/// property of `watch` is that a restarted process sees the same tree.
pub const WATCH_SOURCE: &str = "persisted_events";

/// A full observability snapshot of one run and its child tree.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchView {
    pub run_id: String,
    pub root_run_id: String,
    pub status: RunStatus,
    pub source: &'static str,
    pub tree: WatchNode,
    /// Aggregate root budget ledger. `None` for legacy runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<BudgetLedger>,
    /// Every node currently blocked, categorized.
    pub waiting: Vec<WaitingEntry>,
    /// Run-id chain from the queried root to the deepest non-terminal branch.
    pub critical_path: Vec<String>,
    /// Concrete commands that move the run forward.
    pub recovery: Vec<Recovery>,
}

/// One node in the reconstructed tree.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchNode {
    pub run_id: String,
    pub name: String,
    pub status: RunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Why this node is not making progress (see `waiting_category`).
    pub category: String,
    pub active_calls: usize,
    /// Attributed tokens summed from this run's journal usage.
    pub tokens: u64,
    pub children: Vec<WatchNode>,
}

/// A blocked node plus a human-readable reason.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitingEntry {
    pub run_id: String,
    pub category: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// One actionable recovery step.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recovery {
    pub run_id: String,
    pub reason: String,
    pub command: String,
}

/// Reconstruct the observability view for `run_id` purely from disk.
pub fn reconstruct(
    store: &WorkflowStore,
    run_id: &str,
) -> Result<WatchView, crate::error::WorkflowError> {
    let state = store.load_state(run_id)?;
    let root_run_id = state
        .root_run_id
        .clone()
        .unwrap_or_else(|| state.run_id.clone());

    let mut waiting = Vec::new();
    let mut recovery = Vec::new();
    let mut visited = std::collections::BTreeSet::new();
    let tree = build_node(store, run_id, &mut visited, &mut waiting, &mut recovery)?;
    let status = tree.status.clone();

    let budget = crate::script::is_current_contract(state.contract.as_deref())
        .then(|| store.reconstruct_budget(&root_run_id))
        .transpose()?;

    let critical_path = critical_path(store, run_id)?;

    dedupe_recovery(&mut recovery);

    Ok(WatchView {
        run_id: run_id.to_owned(),
        root_run_id,
        status,
        source: WATCH_SOURCE,
        tree,
        budget,
        waiting,
        critical_path,
        recovery,
    })
}

fn build_node(
    store: &WorkflowStore,
    run_id: &str,
    visited: &mut std::collections::BTreeSet<String>,
    waiting: &mut Vec<WaitingEntry>,
    recovery: &mut Vec<Recovery>,
) -> Result<WatchNode, crate::error::WorkflowError> {
    // Persisted state is the trust boundary: a hand-corrupted parent cycle must
    // not recurse forever or overflow the stack. Reject it explicitly.
    if !visited.insert(run_id.to_owned()) {
        return Err(crate::error::WorkflowError::Invariant(format!(
            "watch detected a parent/child cycle at run {run_id}"
        )));
    }
    let rs = store.reconstruct_state(run_id)?;
    let tokens = node_tokens(store, run_id)?;
    let category = waiting_category(&rs);

    record_waiting(&rs, &category, waiting);
    record_recovery(&rs, recovery);

    let mut children = Vec::new();
    for child_run_id in store.child_run_ids(run_id)? {
        children.push(build_node(
            store,
            &child_run_id,
            visited,
            waiting,
            recovery,
        )?);
    }

    Ok(WatchNode {
        run_id: rs.run_id.clone(),
        name: rs.name.clone(),
        status: rs.status.clone(),
        phase: rs.phase.clone(),
        category,
        active_calls: rs.active.len(),
        tokens,
        children,
    })
}

/// Classify why a node is not making forward progress. Terminal nodes report
/// their terminal status; running nodes distinguish child-workflow waits from
/// in-flight host calls.
fn waiting_category(rs: &ReconstructedState) -> String {
    match rs.status {
        RunStatus::Running => {
            if rs
                .active
                .values()
                .any(|call| call.kind == CallKind::Workflow)
            {
                "waiting_children".to_owned()
            } else if rs.active.is_empty() {
                "running".to_owned()
            } else {
                "waiting_calls".to_owned()
            }
        }
        RunStatus::WaitingHuman => "waiting_human".to_owned(),
        RunStatus::Pausing => "pausing".to_owned(),
        RunStatus::Paused => "paused".to_owned(),
        RunStatus::Cancelling => "cancelling".to_owned(),
        RunStatus::Succeeded => "succeeded".to_owned(),
        RunStatus::Failed => "failed".to_owned(),
        RunStatus::Cancelled => "cancelled".to_owned(),
        RunStatus::Superseded => "superseded".to_owned(),
    }
}

fn record_waiting(rs: &ReconstructedState, category: &str, waiting: &mut Vec<WaitingEntry>) {
    let detail = match rs.status {
        RunStatus::WaitingHuman => rs
            .waiting_gate
            .as_ref()
            .map(|gate| format!("gate {}: {}", gate.label, gate.question)),
        RunStatus::Running if category == "waiting_calls" => {
            Some(format!("{} active call(s)", rs.active.len()))
        }
        RunStatus::Running if category == "waiting_children" => {
            let n = rs
                .active
                .values()
                .filter(|call| call.kind == CallKind::Workflow)
                .count();
            Some(format!("{n} active child workflow(s)"))
        }
        RunStatus::Failed => rs.error.clone(),
        _ => None,
    };
    // Only surface nodes that are actually blocked; plain `running` and
    // terminal nodes (succeeded/failed/cancelled/superseded) are not waiting
    // on anything — a failed run is terminal, surfaced via `recovery` instead.
    let blocked = !rs.status.is_terminal() && category != "running";
    if blocked {
        waiting.push(WaitingEntry {
            run_id: rs.run_id.clone(),
            category: category.to_owned(),
            detail,
        });
    }
}

fn record_recovery(rs: &ReconstructedState, recovery: &mut Vec<Recovery>) {
    match rs.status {
        RunStatus::WaitingHuman => {
            if let Some(gate) = rs.waiting_gate.as_ref() {
                let origin = gate
                    .origin_run_id
                    .clone()
                    .unwrap_or_else(|| rs.run_id.clone());
                recovery.push(Recovery {
                    run_id: origin.clone(),
                    reason: format!("gate {} awaits a human decision", gate.label),
                    command: format!(
                        "servitor-workflows approve {origin} --reason \"...\" (or `reject {origin} --reason \"...\")"
                    ),
                });
            }
        }
        RunStatus::Failed => recovery.push(Recovery {
            run_id: rs.run_id.clone(),
            reason: rs.error.clone().unwrap_or_else(|| "run failed".to_owned()),
            command: format!("servitor-workflows resume {}", rs.run_id),
        }),
        RunStatus::Paused => recovery.push(Recovery {
            run_id: rs.run_id.clone(),
            reason: "run is paused".to_owned(),
            command: format!("servitor-workflows resume {}", rs.run_id),
        }),
        _ => {}
    }
}

/// Deterministic critical path: from `run_id`, repeatedly descend into the
/// first (sorted) non-terminal child until a node has no non-terminal child.
/// The result is the chain whose resolution unblocks forward progress.
fn critical_path(
    store: &WorkflowStore,
    run_id: &str,
) -> Result<Vec<String>, crate::error::WorkflowError> {
    let mut path = vec![run_id.to_owned()];
    let mut current = run_id.to_owned();
    let mut visited = std::collections::BTreeSet::from([run_id.to_owned()]);
    loop {
        let rs = store.reconstruct_state(&current)?;
        if rs.status.is_terminal() {
            break;
        }
        let next = store.child_run_ids(&current)?.into_iter().find(|child| {
            store
                .reconstruct_state(child)
                .map(|child_rs| !child_rs.status.is_terminal())
                .unwrap_or(false)
        });
        match next {
            Some(child) => {
                // A corrupted parent/child cycle must not loop forever.
                if !visited.insert(child.clone()) {
                    return Err(crate::error::WorkflowError::Invariant(format!(
                        "watch detected a parent/child cycle at run {child}"
                    )));
                }
                path.push(child.clone());
                current = child;
            }
            None => break,
        }
    }
    Ok(path)
}

fn node_tokens(store: &WorkflowStore, run_id: &str) -> Result<u64, crate::error::WorkflowError> {
    let journal = store.journal_index(run_id)?;
    Ok(journal
        .values()
        .filter_map(|entry| usage_tokens(entry.usage.as_ref()))
        .fold(0u64, |acc, n| acc.saturating_add(n)))
}

fn dedupe_recovery(recovery: &mut Vec<Recovery>) {
    let mut seen = std::collections::BTreeSet::new();
    recovery.retain(|step| seen.insert(step.command.clone()));
}
