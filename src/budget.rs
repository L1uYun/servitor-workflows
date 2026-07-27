use crate::error::WorkflowError;
use crate::model::{BudgetEvent, BudgetLedger, CallKind};
use crate::store::WorkflowStore;
use serde_json::Value;
use std::sync::Arc;

/// Shared multidimensional budget handle. One per run; wraps the store so every
/// reserve / settle / release is durably recorded to `budget.jsonl`. V2-C
/// children share the parent's ledger by setting `owner_run_id` to the parent.
///
/// Money is in cents (1 = 0.01 USD). `None` means unlimited — the default when
/// the run has no `money_cap`.
#[derive(Clone)]
pub struct Budget {
    store: Arc<WorkflowStore>,
    run_id: String,
    owner_run_id: String,
}

impl Budget {
    pub fn new(store: Arc<WorkflowStore>, run_id: String, owner_run_id: String) -> Self {
        Self {
            store,
            run_id,
            owner_run_id,
        }
    }

    fn ledger_key(&self, key: &str) -> String {
        if self.run_id == self.owner_run_id {
            key.to_owned()
        } else {
            format!("{}:{key}", self.run_id)
        }
    }

    /// Book a call before execution. Idempotent by `key` — a crash after
    /// reservation never double-charges. Returns `Ok(())` when the budget
    /// allows the call; returns `Err(...)` when the call would exceed a hard
    /// limit (calls or money).
    pub fn reserve(&self, key: &str, kind: CallKind, input: &Value) -> Result<(), WorkflowError> {
        let ledger_key = self.ledger_key(key);
        let limit_money = self
            .store
            .reconstruct_budget(&self.owner_run_id)?
            .limit_money;
        let estimate_money = cost_estimate(&kind, input, limit_money);
        // The store owns the read → check → append critical section so sibling
        // children cannot observe the same free capacity and over-reserve.
        self.store.reserve_budget(
            &self.owner_run_id,
            &self.run_id,
            ledger_key,
            kind,
            estimate_money,
        )
    }

    /// Finalize a call after completion. Sets actual cost and tokens.
    /// Idempotent: a second settle for the same key is a no-op.
    pub fn settle(
        &self,
        key: &str,
        actual_money: Option<u64>,
        actual_tokens: u64,
    ) -> Result<(), WorkflowError> {
        let ledger_key = self.ledger_key(key);
        // Idempotency: skip if already settled.
        let ledger = self.store.reconstruct_budget(&self.owner_run_id)?;
        match ledger.reservations.get(&ledger_key) {
            Some(res) if res.settled => return Ok(()),
            Some(_) => {}
            None => return Ok(()),
        }

        self.store.append_budget_event(
            &self.owner_run_id,
            &self.run_id,
            BudgetEvent::Settled {
                key: ledger_key,
                actual_money,
                actual_tokens,
            },
        )
    }

    /// Release a held reservation without charging (cancellation, supersede,
    /// drain). Idempotent: a second release for the same key is a no-op.
    /// A settled key cannot be released.
    pub fn release(&self, key: &str, reason: &str) -> Result<(), WorkflowError> {
        let ledger_key = self.ledger_key(key);
        let ledger = self.store.reconstruct_budget(&self.owner_run_id)?;
        match ledger.reservations.get(&ledger_key) {
            Some(res) if res.released || res.settled => return Ok(()),
            Some(_) => {}
            None => return Ok(()),
        }

        self.store.append_budget_event(
            &self.owner_run_id,
            &self.run_id,
            BudgetEvent::Released {
                key: ledger_key,
                reason: reason.to_owned(),
            },
        )
    }

    /// The originating run id, used to read its journal for settlement usage.
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }
    /// Reconstruct the current budget ledger by replaying `budget.jsonl`.
    pub fn ledger(&self) -> Result<BudgetLedger, WorkflowError> {
        self.store.reconstruct_budget(&self.owner_run_id)
    }
}

// ---------------------------------------------------------------------------
// V2-G placeholder: per-kind cost table
// ---------------------------------------------------------------------------

/// Flat cost table. V2-G replaces this with a configurable provider-aware table;
/// for V2-B every call kind costs zero money (only call count is gated). Token
/// attribution is still tracked per call via `settle`.
///
/// Returns `None` when the cost is zero or money tracking is disabled (i.e.
/// `money_cap` is `None`).
fn cost_estimate(_kind: &CallKind, _input: &Value, money_cap: Option<u64>) -> Option<u64> {
    if money_cap.is_none() {
        return None;
    }
    // V2-G: wire real cost estimates per kind / model / input size.
    // For now, every call costs zero — only call-count gating applies.
    Some(0)
}

// ---------------------------------------------------------------------------
// Helpers for the script VM host layer
// ---------------------------------------------------------------------------

/// Pre-reservation check for the script VM's `HostState::key()` path. Returns
/// the call key unchanged on success, or an error when the budget blocks the
/// call. This is the budget-side of the old `max_calls` in-memory counter.
///
/// `journal_keys` is the set of keys already present in the journal at VM
/// start; those keys are free (they're replay, not new calls).
pub fn budget_gate_key(
    budget: &Budget,
    key: &str,
    kind: CallKind,
    input: &Value,
    journal_keys: &std::collections::BTreeSet<String>,
    max_calls: usize,
) -> Result<(), WorkflowError> {
    // Replayed journal keys don't consume budget — the reservation already
    // exists from the prior execution.
    if journal_keys.contains(key) {
        return Ok(());
    }

    // Count-only fast path: if the ledger already hit max_calls, fail fast
    // without touching disk.
    let ledger = budget.ledger()?;
    let committed = (ledger.used_calls + ledger.held_calls) as usize;
    if committed >= max_calls {
        return Err(WorkflowError::Invariant(format!(
            "workflow exceeded max_calls={max_calls}"
        )));
    }

    budget.reserve(key, kind, input)
}

#[cfg(test)]
mod tests {}
