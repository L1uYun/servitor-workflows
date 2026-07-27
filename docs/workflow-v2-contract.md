# Servitor Workflows v2 — Local Controlled Autonomy Contract

Status: confirmed
Liber Null: #324
Baseline commit: `4a928c3eaa8dad0c3bcad5e6d3d6c1e31989c90f`
Project root: `D:\AgentWork\tools\servitor-workflows`

## Outcome

Make Claude-native dynamic orchestration the minimum behavioral baseline, then exceed it through local, persistent, controlled autonomy: cross-process recovery, structured child-workflow concurrency, human gates, typed command evidence, boundary auditing, cost attribution, explainable capability routing, and persistent critical-path observability.

## Frozen decisions

- New runs use `workflow.v2`; v1 terminal runs remain readable and v1 non-terminal runs remain resumable under frozen v1 replay semantics.
- Boundary enforcement begins in audit mode. Contracts declare `readPaths`, `writePaths`, and network policy; observed violations block success/release. This is not claimed to be an OS sandbox.
- `maxCalls` is a shared hard limit across a workflow tree.
- There is no token limit. Token usage is measured for attribution only and never stops a run.
- `moneyCap` defaults to unlimited and becomes a hard limit only when a contract explicitly supplies it.
- Budget accounting uses reservation and settlement; unknown provider usage remains conservatively estimated. Resume never refreshes a budget and replay never charges twice.
- `workflow()` creates a persistent structured-concurrency tree. Children inherit and may only tighten budgets, boundaries, isolation, and cancellation scope. Child failure rejects only that call; the parent decides policy. Human waiting bubbles visibly through the tree.
- Structured output uses a deterministic local validator for a high-value JSON Schema Draft 2020-12 subset: strict objects, composition, scalar/array constraints, `$defs`, and local `$ref`. Remote refs and custom validator code are forbidden. Provider-native schema may be used, but cannot bypass local validation. At most one correction is allowed.
- Isolation levels are `none`, `worktree`, `process`, and explicitly requested `container`. Worktree is not a security sandbox. Credentials are capability-injected rather than inherited wholesale.
- Observability is a versioned persistent event stream consumed by `watch RUN_ID` and `watch RUN_ID --output jsonl`. It reconstructs parent/child state, active phases, queues, retries, gates, cost/usage, waiting categories, critical path, and recovery commands without in-memory state.
- Agent routing is capability based and explainable. Explicit provider/model choices are never silently replaced. Automatic routing stays inside allowed candidates and records exclusions, choice, and degradation. Maker/checker independence is mechanically expressible.
- The cutover is direct: maintained scripts and the goalchain template migrate to v2. New runs cannot silently fall back to v1.

## Scope and slices

### V2-A — Contract and event foundation

Define v2 metadata, versioned events, parent/child identifiers, v1 compatibility boundaries, append-only persistence, and event-to-state reconstruction.

Acceptance:
- v1 non-terminal fixtures resume under v1 semantics;
- new runs reject missing or non-v2 contract metadata;
- reconstructed state equals online state for fixed traces, including durable `pausing` and `cancelling` requests while active calls drain;
- malformed or torn `events.jsonl` is rejected during reconstruction; automated repair is deferred to V2-H;
- original v1 journals are not rewritten.

### V2-B — Shared multidimensional budget ledger

Implement shared `maxCalls`, optional `moneyCap`, token-usage attribution, reservation/settlement/release, and conservative unknown usage.

Acceptance:
- a crash after reservation and before settlement neither double-charges nor loses the reservation;
- a child cannot bypass parent call or money limits;
- resume grants no fresh capacity;
- token usage never stops a run.

### V2-C — Persistent child-workflow tree

Add `workflow()` with persistent parent/child state, shared concurrency, cancellation propagation, human-wait bubbling, and deterministic replay.

Acceptance:
- three-level crash-window fixtures preserve tree identity, result, call count, and budget;
- child failure remains parent-policy controlled;
- cycles and unbounded recursion fail before dispatch.

### V2-D — Unified structured output

Add the selected Draft 2020-12 subset, provider-native schema pushdown where supported, deterministic candidate extraction, one bounded correction, and safe validation evidence.

Acceptance:
- tagged unions, strict objects, local refs, conflicting schemas, malicious/multiple candidates, and correction crash windows are mechanically judged;
- provider success cannot bypass local validation.

### V2-E — Boundary audit

Persist workflow/agent/command/child declarations, file and Git snapshots, observable network evidence, and secret-safe execution metadata. Block success and release on violations.

Acceptance:
- undeclared writes, excess environment inheritance, undeclared network access, and child permission widening cannot silently succeed;
- journal never stores credential values.

### V2-F — Layered isolation

Implement `none`, worktree lifecycle and evidence, process environment/process-tree controls, and an explicit container capability/refusal path.

Acceptance:
- worktree changes produce patch/commit evidence;
- cancellation leaves no task-owned child process;
- a child cannot weaken parent isolation.

### V2-G — Capability registry and routing

Implement role contracts, provider/model capabilities, effort/context requirements, explainable selection, and maker/checker independence.

Acceptance:
- pinned models are not replaced;
- missing capabilities fail fast;
- permitted degradation is fully explained;
- reviewer independence is mechanically testable.

### V2-H — Live observability and critical path

Implement the persistent event protocol, `watch`, JSONL output, tree views, budget/usage, waiting categories, critical path, and recovery instructions.

Acceptance:
- after killing and restarting the CLI, `watch` reconstructs the same tree and critical path exclusively from persisted events.

### V2-I — Goalchain v2 migration

Migrate readiness, scouts, negotiation, implementation, independent per-slice review, mechanical and semantic gates, release, and writeback. Use child workflows for review instead of external placeholder paths. Preserve only G1–G16 discipline not superseded by runtime guarantees.

Acceptance:
- one real delivery completes dual gates, crash recovery, child review, cost attribution, and boundary audit.

### V2-J — Superiority benchmark and release gate

Fixed behavioral cases: dynamic fan-out, no-barrier pipeline, judge panel, loop-until-dry, schema correction, child workflows, adaptive call/money budgeting, human waiting, child degradation, and cancellation propagation.

Fault injection: host kill, interrupted atomic state write, torn journal tail, absent provider usage, submitted transport without persisted result, child success before parent acknowledgment, unsettled reservation, boundary violation, orphan process, and restart while waiting human.

Acceptance:
- Claude-native-expressible cases are at least behaviorally equivalent;
- local-only cases pass machine verdicts;
- fixed inputs, raw results, and injection seeds are replayable;
- v1 recovery fixtures and the existing test suite do not regress.

## Write boundary

Allowed durable writes:
- this repository's source, tests, examples, README, and `docs/`;
- Liber Null item #324 notes/status;
- disposable workflow and evidence under `D:\AgentWork\_tmp\servitor-workflows-v2`.

Forbidden without a separate user decision:
- push, deployment, installation, service restart, remote mutation, credential access or rotation;
- editing other project repositories;
- rewriting original v1 journals;
- adding token limits or a default money cap;
- silent provider fallback;
- claiming worktree/process isolation is an OS security sandbox.

## Delivery discipline

Each slice is an independent rollback commit. Before commit/done it requires focused tests, full project verification appropriate to the slice, an independent code-review dispatch, resolution of Critical findings, and controller verification. A cross-slice fault invalidates the affected design and stops the next slice. Final closure requires the parity/fault-injection benchmark, Liber Null writeback, and neat closure.

## Mechanical tokens

```yaml
contract_version: workflow.v2
no_token_limit: true
money_cap_default: unlimited
v1_history_readable: true
v1_nonterminal_resumable: true
structured_concurrency: true
boundary_audit: true
persistent_event_stream: true
fault_injection_release_gate: true
```
