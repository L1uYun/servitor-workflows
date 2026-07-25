# servitor-workflows

CLI responses use the agent-native envelope {ok,data,meta,error}; run servitor-workflows schema for contracts.

Rust orchestration above the Rust `servitor` transport.

`servitor` owns agent process submission, inspection, cancellation, and output.
`servitor-workflows` owns dynamic workflow execution, concurrency, commands,
human gates, persisted state, replay, pause, cancellation, and machine run
evidence. Reader-facing delivery reports are workflow outputs, not Rust-rendered
runtime pages.

The execution contract has three primitives:

```text
agent + command + gate
```

Workflows use sandboxed JavaScript because ordinary loops, branches, and
dynamic fan-out are clearer there than in a static DAG. Boa is embedded in the
Rust binary; Node, Deno, V8, and Python are not runtime dependencies.

## Workflow

```javascript
export const meta = {
  name: "audit-routes",
};

phase("discover");
const found = await agent("Find files to audit.", {
  schema: {
    type: "object",
    required: ["files"],
    properties: {
      files: { type: "array", items: { type: "string" } },
    },
  },
});

phase("audit");
const results = await pipeline(
  found.files,
  file => agent(`Audit ${file}.`, { label: file }),
);

return results;
```

Available globals:

```text
args
agent(prompt, options?)
command(program, args?, options?)
gate(question, options?)
phase(name)
parallel(promises)
pipeline(items, worker)
retry(fn, options?)
supersede(options)
```

JavaScript has no direct filesystem or process API. External work crosses the
typed `agent`, `command`, or `gate` boundary.

### Agent options

```javascript
{
  label: "discover",
  agent: "pi",
  model: null,
  cwd: null,
  systemPrompt: null,
  timeoutSeconds: 120,
  nativeArgs: [],
  schema: null,
}
```

The default agent is `pi`.

When an agent call requests structured output, the runtime extracts JSON from free-form model text by:
strip reasoning wrappers → collect candidates (whole text, fenced ```json, balanced `{...}`/`[...]` spans) → trailing-comma repair → shape filter from schema `type` → **schema-valid selection** (required/properties/items): if several candidates validate, take the **last** (final-answer convention). Schema is a selection criterion among candidates, not only a post-check on the first shape match. Pure whole-text JSON still works. The runtime does not set a token budget, retry a
provider silently, or fall back to another provider. No provider-specific prose strippers.

Structured output supports the deliberately small JSON Schema subset used by
workflow contracts: `type`, `required`, `properties`, and `items`.

### Command options

```javascript
{
  label: "test",
  cwd: null,
  timeoutSeconds: 300,
  env: {},
}
```

On Windows, command children are created without a console window. Stdout and
stderr retain a bounded 1 MiB tail each.

`command()` resolves to a structured `CommandResult`:

```json
{
  "argv": ["pwsh", "-NoProfile", "..."],
  "cwd": "D:\\AgentWork",
  "exitCode": 0,
  "stdout": "...",
  "stderr": "...",
  "stdoutTruncated": false,
  "stderrTruncated": false,
  "timedOut": false,
  "durationMs": 751
}
```

The same object is persisted atomically to
`state/servitor-workflows/runs/<run_id>/commands/<key>/result.json` for success,
non-zero exit, and timeout alike. Consume these typed fields directly; do not
re-read console output or add a "read-results" pass. `stdout`/`stderr` are
bounded tails, not business conclusions — a worker that must hand back a machine
verdict should write an explicit evidence JSON at an agreed path instead.

## Control flow primitives

`retry(fn, options?)` — bounded retry around any `agent`/`command` thunk. Each
attempt is a separate journaled call (attempt number is part of the journal key),
so deterministic replay still holds: a succeeded attempt is skipped on resume, a
failed one re-runs. Options:

```javascript
await retry(() => command("pwsh", ["-File", "flaky.ps1"]), {
  maxAttempts: 3,          // fixed attempt cap
  delayMs: 2000,           // first backoff delay
  backoff: 2,              // multiplier (1 = fixed interval)
  wallTimeSeconds: 60,     // total elapsed cap (SLA), stops even if attempts remain
  nonRetryable: ["validation"], // fail fast when error text matches; no retry
});
```

`gate(question, options?)` — pause for a human decision. Beyond yes/no, a gate
can carry an injected value for the human-corrects-input case:

```javascript
const fixed = await gate("give the correct contractPath", {
  expect: "value",                    // ask for a JSON value, not a yes/no
  current: { contractPath: "old.md" },
  hint: "should be under surveys/",
});
await command("pwsh", ["-File", "check.ps1", fixed.value.contractPath]);
```

The run parks at `waiting_human` with `expect`/`current`/`hint` in
`waiting_gate`. Collecting the value (a chat answer, a local decision page) is
the controller's job; inject it with `approve RUN_ID --reason TEXT --value
'{"contractPath":"surveys/new.md"}'`. `get` returns the stored value.

`supersede(options)` — mark the whole run terminal as `superseded` (distinct
from `failed`/`cancelled`) when the direction itself is wrong, recording
`{reason, evidence?, newContract?}` into state. The controller reads `get` →
`status=superseded`, writes the supersede-chain note, and starts a new run whose
readiness gate skips already-produced artifacts. This is continue-as-new, never
in-place script edits. CLI equivalent: `supersede RUN_ID --reason TEXT
[--evidence PATH] [--new-contract TEXT]`.

## CLI

```text
servitor-workflows run WORKFLOW.js [--args JSON] [--max-parallel N] [--max-calls N]
servitor-workflows resume RUN_ID
servitor-workflows get RUN_ID
servitor-workflows list [--limit N] [--status STATUS]
servitor-workflows approve RUN_ID --reason TEXT [--value JSON]
servitor-workflows reject RUN_ID --reason TEXT
servitor-workflows pause RUN_ID [--dry-run]
servitor-workflows cancel RUN_ID [--dry-run]
servitor-workflows supersede RUN_ID --reason TEXT [--evidence PATH] [--new-contract TEXT] [--dry-run]
servitor-workflows inspect RUN_ID
servitor-workflows schema
```

Output modes:

```text
--output json    machine-facing public result, default
--output human   compact status and result
--output quiet   exit status only
```

Exit codes: `0` ok, `1` runtime/terminal failure, `2` invalid input, `3` not found. Errors use the same envelope on stdout.

`run`, `resume`, `get`, `approve`, `reject`, `pause`, `cancel`, and `supersede` return the
low-noise public shape: `run_id`, `status`, and only relevant phase, active
calls, gate, result, error, or terminal `report` path. `list` returns `{runs,count,limit,truncated,total}`. `inspect` is the explicit
detailed surface and adds persisted state plus owner paths, including the
machine `run_summary_path`.

Resume policy: `succeeded|cancelled|superseded` block re-exec; `failed` remains resumable for recovery. `max_calls` budget seeds from journal size so resume does not grant a fresh budget; replaying journaled keys is free.

Defaults:

```text
max_parallel = min(16, available_parallelism)
max_calls = 1000
```

## State and recovery

Default state root:

```text
D:\AgentWork\state\servitor-workflows
```

Override it with `SERVITOR_WORKFLOWS_STATE_ROOT`.

Each run owns:

```text
runs/<RUN_ID>/workflow.js
runs/<RUN_ID>/state.json
runs/<RUN_ID>/journal.jsonl
runs/<RUN_ID>/run-summary.html
runs/<RUN_ID>/pause.request
runs/<RUN_ID>/cancel.request
```

Resume replays the same JavaScript for paused, interrupted, or failed runs.
Stable call identity plus an occurrence index returns completed calls from the
journal, reconnects submitted agent calls through their Servitor run id, and
reruns failed calls under the same stable identity. Succeeded and cancelled runs
remain terminal; resume only reconciles their terminal artifacts. The JavaScript
VM itself is not serialized.
`inspect` exposes `resume_count`; terminal run summaries show the same count.

States:

```text
running | waiting_human | pausing | paused | cancelling | succeeded | failed | cancelled
```

A command completed before interruption is cached. A command interrupted with
the controller process may execute again on resume, so side-effecting commands
must carry their own idempotency check.

Every terminal run owns a self-contained `run-summary.html`. Rust generates it
from persisted state and journal as machine/run evidence and never opens a
browser window. It is not the reader-facing delivery report.

A workflow may return the minimal delivery shape:

```json
{
  "summary": "一句话结论",
  "report": "D:\\absolute\\path\\delivery-report.html"
}
```

The reporting stage should use `reader-centered-reporting` for the source,
judgment, evidence, boundaries, and next action, then `l1uyun-surface` for the
HTML display layer. Rust only accepts `report` when it is an absolute path to
an existing non-empty file. A malformed declared report fails the run; a
workflow that does not promise a reader report may omit `report`.

## Rust SDK

The crate exports:

```rust
use servitor_workflows::{
    Engine, Inspection, PublicRun, RunState, RunStatus,
    ServitorTransport, Transport, WorkflowError, WorkflowStore,
};
```

`Engine::new` accepts a custom `Transport`, which keeps orchestration tests and
embedded callers independent from provider processes. `default_engine()` wires
the environment-backed Servitor transport and workflow state store.

## Build and install

```powershell
$env:CARGO_HOME = 'D:\AgentWork\state\cargo\home'
$env:CARGO_TARGET_DIR = 'D:\AgentWork\state\cargo\targets\servitor-workflows'

cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo install --path . --force
```

The installed binary should share its Cargo `bin` directory with
`servitor-worker.exe`.

Real example:

```powershell
servitor-workflows run D:\AgentWork\tools\servitor-workflows\examples\dynamic.workflow.js
```

Pause/resume evidence uses the original run id:

```powershell
servitor-workflows pause RUN_ID
servitor-workflows resume RUN_ID
servitor-workflows inspect RUN_ID
```

After the resumed run reaches a terminal state, the public result includes
`report` only when the workflow produced and returned a validated delivery
artifact. `inspect` exposes the same run's `state.json`, `journal.jsonl`,
`workflow.js`, and `run-summary.html` paths.


## Report ownership (karma #451)

`run-summary.html` generated by this runtime is a **machine run-evidence artifact** (state, journal pointers, call results). It is not a reader-centered delivery report and does not own human judgment or visual craft.

Human-facing delivery HTML must be produced in the workflow reporting stage with `reader-centered-reporting` + `l1uyun-surface`. The public `report` field is accepted only when the workflow returns an absolute path to an existing non-empty file.

## Structured output (karma #449/#447)

`schema` is a prompt-level contract, not a provider-native structured channel. The prompt still asks for bare JSON; extraction also recovers fenced/balanced JSON from free-form model text (strip reasoning → fenced blocks → balanced spans → trailing-comma repair → expected shape). Prefer literal smoke inputs (`INPUT_JSON={...}`) and evaluator `VERDICT=...` lines when a binary decision is required.

