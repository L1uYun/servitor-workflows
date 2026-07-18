# servitor-workflows

Rust orchestration above the Rust `servitor` transport.

`servitor` owns agent process submission, inspection, cancellation, and output.
`servitor-workflows` owns dynamic workflow execution, concurrency, commands,
human gates, persisted state, replay, pause, cancellation, and the final
reader-facing HTML delivery report.

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

The default agent is `pi`. The runtime does not set a token budget, retry a
provider silently, or fall back to another provider.

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

## CLI

```text
servitor-workflows run WORKFLOW.js [--args JSON] [--max-parallel N] [--max-calls N]
servitor-workflows resume RUN_ID
servitor-workflows get RUN_ID
servitor-workflows approve RUN_ID --reason TEXT
servitor-workflows reject RUN_ID --reason TEXT
servitor-workflows pause RUN_ID
servitor-workflows cancel RUN_ID
servitor-workflows inspect RUN_ID
```

Output modes:

```text
--output json    machine-facing public result, default
--output human   compact status and result
--output quiet   exit status only
```

`run`, `resume`, `get`, `approve`, `reject`, `pause`, and `cancel` return the
low-noise public shape: `run_id`, `status`, and only relevant phase, active
calls, gate, result, error, or terminal `report` path. `inspect` is the explicit
detailed surface and adds persisted state plus owner paths.

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
runs/<RUN_ID>/report.html
runs/<RUN_ID>/pause.request
runs/<RUN_ID>/cancel.request
```

Resume replays the same JavaScript. Stable call identity plus an occurrence
index returns completed calls from the journal and reconnects submitted agent
calls through their Servitor run id. The JavaScript VM itself is not serialized.
`inspect` exposes `resume_count`; terminal HTML reports show the same count.

States:

```text
running | waiting_human | pausing | paused | cancelling | succeeded | failed | cancelled
```

A command completed before interruption is cached. A command interrupted with
the controller process may execute again on resume, so side-effecting commands
must carry their own idempotency check.

Every terminal run owns a self-contained `report.html`. It is generated from
the persisted state and journal, keeps raw detail behind progressive
disclosure, and does not open a browser window. Resuming an older terminal run
backfills a missing report without rerunning completed workflow calls.

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
`report`. `inspect` exposes the same run's `state.json`, `journal.jsonl`,
`workflow.js`, and `report.html` paths.
