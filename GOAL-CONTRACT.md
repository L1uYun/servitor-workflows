# Servitor Workflows Rust Contract

## Purpose

`servitor-workflows` is a Rust dynamic-workflow runtime above the Rust Servitor
transport. A workflow is readable JavaScript generated for a task; Rust owns
execution, persistence, scheduling, limits, recovery, cancellation, commands,
and human gates.

Servitor remains the transport owner for agent processes. The workflow runtime
does not contain provider adapters, model routing, provider fallback, token
budgets, Git policy, CI policy, deployment policy, or UI.

## Script contract

Workflow files use the Claude Dynamic Workflows shape:

```javascript
export const meta = {
  name: "audit-routes",
  description: "Audit routes and verify findings",
};

const found = await agent("List route files.", {
  schema: {
    type: "object",
    required: ["files"],
    properties: { files: { type: "array", items: { type: "string" } } },
  },
});

const audits = await pipeline(found.files, file =>
  agent(`Audit ${file}.`, { label: file }),
);

return audits;
```

The runtime provides these globals:

```text
args
agent(prompt, options?)
command(program, args?, options?)
gate(question, options?)
phase(name)
parallel(promises)
pipeline(items, worker)
```

`agent`, `command`, and `gate` are the three execution primitives. JavaScript
supplies dynamic discovery, loops, branching, fan-out, aggregation, evaluator
cycles, and stop conditions. `parallel` and `pipeline` are small readable
helpers, not separate scheduler concepts.

Agent options:

```text
label, agent, model, cwd, systemPrompt, timeoutSeconds, nativeArgs, schema
```

`agent` defaults to `pi`. With `schema`, the runtime asks for JSON, parses the
result, validates it, and returns the structured value to the script.

Command options:

```text
label, cwd, env, timeoutSeconds
```

Commands run without a visible Windows console. Their result is:

```json
{"exitCode":0,"stdout":"...","stderr":"..."}
```

Gate options contain an optional `label`. A pending gate pauses the run. An
approved run is replayed from the start; completed calls return from the cache,
the gate returns its recorded decision, and execution continues.

## Recovery model

The runtime does not serialize a JavaScript VM. It reruns the same script and
uses stable call identities plus occurrence indexes:

```text
sha256(kind + input + output-affecting options) + occurrence
```

- completed calls return their journaled result;
- submitted Servitor calls reconnect to their durable Servitor run;
- interrupted local commands run again;
- pending gates pause at the same call;
- changed inputs create a different identity and execute normally.

This makes ordinary JavaScript loops and branches resumable without a custom
workflow DSL. Workflow authors must keep side-effecting commands idempotent or
place their own check before repeating them.

## Runtime state

Default owner root:

```text
D:\AgentWork\state\servitor-workflows
```

Override: `SERVITOR_WORKFLOWS_STATE_ROOT`.

Each run owns:

```text
runs/<RUN_ID>/workflow.js
runs/<RUN_ID>/state.json
runs/<RUN_ID>/journal.jsonl
runs/<RUN_ID>/pause.request
runs/<RUN_ID>/cancel.request
```

Workflow states:

```text
running | waiting_human | pausing | paused | cancelling | succeeded | failed | cancelled
```

## CLI contract

```text
run WORKFLOW.js [--args JSON] [--max-parallel N] [--max-calls N]
resume RUN_ID
get RUN_ID
approve RUN_ID --reason TEXT
reject RUN_ID --reason TEXT
pause RUN_ID
cancel RUN_ID
inspect RUN_ID
```

`get` is the low-noise public surface: status, active calls, current gate,
result, or public error. `inspect` adds paths, limits, timestamps, decisions,
and complete persisted state.

Defaults and hard ceilings:

```text
max_parallel = min(16, available CPU floor 1)
max_calls = 1000
```

No hidden retry, provider fallback, or token budget exists. A script expresses
its own bounded repair loop and no-progress stop condition.

## Non-goals

- Python execution or compatibility.
- A TOML DAG as the core workflow format.
- Node/V8/Deno as a runtime dependency.
- Direct filesystem APIs inside JavaScript.
- Plugin/executor registries.
- Hard-coded Git, PR, CI, deploy, or neat implementations.
- Roles, model maps, dashboards, ASCII maps, fleet views, compare, or supervise.

## Acceptance

1. `cargo fmt --all --check` passes.
2. `cargo clippy --all-targets -- -D warnings` passes.
3. `cargo test` proves dynamic fan-out, actual concurrency, structured agent
   output, command execution, human gate replay, resume cache, pause, and cancel.
4. A real Pi workflow completes through the Rust Servitor SDK.
5. The installed `servitor-workflows.exe` resolves from the D-drive Cargo home.
6. Python code, metadata, generated journals, and obsolete viewer assets are
   removed.


## Structured agent output boundary (karma #449/#447/#451)

- `schema` is a **prompt contract**, not a provider-native structured-output channel. The runtime asks for bare JSON and validates the extracted value.
- Extraction is intentional and bounded: strip reasoning wrappers, collect candidates (whole text / fenced ```json / balanced spans), trailing-comma repair, then **schema-valid selection** (last valid wins). Schema participates in candidate selection; this is model-agnostic, not a provider-specific fallback. Transport may still strip protocol delimiters (e.g. Pi think wrappers).
- Real smoke prompts must be **literal and unambiguous** (prefer embedding a concrete `INPUT_JSON={...}` block). Avoid words like `beta` that models treat as product flags.
- Evaluators that only need a pass/fail should require an explicit verdict line such as `VERDICT=APPROVED` / `VERDICT=REJECTED` in addition to any schema, so prose wrapping cannot hide the decision.
- Reader-facing delivery HTML is not generated by Rust; workflows return an absolute report path after `reader-centered-reporting` + `l1uyun-surface`.

