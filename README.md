# servitor-workflows

Dynamic multi-agent workflow layer on top of [servitor](https://github.com/L1uYun/servitor). Parallel fan-out, pipelines, journal-backed replay, sessionful workers, human gates, plan mode, and graceful cancel — all in Python.

## Why

One `servitor run` is one agent call. Real tasks need orchestration: run several agents, pipeline stages, resume from a journal, pause for human approval, or cancel a long fan-out cleanly. servitor-workflows owns that layer; servitor stays transport-only.

## Install

```powershell
pip install -e ../servitor -e .
```

Python 3.11+. Runtime dependency: local `servitor>=0.1.0`. Install both editable projects together.

## 30-second start

```powershell
servitor-workflows run examples/hello_smoke.workflow.py --fresh --output json
servitor-workflows run examples/hello_smoke.workflow.py --plan --output json
```

## Workflow DSL

A workflow is a Python file with `meta = {}` and `async def main(...)`:

```python
meta = {"name": "review", "description": "Two agents review in parallel"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow, plan=False):
    phase("Review")
    a, b = await parallel([
        lambda: agent("Review this code for bugs", {"label": "reviewer-a"}),
        lambda: agent("Review this code for security", {"label": "reviewer-b"}),
    ])
    log(f"a={a}")
    log(f"b={b}")
    return {"a": a, "b": b, "plan": plan}
```

`plan` is exposed so workflows can gate disk/cache side effects during dry runs.

## Commands

```powershell
servitor-workflows run <file.workflow.py> --fresh --output json
servitor-workflows run <file.workflow.py> --resume --output json
servitor-workflows run <file.workflow.py> --plan --output json
servitor-workflows run <file.workflow.py> --cancel-file D:\AgentWork\_tmp\cancel.flag
servitor-workflows map <journal.jsonl>
servitor-workflows summarize <journal.jsonl>
servitor-workflows status <dir>
servitor-workflows compare <dir>
servitor-workflows supervise <dir> --interval 5
```

Useful `run` flags:

- golden path: `--agent` / `--model` / `--plan` / `--resume` / `--fresh` / `--args` / `--budget` / `--output`
- advanced: `--pin-model`, effort family, `--journal` / `--run-id` / `--no-journal`, `--cancel-file`

## Output modes

`run` writes **data to stdout** and **diagnostics to stderr**.

```text
--output human|json|quiet
```

- `human` (default): bounded summary
- `json`: full machine schema (workflow return value)
- `quiet`: suppress stdout
- no legacy mode flags; use `--output` only

On Windows, the CLI initializes stdout/stderr and child Python processes as UTF-8 through the shared Servitor boundary.

## Key APIs

- `agent(prompt, opts)` — one-shot servitor call with journal cache
- `parallel(thunks)` — concurrent fan-out under a semaphore
- `pipeline(items, *stages)` — per-item sequential stages
- `agent.start(prompt, opts)` — sessionful worker
- `agent.waitAny(sessions)` — first actionable session
- `agent.status()` / `agent.stalled(threshold_ms)` — fleet observability
- `human(question, opts)` — declared checkpoint, journaled and replayed

When a turn fails, the agent boundary preserves run evidence (`failure_reason`, `run_dir`, stdout/stderr/metadata paths) instead of collapsing to a bare exception string.

## Isolated write tasks

For a task that modifies a Git project, make its integration evidence explicit:

```python
result = await agent("Implement the approved change, then commit it.", {
    "cwd": r"D:\\AgentWork\\products\\audio-report",
    "isolation": "worktree",
    "worktree_branch": "codex/audio-report-task-slug",
    "verify_command": ["pnpm", "verify"],
})
```

The isolated worktree starts from a recorded base commit. Its journal entry records the branch, final HEAD, dirty state, and (when configured) the verification command and result. A clean worktree is removed after completion; a dirty worktree is retained for inspection. The controller reviews that evidence and performs the merge and any deployment separately.

## Journal

Every run writes `.workflow-journal/<name>.{jsonl,events.jsonl,meta.json,result.json}` beside the workflow file by default, even when launched by absolute path from another cwd. Override with `--journal <path>`. Re-run with `--resume` to skip cached agent calls.

## Plan mode

`--plan` skips model calls (`agent()` returns schema skeletons) but still executes workflow Python. Gate writes with the `plan` flag. Plan-mode fixtures can prove incomplete evidence paths without calling providers.

## Cancellation

`--cancel-file <path>` enables graceful cancellation. The runtime checks the sentinel before each `agent` / `parallel` / `pipeline` / `phase` boundary.

```powershell
New-Item -ItemType File D:\AgentWork\_tmp\cancel.flag
```

In-flight agent calls finish; no new ones start. CLI exits with code 2.

## Windows + pi concurrency

Prefer wave size 1 for `pi` on Windows. Large simultaneous launches can leave prompt-only pending runs. Controllers should set small waves explicitly.

## Role behavior ratchet

Phase 1 is implemented for `code-reviewer`: `docs/role-behavior-ratchet-spec.md`.

The workflow uses two fixed synthetic cases, exact provider/model ids, schema validation, deterministic required/forbidden assertions, journal resume, and evidence fingerprints. `servitor-workflows` owns matrix execution and aggregation; `servitor` remains transport-only.

```powershell
servitor-workflows run examples/role_behavior_eval.workflow.py `
  --args-file <local-args.json> --fresh --output json
```

Select model ids from fresh `servitor models list --agent <agent> --output json` output. Phase 1 acceptance is `aggregate_status=consistent_pass`; transport or schema failures remain `inconclusive`, not provider behavior divergence.

## Testing

All unit tests use fake transport — no real provider calls.

```powershell
python -m pytest -q
```

## License

MIT
