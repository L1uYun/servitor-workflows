# servitor-workflows

1:1 Python port of [claude-dynamic-workflows-codex](https://github.com/scasella/claude-dynamic-workflows-codex) — run dynamic-workflow scripts on local servitor subagents instead of Codex app-server.

## Design

`servitor` remains transport-only. This package adds: workflow DSL, runtime, deterministic provider defaults, journal/resume, sessionful workers, human gates, run sidecars, text status, analytics, and fleet supervision. HTML viewer parity is deferred.

**No upstream code is copied.** This is an independent Python implementation of the same concept, ported module-by-module from the upstream JS source.

## Installation

```powershell
pip install -e .
```

Requires Python 3.11+. Depends on servitor (install it first: pip install -e ../servitor). No other dependencies.## Usage

```powershell
# Run a workflow (plan mode: no model calls)
servitor-workflows run examples/hello.workflow.py --plan --auto-effort --json

# Run with real servitor agents; default provider is pi when available
servitor-workflows run examples/hello_smoke.workflow.py --fresh --json

# Replay from journal cache
servitor-workflows run examples/hello_smoke.workflow.py --resume --json

# Override provider only when needed
servitor-workflows run examples/hello_smoke.workflow.py --agent codebuddy --model kimi-for-coding --fresh --json

# ASCII execution DAG
servitor-workflows map .workflow-journal/hello.workflow.jsonl

# Run summary
servitor-workflows summarize .workflow-journal/hello.workflow.jsonl

# Fleet status
servitor-workflows status .workflow-journal
```

## Testing

All tests use fake transport — no real provider calls.

```powershell
cd D:\AgentWork\tools\servitor-workflows
python -m pytest -q
```

## Module mapping (upstream JS → this package)

| Upstream | This package | Status |
|----------|-------------|--------|
| `runner/src/journal.js` | `journal.py` | ✅ |
| `runner/src/runtime.js` | `runtime.py` + `session_runtime.py` | ✅ |
| `runner/src/runWorkflow.js` | `run_workflow.py` | ✅ |
| `runner/src/codexAgent.js` | `servitor_agent.py` | ✅ |
| `runner/src/codexSession.js` | `servitor_session.py` | ✅ |
| `runner/src/meter.js` | `meter.py` | ✅ |
| `runner/src/modelMap.js` | `model_map.py` | ✅ |
| `runner/src/agentTypes.js` | `agent_types.py` | ✅ |
| `runner/src/worktree.js` | `worktree.py` | ✅ |
| `runner/src/runModel.js` | `run_model.py` | ✅ |
| `runner/src/runSummary.js` | `run_summary.py` | ✅ |
| `runner/src/asciiMap.js` | `ascii_map.py` | ✅ |
| `runner/src/fleetStatus.js` | `fleet_status.py` | ✅ |
| `runner/src/compareRuns.js` | `compare_runs.py` | implemented; tests pending |
| `runner/bin/run-workflow.js` | `cli.py` (`run`) | ✅ |
| `runner/bin/summarize-run.js` | `cli.py` (`summarize`) | ✅ |
| `runner/bin/map-run.js` | `cli.py` (`map`) | ✅ |
| `runner/bin/fleet.js` | `cli.py` (`status`) | ✅ |
| `runner/bin/view-run.js` | — (HTML viewer) | deferred |
| `runner/bin/supervise.js` | `supervise.py` + `cli.py` (`supervise`) | implemented; tests pending |

## Phase status

- Phase 0: package skeleton + transport ✅
- Phase 1: MVP runtime (agent/parallel/pipeline/journal) ✅ 22 tests
- Phase 2: sessionful workers (start/steer/wait/waitAny/cancel/close) ✅ 7 tests
- Phase 3: human gates (questions/answers sidecars) ✅ 6 tests
- Phase 4: run model & sidecars ✅ 5 tests
- Phase 5: summary & analytics ✅
- Phase 6: ASCII map viewer ✅ 3 tests
- Phase 7: fleet status ✅
- Phase 8: agent-dispatch integration ✅ route table slimmed into `agent-dispatch`
- Phase 9: provider defaults ✅ `pi`-first default recorded in meta/events/journal

## Follow-up plan

Current plan source: `D:\AgentWork\tools\servitor-workflows\GOAL-CONTRACT.md`.

Priority order: Codex-as-servitor provider next if needed, then tests for `compare_runs` / `supervise`. HTML viewer is explicitly deferred until requested.
