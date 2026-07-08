# servitor-workflows

Run dynamic multi-agent workflows on top of [servitor](https://github.com/L1uYun/servitor). Parallel fan-out, pipelines, journal-backed replay, sessionful workers, and human gates — all in Python.

## Why

One `servitor run` call is one agent. But real tasks need orchestration: run three agents in parallel, pipeline results through stages, resume from a journal cache, or pause for human approval. servitor-workflows adds that layer without locking you into a GUI or a hosted platform.

## Install

```
pip install -e .
```

Python 3.11+. Depends on `servitor` (install it first).

## 30-second start

```powershell
servitor-workflows run examples/hello_smoke.workflow.py --fresh --json
```

## Workflow DSL

A workflow is a Python file with `meta = {}` and `async def main(...)`:

```python
meta = {"name": "review", "description": "Two agents review in parallel"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Review")
    a, b = await parallel([
        lambda: agent("Review this code for bugs", {"label": "reviewer-a"}),
        lambda: agent("Review this code for security", {"label": "reviewer-b"}),
    ])
    log(f"a={a}")
    log(f"b={b}")
    return {"a": a, "b": b}
```

## Commands

```powershell
servitor-workflows run <file.workflow.py> --fresh --json     # run with real agents
servitor-workflows run <file.workflow.py> --resume --json     # replay from journal
servitor-workflows run <file.workflow.py> --plan --json       # dry run, no model calls
servitor-workflows map <journal.jsonl>                        # ASCII execution DAG
servitor-workflows summarize <journal.jsonl>                  # run summary
servitor-workflows status <dir>                               # fleet status
servitor-workflows compare <dir>                              # compare runs over time
servitor-workflows supervise <dir> --interval 5               # poll fleet status
```

## Key APIs

- `agent(prompt, opts)` — one-shot servitor call with journal cache
- `parallel(thunks)` — concurrent fan-out under a semaphore
- `pipeline(items, *stages)` — per-item sequential stages
- `agent.start(prompt, opts)` — sessionful worker (returns before turn completes)
- `agent.waitAny(sessions)` — first actionable session
- `agent.status()` / `agent.stalled(threshold_ms)` — fleet observability
- `human(question, opts)` — declared checkpoint, journaled and replayed

## Journal

Every run writes `.workflow-journal/<name>.{jsonl,events.jsonl,meta.json,result.json}`. Re-run with `--resume` to skip cached agent calls. State is the source of truth — not memory.

## Testing

All tests use fake transport — no real provider calls.

```
python -m pytest -q
```

## License

MIT
