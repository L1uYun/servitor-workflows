"""Load a Python workflow file and run it with the runtime API injected.

1:1 Python port of runner/src/runWorkflow.js. The JS version uses node:vm to host
the workflow body in an isolated context; Python uses exec() with a restricted
global dict containing only the injected workflow API + Python builtins.

Workflow files are Python modules with an `async def main(...)` that accepts the
injected API:
    meta = {"name": "hello", "description": "smoke test"}

    async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
        phase("Answer")
        result = await agent("Reply with one word: pong.")
        return {"pong": result}
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import sys
import time
from pathlib import Path
from typing import Any

from .runtime import create_runtime


def extract_meta(src: str) -> dict | None:
    """Extract the `meta` dict literal from a workflow source.

    1:1 port of upstream extractMeta(). Anchored to line-start so a comment
    mentioning `meta` can't shadow the real declaration.
    """
    # Match `meta = {` at line start (with optional whitespace)
    tree = ast.parse(src)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "meta":
                    try:
                        return ast.literal_eval(node.value)
                    except (ValueError, SyntaxError):
                        return None
    return None


def _captured_provider_names() -> list[str]:
    """Return servitor captured providers, or an empty list if servitor is unavailable."""
    try:
        import servitor
        return list(servitor.provider_names(captured_only=True))
    except Exception:
        return []


def _meta_default_agent(meta: dict | None) -> str | None:
    if not isinstance(meta, dict):
        return None
    for key in ("agent", "default_agent", "defaultAgent"):
        value = meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def resolve_default_agent(explicit_agent: str | None, meta: dict | None = None) -> tuple[str | None, str]:
    """Resolve the workflow default provider with pi-first low-friction policy.

    Precedence: explicit CLI/API default > workflow meta default > captured provider.
    """
    if explicit_agent:
        return explicit_agent, "flag"
    meta_agent = _meta_default_agent(meta)
    if meta_agent:
        return meta_agent, "workflow"
    captured = _captured_provider_names()
    for preferred in ("pi", "codebuddy", "claude", "agy-tui"):
        if preferred in captured:
            return preferred, "auto"
    return (captured[0], "auto") if captured else (None, "none")


async def run_workflow_file(script_path: str, options: dict | None = None) -> Any:
    """Load and run a workflow file.

    1:1 port of upstream runWorkflowFile().
    """
    src = Path(script_path).read_text(encoding="utf-8")
    return await run_workflow_source(src, {**options, "script_path": script_path})


async def run_workflow_source(src: str, options: dict | None = None) -> Any:
    """Load and run workflow source code with the runtime API injected.

    1:1 port of upstream runWorkflowSource().
    """
    options = options or {}
    meta = extract_meta(src)
    if options.get("default_agent_source"):
        effective_default_agent = options.get("default_agent")
        effective_default_agent_source = options.get("default_agent_source")
    else:
        effective_default_agent, effective_default_agent_source = resolve_default_agent(
            options.get("default_agent"), meta
        )
    if options.get("on_event") and effective_default_agent:
        options["on_event"]({
            "type": "defaults",
            "t": time.time(),
            "agent": effective_default_agent,
            "source": effective_default_agent_source,
        })
    runtime = create_runtime(**{k: v for k, v in options.items()
                                if k in ("args", "budget_total", "budget_meter", "defaults",
                                         "default_model", "pinned_model", "auto_effort",
                                         "pinned_effort", "plan", "on_phase", "on_log",
                                         "on_agent_plan", "on_event", "on_progress",
                                         "journal", "run_agent", "start_session", "human_channel")},
                             default_agent=effective_default_agent)

    if not options.get("nested") and meta and meta.get("name"):
        runtime["log"](f"▶ {meta['name']}" +
                       (f" — {meta['description']}" if meta.get("description") else ""))

    # Load the workflow module from source
    spec = importlib.util.spec_from_loader("__workflow__", loader=None)
    module = importlib.util.module_from_spec(spec)
    # Provide __file__ for the workflow module
    script_path = options.get("script_path")
    if script_path:
        module.__file__ = script_path
    # Inject asyncio into the workflow namespace so async helpers work
    module.__dict__["asyncio"] = asyncio
    exec(compile(src, script_path or "<workflow>", "exec"), module.__dict__)

    if not hasattr(module, "main"):
        raise SyntaxError("Workflow file must define `async def main(...)`")
    main_func = module.main
    if not callable(main_func):
        raise SyntaxError("Workflow `main` must be a callable")

    try:
        api = {k: runtime[k] for k in ("agent", "parallel", "pipeline", "phase", "log",
                                       "budget", "args", "human", "workflow")}
        # Attach sessionful observability methods to agent
        if "agent_status" in runtime:
            api["agent"].status = runtime["agent_status"]  # type: ignore
        if "agent_stalled" in runtime:
            api["agent"].stalled = runtime["agent_stalled"]  # type: ignore
        result = await main_func(**api)
        return result
    finally:
        try:
            await runtime["finalize"]()
        except Exception:
            pass
