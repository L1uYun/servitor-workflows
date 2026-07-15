"""The agent seam: each agent() unit of work runs as one servitor call.

1:1 Python port of runner/src/codexAgent.js, adapted to use servitor SDK instead
of the Codex app-server JSON-RPC client. servitor provides:
  run_agent(provider_name, prompt, ...) -> {run_dir, session_id}
  wait_for_completion(run_dir, wait_seconds) -> completion status
  read_result(run_dir) -> {ok, result, failure_reason, metadata}
  apply_output_contract(meta, schema=..., ...) -> validated meta

The port preserves: schema strictification, retry classification, model resolution,
agentType loading, worktree isolation, per-agent metrics collection.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time
from typing import Any, Callable

from servitor.providers.utility import _hidden_process_kwargs

from .agent_types import load_agent_type
from .model_map import resolve_model
from .meter import tokens_for_thread
from .worktree import create_worktree, is_git_repo


_SERVITOR_RUN_SCRIPT = """
import json
import sys

import servitor

payload = json.load(sys.stdin)
result = servitor.run_agent(**payload)
json.dump(result, sys.stdout, ensure_ascii=False)
"""


class ServitorAgentError(RuntimeError):
    """Agent failure that preserves the servitor run evidence boundary."""

    def __init__(self, evidence: dict[str, Any]):
        self.evidence = evidence
        for key, value in evidence.items():
            setattr(self, key, value)
        failure = evidence.get("failure_reason") or "unknown"
        run_dir = evidence.get("run_dir") or "unknown"
        super().__init__(f"turn ended with failure_reason={failure}; run_dir={run_dir}")
        # Preserve the existing retry-classification contract.
        self.codex_error_info = failure


async def _terminate_process_tree(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    if sys.platform == "win32":
        killer = await asyncio.create_subprocess_exec(
            "taskkill",
            "/PID",
            str(proc.pid),
            "/T",
            "/F",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            **_hidden_process_kwargs(),
        )
        await killer.communicate()
    else:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=2)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


async def _run_isolated_json(
    payload: dict[str, Any],
    *,
    timeout_seconds: float,
    script: str = _SERVITOR_RUN_SCRIPT,
) -> dict[str, Any]:
    process_kwargs = _hidden_process_kwargs()
    if sys.platform == "win32":
        process_kwargs["creationflags"] = (
            process_kwargs.get("creationflags", 0) | subprocess.CREATE_NEW_PROCESS_GROUP
        )
    else:
        process_kwargs["start_new_session"] = True
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **process_kwargs,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(json.dumps(payload, ensure_ascii=False).encode("utf-8")),
            timeout=timeout_seconds,
        )
    except (asyncio.TimeoutError, asyncio.CancelledError):
        await _terminate_process_tree(proc)
        raise
    if proc.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"isolated servitor worker failed ({proc.returncode}): {detail}")
    try:
        result = json.loads(stdout.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("isolated servitor worker returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise RuntimeError("isolated servitor worker returned a non-object result")
    return result


def strictify_schema(s: dict | None) -> dict | None:
    """Normalize a JSON Schema for strict structured outputs.

    1:1 port of upstream strictifySchema(). Every property must be listed in
    `required` and additionalProperties:false on every object.
    """
    if not s or not isinstance(s, dict):
        return s
    if isinstance(s, list):
        return [strictify_schema(x) for x in s]
    out = dict(s)
    props = out.get("properties")
    if props and isinstance(props, dict) and not isinstance(props, list):
        new_props = {k: strictify_schema(v) for k, v in props.items()}
        out["properties"] = new_props
        out["required"] = list(new_props.keys())
        if "additionalProperties" not in out:
            out["additionalProperties"] = False
    if "items" in out:
        out["items"] = strictify_schema(out["items"])
    for kw in ("anyOf", "oneOf", "allOf"):
        if isinstance(out.get(kw), list):
            out[kw] = [strictify_schema(x) for x in out[kw]]
    for kw in ("$defs", "definitions"):
        if out.get(kw) and isinstance(out[kw], dict):
            out[kw] = {k: strictify_schema(v) for k, v in out[kw].items()}
    return out


def parse_schema_result(text: Any, schema: dict | None) -> Any:
    """Parse a result under an optional schema.

    `apply_output_contract(..., expect_json=True)` may already leave `result` as a
    parsed dict/list. Accept structured values as-is when a schema is present;
    only parse strings. Without a schema the value passes through unchanged.
    """
    if not schema:
        return text
    if text is None:
        return None
    if isinstance(text, (dict, list)):
        return text
    if not isinstance(text, str):
        return text
    import json
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return _extract_json(text)


def _extract_json(text: str) -> Any:
    """Tolerate a model that wraps JSON in prose or ```json fences."""
    if not text:
        return None
    import json
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    candidate = fenced.group(1) if fenced else text
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(candidate[start:end + 1])
        except json.JSONDecodeError:
            pass
    return None


# ── retry classification ──────────────────────────────────────────────────────

_RETRYABLE_CODES = {
    "UsageLimitExceeded", "HttpConnectionFailed",
    "ResponseStreamConnectionFailed", "ResponseStreamDisconnected",
    "ResponseTooManyFailedAttempts", "InternalServerError",
}
_RETRYABLE_MSG = re.compile(
    r"(Transport is not connected|app-server exited|timed out|ECONNRESET|EPIPE|"
    r"socket hang up|stream (disconnected|connection))",
    re.IGNORECASE,
)
_NONRETRYABLE_MSG = re.compile(
    r"(BadRequest|Unauthorized|ContextWindowExceeded|invalid request|outputSchema|"
    r"did not return)",
    re.IGNORECASE,
)


def _error_code(e: Exception) -> str | None:
    """Extract the error code from an exception's codexErrorInfo."""
    ci = getattr(e, "codex_error_info", None) or getattr(e, "codexErrorInfo", None)
    if not ci:
        return None
    if isinstance(ci, str):
        return ci
    if isinstance(ci, dict):
        return next(iter(ci.keys()), None)
    return None


def is_retryable(e: Exception) -> bool:
    """Classify whether an error is retryable. Conservative: unknown = no retry."""
    code = _error_code(e)
    if code:
        return code in _RETRYABLE_CODES
    msg = str(e)
    if _NONRETRYABLE_MSG.search(msg):
        return False
    return bool(_RETRYABLE_MSG.search(msg))


async def _with_retry(fn: Callable, *, retries: int = 3, log=None, label: str | None = None):
    """Retry a coroutine on transient errors with exponential backoff."""
    import random
    attempt = 0
    while True:
        try:
            return await fn()
        except Exception as e:
            if attempt >= retries or not is_retryable(e):
                raise
            attempt += 1
            backoff = min(30_000, 1000 * (2 ** (attempt - 1)))
            wait = backoff + random.randint(0, 250)
            if log:
                log(f"  ⟳ retry {attempt}/{retries} ({label or 'agent'}): "
                    f"{str(e)[:140]} — waiting {wait}ms")
            await asyncio.sleep(wait / 1000)


# ── available models cache ────────────────────────────────────────────────────

_available_models: list[str] = []


def get_available_models() -> list[str]:
    """Return the model ids exposed by the most recent servitor models list."""
    return _available_models


def _refresh_available_models():
    """Refresh the available models cache from servitor."""
    global _available_models
    try:
        import servitor
        rows = servitor.model_rows()
        _available_models = [
            r.get("model") for r in rows if r.get("model")
        ]
    except Exception:
        _available_models = []


# ── the agent seam ────────────────────────────────────────────────────────────

async def servitor_agent(
    prompt: str,
    opts: dict | None = None,
    *,
    log: Callable | None = None,
) -> Any:
    """Run `prompt` as one servitor agent call (with retry).

    1:1 port of upstream codexAgent(). Returns string | parsed dict (schema) | None.

    opts keys: agent, model, schema, effort, system_prompt, agent_type, cwd,
    isolation, worktree_branch, verify_command, verify_timeout_seconds, retries,
    default_model, pinned_model, on_metrics, on_integration, on_progress.
    """
    opts = opts or {}
    _log = log or (lambda *_: None)

    # agentType -> system prompt (+ optional model) from the .claude/agents registry.
    system_prompt = opts.get("system_prompt")
    agent_type_model = None
    if opts.get("agent_type"):
        defn = await asyncio.to_thread(
            load_agent_type, opts["agent_type"], opts.get("cwd")
        )
        if defn:
            if not system_prompt:
                system_prompt = defn.get("system_prompt")
            agent_type_model = defn.get("model")
        else:
            _log(f"agentType '{opts['agent_type']}' not found — using default instructions")

    # pinnedModel overrides per-call model, agentType model, and CLI default.
    pinned = opts.get("pinned_model")
    if pinned and opts.get("model") and opts["model"] != pinned:
        _log(f"pinned model '{pinned}' overrides per-call model '{opts['model']}'")
    requested_model = pinned or opts.get("model") or agent_type_model or opts.get("default_model")

    # Worktree isolation.
    cwd = opts.get("cwd")
    worktree = None
    if opts.get("isolation") == "worktree" and cwd:
        if await is_git_repo(cwd):
            worktree = await create_worktree(cwd, branch=opts.get("worktree_branch"))
            cwd = worktree["dir"]
        else:
            _log(f"isolation:'worktree' ignored — {cwd} is not a git repo")

    integration = None
    try:
        result = await _with_retry(
            lambda: _run_one_turn(prompt, {**opts, "system_prompt": system_prompt,
                                           "requested_model": requested_model, "cwd": cwd, "log": _log}),
            retries=opts.get("retries", 3),
            log=_log,
            label=opts.get("label"),
        )
        if worktree:
            verification = await worktree["verify"](
                opts.get("verify_command"), opts.get("verify_timeout_seconds")
            )
            integration = await worktree["cleanup"]()
            if verification:
                integration["verification"] = verification
            if opts.get("on_integration"):
                opts["on_integration"](integration)
            if verification and verification["exit_code"] != 0:
                raise RuntimeError(
                    f"project verification failed in {integration['dir']}: "
                    f"{verification['command']} (exit {verification['exit_code']})"
                )
        return result
    finally:
        if worktree and integration is None:
            r = await worktree["cleanup"]()
            if opts.get("on_integration"):
                opts["on_integration"](r)
            if not r["removed"]:
                _log(f"worktree kept (modified): {r['dir']}")


async def _run_one_turn(prompt: str, opts: dict) -> Any:
    """Run one servitor agent turn. 1:1 port of upstream runOneTurn()."""
    import servitor
    _log = opts.get("log") or (lambda *_: None)
    started_at = time.monotonic()

    if not _available_models:
        _refresh_available_models()

    model = resolve_model(opts.get("requested_model"), _available_models, _log)

    agent_name = opts.get("agent")
    schema = opts.get("schema")
    if schema:
        schema = strictify_schema(schema)

    # Build kwargs for servitor.run_agent.
    # Launch asynchronously (no timeout_seconds) so the provider uses the
    # detached launcher + _DONE/metadata completion protocol. Then wait here.
    # Passing timeout_seconds would force the sync path and couple the isolated
    # worker lifetime to a single blocking subprocess without the DONE wait path.
    run_kwargs: dict[str, Any] = {
        "provider_name": agent_name,
        "prompt": prompt,
        "cwd": opts.get("cwd") or str(pathlib.Path.cwd()),
    }
    if model:
        run_kwargs["model"] = model
    if opts.get("system_prompt"):
        run_kwargs["system_prompt"] = opts["system_prompt"]
    if opts.get("native_args"):
        run_kwargs["native_args"] = opts["native_args"]
    if opts.get("run_dir_label"):
        run_kwargs["run_dir_label"] = opts["run_dir_label"]
    wait_seconds = float(opts.get("timeout_seconds") or 600)
    # Bound detached provider lifetime to the same deadline the workflow waits on.
    # Prevents orphan pi processes after wait_elapsed.
    run_kwargs["provider_timeout_seconds"] = float(
        opts.get("provider_timeout_seconds") or wait_seconds
    )

    # Isolate the blocking provider call so cancellation can reap its process
    # tree instead of leaving asyncio.run waiting on a default-executor thread.
    # Launch is fast (async spawn); wait is the long part and uses wait_for_completion.
    run_info = await _run_isolated_json(
        run_kwargs,
        timeout_seconds=max(30.0, min(wait_seconds, 120.0)),
    )
    run_dir = run_info.get("run_dir") if isinstance(run_info, dict) else getattr(run_info, "run_dir", None)
    if not run_dir:
        raise RuntimeError(f"servitor.run_agent returned no run_dir: {run_info!r}")

    # Wait until metadata terminal status or _DONE.txt (healed if needed).
    def _wait():
        return servitor.wait_for_completion(run_dir, wait_seconds=wait_seconds)

    meta = await asyncio.to_thread(_wait)
    if not isinstance(meta, dict):
        meta = servitor.read_result(run_dir)
    # If wait elapsed, surface still_running as failure below.
    thread_id = meta.get("session_id") if isinstance(meta, dict) else None

    # Apply output contract if schema is provided
    if schema or opts.get("check_contains") or opts.get("check_regex"):
        meta = servitor.apply_output_contract(
            meta,
            expect_json=schema is not None,
            schema=schema,
            check_contains=opts.get("check_contains"),
            check_regex=opts.get("check_regex"),
        )

    # Per-agent metrics: wall time, tokens (if available from servitor metadata)
    metrics = {
        "ms": int((time.monotonic() - started_at) * 1000),
        "model": model,
        "tokens": tokens_for_thread(thread_id) if thread_id else None,
    }
    if opts.get("on_metrics"):
        opts["on_metrics"](metrics)

    result_text = meta.get("result") if isinstance(meta, dict) else None
    ok = meta.get("ok") if isinstance(meta, dict) else False

    if not ok:
        failure = meta.get("failure_reason") if isinstance(meta, dict) else "unknown"
        if failure == "interrupted":
            return None
        run_path = pathlib.Path(run_dir)
        evidence = {
            "failure_reason": failure,
            "run_dir": str(run_path),
            "metadata_path": str(run_path / "metadata.json"),
            "stdout_path": str(run_path / "stdout.txt"),
            "stderr_path": str(run_path / "stderr.txt"),
            "model": model,
            "provider": agent_name,
            "metadata": meta,
        }
        raise ServitorAgentError(evidence)

    return parse_schema_result(result_text, schema)


async def shutdown_client():
    """No persistent client to shut down (servitor uses one-shot CLI calls)."""
    pass


async def get_client(opts: dict | None = None):
    """Refresh available models and return None (no persistent client needed)."""
    _refresh_available_models()
    return None
