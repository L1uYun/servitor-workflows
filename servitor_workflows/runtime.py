"""Provider-neutral runtime: agent/parallel/pipeline/phase/log/budget/args/workflow/human.

1:1 Python port of runner/src/runtime.js createRuntime(). Nothing here mentions
servitor or any specific provider — only agent() reaches a model, via the
servitor_agent seam. Concurrency is capped at min(16, cpu-2), with a hard
1000-agent backstop.
"""
from __future__ import annotations

import asyncio
import contextvars
import os
import sys
import time
from typing import Any, Callable

from .journal import Journal, identity_hash
from .meter import tokens_spent, output_spent
def effort_for_layer_width(width: int) -> str:
    """Thinking effort scales INVERSELY with layer width.

    width 1   -> xhigh   (sole agent: critical gate)
    width >= 2 -> high    (any fan-out: floor)
    """
    return "xhigh" if width <= 1 else "high"


def _default_cap() -> int:
    """Default agent concurrency.

    CPU-derived cap is fine for local fake transports, but real Windows+pi
    launches stall when many agents start at once. Prefer a lower default on
    Windows unless SERVITOR_WORKFLOW_CAP overrides.
    """
    env = os.environ.get("SERVITOR_WORKFLOW_CAP")
    if env:
        try:
            return max(1, min(16, int(env)))
        except ValueError:
            pass
    cpu_cap = min(16, max(1, (os.cpu_count() or 4) - 2))
    if sys.platform == "win32":
        return min(cpu_cap, 2)
    return cpu_cap


_CAP = _default_cap()
_AGENT_CAP = 1000

# ── layer-width context (Python's contextvars = JS's AsyncLocalStorage) ──────
_layer_width = contextvars.ContextVar("_workflow_layer_width", default=1)


def _current_layer_width() -> int:
    return _layer_width.get()


def _schema_skeleton(schema: dict | None) -> Any:
    """Build a minimal value satisfying a JSON Schema, for --plan dry runs."""
    if not schema or not isinstance(schema, dict):
        return ""
    return _skel(schema)


def _skel(s: dict | None) -> Any:
    if not s or not isinstance(s, dict):
        return None
    enum = s.get("enum")
    if isinstance(enum, list) and enum:
        return enum[0]
    if s.get("oneOf") or s.get("anyOf"):
        return _skel((s.get("oneOf") or s.get("anyOf"))[0])
    t = s.get("type")
    if isinstance(t, list):
        t = t[0] if t else None
    if t == "object" or (not t and s.get("properties")):
        o = {}
        for k in (s.get("properties") or {}):
            o[k] = _skel(s["properties"][k])
        return o
    if t == "array":
        return []
    if t in ("number", "integer"):
        return 0
    if t == "boolean":
        return False
    if t == "string":
        return ""
    return None


# ── semaphore (single global, only agent() consumes a slot) ──────────────────

_active = 0
_waiters: list[asyncio.Future] = []


async def _acquire():
    global _active
    if _active < _CAP:
        _active += 1
        return
    fut = asyncio.get_event_loop().create_future()
    _waiters.append(fut)
    await fut
    _active += 1


def _release():
    global _active
    _active -= 1
    if _waiters:
        nxt = _waiters.pop(0)
        if not nxt.done():
            _active += 1
            nxt.set_result(None)
            _active -= 1  # net: we released one, gave it to the waiter


def active_slots() -> int:
    """Test/observability hook: in-flight model slots."""
    return _active


async def _pooled(thunk: Callable) -> Any:
    await _acquire()
    try:
        return await thunk()
    finally:
        _release()


# ── create_runtime ────────────────────────────────────────────────────────────

def create_runtime(
    *,
    args: dict | None = None,
    budget_total: int | None = None,
    budget_meter: str = "total",
    defaults: dict | None = None,
    default_model: str | None = None,
    default_agent: str | None = None,
    pinned_model: str | None = None,
    auto_effort: bool = False,
    pinned_effort: str | None = None,
    plan: bool = False,
    on_phase: Callable | None = None,
    on_log: Callable | None = None,
    on_agent_plan: Callable | None = None,
    on_event: Callable | None = None,
    on_progress: Callable | None = None,
    journal: Journal | None = None,
    run_agent: Callable | None = None,
    start_session: Callable | None = None,
    human_channel: dict | None = None,
) -> dict:
    """Create the workflow runtime API dict.

    1:1 port of upstream createRuntime(). Returns a dict with:
    agent, parallel, pipeline, phase, log, budget, args, workflow, human, CAP, finalize.
    agent.start and agent.waitAny are attached for sessionful workers (Phase 2).
    """
    nonlocal_state = {
        "agent_count": 0,
        "current_phase": None,
        "session_seq": 0,
        "human_seq": 0,
        "human_occ": {},
    }
    open_sessions: list = []

    _run_agent = run_agent
    if _run_agent is None:
        from .servitor_agent import servitor_agent
        _run_agent = servitor_agent

    _defaults = defaults or {}
    _args = args or {}

    def _bump_agent_count():
        nonlocal_state["agent_count"] += 1
        if nonlocal_state["agent_count"] > _AGENT_CAP:
            raise RuntimeError(f"Agent cap ({_AGENT_CAP}) exceeded — runaway workflow?")

    def _check_budget():
        if budget_total:
            spent = tokens_spent() if budget_meter == "output" else tokens_spent()
            if budget_meter == "output":
                spent = output_spent()
            else:
                spent = tokens_spent()
            if spent >= budget_total:
                err = RuntimeError(
                    f"Token budget exhausted ({spent}/{budget_total} {budget_meter} tokens)"
                )
                setattr(err, "code", "BUDGET_EXCEEDED")
                raise err

    def _resolve_effort(call_opts: dict, width: int) -> tuple[str | None, str]:
        if pinned_effort is not None:
            return pinned_effort, "pin"
        if call_opts.get("effort") is not None:
            return call_opts["effort"], "call"
        if auto_effort:
            return effort_for_layer_width(width), "auto"
        if _defaults.get("effort") is not None:
            return _defaults["effort"], "flag"
        return None, "default"

    def _record_event(event_type: str, **kwargs):
        if on_event:
            evt = {"type": event_type, "t": time.time(), **kwargs}
            on_event(evt)

    def _log(message: str):
        if on_log:
            on_log(message)

    # ── agent() ─────────────────────────────────────────────────────────────
    async def agent(prompt: str, opts: dict | None = None) -> Any:
        opts = opts or {}
        _bump_agent_count()
        merged = {**_defaults, **opts}
        agent_name = opts.get("agent") or default_agent
        if agent_name:
            merged["agent"] = agent_name
        label = opts.get("label") or (prompt[:64] if isinstance(prompt, str) else "agent")
        effective_phase = opts.get("phase") or nonlocal_state["current_phase"]

        width = _current_layer_width()
        resolved_effort, effort_src = _resolve_effort(opts, width)
        if resolved_effort is None:
            merged.pop("effort", None)
        else:
            merged["effort"] = resolved_effort

        # --plan dry run
        if plan:
            if on_agent_plan:
                on_agent_plan({
                    "label": label, "phase": effective_phase,
                    "effort": resolved_effort, "width": width,
                    "schema": bool(opts.get("schema")),
                    "agent": agent_name,
                })
            return _schema_skeleton(opts.get("schema"))

        _check_budget()

        # Journal: allocate key and check cache hit
        key = None
        if journal:
            opts_for_key = {**merged, "agent": agent_name, "model": pinned_model or opts.get("model") or default_model}
            key = journal.next_key(prompt, opts_for_key)
            if journal.hit(key):
                _log(f"  ◦ agent (cached): {label}")
                _record_event("cached", id=key, label=label, phase=effective_phase, agent=agent_name)
                return journal.get(key)

        req_model = pinned_model or opts.get("model") or default_model
        effort_tag = f"  ⟪{resolved_effort}⟫" if resolved_effort else ""
        _log(f"  · agent: {label}{('  [schema]' if opts.get('schema') else '')}{effort_tag}")
        _record_event(
            "start", id=key, label=label, phase=effective_phase,
            effort=resolved_effort, model=req_model, agent=agent_name,
        )

        # Capture per-agent metrics
        metrics_holder: dict = {}

        def _on_metrics(m):
            metrics_holder.update(m)

        call_opts = {**merged, "agent": agent_name, "default_model": default_model, "pinned_model": pinned_model,
                     "log": _log, "on_metrics": _on_metrics}
        if on_progress:
            call_opts["on_progress"] = lambda text: on_progress(label, text, key)

        result = await _pooled(lambda: _run_agent(prompt, call_opts))

        _record_event(
            "end", id=key, label=label, phase=effective_phase,
            agent=agent_name,
            effort=resolved_effort,
            model=metrics_holder.get("model", req_model),
            tokens=metrics_holder.get("tokens", {}).get("total") if metrics_holder.get("tokens") else None,
            ms=metrics_holder.get("ms"),
        )

        if key and journal:
            meta = {
                "phase": effective_phase,
                "agent": agent_name,
                "effort": resolved_effort,
                "model": metrics_holder.get("model", req_model),
            }
            tok = metrics_holder.get("tokens")
            if tok:
                meta["tokens"] = tok.get("total")
                meta["tokensOut"] = tok.get("output", 0) + tok.get("reasoning", 0)
            if metrics_holder.get("ms") is not None:
                meta["ms"] = metrics_holder["ms"]
            journal.record(key, label, result, meta)

        return result

    # ── parallel() ──────────────────────────────────────────────────────────
    async def parallel(thunks: list) -> list:
        width = len(thunks)
        async def _run_one(thunk):
            token = _layer_width.set(width)
            try:
                try:
                    result = thunk()
                    if asyncio.iscoroutine(result):
                        result = await result
                    return result
                except Exception as e:
                    _log(f"  ! parallel task failed: {e}")
                    return None
            finally:
                _layer_width.reset(token)
        return list(await asyncio.gather(*[_run_one(t) for t in thunks]))

    # ── pipeline() ──────────────────────────────────────────────────────────
    async def pipeline(items: list, *stages: Callable) -> list:
        width = len(items)
        async def _run_item(item, index):
            token = _layer_width.set(width)
            try:
                value = item
                for stage in stages:
                    try:
                        import inspect as _insp
                        _np = len(_insp.signature(stage).parameters)
                        if _np >= 3:
                            result = stage(value, item, index)
                        elif _np == 2:
                            result = stage(value, item)
                        else:
                            result = stage(value)
                        if asyncio.iscoroutine(result):
                            result = await result
                        if result is None:
                            return None
                        value = result
                    except Exception as e:
                        _log(f"  ! pipeline item {index} dropped: {e}")
                        return None
                return value
            finally:
                _layer_width.reset(token)
        return list(await asyncio.gather(*[_run_item(it, i) for i, it in enumerate(items)]))

    # ── phase() / log() ─────────────────────────────────────────────────────
    def phase(title: str):
        nonlocal_state["current_phase"] = title
        if on_phase:
            on_phase(title)

    def log(message: str):
        _log(message)

    # ── budget ──────────────────────────────────────────────────────────────
    budget = {
        "total": budget_total,
        "spent": lambda: (output_spent() if budget_meter == "output" else tokens_spent()),
        "remaining": lambda: (max(0, budget_total - (output_spent() if budget_meter == "output" else tokens_spent())) if budget_total else float("inf")),
    }

    # ── workflow() ──────────────────────────────────────────────────────────
    async def workflow(ref: Any, sub_args: dict | None = None):
        from .run_workflow import run_workflow_file
        script_path = None
        if ref and isinstance(ref, dict) and ref.get("scriptPath"):
            script_path = ref["scriptPath"]
        else:
            name = ref if isinstance(ref, str) else (ref.get("name") if ref else None)
            if not name:
                raise ValueError("workflow(): pass a {scriptPath}, a name, or {name}")
            # Path-like or named workflow
            if "/" in name or "\\" in name or name.endswith((".py", ".workflow.py")):
                script_path = name
            else:
                script_path = _resolve_named_workflow(name)
        return await run_workflow_file(script_path, {
            "args": sub_args, "budget_total": budget_total, "budget_meter": budget_meter,
            "defaults": _defaults, "default_agent": default_agent,
            "default_model": default_model, "pinned_model": pinned_model,
            "auto_effort": auto_effort, "pinned_effort": pinned_effort, "plan": plan,
            "on_phase": on_phase, "on_log": on_log, "on_agent_plan": on_agent_plan,
            "on_event": on_event, "journal": journal, "start_session": start_session,
            "human_channel": human_channel, "nested": True,
        })

    # ── human() (stub — full impl in Phase 3) ───────────────────────────────
    async def human(question: str, opts: dict | None = None) -> Any:
        opts = opts or {}
        choices = [str(c) for c in opts["choices"]] if isinstance(opts.get("choices"), list) and opts["choices"] else None
        default = opts.get("default") if opts.get("default") is not None else (choices[0] if choices else None)
        qid = str(opts.get("id")) if opts.get("id") is not None else f"q{nonlocal_state['human_seq'] + 1}"
        if opts.get("id") is None:
            nonlocal_state["human_seq"] += 1

        if plan:
            return default

        occ = nonlocal_state["human_occ"].get(qid, 0)
        nonlocal_state["human_occ"][qid] = occ + 1
        key = f"human:{qid}#{occ}"

        # args.checkpointAnswers
        pre = None
        ca = _args.get("checkpointAnswers") if isinstance(_args, dict) else None
        if isinstance(ca, dict) and qid in ca:
            pre = ca[qid]
            _log(f"  ⊟ human ({qid}): answered from args.checkpointAnswers")
            if journal:
                journal.record(key, qid, pre, {"human": True, "question": str(question), "source": "args"})
            return pre

        # journal replay
        if journal and journal.reuse and journal.hit(key):
            _log(f"  ⊟ human ({qid}): answer replayed from the journal")
            _record_event("cached", id=key, label=qid, kind="human")
            return journal.get(key)

        # live channel
        if human_channel:
            payload = {"id": key, "qid": qid, "question": str(question), "choices": choices, "default": default}
            _log(f"  ⊟ human ({qid}): waiting for an answer — {str(question)[:80]}")
            _record_event("question", id=key, label=qid, kind="human", question=payload["question"], choices=choices, default=default)
            try:
                human_channel.get("notify", lambda *_: None)(payload)
            except Exception:
                pass
            got = None
            try:
                got = await asyncio.wait_for(
                    human_channel.get("wait", lambda *_: None)(key, {"timeoutMs": opts.get("timeoutMs", 600000)}),
                    timeout=(opts.get("timeoutMs", 600000) / 1000),
                )
            except (asyncio.TimeoutError, Exception):
                pass
            if got and got.get("answer") is not None:
                _log(f"  ⊟ human ({qid}): answered")
                _record_event("answered", id=key, label=qid, kind="human")
                if journal:
                    journal.record(key, qid, got["answer"], {"human": True, "question": str(question), "source": "live"})
                return got["answer"]

        _log(f"  ⊟ human ({qid}): no answer — using the default")
        if journal:
            journal.record(key, qid, default, {"human": True, "question": str(question), "source": "default"})
        return default

    # ── sessionful workers (Phase 2) ──────────────────────────────────────────
    from .session_runtime import LiveAgentSession, wait_any_live

    _start_session_fn = start_session
    if _start_session_fn is None:
        from .servitor_session import start_servitor_session
        _start_session_fn = start_servitor_session

    _session_seq = [0]
    _open_sessions_sessionful: list = []

    async def _start_live_session(prompt: str, opts: dict | None = None) -> LiveAgentSession:
        opts = opts or {}
        merged = {**_defaults, **opts}
        agent_name = opts.get("agent") or default_agent
        if agent_name:
            merged["agent"] = agent_name
        label = opts.get("label") or (prompt[:64] if isinstance(prompt, str) else "session")
        sess_phase = opts.get("phase") or nonlocal_state["current_phase"]
        width = _current_layer_width()
        resolved_effort, _ = _resolve_effort(opts, width)
        req_model = pinned_model or opts.get("model") or default_model
        _session_seq[0] += 1
        sid = f"s{_session_seq[0]}"

        _bump_agent_count()
        _check_budget()
        await _acquire()

        driver = None
        try:
            driver_opts = {**merged, "default_model": default_model,
                           "pinned_model": pinned_model, "log": _log}
            driver = await _start_session_fn(driver_opts)
        except Exception as e:
            _release()
            raise

        session = LiveAgentSession(
            id=sid, driver=driver, label=label, phase=sess_phase,
            req_model=req_model, effort=resolved_effort,
            replay=None, journal=journal,
            on_event=lambda evt: _record_event(evt.get("type", ""), **{k: v for k, v in evt.items() if k != "type"}),
            on_log=_log, on_progress=on_progress,
        )
        _open_sessions_sessionful.append(session)

        try:
            await session._begin_turn(prompt, {
                "schema": opts.get("schema"), "effort": resolved_effort,
                "timeoutMs": opts.get("timeoutMs"),
            }, "start", _acquire, _release, _bump_agent_count, _check_budget)
        except Exception as e:
            _release()
            try:
                await driver.cleanup()
            except Exception:
                pass
            _open_sessions_sessionful.remove(session)
            raise
        return session

    async def _steer(session: LiveAgentSession, message: str, opts: dict | None = None) -> dict:
        opts = opts or {}
        if session._status == "closed":
            raise RuntimeError(f"Cannot steer closed session {session.label}.")
        if not session._settled and session._completion is not None:
            raise RuntimeError(
                f"Cannot steer session {session.label} while a turn is running. "
                f"Call wait(), waitAny(), or cancel() first."
            )
        _bump_agent_count()
        _check_budget()
        await _acquire()
        try:
            await session._begin_turn(message, {
                "schema": opts.get("schema"),
                "effort": opts.get("effort", session._effort),
                "timeoutMs": opts.get("timeoutMs"),
            }, "steer", _acquire, _release, _bump_agent_count, _check_budget)
        except Exception as e:
            _release()
            raise
        if opts.get("wait", True) is False:
            return session.poll()
        try:
            await session._completion
        except Exception:
            pass
        return session.poll()

    # Override LiveAgentSession.steer to use the runtime's steer
    async def _patched_steer(self_session, message, opts=None):
        return await _steer(self_session, message, opts or {})
    LiveAgentSession.steer = _patched_steer  # type: ignore

    def agent_start(prompt, opts=None):
        return _start_live_session(prompt, opts)

    async def agent_waitany(sessions, opts=None):
        opts = opts or {}
        return await wait_any_live(sessions, opts.get("timeoutMs"))

    def agent_status():
        """Return snapshots of all active sessions for workflow observability.

        Lets a workflow agent check what other agents are doing:
        - running / completed / cancelled / failed / timed_out
        - elapsed_ms and idle_ms for running sessions
        - stalled flag for sessions with no activity
        """
        return [s.poll() for s in _open_sessions_sessionful]

    def agent_stalled(stall_after_ms: float = 120_000):
        """Return sessions that appear stalled (running, no activity)."""
        return [s for s in _open_sessions_sessionful if s.is_stalled(stall_after_ms)]

    agent.start = agent_start  # type: ignore
    agent.waitAny = agent_waitany  # type: ignore
    agent.status = agent_status  # type: ignore
    agent.stalled = agent_stalled  # type: ignore

    # finalize: close sessionful workers + any other cleanup
    async def finalize():
        for s in list(_open_sessions_sessionful):
            try:
                await s.close()
            except Exception:
                pass
        _open_sessions_sessionful.clear()

    def agent_status():
        """Return snapshots of all active sessions for workflow observability.

        Lets a workflow agent check what other agents are doing:
        - running / completed / cancelled / failed / timed_out
        - elapsed_ms and idle_ms for running sessions
        - stalled flag for sessions with no activity
        """
        return [s.poll() for s in _open_sessions_sessionful]

    def agent_stalled(stall_after_ms: float = 120_000):
        """Return sessions that appear stalled (running, no activity)."""
        return [s for s in _open_sessions_sessionful if s.is_stalled(stall_after_ms)]

    agent.start = agent_start  # type: ignore
    agent.waitAny = agent_waitany  # type: ignore
    agent.status = agent_status  # type: ignore
    agent.stalled = agent_stalled  # type: ignore

    # (finalize defined above, before sessionful workers)

    return {
        "agent": agent, "parallel": parallel, "pipeline": pipeline,
        "phase": phase, "log": log, "budget": budget, "args": _args,
        "workflow": workflow, "human": human, "CAP": _CAP, "finalize": finalize,
        # True when --plan: agent() returns schema skeletons and does not call models.
        # Workflow Python still runs; side-effecting disk writes remain the workflow authors responsibility.
        "plan": plan,
    }


def _resolve_named_workflow(name: str) -> str:
    """Resolve a saved-workflow name to a script path."""
    import sys
    from pathlib import Path
    dirs = [Path.cwd() / ".claude" / "workflows", Path.home() / ".claude" / "workflows"]
    files = [f"{name}.py", f"{name}.workflow.py"]
    for d in dirs:
        for f in files:
            p = d / f
            if p.exists():
                return str(p)
    raise FileNotFoundError(
        f'workflow("{name}"): no saved workflow found. Searched {dirs[0]} and {dirs[1]} '
        f'for {name}.py / {name}.workflow.py'
    )
