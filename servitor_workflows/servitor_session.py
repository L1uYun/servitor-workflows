"""Long-lived servitor worker sessions: the driver behind agent.start() / session.steer().

1:1 Python port of runner/src/codexSession.js. The JS version drives the Codex
app-server's thread/start + turn/start protocol; this version drives servitor's
run_agent + wait_for_completion + read_result with session_id/is_resume.

A servitor session is a sequence of run_agent calls sharing the same session_id.
Each turn is one run_agent(prompt, session_id=X, is_resume=False/True) + wait + read.
The first turn uses is_resume=False; subsequent turns (steer) use is_resume=True.

Reuses the one-shot turn primitives from servitor_agent.py (model resolution,
schema strictification, result parsing), so one-shot and sessionful turns behave
identically.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

from .servitor_agent import strictify_schema, parse_schema_result, _refresh_available_models
from .model_map import resolve_model
from .meter import tokens_for_thread, mark_resumed_thread


_DEFAULT_TURN_TIMEOUT_MS = 600_000  # 10 minutes, same as one-shot


async def start_servitor_session(opts: dict) -> dict:
    """Open a servitor session for a long-lived worker.

    1:1 port of upstream startCodexSession(). Returns a SessionDriver dict with
    begin_turn(), interrupt_current(), cleanup(), thread_id, resumed.

    servitor doesn't have a persistent thread concept — session_id is a UUID
    passed to run_agent. The "thread" is the sequence of run_agent calls sharing
    that session_id. is_resume=True on follow-up turns tells the provider to
    continue the conversation.
    """
    import uuid

    from .agent_types import load_agent_type

    log = opts.get("log") or (lambda *_: None)

    # agentType -> developer instructions (+ optional fallback model)
    system_prompt = opts.get("system_prompt")
    agent_type_model = None
    if opts.get("agent_type"):
        defn = await asyncio.to_thread(load_agent_type, opts["agent_type"], opts.get("cwd"))
        if defn:
            if not system_prompt:
                system_prompt = defn.get("system_prompt")
            agent_type_model = defn.get("model")
        else:
            log(f"agentType '{opts['agent_type']}' not found — using default instructions")

    pinned = opts.get("pinned_model")
    requested_model = pinned or opts.get("model") or agent_type_model or opts.get("default_model")

    # Worktree isolation (created once, kept across turns)
    cwd = opts.get("cwd")
    worktree = None
    if opts.get("isolation") == "worktree" and cwd:
        from .worktree import create_worktree, is_git_repo
        if await is_git_repo(cwd):
            worktree = await create_worktree(cwd)
            cwd = worktree["dir"]
        else:
            log(f"isolation:'worktree' ignored — {cwd} is not a git repo")

    # Session ID: use a provided one (resume) or generate a new UUID
    session_id = opts.get("resume_session_id") or str(uuid.uuid4())
    resumed = opts.get("resume_session_id") is not None

    if resumed:
        mark_resumed_thread(session_id)
        log(f"  ↻ session re-attached to session_id {session_id} (warm context)")
    else:
        log(f"  + session created: {session_id}")

    return _ServitorSessionDriver(
        session_id=session_id,
        model=requested_model,
        system_prompt=system_prompt,
        cwd=cwd,
        worktree=worktree,
        log=log,
        resumed=resumed,
        opts=opts,
    )


class _ServitorSessionDriver:
    """Protocol-only session handle. One active turn at a time.

    1:1 port of upstream CodexSessionDriver. Each turn is one servitor run_agent
    call with the shared session_id.
    """

    def __init__(self, *, session_id, model, system_prompt, cwd, worktree, log, resumed, opts):
        self.session_id = session_id
        self.thread_id = session_id  # alias for upstream compat
        self.model = model or None
        self.current_turn_id = None
        self.resumed = resumed
        self._worktree = worktree
        self._log = log
        self._opts = opts
        self._system_prompt = system_prompt
        self._cwd = cwd
        self._active = False

    async def begin_turn(self, prompt: str, turn_opts: dict | None = None) -> dict:
        """Start a turn on the session.

        1:1 port of upstream beginTurn(). Returns {turn_id, completion} once the
        turn has STARTED. completion settles when the turn ends.
        """
        if self._active:
            raise RuntimeError("internal: begin_turn called while a turn is active")

        import servitor
        turn_opts = turn_opts or {}
        schema = turn_opts.get("schema")
        if schema:
            schema = strictify_schema(schema)

        if not _refresh_available_models.__doc__:  # hack to check if we need to refresh
            _refresh_available_models()

        model = resolve_model(self.model, _get_available(), self._log)

        is_resume = self._opts.get("_is_resume", False)
        if self.current_turn_id is not None:
            is_resume = True  # follow-up turns are always resumes

        run_kwargs: dict[str, Any] = {
            "provider_name": self._opts.get("agent"),
            "prompt": prompt,
            "session_id": self.session_id,
            "is_resume": is_resume,
        }
        if self._cwd:
            run_kwargs["cwd"] = self._cwd
        if model:
            run_kwargs["model"] = model
        if self._system_prompt:
            run_kwargs["system_prompt"] = self._system_prompt
        if turn_opts.get("effort"):
            run_kwargs["native_args"] = self._opts.get("native_args", [])

        started_at = time.monotonic()

        run_info = await asyncio.to_thread(servitor.run_agent, **run_kwargs)
        run_dir = run_info.get("run_dir") if isinstance(run_info, dict) else getattr(run_info, "run_dir", None)
        turn_id = run_dir  # use run_dir as turn_id
        self._active = True
        self.current_turn_id = turn_id

        async def _complete():
            try:
                wait_seconds = turn_opts.get("timeoutMs", _DEFAULT_TURN_TIMEOUT_MS) / 1000
                await asyncio.to_thread(servitor.wait_for_completion, run_dir, int(wait_seconds))
                meta = await asyncio.to_thread(servitor.read_result, run_dir)

                if schema or turn_opts.get("check_contains"):
                    meta = await asyncio.to_thread(
                        servitor.apply_output_contract,
                        meta,
                        expect_json=schema is not None,
                        schema=schema,
                    )

                ok = meta.get("ok", False) if isinstance(meta, dict) else False
                result_text = meta.get("result") if isinstance(meta, dict) else None
                failure = meta.get("failure_reason") if isinstance(meta, dict) else None

                status = "completed" if ok else "failed"
                result = parse_schema_result(result_text, schema) if ok else None

                tokens = tokens_for_thread(self.session_id)
                ms = int((time.monotonic() - started_at) * 1000)

                return {
                    "status": status,
                    "result": result,
                    "text": result_text,
                    "error": None if ok else (failure or "turn failed"),
                    "model": model,
                    "tokens": tokens,
                    "ms": ms,
                    "turnId": turn_id,
                }
            except Exception as e:
                return {
                    "status": "failed",
                    "result": None,
                    "text": None,
                    "error": str(e),
                    "model": model,
                    "tokens": None,
                    "ms": int((time.monotonic() - started_at) * 1000),
                    "turnId": turn_id,
                }
            finally:
                self._active = False

        return {"turnId": turn_id, "completion": _complete()}

    async def interrupt_current(self):
        """Interrupt the active turn. 1:1 port of upstream interruptCurrent().

        Phase 1: logical cancel only. servitor doesn't expose process-level
        cancellation yet, so we just mark the turn as interrupted.
        """
        if self._active and self.current_turn_id:
            self._log(f"  ⊘ cancel requested for turn {self.current_turn_id} (logical cancel)")

    async def cleanup(self):
        """Remove the worktree (if any). Kept across all turns, removed only here."""
        if self._worktree:
            try:
                r = await self._worktree["cleanup"]()
                if not r["removed"]:
                    self._log(f"worktree kept (modified): {r['dir']}")
            except Exception as e:
                self._log(f"worktree cleanup failed: {e}")
            self._worktree = None


def _get_available():
    from .servitor_agent import get_available_models
    return get_available_models()
