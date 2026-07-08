"""LiveAgentSession: the workflow-facing handle for sessionful workers.

1:1 Python port of the LiveAgentSession class from runner/src/runtime.js.
Owns per-turn concurrency-slot accounting, budget/cap gating, lifecycle events,
journaling, and the latest snapshot. Exposes ONLY safe methods to the script.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

from .journal import identity_hash


_TIMEOUT = object()  # sentinel for waitAny timeout


def _is_running_status(s: str) -> bool:
    return s in ("starting", "running")


class LiveAgentSession:
    """The workflow-facing handle around a session driver.

    1:1 port of upstream LiveAgentSession. The driver (servitor_session.py or
    a fake in tests) owns the protocol; this class owns the orchestration:
    concurrency slots, budget, events, journal, snapshots.
    """

    def __init__(self, *, id: str, driver, label: str, phase: str | None,
                 req_model: str | None, effort: str | None,
                 replay: list | None = None,
                 journal=None, on_event: Callable | None = None,
                 on_log: Callable | None = None,
                 on_progress: Callable | None = None):
        self.id = id
        self.label = label
        self.phase = phase
        self._driver = driver
        self._req_model = req_model
        self._replay = replay
        self._effort = effort
        self._status = "starting"
        self._turn_count = 0
        self._completion: asyncio.Future | None = None
        self._settled = True
        self._cancel_requested = False
        self._on_event = on_event
        self._on_log = on_log
        self._on_progress = on_progress
        self._journal = journal
        self._last_activity_at = time.time()
        self._started_at = time.time()
        self._on_progress_cb = on_progress
        self._snapshot = {
            "id": id, "label": label, "phase": phase,
            "threadId": getattr(driver, "thread_id", None),
            "turnId": None,
            "status": "starting", "result": None, "text": None, "error": None,
            "model": req_model, "effort": effort, "tokens": None, "ms": None,
        }

    @property
    def status(self) -> str:
        return self._status

    @property
    def thread_id(self) -> str | None:
        return getattr(self._driver, "thread_id", None)

    @property
    def current_turn_id(self):
        return self._snapshot.get("turnId")

    def poll(self) -> dict:
        snap = dict(self._snapshot)
        # Add live elapsed time for running sessions
        if self._status == "running":
            snap["elapsed_ms"] = int((time.time() - self._started_at) * 1000)
            snap["idle_ms"] = int((time.time() - self._last_activity_at) * 1000)
        return snap

    @property
    def last_activity_at(self) -> float:
        return self._last_activity_at

    def is_stalled(self, stall_after_ms: float = 120_000) -> bool:
        """True if this session is running with no activity for stall_after_ms."""
        if self._status != "running":
            return False
        idle = (time.time() - self._last_activity_at) * 1000
        return idle >= stall_after_ms

    def _is_running(self) -> bool:
        return _is_running_status(self._status)

    def _is_actionable(self) -> bool:
        return self._settled or self._status == "closed"

    def _settled_promise(self):
        if self._completion is None:
            fut = asyncio.get_event_loop().create_future()
            fut.set_result(dict(self._snapshot))
            return fut
        return self._completion

    async def _begin_turn(self, prompt: str, turn_opts: dict, kind: str,
                          acquire_fn, release_fn, bump_count_fn, check_budget_fn):
        """Begin one turn (initial or steer). 1:1 port of upstream _beginTurn."""
        from .runtime import _schema_skeleton

        self._cancel_requested = False
        turn_index = self._turn_count
        self._turn_count += 1
        sess_key = f"sess:{self.id}#{turn_index}"
        effort = turn_opts.get("effort", self._effort)

        # Warm-context resume: check if this turn already completed in a prior run
        cached = None
        if self._replay and turn_index < len(self._replay):
            cached = self._replay[turn_index]

        prompt_matches = (
            cached and cached.get("prompt_hash") is not None
            and cached["prompt_hash"] == identity_hash(prompt)
        )

        if cached and not prompt_matches:
            self._replay = None  # diverged — stop replaying
            if self._on_log:
                self._on_log(f"  ⊝ {kind} (prompt changed — re-running live): {self.label}")

        if cached and prompt_matches:
            self._status = "completed"
            self._settled = True
            self._snapshot = {
                "id": self.id, "label": self.label, "phase": self.phase,
                "threadId": getattr(self._driver, "thread_id", None),
                "turnId": None,
                "status": "completed",
                "result": cached.get("result"),
                "text": cached.get("result") if isinstance(cached.get("result"), str) else None,
                "error": None,
                "model": cached.get("model", self._req_model),
                "effort": cached.get("effort", effort),
                "tokens": cached.get("tokens"),
                "ms": cached.get("ms"),
            }
            if self._on_log:
                self._on_log(f"  ◦ {kind} (cached): {self.label}")
            if self._on_event:
                self._on_event({"type": "cached", "id": sess_key, "label": self.label,
                                "phase": self.phase, "kind": "session",
                                "sessionId": self.id, "turn": turn_index})
            release_fn()  # replayed turn does no model work
            return

        # Start the turn
        def _progress_wrapper(text=None, *args):
            self._last_activity_at = time.time()
            if self._on_progress:
                self._on_progress(self.label, text, sess_key)
        try:
            begun = await self._driver.begin_turn(prompt, {
                "schema": turn_opts.get("schema"),
                "effort": effort,
                "timeoutMs": turn_opts.get("timeoutMs"),
                "on_progress": _progress_wrapper,
            })
        except Exception as e:
            self._turn_count -= 1  # roll back
            raise

        self._status = "running"
        self._settled = False
        self._last_activity_at = time.time()
        self._snapshot = {
            "id": self.id, "label": self.label, "phase": self.phase,
            "threadId": getattr(self._driver, "thread_id", None),
            "turnId": begun.get("turnId"),
            "status": "running", "result": None, "text": None, "error": None,
            "model": self._req_model, "effort": effort,
            "tokens": None, "ms": None,
            "startedAt": self._last_activity_at,
        }
        if self._on_log:
            tag = "steer" if kind == "steer" else "agent.start"
            self._on_log(f"  ⟳ {tag}: {self.label}"
                         f"{'  [schema]' if turn_opts.get('schema') else ''}"
                         f"{f'  ⟪{effort}⟫' if effort else ''}")
        if self._on_event:
            self._on_event({
                "type": "start", "id": sess_key, "label": self.label,
                "phase": self.phase, "effort": effort, "model": self._req_model,
                "kind": "session", "sessionId": self.id, "turn": turn_index,
            })

        # Settle in the background
        async def _settle():
            try:
                outcome = await begun["completion"]
            except Exception as e:
                outcome = {"status": "failed", "error": str(e), "turnId": begun.get("turnId")}
            try:
                return await self._settle_turn(outcome, sess_key, turn_index, effort, prompt)
            finally:
                release_fn()

        self._completion = asyncio.get_event_loop().create_task(_settle())

    async def _settle_turn(self, outcome: dict, sess_key: str, turn_index: int,
                           effort: str | None, prompt: str) -> dict:
        """Fold a turn outcome into the snapshot. 1:1 port of upstream _settleTurn."""
        self._settled = True
        self._last_activity_at = time.time()
        raw_status = outcome.get("status", "failed")
        if raw_status == "completed":
            snap_status = "completed"
        elif raw_status == "interrupted":
            snap_status = "cancelled" if self._cancel_requested else "interrupted"
        else:
            snap_status = "failed"

        self._status = snap_status
        self._snapshot = {
            "id": self.id, "label": self.label, "phase": self.phase,
            "threadId": getattr(self._driver, "thread_id", None),
            "turnId": outcome.get("turnId", self._snapshot.get("turnId")),
            "status": snap_status,
            "result": outcome.get("result"),
            "text": outcome.get("text"),
            "error": outcome.get("error"),
            "model": outcome.get("model", self._req_model),
            "effort": effort,
            "tokens": outcome.get("tokens"),
            "ms": outcome.get("ms"),
        }

        if self._on_event:
            self._on_event({
                "type": "end", "id": sess_key, "label": self.label,
                "phase": self.phase, "effort": effort,
                "model": self._snapshot["model"],
                "tokens": self._snapshot["tokens"],
                "ms": self._snapshot["ms"],
                "status": snap_status,
                "kind": "session", "sessionId": self.id, "turn": turn_index,
            })

        if self._journal:
            try:
                self._journal.record(sess_key, self.label,
                                     self._snapshot.get("result") or self._snapshot.get("text"),
                                     {
                                         "phase": self.phase, "effort": effort,
                                         "model": self._snapshot["model"],
                                         "tokens": self._snapshot["tokens"],
                                         "ms": self._snapshot["ms"],
                                         "session": True, "sessionId": self.id,
                                         "turn": turn_index, "status": snap_status,
                                         "threadId": getattr(self._driver, "thread_id", None),
                                         "promptHash": identity_hash(prompt),
                                     })
            except Exception:
                pass

        return dict(self._snapshot)

    async def wait(self, timeout_ms: int | None = None) -> dict:
        """Wait for the current turn to settle. 1:1 port of upstream wait()."""
        if self._settled or self._completion is None:
            return dict(self._snapshot)
        if timeout_ms is None:
            try:
                await asyncio.shield(self._completion)
            except (asyncio.CancelledError, Exception):
                pass
            return dict(self._snapshot)
        try:
            await asyncio.wait_for(asyncio.shield(self._completion), timeout=timeout_ms / 1000)
        except asyncio.TimeoutError:
            return {**self._snapshot, "status": "timed_out"}
        except (asyncio.CancelledError, Exception):
            pass
        return dict(self._snapshot)

    async def steer(self, message: str, opts: dict | None = None) -> dict:
        """Start a follow-up turn on the SAME session. 1:1 port of upstream steer()."""
        opts = opts or {}
        if self._status == "closed":
            raise RuntimeError(f"Cannot steer closed session {self.label}.")
        if not self._settled and self._completion is not None:
            raise RuntimeError(
                f"Cannot steer session {self.label} while a turn is running. "
                f"Call wait(), waitAny(), or cancel() first."
            )
        # These are injected by the runtime wrapper
        raise NotImplementedError("steer must be called through the runtime wrapper")

    async def cancel(self) -> dict:
        """Cancel the active turn. 1:1 port of upstream cancel()."""
        if self._status == "closed":
            return dict(self._snapshot)
        if self._settled or self._completion is None:
            return dict(self._snapshot)
        self._cancel_requested = True
        await self._driver.interrupt_current()
        try:
            await asyncio.shield(self._completion)
        except (asyncio.CancelledError, Exception):
            pass
        return dict(self._snapshot)

    async def close(self):
        """Close the session: cancel active turn + cleanup driver."""
        if self._status == "closed":
            return
        if not self._settled and self._completion is not None:
            try:
                await self.cancel()
            except Exception:
                pass
        try:
            await self._driver.cleanup()
        except Exception:
            pass
        self._status = "closed"
        self._snapshot = {**self._snapshot, "status": "closed"}


async def wait_any_live(sessions: list, timeout_ms: int | None = None) -> dict:
    """Wait for the first session to become actionable.

    1:1 port of upstream waitAnyLive(). Returns:
    {session, index, snapshot, pendingSessions, timedOut}
    """
    lst = [s for s in (sessions if isinstance(sessions, list) else []) if s is not None]
    if not lst:
        return {"session": None, "index": None, "snapshot": None,
                "pendingSessions": [], "timedOut": False}

    def _pending(winner=None):
        return [s for s in lst if s is not winner and s._is_running()]

    # Already actionable? Return the lowest-index one immediately
    for i, s in enumerate(lst):
        if s._is_actionable():
            return {"session": s, "index": i, "snapshot": s.poll(),
                    "pendingSessions": _pending(s), "timedOut": False}

    # Race the running sessions' settle promises
    async def _wait_one(idx: int):
        try:
            await asyncio.shield(lst[idx]._settled_promise())
        except asyncio.CancelledError:
            pass
        return idx

    racers = [asyncio.ensure_future(_wait_one(i)) for i in range(len(lst))]
    if timeout_ms is not None:
        racers.append(asyncio.ensure_future(asyncio.sleep(timeout_ms / 1000, result=_TIMEOUT)))

    done, pending = await asyncio.wait(racers, return_when=asyncio.FIRST_COMPLETED)
    # Cancel remaining waiters
    for p in pending:
        p.cancel()

    # Find the result
    winner_idx = None
    for d in done:
        try:
            result = d.result()
            if result is _TIMEOUT:
                return {"session": None, "index": None, "snapshot": None,
                        "pendingSessions": [s for s in lst if s._is_running()],
                        "timedOut": True}
            winner_idx = result
        except Exception:
            pass

    if winner_idx is None:
        return {"session": None, "index": None, "snapshot": None,
                "pendingSessions": [s for s in lst if s._is_running()],
                "timedOut": False}

    winner = lst[winner_idx]
    # Check for stalled sessions among pending
    stalled = [s for s in lst if s.is_stalled()]
    result = {"session": winner, "index": winner_idx, "snapshot": winner.poll(),
              "pendingSessions": _pending(winner), "timedOut": False}
    if stalled:
        result["stalled"] = stalled
        result["stalledLabels"] = [s.label for s in stalled]
    return result
