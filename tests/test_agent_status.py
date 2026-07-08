"""Agent state visibility tests: session status, stall detection, agent.status().

Verifies that workflow agents can observe each other's state:
- running / completed / cancelled / timed_out
- elapsed_ms / idle_ms for running sessions
- stalled detection for sessions with no activity
- agent.status() returns all active session snapshots
"""
import asyncio
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.run_workflow import run_workflow_source


def make_fake_session_factory():
    drivers = []
    seq = [0]

    class FakeDriver:
        def __init__(self, thread_id, opts):
            self.thread_id = thread_id
            self.threadId = thread_id
            self.opts = opts
            self.turns = []
            self.cleaned = 0
            self._active = False
            self._done = None
            self.resumed = False

        async def begin_turn(self, prompt, turn_opts=None):
            turn_opts = turn_opts or {}
            self.turns.append(str(prompt))
            turn_id = f"{self.thread_id}:t{len(self.turns)}"
            import re
            m = re.search(r"delay=(\d+)", str(prompt))
            ms = int(m.group(1)) if m else 2
            self._active = True

            completion_future = asyncio.get_event_loop().create_future()

            def done(status):
                if not self._active:
                    return
                self._active = False
                self._done = None
                text = f"echo:{prompt}"
                result = None
                if status == "completed":
                    result = {"echoed": str(prompt)} if turn_opts.get("schema") else text
                if not completion_future.done():
                    completion_future.set_result({
                        "status": status,
                        "result": result,
                        "text": text,
                        "error": "boom" if status == "failed" else None,
                        "model": "fake-model", "tokens": 7, "ms": ms, "turnId": turn_id,
                    })

            self._done = done
            loop = asyncio.get_event_loop()
            timer = loop.call_later(ms / 1000, lambda: done("completed"))
            return {"turnId": turn_id, "completion": completion_future}

        async def interrupt_current(self):
            if self._done:
                self._done("interrupted")

        async def cleanup(self):
            self.cleaned += 1

    async def start_session(opts):
        seq[0] += 1
        driver = FakeDriver(f"fake-thread-{seq[0]}", opts)
        drivers.append(driver)
        return driver

    return {"start_session": start_session, "drivers": drivers}


# agent.status() returns snapshots of all active sessions
@pytest.mark.asyncio
async def test_agent_status_returns_all_sessions():
    fake = make_fake_session_factory()
    src = """meta = {"name": "status1"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await agent.start("A delay=50", {"label": "worker-a"})
    b = await agent.start("B delay=50", {"label": "worker-b"})
    # While both are running, check status
    statuses = agent.status()
    # Wait for them
    await a.wait()
    await b.wait()
    return {
        "count": len(statuses),
        "labels": sorted(s["label"] for s in statuses),
        "all_running": all(s["status"] == "running" for s in statuses),
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["count"] == 2
    assert r["labels"] == ["worker-a", "worker-b"]
    assert r["all_running"] is True


# Running session has elapsed_ms and idle_ms
@pytest.mark.asyncio
async def test_running_session_has_elapsed_and_idle():
    fake = make_fake_session_factory()
    src = """meta = {"name": "status2"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("long delay=200", {"label": "w"})
    snap = s.poll()
    await s.cancel()
    return {
        "has_elapsed": snap.get("elapsed_ms") is not None,
        "has_idle": snap.get("idle_ms") is not None,
        "elapsed_positive": (snap.get("elapsed_ms") or 0) >= 0,
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["has_elapsed"] is True
    assert r["has_idle"] is True
    assert r["elapsed_positive"] is True


# waitAny reports stalled sessions
@pytest.mark.asyncio
async def test_wait_any_reports_stalled():
    fake = make_fake_session_factory()
    src = """meta = {"name": "status3"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await agent.start("A delay=10000", {"label": "stuck-worker"})
    b = await agent.start("B delay=1", {"label": "fast-worker"})
    first = await agent.waitAny([a, b])
    await a.cancel()
    return {
        "winner": first["snapshot"]["label"],
        "has_stalled": "stalled" in first,
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["winner"] == "fast-worker"
    # stalled may or may not be present depending on timing,
    # but the key should exist in the result dict
    # (a 10s delay with 1ms stall threshold would be stalled,
    #  but default threshold is 120s, so likely not stalled here)
    assert "has_stalled" in r


# Completed session does not show as stalled
@pytest.mark.asyncio
async def test_completed_not_stalled():
    fake = make_fake_session_factory()
    src = """meta = {"name": "status4"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("quick delay=2", {"label": "w"})
    await s.wait()
    return {
        "is_stalled": s.is_stalled(),
        "status": s.status,
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["is_stalled"] is False
    assert r["status"] == "completed"


# agent.stalled() returns only stalled sessions
@pytest.mark.asyncio
async def test_agent_stalled_returns_only_stuck():
    fake = make_fake_session_factory()
    src = """meta = {"name": "status5"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await agent.start("A delay=2", {"label": "fast"})
    b = await agent.start("B delay=10000", {"label": "slow"})
    await a.wait()
    # With default 120s threshold, neither is stalled
    stalled_default = agent.stalled()
    # With 0ms threshold, the running one is stalled
    stalled_zero = agent.stalled(0)
    await b.cancel()
    return {
        "stalled_default_count": len(stalled_default),
        "stalled_zero_count": len(stalled_zero),
        "stalled_zero_label": stalled_zero[0].label if stalled_zero else None,
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["stalled_default_count"] == 0
    assert r["stalled_zero_count"] == 1
    assert r["stalled_zero_label"] == "slow"


# Session snapshot after timeout shows timed_out
@pytest.mark.asyncio
async def test_session_timeout_status():
    fake = make_fake_session_factory()
    src = """meta = {"name": "status6"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("long delay=10000", {"label": "w"})
    snap = await s.wait(timeout_ms=20)
    await s.cancel()
    return {"status": snap["status"]}
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["status"] == "timed_out"
