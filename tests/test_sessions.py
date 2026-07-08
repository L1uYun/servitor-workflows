"""Sessionful worker tests: agent.start / wait / waitAny / steer / cancel / close.

1:1 Python port of upstream offline.js tests 26-29. Uses a fake session driver
(stands in for servitor_session.start_servitor_session), so these run with NO
servitor and NO tokens.
"""
import asyncio
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.run_workflow import run_workflow_source


def make_fake_session_factory():
    """Create a fake session driver factory.

    1:1 port of upstream makeFakeSessionFactory(). Each turn completes after a
    small delay parsed from the prompt (delay=NN ms), or is interrupted on demand.
    """
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


# 26) agent.start() returns BEFORE the turn completes; wait() resolves completed.
@pytest.mark.asyncio
async def test_agent_start_returns_before_completion():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess1"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("worker delay=40", {"label": "w"})
    early = s.poll()
    fin = await s.wait()
    return {"early": early["status"], "threadId": s.thread_id, "fin": fin["status"], "result": fin["result"]}
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["early"] == "running", "agent.start returns while the turn is still running"
    assert r["threadId"] and r["threadId"].startswith("fake-thread-")
    assert r["fin"] == "completed", "wait() resolves to a completed snapshot"
    assert r["result"] == "echo:worker delay=40"


# 27) agent.waitAny returns the first finisher and lists the still-running ones.
@pytest.mark.asyncio
async def test_wait_any_returns_first_finisher():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess2"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await agent.start("A delay=80", {"label": "a"})
    b = await agent.start("B delay=3", {"label": "b"})
    first = await agent.waitAny([a, b])
    final_a = await a.wait()
    return {
        "winner": first["snapshot"]["label"],
        "index": first["index"],
        "pending_count": len(first["pendingSessions"]),
        "pending": ",".join(s.label for s in first["pendingSessions"]),
        "timed_out": first["timedOut"],
        "final_a": final_a["status"],
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["winner"] == "b", "waitAny returns the first session to finish"
    assert r["index"] == 1
    assert r["pending_count"] == 1
    assert r["pending"] == "a"
    assert r["timed_out"] is False
    assert r["final_a"] == "completed"


# 28) agent.waitAny times out cleanly without cancelling the running turn.
@pytest.mark.asyncio
async def test_wait_any_timeout():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess2b"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await agent.start("A delay=10000", {"label": "a"})
    first = await agent.waitAny([a], {"timeoutMs": 20})
    await a.cancel()
    return {
        "timed_out": first["timedOut"],
        "session": first["session"],
        "pending_count": len(first["pendingSessions"]),
        "pending": ",".join(s.label for s in first["pendingSessions"]),
    }
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["timed_out"] is True, "waitAny reports a timeout"
    assert r["session"] is None, "no winner on timeout"
    assert r["pending_count"] == 1, "still-running session remains pending"
    assert r["pending"] == "a"


# 29) session.steer() starts a 2nd turn on the SAME thread.
@pytest.mark.asyncio
async def test_steer_same_thread():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess3"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("first", {"label": "w"})
    await s.wait()
    before = s.thread_id
    snap = await s.steer("second")
    return {"same": s.thread_id == before, "status": snap["status"], "result": snap["result"]}
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["same"] is True, "steer continues on the SAME thread id"
    assert r["status"] == "completed"
    assert r["result"] == "echo:second"
    assert len(fake["drivers"]) == 1, "exactly one thread/session was created"
    assert fake["drivers"][0].turns == ["first", "second"]


# 30) steer() while a turn is running throws a clear error.
@pytest.mark.asyncio
async def test_steer_while_running_throws():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess4"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("running delay=50", {"label": "w"})
    try:
        await s.steer("too soon")
        return {"error": None}
    except Exception as e:
        return {"error": str(e)}
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["error"] is not None, "steer while running should raise"
    assert "while a turn is running" in r["error"] or "Cannot steer" in r["error"]


# 31) cancel() interrupts the active turn.
@pytest.mark.asyncio
async def test_cancel_interrupts():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess5"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("long delay=10000", {"label": "w"})
    snap = await s.cancel()
    return {"status": snap["status"]}
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["status"] in ("cancelled", "interrupted"), f"cancel should interrupt, got {r['status']}"


# 32) close() cancels the active turn and cleans up the driver.
@pytest.mark.asyncio
async def test_close_cancels_and_cleans():
    fake = make_fake_session_factory()
    src = """meta = {"name": "sess6"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    s = await agent.start("long delay=10000", {"label": "w"})
    await s.close()
    return {"status": s.status, "cleaned": 1}
"""
    r = await run_workflow_source(src, {"start_session": fake["start_session"]})
    assert r["status"] == "closed"
    assert fake["drivers"][0].cleaned == 1, "close should clean up the driver"
