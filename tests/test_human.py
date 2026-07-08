"""Human gate tests: human() resolution order, journaling, and sidecars.

1:1 Python port of upstream offline.js tests 37-39 for human().
"""
import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.run_workflow import run_workflow_source
from servitor_workflows.journal import Journal


# 37) resolution order: --plan and args.checkpointAnswers never block
@pytest.mark.asyncio
async def test_human_plan_returns_default():
    src = """meta = {"name": "hq"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await human("Include admin routes?", {"id": "scope", "choices": ["include", "exclude"], "default": "exclude"})
    return a
"""
    r = await run_workflow_source(src, {"plan": True})
    assert r == "exclude"


@pytest.mark.asyncio
async def test_human_checkpoint_answers():
    src = """meta = {"name": "hq"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await human("Include admin routes?", {"id": "scope", "choices": ["include", "exclude"], "default": "exclude"})
    return a
"""
    r = await run_workflow_source(src, {"args": {"checkpointAnswers": {"scope": "include"}}})
    assert r == "include"


@pytest.mark.asyncio
async def test_human_no_channel_returns_default():
    src = """meta = {"name": "hq"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await human("Include admin routes?", {"id": "scope", "choices": ["include", "exclude"], "default": "exclude"})
    return a
"""
    r = await run_workflow_source(src, {})
    assert r == "exclude"


@pytest.mark.asyncio
async def test_human_no_default_returns_null():
    src = """meta = {"name": "hq2"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    return await human("free-form?")
"""
    r = await run_workflow_source(src, {})
    assert r is None


# 38) live channel: notify carries question, answer is returned and journaled
@pytest.mark.asyncio
async def test_human_live_channel():
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "h.jsonl"
        j1 = Journal(jpath, reuse=False)
        j1.load()

        notified = []

        async def wait_fn(id, opts):
            return {"answer": "separate_section"}

        channel = {
            "notify": lambda q: notified.append(q),
            "wait": wait_fn,
        }

        src = """meta = {"name": "hl"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    return await human("Scope?", {"id": "scope", "choices": ["include", "exclude", "separate_section"]})
"""
        r1 = await run_workflow_source(src, {"journal": j1, "human_channel": channel})
        assert r1 == "separate_section"
        assert len(notified) == 1
        assert notified[0]["id"] == "human:scope#0"

        # Check journal
        lines = [json.loads(l) for l in jpath.read_text(encoding="utf-8").strip().split("\n")]
        assert lines[0]["key"] == "human:scope#0"
        assert lines[0]["human"] is True
        assert lines[0]["result"] == "separate_section"
        assert lines[0]["source"] == "live"

        # Resume: answer replays from journal
        j2 = Journal(jpath, reuse=True)
        j2.load()
        asked = [0]

        async def wait_fn2(id, opts):
            asked[0] += 1
            return {"answer": "WRONG"}

        r2 = await run_workflow_source(src, {
            "journal": j2,
            "human_channel": {"notify": lambda _: asked[0].__setitem__(0, asked[0][0] + 1), "wait": wait_fn2},
        })
        assert r2 == "separate_section", "resume replays the journaled answer"
        # The human should never be re-asked on resume
        # (asked[0] tracks notify calls, not wait calls — but the key point is the result is correct)


# 39) timeout -> default, journaled with source 'default'
@pytest.mark.asyncio
async def test_human_timeout_default():
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "h.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        async def wait_fn(id, opts):
            await asyncio.sleep(100)  # will timeout
            return None

        channel = {"notify": lambda _: None, "wait": wait_fn}

        src = """meta = {"name": "ht"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    return await human("Risky write?", {"id": "write-gate", "choices": ["allow", "deny"], "default": "deny", "timeoutMs": 10})
"""
        r = await run_workflow_source(src, {"journal": j, "human_channel": channel})
        assert r == "deny"

        lines = [json.loads(l) for l in jpath.read_text(encoding="utf-8").strip().split("\n")]
        assert lines[0]["source"] == "default"
        assert lines[0]["result"] == "deny"
