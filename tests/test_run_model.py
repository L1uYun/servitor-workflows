"""Tests for run_model.py and run_summary.py.

1:1 Python port of upstream runModel/summarize-run tests.
"""
import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.run_workflow import run_workflow_source
from servitor_workflows.journal import Journal
from servitor_workflows.run_model import build_run_model, build_live_run_model, live_state, read_events
from servitor_workflows.run_summary import summarize_run, render_end_of_run


@pytest.mark.asyncio
async def test_run_model_basic():
    """build_run_model reads journal entries and structures them."""
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "r.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        async def echo(_p, o):
            return "ok"

        src = """meta = {"name": "rm", "description": "test run model"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Scan")
    await agent("a")
    await agent("b")
    return 1
"""
        await run_workflow_source(src, {"run_agent": echo, "journal": j, "auto_effort": True})

        run = build_run_model(journal_path=str(jpath))
        assert run["name"] == "r"  # journal filename without script
        assert run["description"] == ""  # no script -> no description
        assert len(run["agents"]) == 2
        assert run["agents"][0]["phase"] == "Scan"
        assert run["agents"][1]["phase"] == "Scan"
        assert run["counts"]["agents"] == 2


@pytest.mark.asyncio
async def test_run_model_with_sessions():
    """build_run_model groups session turns into worker rollups."""
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "s.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        # Manually record a session entry
        j.record("sess:s1#0", "worker", "result1", {
            "phase": "Explore", "session": True, "sessionId": "s1",
            "turn": 0, "status": "completed", "threadId": "t1",
        })

        run = build_run_model(journal_path=str(jpath))
        assert len(run["sessions"]) == 1
        assert run["sessions"][0]["id"] == "s1"
        assert run["sessions"][0]["label"] == "worker"
        assert len(run["sessions"][0]["turns"]) == 1


@pytest.mark.asyncio
async def test_summarize_run():
    """summarize_run produces a structured summary."""
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "r.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        async def echo(_p, o):
            o.get("on_metrics", lambda *_: None)({"ms": 42, "model": "gpt-5.5",
                "tokens": {"input": 10, "output": 5, "reasoning": 3, "total": 18}})
            return "ok"

        src = """meta = {"name": "sum"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Scan")
    await agent("a")
    await agent("b", {"phase": "Verify"})
    return 1
"""
        await run_workflow_source(src, {"run_agent": echo, "journal": j, "auto_effort": True})

        s = summarize_run(journal_path=str(jpath))
        assert s["name"] == "r"  # journal filename without script
        assert s["counts"]["journaledAgents"] == 2
        assert s["counts"]["completedAgents"] == 2
        assert s["metrics"]["totalTokens"] == 36  # 18 * 2
        assert s["metrics"]["hasMetrics"] is True
        assert len(s["byPhase"]) == 2
        assert s["byPhase"][0]["phase"] == "Scan"


@pytest.mark.asyncio
async def test_summarize_with_human_checkpoint():
    """human() checkpoints surface as run.checkpoints, not agents."""
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "h.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        src = """meta = {"name": "hc"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    a = await human("Scope?", {"id": "scope", "default": "exclude"})
    return a
"""
        await run_workflow_source(src, {"journal": j})

        run = build_run_model(journal_path=str(jpath))
        assert len(run["agents"]) == 0, "human checkpoint is NOT an agent"
        assert len(run["checkpoints"]) == 1
        assert run["checkpoints"][0]["qid"] == "scope"
        assert run["checkpoints"][0]["answer"] == "exclude"


@pytest.mark.asyncio
async def test_render_end_of_run():
    """render_end_of_run produces a compact summary."""
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "r.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        async def echo(_p, o):
            o.get("on_metrics", lambda *_: None)({"ms": 42, "model": "gpt-5.5",
                "tokens": {"total": 100}})
            return "ok"

        src = """meta = {"name": "eor"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    await agent("a")
    return 1
"""
        await run_workflow_source(src, {"run_agent": echo, "journal": j})

        s = summarize_run(journal_path=str(jpath))
        block = render_end_of_run(s)
        assert "1 agent" in block
        assert "100" in block  # tokens
