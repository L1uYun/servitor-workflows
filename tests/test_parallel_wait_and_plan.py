"""Ratchet: workflow parallel([2]) completes under fake transport; plan exposes flag."""
from __future__ import annotations

from pathlib import Path

import pytest

from servitor_workflows.runtime import create_runtime
from servitor_workflows.run_workflow import run_workflow_source


@pytest.mark.asyncio
async def test_parallel_two_agents_smoke_with_fake_transport():
    calls = []

    async def fake_run(prompt, opts):
        calls.append({"prompt": prompt, "label": (opts or {}).get("label")})
        if "A" in prompt:
            return "A-ok"
        return "B-ok"

    rt = create_runtime(run_agent=fake_run, default_agent="pi", plan=False)
    agent, parallel = rt["agent"], rt["parallel"]

    a, b = await parallel([
        lambda: agent("Reply A", {"label": "a", "timeout_seconds": 5}),
        lambda: agent("Reply B", {"label": "b", "timeout_seconds": 5}),
    ])
    assert a == "A-ok"
    assert b == "B-ok"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_plan_flag_skips_model_and_is_visible():
    called = {"n": 0}

    async def boom(prompt, opts):
        called["n"] += 1
        raise AssertionError("model should not be called in plan mode")

    rt = create_runtime(run_agent=boom, plan=True, default_agent="pi")
    assert rt["plan"] is True
    out = await rt["agent"](
        "anything",
        {"schema": {"type": "object", "properties": {"x": {"type": "string"}}}},
    )
    assert called["n"] == 0
    assert isinstance(out, dict)


@pytest.mark.asyncio
async def test_workflow_source_plan_side_effect_gate(tmp_path):
    target = tmp_path / "side.txt"
    src = f'''
meta = {{"name": "plan-side-effect"}}
async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow, plan=False):
    path = args["path"]
    if not plan:
        open(path, "w", encoding="utf-8").write("wrote")
    return {{"plan": plan}}
'''
    r = await run_workflow_source(src, {"plan": True, "args": {"path": str(target)}})
    assert r["plan"] is True
    assert not target.exists()

    r2 = await run_workflow_source(src, {"plan": False, "args": {"path": str(target)}})
    assert r2["plan"] is False
    assert target.exists()


@pytest.mark.asyncio
async def test_plan_mode_fixture_skips_l3_on_incomplete_evidence():
    fixture = Path(__file__).resolve().parent / "fixtures" / "skill_review_workflow" / "skill_portfolio_review.workflow.py"
    phases = []
    logs = []

    result = await run_workflow_source(
        fixture.read_text(encoding="utf-8"),
        {
            "plan": True,
            "script_path": str(fixture),
            "on_phase": phases.append,
            "on_log": logs.append,
        },
    )

    assert result["plan"] is True
    assert result["l3_ok"] is False
    assert result["l1_missing"] == ["alpha"]
    assert result["l2_missing"] == ["phase_gate"]
    assert "L3-portfolio-synthesis" not in phases
    assert any("Skipping L3 due to incomplete evidence in plan mode" in line for line in logs)


def test_default_cap_honors_env_override(monkeypatch):
    monkeypatch.setenv("SERVITOR_WORKFLOW_CAP", "3")
    import importlib
    import servitor_workflows.runtime as runtime
    importlib.reload(runtime)
    assert runtime._CAP == 3

