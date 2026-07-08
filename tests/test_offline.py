"""Offline unit checks for the provider-neutral pieces — no servitor, no tokens.

1:1 Python port of runner/test/offline.js core tests. Covers:
- extractMeta ignores comment shadows
- parallel(): throwing thunk becomes None
- pipeline(): stage that throws drops that item
- budget global is present without configured total
- journal: stable identity hash, occurrence counting, reuse hit/get
- model resolution
- auto-effort layer width policy
- effort precedence
- effortForLayerWidth boundaries
- schemaSkeleton
- --plan: agent() short-circuits to skeletons
- lifecycle events
"""
import asyncio
import argparse
import json
import tempfile
from pathlib import Path

import pytest

import servitor_workflows.run_workflow as run_workflow
from servitor_workflows.cli import _cmd_run
from servitor_workflows.run_workflow import run_workflow_source, extract_meta
from servitor_workflows.runtime import effort_for_layer_width, _schema_skeleton, create_runtime, active_slots
from servitor_workflows.journal import identity_hash, Journal
from servitor_workflows.model_map import resolve_model, pick_frontier
from servitor_workflows.servitor_agent import is_retryable, strictify_schema
from servitor_workflows.meter import reset_meter, record_token_usage, tokens_spent, output_spent, tokens_for_thread, mark_resumed_thread


# 1) extractMeta must ignore a comment that mentions `meta`.
def test_extract_meta_ignores_comment():
    src = '# note: workflow uses `meta = {}` at the top\nmeta = {"name": "x", "description": "d"}\n'
    meta = extract_meta(src)
    assert meta is not None
    assert meta["name"] == "x"


# 2) Workflow body runs with return value.
@pytest.mark.asyncio
async def test_workflow_returns_value():
    src = 'meta = {"name": "y"}\n\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return 40 + 2\n'
    result = await run_workflow_source(src, {})
    assert result == 42


# 3) parallel(): a throwing thunk becomes None; others survive.
@pytest.mark.asyncio
async def test_parallel_nulls_throwers():
    src = '''meta = {"name": "p"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    def boom():
        raise Exception("boom")
    r = await parallel([
        lambda: 1,
        boom,
        lambda: asyncio.coroutine(lambda: 3)(),  # will be None since it's not async
    ])
    # Replace with simpler version
    return r
'''
    # Simpler test:
    src = '''meta = {"name": "p"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    r = await parallel([
        lambda: 1,
        lambda: (_ for _ in ()).throw(Exception("boom")),
        lambda: 3,
    ])
    return r
'''
    r = await run_workflow_source(src, {})
    assert r == [1, None, 3]


# 4) pipeline(): a stage that throws drops that item to None.
@pytest.mark.asyncio
async def test_pipeline_drops_failing_item():
    src = '''meta = {"name": "pl"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    def stage1(x):
        return x * 10
    def stage2(x):
        if x == 20:
            raise Exception("drop")
        return x + 1
    r = await pipeline([1, 2, 3], stage1, stage2)
    return r
'''
    r = await run_workflow_source(src, {})
    assert r == [11, None, 31]


# 5) budget global is present and sane without a configured total.
@pytest.mark.asyncio
async def test_budget_present_without_total():
    src = '''meta = {"name": "b"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    return {"total": budget["total"], "remaining": budget["remaining"](), "spent": budget["spent"]()}
'''
    r = await run_workflow_source(src, {})
    assert r["total"] is None
    assert r["remaining"] == float("inf")
    assert isinstance(r["spent"], int)


# 6) journal: stable identity hash, occurrence counting, reuse hit/get.
def test_identity_hash_order_independent():
    h1 = identity_hash("hello", {"model": "m", "effort": "low"})
    h2 = identity_hash("hello", {"effort": "low", "model": "m"})
    h3 = identity_hash("hello", {"model": "m", "effort": "high"})
    assert h1 == h2
    assert h1 != h3


def test_journal_occurrence_and_reuse():
    j = Journal(None, reuse=True)
    k0 = j.next_key("hello", {"model": "m"})
    k1 = j.next_key("hello", {"model": "m"})
    assert k0 != k1
    assert j.hit(k0) is False
    j.record(k0, "a", {"ok": 1})
    assert j.hit(k0) is True
    assert j.get(k0) == {"ok": 1}

    j_no = Journal(None, reuse=False)
    j_no.record("x#0", "a", 1)
    assert j_no.hit("x#0") is False


# 8) model resolution
def test_resolve_model_claude_to_servitor():
    have = ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex"]
    assert resolve_model("claude-opus-4-8", have) == "gpt-5.5"
    assert resolve_model("opus", have) == "gpt-5.5"
    assert resolve_model("haiku", have) == "gpt-5.4-mini"
    assert resolve_model("gpt-5.4", have) == "gpt-5.4"
    assert resolve_model("inherit", have) is None
    assert resolve_model(None, have) is None
    assert resolve_model("made-up-model", have) is None
    assert resolve_model("claude-opus", []) == "gpt-5.5"


def test_pick_frontier():
    models = [
        {"id": "gpt-5.4", "isDefault": False},
        {"id": "gpt-5.5", "isDefault": True},
        {"id": "gpt-5.4-mini"},
        {"id": "gpt-5.3-codex"},
        {"id": "gpt-5.3-codex-spark"},
        {"id": "gpt-5.2"},
    ]
    assert pick_frontier(models) == "gpt-5.5"
    assert pick_frontier(["gpt-5.2", "gpt-5.4", "gpt-5.4-mini"]) == "gpt-5.4"
    assert pick_frontier([{"id": "gpt-6", "hidden": True}, {"id": "gpt-5.5"}]) == "gpt-5.5"
    assert pick_frontier([]) is None


# 10) retry classification
def test_is_retryable():
    e1 = Exception("upstream blip")
    setattr(e1, "codex_error_info", "ResponseStreamDisconnected")
    assert is_retryable(e1) is True

    e2 = Exception("boom")
    setattr(e2, "codex_error_info", {"HttpConnectionFailed": {"httpStatusCode": 503}})
    assert is_retryable(e2) is True

    assert is_retryable(Exception("Transport is not connected")) is True
    assert is_retryable(Exception("turn failed: invalid request")) is False

    e3 = Exception("x")
    setattr(e3, "codex_error_info", "ContextWindowExceeded")
    assert is_retryable(e3) is False
    assert is_retryable(Exception("some unknown failure")) is False


# 12) auto-effort: layer width drives thinking effort
@pytest.mark.asyncio
async def test_auto_effort_layer_width():
    async def echo(_prompt, o):
        return o.get("effort") or "(none)"

    src = '''meta = {"name": "ae"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    wide = await parallel([lambda: agent("w" + str(i)) for i in range(8)])
    small = await parallel([lambda: agent("a"), lambda: agent("b"), lambda: agent("c")])
    solo = await agent("solo")
    piped = await pipeline(list(range(1, 10)), lambda x: agent("p" + str(x)))
    return {"wide": wide, "small": small, "solo": solo, "piped": piped}
'''
    r = await run_workflow_source(src, {"auto_effort": True, "run_agent": echo})
    assert r["wide"] == ["high"] * 8
    assert r["small"] == ["high"] * 3
    assert r["solo"] == "xhigh"
    assert r["piped"] == ["high"] * 9


# 13) effort precedence: pin > per-call > auto > flag > omitted
@pytest.mark.asyncio
async def test_effort_precedence():
    async def echo(_prompt, o):
        return o.get("effort") or "(none)"

    r1 = await run_workflow_source(
        'meta = {"name": "p1"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x", {"effort": "low"})\n',
        {"auto_effort": True, "run_agent": echo},
    )
    assert r1 == "low"

    r2 = await run_workflow_source(
        'meta = {"name": "p2"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x", {"effort": "low"})\n',
        {"auto_effort": True, "pinned_effort": "xhigh", "run_agent": echo},
    )
    assert r2 == "xhigh"

    r3 = await run_workflow_source(
        'meta = {"name": "p3"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x")\n',
        {"defaults": {"effort": "medium"}, "run_agent": echo},
    )
    assert r3 == "medium"

    r4 = await run_workflow_source(
        'meta = {"name": "p4"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x")\n',
        {"run_agent": echo},
    )
    assert r4 == "(none)"


# 14) effortForLayerWidth boundaries
def test_effort_for_layer_width():
    assert effort_for_layer_width(1) == "xhigh"
    assert effort_for_layer_width(2) == "high"
    assert effort_for_layer_width(7) == "high"
    assert effort_for_layer_width(8) == "high"
    assert effort_for_layer_width(50) == "high"
    assert effort_for_layer_width(0) == "xhigh"


# 16) schemaSkeleton
def test_schema_skeleton():
    assert _schema_skeleton({
        "type": "object",
        "properties": {
            "findings": {"type": "array"},
            "title": {"type": "string"},
            "n": {"type": "integer"},
            "ok": {"type": "boolean"},
        },
    }) == {"findings": [], "title": "", "n": 0, "ok": False}
    assert _schema_skeleton(None) == ""
    assert _schema_skeleton({"enum": ["a", "b"]}) == "a"


# 17) --plan: agent() short-circuits to skeletons
@pytest.mark.asyncio
async def test_plan_mode_skeletons():
    recs = []
    src = '''meta = {"name": "p"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Scan")
    a = await agent("x", {"schema": {"type": "object", "properties": {"items": {"type": "array"}}}})
    w = await parallel([lambda: agent("y"), lambda: agent("z")])
    return {"n": len(a["items"]), "w": len(w)}
'''
    r = await run_workflow_source(src, {"plan": True, "auto_effort": True, "on_agent_plan": lambda x: recs.append(x)})
    assert len(recs) == 3
    assert recs[0]["phase"] == "Scan"
    assert recs[0]["effort"] == "xhigh"
    assert recs[1]["effort"] == "high"
    assert r["n"] == 0
    assert r["w"] == 2


# 21) lifecycle events: a start + end per agent
@pytest.mark.asyncio
async def test_lifecycle_events():
    events = []
    async def echo(_p, o):
        return "ok"

    src = 'meta = {"name": "e"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    phase("Scan")\n    await agent("a")\n    await parallel([lambda: agent("b"), lambda: agent("c")])\n    return 1\n'
    await run_workflow_source(src, {"run_agent": echo, "auto_effort": True, "on_event": lambda e: events.append(e)})
    starts = [e for e in events if e["type"] == "start"]
    ends = [e for e in events if e["type"] == "end"]
    assert len(starts) == 3
    assert len(ends) == 3
    assert starts[0]["label"] == "a"
    assert starts[0]["phase"] == "Scan"
    assert starts[0]["effort"] == "xhigh"


# 6b) journal persists to file with metrics
@pytest.mark.asyncio
async def test_journal_persists_metrics():
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "m.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()

        async def echo(_p, o):
            o.get("on_metrics", lambda *_: None)({"ms": 42, "model": "gpt-5.5", "tokens": {"input": 10, "output": 5, "reasoning": 3, "total": 18}})
            return "ok"

        src = 'meta = {"name": "m"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    phase("Scan")\n    await agent("a")\n    await agent("b", {"phase": "Verify"})\n    return 1\n'
        await run_workflow_source(src, {"run_agent": echo, "journal": j, "auto_effort": True})

        lines = [json.loads(l) for l in jpath.read_text(encoding="utf-8").strip().split("\n")]
        assert len(lines) == 2
        assert lines[0]["phase"] == "Scan"
        assert lines[1]["phase"] == "Verify"
        assert lines[0]["tokens"] == 18
        assert lines[0]["tokensOut"] == 8
        assert lines[0]["ms"] == 42
        assert lines[0]["model"] == "gpt-5.5"
        assert lines[0]["effort"] == "xhigh"


# strictifySchema
def test_strictify_schema():
    authored = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "painPoints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"pain": {"type": "string"}, "buyer": {"type": "string"}, "whoFeelsItNow": {"type": "string"}},
                    "required": ["pain", "buyer"],
                },
            },
            "summary": {"type": "string"},
        },
        "required": ["painPoints"],
    }
    strict = strictify_schema(authored)
    assert sorted(strict["required"]) == ["painPoints", "summary"]
    assert sorted(strict["properties"]["painPoints"]["items"]["required"]) == ["buyer", "pain", "whoFeelsItNow"]
    assert strict["properties"]["painPoints"]["items"]["additionalProperties"] is False
    assert strict["properties"]["painPoints"]["items"]["properties"]["whoFeelsItNow"]["type"] == "string"
    # input not mutated
    assert authored["properties"]["painPoints"]["items"]["required"] == ["pain", "buyer"]
    assert strictify_schema({"type": "string"}) == {"type": "string"}


# 18) token meter
def test_token_meter():
    reset_meter()
    record_token_usage({"threadId": "t1", "tokenUsage": {"total": {"inputTokens": 100, "outputTokens": 20, "reasoningOutputTokens": 5}}})
    record_token_usage({"threadId": "t2", "tokenUsage": {"total": {"inputTokens": 50, "outputTokens": 10, "reasoningOutputTokens": 0}}})
    assert tokens_spent() == 185
    assert output_spent() == 35
    t1 = tokens_for_thread("t1")
    assert t1["total"] == 125
    assert t1["output"] == 20
    assert tokens_for_thread("nope") is None
    reset_meter()


# 22) lifecycle events carry stable agent id
@pytest.mark.asyncio
async def test_event_ids():
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "e.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()
        events = []
        async def echo(_p, o):
            return "ok"
        src = 'meta = {"name": "ei"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    phase("Scan")\n    await agent("a")\n    await parallel([lambda: agent("b"), lambda: agent("c")])\n    return 1\n'
        await run_workflow_source(src, {"run_agent": echo, "auto_effort": True, "journal": j, "on_event": lambda e: events.append(e)})
        starts = [e for e in events if e["type"] == "start"]
        ends = [e for e in events if e["type"] == "end"]
        assert all(e.get("id") for e in starts), "all starts have an id"
        assert all(e.get("id") for e in ends), "all ends have an id"
        assert starts[0]["id"] == ends[0]["id"], "start/end share the same id"


# journal replay: second run returns all from journal, 0 fake calls
@pytest.mark.asyncio
async def test_journal_replay():
    call_count = {"n": 0}
    async def echo(_p, o):
        call_count["n"] += 1
        return "result"

    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "r.jsonl"
        j1 = Journal(jpath, reuse=False)
        j1.load()
        src = 'meta = {"name": "r"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    a = await agent("p1")\n    b = await agent("p2")\n    c = await agent("p3")\n    return [a, b, c]\n'
        r1 = await run_workflow_source(src, {"run_agent": echo, "journal": j1})
        assert r1 == ["result", "result", "result"]
        assert call_count["n"] == 3

        # Second run with reuse — all cached
        j2 = Journal(jpath, reuse=True)
        j2.load()
        r2 = await run_workflow_source(src, {"run_agent": echo, "journal": j2})
        assert r2 == ["result", "result", "result"]
        assert call_count["n"] == 3  # no new calls


@pytest.mark.asyncio
async def test_default_agent_auto_resolves_to_pi(monkeypatch):
    monkeypatch.setattr(run_workflow, "_captured_provider_names", lambda: ["codebuddy", "pi"])
    calls = []

    async def echo(_p, o):
        calls.append(dict(o))
        return o.get("agent")

    src = 'meta = {"name": "default-agent"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x")\n'
    result = await run_workflow_source(src, {"run_agent": echo})

    assert result == "pi"
    assert calls[0]["agent"] == "pi"


@pytest.mark.asyncio
async def test_default_agent_precedence_explicit_then_meta_then_auto(monkeypatch):
    monkeypatch.setattr(run_workflow, "_captured_provider_names", lambda: ["pi", "codebuddy"])

    async def echo(_p, o):
        return o.get("agent")

    meta_src = 'meta = {"name": "meta-agent", "default_agent": "codebuddy"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x")\n'
    assert await run_workflow_source(meta_src, {"run_agent": echo}) == "codebuddy"
    assert await run_workflow_source(meta_src, {"default_agent": "pi", "run_agent": echo}) == "pi"


@pytest.mark.asyncio
async def test_default_agent_recorded_in_events_and_journal(monkeypatch):
    monkeypatch.setattr(run_workflow, "_captured_provider_names", lambda: ["pi"])
    events = []

    async def echo(_p, o):
        return "ok"

    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "default-agent.jsonl"
        j = Journal(jpath, reuse=False)
        j.load()
        src = 'meta = {"name": "default-agent"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return await agent("x")\n'
        await run_workflow_source(src, {"run_agent": echo, "journal": j, "on_event": lambda e: events.append(e)})

        assert events[0]["type"] == "defaults"
        assert events[0]["agent"] == "pi"
        assert events[0]["source"] == "auto"
        starts = [e for e in events if e["type"] == "start"]
        assert starts[0]["agent"] == "pi"
        rows = [json.loads(line) for line in jpath.read_text(encoding="utf-8").splitlines()]
        assert rows[0]["agent"] == "pi"


def test_cli_meta_records_effective_default_agent(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_workflow, "_captured_provider_names", lambda: ["pi"])
    script = tmp_path / "tiny.workflow.py"
    script.write_text(
        'meta = {"name": "tiny"}\nasync def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):\n    return {"ok": True}\n',
        encoding="utf-8",
    )
    args = argparse.Namespace(
        script=str(script),
        args=None,
        args_file=None,
        budget=None,
        agent=None,
        model=None,
        pin_model=None,
        effort=None,
        auto_effort=False,
        pin_effort=None,
        plan=False,
        resume=False,
        journal=None,
        run_id=None,
        fresh=True,
        no_journal=False,
        summary=False,
        no_summary=False,
        json=True,
        transport="servitor",
    )

    assert _cmd_run(args) == 0

    meta = json.loads((tmp_path / ".workflow-journal" / "tiny.workflow.meta.json").read_text(encoding="utf-8"))
    assert meta["effectiveAgent"] == "pi"
    assert meta["effectiveAgentSource"] == "auto"
