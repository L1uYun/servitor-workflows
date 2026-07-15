"""Tests for StructuredOutput: control/analysis separation with JSON Schema validation.

Covers:
- parse_control_analysis: tag extraction, fallback, dict passthrough, errors
- StructuredOutput schema_fingerprint stability
- runtime.agent() with output=StructuredOutput: plan mode skeleton, real call parsing
- journal identity includes output_schema fingerprint
- backward compat: agent() without output= still returns raw string
- StructuredOutputError classification
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.structured_output import (
    StructuredOutput,
    StructuredOutputError,
    parse_control_analysis,
    schema_skeleton,
)
from servitor_workflows.journal import identity_hash, Journal
from servitor_workflows.run_workflow import run_workflow_source
from servitor_workflows.runtime import create_runtime


# ── parse_control_analysis unit tests ──────────────────────────────────────

def test_parse_normal_control_and_analysis():
    schema = {"type": "object", "properties": {"verdict": {"type": "string"}}, "required": ["verdict"]}
    so = StructuredOutput(control_schema=schema)
    text = '<analysis>The code is clean.</analysis><control>{"verdict": "pass"}</control>'
    r = parse_control_analysis(text, so)
    assert r == {"control": {"verdict": "pass"}, "analysis": "The code is clean."}


def test_parse_control_only_no_analysis_tag():
    schema = {"type": "object", "properties": {"x": {"type": "number"}}, "required": ["x"]}
    so = StructuredOutput(control_schema=schema, analysis=False)
    text = '<control>{"x": 42}</control>'
    r = parse_control_analysis(text, so)
    assert r == {"control": {"x": 42}, "analysis": ""}


def test_parse_missing_control_raises():
    so = StructuredOutput(control_schema={"type": "object"})
    with pytest.raises(StructuredOutputError) as exc_info:
        parse_control_analysis("just some prose without any JSON", so)
    assert exc_info.value.failure_reason == "missing_control"


def test_parse_invalid_json_in_control_raises():
    so = StructuredOutput(control_schema={"type": "object"})
    with pytest.raises(StructuredOutputError) as exc_info:
        parse_control_analysis("<control>not valid json</control>", so)
    assert exc_info.value.failure_reason == "invalid_control_json"


def test_parse_fallback_json_extraction():
    schema = {"type": "object", "properties": {"v": {"type": "string"}}, "required": ["v"]}
    so = StructuredOutput(control_schema=schema)
    text = 'Here is my result: {"v": "ok"}'
    r = parse_control_analysis(text, so)
    assert r["control"] == {"v": "ok"}


def test_parse_dict_passthrough():
    so = StructuredOutput(control_schema={"type": "object"})
    r = parse_control_analysis({"already": "parsed"}, so)
    assert r == {"control": {"already": "parsed"}, "analysis": ""}


def test_parse_analysis_fallback_to_prose_outside_control():
    so = StructuredOutput(control_schema={"type": "object"})
    text = 'Some reasoning. <control>{"ok": true}</control> More text.'
    r = parse_control_analysis(text, so)
    assert r["control"] == {"ok": True}
    assert "Some reasoning." in r["analysis"]


def test_parse_custom_tags():
    so = StructuredOutput(control_schema={"type": "object"}, control_tag="result", analysis_tag="reasoning")
    text = '<reasoning>Because.</reasoning><result>{"a": 1}</result>'
    r = parse_control_analysis(text, so)
    assert r == {"control": {"a": 1}, "analysis": "Because."}


# ── StructuredOutput dataclass tests ───────────────────────────────────────

def test_schema_fingerprint_stability():
    s = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}
    so1 = StructuredOutput(control_schema=s)
    so2 = StructuredOutput(control_schema=s)
    assert so1.schema_fingerprint() == so2.schema_fingerprint()


def test_schema_fingerprint_changes_with_schema():
    so1 = StructuredOutput(control_schema={"type": "object"})
    so2 = StructuredOutput(control_schema={"type": "array"})
    assert so1.schema_fingerprint() != so2.schema_fingerprint()


def test_schema_fingerprint_changes_with_analysis_flag():
    s = {"type": "object"}
    so1 = StructuredOutput(control_schema=s, analysis=True)
    so2 = StructuredOutput(control_schema=s, analysis=False)
    assert so1.schema_fingerprint() != so2.schema_fingerprint()


def test_instruction_text_contains_tag_and_schema():
    s = {"type": "object", "properties": {"v": {"type": "string"}}, "required": ["v"]}
    so = StructuredOutput(control_schema=s)
    text = so.instruction_text()
    assert "<control>" in text
    assert "</control>" in text
    assert '"v"' in text
    assert "analysis" in text


def test_instruction_text_no_analysis_when_disabled():
    so = StructuredOutput(control_schema={"type": "object"}, analysis=False)
    text = so.instruction_text()
    assert "<control>" in text
    assert "analysis" not in text


# ── schema_skeleton (plan mode) ────────────────────────────────────────────

def test_schema_skeleton_basic():
    s = {"type": "object", "properties": {"v": {"type": "string", "enum": ["a", "b"]}}, "required": ["v"]}
    so = StructuredOutput(control_schema=s)
    skel = schema_skeleton(so)
    assert skel == {"control": {"v": "a"}, "analysis": ""}


def test_schema_skeleton_no_analysis():
    s = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    so = StructuredOutput(control_schema=s, analysis=False)
    skel = schema_skeleton(so)
    assert skel == {"control": {"n": 0}, "analysis": None}


# ── StructuredOutputError tests ────────────────────────────────────────────

def test_error_codex_error_info_compatibility():
    err = StructuredOutputError("missing_control", raw="some text")
    assert err.codex_error_info == "missing_control"
    assert err.raw == "some text"
    assert err.schema_errors == []


def test_error_with_schema_errors():
    errs = ["field 'x' is required"]
    err = StructuredOutputError("schema_validation_failed", raw="...", schema_errors=errs)
    assert err.schema_errors == errs


# ── runtime integration: plan mode ─────────────────────────────────────────

async def test_plan_mode_returns_skeleton():
    async def fake_agent(_prompt, _opts):
        pytest.fail("should not call agent in plan mode")

    s = {"type": "object", "properties": {"verdict": {"type": "string", "enum": ["pass", "fail"]}}, "required": ["verdict"]}
    so = StructuredOutput(control_schema=s)
    rt = create_runtime(plan=True, run_agent=fake_agent)
    result = await rt["agent"]("Review this", {"output": so, "label": "test"})
    assert result == {"control": {"verdict": "pass"}, "analysis": ""}


# ── runtime integration: real call with fake transport ─────────────────────

async def test_agent_with_structured_output_parses_control():
    async def fake_agent(prompt, opts):
        # Verify the prompt was augmented with instruction text
        assert "<control>" in prompt
        return '<analysis>Looks good.</analysis><control>{"verdict": "pass"}</control>'

    s = {"type": "object", "properties": {"verdict": {"type": "string"}}, "required": ["verdict"]}
    so = StructuredOutput(control_schema=s)
    rt = create_runtime(run_agent=fake_agent)
    result = await rt["agent"]("Review this", {"output": so, "label": "test"})
    assert result == {"control": {"verdict": "pass"}, "analysis": "Looks good."}


async def test_agent_with_structured_output_missing_control_raises():
    async def fake_agent(prompt, opts):
        return "I forgot to include the control block"

    so = StructuredOutput(control_schema={"type": "object"})
    rt = create_runtime(run_agent=fake_agent)
    with pytest.raises(StructuredOutputError) as exc_info:
        await rt["agent"]("Review this", {"output": so})
    assert exc_info.value.failure_reason == "missing_control"


async def test_agent_with_structured_output_invalid_json_raises():
    async def fake_agent(prompt, opts):
        return "<control>not json at all</control>"

    so = StructuredOutput(control_schema={"type": "object"})
    rt = create_runtime(run_agent=fake_agent)
    with pytest.raises(StructuredOutputError) as exc_info:
        await rt["agent"]("Review this", {"output": so})
    assert exc_info.value.failure_reason == "invalid_control_json"


# ── runtime integration: backward compat ───────────────────────────────────

async def test_agent_without_output_returns_raw():
    async def fake_agent(prompt, opts):
        assert "control" not in prompt  # no instruction appended
        return "just a plain string"

    rt = create_runtime(run_agent=fake_agent)
    result = await rt["agent"]("Hello", {})
    assert result == "just a plain string"


async def test_agent_with_schema_still_works():
    """Legacy opts['schema'] should still work unchanged."""
    async def fake_agent(prompt, opts):
        return '{"x": 1}'

    schema = {"type": "object", "properties": {"x": {"type": "number"}}, "required": ["x"]}
    rt = create_runtime(run_agent=fake_agent)
    result = await rt["agent"]("Hello", {"schema": schema})
    # Legacy schema path returns parsed dict directly (no control/analysis wrapper)
    assert json.loads(result) == {"x": 1}


# ── journal identity includes output_schema ────────────────────────────────

def test_journal_identity_includes_output_schema():
    so = StructuredOutput(control_schema={"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]})
    fp = so.schema_fingerprint()

    opts_with = {"model": "test", "output_schema": fp}
    opts_without = {"model": "test"}

    key_with = identity_hash("same prompt", opts_with)
    key_without = identity_hash("same prompt", opts_without)
    assert key_with != key_without


# ── end-to-end workflow test ───────────────────────────────────────────────

async def test_workflow_with_structured_output():
    async def fake_agent(prompt, opts):
        assert "<control>" in prompt
        return '<control>{"score": 8}</control>'

    src = '''
meta = {"name": "so-test"}

async def main(agent, phase, log):
    from servitor_workflows import StructuredOutput
    so = StructuredOutput(control_schema={
        "type": "object",
        "properties": {"score": {"type": "number"}},
        "required": ["score"]
    })
    result = await agent("Rate this code", {"output": so, "label": "rater"})
    return result
'''
    r = await run_workflow_source(src, {"run_agent": fake_agent})
    assert r == {"control": {"score": 8}, "analysis": ""}


async def test_workflow_structured_output_plan_mode():
    async def fake_agent(prompt, opts):
        pytest.fail("should not call agent in plan mode")

    src = '''
meta = {"name": "so-plan"}

async def main(agent, phase, log, plan):
    from servitor_workflows import StructuredOutput
    so = StructuredOutput(control_schema={
        "type": "object",
        "properties": {"verdict": {"type": "string", "enum": ["pass", "fail"]}},
        "required": ["verdict"]
    })
    result = await agent("Review", {"output": so, "label": "rev"})
    return {"result": result, "plan": plan}
'''
    r = await run_workflow_source(src, {"run_agent": fake_agent, "plan": True})
    assert r["result"] == {"control": {"verdict": "pass"}, "analysis": ""}
    assert r["plan"] is True

