"""Tests for compare_runs.py: collect_comparison() and render_comparison_text().

Uses fake journals created via the Journal API — no real provider calls.
"""
import json
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.journal import Journal
from servitor_workflows.compare_runs import collect_comparison, render_comparison_text, _resolve_targets, _group_name


def _make_journal(tmp: Path, name: str, entries: list[tuple[str, str, str, dict]]) -> str:
    """Create a journal file under .workflow-journal/ with the given entries."""
    jdir = tmp / ".workflow-journal"
    jdir.mkdir(parents=True, exist_ok=True)
    jpath = jdir / f"{name}.workflow.jsonl"
    j = Journal(jpath, reuse=False)
    j.load()
    for key, label, result, meta in entries:
        j.record(key, label, result, meta)
    return str(jpath)


def test_resolve_targets_journal_path():
    """A .jsonl path is returned as-is."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        jpath = _make_journal(tmp, "single", [("k1", "a", "ok", {"phase": "P"})])
        result = _resolve_targets([jpath])
        assert result == [jpath]


def test_resolve_targets_dir_finds_journals():
    """A directory is scanned for .workflow-journal/*.jsonl files."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        _make_journal(tmp, "alpha", [("k1", "a", "ok", {"phase": "P"})])
        _make_journal(tmp, "beta", [("k2", "b", "ok", {"phase": "P"})])
        result = _resolve_targets([str(tmp)])
        assert len(result) == 2


def test_resolve_targets_dedup():
    """Duplicate paths are deduplicated."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        jpath = _make_journal(tmp, "dup", [("k1", "a", "ok", {"phase": "P"})])
        result = _resolve_targets([jpath, jpath])
        assert len(result) == 1


def test_group_name_strips_workflow_suffix():
    """_group_name removes .workflow and --timestamp suffixes."""
    assert _group_name("hello--2026-07-08T12-00.workflow", "/path/hello--2026-07-08T12-00.workflow.jsonl") == "hello"
    assert _group_name(None, "/path/hello.workflow.jsonl") == "hello"
    assert _group_name(None, "/path/smoke--abc123.workflow.jsonl") == "smoke"


def test_collect_comparison_basic():
    """collect_comparison reads multiple journals and groups by workflow name."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        _make_journal(tmp, "hello", [("k1", "a", "ok", {"phase": "P", "ms": 100})])
        _make_journal(tmp, "hello2", [("k2", "a", "ok", {"phase": "P", "ms": 200})])

        data = collect_comparison([str(tmp)])
        assert len(data["rows"]) >= 2


def test_collect_comparison_empty_dir():
    """An empty directory produces no rows."""
    with tempfile.TemporaryDirectory() as tmp:
        data = collect_comparison([tmp])
        assert data["rows"] == []
        assert data["rollups"] == []


def test_render_comparison_text_has_header():
    """render_comparison_text produces a text table with a header."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        _make_journal(tmp, "smoke", [("k1", "a", "ok", {"phase": "P"})])
        data = collect_comparison([str(tmp)])
        text = render_comparison_text(data)
        assert "compare-runs:" in text
        assert "workflow" in text


def test_render_comparison_text_empty():
    """render_comparison_text handles no runs gracefully."""
    with tempfile.TemporaryDirectory() as tmp:
        data = collect_comparison([tmp])
        text = render_comparison_text(data)
        assert "no journaled runs" in text
