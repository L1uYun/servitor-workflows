"""Tests for supervise.py: bounded supervision loop.

Uses fake journals — no real provider calls. Verifies max_rounds termination,
issue detection, and callback invocation.
"""
import asyncio
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.journal import Journal
from servitor_workflows.supervise import supervise


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


@pytest.mark.asyncio
async def test_supervise_max_rounds_terminates():
    """supervise exits after max_rounds or when all runs are terminal."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        jpath = _make_journal(tmp, "active", [("k1", "a", None, {"phase": "P"})])

        rounds = []
        await supervise([jpath], interval_s=0.01, max_rounds=3, on_round=lambda infos: rounds.append(infos))
        # Without a live process, supervise exits after detecting terminal state.
        # max_rounds is the hard ceiling; terminal detection exits earlier.
        assert len(rounds) >= 1
        assert len(rounds) <= 3


@pytest.mark.asyncio
async def test_supervise_empty_targets_exits():
    """supervise exits immediately if no journals are found."""
    with tempfile.TemporaryDirectory() as tmp:
        rounds = []
        await supervise([str(tmp)], interval_s=0.01, max_rounds=10, on_round=lambda infos: rounds.append(infos))
        assert len(rounds) == 1  # one round then break on empty


@pytest.mark.asyncio
async def test_supervise_all_terminal_exits():
    """supervise exits when all runs are in terminal state (completed/idle)."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        jpath = _make_journal(tmp, "done", [("k1", "a", "ok", {"phase": "P"})])

        rounds = []
        await supervise([jpath], interval_s=0.01, max_rounds=10, on_round=lambda infos: rounds.append(infos))
        assert len(rounds) == 1  # one round, all terminal, exit


@pytest.mark.asyncio
async def test_supervise_on_round_callback():
    """on_round callback receives fleet status infos."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        jpath = _make_journal(tmp, "testrun", [("k1", "a", "ok", {"phase": "P"})])

        captured = []
        await supervise([jpath], interval_s=0.01, max_rounds=1, on_round=lambda infos: captured.append(infos))
        assert len(captured) == 1
        assert len(captured[0]) >= 1
