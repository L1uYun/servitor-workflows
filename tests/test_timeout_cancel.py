"""Tests for --cancel-file graceful cancellation (karma #330).

Verifies that:
- cancel_file sentinel is checked before agent/parallel/pipeline/phase calls
- WorkflowCancelled is a subclass of RuntimeError
- Cancel file appearing mid-run stops the next agent call
- No cancel_file means normal execution
"""
import asyncio
import sys
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.run_workflow import run_workflow_source


@pytest.mark.asyncio
async def test_cancel_file_stops_before_agent_call(tmp_path):
    """If cancel_file exists before the workflow starts, agent() should raise WorkflowCancelled."""
    from servitor_workflows.runtime import WorkflowCancelled
    sentinel = tmp_path / "cancel.flag"
    sentinel.write_text("cancel", encoding="utf-8")

    src = '''meta = {"name": "cancel-agent"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    result = await agent("do something", {"agent": "pi"})
    return result
'''
    with pytest.raises(WorkflowCancelled, match="cancel file"):
        await run_workflow_source(src, {"cancel_file": str(sentinel)})


@pytest.mark.asyncio
async def test_cancel_file_created_during_run_stops_next_agent(tmp_path):
    """If cancel_file appears between agent calls, the second agent() should raise."""
    from servitor_workflows.runtime import WorkflowCancelled
    sentinel = tmp_path / "cancel-during.flag"

    call_count = [0]

    async def fake_agent(prompt, opts):
        call_count[0] += 1
        if call_count[0] == 1:
            # After first agent returns, create the sentinel
            sentinel.write_text("cancel", encoding="utf-8")
        return f"agent-{call_count[0]}"

    src = '''meta = {"name": "cancel-mid"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    r1 = await agent("first", {"agent": "pi"})
    r2 = await agent("second", {"agent": "pi"})
    return [r1, r2]
'''
    with pytest.raises(WorkflowCancelled, match="cancel file"):
        await run_workflow_source(src, {"cancel_file": str(sentinel), "run_agent": fake_agent})

    assert call_count[0] == 1  # only the first agent ran


@pytest.mark.asyncio
async def test_cancel_file_checked_before_parallel(tmp_path):
    """cancel_file should be detected before parallel() starts."""
    from servitor_workflows.runtime import WorkflowCancelled
    sentinel = tmp_path / "cancel-parallel.flag"
    sentinel.write_text("cancel", encoding="utf-8")

    src = '''meta = {"name": "cancel-par"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    r = await parallel([lambda: 1, lambda: 2])
    return r
'''
    with pytest.raises(WorkflowCancelled, match="cancel file"):
        await run_workflow_source(src, {"cancel_file": str(sentinel)})


@pytest.mark.asyncio
async def test_cancel_file_checked_before_phase(tmp_path):
    """cancel_file should be detected before phase()."""
    from servitor_workflows.runtime import WorkflowCancelled
    sentinel = tmp_path / "cancel-phase.flag"
    sentinel.write_text("cancel", encoding="utf-8")

    src = '''meta = {"name": "cancel-phase-wf"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("New Phase")
    return "done"
'''
    with pytest.raises(WorkflowCancelled, match="cancel file"):
        await run_workflow_source(src, {"cancel_file": str(sentinel)})


@pytest.mark.asyncio
async def test_no_cancel_file_means_normal_execution():
    """Without a cancel_file, the workflow should run normally."""
    src = '''meta = {"name": "normal"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Step 1")
    return "completed"
'''
    result = await run_workflow_source(src, {"cancel_file": None})
    assert result == "completed"


def test_workflow_cancelled_is_runtime_error():
    """WorkflowCancelled must be a subclass of RuntimeError for pytest to catch it cleanly."""
    from servitor_workflows.runtime import WorkflowCancelled
    assert issubclass(WorkflowCancelled, RuntimeError)
