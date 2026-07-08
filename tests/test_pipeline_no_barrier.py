"""Pipeline no-global-barrier test: items progress independently.

This is a Phase 1 acceptance criterion: fast item finishes stage1 before
slow item, proving no global barrier between items.
"""
import asyncio
import tempfile
from pathlib import Path

import pytest

from servitor_workflows.run_workflow import run_workflow_source


@pytest.mark.asyncio
async def test_pipeline_no_global_barrier():
    """Fast item's stage2 starts before slow item's stage1 finishes.

    This proves items progress independently through stages with no barrier.
    """
    src = '''import time

meta = {"name": "nobarrier"}

async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    timings = {}

    async def slow_s1(item):
        if item == "slow":
            await asyncio.sleep(0.15)
        timings.setdefault(item, {})["s1_done"] = time.monotonic()
        return item

    async def fast_s2(item):
        timings.setdefault(item, {})["s2_start"] = time.monotonic()
        return item

    await pipeline(["slow", "fast"], slow_s1, fast_s2)
    # After pipeline completes, all timings are populated
    return {
        "slow_s1_done": timings["slow"]["s1_done"],
        "fast_s2_start": timings["fast"]["s2_start"],
    }
'''
    r = await run_workflow_source(src, {})
    # The fast item's stage2 must start before the slow item's stage1 finishes
    assert r["fast_s2_start"] < r["slow_s1_done"], \
        "No global barrier: fast item stage2 starts before slow item stage1 finishes"
