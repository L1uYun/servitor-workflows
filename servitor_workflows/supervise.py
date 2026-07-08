"""Fleet supervisor: poll runs, detect issues, answer gates, resume stopped runs.

1:1 Python port of runner/bin/supervise.js core loop. Runs a polling cycle that
checks fleet status and reports issues needing attention.
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable

from .fleet_status import inspect_run, render_fleet_text


async def supervise(
    targets: list[str],
    *,
    interval_s: float = 5.0,
    max_rounds: int | None = None,
    stall_after_ms: float = 120_000,
    on_round: Callable | None = None,
    on_attention: Callable | None = None,
) -> None:
    """Run a supervision loop over fleet targets.

    1:1 port of upstream supervise.js polling cycle. Each round:
    1. Inspect all runs
    2. Report status
    3. Flag runs needing attention (stalled, over-budget, stopped, pending questions)
    4. Sleep interval_s
    5. Repeat until max_rounds or Ctrl-C

    on_round(infos) is called each cycle with the full fleet status.
    on_attention(issues) is called when any run needs attention.
    """
    from .run_model import list_journals
    from pathlib import Path

    round_num = 0
    while True:
        round_num += 1
        if max_rounds and round_num > max_rounds:
            break

        # Resolve journals
        journals: list[str] = []
        for t in targets:
            tpath = Path(t)
            if tpath.suffix == ".jsonl":
                journals.append(str(tpath))
            else:
                for j in list_journals(tpath):
                    journals.append(j["path"])

        infos = [inspect_run(j, stall_after_ms=stall_after_ms) for j in journals]

        if on_round:
            on_round(infos)
        else:
            print(render_fleet_text(infos))

        # Check for attention items
        issues = []
        for info in infos:
            if info.get("pendingQuestions"):
                issues.append({"type": "question", "run": info["name"], "questions": info["pendingQuestions"]})
            if info.get("state") == "stopped":
                issues.append({"type": "stopped", "run": info["name"], "journal": info["journal"]})
            if info.get("overBudget"):
                issues.append({"type": "over_budget", "run": info["name"], "tokens": info["tokens"], "budget": info["budget"]})
            if info.get("stalled"):
                issues.append({"type": "stalled", "run": info["name"], "journal": info["journal"]})

        if issues and on_attention:
            on_attention(issues)
        elif issues:
            for issue in issues:
                print(f"  ⚠ {issue['type']}: {issue['run']}", flush=True)

        if not infos:
            break  # no runs to supervise

        # Check if all runs are terminal
        all_terminal = all(i["state"] in ("completed", "idle") for i in infos)
        if all_terminal:
            break

        await asyncio.sleep(interval_s)
