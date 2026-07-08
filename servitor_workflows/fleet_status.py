"""Fleet supervision: roll the state of many concurrent workflow runs into one digest.

1:1 Python port of runner/src/fleetStatus.js inspect_run() + render_fleet_text().
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from .run_model import build_live_run_model, read_run_meta, result_path_for, progress_path_for
from .run_summary import _fmt_tokens, _fmt_ms


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def inspect_run(journal_path: str, *, now: float | None = None,
                stall_after_ms: float = 120_000,
                is_alive=_pid_alive) -> dict:
    """One run's supervision view. 1:1 port of upstream inspectRun()."""
    now = now or time.time()
    meta = read_run_meta(journal_path)
    script_path = None
    if meta and meta.get("script") and Path(meta["script"]).exists():
        script_path = meta["script"]

    run = build_live_run_model({"journal_path": journal_path, "script_path": script_path})

    result_at = None
    try:
        rp = result_path_for(journal_path)
        if Path(rp).exists():
            result_at = Path(rp).stat().st_mtime
    except OSError:
        pass
    started_at = meta.get("startedAt") if meta else None
    if started_at:
        started_at = started_at / 1000.0  # ms -> s

    if result_at is not None and (started_at is None or result_at >= started_at):
        state = "completed"
    elif meta and is_alive(meta.get("pid")):
        state = "running"
    elif meta:
        state = "stopped"
    else:
        state = "idle"

    running_agents = [a for a in run.get("agents", []) if a.get("status") == "running"]
    done = len(run.get("agents", [])) - len(running_agents)
    current_phase = running_agents[0].get("phase") if running_agents else (
        run["agents"][-1].get("phase") if run.get("agents") else None
    )

    pending_questions = [q for q in run.get("questions", []) if not q.get("answered")]

    tokens = run.get("totals", {}).get("tokens", 0)
    budget = meta.get("budget") if meta else None

    return {
        "journal": journal_path,
        "name": run.get("name", ""),
        "runId": meta.get("runId") if meta else None,
        "script": script_path or (meta.get("script") if meta else None),
        "pid": meta.get("pid") if meta else None,
        "state": state,
        "startedAt": started_at,
        "phase": current_phase,
        "agents": {"done": done, "running": len(running_agents), "total": len(run.get("agents", []))},
        "sessions": len(run.get("sessions", [])),
        "tokens": tokens,
        "budget": budget,
        "overBudget": budget is not None and tokens >= budget,
        "pendingQuestions": pending_questions,
        "result": run.get("result") if state == "completed" else None,
    }


def render_fleet_text(infos: list[dict], *, result_chars: int = 220) -> str:
    """Compact text digest. 1:1 port of upstream renderFleetText()."""
    if not infos:
        return "fleet: no runs found"
    counts: dict[str, int] = {}
    attention = 0
    for r in infos:
        counts[r["state"]] = counts.get(r["state"], 0) + 1
        if r.get("pendingQuestions") or r.get("state") == "stopped" or r.get("overBudget"):
            attention += 1

    head = " · ".join(f"{n} {s}" for s, n in counts.items() if n > 0)
    lines = [f"fleet: {len(infos)} run{'s' if len(infos) != 1 else ''} — {head}"
             + (f"  ⚠ {attention} need{'s' if attention == 1 else ''} attention" if attention else ""),
             ""]

    glyphs = {"running": "▶", "completed": "✔", "stopped": "■", "idle": "·"}
    for r in infos:
        name = r["name"]
        bits = []
        if r["state"] == "running":
            bits.append("running")
        elif r["state"] == "completed":
            bits.append("completed")
        elif r["state"] == "stopped":
            bits.append("stopped WITHOUT a result (resumable)")
        else:
            bits.append("idle")
        if r.get("phase"):
            bits.append(f"phase {r['phase']}")
        bits.append(f"{r['agents']['done']} done" + (f" + {r['agents']['running']} running" if r["agents"]["running"] else ""))
        if r["sessions"]:
            bits.append(f"{r['sessions']} worker{'s' if r['sessions'] != 1 else ''}")
        bits.append(f"{_fmt_tokens(r['tokens'])} tok" + (f" / {_fmt_tokens(r['budget'])} budget" if r.get("budget") else ""))
        lines.append(f"{glyphs.get(r['state'], '?')} {name} — {' · '.join(bits)}")

        if r["state"] == "completed" and r.get("result") is not None:
            import json
            try:
                s = json.dumps(r["result"])
            except Exception:
                s = str(r["result"])
            if len(s) > result_chars:
                s = s[:result_chars] + "…"
            lines.append(f"  result: {s}")

    return "\n".join(lines)
