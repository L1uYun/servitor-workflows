"""Longitudinal run analytics: read MANY journals over time.

1:1 Python port of runner/src/compareRuns.js. Pure beyond reading run files.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .run_summary import summarize_run, _fmt_tokens, _fmt_ms
from .run_model import list_journals


def _resolve_targets(targets: list[str]) -> list[str]:
    """Resolve targets (dirs and/or journal paths) to journal paths."""
    if not targets:
        targets = ["."]
    journals: list[str] = []
    seen: set[str] = set()
    for t in targets:
        tpath = Path(t).resolve()
        if tpath.suffix == ".jsonl":
            if str(tpath) not in seen:
                seen.add(str(tpath))
                journals.append(str(tpath))
        else:
            for j in list_journals(tpath):
                if j["path"] not in seen:
                    seen.add(j["path"])
                    journals.append(j["path"])
    return journals


def _group_name(summary_name: str | None, journal_path: str) -> str:
    import re
    if summary_name:
        return re.sub(r"--[\w.-]+$", "", summary_name)
    base = Path(journal_path).stem.replace(".workflow", "")
    return re.sub(r"--[\w.-]+$", "", base)


def collect_comparison(targets: list[str], *, now: float | None = None) -> dict:
    """Collect comparison data across multiple runs. 1:1 port of collectComparison()."""
    import time
    now = now or time.time()
    rows = []
    for journal in _resolve_targets(targets):
        try:
            s = summarize_run(journal_path=journal)
        except Exception:
            continue
        journaled = s.get("counts", {}).get("journaledAgents", 0)
        if not journaled:
            continue
        when = 0
        try:
            when = Path(journal).stat().st_mtime
        except OSError:
            pass
        null_results = s.get("counts", {}).get("nullResults")
        completed = (journaled - null_results) if null_results is not None else s.get("counts", {}).get("completedAgents", 0)
        rows.append({
            "name": _group_name(s.get("name"), journal),
            "journal": journal,
            "when": when,
            "agoMs": max(0, (now - when) * 1000) if when else None,
            "agents": journaled,
            "completionRate": completed / journaled if journaled else None,
            "nullResults": null_results,
            "cached": s.get("counts", {}).get("cachedAgents"),
            "sessions": s.get("counts", {}).get("sessionWorkers", len(s.get("sessions", []))),
            "tokensRun": s.get("metrics", {}).get("executedTokens") or s.get("metrics", {}).get("totalTokens", 0),
            "tokensAllIn": s.get("metrics", {}).get("totalTokens", 0),
            "wallMs": s.get("metrics", {}).get("runWallMs"),
            "agentMs": s.get("metrics", {}).get("totalAgentMs"),
            "budget": {"total": s["budget"]["total"], "fraction": s["budget"]["fraction"]} if s.get("budget") else None,
            "warnings": len(s.get("warnings", [])),
        })
    rows.sort(key=lambda x: -x["when"])

    groups: dict[str, dict] = {}
    for r in rows:
        g = groups.get(r["name"], {"name": r["name"], "runs": []})
        g["runs"].append(r)
        groups[r["name"]] = g
    rollups = []
    for g in groups.values():
        runs = g["runs"]
        avg_tokens = sum(r["tokensRun"] for r in runs) / len(runs) if runs else 0
        rated = [r for r in runs if r["completionRate"] is not None]
        completion = sum(r["completionRate"] for r in rated) / len(rated) if rated else None
        trend = None
        if len(runs) >= 2 and runs[1]["tokensRun"] > 0:
            trend = (runs[0]["tokensRun"] - runs[1]["tokensRun"]) / runs[1]["tokensRun"]
        rollups.append({
            "name": g["name"], "runs": len(runs), "avgTokens": avg_tokens,
            "completion": completion, "trendPct": trend,
            "lastAgoMs": runs[0]["agoMs"] if runs else None,
        })
    rollups.sort(key=lambda x: (-x["runs"], x.get("lastAgoMs") or 0))
    return {"rows": rows, "rollups": rollups}


def _pct(x):
    return "—" if x is None else str(round(x * 100)) + "%"


def render_comparison_text(data: dict) -> str:
    """1:1 port of upstream renderComparisonText()."""
    rows = data["rows"]
    rollups = data["rollups"]
    if not rows:
        return "compare-runs: no journaled runs found under the given targets"
    lines = [f"compare-runs: {len(rows)} run{'s' if len(rows) != 1 else ''} · "
             f"{len(rollups)} workflow{'s' if len(rollups) != 1 else ''}", ""]

    def _pad(s, n):
        s = str(s)
        return s[:n].ljust(n)

    def _pad_s(s, n):
        s = str(s)
        return s[:n].rjust(n)

    lines.append(_pad("workflow", 26) + _pad_s("when", 9) + _pad_s("agents", 8) +
                 _pad_s("ok", 6) + _pad_s("cached", 8) + _pad_s("tokens", 9) +
                 _pad_s("wall", 9) + "  flags")
    for r in rows:
        flags = []
        if r.get("budget") and r["budget"]["fraction"] >= 1:
            flags.append("over-budget")
        elif r.get("budget") and r["budget"]["fraction"] >= 0.8:
            flags.append("budget>=80%")
        if r.get("nullResults"):
            flags.append(f"{r['nullResults']} null")
        if r.get("warnings"):
            flags.append(f"{r['warnings']} warn")
        ago = _fmt_ms(r["agoMs"]) if r.get("agoMs") else "—"
        lines.append(
            _pad(r["name"], 26) + _pad_s(ago, 9) +
            _pad_s(str(r["agents"]) + (f"({r['sessions']}w)" if r.get("sessions") else ""), 8) +
            _pad_s(_pct(r["completionRate"]), 6) +
            _pad_s(str(r.get("cached") or "—"), 8) +
            _pad_s(_fmt_tokens(r["tokensRun"]) or "—", 9) +
            _pad_s(_fmt_ms(r["wallMs"]) or ("Σ" + _fmt_ms(r["agentMs"]) if r.get("agentMs") else "—"), 9) +
            (f"  ⚠ {' · '.join(flags)}" if flags else "")
        )

    repeated = [g for g in rollups if g["runs"] >= 2]
    if repeated:
        lines.append("")
        lines.append("run-over-run (same workflow):")
        for g in repeated:
            trend = ""
            if g["trendPct"] is not None:
                sign = "+" if g["trendPct"] >= 0 else ""
                trend = f" · latest vs prev: {sign}{round(g['trendPct'] * 100)}% tokens"
            lines.append(f"  {g['name']} — {g['runs']} runs · avg {_fmt_tokens(round(g['avgTokens']))} tok/run · {_pct(g['completion'])} ok{trend}")

    lines.append("")
    lines.append("tokens = the run's own executed spend where the event sidecar can tell, else the journal's all-in total.")
    return "\n".join(lines)
