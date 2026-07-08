"""Run summary: read a workflow run's journal + sidecars and distill a report.

1:1 Python port of runner/src/runSummary.js summarize_run() + renderers.
Pure beyond file reads; NEVER writes the journal.
"""
from __future__ import annotations

from typing import Any

from .run_model import build_run_model, read_events, read_run_meta, live_state


def _fmt_tokens(n: int | None) -> str | None:
    if n is None:
        return None
    if n >= 1e6:
        return (f"{n/1e6:.{'0' if n >= 1e7 else '1'}f}M")
    if n >= 1e3:
        return f"{round(n/1e3)}k"
    return str(n)


def _fmt_ms(ms: int | None) -> str | None:
    if ms is None:
        return None
    sec = ms / 1000
    if sec < 60:
        return f"{sec:.1f}s" if sec < 10 else str(round(sec)) + "s"
    total = round(sec)
    return f"{total // 60}m{str(total % 60).zfill(2)}s"


def _pct(n: float | None) -> str | None:
    if n is None:
        return None
    return str(round(n * 100)) + "%"


def summarize_run(*, journal_path: str, script_path: str | None = None,
                  run_dir: str | None = None, title: str | None = None,
                  include_result: bool = False) -> dict:
    """Build the structured summary. 1:1 port of upstream summarizeRun()."""
    run = build_run_model(journal_path=journal_path, script_path=script_path,
                          run_dir=run_dir, title=title)
    agents = run.get("agents", [])
    events = read_events(journal_path)
    meta = read_run_meta(journal_path)

    sessions = run.get("sessions", [])
    session_turns = sum(len(w["turns"]) for w in sessions)
    steer_turns = sum(max(0, len(w["turns"]) - 1) for w in sessions)
    cancelled_turns = sum(1 for w in sessions for t in w["turns"] if t["status"] == "cancelled")
    failed_turns = sum(1 for w in sessions for t in w["turns"] if t["status"] == "failed")
    interrupted_turns = sum(1 for w in sessions for t in w["turns"] if t["status"] == "interrupted")

    def _is_expected_null(a):
        return a.get("kind") == "session" and a.get("turnStatus") in ("cancelled", "interrupted")

    journaled = len(agents)
    null_results = sum(1 for a in agents if a.get("result") is None and not _is_expected_null(a))
    completed = journaled - sum(1 for a in agents if a.get("result") is None)
    with_tokens = sum(1 for a in agents if isinstance(a.get("tokens"), (int, float)))
    with_ms = sum(1 for a in agents if isinstance(a.get("ms"), (int, float)))
    total_tokens = sum(a.get("tokens") or 0 for a in agents)
    total_agent_ms = sum(a.get("ms") or 0 for a in agents)

    cached_agents = None
    interrupted_agents = None
    run_wall_ms = None
    executed_this_run = None
    executed_tokens = None

    if events:
        ls = live_state(events)
        cached_agents = sum(1 for e in events if e.get("type") == "cached" and e.get("kind") != "human")
        executed_this_run = sum(1 for e in events if e.get("type") == "start")
        interrupted_agents = len(ls["running"]) if ls else 0
        if ls and ls.get("runStartedAt") and ls.get("lastEventAt"):
            run_wall_ms = int(ls["lastEventAt"] * 1000 - ls["runStartedAt"] * 1000)
        ended_ids = {e.get("id") or e.get("label") for e in events if e.get("type") == "end"}
        if ended_ids:
            executed_tokens = sum(a.get("tokens") or 0 for a in agents
                                  if (a.get("id") in ended_ids or a.get("label") in ended_ids))

    interrupted = interrupted_agents or 0
    total_agents = journaled + interrupted

    # by phase
    phase_titles = [p["title"] for p in run.get("phases", [])]
    for a in agents:
        if a.get("phase") and a["phase"] not in phase_titles:
            phase_titles.append(a["phase"])

    by_phase = []
    for ph in phase_titles:
        in_phase = [a for a in agents if a.get("phase") == ph]
        if in_phase:
            by_phase.append({
                "phase": ph, "agents": len(in_phase),
                "tokens": sum(a.get("tokens") or 0 for a in in_phase),
                "agentMs": sum(a.get("ms") or 0 for a in in_phase),
            })

    # by model
    model_map: dict[str, dict] = {}
    for a in agents:
        k = a.get("model") or "(unspecified)"
        m = model_map.get(k, {"model": k, "agents": 0, "tokens": 0})
        m["agents"] += 1
        m["tokens"] += a.get("tokens") or 0
        model_map[k] = m
    by_model = sorted(model_map.values(), key=lambda x: (-x["agents"], -x["tokens"]))

    # by effort
    effort_map: dict[str, dict] = {}
    for a in agents:
        k = a.get("effort") or "default"
        e = effort_map.get(k, {"effort": k, "agents": 0, "tokens": 0})
        e["agents"] += 1
        e["tokens"] += a.get("tokens") or 0
        effort_map[k] = e
    by_effort = sorted(effort_map.values(), key=lambda x: -x["agents"])

    top_by_tokens = sorted([a for a in agents if isinstance(a.get("tokens"), (int, float))],
                           key=lambda x: -(x["tokens"] or 0))[:10]
    top_by_ms = sorted([a for a in agents if isinstance(a.get("ms"), (int, float))],
                       key=lambda x: -(x["ms"] or 0))[:10]

    budget = None
    if meta and isinstance(meta.get("budget"), (int, float)) and meta["budget"] > 0:
        have_executed = events and executed_tokens is not None
        spent = executed_tokens if have_executed else total_tokens
        budget = {
            "total": meta["budget"], "meter": meta.get("budgetMeter", "total"),
            "spent": spent, "basis": "latest-run" if have_executed else "all-in-journal",
            "allInTokens": total_tokens,
            "remaining": max(0, meta["budget"] - spent),
            "fraction": spent / meta["budget"] if meta["budget"] else 0,
        }

    cache = None
    if events and cached_agents and cached_agents > 0:
        touched = cached_agents + (executed_this_run or 0)
        cache = {"cached": cached_agents, "executed": executed_this_run or 0,
                 "touched": touched, "fraction": cached_agents / touched if touched else 0}

    warnings = []
    if journaled == 0:
        warnings.append({"code": "empty-run", "level": "warn", "message": "No completed agents found in the journal."})
    if journaled > 0 and with_tokens < journaled:
        missing = journaled - with_tokens
        level = "warn" if with_tokens == 0 else "info"
        warnings.append({"code": "missing-metrics", "level": level,
                         "message": f"{missing} of {journaled} agents have no token metrics — totals are a lower bound."})
    if null_results >= 3:
        warnings.append({"code": "many-null-results", "level": "warn",
                         "message": f"{null_results} of {journaled} agents returned a null result."})
    if interrupted > 0:
        warnings.append({"code": "interrupted-agents", "level": "warn",
                         "message": f"{interrupted} agent(s) started but never finished in the most recent run."})

    summary = {
        "name": run["name"], "description": run.get("description", ""),
        "sources": {"journal": journal_path, "script": run["sources"].get("script"),
                     "runDir": run["sources"].get("runDir") or run_dir,
                     "events": bool(events), "result": run.get("result") is not None, "meta": bool(meta)},
        "counts": {
            "totalAgents": total_agents, "journaledAgents": journaled,
            "completedAgents": completed, "nullResults": null_results,
            "cachedAgents": cached_agents, "interruptedAgents": interrupted_agents,
            "phases": len(by_phase), "sessionWorkers": len(sessions),
            "sessionTurns": session_turns, "steerTurns": steer_turns,
            "cancelledTurns": cancelled_turns, "failedTurns": failed_turns,
            "interruptedTurns": interrupted_turns,
        },
        "sessions": [{"id": w["id"], "label": w["label"], "phase": w.get("phase"),
                       "model": w.get("model"), "turns": len(w["turns"]),
                       "tokens": w.get("tokens"), "ms": w.get("ms"), "status": w["status"]}
                      for w in sessions],
        "metrics": {
            "hasMetrics": bool(run.get("totals", {}).get("has_metrics")),
            "agentsWithTokens": with_tokens, "agentsWithMs": with_ms,
            "totalTokens": total_tokens, "executedTokens": executed_tokens,
            "totalAgentMs": total_agent_ms, "runWallMs": run_wall_ms,
        },
        "budget": budget,
        "byPhase": by_phase, "byModel": by_model, "byEffort": by_effort,
        "topByTokens": [{"label": a["label"], "phase": a.get("phase"),
                         "model": a.get("model"), "tokens": a["tokens"]} for a in top_by_tokens],
        "topByMs": [{"label": a["label"], "phase": a.get("phase"),
                     "model": a.get("model"), "ms": a["ms"]} for a in top_by_ms],
        "cache": cache, "warnings": warnings,
    }
    if include_result and run.get("result") is not None:
        summary["result"] = run["result"]
    return summary


def render_end_of_run(s: dict) -> str:
    """Compact end-of-run block. 1:1 port of upstream renderEndOfRun()."""
    c = s["counts"]
    if c["journaledAgents"] == 0:
        return ""
    m = s["metrics"]
    totals = [x for x in [
        f"{c['journaledAgents']} agent{'s' if c['journaledAgents'] != 1 else ''}",
        f"{c['sessionWorkers']} worker{'s' if c['sessionWorkers'] != 1 else ''}" if c["sessionWorkers"] else None,
        f"{c['phases']} phases" if c["phases"] > 1 else None,
        f"{_fmt_tokens(m['totalTokens'])} tok" if m.get("totalTokens") else None,
        _fmt_ms(m["runWallMs"]) if m.get("runWallMs") else (_fmt_ms(m["totalAgentMs"]) + " agent-time" if m.get("totalAgentMs") else None),
    ] if x]
    warns = [w for w in s.get("warnings", []) if w["level"] == "warn"]
    lines = [f"Σ {' · '.join(totals)}"]
    if warns:
        lines.append(f"  ⚠ {len(warns)} warning{'s' if len(warns) != 1 else ''} — see the full report")
    return "\n".join(lines)
