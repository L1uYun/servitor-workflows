"""Render a run model as an ASCII execution DAG.

1:1 Python port of runner/src/asciiMap.js. Pure (returns a string).
"""
from __future__ import annotations

from typing import Any


def _fmt_tokens(n: int | None) -> str | None:
    if n is None:
        return None
    if n >= 1e6:
        return f"{n/1e6:.{'0' if n >= 1e7 else '1'}f}M"
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


def _summarize_value(r: Any) -> str:
    """One-line summary of a result value."""
    if r is None:
        return ""
    if isinstance(r, str):
        return " ".join(r.split())
    if not isinstance(r, dict):
        return str(r)
    for k in ("recommended_direction", "recommendation", "one_line_verdict", "tagline",
              "headline", "summary", "answer", "reason"):
        v = r.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for v in r.values():
        if isinstance(v, str) and len(v) > 8:
            return v.strip()
    return ""


def render_map(run: dict, *, width: int = 80, max_agents: int = 12, snippets: bool = True) -> str:
    """Render a run model as an ASCII execution DAG.

    1:1 port of upstream renderMap(). Returns a string.
    """
    lines: list[str] = []
    agents = run.get("agents", [])
    phases = [p for p in run.get("phases", []) if any(a.get("phase") == p["title"] for a in agents)]
    has_metrics = run.get("totals", {}).get("has_metrics", False)

    # Orchestrator node
    name = run.get("name", "?")
    n_workers = len(run.get("sessions", []))
    totals = " · ".join(x for x in [
        f"{len(phases)} phase{'s' if len(phases) != 1 else ''}",
        f"{n_workers} worker{'s' if n_workers != 1 else ''}" if n_workers else None,
        f"{_fmt_tokens(run.get('totals', {}).get('tokens'))} tok" if has_metrics and run.get("totals", {}).get("tokens") else None,
    ] if x)
    lines.append(f"┌─ ◆ {name} {'─' * max(0, width - len(name) - 6)}┐")
    if totals:
        lines.append(f"│ {totals}")
    lines.append(f"└{'─' * (width - 2)}┘")

    if not agents:
        lines.append("  │")
        lines.append("  ▼  (no agents yet — waiting for the first to start…)")
        return "\n".join(lines)

    lines.append("  │")

    for pi, p in enumerate(phases):
        title = p["title"]
        p_agents = [a for a in agents if a.get("phase") == title]
        n_done = sum(1 for a in p_agents if a.get("status") != "running")
        n_run = len(p_agents) - n_done
        pmeta = " · ".join(x for x in [
            f"{n_done} done · {n_run} running" if n_run else f"{len(p_agents)} agent{'s' if len(p_agents) != 1 else ''}",
            f"{_fmt_tokens(sum(a.get('tokens') or 0 for a in p_agents))} tok" if has_metrics and any(a.get("tokens") for a in p_agents) else None,
        ] if x)

        lead = f"▼ {pi + 1} {title} "
        rule_len = max(2, width - len(lead) - len(pmeta) - 4)
        lines.append(f"  {lead}{'─' * rule_len}  {pmeta}")

        shown = p_agents[:max_agents]
        for i, a in enumerate(shown):
            last = (i == len(shown) - 1) and (len(p_agents) <= max_agents)
            conn = "╰─" if last else "├─"
            running = a.get("status") == "running"
            glyph = "⠿" if running else ("◑" if a.get("result") is None else "✓")
            label = a.get("label", "?")
            model = a.get("model") or ""
            effort = a.get("effort") or ""
            cells = [f"{label}"]
            if model:
                cells.append(model)
            if effort:
                cells.append(f"⟪{effort}⟫")
            if not running and has_metrics:
                tok = _fmt_tokens(a.get("tokens"))
                ms = _fmt_ms(a.get("ms"))
                if tok:
                    cells.append(tok)
                if ms:
                    cells.append(ms)
            lines.append(f"  {conn}{glyph} {'  '.join(cells)}")

            if snippets and not running and a.get("result"):
                snip = _summarize_value(a["result"])
                if snip:
                    rail = "      " if last else "  │   "
                    lines.append(f"{rail}{snip[:width - 8]}")

        if len(p_agents) > max_agents:
            lines.append(f"  ╰─ … +{len(p_agents) - max_agents} more")

        if pi < len(phases) - 1:
            next_title = phases[pi + 1]["title"]
            lines.append(f"  ┄ barrier · {title} → {next_title} {'┄' * max(2, width - 20)}┄")

    # Result node
    lines.append("  │")
    lines.append("  ▼")
    result = run.get("result")
    headline = _summarize_value(result) if result is not None else "(no result)"
    lines.append(f"┌─ ✦ result {'─' * max(0, width - 14)}┐")
    lines.append(f"│ {headline[:width - 4]}")
    lines.append(f"└{'─' * (width - 2)}┘")

    return "\n".join(lines)
