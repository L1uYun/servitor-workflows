"""Map a model id requested by a workflow onto a model servitor actually exposes.

1:1 Python port of runner/src/modelMap.js. The JS version maps Claude ids/aliases
to Codex models; here we map Claude ids/aliases to servitor provider models.
"""
from __future__ import annotations

from typing import Any

# Claude tier -> ordered servitor preferences (first available wins).
_FAMILY_PREFERENCES: dict[str, list[str]] = {
    "opus": ["gpt-5.5", "gpt-5.4", "gpt-5.3-codex", "gpt-5.2"],
    "sonnet": ["gpt-5.4", "gpt-5.5", "gpt-5.3-codex", "gpt-5.4-mini"],
    "haiku": ["gpt-5.4-mini", "gpt-5.4", "gpt-5.2"],
}


def model_id(m: Any) -> str | None:
    """Extract the id string from a model object or pass through a string."""
    if isinstance(m, str):
        return m
    if m and isinstance(m, dict):
        return m.get("id") or m.get("slug") or m.get("model") or m.get("name")
    return None


def _claude_family(id_str: str) -> str | None:
    """Match Claude full ids ('claude-opus-4-8') and bare aliases ('opus')."""
    s = str(id_str).lower()
    if "opus" in s:
        return "opus"
    if "sonnet" in s:
        return "sonnet"
    if "haiku" in s:
        return "haiku"
    return None


def resolve_model(requested: str | None, available: list[str] | None = None,
                  log=None) -> str | None:
    """Resolve `requested` to a servitor model id (or None to use config default).

    undefined / "inherit" / "default" -> None
    Claude id or alias -> mapped family preference (best available)
    already-available id -> as-is
    unknown but unavailable -> None (config default) + warn
    """
    if not requested or str(requested).lower() in ("inherit", "default"):
        return None
    available = available or []
    family = _claude_family(requested)
    if family:
        prefs = _FAMILY_PREFERENCES.get(family, [])
        if available:
            pick = next((m for m in prefs if m in available), None)
            if pick is None:
                pick = next((m for m in available if "mini" not in m and "spark" not in m), None)
                if pick is None:
                    pick = available[0]
        else:
            pick = prefs[0] if prefs else None
        if pick:
            if log:
                log(f"model: '{requested}' (Claude) -> '{pick}'")
            return pick
        return None
    if not available:
        return requested  # non-Claude id, can't validate — trust it
    if requested in available:
        return requested
    if log:
        log(f"model: '{requested}' not exposed by servitor -> using config default (have: {', '.join(available)})")
    return None


def pick_frontier(models: list[Any] | None = None) -> str | None:
    """Pick the latest frontier model: newest, strongest, non-mini/spark, non-hidden."""
    if not models:
        return None
    def _ver(s: str) -> float:
        import re
        mt = re.search(r"(\d+(?:\.\d+)?)", str(s))
        return float(mt.group(1)) if mt else -1.0

    eligible = []
    for m in models:
        mid = model_id(m)
        if not mid:
            continue
        is_default = isinstance(m, dict) and bool(m.get("isDefault"))
        hidden = isinstance(m, dict) and bool(m.get("hidden"))
        if hidden or "mini" in mid.lower() or "spark" in mid.lower():
            continue
        eligible.append({"id": mid, "is_default": is_default})

    if not eligible:
        return None
    eligible.sort(key=lambda x: (-_ver(x["id"]), -int(x["is_default"]), len(x["id"])))
    return eligible[0]["id"]
