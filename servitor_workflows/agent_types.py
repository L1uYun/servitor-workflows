"""Resolve a workflow agentType to its system prompt + optional model.

1:1 Python port of runner/src/agentTypes.js. Reads agent definitions from
.claude/agents/<name>.md (project scope walks up from cwd, user scope is ~/.claude).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_cache: dict[str, Any] = {}


def _try_read(path: Path) -> dict | None:
    try:
        return {"path": str(path), "body": path.read_text(encoding="utf-8")}
    except (OSError, UnicodeDecodeError):
        return None


def _find_up(start_dir: Path, rel: str) -> dict | None:
    """Walk up from start_dir looking for rel, return first match."""
    d = start_dir.resolve()
    while True:
        found = _try_read(d / rel)
        if found:
            return found
        parent = d.parent
        if parent == d:
            return None
        d = parent


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    """Parse YAML frontmatter, return (meta, body)."""
    m = re.match(r"^---\r?\n([\s\S]*?)\r?\n---\r?\n?", text)
    if not m:
        return {}, text
    meta: dict[str, str] = {}
    for line in m.group(1).splitlines():
        kv = re.match(r"^([A-Za-z0-9_-]+):\s*(.*)$", line)
        if kv:
            val = kv.group(2).strip()
            val = re.sub(r'^["\']|["\']$', "", val)
            meta[kv.group(1).strip()] = val
    return meta, text[m.end():]


def load_agent_type(name: str | None, cwd: str | None = None) -> dict | None:
    """Return {system_prompt, model?, source} or None if the agentType is unknown.

    `model` is whatever the definition's frontmatter declares — pass it through
    resolve_model() before use.
    """
    if not name:
        return None
    cwd_str = str(cwd or Path.cwd())
    key = f"{cwd_str}::{name}"
    if key in _cache:
        return _cache[key]

    rel = str(Path(".claude") / "agents" / f"{name}.md")
    found = _find_up(Path(cwd_str), rel)
    if not found:
        user_agents = Path.home() / ".claude" / "agents" / f"{name}.md"
        found = _try_read(user_agents)

    result = None
    if found:
        meta, body = _parse_frontmatter(found["body"])
        result = {
            "system_prompt": body.strip(),
            "model": meta.get("model") or None,
            "source": found["path"],
        }
    _cache[key] = result
    return result
