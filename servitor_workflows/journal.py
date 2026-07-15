"""Resume journal: persist each completed agent() result keyed by a stable hash.

1:1 Python port of runner/src/journal.js. Identity = sha256(stable_stringify(
{prompt, model, effort, schema, system_prompt, native_args, role, agent}))[:16]
plus an occurrence index, so reruns can skip work that hasn't changed.

Session turns use `sess:<id>#<turn>` keys; human() answers use `human:<id>#<occ>`.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

# Only the inputs that determine the model's output participate in identity.
# Cosmetic opts (label, cwd, isolation, timeout) do NOT.
_IDENTITY_KEYS = ("model", "effort", "schema", "system_prompt", "native_args", "role", "agent", "output_schema")


def _stable_stringify(v: Any) -> str:
    """Deterministic JSON stringify with sorted keys (matches JS stableStringify)."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return json.dumps(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ",".join(_stable_stringify(x) for x in v) + "]"
    if isinstance(v, dict):
        keys = sorted(v.keys())
        return "{" + ",".join(
            json.dumps(k, ensure_ascii=False) + ":" + _stable_stringify(v[k]) for k in keys
        ) + "}"
    return json.dumps(v, ensure_ascii=False)


def identity_hash(prompt: str, opts: dict | None = None) -> str:
    """Compute a stable 16-char hex hash from prompt + output-affecting opts."""
    identity: dict[str, Any] = {"prompt": str(prompt)}
    if opts:
        for k in _IDENTITY_KEYS:
            if opts.get(k) is not None:
                identity[k] = opts[k]
    return hashlib.sha256(
        _stable_stringify(identity).encode("utf-8")
    ).hexdigest()[:16]


class Journal:
    """Append-only JSONL journal with stable-key cache lookup.

    1:1 port of the upstream Journal class.
    """

    def __init__(self, path: Path | str | None = None, *, reuse: bool = False):
        self.path = Path(path) if path else None
        self.reuse = reuse
        self._cache: dict[str, dict] = {}  # key -> full entry
        self._occ: dict[str, int] = {}     # base_hash -> next occurrence index
        self._loaded = False

    def load(self):
        """Load existing entries from the JSONL file into the in-memory cache."""
        if self._loaded:
            return
        self._loaded = True
        if not self.path or not self.path.exists():
            return
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if entry.get("key"):
                    self._cache[entry["key"]] = entry
            except json.JSONDecodeError:
                continue

    def next_key(self, prompt: str, opts: dict | None = None) -> str:
        """Allocate the stable key for the next agent() call with this identity.

        Must be called once per call, in deterministic order.
        The occurrence counter ensures that calling agent("same prompt") N times
        produces N distinct keys: base#0, base#1, base#2, ...
        """
        base = identity_hash(prompt, opts)
        n = self._occ.get(base, 0)
        self._occ[base] = n + 1
        return f"{base}#{n}"

    def hit(self, key: str) -> bool:
        """True if reuse mode is on AND this key has a cached entry."""
        return self.reuse and key in self._cache

    def get(self, key: str) -> Any:
        """Return the cached result value for key, or None."""
        entry = self._cache.get(key)
        return entry.get("result") if entry else None

    def entry(self, key: str) -> dict | None:
        """Return the full cached entry for key (with metadata), or None."""
        return self._cache.get(key)

    def record(self, key: str, label: str, result: Any, meta: dict | None = None):
        """Record a completed agent's result with metadata.

        Appends to the JSONL file and updates the in-memory cache.
        """
        entry: dict[str, Any] = {"key": key, "label": label, "result": result}
        if meta:
            for k, v in meta.items():
                if v is not None:
                    entry[k] = v
        self._cache[key] = entry
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def entries(self) -> list[dict]:
        """Return all entries in file order (for replay/inspection)."""
        self.load()
        if not self.path or not self.path.exists():
            return []
        result = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return result

    @property
    def count(self) -> int:
        """Number of unique keys in the journal."""
        self.load()
        return len(self._cache)
