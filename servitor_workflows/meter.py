"""Token accounting, fed by per-agent metrics callbacks.

1:1 Python port of runner/src/meter.js. The JS version subscribes to
`thread/tokenUsage/updated` notifications; here it is fed via record_token_usage()
called by the transport seam when servitor metadata exposes usage.
"""
from __future__ import annotations

from typing import Any

# threadId -> {input, output, reasoning, total}
_per_thread: dict[str, dict[str, int]] = {}

# Threads re-attached via resume may report cumulative totals including prior-run
# history. On such a thread's first notification this process, capture the baseline
# and subtract it so the meter only counts THIS process's spend.
_resumed_baselines: dict[str, dict[str, int]] = {}
_pending_resumed: set[str] = set()


def _normalize(b: Any) -> dict[str, int] | None:
    """Normalize a TokenUsageBreakdown into {input, output, reasoning, total}."""
    if not b or not isinstance(b, dict):
        return None
    input_t = b.get("inputTokens", 0) or 0
    output_t = b.get("outputTokens", 0) or 0
    reasoning_t = b.get("reasoningOutputTokens", 0) or 0
    total = b.get("totalTokens")
    if not isinstance(total, (int, float)):
        total = input_t + output_t + reasoning_t
    return {"input": input_t, "output": output_t, "reasoning": reasoning_t, "total": total}


def mark_resumed_thread(thread_id: str | None):
    """Mark a thread as resumed so its first usage notification sets a baseline."""
    if thread_id and thread_id not in _per_thread:
        _pending_resumed.add(thread_id)


def record_token_usage(params: dict):
    """Record a token usage notification for a thread."""
    thread_id = params.get("threadId")
    n = _normalize(params.get("tokenUsage", {}).get("total") if isinstance(params.get("tokenUsage"), dict) else None)
    if not thread_id or not n:
        return
    if thread_id in _pending_resumed:
        _pending_resumed.discard(thread_id)
        last = _normalize(
            params.get("tokenUsage", {}).get("last") if isinstance(params.get("tokenUsage"), dict) else None
        ) or {"input": 0, "output": 0, "reasoning": 0, "total": 0}
        _resumed_baselines[thread_id] = {
            "input": max(0, n["input"] - last["input"]),
            "output": max(0, n["output"] - last["output"]),
            "reasoning": max(0, n["reasoning"] - last["reasoning"]),
            "total": max(0, n["total"] - last["total"]),
        }
    base = _resumed_baselines.get(thread_id)
    if base:
        _per_thread[thread_id] = {
            "input": max(0, n["input"] - base["input"]),
            "output": max(0, n["output"] - base["output"]),
            "reasoning": max(0, n["reasoning"] - base["reasoning"]),
            "total": max(0, n["total"] - base["total"]),
        }
    else:
        _per_thread[thread_id] = n


def tokens_spent() -> int:
    """Total tokens across all threads (input + output + reasoning)."""
    return sum(v["total"] for v in _per_thread.values())


def output_spent() -> int:
    """Output-only tokens (generated + reasoning) across all threads."""
    return sum(v["output"] + v["reasoning"] for v in _per_thread.values())


def tokens_for_thread(thread_id: str) -> dict[str, int] | None:
    """Per-agent attribution: the cumulative breakdown for one thread, or None."""
    return _per_thread.get(thread_id)


def reset_meter():
    """Reset all meter state."""
    _per_thread.clear()
    _resumed_baselines.clear()
    _pending_resumed.clear()
