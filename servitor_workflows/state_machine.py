"""Persistent workflow state and approval gates for controller-owned delivery.

The state machine is intentionally small: it records the task identity, phase,
artifacts, retry/failure evidence, semantic evaluator verdicts, and human
approval state that decide whether a workflow may deliver or run destructive
cleanup. It is a JSON sidecar helper, not a background service.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
import json
from pathlib import Path
from typing import Any

PHASES = {
    "planned",
    "executing",
    "verifying",
    "awaiting_approval",
    "approved",
    "rejected",
    "delivering",
    "cleaning",
    "done",
    "failed",
}
TERMINAL_PHASES = {"done", "failed", "rejected"}
APPROVAL_STATES = {"pending", "confirmed", "rejected"}
PASS_VERDICTS = {"pass", "approve", "approved", "ok"}
FAIL_VERDICTS = {"fail", "reject", "rejected", "block", "blocked"}


@dataclass
class WorkflowStateMachine:
    task_id: str
    phase: str = "planned"
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    retries: int = 0
    approval_state: str = "pending"
    approval_source: str | None = None
    approval_reason: str | None = None
    evaluator: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.task_id:
            raise ValueError("task_id is required")
        if self.phase not in PHASES:
            raise ValueError(f"invalid phase: {self.phase}")
        if self.approval_state not in APPROVAL_STATES:
            raise ValueError(f"invalid approval_state: {self.approval_state}")
        if self.approval_source is not None and self.approval_source not in {"human", "evaluator"}:
            raise ValueError(f"invalid approval_source: {self.approval_source}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowStateMachine":
        return cls(**data)

    @classmethod
    def load(cls, path: str | Path) -> "WorkflowStateMachine":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state sidecar must contain a JSON object")
        return cls.from_dict(data)

    def save(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def advance(self, phase: str) -> None:
        if phase not in PHASES:
            raise ValueError(f"invalid phase: {phase}")
        if self.phase in TERMINAL_PHASES:
            raise RuntimeError(f"cannot advance terminal phase {self.phase}")
        if phase in {"delivering", "cleaning", "done"}:
            self.require_approved()
        self.phase = phase

    def add_artifact(self, *, kind: str, path: str | None = None, evidence: str | None = None, metadata: dict[str, Any] | None = None) -> None:
        if not kind:
            raise ValueError("artifact kind is required")
        self.artifacts.append({"kind": kind, "path": path, "evidence": evidence, "metadata": metadata or {}})

    def record_failure(self, *, phase: str, reason: str, retryable: bool = False, evidence: str | None = None) -> None:
        if not reason:
            raise ValueError("failure reason is required")
        self.failures.append({"phase": phase, "reason": reason, "retryable": retryable, "evidence": evidence})
        if retryable:
            self.retries += 1
        self.phase = "failed" if not retryable else self.phase

    def apply_human_answer(self, answer: str, *, reason: str | None = None) -> str:
        normalized = str(answer or "").strip().lower()
        if normalized in {"confirm", "confirmed", "approve", "approved", "yes", "y"}:
            self.approval_state = "confirmed"
            self.approval_source = "human"
            self.phase = "approved"
        elif normalized in {"reject", "rejected", "no", "n"}:
            self.approval_state = "rejected"
            self.approval_source = "human"
            self.phase = "rejected"
        else:
            self.approval_state = "pending"
            self.approval_source = None
            self.phase = "awaiting_approval"
        self.approval_reason = reason
        return self.approval_state

    def apply_evaluator_result(self, result: dict[str, Any], *, require_understanding: bool = True) -> str:
        """Apply a semantic evaluator result, not just JSON shape.

        Accepted shapes are either {"control": {...}, "analysis": "..."} from
        StructuredOutput or a flat control object. The control object must carry a
        verdict and, when requested, an understanding/evidence field so a regex or
        empty JSON skeleton cannot approve delivery.
        """
        if not isinstance(result, dict):
            raise ValueError("evaluator result must be a dict")
        control = result.get("control") if isinstance(result.get("control"), dict) else result
        verdict = str(control.get("verdict", "")).strip().lower()
        understanding = control.get("understanding") or control.get("evidence") or control.get("reason")
        if require_understanding and not str(understanding or "").strip():
            raise RuntimeError("semantic evaluator result lacks understanding/evidence")
        self.evaluator = {"control": dict(control), "analysis": result.get("analysis")}
        if verdict in PASS_VERDICTS:
            self.approval_state = "confirmed"
            self.approval_source = "evaluator"
            self.phase = "approved"
        elif verdict in FAIL_VERDICTS:
            self.approval_state = "rejected"
            self.approval_source = "evaluator"
            self.phase = "rejected"
        else:
            self.approval_state = "pending"
            self.approval_source = None
            self.phase = "awaiting_approval"
        return self.approval_state

    def require_approved(self) -> None:
        if self.approval_state != "confirmed" or self.approval_source not in {"human", "evaluator"}:
            raise RuntimeError("trusted human/evaluator approval is required before delivery or destructive cleanup")
        if self.approval_source == "evaluator" and not self.evaluator:
            raise RuntimeError("evaluator approval requires stored semantic evaluator evidence")
        if self.approval_source == "human" and not str(self.approval_reason or "").strip():
            raise RuntimeError("human approval requires recorded reason/evidence")
