"""Structured output: control/analysis separation with JSON Schema validation.

Inspired by pi-workflow's <control>/<analysis> split, adapted for the Python DSL.

When a workflow passes output=StructuredOutput(control_schema=...) to agent(),
the runtime:
1. Instructs the model to emit a <control> JSON block and <analysis> prose.
2. Parses the control block from raw text.
3. Validates it against control_schema (via servitor's apply_output_contract).
4. Returns {control: <parsed>, analysis: <prose>} on success.
5. Raises StructuredOutputError on parse or validation failure (classified, retryable).

Without output=, agent() behavior is unchanged (backward compatible).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class StructuredOutput:
    """Declare structured control/analysis output for an agent() call.

    control_schema: JSON Schema dict for the <control> block.
    analysis: if True, the model also emits <analysis> prose, returned alongside control.
    control_key: tag name wrapping the JSON block (default "control").
    analysis_key: tag name wrapping the prose block (default "analysis").
    """
    control_schema: dict
    analysis: bool = True
    control_tag: str = "control"
    analysis_tag: str = "analysis"

    def schema_fingerprint(self) -> str:
        """Stable identity fingerprint for journal hashing.

        Includes the schema content and config so that changing the schema
        invalidates cached journal entries.
        """
        import hashlib
        blob = json.dumps({
            "schema": self.control_schema,
            "analysis": self.analysis,
            "control_tag": self.control_tag,
            "analysis_tag": self.analysis_tag,
        }, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]

    def instruction_text(self) -> str:
        """Prompt instruction appended to the agent prompt to elicit control/analysis."""
        lines = [
            f"\n\n--- Output format ---",
            f"Put machine-readable JSON inside <{self.control_tag}>...</{self.control_tag}>.",
        ]
        if self.analysis:
            lines.append(f"Put your reasoning and explanations inside <{self.analysis_tag}>...</{self.analysis_tag}>.")
        lines.append(f"The <{self.control_tag}> block must be valid JSON matching this schema:")
        lines.append(json.dumps(self.control_schema, ensure_ascii=False, indent=2))
        lines.append("Do not put JSON outside the <{0}> block.".format(self.control_tag))
        if self.analysis:
            lines.append("Do not put explanatory prose outside the <{0}> block.".format(self.analysis_tag))
        return "\n".join(lines)


class StructuredOutputError(RuntimeError):
    """Classified failure from structured output parsing or validation.

    Attributes:
        failure_reason: one of 'missing_control', 'invalid_control_json',
                        'schema_validation_failed', 'missing_analysis'
        raw: the raw model output text (for debugging/journal)
        schema_errors: list of validation errors (if applicable)
    """

    def __init__(self, failure_reason: str, raw: str | None = None, schema_errors: list | None = None):
        self.failure_reason = failure_reason
        self.raw = raw
        self.schema_errors = schema_errors or []
        super().__init__(f"structured output failed: {failure_reason}")

    @property
    def codex_error_info(self) -> str:
        """Compatibility with retry classification in servitor_agent."""
        return self.failure_reason


def parse_control_analysis(
    text: str,
    output: StructuredOutput,
) -> dict[str, Any]:
    """Parse raw model output into {control, analysis}.

    Extracts <control>...</control> JSON and optionally <analysis>...</analysis>
    prose from the raw text. Does NOT validate the control against the schema;
    that is servitor's apply_output_contract job. This function only does
    structural extraction.

    Raises StructuredOutputError if control block is missing or not valid JSON,
    or if analysis is required but missing.
    """
    if not isinstance(text, str):
        # If servitor already parsed to dict (via expect_json), wrap it.
        if isinstance(text, dict):
            return {"control": text, "analysis": ""}
        raise StructuredOutputError("invalid_control_json", raw=str(text))

    ct = output.control_tag
    at = output.analysis_tag

    # Extract control block
    control_pattern = re.compile(
        rf"<{re.escape(ct)}>\s*(.*?)\s*</{re.escape(ct)}>",
        re.DOTALL | re.IGNORECASE,
    )
    control_match = control_pattern.search(text)
    if not control_match:
        # Fallback: try to find a JSON object in the text
        json_candidate = _extract_json_object(text)
        if json_candidate is not None:
            control = json_candidate
        else:
            raise StructuredOutputError("missing_control", raw=text)
    else:
        control_str = control_match.group(1).strip()
        try:
            control = json.loads(control_str)
        except json.JSONDecodeError:
            # Try fenced JSON within the control block
            fenced = _extract_json_object(control_str)
            if fenced is not None:
                control = fenced
            else:
                raise StructuredOutputError("invalid_control_json", raw=text)

    # Extract analysis block (optional)
    analysis_text = ""
    if output.analysis:
        analysis_pattern = re.compile(
            rf"<{re.escape(at)}>\s*(.*?)\s*</{re.escape(at)}>",
            re.DOTALL | re.IGNORECASE,
        )
        analysis_match = analysis_pattern.search(text)
        if analysis_match:
            analysis_text = analysis_match.group(1).strip()
        else:
            # Analysis is optional in practice: some models may not emit the tag.
            # Collect all text outside <control> as analysis fallback.
            stripped = control_pattern.sub("", text).strip()
            if stripped:
                analysis_text = stripped

    return {"control": control, "analysis": analysis_text}


def _extract_json_object(text: str) -> Any:
    """Try to find and parse a JSON object in text."""
    if not text:
        return None
    # Try fenced ```json ... ```
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    candidate = fenced.group(1) if fenced else text
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(candidate[start:end + 1])
        except json.JSONDecodeError:
            pass
    return None


def schema_skeleton(output: StructuredOutput) -> dict[str, Any]:
    """Build a minimal value satisfying control_schema for --plan dry runs."""
    return {
        "control": _skel(output.control_schema),
        "analysis": "" if output.analysis else None,
    }


def _skel(s: dict | None) -> Any:
    """Build a minimal value satisfying a JSON Schema."""
    if not s or not isinstance(s, dict):
        return ""
    enum = s.get("enum")
    if isinstance(enum, list) and enum:
        return enum[0]
    if s.get("oneOf") or s.get("anyOf"):
        return _skel((s.get("oneOf") or s.get("anyOf"))[0])
    t = s.get("type")
    if isinstance(t, list):
        t = t[0] if t else None
    if t == "object" or (not t and s.get("properties")):
        o = {}
        for k in (s.get("properties") or {}):
            o[k] = _skel(s["properties"][k])
        return o
    if t == "array":
        return []
    if t in ("number", "integer"):
        return 0
    if t == "boolean":
        return False
    if t == "string":
        return ""
    return None
