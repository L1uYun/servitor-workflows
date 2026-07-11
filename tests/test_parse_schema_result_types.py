"""parse_schema_result accepts already-parsed dict from apply_output_contract."""
from __future__ import annotations

from servitor_workflows.servitor_agent import parse_schema_result


def test_parse_schema_result_accepts_dict():
    schema = {
        "type": "object",
        "properties": {"skill": {"type": "string"}},
        "required": ["skill"],
        "additionalProperties": True,
    }
    val = {"skill": "x", "verdict": "KEEP"}
    assert parse_schema_result(val, schema) is val


def test_parse_schema_result_parses_string():
    schema = {"type": "object", "properties": {"a": {"type": "number"}}}
    assert parse_schema_result('{"a":1}', schema) == {"a": 1}


def test_parse_schema_result_passthrough_without_schema():
    assert parse_schema_result({"a": 1}, None) == {"a": 1}
