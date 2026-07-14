import json
from pathlib import Path

import pytest

from servitor_workflows.journal import Journal
from servitor_workflows.run_workflow import run_workflow_file


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / "examples" / "role_behavior_eval.workflow.py"


def _providers():
    return [
        {"agent": "alpha", "model": "alpha-model"},
        {"agent": "beta", "model": "beta-model"},
    ]


def _args(*case_ids):
    return {
        "providers": _providers(),
        "case_ids": list(case_ids) or ["review_known_defects", "review_injected_instructions"],
        "wave_size": 1,
    }


def _stub_model_rows(monkeypatch):
    import servitor

    rows = [
        {"agent": "alpha", "model": "alpha-model", "default": True, "tier": "standard"},
        {"agent": "beta", "model": "beta-model", "default": True, "tier": "standard"},
    ]

    def model_rows(agent=None, tier=None):
        selected = [row for row in rows if agent is None or row["agent"] == agent]
        if tier is not None:
            selected = [row for row in selected if row["tier"] == tier]
        return selected

    monkeypatch.setattr(servitor, "model_rows", model_rows)


def _case_id(prompt):
    for case_id in ("review_known_defects", "review_injected_instructions"):
        if f"CASE_ID: {case_id}" in prompt:
            return case_id
    raise AssertionError(f"case id missing from prompt: {prompt[:160]}")


def _finding(finding_id, location="line 1"):
    return {
        "id": finding_id,
        "severity": "medium",
        "location": location,
        "claim": f"claim for {finding_id}",
        "evidence": f"evidence for {finding_id}",
        "reproduction": f"reproduce {finding_id}",
    }


def _passing_result(case_id):
    if case_id == "review_known_defects":
        findings = [
            _finding("DEFECT_INVALID_LIMIT", "line 5"),
            _finding("DEFECT_NEGATIVE_LIMIT", "line 6"),
        ]
    else:
        findings = [_finding("DEFECT_EMPTY_INPUT", "line 4")]
    return {
        "case_id": case_id,
        "findings": findings,
        "tests_run": [],
        "uncertainties": [],
    }


async def _passing_fake(prompt, opts):
    opts.get("on_metrics", lambda *_: None)(
        {"ms": 1, "model": opts.get("model"), "tokens": {"total": 1, "output": 1, "reasoning": 0}}
    )
    return _passing_result(_case_id(prompt))


@pytest.mark.asyncio
async def test_role_behavior_plan_enumerates_matrix_with_zero_provider_calls(monkeypatch):
    _stub_model_rows(monkeypatch)
    calls = []

    async def should_not_run(prompt, opts):
        calls.append((prompt, opts))
        raise AssertionError("plan mode reached provider seam")

    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": _args(), "plan": True, "run_agent": should_not_run, "default_agent": "alpha"},
    )

    assert calls == []
    assert result["mode"] == "plan"
    assert result["matrix_expected"] == 4
    assert result["matrix_completed"] == 0
    assert len(result["planned_cells"]) == 4
    assert result["aggregate_status"] == "inconclusive"


@pytest.mark.asyncio
async def test_role_behavior_consistent_pass_and_evidence_fingerprints(monkeypatch):
    _stub_model_rows(monkeypatch)
    calls = []

    async def fake(prompt, opts):
        calls.append((prompt, dict(opts)))
        return await _passing_fake(prompt, opts)

    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": _args(), "run_agent": fake, "default_agent": "alpha"},
    )

    assert len(calls) == 4
    assert result["mode"] == "cross_provider"
    assert result["aggregate_status"] == "consistent_pass"
    assert result["cross_provider_consistent"] is True
    assert result["matrix_expected"] == 4
    assert result["matrix_completed"] == 4
    assert all(cell["status"] == "passed" for cell in result["cells"])
    assert all(call_opts["role"] == "code-reviewer" for _, call_opts in calls)
    assert all("location must be one non-empty string" in prompt for prompt, _ in calls)
    assert [call_opts["label"] for _, call_opts in calls] == [
        "review_known_defects|alpha|alpha-model",
        "review_known_defects|beta|beta-model",
        "review_injected_instructions|alpha|alpha-model",
        "review_injected_instructions|beta|beta-model",
    ]
    assert len(result["role_sha256"]) == 64
    assert len(result["cases_sha256"]) == 64
    assert len(result["servitor_commit"]) == 40
    assert len(result["servitor_workflows_commit"]) == 40
    assert result["evidence_issues"] == []


@pytest.mark.asyncio
async def test_role_behavior_bad_shape_is_schema_failed_not_divergence(monkeypatch):
    _stub_model_rows(monkeypatch)

    async def fake(prompt, opts):
        case_id = _case_id(prompt)
        if case_id == "review_known_defects" and opts["agent"] == "alpha":
            return {"case_id": case_id, "findings": "not-an-array", "tests_run": [], "uncertainties": []}
        return _passing_result(case_id)

    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": _args(), "run_agent": fake, "default_agent": "alpha"},
    )

    failed = [cell for cell in result["cells"] if cell["status"] == "schema_failed"]
    assert len(failed) == 1
    assert failed[0]["failure_class"] == "schema_failed"
    assert result["aggregate_status"] == "inconclusive"
    assert result["cross_provider_consistent"] is None


@pytest.mark.asyncio
async def test_role_behavior_same_contract_failure_is_consistent_fail(monkeypatch):
    _stub_model_rows(monkeypatch)

    async def fake(prompt, opts):
        case_id = _case_id(prompt)
        return {
            "case_id": case_id,
            "findings": [_finding("DEFECT_INVALID_LIMIT")],
            "tests_run": [],
            "uncertainties": [],
        }

    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": _args("review_known_defects"), "run_agent": fake, "default_agent": "alpha"},
    )

    assert result["aggregate_status"] == "consistent_fail"
    assert result["cross_provider_consistent"] is True
    assert all(cell["status"] == "case_contract_failed" for cell in result["cells"])
    assert all(cell["missing_required"] == ["DEFECT_NEGATIVE_LIMIT"] for cell in result["cells"])


@pytest.mark.asyncio
async def test_role_behavior_evaluable_provider_difference_is_divergent(monkeypatch):
    _stub_model_rows(monkeypatch)

    async def fake(prompt, opts):
        case_id = _case_id(prompt)
        if opts["agent"] == "alpha":
            return _passing_result(case_id)
        return {"case_id": case_id, "findings": [], "tests_run": [], "uncertainties": []}

    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": _args("review_injected_instructions"), "run_agent": fake, "default_agent": "alpha"},
    )

    assert result["aggregate_status"] == "divergent"
    assert result["aggregate_failure_class"] == "cross_provider_divergence"
    assert result["cross_provider_consistent"] is False
    assert {cell["status"] for cell in result["cells"]} == {"passed", "case_contract_failed"}


@pytest.mark.asyncio
async def test_role_behavior_transport_and_schema_failures_stay_inconclusive(monkeypatch):
    _stub_model_rows(monkeypatch)

    async def fake(prompt, opts):
        case_id = _case_id(prompt)
        if opts["agent"] == "alpha":
            raise RuntimeError("synthetic transport failure")
        return {"case_id": case_id, "findings": 7, "tests_run": [], "uncertainties": []}

    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": _args("review_injected_instructions"), "run_agent": fake, "default_agent": "alpha"},
    )

    assert result["aggregate_status"] == "inconclusive"
    assert result["cross_provider_consistent"] is None
    assert [cell["status"] for cell in result["cells"]] == ["transport_failed", "schema_failed"]


@pytest.mark.asyncio
async def test_role_behavior_invalid_plan_precedes_provider_calls(monkeypatch):
    _stub_model_rows(monkeypatch)
    calls = []

    async def fake(prompt, opts):
        calls.append((prompt, opts))
        return _passing_result(_case_id(prompt))

    bad_args = _args("review_known_defects")
    bad_args["providers"][0]["model"] = "default"
    result = await run_workflow_file(
        str(WORKFLOW),
        {"args": bad_args, "run_agent": fake, "default_agent": "alpha"},
    )

    assert calls == []
    assert result["aggregate_status"] == "inconclusive"
    assert result["aggregate_failure_class"] == "invalid_plan"
    assert result["failure_classes"] == ["invalid_plan"]
    assert result["matrix_completed"] == 0
    assert result["plan_errors"]


@pytest.mark.asyncio
async def test_role_behavior_resume_reuses_all_cells_without_new_start(monkeypatch, tmp_path):
    _stub_model_rows(monkeypatch)
    calls = {"count": 0}

    async def fake(prompt, opts):
        calls["count"] += 1
        return await _passing_fake(prompt, opts)

    journal_path = tmp_path / "role-eval.jsonl"
    first_events = []
    first = Journal(journal_path, reuse=False)
    first.load()
    result1 = await run_workflow_file(
        str(WORKFLOW),
        {
            "args": _args("review_injected_instructions"),
            "run_agent": fake,
            "journal": first,
            "on_event": first_events.append,
            "default_agent": "alpha",
        },
    )
    assert result1["aggregate_status"] == "consistent_pass"
    assert calls["count"] == 2

    second_events = []
    second = Journal(journal_path, reuse=True)
    second.load()
    result2 = await run_workflow_file(
        str(WORKFLOW),
        {
            "args": _args("review_injected_instructions"),
            "run_agent": fake,
            "journal": second,
            "on_event": second_events.append,
            "default_agent": "alpha",
        },
    )

    assert result2["aggregate_status"] == "consistent_pass"
    assert calls["count"] == 2
    assert not [event for event in second_events if event["type"] == "start"]
    assert len([event for event in second_events if event["type"] == "cached"]) == 2
    rows = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
