import pytest

from servitor_workflows.state_machine import WorkflowStateMachine


def test_state_machine_requires_semantic_evidence_before_approval():
    state = WorkflowStateMachine(task_id="task-1")

    with pytest.raises(RuntimeError, match="lacks understanding"):
        state.apply_evaluator_result({"control": {"verdict": "pass"}})

    assert state.approval_state == "pending"
    state.apply_evaluator_result({"control": {"verdict": "pass", "understanding": "diff and verification inspected"}})
    assert state.approval_state == "confirmed"
    assert state.approval_source == "evaluator"
    state.advance("delivering")
    assert state.phase == "delivering"


def test_state_machine_blocks_cleanup_without_approval():
    state = WorkflowStateMachine(task_id="task-2")
    state.advance("executing")

    with pytest.raises(RuntimeError, match="approval is required"):
        state.advance("cleaning")

    state.apply_human_answer("approved", reason="user confirmed cleanup")
    assert state.approval_source == "human"
    state.advance("cleaning")
    assert state.phase == "cleaning"


def test_state_machine_rejects_edited_confirmed_state_without_trusted_source():
    state = WorkflowStateMachine(task_id="task-3", approval_state="confirmed")

    with pytest.raises(RuntimeError, match="trusted human/evaluator approval"):
        state.advance("delivering")
