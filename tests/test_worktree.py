import sys
from pathlib import Path

import pytest

from servitor_workflows.terminal import run_git
from servitor_workflows.journal import Journal
from servitor_workflows.runtime import create_runtime
from servitor_workflows.worktree import build_closure_packet, create_worktree, delivery_cleanup_plan


async def _init_repo(path: Path) -> None:
    await run_git(str(path), ["init"])
    await run_git(str(path), ["config", "user.email", "tests@example.invalid"])
    await run_git(str(path), ["config", "user.name", "Workflow Tests"])
    (path / "README.md").write_text("baseline\n", encoding="utf-8")
    await run_git(str(path), ["add", "README.md"])
    await run_git(str(path), ["commit", "-m", "baseline"])


@pytest.mark.asyncio
async def test_named_worktree_records_base_branch_and_verification(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    await _init_repo(repo)
    base = (await run_git(str(repo), ["rev-parse", "HEAD"])).strip()

    worktree = await create_worktree(str(repo), branch="codex/test-integration")
    verification = await worktree["verify"]([sys.executable, "-c", "print('verified')"])
    outcome = await worktree["cleanup"]()

    assert worktree["base_commit"] == base
    assert worktree["branch"] == "codex/test-integration"
    assert verification["exit_code"] == 0
    assert verification["stdout_tail"].strip() == "verified"
    assert verification["stderr_tail"] == ""
    assert outcome == {
        "removed": True,
        "dirty": False,
        "dir": worktree["dir"],
        "base_commit": base,
        "head_commit": base,
        "branch": "codex/test-integration",
    }
    assert (await run_git(str(repo), ["rev-parse", "codex/test-integration"])).strip() == base


@pytest.mark.asyncio
async def test_runtime_journals_integration_evidence(tmp_path: Path):
    journal = Journal(tmp_path / "integration.jsonl")

    async def fake_agent(_prompt, opts):
        opts["on_integration"]({
            "branch": "codex/test-integration",
            "base_commit": "base",
            "head_commit": "head",
            "dirty": False,
            "removed": True,
        })
        return "done"

    runtime = create_runtime(run_agent=fake_agent, journal=journal, default_agent="pi")
    assert await runtime["agent"]("write", {"label": "write"}) == "done"
    assert journal.entries()[0]["integration"]["head_commit"] == "head"


@pytest.mark.asyncio
async def test_worktree_delivery_dry_run_records_push_and_draft_pr(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    await _init_repo(repo)

    worktree = await create_worktree(str(repo), branch="codex/delivery")
    delivery = await worktree["deliver"]({
        "dry_run": True,
        "remote": "origin",
        "pr": {"create": True, "draft": True, "fill": True, "base": "main"},
    })
    outcome = await worktree["cleanup"]()

    assert delivery["ok"] is True
    assert delivery["branch"] == "codex/delivery"
    assert [c["command"] for c in delivery["commands"]] == [
        "git push -u origin codex/delivery",
        "gh pr create --draft --fill --base main",
    ]
    assert all(c["skipped"] == "dry_run" for c in delivery["commands"])
    assert outcome["removed"] is True


@pytest.mark.asyncio
async def test_delivery_cleanup_plan_is_dry_run_until_approved(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    await _init_repo(repo)
    await run_git(str(repo), ["branch", "codex/merged-task"])

    plan = await delivery_cleanup_plan(str(repo), branch="codex/merged-task", include_remote=True)

    assert plan["local_merged"] is True
    assert plan["can_execute"] is False
    assert plan["commands"] == [
        "git branch -d codex/merged-task",
        "git push origin --delete codex/merged-task",
    ]



def test_closure_packet_records_verify_delivery_and_cleanup_gate():
    packet = build_closure_packet(integration={
        "branch": "codex/task",
        "base_commit": "base",
        "head_commit": "head",
        "dirty": False,
        "dir": "D:/tmp/wt",
        "removed": True,
        "verification": {"command": "python -m pytest -q", "exit_code": 0},
        "delivery": {"ok": True, "dry_run": True, "commands": [{"command": "git push -u origin codex/task"}]},
    })

    assert packet["kind"] == "neat_closure_packet"
    assert packet["branch"] == "codex/task"
    assert packet["verification"]["exit_code"] == 0
    assert packet["delivery"]["commands"] == ["git push -u origin codex/task"]
    assert packet["cleanup"] == "dry_run_required_before_branch_delete"
    assert packet["inherited_dirty_state"] == "classify_and_preserve"
    assert packet["next_gate"] == "controller_review"
