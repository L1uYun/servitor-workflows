import sys
from pathlib import Path

import pytest

from servitor_workflows.terminal import run_git
from servitor_workflows.journal import Journal
from servitor_workflows.runtime import create_runtime
from servitor_workflows.worktree import create_worktree


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
