"""Worktree isolation for agent() calls that mutate files in parallel.

1:1 Python port of runner/src/worktree.js. Creates a detached git worktree at HEAD
and runs the agent with its cwd pointed there. On completion the worktree is removed
only if unchanged; if the agent left changes, the worktree is kept.
"""
from __future__ import annotations

import asyncio
import shutil
import shlex
import tempfile
from pathlib import Path

from .terminal import run_command, run_git


async def is_git_repo(cwd: str) -> bool:
    """Check if cwd is inside a git work tree."""
    try:
        result = await run_git(cwd, ["rev-parse", "--is-inside-work-tree"])
        return result.strip() == "true"
    except Exception:
        return False


async def create_worktree(repo_cwd: str, *, branch: str | None = None) -> dict:
    """Create an isolated worktree at the current commit of ``repo_cwd``.

    A branch name makes the task result independently reviewable and mergeable.
    Without one, the legacy detached-worktree behavior is preserved.
    """
    root = (await run_git(repo_cwd, ["rev-parse", "--show-toplevel"])).strip()
    base_commit = (await run_git(root, ["rev-parse", "HEAD"])).strip()
    if branch:
        await run_git(root, ["check-ref-format", "--branch", branch])
    base = Path(tempfile.mkdtemp(prefix="wf-worktree-"))
    wt_dir = base / "wt"
    add_args = ["worktree", "add"]
    if branch:
        add_args.extend(["-b", branch])
    else:
        add_args.append("--detach")
    add_args.extend([str(wt_dir), base_commit])
    await run_git(root, add_args)

    async def verify(command: list[str] | None, timeout: float | None = None) -> dict | None:
        """Run a project verification command inside this worktree."""
        if command is None:
            return None
        if not command or any(not isinstance(part, str) or not part for part in command):
            raise ValueError("verify_command must be a non-empty list of strings")
        stdout, stderr, exit_code = await run_command(command, cwd=str(wt_dir), timeout=timeout)
        return {
            "command": shlex.join(command),
            "exit_code": exit_code,
            "stdout_tail": stdout[-2000:],
            "stderr_tail": stderr[-2000:],
        }

    async def cleanup() -> dict:
        dirty = False
        head_commit = base_commit
        try:
            status = await run_git(str(wt_dir), ["status", "--porcelain"])
            dirty = len(status) > 0
            head_commit = (await run_git(str(wt_dir), ["rev-parse", "HEAD"])).strip()
        except Exception:
            pass
        outcome = {
            "dirty": dirty,
            "dir": str(wt_dir),
            "base_commit": base_commit,
            "head_commit": head_commit,
            "branch": branch,
        }
        if dirty:
            return {"removed": False, **outcome}
        try:
            await run_git(root, ["worktree", "remove", "--force", str(wt_dir)])
        except Exception:
            pass
        try:
            shutil.rmtree(base, ignore_errors=True)
        except Exception:
            pass
        return {"removed": True, **outcome}

    return {
        "dir": str(wt_dir),
        "base_commit": base_commit,
        "branch": branch,
        "verify": verify,
        "cleanup": cleanup,
    }
