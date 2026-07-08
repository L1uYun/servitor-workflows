"""Worktree isolation for agent() calls that mutate files in parallel.

1:1 Python port of runner/src/worktree.js. Creates a detached git worktree at HEAD
and runs the agent with its cwd pointed there. On completion the worktree is removed
only if unchanged; if the agent left changes, the worktree is kept.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from pathlib import Path

from .terminal import run_git


async def is_git_repo(cwd: str) -> bool:
    """Check if cwd is inside a git work tree."""
    try:
        result = await run_git(cwd, ["rev-parse", "--is-inside-work-tree"])
        return result.strip() == "true"
    except Exception:
        return False


async def create_worktree(repo_cwd: str) -> dict:
    """Create a detached worktree at HEAD of the repo containing repo_cwd.

    Returns {dir, cleanup} where cleanup() removes the worktree if clean and
    returns {removed, dirty, dir}.
    """
    root = (await run_git(repo_cwd, ["rev-parse", "--show-toplevel"])).strip()
    base = Path(tempfile.mkdtemp(prefix="wf-worktree-"))
    wt_dir = base / "wt"
    await run_git(root, ["worktree", "add", "--detach", str(wt_dir), "HEAD"])

    async def cleanup() -> dict:
        dirty = False
        try:
            status = await run_git(str(wt_dir), ["status", "--porcelain"])
            dirty = len(status) > 0
        except Exception:
            pass
        if dirty:
            return {"removed": False, "dirty": True, "dir": str(wt_dir)}
        try:
            await run_git(root, ["worktree", "remove", "--force", str(wt_dir)])
        except Exception:
            pass
        try:
            shutil.rmtree(base, ignore_errors=True)
        except Exception:
            pass
        return {"removed": True, "dirty": False, "dir": str(wt_dir)}

    return {"dir": str(wt_dir), "cleanup": cleanup}
