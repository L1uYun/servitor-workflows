"""Terminal/git command execution helpers.

Used by worktree.py and other modules that need to run git or other CLI commands
as subprocesses with async-friendly interfaces.
"""
from __future__ import annotations

import asyncio
import subprocess


async def run_git(cwd: str, args: list[str]) -> str:
    """Run a git command in cwd and return stdout (stripped).

    Raises CalledProcessError on non-zero exit.
    """
    proc = await asyncio.create_subprocess_exec(
        "git", *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode or 1, ["git"] + args,
            output=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
        )
    return stdout.decode("utf-8", errors="replace")


async def run_command(cmd: list[str], cwd: str | None = None,
                      timeout: float | None = None,
                      env: dict | None = None) -> tuple[str, str, int]:
    """Run a command and return (stdout, stderr, returncode)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise
    return (
        stdout.decode("utf-8", errors="replace"),
        stderr.decode("utf-8", errors="replace"),
        proc.returncode or 0,
    )
