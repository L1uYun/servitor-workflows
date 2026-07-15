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
from typing import Any
from pathlib import Path

from .terminal import run_command, run_git


def _command_result(command: list[str], stdout: str, stderr: str, exit_code: int, *, skipped: str | None = None) -> dict[str, Any]:
    result = {
        "command": shlex.join(command),
        "exit_code": exit_code,
        "stdout_tail": stdout[-2000:],
        "stderr_tail": stderr[-2000:],
    }
    if skipped:
        result["skipped"] = skipped
    return result




def build_closure_packet(*, integration: dict[str, Any], next_gate: str = "controller_review") -> dict[str, Any]:
    """Build the neat packet that a controller reviews after verify/delivery.

    This is the concrete closure trigger for integrated/release workflow slices:
    when verification evidence exists, the workflow emits a packet with branch,
    commits, delivery state, cleanup state, inherited-state reminder, and next
    gate instead of leaving closure as a manual memory step.
    """
    verification = integration.get("verification") or {}
    delivery = integration.get("delivery") or {}
    return {
        "kind": "neat_closure_packet",
        "branch": integration.get("branch"),
        "base_commit": integration.get("base_commit"),
        "head_commit": integration.get("head_commit"),
        "dirty": integration.get("dirty"),
        "worktree_dir": integration.get("dir"),
        "worktree_removed": integration.get("removed"),
        "verification": {
            "command": verification.get("command"),
            "exit_code": verification.get("exit_code"),
        },
        "delivery": {
            "ok": delivery.get("ok"),
            "dry_run": delivery.get("dry_run"),
            "commands": [cmd.get("command") for cmd in delivery.get("commands", [])],
            "pr_url": delivery.get("pr_url"),
        },
        "cleanup": "dry_run_required_before_branch_delete",
        "inherited_dirty_state": "classify_and_preserve",
        "next_gate": next_gate,
    }


async def delivery_cleanup_plan(repo_cwd: str, *, branch: str, remote: str = "origin", include_remote: bool = False, approved: bool = False) -> dict[str, Any]:
    """Return a dry-run cleanup plan for task-owned branch/worktree residue.

    The plan is safe by default and refuses executable cleanup without an
    explicit approval flag, so controllers can show a report before deleting
    merged local or remote branches.
    """
    if not branch:
        raise ValueError("branch is required")
    root = (await run_git(repo_cwd, ["rev-parse", "--show-toplevel"])).strip()
    merged = (await run_git(root, ["branch", "--merged", "HEAD", "--format", "%(refname:short)"])).splitlines()
    status = (await run_git(root, ["status", "--porcelain"])).strip()
    local_merged = branch in {line.strip() for line in merged}
    commands = []
    if local_merged:
        commands.append(["git", "branch", "-d", branch])
    if include_remote:
        commands.append(["git", "push", remote, "--delete", branch])
    return {
        "repo": root,
        "branch": branch,
        "remote": remote,
        "include_remote": include_remote,
        "approved": approved,
        "worktree_clean": status == "",
        "local_merged": local_merged,
        "commands": [shlex.join(c) for c in commands],
        "can_execute": approved and local_merged and status == "",
    }


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


    async def deliver(options: dict[str, Any] | bool | None = None) -> dict[str, Any] | None:
        """Push the verified branch and optionally create a draft PR.

        Delivery is opt-in and branch-bound. A dry_run option returns the exact
        command evidence without touching the network, which makes the delivery
        link testable in offline controller checks.
        """
        if not options:
            return None
        if branch is None:
            raise ValueError("delivery requires a named worktree_branch")
        if options is True:
            options = {"push": True, "pr": {"create": True, "draft": True, "fill": True}}
        if not isinstance(options, dict):
            raise ValueError("delivery options must be a dict or true")
        remote = str(options.get("remote") or "origin")
        dry_run = bool(options.get("dry_run"))
        commands: list[dict[str, Any]] = []
        delivery: dict[str, Any] = {"branch": branch, "remote": remote, "dry_run": dry_run, "commands": commands}

        async def run_or_record(command: list[str]) -> dict[str, Any]:
            if dry_run:
                result = _command_result(command, "", "", 0, skipped="dry_run")
            else:
                stdout, stderr, exit_code = await run_command(command, cwd=root)
                result = _command_result(command, stdout, stderr, exit_code)
            commands.append(result)
            return result

        if options.get("push", True):
            # Records/runs: git push -u <remote> <branch>
            await run_or_record(["git", "push", "-u", remote, branch])

        pr_options = options.get("pr")
        if pr_options is True:
            pr_options = {"create": True, "draft": True, "fill": True}
        if isinstance(pr_options, dict) and pr_options.get("create", True):
            cmd = ["gh", "pr", "create"]
            if pr_options.get("draft", True):
                cmd.append("--draft")
            if pr_options.get("fill", True):
                cmd.append("--fill")
            if pr_options.get("base"):
                cmd.extend(["--base", str(pr_options["base"])])
            if pr_options.get("title"):
                cmd.extend(["--title", str(pr_options["title"])])
            if pr_options.get("body"):
                cmd.extend(["--body", str(pr_options["body"])])
            result = await run_or_record(cmd)
            if result["stdout_tail"].strip():
                delivery["pr_url"] = result["stdout_tail"].strip().splitlines()[-1]
        delivery["ok"] = all(c["exit_code"] == 0 for c in commands)
        return delivery

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
        "deliver": deliver,
        "cleanup": cleanup,
    }
