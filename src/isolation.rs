use crate::boundary::BoundaryEvent;
use crate::model::WorktreeState;
use crate::store::WorkflowStore;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

pub(crate) fn create_worktree(
    source: &Path,
    state_root: &Path,
    run_id: &str,
) -> Result<WorktreeState, String> {
    let base_commit = git_text(source, ["rev-parse", "HEAD"])?;
    let parent = state_root.join("worktrees");
    fs::create_dir_all(&parent).map_err(|error| {
        format!(
            "cannot create worktree parent {}: {error}",
            parent.display()
        )
    })?;
    let path = parent.join(run_id);
    if path.exists() {
        return Err(format!(
            "worktree path already exists for run {run_id}: {}",
            path.display()
        ));
    }
    git(
        source,
        [
            "worktree",
            "add",
            "--detach",
            path.to_str()
                .ok_or_else(|| format!("worktree path is not valid UTF-8: {}", path.display()))?,
            base_commit.as_str(),
        ],
    )?;
    Ok(WorktreeState {
        path,
        base_commit,
        finalized: false,
    })
}

pub(crate) fn finalize_worktree(
    store: &WorkflowStore,
    run_id: &str,
    worktree: &WorktreeState,
) -> Result<(), String> {
    if worktree.finalized {
        return Ok(());
    }
    let run_dir = store.run_dir(run_id);
    let patch_path = run_dir.join("worktree.patch");
    let commit_path = run_dir.join("worktree.commit.txt");
    let patch = git_text(
        &worktree.path,
        ["diff", "--binary", worktree.base_commit.as_str()],
    )?;
    fs::write(&patch_path, patch)
        .map_err(|error| format!("cannot write {}: {error}", patch_path.display()))?;
    let commit = git_text(&worktree.path, ["rev-parse", "HEAD"]).ok();
    let status = git_text(&worktree.path, ["status", "--porcelain"])?;
    let evidence = format!(
        "base_commit: {}\nhead: {}\nstatus:\n{}\n",
        worktree.base_commit,
        commit.as_deref().unwrap_or("unavailable"),
        status
    );
    fs::write(&commit_path, evidence)
        .map_err(|error| format!("cannot write {}: {error}", commit_path.display()))?;
    store
        .append_boundary_event(
            run_id,
            BoundaryEvent::WorktreeFinalized {
                path: worktree.path.clone(),
                patch_path,
                commit,
                status,
            },
        )
        .map_err(|error| error.to_string())
}

fn git_text<const N: usize>(cwd: &Path, args: [&str; N]) -> Result<String, String> {
    let output = git(cwd, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git<const N: usize>(cwd: &Path, args: [&str; N]) -> Result<Output, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|error| format!("git worktree command unavailable: {error}"))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "git worktree command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}
