//! Boundary declarations and append-only audit evidence.
//!
//! This module deliberately records and verifies only observable facts at the
//! workflow host boundary. It is not an OS security sandbox: command children
//! can still use paths or networking that the host cannot observe directly.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;

const MAX_SNAPSHOT_FILES: usize = 10_000;
const MAX_SNAPSHOT_FILE_BYTES: u64 = 16 * 1024 * 1024;

pub const BOUNDARY_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BoundaryPolicy {
    #[serde(default)]
    pub read_paths: Vec<PathBuf>,
    #[serde(default)]
    pub write_paths: Vec<PathBuf>,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default)]
    pub environment: EnvironmentPolicy,
    #[serde(default)]
    pub isolation: IsolationLevel,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    #[default]
    None,
    Worktree,
    Process,
    Container,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    #[default]
    Deny,
    Allow,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnvironmentPolicy {
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryEvent {
    Declared {
        policy: BoundaryPolicy,
    },
    CommandObserved {
        key: String,
        program: String,
        cwd: PathBuf,
        environment: Vec<String>,
    },
    AgentObserved {
        key: String,
        cwd: PathBuf,
    },
    NetworkObserved {
        key: String,
        declared: bool,
        source: NetworkEvidenceSource,
    },
    FileSnapshot {
        key: String,
        before: FileSnapshot,
        after: FileSnapshot,
    },
    GitSnapshot {
        key: Option<String>,
        before: GitSnapshot,
        after: GitSnapshot,
    },
    ChildDeclared {
        key: String,
        policy: BoundaryPolicy,
    },
    WorktreeFinalized {
        path: PathBuf,
        patch_path: PathBuf,
        commit: Option<String>,
        status: String,
    },
    Violation {
        key: Option<String>,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkEvidenceSource {
    /// A command's opt-in declaration is observable; command-side socket use is not.
    CommandDeclaration,
    /// An agent request is observable because it reaches the configured transport.
    AgentTransport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSnapshot {
    pub roots: Vec<PathBuf>,
    pub files: Vec<FileSnapshotEntry>,
    pub omitted_files: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_undeclared_writes: Vec<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSnapshotEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified_unix_ms: Option<u128>,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSnapshot {
    pub cwd: PathBuf,
    pub head: Option<String>,
    pub status: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryEnvelope {
    pub version: u32,
    pub sequence: u64,
    pub at: chrono::DateTime<chrono::Utc>,
    pub run_id: String,
    pub event: BoundaryEvent,
}

pub fn resolve_policy(policy: &BoundaryPolicy, base: &Path) -> Result<BoundaryPolicy, String> {
    Ok(BoundaryPolicy {
        read_paths: resolve_paths(&policy.read_paths, base, "readPaths")?,
        write_paths: resolve_paths(&policy.write_paths, base, "writePaths")?,
        network: policy.network.clone(),
        environment: EnvironmentPolicy {
            allow: normalize_environment_names(&policy.environment.allow)?,
        },
        isolation: policy.isolation.clone(),
    })
}

pub fn ensure_child_narrows(parent: &BoundaryPolicy, child: &BoundaryPolicy) -> Result<(), String> {
    ensure_paths_within(
        &child.read_paths,
        &parent.read_paths,
        "child readPaths widen parent boundary",
    )?;
    ensure_paths_within(
        &child.write_paths,
        &parent.write_paths,
        "child writePaths widen parent boundary",
    )?;
    if parent.network == NetworkPolicy::Deny && child.network == NetworkPolicy::Allow {
        return Err("child network policy widens parent boundary".to_owned());
    }
    if isolation_rank(&child.isolation) < isolation_rank(&parent.isolation) {
        return Err(format!(
            "child isolation widens parent boundary: {:?} is weaker than {:?}",
            child.isolation, parent.isolation
        ));
    }
    for name in &child.environment.allow {
        if !parent.environment.allow.contains(name) {
            return Err(format!(
                "child environment allowlist widens parent boundary: {name}"
            ));
        }
    }
    Ok(())
}

fn isolation_rank(isolation: &IsolationLevel) -> u8 {
    match isolation {
        IsolationLevel::None => 0,
        IsolationLevel::Worktree => 1,
        IsolationLevel::Process => 2,
        IsolationLevel::Container => 3,
    }
}

pub fn ensure_cwd_allowed(policy: &BoundaryPolicy, cwd: &Path) -> Result<(), String> {
    ensure_path_within(cwd, &policy.read_paths, "cwd is outside declared readPaths")
}

pub fn validate_command_environment(
    policy: &BoundaryPolicy,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<String>, String> {
    let mut names = environment.keys().cloned().collect::<Vec<_>>();
    names.sort();
    for name in &names {
        if !policy
            .environment
            .allow
            .contains(&name.to_ascii_uppercase())
        {
            return Err(format!("environment variable is not declared: {name}"));
        }
    }
    Ok(names)
}

pub fn ensure_command_policy(
    policy: &BoundaryPolicy,
    cwd: &Path,
    network: bool,
) -> Result<(), String> {
    ensure_cwd_allowed(policy, cwd)?;
    if network && policy.network == NetworkPolicy::Deny {
        return Err("command declares network access but network policy denies it".to_owned());
    }
    Ok(())
}

fn resolve_paths(paths: &[PathBuf], base: &Path, name: &str) -> Result<Vec<PathBuf>, String> {
    paths
        .iter()
        .map(|path| {
            let path = if path.is_absolute() {
                path.clone()
            } else {
                base.join(path)
            };
            lexical_absolute(&path).ok_or_else(|| {
                format!(
                    "{name} contains a non-normalizable path: {}",
                    path.display()
                )
            })
        })
        .collect()
}

fn normalize_environment_names(names: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = names
        .iter()
        .map(|name| {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(format!("invalid environment variable name: {name}"));
            }
            Ok(name.to_ascii_uppercase())
        })
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn ensure_paths_within(
    candidates: &[PathBuf],
    allowed: &[PathBuf],
    message: &str,
) -> Result<(), String> {
    for candidate in candidates {
        ensure_path_within(candidate, allowed, message)?;
    }
    Ok(())
}

fn ensure_path_within(path: &Path, allowed: &[PathBuf], message: &str) -> Result<(), String> {
    if allowed.iter().any(|root| path_within(path, root)) {
        Ok(())
    } else {
        Err(format!("{message}: {}", path.display()))
    }
}

fn path_within(path: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        fn normalized(path: &Path) -> String {
            path.as_os_str()
                .to_string_lossy()
                .trim_start_matches(r"\\?\")
                .trim_start_matches(r"\??\")
                .to_ascii_lowercase()
        }
        let path = normalized(path);
        let root = normalized(root);
        Path::new(&path).starts_with(Path::new(&root))
    }
    #[cfg(not(windows))]
    {
        path.starts_with(root)
    }
}

pub fn snapshot_observable_files(policy: &BoundaryPolicy) -> Result<FileSnapshot, String> {
    let mut roots = policy.read_paths.clone();
    roots.extend(policy.write_paths.iter().cloned());
    roots.sort();
    roots.dedup();
    snapshot_paths(roots)
}

pub fn observed_undeclared_writes(
    before: &FileSnapshot,
    after: &FileSnapshot,
    policy: &BoundaryPolicy,
) -> Vec<PathBuf> {
    let before = before
        .files
        .iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let after = after
        .files
        .iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut writes = BTreeSet::new();
    for (path, entry) in &after {
        if before.get(path).is_none_or(|previous| *previous != *entry) {
            writes.insert(path.clone());
        }
    }
    for path in before.keys().filter(|path| !after.contains_key(*path)) {
        writes.insert(path.clone());
    }
    writes
        .into_iter()
        .filter(|path| {
            !policy
                .write_paths
                .iter()
                .any(|root| path_within(path, root))
        })
        .collect()
}

fn snapshot_paths(roots: Vec<PathBuf>) -> Result<FileSnapshot, String> {
    let mut files = Vec::new();
    let mut omitted_files = 0;
    let mut seen = BTreeSet::new();
    for root in &roots {
        snapshot_directory(root, &mut files, &mut omitted_files, &mut seen)?;
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(FileSnapshot {
        roots,
        files,
        omitted_files,
        observed_undeclared_writes: Vec::new(),
    })
}

pub fn snapshot_git(cwd: &Path) -> GitSnapshot {
    let cwd = cwd.to_path_buf();
    let head = git_output(&cwd, ["rev-parse", "HEAD"]);
    let status = git_output(&cwd, ["status", "--porcelain"]);
    let error = match (&head, &status) {
        (Ok(_), Ok(_)) => None,
        (Err(error), _) | (_, Err(error)) => Some(error.clone()),
    };
    GitSnapshot {
        cwd,
        head: head.ok(),
        status: status.ok(),
        error,
    }
}

fn snapshot_directory(
    root: &Path,
    files: &mut Vec<FileSnapshotEntry>,
    omitted_files: &mut u64,
    seen: &mut BTreeSet<PathBuf>,
) -> Result<(), String> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("cannot snapshot {}: {error}", root.display())),
    };
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("cannot enumerate {}: {error}", root.display()))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if metadata.is_dir() {
            snapshot_directory(&path, files, omitted_files, seen)?;
            continue;
        }
        if !metadata.is_file() || !seen.insert(path.clone()) {
            continue;
        }
        if files.len() >= MAX_SNAPSHOT_FILES {
            *omitted_files += 1;
            continue;
        }
        let modified_unix_ms = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis());
        let sha256 = (metadata.len() <= MAX_SNAPSHOT_FILE_BYTES)
            .then(|| hash_file(&path))
            .transpose()?;
        files.push(FileSnapshotEntry {
            path,
            size: metadata.len(),
            modified_unix_ms,
            sha256,
        });
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String, String> {
    let mut file =
        fs::File::open(path).map_err(|error| format!("cannot hash {}: {error}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot hash {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn git_output<const N: usize>(cwd: &Path, args: [&str; N]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|error| format!("git snapshot unavailable: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git snapshot failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn lexical_absolute(path: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return None;
                }
            }
            Component::Normal(segment) => out.push(segment),
        }
    }
    out.is_absolute().then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> BoundaryPolicy {
        BoundaryPolicy {
            read_paths: vec![PathBuf::from(r"D:\work")],
            write_paths: vec![PathBuf::from(r"D:\work\out")],
            network: NetworkPolicy::Deny,
            environment: EnvironmentPolicy {
                allow: vec!["safe_var".to_owned()],
            },
            isolation: IsolationLevel::None,
        }
    }

    #[test]
    fn child_can_only_narrow_parent_policy() {
        let parent = resolve_policy(&policy(), Path::new(r"D:\work")).expect("parent policy");
        let child = BoundaryPolicy {
            read_paths: vec![PathBuf::from(r"D:\work\sub")],
            write_paths: vec![PathBuf::from(r"D:\work\out\sub")],
            network: NetworkPolicy::Deny,
            environment: EnvironmentPolicy {
                allow: vec!["SAFE_VAR".to_owned()],
            },
            isolation: IsolationLevel::None,
        };
        assert!(ensure_child_narrows(&parent, &child).is_ok());

        let mut widened = child;
        widened.network = NetworkPolicy::Allow;
        assert!(
            ensure_child_narrows(&parent, &widened)
                .expect_err("network widening")
                .contains("widens")
        );

        let mut weaker_isolation = parent.clone();
        weaker_isolation.isolation = IsolationLevel::None;
        let mut process_parent = parent;
        process_parent.isolation = IsolationLevel::Process;
        assert!(
            ensure_child_narrows(&process_parent, &weaker_isolation)
                .expect_err("isolation weakening")
                .contains("isolation")
        );
    }

    #[test]
    fn undeclared_environment_is_rejected_without_value() {
        let policy = resolve_policy(&policy(), Path::new(r"D:\work")).expect("policy");
        let environment = BTreeMap::from([
            ("SAFE_VAR".to_owned(), "not persisted".to_owned()),
            ("TOKEN".to_owned(), "secret".to_owned()),
        ]);
        let error =
            validate_command_environment(&policy, &environment).expect_err("undeclared env");
        assert_eq!(error, "environment variable is not declared: TOKEN");
        assert!(!error.contains("secret"));
    }

    #[test]
    fn environment_names_follow_windows_case_insensitivity() {
        let policy = resolve_policy(&policy(), Path::new(r"D:\work")).expect("policy");
        let environment = BTreeMap::from([("safe_var".to_owned(), "not persisted".to_owned())]);
        assert_eq!(
            validate_command_environment(&policy, &environment).expect("allowed env"),
            vec!["safe_var"]
        );
    }
}
