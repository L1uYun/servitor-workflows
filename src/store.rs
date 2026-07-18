use crate::error::WorkflowError;
use crate::model::{JournalEntry, RunState};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug)]
pub struct WorkflowStore {
    root: PathBuf,
    writes: Mutex<()>,
}

impl WorkflowStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            writes: Mutex::new(()),
        }
    }

    pub fn from_environment() -> Self {
        let root = std::env::var_os("SERVITOR_WORKFLOWS_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(default_state_root);
        Self::new(root)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn run_dir(&self, run_id: &str) -> PathBuf {
        self.root.join("runs").join(run_id)
    }
    pub fn script_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("workflow.js")
    }
    pub fn state_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("state.json")
    }
    pub fn journal_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("journal.jsonl")
    }
    pub fn pause_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("pause.request")
    }
    pub fn cancel_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("cancel.request")
    }

    pub fn create_run(&self, state: &RunState, script: &str) -> Result<(), WorkflowError> {
        let dir = self.run_dir(&state.run_id);
        fs::create_dir_all(&dir).map_err(|source| WorkflowError::Write { path: dir, source })?;
        self.write(&self.script_path(&state.run_id), script.as_bytes())?;
        self.save_state(state)
    }

    pub fn load_script(&self, run_id: &str) -> Result<String, WorkflowError> {
        let path = self.script_path(run_id);
        fs::read_to_string(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                WorkflowError::RunNotFound(run_id.to_owned())
            } else {
                WorkflowError::Read { path, source }
            }
        })
    }

    pub fn load_state(&self, run_id: &str) -> Result<RunState, WorkflowError> {
        let path = self.state_path(run_id);
        let text = fs::read_to_string(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                WorkflowError::RunNotFound(run_id.to_owned())
            } else {
                WorkflowError::Read {
                    path: path.clone(),
                    source,
                }
            }
        })?;
        serde_json::from_str(&text).map_err(WorkflowError::Json)
    }

    pub fn save_state(&self, state: &RunState) -> Result<(), WorkflowError> {
        let bytes = serde_json::to_vec_pretty(state)?;
        self.write(&self.state_path(&state.run_id), &bytes)
    }

    pub fn update_state<F>(&self, run_id: &str, update: F) -> Result<RunState, WorkflowError>
    where
        F: FnOnce(&mut RunState),
    {
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("state write lock poisoned".to_owned()))?;
        let mut state = self.load_state(run_id)?;
        update(&mut state);
        state.updated_at = chrono::Utc::now();
        let bytes = serde_json::to_vec_pretty(&state)?;
        fs::write(self.state_path(run_id), bytes).map_err(|source| WorkflowError::Write {
            path: self.state_path(run_id),
            source,
        })?;
        Ok(state)
    }

    pub fn append(&self, run_id: &str, entry: &JournalEntry) -> Result<(), WorkflowError> {
        let _guard = self
            .writes
            .lock()
            .map_err(|_| WorkflowError::Invariant("journal write lock poisoned".to_owned()))?;
        let path = self.journal_path(run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| WorkflowError::Write {
                path: path.clone(),
                source,
            })?;
        let mut bytes = serde_json::to_vec(entry)?;
        bytes.push(b'\n');
        file.write_all(&bytes)
            .and_then(|_| file.sync_data())
            .map_err(|source| WorkflowError::Write { path, source })
    }

    pub fn journal_index(
        &self,
        run_id: &str,
    ) -> Result<BTreeMap<String, JournalEntry>, WorkflowError> {
        let path = self.journal_path(run_id);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeMap::new());
            }
            Err(source) => return Err(WorkflowError::Read { path, source }),
        };
        let mut index = BTreeMap::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let entry: JournalEntry = serde_json::from_str(line)?;
            index.insert(entry.key.clone(), entry);
        }
        Ok(index)
    }

    pub fn request_pause(&self, run_id: &str) -> Result<(), WorkflowError> {
        self.assert_run(run_id)?;
        self.write(&self.pause_path(run_id), b"")
    }
    pub fn clear_pause(&self, run_id: &str) -> Result<(), WorkflowError> {
        match fs::remove_file(self.pause_path(run_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(WorkflowError::Write {
                path: self.pause_path(run_id),
                source,
            }),
        }
    }
    pub fn request_cancel(&self, run_id: &str) -> Result<(), WorkflowError> {
        self.assert_run(run_id)?;
        self.write(&self.cancel_path(run_id), b"")
    }
    pub fn pause_requested(&self, run_id: &str) -> bool {
        self.pause_path(run_id).exists()
    }
    pub fn cancel_requested(&self, run_id: &str) -> bool {
        self.cancel_path(run_id).exists()
    }

    fn assert_run(&self, run_id: &str) -> Result<(), WorkflowError> {
        if self.run_dir(run_id).is_dir() {
            Ok(())
        } else {
            Err(WorkflowError::RunNotFound(run_id.to_owned()))
        }
    }
    fn write(&self, path: &Path, bytes: &[u8]) -> Result<(), WorkflowError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| WorkflowError::Write {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::write(path, bytes).map_err(|source| WorkflowError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

fn default_state_root() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(r"D:\AgentWork\state\servitor-workflows")
    } else {
        std::env::temp_dir().join("servitor-workflows")
    }
}
