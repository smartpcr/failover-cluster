//! Hard-state persistence implementations.
//!
//! Two implementations are provided:
//!
//! * [`MemoryHardStateStore`] — volatile, in-memory store for testing.
//! * [`FileHardStateStore`] — durable, file-backed store using atomic
//!   write-then-rename for crash safety (KRaft `quorum-state` pattern).

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::{debug, warn};
use xraft_core::error::{Result, XRaftError};
use xraft_core::storage::{HardState, HardStateStore};
use xraft_core::types::Term;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn storage_err(msg: impl Into<String>) -> XRaftError {
    XRaftError::Storage(msg.into())
}

// ---------------------------------------------------------------------------
// MemoryHardStateStore
// ---------------------------------------------------------------------------

/// In-memory hard-state store backed by a simple `Option`.
#[derive(Debug, Default)]
pub struct MemoryHardStateStore {
    state: Option<HardState>,
}

impl MemoryHardStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HardStateStore for MemoryHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        self.state = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        Ok(self.state.clone())
    }
}

// ---------------------------------------------------------------------------
// FileHardStateStore
// ---------------------------------------------------------------------------

/// Name of the on-disk state file, matching KRaft's `quorum-state` convention.
const STATE_FILENAME: &str = "quorum-state";
/// Temp file used during atomic writes.
const STATE_TMP_FILENAME: &str = "quorum-state.tmp";

/// Durable hard-state store backed by a JSON file on disk.
///
/// Persists `HardState` using atomic write-then-rename so that a crash
/// mid-write never corrupts the on-disk copy. Validates term monotonicity
/// and the invariant that `voted_for` is only set once per term.
#[derive(Debug)]
pub struct FileHardStateStore {
    /// Path to the `quorum-state` file.
    path: PathBuf,
    /// Path to the temporary file used during atomic writes.
    tmp_path: PathBuf,
    /// Path to the parent directory (for fsync after rename on Unix).
    #[cfg_attr(not(unix), allow(dead_code))]
    dir_path: PathBuf,
    /// Cached copy of the last persisted state (avoids re-reading from disk).
    cached: HardState,
}

impl FileHardStateStore {
    /// Open (or create) a `FileHardStateStore` rooted at `data_dir`.
    ///
    /// The directory is created if it does not exist. Any previously persisted
    /// state is loaded eagerly so that subsequent `load()` calls are free.
    /// If no persisted state exists, a default `HardState` (term 0, no vote)
    /// is returned by `load()`.
    pub fn open(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir).map_err(|e| storage_err(format!("create dir: {e}")))?;
        let path = data_dir.join(STATE_FILENAME);
        let tmp_path = data_dir.join(STATE_TMP_FILENAME);
        let dir_path = data_dir.to_path_buf();

        // Clean up any leftover temp file from a previous crash.
        if tmp_path.exists() {
            warn!(?tmp_path, "removing leftover temp state file from prior crash");
            fs::remove_file(&tmp_path)
                .map_err(|e| storage_err(format!("remove leftover tmp: {e}")))?;
        }

        let cached = if path.exists() {
            let data =
                fs::read_to_string(&path).map_err(|e| storage_err(format!("read state: {e}")))?;
            let hs: HardState = serde_json::from_str(&data)
                .map_err(|e| storage_err(format!("parse state: {e}")))?;
            debug!(?hs, "loaded persisted hard state");
            hs
        } else {
            debug!("no persisted hard state found; starting with default");
            HardState {
                current_term: Term(0),
                voted_for: None,
            }
        };

        Ok(Self {
            path,
            tmp_path,
            dir_path,
            cached,
        })
    }

    /// Validate that the new state does not violate Raft safety invariants
    /// relative to the previously persisted state.
    fn validate(&self, new: &HardState) -> Result<()> {
        let old = &self.cached;
        // Term must never decrease.
        if new.current_term < old.current_term {
            return Err(storage_err(format!(
                "term monotonicity violation: cannot go from {} to {}",
                old.current_term.0, new.current_term.0,
            )));
        }
        // Within the same term, once voted_for is set it cannot change.
        // This prevents the sequence Some(A) -> None -> Some(B) which
        // would violate the "vote at most once per term" Raft invariant.
        if new.current_term == old.current_term {
            if let Some(old_vote) = old.voted_for {
                match new.voted_for {
                    Some(new_vote) if new_vote != old_vote => {
                        return Err(storage_err(format!(
                            "double vote in term {}: already voted for {:?}, cannot vote for {:?}",
                            old.current_term.0, old_vote, new_vote,
                        )));
                    }
                    None => {
                        return Err(storage_err(format!(
                            "cannot clear voted_for in term {}: already voted for {:?}; advance term first",
                            old.current_term.0, old_vote,
                        )));
                    }
                    _ => {} // Same vote, idempotent — OK.
                }
            }
        }
        Ok(())
    }
}

impl HardStateStore for FileHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        self.validate(state)?;

        let json =
            serde_json::to_string_pretty(state).map_err(|e| storage_err(format!("serialize: {e}")))?;

        // Write to temp file first.
        {
            let mut f = fs::File::create(&self.tmp_path)
                .map_err(|e| storage_err(format!("create tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| storage_err(format!("write tmp: {e}")))?;
            f.sync_all()
                .map_err(|e| storage_err(format!("sync tmp: {e}")))?;
        }

        // Atomic rename.
        fs::rename(&self.tmp_path, &self.path)
            .map_err(|e| storage_err(format!("rename: {e}")))?;

        // Fsync the parent directory to ensure the rename is durable.
        // On Unix, opening a directory and calling fsync() ensures the
        // directory entry is flushed. Windows does not support this; NTFS
        // metadata is journaled so the rename is already durable after the
        // MoveFileEx call returns.
        #[cfg(unix)]
        {
            let dir = fs::File::open(&self.dir_path)
                .map_err(|e| storage_err(format!("open dir for fsync: {e}")))?;
            dir.sync_all()
                .map_err(|e| storage_err(format!("fsync dir: {e}")))?;
        }

        debug!(?state, "persisted hard state");
        self.cached = state.clone();
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        Ok(Some(self.cached.clone()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::{NodeId, Term};

    // -- MemoryHardStateStore tests ------------------------------------------

    #[test]
    fn memory_empty_store_loads_none() {
        let store = MemoryHardStateStore::new();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn memory_persist_and_load() {
        let mut store = MemoryHardStateStore::new();
        let hs = HardState {
            current_term: Term(5),
            voted_for: Some(NodeId(2)),
        };
        store.persist(&hs).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(5));
        assert_eq!(loaded.voted_for, Some(NodeId(2)));
    }

    #[test]
    fn memory_persist_overwrites() {
        let mut store = MemoryHardStateStore::new();
        store
            .persist(&HardState {
                current_term: Term(1),
                voted_for: None,
            })
            .unwrap();
        store
            .persist(&HardState {
                current_term: Term(3),
                voted_for: Some(NodeId(7)),
            })
            .unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(3));
        assert_eq!(loaded.voted_for, Some(NodeId(7)));
    }

    // -- FileHardStateStore tests --------------------------------------------

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn file_default_initial_state() {
        let dir = tmp_dir();
        let store = FileHardStateStore::open(dir.path()).unwrap();
        // A fresh store (no quorum-state file) returns the default HardState.
        let loaded = store.load().unwrap().expect("should return Some");
        assert_eq!(loaded.current_term, Term(0));
        assert_eq!(loaded.voted_for, None);
    }

    #[test]
    fn file_state_persistence() {
        let dir = tmp_dir();
        {
            let mut store = FileHardStateStore::open(dir.path()).unwrap();
            let hs = HardState {
                current_term: Term(5),
                voted_for: Some(NodeId(3)),
            };
            store.persist(&hs).unwrap();
        }
        // Reload from disk.
        let store2 = FileHardStateStore::open(dir.path()).unwrap();
        let loaded = store2.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(5));
        assert_eq!(loaded.voted_for, Some(NodeId(3)));
    }

    #[test]
    fn file_atomic_write_safety() {
        let dir = tmp_dir();
        // Persist a valid state.
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(2),
                voted_for: Some(NodeId(1)),
            })
            .unwrap();

        // Simulate a crash leaving behind a temp file with garbage.
        let tmp_path = dir.path().join(STATE_TMP_FILENAME);
        fs::write(&tmp_path, b"corrupted garbage").unwrap();
        assert!(tmp_path.exists());

        // Reopening should clean up the temp file and load the valid state.
        let store2 = FileHardStateStore::open(dir.path()).unwrap();
        assert!(!tmp_path.exists());
        let loaded = store2.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(2));
        assert_eq!(loaded.voted_for, Some(NodeId(1)));
    }

    #[test]
    fn file_term_monotonicity() {
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(5),
                voted_for: None,
            })
            .unwrap();

        // Attempting to go backwards must fail.
        let result = store.persist(&HardState {
            current_term: Term(3),
            voted_for: None,
        });
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("monotonicity"), "got: {msg}");
    }

    #[test]
    fn file_double_vote_rejected() {
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(5),
                voted_for: Some(NodeId(1)),
            })
            .unwrap();

        // Voting for a different candidate in the same term is illegal.
        let result = store.persist(&HardState {
            current_term: Term(5),
            voted_for: Some(NodeId(2)),
        });
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("double vote"), "got: {msg}");
    }

    #[test]
    fn file_same_vote_same_term_allowed() {
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        let hs = HardState {
            current_term: Term(5),
            voted_for: Some(NodeId(1)),
        };
        store.persist(&hs).unwrap();
        // Re-persisting the same vote is idempotent and must succeed.
        store.persist(&hs).unwrap();
    }

    #[test]
    fn file_new_term_resets_vote() {
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(5),
                voted_for: Some(NodeId(1)),
            })
            .unwrap();
        // Moving to a higher term allows voting for anyone.
        store
            .persist(&HardState {
                current_term: Term(6),
                voted_for: Some(NodeId(2)),
            })
            .unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(6));
        assert_eq!(loaded.voted_for, Some(NodeId(2)));
    }

    #[test]
    fn file_vote_none_to_some_same_term_allowed() {
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(3),
                voted_for: None,
            })
            .unwrap();
        // Setting voted_for from None to Some in the same term is legal.
        store
            .persist(&HardState {
                current_term: Term(3),
                voted_for: Some(NodeId(4)),
            })
            .unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.voted_for, Some(NodeId(4)));
    }

    #[test]
    fn file_clear_vote_same_term_rejected() {
        // Clearing voted_for within the same term is illegal — it would
        // open a path to double-voting: Some(A) -> None -> Some(B).
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(5),
                voted_for: Some(NodeId(1)),
            })
            .unwrap();

        let result = store.persist(&HardState {
            current_term: Term(5),
            voted_for: None,
        });
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("cannot clear voted_for"), "got: {msg}");
    }

    #[test]
    fn file_some_none_some_other_double_vote_prevented() {
        // End-to-end test: the sequence Some(A) → None → Some(B) in the
        // same term must be impossible. The second step (clearing) is the
        // one that must fail.
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();

        // Step 1: vote for node 1 in term 5.
        store
            .persist(&HardState {
                current_term: Term(5),
                voted_for: Some(NodeId(1)),
            })
            .unwrap();

        // Step 2: attempt to clear vote — must fail.
        let clear = store.persist(&HardState {
            current_term: Term(5),
            voted_for: None,
        });
        assert!(clear.is_err(), "clearing vote in same term must fail");

        // Step 3: even if we try voting for someone else, still rejected.
        let revote = store.persist(&HardState {
            current_term: Term(5),
            voted_for: Some(NodeId(2)),
        });
        assert!(revote.is_err(), "double vote must be rejected");

        // Original vote must still be intact.
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.voted_for, Some(NodeId(1)));
    }

    #[test]
    fn file_corrupt_quorum_state_returns_error() {
        let dir = tmp_dir();
        // Write garbage to quorum-state.
        let state_path = dir.path().join(STATE_FILENAME);
        fs::write(&state_path, b"this is not valid json!!!").unwrap();

        let result = FileHardStateStore::open(dir.path());
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("parse state"), "got: {msg}");
    }

    #[test]
    fn file_clear_vote_allowed_with_term_advance() {
        // Clearing voted_for is fine when the term advances.
        let dir = tmp_dir();
        let mut store = FileHardStateStore::open(dir.path()).unwrap();
        store
            .persist(&HardState {
                current_term: Term(5),
                voted_for: Some(NodeId(1)),
            })
            .unwrap();

        // Advance term and clear vote — should succeed.
        store
            .persist(&HardState {
                current_term: Term(6),
                voted_for: None,
            })
            .unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(6));
        assert_eq!(loaded.voted_for, None);
    }
}
