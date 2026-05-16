//! Persistent and in-memory implementations of the canonical
//! [`HardStateStore`](xraft_core::storage::HardStateStore) trait.
//!
//! The Raft hard state — `current_term` + `voted_for` — must be
//! durable across restarts and visible *before* any RPC reply
//! that could lose those decisions on a crash (`architecture.md`
//! §3.3, `tech-spec.md` §5.3).
//!
//! Two implementations live here:
//!
//! * [`MemoryHardStateStore`] — volatile, for tests.
//! * [`FileHardStateStore`] — durable, JSON-serialised under
//!   `<dir>/quorum-state` with a write-tmp + rename atomic
//!   replacement protocol.
//!
//! The atomic protocol matches KRaft's `quorum-state` file:
//! a `.tmp` sibling is written, fsynced, and `rename`-d into
//! place, then the parent directory itself is fsynced so the
//! rename's directory-entry update is durable across power
//! loss (Linux ext4 with default mount options does not
//! guarantee this without an explicit dir fsync; losing the
//! rename would let the node re-vote in the same term after
//! restart and violate Raft safety). A `.tmp` left behind by
//! a crash is detected on [`FileHardStateStore::open`] and
//! removed before the new handle starts serving.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use xraft_core::error::{Result, XRaftError};
use xraft_core::storage::{HardState, HardStateStore};
use xraft_core::types::{NodeId, Term};

const STATE_FILE_NAME: &str = "quorum-state";
const TMP_SUFFIX: &str = ".tmp";

/// JSON wire schema for the on-disk `quorum-state` file. Decouples
/// the serialised form from [`HardState`] so the canonical engine
/// type can evolve without breaking the on-disk format.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OnDiskHardState {
    /// `HardState::current_term`.
    current_term: u64,
    /// `HardState::voted_for` (`None` = nobody voted yet this term).
    voted_for: Option<u64>,
}

impl From<&HardState> for OnDiskHardState {
    fn from(hs: &HardState) -> Self {
        Self {
            current_term: hs.current_term.0,
            voted_for: hs.voted_for.map(|n| n.0),
        }
    }
}

impl From<OnDiskHardState> for HardState {
    fn from(d: OnDiskHardState) -> Self {
        Self {
            current_term: Term(d.current_term),
            voted_for: d.voted_for.map(NodeId),
        }
    }
}

// ---------------------------------------------------------------------------
// MemoryHardStateStore
// ---------------------------------------------------------------------------

/// Volatile in-memory store. Useful for tests and the in-process
/// driver harness; production paths use [`FileHardStateStore`].
#[derive(Debug, Default)]
pub struct MemoryHardStateStore {
    state: RwLock<Option<HardState>>,
}

impl MemoryHardStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HardStateStore for MemoryHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        let mut guard = self
            .state
            .write()
            .map_err(|e| XRaftError::Storage(format!("MemoryHardStateStore lock poisoned: {e}")))?;
        *guard = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        let guard = self
            .state
            .read()
            .map_err(|e| XRaftError::Storage(format!("MemoryHardStateStore lock poisoned: {e}")))?;
        Ok(guard.clone())
    }
}

// ---------------------------------------------------------------------------
// FileHardStateStore
// ---------------------------------------------------------------------------

/// Durable, file-backed implementation of [`HardStateStore`].
///
/// State is serialised as JSON under `<dir>/quorum-state` and
/// written via write-tmp + rename for crash safety. An in-memory
/// cache avoids re-reading the file on every `load()`.
///
/// **First-boot semantics**: [`load`](HardStateStore::load) returns
/// `Ok(None)` until [`persist`](HardStateStore::persist) is called
/// at least once. The driver relies on this to detect bootstrap
/// vs. recovery.
#[derive(Debug)]
pub struct FileHardStateStore {
    dir: PathBuf,
    /// `Some` only after a successful `persist` OR a successful
    /// `open` against an existing file. `None` until then.
    cached: RwLock<Option<HardState>>,
}

impl FileHardStateStore {
    /// Open (or initialise) a hard-state store rooted at `dir`.
    /// Creates the directory if missing; recovers any leftover
    /// `.tmp` file written by an interrupted previous run; and
    /// seeds the in-memory cache from the on-disk JSON when
    /// present.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| {
            XRaftError::Storage(format!(
                "hard-state create_dir_all '{}': {e}",
                dir.display()
            ))
        })?;

        let state_path = dir.join(STATE_FILE_NAME);
        let tmp_path = dir.join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));

        // Clean up any leftover .tmp from an interrupted persist —
        // the canonical file (if any) is still authoritative.
        if tmp_path.exists() {
            // Best-effort removal; surface a Storage error only if
            // the file is present AND undeletable.
            fs::remove_file(&tmp_path).map_err(|e| {
                XRaftError::Storage(format!(
                    "remove stale tmp file '{}': {e}",
                    tmp_path.display()
                ))
            })?;
        }

        let cached = if state_path.exists() {
            let raw = fs::read_to_string(&state_path).map_err(|e| {
                XRaftError::Storage(format!(
                    "read hard-state file '{}': {e}",
                    state_path.display()
                ))
            })?;
            let on_disk: OnDiskHardState = serde_json::from_str(&raw).map_err(|e| {
                XRaftError::Storage(format!(
                    "deserialise hard-state '{}': {e}",
                    state_path.display()
                ))
            })?;
            Some(HardState::from(on_disk))
        } else {
            None
        };

        Ok(Self {
            dir,
            cached: RwLock::new(cached),
        })
    }

    /// Path to the canonical quorum-state file.
    pub fn state_path(&self) -> PathBuf {
        self.dir.join(STATE_FILE_NAME)
    }

    fn atomic_write(&self, state: &HardState) -> Result<()> {
        let tmp_path = self.dir.join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));
        let final_path = self.state_path();
        let on_disk = OnDiskHardState::from(state);
        let serialised = serde_json::to_string_pretty(&on_disk)
            .map_err(|e| XRaftError::Storage(format!("serialise hard-state: {e}")))?;

        // Open + write + fsync the tmp, then rename onto the
        // canonical file. The rename is atomic on POSIX and
        // (since NTFS) on Windows when both paths are on the
        // same volume; cross-volume renames fall back to copy +
        // delete which we accept as best-effort.
        {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| {
                    XRaftError::Storage(format!(
                        "open tmp hard-state '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
            f.write_all(serialised.as_bytes()).map_err(|e| {
                XRaftError::Storage(format!(
                    "write tmp hard-state '{}': {e}",
                    tmp_path.display()
                ))
            })?;
            f.sync_all().map_err(|e| {
                XRaftError::Storage(format!(
                    "fsync tmp hard-state '{}': {e}",
                    tmp_path.display()
                ))
            })?;
        }
        fs::rename(&tmp_path, &final_path).map_err(|e| {
            XRaftError::Storage(format!(
                "rename '{}' -> '{}': {e}",
                tmp_path.display(),
                final_path.display()
            ))
        })?;

        // Make the rename's directory-entry update durable. On
        // Linux ext4 with default mount options the rename's
        // metadata change is *not* guaranteed to survive a power
        // failure without an explicit fsync of the parent dir,
        // even though the data blocks of the renamed file were
        // already fsynced above. For Raft hard state, losing the
        // rename means the node could re-vote in the same term
        // after restart and break Raft's safety invariant. On
        // Windows NTFS the rename is metadata-journaled by the
        // filesystem, and `File::open` on a directory through
        // std requires `FILE_FLAG_BACKUP_SEMANTICS` which std
        // does not expose — so we gate the dir-fsync on `unix`.
        #[cfg(unix)]
        {
            let dir = fs::File::open(&self.dir).map_err(|e| {
                XRaftError::Storage(format!("open dir for fsync '{}': {e}", self.dir.display()))
            })?;
            dir.sync_all().map_err(|e| {
                XRaftError::Storage(format!("fsync dir '{}': {e}", self.dir.display()))
            })?;
        }

        Ok(())
    }
}

impl HardStateStore for FileHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        self.atomic_write(state)?;
        let mut guard = self
            .cached
            .write()
            .map_err(|e| XRaftError::Storage(format!("FileHardStateStore lock poisoned: {e}")))?;
        *guard = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        let guard = self
            .cached
            .read()
            .map_err(|e| XRaftError::Storage(format!("FileHardStateStore lock poisoned: {e}")))?;
        Ok(guard.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_state(term: u64, vote: Option<u64>) -> HardState {
        HardState {
            current_term: Term(term),
            voted_for: vote.map(NodeId),
        }
    }

    #[test]
    fn first_boot_load_returns_none_then_some_after_persist() {
        let tmp = TempDir::new().unwrap();
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        assert!(store.load().unwrap().is_none(), "first-boot load = None");

        let hs = sample_state(5, Some(3));
        store.persist(&hs).unwrap();
        let loaded = store.load().unwrap().expect("persisted state must load");
        assert_eq!(loaded, hs);
    }

    #[test]
    fn reopen_recovers_persisted_state() {
        let tmp = TempDir::new().unwrap();
        let hs = sample_state(42, Some(1));
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&hs).unwrap();
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store2
            .load()
            .unwrap()
            .expect("recovered state must be Some");
        assert_eq!(loaded.current_term.0, 42);
        assert_eq!(loaded.voted_for.map(|n| n.0), Some(1));
    }

    #[test]
    fn open_clears_leftover_tmp_file() {
        let tmp = TempDir::new().unwrap();
        // Drop a stale .tmp file that would otherwise persist
        // across the next open.
        let tmp_file = tmp.path().join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));
        fs::write(&tmp_file, b"{\"garbage\":true}").unwrap();
        assert!(tmp_file.exists());
        let _store = FileHardStateStore::open(tmp.path()).unwrap();
        assert!(!tmp_file.exists(), "stale tmp must be removed on open");
    }

    #[test]
    fn memory_store_roundtrip() {
        let mut store = MemoryHardStateStore::new();
        assert!(store.load().unwrap().is_none());
        let hs = sample_state(7, None);
        store.persist(&hs).unwrap();
        assert_eq!(store.load().unwrap(), Some(hs));
    }

    #[test]
    fn on_disk_schema_roundtrip_is_byte_stable() {
        // The on-disk JSON schema is deliberately decoupled from
        // the in-memory type — this test pins the wire field
        // names so a future rename of HardState fields cannot
        // silently break crash recovery.
        let hs = sample_state(11, Some(2));
        let on_disk = OnDiskHardState::from(&hs);
        let json = serde_json::to_string(&on_disk).unwrap();
        assert!(json.contains("\"current_term\":11"), "json was {json}");
        assert!(json.contains("\"voted_for\":2"), "json was {json}");
        let parsed: OnDiskHardState = serde_json::from_str(&json).unwrap();
        let back: HardState = parsed.into();
        assert_eq!(back, hs);
    }
}
