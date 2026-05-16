//! Persistent and in-memory implementations of the canonical
//! [`HardStateStore`](xraft_core::storage::HardStateStore) trait.
//!
//! The Raft hard state — `current_term`, `voted_for`, and the
//! Stage 7.2 iter-3 addition `commit_index` — must be durable
//! across restarts and visible *before* any RPC reply that could
//! lose those decisions on a crash (`architecture.md` §3.3,
//! `tech-spec.md` §5.3). `commit_index` joins the durable
//! hard state so a node that restarts with a non-empty log can
//! resume applying entries from the same commit watermark it
//! had reached pre-crash, instead of waiting for the leader to
//! re-commit them (Stage 7.2 detailed requirement: "`HardState`
//! itself contains `current_term`, `voted_for`, and
//! `commit_index` per Stage 2.2").
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
//!
//! **Stage 7.2 ΓÇö static voter set bootstrap.** The same file
//! also carries the [`VoterSet`](xraft_core::types::VoterSet)
//! initialised at first boot from `ClusterConfig.voters`. Both
//! pieces of state co-locate as top-level JSON fields so the
//! single atomic write-tmp + rename + dir-fsync protocol covers
//! both; the storage path reconciles two writes that touch
//! different fields by reading the cached combined state,
//! mutating the target field, and rewriting the merged JSON
//! atomically. Legacy files written by Stage 1.2-7.1 code that
//! lack the `voter_set` field still deserialise (the field is
//! `Option<VoterSet>` with `serde(default)`); the next
//! `persist_voter_set` call materialises the new schema on disk.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use xraft_core::error::{Result, XRaftError};
use xraft_core::storage::{HardState, HardStateStore};
use xraft_core::types::{LogIndex, NodeId, Term, VoterSet};

const STATE_FILE_NAME: &str = "quorum-state";
const TMP_SUFFIX: &str = ".tmp";

/// JSON wire schema for the on-disk `quorum-state` file. Decouples
/// the serialised form from [`HardState`] / [`VoterSet`] so the
/// canonical engine types can evolve without breaking the on-disk
/// format.
///
/// Field layout deliberately matches the pre-Stage 7.2 flat shape
/// (`current_term`, `voted_for`) so files written by Stage 1.2-7.1
/// code still deserialise without migration; the Stage 7.2
/// `voter_set` field is `Option` + `#[serde(default)]` so it defaults
/// to `None` for legacy files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OnDiskQuorumState {
    /// `HardState::current_term`.
    current_term: u64,
    /// `HardState::voted_for` (`None` = nobody voted yet this term).
    voted_for: Option<u64>,
    /// Stage 7.2 ΓÇö static voter set established at first boot.
    /// `None` on legacy files; the next `persist_voter_set` call
    /// upgrades the file to the new schema.
    #[serde(default)]
    voter_set: Option<VoterSet>,
    /// Stage 7.2 iter-3 finding #1 ΓÇö durable lower bound on the
    /// engine's `commit_index`. The driver populates this from
    /// `node.commit_index` (clamped to the durable log tip) on
    /// every `Action::PersistHardState` and at the final graceful
    /// shutdown persist. Legacy files written by Stage 1.2-7.1 /
    /// iter-1+2 code lack this field; `#[serde(default)]` keeps
    /// them deserialisable (recovery treats absence as `0`,
    /// matching the conservative "engine starts at snapshot
    /// baseline only" pre-Stage-7.2 behavior).
    #[serde(default)]
    commit_index: u64,
}

impl OnDiskQuorumState {
    fn from_cached(cached: &CachedQuorumState) -> Self {
        Self {
            current_term: cached
                .hard_state
                .as_ref()
                .map(|h| h.current_term.0)
                .unwrap_or(0),
            voted_for: cached
                .hard_state
                .as_ref()
                .and_then(|h| h.voted_for.map(|n| n.0)),
            voter_set: cached.voter_set.clone(),
            // Stage 7.2 iter-3 finding #1: round-trip the
            // commit_index lower bound. Absent (= 0) on a
            // voter-set-only first write — the next
            // `Action::PersistHardState` will rewrite the file
            // with the engine's real commit_index.
            commit_index: cached
                .hard_state
                .as_ref()
                .map(|h| h.commit_index.0)
                .unwrap_or(0),
        }
    }

    /// Whether this on-disk record actually carries hard-state
    /// information (i.e. the engine has called
    /// [`HardStateStore::persist`] at least once). A file written
    /// only by `persist_voter_set` will have `current_term = 0`,
    /// `voted_for = None`, AND `commit_index = 0`; we distinguish
    /// "hard state never persisted" from "hard state persisted at
    /// term 0 with no vote and no committed entries" via the
    /// [`CachedQuorumState::hard_state_persisted`] flag, not via
    /// inspection of the on-disk numbers (which are ambiguous for a
    /// freshly-elected term-0 node).
    ///
    /// This helper is kept for documentation only; the recovery
    /// path on [`FileHardStateStore::open`] always reconstructs
    /// `hard_state_persisted` from
    /// `voted_for.is_some() || current_term > 0 || commit_index > 0`
    /// — `commit_index > 0` joined the disjunction in Stage 7.2 iter-3
    /// so a file whose only persisted progress is committed entries
    /// (term=0 / vote=None pathological case, or a leader that
    /// proposed but never term-bumped) is still recognised as
    /// persisted hard state.
    #[allow(dead_code)]
    fn carries_hard_state(&self) -> bool {
        self.voted_for.is_some() || self.current_term > 0 || self.commit_index > 0
    }
}

/// Single in-memory mirror of the on-disk combined state.
///
/// Stored under a single `RwLock` so the two `persist_*` paths
/// cannot race: each takes the write-lock, mutates the relevant
/// field, atomically rewrites the file, and drops the lock.
///
/// `hard_state_persisted` distinguishes "the engine has never
/// called [`HardStateStore::persist`]" (first boot) from "the engine
/// has called it but the persisted value is `HardState::default()`
/// (term=0, vote=None)". This matters because
/// `xraft-server::Server::start_with_state_machine` uses
/// `load() == Ok(None)` to decide whether to log
/// "bootstrapping at term 0" vs. "recovered hard state from disk".
/// A file produced by `persist_voter_set` alone before any
/// `persist(hard_state)` must NOT be reported as recovered hard
/// state ΓÇö hence the explicit flag rather than inferring from the
/// `current_term` / `voted_for` numbers.
#[derive(Debug, Clone, Default)]
struct CachedQuorumState {
    hard_state: Option<HardState>,
    voter_set: Option<VoterSet>,
    /// `true` once [`HardStateStore::persist`] has been called at
    /// least once in this process OR once an on-disk file was found
    /// with non-default hard-state numbers
    /// (`voted_for.is_some() || current_term > 0 || commit_index > 0`
    /// — the `commit_index > 0` disjunct was added in Stage 7.2
    /// iter-3 when `commit_index` joined the persisted hard state).
    /// See the type doc for why the inference is safe in practice.
    hard_state_persisted: bool,
}

impl From<OnDiskQuorumState> for CachedQuorumState {
    fn from(d: OnDiskQuorumState) -> Self {
        // Stage 7.2 iter-3 finding #1: `commit_index > 0` joins the
        // "persisted" heuristic so a file written by a leader that
        // committed entries before any term/vote change is still
        // recognised as persisted hard state on recovery.
        let hard_state_persisted =
            d.voted_for.is_some() || d.current_term > 0 || d.commit_index > 0;
        let hard_state = if hard_state_persisted {
            Some(HardState {
                current_term: Term(d.current_term),
                voted_for: d.voted_for.map(NodeId),
                commit_index: LogIndex(d.commit_index),
            })
        } else {
            None
        };
        Self {
            hard_state,
            voter_set: d.voter_set,
            hard_state_persisted,
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
    state: RwLock<CachedQuorumState>,
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
        guard.hard_state = Some(state.clone());
        guard.hard_state_persisted = true;
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        let guard = self
            .state
            .read()
            .map_err(|e| XRaftError::Storage(format!("MemoryHardStateStore lock poisoned: {e}")))?;
        Ok(if guard.hard_state_persisted {
            guard.hard_state.clone()
        } else {
            None
        })
    }

    fn persist_voter_set(&mut self, voter_set: &VoterSet) -> Result<()> {
        let mut guard = self
            .state
            .write()
            .map_err(|e| XRaftError::Storage(format!("MemoryHardStateStore lock poisoned: {e}")))?;
        guard.voter_set = Some(voter_set.clone());
        Ok(())
    }

    fn load_voter_set(&self) -> Result<Option<VoterSet>> {
        let guard = self
            .state
            .read()
            .map_err(|e| XRaftError::Storage(format!("MemoryHardStateStore lock poisoned: {e}")))?;
        Ok(guard.voter_set.clone())
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
///
/// **Stage 7.2 combined-write protocol**: both `persist` and
/// `persist_voter_set` take the in-memory cache's write lock,
/// mutate the relevant field, atomically write the merged JSON,
/// then drop the lock. Each write is a single fsynced tmp +
/// rename + dir-fsync so a crash between writes cannot leave one
/// field updated and the other partially written.
#[derive(Debug)]
pub struct FileHardStateStore {
    dir: PathBuf,
    /// In-memory mirror of the on-disk combined state. Updated
    /// after every successful `atomic_write`.
    cached: RwLock<CachedQuorumState>,
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

        let cached: CachedQuorumState = if state_path.exists() {
            let raw = fs::read_to_string(&state_path).map_err(|e| {
                XRaftError::Storage(format!(
                    "read hard-state file '{}': {e}",
                    state_path.display()
                ))
            })?;
            let on_disk: OnDiskQuorumState = serde_json::from_str(&raw).map_err(|e| {
                XRaftError::Storage(format!(
                    "deserialise hard-state '{}': {e}",
                    state_path.display()
                ))
            })?;
            CachedQuorumState::from(on_disk)
        } else {
            CachedQuorumState::default()
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

    fn atomic_write(&self, cached: &CachedQuorumState) -> Result<()> {
        let tmp_path = self.dir.join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));
        let final_path = self.state_path();
        let on_disk = OnDiskQuorumState::from_cached(cached);
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
        let mut guard = self
            .cached
            .write()
            .map_err(|e| XRaftError::Storage(format!("FileHardStateStore lock poisoned: {e}")))?;
        let mut next = guard.clone();
        next.hard_state = Some(state.clone());
        next.hard_state_persisted = true;
        self.atomic_write(&next)?;
        *guard = next;
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        let guard = self
            .cached
            .read()
            .map_err(|e| XRaftError::Storage(format!("FileHardStateStore lock poisoned: {e}")))?;
        Ok(if guard.hard_state_persisted {
            guard.hard_state.clone()
        } else {
            None
        })
    }

    fn persist_voter_set(&mut self, voter_set: &VoterSet) -> Result<()> {
        let mut guard = self
            .cached
            .write()
            .map_err(|e| XRaftError::Storage(format!("FileHardStateStore lock poisoned: {e}")))?;
        let mut next = guard.clone();
        next.voter_set = Some(voter_set.clone());
        self.atomic_write(&next)?;
        *guard = next;
        Ok(())
    }

    fn load_voter_set(&self) -> Result<Option<VoterSet>> {
        let guard = self
            .cached
            .read()
            .map_err(|e| XRaftError::Storage(format!("FileHardStateStore lock poisoned: {e}")))?;
        Ok(guard.voter_set.clone())
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
            commit_index: LogIndex(0),
        }
    }

    fn sample_state_with_commit(term: u64, vote: Option<u64>, commit: u64) -> HardState {
        HardState {
            current_term: Term(term),
            voted_for: vote.map(NodeId),
            commit_index: LogIndex(commit),
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
        let cached = CachedQuorumState {
            hard_state: Some(sample_state_with_commit(11, Some(2), 17)),
            voter_set: None,
            hard_state_persisted: true,
        };
        let on_disk = OnDiskQuorumState::from_cached(&cached);
        let json = serde_json::to_string(&on_disk).unwrap();
        assert!(json.contains("\"current_term\":11"), "json was {json}");
        assert!(json.contains("\"voted_for\":2"), "json was {json}");
        assert!(json.contains("\"commit_index\":17"), "json was {json}");
        let parsed: OnDiskQuorumState = serde_json::from_str(&json).unwrap();
        let back = CachedQuorumState::from(parsed);
        assert_eq!(back.hard_state.as_ref().map(|h| h.current_term.0), Some(11));
        assert_eq!(
            back.hard_state
                .as_ref()
                .and_then(|h| h.voted_for.map(|n| n.0)),
            Some(2)
        );
        assert_eq!(back.hard_state.as_ref().map(|h| h.commit_index.0), Some(17));
        assert!(back.voter_set.is_none());
    }

    /// Stage 7.2 iter-3 finding #1: persisted `commit_index` must
    /// round-trip across `persist` + reopen. Without this the
    /// recovery path cannot use the persisted lower bound to raise
    /// the engine's `commit_index` past the snapshot baseline.
    #[test]
    fn file_store_commit_index_round_trips_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let hs = sample_state_with_commit(7, Some(2), 42);
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&hs).unwrap();
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store2
            .load()
            .unwrap()
            .expect("recovered hard state must be Some");
        assert_eq!(loaded.commit_index, LogIndex(42));
        assert_eq!(loaded.current_term, Term(7));
    }

    // -----------------------------------------------------------------
    // Stage 7.2 ΓÇö static voter set bootstrap tests
    // -----------------------------------------------------------------

    fn sample_voter_set(node_ids: &[u64]) -> VoterSet {
        use xraft_core::types::{DirectoryId, Endpoint, VoterRecord};
        let records: Vec<VoterRecord> = node_ids
            .iter()
            .map(|id| VoterRecord {
                node_id: NodeId(*id),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("127.0.0.1", 6000 + *id as u16)],
            })
            .collect();
        VoterSet::try_new(records).unwrap()
    }

    #[test]
    fn memory_store_voter_set_first_boot_load_returns_none_then_some() {
        let mut store = MemoryHardStateStore::new();
        assert!(
            store.load_voter_set().unwrap().is_none(),
            "first-boot load_voter_set = None"
        );
        let vs = sample_voter_set(&[1, 2, 3]);
        store.persist_voter_set(&vs).unwrap();
        assert_eq!(store.load_voter_set().unwrap().as_ref(), Some(&vs));
    }

    #[test]
    fn memory_store_persist_voter_set_preserves_hard_state() {
        let mut store = MemoryHardStateStore::new();
        let hs = sample_state(7, Some(3));
        store.persist(&hs).unwrap();
        let vs = sample_voter_set(&[1, 2, 3]);
        store.persist_voter_set(&vs).unwrap();
        // Both must remain on the same store after the second write.
        assert_eq!(store.load().unwrap(), Some(hs));
        assert_eq!(store.load_voter_set().unwrap().as_ref(), Some(&vs));
    }

    #[test]
    fn memory_store_persist_hard_state_preserves_voter_set() {
        let mut store = MemoryHardStateStore::new();
        let vs = sample_voter_set(&[1, 2, 3]);
        store.persist_voter_set(&vs).unwrap();
        let hs = sample_state(11, Some(2));
        store.persist(&hs).unwrap();
        assert_eq!(store.load().unwrap(), Some(hs));
        assert_eq!(store.load_voter_set().unwrap().as_ref(), Some(&vs));
    }

    #[test]
    fn file_store_voter_set_first_boot_load_returns_none_then_some() {
        let tmp = TempDir::new().unwrap();
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        assert!(
            store.load_voter_set().unwrap().is_none(),
            "first-boot load_voter_set = None"
        );
        let vs = sample_voter_set(&[1, 2, 3]);
        store.persist_voter_set(&vs).unwrap();
        let loaded = store
            .load_voter_set()
            .unwrap()
            .expect("persisted voter set must load");
        assert_eq!(loaded, vs);
    }

    #[test]
    fn file_store_reopen_recovers_persisted_voter_set() {
        let tmp = TempDir::new().unwrap();
        let vs = sample_voter_set(&[10, 20, 30]);
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist_voter_set(&vs).unwrap();
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store2
            .load_voter_set()
            .unwrap()
            .expect("recovered voter set must be Some");
        assert_eq!(loaded, vs);
    }

    #[test]
    fn file_store_combined_persist_round_trip_preserves_both_fields() {
        // The combined-write protocol must keep both hard_state and
        // voter_set together. Write each independently and assert
        // both reload after a reopen.
        let tmp = TempDir::new().unwrap();
        let hs = sample_state(42, Some(1));
        let vs = sample_voter_set(&[1, 2, 3]);
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&hs).unwrap();
            store.persist_voter_set(&vs).unwrap();
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store2.load().unwrap(), Some(hs));
        assert_eq!(store2.load_voter_set().unwrap().as_ref(), Some(&vs));
    }

    #[test]
    fn file_store_persist_voter_set_then_hard_state_preserves_both() {
        // Reverse ordering of the previous test — voter set first,
        // hard state second — must also leave both fields intact.
        let tmp = TempDir::new().unwrap();
        let hs = sample_state(99, None);
        let vs = sample_voter_set(&[7]);
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist_voter_set(&vs).unwrap();
            store.persist(&hs).unwrap();
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store2.load().unwrap(), Some(hs));
        assert_eq!(store2.load_voter_set().unwrap().as_ref(), Some(&vs));
    }

    #[test]
    fn file_store_legacy_file_without_voter_set_deserialises_with_none() {
        // Stage 7.2 backward compatibility: a `quorum-state` file
        // written by Stage 1.2-7.1 code only has `current_term` and
        // `voted_for`. Loading it must succeed and report
        // `voter_set = None` so the server bootstrap path knows to
        // persist the config-derived voter set on next start.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(STATE_FILE_NAME);
        // Pin the LEGACY flat schema as a string literal so a future
        // accidental rename of an `OnDiskQuorumState` field cannot
        // break recovery of files written before Stage 7.2.
        fs::write(&path, r#"{ "current_term": 5, "voted_for": 3 }"#).unwrap();
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store
            .load()
            .unwrap()
            .expect("legacy file's hard state must load");
        assert_eq!(loaded.current_term.0, 5);
        assert_eq!(loaded.voted_for.map(|n| n.0), Some(3));
        // Stage 7.2 iter-3 finding #1: legacy files lack
        // `commit_index`; serde default must fill in 0 so recovery
        // does not crash on missing-field deserialisation.
        assert_eq!(
            loaded.commit_index,
            LogIndex(0),
            "legacy file must deserialise with commit_index = 0"
        );
        assert!(
            store.load_voter_set().unwrap().is_none(),
            "legacy file must report voter_set = None"
        );
    }

    #[test]
    fn file_store_upgrade_writes_combined_schema_for_legacy_file() {
        // A legacy file's first `persist_voter_set` rewrites the
        // file with the Stage 7.2 combined schema while keeping the
        // prior hard state intact.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(STATE_FILE_NAME);
        fs::write(&path, r#"{ "current_term": 4, "voted_for": 2 }"#).unwrap();

        let vs = sample_voter_set(&[1, 2, 3]);
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist_voter_set(&vs).unwrap();
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        let hs = store2.load().unwrap().expect("hard state preserved");
        assert_eq!(hs.current_term.0, 4);
        assert_eq!(hs.voted_for.map(|n| n.0), Some(2));
        assert_eq!(store2.load_voter_set().unwrap().as_ref(), Some(&vs));
    }

    #[test]
    fn file_store_voter_set_only_load_returns_none_hard_state() {
        // A file that has only seen `persist_voter_set` (no
        // `persist(hard_state)` yet) must NOT report a synthesised
        // hard state to the recovery path. Otherwise the server
        // would log "recovered hard state" on a fresh-start node
        // that has only persisted the bootstrap voter set, breaking
        // the bootstrap vs. recovery telemetry split.
        let tmp = TempDir::new().unwrap();
        let vs = sample_voter_set(&[1]);
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist_voter_set(&vs).unwrap();
            assert!(store.load().unwrap().is_none());
        }
        let store2 = FileHardStateStore::open(tmp.path()).unwrap();
        assert!(
            store2.load().unwrap().is_none(),
            "voter-set-only file must not synthesise hard state on reopen"
        );
        assert_eq!(store2.load_voter_set().unwrap().as_ref(), Some(&vs));
    }
}
