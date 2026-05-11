// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// Licensed under the MIT License.
// -----------------------------------------------------------------------

//! Persistent and in-memory implementations of the Raft hard-state store.
//!
//! The [`HardStateStore`] trait abstracts how a Raft node persists its
//! `term`, `voted_for`, and `commit` index across restarts.  Two concrete
//! implementations are provided:
//!
//! * [`MemoryHardStateStore`] – volatile, for tests.
//! * [`FileHardStateStore`] – durable, backed by an atomic-write file.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur when loading or persisting hard state.
#[derive(Debug, Error)]
pub enum HardStateError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, HardStateError>;

// ---------------------------------------------------------------------------
// HardState
// ---------------------------------------------------------------------------

/// The durable state that a Raft node must persist before responding to RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    /// The latest term the server has seen.
    pub term: u64,

    /// The candidate ID that received a vote in the current term (if any).
    pub voted_for: Option<u64>,

    /// The index of the highest log entry known to be committed.
    pub commit: u64,
}

impl Default for HardState {
    fn default() -> Self {
        Self {
            term: 0,
            voted_for: None,
            commit: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Abstraction over durable Raft hard-state storage.
///
/// # Contract
///
/// * [`load()`](HardStateStore::load) returns `Ok(None)` when **no state has
///   ever been persisted** (first boot).  It returns `Ok(Some(state))` when a
///   previously-persisted state exists (crash recovery).
/// * [`persist()`](HardStateStore::persist) durably stores the given state so
///   that a subsequent `load()` returns `Some`.
pub trait HardStateStore: Send + Sync {
    /// Load the most recently persisted hard state.
    ///
    /// Returns `Ok(None)` if no state has been persisted yet (first boot).
    fn load(&self) -> Result<Option<HardState>>;

    /// Durably persist the given hard state.
    fn persist(&self, state: &HardState) -> Result<()>;
}

// ---------------------------------------------------------------------------
// MemoryHardStateStore
// ---------------------------------------------------------------------------

/// A volatile, in-memory implementation of [`HardStateStore`].
///
/// Useful for unit tests where durability is unnecessary.
#[derive(Debug, Default)]
pub struct MemoryHardStateStore {
    state: RwLock<Option<HardState>>,
}

impl MemoryHardStateStore {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(None),
        }
    }
}

impl HardStateStore for MemoryHardStateStore {
    fn load(&self) -> Result<Option<HardState>> {
        let guard = self.state.read().expect("lock poisoned");
        Ok(guard.clone())
    }

    fn persist(&self, state: &HardState) -> Result<()> {
        let mut guard = self.state.write().expect("lock poisoned");
        *guard = Some(state.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FileHardStateStore
// ---------------------------------------------------------------------------

/// File name used inside the state directory.
const STATE_FILE_NAME: &str = "quorum-state";

/// Durable, file-backed implementation of [`HardStateStore`].
///
/// State is serialised as JSON and written atomically (write-to-temp then
/// rename).  An in-memory cache avoids repeated disk reads on every
/// `load()` call.
///
/// # First-boot vs crash-recovery
///
/// `open()` inspects whether the state file already exists on disk.  If it
/// does **not**, `load()` will return `None` until the first successful
/// `persist()` call — honouring the trait contract that `None` means "no
/// state was ever persisted."
pub struct FileHardStateStore {
    /// Directory that contains the state file.
    dir: PathBuf,

    /// In-memory cache of the last-known state.
    cached: RwLock<HardState>,

    /// `true` once a state file existed on disk at `open()` time **or**
    /// `persist()` has been called at least once during this lifetime.
    ///
    /// When `false`, `load()` returns `None` to signal first-boot.
    has_persisted: RwLock<bool>,
}

impl std::fmt::Debug for FileHardStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHardStateStore")
            .field("dir", &self.dir)
            .field("has_persisted", &self.has_persisted)
            .finish()
    }
}

impl FileHardStateStore {
    /// Open (or create) a hard-state store rooted at `dir`.
    ///
    /// If a `quorum-state` file already exists in `dir`, its contents are
    /// loaded into the in-memory cache and subsequent `load()` calls will
    /// return `Some`.  Otherwise the cache is seeded with a default
    /// `HardState` and `load()` returns `None` until `persist()` is called.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let path = dir.join(STATE_FILE_NAME);

        let (cached, has_persisted) = if path.exists() {
            let data = fs::read_to_string(&path)?;
            let state: HardState = serde_json::from_str(&data)?;
            (state, true)
        } else {
            (HardState::default(), false)
        };

        Ok(Self {
            dir,
            cached: RwLock::new(cached),
            has_persisted: RwLock::new(has_persisted),
        })
    }

    /// Full path to the state file.
    fn state_path(&self) -> PathBuf {
        self.dir.join(STATE_FILE_NAME)
    }

    /// Atomically write `state` to disk (write-tmp + rename).
    fn atomic_write(&self, state: &HardState) -> Result<()> {
        let tmp_path = self.dir.join(format!("{}.tmp", STATE_FILE_NAME));
        let final_path = self.state_path();

        let serialized = serde_json::to_string_pretty(state)?;

        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(serialized.as_bytes())?;
        file.sync_all()?;
        drop(file);

        fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }
}

impl HardStateStore for FileHardStateStore {
    fn load(&self) -> Result<Option<HardState>> {
        let has_persisted = self.has_persisted.read().expect("lock poisoned");
        if !*has_persisted {
            return Ok(None);
        }

        let cached = self.cached.read().expect("lock poisoned");
        Ok(Some(cached.clone()))
    }

    fn persist(&self, state: &HardState) -> Result<()> {
        self.atomic_write(state)?;

        // Update cache and mark as persisted.
        {
            let mut cached = self.cached.write().expect("lock poisoned");
            *cached = state.clone();
        }
        {
            let mut flag = self.has_persisted.write().expect("lock poisoned");
            *flag = true;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Thread-safe wrapper
// ---------------------------------------------------------------------------

/// Cheaply cloneable, thread-safe handle to any [`HardStateStore`].
#[derive(Clone)]
pub struct SharedHardStateStore {
    inner: Arc<dyn HardStateStore>,
}

impl SharedHardStateStore {
    pub fn new<S: HardStateStore + 'static>(store: S) -> Self {
        Self {
            inner: Arc::new(store),
        }
    }
}

impl HardStateStore for SharedHardStateStore {
    fn load(&self) -> Result<Option<HardState>> {
        self.inner.load()
    }

    fn persist(&self, state: &HardState) -> Result<()> {
        self.inner.persist(state)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -- MemoryHardStateStore -----------------------------------------------

    #[test]
    fn memory_store_load_returns_none_before_persist() {
        let store = MemoryHardStateStore::new();
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn memory_store_round_trip() {
        let store = MemoryHardStateStore::new();
        let state = HardState {
            term: 3,
            voted_for: Some(7),
            commit: 42,
        };
        store.persist(&state).unwrap();
        assert_eq!(store.load().unwrap(), Some(state));
    }

    // -- FileHardStateStore -------------------------------------------------

    #[test]
    fn file_store_load_returns_none_on_fresh_directory() {
        let tmp = TempDir::new().unwrap();
        let store = FileHardStateStore::open(tmp.path()).unwrap();

        // No state file exists yet — trait contract requires None.
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn file_store_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = FileHardStateStore::open(tmp.path()).unwrap();

        let state = HardState {
            term: 5,
            voted_for: Some(2),
            commit: 99,
        };
        store.persist(&state).unwrap();
        assert_eq!(store.load().unwrap(), Some(state));
    }

    #[test]
    fn file_store_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        let state = HardState {
            term: 10,
            voted_for: None,
            commit: 200,
        };

        // Persist, drop, reopen.
        {
            let store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&state).unwrap();
        }

        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store.load().unwrap(), Some(state));
    }

    #[test]
    fn file_store_reopen_with_existing_file_returns_some() {
        let tmp = TempDir::new().unwrap();

        let state = HardState {
            term: 1,
            voted_for: Some(1),
            commit: 0,
        };

        // First lifetime — persist state.
        {
            let store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&state).unwrap();
        }

        // Second lifetime — load should return Some (crash-recovery path).
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store.load().unwrap();
        assert!(loaded.is_some(), "existing file ⇒ Some on reload");
        assert_eq!(loaded.unwrap(), state);
    }

    // -- SharedHardStateStore -----------------------------------------------

    #[test]
    fn shared_wrapper_delegates() {
        let inner = MemoryHardStateStore::new();
        let shared = SharedHardStateStore::new(inner);

        assert_eq!(shared.load().unwrap(), None);

        let state = HardState {
            term: 1,
            voted_for: None,
            commit: 0,
        };
        shared.persist(&state).unwrap();
        assert_eq!(shared.load().unwrap(), Some(state));
    }
}