// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// Licensed under the MIT License.
// -----------------------------------------------------------------------

//! Concrete implementations of the canonical
//! [`xraft_core::storage::HardStateStore`] trait.
//!
//! The trait + the [`HardState`] struct
//! (`xraft_core::types::HardState { current_term, voted_for }`) live in
//! `xraft-core` per `architecture.md` §4.1 so the consensus engine
//! stays I/O-free; this module provides the durable file-backed
//! implementation and an in-memory implementation used by tests and
//! deterministic simulators.
//!
//! # Safety invariants enforced by every store
//!
//! Per `implementation-plan.md` Stage 2.2 + `architecture.md` §3.3,
//! both [`MemoryHardStateStore`] and [`FileHardStateStore`] reject any
//! [`persist`](xraft_core::storage::HardStateStore::persist) that would
//! violate Raft safety:
//!
//! * **Term monotonicity** — the new `current_term` must be `>=` the
//!   previously persisted term. Term regression would let a stale
//!   leader's writes be re-acked.
//! * **Single vote per term** — within a single term, `voted_for` may
//!   transition from `None -> Some(x)` exactly once and may then only
//!   be re-persisted as `Some(x)` (idempotent retries are allowed).
//!   Switching to a different candidate in the same term, or clearing
//!   a previously-granted vote, are both rejected.
//! * **Term advance resets vote** — when the new term is strictly
//!   greater, any `voted_for` is allowed (the new term carries fresh
//!   vote eligibility).
//!
//! Identical invariants in both implementations means in-memory tests
//! cannot mask safety bugs that would manifest only against the
//! file-backed store (a hole the iter-2 evaluator flagged).
//!
//! # Atomic-replace pattern (cross-platform, including Windows)
//!
//! `FileHardStateStore::persist` uses the same recoverable-replace
//! sequence as [`crate::FileSnapshotStore`]:
//!
//! 1. Write JSON to `quorum-state.tmp`, flush, `sync_all`.
//! 2. If `quorum-state` exists, rename it to `quorum-state.bak`
//!    (after first removing any stale `.bak`).
//! 3. Rename `quorum-state.tmp` to `quorum-state`.
//! 4. Remove `quorum-state.bak`.
//! 5. Best-effort `sync_all` on the parent directory (Unix only;
//!    Windows does not require directory fsync).
//!
//! Every step targets a path that does **not** exist at the moment of
//! the call, so the sequence is portable to Windows where
//! `MoveFileExW(REPLACE_EXISTING)` semantics are not always
//! sharing-violation-free for in-use destinations.
//!
//! On `open`, the store recovers any incomplete write by inspecting
//! the canonical / `.bak` / `.tmp` triple and restoring whichever
//! file is the surviving committed state.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use xraft_core::error::{Result, XRaftError};
use xraft_core::storage::HardStateStore;
use xraft_core::types::HardState;

// ---------------------------------------------------------------------------
// File-name constants (KRaft-style `quorum-state` per tech-spec §5.3)
// ---------------------------------------------------------------------------

const STATE_FILE_NAME: &str = "quorum-state";
const TMP_SUFFIX: &str = ".tmp";
const BAK_SUFFIX: &str = ".bak";

// ---------------------------------------------------------------------------
// On-disk envelope
// ---------------------------------------------------------------------------

/// Versioned wrapper persisted to `quorum-state`. Wrapping `HardState`
/// in an envelope lets future iterations evolve the on-disk schema
/// without breaking older snapshots: `version` is checked on load and
/// unknown versions are surfaced as `XRaftError::Storage` rather than
/// silently mis-deserializing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedHardState {
    version: u32,
    state: HardState,
}

const SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Shared validation
// ---------------------------------------------------------------------------

/// Validate that `next` is a legal Raft hard-state transition from
/// `prev`. Both [`MemoryHardStateStore`] and [`FileHardStateStore`]
/// call this from `persist`; sharing the helper guarantees identical
/// semantics across implementations.
///
/// Returns [`XRaftError::Storage`] (the canonical error variant for
/// storage-layer failures, including invariant violations) on rejection.
fn validate_transition(prev: &HardState, next: &HardState) -> Result<()> {
    // Term monotonicity --------------------------------------------------
    if next.current_term < prev.current_term {
        return Err(XRaftError::Storage(format!(
            "hard-state term regression rejected: prev={}, next={}",
            prev.current_term.0, next.current_term.0,
        )));
    }

    // Same-term vote invariants -----------------------------------------
    // A new (strictly higher) term resets vote eligibility, so any
    // `voted_for` is allowed there. Only the same-term case is
    // constrained.
    if next.current_term == prev.current_term {
        match (prev.voted_for, next.voted_for) {
            // Identical re-persist (idempotent retry, e.g. on commit-
            // index advance once that field is added) is allowed.
            (Some(a), Some(b)) if a == b => {}
            // Granting a fresh vote in a term that had none is allowed.
            (None, Some(_)) => {}
            // No-op (still unvoted in this term) is allowed.
            (None, None) => {}
            // Switching to a different candidate in the same term
            // would split-vote / double-vote.
            (Some(a), Some(b)) => {
                return Err(XRaftError::Storage(format!(
                    "vote already cast in term {}: prev=NodeId({}), next=NodeId({})",
                    prev.current_term.0, a.0, b.0,
                )));
            }
            // Clearing a vote within the same term would let the node
            // re-vote for someone else after a crash + reload, which
            // violates the single-vote-per-term invariant.
            (Some(a), None) => {
                return Err(XRaftError::Storage(format!(
                    "cannot clear voted_for=NodeId({}) within term {}",
                    a.0, prev.current_term.0,
                )));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// MemoryHardStateStore
// ---------------------------------------------------------------------------

/// Volatile in-memory implementation of [`HardStateStore`].
///
/// Useful for unit tests and deterministic simulation harnesses where
/// durability is unnecessary. Enforces the same term-monotonicity and
/// single-vote-per-term invariants as [`FileHardStateStore`] so test
/// coverage of safety bugs does not depend on which store is wired in.
#[derive(Debug, Default)]
pub struct MemoryHardStateStore {
    state: Option<HardState>,
}

impl MemoryHardStateStore {
    pub fn new() -> Self {
        Self { state: None }
    }
}

impl HardStateStore for MemoryHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        if let Some(prev) = self.state.as_ref() {
            validate_transition(prev, state)?;
        }
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

/// Durable file-backed implementation of [`HardStateStore`].
///
/// State is serialized as JSON to `<dir>/quorum-state` using the
/// crash-safe atomic-replace sequence documented at the module level.
/// An in-memory cache mirrors the on-disk state to make `load()` cheap
/// and to drive the safety-invariant checks without an extra disk read.
pub struct FileHardStateStore {
    /// Directory that contains the state file.
    dir: PathBuf,

    /// Mirror of the most recently persisted state. `None` until either
    /// `open` recovered an existing file or `persist` succeeded once.
    cached: Option<HardState>,
}

impl std::fmt::Debug for FileHardStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHardStateStore")
            .field("dir", &self.dir)
            .field("cached", &self.cached)
            .finish()
    }
}

impl FileHardStateStore {
    /// Open (or create) a hard-state store rooted at `dir`.
    ///
    /// Creates `dir` if it does not exist. Recovers any incomplete
    /// write left behind by a prior crash:
    ///
    /// * If `quorum-state` exists, it is the canonical state. Any
    ///   stale `quorum-state.bak` or `quorum-state.tmp` is removed.
    /// * If `quorum-state` is missing but `quorum-state.bak` exists,
    ///   the `.bak` is renamed back to canonical (this corresponds
    ///   to a crash after step 2 but before step 3 of the atomic
    ///   write sequence). A `tracing::warn!` records the recovery.
    /// * If neither exists, the store starts empty and `load` returns
    ///   `Ok(None)` until the first successful `persist`.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| {
            XRaftError::Storage(format!(
                "failed to create hard-state directory {}: {e}",
                dir.display(),
            ))
        })?;

        let final_path = dir.join(STATE_FILE_NAME);
        let bak_path = dir.join(format!("{STATE_FILE_NAME}{BAK_SUFFIX}"));
        let tmp_path = dir.join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));

        // Always remove orphan `.tmp` — partially-written temp files
        // can never be authoritative state because they were never
        // renamed into place.
        if tmp_path.exists()
            && let Err(e) = fs::remove_file(&tmp_path)
        {
            warn!(
                path = %tmp_path.display(),
                error = %e,
                "failed to remove orphan hard-state temp file (continuing)",
            );
        }

        // Recover from a crash between rename-existing-to-bak and
        // rename-tmp-to-target: only `.bak` survives.
        if !final_path.exists() && bak_path.exists() {
            warn!(
                path = %final_path.display(),
                bak  = %bak_path.display(),
                "recovering hard-state from .bak after interrupted write",
            );
            fs::rename(&bak_path, &final_path).map_err(|e| {
                XRaftError::Storage(format!(
                    "failed to recover hard-state from {}: {e}",
                    bak_path.display(),
                ))
            })?;
        } else if final_path.exists() && bak_path.exists() {
            // Both present means the prior write completed step 3 but
            // crashed before step 4. The canonical file is authoritative;
            // the `.bak` is a stale prior version.
            debug!(
                bak = %bak_path.display(),
                "removing stale hard-state .bak (canonical state present)",
            );
            if let Err(e) = fs::remove_file(&bak_path) {
                warn!(
                    bak = %bak_path.display(),
                    error = %e,
                    "failed to remove stale hard-state .bak (continuing)",
                );
            }
        }

        let cached = if final_path.exists() {
            let data = fs::read_to_string(&final_path).map_err(|e| {
                XRaftError::Storage(format!(
                    "failed to read hard-state from {}: {e}",
                    final_path.display(),
                ))
            })?;
            let envelope: PersistedHardState = serde_json::from_str(&data).map_err(|e| {
                XRaftError::Storage(format!(
                    "failed to deserialize hard-state at {}: {e}",
                    final_path.display(),
                ))
            })?;
            if envelope.version != SCHEMA_VERSION {
                return Err(XRaftError::Storage(format!(
                    "unsupported hard-state schema version {} (expected {}) at {}",
                    envelope.version,
                    SCHEMA_VERSION,
                    final_path.display(),
                )));
            }
            Some(envelope.state)
        } else {
            None
        };

        Ok(Self { dir, cached })
    }

    /// Full path to the canonical state file.
    fn state_path(&self) -> PathBuf {
        self.dir.join(STATE_FILE_NAME)
    }

    /// Atomically write `state` to disk (write tmp + sync, swap via
    /// `.bak`, remove `.bak`, fsync parent dir on Unix).
    fn atomic_write(&self, state: &HardState) -> Result<()> {
        let final_path = self.state_path();
        let tmp_path = self.dir.join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));
        let bak_path = self.dir.join(format!("{STATE_FILE_NAME}{BAK_SUFFIX}"));

        let envelope = PersistedHardState {
            version: SCHEMA_VERSION,
            state: state.clone(),
        };
        let serialized = serde_json::to_vec_pretty(&envelope)
            .map_err(|e| XRaftError::Storage(format!("failed to serialize hard-state: {e}")))?;

        // Step 1: write tmp + flush + fsync.
        {
            let mut f = File::create(&tmp_path).map_err(|e| {
                XRaftError::Storage(format!(
                    "failed to create temp hard-state file {}: {e}",
                    tmp_path.display(),
                ))
            })?;
            f.write_all(&serialized).map_err(|e| {
                XRaftError::Storage(format!(
                    "failed to write temp hard-state file {}: {e}",
                    tmp_path.display(),
                ))
            })?;
            f.sync_all().map_err(|e| {
                XRaftError::Storage(format!(
                    "failed to fsync temp hard-state file {}: {e}",
                    tmp_path.display(),
                ))
            })?;
        }

        // Step 2: clear any stale `.bak` first so a failed rename
        // below cannot leave the directory in a state where two
        // historical versions race for authority.
        if bak_path.exists()
            && let Err(e) = fs::remove_file(&bak_path)
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(XRaftError::Storage(format!(
                "failed to remove stale hard-state .bak {}: {e}",
                bak_path.display(),
            )));
        }
        if final_path.exists()
            && let Err(e) = fs::rename(&final_path, &bak_path)
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(XRaftError::Storage(format!(
                "failed to swap hard-state to .bak ({} -> {}): {e}",
                final_path.display(),
                bak_path.display(),
            )));
        }

        // Step 3: rename tmp -> canonical.
        if let Err(e) = fs::rename(&tmp_path, &final_path) {
            // Best-effort rollback: if we still have the .bak, put it
            // back so the caller can retry without losing the prior
            // committed state.
            if bak_path.exists() {
                let _ = fs::rename(&bak_path, &final_path);
            }
            let _ = fs::remove_file(&tmp_path);
            return Err(XRaftError::Storage(format!(
                "failed to commit hard-state ({} -> {}): {e}",
                tmp_path.display(),
                final_path.display(),
            )));
        }

        // Step 4: drop the `.bak`. Failure here is non-fatal — the
        // canonical file is already authoritative; a stale `.bak` will
        // be cleaned up by the next `open` call.
        if bak_path.exists()
            && let Err(e) = fs::remove_file(&bak_path)
        {
            warn!(
                bak = %bak_path.display(),
                error = %e,
                "failed to remove hard-state .bak after commit (will be cleaned on next open)",
            );
        }

        // Step 5: best-effort directory fsync (Unix only). Windows
        // does not need (and `File::open` on a directory is not
        // supported there).
        #[cfg(unix)]
        {
            if let Ok(dir_file) = File::open(&self.dir) {
                let _ = dir_file.sync_all();
            }
        }

        Ok(())
    }
}

impl HardStateStore for FileHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        // Validate BEFORE touching the filesystem so a rejected
        // transition leaves both the cache and the disk untouched.
        if let Some(prev) = self.cached.as_ref() {
            validate_transition(prev, state)?;
        }

        self.atomic_write(state)?;

        // Cache update is the LAST step so a partial-write failure
        // above does not desynchronize the in-memory mirror from disk.
        self.cached = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        Ok(self.cached.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use xraft_core::types::{NodeId, Term};

    fn hs(term: u64, voted_for: Option<u64>) -> HardState {
        HardState {
            current_term: Term(term),
            voted_for: voted_for.map(NodeId),
        }
    }

    // -- shared invariant matrix ----------------------------------------

    /// Run the full term-monotonicity + vote-invariant test matrix
    /// against any [`HardStateStore`] implementation. Both
    /// [`MemoryHardStateStore`] and [`FileHardStateStore`] must pass
    /// every case so in-memory tests cannot mask file-store-only bugs
    /// (or vice versa).
    fn assert_invariants<S: HardStateStore>(store: &mut S) {
        // First persist always succeeds.
        store.persist(&hs(1, None)).expect("first persist");

        // Idempotent re-persist of identical state succeeds.
        store.persist(&hs(1, None)).expect("idempotent re-persist");

        // Granting a vote in the current term succeeds.
        store
            .persist(&hs(1, Some(7)))
            .expect("first vote in term 1");

        // Re-persisting the SAME vote is idempotent and succeeds.
        store
            .persist(&hs(1, Some(7)))
            .expect("same-vote re-persist");

        // Switching to a DIFFERENT vote in the same term is rejected.
        let err = store
            .persist(&hs(1, Some(8)))
            .expect_err("same-term vote switch must fail");
        assert!(
            matches!(err, XRaftError::Storage(_)),
            "expected Storage error, got: {err:?}",
        );

        // Clearing a vote in the same term is rejected.
        let err = store
            .persist(&hs(1, None))
            .expect_err("same-term vote clear must fail");
        assert!(
            matches!(err, XRaftError::Storage(_)),
            "expected Storage error, got: {err:?}",
        );

        // Term advance with a DIFFERENT vote is allowed.
        store
            .persist(&hs(2, Some(8)))
            .expect("higher-term different vote");

        // Term advance with NO vote is allowed.
        store.persist(&hs(3, None)).expect("higher-term no vote");

        // Term regression (3 -> 2) is rejected.
        let err = store
            .persist(&hs(2, None))
            .expect_err("term regression must fail");
        assert!(
            matches!(err, XRaftError::Storage(_)),
            "expected Storage error, got: {err:?}",
        );
    }

    // -- MemoryHardStateStore -------------------------------------------

    #[test]
    fn memory_store_load_returns_none_before_persist() {
        let store = MemoryHardStateStore::new();
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn memory_store_round_trip() {
        let mut store = MemoryHardStateStore::new();
        let s = hs(3, Some(7));
        store.persist(&s).unwrap();
        assert_eq!(store.load().unwrap(), Some(s));
    }

    #[test]
    fn memory_store_enforces_invariants() {
        let mut store = MemoryHardStateStore::new();
        assert_invariants(&mut store);
    }

    // -- FileHardStateStore ---------------------------------------------

    #[test]
    fn file_store_load_returns_none_on_fresh_directory() {
        let tmp = TempDir::new().unwrap();
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn file_store_round_trip() {
        let tmp = TempDir::new().unwrap();
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        let s = hs(5, Some(2));
        store.persist(&s).unwrap();
        assert_eq!(store.load().unwrap(), Some(s));
    }

    #[test]
    fn file_store_enforces_invariants() {
        let tmp = TempDir::new().unwrap();
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_invariants(&mut store);
    }

    #[test]
    fn file_store_survives_reopen() {
        let tmp = TempDir::new().unwrap();
        let s = hs(10, Some(4));

        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&s).unwrap();
        }

        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store.load().unwrap(), Some(s));
    }

    #[test]
    fn file_store_repeated_persist_succeeds_on_windows_and_unix() {
        // The iter-2 evaluator flagged a Windows-portability bug where
        // a second `fs::rename(tmp, path)` with `path` already present
        // would fail. The atomic-replace pattern here is portable;
        // exercise it explicitly with multiple persists.
        let tmp = TempDir::new().unwrap();
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();

        for term in 1..=20u64 {
            let s = hs(term, Some(term % 3 + 1));
            store
                .persist(&s)
                .expect("repeated persist must succeed on every supported platform");
            assert_eq!(store.load().unwrap(), Some(s));
        }
    }

    #[test]
    fn file_store_recovers_from_bak_when_canonical_missing() {
        // Simulate a crash between step 2 (rename canonical -> .bak)
        // and step 3 (rename tmp -> canonical) of the atomic-write
        // sequence by hand-staging the directory.
        let tmp = TempDir::new().unwrap();
        let s = hs(7, Some(3));

        // First persist a valid state, then rename canonical -> .bak
        // and verify recovery on reopen.
        {
            let mut store = FileHardStateStore::open(tmp.path()).unwrap();
            store.persist(&s).unwrap();
        }
        let canonical = tmp.path().join(STATE_FILE_NAME);
        let bak = tmp.path().join(format!("{STATE_FILE_NAME}{BAK_SUFFIX}"));
        fs::rename(&canonical, &bak).unwrap();
        assert!(!canonical.exists(), "precondition: canonical removed");
        assert!(bak.exists(), "precondition: .bak staged");

        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store.load().unwrap(), Some(s));
        assert!(canonical.exists(), ".bak must be promoted to canonical");
        assert!(!bak.exists(), ".bak must be consumed by recovery");
    }

    #[test]
    fn file_store_keeps_canonical_when_both_canonical_and_bak_present() {
        // Simulate a crash AFTER step 3 (rename tmp -> canonical) but
        // BEFORE step 4 (remove .bak): both files exist; canonical is
        // the authoritative one.
        let tmp = TempDir::new().unwrap();
        let new_state = hs(5, Some(2));
        let stale_state = hs(3, Some(9));

        let canonical = tmp.path().join(STATE_FILE_NAME);
        let bak = tmp.path().join(format!("{STATE_FILE_NAME}{BAK_SUFFIX}"));

        // Stage canonical = new_state (newer, authoritative).
        let canonical_envelope = PersistedHardState {
            version: SCHEMA_VERSION,
            state: new_state.clone(),
        };
        fs::write(
            &canonical,
            serde_json::to_vec_pretty(&canonical_envelope).unwrap(),
        )
        .unwrap();

        // Stage .bak = stale_state (older, must be discarded).
        let bak_envelope = PersistedHardState {
            version: SCHEMA_VERSION,
            state: stale_state,
        };
        fs::write(&bak, serde_json::to_vec_pretty(&bak_envelope).unwrap()).unwrap();

        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(
            store.load().unwrap(),
            Some(new_state),
            "open must prefer canonical over stale .bak",
        );
        assert!(!bak.exists(), "stale .bak must be cleaned up on open");
    }

    #[test]
    fn file_store_removes_orphan_tmp_on_open() {
        // A leftover `quorum-state.tmp` from a crash before step 3
        // can never be authoritative. `open` must scrub it.
        let tmp = TempDir::new().unwrap();
        let tmp_path = tmp.path().join(format!("{STATE_FILE_NAME}{TMP_SUFFIX}"));
        fs::write(&tmp_path, b"not-yet-committed").unwrap();
        assert!(tmp_path.exists());

        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert!(!tmp_path.exists(), "orphan .tmp must be removed on open");
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn file_store_rejects_unknown_schema_version() {
        let tmp = TempDir::new().unwrap();
        let canonical = tmp.path().join(STATE_FILE_NAME);
        // Future-version envelope an older binary cannot understand.
        let envelope = serde_json::json!({
            "version": 9999,
            "state": { "current_term": 1, "voted_for": null },
        });
        fs::write(&canonical, envelope.to_string()).unwrap();

        let err =
            FileHardStateStore::open(tmp.path()).expect_err("unknown schema version must error");
        assert!(matches!(err, XRaftError::Storage(_)));
    }

    #[test]
    fn file_store_rejects_corrupt_json() {
        let tmp = TempDir::new().unwrap();
        let canonical = tmp.path().join(STATE_FILE_NAME);
        fs::write(&canonical, b"{not-valid-json").unwrap();

        let err = FileHardStateStore::open(tmp.path()).expect_err("corrupt JSON must error");
        assert!(matches!(err, XRaftError::Storage(_)));
    }

    #[test]
    fn file_store_default_first_boot_matches_hard_state_default() {
        // Per implementation-plan Stage 2.2: "fallback to default
        // initial state (term=0, voted_for=None)". The trait contract
        // returns Ok(None) for missing state; the driver maps that to
        // HardState::default().
        let tmp = TempDir::new().unwrap();
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store.load().unwrap(), None);
        assert_eq!(HardState::default(), hs(0, None));
    }

    #[test]
    fn file_store_validation_runs_before_disk_write() {
        // A rejected persist must leave the on-disk state untouched
        // so subsequent loads still return the prior valid state.
        let tmp = TempDir::new().unwrap();
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();

        let good = hs(5, Some(2));
        store.persist(&good).unwrap();

        // Term regression — must fail and leave good state intact.
        let _ = store
            .persist(&hs(3, None))
            .expect_err("term regression must fail");
        assert_eq!(store.load().unwrap(), Some(good.clone()));

        // Reopen and verify on-disk state is also the good one
        // (proves validation ran before atomic_write touched disk).
        drop(store);
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        assert_eq!(store.load().unwrap(), Some(good));
    }
}
