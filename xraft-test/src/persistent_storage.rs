//! Shared, clone-able wrappers around the in-memory Raft storage
//! implementations so a [`SimulatedCluster`](crate::SimulatedCluster)
//! can preserve a node's durable Raft state across
//! [`kill`](crate::SimulatedCluster::kill) →
//! [`revive`](crate::SimulatedCluster::revive) — i.e. model a process
//! crash + reboot where the WAL, hard-state file, and snapshot store
//! all survive on disk (which is the *normal* process-restart shape
//! a Raft replica is expected to handle).
//!
//! # Why this exists
//!
//! Stage 8.2's chaos engine emits [`ChaosFault::KillRestart(node)`]
//! events that abort the driver task and respawn a fresh driver
//! against the same `NodeId`. The production
//! [`MemoryLogStore`](xraft_storage::MemoryLogStore),
//! [`MemoryHardStateStore`](xraft_storage::MemoryHardStateStore), and
//! [`MemorySnapshotStore`](xraft_storage::MemorySnapshotStore) are
//! passed BY VALUE into the [`Driver`](xraft_server::Driver) at
//! construction time and dropped along with the driver — so a naive
//! kill+revive would lose the WAL, the persisted term/vote/voter-set,
//! and any snapshots. That breaks the Raft safety invariant "a voter
//! that voted for term T cannot vote again in term T after restart"
//! and exercises a STRICTLY HARDER failure mode than the one Stage
//! 8.2 is asking us to validate.
//!
//! Each `Shared*Store` here wraps an `Arc<Mutex<MemoryXxxStore>>`.
//! The cluster keeps one clone of the trio per node; the driver gets
//! another clone. When the driver is aborted, the cluster's clones
//! retain the underlying state, and a subsequent `revive(node)`
//! hands fresh clones (pointing at the SAME inner state) to a newly
//! spawned driver — the production "preserve disk across restart"
//! shape.
//!
//! All three types are `Send + Sync + 'static + Clone`, which
//! satisfies the bounds the
//! [`Driver`](xraft_server::Driver)`<T, L, HS, SS, SM>` generic
//! parameters require.
//!
//! # Why a `std::sync::Mutex` (not `tokio::sync`)
//!
//! The storage trait methods are synchronous (`fn append(&mut self, …)`),
//! so the driver invokes them from within a `tokio::task` without
//! `.await`. A `std::sync::Mutex` is the right primitive here — the
//! critical section is a single vector push / read, which is bounded
//! and uncontended in the single-driver-per-node simulated harness.
//!
//! [`ChaosFault::KillRestart(node)`]: crate::chaos::ChaosFault::KillRestart

use std::sync::{Arc, Mutex};

use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::message::Entry;
use xraft_core::storage::{HardState, HardStateStore, LogStore, SnapshotMeta, SnapshotStore};
use xraft_core::types::{LogIndex, Term, VoterSet};

use xraft_storage::{MemoryHardStateStore, MemoryLogStore, MemorySnapshotStore};

// ---------------------------------------------------------------------------
// SharedMemoryLogStore
// ---------------------------------------------------------------------------

/// Clone-able wrapper around [`MemoryLogStore`] that survives driver
/// kill+revive cycles. See the [module-level docs](self) for the
/// rationale.
#[derive(Clone, Default, Debug)]
pub struct SharedMemoryLogStore {
    inner: Arc<Mutex<MemoryLogStore>>,
}

impl SharedMemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

fn poisoned_log() -> XRaftError {
    XRaftError::Storage("SharedMemoryLogStore mutex poisoned".into())
}
fn poisoned_hs() -> XRaftError {
    XRaftError::Storage("SharedMemoryHardStateStore mutex poisoned".into())
}
fn poisoned_snap() -> XRaftError {
    XRaftError::Storage("SharedMemorySnapshotStore mutex poisoned".into())
}

impl LogStore for SharedMemoryLogStore {
    fn append(&mut self, entries: &[Entry]) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.append(entries)
    }
    fn get(&self, index: LogIndex) -> XResult<Option<Entry>> {
        let g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.get(index)
    }
    fn get_range(&self, start: LogIndex, end: LogIndex) -> XResult<Vec<Entry>> {
        let g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.get_range(start, end)
    }
    fn last_index(&self) -> LogIndex {
        match self.inner.lock() {
            Ok(g) => g.last_index(),
            Err(_) => LogIndex(0),
        }
    }
    fn last_term(&self) -> Term {
        match self.inner.lock() {
            Ok(g) => g.last_term(),
            Err(_) => Term(0),
        }
    }
    fn truncate_from(&mut self, index: LogIndex) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.truncate_from(index)
    }
    fn term_at(&self, index: LogIndex) -> XResult<Option<Term>> {
        let g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.term_at(index)
    }
    fn flush(&mut self) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.flush()
    }
    fn purge_prefix(&mut self, through_index_inclusive: LogIndex) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_log())?;
        g.purge_prefix(through_index_inclusive)
    }
}

// ---------------------------------------------------------------------------
// SharedMemoryHardStateStore
// ---------------------------------------------------------------------------

/// Clone-able wrapper around [`MemoryHardStateStore`]. Preserves
/// `current_term`, `voted_for`, `commit_index`, and the static voter
/// set across kill+revive.
#[derive(Clone, Default, Debug)]
pub struct SharedMemoryHardStateStore {
    inner: Arc<Mutex<MemoryHardStateStore>>,
}

impl SharedMemoryHardStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HardStateStore for SharedMemoryHardStateStore {
    fn persist(&mut self, state: &HardState) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_hs())?;
        g.persist(state)
    }
    fn load(&self) -> XResult<Option<HardState>> {
        let g = self.inner.lock().map_err(|_| poisoned_hs())?;
        g.load()
    }
    fn persist_voter_set(&mut self, voter_set: &VoterSet) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_hs())?;
        g.persist_voter_set(voter_set)
    }
    fn load_voter_set(&self) -> XResult<Option<VoterSet>> {
        let g = self.inner.lock().map_err(|_| poisoned_hs())?;
        g.load_voter_set()
    }
}

// ---------------------------------------------------------------------------
// SharedMemorySnapshotStore
// ---------------------------------------------------------------------------

/// Clone-able wrapper around [`MemorySnapshotStore`]. Preserves
/// installed snapshots across kill+revive so a follower that has been
/// brought up via `FetchSnapshot` does not have to re-fetch on reboot.
#[derive(Clone, Default, Debug)]
pub struct SharedMemorySnapshotStore {
    inner: Arc<Mutex<MemorySnapshotStore>>,
}

impl SharedMemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for SharedMemorySnapshotStore {
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_snap())?;
        g.save_snapshot(metadata, data)
    }
    fn load_latest_snapshot(&self) -> XResult<Option<(SnapshotMeta, Vec<u8>)>> {
        let g = self.inner.lock().map_err(|_| poisoned_snap())?;
        g.load_latest_snapshot()
    }
    fn load_snapshot(
        &self,
        index: LogIndex,
        term: Term,
    ) -> XResult<Option<(SnapshotMeta, Vec<u8>)>> {
        let g = self.inner.lock().map_err(|_| poisoned_snap())?;
        g.load_snapshot(index, term)
    }
    fn list_snapshots(&self) -> XResult<Vec<SnapshotMeta>> {
        let g = self.inner.lock().map_err(|_| poisoned_snap())?;
        g.list_snapshots()
    }
    fn delete_snapshot(&mut self, id: &str) -> XResult<()> {
        let mut g = self.inner.lock().map_err(|_| poisoned_snap())?;
        g.delete_snapshot(id)
    }
    fn snapshot_exists(&self, index: LogIndex, term: Term) -> bool {
        match self.inner.lock() {
            Ok(g) => g.snapshot_exists(index, term),
            Err(_) => false,
        }
    }
    fn find_by_id(&self, id: &str) -> XResult<Option<SnapshotMeta>> {
        let g = self.inner.lock().map_err(|_| poisoned_snap())?;
        g.find_by_id(id)
    }
    // NOTE: we intentionally do NOT override `snapshot_reader` /
    // `snapshot_reader_from_offset`. The trait's default
    // implementations call `load_snapshot` (which we DO delegate),
    // so the streaming reader works correctly through the wrapper.
}

// ---------------------------------------------------------------------------
// PersistentNodeStorage — bundle held by SimulatedCluster per node
// ---------------------------------------------------------------------------

/// All three shared storage handles for one simulated node. Cloned
/// once at spawn (one copy goes to the driver, one stays in the
/// cluster). On `revive(node)` the cluster fetches its retained copy,
/// clones it again for the new driver, and the underlying `Arc<Mutex>`
/// chain ensures the new driver sees the EXACT durable state the
/// previous driver wrote before being aborted.
#[derive(Clone, Default, Debug)]
pub struct PersistentNodeStorage {
    pub log: SharedMemoryLogStore,
    pub hard_state: SharedMemoryHardStateStore,
    pub snapshot: SharedMemorySnapshotStore,
}

impl PersistentNodeStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// Tests — basic sanity that the wrappers preserve state across clones
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use xraft_core::message::{Entry, EntryPayload};
    use xraft_core::types::{LogIndex, NodeId, Term};

    fn cmd_entry(idx: u64, term: u64) -> Entry {
        Entry {
            index: LogIndex(idx),
            term: Term(term),
            payload: EntryPayload::Command(Bytes::from_static(b"x")),
        }
    }

    #[test]
    fn shared_log_preserves_across_clone() {
        let mut a = SharedMemoryLogStore::new();
        a.append(&[cmd_entry(1, 1)]).unwrap();
        let b = a.clone();
        // Drop the "first driver"'s handle — the underlying state must
        // still be visible to the second clone.
        drop(a);
        assert_eq!(b.last_index(), LogIndex(1));
        assert_eq!(b.last_term(), Term(1));
        assert_eq!(b.get(LogIndex(1)).unwrap().unwrap().term, Term(1));
    }

    #[test]
    fn shared_hard_state_preserves_term_across_clone() {
        let mut a = SharedMemoryHardStateStore::new();
        let hs = HardState {
            current_term: Term(7),
            voted_for: Some(NodeId(2)),
            commit_index: LogIndex(3),
        };
        a.persist(&hs).unwrap();

        let b = a.clone();
        drop(a);
        let loaded_hs = b.load().unwrap().expect("hard state survived");
        assert_eq!(loaded_hs.current_term, Term(7));
        assert_eq!(loaded_hs.voted_for, Some(NodeId(2)));
        assert_eq!(loaded_hs.commit_index, LogIndex(3));
    }
}
