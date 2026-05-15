//! Test-only helpers shared between [`crate::driver`] and [`crate::server`]
//! unit tests.
//!
//! Gated behind `#[cfg(test)]` at the module declaration site so the
//! helpers never enter the public API surface or production binary.
//! Visibility is `pub(crate)` so both test modules can reach them
//! without exporting test-only types from the crate.

use std::sync::{Arc, Mutex};

use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::storage::HardStateStore;
use xraft_core::types::{HardState, NodeId};

/// Build a deterministic 3-voter [`ClusterConfig`] rooted at `id`.
///
/// Constructed inline (no TOML parser dep) so tests stay independent of
/// the loader path. Election timer is tightened so tests that drive
/// ticks reach the candidate transition without burning CPU.
pub(crate) fn three_node_config(id: u64) -> ClusterConfig {
    let voters = vec![
        VoterConfig {
            node_id: 1,
            directory_id: "11111111-1111-1111-1111-111111111111".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6001,
        },
        VoterConfig {
            node_id: 2,
            directory_id: "22222222-2222-2222-2222-222222222222".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6002,
        },
        VoterConfig {
            node_id: 3,
            directory_id: "33333333-3333-3333-3333-333333333333".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6003,
        },
    ];
    ClusterConfig {
        node_id: NodeId(id),
        cluster_id: "test-cluster".to_string(),
        listen_addr: format!("127.0.0.1:600{id}"),
        peers: Vec::new(),
        voters,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        fetch_interval_ms: 50,
        tick_interval_ms: 10,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir: std::env::temp_dir(),
        snapshot_retention_count: 3,
    }
}

/// Build a single-voter [`ClusterConfig`].
///
/// A single-voter cluster is a quorum of one: once the election timer
/// expires the node becomes leader unilaterally. Used by tests that
/// need to drive the engine into leader role and observe leader-only
/// actions (e.g. `AppendEntries` for the no-op entry).
pub(crate) fn single_voter_config() -> ClusterConfig {
    let voters = vec![VoterConfig {
        node_id: 1,
        directory_id: "11111111-1111-1111-1111-111111111111".to_string(),
        host: "127.0.0.1".to_string(),
        port: 6001,
    }];
    ClusterConfig {
        node_id: NodeId(1),
        cluster_id: "single-voter-test".to_string(),
        listen_addr: "127.0.0.1:6001".to_string(),
        peers: Vec::new(),
        voters,
        election_timeout_min_ms: 50,
        election_timeout_max_ms: 100,
        fetch_interval_ms: 25,
        tick_interval_ms: 5,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir: std::env::temp_dir(),
        snapshot_retention_count: 3,
    }
}

/// [`HardStateStore`] wrapper that records every persist call against
/// a counter handle so tests can assert ordering ("persist completed
/// before step returned") without relying on wall-clock timestamps.
#[derive(Debug, Default)]
pub(crate) struct RecordingStore {
    inner: xraft_storage::MemoryHardStateStore,
    persist_count: Arc<Mutex<usize>>,
}

impl RecordingStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: xraft_storage::MemoryHardStateStore::new(),
            persist_count: Arc::new(Mutex::new(0)),
        }
    }
    pub(crate) fn persist_count_handle(&self) -> Arc<Mutex<usize>> {
        self.persist_count.clone()
    }
}

impl HardStateStore for RecordingStore {
    fn persist(&mut self, state: &HardState) -> XResult<()> {
        self.inner.persist(state)?;
        *self.persist_count.lock().unwrap() += 1;
        Ok(())
    }
    fn load(&self) -> XResult<Option<HardState>> {
        self.inner.load()
    }
}

/// [`HardStateStore`] whose `persist` always errors. Used to drive the
/// driver / server poisoning paths deterministically.
#[derive(Debug, Default)]
pub(crate) struct AlwaysFailPersistStore {
    pub(crate) load_value: Option<HardState>,
    persist_attempts: Arc<Mutex<Vec<HardState>>>,
}

impl AlwaysFailPersistStore {
    pub(crate) fn new() -> Self {
        Self {
            load_value: None,
            persist_attempts: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(crate) fn attempts_handle(&self) -> Arc<Mutex<Vec<HardState>>> {
        self.persist_attempts.clone()
    }
}

impl HardStateStore for AlwaysFailPersistStore {
    fn persist(&mut self, state: &HardState) -> XResult<()> {
        self.persist_attempts.lock().unwrap().push(state.clone());
        Err(XRaftError::Storage("simulated disk failure".into()))
    }
    fn load(&self) -> XResult<Option<HardState>> {
        Ok(self.load_value.clone())
    }
}

/// [`HardStateStore`] whose `load` always errors. Used to prove
/// startup-time storage failures propagate as `Storage{op:"load"}`.
#[derive(Debug, Default)]
pub(crate) struct LoadFailStore;

impl HardStateStore for LoadFailStore {
    fn persist(&mut self, _state: &HardState) -> XResult<()> {
        Ok(())
    }
    fn load(&self) -> XResult<Option<HardState>> {
        Err(XRaftError::Storage("simulated unreadable file".into()))
    }
}
