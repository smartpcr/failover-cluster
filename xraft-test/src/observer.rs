//! [`TestObserver`] — minimal [`DriverObserver`] used by the simulated
//! cluster harness to expose the current [`NodeStatus`] of each node
//! and to count snapshot / compaction events for assertion in tests.
//!
//! The production [`XRaftMetrics`](xraft_server::XRaftMetrics) observer
//! requires a Prometheus `Registry` and a full
//! [`StatusPublisher`](xraft_server::StatusPublisher) wiring; for the
//! Stage 8.1 multi-node integration tests we only need to know each
//! node's current role / term / commit-index and how many times the
//! snapshot/log-compaction hooks fired, so we keep that minimal
//! surface (an atomic-counter copy alongside the latest status) and
//! skip the metric registries.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::Notify;

use xraft_core::types::NodeId;
use xraft_server::{DriverObserver, NodeStatus};

/// Per-node observer that captures the latest [`NodeStatus`] handed in
/// by the driver after each event-loop iteration, plus monotonic
/// counters for the Stage 7.3 snapshot / log-compaction hooks
/// (`on_snapshot_taken`, `on_snapshot_installed`, `on_log_compacted`).
#[derive(Debug)]
pub struct TestObserver {
    inner: Arc<Mutex<Option<NodeStatus>>>,
    node_id: NodeId,
    // Atomic counters so the DriverObserver callbacks
    // (which are synchronous `&self` methods) can record without
    // blocking on a tokio::sync::Mutex. `Relaxed` is sufficient — we
    // only need monotonic visibility, not cross-event ordering.
    snapshots_taken: Arc<AtomicU64>,
    snapshots_installed: Arc<AtomicU64>,
    log_compactions: Arc<AtomicU64>,
    snapshot_bytes_total: Arc<AtomicU64>,
    log_entries_compacted_total: Arc<AtomicU64>,
    /// Bumped on every `on_status` so the
    /// event-driven
    /// [`crate::simulated::SimulatedCluster::await_leader`] wait wakes
    /// the instant the driver publishes a new status (role / term /
    /// leader_id transition). One notify is shared across every node
    /// in a cluster so the wait wakes on ANY node's transition.
    state_change: Arc<Notify>,
}

/// Shared inspection handle for a [`TestObserver`].
#[derive(Debug, Clone)]
pub struct TestObserverHandle {
    inner: Arc<Mutex<Option<NodeStatus>>>,
    node_id: NodeId,
    snapshots_taken: Arc<AtomicU64>,
    snapshots_installed: Arc<AtomicU64>,
    log_compactions: Arc<AtomicU64>,
    snapshot_bytes_total: Arc<AtomicU64>,
    log_entries_compacted_total: Arc<AtomicU64>,
}

impl TestObserver {
    /// Build a fresh observer for `node_id` with its OWN notify
    /// (suitable for standalone use; cluster paths should prefer
    /// [`Self::with_state_change`] to share one notify across nodes).
    pub fn new(node_id: NodeId) -> Self {
        Self::with_state_change(node_id, Arc::new(Notify::new()))
    }

    /// Build an observer that shares `state_change` with peers — used
    /// by [`crate::simulated::SimulatedCluster`] so the
    /// cluster-level event-driven `await_leader` loop wakes on ANY
    /// node's status transition.
    pub fn with_state_change(node_id: NodeId, state_change: Arc<Notify>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            node_id,
            snapshots_taken: Arc::new(AtomicU64::new(0)),
            snapshots_installed: Arc::new(AtomicU64::new(0)),
            log_compactions: Arc::new(AtomicU64::new(0)),
            snapshot_bytes_total: Arc::new(AtomicU64::new(0)),
            log_entries_compacted_total: Arc::new(AtomicU64::new(0)),
            state_change,
        }
    }

    /// Borrow an inspection handle. Cheap clone.
    pub fn handle(&self) -> TestObserverHandle {
        TestObserverHandle {
            inner: self.inner.clone(),
            node_id: self.node_id,
            snapshots_taken: self.snapshots_taken.clone(),
            snapshots_installed: self.snapshots_installed.clone(),
            log_compactions: self.log_compactions.clone(),
            snapshot_bytes_total: self.snapshot_bytes_total.clone(),
            log_entries_compacted_total: self.log_entries_compacted_total.clone(),
        }
    }
}

impl TestObserverHandle {
    /// The node id this handle is tracking.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Return the latest observed status, if any has been
    /// captured yet.
    pub async fn status(&self) -> Option<NodeStatus> {
        *self.inner.lock().await
    }

    /// Number of times the driver invoked `on_snapshot_taken` on this
    /// node (i.e. successful local snapshot writes).
    pub fn snapshots_taken(&self) -> u64 {
        self.snapshots_taken.load(Ordering::Relaxed)
    }

    /// Number of times the driver invoked `on_snapshot_installed` on
    /// this node (i.e. successful follower-side snapshot installs).
    pub fn snapshots_installed(&self) -> u64 {
        self.snapshots_installed.load(Ordering::Relaxed)
    }

    /// Number of `Action::TruncateLog::PrefixThroughInclusive`
    /// compactions the driver performed on this node.
    pub fn log_compactions(&self) -> u64 {
        self.log_compactions.load(Ordering::Relaxed)
    }

    /// Cumulative byte size of every snapshot taken on this node
    /// (sum of the `bytes` argument across `on_snapshot_taken` calls).
    pub fn snapshot_bytes_total(&self) -> u64 {
        self.snapshot_bytes_total.load(Ordering::Relaxed)
    }

    /// Cumulative number of log entries reclaimed by compactions on
    /// this node (sum of the `removed` argument across
    /// `on_log_compacted` calls).
    pub fn log_entries_compacted_total(&self) -> u64 {
        self.log_entries_compacted_total.load(Ordering::Relaxed)
    }
}

impl DriverObserver for TestObserver {
    fn on_status<'a>(
        &'a self,
        status: NodeStatus,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let slot = self.inner.clone();
        let notify = self.state_change.clone();
        Box::pin(async move {
            {
                let mut g = slot.lock().await;
                *g = Some(status);
            }
            // Wake the cluster-level
            // event-driven await loops the moment any node publishes
            // a new status. Released the mutex first so a woken
            // waiter doesn't immediately contend with us.
            notify.notify_waiters();
        })
    }

    fn on_append(&self, _n: u64) {}

    fn on_election_won(&self, _elapsed: Duration) {}

    fn on_snapshot_taken(&self, bytes: u64, _elapsed: Duration) {
        self.snapshots_taken.fetch_add(1, Ordering::Relaxed);
        self.snapshot_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn on_snapshot_installed(&self) {
        self.snapshots_installed.fetch_add(1, Ordering::Relaxed);
    }

    fn on_log_compacted(&self, removed: u64) {
        self.log_compactions.fetch_add(1, Ordering::Relaxed);
        self.log_entries_compacted_total
            .fetch_add(removed, Ordering::Relaxed);
    }
}
