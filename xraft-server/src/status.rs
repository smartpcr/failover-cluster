//! Public snapshot of a [`RaftNode`](xraft_core::RaftNode)'s observable
//! state, plus the lock-free publisher the driver loop writes to on
//! every step.
//!
//! Stage 6.1 introduces this surface so the in-process HTTP admin
//! endpoint (`/health`, `/metrics`) can render the engine's role,
//! term, leader, and commit index without reaching into the driver's
//! private async state. The driver writes via
//! [`StatusPublisher::publish`]; the admin server reads via
//! [`StatusPublisher::current`].
//!
//! `StatusPublisher` is `Send + Sync + 'static` so it can be cloned
//! into a `tokio::spawn` task and into `axum::Router` handlers via
//! `Arc::clone`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::ser::SerializeStruct;
use tokio::sync::RwLock;

use xraft_core::types::{LogIndex, NodeId, NodeRole, Term};

/// Numeric encoding of [`NodeRole`] for the `xraft_role` Prometheus
/// gauge, per `architecture.md` §7 and `e2e-scenarios.md` Feature 15.
///
/// The mapping is fixed and MUST NOT change across releases — operator
/// dashboards and alerting rules embed the numeric values.
pub fn role_to_gauge(role: NodeRole) -> i64 {
    match role {
        NodeRole::Follower => 0,
        NodeRole::Candidate => 1,
        NodeRole::PreCandidate => 2,
        NodeRole::Leader => 3,
        NodeRole::Observer => 4,
    }
}

/// Stringly-typed role label used by the JSON `/health` payload. Keep
/// the values lowercase + dasherized so they line up with the engine's
/// own log fields and with the canonical `e2e-scenarios.md` glossary.
pub fn role_to_str(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Follower => "follower",
        NodeRole::Candidate => "candidate",
        NodeRole::PreCandidate => "pre-candidate",
        NodeRole::Leader => "leader",
        NodeRole::Observer => "observer",
    }
}

/// Volatile snapshot of a node's observable state.
///
/// Built by the driver via [`NodeStatus::from_engine`] at startup and
/// after every event-loop iteration. Cloned cheaply across threads
/// (everything is `Copy`-sized so the `Clone` is essentially a memcpy).
///
/// Custom [`Serialize`] impl renders `role` as the canonical
/// lowercase/dasherized string from [`role_to_str`] so the JSON
/// payload returned by `/health` is stable across releases and does
/// not leak the engine's internal `NodeRole` discriminant naming.
#[derive(Debug, Clone, Copy)]
pub struct NodeStatus {
    /// This node's unique id (the local `RaftNode::id`).
    pub node_id: u64,
    /// Role in the cluster at snapshot time.
    pub role: NodeRole,
    /// Persisted `current_term` at snapshot time.
    pub term: u64,
    /// Volatile `commit_index` at snapshot time.
    pub commit_index: u64,
    /// Volatile `last_applied` at snapshot time.
    pub last_applied: u64,
    /// The leader this node currently recognises, if any.
    pub leader_id: Option<u64>,
    /// Mirror of `RaftNode::last_log_index` (log end offset).
    pub last_log_index: u64,
}

impl Serialize for NodeStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("NodeStatus", 7)?;
        s.serialize_field("node_id", &self.node_id)?;
        s.serialize_field("role", role_to_str(self.role))?;
        s.serialize_field("term", &self.term)?;
        s.serialize_field("commit_index", &self.commit_index)?;
        s.serialize_field("last_applied", &self.last_applied)?;
        s.serialize_field("leader_id", &self.leader_id)?;
        s.serialize_field("last_log_index", &self.last_log_index)?;
        s.end()
    }
}

impl NodeStatus {
    /// Build a snapshot from a borrowed [`RaftNode`].
    ///
    /// The driver holds the only `&mut RaftNode` so callers may safely
    /// `&` into it from inside the loop. The borrow lifetime is local
    /// to the call — the returned `NodeStatus` is owned and `Copy`.
    pub fn from_engine(node: &xraft_core::RaftNode) -> Self {
        Self {
            node_id: node.id.0,
            role: node.role,
            term: node.hard_state.current_term.0,
            commit_index: node.commit_index.0,
            last_applied: node.last_applied.0,
            leader_id: node.leader_id.map(|n| n.0),
            last_log_index: node.last_log_index.0,
        }
    }

    /// Construct a placeholder status (everything zero, role
    /// `Follower`) for use before the driver has published its first
    /// snapshot. Surfaces a 503-equivalent JSON body until the engine
    /// reports.
    pub fn placeholder(node_id: NodeId) -> Self {
        Self {
            node_id: node_id.0,
            role: NodeRole::Follower,
            term: 0,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            last_log_index: 0,
        }
    }

    /// Re-derive a `NodeRole` enum from the snapshot.
    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// `Term` newtype accessor.
    pub fn term_typed(&self) -> Term {
        Term(self.term)
    }

    /// `LogIndex` newtype accessor for `commit_index`.
    pub fn commit_index_typed(&self) -> LogIndex {
        LogIndex(self.commit_index)
    }

    /// `NodeId` newtype accessor.
    pub fn node_id_typed(&self) -> NodeId {
        NodeId(self.node_id)
    }
}

/// Multi-reader / single-writer publisher for the latest
/// [`NodeStatus`].
///
/// Implemented over `RwLock<NodeStatus>` because the read path is a
/// single short copy. The driver writes on every event-loop iteration
/// (cadence: tens of microseconds in the happy path), and the admin
/// server reads only on inbound HTTP scrapes (cadence: seconds). A
/// `parking_lot::RwLock` or `arc_swap::ArcSwap` would be marginally
/// faster but neither is in the workspace dep graph; the contention
/// is negligible at the published cadence.
///
/// A separate `appended_entries` atomic counter is exposed for the
/// `xraft_append_records_total` Prometheus counter — it must be
/// monotonic across snapshots so we keep it in an `AtomicU64` rather
/// than rebuilding from the publish stream.
pub struct StatusPublisher {
    latest: RwLock<NodeStatus>,
    appended_entries: AtomicU64,
    /// Monotonic counter incremented every time the operator triggers
    /// a successful config reload (SIGHUP). Exposed via `/health` so
    /// operators have observable proof the reload signal actually
    /// reached and was applied by the server — distinct from "the
    /// signal handler was scheduled but the engine did not reapply
    /// any state". Starts at 0 and only ever increases.
    config_revision: AtomicU64,
}

impl std::fmt::Debug for StatusPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatusPublisher")
            .field(
                "appended_entries",
                &self.appended_entries.load(Ordering::Relaxed),
            )
            .field(
                "config_revision",
                &self.config_revision.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl StatusPublisher {
    /// Construct an `Arc<StatusPublisher>` seeded with a placeholder
    /// status for `node_id`. The driver overwrites it on first poll.
    pub fn new(node_id: NodeId) -> Arc<Self> {
        Arc::new(Self {
            latest: RwLock::new(NodeStatus::placeholder(node_id)),
            appended_entries: AtomicU64::new(0),
            config_revision: AtomicU64::new(0),
        })
    }

    /// Construct a publisher seeded with the supplied status. Used
    /// when the driver knows the engine's state up front (e.g. after
    /// recovery from durable state).
    pub fn from_status(status: NodeStatus) -> Arc<Self> {
        Arc::new(Self {
            latest: RwLock::new(status),
            appended_entries: AtomicU64::new(0),
            config_revision: AtomicU64::new(0),
        })
    }

    /// Publish a new snapshot. The previous snapshot is overwritten;
    /// readers see either the old value or the new value, never a
    /// torn read.
    pub async fn publish(&self, status: NodeStatus) {
        let mut guard = self.latest.write().await;
        *guard = status;
    }

    /// Synchronous publish for non-async contexts (e.g. unit tests).
    /// Acquires the lock with [`tokio::sync::RwLock::try_write`]; if
    /// contention prevents an immediate write the publish is dropped
    /// — the next async publish will recover. Production callers
    /// should use the async [`publish`](Self::publish).
    pub fn try_publish(&self, status: NodeStatus) -> bool {
        match self.latest.try_write() {
            Ok(mut guard) => {
                *guard = status;
                true
            }
            Err(_) => false,
        }
    }

    /// Read the most recently published snapshot. Returns a `Copy`,
    /// so the read lock is released before this function returns.
    pub async fn current(&self) -> NodeStatus {
        *self.latest.read().await
    }

    /// Increment the `xraft_append_records_total` counter by `n`.
    /// Driver calls this once per successful `Action::AppendEntries`
    /// log-store append (counting `entries.len()`).
    pub fn record_appends(&self, n: u64) {
        self.appended_entries.fetch_add(n, Ordering::Relaxed);
    }

    /// Snapshot the monotonic append counter for Prometheus rendering.
    pub fn appended_entries(&self) -> u64 {
        self.appended_entries.load(Ordering::Relaxed)
    }

    /// Increment the SIGHUP-driven config-reload revision counter.
    /// Returns the new value. Called by `main.rs::reload_config` once
    /// the reload has successfully been applied to the engine
    /// (driver tick interval refreshed, log filter swapped, cached
    /// config replaced) — never on a failed/aborted reload.
    pub fn bump_config_revision(&self) -> u64 {
        // fetch_add returns the PREVIOUS value; add 1 for the post-
        // increment value the caller (and the /health JSON consumer)
        // expects.
        self.config_revision.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Read the current config-reload revision counter. Surfaced
    /// through the `/health` JSON as `config_revision` so operators
    /// can verify a SIGHUP actually applied (the value must strictly
    /// increase across each successful reload).
    pub fn config_revision(&self) -> u64 {
        self.config_revision.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_gauge_mapping_matches_architecture_v1() {
        assert_eq!(role_to_gauge(NodeRole::Follower), 0);
        assert_eq!(role_to_gauge(NodeRole::Candidate), 1);
        assert_eq!(role_to_gauge(NodeRole::PreCandidate), 2);
        assert_eq!(role_to_gauge(NodeRole::Leader), 3);
        assert_eq!(role_to_gauge(NodeRole::Observer), 4);
    }

    #[test]
    fn role_str_mapping_matches_canonical_glossary() {
        assert_eq!(role_to_str(NodeRole::Follower), "follower");
        assert_eq!(role_to_str(NodeRole::Candidate), "candidate");
        assert_eq!(role_to_str(NodeRole::PreCandidate), "pre-candidate");
        assert_eq!(role_to_str(NodeRole::Leader), "leader");
        assert_eq!(role_to_str(NodeRole::Observer), "observer");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn publish_then_current_returns_latest() {
        let pub_ = StatusPublisher::new(NodeId(7));
        let mut s = NodeStatus::placeholder(NodeId(7));
        s.term = 42;
        s.commit_index = 1000;
        s.leader_id = Some(7);
        pub_.publish(s).await;
        let got = pub_.current().await;
        assert_eq!(got.term, 42);
        assert_eq!(got.commit_index, 1000);
        assert_eq!(got.leader_id, Some(7));
    }

    #[test]
    fn record_appends_accumulates_monotonically() {
        let pub_ = StatusPublisher::new(NodeId(1));
        assert_eq!(pub_.appended_entries(), 0);
        pub_.record_appends(3);
        pub_.record_appends(5);
        assert_eq!(pub_.appended_entries(), 8);
    }

    #[test]
    fn config_revision_starts_zero_and_bumps_monotonically() {
        let pub_ = StatusPublisher::new(NodeId(1));
        assert_eq!(pub_.config_revision(), 0);
        assert_eq!(pub_.bump_config_revision(), 1);
        assert_eq!(pub_.config_revision(), 1);
        assert_eq!(pub_.bump_config_revision(), 2);
        assert_eq!(pub_.bump_config_revision(), 3);
        assert_eq!(pub_.config_revision(), 3);
    }

    #[test]
    fn placeholder_serializes_with_expected_fields() {
        let s = NodeStatus::placeholder(NodeId(99));
        let json = serde_json::to_value(s).unwrap();
        assert_eq!(json["node_id"], 99);
        assert_eq!(json["role"], "follower");
        assert_eq!(json["term"], 0);
        assert_eq!(json["commit_index"], 0);
        assert_eq!(json["leader_id"], serde_json::Value::Null);
    }
}
