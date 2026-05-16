//! Prometheus metrics surface for the `xraft-server` admin endpoint.
//!
//! Stage 6.1 ships the MVP metrics subset required by the workstream
//! brief — the canonical list from `architecture.md` §7 and
//! `e2e-scenarios.md` Feature 15:
//!
//! - `xraft_current_term` — gauge, persisted term at the latest
//!   [`NodeStatus`] publish.
//! - `xraft_commit_index` — gauge, volatile `commit_index` at the
//!   latest publish.
//! - `xraft_current_leader` — gauge, `NodeId` of the recognised
//!   leader; `-1` when unknown so dashboards can distinguish
//!   "no-leader" from "leader=0".
//! - `xraft_role` — gauge, numeric encoding of [`NodeRole`] per
//!   [`role_to_gauge`](crate::status::role_to_gauge).
//! - `xraft_election_latency_seconds` — histogram, time from
//!   `become_candidate` to `become_leader` for this node. The driver
//!   observes a sample only on the elected hop; followers and stepped-
//!   down candidates contribute nothing.
//! - `xraft_append_records_total` — counter, monotonic total of
//!   entries appended to this node's local log store. Counted
//!   leader-side AND follower-side because the log append happens on
//!   every replica.
//!
//! Stage 7.1 / 7.3 add the remaining canonical metrics
//! (`xraft_replication_lag`, `xraft_commit_latency_seconds`,
//! `xraft_fetch_requests_total`, `xraft_snapshot_installs_total`,
//! `xraft_log_end_offset`) on top of this scaffolding.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

use crate::driver::DriverObserver;
use crate::status::{NodeStatus, StatusPublisher, role_to_gauge};

/// Default histogram bucket layout for `xraft_election_latency_seconds`:
/// 8 exponential buckets starting at 5 ms, factor 2 (5ms, 10, 20, 40,
/// 80, 160, 320, 640 ms). Covers the realistic range for a healthy
/// 3-5 voter cluster on a low-latency network while still flagging
/// pathological multi-second elections.
fn election_latency_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.005, 2.0, 8)
}

/// The Prometheus registry + metric handles, plus the
/// [`StatusPublisher`] the driver writes its observable state into.
///
/// `XRaftMetrics` is the bridge between the driver loop and the
/// `/metrics` and `/health` HTTP handlers. The driver holds an
/// `Arc<XRaftMetrics>` and calls:
///
/// - [`Self::publish_state`] after every event-loop iteration to
///   refresh the gauges (`xraft_current_term`, `xraft_commit_index`,
///   `xraft_current_leader`, `xraft_role`).
/// - [`Self::observe_election_latency`] when transitioning from
///   `Candidate` to `Leader` to record one histogram sample.
/// - [`Self::record_appends`] inside
///   `Action::AppendEntries` to bump the `xraft_append_records_total`
///   counter.
///
/// The HTTP layer holds the same `Arc<XRaftMetrics>` and calls:
///
/// - [`Self::render`] to serialise the registry into the Prometheus
///   text-exposition format.
/// - [`Self::status_publisher`] to access the latest [`NodeStatus`]
///   for `/health`.
pub struct XRaftMetrics {
    registry: Mutex<Registry>,
    current_term: Gauge<i64>,
    commit_index: Gauge<i64>,
    current_leader: Gauge<i64>,
    role: Gauge<i64>,
    election_latency_seconds: Histogram,
    append_records_total: Counter,
    status: Arc<StatusPublisher>,
}

impl std::fmt::Debug for XRaftMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XRaftMetrics")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl XRaftMetrics {
    /// Construct a fresh `XRaftMetrics` over the supplied publisher
    /// and register every metric on a brand-new [`Registry`].
    ///
    /// Wrap the returned value in `Arc` (see
    /// [`Self::shared`](Self::shared)) — both the driver and the HTTP
    /// admin server need shared access.
    pub fn new(status: Arc<StatusPublisher>) -> Self {
        let mut registry = Registry::default();

        let current_term = Gauge::<i64>::default();
        let commit_index = Gauge::<i64>::default();
        let current_leader = Gauge::<i64>::default();
        let role = Gauge::<i64>::default();
        let election_latency_seconds = Histogram::new(election_latency_buckets());
        let append_records_total = Counter::default();

        registry.register(
            "xraft_current_term",
            "Current persisted Raft term (HardState.current_term).",
            current_term.clone(),
        );
        registry.register(
            "xraft_commit_index",
            "Volatile commit_index of this node's log.",
            commit_index.clone(),
        );
        registry.register(
            "xraft_current_leader",
            "NodeId of the leader recognised by this node; -1 when unknown.",
            current_leader.clone(),
        );
        registry.register(
            "xraft_role",
            "Numeric encoding of NodeRole: 0=Follower 1=Candidate 2=PreCandidate 3=Leader 4=Observer.",
            role.clone(),
        );
        registry.register(
            "xraft_election_latency_seconds",
            "Seconds from become_candidate to become_leader for this node.",
            election_latency_seconds.clone(),
        );
        registry.register(
            // prometheus-client appends `_total` to Counter names
            // automatically in the OpenMetrics text rendering, so
            // registering as `xraft_append_records` produces the
            // canonical exposed name `xraft_append_records_total`
            // — matching `architecture.md` §7 / `e2e-scenarios.md`
            // Feature 15. Registering `*_total` here would render
            // a double-suffixed name (the renderer would emit two
            // `_total` segments concatenated).
            "xraft_append_records",
            "Total log entries appended to this node's local log store.",
            append_records_total.clone(),
        );

        Self {
            registry: Mutex::new(registry),
            current_term,
            commit_index,
            current_leader,
            role,
            election_latency_seconds,
            append_records_total,
            status,
        }
    }

    /// Build a shared `Arc<XRaftMetrics>` over a fresh publisher
    /// seeded with the supplied initial status. Convenience for the
    /// server bootstrap path.
    pub fn shared(initial: NodeStatus) -> Arc<Self> {
        let publisher = StatusPublisher::from_status(initial);
        Arc::new(Self::new(publisher))
    }

    /// Apply the supplied snapshot to the gauges AND publish it on
    /// the shared [`StatusPublisher`] so `/health` sees the same
    /// instantaneous view.
    pub async fn publish_state(&self, status: NodeStatus) {
        self.current_term.set(status.term as i64);
        self.commit_index.set(status.commit_index as i64);
        // -1 sentinel disambiguates "no leader" from "leader is NodeId(0)".
        let leader_gauge = status.leader_id.map(|n| n as i64).unwrap_or(-1);
        self.current_leader.set(leader_gauge);
        self.role.set(role_to_gauge(status.role));
        self.status.publish(status).await;
    }

    /// Observe one election-latency sample (`Candidate → Leader`).
    pub fn observe_election_latency(&self, secs: f64) {
        self.election_latency_seconds.observe(secs);
    }

    /// Increment `xraft_append_records_total` by `n` and also bump
    /// the publisher-side counter (used by tests and any future
    /// non-Prometheus surface).
    pub fn record_appends(&self, n: u64) {
        // Counter::inc_by takes the increment as u64 directly.
        self.append_records_total.inc_by(n);
        self.status.record_appends(n);
    }

    /// Borrow the shared [`StatusPublisher`] for the HTTP `/health`
    /// handler and any other read-side consumer.
    pub fn status_publisher(&self) -> Arc<StatusPublisher> {
        self.status.clone()
    }

    /// Render the registry as a Prometheus text-exposition payload.
    pub async fn render(&self) -> Result<String, std::fmt::Error> {
        let mut buf = String::new();
        let registry = self.registry.lock().await;
        encode(&mut buf, &registry)?;
        Ok(buf)
    }
}

impl DriverObserver for XRaftMetrics {
    fn on_status<'a>(
        &'a self,
        status: NodeStatus,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        // Production impl: bridge the driver's per-iteration snapshot
        // into the metrics gauges + the shared StatusPublisher.
        Box::pin(async move {
            self.publish_state(status).await;
        })
    }

    fn on_append(&self, n: u64) {
        self.record_appends(n);
    }

    fn on_election_won(&self, elapsed: Duration) {
        self.observe_election_latency(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::{NodeId, NodeRole};

    fn sample_status() -> NodeStatus {
        let mut s = NodeStatus::placeholder(NodeId(7));
        s.role = NodeRole::Leader;
        s.term = 11;
        s.commit_index = 99;
        s.leader_id = Some(7);
        s.last_log_index = 100;
        s
    }

    #[tokio::test(flavor = "current_thread")]
    async fn publish_state_updates_all_gauges_and_publisher() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(7)));
        metrics.publish_state(sample_status()).await;

        let render = metrics.render().await.expect("render must succeed");
        // Spot-check that every gauge name appears in the output and
        // carries the value we just set.
        assert!(render.contains("xraft_current_term 11"));
        assert!(render.contains("xraft_commit_index 99"));
        assert!(render.contains("xraft_current_leader 7"));
        // Leader role = 3 per the role_to_gauge mapping.
        assert!(render.contains("xraft_role 3"));

        // /health view sees the same state.
        let current = metrics.status_publisher().current().await;
        assert_eq!(current.term, 11);
        assert_eq!(current.commit_index, 99);
        assert_eq!(current.leader_id, Some(7));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn current_leader_is_neg_one_when_unknown() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let mut s = NodeStatus::placeholder(NodeId(1));
        s.leader_id = None;
        metrics.publish_state(s).await;
        let render = metrics.render().await.unwrap();
        assert!(render.contains("xraft_current_leader -1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn record_appends_increments_counter() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        metrics.record_appends(3);
        metrics.record_appends(2);
        let render = metrics.render().await.unwrap();
        assert!(render.contains("xraft_append_records_total 5"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn observe_election_latency_records_in_histogram() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        metrics.observe_election_latency(0.030);
        metrics.observe_election_latency(0.150);
        let render = metrics.render().await.unwrap();
        // Histograms emit `_count` and `_sum` lines plus per-bucket
        // counts. We only assert presence — bucket boundary
        // exact-match assertions are fragile across prometheus-client
        // releases.
        assert!(render.contains("xraft_election_latency_seconds_count 2"));
        assert!(render.contains("xraft_election_latency_seconds_sum"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn role_gauge_tracks_role_transitions() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let mut s = NodeStatus::placeholder(NodeId(1));
        s.role = NodeRole::Candidate;
        metrics.publish_state(s).await;
        assert!(metrics.render().await.unwrap().contains("xraft_role 1"));
        s.role = NodeRole::Leader;
        metrics.publish_state(s).await;
        assert!(metrics.render().await.unwrap().contains("xraft_role 3"));
    }
}
