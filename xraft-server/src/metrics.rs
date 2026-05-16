//! Prometheus metrics surface for the `xraft-server` admin endpoint.
//!
//! The canonical metric list lives in `architecture.md` §7 and
//! `e2e-scenarios.md` Feature 15. Stage 6.1 shipped the MVP subset;
//! Stage 7.1 extends that toward the complete canonical set by adding
//! the leader / replication observability metrics:
//!
//! ### MVP subset (Stage 6.1)
//! - `xraft_current_term` — gauge.
//! - `xraft_commit_index` — gauge.
//! - `xraft_current_leader` — gauge (`-1` when unknown).
//! - `xraft_role` — gauge (numeric encoding per
//!   [`role_to_gauge`](crate::status::role_to_gauge)).
//! - `xraft_election_latency_seconds` — histogram, leader-elected hop.
//! - `xraft_append_records_total` — counter.
//!
//! ### Stage 7.1 additions (leader / replication observability)
//! - `xraft_replication_lag` — gauge per `{replica}` label, entries
//!   behind the leader, leader-only; cleared on
//!   [`Action::StepDown`](xraft_core::message::Action::StepDown).
//! - `xraft_commit_latency_seconds` — histogram, time from proposal
//!   accepted by the driver to commit-index advance past the
//!   proposal's index, leader-only.
//! - `xraft_fetch_requests_total` — counter per `{direction}` label
//!   (`sent` for outbound Fetch RPCs from followers/observers to the
//!   leader, `received` for inbound Fetch RPCs that this node
//!   **accepted as leader for this cluster** — wrong-cluster traffic
//!   and Fetches received while a follower are filtered out per the
//!   `architecture.md` §7 "Fetch RPCs received by leader"
//!   contract).
//!
//! Stage 7.3 will land the remaining canonical metrics
//! (`xraft_snapshot_installs_total`, `xraft_log_end_offset`).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

use crate::driver::{DriverObserver, FetchDirection};
use crate::status::{NodeStatus, StatusPublisher, role_to_gauge};
use xraft_core::types::NodeId;

/// Default histogram bucket layout for `xraft_election_latency_seconds`:
/// 8 exponential buckets starting at 5 ms, factor 2 (5ms, 10, 20, 40,
/// 80, 160, 320, 640 ms). Covers the realistic range for a healthy
/// 3-5 voter cluster on a low-latency network while still flagging
/// pathological multi-second elections.
fn election_latency_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.005, 2.0, 8)
}

/// Histogram bucket layout for `xraft_commit_latency_seconds`. Stage
/// 7.1 measures "proposal accepted by driver → commit-index advance
/// past the entry's index", which on a healthy cluster sits in the
/// low-millisecond range (one leader→follower→leader Fetch RTT) but
/// can grow to seconds under partition / flush stalls. Layout matches
/// `election_latency_buckets`: 8 exponential buckets starting at 5 ms,
/// factor 2 (5ms, 10, 20, 40, 80, 160, 320, 640 ms). Dashboards
/// comparing the two histograms therefore see consistent bucket
/// boundaries.
fn commit_latency_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.005, 2.0, 8)
}

/// Label set for `xraft_replication_lag`. One sample per tracked
/// peer / observer; the leader emits a fresh value on every
/// event-loop iteration via
/// [`DriverObserver::on_replication_lag`]. The peer's `NodeId` is
/// rendered as a decimal string so the Prometheus tag is
/// human-readable (the Counter family's `direction` label uses an
/// enum for the same reason).
#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct ReplicaLabel {
    replica: String,
}

impl ReplicaLabel {
    fn new(replica: NodeId) -> Self {
        // NodeId's Display impl renders as `NodeId(N)` (Debug-style),
        // which is noisy for Prometheus labels. Extract just the
        // numeric id so the rendered label is `replica="2"` rather
        // than `replica="NodeId(2)"` — the rest of the metrics
        // pipeline (operators, dashboards) treats this as a
        // dimension, so the wrapper would force every query to strip
        // it.
        Self {
            replica: replica.0.to_string(),
        }
    }
}

/// Label set for `xraft_fetch_requests_total`. The `direction` label
/// disambiguates outbound (follower / observer issuing a Fetch RPC to
/// the leader) from inbound (leader handling a Fetch RPC from a
/// peer). A single counter family is more dashboard-friendly than two
/// separately-registered counters because operators can sum across
/// both directions or split by either side without a
/// recording rule.
#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct FetchDirectionLabel {
    direction: &'static str,
}

impl From<FetchDirection> for FetchDirectionLabel {
    fn from(d: FetchDirection) -> Self {
        Self {
            direction: match d {
                FetchDirection::Sent => "sent",
                FetchDirection::Received => "received",
            },
        }
    }
}

/// The Prometheus registry + metric handles, plus the
/// [`StatusPublisher`] the driver writes its observable state into.
///
/// `XRaftMetrics` is the bridge between the driver loop and the
/// `/metrics` and `/health` HTTP handlers. The driver holds an
/// `Arc<XRaftMetrics>` and calls the [`DriverObserver`] methods, which
/// in turn update the underlying counters / gauges / histograms.
pub struct XRaftMetrics {
    registry: Mutex<Registry>,
    current_term: Gauge<i64>,
    commit_index: Gauge<i64>,
    current_leader: Gauge<i64>,
    role: Gauge<i64>,
    election_latency_seconds: Histogram,
    append_records_total: Counter,
    /// Stage 7.1: per-replica replication lag (entries behind leader).
    /// Cleared on leader step-down via
    /// [`DriverObserver::on_leader_step_down`].
    replication_lag: Family<ReplicaLabel, Gauge<i64>>,
    /// Stage 7.1: proposal-to-commit wall-clock latency histogram.
    commit_latency_seconds: Histogram,
    /// Stage 7.1: count of Fetch RPCs observed by this node, labelled
    /// by direction. Exposed as `xraft_fetch_requests_total{direction="..."}`.
    fetch_requests: Family<FetchDirectionLabel, Counter>,
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
        let replication_lag = Family::<ReplicaLabel, Gauge<i64>>::default();
        let commit_latency_seconds = Histogram::new(commit_latency_buckets());
        let fetch_requests = Family::<FetchDirectionLabel, Counter>::default();

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
        registry.register(
            "xraft_replication_lag",
            "Entries this peer is behind the leader, computed leader-side as \
             (leader_last_log_index - peer.last_fetch_offset). Reset on leader step-down.",
            replication_lag.clone(),
        );
        registry.register(
            "xraft_commit_latency_seconds",
            "Seconds from proposal accepted by the driver to commit_index advancing past it.",
            commit_latency_seconds.clone(),
        );
        registry.register(
            // Same `_total` auto-suffix rule as `xraft_append_records`:
            // register without suffix, renderer adds it. Exposed as
            // `xraft_fetch_requests_total{direction="sent"}` etc.
            "xraft_fetch_requests",
            "Total Fetch RPCs counted by this node, labelled by direction: \
             `sent` for follower/observer→leader RPC dispatch, `received` for \
             inbound Fetches accepted while leader for this cluster \
             (wrong-cluster and non-leader receipts are filtered out per \
             `architecture.md` §7).",
            fetch_requests.clone(),
        );

        Self {
            registry: Mutex::new(registry),
            current_term,
            commit_index,
            current_leader,
            role,
            election_latency_seconds,
            append_records_total,
            replication_lag,
            commit_latency_seconds,
            fetch_requests,
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

    /// Stage 7.1 — set the replication-lag gauge for `replica` to the
    /// supplied entry count. Called by the driver once per
    /// event-loop iteration per peer while we are leader.
    pub fn set_replication_lag(&self, replica: NodeId, lag: u64) {
        self.replication_lag
            .get_or_create(&ReplicaLabel::new(replica))
            .set(lag as i64);
    }

    /// Stage 7.1 — clear every replication-lag label so a stepped-
    /// down leader does not surface stale lag in the next scrape.
    /// Called by the driver from the `Action::StepDown` arm.
    pub fn clear_replication_lag(&self) {
        self.replication_lag.clear();
    }

    /// Stage 7.1 — observe one proposal-to-commit latency sample.
    /// Called by the driver from `resolve_waiters_at` on the success
    /// path only (failed commits are tracked separately by the
    /// `xraft_propose_failures_total` counter).
    pub fn observe_commit_latency(&self, secs: f64) {
        self.commit_latency_seconds.observe(secs);
    }

    /// Stage 7.1 — increment `xraft_fetch_requests_total{direction=…}`
    /// by one. Sent direction is recorded from the driver's outbound
    /// dispatcher; received direction from the inbound Fetch handler.
    pub fn record_fetch_request(&self, direction: FetchDirection) {
        self.fetch_requests
            .get_or_create(&FetchDirectionLabel::from(direction))
            .inc();
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

    fn on_fetch_request(&self, direction: FetchDirection) {
        self.record_fetch_request(direction);
    }

    fn on_replication_lag(&self, replica: NodeId, lag: u64) {
        self.set_replication_lag(replica, lag);
    }

    fn on_commit_latency(&self, elapsed: Duration) {
        self.observe_commit_latency(elapsed.as_secs_f64());
    }

    fn on_leader_step_down(&self) {
        self.clear_replication_lag();
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

    // ─── Stage 7.1 additions ────────────────────────────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn replication_lag_gauge_is_per_replica() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        metrics.set_replication_lag(NodeId(2), 17);
        metrics.set_replication_lag(NodeId(3), 0);
        let render = metrics.render().await.unwrap();
        assert!(
            render.contains("xraft_replication_lag{replica=\"2\"} 17"),
            "render missing replica=2 line: {render}"
        );
        assert!(
            render.contains("xraft_replication_lag{replica=\"3\"} 0"),
            "render missing replica=3 line: {render}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clear_replication_lag_drops_all_labels() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        metrics.set_replication_lag(NodeId(2), 17);
        metrics.set_replication_lag(NodeId(3), 9);
        metrics.clear_replication_lag();
        let render = metrics.render().await.unwrap();
        // After clear() the family has no labelled samples, so the
        // per-replica lines must be gone. The HELP / TYPE lines
        // (containing the metric name) remain — assert on the
        // label string instead.
        assert!(
            !render.contains("replica=\"2\""),
            "render still contains replica=2 line: {render}"
        );
        assert!(
            !render.contains("replica=\"3\""),
            "render still contains replica=3 line: {render}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn observe_commit_latency_records_in_histogram() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        metrics.observe_commit_latency(0.012);
        metrics.observe_commit_latency(0.080);
        let render = metrics.render().await.unwrap();
        assert!(render.contains("xraft_commit_latency_seconds_count 2"));
        assert!(render.contains("xraft_commit_latency_seconds_sum"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fetch_requests_counter_is_per_direction() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        metrics.record_fetch_request(FetchDirection::Sent);
        metrics.record_fetch_request(FetchDirection::Sent);
        metrics.record_fetch_request(FetchDirection::Received);
        let render = metrics.render().await.unwrap();
        assert!(
            render.contains("xraft_fetch_requests_total{direction=\"sent\"} 2"),
            "render missing sent counter: {render}"
        );
        assert!(
            render.contains("xraft_fetch_requests_total{direction=\"received\"} 1"),
            "render missing received counter: {render}"
        );
    }
}
