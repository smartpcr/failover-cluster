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
//! ### Stage 7.3 additions (snapshot / log-end observability)
//! - `xraft_snapshot_installs_total` — counter, bumped each time a
//!   follower installs a snapshot received via `FetchSnapshot`.
//!   Surfaces InstallSnapshot RPC volume per `architecture.md` §7.
//! - `xraft_log_end_offset` — gauge, mirrors `NodeStatus.last_log_index`
//!   on every `publish_state`. The brief calls this "log end offset"
//!   and dashboards correlate it with `xraft_commit_index` to detect
//!   replication lag at the log layer.
//! - `xraft_snapshot_duration_seconds` — histogram, end-to-end time
//!   from `Action::TakeSnapshot` accepted by the driver to the
//!   snapshot bytes flushed to disk. Observed from
//!   [`DriverObserver::on_snapshot_taken`].
//! - `xraft_snapshot_size_bytes` — histogram, size of each finalised
//!   snapshot in bytes. Observed from
//!   [`DriverObserver::on_snapshot_taken`].
//! - `xraft_log_compaction_events_total` — counter, bumped each time
//!   the driver compacts its log suffix after a snapshot finalises.
//!   Observed from [`DriverObserver::on_log_compacted`].
//!
//! All five metrics are registered and updated by this module so the
//! `architecture.md` §7 canonical set is reachable via `/metrics`.

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

/// Histogram bucket layout for `xraft_snapshot_duration_seconds`.
/// Snapshot creation reads the state machine + serialises it; on a
/// healthy cluster the work is dominated by serialisation cost which
/// scales with state-machine size. 8 exponential buckets starting at
/// 10 ms, factor 2 (10ms, 20, 40, 80, 160, 320, 640 ms, 1.28 s)
/// covers MVP state-machine sizes; multi-second snapshots fall in
/// the `+Inf` bucket and surface on dashboards as a tail-latency
/// alert.
fn snapshot_duration_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.010, 2.0, 8)
}

/// Histogram bucket layout for `xraft_snapshot_size_bytes`. Snapshot
/// size scales with state-machine entries; 10 exponential buckets
/// starting at 1 KiB, factor 4 (1 KiB, 4, 16, 64, 256, 1 MiB, 4, 16,
/// 64, 256 MiB) covers from "trivial" through "operationally
/// concerning" snapshot sizes. Beyond 256 MiB the snapshot push path
/// becomes the dominant replication cost and should trigger an alert.
fn snapshot_size_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(1024.0, 4.0, 10)
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
    /// Stage 7.3 (iter-5): total InstallSnapshot operations completed
    /// on this node. Bumped by
    /// [`DriverObserver::on_snapshot_installed`]. Exposed as
    /// `xraft_snapshot_installs_total`.
    snapshot_installs_total: Counter,
    /// Stage 7.3 (iter-5): last-log-index gauge mirrored from
    /// `NodeStatus.last_log_index` on every `publish_state`. Exposed
    /// as `xraft_log_end_offset`.
    log_end_offset: Gauge<i64>,
    /// Stage 7.3 (iter-8 restoration): end-to-end snapshot-creation
    /// latency. Observed once per `DriverObserver::on_snapshot_taken`.
    snapshot_duration_seconds: Histogram,
    /// Stage 7.3 (iter-8 restoration): size of each finalised
    /// snapshot in bytes. Observed once per
    /// `DriverObserver::on_snapshot_taken`.
    snapshot_size_bytes: Histogram,
    /// Stage 7.3 (iter-8 restoration): count of log-compaction
    /// events. Bumped once per `DriverObserver::on_log_compacted`.
    log_compaction_events_total: Counter,
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
        let snapshot_installs_total = Counter::default();
        let log_end_offset = Gauge::<i64>::default();
        let snapshot_duration_seconds = Histogram::new(snapshot_duration_buckets());
        let snapshot_size_bytes = Histogram::new(snapshot_size_buckets());
        let log_compaction_events_total = Counter::default();

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
        registry.register(
            // `_total` auto-suffix: register as `xraft_snapshot_installs`,
            // renderer emits `xraft_snapshot_installs_total`. Matches the
            // canonical name in `architecture.md` §7.
            "xraft_snapshot_installs",
            "Total InstallSnapshot operations completed on this node. \
             Bumped on every successful follower-side snapshot install.",
            snapshot_installs_total.clone(),
        );
        registry.register(
            "xraft_log_end_offset",
            "Last log index present in this node's local log store; \
             mirrored from NodeStatus.last_log_index on every publish.",
            log_end_offset.clone(),
        );
        registry.register(
            "xraft_snapshot_duration_seconds",
            "End-to-end wall-clock duration of each snapshot finalisation. \
             Observed once per Action::TakeSnapshot completion.",
            snapshot_duration_seconds.clone(),
        );
        registry.register(
            "xraft_snapshot_size_bytes",
            "Bytes written for each finalised snapshot. Observed once per \
             Action::TakeSnapshot completion.",
            snapshot_size_bytes.clone(),
        );
        registry.register(
            // `_total` auto-suffix: register as `xraft_log_compaction_events`,
            // renderer emits `xraft_log_compaction_events_total`.
            "xraft_log_compaction_events",
            "Total log-compaction events on this node. Bumped once per \
             driver compaction run after a snapshot finalises.",
            log_compaction_events_total.clone(),
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
            snapshot_installs_total,
            log_end_offset,
            snapshot_duration_seconds,
            snapshot_size_bytes,
            log_compaction_events_total,
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
        // Stage 7.3 iter-5: mirror last_log_index into the canonical
        // log-end gauge on every publish.
        self.log_end_offset.set(status.last_log_index as i64);
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

    /// Stage 7.3 iter-5 — bump `xraft_snapshot_installs_total` by one.
    /// Called by [`DriverObserver::on_snapshot_installed`] when a
    /// follower completes a snapshot install via `FetchSnapshot`.
    pub fn record_snapshot_install(&self) {
        self.snapshot_installs_total.inc();
    }

    /// Stage 7.3 iter-5 — set the `xraft_log_end_offset` gauge
    /// directly. Normally driven by [`Self::publish_state`]; exposed
    /// for any caller that wants to push a value out-of-band (tests).
    pub fn set_log_end_offset(&self, index: u64) {
        self.log_end_offset.set(index as i64);
    }

    /// Stage 7.3 iter-8 — observe one snapshot finalisation: record
    /// both the wall-clock duration and the snapshot size in bytes.
    /// Called from [`DriverObserver::on_snapshot_taken`].
    pub fn observe_snapshot_taken(&self, bytes: u64, elapsed: Duration) {
        self.snapshot_duration_seconds
            .observe(elapsed.as_secs_f64());
        self.snapshot_size_bytes.observe(bytes as f64);
    }

    /// Stage 7.3 iter-8 — bump `xraft_log_compaction_events_total`
    /// by one. Called from [`DriverObserver::on_log_compacted`].
    pub fn record_log_compaction(&self, _removed: u64) {
        self.log_compaction_events_total.inc();
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

    /// Stage 7.3 iter-5: bump the snapshot-install counter on every
    /// completed follower-side install.
    fn on_snapshot_installed(&self) {
        self.record_snapshot_install();
    }

    /// Stage 7.3 iter-8 restoration: observe a freshly-finalised
    /// snapshot's size and creation duration. Previously a default
    /// no-op despite the driver firing the hook, which regressed the
    /// `xraft_snapshot_duration_seconds` / `xraft_snapshot_size_bytes`
    /// metrics relative to `architecture.md` §7.
    fn on_snapshot_taken(&self, bytes: u64, elapsed: Duration) {
        self.observe_snapshot_taken(bytes, elapsed);
    }

    /// Stage 7.3 iter-8 restoration: bump
    /// `xraft_log_compaction_events_total`. Previously a default no-op.
    fn on_log_compacted(&self, removed: u64) {
        self.record_log_compaction(removed);
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

    // ─── Stage 7.3 observer → metric wiring (iter-5 / iter-8) ───────
    //
    // These tests exercise the `DriverObserver` trait surface — the
    // path the real driver takes — so a regression in
    // `XRaftMetrics`' impl of `on_snapshot_installed`,
    // `on_log_compacted`, or `on_snapshot_taken` (e.g. wired to the
    // wrong counter / histogram, or accidentally reverted to the
    // default no-op) fails CI instead of being silently dropped on
    // the floor.

    #[tokio::test(flavor = "current_thread")]
    async fn publish_state_sets_log_end_offset_gauge() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(7)));
        let mut s = sample_status();
        s.last_log_index = 4321;
        metrics.publish_state(s).await;
        let render = metrics.render().await.unwrap();
        assert!(
            render.contains("xraft_log_end_offset 4321"),
            "render missing log_end_offset gauge: {render}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_install_counter_increments_on_observer_hook() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        // Drive through the trait surface, not the helper method, so
        // a wiring regression (e.g. `on_snapshot_installed` reverting
        // to the default no-op) is caught.
        DriverObserver::on_snapshot_installed(&*metrics);
        DriverObserver::on_snapshot_installed(&*metrics);
        let render = metrics.render().await.unwrap();
        assert!(
            render.contains("xraft_snapshot_installs_total 2"),
            "render missing snapshot install counter: {render}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn log_compaction_counter_increments_on_observer_hook() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        // `removed` value is irrelevant to the counter (which only
        // bumps by one per call) but exercises the new `u64`
        // signature so a future signature change forces this test
        // to be updated rather than silently slipping by.
        DriverObserver::on_log_compacted(&*metrics, 50);
        DriverObserver::on_log_compacted(&*metrics, 25);
        DriverObserver::on_log_compacted(&*metrics, 10);
        let render = metrics.render().await.unwrap();
        assert!(
            render.contains("xraft_log_compaction_events_total 3"),
            "render missing log compaction counter: {render}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_taken_observer_feeds_duration_and_size_histograms() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        // Drive through the trait. Param order is (bytes, elapsed)
        // per the iter-8 signature — swapping them silently would
        // attribute byte counts to the duration histogram (and vice
        // versa); the `_sum` assertions below would flag that.
        DriverObserver::on_snapshot_taken(&*metrics, 4096, Duration::from_millis(75));
        DriverObserver::on_snapshot_taken(&*metrics, 16_384, Duration::from_millis(150));
        let render = metrics.render().await.unwrap();
        assert!(
            render.contains("xraft_snapshot_duration_seconds_count 2"),
            "render missing snapshot-duration count: {render}"
        );
        assert!(
            render.contains("xraft_snapshot_duration_seconds_sum"),
            "render missing snapshot-duration sum: {render}"
        );
        assert!(
            render.contains("xraft_snapshot_size_bytes_count 2"),
            "render missing snapshot-size count: {render}"
        );
        assert!(
            render.contains("xraft_snapshot_size_bytes_sum"),
            "render missing snapshot-size sum: {render}"
        );
    }
}
