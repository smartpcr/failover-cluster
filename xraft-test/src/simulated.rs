//! [`SimulatedCluster`] — multi-node in-process Raft cluster harness
//! used by Stage 8.1 deterministic integration tests.
//!
//! Each [`SimulatedNode`] owns:
//!
//! * a [`RaftNode`](xraft_core::RaftNode) wired through the production
//!   [`Driver`](xraft_server::Driver) event loop,
//! * volatile in-memory storage
//!   ([`MemoryLogStore`](xraft_storage::MemoryLogStore),
//!   [`MemoryHardStateStore`](xraft_storage::MemoryHardStateStore),
//!   [`MemorySnapshotStore`](xraft_storage::MemorySnapshotStore)),
//! * a [`RecordingStateMachine`](crate::state_machine::RecordingStateMachine)
//!   the test inspects after each scenario,
//! * a [`SimulatedTransport`](crate::network::SimulatedTransport) glued
//!   into the shared [`SimulatedNetwork`].
//!
//! The harness exposes the operations the Stage 8.1 scenarios need:
//! [`await_leader`](Self::await_leader), [`propose`](Self::propose),
//! [`kill`](Self::kill) (fail-stop a node by aborting its driver task
//! and unregistering its handler), [`partition`](Self::partition) and
//! [`heal_partition`](Self::heal_partition).
//!
//! The harness intentionally does NOT spin up admin HTTP or real gRPC
//! ports — the [`RealCluster`](crate::real::RealCluster) harness covers
//! that for the real-network scenarios in the same brief.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;

use xraft_core::RaftNode;
use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::storage::{HardStateStore, LogStore, SnapshotStore};
use xraft_core::types::{NodeId, NodeRole};

use xraft_storage::{
    FileHardStateStore, FileLogStore, FileSnapshotStore, MemoryHardStateStore, MemoryLogStore,
    MemorySnapshotStore,
};

use xraft_server::driver::DriverChannels;
use xraft_server::{Driver, DriverConfig, DriverHandle, DriverObserver, NodeStatus};

use crate::clock::{ManualTickController, SimulatedClock};
use crate::network::{SimulatedNetwork, SimulatedTransport};
use crate::observer::{TestObserver, TestObserverHandle};
use crate::state_machine::{RecordingHandle, RecordingStateMachine};

/// Default tick interval the simulated driver uses. Kept tiny so a
/// 3-node election completes within a handful of wall-clock
/// milliseconds, but not SO tight that fetch responses race each
/// other (a too-aggressive fetch cadence under in-process transport
/// caused follower-side "non-contiguous entries" drops in early
/// iter testing).
const DEFAULT_TICK_MS: u64 = 5;
/// Default election-timeout window. The Stage 8.1 brief asserts that
/// "the cluster elects a leader within 2 election timeout periods
/// after startup"; with `min_ms = 500, max_ms = 1000` that bound
/// lands at 2 s, which holds reliably even under workspace-parallel
/// `cargo test --workspace` CPU pressure (where tokio tick latency
/// can spike well above 100 ms).
///
/// Why so wide for an in-process cluster? Under heavy parallel
/// `cargo test --workspace` load on a busy CI box, the tokio
/// runtime can starve nodes for >100 ms; with a tight 60-120 ms
/// window some followers time out into `PreCandidate` BEFORE the
/// newly-elected leader's first fetch reaches them. The KRaft
/// engine honors `PreVoteResponse.leader_hint` and steps a
/// stranded `PreCandidate` back to `Follower`
/// (see `xraft-core/src/node.rs::handle_pre_vote_response`,
/// operator answer
/// `engine-pre-vote-recovery → yes-add-leader-hint-step-down`), but
/// a wide window is still preferable as a belt-and-suspenders
/// safety margin: it keeps the engine on the happy path (followers
/// stay `Follower`) instead of exercising the recovery path on
/// every test run, which keeps the test signal-to-noise high.
const DEFAULT_ELECTION_MIN_MS: u64 = 500;
const DEFAULT_ELECTION_MAX_MS: u64 = 1000;
/// Default fetch cadence — the follower pull interval. 10 ms is fast
/// enough that 1000 sequential proposals converge across all 3
/// followers in <30 s and slow enough that fetch responses do not
/// race each other on the in-process transport.
const DEFAULT_FETCH_MS: u64 = 10;

/// Per-driver-task wall-clock budget [`SimulatedCluster::shutdown`]
/// waits for each node's driver task to exit after signalling
/// `driver.shutdown()`.
///
/// # Why 15 s and not "a couple of seconds"
///
/// The driver's [`Driver::run`](xraft_server::Driver::run) graceful
/// shutdown path is allowed to spend up to
/// `DriverConfig::shutdown_drain_deadline` (5 s by default in
/// `xraft-server`) draining queued events in `graceful_drain`, AND
/// another `shutdown_drain_deadline` budget draining in-flight
/// outbound RPC tasks inside `shutdown_sequence`, before the final
/// hard-state persist + log flush. That gives a legitimate worst
/// case of `2 * shutdown_drain_deadline ≈ 10 s` for a single driver
/// even when the driver is behaving exactly to spec. Under the
/// `stress::throughput::sustained_1000_per_second_for_60s_*` test
/// the engine arrives at shutdown with thousands of recently-acked
/// AppendEntries still mid-replication; the drain genuinely needs
/// most of that budget on a busy machine.
///
/// An earlier `Duration::from_secs(2)` timeout consistently
/// false-positived this test as a "shutdown deadlock" even though
/// every driver was about to exit cleanly within its own budget.
/// 15 s covers `2 * 5 s + 5 s` of slack for tokio scheduler
/// contention on a CI box running `--test-threads=1` after several
/// minutes of other stress tests; it is still tight enough that a
/// real shutdown deadlock (drivers genuinely stuck forever) will
/// fail the test loudly rather than blocking it until the suite's
/// per-test wall-clock timeout fires.
const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// SimulatedClusterConfig
// ---------------------------------------------------------------------------

/// A `(min_ms, max_ms)` election timeout window. Used for per-node
/// election-window overrides via
/// [`SimulatedClusterConfig::per_node_election_overrides`] (the
/// Stage 8.2 clock-skew scenarios).
///
/// Named-field form (rather than a raw `(u64, u64)` tuple) so a
/// mis-ordered `min`/`max` is a compile error rather than a silent
/// runtime swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionWindow {
    /// Minimum election timeout in milliseconds.
    pub min_ms: u64,
    /// Maximum election timeout in milliseconds. Must be ≥ `min_ms`.
    pub max_ms: u64,
}

impl ElectionWindow {
    /// Build an [`ElectionWindow`]. Panics if `max_ms < min_ms`.
    pub fn new(min_ms: u64, max_ms: u64) -> Self {
        assert!(
            max_ms >= min_ms,
            "ElectionWindow: max_ms ({max_ms}) must be >= min_ms ({min_ms})",
        );
        Self { min_ms, max_ms }
    }
}

/// Tunables for [`SimulatedCluster::start`]. All fields have sensible
/// defaults via [`SimulatedClusterConfig::default`].
#[derive(Debug, Clone)]
pub struct SimulatedClusterConfig {
    /// Number of voter nodes in the cluster (3 or 5 for the Stage 8.1
    /// scenarios; any number ≥ 1 is supported).
    pub size: usize,
    /// RNG seed for the [`SimulatedNetwork`]'s drop decisions.
    pub seed: u64,
    /// Per-node tick interval. Smaller values speed up convergence
    /// at the cost of CPU.
    pub tick_ms: u64,
    /// Election timeout lower bound.
    pub election_min_ms: u64,
    /// Election timeout upper bound (must be ≥ `election_min_ms`).
    pub election_max_ms: u64,
    /// Follower fetch cadence.
    pub fetch_ms: u64,
    /// **Per-node election-window overrides** (Stage 8.2 clock-skew
    /// scenarios). Any node id present here uses the override's
    /// `(min_ms, max_ms)` in place of the cluster's default
    /// `election_min_ms` / `election_max_ms`. Nodes NOT present
    /// fall back to the defaults. Use a [`BTreeMap`] (not [`HashMap`])
    /// so node-iteration in spawn order is deterministic and so
    /// `Debug` output is stable for test diagnostics.
    ///
    /// # Why this lives at config level (not a builder method)
    ///
    /// The cluster has ONE entry point ([`SimulatedCluster::start`]);
    /// the per-node override is a property of how the engine is
    /// CONFIGURED, so it belongs in the same struct rather than as a
    /// separate fluent-builder method that callers might forget to
    /// invoke before `start`.
    pub per_node_election_overrides: BTreeMap<NodeId, ElectionWindow>,
    /// When `true`, each node uses file-backed storage
    /// ([`FileLogStore`] / [`FileHardStateStore`] /
    /// [`FileSnapshotStore`]) against a per-node sub-directory under
    /// a process-temp root owned by [`SimulatedCluster`]. The
    /// sub-directory survives [`SimulatedCluster::kill`] so a
    /// subsequent [`SimulatedCluster::restart`] re-opens the SAME
    /// WAL / quorum-state / snapshot files and the engine recovers
    /// its persisted state — true process-restart-with-durable-disk
    /// semantics, not node-replacement-with-fresh-disk.
    ///
    /// When `false` (default for back-compat with Stage 8.1 tests),
    /// each node uses in-memory stores; a restart spawns a fresh
    /// node that must catch up via `AppendEntries` / `InstallSnapshot`
    /// from the leader.
    ///
    /// Stage 8.2 chaos/restart scenarios (e.g.
    /// `random_node_kill_and_restart_committed_entries_survive`)
    /// enable this so the kill+restart path exercises durable
    /// crash recovery as the brief requires.
    pub use_durable_storage: bool,
}

impl Default for SimulatedClusterConfig {
    fn default() -> Self {
        Self {
            size: 3,
            seed: 0xC0FFEE,
            tick_ms: DEFAULT_TICK_MS,
            election_min_ms: DEFAULT_ELECTION_MIN_MS,
            election_max_ms: DEFAULT_ELECTION_MAX_MS,
            fetch_ms: DEFAULT_FETCH_MS,
            per_node_election_overrides: BTreeMap::new(),
            use_durable_storage: false,
        }
    }
}

impl SimulatedClusterConfig {
    /// Construct a config for a 3-node cluster with the default tunables.
    pub fn three_node(seed: u64) -> Self {
        Self {
            size: 3,
            seed,
            ..Self::default()
        }
    }

    /// Construct a config for a 5-node cluster with the default tunables.
    pub fn five_node(seed: u64) -> Self {
        Self {
            size: 5,
            seed,
            ..Self::default()
        }
    }
}

// ---------------------------------------------------------------------------
// SimulatedNode
// ---------------------------------------------------------------------------

/// Per-node bundle inside a [`SimulatedCluster`].
///
/// Kept `pub` so tests can reach in for the [`DriverHandle`] (used to
/// propose on a specific node) or the [`RecordingHandle`] (used to
/// inspect what the SM applied).
pub struct SimulatedNode {
    /// Logical node id (matches the voter record).
    pub node_id: NodeId,
    /// Driver-side `propose` / `query` / `shutdown` surface.
    pub driver: DriverHandle,
    /// Test-side state-machine inspection handle.
    pub recording: RecordingHandle,
    /// Latest-status observer.
    pub status: TestObserverHandle,
    /// Driver task handle. `abort()`-able for fail-stop kill.
    task: Option<JoinHandle<XResult<()>>>,
}

impl SimulatedNode {
    /// Whether the node's driver task is still spawned (not aborted).
    pub fn is_alive(&self) -> bool {
        self.task.is_some()
    }
}

// ---------------------------------------------------------------------------
// SimulatedCluster
// ---------------------------------------------------------------------------

/// In-process 3- or 5-node Raft cluster. Construct via
/// [`SimulatedCluster::start`].
pub struct SimulatedCluster {
    /// All nodes in voter-id order (`[NodeId(1), NodeId(2), …]`).
    pub nodes: Vec<SimulatedNode>,
    /// Shared message-routing fabric. Tests use this to introduce
    /// faults (partitions, packet loss, latency).
    pub network: Arc<SimulatedNetwork>,
    /// Virtual tick counter.
    /// Shared with the [`SimulatedNetwork`] AND with every driver's
    /// [`ManualTickSource`](crate::clock::ManualTickSource) so every
    /// per-RPC latency window AND every driver tick atomically advance
    /// the same clock; tests read [`SimulatedClock::elapsed`] after a
    /// scenario to see the total simulated transit + tick time
    /// charged across all dispatches.
    pub clock: Arc<SimulatedClock>,
    /// Shared tick controller. Every driver listens on this controller;
    /// each [`ManualTickController::trigger`] wakes every node
    /// in lock-step AND advances [`Self::clock`] by `tick_quantum`.
    pub tick_controller: ManualTickController,
    /// Background task that pulses [`Self::tick_controller`] at the
    /// configured wall-clock cadence so default tests don't need to
    /// step ticks manually. Tests can stop this pump and call
    /// [`Self::tick_once`] for fully deterministic control —
    /// see [`Self::detach_tick_pump`].
    tick_pump: Option<JoinHandle<()>>,
    /// ONE notify shared with every node's
    /// [`TestObserver`] and [`RecordingStateMachine`]. Bumped on every
    /// status publish and every SM apply, so
    /// [`Self::await_leader`] / [`Self::await_applied_at_least`] wake
    /// the instant ANY relevant state changes — replaces the fixed
    /// `5 ms` polling cadence an earlier review flagged as
    /// scheduler-dependent.
    pub state_change: Arc<Notify>,
    /// driver tasks that were
    /// aborted via [`Self::kill`] are PARKED here instead of being
    /// dropped, so [`Self::shutdown`] can `.await` each one and
    /// surface any panic that happened BEFORE the abort signal
    /// reached the task. A pre-existing panic would otherwise vanish
    /// when the [`JoinHandle`] dropped — silently turning a real bug
    /// into a clean test pass.
    killed_tasks: Vec<(NodeId, JoinHandle<XResult<()>>)>,
    /// Captured config + voter set so [`Self::restart`] can re-spawn
    /// a killed node with the SAME `node_id` and the SAME voter set
    /// (and, when [`SimulatedClusterConfig::use_durable_storage`] is
    /// enabled, the SAME per-node disk directory — so the engine
    /// recovers from its persisted log + hard-state + snapshot
    /// instead of needing leader-driven catch-up).
    restart_cfg: SimulatedClusterConfig,
    /// Voter set captured at [`Self::start`] time so [`Self::restart`]
    /// uses the same voter records (directory_id, host:port) when
    /// re-spawning. Without this the engine would generate fresh
    /// `directory_id`s on every restart, which the peer-membership
    /// equality checks treat as a different node.
    voters: Vec<VoterConfig>,
    /// Per-node data directory used when
    /// [`SimulatedClusterConfig::use_durable_storage`] is `true`.
    /// Empty otherwise. The directories live under [`Self::_data_root`]
    /// and survive [`Self::kill`] so [`Self::restart`] can re-open
    /// them via the file-backed stores.
    node_dirs: BTreeMap<NodeId, PathBuf>,
    /// Root temp directory owning every per-node sub-directory. The
    /// underscored field name marks it as "owned for RAII cleanup
    /// only" — the [`TempDir`] is removed when [`SimulatedCluster`]
    /// is dropped. `None` when `use_durable_storage = false`.
    _data_root: Option<TempDir>,
}

impl SimulatedCluster {
    /// Spin up `cfg.size` nodes wired through a shared
    /// [`SimulatedNetwork`]. Each node's driver task is spawned
    /// immediately; the harness does NOT wait for leader election —
    /// callers should follow with [`Self::await_leader`].
    pub async fn start(cfg: SimulatedClusterConfig) -> XResult<Self> {
        assert!(cfg.size >= 1, "SimulatedCluster needs at least one voter");
        assert!(
            cfg.election_max_ms >= cfg.election_min_ms,
            "election_max_ms ({}) must be >= election_min_ms ({})",
            cfg.election_max_ms,
            cfg.election_min_ms,
        );
        // Validate per-node election overrides up front so a typo in a
        // clock-skew test surfaces at start() rather than after the
        // driver task has spawned (which would surface as an opaque
        // election-never-fires hang).
        for (node_id, w) in &cfg.per_node_election_overrides {
            assert!(
                node_id.0 >= 1 && (node_id.0 as usize) <= cfg.size,
                "per_node_election_overrides: node id {} is out of range \
                 (cluster size = {})",
                node_id.0,
                cfg.size,
            );
            assert!(
                w.max_ms >= w.min_ms,
                "per_node_election_overrides[{}]: max_ms ({}) must be >= min_ms ({})",
                node_id.0,
                w.max_ms,
                w.min_ms,
            );
        }

        let clock = SimulatedClock::new();
        let network = SimulatedNetwork::new_with_clock(cfg.seed, clock.clone());

        // ONE notify shared across every
        // node's TestObserver and RecordingStateMachine so the
        // cluster-level event-driven await loops wake on any change.
        let state_change = Arc::new(Notify::new());

        // every driver tick flows through a
        // shared ManualTickController whose `trigger` atomically
        // advances `clock` by `tick_quantum`. The default pump task
        // below replaces what `tokio::time::interval` used to do
        // PER-NODE, but funnelled through a single SimulatedClock so
        // the clock IS the authoritative record of simulated time.
        let tick_quantum = Duration::from_millis(cfg.tick_ms);
        let tick_controller = ManualTickController::new(clock.clone(), tick_quantum);

        // Build voter set up front so every node sees the same one.
        let voters: Vec<VoterConfig> = (1..=cfg.size as u64)
            .map(|i| VoterConfig {
                node_id: i,
                directory_id: Uuid::new_v4().to_string(),
                // host:port is purely cosmetic for the simulated
                // transport — peers route by NodeId, not address.
                host: "127.0.0.1".into(),
                port: 10_000 + i as u16,
            })
            .collect();

        // Build per-node data directories if durable storage was
        // requested. The TempDir owns the entire tree; per-node
        // sub-directories survive node kills and get re-opened on
        // restart (true crash-recovery semantics).
        let (data_root, node_dirs) = if cfg.use_durable_storage {
            let root = tempfile::Builder::new()
                .prefix("xraft-test-sim-")
                .tempdir()
                .map_err(|e| {
                    XRaftError::Storage(format!(
                        "SimulatedCluster::start: tempdir for durable storage: {e}"
                    ))
                })?;
            let mut dirs = BTreeMap::new();
            for i in 1..=cfg.size as u64 {
                let dir = root.path().join(format!("node-{i}"));
                std::fs::create_dir_all(&dir).map_err(|e| {
                    XRaftError::Storage(format!(
                        "SimulatedCluster::start: create per-node dir {}: {e}",
                        dir.display()
                    ))
                })?;
                dirs.insert(NodeId(i), dir);
            }
            (Some(root), dirs)
        } else {
            (None, BTreeMap::new())
        };

        let mut nodes = Vec::with_capacity(cfg.size);
        for i in 1..=cfg.size as u64 {
            let node_id = NodeId(i);
            let dir = node_dirs.get(&node_id).map(|p| p.as_path());
            let node = Self::spawn_node(
                node_id,
                voters.clone(),
                &cfg,
                network.clone(),
                tick_controller.tick_source(),
                state_change.clone(),
                dir,
            )?;
            nodes.push(node);
        }

        // Default wall-clock pump: fires `trigger` every `tick_ms` so
        // existing tests don't need to manually step ticks. Tests that
        // want fully deterministic control can call
        // `detach_tick_pump` to abort it and drive ticks via
        // `tick_once`.
        let pump_controller = tick_controller.clone();
        let pump = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_quantum);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Consume the immediate-fire so the first trigger lands at
            // `tick_quantum`, not at t=0.
            interval.tick().await;
            loop {
                interval.tick().await;
                pump_controller.trigger();
            }
        });

        info!(
            target: "xraft_test::simulated",
            size = cfg.size,
            seed = cfg.seed,
            tick_ms = cfg.tick_ms,
            "SimulatedCluster started (manual-tick-controller wired)"
        );

        Ok(Self {
            nodes,
            network,
            clock,
            tick_controller,
            tick_pump: Some(pump),
            state_change,
            killed_tasks: Vec::new(),
            restart_cfg: cfg,
            voters,
            node_dirs,
            _data_root: data_root,
        })
    }

    /// Build + spawn a single node. Extracted so the `start` loop and
    /// the future `revive(node_id)` path share assembly.
    ///
    /// When `data_dir` is `Some`, the node uses file-backed storage
    /// against that directory ([`FileLogStore`] / [`FileHardStateStore`]
    /// / [`FileSnapshotStore`]) — so a restart against the same dir
    /// recovers the node's persisted Raft state. When `data_dir` is
    /// `None`, the node uses volatile in-memory stores (current Stage
    /// 8.1 default).
    fn spawn_node(
        node_id: NodeId,
        voters: Vec<VoterConfig>,
        cfg: &SimulatedClusterConfig,
        network: Arc<SimulatedNetwork>,
        tick_source: Box<dyn xraft_server::TickSource>,
        state_change: Arc<Notify>,
        data_dir: Option<&Path>,
    ) -> XResult<SimulatedNode> {
        // Apply per-node election-window override if one was set in
        // the cluster config — this is the lever the Stage 8.2
        // clock-skew scenarios use to model differential election-
        // timer rates across nodes.
        let (election_min_ms, election_max_ms) = match cfg.per_node_election_overrides.get(&node_id)
        {
            Some(w) => (w.min_ms, w.max_ms),
            None => (cfg.election_min_ms, cfg.election_max_ms),
        };
        let cluster_cfg = ClusterConfig {
            node_id,
            cluster_id: "simulated".into(),
            listen_addr: format!("127.0.0.1:{}", 10_000 + node_id.0 as u16),
            peers: vec![],
            voters: voters.clone(),
            election_timeout_min_ms: election_min_ms,
            election_timeout_max_ms: election_max_ms,
            fetch_interval_ms: cfg.fetch_ms,
            tick_interval_ms: cfg.tick_ms,
            snapshot_interval: 10_000,
            max_log_entries_before_compaction: 100_000,
            data_dir: data_dir
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| std::path::PathBuf::from(".")),
            snapshot_retention_count: 3,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_domain_name: None,
            connect_timeout_ms: 100,
            rpc_timeout_ms: 500,
            max_rpc_retries: 1,
            retry_initial_backoff_ms: 10,
            retry_max_backoff_ms: 50,
            max_message_size: 64 * 1024 * 1024,
            observers: vec![],
            enable_check_quorum: true,
            enable_leader_lease: false,
            check_quorum_interval_ms: None,
        };

        // ---- engine + state machine + observer ----------------------------
        // derive a per-node deterministic
        // seed via `mix64(cfg.seed, node_id)` so the engine's election-
        // timer RNG is reproducible AND distinct across nodes
        // (`cfg.seed` alone would have every node fire at the same
        // tick offset, defeating leader election).
        let node_seed = mix_seed(cfg.seed, node_id.0);
        let mut raft_node = RaftNode::new_with_seed(cluster_cfg.clone(), node_seed)?;
        let sm = RecordingStateMachine::with_state_change(state_change.clone());
        let recording = sm.handle();
        let observer = TestObserver::with_state_change(node_id, state_change);
        let status_handle = observer.handle();

        // ---- driver channels + simulated transport ------------------------
        let channels = DriverChannels::new();
        let inbound = Arc::new(channels.inbound_handler());
        network.register(node_id, inbound);
        let transport = Arc::new(SimulatedTransport::new(network.clone(), node_id));

        let driver_cfg = DriverConfig {
            tick_interval: Duration::from_millis(cfg.tick_ms),
            ..DriverConfig::default()
        };

        // ---- storage ------------------------------------------------------
        // Two assembly paths because the storage traits use static
        // dispatch through generics on `Driver::with_channels`; we
        // can't mix-and-match File* and Memory* at the same call
        // site without trait-object plumbing. Both branches end with
        // the SAME `SimulatedNode` shape so the caller doesn't see
        // the storage-type leak.
        let voter_set = cluster_cfg
            .build_voter_set()?
            .ok_or_else(|| XRaftError::Config("voter set is empty".into()))?;

        let (driver_handle, task) = if let Some(dir) = data_dir {
            let log_store = FileLogStore::open(dir).map_err(|e| {
                XRaftError::Storage(format!(
                    "spawn_node({node_id:?}): FileLogStore::open({}): {e}",
                    dir.display()
                ))
            })?;
            let mut hs_store = FileHardStateStore::open(dir).map_err(|e| {
                XRaftError::Storage(format!(
                    "spawn_node({node_id:?}): FileHardStateStore::open({}): {e}",
                    dir.display()
                ))
            })?;
            let snapshot_store = FileSnapshotStore::open(dir).map_err(|e| {
                XRaftError::Storage(format!(
                    "spawn_node({node_id:?}): FileSnapshotStore::open({}): {e}",
                    dir.display()
                ))
            })?;
            // Persist the static voter set BEFORE constructing the
            // RaftNode so the engine recovers the same voter set the
            // cluster config carries — mirrors what
            // `Server::start_with_state_machine` does at first boot.
            // Idempotent: subsequent opens of the same dir already
            // carry the voter_set in the quorum-state file.
            hs_store.persist_voter_set(&voter_set)?;

            // ---- engine recovery from disk (iter-9 item #2) -------
            // Mirror the production recovery path in
            // `xraft-server::Server::start_with_state_machine`
            // (xraft-server/src/server.rs §"engine recovery").
            // Without these three steps the file-backed branch was
            // structurally identical to the in-memory branch: the
            // RaftNode started with default hard_state (term 0,
            // voted_for None, commit_index 0) and last_log_(index,
            // term) = (0, 0) even when the on-disk log already had
            // committed entries from a prior boot. That made
            // `durable_storage_survives_*` chaos tests pass for the
            // wrong reason (catch-up from the surviving quorum
            // re-replicated everything as if the disk were empty).
            //
            // Step 1 — recover HardState (term, voted_for,
            // commit_index). Required for Raft term monotonicity
            // and to skip already-committed entries on the apply
            // pump's first pass.
            let recovered_hs = hs_store.load().map_err(|e| {
                XRaftError::Storage(format!("spawn_node({node_id:?}): hs_store.load(): {e}"))
            })?;
            if let Some(hs) = recovered_hs {
                raft_node.hard_state = hs;
            }
            // Step 2 — seed last_log_(index, term) from the durable
            // log so election eligibility and replication probes
            // are accurate immediately, not after the first tick.
            raft_node.set_last_log(log_store.last_index(), log_store.last_term());
            // Step 3 — recover snapshot baseline if a snapshot
            // exists on disk; raise commit_index/last_applied to
            // the snapshot's last_included_index so the apply pump
            // doesn't re-apply pre-snapshot entries.
            if let Some((meta, _data)) = snapshot_store.load_latest_snapshot().map_err(|e| {
                XRaftError::Storage(format!(
                    "spawn_node({node_id:?}): snapshot_store.load_latest_snapshot(): {e}"
                ))
            })? {
                if raft_node.commit_index < meta.last_included_index {
                    raft_node.commit_index = meta.last_included_index;
                }
                if raft_node.last_applied < meta.last_included_index {
                    raft_node.last_applied = meta.last_included_index;
                }
                if raft_node.last_snapshot_meta.is_none() {
                    raft_node.last_snapshot_meta = Some(meta);
                }
            }
            // Step 4 — defense-in-depth clamp: raise commit_index
            // from the recovered hard-state checkpoint, but never
            // above the durable log tip. Matches server.rs
            // "Stage 7.2 iter-3 finding #1" comment.
            let persisted_commit_clamped =
                std::cmp::min(raft_node.hard_state.commit_index, log_store.last_index());
            if persisted_commit_clamped > raft_node.commit_index {
                raft_node.commit_index = persisted_commit_clamped;
            }

            let driver = Driver::with_channels(
                channels,
                raft_node,
                log_store,
                hs_store,
                snapshot_store,
                sm,
                transport.clone(),
                driver_cfg,
            )
            .with_observer(Arc::new(observer) as Arc<dyn DriverObserver>)
            .with_tick_source(tick_source);
            let handle = driver.handle();
            let task = tokio::spawn(async move { driver.run().await });
            (handle, task)
        } else {
            let log_store = MemoryLogStore::new();
            let mut hs_store = MemoryHardStateStore::new();
            let snapshot_store = MemorySnapshotStore::default();
            hs_store.persist_voter_set(&voter_set)?;
            let driver = Driver::with_channels(
                channels,
                raft_node,
                log_store,
                hs_store,
                snapshot_store,
                sm,
                transport.clone(),
                driver_cfg,
            )
            .with_observer(Arc::new(observer) as Arc<dyn DriverObserver>)
            .with_tick_source(tick_source);
            let handle = driver.handle();
            let task = tokio::spawn(async move { driver.run().await });
            (handle, task)
        };

        Ok(SimulatedNode {
            node_id,
            driver: driver_handle,
            recording,
            status: status_handle,
            task: Some(task),
        })
    }

    /// Number of nodes currently spawned (including killed ones whose
    /// task handles have not yet been reaped).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the cluster has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Borrow the per-node entry for `node_id`.
    pub fn node(&self, node_id: NodeId) -> Option<&SimulatedNode> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    /// Wait until ONE alive node reports `NodeRole::Leader` AND every
    /// other alive node ALSO reports the same `term` and a
    /// `leader_id == Some(leader)`. Returns the elected leader's
    /// `(NodeId, term)` on success, or
    /// `XRaftError::ElectionTimeout` on deadline.
    ///
    /// Strict follower agreement is
    /// REQUIRED — returning as soon as one node sees itself as Leader
    /// races against follower convergence and lets proposing tests
    /// fire before any follower has acknowledged the new leader.
    ///
    /// # event-driven sim-time deadline
    ///
    /// `deadline` is measured in SIMULATED time, observed via
    /// [`SimulatedClock::elapsed`]. The simulated clock advances
    /// only when ticks fire (default wall-clock pump OR a
    /// [`Self::start_manual_pump`] burst pump OR manual
    /// [`Self::tick_once`] calls), so this method's notion of "time
    /// passing" is decoupled from tokio scheduling.
    ///
    /// # Event-driven wait
    ///
    /// The poll is event-driven on
    /// state change. The loop registers a
    /// [`Self::state_change`] notify waiter, bumped on every
    /// [`TestObserver::on_status`] publish, so an actual
    /// role/term/leader_id transition wakes the loop IMMEDIATELY.
    /// The select races the notify plus a `50 ms` periodic safety-net
    /// wake (bounded deadline-check cadence; defends against any
    /// residual missed-wake race).
    ///
    /// An earlier event-driven variant ALSO raced a
    /// fresh [`crate::clock::ManualTickSource`] listener. That arm
    /// was REMOVED because when [`Self::start_manual_pump`] is
    /// running, the manual fast pump fires hundreds of triggers per
    /// ms of wall-clock; the loop drained the resulting buffered
    /// ticks faster than the driver tasks could process them on
    /// shared workers, starving the very election the test was
    /// waiting for. State-change notify is the correct progress
    /// signal: it fires when a driver actually publishes a new
    /// status, not on every clock advance.
    ///
    /// A wall-clock backstop (`10 × deadline + 30 s`) prevents an
    /// infinite hang in the pathological "no pump is firing" case so
    /// the test author gets a clear timeout instead of a stalled test.
    pub async fn await_leader(&self, deadline: Duration) -> XResult<(NodeId, u64)> {
        let start_sim = self.clock.elapsed();
        let start_wall = Instant::now();
        let wall_backstop = deadline.saturating_mul(10) + Duration::from_secs(30);
        loop {
            // Register the state-change waiter BEFORE checking the
            // predicate so a status publish between check and wait
            // isn't lost (Notify::Notified::enable semantics).
            let state_waiter = self.state_change.notified();
            tokio::pin!(state_waiter);
            state_waiter.as_mut().enable();

            if let Some(converged) = self.try_converged_leader().await {
                return Ok(converged);
            }
            // deadline is SIMULATED time.
            let sim_elapsed = self.clock.elapsed().saturating_sub(start_sim);
            if sim_elapsed >= deadline {
                return Err(XRaftError::ElectionTimeout);
            }
            // Wall-clock backstop: if a pump bug has left the
            // simulated clock frozen, surface a clear timeout instead
            // of hanging forever.
            let wall_elapsed = start_wall.elapsed();
            if wall_elapsed >= wall_backstop {
                return Err(XRaftError::ElectionTimeout);
            }
            // Event-driven wake: real progress signal (status publish)
            // OR a 50 ms safety-net so the deadline check is bounded.
            let remaining_wall = wall_backstop - wall_elapsed;
            let safety_net = Duration::from_millis(50).min(remaining_wall);
            tokio::select! {
                _ = &mut state_waiter => {}
                _ = tokio::time::sleep(safety_net) => {}
            }
        }
    }

    /// Detach the harness's wall-clock tick pump so subsequent
    /// progress requires either manual [`Self::tick_once`] calls or a
    /// re-attached [`Self::start_manual_pump`].
    ///
    /// # Async drain on cancellation
    ///
    /// An earlier version was a sync method that returned the previous
    /// `Option<JoinHandle<()>>`; callers either dropped the handle or
    /// called `.abort()` themselves. Both shapes left a hazard: a
    /// `JoinHandle::abort()` only schedules cancellation, which
    /// surfaces at the task's next `.await` point. The pump task may
    /// therefore fire ONE more `controller.trigger()` after the
    /// caller returns — interleaving an extra tick with the
    /// supposedly-isolated `advance_simulated_time` deterministic
    /// burst that typically follows.
    ///
    /// This version makes this `async`: after `abort()` we `await` the
    /// handle to drain the cancellation, so by the time the function
    /// returns the pump task is GUARANTEED to have stopped firing
    /// triggers. Every call site must now be `cluster.detach_tick_pump().await`.
    pub async fn detach_tick_pump(&mut self) {
        if let Some(handle) = self.tick_pump.take() {
            handle.abort();
            // Await the cancellation so the spawned trigger loop is
            // guaranteed to have stopped before this returns. Without
            // this, the just-aborted task could still fire one trigger
            // before reaching its next `.await` and interleave with
            // the deterministic burst the caller is about to run.
            let _ = handle.await;
        }
    }

    /// Manually fire ONE tick on every attached driver in lock-step,
    /// atomically advancing the cluster's [`SimulatedClock`] by one
    /// tick quantum. Safe to call regardless of whether the wall-clock
    /// pump is still running.
    pub fn tick_once(&self) {
        self.tick_controller.trigger();
    }

    /// Spawn a *manual-trigger fast pump* on the
    /// [`ManualTickController`] — a tokio task that drives
    /// `ticks_per_burst` controller triggers per
    /// [`Self::PUMP_DRAIN_YIELDS`] yield_now cadence. The pump
    /// handle is STORED on the cluster (in `self.tick_pump`) and
    /// will be aborted by [`Self::shutdown`].
    ///
    /// # Why long-running scenarios use this pump
    ///
    /// This is the path the long-running simulated scenarios
    /// (`simulated_propose_thousand_entries`,
    /// `simulated_leader_kill_reelection`,
    /// `simulated_partition_recovery`) use INSTEAD of the harness's
    /// default `tokio::time::interval(tick_quantum)` pump. The pump
    /// is identical in structural intent (one tick per beat) but
    /// COMPRESSES simulated time relative to wall-clock — each beat
    /// fires `ticks_per_burst * tick_quantum` of simulated time in
    /// roughly one [`Self::PUMP_DRAIN_YIELDS`]-yield cadence. The
    /// determinism win is that every tick the driver observes flows
    /// through the test-owned [`ManualTickController`] (not through
    /// `tokio::time::interval`), so the test code can pause the
    /// pump (via [`Self::detach_tick_pump`]) to perform a clean,
    /// race-free burst-advance via [`Self::advance_simulated_time`]
    /// before resuming it.
    ///
    /// # Scheduler-independent cadence
    ///
    /// A naive implementation would pace this loop with
    /// `tokio::time::sleep(500 µs)`
    /// between bursts, which under workspace-parallel `cargo test` —
    /// where many test binaries each claim a multi-thread runtime —
    /// gets jittered into multi-second pauses by the OS timer subsystem
    /// (visible as `simulated_propose_thousand_entries` hanging past
    /// 10 min under default `--test-threads=auto`). The current pump
    /// replaces the wall-clock sleep with a yield-based cadence —
    /// the pump pays [`Self::PUMP_DRAIN_YIELDS`]
    /// `tokio::task::yield_now().await` calls between bursts instead.
    /// Each yield releases one scheduling slot to whichever driver
    /// task is ready, with no dependency on OS timer resolution;
    /// `cargo test --workspace` now completes under default
    /// `--test-threads`.
    ///
    /// # Contract
    ///
    /// * Requires NO pump currently attached — asserted via
    ///   `tick_pump.is_none()`. Call [`Self::detach_tick_pump`]
    ///   first if the harness's default wall-clock pump is still
    ///   running (`SimulatedCluster::start` installs it by default).
    /// * Pump is stored in `self.tick_pump` and aborted by
    ///   [`Self::shutdown`]. Callers must NOT keep a separate
    ///   handle.
    ///
    /// Use [`Self::tick_once`] / [`Self::await_leader_with_manual_ticks`]
    /// for fully step-by-step deterministic phases where you want to
    /// know exactly how many ticks fire and when.
    pub fn start_manual_pump(&mut self, ticks_per_burst: u32) {
        assert!(
            self.tick_pump.is_none(),
            "start_manual_pump requires detach_tick_pump() first"
        );
        assert!(
            ticks_per_burst >= 1,
            "ticks_per_burst must be at least 1; got {ticks_per_burst}"
        );
        let controller = self.tick_controller.clone();
        let handle = tokio::spawn(async move {
            loop {
                for _ in 0..ticks_per_burst {
                    controller.trigger();
                }
                // yields + sub-ms sleep so engine tasks on
                // other worker threads make progress. yield_now()
                // alone reschedules only the CURRENT task on its
                // worker, leaving sibling-worker engine tasks
                // unblocked only by chance — observed flake on
                // Windows.
                for _ in 0..Self::PUMP_DRAIN_YIELDS {
                    tokio::task::yield_now().await;
                }
                tokio::time::sleep(Duration::from_micros(Self::PUMP_DRAIN_PAUSE_MICROS)).await;
            }
        });
        self.tick_pump = Some(handle);
    }

    /// Burst-advance simulated time by firing `count` ticks in batches of
    /// [`Self::ADVANCE_BATCH_SIZE`], yielding
    /// [`Self::PUMP_DRAIN_YIELDS`] times between batches so the
    /// drivers actually drain the queued ticks and emit heartbeats /
    /// replication packets between election-timer increments.
    ///
    /// This is the replacement for `tokio::time::sleep(election_max * N)`
    /// in test phases whose only purpose is to "let simulated time
    /// pass" — e.g. stranding minority partition nodes in
    /// `PreCandidate`.
    ///
    /// # Why batched, not a single mass-trigger
    ///
    /// An earlier dead end fired all `count` triggers in microseconds
    /// of wall-clock then yielded once. That dumped a 300-tick backlog
    /// onto every driver and serialised the way each engine
    /// processed it. In the `same_term_step_down` test, the leader
    /// processed 300 ticks faster than the majority followers
    /// processed theirs, but the majority followers ALSO incremented
    /// their election timer 300 times — without seeing a heartbeat
    /// in between — and spuriously bumped the term, racing the
    /// "leader stays the same" invariant. Batching at the same
    /// cadence as [`Self::start_manual_pump`] eliminates that race:
    /// every batch advances simulated time by `batch * tick_quantum`,
    /// then the runtime drains so heartbeats can flow before the
    /// next batch.
    ///
    /// # Structural enforcement
    ///
    /// The harness asserts `tick_pump.is_none()` so a concurrent
    /// wall-clock or fast pump cannot interleave extra triggers
    /// with the burst. Tests using
    /// [`Self::start_manual_pump`] for the bulk of their run MUST
    /// call [`Self::detach_tick_pump`] before this burst and
    /// re-attach via another `start_manual_pump` after.
    pub async fn advance_simulated_ticks(&self, count: u32) {
        assert!(
            self.tick_pump.is_none(),
            "advance_simulated_ticks requires detach_tick_pump() first \
             (prevents pump interleaving)"
        );
        let batch = Self::ADVANCE_BATCH_SIZE;
        let mut remaining = count;
        while remaining > 0 {
            let n = remaining.min(batch);
            for _ in 0..n {
                self.tick_controller.trigger();
            }
            // yields + sub-ms sleep so engine tasks on
            // sibling worker threads can drain queued ticks.
            for _ in 0..Self::PUMP_DRAIN_YIELDS {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_micros(Self::PUMP_DRAIN_PAUSE_MICROS)).await;
            remaining -= n;
        }
    }

    /// Convenience wrapper: convert a [`Duration`] of simulated time
    /// into the equivalent number of tick quanta and burst-advance.
    /// `at_least` rounds UP so callers asking for "election_max * 3"
    /// get strictly >= that much simulated time.
    ///
    /// Uses the same `tick_pump.is_none()` assertion as
    /// [`Self::advance_simulated_ticks`]; same batched cadence so
    /// heartbeats can flow between election-timer increments.
    pub async fn advance_simulated_time(&self, at_least: Duration) {
        let q = self.tick_controller.tick_quantum().as_micros().max(1) as u64;
        let want = at_least.as_micros() as u64;
        let count = want.div_ceil(q) as u32;
        let count = count.max(1);
        self.advance_simulated_ticks(count).await;
    }

    /// Batch size for [`Self::advance_simulated_ticks`]. Matches the
    /// default `ticks_per_burst` in [`Self::start_manual_pump`] so
    /// burst-advance and the manual fast pump have identical cadence.
    const ADVANCE_BATCH_SIZE: u32 = 4;

    /// Number of `tokio::task::yield_now().await` calls the pump
    /// pays between bursts. Sized for the largest test cluster
    /// (5 nodes × ~3 awaits per tick-drain) with headroom.
    ///
    /// Yields alone are not sufficient on Windows
    /// multi-thread runtimes — `yield_now()` reschedules the
    /// current task but does not surrender the worker thread to the
    /// OS, so other engine tasks pinned to other workers do not
    /// always make progress between yields. The pump now PAIRS each
    /// burst with a sub-millisecond `tokio::time::sleep` (see
    /// [`Self::PUMP_DRAIN_PAUSE_MICROS`]) that yields the worker to
    /// the OS scheduler. Total wall-clock overhead per poll is
    /// still well below the simulated tick quantum.
    const PUMP_DRAIN_YIELDS: usize = 32;

    /// Sub-millisecond wall-clock pause paid between tick bursts so
    /// engine tasks on other worker threads get scheduling time.
    /// Empirically, 32 `yield_now()` calls alone proved insufficient
    /// to drain queued ticks on Windows multi-thread runtimes —
    /// observed as `simulated_three_node_election` failing in 0.00s
    /// (the harness completed its 100-poll budget before any engine
    /// task drained a single tick).  100 µs is well under the
    /// 5 ms simulated tick quantum, so this pause cannot dominate
    /// the test's wall-clock budget.
    const PUMP_DRAIN_PAUSE_MICROS: u64 = 100;

    /// Deterministic-tick-driven version of [`Self::await_leader`].
    /// Caller is responsible for having already detached the
    /// wall-clock pump via [`Self::detach_tick_pump`]. Between each
    /// leader-status poll this advances the cluster clock by
    /// `ticks_per_poll * tick_ms` of simulated time.
    ///
    /// Between polls the loop pays [`Self::PUMP_DRAIN_YIELDS`]
    /// `tokio::task::yield_now().await` calls so every driver task
    /// can drain its queued ticks AND advance its internal state
    /// machine. A bare single `yield_now()` releases only ONE
    /// scheduling slot, which is insufficient for an N-node cluster
    /// to drain `ticks_per_poll` buffered ticks per driver — see
    /// the constant's doc-comment for sizing.
    ///
    /// A naive implementation would pace this loop with
    /// `tokio::time::sleep(500 µs)`, which under
    /// workspace-parallel `cargo test` gets jittered into
    /// multi-second pauses by the OS timer subsystem. Yield-based
    /// pacing eliminates that dependency.
    ///
    /// Note: this is the strict step-by-step deterministic helper.
    /// For long-running scenarios that need a continuous tick stream
    /// (e.g. 1000 sequential proposals) use [`Self::start_manual_pump`]
    /// instead — it spawns the same `trigger()` loop in a background
    /// task so test code can `propose().await` without interleaving
    /// tick advancement by hand.
    ///
    /// Returns `(leader_id, term)` on convergence (one alive Leader
    /// AND every other alive node reporting that leader + that term)
    /// or [`XRaftError::ElectionTimeout`] after `max_simulated_time`
    /// of simulated time has elapsed without convergence.
    pub async fn await_leader_with_manual_ticks(
        &self,
        max_simulated_time: Duration,
        ticks_per_poll: u32,
    ) -> XResult<(NodeId, u64)> {
        assert!(
            self.tick_pump.is_none(),
            "await_leader_with_manual_ticks requires detach_tick_pump() first"
        );
        assert!(
            ticks_per_poll >= 1,
            "ticks_per_poll must be at least 1; got {ticks_per_poll}"
        );
        let tick_quantum = self.tick_controller.tick_quantum();
        let total_ticks_budget =
            (max_simulated_time.as_micros() / tick_quantum.as_micros().max(1)) as u64;
        let polls = total_ticks_budget / ticks_per_poll as u64;
        for _ in 0..polls {
            for _ in 0..ticks_per_poll {
                self.tick_controller.trigger();
            }
            // yields + sub-ms sleep so engine tasks on
            // sibling worker threads can drain queued ticks. See
            // `PUMP_DRAIN_PAUSE_MICROS` doc for why yields alone
            // were insufficient.
            for _ in 0..Self::PUMP_DRAIN_YIELDS {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_micros(Self::PUMP_DRAIN_PAUSE_MICROS)).await;
            if let Some(leader) = self.try_converged_leader().await {
                return Ok(leader);
            }
        }
        Err(XRaftError::ElectionTimeout)
    }

    /// One-shot convergence check: returns `Some((leader, term))`
    /// iff exactly one alive node reports `Leader` AND every other
    /// alive node reports `term == leader_term` and
    /// `leader_id == Some(leader)`. Otherwise `None`. Used by
    /// [`Self::await_leader_with_manual_ticks`].
    pub async fn try_converged_leader(&self) -> Option<(NodeId, u64)> {
        let mut leader: Option<(NodeId, u64)> = None;
        let mut leader_count = 0;
        let mut snapshots = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            if !node.is_alive() {
                continue;
            }
            let snap = node.status.status().await;
            if let Some(ref s) = snap
                && s.role == NodeRole::Leader
            {
                leader_count += 1;
                leader = Some((node.node_id, s.term));
            }
            snapshots.push((node.node_id, snap));
        }
        if leader_count != 1 {
            return None;
        }
        let (leader_id, leader_term) = leader?;
        for (id, snap) in &snapshots {
            if *id == leader_id {
                continue;
            }
            let s = snap.as_ref()?;
            if s.term != leader_term || s.leader_id != Some(leader_id.0) {
                return None;
            }
        }
        Some((leader_id, leader_term))
    }

    /// Return a fresh `(NodeId, NodeStatus)` snapshot for every alive
    /// node. Used for assertions on convergence.
    pub async fn statuses(&self) -> Vec<(NodeId, Option<NodeStatus>)> {
        let mut out = Vec::with_capacity(self.nodes.len());
        for n in &self.nodes {
            out.push((n.node_id, n.status.status().await));
        }
        out
    }

    /// Borrow the current leader's [`DriverHandle`], if exactly one
    /// alive node currently reports the `Leader` role.
    pub async fn leader_handle(&self) -> Option<DriverHandle> {
        let mut found: Option<DriverHandle> = None;
        for node in &self.nodes {
            if !node.is_alive() {
                continue;
            }
            if let Some(s) = node.status.status().await
                && s.role == NodeRole::Leader
            {
                if found.is_some() {
                    return None;
                }
                found = Some(node.driver.clone());
            }
        }
        found
    }

    /// The id of the unique current leader, if any.
    pub async fn leader_id(&self) -> Option<NodeId> {
        let mut found: Option<NodeId> = None;
        for node in &self.nodes {
            if !node.is_alive() {
                continue;
            }
            if let Some(s) = node.status.status().await
                && s.role == NodeRole::Leader
            {
                if found.is_some() {
                    return None;
                }
                found = Some(node.node_id);
            }
        }
        found
    }

    /// Wait until every alive node's recording SM has applied at
    /// least `target` entries. Returns the highest observed apply
    /// count across all nodes on timeout.
    ///
    /// # Deadline semantics
    ///
    /// `deadline` is measured in SIMULATED time via
    /// [`SimulatedClock::elapsed`], matching [`Self::await_leader`].
    /// The poll is event-driven on state change: the loop races the
    /// cluster's [`Self::state_change`] notify (bumped on every
    /// [`crate::state_machine::RecordingStateMachine::apply`] AND
    /// every observer status publish) plus a `50 ms` periodic safety
    /// net. A previously-present tick-source arm was REMOVED for
    /// the same reason as [`Self::await_leader`]: under
    /// [`Self::start_manual_pump`] the listener drained buffered
    /// triggers faster than drivers could process them, starving
    /// apply progress. State-change notify is the correct signal —
    /// it fires when an apply OR status change actually occurs.
    /// Backed by a `10 × deadline + 30 s` wall-clock backstop so a
    /// stalled-pump bug surfaces a clear timeout instead of hanging.
    pub async fn await_applied_at_least(
        &self,
        target: usize,
        deadline: Duration,
    ) -> std::result::Result<(), usize> {
        let start_sim = self.clock.elapsed();
        let start_wall = Instant::now();
        let wall_backstop = deadline.saturating_mul(10) + Duration::from_secs(30);
        loop {
            // Register state-change waiter BEFORE the predicate
            // check (missed-wake guard).
            let state_waiter = self.state_change.notified();
            tokio::pin!(state_waiter);
            state_waiter.as_mut().enable();

            let mut min_observed = usize::MAX;
            let mut max_observed: usize = 0;
            for n in &self.nodes {
                if !n.is_alive() {
                    continue;
                }
                let count = n.recording.len();
                if count < min_observed {
                    min_observed = count;
                }
                if count > max_observed {
                    max_observed = count;
                }
            }
            if min_observed == usize::MAX {
                // every node killed; nothing to converge.
                return Err(0);
            }
            if min_observed >= target {
                return Ok(());
            }
            // SIMULATED-time deadline.
            let sim_elapsed = self.clock.elapsed().saturating_sub(start_sim);
            if sim_elapsed >= deadline {
                return Err(max_observed);
            }
            let wall_elapsed = start_wall.elapsed();
            if wall_elapsed >= wall_backstop {
                return Err(max_observed);
            }
            // Event-driven wake on state change OR 50 ms safety net.
            let remaining_wall = wall_backstop - wall_elapsed;
            let safety_net = Duration::from_millis(50).min(remaining_wall);
            tokio::select! {
                _ = &mut state_waiter => {}
                _ = tokio::time::sleep(safety_net) => {}
            }
        }
    }

    /// Wait until `node_id`'s recording SM has applied at least
    /// `target` entries. Returns the observed apply count on timeout
    /// (or `usize::MAX` if `node_id` is unknown).
    ///
    /// # event-driven
    ///
    /// Uses the same `state_change` notify + 50 ms safety-net wake as
    /// [`Self::await_applied_at_least`]. Tolerates dead nodes: the
    /// recording handle remains valid after [`Self::kill`], so the
    /// latest applied count is still observable.
    pub async fn await_node_applied_at_least(
        &self,
        node_id: NodeId,
        target: usize,
        deadline: Duration,
    ) -> std::result::Result<(), usize> {
        let Some(recording) = self.nodes.iter().find_map(|n| {
            if n.node_id == node_id {
                Some(n.recording.clone())
            } else {
                None
            }
        }) else {
            return Err(usize::MAX);
        };
        let start_sim = self.clock.elapsed();
        let start_wall = Instant::now();
        let wall_backstop = deadline.saturating_mul(10) + Duration::from_secs(30);
        loop {
            let state_waiter = self.state_change.notified();
            tokio::pin!(state_waiter);
            state_waiter.as_mut().enable();

            let observed = recording.len();
            if observed >= target {
                return Ok(());
            }
            let sim_elapsed = self.clock.elapsed().saturating_sub(start_sim);
            if sim_elapsed >= deadline {
                return Err(observed);
            }
            let wall_elapsed = start_wall.elapsed();
            if wall_elapsed >= wall_backstop {
                return Err(observed);
            }
            let remaining_wall = wall_backstop - wall_elapsed;
            let safety_net = Duration::from_millis(50).min(remaining_wall);
            tokio::select! {
                _ = &mut state_waiter => {}
                _ = tokio::time::sleep(safety_net) => {}
            }
        }
    }

    /// Propose `command` against the current leader. Resolves the
    /// leader via [`Self::leader_handle`]; returns
    /// `XRaftError::NotLeader { leader_hint: None }` when no unique
    /// leader is currently elected.
    pub async fn propose(&self, command: Bytes) -> XResult<xraft_core::types::LogIndex> {
        let handle = self
            .leader_handle()
            .await
            .ok_or(XRaftError::NotLeader { leader_hint: None })?;
        handle.propose(command).await
    }

    /// Fail-stop a node: abort its driver task and unregister its
    /// handler from the network. The node's storage is dropped along
    /// with the [`Driver`], so a future `revive` (not implemented in
    /// this stage) would start from an empty log.
    ///
    /// # killed handles are PARKED, not dropped
    ///
    /// A naive `task.abort()`-then-drop shape would hide one failure
    /// mode: if the driver task
    /// PANICKED at any moment before the abort signal reached it, the
    /// panic message would die with the dropped handle and the test
    /// would report `ok`. The current implementation parks the
    /// (still-aborted) handle
    /// in [`Self::killed_tasks`]; [`Self::shutdown`] later `.await`s
    /// every parked handle and classifies a `JoinError::is_panic()`
    /// outcome as a fatal pre-existing panic regardless of when in
    /// the test's lifetime the abort fired.
    pub fn kill(&mut self, node_id: NodeId) {
        // take the task first via a scoped borrow of
        // `self.nodes`, then push into `self.killed_tasks` after the
        // borrow ends to keep the borrow checker happy.
        let mut taken_task: Option<JoinHandle<XResult<()>>> = None;
        for node in &mut self.nodes {
            if node.node_id == node_id
                && let Some(task) = node.task.take()
            {
                task.abort();
                taken_task = Some(task);
                break;
            }
        }
        if let Some(task) = taken_task {
            // park the aborted handle so shutdown() can
            // surface a pre-existing panic. Dropping it here
            // would silently swallow `JoinError::is_panic()`.
            self.killed_tasks.push((node_id, task));
        }
        self.network.unregister(node_id);
        self.network.kill(node_id);
    }

    /// Bring a previously-[`Self::kill`]ed node back online.
    ///
    /// When [`SimulatedClusterConfig::use_durable_storage`] is `true`
    /// (opt-in; the chaos-suite default constructor
    /// `cluster_harness::chaos_cluster_config` sets it to `false`),
    /// the replacement node re-opens the SAME per-node
    /// sub-directory the killed node owned — the engine recovers
    /// its [`HardState`](xraft_core::storage::HardState) (current
    /// term, voted-for, commit_index, voter_set), replays the WAL,
    /// restores the latest snapshot, and resumes from the
    /// recovered apply cursor. This is the durable
    /// process-restart-with-disk-preserved semantics that the
    /// dedicated `durable_storage_survives_full_cluster_kill_restart`
    /// chaos scenario uses.
    ///
    /// When `use_durable_storage` is `false` (the chaos-suite
    /// default), the replacement [`SimulatedNode`] reuses the
    /// killed node's `NodeId` and voter record but starts with an
    /// empty log, empty hard state, and empty snapshot store — the
    /// model is "the node's process crashed AND its disk was
    /// wiped"; the cluster's leader must replicate every committed
    /// entry back to the restarted node via `AppendEntries` (and,
    /// for far-behind followers, `InstallSnapshot`).
    ///
    /// # Errors
    ///
    /// Returns `Err(XRaftError::Config)` if no node with `node_id` is
    /// currently in [`Self::nodes`] OR if the named node is still
    /// alive (`node.is_alive()` is true). The caller MUST call
    /// [`Self::kill`] first.
    pub async fn restart(&mut self, node_id: NodeId) -> XResult<()> {
        // Find the killed slot.
        let slot = self
            .nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .ok_or_else(|| {
                XRaftError::Config(format!("restart({node_id:?}): no such node in cluster"))
            })?;
        if self.nodes[slot].is_alive() {
            return Err(XRaftError::Config(format!(
                "restart({node_id:?}): node is still alive — call kill() first"
            )));
        }

        // Reap the parked-killed handle so we don't carry a dangling
        // aborted JoinHandle once the node is restarted. We `.await`
        // with a short timeout to surface any pre-existing panic the
        // kill() docstring promises — if the task panicked BEFORE the
        // abort signal reached it, that panic is fatal regardless of
        // restart intent.
        let reaped: Option<JoinHandle<XResult<()>>> = {
            let mut idx_to_remove = None;
            for (i, (id, _)) in self.killed_tasks.iter().enumerate() {
                if *id == node_id {
                    idx_to_remove = Some(i);
                    break;
                }
            }
            idx_to_remove.map(|i| self.killed_tasks.swap_remove(i).1)
        };
        if let Some(task) = reaped {
            // Aborted tasks return `JoinError::is_cancelled()` — fine.
            // A `JoinError::is_panic()` would mean the task panicked
            // before our abort landed and the panic must surface.
            match tokio::time::timeout(Duration::from_millis(500), task).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    if e.is_panic() {
                        return Err(XRaftError::Config(format!(
                            "restart({node_id:?}): pre-existing panic in killed driver task: \
                             cannot safely restart a node that panicked: {e:?}"
                        )));
                    }
                    // is_cancelled() — expected for an aborted task.
                }
                Err(_) => {
                    // Task didn't reap within timeout — leave it; the
                    // shutdown path will handle it eventually.
                }
            }
        }

        // Spawn a fresh node with the SAME node_id and the SAME
        // voter set the cluster was started with. When durable
        // storage is enabled, pass the SAME per-node dir back so the
        // file-backed stores re-open the persisted files.
        let dir = self.node_dirs.get(&node_id).map(|p| p.as_path());
        let replacement = Self::spawn_node(
            node_id,
            self.voters.clone(),
            &self.restart_cfg,
            self.network.clone(),
            self.tick_controller.tick_source(),
            self.state_change.clone(),
            dir,
        )?;
        self.nodes[slot] = replacement;

        // Re-mark the node alive in the network. spawn_node's
        // network.register() call adds the new handler, but the
        // `dead` set still carries the kill() marker; without revive,
        // every outbound RPC to/from the new driver would be dropped.
        self.network.revive(node_id);

        info!(
            target: "xraft_test::simulated",
            node_id = node_id.0,
            durable = self.restart_cfg.use_durable_storage,
            "SimulatedCluster::restart re-spawned node"
        );
        Ok(())
    }

    /// Symmetrically partition `a` from `b`. Both directions are cut.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.network.partition(a, b);
    }

    /// Symmetrically partition a `group` of nodes from everyone else.
    pub fn partition_group(&self, group: &[NodeId]) {
        self.network.partition_group(group);
    }

    /// Heal a symmetric partition between `a` and `b`.
    pub fn heal_partition(&self, a: NodeId, b: NodeId) {
        self.network.heal_partition(a, b);
    }

    /// Heal every partition cut currently active.
    pub fn heal_all(&self) {
        self.network.heal_all();
    }

    /// Gracefully shut down every alive node and await their driver
    /// tasks. Idempotent — repeated calls are no-ops after the first.
    ///
    /// # teardown errors are NOT swallowed
    ///
    /// A naive `let _ = tokio::time::timeout(15s, task).await;`
    /// would silently discard EVERY driver task outcome —
    /// `XRaftError` returns, task panics, and shutdown deadlocks alike
    /// would pass undetected. This shutdown instead classifies every outcome
    /// via [`crate::teardown::is_allowed_teardown_noise`]:
    ///
    /// * `Ok(())` from the driver: clean exit, ignored.
    /// * `Err(XRaftError::Storage(...))` matching the Windows tempdir
    ///   teardown race (`rename ... os error 3 | 2`): logged via
    ///   `tracing::warn` and ignored — cosmetic.
    /// * Any other `Err(XRaftError)`: aggregated into the failure
    ///   list; teardown panics at end so test runs cannot pass with
    ///   real driver / storage / transport bugs hidden.
    /// * `JoinError::is_panic()`: aggregated as fatal — driver task
    ///   panics MUST be surfaced.
    /// * Timeout after [`SHUTDOWN_TASK_TIMEOUT`]: aggregated as fatal
    ///   — a shutdown deadlock is a real bug. The timeout is sized
    ///   to cover the driver's own
    ///   `2 * shutdown_drain_deadline` budget plus scheduler slack;
    ///   see the constant's doc for the rationale.
    ///
    /// # killed nodes are drained too
    ///
    /// Nodes killed via [`Self::kill`] now PARK their (still-aborted)
    /// [`JoinHandle`] in [`Self::killed_tasks`] instead of dropping
    /// it. After draining alive drivers, this method drains every
    /// parked handle so a `JoinError::is_panic()` from a pre-existing
    /// panic CANNOT vanish silently. Expected post-abort
    /// `JoinError::is_cancelled()` is tolerated.
    pub async fn shutdown(mut self) {
        // Stop the tick pump first so we don't keep poking dying
        // drivers. await the cancellation
        // — `JoinHandle::abort()` only schedules cancellation; without
        // the subsequent `await` the just-aborted task can still fire
        // one more `controller.trigger()` and race the driver
        // shutdown sequence below.
        if let Some(handle) = self.tick_pump.take() {
            handle.abort();
            let _ = handle.await;
        }
        for node in &self.nodes {
            if node.is_alive() {
                node.driver.shutdown();
            }
        }
        let mut failures: Vec<String> = Vec::new();
        for node in self.nodes.iter_mut() {
            let Some(task) = node.task.take() else {
                continue; // already killed via Self::kill (parked separately)
            };
            let node_id = node.node_id.0;
            match tokio::time::timeout(SHUTDOWN_TASK_TIMEOUT, task).await {
                Ok(Ok(Ok(()))) => {
                    // clean exit
                }
                Ok(Ok(Err(ref e))) if crate::teardown::is_allowed_teardown_noise(e) => {
                    tracing::warn!(
                        target: "xraft_test::simulated",
                        node = node_id,
                        error = %e,
                        "driver exited with allowed teardown noise \
                         (Windows tempdir race; cosmetic)"
                    );
                }
                Ok(Ok(Err(e))) => {
                    failures.push(format!(
                        "node {node_id}: driver returned unexpected XRaftError: {e}"
                    ));
                }
                Ok(Err(je)) if je.is_cancelled() => {
                    // Shouldn't reach (kill() parks the task in
                    // killed_tasks), but tolerate defensively.
                }
                Ok(Err(je)) if je.is_panic() => {
                    failures.push(format!("node {node_id}: driver task PANICKED: {je}"));
                }
                Ok(Err(je)) => {
                    failures.push(format!("node {node_id}: driver task join error: {je}"));
                }
                Err(_elapsed) => {
                    failures.push(format!(
                        "node {node_id}: driver did not exit within {:?} (possible shutdown deadlock)",
                        SHUTDOWN_TASK_TIMEOUT
                    ));
                }
            }
        }
        // drain parked killed
        // tasks. A pre-existing PANIC must surface; a post-abort
        // CANCELLED outcome is expected and tolerated.
        for (node_id, task) in self.killed_tasks.drain(..) {
            let nid = node_id.0;
            match tokio::time::timeout(SHUTDOWN_TASK_TIMEOUT, task).await {
                Ok(Ok(Ok(()))) => {
                    // Driver completed cleanly before the abort
                    // signal landed — acceptable.
                }
                Ok(Ok(Err(ref e))) if crate::teardown::is_allowed_teardown_noise(e) => {
                    tracing::warn!(
                        target: "xraft_test::simulated",
                        node = nid,
                        error = %e,
                        "killed driver exited with allowed teardown noise"
                    );
                }
                Ok(Ok(Err(e))) => {
                    failures.push(format!(
                        "node {nid} (killed): driver returned unexpected XRaftError before abort took effect: {e}"
                    ));
                }
                Ok(Err(je)) if je.is_panic() => {
                    failures.push(format!(
                        "node {nid} (killed): driver task PANICKED before abort took effect: {je}"
                    ));
                }
                Ok(Err(je)) if je.is_cancelled() => {
                    // Expected: kill() called abort() on this handle.
                }
                Ok(Err(je)) => {
                    failures.push(format!("node {nid} (killed): unexpected JoinError: {je}"));
                }
                Err(_elapsed) => {
                    // The runtime should resolve an aborted task
                    // promptly; if it doesn't within the same budget
                    // we apply to graceful driver exits, surface it.
                    failures.push(format!(
                        "node {nid} (killed): aborted task did not resolve within {:?}",
                        SHUTDOWN_TASK_TIMEOUT
                    ));
                }
            }
        }
        if !failures.is_empty() {
            panic!(
                "simulated cluster shutdown surfaced {} unexpected teardown failure(s): {}",
                failures.len(),
                failures.join("; ")
            );
        }
    }
}

/// tiny SplitMix64-style mixer used to
/// derive a per-node deterministic seed from `(cluster_seed, node_id)`.
/// Cheap, well-distributed, no external dep — and explicit so an
/// "are these seeds correlated?" reviewer can verify the bit
/// avalanche by inspection.
fn mix_seed(seed: u64, node_id: u64) -> u64 {
    let mut x = seed
        .wrapping_add(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(node_id.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_seed_is_distinct_across_nodes() {
        let s = 0xC0FF_EE01;
        let mut seen = std::collections::HashSet::new();
        for id in 1..=10 {
            let derived = mix_seed(s, id);
            assert!(
                seen.insert(derived),
                "mix_seed({s}, {id}) collided: {derived}"
            );
        }
    }

    #[test]
    fn mix_seed_is_deterministic() {
        let a = mix_seed(0xDEAD_BEEF, 3);
        let b = mix_seed(0xDEAD_BEEF, 3);
        assert_eq!(a, b);
    }
}
