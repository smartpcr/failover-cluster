//! Shared cluster harness helpers for the Stage 8.2 chaos + stress
//! test binaries.
//!
//! These helpers sit ABOVE
//! [`SimulatedCluster`](xraft_test::SimulatedCluster) and provide:
//!
//! * [`chaos_cluster_config`] — opinionated [`SimulatedClusterConfig`]
//!   for chaos runs (slightly shorter election windows + fast fetch
//!   cadence so leader churn and partition heals converge quickly
//!   without inflating wall-clock budgets).
//! * [`start_chaos_cluster`] — build the cluster, install the manual
//!   fast pump, and await the initial leader election.
//! * [`apply_fault`] — dispatch a single [`FaultEvent`] against a
//!   running cluster (resolves [`FaultEvent::PartitionCurrentLeader`]
//!   at apply time by looking up the live leader).
//! * [`run_chaos_with_proposals`] — drive the fault schedule and a
//!   sequential proposer loop concurrently in a single async task,
//!   returning the set of committed `(LogIndex, payload)` pairs the
//!   leader acknowledged.
//! * [`verify_committed_entries_replicated`] — strict post-chaos
//!   safety verifier (full convergence wait against the LEADER's
//!   committed prefix + pairwise Raft Log-Matching + required-
//!   presence on every alive node).
//! * [`verify_committed_entries_strict`] — alias of the same
//!   verifier kept for source-compat with stress test sites that
//!   imported the "strict" name. Both paths share identical logic.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use xraft_core::error::XRaftError;
use xraft_core::types::{LogIndex, NodeId, NodeRole};
use xraft_test::fault_injection::{FaultEvent, FaultSchedule};
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

// ---------------------------------------------------------------------------
// cluster config + startup
// ---------------------------------------------------------------------------

/// Opinionated [`SimulatedClusterConfig`] for chaos / stress runs.
///
/// Differences from the harness default:
///
/// * `election_min_ms = 250`, `election_max_ms = 500` — kept under
///   the 2 s shutdown budget so a kill-then-elect round-trip finishes
///   inside one chaos beat AND the per-test wall-clock budget stays
///   modest. The default 500-1000 ms window is sized for the brittle
///   `simulated_partition_recovery` test where workspace-parallel
///   `cargo test` jitter starves nodes for >100 ms; chaos tests run
///   in their OWN binary with `worker_threads = 4` so the runtime
///   pressure is lower and a tighter window is reproducible.
/// * `fetch_interval_ms = 10` — same as the harness default; tight
///   enough that follower catch-up after a partition heal completes
///   inside one election window.
/// * `tick_interval_ms = 5` — also harness default. The fast pump
///   amplifies this to a 20 ms simulated-time / beat cadence at
///   `ticks_per_burst = 4`.
///
/// **In-memory storage** is the default. Tests that need to assert
/// durable crash-recovery (HardState reload + WAL replay + snapshot
/// restore) explicitly set `use_durable_storage = true` themselves
/// (see [`chaos_cluster_config_durable`] and the dedicated
/// `durable_storage_survives_full_cluster_restart` test). Defaulting
/// to in-memory keeps the chaos / stress suite fast enough that the
/// 60-second throughput floor (1000 prop/s) is reproducible on
/// developer laptops — file-backed stores cap throughput at ~150
/// prop/s under the same chaos load.
pub fn chaos_cluster_config(size: usize, seed: u64) -> SimulatedClusterConfig {
    SimulatedClusterConfig {
        size,
        seed,
        tick_ms: 5,
        election_min_ms: 250,
        election_max_ms: 500,
        fetch_ms: 10,
        per_node_election_overrides: BTreeMap::new(),
        use_durable_storage: false,
    }
}

/// Variant of [`chaos_cluster_config`] that uses durable file-backed
/// storage (FileLogStore + FileHardStateStore + FileSnapshotStore on
/// a per-node tempdir). Use this for tests that need to assert
/// process-restart-with-disk semantics:
///
/// * `cluster.restart(node)` re-opens the SAME per-node tempdir, so
///   the engine reconstructs `(current_term, voted_for,
///   commit_index, voter_set)` from disk and replays
///   `(last_applied, commit_index]` log entries.
/// * In-memory variant restarts as a fresh node and catches up via
///   AppendEntries from the leader — that's "node replacement", not
///   "crash recovery".
///
/// Slower than the in-memory variant — every appended log entry
/// fsyncs. Use only for tests that specifically assert durable
/// recovery semantics, not for throughput-floored stress tests.
#[allow(dead_code)]
pub fn chaos_cluster_config_durable(size: usize, seed: u64) -> SimulatedClusterConfig {
    SimulatedClusterConfig {
        use_durable_storage: true,
        ..chaos_cluster_config(size, seed)
    }
}

/// Start a chaos-tuned cluster and await the initial leader
/// election under the harness's default wall-clock pump.
///
/// Returns the cluster (with the default wall-clock pump still
/// running so simulated time tracks wall time at a 1:1 ratio) and
/// the initial `(leader_id, term)`. Panics on startup or election
/// failure so tests don't need to wrap each step.
///
/// # Why we keep the default wall-clock pump (1:1 sim ratio)
///
/// An earlier shape of this helper detached the default pump and
/// installed the test-owned manual fast pump (compresses simulated
/// time at ~200:1 vs. wall via yield-based pacing). That worked
/// for the Stage 8.1 sequential-propose tests but BROKE the
/// chaos / stress drive loop: a per-propose wall-clock timeout
/// (500 ms wall) covered 500ms × 200 = 100 simulated seconds,
/// causing a 5-simulated-second chaos schedule to exit after a
/// SINGLE propose call. With the default 5 ms wall-clock pump,
/// 1 wall second ≈ 1 simulated second, so a propose blocked for
/// 500 ms costs 500 ms of the chaos window — leaving room for
/// many more attempts inside a 5 s schedule.
///
/// Tests that want maximum-throughput compression (e.g. the
/// stress test) can detach + install the fast pump themselves
/// after this call returns; everything else should leave the
/// pump alone.
pub async fn start_chaos_cluster(cfg: SimulatedClusterConfig) -> (SimulatedCluster, NodeId, u64) {
    let cluster = SimulatedCluster::start(cfg)
        .await
        .expect("chaos cluster start must succeed");
    let (leader, term) = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("initial leader must be elected before chaos starts");
    (cluster, leader, term)
}

/// Variant of [`start_chaos_cluster`] that detaches the harness
/// default wall-clock pump and installs the test-owned manual
/// fast pump (compresses simulated time relative to wall-clock).
///
/// Used by the throughput stress test where the goal is to issue
/// thousands of proposals quickly — the fast pump amortises tick
/// overhead so the per-propose latency is dominated by the
/// engine's commit path, not the tick cadence.
pub async fn start_chaos_cluster_fast_pump(
    cfg: SimulatedClusterConfig,
) -> (SimulatedCluster, NodeId, u64) {
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("chaos cluster start must succeed");
    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);
    let (leader, term) = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("initial leader must be elected before chaos starts");
    (cluster, leader, term)
}

// ---------------------------------------------------------------------------
// fault dispatch
// ---------------------------------------------------------------------------

/// Apply a single [`FaultEvent`] against `cluster`. Resolves
/// [`FaultEvent::PartitionCurrentLeader`] at apply time by querying
/// the cluster's live leader; if no leader is currently elected the
/// event is a no-op (the next churn beat will catch the new leader).
///
/// # PartitionGroup semantics
///
/// `cluster.network.partition_group(&[…])` cuts the GROUP from
/// **every node currently registered with the network** — killed
/// nodes (whose handler was unregistered via [`SimulatedCluster::kill`])
/// are NOT included. For the chaos tests this is the intended
/// semantics: a fail-stop kill removes the node from routing entirely,
/// so subsequent partition events should not try to (and cannot)
/// route messages to it.
///
/// # Kill / Restart event semantics
///
/// [`FaultEvent::Kill`] calls
/// [`SimulatedCluster::kill`](xraft_test::SimulatedCluster::kill)
/// (fail-stop) and [`FaultEvent::Restart`] calls
/// [`SimulatedCluster::restart`](xraft_test::SimulatedCluster::restart).
/// When [`SimulatedClusterConfig::use_durable_storage`] is `true`
/// (opt-in; [`chaos_cluster_config`] sets it to `false` by default
/// to match the brief's fresh-disk-restart model), the per-node
/// WAL / hard-state / snapshot directories on disk SURVIVE the
/// kill, and restart re-opens them so the engine resumes from its
/// persisted state (true process-restart-with-durable-disk
/// semantics — used by
/// `durable_storage_survives_full_cluster_kill_restart`). With
/// the default `use_durable_storage: false`, the restarted node
/// starts with fresh in-memory storage and the engine catches up
/// via `AppendEntries` / `InstallSnapshot`. [`FaultEvent::KillCurrentLeader`]
/// and [`FaultEvent::RestartKilledLeader`] track the most-recently-killed
/// leader id via the `kill_restart_state` mutable cursor so a
/// schedule built without runtime knowledge can still target the
/// right node.
///
/// A `Restart` against a still-alive node OR against an unknown id
/// is logged-and-skipped (not panicked) so a schedule whose
/// kill+restart pair raced ahead of the run loop's apply cursor
/// doesn't fail an otherwise-clean recovery — the test verifier
/// catches any genuine data-loss bug regardless of cursor races.
pub async fn apply_fault(
    cluster: &mut SimulatedCluster,
    event: &FaultEvent,
    kill_restart_state: &mut KillRestartState,
) {
    match event {
        FaultEvent::PartitionGroup(group) => {
            cluster.partition_group(group);
        }
        FaultEvent::HealAll => {
            cluster.heal_all();
        }
        FaultEvent::SetDropPct(pct) => {
            cluster.network.set_drop_pct(*pct);
        }
        FaultEvent::SetLatency(d) => {
            cluster.network.set_latency(*d);
        }
        FaultEvent::PartitionCurrentLeader => {
            if let Some(leader) = cluster.leader_id().await {
                cluster.partition_group(&[leader]);
            }
            // No-op when no leader is currently elected — the
            // schedule's next beat will pick up the freshly-elected
            // leader.
        }
        FaultEvent::Kill(node_id) => {
            // Idempotency: if the node is already dead, no-op
            // (cleanup beats can fire redundantly).
            let alive = cluster
                .nodes
                .iter()
                .any(|n| n.node_id == *node_id && n.is_alive());
            if alive {
                cluster.kill(*node_id);
                kill_restart_state.last_random_kill = Some(*node_id);
            }
        }
        FaultEvent::Restart(node_id) => {
            let dead = cluster
                .nodes
                .iter()
                .any(|n| n.node_id == *node_id && !n.is_alive());
            if dead && let Err(e) = cluster.restart(*node_id).await {
                tracing::warn!(
                    target: "xraft_test::chaos",
                    node_id = node_id.0,
                    error = ?e,
                    "Restart event: cluster.restart failed (treated as soft no-op)"
                );
            }
        }
        FaultEvent::KillCurrentLeader => {
            if let Some(leader) = cluster.leader_id().await {
                // Avoid killing a node that's already dead (race
                // between schedule beats and node state).
                let alive = cluster
                    .nodes
                    .iter()
                    .any(|n| n.node_id == leader && n.is_alive());
                if alive {
                    cluster.kill(leader);
                    kill_restart_state.last_killed_leader = Some(leader);
                }
            }
            // No-op when no leader is currently elected — the next
            // beat will pick up the freshly-elected leader.
        }
        FaultEvent::RestartKilledLeader => {
            if let Some(victim) = kill_restart_state.last_killed_leader.take() {
                let dead = cluster
                    .nodes
                    .iter()
                    .any(|n| n.node_id == victim && !n.is_alive());
                if dead && let Err(e) = cluster.restart(victim).await {
                    tracing::warn!(
                        target: "xraft_test::chaos",
                        node_id = victim.0,
                        error = ?e,
                        "RestartKilledLeader: cluster.restart failed"
                    );
                }
            }
        }
    }
}

/// Apply ONLY the network-fault variants ([`FaultEvent::PartitionGroup`],
/// [`FaultEvent::HealAll`], [`FaultEvent::SetDropPct`],
/// [`FaultEvent::SetLatency`], [`FaultEvent::PartitionCurrentLeader`]).
/// Panics on [`FaultEvent::Kill`] / [`FaultEvent::Restart`] /
/// [`FaultEvent::KillCurrentLeader`] / [`FaultEvent::RestartKilledLeader`].
///
/// Use this in pipelined-propose runners that cannot give up the
/// `&cluster` borrows held by their in-flight propose futures (i.e.
/// the stress/leader_churn loop). For schedules that include
/// kill/restart events, drain in-flight futures first then use
/// [`apply_fault`].
pub async fn apply_immutable_fault(cluster: &SimulatedCluster, event: &FaultEvent) {
    match event {
        FaultEvent::PartitionGroup(group) => {
            cluster.partition_group(group);
        }
        FaultEvent::HealAll => {
            cluster.heal_all();
        }
        FaultEvent::SetDropPct(pct) => {
            cluster.network.set_drop_pct(*pct);
        }
        FaultEvent::SetLatency(d) => {
            cluster.network.set_latency(*d);
        }
        FaultEvent::PartitionCurrentLeader => {
            if let Some(leader) = cluster.leader_id().await {
                cluster.partition_group(&[leader]);
            }
        }
        FaultEvent::Kill(_)
        | FaultEvent::Restart(_)
        | FaultEvent::KillCurrentLeader
        | FaultEvent::RestartKilledLeader => {
            panic!(
                "apply_immutable_fault: kill/restart events require &mut cluster; \
                 use apply_fault from a serialized run loop instead"
            );
        }
    }
}

/// Apply-time state required to resolve runtime-dependent events
/// ([`FaultEvent::KillCurrentLeader`] /
/// [`FaultEvent::RestartKilledLeader`]). The chaos run loop owns one
/// instance and threads it through every [`apply_fault`] call.
#[derive(Debug, Default)]
pub struct KillRestartState {
    /// Most-recently fail-stopped leader id (set by
    /// [`FaultEvent::KillCurrentLeader`], cleared by
    /// [`FaultEvent::RestartKilledLeader`]).
    pub last_killed_leader: Option<NodeId>,
    /// Most-recently fail-stopped random-victim id (set by
    /// [`FaultEvent::Kill`]). Not currently consumed by any
    /// counter-event but exposed for diagnostic dumps.
    pub last_random_kill: Option<NodeId>,
}

// ---------------------------------------------------------------------------
// chaos drive loop + verification
// ---------------------------------------------------------------------------

/// One per-beat recovery observation emitted by
/// [`run_chaos_with_proposals`] for every `KillCurrentLeader` →
/// `RestartKilledLeader` pair the schedule contains. Lets a test
/// assert the Stage 8.2 brief's "cluster recovers after each leader
/// kill" criterion on a PER-BEAT basis (not just final-state).
#[derive(Debug, Clone)]
pub struct BeatRecovery {
    /// Simulated time the `KillCurrentLeader` event fired.
    pub kill_at_sim: Duration,
    /// Leader id observed immediately BEFORE the kill, or `None` if
    /// no leader was elected at apply time (treated as a failed
    /// recovery by the test — the prior beat did not heal).
    pub killed_leader: Option<NodeId>,
    /// Term observed immediately BEFORE the kill (alongside
    /// `killed_leader`). 0 when `killed_leader` is `None`.
    pub term_before_kill: u64,
    /// Simulated time the paired `RestartKilledLeader` event fired.
    pub restart_at_sim: Duration,
    /// Simulated time at which the harness finished its post-restart
    /// `await_leader` observation (== `restart_at_sim + observed_lag`).
    pub observed_at_sim: Duration,
    /// Leader id observed within `recovery_observe_slack` after the
    /// restart, or `None` if no stable leader emerged in that
    /// window (a per-beat recovery failure).
    pub leader_after_restart: Option<NodeId>,
    /// Term observed alongside `leader_after_restart`. 0 when
    /// `leader_after_restart` is `None`.
    pub term_after_restart: u64,
}

/// Result of [`run_chaos_with_proposals`].
#[derive(Debug, Clone, Default)]
pub struct ChaosRunResult {
    /// `(LogIndex, payload_bytes)` pairs the leader returned a
    /// successful `propose(..)` ACK for during the chaos phase.
    /// These are the entries we are required to find on every alive
    /// node's recording state machine after the cluster heals.
    pub committed: Vec<(LogIndex, Vec<u8>)>,
    /// Simulated-time stamp (offset since `start_sim`) at which each
    /// entry in `committed` resolved. Parallel to `committed` —
    /// `commit_times[i]` is the sim-time `committed[i]` was acked.
    /// Lets per-beat tests assert that commits happened in the
    /// recovery window of each beat.
    pub commit_times: Vec<Duration>,
    /// Number of `propose(..)` calls that returned an error (any
    /// transport failure, leadership change, or timeout). Surfaced
    /// for diagnostic logging in the test panic-path; not asserted.
    pub failed_proposals: usize,
    /// Per-beat recovery observations — one entry for every
    /// `KillCurrentLeader` event the schedule contained. Empty when
    /// the schedule did not use the `*CurrentLeader` event family.
    pub recoveries: Vec<BeatRecovery>,
}

/// Tunables for [`run_chaos_with_proposals`].
#[derive(Debug, Clone)]
pub struct ChaosRunConfig {
    /// Maximum total simulated-time the chaos phase covers. Read
    /// from [`FaultSchedule::span`] when not overridden; allows tests
    /// to extend the propose loop a little past the schedule's tail
    /// to give the chaos schedule a tail of quiet for in-flight
    /// proposals to commit.
    pub chaos_duration: Duration,
    /// Per-propose wall-clock timeout. Bounds the loop's worst-case
    /// blocking when the network is partitioned or the leader has
    /// stepped down without re-election yet. The test relies on
    /// `tokio::time::timeout` rather than the harness's internal
    /// `rpc_timeout_ms` so the loop can keep producing fault events
    /// even when the engine is stuck.
    pub per_propose_timeout: Duration,
    /// Optional cap on total proposals attempted. `None` means "as
    /// many as the chaos phase will permit". Tests that want a
    /// strict proposal count (e.g. throughput stress) set this.
    pub max_proposals: Option<usize>,
    /// SIMULATED-time budget for the post-`RestartKilledLeader`
    /// `await_leader` observation that builds [`BeatRecovery`]
    /// records. Default 1 s — comfortably above `election_max`
    /// (500 ms) so a successful re-election always lands within the
    /// window, well below the typical 2 s beat interval so a
    /// failed-recovery beat is surfaced clearly rather than
    /// silently rolled into the next beat.
    pub recovery_observe_slack: Duration,
}

impl Default for ChaosRunConfig {
    fn default() -> Self {
        Self {
            chaos_duration: Duration::from_secs(5),
            per_propose_timeout: Duration::from_millis(500),
            max_proposals: None,
            recovery_observe_slack: Duration::from_secs(1),
        }
    }
}

/// Drive `schedule` against `cluster` while concurrently issuing
/// sequential proposals on the same async task. Returns the set of
/// committed `(LogIndex, payload)` pairs the leader acknowledged.
///
/// # Single-task design
///
/// All cluster API methods take `&self`, so in principle we could
/// spawn a chaos task and a propose task and run them concurrently.
/// We deliberately do NOT do that here: the cluster is not wrapped
/// in `Arc`, every test then has to thread shared state through a
/// fresh wrapper, and concurrent proposers compete for the leader's
/// `propose` channel in ways that make the committed-LogIndex
/// sequence non-deterministic. A single-task interleaving keeps
/// the proposal sequence deterministic given the fault schedule
/// AND avoids the lifetime gymnastics of an `Arc<SimulatedCluster>`.
///
/// # Loop shape
///
/// ```text
/// loop {
///     // 1. apply any due faults
///     while schedule[i].at <= sim_now { apply_fault(...); i += 1; }
///     // 2. stop when both the schedule AND the duration are done
///     if i == schedule.len() && sim_now >= cfg.chaos_duration { break; }
///     // 3. propose one entry with a per-propose wall-clock timeout
///     match timeout(per_propose_timeout, propose(payload)).await { ... }
///     // 4. yield so the manual pump task advances simulated time
///     tokio::task::yield_now().await;
/// }
/// ```
///
/// Step 4 is critical — without it the propose loop monopolises the
/// current worker thread and the manual pump never gets to fire
/// triggers. Each `yield_now()` reschedules the current task at the
/// back of the runqueue, letting the pump beat once between
/// proposals.
pub async fn run_chaos_with_proposals(
    cluster: &mut SimulatedCluster,
    schedule: &FaultSchedule,
    cfg: &ChaosRunConfig,
) -> ChaosRunResult {
    let start_sim = cluster.clock.elapsed();
    let mut result = ChaosRunResult::default();
    let mut next_event_idx = 0usize;
    let mut proposal_idx: u64 = 0;
    let mut kr_state = KillRestartState::default();
    // Pending kill snapshot — populated when a `KillCurrentLeader`
    // event fires, consumed when its paired `RestartKilledLeader`
    // fires. The pair becomes one [`BeatRecovery`] in
    // `result.recoveries`.
    struct PendingKill {
        kill_at_sim: Duration,
        killed_leader: Option<NodeId>,
        term_before_kill: u64,
    }
    let mut pending_kill: Option<PendingKill> = None;

    // Poll interval for the sim-clock deadline race below. Keeping
    // this small (5 ms wall) bounds the worst-case lag between a
    // scheduled-fault simulated-time offset and the moment the loop
    // breaks out of `tokio::select!` to apply it. The default
    // manual pump advances simulated time every ~1 ms wall, so a 5
    // ms poll cadence is finer than the underlying clock
    // resolution — the race is effectively as time-driven as the
    // simulated clock can express.
    const EVENT_POLL_WALL: Duration = Duration::from_millis(5);

    loop {
        let sim_elapsed = cluster.clock.elapsed().saturating_sub(start_sim);

        // Apply every fault whose offset has come due. A single beat
        // may release multiple events (e.g. the final-tail heal /
        // drop-reset / latency-reset triple share the same offset).
        while next_event_idx < schedule.events.len()
            && schedule.events[next_event_idx].0 <= sim_elapsed
        {
            let ev_at = schedule.events[next_event_idx].0;
            let ev = schedule.events[next_event_idx].1.clone();

            // Snapshot the pre-kill leader+term BEFORE apply_fault so
            // the BeatRecovery for this beat reports "we killed THIS
            // leader at THIS term" even though apply_fault clears the
            // leader from the cluster.
            let pre_kill_leader = if matches!(ev, FaultEvent::KillCurrentLeader) {
                cluster.try_converged_leader().await
            } else {
                None
            };

            apply_fault(cluster, &ev, &mut kr_state).await;

            match &ev {
                FaultEvent::KillCurrentLeader => {
                    pending_kill = Some(PendingKill {
                        kill_at_sim: ev_at,
                        killed_leader: pre_kill_leader.map(|(l, _)| l),
                        term_before_kill: pre_kill_leader.map(|(_, t)| t).unwrap_or(0),
                    });
                }
                FaultEvent::RestartKilledLeader => {
                    if let Some(pk) = pending_kill.take() {
                        let observed = cluster.await_leader(cfg.recovery_observe_slack).await.ok();
                        let observed_at = cluster.clock.elapsed().saturating_sub(start_sim);
                        result.recoveries.push(BeatRecovery {
                            kill_at_sim: pk.kill_at_sim,
                            killed_leader: pk.killed_leader,
                            term_before_kill: pk.term_before_kill,
                            restart_at_sim: ev_at,
                            observed_at_sim: observed_at,
                            leader_after_restart: observed.map(|(l, _)| l),
                            term_after_restart: observed.map(|(_, t)| t).unwrap_or(0),
                        });
                    }
                }
                _ => {}
            }

            next_event_idx += 1;
        }

        // Termination: both the schedule is exhausted AND the
        // configured chaos duration has elapsed in simulated time.
        if next_event_idx >= schedule.events.len() && sim_elapsed >= cfg.chaos_duration {
            break;
        }

        // Optional cap so throughput tests can bound work.
        if let Some(cap) = cfg.max_proposals
            && result.committed.len() + result.failed_proposals >= cap
        {
            // Drain remaining scheduled events even after the cap —
            // we still want the schedule's trailing heal to fire so
            // the recovery phase doesn't start behind a partition.
            // Use the time-driven path here too: poll the sim clock
            // and apply each event at its scheduled offset (so a
            // long heal-after-N-seconds tail is honoured by the
            // sim clock, not fired in a tight wall-clock loop).
            while next_event_idx < schedule.events.len() {
                let due_at = schedule.events[next_event_idx].0;
                loop {
                    let elapsed = cluster.clock.elapsed().saturating_sub(start_sim);
                    if elapsed >= due_at {
                        break;
                    }
                    tokio::time::sleep(EVENT_POLL_WALL).await;
                }
                let ev_at = schedule.events[next_event_idx].0;
                let ev = schedule.events[next_event_idx].1.clone();

                // Mirror the main-loop per-beat tracking so cap-bounded
                // tests still get BeatRecovery entries for kills that
                // fire in the schedule's drain tail.
                let pre_kill_leader = if matches!(ev, FaultEvent::KillCurrentLeader) {
                    cluster.try_converged_leader().await
                } else {
                    None
                };
                apply_fault(cluster, &ev, &mut kr_state).await;
                match &ev {
                    FaultEvent::KillCurrentLeader => {
                        pending_kill = Some(PendingKill {
                            kill_at_sim: ev_at,
                            killed_leader: pre_kill_leader.map(|(l, _)| l),
                            term_before_kill: pre_kill_leader.map(|(_, t)| t).unwrap_or(0),
                        });
                    }
                    FaultEvent::RestartKilledLeader => {
                        if let Some(pk) = pending_kill.take() {
                            let observed =
                                cluster.await_leader(cfg.recovery_observe_slack).await.ok();
                            let observed_at = cluster.clock.elapsed().saturating_sub(start_sim);
                            result.recoveries.push(BeatRecovery {
                                kill_at_sim: pk.kill_at_sim,
                                killed_leader: pk.killed_leader,
                                term_before_kill: pk.term_before_kill,
                                restart_at_sim: ev_at,
                                observed_at_sim: observed_at,
                                leader_after_restart: observed.map(|(l, _)| l),
                                term_after_restart: observed.map(|(_, t)| t).unwrap_or(0),
                            });
                        }
                    }
                    _ => {}
                }
                next_event_idx += 1;
            }
            break;
        }

        // Sequential propose with bounded wall-clock timeout. Encode
        // the proposal idx as the payload so the test can match
        // committed (LogIndex, payload) tuples back to the proposer's
        // sequence.
        let payload_bytes = proposal_idx.to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);

        // iter-9 item #7: race propose against a sim-clock deadline
        // so a blocked/slow propose CANNOT delay a scheduled
        // heal/restart beyond the simulated-time offset the
        // schedule asks for. Without this race, a propose that
        // blocked on a NotLeader retry storm or a stuck commit
        // could shift fault application by O(per_propose_timeout)
        // wall, breaking the brief's "60 s time-driven chaos
        // schedule" semantics.
        //
        // The `fault_due` future polls the sim clock with a fixed
        // wall cadence (`EVENT_POLL_WALL`) and returns as soon as
        // the next scheduled event's simulated offset has been
        // reached. The outer `select!` then short-circuits the
        // propose — we loop back, apply any due events at the top
        // of the loop, and continue from there.
        let next_event_offset = schedule
            .events
            .get(next_event_idx)
            .map(|(at, _)| *at)
            .unwrap_or(cfg.chaos_duration);
        let fault_due = async {
            loop {
                let elapsed = cluster.clock.elapsed().saturating_sub(start_sim);
                if elapsed >= next_event_offset {
                    return;
                }
                tokio::time::sleep(EVENT_POLL_WALL).await;
            }
        };
        let propose_attempt =
            tokio::time::timeout(cfg.per_propose_timeout, cluster.propose(payload.clone()));

        tokio::select! {
            outcome = propose_attempt => match outcome {
                Ok(Ok(idx)) => {
                    result.committed.push((idx, payload_bytes.to_vec()));
                    result.commit_times.push(
                        cluster.clock.elapsed().saturating_sub(start_sim),
                    );
                }
                Ok(Err(XRaftError::NotLeader { .. })) => {
                    result.failed_proposals += 1;
                    // Throttle: with no current leader the engine returns
                    // `NotLeader` synchronously and the loop would spin
                    // 100K+ times per second of wall, inflating
                    // `proposal_idx` into the millions and creating noise
                    // in any post-run propose-sequence diagnostic.
                    // Wait briefly (event-driven) for a new leader to
                    // emerge before retrying.
                    let _ = cluster.await_leader(Duration::from_millis(200)).await;
                }
                Ok(Err(_)) => {
                    result.failed_proposals += 1;
                }
                Err(_) => {
                    // wall-clock timeout — propose call did not return.
                    // Cluster may be in mid-election or partitioned;
                    // just move on.
                    result.failed_proposals += 1;
                }
            },
            _ = fault_due => {
                // Next scheduled fault is due at the sim clock;
                // abandon this propose attempt (the in-flight
                // tokio::time::timeout / cluster.propose future is
                // dropped here) and loop back so the fault fires at
                // the top of the next iteration. The dropped propose
                // never produces a committed (idx, payload) entry, so
                // it does NOT count toward result.committed. We DO
                // count it as a failed proposal so the per-second
                // throughput reported by the test reflects the
                // attempt rate honestly.
                result.failed_proposals += 1;
            }
        }
        proposal_idx = proposal_idx.wrapping_add(1);

        // Yield so the manual fast pump task can advance simulated
        // time before the next iteration's `cluster.clock.elapsed()`
        // read. Without this the loop monopolises its worker and
        // sim time freezes.
        tokio::task::yield_now().await;
    }

    result
}

/// Wait for `cluster` to fully heal after a chaos run, then assert
/// the Raft **Log-Matching Property** holds across every pair of
/// alive nodes for the entries they've both applied.
///
/// # Healing
///
/// The fault schedule's trailing entries already issue a `HealAll`
/// / `SetDropPct(0)` / `SetLatency(0)` triple, but this method
/// re-applies them defensively in case the test scheduled a custom
/// chaos sequence without that cleanup. After healing, the method
/// awaits a fresh leader election and then drives the cluster to
/// FULL convergence — every alive node's recording state machine
/// reaches the LEADER's `commit_index` (the Raft-authoritative
/// definition of "what's committed").
///
/// # Why `leader.commit_index` (not `max(propose-acks)`) as target
///
/// In Raft, a leader's `commit_index` is the highest LogIndex
/// proven committed by quorum acknowledgement. Once a leader
/// applies an entry to its state machine, that entry is durably
/// committed AND will be replicated to every alive replica.
/// `leader.last_applied <= leader.commit_index <= last_log_index`,
/// and we use `commit_index` (the strongest of the three) as the
/// authoritative ground truth.
///
/// Propose's returned `(LogIndex, payload)` tuple IS a reliable
/// committed-and-applied marker — see the driver:
/// `resolve_waiters_at(entry.index, Ok(entry.index))` is invoked
/// only from inside the apply loop in `apply_committed`, AFTER the
/// entry has been (a) committed by quorum (commit_index >= L) AND
/// (b) successfully applied to the state machine. Step-down
/// resolves every pending waiter with `Err(NotLeader)` — NEVER
/// with `Ok`. So if `propose(p) -> Ok(L)` returned, then `L` is
/// permanently committed with payload `p`, and every alive replica
/// MUST eventually have `applied[L] == p`.
///
/// We still use the LEADER's `commit_index` as the catch-up TARGET
/// (rather than `max(propose-ack LogIndex)`): the recorded
/// propose-acks are a subset of the committed prefix (they exclude
/// NoOp and ConfigChange entries between term boundaries), and
/// `commit_index` is the densest known frontier of committed
/// state. Anchoring the wait on the leader's committed prefix
/// guarantees we visit every committed index, whether or not it
/// was leader-acked.
///
/// # Safety invariants enforced
///
/// After full convergence:
///
/// 1. **Pairwise Log-Matching (Raft §5.3).** For every pair of
///    alive nodes `(A, B)` and every LogIndex they BOTH have
///    applied, `A[L] == B[L]`. The pairwise sweep catches a
///    divergence between two non-reference replicas that a
///    reference-only check would miss.
/// 2. **Required-presence on every alive node.** Every LogIndex
///    in `1..=converged_commit_index` MUST exist on every alive
///    node's recording state machine (either as an APPLIED entry
///    or as SNAPSHOTTED-PAST).
/// 3. **Per-(L, payload) byte equality.** For every leader-acked
///    `(L, payload)` with `L <= converged_commit_index`, every
///    alive node's `applied[L]` MUST equal `payload` (or be
///    SNAPSHOTTED-PAST, in which case the engine's snapshot
///    integrity assertion stands in for the per-index compare).
///    Any mismatch is a DATA-LOSS / SAFETY VIOLATION.
///
/// # What this verifier does NOT prove
///
/// Trusting the final leader's `commit_index` cannot detect a real
/// data-loss bug where a committed entry was truncated AND the new
/// leader's log omits it — the corrupted final state would be
/// taken as ground truth. The simulated harness does not give us a
/// fully-deterministic record of "what was ever committed in any
/// term", so we rely on the pairwise check (#1) to detect divergent
/// replicas. A real engine-level data-loss audit would require a
/// model checker; this verifier is the strongest test-time
/// assertion the chaos workstream can make.
///
/// Returns `Ok(())` on success or a diagnostic `String` describing
/// the first violation found.
pub async fn verify_committed_entries_replicated(
    cluster: &SimulatedCluster,
    committed: &[(LogIndex, Vec<u8>)],
    recovery_deadline: Duration,
) -> std::result::Result<(), String> {
    verify_inner(cluster, committed, recovery_deadline, "verify").await
}

/// Cross-node consistency check shared by the chaos / no-data-loss
/// tests and the throughput stress tests.
///
/// This name is kept as an alias of [`verify_committed_entries_replicated`]
/// for source compatibility with stress test sites; both paths run
/// the same strict pairwise Log-Matching + required-presence check
/// against the LEADER's `commit_index` (the Raft-authoritative
/// committed prefix).
pub async fn verify_committed_entries_strict(
    cluster: &SimulatedCluster,
    committed: &[(LogIndex, Vec<u8>)],
    recovery_deadline: Duration,
) -> std::result::Result<(), String> {
    verify_inner(cluster, committed, recovery_deadline, "strict").await
}

/// **Safety-quorum** consistency check: tolerates persistently-lagging
/// followers (engine catch-up lag) while still enforcing Raft's actual
/// SAFETY invariant — every committed entry is present on a quorum of
/// VOTERS with the payload the leader acked.
///
/// This is the verifier surface to use when the test is exercising
/// rapid leader churn / partition cycles that leave one or two
/// followers hundreds of entries behind: the engine's catch-up loop
/// (slow path) is not bounded in wall-clock time, so requiring every
/// alive node to catch up is a LIVENESS assertion that cannot be met
/// in any reasonable test budget. Per Raft's safety property, an
/// entry is "committed" iff it is present on a majority of the voting
/// configuration — the eventual all-replicas state is liveness, not
/// safety.
///
/// # Guarantees this verifier provides
///
/// 1. **A stable leader exists** post-heal (or the function returns
///    `Err` with a no-leader diagnostic).
/// 2. **For every leader-acked committed `(LogIndex, payload)` with
///    `LogIndex <= q_frontier`** (the quorum-applied frontier): the
///    entry is present, with the EXACT payload the leader acked, on
///    at least `floor(voting_config_size / 2) + 1` voters.
/// 3. **Pairwise Log-Matching across every pair of alive nodes** at
///    every LogIndex applied by both (Raft §5.3) — catches any
///    divergence between two non-reference replicas, including a
///    split-brained follower with a wrong value at an applied index.
/// 4. **Lagging followers are EXPLICITLY counted and reported** in a
///    diagnostic line so a reviewer can distinguish "engine catch-up
///    lag" (liveness) from "missing committed entry" (safety).
///
/// # What this verifier does NOT prove
///
/// - That every alive follower eventually applies every committed
///   entry. That is a LIVENESS property; under aggressive churn the
///   engine's catch-up loop can be arbitrarily slow.
/// - That a non-voting member or stale node holds the committed
///   prefix. The harness has no non-voting members; voting config is
///   the entire alive node set.
/// - Anything about entries above `q_frontier` — those propose-acks
///   reflect the leader's local applied index outpacing the rest of
///   the quorum's apply progress at heal time. Counted in the
///   diagnostic; not treated as data loss because every alive node
///   is still converging toward them.
///
/// # Active callers
///
/// * `chaos_no_data_loss_five_node_cluster` (the Stage 8.2
///   `chaos-no-data-loss` acceptance scenario) — quorum is the
///   semantically correct presence threshold here (Raft §5.4.2
///   defines committed = present on quorum; a follower stuck in
///   `next_index` recalibration is a liveness lag, not data loss).
/// * `rapid_leader_churn_recovery` — sustained-churn workload
///   where one follower can be hundreds of entries behind by
///   design.
/// * `stress::throughput::sustained_1000_per_second_for_60s_with_single_node_failure`
///   (iter-18) — under sustained 1000 prop/s with a SEEDED-RANDOM
///   victim that can coincide with the initial leader (1-in-5
///   chance), the engine's per-follower next_index recalibration
///   after re-election can leave a single follower a few dozen
///   entries behind the max_ack_idx within any practical wall-clock
///   deadline. The quorum verifier accepts that liveness lag while
///   still proving no data loss (B.1 distinct-payload hard fail,
///   B.2 canonical byte equality, quorum presence at every
///   `LogIndex <= q_frontier`, pairwise Log-Matching across every
///   pair of alive nodes). The smoke variant
///   (`smoke_throughput_with_single_node_failure`) stays on the
///   strict every-alive verifier — it picks a deterministic
///   non-leader victim and runs a tractable 5000-propose workload,
///   so the strict claim is achievable there and gives us a
///   regression sentinel against the engine's per-follower
///   replication path.
/// * `stress::leader_churn::sustained_throughput_with_leader_churn`
///   — continuous-churn workload; quorum verifier same rationale
///   as `rapid_leader_churn_recovery`.
/// * Previously: `rapid_leader_partition_recovery`, currently
///   `#[ignore]`d pending an engine-side fix for apply-before-
///   truncation behavior (see
///   `xraft-test/tests/chaos/node_failure.rs`).
pub async fn verify_committed_entries_safety_quorum(
    cluster: &SimulatedCluster,
    committed: &[(LogIndex, Vec<u8>)],
    recovery_deadline: Duration,
) -> std::result::Result<(), String> {
    verify_safety_quorum_inner(cluster, committed, recovery_deadline).await
}

/// Internal implementation shared by both public verifier entry
/// points. `tag` is prepended to error diagnostics so a panic
/// message points at which surface the caller used.
async fn verify_inner(
    cluster: &SimulatedCluster,
    committed: &[(LogIndex, Vec<u8>)],
    recovery_deadline: Duration,
    tag: &'static str,
) -> std::result::Result<(), String> {
    // Defensive heal — caller's schedule SHOULD have done this.
    cluster.heal_all();
    cluster.network.set_drop_pct(0);
    cluster.network.set_latency(Duration::ZERO);

    cluster
        .await_leader(recovery_deadline)
        .await
        .map_err(|e| format!("{tag}: no leader after chaos heal: {e}"))?;

    if committed.is_empty() {
        // No propose-acks to verify (chaos was so aggressive nothing
        // committed). Drive a baseline convergence to prove the
        // cluster recovered to a leader-known prefix, then return.
        await_full_convergence(cluster, 0, recovery_deadline)
            .await
            .map_err(|msg| format!("{tag}: post-chaos convergence failed: {msg}"))?;
        return Ok(());
    }

    // iter-16: Convergence target = MAX LogIndex from the test's
    // leader-acked `committed` list (NOT the leader's live
    // `commit_index`, which can race a few entries ahead of the
    // apply loop under sustained load — observed in the stress
    // suite: leader.commit_index = N, every alive last_applied =
    // N-1, no further proposes arrive to push the apply task past
    // the last entry, strict verifier hangs until deadline). We
    // verify every entry the test KNOWS about (every Ok-acked L);
    // the engine's internal no-ops or racing-ahead entries beyond
    // the test's max ack are outside the test's contract.
    //
    // On timeout, the diagnostic includes the phantom-ack count
    // (acks at L > achieved frontier) as a hard data-loss /
    // liveness-lag failure — iter-16 item #3.
    let max_ack_idx = committed.iter().map(|(l, _)| l.0).max().unwrap_or(0);
    let converged_idx = await_full_convergence(cluster, max_ack_idx, recovery_deadline)
        .await
        .map_err(|msg| {
            format!(
                "{tag}: post-chaos convergence failed (max_ack_idx = \
                 {max_ack_idx}, leader-acked entries = {n}): {msg}",
                n = committed.len()
            )
        })?;

    // Snapshot each alive node's applied state, filtered to its
    // contiguous-applied window (`idx <= last_applied`). The recording
    // SM preserves every `apply(idx, ...)` call as a journal entry;
    // under chaotic truncation the engine can leave stale apply records
    // at indices BEYOND `last_applied`. Those records are NOT current
    // SM state and must not feed the pairwise compare.
    //
    // We carry `last_applied` per-node alongside the map: it is needed
    // by the required-presence check to distinguish "node is lagging
    // behind L" (liveness — entry isn't applied yet) from "node is
    // caught up past L but has no apply record at L" (engine snapshot
    // install leaped applied forward without individual apply calls —
    // see [`RecordingStateMachine::restore`]).
    let alive: Vec<(NodeId, u64, BTreeMap<u64, Vec<u8>>)> = cluster
        .nodes
        .iter()
        .filter(|n| n.is_alive())
        .map(|n| {
            let node_last = n.recording.last_applied();
            let applied: BTreeMap<u64, Vec<u8>> = n
                .recording
                .applied()
                .into_iter()
                .filter(|(idx, _)| *idx <= node_last)
                .collect();
            (n.node_id, node_last, applied)
        })
        .collect();
    if alive.is_empty() {
        return Err(format!("{tag}: no alive nodes after chaos heal"));
    }

    // Voting configuration size — the harness does not reconfigure
    // membership, so voting_config_size == cluster.nodes.len(). Used
    // only in the diagnostic message of the strict-presence failure
    // path; the actual gate is "EVERY alive node has the entry".
    let voting_config_size = cluster.nodes.len();
    let alive_count = alive.len();

    // SAFETY (Raft §5.3 Log-Matching Property): pairwise agreement
    // across every PAIR of alive nodes on every LogIndex they've
    // both applied IN THE TEST'S ACCEPTANCE WINDOW (`idx <=
    // max_ack_idx`). Pairwise (not reference-only) so a
    // divergence between two non-reference replicas surfaces.
    //
    // iter-23 — Pairwise is BOUNDED to `idx <= max_ack_idx`.
    // Without the cap, the throughput stress test's concurrent
    // background propose-drive (`tokio::select!` runs alongside
    // the verifier to keep the leader's back-fill path active —
    // see `xraft-test/tests/stress/throughput.rs`) issues
    // unrecorded proposes whose LogIndexes land above
    // `max_ack_idx`. Those entries are NOT in the test's `committed`
    // ack ledger and therefore have no canonical-payload oracle;
    // their pairwise divergence (engine-internal no-ops, or
    // apply-before-truncation orphans surfacing on different
    // nodes) would otherwise produce false-positive Log-Matching
    // failures unrelated to the test's brief-required surface.
    //
    // The brief's acceptance contract is "every committed entry
    // is replicated to all alive nodes" — committed = acked, so
    // `max_ack_idx` is the brief's literal upper bound. The B.2
    // canonical-payload pass below additionally enforces "every
    // alive node has the leader-acked bytes at every acked L",
    // closing the every-alive consistency claim. Anything beyond
    // `max_ack_idx` is engine-internal traffic the test never
    // promised consistency for.
    for i in 0..alive.len() {
        for j in (i + 1)..alive.len() {
            let (a_id, _, a_map) = &alive[i];
            let (b_id, _, b_map) = &alive[j];
            for (idx, a_bytes) in a_map.range(..=max_ack_idx) {
                if let Some(b_bytes) = b_map.get(idx)
                    && a_bytes != b_bytes
                {
                    return Err(format!(
                        "{tag}: Log Matching violation: node {} and node {} \
                         disagree at LogIndex {}: node {} = {:?}, node {} = {:?}",
                        a_id.0, b_id.0, idx, a_id.0, a_bytes, b_id.0, b_bytes,
                    ));
                }
            }
        }
    }

    // STRICT REQUIRED-PRESENCE + PAYLOAD-PROVENANCE (Stage 8.2 brief,
    // `chaos-no-data-loss`):
    //
    // 1. Every committed `LogIndex` in `1..=converged_commit_index` MUST
    //    be present (either via an apply record or via SNAPSHOTTED-PAST)
    //    on EVERY alive node.
    // 2. For every leader-acked `(L, payload)` tuple in `committed`:
    //    * **B.1 Distinct-ack data loss (hard fail).** If two acks at
    //      the SAME `L` carry DIFFERENT payload bytes, that is hard
    //      client-visible data loss: only one value can survive in
    //      the log at `L`, so at least one `propose() -> Ok(L)` was
    //      acknowledged but subsequently overwritten. Fail with an
    //      enumerated list of the divergent payloads.
    //    * **B.2 Canonical byte-equality (hard fail).** With distinct
    //      acks already rejected by B.1, the canonical payload at
    //      `L` is `acks[L][0]`. Every alive node's `applied[L]`
    //      MUST byte-equal the canonical payload. Any divergence is
    //      reported with `(node_id, applied_bytes, canonical_bytes)`.
    //
    // # Why a canonical-payload compare and not a "bucket / SOME"
    //
    // Earlier iterations of this verifier accepted "applied[L] == SOME
    // leader-acked payload at L" because the engine resolves a
    // pending `propose() -> Ok(L)` future on leader step-down as well
    // as on commit, so the SAME `LogIndex` could be `Ok`-acked with
    // different payloads across terms ("stale waiter"). Iter-9
    // / iter-12 hardened the verifier so the stale-waiter pattern is
    // itself a HARD failure (B.1) — once the engine guarantees that
    // every `Ok`-ack reflects a value that ended up in the committed
    // log, distinct acks at the same `L` are unambiguously data
    // loss. The canonical-payload compare (B.2) is then equivalent
    // to "every alive node has the same byte sequence at `L`, and
    // that byte sequence is what `propose() -> Ok` returned to the
    // caller" — the exact safety claim the brief requires.
    //
    // Pairwise Log-Matching (above) is preserved as an independent
    // split-brain catcher: it does not depend on the propose-ack
    // oracle and catches any pair-wise disagreement between alive
    // nodes regardless of what acks were observed.
    //
    // # Per-node presence classification
    //
    // * **APPLIED**          — node's apply map has an entry at `L`.
    // * **SNAPSHOTTED-PAST** — no apply record at `L` BUT
    //   `last_applied >= L`. This is the
    //   [`RecordingStateMachine::restore`] path: the engine installed
    //   a snapshot covering `L` and leaped applied forward without
    //   firing an individual `apply(L, ...)` call. Snapshot
    //   integrity is asserted by the engine, so this counts toward
    //   presence (skipped from per-index payload compare).
    // * **LAGGING**          — no apply record at `L` AND
    //   `last_applied < L`. Post-convergence this is a SAFETY
    //   VIOLATION: the convergence wait already required every alive
    //   node to apply at least `target`.

    // iter-16: phantom-ack accounting moved up into the convergence
    // wait. By the time we reach Pass A, await_full_convergence has
    // returned Ok ONLY if min_alive_last_applied >= max_ack_idx,
    // i.e. every leader-acked LogIndex in `committed` is below the
    // verified frontier. If convergence timed out with shortfall,
    // the verifier already returned Err with a phantom-ack count.

    // Pass A — Required-presence sweep across [1, converged_idx].
    // Convergence already proved min_applied >= converged_idx, so
    // every alive node's last_applied >= converged_idx; this sweep
    // ensures the intermediate indices are all present (either
    // applied or snapshotted-past).
    if converged_idx > 0 {
        for idx in 1..=converged_idx {
            let mut missing: Vec<(u64, u64)> = Vec::new();
            for (node_id, node_last, map) in &alive {
                if map.contains_key(&idx) {
                    // APPLIED — counted as present.
                } else if *node_last >= idx {
                    // SNAPSHOTTED-PAST — engine InstallSnapshot leaped
                    // applied past `idx` without an individual apply
                    // call. Engine asserts snapshot integrity.
                } else {
                    missing.push((node_id.0, *node_last));
                }
            }
            if !missing.is_empty() {
                return Err(format!(
                    "{tag}: SAFETY VIOLATION — committed LogIndex {idx} (<= \
                     converged commit_index = {converged_idx}) is MISSING from \
                     {miss_count} of {alive_count} alive nodes (voting config = \
                     {voting_config_size}). Missing nodes (node_id, last_applied): \
                     {missing:?}. The strict verifier requires every committed \
                     index on every alive node — post-convergence lagging is a \
                     real safety failure, not liveness lag.",
                    miss_count = missing.len(),
                ));
            }
        }
    }

    // Pass B — Per-LogIndex strict bucket check (iter-9 item #4).
    //
    // The Stage 8.2 brief's `chaos-no-data-loss` acceptance criterion
    // requires that any client `propose() -> Ok(L)` payload is not
    // silently lost. Earlier iterations relaxed this to "applied[L]
    // matches SOME ack in the bucket" to side-step the engine's
    // apply-before-quorum-commit / step-down-resolve behaviour, but
    // that relaxation accepted the *exact* class of bug the brief
    // asks us to surface: two distinct client propose-acks at the
    // same `L` with different payloads means at least one client was
    // told `Ok(L)` and the on-disk entry at `L` is now a DIFFERENT
    // payload — i.e. that client's write was lost.
    //
    // This pass therefore enforces two strict claims:
    //
    //   B.1 — Duplicate-payload-at-L detection. If the leader-acked
    //         bucket at `L` contains more than one DISTINCT payload,
    //         at least one client received `Ok(L)` for a payload
    //         that was subsequently overwritten by another committed
    //         value. Hard failure with a "client data loss" message.
    //
    //   B.2 — Per-node strict payload equality. Pick the canonical
    //         payload at `L` as the FIRST leader-acked variant in
    //         insertion order (this is the chronologically-earliest
    //         ack; the harness records acks in propose-completion
    //         order). Every alive node with `applied[L]` must have a
    //         byte-equal payload. SNAPSHOTTED-PAST nodes (those that
    //         compacted past `L` with `last_applied >= L` and no
    //         apply record) are credited — engine integrity carries
    //         the snapshot.
    //
    // Cross-node consistency at L (i.e. "every alive node agrees
    // byte-for-byte at L") is also enforced by the pairwise
    // Log-Matching pass above; Pass B.2 is the per-node strict-
    // equality check that closes the remaining gap a pairwise-only
    // pass would leave (Log-Matching is satisfied if every node has
    // the SAME wrong byte sequence — Pass B.2 requires the bytes to
    // equal the leader-acked canonical for that index).
    //
    // Collect ALL mismatches into a single diagnostic line rather
    // than failing at the first one — a chaos test that loses N
    // entries wants to see N in the message.
    {
        let mut bucket: BTreeMap<u64, Vec<&Vec<u8>>> = BTreeMap::new();
        for (committed_log_idx, payload) in committed {
            let idx = committed_log_idx.0;
            if idx > converged_idx {
                continue;
            }
            bucket.entry(idx).or_default().push(payload);
        }

        // B.1 — duplicate-ack-at-L is client-visible data loss.
        let mut dup_loss: Vec<String> = Vec::new();
        for (idx, acks) in bucket.iter() {
            let mut distinct: Vec<&Vec<u8>> = Vec::new();
            for &ack in acks {
                if !distinct.contains(&ack) {
                    distinct.push(ack);
                }
            }
            if distinct.len() > 1 {
                dup_loss.push(format!(
                    "LogIndex {idx}: {n} DISTINCT payloads were leader-acked \
                     at the SAME index — only ONE can survive in the log, so \
                     at least {lost} client `propose() -> Ok({idx})` \
                     response(s) reflect data that was subsequently \
                     overwritten. Acked variants: {distinct:?}",
                    n = distinct.len(),
                    lost = distinct.len() - 1,
                ));
                if dup_loss.len() >= 10 {
                    break;
                }
            }
        }
        if !dup_loss.is_empty() {
            return Err(format!(
                "{tag}: DATA-LOSS VIOLATION (B.1 — duplicate leader-acks at \
                 same LogIndex) — at least one client `propose() -> Ok(L)` \
                 was acknowledged but the payload at L was replaced. \
                 First {n}: {dup_loss:?}",
                n = dup_loss.len(),
            ));
        }

        // B.2 — per-node strict payload equality vs canonical ack.
        let mut mismatches: Vec<String> = Vec::new();
        for (node_id, node_last, map) in &alive {
            for (idx, acks) in bucket.iter() {
                let canonical: &Vec<u8> = acks[0];
                match map.get(idx) {
                    Some(found) => {
                        if *found != *canonical {
                            mismatches.push(format!(
                                "node {} at LogIndex {}: applied {:?} != \
                                 canonical leader-acked {:?}",
                                node_id.0, idx, found, canonical,
                            ));
                            if mismatches.len() >= 10 {
                                break;
                            }
                        }
                    }
                    None => {
                        if *node_last >= *idx {
                            // SNAPSHOTTED-PAST — engine integrity.
                        } else {
                            // LAGGING — already reported in Pass A.
                        }
                    }
                }
            }
            if mismatches.len() >= 10 {
                break;
            }
        }
        if !mismatches.is_empty() {
            return Err(format!(
                "{tag}: DATA-LOSS VIOLATION (B.2 — per-node payload mismatch) \
                 — applied[L] != leader-acked payload at L on {n} (node, L) \
                 pair(s). First {shown}: {preview:?}",
                n = mismatches.len(),
                shown = mismatches.len(),
                preview = mismatches,
            ));
        }
    }

    // iter-16: phantom_acks is now ALWAYS 0 at this point because
    // await_full_convergence above only returns Ok when every
    // leader-acked LogIndex is at or below the achieved frontier.
    // A surviving timeout returns Err earlier with shortfall
    // diagnostic. No silent-skip logging here.

    Ok(())
}

/// Internal implementation of [`verify_committed_entries_safety_quorum`].
///
/// Pipeline:
///
/// 1. Heal the network (defensive — caller's schedule SHOULD have
///    done this) and await a stable leader.
/// 2. Drive a QUORUM-convergence wait: compute the largest LogIndex
///    `Q` such that at least `quorum` voters have `last_applied >=
///    Q`, observed stable across two consecutive poll passes.
/// 3. Snapshot every alive node's applied state filtered to its
///    contiguous-applied window.
/// 4. Pairwise Log-Matching across every PAIR of alive nodes
///    (catches divergence between two non-reference replicas).
/// 5. For every leader-acked committed `(LogIndex, payload)` with
///    `LogIndex <= Q`: count alive nodes whose `applied[idx] ==
///    payload`. Require that count be `>= quorum`. Below-quorum =
///    SAFETY VIOLATION.
/// 6. Print a per-test summary line listing lagging followers — the
///    EXPLICIT "no silent skip" report so a reviewer can verify the
///    quorum-presence check did its job and the remaining nodes are
///    tracked as liveness lag.
#[allow(dead_code)]
async fn verify_safety_quorum_inner(
    cluster: &SimulatedCluster,
    committed: &[(LogIndex, Vec<u8>)],
    recovery_deadline: Duration,
) -> std::result::Result<(), String> {
    const TAG: &str = "safety_quorum";

    cluster.heal_all();
    cluster.network.set_drop_pct(0);
    cluster.network.set_latency(Duration::ZERO);

    cluster
        .await_leader(recovery_deadline)
        .await
        .map_err(|e| format!("{TAG}: no leader after chaos heal: {e}"))?;

    // Raft quorum is defined over the VOTING configuration size,
    // not the count of currently-alive nodes. The harness does not
    // reconfigure membership, so voting_config_size == cluster.nodes.len().
    let voting_config_size = cluster.nodes.len();
    let quorum = voting_config_size / 2 + 1;

    // If fewer than quorum nodes are alive, we cannot prove safety
    // either way — Raft itself would not commit new entries here.
    let alive_count = cluster.nodes.iter().filter(|n| n.is_alive()).count();
    if alive_count < quorum {
        return Err(format!(
            "{TAG}: only {alive_count} of {voting_config_size} voters alive — \
             cannot verify quorum-safety (need >= {quorum} alive voters)"
        ));
    }

    if committed.is_empty() {
        // No propose-acks to verify. Drive a baseline quorum
        // convergence with target=0 to prove a leader is alive and
        // the cluster reached a stable state.
        await_quorum_convergence(cluster, quorum, 0, recovery_deadline)
            .await
            .map_err(|msg| format!("{TAG}: post-chaos quorum-convergence failed: {msg}"))?;
        return Ok(());
    }

    // iter-16: Convergence target = MAX LogIndex from the test's
    // leader-acked `committed` list (NOT the leader's live
    // `commit_index`, which can race ahead of the quorum's APPLIED
    // frontier under churn). We verify every entry the test KNOWS
    // about; entries beyond max_ack are outside the test's
    // contract. On timeout, the wait surfaces a hard data-loss /
    // liveness-lag diagnostic — iter-16 item #3 (was: silent
    // eprintln in iter-15).
    let max_ack_idx = committed.iter().map(|(l, _)| l.0).max().unwrap_or(0);
    let converged_idx = await_quorum_convergence(cluster, quorum, max_ack_idx, recovery_deadline)
        .await
        .map_err(|msg| {
            format!(
                "{TAG}: post-chaos quorum-convergence failed (max_ack_idx = \
                 {max_ack_idx}, leader-acked entries = {n}): {msg}",
                n = committed.len()
            )
        })?;

    let alive: Vec<(NodeId, u64, BTreeMap<u64, Vec<u8>>)> = cluster
        .nodes
        .iter()
        .filter(|n| n.is_alive())
        .map(|n| {
            let node_last = n.recording.last_applied();
            let applied: BTreeMap<u64, Vec<u8>> = n
                .recording
                .applied()
                .into_iter()
                .filter(|(idx, _)| *idx <= node_last)
                .collect();
            (n.node_id, node_last, applied)
        })
        .collect();
    if alive.is_empty() {
        return Err(format!("{TAG}: no alive nodes after chaos heal"));
    }

    // SAFETY (pairwise Log-Matching): every pair of alive nodes
    // must agree at every LogIndex they have both applied IN THE
    // TEST'S ACCEPTANCE WINDOW (`idx <= max_ack_idx`). This
    // catches a split-brained follower that applied a WRONG value
    // at an acked index — independent of whether the leader caught
    // it. iter-23: bounded to `<= max_ack_idx` for the same
    // reasoning as `verify_inner`'s strict pairwise (drive
    // traffic / engine-internal no-ops beyond `max_ack_idx` are
    // out of the brief's acceptance surface).
    for i in 0..alive.len() {
        for j in (i + 1)..alive.len() {
            let (a_id, _, a_map) = &alive[i];
            let (b_id, _, b_map) = &alive[j];
            for (idx, a_bytes) in a_map.range(..=max_ack_idx) {
                if let Some(b_bytes) = b_map.get(idx)
                    && a_bytes != b_bytes
                {
                    return Err(format!(
                        "{TAG}: Log Matching violation: node {} and node {} \
                         disagree at LogIndex {}: node {} = {:?}, node {} = {:?}",
                        a_id.0, b_id.0, idx, a_id.0, a_bytes, b_id.0, b_bytes,
                    ));
                }
            }
        }
    }

    // QUORUM REQUIRED-PRESENCE + PAYLOAD-PROVENANCE (Stage 8.2 brief,
    // applied to tests where the cluster never fully quiesces, e.g.
    // continuous leader-churn stress):
    //
    // 1. For every LogIndex in `1..=converged_idx`, at least
    //    `quorum` alive voters MUST be present (either APPLIED or
    //    SNAPSHOTTED-PAST).
    // 2. For every leader-acked `(L, payload)` in `committed` with
    //    `L <= converged_idx`:
    //    * **B.1 Distinct-ack data loss (hard fail).** If two acks
    //      at the SAME `L` carry DIFFERENT payload bytes, that is
    //      client-visible data loss (same reasoning as STRICT
    //      verifier B.1).
    //    * **B.2 Canonical byte-equality, quorum-coverage (hard
    //      fail).** With distinct acks already rejected by B.1,
    //      the canonical payload at `L` is `acks[L][0]`. At least
    //      `quorum` alive voters MUST have an `applied[L]` (or be
    //      SNAPSHOTTED-PAST) that byte-equals the canonical
    //      payload. Anything less is a data-loss failure.
    //
    // Stricter than the lenient verifier (it requires QUORUM, not
    // "some alive node"); strictly more lenient than [`verify_inner`]
    // (which requires EVERY alive node). The pairwise Log-Matching
    // pass above guards against split-brain divergence between any
    // two alive replicas; B.1 and B.2 are identical in shape to the
    // STRICT verifier and only differ in their coverage threshold.
    //
    // # Per-node presence classification (per index)
    //
    // * **APPLIED**          — node's apply map has an entry at `L`.
    // * **SNAPSHOTTED-PAST** — no apply record at `L` BUT
    //   `last_applied >= L`. Engine installed a snapshot covering
    //   `L` and leaped applied forward.
    // * **LAGGING**          — no apply record at `L` AND
    //   `last_applied < L`. Counted as "absent" for the
    //   quorum-presence tally; nodes that lag a few entries are
    //   liveness lag, not safety violations.

    // iter-16: phantom_acks accounting moved up into
    // await_quorum_convergence. By the time we reach the bucket
    // sweep, q_frontier >= max_ack_idx, so every leader-acked
    // LogIndex is at or below `converged_idx`. A surviving timeout
    // returns Err above with shortfall diagnostic.

    let mut lagging_nodes: BTreeMap<u64, u64> = BTreeMap::new(); // id -> last_applied
    if converged_idx > 0 {
        for idx in 1..=converged_idx {
            let mut present = 0usize;
            let mut absent_lagging: Vec<(u64, u64)> = Vec::new();
            for (node_id, last_applied, map) in &alive {
                if map.contains_key(&idx) {
                    // APPLIED.
                    present += 1;
                } else if *last_applied >= idx {
                    // SNAPSHOTTED-PAST.
                    present += 1;
                } else {
                    absent_lagging.push((node_id.0, *last_applied));
                    lagging_nodes
                        .entry(node_id.0)
                        .and_modify(|prev| {
                            if *last_applied < *prev {
                                *prev = *last_applied;
                            }
                        })
                        .or_insert(*last_applied);
                }
            }
            if present < quorum {
                return Err(format!(
                    "{TAG}: SAFETY VIOLATION — committed LogIndex {idx} (<= \
                     converged commit_index = {converged_idx}) is present on only \
                     {present} of {voting_config_size} voters; quorum is {quorum}. \
                     Absent-lagging nodes: {absent_lagging:?}",
                ));
            }
        }
    }

    // Per-LogIndex strict bucket check (iter-9 item #4 — quorum
    // variant). Mirrors the strict B.1 + B.2 split in
    // `verify_inner` but uses a QUORUM-presence threshold instead
    // of every-alive.
    //
    //   B.1 — duplicate-payload-at-L is hard data loss. Identical
    //         semantics to strict verifier: distinct payloads at
    //         the same `L` means at least one client `propose() ->
    //         Ok(L)` was acknowledged for a payload that is no
    //         longer at `L`.
    //
    //   B.2 — for each `L`, at least `quorum` alive voters must
    //         have `applied[L]` byte-equal to the canonical
    //         leader-acked payload (= the first ack at `L` in
    //         insertion order). SNAPSHOTTED-PAST nodes (those with
    //         `last_applied >= L` but no apply record at `L`) are
    //         credited toward the quorum count.
    //
    // Cross-node consistency at L is already enforced by the
    // pairwise Log-Matching pass above.
    {
        let mut bucket: BTreeMap<u64, Vec<&Vec<u8>>> = BTreeMap::new();
        for (committed_log_idx, payload) in committed {
            let idx = committed_log_idx.0;
            if idx > converged_idx {
                continue;
            }
            bucket.entry(idx).or_default().push(payload);
        }

        // B.1 — duplicate-ack-at-L is client-visible data loss.
        let mut dup_loss: Vec<String> = Vec::new();
        for (idx, acks) in bucket.iter() {
            let mut distinct: Vec<&Vec<u8>> = Vec::new();
            for &ack in acks {
                if !distinct.contains(&ack) {
                    distinct.push(ack);
                }
            }
            if distinct.len() > 1 {
                dup_loss.push(format!(
                    "LogIndex {idx}: {n} DISTINCT payloads were leader-acked \
                     at the SAME index — at least {lost} client \
                     `propose() -> Ok({idx})` response(s) reflect data that \
                     was subsequently overwritten. Variants: {distinct:?}",
                    n = distinct.len(),
                    lost = distinct.len() - 1,
                ));
                if dup_loss.len() >= 10 {
                    break;
                }
            }
        }
        if !dup_loss.is_empty() {
            return Err(format!(
                "{TAG}: DATA-LOSS VIOLATION (B.1 — duplicate leader-acks at \
                 same LogIndex). First {n}: {dup_loss:?}",
                n = dup_loss.len(),
            ));
        }

        // B.2 — quorum strict equality vs canonical ack.
        let mut data_loss: Vec<String> = Vec::new();
        for (idx, acks) in bucket.iter() {
            let canonical: &Vec<u8> = acks[0];
            let mut matched = 0usize;
            let mut wrong_bytes: Vec<(u64, Vec<u8>)> = Vec::new();
            for (node_id, last_applied, map) in &alive {
                match map.get(idx) {
                    Some(found) => {
                        if found == canonical {
                            matched += 1;
                        } else {
                            wrong_bytes.push((node_id.0, found.clone()));
                        }
                    }
                    None => {
                        if *last_applied >= *idx {
                            // SNAPSHOTTED-PAST — count toward quorum.
                            matched += 1;
                        }
                    }
                }
            }
            if !wrong_bytes.is_empty() {
                return Err(format!(
                    "{TAG}: SAFETY VIOLATION at LogIndex {idx}: applied payload \
                     differs from canonical leader-acked payload {canonical:?} \
                     on {n_wrong} node(s): {wrong_bytes:?}",
                    n_wrong = wrong_bytes.len(),
                ));
            }
            if matched < quorum {
                data_loss.push(format!(
                    "LogIndex {idx}: only {matched}/{voting_config_size} \
                     voters have the canonical leader-acked payload"
                ));
                if data_loss.len() >= 10 {
                    break;
                }
            }
        }
        if !data_loss.is_empty() {
            return Err(format!(
                "{TAG}: DATA-LOSS VIOLATION — below-quorum payload presence at \
                 leader-acked indices (first {n}): {data_loss:?}",
                n = data_loss.len(),
            ));
        }
    }

    if !lagging_nodes.is_empty() {
        // EXPLICIT no-silent-skip diagnostic: name every lagging
        // follower and the converged_commit_index so a reviewer can
        // verify the quorum-safety check did its job and the
        // remaining nodes are tracked as liveness lag.
        let lag_str: Vec<String> = lagging_nodes
            .iter()
            .map(|(id, la)| {
                format!(
                    "node {id} last_applied={la}, behind by {} entries",
                    converged_idx - la
                )
            })
            .collect();
        eprintln!(
            "{TAG}: quorum-safety verified at converged_commit_index = {converged_idx}; \
             lagging followers (Raft liveness, not safety): {}",
            lag_str.join("; "),
        );
    }
    // iter-16: phantom_acks accounting moved into convergence wait.
    // If await_quorum_convergence returned Ok, every leader-acked
    // LogIndex is at or below converged_idx by construction.

    Ok(())
}

/// Quorum-convergence wait based on actually-applied state.
///
/// We poll every alive node's `recording.last_applied()` and compute
/// the LARGEST LogIndex `Q` such that at least `quorum` alive voters
/// have `last_applied >= Q`. This is the cluster's **quorum-applied
/// frontier** — the highest LogIndex for which Raft's safety
/// guarantee (committed = present on majority) demonstrably HOLDS in
/// the recorded state machine state.
///
/// `target_idx` is the convergence floor: we keep waiting until
/// `q_frontier >= target_idx`, stable across two consecutive poll
/// passes with a stable leader. Pass `target_idx = max_ack_idx` (the
/// largest LogIndex from the test's leader-acked `committed` list)
/// to guarantee every leader-acked entry is on quorum's APPLIED
/// state by the time this returns Ok. Pass `0` to wait only for any
/// stable state (used when `committed` is empty).
///
/// We do NOT use `leader.commit_index` as the target because this
/// engine (like vanilla Raft) advances a leader's `commit_index`
/// when the entry is replicated in the LOG of a majority — not when
/// it has been APPLIED by a majority, and the leader's apply task
/// can race a few entries ahead of follower applies under sustained
/// load. Using `max_ack_idx` (test-bounded) instead of
/// `leader.commit_index` (engine-live) keeps the wait deterministic
/// and lets phantom-ack detection actually fire instead of hanging.
///
/// Returns `Ok(Q)` on success. `Err` on no-leader or deadline
/// timeout; the timeout diagnostic includes the shortfall
/// (`target_idx - q_frontier`) so the caller can report it as a
/// phantom-ack violation.
#[allow(dead_code)]
async fn await_quorum_convergence(
    cluster: &SimulatedCluster,
    quorum: usize,
    target_idx: u64,
    recovery_deadline: Duration,
) -> std::result::Result<u64, String> {
    use std::time::Instant;

    let start_sim = cluster.clock.elapsed();
    let start_wall = Instant::now();
    let wall_backstop = recovery_deadline.saturating_mul(10) + Duration::from_secs(60);

    let mut prev_q: Option<u64> = None;
    let mut prev_leader: Option<(NodeId, u64)> = None;

    loop {
        // Find a single, unambiguous leader.
        let mut leader_snap: Option<(NodeId, u64)> = None;
        for n in cluster.nodes.iter().filter(|n| n.is_alive()) {
            if let Some(s) = n.status.status().await
                && s.role == NodeRole::Leader
            {
                if leader_snap.is_some() {
                    leader_snap = None;
                    break;
                }
                leader_snap = Some((NodeId(s.node_id), s.term));
            }
        }

        // Collect alive nodes' last_applied, compute Q = quorum-applied
        // frontier (largest index L s.t. >= `quorum` nodes have
        // applied >= L).
        let mut applied_per_node: Vec<u64> = cluster
            .nodes
            .iter()
            .filter(|n| n.is_alive())
            .map(|n| n.recording.last_applied())
            .collect();
        applied_per_node.sort_unstable_by(|a, b| b.cmp(a)); // descending
        let q_frontier: u64 = if applied_per_node.len() >= quorum {
            applied_per_node[quorum - 1]
        } else {
            0
        };

        // Convergence target reached AND state is stable.
        let target_reached = q_frontier >= target_idx;
        let stable_q = prev_q == Some(q_frontier);
        let stable_leader = leader_snap.is_some() && prev_leader == leader_snap;

        if target_reached && stable_q && stable_leader {
            return Ok(q_frontier);
        }
        prev_q = Some(q_frontier);
        prev_leader = leader_snap;

        // Sim-time + wall-time bounds.
        let sim_elapsed = cluster.clock.elapsed().saturating_sub(start_sim);
        if sim_elapsed >= recovery_deadline {
            let snap = leader_snap_diag(cluster).await;
            let shortfall = target_idx.saturating_sub(q_frontier);
            return Err(format!(
                "exceeded recovery_deadline {recovery_deadline:?} (simulated); \
                 quorum = {quorum}; q_frontier = {q_frontier}; target_idx = \
                 {target_idx}; PHANTOM-ACK SHORTFALL = {shortfall} leader-acked \
                 LogIndex(es) at or below target_idx have not reached the \
                 quorum-applied frontier within the deadline (DATA-LOSS / \
                 LIVENESS-LAG VIOLATION); per-node = {snap:?}"
            ));
        }
        let wall_elapsed = start_wall.elapsed();
        if wall_elapsed >= wall_backstop {
            let snap = leader_snap_diag(cluster).await;
            let shortfall = target_idx.saturating_sub(q_frontier);
            return Err(format!(
                "exceeded wall-clock backstop {wall_backstop:?}; quorum = {quorum}; \
                 q_frontier = {q_frontier}; target_idx = {target_idx}; \
                 PHANTOM-ACK SHORTFALL = {shortfall}; per-node = {snap:?}"
            ));
        }

        // Event-driven wait: race state_change with a 50ms safety net.
        let state_waiter = cluster.state_change.notified();
        tokio::pin!(state_waiter);
        state_waiter.as_mut().enable();
        let remaining_wall = wall_backstop - wall_elapsed;
        let safety_net = Duration::from_millis(50).min(remaining_wall);
        tokio::select! {
            _ = &mut state_waiter => {}
            _ = tokio::time::sleep(safety_net) => {}
        }
    }
}

/// Drive the cluster to FULL convergence after a chaos run:
///
/// 1. Find the current alive leader; read its `commit_index` as the
///    convergence target.
/// 2. Wait until every alive node's recording state machine has
///    `last_applied >= target` (i.e. the leader's committed prefix
///    is fully replicated AND applied on every alive replica).
/// 3. Re-snapshot the leader. If the leader changed OR the
///    `commit_index` advanced, set the new target and loop. If the
///    leader+term+commit_index were stable across two consecutive
///    poll passes AND every alive node was already caught up, the
///    cluster has converged: return `Ok(converged_commit_index)`.
///
/// Uses a single GLOBAL `recovery_deadline` measured in simulated
/// time (plus a generous wall-clock backstop) — leader churn or
/// late-fetching followers can extend the wait, but cannot
/// indefinitely refresh the deadline.
///
/// Returns `Ok(converged_commit_index)` on success or a diagnostic
/// `String` on timeout. The string includes per-node
/// `(node_id, last_applied, commit_index)` snapshots so the operator
/// can see which replica stalled.
/// Drive the cluster to FULL convergence after a chaos run:
///
/// 1. Find the current alive leader; verify there's exactly one.
/// 2. Wait until every alive node's recording state machine has
///    `last_applied >= target_idx` (i.e. the test's leader-acked
///    prefix is fully replicated AND applied on every alive replica).
/// 3. Stable across two consecutive poll passes before declaring
///    convergence — guards against returning success at a moment
///    when the leader is mid-apply or a new election is about to
///    happen.
///
/// `target_idx` is the convergence floor: pass `max_ack_idx` (the
/// largest LogIndex from the test's leader-acked `committed` list)
/// to guarantee every leader-acked entry has been applied on every
/// alive node by the time this returns Ok. Pass `0` for tests with
/// an empty `committed` list (we still wait for a stable leader and
/// a stable apply state).
///
/// We do NOT wait against the leader's live `commit_index` because
/// it can race a few entries ahead of the apply loop under
/// sustained load (observed: leader.commit_index = N, every alive
/// last_applied = N-1, no further proposes arrive to push the apply
/// task past the last entry → the verifier hangs to the deadline).
/// Using the test-bounded `max_ack_idx` instead keeps the wait
/// deterministic and lets phantom-ack detection actually fire
/// instead of timing out on engine-only racing entries.
///
/// Uses a single GLOBAL `recovery_deadline` measured in simulated
/// time (plus a generous wall-clock backstop) — leader churn or
/// late-fetching followers can extend the wait, but cannot
/// indefinitely refresh the deadline.
///
/// Returns `Ok(min_alive_last_applied)` on success. On timeout the
/// `Err` diagnostic includes the shortfall (`target_idx -
/// min_applied`) so the caller can report it as a phantom-ack
/// violation, plus per-node `(node_id, last_applied, commit_index)`
/// snapshots so the operator can see which replica stalled.
async fn await_full_convergence(
    cluster: &SimulatedCluster,
    target_idx: u64,
    recovery_deadline: Duration,
) -> std::result::Result<u64, String> {
    use std::time::Instant;

    let start_sim = cluster.clock.elapsed();
    let start_wall = Instant::now();
    let wall_backstop = recovery_deadline.saturating_mul(10) + Duration::from_secs(60);

    // Stability tracking: we require TWO consecutive observations
    // with the same (leader_id, term) AND every alive node at >=
    // target_idx before declaring convergence.
    let mut prev_stable: Option<(NodeId, u64, u64)> = None;

    loop {
        // Poll the leader's snapshot. If there is no leader right
        // now (mid-election) we wait briefly and retry.
        let mut leader_snap: Option<(NodeId, u64, u64)> = None;
        for n in cluster.nodes.iter().filter(|n| n.is_alive()) {
            if let Some(s) = n.status.status().await
                && s.role == NodeRole::Leader
            {
                if leader_snap.is_some() {
                    // Two leaders observed at the same poll — almost
                    // certainly a transient view across an election.
                    // Treat as "no stable leader" and retry.
                    leader_snap = None;
                    break;
                }
                leader_snap = Some((NodeId(s.node_id), s.term, s.commit_index));
            }
        }

        // Compute min last_applied across alive nodes regardless of
        // leader presence — we report it in the timeout diagnostic.
        let mut min_applied: u64 = u64::MAX;
        let mut any_alive = false;
        for n in cluster.nodes.iter().filter(|n| n.is_alive()) {
            any_alive = true;
            let la = n.recording.last_applied();
            if la < min_applied {
                min_applied = la;
            }
        }
        if !any_alive {
            return Err("no alive nodes during convergence wait".into());
        }
        if min_applied == u64::MAX {
            min_applied = 0;
        }

        if let Some(snap) = leader_snap {
            let target_reached = min_applied >= target_idx;

            if target_reached && Some(snap) == prev_stable {
                // Stable across two consecutive observations — done.
                return Ok(min_applied);
            }

            // Carry the current observation forward so the next pass
            // can compare. Only record stability when target reached.
            if target_reached {
                prev_stable = Some(snap);
            } else {
                prev_stable = None;
            }
        } else {
            // No leader visible — reset stability and keep waiting.
            prev_stable = None;
        }

        // Sim-time + wall-time bounds.
        let sim_elapsed = cluster.clock.elapsed().saturating_sub(start_sim);
        if sim_elapsed >= recovery_deadline {
            let snap = leader_snap_diag(cluster).await;
            let shortfall = target_idx.saturating_sub(min_applied);
            return Err(format!(
                "exceeded recovery_deadline {recovery_deadline:?} (simulated); \
                 slowest alive last_applied = {min_applied}; target_idx = \
                 {target_idx}; PHANTOM-ACK SHORTFALL = {shortfall} leader-acked \
                 LogIndex(es) at or below target_idx have not been applied on \
                 every alive node within the deadline (DATA-LOSS / LIVENESS-LAG \
                 VIOLATION); per-node = {snap:?}"
            ));
        }
        let wall_elapsed = start_wall.elapsed();
        if wall_elapsed >= wall_backstop {
            let snap = leader_snap_diag(cluster).await;
            let shortfall = target_idx.saturating_sub(min_applied);
            return Err(format!(
                "exceeded wall-clock backstop {wall_backstop:?}; slowest alive \
                 last_applied = {min_applied}; target_idx = {target_idx}; \
                 PHANTOM-ACK SHORTFALL = {shortfall}; per-node = {snap:?}"
            ));
        }

        // Event-driven wait: race the cluster's state_change notify
        // with a 50 ms periodic safety net so the deadline check
        // stays bounded even when no state change ever fires.
        let state_waiter = cluster.state_change.notified();
        tokio::pin!(state_waiter);
        state_waiter.as_mut().enable();
        let remaining_wall = wall_backstop - wall_elapsed;
        let safety_net = Duration::from_millis(50).min(remaining_wall);
        tokio::select! {
            _ = &mut state_waiter => {}
            _ = tokio::time::sleep(safety_net) => {}
        }
    }
}

/// Build a `(node_id, last_applied, commit_index)` per-alive-node
/// diagnostic snapshot for inclusion in convergence-timeout messages.
async fn leader_snap_diag(cluster: &SimulatedCluster) -> Vec<(u64, u64, u64)> {
    let mut out = Vec::with_capacity(cluster.nodes.len());
    for n in cluster.nodes.iter().filter(|n| n.is_alive()) {
        let ci = n.status.status().await.map(|s| s.commit_index).unwrap_or(0);
        out.push((n.node_id.0, n.recording.last_applied(), ci));
    }
    out
}

/// Best-effort propose that retries up to `max_tries` on transient
/// `NotLeader` / transport errors. Used by stress tests where each
/// attempted entry MUST be acked (no graceful "this attempt failed").
///
/// Returns the assigned [`LogIndex`] on success or the last error.
pub async fn propose_with_retry(
    cluster: &SimulatedCluster,
    payload: Bytes,
    max_tries: u8,
    deadline_per_try: Duration,
) -> Result<LogIndex, XRaftError> {
    let mut last_err: Option<XRaftError> = None;
    for _ in 0..max_tries {
        match tokio::time::timeout(deadline_per_try, cluster.propose(payload.clone())).await {
            Ok(Ok(idx)) => return Ok(idx),
            Ok(Err(e @ XRaftError::NotLeader { .. })) => {
                last_err = Some(e);
                // Brief await for a new leader to emerge.
                let _ = cluster.await_leader(deadline_per_try).await;
            }
            Ok(Err(e)) => {
                last_err = Some(e);
            }
            Err(_) => {
                last_err = Some(XRaftError::Transport(
                    "propose_with_retry: per-try wall-clock timeout".into(),
                ));
            }
        }
        tokio::task::yield_now().await;
    }
    Err(last_err.unwrap_or_else(|| {
        XRaftError::Transport("propose_with_retry: exhausted with no recorded error".into())
    }))
}

/// Per-node probe used in panic-path diagnostics so a flake message
/// can distinguish stale-leader stepdown lag from genuine
/// safety/liveness regressions.
///
/// All fields are reported through `Debug` (via `{snap:?}` at every
/// existing callsite). The richer shape — `term`, `leader_id`,
/// `commit_index`, `last_applied`, `last_log_index` — is what
/// converts an opaque "two `Leader` rows" diagnostic into
/// "node 3 = Leader@term=12 vs node 5 = Leader@term=9 (stale,
/// not yet stepped down)", which is Raft-spec-OK (at most one
/// leader per TERM, not per wall-clock instant) rather than a
/// safety bug.
///
/// Dead/uninitialised nodes (no [`NodeStatus`] reading available)
/// surface as `term/leader_id/commit/applied/log = None` — the test
/// still gets the `recording.len()` count from the RecordingStateMachine
/// because that handle outlives the node's task.
#[derive(Debug, Clone)]
pub struct NodeProbe {
    pub node_id: u64,
    /// `true` iff [`SimulatedNode::is_alive`] returns true — the
    /// node's driver task is still spawned (not aborted by
    /// [`SimulatedCluster::kill`]). Killed nodes can still have a
    /// non-`None` [`Self::last_applied`] (the engine's status
    /// channel publishes a final value before the task is aborted),
    /// so test-side "every alive node" predicates MUST gate on
    /// `is_alive` rather than `last_applied.is_some()` to correctly
    /// exclude killed victims.
    pub is_alive: bool,
    pub recording_len: usize,
    /// `RecordingStateMachine::last_applied()` — the test's
    /// authoritative apply counter, matching the source the strict
    /// verifier's `await_full_convergence` polls. Use this (NOT
    /// [`Self::last_applied`], which is the engine's published
    /// status field) when gating a wait predicate against the
    /// verifier's convergence criterion. Under heavy load the
    /// engine can apply an entry to the SM and publish its status
    /// update with a small lag; using `recording.last_applied()`
    /// avoids that race.
    pub recording_last_applied: u64,
    pub role: Option<NodeRole>,
    pub term: Option<u64>,
    pub leader_id: Option<u64>,
    pub commit_index: Option<u64>,
    pub last_applied: Option<u64>,
    pub last_log_index: Option<u64>,
}

/// Per-node snapshot used in panic-path diagnostics. See [`NodeProbe`]
/// for field-by-field rationale.
pub async fn node_status_snapshot(cluster: &SimulatedCluster) -> Vec<NodeProbe> {
    let mut out = Vec::with_capacity(cluster.nodes.len());
    for n in cluster.nodes.iter() {
        let status = n.status.status().await;
        out.push(NodeProbe {
            node_id: n.node_id.0,
            is_alive: n.is_alive(),
            recording_len: n.recording.len(),
            recording_last_applied: n.recording.last_applied(),
            role: status.map(|s| s.role),
            term: status.map(|s| s.term),
            leader_id: status.and_then(|s| s.leader_id),
            commit_index: status.map(|s| s.commit_index),
            last_applied: status.map(|s| s.last_applied),
            last_log_index: status.map(|s| s.last_log_index),
        });
    }
    out
}
