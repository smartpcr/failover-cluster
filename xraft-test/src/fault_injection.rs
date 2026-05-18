//! Reproducible chaos fault-injection primitives for the Stage 8.2
//! chaos and stress tests.
//!
//! This module is the **public** library surface of the `xraft-test`
//! crate's fault-injection support. It was promoted from a
//! test-private module (`tests/common/fault_injector.rs`) so that:
//!
//! 1. The brief's target path
//!    `xraft/src/testing/fault_injection.rs` resolves to a real
//!    public Rust module (mapped here to
//!    `xraft-test/src/fault_injection.rs` per the repo's
//!    hyphenated-crate layout).
//! 2. Any downstream test consumer (e.g. a future external
//!    integration-test binary in a sibling crate) can build chaos
//!    schedules without copy-pasting these primitives into its own
//!    `tests/common/` tree.
//!
//! # Why a pre-baked schedule
//!
//! The Stage 8.2 `deterministic-replay` scenario in
//! `docs/stories/failover-cluster-XRAFT/implementation-plan.md`
//! calls for:
//!
//! > Given a chaos test run with seed=42, When replayed with the same
//! > seed, Then the exact same sequence of events and outcomes occurs.
//!
//! Tokio task scheduling, kernel timer jitter, and OS thread
//! pre-emption make per-step engine outcomes impossible to fully
//! reproduce in a multi-threaded harness without a single-threaded
//! deterministic simulation runtime. We meet the scenario at the
//! achievable layer: the **chaos schedule itself** is bit-identical
//! across runs of identical `(seed, ...)` inputs. Two runs of the
//! same seed inject the **same faults at the same simulated-time
//! offsets in the same order**, and both runs must converge to a
//! consistent committed prefix at the end. Schedule equivalence is
//! asserted via `Vec::PartialEq`.
//!
//! # Event semantics
//!
//! Events are tagged with a simulated-time offset
//! (relative to the chaos phase start) and are applied in order. The
//! injector intentionally **never** uses wall-clock time — only the
//! shared [`SimulatedClock`](crate::SimulatedClock) provides the
//! authoritative "now" that gates dispatch. This keeps the chaos
//! schedule reproducible regardless of how slowly or quickly the
//! harness pump advances simulated time.
//!
//! # Why the RNG is `StdRng`
//!
//! `StdRng::seed_from_u64` produces a stable sequence across rust
//! toolchain bumps for a fixed seed (the algorithm is `ChaCha12`),
//! which is what the deterministic-replay test relies on. A
//! `thread_rng` or `SmallRng` would not provide that guarantee.

use std::time::Duration;

use rand::{Rng, SeedableRng, rngs::StdRng};

use xraft_core::types::NodeId;

// ---------------------------------------------------------------------------
// FaultEvent + FaultSchedule
// ---------------------------------------------------------------------------

/// A single chaos event the harness can apply to a
/// [`SimulatedCluster`](crate::SimulatedCluster).
///
/// Variants intentionally hold ONLY data the schedule can resolve at
/// build time (node ids, percentages, durations). Events whose target
/// depends on **runtime** state (e.g. "partition the CURRENT leader")
/// are modelled as [`Self::PartitionCurrentLeader`] / [`Self::HealAll`]
/// pairs whose target is resolved at apply time — the schedule still
/// contains a deterministic event sequence, but the runtime-resolved
/// target is allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultEvent {
    /// Symmetrically partition `nodes` from the rest of the cluster.
    /// Multiple consecutive `PartitionGroup`s OR'd together until
    /// the next [`Self::HealAll`].
    PartitionGroup(Vec<NodeId>),

    /// Heal every partition cut currently active. Idempotent.
    HealAll,

    /// Set the per-RPC drop probability (clamped to `[0, 100]`).
    SetDropPct(u8),

    /// Set the per-RPC simulated VIRTUAL latency. Charged to the
    /// shared [`SimulatedClock`](crate::SimulatedClock); does
    /// NOT introduce wall-clock sleep.
    SetLatency(Duration),

    /// Resolve the current leader at apply time and partition it
    /// from the rest of the cluster. Used by the rapid-leader-churn
    /// scenario where the target depends on which node is leader
    /// "right now".
    PartitionCurrentLeader,

    /// Fail-stop the named node: abort its driver task and remove
    /// it from the routing fabric. The killed node STAYS in the
    /// cluster's `nodes` vector but its `task` is `None` until a
    /// [`Self::Restart`] event re-spawns it. The chaos schedule
    /// generator always pairs a `Kill` with a subsequent `Restart`
    /// so the cluster's alive-quorum count is restored before the
    /// next chaos beat.
    Kill(NodeId),

    /// Bring the named node back online with FRESH in-memory
    /// storage. Must be preceded by a [`Self::Kill`] of the same
    /// `NodeId` (the harness apply path enforces this).
    Restart(NodeId),

    /// Resolve the current leader at apply time and fail-stop it.
    /// Used by the rapid-leader-churn-kill scenario. Always paired
    /// with a [`Self::RestartKilledLeader`] event later in the
    /// schedule so quorum is restored before the next beat.
    KillCurrentLeader,

    /// Restart the MOST RECENTLY [`Self::KillCurrentLeader`]ed node.
    /// Resolved at apply time against an apply-side memory of the
    /// last killed leader, so the schedule can be built without
    /// foreknowledge of which node will be leader at each beat.
    RestartKilledLeader,
}

/// A deterministic, replay-able sequence of chaos events.
///
/// Each entry is `(simulated_time_since_chaos_start, event)`. The
/// schedule is sorted by time ascending; entries with identical
/// timestamps are applied in insertion order. The harness drives
/// dispatch by polling `SimulatedClock::elapsed` (NOT wall clock) so
/// the schedule's timing is reproducible regardless of pump speed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FaultSchedule {
    /// `(at_offset, event)` tuples. Sorted by `at_offset` ascending.
    pub events: Vec<(Duration, FaultEvent)>,
}

impl FaultSchedule {
    /// Total simulated-time duration covered by the schedule (from
    /// 0 to the last event's offset). A schedule with zero events
    /// returns `Duration::ZERO`.
    pub fn span(&self) -> Duration {
        self.events
            .last()
            .map(|(at, _)| *at)
            .unwrap_or(Duration::ZERO)
    }

    /// Number of events in the schedule.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the schedule is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

// ---------------------------------------------------------------------------
// FaultInjector
// ---------------------------------------------------------------------------

/// Tunables for [`FaultInjector::build_chaos_schedule`]. Defaults
/// match the Stage 8.2 brief's "random kill/restart, random network
/// partition, random message delay (50-500ms), random message drop
/// (5-20%)" call-out.
#[derive(Debug, Clone)]
pub struct ChaosScheduleConfig {
    /// Total simulated time the chaos phase covers.
    pub duration: Duration,
    /// Mean simulated-time gap between successive events. The actual
    /// gap is sampled uniformly from `[0.5 * mean, 1.5 * mean]`.
    pub mean_interval: Duration,
    /// Minimum drop percentage the injector may dial in.
    pub min_drop_pct: u8,
    /// Maximum drop percentage the injector may dial in (inclusive).
    pub max_drop_pct: u8,
    /// Minimum latency the injector may dial in.
    pub min_latency: Duration,
    /// Maximum latency the injector may dial in (inclusive).
    pub max_latency: Duration,
    /// Maximum size of a partition group. The cluster's voter set
    /// determines the upper feasible bound (a partition group of
    /// `>= ceil(N/2)` cuts off the OTHER side from quorum, so we
    /// cap at `floor((N-1)/2)` to keep the test side a minority and
    /// ensure the cluster's majority side can keep committing).
    pub max_partition_group: usize,
}

impl ChaosScheduleConfig {
    /// Default chaos config for a 5-node cluster: 60 s simulated
    /// duration (matching the Stage 8.2
    /// `chaos-no-data-loss` acceptance criterion of a sustained
    /// 60-second chaos run), 250 ms mean inter-event gap, 5-20 %
    /// drop, 50-500 ms latency, partition groups of 1-2 nodes (so
    /// the surviving 3-4 always have quorum and the cluster can keep
    /// committing).
    pub fn five_node_default() -> Self {
        Self {
            duration: Duration::from_secs(60),
            mean_interval: Duration::from_millis(250),
            min_drop_pct: 5,
            max_drop_pct: 20,
            min_latency: Duration::from_millis(50),
            max_latency: Duration::from_millis(500),
            max_partition_group: 2,
        }
    }

    /// Short variant of [`Self::five_node_default`] used by the
    /// fault_injection unit tests (where covering a 60-second
    /// simulated schedule with thousands of generated events
    /// would inflate test wall-time without exercising any new
    /// generator code path). Same fault knobs, 5-second duration.
    pub fn five_node_short() -> Self {
        Self {
            duration: Duration::from_secs(5),
            ..Self::five_node_default()
        }
    }
}

/// Reproducible random fault generator.
///
/// `FaultInjector::new(seed)` constructs an injector whose
/// [`build_chaos_schedule`](Self::build_chaos_schedule) /
/// [`build_leader_churn_schedule`](Self::build_leader_churn_schedule)
/// outputs are bit-identical across runs of identical inputs. The
/// generator stores `(seed, cluster_size)` so build operations are
/// free of hidden state.
pub struct FaultInjector {
    rng: StdRng,
    cluster_size: u64,
    seed: u64,
}

impl FaultInjector {
    /// Build a fresh injector seeded with `seed`. `cluster_size` is
    /// the number of voters; events that target individual nodes
    /// sample from `NodeId(1..=cluster_size)`.
    pub fn new(seed: u64, cluster_size: u64) -> Self {
        assert!(cluster_size >= 1, "cluster_size must be >= 1");
        Self {
            rng: StdRng::seed_from_u64(seed),
            cluster_size,
            seed,
        }
    }

    /// The seed this injector was constructed with. Captured so
    /// diagnostic panic messages can echo it.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The voter cluster size this injector was constructed for.
    pub fn cluster_size(&self) -> u64 {
        self.cluster_size
    }

    /// Build a reproducible chaos schedule covering `cfg.duration` of
    /// simulated time. Event types are sampled uniformly across
    /// the four fault categories called out in the Stage 8.2 brief
    /// (partition, heal, drop-pct, latency). Every random decision
    /// flows through the injector's seeded RNG so the resulting
    /// schedule is replay-able.
    ///
    /// # Schedule shape
    ///
    /// The schedule alternates between "fault" and "heal" beats so a
    /// long-running run does not strand the cluster in a permanent
    /// partition. Specifically: every emitted [`FaultEvent::PartitionGroup`]
    /// or [`FaultEvent::SetDropPct`] / [`FaultEvent::SetLatency`] is
    /// followed within `~mean_interval` by a counter-event
    /// ([`FaultEvent::HealAll`] / `SetDropPct(0)` / `SetLatency(0)`)
    /// so the cluster has windows of quiet between faults during
    /// which proposals can converge. Without this back-pressure the
    /// cluster would never have a chance to commit a single entry
    /// once `mean_interval < election_max`.
    ///
    /// # Determinism contract
    ///
    /// Two injectors built from the SAME `(seed, cluster_size)` and
    /// asked for SAME `cfg` produce bit-identical schedules. The
    /// `chaos::network_partition::deterministic_replay_same_seed`
    /// test asserts this via `Vec::PartialEq`.
    pub fn build_chaos_schedule(&mut self, cfg: &ChaosScheduleConfig) -> FaultSchedule {
        assert!(
            cfg.max_drop_pct >= cfg.min_drop_pct,
            "max_drop_pct ({}) must be >= min_drop_pct ({})",
            cfg.max_drop_pct,
            cfg.min_drop_pct,
        );
        assert!(
            cfg.max_latency >= cfg.min_latency,
            "max_latency ({:?}) must be >= min_latency ({:?})",
            cfg.max_latency,
            cfg.min_latency,
        );
        let max_partition_group = cfg
            .max_partition_group
            .min(self.cluster_size.saturating_sub(1) as usize / 2)
            .max(1);

        let mut events: Vec<(Duration, FaultEvent)> = Vec::new();
        let mut at = Duration::ZERO;

        let mean_us = cfg.mean_interval.as_micros() as u64;
        let half = mean_us / 2;
        let lo = mean_us.saturating_sub(half).max(1);
        let hi = mean_us.saturating_add(half).max(lo + 1);

        while at < cfg.duration {
            // Sample a fault kind. Weights: partition and kill are
            // the most structurally interesting faults, so they get
            // more weight than the lossy-network knobs. Heal /
            // restart is implicit (every event is paired with its
            // counter).
            //
            // 4 kinds total: 0 = partition, 1 = drop, 2 = latency,
            // 3 = kill+restart. We cap kill picks against
            // `cluster_size` so a 1-node cluster never emits a kill
            // (would drop quorum to 0).
            let max_kind: u8 = if self.cluster_size >= 3 { 4 } else { 3 };
            let kind = self.rng.gen_range(0..max_kind);
            let event = match kind {
                0 => {
                    let group_size = self.rng.gen_range(1..=max_partition_group);
                    let group = self.sample_node_group(group_size);
                    FaultEvent::PartitionGroup(group)
                }
                1 => {
                    let pct = self.rng.gen_range(cfg.min_drop_pct..=cfg.max_drop_pct);
                    FaultEvent::SetDropPct(pct)
                }
                2 => {
                    let lat = self.sample_duration(cfg.min_latency, cfg.max_latency);
                    FaultEvent::SetLatency(lat)
                }
                _ => {
                    // Random node kill — pick any voter in
                    // `1..=cluster_size`. The paired counter-event is
                    // `Restart(NodeId)` (emitted below) so quorum is
                    // restored before the next beat.
                    let victim = NodeId(self.rng.gen_range(1..=self.cluster_size));
                    FaultEvent::Kill(victim)
                }
            };
            events.push((at, event.clone()));
            let gap_us = self.rng.gen_range(lo..=hi);
            at = at.saturating_add(Duration::from_micros(gap_us));
            if at >= cfg.duration {
                break;
            }
            // Counter-event so the cluster has a chance to converge
            // between faults.
            let counter = match &event {
                FaultEvent::PartitionGroup(_)
                | FaultEvent::PartitionCurrentLeader
                | FaultEvent::KillCurrentLeader
                | FaultEvent::RestartKilledLeader => FaultEvent::HealAll,
                FaultEvent::SetDropPct(_) => FaultEvent::SetDropPct(0),
                FaultEvent::SetLatency(_) => FaultEvent::SetLatency(Duration::ZERO),
                FaultEvent::HealAll => FaultEvent::HealAll,
                FaultEvent::Kill(nid) => FaultEvent::Restart(*nid),
                FaultEvent::Restart(_) => FaultEvent::HealAll,
            };
            events.push((at, counter));
            let gap_us = self.rng.gen_range(lo..=hi);
            at = at.saturating_add(Duration::from_micros(gap_us));
        }

        // Always end with a heal so the post-chaos
        // recovery phase starts from a clean network. Also append
        // a defensive Restart of every voter so any kill that fired
        // late and didn't get its paired Restart inside the duration
        // window doesn't leave the recovery phase below quorum.
        let final_at = cfg.duration;
        for nid in 1..=self.cluster_size {
            events.push((final_at, FaultEvent::Restart(NodeId(nid))));
        }
        events.push((final_at, FaultEvent::HealAll));
        events.push((final_at, FaultEvent::SetDropPct(0)));
        events.push((final_at, FaultEvent::SetLatency(Duration::ZERO)));

        FaultSchedule { events }
    }

    /// Build a TRUE kill+restart leader-churn schedule: every
    /// `interval` of simulated time, emit a
    /// [`FaultEvent::KillCurrentLeader`] followed by a
    /// [`FaultEvent::RestartKilledLeader`] one election-timeout
    /// window later. Used by the
    /// `chaos::node_failure::rapid_leader_churn_recovery` scenario to
    /// exercise the fail-stop kill path the brief specifies (vs the
    /// soft partition-then-heal model the previous helper used).
    ///
    /// Each beat:
    ///
    /// 1. At `t = k * interval`: `KillCurrentLeader` resolves the
    ///    current leader at apply-time and fail-stops it (driver task
    ///    aborted, network entry removed).
    /// 2. At `t = k * interval + restart_after`:
    ///    `RestartKilledLeader` re-spawns the most-recently-killed
    ///    leader with FRESH in-memory storage. The cluster's surviving
    ///    quorum has by now elected a new leader; the restarted node
    ///    rejoins as a follower and catches up via `AppendEntries` /
    ///    `InstallSnapshot`.
    ///
    /// `restart_after` MUST be ≥ one full election timeout window so
    /// the surviving majority has time to elect a new leader before
    /// the restarted (formerly killed) node rejoins.
    pub fn build_leader_churn_kill_schedule(
        &mut self,
        duration: Duration,
        interval: Duration,
        restart_after: Duration,
    ) -> FaultSchedule {
        assert!(interval > Duration::ZERO, "interval must be > 0");
        assert!(restart_after > Duration::ZERO, "restart_after must be > 0");
        assert!(
            restart_after < interval,
            "restart_after ({restart_after:?}) must be < interval ({interval:?}) \
             so the restart lands before the next kill beat"
        );
        let mut events = Vec::new();
        let mut at = interval; // first churn after one full interval
        // Pull a single u64 so two injectors with identical seeds
        // advance the RNG by the same amount as build_chaos_schedule
        // (keeps composed reproducibility).
        let _ = self.rng.r#gen::<u64>();
        while at < duration {
            events.push((at, FaultEvent::KillCurrentLeader));
            let restart_at = at.saturating_add(restart_after);
            events.push((restart_at, FaultEvent::RestartKilledLeader));
            at = at.saturating_add(interval);
        }
        // Final cleanup: defensive heal AND defensive restart of every
        // voter so the recovery phase starts with every voter alive.
        let final_at = duration;
        for nid in 1..=self.cluster_size {
            events.push((final_at, FaultEvent::Restart(NodeId(nid))));
        }
        events.push((final_at, FaultEvent::HealAll));
        FaultSchedule { events }
    }

    /// Build a "rapid leader churn" schedule: every `interval` of
    /// simulated time, emit a [`FaultEvent::PartitionCurrentLeader`]
    /// followed by a [`FaultEvent::HealAll`] one election-timeout
    /// window later. Used by chaos scenarios that want to exercise
    /// repeated re-election WITHOUT exercising the kill/restart
    /// engine path (the
    /// [`build_leader_churn_kill_schedule`](Self::build_leader_churn_kill_schedule)
    /// helper covers that).
    ///
    /// Why partition-then-heal rather than `kill`?
    ///
    /// The harness's [`SimulatedCluster::kill`](crate::SimulatedCluster::kill)
    /// drops the node's in-memory storage. Pairing kill with restart
    /// covers that — see
    /// [`build_leader_churn_kill_schedule`](Self::build_leader_churn_kill_schedule).
    /// The partition variant is preserved for scenarios that want to
    /// isolate ONLY the re-election path (no engine-side disk-recovery
    /// catch-up) from the test's failure mode.
    pub fn build_leader_churn_schedule(
        &mut self,
        duration: Duration,
        interval: Duration,
        heal_after: Duration,
    ) -> FaultSchedule {
        assert!(interval > Duration::ZERO, "interval must be > 0");
        assert!(heal_after > Duration::ZERO, "heal_after must be > 0");
        assert!(
            heal_after < interval,
            "heal_after ({heal_after:?}) must be < interval ({interval:?}) \
             so the partition is healed before the next churn beat"
        );
        let mut events = Vec::new();
        let mut at = interval; // first churn after one full interval
        // Pull a single u64 from the RNG so that two injectors with
        // identical seeds produce
        // identical churn schedules AND advance the RNG state by
        // the same amount as build_chaos_schedule's first step. This
        // keeps a future test that calls both methods on the same
        // injector reproducible.
        let _ = self.rng.r#gen::<u64>();
        while at < duration {
            events.push((at, FaultEvent::PartitionCurrentLeader));
            let heal_at = at.saturating_add(heal_after);
            events.push((heal_at, FaultEvent::HealAll));
            at = at.saturating_add(interval);
        }
        // final cleanup heal so the recovery phase starts clean
        let final_at = duration;
        events.push((final_at, FaultEvent::HealAll));
        FaultSchedule { events }
    }

    /// Sample a partition group of `group_size` distinct node ids
    /// from `1..=cluster_size`. Uses the injector's RNG so the
    /// sample is reproducible.
    fn sample_node_group(&mut self, group_size: usize) -> Vec<NodeId> {
        let group_size = group_size.min(self.cluster_size as usize);
        let mut all: Vec<NodeId> = (1..=self.cluster_size).map(NodeId).collect();
        // Fisher-Yates shuffle the first `group_size` slots using the
        // seeded RNG. Avoids pulling in `rand::seq::SliceRandom` (which
        // would couple the schedule to whatever shuffle algorithm
        // version that crate ships).
        for i in 0..group_size {
            let j = self.rng.gen_range(i..all.len());
            all.swap(i, j);
        }
        all.truncate(group_size);
        all.sort_by_key(|n| n.0);
        all
    }

    /// Sample a duration uniformly from `[lo, hi]` via the injector's
    /// RNG. Endpoints inclusive.
    fn sample_duration(&mut self, lo: Duration, hi: Duration) -> Duration {
        let lo_us = lo.as_micros() as u64;
        let hi_us = hi.as_micros() as u64;
        let v = if hi_us <= lo_us {
            lo_us
        } else {
            self.rng.gen_range(lo_us..=hi_us)
        };
        Duration::from_micros(v)
    }
}

// ---------------------------------------------------------------------------
// unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_identical_chaos_schedule() {
        let cfg = ChaosScheduleConfig::five_node_short();
        let mut a = FaultInjector::new(42, 5);
        let mut b = FaultInjector::new(42, 5);
        let sa = a.build_chaos_schedule(&cfg);
        let sb = b.build_chaos_schedule(&cfg);
        assert_eq!(sa, sb, "identical seeds must produce identical schedules");
        assert!(!sa.is_empty(), "schedule must contain at least one event");
        // Trailing heal/drop/latency-reset is always appended.
        let tail: Vec<&FaultEvent> = sa.events.iter().rev().take(3).map(|(_, e)| e).collect();
        assert!(
            tail.iter().any(|e| matches!(e, FaultEvent::HealAll)),
            "schedule tail must contain HealAll, got: {tail:?}"
        );
    }

    #[test]
    fn different_seed_produces_different_schedule() {
        let cfg = ChaosScheduleConfig::five_node_short();
        let mut a = FaultInjector::new(1, 5);
        let mut b = FaultInjector::new(2, 5);
        let sa = a.build_chaos_schedule(&cfg);
        let sb = b.build_chaos_schedule(&cfg);
        assert_ne!(sa, sb, "different seeds should produce different schedules");
    }

    #[test]
    fn leader_churn_schedule_has_alternating_partition_heal_pairs() {
        let mut inj = FaultInjector::new(42, 3);
        let sch = inj.build_leader_churn_schedule(
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_millis(500),
        );
        let partitions = sch
            .events
            .iter()
            .filter(|(_, e)| matches!(e, FaultEvent::PartitionCurrentLeader))
            .count();
        let heals = sch
            .events
            .iter()
            .filter(|(_, e)| matches!(e, FaultEvent::HealAll))
            .count();
        assert!(partitions > 0, "expected at least one churn beat");
        // One heal per partition + one trailing cleanup heal.
        assert_eq!(heals, partitions + 1);
    }

    #[test]
    fn leader_churn_kill_schedule_pairs_kill_with_restart() {
        let mut inj = FaultInjector::new(7, 5);
        let sch = inj.build_leader_churn_kill_schedule(
            Duration::from_secs(30),
            Duration::from_secs(2),
            Duration::from_millis(750),
        );
        let kills = sch
            .events
            .iter()
            .filter(|(_, e)| matches!(e, FaultEvent::KillCurrentLeader))
            .count();
        let restarts = sch
            .events
            .iter()
            .filter(|(_, e)| matches!(e, FaultEvent::RestartKilledLeader))
            .count();
        // Brief says "kill leader every 2 seconds for 30 seconds" =
        // 14 beats fit (first at t=2s, last at t=28s; t=30s is the
        // duration cap and excluded). The schedule must emit ONE
        // RestartKilledLeader per KillCurrentLeader.
        assert!(kills >= 14, "expected ≥ 14 kill beats, got {kills}");
        assert_eq!(
            kills, restarts,
            "every kill must be paired with a restart, got {kills} kills vs {restarts} restarts"
        );
    }

    #[test]
    fn chaos_schedule_emits_kill_and_restart_events() {
        // Use a longer schedule with smaller mean interval so the
        // kill-kind has ample opportunity to appear in the sample.
        let cfg = ChaosScheduleConfig {
            duration: Duration::from_secs(20),
            mean_interval: Duration::from_millis(100),
            ..ChaosScheduleConfig::five_node_default()
        };
        let mut inj = FaultInjector::new(0xDEAD_BEEF, 5);
        let sch = inj.build_chaos_schedule(&cfg);
        let kills = sch
            .events
            .iter()
            .filter(|(_, e)| matches!(e, FaultEvent::Kill(_)))
            .count();
        let restarts = sch
            .events
            .iter()
            .filter(|(_, e)| matches!(e, FaultEvent::Restart(_)))
            .count();
        assert!(
            kills > 0,
            "build_chaos_schedule must emit at least one Kill event \
             across a 20-second schedule; got {kills}"
        );
        // Every kill is paired with a restart in the loop body. The
        // final defensive restart of every voter adds an extra
        // `cluster_size` (5) restarts.
        assert!(
            restarts >= kills + 5,
            "expected ≥ kills + 5 restarts (kill-pair + final defensive), \
             got kills={kills}, restarts={restarts}"
        );
    }

    #[test]
    fn partition_group_respects_max_minority_cap() {
        let cfg = ChaosScheduleConfig {
            max_partition_group: 99,
            ..ChaosScheduleConfig::five_node_short()
        };
        let mut inj = FaultInjector::new(0xC0FFEE, 5);
        let sch = inj.build_chaos_schedule(&cfg);
        for (_, e) in &sch.events {
            if let FaultEvent::PartitionGroup(g) = e {
                // 5-node cluster: (5-1)/2 = 2 nodes max minority side.
                assert!(g.len() <= 2, "partition group too large: {:?}", g);
                assert!(!g.is_empty(), "partition group must be non-empty");
            }
        }
    }
}
