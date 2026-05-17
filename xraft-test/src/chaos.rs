//! Chaos engine for the xraft simulated cluster harness.
//!
//! The engine deterministically injects faults (node isolation, kill/restart,
//! message drops, partitions) into a `TestCluster`, records each fault into a
//! history vector keyed by simulated time, and exposes helpers to drive the
//! cluster forward and let it settle.
//!
//! ## Invariants
//!
//! 1. Every entry pushed into `history` is timestamped with the simulated
//!    clock value at the moment the fault (or marker) was applied.
//! 2. Timestamps in `history` are monotonically non-decreasing. The
//!    `deterministic_replay` test compares histories from two runs entry by
//!    entry, so any inserted entry MUST carry an accurate timestamp -- never
//!    `Duration::ZERO` as a placeholder.

use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::cluster::TestCluster;
use crate::clock::SimulatedClock;

/// A fault that the chaos engine can inject into the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChaosFault {
    /// Marker fault used to delimit phases of a test (e.g. the boundary
    /// between "inject" and "settle"). Carries no behavioural effect on the
    /// cluster -- it exists only so the history records that the test
    /// transitioned to the settle phase at a particular simulated time.
    Noop,
    /// Isolate the node at the given index from every other node.
    IsolateNode(usize),
    /// Re-establish connectivity for the node at the given index.
    RejoinNode(usize),
    /// Kill the node at the given index and immediately restart it from
    /// persistent storage.
    KillRestart(usize),
    /// Drop the next message from `from` to `to`.
    DropMessage { from: usize, to: usize },
    /// Partition the cluster into a majority side and a minority side.
    /// `minority_size` nodes (lowest indices) are placed on the minority side.
    PartitionMajority { minority_size: usize },
    /// Heal any active partition, restoring full connectivity.
    HealPartition,
}

/// Configuration knobs for the chaos engine.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Deterministic seed for the engine's PRNG.
    pub seed: u64,
    /// Maximum number of faults injected before the engine stops on its own.
    pub max_faults: usize,
    /// Simulated time advanced between successive faults.
    pub step: Duration,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            seed: 0xC0FFEE,
            max_faults: 32,
            step: Duration::from_millis(50),
        }
    }
}

/// Drives fault injection into a `TestCluster`.
pub struct ChaosEngine {
    config: ChaosConfig,
    rng: SmallRng,
    history: Vec<(Duration, ChaosFault)>,
    /// Nodes currently isolated (by index). Tracked so that `apply()` can
    /// be replayed deterministically and so `heal_all()` knows what to undo.
    isolated: Vec<usize>,
    /// Whether the cluster is currently partitioned.
    partitioned: bool,
}

impl ChaosEngine {
    pub fn new(config: ChaosConfig) -> Self {
        let rng = SmallRng::seed_from_u64(config.seed);
        Self {
            config,
            rng,
            history: Vec::new(),
            isolated: Vec::new(),
            partitioned: false,
        }
    }

    /// Read-only view of the recorded fault history.
    pub fn history(&self) -> &[(Duration, ChaosFault)] {
        &self.history
    }

    /// Apply a single fault to the cluster. Records it in the history at the
    /// cluster's current simulated time.
    pub fn apply(&mut self, cluster: &mut TestCluster, fault: ChaosFault) {
        let now = cluster.clock.elapsed();
        self.apply_at(cluster, fault, now);
    }

    /// Apply a fault that only affects network state (drops/partitions/heals),
    /// not node lifecycle. Used by the stress test to share semantics without
    /// also touching kill/restart paths.
    pub fn apply_network_only(&mut self, cluster: &mut TestCluster, fault: ChaosFault) {
        debug_assert!(
            matches!(
                fault,
                ChaosFault::IsolateNode(_)
                    | ChaosFault::RejoinNode(_)
                    | ChaosFault::DropMessage { .. }
                    | ChaosFault::PartitionMajority { .. }
                    | ChaosFault::HealPartition
                    | ChaosFault::Noop
            ),
            "apply_network_only invoked with non-network fault: {fault:?}"
        );
        self.apply(cluster, fault);
    }

    fn apply_at(&mut self, cluster: &mut TestCluster, fault: ChaosFault, sim_time: Duration) {
        match fault {
            ChaosFault::Noop => {}
            ChaosFault::IsolateNode(idx) => {
                cluster.isolate(idx);
                if !self.isolated.contains(&idx) {
                    self.isolated.push(idx);
                }
            }
            ChaosFault::RejoinNode(idx) => {
                cluster.rejoin(idx);
                self.isolated.retain(|&i| i != idx);
            }
            ChaosFault::KillRestart(idx) => {
                cluster.kill(idx);
                cluster.restart(idx);
            }
            ChaosFault::DropMessage { from, to } => {
                cluster.drop_next_message(from, to);
            }
            ChaosFault::PartitionMajority { minority_size } => {
                cluster.partition_minority(minority_size);
                self.partitioned = true;
            }
            ChaosFault::HealPartition => {
                cluster.heal_partition();
                self.partitioned = false;
            }
        }
        self.history.push((sim_time, fault));
    }

    /// Inject a randomly chosen fault, picked deterministically from `self.rng`.
    pub fn inject_random(&mut self, cluster: &mut TestCluster) -> Option<ChaosFault> {
        if self.history.iter().filter(|(_, f)| !matches!(f, ChaosFault::Noop)).count()
            >= self.config.max_faults
        {
            return None;
        }
        let n = cluster.node_count();
        let fault = match self.rng.gen_range(0..6) {
            0 => ChaosFault::IsolateNode(self.rng.gen_range(0..n)),
            1 => {
                if let Some(&idx) = self.isolated.first() {
                    ChaosFault::RejoinNode(idx)
                } else {
                    ChaosFault::IsolateNode(self.rng.gen_range(0..n))
                }
            }
            2 => ChaosFault::KillRestart(self.rng.gen_range(0..n)),
            3 => {
                let from = self.rng.gen_range(0..n);
                let mut to = self.rng.gen_range(0..n);
                if to == from {
                    to = (to + 1) % n;
                }
                ChaosFault::DropMessage { from, to }
            }
            4 => ChaosFault::PartitionMajority {
                minority_size: 1.max(self.rng.gen_range(1..=n / 2)),
            },
            _ => {
                if self.partitioned {
                    ChaosFault::HealPartition
                } else {
                    ChaosFault::IsolateNode(self.rng.gen_range(0..n))
                }
            }
        };
        self.apply(cluster, fault);
        cluster.clock.advance(self.config.step);
        Some(fault)
    }

    /// Heal every active fault: rejoin isolated nodes, heal partitions, etc.
    /// Does not touch the history -- callers wishing to mark the transition
    /// to the settle phase should call [`settle`] instead.
    pub fn heal_all(&mut self, cluster: &mut TestCluster) {
        let isolated: Vec<usize> = self.isolated.drain(..).collect();
        for idx in isolated {
            cluster.rejoin(idx);
        }
        if self.partitioned {
            cluster.heal_partition();
            self.partitioned = false;
        }
    }

    /// Transition the engine into the "settle" phase: heal every active
    /// fault and record a `Noop` marker into the history.
    ///
    /// `sim_time` MUST be the simulated time at which settle is invoked
    /// (typically `cluster.clock.elapsed()`). Recording the actual simulated
    /// time -- not `Duration::ZERO` -- is required to preserve the
    /// monotonic-timestamp invariant of `history`, which the determinism
    /// replay test relies on when comparing histories entry by entry.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let now = cluster.clock.elapsed();
    /// engine.settle(&mut cluster, now);
    /// ```
    pub fn settle(&mut self, cluster: &mut TestCluster, sim_time: Duration) {
        debug_assert!(
            self.history
                .last()
                .map(|(t, _)| *t <= sim_time)
                .unwrap_or(true),
            "settle() called with sim_time {sim_time:?} earlier than last history entry"
        );
        self.heal_all(cluster);
        self.history.push((sim_time, ChaosFault::Noop));
    }

    /// Convenience overload that reads the simulated time from a
    /// `SimulatedClock` reference. Equivalent to
    /// `settle(cluster, clock.elapsed())` but avoids the call-site needing to
    /// repeat the `elapsed()` call when the clock is already in scope.
    pub fn settle_with_clock(
        &mut self,
        cluster: &mut TestCluster,
        clock: &SimulatedClock,
    ) {
        self.settle(cluster, clock.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::TestCluster;

    /// Drive the cluster through random faults and assert that no committed
    /// entry is ever lost across the settle boundary.
    #[test]
    fn chaos_no_data_loss() {
        let mut cluster = TestCluster::new(5);
        let mut engine = ChaosEngine::new(ChaosConfig::default());

        cluster.elect_leader();
        let baseline = cluster.propose_n(100);

        while engine.inject_random(&mut cluster).is_some() {
            cluster.tick(Duration::from_millis(10));
        }

        // Record the actual simulated time at which we transition into the
        // settle phase, so the history's monotonic-timestamp invariant holds.
        let sim_time = cluster.clock.elapsed();
        engine.settle(&mut cluster, sim_time);

        cluster.run_until_quiescent(Duration::from_secs(5));

        let final_committed = cluster.committed_entries();
        for entry in &baseline {
            assert!(
                final_committed.contains(entry),
                "lost committed entry {:?} after settle",
                entry
            );
        }

        // Sanity check: history is timestamp-monotonic, including the Noop
        // marker we just pushed.
        let mut last = Duration::ZERO;
        for (t, _) in engine.history() {
            assert!(*t >= last, "history timestamps regressed at {:?}", t);
            last = *t;
        }
    }

    /// Verify that the recorded history is linearisable across the inject /
    /// settle boundary.
    #[test]
    fn chaos_linearisability() {
        let mut cluster = TestCluster::new(5);
        let mut engine = ChaosEngine::new(ChaosConfig {
            seed: 0xDEADBEEF,
            max_faults: 16,
            step: Duration::from_millis(25),
        });

        cluster.elect_leader();

        let ops = cluster.spawn_linearisability_workload(64);
        while engine.inject_random(&mut cluster).is_some() {
            cluster.tick(Duration::from_millis(5));
        }

        let sim_time = cluster.clock.elapsed();
        engine.settle(&mut cluster, sim_time);

        cluster.run_until_quiescent(Duration::from_secs(5));
        let history = cluster.recorded_ops(&ops);
        crate::linearisability::verify_linearisable(&history)
            .expect("history must be linearisable after settle");

        // The Noop marker must be at-or-after every prior history entry.
        let (settle_ts, settle_fault) = *engine
            .history()
            .last()
            .expect("history must contain the settle marker");
        assert_eq!(settle_fault, ChaosFault::Noop);
        assert_eq!(settle_ts, sim_time);
    }

    /// Two runs with the same seed must produce byte-identical histories,
    /// including timestamps. This test is the canonical reason `settle()`
    /// cannot record `Duration::ZERO`: if it did, the marker would still
    /// match across runs, but the timestamp would be a lie that downstream
    /// consumers (and this test, if it ever stops trusting the marker) would
    /// be unable to detect.
    #[test]
    fn deterministic_replay() {
        fn run(seed: u64) -> Vec<(Duration, ChaosFault)> {
            let mut cluster = TestCluster::new(5);
            let mut engine = ChaosEngine::new(ChaosConfig {
                seed,
                max_faults: 24,
                step: Duration::from_millis(40),
            });
            cluster.elect_leader();
            while engine.inject_random(&mut cluster).is_some() {
                cluster.tick(Duration::from_millis(8));
            }
            let sim_time = cluster.clock.elapsed();
            engine.settle(&mut cluster, sim_time);
            engine.history().to_vec()
        }

        let a = run(0xABCDEF);
        let b = run(0xABCDEF);
        assert_eq!(a, b, "histories diverged across deterministic runs");

        // The final entry of each run is the settle marker and must carry a
        // non-zero, realistic simulated time -- not the `Duration::ZERO`
        // placeholder a previous revision used.
        let (ts, fault) = *a.last().unwrap();
        assert_eq!(fault, ChaosFault::Noop);
        assert!(
            ts > Duration::ZERO,
            "settle marker timestamp must reflect actual simulated time, got {ts:?}"
        );
    }

    /// Regression test for the review feedback that prompted this revision:
    /// `settle()` must record the simulated time supplied by the caller, not
    /// `Duration::ZERO`, so the history's monotonic-timestamp invariant
    /// survives the inject -> settle transition.
    #[test]
    fn settle_records_supplied_sim_time() {
        let mut cluster = TestCluster::new(3);
        let mut engine = ChaosEngine::new(ChaosConfig::default());

        cluster.clock.advance(Duration::from_secs(7));
        let now = cluster.clock.elapsed();
        engine.settle(&mut cluster, now);

        let (ts, fault) = *engine.history().last().expect("settle pushes a marker");
        assert_eq!(fault, ChaosFault::Noop);
        assert_eq!(
            ts, now,
            "settle must record the supplied sim_time, not Duration::ZERO"
        );
    }
}
