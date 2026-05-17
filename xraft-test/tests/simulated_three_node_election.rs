//! Stage 8.1 scenario: three-node-election.
//!
//! Given a 3-node `SimulatedCluster` started simultaneously, when the
//! election timeouts elapse, then exactly one leader is elected and
//! every other node agrees on the term.
//!
//! # Deterministic-tick advancement
//!
//! The Stage 8.1 brief requires the simulated tests to drive ticks
//! deterministically rather than through a wall-clock
//! `tokio::time::interval`. This test detaches the wall-clock pump
//! and uses [`SimulatedCluster::start_manual_pump`] for the election
//! phase: every tick the drivers observe is fired by a test-owned
//! manual pump task driven by
//! [`xraft_test::ManualTickController::trigger`], NOT by a
//! `tokio::time::interval` cadence.
//!
//! The manual pump runs in its own task so the convergence wait
//! (notify-driven [`SimulatedCluster::await_leader`]) does not have
//! to share a worker thread with the tick source. A shared worker
//! starves the drivers under workspace-parallel `cargo test`
//! because yield-based cadences do not force cross-worker scheduling.

use std::time::Duration;

use xraft_core::types::NodeRole;
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_one_leader() {
    let _ = tracing_subscriber::fmt::try_init();

    // 500-1000 ms randomised election window. The test drives
    // simulated time directly so the WALL-clock duration is bounded
    // by tokio scheduling latency, not by `election_max`. The 2 s
    // simulated deadline below binds the simulated-time budget the
    // engine has to elect.
    let cfg = SimulatedClusterConfig {
        election_min_ms: 500,
        election_max_ms: 1000,
        ..SimulatedClusterConfig::three_node(0xC0FF_EE01)
    };
    let election_max = Duration::from_millis(cfg.election_max_ms);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    // Detach the wall-clock pump so all subsequent tick advancement
    // flows through the test-owned [`ManualTickController`].
    cluster.detach_tick_pump().await;
    // Spawn the manual fast pump in its own task so the convergence
    // wait below (notify-driven [`SimulatedCluster::await_leader`])
    // does not have to share a worker thread with the tick source.
    // `ticks_per_burst=4` matches the cadence used by all other
    // simulated tests.
    cluster.start_manual_pump(4);

    // Brief requirement: "elects a leader within 2 election timeout
    // periods after startup". The deadline below is SIMULATED time;
    // the wall-clock backstop inside `await_leader`
    // (= 10 × deadline + 30 s) keeps a runaway-pump bug from
    // hanging the test indefinitely.
    let deadline = election_max * 2;
    let (_leader_id, leader_term) = cluster
        .await_leader(deadline)
        .await
        .expect("must elect a leader within 2 election-timeout periods of simulated time");

    assert!(
        leader_term >= 1,
        "leader term must be >= 1, was {leader_term}"
    );

    // Every alive node should agree on the leader's term — the
    // strict-convergence check inside `await_leader` already pinned
    // this, but re-assert here so a future refactor of that helper
    // cannot silently weaken the test contract.
    let statuses = cluster.statuses().await;
    let mut leader_count = 0;
    for (node_id, snap) in &statuses {
        let snap = snap.as_ref().expect("status must be populated by now");
        assert_eq!(
            snap.term, leader_term,
            "node {} disagrees on term: got {}, expected {}",
            node_id.0, snap.term, leader_term
        );
        if snap.role == NodeRole::Leader {
            leader_count += 1;
        }
    }
    assert_eq!(
        leader_count, 1,
        "exactly one node should be Leader, got {leader_count}; statuses = {statuses:?}"
    );

    cluster.shutdown().await;
}
