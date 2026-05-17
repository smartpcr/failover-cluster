//! Stage 8.2 scenario: rapid-leader-churn-recovery.
//!
//! Brief: "test: rapid leader churn (kill leader every 2 seconds
//! for 30 seconds), verify cluster recovers each time and no
//! committed entries are lost."
//!
//! # Kill semantics
//!
//! This test exercises **true process kill+restart** via
//! [`SimulatedCluster::kill`] followed by
//! [`SimulatedCluster::revive`]. The killed leader's driver task
//! is aborted; its durable Raft state (log, hard-state, snapshot
//! store) is preserved across the abort by the
//! [`xraft_test::PersistentNodeStorage`] wrappers and handed back
//! to the freshly-spawned driver on revive. This matches the
//! brief's "kill leader" semantics (a real process crash where
//! the WAL survives on disk, which Raft is designed to handle
//! via standard fetch / snapshot install).
//!
//! # Cadence and duration
//!
//! "Every 2 seconds for 30 seconds" = 15 cycles at a 2-sim-sec
//! cadence. Each cycle:
//!
//! 1. Fires `PROPOSALS_PER_CYCLE` proposals at the current
//!    reachable leader and records their committed indices.
//! 2. Waits until at least `CYCLE_SIM_GAP` of simulated time has
//!    elapsed since the *previous* kill (so the kill-rate is
//!    strictly the brief's 0.5 Hz at the simulated-time level,
//!    regardless of host CPU speed).
//! 3. Resolves the current leader, kills it (aborts driver), then
//!    immediately revives it (re-spawns driver against preserved
//!    storage). The revived node rejoins the cluster as a
//!    follower at its persisted term.
//!
//! At the end, the test verifies a unique leader emerges and a
//! majority of nodes have applied every recorded committed entry
//! at the returned (index, payload) coordinate.

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use xraft_core::types::NodeId;
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

const CHURN_CYCLES: usize = 15;
const PROPOSALS_PER_CYCLE: usize = 4;
const PROPOSE_RETRIES: u8 = 8;
/// Simulated-time gap between successive leader kills. 2 s × 15
/// cycles = the brief's "every 2 seconds for 30 seconds" window
/// in simulated time. Wall time is decoupled via the fast manual
/// pump.
const CYCLE_SIM_GAP: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rapid_leader_churn_preserves_committed_entries() {
    let _ = tracing_subscriber::fmt::try_init();

    // 5-node cluster: killing the leader still leaves 4 voters,
    // which is a quorum of 5 — so each cycle's re-election window
    // never crosses the quorum-loss line. Storage preservation
    // ensures the revived ex-leader rejoins at its persisted
    // term and does not accept any vote it had already cast.
    let cfg = SimulatedClusterConfig::five_node(0xC0FF_EE83);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("initial leader must be elected");

    let mut committed: Vec<(u64, Bytes)> = Vec::new();
    let mut next_op = 0u64;
    let mut next_kill_at_sim: Duration = cluster.clock.elapsed();

    for cycle in 0..CHURN_CYCLES {
        // Wait until at least `CYCLE_SIM_GAP` of simulated time
        // has elapsed since the previous kill — this is the
        // brief's literal 2-sim-sec cadence.
        while cluster.clock.elapsed() < next_kill_at_sim {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Fire `PROPOSALS_PER_CYCLE` proposals on the current
        // reachable leader. No nodes are network-isolated in this
        // test — `isolated` stays empty.
        let isolated: HashSet<NodeId> = HashSet::new();
        for _ in 0..PROPOSALS_PER_CYCLE {
            let payload = Bytes::copy_from_slice(&next_op.to_be_bytes());
            if let Some(idx) = propose_with_chaos_retry(&cluster, &isolated, payload.clone()).await
            {
                committed.push((idx, payload));
            }
            next_op += 1;
        }

        // Resolve the current leader (lax — tolerate ex-leaders
        // whose status observer hasn't refreshed yet).
        let leader = wait_for_any_reachable_leader(&cluster, &isolated, Duration::from_secs(20))
            .await
            .unwrap_or_else(|| panic!("cycle {cycle}: no reachable leader to kill"));

        tracing::info!(
            target: "rapid_churn",
            cycle,
            leader = leader.0,
            "killing leader (preserved-storage process-restart) at sim_t={:?}",
            cluster.clock.elapsed(),
        );

        // True process kill+restart: aborts the driver task and
        // immediately respawns it against the preserved durable
        // storage. The revived ex-leader rejoins as a follower
        // at its persisted term.
        cluster.kill(leader);
        cluster
            .revive(leader)
            .unwrap_or_else(|e| panic!("cycle {cycle}: revive of node {leader:?} failed: {e}"));

        // Schedule the next kill for `CYCLE_SIM_GAP` later in
        // SIMULATED time so the cadence is host-independent.
        next_kill_at_sim = cluster.clock.elapsed() + CYCLE_SIM_GAP;

        // Wait for a new (or re-elected) leader before the next
        // cycle. Lax resolver — see comment above.
        match wait_for_any_reachable_leader(&cluster, &isolated, Duration::from_secs(30)).await {
            Some(_) => {}
            None => {
                let mut diag = Vec::new();
                for n in &cluster.nodes {
                    let snap = n.status.status().await;
                    let txt = snap
                        .map(|s| {
                            format!(
                                "role={:?} term={} leader_id={:?} commit={} applied={}",
                                s.role, s.term, s.leader_id, s.commit_index, s.last_applied,
                            )
                        })
                        .unwrap_or_else(|| "<no status>".to_string());
                    diag.push(format!(
                        "  node{} alive={}: {}",
                        n.node_id.0,
                        n.is_alive(),
                        txt
                    ));
                }
                panic!(
                    "cycle {cycle}: no new reachable leader after killing leader \
                     {leader:?}\nNode statuses:\n{}",
                    diag.join("\n")
                );
            }
        }
    }

    cluster
        .await_leader(Duration::from_secs(30))
        .await
        .expect("a unique leader must emerge after the final kill+revive");

    let target_idx = committed.iter().map(|(i, _)| *i).max().unwrap_or(0);
    if target_idx > 0 {
        // Catch-up budget. With storage preservation the revived
        // ex-leader has its full log already on disk — it just
        // needs to discover the new leader via heartbeat. The
        // brief's "no committed entries lost" check is majority-
        // based per the spec.
        let majority_n = cluster.len() / 2 + 1;
        let start = std::time::Instant::now();
        loop {
            let mut at_or_above = 0usize;
            for n in &cluster.nodes {
                if !n.is_alive() {
                    continue;
                }
                if n.recording.last_applied() >= target_idx {
                    at_or_above += 1;
                }
            }
            if at_or_above >= majority_n {
                break;
            }
            if start.elapsed() > Duration::from_secs(180) {
                let mut diag: Vec<String> = Vec::new();
                for n in &cluster.nodes {
                    diag.push(format!(
                        "node{}: last_applied={} len={} alive={}",
                        n.node_id.0,
                        n.recording.last_applied(),
                        n.recording.len(),
                        n.is_alive(),
                    ));
                }
                panic!(
                    "post-churn convergence on a majority ({majority_n}/{}) stalled: \
                     target={target_idx}, only {at_or_above} node(s) caught up; nodes: {}",
                    cluster.len(),
                    diag.join(" | ")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Every committed entry the test acked must be present at the
    // returned index on a MAJORITY of nodes (brief's "no committed
    // entries lost" check — majority semantics per Raft).
    let majority = cluster.len() / 2 + 1;
    for (idx, payload) in &committed {
        let mut present = 0usize;
        for n in &cluster.nodes {
            let applied = n.recording.applied();
            if applied
                .iter()
                .any(|(i, p)| *i == *idx && p.as_slice() == payload.as_ref())
            {
                present += 1;
            }
        }
        assert!(
            present >= majority,
            "committed entry at index {idx} present on only {present}/{} nodes; \
             majority {majority} required",
            cluster.len()
        );
    }

    // Sanity guard — the churn run MUST commit at least one entry.
    assert!(
        !committed.is_empty(),
        "rapid-churn committed 0 entries — would assert vacuously"
    );

    cluster.shutdown().await;
}

async fn propose_with_chaos_retry(
    cluster: &SimulatedCluster,
    isolated: &HashSet<NodeId>,
    payload: Bytes,
) -> Option<u64> {
    for _ in 0..PROPOSE_RETRIES {
        match cluster
            .propose_via_reachable_leader(isolated, payload.clone())
            .await
        {
            Ok(idx) => return Some(idx.0),
            Err(_) => {
                let _ = cluster
                    .await_reachable_leader(isolated, Duration::from_secs(5))
                    .await;
            }
        }
    }
    None
}

/// Lax variant of `await_reachable_leader`: returns the first node
/// among the non-isolated, alive set that *currently* believes
/// itself to be leader at the cluster's max term.
async fn wait_for_any_reachable_leader(
    cluster: &SimulatedCluster,
    isolated: &HashSet<NodeId>,
    deadline: Duration,
) -> Option<NodeId> {
    use xraft_core::types::NodeRole;

    let start = std::time::Instant::now();
    let wall_backstop = deadline * 10 + Duration::from_secs(30);
    let sim_deadline_at = cluster.clock.elapsed() + deadline;
    loop {
        if cluster.clock.elapsed() > sim_deadline_at {
            return None;
        }
        if start.elapsed() > wall_backstop {
            return None;
        }
        let mut max_term: u64 = 0;
        let mut leader_at_max: Option<NodeId> = None;
        let mut leader_count_at_max: usize = 0;
        for n in &cluster.nodes {
            if !n.is_alive() {
                continue;
            }
            if isolated.contains(&n.node_id) {
                continue;
            }
            let Some(status) = n.status.status().await else {
                continue;
            };
            let t = status.term;
            if t > max_term {
                max_term = t;
                leader_at_max = None;
                leader_count_at_max = 0;
            }
            if t == max_term && status.role == NodeRole::Leader {
                leader_count_at_max += 1;
                leader_at_max = Some(n.node_id);
            }
        }
        if leader_count_at_max == 1 {
            return leader_at_max;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
