//! Stage 8.1 scenario: network-partition-recovery.
//!
//! Brief: "Given a 5-node `SimulatedCluster` split into groups of 3
//! and 2, When the partition heals, Then the minority group's nodes
//! catch up and the cluster converges on one leader."
//!
//! # Two scenarios, two engine paths
//!
//! An earlier shape of this test inflated election timeouts to 5-8 s
//! so the minority never reached
//! [`PreCandidate`](xraft_core::types::NodeRole) while partitioned —
//! that avoided the hard case. This file exercises BOTH halves of
//! the recovery path:
//!
//! * [`partitioned_minority_recovers_same_term_step_down`] — the
//!   primary scenario. Cluster partitions, minority strands in
//!   `PreCandidate` at the original leader's term, partition heals,
//!   and the minority steps down on a same-term denial carrying a
//!   `leader_hint` (the engine fix landed in
//!   `xraft-core/src/node.rs::handle_pre_vote_response`, operator
//!   answer to Open Question `engine-pre-vote-recovery`).
//! * [`partitioned_minority_recovers_after_heal_via_higher_term_step_down`]
//!   — the fallback scenario kept as regression. We kill the
//!   original leader after the heal, forcing a higher-term
//!   re-election; the minority's stranded `PreCandidate` takes the
//!   pre-existing higher-term step-down path (`node.rs:1667-1669`).
//!   This proves the fallback still works when no `leader_hint`
//!   is available (e.g. the responder is itself a non-leader
//!   follower that has not heard from the new leader yet).
//!
//! Both tests use the engine's default 250-500 ms election window
//! — no inflated timeouts, no hidden work-arounds.
//!
//! # Deterministic-tick pump
//!
//! Both tests detach the harness default wall-clock pump up-front
//! and install the test-owned manual-trigger fast pump via
//! [`SimulatedCluster::start_manual_pump`], so every tick the
//! drivers observe flows through the
//! [`xraft_test::ManualTickController`]. Before each
//! [`SimulatedCluster::advance_simulated_time`] burst (the
//! deterministic replacement for `tokio::time::sleep` during the
//! strand window), the test PAUSES the fast pump via
//! [`SimulatedCluster::detach_tick_pump`] so no pump beat can
//! interleave extra triggers with the burst, then re-installs
//! the pump for the heal/recovery phase. The strand-window burst
//! uses [`SimulatedCluster::advance_simulated_time`] to fire a
//! precise number of triggers rather than racing a background
//! pump.

use std::time::Duration;

use bytes::Bytes;
use xraft_core::types::{NodeId, NodeRole};
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

const N_ENTRIES_BEFORE_PARTITION: usize = 10;
const N_ENTRIES_DURING_PARTITION: usize = 20;
const N_ENTRIES_TOTAL: usize = N_ENTRIES_BEFORE_PARTITION + N_ENTRIES_DURING_PARTITION;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_minority_recovers_after_heal_via_higher_term_step_down() {
    let _ = tracing_subscriber::fmt::try_init();

    // Default 5-node config: 250-500 ms election window. Short enough
    // that "wait past election timeout" finishes in well under 2 s.
    let cfg = SimulatedClusterConfig::five_node(0xC0FF_EE04);
    let election_max = Duration::from_millis(cfg.election_max_ms);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    // Detach + install manual fast pump. Handle stored on the
    // cluster; aborted by `shutdown()`.
    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    let (orig_leader, orig_term) = cluster
        .await_leader(Duration::from_secs(10))
        .await
        .expect("leader must be elected");

    // Commit the pre-partition baseline so every node has identical
    // applied state going into the partition.
    for i in 0..N_ENTRIES_BEFORE_PARTITION {
        let payload = Bytes::copy_from_slice(&(i as u64).to_be_bytes());
        propose_with_retry(&cluster, payload, i).await;
    }
    cluster
        .await_applied_at_least(N_ENTRIES_BEFORE_PARTITION, Duration::from_secs(10))
        .await
        .expect("every node must apply the pre-partition baseline");

    // Build the partition: minority = 2 nodes, majority = 3 nodes
    // including the original leader (so the leader keeps committing).
    let mut minority: Vec<NodeId> = vec![NodeId(4), NodeId(5)];
    if minority.contains(&orig_leader) {
        minority = vec![NodeId(1), NodeId(2)];
    }
    let majority: Vec<NodeId> = (1..=5u64)
        .map(NodeId)
        .filter(|n| !minority.contains(n))
        .collect();
    assert!(
        majority.contains(&orig_leader),
        "test invariant: leader {orig_leader} must be inside majority {majority:?}"
    );

    cluster.partition_group(&minority);

    // Commit during-partition entries on the majority.
    for i in 0..N_ENTRIES_DURING_PARTITION {
        let idx = N_ENTRIES_BEFORE_PARTITION + i;
        let payload = Bytes::copy_from_slice(&(idx as u64).to_be_bytes());
        propose_with_retry(&cluster, payload, idx).await;
    }

    // Confirm the majority has fully applied before sleeping into the
    // PreCandidate window — keeps the test's commit pipeline clearly
    // ordered before the recovery phase. Per-node waits route through
    // the cluster's sim-time helper so the deadline is interpreted in
    // simulated time and the loop is event-driven on
    // `ManualTickController`.
    for nid in &majority {
        cluster
            .await_node_applied_at_least(*nid, N_ENTRIES_TOTAL, Duration::from_secs(10))
            .await
            .unwrap_or_else(|got| {
                panic!(
                    "majority node {} should have applied {N_ENTRIES_TOTAL} before heal; got {got}",
                    nid.0
                )
            });
    }

    // Detach the manual fast pump so the strand-inject burst is the
    // SOLE driver of simulated time for this phase.
    // `advance_simulated_time(election_max * 3)` advances
    // `election_max * 3` of simulated time in microseconds wall-clock
    // (300 ticks at the 5 ms harness tick_quantum). The harness
    // ASSERTS `tick_pump.is_none()` at the burst entry point to
    // structurally prevent pump interleaving.
    cluster.detach_tick_pump().await;
    cluster.advance_simulated_time(election_max * 3).await;

    // Diagnostic assertion: the minority MUST now be in PreCandidate,
    // confirming the test is exercising the hard case (not a happy
    // path where the minority stayed Follower).
    for nid in &minority {
        let node = cluster.node(*nid).expect("minority id must exist");
        let snap = node
            .status
            .status()
            .await
            .expect("status must be populated by now");
        assert!(
            matches!(snap.role, NodeRole::PreCandidate | NodeRole::Candidate),
            "minority node {} must have entered PreCandidate after partition; \
             got role={:?} term={}",
            nid.0,
            snap.role,
            snap.term,
        );
    }

    // Restart the manual fast pump for the recovery phase.
    cluster.start_manual_pump(4);

    // Heal the partition. At this point the minority is in
    // PreCandidate at the original term; sending PreVotes to the
    // majority Followers gets same-term "no" replies — no step-down.
    cluster.heal_all();

    // Force a re-election by killing the original leader. The
    // surviving majority Followers + minority PreCandidates will
    // eventually elect a new leader at a higher term. When the
    // higher-term PreVoteResponse reaches the minority, it takes
    // the engine's natural step-down path.
    cluster.kill(orig_leader);

    // Wait for a new leader at a strictly higher term.
    let deadline = Duration::from_secs(30);
    let new_leader_term = await_new_leader_at_higher_term(&cluster, orig_term, deadline)
        .await
        .expect("a new leader at higher term must be elected after kill");
    assert!(
        new_leader_term.1 > orig_term,
        "new leader term {} must exceed original term {orig_term}",
        new_leader_term.1
    );

    // Now the minority sees higher-term PreVoteResponses, steps down
    // to Follower, fetches, and applies all entries. Verify every
    // ALIVE node (3 originals minus killed leader = 4 nodes) has
    // applied the full prefix.
    cluster
        .await_applied_at_least(N_ENTRIES_TOTAL, Duration::from_secs(30))
        .await
        .unwrap_or_else(|max| {
            let per_node: Vec<(u64, usize)> = cluster
                .nodes
                .iter()
                .filter(|n| n.is_alive())
                .map(|n| (n.node_id.0, n.recording.len()))
                .collect();
            panic!(
                "minority did not catch up to {N_ENTRIES_TOTAL} after kill+heal; \
                 max applied = {max}; per-alive-node = {per_node:?}"
            );
        });

    // Strong consistency: every alive node's applied prefix matches
    // the new leader's byte-for-byte (Raft log-matching property).
    let baseline_node = cluster
        .node(new_leader_term.0)
        .expect("new leader id must exist");
    let baseline = baseline_node.recording.applied();
    for node in cluster.nodes.iter().filter(|n| n.is_alive()) {
        let applied = node.recording.applied();
        assert!(
            applied.len() >= N_ENTRIES_TOTAL,
            "node {} only applied {} of {N_ENTRIES_TOTAL}",
            node.node_id.0,
            applied.len()
        );
        for i in 0..N_ENTRIES_TOTAL {
            assert_eq!(
                applied[i].1, baseline[i].1,
                "node {} entry #{i} diverges from leader {}",
                node.node_id.0, new_leader_term.0
            );
        }
    }

    // Pump is owned by the cluster (in self.tick_pump) and aborted
    // by `shutdown()` — no manual abort needed here.
    cluster.shutdown().await;
}

/// Poll [`SimulatedCluster::await_leader`] until the elected leader's
/// term is strictly greater than `prev_term`. Bails after `deadline`.
async fn await_new_leader_at_higher_term(
    cluster: &SimulatedCluster,
    prev_term: u64,
    deadline: Duration,
) -> std::result::Result<(NodeId, u64), String> {
    let start = std::time::Instant::now();
    loop {
        if let Ok((nid, term)) = cluster.await_leader(Duration::from_millis(500)).await
            && term > prev_term
        {
            return Ok((nid, term));
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "no leader at term > {prev_term} within {deadline:?}"
            ));
        }
    }
}

/// Propose `payload` against the current leader, retrying on
/// transient `NotLeader` errors. Bounded to 10 attempts.
async fn propose_with_retry(cluster: &SimulatedCluster, payload: Bytes, entry_idx: usize) {
    let mut tries = 0u8;
    loop {
        tries += 1;
        match cluster.propose(payload.clone()).await {
            Ok(_) => return,
            Err(xraft_core::error::XRaftError::NotLeader { .. }) if tries < 10 => {
                cluster
                    .await_leader(Duration::from_secs(10))
                    .await
                    .expect("a leader must remain or re-elect during proposes");
            }
            Err(e) => panic!("propose #{entry_idx} failed after {tries} tries: {e}"),
        }
    }
}

/// Primary scenario: minority strands in `PreCandidate`, partition
/// heals, and the engine's same-term `leader_hint` step-down
/// (Open Question `engine-pre-vote-recovery` →
/// `yes-add-leader-hint-step-down`) brings the minority back to
/// `Follower` WITHOUT requiring a leader kill.
///
/// Differences vs the higher-term variant above:
/// * No leader is killed; cluster continues running with the
///   original term throughout.
/// * Recovery completes purely on the engine's same-term
///   `leader_hint` step-down — minority `PreCandidate` receives
///   same-term denials carrying `leader_hint = Some(orig_leader)`,
///   recognises the cluster has a live leader, and steps down.
/// * Shorter run (10 entries during partition) keeps the test
///   under ~6 s wall clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_minority_recovers_same_term_step_down() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = SimulatedClusterConfig::five_node(0xC0FF_EE05);
    let election_max = Duration::from_millis(cfg.election_max_ms);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    // Detach + install manual fast pump. Handle stored on the
    // cluster; aborted by `shutdown()`.
    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    let (orig_leader, orig_term) = cluster
        .await_leader(Duration::from_secs(10))
        .await
        .expect("leader must be elected");

    // Pre-partition baseline.
    const BASELINE: usize = 5;
    const DURING: usize = 10;
    const TOTAL: usize = BASELINE + DURING;
    for i in 0..BASELINE {
        let payload = Bytes::copy_from_slice(&(i as u64).to_be_bytes());
        propose_with_retry(&cluster, payload, i).await;
    }
    cluster
        .await_applied_at_least(BASELINE, Duration::from_secs(10))
        .await
        .expect("every node must apply the pre-partition baseline");

    // Partition: minority = 2, majority = 3 (incl. leader).
    let mut minority: Vec<NodeId> = vec![NodeId(4), NodeId(5)];
    if minority.contains(&orig_leader) {
        minority = vec![NodeId(1), NodeId(2)];
    }
    let majority: Vec<NodeId> = (1..=5u64)
        .map(NodeId)
        .filter(|n| !minority.contains(n))
        .collect();
    assert!(majority.contains(&orig_leader));
    cluster.partition_group(&minority);

    // Commit during-partition entries on the majority.
    for i in 0..DURING {
        let idx = BASELINE + i;
        let payload = Bytes::copy_from_slice(&(idx as u64).to_be_bytes());
        propose_with_retry(&cluster, payload, idx).await;
    }
    for nid in &majority {
        cluster
            .await_node_applied_at_least(*nid, TOTAL, Duration::from_secs(10))
            .await
            .unwrap_or_else(|got| {
                panic!(
                    "majority node {} should have applied {TOTAL} before heal; got {got}",
                    nid.0
                )
            });
    }

    // Detach the manual pump for the strand-inject burst, so
    // simulated time advances by a precise number of triggers
    // rather than racing the background pump. The harness asserts
    // `tick_pump.is_none()` inside `advance_simulated_time` to make
    // this structural.
    cluster.detach_tick_pump().await;
    cluster.advance_simulated_time(election_max * 3).await;

    // Diagnostic: confirm at least one minority node is stranded
    // in PreCandidate (the other may be in Candidate if it raced
    // past Pre-Vote).
    let mut saw_pre_candidate = false;
    for nid in &minority {
        let node = cluster.node(*nid).expect("minority id must exist");
        if let Some(snap) = node.status.status().await
            && matches!(snap.role, NodeRole::PreCandidate | NodeRole::Candidate)
        {
            saw_pre_candidate = true;
        }
    }
    assert!(
        saw_pre_candidate,
        "test invariant: at least one minority node should be in PreCandidate/Candidate \
         at heal time, else the same-term step-down path is dead"
    );

    // Restart the manual pump for the recovery phase.
    cluster.start_manual_pump(4);

    // Heal. With the engine's same-term `leader_hint` step-down,
    // the minority's next PreVote round produces same-term "no,
    // here is the leader" replies carrying
    // `leader_hint = Some(orig_leader)`. The engine then steps down
    // to Follower → fetches → catches up. No leader kill needed.
    cluster.heal_all();

    if let Err(max) = cluster
        .await_applied_at_least(TOTAL, Duration::from_secs(30))
        .await
    {
        let per_node: Vec<(u64, usize, Option<NodeRole>)> =
            futures_per_node_snapshot(&cluster).await;
        panic!(
            "minority did not catch up to {TOTAL} after heal (engine same-term step-down); \
             max applied = {max}; per-node = {per_node:?}"
        );
    }

    // Strong-consistency check: leader's applied prefix matches
    // every alive node's. The term should not have advanced
    // (no leader kill, no election triggered).
    let (final_leader, final_term) = cluster
        .await_leader(Duration::from_secs(5))
        .await
        .expect("a leader must remain after heal");
    assert_eq!(
        final_leader, orig_leader,
        "no leader change expected on same-term step-down recovery"
    );
    assert_eq!(
        final_term, orig_term,
        "term must not advance on same-term step-down recovery"
    );

    let baseline_node = cluster
        .node(final_leader)
        .expect("final leader id must exist");
    let baseline = baseline_node.recording.applied();
    for node in cluster.nodes.iter().filter(|n| n.is_alive()) {
        let applied = node.recording.applied();
        assert!(
            applied.len() >= TOTAL,
            "node {} only applied {} of {TOTAL}",
            node.node_id.0,
            applied.len()
        );
        for i in 0..TOTAL {
            assert_eq!(
                applied[i].1, baseline[i].1,
                "node {} entry #{i} diverges from leader {}",
                node.node_id.0, final_leader
            );
        }
    }

    // Pump is owned by the cluster; aborted by `shutdown()`.
    cluster.shutdown().await;
}

/// Snapshot every alive node's `(node_id, applied_count, role)` for
/// use in panic-path diagnostics.
///
/// This helper is `async` because [`TestObserverHandle::status`] is
/// `async` (it locks a [`tokio::sync::Mutex`] guarding the latest
/// [`xraft_server::NodeStatus`]); awaiting each node's published
/// status and mapping `s.as_ref().map(|s| s.role)` into the result
/// tuple lets `partition_recovery` flake messages report WHICH
/// nodes are stuck in `PreCandidate` / `Candidate` instead of an
/// uninformative `None`.
async fn futures_per_node_snapshot(
    cluster: &SimulatedCluster,
) -> Vec<(u64, usize, Option<NodeRole>)> {
    let mut out = Vec::with_capacity(cluster.nodes.len());
    for n in cluster.nodes.iter().filter(|n| n.is_alive()) {
        let role = n.status.status().await.map(|s| s.role);
        out.push((n.node_id.0, n.recording.len(), role));
    }
    out
}
