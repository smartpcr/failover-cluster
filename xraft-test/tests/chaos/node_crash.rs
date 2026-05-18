//! Stage 8.2 chaos scenarios that exercise **fail-stop** node
//! crashes (irreversible kills via
//! [`SimulatedCluster::kill`](xraft_test::SimulatedCluster::kill)).
//!
//! The brief-named companion file `node_failure.rs` covers
//! partition-based scenarios (rapid leader churn, simultaneous
//! election) where every "killed" node eventually rejoins. This
//! file is specifically about PERMANENT node loss: once we call
//! `kill`, the node is gone for the rest of the test and the
//! cluster must keep committing on the surviving voters.
//!
//! Why two files cover similar territory:
//!
//! * `node_failure.rs` — transient unavailability (partition →
//!   re-elect → heal → rejoin). Exercises the check-quorum
//!   step-down path and the engine's recovery after a leader's
//!   followers come back.
//! * `node_crash.rs` (this file) — permanent unavailability. Exercises
//!   the quorum-shrink commit-index recomputation and the engine's
//!   continued progress under a smaller-but-still-quorate voter set.
//!
//! Both shapes are required by the brief's "random node
//! kill/restart" call-out — kill exercises the fail-stop half;
//! `node_failure.rs::rapid_leader_churn_recovery` exercises the
//! restart-equivalent (partition-then-heal) half.

use std::time::Duration;

use bytes::Bytes;
use xraft_core::types::{LogIndex, NodeId};

use crate::common::cluster_harness::{
    chaos_cluster_config, node_status_snapshot, propose_with_retry, start_chaos_cluster,
    verify_committed_entries_strict,
};

// ---------------------------------------------------------------------------
// kill_leader_new_leader_has_all_committed_entries
// ---------------------------------------------------------------------------

/// Brief: "killing the leader triggers re-election and the new
/// leader has all committed entries". This is the chaos-side
/// companion to the Stage 8.1 integration test; what we add here
/// over the integration variant is the **strict** post-kill
/// pairwise verifier — every alive node, not just the new leader,
/// must agree on every committed `(LogIndex, payload)`.
///
/// Wall-clock budget: ~10-15 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_leader_new_leader_has_all_committed_entries() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = chaos_cluster_config(5, 0xC0FF_EE60);
    let (mut cluster, init_leader, init_term) = start_chaos_cluster(cfg).await;

    // Pre-kill baseline: 30 committed entries on the original
    // leader. The new leader MUST contain every one of these.
    const BASELINE: usize = 30;
    let mut committed: Vec<(LogIndex, Vec<u8>)> = Vec::with_capacity(BASELINE);
    for i in 0..BASELINE {
        let payload_bytes = (i as u64).to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);
        let idx = propose_with_retry(&cluster, payload, 5, Duration::from_secs(2))
            .await
            .unwrap_or_else(|e| panic!("baseline propose #{i} failed: {e}"));
        committed.push((idx, payload_bytes.to_vec()));
    }
    cluster
        .await_applied_at_least(BASELINE, Duration::from_secs(15))
        .await
        .unwrap_or_else(|max| panic!("baseline replication failed; max observed applied = {max}"));

    // Kill the LEADER. The brief's intent is "the new leader has all
    // committed entries" — that is a Leader Completeness check, the
    // single most important consensus invariant under leadership
    // change.
    cluster.kill(init_leader);

    // Wait for a NEW leader at a STRICTLY higher term. If
    // `await_leader` returned init_leader / init_term we'd be
    // observing a stale snapshot from before the kill propagated.
    let new_leader_deadline = Duration::from_secs(15);
    let start = std::time::Instant::now();
    let (new_leader, new_term) = loop {
        let (leader, term) = cluster
            .await_leader(Duration::from_secs(5))
            .await
            .unwrap_or_else(|e| panic!("no new leader after killing {}: {e}", init_leader.0));
        if leader != init_leader && term > init_term {
            break (leader, term);
        }
        if start.elapsed() > new_leader_deadline {
            panic!(
                "deadline expired waiting for new leader != {} with term > {init_term}: \
                 observed leader = {}, term = {term}",
                init_leader.0, leader.0
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_ne!(new_leader, init_leader, "new leader must differ");
    assert!(new_term > init_term, "new term must advance");

    // Post-kill: issue more proposals so the new leader's commit
    // pipeline is exercised AND so the strict verifier has a
    // post-leadership-change committed prefix to check.
    const POST: usize = 20;
    for i in 0..POST {
        let payload_bytes = ((BASELINE + i) as u64).to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);
        let idx = propose_with_retry(&cluster, payload, 5, Duration::from_secs(2))
            .await
            .unwrap_or_else(|e| panic!("post-kill propose #{i} failed: {e}"));
        committed.push((idx, payload_bytes.to_vec()));
    }

    // STRICT pairwise check: every alive node, for every committed
    // LogIndex, has the same bytes AND the leader-acked committed
    // set is fully replicated on every alive node. The new leader
    // (post-kill) MUST contain every pre-kill baseline entry — if
    // it didn't, this would fail with "committed LogIndex N is
    // MISSING from node K's recording".
    let recovery_deadline = Duration::from_secs(45);
    if let Err(msg) = verify_committed_entries_strict(&cluster, &committed, recovery_deadline).await
    {
        let snap = node_status_snapshot(&cluster).await;
        panic!(
            "{msg}\n  killed_leader = {}, new_leader = {}, new_term = {new_term}\n  \
             baseline = {BASELINE}, post = {POST}, total committed = {}\n  per-node = {snap:?}",
            init_leader.0,
            new_leader.0,
            committed.len()
        );
    }

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// kill_two_followers_quorum_keeps_committing
// ---------------------------------------------------------------------------

/// Kill TWO followers in a 5-node cluster (quorum = 3, so 3
/// survivors continue meeting quorum). The cluster must keep
/// committing entries on the surviving 3-node majority WITHOUT
/// any leader change. This exercises the engine's quorum-shrink
/// commit-index recomputation path — when a follower disappears
/// the leader's matchIndex bookkeeping must NOT block commits on
/// the surviving voters.
///
/// Why TWO kills, not one: with one kill (4 survivors, quorum 3)
/// the cluster has slack — the leader can lose a single ack and
/// still commit. With TWO kills (3 survivors, quorum 3) the
/// leader MUST receive every surviving follower's ack to commit,
/// which is the tightest quorum and the most stressful matchIndex
/// path.
///
/// Wall-clock budget: ~10-15 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_two_followers_quorum_keeps_committing() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = chaos_cluster_config(5, 0xC0FF_EE61);
    let (mut cluster, init_leader, init_term) = start_chaos_cluster(cfg).await;

    // Pick two follower victims: the two lowest non-leader node ids.
    // We INTENTIONALLY pick non-leaders so the test isolates the
    // quorum-shrink path from the leader-change path.
    let victims: Vec<NodeId> = (1..=5u64)
        .map(NodeId)
        .filter(|n| *n != init_leader)
        .take(2)
        .collect();
    assert_eq!(victims.len(), 2);

    // Baseline propose: with all 5 alive.
    let mut committed: Vec<(LogIndex, Vec<u8>)> = Vec::new();
    for i in 0..15usize {
        let payload_bytes = (i as u64).to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);
        let idx = propose_with_retry(&cluster, payload, 5, Duration::from_secs(2))
            .await
            .unwrap_or_else(|e| panic!("baseline propose #{i} failed: {e}"));
        committed.push((idx, payload_bytes.to_vec()));
    }

    // Kill both followers in quick succession. The cluster drops
    // from 5 voters to 3; quorum stays at 3 (Raft quorum =
    // floor(N/2)+1 with N = original voter set size — the engine
    // does NOT dynamically resize quorum for fail-stop nodes; the
    // voter set is the static one persisted at startup).
    for victim in &victims {
        cluster.kill(*victim);
    }

    // Post-kill propose: the leader must still be able to commit.
    // If the engine's matchIndex bookkeeping incorrectly required
    // an ack from the killed followers, propose would hang here
    // and the per-try timeout in propose_with_retry would surface
    // a transport error.
    let leader_after = cluster
        .await_leader(Duration::from_secs(10))
        .await
        .expect("leader must survive 2 kills (3 of 5 alive = quorum)");
    assert_eq!(
        leader_after.0, init_leader,
        "leader should not change after killing only non-leaders \
         (init_term={init_term}, new_term={})",
        leader_after.1
    );

    for i in 0..25usize {
        let payload_bytes = ((15 + i) as u64).to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);
        let idx = propose_with_retry(&cluster, payload, 5, Duration::from_secs(3))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "post-kill propose #{i} failed: {e}\n  victims = {:?}\n  leader = {}",
                    victims.iter().map(|n| n.0).collect::<Vec<_>>(),
                    init_leader.0,
                )
            });
        committed.push((idx, payload_bytes.to_vec()));
    }

    // STRICT pairwise check on the surviving 3 alive nodes.
    let recovery_deadline = Duration::from_secs(30);
    if let Err(msg) = verify_committed_entries_strict(&cluster, &committed, recovery_deadline).await
    {
        let snap = node_status_snapshot(&cluster).await;
        panic!(
            "{msg}\n  victims = {:?}, leader = {}\n  per-node = {snap:?}",
            victims.iter().map(|n| n.0).collect::<Vec<_>>(),
            init_leader.0,
        );
    }

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// kill_majority_cluster_stops_committing
// ---------------------------------------------------------------------------

/// Kill THREE of 5 nodes — the surviving 2 cannot meet quorum
/// (quorum = 3) and the cluster MUST refuse to commit. This is the
/// safety-vs-liveness boundary case Raft is designed to honour:
/// under loss of quorum the cluster trades availability for
/// consistency (no leader emerges, no commits happen, no split-
/// brain occurs).
///
/// The test asserts the negative result: propose must NOT succeed
/// after the third kill. Surfaced as a hard timeout on the propose
/// call — if propose returns Ok inside the deadline, the engine
/// has violated quorum.
///
/// Wall-clock budget: ~8 s (mostly waiting for the propose timeout
/// to confirm the no-commit invariant).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_majority_cluster_stops_committing() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = chaos_cluster_config(5, 0xC0FF_EE62);
    let (mut cluster, init_leader, _init_term) = start_chaos_cluster(cfg).await;

    // Baseline: commit one entry so we know the cluster was healthy.
    let pre_payload = Bytes::from_static(b"pre-quorum-loss");
    let _pre_idx = propose_with_retry(&cluster, pre_payload.clone(), 5, Duration::from_secs(2))
        .await
        .expect("baseline propose with 5/5 alive must succeed");

    // Kill 3 nodes — the surviving 2 cannot reach quorum (3). Pick
    // the 3 lowest non-leader ids first; if that's fewer than 3
    // (because the leader IS one of the 3 lowest), include the
    // leader. Either way the cluster drops below quorum.
    let mut victims: Vec<NodeId> = (1..=5u64)
        .map(NodeId)
        .filter(|n| *n != init_leader)
        .take(2)
        .collect();
    // Kill the leader last so the cluster has a chance to commit
    // the first 2 follower kills before losing its leader. This
    // makes the deterministic-fail mode of the test (propose times
    // out) more reliable than the alternative (kill leader first =>
    // election storm immediately).
    victims.push(init_leader);
    for v in &victims {
        cluster.kill(*v);
    }
    assert_eq!(victims.len(), 3);

    // Post-kill propose MUST NOT succeed inside this deadline. The
    // surviving 2 nodes cannot reach quorum (3 of 5), so:
    //   * no leader can be elected (no node can gather 3 votes); AND
    //   * even if a stale leader handle survives, its append cannot
    //     gather 3 acks.
    let payload = Bytes::from_static(b"post-quorum-loss");
    let propose_result =
        tokio::time::timeout(Duration::from_secs(5), cluster.propose(payload)).await;

    match propose_result {
        Err(_) => {
            // Timeout: propose hung as required (no quorum =
            // no commits). PASS.
        }
        Ok(Err(_)) => {
            // Error: propose returned an explicit error (e.g.
            // NotLeader after the killed leader's handle stale-
            // checked). Also acceptable — the contract is "no
            // commit", not "no error".
        }
        Ok(Ok(idx)) => {
            let snap = node_status_snapshot(&cluster).await;
            panic!(
                "SAFETY VIOLATION: propose returned Ok({idx:?}) after 3 of 5 \
                 nodes killed (quorum is impossible)\n  victims = {:?}\n  per-node = {snap:?}",
                victims.iter().map(|n| n.0).collect::<Vec<_>>(),
            );
        }
    }

    cluster.shutdown().await;
}
