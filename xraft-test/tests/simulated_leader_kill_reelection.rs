//! Stage 8.1 scenario: data-consistency-after-failover.
//!
//! Given a 3-node `SimulatedCluster` with 500 committed opaque log
//! entries applied to a test `StateMachine`, when the leader is
//! killed and a new leader elected, then the new leader's
//! `StateMachine` contains all 500 entries in order.
//!
//! # Iter-9 evaluator item 4: deterministic-tick pump
//!
//! Iter-7 flagged that this test still relied on the harness's
//! default wall-clock `tokio::time::interval(tick_quantum)` pump.
//! Iter-9 replaces it with the test-owned manual-trigger fast pump
//! ([`SimulatedCluster::start_manual_pump`]) so every tick the
//! drivers observe flows through the
//! [`xraft_test::ManualTickController`] — same controller the
//! `simulated_three_node_election` deterministic test uses.

use std::time::Duration;

use bytes::Bytes;
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

const N_ENTRIES: usize = 500;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_triggers_reelection_and_preserves_committed_entries() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = SimulatedClusterConfig::three_node(0xC0FF_EE03);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    // Iter-9 evaluator item 4: detach + install manual fast pump.
    // Handle is stored on the cluster; aborted by `shutdown()`.
    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    let (orig_leader, _) = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("leader must be elected");

    // Propose 500 opaque entries. Encode the index as the payload so
    // we can verify replication order on the survivors after kill.
    for i in 0..N_ENTRIES {
        let payload = Bytes::copy_from_slice(&(i as u64).to_be_bytes());
        let mut tries = 0u8;
        loop {
            tries += 1;
            match cluster.propose(payload.clone()).await {
                Ok(_) => break,
                Err(xraft_core::error::XRaftError::NotLeader { .. }) if tries < 5 => {
                    cluster
                        .await_leader(Duration::from_secs(5))
                        .await
                        .expect("leader must re-elect");
                }
                Err(e) => panic!("propose #{i} failed: {e}"),
            }
        }
    }

    // Confirm every alive node has applied all 500 BEFORE the kill —
    // otherwise we cannot make a guarantee about the new leader's
    // log (an entry that committed but only on the old leader's
    // disk is allowed to be lost per Raft, but the brief asserts
    // the entries WERE committed across the quorum).
    if let Err(max) = cluster
        .await_applied_at_least(N_ENTRIES, Duration::from_secs(60))
        .await
    {
        let mut diag: Vec<String> = Vec::new();
        for n in cluster.nodes.iter() {
            let count = n.recording.len();
            let role_term = match n.status.status().await {
                Some(s) => format!("{:?}@term={}, leader_id={:?}", s.role, s.term, s.leader_id),
                None => "no-status".into(),
            };
            diag.push(format!("node{}: applied={count}, {role_term}", n.node_id.0));
        }
        panic!(
            "pre-kill replication failed; max observed = {max}\n  {}",
            diag.join("\n  ")
        );
    }

    // Fail-stop the leader: aborts its driver task and removes its
    // handler from the simulated network so peers see it as dead.
    // The manual pump keeps firing; the killed driver's mpsc sender
    // returns an error on the next `trigger()` and is pruned inline.
    cluster.kill(orig_leader);

    // The remaining two nodes form a quorum (2 of 3) so they elect a
    // new leader within a couple of election windows.
    let (new_leader, new_term) = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a new leader must emerge from the surviving quorum");

    assert_ne!(
        new_leader, orig_leader,
        "the new leader must be one of the survivors, not the killed node"
    );
    let leader_node = cluster
        .node(new_leader)
        .expect("new leader id must match a known node");
    let snap = leader_node
        .status
        .status()
        .await
        .expect("new leader must have published a status");
    assert_eq!(
        snap.term, new_term,
        "leader term {new_term} disagrees with status snapshot {}",
        snap.term
    );

    // Inspect the new leader's recording SM: it MUST contain all 500
    // entries (committed BEFORE the kill on the quorum the new
    // leader is part of).
    let applied = leader_node.recording.applied();
    assert_eq!(
        applied.len(),
        N_ENTRIES,
        "new leader missing pre-failover entries: got {} expected {N_ENTRIES}",
        applied.len()
    );
    for (i, (_, bytes)) in applied.iter().enumerate() {
        assert_eq!(
            bytes.as_slice(),
            (i as u64).to_be_bytes().as_slice(),
            "new leader entry #{i} payload mismatch"
        );
    }

    // Iter-9: pump is owned by the cluster (in self.tick_pump) and
    // aborted by `shutdown()` — no manual abort needed here.
    cluster.shutdown().await;
}
