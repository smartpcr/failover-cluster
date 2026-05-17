//! Stage 8.1 scenario: real-network-5-node-leader-failover.
//!
//! Given a 5-node cluster using real gRPC transport with 50
//! committed entries, when the leader task is cancelled via
//! `JoinHandle::abort()`, then a new leader is elected and all
//! previously committed entries are present in the new leader's log.

use std::time::Duration;

use bytes::Bytes;
use xraft_test::{RealCluster, RealClusterConfig};

const N_ENTRIES: usize = 50;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn five_node_real_grpc_leader_abort_preserves_committed_entries() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut cluster = RealCluster::start(RealClusterConfig::five_node())
        .await
        .expect("real cluster start must succeed");

    let (orig_leader, _) = cluster
        .await_leader(Duration::from_secs(40))
        .await
        .expect("leader must be elected over real gRPC");

    for i in 0..N_ENTRIES {
        let payload = Bytes::copy_from_slice(&(i as u64).to_be_bytes());
        cluster
            .propose(payload)
            .await
            .unwrap_or_else(|e| panic!("propose #{i} failed: {e}"));
    }

    if let Err(max) = cluster
        .await_applied_at_least(N_ENTRIES, Duration::from_secs(120))
        .await
    {
        let mut diag: Vec<String> = Vec::new();
        for n in cluster.nodes.iter() {
            let count = n.recording.len();
            let role_term = match n.handle.as_ref() {
                Some(h) => {
                    let s = h.status().current().await;
                    format!("{:?}@term={}, leader_id={:?}", s.role, s.term, s.leader_id)
                }
                None => "DEAD".into(),
            };
            diag.push(format!("node{}: applied={count}, {role_term}", n.node_id.0));
        }
        panic!(
            "pre-abort replication failed; max observed = {max}\n  {}",
            diag.join("\n  ")
        );
    }

    // Fail-stop the leader by aborting its task — the brief
    // explicitly calls this out: "leader task is cancelled via
    // `JoinHandle::abort()`".
    let killed = cluster
        .kill_leader()
        .await
        .expect("must have killed a leader");
    assert_eq!(
        killed, orig_leader,
        "kill_leader must target the unique leader"
    );

    // 4 of 5 voters remain — they form a quorum and elect a new
    // leader within a couple of election windows.
    let (new_leader, _) = cluster
        .await_leader(Duration::from_secs(40))
        .await
        .expect("a new leader must emerge from the surviving quorum");

    assert_ne!(
        new_leader, orig_leader,
        "the new leader must not be the aborted node"
    );

    let leader_node = cluster
        .node(new_leader)
        .expect("new leader id must match a known node");
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

    cluster.shutdown().await;
}
