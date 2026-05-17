//! Stage 8.1 scenario: real-network-3-node-replication.
//!
//! Given a 3-node cluster using real gRPC transport on localhost,
//! when 100 opaque log entries are proposed and committed, then all
//! 3 nodes' test `StateMachine` instances contain the same 100
//! entries in order.

use std::time::Duration;

use bytes::Bytes;
use xraft_test::{RealCluster, RealClusterConfig};

const N_ENTRIES: usize = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_node_real_grpc_replicates_one_hundred_entries() {
    let _ = tracing_subscriber::fmt::try_init();

    let cluster = RealCluster::start(RealClusterConfig::three_node())
        .await
        .expect("real cluster start must succeed");

    cluster
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

    cluster
        .await_applied_at_least(N_ENTRIES, Duration::from_secs(120))
        .await
        .unwrap_or_else(|max| {
            panic!(
                "not every real-network node converged to {N_ENTRIES} applies within 120s; \
                 max observed = {max}"
            )
        });

    let baseline = cluster.nodes[0].recording.applied();
    assert_eq!(baseline.len(), N_ENTRIES, "node 1 missing entries");
    for (i, (_, bytes)) in baseline.iter().enumerate() {
        assert_eq!(
            bytes.as_slice(),
            (i as u64).to_be_bytes().as_slice(),
            "node 1 entry #{i} payload mismatch"
        );
    }
    for node in &cluster.nodes[1..] {
        let applied = node.recording.applied();
        assert_eq!(
            applied.len(),
            N_ENTRIES,
            "node {} missing entries (got {})",
            node.node_id.0,
            applied.len()
        );
        assert_eq!(
            applied, baseline,
            "node {} log diverges from node 1",
            node.node_id.0
        );
    }

    cluster.shutdown().await;
}
