//! Stage 8.2 scenario: linearisability-check.
//!
//! Brief: "Given a 5-node cluster under chaos for 30 seconds with
//! **concurrent proposals**, When the operation history is
//! validated by the linearisability checker, Then all committed
//! operations form a linearisable history."
//!
//! # Concurrency model
//!
//! Each chaos step spawns `PROPOSALS_PER_STEP` proposal tasks
//! into a `JoinSet`. Each task holds an `Arc<Vec<(NodeId,
//! DriverHandle)>>` snapshot of the cluster's nodes and tries
//! every non-isolated `DriverHandle::propose` in turn until one
//! commits or its retry budget is exhausted. Multiple proposals
//! are therefore IN FLIGHT AT THE SAME TIME at the leader — the
//! brief's "concurrent proposals" requirement.
//!
//! The shared [`HistoryRecorder`] is internally `Arc<Mutex<…>>`
//! so the spawned tasks can record invoke / complete events
//! safely. The recorder timestamps come from the cluster's
//! [`SimulatedClock`], so real-time ordering is measured in
//! simulated time (decoupled from tokio scheduling jitter).
//!
//! # What this test asserts
//!
//! After chaos settles and the cluster converges, the recorded
//! history + each node's applied state must satisfy the four
//! [`xraft_test::verify_linearisable`] invariants:
//!
//! 1. Returned indices are unique.
//! 2. Every successful op's payload appears at the returned
//!    index on every alive node.
//! 3. Real-time order: if A completed before B was invoked then
//!    `index(A) < index(B)`.
//! 4. Prefix agreement across alive nodes up through the max
//!    returned index.
//!
//! # Op semantics
//!
//! Each proposal carries a unique 16-byte payload (`op_id` in
//! 8 BE bytes + an `b"chaos-li"` tag). This makes the apply-
//! equivalence and prefix-agreement checks straightforward
//! bytewise comparisons.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::task::JoinSet;
use xraft_core::error::XRaftError;
use xraft_core::types::NodeId;
use xraft_server::DriverHandle;
use xraft_test::{
    ChaosConfig, ChaosEngine, HistoryRecorder, SimulatedCluster, SimulatedClusterConfig,
    verify_linearisable,
};

const CHAOS_STEPS: usize = 16;
const PROPOSALS_PER_STEP: usize = 4;
const PER_TASK_RETRIES: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_history_is_linearisable() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = SimulatedClusterConfig::five_node(0xC0FF_EE85);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("initial leader must be elected");

    let mut engine = ChaosEngine::new(
        cluster.network.clone(),
        cluster.len(),
        ChaosConfig::with_seed(0xCAFE_1234),
    );
    let recorder = HistoryRecorder::new();
    let mut next_op = 0u64;
    let mut joinset: JoinSet<()> = JoinSet::new();

    for _ in 0..CHAOS_STEPS {
        engine.step(&mut cluster);
        let isolated: Arc<HashSet<NodeId>> = Arc::new(engine.isolated_set());
        let handles: Arc<Vec<(NodeId, DriverHandle)>> = Arc::new(
            cluster
                .nodes
                .iter()
                .map(|n| (n.node_id, n.driver.clone()))
                .collect(),
        );

        for _ in 0..PROPOSALS_PER_STEP {
            let payload = make_payload(next_op);
            next_op += 1;
            let recorder = recorder.clone();
            let isolated = isolated.clone();
            let handles = handles.clone();
            let clock = cluster.clock.clone();

            joinset.spawn(async move {
                let invoked_at = clock.elapsed();
                let op_id = recorder.invoke(payload.clone(), invoked_at);

                let mut last_err: Option<XRaftError> = None;
                for _attempt in 0..PER_TASK_RETRIES {
                    let mut accepted = false;
                    for (nid, h) in handles.iter() {
                        if isolated.contains(nid) {
                            continue;
                        }
                        match h.propose(payload.clone()).await {
                            Ok(idx) => {
                                recorder.complete(op_id, clock.elapsed(), idx);
                                accepted = true;
                                break;
                            }
                            Err(e) => last_err = Some(e),
                        }
                    }
                    if accepted {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if let Some(ref e) = last_err {
                    recorder.complete_err(op_id, clock.elapsed(), e);
                }
            });
        }

        // Light pause so the chaos loop's next fault application
        // doesn't race the just-spawned proposals out of contention.
        // Proposals continue concurrently across step boundaries.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Drain all in-flight proposals so every op has a terminal
    // (`Ok`/`Err`) entry in the recorder before settle/converge.
    while joinset.join_next().await.is_some() {}

    // Stop chaos and converge.
    engine.settle();
    cluster
        .await_leader(Duration::from_secs(30))
        .await
        .expect("a unique leader must emerge after settle");

    // Find the max successful returned index and wait for every
    // alive node to apply at least up to it.
    let history = recorder.snapshot();
    let max_idx = history
        .iter()
        .filter_map(|r| r.returned_index)
        .max()
        .unwrap_or(0);
    if max_idx > 0 {
        let start = std::time::Instant::now();
        loop {
            let mut min_last: u64 = u64::MAX;
            for n in &cluster.nodes {
                let last = n.recording.last_applied();
                if last < min_last {
                    min_last = last;
                }
            }
            if min_last >= max_idx {
                break;
            }
            if start.elapsed() > Duration::from_secs(120) {
                let mut diag: Vec<String> = Vec::new();
                for n in &cluster.nodes {
                    diag.push(format!(
                        "node{}: last_applied={} len={}",
                        n.node_id.0,
                        n.recording.last_applied(),
                        n.recording.len()
                    ));
                }
                panic!(
                    "linearisability convergence stalled: target={max_idx} min_last={min_last}; \
                     nodes: {}",
                    diag.join(" | ")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Collect each node's applied trace + run the checker.
    let mut applied_by_node: Vec<xraft_test::AppliedByNode> = Vec::new();
    for n in &cluster.nodes {
        applied_by_node.push((n.node_id, n.recording.applied()));
    }
    if let Err(violation) = verify_linearisable(&history, &applied_by_node) {
        panic!(
            "linearisability check FAILED: {violation}\n\
             history len = {}, max_idx = {max_idx}",
            history.len()
        );
    }

    // Assert AT LEAST ONE proposal succeeded — otherwise the
    // linearisability check is vacuous.
    let any_committed = history.iter().any(|r| r.returned_index.is_some());
    assert!(
        any_committed,
        "no proposals committed under chaos — linearisability check would be vacuous"
    );

    cluster.shutdown().await;
}

fn make_payload(op_id: u64) -> Bytes {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&op_id.to_be_bytes());
    buf.extend_from_slice(b"chaos-li");
    Bytes::from(buf)
}
