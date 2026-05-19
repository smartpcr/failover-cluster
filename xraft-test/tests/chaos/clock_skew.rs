//! Stage 8.2 chaos scenarios that exercise **per-node election-timer
//! skew** — the engine's analogue to differential wall-clock drift
//! across nodes.
//!
//! # Why election-window skew, not literal clock-rate skew
//!
//! The Raft engine in this workspace is driven by ONE shared
//! [`SimulatedClock`](xraft_test::SimulatedClock) advanced by one
//! [`ManualTickController`](xraft_test::ManualTickController): every
//! node observes the same `tick_quantum` and the same simulated
//! `now()`. Modelling per-node wall-clock drift faithfully (e.g. by
//! per-node tick rates that drop or duplicate ticks) is a major
//! infrastructure change that would also affect heartbeat intervals,
//! fetch cadence, RPC timeouts, etc. — out of scope for Stage 8.2.
//!
//! Within the engine, **the only liveness decision driven by a
//! per-node timer is the election timeout**: every other timer
//! (heartbeat, fetch) is bounded by the cluster-wide
//! `fetch_interval_ms`. So a per-node election window is the
//! tightest faithful model of "node X's clock runs at a different
//! rate than node Y's" for the purposes of Raft safety:
//!
//! * a faster-clock node's election timer fires SOONER →
//!   that node bids for leadership more aggressively;
//! * a slower-clock node's election timer fires LATER → that node
//!   is a more passive follower.
//!
//! Safety properties we assert under skew:
//!
//! 1. **No split-brain.** Even with one node aggressively
//!    election-bidding, at most ONE leader exists per term (Raft
//!    Election Safety).
//! 2. **Log Matching survives skew.** Every committed
//!    `(LogIndex, payload)` replicates to every alive node — the
//!    strict pairwise verifier
//!    [`verify_committed_entries_strict`](crate::common::cluster_harness::verify_committed_entries_strict)
//!    catches any divergence.
//! 3. **Liveness preserved.** The cluster still elects a leader and
//!    commits entries despite the differential election rates.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use xraft_core::types::{LogIndex, NodeId};
use xraft_test::{ElectionWindow, SimulatedCluster, SimulatedClusterConfig};

use crate::common::cluster_harness::{
    node_status_snapshot, propose_with_retry, verify_committed_entries_strict,
};

// ---------------------------------------------------------------------------
// moderate election-window skew (3:1 ratio)
// ---------------------------------------------------------------------------

/// 5-node cluster where TWO nodes have a 3× shorter election window
/// than the rest. The cluster must:
///
/// * elect exactly one leader (no split-brain across terms); AND
/// * commit ≥ 50 entries; AND
/// * pass the strict pairwise Log-Matching check.
///
/// The aggressive-clock pair (nodes 1 and 2 at 150-250 ms vs. the
/// default 500-1000 ms) is most likely to win the first election by
/// virtue of firing earlier — but Raft's term + vote semantics MUST
/// still serialise the outcome to a single leader per term.
///
/// Wall-clock budget: ~10-15 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moderate_election_skew_three_to_one_ratio() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut overrides: BTreeMap<NodeId, ElectionWindow> = BTreeMap::new();
    overrides.insert(NodeId(1), ElectionWindow::new(150, 250));
    overrides.insert(NodeId(2), ElectionWindow::new(150, 250));

    let cfg = SimulatedClusterConfig {
        size: 5,
        seed: 0xC0FF_EE70,
        tick_ms: 5,
        // Default-clock nodes (3, 4, 5) keep the chaos-tuned window.
        election_min_ms: 500,
        election_max_ms: 1000,
        fetch_ms: 10,
        per_node_election_overrides: overrides,
        use_durable_storage: false,
    };
    let cluster = SimulatedCluster::start(cfg)
        .await
        .expect("skew cluster must start");
    let (leader, term) = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader must emerge despite election-window skew");
    eprintln!("moderate skew: first leader = {} at term {term}", leader.0);

    // Commit a meaningful prefix so the strict verifier has something
    // to check beyond "did anyone get elected".
    let mut committed: Vec<(LogIndex, Vec<u8>)> = Vec::with_capacity(50);
    for i in 0..50u64 {
        let payload_bytes = i.to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);
        let idx = propose_with_retry(&cluster, payload, 5, Duration::from_secs(2))
            .await
            .unwrap_or_else(|e| panic!("propose #{i} under election skew failed: {e}"));
        committed.push((idx, payload_bytes.to_vec()));
    }

    // STRICT pairwise check: every alive node agrees on every
    // committed LogIndex's bytes. No split-brain → no divergence.
    let recovery_deadline = Duration::from_secs(20);
    if let Err(msg) = verify_committed_entries_strict(&cluster, &committed, recovery_deadline).await
    {
        let snap = node_status_snapshot(&cluster).await;
        panic!(
            "{msg}\n  first leader = {} at term {term}\n  per-node = {snap:?}",
            leader.0,
        );
    }

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// extreme election-window skew (10:1 ratio)
// ---------------------------------------------------------------------------

/// Extreme skew: ONE node has a 10× shorter election window than the
/// other four. That single fast-clock node may repeatedly trigger
/// elections that fail to gather a quorum (because the slower nodes
/// haven't timed out yet and may still have a valid leader to back),
/// but no committed entry may be lost AND no split-brain may occur.
///
/// This stresses two engine paths simultaneously:
///
/// 1. The PreVote safety check: the fast-clock node's repeated
///    `PreVote` rounds must not promote it to Candidate while the
///    cluster has a healthy leader (otherwise term inflation would
///    force unnecessary leader changes).
/// 2. The leader-step-down quorum check: once a leader IS elected,
///    its check-quorum timer must keep it as leader even when the
///    fast-clock node is constantly bidding.
///
/// Wall-clock budget: ~15-20 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extreme_election_skew_ten_to_one_ratio() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut overrides: BTreeMap<NodeId, ElectionWindow> = BTreeMap::new();
    // Single fast-clock node with very short window. The other 4
    // nodes use the slow default 1000-1500 ms.
    overrides.insert(NodeId(3), ElectionWindow::new(100, 150));

    let cfg = SimulatedClusterConfig {
        size: 5,
        seed: 0xC0FF_EE71,
        tick_ms: 5,
        election_min_ms: 1000,
        election_max_ms: 1500,
        fetch_ms: 10,
        per_node_election_overrides: overrides,
        use_durable_storage: false,
    };
    let cluster = SimulatedCluster::start(cfg)
        .await
        .expect("extreme-skew cluster must start");

    let (leader, term) = cluster
        .await_leader(Duration::from_secs(25))
        .await
        .expect("a leader must emerge despite extreme election skew");
    eprintln!(
        "extreme skew: first leader = {} at term {term} (fast-clock node = 3)",
        leader.0
    );

    // Smaller propose count: extreme skew can produce occasional
    // election-induced step-downs that slow propose throughput. The
    // safety check is what matters.
    let mut committed: Vec<(LogIndex, Vec<u8>)> = Vec::with_capacity(30);
    for i in 0..30u64 {
        let payload_bytes = i.to_be_bytes();
        let payload = Bytes::copy_from_slice(&payload_bytes);
        let idx = propose_with_retry(&cluster, payload, 8, Duration::from_secs(3))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "propose #{i} under extreme election skew failed: {e}\n  \
                     (the cluster may be stuck in repeated PreVote storms — \
                     check engine PreVote handling)"
                )
            });
        committed.push((idx, payload_bytes.to_vec()));
    }

    // Verify: pairwise Log-Matching AND every committed LogIndex
    // present on every alive node. Split-brain or divergent commits
    // under skew would fail here.
    let recovery_deadline = Duration::from_secs(45);
    if let Err(msg) = verify_committed_entries_strict(&cluster, &committed, recovery_deadline).await
    {
        let snap = node_status_snapshot(&cluster).await;
        panic!(
            "{msg}\n  first leader = {} at term {term} (fast-clock node = 3)\n  \
             per-node = {snap:?}",
            leader.0,
        );
    }

    // No split-brain assertion: the final term may have advanced
    // (the fast-clock node may have triggered an election that
    // succeeded mid-run), but exactly ONE node may report leader
    // role at the final term.
    let final_statuses = cluster.statuses().await;
    let mut final_term = term;
    let mut leader_count = 0;
    for (_id, snap) in &final_statuses {
        if let Some(s) = snap
            && s.term > final_term
        {
            final_term = s.term;
        }
    }
    for (id, snap) in &final_statuses {
        if let Some(s) = snap
            && s.term == final_term
            && s.role == xraft_core::types::NodeRole::Leader
        {
            leader_count += 1;
            eprintln!(
                "extreme skew final: node {} is Leader at term {final_term}",
                id.0
            );
        }
    }
    assert!(
        leader_count <= 1,
        "SAFETY: more than one Leader at term {final_term} (split-brain)"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// election-window override is per-node (sanity / unit-level)
// ---------------------------------------------------------------------------

/// Sanity: the override map IS honoured by `SimulatedCluster::start`.
/// Construct a 3-node cluster with NodeId(2) given a far-shorter
/// window than nodes 1 and 3. NodeId(2) must win the first leader
/// election (or very nearly always — over 5 seeds we'd accept ≥3 wins).
///
/// We just run with a single seed and assert NodeId(2) won — if the
/// override is silently ignored, NodeId(2) would win 1/3 of the time
/// at random, and a single seed gives us a 67% miss rate. Cheap
/// signal that catches "I forgot to plumb the override through".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_override_short_window_node_wins_first_election() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut overrides: BTreeMap<NodeId, ElectionWindow> = BTreeMap::new();
    overrides.insert(NodeId(2), ElectionWindow::new(80, 120));

    let cfg = SimulatedClusterConfig {
        size: 3,
        seed: 0xC0FF_EE72,
        tick_ms: 5,
        // Other nodes very slow — gives node 2 a 6-8× head start.
        election_min_ms: 800,
        election_max_ms: 1200,
        fetch_ms: 10,
        per_node_election_overrides: overrides,
        use_durable_storage: false,
    };
    let cluster = SimulatedCluster::start(cfg)
        .await
        .expect("override-honour cluster must start");

    let (leader, term) = cluster
        .await_leader(Duration::from_secs(10))
        .await
        .expect("a leader must emerge");
    assert_eq!(
        leader,
        NodeId(2),
        "node 2's 80-120ms override window should fire BEFORE node 1/3's \
         800-1200ms window; if leader is {} instead the override was not honoured",
        leader.0
    );
    eprintln!("override-honour: node 2 won first election at term {term} as expected");

    cluster.shutdown().await;
}
