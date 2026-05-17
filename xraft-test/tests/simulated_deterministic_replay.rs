//! Stage 8.2 scenario: deterministic-replay.
//!
//! Brief: "Given a chaos test run with seed=42, When replayed with
//! the same seed, Then the exact same sequence of events and
//! outcomes occurs."
//!
//! # Determinism contract
//!
//! Two same-seed chaos runs against fresh clusters of the same
//! shape MUST produce:
//!
//! 1. **Byte-identical fault histories** — the chaos engine's RNG
//!    is seeded; every roll-arm sorts candidate sets before
//!    indexing so HashMap iteration order cannot leak in.
//! 2. **Identical ordered sequence of committed payloads on the
//!    canonical leader.** This is the "exact same outcomes"
//!    invariant the brief asks for: each run picks the alive
//!    leader at settle-time as the canonical node and dumps its
//!    `applied()` vector — `Vec<(LogIndex, Bytes)>`. Both runs
//!    must produce a byte-identical vector. This is stronger
//!    than the iter-2 `BTreeSet` set-equality check which
//!    silently allowed two runs to reorder commits — see Stage
//!    8.2 evaluator iter-2 item 6.
//!
//! # Single-threaded scheduler is REQUIRED
//!
//! Strict ordered-equality across runs requires a deterministic
//! tokio scheduler. We pin this test to
//! `flavor = "current_thread"` so every cooperative yield resumes
//! in the SAME order on each run, and we serialise proposals
//! (one at a time, `await`ed before the next is issued) so the
//! engine's mpsc inbound order is identical across runs.
//! Multi-threaded runtimes would let tasks on different worker
//! threads commit proposals in different log indices on different
//! runs, which would defeat the byte-identical-sequence claim.

use std::time::Duration;

use bytes::Bytes;
use xraft_test::{ChaosConfig, ChaosEngine, SimulatedCluster, SimulatedClusterConfig};

const SEED: u64 = 42;
const CLUSTER_SEED: u64 = 0x000D_EADB_EEF5;
const CHAOS_STEPS: usize = 12;
const PROPOSALS_PER_STEP: usize = 4;

#[tokio::test(flavor = "current_thread")]
async fn chaos_history_is_seed_deterministic() {
    let _ = tracing_subscriber::fmt::try_init();

    let run_a = run_chaos_once(SEED).await;
    let run_b = run_chaos_once(SEED).await;

    // 1) Byte-identical fault histories.
    assert_eq!(
        run_a.fault_history.len(),
        run_b.fault_history.len(),
        "same-seed runs must produce same-length fault histories; got {} vs {}",
        run_a.fault_history.len(),
        run_b.fault_history.len()
    );
    for (i, ((_, fa), (_, fb))) in run_a
        .fault_history
        .iter()
        .zip(run_b.fault_history.iter())
        .enumerate()
    {
        assert_eq!(
            fa, fb,
            "fault #{i} diverged across same-seed runs: {fa:?} vs {fb:?}"
        );
    }

    // 2) Strict ordered-sequence equality on the canonical applied
    //    log. With a single-threaded tokio runtime + manual
    //    tick-pump + serialised proposals, the engine's commit
    //    order is fully deterministic — so every (index, payload)
    //    pair must match across runs.
    assert_eq!(
        run_a.canonical_applied.len(),
        run_b.canonical_applied.len(),
        "same-seed runs must apply the same number of entries on the canonical node; got {} vs {}",
        run_a.canonical_applied.len(),
        run_b.canonical_applied.len()
    );
    for (i, ((idx_a, payload_a), (idx_b, payload_b))) in run_a
        .canonical_applied
        .iter()
        .zip(run_b.canonical_applied.iter())
        .enumerate()
    {
        assert_eq!(
            idx_a, idx_b,
            "applied #{i} index diverged across same-seed runs: {idx_a} vs {idx_b}"
        );
        assert_eq!(
            payload_a, payload_b,
            "applied #{i} (index {idx_a}) payload diverged across same-seed runs"
        );
    }

    // 3) Sanity: at least one proposal must have committed —
    //    otherwise the equality checks above are vacuously true.
    assert!(
        !run_a.canonical_applied.is_empty(),
        "deterministic-replay run committed 0 entries — equality checks would be vacuous"
    );
}

struct RunOutcome {
    fault_history: Vec<(Duration, xraft_test::ChaosFault)>,
    /// Ordered `(LogIndex.0, payload)` sequence applied at the
    /// canonical leader node at the time of settle.
    canonical_applied: Vec<(u64, Vec<u8>)>,
}

async fn run_chaos_once(seed: u64) -> RunOutcome {
    let cfg = SimulatedClusterConfig::five_node(CLUSTER_SEED);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("leader must be elected");

    let mut engine = ChaosEngine::new(
        cluster.network.clone(),
        cluster.len(),
        ChaosConfig::with_seed(seed),
    );

    // Interleave chaos steps with SERIALISED proposals so the
    // engine's inbound mpsc receives proposals in a deterministic
    // order across runs.
    for i in 0..CHAOS_STEPS {
        engine.step(&mut cluster);
        for j in 0..PROPOSALS_PER_STEP {
            let payload = Bytes::from(format!("seed-{seed:#x}-step-{i:02}-op-{j:02}").into_bytes());
            let isolated = engine.isolated_set();
            let _ = cluster
                .propose_via_reachable_leader(&isolated, payload)
                .await;
        }
    }

    let fault_history = engine.history().to_vec();
    engine.settle();

    // Drain in-flight commits. Pump is running so simulated time
    // keeps advancing during this wait.
    tokio::time::sleep(Duration::from_millis(800)).await;
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("cluster must converge on a leader after settle");

    // Canonical node = the node with the largest applied length
    // at settle time. With a deterministic scheduler this is the
    // same node across runs (typically the most-recent leader).
    let canonical_idx = cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_alive())
        .max_by_key(|(_, n)| n.recording.len())
        .map(|(i, _)| i)
        .expect("at least one alive node");
    let canonical_applied: Vec<(u64, Vec<u8>)> = cluster.nodes[canonical_idx].recording.applied();

    cluster.shutdown().await;
    RunOutcome {
        fault_history,
        canonical_applied,
    }
}
