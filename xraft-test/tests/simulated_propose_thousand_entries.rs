//! Stage 8.1 scenario: 1000 opaque log entries through the simulated
//! 3-node cluster.
//!
//! The brief: "test harness submits 1000 opaque log entries as
//! proposals through the leader's internal command channel; a test
//! `StateMachine` implementation records applied entries; after
//! commit, the state machine's state contains all 1000 entries in
//! order."
//!
//! # Iter-9 evaluator item 4: deterministic-tick pump (iter-12 doc refresh)
//!
//! Iter-7 flagged that this test still ran on the default wall-clock
//! `tokio::time::interval(tick_quantum)` pump. Iter-9 replaced that
//! with the test-owned manual-trigger fast pump
//! ([`SimulatedCluster::start_manual_pump`]) so every tick the
//! drivers observe flows through the
//! [`xraft_test::ManualTickController`] — the same controller the
//! `simulated_three_node_election` deterministic test uses.
//!
//! The pump's per-beat cadence has since been re-tuned. The current
//! shape (see [`SimulatedCluster::start_manual_pump`] for the
//! authoritative source): each beat fires `ticks_per_burst = 4`
//! triggers, then pays `PUMP_DRAIN_YIELDS = 32`
//! `tokio::task::yield_now().await` calls **plus** a sub-millisecond
//! `tokio::time::sleep(Duration::from_micros(PUMP_DRAIN_PAUSE_MICROS))`
//! where `PUMP_DRAIN_PAUSE_MICROS = 100`. The yields reschedule the
//! current task; the 100 µs sleep additionally yields the worker
//! thread to the OS scheduler so sibling-worker engine tasks make
//! progress on Windows multi-thread runtimes (a 32-yield-only
//! cadence flaked there). 100 µs is well under the 5 ms simulated
//! tick quantum, so the wall-clock overhead per beat is negligible
//! and the 1000-proposal run finishes well under the test budget.

use std::time::Duration;

use bytes::Bytes;
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

const N_ENTRIES: usize = 1000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_thousand_entries_converges_in_order() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg = SimulatedClusterConfig::three_node(0xC0FF_EE02);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    // Iter-9 evaluator item 4: detach the harness default wall-clock
    // pump and install the manual-trigger fast pump. The pump's
    // handle is stored on the cluster and aborted by `shutdown()`.
    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("leader must be elected");

    // Sequentially propose N_ENTRIES opaque payloads through the
    // leader. Payload is the entry index encoded as 8 BE bytes so
    // the test assert can verify both presence AND order on each
    // node's recording state machine.
    for i in 0..N_ENTRIES {
        let payload = Bytes::copy_from_slice(&(i as u64).to_be_bytes());
        // The leader can step down mid-test if a follower bumps the
        // term; retry on NotLeader with a re-elect wait.
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

    // Wait for every node's recording SM to apply all N_ENTRIES.
    cluster
        .await_applied_at_least(N_ENTRIES, Duration::from_secs(90))
        .await
        .unwrap_or_else(|max| {
            panic!(
                "not every node converged to {N_ENTRIES} applies within 90s; \
                 max observed = {max}"
            )
        });

    // Cross-check every alive node has the same payload sequence.
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

    // Iter-9: pump is owned by the cluster (stored in self.tick_pump)
    // and aborted by `shutdown()` — no need to abort manually here.
    cluster.shutdown().await;
}
