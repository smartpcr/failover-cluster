//! Stage 8.2 scenario: simultaneous-election.
//!
//! Brief: "test: simultaneous election (3 candidates start
//! elections at same term), verify exactly one wins or a new
//! election resolves the tie."
//!
//! # How we force the simultaneous case
//!
//! 1. Build a 3-node cluster with `election_min_ms == election_max_ms`
//!    so every node's election RNG produces the SAME timeout window.
//! 2. Detach the wall-clock pump so simulated time only advances
//!    via explicit triggers.
//! 3. Full-mesh partition: cut every directed edge between every
//!    pair of nodes. The current leader's heartbeats stop reaching
//!    its followers and the followers' fetches stop reaching the
//!    leader.
//! 4. Advance simulated time past `2 * election_max` so every node
//!    transitions Follower → PreCandidate (per the engine's
//!    `Follower election timeout → PreCandidate` path) and then
//!    Candidate. Because all 3 nodes share the same timeout and
//!    the same simulated clock, the transitions happen at the
//!    same tick — the canonical "3 simultaneous candidates"
//!    scenario.
//! 5. Heal every cut. Pre-votes / votes / heartbeats can now flow.
//! 6. Resume the manual pump and assert the cluster converges to
//!    exactly one Leader. Per Raft, either one candidate wins
//!    outright via majority votes, or split-vote forces a new
//!    election that resolves on the next attempt.

use std::time::Duration;

use xraft_core::types::{NodeId, NodeRole};
use xraft_test::{SimulatedCluster, SimulatedClusterConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_three_candidate_election_resolves_to_unique_leader() {
    let _ = tracing_subscriber::fmt::try_init();

    // Equal min/max election timeout: every node times out at the
    // same instant in simulated time. Tight enough that "2x
    // election_max" advance is cheap; loose enough that one
    // election round comfortably fits without re-fire.
    // Very tight election timeout range — the engine requires
    // `max > min`, so we use a 1-ms spread which still puts every
    // node's election timer within one tick quantum (5 ms) of
    // every other node's, giving us the "simultaneous candidate"
    // scenario as soon as the simulated clock crosses the window.
    let cfg = SimulatedClusterConfig {
        size: 3,
        seed: 0xC0FF_EE84,
        election_min_ms: 300,
        election_max_ms: 301,
        ..SimulatedClusterConfig::default()
    };
    let election_max = Duration::from_millis(cfg.election_max_ms);
    let mut cluster = SimulatedCluster::start(cfg)
        .await
        .expect("cluster start must succeed");

    cluster.detach_tick_pump().await;
    cluster.start_manual_pump(4);

    let (orig_leader, orig_term) = cluster
        .await_leader(Duration::from_secs(10))
        .await
        .expect("initial leader must be elected");

    // Full-mesh partition: every node is cut from every other.
    cluster.detach_tick_pump().await;
    let ids: Vec<_> = cluster.nodes.iter().map(|n| n.node_id).collect();
    for i in 0..ids.len() {
        for j in 0..ids.len() {
            if i != j {
                cluster.network.cut_directed(ids[i], ids[j]);
            }
        }
    }

    // Burst-advance simulated time past 2x election_max. With every
    // node fully partitioned, every follower's heartbeat-contact
    // timer expires and the engine transitions Follower → PreCandidate.
    // We poll-advance in small batches and stop as soon as every node
    // has reached PreCandidate or Candidate state, so the "exactly 3
    // simultaneous candidates" snapshot below catches them in the
    // candidate phase rather than after they've cycled back to
    // Follower via PreVote-rejection.
    let candidate_term;
    {
        let mut elapsed = Duration::ZERO;
        let budget = election_max * 8;
        loop {
            if elapsed >= budget {
                break;
            }
            cluster.advance_simulated_time(election_max).await;
            elapsed += election_max;
            let mut counts: std::collections::HashMap<u64, usize> =
                std::collections::HashMap::new();
            let mut all_candidate_or_pre = true;
            for n in &cluster.nodes {
                if let Some(s) = n.status.status().await {
                    if !matches!(s.role, NodeRole::PreCandidate | NodeRole::Candidate) {
                        all_candidate_or_pre = false;
                    }
                    if matches!(s.role, NodeRole::PreCandidate | NodeRole::Candidate) {
                        *counts.entry(s.term).or_insert(0) += 1;
                    }
                } else {
                    all_candidate_or_pre = false;
                }
            }
            if all_candidate_or_pre && counts.values().any(|&c| c == cluster.len()) {
                break;
            }
        }

        // Snapshot every node's role + term and assert the brief's
        // literal property: EXACTLY 3 nodes are in a non-Follower
        // / non-Leader (i.e. candidate) role AT THE SAME TERM.
        let mut role_by_term: std::collections::HashMap<u64, usize> =
            std::collections::HashMap::new();
        let mut per_node: Vec<(NodeId, NodeRole, u64)> = Vec::new();
        for n in &cluster.nodes {
            let s = n
                .status
                .status()
                .await
                .unwrap_or_else(|| panic!("status missing for node {:?}", n.node_id));
            per_node.push((n.node_id, s.role, s.term));
            if matches!(s.role, NodeRole::PreCandidate | NodeRole::Candidate) {
                *role_by_term.entry(s.term).or_insert(0) += 1;
            }
        }
        let same_term_candidates = role_by_term
            .iter()
            .max_by_key(|&(_, count)| *count)
            .map(|(t, c)| (*t, *c));
        let (the_term, the_count) = same_term_candidates.unwrap_or_else(|| {
            panic!(
                "no candidates observed after partition; nodes: {:?}",
                per_node
            )
        });
        assert_eq!(
            the_count,
            cluster.len(),
            "expected exactly 3 simultaneous candidates at the same term \
             (per brief: '3 candidates start elections at same term'); \
             got {the_count} at term {the_term}; per-node: {:?} \
             (orig leader was {orig_leader:?} @ term {orig_term})",
            per_node
        );
        candidate_term = the_term;
    }
    let _ = candidate_term;

    // Heal the partition: pre-votes / votes / heartbeats can now
    // flow. Resume the manual pump so simulated time keeps
    // advancing while the cluster resolves the tie.
    cluster.network.heal_all();
    cluster.start_manual_pump(4);

    // The engine's PreVote → Vote → Leader path resolves at most
    // one election per "term window"; if all 3 cast different
    // PreVotes, the round produces no winner and the next tick
    // window starts a fresh attempt with re-randomised (but
    // since min==max, IDENTICAL) timeouts. The engine's
    // `randomized_election_timeout()` uses the node's seeded RNG
    // — distinct per-node seeds derived via `mix_seed` in the
    // harness — so the post-heal timeout phase produces enough
    // variance for one candidate to win.
    //
    // We allow a generous deadline because in the worst case the
    // first attempt deadlocks (split vote 1-1-1 cannot occur
    // because we have 3 voters and PreVote needs majority 2/3 to
    // even consider becoming Candidate; the realistic worst case
    // is "two PreCandidates each get their own vote + one
    // straggler" → no Candidate emerges, retry on next election
    // timeout). Per the brief: "verify exactly one wins or a NEW
    // ELECTION resolves the tie".
    let (winner, winner_term) = cluster
        .await_leader(Duration::from_secs(30))
        .await
        .expect("a unique leader must emerge from the simultaneous-election scenario");

    assert!(
        winner_term >= orig_term,
        "post-partition leader's term ({winner_term}) must be ≥ pre-partition term ({orig_term})"
    );

    // Verify EXACTLY one leader at the converged term. The
    // engine's `try_converged_leader` already does this — we
    // re-verify by walking statuses ourselves so the failure
    // message is informative.
    let mut leader_count = 0;
    for n in &cluster.nodes {
        if let Some(s) = n.status.status().await
            && s.role == NodeRole::Leader
        {
            leader_count += 1;
            assert_eq!(n.node_id, winner, "the converged leader id must match");
            assert_eq!(s.term, winner_term, "the converged leader term must match");
        }
    }
    assert_eq!(
        leader_count, 1,
        "exactly one node must report `Leader` after resolution; got {leader_count}"
    );

    cluster.shutdown().await;
}
