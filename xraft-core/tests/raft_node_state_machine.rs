//! Stage 3.1 — Raft Node State Machine integration tests.
//!
//! These tests exercise [`xraft_core::RaftNode`] through the *public* crate
//! surface only — that is, the convenience re-exports at the crate root
//! ([`xraft_core::RaftNode`], [`xraft_core::Action`], [`xraft_core::Input`],
//! …) plus the public [`xraft_core::message`] module for payload types
//! (such as [`xraft_core::message::PreVoteResponse`]) that are not crate-
//! root re-exported but are reachable through `pub mod message;` in
//! `xraft-core/src/lib.rs`. This is the same surface a downstream
//! consumer (`xraft-server`, `xraft-test`) wires the consensus engine
//! against. The tests deliberately do not poke private internals; if a
//! future refactor narrows the public API and breaks these tests, that
//! is a genuine consumer-visible regression worth flagging.
//!
//! The first three scenarios mirror the Stage 3.1 acceptance criteria
//! listed in `docs/stories/failover-cluster-XRAFT/implementation-plan.md`:
//!
//! - `initial-state` — a freshly-constructed node is a `Follower` at
//!   `Term(0)` with a running, non-expired election timer.
//! - `election-timeout-triggers-candidacy` — repeated `Input::Tick`s past
//!   the randomised election timeout drive the node out of `Follower`.
//!   Per `architecture.md` §5.1 and `e2e-scenarios.md` Feature 3 the
//!   immediate transition is into `PreCandidate` (Pre-Vote phase)
//!   without bumping the term — the term increment is gated on a
//!   Pre-Vote quorum and is exercised by the cascade test below.
//! - `become-leader-initializes-peers` — `become_leader()` initialises
//!   per-peer replication state (`last_fetch_offset = 0`) and emits a
//!   no-op `Action::AppendEntries` at `last_log_index + 1` so the new
//!   leader can commit at least one entry in its term (Raft Figure 8).
//!
//! A fourth scenario, `scenario_election_cycle_completes_to_candidate_with_term_bump`,
//! drives the FULL cold-start election cycle through the public `step()`
//! API (Tick → PreCandidate → Pre-Vote-quorum cascade) so the literal
//! end-state acceptance contract from `implementation-plan.md`
//! (`election-timeout-triggers-candidacy`: "transitions to Candidate and
//! increments term") and `e2e-scenarios.md` Feature 1
//! ("transitions to Candidate ... increments its epoch ... sends
//! RequestVote RPCs") is verified in addition to the Pre-Vote-first
//! intermediate snapshot.

use std::collections::BTreeMap;

use rand::SeedableRng;
use rand::rngs::StdRng;

use xraft_core::message::PreVoteResponse;
use xraft_core::{
    Action, ClusterConfig, ElectionTimer, EntryPayload, Input, LogIndex, NodeId, NodeRole,
    OutboundMessage, PeerState, RaftNode, Term,
};

/// Cluster_id used in [`three_voter_config`]; injected `PreVoteResponse`s
/// must echo this exactly or the response handler drops them silently
/// (cluster_id mismatch is a Stage 3.2 anti-spoof gate).
const TEST_CLUSTER_ID: &str = "stage-3-1-it";

/// Build a deterministic three-voter cluster config with `node_id = 1`
/// (so the local node has two peers: nodes 2 and 3). The directory_id
/// UUIDs are literal non-nil values so the test is fully reproducible
/// without depending on `Uuid::new_v4`'s entropy.
fn three_voter_config() -> ClusterConfig {
    // The literal cluster_id in the TOML below MUST match
    // [`TEST_CLUSTER_ID`] (the Stage-3.2 vote-traffic handlers drop
    // messages whose cluster_id does not match the local config).
    let toml = r#"
node_id = 1
cluster_id = "stage-3-1-it"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200

[[voters]]
node_id = 1
directory_id = "11111111-1111-4111-8111-111111111111"
host = "node1"
port = 6000

[[voters]]
node_id = 2
directory_id = "22222222-2222-4222-8222-222222222222"
host = "node2"
port = 6001

[[voters]]
node_id = 3
directory_id = "33333333-3333-4333-8333-333333333333"
host = "node3"
port = 6002
"#;
    ClusterConfig::from_toml_str(toml).expect("test config must parse and validate")
}

// -----------------------------------------------------------------------
// Stage 3.1 scenario: initial-state
// -----------------------------------------------------------------------

#[test]
fn scenario_initial_state_follower_term_zero_timer_running() {
    let node = RaftNode::new_with_seed(three_voter_config(), 0xC0FFEE).unwrap();

    // Role / term / vote start in their canonical Stage 3.1 initial state.
    assert_eq!(node.role, NodeRole::Follower);
    assert_eq!(node.current_term(), Term(0));
    assert!(!node.is_leader());
    assert!(node.leader_id.is_none());
    assert_eq!(node.id, NodeId(1));
    assert_eq!(node.commit_index, LogIndex(0));
    assert_eq!(node.last_applied, LogIndex(0));
    assert_eq!(node.last_log_index, LogIndex(0));
    assert_eq!(node.last_log_term, Term(0));
    assert!(node.votes_received.is_empty());
    assert!(node.pre_votes_received.is_empty());

    // Peers are initialised from the structured voter set, excluding self.
    assert!(node.voter_set.is_some());
    assert_eq!(node.peers.len(), 2);
    assert!(node.peers.contains_key(&NodeId(2)));
    assert!(node.peers.contains_key(&NodeId(3)));
    assert!(!node.peers.contains_key(&NodeId(1)));

    // Election timer is initialised but not already expired so a freshly-
    // constructed Follower does not call an immediate election on the
    // first Tick. The randomised timeout lies inside `[min_ticks, max_ticks]`.
    assert!(!node.election_timer.is_expired());
    assert!(node.election_timer.remaining() > 0);
    let timeout = node.election_timer.timeout_ticks();
    let min = node.election_timer.min_ticks();
    let max = node.election_timer.max_ticks();
    assert!(
        timeout >= min && timeout <= max,
        "randomised timeout {timeout} must lie within [{min}, {max}]",
    );
}

// -----------------------------------------------------------------------
// Stage 3.1 scenario: election-timeout-triggers-candidacy
// -----------------------------------------------------------------------

#[test]
fn scenario_election_timeout_triggers_candidacy() {
    let mut node = RaftNode::new_with_seed(three_voter_config(), 7).unwrap();
    let initial_term = node.current_term();
    assert_eq!(node.role, NodeRole::Follower);

    // Pump Ticks past the longest possible randomised timeout. The +5
    // budget covers the `tick()`-then-check vs. expired ordering (the
    // node only acts on the *next* Tick after the timer crosses
    // `timeout_ticks`).
    let max_ticks = node.election_timer.max_ticks() + 5;
    let mut transition_actions: Option<Vec<Action>> = None;
    for _ in 0..max_ticks {
        let actions = node.step(Input::Tick);
        if node.role != NodeRole::Follower {
            transition_actions = Some(actions);
            break;
        }
    }
    let actions = transition_actions
        .expect("Follower must have transitioned out of Follower within max_ticks");

    // Per architecture.md §5.1 / e2e-scenarios.md Feature 3: the Stage-3.1
    // election-timeout reaction is `Follower -> PreCandidate` (Pre-Vote).
    // The real `Candidate` transition is gated on a pre-vote quorum
    // landing in Stage 3.2.
    assert_eq!(
        node.role,
        NodeRole::PreCandidate,
        "Tick-driven election timeout must enter PreCandidate (not Candidate)",
    );
    assert_eq!(
        node.current_term(),
        initial_term,
        "Pre-Vote must NOT increment the term",
    );
    assert_eq!(
        node.hard_state.voted_for, None,
        "Pre-Vote must NOT cast a real vote",
    );
    // The node grants its own pre-vote at the moment of transition so
    // the Pre-Vote tally reflects self-eligibility from the outset.
    assert!(node.pre_votes_received.contains(&node.id));

    // One PreVoteRequest per peer is fanned out to the driver.
    let pre_vote_requests = actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::PreVoteRequest(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        pre_vote_requests,
        node.peers.len(),
        "expected one PreVoteRequest per peer",
    );

    // No real VoteRequests yet — those fire only after Pre-Vote quorum.
    let real_vote_requests = actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::VoteRequest(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        real_vote_requests, 0,
        "Pre-Vote must NOT emit real VoteRequests before pre-vote quorum",
    );

    // No PersistHardState — the term has not changed and no vote was cast.
    let persist_actions = actions
        .iter()
        .filter(|a| matches!(a, Action::PersistHardState))
        .count();
    assert_eq!(
        persist_actions, 0,
        "Pre-Vote must NOT persist hard state (term unchanged, no real vote)",
    );
}

// -----------------------------------------------------------------------
// Stage 3.1 scenario: become-leader-initializes-peers
// -----------------------------------------------------------------------

#[test]
fn scenario_become_leader_initializes_peers_and_emits_noop() {
    let mut node = RaftNode::new_with_seed(three_voter_config(), 0xBEEF).unwrap();

    // Drive into a non-zero term first so the leader's no-op carries a
    // meaningful term value (a no-op at Term(0) would be ambiguous).
    let _ = node.become_candidate();
    let term_before_leader = node.current_term();
    let last_log_before = node.last_log_index;
    assert!(
        term_before_leader.0 > 0,
        "become_candidate must have bumped term"
    );

    let actions = node.become_leader();

    // Role is now Leader and the node knows it leads the current term.
    assert_eq!(node.role, NodeRole::Leader);
    assert_eq!(node.leader_id, Some(node.id));
    assert!(node.is_leader());

    // Per-peer replication state is (re-)initialised on transition.
    assert!(!node.peers.is_empty(), "test config must seed peers");
    for (peer_id, peer) in &node.peers {
        assert_eq!(
            peer.last_fetch_offset,
            LogIndex(0),
            "peer {peer_id:?} last_fetch_offset must be initialised to 0 on become_leader",
        );
        assert!(
            peer.is_voter,
            "all configured peers in this test are voters"
        );
    }

    // BecomeLeader signal is emitted so the driver can flush role-change
    // metrics, log lines, and step-down handlers.
    assert!(
        actions.iter().any(|a| matches!(a, Action::BecomeLeader)),
        "expected Action::BecomeLeader in {actions:?}",
    );

    // A single no-op AppendEntries at last_log_index+1 with the leader's
    // term (Raft Figure 8 — leaders must commit at least one entry in
    // their own term before they can safely commit prior-term entries).
    let expected_index = LogIndex(last_log_before.0 + 1);
    let noop_appended = actions.iter().any(|a| match a {
        Action::AppendEntries(entries) => {
            entries.len() == 1
                && matches!(entries[0].payload, EntryPayload::NoOp)
                && entries[0].term == term_before_leader
                && entries[0].index == expected_index
        }
        _ => false,
    });
    assert!(
        noop_appended,
        "expected an AppendEntries(no-op) at index {expected_index:?} \
         with term {term_before_leader:?}, got {actions:?}",
    );

    // The in-memory log mirror has advanced to reflect the no-op so
    // subsequent election-eligibility / replication-probe checks see the
    // post-no-op log state.
    assert_eq!(node.last_log_index, expected_index);
    assert_eq!(node.last_log_term, term_before_leader);
}

// -----------------------------------------------------------------------
// Acceptance-contract reconciliation: end-state of the full election cycle
// -----------------------------------------------------------------------
//
// The Stage 3.1 implementation plan
// (`docs/stories/failover-cluster-XRAFT/implementation-plan.md`) and the
// e2e scenarios document
// (`docs/stories/failover-cluster-XRAFT/e2e-scenarios.md` Feature 1)
// describe the cold-start election outcome as
// "Follower transitions to Candidate AND increments its term/epoch AND
//  sends RequestVote RPCs to its peers".
// The KRaft architecture (`architecture.md` section 5.1) reaches that
// end state through Pre-Vote-first: the timer expiry enters PreCandidate
// without bumping the term, and a Pre-Vote quorum then cascades into
// Candidate (term++, self-vote, PersistHardState, real VoteRequests).
//
// `scenario_election_timeout_triggers_candidacy` covers the Stage-3.1
// PreCandidate snapshot. The test below covers the FULL cycle end state
// so the literal acceptance contract holds when the cycle completes.

#[test]
fn scenario_election_cycle_completes_to_candidate_with_term_bump() {
    let mut node = RaftNode::new_with_seed(three_voter_config(), 0xCAFE).unwrap();
    let initial_term = node.current_term();
    assert_eq!(node.role, NodeRole::Follower);

    // Phase 1: Tick past the randomised election timeout into PreCandidate
    // (per the architecture's Pre-Vote-first cold-start path).
    let max_ticks = node.election_timer.max_ticks() + 5;
    for _ in 0..max_ticks {
        let _ = node.step(Input::Tick);
        if node.role == NodeRole::PreCandidate {
            break;
        }
    }
    assert_eq!(
        node.role,
        NodeRole::PreCandidate,
        "Tick must drive Follower into PreCandidate within max_ticks",
    );
    assert_eq!(
        node.current_term(),
        initial_term,
        "Pre-Vote phase must NOT bump the term",
    );

    // Phase 2: Inject one PreVoteResponse(true) from peer node-2 via the
    // public step() API. Combined with the candidate's self-pre-vote
    // (recorded at the moment of `become_pre_candidate`), the tally
    // reaches 2-of-3 voters which is the Pre-Vote quorum threshold for a
    // 3-voter cluster. The Stage-3.2 response handler then cascades into
    // `become_candidate`, satisfying the literal acceptance contract.
    let pre_vote_grant = PreVoteResponse {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: 0,
        term: initial_term,
        vote_granted: true,
        leader_hint: None,
    };
    let cascade_actions = node.step(Input::PreVoteResponse {
        from: NodeId(2),
        response: pre_vote_grant,
    });

    // Acceptance-contract end state per implementation-plan.md scenario
    // `election-timeout-triggers-candidacy` and e2e-scenarios.md Feature 1
    // (cold-start election): role=Candidate, term incremented, self-voted,
    // hard state persisted, RequestVote RPCs fanned out to all peers.
    assert_eq!(
        node.role,
        NodeRole::Candidate,
        "Pre-Vote quorum must cascade Follower -> ... -> Candidate",
    );
    assert_eq!(
        node.current_term().0,
        initial_term.0 + 1,
        "Candidate transition must increment the term exactly once \
         (acceptance: implementation-plan.md `election-timeout-triggers-candidacy` / \
         e2e-scenarios.md Feature 1 cold-start election)",
    );
    assert_eq!(
        node.hard_state.voted_for,
        Some(node.id),
        "Candidate must have cast its self-vote",
    );
    assert!(
        cascade_actions
            .iter()
            .any(|a| matches!(a, Action::PersistHardState)),
        "Candidate transition must emit PersistHardState (term and voted_for changed)",
    );

    // Real VoteRequest RPCs are fanned out to all peers (one per peer).
    let real_vote_requests = cascade_actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::VoteRequest(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        real_vote_requests,
        node.peers.len(),
        "expected one VoteRequest per peer after Candidate transition",
    );

    // The stale pre-vote tally is cleared on entering Candidate so a
    // future re-election cycle starts from a fresh slate.
    assert!(
        node.pre_votes_received.is_empty(),
        "Candidate transition must clear the stale pre-vote tally",
    );
}

// -----------------------------------------------------------------------
// Stage 3.1 deliverable surface: ElectionTimer is reachable through the
// public re-export and constructable from its `from_config_ms` entrypoint.
// This guards the `ElectionTimer` deliverable listed in the implementation
// plan against accidental privatisation in a future refactor.
// -----------------------------------------------------------------------

#[test]
fn election_timer_constructable_via_public_api() {
    let mut rng = StdRng::seed_from_u64(13);
    let mut timer = ElectionTimer::from_config_ms(150, 300, 10, &mut rng);

    // Randomised timeout is within the configured range and the timer is
    // not already expired on construction.
    assert!(timer.timeout_ticks() >= timer.min_ticks());
    assert!(timer.timeout_ticks() <= timer.max_ticks());
    assert_eq!(timer.remaining(), timer.timeout_ticks());
    assert!(!timer.is_expired());

    // `tick()` advances logical time; `is_expired()` flips after enough
    // ticks; `reset()` re-randomises the target and clears expiration.
    let target = timer.timeout_ticks();
    for _ in 0..target {
        timer.tick();
    }
    assert!(
        timer.is_expired(),
        "timer must be expired after {target} ticks",
    );
    timer.reset(&mut rng);
    assert!(!timer.is_expired(), "reset() must clear expiration");
}

// -----------------------------------------------------------------------
// Stage 3.1 / 3.2 contract: unsupported-input rejection is visible
// -----------------------------------------------------------------------
//
// The plan (`implementation-plan.md` §3.1, sixth bullet) says
// `step(input)` "processes a single input (Tick, VoteRequest,
// FetchRequest, ClientPropose, etc.) and returns side-effect actions".
// Stages 3.1 and 3.2 implement Tick + Vote/PreVote handling but defer
// FetchRequest / FetchResponse / ClientPropose to Stage 3.3. Rather
// than silently returning an empty `Vec<Action>` for those inputs --
// which would be invisible to the driver and to operators -- the
// engine emits a structured `Action::RejectUnsupportedInput` so the
// driver can reply / surface a metric / forward an "unsupported"
// error. This test pins that contract.

#[test]
fn scenario_stage_3_3_inputs_emit_visible_rejection() {
    use xraft_core::message::{FetchRequest, FetchResponse};

    /// Snapshot of every Raft-state field whose mutation `step()` is
    /// contractually forbidden from causing on a Stage-3.3-deferred input.
    /// The snapshot covers role/term/vote/commit/last-applied (the durable
    /// election surface), the volatile log tip (`last_log_index`/
    /// `last_log_term`), the full election-timer surface
    /// (`elapsed`/`timeout_ticks`/`min_ticks`/`max_ticks`), the leader-
    /// contact tracking surface (`leader_id`, `last_leader_contact_tick`),
    /// the logical-clock surface (`logical_tick`), the in-flight election-
    /// vote surfaces (`votes_received`, `pre_votes_received` — captured as
    /// sorted `Vec<NodeId>` so equality is deterministic regardless of the
    /// underlying `HashSet` iteration order), and the per-peer replication-
    /// progress map (`peers` — captured as `BTreeMap<NodeId, PeerState>`
    /// so equality is deterministic regardless of `HashMap` iteration
    /// order). The test fails LOUDLY if a future refactor accidentally
    /// calls `election_timer.tick()`, `election_timer.reset()`, bumps a
    /// peer's `last_fetch_offset`, or otherwise mutates any of these
    /// fields from inside a rejection arm.
    #[derive(Debug, PartialEq, Eq)]
    struct RaftStateSnapshot {
        role: NodeRole,
        term: Term,
        voted_for: Option<NodeId>,
        commit_index: LogIndex,
        last_applied: LogIndex,
        last_log_index: LogIndex,
        last_log_term: Term,
        timer_elapsed: u64,
        timer_timeout_ticks: u64,
        timer_min_ticks: u64,
        timer_max_ticks: u64,
        leader_id: Option<NodeId>,
        last_leader_contact_tick: Option<u64>,
        logical_tick: u64,
        votes_received: Vec<NodeId>,
        pre_votes_received: Vec<NodeId>,
        peers: BTreeMap<NodeId, PeerState>,
    }

    fn snapshot(node: &RaftNode) -> RaftStateSnapshot {
        let mut votes_received: Vec<NodeId> = node.votes_received.iter().copied().collect();
        votes_received.sort();
        let mut pre_votes_received: Vec<NodeId> = node.pre_votes_received.iter().copied().collect();
        pre_votes_received.sort();
        let peers: BTreeMap<NodeId, PeerState> = node
            .peers
            .iter()
            .map(|(id, state)| (*id, state.clone()))
            .collect();
        RaftStateSnapshot {
            role: node.role,
            term: node.current_term(),
            voted_for: node.voted_for(),
            commit_index: node.commit_index,
            last_applied: node.last_applied,
            last_log_index: node.last_log_index,
            last_log_term: node.last_log_term,
            timer_elapsed: node.election_timer.elapsed(),
            timer_timeout_ticks: node.election_timer.timeout_ticks(),
            timer_min_ticks: node.election_timer.min_ticks(),
            timer_max_ticks: node.election_timer.max_ticks(),
            leader_id: node.leader_id,
            last_leader_contact_tick: node.last_leader_contact_tick,
            logical_tick: node.logical_tick,
            votes_received,
            pre_votes_received,
            peers,
        }
    }

    /// Assert that feeding `input` into `node` yields exactly one
    /// `Action::RejectUnsupportedInput { input_kind: expected_kind, .. }`
    /// with a non-empty `reason` that explicitly references the deferred
    /// "Stage 3.3" wiring, and that the call did NOT mutate any
    /// Raft-state field captured by [`snapshot`].
    fn assert_rejection(node: &mut RaftNode, input: Input, expected_kind: &str) {
        let before = snapshot(node);
        let actions = node.step(input);
        let after = snapshot(node);
        assert_eq!(
            actions.len(),
            1,
            "{expected_kind} must yield exactly one rejection action, got {actions:?}",
        );
        let Action::RejectUnsupportedInput { input_kind, reason } = &actions[0] else {
            panic!(
                "{expected_kind} must yield Action::RejectUnsupportedInput, got {:?}",
                actions[0],
            );
        };
        assert_eq!(
            *input_kind, expected_kind,
            "{expected_kind} rejection must carry input_kind=\"{expected_kind}\", got {input_kind:?}",
        );
        assert!(
            !reason.is_empty(),
            "{expected_kind} rejection must carry a non-empty `reason` so the driver can surface it; got empty string",
        );
        assert!(
            reason.contains("Stage 3.3"),
            "{expected_kind} rejection `reason` must explicitly reference the deferred Stage 3.3 (Log Replication) wiring so operators know which stage replaces the rejection; got {reason:?}",
        );
        assert_eq!(
            after, before,
            "{expected_kind} rejection must NOT mutate any Raft-state field \
             (role/term/vote/commit/last_applied/last_log_index/last_log_term/election_timer/\
             leader_id/last_leader_contact_tick/logical_tick/votes_received/pre_votes_received/peers); \
             before={before:?}, after={after:?}",
        );
    }

    let mut node = RaftNode::new_with_seed(three_voter_config(), 21).unwrap();

    // Sanity: a freshly-constructed node is in the canonical Stage 3.1
    // initial state. The accessors below are normalised under
    // `hard_state` (storage-backed deviation -- see the doc-comment on
    // `RaftNode`).
    assert_eq!(node.current_term(), Term(0));
    assert_eq!(node.voted_for(), None);

    // ClientPropose -- the most operator-visible Stage 3.3 input.
    assert_rejection(
        &mut node,
        Input::ClientPropose(b"hello".to_vec().into()),
        "ClientPropose",
    );

    // FetchRequest -- driver should see a rejection it can forward over RPC.
    let fetch_req = FetchRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: 0,
        replica_id: NodeId(2),
        fetch_offset: LogIndex(0),
        last_fetched_epoch: Term(0),
    };
    assert_rejection(&mut node, Input::FetchRequest(fetch_req), "FetchRequest");

    // FetchResponse -- the follower-side variant; same rejection contract.
    let fetch_resp = FetchResponse {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: 0,
        leader_id: NodeId(2),
        high_watermark: LogIndex(0),
        entries: Vec::new(),
        diverging_epoch: None,
    };
    assert_rejection(&mut node, Input::FetchResponse(fetch_resp), "FetchResponse");

    // FetchRequestAcked -- the leader-side per-peer replication-progress
    // feedback added by the upstream Stage 3.3 message-shape merge. Same
    // visible-rejection contract until the per-peer progress handler
    // lands in Stage 3.3.
    assert_rejection(
        &mut node,
        Input::FetchRequestAcked {
            replica_id: NodeId(2),
            confirmed_offset: LogIndex(0),
        },
        "FetchRequestAcked",
    );

    // Final cumulative check: across all four rejections combined, the
    // node remains in its canonical Stage 3.1 initial state -- no role
    // transition, no term/vote bump, no commit advance, no log tip
    // movement, no leader-contact recording, no logical-tick advance,
    // and no in-flight vote tallies. The election timer has neither
    // ticked nor been reset (so a Tick-driven election timeout fires
    // on the same schedule it would have without these inputs), and
    // every peer's replication-progress fields stay at their
    // construction-time defaults.
    assert_eq!(node.role, NodeRole::Follower);
    assert_eq!(node.current_term(), Term(0));
    assert_eq!(node.voted_for(), None);
    assert_eq!(node.commit_index, LogIndex(0));
    assert_eq!(node.last_applied, LogIndex(0));
    assert_eq!(node.last_log_index, LogIndex(0));
    assert_eq!(node.last_log_term, Term(0));
    assert_eq!(
        node.election_timer.elapsed(),
        0,
        "election_timer must not have been ticked by any rejection arm",
    );
    assert_eq!(
        node.leader_id, None,
        "leader_id must not have been set by any rejection arm",
    );
    assert_eq!(
        node.last_leader_contact_tick, None,
        "last_leader_contact_tick must not have been set by any rejection arm",
    );
    assert_eq!(
        node.logical_tick, 0,
        "logical_tick must not have advanced (only Input::Tick may advance it)",
    );
    assert_eq!(
        node.votes_received.iter().count(),
        0,
        "votes_received must remain empty (no rejection arm runs an election)",
    );
    assert_eq!(
        node.pre_votes_received.iter().count(),
        0,
        "pre_votes_received must remain empty (no rejection arm runs a pre-election)",
    );
    for (peer_id, peer) in node.peers.iter() {
        assert_eq!(
            peer.last_fetch_offset,
            LogIndex(0),
            "peer {peer_id:?} last_fetch_offset must stay at construction-time default",
        );
        assert_eq!(
            peer.last_fetch_time, 0,
            "peer {peer_id:?} last_fetch_time must stay at construction-time default",
        );
        assert_eq!(
            peer.last_caught_up_time, 0,
            "peer {peer_id:?} last_caught_up_time must stay at construction-time default",
        );
    }
}
