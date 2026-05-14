//! Raft node state machine — core consensus engine.
//!
//! `RaftNode` holds the volatile and durable state for a single Raft participant.
//! It processes [`Input`] events and emits [`Action`] side-effects without
//! performing any I/O itself (I/O is delegated to the driver layer in
//! `xraft-server`).
//!
//! # Stage 3.1 / Stage 3.2 scope
//!
//! Stage 3.1 (Raft Node State Machine) established the structural foundation:
//! - [`ElectionTimer`] — randomised tick-based timeout in the
//!   `[election_timeout_min_ms, election_timeout_max_ms]` configured range.
//! - [`PeerState`] — per-peer tracking used by the leader to drive
//!   pull-based replication.
//! - [`RaftNode`] role transitions: `become_follower`, `become_pre_candidate`,
//!   `become_candidate`, `become_leader`.
//! - [`RaftNode::step`] handling for [`Input::Tick`]: detects election timeout
//!   on followers/candidates and triggers an election.
//!
//! Stage 3.2 (Leader Election) adds the on-receive handlers that drive the
//! full Pre-Vote → Vote → Leader cascade across a real cluster:
//! - [`RaftNode::handle_vote_request`] — validate term, log up-to-dateness,
//!   and `voted_for`; grant or reject a real vote with a single coalesced
//!   `PersistHardState` action where applicable.
//! - [`RaftNode::handle_vote_response`] — tally votes from voters,
//!   step down on a higher observed term, transition to `Leader` on quorum.
//! - [`RaftNode::handle_pre_vote_request`] — speculative-grant check that
//!   does NOT mutate term, `voted_for`, or the election timer. Rejected when
//!   the responder still considers a leader recently active (per
//!   `architecture.md` §2.1 — Pre-Vote prevents disruptive elections).
//! - [`RaftNode::handle_pre_vote_response`] — tally pre-votes (including
//!   from voters at a lagging term — Pre-Vote responders do not bump terms),
//!   step down on observed higher term, transition to `Candidate` on
//!   pre-election quorum.
//!
//! Leader-side Fetch handling and follower-side log replication are Stage 3.3
//! territory. Stage 3.2 deliberately keeps `last_leader_contact_tick` updates
//! limited to the contact paths that already exist in 3.1 (the explicit
//! `become_follower(_, Some(leader_id))` call site); the driver layer wires
//! Fetch-response contact updates in Stage 3.3.

use std::collections::HashMap;

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::{Rng, RngCore};

use crate::config::ClusterConfig;
use crate::error::Result;
use crate::message::Entry;
use crate::message::{
    Action, EntryPayload, Input, OutboundMessage, PreVoteRequest, PreVoteResponse, VoteRequest,
    VoteResponse,
};
use crate::types::{HardState, LogIndex, NodeId, NodeRole, Term, VoteGrantedSet, VoterSet};

// ---------------------------------------------------------------------------
// ElectionTimer
// ---------------------------------------------------------------------------

/// A logical-tick election timer with a randomised target inside the
/// `[min_ticks, max_ticks]` range.
///
/// The timer is purely *logical*: callers advance it via [`tick`](Self::tick)
/// (typically once per `Input::Tick`) and check [`is_expired`](Self::is_expired).
/// The randomisation prevents election storms (the canonical Raft mitigation:
/// each node randomises its timeout so that simultaneous elections are rare).
///
/// Conversion of millisecond bounds to ticks uses ceiling division so the
/// resulting timer never expires *before* the configured `min_ms` even when
/// `tick_interval_ms` does not evenly divide it. The timer is clamped to at
/// least `1` tick so a fast tick interval still allows the timer to fire.
#[derive(Debug, Clone)]
pub struct ElectionTimer {
    elapsed_ticks: u64,
    timeout_ticks: u64,
    min_ticks: u64,
    max_ticks: u64,
}

impl ElectionTimer {
    /// Build an election timer from `(election_timeout_min_ms,
    /// election_timeout_max_ms, tick_interval_ms)`.
    ///
    /// Both bounds use ceiling division to avoid expiring earlier than the
    /// configured `min_ms`. Both bounds are clamped to at least `1` tick.
    pub fn from_config_ms<R: Rng + ?Sized>(
        min_ms: u64,
        max_ms: u64,
        tick_interval_ms: u64,
        rng: &mut R,
    ) -> Self {
        let interval = tick_interval_ms.max(1);
        let min_ticks = min_ms.div_ceil(interval).max(1);
        let max_ticks = max_ms.div_ceil(interval).max(min_ticks);
        Self::new(min_ticks, max_ticks, rng)
    }

    /// Build an election timer with explicit `[min_ticks, max_ticks]` bounds.
    /// Panics if `max_ticks < min_ticks` or `min_ticks == 0`.
    pub fn new<R: Rng + ?Sized>(min_ticks: u64, max_ticks: u64, rng: &mut R) -> Self {
        assert!(min_ticks > 0, "ElectionTimer min_ticks must be > 0");
        assert!(
            max_ticks >= min_ticks,
            "ElectionTimer max_ticks ({max_ticks}) must be >= min_ticks ({min_ticks})"
        );
        let timeout_ticks = pick_in_range(min_ticks, max_ticks, rng);
        Self {
            elapsed_ticks: 0,
            timeout_ticks,
            min_ticks,
            max_ticks,
        }
    }

    /// Reset the timer: zero elapsed ticks, re-randomise the target timeout.
    pub fn reset<R: Rng + ?Sized>(&mut self, rng: &mut R) {
        self.elapsed_ticks = 0;
        self.timeout_ticks = pick_in_range(self.min_ticks, self.max_ticks, rng);
    }

    /// Advance the timer by one tick.
    pub fn tick(&mut self) {
        self.elapsed_ticks = self.elapsed_ticks.saturating_add(1);
    }

    /// Whether the timer has elapsed its current target.
    pub fn is_expired(&self) -> bool {
        self.elapsed_ticks >= self.timeout_ticks
    }

    /// Number of ticks remaining before expiry. Saturates to 0 once expired.
    pub fn remaining(&self) -> u64 {
        self.timeout_ticks.saturating_sub(self.elapsed_ticks)
    }

    /// Current target timeout (in ticks).
    pub fn timeout_ticks(&self) -> u64 {
        self.timeout_ticks
    }

    /// Configured minimum (in ticks).
    pub fn min_ticks(&self) -> u64 {
        self.min_ticks
    }

    /// Configured maximum (in ticks).
    pub fn max_ticks(&self) -> u64 {
        self.max_ticks
    }

    /// Elapsed ticks since the last reset.
    pub fn elapsed(&self) -> u64 {
        self.elapsed_ticks
    }
}

/// Pick a value in `[lo, hi]` (inclusive) without panicking on `lo == hi`
/// (the standard `Rng::gen_range` panics on empty ranges).
fn pick_in_range<R: Rng + ?Sized>(lo: u64, hi: u64, rng: &mut R) -> u64 {
    if lo == hi { lo } else { rng.gen_range(lo..=hi) }
}

// ---------------------------------------------------------------------------
// PeerState
// ---------------------------------------------------------------------------

/// Per-peer state tracked by a leader for pull-based replication and by every
/// node for cluster membership awareness.
///
/// Field naming follows the Stage 3.1 specification (`last_fetch_time`,
/// `last_caught_up_time`). The architecture document expresses these as
/// `Instant` to convey monotonic-clock semantics; the engine, however, is
/// deliberately I/O-free and uses the *logical tick* counter (incremented by
/// each [`Input::Tick`]) as its monotonic time source. The field type here
/// is therefore `u64` ticks rather than `std::time::Instant` — this is the
/// engine-internal equivalent of the architecture's `Instant` and allows the
/// state machine to be deterministic and replayable without consulting the
/// wall clock. The driver layer in `xraft-server` may translate ticks to
/// wall-clock durations for metrics / observability when needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerState {
    /// Highest log offset the leader believes this peer has fetched
    /// (the pull-based equivalent of textbook Raft's `match_index`).
    pub last_fetch_offset: LogIndex,
    /// Logical-tick timestamp of this peer's most recent Fetch request.
    /// Used by Check-Quorum (Stage 6) to detect partitioned followers.
    /// Spec name: `last_fetch_time` (architecture.md §3.2). The value is
    /// the engine's logical tick count, not wall clock.
    pub last_fetch_time: u64,
    /// Logical-tick timestamp at which this peer last reached the leader's
    /// log end. Used to gate leadership-transfer and membership-change
    /// protocols. Spec name: `last_caught_up_time` (architecture.md §3.2).
    pub last_caught_up_time: u64,
    /// Whether this peer participates in quorum decisions (false for
    /// `Observer` nodes — non-voting replicas).
    pub is_voter: bool,
}

impl PeerState {
    /// Build an initial follower-progress record for a peer of the given
    /// voter status. Used by `become_leader` to initialise replication
    /// progress for every known peer.
    pub fn new(is_voter: bool) -> Self {
        Self {
            last_fetch_offset: LogIndex(0),
            last_fetch_time: 0,
            last_caught_up_time: 0,
            is_voter,
        }
    }
}

// ---------------------------------------------------------------------------
// RaftNode
// ---------------------------------------------------------------------------

/// Core Raft consensus state machine.
///
/// Processes [`Input`] events and produces a list of side-effect [`Action`]s
/// the driver must execute. This separation keeps the consensus engine pure
/// and deterministic for testing.
///
/// # Driver contract
///
/// The driver layer (in `xraft-server`) is responsible for:
/// 1. Persisting any [`Action::PersistHardState`] before sending RPC replies
///    or another `Input` into the node (Raft safety invariant).
/// 2. Persisting [`Action::AppendEntries`] to the durable [`LogStore`](crate::storage::LogStore)
///    before treating those entries as part of the local log.
/// 3. Mirroring `LogStore::last_index` and `LogStore::last_term` back into
///    the node via [`RaftNode::set_last_log`] after each append (or on
///    startup recovery).
/// 4. Dispatching [`Action::SendMessage`] over the [`Transport`](crate::transport::Transport).
///
/// If any persistence operation fails, the driver MUST halt the node and
/// recover from durable state on restart — partial application of an action
/// list is unsafe.
#[derive(Debug)]
pub struct RaftNode {
    /// This node's identity.
    pub id: NodeId,
    /// Current role in the cluster.
    pub role: NodeRole,
    /// Durable state: current term + vote.
    pub hard_state: HardState,
    /// Index of the highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of the highest log entry applied to the state machine.
    pub last_applied: LogIndex,
    /// Election timer (resets on valid leader contact and on role change).
    pub election_timer: ElectionTimer,
    /// Set of votes received in the current real election (only meaningful
    /// when role is `Candidate`). Concretely the [`VoteGrantedSet`] type
    /// (the Stage 3.2 deliverable) — its `HashSet`-backed semantics dedupe
    /// duplicate grants from the same voter, so retried responses cannot
    /// be double-counted toward quorum.
    pub votes_received: VoteGrantedSet,
    /// Set of pre-votes received in the current pre-election (only meaningful
    /// when role is `PreCandidate`). The pre-vote equivalent of `votes_received`;
    /// also a [`VoteGrantedSet`] so it dedupes duplicate grants and is
    /// cleared on every role transition.
    pub pre_votes_received: VoteGrantedSet,
    /// Per-peer replication progress and liveness tracking.
    /// Populated from `voter_set` (excluding self) on construction and
    /// re-initialised on `become_leader`.
    pub peers: HashMap<NodeId, PeerState>,
    /// The set of voters in the cluster (KRaft-style structured membership).
    /// `None` when the node was bootstrapped from the legacy flat `peers`
    /// list without structured voter metadata. Election cannot proceed
    /// without a voter set; see [`RaftNode::has_election_quorum`].
    pub voter_set: Option<VoterSet>,
    /// Cluster configuration.
    pub config: ClusterConfig,
    /// Known leader for the current term, if any.
    pub leader_id: Option<NodeId>,
    /// Logical-tick timestamp of the last time we observed positive leader
    /// contact (set on [`become_follower`] with a `Some(leader_id)` argument
    /// and by Stage 3.3's Fetch-response handling when wired). The Pre-Vote
    /// rejection check (`architecture.md` §2.1) consults this rather than
    /// the election timer because the election timer is reset on actions
    /// unrelated to leader contact (e.g. on granting a vote). `None` means
    /// "no leader has ever been observed in the current era".
    pub last_leader_contact_tick: Option<u64>,
    /// Logical tick clock — incremented by every [`Input::Tick`].
    /// Used as the timestamp source for the `last_fetch_time` /
    /// `last_caught_up_time` fields on [`PeerState`].
    pub logical_tick: u64,
    /// In-memory mirror of `LogStore::last_index`. Maintained by the node
    /// itself when `become_leader` appends a no-op; the driver must call
    /// [`RaftNode::set_last_log`] for any other log mutation it performs.
    pub last_log_index: LogIndex,
    /// In-memory mirror of `LogStore::last_term`.
    pub last_log_term: Term,
    /// RNG used to randomise election timeouts. Seeded from the system
    /// entropy by default; tests use [`RaftNode::new_with_seed`] for
    /// deterministic behaviour.
    rng: StdRng,
}

impl RaftNode {
    /// Create a new `RaftNode` in `Follower` state at term 0 with no vote.
    /// The election timer is randomised from system entropy.
    ///
    /// # Errors
    ///
    /// Returns an error if `config.validate()` fails or if
    /// `config.build_voter_set()` fails (e.g. an invalid `directory_id`
    /// UUID, a duplicate voter, or a missing endpoint). Surfacing these
    /// errors at construction time prevents the node from silently
    /// degrading into an unable-to-elect state.
    pub fn new(config: ClusterConfig) -> Result<Self> {
        let mut rng = StdRng::from_entropy();
        Self::new_inner(config, &mut rng)
    }

    /// Create a new `RaftNode` with a deterministic RNG seed. Intended for
    /// tests and deterministic simulation harnesses. Same error semantics
    /// as [`new`](Self::new).
    pub fn new_with_seed(config: ClusterConfig, seed: u64) -> Result<Self> {
        let mut rng = StdRng::seed_from_u64(seed);
        Self::new_inner(config, &mut rng)
    }

    fn new_inner<R: RngCore + ?Sized>(config: ClusterConfig, rng: &mut R) -> Result<Self> {
        // Validate the full configuration first so any misconfiguration is
        // surfaced as a typed `XRaftError::Config` rather than silently
        // degrading the engine into an unable-to-elect state.
        config.validate()?;
        let voter_set = config.build_voter_set()?;
        let mut peers = HashMap::new();
        if let Some(vs) = voter_set.as_ref() {
            for v in vs.voters() {
                if v.node_id != config.node_id {
                    peers.insert(v.node_id, PeerState::new(true));
                }
            }
        }
        // We seed the timer's RNG from the same source so the entire engine is
        // deterministic when constructed via `new_with_seed`.
        let mut timer_rng = StdRng::seed_from_u64(rng.next_u64());
        let election_timer = ElectionTimer::from_config_ms(
            config.election_timeout_min_ms,
            config.election_timeout_max_ms,
            config.tick_interval_ms,
            &mut timer_rng,
        );

        Ok(Self {
            id: config.node_id,
            role: NodeRole::Follower,
            hard_state: HardState {
                current_term: Term(0),
                voted_for: None,
            },
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            election_timer,
            votes_received: VoteGrantedSet::new(),
            pre_votes_received: VoteGrantedSet::new(),
            peers,
            voter_set,
            config,
            leader_id: None,
            last_leader_contact_tick: None,
            logical_tick: 0,
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
            rng: StdRng::seed_from_u64(rng.next_u64()),
        })
    }

    /// The current term this node is in.
    pub fn current_term(&self) -> Term {
        self.hard_state.current_term
    }

    /// Whether this node believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// Mirror an updated `last_log_index` / `last_log_term` from the durable
    /// `LogStore` into the node. The driver calls this after applying an
    /// [`Action::AppendEntries`] (or on startup recovery) so subsequent
    /// election eligibility and replication probes are based on the actual
    /// persisted log state.
    pub fn set_last_log(&mut self, index: LogIndex, term: Term) {
        self.last_log_index = index;
        self.last_log_term = term;
    }

    /// Step the node forward by processing an input event.
    ///
    /// Returns a list of [`Action`]s the driver must execute (persist state,
    /// send messages, apply entries, etc.).
    ///
    /// Stage 3.1 implements the [`Input::Tick`] handler. Stage 3.2 wires the
    /// vote / pre-vote request and response handlers
    /// ([`handle_vote_request`](Self::handle_vote_request),
    /// [`handle_vote_response`](Self::handle_vote_response),
    /// [`handle_pre_vote_request`](Self::handle_pre_vote_request),
    /// [`handle_pre_vote_response`](Self::handle_pre_vote_response)).
    /// `FetchRequest` / `FetchResponse` / `ClientPropose` remain Stage 3.3
    /// territory.
    pub fn step(&mut self, input: Input) -> Vec<Action> {
        match input {
            Input::Tick => self.handle_tick(),
            Input::VoteRequest(req) => self.handle_vote_request(req),
            Input::VoteResponse { from, response } => self.handle_vote_response(from, response),
            Input::PreVoteRequest(req) => self.handle_pre_vote_request(req),
            Input::PreVoteResponse { from, response } => {
                self.handle_pre_vote_response(from, response)
            }
            // Stage 3.3 territory — return no actions for now so the driver
            // can be wired without crashing on out-of-stage inputs.
            Input::FetchRequest(_) | Input::FetchResponse(_) | Input::ClientPropose(_) => {
                Vec::new()
            }
        }
    }

    /// Handle an [`Input::Tick`]: advance the logical clock and check whether
    /// the role-specific election-timeout reaction should fire.
    ///
    /// Per `architecture.md` §5.1 (Leader Election with Pre-Vote) and
    /// `e2e-scenarios.md` Feature 3 (Pre-Vote prevents disruptive elections):
    ///
    /// - **Follower** election timeout → enter `PreCandidate` (no term bump,
    ///   send `PreVoteRequest`s). The actual term increment happens only
    ///   after a quorum of pre-votes is received in Stage 3.2.
    /// - **PreCandidate** election timeout → restart the Pre-Vote phase by
    ///   re-issuing `PreVoteRequest`s with a fresh randomised timer. Term
    ///   is *not* bumped: the whole point of Pre-Vote is to avoid term
    ///   inflation when the cluster is unreachable.
    /// - **Candidate** election timeout → fall back to Pre-Vote rather than
    ///   straight re-election. A real Candidate that loses contact has the
    ///   same partition-disruption risk as a Follower; routing through
    ///   `PreCandidate` honours the architecture's "no term bump without
    ///   liveness evidence" invariant.
    /// - **Leader** Tick is a no-op at this stage; Check-Quorum (leader
    ///   self-stepdown when partitioned) lands in Stage 6.
    /// - **Observer** Tick is a no-op; observers do not run elections.
    fn handle_tick(&mut self) -> Vec<Action> {
        self.logical_tick = self.logical_tick.saturating_add(1);
        self.election_timer.tick();

        if !self.election_timer.is_expired() {
            return Vec::new();
        }

        match self.role {
            NodeRole::Follower | NodeRole::PreCandidate | NodeRole::Candidate => {
                self.become_pre_candidate()
            }
            NodeRole::Leader | NodeRole::Observer => Vec::new(),
        }
    }

    /// Force this node to begin a real election immediately.
    ///
    /// Implements the Stage 3.2 `start_election()` specification verbatim
    /// (`implementation-plan.md` Stage 3.2):
    /// - Increment `current_term` by 1.
    /// - Vote for self (`voted_for = Some(self.id)`).
    /// - Persist the new hard state (emitted as
    ///   [`Action::PersistHardState`]) before any RPC leaves the box.
    /// - Reset the election timer with a new random timeout.
    /// - Return a list of [`Action::SendMessage`] entries carrying a
    ///   [`VoteRequest`] to every known peer.
    ///
    /// `start_election()` is the **real-election** entrypoint. The
    /// architecturally-correct election flow goes through Pre-Vote first
    /// (see [`become_pre_candidate`](Self::become_pre_candidate) and
    /// `handle_tick`), and `handle_pre_vote_response` cascades into the
    /// real-vote phase via [`become_candidate`](Self::become_candidate).
    /// `start_election()` exists for callers that want to skip Pre-Vote
    /// and immediately run a real election (e.g. an operator-triggered
    /// leadership transfer in a future stage); it shares its
    /// implementation with `become_candidate` since both must increment
    /// the term, vote for self, persist, and emit `VoteRequest`s.
    pub fn start_election(&mut self) -> Vec<Action> {
        self.become_candidate()
    }

    /// Step down to (or re-affirm) the `Follower` role.
    ///
    /// `term` is taken as the new current term: if it is greater than the
    /// existing term, the vote is cleared and an [`Action::PersistHardState`]
    /// is emitted (Raft requires a node to persist a term/vote bump before
    /// any RPC reply). Passing the existing term is allowed (e.g. when the
    /// node simply wants to acknowledge a leader it has just discovered) and
    /// produces no `PersistHardState` action.
    ///
    /// The election timer is reset and (re-)randomised so a freshly stepped-
    /// down candidate or leader does not immediately re-trigger an election.
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) -> Vec<Action> {
        let mut actions = Vec::new();
        let prior_role = self.role;

        if term > self.hard_state.current_term {
            self.hard_state.current_term = term;
            self.hard_state.voted_for = None;
            actions.push(Action::PersistHardState);
        }

        let stepping_down = matches!(
            prior_role,
            NodeRole::Leader | NodeRole::Candidate | NodeRole::PreCandidate
        );
        self.role = NodeRole::Follower;
        self.leader_id = leader_id;
        // Record leader contact when transitioning with a known leader so the
        // Pre-Vote rejection window (architecture §2.1) starts from now.
        // When stepping down to `None`, clear the prior contact stamp because
        // we no longer have evidence of a healthy leader.
        if leader_id.is_some() {
            self.last_leader_contact_tick = Some(self.logical_tick);
        } else {
            self.last_leader_contact_tick = None;
        }
        self.votes_received.clear();
        self.pre_votes_received.clear();
        self.election_timer.reset(&mut self.rng);
        if stepping_down {
            actions.push(Action::StepDown);
        }
        tracing::debug!(
            node_id = %self.id,
            new_term = %self.hard_state.current_term,
            new_leader = ?leader_id,
            "became Follower"
        );
        actions
    }

    /// Enter the `PreCandidate` role and emit `PreVoteRequest`s.
    ///
    /// The Pre-Vote phase checks quorum reachability *without* incrementing
    /// the term — preventing a partitioned node that comes back from
    /// disrupting an established leader (per architecture §2.1 and §5.1).
    /// Pre-vote granting / response tallying handlers are implemented in
    /// Stage 3.2.
    ///
    /// On a single-voter cluster the self pre-vote alone constitutes a
    /// pre-election quorum, so the node cascades directly into
    /// [`become_candidate`](Self::become_candidate) (which in turn cascades
    /// into [`become_leader`](Self::become_leader)). Without this cascade a
    /// one-node cluster could never elect under the architecture-correct
    /// Pre-Vote-first routing in [`handle_tick`](Self::handle_tick).
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn become_pre_candidate(&mut self) -> Vec<Action> {
        self.role = NodeRole::PreCandidate;
        self.leader_id = None;
        // Stepping into an election round invalidates any prior leader contact
        // evidence — clear it so subsequent pre-vote requests from peers are
        // judged on whether *they* still see a leader, not on ours.
        self.last_leader_contact_tick = None;
        // Clear any stale real-vote tallies from a prior Candidate phase.
        self.votes_received.clear();
        self.pre_votes_received.clear();
        self.pre_votes_received.insert(self.id);
        self.election_timer.reset(&mut self.rng);

        let next_term = Term(self.hard_state.current_term.0.saturating_add(1));
        let mut actions = Vec::new();
        for peer_id in self.peers.keys().copied() {
            actions.push(Action::SendMessage {
                to: peer_id,
                message: OutboundMessage::PreVoteRequest(PreVoteRequest {
                    cluster_id: self.config.cluster_id.clone(),
                    leader_epoch: 0,
                    next_term,
                    candidate_id: self.id,
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                }),
            });
        }
        tracing::debug!(
            node_id = %self.id,
            next_term = %next_term,
            peers = self.peers.len(),
            "became PreCandidate; emitted PreVoteRequests"
        );
        // Single-voter cascade: self pre-vote is a pre-election quorum.
        if self.has_pre_election_quorum() {
            actions.extend(self.become_candidate());
        }
        actions
    }

    /// Enter the `Candidate` role: increment term, vote for self, persist
    /// hard state, and emit `VoteRequest`s to all peers.
    ///
    /// If the node already has election quorum (single-voter cluster),
    /// chains directly into [`become_leader`](Self::become_leader) and appends its actions.
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn become_candidate(&mut self) -> Vec<Action> {
        self.role = NodeRole::Candidate;
        self.hard_state.current_term = Term(self.hard_state.current_term.0.saturating_add(1));
        self.hard_state.voted_for = Some(self.id);
        self.leader_id = None;
        self.last_leader_contact_tick = None;
        // Clear stale pre-vote tallies from the just-completed Pre-Vote phase
        // and start a fresh real-vote tally seeded with our own self-vote.
        self.pre_votes_received.clear();
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.election_timer.reset(&mut self.rng);

        let mut actions = vec![Action::PersistHardState];
        let term = self.hard_state.current_term;
        for peer_id in self.peers.keys().copied() {
            actions.push(Action::SendMessage {
                to: peer_id,
                message: OutboundMessage::VoteRequest(VoteRequest {
                    cluster_id: self.config.cluster_id.clone(),
                    leader_epoch: 0,
                    term,
                    candidate_id: self.id,
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                }),
            });
        }
        tracing::info!(
            node_id = %self.id,
            new_term = %term,
            peers = self.peers.len(),
            "became Candidate; emitted VoteRequests"
        );
        if self.has_election_quorum() {
            actions.extend(self.become_leader());
        }
        actions
    }

    /// Enter the `Leader` role: initialise per-peer replication state and
    /// append a no-op entry to commit any prior-term entries (the standard
    /// Raft "leader completeness" technique).
    ///
    /// Emits `Action::BecomeLeader` followed by `Action::AppendEntries`
    /// containing the no-op entry. The no-op is recorded at
    /// `last_log_index + 1` with the current term; the in-memory mirror is
    /// updated immediately so subsequent state-machine reasoning sees the
    /// new tail. The driver MUST persist the no-op via the durable
    /// `LogStore` before feeding any further input into the node.
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn become_leader(&mut self) -> Vec<Action> {
        self.role = NodeRole::Leader;
        self.leader_id = Some(self.id);
        // We are now the leader — record self-contact so any pre-vote we
        // receive while leader is rejected as "leader is recently active".
        self.last_leader_contact_tick = Some(self.logical_tick);
        // Clear vote tallies — they are no longer meaningful once we have
        // crossed into the Leader role for the current term.
        self.votes_received.clear();
        self.pre_votes_received.clear();
        self.election_timer.reset(&mut self.rng);

        // Initialise per-peer replication state. In the pull model the leader
        // does not yet know any follower's progress, so `last_fetch_offset`
        // is reset to zero; the timestamp fields use the current logical
        // tick as a "started observing" baseline for Check-Quorum.
        for peer in self.peers.values_mut() {
            peer.last_fetch_offset = LogIndex(0);
            peer.last_fetch_time = self.logical_tick;
            peer.last_caught_up_time = self.logical_tick;
        }

        // Append a no-op entry so the leader can commit at least one entry
        // in its term (required to commit prior-term entries safely under
        // Raft Figure 8).
        let noop_index = LogIndex(self.last_log_index.0.saturating_add(1));
        let noop_term = self.hard_state.current_term;
        let noop_entry = Entry {
            index: noop_index,
            term: noop_term,
            payload: EntryPayload::NoOp,
        };
        self.last_log_index = noop_index;
        self.last_log_term = noop_term;

        tracing::info!(
            node_id = %self.id,
            term = %noop_term,
            noop_index = %noop_index,
            peers = self.peers.len(),
            "became Leader; emitted no-op AppendEntries"
        );

        vec![
            Action::BecomeLeader,
            Action::AppendEntries(vec![noop_entry]),
        ]
    }

    /// Whether the votes already collected by this candidate constitute a
    /// quorum. Quorum is computed over **unique voter `NodeId`s** (matching
    /// KRaft semantics — see [`VoterSet::quorum_size`]) so a single broker
    /// with multiple log directories still counts as one vote.
    ///
    /// Returns `false` whenever the node has no structured `voter_set` (the
    /// legacy flat `peers` list does not carry NodeIds and therefore cannot
    /// participate in elections); the node will simply remain a Candidate
    /// until proper structured voter metadata is provided.
    pub fn has_election_quorum(&self) -> bool {
        let Some(vs) = self.voter_set.as_ref() else {
            return false;
        };
        let needed = vs.quorum_size();
        // Only count votes from nodes that are actually voters.
        let voter_ids: std::collections::HashSet<NodeId> =
            vs.voters().iter().map(|v| v.node_id).collect();
        let granted = self
            .votes_received
            .iter()
            .filter(|id| voter_ids.contains(id))
            .count();
        granted >= needed
    }

    /// Whether the pre-votes already collected by this pre-candidate
    /// constitute a quorum. Mirror of [`has_election_quorum`](Self::has_election_quorum)
    /// for the Pre-Vote phase. Used by Stage 3.2.
    pub fn has_pre_election_quorum(&self) -> bool {
        let Some(vs) = self.voter_set.as_ref() else {
            return false;
        };
        let needed = vs.quorum_size();
        let voter_ids: std::collections::HashSet<NodeId> =
            vs.voters().iter().map(|v| v.node_id).collect();
        let granted = self
            .pre_votes_received
            .iter()
            .filter(|id| voter_ids.contains(id))
            .count();
        granted >= needed
    }

    // ---------------------------------------------------------------------
    // Stage 3.2 — Leader Election handlers
    // ---------------------------------------------------------------------

    /// Whether the given `node_id` is in the configured voter set.
    ///
    /// Used to validate the sender of vote / pre-vote messages: non-voter
    /// senders are dropped before they can force a term bump or contribute
    /// to a quorum tally. A node with no `voter_set` configured cannot
    /// participate in elections — every call returns `false` in that case.
    fn is_known_voter(&self, node_id: NodeId) -> bool {
        self.voter_set
            .as_ref()
            .map(|vs| vs.contains(node_id))
            .unwrap_or(false)
    }

    /// Standard Raft up-to-date predicate (architecture.md §6 S4 — Leader
    /// Completeness): the candidate's log is at least as up-to-date as
    /// ours iff its `last_log_term` is strictly greater than ours, or the
    /// terms are equal and its `last_log_index` is at least ours.
    fn candidate_log_is_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        if last_log_term > self.last_log_term {
            return true;
        }
        if last_log_term < self.last_log_term {
            return false;
        }
        last_log_index >= self.last_log_index
    }

    /// Whether this node still considers a leader to be recently active.
    ///
    /// Drives the Pre-Vote rejection rule in `architecture.md` §2.1 / §5.1
    /// and `e2e-scenarios.md` Feature 3
    /// ("Pre-Vote prevents disruptive elections"). We say a leader is
    /// "recently active" when *either* of:
    /// - this node IS the leader (always considered active relative to
    ///   itself); or
    /// - we have an explicit `last_leader_contact_tick` and the elapsed
    ///   logical ticks since that contact are strictly less than the
    ///   current randomized election timeout
    ///   ([`ElectionTimer::timeout_ticks`]). Using the actual randomized
    ///   timeout (rather than just `min_ticks()`) makes the rejection
    ///   window match the receiver's own election timer: while the
    ///   receiver itself would still wait this long before starting an
    ///   election, it must reject pre-votes from peers that might cause
    ///   a disruptive election. Per `implementation-plan.md` Stage 3.2
    ///   and `architecture.md` §5.1 — "followers reject pre-votes if
    ///   they have heard from a leader within the election timeout".
    fn leader_recently_active(&self) -> bool {
        if self.role == NodeRole::Leader {
            return true;
        }
        match self.last_leader_contact_tick {
            Some(t) => {
                let elapsed = self.logical_tick.saturating_sub(t);
                elapsed < self.election_timer.timeout_ticks()
            }
            None => false,
        }
    }

    /// Construct the standard `VoteResponse` envelope carrying this node's
    /// current term, leader-hint, and a granted/denied flag. Used by
    /// [`handle_vote_request`](Self::handle_vote_request).
    fn build_vote_response(&self, granted: bool) -> VoteResponse {
        VoteResponse {
            cluster_id: self.config.cluster_id.clone(),
            leader_epoch: 0,
            term: self.hard_state.current_term,
            vote_granted: granted,
            leader_hint: self.leader_id,
        }
    }

    /// Construct the standard `PreVoteResponse` envelope.
    fn build_pre_vote_response(&self, granted: bool) -> PreVoteResponse {
        PreVoteResponse {
            cluster_id: self.config.cluster_id.clone(),
            leader_epoch: 0,
            term: self.hard_state.current_term,
            vote_granted: granted,
            leader_hint: self.leader_id,
        }
    }

    /// Handle a real-election `VoteRequest`.
    ///
    /// Per `architecture.md` §5.1 and the canonical Raft safety rules:
    /// 1. Reject silently if `cluster_id` does not match (cross-cluster
    ///    misrouting).
    /// 2. Reject silently if the candidate is not in our configured
    ///    voter set — a non-voter must not be able to force a term bump
    ///    on a real voter (rubber-duck blocking issue #2).
    /// 3. If `req.term < current_term`, reply with a denial carrying our
    ///    current term so the stale candidate can step down.
    /// 4. If `req.term > current_term`, adopt the new term, clear our
    ///    vote, step down to follower, and proceed to consider the vote
    ///    in the same `Vec<Action>` so we emit **one** coalesced
    ///    `PersistHardState` covering both the term bump and any vote.
    /// 5. Grant the vote iff `voted_for` is unset (or already this
    ///    candidate, allowing idempotent retries) AND the candidate's
    ///    log is at least as up-to-date as ours.
    /// 6. Granting the vote resets the election timer so we do not start
    ///    our own competing election immediately.
    ///
    /// The returned action vector is ordered `[PersistHardState?,
    /// StepDown?, SendMessage]`. The driver MUST execute them in order
    /// so the hard state is durable before any RPC reply leaves the box
    /// (Raft safety invariant S1 — election safety).
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn handle_vote_request(&mut self, req: VoteRequest) -> Vec<Action> {
        if req.cluster_id != self.config.cluster_id {
            tracing::debug!(
                node_id = %self.id,
                their_cluster = %req.cluster_id,
                our_cluster = %self.config.cluster_id,
                "dropping VoteRequest from foreign cluster"
            );
            return Vec::new();
        }
        if !self.is_known_voter(req.candidate_id) {
            tracing::debug!(
                node_id = %self.id,
                candidate_id = %req.candidate_id,
                "dropping VoteRequest from unknown / non-voter candidate"
            );
            return Vec::new();
        }

        // Stale term: respond with a denial carrying our current term.
        if req.term < self.hard_state.current_term {
            let response = self.build_vote_response(false);
            return vec![Action::SendMessage {
                to: req.candidate_id,
                message: OutboundMessage::VoteResponse(response),
            }];
        }

        let mut actions = Vec::new();
        let mut hard_state_changed = false;
        let mut should_step_down = false;
        let prior_role = self.role;

        // Higher term: adopt it atomically with the (possible) vote grant,
        // so we emit a single coalesced PersistHardState (rubber-duck
        // non-blocking issue #3: avoid double persist).
        if req.term > self.hard_state.current_term {
            self.hard_state.current_term = req.term;
            self.hard_state.voted_for = None;
            hard_state_changed = true;
            should_step_down = matches!(
                prior_role,
                NodeRole::Leader | NodeRole::Candidate | NodeRole::PreCandidate
            );
            self.role = NodeRole::Follower;
            self.leader_id = None;
            self.last_leader_contact_tick = None;
            self.votes_received.clear();
            self.pre_votes_received.clear();
            // Adopting a higher term means our election round is invalidated;
            // start a fresh timer for the new term.
            self.election_timer.reset(&mut self.rng);
        }

        // Decide whether to grant.
        let log_ok = self.candidate_log_is_up_to_date(req.last_log_index, req.last_log_term);
        let vote_ok = match self.hard_state.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };
        let granted = log_ok && vote_ok;

        if granted && self.hard_state.voted_for != Some(req.candidate_id) {
            self.hard_state.voted_for = Some(req.candidate_id);
            hard_state_changed = true;
        }
        if granted {
            // Granting a vote engages us in this election round — reset the
            // timer so we do not immediately start our own competing one.
            self.election_timer.reset(&mut self.rng);
        }

        if hard_state_changed {
            actions.push(Action::PersistHardState);
        }
        if should_step_down {
            actions.push(Action::StepDown);
        }
        let response = self.build_vote_response(granted);
        actions.push(Action::SendMessage {
            to: req.candidate_id,
            message: OutboundMessage::VoteResponse(response),
        });

        tracing::debug!(
            node_id = %self.id,
            candidate_id = %req.candidate_id,
            request_term = %req.term,
            granted,
            log_ok,
            vote_ok,
            "processed VoteRequest"
        );
        actions
    }

    /// Handle a real-election `VoteResponse`.
    ///
    /// Per `architecture.md` §5.1:
    /// 1. Drop silently on `cluster_id` mismatch.
    /// 2. Drop silently if `from` is not a configured voter (rubber-duck
    ///    blocking issue #2: non-voter responses must not bump term or
    ///    contribute to quorum).
    /// 3. If `resp.term > current_term`, step down to follower at the new
    ///    term (term reconciliation; the cluster has moved on).
    /// 4. Otherwise act only while we are a `Candidate` and the response
    ///    matches our current election term. Strict equality is required
    ///    here because real votes are bound to a specific term.
    /// 5. Insert the granter into `votes_received` (idempotent via
    ///    `HashSet`); if a quorum is reached, cascade to
    ///    [`become_leader`](Self::become_leader).
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn handle_vote_response(&mut self, from: NodeId, resp: VoteResponse) -> Vec<Action> {
        if resp.cluster_id != self.config.cluster_id {
            return Vec::new();
        }
        if !self.is_known_voter(from) {
            tracing::debug!(
                node_id = %self.id,
                from = %from,
                "dropping VoteResponse from unknown / non-voter sender"
            );
            return Vec::new();
        }

        if resp.term > self.hard_state.current_term {
            return self.become_follower(resp.term, None);
        }

        if self.role != NodeRole::Candidate || resp.term != self.hard_state.current_term {
            return Vec::new();
        }

        if resp.vote_granted {
            self.votes_received.insert(from);
            if self.has_election_quorum() {
                tracing::info!(
                    node_id = %self.id,
                    term = %self.hard_state.current_term,
                    votes = self.votes_received.len(),
                    "Candidate has election quorum; transitioning to Leader"
                );
                return self.become_leader();
            }
        }
        Vec::new()
    }

    /// Handle a `PreVoteRequest` (speculative election round, no term bump).
    ///
    /// Per `architecture.md` §2.1 / §5.1 and e2e-scenarios.md Feature 3:
    /// 1. Drop silently on `cluster_id` mismatch.
    /// 2. Drop silently if the candidate is not a configured voter.
    /// 3. Grant iff all three hold:
    ///    - `req.next_term > current_term` — the candidate would actually
    ///      advance our term in a real election.
    ///    - The candidate's log is at least as up-to-date as ours.
    ///    - We do NOT currently consider a leader to be recently active
    ///      ([`leader_recently_active`](Self::leader_recently_active)).
    ///      This is the disruption-prevention guarantee: a partitioned node
    ///      that comes back must NOT force a healthy leader to step down.
    ///
    /// Pre-vote handling is intentionally side-effect free with respect
    /// to durable state: it MUST NOT mutate `current_term`, `voted_for`,
    /// the election timer, or `leader_id`. The only emitted action is the
    /// reply `SendMessage`.
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn handle_pre_vote_request(&mut self, req: PreVoteRequest) -> Vec<Action> {
        if req.cluster_id != self.config.cluster_id {
            return Vec::new();
        }
        if !self.is_known_voter(req.candidate_id) {
            return Vec::new();
        }

        let term_ok = req.next_term > self.hard_state.current_term;
        let log_ok = self.candidate_log_is_up_to_date(req.last_log_index, req.last_log_term);
        let leader_active = self.leader_recently_active();
        let granted = term_ok && log_ok && !leader_active;

        tracing::debug!(
            node_id = %self.id,
            candidate_id = %req.candidate_id,
            next_term = %req.next_term,
            granted,
            term_ok,
            log_ok,
            leader_active,
            "processed PreVoteRequest"
        );

        let response = self.build_pre_vote_response(granted);
        vec![Action::SendMessage {
            to: req.candidate_id,
            message: OutboundMessage::PreVoteResponse(response),
        }]
    }

    /// Handle a `PreVoteResponse`.
    ///
    /// Per `architecture.md` §5.1:
    /// 1. Drop silently on `cluster_id` / non-voter sender.
    /// 2. If `resp.term > current_term`, step down to follower at the new
    ///    term. This is term *reconciliation*, not inflation: another
    ///    voter has evidence the cluster has advanced.
    /// 3. Otherwise act only while we are a `PreCandidate`. NOTE: We
    ///    deliberately do **not** require `resp.term == current_term` —
    ///    pre-vote responders never bump their term (that is the entire
    ///    point of Pre-Vote), so a lagging voter at a lower term can
    ///    still legitimately grant a pre-vote (rubber-duck blocking
    ///    issue #3). Stale grants from a previous pre-vote round are
    ///    naturally bounded because [`become_pre_candidate`](Self::become_pre_candidate)
    ///    clears `pre_votes_received` at the start of every round.
    /// 4. Insert the granter into `pre_votes_received`; on pre-election
    ///    quorum cascade to [`become_candidate`](Self::become_candidate).
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn handle_pre_vote_response(&mut self, from: NodeId, resp: PreVoteResponse) -> Vec<Action> {
        if resp.cluster_id != self.config.cluster_id {
            return Vec::new();
        }
        if !self.is_known_voter(from) {
            return Vec::new();
        }

        if resp.term > self.hard_state.current_term {
            return self.become_follower(resp.term, None);
        }

        if self.role != NodeRole::PreCandidate {
            return Vec::new();
        }

        if resp.vote_granted {
            self.pre_votes_received.insert(from);
            if self.has_pre_election_quorum() {
                tracing::info!(
                    node_id = %self.id,
                    current_term = %self.hard_state.current_term,
                    pre_votes = self.pre_votes_received.len(),
                    "PreCandidate has pre-election quorum; transitioning to Candidate"
                );
                return self.become_candidate();
            }
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, VoterConfig};
    use crate::error::XRaftError;
    use crate::message::EntryPayload;
    use crate::types::{NodeId, NodeRole, Term};
    use uuid::Uuid;

    /// Minimal config with two flat peers (no structured voters).
    /// Used for Stage 3.1 baseline assertions where election is not exercised.
    fn test_config() -> ClusterConfig {
        ClusterConfig::from_toml_str(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
peers = ["node2:7000", "node3:7001"]
"#,
        )
        .unwrap()
    }

    /// Three-voter structured config (this node = node 1).
    /// Used to exercise election paths that require `voter_set` to be set.
    fn three_voter_config() -> ClusterConfig {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200

[[voters]]
node_id = 1
directory_id = "{}"
host = "node1"
port = 6000

[[voters]]
node_id = 2
directory_id = "{}"
host = "node2"
port = 6001

[[voters]]
node_id = 3
directory_id = "{}"
host = "node3"
port = 6002
"#,
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        ClusterConfig::from_toml_str(&toml).unwrap()
    }

    /// Single-voter structured config (this node = node 1, no peers).
    /// Used to exercise the auto-promote-to-leader path on a one-node cluster.
    fn single_voter_config() -> ClusterConfig {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200

[[voters]]
node_id = 1
directory_id = "{}"
host = "node1"
port = 6000
"#,
            Uuid::new_v4(),
        );
        ClusterConfig::from_toml_str(&toml).unwrap()
    }

    // -------------------------------------------------------------------
    // Stage 3.1 scenario: initial-state
    // -------------------------------------------------------------------

    #[test]
    fn new_node_starts_as_follower() {
        let node = RaftNode::new_with_seed(test_config(), 1).unwrap();
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(0));
        assert!(!node.is_leader());
        assert!(node.leader_id.is_none());
    }

    #[test]
    fn new_node_has_correct_id() {
        let node = RaftNode::new_with_seed(test_config(), 1).unwrap();
        assert_eq!(node.id, NodeId(1));
    }

    #[test]
    fn new_node_starts_with_zero_indices() {
        let node = RaftNode::new_with_seed(test_config(), 1).unwrap();
        assert_eq!(node.commit_index, LogIndex(0));
        assert_eq!(node.last_applied, LogIndex(0));
        assert_eq!(node.last_log_index, LogIndex(0));
        assert_eq!(node.last_log_term, Term(0));
    }

    #[test]
    fn new_node_has_no_votes() {
        let node = RaftNode::new_with_seed(test_config(), 1).unwrap();
        assert!(node.votes_received.is_empty());
        assert!(node.pre_votes_received.is_empty());
    }

    #[test]
    fn new_node_election_timer_running() {
        // initial-state scenario: election timer is initialised and not
        // already expired (a freshly-constructed Follower must not
        // immediately call an election on the first Tick).
        let node = RaftNode::new_with_seed(test_config(), 7).unwrap();
        assert!(!node.election_timer.is_expired());
        assert!(node.election_timer.remaining() > 0);
        assert!(
            node.election_timer.timeout_ticks() >= node.election_timer.min_ticks()
                && node.election_timer.timeout_ticks() <= node.election_timer.max_ticks()
        );
    }

    #[test]
    fn new_node_with_voter_set_initialises_peers() {
        let node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        assert!(node.voter_set.is_some());
        // Peers exclude self.
        assert_eq!(node.peers.len(), 2);
        assert!(node.peers.contains_key(&NodeId(2)));
        assert!(node.peers.contains_key(&NodeId(3)));
        assert!(!node.peers.contains_key(&NodeId(1)));
        for peer in node.peers.values() {
            assert!(peer.is_voter);
            assert_eq!(peer.last_fetch_offset, LogIndex(0));
        }
    }

    #[test]
    fn new_node_without_structured_voters_has_empty_peers() {
        // The legacy flat `peers = [...]` config carries no NodeIds and so
        // cannot populate the structured `peers` map. This is by design;
        // election will refuse to proceed without a voter set.
        let node = RaftNode::new_with_seed(test_config(), 1).unwrap();
        assert!(node.voter_set.is_none());
        assert!(node.peers.is_empty());
    }

    // -------------------------------------------------------------------
    // Stage 3.1 scenario: election-timeout-triggers-candidacy
    // -------------------------------------------------------------------

    #[test]
    fn election_timeout_triggers_candidacy() {
        // Stage 3.1 scenario: election-timeout-triggers-candidacy.
        //
        // Per architecture.md §5.1 and e2e-scenarios.md Feature 3, an
        // election timeout sends a Follower into the Pre-Vote phase
        // (`PreCandidate`) — *not* directly into `Candidate`. The term must
        // NOT be incremented until a quorum of pre-votes is received
        // (Stage 3.2). This protects an established leader from disruption
        // when a partitioned node rejoins.
        //
        // The promotion `PreCandidate -> Candidate` is exercised in a
        // separate test (`pre_candidate_promotes_to_candidate_on_quorum`)
        // because it requires the Stage-3.2 vote-response handler; here we
        // verify the Stage-3.1 contract: `Input::Tick` past the timer must
        // produce a `PreCandidate` with `PreVoteRequest`s and an unchanged
        // term.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 42).unwrap();
        let initial_term = node.current_term();
        assert_eq!(node.role, NodeRole::Follower);

        let max_ticks = node.election_timer.max_ticks() + 5;
        let mut became_pre_candidate_at = None;
        for i in 0..max_ticks {
            let actions = node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate && became_pre_candidate_at.is_none() {
                became_pre_candidate_at = Some(i);
                // No PersistHardState — Pre-Vote does not bump term.
                assert!(
                    !actions
                        .iter()
                        .any(|a| matches!(a, Action::PersistHardState)),
                    "PreCandidate transition must NOT persist hard state \
                     (term unchanged) — got {actions:?}"
                );
                // One PreVoteRequest per peer.
                let pre_votes = actions
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
                assert_eq!(pre_votes, node.peers.len());
                // No real VoteRequest yet — that fires after pre-vote quorum.
                let vote_requests = actions
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
                    vote_requests, 0,
                    "PreCandidate transition must NOT emit real VoteRequests \
                     before pre-vote quorum"
                );
                break;
            }
        }
        let _idx = became_pre_candidate_at.expect(
            "Follower should have transitioned to PreCandidate within max_ticks of election timeout",
        );
        assert_eq!(
            node.current_term(),
            initial_term,
            "Pre-Vote must NOT increment term (architecture.md §5.1)"
        );
        assert_eq!(
            node.hard_state.voted_for, None,
            "Pre-Vote must NOT cast a real vote"
        );
        assert!(node.pre_votes_received.contains(&node.id));
    }

    #[test]
    fn pre_candidate_promotes_to_candidate_on_quorum() {
        // Synthetic complement to `election_timeout_triggers_candidacy`:
        // exercises the second half of the Pre-Vote → Candidate transition
        // by directly invoking `become_candidate()` (the actual response
        // handler that drives this transition is wired in Stage 3.2). The
        // contract verified here is the Stage-3.1 promise that
        // `become_candidate()` increments the term, persists hard state,
        // and emits real `VoteRequest`s once the Pre-Vote phase is over.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 42).unwrap();
        let _ = node.become_pre_candidate();
        let term_before = node.current_term();
        assert_eq!(node.role, NodeRole::PreCandidate);

        let actions = node.become_candidate();
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term().0, term_before.0 + 1);
        assert_eq!(node.hard_state.voted_for, Some(node.id));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        let vote_requests = actions
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
        assert_eq!(vote_requests, node.peers.len());
        // Stale pre-vote tally cleared on entering Candidate.
        assert!(node.pre_votes_received.is_empty());
    }

    #[test]
    fn single_tick_does_not_trigger_election() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.step(Input::Tick);
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(0));
        assert_eq!(node.logical_tick, 1);
        assert_eq!(node.election_timer.elapsed(), 1);
    }

    #[test]
    fn election_timeout_resets_after_role_change() {
        // After becoming candidate via start_election the timer is reset
        // so the node has a fresh window before re-issuing votes.
        // start_election is the real-election entrypoint (increments term,
        // emits VoteRequests). The architecturally-correct Pre-Vote-first
        // path is exercised separately via handle_tick → become_pre_candidate.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let term_before = node.current_term();
        let actions = node.start_election();
        assert_eq!(node.role, NodeRole::Candidate);
        // start_election satisfies the literal Stage 3.2 contract: term bump,
        // self-vote, persist, and one VoteRequest per peer.
        assert_eq!(node.current_term().0, term_before.0 + 1);
        assert_eq!(node.hard_state.voted_for, Some(node.id));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        let vote_requests = actions
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
        assert_eq!(vote_requests, node.peers.len());
        assert!(!node.election_timer.is_expired());
    }

    #[test]
    fn pre_candidate_election_timeout_restarts_pre_vote() {
        // PreCandidate timeout must restart Pre-Vote (re-issue
        // PreVoteRequests with a fresh timer) — NOT increment the term.
        // The whole point of Pre-Vote is to avoid term inflation when the
        // cluster is unreachable.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 7).unwrap();
        let _ = node.become_pre_candidate();
        let term_before = node.current_term();
        assert_eq!(node.role, NodeRole::PreCandidate);

        // Tick past the new pre-candidate timer.
        let max_ticks = node.election_timer.max_ticks() + 5;
        let mut reissued = false;
        for _ in 0..max_ticks {
            let actions = node.step(Input::Tick);
            if !actions.is_empty() {
                // Restart must produce another batch of PreVoteRequests
                // (one per peer) and no PersistHardState / VoteRequest.
                let pre_votes = actions
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
                assert_eq!(pre_votes, node.peers.len());
                assert!(
                    !actions
                        .iter()
                        .any(|a| matches!(a, Action::PersistHardState)),
                    "PreCandidate restart must NOT bump term"
                );
                reissued = true;
                break;
            }
        }
        assert!(reissued, "PreCandidate must re-issue PreVote on timeout");
        assert_eq!(node.role, NodeRole::PreCandidate);
        assert_eq!(
            node.current_term(),
            term_before,
            "PreCandidate timeout must not bump term"
        );
    }

    #[test]
    fn candidate_election_timeout_falls_back_to_pre_vote() {
        // Per architecture's Pre-Vote design a Candidate that loses contact
        // must NOT keep incrementing term every timeout (that would defeat
        // the purpose of Pre-Vote). It falls back to PreCandidate first.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 11).unwrap();
        let _ = node.become_candidate();
        let term_after_candidate = node.current_term();
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(term_after_candidate, Term(1));

        // Tick past the candidate's election timer.
        let max_ticks = node.election_timer.max_ticks() + 5;
        let mut fell_back = false;
        for _ in 0..max_ticks {
            let _ = node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate {
                fell_back = true;
                break;
            }
        }
        assert!(
            fell_back,
            "Candidate timeout must fall back to PreCandidate, got role {:?}",
            node.role
        );
        assert_eq!(
            node.current_term(),
            term_after_candidate,
            "Candidate→PreCandidate fallback must NOT bump term again"
        );
    }

    // -------------------------------------------------------------------
    // Stage 3.1 scenario: become-leader-initializes-peers
    // -------------------------------------------------------------------

    #[test]
    fn become_leader_initialises_peers_and_emits_noop() {
        // become-leader-initializes-peers scenario: when become_leader runs
        // (1) every peer's last_fetch_offset is initialised, and
        // (2) a no-op Action::AppendEntries is emitted.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Drive into a known term so the no-op uses a non-zero term.
        let _ = node.become_candidate();
        let term_before_leader = node.current_term();
        let actions = node.become_leader();

        assert_eq!(node.role, NodeRole::Leader);
        assert_eq!(node.leader_id, Some(node.id));
        assert!(!node.peers.is_empty());
        for (peer_id, peer) in &node.peers {
            assert_eq!(
                peer.last_fetch_offset,
                LogIndex(0),
                "peer {peer_id:?} last_fetch_offset must be initialised to 0"
            );
        }
        // BecomeLeader action emitted.
        assert!(
            actions.iter().any(|a| matches!(a, Action::BecomeLeader)),
            "expected Action::BecomeLeader, got {actions:?}"
        );
        // AppendEntries with a single no-op at last_log_index+1.
        let noop_appended = actions.iter().any(|a| match a {
            Action::AppendEntries(entries) => {
                entries.len() == 1
                    && matches!(entries[0].payload, EntryPayload::NoOp)
                    && entries[0].term == term_before_leader
                    && entries[0].index == LogIndex(1)
            }
            _ => false,
        });
        assert!(
            noop_appended,
            "expected an AppendEntries(no-op) at index 1 with term {term_before_leader:?}, got {actions:?}"
        );
        // In-memory mirror advanced to reflect the no-op.
        assert_eq!(node.last_log_index, LogIndex(1));
        assert_eq!(node.last_log_term, term_before_leader);
    }

    // -------------------------------------------------------------------
    // Single-voter auto-promote
    // -------------------------------------------------------------------

    #[test]
    fn single_voter_cluster_auto_promotes_to_leader() {
        // A one-voter cluster has quorum size 1, so the Candidate already
        // has self-vote majority and become_candidate cascades into
        // become_leader within a single step.
        let mut node = RaftNode::new_with_seed(single_voter_config(), 5).unwrap();
        assert!(node.peers.is_empty());
        let actions = node.become_candidate();
        assert_eq!(node.role, NodeRole::Leader);
        // Both PersistHardState (from candidate) and BecomeLeader present.
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        assert!(actions.iter().any(|a| matches!(a, Action::BecomeLeader)));
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::AppendEntries(es) if es.len() == 1
                && matches!(es[0].payload, EntryPayload::NoOp)
        )));
    }

    #[test]
    fn election_loop_in_single_voter_cluster_via_tick() {
        // The full Pre-Vote-first path (architecture.md §5.1):
        //   Follower ticks until election timer expires
        //     → handle_tick routes to become_pre_candidate
        //     → self pre-vote satisfies pre-election quorum (1-of-1)
        //     → cascades into become_candidate (term++)
        //     → self vote satisfies election quorum (1-of-1)
        //     → cascades into become_leader (no-op AppendEntries)
        // Verifies the end-to-end handle_tick wiring on a one-node cluster
        // honours the Pre-Vote-first contract while still electing in a
        // single tick window.
        let mut node = RaftNode::new_with_seed(single_voter_config(), 9).unwrap();
        let max_ticks = node.election_timer.max_ticks() + 5;
        let mut became_leader = false;
        for _ in 0..max_ticks {
            node.step(Input::Tick);
            if node.role == NodeRole::Leader {
                became_leader = true;
                break;
            }
        }
        assert!(became_leader, "single-voter cluster must elect itself");
        assert_eq!(node.current_term(), Term(1));
        assert_eq!(node.last_log_index, LogIndex(1));
    }

    // -------------------------------------------------------------------
    // Role transition correctness
    // -------------------------------------------------------------------

    #[test]
    fn become_follower_with_higher_term_persists_and_clears_vote() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Bump to term=5, voted_for=self.
        let _ = node.become_candidate();
        let _ = node.become_candidate();
        let _ = node.become_candidate();
        let _ = node.become_candidate();
        let _ = node.become_candidate();
        assert_eq!(node.current_term(), Term(5));
        assert_eq!(node.hard_state.voted_for, Some(node.id));

        let actions = node.become_follower(Term(7), Some(NodeId(3)));
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(7));
        assert_eq!(node.hard_state.voted_for, None);
        assert_eq!(node.leader_id, Some(NodeId(3)));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        assert!(actions.iter().any(|a| matches!(a, Action::StepDown)));
    }

    #[test]
    fn become_follower_same_term_does_not_persist() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.become_follower(Term(0), Some(NodeId(2)));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        // Already a follower; no StepDown either.
        assert!(!actions.iter().any(|a| matches!(a, Action::StepDown)));
        assert_eq!(node.leader_id, Some(NodeId(2)));
    }

    #[test]
    fn become_pre_candidate_emits_pre_vote_requests_without_term_bump() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let term_before = node.current_term();
        let actions = node.become_pre_candidate();
        assert_eq!(node.role, NodeRole::PreCandidate);
        // No term bump, no PersistHardState.
        assert_eq!(node.current_term(), term_before);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        // One PreVoteRequest per peer.
        let pre_votes = actions
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
        assert_eq!(pre_votes, node.peers.len());
        assert!(node.pre_votes_received.contains(&node.id));
    }

    #[test]
    fn become_candidate_increments_term_and_emits_vote_requests() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.become_candidate();
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term(), Term(1));
        assert_eq!(node.hard_state.voted_for, Some(node.id));
        // PersistHardState is the first action (Raft safety: persist before sending RPCs).
        assert!(matches!(actions.first(), Some(Action::PersistHardState)));
        let vote_requests = actions
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
        assert_eq!(vote_requests, node.peers.len());
    }

    #[test]
    fn has_election_quorum_requires_voter_set() {
        // Without structured voters, has_election_quorum is always false.
        let mut node = RaftNode::new_with_seed(test_config(), 1).unwrap();
        node.votes_received.insert(NodeId(1));
        node.votes_received.insert(NodeId(2));
        node.votes_received.insert(NodeId(3));
        assert!(!node.has_election_quorum());
    }

    #[test]
    fn has_election_quorum_three_voter_majority() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Self-vote alone is not quorum (1 of 2 needed = 2 in a 3-node).
        node.votes_received.insert(NodeId(1));
        assert!(!node.has_election_quorum());
        // Two votes (self + one peer) is quorum.
        node.votes_received.insert(NodeId(2));
        assert!(node.has_election_quorum());
    }

    #[test]
    fn has_election_quorum_ignores_non_voters() {
        // Phantom NodeId not in the voter set must not count toward quorum.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.votes_received.insert(NodeId(1));
        node.votes_received.insert(NodeId(99));
        assert!(!node.has_election_quorum());
    }

    // -------------------------------------------------------------------
    // ElectionTimer unit tests
    // -------------------------------------------------------------------

    #[test]
    fn election_timer_uses_ceiling_division() {
        // 151 ms / 100 ms tick = 2 ticks (ceiling), not 1 (floor).
        let mut rng = StdRng::seed_from_u64(0);
        let timer = ElectionTimer::from_config_ms(151, 200, 100, &mut rng);
        assert!(timer.min_ticks() >= 2);
    }

    #[test]
    fn election_timer_clamps_to_min_one_tick() {
        // When tick interval is huge relative to ms, both bounds collapse to 1.
        let mut rng = StdRng::seed_from_u64(0);
        let timer = ElectionTimer::from_config_ms(0, 0, 1000, &mut rng);
        assert_eq!(timer.min_ticks(), 1);
        assert_eq!(timer.max_ticks(), 1);
        assert_eq!(timer.timeout_ticks(), 1);
    }

    #[test]
    fn election_timer_target_within_range() {
        let mut rng = StdRng::seed_from_u64(123);
        for _ in 0..50 {
            let timer = ElectionTimer::from_config_ms(150, 300, 10, &mut rng);
            assert!(timer.timeout_ticks() >= timer.min_ticks());
            assert!(timer.timeout_ticks() <= timer.max_ticks());
        }
    }

    #[test]
    fn election_timer_tick_and_expiry() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut timer = ElectionTimer::new(3, 3, &mut rng);
        assert!(!timer.is_expired());
        assert_eq!(timer.remaining(), 3);
        timer.tick();
        assert_eq!(timer.remaining(), 2);
        timer.tick();
        timer.tick();
        assert!(timer.is_expired());
        assert_eq!(timer.remaining(), 0);
        // Reset re-randomises and zeros elapsed.
        timer.reset(&mut rng);
        assert!(!timer.is_expired());
        assert_eq!(timer.elapsed(), 0);
    }

    #[test]
    fn peer_state_new_initialises_voter_or_observer() {
        let voter = PeerState::new(true);
        assert!(voter.is_voter);
        assert_eq!(voter.last_fetch_offset, LogIndex(0));
        assert_eq!(voter.last_fetch_time, 0);
        assert_eq!(voter.last_caught_up_time, 0);

        let observer = PeerState::new(false);
        assert!(!observer.is_voter);
    }

    #[test]
    fn deterministic_seed_yields_same_election_timeout() {
        let a = RaftNode::new_with_seed(three_voter_config(), 12345).unwrap();
        let b = RaftNode::new_with_seed(three_voter_config(), 12345).unwrap();
        assert_eq!(
            a.election_timer.timeout_ticks(),
            b.election_timer.timeout_ticks(),
            "same seed must produce identical timer randomisation"
        );
    }

    #[test]
    fn set_last_log_updates_mirror() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.set_last_log(LogIndex(42), Term(7));
        assert_eq!(node.last_log_index, LogIndex(42));
        assert_eq!(node.last_log_term, Term(7));
    }

    // -------------------------------------------------------------------
    // Config-error propagation (iter-3 evaluator finding #2)
    // -------------------------------------------------------------------

    #[test]
    fn new_returns_err_on_invalid_voter_directory_id() {
        // `RaftNode::new_with_seed` MUST surface configuration errors
        // rather than silently degrading into an unable-to-elect state
        // (per iter-2 evaluator finding #2). Bypass `from_toml_str` (which
        // performs its own UUID validation) by constructing the
        // `ClusterConfig` struct directly with a syntactically malformed
        // `directory_id`. `build_voter_set` (or `validate`) must reject it
        // and the error must reach the caller of `new_with_seed`.
        let cfg = ClusterConfig {
            node_id: NodeId(1),
            cluster_id: "test".into(),
            listen_addr: "0.0.0.0:6000".into(),
            peers: Vec::new(),
            voters: vec![VoterConfig {
                node_id: 1,
                directory_id: "not-a-valid-uuid".into(),
                host: "node1".into(),
                port: 6000,
            }],
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            fetch_interval_ms: 50,
            tick_interval_ms: 10,
            snapshot_interval: 10_000,
            max_log_entries_before_compaction: 100_000,
            data_dir: std::path::PathBuf::from("data"),
            snapshot_retention_count: 3,
        };
        let err = RaftNode::new_with_seed(cfg, 1).expect_err(
            "RaftNode::new_with_seed must propagate invalid voter config as Err, \
             not silently degrade voter_set to None",
        );
        assert!(
            matches!(err, XRaftError::Config(_)),
            "expected XRaftError::Config, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("directory_id") || msg.contains("UUID"),
            "error message should mention directory_id or UUID, got: {msg}"
        );
    }

    #[test]
    fn new_succeeds_on_valid_voter_config() {
        // Sanity: well-formed structured voter config still constructs OK.
        let node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        assert!(node.voter_set.is_some());
    }

    // -------------------------------------------------------------------
    // Vote-tally hygiene across role transitions
    // -------------------------------------------------------------------

    #[test]
    fn become_candidate_clears_stale_pre_votes() {
        // After PreCandidate quorum lands (Stage 3.2), entering Candidate
        // must drop the now-irrelevant pre-vote tally so subsequent
        // role-state inspection isn't misleading.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_pre_candidate();
        assert!(!node.pre_votes_received.is_empty());
        let _ = node.become_candidate();
        assert!(
            node.pre_votes_received.is_empty(),
            "become_candidate must clear pre_votes_received"
        );
    }

    #[test]
    fn become_leader_clears_vote_tallies() {
        // Once Leader, both pre-vote and real-vote tallies are stale and
        // must be cleared so the next election starts from a clean slate.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        // Synthetically add a peer's vote so the tally has more than self.
        node.votes_received.insert(NodeId(2));
        node.pre_votes_received.insert(NodeId(2));
        let _ = node.become_leader();
        assert!(
            node.votes_received.is_empty(),
            "become_leader must clear votes_received"
        );
        assert!(
            node.pre_votes_received.is_empty(),
            "become_leader must clear pre_votes_received"
        );
    }

    // -------------------------------------------------------------------
    // Stage 3.2 — Leader Election handler tests
    // -------------------------------------------------------------------

    /// Locate the first `VoteResponse` produced in an action list.
    fn extract_vote_response(actions: &[Action]) -> &VoteResponse {
        actions
            .iter()
            .find_map(|a| match a {
                Action::SendMessage {
                    message: OutboundMessage::VoteResponse(r),
                    ..
                } => Some(r),
                _ => None,
            })
            .expect("expected a VoteResponse SendMessage in the action list")
    }

    /// Locate the first `PreVoteResponse` produced in an action list.
    fn extract_pre_vote_response(actions: &[Action]) -> &PreVoteResponse {
        actions
            .iter()
            .find_map(|a| match a {
                Action::SendMessage {
                    message: OutboundMessage::PreVoteResponse(r),
                    ..
                } => Some(r),
                _ => None,
            })
            .expect("expected a PreVoteResponse SendMessage in the action list")
    }

    /// Build a VoteRequest with the given fields.
    fn vote_req(
        cluster_id: &str,
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> VoteRequest {
        VoteRequest {
            cluster_id: cluster_id.into(),
            leader_epoch: 0,
            term: Term(term),
            candidate_id,
            last_log_index: LogIndex(last_log_index),
            last_log_term: Term(last_log_term),
        }
    }

    /// Build a PreVoteRequest with the given fields.
    fn pre_vote_req(
        cluster_id: &str,
        next_term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> PreVoteRequest {
        PreVoteRequest {
            cluster_id: cluster_id.into(),
            leader_epoch: 0,
            next_term: Term(next_term),
            candidate_id,
            last_log_index: LogIndex(last_log_index),
            last_log_term: Term(last_log_term),
        }
    }

    /// Build a VoteResponse the way a voter would reply.
    fn vote_resp(cluster_id: &str, term: u64, granted: bool) -> VoteResponse {
        VoteResponse {
            cluster_id: cluster_id.into(),
            leader_epoch: 0,
            term: Term(term),
            vote_granted: granted,
            leader_hint: None,
        }
    }

    /// Build a PreVoteResponse the way a voter would reply.
    fn pre_vote_resp(cluster_id: &str, term: u64, granted: bool) -> PreVoteResponse {
        PreVoteResponse {
            cluster_id: cluster_id.into(),
            leader_epoch: 0,
            term: Term(term),
            vote_granted: granted,
            leader_hint: None,
        }
    }

    // ---- handle_vote_request --------------------------------------------

    #[test]
    fn scenario_vote_granted_up_to_date() {
        // Scenario: vote-granted-up-to-date
        // Given a Follower at term=3 with log up to (index=10, term=3),
        // When it receives VoteRequest from candidate=2 at term=4 with
        // last_log=(10, 3), Then grant the vote.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.hard_state.current_term = Term(3);
        node.last_log_index = LogIndex(10);
        node.last_log_term = Term(3);

        let actions = node.handle_vote_request(vote_req("test", 4, NodeId(2), 10, 3));

        let resp = extract_vote_response(&actions);
        assert!(resp.vote_granted, "expected vote grant: {actions:?}");
        assert_eq!(resp.term, Term(4));
        assert_eq!(node.current_term(), Term(4));
        assert_eq!(node.hard_state.voted_for, Some(NodeId(2)));
        // PersistHardState must precede SendMessage so durable state lands
        // before the reply leaves the box.
        assert!(matches!(actions.first(), Some(Action::PersistHardState)));
    }

    #[test]
    fn scenario_vote_rejected_stale_term() {
        // Scenario: vote-rejected-stale-term
        // Given a Follower at term=5, When it receives VoteRequest with
        // term=3, Then reject.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.hard_state.current_term = Term(5);

        let actions = node.handle_vote_request(vote_req("test", 3, NodeId(2), 0, 0));

        let resp = extract_vote_response(&actions);
        assert!(!resp.vote_granted);
        assert_eq!(resp.term, Term(5));
        // No PersistHardState — neither term nor vote changed.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
        // voted_for must NOT have been set.
        assert_eq!(node.hard_state.voted_for, None);
        assert_eq!(node.current_term(), Term(5));
    }

    #[test]
    fn scenario_vote_rejected_stale_log() {
        // Scenario: vote-rejected-stale-log
        // Given a Follower at term=3 with last_log=(15, 3), When it
        // receives VoteRequest with last_log_term=2 (any index), Then
        // reject.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.hard_state.current_term = Term(3);
        node.last_log_index = LogIndex(15);
        node.last_log_term = Term(3);

        // Even an enormous index can't save a stale term.
        let actions = node.handle_vote_request(vote_req("test", 4, NodeId(2), 1_000_000, 2));

        let resp = extract_vote_response(&actions);
        assert!(!resp.vote_granted, "stale-log candidate must be rejected");
        // But the higher term still must be adopted (Raft safety: see a
        // higher term → adopt and clear vote, then reject the vote).
        assert_eq!(node.current_term(), Term(4));
        assert_eq!(node.hard_state.voted_for, None);
    }

    #[test]
    fn handle_vote_request_rejects_when_already_voted_other() {
        // At the same term, voted_for=NodeId(2). A new candidate=NodeId(3)
        // asks for a vote — denied (one vote per term safety invariant).
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.hard_state.current_term = Term(4);
        node.hard_state.voted_for = Some(NodeId(2));

        let actions = node.handle_vote_request(vote_req("test", 4, NodeId(3), 0, 0));

        let resp = extract_vote_response(&actions);
        assert!(!resp.vote_granted);
        // voted_for is unchanged.
        assert_eq!(node.hard_state.voted_for, Some(NodeId(2)));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn handle_vote_request_idempotent_re_grant_to_same_candidate() {
        // Same-candidate retry at the same term: re-grant idempotently,
        // and do not emit a redundant PersistHardState.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.hard_state.current_term = Term(4);
        node.hard_state.voted_for = Some(NodeId(2));

        let actions = node.handle_vote_request(vote_req("test", 4, NodeId(2), 0, 0));

        let resp = extract_vote_response(&actions);
        assert!(resp.vote_granted, "re-grant to same candidate must succeed");
        assert_eq!(node.hard_state.voted_for, Some(NodeId(2)));
        // No PersistHardState — voted_for did not actually change.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn handle_vote_request_steps_down_on_higher_term_as_leader() {
        // Leader at term=2 receives VoteRequest at term=5 — step down to
        // follower at term=5 (StepDown action present), then evaluate the
        // vote at term=5.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let _ = node.become_leader();
        assert_eq!(node.role, NodeRole::Leader);
        let starting_term = node.current_term();

        let actions = node.handle_vote_request(vote_req(
            "test",
            starting_term.0 + 4,
            NodeId(2),
            node.last_log_index.0,
            node.last_log_term.0,
        ));

        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(starting_term.0 + 4));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "higher-term VoteRequest must persist the term bump"
        );
        assert!(
            actions.iter().any(|a| matches!(a, Action::StepDown)),
            "Leader stepping down on higher term must emit StepDown"
        );
        // Single coalesced PersistHardState even though both term and
        // voted_for changed (rubber-duck non-blocking issue #3).
        let persist_count = actions
            .iter()
            .filter(|a| matches!(a, Action::PersistHardState))
            .count();
        assert_eq!(
            persist_count, 1,
            "expected exactly one coalesced PersistHardState, got {actions:?}"
        );
    }

    #[test]
    fn handle_vote_request_drops_silently_on_cluster_id_mismatch() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.handle_vote_request(vote_req("other-cluster", 5, NodeId(2), 0, 0));
        assert!(
            actions.is_empty(),
            "cross-cluster VoteRequest must be dropped silently"
        );
        assert_eq!(node.current_term(), Term(0));
    }

    #[test]
    fn handle_vote_request_drops_non_voter_candidate() {
        // A candidate NodeId not in the voter set must NOT force a term
        // bump (rubber-duck blocking issue #2).
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.handle_vote_request(vote_req("test", 99, NodeId(99), 0, 0));
        assert!(actions.is_empty());
        assert_eq!(
            node.current_term(),
            Term(0),
            "non-voter candidate must not force a term bump"
        );
    }

    #[test]
    fn handle_vote_request_grant_resets_election_timer() {
        // Granting a vote resets the election timer so the granter does
        // not immediately race the candidate with its own election.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Burn down some of the timer.
        for _ in 0..node.election_timer.min_ticks().saturating_sub(1) {
            node.election_timer.tick();
        }
        let elapsed_before = node.election_timer.elapsed();
        assert!(elapsed_before > 0);

        let _ = node.handle_vote_request(vote_req("test", 1, NodeId(2), 0, 0));

        assert_eq!(
            node.election_timer.elapsed(),
            0,
            "granting a vote must reset the election timer"
        );
    }

    // ---- handle_vote_response -------------------------------------------

    #[test]
    fn scenario_election_wins_majority() {
        // Scenario: election-wins-majority
        // Given a 5-node cluster where node 1 starts an election at term=2,
        // When nodes 2 and 3 grant votes, Then node 1 becomes Leader.
        let cfg = five_voter_config();
        let mut node = RaftNode::new_with_seed(cfg, 1).unwrap();
        let _ = node.become_candidate();
        let term = node.current_term();
        // Quorum size of 5 = 3. Self counts as 1; one peer grant = 2; two
        // peer grants = 3 = quorum.

        let a = node.handle_vote_response(NodeId(2), vote_resp("test", term.0, true));
        assert_ne!(
            node.role,
            NodeRole::Leader,
            "one peer grant is not yet a quorum"
        );
        assert!(a.is_empty(), "no actions before quorum");

        let a = node.handle_vote_response(NodeId(3), vote_resp("test", term.0, true));
        assert_eq!(node.role, NodeRole::Leader, "two peer grants → quorum");
        assert!(
            a.iter().any(|x| matches!(x, Action::BecomeLeader)),
            "expected Action::BecomeLeader once quorum reached"
        );
        assert!(
            a.iter()
                .any(|x| matches!(x, Action::AppendEntries(es) if !es.is_empty())),
            "expected no-op AppendEntries on becoming Leader"
        );
    }

    #[test]
    fn handle_vote_response_ignores_when_not_candidate() {
        // A Follower receiving a VoteResponse must not act on it.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.handle_vote_response(NodeId(2), vote_resp("test", 0, true));
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::Follower);
    }

    #[test]
    fn handle_vote_response_steps_down_on_higher_term() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let term = node.current_term();
        let actions = node.handle_vote_response(NodeId(2), vote_resp("test", term.0 + 5, false));
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(term.0 + 5));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn handle_vote_response_ignores_stale_term_grant() {
        // A grant from a past election (lower resp.term than current_term)
        // must not be counted toward the current election quorum.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate(); // term=1
        let _ = node.become_pre_candidate(); // back to PreCandidate at term=1
        let _ = node.become_candidate(); // term=2
        assert_eq!(node.current_term(), Term(2));

        let actions = node.handle_vote_response(NodeId(2), vote_resp("test", 1, true));
        assert!(actions.is_empty(), "stale-term grant must not count");
        // Tally still has only self.
        assert_eq!(node.votes_received.len(), 1);
        assert!(node.votes_received.contains(&node.id));
    }

    #[test]
    fn handle_vote_response_ignores_non_voter_sender() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let term = node.current_term();
        // Higher-term response from non-voter must NOT cause step-down.
        let actions = node.handle_vote_response(NodeId(99), vote_resp("test", term.0 + 10, true));
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term(), term);
    }

    #[test]
    fn handle_vote_response_dedupes_double_grant_via_vote_granted_set() {
        // Even if a peer's grant arrives twice, the HashSet semantics of
        // votes_received must prevent double-counting toward quorum.
        let cfg = five_voter_config();
        let mut node = RaftNode::new_with_seed(cfg, 1).unwrap();
        let _ = node.become_candidate();
        let term = node.current_term();

        let _ = node.handle_vote_response(NodeId(2), vote_resp("test", term.0, true));
        let _ = node.handle_vote_response(NodeId(2), vote_resp("test", term.0, true));

        // Tally = {self, NodeId(2)} → 2 of 5 (quorum=3).
        assert_eq!(node.votes_received.len(), 2);
        assert_ne!(
            node.role,
            NodeRole::Leader,
            "double-grant from same peer must NOT inflate the tally to quorum"
        );
    }

    #[test]
    fn handle_vote_response_drops_cluster_id_mismatch() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let actions = node.handle_vote_response(NodeId(2), vote_resp("other", 999, true));
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::Candidate);
    }

    // ---- handle_pre_vote_request ----------------------------------------

    #[test]
    fn handle_pre_vote_request_grants_when_no_leader() {
        // Fresh follower with no known leader and an empty log must grant
        // a pre-vote for a higher next_term and adequate log.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.handle_pre_vote_request(pre_vote_req("test", 1, NodeId(2), 0, 0));
        let resp = extract_pre_vote_response(&actions);
        assert!(resp.vote_granted);
        // Pre-vote handling must NOT mutate term, voted_for, or timer.
        assert_eq!(node.current_term(), Term(0));
        assert_eq!(node.hard_state.voted_for, None);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn scenario_pre_vote_prevents_disruption() {
        // Scenario: pre-vote-prevents-disruption
        // Given a Follower that has just heard from a leader, When it
        // receives a PreVote, Then it rejects (architecture §2.1).
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Acknowledge leader NodeId(3) at the current term.
        let _ = node.become_follower(Term(0), Some(NodeId(3)));
        assert_eq!(node.leader_id, Some(NodeId(3)));
        assert!(node.last_leader_contact_tick.is_some());

        let actions = node.handle_pre_vote_request(pre_vote_req("test", 1, NodeId(2), 0, 0));
        let resp = extract_pre_vote_response(&actions);
        assert!(
            !resp.vote_granted,
            "pre-vote must be rejected while a leader is recently active"
        );
        assert_eq!(
            resp.leader_hint,
            Some(NodeId(3)),
            "leader hint should propagate so the candidate can route requests"
        );
    }

    #[test]
    fn handle_pre_vote_request_grants_after_lease_expires() {
        // After enough ticks have elapsed since the leader contact (more
        // than election_timer.timeout_ticks()), the leader is no longer
        // "recently active" and a pre-vote should be granted. Using the
        // full randomized election timeout (not just min_ticks) matches
        // the architecture rule "within the election timeout" — see
        // `leader_recently_active`.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_follower(Term(0), Some(NodeId(3)));
        let timeout_ticks = node.election_timer.timeout_ticks();
        for _ in 0..(timeout_ticks + 1) {
            node.logical_tick = node.logical_tick.saturating_add(1);
        }
        // leader_id is still Some(3), but the contact is stale.

        let actions = node.handle_pre_vote_request(pre_vote_req("test", 1, NodeId(2), 0, 0));
        let resp = extract_pre_vote_response(&actions);
        assert!(
            resp.vote_granted,
            "pre-vote must be granted once leader contact is no longer recent"
        );
    }

    #[test]
    fn handle_pre_vote_request_rejects_when_self_is_leader() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let _ = node.become_leader();
        assert_eq!(node.role, NodeRole::Leader);
        let term_before = node.current_term();

        let actions =
            node.handle_pre_vote_request(pre_vote_req("test", term_before.0 + 1, NodeId(2), 0, 0));
        let resp = extract_pre_vote_response(&actions);
        assert!(
            !resp.vote_granted,
            "a Leader must reject pre-votes (Pre-Vote disruption guard)"
        );
        assert_eq!(node.role, NodeRole::Leader, "leader must NOT step down");
    }

    #[test]
    fn handle_pre_vote_request_rejects_stale_next_term() {
        // A pre-vote whose next_term is <= our current_term proves the
        // candidate would lose a real election anyway.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.hard_state.current_term = Term(5);
        let actions = node.handle_pre_vote_request(pre_vote_req("test", 5, NodeId(2), 0, 0));
        let resp = extract_pre_vote_response(&actions);
        assert!(
            !resp.vote_granted,
            "pre-vote with next_term <= current_term must be rejected"
        );
    }

    #[test]
    fn handle_pre_vote_request_rejects_stale_log() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        node.last_log_index = LogIndex(15);
        node.last_log_term = Term(3);
        let actions =
            node.handle_pre_vote_request(pre_vote_req("test", 10, NodeId(2), 1_000_000, 2));
        let resp = extract_pre_vote_response(&actions);
        assert!(
            !resp.vote_granted,
            "candidate's stale-log pre-vote must be rejected"
        );
    }

    #[test]
    fn handle_pre_vote_request_does_not_mutate_state() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let term_before = node.current_term();
        let timer_elapsed_before = node.election_timer.elapsed();
        let voted_for_before = node.hard_state.voted_for;

        let _ = node.handle_pre_vote_request(pre_vote_req("test", 7, NodeId(2), 0, 0));

        assert_eq!(node.current_term(), term_before);
        assert_eq!(node.hard_state.voted_for, voted_for_before);
        assert_eq!(node.election_timer.elapsed(), timer_elapsed_before);
    }

    #[test]
    fn handle_pre_vote_request_drops_non_voter_candidate() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let actions = node.handle_pre_vote_request(pre_vote_req("test", 1, NodeId(99), 0, 0));
        assert!(actions.is_empty());
    }

    // ---- handle_pre_vote_response ---------------------------------------

    #[test]
    fn handle_pre_vote_response_quorum_transitions_to_candidate() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_pre_candidate();
        assert_eq!(node.role, NodeRole::PreCandidate);
        let term_before = node.current_term();

        let actions =
            node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", term_before.0, true));
        // Pre-vote quorum (2 of 3) → cascade into Candidate.
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(
            node.current_term().0,
            term_before.0 + 1,
            "Candidate transition must bump term exactly once"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn handle_pre_vote_response_counts_lower_term_grants() {
        // Rubber-duck blocking issue #3: a lagging voter at a lower term
        // can legitimately grant a pre-vote (pre-vote responders do not
        // bump their term). The grant must still count.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Advance to term=5 then enter PreCandidate.
        node.hard_state.current_term = Term(5);
        let _ = node.become_pre_candidate();

        // Voter 2 responds from a stale term=3.
        let _ = node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", 3, true));

        // Quorum (2 of 3) → cascade to Candidate at term 6.
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term(), Term(6));
    }

    #[test]
    fn handle_pre_vote_response_steps_down_on_higher_term() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_pre_candidate();
        let actions = node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", 99, false));
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(99));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn handle_pre_vote_response_ignores_when_not_pre_candidate() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Already Leader.
        let _ = node.become_candidate();
        let _ = node.become_leader();
        let actions = node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", 0, true));
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::Leader);
    }

    #[test]
    fn handle_pre_vote_response_ignores_non_voter() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_pre_candidate();
        let actions = node.handle_pre_vote_response(NodeId(99), pre_vote_resp("test", 999, true));
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::PreCandidate);
        assert_eq!(node.current_term(), Term(0));
    }

    #[test]
    fn handle_pre_vote_response_dedupes_double_grant() {
        // Quorum of 5 = 3 (self + 2). Two grants from the same voter must
        // NOT count as two — the HashSet semantics dedupe.
        let cfg = five_voter_config();
        let mut node = RaftNode::new_with_seed(cfg, 1).unwrap();
        let _ = node.become_pre_candidate();
        let _ = node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", 0, true));
        let _ = node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", 0, true));
        assert_eq!(node.pre_votes_received.len(), 2); // {self, NodeId(2)}
        assert_eq!(
            node.role,
            NodeRole::PreCandidate,
            "duplicate grant must not satisfy quorum prematurely"
        );
    }

    #[test]
    fn handle_pre_vote_response_late_arrival_after_candidate_is_ignored() {
        // A pre-vote response arriving after the node has already moved on
        // to Candidate must NOT cause any state mutation.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let term = node.current_term();
        let actions = node.handle_pre_vote_response(NodeId(2), pre_vote_resp("test", 0, true));
        assert!(actions.is_empty());
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term(), term);
    }

    // ---- step() routing -------------------------------------------------

    #[test]
    fn step_routes_vote_request_and_response() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Drive into Candidate at term=1.
        let _ = node.become_candidate();
        let term = node.current_term();
        // Vote response → quorum on a 3-node cluster (self + one peer).
        let actions = node.step(Input::VoteResponse {
            from: NodeId(2),
            response: vote_resp("test", term.0, true),
        });
        assert_eq!(node.role, NodeRole::Leader);
        assert!(actions.iter().any(|a| matches!(a, Action::BecomeLeader)));
    }

    #[test]
    fn step_routes_pre_vote_request_and_response() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Pre-vote request routing.
        let actions = node.step(Input::PreVoteRequest(pre_vote_req(
            "test",
            1,
            NodeId(2),
            0,
            0,
        )));
        // We expect a single PreVoteResponse SendMessage.
        let _ = extract_pre_vote_response(&actions);

        // Pre-vote response routing: drive into PreCandidate first.
        let _ = node.become_pre_candidate();
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        // Pre-quorum → Candidate.
        assert_eq!(node.role, NodeRole::Candidate);
    }

    // ---- End-to-end election in a 3-voter cluster ----------------------

    #[test]
    fn three_node_cluster_full_election_via_step() {
        // End-to-end: drive the full Pre-Vote → Vote → Leader cascade on
        // node 1 by feeding it Tick (until pre-candidate), then a
        // PreVoteResponse grant (→ Candidate), then a VoteResponse grant
        // (→ Leader). Verifies all four Stage 3.2 handlers are wired via
        // `step()` and interoperate.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 42).unwrap();
        // 1) Tick until the election timer triggers PreCandidate.
        for _ in 0..(node.election_timer.max_ticks() + 5) {
            node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate {
                break;
            }
        }
        assert_eq!(node.role, NodeRole::PreCandidate);
        assert_eq!(node.current_term(), Term(0)); // Pre-vote does NOT bump term.

        // 2) One peer grants the pre-vote → Candidate at term 1.
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term(), Term(1));

        // 3) One peer grants the real vote → Leader.
        let actions = node.step(Input::VoteResponse {
            from: NodeId(3),
            response: vote_resp("test", 1, true),
        });
        assert_eq!(node.role, NodeRole::Leader);
        assert!(actions.iter().any(|a| matches!(a, Action::BecomeLeader)));
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::AppendEntries(es) if es.len() == 1
                && matches!(es[0].payload, EntryPayload::NoOp)
        )));
    }

    /// Five-voter structured config (this node = node 1). Used by Stage 3.2
    /// quorum tests where 3-of-5 votes wins.
    fn five_voter_config() -> ClusterConfig {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200

[[voters]]
node_id = 1
directory_id = "{}"
host = "node1"
port = 6000

[[voters]]
node_id = 2
directory_id = "{}"
host = "node2"
port = 6001

[[voters]]
node_id = 3
directory_id = "{}"
host = "node3"
port = 6002

[[voters]]
node_id = 4
directory_id = "{}"
host = "node4"
port = 6003

[[voters]]
node_id = 5
directory_id = "{}"
host = "node5"
port = 6004
"#,
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        ClusterConfig::from_toml_str(&toml).unwrap()
    }
}
