//! Raft node state machine — core consensus engine.
//!
//! `RaftNode` holds the volatile and durable state for a single Raft participant.
//! It processes [`Input`] events and emits [`Action`] side-effects without
//! performing any I/O itself (I/O is delegated to the driver layer in
//! `xraft-server`).
//!
//! # Stage 3.1 scope
//!
//! This stage establishes the structural foundation:
//! - [`ElectionTimer`] — randomised tick-based timeout in the
//!   `[election_timeout_min_ms, election_timeout_max_ms]` configured range.
//! - [`PeerState`] — per-peer tracking used by the leader to drive
//!   pull-based replication.
//! - [`RaftNode`] role transitions: `become_follower`, `become_pre_candidate`,
//!   `become_candidate`, `become_leader`.
//! - [`RaftNode::step`] handling for [`Input::Tick`]: detects election timeout
//!   on followers/candidates and triggers an election.
//!
//! Pre-Vote vote tallying, the on-receive handlers for `VoteRequest`,
//! `FetchRequest`, etc., and full log-replication progress tracking are
//! Stage 3.2 / Stage 3.3 territory.

use std::collections::{HashMap, HashSet};

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::{Rng, RngCore};

use crate::config::ClusterConfig;
use crate::error::Result;
use crate::message::Entry;
use crate::message::{Action, EntryPayload, Input, OutboundMessage, PreVoteRequest, VoteRequest};
use crate::types::{HardState, LogIndex, NodeId, NodeRole, Term, VoterSet};

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
    /// when role is `Candidate`).
    pub votes_received: HashSet<NodeId>,
    /// Set of pre-votes received in the current pre-election (only meaningful
    /// when role is `PreCandidate`).
    pub pre_votes_received: HashSet<NodeId>,
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
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            peers,
            voter_set,
            config,
            leader_id: None,
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
    /// Stage 3.1 only implements the [`Input::Tick`] handler. The remaining
    /// variants are placeholders filled in by Stage 3.2 (vote handlers) and
    /// Stage 3.3 (fetch handlers).
    pub fn step(&mut self, input: Input) -> Vec<Action> {
        match input {
            Input::Tick => self.handle_tick(),
            // Stage 3.2 / 3.3 territory — return no actions for now so the
            // driver can be wired without crashing on out-of-stage inputs.
            Input::VoteRequest(_)
            | Input::VoteResponse { .. }
            | Input::PreVoteRequest(_)
            | Input::PreVoteResponse { .. }
            | Input::FetchRequest(_)
            | Input::FetchResponse(_)
            | Input::ClientPropose(_) => Vec::new(),
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

    /// Begin the candidacy process by entering the Pre-Vote phase.
    ///
    /// Stage 3.1 routes election-timeout-driven candidacy through
    /// [`become_pre_candidate`](Self::become_pre_candidate) (per
    /// `architecture.md` §5.1). The historical name `start_election` is
    /// retained so existing callers and the planning-doc Stage 3.2 step
    /// "Implement `start_election()` …" still resolves; the body simply
    /// delegates to `become_pre_candidate`.
    pub fn start_election(&mut self) -> Vec<Action> {
        self.become_pre_candidate()
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
        let voter_ids: HashSet<NodeId> = vs.voters().iter().map(|v| v.node_id).collect();
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
        let voter_ids: HashSet<NodeId> = vs.voters().iter().map(|v| v.node_id).collect();
        let granted = self
            .pre_votes_received
            .iter()
            .filter(|id| voter_ids.contains(id))
            .count();
        granted >= needed
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
        // After becoming pre-candidate the timer is reset so the node has
        // a fresh window before re-issuing pre-votes.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // start_election now begins the Pre-Vote phase (per architecture).
        let _ = node.start_election();
        assert_eq!(node.role, NodeRole::PreCandidate);
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
}
