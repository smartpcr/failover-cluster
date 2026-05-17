//! Raft node state machine ΓÇö core consensus engine.
//!
//! `RaftNode` holds the volatile and durable state for a single Raft participant.
//! It processes [`Input`] events and emits [`Action`] side-effects without
//! performing any I/O itself (I/O is delegated to the driver layer in
//! `xraft-server`).
//!
//! # Stage 3.1 / Stage 3.2 scope
//!
//! Stage 3.1 (Raft Node State Machine) established the structural foundation:
//! - [`ElectionTimer`] ΓÇö randomised tick-based timeout in the
//!   `[election_timeout_min_ms, election_timeout_max_ms]` configured range.
//! - [`PeerState`] ΓÇö per-peer tracking used by the leader to drive
//!   pull-based replication.
//! - [`RaftNode`] role transitions: `become_follower`, `become_pre_candidate`,
//!   `become_candidate`, `become_leader`.
//! - [`RaftNode::step`] handling for [`Input::Tick`]: detects election timeout
//!   on followers/candidates and triggers an election.
//!
//! Stage 3.2 (Leader Election) adds the on-receive handlers that drive the
//! full Pre-Vote ΓåÆ Vote ΓåÆ Leader cascade across a real cluster:
//! - [`RaftNode::handle_vote_request`] ΓÇö validate term, log up-to-dateness,
//!   and `voted_for`; grant or reject a real vote with a single coalesced
//!   `PersistHardState` action where applicable.
//! - [`RaftNode::handle_vote_response`] ΓÇö tally votes from voters,
//!   step down on a higher observed term, transition to `Leader` on quorum.
//! - [`RaftNode::handle_pre_vote_request`] ΓÇö speculative-grant check that
//!   does NOT mutate term, `voted_for`, or the election timer. Rejected when
//!   the responder still considers a leader recently active (per
//!   `architecture.md` ┬º2.1 ΓÇö Pre-Vote prevents disruptive elections).
//! - [`RaftNode::handle_pre_vote_response`] ΓÇö tally pre-votes (including
//!   from voters at a lagging term ΓÇö Pre-Vote responders do not bump terms),
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
    Action, EntryPayload, FetchRequest, FetchResponse, FetchSnapshotRequest, Input, LogTruncation,
    OutboundMessage, PreVoteRequest, PreVoteResponse, VoteRequest, VoteResponse,
};
use crate::storage::SnapshotMeta;
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
/// is therefore `u64` ticks rather than `std::time::Instant` ΓÇö this is the
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
    /// Spec name: `last_fetch_time` (architecture.md ┬º3.2). The value is
    /// the engine's logical tick count, not wall clock.
    pub last_fetch_time: u64,
    /// Logical-tick timestamp at which this peer last reached the leader's
    /// log end. Used to gate leadership-transfer and membership-change
    /// protocols. Spec name: `last_caught_up_time` (architecture.md ┬º3.2).
    pub last_caught_up_time: u64,
    /// Stage 7.1 (iter-6 evaluator finding #1) ΓÇö monotonic per-leader
    /// sequence number stamped on every accepted [`FetchRequest`] from
    /// this peer. Distinct from [`Self::last_fetch_time`] which is a
    /// tick timestamp used for lease-window aging: `last_fetch_seq` is
    /// used by the leader-lease *slow-path* (ReadIndex confirmation
    /// round-trip) to prove "this peer has acknowledged my leadership
    /// at or after the moment a pending read was captured", with
    /// strict-greater (`>`) ordering and without the coarse-tick
    /// ambiguity that affects timestamp comparisons. The corresponding
    /// monotonic source is [`RaftNode::fetch_seq`]; on every accepted
    /// `FetchRequest` the leader bumps `self.fetch_seq` and copies the
    /// new value into the requesting peer's `last_fetch_seq`.
    pub last_fetch_seq: u64,
    /// Whether this peer participates in quorum decisions (false for
    /// `Observer` nodes ΓÇö non-voting replicas).
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
            last_fetch_seq: 0,
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
/// recover from durable state on restart ΓÇö partial application of an action
/// list is unsafe.
#[derive(Debug)]
pub struct RaftNode {
    /// This node's identity.
    pub id: NodeId,
    /// Current role in the cluster.
    pub role: NodeRole,
    /// Durable state: current term, vote, and (Stage 7.2 iter-3)
    /// the persisted lower bound on `commit_index`. The recovery
    /// path in `xraft-server::Server::start_with_state_machine`
    /// raises [`Self::commit_index`] from
    /// `hard_state.commit_index` (clamped to the durable log tip)
    /// so a node that restarts with a non-empty log resumes apply
    /// from the same watermark it had pre-crash, rather than
    /// waiting for the leader to re-commit.
    pub hard_state: HardState,
    /// Index of the highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of the highest log entry applied to the state machine.
    pub last_applied: LogIndex,
    /// Election timer (resets on valid leader contact and on role change).
    pub election_timer: ElectionTimer,
    /// Set of votes received in the current real election (only meaningful
    /// when role is `Candidate`). Concretely the [`VoteGrantedSet`] type
    /// (the Stage 3.2 deliverable) ΓÇö its `HashSet`-backed semantics dedupe
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
    /// rejection check (`architecture.md` ┬º2.1) consults this rather than
    /// the election timer because the election timer is reset on actions
    /// unrelated to leader contact (e.g. on granting a vote). `None` means
    /// "no leader has ever been observed in the current era".
    pub last_leader_contact_tick: Option<u64>,
    /// Logical tick clock ΓÇö incremented by every [`Input::Tick`].
    /// Used as the timestamp source for the `last_fetch_time` /
    /// `last_caught_up_time` fields on [`PeerState`].
    pub logical_tick: u64,
    /// In-memory mirror of `LogStore::last_index`. Maintained by the node
    /// itself when `become_leader` appends a no-op; the driver must call
    /// [`RaftNode::set_last_log`] for any other log mutation it performs.
    pub last_log_index: LogIndex,
    /// In-memory mirror of `LogStore::last_term`.
    pub last_log_term: Term,
    /// Index of the no-op entry the leader appended at `become_leader`.
    /// `None` when this node is not the leader (or never has been in the
    /// current term). Used to gate commit advancement so that a freshly
    /// elected leader cannot commit prior-term entries by replication count
    /// alone ΓÇö Raft Figure 8 safety: a leader may only commit a prior-term
    /// entry once it has committed at least one current-term entry. The
    /// no-op IS that current-term entry; commit advancement is gated on
    /// `candidate_index >= leader_no_op_index`.
    pub leader_no_op_index: Option<LogIndex>,
    /// Logical-tick timestamp of the last [`FetchRequest`] this node sent
    /// to its leader. `None` means we have never fetched. Used by Stage 3.3
    /// follower-side fetch scheduling: when
    /// `logical_tick - last_fetch_tick >= fetch_interval_ticks`, the next
    /// [`Input::Tick`] emits a fresh `FetchRequest` to `leader_id`. Cleared
    /// to `None` on every role change (so a new follower fetches eagerly).
    pub last_fetch_tick: Option<u64>,
    /// Metadata for the most recent durable snapshot, if any.
    ///
    /// Set on:
    /// - [`Input::SnapshotComplete`] — the driver has finished saving a
    ///   snapshot the engine asked for via [`Action::TakeSnapshot`].
    /// - [`Input::SnapshotInstalled`] — the driver has finished restoring
    ///   a leader-supplied snapshot.
    ///
    /// Stage 5.2 wiring — see `implementation-plan.md` §5.2.
    pub last_snapshot_meta: Option<SnapshotMeta>,
    /// `true` while a previously-emitted [`Action::TakeSnapshot`] has not
    /// yet completed (the driver has not fed back
    /// [`Input::SnapshotComplete`]). Stage 5.2 trigger debouncer (see
    /// `implementation-plan.md` §5.2 step 1 and `maybe_take_snapshot`):
    /// without this flag, every committed entry past the threshold would
    /// re-emit `TakeSnapshot`, drowning the driver in duplicate snapshot
    /// requests.
    ///
    /// Cleared on:
    /// - [`Input::SnapshotComplete`] — the in-flight snapshot finished.
    /// - [`Input::SnapshotInstalled`] — a leader-supplied snapshot
    ///   superseded any in-flight local snapshot.
    ///
    /// On a fail-stop driver halt (e.g. `state_machine.snapshot()`
    /// returns `Err`) the flag stays set, but the driver halts so no
    /// further `step` calls happen — the operator-restart recovery path
    /// recreates the engine from durable state with the flag at default
    /// (`false`).
    pub snapshot_in_flight: bool,
    /// Logical-tick timestamp at which this node became Leader, used as
    /// the "baseline" for Leader-Lease validation. Set to
    /// `Some(logical_tick)` in [`become_leader`] and cleared in
    /// [`become_follower`] / `become_pre_candidate` / `become_candidate`.
    /// Stage 7.1: the lease check requires `peer.last_fetch_time >
    /// leader_started_tick` so a freshly-elected leader does not report a
    /// false-positive lease before any follower has actually fetched
    /// (the pre-stamped `peer.last_fetch_time = logical_tick` in
    /// `become_leader` is a Check-Quorum grace-period baseline, NOT
    /// evidence of real follower contact).
    pub leader_started_tick: Option<u64>,
    /// Logical-tick counter incremented each [`Input::Tick`] while
    /// `role == Leader && config.enable_check_quorum` is true. When the
    /// counter reaches [`Self::check_quorum_interval_ticks`], the engine
    /// runs one Check-Quorum pass (counting voters with recent
    /// `peer.last_fetch_time`) and resets the counter to zero. Cleared
    /// on every role transition so a re-elected leader gets a fresh
    /// grace window before the first check fires.
    pub check_quorum_elapsed_ticks: u64,
    /// Pre-computed Check-Quorum / Leader-Lease window in logical ticks
    /// (ceiling-division of `config.effective_check_quorum_interval_ms`
    /// by `config.tick_interval_ms`, floored at 1). Derived at engine
    /// construction time so the hot tick path is not re-doing the
    /// division.
    pub check_quorum_interval_ticks: u64,
    /// Stage 7.1 (iter-6 evaluator finding #1) ΓÇö monotonic counter
    /// incremented on every accepted inbound [`FetchRequest`]. Used as
    /// the source value stamped into [`PeerState::last_fetch_seq`] so
    /// the driver's lease *slow-path* (ReadIndex round-trip) can prove
    /// quorum acknowledgement strictly after a pending read was
    /// captured. Unlike a tick-based timestamp, this counter advances
    /// once per RPC rather than once per tick, so within-tick fetches
    /// are still distinguishable. Never reset on role transitions
    /// (u64-monotonic across the node's lifetime).
    pub fetch_seq: u64,
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
        // Stage 7.2: emit a warning when the cluster is configured with
        // an even number of unique voters. An even voter count provides
        // strictly worse per-node fault tolerance than the next-lower
        // odd count (e.g. a 4-node cluster still only tolerates 1
        // failure, identical to a 3-node cluster, while consuming an
        // extra machine). The cluster is still allowed to form ΓÇö
        // operators occasionally run even-sized clusters during planned
        // membership transitions ΓÇö but the warn is the single
        // canonical signal that surfaces the misconfiguration in logs.
        if let Some(vs) = voter_set.as_ref() {
            let n = vs.unique_node_count();
            if n > 0 && n % 2 == 0 {
                tracing::warn!(
                    target: "xraft::node::bootstrap",
                    node_id = config.node_id.0,
                    voter_count = n,
                    "voter set has an even number of voters ({n}); this provides no \
                     improvement in fault tolerance over the next-lower odd count and \
                     is not recommended for production"
                );
            }
        }
        let mut peers = HashMap::new();
        if let Some(vs) = voter_set.as_ref() {
            for v in vs.voters() {
                if v.node_id != config.node_id {
                    peers.insert(v.node_id, PeerState::new(true));
                }
            }
        }
        // Stage 7.2 — observer registration: configured observers are
        // non-voting peers that pull the log via Fetch RPCs. The
        // leader's known-sender guard in `handle_fetch_request` and
        // `handle_fetch_request_acked` rejects any sender that is not
        // a voter AND not present in `peers`. Seeding observers here
        // (as `PeerState::new(false)`) is what makes the leader
        // accept their Fetch RPCs while still excluding them from
        // quorum computation (see `try_advance_commit_index` which
        // explicitly skips peers with `is_voter == false`). Skip
        // self — a node never tracks a `PeerState` for itself.
        // Also skip any observer id that happens to overlap a voter
        // (`NodeConfig::validate_membership` already rejects that for
        // the local node; this is a belt-and-braces guard for the
        // remote-observer case so a misconfiguration cannot silently
        // demote a voter to non-voting status by overwriting its
        // `PeerState`).
        for observer_id in &config.observers {
            let nid = NodeId(*observer_id);
            if nid == config.node_id {
                continue;
            }
            peers.entry(nid).or_insert_with(|| PeerState::new(false));
        }
        // Stage 6.1: seed the local node's initial role as Observer
        // when its `node_id` appears in `config.observers`. Otherwise
        // every node starts as Follower per the Raft baseline (it may
        // become Candidate/PreCandidate/Leader after the first election
        // timeout). Observer nodes never time out into an election —
        // see the `tick()` arm in this file which skips
        // `become_pre_candidate` for `NodeRole::Observer`.
        let initial_role = if config.observers.contains(&config.node_id.0) {
            NodeRole::Observer
        } else {
            NodeRole::Follower
        };
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
            role: initial_role,
            hard_state: HardState {
                current_term: Term(0),
                voted_for: None,
                commit_index: LogIndex(0),
            },
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            election_timer,
            votes_received: VoteGrantedSet::new(),
            pre_votes_received: VoteGrantedSet::new(),
            peers,
            voter_set,
            leader_id: None,
            last_leader_contact_tick: None,
            logical_tick: 0,
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
            leader_no_op_index: None,
            last_fetch_tick: None,
            last_snapshot_meta: None,
            snapshot_in_flight: false,
            leader_started_tick: None,
            check_quorum_elapsed_ticks: 0,
            // ceil-div so a sub-tick window still translates to at least
            // one tick of grace. Floored at 1 so an over-eager operator
            // cannot collapse the window to 0 ticks (which would fire on
            // every leader tick and likely cause leader churn).
            check_quorum_interval_ticks: {
                let window_ms = config.effective_check_quorum_interval_ms();
                let tick_ms = config.tick_interval_ms.max(1);
                window_ms.div_ceil(tick_ms).max(1)
            },
            fetch_seq: 0,
            config,
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
    /// Stage 3.3 wires the Fetch and ClientPropose handlers
    /// ([`handle_fetch_request`](Self::handle_fetch_request),
    /// [`handle_fetch_request_acked`](Self::handle_fetch_request_acked),
    /// [`handle_fetch_response`](Self::handle_fetch_response),
    /// [`handle_client_propose`](Self::handle_client_propose)).
    pub fn step(&mut self, input: Input) -> Vec<Action> {
        match input {
            Input::Tick => self.handle_tick(),
            Input::VoteRequest(req) => self.handle_vote_request(req),
            Input::VoteResponse { from, response } => self.handle_vote_response(from, response),
            Input::PreVoteRequest(req) => self.handle_pre_vote_request(req),
            Input::PreVoteResponse { from, response } => {
                self.handle_pre_vote_response(from, response)
            }
            Input::FetchRequest(req) => self.handle_fetch_request(req),
            Input::FetchResponse(resp) => self.handle_fetch_response(resp),
            Input::ClientPropose(cmd) => self.handle_client_propose(cmd),
            Input::FetchRequestAcked {
                replica_id,
                confirmed_offset,
            } => self.handle_fetch_request_acked(replica_id, confirmed_offset),
            Input::SnapshotComplete { metadata } => self.handle_snapshot_complete(metadata),
            Input::SnapshotInstalled { metadata } => self.handle_snapshot_installed(metadata),
            Input::FetchSnapshotReceived { metadata, data } => {
                self.handle_fetch_snapshot_received(metadata, data)
            }
        }
    }

    /// Handle an [`Input::Tick`]: advance the logical clock and check whether
    /// the role-specific election-timeout reaction should fire. Stage 3.3
    /// adds follower-side fetch scheduling: a Follower or Observer with a
    /// known `leader_id` emits an `Action::SendMessage` carrying a fresh
    /// [`FetchRequest`] whenever `logical_tick - last_fetch_tick >=
    /// fetch_interval_ticks`. Fetch scheduling runs **before** the
    /// election-timeout check so a busy fetch loop does not race the
    /// election-timer reset path (the reset itself happens inside
    /// [`handle_fetch_response`]).
    ///
    /// Per `architecture.md` ┬º5.1 (Leader Election with Pre-Vote) and
    /// `e2e-scenarios.md` Feature 3 (Pre-Vote prevents disruptive elections):
    ///
    /// - **Follower** election timeout ΓåÆ enter `PreCandidate` (no term bump,
    ///   send `PreVoteRequest`s). The actual term increment happens only
    ///   after a quorum of pre-votes is received in Stage 3.2.
    /// - **PreCandidate** election timeout ΓåÆ restart the Pre-Vote phase by
    ///   re-issuing `PreVoteRequest`s with a fresh randomised timer. Term
    ///   is *not* bumped: the whole point of Pre-Vote is to avoid term
    ///   inflation when the cluster is unreachable.
    /// - **Candidate** election timeout ΓåÆ fall back to Pre-Vote rather than
    ///   straight re-election. A real Candidate that loses contact has the
    ///   same partition-disruption risk as a Follower; routing through
    ///   `PreCandidate` honours the architecture's "no term bump without
    ///   liveness evidence" invariant.
    /// - **Leader** Tick is a no-op at this stage; Check-Quorum (leader
    ///   self-stepdown when partitioned) lands in Stage 6.
    /// - **Observer** Tick is a no-op for elections; observers do not run
    ///   elections. Observers DO participate in fetch scheduling so they can
    ///   replicate the log for read scaling (Stage 3.3).
    fn handle_tick(&mut self) -> Vec<Action> {
        self.logical_tick = self.logical_tick.saturating_add(1);
        self.election_timer.tick();

        let mut actions = Vec::new();

        // Stage 3.3 / Stage 7.2: follower / observer fetch scheduling.
        //
        // Steady-state (leader_id known): emit a directed FetchRequest
        // to the leader on the usual fetch-interval cadence.
        //
        // Bootstrap-discovery (leader_id == None): a freshly-started
        // Follower / Observer has no leader hint until it RECEIVES a
        // `FetchResponse` carrying `leader_id` (set in
        // `handle_fetch_response`). Because KRaft-style replication is
        // pull-based, the engine cannot wait for the leader to push —
        // it must issue Fetches itself to discover who the leader is.
        // Broadcast a Fetch to every voter peer; the actual leader
        // replies with a `FetchResponse` (which sets `leader_id`) and
        // non-leaders silently drop the request (see
        // `handle_fetch_request` at the `self.role != Leader` arm).
        // After the first round of discovery, `last_fetch_tick` is
        // stamped so the broadcast is rate-limited at the same
        // `fetch_interval_ticks` cadence as the steady-state path —
        // there is no fetch storm during bootstrap.
        //
        // The architecture explicitly describes this discovery flow
        // (`architecture.md` §2.5, `e2e-scenarios.md` Feature 11). Without
        // it, a 3-node cluster cannot complete Stage 7.2's
        // `bootstrap-voter-set` acceptance scenario: Node X wins the
        // election but Nodes Y/Z never set `leader_id`, never fetch,
        // and time out into competing elections (leader thrashing).
        if matches!(self.role, NodeRole::Follower | NodeRole::Observer)
            && let Some(req) = self.maybe_build_fetch_request()
        {
            match self.leader_id {
                Some(leader) => {
                    actions.push(Action::SendMessage {
                        to: leader,
                        message: OutboundMessage::FetchRequest(req),
                    });
                    self.last_fetch_tick = Some(self.logical_tick);
                }
                None => {
                    let mut sent = false;
                    for (peer_id, peer_state) in self.peers.iter() {
                        // Defensive self-skip — construction never seeds
                        // self into `peers`, but a future config path
                        // that does would otherwise loopback here.
                        if *peer_id == self.id || !peer_state.is_voter {
                            continue;
                        }
                        actions.push(Action::SendMessage {
                            to: *peer_id,
                            message: OutboundMessage::FetchRequest(req.clone()),
                        });
                        sent = true;
                    }
                    if sent {
                        self.last_fetch_tick = Some(self.logical_tick);
                    }
                }
            }
        }

        // Stage 7.1: leader Check-Quorum tick. Distinct from the election
        // timeout — the leader runs its own coarser-grained reachability
        // check at `check_quorum_interval_ticks` (typically 2× the upper
        // bound of election timeout) and steps down if it cannot count
        // itself plus a majority of voters with recent Fetch activity.
        if self.role == NodeRole::Leader && self.config.enable_check_quorum {
            self.check_quorum_elapsed_ticks = self.check_quorum_elapsed_ticks.saturating_add(1);
            if self.check_quorum_elapsed_ticks >= self.check_quorum_interval_ticks {
                self.check_quorum_elapsed_ticks = 0;
                if !self.check_quorum_reachable() {
                    tracing::warn!(
                        node_id = %self.id,
                        term = %self.hard_state.current_term,
                        interval_ticks = self.check_quorum_interval_ticks,
                        "Check-Quorum failed: leader could not count a majority of voters \
                         with recent Fetch activity; stepping down to Follower"
                    );
                    // Same-term step-down. `become_follower` does NOT emit
                    // PersistHardState when the term is unchanged (we are
                    // already durable at this term + self-vote), but DOES
                    // emit `Action::StepDown` so the driver can drop
                    // leader-side state (waiters, replication-lag metrics).
                    let current_term = self.hard_state.current_term;
                    actions.extend(self.become_follower(current_term, None));
                    return actions;
                }
            }
        }

        if !self.election_timer.is_expired() {
            return actions;
        }

        match self.role {
            NodeRole::Follower | NodeRole::PreCandidate | NodeRole::Candidate => {
                actions.extend(self.become_pre_candidate());
            }
            NodeRole::Leader | NodeRole::Observer => {}
        }
        actions
    }

    /// Convert `config.fetch_interval_ms` to logical ticks (ceiling division,
    /// floored at 1). Used by Stage 3.3 follower fetch scheduling.
    fn fetch_interval_ticks(&self) -> u64 {
        let interval = self.config.tick_interval_ms.max(1);
        self.config.fetch_interval_ms.div_ceil(interval).max(1)
    }

    /// Build the next [`FetchRequest`] to send to the leader, if the fetch
    /// interval has elapsed. Returns `None` when the follower has fetched too
    /// recently. The request asks for entries starting at
    /// `last_log_index + 1` and carries `last_log_term` as the
    /// `last_fetched_epoch` so the leader can detect divergence.
    ///
    /// When `last_fetch_tick` is `None` (a fresh follower / observer that
    /// has just learned of a leader), this method returns a request
    /// immediately rather than waiting for `fetch_interval_ticks` to
    /// elapse: the architecture's intent is "fetch eagerly on the first
    /// opportunity after learning the leader" so a freshly-elected leader
    /// is not delayed by an idle interval before its followers start
    /// catching up. (The corresponding `last_fetch_tick = None` reset on
    /// every `become_follower` / `become_pre_candidate` / `become_candidate`
    /// preserves this guarantee across role transitions.)
    fn maybe_build_fetch_request(&self) -> Option<FetchRequest> {
        let should_fetch = match self.last_fetch_tick {
            None => true,
            Some(t) => self.logical_tick.saturating_sub(t) >= self.fetch_interval_ticks(),
        };
        if !should_fetch {
            return None;
        }
        Some(FetchRequest {
            cluster_id: self.config.cluster_id.clone(),
            leader_epoch: self.hard_state.current_term.0,
            replica_id: self.id,
            fetch_offset: LogIndex(self.last_log_index.0.saturating_add(1)),
            last_fetched_epoch: self.last_log_term,
        })
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
    /// Stage 3.3 additions: `leader_no_op_index` is cleared (the no-op was a
    /// leader-only marker) and `last_fetch_tick` is reset to `None` so the
    /// freshly minted follower fetches eagerly on its next [`Input::Tick`].
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
        // Stage 7.2 (evaluator iter-1 finding #3): the Observer
        // role is a configuration identity, NOT a transient state
        // — a node that booted as Observer (because its `node_id`
        // is in `ClusterConfig.observers`) MUST remain Observer
        // across every higher-term reconciliation. Demoting an
        // Observer to Follower would let the election timer fire
        // and let `handle_tick` enter `become_pre_candidate`,
        // turning the observer into a candidate on the next term
        // bump (violating the "never participates in elections"
        // contract from the workstream brief). Preserve the
        // Observer role here; every other prior role collapses
        // to Follower as the canonical Raft step-down behavior.
        if prior_role == NodeRole::Observer {
            self.role = NodeRole::Observer;
        } else {
            self.role = NodeRole::Follower;
        }
        self.leader_id = leader_id;
        // Record leader contact when transitioning with a known leader so the
        // Pre-Vote rejection window (architecture ┬º2.1) starts from now.
        // When stepping down to `None`, clear the prior contact stamp because
        // we no longer have evidence of a healthy leader.
        if leader_id.is_some() {
            self.last_leader_contact_tick = Some(self.logical_tick);
        } else {
            self.last_leader_contact_tick = None;
        }
        self.votes_received.clear();
        self.pre_votes_received.clear();
        // Stage 3.3: a follower no longer owns the leader-side no-op marker;
        // and a fresh follower must fetch eagerly to catch up ΓÇö reset the
        // fetch scheduling cursor.
        self.leader_no_op_index = None;
        self.last_fetch_tick = None;
        // Stage 7.1 (iter-5 evaluator finding #3): clear leader-only
        // Check-Quorum / Leader-Lease bookkeeping on every step-down.
        // `leader_started_tick` is the lease baseline (Some(tick) only
        // while we are leader) and `check_quorum_elapsed_ticks` is the
        // Check-Quorum interval counter (incremented on Tick while
        // leader); both must reset to their default-construction values
        // so a future re-election starts with a clean slate. The peer
        // sister role-transitions (`become_pre_candidate`,
        // `become_candidate`) already do this; mirroring it here closes
        // the gap on the higher-term VoteRequest / FetchResponse /
        // VoteResponse paths that go through `become_follower`.
        self.leader_started_tick = None;
        self.check_quorum_elapsed_ticks = 0;
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
    /// the term ΓÇö preventing a partitioned node that comes back from
    /// disrupting an established leader (per architecture ┬º2.1 and ┬º5.1).
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
        // evidence ΓÇö clear it so subsequent pre-vote requests from peers are
        // judged on whether *they* still see a leader, not on ours.
        self.last_leader_contact_tick = None;
        // Clear any stale real-vote tallies from a prior Candidate phase.
        self.votes_received.clear();
        self.pre_votes_received.clear();
        self.pre_votes_received.insert(self.id);
        // Stage 3.3: clear the leader-only no-op marker (we are no longer
        // leader) and the fetch-scheduling cursor (an election candidate
        // does not pull from a leader).
        self.leader_no_op_index = None;
        self.last_fetch_tick = None;
        // Stage 7.1: clear leader-side Check-Quorum / Leader-Lease state.
        self.leader_started_tick = None;
        self.check_quorum_elapsed_ticks = 0;
        self.election_timer.reset(&mut self.rng);

        let next_term = Term(self.hard_state.current_term.0.saturating_add(1));
        let mut actions = Vec::new();
        // Stage 7.2 (evaluator iter-1 finding #2): observers are
        // non-voting and MUST NOT participate in elections, neither
        // as candidates nor as solicited grantors. Filter the
        // broadcast to voter peers only so a PreVoteRequest does
        // not even reach an observer (a defense-in-depth on top of
        // `is_known_voter` filtering in `handle_pre_vote_request`).
        for (peer_id, peer_state) in self.peers.iter() {
            if !peer_state.is_voter {
                continue;
            }
            actions.push(Action::SendMessage {
                to: *peer_id,
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
        // Stage 3.3: clear leader-only no-op marker and fetch cursor.
        self.leader_no_op_index = None;
        self.last_fetch_tick = None;
        // Stage 7.1: clear leader-side Check-Quorum / Leader-Lease state
        // (defensive — the candidate path is reachable directly from
        // single-voter pre-vote cascade and from the historical fast-path
        // election trigger).
        self.leader_started_tick = None;
        self.check_quorum_elapsed_ticks = 0;
        self.election_timer.reset(&mut self.rng);

        let mut actions = vec![Action::PersistHardState];
        let term = self.hard_state.current_term;
        // Stage 7.2 (evaluator iter-1 finding #2): observers are
        // non-voting and MUST NOT participate in real elections —
        // mirror `become_pre_candidate`'s voter-only broadcast so
        // a VoteRequest never reaches an observer in the first
        // place (in addition to the `is_known_voter` guard on the
        // receiving side).
        for (peer_id, peer_state) in self.peers.iter() {
            if !peer_state.is_voter {
                continue;
            }
            actions.push(Action::SendMessage {
                to: *peer_id,
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
    ///
    /// Stage 3.3: also records `leader_no_op_index` (used as the Raft
    /// Figure-8 commit gate so prior-term entries cannot be committed by
    /// majority count alone ΓÇö the leader must first commit a current-term
    /// entry, and the no-op IS that current-term entry). On a single-voter
    /// cluster the leader's own no-op already satisfies the quorum, so
    /// commit advancement runs immediately and an [`Action::ApplyToStateMachine`]
    /// for the no-op entry is appended.
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn become_leader(&mut self) -> Vec<Action> {
        self.role = NodeRole::Leader;
        self.leader_id = Some(self.id);
        // We are now the leader ΓÇö record self-contact so any pre-vote we
        // receive while leader is rejected as "leader is recently active".
        self.last_leader_contact_tick = Some(self.logical_tick);
        // Clear vote tallies ΓÇö they are no longer meaningful once we have
        // crossed into the Leader role for the current term.
        self.votes_received.clear();
        self.pre_votes_received.clear();
        // Stage 3.3: a leader does not pull from itself.
        self.last_fetch_tick = None;
        // Stage 7.1: record the logical-tick baseline used by Leader-Lease.
        // `peer.last_fetch_time` is pre-stamped to `logical_tick` below as a
        // Check-Quorum grace baseline, but lease evaluation requires a STRICT
        // `peer.last_fetch_time > leader_started_tick` so the lease only
        // becomes active after at least one real post-election Fetch. This
        // is the rubber-duck Blocker 1 fix: without the strict baseline the
        // lease would be falsely active for one tick after every election.
        self.leader_started_tick = Some(self.logical_tick);
        self.check_quorum_elapsed_ticks = 0;
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
        // Record the no-op index for the Figure-8 commit gate.
        self.leader_no_op_index = Some(noop_index);

        tracing::info!(
            node_id = %self.id,
            term = %noop_term,
            noop_index = %noop_index,
            peers = self.peers.len(),
            "became Leader; emitted no-op AppendEntries"
        );

        let mut actions = vec![
            Action::BecomeLeader,
            Action::AppendEntries(vec![noop_entry]),
        ];

        // Single-voter cluster: the no-op is already replicated to a quorum
        // (just the leader), so commit_index and last_applied can advance
        // immediately without waiting for any peer Fetch. Stage 5.2:
        // `drain_apply_pipeline` also emits `Action::TakeSnapshot` if the
        // post-apply log lag has crossed the configured threshold.
        if self.try_advance_commit_index().is_some() {
            actions.extend(self.drain_apply_pipeline());
        }

        actions
    }

    /// Whether the votes already collected by this candidate constitute a
    /// quorum. Quorum is computed over **unique voter `NodeId`s** (matching
    /// KRaft semantics ΓÇö see [`VoterSet::quorum_size`]) so a single broker
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

    /// Stage 7.1 — Check-Quorum reachability test.
    ///
    /// Returns `true` when this leader counts **itself plus voter peers
    /// whose `last_fetch_time` is within the Check-Quorum window** to a
    /// quorum (i.e. `(n/2)+1` of the configured voter set). Returns
    /// `false` otherwise, signalling the caller (the `handle_tick`
    /// Check-Quorum branch) to step down.
    ///
    /// "Within the window" is defined as
    /// `logical_tick - peer.last_fetch_time < check_quorum_interval_ticks`
    /// (strict `<` — the boundary `==` tick is OUTSIDE the window because
    /// it is exactly `check_quorum_interval_ticks` old). The leader
    /// always counts itself as one of the in-window voters.
    ///
    /// When no `voter_set` is configured (legacy flat `peers` mode),
    /// returns `true` so a node running without structured voter metadata
    /// is never spuriously stepped down by Check-Quorum.
    pub fn check_quorum_reachable(&self) -> bool {
        let Some(vs) = self.voter_set.as_ref() else {
            return true;
        };
        let needed = vs.quorum_size();
        let voter_ids: std::collections::HashSet<NodeId> =
            vs.voters().iter().map(|v| v.node_id).collect();

        // Self always counts when we are still the leader (we are
        // trivially "in contact" with ourselves). The leader-self count
        // is the rubber-duck reminder that a 3-voter cluster needs only
        // ONE other reachable voter for quorum (1 + 1 + 1 = quorum=2 of 3).
        let mut reachable_voters: usize = if voter_ids.contains(&self.id) { 1 } else { 0 };
        let window = self.check_quorum_interval_ticks;
        for (peer_id, peer) in self.peers.iter() {
            if !voter_ids.contains(peer_id) {
                continue;
            }
            // Saturating subtraction guards against logical-tick wrap
            // (which would take centuries at 10ms ticks but stay
            // defensive — overflow here would falsely report a giant
            // age and trip an unnecessary step-down).
            let age = self.logical_tick.saturating_sub(peer.last_fetch_time);
            if age < window {
                reachable_voters = reachable_voters.saturating_add(1);
            }
        }
        reachable_voters >= needed
    }

    /// Stage 7.1 — Leader-Lease evaluation.
    ///
    /// Returns `true` only when **all** of the following hold:
    /// 1. `config.enable_leader_lease` is on.
    /// 2. This node is currently the Leader.
    /// 3. Counting self plus voter peers whose `last_fetch_time` strictly
    ///    exceeds `leader_started_tick` AND is within the
    ///    `check_quorum_interval_ticks` window, the leader can form a
    ///    quorum of the configured voter set.
    ///
    /// Condition (3) is the rubber-duck Blocker 1 fix: `become_leader`
    /// pre-stamps every peer's `last_fetch_time` to the current
    /// `logical_tick` as a Check-Quorum grace baseline. Without the
    /// strict `> leader_started_tick` filter the lease would be falsely
    /// active for the entire first window after every election, before
    /// any follower has actually sent a Fetch RPC.
    ///
    /// Single-voter clusters: self alone is a quorum, so a single-voter
    /// leader with the flag enabled holds the lease at all times while
    /// leading.
    ///
    /// **Internal optimisation only.** Per `tech-spec.md` §2.5 XRAFT v1
    /// does not expose an external client read API; this helper is for
    /// admin status queries and engine-internal `StateMachine` lookups
    /// that want to skip an extra commit-index confirmation round-trip.
    pub fn has_active_lease(&self) -> bool {
        if !self.config.enable_leader_lease {
            return false;
        }
        if self.role != NodeRole::Leader {
            return false;
        }
        let Some(vs) = self.voter_set.as_ref() else {
            return false;
        };
        let Some(started_tick) = self.leader_started_tick else {
            return false;
        };
        let needed = vs.quorum_size();
        let voter_ids: std::collections::HashSet<NodeId> =
            vs.voters().iter().map(|v| v.node_id).collect();
        let mut lease_voters: usize = if voter_ids.contains(&self.id) { 1 } else { 0 };
        let window = self.check_quorum_interval_ticks;
        for (peer_id, peer) in self.peers.iter() {
            if !voter_ids.contains(peer_id) {
                continue;
            }
            // Real post-election fetch evidence required.
            if peer.last_fetch_time <= started_tick {
                continue;
            }
            let age = self.logical_tick.saturating_sub(peer.last_fetch_time);
            if age < window {
                lease_voters = lease_voters.saturating_add(1);
            }
        }
        lease_voters >= needed
    }

    // ---------------------------------------------------------------------
    // Stage 3.2 ΓÇö Leader Election handlers
    // ---------------------------------------------------------------------

    /// Whether the given `node_id` is in the configured voter set.
    ///
    /// Used to validate the sender of vote / pre-vote messages: non-voter
    /// senders are dropped before they can force a term bump or contribute
    /// to a quorum tally. A node with no `voter_set` configured cannot
    /// participate in elections ΓÇö every call returns `false` in that case.
    fn is_known_voter(&self, node_id: NodeId) -> bool {
        self.voter_set
            .as_ref()
            .map(|vs| vs.contains(node_id))
            .unwrap_or(false)
    }

    /// Standard Raft up-to-date predicate (architecture.md ┬º6 S4 ΓÇö Leader
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
    /// Drives the Pre-Vote rejection rule in `architecture.md` ┬º2.1 / ┬º5.1
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
    ///   and `architecture.md` ┬º5.1 ΓÇö "followers reject pre-votes if
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
    /// Per `architecture.md` ┬º5.1 and the canonical Raft safety rules:
    /// 1. Reject silently if `cluster_id` does not match (cross-cluster
    ///    misrouting).
    /// 2. Reject silently if the candidate is not in our configured
    ///    voter set ΓÇö a non-voter must not be able to force a term bump
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
    /// (Raft safety invariant S1 ΓÇö election safety).
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
        // Stage 7.2 (evaluator iter-1 finding #2): observers MUST
        // NOT cast votes in real elections. Reject early — before
        // any term reconciliation — so an observer cannot
        // accidentally grant a vote AND so a higher-term
        // VoteRequest cannot trigger the inline step-down below
        // (which would risk demoting the observer to Follower).
        // The voter-only candidate-side broadcast in
        // `become_candidate` already prevents observers from
        // receiving VoteRequests in well-formed clusters; this
        // guard is defense-in-depth for misrouted / replayed
        // RPCs and across protocol versions.
        if self.role == NodeRole::Observer {
            tracing::debug!(
                node_id = %self.id,
                candidate_id = %req.candidate_id,
                "dropping VoteRequest: this node is an Observer and cannot vote"
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
            // Stage 7.1 (iter-5 evaluator finding #3): the inline
            // higher-term step-down here intentionally bypasses
            // `become_follower` (to coalesce the term-bump + vote-grant
            // into a single PersistHardState — rubber-duck issue #3),
            // so we MUST manually mirror `become_follower`'s leader-
            // only state cleanup. Without this, a stepped-down former
            // leader keeps `leader_started_tick = Some(_)` and the
            // accumulated `check_quorum_elapsed_ticks`, which would
            // make `has_active_lease()` and the next leadership
            // cycle's Check-Quorum window observe stale leader-only
            // bookkeeping (Stage 7.1 field docs at §"Cleared on" call
            // out follower transition as the canonical reset point).
            self.leader_started_tick = None;
            self.check_quorum_elapsed_ticks = 0;
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
            // Granting a vote engages us in this election round ΓÇö reset the
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
    /// Per `architecture.md` ┬º5.1:
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
    /// Per `architecture.md` ┬º2.1 / ┬º5.1 and e2e-scenarios.md Feature 3:
    /// 1. Drop silently on `cluster_id` mismatch.
    /// 2. Drop silently if the candidate is not a configured voter.
    /// 3. Grant iff all three hold:
    ///    - `req.next_term > current_term` ΓÇö the candidate would actually
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
    pub fn handle_pre_vote_request(&self, req: PreVoteRequest) -> Vec<Action> {
        if req.cluster_id != self.config.cluster_id {
            return Vec::new();
        }
        // Stage 7.2 (evaluator iter-1 finding #2): observers MUST
        // NOT grant pre-votes. Pre-vote is the first step toward
        // an election round; an observer issuing a grant could
        // tip a real candidate into believing it has quorum that
        // does not actually include the observer (since
        // `has_pre_election_quorum` correctly does not count
        // non-voter peers, but a quorum-via-self optimistic
        // computation should never have to model this defensively).
        // The voter-only candidate-side broadcast in
        // `become_pre_candidate` already prevents PreVoteRequests
        // from reaching observers in well-formed clusters; this
        // guard is defense-in-depth.
        if self.role == NodeRole::Observer {
            tracing::debug!(
                node_id = %self.id,
                candidate_id = %req.candidate_id,
                "dropping PreVoteRequest: this node is an Observer and cannot vote"
            );
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
    /// Per `architecture.md` ┬º5.1:
    /// 1. Drop silently on `cluster_id` / non-voter sender.
    /// 2. If `resp.term > current_term`, step down to follower at the new
    ///    term. This is term *reconciliation*, not inflation: another
    ///    voter has evidence the cluster has advanced.
    /// 3. Otherwise act only while we are a `PreCandidate`. NOTE: We
    ///    deliberately do **not** require `resp.term == current_term` ΓÇö
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

    // ---------------------------------------------------------------------
    // Stage 3.3 ΓÇö Log Replication handlers
    //
    // The engine remains I/O-free: it does not hold the contents of the log,
    // so it cannot itself materialise [`FetchResponse::entries`], detect
    // log-vs-epoch divergence, or apply committed entries to the state
    // machine. Stage 3.3 introduces three deferred-work `Action` variants
    // and one driver-feedback `Input` variant to bridge the engine and the
    // driver while preserving that pure-state-machine contract:
    //
    // - [`Action::ServeFetch`]: leader hands the driver enough envelope
    //   metadata to materialise a `FetchResponse` from the durable log.
    //   The driver also performs divergence detection (via
    //   `LogStore::term_at(req.fetch_offset - 1)` vs `req.last_fetched_epoch`)
    //   and feeds [`Input::FetchRequestAcked`] back into the engine on
    //   non-diverging reads. Diverging reads emit a `FetchResponse` with
    //   `diverging_epoch = Some(...)` instead and skip the ack.
    //
    // - [`Action::ApplyToStateMachine`]: instructs the driver to read
    //   entries `[from..=to]` from the log and apply them to the state
    //   machine. The engine has already advanced `last_applied` to `to`;
    //   the driver MUST apply the entries (or halt and recover from durable
    //   state on restart). The variant carries indices (not entries) so the
    //   engine stays I/O-free ΓÇö see [`Action::ApplyToStateMachine`] doc
    //   for the rationale.
    //
    // - [`Action::TruncateLog`]: instructs the follower's driver to drop
    //   any entries at or after the given index from the durable log. After
    //   truncation, the driver MUST call [`set_last_log`](Self::set_last_log)
    //   with the actual post-truncation last index/term so the engine's
    //   in-memory mirror reflects durable state.
    //
    // - [`Input::FetchRequestAcked`]: driver-supplied confirmation that a
    //   particular replica has replicated entries up through
    //   `confirmed_offset` (= `req.fetch_offset - 1` after a non-diverging
    //   read). Updating `peer.last_fetch_offset` only on this feedback
    //   (rather than on raw `FetchRequest` arrival) guarantees the leader
    //   never inflates a follower's replication progress on the strength of
    //   an unverified log-tail claim ΓÇö this is a Raft safety invariant
    //   (see rubber-duck blocking issue #1).
    // ---------------------------------------------------------------------

    /// Whether `node_id` is the leader's own id. Used to filter self-fetches
    /// (a leader does not pull from itself).
    fn is_self(&self, node_id: NodeId) -> bool {
        node_id == self.id
    }

    /// Recompute the high watermark from per-peer replication progress.
    ///
    /// Implements the standard Raft commit rule, adapted for the KRaft
    /// pull-based progress representation:
    /// - The leader's own log tail counts as `last_log_index` for itself.
    /// - Each voter peer contributes `peer.last_fetch_offset` (which the
    ///   driver only updates on a *validated* fetch ΓÇö see [`handle_fetch_request_acked`]).
    /// - Non-voter peers (Observers) and any voter without a `PeerState`
    ///   record do NOT contribute to quorum.
    /// - The candidate commit index is the `(quorum_size)`-th largest
    ///   replicated offset.
    /// - Raft Figure-8 safety gate: the candidate index MUST be `>=
    ///   leader_no_op_index`. Until the no-op (a current-term entry) has
    ///   itself been replicated to a majority, no prior-term entry may be
    ///   committed by majority-count alone.
    ///
    /// Returns the new commit index when advancement happened, or `None`
    /// when no advancement is possible. Mutates `self.commit_index` only on
    /// advancement.
    fn try_advance_commit_index(&mut self) -> Option<LogIndex> {
        if self.role != NodeRole::Leader {
            return None;
        }
        let voter_set = self.voter_set.as_ref()?;
        let no_op_index = self.leader_no_op_index?;

        let mut offsets: Vec<LogIndex> = Vec::with_capacity(voter_set.voters().len());
        for v in voter_set.voters() {
            if self.is_self(v.node_id) {
                offsets.push(self.last_log_index);
            } else if let Some(p) = self.peers.get(&v.node_id) {
                if !p.is_voter {
                    continue;
                }
                offsets.push(p.last_fetch_offset);
            } else {
                // Voter unknown to peers map ΓÇö count them as zero so they
                // hold back commit advancement until they are observed.
                offsets.push(LogIndex(0));
            }
        }
        let q = voter_set.quorum_size();
        if offsets.len() < q {
            return None;
        }
        // Sort descending; the q-th value (0-indexed: q-1) is the highest
        // offset replicated to a quorum (including self).
        offsets.sort_by(|a, b| b.cmp(a));
        let candidate = offsets[q - 1];

        if candidate <= self.commit_index {
            return None;
        }
        if candidate < no_op_index {
            // Figure-8 safety: cannot commit a prior-term entry by majority
            // count alone ΓÇö must wait for the current-term no-op to itself
            // be replicated to a quorum.
            return None;
        }
        self.commit_index = candidate;
        tracing::debug!(
            node_id = %self.id,
            new_commit_index = %candidate,
            "leader advanced high watermark"
        );
        Some(candidate)
    }

    /// If `commit_index > last_applied`, build an
    /// [`Action::ApplyToStateMachine`] covering the unapplied range and bump
    /// `last_applied` to `commit_index`. Returns `None` when nothing is
    /// pending.
    ///
    /// The engine bumps `last_applied` optimistically: the driver MUST apply
    /// the entries (or halt and recover from durable state on restart) before
    /// feeding any further input into the node, by the same contract that
    /// requires it to honour [`Action::PersistHardState`] before any RPC
    /// reply.
    ///
    /// Internal helper. Public callers should use
    /// [`apply_committed`](Self::apply_committed), which mirrors the
    /// [`apply_committed()` Stage 3.3 contract](
    /// ../../../../docs/stories/failover-cluster-XRAFT/implementation-plan.md)
    /// and is the symbol the driver dispatches against.
    fn maybe_apply(&mut self) -> Option<Action> {
        if self.commit_index <= self.last_applied {
            return None;
        }
        let from = LogIndex(self.last_applied.0.saturating_add(1));
        let to = self.commit_index;
        self.last_applied = to;
        Some(Action::ApplyToStateMachine { from, to })
    }

    /// Stage 5.2 trigger logic (`implementation-plan.md` §5.2 step 1).
    ///
    /// Returns [`Action::TakeSnapshot`] when:
    /// - no snapshot is currently in flight ([`Self::snapshot_in_flight`] is
    ///   `false`), AND
    /// - `commit_index - last_snapshot_index > config.max_log_entries_before_compaction`.
    ///
    /// Sets `snapshot_in_flight = true` so subsequent `maybe_take_snapshot`
    /// calls (made from later `step`s) won't re-emit the action while the
    /// driver is still working on the previous one. Cleared on
    /// [`Input::SnapshotComplete`] / [`Input::SnapshotInstalled`].
    ///
    /// `through_index` is set to the current `commit_index` — every entry
    /// up to and including this index is durably committed and safe to
    /// fold into the snapshot.
    fn maybe_take_snapshot(&mut self) -> Option<Action> {
        if self.snapshot_in_flight {
            return None;
        }
        let snap_idx = self
            .last_snapshot_meta
            .as_ref()
            .map(|m| m.last_included_index.0)
            .unwrap_or(0);
        let lag = self.commit_index.0.saturating_sub(snap_idx);
        if lag > self.config.max_log_entries_before_compaction {
            self.snapshot_in_flight = true;
            tracing::debug!(
                node_id = %self.id,
                commit_index = %self.commit_index,
                last_snapshot_index = snap_idx,
                threshold = self.config.max_log_entries_before_compaction,
                "snapshot threshold crossed; emitting Action::TakeSnapshot"
            );
            Some(Action::TakeSnapshot {
                through_index: self.commit_index,
            })
        } else {
            None
        }
    }

    /// Stage 5.2 internal helper that bundles the standard "after a
    /// commit-index advance" action sequence:
    /// 1. [`Action::ApplyToStateMachine`] for the newly-committed range
    ///    (via [`Self::maybe_apply`]),
    /// 2. [`Action::TakeSnapshot`] when the post-apply log lag has
    ///    crossed `max_log_entries_before_compaction`
    ///    (via [`Self::maybe_take_snapshot`]).
    ///
    /// Returns the actions in the canonical order so callers can simply
    /// `actions.extend(self.drain_apply_pipeline())`. Empty when nothing
    /// is pending.
    fn drain_apply_pipeline(&mut self) -> Vec<Action> {
        let mut out = Vec::new();
        if let Some(apply) = self.maybe_apply() {
            out.push(apply);
        }
        if let Some(snap) = self.maybe_take_snapshot() {
            out.push(snap);
        }
        out
    }

    /// Stage 3.3 step 5 (`implementation-plan.md` ┬º3.3): emit
    /// [`Action::ApplyToStateMachine`] for every log entry between
    /// `last_applied + 1` and `commit_index`, advancing `last_applied`.
    /// Returns `None` when `last_applied == commit_index` (nothing pending).
    ///
    /// This is the **public** Stage 3.3 entry point a driver can call
    /// directly when it wants to drain the apply pipeline outside of a
    /// regular [`step`](Self::step) call (e.g. during shutdown, snapshot
    /// installation, or after a manual `set_last_log`/`commit_index` repair).
    /// The standard hot path ΓÇö leader after a peer ack, follower after a
    /// `FetchResponse` HW advance, leader after `ClientPropose`,
    /// `become_leader` cascade on a single-voter cluster ΓÇö already calls
    /// the internal helper as part of their action sequence, so the public
    /// method exists primarily as a manual-trigger / re-entry point.
    /// Stage 3.3 step 5 (`implementation-plan.md` §3.3): emit
    /// [`Action::ApplyToStateMachine`] for every log entry between
    /// `last_applied + 1` and `commit_index`, advancing `last_applied`.
    /// Returns `Vec::new()` when `last_applied == commit_index` (nothing
    /// pending).
    ///
    /// This is the **public** Stage 3.3 entry point a driver can call
    /// directly when it wants to drain the apply pipeline outside of a
    /// regular [`step`](Self::step) call (e.g. during shutdown, snapshot
    /// installation, or after a manual `set_last_log`/`commit_index` repair).
    /// The standard hot path — leader after a peer ack, follower after a
    /// `FetchResponse` HW advance, leader after `ClientPropose`,
    /// `become_leader` cascade on a single-voter cluster — already calls
    /// the internal helper as part of their action sequence, so the public
    /// method exists primarily as a manual-trigger / re-entry point.
    ///
    /// Stage 5.2: the returned vector also carries an
    /// [`Action::TakeSnapshot`] when the apply has crossed the snapshot
    /// threshold (`commit_index - last_snapshot_index >
    /// max_log_entries_before_compaction`).
    pub fn apply_committed(&mut self) -> Vec<Action> {
        self.drain_apply_pipeline()
    }

    // ---------------------------------------------------------------------
    // Stage 5.2 — Snapshot Coordination (`implementation-plan.md` §5.2)
    //
    // The engine itself is still I/O-free: it owns no snapshot bytes, no
    // state-machine state, and no `SnapshotStore`. The driver does the
    // actual `state_machine.snapshot()` / `state_machine.restore()` /
    // `SnapshotStore::save_snapshot` calls and then feeds the resulting
    // metadata back into the engine via [`Input::SnapshotComplete`] or
    // [`Input::SnapshotInstalled`]. These handlers update the engine's
    // view of the most recent durable snapshot and, in the
    // `SnapshotComplete` case where the completion actually advances the
    // engine's snapshot anchor, instruct the driver to compact the now-
    // redundant log prefix via
    // [`Action::TruncateLog`](`Action::TruncateLog`) with the
    // [`LogTruncation::PrefixThroughInclusive`] variant. A stale
    // completion (one whose `last_included_index` does not raise the
    // anchor) clears the debouncer but emits no follow-on truncation —
    // the engine already anchors at a fresher index, so instructing the
    // driver to purge through a stale, lower index would express the
    // wrong intent even though prefix purge is idempotent in practice.
    // ---------------------------------------------------------------------

    /// Handle [`Input::SnapshotComplete`]: the driver finished saving a
    /// snapshot the engine asked for. Records the snapshot metadata and,
    /// when the completion actually advances the engine's snapshot
    /// anchor, emits an [`Action::TruncateLog`] of the
    /// [`LogTruncation::PrefixThroughInclusive`] variety so the driver
    /// can compact the log prefix that is now fully covered by the
    /// snapshot. A stale completion (same- or lower-indexed than the
    /// anchor already on record) returns an empty action vec: the
    /// fresher anchor already covers a longer prefix, so emitting a
    /// purge instruction at the stale, lower index would misrepresent
    /// the engine's intent — even though prefix purge is idempotent in
    /// practice. The in-flight debouncer flag is cleared in both
    /// branches so a subsequent threshold crossing can re-emit
    /// [`Action::TakeSnapshot`].
    fn handle_snapshot_complete(&mut self, metadata: SnapshotMeta) -> Vec<Action> {
        // Raise-only update of `last_snapshot_meta`: a same- or
        // lower-indexed completion (e.g. a stale `Input::SnapshotComplete`
        // accidentally delivered after a newer snapshot has already been
        // recorded) must not clobber the fresher anchor. The driver path
        // never replays older completions in practice, but the engine
        // enforces the invariant directly so unit tests and any future
        // alternate driver still see coherent metadata.
        let is_fresh = Self::is_snapshot_meta_newer(self.last_snapshot_meta.as_ref(), &metadata);
        // Stage 5.2 trigger debouncer: a previously-emitted
        // `Action::TakeSnapshot` has now completed; future commit-index
        // advances may emit another `TakeSnapshot` once the lag re-crosses
        // the threshold. Clearing happens in both branches because the
        // driver-side save attempt has resolved one way or the other.
        self.snapshot_in_flight = false;
        if !is_fresh {
            // Stale completion: the engine already anchors at an equal-or-
            // newer snapshot. Emitting `TruncateLog` here would instruct
            // the driver to purge a prefix the engine no longer considers
            // authoritative — the fresher anchor's truncation already
            // covered (or will cover) a longer prefix. Prefix purge is
            // idempotent today, but expressing the wrong intent would
            // bite us once Stage 6.2 wires up physical purging.
            return Vec::new();
        }
        let through = metadata.last_included_index;
        self.last_snapshot_meta = Some(metadata);
        vec![Action::TruncateLog(LogTruncation::PrefixThroughInclusive {
            through_index_inclusive: through,
        })]
    }

    /// Returns `true` when `candidate` should replace `current` as the
    /// node's `last_snapshot_meta` anchor. The rule is "raise-only on
    /// `last_included_index`": `None` is always replaced; `Some(prior)`
    /// is replaced only when `candidate.last_included_index >
    /// prior.last_included_index`. Combined with the engine's existing
    /// raise-only updates of `last_applied` / `commit_index` /
    /// `last_log_*`, this keeps the entire post-snapshot state coherent
    /// even if a stale completion or install is delivered to the engine.
    #[inline]
    fn is_snapshot_meta_newer(current: Option<&SnapshotMeta>, candidate: &SnapshotMeta) -> bool {
        match current {
            None => true,
            Some(prior) => candidate.last_included_index > prior.last_included_index,
        }
    }

    /// Handle [`Input::SnapshotInstalled`]: the driver finished
    /// restoring a leader-supplied snapshot into the state machine and
    /// persisting it to the [`SnapshotStore`](crate::storage::SnapshotStore).
    /// Advances `last_applied` and `commit_index` to the snapshot's
    /// `last_included_index` (no-op if either is already ahead) and
    /// records the metadata.
    ///
    /// The engine deliberately does NOT emit a follow-on
    /// [`Action::TruncateLog`] here: when the driver writes the snapshot
    /// it must also already have purged any stale log entries (the
    /// installed snapshot supersedes them); the engine has no entries
    /// to enforce truncation against on this side of the pipeline.
    fn handle_snapshot_installed(&mut self, metadata: SnapshotMeta) -> Vec<Action> {
        let through = metadata.last_included_index;
        if through > self.last_applied {
            self.last_applied = through;
        }
        if through > self.commit_index {
            self.commit_index = through;
        }
        // The snapshot encodes the log tip at the time it was taken, so
        // the engine's in-memory `last_log_*` mirror must be at least
        // that far along — otherwise a subsequent FetchRequest would
        // claim a position behind the snapshot's coverage and the leader
        // would re-send entries the follower has already absorbed.
        //
        // This is a raise-only safety net: the driver is the authoritative
        // reconciler post-install (it calls `set_last_log(effective_log_tip)`
        // immediately after this `step` returns) and may LOWER `last_log_*`
        // when a mismatched-term wipe leaves the durable log empty. We
        // keep the raise here so that direct `Input::SnapshotInstalled`
        // tests / accidental in-engine callers still get a coherent
        // `last_log_*` view without depending on the driver path.
        if through > self.last_log_index {
            self.last_log_index = through;
            self.last_log_term = metadata.last_included_term;
        }
        // Raise-only update of `last_snapshot_meta`: the driver-side
        // stale-install guard in `handle_install_snapshot` already
        // rejects `metadata.last_included_index <= node.last_applied`
        // before reaching this handler, so this branch is belt-and-
        // braces — it lets the engine's snapshot anchor stay coherent
        // even on direct-step unit tests or any future alternate driver
        // that forgets the install-side guard.
        if Self::is_snapshot_meta_newer(self.last_snapshot_meta.as_ref(), &metadata) {
            self.last_snapshot_meta = Some(metadata);
        }
        // Stage 5.2 trigger debouncer: a leader-supplied snapshot has
        // superseded any in-flight local snapshot; clear the flag so the
        // next threshold crossing can re-emit `Action::TakeSnapshot`.
        self.snapshot_in_flight = false;
        Vec::new()
    }

    /// Handle [`Input::FetchSnapshotReceived`]: the driver has
    /// reassembled a `FetchSnapshot` stream into `(metadata, data)`
    /// and applied the envelope-level fences (cluster_id, leader_epoch,
    /// peer-is-leader, metadata-present). The engine emits exactly one
    /// [`Action::InstallSnapshot`] for the driver to fulfil, OR no
    /// action when the snapshot is stale (its coverage does not advance
    /// `last_applied`).
    ///
    /// **Why route through the engine?** The Stage 5.3 contract
    /// (`implementation-plan.md` §5.2 step 3) requires that "receiving
    /// a FetchSnapshot response produces `Action::InstallSnapshot
    /// { metadata, data }`". By emitting the action here — rather than
    /// having the driver call its install handler directly — the
    /// production path exercises the same action contract as
    /// synthetic / test-injected callers, so a contract regression in
    /// the action arm cannot ship undetected.
    ///
    /// **Engine staleness guard.** The engine refuses to regress
    /// `last_applied` / `commit_index` (see [`handle_snapshot_installed`]),
    /// so a stale snapshot that did reach `state_machine.restore`
    /// would diverge the state machine (older view) from the engine
    /// (newer applied position). Reject those here so the action is
    /// never emitted; the driver's install handler also keeps a
    /// belt-and-braces stale guard for direct `Action::InstallSnapshot`
    /// callers.
    fn handle_fetch_snapshot_received(
        &mut self,
        metadata: SnapshotMeta,
        data: Vec<u8>,
    ) -> Vec<Action> {
        if metadata.last_included_index <= self.last_applied {
            tracing::debug!(
                node_id = %self.id,
                stale_index = %metadata.last_included_index,
                current_last_applied = %self.last_applied,
                "Input::FetchSnapshotReceived ignored: not newer than last_applied"
            );
            return Vec::new();
        }
        vec![Action::InstallSnapshot { metadata, data }]
    }

    /// Handle a `FetchRequest` received by this (leader) node from a
    /// follower or observer.
    ///
    /// Per `architecture.md` ┬º5.2 / ┬º5.4 and the implementation-plan
    /// Stage 3.3 specification:
    /// 1. Drop silently on `cluster_id` mismatch.
    /// 2. If `req.leader_epoch > current_term`, the cluster has advanced
    ///    past us ΓÇö step down to follower at the new term and return the
    ///    step-down actions. We are no longer leader and cannot serve.
    /// 3. If we are not the Leader, drop (the requester's leader-hint
    ///    will route them to the actual leader).
    /// 4. If `req.leader_epoch < current_term`, still serve ΓÇö the response
    ///    will carry our higher `leader_epoch` so the stale follower can
    ///    catch up its term view.
    /// 5. Update `peer.last_fetch_time` to the current logical tick (proof
    ///    of liveness for Check-Quorum) and refresh self-contact so any
    ///    pre-vote we receive while we own the lease is rejected.
    ///    NOTE: `peer.last_fetch_offset` is NOT updated here. It is
    ///    updated only via [`Input::FetchRequestAcked`] after the driver
    ///    has validated the follower's `last_fetched_epoch` against the
    ///    leader's log ΓÇö otherwise a divergent follower could inflate
    ///    quorum (rubber-duck blocking issue #1).
    /// 6. **Stage 5.3 snapshot redirect** (implementation-plan §5.2
    ///    step 4): if `last_snapshot_meta.is_some()` and
    ///    `req.fetch_offset <= snap.last_included_index` and the
    ///    snapshot has a non-empty canonical id, emit an
    ///    [`Action::RedirectToSnapshot`] instead of
    ///    [`Action::ServeFetch`]. The follower's offset falls inside
    ///    the compacted prefix; the only correct response is to send
    ///    it through `FetchSnapshot` to install the snapshot before
    ///    resuming log replication. Mutually exclusive with `ServeFetch`
    ///    for this request.
    /// 7. Otherwise emit an [`Action::ServeFetch`] carrying the envelope
    ///    fields so the driver can construct and dispatch the
    ///    [`FetchResponse`].
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn handle_fetch_request(&mut self, req: FetchRequest) -> Vec<Action> {
        if req.cluster_id != self.config.cluster_id {
            tracing::debug!(
                node_id = %self.id,
                their_cluster = %req.cluster_id,
                our_cluster = %self.config.cluster_id,
                "dropping FetchRequest from foreign cluster"
            );
            return Vec::new();
        }

        // Self-fetch is nonsensical (a leader does not pull from itself).
        // Placed FIRST after cluster_id so a malformed self-loopback with a
        // bogus higher leader_epoch cannot accidentally step ourselves down.
        if self.is_self(req.replica_id) {
            return Vec::new();
        }

        // Stage 3.3 finding-1 fix (iter 4): trust-boundary check ΓÇö only
        // accept FetchRequest from a sender we already recognise as either
        // a configured voter (`is_known_voter`) or a tracked peer
        // (`peers.contains_key`). This guard runs BEFORE the higher-term
        // reconciliation branch below so an unknown same-cluster sender
        // can NEVER force `become_follower(Term(req.leader_epoch), None)`
        // and bump our term / step us down. Mirrors the symmetric
        // unknown-leader guard at the top of `handle_fetch_response`
        // (iter-3 finding-1 fix). Dynamic observer auto-registration is
        // a future-stage concern and belongs in the driver layer, not
        // in this Stage 3.3 hot path.
        if !self.is_known_voter(req.replica_id) && !self.peers.contains_key(&req.replica_id) {
            tracing::warn!(
                node_id = %self.id,
                unknown_replica = %req.replica_id,
                claimed_epoch = req.leader_epoch,
                "dropping FetchRequest from unknown replica (not a voter and not a tracked peer)"
            );
            return Vec::new();
        }

        // Higher-term FetchRequest: a follower has evidence the cluster has
        // advanced past us. Step down (becomes Follower at the new term)
        // and return ΓÇö we cannot serve as leader after that. Reachable
        // only for a sender we already recognise (the known-sender guard
        // above ran first), so an unknown attacker cannot trip this path.
        if req.leader_epoch > self.hard_state.current_term.0 {
            tracing::info!(
                node_id = %self.id,
                their_epoch = req.leader_epoch,
                our_term = %self.hard_state.current_term,
                "FetchRequest carries higher leader_epoch; stepping down"
            );
            return self.become_follower(Term(req.leader_epoch), None);
        }

        if self.role != NodeRole::Leader {
            // Not the leader; the requester should re-route via leader-hint.
            // Silently drop (matching the Vote/PreVote drop convention for
            // out-of-role messages).
            return Vec::new();
        }

        // Stage 3.3 finding-3 fix (iter 3): `fetch_offset` is the next
        // 1-based log index the follower wants (architecture ┬º5.2). A value
        // of 0 is structurally invalid because the driver derives the
        // confirmed_offset by subtracting one (`fetch_offset - 1`) ΓÇö a 0
        // would underflow into u64::MAX and corrupt the leader's per-peer
        // progress map. The empty-log case is correctly encoded as
        // `fetch_offset = 1, last_fetched_epoch = 0` (the follower wants
        // the first entry and has nothing yet). Drop malformed requests
        // before any state mutation or ServeFetch emission.
        if req.fetch_offset == LogIndex(0) {
            tracing::warn!(
                node_id = %self.id,
                replica = %req.replica_id,
                "dropping FetchRequest with invalid fetch_offset=0 (must be >= 1)"
            );
            return Vec::new();
        }

        // Update peer-liveness fields ΓÇö but NOT replication progress.
        // Stage 7.1 (iter-6 evaluator finding #1): also bump the
        // monotonic `fetch_seq` and stamp `peer.last_fetch_seq` so the
        // driver-level lease slow-path can prove "this voter has acked
        // leadership at or after the captured baseline" with a strict
        // (`>`) comparison that does not depend on tick granularity.
        self.fetch_seq = self.fetch_seq.saturating_add(1);
        if let Some(peer) = self.peers.get_mut(&req.replica_id) {
            peer.last_fetch_time = self.logical_tick;
            peer.last_fetch_seq = self.fetch_seq;
        }
        // Refresh self-contact: leader observed activity from the cluster.
        self.last_leader_contact_tick = Some(self.logical_tick);

        let high_watermark = self.commit_index;

        // Stage 5.3 (implementation-plan §5.2 step 4) — leader-side
        // snapshot redirect, emitted by the engine. When the follower's
        // `fetch_offset` falls at or below the compacted prefix
        // (`fetch_offset <= last_snapshot_meta.last_included_index`),
        // the entries in that range have been logically compacted out
        // of the log and the follower must catch up via a
        // `FetchSnapshot` stream rather than waiting for entries the
        // leader no longer holds. Emit `Action::RedirectToSnapshot` so
        // the driver can materialise a `FetchResponse` carrying a
        // `SnapshotRedirect` envelope (mutually exclusive with
        // `entries` and `diverging_epoch`).
        //
        // The redirect requires a canonical, non-empty `snapshot_id`
        // because the follower echoes it back on
        // `FetchSnapshotRequest::snapshot_id` to identify which
        // snapshot to pull. An empty id (e.g. test snapshots written
        // without `SnapshotStore::save_snapshot` normalisation) cannot
        // be the target of a useful redirect — fall through to
        // `ServeFetch` and let the driver produce a divergence /
        // empty-entries response. A `warn!` records the misconfiguration
        // so operators can spot it in logs.
        if let Some(snap) = self.last_snapshot_meta.as_ref()
            && req.fetch_offset.0 <= snap.last_included_index.0
        {
            if !snap.id.is_empty() {
                tracing::debug!(
                    node_id = %self.id,
                    replica = %req.replica_id,
                    fetch_offset = %req.fetch_offset,
                    snapshot_id = %snap.id,
                    last_included_index = %snap.last_included_index,
                    "follower offset is at/below compacted prefix; emitting Action::RedirectToSnapshot"
                );
                return vec![Action::RedirectToSnapshot {
                    to: req.replica_id,
                    cluster_id: self.config.cluster_id.clone(),
                    leader_epoch: self.hard_state.current_term.0,
                    leader_id: self.id,
                    high_watermark,
                    snapshot_metadata: snap.clone(),
                }];
            }
            // Redirect predicate matched but the snapshot has no
            // canonical id. This should not happen for snapshots written
            // by `SnapshotStore::save_snapshot` (it normalises ids), but
            // direct-driver tests or legacy on-disk snapshots could
            // produce this state. Log loudly and fall through to
            // `ServeFetch` so the driver can at least surface a
            // divergence signal.
            tracing::warn!(
                node_id = %self.id,
                replica = %req.replica_id,
                fetch_offset = %req.fetch_offset,
                last_included_index = %snap.last_included_index,
                "snapshot redirect predicate matched but snapshot_id is empty; falling through to ServeFetch"
            );
        }

        vec![Action::ServeFetch {
            to: req.replica_id,
            cluster_id: self.config.cluster_id.clone(),
            leader_epoch: self.hard_state.current_term.0,
            leader_id: self.id,
            high_watermark,
            fetch_offset: req.fetch_offset,
            last_fetched_epoch: req.last_fetched_epoch,
        }]
    }

    /// Driver feedback: the follower at `replica_id` has confirmed
    /// replication up through `confirmed_offset` (the driver validated
    /// divergence first; see [`Input::FetchRequestAcked`]). Update the
    /// per-peer progress monotonically (clamped to the leader's tail) and
    /// re-evaluate the high watermark.
    ///
    /// Emits [`Action::ApplyToStateMachine`] when the high watermark advance
    /// makes new entries committed. No actions when we are not leader,
    /// when the offset is not actually higher than what we already
    /// recorded, or when the no-op safety gate prevents commit advance.
    pub fn handle_fetch_request_acked(
        &mut self,
        replica_id: NodeId,
        confirmed_offset: LogIndex,
    ) -> Vec<Action> {
        if self.role != NodeRole::Leader {
            return Vec::new();
        }
        if !self.is_known_voter(replica_id) && !self.peers.contains_key(&replica_id) {
            // Unknown replica; ignore (the leader does not track progress
            // for nodes it has never observed via configuration or fetch).
            return Vec::new();
        }
        // Clamp to leader's own log tail ΓÇö we cannot honour a follower
        // claim of having entries the leader itself does not have.
        let clamped = LogIndex(confirmed_offset.0.min(self.last_log_index.0));

        let now = self.logical_tick;
        let leader_tail = self.last_log_index;
        if let Some(peer) = self.peers.get_mut(&replica_id) {
            // Monotonic progress: never regress the recorded last_fetch_offset.
            if clamped > peer.last_fetch_offset {
                peer.last_fetch_offset = clamped;
            }
            if peer.last_fetch_offset == leader_tail {
                peer.last_caught_up_time = now;
            }
        }

        let mut actions = Vec::new();
        if self.try_advance_commit_index().is_some() {
            actions.extend(self.drain_apply_pipeline());
        }
        actions
    }

    /// Handle a `FetchResponse` received by this (follower or observer)
    /// node from the leader.
    ///
    /// Per `architecture.md` ┬º5.2 / ┬º5.4 and Stage 3.3 step 2 + step 3:
    /// 1. Drop silently on `cluster_id` mismatch.
    /// 2. If `resp.leader_epoch > current_term`: adopt the new term via
    ///    `become_follower(Term, Some(leader_id))`. Stage 3.3 finding-4 fix:
    ///    do NOT return after the step-down ΓÇö the response is now same-term
    ///    valid and its entries / high watermark / divergence MUST still be
    ///    processed under the new term, otherwise the higher-term leader's
    ///    payload is silently dropped and the follower stays one round
    ///    behind. Fall through to the same-term path.
    /// 3. If `resp.leader_epoch < current_term`, drop ΓÇö stale leader.
    /// 4. Same-term, two-leaders fencing (Stage 3.3 finding-5 fix): if we
    ///    already have `leader_id = Some(known)` and `known != resp.leader_id`,
    ///    the response either comes from a misbehaving peer or has been
    ///    misrouted (two same-term leaders is a Raft safety violation).
    ///    Drop the response WITHOUT resetting the election timer so a
    ///    spurious claimant cannot suppress a genuine election timeout.
    ///    Same fence applies if WE are the leader at this term ΓÇö drop
    ///    defensively.
    /// 5. Same-term valid response: if we are a Candidate or PreCandidate,
    ///    a legitimate same-term leader has been established; step down
    ///    to follower with the leader hint. If we are a Follower /
    ///    Observer without a known leader, adopt the hint.
    /// 6. Reset the election timer and refresh `last_leader_contact_tick`
    ///    (any valid FetchResponse is proof of leader liveness ΓÇö the
    ///    `fetch-resets-election-timer` Stage 3.3 scenario).
    /// 7. If `diverging_epoch` is set: emit [`Action::TruncateLog`] from
    ///    `end_offset + 1` and an immediate re-fetch [`Action::SendMessage`]
    ///    using the leader-supplied consistent point. (We do not update
    ///    `last_log_index` / `last_log_term` here ΓÇö the driver calls
    ///    [`set_last_log`](Self::set_last_log) after truncation.)
    /// 8. If entries are present: emit [`Action::AppendEntries`] and
    ///    advance the in-memory mirror to the last entry. Then advance
    ///    `commit_index` to `min(resp.high_watermark, new last_log_index)`
    ///    and emit [`Action::ApplyToStateMachine`] if anything became
    ///    committed.
    /// 9. If entries are empty: still propagate the high watermark
    ///    (followers learn HW one round late ΓÇö see architecture ┬º5.2).
    #[tracing::instrument(level = "debug", skip(self), fields(node_id = %self.id, current_term = %self.hard_state.current_term))]
    pub fn handle_fetch_response(&mut self, resp: FetchResponse) -> Vec<Action> {
        if resp.cluster_id != self.config.cluster_id {
            return Vec::new();
        }

        // Stage 7.2 bootstrap-discovery fix: ignore non-authoritative
        // responses entirely. The server-driver returns
        // `default_deny_fetch` (FetchResponse with `is_leader = false`)
        // when a non-leader receives a FetchRequest — the responder's
        // `leader_id` is then a best-effort hint and may even be the
        // responder's own id when no leader is known (see
        // `xraft-server::driver::default_deny_fetch`). Without this
        // guard the engine treats those phony leader claims as real:
        //   - it adopts the wrong leader_id via the "leader matches /
        //     no leader yet" arm (causing replication to be routed at
        //     a non-leader), or
        //   - it trips the two-leaders fencing branch and drops every
        //     subsequent legitimate response (locking up replication).
        // The `is_leader` field is documented on `FetchResponse` (see
        // `xraft-core::message::FetchResponse::is_leader`) precisely
        // for this purpose; both the client routing cache and the
        // engine must honour it.
        if !resp.is_leader {
            tracing::trace!(
                node_id = %self.id,
                claimed_leader = %resp.leader_id,
                claimed_epoch = resp.leader_epoch,
                "dropping non-authoritative FetchResponse (is_leader=false)"
            );
            return Vec::new();
        }

        // Stage 7.2 iter-3 finding #3: a leader claim is only acceptable
        // from a CONFIGURED VOTER. Observers are non-voting and cannot be
        // leaders (`architecture.md` §2.1, `tech-spec.md` §2.7) — but
        // observers ARE seeded into `peers` (so the leader's known-sender
        // guard accepts their Fetch RPCs). The iter-2 predicate
        // `is_known_voter || peers.contains_key` accidentally let an
        // observer's FetchResponse with `is_leader = true` slip past as
        // a "tracked peer" — that path is what this filter closes. The
        // strict voter-only check below cannot be relaxed without
        // breaking the static-voter-set invariant. `handle_fetch_request`
        // KEEPS the `peers.contains_key` fallback because observers
        // legitimately send Fetch *requests* (replication direction is
        // leader → observer).
        if !self.is_known_voter(resp.leader_id) {
            tracing::warn!(
                node_id = %self.id,
                unknown_leader = %resp.leader_id,
                claimed_epoch = resp.leader_epoch,
                "dropping FetchResponse: claimed leader is not a configured voter (observers cannot be leaders)"
            );
            return Vec::new();
        }

        let mut actions = Vec::new();

        // Stage 3.3 finding-4: higher-term reconciliation. Adopt the new
        // term via become_follower (which clears stale leader_id and votes
        // and sets the new leader_id from the response), then FALL THROUGH
        // to process the entries / divergence / HW under the new term.
        if resp.leader_epoch > self.hard_state.current_term.0 {
            actions.extend(self.become_follower(Term(resp.leader_epoch), Some(resp.leader_id)));
            // Continue: `resp` is now same-term valid and `self.leader_id`
            // matches `resp.leader_id`, so the fencing check below is a
            // no-op and the entries are processed.
        } else if resp.leader_epoch < self.hard_state.current_term.0 {
            // Stale-leader response ΓÇö drop (we have moved on).
            return actions;
        }

        // Same-term path. Stage 3.3 finding-5: two-leaders fencing.
        if self.role == NodeRole::Leader {
            // Two same-term leaders is a Raft safety violation. We trust
            // our own state; the response is either spurious or from a
            // misbehaving peer.
            tracing::warn!(
                node_id = %self.id,
                current_term = %self.hard_state.current_term,
                claimed_leader = %resp.leader_id,
                "dropping FetchResponse: leader received same-term FetchResponse (safety violation)"
            );
            return Vec::new();
        }
        if let Some(known) = self.leader_id
            && known != resp.leader_id
        {
            tracing::warn!(
                node_id = %self.id,
                current_term = %self.hard_state.current_term,
                known_leader = %known,
                claimed_leader = %resp.leader_id,
                "dropping FetchResponse: two same-term leaders (safety violation)"
            );
            // Critically, do NOT reset the election timer here. A divergent
            // claimant must not be able to suppress our election timeout.
            return Vec::new();
        }

        // Same-term, leader matches (or we had no leader_id yet). Establish
        // or confirm leadership.
        match self.role {
            NodeRole::Candidate | NodeRole::PreCandidate => {
                actions.extend(
                    self.become_follower(self.hard_state.current_term, Some(resp.leader_id)),
                );
            }
            NodeRole::Follower | NodeRole::Observer => {
                if self.leader_id.is_none() {
                    self.leader_id = Some(resp.leader_id);
                }
            }
            NodeRole::Leader => {
                // Already returned above. Unreachable ΓÇö kept for an
                // exhaustive match.
                return Vec::new();
            }
        }
        // Refresh the leader-contact stamp and reset the election timer.
        // Both steps satisfy the `fetch-resets-election-timer` scenario.
        self.last_leader_contact_tick = Some(self.logical_tick);
        self.election_timer.reset(&mut self.rng);

        // Stage 5.2 (implementation-plan §5.2 step 4) — leader-side
        // snapshot redirect. The leader has signalled that the
        // follower's `fetch_offset` is at or below the compacted
        // prefix; switch to FetchSnapshot to catch up. This branch
        // runs AFTER all fencing (cluster_id, known leader, term
        // reconciliation, two-leader fencing, role adoption) and
        // BEFORE divergence/entries processing per the
        // `FetchResponse` mutual-exclusivity contract: when a
        // redirect is present the response carries no entries and
        // no divergence signal, so we emit the FetchSnapshotRequest
        // and return immediately.
        //
        // The follower asks for the snapshot from offset 0 with
        // `max_bytes = 0` (no caller-imposed limit; the leader's
        // chunker decides chunk size). The driver's outbound pipeline
        // reassembles chunks and dispatches `Action::InstallSnapshot`
        // with the validated metadata + bytes.
        if let Some(redirect) = resp.snapshot_redirect {
            tracing::info!(
                node_id = %self.id,
                leader = %resp.leader_id,
                snapshot_id = %redirect.snapshot_id,
                last_included_index = %redirect.last_included_index,
                last_included_term = %redirect.last_included_term,
                "leader redirected fetch to snapshot install"
            );
            actions.push(Action::SendMessage {
                to: resp.leader_id,
                message: OutboundMessage::FetchSnapshotRequest(FetchSnapshotRequest {
                    cluster_id: self.config.cluster_id.clone(),
                    leader_epoch: self.hard_state.current_term.0,
                    replica_id: self.id,
                    snapshot_id: redirect.snapshot_id,
                    offset: 0,
                    max_bytes: 0,
                }),
            });
            self.last_fetch_tick = Some(self.logical_tick);
            return actions;
        }

        // Divergence resolution path.
        if let Some(de) = resp.diverging_epoch {
            let truncate_from = LogIndex(de.end_offset.0.saturating_add(1));
            actions.push(Action::TruncateLog(LogTruncation::SuffixFromInclusive {
                from_index_inclusive: truncate_from,
            }));
            // Immediate re-fetch using the leader-supplied consistent point.
            // The driver's TruncateLog handler will subsequently call
            // set_last_log with the actual post-truncation values, so
            // future Tick-driven fetches use correct local state.
            let refetch = FetchRequest {
                cluster_id: self.config.cluster_id.clone(),
                leader_epoch: self.hard_state.current_term.0,
                replica_id: self.id,
                fetch_offset: truncate_from,
                last_fetched_epoch: de.epoch,
            };
            actions.push(Action::SendMessage {
                to: resp.leader_id,
                message: OutboundMessage::FetchRequest(refetch),
            });
            self.last_fetch_tick = Some(self.logical_tick);
            return actions;
        }

        // Non-diverging entries path.
        if !resp.entries.is_empty() {
            // Sanity: entries must be contiguous starting at last_log_index + 1.
            // If the leader sent an out-of-order batch, drop and let the next
            // tick re-fetch.
            let expected_first = LogIndex(self.last_log_index.0.saturating_add(1));
            if resp.entries[0].index != expected_first {
                tracing::warn!(
                    node_id = %self.id,
                    expected_first = %expected_first,
                    actual_first = %resp.entries[0].index,
                    "dropping FetchResponse with non-contiguous entries"
                );
                return actions;
            }
            // Stage 3.3 finding-2 fix (iter 3): validate the ENTIRE batch is
            // contiguous, not just the first entry. The previous code trusted
            // entries[1..] would be entries[0].index + 1, + 2, ... but a
            // malformed leader (or corrupted wire transport) could send
            // entries with index gaps (e.g. [1, 3]). Appending such a batch
            // would leave the follower's log non-contiguous and violate Raft
            // log-matching invariants. We also reject in-batch term regress
            // (term may only stay the same or grow within a single batch
            // from a single leader epoch) for the same reason. Drop the
            // entire response ΓÇö the next tick re-fetches.
            for w in resp.entries.windows(2) {
                if w[1].index.0 != w[0].index.0.saturating_add(1) {
                    tracing::warn!(
                        node_id = %self.id,
                        prev_index = %w[0].index,
                        next_index = %w[1].index,
                        "dropping FetchResponse with non-contiguous entries within batch"
                    );
                    return actions;
                }
                if w[1].term < w[0].term {
                    tracing::warn!(
                        node_id = %self.id,
                        prev_term = %w[0].term,
                        next_term = %w[1].term,
                        "dropping FetchResponse with regressing term within batch"
                    );
                    return actions;
                }
            }
            let last_entry = resp.entries.last().expect("entries is non-empty");
            let new_last_index = last_entry.index;
            let new_last_term = last_entry.term;
            actions.push(Action::AppendEntries(resp.entries.clone()));
            self.last_log_index = new_last_index;
            self.last_log_term = new_last_term;
        }

        // Propagate the high watermark ΓÇö clamp to our own log tail because
        // we cannot commit entries we have not yet replicated.
        let new_commit = LogIndex(resp.high_watermark.0.min(self.last_log_index.0));
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            actions.extend(self.drain_apply_pipeline());
        }

        actions
    }

    /// Handle an [`Input::ClientPropose`] (Stage 3.3 step 5 / ┬º5.2).
    ///
    /// Only the leader accepts client proposals. The new entry is appended
    /// at `last_log_index + 1` with the current term and emitted as
    /// [`Action::AppendEntries`]. On a single-voter cluster the new entry
    /// already satisfies quorum, so commit advancement runs immediately
    /// and an [`Action::ApplyToStateMachine`] is appended.
    ///
    /// Non-leader callers receive an empty action vector. A typed
    /// `NotLeader` error reply belongs in the higher-level RPC layer
    /// ([`xraft_client::PeerClient`](https://docs.rs/) routes via leader
    /// hints from `VoteResponse` / `FetchResponse`); the engine itself
    /// stays I/O-free and silently drops out-of-role proposals.
    #[tracing::instrument(level = "debug", skip(self, command), fields(node_id = %self.id, current_term = %self.hard_state.current_term, command_len = command.len()))]
    pub fn handle_client_propose(&mut self, command: bytes::Bytes) -> Vec<Action> {
        if self.role != NodeRole::Leader {
            tracing::debug!(
                node_id = %self.id,
                role = ?self.role,
                "dropping ClientPropose: not leader"
            );
            return Vec::new();
        }
        let new_index = LogIndex(self.last_log_index.0.saturating_add(1));
        let new_term = self.hard_state.current_term;
        let entry = Entry {
            index: new_index,
            term: new_term,
            payload: EntryPayload::Command(command),
        };
        self.last_log_index = new_index;
        self.last_log_term = new_term;

        let mut actions = vec![Action::AppendEntries(vec![entry])];
        if self.try_advance_commit_index().is_some() {
            actions.extend(self.drain_apply_pipeline());
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, VoterConfig};
    use crate::error::XRaftError;
    use crate::message::{DivergingEpoch, EntryPayload};
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

    // Stage 6.1 acceptance (iter-3 evaluator finding #3):
    // `ClusterConfig.observers` containing this node's id must seed
    // `NodeRole::Observer` at construction. The reverse — node not in
    // observers — still starts as `Follower`.
    #[test]
    fn new_node_starts_as_observer_when_listed_in_observers_field() {
        let toml = format!(
            r#"
node_id = 5
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200
observers = [5]

[[voters]]
node_id = 1
directory_id = "{}"
host = "node1"
port = 6000
"#,
            Uuid::new_v4(),
        );
        let cfg = ClusterConfig::from_toml_str(&toml).unwrap();
        let node = RaftNode::new_with_seed(cfg, 1).unwrap();
        assert_eq!(
            node.role,
            NodeRole::Observer,
            "node listed in `observers` must construct as Observer"
        );
    }

    #[test]
    fn new_node_with_unrelated_observer_list_still_starts_as_follower() {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200
observers = [7, 8]

[[voters]]
node_id = 1
directory_id = "{}"
host = "node1"
port = 6000
"#,
            Uuid::new_v4(),
        );
        let cfg = ClusterConfig::from_toml_str(&toml).unwrap();
        let node = RaftNode::new_with_seed(cfg, 1).unwrap();
        assert_eq!(
            node.role,
            NodeRole::Follower,
            "node NOT listed in `observers` must construct as Follower"
        );
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

    // ---------------------------------------------------------------
    // Stage 7.2 — observer peer registration
    // ---------------------------------------------------------------

    /// Build a config with `voters = [1, 2, 3]` and `observers = [4]`,
    /// running on node `self_id`. Used to exercise observer seeding
    /// from both the voter-side (leader sees observer in peers) and
    /// the observer-side (the observer node has its own peer view).
    fn three_voter_one_observer_config(self_id: u64) -> ClusterConfig {
        let toml = format!(
            r#"
node_id = {self_id}
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200
observers = [4]

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

    #[test]
    fn observer_seeded_as_non_voting_peer_on_voter_node() {
        // Voter node 1 sees: voters {1,2,3} + observer {4}. Its peers
        // map MUST include node 4 with `is_voter = false` so the
        // leader (when it becomes one) accepts node 4's Fetch RPCs
        // through the known-sender guard in `handle_fetch_request`.
        let node = RaftNode::new_with_seed(three_voter_one_observer_config(1), 1).unwrap();
        assert!(node.voter_set.is_some());
        // Voters 2, 3 plus observer 4 = 3 peers (self excluded).
        assert_eq!(node.peers.len(), 3);
        let p2 = node.peers.get(&NodeId(2)).expect("peer 2 missing");
        let p3 = node.peers.get(&NodeId(3)).expect("peer 3 missing");
        let p4 = node.peers.get(&NodeId(4)).expect("observer peer 4 missing");
        assert!(p2.is_voter, "voter 2 must be a voter peer");
        assert!(p3.is_voter, "voter 3 must be a voter peer");
        assert!(
            !p4.is_voter,
            "observer 4 MUST be seeded as a non-voting peer so the leader \
             accepts its Fetch RPCs while excluding it from quorum"
        );
    }

    #[test]
    fn observer_does_not_seed_self_as_peer() {
        // Observer node 4 should NOT have a `PeerState` for itself
        // even though it's listed in `observers = [4]`.
        let node = RaftNode::new_with_seed(three_voter_one_observer_config(4), 1).unwrap();
        // Observer node 4 sees voters {1,2,3} as voting peers and
        // does NOT have itself in peers.
        assert_eq!(node.peers.len(), 3);
        assert!(!node.peers.contains_key(&NodeId(4)));
        for nid in [NodeId(1), NodeId(2), NodeId(3)] {
            let p = node.peers.get(&nid).expect("voter peer missing");
            assert!(p.is_voter);
        }
        // Observer role gating: a node listed in `observers`
        // constructs as Observer (not Follower).
        assert_eq!(node.role, NodeRole::Observer);
    }

    #[test]
    fn leader_accepts_observer_fetch_and_excludes_from_quorum() {
        // End-to-end engine test for the observer-replicates-without-voting
        // Stage 7.2 scenario:
        // - Voter node 1 becomes leader of a 3-voter + 1-observer
        //   cluster.
        // - An observer (node 4) Fetch RPC is accepted (NOT dropped
        //   by the known-sender guard at `handle_fetch_request:2028`).
        // - An observer ack with a high `confirmed_offset` does NOT
        //   advance commit because the observer does not count toward
        //   the quorum tally in `try_advance_commit_index`.
        let mut node = RaftNode::new_with_seed(three_voter_one_observer_config(1), 7).unwrap();

        // Force the node to become leader at term 1 by advancing it
        // through the pre-vote / vote cycle the hard way: bump term,
        // pretend the prior election round delivered the votes, then
        // call become_leader directly. This is the same shape used by
        // existing test `become_leader_initialises_peers_and_emits_noop`.
        node.hard_state.current_term = Term(1);
        let actions = node.become_leader();
        assert!(matches!(node.role, NodeRole::Leader));
        // No-op append emits AppendEntries; for a 3-voter cluster
        // the single leader can't commit yet (needs at least one
        // voter ack), so no apply pipeline should have fired.
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::AppendEntries(_))),
            "leader emits no-op AppendEntries on becoming leader"
        );

        // The leader sets `last_log_index = 1` (the no-op). Mirror
        // that into the engine view so the FetchRequest from the
        // observer is meaningful.
        let leader_tail = node.last_log_index;
        assert_eq!(leader_tail.0, 1);

        // Build an observer Fetch RPC for offset 1 (the no-op) and
        // verify the leader accepts it (emits ServeFetch, not the
        // unknown-sender drop).
        let fetch_req = FetchRequest {
            cluster_id: node.config.cluster_id.clone(),
            replica_id: NodeId(4),
            leader_epoch: node.hard_state.current_term.0,
            fetch_offset: LogIndex(1),
            last_fetched_epoch: Term(0),
        };
        let actions = node.handle_fetch_request(fetch_req);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ServeFetch { .. })),
            "leader must accept observer Fetch and emit ServeFetch — \
             got actions: {actions:?}"
        );

        // Now simulate the observer ack: claim confirmed_offset = 1.
        // The leader should record the observer's progress in
        // PeerState but NOT advance commit_index since observers
        // don't count toward quorum.
        let pre_commit = node.commit_index;
        let _ = node.handle_fetch_request_acked(NodeId(4), LogIndex(1));
        assert_eq!(
            node.commit_index, pre_commit,
            "observer ack alone must NOT advance commit_index — \
             observers are excluded from high-watermark quorum"
        );
        let observer_peer = node
            .peers
            .get(&NodeId(4))
            .expect("observer peer must still exist");
        assert!(!observer_peer.is_voter, "observer peer remains non-voting");
        assert_eq!(
            observer_peer.last_fetch_offset,
            LogIndex(1),
            "observer's fetch progress was recorded for replication accounting"
        );
    }

    #[test]
    fn observer_node_does_not_become_candidate_on_tick() {
        // Stage 7.2: observer nodes never time out into an election.
        // Tick the observer past its election-timeout budget and assert
        // the role is still `Observer`, term is still 0, and there is
        // no PreVoteRequest in the emitted actions.
        let mut node = RaftNode::new_with_seed(three_voter_one_observer_config(4), 11).unwrap();
        assert_eq!(node.role, NodeRole::Observer);
        let initial_term = node.current_term();
        // Drive way past max_ticks so a Follower would have moved
        // into PreCandidate by now.
        for _ in 0..1000 {
            let actions = node.step(Input::Tick);
            assert!(
                !actions.iter().any(|a| matches!(
                    a,
                    Action::SendMessage {
                        message: OutboundMessage::PreVoteRequest(_),
                        ..
                    }
                )),
                "Observer must never emit PreVoteRequest"
            );
        }
        assert_eq!(node.role, NodeRole::Observer);
        assert_eq!(node.current_term(), initial_term);
    }

    // -------------------------------------------------------------------
    // Stage 7.2 (evaluator iter-1 finding #2): observers MUST NOT
    // participate in elections from either side. Tests cover:
    //   * Candidate-side broadcast filters observers (pre-vote + vote).
    //   * Observer-side request handlers drop incoming pre-vote and
    //     vote requests with no state mutation.
    // -------------------------------------------------------------------

    #[test]
    fn pre_candidate_does_not_send_prevote_to_observer_peers() {
        // Voter node 1 in a (1,2,3)+observer(4) cluster. When the
        // election timer expires it enters PreCandidate and must
        // broadcast PreVoteRequest only to voters 2 and 3, NOT to
        // observer 4. The candidate-side filter is the structural
        // fix; the observer-side reject is defense-in-depth.
        let mut node = RaftNode::new_with_seed(three_voter_one_observer_config(1), 7).unwrap();
        let actions = node.become_pre_candidate();
        let recipients: Vec<NodeId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::SendMessage {
                    to,
                    message: OutboundMessage::PreVoteRequest(_),
                } => Some(*to),
                _ => None,
            })
            .collect();
        assert!(
            recipients.contains(&NodeId(2)) && recipients.contains(&NodeId(3)),
            "voters 2 + 3 must receive PreVoteRequest; got {recipients:?}"
        );
        assert!(
            !recipients.contains(&NodeId(4)),
            "observer 4 MUST NOT receive a PreVoteRequest (non-voting peer); got {recipients:?}"
        );
    }

    #[test]
    fn candidate_does_not_send_vote_to_observer_peers() {
        // Symmetric to the pre-vote test, but for the real-vote
        // broadcast. After become_candidate the emitted VoteRequest
        // recipients must include only voter peers.
        let mut node = RaftNode::new_with_seed(three_voter_one_observer_config(1), 7).unwrap();
        let actions = node.become_candidate();
        let recipients: Vec<NodeId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::SendMessage {
                    to,
                    message: OutboundMessage::VoteRequest(_),
                } => Some(*to),
                _ => None,
            })
            .collect();
        assert!(
            recipients.contains(&NodeId(2)) && recipients.contains(&NodeId(3)),
            "voters 2 + 3 must receive VoteRequest; got {recipients:?}"
        );
        assert!(
            !recipients.contains(&NodeId(4)),
            "observer 4 MUST NOT receive a VoteRequest (non-voting peer); got {recipients:?}"
        );
    }

    #[test]
    fn observer_drops_incoming_vote_request_without_term_bump() {
        // An observer that receives a (misrouted / replayed)
        // VoteRequest at a HIGHER term MUST NOT bump its term, must
        // NOT cast the vote, and must NOT emit a response. The
        // observer role is preserved.
        let mut observer = RaftNode::new_with_seed(three_voter_one_observer_config(4), 1).unwrap();
        assert_eq!(observer.role, NodeRole::Observer);
        let initial_term = observer.current_term();

        let req = VoteRequest {
            cluster_id: "test".into(),
            leader_epoch: 0,
            term: Term(initial_term.0 + 5),
            candidate_id: NodeId(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        };
        let actions = observer.handle_vote_request(req);
        assert!(
            actions.is_empty(),
            "observer must emit no actions for VoteRequest; got {actions:?}"
        );
        assert_eq!(
            observer.role,
            NodeRole::Observer,
            "observer role preserved across rejected VoteRequest"
        );
        assert_eq!(
            observer.current_term(),
            initial_term,
            "observer must NOT bump its term on rejected VoteRequest"
        );
        assert!(
            observer.hard_state.voted_for.is_none(),
            "observer must NOT cast a vote"
        );
    }

    #[test]
    fn observer_drops_incoming_pre_vote_request() {
        // Mirror of the VoteRequest test for PreVoteRequest. The
        // observer must not grant pre-votes.
        let observer = RaftNode::new_with_seed(three_voter_one_observer_config(4), 1).unwrap();
        assert_eq!(observer.role, NodeRole::Observer);

        let req = PreVoteRequest {
            cluster_id: "test".into(),
            leader_epoch: 0,
            next_term: Term(observer.current_term().0 + 1),
            candidate_id: NodeId(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        };
        let actions = observer.handle_pre_vote_request(req);
        assert!(
            actions.is_empty(),
            "observer must emit no actions for PreVoteRequest; got {actions:?}"
        );
    }

    #[test]
    fn observer_preserves_role_on_higher_term_become_follower() {
        // Stage 7.2 (evaluator iter-1 finding #3): an observer that
        // observes a higher term (via FetchResponse, VoteResponse,
        // PreVoteResponse, or any other become_follower call site)
        // must remain Observer, NOT collapse to Follower. Otherwise
        // it would start ticking toward an election on the next
        // tick — violating the "never participates in elections"
        // contract.
        let mut observer = RaftNode::new_with_seed(three_voter_one_observer_config(4), 1).unwrap();
        assert_eq!(observer.role, NodeRole::Observer);
        let starting_term = observer.current_term();
        let higher = Term(starting_term.0 + 42);

        let actions = observer.become_follower(higher, Some(NodeId(1)));
        // PersistHardState is expected (term changed); StepDown is
        // NOT expected (observer was never leader / candidate).
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "term change must yield PersistHardState; got {actions:?}"
        );
        assert!(
            !actions.iter().any(|a| matches!(a, Action::StepDown)),
            "observer that was never leader must NOT emit StepDown; got {actions:?}"
        );
        assert_eq!(
            observer.role,
            NodeRole::Observer,
            "observer role MUST be preserved across higher-term become_follower"
        );
        assert_eq!(observer.current_term(), higher);
        assert_eq!(observer.leader_id, Some(NodeId(1)));

        // Further ticks must NOT turn the observer into a PreCandidate.
        let initial_role = observer.role;
        for _ in 0..1000 {
            let actions = observer.step(Input::Tick);
            assert!(
                !actions.iter().any(|a| matches!(
                    a,
                    Action::SendMessage {
                        message: OutboundMessage::PreVoteRequest(_),
                        ..
                    }
                )),
                "post-term-bump observer must still never emit PreVoteRequest"
            );
        }
        assert_eq!(observer.role, initial_role);
    }

    /// Stage 7.2 bootstrap-discovery: a fresh Follower with no
    /// `leader_id` MUST broadcast `FetchRequest` to every configured
    /// voter peer on tick. Without this, a freshly-elected leader is
    /// never discovered (the leader does not push; followers must pull),
    /// and the cluster cannot complete first-boot election. See the
    /// `handle_tick` comment block describing the bootstrap-discovery
    /// branch for the rationale.
    #[test]
    fn follower_without_leader_id_broadcasts_fetch_to_all_voters_on_tick() {
        // 3-voter config; this node is voter id=1.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 73).unwrap();
        // Force the explicit Follower role with no known leader.
        node.role = NodeRole::Follower;
        node.leader_id = None;
        node.hard_state.current_term = Term(0);

        let actions = node.step(Input::Tick);
        let mut fetch_targets: std::collections::BTreeSet<NodeId> =
            std::collections::BTreeSet::new();
        for a in &actions {
            if let Action::SendMessage {
                to,
                message: OutboundMessage::FetchRequest(_),
            } = a
            {
                fetch_targets.insert(*to);
            }
        }
        // 3-voter config voters are {1, 2, 3}; node id is 1.
        // Broadcast must go to BOTH other voters and never to self.
        let mut expected: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        expected.insert(NodeId(2));
        expected.insert(NodeId(3));
        assert_eq!(
            fetch_targets, expected,
            "bootstrap-discovery: Follower with leader_id=None MUST broadcast \
             FetchRequest to ALL voter peers (and never to self); got {fetch_targets:?}, \
             actions = {actions:?}"
        );
        assert!(
            !fetch_targets.contains(&NodeId(1)),
            "self-skip violated: must not Fetch from self"
        );

        // The broadcast must rate-limit identically to the directed-fetch
        // path: an immediate back-to-back Tick must NOT issue another
        // broadcast (last_fetch_tick was stamped).
        let actions2 = node.step(Input::Tick);
        let any_fetch = actions2.iter().any(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::FetchRequest(_),
                    ..
                }
            )
        });
        assert!(
            !any_fetch,
            "back-to-back Ticks must not re-broadcast: discovery is \
             rate-limited at fetch_interval_ticks"
        );
    }

    /// Stage 7.2: the bootstrap-discovery broadcast also applies to
    /// Observers — they need the same leader-discovery mechanism to
    /// start replicating. But the broadcast MUST NOT cause the
    /// observer to leak into election behaviour (no PreVote, no Vote,
    /// no role change).
    #[test]
    fn observer_without_leader_id_broadcasts_fetch_but_does_not_emit_election_rpc() {
        // 3-voter + 1-observer config, this node is the observer (id=4).
        let mut node = RaftNode::new_with_seed(three_voter_one_observer_config(4), 41).unwrap();
        assert_eq!(node.role, NodeRole::Observer);
        node.leader_id = None;

        let actions = node.step(Input::Tick);
        let mut fetch_targets: std::collections::BTreeSet<NodeId> =
            std::collections::BTreeSet::new();
        for a in &actions {
            if let Action::SendMessage {
                to,
                message: OutboundMessage::FetchRequest(_),
            } = a
            {
                fetch_targets.insert(*to);
            }
        }
        // Observer should broadcast to all three voters: {1, 2, 3}.
        let mut expected: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        expected.insert(NodeId(1));
        expected.insert(NodeId(2));
        expected.insert(NodeId(3));
        assert_eq!(
            fetch_targets, expected,
            "bootstrap-discovery (observer): MUST broadcast FetchRequest to \
             ALL voter peers; got {fetch_targets:?}, actions = {actions:?}"
        );

        // Critical: the observer MUST NOT emit any election-related RPC.
        for a in &actions {
            match a {
                Action::SendMessage {
                    message: OutboundMessage::PreVoteRequest(_),
                    ..
                }
                | Action::SendMessage {
                    message: OutboundMessage::VoteRequest(_),
                    ..
                } => panic!("observer must NEVER emit election RPCs on Tick; got {a:?}",),
                _ => {}
            }
        }
        // The observer's role MUST stay Observer.
        assert_eq!(
            node.role,
            NodeRole::Observer,
            "observer role must not change as a result of bootstrap-discovery Tick"
        );
    }

    #[test]
    fn even_voter_set_emits_warning_log() {
        // Acceptance scenario `even-voter-warning`: a 2-voter
        // configuration must be ACCEPTED but emit a tracing::warn
        // event so operators are alerted that the cluster has
        // reduced per-node fault tolerance. The warning is the
        // single canonical signal — capture it via
        // `tracing_test::traced_test` and assert presence.
        use tracing::Level;
        use tracing::subscriber::with_default;
        use tracing_subscriber::FmtSubscriber;
        use tracing_subscriber::fmt::MakeWriter;

        #[derive(Clone, Default)]
        struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for CaptureWriter {
            type Writer = CaptureWriter;
            fn make_writer(&self) -> Self::Writer {
                self.clone()
            }
        }

        let writer = CaptureWriter::default();
        let buf = writer.0.clone();
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::WARN)
            .with_writer(writer)
            .with_ansi(false)
            .finish();

        // Build a 2-voter config (even count); the engine must
        // construct successfully but emit the warning while doing
        // so.
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
"#,
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let cfg = ClusterConfig::from_toml_str(&toml).unwrap();

        with_default(subscriber, || {
            let node = RaftNode::new(cfg).expect("2-voter config must still construct");
            assert_eq!(node.voter_set.as_ref().map(|v| v.len()), Some(2));
        });

        let captured = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            captured.contains("voter set has an even number of voters"),
            "even-voter-warning scenario: must emit a tracing::warn — captured = {captured:?}"
        );
        assert!(
            captured.contains("WARN"),
            "captured log must be at WARN level: {captured:?}"
        );
    }

    // -------------------------------------------------------------------
    // Stage 3.1 scenario: election-timeout-triggers-candidacy
    // -------------------------------------------------------------------

    #[test]
    fn election_timeout_triggers_candidacy() {
        // Stage 3.1 scenario: election-timeout-triggers-candidacy.
        //
        // Per architecture.md ┬º5.1 and e2e-scenarios.md Feature 3, an
        // election timeout sends a Follower into the Pre-Vote phase
        // (`PreCandidate`) ΓÇö *not* directly into `Candidate`. The term must
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
                // No PersistHardState ΓÇö Pre-Vote does not bump term.
                assert!(
                    !actions
                        .iter()
                        .any(|a| matches!(a, Action::PersistHardState)),
                    "PreCandidate transition must NOT persist hard state \
                     (term unchanged) ΓÇö got {actions:?}"
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
                // No real VoteRequest yet ΓÇö that fires after pre-vote quorum.
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
            "Pre-Vote must NOT increment term (architecture.md ┬º5.1)"
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
        // exercises the second half of the Pre-Vote ΓåÆ Candidate transition
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
        // Stage 7.2 bootstrap-discovery: a Follower with `leader_id == None`
        // now broadcasts FetchRequest to all voter peers on Tick (the
        // pull-based leader-discovery mechanism). The election timer has
        // NOT expired after a single tick, so no election RPC may be
        // emitted. Assert the relaxed invariant: no PreVote / Vote / etc.,
        // and the only emissions are FetchRequest broadcasts.
        for a in &actions {
            match a {
                Action::SendMessage {
                    message: OutboundMessage::FetchRequest(_),
                    ..
                } => {}
                Action::SendMessage {
                    message: OutboundMessage::PreVoteRequest(_),
                    ..
                }
                | Action::SendMessage {
                    message: OutboundMessage::VoteRequest(_),
                    ..
                }
                | Action::PersistHardState
                | Action::BecomeLeader
                | Action::StepDown => panic!(
                    "single Tick on a fresh Follower must NOT trigger an election or \
                     mutate hard state; got {a:?}"
                ),
                _ => {}
            }
        }
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
        // path is exercised separately via handle_tick ΓåÆ become_pre_candidate.
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
        // PreVoteRequests with a fresh timer) ΓÇö NOT increment the term.
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
            "CandidateΓåÆPreCandidate fallback must NOT bump term again"
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
        // The full Pre-Vote-first path (architecture.md ┬º5.1):
        //   Follower ticks until election timer expires
        //     ΓåÆ handle_tick routes to become_pre_candidate
        //     ΓåÆ self pre-vote satisfies pre-election quorum (1-of-1)
        //     ΓåÆ cascades into become_candidate (term++)
        //     ΓåÆ self vote satisfies election quorum (1-of-1)
        //     ΓåÆ cascades into become_leader (no-op AppendEntries)
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

    /// Stage 7.1 iter-5 evaluator finding #3 — `become_follower` MUST
    /// clear the leader-only `leader_started_tick` (the lease
    /// baseline) and `check_quorum_elapsed_ticks` (the Check-Quorum
    /// interval counter) on every step-down. Their field docs at
    /// `pub leader_started_tick` / `pub check_quorum_elapsed_ticks`
    /// explicitly state the reset point is "follower transition" /
    /// "every role transition"; without this clearing a stepped-down
    /// former leader would observe a stale lease baseline on its
    /// next re-election (or — worse — `has_active_lease()` could
    /// return `true` against the OLD baseline while in Follower
    /// role, depending on the lease predicate's role check).
    #[test]
    fn become_follower_clears_stage_7_1_leader_only_state() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        // Drive into the Leader role so the Stage 7.1 leader-only
        // state is non-default.
        let _ = node.become_candidate();
        let _ = node.become_leader();
        assert_eq!(node.role, NodeRole::Leader);
        assert!(
            node.leader_started_tick.is_some(),
            "test precondition: a fresh Leader must have a non-None \
             leader_started_tick (set in become_leader)"
        );
        // Simulate some Check-Quorum tick accumulation.
        node.check_quorum_elapsed_ticks = 42;

        // Step down to follower at a higher term (the canonical path).
        let _ = node.become_follower(Term(node.current_term().0 + 1), None);

        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(
            node.leader_started_tick, None,
            "become_follower MUST clear leader_started_tick on step-down \
             (Stage 7.1 field doc: 'Cleared in become_follower')"
        );
        assert_eq!(
            node.check_quorum_elapsed_ticks, 0,
            "become_follower MUST reset check_quorum_elapsed_ticks to 0 \
             on step-down (Stage 7.1 field doc: 'Cleared on every role \
             transition')"
        );
    }

    /// Stage 7.1 iter-5 evaluator finding #3 — the inline higher-term
    /// `VoteRequest` path bypasses `become_follower` (to coalesce the
    /// term-bump + vote grant into a single PersistHardState — see
    /// rubber-duck non-blocking issue #3) and therefore must
    /// independently mirror the leader-only Stage 7.1 state cleanup.
    /// Without this, a leader that steps down via a higher-term
    /// VoteRequest would keep stale `leader_started_tick` and
    /// `check_quorum_elapsed_ticks`.
    #[test]
    fn handle_vote_request_higher_term_clears_stage_7_1_leader_only_state() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        let _ = node.become_candidate();
        let _ = node.become_leader();
        assert_eq!(node.role, NodeRole::Leader);
        assert!(
            node.leader_started_tick.is_some(),
            "test precondition: Leader has leader_started_tick set"
        );
        node.check_quorum_elapsed_ticks = 17;
        let starting_term = node.current_term();

        // Higher-term VoteRequest: takes the inline step-down path
        // at `node.rs::handle_vote_request` (`req.term >
        // current_term` branch) — NOT the `become_follower` path.
        let _ = node.handle_vote_request(vote_req(
            "test",
            starting_term.0 + 4,
            NodeId(2),
            node.last_log_index.0,
            node.last_log_term.0,
        ));

        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(
            node.leader_started_tick, None,
            "inline higher-term step-down MUST clear leader_started_tick \
             (otherwise a future re-election observes a stale lease baseline)"
        );
        assert_eq!(
            node.check_quorum_elapsed_ticks, 0,
            "inline higher-term step-down MUST reset check_quorum_elapsed_ticks"
        );
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
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_domain_name: None,
            connect_timeout_ms: 5_000,
            rpc_timeout_ms: 30_000,
            max_rpc_retries: 3,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 5_000,
            max_message_size: 64 * 1024 * 1024,
            observers: vec![],
            enable_check_quorum: true,
            enable_leader_lease: false,
            check_quorum_interval_ms: None,
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
    // Stage 3.2 ΓÇö Leader Election handler tests
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
        // No PersistHardState ΓÇö neither term nor vote changed.
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
        // higher term ΓåÆ adopt and clear vote, then reject the vote).
        assert_eq!(node.current_term(), Term(4));
        assert_eq!(node.hard_state.voted_for, None);
    }

    #[test]
    fn handle_vote_request_rejects_when_already_voted_other() {
        // At the same term, voted_for=NodeId(2). A new candidate=NodeId(3)
        // asks for a vote ΓÇö denied (one vote per term safety invariant).
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
        // No PersistHardState ΓÇö voted_for did not actually change.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState))
        );
    }

    #[test]
    fn handle_vote_request_steps_down_on_higher_term_as_leader() {
        // Leader at term=2 receives VoteRequest at term=5 ΓÇö step down to
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
        assert_eq!(node.role, NodeRole::Leader, "two peer grants ΓåÆ quorum");
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

        // Tally = {self, NodeId(2)} ΓåÆ 2 of 5 (quorum=3).
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
        let node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
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
        // receives a PreVote, Then it rejects (architecture ┬º2.1).
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
        // the architecture rule "within the election timeout" ΓÇö see
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
        let node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
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
        let node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
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
        // Pre-vote quorum (2 of 3) ΓåÆ cascade into Candidate.
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

        // Quorum (2 of 3) ΓåÆ cascade to Candidate at term 6.
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
        // NOT count as two ΓÇö the HashSet semantics dedupe.
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
        // Vote response ΓåÆ quorum on a 3-node cluster (self + one peer).
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
        // Pre-quorum ΓåÆ Candidate.
        assert_eq!(node.role, NodeRole::Candidate);
    }

    // ---- End-to-end election in a 3-voter cluster ----------------------

    #[test]
    fn three_node_cluster_full_election_via_step() {
        // End-to-end: drive the full Pre-Vote ΓåÆ Vote ΓåÆ Leader cascade on
        // node 1 by feeding it Tick (until pre-candidate), then a
        // PreVoteResponse grant (ΓåÆ Candidate), then a VoteResponse grant
        // (ΓåÆ Leader). Verifies all four Stage 3.2 handlers are wired via
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

        // 2) One peer grants the pre-vote ΓåÆ Candidate at term 1.
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term(), Term(1));

        // 3) One peer grants the real vote ΓåÆ Leader.
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

    // -------------------------------------------------------------------
    // Stage 3.3 ΓÇö Log Replication scenario tests
    // -------------------------------------------------------------------

    /// Helper: drive `node` from Follower into Leader on a 3-voter cluster
    /// by feeding the minimum number of synthetic responses required to
    /// satisfy Pre-Vote then Vote quorum (2-of-3). Returns when `node.role
    /// == Leader`. Panics if any role check fails.
    fn drive_three_voter_to_leader(node: &mut RaftNode) {
        // Step 1: Tick into PreCandidate.
        let max_ticks = node.election_timer.max_ticks() + 5;
        for _ in 0..max_ticks {
            node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate {
                break;
            }
        }
        assert_eq!(
            node.role,
            NodeRole::PreCandidate,
            "did not become PreCandidate"
        );

        // Step 2: One pre-vote grant from a peer ΓåÆ Candidate (term++).
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        assert_eq!(node.role, NodeRole::Candidate, "did not become Candidate");

        // Step 3: One real-vote grant ΓåÆ Leader (with no-op AppendEntries).
        let cur_term = node.current_term().0;
        let _ = node.step(Input::VoteResponse {
            from: NodeId(3),
            response: vote_resp("test", cur_term, true),
        });
        assert_eq!(node.role, NodeRole::Leader, "did not become Leader");
    }

    fn build_fetch_request_from(
        replica: NodeId,
        fetch_offset: u64,
        last_fetched_epoch: u64,
        leader_epoch: u64,
    ) -> FetchRequest {
        FetchRequest {
            cluster_id: "test".into(),
            leader_epoch,
            replica_id: replica,
            fetch_offset: LogIndex(fetch_offset),
            last_fetched_epoch: Term(last_fetched_epoch),
        }
    }

    fn build_fetch_response_from(
        leader: NodeId,
        leader_epoch: u64,
        high_watermark: u64,
        entries: Vec<Entry>,
        diverging_epoch: Option<DivergingEpoch>,
    ) -> FetchResponse {
        FetchResponse {
            cluster_id: "test".into(),
            leader_epoch,
            leader_id: leader,
            high_watermark: LogIndex(high_watermark),
            entries,
            diverging_epoch,
            snapshot_redirect: None,
            is_leader: true,
        }
    }

    /// Scenario: basic-replication
    ///
    /// Per `implementation-plan.md` Stage 3.3 scenario: "Given a 3-node
    /// cluster with node 1 as leader, When followers send Fetch RPCs,
    /// Then the leader responds with new entries and **after two fetch
    /// rounds** all followers have the entry and the high watermark
    /// advances".
    ///
    /// This requires careful handling of the `confirmed_offset` semantics:
    /// `FetchRequest.fetch_offset = N` means the follower wants entry N
    /// next, which only proves it has entries up to `N - 1`. The driver
    /// (after validating divergence on the leader's `LogStore`) feeds
    /// `Input::FetchRequestAcked { confirmed_offset: req.fetch_offset - 1 }`
    /// ΓÇö NOT `req.fetch_offset`. Concretely:
    ///
    /// - Round 1: req(fetch_offset=1, last_fetched_epoch=0) ΓåÆ ServeFetch.
    ///   Driver acks with confirmed_offset=0 (the follower is empty).
    ///   Leader's view: peer 2 last_fetch_offset=0. Quorum (sorted desc:
    ///   leader=1, peer 2=0, peer 3=0) ΓåÆ q-th=offsets[1]=0. Commit does
    ///   NOT advance.
    /// - Round 2: req(fetch_offset=2, last_fetched_epoch=1) ΓåÆ ServeFetch.
    ///   Driver acks with confirmed_offset=1 (the follower has entry 1).
    ///   Leader's view: peer 2 last_fetch_offset=1. Quorum (sorted desc:
    ///   leader=1, peer 2=1, peer 3=0) ΓåÆ q-th=offsets[1]=1. Commit
    ///   advances to 1; ApplyToStateMachine{1,1} emitted.
    #[test]
    fn scenario_basic_replication() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1).unwrap();
        drive_three_voter_to_leader(&mut node);
        // Sanity: leader, term=1, no-op at index 1, commit still 0.
        assert_eq!(node.role, NodeRole::Leader);
        assert_eq!(node.current_term(), Term(1));
        assert_eq!(node.last_log_index, LogIndex(1));
        assert_eq!(node.commit_index, LogIndex(0));
        assert_eq!(node.last_applied, LogIndex(0));

        // ---------- Round 1: follower asks for entry 1 (has nothing) ----------
        let req1 = build_fetch_request_from(NodeId(2), 1, 0, /*leader_epoch=*/ 1);
        let actions1 = node.step(Input::FetchRequest(req1));

        // Leader emits exactly one ServeFetch carrying the right envelope.
        let serve1 = actions1
            .iter()
            .find(|a| matches!(a, Action::ServeFetch { .. }))
            .expect("ServeFetch action present in round 1");
        match serve1 {
            Action::ServeFetch {
                to,
                cluster_id,
                leader_epoch,
                leader_id,
                high_watermark,
                fetch_offset,
                last_fetched_epoch,
            } => {
                assert_eq!(*to, NodeId(2));
                assert_eq!(cluster_id, "test");
                assert_eq!(*leader_epoch, 1);
                assert_eq!(*leader_id, NodeId(1));
                assert_eq!(*high_watermark, LogIndex(0));
                assert_eq!(*fetch_offset, LogIndex(1));
                assert_eq!(*last_fetched_epoch, Term(0));
            }
            other => panic!("expected ServeFetch, got {other:?}"),
        }

        // Critical Stage 3.3 invariant: receipt of FetchRequest alone must
        // NOT advance per-peer replication progress (the engine has not
        // yet validated the follower's `last_fetched_epoch` against the
        // log). The driver must subsequently feed FetchRequestAcked.
        assert_eq!(
            node.peers.get(&NodeId(2)).unwrap().last_fetch_offset,
            LogIndex(0),
            "FetchRequest receipt must not advance peer.last_fetch_offset"
        );

        // Driver feedback for round 1: confirmed_offset = req.fetch_offset - 1
        // = 0. The follower has CONFIRMED having entries up through offset
        // 0 (i.e. nothing). It has not yet stored entry 1.
        let ack1 = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(2),
            confirmed_offset: LogIndex(0),
        });

        // Per-peer progress reflects the round-1 ack but commit must NOT
        // advance: only 1-of-3 voters (the leader) is at offset >= 1.
        assert_eq!(
            node.peers.get(&NodeId(2)).unwrap().last_fetch_offset,
            LogIndex(0)
        );
        assert_eq!(
            node.commit_index,
            LogIndex(0),
            "single-round confirmation must NOT advance commit (offset 0 is no progress)"
        );
        assert!(
            !ack1
                .iter()
                .any(|a| matches!(a, Action::ApplyToStateMachine { .. })),
            "no ApplyToStateMachine when commit did not advance"
        );

        // ---------- Round 2: follower asks for entry 2 (now has entry 1) ----------
        let req2 = build_fetch_request_from(NodeId(2), 2, 1, /*leader_epoch=*/ 1);
        let actions2 = node.step(Input::FetchRequest(req2));
        let serve2 = actions2
            .iter()
            .find(|a| matches!(a, Action::ServeFetch { .. }))
            .expect("ServeFetch action present in round 2");
        if let Action::ServeFetch {
            fetch_offset,
            last_fetched_epoch,
            ..
        } = serve2
        {
            assert_eq!(*fetch_offset, LogIndex(2));
            assert_eq!(*last_fetched_epoch, Term(1));
        }

        // Driver feedback for round 2: confirmed_offset = req.fetch_offset
        // - 1 = 1. The follower has now CONFIRMED having entry 1.
        let ack2 = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(2),
            confirmed_offset: LogIndex(1),
        });

        // Per-peer progress recorded; commit advances to 1 (2-of-3 majority:
        // leader at last_log_index=1 + node 2 at offset=1, with node 3
        // still at 0). Figure-8 gate satisfied (no_op_index=1).
        assert_eq!(
            node.peers.get(&NodeId(2)).unwrap().last_fetch_offset,
            LogIndex(1)
        );
        assert_eq!(node.commit_index, LogIndex(1));
        assert_eq!(node.last_applied, LogIndex(1));
        let apply = ack2
            .iter()
            .find(|a| matches!(a, Action::ApplyToStateMachine { .. }))
            .expect("ApplyToStateMachine emitted after the second-round ack");
        match apply {
            Action::ApplyToStateMachine { from, to } => {
                assert_eq!(*from, LogIndex(1));
                assert_eq!(*to, LogIndex(1));
            }
            other => panic!("expected ApplyToStateMachine, got {other:?}"),
        }
    }

    /// Scenario: commit-requires-majority
    ///
    /// Given a 5-voter cluster with node 1 as leader (term 1, no-op at
    /// index 1), When only ONE peer acks (2-of-5 ΓÇö short of 3-of-5 quorum)
    /// Then `commit_index` does NOT advance. When a second peer acks
    /// (3-of-5 majority including the leader) Then `commit_index` advances
    /// to 1 and `ApplyToStateMachine` is emitted.
    #[test]
    fn scenario_commit_requires_majority() {
        let mut node = RaftNode::new_with_seed(five_voter_config(), 7).unwrap();
        // Drive into leader on the 5-voter cluster: 3-of-5 votes needed.
        let max_ticks = node.election_timer.max_ticks() + 5;
        for _ in 0..max_ticks {
            node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate {
                break;
            }
        }
        assert_eq!(node.role, NodeRole::PreCandidate);
        // Two pre-vote grants ΓåÆ 3-of-5 (incl. self) ΓåÆ Candidate.
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(3),
            response: pre_vote_resp("test", 0, true),
        });
        assert_eq!(node.role, NodeRole::Candidate);
        // Two vote grants ΓåÆ 3-of-5 (incl. self) ΓåÆ Leader.
        let cur_term = node.current_term().0;
        let _ = node.step(Input::VoteResponse {
            from: NodeId(2),
            response: vote_resp("test", cur_term, true),
        });
        let _ = node.step(Input::VoteResponse {
            from: NodeId(3),
            response: vote_resp("test", cur_term, true),
        });
        assert_eq!(node.role, NodeRole::Leader);
        assert_eq!(node.last_log_index, LogIndex(1));
        assert_eq!(node.commit_index, LogIndex(0));

        // Only ONE peer acks: leader self (offset=1) + node 2 (offset=1)
        // = 2-of-5 ΓåÆ no quorum ΓåÆ no commit advance.
        let one_ack = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(2),
            confirmed_offset: LogIndex(1),
        });
        assert_eq!(
            node.commit_index,
            LogIndex(0),
            "single ack must NOT reach 3-of-5 quorum"
        );
        assert!(
            !one_ack
                .iter()
                .any(|a| matches!(a, Action::ApplyToStateMachine { .. })),
            "no ApplyToStateMachine when commit did not advance"
        );

        // Second peer acks: 3-of-5 majority (self + node 2 + node 3) ΓåÆ commit advances.
        let two_ack = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(3),
            confirmed_offset: LogIndex(1),
        });
        assert_eq!(node.commit_index, LogIndex(1));
        assert_eq!(node.last_applied, LogIndex(1));
        assert!(
            two_ack.iter().any(|a| matches!(
                a,
                Action::ApplyToStateMachine { from, to } if *from == LogIndex(1) && *to == LogIndex(1)
            )),
            "expected ApplyToStateMachine{{1,1}} after majority ack, got {two_ack:?}"
        );

        // A 4th and 5th ack at the same offset must NOT re-emit
        // ApplyToStateMachine (last_applied is already 1).
        let extra = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(4),
            confirmed_offset: LogIndex(1),
        });
        assert!(
            !extra
                .iter()
                .any(|a| matches!(a, Action::ApplyToStateMachine { .. })),
            "redundant ack must not re-emit ApplyToStateMachine"
        );
    }

    /// Scenario: follower-conflict-resolution
    ///
    /// Given a follower at term 5 with leader_id known and a divergent
    /// log tail, When the leader sends a FetchResponse with
    /// `diverging_epoch = Some { epoch: 3, end_offset: 7 }`, Then the
    /// follower emits `TruncateLog { from_index_inclusive: 8 }` AND a
    /// re-fetch SendMessage carrying `fetch_offset: 8`, `last_fetched_epoch: 3`
    /// to the leader.
    #[test]
    fn scenario_follower_conflict_resolution() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 11).unwrap();
        node.hard_state.current_term = Term(5);
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));
        node.last_log_index = LogIndex(10);
        node.last_log_term = Term(4);

        let resp = build_fetch_response_from(
            NodeId(2),
            /*leader_epoch=*/ 5,
            /*high_watermark=*/ 0,
            Vec::new(),
            Some(DivergingEpoch {
                epoch: Term(3),
                end_offset: LogIndex(7),
            }),
        );
        let actions = node.step(Input::FetchResponse(resp));

        // TruncateLog action with from_index_inclusive = end_offset + 1.
        let trunc = actions
            .iter()
            .find(|a| matches!(a, Action::TruncateLog(_)))
            .expect("TruncateLog action emitted on divergence");
        match trunc {
            Action::TruncateLog(LogTruncation::SuffixFromInclusive {
                from_index_inclusive,
            }) => {
                assert_eq!(*from_index_inclusive, LogIndex(8));
            }
            other => panic!("expected TruncateLog(SuffixFromInclusive), got {other:?}"),
        }

        // Re-fetch SendMessage with leader-supplied consistent point.
        let refetch = actions
            .iter()
            .find_map(|a| match a {
                Action::SendMessage {
                    to,
                    message: OutboundMessage::FetchRequest(r),
                } => Some((*to, r.clone())),
                _ => None,
            })
            .expect("re-fetch SendMessage emitted on divergence");
        assert_eq!(refetch.0, NodeId(2));
        assert_eq!(refetch.1.fetch_offset, LogIndex(8));
        assert_eq!(refetch.1.last_fetched_epoch, Term(3));
        assert_eq!(refetch.1.leader_epoch, 5);
        assert_eq!(refetch.1.replica_id, NodeId(1));

        // The handler must NOT mutate last_log_index/term itself ΓÇö
        // truncation is the driver's job, and only the driver knows the
        // post-truncation tail (see rubber-duck blocking issue #3).
        assert_eq!(node.last_log_index, LogIndex(10));
        assert_eq!(node.last_log_term, Term(4));
    }

    /// Scenario: fetch-resets-election-timer
    ///
    /// Given a Follower with a known leader_id whose election timer has
    /// been advanced near expiry, When a valid same-term FetchResponse
    /// (empty entries) arrives, Then the election timer is reset and the
    /// follower's `last_leader_contact_tick` is refreshed ΓÇö i.e. proof of
    /// leader liveness suppresses the impending election.
    #[test]
    fn scenario_fetch_resets_election_timer() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 13).unwrap();
        node.hard_state.current_term = Term(2);
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));
        // Advance election timer almost to expiry.
        let max = node.election_timer.max_ticks();
        for _ in 0..max {
            node.election_timer.tick();
        }
        let pre_elapsed = node.election_timer.elapsed();
        assert!(
            pre_elapsed > 0,
            "election timer must have advanced before reset"
        );

        // Empty same-term response from the known leader.
        let resp = build_fetch_response_from(
            NodeId(2),
            /*leader_epoch=*/ 2,
            /*high_watermark=*/ 0,
            Vec::new(),
            None,
        );
        let _ = node.step(Input::FetchResponse(resp));

        // Election timer reset.
        assert_eq!(
            node.election_timer.elapsed(),
            0,
            "valid FetchResponse must reset the election timer"
        );
        // last_leader_contact_tick refreshed.
        assert!(
            node.last_leader_contact_tick.is_some(),
            "valid FetchResponse must refresh last_leader_contact_tick"
        );
        // Role unchanged (still Follower) and leader_id unchanged.
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.leader_id, Some(NodeId(2)));
    }

    /// Scenario: stale-leader-steps-down
    ///
    /// Given this node is Leader at term 3, When it receives a FetchRequest
    /// carrying `leader_epoch = 5` (higher than its own term), Then it
    /// adopts term 5, steps down to Follower with `voted_for = None`, and
    /// emits an `Action::PersistHardState` so the new term is durable
    /// before any further reaction. The stale leader does NOT serve the
    /// fetch (no `ServeFetch` is emitted).
    #[test]
    fn scenario_stale_leader_steps_down_on_fetch_request() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 17).unwrap();
        drive_three_voter_to_leader(&mut node);
        assert_eq!(node.role, NodeRole::Leader);
        // Force term to 3 to make the test deterministic vs the leader-cascade term.
        node.hard_state.current_term = Term(3);

        let req = build_fetch_request_from(NodeId(2), 1, 0, /*leader_epoch=*/ 5);
        let actions = node.step(Input::FetchRequest(req));

        // Stepped down at the higher term.
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(5));
        assert_eq!(node.hard_state.voted_for, None);
        // PersistHardState emitted before any RPC reply.
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "expected PersistHardState on step-down, got {actions:?}"
        );
        // No ServeFetch ΓÇö we are no longer leader.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::ServeFetch { .. })),
            "stale leader must not serve the fetch, got {actions:?}"
        );
    }

    /// Stage 3.3 finding-4 fix: when a higher-term `FetchResponse` arrives
    /// at this node, after stepping down to the new term we MUST still
    /// process the response's entries and high watermark ΓÇö they are now
    /// same-term valid under the new term. Dropping the payload (the prior
    /// behavior) silently delays follower catch-up by one round.
    ///
    /// Given: this node is a Candidate at term 2 (no leader_id).
    /// When: a FetchResponse arrives carrying leader_epoch=3 (higher term),
    ///       leader_id=2, and entries=[entry(idx=1, term=3)] with
    ///       high_watermark=1.
    /// Then: the node steps down to Follower at term 3 with leader_id=2,
    ///       AND emits AppendEntries(entries) AND advances commit_index to 1
    ///       AND emits ApplyToStateMachine{1,1}.
    #[test]
    fn scenario_higher_term_fetch_response_processes_entries_after_stepdown() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 53).unwrap();
        // Drive into Candidate at term 1.
        let max_ticks = node.election_timer.max_ticks() + 5;
        for _ in 0..max_ticks {
            node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate {
                break;
            }
        }
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        assert_eq!(node.role, NodeRole::Candidate);
        // Force term to 2 so the higher-term arithmetic is unambiguous.
        node.hard_state.current_term = Term(2);

        // Higher-term response with entries.
        let entry1 = Entry {
            index: LogIndex(1),
            term: Term(3),
            payload: EntryPayload::NoOp,
        };
        let resp = build_fetch_response_from(
            /*leader=*/ NodeId(2),
            /*leader_epoch=*/ 3,
            /*high_watermark=*/ 1,
            vec![entry1.clone()],
            None,
        );
        let actions = node.step(Input::FetchResponse(resp));

        // Stepped down at the higher term with the new leader hint.
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(3));
        assert_eq!(node.leader_id, Some(NodeId(2)));
        // PersistHardState emitted by become_follower.
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "expected PersistHardState on higher-term step-down, got {actions:?}"
        );
        // CRITICAL: entries from the response are processed (NOT dropped).
        let appended = actions
            .iter()
            .find_map(|a| match a {
                Action::AppendEntries(es) => Some(es.clone()),
                _ => None,
            })
            .expect("AppendEntries MUST be emitted after higher-term step-down");
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].index, LogIndex(1));
        assert_eq!(appended[0].term, Term(3));
        // In-memory mirror advanced.
        assert_eq!(node.last_log_index, LogIndex(1));
        assert_eq!(node.last_log_term, Term(3));
        // High watermark propagated and apply emitted.
        assert_eq!(node.commit_index, LogIndex(1));
        assert_eq!(node.last_applied, LogIndex(1));
        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::ApplyToStateMachine { from, to } if *from == LogIndex(1) && *to == LogIndex(1)
            )),
            "expected ApplyToStateMachine{{1,1}} after higher-term step-down, got {actions:?}"
        );
    }

    /// Stage 3.3 finding-5 fix: a same-term `FetchResponse` from a leader
    /// id that does NOT match this follower's already-known leader is a
    /// Raft safety violation (two same-term leaders cannot coexist). The
    /// response MUST be dropped, the existing `leader_id` preserved, AND
    /// the election timer NOT reset (so a divergent claimant cannot
    /// suppress a genuine election timeout).
    ///
    /// Given: this node is a Follower at term 4 with leader_id=Some(2)
    ///        and an election timer advanced near expiry.
    /// When: a FetchResponse arrives at the same term with leader_id=3.
    /// Then: leader_id remains Some(2), no AppendEntries is emitted, no
    ///       ApplyToStateMachine is emitted, AND election_timer.elapsed()
    ///       is unchanged (no reset).
    #[test]
    fn scenario_same_term_response_from_different_leader_dropped() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 59).unwrap();
        node.hard_state.current_term = Term(4);
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));
        // Pre-populate with a known log entry so we can confirm no append
        // happens.
        node.last_log_index = LogIndex(5);
        node.last_log_term = Term(4);
        node.commit_index = LogIndex(3);
        node.last_applied = LogIndex(3);

        // Advance the election timer near-expiry.
        let max = node.election_timer.max_ticks();
        for _ in 0..max {
            node.election_timer.tick();
        }
        let pre_elapsed = node.election_timer.elapsed();
        assert!(pre_elapsed > 0, "election timer must have advanced");

        // Spurious response from a DIFFERENT same-term leader, claiming
        // entries we don't have AND a higher HW than we currently know.
        let bogus_entry = Entry {
            index: LogIndex(6),
            term: Term(4),
            payload: EntryPayload::Command(bytes::Bytes::from_static(b"bogus")),
        };
        let resp = build_fetch_response_from(
            /*leader=*/ NodeId(3), // different from known leader (2)
            /*leader_epoch=*/ 4,
            /*high_watermark=*/ 5,
            vec![bogus_entry],
            None,
        );
        let actions = node.step(Input::FetchResponse(resp));

        // Response is dropped ΓÇö no actions emitted.
        assert!(
            actions.is_empty(),
            "two same-term leaders must drop with no actions, got {actions:?}"
        );
        // leader_id PRESERVED ΓÇö no overwrite.
        assert_eq!(node.leader_id, Some(NodeId(2)));
        // No log mutation, no commit/apply mutation.
        assert_eq!(node.last_log_index, LogIndex(5));
        assert_eq!(node.last_log_term, Term(4));
        assert_eq!(node.commit_index, LogIndex(3));
        assert_eq!(node.last_applied, LogIndex(3));
        // Election timer NOT reset ΓÇö a spurious response cannot suppress
        // a genuine timeout.
        assert_eq!(
            node.election_timer.elapsed(),
            pre_elapsed,
            "spurious same-term response must NOT reset election timer"
        );
    }

    /// Stage 3.3 finding-6 fix: the leader MUST NOT serve a FetchRequest
    /// from a replica that is neither a configured voter NOR a tracked
    /// peer. Such requests come from misrouted, malicious, or stale-config
    /// senders; serving them wastes leader bandwidth and risks polluting
    /// quorum calculations if the same id later becomes a tracked peer.
    ///
    /// Given: this node is the leader of a 3-voter cluster (voters: 1,2,3)
    ///        with peer-progress entries for 2 and 3.
    /// When: a FetchRequest arrives from replica_id=99 (not a voter, not a
    ///       known peer).
    /// Then: no actions are emitted (in particular, no `ServeFetch`),
    ///       and no peer record is created for 99.
    #[test]
    fn scenario_fetch_request_from_unknown_replica_dropped() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 67).unwrap();
        drive_three_voter_to_leader(&mut node);
        assert_eq!(node.role, NodeRole::Leader);
        // Sanity: peers map only contains voters 2 and 3.
        assert!(node.peers.contains_key(&NodeId(2)));
        assert!(node.peers.contains_key(&NodeId(3)));
        assert!(!node.peers.contains_key(&NodeId(99)));

        // Stale or misrouted request from an unknown replica.
        let req = build_fetch_request_from(NodeId(99), 1, 0, /*leader_epoch=*/ 1);
        let actions = node.step(Input::FetchRequest(req));

        // Dropped silently ΓÇö no actions whatsoever.
        assert!(
            actions.is_empty(),
            "unknown-replica FetchRequest must be dropped, got {actions:?}"
        );
        // No phantom peer record created.
        assert!(
            !node.peers.contains_key(&NodeId(99)),
            "unknown-replica FetchRequest must not auto-create a peer record"
        );
        // In particular, the un-tracked replica did not get a ServeFetch.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::ServeFetch { .. })),
            "unknown-replica FetchRequest must not emit ServeFetch"
        );
    }

    /// Stage 3.3 finding-1 fix (iter 4): an unknown same-cluster replica
    /// that sends a FetchRequest with `leader_epoch > current_term` must
    /// NOT be able to force the leader to step down or bump its term.
    /// Prior to iter 4 the higher-term reconciliation branch ran BEFORE
    /// the unknown-replica guard, so an unknown attacker could trip
    /// `become_follower(Term(req.leader_epoch), None)` at will and
    /// disrupt cluster leadership. The guard now runs first; this test
    /// pins that ordering.
    ///
    /// Given: this node is the leader of a 3-voter cluster (voters: 1,2,3)
    ///        at term 2 (forced via test setup).
    /// When: a FetchRequest arrives from `replica_id = NodeId(99)` (NOT
    ///       a configured voter, NOT a tracked peer) carrying
    ///       `leader_epoch = 10` (much higher than our term 2).
    /// Then: no actions are emitted, the leader stays Leader, the term
    ///       stays at 2, leader_id is unchanged, and the election timer
    ///       is NOT reset (a divergent claimant must not be able to
    ///       suppress a genuine election timeout from a leader either).
    #[test]
    fn scenario_unknown_replica_higher_term_fetch_request_cannot_force_stepdown() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 83).unwrap();
        drive_three_voter_to_leader(&mut node);
        assert_eq!(node.role, NodeRole::Leader);
        // Force a known starting term so the higher-epoch arithmetic is
        // unambiguous. become_leader leaves us at term 1; bump to 2.
        node.hard_state.current_term = Term(2);
        // Snapshot pre-state so we can prove nothing mutated.
        let pre_term = node.current_term();
        let pre_role = node.role;
        let pre_leader_id = node.leader_id;
        // Advance election timer near expiry to verify it is NOT reset.
        let max = node.election_timer.max_ticks();
        for _ in 0..max {
            node.election_timer.tick();
        }
        let pre_elapsed = node.election_timer.elapsed();
        // Snapshot peer 2's liveness so we can confirm no liveness update
        // happened either (the guard runs before peer-liveness mutation).
        let pre_peer2_fetch_time = node.peers.get(&NodeId(2)).unwrap().last_fetch_time;

        // Bogus request: NodeId(99) is not in voter_set {1,2,3} and not in
        // peers map. Carries a much higher leader_epoch claim.
        let req = build_fetch_request_from(
            NodeId(99),
            /*fetch_offset=*/ 1,
            /*last_fetched_epoch=*/ 0,
            /*leader_epoch=*/ 10,
        );
        let actions = node.step(Input::FetchRequest(req));

        // No actions emitted (in particular: no PersistHardState that
        // would have accompanied a become_follower term bump).
        assert!(
            actions.is_empty(),
            "unknown replica with higher leader_epoch must be dropped, got {actions:?}"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "unknown replica must NOT trigger PersistHardState (no term bump)"
        );
        // Term, role, leader_id all unchanged.
        assert_eq!(
            node.current_term(),
            pre_term,
            "unknown replica must NOT bump our term (was {pre_term:?}, now {:?})",
            node.current_term()
        );
        assert_eq!(
            node.role, pre_role,
            "unknown replica must NOT cause role change away from Leader"
        );
        assert_eq!(
            node.leader_id, pre_leader_id,
            "unknown replica must NOT mutate leader_id"
        );
        // Election timer not reset (become_follower would have reset it).
        assert_eq!(
            node.election_timer.elapsed(),
            pre_elapsed,
            "unknown replica must NOT reset election timer"
        );
        // No phantom peer record created.
        assert!(
            !node.peers.contains_key(&NodeId(99)),
            "unknown replica must NOT auto-create a peer record"
        );
        // Existing peer (NodeId(2)) liveness untouched.
        assert_eq!(
            node.peers.get(&NodeId(2)).unwrap().last_fetch_time,
            pre_peer2_fetch_time,
            "unknown replica must NOT mutate other peers' liveness fields"
        );
    }

    /// Stage 3.3 finding-3 fix (iter 3): a `FetchRequest` with
    /// `fetch_offset == LogIndex(0)` is structurally invalid because the
    /// architecture defines `fetch_offset` as the next 1-based log index
    /// the follower wants. The driver derives the confirmed offset by
    /// subtracting one (`fetch_offset - 1`); a 0 would underflow. The
    /// leader MUST drop such requests before emitting `Action::ServeFetch`
    /// so a malformed sender cannot consume leader bandwidth nor corrupt
    /// the per-peer progress map.
    ///
    /// Given: this node is the leader of a 3-voter cluster.
    /// When: a known voter (NodeId(2)) sends a FetchRequest with
    ///       fetch_offset=LogIndex(0).
    /// Then: no actions are emitted (in particular no ServeFetch), and
    ///       the peer's progress is unchanged.
    #[test]
    fn scenario_fetch_request_with_zero_offset_dropped() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 71).unwrap();
        drive_three_voter_to_leader(&mut node);
        assert_eq!(node.role, NodeRole::Leader);
        // Snapshot peer-2 progress before the bogus request.
        let pre_offset = node.peers.get(&NodeId(2)).unwrap().last_fetch_offset;
        let pre_fetch_time = node.peers.get(&NodeId(2)).unwrap().last_fetch_time;

        // Bogus request: fetch_offset=0 from a known voter.
        let req = build_fetch_request_from(
            NodeId(2),
            /*fetch_offset=*/ 0,
            /*last_fetched_epoch=*/ 0,
            /*leader_epoch=*/ 1,
        );
        let actions = node.step(Input::FetchRequest(req));

        // Dropped silently ΓÇö no ServeFetch, no other actions.
        assert!(
            actions.is_empty(),
            "fetch_offset=0 must be dropped, got {actions:?}"
        );
        // Per-peer progress unchanged (no liveness update either).
        let post_offset = node.peers.get(&NodeId(2)).unwrap().last_fetch_offset;
        let post_fetch_time = node.peers.get(&NodeId(2)).unwrap().last_fetch_time;
        assert_eq!(
            post_offset, pre_offset,
            "fetch_offset=0 must not mutate per-peer last_fetch_offset"
        );
        assert_eq!(
            post_fetch_time, pre_fetch_time,
            "fetch_offset=0 must not refresh per-peer last_fetch_time"
        );
    }

    /// Stage 3.3 finding-1 fix (iter 3): a `FetchResponse` whose
    /// `leader_id` is neither a configured voter nor a known peer MUST
    /// be dropped before any state mutation. Two attack vectors are
    /// closed by this check:
    /// (a) higher-term path ΓÇö without the guard, an unknown sender could
    ///     force a term bump via `become_follower(Term, Some(unknown))`.
    /// (b) same-term `leader_id == None` path ΓÇö without the guard, an
    ///     unknown sender could set `self.leader_id = Some(unknown)` and
    ///     then push entries the follower would accept.
    /// This test exercises both: (a) higher-term unknown-leader response
    /// must NOT bump our term, and (b) same-term-with-no-known-leader
    /// unknown-leader response must NOT adopt the unknown leader.
    #[test]
    fn scenario_fetch_response_from_unknown_leader_dropped() {
        // ---------- Case (a): higher-term unknown-leader ----------
        let mut node = RaftNode::new_with_seed(three_voter_config(), 73).unwrap();
        node.hard_state.current_term = Term(2);
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));

        // Snapshot the election timer so we can assert it is not reset.
        let max = node.election_timer.max_ticks();
        for _ in 0..max {
            node.election_timer.tick();
        }
        let pre_elapsed = node.election_timer.elapsed();

        // Higher-term FetchResponse claiming to be from NodeId(99) (NOT a
        // configured voter in `three_voter_config`, NOT a known peer).
        let bogus_entry = Entry {
            index: LogIndex(1),
            term: Term(5),
            payload: EntryPayload::NoOp,
        };
        let resp = build_fetch_response_from(
            /*leader=*/ NodeId(99),
            /*leader_epoch=*/ 5, // higher than current term 2
            /*high_watermark=*/ 1,
            vec![bogus_entry],
            None,
        );
        let actions = node.step(Input::FetchResponse(resp));

        // Dropped ΓÇö NO term bump, NO leader change, NO entries appended.
        assert!(
            actions.is_empty(),
            "unknown-leader FetchResponse must be dropped, got {actions:?}"
        );
        assert_eq!(
            node.current_term(),
            Term(2),
            "unknown-leader response must NOT bump term"
        );
        assert_eq!(
            node.leader_id,
            Some(NodeId(2)),
            "unknown-leader response must NOT overwrite leader_id"
        );
        assert_eq!(node.last_log_index, LogIndex(0));
        assert_eq!(node.commit_index, LogIndex(0));
        assert_eq!(
            node.election_timer.elapsed(),
            pre_elapsed,
            "unknown-leader response must NOT reset election timer"
        );

        // ---------- Case (b): same-term, leader_id was None, unknown leader ----------
        let mut node2 = RaftNode::new_with_seed(three_voter_config(), 74).unwrap();
        node2.hard_state.current_term = Term(3);
        node2.role = NodeRole::Follower;
        node2.leader_id = None; // no known leader yet
        let max2 = node2.election_timer.max_ticks();
        for _ in 0..max2 {
            node2.election_timer.tick();
        }
        let pre_elapsed2 = node2.election_timer.elapsed();
        let resp2 = build_fetch_response_from(
            /*leader=*/ NodeId(99), // unknown
            /*leader_epoch=*/ 3, // same term
            /*high_watermark=*/ 0,
            Vec::new(),
            None,
        );
        let actions2 = node2.step(Input::FetchResponse(resp2));
        assert!(
            actions2.is_empty(),
            "same-term unknown-leader response must be dropped, got {actions2:?}"
        );
        assert_eq!(
            node2.leader_id, None,
            "same-term unknown-leader response must NOT establish leader_id"
        );
        assert_eq!(
            node2.election_timer.elapsed(),
            pre_elapsed2,
            "same-term unknown-leader response must NOT reset election timer"
        );
    }

    /// Stage 7.2 iter-3 finding #3 regression test: an observer is
    /// seeded into `peers` so the leader's `handle_fetch_request`
    /// known-sender guard accepts the observer's Fetch RPC. But the
    /// REVERSE direction (a FetchResponse with `is_leader = true`
    /// claiming an observer is the leader) must be DROPPED — observers
    /// are non-voting and cannot be leaders. The iter-2 predicate
    /// `is_known_voter || peers.contains_key` accidentally let a
    /// crafted/buggy observer response claim leadership. This test
    /// exercises both the higher-term path (term must not bump) and
    /// the same-term path (leader_id must not be adopted).
    #[test]
    fn fetch_response_with_observer_as_leader_dropped() {
        // 3 voters {1,2,3} + 1 observer {4}, this node = voter 1.
        let cfg = three_voter_one_observer_config(1);

        // ---------- Case (a): same term, no known leader yet,
        //                       observer claims leadership. ----------
        let mut node = RaftNode::new_with_seed(cfg.clone(), 91).unwrap();
        // Sanity: node 4 IS in peers (observer-seeded) but is NOT a voter.
        assert!(
            node.peers.contains_key(&NodeId(4)),
            "observer 4 must be seeded into peers (defense for handle_fetch_request)"
        );
        assert!(
            !node.is_known_voter(NodeId(4)),
            "observer 4 must NOT be a configured voter"
        );

        node.hard_state.current_term = Term(3);
        node.role = NodeRole::Follower;
        node.leader_id = None;
        let max = node.election_timer.max_ticks();
        for _ in 0..max {
            node.election_timer.tick();
        }
        let pre_elapsed = node.election_timer.elapsed();

        // Observer 4 claims leadership with is_leader=true at same term.
        let resp_same = build_fetch_response_from(
            /*leader=*/ NodeId(4),
            /*leader_epoch=*/ 3,
            /*high_watermark=*/ 0,
            Vec::new(),
            None,
        );
        let actions_same = node.step(Input::FetchResponse(resp_same));
        assert!(
            actions_same.is_empty(),
            "same-term observer-as-leader FetchResponse must be dropped, got {actions_same:?}"
        );
        assert_eq!(
            node.leader_id, None,
            "observer-as-leader must NOT establish leader_id"
        );
        assert_eq!(
            node.current_term(),
            Term(3),
            "observer-as-leader must NOT change term"
        );
        assert_eq!(
            node.election_timer.elapsed(),
            pre_elapsed,
            "observer-as-leader response must NOT reset election timer"
        );

        // ---------- Case (b): HIGHER term, observer claims leadership
        //                       with entries. MUST be dropped (no
        //                       term bump, no log mutation). ----------
        let mut node2 = RaftNode::new_with_seed(cfg, 92).unwrap();
        node2.hard_state.current_term = Term(2);
        node2.role = NodeRole::Follower;
        node2.leader_id = Some(NodeId(2));
        let bogus = Entry {
            index: LogIndex(1),
            term: Term(7),
            payload: EntryPayload::NoOp,
        };
        let resp_higher = build_fetch_response_from(
            /*leader=*/ NodeId(4), // observer
            /*leader_epoch=*/ 7, // higher term
            /*high_watermark=*/ 1,
            vec![bogus],
            None,
        );
        let actions_higher = node2.step(Input::FetchResponse(resp_higher));
        assert!(
            actions_higher.is_empty(),
            "higher-term observer-as-leader FetchResponse must be dropped, got {actions_higher:?}"
        );
        assert_eq!(
            node2.current_term(),
            Term(2),
            "observer-as-leader response must NOT bump term — election safety violated otherwise"
        );
        assert_eq!(
            node2.leader_id,
            Some(NodeId(2)),
            "observer-as-leader response must NOT overwrite leader_id"
        );
        assert_eq!(
            node2.last_log_index,
            LogIndex(0),
            "observer-as-leader response must NOT append entries"
        );
        assert_eq!(
            node2.commit_index,
            LogIndex(0),
            "observer-as-leader response must NOT advance commit_index"
        );
    }

    /// follower's log with a gap at index 2 and silently violate Raft's
    /// log-matching invariant. Validate every adjacent pair before
    /// appending; drop the entire response on any gap.
    ///
    /// Given: this node is a Follower at term 5 with a fresh log
    ///        (last_log_index = 0) and leader_id = Some(2).
    /// When: a same-term FetchResponse arrives from leader 2 with entries
    ///       at indices [1, 3] (gap at 2).
    /// Then: no AppendEntries action is emitted, last_log_index stays 0,
    ///       and commit_index stays 0.
    #[test]
    fn scenario_fetch_response_with_intra_batch_gap_dropped() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 79).unwrap();
        node.hard_state.current_term = Term(5);
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));
        // Sanity baseline.
        assert_eq!(node.last_log_index, LogIndex(0));
        assert_eq!(node.commit_index, LogIndex(0));

        let entry_1 = Entry {
            index: LogIndex(1),
            term: Term(5),
            payload: EntryPayload::NoOp,
        };
        let entry_3 = Entry {
            index: LogIndex(3), // GAP: 2 is missing
            term: Term(5),
            payload: EntryPayload::NoOp,
        };
        let resp = build_fetch_response_from(
            /*leader=*/ NodeId(2),
            /*leader_epoch=*/ 5,
            /*high_watermark=*/ 3,
            vec![entry_1, entry_3],
            None,
        );
        let actions = node.step(Input::FetchResponse(resp));

        // No AppendEntries (response dropped after fence-checks).
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::AppendEntries(_))),
            "gapped batch must NOT be appended, got {actions:?}"
        );
        // Log mirror unchanged.
        assert_eq!(
            node.last_log_index,
            LogIndex(0),
            "gapped batch must NOT advance last_log_index"
        );
        // Commit index NOT advanced (we never accepted the entries).
        assert_eq!(
            node.commit_index,
            LogIndex(0),
            "gapped batch must NOT advance commit_index"
        );
        // No ApplyToStateMachine emitted.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::ApplyToStateMachine { .. })),
            "gapped batch must NOT trigger ApplyToStateMachine"
        );
    }

    /// Companion to `scenario_stale_leader_steps_down_on_fetch_request`:
    /// a Candidate that receives a same-term FetchResponse from a leader
    /// must step down to Follower (the leader's existence proves the
    /// election has been decided).
    #[test]
    fn scenario_candidate_steps_down_on_same_term_fetch_response() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 23).unwrap();
        // Drive into Candidate at term 1 (do NOT cascade to Leader).
        let max_ticks = node.election_timer.max_ticks() + 5;
        for _ in 0..max_ticks {
            node.step(Input::Tick);
            if node.role == NodeRole::PreCandidate {
                break;
            }
        }
        let _ = node.step(Input::PreVoteResponse {
            from: NodeId(2),
            response: pre_vote_resp("test", 0, true),
        });
        assert_eq!(node.role, NodeRole::Candidate);
        let cur_term = node.current_term().0;

        // Leader (node 3) sends a same-term FetchResponse.
        let resp = build_fetch_response_from(NodeId(3), cur_term, 0, Vec::new(), None);
        let _ = node.step(Input::FetchResponse(resp));

        // Stepped down to Follower with leader_id = Some(3).
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(cur_term));
        assert_eq!(node.leader_id, Some(NodeId(3)));
    }

    /// Companion: an Observer / Follower that receives a FetchResponse
    /// containing entries appends them and propagates the high watermark.
    #[test]
    fn scenario_follower_appends_entries_and_advances_commit() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 31).unwrap();
        node.hard_state.current_term = Term(2);
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));
        // Follower starts with empty log.
        assert_eq!(node.last_log_index, LogIndex(0));
        assert_eq!(node.commit_index, LogIndex(0));

        let entry1 = Entry {
            index: LogIndex(1),
            term: Term(2),
            payload: EntryPayload::NoOp,
        };
        let entry2 = Entry {
            index: LogIndex(2),
            term: Term(2),
            payload: EntryPayload::Command(bytes::Bytes::from_static(b"x=1")),
        };

        let resp = build_fetch_response_from(
            NodeId(2),
            /*leader_epoch=*/ 2,
            /*high_watermark=*/ 1,
            vec![entry1.clone(), entry2.clone()],
            None,
        );
        let actions = node.step(Input::FetchResponse(resp));

        // AppendEntries action emitted with both entries.
        let appended = actions
            .iter()
            .find_map(|a| match a {
                Action::AppendEntries(es) => Some(es.clone()),
                _ => None,
            })
            .expect("AppendEntries emitted on entry receipt");
        assert_eq!(appended.len(), 2);
        assert_eq!(appended[0].index, LogIndex(1));
        assert_eq!(appended[1].index, LogIndex(2));

        // In-memory mirror advanced to the last entry.
        assert_eq!(node.last_log_index, LogIndex(2));
        assert_eq!(node.last_log_term, Term(2));

        // High watermark = min(resp.high_watermark, our last_log_index)
        // = min(1, 2) = 1; ApplyToStateMachine{1,1} emitted.
        assert_eq!(node.commit_index, LogIndex(1));
        assert_eq!(node.last_applied, LogIndex(1));
        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::ApplyToStateMachine { from, to } if *from == LogIndex(1) && *to == LogIndex(1)
            )),
            "expected ApplyToStateMachine{{1,1}}, got {actions:?}"
        );
    }

    /// ClientPropose on a single-voter cluster commits and applies in one
    /// step (quorum size 1, so the leader's own append already satisfies
    /// the majority requirement and the Figure-8 gate is met by the
    /// election no-op at index 1).
    #[test]
    fn scenario_client_propose_single_voter_commits_immediately() {
        let mut node = RaftNode::new_with_seed(single_voter_config(), 41).unwrap();
        // Drive into Leader (single-voter auto-promote via tick loop).
        let max_ticks = node.election_timer.max_ticks() + 5;
        for _ in 0..max_ticks {
            node.step(Input::Tick);
            if node.role == NodeRole::Leader {
                break;
            }
        }
        assert_eq!(node.role, NodeRole::Leader);
        // No-op already committed.
        assert_eq!(node.last_log_index, LogIndex(1));
        assert_eq!(node.commit_index, LogIndex(1));
        assert_eq!(node.last_applied, LogIndex(1));

        let actions = node.step(Input::ClientPropose(bytes::Bytes::from_static(b"set x 7")));

        // AppendEntries with a single command at index 2.
        let appended = actions
            .iter()
            .find_map(|a| match a {
                Action::AppendEntries(es) => Some(es.clone()),
                _ => None,
            })
            .expect("AppendEntries emitted on ClientPropose");
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].index, LogIndex(2));
        assert!(matches!(appended[0].payload, EntryPayload::Command(_)));

        // Commit advances to 2; ApplyToStateMachine{2,2} emitted.
        assert_eq!(node.last_log_index, LogIndex(2));
        assert_eq!(node.commit_index, LogIndex(2));
        assert_eq!(node.last_applied, LogIndex(2));
        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::ApplyToStateMachine { from, to } if *from == LogIndex(2) && *to == LogIndex(2)
            )),
            "expected ApplyToStateMachine{{2,2}}, got {actions:?}"
        );
    }

    /// ClientPropose on a 3-voter leader: exercises the **multi-voter**
    /// (non-auto-commit) path of §5.2 / Stage 3.3 step 5, complementing
    /// the single-voter auto-commit path covered by
    /// `scenario_client_propose_single_voter_commits_immediately`.
    ///
    /// On a multi-voter cluster the leader appends the new command
    /// entry locally and emits `Action::AppendEntries`, but the entry
    /// CANNOT commit until a quorum of voters has replicated it. Until
    /// then no `Action::ApplyToStateMachine` may be emitted and neither
    /// `commit_index` nor `last_applied` may move.
    ///
    /// This test verifies:
    ///   (a) the immediate ClientPropose response carries
    ///       `AppendEntries` for the new command at index 2 and **no**
    ///       `ApplyToStateMachine`; `commit_index` / `last_applied`
    ///       remain at 0 (not even the no-op at index 1 commits — no
    ///       follower has yet acked anything);
    ///   (b) commit advances only once a follower acks the new command
    ///       index. A partial ack at offset=1 (no-op only) commits the
    ///       no-op alone and leaves the command uncommitted; only the
    ///       subsequent ack at offset=2 — the 2nd-of-3 voter reaching
    ///       the new index — releases the command for apply.
    #[test]
    fn scenario_client_propose_three_voter_waits_for_quorum_ack() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 53).unwrap();
        drive_three_voter_to_leader(&mut node);
        // Sanity: leader at term 1, no-op at index 1, nothing committed,
        // both peers untouched at offset 0.
        assert_eq!(node.role, NodeRole::Leader);
        assert_eq!(node.current_term(), Term(1));
        assert_eq!(node.last_log_index, LogIndex(1));
        assert_eq!(node.commit_index, LogIndex(0));
        assert_eq!(node.last_applied, LogIndex(0));
        assert_eq!(
            node.peers.get(&NodeId(2)).unwrap().last_fetch_offset,
            LogIndex(0)
        );
        assert_eq!(
            node.peers.get(&NodeId(3)).unwrap().last_fetch_offset,
            LogIndex(0)
        );

        // ---------- (a) ClientPropose: append, no commit, no apply ----------
        let propose = node.step(Input::ClientPropose(bytes::Bytes::from_static(b"set k v")));

        // The proposed entry is appended locally at index 2 with the
        // leader's current term and a Command payload.
        let appended = propose
            .iter()
            .find_map(|a| match a {
                Action::AppendEntries(es) => Some(es.clone()),
                _ => None,
            })
            .expect("AppendEntries emitted on ClientPropose");
        assert_eq!(appended.len(), 1, "exactly one entry per ClientPropose");
        assert_eq!(appended[0].index, LogIndex(2));
        assert_eq!(appended[0].term, Term(1));
        assert!(matches!(appended[0].payload, EntryPayload::Command(_)));

        // Leader's local tail reflects the append, but commit/apply do
        // NOT move: peer 2 and peer 3 are still at offset 0, so the
        // sorted offsets are [leader=2, peer2=0, peer3=0] and the q-th
        // value (q=2) is offsets[1]=0 — short of even the no-op at 1.
        assert_eq!(node.last_log_index, LogIndex(2));
        assert_eq!(node.last_log_term, Term(1));
        assert_eq!(
            node.commit_index,
            LogIndex(0),
            "multi-voter ClientPropose must NOT auto-commit",
        );
        assert_eq!(
            node.last_applied,
            LogIndex(0),
            "multi-voter ClientPropose must NOT apply locally",
        );
        assert!(
            !propose
                .iter()
                .any(|a| matches!(a, Action::ApplyToStateMachine { .. })),
            "no ApplyToStateMachine until a quorum acks the new index, got {propose:?}",
        );

        // ---------- (b1) Partial ack at offset=1 commits only the no-op ----------
        // Sorted offsets: [leader=2, peer2=1, peer3=0] -> q-th = offsets[1] = 1.
        // commit_index advances to 1 (no-op), demonstrating that the
        // command at index 2 specifically requires a quorum at
        // offset>=2 — a partial ack short of the new index does NOT
        // commit it. Only ApplyToStateMachine{1,1} for the no-op fires.
        let ack_noop = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(2),
            confirmed_offset: LogIndex(1),
        });
        assert_eq!(node.commit_index, LogIndex(1));
        assert_eq!(node.last_applied, LogIndex(1));
        assert!(
            ack_noop.iter().any(|a| matches!(
                a,
                Action::ApplyToStateMachine { from, to }
                    if *from == LogIndex(1) && *to == LogIndex(1)
            )),
            "expected ApplyToStateMachine{{1,1}} for the no-op, got {ack_noop:?}",
        );

        // ---------- (b2) 2nd voter acks the new index -> command commits ----------
        // Sorted offsets: [leader=2, peer2=2, peer3=0] -> q-th = offsets[1] = 2.
        // The command at index 2 is now replicated on 2-of-3 voters
        // (leader + node 2). Peer 3 is still at offset 0 — explicitly
        // NOT required for quorum. commit_index advances to 2 and
        // ApplyToStateMachine{2,2} is emitted.
        let ack_cmd = node.step(Input::FetchRequestAcked {
            replica_id: NodeId(2),
            confirmed_offset: LogIndex(2),
        });
        assert_eq!(node.commit_index, LogIndex(2));
        assert_eq!(node.last_applied, LogIndex(2));
        assert_eq!(
            node.peers.get(&NodeId(3)).unwrap().last_fetch_offset,
            LogIndex(0),
            "third voter is NOT required for quorum",
        );
        let apply = ack_cmd
            .iter()
            .find(|a| matches!(a, Action::ApplyToStateMachine { .. }))
            .expect("ApplyToStateMachine emitted after the 2nd voter acks the new index");
        match apply {
            Action::ApplyToStateMachine { from, to } => {
                assert_eq!(*from, LogIndex(2));
                assert_eq!(*to, LogIndex(2));
            }
            other => panic!("expected ApplyToStateMachine, got {other:?}"),
        }
    }

    /// ClientPropose on a non-leader is silently dropped (returns no
    /// actions). The transport-layer `NotLeader` error reply belongs to
    /// the higher-level RPC layer.
    #[test]
    fn scenario_client_propose_non_leader_dropped() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 43).unwrap();
        // Default role is Follower; no peers, no leader.
        assert_eq!(node.role, NodeRole::Follower);
        let actions = node.step(Input::ClientPropose(bytes::Bytes::from_static(b"noop")));
        assert!(
            actions.is_empty(),
            "non-leader ClientPropose must be dropped, got {actions:?}"
        );
        // No log mutation.
        assert_eq!(node.last_log_index, LogIndex(0));
    }

    /// Tick-driven fetch scheduling for a Follower with a known leader.
    #[test]
    fn scenario_tick_schedules_follower_fetch() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 47).unwrap();
        node.role = NodeRole::Follower;
        node.leader_id = Some(NodeId(2));
        node.hard_state.current_term = Term(2);

        // First Tick should schedule a FetchRequest because last_fetch_tick
        // is None (eager-fetch on first opportunity).
        let actions = node.step(Input::Tick);
        let fetch = actions
            .iter()
            .find_map(|a| match a {
                Action::SendMessage {
                    to,
                    message: OutboundMessage::FetchRequest(r),
                } => Some((*to, r.clone())),
                _ => None,
            })
            .expect("first Tick must schedule a FetchRequest");
        assert_eq!(fetch.0, NodeId(2));
        assert_eq!(fetch.1.fetch_offset, LogIndex(1));
        assert_eq!(fetch.1.last_fetched_epoch, Term(0));
        assert_eq!(fetch.1.leader_epoch, 2);
        assert_eq!(fetch.1.replica_id, NodeId(1));

        // last_fetch_tick recorded; immediate next Tick should NOT schedule.
        let actions2 = node.step(Input::Tick);
        let any_fetch = actions2.iter().any(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::FetchRequest(_),
                    ..
                }
            )
        });
        assert!(
            !any_fetch,
            "back-to-back Ticks must not double-schedule a fetch"
        );
    }

    // ---- Stage 5.2 — Snapshot Coordination handlers ---------------------

    /// Build a representative `SnapshotMeta` for the snapshot-coordination
    /// tests. The id is left empty here because the engine treats it as
    /// opaque metadata; the driver / store are responsible for normalising
    /// it on save.
    fn test_snapshot_meta(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            id: format!("snapshot-{term:010}-{index:020}"),
            last_included_index: LogIndex(index),
            last_included_term: Term(term),
            voter_set: None,
            size_bytes: Some(42),
            checksum: None,
        }
    }

    #[test]
    fn handle_snapshot_complete_records_metadata_and_emits_prefix_truncate() {
        // Scenario seed: SnapshotComplete with metadata pointing at log
        // index 10 / term 3 → engine records the metadata and emits a
        // single `Action::TruncateLog(PrefixThroughInclusive { 10 })`.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 17).unwrap();
        assert!(node.last_snapshot_meta.is_none());

        let meta = test_snapshot_meta(10, 3);
        let actions = node.step(Input::SnapshotComplete {
            metadata: meta.clone(),
        });

        // Metadata recorded.
        assert_eq!(
            node.last_snapshot_meta.as_ref(),
            Some(&meta),
            "last_snapshot_meta must be recorded on SnapshotComplete",
        );

        // Exactly one Action::TruncateLog(PrefixThroughInclusive).
        assert_eq!(
            actions.len(),
            1,
            "SnapshotComplete must emit exactly one follow-on action, got {actions:?}",
        );
        match &actions[0] {
            Action::TruncateLog(LogTruncation::PrefixThroughInclusive {
                through_index_inclusive,
            }) => {
                assert_eq!(*through_index_inclusive, LogIndex(10));
            }
            other => panic!(
                "expected TruncateLog(PrefixThroughInclusive {{ through_index_inclusive: 10 }}), got {other:?}",
            ),
        }
    }

    #[test]
    fn handle_snapshot_installed_advances_apply_and_commit_and_records_metadata() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 31).unwrap();
        assert_eq!(node.last_applied, LogIndex(0));
        assert_eq!(node.commit_index, LogIndex(0));
        assert_eq!(node.last_log_index, LogIndex(0));
        assert!(node.last_snapshot_meta.is_none());

        let meta = test_snapshot_meta(25, 7);
        let actions = node.step(Input::SnapshotInstalled {
            metadata: meta.clone(),
        });

        assert!(
            actions.is_empty(),
            "SnapshotInstalled must NOT emit any follow-on actions (engine has no entries to truncate against)",
        );
        assert_eq!(
            node.last_applied,
            LogIndex(25),
            "last_applied must advance to the snapshot's last_included_index",
        );
        assert_eq!(
            node.commit_index,
            LogIndex(25),
            "commit_index must advance to the snapshot's last_included_index",
        );
        // Engine mirrors must move forward so subsequent FetchRequests
        // don't claim a position behind the snapshot.
        assert_eq!(node.last_log_index, LogIndex(25));
        assert_eq!(node.last_log_term, Term(7));
        assert_eq!(node.last_snapshot_meta.as_ref(), Some(&meta));
    }

    #[test]
    fn handle_snapshot_installed_is_idempotent_when_already_ahead() {
        // If last_applied / commit_index / last_log_index are already
        // ahead of the snapshot, installing the snapshot must NOT
        // regress them — it is a no-op for the indices but still
        // records metadata.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 41).unwrap();
        node.last_applied = LogIndex(30);
        node.commit_index = LogIndex(30);
        node.last_log_index = LogIndex(40);
        node.last_log_term = Term(9);

        let meta = test_snapshot_meta(25, 7);
        let _ = node.step(Input::SnapshotInstalled {
            metadata: meta.clone(),
        });

        assert_eq!(node.last_applied, LogIndex(30));
        assert_eq!(node.commit_index, LogIndex(30));
        assert_eq!(node.last_log_index, LogIndex(40));
        assert_eq!(node.last_log_term, Term(9));
        assert_eq!(node.last_snapshot_meta.as_ref(), Some(&meta));
    }

    #[test]
    fn handle_snapshot_installed_preserves_fresher_last_snapshot_meta() {
        // Defensive belt-and-braces (Stage 5.2): a stale
        // `Input::SnapshotInstalled` delivered to the engine (e.g. via
        // a direct unit-test step, or any future alternate driver that
        // forgets the driver-side stale-install guard) must NOT clobber
        // a fresher `last_snapshot_meta`. The engine treats the
        // snapshot anchor as raise-only on `last_included_index`,
        // matching the existing raise-only semantics on `last_applied`
        // / `commit_index` / `last_log_*`.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 53).unwrap();
        let fresh = test_snapshot_meta(50, 11);
        node.last_applied = LogIndex(50);
        node.commit_index = LogIndex(50);
        node.last_log_index = LogIndex(50);
        node.last_log_term = Term(11);
        node.last_snapshot_meta = Some(fresh.clone());

        let stale = test_snapshot_meta(25, 7);
        let actions = node.step(Input::SnapshotInstalled {
            metadata: stale.clone(),
        });

        assert!(
            actions.is_empty(),
            "stale SnapshotInstalled must not emit follow-on actions",
        );
        // Indices are unchanged (raise-only guards).
        assert_eq!(node.last_applied, LogIndex(50));
        assert_eq!(node.commit_index, LogIndex(50));
        assert_eq!(node.last_log_index, LogIndex(50));
        assert_eq!(node.last_log_term, Term(11));
        // The fresher snapshot anchor must survive.
        assert_eq!(
            node.last_snapshot_meta.as_ref(),
            Some(&fresh),
            "stale Input::SnapshotInstalled must not clobber a fresher last_snapshot_meta",
        );
    }

    #[test]
    fn handle_snapshot_complete_preserves_fresher_last_snapshot_meta() {
        // Defensive belt-and-braces (Stage 5.2): a same- or lower-indexed
        // `Input::SnapshotComplete` (e.g. an out-of-order completion
        // delivered after a newer snapshot has been recorded via either
        // `SnapshotComplete` or `SnapshotInstalled`) must NOT clobber the
        // fresher anchor and must NOT emit a follow-on `TruncateLog`. The
        // engine already anchors at a longer prefix, so instructing the
        // driver to purge through the stale, lower index would express
        // the wrong intent (prefix purge is idempotent today, but Stage
        // 6.2's physical purge would treat the stale instruction as a
        // genuine — and confusingly named — request). The debouncer
        // flag still clears because the driver-side save attempt has
        // resolved either way.
        let mut node = RaftNode::new_with_seed(three_voter_config(), 67).unwrap();
        let fresh = test_snapshot_meta(50, 11);
        node.last_snapshot_meta = Some(fresh.clone());
        node.snapshot_in_flight = true;

        let stale = test_snapshot_meta(25, 7);
        let actions = node.step(Input::SnapshotComplete {
            metadata: stale.clone(),
        });

        // Stale completion emits no follow-on actions — the fresher
        // anchor already covers (or will cover) a longer prefix.
        assert!(
            actions.is_empty(),
            "stale Input::SnapshotComplete must not emit any follow-on actions, got {actions:?}",
        );
        // The fresher snapshot anchor must survive.
        assert_eq!(
            node.last_snapshot_meta.as_ref(),
            Some(&fresh),
            "stale Input::SnapshotComplete must not clobber a fresher last_snapshot_meta",
        );
        // Debouncer must still clear so the next threshold crossing can
        // re-emit a TakeSnapshot.
        assert!(
            !node.snapshot_in_flight,
            "snapshot_in_flight must clear even when the completion was stale",
        );
    }

    // ---- Stage 5.3 — `Input::FetchSnapshotReceived` action contract -----

    /// Stage 5.3 implementation-plan §5.2 step 3: the driver-side
    /// `OutboundResult::FetchSnapshot` handler feeds the reassembled
    /// `(metadata, data)` into the engine as
    /// `Input::FetchSnapshotReceived`. When the snapshot strictly
    /// advances `last_applied`, the engine MUST emit exactly one
    /// `Action::InstallSnapshot { metadata, data }` so the driver
    /// fulfils it via the same arm synthetic / test-injected actions
    /// flow through.
    #[test]
    fn handle_fetch_snapshot_received_emits_install_snapshot_when_fresh() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 91).unwrap();
        assert_eq!(node.last_applied, LogIndex(0));

        let meta = test_snapshot_meta(50, 7);
        let data = b"fresh-snapshot-bytes".to_vec();
        let actions = node.step(Input::FetchSnapshotReceived {
            metadata: meta.clone(),
            data: data.clone(),
        });

        assert_eq!(
            actions.len(),
            1,
            "fresh FetchSnapshotReceived must emit exactly one action, got {actions:?}",
        );
        match &actions[0] {
            Action::InstallSnapshot {
                metadata: emitted_meta,
                data: emitted_data,
            } => {
                assert_eq!(emitted_meta, &meta);
                assert_eq!(emitted_data, &data);
            }
            other => {
                panic!("expected Action::InstallSnapshot {{ metadata, data }}, got {other:?}",)
            }
        }

        // Engine indices must NOT advance until the driver completes
        // the install and feeds Input::SnapshotInstalled back — keeping
        // the "engine is I/O-free" contract: the snapshot is not yet
        // durable when we emit the action.
        assert_eq!(
            node.last_applied,
            LogIndex(0),
            "last_applied must not advance on the receive-side action emission",
        );
        assert_eq!(
            node.commit_index,
            LogIndex(0),
            "commit_index must not advance on the receive-side action emission",
        );
    }

    /// Stale snapshots (coverage at or behind `last_applied`) must not
    /// produce an `Action::InstallSnapshot`. Restoring an older state-
    /// machine view would diverge the state machine (older) from the
    /// engine (newer applied position), since `handle_snapshot_installed`
    /// refuses to lower `last_applied`.
    #[test]
    fn handle_fetch_snapshot_received_drops_stale_snapshot() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 92).unwrap();
        node.last_applied = LogIndex(100);
        node.commit_index = LogIndex(100);

        // Snapshot covers strictly less than last_applied — stale.
        let stale_below = test_snapshot_meta(50, 7);
        let actions = node.step(Input::FetchSnapshotReceived {
            metadata: stale_below.clone(),
            data: b"stale-below".to_vec(),
        });
        assert!(
            actions.is_empty(),
            "FetchSnapshotReceived with metadata.last_included_index < last_applied must emit no actions, got {actions:?}",
        );

        // Snapshot covers exactly last_applied — still stale (no advance).
        let stale_equal = test_snapshot_meta(100, 9);
        let actions = node.step(Input::FetchSnapshotReceived {
            metadata: stale_equal,
            data: b"stale-equal".to_vec(),
        });
        assert!(
            actions.is_empty(),
            "FetchSnapshotReceived with metadata.last_included_index == last_applied must emit no actions, got {actions:?}",
        );

        // Indices must not move on stale receive.
        assert_eq!(node.last_applied, LogIndex(100));
        assert_eq!(node.commit_index, LogIndex(100));
    }

    // ---- Stage 5.2 — auto snapshot trigger (`maybe_take_snapshot`) ------

    /// Single-voter config with a custom `max_log_entries_before_compaction`.
    /// Used to drive the snapshot-trigger threshold on a one-node cluster
    /// where a `ClientPropose` immediately satisfies quorum.
    fn single_voter_config_with_snapshot_threshold(threshold: u64) -> ClusterConfig {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200
max_log_entries_before_compaction = {threshold}

[[voters]]
node_id = 1
directory_id = "{uuid}"
host = "node1"
port = 6000
"#,
            threshold = threshold,
            uuid = Uuid::new_v4(),
        );
        ClusterConfig::from_toml_str(&toml).unwrap()
    }

    /// Stage 5.2 implementation-plan §5.2 step 1 / scenario
    /// `auto-snapshot-trigger`: with `max_log_entries_before_compaction = 10`,
    /// proposing 12 commands on a single-voter cluster (each immediately
    /// satisfies quorum and advances `commit_index`) must emit exactly one
    /// `Action::TakeSnapshot` once the threshold is crossed.
    #[test]
    fn auto_snapshot_trigger_emits_take_snapshot_when_threshold_crossed() {
        let cfg = single_voter_config_with_snapshot_threshold(10);
        let mut node = RaftNode::new_with_seed(cfg, 71).unwrap();

        // Become leader; this appends a no-op (index 1) and (single voter)
        // commits + applies it. snapshot_in_flight stays false, last_applied=1.
        node.become_pre_candidate();
        node.become_candidate();
        let leader_actions = node.become_leader();
        // No snapshot trigger yet — last_applied=1, snap_idx=0, lag=1<=10.
        assert!(
            !leader_actions
                .iter()
                .any(|a| matches!(a, Action::TakeSnapshot { .. })),
            "no TakeSnapshot expected before threshold crossed; got {leader_actions:?}",
        );
        assert!(!node.snapshot_in_flight);

        // Propose entries 2..=11 (10 more commands). After each, a
        // single-voter commit advances commit_index. Threshold is
        // commit_index - snap_idx > 10. snap_idx = 0 because no snapshot
        // has completed yet. So once commit_index reaches 11 the
        // condition becomes 11 - 0 = 11 > 10 → emit TakeSnapshot.
        let mut take_snapshot_actions: Vec<Action> = Vec::new();
        for i in 2..=12 {
            let actions = node.step(Input::ClientPropose(bytes::Bytes::from(format!("cmd-{i}"))));
            for a in &actions {
                if matches!(a, Action::TakeSnapshot { .. }) {
                    take_snapshot_actions.push(a.clone());
                }
            }
        }

        // Exactly one TakeSnapshot must have been emitted across the
        // 11 proposals (debouncing keeps the next 10 from re-emitting).
        assert_eq!(
            take_snapshot_actions.len(),
            1,
            "exactly one Action::TakeSnapshot must be emitted across the threshold-crossing proposals; got {take_snapshot_actions:?}",
        );
        match &take_snapshot_actions[0] {
            Action::TakeSnapshot { through_index } => {
                assert!(
                    through_index.0 >= 11,
                    "through_index must be at or past the threshold-crossing commit (>=11), got {through_index}",
                );
            }
            other => panic!("expected TakeSnapshot, got {other:?}"),
        }

        // The in-flight flag is set; no further TakeSnapshot can be
        // emitted until SnapshotComplete clears it.
        assert!(
            node.snapshot_in_flight,
            "snapshot_in_flight must be set after the trigger fires",
        );
        let extra = node.step(Input::ClientPropose(bytes::Bytes::from_static(b"another")));
        assert!(
            !extra
                .iter()
                .any(|a| matches!(a, Action::TakeSnapshot { .. })),
            "no second TakeSnapshot must be emitted while snapshot_in_flight is true",
        );

        // Feed back SnapshotComplete; the flag clears and the next
        // commit advance can re-emit TakeSnapshot once the lag
        // re-crosses the threshold.
        let through = node.commit_index;
        let _ = node.step(Input::SnapshotComplete {
            metadata: SnapshotMeta {
                id: String::new(),
                last_included_index: through,
                last_included_term: node.last_log_term,
                voter_set: node.voter_set.clone(),
                size_bytes: Some(0),
                checksum: None,
            },
        });
        assert!(
            !node.snapshot_in_flight,
            "snapshot_in_flight must clear on SnapshotComplete",
        );

        // After SnapshotComplete, snap_idx == commit_index, so the lag
        // resets to 0. The next 11 proposals must re-trigger exactly
        // one more TakeSnapshot.
        let mut more: Vec<Action> = Vec::new();
        for i in 0..12 {
            let acts = node.step(Input::ClientPropose(bytes::Bytes::from(format!(
                "post-{i}"
            ))));
            for a in acts {
                if matches!(a, Action::TakeSnapshot { .. }) {
                    more.push(a);
                }
            }
        }
        assert_eq!(
            more.len(),
            1,
            "after SnapshotComplete, the next threshold crossing must re-trigger exactly one TakeSnapshot; got {more:?}",
        );
    }

    /// `Input::SnapshotInstalled` (the leader-supplied path) must also
    /// clear `snapshot_in_flight` so a subsequent local threshold
    /// crossing can re-emit `Action::TakeSnapshot`.
    #[test]
    fn snapshot_installed_clears_in_flight_flag() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 73).unwrap();
        // Force the flag set as if a TakeSnapshot was emitted.
        node.snapshot_in_flight = true;
        let _ = node.step(Input::SnapshotInstalled {
            metadata: test_snapshot_meta(20, 4),
        });
        assert!(
            !node.snapshot_in_flight,
            "snapshot_in_flight must clear on SnapshotInstalled too (leader-supplied snapshot supersedes any in-flight local snapshot)",
        );
    }

    // -----------------------------------------------------------------
    // Stage 5.2 (impl-plan §5.2 step 4) — follower-side snapshot
    // redirect handling
    // -----------------------------------------------------------------
    //
    // When a `FetchResponse` carries a `SnapshotRedirect`, the follower
    // must:
    //   1. NOT process entries / divergence (mutual exclusivity).
    //   2. Emit a `FetchSnapshotRequest` to the leader carrying the
    //      canonical snapshot id, offset 0, and max_bytes 0.
    //   3. Stamp `last_fetch_tick` so a duplicate redirect storm is
    //      damped while the install is in flight.

    fn fetch_response_with_redirect(
        leader: NodeId,
        leader_epoch: u64,
        snapshot_id: &str,
        last_included_index: u64,
        last_included_term: u64,
    ) -> FetchResponse {
        FetchResponse {
            cluster_id: "test".into(),
            leader_epoch,
            leader_id: leader,
            high_watermark: LogIndex(last_included_index),
            entries: Vec::new(),
            diverging_epoch: None,
            snapshot_redirect: Some(crate::message::SnapshotRedirect {
                snapshot_id: snapshot_id.into(),
                last_included_index: LogIndex(last_included_index),
                last_included_term: Term(last_included_term),
            }),
            is_leader: true,
        }
    }

    #[test]
    fn handle_fetch_response_with_redirect_emits_fetch_snapshot_request() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 991).unwrap();
        // Anchor the follower's view: we know NodeId(2) is the
        // current-term leader.
        node.hard_state.current_term = Term(7);
        node.leader_id = Some(NodeId(2));
        node.role = NodeRole::Follower;

        let resp = fetch_response_with_redirect(NodeId(2), 7, "snap-follower-redirect-1", 42, 6);
        let actions = node.handle_fetch_response(resp);

        // Exactly one outbound FetchSnapshotRequest, addressed to the
        // leader, with the redirect's snapshot_id.
        assert_eq!(
            actions.len(),
            1,
            "redirect must produce exactly one follow-on action, got {actions:?}",
        );
        match &actions[0] {
            Action::SendMessage { to, message } => {
                assert_eq!(
                    *to,
                    NodeId(2),
                    "FetchSnapshotRequest must target the leader"
                );
                match message {
                    OutboundMessage::FetchSnapshotRequest(req) => {
                        assert_eq!(req.snapshot_id, "snap-follower-redirect-1");
                        assert_eq!(req.cluster_id, "test");
                        assert_eq!(req.leader_epoch, 7);
                        assert_eq!(req.replica_id, node.id);
                        assert_eq!(req.offset, 0);
                        assert_eq!(req.max_bytes, 0);
                    }
                    other => {
                        panic!("expected FetchSnapshotRequest, got {other:?}")
                    }
                }
            }
            other => panic!("expected SendMessage(FetchSnapshotRequest), got {other:?}"),
        }
        // Election timer reset is an integral part of the leader-contact
        // pre-fence; assert the redirect path also stamps last_fetch_tick
        // so duplicate redirects don't storm the leader while the
        // install is in flight.
        assert!(
            node.last_fetch_tick.is_some(),
            "redirect path must stamp last_fetch_tick (debounce)",
        );
    }

    /// Mutual exclusivity (FetchResponse contract): when redirect is
    /// present, `entries` and `diverging_epoch` are ignored. This guards
    /// against a misbehaving leader that smuggles entries or a
    /// divergence signal alongside the redirect — the follower must
    /// only honour the redirect.
    #[test]
    fn handle_fetch_response_redirect_takes_precedence_over_divergence_or_entries() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 1313).unwrap();
        node.hard_state.current_term = Term(4);
        node.leader_id = Some(NodeId(2));
        node.role = NodeRole::Follower;
        // Snapshot the engine's last-log mirror BEFORE handling the
        // (would-be) entries so we can prove they were not applied.
        let baseline_last_index = node.last_log_index;
        let baseline_last_term = node.last_log_term;

        let resp = FetchResponse {
            cluster_id: "test".into(),
            leader_epoch: 4,
            leader_id: NodeId(2),
            high_watermark: LogIndex(50),
            // Smuggle entries — must be ignored.
            entries: vec![Entry {
                index: LogIndex(99),
                term: Term(4),
                payload: EntryPayload::NoOp,
            }],
            // Smuggle a divergence signal — must be ignored.
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(3),
                end_offset: LogIndex(7),
            }),
            snapshot_redirect: Some(crate::message::SnapshotRedirect {
                snapshot_id: "snap-takes-precedence".into(),
                last_included_index: LogIndex(50),
                last_included_term: Term(4),
            }),
            is_leader: true,
        };

        let actions = node.handle_fetch_response(resp);

        // Redirect produced exactly one action — no AppendEntries / no
        // truncation from the divergence path.
        assert_eq!(
            actions.len(),
            1,
            "exactly one action (the FetchSnapshotRequest) must be emitted; got {actions:?}",
        );
        assert!(matches!(
            &actions[0],
            Action::SendMessage {
                message: OutboundMessage::FetchSnapshotRequest(_),
                ..
            }
        ));
        // Engine state must be unchanged — no entries appended, no
        // divergence-driven truncation / fetch-pointer reset.
        assert_eq!(
            node.last_log_index, baseline_last_index,
            "redirect must not advance last_log_index via the smuggled entries",
        );
        assert_eq!(
            node.last_log_term, baseline_last_term,
            "redirect must not advance last_log_term via the smuggled entries",
        );
    }

    // -----------------------------------------------------------------
    // Stage 5.3 (impl-plan §5.2 step 4) — leader-side snapshot redirect
    // emitted by the engine via `Action::RedirectToSnapshot`.
    //
    // The leader's `handle_fetch_request` must detect when a follower's
    // `fetch_offset` falls at or below the compacted prefix anchored
    // by `last_snapshot_meta` and emit `Action::RedirectToSnapshot`
    // INSTEAD of `Action::ServeFetch` — keeping the redirect decision
    // I/O-free inside the engine.
    // -----------------------------------------------------------------

    /// Helper: drive `node` (a fresh three-voter config) into the
    /// Leader role at term 1 and install `meta` as the leader's
    /// `last_snapshot_meta` so the redirect predicate can fire.
    fn drive_leader_with_snapshot_anchor(node: &mut RaftNode, meta: SnapshotMeta) {
        drive_three_voter_to_leader(node);
        assert_eq!(node.role, NodeRole::Leader);
        node.last_snapshot_meta = Some(meta);
    }

    /// Scenario: install-snapshot-on-slow-follower (engine half).
    ///
    /// Given a leader anchored at a snapshot covering `[..=50]`,
    /// when a follower with `fetch_offset = 10` sends a `FetchRequest`,
    /// then `handle_fetch_request` MUST emit exactly one
    /// `Action::RedirectToSnapshot` carrying the snapshot's canonical
    /// id, indices, and the leader's envelope. No `Action::ServeFetch`
    /// must be emitted.
    #[test]
    fn handle_fetch_request_emits_redirect_when_offset_in_compacted_prefix() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5301).unwrap();
        // Snapshot covering log entries 1..=50 at term 3.
        let snap = SnapshotMeta {
            id: "snap-stage-5.3-redirect".into(),
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            voter_set: node.voter_set.clone(),
            size_bytes: Some(1_048_576),
            checksum: None,
        };
        drive_leader_with_snapshot_anchor(&mut node, snap.clone());

        let leader_term = node.current_term().0;
        let req = build_fetch_request_from(NodeId(2), 10, 0, leader_term);
        let actions = node.step(Input::FetchRequest(req));

        assert_eq!(
            actions.len(),
            1,
            "exactly one action expected on redirect; got {actions:?}",
        );
        match &actions[0] {
            Action::RedirectToSnapshot {
                to,
                cluster_id,
                leader_epoch,
                leader_id,
                high_watermark,
                snapshot_metadata,
            } => {
                assert_eq!(*to, NodeId(2), "redirect must target the asking follower");
                assert_eq!(cluster_id, "test");
                assert_eq!(*leader_epoch, leader_term);
                assert_eq!(*leader_id, node.id);
                assert_eq!(*high_watermark, node.commit_index);
                assert_eq!(snapshot_metadata.id, snap.id);
                assert_eq!(
                    snapshot_metadata.last_included_index,
                    snap.last_included_index
                );
                assert_eq!(
                    snapshot_metadata.last_included_term,
                    snap.last_included_term
                );
            }
            other => panic!("expected Action::RedirectToSnapshot, got {other:?}"),
        }
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::ServeFetch { .. })),
            "ServeFetch must NOT be emitted alongside a redirect (mutual exclusivity)",
        );
    }

    /// Boundary case: `fetch_offset == last_included_index` is still
    /// inside the compacted prefix (the follower wants the snapshot's
    /// tail entry which has been compacted), so the redirect must fire.
    #[test]
    fn handle_fetch_request_redirect_with_offset_equal_last_included_index() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5302).unwrap();
        let snap = SnapshotMeta {
            id: "snap-boundary".into(),
            last_included_index: LogIndex(50),
            last_included_term: Term(4),
            voter_set: node.voter_set.clone(),
            size_bytes: Some(64),
            checksum: None,
        };
        drive_leader_with_snapshot_anchor(&mut node, snap);
        let leader_term = node.current_term().0;

        let req = build_fetch_request_from(NodeId(2), 50, 4, leader_term);
        let actions = node.step(Input::FetchRequest(req));

        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], Action::RedirectToSnapshot { .. }),
            "fetch_offset == last_included_index must redirect (the snapshot's tail entry has been compacted); got {:?}",
            actions[0],
        );
    }

    /// Boundary case: `fetch_offset == last_included_index + 1` lies
    /// just past the compacted prefix, so the engine must NOT redirect.
    /// The follower is asking for the next entry beyond the snapshot's
    /// tail — the standard resumption case, handled by `ServeFetch`.
    #[test]
    fn handle_fetch_request_does_not_redirect_when_offset_past_snapshot() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5303).unwrap();
        let snap = SnapshotMeta {
            id: "snap-resume".into(),
            last_included_index: LogIndex(50),
            last_included_term: Term(4),
            voter_set: node.voter_set.clone(),
            size_bytes: Some(64),
            checksum: None,
        };
        drive_leader_with_snapshot_anchor(&mut node, snap);
        let leader_term = node.current_term().0;

        // Lift the engine's last_log_index past the snapshot so the
        // ServeFetch path is realistic (the leader actually has entry
        // 51 to potentially serve).
        node.last_log_index = LogIndex(60);
        node.last_log_term = Term(4);

        let req = build_fetch_request_from(NodeId(2), 51, 4, leader_term);
        let actions = node.step(Input::FetchRequest(req));

        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], Action::ServeFetch { .. }),
            "fetch_offset > last_included_index must use ServeFetch, not redirect; got {:?}",
            actions[0],
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::RedirectToSnapshot { .. })),
            "RedirectToSnapshot must NOT be emitted past the snapshot tail",
        );
    }

    /// Even at `fetch_offset == last_included_index + 1` with a WRONG
    /// `last_fetched_epoch`, the engine still emits `ServeFetch` —
    /// divergence detection lives in the driver (it consults the log
    /// store + snapshot anchor to surface the divergence signal). The
    /// engine MUST NOT short-circuit to a redirect here because the
    /// follower's fetch tip is past the compacted prefix. This regression
    /// test guards against accidentally widening the redirect predicate
    /// to consume divergence cases that belong on the `ServeFetch` path.
    #[test]
    fn handle_fetch_request_serves_fetch_when_offset_after_snapshot_tail_even_with_wrong_term() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5304).unwrap();
        let snap = SnapshotMeta {
            id: "snap-wrongterm".into(),
            last_included_index: LogIndex(50),
            last_included_term: Term(4),
            voter_set: node.voter_set.clone(),
            size_bytes: Some(64),
            checksum: None,
        };
        drive_leader_with_snapshot_anchor(&mut node, snap);
        let leader_term = node.current_term().0;
        node.last_log_index = LogIndex(60);
        node.last_log_term = Term(4);

        // last_fetched_epoch = 99 — does not match the snapshot's
        // last_included_term=4. The engine still emits ServeFetch (the
        // driver will surface the divergence via DivergingEpoch).
        let req = build_fetch_request_from(NodeId(2), 51, 99, leader_term);
        let actions = node.step(Input::FetchRequest(req));

        assert!(
            matches!(&actions[0], Action::ServeFetch { .. }),
            "ServeFetch must be emitted so the driver can surface divergence; got {:?}",
            actions[0],
        );
    }

    /// When the snapshot's canonical id is empty (e.g. legacy on-disk
    /// metadata, or a test driver that fed `SnapshotComplete` with
    /// `id = String::new()` directly), the engine MUST NOT emit a
    /// redirect — the follower cannot meaningfully echo an empty
    /// `snapshot_id` on `FetchSnapshotRequest`. Fall through to
    /// `ServeFetch` so the driver can at least surface a divergence
    /// signal. A `warn!` records the misconfiguration; the test only
    /// asserts the action emission.
    #[test]
    fn handle_fetch_request_fallthrough_when_snapshot_id_empty() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5305).unwrap();
        let snap = SnapshotMeta {
            id: String::new(),
            last_included_index: LogIndex(50),
            last_included_term: Term(4),
            voter_set: node.voter_set.clone(),
            size_bytes: Some(64),
            checksum: None,
        };
        drive_leader_with_snapshot_anchor(&mut node, snap);
        let leader_term = node.current_term().0;

        let req = build_fetch_request_from(NodeId(2), 10, 0, leader_term);
        let actions = node.step(Input::FetchRequest(req));

        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], Action::ServeFetch { .. }),
            "empty snapshot_id must fall through to ServeFetch (no usable redirect target); got {:?}",
            actions[0],
        );
    }

    /// Sanity: when no snapshot has ever been taken (`last_snapshot_meta
    /// is None`), the engine always emits `ServeFetch` and never a
    /// redirect, regardless of how small `fetch_offset` is.
    #[test]
    fn handle_fetch_request_serves_fetch_when_no_snapshot_anchor() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5306).unwrap();
        drive_three_voter_to_leader(&mut node);
        assert!(node.last_snapshot_meta.is_none());

        let leader_term = node.current_term().0;
        let req = build_fetch_request_from(NodeId(2), 1, 0, leader_term);
        let actions = node.step(Input::FetchRequest(req));

        assert!(
            matches!(&actions[0], Action::ServeFetch { .. }),
            "no snapshot anchor must always use ServeFetch; got {:?}",
            actions[0],
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::RedirectToSnapshot { .. })),
        );
    }

    /// Liveness: the redirect path still updates `peer.last_fetch_time`
    /// and `last_leader_contact_tick` — a redirect-bound Fetch is
    /// evidence the follower is alive, which Check-Quorum (Stage 6.1)
    /// will rely on. Per-peer replication progress
    /// (`peer.last_fetch_offset`) must NOT advance (that requires the
    /// driver's `FetchRequestAcked` after divergence validation, which
    /// is not fed on a redirect — the redirect proves the follower is
    /// BEHIND the compacted prefix).
    #[test]
    fn handle_fetch_request_redirect_updates_liveness_not_progress() {
        let mut node = RaftNode::new_with_seed(three_voter_config(), 5307).unwrap();
        let snap = SnapshotMeta {
            id: "snap-live".into(),
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            voter_set: node.voter_set.clone(),
            size_bytes: Some(64),
            checksum: None,
        };
        drive_leader_with_snapshot_anchor(&mut node, snap);
        let leader_term = node.current_term().0;

        // Drive a few ticks so logical_tick is non-zero — easier to
        // distinguish a stamped liveness field from the zero default.
        for _ in 0..3 {
            node.step(Input::Tick);
        }
        let tick_before = node.logical_tick;
        // Sanity: peer 2's last_fetch_offset starts at zero.
        let pre_offset = node
            .peers
            .get(&NodeId(2))
            .map(|p| p.last_fetch_offset)
            .unwrap_or(LogIndex(0));
        assert_eq!(pre_offset, LogIndex(0));

        let req = build_fetch_request_from(NodeId(2), 5, 0, leader_term);
        let _ = node.step(Input::FetchRequest(req));

        // Liveness fields stamped at the request-handling tick.
        let peer = node.peers.get(&NodeId(2)).expect("peer 2 must be tracked");
        assert_eq!(
            peer.last_fetch_time, tick_before,
            "peer.last_fetch_time must be stamped on a redirect-bound Fetch"
        );
        assert_eq!(
            node.last_leader_contact_tick,
            Some(tick_before),
            "self-contact must be stamped on a redirect-bound Fetch (Check-Quorum input)"
        );
        // Replication progress unchanged — the redirect is NOT proof
        // the follower has any entry.
        assert_eq!(
            peer.last_fetch_offset, pre_offset,
            "peer.last_fetch_offset must NOT advance on a redirect (the follower is BEHIND the compacted prefix)",
        );
    }

    // ===================================================================
    // Stage 7.1 — Check Quorum and Leader Lease
    // ===================================================================

    /// Three-voter config with Check-Quorum ON (the default). This node
    /// = 1; peers = 2 and 3. Election timeout 100-200ms, tick 10ms.
    /// `check_quorum_interval_ms` defaults to `2 * 200 = 400`, i.e. 40
    /// ticks.
    fn three_voter_check_quorum_config() -> ClusterConfig {
        let mut cfg = three_voter_config();
        cfg.enable_check_quorum = true;
        cfg.enable_leader_lease = true;
        cfg
    }

    /// Drive the supplied node directly to Leader with a deterministic
    /// becomes-leader cascade (Pre-Vote → Candidate → Leader). Tests
    /// that want a "clean" leader for Check-Quorum / Leader-Lease
    /// scenarios use this helper to skip election plumbing.
    fn force_leader(node: &mut RaftNode) {
        let _ = node.become_pre_candidate();
        let _ = node.become_candidate();
        // Manually grant the second vote so we cross the threshold;
        // in a 3-voter cluster the self-vote + one peer vote is a
        // quorum.
        node.votes_received.insert(NodeId(2));
        let _ = node.become_leader();
        assert_eq!(node.role, NodeRole::Leader, "force_leader did not promote");
    }

    /// Build a FetchRequest at this leader's current term, from
    /// `peer_id`, asking for entries starting at the first
    /// post-snapshot index (1 — `fetch_offset = 0` is rejected by
    /// the engine as malformed, per the architecture's 1-based
    /// indexing). `epoch_override` lets callers spoof a higher
    /// `leader_epoch` for term-bump tests.
    fn fetch_at(node: &RaftNode, peer_id: NodeId, epoch_override: Option<u64>) -> FetchRequest {
        let leader_epoch = epoch_override.unwrap_or(node.hard_state.current_term.0);
        FetchRequest {
            cluster_id: node.config.cluster_id.clone(),
            leader_epoch,
            replica_id: peer_id,
            // The empty-log case in architecture §5.2 is encoded as
            // `fetch_offset = 1, last_fetched_epoch = 0`. We use that
            // so the engine accepts the request and updates
            // `peer.last_fetch_time` (the data we actually care about
            // for Check-Quorum / Leader-Lease tests).
            fetch_offset: LogIndex(1),
            last_fetched_epoch: Term(0),
        }
    }

    /// Run `n` Tick steps against `node`, returning the union of all
    /// actions emitted (useful for asserting that StepDown happened).
    fn run_ticks(node: &mut RaftNode, n: u64) -> Vec<Action> {
        let mut all = Vec::new();
        for _ in 0..n {
            all.extend(node.step(Input::Tick));
        }
        all
    }

    #[test]
    fn check_quorum_interval_ticks_is_derived_from_2x_election_timeout_max() {
        // Default config: election_timeout_max = 200ms, tick = 10ms.
        // effective_check_quorum_interval = 2 * 200 = 400ms = 40 ticks.
        let node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 1).unwrap();
        assert_eq!(node.check_quorum_interval_ticks, 40);
    }

    #[test]
    fn check_quorum_interval_ticks_honours_explicit_override() {
        let mut cfg = three_voter_check_quorum_config();
        cfg.check_quorum_interval_ms = Some(123); // 12.3 ticks → ceil to 13
        let node = RaftNode::new_with_seed(cfg, 1).unwrap();
        assert_eq!(node.check_quorum_interval_ticks, 13);
    }

    #[test]
    fn check_quorum_steps_down_when_partitioned() {
        // Scenario "check-quorum-steps-down": a 3-voter leader cut off
        // from both peers for >= `check_quorum_interval_ticks` must
        // step down. We never feed any FetchRequest so the peers'
        // `last_fetch_time` stays at the `become_leader` baseline; once
        // `logical_tick - last_fetch_time >= interval` (40 ticks here)
        // the leader counts only itself (1) which is < quorum (2 of 3)
        // and emits StepDown.
        let mut node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 7).unwrap();
        force_leader(&mut node);
        let interval = node.check_quorum_interval_ticks;
        // Run exactly `interval` ticks — the check fires on the
        // `interval`-th tick and produces a StepDown.
        let actions = run_ticks(&mut node, interval);
        assert_eq!(
            node.role,
            NodeRole::Follower,
            "leader did not step down after {interval} ticks without peer contact"
        );
        assert!(
            actions.iter().any(|a| matches!(a, Action::StepDown)),
            "actions did not contain a StepDown: {actions:?}"
        );
    }

    #[test]
    fn check_quorum_keeps_leader_when_healthy_3voter() {
        // Scenario "check-quorum-healthy": with one peer regularly
        // fetching, leader + 1 peer = 2 of 3 = quorum, no step-down.
        let mut node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 9).unwrap();
        force_leader(&mut node);
        let interval = node.check_quorum_interval_ticks;
        // Tick `interval - 1` times, then synthesise a fetch from
        // peer 2 (which stamps `peer.last_fetch_time = logical_tick`),
        // then tick `interval` more times. The peer's recency window
        // is reset on each fetch, so even at the `interval`-th tick
        // the leader counts self + peer2 = 2 = quorum.
        run_ticks(&mut node, interval - 1);
        // Synthesise a Fetch from peer 2 at the current term.
        let fetch_req = fetch_at(&node, NodeId(2), None);
        let _ = node.handle_fetch_request(fetch_req);
        // Now tick another full interval — the peer-2 stamp is
        // recent, so check-quorum still sees a majority.
        run_ticks(&mut node, interval);
        assert_eq!(
            node.role,
            NodeRole::Leader,
            "leader stepped down despite peer-2 fetching within the window"
        );
    }

    #[test]
    fn check_quorum_disabled_does_not_step_down() {
        // With `enable_check_quorum = false` the partitioned-leader
        // scenario above MUST NOT trigger any step-down — operators
        // disabling the flag opt out of leader self-monitoring.
        let mut cfg = three_voter_check_quorum_config();
        cfg.enable_check_quorum = false;
        let mut node = RaftNode::new_with_seed(cfg, 13).unwrap();
        force_leader(&mut node);
        // 10x the interval is plenty.
        let interval = node.check_quorum_interval_ticks;
        run_ticks(&mut node, interval.saturating_mul(10));
        assert_eq!(
            node.role,
            NodeRole::Leader,
            "leader stepped down despite enable_check_quorum=false"
        );
    }

    #[test]
    fn leader_lease_inactive_when_disabled() {
        let mut cfg = three_voter_check_quorum_config();
        cfg.enable_leader_lease = false;
        let mut node = RaftNode::new_with_seed(cfg, 17).unwrap();
        force_leader(&mut node);
        assert!(
            !node.has_active_lease(),
            "lease must be inactive when enable_leader_lease=false"
        );
    }

    #[test]
    fn leader_lease_inactive_before_first_post_election_fetch() {
        // Rubber-duck Blocker 1: `become_leader` pre-stamps
        // `peer.last_fetch_time = logical_tick` as the Check-Quorum
        // grace baseline. The lease check must NOT treat that as
        // evidence of follower contact, so a freshly-elected leader
        // with no real Fetch yet returns false.
        let mut node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 19).unwrap();
        force_leader(&mut node);
        assert!(
            !node.has_active_lease(),
            "lease must be inactive immediately after election with no post-election fetches"
        );
    }

    #[test]
    fn leader_lease_active_after_majority_recent_fetches() {
        // After a fetch from one peer (3-voter cluster: leader + 1 peer
        // = quorum) the lease is active.
        let mut node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 23).unwrap();
        force_leader(&mut node);
        // Advance the logical clock so the post-fetch stamp is
        // strictly greater than `leader_started_tick`.
        let _ = node.step(Input::Tick);
        let fetch_req = fetch_at(&node, NodeId(2), None);
        let _ = node.handle_fetch_request(fetch_req);
        assert!(
            node.has_active_lease(),
            "lease must be active after a post-election fetch from one peer (leader + peer2 = quorum)"
        );
    }

    #[test]
    fn leader_lease_inactive_after_step_down() {
        let mut node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 29).unwrap();
        force_leader(&mut node);
        let _ = node.step(Input::Tick);
        let fetch_req = fetch_at(&node, NodeId(2), None);
        let _ = node.handle_fetch_request(fetch_req);
        assert!(node.has_active_lease());
        // Step down. The lease must immediately become inactive.
        let current_term = node.hard_state.current_term;
        let _ = node.become_follower(current_term, None);
        assert!(
            !node.has_active_lease(),
            "lease must be inactive once we are no longer leader"
        );
    }

    #[test]
    fn leader_lease_active_for_single_voter_cluster() {
        // Single-voter cluster: leader counts itself = 1 = quorum
        // (1 of 1). Lease is always active while leading, even without
        // any peer fetches (there are no peers).
        let mut cfg = single_voter_config();
        cfg.enable_leader_lease = true;
        cfg.enable_check_quorum = true;
        let mut node = RaftNode::new_with_seed(cfg, 31).unwrap();
        // Single-voter cluster cascades to leader via pre_candidate.
        let _ = node.become_pre_candidate();
        assert_eq!(node.role, NodeRole::Leader);
        assert!(
            node.has_active_lease(),
            "single-voter leader must hold the lease unconditionally while leading"
        );
    }

    #[test]
    fn higher_term_fetch_snapshot_request_is_rejected_in_node_engine() {
        // Engine-level: handle_fetch_request already steps down on a
        // higher leader_epoch (audit confirms). This test makes the
        // contract explicit so a future refactor cannot regress it.
        let mut node = RaftNode::new_with_seed(three_voter_check_quorum_config(), 37).unwrap();
        force_leader(&mut node);
        let leader_term_before = node.hard_state.current_term;
        let fetch_req = fetch_at(&node, NodeId(2), Some(leader_term_before.0 + 1));
        let _ = node.handle_fetch_request(fetch_req);
        assert_eq!(
            node.role,
            NodeRole::Follower,
            "leader did not step down on a higher-term FetchRequest"
        );
        assert!(
            node.hard_state.current_term.0 > leader_term_before.0,
            "term did not advance on a higher-term FetchRequest"
        );
    }
}
