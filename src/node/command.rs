//! Commands emitted by the state machine.
//!
//! Commands describe **what the host should do** in response to an
//! event. The state machine never executes them itself; it only
//! returns them in the order they should be applied. Side-effects
//! (timers, RPC transmission, persistence) live entirely in the host.

use crate::node::role::RoleKind;
use crate::types::{LogIndex, LogMetadata, NodeId, Term};

/// All possible outputs from a single call to
/// [`RaftNode::handle`](crate::RaftNode::handle).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    // ---------- timers ----------
    /// (Re)start the election timer with a fresh randomised timeout.
    ResetElectionTimer,
    /// Stop the election timer (used when becoming leader or observer).
    StopElectionTimer,
    /// Start the heartbeat timer (used on transition to leader).
    StartHeartbeatTimer,
    /// Stop the heartbeat timer (used on step-down from leader).
    StopHeartbeatTimer,

    // ---------- RPC out ----------
    /// Broadcast a `RequestVote` (real or pre-vote) to all peer voters.
    BroadcastRequestVote {
        /// `true` for the pre-vote probe.
        pre_vote: bool,
        /// Prospective term being asked about.
        term: Term,
        /// Local node's last log metadata.
        last_log: LogMetadata,
    },

    /// Reply to a `RequestVote` from `to`.
    SendVoteResponse {
        /// `true` if this response is to a pre-vote request.
        pre_vote: bool,
        /// Destination peer.
        to: NodeId,
        /// Local node's `current_term` at the time of the response.
        term: Term,
        /// Whether the vote / pre-vote was granted.
        vote_granted: bool,
    },

    /// Broadcast an empty `AppendEntries` (heartbeat) to all followers
    /// and observers.
    BroadcastHeartbeat {
        /// Leader's term.
        term: Term,
        /// Leader's commit index.
        leader_commit: LogIndex,
    },

    /// Reply to an `AppendEntries` from `to`.
    SendAppendEntriesResponse {
        /// Destination peer.
        to: NodeId,
        /// Local node's `current_term`.
        term: Term,
        /// `true` if the append was consistent and accepted.
        success: bool,
        /// Match index after the append (when `success`).
        match_index: LogIndex,
    },

    // ---------- log stage hook ----------
    /// Newly elected leader asks the log stage to append the per-term
    /// blank no-op entry (Raft §8) which establishes commit for any
    /// uncommitted records from the previous term.
    AppendLeaderNoop {
        /// The term the leader is starting.
        term: Term,
    },

    // ---------- persistence ----------
    /// Flush `current_term` / `voted_for` to stable storage *before*
    /// sending any RPC response that follows in this command batch.
    PersistState,

    // ---------- observability ----------
    /// The node's role changed. Useful for tests and telemetry.
    RoleChanged {
        /// Role we are leaving.
        previous: RoleKind,
        /// Role we are entering.
        current: RoleKind,
        /// `current_term` after the transition.
        term: Term,
        /// Current leader (if known) after the transition.
        leader: Option<NodeId>,
    },
}
