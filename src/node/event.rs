//! Events delivered to the Raft state machine.
//!
//! Events are the **inputs** to the SM. A host runtime translates wire
//! messages, timer expirations, and configuration changes into these
//! typed values and feeds them to
//! [`RaftNode::handle`](crate::RaftNode::handle). The SM stays pure:
//! given a `(state, event)` pair, it produces a deterministic
//! `(state', Vec<Command>)`.

use crate::types::{LogIndex, LogMetadata, NodeId, Term};

/// All events the state machine understands.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    // ---------- timers ----------
    /// The local election timer expired.
    ElectionTimeout,

    /// The leader's heartbeat timer fired. Leaders broadcast empty
    /// `AppendEntries` to refresh follower election timers.
    HeartbeatTick,

    // ---------- inbound RPC requests ----------
    /// A peer is asking us to vote for it.
    RequestVoteRequest {
        /// `true` for a pre-vote probe; `false` for a real vote.
        pre_vote: bool,
        /// Candidate's identifier.
        candidate_id: NodeId,
        /// Candidate's prospective term — `current_term + 1` for
        /// pre-votes, `current_term` for real votes.
        candidate_term: Term,
        /// Candidate's last log metadata, for the up-to-date check.
        candidate_log: LogMetadata,
        /// Whether the local election timer would have expired by now
        /// (used to honour the leader-lease/check-quorum guard on
        /// pre-vote responses; ignored for real votes).
        local_election_timeout_elapsed: bool,
    },

    /// A peer wants us to append log entries (or is sending a
    /// heartbeat with no entries).
    AppendEntriesRequest {
        /// Leader's identifier.
        leader_id: NodeId,
        /// Leader's term.
        leader_term: Term,
        /// `prev_log_index` from the `AppendEntries` RPC.
        prev_log_index: LogIndex,
        /// `prev_log_term` from the `AppendEntries` RPC.
        prev_log_term: Term,
        /// Leader's commit index.
        leader_commit: LogIndex,
        /// True if the host's log stage has already determined the
        /// append is consistent (matching `prev_log_index` /
        /// `prev_log_term`).
        log_ok: bool,
        /// Number of entries the leader is asking us to append.
        entry_count: u64,
    },

    // ---------- inbound RPC responses ----------
    /// A peer responded to our `RequestVote`.
    RequestVoteResponse {
        /// `true` if this response was for a pre-vote probe.
        pre_vote: bool,
        /// Peer that sent the response.
        from: NodeId,
        /// Term in the response.
        term: Term,
        /// `true` iff the peer granted the vote.
        vote_granted: bool,
    },

    /// A peer responded to one of our `AppendEntries` (only meaningful
    /// while we are leader).
    AppendEntriesResponse {
        /// Peer that sent the response.
        from: NodeId,
        /// Term in the response.
        term: Term,
        /// Whether the peer accepted the entries.
        success: bool,
        /// Peer's match index after applying our entries (only
        /// meaningful when `success == true`).
        match_index: LogIndex,
    },

    // ---------- local log updates ----------
    /// The local log layer's tail moved (append or truncation).
    ///
    /// The SM caches `(last_index, last_term)` so vote handling can
    /// run the up-to-date check without calling out. The host is
    /// authoritative; backward moves after log truncation are
    /// accepted.
    LogTailUpdated {
        /// Fresh log metadata.
        metadata: LogMetadata,
    },

    // ---------- membership ----------
    /// Promote `node` from observer to voter.
    ///
    /// Precondition: the host must only emit this event after the
    /// corresponding `VotersRecord` (or equivalent membership-change
    /// record) has been committed by the log/configuration stage. The
    /// state machine does not enforce joint-consensus rules itself.
    PromoteToVoter {
        /// Identifier of the node being promoted.
        node: NodeId,
    },

    /// Demote `node` from voter to observer. Same commit-precondition
    /// applies. If `node == self`, the local node immediately steps
    /// down into the `Observer` role.
    DemoteToObserver {
        /// Identifier of the node being demoted.
        node: NodeId,
    },

    // ---------- lifecycle ----------
    /// Shut the state machine down. Further events are rejected.
    Shutdown,
}
