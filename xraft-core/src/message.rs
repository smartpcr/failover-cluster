//! Raft protocol message definitions.
//!
//! These are the in-memory representations used by the consensus engine.
//! Wire-format (protobuf) conversions will be added in Stage 1.3.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage::SnapshotMeta;
use crate::types::{LogIndex, NodeId, Term};

/// Payload carried by a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// An application command.
    Command(Bytes),
    /// A no-op entry appended by a newly elected leader.
    NoOp,
    /// A snapshot marker (metadata only, data stored externally).
    Snapshot(SnapshotMeta),
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub payload: EntryPayload,
}

/// Inputs consumed by the Raft state machine.
#[derive(Debug, Clone)]
pub enum Input {
    Tick,
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    ClientPropose(Bytes),
}

/// Side-effects emitted by the Raft state machine.
#[derive(Debug, Clone)]
pub enum Action {
    PersistHardState,
    AppendEntries(Vec<Entry>),
    SendMessage {
        to: NodeId,
        message: OutboundMessage,
    },
    ApplyToStateMachine(Vec<Entry>),
    TakeSnapshot,
    /// Instruct the driver to install a snapshot received from the leader.
    InstallSnapshot {
        metadata: SnapshotMeta,
        data: Vec<u8>,
    },
    BecomeLeader,
    StepDown,
}

/// Messages sent over the network.
#[derive(Debug, Clone)]
pub enum OutboundMessage {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    FetchSnapshotRequest(FetchSnapshotRequest),
}

/// Request to vote for a candidate (real election with incremented term).
///
/// Every RPC carries `cluster_id` and `leader_epoch` per architecture §2.3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to a vote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

/// Pre-vote request — sent before incrementing term to check quorum
/// reachability and prevent disruption by partitioned nodes (architecture §2.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    /// The term the candidate *would* use if the pre-vote succeeds.
    pub next_term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to a pre-vote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteResponse {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

/// Follower-initiated fetch request (KRaft-style pull replication).
///
/// Carries `last_fetched_epoch` so the leader can detect log divergence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub follower_id: NodeId,
    pub term: Term,
    pub fetch_offset: LogIndex,
    /// The epoch (term) of the last entry the follower has.
    pub last_fetched_epoch: Term,
}

/// Information about a diverging epoch returned by the leader when the
/// follower's log has diverged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergingEpoch {
    /// The epoch that diverged.
    pub epoch: Term,
    /// The offset at which the epoch ends on the leader's log.
    pub end_offset: LogIndex,
}

/// Leader's response to a fetch request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub leader_id: NodeId,
    pub high_watermark: LogIndex,
    pub entries: Vec<Entry>,
    /// Set when the leader detects the follower's log has diverged.
    pub diverging_epoch: Option<DivergingEpoch>,
}

/// Request to fetch a snapshot from the leader (chunked transfer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub follower_id: NodeId,
    pub term: Term,
    /// Byte offset to resume from (0 for initial request).
    pub offset: u64,
    /// Maximum chunk size in bytes.
    pub max_bytes: u64,
}

/// A single chunk of a snapshot being transferred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotChunk {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub metadata: SnapshotMeta,
    pub data: Vec<u8>,
    pub offset: u64,
    /// True when this is the final chunk.
    pub done: bool,
}
