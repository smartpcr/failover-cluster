//! Raft protocol message definitions.
//!
//! These are the in-memory representations used by the consensus engine.
//! Wire-format (protobuf) conversions will be added in Stage 1.3.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{LogIndex, NodeId, Term};

/// Payload carried by a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// An application command.
    Command(Bytes),
    /// A no-op entry appended by a newly elected leader.
    NoOp,
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
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    ClientPropose(Bytes),
}

/// Side-effects emitted by the Raft state machine.
#[derive(Debug, Clone)]
pub enum Action {
    PersistHardState,
    AppendEntries(Vec<Entry>),
    SendMessage { to: NodeId, message: OutboundMessage },
    ApplyToStateMachine(Vec<Entry>),
    TakeSnapshot,
    BecomeLeader,
    StepDown,
}

/// Messages sent over the network.
#[derive(Debug, Clone)]
pub enum OutboundMessage {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    FetchResponse(FetchResponse),
}

/// Request to vote for a candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub pre_vote: bool,
}

/// Response to a vote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
    pub pre_vote: bool,
}

/// Follower-initiated fetch request (KRaft-style pull replication).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub follower_id: NodeId,
    pub term: Term,
    pub fetch_offset: LogIndex,
}

/// Leader's response to a fetch request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub term: Term,
    pub leader_id: NodeId,
    pub high_watermark: LogIndex,
    pub entries: Vec<Entry>,
}
