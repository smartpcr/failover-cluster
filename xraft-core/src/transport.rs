//! Transport trait — defines how messages are sent between nodes.
//!
//! Concrete implementations live in `xraft-transport`.
//! Method names follow `implementation-plan.md` Stage 4.1:
//! `send_vote`, `send_pre_vote`, `send_fetch`, `send_fetch_snapshot`.

use crate::error::Result;
use crate::message::{
    FetchRequest, FetchResponse, FetchSnapshotChunk, FetchSnapshotRequest, PreVoteRequest,
    PreVoteResponse, VoteRequest, VoteResponse,
};
use crate::types::NodeId;

/// Abstraction over the network transport layer.
///
/// Each method sends one RPC to the identified peer and awaits a response.
/// The `xraft-transport` crate provides a gRPC-based implementation.
pub trait Transport: Send + Sync {
    /// Send a vote request to a peer (real election with incremented term).
    fn send_vote(
        &self,
        to: NodeId,
        request: VoteRequest,
    ) -> impl std::future::Future<Output = Result<VoteResponse>> + Send;

    /// Send a pre-vote request to a peer (no term increment).
    fn send_pre_vote(
        &self,
        to: NodeId,
        request: PreVoteRequest,
    ) -> impl std::future::Future<Output = Result<PreVoteResponse>> + Send;

    /// Send a fetch request to a peer (pull-based replication).
    fn send_fetch(
        &self,
        to: NodeId,
        request: FetchRequest,
    ) -> impl std::future::Future<Output = Result<FetchResponse>> + Send;

    /// Send a snapshot fetch request to a peer (chunked transfer).
    fn send_fetch_snapshot(
        &self,
        to: NodeId,
        request: FetchSnapshotRequest,
    ) -> impl std::future::Future<Output = Result<Vec<FetchSnapshotChunk>>> + Send;
}
