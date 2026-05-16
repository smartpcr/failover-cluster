//! Transport trait — defines how messages are sent between nodes.
//!
//! Concrete implementations live in `xraft-transport`.
//! Method names follow `implementation-plan.md` Stage 4.1:
//! `send_vote`, `send_pre_vote`, `send_fetch`, `send_fetch_snapshot`,
//! `start_server`.

use std::pin::Pin;
use std::sync::Arc;

use crate::error::Result;
use crate::message::{
    FetchRequest, FetchResponse, FetchSnapshotChunk, FetchSnapshotRequest, PreVoteRequest,
    PreVoteResponse, VoteRequest, VoteResponse,
};
use crate::types::NodeId;

/// A stream of `FetchSnapshotChunk` items yielded by a server-streaming
/// `FetchSnapshot` RPC.
///
/// This is a type alias for a pinned, boxed, `Send`-safe stream, matching
/// the proto definition:
/// ```protobuf
/// rpc FetchSnapshot(FetchSnapshotRequest) returns (stream FetchSnapshotChunk);
/// ```
pub type SnapshotChunkStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<FetchSnapshotChunk>> + Send>>;

/// Server-side dispatch trait for incoming Raft RPCs.
///
/// The gRPC server in `xraft-transport` translates each incoming protobuf
/// request to its canonical Rust counterpart, calls the matching handler
/// method on the `RaftMessageHandler` injected at construction time, and
/// translates the returned response back to protobuf. The handler is the
/// integration seam between the transport layer (Stage 4.1) and the
/// driver loop (Stage 4.2) that ultimately owns the `RaftNode`.
///
/// # Implementation contract
///
/// Implementations MUST drive any `Action`s emitted by the underlying
/// `RaftNode` to completion BEFORE returning the response. In particular,
/// `Action::PersistHardState` must be persisted before a `VoteResponse`
/// granting a vote is returned, otherwise a crash between reply and
/// persistence can violate the Raft single-vote-per-term invariant.
///
/// Stage 4.1 ships a stub-friendly trait so the gRPC machinery can be
/// exercised in isolation; the real `RaftNode`-backed implementation is
/// added in Stage 4.2 alongside the driver loop.
pub trait RaftMessageHandler: Send + Sync + 'static {
    /// Handle an inbound real-election `VoteRequest`.
    fn handle_vote(
        &self,
        request: VoteRequest,
    ) -> impl std::future::Future<Output = Result<VoteResponse>> + Send;

    /// Handle an inbound `PreVoteRequest` (no term mutation on the responder).
    fn handle_pre_vote(
        &self,
        request: PreVoteRequest,
    ) -> impl std::future::Future<Output = Result<PreVoteResponse>> + Send;

    /// Handle an inbound `FetchRequest` (pull-based replication).
    fn handle_fetch(
        &self,
        request: FetchRequest,
    ) -> impl std::future::Future<Output = Result<FetchResponse>> + Send;

    /// Handle an inbound `FetchSnapshotRequest` and return a server-streaming
    /// chunk stream. The first chunk MUST carry `SnapshotMeta`.
    ///
    /// The terminal-chunk marker `done = true` indicates the **entire
    /// snapshot payload** has been delivered. A bounded request
    /// (`FetchSnapshotRequest.max_bytes > 0`) that does not cover the
    /// snapshot tail legitimately ends with the final chunk carrying
    /// `done = false` — the caller resumes via a follow-up
    /// `FetchSnapshotRequest` at `offset = request.offset + bytes_received`
    /// (see [`SnapshotStore::snapshot_reader_from_offset`](crate::storage::SnapshotStore::snapshot_reader_from_offset)).
    /// Servers MUST NOT exceed `request.max_bytes` total payload across
    /// the response window when `max_bytes > 0`; over-served streams
    /// are treated by callers as a protocol violation.
    fn handle_fetch_snapshot(
        &self,
        request: FetchSnapshotRequest,
    ) -> impl std::future::Future<Output = Result<SnapshotChunkStream>> + Send;
}

/// Abstraction over the network transport layer.
///
/// Each `send_*` method sends one RPC to the identified peer and awaits a
/// response. `start_server` boots the inbound gRPC server and runs until the
/// transport's shutdown signal is fired. The `xraft-transport` crate provides
/// a gRPC-based implementation.
///
/// `start_server` takes `self: Arc<Self>` so the returned future is `'static`
/// and can be passed to `tokio::spawn`. Outbound `send_*` methods take `&self`
/// because they do not need to outlive the caller's borrow.
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

    /// Send a snapshot fetch request to a peer (server-streaming chunked transfer).
    ///
    /// Returns a stream of [`FetchSnapshotChunk`] items matching the proto:
    /// `rpc FetchSnapshot(FetchSnapshotRequest) returns (stream FetchSnapshotChunk);`
    ///
    /// The first chunk carries [`SnapshotMeta`](crate::storage::SnapshotMeta)
    /// in its `metadata` field; subsequent chunks carry only payload data.
    ///
    /// The `done = true` marker indicates the **entire snapshot
    /// payload** has been delivered. When the request specifies a
    /// bounded window (`FetchSnapshotRequest.max_bytes > 0`) and that
    /// window does **not** cover the snapshot tail, the final chunk
    /// of the response legitimately carries `done = false` and
    /// `bytes_received` will equal `request.max_bytes` — the caller
    /// is expected to resume by issuing a follow-up request at
    /// `offset = request.offset + bytes_received`. This resumable
    /// bounded-window contract is enforced by
    /// [`SnapshotStore::snapshot_reader_from_offset`](crate::storage::SnapshotStore::snapshot_reader_from_offset)
    /// on the leader and by `MessageRouter` on the follower.
    /// Peers MUST NOT exceed `request.max_bytes` total payload bytes
    /// across the streamed response window; over-served streams are
    /// treated by callers as a protocol violation.
    fn send_fetch_snapshot(
        &self,
        to: NodeId,
        request: FetchSnapshotRequest,
    ) -> impl std::future::Future<Output = Result<SnapshotChunkStream>> + Send;

    /// Bind the inbound gRPC server to the configured listen address and
    /// serve until shutdown is signalled. Returns `Ok(())` on graceful
    /// shutdown or an `Err` if the listener cannot bind / serve.
    ///
    /// Takes `self: Arc<Self>` so the returned future has a `'static`
    /// lifetime and can be spawned via `tokio::spawn(transport.start_server())`.
    fn start_server(
        self: Arc<Self>,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static;
}
