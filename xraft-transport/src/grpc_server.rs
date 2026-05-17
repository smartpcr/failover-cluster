//! gRPC server adapter: bridges tonic's generated `RaftService` trait to
//! the [`RaftMessageHandler`] supplied by the embedding application.
//!
//! The adapter is intentionally thin — it converts between protobuf and
//! canonical Rust types and forwards the call to the handler. All Raft
//! state lives behind the handler (typically the Stage 4.2 driver loop),
//! never inside this struct.

// `tonic::Status` is a large error type (≈176 bytes) carrying gRPC metadata,
// trailers, and message; every tonic service method returns it. Boxing
// would obscure call sites for no real benefit.
#![allow(clippy::result_large_err)]

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{debug, error};

use xraft_core::message::{FetchRequest, FetchSnapshotRequest, PreVoteRequest, VoteRequest};
use xraft_core::transport::RaftMessageHandler;

use crate::pb;
use crate::pb::raft_service_server::{RaftService, RaftServiceServer};

/// Adapter that implements the tonic-generated `RaftService` trait by
/// dispatching every incoming RPC to a [`RaftMessageHandler`].
///
/// Construct via [`RaftGrpcServer::new`] and turn into a tonic service via
/// [`RaftGrpcServer::into_service`]; the resulting `RaftServiceServer<Self>`
/// can be plugged into `tonic::transport::Server::builder().add_service(...)`.
#[derive(Debug)]
pub struct RaftGrpcServer<H: RaftMessageHandler> {
    handler: Arc<H>,
}

impl<H: RaftMessageHandler> RaftGrpcServer<H> {
    /// Wrap a handler in the gRPC adapter.
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }

    /// Consume `self` and produce the tonic `RaftServiceServer` that can be
    /// added to a `Server::builder()`.
    pub fn into_service(self) -> RaftServiceServer<Self> {
        RaftServiceServer::new(self)
    }
}

/// Type alias for the server-streaming response produced by `FetchSnapshot`.
type FetchSnapshotStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<pb::FetchSnapshotChunk, Status>> + Send>>;

#[tonic::async_trait]
impl<H: RaftMessageHandler> RaftService for RaftGrpcServer<H> {
    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let req = VoteRequest::from(request.into_inner());
        debug!(target: "xraft_transport::server", candidate = req.candidate_id.0, term = req.term.0, "Vote RPC");
        let resp = self.handler.handle_vote(req).await.map_err(|e| {
            error!(target: "xraft_transport::server", "Vote handler error: {e}");
            Status::internal(format!("vote handler error: {e}"))
        })?;
        Ok(Response::new(pb::VoteResponse::from(&resp)))
    }

    async fn pre_vote(
        &self,
        request: Request<pb::PreVoteRequest>,
    ) -> Result<Response<pb::PreVoteResponse>, Status> {
        let req = PreVoteRequest::from(request.into_inner());
        debug!(target: "xraft_transport::server", candidate = req.candidate_id.0, next_term = req.next_term.0, "PreVote RPC");
        let resp = self.handler.handle_pre_vote(req).await.map_err(|e| {
            error!(target: "xraft_transport::server", "PreVote handler error: {e}");
            Status::internal(format!("pre_vote handler error: {e}"))
        })?;
        Ok(Response::new(pb::PreVoteResponse::from(&resp)))
    }

    async fn fetch(
        &self,
        request: Request<pb::FetchRequest>,
    ) -> Result<Response<pb::FetchResponse>, Status> {
        let req = FetchRequest::from(request.into_inner());
        debug!(target: "xraft_transport::server", replica = req.replica_id.0, fetch_offset = req.fetch_offset.0, "Fetch RPC");
        let resp = self.handler.handle_fetch(req).await.map_err(|e| {
            error!(target: "xraft_transport::server", "Fetch handler error: {e}");
            Status::internal(format!("fetch handler error: {e}"))
        })?;
        let proto_resp = pb::FetchResponse::try_from(&resp).map_err(|e| {
            error!(target: "xraft_transport::server", "Fetch response encode error: {e}");
            Status::internal(format!("fetch response encode error: {e}"))
        })?;
        Ok(Response::new(proto_resp))
    }

    type FetchSnapshotStream = FetchSnapshotStream;

    async fn fetch_snapshot(
        &self,
        request: Request<pb::FetchSnapshotRequest>,
    ) -> Result<Response<Self::FetchSnapshotStream>, Status> {
        let req = FetchSnapshotRequest::from(request.into_inner());
        debug!(target: "xraft_transport::server", replica = req.replica_id.0, snapshot_id = %req.snapshot_id, "FetchSnapshot RPC");
        let stream = self.handler.handle_fetch_snapshot(req).await.map_err(|e| {
            error!(target: "xraft_transport::server", "FetchSnapshot handler error: {e}");
            Status::internal(format!("fetch_snapshot handler error: {e}"))
        })?;
        let mapped: Self::FetchSnapshotStream = Box::pin(stream.map(|item| match item {
            Ok(chunk) => Ok(pb::FetchSnapshotChunk::from(&chunk)),
            Err(e) => Err(Status::internal(format!(
                "fetch_snapshot stream error: {e}"
            ))),
        }));
        Ok(Response::new(mapped))
    }
}
