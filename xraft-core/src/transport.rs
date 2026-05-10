//! Transport trait — defines how messages are sent between nodes.
//!
//! Concrete implementations live in `xraft-transport`.

use crate::error::Result;
use crate::message::{FetchRequest, FetchResponse, VoteRequest, VoteResponse};
use crate::types::NodeId;

/// Abstraction over the network transport layer.
pub trait Transport: Send + Sync {
    /// Send a vote request to a peer.
    fn send_vote_request(
        &self,
        to: NodeId,
        request: VoteRequest,
    ) -> impl std::future::Future<Output = Result<VoteResponse>> + Send;

    /// Send a fetch request to a peer (pull-based replication).
    fn send_fetch_request(
        &self,
        to: NodeId,
        request: FetchRequest,
    ) -> impl std::future::Future<Output = Result<FetchResponse>> + Send;
}
