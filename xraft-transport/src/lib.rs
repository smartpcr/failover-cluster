//! `xraft-transport` — gRPC transport layer via tonic.
//!
//! This crate provides the wire-level transport for XRAFT RPCs:
//! - `pb` — generated tonic service trait, server, and client. Message
//!   types are re-exported from `xraft_core::message::proto::*` (see
//!   `build.rs`); only the service plumbing is generated here.
//! - [`RaftGrpcServer`] — adapts an `xraft_core::transport::RaftMessageHandler`
//!   to the generated `RaftService` server trait.
//! - [`RaftGrpcClient`] — connection-pooled, retry-enabled tonic client
//!   for sending Vote / PreVote / Fetch / FetchSnapshot RPCs to peers.
//! - [`GrpcTransport`] — composes server + client into a single object
//!   implementing [`xraft_core::transport::Transport`].

pub mod grpc;
pub mod grpc_client;
pub mod grpc_server;

/// Generated tonic service trait, server, and client for `RaftService`.
///
/// Message types are imported from `xraft_core::message::proto::*` via the
/// `extern_path` configuration in this crate's `build.rs`, so this module
/// contains only `raft_service_server` and `raft_service_client` plus a
/// few wrapper types.
pub mod pb {
    // re-export the canonical proto message types so call-sites can write
    // `pb::VoteRequest` without reaching into xraft-core directly.
    pub use ::xraft_core::message::proto::*;

    // tonic-generated service plumbing.
    tonic::include_proto!("xraft");
}

pub use grpc::{GrpcTransport, GrpcTransportConfig, TlsTransportConfig};
pub use grpc_client::RaftGrpcClient;
pub use grpc_server::RaftGrpcServer;
