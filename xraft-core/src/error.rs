//! Error types for XRAFT.

use thiserror::Error;

/// Top-level error type for XRAFT operations.
#[derive(Debug, Error)]
pub enum XRaftError {
    /// Storage I/O error.
    #[error("storage error: {0}")]
    Storage(String),
    /// Transport / network error.
    #[error("transport error: {0}")]
    Transport(String),
    /// Operation rejected because this node is not the leader.
    #[error("not leader; current leader hint: {leader_hint:?}")]
    NotLeader {
        leader_hint: Option<crate::types::NodeId>,
    },
    /// Election timed out without achieving quorum.
    #[error("election timeout")]
    ElectionTimeout,
    /// Received a message with an invalid or stale term.
    #[error("invalid term: {0}")]
    InvalidTerm(String),
    /// Log consistency check failed.
    #[error("log inconsistency: {0}")]
    LogInconsistency(String),
    /// The node is shutting down.
    #[error("shutdown in progress")]
    Shutdown,
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, XRaftError>;
