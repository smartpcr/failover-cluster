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
    /// Snapshot file is corrupt (bad magic, CRC mismatch, truncated header).
    #[error("corrupt snapshot: {0}")]
    CorruptSnapshot(String),
    /// Requested snapshot was not found in the store.
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    /// Operation explicitly out of scope for v1 — used by Stage 7.2
    /// `AddVoter` / `RemoveVoter` rejection per `tech-spec.md` §2.7,
    /// `architecture.md` §5.5, and `e2e-scenarios.md` Feature 12 ΓÇö
    /// dynamic cluster membership is deferred to a future story
    /// entirely, not a stretch goal within XRAFT v1. Returned by
    /// every boundary (admin HTTP, `DriverHandle::add_voter` /
    /// `remove_voter`) that an operator might use to mutate the
    /// voter set; the voter set is therefore guaranteed to stay
    /// static for the v1 deliverable.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, XRaftError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;

    #[test]
    fn display_storage_error() {
        let e = XRaftError::Storage("disk full".into());
        assert_eq!(format!("{e}"), "storage error: disk full");
    }

    #[test]
    fn display_transport_error() {
        let e = XRaftError::Transport("connection refused".into());
        assert_eq!(format!("{e}"), "transport error: connection refused");
    }

    #[test]
    fn display_not_leader_with_hint() {
        let e = XRaftError::NotLeader {
            leader_hint: Some(NodeId(3)),
        };
        let msg = format!("{e}");
        assert!(msg.contains("not leader"), "got: {msg}");
        assert!(msg.contains("NodeId(3)"), "got: {msg}");
    }

    #[test]
    fn display_not_leader_no_hint() {
        let e = XRaftError::NotLeader { leader_hint: None };
        let msg = format!("{e}");
        assert!(msg.contains("not leader"), "got: {msg}");
        assert!(msg.contains("None"), "got: {msg}");
    }

    #[test]
    fn display_election_timeout() {
        let e = XRaftError::ElectionTimeout;
        assert_eq!(format!("{e}"), "election timeout");
    }

    #[test]
    fn display_invalid_term() {
        let e = XRaftError::InvalidTerm("stale term 3".into());
        assert_eq!(format!("{e}"), "invalid term: stale term 3");
    }

    #[test]
    fn display_log_inconsistency() {
        let e = XRaftError::LogInconsistency("gap at index 5".into());
        assert_eq!(format!("{e}"), "log inconsistency: gap at index 5");
    }

    #[test]
    fn display_shutdown() {
        let e = XRaftError::Shutdown;
        assert_eq!(format!("{e}"), "shutdown in progress");
    }

    #[test]
    fn display_config_error() {
        let e = XRaftError::Config("missing node_id".into());
        assert_eq!(format!("{e}"), "configuration error: missing node_id");
    }

    #[test]
    fn display_corrupt_snapshot() {
        let e = XRaftError::CorruptSnapshot("bad CRC at offset 0".into());
        assert_eq!(format!("{e}"), "corrupt snapshot: bad CRC at offset 0");
    }

    #[test]
    fn display_snapshot_not_found() {
        let e = XRaftError::SnapshotNotFound("snapshot-0000000002-00000000000000000010".into());
        let msg = format!("{e}");
        assert!(msg.contains("snapshot not found"), "got: {msg}");
    }

    #[test]
    fn display_unsupported() {
        // Stage 7.2 boundary rejection: every `AddVoter` / `RemoveVoter`
        // entry point must surface the same error variant so callers
        // can pattern-match `Err(XRaftError::Unsupported(_))` without
        // string-sniffing the diagnostic.
        let e = XRaftError::Unsupported(
            "AddVoter is out of scope for v1 — dynamic membership is deferred to a \
             future story entirely (per architecture.md §5.5 and e2e-scenarios.md \
             Feature 12)"
                .into(),
        );
        let msg = format!("{e}");
        assert!(msg.starts_with("unsupported: "), "got: {msg}");
        assert!(msg.contains("AddVoter"), "got: {msg}");
        assert!(msg.contains("out of scope for v1"), "got: {msg}");
    }
}
