//! Error type for the Raft node state machine.

use core::fmt;

use crate::types::{NodeId, Term};

/// Errors returned by the state machine when an event is invalid or
/// inconsistent with the current configuration.
///
/// Receiving a stale-term RPC or losing an election is **not** an
/// error — those are normal protocol outcomes and produce a response
/// command, not an `Err`. Errors here always indicate a programming
/// mistake by the host runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RaftError {
    /// The host tried to drive a node that has already shut down.
    AlreadyShutDown,

    /// A configuration event referenced a node that is not part of
    /// the cluster, or attempted an illegal role change (e.g.
    /// promoting a node that is already a voter).
    InvalidMembershipChange {
        /// The node the host attempted to promote/demote.
        node: NodeId,
        /// Human-readable detail.
        reason: &'static str,
    },

    /// A response event carried a term that violates monotonicity
    /// invariants the host is responsible for maintaining.
    InconsistentTerm {
        /// Term reported by the event.
        event_term: Term,
        /// Term currently held by the state machine.
        current_term: Term,
        /// Human-readable detail.
        reason: &'static str,
    },
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftError::AlreadyShutDown => write!(f, "raft node has already shut down"),
            RaftError::InvalidMembershipChange { node, reason } => {
                write!(f, "invalid membership change for {node}: {reason}")
            }
            RaftError::InconsistentTerm {
                event_term,
                current_term,
                reason,
            } => write!(
                f,
                "inconsistent term (event {event_term}, current {current_term}): {reason}"
            ),
        }
    }
}

impl std::error::Error for RaftError {}

/// `Result` alias used throughout the crate.
pub type RaftResult<T> = Result<T, RaftError>;
