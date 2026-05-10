//! Core Raft type definitions.

use serde::{Deserialize, Serialize};

/// Unique identifier for a node in the cluster.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct NodeId(pub u64);

/// Raft term — monotonically increasing logical clock.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct Term(pub u64);

/// 1-based position in the replicated log.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct LogIndex(pub u64);

/// Role a node can be in within the Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRole {
    /// Active leader serving reads and writes.
    Leader,
    /// Passive follower replicating from the leader.
    Follower,
    /// Pre-candidate: checking quorum reachability before starting a real election.
    PreCandidate,
    /// Candidate: running an election with an incremented term.
    Candidate,
    /// Non-voting observer replicating the log for read scaling or standby.
    Observer,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Follower
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Term({})", self.0)
    }
}

impl std::fmt::Display for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogIndex({})", self.0)
    }
}
