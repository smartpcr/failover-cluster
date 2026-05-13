//! Roles a Raft node can occupy.
//!
//! The roles model both the standard Raft trio (Follower / Candidate /
//! Leader) and the additions popularised by Apache Kafka's `KRaft`:
//!
//! - **`PreCandidate`** — runs the [Pre-Vote] optimisation before bumping
//!   the term, preventing isolated nodes from disrupting an otherwise
//!   healthy cluster.
//! - **Observer** — a non-voting replica that pulls the log but never
//!   participates in elections (Kafka brokers operating against the
//!   `KRaft` metadata quorum).
//!
//! [Pre-Vote]: https://groups.csail.mit.edu/tds/papers/Ongaro/thesis.pdf

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{LogIndex, NodeId};

/// Type-tag for [`Role`] used by telemetry and tests when the per-role
/// data is irrelevant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoleKind {
    /// Follower (default state).
    Follower,
    /// Pre-vote candidacy probe; term not yet incremented.
    PreCandidate,
    /// Real candidacy; term incremented, self-vote cast.
    Candidate,
    /// Leader (replicates entries, sends heartbeats).
    Leader,
    /// Non-voting observer.
    Observer,
}

impl RoleKind {
    /// Returns `true` for roles that participate in elections and may
    /// be elected leader.
    #[must_use]
    pub const fn is_voter_role(self) -> bool {
        matches!(
            self,
            RoleKind::Follower | RoleKind::PreCandidate | RoleKind::Candidate | RoleKind::Leader,
        )
    }
}

/// Per-role state carried by a [`crate::RaftNode`].
#[derive(Debug, Clone)]
pub enum Role {
    /// Follower; may know the current leader via heartbeats.
    Follower {
        /// The leader most recently heard from in `current_term`, if any.
        leader_hint: Option<NodeId>,
    },

    /// Pre-vote candidate. **Does not** bump `current_term`.
    PreCandidate {
        /// Voters whose pre-vote we have received (always includes self).
        votes_received: BTreeSet<NodeId>,
    },

    /// Real candidate; has incremented `current_term` and voted for self.
    Candidate {
        /// Voters whose vote we have received (always includes self).
        votes_received: BTreeSet<NodeId>,
    },

    /// Leader; tracks per-follower replication progress.
    Leader {
        /// `next_index[peer]` — index of the next log entry to send.
        next_index: BTreeMap<NodeId, LogIndex>,
        /// `match_index[peer]` — index of the highest entry known
        /// replicated on `peer`.
        match_index: BTreeMap<NodeId, LogIndex>,
    },

    /// Non-voting observer.
    Observer,
}

impl Role {
    /// The discriminant of this role.
    #[must_use]
    pub const fn kind(&self) -> RoleKind {
        match self {
            Role::Follower { .. } => RoleKind::Follower,
            Role::PreCandidate { .. } => RoleKind::PreCandidate,
            Role::Candidate { .. } => RoleKind::Candidate,
            Role::Leader { .. } => RoleKind::Leader,
            Role::Observer => RoleKind::Observer,
        }
    }

    /// Fresh `Follower` state with no known leader.
    #[must_use]
    pub fn fresh_follower() -> Self {
        Role::Follower { leader_hint: None }
    }

    /// `Follower` state with a known leader hint.
    #[must_use]
    pub fn follower_with_leader(leader: NodeId) -> Self {
        Role::Follower {
            leader_hint: Some(leader),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_kind_discriminates() {
        assert_eq!(Role::fresh_follower().kind(), RoleKind::Follower);
        assert_eq!(Role::Observer.kind(), RoleKind::Observer);
    }

    #[test]
    fn voter_role_classification() {
        assert!(RoleKind::Follower.is_voter_role());
        assert!(RoleKind::PreCandidate.is_voter_role());
        assert!(RoleKind::Candidate.is_voter_role());
        assert!(RoleKind::Leader.is_voter_role());
        assert!(!RoleKind::Observer.is_voter_role());
    }
}
