//! Static configuration of a Raft node.
//!
//! Configuration is *static* in the sense that it does not change
//! during a single call to the state machine. Voter-set changes at
//! runtime are modelled as `Event::PromoteToVoter` /
//! `DemoteToObserver` and a separate membership-change stage will
//! gate those events on committed configuration entries.

use std::collections::BTreeSet;

use crate::error::{RaftError, RaftResult};
use crate::types::NodeId;

/// Configuration handed to the state machine at construction.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Identifier of the local node.
    pub id: NodeId,
    /// Voting members of the cluster, including the local node iff
    /// the local node is a voter.
    pub voters: BTreeSet<NodeId>,
    /// Non-voting observers (`KRaft` brokers, learners, hot standbys).
    pub observers: BTreeSet<NodeId>,
    /// Whether the state machine should use the Pre-Vote optimisation.
    pub pre_vote_enabled: bool,
}

impl NodeConfig {
    /// Build a new configuration, validating basic invariants.
    pub fn new(
        id: NodeId,
        voters: BTreeSet<NodeId>,
        observers: BTreeSet<NodeId>,
        pre_vote_enabled: bool,
    ) -> RaftResult<Self> {
        if !voters.is_disjoint(&observers) {
            return Err(RaftError::InvalidMembershipChange {
                node: id,
                reason: "voters and observers must be disjoint",
            });
        }
        if !voters.contains(&id) && !observers.contains(&id) {
            return Err(RaftError::InvalidMembershipChange {
                node: id,
                reason: "local node must be in voters or observers",
            });
        }
        Ok(Self {
            id,
            voters,
            observers,
            pre_vote_enabled,
        })
    }

    /// True when the local node is part of the voter set.
    #[must_use]
    pub fn is_voter(&self) -> bool {
        self.voters.contains(&self.id)
    }

    /// True when the local node is part of the observer set.
    #[must_use]
    pub fn is_observer(&self) -> bool {
        self.observers.contains(&self.id)
    }

    /// Number of votes required for a majority — `(N / 2) + 1` where
    /// `N` is the total number of voters.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        (self.voters.len() / 2) + 1
    }

    /// Number of voters whose failure the cluster can tolerate.
    /// `(N - 1) / 2` — matches the `KRaft` article's formula.
    #[must_use]
    pub fn fault_tolerance(&self) -> usize {
        self.voters.len().saturating_sub(1) / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(xs: &[u64]) -> BTreeSet<NodeId> {
        xs.iter().copied().map(NodeId::new).collect()
    }

    #[test]
    fn voter_membership_is_detected() {
        let cfg = NodeConfig::new(NodeId::new(1), ids(&[1, 2, 3]), ids(&[]), true).unwrap();
        assert!(cfg.is_voter());
        assert!(!cfg.is_observer());
    }

    #[test]
    fn observer_membership_is_detected() {
        let cfg = NodeConfig::new(NodeId::new(7), ids(&[1, 2, 3]), ids(&[7, 8]), true).unwrap();
        assert!(!cfg.is_voter());
        assert!(cfg.is_observer());
    }

    #[test]
    fn quorum_sizes_match_paper() {
        let c3 = NodeConfig::new(NodeId::new(1), ids(&[1, 2, 3]), ids(&[]), true).unwrap();
        assert_eq!(c3.quorum_size(), 2);
        assert_eq!(c3.fault_tolerance(), 1);

        let c5 = NodeConfig::new(NodeId::new(1), ids(&[1, 2, 3, 4, 5]), ids(&[]), true).unwrap();
        assert_eq!(c5.quorum_size(), 3);
        assert_eq!(c5.fault_tolerance(), 2);

        let c4 = NodeConfig::new(NodeId::new(1), ids(&[1, 2, 3, 4]), ids(&[]), true).unwrap();
        assert_eq!(c4.quorum_size(), 3);
        assert_eq!(c4.fault_tolerance(), 1);
    }

    #[test]
    fn overlapping_voters_observers_rejected() {
        let err = NodeConfig::new(NodeId::new(1), ids(&[1, 2]), ids(&[2, 3]), true).unwrap_err();
        assert!(matches!(err, RaftError::InvalidMembershipChange { .. }));
    }

    #[test]
    fn local_node_must_be_known() {
        let err = NodeConfig::new(NodeId::new(99), ids(&[1, 2, 3]), ids(&[]), true).unwrap_err();
        assert!(matches!(err, RaftError::InvalidMembershipChange { .. }));
    }
}
