//! Raft node state machine — core consensus engine.
//!
//! `RaftNode` holds the volatile and durable state for a single Raft participant.
//! It processes [`Input`] events and emits [`Action`] side-effects without
//! performing any I/O itself (I/O is delegated to the driver layer in `xraft-server`).
//!
//! Full election, replication, and snapshotting logic will be implemented in
//! Stage 3 (Election & Leader Lifecycle) and Stage 4 (Pull-Based Replication).

use std::collections::{HashMap, HashSet};

use crate::config::ClusterConfig;
use crate::message::{Action, Input};
use crate::types::{HardState, LogIndex, NodeId, NodeRole, Term};

/// Volatile follower / replication progress tracked by the leader.
#[derive(Debug, Clone)]
pub struct ReplicationState {
    /// The next log index to send to this follower.
    pub next_index: LogIndex,
    /// The highest log index known to be replicated on this follower.
    pub match_index: LogIndex,
}

/// Core Raft consensus state machine.
///
/// Processes inputs (ticks, RPCs, client proposals) and produces a list of
/// side-effect [`Action`]s that the driver must execute. This separation
/// keeps the consensus engine pure and testable.
#[derive(Debug)]
pub struct RaftNode {
    /// This node's identity.
    pub id: NodeId,
    /// Current role in the cluster.
    pub role: NodeRole,
    /// Durable state: current term + vote.
    pub hard_state: HardState,
    /// Index of the highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of the highest log entry applied to the state machine.
    pub last_applied: LogIndex,
    /// Ticks since last heartbeat/election event — used for timeout detection.
    pub ticks_since_last_event: u64,
    /// Randomised election timeout for this term (in ticks).
    pub election_timeout_ticks: u64,
    /// Set of votes received in the current election (only meaningful when
    /// role is `Candidate` or `PreCandidate`).
    pub votes_received: HashSet<NodeId>,
    /// Per-follower replication progress (only meaningful when role is `Leader`).
    pub follower_progress: HashMap<NodeId, ReplicationState>,
    /// Cluster configuration.
    pub config: ClusterConfig,
    /// Known leader for the current term, if any.
    pub leader_id: Option<NodeId>,
}

impl RaftNode {
    /// Create a new `RaftNode` in `Follower` state at term 0 with no vote.
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            id: config.node_id,
            role: NodeRole::Follower,
            hard_state: HardState {
                current_term: Term(0),
                voted_for: None,
            },
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            ticks_since_last_event: 0,
            election_timeout_ticks: 0,
            votes_received: HashSet::new(),
            follower_progress: HashMap::new(),
            config,
            leader_id: None,
        }
    }

    /// The current term this node is in.
    pub fn current_term(&self) -> Term {
        self.hard_state.current_term
    }

    /// Whether this node believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// Step the node forward by processing an input event.
    ///
    /// Returns a list of [`Action`]s the driver must execute (persist state,
    /// send messages, apply entries, etc.). The actual logic will be filled
    /// in during Stage 3 / Stage 4.
    pub fn step(&mut self, _input: Input) -> Vec<Action> {
        // Placeholder — full implementation in Stage 3 (elections) and
        // Stage 4 (pull-based replication).
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use crate::types::{NodeId, NodeRole, Term};

    fn test_config() -> ClusterConfig {
        ClusterConfig::from_toml_str(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "0.0.0.0:6000"
peers = ["node2:6000", "node3:6000"]
"#,
        )
        .unwrap()
    }

    #[test]
    fn new_node_starts_as_follower() {
        let node = RaftNode::new(test_config());
        assert_eq!(node.role, NodeRole::Follower);
        assert_eq!(node.current_term(), Term(0));
        assert!(!node.is_leader());
        assert!(node.leader_id.is_none());
    }

    #[test]
    fn new_node_has_correct_id() {
        let node = RaftNode::new(test_config());
        assert_eq!(node.id, NodeId(1));
    }

    #[test]
    fn new_node_starts_with_zero_indices() {
        let node = RaftNode::new(test_config());
        assert_eq!(node.commit_index, LogIndex(0));
        assert_eq!(node.last_applied, LogIndex(0));
    }

    #[test]
    fn new_node_has_no_votes() {
        let node = RaftNode::new(test_config());
        assert!(node.votes_received.is_empty());
        assert!(node.follower_progress.is_empty());
    }

    #[test]
    fn step_returns_empty_actions_placeholder() {
        let mut node = RaftNode::new(test_config());
        let actions = node.step(Input::Tick);
        assert!(actions.is_empty());
    }

    #[test]
    fn replication_state_fields() {
        let rs = ReplicationState {
            next_index: LogIndex(5),
            match_index: LogIndex(3),
        };
        assert_eq!(rs.next_index, LogIndex(5));
        assert_eq!(rs.match_index, LogIndex(3));
    }
}
