//! Core Raft type definitions.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// KRaft-style directory identifier for voter disambiguation.
///
/// Backed by a UUID (v4) as specified in the architecture / implementation plan.
/// Default value is the nil UUID (`00000000-0000-0000-0000-000000000000`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DirectoryId(pub Uuid);

impl DirectoryId {
    /// Generate a new random `DirectoryId` using UUID v4.
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a `DirectoryId` from the nil UUID.
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }
}

impl Default for DirectoryId {
    fn default() -> Self {
        Self::nil()
    }
}

impl PartialOrd for DirectoryId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DirectoryId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

/// Role a node can be in within the Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum NodeRole {
    /// Active leader serving reads and writes.
    Leader,
    /// Passive follower replicating from the leader.
    #[default]
    Follower,
    /// Pre-candidate: checking quorum reachability before starting a real election.
    PreCandidate,
    /// Candidate: running an election with an incremented term.
    Candidate,
    /// Non-voting observer replicating the log for read scaling or standby.
    Observer,
}

/// Safety-critical voting state persisted before any RPC reply.
///
/// Only `current_term` and `voted_for` are persisted; `commit_index` and
/// `last_applied` are volatile and rebuilt from the log on recovery
/// (per `architecture.md` §3.3 / `implementation-plan.md` Stage 1.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

/// A network endpoint for reaching a node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Format as `host:port`.
    pub fn to_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// A single voter (or observer) record in the quorum configuration.
///
/// Mirrors KRaft's `VoterRecord`: a unique `(NodeId, DirectoryId)` plus
/// reachable endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterRecord {
    pub node_id: NodeId,
    pub directory_id: DirectoryId,
    pub endpoints: Vec<Endpoint>,
}

/// The set of voters forming the quorum configuration.
///
/// In KRaft, the voter key is `(NodeId, DirectoryId)` — a single physical node
/// may host multiple log directories. Each `(NodeId, DirectoryId)` pair must be
/// unique within the set, but **quorum is computed over unique `NodeId`s**, not
/// `(NodeId, DirectoryId)` pairs. This matches KRaft semantics where quorum is
/// over broker nodes: a single broker with multiple directories still counts as
/// one vote for quorum purposes.
///
/// A voter set must contain at least one voter and all `(NodeId, DirectoryId)`
/// pairs must be unique. Static for v1 — defined at bootstrap and immutable
/// for the cluster lifetime. Dynamic membership is deferred to a future story.
///
/// The `voters` field is private to enforce invariants established by
/// [`VoterSet::try_new`]. Use [`VoterSet::voters`] for read access and
/// [`VoterSet::into_voters`] for consuming access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VoterSet {
    voters: Vec<VoterRecord>,
}

/// Errors that occur when constructing an invalid `VoterSet`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoterSetError {
    /// The voter set must contain at least one voter.
    Empty,
    /// Duplicate `(NodeId, DirectoryId)` pair found.
    DuplicateVoter {
        node_id: NodeId,
        directory_id: DirectoryId,
    },
    /// A voter record has no endpoints.
    MissingEndpoints { node_id: NodeId },
    /// A voter record has a nil DirectoryId.
    NilDirectoryId { node_id: NodeId },
}

impl std::fmt::Display for VoterSetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VoterSetError::Empty => write!(f, "voter set must contain at least one voter"),
            VoterSetError::DuplicateVoter {
                node_id,
                directory_id,
            } => {
                write!(
                    f,
                    "duplicate voter: node_id={node_id}, directory_id={directory_id}"
                )
            }
            VoterSetError::MissingEndpoints { node_id } => {
                write!(f, "voter {node_id} has no endpoints")
            }
            VoterSetError::NilDirectoryId { node_id } => {
                write!(f, "voter {node_id} has nil directory_id")
            }
        }
    }
}

impl std::error::Error for VoterSetError {}

impl VoterSet {
    /// Validate that a slice of voters satisfies all VoterSet invariants.
    fn validate(voters: &[VoterRecord]) -> std::result::Result<(), VoterSetError> {
        if voters.is_empty() {
            return Err(VoterSetError::Empty);
        }
        let mut seen = std::collections::HashSet::new();
        for v in voters {
            if v.directory_id.0.is_nil() {
                return Err(VoterSetError::NilDirectoryId { node_id: v.node_id });
            }
            if v.endpoints.is_empty() {
                return Err(VoterSetError::MissingEndpoints { node_id: v.node_id });
            }
            if !seen.insert((v.node_id, v.directory_id)) {
                return Err(VoterSetError::DuplicateVoter {
                    node_id: v.node_id,
                    directory_id: v.directory_id,
                });
            }
        }
        Ok(())
    }

    /// Create a new `VoterSet`, validating that:
    /// - The set is non-empty.
    /// - No duplicate `(NodeId, DirectoryId)` pairs exist.
    /// - Every voter has a non-nil `DirectoryId`.
    /// - Every voter has at least one endpoint.
    pub fn try_new(voters: Vec<VoterRecord>) -> std::result::Result<Self, VoterSetError> {
        Self::validate(&voters)?;
        Ok(Self { voters })
    }

    /// Borrow the voter records.
    pub fn voters(&self) -> &[VoterRecord] {
        &self.voters
    }

    /// Consume the `VoterSet` and return the underlying `Vec<VoterRecord>`.
    pub fn into_voters(self) -> Vec<VoterRecord> {
        self.voters
    }

    /// Number of voters in the set.
    pub fn len(&self) -> usize {
        self.voters.len()
    }

    /// Whether the voter set is empty — always false for a validly constructed set.
    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    /// Check if a given `NodeId` is a voter.
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.voters.iter().any(|v| v.node_id == node_id)
    }

    /// Compute the quorum size (majority) for this voter set.
    ///
    /// Quorum is based on the number of unique `NodeId`s, not the total
    /// number of `(NodeId, DirectoryId)` pairs. A single physical node with
    /// multiple directories counts once for quorum purposes — matching
    /// KRaft semantics where quorum is over brokers, not log directories.
    pub fn quorum_size(&self) -> usize {
        let unique_nodes: std::collections::HashSet<NodeId> =
            self.voters.iter().map(|v| v.node_id).collect();
        unique_nodes.len() / 2 + 1
    }

    /// Count unique `NodeId`s in this voter set.
    ///
    /// Useful for physical-node-level reasoning (e.g., how many distinct machines
    /// are in the cluster), as opposed to `len()` which counts logical voter
    /// records (potentially multiple per physical node in KRaft).
    pub fn unique_node_count(&self) -> usize {
        let nodes: std::collections::HashSet<NodeId> =
            self.voters.iter().map(|v| v.node_id).collect();
        nodes.len()
    }
}

/// Custom `Deserialize` for `VoterSet` that enforces validation on
/// deserialized data, preventing construction of invalid voter sets
/// from untrusted input (network, config files, snapshots).
impl<'de> Deserialize<'de> for VoterSet {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct VoterSetRaw {
            voters: Vec<VoterRecord>,
        }

        let raw = VoterSetRaw::deserialize(deserializer)?;
        VoterSet::try_new(raw.voters).map_err(serde::de::Error::custom)
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

impl std::fmt::Display for DirectoryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectoryId({})", self.0.hyphenated())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_default_is_zero() {
        assert_eq!(NodeId::default(), NodeId(0));
    }

    #[test]
    fn term_default_is_zero() {
        assert_eq!(Term::default(), Term(0));
    }

    #[test]
    fn log_index_default_is_zero() {
        assert_eq!(LogIndex::default(), LogIndex(0));
    }

    #[test]
    fn directory_id_default_is_nil_uuid() {
        assert_eq!(DirectoryId::default(), DirectoryId(Uuid::nil()));
        assert!(DirectoryId::default().0.is_nil());
    }

    #[test]
    fn directory_id_new_generates_v4() {
        let id = DirectoryId::new_random();
        assert!(!id.0.is_nil());
        assert_eq!(id.0.get_version_num(), 4);
    }

    #[test]
    fn directory_id_uuid_uniqueness() {
        let a = DirectoryId::new_random();
        let b = DirectoryId::new_random();
        assert_ne!(a, b);
    }

    #[test]
    fn node_role_default_is_follower() {
        assert_eq!(NodeRole::default(), NodeRole::Follower);
    }

    #[test]
    fn node_id_ordering() {
        assert!(NodeId(1) < NodeId(2));
        assert!(NodeId(5) > NodeId(3));
        assert_eq!(NodeId(7), NodeId(7));
    }

    #[test]
    fn term_ordering() {
        assert!(Term(1) < Term(2));
        assert!(Term(10) > Term(9));
    }

    #[test]
    fn log_index_ordering() {
        assert!(LogIndex(1) < LogIndex(2));
    }

    #[test]
    fn display_node_id() {
        assert_eq!(format!("{}", NodeId(42)), "NodeId(42)");
    }

    #[test]
    fn display_term() {
        assert_eq!(format!("{}", Term(5)), "Term(5)");
    }

    #[test]
    fn display_log_index() {
        assert_eq!(format!("{}", LogIndex(100)), "LogIndex(100)");
    }

    #[test]
    fn display_directory_id() {
        let id = DirectoryId(Uuid::nil());
        let display = format!("{id}");
        assert!(display.starts_with("DirectoryId("));
        assert!(display.contains("00000000-0000-0000-0000-000000000000"));
    }

    #[test]
    fn directory_id_serde_roundtrip() {
        let id = DirectoryId::new_random();
        let json = serde_json::to_string(&id).unwrap();
        let id2: DirectoryId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn directory_id_from_known_uuid() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = DirectoryId(uuid);
        assert_eq!(id.0.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn node_id_hash_in_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(NodeId(1));
        set.insert(NodeId(2));
        set.insert(NodeId(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn node_role_all_variants() {
        let roles = [
            NodeRole::Leader,
            NodeRole::Follower,
            NodeRole::PreCandidate,
            NodeRole::Candidate,
            NodeRole::Observer,
        ];
        assert_eq!(roles.len(), 5);
        // All variants are distinct.
        for (i, a) in roles.iter().enumerate() {
            for (j, b) in roles.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn hard_state_fields() {
        let hs = HardState {
            current_term: Term(5),
            voted_for: Some(NodeId(3)),
        };
        assert_eq!(hs.current_term, Term(5));
        assert_eq!(hs.voted_for, Some(NodeId(3)));
    }

    #[test]
    fn hard_state_no_vote() {
        let hs = HardState {
            current_term: Term(1),
            voted_for: None,
        };
        assert!(hs.voted_for.is_none());
    }

    #[test]
    fn hard_state_serde_roundtrip() {
        let hs = HardState {
            current_term: Term(10),
            voted_for: Some(NodeId(2)),
        };
        let json = serde_json::to_string(&hs).unwrap();
        let hs2: HardState = serde_json::from_str(&json).unwrap();
        assert_eq!(hs, hs2);
    }

    #[test]
    fn node_id_serde_roundtrip() {
        let id = NodeId(42);
        let json = serde_json::to_string(&id).unwrap();
        let id2: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn term_serde_roundtrip() {
        let t = Term(99);
        let json = serde_json::to_string(&t).unwrap();
        let t2: Term = serde_json::from_str(&json).unwrap();
        assert_eq!(t, t2);
    }

    #[test]
    fn node_role_serde_roundtrip() {
        for role in [
            NodeRole::Leader,
            NodeRole::Follower,
            NodeRole::PreCandidate,
            NodeRole::Candidate,
            NodeRole::Observer,
        ] {
            let json = serde_json::to_string(&role).unwrap();
            let role2: NodeRole = serde_json::from_str(&json).unwrap();
            assert_eq!(role, role2);
        }
    }

    #[test]
    fn node_id_copy_semantics() {
        let a = NodeId(1);
        let b = a; // Copy
        assert_eq!(a, b); // `a` is still usable
    }

    #[test]
    fn endpoint_display() {
        let ep = Endpoint::new("10.0.0.1", 6000);
        assert_eq!(format!("{ep}"), "10.0.0.1:6000");
        assert_eq!(ep.to_address(), "10.0.0.1:6000");
    }

    #[test]
    fn endpoint_serde_roundtrip() {
        let ep = Endpoint::new("host1", 7000);
        let json = serde_json::to_string(&ep).unwrap();
        let ep2: Endpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(ep, ep2);
    }

    #[test]
    fn voter_record_fields() {
        let vr = VoterRecord {
            node_id: NodeId(1),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("host1", 6000)],
        };
        assert_eq!(vr.node_id, NodeId(1));
        assert_eq!(vr.endpoints.len(), 1);
    }

    #[test]
    fn voter_set_quorum_size() {
        let make_voter = |id: u64| VoterRecord {
            node_id: NodeId(id),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("host", 6000)],
        };
        let vs3 = VoterSet::try_new(vec![make_voter(1), make_voter(2), make_voter(3)]).unwrap();
        assert_eq!(vs3.quorum_size(), 2);
        assert_eq!(vs3.len(), 3);
        assert!(!vs3.is_empty());

        let vs5 = VoterSet::try_new(vec![
            make_voter(1),
            make_voter(2),
            make_voter(3),
            make_voter(4),
            make_voter(5),
        ])
        .unwrap();
        assert_eq!(vs5.quorum_size(), 3);
    }

    #[test]
    fn voter_set_contains() {
        let vs = VoterSet::try_new(vec![
            VoterRecord {
                node_id: NodeId(1),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("host1", 6000)],
            },
            VoterRecord {
                node_id: NodeId(2),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("host2", 6000)],
            },
        ])
        .unwrap();
        assert!(vs.contains(NodeId(1)));
        assert!(vs.contains(NodeId(2)));
        assert!(!vs.contains(NodeId(3)));
    }

    #[test]
    fn voter_set_rejects_empty() {
        let result = VoterSet::try_new(vec![]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err, VoterSetError::Empty);
        assert!(err.to_string().contains("at least one voter"));
    }

    #[test]
    fn voter_set_rejects_duplicate_node_directory() {
        let dir = DirectoryId::new_random();
        let result = VoterSet::try_new(vec![
            VoterRecord {
                node_id: NodeId(1),
                directory_id: dir,
                endpoints: vec![Endpoint::new("host1", 6000)],
            },
            VoterRecord {
                node_id: NodeId(1),
                directory_id: dir,
                endpoints: vec![Endpoint::new("host2", 6001)],
            },
        ]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, VoterSetError::DuplicateVoter { .. }));
    }

    #[test]
    fn voter_set_allows_same_node_different_directory() {
        // KRaft design: same physical node can host multiple log directories.
        // The voter key is (NodeId, DirectoryId), not just NodeId.
        // However, quorum is computed over unique NodeIds to prevent a single
        // machine from inflating quorum.
        let vs = VoterSet::try_new(vec![
            VoterRecord {
                node_id: NodeId(1),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("host1", 6000)],
            },
            VoterRecord {
                node_id: NodeId(1),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("host2", 6001)],
            },
        ]);
        assert!(vs.is_ok());
        let vs = vs.unwrap();
        assert_eq!(vs.len(), 2);
        assert_eq!(vs.unique_node_count(), 1);
        // Quorum is based on unique nodes (1), not total records (2)
        assert_eq!(vs.quorum_size(), 1);
    }

    #[test]
    fn voter_set_rejects_nil_directory_id() {
        let result = VoterSet::try_new(vec![VoterRecord {
            node_id: NodeId(1),
            directory_id: DirectoryId::nil(),
            endpoints: vec![Endpoint::new("host1", 6000)],
        }]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VoterSetError::NilDirectoryId { .. }
        ));
    }

    #[test]
    fn voter_set_rejects_missing_endpoints() {
        let result = VoterSet::try_new(vec![VoterRecord {
            node_id: NodeId(1),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![],
        }]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VoterSetError::MissingEndpoints { .. }
        ));
    }

    #[test]
    fn voter_set_serde_roundtrip() {
        let vs = VoterSet::try_new(vec![VoterRecord {
            node_id: NodeId(1),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("host1", 6000), Endpoint::new("host1", 6001)],
        }])
        .unwrap();
        let json = serde_json::to_string(&vs).unwrap();
        let vs2: VoterSet = serde_json::from_str(&json).unwrap();
        assert_eq!(vs, vs2);
    }

    #[test]
    fn voter_set_deserialize_rejects_empty() {
        let json = r#"{"voters":[]}"#;
        let result: Result<VoterSet, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("at least one voter"),
            "expected validation error, got: {err}"
        );
    }

    #[test]
    fn voter_set_deserialize_rejects_nil_directory_id() {
        let json = r#"{"voters":[{"node_id":1,"directory_id":"00000000-0000-0000-0000-000000000000","endpoints":[{"host":"h","port":6000}]}]}"#;
        let result: Result<VoterSet, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nil directory_id"),
            "expected validation error, got: {err}"
        );
    }

    #[test]
    fn voter_set_deserialize_rejects_missing_endpoints() {
        let json = r#"{"voters":[{"node_id":1,"directory_id":"550e8400-e29b-41d4-a716-446655440000","endpoints":[]}]}"#;
        let result: Result<VoterSet, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no endpoints"),
            "expected validation error, got: {err}"
        );
    }

    #[test]
    fn voter_set_voters_accessor() {
        let make_voter = |id: u64| VoterRecord {
            node_id: NodeId(id),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("host", 6000)],
        };
        let vs = VoterSet::try_new(vec![make_voter(1), make_voter(2)]).unwrap();
        let slice = vs.voters();
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0].node_id, NodeId(1));
        assert_eq!(slice[1].node_id, NodeId(2));
    }

    #[test]
    fn voter_set_into_voters() {
        let make_voter = |id: u64| VoterRecord {
            node_id: NodeId(id),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("host", 6000)],
        };
        let vs = VoterSet::try_new(vec![make_voter(1), make_voter(2)]).unwrap();
        let vec = vs.into_voters();
        assert_eq!(vec.len(), 2);
        assert_eq!(vec[0].node_id, NodeId(1));
    }

    #[test]
    fn voter_set_error_display() {
        let e = VoterSetError::Empty;
        assert!(e.to_string().contains("at least one"));

        let e = VoterSetError::DuplicateVoter {
            node_id: NodeId(1),
            directory_id: DirectoryId::nil(),
        };
        assert!(e.to_string().contains("duplicate voter"));

        let e = VoterSetError::MissingEndpoints { node_id: NodeId(2) };
        assert!(e.to_string().contains("no endpoints"));

        let e = VoterSetError::NilDirectoryId { node_id: NodeId(3) };
        assert!(e.to_string().contains("nil directory_id"));
    }
}
