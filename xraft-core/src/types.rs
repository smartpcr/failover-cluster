//! Core scalar / identifier types for the xraft crate.
//!
//! This module owns the four canonical types re-exported from `lib.rs`:
//! [`ClusterId`], [`NodeId`], [`Offset`], and [`Term`]. Every other concept
//! lives in its own module:
//!
//! * `Role` — `consensus_state` / `node_state`
//! * `VoterInfo` / `VotersRecord` / `Endpoint` — `voter`
//! * `LogEntry` / `EntryType` — `log_entry`
//!
//! Each type is defined exactly once here. Do not re-add `Role`, `VoterInfo`,
//! `Endpoint`, or any log-entry shape to this file — that was the bug the
//! reviewer flagged when several merge iterations were concatenated.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a node (broker) in the cluster.
///
/// Stable for the lifetime of the broker process. Used as the voting and
/// quorum-counting key in the Raft protocol.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

/// Raft term — monotonically increasing logical clock incremented at the
/// start of every election. Persisted before any RPC reply (safety
/// invariant from architecture §3.3).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct Term(pub u64);

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Term({})", self.0)
    }
}

/// 0-based offset into the replicated log (KRaft / Kafka convention).
///
/// `Offset(0)` is the offset of the first record in an empty-or-fresh log;
/// `log_end_offset` is the offset of the *next* record to be appended. Use
/// `Option<Offset>` to model the absence of an offset rather than a signed
/// sentinel.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct Offset(pub u64);

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Offset({})", self.0)
    }
}

/// UUID-based identity for the Raft cluster as a whole.
///
/// Carried in every RPC so a node can detect (and reject) traffic from a
/// different cluster that happens to reuse the same network endpoints —
/// the canonical KRaft "wrong cluster" safeguard.
///
/// Intentionally does **not** implement [`Default`]: every running cluster
/// has a real generated id, so silently materialising a nil-UUID identity
/// would mask bootstrap bugs. Use [`ClusterId::new_random`] at bootstrap,
/// or [`ClusterId::nil`] only as an explicit placeholder in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    /// Generate a fresh random `ClusterId` using UUID v4. Called once when
    /// a brand-new cluster is bootstrapped.
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Construct the nil-UUID `ClusterId`. Reserved for placeholder use in
    /// tests; never a valid production cluster identity.
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }
}

impl PartialOrd for ClusterId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ClusterId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClusterId({})", self.0.hyphenated())
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
    fn offset_default_is_zero() {
        assert_eq!(Offset::default(), Offset(0));
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
    fn offset_ordering() {
        assert!(Offset(0) < Offset(1));
        assert!(Offset(100) > Offset(99));
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
    fn display_offset() {
        assert_eq!(format!("{}", Offset(100)), "Offset(100)");
    }

    #[test]
    fn display_cluster_id() {
        let cid = ClusterId::nil();
        assert_eq!(
            format!("{cid}"),
            "ClusterId(00000000-0000-0000-0000-000000000000)"
        );
    }

    #[test]
    fn cluster_id_new_random_is_v4_and_not_nil() {
        let cid = ClusterId::new_random();
        assert!(!cid.0.is_nil());
        assert_eq!(cid.0.get_version_num(), 4);
    }

    #[test]
    fn cluster_id_nil_is_nil() {
        assert!(ClusterId::nil().0.is_nil());
    }

    #[test]
    fn cluster_id_uniqueness() {
        let a = ClusterId::new_random();
        let b = ClusterId::new_random();
        assert_ne!(a, b);
    }

    #[test]
    fn cluster_id_ord_is_total_and_byte_lex() {
        let lo = ClusterId(Uuid::from_bytes([0x00; 16]));
        let hi = ClusterId(Uuid::from_bytes([0xff; 16]));
        assert!(lo < hi);
        assert_eq!(lo.cmp(&lo), Ordering::Equal);
        assert_eq!(hi.partial_cmp(&lo), Some(Ordering::Greater));
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
    fn offset_serde_roundtrip() {
        let o = Offset(12345);
        let json = serde_json::to_string(&o).unwrap();
        let o2: Offset = serde_json::from_str(&json).unwrap();
        assert_eq!(o, o2);
    }

    #[test]
    fn cluster_id_serde_roundtrip() {
        let cid = ClusterId::new_random();
        let json = serde_json::to_string(&cid).unwrap();
        let cid2: ClusterId = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, cid2);
    }

    #[test]
    fn node_id_hashes_in_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(NodeId(1));
        set.insert(NodeId(2));
        set.insert(NodeId(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn cluster_id_hashes_in_set() {
        use std::collections::HashSet;
        let a = ClusterId::new_random();
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(a);
        assert_eq!(set.len(), 1);
    }
}
