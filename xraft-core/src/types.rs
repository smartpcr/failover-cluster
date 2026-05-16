//! Core scalar identifier types for the xraft consensus engine.
//!
//! Each public type in this module is defined exactly once. Higher-level
//! types live in their own modules:
//!
//! * `Role` — see [`crate::consensus_state`]
//! * `VoterInfo` / `VotersRecord` — see [`crate::voter`]
//! * `LogEntry` / `EntryType` — see [`crate::log_entry`]
//!
//! Re-exports of the canonical types defined here are wired up in
//! [`crate`] via `pub use types::{ClusterId, NodeId, Offset, Term};`.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Monotonically-increasing logical clock that partitions the cluster's
/// history into a series of leaderships (Raft §5.1).
///
/// A new term is started whenever a node initiates an election. Terms are
/// compared with the natural ordering on the inner `u64`.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Term(pub u64);

impl Term {
    /// Term zero — the value used before any leader has been elected.
    pub const ZERO: Term = Term(0);

    /// Return the next term (`self + 1`).
    #[inline]
    pub fn next(self) -> Term {
        Term(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a node (voter or observer) in the cluster.
///
/// Backed by a `u64` so it can be cheaply copied and used as a map key.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// 0-based byte/record offset into a replicated log.
///
/// Kafka/KRaft-style log identifier used for fetch positions, commit
/// points and high-watermark tracking.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Offset(pub u64);

impl Offset {
    /// Offset zero — the first valid log position.
    pub const ZERO: Offset = Offset(0);
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// KRaft-style globally unique identifier for a Raft cluster.
///
/// Backed by a UUID (v4) so two independently bootstrapped clusters can
/// never collide. The default value is the nil UUID, which is used as a
/// sentinel meaning "not yet bootstrapped".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    /// Generate a fresh, random `ClusterId` (UUID v4).
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Nil cluster id (`00000000-0000-0000-0000-000000000000`).
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }
}

impl Default for ClusterId {
    fn default() -> Self {
        Self::nil()
    }
}

impl PartialOrd for ClusterId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ClusterId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_next_increments() {
        assert_eq!(Term(0).next(), Term(1));
        assert_eq!(Term(41).next(), Term(42));
    }

    #[test]
    fn term_ordering_uses_inner_u64() {
        assert!(Term(1) < Term(2));
        assert_eq!(Term::ZERO, Term(0));
    }

    #[test]
    fn node_id_display_is_prefixed() {
        assert_eq!(NodeId(7).to_string(), "node-7");
    }

    #[test]
    fn offset_default_is_zero() {
        assert_eq!(Offset::default(), Offset::ZERO);
        assert_eq!(Offset::ZERO.to_string(), "0");
    }

    #[test]
    fn cluster_id_default_is_nil() {
        assert_eq!(ClusterId::default(), ClusterId::nil());
        assert_eq!(
            ClusterId::nil().to_string(),
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn cluster_id_random_is_unique() {
        let a = ClusterId::new_random();
        let b = ClusterId::new_random();
        assert_ne!(a, b);
    }

    #[test]
    fn cluster_id_total_order() {
        let a = ClusterId(Uuid::from_u128(1));
        let b = ClusterId(Uuid::from_u128(2));
        assert!(a < b);
    }
}
