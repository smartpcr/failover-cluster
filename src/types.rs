//! Core scalar types used throughout the Raft node state machine.
//!
//! These are intentionally newtypes over primitives so the type system
//! prevents mixing, for example, a [`Term`] with a [`LogIndex`].

use core::fmt;

/// Identifier of a Raft node (voter or observer).
///
/// In Apache Kafka's `KRaft` this corresponds to the controller's
/// `node.id`. Within this crate it is an opaque, totally-ordered
/// identifier — equality and ordering are by the contained `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Construct a `NodeId` from its raw `u64`.
    #[inline]
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Return the inner `u64`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl From<u64> for NodeId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// Raft term (a.k.a. epoch in `KRaft`).
///
/// Terms act as a logical clock: they increase monotonically and are
/// used to detect stale leaders and candidates. Term `0` is the
/// "pre-history" term that nodes start in before any election has
/// taken place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Term(pub u64);

impl Term {
    /// The initial term used by a freshly-initialised node.
    pub const ZERO: Term = Term(0);

    /// Construct a `Term` from its raw `u64`.
    #[inline]
    #[must_use]
    pub const fn new(t: u64) -> Self {
        Self(t)
    }

    /// Return the inner `u64`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Return the next term (`self + 1`). Saturates at `u64::MAX`.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "term-{}", self.0)
    }
}

impl From<u64> for Term {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// Position of an entry in the replicated log (1-based; `0` means
/// "no entry").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// Sentinel meaning "before any entry has been appended".
    pub const ZERO: LogIndex = LogIndex(0);

    /// Construct a `LogIndex` from its raw `u64`.
    #[inline]
    #[must_use]
    pub const fn new(i: u64) -> Self {
        Self(i)
    }

    /// Return the inner `u64`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Index immediately after `self` (`self + 1`), saturating.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "idx-{}", self.0)
    }
}

impl From<u64> for LogIndex {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// Metadata about a node's local log, cached inside the state machine
/// purely so that vote handling can perform the up-to-date check from
/// the Raft paper (§5.4.1).
///
/// This struct is **not** part of `PersistentState`: the log itself is
/// owned by a separate stage. The host is expected to keep this cache
/// fresh by emitting
/// [`Event::LogTailUpdated`](crate::Event::LogTailUpdated) whenever
/// the local log's tail moves — including after a truncation, where
/// `last_term` can legitimately move backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, PartialOrd, Ord)]
pub struct LogMetadata {
    /// Index of the last entry present locally.
    pub last_index: LogIndex,
    /// Term of the entry at `last_index`.
    pub last_term: Term,
}

impl LogMetadata {
    /// Empty log metadata: index = 0, term = 0.
    pub const EMPTY: LogMetadata = LogMetadata {
        last_index: LogIndex::ZERO,
        last_term: Term::ZERO,
    };

    /// True when the log described by `candidate` is at least as
    /// up-to-date as `self`, per Raft §5.4.1:
    ///
    /// > If the logs have last entries with different terms, then the
    /// > log with the later term is more up-to-date. If the logs end
    /// > with the same term, then whichever log is longer is more
    /// > up-to-date.
    #[must_use]
    pub fn is_at_least_as_up_to_date_as(self, candidate: LogMetadata) -> bool {
        match candidate.last_term.cmp(&self.last_term) {
            core::cmp::Ordering::Greater => true,
            core::cmp::Ordering::Less => false,
            core::cmp::Ordering::Equal => candidate.last_index >= self.last_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_next_increments() {
        assert_eq!(Term::new(0).next(), Term::new(1));
        assert_eq!(Term::new(42).next(), Term::new(43));
    }

    #[test]
    fn term_next_saturates() {
        assert_eq!(Term::new(u64::MAX).next(), Term::new(u64::MAX));
    }

    #[test]
    fn log_index_ordering() {
        assert!(LogIndex::new(1) < LogIndex::new(2));
        assert_eq!(LogIndex::ZERO, LogIndex::new(0));
    }

    #[test]
    fn up_to_date_check_higher_term_wins() {
        let ours = LogMetadata {
            last_index: LogIndex::new(100),
            last_term: Term::new(1),
        };
        let theirs = LogMetadata {
            last_index: LogIndex::new(1),
            last_term: Term::new(2),
        };
        assert!(ours.is_at_least_as_up_to_date_as(theirs));
        assert!(!theirs.is_at_least_as_up_to_date_as(ours));
    }

    #[test]
    fn up_to_date_check_same_term_longer_log_wins() {
        let short = LogMetadata {
            last_index: LogIndex::new(5),
            last_term: Term::new(3),
        };
        let long = LogMetadata {
            last_index: LogIndex::new(9),
            last_term: Term::new(3),
        };
        assert!(short.is_at_least_as_up_to_date_as(long));
        assert!(!long.is_at_least_as_up_to_date_as(short));
    }

    #[test]
    fn up_to_date_check_equal_is_acceptable() {
        let a = LogMetadata {
            last_index: LogIndex::new(7),
            last_term: Term::new(2),
        };
        let b = LogMetadata {
            last_index: LogIndex::new(7),
            last_term: Term::new(2),
        };
        assert!(a.is_at_least_as_up_to_date_as(b));
        assert!(b.is_at_least_as_up_to_date_as(a));
    }

    #[test]
    fn node_id_display() {
        assert_eq!(NodeId::new(3).to_string(), "node-3");
    }
}
