//! Persistent and volatile state of a Raft node, separated as in the
//! Raft paper Figure 2.

use crate::types::{LogIndex, LogMetadata, NodeId, Term};

/// Consensus state that **must** be persisted to stable storage before
/// any RPC response that depends on it is sent.
///
/// Following the Raft paper (§5.1 / §5.2), only these two fields are
/// truly part of "persistent state for safety". The log itself is
/// persistent too, but is owned by a separate stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistentState {
    /// Latest term the node has seen.
    pub current_term: Term,
    /// Candidate this node voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
}

impl PersistentState {
    /// Fresh persistent state for a node that has never run yet.
    pub const INITIAL: PersistentState = PersistentState {
        current_term: Term::ZERO,
        voted_for: None,
    };

    /// Adopt a higher term: bump `current_term` and clear `voted_for`.
    ///
    /// Returns `true` if any field actually changed.
    pub fn observe_term(&mut self, new_term: Term) -> bool {
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = None;
            true
        } else {
            false
        }
    }
}

/// Volatile state every node keeps. Lost on restart and reconstructed
/// from the log / leader heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VolatileState {
    /// Index of the highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of the highest log entry applied to the state machine.
    pub last_applied: LogIndex,
    /// Identifier of the current leader, if known.
    pub leader_id: Option<NodeId>,
}

impl VolatileState {
    /// Fresh volatile state.
    pub const INITIAL: VolatileState = VolatileState {
        commit_index: LogIndex::ZERO,
        last_applied: LogIndex::ZERO,
        leader_id: None,
    };

    /// Advance `commit_index` to `new_index`, refusing to move
    /// backwards. Returns whether any change occurred.
    pub fn advance_commit(&mut self, new_index: LogIndex) -> bool {
        if new_index > self.commit_index {
            self.commit_index = new_index;
            true
        } else {
            false
        }
    }
}

/// Wrapper around [`LogMetadata`] that the host updates whenever the
/// local log's tail moves.
///
/// **No monotonicity is enforced here.** Followers legitimately
/// truncate uncommitted suffixes when their log diverges from a new
/// leader's, after which the local `last_term` can move backwards
/// (e.g. uncommitted term-5 entries are overwritten by committed
/// term-4 entries from the leader). The authoritative invariant lives
/// in the log stage: the cache must mirror its tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogMetadataCache(LogMetadata);

impl LogMetadataCache {
    /// Empty cache (index = 0, term = 0).
    pub const EMPTY: LogMetadataCache = LogMetadataCache(LogMetadata::EMPTY);

    /// Borrow the underlying metadata.
    #[must_use]
    pub fn get(&self) -> LogMetadata {
        self.0
    }

    /// Overwrite the cache with the latest known log tail.
    ///
    /// Accepts any value because the host's log stage is the source
    /// of truth — including legitimate backward moves after log
    /// truncation.
    pub fn update(&mut self, next: LogMetadata) {
        self.0 = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_higher_term_clears_vote() {
        let mut ps = PersistentState {
            current_term: Term::new(3),
            voted_for: Some(NodeId::new(1)),
        };
        assert!(ps.observe_term(Term::new(5)));
        assert_eq!(ps.current_term, Term::new(5));
        assert_eq!(ps.voted_for, None);
    }

    #[test]
    fn observe_same_or_lower_term_is_noop() {
        let mut ps = PersistentState {
            current_term: Term::new(5),
            voted_for: Some(NodeId::new(1)),
        };
        assert!(!ps.observe_term(Term::new(5)));
        assert!(!ps.observe_term(Term::new(4)));
        assert_eq!(ps.current_term, Term::new(5));
        assert_eq!(ps.voted_for, Some(NodeId::new(1)));
    }

    #[test]
    fn commit_index_is_monotonic() {
        let mut vs = VolatileState::INITIAL;
        assert!(vs.advance_commit(LogIndex::new(3)));
        assert!(!vs.advance_commit(LogIndex::new(3)));
        assert!(!vs.advance_commit(LogIndex::new(2)));
        assert!(vs.advance_commit(LogIndex::new(4)));
        assert_eq!(vs.commit_index, LogIndex::new(4));
    }

    #[test]
    fn log_metadata_cache_accepts_monotonic_growth() {
        let mut cache = LogMetadataCache::EMPTY;
        cache.update(LogMetadata {
            last_index: LogIndex::new(1),
            last_term: Term::new(1),
        });
        cache.update(LogMetadata {
            last_index: LogIndex::new(2),
            last_term: Term::new(1),
        });
        cache.update(LogMetadata {
            last_index: LogIndex::new(2),
            last_term: Term::new(2),
        });
        assert_eq!(cache.get().last_index, LogIndex::new(2));
    }

    #[test]
    fn log_metadata_cache_accepts_truncation() {
        // Follower had uncommitted term-5 entries; new leader's log
        // forces truncation back to term-4 / shorter index.
        let mut cache = LogMetadataCache::EMPTY;
        cache.update(LogMetadata {
            last_index: LogIndex::new(10),
            last_term: Term::new(5),
        });
        cache.update(LogMetadata {
            last_index: LogIndex::new(8),
            last_term: Term::new(4),
        });
        assert_eq!(
            cache.get(),
            LogMetadata {
                last_index: LogIndex::new(8),
                last_term: Term::new(4)
            }
        );
    }
}
