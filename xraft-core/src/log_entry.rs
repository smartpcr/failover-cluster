use serde::{Deserialize, Serialize};

/// Classification of a Raft log entry.
///
/// Used by the log compaction pipeline to decide which entries can be
/// discarded after a snapshot is installed and which must be preserved
/// (e.g. configuration changes that affect cluster membership).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntryType {
    /// A no-op entry committed by a newly elected leader to advance the
    /// commit index for entries from previous terms.
    NoOp,

    /// A normal state-machine command replicated to followers and applied
    /// once committed.
    Normal,

    /// A cluster membership change (joint consensus or single-server
    /// reconfiguration). Must be retained across compaction so the
    /// configuration history can be replayed.
    ConfigChange,

    /// A marker entry indicating that a snapshot was installed at this
    /// index. Entries at or below the snapshot's last-included index can
    /// be removed by the compaction pipeline.
    Snapshot,
}

/// A single Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Monotonically increasing log index assigned by the leader.
    pub index: u64,

    /// Term in which this entry was created by the leader.
    pub term: u64,

    /// Classification of this entry.
    pub kind: EntryType,

    /// Opaque payload — a serialized state-machine command, configuration
    /// change descriptor, or snapshot metadata, depending on `kind`.
    pub payload: Vec<u8>,
}

impl LogEntry {
    /// Construct a new log entry.
    pub fn new(index: u64, term: u64, kind: EntryType, payload: Vec<u8>) -> Self {
        Self { index, term, kind, payload }
    }

    /// Returns `true` if this entry may be discarded once a snapshot covers
    /// its index. Configuration changes and snapshot markers are retained.
    pub fn is_compactable(&self) -> bool {
        matches!(self.kind, EntryType::Normal | EntryType::NoOp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_and_noop_are_compactable() {
        assert!(LogEntry::new(1, 1, EntryType::Normal, vec![]).is_compactable());
        assert!(LogEntry::new(2, 1, EntryType::NoOp, vec![]).is_compactable());
    }

    #[test]
    fn config_change_and_snapshot_are_retained() {
        assert!(!LogEntry::new(3, 1, EntryType::ConfigChange, vec![]).is_compactable());
        assert!(!LogEntry::new(4, 1, EntryType::Snapshot, vec![]).is_compactable());
    }
}
