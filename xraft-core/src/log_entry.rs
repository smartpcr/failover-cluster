//! Raft log entry definitions used by the xraft core.
//!
//! Each committed entry in the replicated log carries a term, an index, a
//! payload, and an [`EntryType`] tag that tells the state machine and the
//! cluster-management layer how the payload should be interpreted.

use serde::{Deserialize, Serialize};

/// Classifies a single entry in the Raft log.
///
/// The variant determines how the entry is applied:
///
/// * [`EntryType::NoOp`] — committed by a newly elected leader to advance the
///   commit index of its term without altering state machine state.
/// * [`EntryType::Command`] — an opaque client command that is forwarded to
///   the state machine when committed.
/// * [`EntryType::Configuration`] — a cluster membership change that updates
///   the active voter set as part of joint-consensus reconfiguration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntryType {
    /// No-op entry written at the start of a new leader's term.
    NoOp,
    /// Client command to be applied to the state machine on commit.
    Command,
    /// Cluster membership / configuration change.
    Configuration,
}

/// A single entry in the Raft replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Leader term in which this entry was created.
    pub term: u64,
    /// Monotonically increasing position of this entry in the log.
    pub index: u64,
    /// Classification tag for this entry.
    pub entry_type: EntryType,
    /// Opaque payload bytes; interpretation depends on `entry_type`.
    pub data: Vec<u8>,
}

impl LogEntry {
    /// Construct a new log entry.
    pub fn new(term: u64, index: u64, entry_type: EntryType, data: Vec<u8>) -> Self {
        Self {
            term,
            index,
            entry_type,
            data,
        }
    }

    /// Returns `true` if this entry represents a configuration change.
    pub fn is_configuration(&self) -> bool {
        matches!(self.entry_type, EntryType::Configuration)
    }

    /// Returns `true` if this entry is a leader-elected no-op marker.
    pub fn is_noop(&self) -> bool {
        matches!(self.entry_type, EntryType::NoOp)
    }
}
