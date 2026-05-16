use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// Type of log entry — distinguishes application commands from control records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command.
    Command,
    /// Leader no-op appended at the start of a new term.
    LeaderChangeMessage,
    /// Membership change — contains a complete `VotersRecord`.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-based).
    pub offset: Offset,
    /// Term in which the entry was created.
    pub term: Term,
    /// Type of this entry.
    pub entry_type: EntryType,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

impl LogEntry {
    /// Create a command log entry.
    pub fn command(offset: Offset, term: Term, payload: Vec<u8>) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::Command,
            payload,
        }
    }

    /// Create a leader change message entry.
    pub fn leader_change(offset: Offset, term: Term) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Vec::new(),
        }
    }
}
