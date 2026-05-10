//! Storage trait definitions.
//!
//! Traits live in `xraft-core` to keep the consensus engine I/O-free.
//! Concrete implementations are in `xraft-storage`.

use crate::error::Result;
use crate::message::Entry;
use crate::types::{LogIndex, Term};

/// Durable, append-only log storage.
pub trait LogStore: Send + Sync {
    /// Append entries to the log.
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    /// Retrieve the entry at the given index.
    fn get(&self, index: LogIndex) -> Result<Option<Entry>>;
    /// Retrieve entries in the half-open range `[start, end)`.
    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>>;
    /// The index of the last entry, or 0 if empty.
    fn last_index(&self) -> LogIndex;
    /// The term of the last entry, or Term(0) if empty.
    fn last_term(&self) -> Term;
    /// Remove all entries from `index` onward (inclusive).
    fn truncate_from(&mut self, index: LogIndex) -> Result<()>;
    /// The term of the entry at the given index, if it exists.
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>>;
    /// Flush buffered writes to durable storage.
    fn flush(&mut self) -> Result<()>;
}

/// Persistent hard state (term + vote).
pub trait HardStateStore: Send + Sync {
    /// Persist the hard state to durable storage.
    fn persist(&mut self, state: &HardState) -> Result<()>;
    /// Load the most recently persisted hard state.
    fn load(&self) -> Result<Option<HardState>>;
}

/// Durable snapshot storage.
pub trait SnapshotStore: Send + Sync {
    /// Save a snapshot with the given metadata and data.
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> Result<()>;
    /// Load the most recent snapshot.
    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>>;
    /// List available snapshots, newest first.
    fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>>;
    /// Delete a specific snapshot.
    fn delete_snapshot(&mut self, id: &str) -> Result<()>;
}

/// Safety-critical voting state persisted before any RPC reply.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<crate::types::NodeId>,
}

/// Metadata associated with a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub id: String,
}
