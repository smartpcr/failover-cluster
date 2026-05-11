//! In-memory log store for testing and prototyping.
//!
//! Implements `xraft_core::storage::LogStore` backed by a `Vec<Entry>`.
//! Not suitable for production — entries are lost on restart.

use xraft_core::error::Result;
use xraft_core::message::Entry;
use xraft_core::storage::LogStore;
use xraft_core::types::{LogIndex, Term};

/// In-memory log store backed by a simple `Vec`.
#[derive(Debug, Default)]
pub struct MemoryLogStore {
    entries: Vec<Entry>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStore for MemoryLogStore {
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn get(&self, index: LogIndex) -> Result<Option<Entry>> {
        if index.0 == 0 {
            return Ok(None);
        }
        Ok(self.entries.iter().find(|e| e.index == index).cloned())
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.index >= start && e.index < end)
            .cloned()
            .collect())
    }

    fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(LogIndex(0), |e| e.index)
    }

    fn last_term(&self) -> Term {
        self.entries.last().map_or(Term(0), |e| e.term)
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
        self.entries.retain(|e| e.index < index);
        Ok(())
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>> {
        Ok(self.entries.iter().find(|e| e.index == index).map(|e| e.term))
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::message::{Entry, EntryPayload};

    fn make_entry(index: u64, term: u64) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::NoOp,
        }
    }

    #[test]
    fn empty_log_defaults() {
        let log = MemoryLogStore::new();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert!(log.get(LogIndex(1)).unwrap().is_none());
    }

    #[test]
    fn append_and_get() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 1), make_entry(2, 1)]).unwrap();
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.last_term(), Term(1));

        let entry = log.get(LogIndex(1)).unwrap().unwrap();
        assert_eq!(entry.index, LogIndex(1));
        assert_eq!(entry.term, Term(1));
    }

    #[test]
    fn get_range() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();
        let range = log.get_range(LogIndex(1), LogIndex(3)).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].index, LogIndex(1));
        assert_eq!(range[1].index, LogIndex(2));
    }

    #[test]
    fn truncate_from() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();
        log.truncate_from(LogIndex(2)).unwrap();
        assert_eq!(log.last_index(), LogIndex(1));
        assert!(log.get(LogIndex(2)).unwrap().is_none());
    }

    #[test]
    fn term_at() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 5)]).unwrap();
        assert_eq!(log.term_at(LogIndex(1)).unwrap(), Some(Term(5)));
        assert_eq!(log.term_at(LogIndex(2)).unwrap(), None);
    }

    #[test]
    fn get_index_zero_returns_none() {
        let log = MemoryLogStore::new();
        assert!(log.get(LogIndex(0)).unwrap().is_none());
    }

    #[test]
    fn flush_is_noop() {
        let mut log = MemoryLogStore::new();
        assert!(log.flush().is_ok());
    }
}
