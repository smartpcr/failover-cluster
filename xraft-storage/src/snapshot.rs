//! In-memory snapshot store for testing and prototyping.
//!
//! Implements `xraft_core::storage::SnapshotStore` backed by a `Vec`.
//! Not suitable for production — snapshots are lost on restart.

use xraft_core::error::Result;
use xraft_core::storage::{SnapshotMeta, SnapshotStore};

/// In-memory snapshot store backed by a `Vec` of `(meta, data)` pairs.
#[derive(Debug, Default)]
pub struct MemorySnapshotStore {
    snapshots: Vec<(SnapshotMeta, Vec<u8>)>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> Result<()> {
        self.snapshots.push((metadata, data.to_vec()));
        Ok(())
    }

    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>> {
        Ok(self
            .snapshots
            .iter()
            .max_by_key(|(m, _)| m.last_included_index)
            .cloned())
    }

    fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>> {
        let mut metas: Vec<SnapshotMeta> =
            self.snapshots.iter().map(|(m, _)| m.clone()).collect();
        metas.sort_by(|a, b| b.last_included_index.cmp(&a.last_included_index));
        Ok(metas)
    }

    fn delete_snapshot(&mut self, id: &str) -> Result<()> {
        self.snapshots.retain(|(m, _)| m.id != id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::{LogIndex, Term};

    fn test_meta(id: &str, index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(index),
            last_included_term: Term(term),
            id: id.to_string(),
            voter_set: None,
        }
    }

    #[test]
    fn empty_store_returns_none() {
        let store = MemorySnapshotStore::new();
        assert!(store.load_latest_snapshot().unwrap().is_none());
        assert!(store.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn save_and_load() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"state-data")
            .unwrap();
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.id, "snap-1");
        assert_eq!(meta.last_included_index, LogIndex(10));
        assert_eq!(data, b"state-data");
    }

    #[test]
    fn latest_is_last_saved() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"v1")
            .unwrap();
        store
            .save_snapshot(test_meta("snap-2", 20, 3), b"v2")
            .unwrap();
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.id, "snap-2");
        assert_eq!(data, b"v2");
    }

    #[test]
    fn latest_selects_by_index_not_insertion_order() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("snap-2", 20, 3), b"v2")
            .unwrap();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"v1")
            .unwrap();
        let (meta, _) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.id, "snap-2");
        assert_eq!(meta.last_included_index, LogIndex(20));
    }

    #[test]
    fn list_newest_first() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store.save_snapshot(test_meta("c", 3, 2), b"").unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].id, "c");
        assert_eq!(list[2].id, "a");
    }

    #[test]
    fn list_sorts_by_index_not_insertion_order() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("c", 3, 2), b"").unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list[0].id, "c");
        assert_eq!(list[1].id, "b");
        assert_eq!(list[2].id, "a");
    }

    #[test]
    fn delete_snapshot() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store.delete_snapshot("a").unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "b");
    }
}
