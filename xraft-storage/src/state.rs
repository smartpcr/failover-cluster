//! In-memory hard-state persistence for testing and prototyping.
//!
//! Implements `xraft_core::storage::HardStateStore` backed by an `Option`.
//! Not suitable for production — state is lost on restart.

use xraft_core::error::Result;
use xraft_core::storage::{HardState, HardStateStore};

/// In-memory hard-state store backed by a simple `Option`.
#[derive(Debug, Default)]
pub struct MemoryHardStateStore {
    state: Option<HardState>,
}

impl MemoryHardStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HardStateStore for MemoryHardStateStore {
    fn persist(&mut self, state: &HardState) -> Result<()> {
        self.state = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<HardState>> {
        Ok(self.state.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::{NodeId, Term};

    #[test]
    fn empty_store_loads_none() {
        let store = MemoryHardStateStore::new();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn persist_and_load() {
        let mut store = MemoryHardStateStore::new();
        let hs = HardState {
            current_term: Term(5),
            voted_for: Some(NodeId(2)),
        };
        store.persist(&hs).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(5));
        assert_eq!(loaded.voted_for, Some(NodeId(2)));
    }

    #[test]
    fn persist_overwrites() {
        let mut store = MemoryHardStateStore::new();
        store
            .persist(&HardState {
                current_term: Term(1),
                voted_for: None,
            })
            .unwrap();
        store
            .persist(&HardState {
                current_term: Term(3),
                voted_for: Some(NodeId(7)),
            })
            .unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(3));
        assert_eq!(loaded.voted_for, Some(NodeId(7)));
    }
}
