mod snapshot_store;
mod state;

// Re-export the trait from where it actually lives. Trying to re-export
// it through `snapshot_store::SnapshotStore` would hit E0603 because the
// inner module brings it in via a private `use xraft_core::storage::...`
// statement — only `pub use` items in `snapshot_store.rs` are visible
// to outer modules for re-export.
pub use xraft_core::storage::{HardStateStore, SnapshotStore};

// Re-export the snapshot-store implementations + chunk reader so
// downstream crates can name them. Without these, every public item
// defined in `snapshot_store.rs` is dead-code from the crate's POV
// (the trait re-export above is the only crate-public surface
// otherwise) and the workspace's `-D warnings` policy turns the
// resulting `dead_code` lints into compilation errors.
pub use snapshot_store::{
    DEFAULT_CHUNK_SIZE, FileSnapshotStore, MemorySnapshotStore, SnapshotChunkReader,
};

// Re-export the hard-state implementations so downstream crates can
// wire the canonical Stage 2.2 storage into their RaftNode driver.
pub use state::{FileHardStateStore, MemoryHardStateStore};
