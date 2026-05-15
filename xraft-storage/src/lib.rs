mod log;
mod log_format;
mod log_segment;
mod snapshot_store;

// Re-export the trait from where it actually lives. Trying to re-export
// it through `snapshot_store::SnapshotStore` would hit E0603 because the
// inner module brings it in via a private `use xraft_core::storage::...`
// statement — only `pub use` items in `snapshot_store.rs` are visible
// to outer modules for re-export.
pub use xraft_core::storage::{LogStore, SnapshotStore};

// Re-export the snapshot-store implementations + chunk reader so
// downstream crates can name them. Without these, every public item
// defined in `snapshot_store.rs` is dead-code from the crate's POV
// (the trait re-export above is the only crate-public surface
// otherwise) and the workspace's `-D warnings` policy turns the
// resulting `dead_code` lints into compilation errors.
pub use snapshot_store::{
    DEFAULT_CHUNK_SIZE, FileSnapshotStore, MemorySnapshotStore, SnapshotChunkReader,
};

// Re-export the log-store implementations so downstream crates (the
// Raft node, integration tests) can construct a WAL. Without these
// re-exports the `log` module's public items would be unreachable
// from outside the crate and trigger `dead_code` under the
// workspace-wide `-D warnings` policy.
pub use log::{DEFAULT_MAX_SEGMENT_SIZE, FileLogStore, MemoryLogStore};
