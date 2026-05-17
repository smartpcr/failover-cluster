mod hard_state;
mod log;
mod snapshot_store;

// Re-export the trait from where it actually lives. Trying to re-export
// it through `snapshot_store::SnapshotStore` would hit E0603 because the
// inner module brings it in via a private `use xraft_core::storage::...`
// statement — only `pub use` items in `snapshot_store.rs` are visible
// to outer modules for re-export.
pub use xraft_core::storage::{HardStateStore, LogStore, SnapshotStore};

// Re-export the snapshot-store implementations + chunk reader so
// downstream crates can name them. Without these, every public item
// defined in `snapshot_store.rs` is dead-code from the crate's POV
// (the trait re-export above is the only crate-public surface
// otherwise) and the workspace's `-D warnings` policy turns the
// resulting `dead_code` lints into compilation errors.
pub use snapshot_store::{
    DEFAULT_CHUNK_SIZE, FileSnapshotStore, MemorySnapshotStore, SnapshotChunkReader,
};

// Stage 6.1: the server-assembly path needs `FileLogStore`,
// `MemoryLogStore`, and `FileHardStateStore` — re-export them
// here so `xraft-server` does not have to reach into private
// modules. `log.rs` is an existing-but-orphaned module that
// implements the canonical `LogStore`; `hard_state.rs` is new
// Stage 6.1 code that implements the canonical `HardStateStore`
// trait (the existing `state.rs` file defines its own
// incompatible trait + HardState type and is intentionally NOT
// wired in here).
pub use hard_state::{FileHardStateStore, MemoryHardStateStore};
pub use log::{FileLogStore, MemoryLogStore};

// Stage 7.3 (iter-5) leader-epoch-checkpoint surface. Re-export the
// Kafka-compatible checkpoint helpers so the canonical names live in
// the crate's public API and a `cargo doc --no-deps` lists them under
// `xraft_storage::`. `architecture.md` §6 + `implementation-plan.md`
// §357,367 require this surface even though the engine hasn't yet
// wired it into the WAL recovery path (engine-leader-epoch-recovery
// open question).
pub use log::{
    LEADER_EPOCH_CHECKPOINT_FILE, LeaderEpochEntry, read_leader_epoch_checkpoint,
    write_leader_epoch_checkpoint,
};
