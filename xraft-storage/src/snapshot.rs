//! Legacy snapshot-module placeholder.
//!
//! The currently wired snapshot scaffolding for Stage 1.1 lives in
//! `snapshot_store.rs` next to this file; that module exposes the
//! `SnapshotStore` trait surface used by the rest of the workspace.
//! An earlier orphan implementation that lived in this file was
//! reduced to this placeholder so the crate no longer carries two
//! parallel snapshot bodies. Full Stage 2.3 work belongs to the
//! Stage 2.3 (Snapshot Store) workstream. Physical deletion is
//! prohibited by the workstream brief, so the file is retained as
//! a compiled doc-only stub wired into `lib.rs` via a private
//! `mod snapshot;`.
