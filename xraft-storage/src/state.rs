//! Hard-state persistence placeholder.
//!
//! Stage 1.1 owns crate scaffolding only. The canonical hard-state
//! contract (term + voted-for, plus the durability trait) is the
//! property of `xraft_core::storage` and is delivered by the
//! Stage 2.2 (Persistent Raft State) workstream, not this one. An
//! earlier orphan implementation that lived in this file defined a
//! divergent hard-state type and pulled in undeclared dependencies;
//! it was reduced to this placeholder so the crate no longer carries
//! a competing hard-state contract or imports that the manifest does
//! not declare. Physical deletion is prohibited by the workstream
//! brief, so the file is retained as a compiled doc-only stub wired
//! into `lib.rs` via a private `mod state;`.
