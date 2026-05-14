//! Write-ahead log placeholder.
//!
//! Stage 1.1 (Cargo Workspace & Crate Layout) owns only crate
//! scaffolding. The full WAL implementation (binary frame format,
//! segment rotation, CRC32 verification, crash recovery) is the
//! deliverable of the Stage 2.1 workstream, not this one. An
//! earlier orphan implementation that lived here was reduced to
//! this placeholder to satisfy the iter-1 evaluator finding about
//! dead/uncompiled module bodies. Physical deletion is prohibited
//! by the workstream brief, so the file is retained as a compiled
//! doc-only stub wired into `lib.rs` via a private `mod log;`.
