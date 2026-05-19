//! Shared helpers for the Stage 8.2 chaos + stress test binaries.
//!
//! Cargo discovers integration tests only at the top level of
//! `tests/`. Files under `tests/common/` are included into each test
//! binary via `mod common;` in the top-level wrapper file (see
//! `tests/chaos_tests.rs`, `tests/stress_tests.rs`). The `mod.rs`
//! pattern (rather than a sibling `tests/common.rs`) is the canonical
//! way to keep this module from being mistaken for a standalone test
//! binary.
//!
//! Sub-modules:
//!
//! * [`cluster_harness`] — utilities for building a chaos-tuned
//!   [`SimulatedCluster`](xraft_test::SimulatedCluster), proposing
//!   under faults with retry, applying scheduled faults at the right
//!   simulated-time offsets, and verifying that every alive node's
//!   recording state machine contains every committed entry. The
//!   fault-injection primitives themselves
//!   ([`FaultInjector`](xraft_test::FaultInjector),
//!   [`FaultSchedule`](xraft_test::FaultSchedule),
//!   [`FaultEvent`](xraft_test::FaultEvent)) live in the
//!   `xraft-test` library crate
//!   ([`xraft_test::fault_injection`]) and are imported directly
//!   wherever needed.
//!
//! Iter-14 rename note: this module was previously `common::harness`;
//! the rename to `common::cluster_harness` aligns the file path with
//! the workstream brief's `Target files` list
//! (`tests/common/cluster_harness.rs`).

// The compiler will warn about unused items when a particular test
// binary doesn't touch every helper. Suppress at module level rather
// than per-fn because every wrapper binary uses a subset.
#![allow(dead_code)]

pub mod cluster_harness;
