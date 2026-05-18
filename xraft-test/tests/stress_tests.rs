//! Top-level cargo integration-test binary for the Stage 8.2 stress
//! scenarios (sustained throughput under load).
//!
//! See `tests/chaos_tests.rs` for the rationale behind the wrapper
//! pattern — same shape, different test scope.

mod common;

mod stress;
