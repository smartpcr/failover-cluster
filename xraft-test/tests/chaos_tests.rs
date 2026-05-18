//! Top-level cargo integration-test binary for the Stage 8.2 chaos
//! scenarios.
//!
//! Cargo discovers integration tests only at the top level of
//! `tests/`; files under `tests/chaos/` and `tests/common/` become
//! modules of this binary via the `mod` declarations below. Every
//! `#[tokio::test]` inside those modules is discovered and run by
//! `cargo test --test chaos_tests`.

mod common;

mod chaos;
