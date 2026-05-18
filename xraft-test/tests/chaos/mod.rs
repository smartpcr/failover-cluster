//! Chaos test scenarios for Stage 8.2.
//!
//! Each sub-module owns one scenario family from the brief. To
//! keep this module-doc trivially robust against
//! `clippy::doc_lazy_continuation` (the lint is sensitive to the
//! exact whitespace beneath multi-line list items), the
//! per-scenario descriptions live in each sub-module's own
//! doc-comment rather than as nested bullets here.
//!
//! * [`network_partition`] — random partition / drop / latency
//!   chaos plus deterministic-replay equivalence.
//! * [`node_failure`] — leader-targeted unavailability via
//!   partition and simultaneous-election resolution.
//! * [`node_crash`] — permanent fail-stop node loss.
//! * [`clock_skew`] — differential election-timer rates.
//!
//! Sub-modules are declared `pub` (rather than the default
//! `pub(crate)`) so a future external test binary could re-include
//! the same scenarios with different seed sets without copy-pasting
//! the function bodies. The current chaos-test wrapper binary
//! (`tests/chaos_tests.rs`) does not rely on this, but the looser
//! visibility costs nothing.

pub mod clock_skew;
pub mod network_partition;
pub mod node_crash;
pub mod node_failure;
