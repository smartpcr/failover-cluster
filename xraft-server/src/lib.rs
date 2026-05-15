//! `xraft-server` — driver layer that wires the I/O-free `xraft-core`
//! consensus engine to durable storage, transport, and the host process.
//!
//! # Stage 2.2 — Persistent Raft State
//!
//! [`Driver`] (the storage-side primitive) and [`Server`] (the
//! lifecycle wrapper) are the Stage 2.2 deliverables on the consumer
//! side of the [`HardStateStore`](xraft_core::storage::HardStateStore)
//! trait. Together they:
//!
//! * Load any previously persisted [`HardState`](xraft_core::types::HardState)
//!   from `<config.data_dir>/quorum-state` on startup, falling back to
//!   [`HardState::default`](xraft_core::types::HardState::default) when
//!   the store is empty (per `implementation-plan.md` Stage 2.2 contract).
//! * Construct a [`RaftNode`](xraft_core::RaftNode) at the recovered
//!   term and vote via
//!   [`RaftNode::new_with_initial_hard_state`](xraft_core::node::RaftNode::new_with_initial_hard_state).
//! * Process [`Action::PersistHardState`](xraft_core::message::Action::PersistHardState)
//!   inline by reading `node.hard_state` and persisting it **before**
//!   returning any other action to the caller. This makes the Raft safety
//!   invariant ("durable state lands before any RPC reply") impossible to
//!   violate from the call site.
//! * Poison the [`Driver`] on persist failure so subsequent inputs
//!   cannot produce actions derived from un-persisted state.
//! * Surface engine actions Stage 2.2 has no driver for (durable log
//!   append, state-machine apply, snapshot, fetch service) as
//!   [`ServerError::UnsupportedAction`] rather than silently dropping
//!   them.
//!
//! The [`xraft-server` binary](../xraft-server/index.html) calls
//! [`Server::open`] in its `main` so the persistent-state primitives
//! are wired into the production startup path, not just the tests.
//! Higher Stage workstreams (3.x replication, 4.x state-machine apply,
//! 5.x transport / RPC) replace the action classifier with real
//! dispatch without changing the Stage 2.2 persistence guarantee.

pub mod driver;
pub mod server;

#[cfg(test)]
pub(crate) mod test_support;

pub use driver::{Driver, DriverError};
// `HARD_STATE_DIR_NAME` is the deprecated alias for `HARD_STATE_FILE_NAME`
// (see server.rs). Re-exported alongside the canonical name so any
// downstream caller that imported the pre-iter-4 name still compiles
// against this crate; the deprecation attribute on the constant
// definition will surface a warning at the call site to nudge migration.
#[allow(deprecated)]
pub use server::HARD_STATE_DIR_NAME;
pub use server::{HARD_STATE_FILE_NAME, Server, ServerError};
