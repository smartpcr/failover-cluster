//! `xraft-core` — Raft consensus engine, pure logic, no I/O.

pub mod config;
pub mod error;
pub mod message;
pub mod node;
pub mod state_machine;
pub mod storage;
pub mod transport;
pub mod types;

// ---------------------------------------------------------------------------
// Convenience re-exports for the most-used public API surface.
//
// Downstream crates (`xraft-server`, `xraft-transport`) wire the consensus
// engine via these names. Re-exporting at crate root keeps call sites short
// (e.g. `xraft_core::RaftNode` rather than `xraft_core::node::RaftNode`).
// ---------------------------------------------------------------------------

pub use config::ClusterConfig;
pub use error::{Result, XRaftError};
pub use message::{Action, Entry, EntryPayload, Input, OutboundMessage};
pub use node::{ElectionTimer, PeerState, RaftNode};
pub use state_machine::{NoOpStateMachine, StateMachine};
pub use types::{
    HardState, LogIndex, NodeId, NodeRole, Term, VoteGrantedSet, VoterRecord, VoterSet,
};
