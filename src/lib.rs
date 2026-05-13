//! # failover-cluster-raft
//!
//! Rust implementation of the Raft consensus protocol for the
//! `failover-cluster` project, modelled on Apache Kafka's **`KRaft`**
//! protocol (KIP-500 et al.).
//!
//! This crate is built up stage-by-stage. The current stage covers
//! the **Raft Node State Machine** — the deterministic core that
//! drives role transitions (Follower → Candidate → Leader → …) and
//! enforces Raft's safety invariants. Subsequent stages will add the
//! persistent log, RPC wire format, network transport, snapshotting,
//! and dynamic-membership commit protocol.
//!
//! ## Design
//!
//! The state machine is a [pure Mealy machine][mealy]: each call to
//! [`RaftNode::handle`] takes an [`Event`] and returns a `Vec<Command>`
//! describing the side-effects the host runtime should perform. The
//! SM itself never touches the network, disk, or clock.
//!
//! [mealy]: https://en.wikipedia.org/wiki/Mealy_machine
//!
//! ## Roles
//!
//! Following `KRaft`, this crate models five [roles][RoleKind]:
//!
//! | Role            | Description                                            |
//! |-----------------|--------------------------------------------------------|
//! | `Follower`      | Default state. Accepts `AppendEntries`/`RequestVote`. |
//! | `PreCandidate`  | Pre-vote probe. Does **not** bump `current_term`.     |
//! | `Candidate`     | Real candidacy. Bumps `current_term`, self-votes.     |
//! | `Leader`        | Replicates entries; sends heartbeats.                  |
//! | `Observer`      | Non-voting replica (Kafka brokers in `KRaft`).         |

#![deny(rust_2018_idioms)]
#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod node;
pub mod types;

pub use config::NodeConfig;
pub use error::{RaftError, RaftResult};
pub use node::{
    Command, Event, LogMetadataCache, PersistentState, RaftNode, Role, RoleKind, VolatileState,
};
pub use types::{LogIndex, LogMetadata, NodeId, Term};
