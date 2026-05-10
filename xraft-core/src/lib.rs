//! `xraft-core` — Raft consensus engine, pure logic, no I/O.

pub mod config;
pub mod error;
pub mod message;
pub mod node;
pub mod state_machine;
pub mod storage;
pub mod transport;
pub mod types;
