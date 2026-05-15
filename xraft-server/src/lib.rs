//! `xraft-server` — async runtime driver, message router, and
//! eventual main binary glue.
//!
//! Stage 4.2 introduces the [`driver`] module: the long-running event
//! loop that owns a single [`RaftNode`](xraft_core::RaftNode), pumps
//! inputs from tick / inbound RPCs / outbound results / client commands
//! / shutdown, and dispatches the resulting `Action`s against the
//! storage + transport + state-machine backends.
//!
//! Stage 5 will assemble all of these into the `xraft-server` binary
//! (see `main.rs`).

pub mod driver;
pub mod server;

pub use driver::{
    Driver, DriverConfig, DriverHandle, DriverInboundHandler, InboundRpc, MessageRouter,
    OutboundResult,
};
