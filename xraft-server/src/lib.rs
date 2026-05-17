//! `xraft-server` — async runtime driver, message router, HTTP admin
//! endpoint, and the main binary glue assembled in [`server::Server`].
//!
//! Stage 4.2 introduced the [`driver`] module: the long-running event
//! loop that owns a single [`RaftNode`](xraft_core::RaftNode), pumps
//! inputs from tick / inbound RPCs / outbound results / client commands
//! / shutdown, and dispatches the resulting `Action`s against the
//! storage + transport + state-machine backends.
//!
//! Stage 6.1 adds the bootstrap, lifecycle, and observability surface:
//! - [`status`] — `NodeStatus` snapshot + `StatusPublisher`
//! - [`metrics`] — Prometheus `Registry` + MVP counter/gauge/histogram
//! - [`admin`] — axum `/health` + `/metrics` HTTP server
//! - [`server`] — `Server::start` assembles config → storage → raft →
//!   transport → driver → admin into a single runnable unit
//! - [`main`](../xraft-server/src/main.rs) — clap CLI, tracing-JSON
//!   logging, SIGTERM/SIGINT/SIGHUP signal handling

pub mod admin;
pub mod driver;
pub mod metrics;
pub mod server;
pub mod status;
pub mod teardown;

pub use admin::{AdminConfig, AdminServer, ClusterInfo, router as admin_router};
pub use driver::{
    Driver, DriverConfig, DriverHandle, DriverInboundHandler, DriverObserver, InboundRpc,
    IntervalTickSource, MessageRouter, OutboundResult, TickSource, TriggeredSnapshotInfo,
};
pub use metrics::XRaftMetrics;
pub use server::{Server, ServerConfig, ServerHandle};
pub use status::{NodeStatus, StatusPublisher, role_to_gauge, role_to_str};
