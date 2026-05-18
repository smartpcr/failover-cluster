//! Stress test scenarios for Stage 8.2.
//!
//! The brief calls for a sustained-throughput stress test under
//! random single-node failures. The implementation is split across:
//!
//! * [`throughput`] — pipelined sustained throughput with a SINGLE
//!   mid-run fail-stop kill (steady-state half of the brief).
//! * [`leader_churn`] — pipelined sustained throughput while a
//!   leader-targeted partition schedule fires every 4 s
//!   (churn-under-load half). The 4 s interval matches
//!   `leader_churn::CHURN_INTERVAL` and leaves ~3 s of steady-state
//!   per beat for followers to drain their post-rejoin fetch
//!   backlog before the next partition fires.

pub mod leader_churn;
pub mod throughput;
