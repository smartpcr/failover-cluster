//! `xraft-test` — integration test harness for the XRAFT engine.
//!
//! Provides two cluster harnesses for Stage 8.1 multi-node tests:
//!
//! * [SimulatedCluster] — in-process clusters wired through
//!   [SimulatedNetwork] and [SimulatedClock] for deterministic,
//!   fast-running tests that can model latency, packet loss, and
//!   network partitions.
//! * [RealCluster] — clusters that boot real `xraft-server::Server`
//!   instances binding to localhost ports and talking real gRPC,
//!   used by the brief's real-network 3-node and 5-node scenarios.

pub mod clock;
pub mod fault_injection;
pub mod network;
pub mod observer;
pub mod real;
pub mod simulated;
pub mod state_machine;
pub mod teardown;

pub use clock::{ManualTickController, ManualTickSource, SimulatedClock};
pub use fault_injection::{ChaosScheduleConfig, FaultEvent, FaultInjector, FaultSchedule};
pub use network::{SimulatedNetwork, SimulatedTransport};
pub use observer::{TestObserver, TestObserverHandle};
pub use real::{RealCluster, RealClusterConfig, RealNode};
pub use simulated::{ElectionWindow, SimulatedCluster, SimulatedClusterConfig, SimulatedNode};
pub use state_machine::{RecordingHandle, RecordingStateMachine};
