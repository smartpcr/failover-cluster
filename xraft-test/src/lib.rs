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
//!
//! Stage 8.2 adds [`chaos`] and [`linearisability`] — a seeded
//! deterministic fault injector and a post-hoc history validator.

pub mod chaos;
pub mod clock;
pub mod linearisability;
pub mod network;
pub mod observer;
pub mod persistent_storage;
pub mod real;
pub mod simulated;
pub mod state_machine;
pub mod teardown;

pub use chaos::{ChaosConfig, ChaosEngine, ChaosFault, ChaosWeights};
pub use clock::{ManualTickController, ManualTickSource, SimulatedClock};
pub use linearisability::{
    AppliedByNode, HistoryRecorder, LinearisabilityViolation, OpRecord, verify_linearisable,
};
pub use network::{SimulatedNetwork, SimulatedTransport};
pub use observer::{TestObserver, TestObserverHandle};
pub use persistent_storage::{
    PersistentNodeStorage, SharedMemoryHardStateStore, SharedMemoryLogStore,
    SharedMemorySnapshotStore,
};
pub use real::{RealCluster, RealClusterConfig, RealNode};
pub use simulated::{SimulatedCluster, SimulatedClusterConfig, SimulatedNode};
pub use state_machine::{RecordingHandle, RecordingStateMachine};
