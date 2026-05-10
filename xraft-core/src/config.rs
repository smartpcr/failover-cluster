//! Cluster configuration.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::types::NodeId;

/// Configuration for an XRAFT cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's unique identifier.
    pub node_id: NodeId,
    /// Logical cluster identifier.
    pub cluster_id: String,
    /// Address to listen on for gRPC traffic.
    pub listen_addr: String,
    /// Peer addresses in the cluster.
    pub peers: Vec<String>,

    /// Minimum election timeout in milliseconds.
    #[serde(default = "default_election_timeout_min")]
    pub election_timeout_min_ms: u64,
    /// Maximum election timeout in milliseconds.
    #[serde(default = "default_election_timeout_max")]
    pub election_timeout_max_ms: u64,
    /// Fetch interval in milliseconds (pull-based replication).
    #[serde(default = "default_fetch_interval")]
    pub fetch_interval_ms: u64,
    /// Tick interval in milliseconds.
    #[serde(default = "default_tick_interval")]
    pub tick_interval_ms: u64,
    /// Number of applied entries between snapshots.
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,
    /// Maximum log entries before triggering compaction.
    #[serde(default = "default_max_log_entries")]
    pub max_log_entries_before_compaction: u64,
    /// Directory for persistent data (log segments, snapshots, quorum-state).
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

fn default_election_timeout_min() -> u64 {
    150
}
fn default_election_timeout_max() -> u64 {
    300
}
fn default_fetch_interval() -> u64 {
    50
}
fn default_tick_interval() -> u64 {
    10
}
fn default_snapshot_interval() -> u64 {
    10_000
}
fn default_max_log_entries() -> u64 {
    100_000
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}
