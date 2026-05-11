//! Cluster configuration with TOML loading, environment variable overrides,
//! and validation.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::XRaftError;
use crate::types::{DirectoryId, Endpoint, NodeId, VoterRecord, VoterSet};

/// TOML-friendly voter record for structured cluster membership configuration.
///
/// Complements the flat `peers` field with full KRaft-style voter metadata
/// including `DirectoryId` and typed endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoterConfig {
    pub node_id: u64,
    pub directory_id: String,
    pub host: String,
    pub port: u16,
}

/// Configuration for an XRAFT cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's unique identifier.
    pub node_id: NodeId,
    /// Logical cluster identifier.
    pub cluster_id: String,
    /// Address to listen on for gRPC traffic (e.g. `"0.0.0.0:6000"`).
    pub listen_addr: String,
    /// Peer addresses in the cluster (e.g. `["host1:6000", "host2:6001"]`).
    /// Used for simple deployments. For full KRaft-style voter metadata, use
    /// the `voters` field instead.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Structured voter records with `DirectoryId` and endpoints.
    /// When provided, this takes precedence over `peers` for building the
    /// `VoterSet` used by the consensus engine.
    #[serde(default)]
    pub voters: Vec<VoterConfig>,

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

impl ClusterConfig {
    /// Parse a TOML string into a validated `ClusterConfig`.
    ///
    /// This method does **not** read environment variables. Use
    /// [`from_toml_str_with_env`] or call [`apply_env_overrides`] explicitly
    /// if you need `XRAFT_*` environment variable support.
    pub fn from_toml_str(s: &str) -> Result<Self, XRaftError> {
        let config: ClusterConfig =
            toml::from_str(s).map_err(|e| XRaftError::Config(format!("TOML parse error: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Parse a TOML string, apply `XRAFT_*` environment variable overrides,
    /// then validate. Convenience wrapper for production use where env-based
    /// overrides are desired.
    pub fn from_toml_str_with_env(s: &str) -> Result<Self, XRaftError> {
        let mut config: ClusterConfig =
            toml::from_str(s).map_err(|e| XRaftError::Config(format!("TOML parse error: {e}")))?;
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a TOML file, apply env overrides, and validate.
    pub fn load(path: &Path) -> Result<Self, XRaftError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| XRaftError::Config(format!("failed to read {}: {e}", path.display())))?;
        Self::from_toml_str_with_env(&content)
    }

    /// Apply `XRAFT_*` environment variable overrides from the real
    /// environment. Returns an error if a numeric variable is set but
    /// cannot be parsed.
    pub fn apply_env_overrides(&mut self) -> Result<(), XRaftError> {
        self.apply_env_overrides_with(|key| std::env::var(key))
    }

    /// Apply `XRAFT_*` environment variable overrides using a custom env reader.
    ///
    /// Returns an error if a numeric environment variable is set to a non-empty,
    /// non-parseable value. Empty strings are silently ignored for all variables
    /// (treated as "not set").
    ///
    /// Supported variables:
    /// - `XRAFT_NODE_ID` — overrides `node_id`
    /// - `XRAFT_CLUSTER_ID` — overrides `cluster_id`
    /// - `XRAFT_LISTEN_ADDR` — overrides `listen_addr`
    /// - `XRAFT_PEERS` — comma-separated list of peer addresses
    /// - `XRAFT_ELECTION_TIMEOUT_MIN_MS` — overrides `election_timeout_min_ms`
    /// - `XRAFT_ELECTION_TIMEOUT_MAX_MS` — overrides `election_timeout_max_ms`
    /// - `XRAFT_FETCH_INTERVAL_MS` — overrides `fetch_interval_ms`
    /// - `XRAFT_TICK_INTERVAL_MS` — overrides `tick_interval_ms`
    /// - `XRAFT_SNAPSHOT_INTERVAL` — overrides `snapshot_interval`
    /// - `XRAFT_MAX_LOG_ENTRIES` — overrides `max_log_entries_before_compaction`
    /// - `XRAFT_DATA_DIR` — overrides `data_dir`
    fn apply_env_overrides_with<F, E>(&mut self, env_var: F) -> std::result::Result<(), XRaftError>
    where
        F: Fn(&str) -> std::result::Result<String, E>,
        E: std::fmt::Debug,
    {
        if let Ok(val) = env_var("XRAFT_NODE_ID") {
            if val.is_empty() {
                // empty string is treated as "not set"
            } else {
                self.node_id = NodeId(val.parse::<u64>().map_err(|_| {
                    XRaftError::Config(format!("XRAFT_NODE_ID: invalid u64 value '{val}'"))
                })?);
            }
        }
        if let Ok(val) = env_var("XRAFT_CLUSTER_ID")
            && !val.is_empty()
        {
            self.cluster_id = val;
        }
        if let Ok(val) = env_var("XRAFT_LISTEN_ADDR")
            && !val.is_empty()
        {
            self.listen_addr = val;
        }
        if let Ok(val) = env_var("XRAFT_PEERS")
            && !val.is_empty()
        {
            self.peers = val
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Ok(val) = env_var("XRAFT_ELECTION_TIMEOUT_MIN_MS")
            && !val.is_empty()
        {
            self.election_timeout_min_ms = val.parse::<u64>().map_err(|_| {
                XRaftError::Config(format!(
                    "XRAFT_ELECTION_TIMEOUT_MIN_MS: invalid u64 value '{val}'"
                ))
            })?;
        }
        if let Ok(val) = env_var("XRAFT_ELECTION_TIMEOUT_MAX_MS")
            && !val.is_empty()
        {
            self.election_timeout_max_ms = val.parse::<u64>().map_err(|_| {
                XRaftError::Config(format!(
                    "XRAFT_ELECTION_TIMEOUT_MAX_MS: invalid u64 value '{val}'"
                ))
            })?;
        }
        if let Ok(val) = env_var("XRAFT_FETCH_INTERVAL_MS")
            && !val.is_empty()
        {
            self.fetch_interval_ms = val.parse::<u64>().map_err(|_| {
                XRaftError::Config(format!(
                    "XRAFT_FETCH_INTERVAL_MS: invalid u64 value '{val}'"
                ))
            })?;
        }
        if let Ok(val) = env_var("XRAFT_TICK_INTERVAL_MS")
            && !val.is_empty()
        {
            self.tick_interval_ms = val.parse::<u64>().map_err(|_| {
                XRaftError::Config(format!("XRAFT_TICK_INTERVAL_MS: invalid u64 value '{val}'"))
            })?;
        }
        if let Ok(val) = env_var("XRAFT_SNAPSHOT_INTERVAL")
            && !val.is_empty()
        {
            self.snapshot_interval = val.parse::<u64>().map_err(|_| {
                XRaftError::Config(format!(
                    "XRAFT_SNAPSHOT_INTERVAL: invalid u64 value '{val}'"
                ))
            })?;
        }
        if let Ok(val) = env_var("XRAFT_MAX_LOG_ENTRIES")
            && !val.is_empty()
        {
            self.max_log_entries_before_compaction = val.parse::<u64>().map_err(|_| {
                XRaftError::Config(format!("XRAFT_MAX_LOG_ENTRIES: invalid u64 value '{val}'"))
            })?;
        }
        if let Ok(val) = env_var("XRAFT_DATA_DIR")
            && !val.is_empty()
        {
            self.data_dir = PathBuf::from(val);
        }
        Ok(())
    }

    /// Validate the configuration for logical consistency.
    pub fn validate(&self) -> Result<(), XRaftError> {
        if self.cluster_id.is_empty() {
            return Err(XRaftError::Config("cluster_id must not be empty".into()));
        }
        if self.listen_addr.is_empty() {
            return Err(XRaftError::Config("listen_addr must not be empty".into()));
        }
        Self::validate_address(&self.listen_addr, "listen_addr")?;

        // Validate peer addresses
        for (i, peer) in self.peers.iter().enumerate() {
            if peer.is_empty() {
                return Err(XRaftError::Config(format!(
                    "peer address at index {i} must not be empty"
                )));
            }
            Self::validate_address(peer, &format!("peer[{i}]"))?;
        }

        // Check for duplicate peer addresses
        {
            let mut seen = std::collections::HashSet::new();
            for peer in &self.peers {
                if !seen.insert(peer.as_str()) {
                    return Err(XRaftError::Config(format!(
                        "duplicate peer address: '{peer}'"
                    )));
                }
            }
        }

        // Self-membership: listen_addr should not appear in peers.
        // Compare parsed (host, port) tuples so that wildcard listen hosts
        // (0.0.0.0, [::], ::) are recognised as matching any peer host on the
        // same port. For non-wildcard hosts we fall back to exact string
        // comparison since DNS resolution is out of scope at config time.
        if let Some((listen_host, listen_port)) = Self::parse_host_port(&self.listen_addr) {
            let listen_is_wildcard = matches!(
                listen_host.as_str(),
                "0.0.0.0" | "::" | "[::]"
            );
            for peer in &self.peers {
                let is_self = if let Some((peer_host, peer_port)) = Self::parse_host_port(peer) {
                    if listen_port != peer_port {
                        false
                    } else if listen_is_wildcard {
                        // A wildcard listen address matches any peer on the same port
                        true
                    } else {
                        listen_host == peer_host
                    }
                } else {
                    // Unparseable peer — fall back to exact string match
                    peer == &self.listen_addr
                };
                if is_self {
                    return Err(XRaftError::Config(format!(
                        "listen_addr '{}' must not appear in peers (a node should not peer \
                         with itself); peer '{}' resolves to the same endpoint",
                        self.listen_addr, peer
                    )));
                }
            }
        }

        // Validate structured voters if provided
        if !self.voters.is_empty() {
            let mut seen_ids = std::collections::HashSet::new();
            let mut seen_endpoints = std::collections::HashSet::new();
            for (i, voter) in self.voters.iter().enumerate() {
                if voter.host.trim().is_empty() {
                    return Err(XRaftError::Config(format!(
                        "voter[{i}] has an empty host"
                    )));
                }
                if voter.port == 0 {
                    return Err(XRaftError::Config(format!(
                        "voter[{i}] port must not be zero"
                    )));
                }
                // Validate directory_id is a valid, non-nil UUID
                let parsed_uuid = uuid::Uuid::parse_str(&voter.directory_id).map_err(|_| {
                    XRaftError::Config(format!(
                        "voter[{i}] directory_id '{}' is not a valid UUID",
                        voter.directory_id
                    ))
                })?;
                if parsed_uuid.is_nil() {
                    return Err(XRaftError::Config(format!(
                        "voter[{i}] directory_id must not be the nil UUID"
                    )));
                }
                let key = (voter.node_id, voter.directory_id.clone());
                if !seen_ids.insert(key) {
                    return Err(XRaftError::Config(format!(
                        "duplicate voter: node_id={}, directory_id={}",
                        voter.node_id, voter.directory_id
                    )));
                }
                // Reject duplicate endpoints across voters
                let endpoint_key = format!("{}:{}", voter.host.trim(), voter.port);
                if !seen_endpoints.insert(endpoint_key.clone()) {
                    return Err(XRaftError::Config(format!(
                        "duplicate voter endpoint: '{endpoint_key}'"
                    )));
                }
            }
        }

        if self.election_timeout_min_ms == 0 {
            return Err(XRaftError::Config(
                "election_timeout_min_ms must be > 0".into(),
            ));
        }
        if self.election_timeout_max_ms <= self.election_timeout_min_ms {
            return Err(XRaftError::Config(format!(
                "election_timeout_max_ms ({}) must be > election_timeout_min_ms ({})",
                self.election_timeout_max_ms, self.election_timeout_min_ms
            )));
        }
        if self.fetch_interval_ms == 0 {
            return Err(XRaftError::Config("fetch_interval_ms must be > 0".into()));
        }
        if self.tick_interval_ms == 0 {
            return Err(XRaftError::Config("tick_interval_ms must be > 0".into()));
        }
        if self.snapshot_interval == 0 {
            return Err(XRaftError::Config("snapshot_interval must be > 0".into()));
        }
        if self.max_log_entries_before_compaction == 0 {
            return Err(XRaftError::Config(
                "max_log_entries_before_compaction must be > 0".into(),
            ));
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(XRaftError::Config("data_dir must not be empty".into()));
        }
        Ok(())
    }

    /// Build a `VoterSet` from the structured `voters` configuration.
    ///
    /// Returns `None` if no structured voters are configured (falls back to
    /// the flat `peers` model).
    pub fn build_voter_set(&self) -> Result<Option<VoterSet>, XRaftError> {
        if self.voters.is_empty() {
            return Ok(None);
        }
        let records: Vec<VoterRecord> = self
            .voters
            .iter()
            .map(|v| {
                let uuid = uuid::Uuid::parse_str(&v.directory_id).map_err(|_| {
                    XRaftError::Config(format!(
                        "voter directory_id '{}' is not a valid UUID",
                        v.directory_id
                    ))
                })?;
                Ok(VoterRecord {
                    node_id: NodeId(v.node_id),
                    directory_id: DirectoryId(uuid),
                    endpoints: vec![Endpoint::new(v.host.clone(), v.port)],
                })
            })
            .collect::<Result<Vec<_>, XRaftError>>()?;
        let vs = VoterSet::try_new(records).map_err(|e| {
            XRaftError::Config(format!("invalid voter set: {e}"))
        })?;
        Ok(Some(vs))
    }

    /// Split an address string into `(host, port)`.
    ///
    /// Returns `None` if the address is not in a recognisable `host:port`
    /// format. Used by the self-membership check to compare listen and peer
    /// addresses at the (host, port) level rather than raw strings.
    fn parse_host_port(addr: &str) -> Option<(String, u16)> {
        let colon_pos = addr.rfind(':')?;
        let host = &addr[..colon_pos];
        let port_str = &addr[colon_pos + 1..];
        if host.is_empty() || port_str.is_empty() {
            return None;
        }
        let port: u16 = port_str.parse().ok()?;
        Some((host.to_lowercase(), port))
    }

    /// Validate that an address string is in `host:port` format with a
    /// non-empty host and a numeric port in the valid range (1–65535).
    fn validate_address(addr: &str, field: &str) -> Result<(), XRaftError> {
        // Find the last colon to handle IPv6 addresses like [::1]:8080
        let colon_pos = addr.rfind(':').ok_or_else(|| {
            XRaftError::Config(format!("{field} '{addr}' must be in host:port format"))
        })?;
        let host = &addr[..colon_pos];
        let port_str = &addr[colon_pos + 1..];
        if host.is_empty() {
            return Err(XRaftError::Config(format!(
                "{field} '{addr}' has an empty host"
            )));
        }
        if port_str.is_empty() {
            return Err(XRaftError::Config(format!(
                "{field} '{addr}' has an empty port"
            )));
        }
        let port: u64 = port_str.parse().map_err(|_| {
            XRaftError::Config(format!(
                "{field} '{addr}' has a non-numeric port '{port_str}'"
            ))
        })?;
        if port == 0 || port > 65535 {
            return Err(XRaftError::Config(format!(
                "{field} '{addr}' port {port} is out of valid range 1–65535"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn valid_toml() -> &'static str {
        r#"
node_id = 1
cluster_id = "test-cluster"
listen_addr = "0.0.0.0:6001"
peers = ["node0:6000", "node2:6002"]
"#
    }

    fn full_toml() -> &'static str {
        r#"
node_id = 2
cluster_id = "prod-cluster"
listen_addr = "10.0.0.2:7000"
peers = ["10.0.0.1:7000", "10.0.0.3:7000"]
election_timeout_min_ms = 200
election_timeout_max_ms = 400
fetch_interval_ms = 100
tick_interval_ms = 20
snapshot_interval = 5000
max_log_entries_before_compaction = 50000
data_dir = "/var/lib/xraft"
"#
    }

    #[test]
    fn config_from_toml_all_fields() {
        let cfg = ClusterConfig::from_toml_str(full_toml()).unwrap();
        assert_eq!(cfg.node_id, NodeId(2));
        assert_eq!(cfg.cluster_id, "prod-cluster");
        assert_eq!(cfg.listen_addr, "10.0.0.2:7000");
        assert_eq!(cfg.peers, vec!["10.0.0.1:7000", "10.0.0.3:7000"]);
        assert_eq!(cfg.election_timeout_min_ms, 200);
        assert_eq!(cfg.election_timeout_max_ms, 400);
        assert_eq!(cfg.fetch_interval_ms, 100);
        assert_eq!(cfg.tick_interval_ms, 20);
        assert_eq!(cfg.snapshot_interval, 5000);
        assert_eq!(cfg.max_log_entries_before_compaction, 50000);
        assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/xraft"));
    }

    #[test]
    fn config_defaults_applied() {
        let cfg = ClusterConfig::from_toml_str(valid_toml()).unwrap();
        assert_eq!(cfg.election_timeout_min_ms, 150);
        assert_eq!(cfg.election_timeout_max_ms, 300);
        assert_eq!(cfg.fetch_interval_ms, 50);
        assert_eq!(cfg.tick_interval_ms, 10);
        assert_eq!(cfg.snapshot_interval, 10_000);
        assert_eq!(cfg.max_log_entries_before_compaction, 100_000);
        assert_eq!(cfg.data_dir, PathBuf::from("data"));
    }

    #[test]
    fn config_load_from_file() {
        // Use from_toml_str (not load()) to avoid reading real XRAFT_* env vars
        // that could alter assertions in CI or developer environments.
        let dir = std::env::temp_dir().join("xraft-test-config-load");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(valid_toml().as_bytes()).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let cfg = ClusterConfig::from_toml_str(&content).unwrap();
        assert_eq!(cfg.node_id, NodeId(1));
        assert_eq!(cfg.cluster_id, "test-cluster");
        assert_eq!(cfg.listen_addr, "0.0.0.0:6001");
        assert_eq!(cfg.peers, vec!["node0:6000", "node2:6002"]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn config_load_missing_file() {
        let result = ClusterConfig::load(Path::new("/nonexistent/path/node.toml"));
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("failed to read"), "got: {msg}");
    }

    #[test]
    fn config_validate_empty_cluster_id() {
        let toml = r#"
node_id = 1
cluster_id = ""
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("cluster_id"));
    }

    #[test]
    fn config_validate_empty_listen_addr() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = ""
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("listen_addr"));
    }

    #[test]
    fn config_validate_listen_addr_no_port() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "localhost"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("host:port"));
    }

    #[test]
    fn config_single_node_cluster_empty_peers() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = []
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert!(cfg.peers.is_empty());
    }

    #[test]
    fn config_validate_peer_missing_port() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("host:port"));
    }

    #[test]
    fn config_validate_election_timeout_min_zero() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
election_timeout_min_ms = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("election_timeout_min_ms"));
    }

    #[test]
    fn config_validate_election_timeout_max_le_min() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
election_timeout_min_ms = 300
election_timeout_max_ms = 150
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("election_timeout_max_ms"));
    }

    #[test]
    fn config_validate_election_timeout_max_eq_min() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
election_timeout_min_ms = 200
election_timeout_max_ms = 200
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("election_timeout_max_ms"));
    }

    #[test]
    fn config_validate_zero_fetch_interval() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
fetch_interval_ms = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("fetch_interval_ms"));
    }

    #[test]
    fn config_validate_zero_tick_interval() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
tick_interval_ms = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("tick_interval_ms"));
    }

    #[test]
    fn config_validate_zero_snapshot_interval() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
snapshot_interval = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("snapshot_interval"));
    }

    #[test]
    fn config_validate_zero_max_log_entries() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
max_log_entries_before_compaction = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("max_log_entries_before_compaction"));
    }

    #[test]
    fn config_validate_empty_data_dir() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000"]
data_dir = ""
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("data_dir"));
    }

    /// Helper: parse TOML then apply overrides from a map, then validate.
    fn parse_with_env(
        toml: &str,
        env: &std::collections::HashMap<&str, &str>,
    ) -> Result<ClusterConfig, XRaftError> {
        let mut config: ClusterConfig = toml::from_str(toml)
            .map_err(|e| XRaftError::Config(format!("TOML parse error: {e}")))?;
        let env_owned: std::collections::HashMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        config.apply_env_overrides_with(|key| {
            env_owned
                .get(key)
                .cloned()
                .ok_or(std::env::VarError::NotPresent)
        })?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn config_env_override_node_id() {
        let env = std::collections::HashMap::from([("XRAFT_NODE_ID", "42")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.node_id, NodeId(42));
    }

    #[test]
    fn config_env_override_cluster_id() {
        let env = std::collections::HashMap::from([("XRAFT_CLUSTER_ID", "env-cluster")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.cluster_id, "env-cluster");
    }

    #[test]
    fn config_env_override_listen_addr() {
        let env = std::collections::HashMap::from([("XRAFT_LISTEN_ADDR", "127.0.0.1:9999")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.listen_addr, "127.0.0.1:9999");
    }

    #[test]
    fn config_env_override_peers() {
        let env = std::collections::HashMap::from([("XRAFT_PEERS", "a:1,b:2,c:3")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.peers, vec!["a:1", "b:2", "c:3"]);
    }

    #[test]
    fn config_env_override_peers_filters_empty() {
        let env = std::collections::HashMap::from([("XRAFT_PEERS", "a:1,,  ,b:2,")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.peers, vec!["a:1", "b:2"]);
    }

    #[test]
    fn config_env_override_election_timeouts() {
        let env = std::collections::HashMap::from([
            ("XRAFT_ELECTION_TIMEOUT_MIN_MS", "500"),
            ("XRAFT_ELECTION_TIMEOUT_MAX_MS", "1000"),
        ]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.election_timeout_min_ms, 500);
        assert_eq!(cfg.election_timeout_max_ms, 1000);
    }

    #[test]
    fn config_env_override_data_dir() {
        let env = std::collections::HashMap::from([("XRAFT_DATA_DIR", "/tmp/xraft-data")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/xraft-data"));
    }

    #[test]
    fn config_env_override_fetch_interval() {
        let env = std::collections::HashMap::from([("XRAFT_FETCH_INTERVAL_MS", "75")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.fetch_interval_ms, 75);
    }

    #[test]
    fn config_env_override_validates_after_apply() {
        // Override listen_addr to invalid value → should fail validation.
        let env = std::collections::HashMap::from([("XRAFT_LISTEN_ADDR", "no-port")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("host:port"));
    }

    #[test]
    fn config_env_override_empty_listen_addr_not_applied() {
        // Empty string should not override (apply_env_overrides_with skips empty).
        let env = std::collections::HashMap::from([("XRAFT_LISTEN_ADDR", "")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:6001");
    }

    #[test]
    fn config_invalid_toml_syntax() {
        let result = ClusterConfig::from_toml_str("not valid toml {{{");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("TOML parse error"), "got: {msg}");
    }

    #[test]
    fn config_roundtrip_serialize() {
        let cfg = ClusterConfig::from_toml_str(valid_toml()).unwrap();
        let serialized = toml::to_string(&cfg).unwrap();
        let cfg2 = ClusterConfig::from_toml_str(&serialized).unwrap();
        assert_eq!(cfg.node_id, cfg2.node_id);
        assert_eq!(cfg.cluster_id, cfg2.cluster_id);
        assert_eq!(cfg.peers, cfg2.peers);
    }

    // --- Malformed env override tests ---

    #[test]
    fn config_env_malformed_node_id_returns_error() {
        let env = std::collections::HashMap::from([("XRAFT_NODE_ID", "abc")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_NODE_ID"), "got: {msg}");
        assert!(msg.contains("invalid u64"), "got: {msg}");
    }

    #[test]
    fn config_env_malformed_election_min_returns_error() {
        let env =
            std::collections::HashMap::from([("XRAFT_ELECTION_TIMEOUT_MIN_MS", "not_a_number")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_ELECTION_TIMEOUT_MIN_MS"), "got: {msg}");
    }

    #[test]
    fn config_env_malformed_election_max_returns_error() {
        let env = std::collections::HashMap::from([("XRAFT_ELECTION_TIMEOUT_MAX_MS", "x")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_ELECTION_TIMEOUT_MAX_MS"), "got: {msg}");
    }

    #[test]
    fn config_env_malformed_fetch_interval_returns_error() {
        let env = std::collections::HashMap::from([("XRAFT_FETCH_INTERVAL_MS", "bad")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_FETCH_INTERVAL_MS"), "got: {msg}");
    }

    #[test]
    fn config_env_malformed_tick_interval_returns_error() {
        let env = std::collections::HashMap::from([("XRAFT_TICK_INTERVAL_MS", "nope")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_TICK_INTERVAL_MS"), "got: {msg}");
    }

    #[test]
    fn config_env_malformed_snapshot_interval_returns_error() {
        let env = std::collections::HashMap::from([("XRAFT_SNAPSHOT_INTERVAL", "?!")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_SNAPSHOT_INTERVAL"), "got: {msg}");
    }

    #[test]
    fn config_env_malformed_max_log_entries_returns_error() {
        let env = std::collections::HashMap::from([("XRAFT_MAX_LOG_ENTRIES", "oops")]);
        let result = parse_with_env(valid_toml(), &env);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("XRAFT_MAX_LOG_ENTRIES"), "got: {msg}");
    }

    // --- Additional env override coverage ---

    #[test]
    fn config_env_override_tick_interval() {
        let env = std::collections::HashMap::from([("XRAFT_TICK_INTERVAL_MS", "25")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.tick_interval_ms, 25);
    }

    #[test]
    fn config_env_override_snapshot_interval() {
        let env = std::collections::HashMap::from([("XRAFT_SNAPSHOT_INTERVAL", "5000")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.snapshot_interval, 5000);
    }

    #[test]
    fn config_env_override_max_log_entries() {
        let env = std::collections::HashMap::from([("XRAFT_MAX_LOG_ENTRIES", "200000")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.max_log_entries_before_compaction, 200_000);
    }

    // --- Stronger address validation tests ---

    #[test]
    fn config_validate_listen_addr_non_numeric_port() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:abc"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("non-numeric port"), "got: {msg}");
    }

    #[test]
    fn config_validate_listen_addr_port_zero() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:0"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("out of valid range"), "got: {msg}");
    }

    #[test]
    fn config_validate_listen_addr_port_too_large() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:99999"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("out of valid range"), "got: {msg}");
    }

    #[test]
    fn config_validate_listen_addr_empty_host() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = ":6000"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty host"), "got: {msg}");
    }

    #[test]
    fn config_validate_peer_non_numeric_port() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:abc"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("non-numeric port"), "got: {msg}");
    }

    #[test]
    fn config_validate_peer_empty_host() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = [":6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty host"), "got: {msg}");
    }

    #[test]
    fn config_validate_listen_addr_trailing_colon() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "localhost:"
peers = ["node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty port"), "got: {msg}");
    }

    // --- Peer uniqueness and self-membership validation ---

    #[test]
    fn config_validate_duplicate_peers() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6000", "node2:6001", "node1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate peer"), "got: {msg}");
    }

    #[test]
    fn config_validate_self_in_peers() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:6001", "0.0.0.0:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("must not appear in peers"), "got: {msg}");
    }

    #[test]
    fn config_validate_self_in_peers_wildcard_catches_localhost() {
        // 0.0.0.0 is a wildcard — a peer on 127.0.0.1 with the same port is self.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["127.0.0.1:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("must not appear in peers"), "got: {msg}");
        assert!(msg.contains("127.0.0.1:6000"), "got: {msg}");
    }

    #[test]
    fn config_validate_self_in_peers_wildcard_catches_hostname() {
        // 0.0.0.0 wildcard — any host on the same port is self.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["myhost:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("must not appear in peers"), "got: {msg}");
    }

    #[test]
    fn config_validate_self_in_peers_wildcard_different_port_ok() {
        // Same wildcard host but different port — should be fine.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["127.0.0.1:6001"]
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.peers, vec!["127.0.0.1:6001"]);
    }

    #[test]
    fn config_validate_self_in_peers_non_wildcard_exact_match() {
        // Non-wildcard listen addr — only exact host match is rejected.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "10.0.0.5:6000"
peers = ["10.0.0.5:6000"]
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("must not appear in peers"), "got: {msg}");
    }

    #[test]
    fn config_validate_self_in_peers_non_wildcard_different_host_ok() {
        // Non-wildcard listen addr with different host — should pass.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "10.0.0.5:6000"
peers = ["10.0.0.6:6000"]
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.peers, vec!["10.0.0.6:6000"]);
    }

    // --- Structured voters tests ---

    #[test]
    fn config_with_structured_voters() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000

[[voters]]
node_id = 2
directory_id = "550e8400-e29b-41d4-a716-446655440001"
host = "10.0.0.2"
port = 6000

[[voters]]
node_id = 3
directory_id = "550e8400-e29b-41d4-a716-446655440002"
host = "10.0.0.3"
port = 6000
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.voters.len(), 3);
        let vs = cfg.build_voter_set().unwrap().unwrap();
        assert_eq!(vs.len(), 3);
        assert_eq!(vs.quorum_size(), 2);
    }

    #[test]
    fn config_validate_voter_invalid_uuid() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "not-a-uuid"
host = "10.0.0.1"
port = 6000
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not a valid UUID"), "got: {msg}");
    }

    #[test]
    fn config_validate_voter_duplicate() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.2"
port = 6001
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate voter"), "got: {msg}");
    }

    #[test]
    fn config_validate_voter_empty_host() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = ""
port = 6000
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty host"), "got: {msg}");
    }

    #[test]
    fn config_no_voters_build_returns_none() {
        let cfg = ClusterConfig::from_toml_str(valid_toml()).unwrap();
        let vs = cfg.build_voter_set().unwrap();
        assert!(vs.is_none());
    }

    #[test]
    fn config_validate_voter_nil_uuid_rejected() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "00000000-0000-0000-0000-000000000000"
host = "10.0.0.1"
port = 6000
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("nil UUID"), "got: {msg}");
    }

    #[test]
    fn config_env_override_empty_peers_not_applied() {
        // Empty XRAFT_PEERS should be treated as "not set", preserving TOML peers.
        let env = std::collections::HashMap::from([("XRAFT_PEERS", "")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.peers, vec!["node0:6000", "node2:6002"]);
    }

    #[test]
    fn config_validate_voter_port_zero() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("port must not be zero"), "got: {msg}");
    }

    #[test]
    fn config_validate_voter_duplicate_endpoint() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000

[[voters]]
node_id = 2
directory_id = "550e8400-e29b-41d4-a716-446655440001"
host = "10.0.0.1"
port = 6000
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate voter endpoint"), "got: {msg}");
    }

    #[test]
    fn config_validate_voter_whitespace_host() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "  "
port = 6000
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty host"), "got: {msg}");
    }

    #[test]
    fn config_env_peers_whitespace_only_entries_filtered() {
        let env = std::collections::HashMap::from([("XRAFT_PEERS", "a:1,  , ,b:2")]);
        let cfg = parse_with_env(valid_toml(), &env).unwrap();
        assert_eq!(cfg.peers, vec!["a:1", "b:2"]);
    }

    #[test]
    fn config_load_with_env_integration() {
        // Verify load() works end-to-end with file I/O (env isolation not
        // possible here, but we validate the file-read path is functional).
        let dir = std::env::temp_dir().join("xraft-test-config-load-env");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.toml");
        std::fs::write(&path, valid_toml()).unwrap();

        // load() calls from_toml_str_with_env — it may pick up real env vars,
        // so we only assert on fields that env vars won't override in a typical
        // test environment (cluster_id is rarely set as XRAFT_CLUSTER_ID).
        let result = ClusterConfig::load(&path);
        assert!(result.is_ok(), "load() should succeed: {:?}", result.err());

        std::fs::remove_dir_all(&dir).ok();
    }
}
