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
    /// Maximum number of snapshot files to retain on disk. Older snapshots
    /// beyond this limit are deleted after a new snapshot is saved.
    #[serde(default = "default_snapshot_retention_count")]
    pub snapshot_retention_count: usize,

    // -----------------------------------------------------------------------
    // gRPC transport configuration (Stage 4.1)
    // -----------------------------------------------------------------------
    /// Enable TLS for gRPC traffic. When `true`, both `tls_cert_path` and
    /// `tls_key_path` MUST be provided. When `false` (the default), peers
    /// communicate in plaintext.
    ///
    /// **Security note (v1):** TLS provides on-the-wire encryption only.
    /// It does NOT bind a peer certificate to a Raft `NodeId` (no mutual TLS
    /// in v1 per `tech-spec.md` §2.7); a malicious peer with a valid cert
    /// can still claim arbitrary `candidate_id` / `replica_id`. NodeId trust
    /// is assumed at the cluster-membership boundary.
    #[serde(default)]
    pub tls_enabled: bool,
    /// PEM-encoded server certificate path. Required when `tls_enabled = true`.
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    /// PEM-encoded server private key path. Required when `tls_enabled = true`.
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    /// PEM-encoded CA certificate the *client* uses to verify peer servers.
    /// Optional; when absent, the transport reuses the server's own
    /// `tls_cert_path` as the client-side trust anchor. This makes
    /// `tls_cert_path` + `tls_key_path` sufficient for a single-cert cluster
    /// per the Stage 4.1 brief — provide `tls_ca_path` explicitly when nodes
    /// present distinct certs signed by a shared CA.
    #[serde(default)]
    pub tls_ca_path: Option<PathBuf>,
    /// SNI / TLS server-name override applied by the *client* when connecting
    /// to a peer. Useful when peer endpoints use IP addresses but the cert's
    /// SAN lists a hostname (e.g. `"localhost"` for self-signed test certs).
    #[serde(default)]
    pub tls_domain_name: Option<String>,

    /// Per-RPC connection establishment timeout (milliseconds).
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Per-RPC end-to-end timeout (milliseconds).
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,
    /// Maximum number of retry attempts for unary RPCs (`Vote`, `PreVote`,
    /// `Fetch`). Streaming `FetchSnapshot` only retries the *initial* RPC
    /// invocation, never mid-stream — see `xraft-transport` for details.
    #[serde(default = "default_max_rpc_retries")]
    pub max_rpc_retries: usize,
    /// Initial backoff between retries (milliseconds). Doubles on each
    /// subsequent failure up to `retry_max_backoff_ms`.
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    /// Cap on the exponential backoff between retries (milliseconds).
    #[serde(default = "default_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
    /// Maximum decoded gRPC message size in bytes. Defaults to 64 MiB so
    /// large `FetchResponse` batches and snapshot chunks are not capped by
    /// tonic's default 4 MiB limit. Applied to both client and server.
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Node IDs that participate in the cluster as **observers** —
    /// they replicate the log and answer reads but DO NOT vote in
    /// elections. When this node's `node_id` appears in this list the
    /// engine starts in [`NodeRole::Observer`](crate::types::NodeRole::Observer)
    /// instead of [`NodeRole::Follower`](crate::types::NodeRole::Follower).
    /// Empty by default (a node not in this list participates as a
    /// regular voter / follower per the voters set).
    #[serde(default)]
    pub observers: Vec<u64>,
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
fn default_snapshot_retention_count() -> usize {
    3
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}
fn default_rpc_timeout_ms() -> u64 {
    10_000
}
fn default_max_rpc_retries() -> usize {
    3
}
fn default_retry_initial_backoff_ms() -> u64 {
    100
}
fn default_retry_max_backoff_ms() -> u64 {
    5_000
}
fn default_max_message_size() -> usize {
    64 * 1024 * 1024
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
        if let Ok(val) = env_var("XRAFT_SNAPSHOT_RETENTION_COUNT")
            && !val.is_empty()
        {
            self.snapshot_retention_count = val.parse::<usize>().map_err(|_| {
                XRaftError::Config(format!(
                    "XRAFT_SNAPSHOT_RETENTION_COUNT: invalid usize value '{val}'"
                ))
            })?;
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
            let listen_is_wildcard = matches!(listen_host.as_str(), "0.0.0.0" | "::" | "[::]");
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
                    return Err(XRaftError::Config(format!("voter[{i}] has an empty host")));
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
        if self.snapshot_retention_count == 0 {
            return Err(XRaftError::Config(
                "snapshot_retention_count must be >= 1 (use a positive number to retain N snapshots; default is 3)".into(),
            ));
        }

        // gRPC transport validation (Stage 4.1) ---------------------------
        if self.tls_enabled {
            if self.tls_cert_path.is_none() {
                return Err(XRaftError::Config(
                    "tls_enabled = true requires tls_cert_path to be set".into(),
                ));
            }
            if self.tls_key_path.is_none() {
                return Err(XRaftError::Config(
                    "tls_enabled = true requires tls_key_path to be set".into(),
                ));
            }
        }
        if self.connect_timeout_ms == 0 {
            return Err(XRaftError::Config("connect_timeout_ms must be > 0".into()));
        }
        if self.rpc_timeout_ms == 0 {
            return Err(XRaftError::Config("rpc_timeout_ms must be > 0".into()));
        }
        if self.retry_initial_backoff_ms == 0 {
            return Err(XRaftError::Config(
                "retry_initial_backoff_ms must be > 0".into(),
            ));
        }
        if self.retry_max_backoff_ms < self.retry_initial_backoff_ms {
            return Err(XRaftError::Config(format!(
                "retry_max_backoff_ms ({}) must be >= retry_initial_backoff_ms ({})",
                self.retry_max_backoff_ms, self.retry_initial_backoff_ms
            )));
        }
        if self.max_message_size == 0 {
            return Err(XRaftError::Config("max_message_size must be > 0".into()));
        }
        Ok(())
    }

    /// Resolve a peer's URL from the structured `voters` configuration.
    ///
    /// Returns `Some(url)` for any voter whose `node_id` matches `peer`,
    /// excluding `self.node_id`. The URL scheme is `https://` when
    /// `tls_enabled` is `true`, otherwise `http://`. Returns `None` if no
    /// matching voter is configured.
    ///
    /// **Note:** the flat `peers: Vec<String>` field cannot be used for
    /// outbound RPC routing because it lacks `NodeId` keys. Configure the
    /// `voters` array for any deployment that needs the gRPC transport.
    pub fn peer_endpoint(&self, peer: NodeId) -> Option<String> {
        if peer == self.node_id {
            return None;
        }
        let scheme = if self.tls_enabled { "https" } else { "http" };
        self.voters
            .iter()
            .find(|v| v.node_id == peer.0)
            .map(|v| format!("{scheme}://{}:{}", v.host, v.port))
    }

    /// Build a `NodeId -> URL` map for every configured voter except this
    /// node. Used by the gRPC client to seed its connection pool.
    pub fn peer_endpoints(&self) -> std::collections::HashMap<NodeId, String> {
        let scheme = if self.tls_enabled { "https" } else { "http" };
        self.voters
            .iter()
            .filter(|v| v.node_id != self.node_id.0)
            .map(|v| {
                (
                    NodeId(v.node_id),
                    format!("{scheme}://{}:{}", v.host, v.port),
                )
            })
            .collect()
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
        let vs = VoterSet::try_new(records)
            .map_err(|e| XRaftError::Config(format!("invalid voter set: {e}")))?;
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

// ---------------------------------------------------------------------------
// NodeConfig — top-level TOML schema (Stage 1.2 / Stage 6.1)
// ---------------------------------------------------------------------------

/// Top-level TOML configuration consumed by the `xraft-server`
/// binary. `NodeConfig` wraps the engine-facing [`ClusterConfig`]
/// (via `#[serde(flatten)]` so existing TOML files load unchanged)
/// and adds the small set of node-/server-level knobs that don't
/// belong in the cluster-wide engine config.
///
/// Lifecycle (Stage 6.1 server bootstrap):
///
/// 1. [`NodeConfig::load`] reads + parses the TOML, applies
///    `XRAFT_*` env overrides, then runs [`NodeConfig::validate`].
/// 2. Caller optionally applies CLI overrides (e.g. `--node-id`,
///    `--admin-listen`) and **must** re-run [`NodeConfig::validate`]
///    afterwards.
/// 3. [`NodeConfig::into_cluster_config`] yields the engine-facing
///    [`ClusterConfig`] consumed by `RaftNode::new` and friends.
///    The server-level fields (e.g. [`NodeConfig::admin_listen_addr`])
///    are read directly off `NodeConfig` and projected into the
///    `xraft-server`-only `ServerConfig`.
///
/// The observer list is part of [`ClusterConfig::observers`] (kept
/// alongside the cluster-wide config so it survives
/// [`NodeConfig::into_cluster_config`] and reaches the engine —
/// `RaftNode::new` consults it to seed the local node's initial role
/// as [`NodeRole::Observer`](crate::types::NodeRole::Observer) when
/// applicable, instead of the default
/// [`NodeRole::Follower`](crate::types::NodeRole::Follower)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Inline cluster-wide configuration. `#[serde(flatten)]`
    /// keeps the on-disk TOML shape identical to a plain
    /// [`ClusterConfig`] so existing config files load without
    /// modification. The observer list lives on
    /// [`ClusterConfig::observers`] (deserialised from the same
    /// top-level `observers = [...]` TOML key thanks to flatten).
    #[serde(flatten)]
    pub cluster: ClusterConfig,
    /// Optional `host:port` for the admin HTTP listener that serves
    /// `/health` and `/metrics`. Server-only — the consensus engine
    /// does not consult it. CLI `--admin-listen` overrides this
    /// value at startup; the default
    /// (`xraft_server::server::DEFAULT_ADMIN_LISTEN_ADDR`) applies
    /// when neither is set.
    ///
    /// Not hot-reloadable: changes here on a SIGHUP reload are
    /// logged-and-ignored — restart the process to move the admin
    /// listener (see `xraft-server/src/main.rs::reload_config`).
    #[serde(default)]
    pub admin_listen_addr: Option<String>,
}

impl NodeConfig {
    /// Load + parse + env-override + validate from a TOML file.
    ///
    /// Convenience wrapper for the production server bootstrap
    /// path. Equivalent to reading the file and calling
    /// [`NodeConfig::from_toml_str_with_env`].
    pub fn load(path: &Path) -> Result<Self, XRaftError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| XRaftError::Config(format!("failed to read {}: {e}", path.display())))?;
        Self::from_toml_str_with_env(&content)
    }

    /// Parse + validate without applying env overrides.
    pub fn from_toml_str(s: &str) -> Result<Self, XRaftError> {
        let cfg: NodeConfig =
            toml::from_str(s).map_err(|e| XRaftError::Config(format!("TOML parse error: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse, apply `XRAFT_*` env overrides, then validate.
    pub fn from_toml_str_with_env(s: &str) -> Result<Self, XRaftError> {
        let mut cfg: NodeConfig =
            toml::from_str(s).map_err(|e| XRaftError::Config(format!("TOML parse error: {e}")))?;
        cfg.apply_env_overrides()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Apply `XRAFT_*` env-var overrides. Delegates cluster-level
    /// fields to [`ClusterConfig::apply_env_overrides`] and reads
    /// `XRAFT_OBSERVERS` (comma-separated `u64` list) for the
    /// observer roster.
    pub fn apply_env_overrides(&mut self) -> Result<(), XRaftError> {
        self.cluster.apply_env_overrides()?;
        if let Ok(val) = std::env::var("XRAFT_OBSERVERS")
            && !val.is_empty()
        {
            let mut parsed = Vec::with_capacity(4);
            for part in val.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let id: u64 = trimmed.parse().map_err(|_| {
                    XRaftError::Config(format!("XRAFT_OBSERVERS: invalid u64 value '{trimmed}'"))
                })?;
                parsed.push(id);
            }
            self.cluster.observers = parsed;
        }
        Ok(())
    }

    /// Cluster-level validation **plus** node-membership rules.
    /// Idempotent: callers that mutate `self` after [`load`] (e.g.
    /// a CLI `--node-id` override in `main.rs`) **must** re-call
    /// `validate()` to verify the override is still consistent.
    pub fn validate(&self) -> Result<(), XRaftError> {
        self.cluster.validate()?;
        self.validate_membership()?;
        Ok(())
    }

    /// Verify the local `node_id` is a recognised cluster member.
    ///
    /// Acceptance rules:
    ///
    /// - `voters` **must be non-empty** — every cluster MUST define
    ///   at least one structured `[[voters]]` entry, even a single-
    ///   node cluster. The legacy flat `peers` list cannot stand in
    ///   because it lacks `NodeId` keys, and an empty voter set
    ///   leaves [`ClusterConfig::build_voter_set`] returning `None`
    ///   so `RaftNode::has_election_quorum` would always return
    ///   `false` — i.e. the accepted config could never elect a
    ///   leader.
    /// - `node_id` MUST appear in exactly one of `voters[].node_id`
    ///   or `observers[]`. Appearing in both is rejected as a
    ///   configuration error.
    pub fn validate_membership(&self) -> Result<(), XRaftError> {
        let self_id = self.cluster.node_id.0;
        if self.cluster.voters.is_empty() {
            return Err(XRaftError::Config(format!(
                "configuration MUST specify at least one structured `[[voters]]` entry \
                 (even for a single-node cluster) — empty voters leaves the engine with \
                 no quorum metadata and it can never elect a leader. Add a [[voters]] \
                 block for node_id = {self_id} pointing at this node's listen_addr."
            )));
        }
        let in_voters = self.cluster.voters.iter().any(|v| v.node_id == self_id);
        let in_observers = self.cluster.observers.contains(&self_id);
        if !in_voters && !in_observers {
            let voter_ids: Vec<u64> = self.cluster.voters.iter().map(|v| v.node_id).collect();
            return Err(XRaftError::Config(format!(
                "node_id {self_id} is not present in the voters list or observers list \
                 (voters = {voter_ids:?}, observers = {observers:?}); each node MUST be \
                 a member of exactly one set",
                observers = self.cluster.observers,
            )));
        }
        if in_voters && in_observers {
            return Err(XRaftError::Config(format!(
                "node_id {self_id} appears in BOTH the voters and observers lists; \
                 each node MUST be a member of exactly one set"
            )));
        }
        Ok(())
    }

    /// Borrow the inner [`ClusterConfig`] without consuming.
    pub fn cluster_config(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Borrow the observer list (lives on [`ClusterConfig::observers`]).
    pub fn observers(&self) -> &[u64] {
        &self.cluster.observers
    }

    /// Consume `self` and return the engine-facing
    /// [`ClusterConfig`]. The observer set is preserved on the
    /// returned [`ClusterConfig`] so the engine can seed
    /// [`NodeRole::Observer`](crate::types::NodeRole::Observer)
    /// when the local `node_id` is in the observer list (see
    /// `RaftNode::new`).
    pub fn into_cluster_config(self) -> ClusterConfig {
        self.cluster
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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
peers = ["node1:7000"]
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

    // ---------------------------------------------------------------------
    // Stage 4.1 transport-config tests
    // ---------------------------------------------------------------------

    #[test]
    fn config_transport_defaults_applied() {
        // No transport keys set in TOML — every transport field should fall
        // back to its `default_*` value (or `None` for the optional fields).
        let cfg = ClusterConfig::from_toml_str(valid_toml()).unwrap();
        assert!(!cfg.tls_enabled, "tls_enabled defaults to false");
        assert!(cfg.tls_cert_path.is_none());
        assert!(cfg.tls_key_path.is_none());
        assert!(cfg.tls_ca_path.is_none());
        assert!(cfg.tls_domain_name.is_none());
        assert_eq!(cfg.connect_timeout_ms, 5_000);
        assert_eq!(cfg.rpc_timeout_ms, 10_000);
        assert_eq!(cfg.max_rpc_retries, 3);
        assert_eq!(cfg.retry_initial_backoff_ms, 100);
        assert_eq!(cfg.retry_max_backoff_ms, 5_000);
        assert_eq!(cfg.max_message_size, 64 * 1024 * 1024);
    }

    #[test]
    fn config_tls_enabled_missing_cert_path_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
tls_enabled = true
tls_key_path = "/tmp/k.pem"
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("tls_cert_path"), "got: {msg}");
    }

    #[test]
    fn config_tls_enabled_missing_key_path_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
tls_enabled = true
tls_cert_path = "/tmp/c.pem"
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("tls_key_path"), "got: {msg}");
    }

    #[test]
    fn config_tls_cert_and_key_only_validates() {
        // Brief contract: cert + key alone is a valid, complete TLS config.
        // No CA path is required.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
tls_enabled = true
tls_cert_path = "/tmp/c.pem"
tls_key_path = "/tmp/k.pem"
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert!(cfg.tls_enabled);
        assert_eq!(cfg.tls_cert_path.unwrap(), PathBuf::from("/tmp/c.pem"));
        assert_eq!(cfg.tls_key_path.unwrap(), PathBuf::from("/tmp/k.pem"));
        assert!(
            cfg.tls_ca_path.is_none(),
            "CA path not required when cert+key suffice"
        );
    }

    #[test]
    fn config_tls_disabled_defaults_ok() {
        // Sanity: when tls_enabled is false, missing cert/key paths must NOT
        // produce a validation error.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
tls_enabled = false
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert!(!cfg.tls_enabled);
    }

    #[test]
    fn config_validate_zero_connect_timeout_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
connect_timeout_ms = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("connect_timeout_ms"));
    }

    #[test]
    fn config_validate_zero_rpc_timeout_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
rpc_timeout_ms = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("rpc_timeout_ms"));
    }

    #[test]
    fn config_validate_zero_retry_initial_backoff_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
retry_initial_backoff_ms = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("retry_initial_backoff_ms"));
    }

    #[test]
    fn config_validate_retry_max_lt_initial_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
retry_initial_backoff_ms = 500
retry_max_backoff_ms = 100
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("retry_max_backoff_ms"), "got: {msg}");
    }

    #[test]
    fn config_validate_zero_max_message_size_errors() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
peers = ["node1:7000"]
max_message_size = 0
"#;
        let err = ClusterConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("max_message_size"));
    }

    #[test]
    fn config_peer_endpoint_http_scheme_when_tls_disabled() {
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
port = 6001
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        let url = cfg.peer_endpoint(NodeId(2)).unwrap();
        assert_eq!(url, "http://10.0.0.2:6001");
    }

    #[test]
    fn config_peer_endpoint_https_scheme_when_tls_enabled() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
tls_enabled = true
tls_cert_path = "/tmp/c.pem"
tls_key_path = "/tmp/k.pem"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000

[[voters]]
node_id = 2
directory_id = "550e8400-e29b-41d4-a716-446655440001"
host = "10.0.0.2"
port = 6001
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        let url = cfg.peer_endpoint(NodeId(2)).unwrap();
        assert_eq!(url, "https://10.0.0.2:6001");
    }

    #[test]
    fn config_peer_endpoint_returns_none_for_self() {
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
port = 6001
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert!(
            cfg.peer_endpoint(NodeId(1)).is_none(),
            "self has no peer URL"
        );
    }

    #[test]
    fn config_peer_endpoint_returns_none_for_unknown_peer() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert!(
            cfg.peer_endpoint(NodeId(99)).is_none(),
            "unknown peer id returns None"
        );
    }

    #[test]
    fn config_peer_endpoints_excludes_self_and_uses_correct_scheme() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "0.0.0.0:6000"
tls_enabled = true
tls_cert_path = "/tmp/c.pem"
tls_key_path = "/tmp/k.pem"

[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000

[[voters]]
node_id = 2
directory_id = "550e8400-e29b-41d4-a716-446655440001"
host = "10.0.0.2"
port = 6001

[[voters]]
node_id = 3
directory_id = "550e8400-e29b-41d4-a716-446655440002"
host = "10.0.0.3"
port = 6002
"#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        let map = cfg.peer_endpoints();
        assert_eq!(map.len(), 2, "self is excluded");
        assert!(!map.contains_key(&NodeId(1)), "self not present");
        assert_eq!(map.get(&NodeId(2)).unwrap(), "https://10.0.0.2:6001");
        assert_eq!(map.get(&NodeId(3)).unwrap(), "https://10.0.0.3:6002");
    }

    // -----------------------------------------------------------------------
    // NodeConfig tests (Stage 6.1)
    // -----------------------------------------------------------------------

    #[test]
    fn node_config_round_trips_existing_cluster_toml_unchanged() {
        // A `full_toml()`-shaped config (engine-only fields) plus a
        // single `[[voters]]` block for the local node still loads
        // as `NodeConfig` and yields the same engine-facing
        // `ClusterConfig` via `into_cluster_config()`. `[[voters]]`
        // is now required (see `validate_membership`).
        let toml = format!(
            r#"{full}
[[voters]]
node_id = 2
directory_id = "550e8400-e29b-41d4-a716-446655440002"
host = "10.0.0.2"
port = 7000
"#,
            full = full_toml()
        );
        let cfg = NodeConfig::from_toml_str(&toml).expect("must parse");
        assert!(
            cfg.cluster.observers.is_empty(),
            "default observers is empty"
        );
        assert!(
            cfg.admin_listen_addr.is_none(),
            "admin_listen_addr is optional, defaults to None"
        );
        let cluster = cfg.into_cluster_config();
        assert_eq!(cluster.node_id, NodeId(2));
        assert_eq!(cluster.cluster_id, "prod-cluster");
        assert_eq!(cluster.listen_addr, "10.0.0.2:7000");
    }

    #[test]
    fn node_config_admin_listen_addr_round_trip_from_toml() {
        // Operator-supplied `admin_listen_addr` in the TOML lands
        // on `NodeConfig` (server-only field; engine `ClusterConfig`
        // never sees it). The default is `None` (the binary then
        // falls back to `DEFAULT_ADMIN_LISTEN_ADDR`).
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "10.0.0.1:6000"
admin_listen_addr = "0.0.0.0:9001"
[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000
"#;
        let cfg = NodeConfig::from_toml_str(toml).expect("must parse");
        assert_eq!(
            cfg.admin_listen_addr.as_deref(),
            Some("0.0.0.0:9001"),
            "admin_listen_addr deserialises off the top-level TOML key"
        );
    }

    #[test]
    fn node_config_flatten_works_with_voters_array() {
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "10.0.0.1:6000"
observers = [4, 5]
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
"#;
        let cfg = NodeConfig::from_toml_str(toml).expect("must parse");
        assert_eq!(cfg.cluster.observers, vec![4, 5]);
        assert_eq!(cfg.cluster.voters.len(), 2);
        assert_eq!(cfg.cluster.voters[0].host, "10.0.0.1");
    }

    #[test]
    fn node_config_rejects_empty_voters() {
        // Stage 6.1 hardening: empty `voters` was previously accepted
        // as an "implicit single-node bootstrap", but that left the
        // engine with no `voter_set`, so `has_election_quorum` always
        // returned false and the cluster could never elect a leader.
        // The contract is now: every cluster MUST declare at least one
        // structured `[[voters]]` entry (even single-node clusters).
        let toml = r#"
node_id = 99
cluster_id = "c"
listen_addr = "127.0.0.1:6000"
"#;
        let err = NodeConfig::from_toml_str(toml).expect_err("empty voters must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("[[voters]]"),
            "error must guide operator to add [[voters]]: {msg}"
        );
        assert!(
            msg.contains("at least one"),
            "error must explain the at-least-one rule: {msg}"
        );
    }

    #[test]
    fn node_config_rejects_observer_only_without_voters() {
        // Observers cannot stand in for voters because an observer-
        // only cluster has nobody who can win an election. Configuring
        // `observers = [...]` without `[[voters]]` is rejected for the
        // same reason as empty voters/observers.
        let toml = r#"
node_id = 5
cluster_id = "c"
listen_addr = "127.0.0.1:6000"
observers = [5]
"#;
        let err = NodeConfig::from_toml_str(toml)
            .expect_err("observer-only (no voters) must be rejected");
        assert!(format!("{err}").contains("[[voters]]"));
    }

    #[test]
    fn node_config_membership_rejects_node_outside_voter_set() {
        let toml = r#"
node_id = 99
cluster_id = "c"
listen_addr = "10.0.0.9:6000"
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
"#;
        let err = NodeConfig::from_toml_str(toml).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("node_id 99"),
            "error must name the missing node_id: {msg}"
        );
        assert!(
            msg.contains("not present"),
            "error must say not present: {msg}"
        );
    }

    #[test]
    fn node_config_membership_accepts_node_in_voter_set() {
        let toml = r#"
node_id = 2
cluster_id = "c"
listen_addr = "10.0.0.2:6000"
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
"#;
        NodeConfig::from_toml_str(toml).expect("voter membership valid");
    }

    #[test]
    fn node_config_membership_accepts_node_in_observer_set() {
        let toml = r#"
node_id = 5
cluster_id = "c"
listen_addr = "10.0.0.5:6000"
observers = [5, 6]
[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000
"#;
        let cfg = NodeConfig::from_toml_str(toml).expect("observer membership valid");
        assert_eq!(cfg.cluster.observers, vec![5, 6]);
    }

    #[test]
    fn node_config_membership_rejects_both_voter_and_observer() {
        // A node MUST NOT be a voter AND an observer simultaneously.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "10.0.0.1:6000"
observers = [1]
[[voters]]
node_id = 1
directory_id = "550e8400-e29b-41d4-a716-446655440000"
host = "10.0.0.1"
port = 6000
"#;
        let err = NodeConfig::from_toml_str(toml).unwrap_err();
        assert!(format!("{err}").contains("BOTH"));
    }

    #[test]
    fn node_config_into_cluster_config_preserves_observers() {
        // Observers now live on ClusterConfig directly, so the
        // conversion is lossless. The engine consults
        // `cluster.observers` to seed `NodeRole::Observer` when
        // applicable (see `RaftNode::new_inner`).
        let mut cluster = ClusterConfig::from_toml_str(valid_toml()).unwrap();
        cluster.observers = vec![10, 11];
        // Add a [[voters]] entry programmatically — the tightened
        // `validate_membership` requires at least one voter and we
        // need the local node_id to be a member somewhere.
        cluster.voters = vec![VoterConfig {
            node_id: cluster.node_id.0,
            directory_id: "550e8400-e29b-41d4-a716-446655440003".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6001,
        }];
        let cfg = NodeConfig {
            cluster,
            admin_listen_addr: None,
        };
        cfg.validate().expect("populated cfg must validate");
        let cluster = cfg.into_cluster_config();
        assert_eq!(cluster.node_id, NodeId(1));
        assert_eq!(cluster.observers, vec![10, 11]);
    }

    #[test]
    fn node_config_validate_re_runs_membership_after_mutation() {
        // Models the main.rs CLI `--node-id` override path:
        // load, mutate node_id, re-validate.
        let toml = r#"
node_id = 1
cluster_id = "c"
listen_addr = "10.0.0.1:6000"
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
"#;
        let mut cfg = NodeConfig::from_toml_str(toml).expect("initial valid");
        // Override node_id to a non-member ⇒ re-validate must error.
        cfg.cluster.node_id = NodeId(7);
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err}").contains("node_id 7"));
        // Override to a valid voter ⇒ re-validate must succeed.
        cfg.cluster.node_id = NodeId(2);
        cfg.validate().expect("override to valid voter must pass");
    }
}
