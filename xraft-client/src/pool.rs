//! Peer-RPC `ConnectionPool` for the XRAFT server (Stage 6.1).
//!
//! The pool is a thin façade over the canonical
//! [`RaftGrpcClient`](xraft_transport::grpc_client::RaftGrpcClient)
//! that ships with `xraft-transport`. The transport already maintains
//! the per-peer channel cache + connect-mutex set; the pool's job is
//! to:
//!
//! - Hold a shared [`Arc<RaftGrpcClient>`] that the
//!   [`GrpcTransport`](xraft_transport::grpc::GrpcTransport)
//!   instance **also** uses for outbound peer RPCs, so there is a
//!   single channel cache per process — not two competing pools.
//! - Expose the resolved `NodeId → URL` map so the admin surface
//!   (and tests) can see who the node is configured to talk to
//!   without re-deriving from [`ClusterConfig`].
//! - Provide a stable, server-bootstrap-visible component to
//!   satisfy Stage 6.1 of the implementation plan: "initialise the
//!   `ConnectionPool` for peer RPCs".
//!
//! ### Lifecycle
//!
//! ```ignore
//! // Stage 6.1 server bootstrap (xraft-server/src/server.rs):
//! let pool = ConnectionPool::from_cluster_config(&cluster)?;
//! let transport = GrpcTransport::with_client(
//!     grpc_cfg,
//!     inbound_handler,
//!     pool.client(),       // <-- shared client
//! );
//! ```
//!
//! Cloning the pool is cheap (one `Arc` bump). Drop the last clone
//! to release the underlying channels; the gRPC transport's
//! shutdown path is independent and does not need a pool drop.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use xraft_core::config::ClusterConfig;
use xraft_core::error::Result as XResult;
use xraft_core::types::NodeId;
use xraft_transport::grpc::TlsTransportConfig;
use xraft_transport::grpc_client::{RaftGrpcClient, RaftGrpcClientConfig};

/// Shared peer-RPC connection pool.
///
/// Holds the shared `Arc<RaftGrpcClient>` plus the resolved
/// `NodeId → URL` roster (sourced from
/// [`ClusterConfig::peer_endpoints`]).
///
/// Construct via [`ConnectionPool::from_cluster_config`] in the
/// server bootstrap path; clone (cheap — Arc bump) anywhere the
/// pool needs to be observed or used for outbound RPCs.
#[derive(Clone)]
pub struct ConnectionPool {
    client: Arc<RaftGrpcClient>,
    peer_endpoints: HashMap<NodeId, String>,
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("peer_count", &self.peer_endpoints.len())
            .field("peers", &self.peer_endpoints.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ConnectionPool {
    /// Build a pool directly from an [`Arc<RaftGrpcClient>`] and a
    /// pre-resolved peer roster. Used by
    /// [`ConnectionPool::from_cluster_config`] and by tests.
    pub fn new(client: Arc<RaftGrpcClient>, peer_endpoints: HashMap<NodeId, String>) -> Self {
        Self {
            client,
            peer_endpoints,
        }
    }

    /// Build a pool from the canonical [`ClusterConfig`]. Resolves
    /// TLS material from `cluster.tls_*` when TLS is enabled, and
    /// converts `cluster.connect_timeout_ms` /
    /// `cluster.rpc_timeout_ms` / retry knobs into the
    /// `RaftGrpcClientConfig` shape the transport expects.
    ///
    /// The returned pool's [`Arc<RaftGrpcClient>`] can be shared
    /// with [`GrpcTransport::with_client`](xraft_transport::grpc::GrpcTransport::with_client)
    /// so the inbound transport and the operator-visible pool
    /// observe the SAME peer channel cache.
    pub fn from_cluster_config(cluster: &ClusterConfig) -> XResult<Self> {
        let tls = if cluster.tls_enabled {
            Some(Arc::new(TlsTransportConfig::from_cluster_config(cluster)?))
        } else {
            None
        };
        let peer_endpoints = cluster.peer_endpoints();
        let client_cfg = RaftGrpcClientConfig {
            peer_endpoints: peer_endpoints.clone(),
            connect_timeout: Duration::from_millis(cluster.connect_timeout_ms),
            rpc_timeout: Duration::from_millis(cluster.rpc_timeout_ms),
            max_retries: cluster.max_rpc_retries,
            initial_backoff: Duration::from_millis(cluster.retry_initial_backoff_ms),
            max_backoff: Duration::from_millis(cluster.retry_max_backoff_ms),
            max_message_size: cluster.max_message_size,
            tls,
        };
        Ok(Self {
            client: Arc::new(RaftGrpcClient::new(client_cfg)),
            peer_endpoints,
        })
    }

    /// Borrow the shared outbound client. Pass to
    /// [`GrpcTransport::with_client`](xraft_transport::grpc::GrpcTransport::with_client)
    /// to share the per-peer channel cache between this pool and
    /// the server's inbound transport instance.
    pub fn client(&self) -> Arc<RaftGrpcClient> {
        self.client.clone()
    }

    /// Borrow the resolved peer roster.
    pub fn peer_endpoints(&self) -> &HashMap<NodeId, String> {
        &self.peer_endpoints
    }

    /// Number of peers the pool was configured with (excludes
    /// `self`, since [`ClusterConfig::peer_endpoints`] filters it).
    pub fn len(&self) -> usize {
        self.peer_endpoints.len()
    }

    /// Convenience predicate: `true` iff no peers are configured
    /// (single-node bootstrap deployment).
    pub fn is_empty(&self) -> bool {
        self.peer_endpoints.is_empty()
    }

    /// Look up a single peer's URL by `NodeId`. Returns `None`
    /// when the peer is not in the configured roster (e.g.
    /// querying for `self.node_id`).
    pub fn endpoint_for(&self, peer: NodeId) -> Option<&str> {
        self.peer_endpoints.get(&peer).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::config::{ClusterConfig, VoterConfig};

    fn three_node_cluster() -> ClusterConfig {
        ClusterConfig {
            node_id: NodeId(1),
            cluster_id: "c".into(),
            listen_addr: "10.0.0.1:6000".into(),
            peers: vec![],
            voters: vec![
                VoterConfig {
                    node_id: 1,
                    directory_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                    host: "10.0.0.1".into(),
                    port: 6000,
                },
                VoterConfig {
                    node_id: 2,
                    directory_id: "550e8400-e29b-41d4-a716-446655440001".into(),
                    host: "10.0.0.2".into(),
                    port: 6000,
                },
                VoterConfig {
                    node_id: 3,
                    directory_id: "550e8400-e29b-41d4-a716-446655440002".into(),
                    host: "10.0.0.3".into(),
                    port: 6000,
                },
            ],
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            tick_interval_ms: 10,
            snapshot_interval: 10_000,
            max_log_entries_before_compaction: 100_000,
            data_dir: "data".into(),
            snapshot_retention_count: 3,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_domain_name: None,
            connect_timeout_ms: 5_000,
            rpc_timeout_ms: 10_000,
            max_rpc_retries: 3,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 5_000,
            max_message_size: 64 * 1024 * 1024,
            observers: vec![],
        }
    }

    #[test]
    fn from_cluster_config_excludes_self() {
        let cluster = three_node_cluster();
        let pool = ConnectionPool::from_cluster_config(&cluster).expect("pool builds");
        assert_eq!(pool.len(), 2, "3 voters minus self = 2 peers");
        assert!(pool.endpoint_for(NodeId(1)).is_none(), "self excluded");
        assert_eq!(
            pool.endpoint_for(NodeId(2)).unwrap(),
            "http://10.0.0.2:6000"
        );
        assert_eq!(
            pool.endpoint_for(NodeId(3)).unwrap(),
            "http://10.0.0.3:6000"
        );
    }

    #[test]
    fn empty_pool_for_single_node_bootstrap() {
        let mut cluster = three_node_cluster();
        cluster.voters.clear();
        let pool = ConnectionPool::from_cluster_config(&cluster).expect("pool builds");
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn clone_shares_underlying_client_arc() {
        let cluster = three_node_cluster();
        let pool_a = ConnectionPool::from_cluster_config(&cluster).expect("pool builds");
        let pool_b = pool_a.clone();
        // Both clones hold pointers to the SAME underlying client —
        // the channel cache is shared so concurrent RPCs through
        // either clone reuse the same TCP/TLS handshake.
        assert!(
            Arc::ptr_eq(&pool_a.client(), &pool_b.client()),
            "clone must share the underlying Arc<RaftGrpcClient>"
        );
    }

    #[test]
    fn debug_includes_peer_count() {
        let cluster = three_node_cluster();
        let pool = ConnectionPool::from_cluster_config(&cluster).expect("pool builds");
        let dbg = format!("{pool:?}");
        assert!(
            dbg.contains("peer_count: 2"),
            "Debug should include peer_count: {dbg}"
        );
    }
}
