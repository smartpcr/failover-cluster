//! Composite gRPC transport: bundles the inbound server and outbound
//! client into a single object implementing
//! [`xraft_core::transport::Transport`].
//!
//! Construction flow:
//! 1. Build a [`GrpcTransportConfig`] (helper: `from_cluster_config`).
//! 2. Build a [`RaftMessageHandler`] (typically a Stage 4.2 driver).
//! 3. `GrpcTransport::new(cfg, handler)` -> `Arc<GrpcTransport<H>>`.
//! 4. `tokio::spawn(transport.clone().start_server())` to serve inbound RPCs.
//! 5. Use `transport.send_*` to fan out to peers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{debug, info};

use xraft_core::config::ClusterConfig;
use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotRequest, PreVoteRequest, PreVoteResponse,
    VoteRequest, VoteResponse,
};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream, Transport};
use xraft_core::types::NodeId;

use crate::grpc_client::{RaftGrpcClient, RaftGrpcClientConfig};
use crate::grpc_server::RaftGrpcServer;

/// Materialised TLS configuration: PEM bytes + CA trust anchor + SNI override.
///
/// Built from `ClusterConfig.tls_*` paths; PEM files are read once at
/// transport construction so subsequent reconnects do not re-touch the
/// filesystem.
///
/// # Single-cert clusters
///
/// Per the workstream brief, supplying just `tls_cert_path` + `tls_key_path`
/// is sufficient for a working TLS-enabled cluster. When `tls_ca_path` is
/// omitted, [`TlsTransportConfig::from_cluster_config`] reuses the server's
/// own cert as the client-side trust anchor — i.e. every node in the cluster
/// presents the SAME cert and trusts the SAME cert. This matches typical
/// homelab / dev deployments where one self-signed cert is shared across the
/// quorum. Provide `tls_ca_path` explicitly when nodes use distinct certs
/// signed by a shared CA.
#[derive(Debug, Clone)]
pub struct TlsTransportConfig {
    /// Server-side certificate (PEM-encoded).
    pub server_cert_pem: Vec<u8>,
    /// Server-side private key (PEM-encoded).
    pub server_key_pem: Vec<u8>,
    /// CA certificate the *client* uses to verify peer servers (PEM-encoded).
    /// Defaults to the server's own cert when [`ClusterConfig::tls_ca_path`]
    /// is unset; see the type-level docs for the rationale.
    pub ca_cert_pem: Option<Vec<u8>>,
    /// SNI / TLS server-name override applied by the client.
    pub domain_name: Option<String>,
}

impl TlsTransportConfig {
    /// Read TLS PEM material from the paths in `ClusterConfig`.
    ///
    /// Falls back to using `tls_cert_path` as the client's CA trust anchor
    /// when `tls_ca_path` is not configured — see the [`TlsTransportConfig`]
    /// doc comment for the single-cert-per-cluster rationale.
    pub fn from_cluster_config(cfg: &ClusterConfig) -> XResult<Self> {
        let cert_path = cfg
            .tls_cert_path
            .as_ref()
            .ok_or_else(|| XRaftError::Config("tls_enabled but tls_cert_path not set".into()))?;
        let key_path = cfg
            .tls_key_path
            .as_ref()
            .ok_or_else(|| XRaftError::Config("tls_enabled but tls_key_path not set".into()))?;
        let server_cert_pem = std::fs::read(cert_path).map_err(|e| {
            XRaftError::Config(format!(
                "failed to read tls_cert_path '{}': {e}",
                cert_path.display()
            ))
        })?;
        let server_key_pem = std::fs::read(key_path).map_err(|e| {
            XRaftError::Config(format!(
                "failed to read tls_key_path '{}': {e}",
                key_path.display()
            ))
        })?;
        // When tls_ca_path is unset, fall back to the server's own cert as
        // the trust anchor. This makes cert+key alone sufficient for a
        // working cluster per the workstream brief.
        let ca_cert_pem = match &cfg.tls_ca_path {
            Some(p) => Some(std::fs::read(p).map_err(|e| {
                XRaftError::Config(format!("failed to read tls_ca_path '{}': {e}", p.display()))
            })?),
            None => Some(server_cert_pem.clone()),
        };
        Ok(Self {
            server_cert_pem,
            server_key_pem,
            ca_cert_pem,
            domain_name: cfg.tls_domain_name.clone(),
        })
    }
}

/// Configuration for the composite gRPC transport.
///
/// Build via `from_cluster_config(&ClusterConfig)` to derive every field
/// from the canonical config; or construct manually for tests.
#[derive(Debug, Clone)]
pub struct GrpcTransportConfig {
    /// Address the inbound server binds to (e.g. `"0.0.0.0:6000"`).
    pub listen_addr: String,
    /// Map of `NodeId -> peer URL` for outbound RPCs.
    pub peer_endpoints: HashMap<NodeId, String>,
    /// Per-RPC connection timeout.
    pub connect_timeout: Duration,
    /// Per-RPC end-to-end timeout (each retry attempt has this budget).
    pub rpc_timeout: Duration,
    /// Maximum unary-RPC retry attempts after a transient failure.
    pub max_retries: usize,
    /// Initial exponential-backoff delay.
    pub retry_initial_backoff: Duration,
    /// Cap on the exponential-backoff delay.
    pub retry_max_backoff: Duration,
    /// Maximum decoded gRPC message size (default 64 MiB).
    pub max_message_size: usize,
    /// TLS material; `None` = plaintext HTTP/2.
    pub tls: Option<Arc<TlsTransportConfig>>,
}

impl GrpcTransportConfig {
    /// Derive a transport config from the canonical `ClusterConfig`.
    ///
    /// Returns an error when the user has configured the legacy flat
    /// `peers: Vec<String>` field but left the structured `voters` field
    /// empty; see [`peer_endpoints_from_cluster_config`] for the
    /// rationale.
    pub fn from_cluster_config(cfg: &ClusterConfig) -> XResult<Self> {
        let peer_endpoints = peer_endpoints_from_cluster_config(cfg)?;
        let tls = if cfg.tls_enabled {
            Some(Arc::new(TlsTransportConfig::from_cluster_config(cfg)?))
        } else {
            None
        };
        Ok(Self {
            listen_addr: cfg.listen_addr.clone(),
            peer_endpoints,
            connect_timeout: Duration::from_millis(cfg.connect_timeout_ms),
            rpc_timeout: Duration::from_millis(cfg.rpc_timeout_ms),
            max_retries: cfg.max_rpc_retries,
            retry_initial_backoff: Duration::from_millis(cfg.retry_initial_backoff_ms),
            retry_max_backoff: Duration::from_millis(cfg.retry_max_backoff_ms),
            max_message_size: cfg.max_message_size,
            tls,
        })
    }
}

/// Resolve a `NodeId -> URL` routing map from the canonical
/// [`ClusterConfig`], or return an actionable config error when the
/// caller has populated the legacy flat `peers: Vec<String>` field but
/// left the structured `voters` field empty.
///
/// `ClusterConfig::peer_endpoints` derives its `NodeId` keys from
/// `cluster.voters`, so a config with `peers` populated but `voters`
/// empty silently produces an empty map — and any subsequent
/// `send_*` call would later fail with "no endpoint configured for
/// peer" rather than at construction time. Surfacing the misconfig
/// here lets operators fix the deployment before the gRPC transport
/// is wired into the consensus loop. Shared by both
/// [`GrpcTransportConfig::from_cluster_config`] and
/// `xraft_client::ConnectionPool::from_cluster_config` so the two
/// helpers stay in lockstep on the contract.
///
/// Legitimate single-node bootstrap (`peers` empty AND `voters`
/// empty / self-only) returns `Ok(<empty map>)` — the transport will
/// simply have no outbound peers.
pub fn peer_endpoints_from_cluster_config(cfg: &ClusterConfig) -> XResult<HashMap<NodeId, String>> {
    if cfg.voters.is_empty() && !cfg.peers.is_empty() {
        return Err(XRaftError::Config(format!(
            "ClusterConfig.peers (legacy host:port list, {} entr{}) cannot be used for gRPC \
             outbound routing because it lacks NodeId keys; populate ClusterConfig.voters with \
             VoterConfig entries (one per cluster member including this node) and re-run. \
             See ClusterConfig::peer_endpoints docs for the supported routing model.",
            cfg.peers.len(),
            if cfg.peers.len() == 1 { "y" } else { "ies" }
        )));
    }
    Ok(cfg.peer_endpoints())
}

/// gRPC implementation of [`Transport`].
///
/// The struct owns:
/// - the outbound [`RaftGrpcClient`] (handles connection pool + retries),
///   shared as `Arc<RaftGrpcClient>` so callers (e.g.
///   [`xraft_client::pool::ConnectionPool`](../../xraft_client/pool/struct.ConnectionPool.html))
///   can hold the SAME pool instance,
/// - a shared [`Notify`] used to signal graceful server shutdown,
/// - the [`RaftMessageHandler`] dispatched into by the inbound server.
///
/// Wrap in `Arc` and call `start_server()` to serve; calls to `send_*`
/// can use any cheap clone of the `Arc`.
#[derive(Debug)]
pub struct GrpcTransport<H: RaftMessageHandler> {
    config: GrpcTransportConfig,
    handler: Arc<H>,
    client: Arc<RaftGrpcClient>,
    shutdown: Arc<Notify>,
}

impl<H: RaftMessageHandler> GrpcTransport<H> {
    /// Construct a new gRPC transport with the supplied configuration and
    /// inbound handler. Builds a fresh `RaftGrpcClient` internally.
    /// Use [`GrpcTransport::with_client`] when you need to *share* the
    /// outbound client with a caller-owned
    /// [`ConnectionPool`](../../xraft_client/pool/struct.ConnectionPool.html).
    pub fn new(config: GrpcTransportConfig, handler: Arc<H>) -> Self {
        let client = Arc::new(RaftGrpcClient::new(Self::client_config(&config)));
        Self::with_client(config, handler, client)
    }

    /// Construct a new gRPC transport over a caller-supplied
    /// [`Arc<RaftGrpcClient>`]. Used by
    /// [`xraft_client::pool::ConnectionPool`](../../xraft_client/pool/struct.ConnectionPool.html)
    /// so the same pool instance is shared between the server's
    /// inbound transport and the operator-visible pool surface
    /// stored on `ServerHandle`.
    ///
    /// The supplied `client` MUST have been built from a config
    /// consistent with `config.peer_endpoints` (typically via
    /// [`Self::client_config`]); mismatched peer rosters will not
    /// be reconciled — the inbound side uses `config` and the
    /// outbound side uses whatever the client was configured with.
    pub fn with_client(
        config: GrpcTransportConfig,
        handler: Arc<H>,
        client: Arc<RaftGrpcClient>,
    ) -> Self {
        let shutdown = Arc::new(Notify::new());
        Self {
            config,
            handler,
            client,
            shutdown,
        }
    }

    /// Derive a [`RaftGrpcClientConfig`] from a transport config —
    /// used by both [`Self::new`] and external pool constructors
    /// (e.g. [`xraft_client::pool::ConnectionPool::from_cluster_config`])
    /// so the client + transport agree on every knob.
    pub fn client_config(config: &GrpcTransportConfig) -> RaftGrpcClientConfig {
        RaftGrpcClientConfig {
            peer_endpoints: config.peer_endpoints.clone(),
            connect_timeout: config.connect_timeout,
            rpc_timeout: config.rpc_timeout,
            max_retries: config.max_retries,
            initial_backoff: config.retry_initial_backoff,
            max_backoff: config.retry_max_backoff,
            max_message_size: config.max_message_size,
            tls: config.tls.clone(),
        }
    }

    /// Borrow the shared outbound client. Use this to expose the
    /// same `Arc<RaftGrpcClient>` to a
    /// [`ConnectionPool`](../../xraft_client/pool/struct.ConnectionPool.html)
    /// or any other peer-RPC consumer.
    pub fn client(&self) -> Arc<RaftGrpcClient> {
        self.client.clone()
    }

    /// Trigger a graceful shutdown of any running `start_server` future.
    /// Safe to call multiple times; idempotent.
    pub fn shutdown(&self) {
        debug!(target: "xraft_transport", "shutdown signal fired");
        self.shutdown.notify_waiters();
        // notify_one() also stores a permit so a server that has not yet
        // started its `notified()` await still wakes on first poll.
        self.shutdown.notify_one();
    }

    /// Borrow the configured listen address. Test-only inspector.
    #[cfg(test)]
    pub fn listen_addr(&self) -> &str {
        &self.config.listen_addr
    }
}

impl<H: RaftMessageHandler> Transport for GrpcTransport<H> {
    fn send_vote(
        &self,
        to: NodeId,
        request: VoteRequest,
    ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
        self.client.send_vote(to, request)
    }

    fn send_pre_vote(
        &self,
        to: NodeId,
        request: PreVoteRequest,
    ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
        self.client.send_pre_vote(to, request)
    }

    fn send_fetch(
        &self,
        to: NodeId,
        request: FetchRequest,
    ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
        self.client.send_fetch(to, request)
    }

    fn send_fetch_snapshot(
        &self,
        to: NodeId,
        request: FetchSnapshotRequest,
    ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
        self.client.send_fetch_snapshot(to, request)
    }

    // Cannot use async fn syntax here because the trait declaration in
    // `xraft_core::transport::Transport` requires the explicit
    // `impl Future + Send + 'static` form so the Send / 'static bounds
    // are part of the public contract.
    #[allow(clippy::manual_async_fn)]
    fn start_server(
        self: Arc<Self>,
    ) -> impl std::future::Future<Output = XResult<()>> + Send + 'static {
        async move {
            // Use `tokio::net::TcpListener::bind(&str)` so the listen
            // address may be either a literal SocketAddr
            // (`"0.0.0.0:6000"`) OR a hostname (`"localhost:6000"`).
            // The previous `parse::<SocketAddr>()`-then-bind path
            // rejected hostnames before any DNS resolution even though
            // `ClusterConfig::validate_address` accepts them, so a
            // perfectly valid config such as `listen_addr =
            // "localhost:6000"` would fail at startup. `bind(&str)`
            // delegates to `ToSocketAddrs` which walks every resolved
            // address until one succeeds, so dual-stack hostnames bind
            // robustly without us picking a single address family up
            // front. The earlier sync pre-bind cannot be preserved
            // here because `std::net::TcpListener::bind` on a hostname
            // would suffer from the same v4/v6 selection issue; tokio's
            // bind is the canonical fix.
            let listener = tokio::net::TcpListener::bind(self.config.listen_addr.as_str())
                .await
                .map_err(|e| {
                    XRaftError::Transport(format!(
                        "bind gRPC listener on '{}': {e}",
                        self.config.listen_addr
                    ))
                })?;
            self.serve_inner(listener).await
        }
    }
}

impl<H: RaftMessageHandler> GrpcTransport<H> {
    /// Serve gRPC over a caller-supplied, **already-bound**
    /// [`tokio::net::TcpListener`].
    ///
    /// Lets the caller (e.g. Stage 6.1 `Server::start`) bind the
    /// listener **synchronously** so port conflicts and DNS
    /// resolution failures surface BEFORE the gRPC task is
    /// spawned, AND so the actual listening port (when the config
    /// specified ephemeral `:0`) can be inspected via
    /// `listener.local_addr()` before serving begins.
    ///
    /// The TLS path is unchanged: when `self.config.tls` is set,
    /// tonic terminates TLS for each accepted connection inside
    /// its service stack. The caller's listener stays plaintext
    /// TCP.
    pub async fn start_server_with_listener(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
    ) -> XResult<()> {
        self.serve_inner(listener).await
    }

    async fn serve_inner(self: Arc<Self>, listener: tokio::net::TcpListener) -> XResult<()> {
        let addr = listener
            .local_addr()
            .map_err(|e| XRaftError::Transport(format!("listener local_addr: {e}")))?;
        let adapter = RaftGrpcServer::new(self.handler.clone());
        let svc = adapter
            .into_service()
            .max_decoding_message_size(self.config.max_message_size)
            .max_encoding_message_size(self.config.max_message_size);

        let mut builder = Server::builder();
        if let Some(tls) = &self.config.tls {
            let identity = Identity::from_pem(&tls.server_cert_pem, &tls.server_key_pem);
            let tls_config = ServerTlsConfig::new().identity(identity);
            builder = builder
                .tls_config(tls_config)
                .map_err(|e| XRaftError::Transport(format!("server tls_config: {e}")))?;
        }

        info!(target: "xraft_transport", addr = %addr, tls = self.config.tls.is_some(), "starting gRPC server (pre-bound listener)");

        let shutdown = self.shutdown.clone();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        builder
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async move {
                shutdown.notified().await;
            })
            .await
            .map_err(|e| XRaftError::Transport(format!("gRPC server: {e}")))?;

        info!(target: "xraft_transport", addr = %addr, "gRPC server stopped");
        Ok(())
    }
}
