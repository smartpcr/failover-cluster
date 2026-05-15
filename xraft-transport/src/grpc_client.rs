//! gRPC client with per-peer connection pool, exponential backoff retries,
//! and optional TLS.
//!
//! `RaftGrpcClient` is constructed with a static map of `NodeId -> URL`
//! plus retry / timeout / TLS configuration, and exposes the four send-side
//! methods needed by the [`Transport`](xraft_core::transport::Transport)
//! trait.
//!
//! # Connection pool
//!
//! tonic `Channel`s are cheaply cloneable handles backed by a multiplexed
//! HTTP/2 connection. The pool maps `NodeId` to `Channel` and uses
//! double-checked locking so a slow connection attempt for peer A does
//! not block lookups for peer B. Channels are evicted from the pool
//! when an RPC observes a transport-level error so the next call
//! re-establishes a fresh connection.
//!
//! # Retry policy
//!
//! Unary RPCs (`Vote`, `PreVote`, `Fetch`) retry on
//! [`tonic::Code::Unavailable`] up to `max_retries` times with exponential
//! backoff **with equal jitter** (sleep in `[backoff/2, backoff]`), capped
//! at `max_backoff`. Equal jitter satisfies the `architecture.md` §2.3
//! requirement of "exponential with jitter, max 5 s" and prevents
//! synchronised reconnect storms when many peers detect the same outage
//! simultaneously. Streaming `FetchSnapshot` only retries the initial RPC
//! invocation; mid-stream errors are surfaced to the caller because
//! re-running the entire stream from scratch would corrupt offset-tracking.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::RwLock;
use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, warn};

use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotChunk, FetchSnapshotRequest, PreVoteRequest,
    PreVoteResponse, VoteRequest, VoteResponse,
};
use xraft_core::transport::SnapshotChunkStream;
use xraft_core::types::NodeId;

use crate::grpc::TlsTransportConfig;
use crate::pb;
use crate::pb::raft_service_client::RaftServiceClient;

/// Return a jittered sleep duration in `[base / 2, base]`.
///
/// Equal-jitter exponential backoff: the next sleep is uniformly random
/// between half the current backoff and the full backoff. The exponential
/// state (`backoff` doubled per attempt) is preserved by the caller, so
/// operators still observe the configured doubling pattern at the upper
/// bound while reconnect attempts from peers that detect the same outage
/// at the same instant are spread over time, avoiding a thundering-herd
/// reconnect storm against a recovering leader. Required by
/// `architecture.md` §2.3 ("Connection backoff: exponential with jitter,
/// max 5 s").
fn jittered_sleep_duration(base: Duration) -> Duration {
    let half = base / 2;
    let fraction: f64 = rand::random();
    half + half.mul_f64(fraction)
}

/// Configuration for [`RaftGrpcClient`].
#[derive(Debug, Clone)]
pub struct RaftGrpcClientConfig {
    /// Map of `NodeId -> peer URL` (e.g. `http://10.0.0.2:6000`).
    pub peer_endpoints: HashMap<NodeId, String>,
    /// Per-RPC connection-establishment timeout.
    pub connect_timeout: Duration,
    /// Per-RPC end-to-end timeout (applies separately to each retry attempt).
    pub rpc_timeout: Duration,
    /// Maximum retry attempts for unary RPCs after a transient failure.
    pub max_retries: usize,
    /// Initial backoff delay; doubles after each failed retry up to `max_backoff`.
    pub initial_backoff: Duration,
    /// Cap on the exponential backoff delay.
    pub max_backoff: Duration,
    /// Maximum decoded gRPC message size in bytes (default 64 MiB).
    pub max_message_size: usize,
    /// TLS configuration. When `None`, plaintext HTTP/2 is used.
    pub tls: Option<Arc<TlsTransportConfig>>,
}

/// gRPC client for outbound Raft RPCs to peers in the cluster.
#[derive(Debug)]
pub struct RaftGrpcClient {
    config: RaftGrpcClientConfig,
    pool: Arc<RwLock<HashMap<NodeId, Channel>>>,
}

impl RaftGrpcClient {
    /// Construct a new client with the supplied configuration.
    pub fn new(config: RaftGrpcClientConfig) -> Self {
        Self {
            config,
            pool: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Number of channels currently held in the pool.
    ///
    /// Test-friendly inspector; cheap to call (single read-lock acquisition).
    pub async fn pool_size(&self) -> usize {
        self.pool.read().await.len()
    }

    /// Build a `tonic::transport::Endpoint` from a peer URL with the
    /// configured connect timeout, TLS, and message-size caps applied.
    fn build_endpoint(&self, url: &str) -> XResult<Endpoint> {
        let mut endpoint = Endpoint::from_shared(url.to_string())
            .map_err(|e| XRaftError::Transport(format!("invalid peer URL '{url}': {e}")))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.rpc_timeout);
        if let Some(tls) = &self.config.tls {
            let mut tls_cfg = ClientTlsConfig::new();
            if let Some(ca_pem) = &tls.ca_cert_pem {
                tls_cfg = tls_cfg.ca_certificate(Certificate::from_pem(ca_pem));
            }
            if let Some(domain) = &tls.domain_name {
                tls_cfg = tls_cfg.domain_name(domain);
            }
            endpoint = endpoint
                .tls_config(tls_cfg)
                .map_err(|e| XRaftError::Transport(format!("client tls_config: {e}")))?;
        }
        Ok(endpoint)
    }

    /// Get an existing pooled channel for `peer` or open a fresh one with
    /// exponential-backoff retry on connection failure.
    async fn channel_for(&self, peer: NodeId) -> XResult<Channel> {
        // Fast path: read-lock and clone if cached.
        if let Some(channel) = self.pool.read().await.get(&peer).cloned() {
            return Ok(channel);
        }

        let url = self
            .config
            .peer_endpoints
            .get(&peer)
            .cloned()
            .ok_or_else(|| {
                XRaftError::Transport(format!(
                    "no endpoint configured for peer {peer:?}; check ClusterConfig.voters"
                ))
            })?;
        let endpoint = self.build_endpoint(&url)?;
        let channel = self.connect_with_backoff(endpoint, peer).await?;

        // Slow path: take the write lock and double-check before inserting.
        let mut pool = self.pool.write().await;
        let entry = pool.entry(peer).or_insert(channel).clone();
        Ok(entry)
    }

    async fn connect_with_backoff(&self, endpoint: Endpoint, peer: NodeId) -> XResult<Channel> {
        let mut backoff = self.config.initial_backoff;
        let mut attempt: usize = 0;
        loop {
            match endpoint.clone().connect().await {
                Ok(channel) => {
                    if attempt > 0 {
                        debug!(target: "xraft_transport::client", peer = peer.0, attempt, "connect succeeded after retries");
                    }
                    return Ok(channel);
                }
                Err(e) if attempt < self.config.max_retries => {
                    warn!(target: "xraft_transport::client", peer = peer.0, attempt, "connect failed, backing off {backoff:?}: {e}");
                    tokio::time::sleep(jittered_sleep_duration(backoff)).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    attempt += 1;
                }
                Err(e) => {
                    return Err(XRaftError::Transport(format!(
                        "connect to peer {} after {} attempts: {e}",
                        peer.0,
                        attempt + 1
                    )));
                }
            }
        }
    }

    /// Drop the pooled channel for `peer` so the next `channel_for` call
    /// rebuilds it.
    async fn invalidate(&self, peer: NodeId) {
        let mut pool = self.pool.write().await;
        pool.remove(&peer);
    }

    /// Build a typed RaftService client over a channel with the configured
    /// message-size limits applied.
    fn typed_client(&self, channel: Channel) -> RaftServiceClient<Channel> {
        RaftServiceClient::new(channel)
            .max_decoding_message_size(self.config.max_message_size)
            .max_encoding_message_size(self.config.max_message_size)
    }

    /// Decide whether a tonic error represents a transient transport-level
    /// failure that justifies a retry on a fresh connection.
    fn is_retriable(status: &tonic::Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::ResourceExhausted
                | tonic::Code::Aborted
        )
    }

    // --------------------------------------------------------------
    // Public RPC methods (unary)
    // --------------------------------------------------------------

    /// Send a `Vote` RPC to `peer` with retry on transient failure.
    pub async fn send_vote(&self, peer: NodeId, request: VoteRequest) -> XResult<VoteResponse> {
        let proto = pb::VoteRequest::from(&request);
        let mut backoff = self.config.initial_backoff;
        let mut attempt: usize = 0;
        loop {
            let channel = self.channel_for(peer).await?;
            let mut client = self.typed_client(channel);
            let req = Request::new(proto.clone());
            match client.vote(req).await {
                Ok(resp) => return Ok(VoteResponse::from(resp.into_inner())),
                Err(status) if Self::is_retriable(&status) && attempt < self.config.max_retries => {
                    warn!(target: "xraft_transport::client", peer = peer.0, attempt, "Vote RPC retriable error, backing off {backoff:?}: {status}");
                    self.invalidate(peer).await;
                    tokio::time::sleep(jittered_sleep_duration(backoff)).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    attempt += 1;
                }
                Err(status) => {
                    return Err(XRaftError::Transport(format!(
                        "Vote RPC to peer {} after {} attempts: {status}",
                        peer.0,
                        attempt + 1
                    )));
                }
            }
        }
    }

    /// Send a `PreVote` RPC to `peer` with retry on transient failure.
    pub async fn send_pre_vote(
        &self,
        peer: NodeId,
        request: PreVoteRequest,
    ) -> XResult<PreVoteResponse> {
        let proto = pb::PreVoteRequest::from(&request);
        let mut backoff = self.config.initial_backoff;
        let mut attempt: usize = 0;
        loop {
            let channel = self.channel_for(peer).await?;
            let mut client = self.typed_client(channel);
            let req = Request::new(proto.clone());
            match client.pre_vote(req).await {
                Ok(resp) => return Ok(PreVoteResponse::from(resp.into_inner())),
                Err(status) if Self::is_retriable(&status) && attempt < self.config.max_retries => {
                    warn!(target: "xraft_transport::client", peer = peer.0, attempt, "PreVote RPC retriable error, backing off {backoff:?}: {status}");
                    self.invalidate(peer).await;
                    tokio::time::sleep(jittered_sleep_duration(backoff)).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    attempt += 1;
                }
                Err(status) => {
                    return Err(XRaftError::Transport(format!(
                        "PreVote RPC to peer {} after {} attempts: {status}",
                        peer.0,
                        attempt + 1
                    )));
                }
            }
        }
    }

    /// Send a `Fetch` RPC to `peer` with retry on transient failure.
    pub async fn send_fetch(&self, peer: NodeId, request: FetchRequest) -> XResult<FetchResponse> {
        let proto = pb::FetchRequest::from(&request);
        let mut backoff = self.config.initial_backoff;
        let mut attempt: usize = 0;
        loop {
            let channel = self.channel_for(peer).await?;
            let mut client = self.typed_client(channel);
            let req = Request::new(proto.clone());
            match client.fetch(req).await {
                Ok(resp) => {
                    let canonical = FetchResponse::try_from(resp.into_inner()).map_err(|e| {
                        XRaftError::Transport(format!("Fetch response decode: {e}"))
                    })?;
                    return Ok(canonical);
                }
                Err(status) if Self::is_retriable(&status) && attempt < self.config.max_retries => {
                    warn!(target: "xraft_transport::client", peer = peer.0, attempt, "Fetch RPC retriable error, backing off {backoff:?}: {status}");
                    self.invalidate(peer).await;
                    tokio::time::sleep(jittered_sleep_duration(backoff)).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    attempt += 1;
                }
                Err(status) => {
                    return Err(XRaftError::Transport(format!(
                        "Fetch RPC to peer {} after {} attempts: {status}",
                        peer.0,
                        attempt + 1
                    )));
                }
            }
        }
    }

    /// Send a `FetchSnapshot` RPC to `peer` and return the server-streaming
    /// chunk stream.
    ///
    /// Retries cover only the *initial* RPC call (i.e. before any chunks
    /// arrive). Once the stream begins, mid-stream errors are surfaced to
    /// the caller as `Err` items in the returned stream — automatic
    /// whole-stream retry would corrupt the byte-offset book-keeping the
    /// follower uses to resume snapshot transfer.
    pub async fn send_fetch_snapshot(
        &self,
        peer: NodeId,
        request: FetchSnapshotRequest,
    ) -> XResult<SnapshotChunkStream> {
        let proto = pb::FetchSnapshotRequest::from(&request);
        let mut backoff = self.config.initial_backoff;
        let mut attempt: usize = 0;
        let stream = loop {
            let channel = self.channel_for(peer).await?;
            let mut client = self.typed_client(channel);
            let req = Request::new(proto.clone());
            match client.fetch_snapshot(req).await {
                Ok(resp) => break resp.into_inner(),
                Err(status) if Self::is_retriable(&status) && attempt < self.config.max_retries => {
                    warn!(target: "xraft_transport::client", peer = peer.0, attempt, "FetchSnapshot init retriable error, backing off {backoff:?}: {status}");
                    self.invalidate(peer).await;
                    tokio::time::sleep(jittered_sleep_duration(backoff)).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    attempt += 1;
                }
                Err(status) => {
                    return Err(XRaftError::Transport(format!(
                        "FetchSnapshot RPC init to peer {} after {} attempts: {status}",
                        peer.0,
                        attempt + 1
                    )));
                }
            }
        };

        let mapped: SnapshotChunkStream = Box::pin(stream.map(|item| {
            match item {
                Ok(proto_chunk) => FetchSnapshotChunk::try_from(proto_chunk)
                    .map_err(|e| XRaftError::Transport(format!("FetchSnapshot chunk decode: {e}"))),
                Err(status) => Err(XRaftError::Transport(format!(
                    "FetchSnapshot stream error: {status}"
                ))),
            }
        }));

        Ok(mapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn jittered_sleep_within_equal_jitter_bounds() {
        let base = Duration::from_millis(400);
        let half = base / 2;
        for _ in 0..2_000 {
            let d = jittered_sleep_duration(base);
            assert!(d >= half, "jittered sleep {d:?} below lower bound {half:?}");
            assert!(d <= base, "jittered sleep {d:?} above upper bound {base:?}");
        }
    }

    #[test]
    fn jittered_sleep_actually_varies() {
        // Equal-jitter must produce a spread of values across many calls,
        // otherwise the implementation is silently fixed-delay.
        let base = Duration::from_millis(1000);
        let samples: Vec<Duration> = (0..256).map(|_| jittered_sleep_duration(base)).collect();
        let unique: HashSet<u128> = samples.iter().map(|d| d.as_nanos()).collect();
        assert!(
            unique.len() >= 16,
            "expected jittered sleeps to vary; only saw {} distinct values out of {}",
            unique.len(),
            samples.len()
        );
    }

    #[test]
    fn jittered_sleep_zero_base_is_zero() {
        // Edge case: a zero backoff must not panic and must still be in
        // the (degenerate) [0, 0] range.
        let d = jittered_sleep_duration(Duration::ZERO);
        assert_eq!(d, Duration::ZERO);
    }
}
