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
//! HTTP/2 connection. The pool maps `NodeId` to `Channel`. A *separate*
//! per-peer connect mutex (`connect_locks`) serialises concurrent
//! `endpoint.connect()` attempts for the same peer: when several tasks
//! observe an empty pool entry at the same instant — typically right after
//! [`RaftGrpcClient::invalidate`] drops a stale channel on a transient RPC
//! failure — exactly one task performs the TCP/TLS handshake and the
//! others wait for it to publish the channel into the pool. Different
//! peers use different mutexes so a slow connect attempt for peer A still
//! never blocks lookups or connects for peer B (the original design
//! intent of the double-checked locking pattern). The connect-lock map
//! is bounded in size by the configured `peer_endpoints` set: unknown
//! peers are rejected *before* a per-peer mutex is created, so adversarial
//! or buggy callers cannot grow the map.
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
//!
//! Connect-time failures (peer unreachable, TLS handshake error) are
//! folded into the *same* outer retry loop as RPC failures — each call
//! to [`RaftGrpcClient::channel_for`] performs at most ONE connection
//! attempt, and the surrounding RPC loop applies the shared
//! [`max_retries`](RaftGrpcClientConfig::max_retries) /
//! [`initial_backoff`](RaftGrpcClientConfig::initial_backoff) budget.
//! This keeps the worst-case connection cost against a dead peer at
//! `O(max_retries)` total attempts (rather than `O(max_retries²)` that
//! a nested connect-retry would produce).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
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
    ///
    /// This budget is **shared** between connect failures and RPC
    /// failures within a single send call. A peer that is unreachable
    /// for the duration of the call therefore drives at most
    /// `max_retries + 1` total connection attempts, not `max_retries²`.
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

/// Internal channel-acquisition outcome that distinguishes between
/// *fatal* errors (caller must give up) and *retriable* connect failures
/// (caller should back off and try the whole `channel_for` cycle again).
#[derive(Debug)]
enum ChannelError {
    /// Configuration / endpoint-construction failure — no peer URL, an
    /// invalid URL, or a TLS setup problem. Retrying cannot help.
    Misconfigured(XRaftError),
    /// Single-attempt TCP/TLS connect failed. The outer RPC retry loop
    /// applies the shared backoff budget and re-enters `channel_for`.
    Connect(String),
}

impl From<ChannelError> for XRaftError {
    fn from(err: ChannelError) -> Self {
        match err {
            ChannelError::Misconfigured(e) => e,
            ChannelError::Connect(msg) => XRaftError::Transport(msg),
        }
    }
}

/// gRPC client for outbound Raft RPCs to peers in the cluster.
#[derive(Debug)]
pub struct RaftGrpcClient {
    config: RaftGrpcClientConfig,
    pool: Arc<RwLock<HashMap<NodeId, Channel>>>,
    /// Per-peer mutex that serialises concurrent `endpoint.connect()`
    /// attempts for the same peer. Created lazily on first lookup for a
    /// configured peer. The map is bounded in size by
    /// `config.peer_endpoints.len()` because unknown peers are rejected
    /// before a mutex is created. Entries are never removed: a few-byte
    /// `Arc<Mutex<()>>` per cluster member is cheaper than re-allocating
    /// the mutex on every reconnect cycle.
    connect_locks: Arc<RwLock<HashMap<NodeId, Arc<Mutex<()>>>>>,
}

impl RaftGrpcClient {
    /// Construct a new client with the supplied configuration.
    pub fn new(config: RaftGrpcClientConfig) -> Self {
        Self {
            config,
            pool: Arc::new(RwLock::new(HashMap::new())),
            connect_locks: Arc::new(RwLock::new(HashMap::new())),
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

    /// Acquire (or lazily create) the per-peer connect mutex.
    ///
    /// The double-checked locking on `connect_locks` avoids taking the
    /// write-lock on every call once the mutex Arc exists. Callers must
    /// ensure `peer` is already known-valid (i.e. present in
    /// `peer_endpoints`) before invoking this helper; otherwise the
    /// connect-lock map would grow without bound for adversarial inputs.
    async fn connect_lock_for(&self, peer: NodeId) -> Arc<Mutex<()>> {
        if let Some(m) = self.connect_locks.read().await.get(&peer).cloned() {
            return m;
        }
        let mut locks = self.connect_locks.write().await;
        locks
            .entry(peer)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Get an existing pooled channel for `peer` or open a fresh one with
    /// a **single** connection attempt.
    ///
    /// On a connect failure, returns [`ChannelError::Connect`]. The
    /// caller's RPC retry loop is responsible for deciding whether to
    /// back off and retry; this avoids the nested retry loop that
    /// previously caused a dead peer to consume `max_retries²`
    /// connection attempts per RPC.
    ///
    /// Concurrent calls for the *same* peer that miss the fast path
    /// serialise on a per-peer mutex so they share one connection
    /// attempt instead of all racing `endpoint.connect()` in parallel —
    /// preventing connection storms against a recovering peer right
    /// after [`invalidate`](Self::invalidate). Calls for *different*
    /// peers proceed in parallel because each has its own mutex.
    async fn channel_for(&self, peer: NodeId) -> Result<Channel, ChannelError> {
        // Fast path: read-lock and clone if cached. No connect mutex
        // needed when the channel is already published.
        if let Some(channel) = self.pool.read().await.get(&peer).cloned() {
            return Ok(channel);
        }

        // Resolve and validate the peer URL *before* touching the
        // connect-lock map. Unknown peers fail here without creating a
        // per-peer mutex, keeping `connect_locks` bounded by
        // `peer_endpoints.len()`.
        let url = self
            .config
            .peer_endpoints
            .get(&peer)
            .cloned()
            .ok_or_else(|| {
                ChannelError::Misconfigured(XRaftError::Transport(format!(
                    "no endpoint configured for peer {peer:?}; check ClusterConfig.voters"
                )))
            })?;
        let endpoint = self
            .build_endpoint(&url)
            .map_err(ChannelError::Misconfigured)?;

        // Serialise concurrent connect attempts for this peer. We drop
        // the `connect_locks` RwLock guard inside `connect_lock_for`
        // before awaiting the per-peer mutex, so different peers never
        // block each other and the global lock is never held across the
        // `endpoint.connect()` await.
        let connect_lock = self.connect_lock_for(peer).await;
        let _guard = connect_lock.lock().await;

        // Re-check the pool under the per-peer mutex: a concurrent task
        // that won the previous race may have just published a channel
        // while we were queued behind it.
        if let Some(channel) = self.pool.read().await.get(&peer).cloned() {
            return Ok(channel);
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| ChannelError::Connect(format!("connect to peer {}: {e}", peer.0)))?;

        // Publish. `entry().or_insert(...)` is defensive: while no other
        // task can be inside `channel_for` for this peer (we hold the
        // per-peer mutex), the `or_insert` form keeps this insertion
        // race-free against any future writer to `pool` and matches the
        // pattern used elsewhere in the file.
        let mut pool = self.pool.write().await;
        let entry = pool.entry(peer).or_insert(channel).clone();
        Ok(entry)
    }

    /// Acquire a channel for one RPC attempt within the outer retry loop.
    ///
    /// Returns:
    ///   - `Ok(Some(channel))` — got a channel, proceed with the RPC.
    ///   - `Ok(None)` — connect failed but retries remain; this function
    ///     has already slept for the jittered backoff and advanced
    ///     `attempt` / `backoff`. The caller should `continue` the
    ///     retry loop.
    ///   - `Err(_)` — fatal misconfiguration, or connect failed and the
    ///     retry budget is exhausted. The caller propagates this.
    async fn channel_for_attempt(
        &self,
        peer: NodeId,
        op: &'static str,
        attempt: &mut usize,
        backoff: &mut Duration,
    ) -> XResult<Option<Channel>> {
        match self.channel_for(peer).await {
            Ok(channel) => Ok(Some(channel)),
            Err(ChannelError::Connect(msg)) if *attempt < self.config.max_retries => {
                warn!(
                    target: "xraft_transport::client",
                    peer = peer.0,
                    attempt = *attempt,
                    "{op} connect failed, backing off {backoff:?}: {msg}"
                );
                tokio::time::sleep(jittered_sleep_duration(*backoff)).await;
                *backoff = (*backoff * 2).min(self.config.max_backoff);
                *attempt += 1;
                Ok(None)
            }
            Err(ChannelError::Connect(msg)) => Err(XRaftError::Transport(format!(
                "{op} connect to peer {} after {} attempts: {msg}",
                peer.0,
                *attempt + 1
            ))),
            Err(e @ ChannelError::Misconfigured(_)) => Err(e.into()),
        }
    }

    /// Drop the pooled channel for `peer` so the next `channel_for` call
    /// rebuilds it.
    ///
    /// This is cache eviction, **not** a fence: it does not cancel an
    /// in-flight `connect()` attempt that is happening under the per-peer
    /// connect mutex. A connect that began before an `invalidate` call
    /// will still publish its (freshly-established) channel afterwards.
    /// That is correct behaviour — the freshly-connected channel is by
    /// definition newer than the one being invalidated.
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
            let channel = match self
                .channel_for_attempt(peer, "Vote", &mut attempt, &mut backoff)
                .await?
            {
                Some(ch) => ch,
                None => continue,
            };
            if attempt > 0 {
                debug!(target: "xraft_transport::client", peer = peer.0, attempt, "Vote attempt after retry");
            }
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
            let channel = match self
                .channel_for_attempt(peer, "PreVote", &mut attempt, &mut backoff)
                .await?
            {
                Some(ch) => ch,
                None => continue,
            };
            if attempt > 0 {
                debug!(target: "xraft_transport::client", peer = peer.0, attempt, "PreVote attempt after retry");
            }
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
            let channel = match self
                .channel_for_attempt(peer, "Fetch", &mut attempt, &mut backoff)
                .await?
            {
                Some(ch) => ch,
                None => continue,
            };
            if attempt > 0 {
                debug!(target: "xraft_transport::client", peer = peer.0, attempt, "Fetch attempt after retry");
            }
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
            let channel = match self
                .channel_for_attempt(peer, "FetchSnapshot", &mut attempt, &mut backoff)
                .await?
            {
                Some(ch) => ch,
                None => continue,
            };
            if attempt > 0 {
                debug!(target: "xraft_transport::client", peer = peer.0, attempt, "FetchSnapshot attempt after retry");
            }
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

    /// Snapshot the set of peers that have a per-peer connect mutex
    /// allocated. Test-only inspector for verifying the bound on
    /// `connect_locks` growth.
    #[cfg(test)]
    async fn connect_locked_peers(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self.connect_locks.read().await.keys().copied().collect();
        v.sort_by_key(|p| p.0);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

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

    #[test]
    fn channel_error_misconfigured_propagates_inner_error() {
        let inner = XRaftError::Transport("invalid peer URL 'bad': parse error".to_string());
        let err: XRaftError = ChannelError::Misconfigured(inner).into();
        match err {
            XRaftError::Transport(msg) => {
                assert!(msg.contains("invalid peer URL"), "got: {msg}");
            }
            other => panic!("expected Transport, got {other:?}"),
        }
    }

    #[test]
    fn channel_error_connect_wraps_message_in_transport() {
        let err: XRaftError = ChannelError::Connect("connect to peer 7: refused".to_string()).into();
        match err {
            XRaftError::Transport(msg) => {
                assert!(msg.contains("connect to peer 7"), "got: {msg}");
                assert!(msg.contains("refused"), "got: {msg}");
            }
            other => panic!("expected Transport, got {other:?}"),
        }
    }

    /// Build a config that points one peer at the supplied address with
    /// generous timeouts and no retry budget — useful for tests that
    /// exercise `channel_for` directly.
    fn test_config(peer: NodeId, addr: std::net::SocketAddr) -> RaftGrpcClientConfig {
        let mut endpoints = HashMap::new();
        endpoints.insert(peer, format!("http://{addr}"));
        RaftGrpcClientConfig {
            peer_endpoints: endpoints,
            connect_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(5),
            max_retries: 0,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            max_message_size: 1024,
            tls: None,
        }
    }

    /// Concurrent `channel_for` calls for the same peer must serialise on
    /// the per-peer connect mutex so that no more than one TCP connect is
    /// observed by the server at any instant — the *core* invariant the
    /// fix introduces, and the one the review comment asked for.
    ///
    /// A local `TcpListener` accepts connections, holds each socket open
    /// for a window long enough that parallel clients would overlap, and
    /// tracks the peak number of simultaneously-open inbound sockets via
    /// an atomic counter. Without serialisation the peak would equal the
    /// caller fan-out; with serialisation it must be exactly 1.
    #[tokio::test]
    async fn channel_for_serialises_concurrent_connects_to_same_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let total = Arc::new(AtomicUsize::new(0));

        let active_srv = Arc::clone(&active);
        let peak_srv = Arc::clone(&peak);
        let total_srv = Arc::clone(&total);

        // Server: hold each accepted socket open for a window long enough
        // that two unsynchronised clients would visibly overlap.
        let server = tokio::spawn(async move {
            loop {
                let (sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                total_srv.fetch_add(1, Ordering::SeqCst);
                let active_inner = Arc::clone(&active_srv);
                let peak_inner = Arc::clone(&peak_srv);
                tokio::spawn(async move {
                    let n = active_inner.fetch_add(1, Ordering::SeqCst) + 1;
                    peak_inner.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    active_inner.fetch_sub(1, Ordering::SeqCst);
                    drop(sock);
                });
            }
        });

        let peer = NodeId(1);
        let client = Arc::new(RaftGrpcClient::new(test_config(peer, addr)));

        const FANOUT: usize = 8;
        let mut joins = Vec::with_capacity(FANOUT);
        for _ in 0..FANOUT {
            let c = Arc::clone(&client);
            joins.push(tokio::spawn(async move {
                // Connection will fail (server never speaks HTTP/2) but
                // the TCP accept on the listener side is what we count.
                let _ = c.channel_for(peer).await;
            }));
        }
        for j in joins {
            let _ = j.await;
        }

        server.abort();

        let observed_peak = peak.load(Ordering::SeqCst);
        let observed_total = total.load(Ordering::SeqCst);
        assert_eq!(
            observed_peak, 1,
            "expected per-peer serialisation; saw {observed_peak} simultaneous inbound \
             connections (total accepts: {observed_total})",
        );

        // Exactly one connect-mutex was created for this peer, regardless
        // of fan-out, and the pool stays empty because every connect fails
        // at the HTTP/2 layer.
        assert_eq!(client.connect_locked_peers().await, vec![peer]);
        assert_eq!(client.pool_size().await, 0);
    }

    /// Concurrent `channel_for` calls for *different* peers must NOT
    /// serialise — that would re-introduce the head-of-line blocking the
    /// original double-checked locking pattern was designed to avoid.
    ///
    /// Each peer connects to its own local listener that holds the socket
    /// open for the same window. A shared atomic counter aggregates the
    /// active inbound-socket count across BOTH listeners, so we can
    /// distinguish "serialised across peers" (peak = 1) from "parallel
    /// across peers" (peak = 2). The fix must produce the latter.
    #[tokio::test]
    async fn channel_for_does_not_serialise_across_different_peers() {
        let listener_a = TcpListener::bind("127.0.0.1:0").await.expect("bind a");
        let listener_b = TcpListener::bind("127.0.0.1:0").await.expect("bind b");
        let addr_a = listener_a.local_addr().expect("local_addr a");
        let addr_b = listener_b.local_addr().expect("local_addr b");

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let spawn_listener = |listener: TcpListener,
                              active: Arc<AtomicUsize>,
                              peak: Arc<AtomicUsize>| {
            tokio::spawn(async move {
                loop {
                    let (sock, _) = match listener.accept().await {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let active_inner = Arc::clone(&active);
                    let peak_inner = Arc::clone(&peak);
                    tokio::spawn(async move {
                        let n = active_inner.fetch_add(1, Ordering::SeqCst) + 1;
                        peak_inner.fetch_max(n, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(80)).await;
                        active_inner.fetch_sub(1, Ordering::SeqCst);
                        drop(sock);
                    });
                }
            })
        };

        let srv_a = spawn_listener(listener_a, Arc::clone(&active), Arc::clone(&peak));
        let srv_b = spawn_listener(listener_b, Arc::clone(&active), Arc::clone(&peak));

        let peer_a = NodeId(1);
        let peer_b = NodeId(2);
        let mut endpoints = HashMap::new();
        endpoints.insert(peer_a, format!("http://{addr_a}"));
        endpoints.insert(peer_b, format!("http://{addr_b}"));
        let cfg = RaftGrpcClientConfig {
            peer_endpoints: endpoints,
            connect_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(5),
            max_retries: 0,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            max_message_size: 1024,
            tls: None,
        };
        let client = Arc::new(RaftGrpcClient::new(cfg));

        let ca = Arc::clone(&client);
        let cb = Arc::clone(&client);
        let ja = tokio::spawn(async move {
            let _ = ca.channel_for(peer_a).await;
        });
        let jb = tokio::spawn(async move {
            let _ = cb.channel_for(peer_b).await;
        });
        let _ = ja.await;
        let _ = jb.await;

        srv_a.abort();
        srv_b.abort();

        let observed_peak = peak.load(Ordering::SeqCst);
        assert_eq!(
            observed_peak, 2,
            "expected peer A and peer B connects to overlap; saw peak {observed_peak} \
             (different peers must use independent connect mutexes)",
        );
    }

    /// Unknown peers must be rejected *before* a per-peer connect mutex
    /// is created, so that adversarial or buggy callers cannot grow
    /// `connect_locks` without bound.
    #[tokio::test]
    async fn channel_for_unknown_peer_does_not_grow_connect_locks() {
        let cfg = RaftGrpcClientConfig {
            peer_endpoints: HashMap::new(),
            connect_timeout: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(50),
            max_retries: 0,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            max_message_size: 1024,
            tls: None,
        };
        let client = RaftGrpcClient::new(cfg);

        for id in 0..16u64 {
            let res = client.channel_for(NodeId(id)).await;
            assert!(
                matches!(res, Err(ChannelError::Misconfigured(_))),
                "expected Misconfigured for unknown peer {id}, got {res:?}"
            );
        }
        assert!(
            client.connect_locked_peers().await.is_empty(),
            "connect_locks must stay empty for unknown peers"
        );
    }
}
