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
//! HTTP/2 connection. The pool maps `NodeId` to `Channel`. Concurrent
//! RPCs against an uncached peer are funnelled through a **per-peer**
//! `tokio::sync::Mutex` so they share a single TCP/TLS handshake instead
//! of racing N parallel `endpoint.connect()` calls — preventing a
//! cold-start / post-invalidate connection storm against a recovering
//! peer. Concurrent traffic against *different* peers still proceeds
//! fully in parallel: peer A's slow connect never blocks peer B's
//! lookups, because each peer owns its own mutex. Channels are evicted
//! from the pool when an RPC observes a transport-level error so the
//! next call re-establishes a fresh connection (re-entering the same
//! per-peer serialised connect path).
//!
//! # Retry policy
//!
//! Unary RPCs (`Vote`, `PreVote`, `Fetch`) retry on
//! [`tonic::Code::Unavailable`] and [`tonic::Code::DeadlineExceeded`] up
//! to `max_retries` times with exponential backoff **with equal jitter**
//! (sleep in `[backoff/2, backoff]`), capped at `max_backoff`. Equal
//! jitter satisfies the `architecture.md` §2.3 requirement of
//! "exponential with jitter, max 5 s" and prevents synchronised
//! reconnect storms when many peers detect the same outage
//! simultaneously. Streaming `FetchSnapshot` only retries the initial RPC
//! invocation; mid-stream errors are surfaced to the caller because
//! re-running the entire stream from scratch would corrupt offset-tracking.
//!
//! Other tonic status codes are deliberately **not** retried at the
//! transport layer. In particular:
//!
//! * `ResourceExhausted` indicates that the *server* has hit a rate or
//!   quota limit. Repeating the call from the transport layer competes
//!   with — and undermines — whatever admission-control / load-shedding
//!   response the peer is using to recover, and burns this client's
//!   retry budget on a condition that exponential backoff alone cannot
//!   resolve. The Raft layer above re-issues the logical RPC on its own
//!   schedule, which is the correct place to react to sustained server
//!   pressure.
//! * `Aborted` carries gRPC's documented semantics of a higher-level
//!   concurrency conflict ("retry at a higher level") rather than a
//!   transient transport failure. The Raft state machine is the only
//!   layer with enough context to decide whether re-issuing makes sense.
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
    /// Per-peer async mutexes that serialise *connection attempts*.
    ///
    /// Without these, a burst of N concurrent RPCs against the same
    /// uncached peer would each invoke `endpoint.connect().await` in
    /// parallel and then race at the pool's write lock — only one
    /// channel is kept, the other N − 1 freshly-completed TCP/TLS
    /// handshakes are silently discarded. That is wasteful at cold
    /// start and pathological after `invalidate()` triggers a
    /// correlated reconnect burst against a recovering peer.
    ///
    /// The map is sized once at construction from `peer_endpoints`
    /// and is then immutable, so no outer lock is required around the
    /// lookup. Each entry is an `Arc<tokio::sync::Mutex<()>>` that is
    /// held *only* across the slow-path connect of `channel_for` for
    /// that single peer; other peers proceed independently.
    connect_locks: Arc<HashMap<NodeId, Arc<tokio::sync::Mutex<()>>>>,
}

impl RaftGrpcClient {
    /// Construct a new client with the supplied configuration.
    pub fn new(config: RaftGrpcClientConfig) -> Self {
        // Pre-allocate one connect mutex per configured peer so the
        // map can be a plain immutable `HashMap` after construction
        // (no outer lock needed on the hot lookup path).
        let connect_locks: HashMap<NodeId, Arc<tokio::sync::Mutex<()>>> = config
            .peer_endpoints
            .keys()
            .cloned()
            .map(|peer| (peer, Arc::new(tokio::sync::Mutex::new(()))))
            .collect();
        Self {
            config,
            pool: Arc::new(RwLock::new(HashMap::new())),
            connect_locks: Arc::new(connect_locks),
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
    /// a **single** connection attempt.
    ///
    /// Concurrent callers that all miss the read-lock fast path for the
    /// same peer are serialised on a per-peer `tokio::sync::Mutex`, so
    /// only ONE TCP/TLS handshake is performed per cold-start burst.
    /// The per-peer mutex is dropped before the function returns, and
    /// callers for *other* peers are never blocked.
    ///
    /// On a connect failure, returns [`ChannelError::Connect`]. The
    /// caller's RPC retry loop is responsible for deciding whether to
    /// back off and retry; this avoids the nested retry loop that
    /// previously caused a dead peer to consume `max_retries²`
    /// connection attempts per RPC.
    async fn channel_for(&self, peer: NodeId) -> Result<Channel, ChannelError> {
        // Fast path: read-lock and clone if cached.
        if let Some(channel) = self.pool.read().await.get(&peer).cloned() {
            return Ok(channel);
        }

        // Per-peer connect serialisation. Look up the peer's mutex; an
        // unknown peer is a configuration error (matches the original
        // behaviour of returning `Misconfigured` before any connect).
        let connect_lock = self
            .connect_locks
            .get(&peer)
            .cloned()
            .ok_or_else(|| {
                ChannelError::Misconfigured(XRaftError::Transport(format!(
                    "no endpoint configured for peer {peer:?}; check ClusterConfig.voters"
                )))
            })?;

        // Hold the per-peer guard across the entire connect + insert
        // critical section so that concurrent tasks for *this* peer
        // share the single handshake performed below. Other peers
        // proceed in parallel because each peer has its own mutex.
        let _connect_guard = connect_lock.lock().await;

        // Double-check the pool now that we hold the per-peer guard —
        // a task that was ahead of us in the per-peer queue may have
        // already connected and populated the pool, in which case we
        // skip the redundant handshake entirely.
        if let Some(channel) = self.pool.read().await.get(&peer).cloned() {
            return Ok(channel);
        }

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
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| ChannelError::Connect(format!("connect to peer {}: {e}", peer.0)))?;

        // We still hold the per-peer connect guard, so no other task
        // can be racing to insert a channel for this peer. A plain
        // insert is sufficient and clearer than `entry().or_insert()`.
        // A concurrent `invalidate(peer)` is benign: either it ran
        // before our insert (pool was empty here — fine) or it runs
        // after, in which case the next caller will rebuild via this
        // same serialised path.
        self.pool.write().await.insert(peer, channel.clone());
        Ok(channel)
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
    ///
    /// Only [`tonic::Code::Unavailable`] (server is unreachable, channel
    /// torn down, HTTP/2 GOAWAY, etc.) and [`tonic::Code::DeadlineExceeded`]
    /// (the per-RPC timeout fired) are treated as transport-transient.
    /// Both clear cleanly with a fresh channel after a jittered backoff.
    ///
    /// Other status codes — including `ResourceExhausted` (server-side
    /// overload / quota) and `Aborted` (gRPC's higher-level concurrency
    /// conflict; "retry at a higher level" per the gRPC spec) — must NOT
    /// be retried at the transport layer. They are surfaced to the Raft
    /// layer above, which has the cluster-wide context needed to decide
    /// whether and when to re-issue the logical RPC. See the module-level
    /// `# Retry policy` section for the full rationale.
    fn is_retriable(status: &tonic::Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
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

    #[test]
    fn connect_locks_initialised_for_every_configured_peer() {
        // The per-peer connect serialisation only works if every
        // configured peer has a pre-allocated mutex. Guard against a
        // future refactor that forgets to wire up `connect_locks`.
        let mut peers = HashMap::new();
        peers.insert(NodeId(1), "http://10.0.0.1:6000".to_string());
        peers.insert(NodeId(2), "http://10.0.0.2:6000".to_string());
        peers.insert(NodeId(7), "http://10.0.0.7:6000".to_string());
        let cfg = RaftGrpcClientConfig {
            peer_endpoints: peers.clone(),
            connect_timeout: Duration::from_secs(1),
            rpc_timeout: Duration::from_secs(1),
            max_retries: 0,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            max_message_size: 1024,
            tls: None,
        };
        let client = RaftGrpcClient::new(cfg);
        assert_eq!(client.connect_locks.len(), peers.len());
        for peer in peers.keys() {
            assert!(
                client.connect_locks.contains_key(peer),
                "missing connect mutex for {peer:?}"
            );
        }
    }

    #[test]
    fn is_retriable_matches_documented_policy() {
        // Lock in the documented retry policy: only `Unavailable` and
        // `DeadlineExceeded` are transport-transient. Every other tonic
        // status code — including `ResourceExhausted` (server-side
        // overload) and `Aborted` (gRPC's "retry at a higher level"
        // concurrency conflict) — must be surfaced to the Raft layer
        // above without consuming this client's retry budget.
        use tonic::{Code, Status};

        // Retried.
        assert!(RaftGrpcClient::is_retriable(&Status::new(
            Code::Unavailable,
            ""
        )));
        assert!(RaftGrpcClient::is_retriable(&Status::new(
            Code::DeadlineExceeded,
            ""
        )));

        // Explicitly NOT retried. `ResourceExhausted` and `Aborted` are
        // the two codes called out in the module-level docs; the rest
        // pin down the full status-code surface so a future edit that
        // adds a code has to update this test deliberately.
        let non_retriable = [
            Code::Ok,
            Code::Cancelled,
            Code::Unknown,
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::Internal,
            Code::DataLoss,
            Code::Unauthenticated,
        ];
        for code in non_retriable {
            assert!(
                !RaftGrpcClient::is_retriable(&Status::new(code, "")),
                "tonic::Code::{code:?} must not be retriable at the transport layer"
            );
        }
    }
}
