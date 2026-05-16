//! Integration tests for the gRPC transport layer (Stage 4.1).
//!
//! Each test wires a real `tonic::transport::Server` to a stub
//! [`RaftMessageHandler`] and exercises the corresponding scenario from
//! the workstream brief:
//!
//! - `grpc_vote_roundtrip` — Vote RPC end-to-end with field equality.
//! - `connection_retry` — Client retries when the peer is briefly
//!   unreachable and eventually succeeds.
//! - `concurrent_rpcs` — 50 concurrent Fetch RPCs share one server
//!   without deadlock or response cross-talk.
//! - `tls_transport` — Self-signed TLS cert + key are accepted on both
//!   ends and the Vote RPC succeeds.

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinSet;

use xraft_core::config::ClusterConfig;
use xraft_core::error::Result as XResult;
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotChunk, FetchSnapshotRequest, PreVoteRequest,
    PreVoteResponse, VoteRequest, VoteResponse,
};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream, Transport};
use xraft_core::types::{LogIndex, NodeId, Term};

use xraft_transport::grpc::{
    GrpcTransport, GrpcTransportConfig, TlsTransportConfig, peer_endpoints_from_cluster_config,
};
use xraft_transport::grpc_client::{RaftGrpcClient, RaftGrpcClientConfig};
use xraft_transport::grpc_server::RaftGrpcServer;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Pick a free TCP port by asking the OS to bind one and immediately
/// releasing it. There is an inherent race window between `drop(listener)`
/// and the test re-binding to that port; in practice this is not an issue
/// because the kernel returns ports from a low-contention pool.
fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

const TEST_CLUSTER_ID: &str = "test-cluster";
const TEST_LEADER_EPOCH: u64 = 7;
const SERVER_NODE_ID: u64 = 1;
const CLIENT_NODE_ID: u64 = 2;

/// Stub handler that returns canned responses for every RPC and counts
/// how many times each method was called.
#[derive(Default)]
struct StubHandler {
    vote_calls: AtomicU64,
    pre_vote_calls: AtomicU64,
    fetch_calls: AtomicU64,
    fetch_snapshot_calls: AtomicU64,
    /// When true, `handle_fetch_snapshot` emits chunk0 then a synthetic
    /// mid-stream `XRaftError::Transport` error. Used by
    /// `fetch_snapshot_mid_stream_transport_error_evicts_channel` to
    /// drive the client's eviction policy through a real wire path.
    fetch_snapshot_mid_stream_error: AtomicBool,
}

impl StubHandler {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Build a stub whose FetchSnapshot stream emits one chunk then a
    /// `XRaftError::Transport` error. Server-side this maps to
    /// `Status::unavailable`, which the client sees as a retriable
    /// mid-stream transport failure.
    fn with_mid_stream_error() -> Arc<Self> {
        let h = Self::default();
        h.fetch_snapshot_mid_stream_error
            .store(true, Ordering::SeqCst);
        Arc::new(h)
    }
}

impl RaftMessageHandler for StubHandler {
    async fn handle_vote(&self, req: VoteRequest) -> XResult<VoteResponse> {
        self.vote_calls.fetch_add(1, Ordering::SeqCst);
        Ok(VoteResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            term: req.term,
            vote_granted: true,
            leader_hint: Some(NodeId(SERVER_NODE_ID)),
        })
    }

    async fn handle_pre_vote(&self, req: PreVoteRequest) -> XResult<PreVoteResponse> {
        self.pre_vote_calls.fetch_add(1, Ordering::SeqCst);
        Ok(PreVoteResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            term: req.next_term,
            vote_granted: true,
            leader_hint: None,
        })
    }

    async fn handle_fetch(&self, req: FetchRequest) -> XResult<FetchResponse> {
        // bump after capturing the previous value so test responses can
        // verify per-call distinctness via `high_watermark`.
        let prev = self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        Ok(FetchResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            leader_id: NodeId(SERVER_NODE_ID),
            high_watermark: LogIndex(prev),
            entries: Vec::new(),
            diverging_epoch: None,
            snapshot_redirect: None,
            is_leader: true,
        })
    }

    async fn handle_fetch_snapshot(
        &self,
        _req: FetchSnapshotRequest,
    ) -> XResult<SnapshotChunkStream> {
        self.fetch_snapshot_calls.fetch_add(1, Ordering::SeqCst);
        // Two-chunk stream exercising the canonical FetchSnapshot wire
        // contract: the first chunk carries SnapshotMeta + a payload slice
        // and `done = false`; the second chunk carries no metadata and
        // sets `done = true`.
        let meta = xraft_core::storage::SnapshotMeta {
            last_included_index: LogIndex(123),
            last_included_term: Term(4),
            id: "snap-test".to_string(),
            voter_set: None,
            size_bytes: Some(12),
            checksum: None,
        };
        let chunk0 = FetchSnapshotChunk {
            cluster_id: TEST_CLUSTER_ID.to_string(),
            leader_epoch: TEST_LEADER_EPOCH,
            chunk_index: 0,
            data: b"snap-part-1-".to_vec(),
            done: false,
            metadata: Some(meta),
        };
        let chunk1 = FetchSnapshotChunk {
            cluster_id: TEST_CLUSTER_ID.to_string(),
            leader_epoch: TEST_LEADER_EPOCH,
            chunk_index: 1,
            data: b"end".to_vec(),
            done: true,
            metadata: None,
        };
        let stream: SnapshotChunkStream =
            if self.fetch_snapshot_mid_stream_error.load(Ordering::SeqCst) {
                // Emit one good chunk then a transport-class error. The
                // server adapter maps `XRaftError::Transport` to
                // `Status::unavailable`, which is the retriable code the
                // client's eviction policy keys off of.
                Box::pin(futures::stream::iter(vec![
                    Ok(chunk0),
                    Err(xraft_core::error::XRaftError::Transport(
                        "synthetic mid-stream transport failure".to_string(),
                    )),
                ]))
            } else {
                Box::pin(futures::stream::iter(vec![Ok(chunk0), Ok(chunk1)]))
            };
        Ok(stream)
    }
}

/// Build a canonical `VoteRequest` with deterministic test values.
fn sample_vote_request() -> VoteRequest {
    VoteRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        term: Term(42),
        candidate_id: NodeId(CLIENT_NODE_ID),
        last_log_index: LogIndex(99),
        last_log_term: Term(41),
    }
}

/// Build a canonical `FetchRequest` with deterministic test values.
fn sample_fetch_request(replica: u64) -> FetchRequest {
    FetchRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        replica_id: NodeId(replica),
        fetch_offset: LogIndex(0),
        last_fetched_epoch: Term(0),
    }
}

/// Make a default `RaftGrpcClientConfig` that talks to a single peer
/// (`SERVER_NODE_ID`) at `endpoint`. Tunables are tightened so transient
/// failures surface within seconds rather than minutes.
fn client_config(endpoint: String) -> RaftGrpcClientConfig {
    let mut peer_endpoints = HashMap::new();
    peer_endpoints.insert(NodeId(SERVER_NODE_ID), endpoint);
    RaftGrpcClientConfig {
        peer_endpoints,
        connect_timeout: Duration::from_millis(500),
        rpc_timeout: Duration::from_secs(2),
        max_retries: 8,
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(400),
        max_message_size: 4 * 1024 * 1024,
        tls: None,
    }
}

/// Spawn a tonic server bound to `addr` that dispatches into `handler`.
///
/// Returns a `Notify` whose `notify_one()` cleanly stops the server, plus
/// the join handle so the test can `await` the shutdown.
fn spawn_plain_server(
    addr: std::net::SocketAddr,
    handler: Arc<StubHandler>,
) -> (
    Arc<Notify>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    let svc = RaftGrpcServer::new(handler).into_service();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                shutdown_clone.notified().await;
            })
            .await
    });
    (shutdown, handle)
}

/// Wait up to `timeout` for the test's server socket to accept a TCP
/// connection. Used to avoid client-start-before-server races in tests
/// that explicitly bind a known port.
async fn wait_for_listening(addr: std::net::SocketAddr, timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server at {addr} never started accepting within {timeout:?}");
}

// ---------------------------------------------------------------------------
// Scenario: grpc-vote-roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_vote_roundtrip() {
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let handler = StubHandler::new();
    let (shutdown, srv_handle) = spawn_plain_server(addr, handler.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let client = RaftGrpcClient::new(client_config(endpoint));

    let req = sample_vote_request();
    let resp = client
        .send_vote(NodeId(SERVER_NODE_ID), req.clone())
        .await
        .expect("vote rpc succeeds");

    assert_eq!(resp.cluster_id, req.cluster_id, "cluster_id roundtrip");
    assert_eq!(
        resp.leader_epoch, req.leader_epoch,
        "leader_epoch roundtrip"
    );
    assert_eq!(resp.term, req.term, "term roundtrip");
    assert!(resp.vote_granted, "stub handler always grants the vote");
    assert_eq!(
        resp.leader_hint,
        Some(NodeId(SERVER_NODE_ID)),
        "leader_hint propagates"
    );
    assert_eq!(
        handler.vote_calls.load(Ordering::SeqCst),
        1,
        "exactly one server call"
    );

    shutdown.notify_one();
    srv_handle.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Scenario: connection-retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn connection_retry() {
    // Bind a free port but DO NOT start the server yet — the first client
    // attempts will fail at the TCP layer. We then start the server after
    // a delay and verify the client recovers via its connect-time retry.
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut cfg = client_config(endpoint);
    // Lift retry budget so the deferred server start fits comfortably.
    cfg.max_retries = 30;
    cfg.initial_backoff = Duration::from_millis(50);
    cfg.max_backoff = Duration::from_millis(200);
    cfg.connect_timeout = Duration::from_millis(200);
    let client = Arc::new(RaftGrpcClient::new(cfg));

    let client_for_rpc = client.clone();
    let rpc_task = tokio::spawn(async move {
        client_for_rpc
            .send_vote(NodeId(SERVER_NODE_ID), sample_vote_request())
            .await
    });

    // Hold the server back so the first connection attempt definitely
    // fails. 250ms is well past the client's first connect_timeout.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let handler = StubHandler::new();
    let (shutdown, srv_handle) = spawn_plain_server(addr, handler.clone());

    // Wait for the deferred RPC to complete; tokio::time::timeout caps the
    // test at 10s so a hung retry loop fails loudly.
    let resp = tokio::time::timeout(Duration::from_secs(10), rpc_task)
        .await
        .expect("rpc completes within timeout")
        .expect("rpc task did not panic")
        .expect("vote rpc succeeds after server start");
    assert!(resp.vote_granted);
    assert_eq!(handler.vote_calls.load(Ordering::SeqCst), 1);

    shutdown.notify_one();
    srv_handle.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Scenario: concurrent-rpcs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_rpcs() {
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let handler = StubHandler::new();
    let (shutdown, srv_handle) = spawn_plain_server(addr, handler.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let client = Arc::new(RaftGrpcClient::new(client_config(endpoint)));

    let mut set: JoinSet<XResult<FetchResponse>> = JoinSet::new();
    for replica in 0..50u64 {
        let client = client.clone();
        let req = sample_fetch_request(replica + 100); // replica IDs 100..150
        set.spawn(async move { client.send_fetch(NodeId(SERVER_NODE_ID), req).await });
    }

    let mut completed = 0usize;
    let mut watermarks: Vec<u64> = Vec::with_capacity(50);
    while let Some(joined) = set.join_next().await {
        let resp = joined
            .expect("rpc task did not panic")
            .expect("fetch rpc succeeds");
        assert_eq!(resp.cluster_id, TEST_CLUSTER_ID);
        assert_eq!(resp.leader_id, NodeId(SERVER_NODE_ID));
        assert!(resp.entries.is_empty());
        assert!(resp.diverging_epoch.is_none());
        watermarks.push(resp.high_watermark.0);
        completed += 1;
    }

    assert_eq!(completed, 50, "all 50 RPCs completed");
    assert_eq!(
        handler.fetch_calls.load(Ordering::SeqCst),
        50,
        "server saw 50 calls"
    );

    // The server stamps each response with the pre-increment value of the
    // counter, so every RPC must see a unique watermark in [0, 50). This
    // catches response cross-talk where one client sees another's reply.
    watermarks.sort_unstable();
    let unique: std::collections::HashSet<_> = watermarks.iter().copied().collect();
    assert_eq!(unique.len(), 50, "no two responses shared a watermark");
    assert_eq!(*watermarks.first().unwrap(), 0);
    assert_eq!(*watermarks.last().unwrap(), 49);

    // Connection pool should hold exactly one channel for the single peer
    // even after 50 concurrent uses — verifies pool sharing (not per-RPC
    // reconnect).
    assert_eq!(client.pool_size().await, 1, "single pooled channel reused");

    shutdown.notify_one();
    srv_handle.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Scenario: tls-transport
// ---------------------------------------------------------------------------

/// Generate a self-signed cert covering `localhost` and `127.0.0.1`.
fn issue_localhost_cert() -> (Vec<u8>, Vec<u8>) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert =
        rcgen::generate_simple_self_signed(subject_alt_names).expect("rcgen self-signed cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();
    (cert_pem, key_pem)
}

#[tokio::test]
async fn tls_transport() {
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    // Use the SNI-friendly hostname so the client's domain_name override
    // matches the cert SAN.
    let endpoint = format!("https://localhost:{port}");

    let (cert_pem, key_pem) = issue_localhost_cert();
    let temp = tempfile::tempdir().expect("tempdir");
    let cert_path = temp.path().join("cert.pem");
    let key_path = temp.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    let cluster = ClusterConfig {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        node_id: NodeId(SERVER_NODE_ID),
        listen_addr: addr.to_string(),
        peers: Vec::new(),
        voters: vec![
            xraft_core::config::VoterConfig {
                node_id: SERVER_NODE_ID,
                directory_id: "00000000-0000-0000-0000-000000000001".to_string(),
                host: "localhost".to_string(),
                port,
            },
            xraft_core::config::VoterConfig {
                node_id: CLIENT_NODE_ID,
                directory_id: "00000000-0000-0000-0000-000000000002".to_string(),
                host: "localhost".to_string(),
                port: pick_free_port(),
            },
        ],
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        fetch_interval_ms: 50,
        tick_interval_ms: 10,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir: std::path::PathBuf::from("data"),
        snapshot_retention_count: 3,
        tls_enabled: true,
        tls_cert_path: Some(cert_path.clone()),
        tls_key_path: Some(key_path.clone()),
        tls_ca_path: Some(cert_path.clone()),
        tls_domain_name: Some("localhost".to_string()),
        connect_timeout_ms: 2_000,
        rpc_timeout_ms: 5_000,
        max_rpc_retries: 3,
        retry_initial_backoff_ms: 50,
        retry_max_backoff_ms: 400,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    };

    // Build the server transport (it owns the TLS config + handler).
    let handler = StubHandler::new();
    let server_cfg = GrpcTransportConfig::from_cluster_config(&cluster).unwrap();
    let server_transport: Arc<GrpcTransport<StubHandler>> =
        Arc::new(GrpcTransport::new(server_cfg, handler.clone()));
    let serve_handle = tokio::spawn(server_transport.clone().start_server());
    wait_for_listening(addr, Duration::from_secs(3)).await;

    // Build a client transport pointing back at the server.
    let mut client_cluster = cluster.clone();
    client_cluster.node_id = NodeId(CLIENT_NODE_ID);
    let tls = Arc::new(TlsTransportConfig::from_cluster_config(&client_cluster).unwrap());
    let mut client_cfg = client_config(endpoint);
    client_cfg.tls = Some(tls);
    client_cfg.connect_timeout = Duration::from_secs(2);
    client_cfg.rpc_timeout = Duration::from_secs(5);
    let client = RaftGrpcClient::new(client_cfg);

    let resp = client
        .send_vote(NodeId(SERVER_NODE_ID), sample_vote_request())
        .await
        .expect("vote rpc over TLS succeeds");
    assert!(resp.vote_granted);
    assert_eq!(resp.term, Term(42));
    assert_eq!(handler.vote_calls.load(Ordering::SeqCst), 1);

    server_transport.shutdown();
    let join_result = tokio::time::timeout(Duration::from_secs(5), serve_handle)
        .await
        .expect("tls server task completes within shutdown timeout");
    let server_result = join_result.expect("tls server task did not panic");
    server_result.expect("tls server reported graceful shutdown");
}

// ---------------------------------------------------------------------------
// Scenario: tls-transport (cert + key sufficient, no explicit CA)
// ---------------------------------------------------------------------------
//
// Confirms the workstream-brief contract that supplying only `tls_cert_path`
// and `tls_key_path` is sufficient to bring up a working TLS-enabled cluster.
// `tls_ca_path` is intentionally NOT set; the transport must reuse the
// server's own cert as the client-side trust anchor.

#[tokio::test]
async fn tls_transport_cert_and_key_only() {
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("https://localhost:{port}");

    let (cert_pem, key_pem) = issue_localhost_cert();
    let temp = tempfile::tempdir().expect("tempdir");
    let cert_path = temp.path().join("cert.pem");
    let key_path = temp.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    let cluster = ClusterConfig {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        node_id: NodeId(SERVER_NODE_ID),
        listen_addr: addr.to_string(),
        peers: Vec::new(),
        voters: vec![
            xraft_core::config::VoterConfig {
                node_id: SERVER_NODE_ID,
                directory_id: "00000000-0000-0000-0000-000000000001".to_string(),
                host: "localhost".to_string(),
                port,
            },
            xraft_core::config::VoterConfig {
                node_id: CLIENT_NODE_ID,
                directory_id: "00000000-0000-0000-0000-000000000002".to_string(),
                host: "localhost".to_string(),
                port: pick_free_port(),
            },
        ],
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        fetch_interval_ms: 50,
        tick_interval_ms: 10,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir: std::path::PathBuf::from("data"),
        snapshot_retention_count: 3,
        tls_enabled: true,
        tls_cert_path: Some(cert_path.clone()),
        tls_key_path: Some(key_path.clone()),
        // Brief's "minimum config" — no CA path. Transport must fall back to
        // the server's own cert as trust anchor.
        tls_ca_path: None,
        tls_domain_name: Some("localhost".to_string()),
        connect_timeout_ms: 2_000,
        rpc_timeout_ms: 5_000,
        max_rpc_retries: 3,
        retry_initial_backoff_ms: 50,
        retry_max_backoff_ms: 400,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    };

    let handler = StubHandler::new();
    let server_cfg = GrpcTransportConfig::from_cluster_config(&cluster).unwrap();
    let server_transport: Arc<GrpcTransport<StubHandler>> =
        Arc::new(GrpcTransport::new(server_cfg, handler.clone()));
    let serve_handle = tokio::spawn(server_transport.clone().start_server());
    wait_for_listening(addr, Duration::from_secs(3)).await;

    let mut client_cluster = cluster.clone();
    client_cluster.node_id = NodeId(CLIENT_NODE_ID);
    let tls = Arc::new(TlsTransportConfig::from_cluster_config(&client_cluster).unwrap());
    assert!(
        tls.ca_cert_pem.is_some(),
        "ca_cert_pem must fall back to server cert when tls_ca_path is unset"
    );
    let mut client_cfg = client_config(endpoint);
    client_cfg.tls = Some(tls);
    client_cfg.connect_timeout = Duration::from_secs(2);
    client_cfg.rpc_timeout = Duration::from_secs(5);
    let client = RaftGrpcClient::new(client_cfg);

    let resp = client
        .send_vote(NodeId(SERVER_NODE_ID), sample_vote_request())
        .await
        .expect("vote rpc over TLS succeeds with cert+key only");
    assert!(resp.vote_granted);
    assert_eq!(resp.term, Term(42));
    assert_eq!(handler.vote_calls.load(Ordering::SeqCst), 1);

    server_transport.shutdown();
    let join_result = tokio::time::timeout(Duration::from_secs(5), serve_handle)
        .await
        .expect("tls server task completes within shutdown timeout");
    let server_result = join_result.expect("tls server task did not panic");
    server_result.expect("tls server reported graceful shutdown");
}

// ---------------------------------------------------------------------------
// Scenario: pre-vote-roundtrip
// ---------------------------------------------------------------------------
//
// Exercises the PreVote unary RPC end-to-end: a `PreVoteRequest` sent via
// `RaftGrpcClient::send_pre_vote` reaches the server, dispatches into
// `RaftMessageHandler::handle_pre_vote`, and the response decodes back into
// the canonical Rust type with every field intact.

#[tokio::test]
async fn pre_vote_roundtrip() {
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let handler = StubHandler::new();
    let (shutdown, srv_handle) = spawn_plain_server(addr, handler.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let client = RaftGrpcClient::new(client_config(endpoint));

    let req = PreVoteRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        next_term: Term(99),
        candidate_id: NodeId(CLIENT_NODE_ID),
        last_log_index: LogIndex(77),
        last_log_term: Term(98),
    };
    let resp = client
        .send_pre_vote(NodeId(SERVER_NODE_ID), req.clone())
        .await
        .expect("pre_vote rpc succeeds");

    // The stub echoes cluster_id / leader_epoch and copies next_term into
    // term; vote_granted is true and leader_hint is None.
    assert_eq!(resp.cluster_id, req.cluster_id, "cluster_id roundtrip");
    assert_eq!(
        resp.leader_epoch, req.leader_epoch,
        "leader_epoch roundtrip"
    );
    assert_eq!(resp.term, req.next_term, "term mirrors next_term");
    assert!(resp.vote_granted, "stub handler grants the pre-vote");
    assert_eq!(resp.leader_hint, None, "no leader_hint expected");
    assert_eq!(
        handler.pre_vote_calls.load(Ordering::SeqCst),
        1,
        "exactly one server pre_vote call"
    );
    assert_eq!(
        handler.vote_calls.load(Ordering::SeqCst),
        0,
        "pre_vote does not invoke vote"
    );

    shutdown.notify_one();
    srv_handle.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Scenario: fetch-snapshot-streaming
// ---------------------------------------------------------------------------
//
// Exercises the FetchSnapshot server-streaming RPC end-to-end. The stub
// emits a two-chunk stream where the first chunk carries `SnapshotMeta`
// and `done = false`, and the second carries no metadata and `done = true`.
// The client consumes the stream and we assert chunk count, ordering,
// metadata propagation, and the final `done` flag.

#[tokio::test]
async fn fetch_snapshot_streaming() {
    use futures::StreamExt as _;

    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let handler = StubHandler::new();
    let (shutdown, srv_handle) = spawn_plain_server(addr, handler.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let client = RaftGrpcClient::new(client_config(endpoint));

    let req = FetchSnapshotRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        replica_id: NodeId(CLIENT_NODE_ID),
        snapshot_id: "snap-test".to_string(),
        offset: 0,
        max_bytes: 0,
    };
    let mut stream = client
        .send_fetch_snapshot(NodeId(SERVER_NODE_ID), req)
        .await
        .expect("fetch_snapshot rpc initial response succeeds");

    let mut chunks: Vec<FetchSnapshotChunk> = Vec::new();
    while let Some(item) = stream.next().await {
        let chunk = item.expect("chunk decodes without transport error");
        chunks.push(chunk);
    }

    assert_eq!(chunks.len(), 2, "stub emits exactly two chunks");
    assert_eq!(chunks[0].chunk_index, 0, "first chunk has chunk_index 0");
    assert_eq!(chunks[1].chunk_index, 1, "second chunk has chunk_index 1");

    // First chunk carries metadata.
    let meta = chunks[0]
        .metadata
        .as_ref()
        .expect("first chunk must carry SnapshotMeta");
    assert_eq!(meta.id, "snap-test");
    assert_eq!(meta.last_included_index, LogIndex(123));
    assert_eq!(meta.last_included_term, Term(4));
    assert_eq!(meta.size_bytes, Some(12));
    assert!(!chunks[0].done, "non-final chunk has done = false");
    assert_eq!(chunks[0].data, b"snap-part-1-");

    // Final chunk: no metadata, done = true, terminal payload.
    assert!(
        chunks[1].metadata.is_none(),
        "non-first chunk must omit SnapshotMeta"
    );
    assert!(chunks[1].done, "final chunk has done = true");
    assert_eq!(chunks[1].data, b"end");

    assert_eq!(
        handler.fetch_snapshot_calls.load(Ordering::SeqCst),
        1,
        "exactly one server fetch_snapshot call"
    );

    shutdown.notify_one();
    srv_handle.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Scenario: hostname-listen-addr (iter-2 fix for prior evaluator finding)
// ---------------------------------------------------------------------------
//
// `ClusterConfig::validate_address` accepts hostnames such as
// `localhost:6000`, but the previous `GrpcTransport::start_server`
// parsed `listen_addr` as `std::net::SocketAddr` *before* binding and
// rejected any value that wasn't a literal IP. That made a perfectly
// valid `listen_addr = "localhost:<port>"` fail at startup. The fix
// delegates binding to `tokio::net::TcpListener::bind(&str)`, which
// walks DNS-resolved addresses. This test reserves a port via
// `pick_free_port`, configures `listen_addr = "localhost:<port>"`,
// starts the server, and proves a real RPC succeeds against the
// hostname-configured listener.

#[tokio::test]
async fn start_server_accepts_hostname_listen_addr() {
    let port = pick_free_port();
    // Pick an alternative free port for the second voter so the
    // ClusterConfig validation accepts the literal.
    let other_port = pick_free_port();
    // The probe target — uses 127.0.0.1 because the OS DNS resolver
    // consistently maps `localhost` to that loopback address on test
    // hosts.
    let probe_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let cluster = ClusterConfig {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        node_id: NodeId(SERVER_NODE_ID),
        // Hostname-form listen_addr — the gap the iter-1 evaluator flagged.
        listen_addr: format!("localhost:{port}"),
        peers: Vec::new(),
        voters: vec![
            xraft_core::config::VoterConfig {
                node_id: SERVER_NODE_ID,
                directory_id: "00000000-0000-0000-0000-000000000001".to_string(),
                host: "localhost".to_string(),
                port,
            },
            xraft_core::config::VoterConfig {
                node_id: CLIENT_NODE_ID,
                directory_id: "00000000-0000-0000-0000-000000000002".to_string(),
                host: "localhost".to_string(),
                port: other_port,
            },
        ],
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        fetch_interval_ms: 50,
        tick_interval_ms: 10,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir: std::path::PathBuf::from("data"),
        snapshot_retention_count: 3,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_domain_name: None,
        connect_timeout_ms: 2_000,
        rpc_timeout_ms: 5_000,
        max_rpc_retries: 3,
        retry_initial_backoff_ms: 50,
        retry_max_backoff_ms: 400,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    };

    let handler = StubHandler::new();
    let server_cfg = GrpcTransportConfig::from_cluster_config(&cluster)
        .expect("from_cluster_config accepts hostname listen_addr");
    let server_transport: Arc<GrpcTransport<StubHandler>> =
        Arc::new(GrpcTransport::new(server_cfg, handler.clone()));
    let serve_handle = tokio::spawn(server_transport.clone().start_server());

    // Verify the listener actually bound — i.e., the hostname resolved
    // and `tokio::net::TcpListener::bind` accepted it. If the prior bug
    // were still present, the spawned future would have returned a
    // `Config` error rather than holding open the port.
    wait_for_listening(probe_addr, Duration::from_secs(3)).await;

    let client = RaftGrpcClient::new(client_config(endpoint));
    let resp = client
        .send_vote(NodeId(SERVER_NODE_ID), sample_vote_request())
        .await
        .expect("vote rpc against hostname-bound server succeeds");
    assert!(resp.vote_granted);
    assert_eq!(handler.vote_calls.load(Ordering::SeqCst), 1);

    server_transport.shutdown();
    let join_result = tokio::time::timeout(Duration::from_secs(5), serve_handle)
        .await
        .expect("hostname-bound server task completes within shutdown timeout");
    let server_result = join_result.expect("hostname-bound server task did not panic");
    server_result.expect("hostname-bound server reported graceful shutdown");
}

// ---------------------------------------------------------------------------
// Scenario: legacy-peers-rejected (iter-2 fix for prior evaluator finding)
// ---------------------------------------------------------------------------
//
// `ClusterConfig::peer_endpoints` derives its `NodeId -> URL` map from
// `cluster.voters`, so a config that populates only the legacy
// `peers: Vec<String>` field silently produces an empty routing map.
// The previous `GrpcTransportConfig::from_cluster_config` swallowed
// that silently, so the transport would *appear* to construct and only
// fail later when a real `send_*` call had no endpoint for any peer.
// This test asserts construction now errors at the misconfig with an
// actionable message naming both `ClusterConfig.peers` and `voters`.

fn make_legacy_peers_only_cluster() -> ClusterConfig {
    ClusterConfig {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        node_id: NodeId(SERVER_NODE_ID),
        listen_addr: "127.0.0.1:0".to_string(),
        // Legacy field populated; voters left empty — the exact misconfig
        // shape the evaluator flagged.
        peers: vec!["10.0.0.2:6000".to_string(), "10.0.0.3:6000".to_string()],
        voters: Vec::new(),
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        fetch_interval_ms: 50,
        tick_interval_ms: 10,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir: std::path::PathBuf::from("data"),
        snapshot_retention_count: 3,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_domain_name: None,
        connect_timeout_ms: 2_000,
        rpc_timeout_ms: 5_000,
        max_rpc_retries: 3,
        retry_initial_backoff_ms: 50,
        retry_max_backoff_ms: 400,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    }
}

#[test]
fn from_cluster_config_rejects_legacy_peers_without_voters() {
    let cluster = make_legacy_peers_only_cluster();

    let err = GrpcTransportConfig::from_cluster_config(&cluster)
        .expect_err("legacy peers without voters MUST be rejected by transport config");
    let msg = err.to_string();
    assert!(
        msg.contains("ClusterConfig.peers"),
        "error must name the offending field: {msg}"
    );
    assert!(
        msg.contains("voters"),
        "error must point to the fix (populate voters): {msg}"
    );

    // The shared helper used by both transport + client pool must
    // surface the same error so misconfig is caught uniformly across
    // entry points.
    let err = peer_endpoints_from_cluster_config(&cluster)
        .expect_err("helper must reject the same misconfig");
    let msg = err.to_string();
    assert!(msg.contains("ClusterConfig.peers"));
    assert!(msg.contains("voters"));
}

#[test]
fn peer_endpoints_helper_accepts_single_node_bootstrap() {
    // Inverse check: a legitimate single-node bootstrap — BOTH peers and
    // voters empty — must NOT be rejected; the result is just an empty
    // map (no outbound peers), which is correct for bootstrap.
    let mut cluster = make_legacy_peers_only_cluster();
    cluster.peers = Vec::new();
    let endpoints = peer_endpoints_from_cluster_config(&cluster)
        .expect("single-node bootstrap (peers & voters both empty) must be accepted");
    assert!(
        endpoints.is_empty(),
        "single-node bootstrap has no outbound peers"
    );
}

// ---------------------------------------------------------------------------
// Scenario: fetch-snapshot-mid-stream-eviction (iter-2 fix for prior finding)
// ---------------------------------------------------------------------------
//
// The module-level pool contract on `RaftGrpcClient` says: observed
// transport errors evict the cached channel so the next RPC dials a
// fresh connection. The previous `send_fetch_snapshot` implementation
// honoured this on *initial-RPC* failures but not on *mid-stream*
// failures (its `stream.map(...)` was a pure sync mapping that had no
// way to touch the pool). This test drives a real wire-path
// mid-stream `Status::unavailable` and asserts:
//   1. the failure surfaces as an `Err` item in the client stream, and
//   2. the cached channel for the peer is evicted (pool_size drops to 0).

#[tokio::test]
async fn fetch_snapshot_mid_stream_transport_error_evicts_channel() {
    use futures::StreamExt as _;

    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    // Stub configured to emit a synthetic mid-stream transport error
    // after the first chunk. The server's adapter maps that to
    // Status::unavailable, which is the retriable code the client uses
    // to trigger channel eviction.
    let handler = StubHandler::with_mid_stream_error();
    let (shutdown, srv_handle) = spawn_plain_server(addr, handler.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let mut cfg = client_config(endpoint);
    // We are NOT trying to drive retry here — initial-RPC succeeds and the
    // failure is mid-stream. The retry budget only affects connect-time.
    cfg.max_retries = 0;
    let client = RaftGrpcClient::new(cfg);

    let req = FetchSnapshotRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        replica_id: NodeId(CLIENT_NODE_ID),
        snapshot_id: "snap-test".to_string(),
        offset: 0,
        max_bytes: 0,
    };
    let mut stream = client
        .send_fetch_snapshot(NodeId(SERVER_NODE_ID), req)
        .await
        .expect("initial send_fetch_snapshot succeeds (failure is mid-stream)");

    // First chunk: Ok.
    let first = stream
        .next()
        .await
        .expect("stream yields a first item")
        .expect("first chunk decodes ok");
    assert_eq!(first.chunk_index, 0, "first chunk arrives intact");

    // Pool MUST have cached the channel by now since at least one
    // RPC has completed.
    assert_eq!(
        client.pool_size().await,
        1,
        "channel cached after successful initial RPC + first chunk"
    );

    // Second item: the synthetic Err. This is the moment the new
    // code MUST evict the cached channel (per pool contract).
    let second = stream.next().await.expect("stream yields a second item");
    assert!(
        second.is_err(),
        "second item must be the mid-stream transport error"
    );
    // No more items.
    assert!(
        stream.next().await.is_none(),
        "stream terminates after the error item"
    );

    // The eviction is awaited inside `.then(...)`, which has already run
    // by the time the `Err` is delivered to the consumer. The pool
    // SHOULD now be empty.
    assert_eq!(
        client.pool_size().await,
        0,
        "retriable mid-stream transport error must evict the cached channel"
    );

    shutdown.notify_one();
    srv_handle.await.unwrap().unwrap();
}
