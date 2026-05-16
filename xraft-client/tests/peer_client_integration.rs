//! Integration tests for [`xraft_client::peer::PeerClient`] and
//! [`xraft_client::pool::ConnectionPool`] over a **real** tonic
//! `tonic::transport::Server` carrying real `RaftGrpcServer` traffic.
//!
//! The transport layer in `xraft-transport/tests/grpc_integration.rs`
//! already covers raw `RaftGrpcClient` reconnect behaviour. These
//! tests sit one layer up: they exercise the user-facing `PeerClient`
//! façade so a regression in the typed wrapper (or in the pool's
//! caching / redirect logic) is caught directly at the surface
//! consumers depend on.
//!
//! Scenarios covered:
//!
//! - `peer_client_reconnect_after_restart` — Stage 6.2 scenario
//!   "peer-client-reconnect": a cached `PeerClient` whose target
//!   peer briefly disappears (server stopped) and re-binds on the
//!   SAME port must transparently reconnect on the next RPC and
//!   succeed without the caller seeing the transient.
//! - `pool_fetch_via_leader_follows_advertised_leader` — Stage
//!   6.2 evaluator feedback item 4: `ConnectionPool::fetch_via_leader`
//!   issues a `Fetch` to `prefer`, observes `is_leader=false` with a
//!   different `leader_id`, transparently hops once to the
//!   advertised leader, and returns the leader's `is_leader=true`
//!   response.

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::error::Result as XResult;
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotChunk, FetchSnapshotRequest, PreVoteRequest,
    PreVoteResponse, VoteRequest, VoteResponse,
};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream};
use xraft_core::types::{LogIndex, NodeId, Term};

use xraft_transport::grpc_client::{RaftGrpcClient, RaftGrpcClientConfig};
use xraft_transport::grpc_server::RaftGrpcServer;

use xraft_client::pool::ConnectionPool;

const TEST_CLUSTER_ID: &str = "test-cluster";
const TEST_LEADER_EPOCH: u64 = 7;
const TEST_TERM: u64 = 42;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Bind an ephemeral port and KEEP the listener so the port stays
/// reserved through the hand-off to `spawn_plain_server_from_listener`.
///
/// Race-free alternative to `pick_free_port` + `spawn_plain_server`:
/// the original two-step (bind probe → drop → re-bind inside a spawned
/// task) opens a window for another process or parallel test to snatch
/// the port between the drop and the re-bind. That window is what
/// produced the iter 6 post-pass gate flake
/// "Fetch connect to peer 2 after 9 attempts: connect to peer 2:
/// transport error" on `pool_fetch_via_leader_follows_advertised_leader`
/// — the leader server's bind silently failed inside its spawned task
/// while the redirect Fetch retried into an empty port.
///
/// The returned `std::net::TcpListener` is consumed by
/// `spawn_plain_server_from_listener`, which converts it into a
/// `tokio::net::TcpListener` and feeds
/// `tonic::Server::serve_with_incoming_shutdown` — the same
/// pre-bound-listener pattern production uses in
/// `xraft-transport/src/grpc.rs::start_server_with_listener`.
fn bind_test_listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("listener local_addr");
    // tonic's `serve_with_incoming_shutdown` drives the listener with
    // tokio's async accept loop, which requires the underlying socket
    // to be in non-blocking mode. `tokio::net::TcpListener::from_std`
    // *does not* flip the flag for us — leaving the std listener in
    // blocking mode produces a `WouldBlock`-storm panic the first time
    // tonic polls the accept future.
    listener
        .set_nonblocking(true)
        .expect("set listener to non-blocking");
    (listener, addr)
}

/// Wait up to `timeout` for `addr` to accept TCP connections. Used
/// to bridge the race between `tonic::Server::serve_with_shutdown`
/// returning the spawned future and the OS-level accept queue being
/// ready.
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

/// Wait up to `timeout` for `addr` to STOP accepting TCP connections
/// (i.e., the previous bind has been released). Without this gate
/// the second `bind()` in the reconnect test can race against the
/// kernel's TIME_WAIT for the listening socket and spuriously
/// fail with `EADDRINUSE`.
async fn wait_for_port_free(addr: std::net::SocketAddr, timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if TcpListener::bind(addr).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port {addr} still bound after {timeout:?}");
}

/// Stub `RaftMessageHandler` whose `Fetch` reply identity is
/// configurable per instance — every other RPC just returns a canned
/// success so the tests stay focused on the routing/reconnect
/// behaviour they care about.
struct StubHandler {
    /// `leader_id` echoed in `FetchResponse`. Lets the redirect test
    /// host a "follower" that points to a different node.
    leader_id: NodeId,
    /// `is_leader` echoed in `FetchResponse`. Setting this to `false`
    /// turns the stub into a redirecting follower for the
    /// `pool_fetch_via_leader_*` test.
    is_leader: bool,
    /// Number of `Fetch` RPCs handled; lets tests verify the
    /// transparent-redirect path actually visited the second peer.
    fetch_calls: AtomicU64,
    /// Number of `Vote` RPCs handled; the reconnect test asserts the
    /// second post-restart RPC reached the freshly-rebound server.
    vote_calls: AtomicU64,
}

impl StubHandler {
    fn leader(node_id: u64) -> Arc<Self> {
        Arc::new(Self {
            leader_id: NodeId(node_id),
            is_leader: true,
            fetch_calls: AtomicU64::new(0),
            vote_calls: AtomicU64::new(0),
        })
    }

    fn follower_pointing_to(advertised_leader: u64) -> Arc<Self> {
        Arc::new(Self {
            leader_id: NodeId(advertised_leader),
            is_leader: false,
            fetch_calls: AtomicU64::new(0),
            vote_calls: AtomicU64::new(0),
        })
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
            leader_hint: Some(self.leader_id),
        })
    }

    async fn handle_pre_vote(&self, req: PreVoteRequest) -> XResult<PreVoteResponse> {
        Ok(PreVoteResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            term: req.next_term,
            vote_granted: true,
            leader_hint: Some(self.leader_id),
        })
    }

    async fn handle_fetch(&self, req: FetchRequest) -> XResult<FetchResponse> {
        self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        Ok(FetchResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            leader_id: self.leader_id,
            high_watermark: LogIndex(99),
            entries: Vec::new(),
            diverging_epoch: None,
            snapshot_redirect: None,
            is_leader: self.is_leader,
        })
    }

    async fn handle_fetch_snapshot(
        &self,
        _req: FetchSnapshotRequest,
    ) -> XResult<SnapshotChunkStream> {
        let stream: SnapshotChunkStream = Box::pin(futures::stream::iter(Vec::<
            XResult<FetchSnapshotChunk>,
        >::new()));
        Ok(stream)
    }
}

fn sample_vote_request(candidate: u64) -> VoteRequest {
    VoteRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        term: Term(TEST_TERM),
        candidate_id: NodeId(candidate),
        last_log_index: LogIndex(0),
        last_log_term: Term(0),
    }
}

fn sample_fetch_request(replica: u64) -> FetchRequest {
    FetchRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        replica_id: NodeId(replica),
        fetch_offset: LogIndex(0),
        last_fetched_epoch: Term(0),
    }
}

fn spawn_plain_server<H: RaftMessageHandler + Send + Sync + 'static>(
    addr: std::net::SocketAddr,
    handler: Arc<H>,
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

/// Spawn a tonic server that adopts an ALREADY-BOUND `TcpListener`
/// instead of re-binding the address inside the spawned task.
///
/// Paired with `bind_test_listener` to close the bind race that the
/// `pick_free_port` + `spawn_plain_server` pair leaves open between
/// dropping the probe listener and rebinding inside the spawn. Used by
/// `pool_fetch_via_leader_follows_advertised_leader` where two servers
/// must come up concurrently and a stolen port surfaces as a
/// "connect to peer N: transport error" retry storm during the
/// redirect hop.
///
/// Mirrors `xraft-transport/src/grpc.rs::start_server_with_listener`
/// which is the production path for the same pattern.
fn spawn_plain_server_from_listener<H: RaftMessageHandler + Send + Sync + 'static>(
    std_listener: TcpListener,
    handler: Arc<H>,
) -> (
    Arc<Notify>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    let svc = RaftGrpcServer::new(handler).into_service();
    let handle = tokio::spawn(async move {
        let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
            .expect("convert std TcpListener to tokio TcpListener");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async move {
                shutdown_clone.notified().await;
            })
            .await
    });
    (shutdown, handle)
}

/// Build a one-peer `RaftGrpcClient` config aimed at `endpoint`.
fn single_peer_client_config(peer: NodeId, endpoint: String) -> RaftGrpcClientConfig {
    let mut peer_endpoints = HashMap::new();
    peer_endpoints.insert(peer, endpoint);
    RaftGrpcClientConfig {
        peer_endpoints,
        connect_timeout: Duration::from_millis(500),
        rpc_timeout: Duration::from_secs(2),
        max_retries: 30,
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(300),
        max_message_size: 4 * 1024 * 1024,
        tls: None,
    }
}

/// Build a two-peer `ClusterConfig` for the redirect test. Node 1
/// is the "follower" listening on `follower_port`; node 2 is the
/// "leader" listening on `leader_port`.
fn two_peer_cluster(follower_port: u16, leader_port: u16) -> ClusterConfig {
    ClusterConfig {
        node_id: NodeId(99),
        cluster_id: TEST_CLUSTER_ID.into(),
        listen_addr: "127.0.0.1:0".into(),
        peers: vec![],
        voters: vec![
            VoterConfig {
                node_id: 1,
                directory_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                host: "127.0.0.1".into(),
                port: follower_port,
            },
            VoterConfig {
                node_id: 2,
                directory_id: "550e8400-e29b-41d4-a716-446655440001".into(),
                host: "127.0.0.1".into(),
                port: leader_port,
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
        connect_timeout_ms: 500,
        rpc_timeout_ms: 2_000,
        max_rpc_retries: 8,
        retry_initial_backoff_ms: 50,
        retry_max_backoff_ms: 300,
        max_message_size: 4 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    }
}

// ---------------------------------------------------------------------------
// Scenario: peer-client-reconnect
// ---------------------------------------------------------------------------

/// Stage 6.2 brief: "Given a PeerClient connected to a peer that
/// restarts, When the next RPC is sent, Then the client reconnects
/// automatically and the RPC succeeds."
///
/// The transport layer already has a `connection_retry` test for the
/// raw `RaftGrpcClient`; this test verifies the integration through
/// `PeerClient`. The shared `RaftGrpcClient` underneath PeerClient is
/// what owns the reconnect machinery, so this is an end-to-end
/// contract test that the PeerClient surface (which is what consumers
/// touch) honours the same recovery semantics.
#[tokio::test]
async fn peer_client_reconnect_after_restart() {
    let port = pick_free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let peer = NodeId(1);

    // Phase 1: bring up the server and confirm one Vote round-trips.
    let handler_a = StubHandler::leader(1);
    let (shutdown_a, srv_a) = spawn_plain_server(addr, handler_a.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let client = Arc::new(RaftGrpcClient::new(single_peer_client_config(
        peer,
        endpoint.clone(),
    )));
    let peer_client = xraft_client::peer::PeerClient::new(peer, client.clone());

    let resp = peer_client
        .vote(sample_vote_request(2))
        .await
        .expect("vote rpc succeeds against the first server");
    assert!(resp.vote_granted);
    assert_eq!(handler_a.vote_calls.load(Ordering::SeqCst), 1);

    // Phase 2: stop the server. The PeerClient is cached and holds
    // its (now-stale) gRPC channel; the next RPC must transparently
    // recover.
    shutdown_a.notify_one();
    srv_a
        .await
        .expect("first server join")
        .expect("first server clean exit");
    wait_for_port_free(addr, Duration::from_secs(3)).await;

    // Phase 3: re-bind a fresh server on the same port with a new
    // handler instance — proves the reconnected client is talking
    // to the new process, not a stale connection.
    let handler_b = StubHandler::leader(1);
    let (shutdown_b, srv_b) = spawn_plain_server(addr, handler_b.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    let resp = tokio::time::timeout(
        Duration::from_secs(10),
        peer_client.vote(sample_vote_request(2)),
    )
    .await
    .expect("vote rpc completes within 10s after server restart")
    .expect("vote rpc succeeds after reconnect");

    assert!(resp.vote_granted, "post-restart vote granted");
    assert_eq!(
        handler_b.vote_calls.load(Ordering::SeqCst),
        1,
        "post-restart vote hits the NEW server instance, not a stale connection"
    );

    shutdown_b.notify_one();
    srv_b
        .await
        .expect("second server join")
        .expect("second server clean exit");
}

// ---------------------------------------------------------------------------
// Scenario: pool-fetch-via-leader transparent redirect
// ---------------------------------------------------------------------------

/// Stage 6.2 evaluator-feedback iter 1 item 4: when a `Fetch` is
/// sent to a follower (`is_leader=false`) that advertises a
/// different `leader_id`, `ConnectionPool::fetch_via_leader` must
/// transparently retry against the advertised leader within one hop
/// and surface the leader's response to the caller.
#[tokio::test]
async fn pool_fetch_via_leader_follows_advertised_leader() {
    // Use the held-listener variant so the leader / follower ports
    // can't be stolen between `bind` and `serve`. See
    // `bind_test_listener` / `spawn_plain_server_from_listener` doc
    // comments for why the drop-then-rebind pattern flakes under
    // parallel `cargo test`.
    let (follower_listener, follower_addr) = bind_test_listener();
    let (leader_listener, leader_addr) = bind_test_listener();
    let follower_port = follower_addr.port();
    let leader_port = leader_addr.port();

    // Stand up two servers: node 1 = follower that points to leader_id=2,
    //                       node 2 = leader (is_leader=true).
    let follower_handler = StubHandler::follower_pointing_to(2);
    let leader_handler = StubHandler::leader(2);
    let (sh_f, jh_f) =
        spawn_plain_server_from_listener(follower_listener, follower_handler.clone());
    let (sh_l, jh_l) = spawn_plain_server_from_listener(leader_listener, leader_handler.clone());
    wait_for_listening(follower_addr, Duration::from_secs(2)).await;
    wait_for_listening(leader_addr, Duration::from_secs(2)).await;

    // Build the pool over both peers. We're constructing it manually
    // (not via `from_cluster_config`) so we can override the
    // transport client to point at the test ports without parsing
    // through ClusterConfig URL plumbing.
    let cluster = two_peer_cluster(follower_port, leader_port);
    let pool = ConnectionPool::from_cluster_config(&cluster)
        .expect("pool builds from test cluster config");

    // Sanity: redirect path. Issuing fetch_via_leader(prefer=node 1)
    // should hit the follower, see is_leader=false with leader_id=2,
    // and hop to node 2.
    let resp = pool
        .fetch_via_leader(NodeId(1), sample_fetch_request(99))
        .await
        .expect("fetch_via_leader succeeds");

    assert!(
        resp.is_leader,
        "final response must come from the leader (is_leader=true)"
    );
    assert_eq!(
        resp.leader_id,
        NodeId(2),
        "final response carries leader id 2"
    );
    assert_eq!(
        follower_handler.fetch_calls.load(Ordering::SeqCst),
        1,
        "follower received exactly one Fetch (the initial probe)"
    );
    assert_eq!(
        leader_handler.fetch_calls.load(Ordering::SeqCst),
        1,
        "leader received exactly one Fetch (the redirect hop)"
    );

    // The leader's reply should have populated the pool's leader
    // hint cache so a follow-up call routes straight to the leader
    // without re-querying the follower.
    assert_eq!(
        pool.leader_hint(),
        Some(NodeId(2)),
        "pool's leader hint cache is populated by the leader's is_leader=true reply"
    );

    let resp2 = pool
        .fetch_via_leader(NodeId(1), sample_fetch_request(99))
        .await
        .expect("second fetch_via_leader succeeds");
    assert!(resp2.is_leader);
    assert_eq!(
        follower_handler.fetch_calls.load(Ordering::SeqCst),
        1,
        "follower NOT contacted again — cached hint routed directly to leader"
    );
    assert_eq!(
        leader_handler.fetch_calls.load(Ordering::SeqCst),
        2,
        "leader received the second Fetch as the cached hint target"
    );

    sh_f.notify_one();
    sh_l.notify_one();
    let _ = jh_f
        .await
        .expect("follower join")
        .map_err(|e| e.to_string());
    let _ = jh_l.await.expect("leader join").map_err(|e| e.to_string());
}
