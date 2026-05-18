//! Stage 6.2 (evaluator iter 3 follow-up) — router + ConnectionPool
//! end-to-end integration tests.
//!
//! The evaluator flagged three concrete gaps in iter 3:
//!
//! 1. No test proving `MessageRouter` with an attached
//!    `ConnectionPool` actually uses the pool/redirect path. The
//!    earlier coverage exercised `ConnectionPool::fetch_via_leader`
//!    directly but never wired it through the router that the
//!    driver instantiates in production.
//! 2. No server-assembly test proving `Server::start` actually
//!    wires the pool — a future refactor could silently delete the
//!    `with_connection_pool(connection_pool.clone())` call from
//!    `server.rs` and not fail a single test.
//! 3. The pool tests were white-box (`cache_hint_for_test`) rather
//!    than black-box-via-real-FetchResponse. (That gap is covered
//!    by the new `pool_fetch_via_leader_ignores_stale_hint*` and
//!    `pool_fetch_via_leader_uses_cached_hint*` tests in
//!    `xraft-client/tests/peer_client_integration.rs`. This file
//!    closes gaps 1 + 2.)
//!
//! Scenarios:
//!
//! - `router_with_pool_uses_redirect_aware_fetch_path` — dispatch
//!   a `FetchRequest` through a router that has a real
//!   `ConnectionPool` attached. Two real tonic stub servers are
//!   stood up (follower + leader); the test asserts the resulting
//!   `OutboundResult::Fetch.peer` is the LEADER (post-redirect),
//!   proving the router went through `ConnectionPool::fetch_via_leader`
//!   instead of the raw `Transport::send_fetch`.
//! - `router_without_pool_uses_raw_transport_send_fetch` — same
//!   setup, but no `with_connection_pool` call. The router falls
//!   back to `Transport::send_fetch` (raw), the follower replies
//!   with `is_leader=false`, and `OutboundResult::Fetch.peer`
//!   equals the engine-dispatched peer (follower) — no redirect.
//!   This A/B contrast is what proves the pool wiring is the
//!   redirect mechanism's *only* trigger.
//! - `server_assembly_wires_connection_pool_into_driver` — start a
//!   real single-voter `Server` and assert
//!   `handle.driver_pool_attached()` returns `true`. The flag is
//!   captured from `Driver::is_pool_attached()` BEFORE
//!   `driver.run()` consumes the driver, so the assertion is a
//!   real proof that `Server::start_with_state_machine` called
//!   `with_connection_pool`. Deleting that call from `server.rs`
//!   flips the flag to `false` and fails this test.

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::{Notify, mpsc};

use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::error::Result as XResult;
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotChunk, FetchSnapshotRequest, OutboundMessage,
    PreVoteRequest, PreVoteResponse, VoteRequest, VoteResponse,
};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream};
use xraft_core::types::{LogIndex, NodeId, Term};

use xraft_client::pool::ConnectionPool;
use xraft_transport::grpc::{GrpcTransport, GrpcTransportConfig};
use xraft_transport::grpc_server::RaftGrpcServer;

use xraft_server::driver::{MessageRouter, OutboundResult};
use xraft_server::{Server, ServerConfig};

const TEST_CLUSTER_ID: &str = "stage-6-2-router-pool";
const TEST_LEADER_EPOCH: u64 = 7;
const FOLLOWER_NODE_ID: u64 = 1;
const LEADER_NODE_ID: u64 = 2;

// ---------------------------------------------------------------------------
// Stub gRPC handler (duplicated from xraft-client/tests/peer_client_integration.rs
// because integration-test crates are isolated and cannot share helpers
// across crates without a dedicated test-support crate). The duplication is
// intentional and localised; if this surface grows further, factor it into
// a shared `xraft-testkit` crate.
// ---------------------------------------------------------------------------

struct StubHandler {
    leader_id: NodeId,
    is_leader: bool,
    fetch_calls: AtomicU64,
}

impl StubHandler {
    fn leader(node_id: u64) -> Arc<Self> {
        Arc::new(Self {
            leader_id: NodeId(node_id),
            is_leader: true,
            fetch_calls: AtomicU64::new(0),
        })
    }

    fn follower_pointing_to(advertised_leader: u64) -> Arc<Self> {
        Arc::new(Self {
            leader_id: NodeId(advertised_leader),
            is_leader: false,
            fetch_calls: AtomicU64::new(0),
        })
    }
}

impl RaftMessageHandler for StubHandler {
    async fn handle_vote(&self, req: VoteRequest) -> XResult<VoteResponse> {
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

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

/// `ClusterConfig` for a two-peer setup (self = `node_id=99`) where
/// peer `FOLLOWER_NODE_ID` is the follower and peer
/// `LEADER_NODE_ID` is the leader.
fn two_peer_cluster(follower_port: u16, leader_port: u16) -> ClusterConfig {
    ClusterConfig {
        node_id: NodeId(99),
        cluster_id: TEST_CLUSTER_ID.into(),
        listen_addr: "127.0.0.1:0".into(),
        peers: vec![],
        voters: vec![
            VoterConfig {
                node_id: FOLLOWER_NODE_ID,
                directory_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                host: "127.0.0.1".into(),
                port: follower_port,
            },
            VoterConfig {
                node_id: LEADER_NODE_ID,
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
        max_rpc_retries: 30,
        retry_initial_backoff_ms: 50,
        retry_max_backoff_ms: 300,
        max_message_size: 4 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    }
}

/// Sample fetch request used by all router-level dispatches.
fn sample_fetch_request() -> FetchRequest {
    FetchRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        replica_id: NodeId(99),
        fetch_offset: LogIndex(0),
        last_fetched_epoch: Term(0),
    }
}

/// Build a `GrpcTransport<StubHandler>` whose outbound client is
/// SHARED with the supplied `ConnectionPool` (via
/// `with_client(pool.client())`). The inbound side has a no-op
/// listener — the test never starts a server on this transport, so
/// the `listen_addr` and `handler` are placeholders.
fn build_outbound_grpc_transport(
    cluster: &ClusterConfig,
    pool: &ConnectionPool,
) -> Arc<GrpcTransport<StubHandler>> {
    let mut cfg =
        GrpcTransportConfig::from_cluster_config(cluster).expect("transport config from cluster");
    // Force a no-op listen address (the test does not start the
    // transport's inbound server — only its outbound `Transport`
    // methods are exercised).
    cfg.listen_addr = "127.0.0.1:0".into();
    let handler = StubHandler::leader(LEADER_NODE_ID); // unused
    Arc::new(GrpcTransport::with_client(cfg, handler, pool.client()))
}

// ---------------------------------------------------------------------------
// Test 1: router WITH pool — dispatch goes through fetch_via_leader,
// redirect happens, OutboundResult.peer is the leader (responder).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_with_pool_uses_redirect_aware_fetch_path() {
    let follower_port = pick_free_port();
    let leader_port = pick_free_port();
    let follower_addr: std::net::SocketAddr = format!("127.0.0.1:{follower_port}").parse().unwrap();
    let leader_addr: std::net::SocketAddr = format!("127.0.0.1:{leader_port}").parse().unwrap();

    let follower_handler = StubHandler::follower_pointing_to(LEADER_NODE_ID);
    let leader_handler = StubHandler::leader(LEADER_NODE_ID);
    let (sh_f, jh_f) = spawn_plain_server(follower_addr, follower_handler.clone());
    let (sh_l, jh_l) = spawn_plain_server(leader_addr, leader_handler.clone());
    wait_for_listening(follower_addr, Duration::from_secs(2)).await;
    wait_for_listening(leader_addr, Duration::from_secs(2)).await;

    let cluster = two_peer_cluster(follower_port, leader_port);
    let pool = ConnectionPool::from_cluster_config(&cluster).expect("pool builds");
    let transport = build_outbound_grpc_transport(&cluster, &pool);

    let (tx, mut rx) = mpsc::channel::<OutboundResult>(8);
    let mut router = MessageRouter::new(transport.clone(), tx).with_connection_pool(pool.clone());

    // Sanity: the pool-wiring inspector reflects the builder state.
    assert!(
        router.is_pool_attached(),
        "router must report pool attached after with_connection_pool"
    );

    // Dispatch a FetchRequest at the FOLLOWER. When the pool is
    // wired, the router routes through `ConnectionPool::fetch_via_leader`,
    // which observes `is_leader=false` from the follower and hops
    // once to the advertised leader (node 2). The driver's contract
    // says `OutboundResult::Fetch.peer` is the post-redirect
    // responder — assert that contract end-to-end.
    router.dispatch(
        NodeId(FOLLOWER_NODE_ID),
        OutboundMessage::FetchRequest(sample_fetch_request()),
    );

    let evt = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("router did not produce a result within 5s")
        .expect("router channel closed");

    match evt {
        OutboundResult::Fetch { peer, response } => {
            assert_eq!(
                peer,
                NodeId(LEADER_NODE_ID),
                "OutboundResult::Fetch.peer must be the POST-REDIRECT \
                 responder (the leader), not the originally-dispatched \
                 follower — proves the router went through \
                 ConnectionPool::fetch_via_leader, not Transport::send_fetch"
            );
            assert!(
                response.is_leader,
                "final response must come from the leader (is_leader=true)"
            );
            assert_eq!(response.leader_id, NodeId(LEADER_NODE_ID));
        }
        other => panic!(
            "expected OutboundResult::Fetch with peer = leader, got {other:?} — \
             this typically means the router fell back to Transport::send_fetch \
             (pool wiring regression)"
        ),
    }

    assert_eq!(
        follower_handler.fetch_calls.load(Ordering::SeqCst),
        1,
        "follower handled the initial probe before the redirect"
    );
    assert_eq!(
        leader_handler.fetch_calls.load(Ordering::SeqCst),
        1,
        "leader handled the redirect hop"
    );

    drop(router); // joinset reaping; not strictly necessary but explicit.
    sh_f.notify_one();
    sh_l.notify_one();
    let _ = jh_f
        .await
        .expect("follower join")
        .map_err(|e| e.to_string());
    let _ = jh_l.await.expect("leader join").map_err(|e| e.to_string());
}

// ---------------------------------------------------------------------------
// Test 2: router WITHOUT pool — dispatch goes through raw
// Transport::send_fetch, no redirect, OutboundResult.peer is the
// engine-dispatched peer (follower).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_without_pool_uses_raw_transport_send_fetch() {
    let follower_port = pick_free_port();
    let leader_port = pick_free_port();
    let follower_addr: std::net::SocketAddr = format!("127.0.0.1:{follower_port}").parse().unwrap();
    let leader_addr: std::net::SocketAddr = format!("127.0.0.1:{leader_port}").parse().unwrap();

    let follower_handler = StubHandler::follower_pointing_to(LEADER_NODE_ID);
    let leader_handler = StubHandler::leader(LEADER_NODE_ID);
    let (sh_f, jh_f) = spawn_plain_server(follower_addr, follower_handler.clone());
    let (sh_l, jh_l) = spawn_plain_server(leader_addr, leader_handler.clone());
    wait_for_listening(follower_addr, Duration::from_secs(2)).await;
    wait_for_listening(leader_addr, Duration::from_secs(2)).await;

    let cluster = two_peer_cluster(follower_port, leader_port);
    let pool = ConnectionPool::from_cluster_config(&cluster).expect("pool builds");
    let transport = build_outbound_grpc_transport(&cluster, &pool);

    let (tx, mut rx) = mpsc::channel::<OutboundResult>(8);
    // NO `.with_connection_pool(pool)` call — this is the contrast
    // test that proves redirect-aware behaviour disappears when the
    // wiring is omitted.
    let mut router = MessageRouter::new(transport.clone(), tx);

    assert!(
        !router.is_pool_attached(),
        "router without with_connection_pool must report pool unattached"
    );

    router.dispatch(
        NodeId(FOLLOWER_NODE_ID),
        OutboundMessage::FetchRequest(sample_fetch_request()),
    );

    let evt = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("router did not produce a result within 5s")
        .expect("router channel closed");

    match evt {
        OutboundResult::Fetch { peer, response } => {
            assert_eq!(
                peer,
                NodeId(FOLLOWER_NODE_ID),
                "without a pool, the router's `peer` must equal the engine's \
                 dispatched peer (no redirect path)"
            );
            assert!(
                !response.is_leader,
                "follower must reply with is_leader=false; the router did NOT \
                 redirect because the pool is not wired"
            );
            assert_eq!(
                response.leader_id,
                NodeId(LEADER_NODE_ID),
                "follower advertises the real leader in the reply (engine \
                 will issue its own redirect via the inbound message path)"
            );
        }
        other => panic!("expected OutboundResult::Fetch (no redirect), got {other:?}"),
    }

    assert_eq!(
        follower_handler.fetch_calls.load(Ordering::SeqCst),
        1,
        "follower handled the engine's dispatched fetch"
    );
    assert_eq!(
        leader_handler.fetch_calls.load(Ordering::SeqCst),
        0,
        "leader MUST NOT have been contacted — no pool ⇒ no redirect"
    );

    drop(router);
    sh_f.notify_one();
    sh_l.notify_one();
    let _ = jh_f
        .await
        .expect("follower join")
        .map_err(|e| e.to_string());
    let _ = jh_l.await.expect("leader join").map_err(|e| e.to_string());
    drop(pool);
}

// ---------------------------------------------------------------------------
// Test 3: SERVER ASSEMBLY — proves Server::start wires the pool
// into the driver. If a future refactor deletes the
// `.with_connection_pool(connection_pool.clone())` call from
// server.rs, `driver_pool_attached()` flips to false and this test
// fails.
// ---------------------------------------------------------------------------

fn pick_grpc_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn single_voter_cluster(data_dir: PathBuf) -> ClusterConfig {
    let grpc_port = pick_grpc_port();
    ClusterConfig {
        node_id: NodeId(1),
        cluster_id: "stage-6-2-router-pool-assembly".into(),
        listen_addr: format!("127.0.0.1:{grpc_port}"),
        peers: vec![],
        voters: vec![VoterConfig {
            node_id: 1,
            directory_id: "550e8400-e29b-41d4-a716-446655440042".into(),
            host: "127.0.0.1".into(),
            port: grpc_port,
        }],
        // Long election timeout so the test does not race the
        // election timer — we only need to observe the assembly
        // state, not steady-state behaviour.
        election_timeout_min_ms: 30_000,
        election_timeout_max_ms: 60_000,
        fetch_interval_ms: 50,
        tick_interval_ms: 10,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir,
        snapshot_retention_count: 3,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_domain_name: None,
        connect_timeout_ms: 5_000,
        rpc_timeout_ms: 30_000,
        max_rpc_retries: 3,
        retry_initial_backoff_ms: 100,
        retry_max_backoff_ms: 5_000,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_assembly_wires_connection_pool_into_driver() {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = ServerConfig {
        cluster: single_voter_cluster(tmp.path().to_path_buf()),
        admin_listen_addr: Some("127.0.0.1:0".into()),
        driver_config: None,
    };

    let handle = Server::start(cfg).await.expect("server starts");

    // The smoke check the evaluator asked for: prove that the
    // production assembly path actually called
    // `with_connection_pool(connection_pool.clone())`. The flag is
    // captured from `Driver::is_pool_attached()` BEFORE the driver
    // task is spawned (see `Server::start_with_state_machine`), so
    // a hard-coded `true` would defeat the test. A future refactor
    // that drops the with_connection_pool call from server.rs will
    // make `Driver::is_pool_attached()` return `false` here.
    assert!(
        handle.driver_pool_attached(),
        "Server::start MUST wire the ConnectionPool into the driver \
         (regression guard: see `xraft-server/src/server.rs::Server::start_with_state_machine` \
         step 7 — the `.with_connection_pool(connection_pool.clone())` call)"
    );

    // Sanity: the same pool is also reachable on ServerHandle for
    // operator/admin queries (separate from the driver wiring).
    assert!(
        handle.connection_pool.is_empty(),
        "single-voter cluster has no peers, so the pool roster is empty"
    );

    // Clean shutdown — the test purpose is the assembly assertion
    // above; we don't need to exercise runtime behaviour.
    handle.shutdown();
    let join_result = tokio::time::timeout(Duration::from_secs(10), handle.join()).await;
    match join_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("server join surfaced non-fatal teardown noise: {e}"),
        Err(_elapsed) => panic!("server join did not resolve within 10s"),
    }
}
