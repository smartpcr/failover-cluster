//! Integration test for Stage 6.2 evaluator feedback iter 1 item 6:
//! prove that a `PeerClient` recovers from a peer restart by
//! exercising **real** RPCs against a tonic gRPC server that is
//! killed and then re-spawned on the same port. The transport
//! layer's per-peer retry-with-jitter loop is what performs the
//! reconnect; this test confirms the wiring carries through the
//! `PeerClient` façade without requiring a second handshake at the
//! client crate layer.
//!
//! Scenario (from the workstream brief, "peer-client-reconnect"):
//! Given a PeerClient connected to a peer that restarts, When the
//! next RPC is sent, Then the client reconnects automatically and
//! the RPC succeeds.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use xraft_core::error::Result as XResult;
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotRequest, PreVoteRequest, PreVoteResponse,
    VoteRequest, VoteResponse,
};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream};
use xraft_core::types::{LogIndex, NodeId, Term};

use xraft_client::peer::PeerClient;
use xraft_transport::grpc_client::{RaftGrpcClient, RaftGrpcClientConfig};
use xraft_transport::grpc_server::RaftGrpcServer;

const TEST_CLUSTER_ID: &str = "test-cluster";
const TEST_LEADER_EPOCH: u64 = 7;
const SERVER_NODE_ID: u64 = 1;
const CLIENT_NODE_ID: u64 = 2;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[derive(Default)]
struct StubHandler {
    vote_calls: AtomicU64,
}

impl StubHandler {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
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
            // Critical for this test: prove the leader-hint cache
            // populates from real RPC responses (not just from the
            // `#[cfg(test)]` cache-poking helper).
            leader_hint: Some(NodeId(SERVER_NODE_ID)),
        })
    }

    async fn handle_pre_vote(&self, req: PreVoteRequest) -> XResult<PreVoteResponse> {
        Ok(PreVoteResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            term: req.next_term,
            vote_granted: true,
            leader_hint: None,
        })
    }

    async fn handle_fetch(&self, req: FetchRequest) -> XResult<FetchResponse> {
        Ok(FetchResponse {
            cluster_id: req.cluster_id,
            leader_epoch: req.leader_epoch,
            leader_id: NodeId(SERVER_NODE_ID),
            high_watermark: LogIndex(0),
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
        use futures_util::stream;
        Ok(Box::pin(stream::empty()))
    }
}

fn spawn_plain_server(
    addr: SocketAddr,
    handler: Arc<StubHandler>,
) -> (
    Arc<Notify>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let shutdown = Arc::new(Notify::new());
    let shutdown_for_task = shutdown.clone();
    let svc = RaftGrpcServer::new(handler).into_service();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                shutdown_for_task.notified().await;
            })
            .await
    });
    (shutdown, handle)
}

async fn wait_for_listening(addr: SocketAddr, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("server at {addr} did not start listening within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_port_release(addr: SocketAddr, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // A successful bind proves the previous serve task has fully
        // released the listener — drop the listener immediately so
        // the next spawn can take it.
        if TcpListener::bind(addr).is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("port {} did not release within {timeout:?}", addr.port());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn client_for(addr: SocketAddr) -> Arc<RaftGrpcClient> {
    let endpoint = format!("http://127.0.0.1:{}", addr.port());
    let mut peers = HashMap::new();
    peers.insert(NodeId(SERVER_NODE_ID), endpoint);
    // Reconnect budget: 30 retries × ≤200ms backoff ≈ 6s headroom for
    // a port-release + serve-restart cycle on slow CI runners.
    Arc::new(RaftGrpcClient::new(RaftGrpcClientConfig {
        peer_endpoints: peers,
        connect_timeout: Duration::from_millis(250),
        rpc_timeout: Duration::from_secs(2),
        max_retries: 30,
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        max_message_size: 1024 * 1024,
        tls: None,
    }))
}

fn sample_vote_request() -> VoteRequest {
    VoteRequest {
        cluster_id: TEST_CLUSTER_ID.to_string(),
        leader_epoch: TEST_LEADER_EPOCH,
        term: Term(3),
        candidate_id: NodeId(CLIENT_NODE_ID),
        last_log_index: LogIndex(10),
        last_log_term: Term(2),
    }
}

// ---------------------------------------------------------------------------
// Scenario: peer-client-reconnect
// ---------------------------------------------------------------------------

#[tokio::test]
async fn peer_client_recovers_after_peer_restart() {
    // 1. Bind a port, spawn a server, wait until it's listening.
    let port = pick_free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let handler_v1 = StubHandler::new();
    let (shutdown_v1, srv_v1) = spawn_plain_server(addr, handler_v1.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    // 2. Build a real PeerClient — same transport client the
    //    production ConnectionPool / GrpcTransport uses.
    let transport = client_for(addr);
    let peer = PeerClient::new(NodeId(SERVER_NODE_ID), transport.clone());

    // 3. First RPC succeeds and primes the leader-hint cache.
    let resp = peer
        .vote(sample_vote_request())
        .await
        .expect("first vote rpc succeeds");
    assert!(resp.vote_granted);
    assert_eq!(handler_v1.vote_calls.load(Ordering::SeqCst), 1);
    // Critical: the cache populated from a REAL response (not the
    // `#[cfg(test)]` helper). Proves the wire path through
    // `PeerClient::vote` updates the hint cache.
    assert_eq!(
        peer.leader_hint(),
        Some(NodeId(SERVER_NODE_ID)),
        "leader-hint cache must populate from a real vote response"
    );

    // 4. Kill the server. Wait for the OS to fully release the port
    //    so the v2 spawn doesn't race on bind.
    shutdown_v1.notify_one();
    srv_v1
        .await
        .expect("v1 serve task did not panic")
        .expect("v1 serve task did not error");
    wait_for_port_release(addr, Duration::from_secs(2)).await;

    // 5. Restart on the same port with a fresh handler.
    let handler_v2 = StubHandler::new();
    let (shutdown_v2, srv_v2) = spawn_plain_server(addr, handler_v2.clone());
    wait_for_listening(addr, Duration::from_secs(2)).await;

    // 6. Issue the second RPC. The transport's per-peer retry loop
    //    (xraft-transport, exponential backoff with jitter) handles
    //    the reconnect transparently — `PeerClient::vote` returns
    //    Ok without the caller having to invalidate anything.
    let resp = peer
        .vote(sample_vote_request())
        .await
        .expect("second vote rpc succeeds after restart");
    assert!(resp.vote_granted);
    // The v2 server handled exactly one vote — proves the RPC went
    // to the restarted instance (not a cached connection to the
    // dead v1 server).
    assert_eq!(handler_v2.vote_calls.load(Ordering::SeqCst), 1);
    // And the leader-hint cache is still populated (the v2 response
    // carries the same hint; the epoch-fenced cache accepts a hint
    // at the same epoch).
    assert_eq!(peer.leader_hint(), Some(NodeId(SERVER_NODE_ID)));

    // 7. Clean up.
    shutdown_v2.notify_one();
    srv_v2
        .await
        .expect("v2 serve task did not panic")
        .expect("v2 serve task did not error");
}
