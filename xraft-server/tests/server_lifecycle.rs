//! Stage 6.1 integration tests for the [`Server`] lifecycle.
//!
//! Each test exercises one scenario from the workstream brief:
//!
//! - `server-startup` — a valid config starts the server in <1s.
//! - `graceful-shutdown` — `shutdown()` + `join()` completes
//!   cleanly within a few seconds.
//! - `health-endpoint` — `GET /health` returns JSON with the
//!   expected fields.
//!
//! Tests scope shared state to a per-test [`TempDir`] so they can
//! run concurrently and leave no on-disk artifacts behind.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::types::NodeId;
use xraft_server::{Server, ServerConfig};

/// Bind 127.0.0.1:0 to obtain an unused port, then drop the
/// listener. The window between drop and a re-bind is small;
/// acceptable for local integration runs.
fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn single_voter_cluster_config(data_dir: PathBuf) -> ClusterConfig {
    let grpc_port = pick_port();
    ClusterConfig {
        node_id: NodeId(1),
        cluster_id: "stage-6-1-test".into(),
        listen_addr: format!("127.0.0.1:{grpc_port}"),
        peers: vec![],
        voters: vec![VoterConfig {
            node_id: 1,
            directory_id: uuid::Uuid::new_v4().to_string(),
            host: "127.0.0.1".into(),
            port: grpc_port,
        }],
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
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
        rpc_timeout_ms: 10_000,
        max_rpc_retries: 3,
        retry_initial_backoff_ms: 100,
        retry_max_backoff_ms: 5_000,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
    }
}

fn build_server_config(tmp: &TempDir) -> ServerConfig {
    ServerConfig {
        cluster: single_voter_cluster_config(tmp.path().to_path_buf()),
        admin_listen_addr: Some("127.0.0.1:0".into()),
        driver_config: None,
    }
}

/// Workstream brief: "Given a valid config file, When the server
/// binary is started, Then it initializes all components and
/// begins as a Follower within 1 second."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_startup_completes_within_one_second() {
    let tmp = TempDir::new().unwrap();
    // Use a long election timeout so the single-voter cluster does
    // NOT immediately auto-promote itself to Leader during the 1s
    // observation window — the brief specifically requires the
    // node to "begin as a Follower". The default 150ms timeout in
    // `single_voter_cluster_config` would let the node transition
    // to Leader before the test reads `/health`.
    let mut cluster = single_voter_cluster_config(tmp.path().to_path_buf());
    cluster.election_timeout_min_ms = 30_000;
    cluster.election_timeout_max_ms = 60_000;
    let cfg = ServerConfig {
        cluster,
        admin_listen_addr: Some("127.0.0.1:0".into()),
        driver_config: None,
    };

    let start = Instant::now();
    let handle = Server::start(cfg).await.expect("server must start");
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "server-startup scenario: must initialise within 1s, took {elapsed:?}"
    );
    assert!(handle.admin_addr.port() > 0);
    assert!(!handle.grpc_listen_addr.is_empty());

    // The brief explicitly requires the node to "begin as a
    // Follower". Poll /health until the publisher has emitted its
    // first NodeStatus snapshot, but stay strictly inside the 1s
    // budget. The long election timeout above guarantees the node
    // cannot have transitioned out of Follower during this window.
    let deadline = start + Duration::from_secs(1);
    let mut last_body = String::new();
    let mut observed_role: Option<String> = None;
    while Instant::now() < deadline {
        last_body = http_get(&handle.admin_addr.to_string(), "/health").await;
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&last_body)
            && let Some(role) = json.get("role").and_then(|v| v.as_str())
        {
            observed_role = Some(role.to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let role = observed_role.unwrap_or_else(|| {
        panic!("server-startup: /health never published a 'role' field within 1s; last body = {last_body}")
    });
    assert_eq!(
        role, "follower",
        "server-startup: node must begin as Follower (per Stage 6.1 brief), observed role = {role:?}"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join())
        .await
        .expect("teardown must complete within deadline");
}

/// Workstream brief: "Given a running server, When SIGTERM is
/// received, Then state is persisted, connections are drained,
/// and the process exits with code 0."
///
/// We model `SIGTERM` with a direct call to
/// `ServerHandle::shutdown()` since the signal-loop translation
/// only forwards to that. The "code 0" requirement maps to
/// `ServerHandle::join` returning `Ok(())`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_completes_cleanly() {
    let tmp = TempDir::new().unwrap();
    let cfg = build_server_config(&tmp);
    let handle = Server::start(cfg).await.expect("server must start");

    // Give the engine a couple of ticks to publish at least one
    // NodeStatus snapshot through the observer pipeline.
    tokio::time::sleep(Duration::from_millis(100)).await;

    handle.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(5), handle.join())
        .await
        .expect("graceful shutdown must complete within 5s");
    res.expect("join must return Ok(())");
}

/// Workstream brief: "Given a running server, When GET /health
/// is called, Then it returns JSON with node_id, role, term, and
/// leader_id fields."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_endpoint_returns_expected_json_fields() {
    let tmp = TempDir::new().unwrap();
    let cfg = build_server_config(&tmp);
    let handle = Server::start(cfg).await.expect("server must start");

    // Allow the engine to publish its first NodeStatus through
    // the observer pipeline. Tests on a slow CI box may take a
    // few ticks (10ms each) to settle.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let url = format!("http://{}/health", handle.admin_addr);
    // Use the lightweight blocking-style fetch via reqwest only
    // if available; otherwise drive a raw TcpStream + HTTP/1.1
    // GET. Avoid pulling in reqwest as a heavy dep for one test.
    let body = http_get(&handle.admin_addr.to_string(), "/health").await;
    let json: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("invalid JSON: {body} ({e})"));

    // Required fields per the scenario.
    assert_eq!(json["node_id"], 1, "node_id field missing: {url} -> {json}");
    assert!(json["role"].is_string(), "role field missing: {json}");
    assert!(json["term"].is_number(), "term field missing: {json}");
    // leader_id may be null on first publish but the field MUST
    // be present.
    assert!(
        json.get("leader_id").is_some(),
        "leader_id field missing: {json}"
    );
    // Bonus fields the publisher always emits.
    assert!(
        json["commit_index"].is_number(),
        "commit_index field missing: {json}"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join())
        .await
        .expect("teardown must complete within deadline");
}

/// Verify `/metrics` exposes the MVP Prometheus metric set
/// required by Stage 6.1 (`xraft_current_term`,
/// `xraft_commit_index`, `xraft_current_leader`, `xraft_role`,
/// `xraft_election_latency_seconds`, `xraft_append_records_total`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_exposes_mvp_metric_set() {
    let tmp = TempDir::new().unwrap();
    let cfg = build_server_config(&tmp);
    let handle = Server::start(cfg).await.expect("server must start");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let body = http_get(&handle.admin_addr.to_string(), "/metrics").await;

    for metric in [
        "xraft_current_term",
        "xraft_commit_index",
        "xraft_current_leader",
        "xraft_role",
        "xraft_election_latency_seconds",
        "xraft_append_records_total",
    ] {
        assert!(
            body.contains(metric),
            "metrics output must include '{metric}', body = {body}"
        );
    }

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

// ---------------------------------------------------------------------------
// Minimal HTTP/1.1 GET helper (avoids adding `reqwest` as a heavy
// dev-dep for two integration tests).
// ---------------------------------------------------------------------------

async fn http_get(host_port: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(host_port)
        .await
        .unwrap_or_else(|e| panic!("connect to {host_port} failed: {e}"));
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .expect("send request");

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    let raw = String::from_utf8_lossy(&buf).into_owned();
    // Strip the HTTP status line + headers, return only the
    // body after the CRLF CRLF separator.
    match raw.split_once("\r\n\r\n") {
        Some((_, body)) => body.to_string(),
        None => raw,
    }
}
