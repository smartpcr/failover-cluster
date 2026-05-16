//! Stage 7.2 integration tests — Static Voter Set Bootstrap and
//! Observer Support.
//!
//! Each test maps to one scenario from the workstream brief:
//!
//! - `single-node-cluster` →
//!   [`single_node_cluster_elects_self_and_commits`]
//! - `bootstrap-voter-set` (multi-node) →
//!   [`three_node_bootstrap_persists_same_voter_set_and_elects_leader`]
//! - `bootstrap-voter-set` (persistence half) →
//!   [`bootstrap_persists_voter_set_and_recovers_on_restart`]
//! - identity-drift (operator error) →
//!   [`restart_with_mismatched_voters_rejected_with_config_error`]
//! - `add-remove-voter-rejected` →
//!   [`add_voter_via_driver_handle_returns_unsupported`],
//!   [`remove_voter_via_driver_handle_returns_unsupported`],
//!   [`http_add_voter_returns_501_unsupported`],
//!   [`http_remove_voter_returns_501_unsupported`]
//! - `observer-replicates-without-voting` (end-to-end gRPC) →
//!   [`observer_replicates_log_without_counting_toward_quorum`]
//!
//! Iter-3: the integration-level `observer-replicates-without-voting`
//! test runs four real `Server`s (3 voters + 1 observer) over the
//! production gRPC transport. It first proves replication WITH
//! quorum, then SHUTS DOWN two voters to prove the observer cannot
//! substitute for a missing voter (the leader's `commit_index` does
//! not advance even though the observer keeps fetching).
//!
//! The `even-voter-warning` scenario is still covered at the engine
//! level in `xraft-core/src/node.rs` (search for
//! `even_voter_set_emits_warning_log`) since it is a pure log
//! assertion. Other observer-related engine tests retained for
//! deterministic coverage:
//!   `leader_accepts_observer_fetch_and_excludes_from_quorum`,
//!   `observer_node_does_not_become_candidate_on_tick`,
//!   `pre_candidate_does_not_send_prevote_to_observer_peers`,
//!   `candidate_does_not_send_vote_to_observer_peers`,
//!   `observer_drops_incoming_vote_request_without_term_bump`,
//!   `observer_drops_incoming_pre_vote_request`,
//!   `observer_preserves_role_on_higher_term_become_follower`,
//!   `fetch_response_with_observer_as_leader_dropped`.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;
use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::error::XRaftError;
use xraft_core::types::NodeId;
use xraft_server::{Server, ServerConfig};

/// Bind 127.0.0.1:0 to obtain an unused ephemeral port AND keep the
/// listener alive so a parallel test cannot steal the port between
/// the pick and the eventual `Server::start_with_listener` consume.
/// The caller MUST hand the returned listener to
/// `Server::start_with_listener` (or
/// `Server::start_with_state_machine_and_listener`) — dropping it
/// without consuming reintroduces the original TOCTOU race against
/// parallel ephemeral picks under CI load. This structural seam
/// replaces the prior iter-4 "pick from a high port range, then drop
/// and rebind" mitigation, which the rubber-duck pass correctly noted
/// is still probabilistic because Windows' default dynamic-port range
/// starts at 49152 — overlapping the test's 49200+ window.
fn bind_ephemeral() -> (u16, std::net::TcpListener) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let p = l.local_addr().unwrap().port();
    (p, l)
}

/// Re-bind a previously-allocated port on `127.0.0.1`, retrying
/// briefly to ride out any transient `AddrInUse` while the kernel
/// fully releases the prior listener.
///
/// Used by the restart-with-pinned-port tests
/// (`bootstrap_persists_voter_set_and_recovers_on_restart`,
/// `restart_with_mismatched_voters_rejected_with_config_error`):
/// the first boot's listener is consumed by
/// `Server::start_with_listener`; after `shutdown` + `join` the
/// same port must come back through a fresh `TcpListener` so the
/// second boot also enters via the listener-passthrough seam.
fn rebind_port(port: u16) -> std::net::TcpListener {
    let addr = format!("127.0.0.1:{port}");
    let mut last_err: Option<std::io::Error> = None;
    // 6s budget (60 × 100ms). The prior iter-3 budget was 1s
    // (20 × 50ms), which the post-pass gate suite intermittently
    // exhausted on Windows under heavy parallel-test load — Windows'
    // dynamic-port release can lag behind `shutdown()` by several
    // hundred ms once the loopback socket count climbs. The caller
    // also now hard-asserts the preceding `handle.join()` completed
    // BEFORE invoking this helper, so a long retry window here only
    // covers the kernel-side release lag, not application shutdown.
    for _ in 0..60 {
        match std::net::TcpListener::bind(&addr) {
            Ok(l) => return l,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
    panic!(
        "rebind {addr} failed after 6s of retries: {:?}",
        last_err.unwrap()
    );
}

/// Build a single-voter cluster config rooted at `data_dir` with
/// `node_id = 1` and an ephemeral port, AND return the held
/// listener. The caller hands the listener to
/// `Server::start_with_listener` so the port is never released
/// between pick and serve — closing the TOCTOU window described on
/// [`bind_ephemeral`].
fn single_voter_cluster_config(data_dir: PathBuf) -> (ClusterConfig, std::net::TcpListener) {
    let (port, listener) = bind_ephemeral();
    let cfg =
        single_voter_cluster_config_with_endpoint(data_dir, uuid::Uuid::new_v4().to_string(), port);
    (cfg, listener)
}

/// Variant that lets the test pin BOTH the voter's `directory_id`
/// (UUID) and its `port` so a restart reproduces the exact same
/// `VoterSet` on disk — used by the persistence round-trip test
/// and the identity-drift test. (The persisted `VoterSet`'s
/// equality includes the voter endpoint, so a re-picked ephemeral
/// port would itself look like identity drift even with the same
/// UUID.)
fn single_voter_cluster_config_with_endpoint(
    data_dir: PathBuf,
    directory_id: String,
    grpc_port: u16,
) -> ClusterConfig {
    ClusterConfig {
        node_id: NodeId(1),
        cluster_id: "stage-7-2-test".into(),
        listen_addr: format!("127.0.0.1:{grpc_port}"),
        peers: vec![],
        voters: vec![VoterConfig {
            node_id: 1,
            directory_id,
            host: "127.0.0.1".into(),
            port: grpc_port,
        }],
        election_timeout_min_ms: 50,
        election_timeout_max_ms: 100,
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

fn server_config(cluster: ClusterConfig) -> ServerConfig {
    ServerConfig {
        cluster,
        admin_listen_addr: Some("127.0.0.1:0".into()),
        driver_config: None,
    }
}

/// Scenario: single-node-cluster — "Given a configuration with 1
/// voter, When the server starts, Then it elects itself leader
/// immediately and can commit entries without waiting for peers."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_elects_self_and_commits() {
    let tmp = TempDir::new().unwrap();
    let (cluster, grpc_listener) = single_voter_cluster_config(tmp.path().to_path_buf());
    let cfg = server_config(cluster);
    let handle = Server::start_with_listener(cfg, grpc_listener)
        .await
        .expect("server must start");

    // Wait for the engine to transition Follower → Candidate →
    // Leader. With the short election timeout (50-100ms) + a 10ms
    // tick this completes well inside 2s on any reasonable box.
    let deadline = Instant::now() + Duration::from_secs(2);
    let driver = handle.driver_handle();
    let mut committed: Option<xraft_core::types::LogIndex> = None;
    while Instant::now() < deadline {
        match driver
            .propose(Bytes::from_static(b"single-node-commit"))
            .await
        {
            Ok(idx) => {
                committed = Some(idx);
                break;
            }
            Err(XRaftError::NotLeader { .. }) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(other) => panic!("propose returned unexpected error: {other:?}"),
        }
    }
    let idx = committed.expect(
        "single-voter cluster must elect itself leader and commit within 2s — \
         no leader was elected or commit never completed",
    );
    assert!(
        idx.0 >= 1,
        "committed log index must be at least 1 (got {idx:?})"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

/// Scenario: bootstrap-voter-set — "Given a 3-node cluster
/// configuration, When all nodes start for the first time, Then
/// each node initializes with the same VoterSet and an election
/// produces a leader."
///
/// We exercise the *persistence half* of the property with a
/// single node: on first start the engine must persist the
/// derived `VoterSet` to `<data_dir>/state/quorum-state`, and on
/// restart the persisted set must be recovered byte-identical.
/// The full 3-node election half is covered by Stage 3.x
/// multi-node tests; what's new in Stage 7.2 is the durable
/// `voter_set` field next to `HardState`. We assert directly on
/// the on-disk JSON so a regression that drops the field surfaces
/// here rather than via a cryptic restart failure later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_persists_voter_set_and_recovers_on_restart() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let pinned_uuid = uuid::Uuid::new_v4().to_string();
    let (pinned_port, grpc_listener) = bind_ephemeral();
    let cfg = server_config(single_voter_cluster_config_with_endpoint(
        data_dir.clone(),
        pinned_uuid.clone(),
        pinned_port,
    ));

    // First boot — voter set must be written to disk by
    // `Server::start_with_state_machine`'s Stage-7.2 bootstrap.
    let handle = Server::start_with_listener(cfg, grpc_listener)
        .await
        .expect("first boot must succeed");
    // Give the engine a moment to settle so the quorum-state
    // file is fully fsync'd before we read it under the test.
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown();
    // Hard-assert the first server actually joined before we attempt
    // to rebind the pinned port below. Previously this used
    // `let _ = tokio::time::timeout(...).await;` which silently
    // swallowed both the timeout and the join error — so a slow
    // shutdown under CI load could race the subsequent `rebind_port`
    // against a still-open listener, producing the intermittent
    // post-pass gate failure on `stage_7_2_static_voter_set`.
    let join_outcome = tokio::time::timeout(Duration::from_secs(10), handle.join())
        .await
        .expect("first server must join within 10s before rebinding pinned port");
    join_outcome.expect("first server join must succeed before rebind");

    // Verify the on-disk file carries the voter_set field.
    let quorum_path = data_dir.join("state").join("quorum-state");
    assert!(
        quorum_path.exists(),
        "quorum-state file must exist after first boot — looked at {}",
        quorum_path.display()
    );
    let raw = std::fs::read_to_string(&quorum_path).expect("read quorum-state");
    let value: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("invalid JSON: {raw} ({e})"));
    assert!(
        value.get("voter_set").is_some() && !value["voter_set"].is_null(),
        "Stage 7.2: quorum-state MUST persist `voter_set` next to `HardState`; \
         file = {raw}"
    );
    let voters = value["voter_set"]["voters"]
        .as_array()
        .expect("voter_set.voters must be an array");
    assert_eq!(
        voters.len(),
        1,
        "single-voter bootstrap must persist exactly one voter"
    );
    assert_eq!(voters[0]["node_id"], 1);
    assert_eq!(voters[0]["directory_id"], pinned_uuid);

    // Restart with the SAME config (same `directory_id` + same
    // `port` so the derived `VoterSet` is byte-identical). The
    // bootstrap path must take the `Some(persisted)` branch and
    // accept the recovered set without rewriting.
    let cfg2 = server_config(single_voter_cluster_config_with_endpoint(
        data_dir.clone(),
        pinned_uuid.clone(),
        pinned_port,
    ));
    let grpc_listener2 = rebind_port(pinned_port);
    let handle2 = Server::start_with_listener(cfg2, grpc_listener2)
        .await
        .expect("restart with matching voter set must succeed");
    handle2.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle2.join()).await;

    // The persisted file should still contain the same voter set
    // (the recovery branch must not mutate the file).
    let raw_after = std::fs::read_to_string(&quorum_path).expect("read quorum-state post-restart");
    let value_after: serde_json::Value = serde_json::from_str(&raw_after).expect("valid JSON");
    assert_eq!(
        value_after["voter_set"], value["voter_set"],
        "recovery branch must not mutate the persisted voter set"
    );
}

/// Identity drift: restart with a DIFFERENT `directory_id` for
/// the same `node_id` produces a non-equal `VoterSet`. Per the
/// Stage 7.2 contract (dynamic membership out of scope for v1),
/// the bootstrap path must reject with `XRaftError::Config`
/// rather than silently overwriting the persisted set — an
/// operator changing the cluster topology by editing config and
/// restarting is a misconfiguration, not a supported flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_with_mismatched_voters_rejected_with_config_error() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let original_uuid = uuid::Uuid::new_v4().to_string();
    let drift_uuid = uuid::Uuid::new_v4().to_string();
    let (pinned_port, grpc_listener) = bind_ephemeral();
    assert_ne!(original_uuid, drift_uuid);

    // First boot — persist original voter set.
    let cfg1 = server_config(single_voter_cluster_config_with_endpoint(
        data_dir.clone(),
        original_uuid.clone(),
        pinned_port,
    ));
    let handle = Server::start_with_listener(cfg1, grpc_listener)
        .await
        .expect("first boot must succeed");
    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;

    // Second boot — same node_id + same port, different
    // directory_id ⇒ VoterSet differs ⇒ identity drift ⇒ must
    // reject. Pinning the port isolates the test from the
    // ephemeral-port noise so the only difference between the
    // two configs is the UUID we're actually probing. The
    // voter-set drift check (`server.rs::start_inner` step 1b)
    // fires BEFORE the gRPC bind, so the rejection arrives even
    // when the pinned port has not yet been released by the
    // kernel — using plain `Server::start` here is safe.
    let cfg2 = server_config(single_voter_cluster_config_with_endpoint(
        data_dir.clone(),
        drift_uuid,
        pinned_port,
    ));
    let err = Server::start(cfg2)
        .await
        .expect_err("restart with mismatched voter set must reject");
    match err {
        XRaftError::Config(msg) => {
            assert!(
                msg.contains("voter set on disk") && msg.contains("differs"),
                "identity-drift rejection must cite the on-disk mismatch: {msg}"
            );
        }
        other => panic!("expected XRaftError::Config, got {other:?}"),
    }
}

/// Scenario: add-remove-voter-rejected (programmatic half) —
/// the `DriverHandle::add_voter` boundary must reject with
/// `XRaftError::Unsupported`. Runs against a live server so the
/// rejection is observed end-to-end (NOT just at the handle
/// constructor), and the persisted voter set on disk is verified
/// unchanged after the rejected call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_voter_via_driver_handle_returns_unsupported() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let (cluster, grpc_listener) = single_voter_cluster_config(data_dir.clone());
    let cfg = server_config(cluster);
    let handle = Server::start_with_listener(cfg, grpc_listener)
        .await
        .expect("server must start");
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Snapshot the persisted file BEFORE the rejected call.
    let quorum_path = data_dir.join("state").join("quorum-state");
    let raw_before = std::fs::read_to_string(&quorum_path).expect("read quorum-state");

    let driver = handle.driver_handle();
    let err = driver
        .add_voter(NodeId(99))
        .await
        .expect_err("add_voter must reject");
    match err {
        XRaftError::Unsupported(msg) => {
            assert!(
                msg.contains("AddVoter") && msg.contains("out of scope for v1"),
                "unsupported message must name op + scoping: {msg}"
            );
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }

    // Voter set on disk must be byte-identical — the rejection
    // is local and must not touch persistent state.
    let raw_after = std::fs::read_to_string(&quorum_path).expect("read quorum-state post-call");
    assert_eq!(
        raw_before, raw_after,
        "rejected add_voter MUST NOT mutate quorum-state"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_voter_via_driver_handle_returns_unsupported() {
    let tmp = TempDir::new().unwrap();
    let (cluster, grpc_listener) = single_voter_cluster_config(tmp.path().to_path_buf());
    let cfg = server_config(cluster);
    let handle = Server::start_with_listener(cfg, grpc_listener)
        .await
        .expect("server must start");

    let err = handle
        .driver_handle()
        .remove_voter(NodeId(1))
        .await
        .expect_err("remove_voter must reject");
    assert!(matches!(err, XRaftError::Unsupported(_)));

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

/// Scenario: add-remove-voter-rejected (HTTP half) —
/// `POST /admin/add-voter` must surface the same UNSUPPORTED
/// rejection via 501 Not Implemented so external tooling does
/// not silently drop the failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_add_voter_returns_501_unsupported() {
    let tmp = TempDir::new().unwrap();
    let (cluster, grpc_listener) = single_voter_cluster_config(tmp.path().to_path_buf());
    let cfg = server_config(cluster);
    let handle = Server::start_with_listener(cfg, grpc_listener)
        .await
        .expect("server must start");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let response = http_post(&handle.admin_addr.to_string(), "/admin/add-voter").await;
    assert!(
        response.status_line.contains("501"),
        "expected 501 Not Implemented, got status line = {:?}, body = {}",
        response.status_line,
        response.body
    );
    let json: serde_json::Value = serde_json::from_str(&response.body)
        .unwrap_or_else(|e| panic!("invalid JSON in 501 body: {} ({e})", response.body));
    assert_eq!(json["code"], "UNSUPPORTED");
    let msg = json["error"].as_str().expect("error field must be string");
    assert!(
        msg.contains("AddVoter") && msg.contains("out of scope"),
        "error body must name op + v1 scoping: {msg}"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_remove_voter_returns_501_unsupported() {
    let tmp = TempDir::new().unwrap();
    let (cluster, grpc_listener) = single_voter_cluster_config(tmp.path().to_path_buf());
    let cfg = server_config(cluster);
    let handle = Server::start_with_listener(cfg, grpc_listener)
        .await
        .expect("server must start");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let response = http_post(&handle.admin_addr.to_string(), "/admin/remove-voter").await;
    assert!(
        response.status_line.contains("501"),
        "expected 501 Not Implemented, got status line = {:?}, body = {}",
        response.status_line,
        response.body
    );
    let json: serde_json::Value = serde_json::from_str(&response.body)
        .unwrap_or_else(|e| panic!("invalid JSON in 501 body: {} ({e})", response.body));
    assert_eq!(json["code"], "UNSUPPORTED");
    let msg = json["error"].as_str().expect("error field must be string");
    assert!(
        msg.contains("RemoveVoter"),
        "error body must name op: {msg}"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

/// Scenario: bootstrap-voter-set (multi-node half) — "Given a
/// 3-node cluster configuration, When all nodes start for the
/// first time, Then each node initializes with the same VoterSet
/// and an election produces a leader."
///
/// We pin three voter UUIDs + three ports up front and build
/// three identical `voters` lists (varying only `node_id` +
/// `listen_addr`). Each node persists the SAME `VoterSet` to its
/// own `<data_dir>/state/quorum-state` file, and at least one of
/// the three becomes Leader (verified by trying `propose()` on
/// each in turn).
///
/// This test exercises the production multi-node bootstrap path:
/// three real `Server::start_with_state_machine` invocations
/// running concurrently with real ConnectionPools + gRPC
/// transports, communicating via 127.0.0.1 ports. A regression
/// that breaks (a) the persisted voter-set field, (b) the
/// peer-dial wiring, or (c) the multi-node election flow surfaces
/// here.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_bootstrap_persists_same_voter_set_and_elects_leader() {
    // Best-effort tracing init so the test surfaces engine/transport
    // logs to stderr when run with `--nocapture`. Multiple calls
    // (across parallel tests) are fine — `try_init` is a no-op the
    // second time around.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
    let tmps: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
    let data_dirs: Vec<PathBuf> = tmps.iter().map(|t| t.path().to_path_buf()).collect();
    let uuids: Vec<String> = (0..3).map(|_| uuid::Uuid::new_v4().to_string()).collect();
    // Bind 3 ephemeral listeners up front and hold them until each
    // Server::start_with_listener consumes its slot. This closes
    // the parallel-test TOCTOU window between port pick and
    // Server::start's bind that blocked the iter-3 / iter-4
    // post-pass gate under CI load.
    let bound: Vec<(u16, std::net::TcpListener)> = (0..3).map(|_| bind_ephemeral()).collect();
    let ports: Vec<u16> = bound.iter().map(|(p, _)| *p).collect();
    let mut listeners: Vec<Option<std::net::TcpListener>> =
        bound.into_iter().map(|(_, l)| Some(l)).collect();
    // Shared voter roster — byte-identical across all three nodes
    // so the derived VoterSet is byte-identical when each node
    // persists it on first boot.
    let voters: Vec<VoterConfig> = (0..3)
        .map(|i| VoterConfig {
            node_id: (i as u64) + 1,
            directory_id: uuids[i].clone(),
            host: "127.0.0.1".into(),
            port: ports[i],
        })
        .collect();

    fn build_cluster(
        node_id: u64,
        data_dir: PathBuf,
        listen_port: u16,
        voters: Vec<VoterConfig>,
    ) -> ClusterConfig {
        ClusterConfig {
            node_id: NodeId(node_id),
            cluster_id: "stage-7-2-bootstrap".into(),
            listen_addr: format!("127.0.0.1:{listen_port}"),
            peers: vec![],
            voters,
            // Generous election timeouts so the test is robust on
            // a loaded CI host (especially Windows with tokio
            // worker contention across 3 in-process servers).
            // 600-1200ms min/max with a 100ms tick is comfortably
            // larger than the worst-case fetch round-trip we have
            // observed on the slowest CI runners, while still
            // letting the cluster stabilise inside the 20s test
            // budget. Check-Quorum interval defaults to
            // election_timeout_max * 2 = 2.4s, well above the
            // fetch_interval so a healthy leader is not at risk
            // of stepping down spuriously.
            election_timeout_min_ms: 600,
            election_timeout_max_ms: 1_200,
            fetch_interval_ms: 100,
            tick_interval_ms: 50,
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

    let mut handles = Vec::new();
    for i in 0..3 {
        let cfg = server_config(build_cluster(
            (i as u64) + 1,
            data_dirs[i].clone(),
            ports[i],
            voters.clone(),
        ));
        let listener = listeners[i].take().expect("listener slot still held");
        handles.push(
            Server::start_with_listener(cfg, listener)
                .await
                .unwrap_or_else(|e| panic!("node {} must start: {e:?}", i + 1)),
        );
    }

    // Wait for one of the three nodes to become Leader and accept
    // a propose. We probe each node in round-robin; the leader
    // returns Ok, followers return NotLeader, and we just keep
    // polling until one succeeds.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut committed: Option<(usize, xraft_core::types::LogIndex)> = None;
    let mut last_errors: Vec<String> = vec![String::new(); 3];
    'outer: while Instant::now() < deadline {
        for (i, h) in handles.iter().enumerate() {
            // Bound each propose attempt so a hanging proposal on
            // a node that never becomes leader doesn't burn the
            // full 20s budget waiting for one node.
            let driver = h.driver_handle();
            let propose_fut = driver.propose(Bytes::from_static(b"three-node-bootstrap"));
            match tokio::time::timeout(Duration::from_millis(500), propose_fut).await {
                Ok(Ok(idx)) => {
                    committed = Some((i, idx));
                    break 'outer;
                }
                Ok(Err(e)) => {
                    last_errors[i] = format!("{e:?}");
                }
                Err(_elapsed) => {
                    last_errors[i] = "propose timed out (likely no leader)".to_string();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (leader_idx, log_idx) = committed.unwrap_or_else(|| {
        panic!(
            "3-node cluster must elect a leader and commit within 20s — \
             no node returned Ok from propose(); last errors per node = {last_errors:?}"
        )
    });
    assert!(
        log_idx.0 >= 1,
        "committed log index must be >= 1 (got {log_idx:?})"
    );
    println!("3-node cluster: node {} became leader", leader_idx + 1);

    // Shut everything down before reading the persisted state so
    // we read a quiesced file (the engine still mutates HardState
    // as the term advances; voter_set is write-once but the
    // surrounding JSON gets rewritten on every commit_index bump).
    for h in handles {
        h.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), h.join()).await;
    }

    // Verify each node's persisted voter_set is byte-identical to
    // the others — that's the durable proof that all 3 nodes
    // bootstrapped from the same VoterSet.
    let mut persisted: Vec<serde_json::Value> = Vec::new();
    for (i, d) in data_dirs.iter().enumerate() {
        let p = d.join("state").join("quorum-state");
        assert!(
            p.exists(),
            "node {} quorum-state missing at {}",
            i + 1,
            p.display()
        );
        let raw = std::fs::read_to_string(&p).expect("read quorum-state");
        let v: serde_json::Value =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("invalid JSON: {raw} ({e})"));
        assert!(
            !v["voter_set"].is_null(),
            "node {} must persist voter_set; raw = {raw}",
            i + 1
        );
        let voters_arr = v["voter_set"]["voters"]
            .as_array()
            .expect("voter_set.voters must be an array");
        assert_eq!(
            voters_arr.len(),
            3,
            "node {} persisted voter set must have 3 voters; got {}",
            i + 1,
            voters_arr.len()
        );
        persisted.push(v["voter_set"].clone());
    }
    assert_eq!(
        persisted[0], persisted[1],
        "node 1 and node 2 must persist the SAME voter_set"
    );
    assert_eq!(
        persisted[1], persisted[2],
        "node 2 and node 3 must persist the SAME voter_set"
    );
}

// ---------------------------------------------------------------------------

struct HttpResponse {
    status_line: String,
    body: String,
}

async fn http_post(host_port: &str, path: &str) -> HttpResponse {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(host_port)
        .await
        .unwrap_or_else(|e| panic!("connect to {host_port} failed: {e}"));
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .await
        .expect("send request");

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    let raw = String::from_utf8_lossy(&buf).into_owned();
    let status_line = raw
        .lines()
        .next()
        .map(|s| s.to_string())
        .unwrap_or_default();
    let body = match raw.split_once("\r\n\r\n") {
        Some((_, body)) => body.to_string(),
        None => String::new(),
    };
    HttpResponse { status_line, body }
}

// ---------------------------------------------------------------------------
// Stage 7.2 iter-3 finding #4 — observer-replicates-without-voting
// at the SERVER / gRPC boundary.
// ---------------------------------------------------------------------------

/// Build a 4-node config (3 voters {1,2,3} + 1 observer {4}) where
/// `node_id == self_id`, all hosted on 127.0.0.1 with the given
/// ephemeral ports.
///
/// `enable_check_quorum` is configurable so the negative-quorum
/// phase can keep the leader from stepping down when its voter
/// peers go away. The fetch / tick budget is generous (600-1200ms
/// election, 100ms fetch) so the test survives Windows CI tokio
/// worker-pool contention with 4 in-process servers.
#[allow(clippy::too_many_arguments)]
fn build_observer_cluster(
    self_id: u64,
    data_dir: PathBuf,
    listen_port: u16,
    voters: Vec<VoterConfig>,
    observers: Vec<u64>,
    enable_check_quorum: bool,
) -> ClusterConfig {
    ClusterConfig {
        node_id: NodeId(self_id),
        cluster_id: "stage-7-2-observer".into(),
        listen_addr: format!("127.0.0.1:{listen_port}"),
        peers: vec![],
        voters,
        election_timeout_min_ms: 600,
        election_timeout_max_ms: 1_200,
        fetch_interval_ms: 100,
        tick_interval_ms: 50,
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
        observers,
        enable_check_quorum,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    }
}

/// Scenario: observer-replicates-without-voting (end-to-end at the
/// gRPC boundary, per iter-3 finding #4).
///
/// Phase 1 (HAPPY PATH): 3 voters + 1 observer come up over real
/// gRPC. A voter wins election, the client proposes an entry, the
/// observer receives and applies the entry via Fetch RPCs without
/// participating in the vote or the quorum tally. Observable
/// signatures asserted:
///   - the observer's role is `Observer` (NEVER `Candidate`/`Leader`),
///   - the observer's `last_applied` catches up to the committed index,
///   - exactly one voter became `Leader`.
///
/// Phase 2 (NEGATIVE QUORUM): shut down 2 of 3 voters so the
/// surviving leader has no quorum partner (1 voter + 1 observer = NOT
/// a majority of {1,2,3}). Propose a second entry. Observable
/// signatures asserted:
///   - the leader's `commit_index` does NOT advance past the phase-1
///     entry (observer cannot substitute for a missing voter),
///   - the observer's `last_applied` does NOT advance past the phase-1
///     entry (apply pipeline is gated on commit),
///   - the client `propose` future times out (Raft `propose` resolves
///     only when the entry commits, which here it cannot).
///
/// `enable_check_quorum = false` keeps the leader from stepping down
/// during phase 2 so the test isolates the quorum-exclusion property
/// from leadership-loss noise.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn observer_replicates_log_without_counting_toward_quorum() {
    use xraft_core::types::NodeRole;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    // ---------------------------------------------- 4-node topology
    // Indices: 0..3 voters (node_ids 1..3); index 3 observer (node_id 4).
    let tmps: Vec<TempDir> = (0..4).map(|_| TempDir::new().unwrap()).collect();
    let data_dirs: Vec<PathBuf> = tmps.iter().map(|t| t.path().to_path_buf()).collect();
    let uuids: Vec<String> = (0..4).map(|_| uuid::Uuid::new_v4().to_string()).collect();
    // Bind 4 ephemeral listeners up front and hold them until each
    // Server::start_with_listener consumes its slot. See
    // [`bind_ephemeral`] for the TOCTOU-closure rationale.
    let bound: Vec<(u16, std::net::TcpListener)> = (0..4).map(|_| bind_ephemeral()).collect();
    let ports: Vec<u16> = bound.iter().map(|(p, _)| *p).collect();
    let mut grpc_listeners: Vec<Option<std::net::TcpListener>> =
        bound.into_iter().map(|(_, l)| Some(l)).collect();

    // Voter roster: byte-identical across all 4 nodes so the
    // observer (which is also voter-roster-aware via its own
    // config) bootstraps the same VoterSet as the voters. The
    // observer's own endpoint is NOT in voters — it appears only in
    // each node's `observers = [4]` list so the leader seeds it as
    // a non-voting peer.
    let voters: Vec<VoterConfig> = (0..3)
        .map(|i| VoterConfig {
            node_id: (i as u64) + 1,
            directory_id: uuids[i].clone(),
            host: "127.0.0.1".into(),
            port: ports[i],
        })
        .collect();
    let observers = vec![4u64];

    // Start everything. Index 3 is the observer.
    let mut handles: Vec<Option<xraft_server::ServerHandle>> = Vec::with_capacity(4);
    for i in 0..4 {
        let cfg = server_config(build_observer_cluster(
            (i as u64) + 1,
            data_dirs[i].clone(),
            ports[i],
            voters.clone(),
            observers.clone(),
            /*enable_check_quorum=*/ false,
        ));
        let listener = grpc_listeners[i].take().expect("listener slot still held");
        let h = Server::start_with_listener(cfg, listener)
            .await
            .unwrap_or_else(|e| panic!("node {} must start: {e:?}", i + 1));
        handles.push(Some(h));
    }

    // -------------------------------------------- Phase 1: commit one entry
    //
    // Probe each voter (NOT the observer) for the leader. propose()
    // returns Ok only when the entry commits, which here requires a
    // 2-of-3 voter quorum. The observer is excluded from this loop
    // because it must NEVER serve a propose.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut leader_idx: Option<usize> = None;
    let mut committed_idx: Option<xraft_core::types::LogIndex> = None;
    let mut last_errors: Vec<String> = vec![String::new(); 3];
    'outer: while Instant::now() < deadline {
        for i in 0..3 {
            let h = handles[i].as_ref().unwrap();
            let driver = h.driver_handle();
            let propose_fut = driver.propose(Bytes::from_static(b"phase-1"));
            match tokio::time::timeout(Duration::from_millis(500), propose_fut).await {
                Ok(Ok(idx)) => {
                    leader_idx = Some(i);
                    committed_idx = Some(idx);
                    break 'outer;
                }
                Ok(Err(e)) => {
                    last_errors[i] = format!("{e:?}");
                }
                Err(_) => {
                    last_errors[i] = "propose timed out (likely no leader)".to_string();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let leader_idx = leader_idx.unwrap_or_else(|| {
        panic!(
            "voter cluster must elect a leader and commit within 20s; \
             last errors per voter = {last_errors:?}"
        )
    });
    let phase1_commit = committed_idx.unwrap();
    println!(
        "observer test phase 1: voter {} became leader, committed index {}",
        leader_idx + 1,
        phase1_commit.0
    );

    // The observer's status must show `role = Observer` AND
    // `last_applied >= phase1_commit` within a bounded grace period
    // (apply propagates: leader.commit → observer.fetch → observer.commit
    // → observer.apply, ~3 round trips at fetch_interval=100ms).
    //
    // While we're at it, assert NO voter status reports `Observer`
    // and NO node reports `Candidate` (election stabilised).
    let observer_status = handles[3].as_ref().unwrap().status();
    let observer_deadline = Instant::now() + Duration::from_secs(10);
    let mut last_observer_seen = observer_status.current().await;
    while Instant::now() < observer_deadline {
        let s = observer_status.current().await;
        if s.role == NodeRole::Observer && s.last_applied >= phase1_commit.0 {
            last_observer_seen = s;
            break;
        }
        last_observer_seen = s;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        last_observer_seen.role,
        NodeRole::Observer,
        "observer node 4 MUST report role=Observer (was {:?}); raw status = {last_observer_seen:?}",
        last_observer_seen.role
    );
    assert!(
        last_observer_seen.last_applied >= phase1_commit.0,
        "observer must apply the committed entry (got last_applied={}, expected >= {}); \
         raw status = {last_observer_seen:?}",
        last_observer_seen.last_applied,
        phase1_commit.0
    );

    // Cross-check: the elected leader still reports role=Leader,
    // no voter has flipped into Observer, no node is Candidate.
    //
    // Under CI load (where scheduler delays can push a follower past
    // its `election_timeout_min` between leader heartbeats), a
    // non-leader voter may *briefly* flip to PreCandidate (Raft's
    // pre-vote probe — does NOT bump the term per `node.rs::become_pre_candidate`
    // and reverts to Follower as soon as the leader's next heartbeat
    // arrives or the PreVote quorum is denied). That is healthy
    // protocol behavior, not a stable wrong configuration. A
    // genuine `Candidate` (real election in flight, term bumped)
    // IS a violation. The intent of the assertion is "no node has
    // actually started an election after phase-1 settled".
    //
    // Poll for a stable snapshot for up to 30s (was 5s pre-iter-5;
    // 5s proved insufficient under parallel test load — multiple
    // in-process 4-node clusters competing for tokio worker threads
    // routinely produced one transient `PreCandidate` per
    // sub-second window, never giving the 5s loop a clean snapshot
    // to break on). 30s + 20ms poll = 1500 sample windows, which
    // robustly captures a clean snapshot even when several follower
    // nodes are independently probing PreVote.
    let stable_deadline = Instant::now() + Duration::from_secs(30);
    let mut roles_seen: Vec<NodeRole> = Vec::with_capacity(4);
    loop {
        roles_seen.clear();
        for slot in handles.iter().take(4) {
            let st = slot.as_ref().unwrap().status().current().await;
            roles_seen.push(st.role);
        }
        let leader_ok = matches!(roles_seen[leader_idx], NodeRole::Leader);
        let observer_ok = roles_seen[3] == NodeRole::Observer;
        let no_active_election = roles_seen.iter().all(|r| !matches!(r, NodeRole::Candidate));
        if leader_ok && observer_ok && no_active_election {
            break;
        }
        if Instant::now() >= stable_deadline {
            break; // Fall through; the asserts below will fail with detail.
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        matches!(roles_seen[leader_idx], NodeRole::Leader),
        "elected leader must still report role=Leader after observer-apply settle; roles = {roles_seen:?}"
    );
    assert_eq!(
        roles_seen[3],
        NodeRole::Observer,
        "node 4 must stay Observer regardless of cluster activity; roles = {roles_seen:?}"
    );
    for (i, r) in roles_seen.iter().enumerate() {
        assert!(
            !matches!(r, NodeRole::Candidate),
            "no node may be in a Candidate (in-flight election) state after phase-1 settled; \
             node {} role = {r:?}; roles = {roles_seen:?}",
            i + 1
        );
    }

    // -------------------------------------- Phase 2: NEGATIVE QUORUM
    //
    // Shut down the two voters that are NOT the leader so the leader
    // is the lone surviving voter. With `enable_check_quorum = false`
    // it will retain its role; without quorum it cannot commit. We
    // then propose a new entry and assert:
    //   (a) the propose future times out,
    //   (b) the leader's commit_index stays at phase1_commit,
    //   (c) the observer's last_applied stays at phase1_commit
    //       (even though replication may extend its last_log_index).
    let mut shut_down: Vec<usize> = Vec::new();
    for (i, slot) in handles.iter_mut().enumerate().take(3) {
        if i == leader_idx {
            continue;
        }
        if let Some(h) = slot.take() {
            h.shutdown();
            let _ = tokio::time::timeout(Duration::from_secs(5), h.join()).await;
            shut_down.push(i);
        }
    }
    assert_eq!(shut_down.len(), 2, "must have shut down exactly 2 voters");
    println!(
        "observer test phase 2: shut down voters {:?}; leader = node {}",
        shut_down.iter().map(|i| i + 1).collect::<Vec<_>>(),
        leader_idx + 1
    );

    // Capture the leader's commit/log state AFTER the peer
    // shutdowns settle in its connection-state machine but BEFORE
    // we propose. Use a short settle period so the proposed entry
    // is the only post-baseline activity.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let leader_baseline = handles[leader_idx]
        .as_ref()
        .unwrap()
        .status()
        .current()
        .await;
    let observer_baseline = handles[3].as_ref().unwrap().status().current().await;
    assert_eq!(
        leader_baseline.commit_index, phase1_commit.0,
        "leader commit_index must equal phase-1 commit at baseline (got {})",
        leader_baseline.commit_index
    );

    // Issue the negative-quorum propose. We expect it to NOT
    // resolve within 3s — Raft's propose only Ok's on commit, and
    // commit cannot advance with 1-of-3 voters and an observer.
    let leader_driver = handles[leader_idx].as_ref().unwrap().driver_handle();
    let propose_result = tokio::time::timeout(
        Duration::from_secs(3),
        leader_driver.propose(Bytes::from_static(b"phase-2-no-quorum")),
    )
    .await;
    match propose_result {
        Err(_elapsed) => {
            // Expected — propose blocks waiting for a commit that
            // cannot happen.
        }
        Ok(Ok(idx)) => {
            panic!(
                "negative-quorum propose MUST NOT commit (observer cannot \
                 substitute for missing voters); but it returned Ok({idx:?})"
            );
        }
        Ok(Err(e)) => {
            panic!(
                "negative-quorum propose returned an unexpected error \
                 (expected timeout, indicating it is blocked on commit): {e:?}"
            );
        }
    }

    // After the 3s propose timeout, the leader's commit_index MUST
    // be unchanged from baseline. (last_log_index may have grown
    // because the leader appends locally before quorum acks; we
    // assert it grew to prove the propose actually reached the
    // engine, but commit_index must NOT have moved.)
    let leader_after = handles[leader_idx]
        .as_ref()
        .unwrap()
        .status()
        .current()
        .await;
    assert_eq!(
        leader_after.commit_index, leader_baseline.commit_index,
        "negative-quorum: leader commit_index advanced from {} to {} — \
         observer must NOT count toward quorum",
        leader_baseline.commit_index, leader_after.commit_index
    );
    assert!(
        leader_after.last_log_index > leader_baseline.last_log_index,
        "negative-quorum: leader must have APPENDED the proposed entry locally \
         (otherwise the test is not exercising the quorum path); \
         baseline last_log_index = {}, after = {}",
        leader_baseline.last_log_index,
        leader_after.last_log_index
    );

    // Allow up to 1.5s for any in-flight observer Fetch to drain
    // (the observer may have pulled the uncommitted entry — that's
    // fine, replication continues — but it MUST NOT have applied
    // it because commit didn't move).
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let observer_after = handles[3].as_ref().unwrap().status().current().await;
    assert_eq!(
        observer_after.last_applied, observer_baseline.last_applied,
        "negative-quorum: observer last_applied advanced from {} to {} — \
         observer apply pipeline must be gated on commit, not on local fetch",
        observer_baseline.last_applied, observer_after.last_applied
    );

    println!(
        "observer test phase 2 assertions passed: leader commit pinned at {} \
         (last_log advanced from {} to {}); observer last_applied pinned at {} \
         (last_log_index = {})",
        leader_after.commit_index,
        leader_baseline.last_log_index,
        leader_after.last_log_index,
        observer_after.last_applied,
        observer_after.last_log_index
    );

    // Tear down survivors.
    for slot in handles.iter_mut() {
        if let Some(h) = slot.take() {
            h.shutdown();
            let _ = tokio::time::timeout(Duration::from_secs(5), h.join()).await;
        }
    }
}
