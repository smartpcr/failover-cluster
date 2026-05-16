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
/// This in-process variant exercises the drain path
/// (`ServerHandle::shutdown()` → `handle.join()`) without going
/// through the OS signal layer. The **acceptance scenario** —
/// real SIGTERM (Unix) / CTRL_BREAK_EVENT (Windows) against the
/// spawned binary with an exit-code-0 assertion — is covered by
/// the two `graceful_shutdown_acceptance_*` tests below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_process_shutdown_drains_cleanly() {
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

// ---- Real-signal acceptance test (Stage 6.1 graceful-shutdown) -----
//
// These tests spawn the real `xraft-server` binary, wait for
// `/health` to come up, deliver an OS-level signal (SIGTERM on
// Unix, CTRL_BREAK_EVENT on Windows), and assert the process
// exits with status 0 within a generous timeout. This directly
// satisfies the brief's "graceful-shutdown" acceptance scenario.

#[cfg(any(unix, windows))]
fn xraft_server_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_xraft-server"))
}

#[cfg(any(unix, windows))]
fn pick_real_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

#[cfg(any(unix, windows))]
fn real_binary_toml(node_id: u64, data_dir: &std::path::Path, grpc_port: u16) -> String {
    let directory_id = uuid::Uuid::new_v4();
    // Forward-slash all separators so the TOML literal is portable
    // across Unix and Windows (backslashes would be interpreted as
    // TOML escape sequences and fail to parse).
    let data_dir_str = data_dir.display().to_string().replace('\\', "/");
    format!(
        r#"
node_id = {node_id}
cluster_id = "stage-6-1-lifecycle-accept"
listen_addr = "127.0.0.1:{grpc_port}"
peers = []
election_timeout_min_ms = 150
election_timeout_max_ms = 300
fetch_interval_ms = 50
tick_interval_ms = 10
snapshot_interval = 10000
max_log_entries_before_compaction = 100000
data_dir = "{data_dir_str}"
snapshot_retention_count = 3
tls_enabled = false
connect_timeout_ms = 5000
rpc_timeout_ms = 10000
max_rpc_retries = 3
retry_initial_backoff_ms = 100
retry_max_backoff_ms = 5000
max_message_size = 67108864

[[voters]]
node_id = {node_id}
directory_id = "{directory_id}"
host = "127.0.0.1"
port = {grpc_port}
"#,
    )
}

/// Generate a TOML with TWO voters so a CLI `--node-id` override
/// can legitimately pick a `node_id` that DIFFERS from the file's
/// template `node_id`. This is the "shared config template"
/// pattern described in `xraft-server/src/main.rs:176-179` —
/// an operator ships a single TOML to every node and identifies
/// the running node solely via `--node-id`.
///
/// `file_node_id` is the TOML's top-level `node_id` field (the
/// template default). `listen_port` is the running process's
/// actual gRPC listener. `voter_a_port` / `voter_b_port` are the
/// ports claimed by the two voter records; one usually matches
/// `listen_port`. The two voters have fixed `node_id`s of `1`
/// and `2` so a CLI `--node-id 2` against a file `node_id = 1`
/// still validates (both IDs are in the voter set).
#[cfg(unix)]
fn multi_voter_toml(
    file_node_id: u64,
    data_dir: &std::path::Path,
    listen_port: u16,
    voter_a_port: u16,
    voter_b_port: u16,
) -> String {
    let directory_a = uuid::Uuid::new_v4();
    let directory_b = uuid::Uuid::new_v4();
    let data_dir_str = data_dir.display().to_string().replace('\\', "/");
    format!(
        r#"
node_id = {file_node_id}
cluster_id = "stage-6-1-shared-template"
listen_addr = "127.0.0.1:{listen_port}"
peers = []
election_timeout_min_ms = 150
election_timeout_max_ms = 300
fetch_interval_ms = 50
tick_interval_ms = 10
snapshot_interval = 10000
max_log_entries_before_compaction = 100000
data_dir = "{data_dir_str}"
snapshot_retention_count = 3
tls_enabled = false
connect_timeout_ms = 5000
rpc_timeout_ms = 10000
max_rpc_retries = 3
retry_initial_backoff_ms = 100
retry_max_backoff_ms = 5000
max_message_size = 67108864

[[voters]]
node_id = 1
directory_id = "{directory_a}"
host = "127.0.0.1"
port = {voter_a_port}

[[voters]]
node_id = 2
directory_id = "{directory_b}"
host = "127.0.0.1"
port = {voter_b_port}
"#,
    )
}

/// Drop-guard that kills a spawned child if the test panics or
/// returns early before its own cleanup. Stops orphan
/// `xraft-server` processes from holding TCP listeners open.
#[cfg(any(unix, windows))]
struct ChildGuard(std::process::Child);

#[cfg(any(unix, windows))]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Poll `/health` over a raw TCP socket until it returns
/// `HTTP/1.1 200 OK` with a `node_id` field, or the deadline
/// elapses. Returns whether health was observed.
#[cfg(any(unix, windows))]
fn wait_for_health(admin_listen: &str, deadline: std::time::Instant) -> bool {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    while std::time::Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect(admin_listen) {
            s.set_read_timeout(Some(Duration::from_millis(500))).ok();
            let req = format!(
                "GET /health HTTP/1.1\r\nHost: {admin_listen}\r\nConnection: close\r\n\r\n"
            );
            if s.write_all(req.as_bytes()).is_ok() {
                let mut buf = Vec::with_capacity(1024);
                let _ = s.read_to_end(&mut buf);
                let body = String::from_utf8_lossy(&buf);
                if body.contains("200 OK") && body.contains("\"node_id\"") {
                    return true;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// **Stage 6.1 graceful-shutdown acceptance scenario (Unix).**
///
/// Spawns the real `xraft-server` binary with a valid config,
/// waits for `/health` to serve, sends a real `SIGTERM` via
/// `nix::sys::signal::kill`, and asserts the process exits with
/// status 0 within 5 s. This proves the full OS signal handling
/// path end-to-end (signal handler install → tokio::select →
/// graceful drain → `ExitCode::SUCCESS`), not just the
/// in-process drain that `in_process_shutdown_drains_cleanly`
/// covers.
#[cfg(unix)]
#[test]
fn graceful_shutdown_acceptance_real_sigterm_exits_zero() {
    use std::process::Command;
    use std::time::Instant;

    let tmp = TempDir::new().expect("tempdir");
    let cfg_path = tmp.path().join("node.toml");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let grpc_port = pick_real_port();
    let admin_port = pick_real_port();
    std::fs::write(&cfg_path, real_binary_toml(1, &data_dir, grpc_port)).unwrap();
    let admin_listen = format!("127.0.0.1:{admin_port}");

    let child = Command::new(xraft_server_bin())
        .arg("--config")
        .arg(&cfg_path)
        .arg("--admin-listen")
        .arg(&admin_listen)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn xraft-server binary");
    let mut guard = ChildGuard(child);
    let pid_raw = guard.0.id() as i32;

    let health_deadline = Instant::now() + Duration::from_secs(5);
    assert!(
        wait_for_health(&admin_listen, health_deadline),
        "/health did not return 200 within 5s"
    );

    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid_raw), Signal::SIGTERM).expect("send SIGTERM");

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < exit_deadline {
        match guard.0.try_wait() {
            Ok(Some(st)) => {
                exit_status = Some(st);
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    let st = exit_status.expect("process did not exit within 5s of SIGTERM");
    assert!(
        st.success(),
        "binary must exit with code 0 after SIGTERM per Stage 6.1 acceptance scenario, got {st:?}"
    );
}

/// **Stage 6.1 graceful-shutdown acceptance scenario — SIGINT (Unix).**
///
/// The brief explicitly lists `SIGTERM`/`SIGINT` as the two
/// signals that trigger graceful shutdown
/// (`xraft-server/src/main.rs:488,492` register handlers for
/// both). The companion test
/// `graceful_shutdown_acceptance_real_sigterm_exits_zero`
/// covers the `SIGTERM` path; this test covers `SIGINT` (the
/// Ctrl-C signal an operator sends from an interactive shell)
/// against the real binary. Without this, the SIGINT branch of
/// the `tokio::select!` in `wait_for_shutdown_signal` is wired
/// but never proven end-to-end at the OS-signal level.
///
/// Asserts the same exit-code-0 contract as the SIGTERM test —
/// any divergence in drain behaviour between the two signals
/// would be caught here.
#[cfg(unix)]
#[test]
fn graceful_shutdown_acceptance_real_sigint_exits_zero() {
    use std::process::Command;
    use std::time::Instant;

    let tmp = TempDir::new().expect("tempdir");
    let cfg_path = tmp.path().join("node.toml");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let grpc_port = pick_real_port();
    let admin_port = pick_real_port();
    std::fs::write(&cfg_path, real_binary_toml(1, &data_dir, grpc_port)).unwrap();
    let admin_listen = format!("127.0.0.1:{admin_port}");

    let child = Command::new(xraft_server_bin())
        .arg("--config")
        .arg(&cfg_path)
        .arg("--admin-listen")
        .arg(&admin_listen)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn xraft-server binary");
    let mut guard = ChildGuard(child);
    let pid_raw = guard.0.id() as i32;

    let health_deadline = Instant::now() + Duration::from_secs(5);
    assert!(
        wait_for_health(&admin_listen, health_deadline),
        "/health did not return 200 within 5s"
    );

    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid_raw), Signal::SIGINT).expect("send SIGINT");

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < exit_deadline {
        match guard.0.try_wait() {
            Ok(Some(st)) => {
                exit_status = Some(st);
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    let st = exit_status.expect("process did not exit within 5s of SIGINT");
    assert!(
        st.success(),
        "binary must exit with code 0 after SIGINT per Stage 6.1 acceptance scenario (brief: \
         \"SIGTERM/SIGINT trigger graceful shutdown\"), got {st:?}"
    );
}

/// **Stage 6.1 graceful-shutdown acceptance scenario (Windows).**
///
/// Windows has no SIGTERM; the equivalent operator-requested
/// graceful shutdown is `CTRL_BREAK_EVENT` delivered via
/// `GenerateConsoleCtrlEvent` to a child created with
/// `CREATE_NEW_PROCESS_GROUP`. `CTRL_C_EVENT` does NOT propagate
/// to such a child from its parent, so `CTRL_BREAK_EVENT` is the
/// only valid signal path to test on Windows.
#[cfg(windows)]
#[test]
fn graceful_shutdown_acceptance_real_ctrl_break_exits_zero() {
    use std::os::windows::process::CommandExt;
    use std::process::Command;
    use std::time::Instant;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

    let tmp = TempDir::new().expect("tempdir");
    let cfg_path = tmp.path().join("node.toml");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let grpc_port = pick_real_port();
    let admin_port = pick_real_port();
    std::fs::write(&cfg_path, real_binary_toml(1, &data_dir, grpc_port)).unwrap();
    let admin_listen = format!("127.0.0.1:{admin_port}");

    let child = Command::new(xraft_server_bin())
        .arg("--config")
        .arg(&cfg_path)
        .arg("--admin-listen")
        .arg(&admin_listen)
        .env("RUST_LOG", "warn")
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .expect("spawn xraft-server binary");
    let mut guard = ChildGuard(child);
    let pid = guard.0.id();

    let health_deadline = Instant::now() + Duration::from_secs(5);
    assert!(
        wait_for_health(&admin_listen, health_deadline),
        "/health did not return 200 within 5s"
    );

    use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
    // SAFETY: `GenerateConsoleCtrlEvent` is an FFI call that takes
    // plain integer parameters and reports failure via its return
    // value. `pid` is the OS-allocated process id returned by
    // `Child::id()` and is valid until the child is reaped.
    let ok = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid) };
    assert!(
        ok != 0,
        "GenerateConsoleCtrlEvent failed for child pid {pid} (GetLastError = {})",
        std::io::Error::last_os_error()
    );

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < exit_deadline {
        match guard.0.try_wait() {
            Ok(Some(st)) => {
                exit_status = Some(st);
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    let st = exit_status.expect("process did not exit within 5s of CTRL_BREAK");
    assert!(
        st.success(),
        "binary must exit with code 0 after CTRL_BREAK per Stage 6.1 acceptance scenario, got {st:?}"
    );
}

/// **Stage 6.1 structured-logging brief step (Unix).**
///
/// The brief explicitly requires "structured logging with
/// `tracing`: JSON output format, configurable log level via
/// `RUST_LOG` env var" — wired in
/// `xraft-server/src/main.rs::init_tracing` via
/// `fmt::layer().json()` (which writes to stdout by default).
/// Prior iters proved the lifecycle and signal paths but never
/// asserted that the on-the-wire log output is actually
/// JSON-shaped.
///
/// This test spawns the real binary with `RUST_LOG=info` and
/// `Stdio::piped()` stdout, waits for `/health` to confirm
/// startup (so we know the `init_tracing` call ran and at least
/// the startup "starting xraft-server" info line was emitted),
/// sends a clean SIGTERM, reads the captured stdout to
/// completion, and asserts at least one non-empty line parses
/// as a JSON object containing the `timestamp` field
/// `tracing_subscriber::fmt::layer().json()` emits by default.
///
/// This is the missing end-to-end coverage for the brief's
/// structured-logging step that complements the existing
/// signal/HTTP/SIGHUP tests.
#[cfg(unix)]
#[test]
fn structured_logging_emits_json_lines_to_stdout() {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let tmp = TempDir::new().expect("tempdir");
    let cfg_path = tmp.path().join("node.toml");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let grpc_port = pick_real_port();
    let admin_port = pick_real_port();
    std::fs::write(&cfg_path, real_binary_toml(1, &data_dir, grpc_port)).unwrap();
    let admin_listen = format!("127.0.0.1:{admin_port}");

    let child = Command::new(xraft_server_bin())
        .arg("--config")
        .arg(&cfg_path)
        .arg("--admin-listen")
        .arg(&admin_listen)
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn xraft-server binary");
    let mut guard = ChildGuard(child);
    let pid_raw = guard.0.id() as i32;

    let health_deadline = Instant::now() + Duration::from_secs(5);
    assert!(
        wait_for_health(&admin_listen, health_deadline),
        "/health did not return 200 within 5s"
    );

    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid_raw), Signal::SIGTERM).expect("send SIGTERM");

    // Wait for the process to exit so the stdout pipe closes
    // and `read_to_end` returns rather than blocking.
    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < exit_deadline {
        match guard.0.try_wait() {
            Ok(Some(st)) => {
                exit_status = Some(st);
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    let exit_status = exit_status.expect("process did not exit within 5s of SIGTERM");
    assert!(
        exit_status.success(),
        "binary must exit cleanly after SIGTERM, got {exit_status:?}"
    );

    let mut stdout_buf = Vec::new();
    guard
        .0
        .stdout
        .take()
        .expect("stdout was piped at spawn")
        .read_to_end(&mut stdout_buf)
        .expect("read child stdout");
    let stdout = String::from_utf8_lossy(&stdout_buf);

    // Every non-empty stdout line emitted by the tracing JSON
    // layer must parse as a JSON object carrying the default
    // `timestamp` field. Count valid lines and keep a sample
    // failure for debugging if zero are found.
    let mut valid_json_lines = 0usize;
    let mut sample_failure: Option<String> = None;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(j) if j.is_object() && j.get("timestamp").is_some() => {
                valid_json_lines += 1;
            }
            Ok(j) => {
                if sample_failure.is_none() {
                    sample_failure = Some(format!("parsed as JSON but missing `timestamp`: {j}"));
                }
            }
            Err(e) => {
                if sample_failure.is_none() {
                    sample_failure = Some(format!("not valid JSON ({e}): {trimmed}"));
                }
            }
        }
    }
    assert!(
        valid_json_lines >= 1,
        "structured-logging brief: stdout must contain at least one JSON-formatted log \
         line with a `timestamp` field, found 0 (sample failure: {sample_failure:?}). \
         Full stdout ({} bytes):\n{stdout}",
        stdout.len()
    );
}

/// **Stage 6.1 SIGHUP-reload end-to-end test (Unix).**
///
/// Spawns the real `xraft-server` binary with `--node-id 1` (an
/// explicit CLI override that matches the file's `node_id` so
/// startup membership validation passes), waits for `/health` to
/// return `config_revision = 0`, rewrites the TOML with a
/// hot-reloadable change (`tick_interval_ms = 25`, was `10`),
/// sends a real `SIGHUP` via `nix::sys::signal::kill`, and
/// asserts:
///
/// 1. `/health` reports `config_revision >= 1` within 5 s — proof
///    that the full reload pipeline ran end-to-end (TOML re-read,
///    CLI override re-applied, `NodeConfig::validate()` passed,
///    driver tick-interval refreshed, counter bumped).
/// 2. The child process is still alive (`try_wait` returns
///    `Ok(None)`) — the new `cli_node_id_override` re-application
///    branch did NOT panic or kill the binary.
/// 3. A subsequent `SIGTERM` produces a clean exit-code-0
///    shutdown — the reloaded server still honours the
///    `graceful-shutdown` brief scenario.
///
/// This is the missing end-to-end coverage for the iter-8 fix
/// to `wait_for_shutdown_signal` / `reload_config` and exercises
/// SIGHUP (the third signal kind the binary subscribes to,
/// previously only covered indirectly by in-process unit tests
/// in `xraft-server/src/main.rs::reload_config_*`).
#[cfg(unix)]
#[test]
fn sighup_reload_bumps_config_revision_and_keeps_binary_alive() {
    use std::process::Command;
    use std::time::Instant;

    let tmp = TempDir::new().expect("tempdir");
    let cfg_path = tmp.path().join("node.toml");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let grpc_port = pick_real_port();
    let admin_port = pick_real_port();
    std::fs::write(&cfg_path, real_binary_toml(1, &data_dir, grpc_port)).unwrap();
    let admin_listen = format!("127.0.0.1:{admin_port}");

    // Pass `--node-id 1` so the new iter-8 CLI-override
    // reapplication branch in `reload_config` actually runs (it
    // is a no-op when `cli_node_id_override` is `None`).
    let child = Command::new(xraft_server_bin())
        .arg("--config")
        .arg(&cfg_path)
        .arg("--node-id")
        .arg("1")
        .arg("--admin-listen")
        .arg(&admin_listen)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn xraft-server binary");
    let mut guard = ChildGuard(child);
    let pid_raw = guard.0.id() as i32;

    // Wait for /health to come up and report `config_revision = 0`.
    let health_deadline = Instant::now() + Duration::from_secs(5);
    let initial_rev = poll_config_revision(&admin_listen, health_deadline)
        .expect("/health must return 200 with a config_revision field within 5s");
    assert_eq!(
        initial_rev, 0,
        "config_revision must start at 0 before any SIGHUP"
    );

    // Rewrite the TOML with a hot-reloadable change (bump the
    // tick interval). The voter set is unchanged so the
    // re-applied `--node-id 1` override still validates.
    let reloaded_toml = real_binary_toml(1, &data_dir, grpc_port)
        .replace("tick_interval_ms = 10", "tick_interval_ms = 25");
    assert!(
        reloaded_toml.contains("tick_interval_ms = 25"),
        "test fixture must contain the bumped tick interval — actual: {reloaded_toml}"
    );
    std::fs::write(&cfg_path, &reloaded_toml).unwrap();

    // Send a real SIGHUP.
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid_raw), Signal::SIGHUP).expect("send SIGHUP");

    // Poll /health until config_revision bumps to >= 1, or 5s elapses.
    let bump_deadline = Instant::now() + Duration::from_secs(5);
    let mut observed_rev: u64 = 0;
    while Instant::now() < bump_deadline {
        if let Some(r) =
            poll_config_revision(&admin_listen, Instant::now() + Duration::from_millis(200))
            && r >= 1
        {
            observed_rev = r;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        observed_rev >= 1,
        "SIGHUP must bump config_revision to >= 1 within 5s, observed {observed_rev}"
    );

    // Child must still be running — the SIGHUP path (including
    // the iter-8 cli_node_id_override re-application branch) must
    // NOT have panicked or killed the binary.
    match guard.0.try_wait() {
        Ok(None) => { /* still running, expected */ }
        Ok(Some(st)) => panic!(
            "binary exited unexpectedly after SIGHUP (expected to keep running), \
             status = {st:?}"
        ),
        Err(e) => panic!("try_wait failed after SIGHUP: {e}"),
    }

    // Now send SIGTERM and assert a clean exit-code-0 shutdown
    // — proves the reload didn't break the graceful-shutdown path.
    kill(Pid::from_raw(pid_raw), Signal::SIGTERM).expect("send SIGTERM");
    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < exit_deadline {
        match guard.0.try_wait() {
            Ok(Some(st)) => {
                exit_status = Some(st);
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    let st = exit_status.expect("process did not exit within 5s of SIGTERM");
    assert!(
        st.success(),
        "binary must exit with code 0 after SIGTERM following a successful SIGHUP, got {st:?}"
    );
}

/// Single GET /health and parse the JSON body's `config_revision`
/// field. Returns `None` on any failure (TCP, HTTP, JSON parse,
/// missing field) so the caller can poll until success.
#[cfg(unix)]
fn poll_config_revision(admin_listen: &str, deadline: std::time::Instant) -> Option<u64> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    while std::time::Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect(admin_listen) {
            s.set_read_timeout(Some(Duration::from_millis(500))).ok();
            let req = format!(
                "GET /health HTTP/1.1\r\nHost: {admin_listen}\r\nConnection: close\r\n\r\n"
            );
            if s.write_all(req.as_bytes()).is_ok() {
                let mut buf = Vec::with_capacity(2048);
                let _ = s.read_to_end(&mut buf);
                let raw = String::from_utf8_lossy(&buf);
                // Split off the HTTP headers — the JSON body is
                // after the first blank line.
                if let Some(body_start) = raw.find("\r\n\r\n")
                    && raw.contains("200 OK")
                {
                    let body = &raw[body_start + 4..];
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body.trim())
                        && let Some(rev) = json.get("config_revision").and_then(|v| v.as_u64())
                    {
                        return Some(rev);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

/// Single GET /health. Returns the parsed JSON body or `None`
/// on any TCP / HTTP / JSON-parse failure. Used by tests that
/// need to inspect more than one field of the response (e.g. the
/// shared-template SIGHUP test needs `node_id` and
/// `config_revision` from the same snapshot).
#[cfg(unix)]
fn fetch_health_json(admin_listen: &str) -> Option<serde_json::Value> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let mut s = TcpStream::connect(admin_listen).ok()?;
    s.set_read_timeout(Some(Duration::from_millis(500))).ok();
    let req = format!("GET /health HTTP/1.1\r\nHost: {admin_listen}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).ok()?;
    let mut buf = Vec::with_capacity(2048);
    let _ = s.read_to_end(&mut buf);
    let raw = String::from_utf8_lossy(&buf);
    if !raw.contains("200 OK") {
        return None;
    }
    let body_start = raw.find("\r\n\r\n")?;
    let body = &raw[body_start + 4..];
    serde_json::from_str::<serde_json::Value>(body.trim()).ok()
}

/// **Stage 6.1 SIGHUP-reload shared-template MISMATCH test (Unix).**
///
/// Direct end-to-end coverage of the "shared config template"
/// scenario described in `xraft-server/src/main.rs:176-179`:
/// multiple nodes are deployed from a single TOML and identify
/// themselves via `--node-id <N>`, so the file's `node_id` field
/// does NOT match the running node's identity. The companion
/// test `sighup_reload_bumps_config_revision_and_keeps_binary_alive`
/// covers the MATCHING case (file `node_id` == CLI `--node-id`),
/// but only this test exercises the iter-8 `cli_node_id_override`
/// re-application branch in `reload_config` with a real mismatch.
///
/// Test plan (each step has a direct observable):
///
/// 1. Write a TOML with `node_id = 1` (template default) and
///    TWO voters `[1, 2]`. Spawn `xraft-server --node-id 2` so
///    the running identity is `2`, NOT `1`.
/// 2. **Startup observable**: `/health.node_id == 2`. Proves the
///    CLI override was applied at startup (the file's `1` was
///    overridden).
/// 3. **Phase A (happy reload)**: rewrite the TOML with a benign
///    hot-reloadable change (`tick_interval_ms` 10 → 25). Voters
///    are unchanged, so the re-applied `--node-id 2` override
///    still satisfies `validate_membership` and reload succeeds.
///    `config_revision` must bump to `1`.
/// 4. **Phase B (drift rejection)**: rewrite the TOML to a
///    SINGLE-voter set `[1]`. The file's `node_id = 1` is still
///    a valid voter, BUT the running override `2` is no longer
///    in the set. With the iter-8 fix the override is re-applied
///    inside `reload_config`, `NodeConfig::validate()` fails on
///    the new membership, and the function returns early without
///    bumping. **Observable**: after a 2-second wait,
///    `config_revision` must STILL equal `1` (NOT `2`).
/// 5. The process must still be alive after both SIGHUPs
///    (`try_wait` returns `Ok(None)`).
/// 6. SIGTERM must produce a clean exit-code-0 shutdown.
///
/// **Why this test catches the iter-7 bug**: on the pre-iter-8
/// code path, `reload_config` set
/// `new_cfg.cluster.node_id = file_node_id` and never re-applied
/// the CLI override; the drifted Phase-B file would still pass
/// `validate_membership` under `node_id = 1` (which IS in the
/// trimmed voter set), `config_revision` would bump to `2`, and
/// the cached snapshot would silently revert to identity `1` —
/// diverging from the running engine's actual identity `2`. The
/// iter-8 fix is exactly the wedge between these two outcomes.
#[cfg(unix)]
#[test]
fn sighup_reload_reapplies_mismatched_cli_node_id_override_for_shared_template() {
    use std::process::Command;
    use std::time::Instant;

    let tmp = TempDir::new().expect("tempdir");
    let cfg_path = tmp.path().join("node.toml");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let listen_port = pick_real_port();
    let voter_a_port = pick_real_port();
    let admin_port = pick_real_port();
    let admin_listen = format!("127.0.0.1:{admin_port}");

    // Template: file `node_id = 1`, voters = [1, 2]. The running
    // process listens on `listen_port` (matches voter 2's port).
    // Voter 1's port (`voter_a_port`) is a placeholder; nothing
    // listens there — Raft peer RPCs to voter 1 will fail, but
    // that's fine because this test only exercises the lifecycle
    // path, not replication.
    let toml_v1 = multi_voter_toml(
        /*file_node_id=*/ 1,
        &data_dir,
        listen_port,
        voter_a_port,
        listen_port,
    );
    std::fs::write(&cfg_path, &toml_v1).unwrap();

    // Spawn with `--node-id 2` — explicitly DIFFERENT from the
    // file's `1`. This is the exact shared-template scenario the
    // iter-8 fix targets.
    let child = Command::new(xraft_server_bin())
        .arg("--config")
        .arg(&cfg_path)
        .arg("--node-id")
        .arg("2")
        .arg("--admin-listen")
        .arg(&admin_listen)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn xraft-server binary");
    let mut guard = ChildGuard(child);
    let pid_raw = guard.0.id() as i32;

    // Startup observable: `/health.node_id == 2` (the CLI override
    // applied at startup, NOT the file's `1`).
    let health_deadline = Instant::now() + Duration::from_secs(5);
    let initial = loop {
        if let Some(j) = fetch_health_json(&admin_listen) {
            break j;
        }
        if Instant::now() >= health_deadline {
            panic!("/health did not serve within 5s");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        initial["node_id"], 2,
        "startup: CLI --node-id 2 must override file's node_id=1 — got {initial}"
    );
    assert_eq!(
        initial["config_revision"], 0,
        "startup: config_revision must start at 0 — got {initial}"
    );

    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    // Phase A: SIGHUP with hot-reloadable change. Voters
    // unchanged so the re-applied `--node-id 2` override still
    // satisfies validation → reload succeeds, revision bumps to 1.
    let toml_v2 = toml_v1.replace("tick_interval_ms = 10", "tick_interval_ms = 25");
    assert!(
        toml_v2.contains("tick_interval_ms = 25"),
        "phase A fixture: must contain bumped tick interval"
    );
    std::fs::write(&cfg_path, &toml_v2).unwrap();
    kill(Pid::from_raw(pid_raw), Signal::SIGHUP).expect("send SIGHUP (phase A)");

    let bump_deadline = Instant::now() + Duration::from_secs(5);
    let mut after_a: Option<serde_json::Value> = None;
    while Instant::now() < bump_deadline {
        if let Some(j) = fetch_health_json(&admin_listen)
            && j["config_revision"].as_u64() == Some(1)
        {
            after_a = Some(j);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let after_a = after_a.expect("phase A: SIGHUP must bump config_revision to 1 within 5s");
    assert_eq!(
        after_a["node_id"], 2,
        "phase A: /health.node_id must remain 2 after reload — got {after_a}"
    );

    // Phase B: rewrite the TOML to remove voter 2. The file's
    // `node_id = 1` is still a valid voter, but the RUNNING
    // override `2` is no longer in the voter set. With the iter-8
    // fix the override is re-applied inside `reload_config`,
    // `validate()` fails, and the function returns early WITHOUT
    // bumping. The pre-iter-8 buggy code path would accept the
    // file unchanged (because it sets `cluster.node_id` from the
    // file, not the override) and bump revision to 2 — making
    // `config_revision == 1` after a safe wait the wedge between
    // the fix-present and fix-absent code paths.
    let toml_v3 = real_binary_toml(/*node_id=*/ 1, &data_dir, listen_port);
    std::fs::write(&cfg_path, &toml_v3).unwrap();
    kill(Pid::from_raw(pid_raw), Signal::SIGHUP).expect("send SIGHUP (phase B)");

    // Wait long enough that any successful reload would have
    // bumped the revision. Phase A's bump arrived in under
    // ~200ms on the same machine, so 2s is a generous safety
    // margin without making the test slow.
    std::thread::sleep(Duration::from_secs(2));
    let after_b = fetch_health_json(&admin_listen).expect("/health after phase B");
    assert_eq!(
        after_b["config_revision"], 1,
        "phase B: iter-8 fix MUST reject the drifted file (running override 2 is no \
         longer in voter set [1]); config_revision must remain 1, got {after_b}"
    );
    assert_eq!(
        after_b["node_id"], 2,
        "phase B: /health.node_id must remain 2 (running engine identity unchanged) \
         — got {after_b}"
    );

    // Process must still be alive after both SIGHUPs — neither
    // a successful reload (phase A) nor a rejected reload
    // (phase B) is allowed to crash a healthy server.
    match guard.0.try_wait() {
        Ok(None) => { /* still running, expected */ }
        Ok(Some(st)) => panic!(
            "binary exited unexpectedly after SIGHUP phase B (expected to keep \
             running), status = {st:?}"
        ),
        Err(e) => panic!("try_wait failed after SIGHUP phase B: {e}"),
    }

    // SIGTERM → clean exit-code-0 shutdown.
    kill(Pid::from_raw(pid_raw), Signal::SIGTERM).expect("send SIGTERM");
    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < exit_deadline {
        match guard.0.try_wait() {
            Ok(Some(st)) => {
                exit_status = Some(st);
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    let st = exit_status.expect("process did not exit within 5s of SIGTERM");
    assert!(
        st.success(),
        "binary must exit with code 0 after SIGTERM following two SIGHUPs, got {st:?}"
    );
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

/// Verify `/metrics` exposes the canonical Prometheus metric set:
/// the Stage 6.1 MVP subset plus the Stage 7.1 leader / replication
/// observability extensions (`xraft_replication_lag`,
/// `xraft_commit_latency_seconds`, `xraft_fetch_requests_total`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_exposes_mvp_metric_set() {
    let tmp = TempDir::new().unwrap();
    let cfg = build_server_config(&tmp);
    let handle = Server::start(cfg).await.expect("server must start");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let body = http_get(&handle.admin_addr.to_string(), "/metrics").await;

    for metric in [
        // Stage 6.1 MVP subset — every entry emits at least one
        // sample row at startup (gauges initialise to 0, histograms
        // emit bucket rows even with zero count).
        "xraft_current_term",
        "xraft_commit_index",
        "xraft_current_leader",
        "xraft_role",
        "xraft_election_latency_seconds",
        "xraft_append_records_total",
        // Stage 7.1 — histograms emit bucket rows even with zero
        // observations, so the rendered `_seconds` sample-row
        // substring is reliable.
        "xraft_commit_latency_seconds",
    ] {
        assert!(
            body.contains(metric),
            "metrics output must include '{metric}', body = {body}"
        );
    }

    // Stage 7.1 — `xraft_replication_lag` and `xraft_fetch_requests`
    // are `Family<Label, …>` metrics: prometheus-client only emits
    // sample rows once a label set has been observed. In this
    // single-node lifecycle test no follower has been registered
    // (replication_lag) and no Fetch RPC has been issued
    // (fetch_requests), so the rendered exposition for these two
    // contains ONLY the HELP and TYPE descriptor lines. Assert on
    // the descriptor lines so the test stays sensitive to
    // accidental de-registration without requiring live traffic.
    // The Counter family is registered without the `_total` suffix
    // (the renderer auto-appends it on sample rows) — the HELP line
    // therefore uses the bare name.
    for descriptor in [
        "# HELP xraft_replication_lag",
        "# HELP xraft_fetch_requests",
    ] {
        assert!(
            body.contains(descriptor),
            "metrics output must include descriptor line '{descriptor}', body = {body}"
        );
    }

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
}

/// Negative-assertion guard for Stage 7.3 metrics that are NOT
/// yet wired into the registry. Stage 7.1's leader / replication
/// observability extensions (`xraft_replication_lag`,
/// `xraft_commit_latency_seconds`, `xraft_fetch_requests_total`)
/// have ALREADY landed via the Check-Quorum-and-Leader-Lease
/// merge, so they intentionally fall outside this list — see the
/// companion `metrics_endpoint_exposes_mvp_metric_set` test which
/// asserts they ARE present.
///
/// This negative test keeps the Stage 7.3 (log compaction) scope
/// structural: when Stage 7.3 lands its owner must update this
/// list at the same time it extends the registry, preventing
/// accidental cross-stage scope creep and giving the later stage
/// a compile-time-style reminder via a single failing test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_excludes_stage_7_3_metrics() {
    let tmp = TempDir::new().unwrap();
    let cfg = build_server_config(&tmp);
    let handle = Server::start(cfg).await.expect("server must start");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let body = http_get(&handle.admin_addr.to_string(), "/metrics").await;

    for later_stage_metric in [
        // Stage 7.3 — log compaction metrics, not yet registered.
        "xraft_snapshot_installs_total",
        "xraft_log_end_offset",
    ] {
        assert!(
            !body.contains(later_stage_metric),
            "/metrics must not expose '{later_stage_metric}' until Stage 7.3 \
             registers it; body = {body}"
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
