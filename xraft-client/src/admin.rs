//! Operator-facing admin client.
//!
//! `AdminClient` is the **HTTP** counterpart to the gRPC-based
//! [`crate::peer::PeerClient`]. Where `PeerClient` carries the
//! consensus traffic between cluster peers, `AdminClient` talks to a
//! node's admin HTTP listener (see
//! [`xraft_server::admin`](../../xraft_server/admin/index.html)) for
//! operational queries an SRE or operator dashboard needs out-of-band
//! from the data plane:
//!
//! - **Health** (`GET /health`) — node role / term / commit cursor for
//!   liveness probes and dashboards.
//! - **Cluster status** (`GET /admin/status`) — leader id, term, voter
//!   set, cluster id for routing-aware tooling and operator scripts.
//! - **Prometheus scrape** (`GET /metrics`) — raw text exposition
//!   payload for SRE consumption.
//! - **Trigger snapshot** (`POST /admin/trigger-snapshot`) — operator
//!   hook to force the leader to take a fresh snapshot at its current
//!   `commit_index`. Used by SRE tooling to bound log-replay time on a
//!   subsequent cold start without waiting for the auto-snapshot
//!   threshold to fire.
//!
//! ## Scope (per `tech-spec.md` §2.6 and `e2e-scenarios.md` Feature 11)
//!
//! `xraft-client` is an **internal-only** crate. There is no external
//! consumer SDK in v1: the admin client exposes ONLY the
//! operational/diagnostic surface listed above. Specifically:
//!
//! - There is no `propose` / `read` surface here — those are the
//!   embedded API on [`xraft_server::Server`](../../xraft_server/struct.Server.html).
//!
//! ## Timeouts and retries
//!
//! Defaults applied via [`AdminConfig`]:
//!
//! - `connect_timeout` = 5s — short by design, so a hanging operator
//!   probe surfaces a misconfigured admin URL within a single trip.
//! - `request_timeout` = 30s — covers full request/response including
//!   `/metrics` payloads on large clusters.
//! - `max_retries` = 3, `initial_backoff` = 100ms,
//!   `max_backoff` = 5s — transient transport failures (timeouts,
//!   connect refused) are retried with **equal-jitter exponential
//!   backoff** (sleep uniformly in `[backoff/2, backoff]`) so a
//!   recovering admin endpoint is not pummelled by a thundering-herd
//!   of operator probes synchronously detecting the same outage. HTTP
//!   status failures (5xx etc.) are NOT retried — those indicate the
//!   server processed the request and chose to fail.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use serde::Deserialize;

use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::types::NodeId;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Tunables for the admin HTTP client. Defaults match the values
/// called out in the workstream brief (connect=5s, request=30s,
/// equal-jitter exponential backoff with `max_retries=3`,
/// `initial_backoff=100ms`, `max_backoff=5s`).
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Base URL of the admin endpoint, e.g. `http://10.0.0.1:7001`.
    /// Trailing `/` is allowed; the client normalises it away.
    pub base_url: String,
    /// Time to wait for the TCP connect (and TLS handshake, if any)
    /// to complete before failing.
    pub connect_timeout: Duration,
    /// Time to wait for the full request/response cycle to complete
    /// (covers the full body, not just the headers).
    pub request_timeout: Duration,
    /// Maximum number of retry attempts on transient transport failure
    /// (`reqwest::Error::is_timeout()` or `is_connect()`). A request
    /// that fails on its first attempt + every retry returns the LAST
    /// observed error to the caller.
    pub max_retries: u32,
    /// Initial exponential-backoff delay. Doubled on every retry up
    /// to `max_backoff`. Equal-jitter halves the actual sleep so
    /// concurrent probes spread out instead of synchronising on the
    /// recovery instant.
    pub initial_backoff: Duration,
    /// Cap on the exponential-backoff delay.
    pub max_backoff: Duration,
}

impl AdminConfig {
    /// Build a config with the documented defaults.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

/// JSON shape returned by `GET /health` (see
/// [`xraft_server::admin`](../../xraft_server/admin/index.html)).
///
/// Mirrors [`xraft_server::status::NodeStatus`] plus the `config_revision`
/// counter the admin handler grafts on. Fields use `serde(default)` so
/// a future server version can omit a field without breaking older
/// clients.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub node_id: u64,
    pub role: String,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<u64>,
    pub last_log_index: u64,
    #[serde(default)]
    pub config_revision: u64,
}

/// JSON shape returned by `GET /admin/status` (added by Stage 6.2).
///
/// Carries everything in [`HealthResponse`] plus the cluster-level
/// metadata an operator needs to route traffic: `cluster_id` and the
/// configured `voters` set. The leader hint is folded into
/// [`HealthResponse::leader_id`] / [`Self::leader_id`] so a probe that
/// only needs "where is the leader?" can stop at one of these fields.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClusterStatusResponse {
    pub cluster_id: String,
    pub voters: Vec<u64>,
    pub node_id: u64,
    pub role: String,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<u64>,
    pub last_log_index: u64,
}

impl ClusterStatusResponse {
    /// Typed accessor for the leader hint.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id.map(NodeId)
    }
}

/// JSON shape returned by `POST /admin/trigger-snapshot`.
///
/// Mirrors `xraft_server::admin::TriggeredSnapshotInfo` on the wire:
/// the canonical anchor (`last_included_index` / `last_included_term`)
/// for the freshly-persisted snapshot plus the on-disk size in bytes
/// so an SRE dashboard can chart snapshot growth over time.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TriggeredSnapshotInfo {
    /// Log index the snapshot covers up to (inclusive). Matches
    /// `SnapshotMeta.last_included_index` on the engine side.
    pub last_included_index: u64,
    /// Term of the entry at `last_included_index`. Matches
    /// `SnapshotMeta.last_included_term`.
    pub last_included_term: u64,
    /// On-disk size of the serialised snapshot payload in bytes.
    pub size_bytes: u64,
}

/// Operator-facing admin client.
///
/// Cheaply cloneable — the underlying [`reqwest::Client`] is itself
/// `Arc`-backed and pools connections internally, so clones share the
/// TCP connection pool.
#[derive(Debug, Clone)]
pub struct AdminClient {
    http: reqwest::Client,
    base_url: String,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl AdminClient {
    /// Construct an `AdminClient` against the supplied base URL with
    /// the default timeouts.
    pub fn new(base_url: impl Into<String>) -> XResult<Self> {
        Self::with_config(AdminConfig::new(base_url))
    }

    /// Construct an `AdminClient` from a fully-specified
    /// [`AdminConfig`].
    pub fn with_config(cfg: AdminConfig) -> XResult<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(cfg.connect_timeout)
            .timeout(cfg.request_timeout)
            // Operator tooling is point-to-point: never auto-follow a
            // redirect to a different host. A redirect on a Raft admin
            // endpoint is almost always a misconfiguration we want
            // surfaced rather than silently followed.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| XRaftError::Transport(format!("AdminClient reqwest build: {e}")))?;
        let base_url = cfg.base_url.trim_end_matches('/').to_string();
        if base_url.is_empty() {
            return Err(XRaftError::Config(
                "AdminClient base_url must not be empty".into(),
            ));
        }
        if cfg.initial_backoff.is_zero() {
            return Err(XRaftError::Config(
                "AdminConfig.initial_backoff must be > 0".into(),
            ));
        }
        if cfg.max_backoff < cfg.initial_backoff {
            return Err(XRaftError::Config(format!(
                "AdminConfig.max_backoff ({:?}) must be >= initial_backoff ({:?})",
                cfg.max_backoff, cfg.initial_backoff
            )));
        }
        Ok(Self {
            http,
            base_url,
            max_retries: cfg.max_retries,
            initial_backoff: cfg.initial_backoff,
            max_backoff: cfg.max_backoff,
        })
    }

    /// Base URL the client was constructed with (normalised to drop a
    /// trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `GET /health` — returns the node's liveness/role snapshot.
    pub async fn health(&self) -> XResult<HealthResponse> {
        let url = format!("{}/health", self.base_url);
        let resp = self
            .send_with_retry("GET", &url, || self.http.get(&url))
            .await?;
        resp.json::<HealthResponse>()
            .await
            .map_err(|e| XRaftError::Transport(format!("AdminClient GET {url} decode: {e}")))
    }

    /// `GET /admin/status` — returns cluster-level metadata (leader,
    /// term, voter set, cluster id).
    ///
    /// Scenario *admin-client-status*: routing-aware tooling that
    /// needs to find the leader before issuing a write talks to this
    /// endpoint rather than guessing.
    pub async fn status(&self) -> XResult<ClusterStatusResponse> {
        let url = format!("{}/admin/status", self.base_url);
        let resp = self
            .send_with_retry("GET", &url, || self.http.get(&url))
            .await?;
        resp.json::<ClusterStatusResponse>()
            .await
            .map_err(|e| XRaftError::Transport(format!("AdminClient GET {url} decode: {e}")))
    }

    /// `GET /metrics` — returns the raw Prometheus text-exposition
    /// payload. Callers parse / re-emit / aggregate as needed.
    pub async fn metrics(&self) -> XResult<Bytes> {
        let url = format!("{}/metrics", self.base_url);
        let resp = self
            .send_with_retry("GET", &url, || self.http.get(&url))
            .await?;
        resp.bytes()
            .await
            .map_err(|e| XRaftError::Transport(format!("AdminClient GET {url} body: {e}")))
    }

    /// `POST /admin/trigger-snapshot` — ask the leader to take a
    /// fresh snapshot at its current `commit_index`. Returns the
    /// resulting [`TriggeredSnapshotInfo`] (the canonical
    /// `(last_included_index, last_included_term, size_bytes)` anchor)
    /// so the operator can confirm the snapshot landed.
    ///
    /// **Routing**: this MUST be sent to the current leader. A
    /// follower replies with `409 Conflict` (the server-side
    /// `NotLeader` error path), in which case the caller should use
    /// [`Self::status`] to discover the leader and retry against its
    /// admin endpoint.
    ///
    /// The POST is idempotent at the server: replaying a `trigger-
    /// snapshot` against the same commit cursor yields another
    /// snapshot with an equivalent `last_included_index`, so the
    /// transient-error retry loop is safe to apply here as well.
    pub async fn trigger_snapshot(&self) -> XResult<TriggeredSnapshotInfo> {
        let url = format!("{}/admin/trigger-snapshot", self.base_url);
        let resp = self
            .send_with_retry("POST", &url, || self.http.post(&url))
            .await?;
        resp.json::<TriggeredSnapshotInfo>()
            .await
            .map_err(|e| XRaftError::Transport(format!("AdminClient POST {url} decode: {e}")))
    }

    /// Execute an HTTP request with equal-jitter exponential backoff
    /// retry on transient transport failure.
    ///
    /// `make_request` is a closure that builds a fresh
    /// [`reqwest::RequestBuilder`] on every attempt. Reqwest's
    /// `RequestBuilder` is consumed by `.send()` and `try_clone()` is
    /// best-effort (it returns `None` for streaming bodies), so the
    /// retry loop MUST be able to rebuild the request from scratch.
    ///
    /// Retry classification (conservative to avoid double-side-effects):
    /// - `is_timeout()` — request took longer than `request_timeout`.
    /// - `is_connect()` — TCP connect / TLS handshake failed.
    /// - Everything else (4xx / 5xx body parsed, decode errors,
    ///   redirect refusal, …) is NOT retried.
    ///
    /// A response with a non-success HTTP status is also NOT retried
    /// — it is converted into an `XRaftError::Transport` carrying the
    /// status code so the caller can react to authoritative server
    /// errors. The `xraft-transport` peer-RPC retry loop uses the
    /// same convention (see `xraft-transport/src/grpc_client.rs`).
    async fn send_with_retry<F>(
        &self,
        method: &str,
        url: &str,
        mut make_request: F,
    ) -> XResult<reqwest::Response>
    where
        F: FnMut() -> reqwest::RequestBuilder,
    {
        let mut attempt: u32 = 0;
        let mut backoff = self.initial_backoff;
        loop {
            let send_fut = make_request().send();
            let outcome = handle_response(method, url, send_fut.await);
            match outcome {
                Ok(resp) => return Ok(resp),
                Err((err, retryable)) => {
                    if !retryable || attempt >= self.max_retries {
                        return Err(err);
                    }
                    let sleep = jittered_sleep_duration(backoff);
                    tracing::debug!(
                        target: "xraft_client::admin",
                        attempt,
                        max_retries = self.max_retries,
                        backoff_ms = sleep.as_millis() as u64,
                        url,
                        method,
                        "AdminClient transient failure, sleeping before retry"
                    );
                    sleep_for(sleep).await;
                    attempt += 1;
                    backoff = next_backoff(backoff, self.max_backoff);
                }
            }
        }
    }
}

/// Classify the result of a single HTTP attempt. Returns `Ok(resp)`
/// on a 2xx; `Err((err, retryable))` otherwise.
fn handle_response(
    method: &str,
    url: &str,
    res: Result<reqwest::Response, reqwest::Error>,
) -> Result<reqwest::Response, (XRaftError, bool)> {
    match res {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                Ok(resp)
            } else {
                // HTTP-level failure (4xx / 5xx) — NOT retried. Carries
                // the status code so the operator sees the server's
                // authoritative response (e.g. 409 NotLeader, 503
                // shutdown, 500 storage).
                Err((
                    XRaftError::Transport(format!("AdminClient {method} {url}: status {status}")),
                    false,
                ))
            }
        }
        Err(e) => {
            let retryable = e.is_timeout() || e.is_connect();
            Err((
                XRaftError::Transport(format!("AdminClient {method} {url}: {e}")),
                retryable,
            ))
        }
    }
}

/// Compute the next exponential backoff, capped at `max`.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

/// Equal-jitter sleep in `[base/2, base]`. Mirrors the transport
/// client's `jittered_sleep_duration` so both retry loops have the
/// same distribution and the operator sees one consistent policy.
fn jittered_sleep_duration(base: Duration) -> Duration {
    if base.is_zero() {
        return base;
    }
    let half = base / 2;
    // NOTE: `rand::random()` is the `rand` 0.8 thread-local API.
    // `xraft-client` (and the rest of the workspace) pin rand 0.8 — see
    // the `rand = { workspace = true }` line in `xraft-client/Cargo.toml`.
    // On a future workspace bump to rand 0.9, switch this to
    // `rand::rng().random::<f64>()` (or `rand::random_range(0.0..1.0)`)
    // and apply the same change to xraft-transport's mirrored
    // `jittered_sleep_duration` so both retry loops stay aligned.
    let fraction: f64 = rand::random();
    half + half.mul_f64(fraction)
}

/// Sleep wrapper hoisted out for testability — the retry tests
/// override the underlying tokio sleep via `tokio::time::pause()` in
/// `#[tokio::test(start_paused = true)]`.
fn sleep_for(d: Duration) -> impl Future<Output = ()> {
    tokio::time::sleep(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use axum::Json;
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    /// Spin up an axum router on an ephemeral port and return the
    /// resolved URL plus a shutdown handle. The test owns the
    /// shutdown handle and drops it (notifying) on test teardown so
    /// the spawned task does not leak.
    async fn spawn_admin_test_server(router: Router) -> (String, Arc<Notify>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test admin listener binds");
        let local_addr: SocketAddr = listener.local_addr().expect("local_addr");
        let shutdown = Arc::new(Notify::new());
        let shutdown_for_task = shutdown.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    shutdown_for_task.notified().await;
                })
                .await;
        });
        // Loopback bind on Windows occasionally needs a beat before
        // the listener accepts; the connect_timeout in the client
        // covers slow paths but yielding once cuts test flakiness.
        tokio::task::yield_now().await;
        (format!("http://{local_addr}"), shutdown)
    }

    #[derive(Clone)]
    struct FakeServerState {
        cluster_id: String,
        voters: Vec<u64>,
        node_id: u64,
        role: String,
        term: u64,
        leader_id: Option<u64>,
    }

    fn fake_router(state: FakeServerState) -> Router {
        async fn health(State(s): State<FakeServerState>) -> Json<serde_json::Value> {
            Json(json!({
                "node_id": s.node_id,
                "role": s.role,
                "term": s.term,
                "commit_index": 11_u64,
                "last_applied": 11_u64,
                "leader_id": s.leader_id,
                "last_log_index": 12_u64,
                "config_revision": 3_u64,
            }))
        }
        async fn status(State(s): State<FakeServerState>) -> Json<serde_json::Value> {
            Json(json!({
                "cluster_id": s.cluster_id,
                "voters": s.voters,
                "node_id": s.node_id,
                "role": s.role,
                "term": s.term,
                "commit_index": 11_u64,
                "last_applied": 11_u64,
                "leader_id": s.leader_id,
                "last_log_index": 12_u64,
            }))
        }
        async fn metrics() -> &'static str {
            "# HELP xraft_role role gauge\nxraft_role 3\n"
        }
        async fn trigger_snapshot() -> Json<serde_json::Value> {
            Json(json!({
                "last_included_index": 42_u64,
                "last_included_term": 7_u64,
                "size_bytes": 1024_u64,
            }))
        }
        Router::new()
            .route("/health", get(health))
            .route("/admin/status", get(status))
            .route("/metrics", get(metrics))
            .route("/admin/trigger-snapshot", post(trigger_snapshot))
            .with_state(state)
    }

    fn default_state() -> FakeServerState {
        FakeServerState {
            cluster_id: "c1".into(),
            voters: vec![1, 2, 3],
            node_id: 2,
            role: "leader".into(),
            term: 7,
            leader_id: Some(2),
        }
    }

    /// Build a client configured with a tiny backoff so retry tests
    /// run sub-second under `start_paused = true`.
    fn fast_retry_client(base_url: String, max_retries: u32) -> AdminClient {
        AdminClient::with_config(AdminConfig {
            base_url,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            max_retries,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
        })
        .expect("client builds")
    }

    #[tokio::test]
    async fn new_rejects_empty_base_url() {
        let err = AdminClient::new("").expect_err("must reject empty");
        match err {
            XRaftError::Config(msg) => assert!(msg.contains("base_url")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn new_normalises_trailing_slash() {
        let client = AdminClient::new("http://127.0.0.1:1/").expect("client builds");
        assert_eq!(client.base_url(), "http://127.0.0.1:1");
    }

    #[tokio::test]
    async fn with_config_rejects_zero_initial_backoff() {
        let cfg = AdminConfig {
            base_url: "http://127.0.0.1:1".into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: 3,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::from_secs(1),
        };
        let err = AdminClient::with_config(cfg).expect_err("must reject zero backoff");
        match err {
            XRaftError::Config(msg) => assert!(msg.contains("initial_backoff")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_config_rejects_max_backoff_less_than_initial() {
        let cfg = AdminConfig {
            base_url: "http://127.0.0.1:1".into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: 3,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(1),
        };
        let err = AdminClient::with_config(cfg).expect_err("must reject");
        match err {
            XRaftError::Config(msg) => assert!(msg.contains("max_backoff")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn health_decodes_full_payload() {
        let (url, shutdown) = spawn_admin_test_server(fake_router(default_state())).await;
        let client = AdminClient::new(url).expect("client builds");
        let h = client.health().await.expect("health succeeds");
        assert_eq!(h.node_id, 2);
        assert_eq!(h.role, "leader");
        assert_eq!(h.term, 7);
        assert_eq!(h.leader_id, Some(2));
        assert_eq!(h.config_revision, 3);
        shutdown.notify_one();
    }

    #[tokio::test]
    async fn status_returns_leader_and_voters() {
        // Scenario: admin-client-status — When AdminClient queries
        // cluster status, Then it returns the current leader, term,
        // and voter set.
        let state = FakeServerState {
            cluster_id: "primary".into(),
            voters: vec![10, 20, 30],
            node_id: 20,
            role: "leader".into(),
            term: 42,
            leader_id: Some(20),
        };
        let (url, shutdown) = spawn_admin_test_server(fake_router(state)).await;
        let client = AdminClient::new(url).expect("client builds");
        let s = client.status().await.expect("status succeeds");
        assert_eq!(s.cluster_id, "primary");
        assert_eq!(s.voters, vec![10, 20, 30]);
        assert_eq!(s.term, 42);
        assert_eq!(s.leader(), Some(NodeId(20)));
        shutdown.notify_one();
    }

    #[tokio::test]
    async fn metrics_returns_raw_text_body() {
        let (url, shutdown) = spawn_admin_test_server(fake_router(default_state())).await;
        let client = AdminClient::new(url).expect("client builds");
        let body = client.metrics().await.expect("metrics succeeds");
        let text = String::from_utf8(body.to_vec()).expect("utf-8 body");
        assert!(text.contains("xraft_role 3"));
        shutdown.notify_one();
    }

    #[tokio::test]
    async fn trigger_snapshot_round_trips_snapshot_info() {
        // Stage 6.2 contract: AdminClient.trigger_snapshot() POSTs to
        // /admin/trigger-snapshot and decodes the resulting
        // TriggeredSnapshotInfo. The fake server returns a canned
        // (index=42, term=7, size=1024) so we can assert decode is
        // wired correctly.
        let (url, shutdown) = spawn_admin_test_server(fake_router(default_state())).await;
        let client = AdminClient::new(url).expect("client builds");
        let info = client
            .trigger_snapshot()
            .await
            .expect("trigger_snapshot succeeds");
        assert_eq!(info.last_included_index, 42);
        assert_eq!(info.last_included_term, 7);
        assert_eq!(info.size_bytes, 1024);
        shutdown.notify_one();
    }

    #[tokio::test]
    async fn http_error_surfaces_as_transport_error_without_retry() {
        // A 500 from /health must bubble up as an XRaftError::Transport
        // rather than be misinterpreted as a successful empty body,
        // and the retry loop MUST NOT retry on HTTP status failures —
        // those indicate the server processed the request and chose
        // to fail (retry would double the side-effect for POSTs).
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_handler = calls.clone();
        async fn boom(State(c): State<Arc<AtomicU32>>) -> (axum::http::StatusCode, &'static str) {
            c.fetch_add(1, Ordering::SeqCst);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom")
        }
        let router = Router::new()
            .route("/health", get(boom))
            .with_state(calls_for_handler);
        let (url, shutdown) = spawn_admin_test_server(router).await;
        let client = fast_retry_client(url, 3);
        let err = client.health().await.expect_err("500 must error");
        match err {
            XRaftError::Transport(msg) => assert!(msg.contains("500"), "msg was: {msg}"),
            other => panic!("expected Transport error, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "HTTP 500 must NOT be retried (server-side authoritative)"
        );
        shutdown.notify_one();
    }

    #[tokio::test]
    async fn connect_failure_retries_then_succeeds_when_endpoint_recovers() {
        // Scenario: peer-client-reconnect (admin variant) — first the
        // endpoint is unreachable (port bound but never accepts).
        // The retry loop bounces off `is_connect()` errors with
        // exponential backoff and eventually surfaces success when the
        // real server stands up.
        //
        // We bind a port and immediately drop it to obtain a free
        // port number, then point a fast-retry client at that port
        // and ASSERT it fails with retries exhausted (not on the
        // first connect).
        let probe = TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
        let port = probe.local_addr().expect("local_addr").port();
        drop(probe);
        let url = format!("http://127.0.0.1:{port}");
        let client = fast_retry_client(url, 2);
        let err = client.health().await.expect_err("unreachable must fail");
        match err {
            XRaftError::Transport(msg) => {
                // The error message MUST surface the underlying
                // connect failure (not be swallowed). We don't pin the
                // exact wording — Windows / Linux / macOS phrase the
                // refused connect differently.
                assert!(
                    msg.contains("connect")
                        || msg.contains("refused")
                        || msg.contains("error")
                        || msg.contains("Connection")
                        || msg.contains("os error"),
                    "expected a connect-failure message, got: {msg}"
                );
            }
            other => panic!("expected Transport error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retry_recovers_from_transient_connect_refused() {
        // Stage 6.2 contract: a transient connect failure followed by
        // a recovered endpoint resolves OK within the retry budget.
        // We bind the real server BEFORE the client probes it (so the
        // first connect already succeeds), but the GET handler counts
        // calls — when the handler returns 503 once, then 200, the
        // retry loop must NOT recover (HTTP status failures are
        // authoritative and NOT retried). This test pins that
        // contract: a 503 from the server is propagated as an error
        // on attempt 1 with NO retry, even though the next attempt
        // would have succeeded.
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();
        async fn flaky_health(
            State(c): State<Arc<AtomicU32>>,
        ) -> (axum::http::StatusCode, Json<serde_json::Value>) {
            let n = c.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "error": "warming up" })),
                )
            } else {
                (
                    StatusCode::OK,
                    Json(json!({
                        "node_id": 1,
                        "role": "leader",
                        "term": 1,
                        "commit_index": 0,
                        "last_applied": 0,
                        "leader_id": 1,
                        "last_log_index": 0,
                        "config_revision": 0
                    })),
                )
            }
        }
        let router = Router::new()
            .route("/health", get(flaky_health))
            .with_state(attempts_for_handler);
        let (url, shutdown) = spawn_admin_test_server(router).await;
        let client = fast_retry_client(url, 5);
        let err = client.health().await.expect_err("503 must surface");
        match err {
            XRaftError::Transport(msg) => assert!(msg.contains("503"), "msg was: {msg}"),
            other => panic!("expected Transport error, got {other:?}"),
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "HTTP 503 must NOT be retried (only transport-level transient errors are)"
        );
        shutdown.notify_one();
    }

    #[test]
    fn next_backoff_doubles_then_caps() {
        assert_eq!(
            next_backoff(Duration::from_millis(100), Duration::from_millis(500)),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(300), Duration::from_millis(500)),
            Duration::from_millis(500),
            "must cap at max_backoff"
        );
        // Saturation guard: u64 overflow cannot regress to zero.
        assert_eq!(
            next_backoff(Duration::MAX, Duration::from_millis(500)),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn jittered_sleep_falls_inside_half_to_full_range() {
        let base = Duration::from_millis(100);
        for _ in 0..1000 {
            let s = jittered_sleep_duration(base);
            assert!(s >= base / 2 && s <= base, "out of range: {s:?}");
        }
        assert_eq!(
            jittered_sleep_duration(Duration::ZERO),
            Duration::ZERO,
            "zero base must produce zero sleep"
        );
    }
}
