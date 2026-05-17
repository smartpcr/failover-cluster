//! HTTP admin endpoint: `/health` (JSON node status), `/metrics`
//! (Prometheus text-format scrape), and `/admin/status` (cluster-level
//! routing snapshot).
//!
//! Both routes are served from a single `axum::Router` mounted on a
//! dedicated listen address (see [`AdminConfig::listen_addr`]). The
//! admin server is **separate** from the gRPC consensus transport on
//! purpose: it can be exposed to a Prometheus scraper / liveness probe
//! without giving that probe a path into the consensus RPC surface.
//!
//! ## Endpoints
//!
//! - `GET /health` — JSON: `{ node_id, role, term, commit_index,
//!   last_applied, leader_id, last_log_index, config_revision }`.
//!   Always returns `200 OK` once the server is listening; the
//!   consumer infers liveness from the response body (e.g.
//!   `role != "follower" || leader_id != null`).
//! - `GET /admin/status` — Stage 6.2 cluster-level snapshot. Adds
//!   `cluster_id` and the configured `voters` set on top of the
//!   `/health` fields so routing-aware tooling can locate the
//!   leader without poking every voter in turn.
//! - `GET /metrics` — Prometheus text-exposition payload. Content
//!   type `application/openmetrics-text` per the OpenMetrics spec.
//! - `GET /` — minimal 200 banner so a default kube-style liveness
//!   probe at `/` does not 404.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tracing::{error, info};

use xraft_core::error::XRaftError;

use crate::driver::DriverHandle;
use crate::metrics::XRaftMetrics;
use crate::status::{NodeStatus, StatusPublisher, role_to_str};

/// Cluster-level metadata surfaced via `GET /admin/status`.
///
/// Constructed from [`xraft_core::config::ClusterConfig`] at server
/// bootstrap and wrapped in an `Arc` so it can be cheaply cloned into
/// the axum handler state.
///
/// Fields are deliberately a stable JSON projection: `cluster_id`
/// matches the canonical config field, `voters` is the ordered list
/// of voter `node_id`s. Operator tooling consumes this verbatim so
/// any future field additions MUST keep these two fields stable.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterInfo {
    /// Operator-assigned cluster identifier (mirrors
    /// [`xraft_core::config::ClusterConfig::cluster_id`]).
    pub cluster_id: String,
    /// Voter `node_id`s in roster order.
    pub voters: Vec<u64>,
}

impl ClusterInfo {
    /// Build a `ClusterInfo` from the canonical `ClusterConfig`.
    pub fn from_cluster_config(cluster: &xraft_core::config::ClusterConfig) -> Self {
        Self {
            cluster_id: cluster.cluster_id.clone(),
            voters: cluster.voters.iter().map(|v| v.node_id).collect(),
        }
    }
}

/// Wire-shape returned by `GET /admin/status`.
///
/// Composes the [`NodeStatus`] projection with the cluster-level
/// metadata from [`ClusterInfo`] (`cluster_id`, `voters`) as
/// **explicit struct fields**. This deliberately replaces the
/// earlier "serialize `NodeStatus` to a JSON map, then `obj.insert`
/// the cluster fields" pattern: if a future revision of
/// [`NodeStatus`] grows a `cluster_id` or `voters` field, the
/// collision now surfaces here as a compile error (duplicate struct
/// member) instead of silently overwriting the engine field at
/// runtime.
///
/// The wire field set MUST stay byte-compatible with
/// `xraft_client::admin::ClusterStatusResponse`, which is the
/// canonical operator-tooling decoder. Any new field belongs on
/// both sides in lock-step.
///
/// Field order matches the historical wire layout (status fields
/// first, then cluster fields) so a snapshot of an actual response
/// captured before this refactor still hashes identically.
#[derive(Debug, Serialize)]
struct AdminStatusResponse<'a> {
    node_id: u64,
    role: &'static str,
    term: u64,
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<u64>,
    last_log_index: u64,
    cluster_id: &'a str,
    voters: &'a [u64],
}

impl<'a> AdminStatusResponse<'a> {
    /// Build the response from a freshly-published [`NodeStatus`]
    /// and the bootstrap-time [`ClusterInfo`] snapshot.
    fn from_parts(status: &NodeStatus, cluster: &'a ClusterInfo) -> Self {
        Self {
            node_id: status.node_id,
            role: role_to_str(status.role),
            term: status.term,
            commit_index: status.commit_index,
            last_applied: status.last_applied,
            leader_id: status.leader_id,
            last_log_index: status.last_log_index,
            cluster_id: &cluster.cluster_id,
            voters: &cluster.voters,
        }
    }
}

/// Configuration for the admin HTTP listener.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// `host:port` to bind. Use `0.0.0.0:0` to bind an ephemeral
    /// port (test harnesses read the resolved port via
    /// [`AdminServer::local_addr`]).
    pub listen_addr: String,
}

impl AdminConfig {
    /// Construct a config with `listen_addr` parsed from a string
    /// the caller has already validated to be a `host:port` form.
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
        }
    }
}

/// Shared state passed to every axum handler.
#[derive(Clone)]
struct AdminState {
    metrics: Arc<XRaftMetrics>,
    status: Arc<StatusPublisher>,
    cluster_info: Arc<ClusterInfo>,
    /// Optional driver handle used by mutating admin endpoints
    /// (currently only `POST /admin/trigger-snapshot`). `None` when
    /// the router is constructed standalone in tests that don't
    /// exercise the snapshot path.
    driver: Option<DriverHandle>,
}

/// Build the [`Router`] for the admin endpoints over the supplied
/// metrics + status + cluster-info shared state. Exposed publicly
/// so integration tests can drive `Router::oneshot` against the
/// same routes the production binary serves.
///
/// The `POST /admin/trigger-snapshot` route is registered
/// unconditionally; when no [`DriverHandle`] is supplied (via
/// [`router_with_driver`]) the handler returns
/// `503 Service Unavailable` so callers receive a clear error
/// rather than a 404 they could misinterpret as a routing miss.
pub fn router(metrics: Arc<XRaftMetrics>, cluster_info: Arc<ClusterInfo>) -> Router {
    router_inner(metrics, cluster_info, None)
}

/// Variant of [`router`] that wires in a [`DriverHandle`] so the
/// `POST /admin/trigger-snapshot` route can drive an
/// operator-triggered snapshot through the driver loop. The driver
/// handle is cloneable; callers retain their own copy for graceful
/// shutdown sequencing.
pub fn router_with_driver(
    metrics: Arc<XRaftMetrics>,
    cluster_info: Arc<ClusterInfo>,
    driver: DriverHandle,
) -> Router {
    router_inner(metrics, cluster_info, Some(driver))
}

fn router_inner(
    metrics: Arc<XRaftMetrics>,
    cluster_info: Arc<ClusterInfo>,
    driver: Option<DriverHandle>,
) -> Router {
    let status = metrics.status_publisher();
    let state = AdminState {
        metrics,
        status,
        cluster_info,
        driver,
    };
    Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler))
        .route("/admin/status", get(admin_status_handler))
        .route("/admin/trigger-snapshot", post(trigger_snapshot_handler))
        .route("/admin/add-voter", post(add_voter_handler))
        .route("/admin/remove-voter", post(remove_voter_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

async fn root_handler() -> &'static str {
    "xraft-server admin endpoint — see /health, /admin/status and /metrics"
}

/// Handler for `POST /admin/add-voter` (Stage 7.2). Unconditionally
/// returns `501 Not Implemented` carrying the `XRaftError::Unsupported`
/// rejection message. Dynamic membership is **out of scope for v1**
/// (per `tech-spec.md` §2.7, `architecture.md` §5.5, and
/// `e2e-scenarios.md` Feature 12 — deferred to a future story
/// entirely; not a stretch goal within XRAFT). The response is
/// independent of whether a [`DriverHandle`] is wired into the admin
/// state so a stand-alone test router returns the same `501` rather
/// than `503 Service Unavailable`, matching the symmetric semantics
/// of [`crate::driver::DriverHandle::add_voter`] which also rejects
/// locally without touching the driver loop.
async fn add_voter_handler() -> Response {
    let body = serde_json::json!({
        "error": "AddVoter is out of scope for v1 — dynamic cluster membership \
                  is deferred to a future story entirely (per tech-spec.md \
                  §2.7, architecture.md §5.5, e2e-scenarios.md Feature 12). \
                  The voter set is static after first boot; restart the \
                  cluster with a different configuration to change membership.",
        "code": "UNSUPPORTED",
    });
    (StatusCode::NOT_IMPLEMENTED, Json(body)).into_response()
}

/// Handler for `POST /admin/remove-voter` (Stage 7.2). See
/// [`add_voter_handler`] for the rejection rationale; same status
/// and body shape so operator tooling can handle the pair uniformly.
async fn remove_voter_handler() -> Response {
    let body = serde_json::json!({
        "error": "RemoveVoter is out of scope for v1 — dynamic cluster membership \
                  is deferred to a future story entirely (per tech-spec.md \
                  §2.7, architecture.md §5.5, e2e-scenarios.md Feature 12). \
                  The voter set is static after first boot; restart the \
                  cluster with a different configuration to change membership.",
        "code": "UNSUPPORTED",
    });
    (StatusCode::NOT_IMPLEMENTED, Json(body)).into_response()
}

/// Handler for `GET /health`. Returns the latest [`NodeStatus`]
/// snapshot as JSON, augmented with the SIGHUP-driven
/// `config_revision` counter so operators can observe that a reload
/// actually applied (the counter strictly increases on each
/// successful reload).
async fn health_handler(State(state): State<AdminState>) -> Response {
    let snapshot = state.status.current().await;
    let mut body = serde_json::to_value(snapshot).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = body.as_object_mut() {
        obj.insert(
            "config_revision".to_string(),
            serde_json::Value::from(state.status.config_revision()),
        );
    }
    Json(body).into_response()
}

/// Handler for `GET /admin/status`. Returns the latest node-status
/// snapshot fused with the cluster-level routing metadata
/// ([`ClusterInfo`]). Routing-aware tooling (Stage 6.2 `AdminClient`)
/// hits this endpoint instead of `/health` when it needs to discover
/// the leader without iterating every voter.
async fn admin_status_handler(State(state): State<AdminState>) -> Response {
    let snapshot = state.status.current().await;
    let body = AdminStatusResponse::from_parts(&snapshot, &state.cluster_info);
    Json(body).into_response()
}

/// Handler for `POST /admin/trigger-snapshot` (Stage 6.2, evaluator
/// feedback iter 1 item 2). Drives a synchronous
/// operator-triggered snapshot through the driver loop and
/// surfaces the resulting [`crate::driver::TriggeredSnapshotInfo`] as JSON.
///
/// Status-code mapping (mirrored by
/// `xraft_client::admin::AdminClient::trigger_snapshot`):
/// - `200 OK` — snapshot taken; body is the JSON
///   `TriggeredSnapshotInfo`.
/// - `409 Conflict` — local node is not the leader; body is
///   `{"error": "...", "leader_hint": <node_id>?}` so the operator
///   tool can redirect.
/// - `409 Conflict` — a snapshot is already in flight (engine
///   `snapshot_in_flight = true`); operator backs off.
/// - `503 Service Unavailable` — driver is shutting down OR the
///   admin server was built without a driver handle (test-only
///   path); body carries the reason.
/// - `500 Internal Server Error` — storage / SM error during
///   snapshot persistence. Driver has halted; body carries the
///   reason.
async fn trigger_snapshot_handler(State(state): State<AdminState>) -> Response {
    let Some(driver) = state.driver.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "admin server built without a driver handle; trigger-snapshot unavailable"
            })),
        )
            .into_response();
    };
    match driver.trigger_snapshot().await {
        Ok(info) => (StatusCode::OK, Json(info)).into_response(),
        Err(XRaftError::NotLeader { leader_hint }) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "not leader; retry against the cluster leader",
                "leader_hint": leader_hint.map(|n| n.0),
            })),
        )
            .into_response(),
        Err(XRaftError::Config(msg)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
        Err(XRaftError::Shutdown) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "driver is shutting down; retry against another node",
            })),
        )
            .into_response(),
        Err(XRaftError::Transport(msg)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": format!("driver transport error: {msg}"),
            })),
        )
            .into_response(),
        Err(e) => {
            error!(target: "xraft_server::admin", error = %e, "trigger-snapshot failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("{e}") })),
            )
                .into_response()
        }
    }
}

/// Handler for `GET /metrics`. Renders the Prometheus registry as
/// text-exposition format with the OpenMetrics content-type header.
async fn metrics_handler(State(state): State<AdminState>) -> Response {
    match state.metrics.render().await {
        Ok(body) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(e) => {
            error!(target: "xraft_server::admin", error = %e, "metrics render failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("metrics render failed: {e}"),
            )
                .into_response()
        }
    }
}

/// Pre-bound admin listener, returned by [`AdminServer::bind`].
///
/// Holding the builder reserves the admin port WITHOUT spawning the
/// axum serve task. Callers (the Stage 6.1 [`crate::Server::start`]
/// path) use this to surface admin-bind failures synchronously
/// BEFORE any sibling task (gRPC, driver) is spawned — so a bind
/// error cannot leak already-spawned background tasks.
///
/// Drop the builder to release the listener without ever serving.
#[derive(Debug)]
pub struct AdminServerBuilder {
    /// Resolved local listen address (handles ephemeral `:0`).
    pub local_addr: SocketAddr,
    /// Bound listener; consumed by [`AdminServerBuilder::serve`].
    listener: TcpListener,
}

impl AdminServerBuilder {
    /// Inspect the resolved local listen address before deciding to
    /// `serve()`. Useful for logging or for tests that want to know
    /// the ephemeral port pre-spawn.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Consume the builder, spawn the axum serve task, and return a
    /// running [`AdminServer`] handle. Infallible: the bind already
    /// succeeded, so the only thing left is the spawn itself.
    pub fn serve(self, metrics: Arc<XRaftMetrics>, cluster_info: Arc<ClusterInfo>) -> AdminServer {
        self.serve_inner(metrics, cluster_info, None)
    }

    /// Like [`serve`](Self::serve) but wires in a [`DriverHandle`] so
    /// `POST /admin/trigger-snapshot` becomes operational. Production
    /// callers (`crate::Server::start_with_state_machine`) take this
    /// path; tests typically use [`serve`](Self::serve) when they do
    /// not need to exercise the snapshot trigger.
    pub fn serve_with_driver(
        self,
        metrics: Arc<XRaftMetrics>,
        cluster_info: Arc<ClusterInfo>,
        driver: DriverHandle,
    ) -> AdminServer {
        self.serve_inner(metrics, cluster_info, Some(driver))
    }

    fn serve_inner(
        self,
        metrics: Arc<XRaftMetrics>,
        cluster_info: Arc<ClusterInfo>,
        driver: Option<DriverHandle>,
    ) -> AdminServer {
        let Self {
            local_addr,
            listener,
        } = self;
        info!(target: "xraft_server::admin", addr = %local_addr, "admin HTTP server serving");

        let router = router_inner(metrics, cluster_info, driver);
        let shutdown = Arc::new(Notify::new());
        let shutdown_for_task = shutdown.clone();

        let serve_task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    shutdown_for_task.notified().await;
                })
                .await
        });

        AdminServer {
            local_addr,
            shutdown,
            serve_task,
        }
    }
}

/// Running admin HTTP server. Drop the handle (after [`shutdown`])
/// to release the listener.
pub struct AdminServer {
    /// Resolved listen address. Differs from [`AdminConfig::listen_addr`]
    /// when the config requested ephemeral port `0`.
    pub local_addr: SocketAddr,
    /// Shutdown notifier; cloned and passed to the serve task.
    shutdown: Arc<Notify>,
    /// Join handle for the spawned serve task.
    serve_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl AdminServer {
    /// Synchronously bind the admin listener WITHOUT spawning a
    /// serve task. Use when the caller (e.g.
    /// [`crate::Server::start_with_state_machine`]) needs to reserve
    /// the admin port before spawning sibling tasks, so a bind
    /// failure surfaces synchronously and cannot leak already-
    /// spawned background tasks.
    ///
    /// Returns an [`AdminServerBuilder`] holding the bound listener.
    /// Call [`AdminServerBuilder::serve`] to start the axum task, or
    /// drop the builder to release the listener.
    pub async fn bind(cfg: &AdminConfig) -> Result<AdminServerBuilder, XRaftError> {
        let addr: SocketAddr = cfg.listen_addr.parse().map_err(|e| {
            XRaftError::Config(format!(
                "admin listen_addr '{}' must parse as host:port: {e}",
                cfg.listen_addr
            ))
        })?;
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            XRaftError::Config(format!(
                "admin listener bind '{}' failed: {e}",
                cfg.listen_addr
            ))
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            XRaftError::Config(format!("admin listener local_addr query failed: {e}"))
        })?;
        info!(target: "xraft_server::admin", addr = %local_addr, "admin HTTP server bound");
        Ok(AdminServerBuilder {
            local_addr,
            listener,
        })
    }

    /// Convenience wrapper: [`AdminServer::bind`] followed
    /// immediately by [`AdminServerBuilder::serve`]. Used by tests
    /// and any caller that does not need synchronous bind/spawn
    /// separation.
    pub async fn start(
        cfg: &AdminConfig,
        metrics: Arc<XRaftMetrics>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Result<Self, XRaftError> {
        let builder = Self::bind(cfg).await?;
        Ok(builder.serve(metrics, cluster_info))
    }

    /// Signal the admin server to shut down. Idempotent.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
        // Stash a permit so a notifier that fires before the serve
        // task's `notified()` await still wakes the loop on first
        // poll.
        self.shutdown.notify_one();
    }

    /// Fail-stop the admin server by aborting its serve task at the
    /// next `.await` point. Used by [`crate::ServerHandle::abort`].
    pub fn abort(&self) {
        self.serve_task.abort();
    }

    /// Await graceful exit of the serve task.
    pub async fn join(self) -> Result<(), XRaftError> {
        match self.serve_task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(XRaftError::Transport(format!("admin serve error: {e}"))),
            Err(e) => Err(XRaftError::Transport(format!(
                "admin serve task join error: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::NodeStatus;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;
    use xraft_core::types::{NodeId, NodeRole};

    fn leader_status() -> NodeStatus {
        let mut s = NodeStatus::placeholder(NodeId(2));
        s.role = NodeRole::Leader;
        s.term = 5;
        s.commit_index = 42;
        s.leader_id = Some(2);
        s.last_applied = 41;
        s.last_log_index = 50;
        s
    }

    fn test_cluster_info() -> Arc<ClusterInfo> {
        Arc::new(ClusterInfo {
            cluster_id: "test-cluster".into(),
            voters: vec![1, 2, 3],
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn health_endpoint_returns_node_status_json() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(2)));
        metrics.publish_state(leader_status()).await;
        let app = router(metrics, test_cluster_info());

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["node_id"], 2);
        assert_eq!(v["role"], "leader");
        assert_eq!(v["term"], 5);
        assert_eq!(v["commit_index"], 42);
        assert_eq!(v["leader_id"], 2);
        assert_eq!(v["last_applied"], 41);
        assert_eq!(v["last_log_index"], 50);
        // The `config_revision` field must always be present (per
        // Stage 6.1 SIGHUP-applied contract). Value starts at 0
        // because no reload has been triggered.
        assert_eq!(v["config_revision"], 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn health_endpoint_surfaces_bumped_config_revision() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(4)));
        let status = metrics.status_publisher();
        // Simulate two successful SIGHUP-driven reloads.
        let r1 = status.bump_config_revision();
        let r2 = status.bump_config_revision();
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
        let app = router(metrics, test_cluster_info());

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["config_revision"], 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn admin_status_endpoint_returns_cluster_metadata_plus_status() {
        // Stage 6.2 contract: /admin/status must carry cluster_id +
        // voters so AdminClient.status() can identify the leader and
        // the voter roster in a single round-trip.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(2)));
        metrics.publish_state(leader_status()).await;
        let app = router(metrics, test_cluster_info());

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["cluster_id"], "test-cluster");
        assert_eq!(v["voters"], serde_json::json!([1, 2, 3]));
        // Node-status fields are still surfaced.
        assert_eq!(v["node_id"], 2);
        assert_eq!(v["role"], "leader");
        assert_eq!(v["term"], 5);
        assert_eq!(v["leader_id"], 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_endpoint_emits_openmetrics_text() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(3)));
        metrics.publish_state(leader_status()).await;
        metrics.record_appends(7);
        let app = router(metrics, test_cluster_info());

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap_or_default().to_string())
            .unwrap_or_default();
        assert!(
            ct.starts_with("application/openmetrics-text"),
            "content-type was: {ct}"
        );
        let bytes = to_bytes(resp.into_body(), 128 * 1024).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert!(body.contains("xraft_current_term 5"));
        assert!(body.contains("xraft_append_records_total 7"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn root_returns_banner_not_404() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_binds_ephemeral_port_and_shuts_down_gracefully() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let server = AdminServer::start(
            &AdminConfig::new("127.0.0.1:0"),
            metrics,
            test_cluster_info(),
        )
        .await
        .expect("admin start must succeed");
        let addr = server.local_addr;
        assert!(addr.port() > 0, "ephemeral port must be assigned");
        server.shutdown();
        server.join().await.expect("admin join must succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bind_then_serve_separates_listener_reservation_from_spawn() {
        // Reserve port, inspect local_addr, then spawn the serve
        // task. This is the path `Server::start_with_state_machine`
        // uses to surface bind failures synchronously BEFORE any
        // sibling task is spawned.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let builder = AdminServer::bind(&AdminConfig::new("127.0.0.1:0"))
            .await
            .expect("bind must succeed");
        let bound_addr = builder.local_addr();
        assert!(bound_addr.port() > 0, "ephemeral port must be assigned");
        let server = builder.serve(metrics, test_cluster_info());
        assert_eq!(server.local_addr, bound_addr, "serve preserves bound addr");
        server.shutdown();
        server.join().await.expect("admin join must succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bind_surfaces_port_in_use_error_synchronously() {
        // Hold a listener on a fixed port, then bind() must fail.
        // Proves the bind failure surfaces synchronously (so the
        // server-start path can refuse to spawn sibling tasks).
        let blocker = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("blocker bind");
        let blocked_addr = blocker.local_addr().expect("local_addr").to_string();
        let err = AdminServer::bind(&AdminConfig::new(blocked_addr))
            .await
            .expect_err("bind to in-use port must fail");
        match err {
            XRaftError::Config(msg) => assert!(
                msg.contains("bind"),
                "error must mention bind failure: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trigger_snapshot_without_driver_returns_service_unavailable() {
        // Stage 6.2: the `POST /admin/trigger-snapshot` route must
        // be registered even when the router is built without a
        // driver handle (test-only fast path), and the handler must
        // surface a 503 with a clear error body so callers do not
        // mis-interpret a missing-driver scenario as a 404 routing
        // miss.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/trigger-snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let err = v["error"].as_str().expect("error field must be string");
        assert!(
            err.contains("trigger-snapshot unavailable"),
            "error body must explain the missing-driver path: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trigger_snapshot_route_only_responds_to_post() {
        // Stage 6.2 wire contract: the trigger-snapshot endpoint is
        // POST-only (mutating operation). A GET against the same
        // path must yield 405 Method Not Allowed so an operator who
        // mistakes it for a read endpoint gets a clear error.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/trigger-snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_voter_endpoint_returns_501_with_unsupported_body() {
        // Stage 7.2: dynamic membership is out of scope for v1. The
        // admin endpoint must surface this via 501 Not Implemented
        // (not 503 — the rejection is intrinsic to v1, not a
        // missing-driver runtime gap) with a JSON body that carries
        // the explicit "UNSUPPORTED" code and an operator-readable
        // explanation referencing the scoping decision.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/add-voter")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["code"], "UNSUPPORTED");
        let err = v["error"].as_str().expect("error field must be string");
        assert!(
            err.contains("out of scope for v1"),
            "error body must explain v1 scoping: {err}"
        );
        assert!(
            err.contains("AddVoter"),
            "error body must name the rejected operation: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_voter_endpoint_returns_501_with_unsupported_body() {
        // Stage 7.2: symmetric to add-voter — see that test for the
        // status-code rationale.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/remove-voter")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["code"], "UNSUPPORTED");
        let err = v["error"].as_str().expect("error field must be string");
        assert!(
            err.contains("RemoveVoter"),
            "error body must name the rejected operation: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_voter_route_only_responds_to_post() {
        // Membership-mutation endpoints must be POST-only so a
        // misdirected GET cannot accidentally probe the route as a
        // read.
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/add-voter")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_voter_route_only_responds_to_post() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let app = router(metrics, test_cluster_info());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/remove-voter")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
