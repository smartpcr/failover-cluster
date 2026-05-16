//! HTTP admin endpoint: `/health` (JSON node status) and `/metrics`
//! (Prometheus text-format scrape).
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
//!   last_applied, leader_id, last_log_index }`. Always returns
//!   `200 OK` once the server is listening; the consumer infers
//!   liveness from the response body (e.g. `role != "follower" ||
//!   leader_id != null`).
//! - `GET /metrics` — Prometheus text-exposition payload. Content
//!   type `application/openmetrics-text` per the OpenMetrics spec.
//! - `GET /` — minimal 200 banner so a default kube-style liveness
//!   probe at `/` does not 404.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tracing::{error, info};

use xraft_core::error::XRaftError;

use crate::metrics::XRaftMetrics;
use crate::status::StatusPublisher;

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
}

/// Build the [`Router`] for the admin endpoints over the supplied
/// metrics + status shared state. Exposed publicly so integration
/// tests can drive `Router::oneshot` against the same routes the
/// production binary serves.
pub fn router(metrics: Arc<XRaftMetrics>) -> Router {
    let status = metrics.status_publisher();
    let state = AdminState { metrics, status };
    Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

async fn root_handler() -> &'static str {
    "xraft-server admin endpoint — see /health and /metrics"
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
    pub fn serve(self, metrics: Arc<XRaftMetrics>) -> AdminServer {
        let Self {
            local_addr,
            listener,
        } = self;
        info!(target: "xraft_server::admin", addr = %local_addr, "admin HTTP server serving");

        let router = router(metrics);
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
    pub async fn start(cfg: &AdminConfig, metrics: Arc<XRaftMetrics>) -> Result<Self, XRaftError> {
        let builder = Self::bind(cfg).await?;
        Ok(builder.serve(metrics))
    }

    /// Signal the admin server to shut down. Idempotent.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
        // Stash a permit so a notifier that fires before the serve
        // task's `notified()` await still wakes the loop on first
        // poll.
        self.shutdown.notify_one();
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

    #[tokio::test(flavor = "current_thread")]
    async fn health_endpoint_returns_node_status_json() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(2)));
        metrics.publish_state(leader_status()).await;
        let app = router(metrics);

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
        let app = router(metrics);

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
    async fn metrics_endpoint_emits_openmetrics_text() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(3)));
        metrics.publish_state(leader_status()).await;
        metrics.record_appends(7);
        let app = router(metrics);

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
        let app = router(metrics);
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_binds_ephemeral_port_and_shuts_down_gracefully() {
        let metrics = XRaftMetrics::shared(NodeStatus::placeholder(NodeId(1)));
        let server = AdminServer::start(&AdminConfig::new("127.0.0.1:0"), metrics)
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
        let server = builder.serve(metrics);
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
}
