//! XRAFT server binary entry point.
//!
//! Stage 6.1 wires up the production lifecycle:
//!
//! 1. Parse CLI args (`--config`, `--node-id`, `--admin-listen`).
//! 2. Initialise structured (JSON) tracing — log level controlled
//!    via `RUST_LOG` env var (default `info`). The `EnvFilter` is
//!    wrapped in a [`tracing_subscriber::reload::Layer`] so the
//!    SIGHUP handler can swap it at runtime without restarting.
//! 3. Load + validate config via the [`xraft_core::config::NodeConfig`]
//!    path (Stage 1.2 schema): read the TOML, apply `XRAFT_*` env
//!    overrides, then run cluster-level **plus** node-membership
//!    validation (`node_id` must be in voters or observers).
//! 4. Optionally override the `node_id` from the CLI; this re-runs
//!    `NodeConfig::validate()` so the override cannot silently
//!    bypass the membership check.
//! 5. Start the [`Server`] which assembles storage, transport,
//!    driver, and the admin HTTP endpoint.
//! 6. Wait for a shutdown signal:
//!    - Unix: `SIGTERM` / `SIGINT` → graceful exit;
//!      `SIGHUP` → live reload (re-read config + re-parse
//!      `RUST_LOG`; observable runtime change is the log filter
//!      swap + the new validated [`NodeConfig`] cached in shared
//!      state; non-hot-reloadable fields like `listen_addr`,
//!      `voters`, `data_dir`, and `admin_listen_addr` are
//!      logged-and-ignored).
//!    - Windows: `Ctrl+C` → graceful exit.
//! 7. Trigger `ServerHandle::shutdown` + `join` and exit with
//!    code 0 on clean shutdown, non-zero on failure.

#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
#[cfg(unix)]
use std::sync::Arc;

use clap::Parser;
#[cfg(unix)]
use tokio::sync::RwLock;
#[cfg(unix)]
use tracing::warn;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, fmt, reload};

use xraft_core::config::NodeConfig;
#[cfg(unix)]
use xraft_server::ServerHandle;
use xraft_server::{Server, ServerConfig};

/// Default `RUST_LOG` filter when the env var is unset.
const DEFAULT_LOG_FILTER: &str = "info,xraft_core=info,xraft_server=info,xraft_transport=info";

/// CLI arguments for the `xraft-server` binary.
///
/// `--config` is required; everything else is optional. The
/// `clap` derive macro generates `--help` from the doc comments
/// here so operators get a usable usage message via
/// `xraft-server --help`.
#[derive(Debug, Parser)]
#[command(
    name = "xraft-server",
    version,
    about = "XRAFT consensus server",
    long_about = "Run a single XRAFT consensus node. \
                  Reads cluster + node config from a TOML file, \
                  initialises file-backed storage under the configured \
                  data_dir, starts the gRPC consensus transport, and \
                  serves /health + /metrics over an HTTP admin endpoint."
)]
struct Cli {
    /// Path to the TOML config file (required).
    #[arg(long, short = 'c', value_name = "PATH")]
    config: PathBuf,

    /// Override the `node_id` from the config file. Useful when
    /// running multiple nodes from the same config template.
    /// Membership validation re-runs after the override so the
    /// CLI cannot silently bypass `node_id in voters | observers`.
    #[arg(long, value_name = "ID")]
    node_id: Option<u64>,

    /// Override the admin HTTP listen address (`host:port`).
    /// Defaults to [`xraft_server::server::DEFAULT_ADMIN_LISTEN_ADDR`]
    /// when neither this flag nor the config supplies one.
    #[arg(long, value_name = "HOST:PORT")]
    admin_listen: Option<String>,
}

fn main() -> ExitCode {
    // tokio runtime is built manually so we can return ExitCode
    // distinct from the runtime's default panic-on-error.
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to build tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };

    match rt.block_on(async_main()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(target: "xraft_server::main", error = %e, "server exited with error");
            ExitCode::from(1)
        }
    }
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let reload_handle = init_tracing();

    // Validate the config path early so the operator gets a
    // clean error instead of a panic deep in the loader.
    if !cli.config.exists() {
        return Err(format!("--config path '{}' does not exist", cli.config.display()).into());
    }

    // Stage 1.2 NodeConfig path: load → env-overrides → validate
    // (cluster + membership). The CLI `--node-id` override
    // re-runs validation so it cannot bypass membership rules.
    let mut node_cfg = NodeConfig::load(&cli.config)?;
    if let Some(id_override) = cli.node_id {
        info!(
            target: "xraft_server::main",
            original = node_cfg.cluster.node_id.0,
            override_value = id_override,
            "applying --node-id CLI override"
        );
        node_cfg.cluster.node_id = xraft_core::types::NodeId(id_override);
        node_cfg.validate()?;
    }

    let mut cfg = ServerConfig::from_node_config(node_cfg.clone())?;
    if let Some(addr) = cli.admin_listen {
        cfg.admin_listen_addr = Some(addr);
    }

    info!(
        target: "xraft_server::main",
        node_id = cfg.cluster.node_id.0,
        cluster_id = %cfg.cluster.cluster_id,
        listen_addr = %cfg.cluster.listen_addr,
        admin_listen = %cfg.admin_addr(),
        data_dir = %cfg.cluster.data_dir.display(),
        "starting xraft-server"
    );

    let handle = Server::start(cfg).await?;
    info!(
        target: "xraft_server::main",
        grpc = %handle.grpc_listen_addr,
        admin = %handle.admin_addr,
        "xraft-server ready"
    );

    // Shared state for SIGHUP reload: the latest validated
    // NodeConfig is observable to any code that holds a clone of
    // this Arc. The reload handler swaps the contents under
    // write-lock so concurrent readers see a consistent snapshot.
    #[cfg(unix)]
    let config_state = Arc::new(RwLock::new(node_cfg));

    #[cfg(unix)]
    wait_for_shutdown_signal(cli.config.clone(), config_state, reload_handle, &handle).await;

    #[cfg(not(unix))]
    {
        let _ = reload_handle;
        wait_for_shutdown_signal().await;
    }

    info!(target: "xraft_server::main", "shutdown signal received — draining");
    handle.shutdown();
    handle.join().await?;
    info!(target: "xraft_server::main", "xraft-server exited cleanly");
    Ok(())
}

/// `tracing_subscriber::reload` handle type alias.
///
/// `Registry` is the concrete subscriber so the handle can be
/// stored in plain code without leaking generic parameters.
type LogFilterReloadHandle = reload::Handle<EnvFilter, Registry>;

/// Initialise the global `tracing` subscriber with JSON output
/// to stdout. Filter is read from `RUST_LOG`, falling back to
/// [`DEFAULT_LOG_FILTER`] when unset.
///
/// Returns a [`LogFilterReloadHandle`] (`None` only when the
/// global subscriber was already installed by a test harness)
/// that the SIGHUP handler uses to swap the filter at runtime.
fn init_tracing() -> Option<LogFilterReloadHandle> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    let json_layer = fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false);
    let result = tracing_subscriber::registry()
        .with(filter_layer)
        .with(json_layer)
        .try_init();
    if let Err(e) = result {
        // Already initialised by a test harness — not fatal.
        eprintln!("warning: failed to install tracing subscriber: {e}");
        return None;
    }
    Some(reload_handle)
}

/// Re-read the config file at `path`, re-validate, and swap the
/// latest [`NodeConfig`] into shared state. Also refreshes the
/// log filter from the current `RUST_LOG` env var via
/// `reload_handle`, **and** applies the new `tick_interval` live
/// to the running [`Driver`] via [`DriverHandle::reload_tick_interval`].
/// Increments the [`StatusPublisher::config_revision`] counter so
/// the bump is observable via `/health` — the operator's proof
/// the SIGHUP actually applied (and not just acknowledged).
///
/// Reload errors are logged but never propagate — a botched
/// SIGHUP must NOT crash a healthy server.
///
/// Stage 6.1 reload semantics:
/// - HOT-RELOADABLE:
///   - `RUST_LOG` env var → log filter is swapped.
///   - `tick_interval_ms` → driver `tokio::time::interval` rebuilt;
///     the next tick fires at the new cadence (no restart).
///   - The full NodeConfig is re-parsed + cached in shared state.
///   - `config_revision` counter is bumped ONLY when every
///     engine-critical hot-reload step succeeds (currently:
///     the driver tick-interval refresh). If the driver send
///     fails — e.g. the driver task has already shut down —
///     `reload_config` returns early without bumping, so
///     operators watching `/health` can distinguish a partial
///     apply (cached snapshot updated, but tick cadence still
///     stale) from a fully-successful reload.
/// - NOT HOT-RELOADABLE: `listen_addr`, `voters`, `data_dir`,
///   `node_id`, `cluster_id`, `admin_listen_addr`. Changing these
///   requires a restart; the reload handler logs which fields differ
///   so operators know a restart is needed.
#[cfg(unix)]
async fn reload_config(
    path: &Path,
    state: &Arc<RwLock<NodeConfig>>,
    reload_handle: &Option<LogFilterReloadHandle>,
    server: &ServerHandle,
) {
    let new_cfg = match NodeConfig::load(path) {
        Ok(c) => c,
        Err(e) => {
            error!(
                target: "xraft_server::main",
                path = %path.display(),
                error = %e,
                "SIGHUP reload: failed to load config; keeping previous values"
            );
            return;
        }
    };

    // Diff against the previous snapshot so operators can see
    // which non-hot-reloadable fields they tried to change.
    {
        let prev = state.read().await;
        let prev_c = &prev.cluster;
        let new_c = &new_cfg.cluster;
        if prev_c.node_id != new_c.node_id {
            warn!(
                target: "xraft_server::main",
                old = prev_c.node_id.0,
                new = new_c.node_id.0,
                "SIGHUP reload: node_id is NOT hot-reloadable — restart required"
            );
        }
        if prev_c.cluster_id != new_c.cluster_id {
            warn!(
                target: "xraft_server::main",
                old = %prev_c.cluster_id,
                new = %new_c.cluster_id,
                "SIGHUP reload: cluster_id is NOT hot-reloadable — restart required"
            );
        }
        if prev_c.listen_addr != new_c.listen_addr {
            warn!(
                target: "xraft_server::main",
                old = %prev_c.listen_addr,
                new = %new_c.listen_addr,
                "SIGHUP reload: listen_addr is NOT hot-reloadable — restart required"
            );
        }
        if prev_c.data_dir != new_c.data_dir {
            warn!(
                target: "xraft_server::main",
                old = %prev_c.data_dir.display(),
                new = %new_c.data_dir.display(),
                "SIGHUP reload: data_dir is NOT hot-reloadable — restart required"
            );
        }
        if prev_c.voters != new_c.voters {
            warn!(
                target: "xraft_server::main",
                "SIGHUP reload: voters set is NOT hot-reloadable — restart required (membership changes go through Stage 5.x AddNode/RemoveNode RPC)"
            );
        }
        if prev.admin_listen_addr != new_cfg.admin_listen_addr {
            warn!(
                target: "xraft_server::main",
                old = ?prev.admin_listen_addr,
                new = ?new_cfg.admin_listen_addr,
                "SIGHUP reload: admin_listen_addr is NOT hot-reloadable — restart required (the admin HTTP listener is bound at startup)"
            );
        }
    }

    // Swap the new config in. Observable change: any code path
    // that reads from `state` after this point sees the new
    // values. The engine itself is not re-wired in Stage 6.1.
    let new_tick_interval = std::time::Duration::from_millis(new_cfg.cluster.tick_interval_ms);
    {
        let mut guard = state.write().await;
        *guard = new_cfg;
    }

    // Apply the new tick interval live to the running driver.
    // This is the engine-visible portion of the reload: the next
    // `Driver::run` tick honours the new cadence without a
    // restart. The driver enqueues an internal `DriverEvent` and
    // rebuilds `self.tick = interval(new)` inside its select!.
    //
    // If the send fails the driver has already shut down. We
    // suppress the `config_revision` bump in that case (early
    // return below) so operators watching `/health` do NOT
    // mistake a partial apply — cached `NodeConfig` snapshot
    // updated, but tick cadence still stale — for a fully-
    // successful reload.
    let driver = server.driver_handle();
    if let Err(e) = driver.reload_tick_interval(new_tick_interval).await {
        warn!(
            target: "xraft_server::main",
            error = %e,
            "SIGHUP reload: driver tick-interval refresh failed (driver shutting down?); \
             config_revision NOT bumped — /health continues reporting the previous revision \
             so operators can distinguish a partial apply from a full one"
        );
        // Engine-critical step failed: skip the
        // `config_revision` bump (and the log-filter refresh
        // below) so the operator-visible "fully applied"
        // signal stays in sync with reality. The cached
        // `NodeConfig` snapshot swapped in above is left
        // alone; no engine code reads it outside this reload
        // path in Stage 6.1, and the next successful SIGHUP
        // will overwrite it.
        return;
    }
    info!(
        target: "xraft_server::main",
        tick_ms = new_tick_interval.as_millis(),
        "SIGHUP reload: driver tick interval refreshed live"
    );

    // Refresh log filter from the current `RUST_LOG`. This is
    // the most operationally useful hot-reload because it lets
    // operators bump log verbosity on a live server without
    // disrupting traffic.
    if let Some(handle) = reload_handle {
        let new_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
        let filter_repr = format!("{new_filter}");
        if let Err(e) = handle.reload(new_filter) {
            error!(
                target: "xraft_server::main",
                error = %e,
                "SIGHUP reload: failed to swap log filter"
            );
        } else {
            info!(
                target: "xraft_server::main",
                filter = %filter_repr,
                "SIGHUP reload: log filter refreshed from RUST_LOG"
            );
        }
    }

    info!(
        target: "xraft_server::main",
        path = %path.display(),
        "SIGHUP reload: config re-read + validated; cached snapshot updated"
    );

    // Bump the SIGHUP-applied counter so the change is observable
    // via /health. This MUST happen after the driver / log-filter
    // applies above — bumping first would let operators see a
    // revision change for a botched reload.
    let revision = server.status().bump_config_revision();
    info!(
        target: "xraft_server::main",
        config_revision = revision,
        "SIGHUP reload: config_revision bumped (observable via /health)"
    );
}

/// Block until a graceful-shutdown signal arrives.
///
/// On Unix this races `SIGTERM`, `SIGINT`, and `SIGHUP`.
/// `SIGTERM` / `SIGINT` return so the caller can drain and exit.
/// `SIGHUP` triggers an in-place [`reload_config`] and loops back
/// to wait for the next signal — the server keeps running.
#[cfg(unix)]
async fn wait_for_shutdown_signal(
    config_path: PathBuf,
    config_state: Arc<RwLock<NodeConfig>>,
    reload_handle: Option<LogFilterReloadHandle>,
    server: &ServerHandle,
) {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            error!(target: "xraft_server::main", error = %e, "failed to install SIGTERM handler");
            return;
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            error!(target: "xraft_server::main", error = %e, "failed to install SIGINT handler");
            return;
        }
    };
    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            error!(target: "xraft_server::main", error = %e, "failed to install SIGHUP handler");
            return;
        }
    };

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!(target: "xraft_server::main", signal = "SIGTERM", "shutdown signal");
                break;
            }
            _ = sigint.recv() => {
                info!(target: "xraft_server::main", signal = "SIGINT", "shutdown signal");
                break;
            }
            _ = sighup.recv() => {
                info!(
                    target: "xraft_server::main",
                    signal = "SIGHUP",
                    "reload requested — re-reading config and refreshing log filter"
                );
                reload_config(&config_path, &config_state, &reload_handle, server).await;
                continue;
            }
        }
    }
}

/// Windows (and any non-Unix target): race `Ctrl-C` and (on
/// Windows specifically) `Ctrl-Break` so console-attached child
/// processes started with `CREATE_NEW_PROCESS_GROUP` can be shut
/// down cleanly via `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT,
/// pid)`. `Ctrl-C` does NOT propagate to a child in a new
/// process group, so the integration test harness MUST use
/// `Ctrl-Break` — the binary therefore must subscribe to it.
#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_c};
        let mut c = match ctrl_c() {
            Ok(s) => s,
            Err(e) => {
                error!(target: "xraft_server::main", error = %e, "ctrl_c handler install failed");
                return;
            }
        };
        let mut b = match ctrl_break() {
            Ok(s) => s,
            Err(e) => {
                error!(target: "xraft_server::main", error = %e, "ctrl_break handler install failed");
                return;
            }
        };
        tokio::select! {
            _ = c.recv() => {
                info!(target: "xraft_server::main", signal = "ctrl_c", "shutdown signal");
            }
            _ = b.recv() => {
                info!(target: "xraft_server::main", signal = "ctrl_break", "shutdown signal");
            }
        }
    }
    #[cfg(not(windows))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!(target: "xraft_server::main", error = %e, "ctrl_c handler failed");
        }
        info!(target: "xraft_server::main", signal = "ctrl_c", "shutdown signal");
    }
}
