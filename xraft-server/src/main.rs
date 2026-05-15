//! `xraft-server` binary entry point.
//!
//! Stage 2.2 wiring: parses the cluster TOML, opens a [`Server`] backed
//! by a [`FileHardStateStore`](xraft_storage::FileHardStateStore) whose
//! canonical file is `<config.data_dir>/quorum-state` (the file itself —
//! NOT a `quorum-state` subdirectory; see `architecture.md` §3.3), and
//! runs the tick loop until Ctrl-C (SIGINT/SIGBREAK on Windows) or a
//! fatal error from the consensus engine.
//!
//! The binary intentionally does **not** wire transport, log replication
//! or state-machine apply — those are later stages. Any engine action
//! Stage 2.2 cannot honour is surfaced as
//! [`ServerError::UnsupportedAction`] and terminates the process with a
//! non-zero exit code, so it is impossible to ship Stage 2.2 in a state
//! that pretends to support replication.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use xraft_core::config::ClusterConfig;
use xraft_server::{Server, ServerError};

#[derive(Debug, Parser)]
#[command(
    name = "xraft-server",
    version,
    about = "XRAFT consensus server (Stage 2.2 — persistent hard state wired)"
)]
struct Args {
    /// Path to the cluster TOML configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

fn main() -> ExitCode {
    init_tracing();
    let args = Args::parse();

    let config = match ClusterConfig::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!(path = %args.config.display(), error = %e, "failed to load configuration");
            return ExitCode::from(2);
        }
    };

    info!(
        node_id = config.node_id.0,
        cluster_id = %config.cluster_id,
        listen_addr = %config.listen_addr,
        data_dir = %config.data_dir.display(),
        hard_state_path = %Server::hard_state_path(&config).display(),
        "xraft-server starting (Stage 2.2 lifecycle)",
    );

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to construct tokio runtime");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(config)) {
        Ok(()) => {
            info!("xraft-server shutdown complete");
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!(error = %e, "xraft-server fatal");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: ClusterConfig) -> Result<(), ServerError> {
    let server = Server::open(config)?;
    let shutdown = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            // Falling back to never_shutdown on a missing signal handler
            // would let the process spin forever; better to log and let
            // the next tick path drive the loop. We swallow the error
            // because the main usecase (production Linux/Windows hosts)
            // always installs ctrl_c successfully.
            error!(error = %e, "failed to install ctrl_c handler; shutdown signal disabled");
            std::future::pending::<()>().await;
        }
    };
    server.run(shutdown).await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .ok();
}
