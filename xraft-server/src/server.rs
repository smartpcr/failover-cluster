//! Server lifecycle: ties [`Driver`] to a tokio-driven tick loop and
//! wires the production startup path called from the `xraft-server`
//! binary.
//!
//! # Stage 2.2 — Persistent Raft State (lifecycle wiring)
//!
//! The persistent-state primitives — [`HardStateStore`], the canonical
//! [`HardState`] envelope, [`RaftNode::new_with_initial_hard_state`] and
//! [`Driver::open_file`] — were delivered in earlier iterations. This
//! module is what makes them **observable from the production binary**:
//!
//! 1. [`Server::open`] (the binary's entry point) calls
//!    [`Driver::open_file`] against `config.data_dir`, which loads any
//!    persisted [`HardState`] from `<config.data_dir>/quorum-state`
//!    (the file, per `architecture.md` §3.3 — not a same-named
//!    subdirectory) and constructs the [`RaftNode`] at the recovered
//!    term + vote.
//! 2. [`Server::step`] forwards inputs through [`Driver::step`], so the
//!    "persist BEFORE any RPC reply leaves the box" ordering is enforced
//!    on every request the server processes.
//! 3. [`Server::run`] drives [`Input::Tick`] at the configured cadence
//!    and exits cleanly when the supplied shutdown future resolves.
//! 4. Any [`Action`] the engine emits that Stage 2.2 cannot honour
//!    (durable log append, state-machine apply, snapshot, fetch service)
//!    is surfaced as [`ServerError::UnsupportedAction`] rather than
//!    silently dropped — Stage 2.2 must not pretend to have wired up
//!    log replication or state-machine apply.
//!
//! Higher-stage workstreams replace `step`'s action dispatcher with
//! real transport / log / state-machine plumbing without changing the
//! Stage 2.2 persistence guarantee.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{debug, error, info};

use xraft_core::config::ClusterConfig;
use xraft_core::message::{Action, Input};
use xraft_core::storage::HardStateStore;
use xraft_storage::FileHardStateStore;

use crate::driver::{Driver, DriverError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Filename of the canonical hard-state file inside `config.data_dir`.
///
/// Matches the on-disk layout pinned by `architecture.md` §3.3:
///
/// ```text
/// <data_dir>/
/// ├── quorum-state                              # HardState (JSON, atomic write)
/// ├── metadata/...
/// └── ...
/// ```
///
/// `quorum-state` is **the file**, not a subdirectory. This name is
/// also the canonical filename used inside
/// [`xraft_storage::FileHardStateStore`], so calling
/// `FileHardStateStore::open(<data_dir>)` produces
/// `<data_dir>/quorum-state` directly — no nesting. The constant is
/// re-exported so operator tooling (and the binary's startup log) can
/// reference the same name without re-typing the literal.
pub const HARD_STATE_FILE_NAME: &str = "quorum-state";

/// **Deprecated** alias for [`HARD_STATE_FILE_NAME`].
///
/// The previous iteration of this module exported the same string under
/// the misleading name `HARD_STATE_DIR_NAME` (it identifies a *file*,
/// not a directory). The constant was renamed to
/// [`HARD_STATE_FILE_NAME`] in iter 4 to make that contract obvious;
/// this `pub use`-style alias is retained so any downstream caller that
/// imported the old name still compiles. The two constants have
/// **identical values** (`"quorum-state"`), so behaviour is unchanged
/// on either side of the rename.
///
/// New code should use [`HARD_STATE_FILE_NAME`]. The deprecation
/// attribute will surface an `unused_imports`-style warning at the
/// call site to nudge migration without breaking the build.
#[deprecated(
    since = "0.2.0",
    note = "renamed to `HARD_STATE_FILE_NAME` because it identifies a file, not a directory; \
            this alias resolves to the identical string `\"quorum-state\"` and will be removed \
            in a future major version"
)]
pub const HARD_STATE_DIR_NAME: &str = HARD_STATE_FILE_NAME;

// ---------------------------------------------------------------------------
// ServerError
// ---------------------------------------------------------------------------

/// Errors returned by [`Server`].
///
/// Distinguishes driver / storage errors (recoverable iff the on-disk
/// state is intact) from configuration errors (operator-fix needed) and
/// engine actions that this stage cannot dispatch.
#[derive(Debug, Error)]
pub enum ServerError {
    /// A [`Driver`] operation failed (load, persist, or post-poison
    /// rejection). Wraps the canonical [`DriverError`] without flattening
    /// so callers can pattern-match on the underlying cause.
    #[error("driver error: {0}")]
    Driver(#[from] DriverError),

    /// The engine produced an [`Action`] that this stage's server has
    /// no driver for (e.g. [`Action::AppendEntries`] for log replication
    /// or [`Action::ApplyToStateMachine`] for state-machine apply). The
    /// kind name is kept as a `&'static str` to make grep-based audits
    /// of "what does Stage 2.2 not wire?" trivial. Higher stages replace
    /// the default classifier so these kinds become routable instead.
    #[error(
        "engine emitted unsupported action ({0}); Stage 2.2 server has no driver for this kind"
    )]
    UnsupportedAction(&'static str),

    /// A previous call to [`Server::step`] surfaced an
    /// [`ServerError::UnsupportedAction`] (or the
    /// driver-contract-violation `PersistHardState` leak) and the
    /// server has marked itself **terminal** to stop callers from
    /// ignoring the error and continuing to step an engine whose
    /// durable action was never dispatched.
    ///
    /// Recovery requires constructing a fresh [`Server`]. The contained
    /// `&'static str` is the action kind that originally tripped the
    /// stop so audits can correlate "stopped because" entries with the
    /// originally-rejected action.
    #[error(
        "server stopped after unsupported action ({0}); \
         construct a fresh Server (Stage 2.2 has no driver for this action kind)"
    )]
    Stopped(&'static str),
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// The Stage 2.2 server. Owns a [`Driver`] (and through it the
/// [`RaftNode`](xraft_core::node::RaftNode) + [`HardStateStore`]) for
/// its lifetime.
///
/// Generic over the store with a default of [`FileHardStateStore`] so
/// production callers get the disk-backed behaviour without naming
/// the type parameter, while crate-internal tests can substitute an
/// in-memory or failure-injecting store via the `#[cfg(test)]`
/// [`Server::from_driver`] constructor.
pub struct Server<S: HardStateStore = FileHardStateStore> {
    driver: Driver<S>,
    /// Cached so [`Server::run`] does not have to reach back into the
    /// engine's config on every tick.
    tick_interval_ms: u64,
    /// `Some(kind)` after [`Server::step`] surfaces an
    /// [`ServerError::UnsupportedAction`] (or the driver-contract-violation
    /// `PersistHardState` leak from [`Driver::step`]). Subsequent calls
    /// to [`Server::step`] short-circuit with [`ServerError::Stopped`]
    /// so callers that catch `UnsupportedAction` cannot keep stepping
    /// an engine whose previous durable action was never dispatched.
    /// Recovery requires constructing a fresh [`Server`].
    stopped: Option<&'static str>,
}

impl<S: HardStateStore> std::fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("driver", &self.driver)
            .field("tick_interval_ms", &self.tick_interval_ms)
            .field("stopped", &self.stopped)
            .finish()
    }
}

// -- File-backed (production) constructors ----------------------------------

impl Server<FileHardStateStore> {
    /// Open a server backed by [`FileHardStateStore`] with the canonical
    /// hard-state file at `<config.data_dir>/quorum-state`.
    ///
    /// This is the entry point the `xraft-server` binary calls. The
    /// directory passed to [`FileHardStateStore::open`] is
    /// `config.data_dir` itself — **not** a `quorum-state` subdirectory —
    /// so the layout matches `architecture.md` §3.3 exactly:
    /// `<data_dir>/quorum-state` is the file, sitting alongside future
    /// `<data_dir>/metadata/` (log) and `<data_dir>/snapshots/` trees.
    ///
    /// It physically loads any previously persisted [`HardState`] from
    /// disk so the engine boots at the recovered (term, voted_for),
    /// satisfying the Stage 2.2 acceptance scenario "process restart
    /// must not vote twice in the same term."
    pub fn open(config: ClusterConfig) -> Result<Self, ServerError> {
        let dir = config.data_dir.clone();
        Self::open_in_dir(config, dir)
    }

    /// Open a server with an explicit hard-state directory. Used by
    /// tests that need an isolated [`tempfile::TempDir`] root and by
    /// operators who want to deviate from the default layout (e.g.
    /// running multiple test clusters under the same `data_dir`).
    ///
    /// The canonical file written/read by [`FileHardStateStore`] is
    /// `<dir>/quorum-state` — `dir` is treated as the parent
    /// directory, never as the file itself.
    pub fn open_in_dir(config: ClusterConfig, dir: impl AsRef<Path>) -> Result<Self, ServerError> {
        let tick_interval_ms = config.tick_interval_ms.max(1);
        let driver = Driver::<FileHardStateStore>::open_file(config, dir)?;
        Ok(Self {
            driver,
            tick_interval_ms,
            stopped: None,
        })
    }

    /// Return the canonical hard-state **file** path derived from
    /// `config` (i.e. `<config.data_dir>/quorum-state`).
    ///
    /// Exposed so operator tooling (and the binary's startup log line)
    /// can show the path before [`Server::open`] is called. The name
    /// emphasises that the returned path identifies a *file*, not a
    /// directory — this is the path operators inspect with `cat`/`hexdump`
    /// to read the persisted (term, voted_for) state.
    pub fn hard_state_path(config: &ClusterConfig) -> PathBuf {
        config.data_dir.join(HARD_STATE_FILE_NAME)
    }
}

// -- Generic (test + production) operations ---------------------------------

impl<S: HardStateStore> Server<S> {
    /// Test-only constructor that wraps a caller-built [`Driver`]. Used
    /// to exercise failure-injection scenarios (e.g. an
    /// `AlwaysFailPersistStore`) without going through the file-backed
    /// open path. Hidden behind `#[cfg(test)]` so production callers
    /// cannot bypass [`Server::open`]'s persistence wiring.
    #[cfg(test)]
    pub(crate) fn from_driver(driver: Driver<S>, tick_interval_ms: u64) -> Self {
        Self {
            driver,
            tick_interval_ms: tick_interval_ms.max(1),
            stopped: None,
        }
    }

    /// Read-only access to the underlying driver. Mutating accessors
    /// are intentionally not exposed: every state mutation must go
    /// through [`Server::step`] so the [`Driver`]'s persist-before-reply
    /// ordering cannot be bypassed.
    pub fn driver(&self) -> &Driver<S> {
        &self.driver
    }

    /// Cadence (in milliseconds) that [`Server::run`] uses to fire
    /// [`Input::Tick`].
    pub fn tick_interval_ms(&self) -> u64 {
        self.tick_interval_ms
    }

    /// `true` iff a previous [`Server::step`] returned an
    /// [`ServerError::UnsupportedAction`] (or the
    /// driver-contract-violation `PersistHardState` leak), in which
    /// case all subsequent [`Server::step`] calls return
    /// [`ServerError::Stopped`] until a fresh [`Server`] is
    /// constructed. Operators / supervisors can poll this to decide
    /// whether to restart the host.
    pub fn is_stopped(&self) -> bool {
        self.stopped.is_some()
    }

    /// The originally-rejected action kind that put the server into
    /// the `Stopped` state, or `None` if the server is still live.
    pub fn stop_reason(&self) -> Option<&'static str> {
        self.stopped
    }

    /// Step the engine on `input`, dispatching the resulting actions
    /// per the Stage 2.2 server contract.
    ///
    /// Persistence ordering is delegated to [`Driver::step`]: every
    /// emitted [`Action::PersistHardState`] is written to disk before
    /// `step` returns, and the action vector handed back to this
    /// function never contains `PersistHardState`.
    ///
    /// This server then **classifies** every remaining action:
    ///
    /// | Action kind                                                           | Stage 2.2 disposition |
    /// |-----------------------------------------------------------------------|------------------------|
    /// | [`Action::SendMessage`], [`Action::BecomeLeader`], [`Action::StepDown`] | Logged at `debug!` and returned to the caller for inspection. Transport / role-notification wiring is a later stage; the actions are **safe to drop** as long as the persistence invariant is honoured (which it already is). |
    /// | [`Action::AppendEntries`], [`Action::ApplyToStateMachine`], [`Action::TruncateLog`], [`Action::TakeSnapshot`], [`Action::InstallSnapshot`], [`Action::ServeFetch`] | Returned as [`ServerError::UnsupportedAction`]. These are durable / state-machine actions that cannot be silently swallowed without risking data loss; surfacing them as a typed error makes "Stage 2.2 has no log/state-machine driver yet" impossible to forget at higher-stage wiring time. |
    ///
    /// The classifier is intentionally **strict**: drop-and-pretend on
    /// durable actions would let the engine advance state that the
    /// disk does not reflect, which is exactly the bug the persistence
    /// work item is designed to prevent.
    ///
    /// # Terminal-after-Unsupported semantics
    ///
    /// The very first line of `step` checks an internal `stopped` flag.
    /// Once an [`ServerError::UnsupportedAction`] has been returned (or
    /// the driver-contract `PersistHardState` leak), the flag is set
    /// **before** the error is yielded, so any subsequent `step` call
    /// short-circuits with [`ServerError::Stopped`] (carrying the
    /// originally-rejected kind). This prevents a caller that catches
    /// `UnsupportedAction` from quietly ignoring it and re-stepping an
    /// engine whose previous durable action was never dispatched —
    /// which would let in-memory state advance past what the
    /// transport/log/state-machine layers have actually delivered.
    /// Recovery from `Stopped` requires constructing a fresh `Server`.
    pub fn step(&mut self, input: Input) -> Result<Vec<Action>, ServerError> {
        if let Some(kind) = self.stopped {
            // Server is terminal after a prior UnsupportedAction (or
            // PersistHardState leak). Refuse further steps so callers
            // cannot keep advancing an engine whose previous durable
            // action was never dispatched. The originally-rejected kind
            // is preserved in the error so audits can correlate the
            // stop reason with the first-failure entry in the log.
            error!(
                kind = kind,
                "Server::step refused: server stopped after prior UnsupportedAction; \
                 construct a fresh Server to resume",
            );
            return Err(ServerError::Stopped(kind));
        }
        let actions = self.driver.step(input)?;
        let mut out = Vec::with_capacity(actions.len());
        for action in actions {
            match Self::classify(&action) {
                ActionDisposition::DropAndReturn => {
                    debug!(
                        action = ?action,
                        "Server: dropping transport/notification action (Stage 2.2 has no transport driver)",
                    );
                    out.push(action);
                }
                ActionDisposition::Unsupported(kind) => {
                    error!(
                        action = ?action,
                        kind = kind,
                        "Server: engine emitted unsupported action; Stage 2.2 server has no driver for this kind",
                    );
                    // Mark the server terminal BEFORE returning so a
                    // catch-and-continue caller cannot get a fresh step
                    // through. Recovery requires a new Server.
                    self.stopped = Some(kind);
                    return Err(ServerError::UnsupportedAction(kind));
                }
                ActionDisposition::Persist => {
                    // Driver::step filters Action::PersistHardState out
                    // of the returned vector. Reaching this branch means
                    // the driver contract was violated — fail loud AND
                    // mark the server terminal so the contract violation
                    // cannot be ignored on subsequent calls.
                    error!(
                        "Server: driver contract violation - PersistHardState reached server classifier; \
                         marking server terminal",
                    );
                    self.stopped = Some("PersistHardState");
                    return Err(ServerError::UnsupportedAction("PersistHardState"));
                }
            }
        }
        Ok(out)
    }

    /// Convenience wrapper around `step(Input::Tick)`.
    pub fn tick(&mut self) -> Result<Vec<Action>, ServerError> {
        self.step(Input::Tick)
    }

    /// Run the tick loop until `shutdown` resolves.
    ///
    /// Drives [`Input::Tick`] every `tick_interval_ms`. If a tick step
    /// returns an error (driver poisoned, persist failure, unsupported
    /// action), the loop returns it immediately — there is no
    /// "best-effort retry" path because the persistence invariant
    /// requires a clean restart from durable state.
    ///
    /// Uses [`MissedTickBehavior::Skip`] so a host that pauses (e.g. a
    /// VM live-migration pause) does not get a burst of catch-up ticks
    /// that would cause spurious election-timer expiries.
    pub async fn run<F>(mut self, shutdown: F) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send,
    {
        let period = Duration::from_millis(self.tick_interval_ms);
        // Start the first tick one period out — calling `tick()` on a
        // freshly-constructed `tokio::time::Interval` would otherwise
        // resolve immediately and burn the first tick on the same poll
        // cycle as the shutdown future, making short-lived test runs
        // racy. `interval_at(now + period, period)` makes the cadence
        // deterministic regardless of how soon shutdown is awaited.
        let mut ticker = interval_at(Instant::now() + period, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!(
            node_id = self.driver.node().id.0,
            tick_interval_ms = self.tick_interval_ms,
            "Server::run starting tick loop",
        );

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                // Bias the shutdown branch so a shutdown signal that
                // arrives concurrently with a tick is honoured promptly
                // (otherwise tokio's pseudo-random select could keep
                // ticking on a saturated runtime).
                biased;
                _ = &mut shutdown => {
                    info!("Server::run shutdown signal received; exiting tick loop");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.step(Input::Tick) {
                        error!(error = %e, "Server::run tick failed; aborting loop");
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Decompose into the underlying store for explicit drop sequencing.
    /// Mirrors [`Driver::into_store`]; the engine is dropped in the
    /// process and cannot escape the server.
    pub fn into_store(self) -> S {
        let _ = self.tick_interval_ms;
        self.driver.into_store()
    }

    // -- Action classification ----------------------------------------------

    fn classify(action: &Action) -> ActionDisposition {
        match action {
            Action::PersistHardState => ActionDisposition::Persist,
            Action::SendMessage { .. } | Action::BecomeLeader | Action::StepDown => {
                ActionDisposition::DropAndReturn
            }
            Action::AppendEntries(_) => ActionDisposition::Unsupported("AppendEntries"),
            Action::ApplyToStateMachine { .. } => {
                ActionDisposition::Unsupported("ApplyToStateMachine")
            }
            Action::TruncateLog { .. } => ActionDisposition::Unsupported("TruncateLog"),
            Action::TakeSnapshot => ActionDisposition::Unsupported("TakeSnapshot"),
            Action::InstallSnapshot { .. } => ActionDisposition::Unsupported("InstallSnapshot"),
            Action::ServeFetch { .. } => ActionDisposition::Unsupported("ServeFetch"),
        }
    }
}

/// Internal classification verdict for a single [`Action`].
enum ActionDisposition {
    /// Transport / notification action — drop with a debug log and
    /// return it to the caller for observability.
    DropAndReturn,
    /// Durable / state-machine action that Stage 2.2 cannot honour;
    /// surface as [`ServerError::UnsupportedAction`].
    Unsupported(&'static str),
    /// `Action::PersistHardState`. Should never reach the classifier
    /// because [`Driver::step`] filters it; reaching this branch means
    /// a driver-contract violation and is escalated as an error.
    Persist,
}

// ---------------------------------------------------------------------------
// Helper used by `xraft-server` binary
// ---------------------------------------------------------------------------

/// Default no-op shutdown signal. Used by tests that drive `run` via
/// a different exit path (poisoning, timeout) than a real shutdown
/// signal — production binaries provide [`tokio::signal::ctrl_c`].
#[doc(hidden)]
pub async fn never_shutdown() {
    // Future that never resolves. Equivalent to `pending::<()>().await`
    // but spelled out so a reader does not have to know `pending`.
    std::future::pending::<()>().await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use xraft_core::message::{Action, Input, OutboundMessage, VoteRequest};
    use xraft_core::types::{HardState, LogIndex, NodeId, NodeRole, Term};
    use xraft_storage::MemoryHardStateStore;

    use crate::driver::{Driver, DriverError};
    use crate::test_support::{
        AlwaysFailPersistStore, LoadFailStore, RecordingStore, single_voter_config,
        three_node_config,
    };

    // ----- helpers --------------------------------------------------------

    fn vote_request(term: u64, candidate: u64) -> VoteRequest {
        VoteRequest {
            cluster_id: "test-cluster".to_string(),
            leader_epoch: 0,
            term: Term(term),
            candidate_id: NodeId(candidate),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }
    }

    // -- Server::open / open_in_dir ---------------------------------------

    #[test]
    fn server_open_in_dir_uses_default_when_store_is_empty() {
        let tmp = TempDir::new().unwrap();
        let server = Server::open_in_dir(three_node_config(1), tmp.path()).expect("open succeeds");
        assert_eq!(server.driver().node().hard_state, HardState::default());
        assert_eq!(server.driver().node().role, NodeRole::Follower);
    }

    #[test]
    fn server_open_creates_canonical_quorum_state_file_directly_under_data_dir() {
        // Per architecture.md §3.3 and implementation-plan.md Stage 2.2,
        // `<data_dir>/quorum-state` is the FILE itself (not a
        // subdirectory containing a same-named file). This test pins
        // that contract: after `Server::open` + a step that triggers a
        // persist, the canonical file lives directly under
        // `config.data_dir` and there is no nested `quorum-state/`
        // subdirectory.
        let tmp = TempDir::new().unwrap();
        let mut config = three_node_config(1);
        config.data_dir = tmp.path().to_path_buf();

        let canonical_file = Server::hard_state_path(&config);
        assert_eq!(canonical_file, tmp.path().join("quorum-state"));

        // Drive a persist via VoteRequest at higher term so the
        // engine emits Action::PersistHardState and the driver writes
        // the file.
        {
            let mut server = Server::open(config.clone()).expect("open succeeds");
            // Pre-persist invariant: the file does not exist yet — open()
            // must NOT eagerly write a default state to disk.
            assert!(
                !canonical_file.exists(),
                "open() must not eagerly write a default hard-state file at {canonical_file:?}",
            );
            // Equally important: there must be no nested
            // `<data_dir>/quorum-state/` directory either, because the
            // bug we are pinning here was that `Server::open` passed
            // `<data_dir>/quorum-state` as the parent directory to
            // FileHardStateStore (creating a directory of that name),
            // resulting in a `<data_dir>/quorum-state/quorum-state`
            // file. Verify the corrupted layout is impossible.
            let nested = tmp.path().join("quorum-state").join("quorum-state");
            assert!(
                !nested.exists(),
                "must not create a nested {nested:?} — the canonical file lives directly under data_dir",
            );

            let _ = server
                .step(Input::VoteRequest(vote_request(7, 2)))
                .expect("step ok");
        }

        // Post-persist: the canonical FILE exists at <data_dir>/quorum-state,
        // and it is a regular file (not a directory).
        let md = std::fs::metadata(&canonical_file).expect("file must exist");
        assert!(
            md.is_file(),
            "canonical hard-state path must be a regular file, got {md:?}",
        );

        // No nested directory was ever created.
        let nested = tmp.path().join("quorum-state").join("quorum-state");
        assert!(
            !nested.exists(),
            "no nested {nested:?} must be produced by Server::open + step",
        );

        // Reopen the server: the recovered hard state must match what
        // step persisted. This proves Server::open uses the canonical
        // path on the recovery side, not just the write side.
        let server2 = Server::open(config).expect("reopen succeeds");
        assert_eq!(server2.driver().node().hard_state.current_term, Term(7));
        assert_eq!(
            server2.driver().node().hard_state.voted_for,
            Some(NodeId(2)),
            "Server::open must recover the same path it wrote to",
        );
    }

    #[test]
    fn server_open_propagates_corrupt_state_file() {
        let tmp = TempDir::new().unwrap();
        // Stage a corrupt quorum-state file in the dir we're about to
        // open as a hard-state store.
        std::fs::write(tmp.path().join("quorum-state"), b"{not-valid-json").unwrap();
        let err = Server::open_in_dir(three_node_config(1), tmp.path())
            .expect_err("corrupt state must surface as ServerError");
        match err {
            ServerError::Driver(DriverError::Storage { op, .. }) => {
                assert_eq!(op, "open", "expected open-time storage error, got op={op}");
            }
            other => panic!("expected Driver(Storage{{op: open}}), got {other:?}"),
        }
    }

    // -- Persistence across restart (the Stage 2.2 acceptance scenario) ---

    #[test]
    fn server_persists_hard_state_across_lifecycle_via_file_store() {
        let tmp = TempDir::new().unwrap();

        // 1. Open server, send a VoteRequest at higher term so the
        //    engine bumps term + grants vote + emits PersistHardState.
        {
            let mut server = Server::open_in_dir(three_node_config(1), tmp.path()).expect("open 1");
            assert_eq!(server.driver().node().hard_state, HardState::default());

            let returned = server
                .step(Input::VoteRequest(vote_request(7, 2)))
                .expect("step ok");
            assert_eq!(server.driver().node().hard_state.current_term, Term(7));
            assert_eq!(server.driver().node().hard_state.voted_for, Some(NodeId(2)));

            // The VoteResponse is in the returned vec; PersistHardState is not.
            assert!(
                !returned
                    .iter()
                    .any(|a| matches!(a, Action::PersistHardState))
            );
            assert!(returned.iter().any(|a| matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::VoteResponse(_),
                    ..
                }
            )));
        } // server dropped → file store closed.

        // 2. Reopen at the SAME directory. The recovered hard state
        //    must match what step persisted.
        let server2 = Server::open_in_dir(three_node_config(1), tmp.path()).expect("open 2");
        assert_eq!(server2.driver().node().hard_state.current_term, Term(7));
        assert_eq!(
            server2.driver().node().hard_state.voted_for,
            Some(NodeId(2))
        );
        assert_eq!(
            server2.driver().node().role,
            NodeRole::Follower,
            "recovery must not auto-elect",
        );
    }

    // -- step: action classification --------------------------------------

    #[test]
    fn server_step_drops_send_message_with_debug_log_and_returns_it() {
        // VoteRequest at higher term → engine emits VoteResponse SendMessage.
        // Stage 2.2 server has no transport, so it logs+drops but still
        // returns it for caller inspection.
        let tmp = TempDir::new().unwrap();
        let mut server = Server::open_in_dir(three_node_config(1), tmp.path()).expect("open ok");
        let returned = server.step(Input::VoteRequest(vote_request(3, 2))).unwrap();
        assert!(returned.iter().any(|a| matches!(
            a,
            Action::SendMessage {
                message: OutboundMessage::VoteResponse(_),
                ..
            }
        )));
    }

    #[test]
    fn server_step_returns_unsupported_action_for_durable_kinds() {
        // A single-voter cluster that wins an election emits
        // Action::AppendEntries (the no-op entry the new leader
        // appends). Stage 2.2 has no log driver, so step must surface
        // ServerError::UnsupportedAction("AppendEntries") and refuse to
        // silently drop the durable action.
        let tmp = TempDir::new().unwrap();
        let mut server = Server::open_in_dir(single_voter_config(), tmp.path()).expect("open ok");

        let mut last_err: Option<ServerError> = None;
        for _ in 0..2_000 {
            match server.step(Input::Tick) {
                Ok(_) => continue,
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }

        let err = last_err.expect(
            "single-voter cluster must reach leader within 2000 ticks and emit a durable action",
        );
        match err {
            ServerError::UnsupportedAction(kind) => {
                // Either AppendEntries (for the leader's no-op entry)
                // or ApplyToStateMachine (after the entry is committed
                // and the engine instructs the driver to apply it) is
                // an acceptable signal that we hit the durable-action
                // boundary the classifier protects.
                assert!(
                    matches!(
                        kind,
                        "AppendEntries" | "ApplyToStateMachine" | "ServeFetch" | "TakeSnapshot"
                    ),
                    "expected durable-kind UnsupportedAction, got {kind:?}",
                );
            }
            other => panic!("expected UnsupportedAction, got {other:?}"),
        }
    }

    /// Iter-5 regression: a caller that catches
    /// [`ServerError::UnsupportedAction`] and tries to step again must
    /// be rejected with [`ServerError::Stopped`] carrying the
    /// originally-rejected kind. Without this, a catch-and-continue
    /// supervisor could keep advancing the engine past durable actions
    /// that no transport / log driver delivered, defeating the whole
    /// point of surfacing the action as a typed error.
    #[test]
    fn server_step_is_terminal_after_unsupported_action_subsequent_steps_return_stopped() {
        let tmp = TempDir::new().unwrap();
        let mut server = Server::open_in_dir(single_voter_config(), tmp.path()).expect("open ok");

        // Drive ticks until the single-voter cluster wins and the
        // classifier surfaces UnsupportedAction.
        let mut first_kind: Option<&'static str> = None;
        for _ in 0..2_000 {
            match server.step(Input::Tick) {
                Ok(_) => continue,
                Err(ServerError::UnsupportedAction(kind)) => {
                    first_kind = Some(kind);
                    break;
                }
                Err(other) => panic!("expected UnsupportedAction, got {other:?}"),
            }
        }
        let original_kind =
            first_kind.expect("single-voter cluster must hit UnsupportedAction within 2000 ticks");

        // The server must now be terminal. is_stopped() exposes that.
        assert!(
            server.is_stopped(),
            "Server must be terminal after UnsupportedAction",
        );
        assert_eq!(
            server.stop_reason(),
            Some(original_kind),
            "stop_reason must preserve the originally-rejected kind",
        );

        // ANY subsequent step — Tick, VoteRequest, anything — must
        // short-circuit with Stopped(original_kind), not with another
        // UnsupportedAction (which would let a polling supervisor see
        // the same error each tick and assume "transient, will retry").
        let again = server
            .step(Input::Tick)
            .expect_err("post-Stopped step must be rejected");
        match again {
            ServerError::Stopped(kind) => assert_eq!(
                kind, original_kind,
                "Stopped must carry the original kind, got {kind:?}",
            ),
            other => panic!("expected ServerError::Stopped, got {other:?}"),
        }

        // A second post-stop step is also Stopped (the flag is sticky,
        // not one-shot).
        let again2 = server
            .step(Input::VoteRequest(vote_request(99, 2)))
            .expect_err("post-Stopped step must remain rejected");
        assert!(
            matches!(again2, ServerError::Stopped(k) if k == original_kind),
            "got {again2:?}",
        );

        // The driver itself is NOT poisoned — disk state is consistent
        // and the engine's in-memory state is still inspectable. Only
        // forward progress through the server is blocked.
        assert!(
            !server.driver().is_poisoned(),
            "driver MUST NOT be poisoned by an UnsupportedAction (no persist failed)",
        );
    }

    /// Iter-5 regression: when [`Server::run`] hits an
    /// [`ServerError::UnsupportedAction`] on a tick, it must return the
    /// error and exit the loop — the post-Stopped state means there is
    /// no useful work the loop could do anyway, but this test pins the
    /// `run` exit path explicitly so a future refactor cannot make it
    /// silently swallow the error and keep ticking.
    #[tokio::test]
    async fn server_run_exits_on_first_unsupported_action() {
        let tmp = TempDir::new().unwrap();
        let server = Server::open_in_dir(single_voter_config(), tmp.path()).expect("open ok");

        let result =
            tokio::time::timeout(Duration::from_secs(2), server.run(super::never_shutdown()))
                .await
                .expect("run must complete (via error) within timeout");

        match result {
            Err(ServerError::UnsupportedAction(kind)) => {
                assert!(
                    matches!(
                        kind,
                        "AppendEntries" | "ApplyToStateMachine" | "ServeFetch" | "TakeSnapshot"
                    ),
                    "expected durable-kind UnsupportedAction, got {kind:?}",
                );
            }
            other => panic!("expected Err(UnsupportedAction), got {other:?}"),
        }
    }

    #[test]
    fn server_step_propagates_persist_failure_and_subsequent_steps_are_poisoned() {
        // Drive through the test-only constructor with a failing store.
        // VoteRequest at higher term triggers a persist; the driver
        // poisons; subsequent step is rejected with Poisoned.
        let driver = Driver::open(three_node_config(1), AlwaysFailPersistStore::new())
            .expect("driver opens (load returns Ok(None))");
        let mut server = Server::from_driver(driver, 10);

        let err = server
            .step(Input::VoteRequest(vote_request(5, 2)))
            .expect_err("persist failure must propagate");
        match &err {
            ServerError::Driver(DriverError::Storage { op, .. }) => assert_eq!(*op, "persist"),
            other => panic!("expected Driver(Storage{{op: persist}}), got {other:?}"),
        }
        assert!(server.driver().is_poisoned(), "driver must be poisoned");

        let again = server
            .step(Input::Tick)
            .expect_err("subsequent step must be rejected");
        assert!(
            matches!(again, ServerError::Driver(DriverError::Poisoned)),
            "got {again:?}",
        );
    }

    #[test]
    fn server_open_in_dir_propagates_load_failure() {
        // LoadFailStore can only be wired through a custom Driver. Use
        // the test-only `from_driver` after constructing the driver
        // ourselves; this proves the error type chain end-to-end.
        let err = Driver::open(three_node_config(1), LoadFailStore)
            .map(|d| Server::from_driver(d, 10))
            .map(|_| ())
            .expect_err("load failure must surface");
        match err {
            DriverError::Storage { op, .. } => assert_eq!(op, "load"),
            other => panic!("expected Storage{{op: load}}, got {other:?}"),
        }
    }

    // -- Recording store: persist counter sanity --------------------------

    #[test]
    fn server_step_records_persist_calls_through_driver() {
        let store = RecordingStore::new();
        let counter = store.persist_count_handle();
        let driver = Driver::open(three_node_config(1), store).expect("open ok");
        let mut server = Server::from_driver(driver, 10);

        let _ = server
            .step(Input::VoteRequest(vote_request(11, 2)))
            .unwrap();
        assert_eq!(*counter.lock().unwrap(), 1, "exactly one persist call");
        assert_eq!(server.driver().node().hard_state.current_term, Term(11));
    }

    // -- run: tokio lifecycle --------------------------------------------

    #[tokio::test]
    async fn server_run_exits_cleanly_on_shutdown_signal() {
        // Open a fresh follower and run with a oneshot shutdown that
        // we trigger after a few tick intervals. The loop must exit
        // with Ok(()) and the durable state must be unchanged (no
        // election happened in the short test window because the
        // tighter cadence still does not exceed election_timeout_max
        // when tick_interval is the default 10ms × ~2 ticks).
        let tmp = TempDir::new().unwrap();
        let mut config = three_node_config(1);
        config.tick_interval_ms = 5;
        config.election_timeout_min_ms = 5_000;
        config.election_timeout_max_ms = 10_000;
        let server = Server::open_in_dir(config, tmp.path()).expect("open ok");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        let run_handle = tokio::spawn(server.run(shutdown));

        // Let a handful of ticks fire so we know the loop is actually
        // pumping, then signal shutdown.
        tokio::time::sleep(Duration::from_millis(40)).await;
        let _ = shutdown_tx.send(());

        let result = tokio::time::timeout(Duration::from_secs(2), run_handle)
            .await
            .expect("run task must terminate within timeout")
            .expect("join handle must succeed");
        assert!(result.is_ok(), "expected clean shutdown, got {result:?}");
    }

    #[tokio::test]
    async fn server_run_returns_error_on_persist_failure_during_tick() {
        // Wire a single-voter cluster (election triggers itself) with a
        // failing persist store. The first tick that crosses the
        // PreCandidate → Candidate boundary will trigger a persist
        // attempt, which fails, which poisons the driver, which surfaces
        // through `run` as ServerError::Driver(Storage{op:"persist"}).
        let driver = Driver::open(single_voter_config(), AlwaysFailPersistStore::new())
            .expect("driver opens");
        let attempts = driver.store().attempts_handle();
        let server = Server::from_driver(driver, 5);

        // never_shutdown ⇒ run() exits only via the error path.
        let result =
            tokio::time::timeout(Duration::from_secs(2), server.run(super::never_shutdown()))
                .await
                .expect("run must complete (via error) within timeout");

        match result {
            Err(ServerError::Driver(DriverError::Storage { op, .. })) => {
                assert_eq!(op, "persist");
            }
            Err(ServerError::Driver(DriverError::Poisoned)) => {
                // Acceptable: a tick after the first persist failure
                // hits the poison check before the second persist.
            }
            other => {
                panic!("expected Driver(Storage{{op:persist}}) or Driver(Poisoned), got {other:?}",)
            }
        }
        assert!(
            !attempts.lock().unwrap().is_empty(),
            "at least one persist attempt should have been recorded",
        );
    }

    #[tokio::test]
    async fn server_run_uses_biased_select_so_shutdown_wins_over_pending_tick() {
        // Construct a server whose tick cadence is 1ms — fast enough
        // that shutdown and tick are practically always racing. The
        // biased select in `run` must still let shutdown win.
        let tmp = TempDir::new().unwrap();
        let mut config = three_node_config(1);
        config.tick_interval_ms = 1;
        config.election_timeout_min_ms = 5_000;
        config.election_timeout_max_ms = 10_000;
        let server = Server::open_in_dir(config, tmp.path()).expect("open ok");

        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled2 = cancelled.clone();
        let shutdown = async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            cancelled2.store(true, Ordering::SeqCst);
        };

        tokio::time::timeout(Duration::from_secs(1), server.run(shutdown))
            .await
            .expect("run must terminate")
            .expect("run must return Ok");
        assert!(cancelled.load(Ordering::SeqCst));
    }

    // -- into_store --------------------------------------------------------

    #[test]
    fn server_into_store_returns_only_the_store() {
        // Compile-time check: the only thing extractable from a Server
        // is the store. The engine cannot escape, satisfying the
        // evaluator's "no public mutable bypass" requirement.
        let server = Server::from_driver(
            Driver::open(three_node_config(1), MemoryHardStateStore::new()).unwrap(),
            10,
        );
        let _store: MemoryHardStateStore = server.into_store();
    }

    // -- Deprecated alias -------------------------------------------------

    /// Iter-5 regression: the deprecated `HARD_STATE_DIR_NAME` alias
    /// must resolve to the identical string as the canonical
    /// `HARD_STATE_FILE_NAME`. If a future refactor accidentally
    /// changed one without the other, downstream callers that imported
    /// the old name would silently start writing to a different path.
    #[test]
    #[allow(deprecated)]
    fn deprecated_hard_state_dir_name_matches_canonical_file_name() {
        assert_eq!(
            super::HARD_STATE_DIR_NAME,
            super::HARD_STATE_FILE_NAME,
            "deprecated alias must resolve to the same string as the canonical name",
        );
        assert_eq!(super::HARD_STATE_DIR_NAME, "quorum-state");
    }
}
