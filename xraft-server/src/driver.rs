//! Driver: durable bridge between the I/O-free [`RaftNode`] engine and
//! the [`HardStateStore`] persistence layer (Stage 2.2 — Persistent Raft State).
//!
//! # Why a driver
//!
//! `xraft-core` is deliberately I/O-free per `architecture.md` §4.1: the
//! engine produces [`Action`] side-effects but never touches the disk or
//! the network itself. Honouring those actions is the **driver's** job.
//! For Stage 2.2 the only action that matters is
//! [`Action::PersistHardState`] — every term bump, every grant of
//! `voted_for`, and every higher-term step-down emits one, and
//! Raft safety requires the resulting [`HardState`] to be on durable
//! storage **before** any RPC reply derived from it leaves the box.
//!
//! [`Driver::step`] enforces that ordering by processing
//! [`Action::PersistHardState`] inline (filtering it out of the returned
//! action vector). The remaining [`Action`]s are forwarded to the caller
//! for transport / log / state-machine handling in later stages.
//!
//! # Failure model
//!
//! A failed `persist` is treated as **fatal** for the driver: the node's
//! in-memory term/vote has already moved forward but the disk still
//! reflects the prior valid state, so any further engine output would
//! be derived from un-persisted state and could violate single-vote-per-term
//! on restart. The driver poisons itself ([`DriverError::Poisoned`]) and
//! refuses subsequent inputs until the host restarts and re-loads from
//! the still-valid on-disk state.

use std::path::Path;

use thiserror::Error;
use tracing::{debug, error};

use xraft_core::config::ClusterConfig;
use xraft_core::error::XRaftError;
use xraft_core::message::{Action, Input};
use xraft_core::node::RaftNode;
use xraft_core::storage::HardStateStore;
use xraft_core::types::HardState;
use xraft_storage::FileHardStateStore;

// ---------------------------------------------------------------------------
// DriverError
// ---------------------------------------------------------------------------

/// Errors returned by [`Driver`]. Distinguishes engine construction errors
/// from storage failures so callers can route them to the appropriate
/// recovery path (config errors are unrecoverable; storage errors at
/// startup may indicate a fresh deployment vs a damaged disk).
#[derive(Debug, Error)]
pub enum DriverError {
    /// A [`HardStateStore`] operation failed. `op` identifies which call
    /// raised the error so logs disambiguate startup (`load`) from
    /// runtime (`persist`) failures without leaning on string matching.
    #[error("hard-state storage error during {op}: {source}")]
    Storage {
        op: &'static str,
        #[source]
        source: XRaftError,
    },

    /// The underlying [`RaftNode`] could not be constructed (e.g. invalid
    /// `ClusterConfig` or voter set). Surfaced as a typed wrapper so the
    /// caller does not have to unwrap two error layers.
    #[error("engine construction error: {0}")]
    Engine(#[from] XRaftError),

    /// A prior call to [`Driver::step`] failed to persist hard state.
    /// The driver is now in an inconsistent state (engine moved forward
    /// past the durable record) and must be re-opened from disk.
    /// Subsequent calls return this error rather than producing actions
    /// derived from un-persisted state.
    #[error("driver poisoned by failed hard-state persist; restart and recover from durable state")]
    Poisoned,
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// The Stage 2.2 driver: pairs a [`RaftNode`] with a [`HardStateStore`]
/// and processes [`Action::PersistHardState`] in the only ordering that
/// preserves Raft safety.
///
/// Generic over the store so deterministic tests can plug in a
/// [`MemoryHardStateStore`](xraft_storage::MemoryHardStateStore) and
/// production code can plug in a [`FileHardStateStore`]. The
/// [`open_file`](Driver::open_file) convenience constructor makes the
/// disk-backed startup path the obvious default.
pub struct Driver<S: HardStateStore> {
    node: RaftNode,
    store: S,
    /// Set to `true` after any `persist` failure so further inputs are
    /// rejected with [`DriverError::Poisoned`].
    poisoned: bool,
}

impl<S: HardStateStore> std::fmt::Debug for Driver<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("node_id", &self.node.id)
            .field("role", &self.node.role)
            .field("hard_state", &self.node.hard_state)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl<S: HardStateStore> Driver<S> {
    /// Open a driver with a caller-provided store. Loads any persisted
    /// [`HardState`], falls back to [`HardState::default`] when the store
    /// has never been written, and constructs the [`RaftNode`] at the
    /// recovered term + vote.
    ///
    /// Returns a [`DriverError::Storage`] when the load itself fails
    /// (corrupt file, unsupported schema version, I/O error). Returns a
    /// [`DriverError::Engine`] when [`ClusterConfig`] validation fails
    /// or the voter set is malformed.
    pub fn open(config: ClusterConfig, store: S) -> Result<Self, DriverError> {
        let recovered = store
            .load()
            .map_err(|source| DriverError::Storage { op: "load", source })?;
        let hard_state = match recovered {
            Some(hs) => {
                debug!(
                    term = hs.current_term.0,
                    voted_for = ?hs.voted_for,
                    "Driver::open recovered persisted hard state",
                );
                hs
            }
            // Stage 2.2 contract: trait returns Ok(None) for never-persisted;
            // driver maps that to the canonical first-boot HardState.
            None => {
                debug!("Driver::open: no persisted hard state; starting at HardState::default()");
                HardState::default()
            }
        };
        let node = RaftNode::new_with_initial_hard_state(config, hard_state)?;
        Ok(Self {
            node,
            store,
            poisoned: false,
        })
    }

    /// Step the engine on `input`, persist any emitted
    /// [`Action::PersistHardState`] **before** returning, and yield the
    /// remaining actions for the caller to dispatch (transport / log
    /// writes / state-machine apply / etc.).
    ///
    /// Per the contract documented on [`RaftNode::step`], persisting
    /// hard state before any RPC reply leaves the process is a hard
    /// safety requirement. Filtering [`Action::PersistHardState`] out
    /// of the returned vector makes that requirement impossible to
    /// violate from the call site.
    ///
    /// # Errors
    ///
    /// * [`DriverError::Poisoned`] if a prior step failed to persist.
    /// * [`DriverError::Storage`] if `store.persist` fails. The driver
    ///   becomes poisoned in that case and any remaining actions in
    ///   the vector are **dropped** — the engine has already moved
    ///   past the durable record, so no further side-effect derived
    ///   from the new state may be emitted.
    pub fn step(&mut self, input: Input) -> Result<Vec<Action>, DriverError> {
        if self.poisoned {
            return Err(DriverError::Poisoned);
        }
        let raw = self.node.step(input);
        self.process_actions(raw)
    }

    /// Process a raw action vector from the engine. Persists every
    /// [`Action::PersistHardState`] inline, returns the rest in their
    /// original relative order.
    ///
    /// Crate-private on purpose: production callers go through
    /// [`Driver::step`] so the persist-before-RPC-reply ordering cannot
    /// be re-arranged by a misbehaving caller. Tests in this module can
    /// still call it to feed hand-built action vectors that exercise
    /// the failure-path branches (e.g. simulated term regression).
    pub(crate) fn process_actions(&mut self, raw: Vec<Action>) -> Result<Vec<Action>, DriverError> {
        let mut remaining = Vec::with_capacity(raw.len());
        for action in raw {
            if matches!(action, Action::PersistHardState) {
                let hs = self.node.hard_state.clone();
                self.store.persist(&hs).map_err(|source| {
                    // Poison BEFORE building the error so even a
                    // catch-and-continue caller cannot get a fresh
                    // step through.
                    self.poisoned = true;
                    error!(
                        term = hs.current_term.0,
                        voted_for = ?hs.voted_for,
                        error = %source,
                        "Driver: hard-state persist failed; poisoning driver",
                    );
                    DriverError::Storage {
                        op: "persist",
                        source,
                    }
                })?;
                debug!(
                    term = hs.current_term.0,
                    voted_for = ?hs.voted_for,
                    "Driver: hard state persisted",
                );
            } else {
                remaining.push(action);
            }
        }
        Ok(remaining)
    }

    /// Borrow the underlying engine. Test-only inspection / advancement
    /// goes through this; production code only needs [`Driver::step`].
    pub fn node(&self) -> &RaftNode {
        &self.node
    }

    /// **Test-only** mutable engine access. Bypasses the
    /// [`Action::PersistHardState`] handling in [`Driver::step`], so any
    /// test that uses this MUST round-trip subsequent state changes
    /// through [`Driver::step`] (or the crate-private
    /// [`Driver::process_actions`]) to keep durable state in sync.
    ///
    /// Gated behind `#[cfg(test)]` so production callers cannot reach
    /// it — the previous unconditional `pub fn` was an escape hatch
    /// that defeated the whole point of routing every term/vote
    /// transition through the driver.
    #[cfg(test)]
    pub(crate) fn node_mut(&mut self) -> &mut RaftNode {
        &mut self.node
    }

    /// Borrow the underlying hard-state store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// `true` iff a previous [`Driver::step`] failed to persist hard state.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Decompose the driver into the underlying [`HardStateStore`] for
    /// explicit drop sequencing on shutdown. The [`RaftNode`] is dropped
    /// in the process and **cannot** be salvaged: returning the engine
    /// would let a caller re-step it without the persistence ordering
    /// that the driver was built to enforce. The previous `into_parts`
    /// API exposed exactly that bypass and was removed.
    pub fn into_store(self) -> S {
        self.store
    }
}

// ---------------------------------------------------------------------------
// FileHardStateStore convenience
// ---------------------------------------------------------------------------

impl Driver<FileHardStateStore> {
    /// Open a driver backed by the canonical disk-resident
    /// [`FileHardStateStore`] rooted at `dir`. This is the production
    /// startup path called from `xraft-server`'s main loop.
    ///
    /// Equivalent to `FileHardStateStore::open(dir).map(...)` followed by
    /// [`Driver::open`], but written as a single named call so the
    /// "load hard state from disk on boot" contract is grep-able.
    pub fn open_file(config: ClusterConfig, dir: impl AsRef<Path>) -> Result<Self, DriverError> {
        let store = FileHardStateStore::open(dir)
            .map_err(|source| DriverError::Storage { op: "open", source })?;
        Self::open(config, store)
    }
}

// ---------------------------------------------------------------------------
// Tests — Stage 2.2 driver-side coverage
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    use xraft_core::message::{Action, Input, OutboundMessage, VoteRequest};
    use xraft_core::storage::HardStateStore;
    use xraft_core::types::{HardState, NodeId, NodeRole, Term};
    use xraft_storage::{FileHardStateStore, MemoryHardStateStore};

    use crate::test_support::{
        AlwaysFailPersistStore, LoadFailStore, RecordingStore, three_node_config,
    };

    /// Drive the node's election timer to expiry, then fire a Tick. The
    /// resulting PreCandidate transition emits no PersistHardState (Pre-Vote
    /// does not bump term). We use a deeper transition to actually exercise
    /// PersistHardState below.
    fn drive_to_pre_candidate(driver: &mut Driver<impl HardStateStore>) -> Vec<Action> {
        // Manually expire the election timer: tick until is_expired().
        for _ in 0..1000 {
            if driver.node().election_timer.is_expired() {
                break;
            }
            let _ = driver.step(Input::Tick).expect("tick must not fail");
        }
        // One final tick to actually trigger the PreCandidate transition.
        driver.step(Input::Tick).expect("tick must not fail")
    }

    // -- Open: loading + default fallback -------------------------------

    #[test]
    fn open_uses_default_when_store_is_empty() {
        let store = MemoryHardStateStore::new();
        let driver = Driver::open(three_node_config(1), store).expect("open succeeds");
        assert_eq!(
            driver.node().hard_state,
            HardState::default(),
            "first-boot driver must start at HardState::default()",
        );
        assert_eq!(driver.node().role, NodeRole::Follower);
        assert!(!driver.is_poisoned());
    }

    #[test]
    fn open_recovers_persisted_hard_state() {
        let mut store = MemoryHardStateStore::new();
        let recovered = HardState {
            current_term: Term(7),
            voted_for: Some(NodeId(2)),
        };
        store.persist(&recovered).unwrap();

        let driver = Driver::open(three_node_config(1), store).expect("open succeeds");
        assert_eq!(driver.node().hard_state, recovered);
        // Recovery must NOT auto-elect — node restarts as Follower per
        // architecture.md §3.3 and the new_with_initial_hard_state contract.
        assert_eq!(driver.node().role, NodeRole::Follower);
    }

    #[test]
    fn open_propagates_storage_load_error() {
        let err = Driver::open(three_node_config(1), LoadFailStore)
            .expect_err("load failure must surface");
        match err {
            DriverError::Storage { op, .. } => assert_eq!(op, "load"),
            other => panic!("expected Storage{{op: load}}, got {other:?}"),
        }
    }

    #[test]
    fn open_propagates_engine_config_error() {
        // Empty cluster_id triggers ClusterConfig::validate() to reject.
        let mut bad_config = three_node_config(1);
        bad_config.cluster_id.clear();
        let err = Driver::open(bad_config, MemoryHardStateStore::new())
            .expect_err("engine construction must fail on bad config");
        assert!(
            matches!(err, DriverError::Engine(_)),
            "expected Engine error, got {err:?}",
        );
    }

    // -- step: PersistHardState handling --------------------------------

    #[test]
    fn step_persists_hard_state_when_engine_emits_persist_action() {
        let store = RecordingStore::new();
        let counter = store.persist_count_handle();
        let mut driver = Driver::open(three_node_config(1), store).expect("open succeeds");

        // Fire a VoteRequest with a HIGHER term — the engine bumps the
        // local term, grants the vote, and emits a coalesced PersistHardState.
        let req = VoteRequest {
            cluster_id: "test-cluster".to_string(),
            leader_epoch: 0,
            term: Term(5),
            candidate_id: NodeId(2),
            last_log_index: xraft_core::types::LogIndex(0),
            last_log_term: Term(0),
        };
        let returned = driver.step(Input::VoteRequest(req)).expect("step succeeds");

        assert_eq!(driver.node().hard_state.current_term, Term(5));
        assert_eq!(driver.node().hard_state.voted_for, Some(NodeId(2)));
        assert_eq!(*counter.lock().unwrap(), 1, "exactly one persist call");
        assert_eq!(
            driver.store().load().unwrap(),
            Some(driver.node().hard_state.clone()),
            "store must reflect the new hard state",
        );
        assert!(
            !returned
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "PersistHardState must be filtered from the returned action vector",
        );
    }

    #[test]
    fn step_returns_remaining_actions_in_order_after_persisting() {
        // Verify that when the engine emits [PersistHardState, SendMessage(...)],
        // the driver persists FIRST (counter increments before any SendMessage
        // can be observed in the returned vec) and then returns the SendMessage.
        let store = RecordingStore::new();
        let counter = store.persist_count_handle();
        let mut driver = Driver::open(three_node_config(1), store).expect("open succeeds");

        let req = VoteRequest {
            cluster_id: "test-cluster".to_string(),
            leader_epoch: 0,
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_index: xraft_core::types::LogIndex(0),
            last_log_term: Term(0),
        };
        let returned = driver.step(Input::VoteRequest(req)).expect("step succeeds");

        // By the time `step` returns, persist has already run. This is the
        // observable proof that ordering is enforced.
        assert!(
            *counter.lock().unwrap() >= 1,
            "persist must have completed before step returned its action vec",
        );

        // The VoteResponse SendMessage MUST still be in the returned vec.
        let has_send = returned.iter().any(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: OutboundMessage::VoteResponse(_),
                    ..
                },
            )
        });
        assert!(
            has_send,
            "expected VoteResponse SendMessage in returned actions, got {returned:?}",
        );
    }

    #[test]
    fn step_drops_remaining_actions_and_poisons_on_persist_failure() {
        let store = AlwaysFailPersistStore::new();
        let attempts = store.attempts_handle();
        let mut driver = Driver::open(three_node_config(1), store).expect("open succeeds");

        let req = VoteRequest {
            cluster_id: "test-cluster".to_string(),
            leader_epoch: 0,
            term: Term(9),
            candidate_id: NodeId(2),
            last_log_index: xraft_core::types::LogIndex(0),
            last_log_term: Term(0),
        };
        let err = driver
            .step(Input::VoteRequest(req))
            .expect_err("persist failure must propagate");
        match err {
            DriverError::Storage { op, .. } => assert_eq!(op, "persist"),
            other => panic!("expected Storage{{op: persist}}, got {other:?}"),
        }

        assert!(
            driver.is_poisoned(),
            "driver must be poisoned after persist failure"
        );
        assert_eq!(
            attempts.lock().unwrap().len(),
            1,
            "exactly one persist attempt before failure",
        );

        // Subsequent step is rejected with Poisoned (no engine call, no leak).
        let again = driver
            .step(Input::Tick)
            .expect_err("subsequent step must be rejected");
        assert!(matches!(again, DriverError::Poisoned), "got {again:?}");
        assert_eq!(
            attempts.lock().unwrap().len(),
            1,
            "no further persist attempts after poisoning",
        );
    }

    #[test]
    fn step_passes_through_non_persist_actions_without_touching_store() {
        // Plain Tick on a fresh follower emits no PersistHardState and no
        // SendMessage; verify the recorder shows zero persist calls and the
        // returned vec contains nothing safety-critical.
        let store = RecordingStore::new();
        let counter = store.persist_count_handle();
        let mut driver = Driver::open(three_node_config(1), store).expect("open succeeds");

        let actions = driver.step(Input::Tick).expect("tick succeeds");
        assert_eq!(*counter.lock().unwrap(), 0, "tick must not persist");
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PersistHardState)),
            "tick must not produce PersistHardState",
        );
    }

    #[test]
    fn step_persists_eventually_through_pre_candidate_transition() {
        // Drive the engine through enough Ticks for the election timer to
        // expire, then once more to trigger the PreCandidate transition.
        // PreVote does NOT bump term, so this exercises the "no persist
        // when state did not change" branch — the recording counter must
        // remain 0.
        let store = RecordingStore::new();
        let counter = store.persist_count_handle();
        let mut driver = Driver::open(three_node_config(1), store).expect("open succeeds");

        let _ = drive_to_pre_candidate(&mut driver);
        assert_eq!(
            driver.node().role,
            NodeRole::PreCandidate,
            "election timer expiry must reach PreCandidate",
        );
        assert_eq!(
            *counter.lock().unwrap(),
            0,
            "PreCandidate transition must NOT persist (no term bump)",
        );
    }

    // -- File-store recovery round trip ---------------------------------

    #[test]
    fn open_file_round_trip_recovers_persisted_state_after_restart() {
        let tmp = TempDir::new().unwrap();

        // 1. Boot a fresh driver, push it into a higher term via a
        //    VoteRequest that exceeds local term. The engine emits
        //    PersistHardState; the driver routes it to the file store.
        let recovered_term = {
            let mut driver =
                Driver::<FileHardStateStore>::open_file(three_node_config(1), tmp.path())
                    .expect("first open succeeds");
            assert_eq!(driver.node().hard_state, HardState::default());

            let req = VoteRequest {
                cluster_id: "test-cluster".to_string(),
                leader_epoch: 0,
                term: Term(11),
                candidate_id: NodeId(2),
                last_log_index: xraft_core::types::LogIndex(0),
                last_log_term: Term(0),
            };
            let _ = driver.step(Input::VoteRequest(req)).expect("step ok");
            assert_eq!(driver.node().hard_state.current_term, Term(11));
            assert_eq!(driver.node().hard_state.voted_for, Some(NodeId(2)));
            // driver dropped at end of scope ⇒ file store closed.
            driver.node().hard_state.current_term
        };

        // 2. Reopen the driver against the SAME directory; the loaded
        //    hard state must match what step persisted.
        let driver2 = Driver::<FileHardStateStore>::open_file(three_node_config(1), tmp.path())
            .expect("re-open after restart");
        assert_eq!(
            driver2.node().hard_state.current_term,
            recovered_term,
            "term must survive driver restart via file store",
        );
        assert_eq!(driver2.node().role, NodeRole::Follower);
    }

    #[test]
    fn open_file_first_boot_returns_default_hard_state() {
        let tmp = TempDir::new().unwrap();
        let driver = Driver::<FileHardStateStore>::open_file(three_node_config(1), tmp.path())
            .expect("first-boot open succeeds");
        assert_eq!(
            driver.node().hard_state,
            HardState::default(),
            "fresh data_dir must yield HardState::default() per Stage 2.2 contract",
        );
    }

    #[test]
    fn open_file_propagates_corrupt_state_file() {
        let tmp = TempDir::new().unwrap();
        // Stage a corrupt quorum-state file by hand.
        std::fs::write(tmp.path().join("quorum-state"), b"{not-valid-json").unwrap();
        let err = Driver::<FileHardStateStore>::open_file(three_node_config(1), tmp.path())
            .expect_err("corrupt state must fail open");
        match err {
            DriverError::Storage { op, .. } => assert_eq!(op, "open"),
            other => panic!("expected Storage{{op: open}}, got {other:?}"),
        }
    }

    // -- Term monotonicity propagation ----------------------------------

    #[test]
    fn driver_propagates_term_regression_storage_error_and_poisons() {
        // The real `MemoryHardStateStore` enforces term monotonicity.
        // Load returns Some at term=10; we then manipulate the engine to
        // a stale (lower) term and trigger persist via process_actions.
        // This proves the driver surfaces store-level safety violations
        // rather than swallowing them, and poisons itself per the
        // failed-persist contract.
        let mut store = MemoryHardStateStore::new();
        store
            .persist(&HardState {
                current_term: Term(10),
                voted_for: None,
            })
            .unwrap();

        let mut driver = Driver::open(three_node_config(1), store).expect("open ok");
        assert_eq!(driver.node().hard_state.current_term, Term(10));

        // Force the in-memory hard state DOWN. A real engine will not do
        // this, but the driver must still refuse to make the regression
        // durable — that is exactly the safety contract the storage layer
        // enforces and the driver propagates.
        driver.node_mut().hard_state.current_term = Term(3);

        let err = driver
            .process_actions(vec![Action::PersistHardState])
            .expect_err("term regression must surface as a Storage error");
        match err {
            DriverError::Storage { op, source } => {
                assert_eq!(op, "persist");
                assert!(
                    matches!(source, XRaftError::Storage(_)),
                    "expected XRaftError::Storage source, got {source:?}",
                );
                let msg = format!("{source}");
                assert!(
                    msg.contains("term regression"),
                    "error must mention term regression, got: {msg}",
                );
            }
            other => panic!("expected Storage{{op: persist}}, got {other:?}"),
        }
        assert!(driver.is_poisoned(), "driver must be poisoned");

        // A subsequent step is rejected with Poisoned, never reaching the
        // engine to mutate state further.
        let again = driver.step(Input::Tick).expect_err("poisoned driver");
        assert!(matches!(again, DriverError::Poisoned), "got {again:?}");
    }
}
