//! State machine callback trait — applied by the consensus layer after commit.
//!
//! The trait name and signatures follow `architecture.md` §4.1, which is
//! authoritative for trait definitions. This is the extension point for
//! consumers of XRAFT: the library provides the replicated log, and the
//! application supplies its own [`StateMachine`] implementation to give the
//! committed entries semantics.

use crate::error::Result;
use crate::types::LogIndex;

/// Stage 7.3 — owned, sendable handle that knows how to serialize a
/// previously-captured state-machine snapshot into bytes.
///
/// Returned by [`StateMachine::begin_snapshot`]. The two-phase API
/// exists so the driver can hold the state-machine lock JUST long
/// enough to capture an immutable view of state, release the lock,
/// then perform the (potentially expensive) bytes serialization on
/// the blocking pool without keeping other applies/queries waiting.
///
/// Implementations must be self-contained: any references into the
/// underlying state machine must be cloned or `Arc`-shared by
/// `begin_snapshot` BEFORE the lock is dropped. The `'static` bound
/// makes a sneaky borrow of `&self` a compile error.
pub trait SnapshotSerializer: Send + 'static {
    /// Serialize the captured state into snapshot bytes. Consumes
    /// `self` so a serializer cannot be replayed (and the captured
    /// state can be moved into the work without an extra clone).
    fn serialize(self: Box<Self>) -> Result<Vec<u8>>;
}

/// Backward-compat serializer used by the default
/// [`StateMachine::begin_snapshot`] implementation: holds the bytes
/// already produced by [`StateMachine::snapshot`] and returns them
/// verbatim.
pub struct EagerSerializer(pub Vec<u8>);

impl SnapshotSerializer for EagerSerializer {
    fn serialize(self: Box<Self>) -> Result<Vec<u8>> {
        Ok(self.0)
    }
}

/// Stage 7.3 (iter 9) — capability declaration for an SM's
/// [`StateMachine::begin_snapshot`] implementation, used by the
/// driver and by operators to scope the
/// "background-snapshot-nonblocking" client-latency SLA from
/// `architecture.md` §7 / `e2e-scenarios.md` Feature 15 / the
/// Stage 7.3 implementation plan.
///
/// The SLA — `client request latency does not spike above 2× baseline
/// during a background snapshot` — is achievable for ANY state machine
/// whose `begin_snapshot` is **non-blocking with respect to the
/// state-machine mutex** (i.e. holds the SM lock for `O(1)` /
/// bounded time). It is **NOT achievable in general** for state
/// machines that rely on the trait default `begin_snapshot`, which
/// eagerly serializes the entire state under the SM lock — for
/// such SMs the driver task is parked for the snapshot's full
/// wall-clock and propose response latency is bounded by the
/// snapshot duration rather than by 2× baseline.
///
/// The driver does NOT branch on this value (the snapshot pipeline
/// is identical for both modes); it is a documentation / contract
/// marker so operators can:
///
/// 1. Statically reason about which SMs meet the SLA without
///    reading the trait `impl`s.
/// 2. Write contract-asserting tests that `assert_eq!` the mode
///    BEFORE running an SLA regression, surfacing the boundary
///    when a regression test is run against the wrong SM kind.
///
/// Production state machines that need the SLA SHOULD override
/// [`StateMachine::begin_snapshot`] with a copy-on-write / shallow-
/// clone capture (see the `CoWKvStateMachine` example in this
/// module's tests) AND override
/// [`StateMachine::snapshot_capture_mode`] to return
/// `NonBlockingCapture` so the SLA contract is explicit at the
/// trait level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCaptureMode {
    /// `begin_snapshot` is bounded (`O(1)` / shallow-clone / CoW)
    /// and holds the SM lock only for the duration of the capture
    /// handshake — the serializer owns its immutable view by the
    /// time the lock is released. State machines in this mode
    /// meet the
    /// `background-snapshot-nonblocking` client-latency SLA: a
    /// concurrent `propose` issued during a snapshot completes
    /// within 2× baseline (see
    /// `scenario_background_snapshot_keeps_propose_latency_within_2x_baseline`
    /// in `xraft-server::driver`).
    NonBlockingCapture,
    /// `begin_snapshot` is `O(state-bytes)` — the trait default
    /// (which eagerly calls `self.snapshot()`) is in this mode.
    /// The SM lock is held for the snapshot's full wall-clock,
    /// so concurrent `apply` calls — and the `propose` responses
    /// that depend on them — are deferred until the snapshot
    /// capture completes. The reactor stays free (the work runs
    /// on the blocking pool), but the driver task is parked.
    /// State machines in this mode are EXPLICITLY OUT OF SCOPE
    /// for the
    /// `background-snapshot-nonblocking` client-latency SLA;
    /// production state machines that need the SLA must
    /// override `begin_snapshot` with a non-blocking capture
    /// AND override `snapshot_capture_mode` to return
    /// `NonBlockingCapture`.
    EagerMayStallDriver,
}

/// A deterministic state machine driven by committed log entries.
///
/// Consumers implement this trait and inject it at server startup. The driver
/// in `xraft-server` invokes the methods as follows:
///
/// * [`apply`](Self::apply) is called for each committed entry, in log order,
///   exactly once. The returned bytes are the serialised result of the applied
///   command — the embedded `read` API in `xraft-server` (see
///   `architecture.md` §2.4) uses this to support read-after-write patterns
///   (e.g. returning the new value produced by a `Put`).
/// * [`query`](Self::query) is the read-only path. The driver routes the
///   server's embedded `read` API requests through this method against the
///   already-committed state. `query` must not mutate the state machine.
/// * [`snapshot`](Self::snapshot) is called when the driver decides to take
///   a snapshot (log compaction trigger).
/// * [`restore`](Self::restore) is called when a snapshot is installed from a
///   leader, or on startup when replaying the most recent local snapshot
///   before tailing the log.
///
/// Implementations must be deterministic: given the same sequence of
/// `apply` calls (from the same starting state) every replica must produce
/// the same internal state and the same byte-for-byte responses. Non-determinism
/// here breaks the Raft safety guarantees.
pub trait StateMachine: Send + Sync {
    /// Apply a committed entry's payload to the state machine.
    ///
    /// `index` is the log position of the entry being applied; implementations
    /// can use it for idempotency checks or telemetry, but must not skip an
    /// apply based on it — the driver guarantees in-order, once-only delivery.
    ///
    /// Returns the serialised command result. May be empty when the command
    /// has no meaningful return value (e.g. a fire-and-forget write).
    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<Vec<u8>>;

    /// Run a read-only query against committed state.
    ///
    /// Implementations MUST NOT mutate state from this method. The driver
    /// invokes `query` against the state machine state currently applied on
    /// the serving driver instance (i.e. at `last_applied >= prior commit
    /// index` for the same leader).
    ///
    /// In v1 this is an apply-cursor read on the serving leader: it sees
    /// every entry the local driver has applied at call time, but does not
    /// perform a quorum confirmation (no ReadIndex / leader lease). Callers
    /// that need cross-leader linearizable reads must wait for a future
    /// ReadIndex / leader-lease protocol — see `tech-spec.md` §2.6.
    fn query(&self, query: &[u8]) -> Result<Vec<u8>>;

    /// Take a snapshot of the current state machine state.
    ///
    /// The returned bytes are opaque to XRAFT; they will be handed back to
    /// [`restore`](Self::restore) on a peer (or on this node after a restart).
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Stage 7.3 — two-phase snapshot capture for non-blocking
    /// background snapshots.
    ///
    /// The driver acquires the state-machine lock, calls
    /// `begin_snapshot` (which should be O(state) — typically a shallow
    /// clone of an `Arc`-shared inner state), DROPS THE LOCK, and then
    /// calls [`SnapshotSerializer::serialize`] on the returned handle
    /// from a `tokio::task::spawn_blocking` worker. The expensive
    /// serialization work therefore runs WITHOUT holding the SM lock,
    /// so concurrent `apply` / `query` calls proceed at full speed.
    ///
    /// **Snapshot consistency contract (iter-5):** the serializer
    /// returned by `begin_snapshot` MUST capture an immutable view of
    /// the state as of the call to `begin_snapshot`. Applies that
    /// happen AFTER `begin_snapshot` returns MUST NOT appear in the
    /// bytes returned by [`SnapshotSerializer::serialize`].
    /// Implementations satisfy this either by deep-cloning the
    /// captured state into the serializer (simplest), or by using a
    /// persistent / copy-on-write data structure where the serializer
    /// holds a structurally-shared snapshot that is decoupled from
    /// further mutations. Returning a live `Arc<Mutex<_>>` of the
    /// underlying state is INCORRECT — post-begin applies would
    /// appear in the snapshot, violating Raft's `last_included_index`
    /// contract (the snapshot is supposed to cover entries `[..= idx]`,
    /// not `[..= idx] + whatever-else-the-scheduler-let-through`).
    ///
    /// The default implementation performs the legacy in-lock
    /// serialization by calling `self.snapshot()` directly and wrapping
    /// the resulting bytes in an [`EagerSerializer`] — which trivially
    /// satisfies the immutability contract because the bytes are
    /// produced before the SM lock is released.
    ///
    /// **Stage 7.3 (iter 9) — non-blocking SLA boundary.** The
    /// default impl is EXPLICITLY OUT OF SCOPE for the
    /// "background-snapshot-nonblocking" client-latency SLA from
    /// `architecture.md` §7 / `e2e-scenarios.md` Feature 15: it
    /// runs `self.snapshot()` under the SM mutex, so a slow
    /// `snapshot()` (anything more than a few ms for any
    /// non-trivial state) parks the driver task and defers
    /// concurrent `propose` responses for the snapshot's full
    /// wall-clock. The reactor itself stays free — the heavy
    /// work runs on `tokio::task::spawn_blocking` — but a
    /// single-voter cluster's `propose` -> commit -> apply path
    /// shares the SM mutex with the in-flight snapshot, so the
    /// `apply` waits, which in turn defers the `propose`
    /// completion. Production state machines that need the SLA
    /// MUST override this method with a copy-on-write / shallow
    /// clone capture (see [`super::state_machine::tests::CoWKvStateMachine`]
    /// for the canonical pattern) and ALSO override
    /// [`StateMachine::snapshot_capture_mode`] to return
    /// [`SnapshotCaptureMode::NonBlockingCapture`] so the contract
    /// is explicit at the trait level. The driver does not branch
    /// on this value — it is a documentation gate, and tests can
    /// `assert_eq!(sm.snapshot_capture_mode(),
    /// SnapshotCaptureMode::NonBlockingCapture)` before running an
    /// SLA regression to surface the boundary at test setup time
    /// rather than via a flaky latency assertion.
    fn begin_snapshot(&self) -> Result<Box<dyn SnapshotSerializer>> {
        let bytes = self.snapshot()?;
        Ok(Box::new(EagerSerializer(bytes)))
    }

    /// Stage 7.3 (iter 9) — declare the non-blocking SLA
    /// capability of this state machine's [`begin_snapshot`]
    /// implementation. See [`SnapshotCaptureMode`] for the full
    /// contract and rationale.
    ///
    /// The trait default returns
    /// [`SnapshotCaptureMode::EagerMayStallDriver`] because the
    /// default `begin_snapshot` impl above eagerly serializes
    /// under the SM lock and is therefore explicitly OUT OF
    /// SCOPE for the
    /// "background-snapshot-nonblocking" client-latency SLA.
    /// Production state machines that override `begin_snapshot`
    /// with a bounded / CoW capture should override this method
    /// to return [`SnapshotCaptureMode::NonBlockingCapture`].
    fn snapshot_capture_mode(&self) -> SnapshotCaptureMode {
        SnapshotCaptureMode::EagerMayStallDriver
    }

    /// Restore the state machine from a snapshot.
    ///
    /// After this call returns `Ok`, the state machine must behave as if the
    /// sequence of entries captured in the snapshot had been applied in order.
    fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

/// A minimal no-op state machine that discards applied entries.
///
/// Used for testing and as a baseline default. Both [`apply`] and [`query`]
/// log the call at `tracing::debug!` level and return an empty `Vec<u8>`.
/// Snapshots are empty by construction and `restore` is a no-op.
///
/// [`apply`]: StateMachine::apply
/// [`query`]: StateMachine::query
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpStateMachine;

impl StateMachine for NoOpStateMachine {
    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<Vec<u8>> {
        tracing::debug!(
            target: "xraft_core::state_machine",
            index = index.0,
            command_len = command.len(),
            "NoOpStateMachine: apply (discarded)"
        );
        Ok(Vec::new())
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
        tracing::debug!(
            target: "xraft_core::state_machine",
            query_len = query.len(),
            "NoOpStateMachine: query (empty result)"
        );
        Ok(Vec::new())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        tracing::debug!(
            target: "xraft_core::state_machine",
            "NoOpStateMachine: snapshot (empty)"
        );
        Ok(Vec::new())
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        tracing::debug!(
            target: "xraft_core::state_machine",
            snapshot_len = snapshot.len(),
            "NoOpStateMachine: restore (discarded)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Concrete state machine used to exercise the snapshot/restore
    /// roundtrip contract. Treats each command as a `key=value` UTF-8
    /// assignment and `query` as a key lookup. Snapshots are serialised
    /// with `bincode` so the roundtrip is byte-stable.
    #[derive(Default)]
    struct KvStateMachine {
        kv: BTreeMap<String, String>,
        last_applied: LogIndex,
    }

    impl KvStateMachine {
        fn parse(command: &[u8]) -> (String, String) {
            let s = std::str::from_utf8(command).expect("test command must be utf8");
            let (k, v) = s.split_once('=').expect("test command must be key=value");
            (k.to_string(), v.to_string())
        }
    }

    impl StateMachine for KvStateMachine {
        fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<Vec<u8>> {
            let (k, v) = Self::parse(command);
            self.kv.insert(k, v.clone());
            self.last_applied = index;
            Ok(v.into_bytes())
        }

        fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
            let key = std::str::from_utf8(query).expect("test query must be utf8");
            Ok(self
                .kv
                .get(key)
                .map(|v| v.as_bytes().to_vec())
                .unwrap_or_default())
        }

        fn snapshot(&self) -> Result<Vec<u8>> {
            let payload: Vec<(String, String)> = self
                .kv
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            bincode::serialize(&(self.last_applied.0, payload))
                .map_err(|e| crate::error::XRaftError::Storage(format!("kv snapshot: {e}")))
        }

        fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
            let (last_applied, payload): (u64, Vec<(String, String)>) =
                bincode::deserialize(snapshot)
                    .map_err(|e| crate::error::XRaftError::Storage(format!("kv restore: {e}")))?;
            self.last_applied = LogIndex(last_applied);
            self.kv = payload.into_iter().collect();
            Ok(())
        }
    }

    #[test]
    fn noop_apply_returns_empty_for_ten_entries() {
        // Scenario: noop-apply — applying 10 entries returns Ok(empty Vec<u8>)
        // for each call and produces no error.
        let mut sm = NoOpStateMachine;
        for i in 1..=10u64 {
            let out = sm
                .apply(LogIndex(i), format!("entry-{i}").as_bytes())
                .expect("apply must not fail");
            assert!(out.is_empty(), "NoOp apply must return empty bytes");
        }
    }

    #[test]
    fn noop_query_returns_empty() {
        // Scenario: noop-query — any query returns Ok(empty Vec<u8>).
        let sm = NoOpStateMachine;
        let out = sm.query(b"anything").expect("query must not fail");
        assert!(out.is_empty(), "NoOp query must return empty bytes");

        let out_empty = sm.query(&[]).expect("query of empty bytes must not fail");
        assert!(out_empty.is_empty());
    }

    #[test]
    fn noop_snapshot_restore_roundtrip_is_noop() {
        let sm = NoOpStateMachine;
        let snap = sm.snapshot().expect("snapshot must not fail");
        assert!(snap.is_empty());
        let mut sm2 = NoOpStateMachine;
        sm2.restore(&snap).expect("restore must not fail");
        assert!(sm2.query(b"k").unwrap().is_empty());
    }

    #[test]
    fn snapshot_restore_roundtrip_preserves_state() {
        // Scenario: snapshot-restore-roundtrip — a snapshot taken from the
        // original instance, when fed to `restore` on a fresh instance,
        // yields a state machine whose queries match the original.
        let mut original = KvStateMachine::default();
        original.apply(LogIndex(1), b"alpha=1").unwrap();
        original.apply(LogIndex(2), b"beta=two").unwrap();
        original.apply(LogIndex(3), b"alpha=overwrite").unwrap();

        let snap = original.snapshot().expect("snapshot must not fail");
        assert!(!snap.is_empty(), "non-trivial state should serialise");

        let mut restored = KvStateMachine::default();
        restored.restore(&snap).expect("restore must not fail");

        // Equivalent state: every query returns the same bytes as on the
        // original.
        for key in ["alpha", "beta", "missing"] {
            let lhs = original.query(key.as_bytes()).unwrap();
            let rhs = restored.query(key.as_bytes()).unwrap();
            assert_eq!(lhs, rhs, "query result mismatch for key {key}");
        }
        assert_eq!(original.last_applied, restored.last_applied);
    }

    #[test]
    fn apply_returns_command_result_bytes() {
        // The architecture.md §4.1 signature requires apply to return the
        // serialised command result — exercise that path with the concrete
        // KvStateMachine so the contract is testable, not just structural.
        let mut sm = KvStateMachine::default();
        let result = sm.apply(LogIndex(1), b"k=v").expect("apply must not fail");
        assert_eq!(result, b"v");
    }

    /// State machine whose `begin_snapshot` performs an **immutable
    /// snapshot capture**: it briefly locks the inner state, deep-
    /// clones the BTreeMap into an owned `Snapshot`, then drops the
    /// lock. The serializer holds the cloned map, so post-begin
    /// applies on the live state machine cannot influence the bytes
    /// produced by `serialize()`. This is the correct CoW pattern for
    /// satisfying the Stage 7.3 / iter-5 immutability contract on
    /// `StateMachine::begin_snapshot`.
    #[derive(Default)]
    struct CoWKvStateMachine {
        inner: std::sync::Arc<std::sync::Mutex<BTreeMap<String, String>>>,
    }

    /// Owned, immutable view captured at `begin_snapshot` time. No
    /// shared references back into the live state machine.
    struct CoWSerializer {
        captured: BTreeMap<String, String>,
    }

    impl SnapshotSerializer for CoWSerializer {
        fn serialize(self: Box<Self>) -> Result<Vec<u8>> {
            let payload: Vec<(String, String)> = self.captured.into_iter().collect();
            bincode::serialize(&payload)
                .map_err(|e| crate::error::XRaftError::Storage(format!("cow snapshot: {e}")))
        }
    }

    impl StateMachine for CoWKvStateMachine {
        fn apply(&mut self, _index: LogIndex, command: &[u8]) -> Result<Vec<u8>> {
            let s = std::str::from_utf8(command).unwrap();
            let (k, v) = s.split_once('=').unwrap();
            self.inner
                .lock()
                .unwrap()
                .insert(k.to_string(), v.to_string());
            Ok(Vec::new())
        }
        fn query(&self, _query: &[u8]) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn snapshot(&self) -> Result<Vec<u8>> {
            let g = self.inner.lock().unwrap();
            let payload: Vec<(String, String)> =
                g.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            bincode::serialize(&payload)
                .map_err(|e| crate::error::XRaftError::Storage(format!("kv snapshot: {e}")))
        }
        fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
            let payload: Vec<(String, String)> = bincode::deserialize(snapshot)
                .map_err(|e| crate::error::XRaftError::Storage(format!("cow restore: {e}")))?;
            *self.inner.lock().unwrap() = payload.into_iter().collect();
            Ok(())
        }
        fn begin_snapshot(&self) -> Result<Box<dyn SnapshotSerializer>> {
            // Brief lock + deep-clone of the inner map = an immutable
            // owned view. The serializer holds this owned copy and is
            // fully decoupled from subsequent applies on `self.inner`.
            let captured = self.inner.lock().unwrap().clone();
            Ok(Box::new(CoWSerializer { captured }))
        }
        fn snapshot_capture_mode(&self) -> SnapshotCaptureMode {
            // The CoW `begin_snapshot` above performs an
            // `Arc<Mutex<_>>::lock().clone()` — bounded in the SM
            // lock — so this SM is in scope for the Stage 7.3
            // non-blocking client-latency SLA.
            SnapshotCaptureMode::NonBlockingCapture
        }
    }

    #[test]
    fn default_begin_snapshot_serializes_eagerly() {
        // The default `begin_snapshot` impl invokes `snapshot()` directly
        // (legacy in-lock behaviour). Verify the wrapper returns the same
        // bytes the eager `snapshot()` call would.
        let mut sm = KvStateMachine::default();
        sm.apply(LogIndex(1), b"a=1").unwrap();
        sm.apply(LogIndex(2), b"b=2").unwrap();
        let eager_bytes = sm.snapshot().unwrap();
        let via_begin = sm.begin_snapshot().unwrap().serialize().unwrap();
        assert_eq!(eager_bytes, via_begin);
    }

    #[test]
    fn cow_begin_snapshot_captures_immutable_view_post_begin_applies_excluded() {
        // Iter-5 evaluator item 1 — the immutability contract test.
        //
        // Scenario: a state machine implements `begin_snapshot` to
        // capture an OWNED clone of the inner state. After
        // `begin_snapshot` returns, the SM is fully mutable —
        // concurrent applies MUST succeed AND must NOT influence
        // the bytes returned by the serializer's `serialize()` call.
        //
        // Prior (iter-4) impl returned a live `Arc<Mutex<_>>`, so
        // post-begin applies leaked into the snapshot. The evaluator
        // flagged this in iter-4 item 1: snapshots could include
        // entries beyond their advertised `last_included_index`,
        // violating Raft's snapshot semantics.
        let mut sm = CoWKvStateMachine::default();
        sm.apply(LogIndex(1), b"alpha=1").unwrap();
        sm.apply(LogIndex(2), b"beta=2").unwrap();

        // Phase 1: capture (would happen under SM lock in driver).
        let serializer = sm.begin_snapshot().expect("begin_snapshot");

        // Phase 2 (without holding any lock): mutate the SM. This is
        // the "post-begin apply" that the immutability contract
        // forbids from appearing in the serialized snapshot.
        sm.apply(LogIndex(3), b"gamma=POST_BEGIN").unwrap();
        sm.apply(LogIndex(4), b"alpha=POST_BEGIN_OVERWRITE")
            .unwrap();

        // Phase 3: serialize the captured view (still without any
        // SM lock — proves the decoupling).
        let bytes = serializer.serialize().expect("serialize");

        // Restore into a fresh SM and inspect.
        let mut restored = CoWKvStateMachine::default();
        restored.restore(&bytes).expect("restore");
        let restored_state = restored.inner.lock().unwrap();

        // Entries written BEFORE begin_snapshot are present.
        assert_eq!(restored_state.get("alpha"), Some(&"1".to_string()));
        assert_eq!(restored_state.get("beta"), Some(&"2".to_string()));

        // Entry written AFTER begin_snapshot is NOT in the snapshot.
        assert_eq!(
            restored_state.get("gamma"),
            None,
            "post-begin_snapshot apply leaked into snapshot — immutability contract violated"
        );

        // Overwrite-after-begin must NOT clobber the captured value.
        assert_eq!(
            restored_state.get("alpha"),
            Some(&"1".to_string()),
            "post-begin_snapshot overwrite of 'alpha' leaked into snapshot"
        );

        // Exactly the pre-begin entries (alpha=1, beta=2) and
        // nothing else.
        assert_eq!(
            restored_state.len(),
            2,
            "snapshot has {} entries, expected exactly 2 (alpha + beta as of begin_snapshot)",
            restored_state.len()
        );
    }

    #[test]
    fn snapshot_capture_mode_defaults_to_eager_may_stall_driver() {
        // Stage 7.3 (iter 9) — the trait default `begin_snapshot`
        // serializes eagerly under the SM lock and is EXPLICITLY
        // OUT OF SCOPE for the `background-snapshot-nonblocking`
        // client-latency SLA. The trait default
        // `snapshot_capture_mode` must surface this as
        // `EagerMayStallDriver` so operators / tests can detect
        // the boundary without re-reading every `impl`.
        let sm = KvStateMachine::default();
        assert_eq!(
            sm.snapshot_capture_mode(),
            SnapshotCaptureMode::EagerMayStallDriver,
            "trait default begin_snapshot eagerly serializes under the SM lock; \
             snapshot_capture_mode must surface this as EagerMayStallDriver",
        );

        // The NoOp SM also inherits the default
        // `snapshot_capture_mode` — it never overrode it, and
        // although its `snapshot()` is trivially fast, the trait
        // default is the safer reported mode (operators should
        // not infer "non-blocking" from absence of an override).
        let noop = NoOpStateMachine;
        assert_eq!(
            noop.snapshot_capture_mode(),
            SnapshotCaptureMode::EagerMayStallDriver,
        );
    }

    #[test]
    fn cow_snapshot_capture_mode_is_non_blocking() {
        // Stage 7.3 (iter 9) — the canonical CoW pattern
        // (`CoWKvStateMachine`) overrides `begin_snapshot` with a
        // bounded shallow-clone capture AND overrides
        // `snapshot_capture_mode` to declare its
        // `NonBlockingCapture` capability. SLA regression tests
        // (e.g. `scenario_background_snapshot_keeps_propose_latency_within_2x_baseline`)
        // can `assert_eq!` this before measuring latency so the
        // contract is enforced at the trait level rather than via
        // a flaky wall-clock assertion alone.
        let sm = CoWKvStateMachine::default();
        assert_eq!(
            sm.snapshot_capture_mode(),
            SnapshotCaptureMode::NonBlockingCapture,
        );
    }
}
