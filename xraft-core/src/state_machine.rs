//! State machine callback trait — applied by the consensus layer after commit.
//!
//! The trait name and signatures follow `architecture.md` §4.1, which is
//! authoritative for trait definitions. This is the extension point for
//! consumers of XRAFT: the library provides the replicated log, and the
//! application supplies its own [`StateMachine`] implementation to give the
//! committed entries semantics.

use crate::error::Result;
use crate::types::LogIndex;

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
    /// guarantees the state machine has caught up to at least the read's
    /// required commit index before invoking `query` (linearizable reads).
    fn query(&self, query: &[u8]) -> Result<Vec<u8>>;

    /// Take a snapshot of the current state machine state.
    ///
    /// The returned bytes are opaque to XRAFT; they will be handed back to
    /// [`restore`](Self::restore) on a peer (or on this node after a restart).
    fn snapshot(&self) -> Result<Vec<u8>>;

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

// Blanket impl so `Box<dyn StateMachine + Send + Sync>` itself satisfies the
// `StateMachine` bound used by the driver (`SM: StateMachine + Send + Sync +
// 'static`). Without this, the trait is only object-safe in the compile-time
// sense — downstream consumers that want runtime dispatch (e.g. selecting a
// state machine from configuration at startup) cannot plug a boxed
// implementation into the driver's generic API.
impl<T: StateMachine + ?Sized> StateMachine for Box<T> {
    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<Vec<u8>> {
        (**self).apply(index, command)
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
        (**self).query(query)
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        (**self).snapshot()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        (**self).restore(snapshot)
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

    #[test]
    fn state_machine_trait_is_object_safe() {
        // The driver dispatches on a generic `SM: StateMachine` today, but
        // downstream consumers (e.g. a host that needs to pick the state
        // machine at runtime from configuration) must be able to hold the
        // implementation behind `Box<dyn StateMachine + Send + Sync>`.
        // This test fails to compile if a future trait change accidentally
        // breaks object safety (e.g. adding a generic method or `Self`-by-
        // value receiver).
        let mut boxed: Box<dyn StateMachine + Send + Sync> = Box::new(NoOpStateMachine);
        let _ = boxed.apply(LogIndex(1), b"cmd").expect("apply via dyn");
        let _ = boxed.query(b"q").expect("query via dyn");
        let snap = boxed.snapshot().expect("snapshot via dyn");
        boxed.restore(&snap).expect("restore via dyn");
    }

    #[test]
    fn noop_state_machine_satisfies_send_and_sync_bounds() {
        // Compile-time check: NoOpStateMachine must satisfy the supertrait
        // bounds (`Send + Sync`) declared on `StateMachine`. The driver
        // crosses tokio task boundaries and would fail to bind a non-Send
        // implementation; catch that regression here rather than in the
        // server crate's downstream tests.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NoOpStateMachine>();
    }

    #[test]
    fn snapshot_restore_is_idempotent_across_multiple_cycles() {
        // Beyond the single-pass roundtrip, a state machine must remain
        // equivalent after repeated snapshot→restore cycles. This catches
        // bugs where `snapshot` leaks transient state (e.g. an apply cursor)
        // or `restore` fails to clear prior state on a non-fresh instance.
        let mut original = KvStateMachine::default();
        original.apply(LogIndex(1), b"k1=v1").unwrap();
        original.apply(LogIndex(2), b"k2=v2").unwrap();

        let snap_a = original.snapshot().expect("snap A");

        let mut hop = KvStateMachine::default();
        hop.restore(&snap_a).expect("restore A");
        let snap_b = hop.snapshot().expect("snap B");

        let mut final_sm = KvStateMachine::default();
        final_sm.restore(&snap_b).expect("restore B");

        // After two snapshot→restore hops the third instance must answer
        // queries identically to the first and the snapshot bytes must be
        // byte-stable across hops.
        assert_eq!(
            snap_a, snap_b,
            "snapshot bytes must be stable across cycles"
        );
        for key in ["k1", "k2", "missing"] {
            assert_eq!(
                original.query(key.as_bytes()).unwrap(),
                final_sm.query(key.as_bytes()).unwrap(),
                "query mismatch after multi-cycle restore for {key}"
            );
        }
        assert_eq!(original.last_applied, final_sm.last_applied);
    }

    #[test]
    fn apply_sequence_is_deterministic_across_independent_instances() {
        // Determinism is a Raft safety prerequisite (see trait docstring):
        // two replicas given the same apply sequence from the same starting
        // state must produce identical internal state. Exercise that with
        // independent KvStateMachine instances and compare their snapshots.
        let commands: &[(u64, &[u8])] = &[
            (1, b"alpha=1"),
            (2, b"beta=2"),
            (3, b"alpha=overwrite"),
            (4, b"gamma=3"),
        ];

        let mut a = KvStateMachine::default();
        let mut b = KvStateMachine::default();
        let mut results_a = Vec::new();
        let mut results_b = Vec::new();
        for (idx, cmd) in commands {
            results_a.push(a.apply(LogIndex(*idx), cmd).unwrap());
            results_b.push(b.apply(LogIndex(*idx), cmd).unwrap());
        }

        assert_eq!(results_a, results_b, "apply results must be deterministic");
        assert_eq!(
            a.snapshot().unwrap(),
            b.snapshot().unwrap(),
            "snapshots of equivalent states must be byte-equal"
        );
    }

    #[test]
    fn restore_replaces_prior_state_rather_than_merging() {
        // `restore` semantics from the trait docstring: after the call the
        // state machine must behave as if only the captured snapshot's
        // entries had been applied. Any data the target instance had before
        // the restore must be wiped — guard against a merge-style restore
        // that would silently corrupt cluster state.
        let mut donor = KvStateMachine::default();
        donor.apply(LogIndex(1), b"donor_key=donor_value").unwrap();
        let snap = donor.snapshot().unwrap();

        let mut target = KvStateMachine::default();
        target
            .apply(LogIndex(99), b"stale_key=stale_value")
            .unwrap();
        target.restore(&snap).expect("restore must not fail");

        assert_eq!(
            target.query(b"stale_key").unwrap(),
            Vec::<u8>::new(),
            "restore must drop stale_key, not merge it"
        );
        assert_eq!(
            target.query(b"donor_key").unwrap(),
            b"donor_value",
            "restore must populate keys from the snapshot"
        );
        assert_eq!(
            target.last_applied,
            LogIndex(1),
            "last_applied must reflect the snapshot, not the prior local value"
        );
    }

    #[test]
    fn noop_apply_accepts_empty_command_bytes() {
        // Edge case: the driver may emit a zero-byte command (e.g. a no-op
        // entry committed for leader-lease purposes). `apply` must accept
        // an empty slice without panic and still return Ok(empty).
        let mut sm = NoOpStateMachine;
        let out = sm.apply(LogIndex(1), &[]).expect("apply must accept empty");
        assert!(out.is_empty());
    }

    #[test]
    fn boxed_dyn_state_machine_implements_state_machine() {
        // The blanket `impl<T: StateMachine + ?Sized> StateMachine for Box<T>`
        // must let a `Box<dyn StateMachine + Send + Sync>` itself satisfy the
        // `StateMachine` bound the driver uses (`SM: StateMachine + Send +
        // Sync + 'static`). This is what makes the trait usable for runtime
        // dispatch by downstream consumers (e.g. picking the implementation
        // at startup from configuration), not just the object-safety check
        // in `state_machine_trait_is_object_safe`.
        fn accepts_state_machine<SM: StateMachine + Send + Sync + 'static>(
            mut sm: SM,
        ) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
            let applied = sm.apply(LogIndex(7), b"k=v").expect("boxed apply");
            let queried = sm.query(b"k").expect("boxed query");
            let snap = sm.snapshot().expect("boxed snapshot");
            sm.restore(&snap).expect("boxed restore");
            (applied, queried, snap)
        }

        let boxed: Box<dyn StateMachine + Send + Sync> = Box::new(KvStateMachine::default());
        let (applied, _queried, snap) = accepts_state_machine(boxed);
        // KvStateMachine returns the value bytes from apply.
        assert_eq!(applied, b"v");
        // Snapshot must be non-empty since we inserted one key before restore.
        assert!(!snap.is_empty());
    }

    #[test]
    fn boxed_dyn_state_machine_forwards_to_inner_impl() {
        // Verify the blanket impl actually forwards each method to the inner
        // T rather than silently no-op'ing. We use a stateful KvStateMachine
        // wrapped in Box<dyn> and confirm apply mutates the inner state and
        // query observes the mutation through the box.
        let mut boxed: Box<dyn StateMachine + Send + Sync> = Box::new(KvStateMachine::default());
        let applied = boxed.apply(LogIndex(1), b"alpha=one").expect("apply");
        assert_eq!(
            applied, b"one",
            "blanket impl must surface inner apply result"
        );

        let queried = boxed.query(b"alpha").expect("query");
        assert_eq!(
            queried, b"one",
            "blanket impl must route query to inner state"
        );

        let snap = boxed.snapshot().expect("snapshot");
        let mut fresh: Box<dyn StateMachine + Send + Sync> = Box::new(KvStateMachine::default());
        fresh.restore(&snap).expect("restore");
        assert_eq!(
            fresh.query(b"alpha").expect("query fresh"),
            b"one",
            "restore through Box must repopulate the inner state machine"
        );
    }
}
