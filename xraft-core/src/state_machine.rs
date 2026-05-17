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
}
