//! Test [`StateMachine`] implementations used by the Stage 8.1
//! multi-node integration tests.
//!
//! These types deliberately live in the public surface of `xraft-test`
//! so external workstreams (Stage 8.2 chaos / 8.3 linearisability) can
//! reuse the same observer-style state machine when asserting on
//! per-node apply order.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use xraft_core::error::Result;
use xraft_core::state_machine::StateMachine;
use xraft_core::types::LogIndex;

/// A `StateMachine` that records every applied command into a shared
/// `Vec<(LogIndex, Vec<u8>)>`, in the order the driver delivers them.
///
/// The recorded log is held behind an `Arc<Mutex<_>>` so tests can
/// snapshot it from outside the driver loop after waiting for
/// convergence. The shared handle is obtained via
/// [`RecordingStateMachine::handle`] before the SM is moved into the
/// `Driver` — once the driver owns the SM, the shared handle is the
/// only inspection surface available.
///
/// `query` returns the BIG-endian-encoded `last_applied` index so a
/// caller-facing `read()` can confirm SM progress without a separate
/// observation channel. The empty-byte query path returns the
/// number of applied entries.
///
/// `snapshot` / `restore` are byte-stable roundtrips: a `bincode`
/// serialisation of the full applied vector. This is sufficient for
/// every Stage 8.1 scenario (partition-recovery test installs a
/// snapshot when a follower has fallen far behind), and matches the
/// canonical `StateMachine` contract that `restore(snapshot(x)) == x`.
#[derive(Debug, Clone, Default)]
pub struct RecordingStateMachine {
    inner: Arc<Mutex<RecordingInner>>,
    /// Bumped on every `apply()` so the
    /// [`RecordingHandle::await_applied_at_least`] /
    /// [`crate::simulated::SimulatedCluster::await_applied_at_least`]
    /// wait loops are EVENT-DRIVEN instead of fixed-cadence polling.
    /// Held alongside (not inside) `inner` so the waiter can be
    /// registered without holding the inner mutex across `.await`.
    state_change: Arc<Notify>,
}

#[derive(Debug, Default)]
struct RecordingInner {
    /// `(index, command_bytes)` for every entry the driver has applied.
    /// Drives both the equivalence assertions in tests and the snapshot
    /// roundtrip payload.
    applied: Vec<(u64, Vec<u8>)>,
}

/// Shared, cheaply-cloneable inspection handle for a
/// [`RecordingStateMachine`]. Obtain via
/// [`RecordingStateMachine::handle`] BEFORE moving the SM into the
/// driver — once the driver owns it, this is the only way to observe
/// applied entries.
#[derive(Debug, Clone)]
pub struct RecordingHandle {
    inner: Arc<Mutex<RecordingInner>>,
    /// Shared with the owning [`RecordingStateMachine`] so the
    /// event-driven [`Self::await_applied_at_least`] wait wakes on
    /// every `apply()`.
    state_change: Arc<Notify>,
}

impl RecordingStateMachine {
    /// Build a fresh, empty recording state machine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a recording SM whose `apply` events bump `state_change`.
    /// Used by the [`crate::simulated::SimulatedCluster`] harness to
    /// share ONE notify across every node's recording so a
    /// cluster-level wait loop wakes the instant ANY node applies.
    pub fn with_state_change(state_change: Arc<Notify>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecordingInner::default())),
            state_change,
        }
    }

    /// Borrow a shared inspection handle. Cheap clone.
    pub fn handle(&self) -> RecordingHandle {
        RecordingHandle {
            inner: self.inner.clone(),
            state_change: self.state_change.clone(),
        }
    }
}

impl RecordingHandle {
    /// Number of entries currently applied to the SM.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("recording sm poisoned")
            .applied
            .len()
    }

    /// Whether nothing has been applied yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot the applied `(index, bytes)` sequence in order.
    pub fn applied(&self) -> Vec<(u64, Vec<u8>)> {
        self.inner
            .lock()
            .expect("recording sm poisoned")
            .applied
            .clone()
    }

    /// The last log index the SM has applied, or 0 when empty.
    pub fn last_applied(&self) -> u64 {
        self.inner
            .lock()
            .expect("recording sm poisoned")
            .applied
            .last()
            .map(|(i, _)| *i)
            .unwrap_or(0)
    }

    /// Wait until the SM has applied at least `target` entries OR
    /// until `deadline` elapses. Returns `Ok(())` on success, or the
    /// observed count on timeout.
    ///
    /// # Event-driven wait
    ///
    /// A naive poll-based implementation would sleep `5 ms` between
    /// polls — a fixed wall-clock cadence that compounds under
    /// workspace-parallel scheduler pressure. This loop is wired to
    /// the shared
    /// [`Notify`](tokio::sync::Notify) bumped by every
    /// [`RecordingStateMachine::apply`] call, so the loop wakes the
    /// instant an entry is applied. A `50 ms` periodic safety-net
    /// wake defends against the (theoretically eliminated by
    /// `Notified::enable`) "notify fires between check and wait"
    /// race and bounds the deadline-check granularity.
    pub async fn await_applied_at_least(
        &self,
        target: usize,
        deadline: std::time::Duration,
    ) -> std::result::Result<(), usize> {
        let start = tokio::time::Instant::now();
        loop {
            // Register the waiter BEFORE checking the predicate so a
            // notify fired between check and wait is not lost.
            let waiter = self.state_change.notified();
            tokio::pin!(waiter);
            waiter.as_mut().enable();

            let observed = self.len();
            if observed >= target {
                return Ok(());
            }
            let elapsed = start.elapsed();
            if elapsed >= deadline {
                return Err(observed);
            }
            let remaining = deadline - elapsed;
            let wake_after = std::time::Duration::from_millis(50).min(remaining);
            tokio::select! {
                _ = &mut waiter => {}
                _ = tokio::time::sleep(wake_after) => {}
            }
        }
    }
}

impl StateMachine for RecordingStateMachine {
    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<Vec<u8>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| xraft_core::error::XRaftError::Storage(format!("recording sm: {e}")))?;
        guard.applied.push((index.0, command.to_vec()));
        drop(guard);
        // Bump the shared notify so the
        // event-driven `await_applied_at_least` wait wakes
        // immediately on every apply.
        self.state_change.notify_waiters();
        Ok(command.to_vec())
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| xraft_core::error::XRaftError::Storage(format!("recording sm: {e}")))?;
        if query.is_empty() {
            Ok((guard.applied.len() as u64).to_be_bytes().to_vec())
        } else {
            let last = guard.applied.last().map(|(i, _)| *i).unwrap_or(0);
            Ok(last.to_be_bytes().to_vec())
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| xraft_core::error::XRaftError::Storage(format!("recording sm: {e}")))?;
        bincode::serialize(&guard.applied)
            .map_err(|e| xraft_core::error::XRaftError::Storage(format!("recording sm: {e}")))
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        let applied: Vec<(u64, Vec<u8>)> = bincode::deserialize(snapshot)
            .map_err(|e| xraft_core::error::XRaftError::Storage(format!("recording sm: {e}")))?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| xraft_core::error::XRaftError::Storage(format!("recording sm: {e}")))?;
        guard.applied = applied;
        drop(guard);
        // A snapshot install can leap the applied count forward;
        // wake any event-driven waiter so it observes the new state.
        self.state_change.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_records_entries_in_order() {
        let mut sm = RecordingStateMachine::new();
        let h = sm.handle();
        sm.apply(LogIndex(1), b"a").unwrap();
        sm.apply(LogIndex(2), b"b").unwrap();
        sm.apply(LogIndex(3), b"c").unwrap();
        let applied = h.applied();
        assert_eq!(applied.len(), 3);
        assert_eq!(applied[0], (1, b"a".to_vec()));
        assert_eq!(applied[2], (3, b"c".to_vec()));
    }

    #[test]
    fn snapshot_restore_roundtrip() {
        let mut sm = RecordingStateMachine::new();
        sm.apply(LogIndex(1), b"alpha").unwrap();
        sm.apply(LogIndex(2), b"beta").unwrap();
        let bytes = sm.snapshot().unwrap();

        let mut other = RecordingStateMachine::new();
        other.restore(&bytes).unwrap();
        assert_eq!(other.handle().applied(), sm.handle().applied());
    }
}
