//! State machine callback trait — applied by the consensus layer after commit.
//!
//! Named `StateMachineCallback` per `implementation-plan.md` Stage 5.1.
//! This is the extension point for consumers: XRAFT provides the replicated
//! log; the application supplies its own state machine implementation.

use crate::error::Result;
use crate::types::LogIndex;

/// A deterministic state machine driven by committed log entries.
///
/// Consumers implement this trait and inject it at server startup.
/// The driver calls `apply` for each committed entry in log order,
/// `snapshot` when a snapshot is triggered, and `restore` when
/// installing a snapshot received from the leader.
pub trait StateMachineCallback: Send + Sync {
    /// Apply a committed entry's payload to the state machine.
    ///
    /// `index` is the log position of the entry being applied.
    fn apply(&mut self, index: LogIndex, entry: &[u8]) -> Result<()>;

    /// Take a snapshot of the current state machine state.
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Restore the state machine from a snapshot.
    fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

/// A minimal no-op state machine that discards applied entries.
///
/// Used for testing and as a baseline. Logs applied entries via `tracing`.
pub struct NoOpStateMachine;

impl StateMachineCallback for NoOpStateMachine {
    fn apply(&mut self, index: LogIndex, entry: &[u8]) -> Result<()> {
        tracing::debug!(
            index = index.0,
            entry_len = entry.len(),
            "NoOpStateMachine: apply (discarded)"
        );
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        tracing::debug!("NoOpStateMachine: snapshot (empty)");
        Ok(Vec::new())
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        tracing::debug!(
            snapshot_len = snapshot.len(),
            "NoOpStateMachine: restore (discarded)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_apply_returns_ok() {
        let mut sm = NoOpStateMachine;
        for i in 1..=10 {
            assert!(sm.apply(LogIndex(i), b"data").is_ok());
        }
    }

    #[test]
    fn noop_snapshot_returns_empty() {
        let sm = NoOpStateMachine;
        let snap = sm.snapshot().unwrap();
        assert!(snap.is_empty());
    }

    #[test]
    fn noop_restore_returns_ok() {
        let mut sm = NoOpStateMachine;
        assert!(sm.restore(b"some snapshot data").is_ok());
    }

    #[test]
    fn noop_snapshot_restore_roundtrip() {
        let sm = NoOpStateMachine;
        let snap = sm.snapshot().unwrap();
        let mut sm2 = NoOpStateMachine;
        assert!(sm2.restore(&snap).is_ok());
    }
}
