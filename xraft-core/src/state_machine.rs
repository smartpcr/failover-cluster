//! State machine trait — applied by the consensus layer after commit.

use crate::error::Result;

/// A deterministic state machine driven by committed log entries.
pub trait StateMachine: Send + Sync {
    /// Apply a committed entry's payload to the state machine.
    fn apply(&mut self, data: &[u8]) -> Result<()>;

    /// Take a snapshot of the current state machine state.
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Restore the state machine from a snapshot.
    fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}
