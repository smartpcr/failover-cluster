use serde::{Deserialize, Serialize};

/// A point-in-time snapshot of application state, used for compaction and
/// fast follower catch-up in the Raft log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
