use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::log_entry::{EntryType, LogEntry};
use crate::types::{NodeId, Term};
use crate::voter::{Endpoint, VoterInfo, VotersRecord};

/// The four node roles in xraft (architecture §2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap/recovery; terminal state for removed voters.
    Unattached,
    /// Follower replicating from the leader.
    Follower,
    /// Candidate running an election.
    Candidate,
    /// Leader serving client requests and replicating to followers.
    Leader,
}

/// Tracks a VotersRecord that has been appended but not yet committed.
///
/// While pending, HW advancement for entries at or after `offset` uses
/// `voters` instead of the committed voter set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMembershipChange {
    /// Log offset of the uncommitted VotersRecord.
    pub offset: u64,
    /// The proposed new voter set.
    pub voters: Vec<VoterInfo>,
    /// The node that was promoted from observer to pending voter.
    /// Used to restore observer state if the VotersRecord is truncated.
    pub promoted_node_id: NodeId,
    /// Endpoint of the promoted node (for restoration on truncation).
    pub promoted_endpoint: Endpoint,
}

/// Per-follower/observer replication progress tracked by the leader
/// (architecture §3.2 `FollowerProgress`).
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    /// Next offset the follower wants to read (= follower's log_end_offset).
    /// The follower has replicated entries `[0, fetch_offset)`.
    pub fetch_offset: u64,
    /// Whether this node counts toward quorum (voter vs observer).
    /// Observers replicate via Fetch but their fetch_offset is excluded
    /// from HW calculation (architecture §5.4).
    pub is_voter: bool,
}

/// Error type for LogStore operations.
#[derive(Debug, Clone)]
pub struct LogStoreError {
    pub message: String,
}

impl fmt::Display for LogStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogStoreError: {}", self.message)
    }
}

impl std::error::Error for LogStoreError {}

impl LogStoreError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
        }
    }
}

/// Trait abstracting the replicated log store (architecture §4.1).
///
/// All mutating methods take `&self` — implementations use interior
/// mutability (e.g. `tokio::sync::Mutex`) consistent with `Send + Sync`.
/// The `IoStage` holds an owned `Box<dyn LogStore>` and invokes it via
/// `&self` concurrently with other I/O traits via `tokio::join!`.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries. Must fsync before returning Ok.
    async fn append(&self, entries: &[LogEntry]) -> Result<(), LogStoreError>;

    /// Read entries in [start_offset, end_offset).
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>, LogStoreError>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    async fn truncate_suffix(&self, from_offset: u64) -> Result<(), LogStoreError>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<(), LogStoreError>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;

    /// The next offset to be written.
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, LogStoreError>;

    /// Scan uncommitted entries (from `high_watermark` to end) for a VotersRecord.
    async fn has_uncommitted_voters_record(&self, high_watermark: u64) -> Result<bool, LogStoreError>;
}

/// In-memory log store implementing the async `LogStore` trait.
///
/// Uses `std::sync::Mutex` (not `tokio::sync::Mutex`) for interior
/// mutability because `InMemoryLog` also implements the synchronous
/// `SyncLogOps` trait below, which is invoked from the event loop and
/// cannot `.await` a lock. All critical sections are short, allocation-
/// free `Vec` operations and the guard is never held across `.await`,
/// so a blocking mutex is appropriate here. Offsets are tracked with
/// atomics, consistent with the `Send + Sync + 'static` bound.
pub struct InMemoryLog {
    entries: Mutex<Vec<LogEntry>>,
    start_offset: AtomicU64,
    end_offset: AtomicU64,
}

impl fmt::Debug for InMemoryLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryLog")
            .field("start_offset", &self.start_offset.load(Ordering::SeqCst))
            .field("end_offset", &self.end_offset.load(Ordering::SeqCst))
            .finish()
    }
}

impl Default for InMemoryLog {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryLog {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            start_offset: AtomicU64::new(0),
            end_offset: AtomicU64::new(0),
        }
    }

    /// Acquire the `entries` lock, recovering from poisoning.
    ///
    /// `std::sync::Mutex` poisons on panic so that other threads observe
    /// the failure. The only invariant protected by this lock is "is a
    /// valid `Vec<LogEntry>`", which holds regardless of poisoning. We
    /// therefore recover the inner `Vec` via `PoisonError::into_inner()`
    /// rather than cascading a panic into every subsequent caller.
    fn lock_entries(&self) -> MutexGuard<'_, Vec<LogEntry>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Set the start offset for testing purposes (simulating log compaction).
    #[cfg(test)]
    pub fn set_start_offset_for_test(&self, offset: u64) {
        self.start_offset.store(offset, Ordering::SeqCst);
    }
}

#[async_trait]
impl LogStore for InMemoryLog {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), LogStoreError> {
        let mut log = self.lock_entries();
        for entry in entries {
            log.push(entry.clone());
        }
        if let Some(last) = entries.last() {
            self.end_offset.store(last.offset + 1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>, LogStoreError> {
        let log = self.lock_entries();
        let result = log
            .iter()
            .filter(|e| e.offset >= start_offset && e.offset < end_offset)
            .cloned()
            .collect();
        Ok(result)
    }

    async fn truncate_suffix(&self, from_offset: u64) -> Result<(), LogStoreError> {
        let mut log = self.lock_entries();
        log.retain(|e| e.offset < from_offset);
        let new_end = log.last().map(|e| e.offset + 1).unwrap_or(0);
        self.end_offset.store(new_end, Ordering::SeqCst);
        Ok(())
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<(), LogStoreError> {
        let mut log = self.lock_entries();
        log.retain(|e| e.offset >= up_to_offset);
        self.start_offset.store(up_to_offset, Ordering::SeqCst);
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::SeqCst)
    }

    fn log_end_offset(&self) -> u64 {
        self.end_offset.load(Ordering::SeqCst)
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, LogStoreError> {
        let log = self.lock_entries();
        Ok(log.iter().find(|e| e.offset == offset).cloned())
    }

    async fn has_uncommitted_voters_record(&self, high_watermark: u64) -> Result<bool, LogStoreError> {
        let log = self.lock_entries();
        Ok(log
            .iter()
            .filter(|e| e.offset >= high_watermark)
            .any(|e| e.entry_type == EntryType::VotersRecord))
    }
}

/// Synchronous log operations used by the `MembershipManager`.
///
/// The async `LogStore` trait is designed for the I/O stage where fsync
/// and network I/O are involved. The `MembershipManager` operates on
/// in-memory state within the event loop and needs synchronous access.
/// Append is fallible so that storage errors can be surfaced to the
/// caller (e.g. `handle_add_voter`) without panicking.
pub trait SyncLogOps: Send + Sync {
    /// Append a single entry to the log. Returns an error if the
    /// underlying storage cannot persist the entry.
    fn append_entry(&self, entry: LogEntry) -> Result<(), LogStoreError>;

    /// Check for uncommitted VotersRecord entries at or past `high_watermark`.
    fn has_uncommitted_voters_record_sync(&self, high_watermark: u64) -> bool;

    /// Read up to `max_entries` starting at `from_offset`.
    fn read_entries(&self, from_offset: u64, max_entries: usize) -> Vec<LogEntry>;

    /// Read entries starting at `from_offset`, respecting both `max_entries`
    /// and `max_bytes` limits. Entries are included until either limit is
    /// reached. If `max_bytes` is 0, only `max_entries` is enforced.
    fn read_entries_bounded(
        &self,
        from_offset: u64,
        max_entries: usize,
        max_bytes: u32,
    ) -> Vec<LogEntry>;

    /// The next offset to be written.
    fn end_offset(&self) -> u64;

    /// The first offset still in the log (after compaction/snapshot).
    fn start_offset(&self) -> u64;

    /// Return the term of the entry at the given offset, or None if absent.
    fn entry_term_at(&self, offset: u64) -> Option<Term>;

    /// Find the end offset of a given epoch (term) in the log.
    /// Returns the offset of the first entry with a term > `epoch`,
    /// i.e., the exclusive upper bound of entries with that epoch.
    fn epoch_end_offset(&self, epoch: Term) -> u64;

    /// Truncate the log suffix starting at the given offset.
    fn truncate_suffix_sync(&self, from_offset: u64);
}

impl SyncLogOps for InMemoryLog {
    fn append_entry(&self, entry: LogEntry) -> Result<(), LogStoreError> {
        let mut log = self.lock_entries();
        let next_offset = entry.offset + 1;
        log.push(entry);
        self.end_offset.store(next_offset, Ordering::SeqCst);
        Ok(())
    }

    fn has_uncommitted_voters_record_sync(&self, high_watermark: u64) -> bool {
        let log = self.lock_entries();
        log.iter()
            .filter(|e| e.offset >= high_watermark)
            .any(|e| e.entry_type == EntryType::VotersRecord)
    }

    fn read_entries(&self, from_offset: u64, max_entries: usize) -> Vec<LogEntry> {
        let log = self.lock_entries();
        log.iter()
            .filter(|e| e.offset >= from_offset)
            .take(max_entries)
            .cloned()
            .collect()
    }

    fn read_entries_bounded(
        &self,
        from_offset: u64,
        max_entries: usize,
        max_bytes: u32,
    ) -> Vec<LogEntry> {
        let log = self.lock_entries();
        let mut result = Vec::new();
        let mut total_bytes: u64 = 0;
        for entry in log.iter().filter(|e| e.offset >= from_offset) {
            if result.len() >= max_entries {
                break;
            }
            // Approximate entry size: payload + fixed overhead for offset/term/type
            let entry_size = entry.payload.len() as u64 + 24;
            if max_bytes > 0 && total_bytes + entry_size > max_bytes as u64 && !result.is_empty() {
                break;
            }
            total_bytes += entry_size;
            result.push(entry.clone());
        }
        result
    }

    fn end_offset(&self) -> u64 {
        self.end_offset.load(Ordering::SeqCst)
    }

    fn start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::SeqCst)
    }

    fn entry_term_at(&self, offset: u64) -> Option<Term> {
        let log = self.lock_entries();
        log.iter().find(|e| e.offset == offset).map(|e| e.term)
    }

    fn epoch_end_offset(&self, epoch: Term) -> u64 {
        let log = self.lock_entries();
        // Find the first entry with term > epoch, return its offset.
        // If no such entry exists, return end_offset.
        for entry in log.iter() {
            if entry.term > epoch && entry.offset > 0 {
                return entry.offset;
            }
        }
        self.end_offset.load(Ordering::SeqCst)
    }

    fn truncate_suffix_sync(&self, from_offset: u64) {
        let mut log = self.lock_entries();
        log.retain(|e| e.offset < from_offset);
        let new_end = log.last().map(|e| e.offset + 1).unwrap_or(0);
        self.end_offset.store(new_end, Ordering::SeqCst);
    }
}

/// Public read projection of consensus state (architecture §5.11).
///
/// Returned by `RaftNode::read()` via a `tokio::sync::watch` channel.
/// Contains only the committed voter set — pending membership changes
/// are not visible until committed.
#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    /// The committed voter set — updated only when a VotersRecord commits.
    /// Elections, Check Quorum, and external queries all use this set.
    pub voter_set: Vec<VoterInfo>,
}

/// Full internal protocol state (pub(crate) in production, pub here for testing).
///
/// The public `ConsensusState` returned by `read()` is a separate, smaller
/// projection — see architecture §3.2.
#[derive(Debug)]
pub struct NodeState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,

    // Log boundaries
    pub log_end_offset: u64,
    /// Exclusive upper bound of committed offsets: entries with offset < HW
    /// are committed.
    pub high_watermark: u64,

    // Voter set (from latest COMMITTED VotersRecord)
    pub voter_set: Vec<VoterInfo>,

    // Observers (non-voting, replicating via Fetch)
    pub observers: HashSet<NodeId>,
    /// Network endpoints for registered observers. Preserved so that a
    /// truncated VotersRecord can restore the promoted node's endpoint.
    pub observer_endpoints: HashMap<NodeId, Endpoint>,

    // Pending membership change (at most one)
    pub pending_membership_change: Option<PendingMembershipChange>,

    // Leader-only: per-follower replication progress
    pub follower_state: HashMap<NodeId, FollowerProgress>,
}

impl NodeState {
    /// Create a new NodeState for a leader with the given voter set.
    pub fn new_leader(node_id: NodeId, term: Term, voter_set: Vec<VoterInfo>) -> Self {
        let mut follower_state = HashMap::new();
        for voter in &voter_set {
            if voter.node_id != node_id {
                follower_state.insert(
                    voter.node_id,
                    FollowerProgress {
                        node_id: voter.node_id,
                        fetch_offset: 0,
                        is_voter: true,
                    },
                );
            }
        }

        NodeState {
            node_id,
            current_term: term,
            role: Role::Leader,
            leader_id: Some(node_id),
            log_end_offset: 0,
            high_watermark: 0,
            voter_set,
            observers: HashSet::new(),
            observer_endpoints: HashMap::new(),
            pending_membership_change: None,
            follower_state,
        }
    }

    /// Project the internal state into a `ConsensusState` for external
    /// consumption. The event loop calls this after every state mutation
    /// and publishes the result via a `tokio::sync::watch` channel.
    pub fn to_consensus_state(&self) -> ConsensusState {
        ConsensusState {
            node_id: self.node_id,
            current_term: self.current_term,
            role: self.role,
            leader_id: self.leader_id,
            log_end_offset: self.log_end_offset,
            high_watermark: self.high_watermark,
            voter_set: self.voter_set.clone(),
        }
        self.pending_membership_change = Some(PendingMembershipChange {
            offset,
            proposed_voter_set,
        });
        Ok(())
    }

    /// Check if a node is in the committed voter set.
    pub fn is_voter(&self, node_id: NodeId) -> bool {
        self.voter_set.iter().any(|v| v.node_id == node_id)
    }

    /// Check if a node is a registered observer.
    pub fn is_observer(&self, node_id: NodeId) -> bool {
        self.observers.contains(&node_id)
    }

    /// Get the voter set that should be used for HW advancement at the
    /// given offset. If a pending membership change exists and the offset
    /// is at or after the VotersRecord's offset, use the pending voter set.
    pub fn effective_voter_set_for_hw(&self, offset: u64) -> &[VoterInfo] {
        if let Some(ref pending) = self.pending_membership_change {
            if offset >= pending.offset {
                return &pending.voters;
            }
        }
        &self.voter_set
    }

    /// Compute the majority offset for a given voter set.
    ///
    /// HW = sorted descending fetch_offsets of voters, pick index ⌊V/2⌋.
    /// Only voters contribute. The leader's own offset counts.
    fn compute_hw_with_voters(&self, voters: &[VoterInfo], log_end_offset: u64) -> u64 {
        let mut offsets: Vec<u64> = voters
            .iter()
            .map(|v| {
                if v.node_id == self.node_id {
                    log_end_offset
                } else {
                    self.follower_state
                        .get(&v.node_id)
                        .map(|fp| fp.fetch_offset)
                        .unwrap_or(0)
                }
            })
            .collect();

        offsets.sort_unstable();
        offsets.reverse();

        let majority_idx = offsets.len() / 2; // ⌊V/2⌋ (0-indexed)
        offsets.get(majority_idx).copied().unwrap_or(0)
    }

    /// Compute the high watermark using dual-quorum semantics (§5.5).
    ///
    /// When a pending VotersRecord exists at offset P:
    /// - Entries before P require a majority of the **committed** voter set.
    /// - Entries at or after P require a majority of the **new** voter set.
    pub fn compute_high_watermark(&self, log_end_offset: u64) -> u64 {
        match &self.pending_membership_change {
            None => self.compute_hw_with_voters(&self.voter_set, log_end_offset),
            Some(pending) => {
                // Phase 1: entries before VotersRecord use committed voter set
                let hw_committed =
                    self.compute_hw_with_voters(&self.voter_set, log_end_offset);

                if hw_committed < pending.offset {
                    // Committed voters haven't reached VotersRecord yet.
                    return hw_committed;
                }

                // Phase 2: entries at/after VotersRecord use the new voter set
                let hw_new =
                    self.compute_hw_with_voters(&pending.voters, log_end_offset);

                // New quorum determines advancement at/past VotersRecord offset.
                hw_new.max(pending.offset)
            }
        }
    }

    /// Finalize a pending membership change (called when the VotersRecord
    /// is committed). Atomically replaces the committed voter set and
    /// clears the pending change.
    ///
    /// After this call:
    /// - `voter_set` contains the new voter set
    /// - The promoted node participates in elections and Check Quorum
    /// - `to_consensus_state()` returns the updated voter set
    /// - `election_voter_set()` includes the new voter
    /// - `check_quorum_voter_set()` includes the new voter
    pub fn commit_membership_change(&mut self) {
        if let Some(pending) = self.pending_membership_change.take() {
            self.voter_set = pending.voters;
            // Mark the promoted node as a voter in follower tracking
            if let Some(fp) = self.follower_state.get_mut(&pending.promoted_node_id) {
                fp.is_voter = true;
            }
            // Clean up observer endpoint entry (node is now a full voter)
            self.observer_endpoints.remove(&pending.promoted_node_id);
        }
    }

    /// Returns the voter set used for elections (Vote/PreVote RPCs).
    ///
    /// Always returns the **committed** voter set — pending membership
    /// changes do NOT affect election quorum until committed (§5.5).
    pub fn election_voter_set(&self) -> &[VoterInfo] {
        &self.voter_set
    }

    /// Returns the voter set used for Check Quorum validation.
    ///
    /// Always the **committed** voter set — the leader must receive
    /// Fetch requests from a majority of committed voters within the
    /// election timeout to maintain leadership (architecture §5.8).
    pub fn check_quorum_voter_set(&self) -> &[VoterInfo] {
        &self.voter_set
    }

    /// Validate Check Quorum: returns true if the leader has received
    /// recent Fetch requests from a majority of the committed voter set.
    ///
    /// `recent_fetchers` is the set of node IDs that have sent a Fetch
    /// request within the election timeout window.
    pub fn check_quorum_met(&self, recent_fetchers: &HashSet<NodeId>) -> bool {
        let voters = self.check_quorum_voter_set();
        let mut count = 0usize;
        for v in voters {
            if v.node_id == self.node_id || recent_fetchers.contains(&v.node_id) {
                count += 1;
            }
        }
        count > voters.len() / 2
    }

    /// Determine if a VoteRequest should be granted based on the
    /// committed voter set. Only nodes in the committed voter set
    /// can vote or stand for election.
    pub fn can_vote_for(&self, candidate_id: NodeId) -> bool {
        self.voter_set.iter().any(|v| v.node_id == candidate_id)
    }

    /// Apply a committed VotersRecord control entry from the replicated log.
    ///
    /// Called by the event loop on ALL nodes (leader, follower, observer) when
    /// HW advances past a VotersRecord entry. The `record` parameter is
    /// deserialized from the committed log entry's payload.
    ///
    /// On the **leader**, a `pending_membership_change` already exists and is
    /// committed (voter set replaced, pending cleared, promoted node marked).
    ///
    /// On **followers/observers**, no local `pending_membership_change` exists.
    /// The voter set is replaced directly from the deserialized `VotersRecord`,
    /// and `FollowerProgress` entries are updated to reflect the new voter
    /// membership.
    pub fn apply_voters_record_from_log(&mut self, record: &VotersRecord) {
        if let Some(pending) = self.pending_membership_change.take() {
            // Leader path: commit the pending change (which matches the record)
            self.voter_set = pending.voters;
            if let Some(fp) = self.follower_state.get_mut(&pending.promoted_node_id) {
                fp.is_voter = true;
            }
            self.observer_endpoints.remove(&pending.promoted_node_id);
        } else {
            // Follower/observer path: apply the VotersRecord from the log directly.
            // Determine which nodes are new voters vs removed voters.
            let old_voter_ids: std::collections::HashSet<NodeId> =
                self.voter_set.iter().map(|v| v.node_id).collect();
            let new_voter_ids: std::collections::HashSet<NodeId> =
                record.voters.iter().map(|v| v.node_id).collect();

            // Mark newly added voters in follower_state
            for vid in new_voter_ids.difference(&old_voter_ids) {
                // Remove from observers if present (promoted)
                self.observers.remove(vid);
                self.observer_endpoints.remove(vid);

                if let Some(fp) = self.follower_state.get_mut(vid) {
                    fp.is_voter = true;
                }
            }

            // Mark removed voters in follower_state
            for vid in old_voter_ids.difference(&new_voter_ids) {
                if let Some(fp) = self.follower_state.get_mut(vid) {
                    fp.is_voter = false;
                }
            }

            // Atomically replace the voter set
            self.voter_set = record.voters.clone();
        }
    }

    /// Apply a committed VotersRecord using the local pending state (leader only).
    ///
    /// Delegates to `commit_membership_change`. For the general path that
    /// works on all nodes, use `apply_voters_record_from_log`.
    pub fn apply_voters_record(&mut self) {
        self.commit_membership_change();
    }

    #[test]
    fn advance_hw_rejects_non_contiguous_entries() {
        let mut state = default_state();

        // Provide entries at offsets 0 and 2, skipping 1.
        let entries = vec![
            make_data_entry(0, 1),
            make_data_entry(2, 1),
        ];

        let result = state.advance_high_watermark(3, &entries);
        assert!(result.is_err());
        assert!(
            matches!(&result, Err(RaftError::NonContiguousCommit(_))),
            "expected NonContiguousCommit, got {:?}",
            result
        );
        // HW must not have advanced.
        assert_eq!(state.high_watermark(), 0);
    }

    #[test]
    fn advance_hw_rejects_insufficient_entry_count() {
        let mut state = default_state();

        // Only provide 2 entries for a range of 3.
        let entries = vec![
            make_data_entry(0, 1),
            make_data_entry(1, 1),
        ];

        let result = state.advance_high_watermark(3, &entries);
        assert!(result.is_err());
        assert_eq!(state.high_watermark(), 0);
    }

    #[test]
    fn advance_hw_applies_voters_record() {
        let mut state = default_state();

        let new_voter_set = VoterSet::from_iter(vec![
            make_node_id(1),
            make_node_id(2),
            make_node_id(4),
        ]);

        // Propose the membership change first.
        state
            .propose_membership_change(1, new_voter_set.clone())
            .unwrap();
        assert!(state.pending_membership_change().is_some());

        let entries = vec![
            make_data_entry(0, 1),
            make_voters_record_entry(1, 1, new_voter_set.clone()),
            make_data_entry(2, 1),
        ];

        state.advance_high_watermark(3, &entries).unwrap();
        assert_eq!(state.high_watermark(), 3);
        assert_eq!(state.voter_set(), &new_voter_set);
        // Pending membership change should be cleared.
        assert!(state.pending_membership_change().is_none());
    }

    #[test]
    fn advance_hw_rejects_backward_movement() {
        let mut state = default_state();

        let entries = vec![make_data_entry(0, 1)];
        state.advance_high_watermark(1, &entries).unwrap();

        let result = state.advance_high_watermark(1, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn advance_hw_rejects_duplicate_offsets() {
        let mut state = default_state();

        let entries = vec![
            make_data_entry(0, 1),
            make_data_entry(0, 1), // duplicate
        ];

        let result = state.advance_high_watermark(2, &entries);
        assert!(result.is_err());
    }

    #[test]
    fn clear_pending_if_truncated_clears_when_at_boundary() {
        let mut state = default_state();
        let new_vs = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2)]);
        state.propose_membership_change(5, new_vs).unwrap();
        assert!(state.pending_membership_change().is_some());

        state.clear_pending_if_truncated(5);
        assert!(state.pending_membership_change().is_none());
    }

    #[test]
    fn clear_pending_if_truncated_preserves_when_below_boundary() {
        let mut state = default_state();
        let new_vs = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2)]);
        state.propose_membership_change(3, new_vs).unwrap();

        state.clear_pending_if_truncated(5);
        assert!(state.pending_membership_change().is_some());
    }

    #[test]
    fn restore_from_snapshot_stores_last_included_term() {
        let mut state = default_state();
        let vs = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2)]);
        let vr = VotersRecord::new(vs.clone(), 10);

        state.restore_from_snapshot_metadata(10, 3, 5, vs, vr);
        assert_eq!(state.snapshot_last_term(), 3);
        assert_eq!(state.high_watermark(), 10);
        assert_eq!(state.leader_epoch(), 5);
    }
}