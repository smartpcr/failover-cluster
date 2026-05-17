//! Raft protocol message definitions.
//!
//! These are the in-memory representations used by the consensus engine.
//! The `proto` submodule re-exports generated protobuf types; conversion
//! traits (`From`/`TryFrom`) bridge the wire format and the canonical Rust
//! types. Conversions that can fail (e.g. `Entry` → `proto::LogEntry`,
//! which rejects the in-memory-only `Snapshot` variant) use `TryFrom`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage::SnapshotMeta;
use crate::types::{LogIndex, NodeId, Term};

// ---------------------------------------------------------------------------
// Re-export generated protobuf types
// ---------------------------------------------------------------------------

/// Generated protobuf types for all wire RPCs.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/xraft.rs"));
}

// ---------------------------------------------------------------------------
// Canonical Rust types (defined in Stage 1.2, extended in Stage 1.3)
// ---------------------------------------------------------------------------

/// Payload carried by a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// An application command.
    Command(Bytes),
    /// A no-op entry appended by a newly elected leader.
    NoOp,
    /// A configuration change carrying the new voter set.
    ConfigChange(crate::types::VoterSet),
    /// A snapshot marker (metadata only, data stored externally).
    /// This is an in-memory compaction marker only — it has **no** protobuf
    /// wire representation and must never be serialised to the wire.
    Snapshot(SnapshotMeta),
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub payload: EntryPayload,
}

/// Inputs consumed by the Raft state machine.
///
/// `Vote` / `PreVote` response variants carry an explicit `from: NodeId` because
/// the on-the-wire response struct does not embed the responder identity (see
/// the doc comment on [`VoteResponse`] / [`PreVoteResponse`]). The transport
/// layer is responsible for deriving `from` from the connection context and
/// passing it in. Other responses (`FetchResponse`) embed the responder ID
/// (`leader_id`) inside the response payload so a separate field is unnecessary.
#[derive(Debug, Clone)]
pub enum Input {
    Tick,
    VoteRequest(VoteRequest),
    VoteResponse {
        from: NodeId,
        response: VoteResponse,
    },
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse {
        from: NodeId,
        response: PreVoteResponse,
    },
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    ClientPropose(Bytes),
    /// Driver feedback after validating a [`FetchRequest`] (Stage 3.3).
    ///
    /// `handle_fetch_request` cannot itself check log divergence because the
    /// engine is I/O-free and does not hold log entries. The driver, while
    /// processing [`Action::ServeFetch`](Action::ServeFetch), reads the leader's
    /// `LogStore::term_at(req.fetch_offset - 1)` and compares it to
    /// `req.last_fetched_epoch`. If they match, the follower's confirmed
    /// replication tip is `req.fetch_offset - 1`; the driver feeds this back
    /// via `FetchRequestAcked` so the engine can update peer progress and
    /// potentially advance the high watermark. If divergence is detected,
    /// the driver feeds the response with `DivergingEpoch` instead and does
    /// NOT emit this input — the diverging follower has not actually
    /// replicated those entries.
    FetchRequestAcked {
        replica_id: NodeId,
        confirmed_offset: LogIndex,
    },
    /// Driver feedback that an [`Action::TakeSnapshot`] cycle has
    /// completed: the state machine has serialised its state, the
    /// `SnapshotStore` has persisted the bytes, and `metadata`
    /// describes what was saved.
    ///
    /// On receiving this the engine records the latest snapshot
    /// metadata and emits an [`Action::TruncateLog`] of the
    /// [`LogTruncation::PrefixThroughInclusive`] variety so the
    /// driver can compact the log prefix that is now fully covered
    /// by the snapshot. Stage 5.2 wiring — see
    /// `implementation-plan.md` §5.2.
    SnapshotComplete {
        metadata: SnapshotMeta,
    },
    /// Driver feedback that an [`Action::InstallSnapshot`] has
    /// completed: the state machine has been restored from the
    /// leader-supplied snapshot bytes and the `SnapshotStore` has
    /// persisted them.
    ///
    /// On receiving this the engine advances `last_applied` and
    /// `commit_index` to `metadata.last_included_index` (no-op if
    /// already ahead) and records the metadata as the most recent
    /// snapshot. Stage 5.2 wiring — see `implementation-plan.md`
    /// §5.2.
    SnapshotInstalled {
        metadata: SnapshotMeta,
    },
    /// Driver feedback that a `Transport::send_fetch_snapshot` stream
    /// has been fully reassembled into a `(metadata, data)` tuple.
    ///
    /// The driver has applied envelope-level fences (`cluster_id`,
    /// `leader_epoch`, peer == recognised leader, metadata present)
    /// before feeding this input. The engine then performs its own
    /// staleness check (`metadata.last_included_index > last_applied`)
    /// and emits exactly one [`Action::InstallSnapshot`] for the driver
    /// to fulfil. A stale snapshot — one whose coverage is at or behind
    /// the state machine's apply point — emits no action so that the
    /// driver does not regress the state machine via `restore`.
    ///
    /// Stage 5.3 (`implementation-plan.md` §5.2 step 3): this is the
    /// engine-side handoff that the install_snapshot contract calls
    /// out — "receiving a FetchSnapshot response produces
    /// `Action::InstallSnapshot { metadata, data }`". Routing the
    /// production path through this input ensures the action contract
    /// is exercised end-to-end (not just by synthetic tests).
    FetchSnapshotReceived {
        metadata: SnapshotMeta,
        data: Vec<u8>,
    },
}

/// Side-effects emitted by the Raft state machine.
#[derive(Debug, Clone)]
pub enum Action {
    PersistHardState,
    AppendEntries(Vec<Entry>),
    SendMessage {
        to: NodeId,
        message: OutboundMessage,
    },
    /// Instruct the driver to read the inclusive range `[from, to]` (1-based)
    /// from the durable `LogStore` and apply each entry to the state machine
    /// callback. The engine has already advanced `last_applied` to `to`; the
    /// driver MUST apply the entries (or halt and recover from durable state
    /// on restart) before feeding any further input into the node, by the
    /// same contract that requires it to honour [`Action::PersistHardState`]
    /// before any RPC reply.
    ///
    /// **Why the engine emits indices, not entries**: the engine is I/O-free
    /// and does not hold log entries (only the index/term mirror tail). The
    /// driver looks up entries via `LogStore::get_range(from, to + 1)` when
    /// dispatching to the state machine. This matches the
    /// `apply_committed()` Stage 3.3 contract in `implementation-plan.md`
    /// while keeping the engine pure.
    ApplyToStateMachine {
        from: LogIndex,
        to: LogIndex,
    },
    /// Instruct the driver to take a snapshot covering all log entries
    /// up to and including `through_index`.
    ///
    /// The driver looks up the term at `through_index` via
    /// `LogStore::term_at`, calls `StateMachine::snapshot()` to obtain
    /// the serialised state, persists it via `SnapshotStore::save_snapshot`,
    /// and feeds [`Input::SnapshotComplete`] back into the engine so it
    /// can record the new snapshot's metadata and emit the follow-on
    /// [`Action::TruncateLog`] for prefix compaction. Stage 5.2
    /// coordination — see `implementation-plan.md` §5.2.
    TakeSnapshot {
        through_index: LogIndex,
    },
    /// Instruct the driver to install a snapshot received from the leader.
    InstallSnapshot {
        metadata: SnapshotMeta,
        data: Vec<u8>,
    },
    BecomeLeader,
    StepDown,
    /// Instruct the driver (acting as leader) to materialize a `FetchResponse`
    /// for the given peer and dispatch it. The engine cannot construct the
    /// response itself because it does not hold log entries; the driver
    /// looks up `entries[fetch_offset .. fetch_offset + max_batch)` from
    /// the durable `LogStore`, performs divergence detection by comparing
    /// `LogStore::term_at(fetch_offset - 1)` with `last_fetched_epoch`, and
    /// builds a [`FetchResponse`] using the envelope fields provided here.
    /// On a successful (non-diverging) read, the driver also emits
    /// [`Input::FetchRequestAcked`](Input::FetchRequestAcked) so the engine
    /// can advance the per-peer replication tip and the high watermark.
    ///
    /// All envelope fields (`cluster_id`, `leader_epoch`, `leader_id`,
    /// `high_watermark`) are captured at action-emit time so the driver does
    /// not race against subsequent node mutations.
    ServeFetch {
        to: NodeId,
        cluster_id: String,
        leader_epoch: u64,
        leader_id: NodeId,
        high_watermark: LogIndex,
        fetch_offset: LogIndex,
        last_fetched_epoch: Term,
    },
    /// Stage 5.3 (implementation-plan §5.2 step 4) — leader-side snapshot
    /// redirect emitted by the engine.
    ///
    /// `RaftNode::handle_fetch_request` emits this action — instead of
    /// [`Action::ServeFetch`] — when it detects that the follower's
    /// `fetch_offset` is at or below the compacted prefix anchored by
    /// `last_snapshot_meta` (i.e. the snapshot covers entries up to
    /// `snapshot_metadata.last_included_index` and the follower is asking
    /// for an index inside that range). The driver materialises an empty
    /// [`FetchResponse`] carrying
    /// [`snapshot_redirect: Some(SnapshotRedirect{…})`](FetchResponse::snapshot_redirect)
    /// and dispatches it to `to`; the follower then issues a
    /// [`FetchSnapshotRequest`] via the redirect handler in
    /// [`RaftNode::handle_fetch_response`].
    ///
    /// **Mutual exclusivity with `Action::ServeFetch`**: for a single
    /// inbound `FetchRequest` the engine emits exactly one of the two
    /// actions, never both. `RedirectToSnapshot` carries no
    /// `fetch_offset` / `last_fetched_epoch` because the follower is
    /// behind the compacted prefix — the redirect itself supersedes any
    /// log-tail comparison.
    ///
    /// **No `FetchRequestAcked` follow-up**: the redirect does NOT prove
    /// the follower has replicated any entry. In fact the opposite —
    /// the follower is BEHIND the compacted prefix. Driver-side
    /// peer-progress / high-watermark advancement must not run here
    /// (this is asserted by
    /// `redirect_to_snapshot_does_not_advance_peer_progress` in
    /// `xraft-server`).
    ///
    /// All envelope fields are captured at action-emit time, matching
    /// the [`Action::ServeFetch`] contract so the driver does not race
    /// against subsequent node mutations (e.g. a step-down between
    /// `step()` and dispatch).
    RedirectToSnapshot {
        to: NodeId,
        cluster_id: String,
        leader_epoch: u64,
        leader_id: NodeId,
        high_watermark: LogIndex,
        snapshot_metadata: SnapshotMeta,
    },
    /// Instruct the driver to mutate its durable log.
    ///
    /// Two truncation modes are supported (see [`LogTruncation`]):
    ///
    /// - [`LogTruncation::SuffixFromInclusive`]: Stage 3.3 follower
    ///   divergence resolution — drop entries with `index >= from`
    ///   so the follower can re-fetch from the leader-supplied
    ///   consistent point. After truncation the driver MUST call
    ///   [`RaftNode::set_last_log`](crate::node::RaftNode::set_last_log)
    ///   with the actual post-truncation last index/term so the
    ///   engine's in-memory mirror is consistent with durable state.
    /// - [`LogTruncation::PrefixThroughInclusive`]: Stage 5.2
    ///   post-snapshot compaction — drop entries with
    ///   `index <= through` so the log can release the bytes whose
    ///   semantics are now captured by the most recent snapshot.
    ///   Drivers without prefix-purge support yet (Stage 5.2 only
    ///   adds the contract; the segmented-log GC lands in Stage 6.2)
    ///   may treat this as a logging no-op while the variant flows
    ///   through the pipeline.
    TruncateLog(LogTruncation),
}

/// Direction of an [`Action::TruncateLog`] request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogTruncation {
    /// Drop entries with `index >= from_index_inclusive` (suffix).
    /// Stage 3.3 follower divergence resolution.
    SuffixFromInclusive { from_index_inclusive: LogIndex },
    /// Drop entries with `index <= through_index_inclusive` (prefix).
    /// Stage 5.2 post-snapshot log compaction.
    PrefixThroughInclusive { through_index_inclusive: LogIndex },
}

/// Messages sent over the network.
#[derive(Debug, Clone)]
pub enum OutboundMessage {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    FetchSnapshotRequest(FetchSnapshotRequest),
}

/// Request to vote for a candidate (real election with incremented term).
///
/// Every RPC carries `cluster_id` and `leader_epoch` per architecture §2.3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to a vote request.
///
/// Note: `voter_id` is not carried on the wire — the transport layer
/// identifies the responder from the connection context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub vote_granted: bool,
    /// Last known leader, for client routing.
    pub leader_hint: Option<NodeId>,
}

/// Pre-vote request — sent before incrementing term to check quorum
/// reachability and prevent disruption by partitioned nodes (architecture §2.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    /// The term the candidate *would* use if the pre-vote succeeds.
    pub next_term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to a pre-vote request.
///
/// Note: `voter_id` is not carried on the wire — the transport layer
/// identifies the responder from the connection context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteResponse {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub term: Term,
    pub vote_granted: bool,
    /// Last known leader, for client routing.
    pub leader_hint: Option<NodeId>,
}

/// Follower-initiated fetch request (KRaft-style pull replication).
///
/// Carries `last_fetched_epoch` so the leader can detect log divergence.
/// The follower is identified by `replica_id` on the wire; `leader_epoch`
/// serves as the fencing epoch (no separate `term` field).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub replica_id: NodeId,
    pub fetch_offset: LogIndex,
    /// The epoch (term) of the last entry the follower has.
    pub last_fetched_epoch: Term,
}

/// Information about a diverging epoch returned by the leader when the
/// follower's log has diverged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergingEpoch {
    /// The epoch that diverged.
    pub epoch: Term,
    /// The offset at which the epoch ends on the leader's log.
    pub end_offset: LogIndex,
}

/// Returned by the leader on a `FetchResponse` when the follower's
/// `fetch_offset` falls at or below the leader's compacted prefix
/// (i.e. `fetch_offset <= last_snapshot_meta.last_included_index`).
///
/// Stage 5.2 implementation-plan §5.2 step 4: "when a follower's
/// `last_fetch_offset` is before the log start (entries were
/// compacted), respond to the follower's Fetch with a redirect to
/// `FetchSnapshot`, then stream snapshot chunks". The redirect carries
/// the snapshot's `id` (so the follower can issue
/// `FetchSnapshotRequest { snapshot_id, offset: 0, .. }`) plus the
/// snapshot's `last_included_index` / `last_included_term` (so the
/// follower can validate the resume point and discover the post-restore
/// next-fetch offset locally).
///
/// Mutual exclusivity contract: when `snapshot_redirect.is_some()`,
/// `entries` MUST be empty and `diverging_epoch` MUST be `None`. The
/// follower processes the redirect and returns immediately; no entries
/// or divergence resolution happens on the same response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRedirect {
    /// Canonical snapshot identifier the leader is offering. The
    /// follower echoes this in `FetchSnapshotRequest::snapshot_id`.
    pub snapshot_id: String,
    /// Last log index covered by the offered snapshot.
    pub last_included_index: LogIndex,
    /// Term at `last_included_index`.
    pub last_included_term: Term,
}

/// Leader's response to a fetch request.
///
/// `leader_epoch` serves as the fencing epoch (no separate `term` field).
///
/// Stage 5.2 (implementation-plan §5.2 step 4): `snapshot_redirect`
/// carries a hand-off to `FetchSnapshot` when the follower's
/// `fetch_offset` is at or below the leader's compacted prefix.
/// `entries` and `diverging_epoch` are mutually exclusive with
/// `snapshot_redirect`: at most one of the three signals is set on
/// any given response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub leader_id: NodeId,
    pub high_watermark: LogIndex,
    pub entries: Vec<Entry>,
    /// Set when the leader detects the follower's log has diverged.
    pub diverging_epoch: Option<DivergingEpoch>,
    /// Set when the follower's `fetch_offset` is at or below the
    /// leader's compacted prefix; instructs the follower to switch to
    /// `FetchSnapshot` to catch up.
    pub snapshot_redirect: Option<SnapshotRedirect>,
    /// Stage 6.2 leader-hint integrity flag (evaluator feedback iter
    /// 1 item 5). `true` iff the responder is acting as the leader
    /// at response time — i.e. the response carries authoritative
    /// leader state (entries / divergence / snapshot redirect served
    /// from leader role). `false` on the `default_deny_fetch` path
    /// where the responder echoes `leader_id` as a best-effort hint
    /// (possibly its own id when no leader is known). Clients
    /// (`xraft-client::PeerClient`) cache the `(leader_id,
    /// leader_epoch)` tuple as a routing hint ONLY when this is
    /// `true`, otherwise a hint sourced from a non-authoritative
    /// response could pin the cache to a deposed leader or to the
    /// responder's own id.
    pub is_leader: bool,
}

/// Request to fetch a snapshot from the leader (chunked transfer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotRequest {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub replica_id: NodeId,
    /// Identifies which snapshot to fetch.
    pub snapshot_id: String,
    /// Byte offset into the snapshot payload for resumable transfer.
    /// 0 means start from the beginning.
    pub offset: u64,
    /// Maximum bytes to return in this response. 0 means no limit.
    pub max_bytes: u64,
}

/// A single chunk of a snapshot being transferred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotChunk {
    pub cluster_id: String,
    pub leader_epoch: u64,
    pub chunk_index: u64,
    pub data: Vec<u8>,
    /// True when this is the final chunk.
    pub done: bool,
    /// Snapshot metadata — present only in the first chunk.
    pub metadata: Option<SnapshotMeta>,
}

// ---------------------------------------------------------------------------
// Protobuf ↔ Rust conversion traits
// ---------------------------------------------------------------------------

// --- VoteRequest ---

impl From<&VoteRequest> for proto::VoteRequest {
    fn from(r: &VoteRequest) -> Self {
        Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            candidate_id: r.candidate_id.0,
            term: r.term.0,
            last_log_index: r.last_log_index.0,
            last_log_term: r.last_log_term.0,
        }
    }
}

impl From<proto::VoteRequest> for VoteRequest {
    fn from(p: proto::VoteRequest) -> Self {
        Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            term: Term(p.term),
            candidate_id: NodeId(p.candidate_id),
            last_log_index: LogIndex(p.last_log_index),
            last_log_term: Term(p.last_log_term),
        }
    }
}

// --- VoteResponse ---

impl From<&VoteResponse> for proto::VoteResponse {
    fn from(r: &VoteResponse) -> Self {
        Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            term: r.term.0,
            vote_granted: r.vote_granted,
            leader_hint: r.leader_hint.map(|n| n.0),
        }
    }
}

impl From<proto::VoteResponse> for VoteResponse {
    fn from(p: proto::VoteResponse) -> Self {
        Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            term: Term(p.term),
            vote_granted: p.vote_granted,
            leader_hint: p.leader_hint.map(NodeId),
        }
    }
}

// --- PreVoteRequest ---

impl From<&PreVoteRequest> for proto::PreVoteRequest {
    fn from(r: &PreVoteRequest) -> Self {
        Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            candidate_id: r.candidate_id.0,
            term: r.next_term.0,
            last_log_index: r.last_log_index.0,
            last_log_term: r.last_log_term.0,
        }
    }
}

impl From<proto::PreVoteRequest> for PreVoteRequest {
    fn from(p: proto::PreVoteRequest) -> Self {
        Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            next_term: Term(p.term),
            candidate_id: NodeId(p.candidate_id),
            last_log_index: LogIndex(p.last_log_index),
            last_log_term: Term(p.last_log_term),
        }
    }
}

// --- PreVoteResponse ---

impl From<&PreVoteResponse> for proto::PreVoteResponse {
    fn from(r: &PreVoteResponse) -> Self {
        Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            term: r.term.0,
            vote_granted: r.vote_granted,
            leader_hint: r.leader_hint.map(|n| n.0),
        }
    }
}

impl From<proto::PreVoteResponse> for PreVoteResponse {
    fn from(p: proto::PreVoteResponse) -> Self {
        Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            term: Term(p.term),
            vote_granted: p.vote_granted,
            leader_hint: p.leader_hint.map(NodeId),
        }
    }
}

// --- FetchRequest ---

impl From<&FetchRequest> for proto::FetchRequest {
    fn from(r: &FetchRequest) -> Self {
        Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            replica_id: r.replica_id.0,
            fetch_offset: r.fetch_offset.0,
            last_fetched_epoch: r.last_fetched_epoch.0,
        }
    }
}

impl From<proto::FetchRequest> for FetchRequest {
    fn from(p: proto::FetchRequest) -> Self {
        Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            replica_id: NodeId(p.replica_id),
            fetch_offset: LogIndex(p.fetch_offset),
            last_fetched_epoch: Term(p.last_fetched_epoch),
        }
    }
}

// --- DivergingEpoch ---

impl From<&DivergingEpoch> for proto::DivergingEpoch {
    fn from(d: &DivergingEpoch) -> Self {
        Self {
            epoch: d.epoch.0,
            end_offset: d.end_offset.0,
        }
    }
}

impl From<proto::DivergingEpoch> for DivergingEpoch {
    fn from(p: proto::DivergingEpoch) -> Self {
        Self {
            epoch: Term(p.epoch),
            end_offset: LogIndex(p.end_offset),
        }
    }
}

// --- LogEntry / Entry ---

impl TryFrom<&Entry> for proto::LogEntry {
    type Error = String;

    fn try_from(e: &Entry) -> Result<Self, Self::Error> {
        let (entry_type, data) = match &e.payload {
            EntryPayload::Command(bytes) => (proto::EntryType::Command as i32, bytes.to_vec()),
            EntryPayload::NoOp => (proto::EntryType::NoOp as i32, Vec::new()),
            EntryPayload::ConfigChange(voter_set) => {
                let data = bincode::serialize(voter_set)
                    .expect("VoterSet bincode serialisation must not fail");
                (proto::EntryType::Config as i32, data)
            }
            EntryPayload::Snapshot(_) => {
                return Err("EntryPayload::Snapshot is an in-memory compaction marker \
                     and must not be serialised to the wire"
                    .to_string());
            }
        };
        Ok(Self {
            index: e.index.0,
            term: e.term.0,
            entry_type,
            data,
        })
    }
}

impl TryFrom<proto::LogEntry> for Entry {
    type Error = String;

    fn try_from(p: proto::LogEntry) -> Result<Self, Self::Error> {
        let entry_type = proto::EntryType::try_from(p.entry_type)
            .map_err(|_| format!("unknown entry_type discriminant: {}", p.entry_type))?;
        let payload = match entry_type {
            proto::EntryType::Command => EntryPayload::Command(Bytes::from(p.data)),
            proto::EntryType::NoOp => EntryPayload::NoOp,
            proto::EntryType::Config => {
                let voter_set: crate::types::VoterSet = bincode::deserialize(&p.data)
                    .map_err(|e| format!("failed to deserialise VoterSet: {e}"))?;
                EntryPayload::ConfigChange(voter_set)
            }
        };
        Ok(Self {
            index: LogIndex(p.index),
            term: Term(p.term),
            payload,
        })
    }
}

// --- FetchResponse ---

impl TryFrom<&FetchResponse> for proto::FetchResponse {
    type Error = String;

    fn try_from(r: &FetchResponse) -> Result<Self, Self::Error> {
        let entries: Result<Vec<proto::LogEntry>, String> =
            r.entries.iter().map(proto::LogEntry::try_from).collect();
        Ok(Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            leader_id: r.leader_id.0,
            high_watermark: r.high_watermark.0,
            entries: entries?,
            diverging_epoch: r.diverging_epoch.as_ref().map(proto::DivergingEpoch::from),
            snapshot_redirect: r
                .snapshot_redirect
                .as_ref()
                .map(proto::SnapshotRedirect::from),
            is_leader: r.is_leader,
        })
    }
}

impl TryFrom<proto::FetchResponse> for FetchResponse {
    type Error = String;

    fn try_from(p: proto::FetchResponse) -> Result<Self, Self::Error> {
        let entries: Result<Vec<Entry>, String> =
            p.entries.into_iter().map(Entry::try_from).collect();
        Ok(Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            leader_id: NodeId(p.leader_id),
            high_watermark: LogIndex(p.high_watermark),
            entries: entries?,
            diverging_epoch: p.diverging_epoch.map(DivergingEpoch::from),
            snapshot_redirect: p.snapshot_redirect.map(SnapshotRedirect::from),
            is_leader: p.is_leader,
        })
    }
}

// --- SnapshotRedirect ---

impl From<&SnapshotRedirect> for proto::SnapshotRedirect {
    fn from(r: &SnapshotRedirect) -> Self {
        Self {
            snapshot_id: r.snapshot_id.clone(),
            last_included_index: r.last_included_index.0,
            last_included_term: r.last_included_term.0,
        }
    }
}

impl From<proto::SnapshotRedirect> for SnapshotRedirect {
    fn from(p: proto::SnapshotRedirect) -> Self {
        Self {
            snapshot_id: p.snapshot_id,
            last_included_index: LogIndex(p.last_included_index),
            last_included_term: Term(p.last_included_term),
        }
    }
}

// --- SnapshotMetadata / SnapshotMeta ---

impl From<&SnapshotMeta> for proto::SnapshotMetadata {
    fn from(m: &SnapshotMeta) -> Self {
        let voter_set = m.voter_set.as_ref().map(|vs| proto::VoterSet {
            voters: vs
                .voters()
                .iter()
                .map(|vr| proto::VoterRecord {
                    node_id: vr.node_id.0,
                    directory_id: vr.directory_id.0.to_string(),
                    endpoints: vr
                        .endpoints
                        .iter()
                        .map(|ep| proto::Endpoint {
                            host: ep.host.clone(),
                            port: ep.port as u32,
                        })
                        .collect(),
                })
                .collect(),
        });
        Self {
            last_included_index: m.last_included_index.0,
            last_included_term: m.last_included_term.0,
            voter_set,
            snapshot_id: m.id.clone(),
            size_bytes: m.size_bytes,
            checksum: m.checksum,
        }
    }
}

impl TryFrom<proto::SnapshotMetadata> for SnapshotMeta {
    type Error = String;

    fn try_from(p: proto::SnapshotMetadata) -> Result<Self, Self::Error> {
        let voter_set = match p.voter_set {
            None => None,
            Some(vs) => {
                let records: Result<Vec<crate::types::VoterRecord>, String> = vs
                    .voters
                    .into_iter()
                    .map(|vr| {
                        let directory_id = uuid::Uuid::parse_str(&vr.directory_id)
                            .map_err(|e| format!("invalid directory_id UUID: {e}"))?;
                        Ok(crate::types::VoterRecord {
                            node_id: NodeId(vr.node_id),
                            directory_id: crate::types::DirectoryId(directory_id),
                            endpoints: vr
                                .endpoints
                                .into_iter()
                                .map(|ep| {
                                    let port = u16::try_from(ep.port)
                                        .map_err(|_| format!("port {} out of range", ep.port))?;
                                    Ok(crate::types::Endpoint {
                                        host: ep.host,
                                        port,
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect();
                let records = records?;
                if records.is_empty() {
                    None
                } else {
                    Some(
                        crate::types::VoterSet::try_new(records)
                            .map_err(|e| format!("invalid voter set: {e}"))?,
                    )
                }
            }
        };
        Ok(Self {
            last_included_index: LogIndex(p.last_included_index),
            last_included_term: Term(p.last_included_term),
            id: p.snapshot_id,
            voter_set,
            size_bytes: p.size_bytes,
            checksum: p.checksum,
        })
    }
}

// --- FetchSnapshotRequest ---

impl From<&FetchSnapshotRequest> for proto::FetchSnapshotRequest {
    fn from(r: &FetchSnapshotRequest) -> Self {
        Self {
            cluster_id: r.cluster_id.clone(),
            leader_epoch: r.leader_epoch,
            replica_id: r.replica_id.0,
            snapshot_id: r.snapshot_id.clone(),
            offset: r.offset,
            max_bytes: r.max_bytes,
        }
    }
}

impl From<proto::FetchSnapshotRequest> for FetchSnapshotRequest {
    fn from(p: proto::FetchSnapshotRequest) -> Self {
        Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            replica_id: NodeId(p.replica_id),
            snapshot_id: p.snapshot_id,
            offset: p.offset,
            max_bytes: p.max_bytes,
        }
    }
}

// --- FetchSnapshotChunk ---

impl From<&FetchSnapshotChunk> for proto::FetchSnapshotChunk {
    fn from(c: &FetchSnapshotChunk) -> Self {
        Self {
            cluster_id: c.cluster_id.clone(),
            leader_epoch: c.leader_epoch,
            chunk_index: c.chunk_index,
            data: c.data.clone(),
            done: c.done,
            metadata: c.metadata.as_ref().map(proto::SnapshotMetadata::from),
        }
    }
}

impl TryFrom<proto::FetchSnapshotChunk> for FetchSnapshotChunk {
    type Error = String;

    fn try_from(p: proto::FetchSnapshotChunk) -> Result<Self, Self::Error> {
        let metadata = match p.metadata {
            Some(m) => Some(SnapshotMeta::try_from(m)?),
            None => None,
        };
        Ok(Self {
            cluster_id: p.cluster_id,
            leader_epoch: p.leader_epoch,
            chunk_index: p.chunk_index,
            data: p.data,
            done: p.done,
            metadata,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn make_voter_set() -> crate::types::VoterSet {
        use crate::types::{DirectoryId, Endpoint, VoterRecord, VoterSet};
        VoterSet::try_new(vec![VoterRecord {
            node_id: NodeId(1),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("127.0.0.1", 9000)],
        }])
        .unwrap()
    }

    #[test]
    fn proto_roundtrip_vote_request() {
        let req = VoteRequest {
            cluster_id: "test-cluster".into(),
            leader_epoch: 2,
            term: Term(5),
            candidate_id: NodeId(1),
            last_log_index: LogIndex(10),
            last_log_term: Term(4),
        };
        let proto_req = proto::VoteRequest::from(&req);

        let mut buf = Vec::new();
        proto_req.encode(&mut buf).unwrap();
        let decoded = proto::VoteRequest::decode(buf.as_slice()).unwrap();

        let roundtripped = VoteRequest::from(decoded);
        assert_eq!(roundtripped.cluster_id, "test-cluster");
        assert_eq!(roundtripped.leader_epoch, 2);
        assert_eq!(roundtripped.term, Term(5));
        assert_eq!(roundtripped.candidate_id, NodeId(1));
        assert_eq!(roundtripped.last_log_index, LogIndex(10));
        assert_eq!(roundtripped.last_log_term, Term(4));
    }

    #[test]
    fn proto_roundtrip_vote_response_with_leader_hint() {
        let resp = VoteResponse {
            cluster_id: "c".into(),
            leader_epoch: 3,
            term: Term(7),
            vote_granted: true,
            leader_hint: Some(NodeId(5)),
        };
        let proto_resp = proto::VoteResponse::from(&resp);
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::VoteResponse::decode(buf.as_slice()).unwrap();
        let rt = VoteResponse::from(decoded);
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.leader_epoch, 3);
        assert_eq!(rt.term, Term(7));
        assert!(rt.vote_granted);
        assert_eq!(rt.leader_hint, Some(NodeId(5)));
    }

    #[test]
    fn proto_roundtrip_vote_response_without_leader_hint() {
        let resp = VoteResponse {
            cluster_id: "c".into(),
            leader_epoch: 1,
            term: Term(2),
            vote_granted: false,
            leader_hint: None,
        };
        let proto_resp = proto::VoteResponse::from(&resp);
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::VoteResponse::decode(buf.as_slice()).unwrap();
        let rt = VoteResponse::from(decoded);
        assert!(!rt.vote_granted);
        assert_eq!(rt.leader_hint, None);
    }

    #[test]
    fn proto_roundtrip_pre_vote_request() {
        let req = PreVoteRequest {
            cluster_id: "pv-cluster".into(),
            leader_epoch: 4,
            next_term: Term(10),
            candidate_id: NodeId(7),
            last_log_index: LogIndex(50),
            last_log_term: Term(9),
        };
        let proto_req = proto::PreVoteRequest::from(&req);
        let mut buf = Vec::new();
        proto_req.encode(&mut buf).unwrap();
        let decoded = proto::PreVoteRequest::decode(buf.as_slice()).unwrap();
        let rt = PreVoteRequest::from(decoded);
        assert_eq!(rt.cluster_id, "pv-cluster");
        assert_eq!(rt.leader_epoch, 4);
        assert_eq!(rt.next_term, Term(10));
        assert_eq!(rt.candidate_id, NodeId(7));
        assert_eq!(rt.last_log_index, LogIndex(50));
        assert_eq!(rt.last_log_term, Term(9));
    }

    #[test]
    fn proto_roundtrip_pre_vote_response() {
        let resp = PreVoteResponse {
            cluster_id: "c".into(),
            leader_epoch: 2,
            term: Term(8),
            vote_granted: true,
            leader_hint: Some(NodeId(3)),
        };
        let proto_resp = proto::PreVoteResponse::from(&resp);
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::PreVoteResponse::decode(buf.as_slice()).unwrap();
        let rt = PreVoteResponse::from(decoded);
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.leader_epoch, 2);
        assert_eq!(rt.term, Term(8));
        assert!(rt.vote_granted);
        assert_eq!(rt.leader_hint, Some(NodeId(3)));
    }

    #[test]
    fn proto_roundtrip_pre_vote_response_no_hint() {
        let resp = PreVoteResponse {
            cluster_id: "c".into(),
            leader_epoch: 1,
            term: Term(3),
            vote_granted: false,
            leader_hint: None,
        };
        let proto_resp = proto::PreVoteResponse::from(&resp);
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::PreVoteResponse::decode(buf.as_slice()).unwrap();
        let rt = PreVoteResponse::from(decoded);
        assert!(!rt.vote_granted);
        assert_eq!(rt.leader_hint, None);
    }

    #[test]
    fn log_entry_command_roundtrip() {
        let entry = Entry {
            index: LogIndex(42),
            term: Term(3),
            payload: EntryPayload::Command(Bytes::from_static(b"hello")),
        };
        let proto_entry = proto::LogEntry::try_from(&entry).unwrap();
        let mut buf = Vec::new();
        proto_entry.encode(&mut buf).unwrap();
        let decoded = proto::LogEntry::decode(buf.as_slice()).unwrap();
        let rt = Entry::try_from(decoded).unwrap();
        assert_eq!(rt.index, LogIndex(42));
        assert_eq!(rt.term, Term(3));
        assert_eq!(
            rt.payload,
            EntryPayload::Command(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn log_entry_noop_roundtrip() {
        let entry = Entry {
            index: LogIndex(1),
            term: Term(1),
            payload: EntryPayload::NoOp,
        };
        let proto_entry = proto::LogEntry::try_from(&entry).unwrap();
        let mut buf = Vec::new();
        proto_entry.encode(&mut buf).unwrap();
        let decoded = proto::LogEntry::decode(buf.as_slice()).unwrap();
        let rt = Entry::try_from(decoded).unwrap();
        assert_eq!(rt.payload, EntryPayload::NoOp);
    }

    #[test]
    fn log_entry_config_roundtrip() {
        let vs = make_voter_set();
        let entry = Entry {
            index: LogIndex(5),
            term: Term(2),
            payload: EntryPayload::ConfigChange(vs.clone()),
        };
        let proto_entry = proto::LogEntry::try_from(&entry).unwrap();
        assert_eq!(proto_entry.entry_type, proto::EntryType::Config as i32);

        let mut buf = Vec::new();
        proto_entry.encode(&mut buf).unwrap();
        let decoded = proto::LogEntry::decode(buf.as_slice()).unwrap();
        let rt = Entry::try_from(decoded).unwrap();
        match &rt.payload {
            EntryPayload::ConfigChange(rt_vs) => assert_eq!(rt_vs, &vs),
            other => panic!("expected ConfigChange, got {other:?}"),
        }
    }

    #[test]
    fn log_entry_snapshot_returns_error_on_serialise() {
        let entry = Entry {
            index: LogIndex(1),
            term: Term(1),
            payload: EntryPayload::Snapshot(SnapshotMeta {
                last_included_index: LogIndex(0),
                last_included_term: Term(0),
                id: "snap".into(),
                voter_set: None,
                size_bytes: None,
                checksum: None,
            }),
        };
        let result = proto::LogEntry::try_from(&entry);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("in-memory compaction marker"));
    }

    #[test]
    fn proto_roundtrip_fetch_request() {
        let req = FetchRequest {
            cluster_id: "c".into(),
            leader_epoch: 1,
            replica_id: NodeId(3),
            fetch_offset: LogIndex(100),
            last_fetched_epoch: Term(4),
        };
        let proto_req = proto::FetchRequest::from(&req);
        let mut buf = Vec::new();
        proto_req.encode(&mut buf).unwrap();
        let decoded = proto::FetchRequest::decode(buf.as_slice()).unwrap();
        let rt = FetchRequest::from(decoded);
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.leader_epoch, 1);
        assert_eq!(rt.replica_id, NodeId(3));
        assert_eq!(rt.fetch_offset, LogIndex(100));
        assert_eq!(rt.last_fetched_epoch, Term(4));
    }

    #[test]
    fn proto_roundtrip_fetch_response_with_entries_and_diverging_epoch() {
        let entries = vec![
            Entry {
                index: LogIndex(10),
                term: Term(3),
                payload: EntryPayload::Command(Bytes::from_static(b"a")),
            },
            Entry {
                index: LogIndex(11),
                term: Term(3),
                payload: EntryPayload::NoOp,
            },
        ];
        let resp = FetchResponse {
            cluster_id: "c".into(),
            leader_epoch: 5,
            leader_id: NodeId(1),
            high_watermark: LogIndex(9),
            entries: entries.clone(),
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(2),
                end_offset: LogIndex(8),
            }),
            snapshot_redirect: None,
            is_leader: true,
        };
        let proto_resp = proto::FetchResponse::try_from(&resp).unwrap();
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::FetchResponse::decode(buf.as_slice()).unwrap();
        let rt = FetchResponse::try_from(decoded).unwrap();
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.leader_epoch, 5);
        assert_eq!(rt.leader_id, NodeId(1));
        assert_eq!(rt.high_watermark, LogIndex(9));
        assert_eq!(rt.entries.len(), 2);
        assert_eq!(rt.entries[0].index, LogIndex(10));
        assert_eq!(rt.entries[1].payload, EntryPayload::NoOp);
        let de = rt.diverging_epoch.unwrap();
        assert_eq!(de.epoch, Term(2));
        assert_eq!(de.end_offset, LogIndex(8));
    }

    #[test]
    fn proto_roundtrip_fetch_response_empty_entries_no_diverging() {
        let resp = FetchResponse {
            cluster_id: "c".into(),
            leader_epoch: 1,
            leader_id: NodeId(2),
            high_watermark: LogIndex(0),
            entries: vec![],
            diverging_epoch: None,
            snapshot_redirect: None,
            is_leader: true,
        };
        let proto_resp = proto::FetchResponse::try_from(&resp).unwrap();
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::FetchResponse::decode(buf.as_slice()).unwrap();
        let rt = FetchResponse::try_from(decoded).unwrap();
        assert!(rt.entries.is_empty());
        assert!(rt.diverging_epoch.is_none());
        assert!(rt.snapshot_redirect.is_none());
    }

    /// Stage 5.2 — proto roundtrip with `snapshot_redirect` set.
    /// Asserts the new field crosses the wire intact under the
    /// mutual-exclusivity contract (entries empty, diverging_epoch
    /// None).
    #[test]
    fn proto_roundtrip_fetch_response_with_snapshot_redirect() {
        let resp = FetchResponse {
            cluster_id: "c".into(),
            leader_epoch: 9,
            leader_id: NodeId(7),
            high_watermark: LogIndex(123),
            entries: vec![],
            diverging_epoch: None,
            snapshot_redirect: Some(SnapshotRedirect {
                snapshot_id: "snap-roundtrip".into(),
                last_included_index: LogIndex(123),
                last_included_term: Term(8),
            }),
            is_leader: true,
        };
        let proto_resp = proto::FetchResponse::try_from(&resp).unwrap();
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::FetchResponse::decode(buf.as_slice()).unwrap();
        let rt = FetchResponse::try_from(decoded).unwrap();
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.leader_epoch, 9);
        assert_eq!(rt.leader_id, NodeId(7));
        assert_eq!(rt.high_watermark, LogIndex(123));
        assert!(rt.entries.is_empty(), "entries must roundtrip empty");
        assert!(
            rt.diverging_epoch.is_none(),
            "diverging_epoch must roundtrip None",
        );
        let redirect = rt
            .snapshot_redirect
            .expect("snapshot_redirect must survive proto roundtrip");
        assert_eq!(redirect.snapshot_id, "snap-roundtrip");
        assert_eq!(redirect.last_included_index, LogIndex(123));
        assert_eq!(redirect.last_included_term, Term(8));
        assert!(
            rt.is_leader,
            "is_leader=true must survive the proto roundtrip"
        );
    }

    /// Stage 6.2 (evaluator feedback iter 1 item 5): a non-leader
    /// responder MUST be able to signal `is_leader=false` on the wire
    /// so a follower's hint cache does not pin to a deposed leader.
    #[test]
    fn proto_roundtrip_fetch_response_default_deny_carries_is_leader_false() {
        let resp = FetchResponse {
            cluster_id: "c".into(),
            leader_epoch: 0,
            leader_id: NodeId(3),
            high_watermark: LogIndex(0),
            entries: vec![],
            diverging_epoch: None,
            snapshot_redirect: None,
            is_leader: false,
        };
        let proto_resp = proto::FetchResponse::try_from(&resp).unwrap();
        let mut buf = Vec::new();
        proto_resp.encode(&mut buf).unwrap();
        let decoded = proto::FetchResponse::decode(buf.as_slice()).unwrap();
        let rt = FetchResponse::try_from(decoded).unwrap();
        assert!(
            !rt.is_leader,
            "non-leader default_deny response must roundtrip is_leader=false"
        );
    }

    #[test]
    fn proto_roundtrip_snapshot_metadata() {
        let vs = make_voter_set();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(100),
            last_included_term: Term(5),
            id: "snap-42".into(),
            voter_set: Some(vs.clone()),
            size_bytes: Some(1024),
            checksum: Some(0xDEADBEEF),
        };
        let proto_meta = proto::SnapshotMetadata::from(&meta);
        let mut buf = Vec::new();
        proto_meta.encode(&mut buf).unwrap();
        let decoded = proto::SnapshotMetadata::decode(buf.as_slice()).unwrap();
        let rt = SnapshotMeta::try_from(decoded).unwrap();
        assert_eq!(rt.last_included_index, LogIndex(100));
        assert_eq!(rt.last_included_term, Term(5));
        assert_eq!(rt.id, "snap-42");
        assert_eq!(rt.voter_set.as_ref().unwrap(), &vs);
        // Proto now carries size_bytes/checksum for end-to-end snapshot transfer validation.
        assert_eq!(rt.size_bytes, Some(1024));
        assert_eq!(rt.checksum, Some(0xDEADBEEF));
    }

    #[test]
    fn proto_roundtrip_snapshot_metadata_no_voter_set() {
        let meta = SnapshotMeta {
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            id: "snap-empty".into(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };
        let proto_meta = proto::SnapshotMetadata::from(&meta);
        let mut buf = Vec::new();
        proto_meta.encode(&mut buf).unwrap();
        let decoded = proto::SnapshotMetadata::decode(buf.as_slice()).unwrap();
        let rt = SnapshotMeta::try_from(decoded).unwrap();
        assert_eq!(rt.last_included_index, LogIndex(50));
        assert_eq!(rt.id, "snap-empty");
        assert!(rt.voter_set.is_none());
    }

    #[test]
    fn proto_roundtrip_fetch_snapshot_request() {
        let req = FetchSnapshotRequest {
            cluster_id: "c".into(),
            leader_epoch: 3,
            replica_id: NodeId(5),
            snapshot_id: "snap-99".into(),
            offset: 1024,
            max_bytes: 4096,
        };
        let proto_req = proto::FetchSnapshotRequest::from(&req);
        let mut buf = Vec::new();
        proto_req.encode(&mut buf).unwrap();
        let decoded = proto::FetchSnapshotRequest::decode(buf.as_slice()).unwrap();
        let rt = FetchSnapshotRequest::from(decoded);
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.leader_epoch, 3);
        assert_eq!(rt.replica_id, NodeId(5));
        assert_eq!(rt.snapshot_id, "snap-99");
        assert_eq!(rt.offset, 1024);
        assert_eq!(rt.max_bytes, 4096);
    }

    #[test]
    fn proto_roundtrip_fetch_snapshot_chunk_with_metadata() {
        let vs = make_voter_set();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(100),
            last_included_term: Term(5),
            id: "snap-42".into(),
            voter_set: Some(vs),
            size_bytes: Some(4096),
            checksum: Some(0xCAFE),
        };
        let chunk = FetchSnapshotChunk {
            cluster_id: "c".into(),
            leader_epoch: 3,
            chunk_index: 0,
            data: vec![1, 2, 3, 4],
            done: false,
            metadata: Some(meta.clone()),
        };
        let proto_chunk = proto::FetchSnapshotChunk::from(&chunk);
        let mut buf = Vec::new();
        proto_chunk.encode(&mut buf).unwrap();
        let decoded = proto::FetchSnapshotChunk::decode(buf.as_slice()).unwrap();
        let rt = FetchSnapshotChunk::try_from(decoded).unwrap();
        assert_eq!(rt.cluster_id, "c");
        assert_eq!(rt.chunk_index, 0);
        assert_eq!(rt.data, vec![1, 2, 3, 4]);
        assert!(!rt.done);
        let rt_meta = rt.metadata.unwrap();
        assert_eq!(rt_meta.last_included_index, meta.last_included_index);
        assert_eq!(rt_meta.id, "snap-42");
        assert_eq!(rt_meta.size_bytes, Some(4096));
        assert_eq!(rt_meta.checksum, Some(0xCAFE));
    }

    #[test]
    fn proto_roundtrip_fetch_snapshot_chunk_no_metadata() {
        let chunk = FetchSnapshotChunk {
            cluster_id: "c".into(),
            leader_epoch: 3,
            chunk_index: 5,
            data: vec![10, 20],
            done: true,
            metadata: None,
        };
        let proto_chunk = proto::FetchSnapshotChunk::from(&chunk);
        let mut buf = Vec::new();
        proto_chunk.encode(&mut buf).unwrap();
        let decoded = proto::FetchSnapshotChunk::decode(buf.as_slice()).unwrap();
        let rt = FetchSnapshotChunk::try_from(decoded).unwrap();
        assert!(rt.done);
        assert!(rt.metadata.is_none());
    }

    #[test]
    fn log_entry_types_discriminant_roundtrip() {
        // Verify all three entry types roundtrip with correct discriminants
        let vs = make_voter_set();
        let entries: Vec<(proto::EntryType, EntryPayload)> = vec![
            (
                proto::EntryType::Command,
                EntryPayload::Command(Bytes::from_static(b"cmd")),
            ),
            (proto::EntryType::NoOp, EntryPayload::NoOp),
            (proto::EntryType::Config, EntryPayload::ConfigChange(vs)),
        ];
        for (expected_type, payload) in entries {
            let entry = Entry {
                index: LogIndex(1),
                term: Term(1),
                payload,
            };
            let proto_entry = proto::LogEntry::try_from(&entry).unwrap();
            assert_eq!(proto_entry.entry_type, expected_type as i32);
            let mut buf = Vec::new();
            proto_entry.encode(&mut buf).unwrap();
            let decoded = proto::LogEntry::decode(buf.as_slice()).unwrap();
            assert_eq!(decoded.entry_type, expected_type as i32);
            // Also roundtrip back to Rust
            let rt = Entry::try_from(decoded).unwrap();
            assert_eq!(rt.index, LogIndex(1));
            assert_eq!(rt.term, Term(1));
        }
    }

    #[test]
    fn log_entry_invalid_entry_type_returns_error() {
        let bad_entry = proto::LogEntry {
            index: 1,
            term: 1,
            entry_type: 99, // invalid discriminant
            data: vec![],
        };
        let result = Entry::try_from(bad_entry);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("unknown entry_type discriminant: 99")
        );
    }

    #[test]
    fn log_entry_config_malformed_data_returns_error() {
        let bad_config = proto::LogEntry {
            index: 1,
            term: 1,
            entry_type: proto::EntryType::Config as i32,
            data: vec![0xFF, 0xFE, 0xFD], // not valid bincode-encoded VoterSet
        };
        let result = Entry::try_from(bad_config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("failed to deserialise VoterSet")
        );
    }
}
