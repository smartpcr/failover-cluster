//! Storage trait definitions.
//!
//! Traits live in `xraft-core` to keep the consensus engine I/O-free.
//! Concrete implementations are in `xraft-storage`.

use crate::error::Result;
use crate::message::Entry;
use crate::types::{LogIndex, Term};

// Re-export HardState from types (canonical location per implementation-plan).
pub use crate::types::HardState;

/// Durable, append-only log storage.
pub trait LogStore: Send + Sync {
    /// Append entries to the log.
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    /// Retrieve the entry at the given index.
    fn get(&self, index: LogIndex) -> Result<Option<Entry>>;
    /// Retrieve entries in the half-open range `[start, end)`.
    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>>;
    /// The index of the last entry, or 0 if empty.
    fn last_index(&self) -> LogIndex;
    /// The term of the last entry, or Term(0) if empty.
    fn last_term(&self) -> Term;
    /// Remove all entries from `index` onward (inclusive).
    fn truncate_from(&mut self, index: LogIndex) -> Result<()>;
    /// The term of the entry at the given index, if it exists.
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>>;
    /// Flush buffered writes to durable storage.
    fn flush(&mut self) -> Result<()>;
    /// Remove all entries with `index <= through_index_inclusive` from
    /// the log.
    ///
    /// Called by the driver after a snapshot has been durably persisted
    /// to the [`SnapshotStore`] — the snapshot supersedes all entries at
    /// or below `through_index_inclusive`, so the log can reclaim them.
    ///
    /// **Contract:**
    /// - Idempotent: a no-op when no entries are at or below
    ///   `through_index_inclusive` (e.g. an already-compacted log).
    /// - After the call returns, `get(idx)`, `get_range(.., idx + 1)`,
    ///   and `term_at(idx)` MUST return `None` for every
    ///   `idx <= through_index_inclusive`.
    /// - The purge MUST be restart-safe: any durable state that would
    ///   otherwise resurrect a compacted entry on replay (e.g. WAL
    ///   segment frames) must be either deleted OR shadowed by a
    ///   persisted low-watermark marker.
    /// - Implementations MAY retain physical bytes (segments, WAL
    ///   frames) that span the purge boundary; only the *logical view*
    ///   returned by reads must respect the cut.
    ///
    /// Required so that the driver's
    /// `Action::TruncateLog(PrefixThroughInclusive)` arm can rely on
    /// every concrete `LogStore` actually reclaiming the prefix rather
    /// than silently ignoring the request — see Stage 5.3 snapshot
    /// coordination in `implementation-plan.md`.
    fn purge_prefix(&mut self, through_index_inclusive: LogIndex) -> Result<()>;
}

/// Persistent hard state (term + vote).
pub trait HardStateStore: Send + Sync {
    /// Persist the hard state to durable storage.
    fn persist(&mut self, state: &HardState) -> Result<()>;
    /// Load the most recently persisted hard state.
    fn load(&self) -> Result<Option<HardState>>;
}

/// A single chunk yielded by a snapshot reader.
#[derive(Debug, Clone)]
pub struct SnapshotChunkItem {
    /// Zero-based index of this chunk in the stream.
    /// Uses u64 to support pathological chunk counts (e.g. very large
    /// snapshots with small chunk sizes) without overflow.
    pub chunk_index: u64,
    /// Raw payload bytes for this chunk.
    pub data: Vec<u8>,
    /// `true` when this is the final chunk.
    pub done: bool,
    /// Present only on the first chunk (`chunk_index == 0`).
    pub metadata: Option<SnapshotMeta>,
}

impl SnapshotChunkItem {
    /// Convert this chunk item into a [`FetchSnapshotChunk`] RPC message.
    ///
    /// The `cluster_id` and `leader_epoch` fields are provided by the
    /// caller (typically the Raft leader handling InstallSnapshot RPCs).
    ///
    /// `chunk_index` is u64 here and in the proto (`uint64 chunk_index = 3`),
    /// so no truncation occurs during conversion.
    pub fn into_fetch_chunk(
        self,
        cluster_id: String,
        leader_epoch: u64,
    ) -> crate::message::FetchSnapshotChunk {
        crate::message::FetchSnapshotChunk {
            cluster_id,
            leader_epoch,
            chunk_index: self.chunk_index,
            data: self.data,
            done: self.done,
            metadata: self.metadata,
        }
    }
}

/// Durable snapshot storage.
///
/// # FetchSnapshot RPC integration
///
/// The `snapshot_reader` method is the bridge between the snapshot store and
/// the `FetchSnapshot` RPC (chunked snapshot transfer from leader to follower):
///
/// 1. Leader receives `FetchSnapshotRequest` identifying the desired snapshot.
/// 2. Leader calls [`SnapshotStore::snapshot_reader`] to open a streaming reader.
/// 3. Each yielded [`SnapshotChunkItem`] is converted to a wire
///    [`FetchSnapshotChunk`](crate::message::FetchSnapshotChunk) via
///    [`SnapshotChunkItem::into_fetch_chunk`] and sent over the transport.
/// 4. The follower reassembles chunks, verifies metadata, and calls
///    [`SnapshotStore::save_snapshot`] + `StateMachine::restore`.
///
/// The transport layer (`Transport::send_fetch_snapshot`) handles the wire
/// protocol; the snapshot store provides the data source and sink.
pub trait SnapshotStore: Send + Sync {
    /// Save a snapshot with the given metadata and data.
    ///
    /// The `metadata.id` field is **normalized** to a canonical form derived
    /// from `last_included_term` and `last_included_index`:
    /// `snapshot-{term:010}-{index:020}`. The caller-supplied id is discarded.
    /// All subsequent operations (`load_snapshot`, `delete_snapshot`,
    /// `list_snapshots`) use the canonical id.
    ///
    /// If a snapshot already exists at the same `last_included_index` with a
    /// **higher** term, implementations must reject the save with an error to
    /// prevent accidental term regression.
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> Result<()>;
    /// Load the most recent snapshot.
    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>>;
    /// Load a specific snapshot by index and term.
    ///
    /// Returns `Ok(None)` when no snapshot matches. The default implementation
    /// only checks the latest snapshot. Concrete implementations that retain
    /// multiple snapshots (e.g. a file-backed store) should override this
    /// method for direct lookup by index and term.
    fn load_snapshot(
        &self,
        index: LogIndex,
        term: Term,
    ) -> Result<Option<(SnapshotMeta, Vec<u8>)>> {
        // Default: check if the latest snapshot matches.
        if let Some((meta, data)) = self.load_latest_snapshot()?
            && meta.last_included_index == index
            && meta.last_included_term == term
        {
            return Ok(Some((meta, data)));
        }
        Ok(None)
    }
    /// List available snapshots, newest first.
    fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>>;
    /// Delete a specific snapshot.
    ///
    /// The `id` must be the **canonical** snapshot id
    /// (`snapshot-{term:010}-{index:020}`) as returned by [`SnapshotMeta::id`]
    /// after a save or load operation. Caller-supplied ids passed to
    /// [`save_snapshot`](SnapshotStore::save_snapshot) are normalized to
    /// canonical form and are **not** stored — using the original
    /// caller-supplied id will return an error unless it happens to match
    /// the canonical form.
    ///
    /// Returns an error if no snapshot with the given id exists in the
    /// store's index. If the snapshot is in the index but the backing file
    /// has been externally removed, implementations should still remove the
    /// index entry (with a warning) rather than failing.
    fn delete_snapshot(&mut self, id: &str) -> Result<()>;
    /// Check whether a snapshot exists for the given index and term.
    fn snapshot_exists(&self, index: LogIndex, term: Term) -> bool;

    /// Look up a snapshot's metadata by its canonical id string.
    ///
    /// This is the bridge between [`FetchSnapshotRequest`](crate::message::FetchSnapshotRequest)
    /// (which identifies snapshots by `snapshot_id: String`) and the reader
    /// APIs that require a [`SnapshotMeta`]. The RPC handler calls this to
    /// resolve the id before opening a [`snapshot_reader`](SnapshotStore::snapshot_reader).
    ///
    /// Returns `Ok(None)` when no snapshot with the given id exists.
    /// The default implementation scans [`list_snapshots`](SnapshotStore::list_snapshots).
    fn find_by_id(&self, id: &str) -> Result<Option<SnapshotMeta>> {
        Ok(self.list_snapshots()?.into_iter().find(|m| m.id == id))
    }

    /// Read a snapshot in fixed-size chunks for streamed transfer.
    ///
    /// `chunk_size` of 0 uses a default (typically 1 MiB). The returned
    /// iterator yields `SnapshotChunkItem`s; the first item carries metadata.
    ///
    /// The returned iterator is `Send` so that it can be used in async
    /// contexts (e.g. tonic gRPC streaming for `FetchSnapshot` RPCs).
    ///
    /// The default implementation loads the snapshot into memory via
    /// [`load_snapshot`](SnapshotStore::load_snapshot) and splits it.
    /// Implementations may override for zero-copy streaming.
    fn snapshot_reader(
        &self,
        meta: &SnapshotMeta,
        chunk_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Result<SnapshotChunkItem>> + Send>> {
        self.snapshot_reader_from_offset(meta, chunk_size, 0, None)
    }

    /// Read a snapshot starting from a byte offset, optionally limited to
    /// `max_bytes` of payload data.
    ///
    /// This is the primary bridge between [`FetchSnapshotRequest`](crate::message::FetchSnapshotRequest)
    /// and the snapshot store for **resumable** transfers:
    ///
    /// ```text
    /// Follower ──► FetchSnapshotReq(offset=1MB, max_bytes=1MB) ──► Leader
    /// Leader calls snapshot_reader_from_offset(meta, chunk_size, 1MB, Some(1MB))
    /// Leader ◄── yields chunks starting at byte offset 1MB ──► Follower
    /// ```
    ///
    /// When `offset > 0`, chunk indices are adjusted to reflect the logical
    /// position within the full snapshot (i.e. `chunk_index = offset / chunk_size`).
    /// The first yielded chunk still carries metadata so the receiver can
    /// verify identity.
    ///
    /// `max_bytes` of `None` or `Some(0)` means read until the end. If
    /// `offset >= payload_size`, an empty iterator is returned.
    ///
    /// The default implementation loads the full snapshot and slices it.
    /// File-backed implementations may override this with a seek-based reader.
    fn snapshot_reader_from_offset(
        &self,
        meta: &SnapshotMeta,
        chunk_size: usize,
        offset: u64,
        max_bytes: Option<u64>,
    ) -> Result<Box<dyn Iterator<Item = Result<SnapshotChunkItem>> + Send>> {
        let chunk_size = if chunk_size == 0 {
            1024 * 1024
        } else {
            chunk_size
        };
        let (loaded_meta, full_data) = self
            .load_snapshot(meta.last_included_index, meta.last_included_term)?
            .ok_or_else(|| {
                crate::error::XRaftError::Storage(format!(
                    "snapshot_reader: no snapshot found for (term={}, index={})",
                    meta.last_included_term.0, meta.last_included_index.0,
                ))
            })?;

        // Safe u64-to-usize conversion (avoids silent truncation on 32-bit).
        let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
        // Limit to max_bytes if specified and non-zero.
        let effective_max = match max_bytes {
            Some(mb) if mb > 0 => usize::try_from(mb).unwrap_or(usize::MAX),
            _ => usize::MAX,
        };

        // If offset is at or past the end, return a single empty done chunk
        // with metadata so the receiver knows the transfer is complete.
        if offset_usize >= full_data.len() && (offset > 0 || full_data.is_empty()) {
            let base_ci = if chunk_size > 0 && !full_data.is_empty() {
                offset_usize / chunk_size
            } else {
                0
            };
            return Ok(Box::new(std::iter::once(Ok(SnapshotChunkItem {
                chunk_index: base_ci as u64,
                data: Vec::new(),
                done: true,
                metadata: Some(loaded_meta),
            }))));
        }

        // Slice the payload to the requested window.
        let data = if full_data.is_empty() {
            &full_data[..]
        } else {
            let end = std::cmp::min(full_data.len(), offset_usize.saturating_add(effective_max));
            &full_data[offset_usize..end]
        };

        // Starting chunk index reflects the byte offset into the full payload.
        // Use `checked_div` rather than the manual `if chunk_size > 0` guard
        // so clippy's `manual_checked_ops` lint (Rust 1.95+) stays happy
        // under the workspace's `-D warnings` policy.
        let base_chunk_index = offset_usize.checked_div(chunk_size).unwrap_or(0);
        // The window reaches the end of the full payload.
        let window_covers_tail = offset_usize + data.len() >= full_data.len();

        let total_window_chunks = if data.is_empty() {
            1
        } else {
            data.len().div_ceil(chunk_size)
        };

        let mut chunks = Vec::with_capacity(total_window_chunks);
        if data.is_empty() {
            chunks.push(Ok(SnapshotChunkItem {
                chunk_index: base_chunk_index as u64,
                data: Vec::new(),
                done: true,
                metadata: Some(loaded_meta),
            }));
        } else {
            for (i, chunk_data) in data.chunks(chunk_size).enumerate() {
                let is_last_in_window = i == total_window_chunks - 1;
                // `done` means the entire snapshot payload is exhausted.
                let done = is_last_in_window && window_covers_tail;
                chunks.push(Ok(SnapshotChunkItem {
                    chunk_index: (base_chunk_index + i) as u64,
                    data: chunk_data.to_vec(),
                    done,
                    // First yielded chunk always carries metadata.
                    metadata: if i == 0 {
                        Some(loaded_meta.clone())
                    } else {
                        None
                    },
                }));
            }
        }

        Ok(Box::new(chunks.into_iter()))
    }
}

/// Metadata associated with a snapshot.
///
/// Snapshots capture the full state machine state at a given log position,
/// including the cluster membership (voter set) so that restored nodes know
/// the current configuration without replaying the log.
///
/// The `voter_set` field is required for normal production snapshots.
/// [`SnapshotStore::save_snapshot`] implementations **must** reject saves
/// with `voter_set = None` to enforce this invariant. The field is typed as
/// `Option<VoterSet>` only to support deserialization of on-disk snapshots
/// written by older software versions that did not persist voter sets.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub id: String,
    /// The voter set at the time of the snapshot.
    ///
    /// Required for all new snapshots. `None` only when loading legacy
    /// on-disk snapshots that predate membership tracking. Implementations
    /// of [`SnapshotStore::save_snapshot`] reject `None` at save time.
    pub voter_set: Option<crate::types::VoterSet>,
    /// Size of the snapshot payload data in bytes.
    /// Populated by the snapshot store on save/load; `None` when not yet known.
    #[serde(default)]
    pub size_bytes: Option<u64>,
    /// CRC32 checksum of the snapshot payload data, zero-extended to u64.
    /// Populated by the snapshot store on save/load; `None` when not yet known.
    #[serde(default)]
    pub checksum: Option<u64>,
}

impl SnapshotMeta {
    /// Returns the voter set, or an error if it is missing.
    ///
    /// Use this when the caller requires membership information (e.g.
    /// applying a leader-sent snapshot). Bootstrap or legacy snapshots
    /// that lack a voter set will produce a clear error rather than a
    /// silent `None`.
    pub fn voter_set_required(&self) -> crate::error::Result<&crate::types::VoterSet> {
        self.voter_set.as_ref().ok_or_else(|| {
            crate::error::XRaftError::Storage(format!(
                "snapshot {} (term={}, index={}) is missing required voter_set metadata",
                self.id, self.last_included_term.0, self.last_included_index.0,
            ))
        })
    }
}
