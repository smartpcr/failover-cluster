// -----------------------------------------------------------------------
// <copyright file="log.rs" company="Microsoft Corp.">
//     Copyright (c) Microsoft Corp. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

//! Write-ahead log implementations for the Raft [`LogStore`] trait.
//!
//! Two implementations are provided:
//!
//! * [`MemoryLogStore`] — volatile, in-memory store for testing.
//! * [`FileLogStore`] — durable, file-backed WAL with CRC-32 integrity
//!   checks, configurable segment rotation, and automatic crash recovery.
//!
//! # Binary frame format (on disk)
//!
//! ```text
//! ┌──────────┬───────┬──────┬─────┬─────────────┬──────────────┬───────┐
//! │ data_len │ index │ term │ tag │ payload_len │ payload_data │ crc32 │
//! │  u32 LE  │ u64LE │u64LE │ u8  │   u32 LE    │  [u8; N]     │u32 LE │
//! └──────────┴───────┴──────┴─────┴─────────────┴──────────────┴───────┘
//!   4 bytes    8       8      1      4              N             4
//! ```
//!
//! * `data_len` = 21 + N  (covers index through payload_data)
//! * `crc32` is computed over the data section (index..payload_data)
//! * Total frame = `data_len + 8` bytes
//!
//! # Segment files
//!
//! Segments are named `{base_index:020}.wal` and rotate when the active
//! segment exceeds [`DEFAULT_MAX_SEGMENT_SIZE`] (64 MiB by default).
//!
//! # Dependencies required in `xraft-storage/Cargo.toml`
//!
//! ```toml
//! crc32fast = "1"
//! bincode = "1"
//! bytes = { workspace = true }
//! ```

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;

use xraft_core::error::{Result, XRaftError};
use xraft_core::message::{Entry, EntryPayload};
use xraft_core::storage::LogStore;
use xraft_core::types::{LogIndex, Term};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn storage_err(msg: impl Into<String>) -> XRaftError {
    XRaftError::Storage(msg.into())
}

fn io_to_storage(e: io::Error) -> XRaftError {
    storage_err(e.to_string())
}

fn crc32_of(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

fn segment_filename(base_index: LogIndex) -> String {
    format!("{:020}.wal", base_index.0)
}

fn parse_segment_filename(path: &Path) -> Result<LogIndex> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| storage_err("invalid segment filename"))?;
    let idx: u64 = stem
        .parse()
        .map_err(|_| storage_err(format!("non-numeric segment stem: {stem}")))?;
    Ok(LogIndex(idx))
}

// ---------------------------------------------------------------------------
// MemoryLogStore
// ---------------------------------------------------------------------------

/// In-memory log store backed by a simple `Vec`.
///
/// **Not suitable for production** — entries are lost on restart.
///
/// # Purge contract
///
/// Mirrors [`FileLogStore`]'s low-watermark behaviour: after
/// `purge_prefix(N)` returns, every read API
/// (`get`, `get_range`, `term_at`, `last_index`, `last_term`) treats
/// indices `<= N` as if the entries never existed — even if a later
/// `append` re-inserts an entry at one of those indices. The
/// watermark (`first_valid_index`) is monotonically non-decreasing,
/// so an out-of-order `purge_prefix` call with a lower argument is a
/// no-op. This makes the in-memory and file-backed stores satisfy
/// the same `LogStore::purge_prefix` post-condition without relying
/// on the absence of out-of-order appends.
#[derive(Debug, Default)]
pub struct MemoryLogStore {
    entries: Vec<Entry>,
    /// Low-watermark of the logically valid log. Entries with
    /// `index <= first_valid_index` are dead and filtered out of every
    /// read; the field only advances forward.
    first_valid_index: LogIndex,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStore for MemoryLogStore {
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        // Reject snapshot markers — they are in-memory only and stored via
        // SnapshotStore, not the WAL.
        for entry in entries {
            if matches!(entry.payload, EntryPayload::Snapshot(_)) {
                return Err(storage_err(
                    "EntryPayload::Snapshot is an in-memory compaction marker \
                     and must not be written to WAL",
                ));
            }
        }
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn get(&self, index: LogIndex) -> Result<Option<Entry>> {
        if index.0 == 0 || index <= self.first_valid_index {
            return Ok(None);
        }
        Ok(self.entries.iter().find(|e| e.index == index).cloned())
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.index >= start && e.index < end && e.index > self.first_valid_index)
            .cloned()
            .collect())
    }

    fn last_index(&self) -> LogIndex {
        // Filter so a re-append at `<= first_valid_index` cannot
        // become the reported tail. `entries` is not strictly ordered
        // after such a re-append, so use `max()` rather than `last()`.
        self.entries
            .iter()
            .filter(|e| e.index > self.first_valid_index)
            .map(|e| e.index)
            .max()
            .unwrap_or(LogIndex(0))
    }

    fn last_term(&self) -> Term {
        self.entries
            .iter()
            .filter(|e| e.index > self.first_valid_index)
            .max_by_key(|e| e.index)
            .map_or(Term(0), |e| e.term)
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
        self.entries.retain(|e| e.index < index);
        Ok(())
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>> {
        if index <= self.first_valid_index {
            return Ok(None);
        }
        Ok(self
            .entries
            .iter()
            .find(|e| e.index == index)
            .map(|e| e.term))
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn purge_prefix(&mut self, through_index_inclusive: LogIndex) -> Result<()> {
        // Monotonic watermark: a re-issued lower (or equal) floor is
        // a no-op. This both satisfies the trait's idempotency
        // contract and makes the in-memory store symmetric with
        // `FileLogStore`'s persisted `first_valid_index`, so an
        // out-of-order `append` at `<= first_valid_index` cannot
        // silently resurface from any read API.
        if through_index_inclusive > self.first_valid_index {
            self.first_valid_index = through_index_inclusive;
        }
        // Reclaim Vec space for entries the floor now hides. Reads
        // still filter by `first_valid_index`, so even if a future
        // out-of-order `append` reinserts an entry `<= first_valid_index`
        // it will stay invisible.
        self.entries
            .retain(|e| e.index > self.first_valid_index);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FileLogStore — constants and internal types
// ---------------------------------------------------------------------------

const PAYLOAD_TAG_NOOP: u8 = 0;
const PAYLOAD_TAG_COMMAND: u8 = 1;
const PAYLOAD_TAG_SNAPSHOT: u8 = 2;
const PAYLOAD_TAG_CONFIG_CHANGE: u8 = 3;

/// Default maximum segment size before rotation (64 MiB).
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

const SEGMENT_EXT: &str = "wal";

/// Sidecar marker file holding the durable low-watermark
/// (`first_valid_index`) for a [`FileLogStore`]. Entries with
/// `index <= first_valid_index` are treated as compacted and never
/// surface from reads or recovery — restart-safe prefix purge.
///
/// Format: 8 little-endian bytes encoding `first_valid_index.0` as a
/// `u64`. Written via `fsync` before any segment-level purge so that a
/// crash mid-purge leaves the store in a state that suppresses
/// resurrected entries on the next recovery.
const PURGE_MARKER_FILE: &str = "purge.idx";

/// Fixed byte overhead per entry: index(8) + term(8) + tag(1) + payload_len(4).
const ENTRY_HEADER_LEN: usize = 21;

/// Metadata for a single WAL segment file.
#[derive(Debug)]
struct SegmentInfo {
    #[expect(dead_code)]
    base_index: LogIndex,
    path: PathBuf,
}

// ---------------------------------------------------------------------------
// FileLogStore
// ---------------------------------------------------------------------------

/// Durable, file-backed write-ahead log with CRC-32 integrity, segment
/// rotation, and crash recovery.
///
/// All appended entries are kept in an in-memory [`BTreeMap`] for fast
/// reads; the on-disk segments are the durable source of truth and are
/// replayed on [`open`](FileLogStore::open).
pub struct FileLogStore {
    dir: PathBuf,
    segments: Vec<SegmentInfo>,
    active_writer: Option<File>,
    active_segment_size: u64,
    /// In-memory cache of every entry in the log.
    entries: BTreeMap<LogIndex, Entry>,
    /// Maps each entry to its physical location: `(segment_vec_index, byte_offset)`.
    offsets: BTreeMap<LogIndex, (usize, u64)>,
    max_segment_size: u64,
    /// Durable low-watermark: all entries with `index <= first_valid_index`
    /// have been logically removed by a prior [`LogStore::purge_prefix`]
    /// call. Persisted to [`PURGE_MARKER_FILE`] and consulted during
    /// recovery so segment frames that span the cut never resurface.
    first_valid_index: LogIndex,
}

// Manual impls not required — all fields are `Send + Sync`.

impl FileLogStore {
    /// Open (or create) a WAL in `dir` with the default 64 MiB segment cap.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_max_segment_size(dir, DEFAULT_MAX_SEGMENT_SIZE)
    }

    /// Open (or create) a WAL in `dir` with a custom segment-size cap.
    pub fn open_with_max_segment_size(
        dir: impl AsRef<Path>,
        max_segment_size: u64,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(io_to_storage)?;

        let mut store = Self {
            dir,
            segments: Vec::new(),
            active_writer: None,
            active_segment_size: 0,
            entries: BTreeMap::new(),
            offsets: BTreeMap::new(),
            max_segment_size,
            first_valid_index: LogIndex(0),
        };

        store.load_purge_marker()?;
        store.recover()?;
        Ok(store)
    }

    // -- recovery ----------------------------------------------------------

    /// Load the durable purge low-watermark from the sidecar marker file.
    ///
    /// Missing file is treated as `first_valid_index = LogIndex(0)`
    /// (no purge ever issued). Corrupt or short content errors out so
    /// the operator notices rather than silently resurrecting compacted
    /// entries.
    fn load_purge_marker(&mut self) -> Result<()> {
        let path = self.dir.join(PURGE_MARKER_FILE);
        if !path.exists() {
            return Ok(());
        }
        let buf = fs::read(&path).map_err(io_to_storage)?;
        if buf.len() != 8 {
            return Err(storage_err(format!(
                "purge marker {} has wrong length: {} (expected 8)",
                path.display(),
                buf.len(),
            )));
        }
        let value = u64::from_le_bytes(buf.try_into().unwrap());
        self.first_valid_index = LogIndex(value);
        Ok(())
    }

    /// Atomically persist `first_valid_index` to the sidecar marker file.
    ///
    /// Writes through a `tmp` file + rename so a crash mid-write cannot
    /// leave a torn marker. Synced before rename so the on-disk state
    /// is durable before returning.
    fn persist_purge_marker(&self) -> Result<()> {
        let final_path = self.dir.join(PURGE_MARKER_FILE);
        let tmp_path = self.dir.join(format!("{PURGE_MARKER_FILE}.tmp"));
        {
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)
                .map_err(io_to_storage)?;
            f.write_all(&self.first_valid_index.0.to_le_bytes())
                .map_err(io_to_storage)?;
            f.sync_all().map_err(io_to_storage)?;
        }
        fs::rename(&tmp_path, &final_path).map_err(io_to_storage)?;
        Ok(())
    }

    /// Scan existing segment files, replay valid frames, and truncate any
    /// corrupt tail on the last segment.
    fn recover(&mut self) -> Result<()> {
        let mut wal_files: Vec<_> = fs::read_dir(&self.dir)
            .map_err(io_to_storage)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == SEGMENT_EXT))
            .collect();

        wal_files.sort_by_key(|e| e.file_name());

        let count = wal_files.len();
        for (i, dir_entry) in wal_files.into_iter().enumerate() {
            let path = dir_entry.path();
            let base_index = parse_segment_filename(&path)?;
            let seg_idx = self.segments.len();
            self.segments.push(SegmentInfo {
                base_index,
                path: path.clone(),
            });
            let is_last = i == count - 1;
            self.recover_segment(&path, seg_idx, is_last)?;
        }

        // Open active writer on the last segment.
        if let Some(last) = self.segments.last() {
            let file = OpenOptions::new()
                .append(true)
                .open(&last.path)
                .map_err(io_to_storage)?;
            self.active_segment_size = file.metadata().map_err(io_to_storage)?.len();
            self.active_writer = Some(file);
        }

        Ok(())
    }

    /// Replay all frames in a single segment file.  If `is_last` is true,
    /// a corrupt / truncated tail is silently trimmed; otherwise corruption
    /// is a hard error.
    fn recover_segment(&mut self, path: &Path, seg_idx: usize, is_last: bool) -> Result<()> {
        let buf = fs::read(path).map_err(io_to_storage)?;
        let mut cursor: usize = 0;

        while cursor < buf.len() {
            let frame_start = cursor;
            match Self::decode_frame(&buf, cursor) {
                Ok((entry, next)) => {
                    // Stage 5.3 restart-safe prefix purge: a previously
                    // issued `purge_prefix(through)` persisted
                    // `first_valid_index = through`, so any segment frame
                    // covering an index `<= first_valid_index` is a dead
                    // remnant and must NOT be exposed to readers. Skip it
                    // entirely — we do not record it in `entries` /
                    // `offsets`, so `get`, `get_range`, `term_at`,
                    // `last_index`, `last_term` all behave as if the
                    // entry no longer exists.
                    if entry.index > self.first_valid_index {
                        self.offsets
                            .insert(entry.index, (seg_idx, frame_start as u64));
                        self.entries.insert(entry.index, entry);
                    }
                    cursor = next;
                }
                Err(_) if is_last => {
                    // Truncate the corrupt tail.
                    let f = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(io_to_storage)?;
                    f.set_len(frame_start as u64).map_err(io_to_storage)?;
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    // -- frame codec -------------------------------------------------------

    /// Encode an [`Entry`] into a self-describing, CRC-protected frame.
    fn encode_frame(entry: &Entry) -> Vec<u8> {
        let data = Self::serialize_entry(entry);
        let crc = crc32_of(&data);

        let mut frame = Vec::with_capacity(4 + data.len() + 4);
        frame.extend_from_slice(&(data.len() as u32).to_le_bytes());
        frame.extend_from_slice(&data);
        frame.extend_from_slice(&crc.to_le_bytes());
        frame
    }

    /// Decode one frame starting at `offset` in `buf`.
    /// Returns `(entry, next_offset)` on success.
    fn decode_frame(buf: &[u8], offset: usize) -> Result<(Entry, usize)> {
        let remaining = buf.len().saturating_sub(offset);

        if remaining < 4 {
            return Err(storage_err("truncated frame: missing data_len"));
        }

        let data_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        let total = 4 + data_len + 4; // header + data + crc

        if remaining < total {
            return Err(storage_err("truncated frame: incomplete body"));
        }

        let data_start = offset + 4;
        let data_end = data_start + data_len;
        let data = &buf[data_start..data_end];

        let stored_crc = u32::from_le_bytes(buf[data_end..data_end + 4].try_into().unwrap());
        let computed_crc = crc32_of(data);

        if stored_crc != computed_crc {
            return Err(storage_err(format!(
                "CRC mismatch at byte {offset}: stored={stored_crc:#010x} \
                 computed={computed_crc:#010x}"
            )));
        }

        let entry = Self::deserialize_entry(data)?;
        Ok((entry, offset + total))
    }

    /// Serialize entry fields into the data section of a frame.
    ///
    /// # Panics
    ///
    /// Panics if `entry.payload` is `EntryPayload::Snapshot`. Snapshot entries
    /// are in-memory compaction markers only and must **never** be persisted to
    /// WAL segment files. The driver must intercept snapshot markers before
    /// calling `LogStore::append`.
    fn serialize_entry(entry: &Entry) -> Vec<u8> {
        let (tag, payload_bytes) = match &entry.payload {
            EntryPayload::NoOp => (PAYLOAD_TAG_NOOP, Vec::new()),
            EntryPayload::Command(b) => (PAYLOAD_TAG_COMMAND, b.to_vec()),
            EntryPayload::Snapshot(_) => {
                panic!(
                    "BUG: EntryPayload::Snapshot is an in-memory compaction marker \
                     and must never be written to WAL segment files"
                );
            }
            EntryPayload::ConfigChange(vs) => {
                let encoded =
                    bincode::serialize(vs).expect("VoterSet serialization should not fail");
                (PAYLOAD_TAG_CONFIG_CHANGE, encoded)
            }
        };

        let mut buf = Vec::with_capacity(ENTRY_HEADER_LEN + payload_bytes.len());
        buf.extend_from_slice(&entry.index.0.to_le_bytes());
        buf.extend_from_slice(&entry.term.0.to_le_bytes());
        buf.push(tag);
        buf.extend_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload_bytes);
        buf
    }

    /// Deserialize entry fields from the data section of a frame.
    fn deserialize_entry(data: &[u8]) -> Result<Entry> {
        if data.len() < ENTRY_HEADER_LEN {
            return Err(storage_err(format!(
                "entry data too short: {} < {ENTRY_HEADER_LEN}",
                data.len()
            )));
        }

        let index = LogIndex(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        let term = Term(u64::from_le_bytes(data[8..16].try_into().unwrap()));
        let tag = data[16];
        let payload_len = u32::from_le_bytes(data[17..21].try_into().unwrap()) as usize;

        if data.len() < ENTRY_HEADER_LEN + payload_len {
            return Err(storage_err("entry payload truncated"));
        }

        let payload_data = &data[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + payload_len];

        let payload = match tag {
            PAYLOAD_TAG_NOOP => EntryPayload::NoOp,
            PAYLOAD_TAG_COMMAND => EntryPayload::Command(Bytes::copy_from_slice(payload_data)),
            PAYLOAD_TAG_SNAPSHOT => {
                // Snapshot markers must never appear in WAL segments. If one is
                // found in a legacy file, reject it as corrupt.
                return Err(storage_err(
                    "EntryPayload::Snapshot found in WAL segment; snapshot entries \
                     are in-memory compaction markers and must not be persisted to WAL",
                ));
            }
            PAYLOAD_TAG_CONFIG_CHANGE => {
                let vs: xraft_core::types::VoterSet = bincode::deserialize(payload_data)
                    .map_err(|e| storage_err(format!("VoterSet decode failed: {e}")))?;
                EntryPayload::ConfigChange(vs)
            }
            other => {
                return Err(storage_err(format!("unknown payload tag: {other}")));
            }
        };

        Ok(Entry {
            index,
            term,
            payload,
        })
    }

    // -- segment management ------------------------------------------------

    fn should_rotate(&self, additional: usize) -> bool {
        self.active_writer.is_some()
            && self.active_segment_size + additional as u64 > self.max_segment_size
    }

    fn rotate_segment(&mut self, next_base: LogIndex) -> Result<()> {
        if let Some(ref w) = self.active_writer {
            w.sync_all().map_err(io_to_storage)?;
        }
        self.active_writer = None;
        self.active_segment_size = 0;
        self.create_segment(next_base)
    }

    fn create_segment(&mut self, base_index: LogIndex) -> Result<()> {
        let name = segment_filename(base_index);
        let path = self.dir.join(&name);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_to_storage)?;

        self.segments.push(SegmentInfo { base_index, path });
        self.active_writer = Some(file);
        self.active_segment_size = 0;
        Ok(())
    }

    /// Re-open the active writer on the last segment, or clear it if no
    /// segments remain.
    fn reopen_active_writer(&mut self) -> Result<()> {
        if let Some(last) = self.segments.last() {
            let file = OpenOptions::new()
                .append(true)
                .open(&last.path)
                .map_err(io_to_storage)?;
            self.active_segment_size = file.metadata().map_err(io_to_storage)?.len();
            self.active_writer = Some(file);
        } else {
            self.active_writer = None;
            self.active_segment_size = 0;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// LogStore implementation
// ---------------------------------------------------------------------------

impl LogStore for FileLogStore {
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            // Snapshot entries are in-memory compaction markers only and must
            // never be persisted to WAL segment files. The driver intercepts
            // snapshot markers and stores them via SnapshotStore instead.
            if matches!(entry.payload, EntryPayload::Snapshot(_)) {
                return Err(storage_err(
                    "EntryPayload::Snapshot is an in-memory compaction marker \
                     and must not be written to WAL segment files",
                ));
            }
            let frame = Self::encode_frame(entry);

            if self.should_rotate(frame.len()) {
                self.rotate_segment(entry.index)?;
            }
            if self.active_writer.is_none() {
                self.create_segment(entry.index)?;
            }

            let offset = self.active_segment_size;
            let seg_idx = self.segments.len() - 1;

            self.active_writer
                .as_mut()
                .unwrap()
                .write_all(&frame)
                .map_err(io_to_storage)?;

            self.active_segment_size += frame.len() as u64;
            self.entries.insert(entry.index, entry.clone());
            self.offsets.insert(entry.index, (seg_idx, offset));
        }
        Ok(())
    }

    fn get(&self, index: LogIndex) -> Result<Option<Entry>> {
        if index.0 == 0 {
            return Ok(None);
        }
        Ok(self.entries.get(&index).cloned())
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>> {
        Ok(self
            .entries
            .range(start..end)
            .map(|(_, e)| e.clone())
            .collect())
    }

    fn last_index(&self) -> LogIndex {
        self.entries
            .keys()
            .next_back()
            .copied()
            .unwrap_or(LogIndex(0))
    }

    fn last_term(&self) -> Term {
        self.entries
            .values()
            .next_back()
            .map_or(Term(0), |e| e.term)
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
        // Find the physical location of the first entry to remove.
        let location = self.offsets.range(index..).next().map(|(_, &loc)| loc);

        let (seg_idx, byte_offset) = match location {
            Some(loc) => loc,
            None => return Ok(()), // nothing to truncate
        };

        // Close the active writer before mutating files.
        self.active_writer = None;

        // Truncate the segment containing the first removed entry.
        // Both the `set_len` metadata update AND the implicit data
        // de-allocation must be durable before we proceed — otherwise a
        // crash between `set_len` and the next OS flush can leave the
        // boundary segment with a stale file size while the data blocks
        // are partially updated (or vice-versa), producing an
        // inconsistent on-disk frame at the truncation point. An fsync
        // here pins both the metadata and the data side so recovery
        // always sees a clean trailing edge.
        {
            let seg_path = &self.segments[seg_idx].path;
            let f = OpenOptions::new()
                .write(true)
                .open(seg_path)
                .map_err(io_to_storage)?;
            f.set_len(byte_offset).map_err(io_to_storage)?;
            f.sync_all().map_err(io_to_storage)?;
        }

        // Delete all subsequent segment files.
        for seg in self.segments.drain(seg_idx + 1..) {
            let _ = fs::remove_file(&seg.path);
        }

        // If the truncated segment is now empty, remove it as well.
        if byte_offset == 0
            && let Some(seg) = self.segments.pop()
        {
            let _ = fs::remove_file(&seg.path);
        }

        // Purge in-memory caches.
        let to_remove: Vec<LogIndex> = self.entries.range(index..).map(|(k, _)| *k).collect();
        for k in &to_remove {
            self.entries.remove(k);
            self.offsets.remove(k);
        }

        self.reopen_active_writer()
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>> {
        Ok(self.entries.get(&index).map(|e| e.term))
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(ref w) = self.active_writer {
            w.sync_all().map_err(io_to_storage)?;
        }
        Ok(())
    }

    fn purge_prefix(&mut self, through_index_inclusive: LogIndex) -> Result<()> {
        // Idempotent: a same-or-lower watermark means a previous purge
        // (or the all-zero default) already covered this range; nothing
        // to do.
        if through_index_inclusive <= self.first_valid_index {
            return Ok(());
        }

        // Step 1 (durability-first): persist the new low-watermark
        // BEFORE we drop any in-memory state or touch segment files.
        // A crash between this step and the in-memory purge leaves the
        // store correct: on restart, `load_purge_marker` reads the new
        // value and `recover_segment` skips any frame `<= through`. The
        // dead frames remain on disk (reclaimed by Stage 6.2 segment GC)
        // but never surface to readers.
        let new_floor = through_index_inclusive;
        let prior_floor = self.first_valid_index;
        self.first_valid_index = new_floor;
        if let Err(e) = self.persist_purge_marker() {
            // Roll back the in-memory floor so a retry can attempt the
            // marker write again without the in-memory state having
            // already diverged.
            self.first_valid_index = prior_floor;
            return Err(e);
        }

        // Step 2: drop in-memory entries / offsets at or below the new
        // floor. After this, the public read API (`get`, `get_range`,
        // `term_at`, `last_index`, `last_term`) treats those entries
        // as if they never existed.
        let drop_keys: Vec<LogIndex> = self
            .entries
            .range(..=through_index_inclusive)
            .map(|(k, _)| *k)
            .collect();
        for k in &drop_keys {
            self.entries.remove(k);
            self.offsets.remove(k);
        }

        // Step 3 (best-effort durable reclaim): delete every NON-active
        // segment whose entire index range is `<= through`. Two segments
        // s_i and s_{i+1} bracket s_i's max index at `s_{i+1}.base_index - 1`,
        // so `s_i` is fully covered iff `s_{i+1}.base_index <= through + 1`.
        // The active (last) segment is never deleted here — it would
        // invalidate `active_writer` and is owned by the append path; if
        // its frames are fully covered they'll be dropped on the next
        // rotation. Either way the purge marker plus the in-memory
        // filtering guarantees correctness regardless of physical
        // segment reclaim.
        if self.segments.len() > 1 {
            let active_seg_idx = self.segments.len() - 1;
            let mut delete_until: Option<usize> = None;
            for i in 0..active_seg_idx {
                let next_base = self.segments[i + 1].base_index;
                // s_i fully covered iff its max entry index <= through.
                // max entry index in s_i = s_{i+1}.base_index - 1
                if next_base.0 == 0 || next_base.0.saturating_sub(1) <= through_index_inclusive.0 {
                    delete_until = Some(i);
                } else {
                    break;
                }
            }
            if let Some(last_to_drop) = delete_until {
                // Remove segments[0..=last_to_drop] from disk and from
                // the in-memory segment vec, then rebuild the offsets
                // map's segment-index references to reflect the shift.
                let drop_count = last_to_drop + 1;
                let dropped: Vec<SegmentInfo> = self.segments.drain(0..drop_count).collect();
                for seg in dropped {
                    let _ = fs::remove_file(&seg.path);
                }
                // Shift surviving offsets' seg_idx by `-drop_count`.
                // (No offsets survive in `[0..drop_count]` because we
                // also purged in-memory entries above; but defensively
                // walk the map.)
                let mut new_offsets: BTreeMap<LogIndex, (usize, u64)> = BTreeMap::new();
                for (idx, (seg_idx, byte_off)) in self.offsets.iter() {
                    let shifted = seg_idx
                        .checked_sub(drop_count)
                        .expect("offset for surviving entry must reference a kept segment");
                    new_offsets.insert(*idx, (shifted, *byte_off));
                }
                self.offsets = new_offsets;
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::message::{Entry, EntryPayload};
    use xraft_core::storage::SnapshotMeta;

    fn make_entry(index: u64, term: u64) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::NoOp,
        }
    }

    fn make_cmd_entry(index: u64, term: u64, data: &[u8]) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::Command(Bytes::copy_from_slice(data)),
        }
    }

    // -- MemoryLogStore tests (preserved) ----------------------------------

    #[test]
    fn empty_log_defaults() {
        let log = MemoryLogStore::new();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert!(log.get(LogIndex(1)).unwrap().is_none());
    }

    #[test]
    fn append_and_get() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 1), make_entry(2, 1)]).unwrap();
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.last_term(), Term(1));

        let entry = log.get(LogIndex(1)).unwrap().unwrap();
        assert_eq!(entry.index, LogIndex(1));
        assert_eq!(entry.term, Term(1));
    }

    #[test]
    fn get_range() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();
        let range = log.get_range(LogIndex(1), LogIndex(3)).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].index, LogIndex(1));
        assert_eq!(range[1].index, LogIndex(2));
    }

    #[test]
    fn truncate_from() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();
        log.truncate_from(LogIndex(2)).unwrap();
        assert_eq!(log.last_index(), LogIndex(1));
        assert!(log.get(LogIndex(2)).unwrap().is_none());
    }

    #[test]
    fn term_at() {
        let mut log = MemoryLogStore::new();
        log.append(&[make_entry(1, 5)]).unwrap();
        assert_eq!(log.term_at(LogIndex(1)).unwrap(), Some(Term(5)));
        assert_eq!(log.term_at(LogIndex(2)).unwrap(), None);
    }

    #[test]
    fn get_index_zero_returns_none() {
        let log = MemoryLogStore::new();
        assert!(log.get(LogIndex(0)).unwrap().is_none());
    }

    #[test]
    fn flush_is_noop() {
        let mut log = MemoryLogStore::new();
        assert!(log.flush().is_ok());
    }

    // -- FileLogStore tests ------------------------------------------------

    /// Create a fresh temp directory for a test, cleaning up any prior run.
    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("xraft-wal-tests").join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn file_empty_log_defaults() {
        let dir = test_dir("file_empty_log_defaults");
        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert!(log.get(LogIndex(1)).unwrap().is_none());
    }

    #[test]
    fn file_append_and_get() {
        let dir = test_dir("file_append_and_get");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_entry(1, 1), make_entry(2, 1)]).unwrap();

        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.last_term(), Term(1));

        let e = log.get(LogIndex(1)).unwrap().unwrap();
        assert_eq!(e.index, LogIndex(1));
        assert_eq!(e.term, Term(1));
    }

    #[test]
    fn file_append_command_payload() {
        let dir = test_dir("file_append_command_payload");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_cmd_entry(1, 1, b"hello world")]).unwrap();
        log.flush().unwrap();

        // Re-open and verify payload survived.
        drop(log);
        let log = FileLogStore::open(&dir).unwrap();
        let e = log.get(LogIndex(1)).unwrap().unwrap();
        match e.payload {
            EntryPayload::Command(ref b) => assert_eq!(&b[..], b"hello world"),
            _ => panic!("expected Command payload"),
        }
    }

    #[test]
    fn file_get_range() {
        let dir = test_dir("file_get_range");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();

        let range = log.get_range(LogIndex(1), LogIndex(3)).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].index, LogIndex(1));
        assert_eq!(range[1].index, LogIndex(2));
    }

    #[test]
    fn file_truncate_from_middle() {
        let dir = test_dir("file_truncate_from_middle");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();

        log.truncate_from(LogIndex(2)).unwrap();

        assert_eq!(log.last_index(), LogIndex(1));
        assert!(log.get(LogIndex(2)).unwrap().is_none());

        // Can still append after truncation.
        log.append(&[make_entry(2, 3)]).unwrap();
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.last_term(), Term(3));
    }

    #[test]
    fn file_truncate_all() {
        let dir = test_dir("file_truncate_all");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_entry(1, 1), make_entry(2, 1)]).unwrap();
        log.truncate_from(LogIndex(1)).unwrap();

        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
    }

    #[test]
    fn file_truncate_beyond_last_is_noop() {
        let dir = test_dir("file_truncate_beyond");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_entry(1, 1)]).unwrap();
        log.truncate_from(LogIndex(99)).unwrap();

        assert_eq!(log.last_index(), LogIndex(1));
    }

    #[test]
    fn file_term_at() {
        let dir = test_dir("file_term_at");
        let mut log = FileLogStore::open(&dir).unwrap();

        log.append(&[make_entry(1, 5)]).unwrap();
        assert_eq!(log.term_at(LogIndex(1)).unwrap(), Some(Term(5)));
        assert_eq!(log.term_at(LogIndex(2)).unwrap(), None);
    }

    #[test]
    fn file_get_index_zero() {
        let dir = test_dir("file_get_index_zero");
        let log = FileLogStore::open(&dir).unwrap();
        assert!(log.get(LogIndex(0)).unwrap().is_none());
    }

    #[test]
    fn file_crash_recovery() {
        let dir = test_dir("file_crash_recovery");

        // Phase 1: write entries and flush.
        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[
                make_entry(1, 1),
                make_cmd_entry(2, 1, b"data"),
                make_entry(3, 2),
            ])
            .unwrap();
            log.flush().unwrap();
        }

        // Phase 2: reopen — entries must survive.
        {
            let log = FileLogStore::open(&dir).unwrap();
            assert_eq!(log.last_index(), LogIndex(3));
            assert_eq!(log.last_term(), Term(2));
            let e = log.get(LogIndex(2)).unwrap().unwrap();
            match e.payload {
                EntryPayload::Command(ref b) => assert_eq!(&b[..], b"data"),
                _ => panic!("expected Command"),
            }
        }
    }

    #[test]
    fn file_corrupt_tail_recovery() {
        let dir = test_dir("file_corrupt_tail_recovery");

        // Write valid entries.
        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[make_entry(1, 1), make_entry(2, 1)]).unwrap();
            log.flush().unwrap();
        }

        // Corrupt the tail of the segment file by appending garbage.
        {
            let mut wal_files: Vec<_> = fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == SEGMENT_EXT))
                .collect();
            assert!(!wal_files.is_empty());
            wal_files.sort_by_key(|e| e.file_name());
            let path = wal_files.last().unwrap().path();
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            // Write a partial frame header followed by junk.
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02]).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — should recover first two entries, discarding the
        // corrupt tail.
        {
            let log = FileLogStore::open(&dir).unwrap();
            assert_eq!(log.last_index(), LogIndex(2));
            assert_eq!(log.last_term(), Term(1));
        }
    }

    #[test]
    fn file_crc_integrity() {
        let dir = test_dir("file_crc_integrity");

        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[make_entry(1, 1)]).unwrap();
            log.flush().unwrap();
        }

        // Flip a byte inside the data section of the first frame.
        {
            let wal_path: PathBuf = fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .find(|e| e.path().extension().is_some_and(|x| x == SEGMENT_EXT))
                .unwrap()
                .path();
            let mut data = fs::read(&wal_path).unwrap();
            // Byte 4 is the first byte of the index field; flip it.
            data[4] ^= 0xFF;
            fs::write(&wal_path, &data).unwrap();
        }

        // Recovery should detect the CRC mismatch and truncate the
        // corrupt frame (which is the only frame, so the log is empty).
        {
            let log = FileLogStore::open(&dir).unwrap();
            assert_eq!(log.last_index(), LogIndex(0));
        }
    }

    #[test]
    fn file_segment_rotation() {
        let dir = test_dir("file_segment_rotation");
        // Use a tiny segment cap so rotation triggers quickly.
        let mut log = FileLogStore::open_with_max_segment_size(&dir, 100).unwrap();

        // Each NoOp frame is ~29 bytes.  After ~4 entries the first
        // segment exceeds 100 bytes, triggering rotation.
        for i in 1..=10 {
            log.append(&[make_entry(i, 1)]).unwrap();
        }

        assert!(log.segments.len() >= 2, "expected segment rotation");
        assert_eq!(log.last_index(), LogIndex(10));

        // Verify all entries are readable.
        for i in 1..=10 {
            assert!(log.get(LogIndex(i)).unwrap().is_some());
        }

        // Flush, close, reopen — entries survive across segments.
        log.flush().unwrap();
        drop(log);

        let log = FileLogStore::open_with_max_segment_size(&dir, 100).unwrap();
        assert_eq!(log.last_index(), LogIndex(10));
        for i in 1..=10 {
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
        }
    }

    #[test]
    fn file_truncate_across_segments() {
        let dir = test_dir("file_truncate_across_segments");
        let mut log = FileLogStore::open_with_max_segment_size(&dir, 100).unwrap();

        for i in 1..=10 {
            log.append(&[make_entry(i, 1)]).unwrap();
        }
        let seg_count_before = log.segments.len();
        assert!(seg_count_before >= 2);

        // Truncate from index 3 — should remove segments that held 3..10.
        log.truncate_from(LogIndex(3)).unwrap();
        assert_eq!(log.last_index(), LogIndex(2));
        assert!(log.segments.len() < seg_count_before);

        // Append new entries after truncation.
        log.append(&[make_entry(3, 5), make_entry(4, 5)]).unwrap();
        assert_eq!(log.last_index(), LogIndex(4));
        assert_eq!(log.last_term(), Term(5));
    }

    #[test]
    fn file_snapshot_entry_rejected_by_wal() {
        let dir = test_dir("file_snapshot_entry_rejected_by_wal");
        let meta = SnapshotMeta {
            last_included_index: LogIndex(10),
            last_included_term: Term(3),
            id: "snap-1".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };
        let entry = Entry {
            index: LogIndex(11),
            term: Term(3),
            payload: EntryPayload::Snapshot(meta),
        };

        let mut log = FileLogStore::open(&dir).unwrap();
        // Snapshot entries are in-memory compaction markers and must NOT be
        // persisted to WAL segment files. append() must return an error.
        let result = log.append(&[entry]);
        assert!(result.is_err(), "Snapshot entries must be rejected by WAL");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("compaction marker"),
            "error message should mention compaction marker, got: {err_msg}"
        );
    }

    #[test]
    fn memory_snapshot_entry_rejected_by_wal() {
        let meta = SnapshotMeta {
            last_included_index: LogIndex(10),
            last_included_term: Term(3),
            id: "snap-1".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };
        let entry = Entry {
            index: LogIndex(11),
            term: Term(3),
            payload: EntryPayload::Snapshot(meta),
        };

        let mut log = MemoryLogStore::new();
        let result = log.append(&[entry]);
        assert!(result.is_err(), "Snapshot entries must be rejected by MemoryLogStore");
    }

    // ---- purge_prefix --------------------------------------------------

    /// MemoryLogStore: after `purge_prefix(N)`, entries at indices
    /// `<= N` must not be reachable via `get`, and entries `> N`
    /// remain intact. Volatile store has no restart concern.
    #[test]
    fn memory_purge_prefix_drops_only_covered_entries() {
        let mut log = MemoryLogStore::new();
        let entries: Vec<Entry> = (1..=10).map(|i| make_entry(i, 1)).collect();
        log.append(&entries).unwrap();

        log.purge_prefix(LogIndex(5)).unwrap();

        for i in 1..=5 {
            assert!(
                log.get(LogIndex(i)).unwrap().is_none(),
                "entry {i} <= purge floor must be gone",
            );
        }
        for i in 6..=10 {
            assert!(
                log.get(LogIndex(i)).unwrap().is_some(),
                "entry {i} > purge floor must remain",
            );
        }
        assert_eq!(log.last_index(), LogIndex(10));
    }

    /// MemoryLogStore: `purge_prefix` is idempotent and a lower
    /// watermark does not re-resurrect previously purged entries.
    #[test]
    fn memory_purge_prefix_idempotent_and_monotonic() {
        let mut log = MemoryLogStore::new();
        log.append(&(1..=10).map(|i| make_entry(i, 1)).collect::<Vec<_>>())
            .unwrap();

        log.purge_prefix(LogIndex(5)).unwrap();
        // Re-issuing a lower floor is a no-op — purged entries stay gone.
        log.purge_prefix(LogIndex(3)).unwrap();
        for i in 1..=5 {
            assert!(log.get(LogIndex(i)).unwrap().is_none());
        }
        // Re-issuing the same floor is a no-op.
        log.purge_prefix(LogIndex(5)).unwrap();
        for i in 1..=5 {
            assert!(log.get(LogIndex(i)).unwrap().is_none());
        }
        // Advancing the floor purges more.
        log.purge_prefix(LogIndex(7)).unwrap();
        for i in 1..=7 {
            assert!(log.get(LogIndex(i)).unwrap().is_none());
        }
        for i in 8..=10 {
            assert!(log.get(LogIndex(i)).unwrap().is_some());
        }
    }

    /// MemoryLogStore: an out-of-order `append` at an index `<= N`
    /// AFTER `purge_prefix(N)` MUST NOT surface from any read API.
    /// This is the precise bypass the watermark closes — before the
    /// `first_valid_index` field existed, `purge_prefix` only
    /// scrubbed the `Vec`, so a subsequent re-append at a purged
    /// index would resurrect the entry on the next `get` /
    /// `get_range` / `term_at` / `last_index` / `last_term` call,
    /// breaking the trait's purge post-condition. Regression test
    /// for review feedback on `xraft-storage/src/log.rs:164`.
    #[test]
    fn memory_reappend_below_purge_floor_stays_invisible() {
        let mut log = MemoryLogStore::new();
        log.append(&(1..=10).map(|i| make_entry(i, 1)).collect::<Vec<_>>())
            .unwrap();

        log.purge_prefix(LogIndex(5)).unwrap();

        // Out-of-order re-append at an index covered by the purge
        // floor. Use a distinctive term so any leak would be obvious.
        log.append(&[make_entry(3, 99)]).unwrap();

        assert!(
            log.get(LogIndex(3)).unwrap().is_none(),
            "re-appended entry at purged index must stay invisible to get()",
        );
        assert!(
            log.term_at(LogIndex(3)).unwrap().is_none(),
            "re-appended entry at purged index must stay invisible to term_at()",
        );

        let range = log.get_range(LogIndex(1), LogIndex(6)).unwrap();
        for e in &range {
            assert!(
                e.index > LogIndex(5),
                "get_range returned entry at index {} (must be > purge floor 5)",
                e.index.0,
            );
        }
        assert_eq!(
            range.len(),
            0,
            "no entry in [1, 6) should survive the purge floor",
        );

        // The resurrected entry at index 3 with term 99 must not
        // hijack the reported tail.
        assert_eq!(log.last_index(), LogIndex(10));
        assert_eq!(log.last_term(), Term(1));
    }

    /// FileLogStore: after `purge_prefix(N)` and a restart, entries
    /// at indices `<= N` MUST NOT resurface from WAL replay. This is
    /// the durability contract — without the `purge.idx` marker,
    /// `recover_segment` would re-load every frame on disk and the
    /// store would silently undo the prefix purge.
    #[test]
    fn file_purge_prefix_survives_restart() {
        let dir = test_dir("file_purge_prefix_survives_restart");
        {
            let mut log = FileLogStore::open(&dir).unwrap();
            let entries: Vec<Entry> = (1..=10).map(|i| make_entry(i, 1)).collect();
            log.append(&entries).unwrap();
            log.flush().unwrap();
            log.purge_prefix(LogIndex(5)).unwrap();
            assert!(log.get(LogIndex(5)).unwrap().is_none());
            assert!(log.get(LogIndex(6)).unwrap().is_some());
        }
        // Reopen — purge marker must be replayed so entries 1..=5
        // stay invisible even though the on-disk WAL frames may
        // still encode them (Stage 6.2 segment GC reclaims later).
        let log = FileLogStore::open(&dir).unwrap();
        for i in 1..=5 {
            assert!(
                log.get(LogIndex(i)).unwrap().is_none(),
                "entry {i} <= purge floor MUST stay purged across restart",
            );
            assert!(
                log.term_at(LogIndex(i)).unwrap().is_none(),
                "term_at({i}) MUST also stay None across restart",
            );
        }
        for i in 6..=10 {
            assert!(
                log.get(LogIndex(i)).unwrap().is_some(),
                "entry {i} > purge floor MUST be preserved across restart",
            );
        }
        assert_eq!(log.last_index(), LogIndex(10));
    }

    /// FileLogStore: when an entire non-active segment is fully
    /// covered by the new floor, `purge_prefix` reclaims it on disk
    /// (best-effort) while preserving the surviving suffix. The
    /// active segment is never deleted — its frames are dropped via
    /// the marker filter, not by file removal.
    #[test]
    fn file_purge_prefix_reclaims_fully_covered_segments() {
        let dir = test_dir("file_purge_prefix_reclaims_fully_covered_segments");
        // Small segment size so multiple segments are created across
        // the appends (each entry's serialised size > 32, so 100-byte
        // segment forces frequent rotation).
        let mut log = FileLogStore::open_with_max_segment_size(&dir, 100).unwrap();
        let entries: Vec<Entry> = (1..=20).map(|i| make_entry(i, 1)).collect();
        log.append(&entries).unwrap();
        log.flush().unwrap();

        // Purge entries 1..=15. After the call, the surviving
        // entries 16..=20 must still be reachable.
        log.purge_prefix(LogIndex(15)).unwrap();
        for i in 1..=15 {
            assert!(log.get(LogIndex(i)).unwrap().is_none());
        }
        for i in 16..=20 {
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
        }
        assert_eq!(log.last_index(), LogIndex(20));

        // Reopen and verify the surviving suffix still loads correctly
        // even if the dropped segments are gone.
        drop(log);
        let log = FileLogStore::open_with_max_segment_size(&dir, 100).unwrap();
        for i in 1..=15 {
            assert!(log.get(LogIndex(i)).unwrap().is_none());
        }
        for i in 16..=20 {
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
        }
        assert_eq!(log.last_index(), LogIndex(20));
    }
}
