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
#[derive(Debug)]
pub struct MemoryLogStore {
    entries: Vec<Entry>,
    /// Logical low-watermark advanced by [`LogStore::purge_prefix`].
    /// Initialised to `LogIndex(1)` so a fresh log reports "every
    /// index ≥ 1 is valid"; bumped to `through + 1` on each prefix
    /// purge.
    first_valid: LogIndex,
}

impl Default for MemoryLogStore {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            first_valid: LogIndex(1),
        }
    }
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
        if index.0 == 0 {
            return Ok(None);
        }
        Ok(self.entries.iter().find(|e| e.index == index).cloned())
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.index >= start && e.index < end)
            .cloned()
            .collect())
    }

    fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(LogIndex(0), |e| e.index)
    }

    fn last_term(&self) -> Term {
        self.entries.last().map_or(Term(0), |e| e.term)
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
        self.entries.retain(|e| e.index < index);
        Ok(())
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>> {
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
        // Volatile store: dropping in-memory entries is the entire
        // purge. Idempotent — entries.retain on an already-purged
        // store is a cheap no-op walk.
        self.entries.retain(|e| e.index > through_index_inclusive);
        if through_index_inclusive >= self.first_valid {
            self.first_valid = LogIndex(through_index_inclusive.0 + 1);
        }
        Ok(())
    }

    fn first_valid_index(&self) -> LogIndex {
        self.first_valid
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

/// Kafka-compatible leader-epoch checkpoint sidecar file (Stage 7.3
/// surface, iter-5). Records the `(leader_epoch, start_offset)` tuples
/// from which the divergence-detection path can reconstruct epoch
/// boundaries after a crash or leader change.
///
/// Format (text, line-oriented, Kafka-equivalent):
/// ```text
/// 0
/// <num_entries>
/// <epoch_1> <start_offset_1>
/// <epoch_2> <start_offset_2>
/// …
/// ```
///
/// Line 1 is the format version (`0`). Line 2 is the entry count.
/// Subsequent lines hold space-separated `(epoch, start_offset)`
/// tuples in ascending epoch order. See [`read_leader_epoch_checkpoint`]
/// and [`write_leader_epoch_checkpoint`] for the I/O helpers.
pub const LEADER_EPOCH_CHECKPOINT_FILE: &str = "leader-epoch-checkpoint";

/// In-memory representation of a leader-epoch checkpoint entry.
/// Mirrors Kafka's `EpochEntry`: every time a new leader is elected
/// at epoch `e`, an entry `(e, first_offset_written_at_epoch_e)` is
/// appended to the checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderEpochEntry {
    /// Leader epoch (Raft term).
    pub epoch: u64,
    /// First log offset (1-based) written under that epoch.
    pub start_offset: u64,
}

/// Read `dir/leader-epoch-checkpoint` if it exists, returning the
/// ordered list of `(epoch, start_offset)` entries. Returns `Ok(vec![])`
/// when the file is absent — equivalent to "no epoch boundaries yet".
///
/// Errors: I/O failures (other than `NotFound`) or a malformed file
/// (non-integer line, version mismatch, count mismatch) surface as
/// `std::io::Error` so the caller can decide whether to fail recovery
/// or reset the checkpoint. The parser is intentionally strict — a
/// silently-truncated file should be loud, not subtly wrong.
pub fn read_leader_epoch_checkpoint(dir: &Path) -> std::io::Result<Vec<LeaderEpochEntry>> {
    use std::io::{BufRead, BufReader};
    let path = dir.join(LEADER_EPOCH_CHECKPOINT_FILE);
    let f = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut lines = BufReader::new(f).lines();
    let version_line = lines.next().transpose()?.unwrap_or_default();
    let version: u32 = version_line.trim().parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("leader-epoch-checkpoint: bad version line {version_line:?}: {e}"),
        )
    })?;
    if version != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("leader-epoch-checkpoint: unsupported version {version}"),
        ));
    }
    let count_line = lines.next().transpose()?.unwrap_or_default();
    let expected: usize = count_line.trim().parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("leader-epoch-checkpoint: bad count line {count_line:?}: {e}"),
        )
    })?;
    let mut out = Vec::with_capacity(expected);
    for line in lines {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_ascii_whitespace();
        let epoch: u64 = parts
            .next()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("leader-epoch-checkpoint: missing epoch in {line:?}"),
                )
            })?
            .parse()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("leader-epoch-checkpoint: bad epoch in {line:?}: {e}"),
                )
            })?;
        let start_offset: u64 = parts
            .next()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("leader-epoch-checkpoint: missing offset in {line:?}"),
                )
            })?
            .parse()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("leader-epoch-checkpoint: bad offset in {line:?}: {e}"),
                )
            })?;
        out.push(LeaderEpochEntry {
            epoch,
            start_offset,
        });
    }
    if out.len() != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "leader-epoch-checkpoint: count mismatch (header={expected}, found={})",
                out.len()
            ),
        ));
    }
    Ok(out)
}

/// Atomically write `entries` to `dir/leader-epoch-checkpoint` using
/// the write-to-temp-then-rename pattern so a crash mid-write never
/// leaves a half-written file. `fsync`s the file before rename and
/// the directory after rename — the same durability discipline used by
/// the WAL segment writer.
pub fn write_leader_epoch_checkpoint(
    dir: &Path,
    entries: &[LeaderEpochEntry],
) -> std::io::Result<()> {
    use std::io::Write;
    let final_path = dir.join(LEADER_EPOCH_CHECKPOINT_FILE);
    let tmp_path = dir.join(format!("{LEADER_EPOCH_CHECKPOINT_FILE}.tmp"));
    {
        let mut tmp = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        writeln!(tmp, "0")?;
        writeln!(tmp, "{}", entries.len())?;
        for e in entries {
            writeln!(tmp, "{} {}", e.epoch, e.start_offset)?;
        }
        tmp.sync_all()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Fixed byte overhead per entry: index(8) + term(8) + tag(1) + payload_len(4).
const ENTRY_HEADER_LEN: usize = 21;

/// Metadata for a single WAL segment file.
#[derive(Debug)]
struct SegmentInfo {
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
    /// Stage 7.3 iter-5: in-memory mirror of the on-disk leader-epoch
    /// checkpoint (`leader-epoch-checkpoint`). One entry per
    /// `(epoch, first_offset_under_that_epoch)` boundary, in ascending
    /// epoch order. Mutated by `append`/`truncate_from`/`purge_prefix`
    /// and persisted by `flush` when `epoch_dirty` is set.
    epoch_entries: Vec<LeaderEpochEntry>,
    /// True when `epoch_entries` has been mutated since the last
    /// successful `write_leader_epoch_checkpoint`. `flush` consults
    /// this so a no-op `flush` does not rewrite the checkpoint file.
    epoch_dirty: bool,
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
            epoch_entries: Vec::new(),
            epoch_dirty: false,
        };

        store.load_purge_marker()?;
        store.recover()?;
        store.load_or_backfill_epoch_checkpoint()?;
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

    /// Stage 7.3 iter-5 — load the on-disk leader-epoch checkpoint into
    /// `epoch_entries`. When the file is absent (fresh dir or pre-iter-5
    /// store) but the WAL replay produced entries, derive the boundary
    /// list by scanning the replayed entries and mark the in-memory
    /// state dirty so the next [`LogStore::flush`] persists it. This
    /// lets a store opened against a pre-existing WAL (e.g. one
    /// recovered from a Stage 7.2 snapshot) immediately answer
    /// epoch-divergence queries without forcing the engine to re-write
    /// every entry.
    fn load_or_backfill_epoch_checkpoint(&mut self) -> Result<()> {
        match read_leader_epoch_checkpoint(&self.dir) {
            Ok(entries) => {
                self.epoch_entries = entries;
            }
            Err(e) => {
                return Err(storage_err(format!(
                    "leader-epoch-checkpoint read failed: {e}"
                )));
            }
        }
        if self.epoch_entries.is_empty() && !self.entries.is_empty() {
            let mut last_epoch: Option<u64> = None;
            for (idx, entry) in self.entries.iter() {
                if last_epoch != Some(entry.term.0) {
                    self.epoch_entries.push(LeaderEpochEntry {
                        epoch: entry.term.0,
                        start_offset: idx.0,
                    });
                    last_epoch = Some(entry.term.0);
                }
            }
            if !self.epoch_entries.is_empty() {
                self.epoch_dirty = true;
            }
        }
        Ok(())
    }

    /// Stage 7.3 iter-5 — borrow the in-memory leader-epoch checkpoint
    /// snapshot. Exposed for tests and operator tooling that needs to
    /// verify divergence-detection inputs without re-reading the
    /// sidecar file.
    pub fn epoch_entries(&self) -> &[LeaderEpochEntry] {
        &self.epoch_entries
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

            // Stage 7.3 iter-5: maintain the leader-epoch checkpoint.
            // A new epoch entry is appended every time the term of the
            // newly-appended entry differs from the term of the most
            // recent epoch boundary. Same-term appends fall through
            // (no new boundary). The checkpoint is flushed durably by
            // [`Self::flush`] AFTER the WAL `sync_all` so a crash
            // never leaves the checkpoint referencing a non-durable
            // entry.
            let last_epoch = self.epoch_entries.last().map(|e| e.epoch);
            if last_epoch != Some(entry.term.0) {
                self.epoch_entries.push(LeaderEpochEntry {
                    epoch: entry.term.0,
                    start_offset: entry.index.0,
                });
                self.epoch_dirty = true;
            }
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
        {
            let seg_path = &self.segments[seg_idx].path;
            let f = OpenOptions::new()
                .write(true)
                .open(seg_path)
                .map_err(io_to_storage)?;
            f.set_len(byte_offset).map_err(io_to_storage)?;
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

        // Stage 7.3 iter-5: drop any epoch boundaries that anchor at
        // or past the truncation point. Boundaries with
        // `start_offset < index.0` survive because their epoch
        // pre-dates the divergence and still describes valid
        // (now-tail) entries. This implements the
        // "epoch-checkpoint divergence behaviour" required by
        // `implementation-plan.md:367` — after a leader-conflict
        // truncate, the checkpoint accurately reflects which
        // epoch-starts remain on this replica.
        let len_before = self.epoch_entries.len();
        self.epoch_entries.retain(|e| e.start_offset < index.0);
        if self.epoch_entries.len() != len_before {
            self.epoch_dirty = true;
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
        // Stage 7.3 iter-5: persist the leader-epoch checkpoint AFTER
        // the WAL fsync so the on-disk checkpoint never points at an
        // entry that is not yet durable. The checkpoint writer does
        // its own write-to-tmp-then-rename + fsync so the file is
        // crash-safe on its own.
        if self.epoch_dirty {
            write_leader_epoch_checkpoint(&self.dir, &self.epoch_entries)
                .map_err(|e| storage_err(format!("leader-epoch-checkpoint flush failed: {e}")))?;
            self.epoch_dirty = false;
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

        // Stage 7.3 iter-5: prune leader-epoch checkpoint entries that
        // are entirely covered by the snapshot. An entry with
        // `start_offset <= through` describes an epoch whose first
        // offset is now compacted; the SnapshotMeta's
        // `last_included_term` is the authoritative source of truth
        // for indices ≤ through, so the boundary list only needs to
        // anchor epochs whose first offset is still on-disk
        // (`start_offset > through`). Rubber-duck flag: must run here
        // AND in `truncate_from`, not just truncate_from.
        let len_before = self.epoch_entries.len();
        self.epoch_entries
            .retain(|e| e.start_offset > through_index_inclusive.0);
        if self.epoch_entries.len() != len_before {
            self.epoch_dirty = true;
        }

        Ok(())
    }

    fn first_valid_index(&self) -> LogIndex {
        // Internal `first_valid_index` stores "every index ≤ this is
        // compacted". The trait contract expects "lowest index still
        // logically present" — for a fresh log that's `LogIndex(1)`;
        // after a purge it's `internal + 1`. Translate accordingly so
        // the `on_log_compacted` reclaim math the driver does
        // (`through - (prev_first_valid - 1)`) lines up with the
        // pre-iter-5 default of `LogIndex(1)`.
        LogIndex(self.first_valid_index.0.saturating_add(1))
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

    #[test]
    fn leader_epoch_checkpoint_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        // Empty file is missing → reads as []
        let read = read_leader_epoch_checkpoint(dir.path()).unwrap();
        assert!(read.is_empty());
        let entries = vec![
            LeaderEpochEntry {
                epoch: 1,
                start_offset: 1,
            },
            LeaderEpochEntry {
                epoch: 3,
                start_offset: 17,
            },
            LeaderEpochEntry {
                epoch: 4,
                start_offset: 42,
            },
        ];
        write_leader_epoch_checkpoint(dir.path(), &entries).unwrap();
        let round = read_leader_epoch_checkpoint(dir.path()).unwrap();
        assert_eq!(round, entries);
        // Header version line must be `0`
        let raw = std::fs::read_to_string(dir.path().join(LEADER_EPOCH_CHECKPOINT_FILE)).unwrap();
        let mut lines = raw.lines();
        assert_eq!(lines.next(), Some("0"));
        assert_eq!(lines.next(), Some("3"));
    }

    #[test]
    fn leader_epoch_checkpoint_bad_version_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LEADER_EPOCH_CHECKPOINT_FILE), "99\n0\n").unwrap();
        let err = read_leader_epoch_checkpoint(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

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

    /// Create a fresh temp directory for a test, cleaning up any
    /// prior run. Names are scoped by `(test-name, pid,
    /// per-process counter)` so a second cargo-test invocation —
    /// or a parallel test binary that happens to reuse a test name
    /// — can never collide with a still-open file handle from an
    /// earlier run. The earlier shared-name design was prone to
    /// Windows `os error 5: Access is denied` flakes because the
    /// OS lags file-handle release across process exits.
    fn test_dir(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let pid = std::process::id();
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir()
            .join("xraft-wal-tests")
            .join(format!("{name}-{pid}-{seq}"));
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
        assert!(
            result.is_err(),
            "Snapshot entries must be rejected by MemoryLogStore"
        );
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

    // -- Stage 7.3 iter-5: leader-epoch checkpoint integration -------------

    #[test]
    fn file_log_append_records_new_epoch_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FileLogStore::open(dir.path()).unwrap();
        // Same-term appends create exactly ONE boundary.
        log.append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)])
            .unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[LeaderEpochEntry {
                epoch: 1,
                start_offset: 1,
            }]
        );
        // A term bump opens a new boundary at the entry that triggered it.
        log.append(&[make_entry(4, 2), make_entry(5, 2)]).unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[
                LeaderEpochEntry {
                    epoch: 1,
                    start_offset: 1,
                },
                LeaderEpochEntry {
                    epoch: 2,
                    start_offset: 4,
                },
            ]
        );
        // Skipping a term is fine — the next boundary records that.
        log.append(&[make_entry(6, 5)]).unwrap();
        assert_eq!(log.epoch_entries().last().unwrap().epoch, 5);
        assert_eq!(log.epoch_entries().last().unwrap().start_offset, 6);
    }

    #[test]
    fn file_log_flush_persists_epoch_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = FileLogStore::open(dir.path()).unwrap();
            log.append(&[make_entry(1, 1), make_entry(2, 2), make_entry(3, 3)])
                .unwrap();
            log.flush().unwrap();
        }
        // Reopen: the on-disk checkpoint should be the authoritative
        // source (no backfill needed).
        let log = FileLogStore::open(dir.path()).unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[
                LeaderEpochEntry {
                    epoch: 1,
                    start_offset: 1,
                },
                LeaderEpochEntry {
                    epoch: 2,
                    start_offset: 2,
                },
                LeaderEpochEntry {
                    epoch: 3,
                    start_offset: 3,
                },
            ]
        );
    }

    #[test]
    fn file_log_open_backfills_epoch_checkpoint_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Append entries WITHOUT flushing → no checkpoint file
            // written. To simulate a pre-iter-5 store, manually remove
            // any checkpoint file the new append path may have created
            // in-memory but never persisted.
            let mut log = FileLogStore::open(dir.path()).unwrap();
            log.append(&[
                make_entry(1, 1),
                make_entry(2, 1),
                make_entry(3, 2),
                make_entry(4, 4),
            ])
            .unwrap();
            // Sync only the WAL (not the checkpoint): the WAL fsync is
            // implicit on drop because the OS will buffer-flush, but
            // to keep the test deterministic on platforms with weaker
            // close semantics we force-sync just the WAL.
            // (We deliberately do NOT call `log.flush()` — that would
            //  also write the checkpoint.)
            // Touch the active writer to force a fdatasync-equivalent:
            if let Some(ref w) = log.active_writer {
                w.sync_all().unwrap();
            }
            drop(log);
            // Remove any stale checkpoint (defensive — none should
            // exist because we never called flush()).
            let chk = dir.path().join(LEADER_EPOCH_CHECKPOINT_FILE);
            if chk.exists() {
                std::fs::remove_file(&chk).unwrap();
            }
        }
        let log = FileLogStore::open(dir.path()).unwrap();
        // Backfill walks the recovered WAL entries and reconstructs
        // every (term, first_offset) boundary.
        assert_eq!(
            log.epoch_entries(),
            &[
                LeaderEpochEntry {
                    epoch: 1,
                    start_offset: 1,
                },
                LeaderEpochEntry {
                    epoch: 2,
                    start_offset: 3,
                },
                LeaderEpochEntry {
                    epoch: 4,
                    start_offset: 4,
                },
            ]
        );
    }

    #[test]
    fn file_log_truncate_from_prunes_epoch_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FileLogStore::open(dir.path()).unwrap();
        log.append(&[
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 2),
            make_entry(4, 3),
            make_entry(5, 3),
        ])
        .unwrap();
        // Truncate from index 3 → boundary at start_offset 3 (epoch 2)
        // and 4 (epoch 3) are dropped; the epoch-1 boundary survives.
        log.truncate_from(LogIndex(3)).unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[LeaderEpochEntry {
                epoch: 1,
                start_offset: 1,
            }]
        );
        // Flush + reopen: the pruned checkpoint persists.
        log.flush().unwrap();
        drop(log);
        let log = FileLogStore::open(dir.path()).unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[LeaderEpochEntry {
                epoch: 1,
                start_offset: 1,
            }]
        );
    }

    #[test]
    fn file_log_purge_prefix_prunes_epoch_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FileLogStore::open(dir.path()).unwrap();
        log.append(&[
            make_entry(1, 1),
            make_entry(2, 2),
            make_entry(3, 2),
            make_entry(4, 3),
            make_entry(5, 3),
        ])
        .unwrap();
        // Snapshot through index 3 → entries with start_offset <= 3
        // are dropped; only the epoch-3 boundary (start_offset = 4)
        // survives.
        log.purge_prefix(LogIndex(3)).unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[LeaderEpochEntry {
                epoch: 3,
                start_offset: 4,
            }]
        );
        log.flush().unwrap();
        drop(log);
        let log = FileLogStore::open(dir.path()).unwrap();
        assert_eq!(
            log.epoch_entries(),
            &[LeaderEpochEntry {
                epoch: 3,
                start_offset: 4,
            }]
        );
    }

    #[test]
    fn file_log_first_valid_index_translates_default_and_after_purge() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = FileLogStore::open(dir.path()).unwrap();
        // Fresh log: trait first_valid_index is LogIndex(1) (matches
        // the default trait impl) so the driver's reclaim math
        // (`through + 1 - prev`) computes a correct count on first
        // compaction.
        assert_eq!(log.first_valid_index(), LogIndex(1));
        log.append(&[
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 2),
        ])
        .unwrap();
        log.purge_prefix(LogIndex(2)).unwrap();
        // After purge through 2, lowest valid index is 3.
        assert_eq!(log.first_valid_index(), LogIndex(3));
        // Idempotent re-purge through 2 leaves it unchanged.
        log.purge_prefix(LogIndex(2)).unwrap();
        assert_eq!(log.first_valid_index(), LogIndex(3));
    }
}
