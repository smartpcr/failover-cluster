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
use xraft_core::storage::{LogStore, SnapshotMeta};
use xraft_core::types::{LogIndex, Term, VoterSet};

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

/// fsync a directory entry so that file create/delete operations on it
/// survive a crash.
///
/// On Unix-family operating systems POSIX requires opening the directory
/// for read and calling `fsync` to durably flush directory metadata.
/// On Windows there is no documented per-directory sync — `File::open` on
/// a directory fails and metadata for `NTFS` is journaled separately, so
/// this is a no-op there.
fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let f = File::open(dir).map_err(io_to_storage)?;
        f.sync_all().map_err(io_to_storage)?;
    }
    #[cfg(not(unix))]
    {
        // Reference the parameter so the lint stays quiet on non-Unix.
        let _ = dir;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// MemoryLogStore
// ---------------------------------------------------------------------------

/// In-memory log store backed by a simple `Vec`.
///
/// **Not suitable for production** — entries are lost on restart.
#[derive(Debug, Default)]
pub struct MemoryLogStore {
    entries: Vec<Entry>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStore for MemoryLogStore {
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
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

/// Fixed byte overhead per entry: index(8) + term(8) + tag(1) + payload_len(4).
const ENTRY_HEADER_LEN: usize = 21;

/// Metadata for a single WAL segment file.
#[derive(Debug)]
struct SegmentInfo {
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
#[derive(Debug)]
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
        };

        store.recover()?;
        Ok(store)
    }

    // -- recovery ----------------------------------------------------------

    /// Scan existing segment files, replay valid frames, and truncate any
    /// corrupt tail on the last segment.
    fn recover(&mut self) -> Result<()> {
        let wal_files: Vec<_> = fs::read_dir(&self.dir)
            .map_err(io_to_storage)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == SEGMENT_EXT))
            .collect();

        // Sort by parsed base_index rather than raw file_name to make
        // segment ordering explicit and independent of any future
        // change to the filename format. Filenames are validated up
        // front so a corrupted/foreign file aborts recovery cleanly.
        let mut parsed: Vec<(LogIndex, PathBuf)> = wal_files
            .into_iter()
            .map(|de| {
                let path = de.path();
                parse_segment_filename(&path).map(|idx| (idx, path))
            })
            .collect::<Result<Vec<_>>>()?;
        parsed.sort_by_key(|(idx, _)| *idx);

        let count = parsed.len();
        let mut expected_next: Option<LogIndex> = None;
        for (i, (base_index, path)) in parsed.into_iter().enumerate() {
            let seg_idx = self.segments.len();
            self.segments.push(SegmentInfo { path: path.clone() });
            let is_last = i == count - 1;
            expected_next =
                self.recover_segment(&path, base_index, seg_idx, is_last, expected_next)?;
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
    /// a **torn / truncated** tail (frame header or body cut short) is
    /// silently trimmed; CRC or semantic corruption on a complete frame is
    /// always a hard error so we never silently lose committed entries to
    /// bit-rot. Earlier segments must decode cleanly throughout.
    ///
    /// `base_index` is the index encoded in the segment filename. The
    /// first valid frame must match it — a mismatch indicates a renamed
    /// or corrupted file and is rejected so the recovery does not
    /// silently load entries at the wrong logical position.
    ///
    /// `expected_next_index` (if `Some`) is the index that the first
    /// entry of this segment must equal to maintain global log
    /// contiguity across segments. Returns the new expected-next-index
    /// after replay (`last_recovered_index + 1`, or the unchanged value
    /// if no entries were recovered).
    fn recover_segment(
        &mut self,
        path: &Path,
        base_index: LogIndex,
        seg_idx: usize,
        is_last: bool,
        expected_next_index: Option<LogIndex>,
    ) -> Result<Option<LogIndex>> {
        let buf = fs::read(path).map_err(io_to_storage)?;
        let mut cursor: usize = 0;
        let mut first_in_segment = true;
        let mut last_index_seen: Option<LogIndex> = None;

        while cursor < buf.len() {
            let frame_start = cursor;
            match Self::decode_frame(&buf, cursor) {
                Ok((entry, next)) => {
                    if first_in_segment {
                        if entry.index != base_index {
                            return Err(storage_err(format!(
                                "segment {} first entry index {} does not match \
                                 filename base_index {}",
                                path.display(),
                                entry.index.0,
                                base_index.0,
                            )));
                        }
                        if let Some(expected) = expected_next_index
                            && entry.index != expected
                        {
                            return Err(storage_err(format!(
                                "non-contiguous WAL: segment {} starts at \
                                 index {} but previous segment ended expecting {}",
                                path.display(),
                                entry.index.0,
                                expected.0,
                            )));
                        }
                    } else if let Some(prev) = last_index_seen
                        && entry.index.0 != prev.0 + 1
                    {
                        return Err(storage_err(format!(
                            "non-contiguous WAL: segment {} jumps from index \
                             {} to {}",
                            path.display(),
                            prev.0,
                            entry.index.0,
                        )));
                    }
                    first_in_segment = false;
                    last_index_seen = Some(entry.index);
                    self.offsets
                        .insert(entry.index, (seg_idx, frame_start as u64));
                    self.entries.insert(entry.index, entry);
                    cursor = next;
                }
                Err(e) if is_last && Self::is_truncated_frame_error(&e) => {
                    // Torn write at the end of the most recent segment —
                    // standard crash-recovery: trim and fsync.
                    tracing::warn!(
                        "WAL: trimming torn tail of {} starting at byte {}: {}",
                        path.display(),
                        frame_start,
                        e
                    );
                    let f = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(io_to_storage)?;
                    f.set_len(frame_start as u64).map_err(io_to_storage)?;
                    f.sync_all().map_err(io_to_storage)?;
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(last_index_seen
            .map(|i| LogIndex(i.0 + 1))
            .or(expected_next_index))
    }

    /// Returns true when the error indicates an incomplete frame at end
    /// of buffer (torn write), as opposed to CRC or semantic corruption
    /// of a complete frame. Used by recovery to decide whether to
    /// silently trim or fail loud.
    fn is_truncated_frame_error(e: &XRaftError) -> bool {
        matches!(e, XRaftError::Storage(s) if s.starts_with("truncated frame"))
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
    fn serialize_entry(entry: &Entry) -> Vec<u8> {
        let (tag, payload_bytes) = match &entry.payload {
            EntryPayload::NoOp => (PAYLOAD_TAG_NOOP, Vec::new()),
            EntryPayload::Command(b) => (PAYLOAD_TAG_COMMAND, b.to_vec()),
            EntryPayload::Snapshot(meta) => {
                let encoded =
                    bincode::serialize(meta).expect("SnapshotMeta serialization should not fail");
                (PAYLOAD_TAG_SNAPSHOT, encoded)
            }
            EntryPayload::ConfigChange(voter_set) => {
                let encoded =
                    bincode::serialize(voter_set).expect("VoterSet serialization should not fail");
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
                let meta: SnapshotMeta = bincode::deserialize(payload_data)
                    .map_err(|e| storage_err(format!("SnapshotMeta decode failed: {e}")))?;
                EntryPayload::Snapshot(meta)
            }
            PAYLOAD_TAG_CONFIG_CHANGE => {
                let voter_set: VoterSet = bincode::deserialize(payload_data)
                    .map_err(|e| storage_err(format!("VoterSet decode failed: {e}")))?;
                EntryPayload::ConfigChange(voter_set)
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

        // Defensively wipe any stale bytes left behind by a partially
        // completed truncation that didn't durably remove the previous
        // file at this path. Without this a follow-up `append` could
        // write past stale entries, producing a torn / mixed segment
        // that corrupts recovery.
        {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(io_to_storage)?;
            f.sync_all().map_err(io_to_storage)?;
        }
        // Reopen in append mode so subsequent writes are atomically
        // positioned at end-of-file (defending against any future code
        // that might race on the file handle).
        let file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(io_to_storage)?;

        self.segments.push(SegmentInfo { path });
        self.active_writer = Some(file);
        self.active_segment_size = 0;
        // Persist the new directory entry so a crash before the first
        // batch fsync doesn't lose the filename.
        sync_dir(&self.dir)?;
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
        if entries.is_empty() {
            return Ok(());
        }

        // Continuity check: the batch must be strictly monotonic by index
        // and (after the first batch) start at last_index + 1. Catching
        // gaps and out-of-order writes here prevents a corrupt log from
        // ever reaching disk — far easier to debug than a recovery-time
        // failure on a different node.
        let current_last = self.last_index();
        let expected_first = if current_last.0 == 0 {
            None
        } else {
            Some(LogIndex(current_last.0 + 1))
        };
        for (i, entry) in entries.iter().enumerate() {
            let expected = if i == 0 {
                expected_first.unwrap_or(entry.index)
            } else {
                LogIndex(entries[i - 1].index.0 + 1)
            };
            if entry.index != expected {
                return Err(storage_err(format!(
                    "non-contiguous append: entry[{i}].index = {} but \
                     expected {} (current last_index = {})",
                    entry.index.0, expected.0, current_last.0,
                )));
            }
        }

        // Stage the per-entry mutations and apply them only after the
        // disk write + fsync succeed. If sync_all fails the WAL must
        // not advertise entries it couldn't persist.
        let mut staged: Vec<(LogIndex, Entry, usize, u64)> = Vec::with_capacity(entries.len());

        for entry in entries {
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
            staged.push((entry.index, entry.clone(), seg_idx, offset));
        }

        // fsync after each append batch so the Raft layer can rely on
        // durability the moment append() returns. Without this the bytes
        // sit only in the OS page cache and can be lost on power failure
        // — violating Raft's persistence contract.
        if let Some(ref w) = self.active_writer {
            w.sync_all().map_err(io_to_storage)?;
        }

        // Now safe to publish in-memory state.
        for (idx, entry, seg_idx, offset) in staged {
            self.entries.insert(idx, entry);
            self.offsets.insert(idx, (seg_idx, offset));
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
            // fsync the truncation so it survives a crash. Conflict
            // resolution relies on the truncated tail being gone for good.
            f.sync_all().map_err(io_to_storage)?;
        }

        // Delete all subsequent segment files. Errors must propagate —
        // a stale post-truncation segment would resurrect divergent
        // entries on the next restart.
        let to_drop: Vec<SegmentInfo> = self.segments.drain(seg_idx + 1..).collect();
        for seg in &to_drop {
            fs::remove_file(&seg.path).map_err(io_to_storage)?;
        }

        // If the truncated segment is now empty, remove it as well.
        let mut removed_dir_entry = !to_drop.is_empty();
        if byte_offset == 0
            && let Some(seg) = self.segments.pop()
        {
            fs::remove_file(&seg.path).map_err(io_to_storage)?;
            removed_dir_entry = true;
        }

        // Persist the directory metadata if we changed it.
        if removed_dir_entry {
            sync_dir(&self.dir)?;
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::message::{Entry, EntryPayload};

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

        // Recovery must reject CRC mismatch on a complete frame —
        // silent truncation would let bit-rot lose committed entries.
        // The follower can then refuse to start so the operator can
        // re-replicate from the leader or restore from a snapshot.
        {
            let err = FileLogStore::open(&dir).expect_err("CRC mismatch must surface");
            let msg = format!("{err}");
            assert!(
                msg.contains("CRC mismatch"),
                "expected CRC error, got: {msg}"
            );
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
    fn file_snapshot_entry_roundtrip() {
        let dir = test_dir("file_snapshot_entry_roundtrip");
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
            payload: EntryPayload::Snapshot(meta.clone()),
        };

        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[entry]).unwrap();
            log.flush().unwrap();
        }

        let log = FileLogStore::open(&dir).unwrap();
        let recovered = log.get(LogIndex(11)).unwrap().unwrap();
        match recovered.payload {
            EntryPayload::Snapshot(ref m) => {
                assert_eq!(m.id, "snap-1");
                assert_eq!(m.last_included_index, LogIndex(10));
                assert_eq!(m.last_included_term, Term(3));
            }
            _ => panic!("expected Snapshot payload"),
        }
    }

    // Work-item scenario: append-and-read.
    // Given an empty FileLogStore, when 100 entries are appended,
    // then `get(i)` returns the correct entry for each index and
    // `last_index` returns 100.
    #[test]
    fn file_append_and_read_100_entries() {
        let dir = test_dir("file_append_and_read_100_entries");
        let mut log = FileLogStore::open(&dir).unwrap();

        let entries: Vec<Entry> = (1..=100)
            .map(|i| make_cmd_entry(i, 1, format!("entry-{i}").as_bytes()))
            .collect();
        log.append(&entries).unwrap();

        assert_eq!(log.last_index(), LogIndex(100));
        assert_eq!(log.last_term(), Term(1));

        for i in 1..=100u64 {
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
            match e.payload {
                EntryPayload::Command(ref b) => {
                    assert_eq!(&b[..], format!("entry-{i}").as_bytes());
                }
                _ => panic!("expected Command payload at {i}"),
            }
        }
    }

    // Work-item scenario: truncate-divergent.
    // Given a log with 50 entries, when `truncate_from(30)` is called,
    // then `last_index` returns 29 and `get(30)` returns None.
    #[test]
    fn file_truncate_from_index_30_of_50() {
        let dir = test_dir("file_truncate_from_index_30_of_50");
        let mut log = FileLogStore::open(&dir).unwrap();

        let entries: Vec<Entry> = (1..=50).map(|i| make_entry(i, 1)).collect();
        log.append(&entries).unwrap();
        assert_eq!(log.last_index(), LogIndex(50));

        log.truncate_from(LogIndex(30)).unwrap();

        assert_eq!(log.last_index(), LogIndex(29));
        assert!(log.get(LogIndex(30)).unwrap().is_none());
        assert!(log.get(LogIndex(50)).unwrap().is_none());
        // Entries before the truncation point must be intact.
        assert_eq!(log.get(LogIndex(29)).unwrap().unwrap().index, LogIndex(29));
        assert_eq!(log.get(LogIndex(1)).unwrap().unwrap().index, LogIndex(1));
    }

    // Append-then-fsync contract: data must survive a process crash
    // (modeled by `drop`) without the caller invoking `flush()`. This
    // test would fail if `append` stopped fsync'ing at the end of the
    // batch.
    #[test]
    fn file_appends_are_durable_without_explicit_flush() {
        let dir = test_dir("file_appends_are_durable_without_explicit_flush");

        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[
                make_cmd_entry(1, 1, b"alpha"),
                make_cmd_entry(2, 1, b"beta"),
                make_cmd_entry(3, 2, b"gamma"),
            ])
            .unwrap();
            // No explicit flush — durability must come from append().
        }

        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(3));
        assert_eq!(log.last_term(), Term(2));
        match log.get(LogIndex(2)).unwrap().unwrap().payload {
            EntryPayload::Command(ref b) => assert_eq!(&b[..], b"beta"),
            _ => panic!("expected Command payload"),
        }
    }

    // A truncation must itself be durable. Without fsync after
    // `set_len`, a crash could resurrect entries the conflict-resolution
    // path believed it had erased.
    #[test]
    fn file_truncation_is_durable_without_explicit_flush() {
        let dir = test_dir("file_truncation_is_durable");

        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&(1..=20).map(|i| make_entry(i, 1)).collect::<Vec<_>>())
                .unwrap();
            log.truncate_from(LogIndex(10)).unwrap();
            // No flush — truncation durability must come from truncate_from().
        }

        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(9));
        assert!(log.get(LogIndex(10)).unwrap().is_none());
        assert!(log.get(LogIndex(20)).unwrap().is_none());
    }

    // Exercises segment rotation explicitly with the documented
    // 1 KB threshold from the work-item scenario.
    #[test]
    fn file_segment_rotation_with_1kb_threshold() {
        let dir = test_dir("file_segment_rotation_1kb");
        let mut log = FileLogStore::open_with_max_segment_size(&dir, 1024).unwrap();

        // Each command-entry frame is roughly 50–60 bytes here, so 60
        // entries comfortably exceeds 1 KB and forces multiple
        // rotations.
        for i in 1..=60u64 {
            log.append(&[make_cmd_entry(i, 1, b"payload-bytes")])
                .unwrap();
        }

        assert!(
            log.segments.len() >= 2,
            "expected at least one rotation, got {} segment(s)",
            log.segments.len()
        );
        assert_eq!(log.last_index(), LogIndex(60));

        // Reads must succeed across segment boundaries.
        for i in 1..=60u64 {
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
        }

        // get_range across a segment boundary returns a contiguous
        // slice in index order.
        let range = log.get_range(LogIndex(20), LogIndex(40)).unwrap();
        assert_eq!(range.len(), 20);
        for (offset, e) in range.iter().enumerate() {
            assert_eq!(e.index, LogIndex(20 + offset as u64));
        }
    }

    // Work-item scenario: crash-recovery reconstructs the in-memory
    // index after restart so subsequent reads continue to be O(1).
    #[test]
    fn file_in_memory_index_rebuilt_on_restart() {
        let dir = test_dir("file_in_memory_index_rebuilt");

        {
            let mut log = FileLogStore::open_with_max_segment_size(&dir, 256).unwrap();
            for i in 1..=30u64 {
                log.append(&[make_cmd_entry(i, 1, b"x")]).unwrap();
            }
            // No flush — durability is part of append's contract.
        }

        let log = FileLogStore::open_with_max_segment_size(&dir, 256).unwrap();

        // Every previously-written index must be present in the
        // rebuilt offset index.
        assert_eq!(log.entries.len(), 30);
        assert_eq!(log.offsets.len(), 30);
        for i in 1..=30u64 {
            assert!(log.offsets.contains_key(&LogIndex(i)));
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
        }
        assert_eq!(log.last_index(), LogIndex(30));
    }

    #[test]
    fn file_append_empty_batch_is_noop() {
        let dir = test_dir("file_append_empty_batch_is_noop");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[]).unwrap();
        assert_eq!(log.last_index(), LogIndex(0));
        // Empty batch must not create a segment file.
        let wal_count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == SEGMENT_EXT))
            .count();
        assert_eq!(wal_count, 0);
    }

    // The append-time continuity check rejects a batch that skips an
    // index (gap), avoiding an inconsistent WAL.
    #[test]
    fn file_append_rejects_index_gap() {
        let dir = test_dir("file_append_rejects_index_gap");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[make_entry(1, 1)]).unwrap();
        let err = log
            .append(&[make_entry(3, 1)])
            .expect_err("gap must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("non-contiguous"), "got: {msg}");
        // The bad batch must not have left state behind.
        assert_eq!(log.last_index(), LogIndex(1));
    }

    #[test]
    fn file_append_rejects_out_of_order_within_batch() {
        let dir = test_dir("file_append_rejects_out_of_order_within_batch");
        let mut log = FileLogStore::open(&dir).unwrap();
        let err = log
            .append(&[make_entry(1, 1), make_entry(3, 1), make_entry(2, 1)])
            .expect_err("out-of-order entries must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("non-contiguous"), "got: {msg}");
        // Nothing must have been published to in-memory state.
        assert_eq!(log.last_index(), LogIndex(0));
    }

    // ConfigChange entries roundtrip through the WAL frame format and
    // survive crash recovery.
    #[test]
    fn file_config_change_entry_roundtrip() {
        use xraft_core::types::{DirectoryId, Endpoint, NodeId, VoterRecord, VoterSet};
        let dir = test_dir("file_config_change_entry_roundtrip");

        let voter = VoterRecord {
            node_id: NodeId(7),
            directory_id: DirectoryId(uuid::Uuid::new_v4()),
            endpoints: vec![Endpoint {
                host: "127.0.0.1".to_string(),
                port: 5432,
            }],
        };
        let voter_set = VoterSet::try_new(vec![voter.clone()]).expect("valid voter set");
        let entry = Entry {
            index: LogIndex(1),
            term: Term(2),
            payload: EntryPayload::ConfigChange(voter_set.clone()),
        };

        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[entry]).unwrap();
        }

        let log = FileLogStore::open(&dir).unwrap();
        let recovered = log.get(LogIndex(1)).unwrap().unwrap();
        match recovered.payload {
            EntryPayload::ConfigChange(ref vs) => {
                assert_eq!(vs.voters().len(), 1);
                assert_eq!(vs.voters()[0].node_id, NodeId(7));
            }
            _ => panic!("expected ConfigChange payload"),
        }
    }

    // If a stale segment file exists at the path of a fresh segment
    // (e.g. a previous truncation deleted entries from memory but the
    // file removal failed and was silently retried), `create_segment`
    // must wipe it so the next append doesn't read mixed bytes after
    // recovery.
    #[test]
    fn file_create_segment_wipes_stale_file() {
        let dir = test_dir("file_create_segment_wipes_stale_file");

        // Pre-plant a "stale" segment file at the path that the WAL
        // will use for its first segment (base_index = 1).
        fs::write(dir.join(segment_filename(LogIndex(1))), b"GARBAGE_BYTES").unwrap();

        let mut log = FileLogStore::open(&dir).unwrap();
        // Recovery rejects the garbage file; we have to start fresh.
        // Wait — actually open() will try to recover the planted file
        // and fail on the bad header. The planted file is a foreign
        // file at a valid segment-name path, so let's first write
        // valid entries then simulate stale-file by manipulating
        // segments through normal API paths.
        log.append(&[make_entry(1, 1)]).unwrap();
        // Drop the writer so we can mutate the file out-of-band.
        drop(log);

        // Re-plant garbage at the segment path.
        let seg_path = dir.join(segment_filename(LogIndex(1)));
        // Re-open: should succeed (valid frame at start of file),
        // then fully truncate so the segment is removed.
        let mut log = FileLogStore::open(&dir).unwrap();
        log.truncate_from(LogIndex(1)).unwrap();
        assert!(!seg_path.exists(), "truncate must remove empty segment");

        // Re-plant a stale file at the same path with garbage. In a
        // real crash this could happen if a previous create_segment
        // wrote some bytes before crashing.
        fs::write(&seg_path, b"STALE_GARBAGE_DATA").unwrap();

        // A fresh append at index 1 must wipe the stale file before
        // writing — otherwise the next recovery would either fail or
        // load the stale bytes.
        log.append(&[make_entry(1, 5)]).unwrap();
        drop(log);

        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(1));
        assert_eq!(log.get(LogIndex(1)).unwrap().unwrap().term, Term(5));
    }

    // Recovery must reject a WAL whose segment files describe a
    // non-contiguous index sequence (e.g. seg 1 ends at index 5 but
    // seg 2 starts at index 8). This catches a class of bugs where a
    // partial truncate left orphan segments behind.
    #[test]
    fn file_recovery_rejects_gap_between_segments() {
        let dir = test_dir("file_recovery_rejects_gap_between_segments");

        // Build a clean two-segment log [1..=4] across a small cap.
        {
            let mut log = FileLogStore::open_with_max_segment_size(&dir, 80).unwrap();
            for i in 1..=4u64 {
                log.append(&[make_entry(i, 1)]).unwrap();
            }
            // Sanity: rotation actually happened.
            assert!(log.segments.len() >= 2);
        }

        // Manually plant a third segment whose first entry's index
        // creates a gap (jumps past last_index).
        let gap_seg_path = dir.join(segment_filename(LogIndex(99)));
        let frame = FileLogStore::encode_frame(&make_entry(99, 1));
        fs::write(&gap_seg_path, &frame).unwrap();

        let err = FileLogStore::open(&dir).expect_err("non-contiguous WAL must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("non-contiguous"), "got: {msg}");
    }

    // Recovery must reject a WAL whose segment-filename base_index
    // doesn't match the first frame's index (e.g. a stale file moved
    // into the directory).
    #[test]
    fn file_recovery_rejects_filename_index_mismatch() {
        let dir = test_dir("file_recovery_rejects_filename_index_mismatch");

        // Write a single valid entry whose index is 5, but encode it
        // and store it under a filename claiming base_index = 1.
        let frame = FileLogStore::encode_frame(&make_entry(5, 1));
        fs::write(dir.join(segment_filename(LogIndex(1))), &frame).unwrap();

        let err = FileLogStore::open(&dir).expect_err("filename mismatch must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("does not match filename base_index"),
            "got: {msg}"
        );
    }
}
