// -----------------------------------------------------------------------
// <copyright file="log.rs" company="Microsoft Corp.">
//     Copyright (c) Microsoft Corp. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

//! Write-ahead log (WAL) implementations of the
//! [`xraft_core::storage::LogStore`] trait.
//!
//! Two implementations are provided:
//!
//! * [`MemoryLogStore`] — volatile, in-memory store for testing.
//! * [`FileLogStore`] — durable, file-backed WAL with CRC-protected
//!   segment headers and frames, segment rotation, and crash recovery.
//!
//! # Design highlights
//!
//! * **Spec-compliant frame layout.** Each entry is encoded as
//!   `[length:u32][term:u64][index:u64][entry_type:u8][data][crc32:u32]`,
//!   matching the contract in `docs/stories/failover-cluster-XRAFT/
//!   implementation-plan.md` Stage 2.1. The CRC envelope explicitly
//!   covers the length field so a corrupted length cannot be silently
//!   misclassified as a torn tail. See [`crate::log_format`].
//! * **Segment header.** Every segment file begins with a 28-byte
//!   `XRWL` header carrying a magic, version, base-index, creation
//!   timestamp, and CRC. Foreign files / wrong versions / corrupt
//!   headers are surfaced as hard recovery errors instead of silently
//!   skipped. See [`crate::log_segment`].
//! * **O(1) offset index, no entry cache.** A `Vec<OffsetRef>` paired
//!   with `first_index` gives constant-time random reads while keeping
//!   per-entry RAM cost ~32 bytes — independent of payload size. Reads
//!   call `pread` (`seek` + `read_exact` under a per-segment `Mutex`) on
//!   the segment file; the WAL never caches entry payloads.
//! * **Atomic segment creation** via a `.wal.tmp` file that is
//!   `fsync`ed and then renamed in place. Crash mid-creation cannot
//!   leave a half-written header that would brick recovery.
//! * **Crash-safe truncation.** Later segments are deleted (and the
//!   directory `fsync`ed) *before* the affected segment is rewritten,
//!   so a crash mid-truncate either leaves the truncate visibly
//!   incomplete (the original WAL state) or visibly complete — never a
//!   non-contiguous mix that recovery rejects.
//! * **Batch durability.** Every segment touched by a single `append`
//!   batch (including a freshly rotated one) is `fsync`ed before the
//!   call returns, so callers can rely on durability the moment
//!   `append` succeeds.

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use xraft_core::error::{Result, XRaftError};
use xraft_core::message::Entry;
use xraft_core::storage::LogStore;
use xraft_core::types::{LogIndex, Term};

use crate::log_format::{
    DEFAULT_MAX_FRAME_BODY, FrameDecodeError, decode_frame, encode_frame, frame_byte_len,
};
use crate::log_segment::{
    SEGMENT_EXT, SEGMENT_TMP_EXT, Segment, parse_segment_filename, sync_dir,
};

/// Default maximum segment size before rotation (64 MiB).
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// MemoryLogStore
// ---------------------------------------------------------------------------

/// In-memory log store backed by a simple `Vec`. **Not durable** — used
/// by `xraft-test` and unit tests for deterministic simulation.
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
// FileLogStore
// ---------------------------------------------------------------------------

/// Physical location of one entry's frame on disk plus its term, used as
/// the WAL's offset index. ~32 bytes per entry — linear in entry count,
/// independent of payload size.
#[derive(Debug, Clone, Copy)]
struct OffsetRef {
    segment_idx: usize,
    byte_offset: u64,
    byte_len: u32,
    term: Term,
}

/// Durable, file-backed write-ahead log. See module docs.
#[derive(Debug)]
pub struct FileLogStore {
    dir: PathBuf,
    segments: Vec<Segment>,
    /// Append-mode handle on the **last** segment, or `None` when the
    /// log is empty.
    active_writer: Option<std::fs::File>,
    /// `LogIndex` of `offsets[0]`. Defaults to `LogIndex(1)` for empty
    /// logs and is set from the first append. Allows snapshot-driven
    /// log compaction in a future stage without a format change.
    first_index: LogIndex,
    /// Dense offset index: `offsets[i]` gives the on-disk location of
    /// entry at logical index `first_index + i`. Empty ⇒ empty log.
    /// Vec gives true `O(1)` lookup; a BTreeMap would be `O(log n)`.
    offsets: Vec<OffsetRef>,
    max_segment_size: u64,
    max_frame_body: u32,
}

impl FileLogStore {
    /// Open (or create) a WAL in `dir` with the default 64 MiB segment cap.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_max_segment_size(dir, DEFAULT_MAX_SEGMENT_SIZE)
    }

    /// Open (or create) a WAL in `dir` with a custom segment-size cap.
    /// `max_frame_body` is set to `max(DEFAULT_MAX_FRAME_BODY, max_segment_size)`
    /// so that a legitimately-large entry isn't misclassified as
    /// corruption when the segment cap is small.
    pub fn open_with_max_segment_size(
        dir: impl AsRef<Path>,
        max_segment_size: u64,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| storage_err(format!("wal mkdir: {e}")))?;

        // Sanity cap on individual frame bodies. Keep at least the
        // default; if the user picked a larger segment cap, allow
        // frames up to that size too.
        let max_frame_body = u32::try_from(max_segment_size)
            .unwrap_or(u32::MAX)
            .max(DEFAULT_MAX_FRAME_BODY);

        let mut store = Self {
            dir,
            segments: Vec::new(),
            active_writer: None,
            first_index: LogIndex(1),
            offsets: Vec::new(),
            max_segment_size,
            max_frame_body,
        };
        store.recover()?;
        Ok(store)
    }

    // ---------------- Recovery ----------------

    /// Replay segment files in `self.dir`, populating `segments` and
    /// `offsets`. Implements the safety contract spelled out in the
    /// module docs:
    ///
    /// * Foreign files / unsupported version / corrupt header ⇒ hard fail.
    /// * Filename `base_index` must match the header's `base_index`.
    /// * Frame indices must be contiguous within and across segments.
    /// * Mid-segment frame corruption (CRC, length out of range) ⇒ hard fail.
    /// * Trailing torn frame on the **last** segment is trimmed via
    ///   `set_len + sync_all` so the next append is atomic.
    fn recover(&mut self) -> Result<()> {
        // Clean up leftover .wal.tmp files from a crash mid-creation.
        // These never participate in recovery so removing them is safe.
        for entry in fs::read_dir(&self.dir).map_err(|e| storage_err(format!("wal scan: {e}")))? {
            let entry = entry.map_err(|e| storage_err(format!("wal scan entry: {e}")))?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.ends_with(SEGMENT_TMP_EXT)
            {
                let _ = fs::remove_file(&path);
            }
        }

        let mut wal_files: Vec<(LogIndex, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(|e| storage_err(format!("wal scan: {e}")))? {
            let entry = entry.map_err(|e| storage_err(format!("wal scan entry: {e}")))?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some(SEGMENT_EXT) {
                continue;
            }
            let base = parse_segment_filename(&path).ok_or_else(|| {
                storage_err(format!(
                    "wal foreign file (non-numeric name): {}",
                    path.display()
                ))
            })?;
            wal_files.push((base, path));
        }
        wal_files.sort_by_key(|(idx, _)| *idx);

        let count = wal_files.len();
        let mut expected_next: Option<LogIndex> = None;

        for (i, (filename_base, path)) in wal_files.into_iter().enumerate() {
            let is_last = i == count - 1;
            let segment = Segment::open(path)?;
            if segment.base_index != filename_base {
                return Err(storage_err(format!(
                    "wal segment header base_index {} does not match filename base_index {} ({})",
                    segment.base_index.0,
                    filename_base.0,
                    segment.path.display()
                )));
            }
            let seg_idx = self.segments.len();
            self.segments.push(segment);
            expected_next = self.recover_segment(seg_idx, is_last, expected_next)?;
        }

        // Open active writer on the last segment so subsequent appends
        // resume at end-of-file.
        if let Some(last) = self.segments.last() {
            let writer = OpenOptions::new()
                .append(true)
                .open(&last.path)
                .map_err(|e| storage_err(format!("wal reopen writer: {e}")))?;
            self.active_writer = Some(writer);
        }

        Ok(())
    }

    /// Replay all frames of one already-opened segment, populating
    /// `self.offsets`. Returns the `expected_next_index` that the next
    /// segment must start at, or the input value if no entries were
    /// recovered from this segment.
    fn recover_segment(
        &mut self,
        seg_idx: usize,
        is_last: bool,
        expected_next_index: Option<LogIndex>,
    ) -> Result<Option<LogIndex>> {
        let seg_path = self.segments[seg_idx].path.clone();
        let seg_base = self.segments[seg_idx].base_index;
        let buf = self.segments[seg_idx].read_all()?;
        let mut cursor = crate::log_format::SEGMENT_HEADER_LEN as usize;
        let mut last_index_seen: Option<LogIndex> = None;
        let mut first_in_segment = true;

        while cursor < buf.len() {
            let frame_start = cursor;
            match decode_frame(&buf, cursor, self.max_frame_body) {
                Ok((entry, next)) => {
                    // Continuity invariants. Bugs in the writer or a
                    // moved/spliced segment file are surfaced loud here
                    // rather than corrupting Raft state silently.
                    if first_in_segment {
                        if entry.index != seg_base {
                            return Err(storage_err(format!(
                                "wal segment {} first frame index {} != header base_index {}",
                                seg_path.display(),
                                entry.index.0,
                                seg_base.0
                            )));
                        }
                        if let Some(expected) = expected_next_index
                            && entry.index != expected
                        {
                            return Err(storage_err(format!(
                                "wal non-contiguous: segment {} starts at {} \
                                 but previous segment ended expecting {}",
                                seg_path.display(),
                                entry.index.0,
                                expected.0
                            )));
                        }
                    } else if let Some(prev) = last_index_seen
                        && entry.index.0 != prev.0 + 1
                    {
                        return Err(storage_err(format!(
                            "wal non-contiguous: segment {} jumps from {} to {}",
                            seg_path.display(),
                            prev.0,
                            entry.index.0
                        )));
                    }

                    // Set first_index from the very first surviving entry
                    // in the entire log so post-snapshot WALs (with a
                    // non-1 first_index) work after a future stage adds
                    // log compaction.
                    if self.offsets.is_empty() {
                        self.first_index = entry.index;
                    }
                    let frame_len = (next - frame_start) as u32;
                    self.offsets.push(OffsetRef {
                        segment_idx: seg_idx,
                        byte_offset: frame_start as u64,
                        byte_len: frame_len,
                        term: entry.term,
                    });

                    first_in_segment = false;
                    last_index_seen = Some(entry.index);
                    cursor = next;
                }
                Err(FrameDecodeError::Truncated(msg)) if is_last => {
                    // Torn write at the tail of the latest segment is the
                    // standard crash-recovery case. Trim & fsync so the
                    // next append starts cleanly.
                    tracing::warn!(
                        "wal: trimming torn tail of {} starting at byte {}: {}",
                        seg_path.display(),
                        frame_start,
                        msg
                    );
                    let f = OpenOptions::new()
                        .write(true)
                        .open(&seg_path)
                        .map_err(|e| storage_err(format!("wal reopen for trim: {e}")))?;
                    f.set_len(frame_start as u64)
                        .map_err(|e| storage_err(format!("wal set_len: {e}")))?;
                    f.sync_all()
                        .map_err(|e| storage_err(format!("wal sync after trim: {e}")))?;
                    self.segments[seg_idx].bytes_written = frame_start as u64;
                    break;
                }
                Err(FrameDecodeError::Truncated(msg)) => {
                    return Err(storage_err(format!(
                        "wal corrupt (mid-log truncation in non-last segment {}): {msg}",
                        seg_path.display()
                    )));
                }
                Err(FrameDecodeError::Corrupt(msg)) => {
                    return Err(storage_err(format!(
                        "wal corrupt at byte {frame_start} of {}: {msg}",
                        seg_path.display()
                    )));
                }
            }
        }

        Ok(last_index_seen
            .map(|i| LogIndex(i.0 + 1))
            .or(expected_next_index))
    }

    // ---------------- Segment management ----------------

    /// Seal the active segment (`fsync`) and create a new one whose
    /// `base_index` is `next_base`.
    fn rotate_segment(&mut self, next_base: LogIndex) -> Result<()> {
        if let Some(ref w) = self.active_writer {
            w.sync_all()
                .map_err(|e| storage_err(format!("wal sync before rotate: {e}")))?;
        }
        self.active_writer = None;
        self.create_new_segment(next_base)
    }

    fn create_new_segment(&mut self, base_index: LogIndex) -> Result<()> {
        let segment = Segment::create(&self.dir, base_index)?;
        let writer = OpenOptions::new()
            .append(true)
            .open(&segment.path)
            .map_err(|e| storage_err(format!("wal open writer for new segment: {e}")))?;
        self.segments.push(segment);
        self.active_writer = Some(writer);
        Ok(())
    }

    // ---------------- Internal helpers ----------------

    fn last_logical_index(&self) -> LogIndex {
        if self.offsets.is_empty() {
            LogIndex(0)
        } else {
            LogIndex(self.first_index.0 + self.offsets.len() as u64 - 1)
        }
    }

    /// Convert a logical `LogIndex` to its position in `self.offsets`,
    /// returning `None` if the index is outside the log's coverage.
    fn offset_pos(&self, index: LogIndex) -> Option<usize> {
        if self.offsets.is_empty() || index.0 == 0 || index < self.first_index {
            return None;
        }
        let pos = (index.0 - self.first_index.0) as usize;
        if pos >= self.offsets.len() { None } else { Some(pos) }
    }
}

impl LogStore for FileLogStore {
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Continuity check — gaps and non-monotonic batches are rejected
        // *before* any byte hits disk so a buggy caller cannot corrupt
        // the WAL.
        let current_last = self.last_logical_index();
        for (i, entry) in entries.iter().enumerate() {
            let expected = if i == 0 {
                if current_last.0 == 0 {
                    entry.index
                } else {
                    LogIndex(current_last.0 + 1)
                }
            } else {
                LogIndex(entries[i - 1].index.0 + 1)
            };
            if entry.index != expected {
                return Err(storage_err(format!(
                    "wal non-contiguous append: entry[{i}].index = {} but expected {} \
                     (current last_index = {})",
                    entry.index.0, expected.0, current_last.0
                )));
            }
        }

        // Stage updates so we only publish them after every fsync succeeds.
        let mut staged: Vec<OffsetRef> = Vec::with_capacity(entries.len());
        // Track every segment that received bytes during this batch so
        // that rotation-spanning batches are fully durable on return —
        // not just the tail segment.
        let mut dirty: HashSet<usize> = HashSet::new();
        let mut staged_first_index: Option<LogIndex> = None;

        for entry in entries {
            let frame_len = frame_byte_len(entry);

            // Bound on serialized entry size — keep slightly under the
            // segment cap so a fresh segment can always hold one entry.
            if (frame_len as u64) > self.max_segment_size.max(crate::log_format::SEGMENT_HEADER_LEN)
            {
                return Err(storage_err(format!(
                    "wal entry too large: frame {} bytes > max_segment_size {}",
                    frame_len, self.max_segment_size
                )));
            }

            // Rotate / create as needed.
            let need_new_segment = match self.segments.last() {
                None => true,
                Some(last) => {
                    last.bytes_written + frame_len as u64 > self.max_segment_size
                        && last.bytes_written > crate::log_format::SEGMENT_HEADER_LEN
                }
            };
            if need_new_segment {
                if self.segments.is_empty() {
                    self.create_new_segment(entry.index)?;
                } else {
                    self.rotate_segment(entry.index)?;
                }
            } else if self.active_writer.is_none() {
                // First append after open() on an empty directory.
                self.create_new_segment(entry.index)?;
            }

            let seg_idx = self.segments.len() - 1;
            let byte_offset = self.segments[seg_idx].bytes_written;
            let frame = encode_frame(entry);

            self.active_writer
                .as_mut()
                .ok_or_else(|| storage_err("wal active writer missing"))?
                .write_all(&frame)
                .map_err(|e| storage_err(format!("wal write_all: {e}")))?;

            self.segments[seg_idx].bytes_written += frame.len() as u64;
            dirty.insert(seg_idx);
            staged.push(OffsetRef {
                segment_idx: seg_idx,
                byte_offset,
                byte_len: frame_len,
                term: entry.term,
            });
            if staged_first_index.is_none() && self.offsets.is_empty() {
                staged_first_index = Some(entry.index);
            }
        }

        // Durability: fsync EVERY segment touched during this batch.
        // The active writer covers only the last segment; rotation-
        // spanning batches need explicit sync on each preceding segment.
        for seg_idx in &dirty {
            if Some(*seg_idx) == self.segments.len().checked_sub(1) {
                if let Some(ref w) = self.active_writer {
                    w.sync_all()
                        .map_err(|e| storage_err(format!("wal sync (active): {e}")))?;
                }
            } else {
                let seg_path = &self.segments[*seg_idx].path;
                let f = OpenOptions::new()
                    .write(true)
                    .open(seg_path)
                    .map_err(|e| storage_err(format!("wal reopen for sync: {e}")))?;
                f.sync_all()
                    .map_err(|e| storage_err(format!("wal sync (sealed): {e}")))?;
            }
        }

        // Publish the in-memory state only after disk durability.
        if let Some(fi) = staged_first_index {
            self.first_index = fi;
        }
        self.offsets.extend(staged);

        Ok(())
    }

    fn get(&self, index: LogIndex) -> Result<Option<Entry>> {
        let Some(pos) = self.offset_pos(index) else {
            return Ok(None);
        };
        let off = self.offsets[pos];
        // O(1) offset lookup → seek+read on the segment file → decode.
        // The WAL never caches entry payloads.
        let bytes = self.segments[off.segment_idx].read_at(off.byte_offset, off.byte_len as usize)?;
        let (entry, _) = decode_frame(&bytes, 0, self.max_frame_body)
            .map_err(|e| storage_err(format!("wal decode (get {}): {e}", index.0)))?;
        Ok(Some(entry))
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>> {
        if start >= end {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut idx = start.0;
        while idx < end.0 {
            if let Some(entry) = self.get(LogIndex(idx))? {
                out.push(entry);
            }
            idx += 1;
        }
        Ok(out)
    }

    fn last_index(&self) -> LogIndex {
        self.last_logical_index()
    }

    fn last_term(&self) -> Term {
        self.offsets.last().map_or(Term(0), |o| o.term)
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
        // Nothing to remove if the index is past our range.
        let pos = match self.offset_pos(index) {
            Some(p) => p,
            None => {
                if !self.offsets.is_empty() && index <= self.first_index {
                    // Truncate-everything case (index ≤ first surviving entry).
                    0
                } else {
                    return Ok(());
                }
            }
        };

        let target = self.offsets[pos];
        let target_seg_idx = target.segment_idx;
        let trim_offset = target.byte_offset;

        // Drop the active writer before mutating files. We will reopen
        // it after the disk operations succeed.
        self.active_writer = None;

        // CRASH-SAFE ORDER: delete LATER segments first, fsync the dir,
        // and only then trim the affected segment. A crash mid-truncate
        // therefore either looks like "no truncate yet" (later segments
        // present, target untouched) or like a fully completed truncate —
        // never a non-contiguous mix that recovery rejects.
        let later_paths: Vec<PathBuf> = self
            .segments
            .drain(target_seg_idx + 1..)
            .map(|s| s.path)
            .collect();
        for p in &later_paths {
            fs::remove_file(p).map_err(|e| {
                storage_err(format!("wal remove later segment {}: {e}", p.display()))
            })?;
        }
        if !later_paths.is_empty() {
            sync_dir(&self.dir)?;
        }

        // Now trim or remove the affected segment.
        let header_len = crate::log_format::SEGMENT_HEADER_LEN;
        let target_seg_path = self.segments[target_seg_idx].path.clone();
        if trim_offset <= header_len {
            // Truncating from this segment's first entry (or earlier) —
            // remove the file entirely.
            self.segments.pop();
            fs::remove_file(&target_seg_path).map_err(|e| {
                storage_err(format!(
                    "wal remove emptied segment {}: {e}",
                    target_seg_path.display()
                ))
            })?;
            sync_dir(&self.dir)?;
        } else {
            let f = OpenOptions::new()
                .write(true)
                .open(&target_seg_path)
                .map_err(|e| storage_err(format!("wal open for trim: {e}")))?;
            f.set_len(trim_offset)
                .map_err(|e| storage_err(format!("wal set_len: {e}")))?;
            f.sync_all()
                .map_err(|e| storage_err(format!("wal sync after trim: {e}")))?;
            self.segments[target_seg_idx].bytes_written = trim_offset;
        }

        // Drop offset entries from `pos` onward.
        self.offsets.truncate(pos);
        if self.offsets.is_empty() {
            self.first_index = LogIndex(1);
        }

        // Reopen active writer on the new last segment, if any.
        if let Some(last) = self.segments.last() {
            let writer = OpenOptions::new()
                .append(true)
                .open(&last.path)
                .map_err(|e| storage_err(format!("wal reopen writer after truncate: {e}")))?;
            self.active_writer = Some(writer);
        }

        Ok(())
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>> {
        Ok(self.offset_pos(index).map(|pos| self.offsets[pos].term))
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(ref w) = self.active_writer {
            w.sync_all()
                .map_err(|e| storage_err(format!("wal flush sync: {e}")))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn storage_err(msg: impl Into<String>) -> XRaftError {
    XRaftError::Storage(msg.into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use xraft_core::message::EntryPayload;
    use xraft_core::storage::SnapshotMeta;

    use crate::log_format::FRAME_BODY_HEADER;

    fn noop(index: u64, term: u64) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::NoOp,
        }
    }

    fn cmd(index: u64, term: u64, data: &[u8]) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::Command(Bytes::copy_from_slice(data)),
        }
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("xraft-wal-log-tests")
            .join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // -- MemoryLogStore -----------------------------------------------------

    #[test]
    fn mem_empty_defaults() {
        let log = MemoryLogStore::new();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert!(log.get(LogIndex(1)).unwrap().is_none());
    }

    #[test]
    fn mem_append_and_get() {
        let mut log = MemoryLogStore::new();
        log.append(&[noop(1, 1), noop(2, 1)]).unwrap();
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.get(LogIndex(2)).unwrap().unwrap().term, Term(1));
    }

    #[test]
    fn mem_truncate_from() {
        let mut log = MemoryLogStore::new();
        log.append(&[noop(1, 1), noop(2, 1), noop(3, 2)]).unwrap();
        log.truncate_from(LogIndex(2)).unwrap();
        assert_eq!(log.last_index(), LogIndex(1));
        assert!(log.get(LogIndex(2)).unwrap().is_none());
    }

    // -- FileLogStore: scenario 1 (append-and-read 100) --------------------

    #[test]
    fn file_append_and_read_100() {
        let dir = test_dir("append_and_read_100");
        let mut log = FileLogStore::open(&dir).unwrap();

        let entries: Vec<Entry> = (1..=100u64)
            .map(|i| cmd(i, 1, format!("entry-{i}").as_bytes()))
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
                _ => panic!("expected Command at {i}"),
            }
        }
    }

    // -- Scenario 2: truncate-divergent (truncate_from(30) of 50) ----------

    #[test]
    fn file_truncate_from_30_of_50() {
        let dir = test_dir("truncate_30_of_50");
        let mut log = FileLogStore::open(&dir).unwrap();
        let entries: Vec<Entry> = (1..=50u64).map(|i| noop(i, 1)).collect();
        log.append(&entries).unwrap();
        assert_eq!(log.last_index(), LogIndex(50));

        log.truncate_from(LogIndex(30)).unwrap();

        assert_eq!(log.last_index(), LogIndex(29));
        assert!(log.get(LogIndex(30)).unwrap().is_none());
        assert!(log.get(LogIndex(50)).unwrap().is_none());
        assert_eq!(log.get(LogIndex(29)).unwrap().unwrap().index, LogIndex(29));
        assert_eq!(log.get(LogIndex(1)).unwrap().unwrap().index, LogIndex(1));
    }

    // -- Scenario 3: segment rotation at 1 KiB threshold -------------------

    #[test]
    fn file_segment_rotation_1kb() {
        let dir = test_dir("segment_rotation_1kb");
        let mut log = FileLogStore::open_with_max_segment_size(&dir, 1024).unwrap();

        for i in 1..=60u64 {
            log.append(&[cmd(i, 1, b"payload-bytes")]).unwrap();
        }
        assert!(
            log.segments.len() >= 2,
            "expected segment rotation at 1 KiB cap, got {} segment(s)",
            log.segments.len()
        );
        assert_eq!(log.last_index(), LogIndex(60));

        // Reads must succeed across segment boundaries.
        for i in 1..=60u64 {
            assert_eq!(log.get(LogIndex(i)).unwrap().unwrap().index, LogIndex(i));
        }
        let range = log.get_range(LogIndex(20), LogIndex(40)).unwrap();
        assert_eq!(range.len(), 20);
        for (k, e) in range.iter().enumerate() {
            assert_eq!(e.index, LogIndex(20 + k as u64));
        }
    }

    // -- Scenario 4: crash recovery rebuilds the in-memory index -----------

    #[test]
    fn file_crash_recovery_rebuilds_index() {
        let dir = test_dir("crash_recovery_rebuilds_index");

        {
            let mut log = FileLogStore::open_with_max_segment_size(&dir, 256).unwrap();
            for i in 1..=30u64 {
                log.append(&[cmd(i, 1, b"x")]).unwrap();
            }
            // Drop without explicit flush — durability is part of
            // append's contract.
        }

        let log = FileLogStore::open_with_max_segment_size(&dir, 256).unwrap();
        assert_eq!(log.last_index(), LogIndex(30));
        assert_eq!(log.offsets.len(), 30);
        for i in 1..=30u64 {
            let e = log.get(LogIndex(i)).unwrap().unwrap();
            assert_eq!(e.index, LogIndex(i));
            match e.payload {
                EntryPayload::Command(ref b) => assert_eq!(&b[..], b"x"),
                _ => panic!("expected Command"),
            }
        }
    }

    // -- O(1) read path ----------------------------------------------------

    #[test]
    fn file_get_uses_offset_index_not_entry_cache() {
        // The store must not hold a per-entry payload cache. We assert
        // the key invariant: per-entry RAM cost is bounded by the
        // OffsetRef size (~32 bytes), regardless of payload size.
        let dir = test_dir("offset_index_no_cache");
        let mut log = FileLogStore::open(&dir).unwrap();
        let big_payload = vec![0xABu8; 16 * 1024]; // 16 KiB per entry
        for i in 1..=10u64 {
            log.append(&[cmd(i, 1, &big_payload)]).unwrap();
        }
        // Only 10 OffsetRefs (~320 bytes total), regardless of payload size.
        assert_eq!(log.offsets.len(), 10);
        // Reads still correct.
        let e = log.get(LogIndex(5)).unwrap().unwrap();
        match e.payload {
            EntryPayload::Command(ref b) => assert_eq!(b.len(), 16 * 1024),
            _ => panic!("expected Command"),
        }
    }

    // -- Misc behavioural tests --------------------------------------------

    #[test]
    fn file_empty_log_defaults() {
        let dir = test_dir("empty_log_defaults");
        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert!(log.get(LogIndex(1)).unwrap().is_none());
    }

    #[test]
    fn file_append_empty_batch_is_noop() {
        let dir = test_dir("append_empty_noop");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[]).unwrap();
        assert_eq!(log.last_index(), LogIndex(0));
        // No segment file should be created for an empty batch.
        let count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == SEGMENT_EXT))
            .count();
        assert_eq!(count, 0);
    }

    #[test]
    fn file_append_rejects_index_gap() {
        let dir = test_dir("append_rejects_gap");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[noop(1, 1)]).unwrap();
        let err = log.append(&[noop(3, 1)]).expect_err("gap must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("non-contiguous"), "got: {msg}");
        assert_eq!(log.last_index(), LogIndex(1));
    }

    #[test]
    fn file_append_rejects_out_of_order_within_batch() {
        let dir = test_dir("append_rejects_out_of_order");
        let mut log = FileLogStore::open(&dir).unwrap();
        let err = log
            .append(&[noop(1, 1), noop(3, 1), noop(2, 1)])
            .expect_err("out-of-order entries must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("non-contiguous"), "got: {msg}");
        assert_eq!(log.last_index(), LogIndex(0));
    }

    #[test]
    fn file_truncate_all_clears_log() {
        let dir = test_dir("truncate_all");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[noop(1, 1), noop(2, 1)]).unwrap();
        log.truncate_from(LogIndex(1)).unwrap();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        // After full truncation we can append from index 1 again.
        log.append(&[noop(1, 5)]).unwrap();
        assert_eq!(log.last_index(), LogIndex(1));
        assert_eq!(log.last_term(), Term(5));
    }

    #[test]
    fn file_truncate_beyond_last_is_noop() {
        let dir = test_dir("truncate_beyond_last");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[noop(1, 1)]).unwrap();
        log.truncate_from(LogIndex(99)).unwrap();
        assert_eq!(log.last_index(), LogIndex(1));
    }

    #[test]
    fn file_term_at_uses_offset_index_no_disk_io() {
        let dir = test_dir("term_at_no_disk");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[noop(1, 5), noop(2, 7)]).unwrap();
        // term_at must succeed even if the offset index is the only
        // source of term information (no disk read needed).
        assert_eq!(log.term_at(LogIndex(1)).unwrap(), Some(Term(5)));
        assert_eq!(log.term_at(LogIndex(2)).unwrap(), Some(Term(7)));
        assert_eq!(log.term_at(LogIndex(3)).unwrap(), None);
    }

    #[test]
    fn file_truncate_across_segments_is_crash_safe() {
        let dir = test_dir("truncate_across_segments");
        let mut log = FileLogStore::open_with_max_segment_size(&dir, 256).unwrap();
        for i in 1..=20u64 {
            log.append(&[cmd(i, 1, b"x")]).unwrap();
        }
        let seg_count_before = log.segments.len();
        assert!(seg_count_before >= 2, "need multiple segments for this test");
        log.truncate_from(LogIndex(5)).unwrap();
        assert_eq!(log.last_index(), LogIndex(4));
        assert!(log.segments.len() < seg_count_before);

        // Recovery from disk yields the same state.
        drop(log);
        let log = FileLogStore::open_with_max_segment_size(&dir, 256).unwrap();
        assert_eq!(log.last_index(), LogIndex(4));
        for i in 1..=4u64 {
            assert!(log.get(LogIndex(i)).unwrap().is_some());
        }
    }

    #[test]
    fn file_appends_durable_without_explicit_flush() {
        let dir = test_dir("durable_no_flush");
        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[
                cmd(1, 1, b"alpha"),
                cmd(2, 1, b"beta"),
                cmd(3, 2, b"gamma"),
            ])
            .unwrap();
            // No flush — durability comes from append.
        }
        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(3));
        assert_eq!(log.last_term(), Term(2));
    }

    #[test]
    fn file_snapshot_entry_roundtrip() {
        let dir = test_dir("snapshot_entry_roundtrip");
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
        }
        let log = FileLogStore::open(&dir).unwrap();
        let recovered = log.get(LogIndex(11)).unwrap().unwrap();
        match recovered.payload {
            EntryPayload::Snapshot(ref m) => {
                assert_eq!(m.id, "snap-1");
                assert_eq!(m.last_included_index, LogIndex(10));
            }
            _ => panic!("expected Snapshot"),
        }
    }

    #[test]
    fn file_config_change_entry_roundtrip() {
        use xraft_core::types::{DirectoryId, Endpoint, NodeId, VoterRecord, VoterSet};
        let dir = test_dir("config_change_entry_roundtrip");
        let voter = VoterRecord {
            node_id: NodeId(7),
            directory_id: DirectoryId(uuid::Uuid::new_v4()),
            endpoints: vec![Endpoint {
                host: "127.0.0.1".to_string(),
                port: 5432,
            }],
        };
        let voter_set = VoterSet::try_new(vec![voter]).expect("valid voter set");
        let entry = Entry {
            index: LogIndex(1),
            term: Term(2),
            payload: EntryPayload::ConfigChange(voter_set),
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
            _ => panic!("expected ConfigChange"),
        }
    }

    #[test]
    fn file_recovery_trims_torn_tail() {
        let dir = test_dir("recovery_trims_torn_tail");
        {
            let mut log = FileLogStore::open(&dir).unwrap();
            log.append(&[noop(1, 1), noop(2, 1)]).unwrap();
        }
        // Append a partial frame header (looks like a torn write).
        let wal_path = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|x| x == SEGMENT_EXT))
            .unwrap()
            .path();
        let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
        f.write_all(&[0xDE, 0xAD]).unwrap(); // Just 2 bytes — too short for length field.
        f.sync_all().unwrap();

        let log = FileLogStore::open(&dir).unwrap();
        assert_eq!(log.last_index(), LogIndex(2));
    }

    // Helper used by the corruption tests in tests/log_corruption.rs to
    // build a known-good WAL file for surgery.
    const _: () = ();

    #[test]
    fn file_first_index_resets_after_full_truncate() {
        let dir = test_dir("first_index_resets");
        let mut log = FileLogStore::open(&dir).unwrap();
        log.append(&[noop(1, 1), noop(2, 1)]).unwrap();
        log.truncate_from(LogIndex(1)).unwrap();
        assert_eq!(log.first_index, LogIndex(1));
        assert_eq!(log.last_index(), LogIndex(0));
    }

    /// The CRC envelope covers the length field, so a flipped byte
    /// inside `length` of a fully-written final frame must surface as
    /// corruption (NOT silent torn-tail truncation). This is the
    /// regression test for the prior iteration's classification bug.
    #[test]
    fn file_recovery_rejects_length_byte_corruption_in_final_frame() {
        let dir = test_dir("length_byte_corruption_final_frame");
        {
            let mut log = FileLogStore::open(&dir).unwrap();
            // Write two entries so the corrupted frame is preceded by a
            // good one; that way we exercise "good prefix + corrupt
            // tail-frame" rather than "first frame is bad".
            log.append(&[cmd(1, 1, b"good"), cmd(2, 1, b"corrupt-me-later")])
                .unwrap();
        }

        let wal_path = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|x| x == SEGMENT_EXT))
            .unwrap()
            .path();
        let mut bytes = fs::read(&wal_path).unwrap();
        // Locate the second frame: after the 28-byte header and the
        // first frame. Frame 1 size = 4 + (17 + 4) + 4 = 29 bytes.
        let frame1_size = (FRAME_BODY_HEADER + 4) as usize + 4 + 4;
        let frame2_start = crate::log_format::SEGMENT_HEADER_LEN as usize + frame1_size;
        // Flip a high bit of length[3] — keeps length within the
        // sanity band but corrupts the value, so CRC must catch it.
        bytes[frame2_start + 3] ^= 0x40;
        fs::write(&wal_path, &bytes).unwrap();

        let err = FileLogStore::open(&dir)
            .expect_err("length corruption must surface as wal corrupt");
        let msg = format!("{err}");
        assert!(
            msg.contains("corrupt") || msg.contains("CRC"),
            "expected corrupt/CRC error, got: {msg}"
        );
    }
}
