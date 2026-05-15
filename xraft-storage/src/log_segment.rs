// -----------------------------------------------------------------------
// <copyright file="log_segment.rs" company="Microsoft Corp.">
//     Copyright (c) Microsoft Corp. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

//! On-disk WAL segment file: header validation, scan-based recovery, and a
//! locked read handle for serving `get` calls without disturbing the active
//! writer's append position.
//!
//! Filenames follow `{base_index:020}.wal`. New segments are written via a
//! `.wal.tmp` companion file and atomically renamed once the header is
//! durable so a crash mid-creation cannot leave a header-less or
//! partial-header `.wal` file that bricks recovery.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use xraft_core::error::{Result, XRaftError};
use xraft_core::types::LogIndex;

use crate::log_format::{
    SEGMENT_HEADER_LEN, SegmentHeader, decode_segment_header, encode_segment_header,
};

pub(crate) const SEGMENT_EXT: &str = "wal";
pub(crate) const SEGMENT_TMP_EXT: &str = "wal.tmp";

/// Build the on-disk filename for a segment whose first entry is at
/// `base_index`. Width-20 zero padding keeps lexicographic sort order
/// equal to numeric sort order.
pub(crate) fn segment_filename(base_index: LogIndex) -> String {
    format!("{:020}.{SEGMENT_EXT}", base_index.0)
}

/// Parse the `base_index` out of a `{N:020}.wal` filename. Returns `None`
/// if the file does not match the WAL naming convention so the caller can
/// classify foreign files differently from corrupt ones.
pub(crate) fn parse_segment_filename(path: &Path) -> Option<LogIndex> {
    let stem = path.file_stem()?.to_str()?;
    stem.parse::<u64>().ok().map(LogIndex)
}

fn storage_err(msg: impl Into<String>) -> XRaftError {
    XRaftError::Storage(msg.into())
}

fn io_to_storage(e: io::Error) -> XRaftError {
    storage_err(format!("wal io: {e}"))
}

/// `fsync` the directory entry so that segment create/delete operations
/// survive a crash. Unix-only (POSIX requirement); Windows is a documented
/// no-op since `forbid(unsafe_code)` precludes the raw `CreateFileW` +
/// `FlushFileBuffers` dance and NTFS journals filename metadata
/// independently.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let f = File::open(dir).map_err(io_to_storage)?;
        f.sync_all().map_err(io_to_storage)?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Segment
// ---------------------------------------------------------------------------

/// One open WAL segment: its on-disk path, the `base_index` confirmed
/// against the header, the durable byte length, and a locked read handle.
///
/// The read handle is separate from the writer so that `LogStore::get`
/// (`&self`) can serve reads concurrently with appends without competing
/// on file-position state.
#[derive(Debug)]
pub(crate) struct Segment {
    pub path: PathBuf,
    pub base_index: LogIndex,
    /// Number of bytes durably persisted in this segment, including the
    /// 28-byte header.
    pub bytes_written: u64,
    reader: Mutex<File>,
}

impl Segment {
    /// Create a new segment file at `dir/{base_index:020}.wal`.
    ///
    /// The header is written to a `.wal.tmp` companion file, fsynced, and
    /// then atomically renamed to the final filename. This means a crash
    /// during creation can only leave (a) no file at all, or (b) a leftover
    /// `.wal.tmp` that recovery cleans up — never a partial-header `.wal`
    /// file that would brick the next startup.
    pub fn create(dir: &Path, base_index: LogIndex) -> Result<Self> {
        let final_path = dir.join(segment_filename(base_index));
        let tmp_path = final_path.with_extension(SEGMENT_TMP_EXT);

        let header = SegmentHeader {
            base_index,
            created_at_unix_ms: now_unix_ms(),
        };
        let header_bytes = encode_segment_header(&header);

        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(io_to_storage)?;
            io::Write::write_all(&mut f, &header_bytes).map_err(io_to_storage)?;
            f.sync_all().map_err(io_to_storage)?;
        }
        // Atomic rename: after this point a crash leaves a complete
        // header-only segment, never a torn header.
        fs::rename(&tmp_path, &final_path).map_err(io_to_storage)?;
        sync_dir(dir)?;

        Self::reopen(final_path, base_index, SEGMENT_HEADER_LEN)
    }

    /// Open an existing segment file. Validates the header and returns a
    /// `Segment` whose `bytes_written` reflects the on-disk size.
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut f = File::open(&path).map_err(io_to_storage)?;
        let mut header_bytes = [0u8; SEGMENT_HEADER_LEN as usize];
        f.read_exact(&mut header_bytes)
            .map_err(|e| storage_err(format!("wal short header for {}: {e}", path.display())))?;
        let header = decode_segment_header(&header_bytes).map_err(|e| match e {
            crate::log_format::HeaderDecodeError::ForeignFile => {
                storage_err(format!("wal foreign file: {}", path.display()))
            }
            crate::log_format::HeaderDecodeError::UnsupportedVersion(v) => storage_err(format!(
                "wal unsupported version {v} in {}",
                path.display()
            )),
            crate::log_format::HeaderDecodeError::Corrupt(msg) => {
                storage_err(format!("wal corrupt header in {}: {msg}", path.display()))
            }
            crate::log_format::HeaderDecodeError::ShortHeader => {
                storage_err(format!("wal short header for {}", path.display()))
            }
        })?;

        let bytes_written = f
            .metadata()
            .map_err(io_to_storage)?
            .len();
        Self::reopen(path, header.base_index, bytes_written)
    }

    fn reopen(path: PathBuf, base_index: LogIndex, bytes_written: u64) -> Result<Self> {
        let reader = File::open(&path).map_err(io_to_storage)?;
        Ok(Self {
            path,
            base_index,
            bytes_written,
            reader: Mutex::new(reader),
        })
    }

    /// Read the full segment file into memory. Used by recovery to scan
    /// frames sequentially. The 28-byte header is included at offset 0.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        fs::read(&self.path).map_err(io_to_storage)
    }

    /// Read `len` bytes at `byte_offset` from this segment. Used by `get`
    /// to materialise an entry on demand without ever caching its bytes.
    pub fn read_at(&self, byte_offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut f = self
            .reader
            .lock()
            .map_err(|_| storage_err("wal segment reader mutex poisoned"))?;
        f.seek(SeekFrom::Start(byte_offset)).map_err(io_to_storage)?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf).map_err(io_to_storage)?;
        Ok(buf)
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("xraft-wal-segment-tests")
            .join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn create_then_open_roundtrip() {
        let dir = tmp_dir("create_then_open_roundtrip");
        let seg = Segment::create(&dir, LogIndex(1)).unwrap();
        assert_eq!(seg.base_index, LogIndex(1));
        assert_eq!(seg.bytes_written, SEGMENT_HEADER_LEN);
        drop(seg);

        let path = dir.join(segment_filename(LogIndex(1)));
        let seg = Segment::open(path).unwrap();
        assert_eq!(seg.base_index, LogIndex(1));
    }

    #[test]
    fn segment_filename_is_zero_padded_20() {
        assert_eq!(segment_filename(LogIndex(1)), "00000000000000000001.wal");
        assert_eq!(
            segment_filename(LogIndex(123_456)),
            "00000000000000123456.wal"
        );
    }

    #[test]
    fn parse_segment_filename_round_trip() {
        let n = segment_filename(LogIndex(42));
        let path = Path::new(&n);
        assert_eq!(parse_segment_filename(path), Some(LogIndex(42)));
    }

    #[test]
    fn parse_non_numeric_filename_returns_none() {
        let path = Path::new("foreign-file.wal");
        assert!(parse_segment_filename(path).is_none());
    }

    #[test]
    fn open_rejects_foreign_file() {
        let dir = tmp_dir("open_rejects_foreign_file");
        let path = dir.join(segment_filename(LogIndex(1)));
        fs::write(&path, b"NOT_AN_XRAFT_WAL_FILE_PADDING_PADDING").unwrap();
        let err = Segment::open(path).expect_err("foreign magic must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("foreign file"), "got: {msg}");
    }
}
