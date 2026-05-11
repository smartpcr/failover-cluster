//! Snapshot store implementations for the Raft [`SnapshotStore`] trait.
//!
//! Two implementations are provided:
//!
//! * [`MemorySnapshotStore`] - volatile, in-memory store for testing.
//! * [`FileSnapshotStore`] - durable, file-backed store with binary header,
//!   configurable retention, and chunked reading for streamed transfer.
//!
//! # Binary snapshot file format
//!
//! ```text
//! +-------+---------+-------------------+---------------+-----------+----------+-----------+---------+---------+
//! | magic | version | last_included_idx | last_inc_term | vs_len    | vs_bytes | data_len  | payload | crc32   |
//! | u32LE | u16 LE  |      u64 LE       |    u64 LE     | u32 LE    | [u8; N]  |  u64 LE   | [u8; M] | u32 LE  |
//! +-------+---------+-------------------+---------------+-----------+----------+-----------+---------+---------+
//! ```
//!
//! The CRC32 checksum covers the payload data only. It is verified on
//! decode and during chunked streaming to detect same-length corruption.
//!
//! The snapshot `id` is **not** stored in the binary header. It is derived
//! from the canonical filename `snapshot-{term:010}-{index:020}` (zero-padded
//! 10-digit term and 20-digit index).
//!
//! Files are named `snapshot-{term:010}-{index:020}.bin` inside the
//! `<data_dir>/snapshots/` subdirectory, which is created automatically by
//! [`FileSnapshotStore::open`]. For example, term 2 / index 10 becomes
//! `snapshot-0000000002-00000000000000000010.bin`.

use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use xraft_core::error::{Result, XRaftError};
use xraft_core::storage::{SnapshotChunkItem, SnapshotMeta, SnapshotStore};
use xraft_core::types::{LogIndex, Term};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic number identifying a valid snapshot file (`XSNP` in ASCII).
const SNAPSHOT_MAGIC: u32 = 0x504E_5358; // "XSNP" little-endian
/// Current snapshot format version.
const SNAPSHOT_VERSION: u16 = 1;
/// Fixed header size: magic(4) + version(2) + index(8) + term(8) + vs_len(4) = 26.
const FIXED_HEADER_SIZE: usize = 4 + 2 + 8 + 8 + 4;
/// Default chunk size for streamed reads (1 MiB).
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;
/// Subdirectory name under the data dir for snapshot files.
const SNAPSHOTS_SUBDIR: &str = "snapshots";

const SNAPSHOT_EXT: &str = "bin";

/// Maximum allowed voter-set encoded size (100 MB).
const MAX_VOTER_SET_LEN: u32 = 100_000_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn storage_err(msg: impl Into<String>) -> XRaftError {
    XRaftError::Storage(msg.into())
}

fn io_to_storage(e: std::io::Error) -> XRaftError {
    storage_err(e.to_string())
}

/// Convert an IO error to a storage error with path context.
fn io_with_path(e: std::io::Error, path: &Path) -> XRaftError {
    storage_err(format!("{}: {}", path.display(), e))
}

/// Build the canonical filename for a snapshot.
///
/// Uses zero-padded term (10 digits) and index (20 digits):
/// `snapshot-0000000002-00000000000000000010.bin`.
fn snapshot_filename(term: Term, index: LogIndex) -> String {
    format!("snapshot-{:010}-{:020}.{SNAPSHOT_EXT}", term.0, index.0)
}

/// Derive the canonical snapshot id from term and index.
///
/// Returns `snapshot-{term:010}-{index:020}` — e.g. `snapshot-0000000002-00000000000000000010`.
fn canonical_id(term: Term, index: LogIndex) -> String {
    format!("snapshot-{:010}-{:020}", term.0, index.0)
}

/// Parse term and index from a snapshot filename like
/// `snapshot-{term}-{index}.bin` where term and index may or may not be
/// zero-padded.
fn parse_snapshot_filename(path: &Path) -> Option<(Term, LogIndex)> {
    let stem = path.file_stem()?.to_str()?;
    let rest = stem.strip_prefix("snapshot-")?;
    let mut parts = rest.splitn(2, '-');
    let term: u64 = parts.next()?.parse().ok()?;
    let index: u64 = parts.next()?.parse().ok()?;
    Some((Term(term), LogIndex(index)))
}

// ---------------------------------------------------------------------------
// Binary header encoding / decoding
// ---------------------------------------------------------------------------

/// Encode snapshot metadata + data into the binary file format.
///
/// The snapshot `id` is NOT persisted; it is derived from the canonical
/// filename `snapshot-{term}-{index}`.
fn encode_snapshot(meta: &SnapshotMeta, data: &[u8]) -> Result<Vec<u8>> {
    let voter_set_bytes = match &meta.voter_set {
        Some(vs) => bincode::serialize(vs).map_err(|e| storage_err(e.to_string()))?,
        None => Vec::new(),
    };
    let voter_set_len =
        u32::try_from(voter_set_bytes.len()).map_err(|_| storage_err("voter_set too large"))?;
    let data_len =
        u64::try_from(data.len()).map_err(|_| storage_err("snapshot data length exceeds u64"))?;

    // +4 for trailing CRC32. Use checked arithmetic to prevent overflow.
    let total = FIXED_HEADER_SIZE
        .checked_add(voter_set_bytes.len())
        .and_then(|v| v.checked_add(8))
        .and_then(|v| v.checked_add(data.len()))
        .and_then(|v| v.checked_add(4))
        .ok_or_else(|| storage_err("overflow computing snapshot encoding size"))?;
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buf.extend_from_slice(&meta.last_included_index.0.to_le_bytes());
    buf.extend_from_slice(&meta.last_included_term.0.to_le_bytes());
    buf.extend_from_slice(&voter_set_len.to_le_bytes());
    buf.extend_from_slice(&voter_set_bytes);
    buf.extend_from_slice(&data_len.to_le_bytes());
    buf.extend_from_slice(data);

    // CRC32 of the payload data.
    let crc = crc32fast::hash(data);
    buf.extend_from_slice(&crc.to_le_bytes());

    Ok(buf)
}

/// Decode a snapshot file's raw bytes into metadata and payload data.
///
/// The `id` field on the returned [`SnapshotMeta`] is set to the canonical
/// id `snapshot-{term}-{index}`, not read from the binary data.
fn decode_snapshot(raw: &[u8]) -> Result<(SnapshotMeta, Vec<u8>)> {
    if raw.len() < FIXED_HEADER_SIZE {
        return Err(storage_err("snapshot file too short for header"));
    }

    let magic = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    if magic != SNAPSHOT_MAGIC {
        return Err(storage_err(format!(
            "bad snapshot magic: expected {SNAPSHOT_MAGIC:#010x}, got {magic:#010x}"
        )));
    }

    let version = u16::from_le_bytes(raw[4..6].try_into().unwrap());
    if version != SNAPSHOT_VERSION {
        return Err(storage_err(format!(
            "unsupported snapshot version: {version}"
        )));
    }

    let last_included_index = LogIndex(u64::from_le_bytes(raw[6..14].try_into().unwrap()));
    let last_included_term = Term(u64::from_le_bytes(raw[14..22].try_into().unwrap()));
    let voter_set_len = u32::from_le_bytes(raw[22..26].try_into().unwrap()) as usize;

    // Guard against absurdly large voter_set_len to prevent OOM on
    // corrupted files. This mirrors the check in read_meta_from_file and
    // decode_header_for_streaming. Must be checked before computing offsets
    // that depend on voter_set_len to avoid misleading "truncated" errors.
    if voter_set_len > MAX_VOTER_SET_LEN as usize {
        return Err(storage_err(format!(
            "voter_set_len too large: {voter_set_len}"
        )));
    }

    let vs_start = FIXED_HEADER_SIZE;
    let vs_end = vs_start
        .checked_add(voter_set_len)
        .ok_or_else(|| storage_err("overflow in voter_set_len"))?;
    let data_len_end = vs_end
        .checked_add(8)
        .ok_or_else(|| storage_err("overflow in data_len offset"))?;
    if raw.len() < data_len_end {
        return Err(storage_err(
            "snapshot file truncated in voter-set or data-len",
        ));
    }

    let voter_set = if voter_set_len > 0 {
        let vs = bincode::deserialize(&raw[vs_start..vs_end])
            .map_err(|e| storage_err(format!("voter_set decode: {e}")))?;
        Some(vs)
    } else {
        None
    };

    let data_len_u64 = u64::from_le_bytes(raw[vs_end..data_len_end].try_into().unwrap());
    let data_len = usize::try_from(data_len_u64)
        .map_err(|_| storage_err("data_len exceeds addressable memory"))?;
    let data_start = data_len_end;
    let data_end = data_start
        .checked_add(data_len)
        .ok_or_else(|| storage_err("overflow in data_len"))?;
    let crc_end = data_end
        .checked_add(4)
        .ok_or_else(|| storage_err("overflow in crc offset"))?;
    if raw.len() < crc_end {
        return Err(storage_err(
            "snapshot file truncated in payload data or crc",
        ));
    }

    if raw.len() > crc_end {
        return Err(storage_err(format!(
            "snapshot has {} trailing bytes after crc (expected exact length {})",
            raw.len() - crc_end,
            crc_end
        )));
    }

    let data = raw[data_start..data_end].to_vec();

    // Verify CRC32 of the payload.
    let stored_crc = u32::from_le_bytes(raw[data_end..crc_end].try_into().unwrap());
    let computed_crc = crc32fast::hash(&data);
    if stored_crc != computed_crc {
        return Err(storage_err(format!(
            "snapshot payload CRC mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
        )));
    }

    let id = canonical_id(last_included_term, last_included_index);

    let meta = SnapshotMeta {
        last_included_index,
        last_included_term,
        id,
        voter_set,
        size_bytes: Some(data_len as u64),
        checksum: Some(computed_crc as u64),
    };

    Ok((meta, data))
}

/// Compute the byte offset where the payload data begins in a snapshot file,
/// without reading the full file. Returns `(meta, header_size, data_len, stored_crc)`.
/// The reader is left positioned at the start of the payload data.
fn decode_header_for_streaming(
    file: &mut BufReader<File>,
    file_len: u64,
) -> Result<(SnapshotMeta, u64, u64, u32)> {
    let mut hdr = [0u8; FIXED_HEADER_SIZE];
    file.read_exact(&mut hdr).map_err(io_to_storage)?;

    let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
    if magic != SNAPSHOT_MAGIC {
        return Err(storage_err("bad snapshot magic"));
    }
    let version = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
    if version != SNAPSHOT_VERSION {
        return Err(storage_err(format!(
            "unsupported snapshot version: {version}"
        )));
    }

    let last_included_index = LogIndex(u64::from_le_bytes(hdr[6..14].try_into().unwrap()));
    let last_included_term = Term(u64::from_le_bytes(hdr[14..22].try_into().unwrap()));

    let voter_set_len = u32::from_le_bytes(hdr[22..26].try_into().unwrap());
    if voter_set_len > MAX_VOTER_SET_LEN {
        return Err(storage_err(format!(
            "voter_set_len too large: {voter_set_len}"
        )));
    }

    let voter_set = if voter_set_len > 0 {
        let mut vs_buf = vec![0u8; voter_set_len as usize];
        file.read_exact(&mut vs_buf).map_err(io_to_storage)?;
        let vs = bincode::deserialize(&vs_buf)
            .map_err(|e| storage_err(format!("voter_set decode: {e}")))?;
        Some(vs)
    } else {
        None
    };

    let mut dl = [0u8; 8];
    file.read_exact(&mut dl).map_err(io_to_storage)?;
    let data_len = u64::from_le_bytes(dl);

    let header_size = (FIXED_HEADER_SIZE as u64)
        .checked_add(voter_set_len as u64)
        .and_then(|v| v.checked_add(8))
        .ok_or_else(|| storage_err("overflow computing header size"))?;

    // Validate file size = header + data + 4 (CRC).
    let expected_len = header_size
        .checked_add(data_len)
        .and_then(|v| v.checked_add(4))
        .ok_or_else(|| storage_err("overflow computing expected file size"))?;
    if file_len != expected_len {
        return Err(storage_err(format!(
            "snapshot file size mismatch: expected {} bytes, got {}",
            expected_len, file_len
        )));
    }

    // Read stored CRC from the end of the file without disturbing the
    // reader position for subsequent payload streaming.
    let current_pos = file.stream_position().map_err(io_to_storage)?;
    let crc_offset = header_size
        .checked_add(data_len)
        .ok_or_else(|| storage_err("overflow computing crc offset"))?;
    file.seek(SeekFrom::Start(crc_offset))
        .map_err(io_to_storage)?;
    let mut crc_buf = [0u8; 4];
    file.read_exact(&mut crc_buf).map_err(io_to_storage)?;
    let stored_crc = u32::from_le_bytes(crc_buf);
    // Seek back to start of payload data.
    file.seek(SeekFrom::Start(current_pos))
        .map_err(io_to_storage)?;

    let id = canonical_id(last_included_term, last_included_index);
    let meta = SnapshotMeta {
        last_included_index,
        last_included_term,
        id,
        voter_set,
        size_bytes: Some(data_len),
        checksum: Some(stored_crc as u64),
    };

    Ok((meta, header_size, data_len, stored_crc))
}

// ---------------------------------------------------------------------------
// MemorySnapshotStore
// ---------------------------------------------------------------------------

/// In-memory snapshot store backed by a `Vec` of `(meta, data)` pairs.
#[derive(Debug, Default)]
pub struct MemorySnapshotStore {
    snapshots: Vec<(SnapshotMeta, Vec<u8>)>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> Result<()> {
        // Enforce voter_set is present on all new saves.
        if metadata.voter_set.is_none() {
            return Err(XRaftError::Storage(
                "save_snapshot: voter_set is required for new snapshots".to_string(),
            ));
        }

        // Reject lower-term saves at the same index to prevent term regression.
        if let Some(existing) = self
            .snapshots
            .iter()
            .find(|(m, _)| m.last_included_index == metadata.last_included_index)
            && existing.0.last_included_term > metadata.last_included_term
        {
            return Err(XRaftError::Storage(format!(
                "refusing to replace snapshot at index {} term {} with lower term {}",
                metadata.last_included_index.0,
                existing.0.last_included_term.0,
                metadata.last_included_term.0,
            )));
        }

        // Normalize id and populate size_bytes/checksum per the contract.
        let metadata = SnapshotMeta {
            id: canonical_id(metadata.last_included_term, metadata.last_included_index),
            size_bytes: Some(data.len() as u64),
            checksum: Some(crc32fast::hash(data) as u64),
            ..metadata
        };
        // Remove any existing snapshot at the same index.
        self.snapshots
            .retain(|(m, _)| m.last_included_index != metadata.last_included_index);
        self.snapshots.push((metadata, data.to_vec()));
        Ok(())
    }

    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>> {
        Ok(self
            .snapshots
            .iter()
            .max_by_key(|(m, _)| m.last_included_index)
            .cloned())
    }

    fn load_snapshot(
        &self,
        index: LogIndex,
        term: Term,
    ) -> Result<Option<(SnapshotMeta, Vec<u8>)>> {
        let target_id = canonical_id(term, index);
        Ok(self
            .snapshots
            .iter()
            .find(|(m, _)| m.id == target_id)
            .cloned())
    }

    fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>> {
        let mut metas: Vec<SnapshotMeta> = self.snapshots.iter().map(|(m, _)| m.clone()).collect();
        metas.sort_by(|a, b| b.last_included_index.cmp(&a.last_included_index));
        Ok(metas)
    }

    fn delete_snapshot(&mut self, id: &str) -> Result<()> {
        let len_before = self.snapshots.len();
        self.snapshots.retain(|(m, _)| m.id != id);
        // If no match by canonical id, try to extract term+index from the
        // caller-supplied id and match by canonical form.
        if self.snapshots.len() == len_before {
            let with_ext = PathBuf::from(format!("{}.bin", id));
            let parsed = parse_snapshot_filename(&with_ext)
                .or_else(|| parse_snapshot_filename(Path::new(id)));
            if let Some((term, index)) = parsed {
                let cid = canonical_id(term, index);
                let len_before2 = self.snapshots.len();
                self.snapshots.retain(|(m, _)| m.id != cid);
                if self.snapshots.len() == len_before2 {
                    return Err(XRaftError::Storage(format!("snapshot not found: {id}")));
                }
            } else {
                return Err(XRaftError::Storage(format!("snapshot not found: {id}")));
            }
        }
        Ok(())
    }

    fn snapshot_exists(&self, index: LogIndex, term: Term) -> bool {
        let target_id = canonical_id(term, index);
        self.snapshots.iter().any(|(m, _)| m.id == target_id)
    }

    fn find_by_id(&self, id: &str) -> Result<Option<SnapshotMeta>> {
        Ok(self
            .snapshots
            .iter()
            .find(|(m, _)| m.id == id)
            .map(|(m, _)| m.clone()))
    }
}

// ---------------------------------------------------------------------------
// FileSnapshotStore
// ---------------------------------------------------------------------------

/// Durable, file-backed snapshot store.
///
/// Each snapshot is stored as an individual file under `<data_dir>/snapshots/`
/// using the binary header format documented at the module level.
/// On [`open`](FileSnapshotStore::open) every `*.bin` snapshot file is
/// scanned and its metadata is loaded into memory for fast listing and
/// latest-snapshot resolution. Snapshot data is read from disk on demand.
///
/// When a new snapshot is saved, any older snapshots beyond
/// `max_retained` are automatically pruned (oldest first by
/// `last_included_index`). Set `max_retained` to `0` to disable pruning.
///
/// **ID contract:** The `id` field on [`SnapshotMeta`] is always the
/// canonical form `snapshot-{term}-{index}`, regardless of what the caller
/// supplies. Both [`MemorySnapshotStore`] and [`FileSnapshotStore`] normalize
/// ids to canonical form for consistency.
pub struct FileSnapshotStore {
    /// The canonical `<data_dir>/snapshots/` directory.
    dir: PathBuf,
    /// Cached metadata sorted by `last_included_index` descending (newest first).
    index: Vec<SnapshotMeta>,
    /// Maximum number of snapshot files to keep.
    /// 0 disables pruning (used in tests); production code should go through
    /// [`from_config`](Self::from_config) which validates `>= 1` via
    /// [`ClusterConfig`](xraft_core::config::ClusterConfig).
    max_retained: usize,
}

impl FileSnapshotStore {
    /// Default retention count: keep the 3 most recent snapshots.
    pub const DEFAULT_MAX_RETAINED: usize = 3;

    /// Open (or create) a snapshot store using settings from a [`ClusterConfig`].
    ///
    /// Uses `config.data_dir` as the base directory and
    /// `config.snapshot_retention_count` as the retention policy.
    pub fn from_config(config: &xraft_core::config::ClusterConfig) -> Result<Self> {
        Self::open_with_retention(&config.data_dir, config.snapshot_retention_count)
    }

    /// Open (or create) a snapshot store under `<data_dir>/snapshots/` with
    /// default retention.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_retention(data_dir, Self::DEFAULT_MAX_RETAINED)
    }

    /// Open (or create) a snapshot store under `<data_dir>/snapshots/` with a
    /// custom retention count.
    pub fn open_with_retention(data_dir: impl AsRef<Path>, max_retained: usize) -> Result<Self> {
        let dir = data_dir.as_ref().join(SNAPSHOTS_SUBDIR);
        fs::create_dir_all(&dir).map_err(|e| io_with_path(e, &dir))?;

        let mut store = Self {
            dir,
            index: Vec::new(),
            max_retained,
        };
        store.recover_incomplete_writes()?;
        store.rebuild_index()?;
        // Enforce retention immediately on open. This handles the case where
        // the directory already contains more snapshots than max_retained
        // (e.g. retention was lowered, or snapshots were manually copied in).
        store.prune()?;
        Ok(store)
    }

    /// Return the directory used for snapshot storage.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Recover from incomplete write operations on startup.
    ///
    /// Handles crash recovery for the backup-then-rename write strategy:
    /// - If `.bin` is missing but `.bin.bak` exists, restore from backup
    /// - If both `.bin` and `.bin.bak` exist, remove stale backup
    /// - Remove orphaned `.bin.tmp` files
    fn recover_incomplete_writes(&self) -> Result<()> {
        let entries: Vec<_> = fs::read_dir(&self.dir)
            .map_err(|e| io_with_path(e, &self.dir))?
            .filter_map(|e| e.ok())
            .collect();

        // Clean up .tmp files (incomplete writes).
        for entry in &entries {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "tmp")
                && let Err(e) = fs::remove_file(&path)
            {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "failed to remove orphaned tmp file during recovery"
                );
            }
        }

        // Handle .bak files (interrupted rename sequences).
        for entry in &entries {
            let path = entry.path();
            let path_str = path.to_string_lossy();
            if path_str.ends_with(".bin.bak") {
                let bin_path_str = path_str.trim_end_matches(".bak");
                let bin_path = PathBuf::from(bin_path_str.to_string());
                if bin_path.exists() {
                    // Both .bin and .bak exist; rename completed. Remove backup.
                    if let Err(e) = fs::remove_file(&path) {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "failed to remove stale backup during recovery"
                        );
                    }
                } else {
                    // .bin missing; crashed after backup but before temp rename.
                    fs::rename(&path, &bin_path).map_err(|e| io_with_path(e, &path))?;
                }
            }
        }

        Ok(())
    }

    /// Scan the directory for `.bin` snapshot files and populate the in-memory
    /// index. Reads each file's header and streams the payload through a CRC32
    /// check to detect both structural corruption and same-length bit-rot.
    ///
    /// Validates that each file's header metadata matches its filename. Files
    /// where the header disagrees with the filename, or whose CRC is invalid,
    /// are treated as corrupt.
    /// When multiple files share the same `last_included_index`, only the one
    /// with the highest term is kept; the stale file is deleted.
    ///
    /// **Safety invariant:** if the snapshot file with the highest
    /// `last_included_index` (by filename) is corrupt (header, CRC, or
    /// filename mismatch), this method returns an error instead of
    /// silently falling back to an older snapshot. Falling back would be
    /// unsafe when the WAL has already been compacted past the older
    /// snapshot's index. Corrupt files at *lower* indices are silently
    /// skipped, since valid newer snapshots supersede them.
    fn rebuild_index(&mut self) -> Result<()> {
        self.index.clear();

        let entries: Vec<_> = fs::read_dir(&self.dir)
            .map_err(|e| io_with_path(e, &self.dir))?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let path = e.path();
                path.extension().is_some_and(|ext| ext == SNAPSHOT_EXT)
                    && parse_snapshot_filename(&path).is_some()
            })
            .collect();

        // Track the highest (index, term) seen by filename and which
        // (index, term) pairs were skipped, so we can enforce the safety
        // invariant afterwards. We compare by (index, term) so that a
        // corrupt higher-term file at the same index as a valid lower-term
        // file is still detected.
        let mut highest_fname: Option<(LogIndex, Term)> = None;
        let mut skipped_keys = std::collections::HashSet::<(LogIndex, Term)>::new();
        // Track paths of corrupt/skipped files so we can clean them up.
        let mut corrupt_paths: Vec<PathBuf> = Vec::new();

        for dir_entry in &entries {
            let path = dir_entry.path();
            let (fname_term, fname_index) = match parse_snapshot_filename(&path) {
                Some(v) => v,
                None => continue,
            };

            let key = (fname_index, fname_term);
            if highest_fname.is_none_or(|h| key > h) {
                highest_fname = Some(key);
            }

            match Self::read_meta_from_file(&path) {
                Ok(meta) => {
                    if meta.last_included_term != fname_term
                        || meta.last_included_index != fname_index
                    {
                        tracing::warn!(
                            path = %path.display(),
                            header_term = meta.last_included_term.0,
                            header_index = meta.last_included_index.0,
                            filename_term = fname_term.0,
                            filename_index = fname_index.0,
                            "snapshot header/filename mismatch, skipping"
                        );
                        skipped_keys.insert(key);
                        corrupt_paths.push(path);
                        continue;
                    }
                    // If the filename is not in canonical form, rename it so
                    // that load/delete/prune can find it at the canonical path.
                    let canonical = self.dir.join(snapshot_filename(fname_term, fname_index));
                    if path != canonical {
                        if canonical.exists() {
                            // A canonical file already exists (e.g. both
                            // `snapshot-2-10.bin` and
                            // `snapshot-0000000002-...-10.bin` are present).
                            // Remove the non-canonical duplicate.
                            fs::remove_file(&path).map_err(|e| {
                                storage_err(format!(
                                    "failed to remove non-canonical duplicate snapshot {}: {}",
                                    path.display(),
                                    e
                                ))
                            })?;
                            // The canonical file will be indexed on its own
                            // iteration.
                            continue;
                        }
                        fs::rename(&path, &canonical).map_err(|e| io_with_path(e, &path))?;
                        tracing::info!(
                            from = %path.display(),
                            to = %canonical.display(),
                            "renamed non-canonical snapshot to canonical form"
                        );
                    }
                    self.index.push(meta);
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "skipping corrupt snapshot"
                    );
                    skipped_keys.insert(key);
                    corrupt_paths.push(path);
                }
            }
        }

        // Sort newest first by (index DESC, term DESC).
        self.index.sort_by(|a, b| {
            b.last_included_index
                .cmp(&a.last_included_index)
                .then_with(|| b.last_included_term.cmp(&a.last_included_term))
        });

        // **Safety invariant:** if the file with the highest (index, term)
        // by filename was skipped due to corruption or header mismatch,
        // return an error. Silently falling back to an older snapshot
        // would be unsafe when the WAL has been compacted past it.
        // We compare by (index, term) so that a corrupt higher-term
        // snapshot at the same index is also caught.
        if let Some(highest) = highest_fname
            && skipped_keys.contains(&highest)
        {
            // Check whether a valid file at the same or higher
            // (index, term) exists.
            let have_valid = self
                .index
                .first()
                .is_some_and(|m| (m.last_included_index, m.last_included_term) >= highest);
            if !have_valid {
                return Err(storage_err(format!(
                    "latest snapshot (term {}, index {}) is corrupt or has \
                     header/filename mismatch; refusing to fall back to \
                     older snapshot",
                    highest.1.0, highest.0.0
                )));
            }
        }

        // Deduplicate same-index entries: keep highest term, delete stale files.
        let mut seen_indices = std::collections::HashSet::new();
        let mut keep = Vec::with_capacity(self.index.len());
        let all_metas: Vec<_> = self.index.drain(..).collect();
        for meta in all_metas {
            if seen_indices.contains(&meta.last_included_index) {
                let stale_path = self.dir.join(snapshot_filename(
                    meta.last_included_term,
                    meta.last_included_index,
                ));
                if stale_path.exists() {
                    fs::remove_file(&stale_path).map_err(|e| {
                        storage_err(format!(
                            "failed to remove stale same-index snapshot {}: {}",
                            stale_path.display(),
                            e
                        ))
                    })?;
                }
            } else {
                seen_indices.insert(meta.last_included_index);
                keep.push(meta);
            }
        }
        self.index = keep;

        // Clean up corrupt/mismatched files from disk. The safety check
        // above already prevented fallback to older snapshots when the
        // newest is corrupt, so any remaining corrupt files at lower
        // indices are safe to remove.
        for path in corrupt_paths {
            if path.exists() {
                fs::remove_file(&path).map_err(|e| {
                    storage_err(format!(
                        "failed to remove corrupt snapshot file {}: {}",
                        path.display(),
                        e
                    ))
                })?;
            }
        }

        Ok(())
    }

    /// Build the on-disk path for a snapshot.
    fn snapshot_path_from_meta(&self, meta: &SnapshotMeta) -> PathBuf {
        self.dir.join(snapshot_filename(
            meta.last_included_term,
            meta.last_included_index,
        ))
    }

    /// Read metadata from a snapshot file and verify payload CRC integrity.
    ///
    /// Validates the header, voter-set, data_len field, file size, and
    /// streams the payload through a CRC32 hasher to detect same-length
    /// corruption. This ensures that `list_snapshots` never advertises a
    /// snapshot whose payload is corrupt, and that `rebuild_index` can
    /// correctly identify corrupt files for the safety invariant check.
    fn read_meta_from_file(path: &Path) -> Result<SnapshotMeta> {
        let file = File::open(path).map_err(|e| io_with_path(e, path))?;
        let file_len = file.metadata().map_err(|e| io_with_path(e, path))?.len();
        let mut rdr = BufReader::new(file);

        let mut hdr = [0u8; FIXED_HEADER_SIZE];
        rdr.read_exact(&mut hdr).map_err(io_to_storage)?;

        let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        if magic != SNAPSHOT_MAGIC {
            return Err(storage_err("bad snapshot magic"));
        }
        let version = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
        if version != SNAPSHOT_VERSION {
            return Err(storage_err(format!(
                "unsupported snapshot version: {version}"
            )));
        }

        let last_included_index = LogIndex(u64::from_le_bytes(hdr[6..14].try_into().unwrap()));
        let last_included_term = Term(u64::from_le_bytes(hdr[14..22].try_into().unwrap()));
        let voter_set_len = u32::from_le_bytes(hdr[22..26].try_into().unwrap()) as usize;

        if voter_set_len > MAX_VOTER_SET_LEN as usize {
            return Err(storage_err(format!(
                "voter_set_len too large: {voter_set_len}"
            )));
        }

        let voter_set = if voter_set_len > 0 {
            let mut vs_buf = vec![0u8; voter_set_len];
            rdr.read_exact(&mut vs_buf).map_err(io_to_storage)?;
            let vs = bincode::deserialize(&vs_buf)
                .map_err(|e| storage_err(format!("voter_set decode: {e}")))?;
            Some(vs)
        } else {
            None
        };

        // Read and validate data_len against actual file size.
        let mut dl = [0u8; 8];
        rdr.read_exact(&mut dl)
            .map_err(|e| storage_err(format!("failed to read data_len: {e}")))?;
        let data_len = u64::from_le_bytes(dl);

        // Use checked arithmetic to prevent overflow on malformed data_len.
        let expected_file_size = (FIXED_HEADER_SIZE as u64)
            .checked_add(voter_set_len as u64)
            .and_then(|v| v.checked_add(8))
            .and_then(|v| v.checked_add(data_len))
            .and_then(|v| v.checked_add(4))
            .ok_or_else(|| storage_err("overflow computing expected file size from data_len"))?;
        if file_len != expected_file_size {
            return Err(storage_err(format!(
                "snapshot file size mismatch: expected {} bytes (header + vs({}) + data_len({}) + payload), got {}",
                expected_file_size, voter_set_len, data_len, file_len
            )));
        }

        // Stream the payload through a CRC32 hasher to detect same-length
        // corruption without loading the entire payload into memory at once.
        let mut crc_hasher = crc32fast::Hasher::new();
        let remaining_u64 = data_len;
        let mut remaining = usize::try_from(remaining_u64)
            .map_err(|_| storage_err("data_len exceeds addressable memory"))?;
        let mut chunk_buf = vec![0u8; std::cmp::min(remaining, 64 * 1024)];
        while remaining > 0 {
            let to_read = std::cmp::min(chunk_buf.len(), remaining);
            rdr.read_exact(&mut chunk_buf[..to_read]).map_err(|e| {
                storage_err(format!("failed to read payload for CRC verification: {e}"))
            })?;
            crc_hasher.update(&chunk_buf[..to_read]);
            remaining -= to_read;
        }
        let computed_crc = crc_hasher.finalize();

        // Read the stored CRC from end of file.
        let mut crc_buf = [0u8; 4];
        rdr.read_exact(&mut crc_buf)
            .map_err(|e| storage_err(format!("failed to read stored CRC: {e}")))?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        if computed_crc != stored_crc {
            return Err(storage_err(format!(
                "snapshot payload CRC mismatch during indexing: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
            )));
        }

        let id = canonical_id(last_included_term, last_included_index);

        Ok(SnapshotMeta {
            last_included_index,
            last_included_term,
            id,
            voter_set,
            size_bytes: Some(data_len),
            checksum: Some(stored_crc as u64),
        })
    }

    /// Read metadata + data from a snapshot file.
    fn read_snapshot_file(path: &Path) -> Result<(SnapshotMeta, Vec<u8>)> {
        let buf = fs::read(path).map_err(|e| io_with_path(e, path))?;
        decode_snapshot(&buf)
    }

    /// Prune old snapshots beyond `max_retained`.
    ///
    /// File deletion is performed *before* removing the entry from the
    /// in-memory index so that a failed `remove_file` does not leave the
    /// index out of sync with disk.
    fn prune(&mut self) -> Result<()> {
        if self.max_retained == 0 {
            return Ok(());
        }
        while self.index.len() > self.max_retained {
            if let Some(old) = self.index.last() {
                let path = self.snapshot_path_from_meta(old);
                if path.exists() {
                    fs::remove_file(&path).map_err(|e| io_with_path(e, &path))?;
                }
                self.index.pop();
            }
        }
        Ok(())
    }

    /// Crash-safe write using backup-then-rename.
    ///
    /// 1. Write data to `path.tmp`, flush, fsync.
    /// 2. If `path` exists, rename it to `path.bak`.
    /// 3. Rename `path.tmp` to `path`.
    /// 4. Remove `path.bak`.
    /// 5. Best-effort directory fsync (Unix).
    ///
    /// On crash at any step, [`recover_incomplete_writes`] restores consistency.
    fn atomic_write(&self, path: &Path, blob: &[u8]) -> Result<()> {
        let tmp_path = path.with_extension("bin.tmp");
        let bak_path = path.with_extension("bin.bak");

        // Write + fsync temp file.
        {
            let mut f = File::create(&tmp_path).map_err(|e| io_with_path(e, &tmp_path))?;
            f.write_all(blob).map_err(|e| io_with_path(e, &tmp_path))?;
            f.flush().map_err(|e| io_with_path(e, &tmp_path))?;
            f.sync_all().map_err(|e| io_with_path(e, &tmp_path))?;
        }

        // If destination exists, rename to backup first.
        // Remove any pre-existing stale .bak first to prevent a failed
        // overwrite from blocking the entire save operation.
        if path.exists()
            && bak_path.exists()
            && let Err(e) = fs::remove_file(&bak_path)
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(storage_err(format!(
                "failed to remove stale backup before save: {e}"
            )));
        }
        if path.exists()
            && let Err(e) = fs::rename(path, &bak_path)
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(io_with_path(e, path));
        }

        // Rename temp to destination.
        if let Err(e) = fs::rename(&tmp_path, path) {
            if bak_path.exists() {
                let _ = fs::rename(&bak_path, path);
            }
            let _ = fs::remove_file(&tmp_path);
            return Err(io_with_path(e, &tmp_path));
        }

        // Remove backup.
        if bak_path.exists() {
            let _ = fs::remove_file(&bak_path);
        }

        // Best-effort directory fsync (Unix only).
        #[cfg(unix)]
        {
            if let Ok(dir_file) = File::open(&self.dir) {
                let _ = dir_file.sync_all();
            }
        }

        Ok(())
    }

    /// Read a snapshot's payload data in fixed-size chunks, streaming from
    /// disk without loading the entire file into memory.
    ///
    /// The `meta` parameter is used only to locate the file on disk. The
    /// actual metadata returned in chunk items is read from the file header
    /// and verified against the caller's metadata. This prevents streaming
    /// a valid file with incorrect caller-supplied metadata in
    /// `FetchSnapshotChunk` RPCs.
    ///
    /// The reader also computes a rolling CRC32 over the streamed payload
    /// and verifies it against the stored checksum on the final chunk.
    ///
    /// Default chunk size is 1 MiB when `chunk_size` is 0.
    pub fn chunked_reader(
        &self,
        meta: &SnapshotMeta,
        chunk_size: usize,
    ) -> Result<SnapshotChunkReader> {
        let path = self.snapshot_path_from_meta(meta);
        let file = File::open(&path).map_err(|e| io_with_path(e, &path))?;
        let file_len = file.metadata().map_err(|e| io_with_path(e, &path))?.len();
        let mut reader = BufReader::new(file);

        let (file_meta, _header_size, data_len, stored_crc) =
            decode_header_for_streaming(&mut reader, file_len)?;

        // Verify caller metadata matches file metadata. This prevents
        // streaming a valid file with wrong id/voter_set in the chunks.
        if file_meta.last_included_index != meta.last_included_index
            || file_meta.last_included_term != meta.last_included_term
        {
            return Err(storage_err(format!(
                "chunked_reader: caller metadata (term={}, index={}) does not match \
                 file metadata (term={}, index={})",
                meta.last_included_term.0,
                meta.last_included_index.0,
                file_meta.last_included_term.0,
                file_meta.last_included_index.0,
            )));
        }

        let chunk_size = if chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            chunk_size
        };

        let data_len_usize = usize::try_from(data_len).map_err(|_| {
            storage_err("snapshot data_len exceeds addressable memory for chunked reading")
        })?;

        let total_chunks = if data_len_usize == 0 {
            1
        } else {
            // Use checked arithmetic to prevent overflow with pathological
            // chunk sizes (e.g. chunk_size = 1 on a very large snapshot).
            data_len_usize
                .checked_add(chunk_size - 1)
                .map(|v| v / chunk_size)
                .ok_or_else(|| storage_err("overflow computing chunk count"))?
        };

        Ok(SnapshotChunkReader {
            reader,
            meta: Some(file_meta),
            chunk_size,
            remaining: data_len_usize,
            data_len: data_len_usize,
            chunk_index: 0,
            total_chunks,
            finished: false,
            crc_hasher: crc32fast::Hasher::new(),
            stored_crc,
            skip_crc: false,
            window_covers_tail: true,
            first_emitted: false,
        })
    }

    /// Create a streaming chunk reader starting at a byte `offset` into the
    /// payload, optionally limited to `max_bytes`.
    ///
    /// This enables resumable snapshot transfer: the leader seeks directly
    /// to the byte offset requested by the follower's [`FetchSnapshotRequest`]
    /// without reading or discarding preceding payload data.
    ///
    /// When `offset > 0`, the CRC check covers only the streamed window —
    /// full-payload CRC validation is the responsibility of the receiver
    /// after reassembling all chunks.
    ///
    /// Returns an empty reader when `offset >= data_len`.
    pub fn chunked_reader_from_offset(
        &self,
        meta: &SnapshotMeta,
        chunk_size: usize,
        offset: u64,
        max_bytes: Option<u64>,
    ) -> Result<SnapshotChunkReader> {
        let path = self.snapshot_path_from_meta(meta);
        let file = File::open(&path).map_err(|e| io_with_path(e, &path))?;
        let file_len = file.metadata().map_err(|e| io_with_path(e, &path))?.len();
        let mut reader = BufReader::new(file);

        let (file_meta, header_size, data_len, stored_crc) =
            decode_header_for_streaming(&mut reader, file_len)?;

        if file_meta.last_included_index != meta.last_included_index
            || file_meta.last_included_term != meta.last_included_term
        {
            return Err(storage_err(format!(
                "chunked_reader_from_offset: caller metadata (term={}, index={}) does not match \
                 file metadata (term={}, index={})",
                meta.last_included_term.0,
                meta.last_included_index.0,
                file_meta.last_included_term.0,
                file_meta.last_included_index.0,
            )));
        }

        let chunk_size = if chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            chunk_size
        };

        let data_len_usize = usize::try_from(data_len).map_err(|_| {
            storage_err("snapshot data_len exceeds addressable memory for chunked reading")
        })?;

        let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);

        // If offset is beyond the payload, return a reader that yields a
        // single empty done chunk with metadata (so the receiver knows the
        // transfer is complete).
        if offset_usize >= data_len_usize && data_len_usize > 0 {
            // Compute the logical chunk index that the offset corresponds to.
            // Use `checked_div` to satisfy clippy's `manual_checked_ops` lint
            // (new in Rust 1.95, promoted to error by the workspace's
            // `-D warnings` policy).
            let logical_chunk_index =
                (offset_usize.checked_div(chunk_size).unwrap_or(0)) as u64;
            return Ok(SnapshotChunkReader {
                reader,
                meta: Some(file_meta),
                chunk_size,
                remaining: 0,
                data_len: 0, // treat as empty so iterator yields one done chunk
                chunk_index: logical_chunk_index,
                total_chunks: 1,
                finished: false,
                crc_hasher: crc32fast::Hasher::new(),
                stored_crc,
                skip_crc: true,
                window_covers_tail: true,
                first_emitted: false,
            });
        }

        // Empty payload with offset > 0: yield a single empty done chunk.
        if data_len_usize == 0 && offset > 0 {
            return Ok(SnapshotChunkReader {
                reader,
                meta: Some(file_meta),
                chunk_size,
                remaining: 0,
                data_len: 0,
                chunk_index: 0,
                total_chunks: 1,
                finished: false,
                crc_hasher: crc32fast::Hasher::new(),
                stored_crc,
                skip_crc: true,
                window_covers_tail: true,
                first_emitted: false,
            });
        }

        // Compute the effective window size.
        let remaining_from_offset = data_len_usize.saturating_sub(offset_usize);
        let effective_window = match max_bytes {
            Some(mb) if mb > 0 => {
                let mb_usize = usize::try_from(mb).unwrap_or(usize::MAX);
                std::cmp::min(remaining_from_offset, mb_usize)
            }
            _ => remaining_from_offset,
        };

        // Seek past the offset bytes within the payload.
        if offset > 0 {
            let payload_start = header_size;
            reader
                .seek(SeekFrom::Start(payload_start + offset))
                .map_err(|e| io_with_path(e, &path))?;
        }

        let total_chunks = if effective_window == 0 {
            1
        } else {
            effective_window
                .checked_add(chunk_size - 1)
                .map(|v| v / chunk_size)
                .ok_or_else(|| storage_err("overflow computing chunk count"))?
        };

        // Use `checked_div` to satisfy clippy's `manual_checked_ops` lint
        // (new in Rust 1.95). Same shape as `logical_chunk_index` above.
        let base_chunk_index = (offset_usize.checked_div(chunk_size).unwrap_or(0)) as u64;

        // Partial reads (offset > 0 or max_bytes < data_len) skip CRC since
        // the hasher only covers the streamed window, not the full payload.
        // Full-payload CRC validation is the receiver's responsibility after
        // reassembling all chunks.
        let is_partial = offset > 0 || effective_window < data_len_usize;

        // The window covers the tail of the payload when the bytes we'll
        // stream reach the end of the full payload data.
        let window_covers_tail = offset_usize + effective_window >= data_len_usize;

        Ok(SnapshotChunkReader {
            reader,
            meta: Some(file_meta),
            chunk_size,
            remaining: effective_window,
            data_len: data_len_usize,
            chunk_index: base_chunk_index,
            total_chunks,
            finished: false,
            crc_hasher: crc32fast::Hasher::new(),
            stored_crc,
            skip_crc: is_partial,
            window_covers_tail,
            first_emitted: false,
        })
    }
}

impl SnapshotStore for FileSnapshotStore {
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> Result<()> {
        // Enforce voter_set is present on all new saves.
        if metadata.voter_set.is_none() {
            return Err(storage_err(
                "save_snapshot: voter_set is required for new snapshots",
            ));
        }

        // Reject lower-term saves at the same index to prevent term regression.
        if let Some(existing) = self
            .index
            .iter()
            .find(|m| m.last_included_index == metadata.last_included_index)
            && existing.last_included_term > metadata.last_included_term
        {
            return Err(storage_err(format!(
                "refusing to replace snapshot at index {} term {} with lower term {}",
                metadata.last_included_index.0,
                existing.last_included_term.0,
                metadata.last_included_term.0,
            )));
        }

        // Normalize id to canonical form and populate size/checksum.
        let crc = crc32fast::hash(data);
        let metadata = SnapshotMeta {
            id: canonical_id(metadata.last_included_term, metadata.last_included_index),
            size_bytes: Some(data.len() as u64),
            checksum: Some(crc as u64),
            ..metadata
        };

        let path = self.snapshot_path_from_meta(&metadata);
        let blob = encode_snapshot(&metadata, data)?;

        self.atomic_write(&path, &blob)?;

        // Remove stale entries at same index (handles same-index/different-term).
        // Do this BEFORE inserting the new entry to avoid crash-window ambiguity.
        let stale: Vec<SnapshotMeta> = self
            .index
            .iter()
            .filter(|m| m.last_included_index == metadata.last_included_index)
            .cloned()
            .collect();
        for old in &stale {
            let old_path = self.snapshot_path_from_meta(old);
            if old_path != path && old_path.exists() {
                fs::remove_file(&old_path).map_err(|e| {
                    storage_err(format!(
                        "failed to remove stale same-index snapshot {}: {}",
                        old_path.display(),
                        e
                    ))
                })?;
            }
        }
        self.index
            .retain(|m| m.last_included_index != metadata.last_included_index);

        // Insert maintaining newest-first sort order.
        let pos = self
            .index
            .iter()
            .position(|m| m.last_included_index < metadata.last_included_index)
            .unwrap_or(self.index.len());
        self.index.insert(pos, metadata);

        self.prune()?;
        Ok(())
    }

    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>> {
        if self.index.is_empty() {
            return Ok(None);
        }

        // The index only contains snapshots that passed validation during
        // rebuild_index (header integrity, file size, filename consistency).
        // Corrupt files are excluded from the index at open time, so index[0]
        // is the newest *valid* snapshot. If a read error occurs here (e.g.
        // the file was removed or corrupted after open), we surface the error
        // rather than silently falling back to an older snapshot.
        let newest = &self.index[0];
        let path = self.snapshot_path_from_meta(newest);
        let (loaded_meta, data) = Self::read_snapshot_file(&path)?;
        Ok(Some((loaded_meta, data)))
    }

    fn load_snapshot(
        &self,
        index: LogIndex,
        term: Term,
    ) -> Result<Option<(SnapshotMeta, Vec<u8>)>> {
        let target_id = canonical_id(term, index);
        let meta = match self.index.iter().find(|m| m.id == target_id) {
            Some(m) => m,
            None => return Ok(None),
        };
        let path = self.snapshot_path_from_meta(meta);
        let (loaded_meta, data) = Self::read_snapshot_file(&path)?;
        Ok(Some((loaded_meta, data)))
    }

    fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>> {
        Ok(self.index.clone())
    }

    fn delete_snapshot(&mut self, id: &str) -> Result<()> {
        // Try exact match on canonical id first. If no match, attempt to
        // parse term+index from the caller-supplied id and match by those
        // fields. This prevents silent no-ops when the caller supplies a
        // non-canonical id.
        let pos = self.index.iter().position(|m| m.id == id).or_else(|| {
            // Try to extract term+index from the id string.
            let path = Path::new(id);
            // parse_snapshot_filename expects a .bin extension; try adding one.
            let with_ext = PathBuf::from(format!("{}.bin", id));
            parse_snapshot_filename(&with_ext)
                .and_then(|(term, index)| {
                    let cid = canonical_id(term, index);
                    self.index.iter().position(|m| m.id == cid)
                })
                .or_else(|| {
                    parse_snapshot_filename(path).and_then(|(term, index)| {
                        let cid = canonical_id(term, index);
                        self.index.iter().position(|m| m.id == cid)
                    })
                })
        });

        if let Some(pos) = pos {
            let path = self.snapshot_path_from_meta(&self.index[pos]);
            if path.exists() {
                fs::remove_file(&path).map_err(|e| io_with_path(e, &path))?;
            } else {
                // The file was expected to be on disk (it was in our index)
                // but has been externally removed. Log a warning and proceed
                // with removing from the index to maintain consistency.
                tracing::warn!(
                    id = id,
                    path = %path.display(),
                    "snapshot file missing from disk during delete (externally removed?)"
                );
            }
            self.index.remove(pos);
        } else {
            return Err(storage_err(format!("snapshot not found: {id}")));
        }
        Ok(())
    }

    fn snapshot_exists(&self, index: LogIndex, term: Term) -> bool {
        // Only report existence for snapshots that have been validated into
        // the in-memory index (i.e. files whose header/CRC were checked on
        // open or save). An externally-dropped file that has not been
        // validated through rebuild_index is not considered to exist.
        let target_id = canonical_id(term, index);
        self.index.iter().any(|m| m.id == target_id)
    }

    fn find_by_id(&self, id: &str) -> Result<Option<SnapshotMeta>> {
        Ok(self.index.iter().find(|m| m.id == id).cloned())
    }

    /// Override the default in-memory chunking with efficient file streaming.
    ///
    /// Delegates to [`FileSnapshotStore::chunked_reader`] which reads directly
    /// from the snapshot file without loading the full payload into memory.
    fn snapshot_reader(
        &self,
        meta: &SnapshotMeta,
        chunk_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Result<SnapshotChunkItem>> + Send>> {
        let reader = self.chunked_reader(meta, chunk_size)?;
        Ok(Box::new(reader))
    }

    /// Offset-aware file streaming for resumable FetchSnapshot RPCs.
    ///
    /// Seeks directly to the requested byte offset in the snapshot file,
    /// avoiding the need to read and discard preceding data. When `max_bytes`
    /// is specified and non-zero, limits the total bytes yielded.
    fn snapshot_reader_from_offset(
        &self,
        meta: &SnapshotMeta,
        chunk_size: usize,
        offset: u64,
        max_bytes: Option<u64>,
    ) -> Result<Box<dyn Iterator<Item = Result<SnapshotChunkItem>> + Send>> {
        let reader = self.chunked_reader_from_offset(meta, chunk_size, offset, max_bytes)?;
        Ok(Box::new(reader))
    }
}

// ---------------------------------------------------------------------------
// SnapshotChunkItem & SnapshotChunkReader
// ---------------------------------------------------------------------------

// SnapshotChunkItem is defined in xraft_core::storage. The `into_fetch_chunk`
// convenience method lives there as well to satisfy Rust orphan rules.

/// Streaming iterator over fixed-size chunks of snapshot payload data.
///
/// Created via [`FileSnapshotStore::chunked_reader`]. Reads directly from the
/// underlying file via a buffered reader. The full snapshot is never loaded
/// into memory at once.
///
/// Read errors are surfaced as `Some(Err(...))`, never silently swallowed
/// as `None`. A truncated or corrupt file during streaming produces an
/// explicit error, distinguishable from a clean end of stream.
pub struct SnapshotChunkReader {
    reader: BufReader<File>,
    meta: Option<SnapshotMeta>,
    chunk_size: usize,
    remaining: usize,
    data_len: usize,
    chunk_index: u64,
    total_chunks: usize,
    finished: bool,
    /// Incrementally computed CRC32 over streamed payload data.
    crc_hasher: crc32fast::Hasher,
    /// CRC32 value stored in the snapshot file, verified on final chunk.
    stored_crc: u32,
    /// When true, skip the final CRC check (used for offset-based partial reads
    /// where the CRC covers only the window, not the full payload).
    skip_crc: bool,
    /// True when this reader's window extends to the end of the full payload.
    /// Used to set the `done` flag correctly: `done` means "full snapshot
    /// payload exhausted", not "window exhausted".
    window_covers_tail: bool,
    /// Whether the first chunk has been emitted yet. Used to ensure the first
    /// yielded chunk always carries metadata, regardless of `chunk_index`.
    first_emitted: bool,
}

impl SnapshotChunkReader {
    /// Total payload size in bytes.
    pub fn total_size(&self) -> usize {
        self.data_len
    }

    /// Number of chunks that will be yielded in total.
    pub fn chunk_count(&self) -> usize {
        self.total_chunks
    }
}

impl Iterator for SnapshotChunkReader {
    type Item = Result<SnapshotChunkItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // Empty payload: yield exactly one chunk with metadata and empty data.
        // CRC of empty data is verified immediately (unless skip_crc).
        if self.data_len == 0 {
            self.finished = true;
            if !self.skip_crc {
                let computed_crc = self.crc_hasher.clone().finalize();
                if computed_crc != self.stored_crc {
                    return Some(Err(storage_err(format!(
                        "snapshot payload CRC mismatch during streaming: stored {:#010x}, computed {:#010x}",
                        self.stored_crc, computed_crc
                    ))));
                }
            }
            let metadata = self.meta.take();
            return Some(Ok(SnapshotChunkItem {
                chunk_index: self.chunk_index,
                data: Vec::new(),
                done: true,
                metadata,
            }));
        }

        if self.remaining == 0 {
            return None;
        }

        let to_read = std::cmp::min(self.chunk_size, self.remaining);
        let mut buf = vec![0u8; to_read];
        if let Err(e) = self.reader.read_exact(&mut buf) {
            // Surface read errors explicitly instead of silently returning None.
            self.finished = true;
            return Some(Err(storage_err(format!(
                "chunk read error at offset {}: {e}",
                self.data_len - self.remaining
            ))));
        }
        self.crc_hasher.update(&buf);
        self.remaining -= to_read;

        let idx = self.chunk_index;
        self.chunk_index += 1;
        let window_exhausted = self.remaining == 0;
        // `done` means the entire snapshot payload has been consumed, not just
        // the current transfer window. Only set done when the window covers
        // the tail of the payload AND the window is exhausted.
        let done = window_exhausted && self.window_covers_tail;
        if window_exhausted {
            self.finished = true;
            // Verify CRC on final chunk (skip for partial/offset reads).
            if !self.skip_crc {
                let computed_crc = self.crc_hasher.clone().finalize();
                if computed_crc != self.stored_crc {
                    return Some(Err(storage_err(format!(
                        "snapshot payload CRC mismatch during streaming: stored {:#010x}, computed {:#010x}",
                        self.stored_crc, computed_crc
                    ))));
                }
            }
        }
        // First yielded chunk always carries metadata, regardless of chunk_index.
        let metadata = if !self.first_emitted {
            self.first_emitted = true;
            self.meta.take()
        } else {
            None
        };

        Some(Ok(SnapshotChunkItem {
            chunk_index: idx,
            data: buf,
            done,
            metadata,
        }))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::{DirectoryId, Endpoint, LogIndex, NodeId, Term, VoterRecord, VoterSet};

    fn make_test_voter_set() -> xraft_core::types::VoterSet {
        VoterSet::try_new(vec![VoterRecord {
            node_id: NodeId(1),
            directory_id: DirectoryId::new_random(),
            endpoints: vec![Endpoint::new("127.0.0.1", 9000)],
        }])
        .unwrap()
    }

    fn test_meta(id: &str, index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(index),
            last_included_term: Term(term),
            id: id.to_string(),
            voter_set: Some(make_test_voter_set()),
            size_bytes: None,
            checksum: None,
        }
    }

    /// Helper for tests that specifically exercise the legacy/load path
    /// where voter_set may be None (e.g. decode tests for on-disk format).
    fn test_meta_no_voter_set(id: &str, index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(index),
            last_included_term: Term(term),
            id: id.to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        }
    }

    /// Compute the byte offset where payload data begins in a raw snapshot
    /// file. Accounts for the variable-length voter-set encoding.
    fn payload_offset_in_raw(raw: &[u8]) -> usize {
        let vs_len = u32::from_le_bytes(raw[22..26].try_into().unwrap()) as usize;
        FIXED_HEADER_SIZE + vs_len + 8
    }

    // -----------------------------------------------------------------------
    // MemorySnapshotStore tests
    // -----------------------------------------------------------------------

    #[test]
    fn empty_store_returns_none() {
        let store = MemorySnapshotStore::new();
        assert!(store.load_latest_snapshot().unwrap().is_none());
        assert!(store.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn save_and_load() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"state-data")
            .unwrap();
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(
            meta.id, "snapshot-0000000002-00000000000000000010",
            "id is canonical"
        );
        assert_eq!(meta.last_included_index, LogIndex(10));
        assert_eq!(data, b"state-data");
    }

    #[test]
    fn latest_is_last_saved() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"v1")
            .unwrap();
        store
            .save_snapshot(test_meta("snap-2", 20, 3), b"v2")
            .unwrap();
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.id, "snapshot-0000000003-00000000000000000020");
        assert_eq!(data, b"v2");
    }

    #[test]
    fn latest_selects_by_index_not_insertion_order() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("snap-2", 20, 3), b"v2")
            .unwrap();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"v1")
            .unwrap();
        let (meta, _) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.id, "snapshot-0000000003-00000000000000000020");
        assert_eq!(meta.last_included_index, LogIndex(20));
    }

    #[test]
    fn list_newest_first() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store.save_snapshot(test_meta("c", 3, 2), b"").unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].id, "snapshot-0000000002-00000000000000000003");
        assert_eq!(list[2].id, "snapshot-0000000001-00000000000000000001");
    }

    #[test]
    fn memory_save_populates_size_bytes_and_checksum() {
        let mut store = MemorySnapshotStore::new();
        let data = b"hello-snapshot";
        store.save_snapshot(test_meta("x", 10, 2), data).unwrap();
        let (meta, _) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.size_bytes, Some(data.len() as u64));
        assert_eq!(meta.checksum, Some(crc32fast::hash(data) as u64));
    }

    #[test]
    fn list_sorts_by_index_not_insertion_order() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("c", 3, 2), b"").unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list[0].id, "snapshot-0000000002-00000000000000000003");
        assert_eq!(list[1].id, "snapshot-0000000001-00000000000000000002");
        assert_eq!(list[2].id, "snapshot-0000000001-00000000000000000001");
    }

    #[test]
    fn delete_snapshot() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store
            .delete_snapshot("snapshot-0000000001-00000000000000000001")
            .unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "snapshot-0000000001-00000000000000000002");
    }

    #[test]
    fn memory_delete_with_caller_id_fails_use_canonical() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("my-custom-id", 5, 1), b"data")
            .unwrap();
        // Deleting by the original caller id returns an error — must use canonical id.
        let result = store.delete_snapshot("my-custom-id");
        assert!(result.is_err(), "non-canonical/unknown id must error");
        assert_eq!(store.list_snapshots().unwrap().len(), 1);
        // Canonical id works.
        store
            .delete_snapshot("snapshot-0000000001-00000000000000000005")
            .unwrap();
        assert!(store.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn memory_delete_with_unpadded_snapshot_id() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("x", 5, 1), b"data").unwrap();
        // Delete using an unpadded but parseable snapshot id.
        store.delete_snapshot("snapshot-1-5").unwrap();
        assert!(
            store.list_snapshots().unwrap().is_empty(),
            "unpadded snapshot-term-index id must resolve to canonical and delete"
        );
    }

    #[test]
    fn file_delete_with_unpadded_snapshot_id() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store.save_snapshot(test_meta("x", 42, 7), b"data").unwrap();
        // Delete using an unpadded but parseable snapshot id.
        store.delete_snapshot("snapshot-7-42").unwrap();
        assert!(
            store.list_snapshots().unwrap().is_empty(),
            "unpadded snapshot-term-index id must resolve to canonical and delete"
        );
    }

    #[test]
    fn memory_save_same_index_replaces() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("v1", 10, 2), b"first")
            .unwrap();
        store
            .save_snapshot(test_meta("v2", 10, 3), b"second")
            .unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "snapshot-0000000003-00000000000000000010");
        let (_, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(data, b"second");
    }

    #[test]
    fn memory_save_rejects_none_voter_set() {
        let mut store = MemorySnapshotStore::new();
        let result = store.save_snapshot(test_meta_no_voter_set("x", 10, 2), b"data");
        assert!(result.is_err(), "save_snapshot must reject voter_set=None");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("voter_set is required"),
            "error should explain why, got: {err_msg}"
        );
    }

    #[test]
    fn file_save_rejects_none_voter_set() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open(dir.path()).unwrap();
        let result = store.save_snapshot(test_meta_no_voter_set("x", 10, 2), b"data");
        assert!(result.is_err(), "save_snapshot must reject voter_set=None");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("voter_set is required"),
            "error should explain why, got: {err_msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Binary encode/decode tests
    // -----------------------------------------------------------------------

    #[test]
    fn encode_decode_roundtrip_no_voter_set() {
        let meta = test_meta_no_voter_set("my-custom-snap-id", 10, 2);
        let data = b"hello world";
        let encoded = encode_snapshot(&meta, data).unwrap();
        let (decoded_meta, decoded_data) = decode_snapshot(&encoded).unwrap();
        // id is derived from term+index, not the original caller-supplied id.
        assert_eq!(decoded_meta.id, "snapshot-0000000002-00000000000000000010");
        assert_eq!(decoded_meta.last_included_index, LogIndex(10));
        assert_eq!(decoded_meta.last_included_term, Term(2));
        assert!(decoded_meta.voter_set.is_none());
        assert_eq!(decoded_data, data);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut encoded = encode_snapshot(&test_meta_no_voter_set("x", 1, 1), b"data").unwrap();
        encoded[0] = 0xFF;
        assert!(decode_snapshot(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_truncated_header() {
        let encoded = encode_snapshot(&test_meta_no_voter_set("x", 1, 1), b"data").unwrap();
        assert!(decode_snapshot(&encoded[..20]).is_err());
    }

    #[test]
    fn encode_decode_canonical_id_from_term_index() {
        let test_cases = vec![
            (1u64, 1u64, "snapshot-0000000001-00000000000000000001"),
            (5, 100, "snapshot-0000000005-00000000000000000100"),
            (10, 42, "snapshot-0000000010-00000000000000000042"),
        ];
        for (term, index, expected_id) in test_cases {
            let meta = SnapshotMeta {
                last_included_index: LogIndex(index),
                last_included_term: Term(term),
                id: "any-user-id".to_string(),
                voter_set: None, // encode/decode doesn't enforce voter_set
                size_bytes: None,
                checksum: None,
            };
            let encoded = encode_snapshot(&meta, b"x").unwrap();
            let (decoded, _) = decode_snapshot(&encoded).unwrap();
            assert_eq!(decoded.id, expected_id);
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let meta = test_meta_no_voter_set("trail-test", 5, 1);
        let data = b"payload";
        let mut encoded = encode_snapshot(&meta, data).unwrap();
        encoded.extend_from_slice(b"EXTRAGARBAGE123");
        let result = decode_snapshot(&encoded);
        assert!(result.is_err(), "trailing bytes should be rejected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("trailing"),
            "error should mention trailing bytes"
        );
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let encoded =
            encode_snapshot(&test_meta_no_voter_set("x", 1, 1), b"long data here").unwrap();
        let short = &encoded[..encoded.len() - 5];
        assert!(decode_snapshot(short).is_err());
    }

    #[test]
    fn decode_rejects_oversized_voter_set_len() {
        let meta = test_meta_no_voter_set("x", 1, 1);
        let mut encoded = encode_snapshot(&meta, b"data").unwrap();
        // Overwrite voter_set_len (bytes 22..26) with a value exceeding MAX_VOTER_SET_LEN.
        let big_len: u32 = MAX_VOTER_SET_LEN + 1;
        encoded[22..26].copy_from_slice(&big_len.to_le_bytes());
        let result = decode_snapshot(&encoded);
        assert!(result.is_err(), "voter_set_len > MAX must be rejected");
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("voter_set_len too large"), "got: {msg}");
    }

    // -----------------------------------------------------------------------
    // FileSnapshotStore tests
    // -----------------------------------------------------------------------

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn file_creates_snapshots_subdir() {
        let dir = temp_dir();
        let store = FileSnapshotStore::open(dir.path()).unwrap();
        assert!(store.dir().ends_with("snapshots"));
        assert!(dir.path().join("snapshots").is_dir());
    }

    #[test]
    fn file_from_config_uses_retention_count() {
        let dir = temp_dir();
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test"
listen_addr = "127.0.0.1:6000"
snapshot_retention_count = 2
data_dir = "{}"
"#,
            dir.path().display().to_string().replace('\\', "\\\\")
        );
        let config = xraft_core::config::ClusterConfig::from_toml_str(&toml).unwrap();
        let mut store = FileSnapshotStore::from_config(&config).unwrap();
        for i in 1..=4u64 {
            store
                .save_snapshot(test_meta(&format!("s{i}"), i * 10, i), b"data")
                .unwrap();
        }
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 2, "retention from config is 2");
        assert_eq!(list[0].last_included_index, LogIndex(40));
        assert_eq!(list[1].last_included_index, LogIndex(30));
    }

    #[test]
    fn file_empty_store_returns_none() {
        let dir = temp_dir();
        let store = FileSnapshotStore::open(dir.path()).unwrap();
        assert!(store.load_latest_snapshot().unwrap().is_none());
        assert!(store.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn file_save_and_load_roundtrips_metadata() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open(dir.path()).unwrap();
        let data = b"state-data";
        store
            .save_snapshot(test_meta("my-custom-id", 10, 2), data)
            .unwrap();
        let (meta, loaded_data) = store.load_latest_snapshot().unwrap().unwrap();
        // id is canonical: derived from term + index
        assert_eq!(meta.id, "snapshot-0000000002-00000000000000000010");
        assert_eq!(meta.last_included_index, LogIndex(10));
        assert_eq!(meta.last_included_term, Term(2));
        assert_eq!(loaded_data, data);
        // size_bytes and checksum are populated by the store on save/load.
        assert_eq!(meta.size_bytes, Some(data.len() as u64));
        assert_eq!(meta.checksum, Some(crc32fast::hash(data) as u64));
    }

    #[test]
    fn file_save_and_load_roundtrips_after_reopen() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open(dir.path()).unwrap();
            store
                .save_snapshot(test_meta("my-id-42", 10, 2), b"payload")
                .unwrap();
        }
        let store = FileSnapshotStore::open(dir.path()).unwrap();
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(
            meta.id, "snapshot-0000000002-00000000000000000010",
            "canonical id roundtrips across reopen"
        );
        assert_eq!(data, b"payload");
    }

    #[test]
    fn file_save_normalizes_caller_id() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open(dir.path()).unwrap();
        store
            .save_snapshot(test_meta("arbitrary-caller-id", 42, 7), b"data")
            .unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(
            list[0].id, "snapshot-0000000007-00000000000000000042",
            "id is canonical, not caller-supplied"
        );
    }

    #[test]
    fn file_latest_selects_by_index() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open(dir.path()).unwrap();
        store
            .save_snapshot(test_meta("snap-2", 20, 3), b"v2")
            .unwrap();
        store
            .save_snapshot(test_meta("snap-1", 10, 2), b"v1")
            .unwrap();
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.last_included_index, LogIndex(20));
        assert_eq!(data, b"v2");
    }

    #[test]
    fn file_list_newest_first() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store.save_snapshot(test_meta("a", 1, 1), b"").unwrap();
        store.save_snapshot(test_meta("b", 2, 1), b"").unwrap();
        store.save_snapshot(test_meta("c", 3, 2), b"").unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].last_included_index, LogIndex(3));
        assert_eq!(list[2].last_included_index, LogIndex(1));
    }

    #[test]
    fn file_delete_snapshot() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(
                test_meta("snapshot-0000000001-00000000000000000001", 1, 1),
                b"da",
            )
            .unwrap();
        store
            .save_snapshot(
                test_meta("snapshot-0000000001-00000000000000000002", 2, 1),
                b"db",
            )
            .unwrap();
        store
            .delete_snapshot("snapshot-0000000001-00000000000000000001")
            .unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].last_included_index, LogIndex(2));
        let snap_dir = dir.path().join("snapshots");
        let files: Vec<_> = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == SNAPSHOT_EXT))
            .collect();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn file_survives_reopen() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("snap-1", 10, 2), b"hello")
                .unwrap();
            store
                .save_snapshot(test_meta("snap-2", 20, 3), b"world")
                .unwrap();
        }
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].last_included_index, LogIndex(20));
        assert_eq!(list[1].last_included_index, LogIndex(10));

        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.last_included_index, LogIndex(20));
        assert_eq!(data, b"world");
    }

    #[test]
    fn file_prune_oldest_beyond_retention() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 2).unwrap();
        store.save_snapshot(test_meta("a", 1, 1), b"da").unwrap();
        store.save_snapshot(test_meta("b", 2, 1), b"db").unwrap();
        store.save_snapshot(test_meta("c", 3, 2), b"dc").unwrap();

        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].last_included_index, LogIndex(3));
        assert_eq!(list[1].last_included_index, LogIndex(2));

        let snap_dir = dir.path().join("snapshots");
        let files: Vec<_> = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == SNAPSHOT_EXT))
            .collect();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn file_retention_default_3() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open(dir.path()).unwrap();
        for i in 1..=5u64 {
            store
                .save_snapshot(test_meta(&format!("s{i}"), i * 10, i), b"data")
                .unwrap();
        }
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 3, "default retention is 3");
        assert_eq!(list[0].last_included_index, LogIndex(50));
        assert_eq!(list[1].last_included_index, LogIndex(40));
        assert_eq!(list[2].last_included_index, LogIndex(30));
    }

    #[test]
    fn file_retention_enforced_on_open() {
        let dir = temp_dir();
        // Save 5 snapshots with unlimited retention.
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            for i in 1..=5u64 {
                store
                    .save_snapshot(test_meta(&format!("s{i}"), i * 10, i), b"data")
                    .unwrap();
            }
            assert_eq!(store.list_snapshots().unwrap().len(), 5);
        }

        // Reopen with retention=2 — excess snapshots should be pruned
        // immediately on open, not deferred to the next save.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 2).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(
            list.len(),
            2,
            "retention must be enforced on open, not just on save"
        );
        assert_eq!(list[0].last_included_index, LogIndex(50));
        assert_eq!(list[1].last_included_index, LogIndex(40));

        // Verify files on disk match.
        let snap_dir = dir.path().join("snapshots");
        let file_count = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == SNAPSHOT_EXT))
            .count();
        assert_eq!(
            file_count, 2,
            "excess snapshot files must be deleted on open"
        );
    }

    #[test]
    fn file_retention_enforced_on_open_with_preexisting_files() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Manually write 4 snapshot files (simulating files copied in or
        // left over from a previous configuration).
        for i in 1..=4u64 {
            let meta = test_meta(&format!("pre{i}"), i * 10, i);
            let blob = encode_snapshot(&meta, b"preexisting").unwrap();
            std::fs::write(
                snap_dir.join(snapshot_filename(Term(i), LogIndex(i * 10))),
                &blob,
            )
            .unwrap();
        }

        // Open with retention=2 — should prune immediately.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 2).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].last_included_index, LogIndex(40));
        assert_eq!(list[1].last_included_index, LogIndex(30));
    }

    #[test]
    fn file_corrupt_snapshot_skipped_on_rebuild() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("good", 10, 2), b"valid")
                .unwrap();
        }
        let snap_dir = dir.path().join("snapshots");
        std::fs::write(
            snap_dir.join("snapshot-0000000001-00000000000000000005.bin"),
            b"garbage data",
        )
        .unwrap();

        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].last_included_index, LogIndex(10));
        // Corrupt file at lower index must be deleted from disk.
        assert!(
            !snap_dir
                .join("snapshot-0000000001-00000000000000000005.bin")
                .exists(),
            "corrupt snapshot file must be cleaned up from disk"
        );
    }

    #[test]
    fn file_overwrite_same_index() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("v1", 10, 2), b"first")
            .unwrap();
        store
            .save_snapshot(test_meta("v2", 10, 2), b"second")
            .unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        drop(store);
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        let (_, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(data, b"second");
    }

    #[test]
    fn file_non_snapshot_files_ignored() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        std::fs::write(snap_dir.join("readme.txt"), b"not a snapshot").unwrap();
        std::fs::write(snap_dir.join("other.bin"), b"no snapshot prefix").unwrap();
        let store = FileSnapshotStore::open(dir.path()).unwrap();
        assert!(store.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn file_overwrite_same_index_different_term() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("old-term", 10, 2), b"term-2-data")
            .unwrap();
        let snap_dir = dir.path().join(SNAPSHOTS_SUBDIR);
        let count_files = || -> usize {
            std::fs::read_dir(&snap_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|ext| ext == SNAPSHOT_EXT))
                .count()
        };
        assert_eq!(count_files(), 1);

        store
            .save_snapshot(test_meta("new-term", 10, 5), b"term-5-data")
            .unwrap();

        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        // Canonical id, not caller-supplied.
        assert_eq!(list[0].id, "snapshot-0000000005-00000000000000000010");
        assert_eq!(list[0].last_included_term, Term(5));

        assert_eq!(
            count_files(),
            1,
            "stale file at same index but different term must be deleted"
        );

        // Verify after reopen: no stale snapshots reappear.
        drop(store);
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1, "no stale snapshots after reopen");
        assert_eq!(list[0].id, "snapshot-0000000005-00000000000000000010");
        let (_, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(data, b"term-5-data");
    }

    // -----------------------------------------------------------------------
    // Voter set and metadata roundtrip tests
    // -----------------------------------------------------------------------

    fn make_voter_set() -> VoterSet {
        VoterSet::try_new(vec![
            VoterRecord {
                node_id: NodeId(1),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("127.0.0.1", 9000)],
            },
            VoterRecord {
                node_id: NodeId(2),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("127.0.0.1", 9001)],
            },
        ])
        .unwrap()
    }

    #[test]
    fn encode_decode_roundtrip_with_voter_set() {
        let vs = make_voter_set();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            id: "vs-snap".to_string(),
            voter_set: Some(vs.clone()),
            size_bytes: None,
            checksum: None,
        };
        let data = b"payload with voters";
        let encoded = encode_snapshot(&meta, data).unwrap();
        let (decoded_meta, decoded_data) = decode_snapshot(&encoded).unwrap();
        assert_eq!(
            decoded_meta.id, "snapshot-0000000003-00000000000000000050",
            "id is canonical from term+index"
        );
        assert_eq!(decoded_meta.last_included_index, LogIndex(50));
        assert_eq!(decoded_meta.last_included_term, Term(3));
        assert_eq!(
            decoded_meta.voter_set.as_ref().unwrap().voters().len(),
            vs.voters().len()
        );
        assert_eq!(decoded_data, data);
    }

    #[test]
    fn file_voter_set_roundtrip_through_store() {
        let dir = temp_dir();
        let vs = make_voter_set();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(100),
            last_included_term: Term(5),
            id: "vs-roundtrip".to_string(),
            voter_set: Some(vs.clone()),
            size_bytes: None,
            checksum: None,
        };
        {
            let mut store = FileSnapshotStore::open(dir.path()).unwrap();
            store.save_snapshot(meta.clone(), b"voter-data").unwrap();
        }
        // Reopen and verify voter_set is preserved from file.
        let store = FileSnapshotStore::open(dir.path()).unwrap();
        let (loaded, data) = store.load_latest_snapshot().unwrap().unwrap();
        // Canonical id, not the original caller-supplied "vs-roundtrip".
        assert_eq!(loaded.id, "snapshot-0000000005-00000000000000000100");
        assert_eq!(
            loaded.voter_set.as_ref().unwrap().voters().len(),
            vs.voters().len(),
            "voter_set must roundtrip through file store"
        );
        assert_eq!(data, b"voter-data");

        let list = store.list_snapshots().unwrap();
        assert!(list[0].voter_set.is_some());
        assert_eq!(list[0].voter_set.as_ref().unwrap().voters().len(), 2);
    }

    // -----------------------------------------------------------------------
    // Chunked reader tests
    // -----------------------------------------------------------------------

    #[test]
    fn chunked_reader_basic() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();

        let payload = vec![0xABu8; 5 * 1024 * 1024];
        let meta = test_meta("snapshot-0000000001-00000000000000000100", 100, 1);
        store.save_snapshot(meta.clone(), &payload).unwrap();

        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store
            .chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE)
            .unwrap();
        assert_eq!(reader.chunk_count(), 5);

        let mut reassembled = Vec::new();
        let mut count = 0;
        for item_result in reader {
            let item = item_result.unwrap();
            if count == 0 {
                assert_eq!(item.chunk_index, 0);
                assert!(item.metadata.is_some(), "first chunk has metadata");
                let m = item.metadata.unwrap();
                assert_eq!(m.id, "snapshot-0000000001-00000000000000000100");
                assert_eq!(m.last_included_index, LogIndex(100));
            } else {
                assert!(item.metadata.is_none(), "non-first chunk has no metadata");
            }
            reassembled.extend_from_slice(&item.data);
            count += 1;
        }
        assert_eq!(count, 5);
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn chunked_reader_done_flag() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();

        let payload = vec![0xEEu8; 2 * 1024 * 1024];
        let meta = test_meta("snap-done", 50, 1);
        store.save_snapshot(meta.clone(), &payload).unwrap();

        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let items: Vec<_> = store
            .chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 2);
        assert!(!items[0].done, "first chunk is not done");
        assert!(items[1].done, "last chunk is done");
        assert_eq!(items[0].chunk_index, 0);
        assert_eq!(items[1].chunk_index, 1);
    }

    #[test]
    fn chunked_reader_partial_last_chunk() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();

        let payload = vec![0xCDu8; 2_500_000];
        let meta = test_meta("snapshot-0000000001-00000000000000000050", 50, 1);
        store.save_snapshot(meta.clone(), &payload).unwrap();

        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store
            .chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE)
            .unwrap();
        assert_eq!(reader.chunk_count(), 3);

        let chunks: Vec<SnapshotChunkItem> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].data.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks[1].data.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks[2].data.len(), 2_500_000 - 2 * DEFAULT_CHUNK_SIZE);
        assert!(chunks[2].done);
        assert!(!chunks[0].done);

        let reassembled: Vec<u8> = chunks.into_iter().flat_map(|c| c.data).collect();
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn chunked_reader_zero_chunk_size_uses_default() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let meta = test_meta("snapshot-0000000001-00000000000000000001", 1, 1);
        store.save_snapshot(meta.clone(), b"small").unwrap();
        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store.chunked_reader(&loaded_meta, 0).unwrap();
        assert_eq!(reader.chunk_count(), 1);
        let items: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 1);
        assert!(items[0].done);
        assert!(items[0].metadata.is_some());
    }

    #[test]
    fn chunked_reader_single_chunk() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let meta = test_meta("one-chunk", 1, 1);
        store.save_snapshot(meta.clone(), b"tiny").unwrap();

        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let items: Vec<_> = store
            .chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].chunk_index, 0);
        assert!(items[0].done);
        assert!(items[0].metadata.is_some());
        assert_eq!(items[0].data, b"tiny");
    }

    #[test]
    fn chunked_reader_does_not_load_full_payload() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xAAu8; 4 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("stream-test", 1, 1), &payload)
            .unwrap();
        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store
            .chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE)
            .unwrap();
        assert_eq!(reader.total_size(), 4 * DEFAULT_CHUNK_SIZE);
        assert_eq!(reader.chunk_count(), 4);

        let mut total = 0;
        for item_result in reader {
            total += item_result.unwrap().data.len();
        }
        assert_eq!(total, 4 * DEFAULT_CHUNK_SIZE);
    }

    // -----------------------------------------------------------------------
    // Crash recovery tests
    // -----------------------------------------------------------------------

    #[test]
    fn recover_bak_file_when_bin_missing() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Simulate crash after old->bak rename but before tmp->bin rename.
        let meta = test_meta("x", 10, 2);
        let blob = encode_snapshot(&meta, b"recovered-data").unwrap();
        std::fs::write(
            snap_dir.join("snapshot-0000000002-00000000000000000010.bin.bak"),
            &blob,
        )
        .unwrap();

        let store = FileSnapshotStore::open(dir.path()).unwrap();
        let (loaded, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(loaded.id, "snapshot-0000000002-00000000000000000010");
        assert_eq!(data, b"recovered-data");
    }

    #[test]
    fn recover_removes_stale_bak_when_bin_exists() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("good", 10, 2), b"current")
                .unwrap();
        }
        let snap_dir = dir.path().join("snapshots");
        std::fs::write(
            snap_dir.join("snapshot-0000000002-00000000000000000010.bin.bak"),
            b"stale backup",
        )
        .unwrap();

        let _store = FileSnapshotStore::open(dir.path()).unwrap();
        assert!(
            !snap_dir
                .join("snapshot-0000000002-00000000000000000010.bin.bak")
                .exists(),
            "stale bak should be cleaned up"
        );
    }

    #[test]
    fn recover_removes_orphaned_tmp_files() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        std::fs::write(
            snap_dir.join("snapshot-0000000001-00000000000000000005.bin.tmp"),
            b"orphaned temp",
        )
        .unwrap();

        let _store = FileSnapshotStore::open(dir.path()).unwrap();
        assert!(
            !snap_dir
                .join("snapshot-0000000001-00000000000000000005.bin.tmp")
                .exists(),
            "orphaned tmp should be cleaned up"
        );
    }

    // -----------------------------------------------------------------------
    // Filename parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_snapshot_filename_valid() {
        let path = Path::new("snapshot-0000000003-00000000000000000100.bin");
        let (term, index) = parse_snapshot_filename(path).unwrap();
        assert_eq!(term, Term(3));
        assert_eq!(index, LogIndex(100));
    }

    #[test]
    fn parse_snapshot_filename_invalid() {
        assert!(parse_snapshot_filename(Path::new("not-a-snapshot.bin")).is_none());
        assert!(parse_snapshot_filename(Path::new("snapshot-.bin")).is_none());
    }

    // -----------------------------------------------------------------------
    // Filename/header consistency & crash-window dedup tests
    // -----------------------------------------------------------------------

    #[test]
    fn file_header_filename_mismatch_skipped_with_valid_present() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Good snapshot at index 20.
        let good_meta = test_meta("x", 20, 3);
        let good_blob = encode_snapshot(&good_meta, b"good-data").unwrap();
        std::fs::write(
            snap_dir.join("snapshot-0000000003-00000000000000000020.bin"),
            &good_blob,
        )
        .unwrap();

        // Mismatched snapshot at index 10: filename says term=5 but header says term=2.
        let bad_meta = test_meta("x", 10, 2);
        let bad_blob = encode_snapshot(&bad_meta, b"data").unwrap();
        std::fs::write(
            snap_dir.join("snapshot-0000000005-00000000000000000010.bin"),
            &bad_blob,
        )
        .unwrap();

        // Mismatched file is skipped; valid snapshot is indexed.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1, "only valid snapshot indexed");
        assert_eq!(list[0].last_included_index, LogIndex(20));
        // Mismatched file must be cleaned up from disk.
        assert!(
            !snap_dir
                .join("snapshot-0000000005-00000000000000000010.bin")
                .exists(),
            "mismatched snapshot file must be deleted from disk"
        );
    }

    #[test]
    fn file_crash_window_same_index_dedup_on_reopen() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Simulate crash window: two valid files for the same index, different terms.
        let meta_old = test_meta("x", 10, 2);
        let blob_old = encode_snapshot(&meta_old, b"old").unwrap();
        std::fs::write(
            snap_dir.join("snapshot-0000000002-00000000000000000010.bin"),
            &blob_old,
        )
        .unwrap();

        let meta_new = test_meta("x", 10, 5);
        let blob_new = encode_snapshot(&meta_new, b"new").unwrap();
        std::fs::write(
            snap_dir.join("snapshot-0000000005-00000000000000000010.bin"),
            &blob_new,
        )
        .unwrap();

        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1, "only highest-term kept");
        assert_eq!(list[0].last_included_term, Term(5));
        assert_eq!(list[0].id, "snapshot-0000000005-00000000000000000010");

        // Stale file should have been cleaned up.
        assert!(
            !snap_dir
                .join("snapshot-0000000002-00000000000000000010.bin")
                .exists(),
            "stale same-index file must be deleted"
        );
    }

    #[test]
    fn file_delete_with_canonical_id_after_save() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("caller-supplied-id", 42, 7), b"data")
            .unwrap();
        // Delete using canonical id (the only id that works).
        store
            .delete_snapshot("snapshot-0000000007-00000000000000000042")
            .unwrap();
        assert!(store.list_snapshots().unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Corruption / integrity edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn chunked_reader_surfaces_read_error_on_truncated_file() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();

        let payload = vec![0xABu8; 3 * DEFAULT_CHUNK_SIZE];
        let meta = test_meta("snap-trunc", 100, 1);
        store.save_snapshot(meta.clone(), &payload).unwrap();

        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();

        // Truncate the file mid-payload to simulate corruption.
        let snap_dir = dir.path().join("snapshots");
        let snap_path = snap_dir.join("snapshot-0000000001-00000000000000000100.bin");
        let file_len = std::fs::metadata(&snap_path).unwrap().len();
        // Remove the last chunk-and-a-half worth of data.
        let truncated_len = file_len - (DEFAULT_CHUNK_SIZE as u64 + 100);
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&snap_path)
                .unwrap();
            f.set_len(truncated_len).unwrap();
        }

        // chunked_reader should detect the file size mismatch at open time.
        let result = store.chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE);
        assert!(
            result.is_err(),
            "chunked_reader must detect truncated file at construction"
        );
        // Use map_err to extract the error message without requiring Debug on Ok type.
        let err = result.err().unwrap();
        let err_msg = format!("{}", err);
        assert!(
            err_msg.contains("file size"),
            "error should mention file size, got: {err_msg}"
        );
    }

    #[test]
    fn chunk_item_converts_to_fetch_snapshot_chunk() {
        let meta = SnapshotMeta {
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            id: "snapshot-0000000003-00000000000000000050".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };
        let item = SnapshotChunkItem {
            chunk_index: 0,
            data: vec![0xAA; 100],
            done: false,
            metadata: Some(meta.clone()),
        };

        let fetch = item.into_fetch_chunk("cluster-1".to_string(), 42);
        assert_eq!(fetch.cluster_id, "cluster-1");
        assert_eq!(fetch.leader_epoch, 42);
        assert_eq!(fetch.chunk_index, 0);
        assert_eq!(fetch.data.len(), 100);
        assert!(!fetch.done);
        assert!(fetch.metadata.is_some());
        assert_eq!(fetch.metadata.unwrap().last_included_index, LogIndex(50));
    }

    #[test]
    fn chunk_item_converts_done_chunk_without_meta() {
        let item = SnapshotChunkItem {
            chunk_index: 5,
            data: vec![0xBB; 50],
            done: true,
            metadata: None,
        };

        let fetch = item.into_fetch_chunk("c2".to_string(), 99);
        assert_eq!(fetch.chunk_index, 5);
        assert!(fetch.done);
        assert!(fetch.metadata.is_none());
    }

    #[test]
    fn file_header_mismatch_index_skipped() {
        // Header says index=10 but filename says index=20.
        // The mismatched file is skipped (corruption policy: skip with warning).
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        let meta = test_meta("x", 10, 2); // index=10, term=2
        let blob = encode_snapshot(&meta, b"data").unwrap();
        // Filename claims index=20, term=2 — mismatch on index.
        std::fs::write(
            snap_dir.join("snapshot-0000000002-00000000000000000020.bin"),
            &blob,
        )
        .unwrap();

        // Single mismatched file is the latest, so open must error.
        let result = FileSnapshotStore::open_with_retention(dir.path(), 0);
        assert!(
            result.is_err(),
            "mismatched header index on latest must error"
        );
    }

    // -----------------------------------------------------------------------
    // Chunked reader metadata verification tests
    // -----------------------------------------------------------------------

    #[test]
    fn chunked_reader_rejects_wrong_caller_term() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();

        store
            .save_snapshot(test_meta("snap-a", 100, 5), b"payload-a")
            .unwrap();

        // Caller provides meta with wrong term — file won't exist at that path.
        let wrong_term_meta = SnapshotMeta {
            last_included_index: LogIndex(100),
            last_included_term: Term(999),
            id: "snapshot-0000000999-00000000000000000100".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };
        let result = store.chunked_reader(&wrong_term_meta, DEFAULT_CHUNK_SIZE);
        assert!(result.is_err(), "wrong term should fail (file not found)");
    }

    #[test]
    fn chunked_reader_uses_file_metadata_not_caller() {
        let dir = temp_dir();
        let vs = make_voter_set();
        let meta_with_vs = SnapshotMeta {
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            id: "any".to_string(),
            voter_set: Some(vs.clone()),
            size_bytes: None,
            checksum: None,
        };
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(meta_with_vs.clone(), b"vs-payload")
            .unwrap();

        // Caller provides meta WITHOUT voter_set but same term/index.
        let caller_meta = SnapshotMeta {
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            id: "snapshot-0000000003-00000000000000000050".to_string(),
            voter_set: None, // differs from file
            size_bytes: None,
            checksum: None,
        };

        let reader = store
            .chunked_reader(&caller_meta, DEFAULT_CHUNK_SIZE)
            .unwrap();
        let items: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 1);

        // The metadata in the chunk should come from the FILE, not
        // the caller — so voter_set should be present.
        let chunk_meta = items[0].metadata.as_ref().unwrap();
        assert!(
            chunk_meta.voter_set.is_some(),
            "chunk metadata must come from file, not caller"
        );
        assert_eq!(
            chunk_meta.voter_set.as_ref().unwrap().voters().len(),
            vs.voters().len()
        );
    }

    // -----------------------------------------------------------------------
    // Same-length payload corruption detection (CRC)
    // -----------------------------------------------------------------------

    #[test]
    fn decode_detects_same_length_payload_corruption() {
        let meta = test_meta_no_voter_set("crc-test", 10, 2);
        let data = b"original payload data here";
        let mut encoded = encode_snapshot(&meta, data).unwrap();

        // Corrupt a byte in the payload without changing file length.
        // With no voter_set, the payload starts after FIXED_HEADER(26) + vs_len(0) + data_len(8) = byte 34.
        let payload_offset = FIXED_HEADER_SIZE + 8;
        encoded[payload_offset] ^= 0xFF;

        let result = decode_snapshot(&encoded);
        assert!(result.is_err(), "same-length corruption must be detected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("CRC"),
            "error should mention CRC, got: {err_msg}"
        );
    }

    #[test]
    fn file_store_detects_same_length_payload_corruption_on_load() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("crc", 10, 2), b"important state data")
            .unwrap();
        drop(store);

        // Corrupt a payload byte on disk without changing file length.
        // Corrupt the byte just before the trailing CRC (last payload byte).
        let snap_dir = dir.path().join("snapshots");
        let path = snap_dir.join("snapshot-0000000002-00000000000000000010.bin");
        let mut raw = std::fs::read(&path).unwrap();
        let payload_end = raw.len() - 4; // 4 bytes of CRC at end
        raw[payload_end - 1] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        // read_meta_from_file now verifies CRC during indexing, so the
        // corrupt file is detected at open time. Since it's the only (and
        // therefore newest) snapshot, open must fail.
        let result = FileSnapshotStore::open_with_retention(dir.path(), 0);
        assert!(
            result.is_err(),
            "same-length corruption must be detected during rebuild/open"
        );
        let err_msg = format!("{}", result.err().unwrap());
        assert!(
            err_msg.contains("corrupt") || err_msg.contains("CRC"),
            "error should mention corruption or CRC, got: {err_msg}"
        );
    }

    #[test]
    fn same_length_crc_corruption_on_newest_fails_open() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("old", 10, 1), b"old-state")
                .unwrap();
            store
                .save_snapshot(test_meta("new", 20, 2), b"new-state-data-here")
                .unwrap();
        }

        // Corrupt a payload byte in the newest snapshot without changing
        // file length — same-length CRC corruption.
        let snap_dir = dir.path().join("snapshots");
        let path = snap_dir.join("snapshot-0000000002-00000000000000000020.bin");
        let mut raw = std::fs::read(&path).unwrap();
        let payload_end = raw.len() - 4; // 4 bytes of CRC at end
        raw[payload_end - 1] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        // rebuild_index now detects CRC corruption during indexing.
        // Since the newest snapshot (index 20) is corrupt and no valid
        // snapshot at index >= 20 exists, open must fail.
        let result = FileSnapshotStore::open_with_retention(dir.path(), 0);
        assert!(
            result.is_err(),
            "same-length CRC corruption on newest must fail open, not fall back"
        );
        let err_msg = format!("{}", result.err().unwrap());
        assert!(
            err_msg.contains("20"),
            "error should reference the corrupt snapshot's index (20), got: {err_msg}"
        );
    }

    #[test]
    fn same_length_crc_corruption_on_non_newest_skipped() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("old", 10, 1), b"old-state-payload")
                .unwrap();
            store
                .save_snapshot(test_meta("new", 20, 2), b"new-state")
                .unwrap();
        }

        // Corrupt a payload byte in the OLDER snapshot without changing
        // file length — same-length CRC corruption on non-newest.
        let snap_dir = dir.path().join("snapshots");
        let path = snap_dir.join("snapshot-0000000001-00000000000000000010.bin");
        let mut raw = std::fs::read(&path).unwrap();
        let payload_end = raw.len() - 4; // 4 bytes of CRC at end
        raw[payload_end - 1] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        // Since the corrupt snapshot is NOT the newest, it should be
        // silently skipped during rebuild. open succeeds.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1, "corrupt non-newest is skipped");
        assert_eq!(list[0].last_included_index, LogIndex(20));

        // list_snapshots must NOT advertise the corrupt snapshot.
        assert!(
            list.iter().all(|m| m.last_included_index != LogIndex(10)),
            "corrupt snapshot must not appear in list_snapshots"
        );

        // Corrupt file must be cleaned up from disk.
        assert!(
            !snap_dir
                .join("snapshot-0000000001-00000000000000000010.bin")
                .exists(),
            "corrupt non-newest snapshot file must be deleted from disk"
        );
    }

    #[test]
    fn chunked_reader_detects_same_length_corruption_via_crc() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xAAu8; 2 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("crc-chunk", 10, 2), &payload)
            .unwrap();

        // Corrupt a byte in the second chunk of the payload AFTER save
        // but while the store is still open (simulates on-disk bit-rot
        // between save and chunked read within the same session).
        let snap_dir = dir.path().join("snapshots");
        let path = snap_dir.join("snapshot-0000000002-00000000000000000010.bin");
        let mut raw = std::fs::read(&path).unwrap();
        // Corrupt a byte in the second half of the payload (second chunk).
        // CRC is last 4 bytes; payload is the 2*DEFAULT_CHUNK_SIZE bytes before that.
        let second_chunk_offset = raw.len() - 4 - DEFAULT_CHUNK_SIZE + 10;
        raw[second_chunk_offset] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        let loaded_meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store
            .chunked_reader(&loaded_meta, DEFAULT_CHUNK_SIZE)
            .unwrap();

        let items: Vec<_> = reader.collect();
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        assert!(
            items[1].is_err(),
            "CRC mismatch must be detected on final chunk"
        );
        let err_msg = format!("{}", items[1].as_ref().unwrap_err());
        assert!(
            err_msg.contains("CRC"),
            "error should mention CRC, got: {err_msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Corrupt-newest recovery semantics
    // -----------------------------------------------------------------------

    #[test]
    fn corrupt_newest_does_not_silently_fall_back_to_older() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("old", 10, 1), b"old-state")
                .unwrap();
            store
                .save_snapshot(test_meta("mid", 20, 2), b"mid-state")
                .unwrap();
            store
                .save_snapshot(test_meta("new", 30, 3), b"new-state")
                .unwrap();
        }

        // Corrupt only the newest snapshot (index 30).
        let snap_dir = dir.path().join("snapshots");
        std::fs::write(
            snap_dir.join("snapshot-0000000003-00000000000000000030.bin"),
            b"totally corrupt",
        )
        .unwrap();

        // Opening must error — NOT silently return the index-20 snapshot.
        let result = FileSnapshotStore::open_with_retention(dir.path(), 0);
        assert!(
            result.is_err(),
            "must error when newest snapshot is corrupt, not fall back"
        );
        let err_msg = format!("{}", result.err().unwrap());
        assert!(
            err_msg.contains("30"),
            "error should reference the corrupt snapshot's index, got: {err_msg}"
        );
    }

    #[test]
    fn corrupt_non_newest_is_silently_skipped() {
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("old", 10, 1), b"old-state")
                .unwrap();
            store
                .save_snapshot(test_meta("new", 20, 2), b"new-state")
                .unwrap();
        }

        // Corrupt the older snapshot (index 10).
        let snap_dir = dir.path().join("snapshots");
        std::fs::write(
            snap_dir.join("snapshot-0000000001-00000000000000000010.bin"),
            b"corrupted old",
        )
        .unwrap();

        // Opening succeeds — corrupt file at lower index is safely skipped.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].last_included_index, LogIndex(20));
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.last_included_index, LogIndex(20));
        assert_eq!(data, b"new-state");
        // Corrupt file must be cleaned up from disk.
        assert!(
            !snap_dir
                .join("snapshot-0000000001-00000000000000000010.bin")
                .exists(),
            "corrupt non-newest snapshot file must be deleted from disk"
        );
    }

    #[test]
    fn retention_does_not_prune_valid_when_newest_crc_corrupt() {
        // This test verifies the interaction between CRC verification and
        // retention: if the newest snapshot has same-length CRC corruption,
        // open must fail BEFORE pruning older valid snapshots.
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            store
                .save_snapshot(test_meta("a", 10, 1), b"state-a")
                .unwrap();
            store
                .save_snapshot(test_meta("b", 20, 2), b"state-b")
                .unwrap();
            store
                .save_snapshot(test_meta("c", 30, 3), b"state-c-payload")
                .unwrap();
        }

        // Corrupt the newest snapshot's payload (same-length CRC corruption).
        let snap_dir = dir.path().join("snapshots");
        let path = snap_dir.join("snapshot-0000000003-00000000000000000030.bin");
        let mut raw = std::fs::read(&path).unwrap();
        // Compute the real payload start offset from the file's voter_set_len.
        let po = payload_offset_in_raw(&raw);
        raw[po] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        // Open with retention=1: if CRC isn't checked during indexing,
        // the corrupt newest would be indexed, older valid ones pruned,
        // and then load_latest_snapshot would fail with no fallback.
        // With CRC checked during indexing, open must fail immediately.
        let result = FileSnapshotStore::open_with_retention(dir.path(), 1);
        assert!(
            result.is_err(),
            "must fail at open, not silently prune valid snapshots"
        );

        // Verify the older snapshots were NOT deleted (open failed before prune).
        assert!(
            snap_dir
                .join("snapshot-0000000001-00000000000000000010.bin")
                .exists(),
            "older valid snapshot must not be pruned when newest is corrupt"
        );
        assert!(
            snap_dir
                .join("snapshot-0000000002-00000000000000000020.bin")
                .exists(),
            "older valid snapshot must not be pruned when newest is corrupt"
        );
    }

    #[test]
    fn corrupt_higher_term_same_index_fails_open() {
        // Two snapshot files at the same index but different terms.
        // The higher-term file is corrupt. rebuild_index must detect
        // that the *highest* snapshot (by (index, term)) is corrupt
        // and refuse to fall back to the lower-term valid snapshot.
        let dir = temp_dir();
        {
            let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
            // Save at term=1, index=10
            store
                .save_snapshot(test_meta("a", 10, 1), b"state-a")
                .unwrap();
        }

        // Manually write a second file at term=2, index=10 (higher term).
        let snap_dir = dir.path().join("snapshots");
        let higher_term_path = snap_dir.join("snapshot-0000000002-00000000000000000010.bin");
        // Write garbage so it fails header validation.
        std::fs::write(&higher_term_path, b"not a valid snapshot file").unwrap();

        // Open must fail: the highest (index=10, term=2) file is corrupt,
        // even though (index=10, term=1) is valid.
        let result = FileSnapshotStore::open(dir.path());
        assert!(
            result.is_err(),
            "must fail when highest-term snapshot at same index is corrupt"
        );
        let msg = format!("{}", result.err().unwrap());
        assert!(
            msg.contains("corrupt") || msg.contains("mismatch"),
            "error message should mention corruption: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Non-canonical filename renaming tests
    // -----------------------------------------------------------------------

    #[test]
    fn non_canonical_filename_renamed_on_open() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Write a valid snapshot with a non-canonical (unpadded) filename.
        let meta = test_meta("x", 10, 2);
        let blob = encode_snapshot(&meta, b"non-canonical-data").unwrap();
        let unpadded = snap_dir.join("snapshot-2-10.bin");
        let canonical = snap_dir.join("snapshot-0000000002-00000000000000000010.bin");
        std::fs::write(&unpadded, &blob).unwrap();

        // Open: the file should be renamed to canonical form and indexed.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].last_included_index, LogIndex(10));
        assert_eq!(list[0].last_included_term, Term(2));

        // Unpadded file is gone, canonical file exists.
        assert!(
            !unpadded.exists(),
            "non-canonical file should have been renamed"
        );
        assert!(
            canonical.exists(),
            "canonical file should exist after rename"
        );

        // Load works via the canonical path.
        let (loaded_meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(loaded_meta.last_included_index, LogIndex(10));
        assert_eq!(data, b"non-canonical-data");
    }

    #[test]
    fn non_canonical_duplicate_removed_when_canonical_exists() {
        let dir = temp_dir();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Write a valid snapshot at canonical path.
        let meta = test_meta("x", 10, 2);
        let blob = encode_snapshot(&meta, b"canonical-data").unwrap();
        let canonical = snap_dir.join("snapshot-0000000002-00000000000000000010.bin");
        std::fs::write(&canonical, &blob).unwrap();

        // Also write the same snapshot with a non-canonical filename.
        let unpadded = snap_dir.join("snapshot-2-10.bin");
        std::fs::write(&unpadded, &blob).unwrap();

        // Open should succeed — non-canonical duplicate is removed.
        let store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 1);
        assert!(
            !unpadded.exists(),
            "non-canonical duplicate should be removed"
        );
        assert!(canonical.exists(), "canonical file should remain");
    }

    // -----------------------------------------------------------------------
    // SnapshotMeta size_bytes/checksum tests
    // -----------------------------------------------------------------------

    #[test]
    fn snapshot_meta_has_size_and_checksum_after_save_load() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = b"payload-for-size-check";
        store
            .save_snapshot(test_meta("s1", 10, 2), payload)
            .unwrap();

        let (meta, _data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(
            meta.size_bytes,
            Some(payload.len() as u64),
            "size_bytes must be populated on load"
        );
        assert!(
            meta.checksum.is_some(),
            "checksum must be populated on load"
        );
        // Verify the checksum matches CRC32 of the payload.
        let expected_crc = crc32fast::hash(payload) as u64;
        assert_eq!(meta.checksum, Some(expected_crc));
    }

    #[test]
    fn list_snapshots_includes_size_and_checksum() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("s1", 10, 2), b"data-a")
            .unwrap();
        store
            .save_snapshot(test_meta("s2", 20, 3), b"data-bb")
            .unwrap();

        let list = store.list_snapshots().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].size_bytes, Some(7)); // "data-bb"
        assert_eq!(list[1].size_bytes, Some(6)); // "data-a"
        assert!(list[0].checksum.is_some());
        assert!(list[1].checksum.is_some());
    }

    #[test]
    fn snapshot_exists_trait_method() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        assert!(!store.snapshot_exists(LogIndex(10), Term(2)));

        store
            .save_snapshot(test_meta("s1", 10, 2), b"data")
            .unwrap();
        assert!(store.snapshot_exists(LogIndex(10), Term(2)));
        assert!(!store.snapshot_exists(LogIndex(10), Term(3)));
        assert!(!store.snapshot_exists(LogIndex(20), Term(2)));
    }

    #[test]
    fn memory_snapshot_exists_trait_method() {
        let mut store = MemorySnapshotStore::new();
        assert!(!store.snapshot_exists(LogIndex(10), Term(2)));

        store
            .save_snapshot(test_meta("s1", 10, 2), b"data")
            .unwrap();
        assert!(store.snapshot_exists(LogIndex(10), Term(2)));
        assert!(!store.snapshot_exists(LogIndex(10), Term(3)));
    }

    // -----------------------------------------------------------------------
    // Lower-term rejection tests
    // -----------------------------------------------------------------------

    #[test]
    fn memory_rejects_lower_term_at_same_index() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("high", 10, 5), b"high-term")
            .unwrap();
        let result = store.save_snapshot(test_meta("low", 10, 3), b"low-term");
        assert!(result.is_err(), "lower-term save must be rejected");
        // Original snapshot remains.
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.last_included_term, Term(5));
        assert_eq!(data, b"high-term");
    }

    #[test]
    fn file_rejects_lower_term_at_same_index() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("high", 10, 5), b"high-term")
            .unwrap();
        let result = store.save_snapshot(test_meta("low", 10, 3), b"low-term");
        assert!(result.is_err(), "lower-term save must be rejected");
        let (meta, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(meta.last_included_term, Term(5));
        assert_eq!(data, b"high-term");
    }

    // -----------------------------------------------------------------------
    // delete_snapshot error on unknown id tests
    // -----------------------------------------------------------------------

    #[test]
    fn memory_delete_unknown_id_errors() {
        let mut store = MemorySnapshotStore::new();
        let result = store.delete_snapshot("nonexistent-id");
        assert!(result.is_err(), "delete of unknown id must error");
    }

    #[test]
    fn file_delete_unknown_id_errors() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let result = store.delete_snapshot("nonexistent-id");
        assert!(result.is_err(), "delete of unknown id must error");
    }

    // -----------------------------------------------------------------------
    // Trait-level snapshot_reader tests
    // -----------------------------------------------------------------------

    #[test]
    fn memory_snapshot_reader_via_trait() {
        let mut store = MemorySnapshotStore::new();
        let payload = vec![0xBBu8; 3 * 1024 * 1024];
        store
            .save_snapshot(test_meta("reader-test", 50, 3), &payload)
            .unwrap();

        let meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store.snapshot_reader(&meta, 1024 * 1024).unwrap();

        let mut reassembled = Vec::new();
        let mut count = 0;
        for item_result in reader {
            let item = item_result.unwrap();
            if count == 0 {
                assert!(item.metadata.is_some(), "first chunk has metadata");
            } else {
                assert!(item.metadata.is_none());
            }
            reassembled.extend_from_slice(&item.data);
            count += 1;
        }
        assert_eq!(count, 3);
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn file_snapshot_reader_via_trait() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xCCu8; 2 * 1024 * 1024 + 100];
        store
            .save_snapshot(test_meta("reader-file", 100, 5), &payload)
            .unwrap();

        let meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let reader = store.snapshot_reader(&meta, 1024 * 1024).unwrap();

        let mut reassembled = Vec::new();
        let mut chunk_count = 0;
        let mut last_done = false;
        for item_result in reader {
            let item = item_result.unwrap();
            last_done = item.done;
            reassembled.extend_from_slice(&item.data);
            chunk_count += 1;
        }
        assert_eq!(chunk_count, 3);
        assert!(last_done);
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn memory_snapshot_reader_empty_payload() {
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("empty", 1, 1), b"").unwrap();

        let meta = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let items: Vec<_> = store
            .snapshot_reader(&meta, 1024 * 1024)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 1);
        assert!(items[0].done);
        assert!(items[0].data.is_empty());
        assert!(items[0].metadata.is_some());
    }

    // -----------------------------------------------------------------------
    // Memory store snapshot_reader on non-latest snapshots
    // -----------------------------------------------------------------------

    #[test]
    fn memory_snapshot_reader_on_non_latest_snapshot() {
        // MemorySnapshotStore overrides load_snapshot for direct lookup,
        // so snapshot_reader (via the trait default) should work on any
        // retained snapshot, not just the latest.
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("old", 10, 1), b"old-payload")
            .unwrap();
        store
            .save_snapshot(test_meta("mid", 20, 2), b"mid-payload")
            .unwrap();
        store
            .save_snapshot(test_meta("new", 30, 3), b"new-payload")
            .unwrap();

        // Read the oldest snapshot (not latest) via snapshot_reader.
        let oldest_meta = store
            .list_snapshots()
            .unwrap()
            .into_iter()
            .find(|m| m.last_included_index == LogIndex(10))
            .unwrap();
        let items: Vec<_> = store
            .snapshot_reader(&oldest_meta, 1024 * 1024)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 1);
        assert!(items[0].done);
        assert_eq!(items[0].data, b"old-payload");
        assert!(items[0].metadata.is_some());

        // Read the middle snapshot too.
        let mid_meta = store
            .list_snapshots()
            .unwrap()
            .into_iter()
            .find(|m| m.last_included_index == LogIndex(20))
            .unwrap();
        let mid_items: Vec<_> = store
            .snapshot_reader(&mid_meta, 1024 * 1024)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(mid_items[0].data, b"mid-payload");
    }

    #[test]
    fn file_snapshot_reader_on_non_latest_snapshot() {
        // FileSnapshotStore overrides both load_snapshot and snapshot_reader
        // for efficient file streaming on any retained snapshot.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("old", 10, 1), b"old-file-payload")
            .unwrap();
        store
            .save_snapshot(test_meta("new", 20, 2), b"new-file-payload")
            .unwrap();

        // Read the older snapshot via the trait's snapshot_reader.
        let oldest = store
            .list_snapshots()
            .unwrap()
            .into_iter()
            .find(|m| m.last_included_index == LogIndex(10))
            .unwrap();
        let items: Vec<_> = store
            .snapshot_reader(&oldest, DEFAULT_CHUNK_SIZE)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].data, b"old-file-payload");
        assert!(items[0].done);
        assert!(items[0].metadata.is_some());
        assert_eq!(
            items[0].metadata.as_ref().unwrap().last_included_index,
            LogIndex(10)
        );
    }

    // -----------------------------------------------------------------------
    // voter_set_required() helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn voter_set_required_returns_voter_set_when_present() {
        let vs = make_voter_set();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(10),
            last_included_term: Term(2),
            id: "test".to_string(),
            voter_set: Some(vs.clone()),
            size_bytes: None,
            checksum: None,
        };
        let result = meta.voter_set_required();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().voters().len(), vs.voters().len());
    }

    #[test]
    fn voter_set_required_errors_when_none() {
        let meta = SnapshotMeta {
            last_included_index: LogIndex(10),
            last_included_term: Term(2),
            id: "snapshot-0000000002-00000000000000000010".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };
        let result = meta.voter_set_required();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("missing required voter_set"),
            "error should explain missing voter_set, got: {err_msg}"
        );
    }

    // -----------------------------------------------------------------------
    // FetchSnapshot chunk conversion integration tests
    // -----------------------------------------------------------------------

    #[test]
    fn chunked_reader_first_chunk_has_metadata_rest_do_not() {
        let dir = temp_dir();
        let vs = make_voter_set();
        let meta_with_vs = SnapshotMeta {
            last_included_index: LogIndex(100),
            last_included_term: Term(5),
            id: "snap-fetch-test".to_string(),
            voter_set: Some(vs),
            size_bytes: None,
            checksum: None,
        };
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xBBu8; 3 * DEFAULT_CHUNK_SIZE];
        store.save_snapshot(meta_with_vs, &payload).unwrap();

        let loaded = store.list_snapshots().unwrap().into_iter().next().unwrap();
        let chunks: Vec<_> = store
            .chunked_reader(&loaded, DEFAULT_CHUNK_SIZE)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(chunks.len(), 3);

        // First chunk: metadata present with voter_set.
        assert!(chunks[0].metadata.is_some());
        let first_meta = chunks[0].metadata.as_ref().unwrap();
        assert_eq!(first_meta.last_included_index, LogIndex(100));
        assert!(first_meta.voter_set.is_some());
        assert!(!chunks[0].done);

        // Middle chunks: no metadata.
        assert!(chunks[1].metadata.is_none());
        assert!(!chunks[1].done);

        // Last chunk: no metadata, done=true.
        assert!(chunks[2].metadata.is_none());
        assert!(chunks[2].done);

        // Convert to FetchSnapshotChunk messages and verify wire readiness.
        let fetch_chunks: Vec<_> = chunks
            .into_iter()
            .map(|c| c.into_fetch_chunk("test-cluster".to_string(), 42))
            .collect();
        assert!(fetch_chunks[0].metadata.is_some());
        assert!(fetch_chunks[1].metadata.is_none());
        assert!(fetch_chunks[2].metadata.is_none());
        assert_eq!(fetch_chunks[0].cluster_id, "test-cluster");
        assert_eq!(fetch_chunks[2].leader_epoch, 42);
        assert!(fetch_chunks[2].done);
    }

    // -----------------------------------------------------------------------
    // Snapshot save with voter_set roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn memory_store_preserves_voter_set_on_save_and_load() {
        let mut store = MemorySnapshotStore::new();
        let vs = make_voter_set();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(50),
            last_included_term: Term(3),
            id: "vs-memory-test".to_string(),
            voter_set: Some(vs.clone()),
            size_bytes: None,
            checksum: None,
        };
        store.save_snapshot(meta, b"data-with-vs").unwrap();
        let (loaded, data) = store.load_latest_snapshot().unwrap().unwrap();
        assert_eq!(data, b"data-with-vs");
        assert!(loaded.voter_set.is_some());
        assert_eq!(
            loaded.voter_set.as_ref().unwrap().voters().len(),
            vs.voters().len()
        );
        // voter_set_required succeeds.
        assert!(loaded.voter_set_required().is_ok());
    }

    // -----------------------------------------------------------------------
    // find_by_id and FetchSnapshotRequest bridging tests
    // -----------------------------------------------------------------------

    #[test]
    fn memory_find_by_id_returns_matching_meta() {
        let mut store = MemorySnapshotStore::new();
        store
            .save_snapshot(test_meta("x", 10, 2), b"data1")
            .unwrap();
        store
            .save_snapshot(test_meta("y", 20, 3), b"data2")
            .unwrap();

        let found = store
            .find_by_id("snapshot-0000000002-00000000000000000010")
            .unwrap();
        assert!(found.is_some());
        let meta = found.unwrap();
        assert_eq!(meta.last_included_index, LogIndex(10));
        assert_eq!(meta.last_included_term, Term(2));
    }

    #[test]
    fn memory_find_by_id_returns_none_for_unknown() {
        let store = MemorySnapshotStore::new();
        assert!(store.find_by_id("nonexistent-id").unwrap().is_none());
    }

    #[test]
    fn file_find_by_id_returns_matching_meta() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("x", 42, 7), b"payload")
            .unwrap();

        let found = store
            .find_by_id("snapshot-0000000007-00000000000000000042")
            .unwrap();
        assert!(found.is_some());
        let meta = found.unwrap();
        assert_eq!(meta.last_included_index, LogIndex(42));
        assert_eq!(meta.last_included_term, Term(7));
    }

    #[test]
    fn file_find_by_id_returns_none_for_unknown() {
        let dir = temp_dir();
        let store = FileSnapshotStore::open(dir.path()).unwrap();
        assert!(
            store
                .find_by_id("snapshot-0000000099-00000000000000009999")
                .unwrap()
                .is_none()
        );
    }

    /// Demonstrates the full FetchSnapshotRequest → find_by_id → snapshot_reader
    /// bridging pattern that the RPC layer will use.
    #[test]
    fn fetch_snapshot_request_to_reader_bridge() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xCCu8; 2 * DEFAULT_CHUNK_SIZE + 100];
        store
            .save_snapshot(test_meta("any-caller-id", 50, 3), &payload)
            .unwrap();

        // Simulate what the RPC handler does when it receives a FetchSnapshotRequest:
        // 1. Look up the snapshot by the request's snapshot_id.
        let snapshot_id = "snapshot-0000000003-00000000000000000050";
        let meta = store
            .find_by_id(snapshot_id)
            .unwrap()
            .expect("snapshot must exist");

        // 2. Open a chunked reader for streaming.
        let chunks: Vec<_> = store
            .snapshot_reader(&meta, DEFAULT_CHUNK_SIZE)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 3);
        assert!(chunks[0].metadata.is_some());
        assert_eq!(
            chunks[0].metadata.as_ref().unwrap().last_included_index,
            LogIndex(50)
        );
        assert!(chunks[2].done);

        // 3. Convert to FetchSnapshotChunk wire messages.
        let wire_chunks: Vec<_> = chunks
            .into_iter()
            .map(|c| c.into_fetch_chunk("cluster-1".to_string(), 99))
            .collect();
        assert_eq!(wire_chunks.len(), 3);
        assert_eq!(wire_chunks[0].cluster_id, "cluster-1");
        assert_eq!(wire_chunks[0].leader_epoch, 99);

        // Reassemble and verify payload integrity.
        let reassembled: Vec<u8> = wire_chunks.into_iter().flat_map(|c| c.data).collect();
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn offset_reader_reads_from_middle_of_payload() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        // 3 chunks: 2 full + 100 bytes partial
        let payload = vec![0xABu8; 2 * DEFAULT_CHUNK_SIZE + 100];
        store
            .save_snapshot(test_meta("x", 50, 3), &payload)
            .unwrap();

        let meta = store
            .find_by_id("snapshot-0000000003-00000000000000000050")
            .unwrap()
            .unwrap();

        // Read starting from byte offset = DEFAULT_CHUNK_SIZE (skip first chunk).
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE as u64, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        // Should yield 2 chunks: one full + one partial
        assert_eq!(chunks.len(), 2);
        // chunk_index should reflect the offset position
        assert_eq!(chunks[0].chunk_index, 1);
        assert_eq!(chunks[1].chunk_index, 2);
        assert!(chunks[1].done);
        // First yielded chunk carries metadata
        assert!(chunks[0].metadata.is_some());

        // Reassemble and verify
        let reassembled: Vec<u8> = chunks.into_iter().flat_map(|c| c.data).collect();
        assert_eq!(reassembled, payload[DEFAULT_CHUNK_SIZE..]);
    }

    #[test]
    fn offset_reader_with_max_bytes_limits_output() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xCDu8; 3 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("x", 100, 5), &payload)
            .unwrap();

        let meta = store
            .find_by_id("snapshot-0000000005-00000000000000000100")
            .unwrap()
            .unwrap();

        // Read 1 chunk's worth from offset 0
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(
                &meta,
                DEFAULT_CHUNK_SIZE,
                0,
                Some(DEFAULT_CHUNK_SIZE as u64),
            )
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        // Not done because there's more data beyond our window
        assert!(!chunks[0].done);
        assert_eq!(chunks[0].data.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks[0].data, payload[..DEFAULT_CHUNK_SIZE]);
    }

    #[test]
    fn offset_reader_past_end_returns_empty() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xEFu8; 100];
        store
            .save_snapshot(test_meta("x", 10, 1), &payload)
            .unwrap();

        let meta = store
            .find_by_id("snapshot-0000000001-00000000000000000010")
            .unwrap()
            .unwrap();

        // offset = 200 > payload len 100
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, 200, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(
            chunks.len(),
            1,
            "offset past end returns a single done chunk"
        );
        assert!(chunks[0].done, "chunk must be done");
        assert!(chunks[0].data.is_empty(), "chunk data must be empty");
        assert!(chunks[0].metadata.is_some(), "chunk must carry metadata");
    }

    #[test]
    fn offset_reader_zero_offset_matches_full_reader() {
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0x42u8; DEFAULT_CHUNK_SIZE + 50];
        store
            .save_snapshot(test_meta("x", 20, 2), &payload)
            .unwrap();

        let meta = store
            .find_by_id("snapshot-0000000002-00000000000000000020")
            .unwrap()
            .unwrap();

        // offset=0, no max_bytes => should match full reader
        let full: Vec<u8> = store
            .snapshot_reader(&meta, DEFAULT_CHUNK_SIZE)
            .unwrap()
            .flat_map(|r| r.unwrap().data)
            .collect();

        let from_offset: Vec<u8> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, 0, None)
            .unwrap()
            .flat_map(|r| r.unwrap().data)
            .collect();

        assert_eq!(full, from_offset);
        assert_eq!(full, payload);
    }

    #[test]
    fn resumable_transfer_reassembles_correctly() {
        // Simulate a full resumable transfer: read in 3 sequential requests
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload: Vec<u8> = (0..250u8)
            .cycle()
            .take(3 * DEFAULT_CHUNK_SIZE + 77)
            .collect();
        store
            .save_snapshot(test_meta("x", 200, 4), &payload)
            .unwrap();

        let meta = store
            .find_by_id("snapshot-0000000004-00000000000000000200")
            .unwrap()
            .unwrap();

        let chunk_size = DEFAULT_CHUNK_SIZE;
        let mut reassembled = Vec::new();
        let mut offset = 0u64;

        loop {
            let chunks: Vec<_> = store
                .snapshot_reader_from_offset(&meta, chunk_size, offset, Some(chunk_size as u64))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();

            if chunks.is_empty() {
                break;
            }

            for chunk in &chunks {
                reassembled.extend_from_slice(&chunk.data);
                offset += chunk.data.len() as u64;
            }

            if chunks.last().is_some_and(|c| c.done) {
                break;
            }
        }

        assert_eq!(reassembled, payload);
    }

    // -----------------------------------------------------------------------
    // Resumable transfer (offset/max_bytes) correctness tests
    // -----------------------------------------------------------------------

    #[test]
    fn offset_reader_done_false_when_window_before_eof() {
        // When max_bytes limits the window so it doesn't reach EOF,
        // `done` must be false on every yielded chunk.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xAAu8; 3 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("x", 50, 3), &payload)
            .unwrap();
        let meta = store
            .find_by_id("snapshot-0000000003-00000000000000000050")
            .unwrap()
            .unwrap();

        // Read only the first chunk (offset=0, max_bytes=DEFAULT_CHUNK_SIZE).
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(
                &meta,
                DEFAULT_CHUNK_SIZE,
                0,
                Some(DEFAULT_CHUNK_SIZE as u64),
            )
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert!(
            !chunks[0].done,
            "done must be false: window doesn't reach EOF"
        );

        // Read the middle chunk only.
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(
                &meta,
                DEFAULT_CHUNK_SIZE,
                DEFAULT_CHUNK_SIZE as u64,
                Some(DEFAULT_CHUNK_SIZE as u64),
            )
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert!(
            !chunks[0].done,
            "done must be false: middle window doesn't reach EOF"
        );
    }

    #[test]
    fn offset_reader_done_true_when_window_reaches_eof() {
        // When the window extends to the end of the payload, the last
        // chunk's `done` must be true.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xBBu8; 2 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("x", 60, 4), &payload)
            .unwrap();
        let meta = store
            .find_by_id("snapshot-0000000004-00000000000000000060")
            .unwrap()
            .unwrap();

        // Read from offset=DEFAULT_CHUNK_SIZE with no max_bytes — reaches EOF.
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE as u64, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].done, "done must be true: window reaches EOF");
    }

    #[test]
    fn offset_reader_metadata_on_first_yielded_chunk_when_offset_nonzero() {
        // The first yielded chunk must carry metadata even when offset > 0,
        // so the receiver can verify the snapshot identity on resume.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xCCu8; 3 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("x", 70, 5), &payload)
            .unwrap();
        let meta = store
            .find_by_id("snapshot-0000000005-00000000000000000070")
            .unwrap()
            .unwrap();

        // Read starting from the second chunk.
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE as u64, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 2);
        // First yielded chunk (chunk_index=1) must still have metadata.
        assert!(
            chunks[0].metadata.is_some(),
            "first yielded chunk must carry metadata even with offset > 0"
        );
        assert_eq!(chunks[0].chunk_index, 1);
        let m = chunks[0].metadata.as_ref().unwrap();
        assert_eq!(m.last_included_index, LogIndex(70));
        assert_eq!(m.last_included_term, Term(5));
        // Second chunk has no metadata.
        assert!(chunks[1].metadata.is_none());
    }

    #[test]
    fn offset_reader_partial_window_small_payload() {
        // Small payload with partial window reads.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = b"abcdefghij"; // 10 bytes
        store.save_snapshot(test_meta("x", 10, 1), payload).unwrap();
        let meta = store
            .find_by_id("snapshot-0000000001-00000000000000000010")
            .unwrap()
            .unwrap();

        // Read 5 bytes from offset 3 with chunk_size 4.
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, 4, 3, Some(5))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        // Should yield 2 chunks: 4 bytes + 1 byte
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data, b"defg");
        assert_eq!(chunks[1].data, b"h");
        // Window ends at offset 3+5=8, payload is 10 bytes, so not at EOF.
        assert!(!chunks[0].done);
        assert!(!chunks[1].done, "window doesn't reach EOF");
        // First chunk carries metadata.
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn offset_reader_partial_window_reaching_eof() {
        // Window that starts mid-payload and reaches EOF.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = b"abcdefghij"; // 10 bytes
        store.save_snapshot(test_meta("x", 10, 1), payload).unwrap();
        let meta = store
            .find_by_id("snapshot-0000000001-00000000000000000010")
            .unwrap()
            .unwrap();

        // Read from offset 7 with max_bytes=100 (more than remaining).
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, 4, 7, Some(100))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, b"hij");
        assert!(chunks[0].done, "window reaches EOF, done must be true");
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn memory_offset_reader_done_false_when_window_before_eof() {
        // Same test on MemorySnapshotStore (uses the default trait impl).
        let mut store = MemorySnapshotStore::new();
        let payload = vec![0xAAu8; 3000];
        store
            .save_snapshot(test_meta("x", 50, 3), &payload)
            .unwrap();
        let meta = store.list_snapshots().unwrap().into_iter().next().unwrap();

        // Read 1000 bytes from offset 0 with chunk_size 1000.
        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, 1000, 0, Some(1000))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert!(
            !chunks[0].done,
            "done must be false: window doesn't reach EOF"
        );
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn memory_offset_reader_metadata_on_resumed_chunk() {
        // Trait default: first yielded chunk carries metadata even at offset > 0.
        let mut store = MemorySnapshotStore::new();
        let payload = vec![0xBBu8; 3000];
        store
            .save_snapshot(test_meta("x", 50, 3), &payload)
            .unwrap();
        let meta = store.list_snapshots().unwrap().into_iter().next().unwrap();

        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, 1000, 1000, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 2);
        assert!(
            chunks[0].metadata.is_some(),
            "first yielded chunk must carry metadata on resume"
        );
        assert_eq!(chunks[0].chunk_index, 1);
        assert!(chunks[1].metadata.is_none());
        assert!(chunks[1].done, "last chunk reaches EOF");
    }

    #[test]
    fn memory_offset_reader_empty_snapshot_offset_nonzero() {
        // Empty snapshot with offset > 0 should return a single done chunk with metadata.
        let mut store = MemorySnapshotStore::new();
        store.save_snapshot(test_meta("x", 1, 1), b"").unwrap();
        let meta = store.list_snapshots().unwrap().into_iter().next().unwrap();

        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, 1000, 1, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(
            chunks.len(),
            1,
            "empty snapshot offset>0 => single done chunk"
        );
        assert!(chunks[0].done);
        assert!(chunks[0].data.is_empty());
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn file_offset_reader_empty_snapshot_offset_nonzero() {
        // FileSnapshotStore: empty snapshot with offset > 0 returns done chunk.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store.save_snapshot(test_meta("x", 1, 1), b"").unwrap();
        let meta = store
            .find_by_id("snapshot-0000000001-00000000000000000001")
            .unwrap()
            .unwrap();

        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, 1, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(
            chunks.len(),
            1,
            "empty snapshot offset>0 => single done chunk"
        );
        assert!(chunks[0].done);
        assert!(chunks[0].data.is_empty());
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn offset_reader_max_bytes_zero_reads_all() {
        // max_bytes=Some(0) is treated as "read until end".
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = vec![0xDDu8; 2 * DEFAULT_CHUNK_SIZE];
        store
            .save_snapshot(test_meta("x", 80, 6), &payload)
            .unwrap();
        let meta = store
            .find_by_id("snapshot-0000000006-00000000000000000080")
            .unwrap()
            .unwrap();

        let total: usize = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, 0, Some(0))
            .unwrap()
            .map(|r| r.unwrap().data.len())
            .sum();

        assert_eq!(total, payload.len());
    }

    #[test]
    fn offset_reader_huge_u64_offset_no_panic() {
        // u64::MAX offset should not panic (tests safe narrowing).
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        store
            .save_snapshot(test_meta("x", 10, 1), b"small payload")
            .unwrap();
        let meta = store
            .find_by_id("snapshot-0000000001-00000000000000000010")
            .unwrap()
            .unwrap();

        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, u64::MAX, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].done);
        assert!(chunks[0].data.is_empty());
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn offset_reader_offset_at_exact_end() {
        // offset == data_len should return a single empty done chunk.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload = b"exactly ten";
        store.save_snapshot(test_meta("x", 10, 1), payload).unwrap();
        let meta = store
            .find_by_id("snapshot-0000000001-00000000000000000010")
            .unwrap()
            .unwrap();

        let chunks: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, payload.len() as u64, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].done);
        assert!(chunks[0].data.is_empty());
        assert!(chunks[0].metadata.is_some());
    }

    #[test]
    fn resumable_transfer_done_only_on_final_request() {
        // Full resumable transfer: verify done is false on intermediate
        // requests and true only on the final request.
        let dir = temp_dir();
        let mut store = FileSnapshotStore::open_with_retention(dir.path(), 0).unwrap();
        let payload: Vec<u8> = (0..250u8).cycle().take(3 * DEFAULT_CHUNK_SIZE).collect();
        store
            .save_snapshot(test_meta("x", 300, 7), &payload)
            .unwrap();
        let meta = store
            .find_by_id("snapshot-0000000007-00000000000000000300")
            .unwrap()
            .unwrap();

        let cs = DEFAULT_CHUNK_SIZE as u64;

        // Request 1: offset=0, max_bytes=cs
        let c1: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, 0, Some(cs))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(!c1.last().unwrap().done, "request 1: not at EOF");
        assert!(c1[0].metadata.is_some(), "request 1: has metadata");

        // Request 2: offset=cs, max_bytes=cs
        let c2: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, cs, Some(cs))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(!c2.last().unwrap().done, "request 2: not at EOF");
        assert!(
            c2[0].metadata.is_some(),
            "request 2: has metadata on resume"
        );

        // Request 3: offset=2*cs, max_bytes=cs — reaches EOF
        let c3: Vec<_> = store
            .snapshot_reader_from_offset(&meta, DEFAULT_CHUNK_SIZE, 2 * cs, Some(cs))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(c3.last().unwrap().done, "request 3: at EOF, done=true");
        assert!(
            c3[0].metadata.is_some(),
            "request 3: has metadata on resume"
        );

        // Reassemble and verify.
        let mut reassembled = Vec::new();
        for chunks in [&c1, &c2, &c3] {
            for c in chunks {
                reassembled.extend_from_slice(&c.data);
            }
        }
        assert_eq!(reassembled, payload);
    }
}
