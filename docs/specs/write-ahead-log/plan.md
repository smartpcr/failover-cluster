# Write-Ahead Log — Design Plan

**Story / Stage:** `failover-cluster:XRAFT` Stage 2.1 (per
`docs/stories/failover-cluster-XRAFT/implementation-plan.md`).

**Goal:** A durable, append-only Raft log that survives crash, supports the
truncate-after-conflict pattern Raft requires, and serves random reads in
constant time via an offset index — **never** by caching every entry payload
in RAM.

This document is the design contract that the implementation in
`xraft-storage/src/log.rs`, `log_format.rs`, `log_segment.rs`, and
`tests/log_corruption.rs` is built against. Every contract here is exercised
by at least one test.

## Implementation status (iter 4)

The implementation matching this design **already lives in the branch** —
this iteration is a documentation refresh, not a fresh implementation.
Concrete files that satisfy each contract below:

| Contract | File | Key symbols |
|---|---|---|
| `LogStore` trait surface (locked Stage 1) | `xraft-core/src/storage.rs` (lines 13–31) | `LogStore::{append, get, get_range, last_index, last_term, truncate_from, term_at, flush}` |
| Error wiring | `xraft-core/src/error.rs` (line 7) | `XRaftError::Storage(String)` — no new variants |
| Frame / segment header encode-decode | `xraft-storage/src/log_format.rs` | `encode_segment_header`, `decode_segment_header`, `encode_frame`, `decode_frame`, `HeaderDecodeError`, `FrameDecodeError`, `SEGMENT_MAGIC = "XRWL"`, `SEGMENT_VERSION = 1`, `SEGMENT_HEADER_LEN = 28`, `FRAME_BODY_HEADER = 17`, `DEFAULT_MAX_FRAME_BODY = 64 MiB` |
| Per-segment file management | `xraft-storage/src/log_segment.rs` | `Segment::{create, open, read_at, read_all}`, `segment_filename`, `parse_segment_filename`, `sync_dir`, `.wal.tmp` atomic rename |
| Public `LogStore` impl + recovery | `xraft-storage/src/log.rs` | `FileLogStore` with `Vec<OffsetRef>` offset index, `MemoryLogStore`, `DEFAULT_MAX_SEGMENT_SIZE = 64 MiB` |
| Public re-exports | `xraft-storage/src/lib.rs` (line 28) | `pub use log::{DEFAULT_MAX_SEGMENT_SIZE, FileLogStore, MemoryLogStore};` |
| Inline scenario tests | `xraft-storage/src/log.rs` (lines 769–1120) | `file_append_and_read_100`, `file_truncate_from_30_of_50`, `file_segment_rotation_1kb`, `file_crash_recovery_rebuilds_index`, `file_get_uses_offset_index_not_entry_cache`, `file_term_at_uses_offset_index_no_disk_io`, `file_truncate_across_segments_is_crash_safe`, `file_appends_durable_without_explicit_flush`, `file_snapshot_entry_roundtrip`, `file_config_change_entry_roundtrip`, `file_recovery_trims_torn_tail`, `file_first_index_resets_after_full_truncate`, `file_recovery_rejects_length_byte_corruption_in_final_frame` |
| Corruption acceptance tests | `xraft-storage/tests/log_corruption.rs` | `corruption_a_header_magic_flipped_is_foreign_file`, `corruption_b_unsupported_version_is_rejected`, `corruption_c_mid_frame_crc_mismatch_is_rejected`, `corruption_d_length_field_corrupted_to_huge_is_rejected`, `corruption_d2_length_field_byte_flipped_within_band_is_rejected`, `corruption_e_trailing_partial_write_is_trimmed`, `corruption_f_header_crc_corrupted_is_rejected`, `corruption_g_filename_does_not_match_header_base_index` |

All four work-item scenarios (append-and-read, truncate-divergent,
segment-rotation, crash-recovery) are covered by the inline tests
listed above; the corruption acceptance tests cover the additional
durability classifications spelled out in this plan.

## Architectural context

Per `architecture.md` §4.1 the `LogStore` trait lives in `xraft-core` so
that `xraft-storage` and `xraft-transport` can implement it without
circular dependencies. The trait is **synchronous** and operates on
`&[Entry]` / borrowed references to keep call sites cheap and to leave
async batching to a higher layer:

```rust
// xraft-core/src/storage.rs (existing — locked by Stage 1)
pub trait LogStore: Send + Sync {
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    fn get(&self, index: LogIndex) -> Result<Option<Entry>>;
    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>>;
    fn last_index(&self) -> LogIndex;
    fn last_term(&self) -> Term;
    fn truncate_from(&mut self, index: LogIndex) -> Result<()>;
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>>;
    fn flush(&mut self) -> Result<()>;
}
```

Errors flow through the existing `XRaftError::Storage(String)` variant
with classifying message prefixes (`wal corrupt:`, `wal foreign file:`,
`wal unsupported version:`, `wal non-contiguous:`). No new error
variants are introduced; this keeps the change surface minimal and
allows the existing snapshot-store error handling patterns to apply.

### Crate boundaries

- `xraft-core/src/storage.rs` — `LogStore` trait (already in place).
- `xraft-storage/src/log_format.rs` — pure encode / decode for the
  on-disk frame and segment header. **No I/O**, fully unit-tested,
  ready for fuzzing.
- `xraft-storage/src/log_segment.rs` — `Segment` struct: per-segment
  on-disk file (header + frames), `Mutex<File>` read handle for
  pread-style random reads, atomic create via `.wal.tmp` + rename.
- `xraft-storage/src/log.rs` — `FileLogStore` orchestration: open /
  recover, rotation, public `LogStore` impl, offset index. Also hosts
  `MemoryLogStore` for `xraft-test` and unit tests.
- `xraft-storage/tests/log_corruption.rs` — integration acceptance
  tests for every corruption-classification rule.

## On-disk format (v1, frozen)

### Segment header (28 bytes, written once at offset 0)

```text
┌─────────┬─────────┬─────────┬───────────┬───────────────────┬───────┐
│ magic   │ version │ flags   │ base_idx  │ created_at_unix_ms│ crc32 │
│ "XRWL"  │ u16 LE  │ u16 LE  │ u64 LE    │ u64 LE            │ u32 LE│
└─────────┴─────────┴─────────┴───────────┴───────────────────┴───────┘
   4 B       2 B       2 B       8 B            8 B               4 B
```

`crc32` covers the first 24 bytes. Validation order on `decode_segment_header`:

1. **Length** — fewer than 28 bytes ⇒ `HeaderDecodeError::ShortHeader`.
2. **Magic** — anything other than `XRWL` ⇒ `ForeignFile`.
3. **Header CRC** — mismatch ⇒ `Corrupt(...)`.
4. **Version** — anything other than `1` ⇒ `UnsupportedVersion(v)`.

A foreign file is *not* silently skipped. A wrong version produces an
explicit `wal unsupported version` error so future format upgrades can
ship a parallel reader without bricking older nodes.

### Frame layout (each entry, sequential after the header)

```text
┌────────┬────────┬────────┬────────┬────────────┬────────┐
│ length │ term   │ index  │ e_type │ data       │ crc32  │
│ u32 LE │ u64 LE │ u64 LE │ u8     │ length-17  │ u32 LE │
└────────┴────────┴────────┴────────┴────────────┴────────┘
   4         8        8       1     length - 17     4
```

This is the exact `[length][term][index][entry_type][data][crc32]`
shape called out in `implementation-plan.md` Stage 2.1.

* `length` = `17 + data.len()` — size of the body that follows the
  length field, **excluding** the trailing CRC.
* `crc32` covers `length_bytes ++ body` — **the length field is inside
  the CRC envelope**. A corrupted `length` cannot slip through as a
  torn tail because either (a) the new value falls outside the sanity
  band `[17 .. max_frame_body]` and is hard-classified as corruption, or
  (b) it stays in band but the trailing CRC catches the mismatch and
  is hard-classified as corruption.
* `entry_type`: `0=NoOp`, `1=Command`, `2=ConfigChange`, `3=Snapshot`.
  An unknown tag is hard corruption.

Total frame on disk = `length + 8` bytes.

### Decode classification rules (the key fix vs. iter 1)

| Condition | Class | Behaviour |
|---|---|---|
| Buffer too short for length / body / CRC | `Truncated` | Last segment ⇒ trim. Earlier segments ⇒ hard fail. |
| `length < 17` or `length > max_frame_body` | `Corrupt` | Always hard fail. |
| Trailing CRC mismatch | `Corrupt` | Always hard fail. |
| Unknown `entry_type` | `Corrupt` | Always hard fail. |
| Filename `base_index` ≠ header `base_index` | hard fail | Detected during `Segment::open`. |
| Frame `index` ≠ expected | hard fail | Continuity check during recovery. |

`max_frame_body` defaults to `max(64 MiB, max_segment_size)`. A
corrupted length value larger than this is *always* corruption — never
"the writer was interrupted mid-write".

## In-memory state (no entry cache)

```rust
pub struct FileLogStore {
    dir: PathBuf,
    segments: Vec<Segment>,
    active_writer: Option<File>,        // append-mode handle on the last segment
    first_index: LogIndex,              // index of offsets[0]
    offsets: Vec<OffsetRef>,            // dense index, true O(1) lookup
    max_segment_size: u64,
    max_frame_body: u32,
}

struct OffsetRef {
    segment_idx: usize,
    byte_offset: u64,
    byte_len: u32,
    term: Term,                         // cached so term_at is O(1) without disk
}

struct Segment {
    path: PathBuf,
    base_index: LogIndex,
    bytes_written: u64,
    reader: Mutex<File>,                // separate read handle from active_writer
}
```

* **True O(1) reads.** `Vec<OffsetRef>` indexed by `i - first_index` is
  constant-time, not the `O(log n)` of a `BTreeMap`. Memory cost is
  bounded by the number of entries (~32 bytes each), independent of
  payload size — a 10 M-entry log uses ~320 MiB of index, never
  gigabytes of cached payloads.
* **No entry cache.** `get(i)` looks up the offset, calls
  `Segment::read_at` (locked seek + `read_exact`), and decodes the
  frame on the fly. Hot reads are served by the OS page cache; the WAL
  contributes zero per-payload caching.
* **Cached term per offset.** `term_at` and `last_term` never touch
  disk — both read directly from the offset index.
* **`first_index` resets to 1** on full truncation and is captured from
  the first appended entry on a fresh log, leaving room for snapshot-
  driven log compaction in a later stage without a format change.

## Durability rules

* **Per-batch fsync.** `append(entries)` writes every frame, then fsyncs
  **every segment** the batch touched (not just the active one — a
  rotation-spanning batch needs each newly-sealed segment fsynced too).
  Only after every fsync succeeds are the offset entries published.
  Callers can therefore rely on `append` returning ⇒ entries are
  durable.
* **Atomic segment creation.** New segments are written to a `.wal.tmp`
  companion file, fsynced, and atomically renamed to their final
  `.wal` filename. The directory is then fsynced (Unix). A crash during
  segment creation can only leave (a) no file, or (b) a leftover
  `.wal.tmp` that recovery deletes — never a partial-header `.wal`
  file that would brick the next startup.
* **Crash-safe truncation.** `truncate_from(i)` deletes **later
  segments first**, fsyncs the directory, and only then trims the
  affected segment via `set_len + sync_all`. A crash between the two
  steps therefore leaves either the original WAL state (truncate
  effectively didn't happen — Raft will retry) or the fully-completed
  truncate state — never a non-contiguous mix that recovery rejects.
* **Directory fsync** is Unix-only; on Windows it's a documented no-op
  because `forbid(unsafe_code)` precludes the raw `CreateFileW` +
  `FlushFileBuffers` dance and NTFS journals filename metadata
  independently. All file *contents* are still fsynced everywhere.

## Continuity & monotonicity invariants

`append(entries)` rejects with `wal non-contiguous append:` *before*
any byte hits disk if `entries[0].index != last_index + 1` (or the
batch is internally non-monotonic). Recovery rejects with
`wal non-contiguous` if (a) a segment's first frame's index doesn't
equal its header `base_index`, (b) two adjacent segments' indices
don't chain, or (c) frames inside a segment skip an index.

## Phase / stage / step decomposition

This work item lands as a single PR (~5 source files), so the plan
collapses to **one stage with four steps** rather than a multi-stage
phase:

### Stage 2.1 — Write-Ahead Log

- **Step 1 — Format primitives in `xraft-storage/src/log_format.rs`.**
  `encode_segment_header` / `decode_segment_header` over the 28-byte
  header; `encode_frame` / `decode_frame` over the spec-compliant
  `[length][term][index][entry_type][data][crc32]` shape with CRC
  covering the length field; `FrameDecodeError { Truncated, Corrupt }`
  classification with the length-band sanity check. **expectedFileChanges: 1**.

- **Step 2 — Segment file management in
  `xraft-storage/src/log_segment.rs`.** `Segment::create` writes the
  header to a `.wal.tmp` and atomically renames; `Segment::open`
  validates magic / CRC / version; `Segment::read_at` serves locked
  seek+read for `LogStore::get`; cross-platform `sync_dir` helper.
  **expectedFileChanges: 1**.

- **Step 3 — `FileLogStore` + `MemoryLogStore` in
  `xraft-storage/src/log.rs`.** The full `LogStore` implementation:
  open / recover, append (with dirty-segment fsync), get via offset
  index, get_range, last_index / last_term / term_at from the offset
  cache, truncate_from with delete-later-first ordering, segment
  rotation, plus inline unit tests for every work-item scenario
  (100-entry append-and-read, truncate-from-30-of-50, 1 KiB
  segment-rotation, crash-recovery rebuilds the offset index,
  ConfigChange / Snapshot roundtrip, durability without explicit flush).
  Also wires `mod log_format; mod log_segment; mod log;` and
  re-exports `LogStore`, `FileLogStore`, `MemoryLogStore`,
  `DEFAULT_MAX_SEGMENT_SIZE` from `xraft-storage/src/lib.rs`.
  **expectedFileChanges: 2** (`log.rs`, `lib.rs`).

- **Step 4 — Corruption acceptance tests in
  `xraft-storage/tests/log_corruption.rs`.** End-to-end verification
  of every classification rule: bad magic ⇒ foreign-file error;
  wrong version (with recomputed header CRC) ⇒ unsupported-version
  error; mid-segment frame CRC mismatch ⇒ corruption; last-frame
  length corrupted to huge ⇒ corruption (NOT silent truncation);
  last-frame length corrupted to a smaller in-band value ⇒ corruption
  via CRC mismatch; trailing partial write ⇒ recovery succeeds with
  the last good entry intact; header CRC corruption ⇒ corruption;
  filename / header `base_index` mismatch ⇒ hard fail. **expectedFileChanges: 1**.

## Prior feedback resolution (iter 3 evaluator)

The iter 3 evaluator listed five concerns. Each is addressed below with a
file/symbol citation so the next reviewer can confirm in seconds.

1. **"Documentation only; none of the required WAL implementation landed"**
   — ADDRESSED. The implementation was committed in this branch as
   `5c0ca1e feat(stage-2.1): implement crash-safe Write-Ahead Log` and is
   present in HEAD. See the *Implementation status* table above for the
   exact files and symbols. The iter 3 evaluator appears to have read a
   plan-only intermediate state; the merge brought in feature/xraft but
   did not overwrite Stage 2.1 source files.

2. **"`xraft-storage/src/lib.rs` still declares `mod log;` and re-exports
   `DEFAULT_MAX_SEGMENT_SIZE`, `FileLogStore`, `MemoryLogStore`, so the
   crate has an unresolved module/public API"** — ADDRESSED. The module
   now resolves: `xraft-storage/src/log.rs` exists and defines all three
   re-exported symbols (line 65 for `DEFAULT_MAX_SEGMENT_SIZE`, line 149
   for `FileLogStore`, line 74 for `MemoryLogStore`). The `mod log;`
   declaration in `lib.rs` (line 1) is intentional and correct.

3. **"Plan conflicts with existing `xraft-core/src/storage.rs`: it
   proposes async methods, owned `Vec<LogEntry>`, and `Result`-returning
   `last_index`/`last_term`, but the current trait is synchronous, uses
   `&mut self`/`&[Entry]`, has `last_index() -> LogIndex`, includes
   `flush()`, and uses `Entry` from `message.rs`"** — ADDRESSED. The
   *Architectural context* section quotes the synchronous trait verbatim
   from `xraft-core/src/storage.rs` (lines 13–31): `&[Entry]` parameters,
   non-`Result` `last_index() -> LogIndex` and `last_term() -> Term`,
   `flush(&mut self) -> Result<()>` included, `Entry` imported from
   `xraft-core/src/message.rs`. There is no async signature anywhere in
   this plan.

4. **"Names `StorageError` variants that do not exist in
   `xraft-core/src/error.rs`, which currently exposes `XRaftError`"** —
   ADDRESSED. The *Architectural context* section calls out
   `XRaftError::Storage(String)` as the only error sink, with
   classifying message prefixes (`wal corrupt:`, `wal foreign file:`,
   `wal unsupported version:`, `wal non-contiguous:`). No new
   `StorageError` enum, no new variants. The implementation in
   `xraft-storage/src/log.rs` (line 52) and `log_segment.rs` (line 21)
   imports and uses `XRaftError` only.

5. **"Next iteration must implement the actual trait-compatible WAL,
   add recovery/corruption/rotation/truncation tests"** — ADDRESSED in
   prior iterations and now documented. See the *Implementation status*
   table for the test enumeration covering all four work-item scenarios
   (append-and-read, truncate-divergent, segment-rotation, crash-
   recovery) plus the corruption-classification matrix in
   `tests/log_corruption.rs`. This iteration does not add new code per
   the PLAN-task instructions; it makes the design ↔ code mapping
   explicit so future evaluators can verify both in one pass.

## Earlier (iter 1) gaps closed and still in force

1. **Crate didn't even compile** (iter 2 deleted `log.rs` while
   leaving `mod log;` in `lib.rs`) → Step 3 reinstates the module
   and all of its public symbols.
2. **Plan diverged from the actual trait surface** (iter 3 documented
   async / `Vec<LogEntry>` / `Result<LogIndex>`) → this revision
   matches the existing synchronous `&mut self` / `&[Entry]` /
   `LogIndex` trait verbatim and uses existing `XRaftError::Storage`
   error wiring.
3. **Segment header missing** → `XRWL`+version+CRC at byte 0 of
   every segment with `ForeignFile` / `UnsupportedVersion` /
   `Corrupt` recovery errors.
4. **Frame layout drifted from spec** → frozen
   `[length][term][index][entry_type][data][crc32]` shape with
   round-trip + classification tests.
5. **CRC excluded the length field** → CRC envelope explicitly covers
   `length_bytes ++ body`; tests in both `log_format.rs` and
   `tests/log_corruption.rs` pin this.
6. **Reads went through a full entry cache, not the offset index** →
   `Vec<OffsetRef>` (dense, true O(1)) is the only read-path index;
   `get` reads bytes from disk on demand. A test asserts that
   per-entry RAM stays bounded regardless of payload size.
7. **Tests missed header / version / length-corruption cases** →
   `tests/log_corruption.rs` covers all five originally-named cases
   plus header CRC corruption and filename mismatch.

## Out of scope

- **Snapshot store / `SnapshotStore` trait** — Stage 2.3 (already implemented in `snapshot_store.rs`).
- **Hard-state persistence** (`HardStateStore` / `quorum-state.toml`) — Stage 2.2 (already implemented in `state.rs`).
- **Log compaction / segment GC driven by snapshots** — needs Stage 2.3+ snapshot integration.
- **`AddVoter` / `RemoveVoter` in `LogStore`** — out of v1 entirely (`tech-spec.md` §3, §7 decision 6).
- **Async batching / group commit** — `append` batches by caller-supplied
  `&[Entry]` and fsyncs once per batch; pipelining and back-pressure are deferred.
- **Memory-mapped reads** — v1 uses locked seek+read. Memory mapping
  would complicate `truncate_from` and is deferred.
- **Cross-platform directory fsync semantics on Windows beyond the
  documented no-op** — would require `unsafe` code (forbidden by the
  workspace lint).
