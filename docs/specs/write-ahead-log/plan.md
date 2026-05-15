# Write-Ahead Log — Design Plan

**Story / Stage:** `failover-cluster:XRAFT` Stage 2.1 (per
`docs/stories/failover-cluster-XRAFT/implementation-plan.md`).

**Goal:** Deliver a durable, append-only Raft log (`LogStore` trait + a file-
backed `FileLogStore` implementation) that survives crash, supports the
truncate-after-conflict pattern Raft requires, and offers O(1) random reads via
an offset index — not a full in-memory entry cache.

This plan is the design contract the coding pipeline implements. It deliberately
nails down the on-disk format, header, and durability rules so the
implementation cannot drift from the architecture/spec contracts. The previous
WAL iteration scored 81 because of five specific contract drifts; each is
explicitly enforced below (see "Prior-iteration gaps closed").

## Architectural context

Per `architecture.md` §4.1, *all* trait definitions live in `xraft-core` so
`xraft-storage` and `xraft-transport` can implement them without circular
dependencies. The Raft state machine in `xraft-core` calls into `LogStore` via
trait objects; it never sees file paths or segments. That separation also lets
`xraft-test` swap in a `MemoryLogStore` for deterministic simulation.

The on-disk format is fixed at v1 by this stage; a header `magic + version` byte
is the single forward-compatibility seam. Version bumps in later phases can
land a parallel reader without breaking v1 segments.

### Crate boundaries

- `xraft-core/src/storage.rs` — `LogStore` trait, `LogEntry`/`EntryPayload`
  value types, error wiring, plus a `MemoryLogStore` reference impl used by
  unit tests and by `xraft-test`.
- `xraft-storage/src/log.rs` — `FileLogStore` orchestration (open/recover,
  rotation, public `LogStore` impl, offset index).
- `xraft-storage/src/log_format.rs` — pure encode/decode of the on-disk frame
  and the segment header. No I/O. Easily fuzzable / unit-testable.
- `xraft-storage/src/log_segment.rs` — one `Segment` = one open file handle +
  base index + size accounting + scan iterator.

### On-disk format (v1, frozen)

**Segment header (32 bytes, written once at offset 0 when the segment is
created):**

```
[magic:   4 bytes "XRWL"]   // ASCII X-Raft Write-ahead Log
[version: u16 = 1]          // little-endian
[flags:   u16 = 0]          // reserved
[base_index: u64]           // first LogIndex this segment may contain
[created_at_unix_ms: u64]   // diagnostic only
[crc32 of the 28 bytes above: u32]
```

A foreign file (no `XRWL` magic) → recovery error, *not* silent skip. A wrong
`version` → recovery error with explicit "unsupported segment version" message.

**Frame layout (each entry, sequential after the header):**

```
[length:     u32]   // payload length = 8 + 8 + 1 + data.len()
[term:       u64]
[index:      u64]
[entry_type: u8]    // 0=NoOp, 1=Command, 2=ConfigChange, 3=Snapshot
[data:       bytes (length - 17)]
[crc32:      u32]   // CRC32C of [length .. data] inclusive — INCLUDES length
```

This matches the `[length][term][index][entry_type][data][crc32]` shape called
out verbatim in `implementation-plan.md` Stage 2.1. The CRC explicitly covers
the length field so that a corrupt length cannot be silently classified as
"torn tail". Recovery rule: if a frame's CRC fails, recovery aborts with
`StorageError::Corrupt { segment, offset }`. The only condition that legally
truncates a tail is *short read* (file ended mid-frame after the prior frame's
CRC verified). Anything else surfaces as corruption.

### In-memory offset index — O(1), not an entry cache

A `BTreeMap<LogIndex, OffsetRef>` where `OffsetRef = (segment_id: u64,
byte_offset: u64, byte_len: u32)`. Roughly 28 bytes per entry, so a 10M-entry
log uses ~280 MB index, not gigabytes of cached entry payloads.

`get(i)` ⇒ index lookup ⇒ `pread` on the segment file ⇒ frame decode ⇒ CRC
check ⇒ return. No full-log `BTreeMap<LogIndex, Entry>` cache. Hot reads are
served by the OS page cache; the WAL adds zero entry-level caching of its own.

### Durability rules

- `append(entries)` writes all frames, then `file.sync_data()` on the active
  segment, then returns. A batch is durable on return — never before.
- Segment create/delete also `fsync` the parent data directory (Unix). On
  Windows we open the directory via `CreateFile` with `FILE_FLAG_BACKUP_SEMANTICS`
  and `FlushFileBuffers`; if that's not viable the implementer documents the
  fallback (file-only fsync) explicitly in the module doc comment.
- `truncate_from(i)` rewrites the affected segment to the cut point with
  `set_len + sync_data`, deletes any later segments, fsyncs the directory,
  rebuilds the offset index slice, *then* returns. After return, `last_index`
  is `i - 1` and `get(i)` is `None`.

### Continuity & monotonicity invariants

`append(entries)` rejects with `StorageError::NonContiguousAppend` if
`entries[0].index != last_index + 1` or if any term goes backward inside the
batch. This catches Raft state-machine bugs at the seam.

---

## Phase 1 — Write-Ahead Log

### Stage 1.1 — `LogStore` trait surface in `xraft-core`

Establish the trait everyone else codes against; nothing here touches disk.

- Add `xraft-core/src/storage.rs` defining `pub trait LogStore: Send + Sync`
  with async methods `append(&self, entries: Vec<LogEntry>) -> Result<()>`,
  `get(&self, index: LogIndex) -> Result<Option<LogEntry>>`, `get_range(&self,
  start: LogIndex, end: LogIndex) -> Result<Vec<LogEntry>>`, `last_index(&self)
  -> Result<LogIndex>`, `last_term(&self) -> Result<Term>`, `term_at(&self,
  index: LogIndex) -> Result<Option<Term>>`, `truncate_from(&self, index:
  LogIndex) -> Result<()>`. Add `LogEntry { index, term, payload:
  EntryPayload }` and `EntryPayload { NoOp, Command(Bytes), ConfigChange(Bytes),
  Snapshot(Bytes) }`. Add `StorageError` variants
  (`Io`, `Corrupt`, `NonContiguousAppend`, `UnsupportedVersion`,
  `ForeignFile`). **expectedFileChanges: 3** (`storage.rs`, `error.rs`,
  `lib.rs`).

- Add `MemoryLogStore` (in-memory `Vec`-backed reference impl) plus unit tests
  for append, get, get_range, truncate_from, term_at, and rejection of a
  non-contiguous append. Used by `xraft-core` callers and by `xraft-test` for
  deterministic simulation. **expectedFileChanges: 2** (`storage.rs`,
  optional `storage_tests.rs`).

### Stage 1.2 — On-disk format primitives in `xraft-storage`

Pure encode/decode + segment header — zero I/O orchestration. Easy to fuzz.

- Add `xraft-storage/src/log_format.rs` with `encode_frame(buf, entry) ->
  usize`, `decode_frame(bytes) -> Result<(LogEntry, usize), FrameError>`,
  `FrameError { Truncated, BadCrc, BadLength, BadEntryType }`. Encode writes
  the frozen `[length:u32][term:u64][index:u64][entry_type:u8][data][crc32:u32]`
  layout; decode validates CRC over `length..data` so a corrupt `length` does
  not slip through as a short tail. Inline unit tests for round-trip,
  CRC-mismatch, single-byte-flip in length, and unknown entry type.
  **expectedFileChanges: 2** (`log_format.rs`, `lib.rs`).

- Add `xraft-storage/src/log_segment.rs` with `Segment { id, base_index, file,
  bytes_written }`, `Segment::create(dir, id, base_index)` (writes the
  `XRWL`+v1 header and fsyncs), `Segment::open(path)` (validates header,
  returns `ForeignFile` if magic missing or `UnsupportedVersion` for non-v1),
  and `Segment::scan() -> impl Iterator<Item = Result<(offset, LogEntry)>>`
  used by recovery. Unit tests cover header round-trip, foreign-file
  rejection, version mismatch, and scan over a known fixture.
  **expectedFileChanges: 2** (`log_segment.rs`, `lib.rs`).

### Stage 1.3 — `FileLogStore` core operations

Wire the format primitives into the actual `LogStore` trait impl. Reads must
go through the offset index.

- Add `xraft-storage/src/log.rs` with `FileLogStore { dir, active: Segment,
  sealed: Vec<SegmentRef>, offsets: BTreeMap<LogIndex, OffsetRef>, max_segment_size,
  inner: Mutex<…> }` plus `FileLogStore::open(dir, opts)` that creates the
  initial segment when the directory is empty and registers it as active.
  Implement `append` (continuity check → encode → write → `sync_data` → update
  `offsets` and `bytes_written` → record `last_term`) and `get` (lookup
  `OffsetRef` → `pread` → `decode_frame` → return). No full-entry cache. Unit
  test the **append-and-read** scenario (100 entries, every `get(i)` returns
  the right entry, `last_index == 100`). **expectedFileChanges: 3** (`log.rs`,
  `lib.rs` exports, `Cargo.toml` deps if needed).

- Implement `get_range`, `last_index`, `last_term`, `term_at` on
  `FileLogStore`. `get_range` walks the offset index for the requested span,
  batching `pread`s within a segment. Unit test for ranges that span segments
  and out-of-bounds ranges. **expectedFileChanges: 1** (`log.rs`).

- Implement `truncate_from(index)`: locate the segment owning `index`, rewrite
  it via `set_len(cut_offset)` + `sync_data`, delete every later segment,
  fsync the parent directory, drop offset-index entries `>= index`, recompute
  `last_term` from the highest surviving entry. Unit test the
  **truncate-divergent** scenario (50 entries, `truncate_from(30)` →
  `last_index == 29`, `get(30) == None`). **expectedFileChanges: 1**
  (`log.rs`).

- Implement segment rotation: when an `append` would push `bytes_written`
  past `max_segment_size` (configurable, default 64 MiB), seal the active
  segment, create a new one with `base_index = last_index + 1`, fsync the
  directory. Unit test the **segment-rotation** scenario at
  `max_segment_size = 1 KiB`: confirm a second segment file appears on disk
  and `get_range` across the boundary is correct. **expectedFileChanges: 1**
  (`log.rs`).

### Stage 1.4 — Crash recovery & corruption surfacing

The other half of durability — what happens on `open()` after a crash.

- Implement `FileLogStore::recover(dir)`: list `*.log`, sort by id, validate
  every segment header (`ForeignFile` / `UnsupportedVersion` are hard
  errors), scan frames in order to rebuild the offset index. The *only*
  legal tail truncation is a short read on the *last* segment, *after* the
  prior frame's CRC verified. Any other decode failure (`BadCrc`,
  `BadLength`, gap in indices) returns `StorageError::Corrupt {segment,
  offset}` so the operator sees committed data loss instead of silent
  truncation. Unit test the **crash-recovery** scenario (write N entries,
  drop the store, reopen, verify all reads + `last_index` + `last_term` +
  offset index population). **expectedFileChanges: 1** (`log.rs`).

- Add explicit corruption acceptance tests in
  `xraft-storage/tests/log_corruption.rs`: (a) header magic flipped → open
  returns `ForeignFile`; (b) header version bumped to 99 → open returns
  `UnsupportedVersion`; (c) mid-segment frame CRC byte flipped → open returns
  `Corrupt`; (d) `length` field of the last frame corrupted to a huge value
  → open returns `Corrupt` (NOT silent truncation); (e) trailing partial
  write on the last segment → open succeeds, last good index intact.
  **expectedFileChanges: 1**.

- Wire the public surface: re-export `LogStore`, `FileLogStore`,
  `MemoryLogStore`, `DEFAULT_MAX_SEGMENT_SIZE` from `xraft-storage/src/lib.rs`;
  add a brief module doc comment in `log.rs` summarizing the format,
  durability contract, and the "CRC covers length" invariant so future
  contributors do not drift. **expectedFileChanges: 2** (`lib.rs`, `log.rs`).

---

## Prior-iteration gaps closed by this plan

1. **Segment header missing** → Stage 1.2 step 2 makes `XRWL`+version+CRC the
   first 32 bytes of every segment, with explicit `ForeignFile` /
   `UnsupportedVersion` errors.
2. **Frame layout drifted from spec** → Stage 1.2 step 1 freezes the exact
   `[length][term][index][entry_type][data][crc32]` shape from
   `implementation-plan.md`; round-trip tests pin it.
3. **CRC excluded the length field** → Stage 1.2 step 1 mandates CRC over
   `length..data`; Stage 1.4 step 2 ships a length-corruption test.
4. **Reads went through a full entry cache, not the offset index** →
   Stage 1.3 makes `offsets: BTreeMap<LogIndex, OffsetRef>` the only read
   path; the design notes call out "no entry cache" explicitly.
5. **Tests missed header/version/length-corruption cases** → Stage 1.4
   step 2 adds the dedicated corruption test file with all five cases.

---

## Out of scope

- **Snapshot store / `SnapshotStore` trait** — Stage 2.3.
- **Hard-state persistence** (`HardStateStore`, `quorum-state` file) —
  Stage 2.2.
- **Log compaction / segment GC** — driven by snapshots; Stage 2.3+.
- **`AddVoter` / `RemoveVoter`** — out of v1 entirely
  (`tech-spec.md` §3, §7 decision 6).
- **Async batching / group commit optimization** — `append` batches by
  caller-supplied `Vec<LogEntry>` and fsyncs once; pipelining and
  back-pressure are explicitly deferred.
- **Memory-mapped reads** — v1 uses `pread` only. Memory-mapping would
  complicate `truncate_from` and is deferred.
- **Cross-platform directory fsync semantics on Windows beyond
  `FlushFileBuffers`** — documented as a known caveat.
