# Snapshot Store -- iter (fresh after history reset)

## Iteration Summary

The prior `iter-notes.md` archived in this worktree was inherited
from a different workstream (Stage 3.2 Leader Election / Stage 3.3
Log Replication). Disregard those entries -- they apply to other
stages that ran in this same worktree directory. THIS workstream
is `ws-failover-cluster-xraft-phase-persistent-storage-stage-snapshot-store`
on branch `ws/failover-cluster-XRAFT/phase-persistent-storage-stage-snapshot-store`.

The Snapshot Store implementation is **already merged** into this
branch via:

- `63b2ad5 [impl] Snapshot Store (#7)` -- the original merge from
  `feature/xraft` carrying the full implementation.
- `c5f324c / 058b0c3 / 0a5af09 / 5c8b155` -- four prior
  "address review comment" commits on this same branch.
- `4075659` (HEAD) -- merge of `origin/feature/xraft` bringing in
  Leader Election (#10) and Log Replication (#19) on top.

Worktree at iter start: `git status --short` -> empty (clean).
No new code is needed this iter. Per-iter gate chain is green.

## Files touched THIS iter

[NOTE: this file is `.forge/notes/iter-1.md`, the auto-archived
copy of `.forge/iter-notes.md` after iter 1 ended. The "this file"
phrasing below is preserved verbatim from the original
iter-notes.md but, when read inside notes/iter-1.md, "this file"
should be understood as "iter-notes.md as written during iter 1".]

Actively edited (during iter 1):
- `.forge/iter-notes.md` -- the live iter-notes file (NOT this
  archived copy). Replaces the stale Stage-3.3 narrative archived
  from a previous unrelated run.

No source-code edits this iter. The Rust workspace
(`xraft-core`, `xraft-storage`, `xraft-transport`, `xraft-server`,
`xraft-client`, `xraft-test`) is untouched.

## Snapshot Store implementation surface (already merged)

Trait definition (xraft-core):
- `xraft-core/src/storage.rs:97` -- `pub trait SnapshotStore`
  with `save_snapshot`, `load_snapshot`, `list_snapshots`,
  `delete_snapshot`, plus chunked-read entry points.

Implementations (xraft-storage):
- `xraft-storage/src/snapshot_store.rs` (4362 lines)
  - `MemorySnapshotStore` -- volatile in-memory store for tests.
  - `FileSnapshotStore` -- durable file-backed store.
  - `SnapshotChunkReader` -- streamed chunked reader for transfer.
  - `DEFAULT_CHUNK_SIZE` (1 MiB) constant.
  - Binary header: magic `XSNP` (u32 LE) + version (u16 LE) +
    last_included_index (u64 LE) + last_included_term (u64 LE) +
    voter_set length (u32 LE) + voter_set bytes + payload length
    (u64 LE) + payload + CRC32 (u32 LE) covering payload only.
  - Canonical filename `snapshot-{term:010}-{index:020}.bin`
    under `<data_dir>/snapshots/`.
- `xraft-storage/src/lib.rs` -- re-exports
  `MemorySnapshotStore`, `FileSnapshotStore`, `SnapshotChunkReader`,
  `DEFAULT_CHUNK_SIZE`, and `SnapshotStore` (the trait).

Note on the 5 target paths in the workstream brief
(`src/storage/snapshot/{mod,store,file,metadata,writer,reader}.rs`,
`tests/snapshot_store_test.rs`): those are .NET / generic-template
placeholders. The Rust crate flattens the equivalent surface into
`xraft-storage/src/snapshot_store.rs` (single module file with
embedded `#[cfg(test)] mod tests`). The implementation covers
every concept those paths describe (store trait, file impl, in-mem
impl, metadata header, writer/reader, chunked reader, integration
tests). 112 storage unit tests cover the surface end-to-end.

## Pre-existing oddity (NOT addressed this iter)

`xraft-storage/src/snapshot.rs` is 4362 lines, near-identical to
`snapshot_store.rs`, and is **not** declared as a module in
`lib.rs` (no `mod snapshot;` line). It is dead code on disk --
tracked in HEAD since `779c9f2` but never compiled. The two
files diverge only on three small Rust-1.95 clippy fixes that
landed on `snapshot_store.rs` in commits `6a9349f`, `501743e`,
`d4f46b2` (sort_by_key + checked_div). Not deleting it: the
brief forbids removing files under production paths without an
explicit instruction, and removing it has no functional effect
(it's already orphaned from the build graph).

## Decisions made this iter

- Treat this as a no-op verification iter. The workstream's
  deliverable is already on the branch; the right action is to
  re-verify gates and document state, not to re-implement what's
  there or aggressively refactor.
- Do not delete the orphan `snapshot.rs`. The brief's
  "DO NOT DELETE PRODUCTION CODE" rule applies; surfacing as a
  pre-existing-issue note instead.
- Do not retouch the Rust source. Build + fmt + clippy + tests
  all pass; any edit risks regressing a green workstream.

## Dead ends tried this iter

- None.

## Open questions surfaced this iter

- None requiring operator input. The pre-existing orphan
  `xraft-storage/src/snapshot.rs` could be removed in a small
  follow-up cleanup workstream, but it does not block anything.

## Build / quality / test state at end of iter

Per-iter gate chain (re-verified from worktree root):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace --no-fail-fast` -> exit 0.
  - xraft-core unit tests: 229 passed.
  - xraft-storage unit tests: 112 passed.
  - xraft-client / xraft-server / xraft-test / xraft-transport:
    0 tests (binaries / harness crates).
  - Total: 341 passing, 0 failing, 0 ignored.
- `git --no-pager status --short` -> empty (worktree was clean
  on entry; only `.forge/iter-notes.md` is touched this iter,
  and `.forge/` is excluded from the worktree's git index).

## What's still left for future iters / follow-up workstreams

- Stage scope is done; no follow-up needed for this workstream.
- Optional cleanup workstream: delete the orphan
  `xraft-storage/src/snapshot.rs` (not in any module tree, not
  compiled). Out of scope here.
