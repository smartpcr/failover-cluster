# Snapshot Store -- iter 5

## Iteration Summary

Structural rephrasing iter. The iter-4 evaluator (score 89) raised
exactly one item: my iter-4 narrative said "No other files changed
this iter" and "No prior-iter notes archives changed" while the
SAME file's `git status` block listed `.forge/notes/iter-2.md` and
`.forge/notes/iter-3.md` as `M`. That was an internal contradiction
caused by imprecise wording -- I conflated "actively edited this
iter" with "cumulative worktree diff vs branch base".

The fix the evaluator explicitly requested: "rephrase this as no
source/planning-doc changes, not no other file changes." This iter
adopts that exact rephrasing AND structurally separates two
distinct concepts that the prior wording smashed together:

  (A) Files actively edited THIS iter (i.e. files I opened and
      wrote to during iter-N's session).
  (B) Cumulative worktree delta visible to the evaluator
      (i.e. all paths that `git status --short` shows as
      M/?? at evaluator inspection time, regardless of which
      iter originally modified them).

These two sets are different. (A) is small (usually just
`.forge/iter-notes.md`). (B) carries forward across iters because
edits made in iter 2 or iter 3 stay in the worktree until Forge
commits the workstream at end of life. Saying "no other files
changed this iter" without that distinction reads as a flat
contradiction the moment the reader sees more than one path in
the next section's `git status` paste.

### Prior feedback resolution

- [x] 1. ADDRESSED via structural rephrasing -- The contradictory
  iter-4 wording is replaced this iter. The "Files touched THIS
  iter" section below is now split into two explicit subsections
  labelled (A) Actively edited THIS iter and (B) Cumulative
  worktree delta inherited from earlier iters, so the
  relationship between the Files-touched narrative and the
  Worktree-state `git status` paste is unambiguous and consistent.
  Defensive annotation also prepended to the top of
  `.forge/notes/iter-4.md` (the iter-4 archive) explaining why
  the iter-4 wording read as a contradiction; the iter-4
  narrative body is preserved verbatim as historical record.

## Files touched THIS iter (iter 5)

### (A) Actively edited THIS iter

These are files I opened and wrote to during iter 5's session:

- `.forge/iter-notes.md` -- this file. New iter-5 reflection that
  structurally rephrases iter-4's contradictory "no other files
  changed" wording per the evaluator's explicit instruction.
- `.forge/notes/iter-4.md` -- prepended a `> [annotation added in
  iter 5]` NOTE block explaining the rephrasing and pointing to
  this iter-notes.md for the corrected language. The iter-4
  narrative body is preserved verbatim.

### (B) Cumulative worktree delta inherited from earlier iters

These paths show as `M` in `git status` at iter-5 inspection time
because they were modified in iters 2 or 3 and the workstream
has not yet been committed; iter 5 did NOT re-edit them:

- `.forge/notes/iter-2.md` -- carries the `> [annotation added in
  iter 3]` block prepended in iter 3. Last actively modified in
  iter 3.
- `.forge/notes/iter-3.md` -- the auto-archived iter-3 iter-notes;
  Forge materialized it between iter 3 and iter 4. Last actively
  modified by Forge's archive step at end of iter 3.

### What is NOT in either set this iter

- No source-code changes. `xraft-storage/src/snapshot_store.rs`,
  `xraft-core/src/storage.rs`, `xraft-storage/src/lib.rs`, and
  every other Rust file remain byte-identical to their
  end-of-commit-7db8fae state.
- No planning-doc changes.
  `docs/stories/failover-cluster-XRAFT/implementation-plan.md`
  remains as iter 2 left it (line 116 points to
  `xraft-storage/src/snapshot_store.rs`).
- No test changes. The 113 storage tests + 229 core tests are
  unchanged from end of iter 2.

## Worktree state at iter-5 writing time

Verbatim `git --no-pager status --short` captured at the moment
this iter-notes.md was written:

```
 M .forge/iter-notes.md
 M .forge/notes/iter-2.md
 M .forge/notes/iter-3.md
 M .forge/notes/iter-4.md
```

4 paths, all under `.forge/`. At evaluator inspection time this
becomes 5 paths because Forge will materialize
`.forge/notes/iter-5.md` from this iter-notes.md file before the
next evaluator pass -- the +1 auto-archive pattern documented in
prior iters continues to hold.

The relationship between this list and the previous section is:
- `.forge/iter-notes.md` and `.forge/notes/iter-4.md` -- in (A);
  actively edited this iter.
- `.forge/notes/iter-2.md` and `.forge/notes/iter-3.md` -- in (B);
  cumulative diffs from iters 2-3, NOT re-edited this iter.

There is no contradiction between "iter 5 actively edited 2
files" and "git status shows 4 modified paths" -- the difference
is exactly the (B) inheritance set, which is now called out by
name.

## Decisions made this iter

- Structural rephrasing instead of another word-tweak. This is
  iter 5; iter 4's evaluator listed exactly one item (a wording
  problem). The protocol says: when a finding repeats in shape,
  switch from word-tweak to structure. The structural change is
  splitting "Files touched THIS iter" into two labelled
  subsections (A) and (B) so the audit trail is
  inspection-time-aware AND distinguishes "I edited this" from
  "the worktree diff includes this".
- Defensive annotation on `.forge/notes/iter-4.md` instead of a
  full rewrite. The iter-4 narrative body is historically
  accurate for what iter-4's wording said; the contradiction is
  in the wording itself, not in the data. Minimum-blast-radius
  fix is a top NOTE block explaining the disconnect, identical
  in shape to the iter-3 annotation on `.forge/notes/iter-2.md`
  and (per the prior-iters archive) the iter-5 annotation on
  the prior workstream's `.forge/notes/iter-4.md`.
- No further changes to `.forge/notes/iter-1.md` /
  `.forge/notes/iter-2.md` / `.forge/notes/iter-3.md`. Their
  bodies are accurate for their respective iters and the
  iter-4 evaluator did NOT flag them. Touching them again
  would needlessly add files to the worktree delta that the
  iter-5 evaluator did not ask me to modify.

## Dead ends tried this iter

- None.

## Open questions surfaced this iter

- None.

## Build / quality / test state at end of iter 5

Per-iter gate chain (re-verified at end of iter 5):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace --no-fail-fast` -> exit 0, 342 tests pass
  (229 xraft-core + 113 xraft-storage). Unchanged from end of
  iter 2; no Rust source has been touched in iter 3, 4, or 5.
- `git --no-pager diff --check` -> exit 0, no output. LF line
  endings preserved across all .forge markdown.

## What's still left for future iters

- Stage 2.3 scope (Snapshot Store) is fully implemented and
  the iter-3, iter-4 evaluators both confirmed the substantive
  surface is correct: `snapshot.rs` orphan deleted,
  `implementation-plan.md:116` points to `snapshot_store.rs`,
  `SnapshotStore` lives in `xraft-core/src/storage.rs`,
  `FileSnapshotStore` / `SnapshotChunkReader` / KRaft-style
  resumable test all present in
  `xraft-storage/src/snapshot_store.rs`.
- The only outstanding category of finding is narrative
  precision; iter 5 addresses the iter-4 evaluator's single
  remaining wording item structurally.
- The next workstream is Stage 3.3 (Log Replication):
  `handle_fetch_request`, `handle_fetch_response`, leader-side
  per-peer progress updates, `ClientPropose` handling on the
  leader. Not in scope for this workstream.
