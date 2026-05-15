# Stage 2.3 Snapshot Store -- iter 3

## Iteration summary

Notes-audit-accuracy iter; STRUCTURAL fix this time, not another
word-tweak. The iter-2 evaluator (score 89) flagged three items, all
caused by ONE mechanism: Forge auto-archives `.forge/iter-notes.md`
to `.forge/notes/iter-N.md` between iters as a byte-identical copy,
so every grep hit and every file-status claim my notes make is
mirrored in the auto-archive. My iter-2 fix attempted to acknowledge
this with prose but still made specific COUNT claims (4 hits, 1 hit)
that were wrong by exactly 2x at evaluator-inspection time.

The structural fix this iter:
  1. Factor "Files touched THIS iter" into THREE explicit subsets
     (A) actively edited / (B) Forge auto-archive / (C) carried-over
     so the auto-archive path is never confused with an active edit.
  2. Drop count-based grep claims entirely. Replace with category-
     based prose assertions that hold regardless of how many copies
     of iter-notes.md exist in the worktree.

### Prior feedback resolution

- [x] 1. ADDRESSED STRUCTURALLY -- The "Files touched THIS iter"
  section below is now factored into subsets (A) actively edited,
  (B) Forge auto-archive, (C) carried-over M from prior iters,
  (D) pre-existing tracked unchanged. The iter-2 evaluator's
  complaint was that I lumped `.forge/notes/iter-2.md` into
  "not touched" when in fact it was a Forge auto-archive that
  showed as M. Subset (B) now exists explicitly to account for
  Forge auto-archives; subset (C) accounts for prior-iter M
  paths that carry over because the workstream is not yet
  committed. Both are distinct from "I actively edited" (A).

- [x] 2. ADDRESSED STRUCTURALLY -- Verification of "iter-7.md"
  removal no longer claims a hit count. The structural problem
  with count claims: Forge mirrors iter-notes.md as
  `.forge/notes/iter-3.md` (verbatim copy), so every grep
  citation in iter-notes.md is doubled at evaluator time, and
  any predicted count is off by 2x. Iter-3's count-INDEPENDENT
  assertion: NO surviving "iter-7.md" string in any .forge/
  file is a load-bearing archive-path CLAIM about the current
  iter's archive. Every surviving hit falls into one of:
    (i)   a descriptive citation inside this resolution-checklist
          item, explaining what was removed from prior iter notes;
    (ii)  the byte-identical mirror of (i) inside the Forge
          auto-archive `.forge/notes/iter-3.md`;
    (iii) the post-hoc explanation in `.forge/notes/iter-1.md`
          recording what iter 1 wrongly claimed (kept as
          historical record per iter-2 evaluator's commendation);
    (iv)  the iter-2 auto-archive `.forge/notes/iter-2.md`,
          which is a byte-identical mirror of iter-2's
          iter-notes.md and is preserved as historical record.
  No live `iter-notes.md` produced from iter 3 onward will
  contain a load-bearing iter-7.md archive-path claim.

- [x] 3. ADDRESSED STRUCTURALLY -- Same structural pattern as
  item 2, applied to the "Stage 2.3 Snapshot Store -- iter 7"
  H1-header string. Count-INDEPENDENT assertion: the H1 of
  every LIVE archive file is byte-correct:
    `.forge/iter-notes.md`    line 1 = "# Stage 2.3 Snapshot Store -- iter 3"
    `.forge/notes/iter-1.md`  line 1 = "# Stage 2.3 Snapshot Store -- iter 1 (corrected post-hoc in iter 2)"
  Any "iter 7" header string surviving in .forge/ is inside a
  resolution-checklist citation (in iter-notes.md or its
  auto-archive iter-3.md) or in a historical archive
  (iter-2.md, which mirrors iter-2's iter-notes.md). None is
  a live H1 of a current archive.

## Worktree state at iter-3 writing time

Verbatim `git --no-pager status --short` captured while
writing this file:

    M .forge/iter-notes.md
    M .forge/notes/iter-1.md
    M .forge/notes/iter-2.md

3 paths under .forge/. At evaluator-inspection time Forge will
have additionally materialized `.forge/notes/iter-3.md` (a
byte-identical copy of this file), bringing the count to 4.
The mapping of each M path to the (A)/(B)/(C) subsets below:

  - `.forge/iter-notes.md`    -> (A) actively edited THIS iter
  - `.forge/notes/iter-1.md`  -> (C) M carried over from iter 2
  - `.forge/notes/iter-2.md`  -> (C) M carried over from iter 2's auto-archive
  - `.forge/notes/iter-3.md`  -> (B) Forge auto-archive THIS iter (appears at evaluator time only)

## Files touched THIS iter (iter 3)

### (A) Actively edited THIS iter

Files I opened and wrote to in iter 3's session:

- `.forge/iter-notes.md` -- this file. Restructured iter-3
  reflection that drops count-based grep claims and adds
  explicit subsets (A)/(B)/(C)/(D) for file accounting.

### (B) Forge auto-archive THIS iter

Files Forge will materialize after my session ends, before
the iter-3 evaluator inspects the tree:

- `.forge/notes/iter-3.md` -- byte-identical copy of this
  `.forge/iter-notes.md`. Any string appearing in this live
  file (including iter-7 citations in the resolution checklist
  above) will appear mirrored in the iter-3 auto-archive. This
  is the mechanism that broke iter-2's count claims.

### (C) M carried over from prior iters (NOT actively edited THIS iter)

These show as M in iter-3's `git status` because they were
edited in iter 2 and the workstream branch is not yet committed:

- `.forge/notes/iter-1.md` -- iter-2's post-hoc rewrite of
  the iter-1 archive (the iter-2 evaluator's commendation:
  "now self-identifies as the iter-1 archive"). Iter 3 leaves
  it untouched.
- `.forge/notes/iter-2.md` -- Forge auto-archive of iter-2's
  iter-notes.md, materialized between iter 2 and iter 3 by
  Forge. Iter 3 did NOT actively edit it. The iter-2 evaluator's
  Item 1 specifically asked me to acknowledge this distinction;
  this entire subset (C) plus the `.forge/notes/iter-2.md ->
  carried over from iter 2's auto-archive` mapping above is
  the structural form of that acknowledgement.

### (D) Pre-existing tracked, unchanged THIS iter

- `.forge/notes/iter-4.md`, `iter-5.md`, `iter-6.md` --
  stale archives from a prior workstream attempt, committed in
  `997badd`. Not flagged by any evaluator; not edited.
- All Rust source under `xraft-core/`, `xraft-storage/`,
  `xraft-transport/`, etc. The substantive snapshot-store
  surface (`snapshot_store.rs`, `storage.rs`) is in commit
  `997badd` and remains complete.
- `docs/stories/failover-cluster-XRAFT/implementation-plan.md`.

## Build / quality / test state

No Rust source touched in iters 2 or 3. Full gate chain was
re-verified at end of iter 1 (just minutes before iter 2),
exit codes 0:

- `cargo build --workspace` -> 0
- `cargo fmt --check --all` -> 0
- `cargo clippy --workspace --all-targets -- -D warnings` -> 0
- `cargo test --workspace --no-fail-fast` -> 0; 342 tests pass
  (229 xraft-core + 113 xraft-storage)

`git --no-pager diff --check` re-verified at end of iter 3
-> exit 0; LF-only line endings preserved on the one edited
file.

## What's still left

- Stage 2.3 (Snapshot Store) source surface: complete and verified
  by 3 prior evaluators; no further code changes warranted.
- Next workstream: Stage 3.3 (Log Replication). Out of scope here.
