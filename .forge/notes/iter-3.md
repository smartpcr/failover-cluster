# Snapshot Store -- iter 3

## Iteration Summary

Pure narrative-correction iter. The iter-2 evaluator (score 89,
verdict iterate) raised 4 items, all of which are the same
critique restated with different line references: my iter-2
narrative listed source paths as "Files touched THIS iter" /
"Worktree state at iter-2 writing time" that, by the time the
iter-2 evaluator inspected the worktree, were no longer in
`git status --short` (they had been auto-committed mid-iter as
commit `7db8fae`). The evaluator explicitly notes this is a
traceability concern, not a Stage 2.3 functional concern: "The
current branch appears to contain a substantive, tested Rust
snapshot store that satisfies Stage 2.3, but the only
Engineer-changed files this iter are notes and their change
narrative conflicts with the ground-truth file list."

The fix is structural, not another word-tweak. Two parts:
1. Prepend a `> [annotation added in iter 3]` block to
   `.forge/notes/iter-2.md` (the iter-2 archive) explaining
   the chronology — narrative was accurate at writing time,
   went stale after Forge's mid-iter auto-commit. Body of the
   archive is preserved verbatim as the historical record.
2. Write THIS iter-3 iter-notes.md with the iter-3 work strictly
   limited to what `git status` actually shows for iter 3. Past
   work (snapshot.rs deletion, implementation-plan.md edit,
   iter-1.md self-reference fix, KRaft-style resumable test) is
   attributed to the commits where it lives (`7db8fae` and
   `231fa5b`), not to this iter's worktree diff.

### Prior feedback resolution

- [x] 1. ADDRESSED -- Iter-3 iter-notes.md (this file) lists ONLY
  what is in `git status --short` for iter 3. Verbatim status
  capture in "Worktree state at iter-3 writing time" below shows
  exactly the two files the iter-2 evaluator pointed at:
  `.forge/iter-notes.md` and `.forge/notes/iter-2.md`. The stale
  iter-2 narrative claiming `.forge/notes/iter-1.md` /
  `implementation-plan.md` / `snapshot.rs` are in this iter's
  changed-file list is annotated as historical at the top of
  `.forge/notes/iter-2.md` so a fresh reader knows why it
  appears to disagree with the ground truth.

- [x] 2. ADDRESSED via the same iter-2-archive annotation. The
  paragraph the iter-2 evaluator pointed at (lines 94-109,
  "Files touched THIS iter (iter 2)") still stands as the
  Engineer's record of what he edited DURING iter 2 — but the
  prepended NOTE block makes it explicit that those edits
  landed in commit `7db8fae` mid-iter and were therefore no
  longer in `git status --short` by the time the iter-2
  evaluator inspected the worktree. The Engineer's iter-2 edits
  themselves are real and preserved on the branch; only the
  attribution shape ("uncommitted in this iter" vs "committed
  mid-iter") was misleading and is now clarified.

- [x] 3. ADDRESSED via the same iter-2-archive annotation. The
  pasted `git status --short` block in iter-2 archive lines
  117-131 is real ground truth for the moment it was captured
  (after my edits, before the auto-commit). The annotation
  block at the top of the file calls out the chronology so a
  fresh reader understands why it does not match the iter-3
  evaluator's `git status` view. NO attempt is made to rewrite
  the iter-2 status capture itself — that would falsify the
  historical record. Iter 3's iter-notes.md (this file) carries
  the iter-3-current `git status` capture below, which IS the
  ground truth for iter-3 evaluator inspection.

- [x] 4. ADDRESSED via the same iter-2-archive annotation. The
  iter-2 narrative's "Prior feedback resolution" block (iter-2
  archive lines 48-81) describes Engineer-edits to source/doc
  paths that are real and present on the branch (in commit
  7db8fae) but are NOT in the iter-2-or-iter-3 worktree diff.
  The annotation reframes them as "landed in commit 7db8fae"
  rather than "Engineer changes this iter". The iter-1
  evaluator's three findings (orphan snapshot.rs, doc pointing
  at wrong file, archive self-reference) ARE all resolved on
  the current branch — the iter-2 evaluator independently
  verified this in its "Improvements this iteration" section
  ("xraft-storage/src/snapshot.rs is absent",
  "implementation-plan.md:116 points to snapshot_store.rs",
  "the old self-reference problem is fixed in the current
  workspace").

## Files touched THIS iter (iter 3)

Actively edited / created by me in iter 3:
- `.forge/iter-notes.md` -- this file. New iter-3 reflection
  with the 4-item resolution checklist above.
- `.forge/notes/iter-2.md` -- prepended a `> [annotation added
  in iter 3]` block at the top explaining the iter-2 narrative
  went stale 0.0 seconds after the mid-iter auto-commit. Body
  preserved verbatim.

No source-code edits this iter. No doc edits this iter. The
Stage 2.3 implementation (`xraft-storage/src/snapshot_store.rs`
+ `xraft-core/src/storage.rs` trait + planning docs all
pointing at the right module) is unchanged from end-of-iter-2,
which is unchanged from commit 7db8fae as confirmed by the
iter-2 evaluator's "Improvements this iteration" verification.

## Worktree state at iter-3 writing time

Verbatim `git --no-pager status --short` captured AT START OF
ITER 3 (before any edit, just to anchor the chronology):

```
(empty -- working tree clean; HEAD = 231fa5b)
```

Verbatim `git --no-pager status --short` AFTER my iter-3 edits
(this iter-notes.md + the iter-2 archive annotation):

```
 M .forge/iter-notes.md
 M .forge/notes/iter-2.md
```

2 paths total, both modified, both under `.forge/`. At evaluator
inspection time this becomes 3 paths because Forge will
materialize `.forge/notes/iter-3.md` from this iter-notes.md
file before the next evaluator pass — the structural +1
auto-archive pattern. Policy: for every iter N, the evaluator's
inspection-time path count = the in-iter `git status --short`
line count + 1.

These ARE the only two files I edited this iter. The 4 iter-2
evaluator findings are about a NARRATIVE/ATTRIBUTION mismatch,
not about missing or wrong source code, so the fix surface is
strictly inside `.forge/`. Touching source files this iter
would be wrong — the source state already passes the evaluator.

## Decisions made this iter

- Annotate-don't-rewrite for `.forge/notes/iter-2.md`. The
  iter-2 narrative body is the Engineer's truthful record of
  what he saw at iter-2 writing time. Rewriting the body to
  match iter-3's ground truth would falsify history. A
  prepended NOTE block explains the chronology so the body
  reads correctly in context. Same structural pattern used
  successfully in the prior workstream's iter-5 (Stage 3.2
  Leader Election) for an analogous +1 auto-archive issue.

- THIS iter-notes.md is terse and limited to iter-3 work only.
  Past iters' substantive work is referenced by commit hash
  (7db8fae for the snapshot.rs cleanup + doc fix + KRaft test;
  231fa5b for the iter-2 postscript auto-commit), not
  re-narrated as "files touched this iter".

- No source-code edits, no doc-only edits, no test-only edits.
  The iter-2 evaluator confirmed Stage 2.3 functional surface
  is correct. The 4 findings are exclusively narrative; the
  fix surface is exclusively `.forge/`.

## Dead ends tried this iter

- None.

## Open questions surfaced this iter

- None.

## Build / quality / test state at end of iter 3

Per-iter gate chain (re-verified at end of iter 3):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace --no-fail-fast` -> exit 0, 342 tests
  pass (229 xraft-core + 113 xraft-storage). Unchanged from
  end-of-iter-2 (which was the post-7db8fae state); no Rust
  source has been touched in iter 3.
- `git --no-pager diff --check` -> exit 0, no output. Both
  edited `.forge/*.md` files are LF + ASCII clean.

## What's still left for future iters

- Stage 2.3 source surface is fully done; iter-2 evaluator
  independently verified `snapshot.rs` is absent,
  `implementation-plan.md:116` points to `snapshot_store.rs`,
  and the iter-1.md archive self-reference is fixed. This iter
  (3) exists only to correct the narrative attribution that
  caused iter 2 to score 89 instead of pass.
- If iter-4 evaluator still flags narrative items, the next
  structural escalation is to delete the iter-2 archive's
  problematic paragraphs entirely (rather than annotate) and
  defer the historical record to git log + commit messages.
  Doing that now would be premature; the annotation approach
  matches the prior-workstream pattern that DID converge.
