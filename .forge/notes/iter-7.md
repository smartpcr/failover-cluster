> [NOTE added in iter 8] One specific claim in the iter-7 body
> below is factually incorrect and was retracted in iter 8. Read
> `.forge/iter-notes.md` (or `.forge/notes/iter-8.md` once Forge
> archives it) for the corrected version. The error is:
>
> 1. The iter-7 Iteration Summary opens with "The iter-6 evaluator
>    (evaluator verdict pass at score 94 ...)". Per the iter-7
>    evaluator's explicit statement, the actual iter-6 evaluator
>    output was `score 89` with verdict `iterate`. The "verdict
>    pass at score 94" attribution belongs to the iter-5 evaluator,
>    not the iter-6 evaluator -- iter 7 conflated the two iters'
>    outputs while drafting the structural-rule narrative. The body
>    of iter 7 then proceeds to address iter-6's actual findings
>    (the "(score 94, iterate)" misquote and the "held below pass"
>    misquote that iter 6 itself made about iter 5), but the
>    opening sentence's evaluator-attribution is wrong.
>
> Iter 8 has added a verified ground-truth table at the top of
> `.forge/iter-notes.md` listing each past iter's score and
> verdict (sourced from the iter-7 evaluator's iteration-history
> block) so future iters cross-check before writing prose.
>
> The iter-7 narrative body is preserved verbatim below this
> annotation block so the historical record of what iter 7
> originally said remains traceable.
# Stage 3.2: Leader Election -- iter 7 (post-merge cycle)

## Iteration Summary

Targeted corrective iter. The iter-6 evaluator (evaluator verdict
pass at score 94 -- see structural-rule note below; convergence
detector imposed BLOCKED override) caught two narrative inaccuracies
in iter 6's notes that are LITERALLY THE SAME shape as the iter-4
inaccuracies that iter 5 already fixed. This is the second
occurrence of the same mistake (iter-4 wrote "(score 92, iterate)"
about iter-3; iter-6 wrote "(score 94, iterate)" about iter-5). Per
the STRICT-PER-ITEM-ATTENTION protocol, repeating the same
edit-shape failure twice is a strong signal to switch to a
structural fix.

This iter actively modifies TWO files:
1. `.forge/iter-notes.md` (this file) -- a fresh iter-7 narrative
   that applies a STRUCTURAL framing rule (see below) so the same
   inaccuracy cannot recur.
2. `.forge/notes/iter-6.md` -- a defensive `[NOTE added in iter 7]`
   block prepended at the top retracting the two specific incorrect
   claims and pointing to this iter-notes.md for the corrected
   version. Same minimum-blast-radius pattern iter 5 used for
   notes/iter-4.md.

### STRUCTURAL framing rule adopted in iter 7

To prevent the "(score N, iterate)" mistake from recurring in any
future iter of this workstream, this iter adopts a single canonical
phrasing rule for all references to past evaluator results:

  NEVER write "(score N, iterate)" or "(score N, pass)" or any
  parenthetical that conflates evaluator verdict with the convergence
  detector's BLOCKED state. Instead, ALWAYS write:

    "the iter-K evaluator (verdict pass at score N; convergence
     detector imposed BLOCKED override)"

  or, when the evaluator verdict really was iterate:

    "the iter-K evaluator (verdict iterate at score N)"

The reason "(score N, iterate)" is wrong: Forge's evaluator and
Forge's convergence detector are TWO INDEPENDENT scoring stages.
The evaluator emits a verdict (pass/iterate) and a numeric score.
The convergence detector then runs its own checks (checkbox-count
heuristic, etc.) and may impose a BLOCKED override that prevents
pass-progression even when the evaluator's verdict is pass. So
saying "(score N, iterate)" implies the evaluator emitted iterate,
when in fact the iterate continuation came from the detector. Iter
6 made this mistake (caught by iter-6 evaluator); iter 4 made the
same mistake about iter 3 (caught by iter-4 evaluator). The
canonical rule above eliminates the ambiguity.

### Prior feedback resolution -- iter-6 evaluator

- [x] 1. ADDRESSED -- iter-6's "(score 94, iterate)" verdict
  reference for the iter-5 evaluator (iter-notes.md and
  notes/iter-6.md line 5) is corrected. The iter-5 evaluator's
  actual verdict was PASS at score 94; the "iterate" continuation
  came from the convergence detector's BLOCKED override (the
  detector counted 0 `[x]` markers against 2 `[ ]` items from
  iter-4's prior list and held pass-progression back independently
  of the evaluator). This iter's narrative uses the canonical
  framing rule throughout (verdict pass; detector BLOCKED override)
  and the iter-6.md archive defensive annotation explicitly states
  the same.

- [x] 2. ADDRESSED -- iter-6's "the score was held below pass" claim
  (iter-notes.md and notes/iter-6.md line 8) is corrected. Score 94
  is ABOVE the pass threshold; the evaluator did emit a passing
  verdict. What actually happened is that the convergence detector's
  BLOCKED override prevented pass-progression despite the passing
  evaluator score. This iter's narrative explicitly says "the
  convergence detector blocked progression despite a passing
  evaluator score" and the iter-6.md defensive annotation calls out
  the same correction.

## Files touched THIS iter (iter 7, post-merge cycle)

Actively edited by me in iter 7:
- `.forge/iter-notes.md` -- this file. Replaces the iter-6 body with
  iter-7 reflection that adopts the structural framing rule.
- `.forge/notes/iter-6.md` -- prepended a `[NOTE added in iter 7]`
  block at the top retracting the two specific incorrect claims
  while preserving the historical body verbatim.

NOT actively edited this iter, but expected in `git status` because
they still differ from HEAD `93adda5`:
- `.forge/notes/iter-1.md`, `.forge/notes/iter-2.md`,
  `.forge/notes/iter-3.md`, `.forge/notes/iter-4.md`,
  `.forge/notes/iter-5.md` -- frozen archives from earlier iters of
  this post-merge cycle. Unchanged in iter 7.

Will appear at evaluator inspection time but NOT in the worktree
while I am writing these notes:
- `.forge/notes/iter-7.md` -- Forge's auto-archive of this very
  iter-notes.md file. Same content as this file at end of iter 7.

## Worktree state at iter-7 writing time

Verbatim `git --no-pager status --porcelain` captured immediately
before this iter ends:

```
 M .forge/iter-notes.md
 M .forge/notes/iter-1.md
 M .forge/notes/iter-2.md
 M .forge/notes/iter-3.md
 M .forge/notes/iter-4.md
 M .forge/notes/iter-5.md
 M .forge/notes/iter-6.md
```

Seven modified tracked files at iter-end. AT EVALUATOR INSPECTION
TIME this becomes EIGHT paths because Forge will materialize
`.forge/notes/iter-7.md` from this file between iter-end and
evaluator-start.

## Decisions made this iter

- Structural framing rule, not another word-tweak. The "(score N,
  iterate)" mistake has now happened twice in this workstream (iter
  4 about iter 3, iter 6 about iter 5). Repeating a minimal word
  tweak would risk a third occurrence. Adopting an explicit
  canonical phrasing rule (see "STRUCTURAL framing rule adopted in
  iter 7" above) means future iters have a single rule to follow.
- Defensive annotation on notes/iter-6.md instead of full rewrite.
  Same minimum-blast-radius pattern iter 5 used for notes/iter-4.md.
  The iter-6 narrative body remains historically accurate as a
  record of what iter 6 thought at the time; the annotation
  retracts only the two specific factual errors.
- No edits to other notes archives. They are LF + ASCII clean and
  the iter-6 evaluator already accepted them (matched the
  seven-file ground truth, no findings against them).

## Dead ends tried this iter

- None.

## Open questions surfaced this iter

- If the iter-7 evaluator finds a THIRD occurrence of the same
  framing mistake, the next iter (iter 8) should escalate as an
  Open Question for operator intervention. Current strategy is
  the structural framing rule; if that doesn't hold, the issue is
  systemic and needs an operator pin (e.g. "stop blocking on
  evaluator-verdict misquotes in audit narrative" or similar).

## Build / quality / test state at end of iter 7

Per-iter gate chain (re-verified at end of iter 7):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0, 323 tests pass
  (211 xraft-core + 112 xraft-storage). No Rust source touched in
  any iter of the post-merge cycle.
- `git --no-pager diff --check` -> exit 0, no whitespace problems.
  All files written via `[System.IO.File]::WriteAllText` with
  CRLF-to-LF normalization to avoid Windows line-ending issues.

## What's still left for future iters

- Stage 3.2 (Leader Election) is COMPLETE and merged upstream via
  PR #10 (`b266a71`). The iter-5 evaluator confirmed "no remaining
  Stage 3.2 implementation or current changed-file narrative
  issues found." The iter-6 evaluator's two findings were the
  same shape of narrative inaccuracy iter 5 had to fix; both are
  fixed in this iter via structural framing rule.
- Stage 3.3 (Log Replication) is the next workstream:
  `handle_fetch_request`, `handle_fetch_response`, leader-side
  per-peer progress updates, and `ClientPropose` handling on the
  leader. Lives on a different branch.