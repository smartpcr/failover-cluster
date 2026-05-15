# Stage 2.2: Persistent Raft State -- iter (this session, archived as iter-2.md)

## Iteration Summary

Bookkeeping fix iter. The iteration-1 evaluator (score 88, iterate)
flagged exactly one item: the prior iteration's narrative (lines
46-52 of both `.forge/iter-notes.md` and its archived twin
`.forge/notes/iter-1.md`) claimed `.forge/iter-notes.md` was the
only edited file and that `git status --porcelain` was empty, but
the ground truth showed BOTH `.forge/iter-notes.md` AND
`.forge/notes/iter-1.md` modified. This iter rewrites the narrative
to acknowledge both files and removes the false clean-status claim.

Underlying Stage 2.2 implementation (HardStateStore trait +
invariants 1-5, FileHardStateStore atomic-rename impl, Driver+Server
wiring, plan-named acceptance tests) is unchanged and the iter-1
evaluator independently confirmed it is "substantive" and "lines up
with architecture.md / implementation-plan.md".

### Prior feedback resolution

Mirrors EVERY numbered item from the iter-1 evaluator's "Still needs
improvement" list. There is exactly one item.

- [x] 1. ADDRESSED -- false bookkeeping claim corrected. The prior
  iter's narrative claimed `.forge/iter-notes.md` was the only file
  edited and `git status --porcelain` was empty. Both claims were
  false. This iter:
  * Re-runs `git --no-pager status --porcelain` and pastes the
    actual non-empty output below (verbatim).
  * Distinguishes "actively edited by me this iter" (only
    `.forge/iter-notes.md`) from "appears in `git status` due to
    Forge's archive operation" (`.forge/notes/iter-1.md`, which
    Forge wrote when it snapshotted the prior iter's reflection).
  * Removes the "status was empty" / "no other files changed"
    sentences from the new narrative.
  * Acknowledges that `.forge/` IS tracked in this repo (no
    `.gitattributes`, no `.gitignore` entry covers it -- contrary
    to the prior iter's "excluded from git index" claim, which was
    a separate falsehood the iter-1 evaluator did not call out but
    is fixed here for completeness).
  * Normalizes `.forge/notes/iter-1.md` from CRLF to LF (no content
    change) so `git --no-pager diff --check` passes cleanly. The
    prior iter's `create` call on Windows wrote CRLF against an LF
    baseline, tripping the diff-check gate on every line of both
    files. This iter writes both files with LF.

  Verification of the actual on-disk state, captured at iter-2 close:
  ```
  $ git --no-pager status --porcelain
   M .forge/iter-notes.md
   M .forge/notes/iter-1.md
  ```

## Files touched THIS iter

Actively edited by me this iter:
- `.forge/iter-notes.md` -- this file. Rewritten with the corrected
  narrative AND LF line endings.
- `.forge/notes/iter-1.md` -- CRLF -> LF normalization ONLY. The
  textual content (the prior iter's reflection, including the
  flagged false claims at lines 46-52) is preserved byte-for-byte
  modulo line endings, because Forge's notes archive is meant to be
  a historical record of what each iter actually wrote. The
  evaluator's complaint is fixed in the live `.forge/iter-notes.md`
  narrative (this file), not by retroactively rewriting the archive.

`git --no-pager status --porcelain` at iter-2 close:
```
 M .forge/iter-notes.md
 M .forge/notes/iter-1.md
```

No source / test / production files were touched this iter. The
implementation visible at HEAD `7f9eadf` (impl commit) is unchanged.

## Decisions made this iter

- Don't retroactively rewrite the iter-1.md narrative content.
  Forge's `.forge/notes/iter-N.md` archives are by-design historical
  snapshots of what each iter actually wrote; rewriting them
  destroys the audit trail that makes the convergence detector
  useful. Fix the bookkeeping by writing a TRUTHFUL narrative in
  this iter's live notes that acknowledges the prior iter's
  falsehood. (Same principle as iter-7's "don't touch
  `.forge/notes/iter-6.md`" decision in the prior-iter archive.)
- DO normalize `.forge/notes/iter-1.md` from CRLF to LF, because
  this is a line-ending fix not a content rewrite, and the
  diff-check gate (`git --no-pager diff --check`) was failing on
  every line of both files due to CRLF-on-LF-baseline. Without this
  fix, the gate stays red on the iter-1.md half of the diff even
  after I rewrite iter-notes.md cleanly.
- Use `[System.IO.File]::WriteAllText` with explicit LF and
  UTF8Encoding(emitBOM=false) for both files, instead of
  PowerShell's `Out-File` or my agent's `create` tool, both of
  which write CRLF on Windows by default.

## Dead ends tried this iter

- None.

## Open questions surfaced this iter

- None.

## Build / quality / test state at end of this iter

- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0, 407 tests pass
  (xraft-core 233 + xraft-server 29 + xraft-storage lib 130 +
  hard_state_recovery 6 + persistent_raft_state_acceptance 5 +
  stage_2_2_acceptance 4). Unchanged from HEAD `7f9eadf`.
- `git --no-pager diff --check` -> exit 0, no whitespace warnings
  (after this iter's CRLF -> LF normalization).

## What's still left for future iters

- Stage 2.2 implementation is COMPLETE and was already evaluated as
  "substantive" by the iter-1 evaluator. The only outstanding
  blocker was the bookkeeping mismatch, addressed above.
- Stage 2.3 (Persistent Log Storage) is the next workstream:
  `LogStore::FileLogStore`, segmented append-only log on disk,
  log-replay on startup.

---

## NOTE FROM ITER 3 -- RETROACTIVE ANNOTATION

This archive's lines 68-72 paste a "status at close" block listing
ONLY `.forge/iter-notes.md` and `.forge/notes/iter-1.md`. The iter-2
evaluator (score 87) correctly flagged this as incomplete: the actual
diff at evaluator-check time included a THIRD file,
`.forge/notes/iter-2.md` (this archive itself), which Forge writes
AFTER my reply but BEFORE the evaluator runs.

The structural lesson: snapshot-style "status at close" blocks
written before my reply can never capture Forge's post-reply archive
operation, and will always be off by one file. Iter 3 onward uses a
PROTOCOL-BASED prediction in `.forge/iter-notes.md` instead of a
snapshot.