# Stage 2.3 Snapshot Store -- iter 1 (corrected post-hoc in iter 2)

## What this archive actually is

This file is the iter-1 archive of a freshly-restarted Snapshot
Store workstream attempt. Forge auto-archives the prior iter's
`.forge/iter-notes.md` to `.forge/notes/iter-N.md` between
iters; iter 1 of the fresh attempt was archived here.

## What iter 1 actually did

Iter 1 was a no-op iter that incorrectly self-identified as
`iter 7` because the prior-iter notes archive prepended into
the iter-1 prompt carried over from a STALE earlier workstream
attempt that had reached iter 5 / iter 6. The stale headers
misled the iter-1 agent into continuing that numbering.

Iter 1 produced a 61-line `.forge/iter-notes.md` containing
two false claims that the iter-1 evaluator (score 87) flagged:

1. It claimed `git status --short` was empty / worktree clean,
   but `M .forge/iter-notes.md` and `M .forge/notes/iter-1.md`
   were already in the diff at evaluator-inspection time.
2. It claimed the auto-archive path would be
   `.forge/notes/iter-7.md`, but Forge actually archived to
   `.forge/notes/iter-1.md` (this file).

No code, test, or planning-doc edit happened in iter 1; the
substantive snapshot-store surface (commit `997badd`) was
already complete and the iter-1 evaluator confirmed it remains
substantive.

## Why this archive is now short

The original iter-1 archive was a verbatim copy of the iter-1
`iter-notes.md` (the iter-7-titled narrative). The iter-2
agent (this rewrite) replaced that copy with the brief honest
record above, per the iter-1 evaluator's item 3:
"the iter-1 archive ... duplicates live iter notes instead of
preserving iter-1 history." Preserving a verbatim copy of the
flawed live notes adds zero historical value; preserving an
honest summary of what iter 1 actually did is the audit-trail
content that matters.

The iter-2 live `iter-notes.md` written alongside this fix
contains a `### Prior feedback resolution` block that addresses
all three iter-1 evaluator items.
