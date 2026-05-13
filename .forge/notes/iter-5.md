# Stage 3.1: Raft Node State Machine — iter 5

## Iteration Summary

### Prior feedback resolution
The iter-4 evaluator review (score 94, iterate) listed:
> Still needs improvement:
> - [ ] 1. None — no remaining Stage 3.1 issues identified in the changed files reviewed.

- [x] 1. ADDRESSED (no-op) -- there were no numbered code/doc findings
  from the evaluator. The pass was rejected by the post-pass quality gate
  (`cargo fmt --check --all`), not by the evaluator. Operator's manual
  retry note explicitly says: *"Run `cargo fmt` in the worktree to fix
  formatting; the per-iter gate will catch any remaining issues before
  the next pass attempt."* That is what iter 5 does.

### Root cause of the demote-from-pass
The iter-3/iter-4 edits to `xraft-core/src/node.rs` (struct-variant
`Action::PersistHardState` `assert!` blocks, the multi-arm `match
self.role` in `handle_tick`, the `if lo == hi` collapse, the
`become_candidate` term-bump line, and the `use crate::message::{...}`
import grouping) were all rustfmt-non-conformant. `cargo build` did not
catch this; only `cargo fmt --check --all` does.

## Files touched THIS iter (iter 5)
- `xraft-core/src/node.rs` -- `cargo fmt --all` applied. ~302 lines of
  pure whitespace/style reflow inside the file
  (single-arm `assert!` calls expanded to multi-line builder form,
  combined `match` arm, single-line `if/else` collapse, import
  re-grouping, term-bump line collapsed to one line). NO behavioural
  change. No symbol renames. No tests added or removed.
- `.forge/iter-notes.md` -- this file (iter-5 reflection).

## Decisions made this iter
- **Run `cargo fmt --all` and nothing else.** The evaluator explicitly
  said no remaining issues. The operator note explicitly said the only
  problem is formatting. Adding any other change would be scope creep
  and risks introducing a NEW evaluator finding for a workstream that
  is otherwise at score 94.
- **Did NOT touch any logic, tests, comments, or docs.** Pure mechanical
  formatter pass. Verified by re-running clippy + tests after fmt.

## Dead ends tried this iter
- None. The remediation path was deterministic.

## Build / quality / test state at end of iter 5
The Phase 56 per-iter gate chain (build -> quality -> test):
- `cargo build --workspace` -> exit 0, "Finished `dev` profile ... 12.03s".
- `cargo fmt --check --all` -> exit 0, no diff output (was failing in iter 4).
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0, **289 tests pass** (177 xraft-core
  + 112 xraft-storage), unchanged from iter 4.

## git diff --stat (cumulative vs base, after fmt)
```
 xraft-core/src/lib.rs     |   15 +
 xraft-core/src/message.rs |   17 +-
 xraft-core/src/node.rs    | 1628 ++++++++++++++++++++++++++++++++++++++++++++--
 3 files changed, 1495 insertions(+), 165 deletions(-)
```
(node.rs grew ~250 lines vs iter 4 purely from the formatter expanding
single-line `assert!` and `match`/`if-else` constructs into multi-line
form; no semantic delta.)

## What's still left for future iters
- Stage 3.1 scope is fully implemented, evaluator-clean, and now
  fmt-clean. Awaiting the iter-5 evaluator + per-iter gate.
- Stage 3.2 (Leader Election) remains the next workstream. Same notes
  as iter 4 apply: vote-tally cascade through `Input::VoteResponse {
  from, response }` and PreVote -> Vote handler implementations.
