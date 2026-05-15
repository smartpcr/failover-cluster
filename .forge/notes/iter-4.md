# Stage 3.3: Log Replication -- iter 4

## Iteration Summary

Resolved the single iter-3 evaluator finding (score 89, verdict
iterate). The fix is symmetric to the iter-3 finding-1 fix on
`handle_fetch_response`: the unknown-replica trust-boundary guard
in `handle_fetch_request` was placed AFTER the higher-term
reconciliation branch, so an unknown same-cluster replica with
`leader_epoch > current_term` could still reach
`become_follower(Term(req.leader_epoch), None)` and force a stepdown.
This iter moves the guard to the very top of the function (right
after cluster_id and self-fetch checks, BEFORE the higher-term
branch) so unknown senders cannot mutate any state — including term,
role, leader_id, the election timer, or per-peer liveness fields.
A new direct regression test pins the new ordering. Per-iter gate
chain (build, fmt --check, clippy -D warnings, test, diff --check)
is green; xraft-core test count rose from 227 to 228 (+1 new this
iter), xraft-storage stays at 112.

### Prior feedback resolution

- [x] 1. ADDRESSED -- xraft-core/src/node.rs handle_fetch_request
  -- Reordered the function's defensive checks so the unknown-replica
  guard runs BEFORE the higher-term reconciliation branch. New order:
  (1) cluster_id check; (2) is_self drop (also moved up so a
  malformed self-loopback with bogus higher leader_epoch can never
  step ourselves down); (3) unknown-replica drop (NEW POSITION);
  (4) higher-term step-down (now reachable only for known senders);
  (5) not-leader drop; (6) fetch_offset == 0 drop; (7) per-peer
  liveness update + ServeFetch. Mirrors the symmetric ordering
  the iter-3 fix established for handle_fetch_response (cluster ->
  unknown-leader -> higher-term reconciliation). Unknown senders can
  no longer trip `become_follower(Term(req.leader_epoch), None)` to
  force a stepdown / term bump. New test
  `scenario_unknown_replica_higher_term_fetch_request_cannot_force_stepdown`
  exercises a NodeId(99) (not in voter_set {1,2,3}, not in peers)
  request carrying leader_epoch=10 against a leader at term 2 and
  asserts: no actions emitted, no PersistHardState, term stays 2,
  role stays Leader, leader_id unchanged, election timer NOT reset,
  no phantom peer record, other peers' liveness untouched.

  Symmetry verification (the trust-boundary check now runs first in
  BOTH handlers):
  - handle_fetch_request: cluster_id -> is_self -> unknown-replica
    -> higher-term -> not-leader -> fetch_offset==0 -> serve.
  - handle_fetch_response: cluster_id -> unknown-leader -> higher-term
    -> two-leaders fence -> ... -> apply.

## Files touched THIS iter (iter 4)

Actively edited by me in iter 4:

- `xraft-core/src/node.rs` -- One functional reorder in
  `handle_fetch_request`: moved `is_self` and the
  `is_known_voter || peers.contains_key` guard ABOVE the higher-term
  step-down branch. Updated the leading doc-comment hooks for
  steps 1-6 in the handler body. Net code delta is small (block-
  reorder + tracing-level upgrade from `debug` to `warn` for the
  unknown-replica drop, matching the response-side guard).
  One new test appended near the other Stage 3.3 scenarios (between
  `scenario_fetch_request_from_unknown_replica_dropped` and
  `scenario_fetch_request_with_zero_offset_dropped`):
  `scenario_unknown_replica_higher_term_fetch_request_cannot_force_stepdown`.

- `.forge/iter-notes.md` -- this file. Iter-4 reflection. Written
  with LF line endings.

- `.forge/notes/iter-1.md` -- still LF-normalized from iter 2; no
  fresh edit this iter, file remains in worktree delta because the
  iter-2 normalization pass has not yet been committed by Forge.
  Defensive re-check at end of iter 4 confirms CR-bytes = 0.

- `.forge/notes/iter-2.md` -- the Stage 3.3 iter-2 reflection
  Forge auto-archived from iter-notes.md between the iter-2 and
  iter-3 agent runs. Not touched this iter; CR-bytes = 0.

- `.forge/notes/iter-3.md` -- the Stage 3.3 iter-3 reflection
  Forge auto-archived from iter-notes.md between the iter-3 and
  iter-4 agent runs. Not touched this iter; CR-bytes = 0.

- `xraft-core/src/message.rs` -- still in the worktree delta from
  iter 2 (the unified `Action::ApplyToStateMachine { from, to }`
  variant); not touched this iter.

## Worktree state at iter-4 writing time

Verbatim `git --no-pager status --short` captured while writing
these notes:

```
 M .forge/iter-notes.md
 M .forge/notes/iter-1.md
 M .forge/notes/iter-2.md
 M .forge/notes/iter-3.md
 M xraft-core/src/message.rs
 M xraft-core/src/node.rs
```

6 paths total (6 modified, 0 untracked). At evaluator inspection
time this becomes 7 paths because Forge will materialize
`.forge/notes/iter-4.md` from this iter-notes.md file before the
next evaluator pass — the structural +1 auto-archive pattern
documented in the cumulative iter-5 (Stage 3.2) notes continues
to hold for Stage 3.3. Policy statement: for every iter N, the
evaluator's inspection-time path count = the in-iter
`git status --short` line count + 1.

## Decisions made this iter

- Chose to also move `is_self` ABOVE the higher-term branch (not
  just the unknown-replica guard the evaluator asked about).
  Rationale: a malformed self-loopback FetchRequest carrying a bogus
  higher leader_epoch could otherwise step ourselves down. Self
  never legitimately sends fetch requests to itself in normal
  operation, so a higher-epoch self-fetch is always either a bug
  or an attack. Dropping it before any state mutation is strictly
  safer and one extra moved check.

- Upgraded the unknown-replica drop's tracing level from `debug` to
  `warn`. Rationale: this is now a security-boundary check (an
  unknown sender attempting to disrupt leadership), not just a
  routine "stale-config sender" diagnostic. The response-side
  unknown-leader guard already uses `warn`; symmetry preserved.

- The new test exhaustively asserts the no-mutation contract:
  no actions, no PersistHardState (which become_follower would
  emit), term unchanged, role unchanged, leader_id unchanged,
  election timer NOT reset, no phantom peer record, OTHER peers'
  liveness fields untouched. Covers every observable side effect
  the previous broken ordering could have produced.

- No changes to `handle_fetch_response`: that handler's ordering
  was already correct as of iter 3 (cluster -> unknown-leader ->
  higher-term -> two-leaders-fence). The iter-3 evaluator
  independently verified this.

- No changes to message.rs or any other non-node.rs source. The
  fix is purely a reorder + tracing-level tweak inside the
  handle_fetch_request function body.

## Dead ends tried this iter

- None. The fix design was straightforward once the iter-3
  evaluator pinpointed the exact ordering gap.

## Open questions surfaced this iter

- None. The Stage 3.3 trust-boundary surface (request + response
  unknown-sender guards, two-leaders fence, malformed-offset drop,
  intra-batch contiguity validation) is now closed and symmetric.

## Build / quality / test state at end of iter 4

Per-iter gate chain (re-verified at end of iter 4):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0; xraft-core 228 passed
  (was 227 + 1 new this iter); xraft-storage 112 passed (unchanged);
  340 total non-zero test cases pass across the workspace.
- `git --no-pager diff --check` -> exit 0 (all .forge markdown
  files remain LF-clean; defensive re-check at end of iter 4).

## What's still left for future iters

- Stage 3.3 (Log Replication) engine is now complete with all
  ten cumulative findings (six from iter 1, three from iter 2,
  one from iter 3) resolved with structural fixes plus seven
  demonstration tests (three from iter 2, three from iter 3, one
  from iter 4). The trust-boundary check ordering is symmetric
  between request and response handlers.
- Stage 3.4 (next workstream) will likely wire the new `Action`
  variants (`ServeFetch`, `ApplyToStateMachine`, `TruncateLog`,
  `AppendEntries`) into the driver layer (xraft-server /
  xraft-client) for an actual runnable replication pipeline.
  That remains out of scope for Stage 3.3.