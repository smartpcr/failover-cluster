# Stage 3.3: Log Replication -- iter 2

## Iteration Summary

Resolved all six numbered findings from the iter-1 evaluator (score 78,
verdict iterate). Two findings (1 + 2) were API-shape contract gaps;
one (3) was a test-semantics bug; three (4 + 5 + 6) were
replication-safety / leader-fencing edge cases in `handle_fetch_response`
and `handle_fetch_request`. All six are addressed structurally rather
than via wording tweaks. Three new acceptance tests demonstrate the
fence-fix behaviors of findings 4, 5, and 6 head-on, and the existing
`scenario_basic_replication` was rewritten to exercise the correct
two-round confirmed-offset arithmetic that finding 3 called out. Per-iter
gate chain (build, fmt --check, clippy -D warnings, test, diff --check)
is green; xraft-core test count rose from 221 to 224 (+3 new), and
xraft-storage stays at 112.

### Prior feedback resolution

- [x] 1. ADDRESSED -- Added `pub fn apply_committed(&mut self) -> Option<Action>`
  at `xraft-core/src/node.rs` (around line 1383). It is a thin public
  wrapper over the engine-internal `maybe_apply()` helper, so external
  drivers (Stage 4 server / replication driver) can advance the apply
  pointer at their own cadence as the impl-plan §3.3 requires. Verified
  via `grep -nE "pub fn apply_committed" xraft-core/src/node.rs` and
  the new `scenario_higher_term_fetch_response_processes_entries_after_stepdown`
  test exercises the same maybe_apply emission path through the public
  step API.

- [x] 2. ADDRESSED -- Unified the two prior action variants
  (`Action::ApplyToStateMachine(Vec<Entry>)` legacy + `Action::ApplyCommitted{from,to}`
  engine-pure) into a single `Action::ApplyToStateMachine { from: LogIndex, to: LogIndex }`
  in `xraft-core/src/message.rs` (around lines 100-117). The new variant
  matches the impl-plan §3.3 contract name AND keeps the engine I/O-free
  (driver reads entries via `LogStore::get_range(from, to+1)` rather than
  the engine cloning entries into the action payload). All 22
  `ApplyCommitted` references in `xraft-core/src/node.rs` were renamed
  in one shot via PowerShell `(Get-Content -Raw) -replace`. There is
  now exactly one apply-shaped action variant; verified via
  `grep -nE "ApplyCommitted|ApplyToStateMachine" xraft-core/src/`
  showing zero `ApplyCommitted` matches and a single
  `ApplyToStateMachine { from, to }` definition.

- [x] 3. ADDRESSED -- Rewrote `scenario_basic_replication` (around
  `xraft-core/src/node.rs:3434-3585`) to exercise two real fetch rounds
  with the correct confirmed-offset arithmetic. The test now does:
  round 1 -- follower issues `FetchRequest { fetch_offset: 1, last_fetched_epoch: 0 }`
  proving the follower has zero entries; driver feeds back
  `FetchRequestAcked { confirmed_offset: 0 }` (= req.fetch_offset - 1),
  asserts no commit advance (1-of-3 quorum). Round 2 -- follower issues
  `FetchRequest { fetch_offset: 2, last_fetched_epoch: 1 }` proving
  the follower now has the no-op at index 1; driver feeds back
  `FetchRequestAcked { confirmed_offset: 1 }`, asserts commit advances
  to 1 and `ApplyToStateMachine { from: 1, to: 1 }` is emitted. The
  bogus same-round shortcut from iter-1 (request fetch_offset=1, ack
  confirmed_offset=1 in the same round) is gone. The
  `Input::FetchRequestAcked` docstring in message.rs already specified
  this semantic; only the test was wrong.

- [x] 4. ADDRESSED -- `handle_fetch_response` no longer early-returns
  after a higher-term step-down. The handler now calls
  `actions.extend(self.become_follower(Term(resp.leader_epoch), Some(resp.leader_id)))`
  and falls through into the normal same-term reconciliation path so
  the response's entries, high watermark, and divergence hint get
  processed under the new term. Verified by the new
  `scenario_higher_term_fetch_response_processes_entries_after_stepdown`
  test, which sends a higher-term FetchResponse carrying a single
  entry + high_watermark=1 and asserts the node ends at term=3,
  leader_id=Some(2), last_log_index=1, commit_index=1, and emits
  `ApplyToStateMachine { from: 1, to: 1 }`. The prior behavior
  (silently dropping the payload) would have failed this assertion.

- [x] 5. ADDRESSED -- `handle_fetch_response` now fences the
  two-same-term-leaders case BEFORE any state mutation. After the
  higher-term branch (so a legitimate higher-term takeover still
  works), the handler checks two new guards: (a) drop if
  `self.role == NodeRole::Leader` (a same-term peer is not the
  authoritative leader for us); (b) drop if `self.leader_id == Some(known)`
  AND `known != resp.leader_id` (two same-term leaders is a Raft
  safety violation). Both drops return `Vec::new()` and CRITICALLY
  do NOT call `election_timer.reset()` -- a divergent claimant must
  not be able to suppress a genuine election timeout. The new
  `scenario_same_term_response_from_different_leader_dropped` test
  verifies the leader_id is preserved AND the election timer's
  elapsed counter is unchanged after the dropped response.

- [x] 6. ADDRESSED -- `handle_fetch_request` now drops requests from
  unknown replicas (around `xraft-core/src/node.rs:1428-1450`). After
  the existing self-fetch and stale-leader checks, the handler verifies
  `is_known_voter(replica_id) || self.peers.contains_key(&replica_id)`;
  when neither holds, the request is dropped silently with no
  ServeFetch action emitted. This mirrors the existing filter in
  `handle_fetch_request_acked` (line ~1700 area). Static observers
  (in `peers` map with `is_voter=false`) are still served; only
  totally unknown replica ids (no voter, no peer record) are dropped.
  The new `scenario_fetch_request_from_unknown_replica_dropped` test
  exercises this with replica_id=99 against a 3-voter cluster
  (voters: 1, 2, 3) and asserts no ServeFetch is emitted and no
  phantom peer record is created.

## Files touched THIS iter (iter 2)

Actively edited by me in iter 2:

- `xraft-core/src/message.rs` -- Replaced `Action::ApplyToStateMachine(Vec<Entry>)`
  legacy variant + `Action::ApplyCommitted { from, to }` engine-pure
  variant with a single `Action::ApplyToStateMachine { from: LogIndex, to: LogIndex }`
  carrying the engine-purity rationale in its docstring. Net change: -1
  variant.

- `xraft-core/src/node.rs` -- Six functional fixes in one file:
  (1) added `pub fn apply_committed()` wrapping `maybe_apply()`;
  (2) rewrote `maybe_apply()` to emit the unified action variant
  (also bulk-renamed all 22 `ApplyCommitted` refs across impl + tests
  + docstrings via `(Get-Content -Raw) -replace`);
  (3) rewrote `scenario_basic_replication` for true two-round
  confirmed-offset semantics;
  (4) `handle_fetch_response` no longer early-returns after
  higher-term `become_follower`;
  (5) added two-leaders fence in `handle_fetch_response` (drop if
  Leader, drop if known leader_id != resp.leader_id, both without
  resetting the election timer);
  (6) added unknown-replica filter in `handle_fetch_request`.
  Three new tests appended near the other Stage 3.3 scenarios:
  `scenario_higher_term_fetch_response_processes_entries_after_stepdown`,
  `scenario_same_term_response_from_different_leader_dropped`,
  `scenario_fetch_request_from_unknown_replica_dropped`.

- `.forge/iter-notes.md` -- this file. Iter-2 reflection. Written with
  LF line endings (the iter-1 archive was CRLF, which `git diff --check`
  flagged as trailing whitespace; iter 2 fixes this for both
  iter-notes.md and the iter-1 archive below).

- `.forge/notes/iter-1.md` -- defensive line-ending normalization.
  The iter-1 agent wrote iter-notes.md with CRLF (PowerShell default)
  and Forge file-copied it to notes/iter-1.md verbatim. `git diff --check`
  flagged every line as "trailing whitespace" because the repo treats
  `.md` as LF. Iter 2 normalizes this archive to LF in place; the
  narrative body is preserved byte-for-byte modulo line endings.

## Worktree state at iter-2 writing time

Verbatim `git --no-pager status --short` captured while writing
these notes:

```
 M .forge/iter-notes.md
 M .forge/notes/iter-1.md
 M xraft-core/src/message.rs
 M xraft-core/src/node.rs
```

4 paths total (4 modified, 0 untracked). At evaluator inspection time
this becomes 5 paths because Forge will materialize
`.forge/notes/iter-2.md` from this iter-notes.md file before the next
evaluator pass -- the structural +1 auto-archive pattern documented
in the cumulative iter-5 notes (Stage 3.2 prior workstream) continues
to hold for Stage 3.3 too. Policy statement: for every iter N, the
evaluator's inspection-time path count = the in-iter `git status --short`
line count + 1.

## Decisions made this iter

- All six findings are FIX (not DEFER). None of them require
  cross-workstream changes; all live in xraft-core and were caused
  by under-thought iter-1 implementation choices.

- The `Action::ApplyCommitted` removal is a hard rename, not an alias.
  An `Action::ApplyCommitted` -> `Action::ApplyToStateMachine` alias
  would let downstream code keep using the old name and silently
  drift from the impl-plan name. Since no production code outside
  xraft-core consumed the old variant (verified via
  `grep -rnE "ApplyCommitted|ApplyToStateMachine" --include='*.rs' .`
  before the rename), the only churn is internal to xraft-core and
  was bulk-renamed in one shot.

- The two-leaders fence (finding 5) is placed AFTER the higher-term
  branch, NOT before. Rationale: a legitimate higher-term takeover
  by a brand-new leader would otherwise trip the fence (our existing
  `leader_id` would not match the new leader's id at same-term
  evaluation time). By stepping down first and only then evaluating
  the fence, the fence becomes a no-op for legitimate higher-term
  takeovers (after step-down `leader_id == resp.leader_id`) but
  still trips for the bogus-same-term-claimant case.

- Both fence drops return `Vec::new()` and explicitly DO NOT touch
  the election timer. A divergent same-term claimant must not be
  allowed to suppress a real election timeout -- otherwise an
  attacker (or a buggy stale leader) could indefinitely starve a
  follower out of starting an election by sending periodic dropped
  responses.

- The `scenario_basic_replication` rewrite (finding 3) is a full
  rewrite to two-round semantics, NOT a one-line tweak. The iter-1
  test conflated `fetch_offset` (the next index the follower wants)
  with `confirmed_offset` (the highest index the follower already
  has) -- those are off-by-one. Patching the constant alone would
  hide the underlying confusion; rewriting the test with explicit
  round-1 and round-2 sections makes the two-round protocol legible
  in the test code itself.

- `scenario_commit_requires_majority` was NOT rewritten. That test
  feeds `Input::FetchRequestAcked { confirmed_offset: 1 }` directly
  to exercise the per-peer progress + quorum advance logic. The
  driver-derived semantics (ack reflects fetch_offset - 1 from a
  prior request round) is already valid for the values the test
  uses; exercising the FetchRequest -> ServeFetch -> FetchResponse
  -> FetchRequestAcked round-trip would just duplicate
  `scenario_basic_replication`'s coverage. Test scope kept narrow.

- iter-2 line-ending hygiene: wrote both iter-notes.md and the
  iter-1 archive with LF only. PowerShell's default Set-Content
  emits CRLF on Windows; iter 2 uses [IO.File]::WriteAllText with
  the LF-converted text to ensure `git diff --check` exits 0.
  This is the same line-ending discipline the cumulative iter-5
  notes (Stage 3.2 workstream) called out.

## Dead ends tried this iter

- None this iter. The fix designs were straightforward once the
  iter-1 evaluator findings pointed at the exact line ranges.

## Open questions surfaced this iter

- None. All six findings have been addressed within xraft-core; no
  cross-workstream coupling discovered.

## Build / quality / test state at end of iter 2

Per-iter gate chain (re-verified at end of iter 2):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0; xraft-core 224 passed
  (was 221 + 3 new this iter); xraft-storage 112 passed (unchanged);
  336 total non-zero test cases pass across the workspace.
- `git --no-pager diff --check` -> exit 0 (after iter-2's LF
  normalization of iter-notes.md and notes/iter-1.md).

## What's still left for future iters

- Stage 3.3 (Log Replication) engine scope is now complete: pull-based
  fetch / serve / response handling, follower append + truncate +
  HW propagation, leader per-peer progress tracking + majority commit
  advance, client propose, fetch-timer scheduling. All six iter-1
  evaluator findings resolved with structural fixes plus three
  demonstration tests.
- Stage 3.4 (next workstream) will likely wire the new `Action`
  variants (`ServeFetch`, `ApplyToStateMachine`, `TruncateLog`,
  `AppendEntries`) into the driver layer (xraft-server / xraft-client),
  giving the engine an actual runnable replication pipeline. That is
  out of scope for Stage 3.3.