# Stage 3.3: Log Replication -- iter 3

## Iteration Summary

Resolved all three iter-2 evaluator findings (score 87, verdict iterate).
All three are edge-case validation gaps around malformed or untrusted
fetch traffic — exactly the safety-relevant trust-boundary checks the
evaluator flagged as "iterate once more". Each fix lands as a focused
guard before any state mutation, so a malformed sender can never bump
our term, overwrite our leader_id, corrupt our log, or consume leader
bandwidth. Three new direct regression tests demonstrate the fixes.
Per-iter gate chain (build, fmt --check, clippy -D warnings, test,
diff --check) is green; xraft-core test count rose from 224 to 227
(+3 new this iter), xraft-storage stays at 112.

### Prior feedback resolution

- [x] 1. ADDRESSED -- xraft-core/src/node.rs handle_fetch_response
  -- Added a leader-id membership guard at the very top of the
  function (right after the cluster_id check, BEFORE any state
  mutation including the higher-term step-down). The guard mirrors
  the existing finding-6 filter on handle_fetch_request:
  `if !self.is_known_voter(resp.leader_id) && !self.peers.contains_key(&resp.leader_id) { drop }`.
  This closes both attack vectors the evaluator highlighted: (a) the
  higher-term branch can no longer be tricked into adopting an
  unknown leader via `become_follower(Term, Some(unknown))` and
  bumping our term; (b) the same-term `if self.leader_id.is_none()`
  adopt path at the cited node.rs:1609-1635 lines can no longer
  install an unknown leader_id. New test
  `scenario_fetch_response_from_unknown_leader_dropped` exercises
  BOTH cases in one test (case (a): higher-term unknown leader at
  Term 5 from NodeId(99) — asserts term stays at 2, leader_id stays
  Some(2), election timer NOT reset; case (b): same-term Term 3
  unknown leader from NodeId(99) with leader_id=None pre-state —
  asserts leader_id stays None, election timer NOT reset).

- [x] 2. ADDRESSED -- xraft-core/src/node.rs handle_fetch_response
  non-diverging entries path -- Added a `for w in resp.entries.windows(2)`
  loop AFTER the existing `entries[0].index == expected_first` check
  that validates EVERY adjacent pair: `w[1].index == w[0].index + 1`.
  Any gap (e.g. `[1, 3]`) drops the entire response. As a defense-
  in-depth bonus, the same loop also rejects in-batch term regress
  (`w[1].term < w[0].term`) since term may only stay the same or
  grow within entries from a single leader epoch. The previous code
  would have appended a gapped batch wholesale, leaving the
  follower's log non-contiguous and violating Raft log-matching.
  New test `scenario_fetch_response_with_intra_batch_gap_dropped`
  exercises a `[entry(1, term=5), entry(3, term=5)]` batch and
  asserts no AppendEntries action, last_log_index unchanged at 0,
  commit_index unchanged at 0, no ApplyToStateMachine.

- [x] 3. ADDRESSED -- xraft-core/src/node.rs handle_fetch_request
  -- Added an `if req.fetch_offset == LogIndex(0) { drop }` guard
  right after the self-fetch check and BEFORE the unknown-replica
  filter. fetch_offset is the next 1-based log index the follower
  wants (architecture §5.2); 0 is structurally invalid because the
  driver derives confirmed_offset by subtracting one
  (`fetch_offset - 1`), and `LogIndex(0).0.checked_sub(1)` would
  underflow into u64::MAX and corrupt the leader's per-peer
  progress map. The empty-log case is correctly encoded as
  `fetch_offset = 1, last_fetched_epoch = 0`. New test
  `scenario_fetch_request_with_zero_offset_dropped` exercises a
  fetch_offset=0 request from the known voter NodeId(2) and asserts
  no actions emitted (in particular no ServeFetch) AND that the
  per-peer progress (last_fetch_offset, last_fetch_time) is
  unchanged — the drop happens before the peer-liveness update.

## Files touched THIS iter (iter 3)

Actively edited by me in iter 3:

- `xraft-core/src/node.rs` -- Three functional fixes in one file:
  (1) added unknown-leader guard at the top of `handle_fetch_response`
  (lines just after the cluster_id check; protects both higher-term
  and same-term branches);
  (2) added `windows(2)` contiguity + term-non-regress validation in
  the non-diverging entries path of `handle_fetch_response`;
  (3) added `fetch_offset == LogIndex(0)` rejection in
  `handle_fetch_request` before unknown-replica check.
  Three new tests appended near the other Stage 3.3 scenarios:
  `scenario_fetch_request_with_zero_offset_dropped`,
  `scenario_fetch_response_from_unknown_leader_dropped` (covers BOTH
  higher-term and same-term unknown-leader cases),
  `scenario_fetch_response_with_intra_batch_gap_dropped`.

- `.forge/iter-notes.md` -- this file. Iter-3 reflection. Written
  with LF line endings (the iter-1 + iter-2 archives are also
  normalized to LF in this iter — see below).

- `.forge/notes/iter-1.md` -- still LF-normalized from iter 2; no
  fresh edit needed this iter, but the file remains in the worktree
  delta because the iter-2 normalization pass that converted CRLF
  to LF has not yet been committed by Forge. Defensive re-check at
  end of iter 3 confirms CR-bytes = 0.

- `.forge/notes/iter-2.md` -- the Stage 3.3 iter-2 reflection that
  Forge auto-archived from `.forge/iter-notes.md` between the iter-2
  agent run and this iter-3 agent run. The committed-tree version
  was the old Stage 3.2 iter-2 content; Forge replaced it with the
  Stage 3.3 iter-2 content. This is normal Forge archival behavior
  and not a manual edit by me this iter. Verified CR-bytes = 0.

- `xraft-core/src/message.rs` -- still in the worktree delta from
  iter 2 (the unified `Action::ApplyToStateMachine { from, to }`
  variant); not touched this iter.

## Worktree state at iter-3 writing time

Verbatim `git --no-pager status --short` captured while writing
these notes:

```
 M .forge/iter-notes.md
 M .forge/notes/iter-1.md
 M .forge/notes/iter-2.md
 M xraft-core/src/message.rs
 M xraft-core/src/node.rs
```

5 paths total (5 modified, 0 untracked). At evaluator inspection
time this becomes 6 paths because Forge will materialize
`.forge/notes/iter-3.md` from this iter-notes.md file before the
next evaluator pass — the structural +1 auto-archive pattern
documented in the cumulative iter-5 (Stage 3.2) notes continues to
hold for Stage 3.3. Policy statement: for every iter N, the
evaluator's inspection-time path count = the in-iter
`git status --short` line count + 1.

## Decisions made this iter

- All three findings are FIX (not DEFER). All three live in
  xraft-core and require zero cross-workstream coupling.

- Finding 1's guard is placed at the VERY TOP of
  `handle_fetch_response`, before the higher-term branch. Rationale:
  if we put it inside the same-term branch only, an unknown sender
  could still force a term bump by sending a higher-term response
  (the higher-term branch runs first and would call
  `become_follower(Term(higher), Some(unknown))` before the guard
  could fire). Placing the guard above both branches makes the
  unknown-leader drop unconditional and eliminates the race entirely.

- Finding 2's loop also rejects in-batch term-regress
  (`w[1].term < w[0].term`) on top of the index-contiguity check
  the evaluator asked for. Rationale: defense in depth. Within a
  single FetchResponse from a single leader epoch, terms must be
  non-decreasing (a leader cannot create entries with a smaller
  term than its own). Catching this here is one extra line and
  closes a related malformed-batch path. The evaluator did not
  require it but the symmetry felt valuable.

- Finding 3's guard is placed AFTER the self-fetch check (so a
  self-fetch with offset=0 still hits the self-fetch drop and
  doesn't generate two log lines) but BEFORE the unknown-replica
  check (so a malformed-but-unknown sender is rejected on the
  cheaper structural check first). The guard runs BEFORE the
  per-peer liveness update so a malformed request cannot refresh
  the leader's last_fetch_time stamp.

- Re-used the existing `is_known_voter || peers.contains_key`
  predicate shape (already used in `handle_fetch_request` from
  iter 2's finding-6 fix) for the new finding-1 guard. Symmetry
  between request and response handlers makes the trust boundary
  legible: requests AND responses both require known sender.

- No changes to message.rs this iter. The `Action` and `Input`
  variant shapes are correct and stable as of iter 2.

- Test `scenario_fetch_response_from_unknown_leader_dropped`
  collapses both higher-term and same-term unknown-leader cases
  into one test function (separated by `// ---------- Case (a/b)
  ----------` comment markers). Rationale: they both verify the
  SAME guard (the new top-of-function leader-id check), so a single
  test exercising both cases is more legible than two near-duplicate
  tests, and a regression in either case will fail this single test.

## Dead ends tried this iter

- None. All three fix designs were straightforward once the
  iter-2 evaluator findings pinpointed the exact line ranges and
  semantics.

## Open questions surfaced this iter

- None. All three findings have been addressed within xraft-core;
  no cross-workstream coupling discovered.

## Build / quality / test state at end of iter 3

Per-iter gate chain (re-verified at end of iter 3):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0 (after one auto-fmt pass to
  collapse a multi-space comment alignment that rustfmt wanted to
  rewrite).
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0; xraft-core 227 passed
  (was 224 + 3 new this iter); xraft-storage 112 passed (unchanged);
  339 total non-zero test cases pass across the workspace.
- `git --no-pager diff --check` -> exit 0 (all .forge markdown
  files remain LF-clean; defensive re-check at end of iter 3).

## What's still left for future iters

- Stage 3.3 (Log Replication) engine is now complete and battle-
  hardened against malformed / untrusted fetch traffic. Six iter-1
  findings + three iter-2 findings = nine total findings, all
  resolved with structural fixes plus six demonstration tests
  (three from iter 2, three from iter 3).
- Stage 3.4 (next workstream) will likely wire the new `Action`
  variants (`ServeFetch`, `ApplyToStateMachine`, `TruncateLog`,
  `AppendEntries`) into the driver layer (xraft-server / xraft-client),
  giving the engine an actual runnable replication pipeline. That
  remains out of scope for Stage 3.3.