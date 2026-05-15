# Stage 3.3: Log Replication -- iter 7 (Stage 3.3 implementation iter 1)

## Iteration Summary

First iter of the new Log Replication workstream. Scaffolded the engine
side of KRaft pull-based replication into `xraft-core`: leader-side
`handle_fetch_request` + `handle_fetch_request_acked`, follower-side
`handle_fetch_response` (with divergence resolution and election-timer
reset), `handle_client_propose`, `try_advance_commit_index` (with
Figure-8 no-op gate), and `maybe_apply` (range-form `ApplyCommitted`
emission). Added 10 new tests; full workspace gate chain green.

### Prior feedback resolution

- [x] 1. ADDRESSED (no-op carry-over) -- The iter-6 evaluator carried
  Stage 3.2 forward as "None -- no remaining issues". Stage 3.2 stayed
  byte-identical for the entire iter-3..iter-6 sequence; the outstanding
  iter-5 finding ("None") was already marked `[x] ADDRESSED (no-op)` in
  iter 6. iter 7 starts the Stage 3.3 workstream proper, so the audit
  trail moves on to the new acceptance criteria below.

## Files touched THIS iter (iter 7)

Actively edited by me in iter 7:
- `xraft-core/src/message.rs` -- added `Input::FetchRequestAcked
  { replica_id, confirmed_offset }`, `Action::ApplyCommitted { from, to }`
  (range form, both ends inclusive), `Action::ServeFetch { ... }`
  (self-contained envelope so the driver can serialise FetchResponse
  without re-reading engine state), `Action::TruncateLog
  { from_index_inclusive }`.
- `xraft-core/src/node.rs` -- added two new `RaftNode` fields
  (`leader_no_op_index`, `last_fetch_tick`); restructured `handle_tick`
  so fetch scheduling fires before the election-timeout check and
  before the no-op gate; added helpers `fetch_interval_ticks` and
  `maybe_build_fetch_request` (eager-fire when `last_fetch_tick`
  is `None`); updated all four `become_*` transitions to manage the
  two new fields; implemented the four Stage 3.3 handlers + the two
  helpers (`try_advance_commit_index`, `maybe_apply`) + a public
  `apply_committed()` alias; added 10 scenario tests in `mod tests`.

Will appear at evaluator inspection time (Forge auto-archives this
iter-notes.md to `notes/iter-7.md` after iter end -- structural
+1 path-count; documented in iter 5 / iter 6).

## Decisions made this iter

- `Action::ServeFetch` is self-contained (carries cluster_id,
  leader_epoch, leader_id, high_watermark, fetch_offset,
  last_fetched_epoch). Engine never reads log entries; driver
  materialises the FetchResponse and performs divergence detection
  via `LogStore::term_at(fetch_offset - 1)`.
- `Input::FetchRequestAcked { replica_id, confirmed_offset }` is the
  ONLY path that updates `peer.last_fetch_offset`. Receipt of a raw
  `FetchRequest` updates `peer.last_fetch_time` but NOT
  `last_fetch_offset` -- otherwise a divergent follower could inflate
  quorum (rubber-duck blocking issue #1, Raft safety invariant).
- `Action::ApplyCommitted { from, to }` is a range; engine bumps
  `last_applied = to` optimistically. Driver MUST apply or halt and
  recover from durable state on restart.
- Divergence handler does NOT mutate `last_log_index/term` -- driver
  truncates and then calls `set_last_log` (rubber-duck blocking #3).
- Same-term valid `FetchResponse` makes Candidate / PreCandidate step
  down to Follower with leader hint; an existing Follower without
  `leader_id` adopts the hint (rubber-duck blocking #5).
- Leader cascading: `become_leader` calls `try_advance_commit_index` +
  `maybe_apply` so a single-voter cluster commits the no-op
  immediately. `handle_client_propose` does the same so single-voter
  client writes commit in one step.
- `maybe_build_fetch_request` returns `Some(...)` whenever
  `last_fetch_tick` is `None` -- a fresh follower fetches eagerly on
  the next tick rather than idling for one full `fetch_interval_ms`.
  This matches the doc-string intent of the
  `last_fetch_tick = None` reset on every `become_*` transition.

## Dead ends tried this iter

- Initial pass declared a private `build_fetch_request` helper inside
  the Stage 3.3 handler block that duplicated the existing
  `maybe_build_fetch_request`. Removed before the build to avoid the
  `dead_code` warning (clippy `-D warnings` would have failed).
- First pass at `scenario_tick_schedules_follower_fetch` failed
  because `maybe_build_fetch_request` required a full
  `fetch_interval_ticks` window before firing even when
  `last_fetch_tick` was `None`. Fixed by eager-fire short-circuit
  rather than re-shaping the test (the test encodes the documented
  contract).

## Open questions surfaced this iter

- None. The five rubber-duck blocking issues raised during the
  pre-implementation design pass were all adopted; no further design
  ambiguity surfaced during implementation.

## Build / quality / test state at end of iter 7

Per-iter gate chain (verified at end of iter 7):

- `cargo build --workspace` -> exit 0.
- `cargo fmt --check --all` -> exit 0, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0, 333 tests pass
  (221 xraft-core, 112 xraft-storage; +10 new in xraft-core).
- `git --no-pager diff --check` -> exit 0, no output.

## Worktree state at iter-7 writing time

Verbatim `git --no-pager status --short` captured at iter-7 writing
time:

```
 M xraft-core/src/message.rs
 M xraft-core/src/node.rs
```

2 paths in the worktree right now. At evaluator inspection time this
becomes 3 paths because Forge will materialize `.forge/notes/iter-7.md`
from this iter-notes.md file before the next evaluator pass (the
+1 auto-archive structural pattern documented in iter 5).

## What's still left for future iters

- Stage 3.3 engine-side handlers + tests are landed and green. Stage
  3.3 is functionally complete from the engine's perspective.
- Driver-side wiring of the new `Action::{ServeFetch, ApplyCommitted,
  TruncateLog}` variants and the new `Input::FetchRequestAcked` feedback
  in `xraft-server` will land in a later workstream alongside
  application-state-machine + transport integration. This is intentional
  -- the workstream brief lists only `xraft-core` files in scope.
- Stage 3.4 (Snapshots) is the next workstream:
  `handle_fetch_snapshot_request`, `handle_fetch_snapshot_response`,
  snapshot-on-log-compaction policy, leader-side snapshot dispatch,
  follower-side restore-from-snapshot.
