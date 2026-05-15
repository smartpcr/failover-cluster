# Stage 3.2: Leader Election -- iter 1

> NOTE: This archive was defensively overwritten in iter 3 to (a) drop
> a stale `min_ticks()` design claim that iter 2 superseded and (b)
> correct a misleading single-file diff stat that became stale once
> iter 2 expanded the change set. The iter-1 narrative shape is
> preserved; the corrected text below describes what iter 1 actually
> delivered AND points forward to the iter-2 design change so the
> archive does not contradict the current code.

## Workstream switch

- Stage 3.1 (Raft Node State Machine) merged via PR #9 (HEAD: 9ebab80).
- This worktree is a fresh start for Stage 3.2 -- no prior in-flight
  changes from iter-5 of Stage 3.1 to carry forward. The "Notes from
  prior iterations" archive refers to Stage 3.1's iter 1-5 and is
  retained for architectural continuity, not for resumable state.

## Files touched this iter (iter 1)

- `xraft-core/src/node.rs` (+1215 / -20):
  - Added field `last_leader_contact_tick: Option<u64>` to `RaftNode`,
    maintained by `become_follower(_, Some)` / `become_leader` (set);
    `become_pre_candidate` / `become_candidate` / `become_follower(_, None)`
    (clear).
  - Updated `step()` to route `Input::{VoteRequest, VoteResponse,
    PreVoteRequest, PreVoteResponse}` to their new handlers (was no-op).
  - Added Stage 3.2 handlers on `RaftNode`:
    - `handle_vote_request(req) -> Vec<Action>` -- cluster-id +
      voter-set validation; coalesced `PersistHardState` covering
      both term bump and `voted_for` set; emits `StepDown` when
      relevant; resets election timer on grant.
    - `handle_vote_response(from, resp)` -- strict term equality +
      role check; HashSet dedupe via `votes_received`; cascade to
      `become_leader` on quorum.
    - `handle_pre_vote_request(req)` -- pure-function reply, no
      durable-state mutation; rejects when `leader_recently_active()`.
    - `handle_pre_vote_response(from, resp)` -- counts grants
      regardless of `resp.term` value (lagging voters can grant);
      cascade to `become_candidate` on quorum.
  - Added helpers: `is_known_voter`, `candidate_log_is_up_to_date`,
    `leader_recently_active`, `build_vote_response`,
    `build_pre_vote_response`.
  - +34 new unit tests (211 total in xraft-core, was 177); fixtures
    `vote_req`/`pre_vote_req`/`vote_resp`/`pre_vote_resp` plus a
    `five_voter_config()` builder for 5-node quorum scenarios.
- `.forge/iter-notes.md` -- iter-1 reflection (archived to this file
  at end of iter 1).

## Decisions made this iter (informed by rubber-duck critique)

- Pre-Vote lease via a separate field, NOT the election timer.
  The rubber-duck flagged that `!election_timer.is_expired()` is
  brittle because granting a vote resets the timer (a
  leader-independent event). Added `last_leader_contact_tick:
  Option<u64>` updated only on real leader-contact transitions.
  `leader_recently_active()` consults this with a threshold equal
  to the receiver's current randomized election timeout
  (`election_timer.timeout_ticks()` -- see note below on the
  iter-2 design refinement).

  [Note added in iter 3] Iter 1 originally implemented this
  threshold as `election_timer.min_ticks()` (the lower bound of the
  randomized timeout range) for a conservative bias toward
  disruption prevention. Iter 2 revisited this against the literal
  architecture rule "within the election timeout" and changed the
  threshold to `election_timer.timeout_ticks()` (the receiver's
  current full randomized timeout). The current code uses
  `timeout_ticks()`; see `.forge/iter-notes.md` (iter-2 reflection,
  archived) for the rationale and the
  `handle_pre_vote_request_grants_after_lease_expires` test that
  asserts the new threshold.

- Voter-set membership check on senders. Rubber-duck blocking #2:
  drop vote/pre-vote requests from non-voter `candidate_id` and
  ignore responses from non-voter `from` (including non-voter
  higher-term responses -- those must NOT force a step-down).

- Coalesced `PersistHardState` for higher-term `VoteRequest`.
  Instead of calling `become_follower` (which emits its own
  `PersistHardState`) and then setting `voted_for` (another
  `PersistHardState`), the handler inlines the role transition so a
  single coalesced `PersistHardState` covers both mutations. Test
  `handle_vote_request_steps_down_on_higher_term_as_leader` asserts
  exactly one `PersistHardState` in the action list.

- Pre-Vote response counts lower-term grants. Rubber-duck blocking
  #3: a lagging follower at a lower term can legitimately grant a
  pre-vote (pre-vote responders don't bump term). The role check
  (`role == PreCandidate`) plus `become_pre_candidate` clearing
  `pre_votes_received` at round start bounds stale-grant risk; the
  real-vote phase enforces strict term equality.

- Stepping down on a higher-term Pre-Vote *response* IS correct.
  This is term reconciliation (the cluster moved on) -- not term
  inflation (which Pre-Vote guards against). Only accepted from
  known voters.

- Cluster-id mismatch: drop silently (no response). Matches KRaft
  semantics; observability via tracing::debug.

## Dead ends tried this iter

- None -- the rubber-duck pass caught the brittle
  election-timer-as-lease proxy before implementation, saving an
  evaluator round.

## Build / quality / test state at end of iter 1

- `cargo build --workspace` -> exit 0, 2.22s.
- `cargo fmt --check --all` -> exit 0 (after one `cargo fmt --all` to
  reflow newly-added handlers; same lesson as Stage 3.1 iter 5).
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0.
- `cargo test --workspace` -> exit 0, 323 tests pass (211 xraft-core,
  was 177 = +34 new Stage 3.2 tests; 112 xraft-storage unchanged).

## git diff --stat (iter-1 scope only)

```
 xraft-core/src/node.rs | 1235 ++++++++++++++++++++++++++++++++++++++
 1 file changed (iter-1 scope), 1215 insertions(+), 20 deletions(-)
```

[Annotation added in iter 3] The single-file diff above describes the
end-of-iter-1 cumulative state. Iter 2 added edits to
`xraft-core/src/lib.rs` (re-exported `VoteGrantedSet`) and
`xraft-core/src/types.rs` (new `VoteGrantedSet` newtype), so by the
time iter 2 was evaluated the cumulative diff covered three source
files plus the two markdown notes. The iter-notes.md current at any
given iter (and at end of iter 2 the file describes the iter-2 totals,
at end of iter 3 the iter-3 totals) is the authoritative source for
the current cumulative diff stat.

## What's still left for future iters

- Stage 3.2 scope is fully implemented. Awaiting evaluator pass.
- Stage 3.3 (Log Replication) will:
  - Wire `last_leader_contact_tick` updates on `Input::FetchResponse`
    handling so the Pre-Vote lease check stays accurate in live
    clusters.
  - Implement `handle_fetch_request` / `handle_fetch_response` for
    pull-based replication.
  - Implement `ClientPropose` handling on the leader.
