# Stage 3.1: Raft Node State Machine — iter 3 (REWRITTEN in iter 4)

> NOTE: Iter-4 evaluator flagged this archive as stale (it had been
> auto-archived with the iter-2 narrative, NOT the iter-3 one). The text
> below has been overwritten in iter 4 to accurately reflect what
> happened in iter 3.

## Evaluator iter-2 verdict (score 86, iterate)
3 numbered findings, all addressed in iter 3.

## Files touched in iter 3
- `xraft-core/src/node.rs` — substantial restructuring of `handle_tick`,
  `become_pre_candidate`, `become_candidate`, `become_leader`, plus
  `RaftNode::new` / `new_with_seed` signatures. Added 7 new tests; rewrote
  2 affected scenario tests. Renamed `PeerState` fields (3 sites).

## Decisions made in iter 3 (NEW or REVISED from earlier iters)
- **REVISED iter-1 decision: Pre-Vote routing.** Iter 1 deliberately
  routed Follower-Tick directly to `become_candidate` to satisfy the
  literal Stage 3.1 acceptance scenario "election-timeout-triggers-
  candidacy". The evaluator pointed out this conflicts with
  `architecture.md` §5.1 + `e2e-scenarios.md` Feature 3 which require
  Pre-Vote first. Re-routed Follower → `become_pre_candidate` (no term
  bump, sends `PreVoteRequest`s). `start_election` now delegates to
  `become_pre_candidate`.
- **NEW: Candidate Tick falls back to Pre-Vote**, not direct re-election.
  Rubber-duck pointed out a Candidate that loses contact would otherwise
  keep inflating the term, defeating Pre-Vote's purpose. Same routing
  for PreCandidate Tick (restart Pre-Vote).
- **NEW: Single-voter cascade through Pre-Vote.** `become_pre_candidate`
  now checks `has_pre_election_quorum()` after self-vote and chains into
  `become_candidate` (which already cascades to `become_leader`).
  Without this a 1-voter cluster could never elect under the new routing.
- **REVISED iter-1 decision: Constructor signatures.** Iter-1's
  `.ok().flatten()` on `build_voter_set` silently masked config errors
  (evaluator finding #2). Changed `RaftNode::new` and `new_with_seed`
  to return `Result<Self, XRaftError>` and added `config.validate()?`
  for defense-in-depth. Updated all 23 in-file test sites with `.unwrap()`.
- **REVISED iter-1 decision: `PeerState` field names.** Renamed
  `last_fetch_tick` → `last_fetch_time` and `last_caught_up_tick` →
  `last_caught_up_time` to match `implementation-plan.md:138` and
  `architecture.md:283-292`. The type stays `u64` (logical-tick count);
  doc comment explains the engine is I/O-free and uses `Input::Tick` as
  its monotonic time source — the engine's equivalent of the
  architecture doc's `Instant`.
- **NEW: Vote-tally hygiene.** `become_pre_candidate` clears
  `votes_received`; `become_candidate` clears `pre_votes_received`;
  `become_leader` clears both. Caught by rubber-duck pre-implementation.

## New tests added in iter 3 (7)
- `pre_candidate_promotes_to_candidate_on_quorum`
- `pre_candidate_election_timeout_restarts_pre_vote`
- `candidate_election_timeout_falls_back_to_pre_vote`
- `new_returns_err_on_invalid_voter_directory_id`
- `new_succeeds_on_valid_voter_config`
- `become_candidate_clears_stale_pre_votes`
- `become_leader_clears_vote_tallies`

## Tests rewritten in iter 3 (2)
- `election_timeout_triggers_candidacy` — now asserts Follower→PreCandidate
  with no term bump.
- `election_timeout_resets_after_role_change` — now asserts PreCandidate.

## Build / test state at end of iter 3
- `cargo build --workspace` → exit 0, "Finished `dev` profile in 2.04s".
- `cargo clippy --workspace --all-targets -- -D warnings` → exit 0.
- `cargo test --workspace` → 289 tests pass (177 xraft-core + 112 xraft-storage).
- `git diff --stat`:
  ```
   xraft-core/src/lib.rs     |   15 +
   xraft-core/src/message.rs |   17 +-
   xraft-core/src/node.rs    | 1378 +++++++++++++++++++++++++++++++++++++++
   3 files changed, 1360 insertions(+), 50 deletions(-)
  ```
