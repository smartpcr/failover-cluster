---
title: "Raft Node State Machine"
slug: "raft-node-state-machine"
parent_phase: "Phase 3 — Raft Consensus Engine"
stage_anchor: "stage-raft-node-state-machine"
status: "planned"
---

# Raft Node State Machine — Design Narrative

## Context and Intent

XRAFT is a Rust implementation of the Raft consensus protocol that follows
the KRaft (Apache Kafka's Raft) variant: pull-based log replication, a
single voter set per cluster, gRPC transport, and a strict I/O-free core
engine. References that shaped the design:

- KRaft deep dive (Red Hat Developers, Sep 2025) — pull-based fetch model
  and `quorum-state` persistence.
- Confluent KRaft learn module — leader-epoch fencing, observer joins.
- `dragotin/kraft` (Rust KRaft prototype) — naming and crate split.

The work item **"Raft Node State Machine"** scopes the *engine*: the pure
state object every node carries, the input dispatcher, and the role-
transition methods. It is the foundation that Stage 3.2 (Leader Election)
and Stage 3.3 (Log Replication) build on. The engine is deliberately
isolated from I/O — it consumes `Input` enums and returns `Vec<Action>`
side-effects that the driver in `xraft-server` later materialises against
the storage and transport traits.

Crate placement is `xraft-core/src/node.rs` (engine), reusing the existing
`xraft-core/src/{message,types,config,error}.rs` modules from Stages 1.2
and 1.3. The work item must NOT touch `xraft-storage`, `xraft-transport`,
or `xraft-server` — wiring is owned by later stages.

## Architectural Approach

### Pure step-function engine

Following the Heidi Howard / TigerBeetle pattern and matching KRaft's
`KafkaRaftClient`, the engine is a single object accessed only through:

```rust
pub fn step(&mut self, input: Input) -> Vec<Action>
```

The driver (Stage 4.2) pushes one `Input` at a time and is responsible
for honouring the returned `Action`s in order. The engine performs no
I/O, holds no log entries (only the index/term tail mirror), and uses no
threads. This makes the engine deterministic and trivially testable in a
single-threaded `cargo test` harness.

### Why pull-based (KRaft) rather than push-based (textbook Raft)

KRaft inverts the AppendEntries push: followers initiate `Fetch` RPCs and
the leader replies. The engine therefore exposes no heartbeat timer; the
follower's `Fetch` cadence (Stage 3.3) is what proves leader liveness.
Stage 3.1 still carries an `Action::AppendEntries` — but as a *local* leader-
side log-write side-effect (no-op entry on election), not a wire RPC.

### Pre-Vote first, real vote second

`NodeRole` distinguishes `PreCandidate` from `Candidate` so the engine
can speculatively poll quorum reachability without bumping `current_term`
or persisting `voted_for`. This prevents partition-induced term inflation
(the textbook Raft disruption problem). The role transitions are wired
in Stage 3.1; the on-receive handlers that interpret responses are Stage
3.2.

### Logical-tick timer, not wall-clock

`ElectionTimer` counts ticks (`Input::Tick`) not milliseconds. The driver
fires `Tick` at `tick_interval_ms` (default 50 ms) and the engine
converts the configured `[election_timeout_min_ms, election_timeout_max_ms]`
range into a ticks range using ceiling division. This keeps the engine
fully reproducible in the deterministic test harness (Stage 8) which
advances a manual clock by tick count.

### Action-based unsupported-input fence

Stage 3.1 only owns `Input::Tick` (plus construction). Inputs whose
handlers live in 3.2/3.3 (`ClientPropose`, `FetchRequest`, `FetchResponse`,
`FetchRequestAcked`, and the four vote/pre-vote variants) must be visibly
rejected via `Action::RejectUnsupportedInput` rather than silently
ignored. This keeps Stage 3.1 ↔ 3.2/3.3 boundaries explicit and
prevents the driver from believing it has been served.

## Phase → Stage → Step Decomposition

The work item is one phase. Each stage groups closely related steps that
build on each other but should land as separate PRs. File budgets are
honest: every step touches `xraft-core/src/node.rs` plus, at most, a
sibling module for re-exports or enum surface.

### Phase: Raft Node State Machine

#### Stage 1: Engine Data Structures

The foundation: the structs and enum surface every later stage depends
on. Each step here is a self-contained type with its own unit tests.

- Step 1.1 — `ElectionTimer`: logical-tick randomised timer with
  `from_config_ms`, `new`, `reset`, `tick`, `is_expired`, `remaining`,
  and the `pick_in_range` helper. Ceiling-division for `min_ms` / `max_ms`
  → ticks; minimum 1 tick clamp. Unit tests cover randomisation range,
  reset re-rolls the target, and edge cases (min==max, sub-tick interval).
  Files: `xraft-core/src/node.rs`. Budget: 1.
- Step 1.2 — `PeerState`: per-peer replication tracker. Fields:
  `last_fetch_offset: LogIndex`, `last_fetch_time_tick: u64`,
  `last_caught_up_time_tick: u64`, `is_voter: bool`. Constructor +
  `record_fetch` helper + tests. Files: `xraft-core/src/node.rs`. Budget: 1.
- Step 1.3 — `NodeRole::PreCandidate` and `VoteGrantedSet` in
  `xraft-core/src/types.rs`. Re-export through `xraft-core/src/lib.rs`.
  Backfill 1-2 derive tests. Files: `xraft-core/src/types.rs`,
  `xraft-core/src/lib.rs`. Budget: 2.
- Step 1.4 — `RaftNode` struct, `new(config, voters, state_machine)`
  constructor, accessors (`role`, `current_term`, `voted_for`, `id`,
  `commit_index`, `last_applied`), and the "initial-state" test
  (Follower, term 0, timer running, no votes). Files:
  `xraft-core/src/node.rs`, `xraft-core/src/lib.rs` re-exports. Budget: 2.

#### Stage 2: Role Transition Methods

Each `become_*` is one PR. All transitions share invariants (reset
timer, clear vote tally as appropriate, emit observability events) but
they have distinct preconditions and side-effects, so they get their own
review windows.

- Step 2.1 — `become_follower(term, leader_id: Option<NodeId>)`: clears
  `voted_for` if `term > current_term`, sets role, records leader contact
  (when `Some`), emits `Action::PersistHardState` and `Action::StepDown`
  if previously Leader. Tests cover the "higher-term forces step-down"
  invariant. Files: `xraft-core/src/node.rs`. Budget: 1.
- Step 2.2 — `become_pre_candidate()`: must NOT mutate term or
  `voted_for`; instantiates a fresh `VoteGrantedSet` keyed on the
  hypothetical term `current_term + 1`; resets election timer. Tests
  assert term and `voted_for` are unchanged. Files:
  `xraft-core/src/node.rs`. Budget: 1.
- Step 2.3 — `become_candidate()`: increments `current_term`, sets
  `voted_for = Some(self.id)`, fresh `VoteGrantedSet` pre-credited with
  self-vote, resets election timer, emits `Action::PersistHardState`.
  Tests: term bumps by exactly one; self-vote is the only granted vote.
  Files: `xraft-core/src/node.rs`. Budget: 1.
- Step 2.4 — `become_leader()`: initialises `peers` map with one
  `PeerState` per voter (excluding self), `last_fetch_offset =
  last_log_index + 1`, emits `Action::BecomeLeader` followed by
  `Action::AppendEntries(vec![Entry::no_op])`. Tests assert no-op entry is
  emitted with `term == current_term` and peer init is complete. Files:
  `xraft-core/src/node.rs`. Budget: 1.

#### Stage 3: Input Dispatcher and Tick Handler

The public surface (`step`) and the only input owned by 3.1 (`Tick`).
The unsupported-input fence lives here too because it is the contract
boundary with 3.2/3.3.

- Step 3.1 — Extend `Input` / `Action` enums in
  `xraft-core/src/message.rs` to the full Stage 3.1 surface: `Input::Tick`,
  the four vote/pre-vote variants (declared, handlers come in 3.2), and
  the stubs for `ClientPropose` / `FetchRequest` / `FetchResponse` /
  `FetchRequestAcked` (declared, rejected for now). `Action` surface adds
  `PersistHardState`, `AppendEntries`, `SendMessage`, `BecomeLeader`,
  `StepDown`, `RejectUnsupportedInput`. Re-export through `lib.rs`.
  Files: `xraft-core/src/message.rs`, `xraft-core/src/lib.rs`. Budget: 2.
- Step 3.2 — `step(input)` skeleton: match on `Input`, dispatch to
  per-variant private helpers, accumulate `Vec<Action>` via a local
  buffer. Implements `Input::Tick`: increment timer; on expiry for
  `Follower` / `PreCandidate` / `Candidate`, call `become_pre_candidate()`
  (or `become_candidate()` if already `PreCandidate`). Leader Ticks are
  a no-op (KRaft has no leader heartbeat). Includes a tracing span per
  step and `tracing::info!` per role transition. Tests cover the
  Follower-→PreCandidate-→Candidate cascade and Leader Tick no-op.
  Files: `xraft-core/src/node.rs`. Budget: 1.
- Step 3.3 — `RejectUnsupportedInput` path for the seven inputs whose
  handlers belong to Stage 3.2/3.3. Each rejection includes the input
  discriminant string so the driver can log/alert. Tests assert exactly
  one `Action::RejectUnsupportedInput` is emitted and no other side
  effects occur for each unsupported input. Files:
  `xraft-core/src/node.rs`. Budget: 1.

## Cross-Cutting Concerns

- **Tracing.** Every role transition and every `step` invocation emits a
  `tracing` event under `xraft_core::node`. Tests do not assert on log
  output, but the spans give the driver in Stage 4 observable seams.
- **Determinism.** All randomisation flows through a single `&mut StdRng`
  owned by `RaftNode`. Construction accepts an optional explicit seed so
  the deterministic harness (Stage 8) can produce reproducible runs.
- **Storage trait surface is unchanged.** Stage 3.1 does not modify
  `LogStore` / `HardStateStore` / `SnapshotStore` traits. The engine
  works against the in-memory log/term mirror only; durability is
  enforced by `Action::PersistHardState` which the driver honours before
  any RPC reply.

## Out of Scope (for this work item)

- **Stage 3.2 — Leader Election handlers.** `handle_vote_request`,
  `handle_vote_response`, `handle_pre_vote_request`,
  `handle_pre_vote_response`, quorum-tally logic, and the step-down on
  higher-term observation belong to 3.2. Stage 3.1 only emits these
  variants from `Input` as `RejectUnsupportedInput`.
- **Stage 3.3 — Log Replication.** Leader-side Fetch service, follower-
  side Fetch scheduling, high-watermark advancement, `ApplyToStateMachine`
  emission, `DivergingEpoch` truncation, and `FetchRequestAcked` peer-
  progress update are all 3.3.
- **gRPC transport / driver loop.** Phases 4 and 5 own the network and
  the `tokio::select!` event loop.
- **Snapshot install / take.** Phase 5.2.
- **Membership changes.** Phase 6.
- **Admin HTTP and metrics.** Phase 7.

## Open Questions

1. **Where should `last_leader_contact_tick` updates land?** Stage 3.1
   only updates this field from the explicit `become_follower(_, Some(id))`
   path. Stage 3.3 needs to also bump it on every successful
   `FetchResponse`. We resolve this by leaving the field public-within-
   crate so 3.3 can update without re-architecting.
2. **Should `PreCandidate` count its own pre-vote?** We follow the
   `etcd-raft` convention: yes, self-pre-vote is pre-credited so a
   single-node cluster can self-promote. Documented in `become_pre_candidate`
   doc comment.
3. **`Action` ordering.** The driver must process `Action`s in the order
   emitted. The Stage 3.1 invariant is "PersistHardState before any
   SendMessage that depends on the persisted field" — encoded by always
   pushing `PersistHardState` first in the affected transitions.
