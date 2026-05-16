---
title: "Raft Node State Machine"
slug: "raft-node-state-machine"
parent_story: "failover-cluster:XRAFT"
parent_phase_anchor: "phase-consensus-engine"
new_stage_anchor: "stage-raft-node-state-machine"
inserts_before: "stage-leader-election"
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
transition methods. It is a *new* stage inserted before
`stage-leader-election` (Stage 3.2 in `implementation-plan.md`) and
`stage-log-replication` (Stage 3.3), both of which depend on the
structures and the `step()` dispatcher delivered here.

Crate placement is `xraft-core/src/node.rs` (engine), reusing the
existing `xraft-core/src/{message,types,config,error}.rs` modules from
the Project-Scaffolding phase. The work item must NOT touch
`xraft-storage`, `xraft-transport`, or `xraft-server` — wiring is owned
by later phases (Persistent Storage, Networking, Integration).

## Architectural Approach

### Pure step-function engine

Following the Heidi Howard / TigerBeetle pattern and matching KRaft's
`KafkaRaftClient`, the engine is a single object accessed only through:

```rust
pub fn step(&mut self, input: Input) -> Vec<Action>
```

The driver (Integration phase, `stage-node-driver-and-server`) pushes
one `Input` at a time and is responsible for honouring the returned
`Action`s in order. The engine performs no I/O, holds no log entries
(only the index/term tail mirror), and uses no threads. This makes the
engine deterministic and trivially testable in a single-threaded
`cargo test` harness.

### Why pull-based (KRaft) rather than push-based (textbook Raft)

KRaft inverts the AppendEntries push: followers initiate `Fetch` RPCs
and the leader replies. The engine therefore exposes no heartbeat
timer; the follower's `Fetch` cadence (Stage 3.3) is what proves leader
liveness. Stage 3.1 still carries an `Action::AppendEntries` — but as a
*local* leader-side log-write side-effect (the no-op entry emitted on
election), not a wire RPC.

### Pre-Vote first, real vote second

`NodeRole` distinguishes `PreCandidate` from `Candidate` so the engine
can speculatively poll quorum reachability without bumping
`current_term` or persisting `voted_for`. This prevents partition-
induced term inflation (the textbook Raft disruption problem). The
role-transition methods (`become_pre_candidate`, `become_candidate`)
are wired in Stage 3.1; the on-receive handlers that interpret
PreVote/Vote responses and tally quorums are Stage 3.2.

**Pre-Vote safety — timer-driven re-election must not bump term.**
A `PreCandidate` whose election timer expires must NOT transition to
`Candidate`. Doing so would increment `current_term` without ever
receiving a quorum of pre-votes and reintroduces the very disruption
Pre-Vote exists to prevent (architecture.md §5.1). The Stage 3.1 tick
handler therefore restarts the Pre-Vote round (stays `PreCandidate`,
re-randomises the timer, re-emits `PreVoteRequest`s) on a PreCandidate
timeout. The `Follower → PreCandidate` transition still happens on a
Follower timeout. The `PreCandidate → Candidate` promotion is owned by
the Stage 3.2 `handle_pre_vote_response` quorum-tally path — never by
the tick handler.

### Logical-tick timer, not wall-clock

`ElectionTimer` counts ticks (`Input::Tick`) not milliseconds. The
driver fires `Tick` at `tick_interval_ms` (default 50 ms) and the
engine converts the configured `[election_timeout_min_ms,
election_timeout_max_ms]` range into a ticks range using ceiling
division (clamped to ≥ 1). This keeps the engine fully reproducible in
the deterministic test harness which advances a manual clock by tick
count.

### Action-based unsupported-input fence

Stage 3.1 only *handles* `Input::Tick` (plus construction). All other
declared `Input` variants whose handlers belong to later stages must be
visibly rejected via `Action::RejectUnsupportedInput` rather than
silently ignored. Stage 3.1 declares the full `Input` enum surface so
the driver can compile against it; later stages replace each rejection
with a real handler one at a time. This keeps the Stage 3.1 ↔ 3.2 ↔ 3.3
boundaries explicit and prevents the driver from believing it has been
served.

At Stage 3.1 landing time, eight inputs are rejected, grouped by the
stage that will later replace each rejection with a real handler:

- **Four Stage-3.2-deferred** (replaced when `stage-leader-election`
  lands): `Input::VoteRequest`, `Input::VoteResponse`,
  `Input::PreVoteRequest`, `Input::PreVoteResponse`.
- **Four Stage-3.3-deferred** (replaced when `stage-log-replication`
  lands): `Input::ClientPropose`, `Input::FetchRequest`,
  `Input::FetchResponse`, `Input::FetchRequestAcked`.

Each rejection carries a stable `input_kind` discriminant string and a
`reason` that names the deferring stage so operators and the driver
can route metrics/alerts.

## Phase → Stage → Step Decomposition

The work item is one phase. Each stage groups closely related steps
that build on each other but should land as separate PRs. File budgets
are honest: every step touches `xraft-core/src/node.rs` plus, at most,
a sibling module for re-exports or enum surface.

### Phase: Raft Node State Machine

#### Stage 1: Engine Data Structures

The foundation: the structs and enum surface every later stage depends
on. Each step here is a self-contained type with its own unit tests.

- Step 1.1 — `ElectionTimer`: logical-tick randomised timer with
  `from_config_ms`, `new`, `reset`, `tick`, `is_expired`, `remaining`,
  plus the `pick_in_range` helper. Ceiling-division for `min_ms` /
  `max_ms` → ticks; minimum 1 tick clamp; `max_ticks` clamped to at
  least `min_ticks`. Unit tests cover randomisation range, that
  `reset` re-rolls the target, and edge cases (`min == max`,
  sub-tick interval). Files: `xraft-core/src/node.rs`. Budget: 1.
- Step 1.2 — `PeerState`: per-peer replication tracker. Fields
  `last_fetch_offset: LogIndex`, `last_fetch_time: u64` (logical
  ticks; spec name from `architecture.md` §3.2),
  `last_caught_up_time: u64`, `is_voter: bool`. Provides
  `PeerState::new(is_voter)`. Tests cover field initialisation and
  the `Eq/Debug` derives needed by leader-election assertions. Files:
  `xraft-core/src/node.rs`. Budget: 1.
- Step 1.3 — `NodeRole::PreCandidate` and `VoteGrantedSet` in
  `xraft-core/src/types.rs`. Re-export through `xraft-core/src/lib.rs`.
  Backfill 1–2 derive tests on `VoteGrantedSet` (dedupes duplicate
  grants from the same voter — protects quorum-tally correctness in
  Stage 3.2). Files: `xraft-core/src/types.rs`,
  `xraft-core/src/lib.rs`. Budget: 2.
- Step 1.4 — `RaftNode` struct plus the two constructors and the
  read-only accessors:
  - `pub fn new(config: ClusterConfig) -> Result<Self>` — entropy-
    seeded RNG, production entry point.
  - `pub fn new_with_seed(config: ClusterConfig, seed: u64) -> Result<Self>`
    — deterministic RNG seed for tests and the simulation harness.
  - Both constructors validate `config` and build the `VoterSet`,
    returning `XRaftError::Config` on misconfiguration rather than
    silently degrading the engine.
  - Accessors: `current_term`, `voted_for`, `is_leader`,
    `set_last_log(index, term)`.
  - Initial state: `Follower`, term 0, no vote, election timer
    armed, `peers` populated from `voter_set` (excluding self) with
    `PeerState::new(true)`.
  - Tests: initial-state assertions and a `new_with_seed`
    determinism test (same seed → identical timer target).
  Files: `xraft-core/src/node.rs`, `xraft-core/src/lib.rs`
  (re-exports for `RaftNode`, `PeerState`, `ElectionTimer`). Budget: 2.

#### Stage 2: Role Transition Methods

Each `become_*` is one PR. All transitions share invariants (reset
timer, clear vote tally as appropriate, emit observability events) but
they have distinct preconditions and side-effects, so they get their
own review windows.

- Step 2.1 — `become_follower(term, leader_id: Option<NodeId>)`.
  Enforces the Raft §5.1 stale-term guard: a call with
  `term < current_term` is a debug-asserted no-op (no role change, no
  leader recorded, no actions emitted, `tracing::warn!`). If
  `term > current_term`, clears `voted_for` and emits
  `Action::PersistHardState`. Emits `Action::StepDown` if previously
  `Leader`. Records leader contact when `leader_id` is `Some`. Tests
  cover higher-term step-down, stale-term ignore, and that
  `voted_for` is preserved when term is unchanged. Files:
  `xraft-core/src/node.rs`. Budget: 1.
- Step 2.2 — `become_pre_candidate()`: MUST NOT mutate
  `current_term` or `voted_for`; sets `role = PreCandidate`,
  installs a fresh `pre_votes_received` (`VoteGrantedSet`) pre-credited
  with a self-pre-vote, resets the election timer with a fresh
  random target, and returns the `Action::SendMessage(PreVoteRequest)`
  fan-out to every voter peer. Tests assert term and `voted_for` are
  unchanged, the self pre-vote is the sole entry, and one
  `SendMessage` per peer is emitted. Files: `xraft-core/src/node.rs`.
  Budget: 1.
- Step 2.3 — `become_candidate()`: increments `current_term` by
  exactly one, sets `voted_for = Some(self.id)`, installs a fresh
  `votes_received` pre-credited with a self-vote, resets the
  election timer, emits `Action::PersistHardState` (term/vote bump
  must be durable before any RPC) followed by the
  `Action::SendMessage(VoteRequest)` fan-out. Tests assert the term
  bump magnitude, self-vote presence, and `PersistHardState` ordering
  before any `SendMessage`. Files: `xraft-core/src/node.rs`.
  Budget: 1.
- Step 2.4 — `become_leader()`: initialises the `peers` map (one
  `PeerState` per voter except self, `last_fetch_offset = LogIndex(0)`,
  `last_fetch_time = self.logical_tick`,
  `last_caught_up_time = self.logical_tick`); emits
  `Action::BecomeLeader` followed by
  `Action::AppendEntries(vec![Entry { term: current_term, payload:
  EntryPayload::NoOp, .. }])`. Tests assert the no-op entry uses
  `current_term`, peers are fully initialised, and the action ordering
  is `BecomeLeader → AppendEntries`. Files: `xraft-core/src/node.rs`.
  Budget: 1.

#### Stage 3: Input Dispatcher and Tick Handler

The public surface (`step`) and the only input owned by 3.1
(`Tick`). The unsupported-input fence lives here too because it is
the contract boundary with 3.2/3.3.

- Step 3.1 — Extend `Input` / `Action` enums in
  `xraft-core/src/message.rs` to the full Stage 3.1 surface:
  - `Input::Tick`, plus the four Stage-3.2-deferred variants
    (`VoteRequest`, `VoteResponse`, `PreVoteRequest`,
    `PreVoteResponse`) and the four Stage-3.3-deferred variants
    (`ClientPropose`, `FetchRequest`, `FetchResponse`,
    `FetchRequestAcked`). All eight are declared in 3.1 so the
    driver can compile against the full surface; their handlers are
    placeholder `RejectUnsupportedInput` arms (see Step 3.3).
  - `Action` surface adds `PersistHardState`, `AppendEntries`,
    `SendMessage { target, message: OutboundMessage }`,
    `BecomeLeader`, `StepDown`, `RejectUnsupportedInput {
    input_kind: &'static str, reason: String }`. Stage-3.3-only
    actions (`ApplyToStateMachine`, `TakeSnapshot`,
    `InstallSnapshot`, `ServeFetch`, `TruncateLog`) are declared
    here but unused until 3.3.
  - Re-export `Input`, `Action`, `OutboundMessage` through
    `lib.rs`. Files: `xraft-core/src/message.rs`,
    `xraft-core/src/lib.rs`. Budget: 2.
- Step 3.2 — `step(input)` skeleton and `Input::Tick` handling.
  Match on `Input`, dispatch to per-variant private helpers,
  accumulate `Vec<Action>` via a local buffer. The match
  deliberately enumerates every variant (no wildcard arm) so Rust's
  exhaustiveness checker forces any future `Input` addition to
  pick between a real handler and another rejection. Tick handling:
  increment `self.logical_tick`, `self.election_timer.tick()`; on
  expiry, branch by `self.role`:
  - `Follower` → call `become_pre_candidate()`.
  - **`PreCandidate` → call `become_pre_candidate()` again** —
    restart the Pre-Vote round (re-roll timer, re-emit
    `PreVoteRequest`s). **No term bump on a timer-driven re-roll**;
    that promotion is owned by `handle_pre_vote_response` in
    Stage 3.2 once it observes a quorum of pre-vote grants.
  - `Candidate` → fall back to `become_pre_candidate()`. A
    Candidate that has timed out has the same partition-disruption
    risk as a Follower; routing through `PreCandidate` honours the
    "no term bump without liveness evidence" invariant.
  - `Leader` and `Observer` → no-op (KRaft has no leader heartbeat;
    Check-Quorum lands in the future
    `stage-cluster-bootstrap-and-membership`).
  Adds a `#[tracing::instrument]` span per `step` invocation and a
  `tracing::info!` per role transition. Tests cover the
  Follower→PreCandidate transition, the PreCandidate-timeout
  pre-vote-restart (term unchanged), the Candidate-timeout fallback
  to PreCandidate, and the Leader / Observer no-op. Files:
  `xraft-core/src/node.rs`. Budget: 1.
- Step 3.3 — `RejectUnsupportedInput` fence for the **eight**
  inputs whose handlers belong to a later stage:
  - **Four Stage-3.2-deferred**: `VoteRequest`, `VoteResponse`,
    `PreVoteRequest`, `PreVoteResponse` — `reason` names "Stage 3.2
    (Leader Election)".
  - **Four Stage-3.3-deferred**: `ClientPropose`, `FetchRequest`,
    `FetchResponse`, `FetchRequestAcked` — `reason` names "Stage 3.3
    (Log Replication)".
  Each rejection emits exactly one `Action::RejectUnsupportedInput`
  with a stable `input_kind: &'static str` discriminant and the
  `reason` string. The arm bodies do not touch `self`, encoding the
  no-mutation invariant. Tests assert one-and-only-one rejection per
  input and that role/term/vote/commit/last-applied/log-tip/election
  timer all stay unchanged. Files: `xraft-core/src/node.rs`.
  Budget: 1.

## Cross-Cutting Concerns

- **Tracing.** Every role transition and every `step` invocation
  emits a `tracing` event under `xraft_core::node`. Tests do not
  assert on log output, but the spans give the driver in the
  Integration phase observable seams.
- **Determinism.** All randomisation flows through a single
  `StdRng` owned by `RaftNode`. The election timer's own RNG is
  re-seeded from the node RNG at construction so the entire engine
  is deterministic when constructed via `new_with_seed`.
- **Storage trait surface is unchanged.** Stage 3.1 does not modify
  `LogStore` / `HardStateStore` / `SnapshotStore` traits. The engine
  works against the in-memory log/term mirror only; durability is
  enforced by `Action::PersistHardState` which the driver honours
  before any RPC reply.

## Out of Scope (for this work item)

- **Stage 3.2 — Leader Election handlers.**
  `handle_vote_request`, `handle_vote_response`,
  `handle_pre_vote_request`, `handle_pre_vote_response`, the
  quorum-tally logic, the `PreCandidate → Candidate` promotion on
  pre-vote quorum, and the higher-term step-down on observed
  response terms all belong to 3.2 (`stage-leader-election`).
  Stage 3.1 only declares those `Input` variants and rejects them.
- **Stage 3.3 — Log Replication.** Leader-side Fetch service,
  follower-side Fetch scheduling, high-watermark advancement,
  `ApplyToStateMachine` emission, `DivergingEpoch` truncation, and
  `FetchRequestAcked` per-peer-progress update are all 3.3
  (`stage-log-replication`).
- **gRPC transport / driver loop.** Networking phase
  (`stage-grpc-transport`) and Integration phase
  (`stage-node-driver-and-server`) own the network and the
  `tokio::select!` event loop.
- **Snapshot install / take.** Persistent Storage phase
  (`stage-snapshot-storage`) and a future
  `stage-snapshot-install-protocol`.
- **Membership changes / Check-Quorum.** Integration phase
  (`stage-cluster-bootstrap-and-membership`).
- **Admin HTTP and metrics.** Integration phase
  (`stage-admin-api-and-observability`).

## Open Questions

1. **Where should `last_leader_contact_tick` updates land?**
   Stage 3.1 only updates this field from the explicit
   `become_follower(_, Some(id))` path. Stage 3.3 will also bump it
   on every successful `FetchResponse`. We resolve this by leaving
   the field `pub` within the crate so 3.3 can update without
   re-architecting.
2. **Should `PreCandidate` count its own pre-vote?** We follow the
   `etcd-raft` convention: yes, self-pre-vote is pre-credited so a
   single-node cluster can self-promote. Documented in
   `become_pre_candidate` doc comment.
3. **`Action` ordering.** The driver must process `Action`s in the
   order emitted. The Stage 3.1 invariant is "`PersistHardState`
   before any `SendMessage` that depends on the persisted field" —
   encoded by always pushing `PersistHardState` first in the
   affected transitions.
4. **Leftover backup artefacts (`xraft-core/src/node.rs.review-backup`,
   four `docs/stories/.../*.iter-snapshot.bak` files).** Operator
   guidance is to defer cleanup of the remaining
   `node.rs.review-backup` to a dedicated future workstream; the four
   `.iter-snapshot.bak` files are now gitignored. No source-tree
   changes belong in this planning iteration.
