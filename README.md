# failover-cluster

Rust implementation of the Raft consensus protocol, modelled on Apache
Kafka's **KRaft** (Kafka Raft) protocol. The crate is built up
stage-by-stage; the present commit lands the **Raft Node State Machine**
stage.

## What this stage provides

A deterministic, pure state machine that drives a single Raft node
through the standard role transitions plus the KRaft additions:

| Role            | Description                                           |
|-----------------|-------------------------------------------------------|
| `Follower`      | Default state; accepts append/vote RPCs.             |
| `PreCandidate`  | Pre-vote probe; does *not* bump `current_term`.      |
| `Candidate`     | Real candidacy; bumps `current_term` and self-votes. |
| `Leader`        | Replicates entries and sends heartbeats.              |
| `Observer`      | Non-voting replica (Kafka brokers under KRaft).       |

The state machine is a Mealy machine: each call to `RaftNode::handle`
takes an `Event` (timer tick, inbound RPC, membership change) and
returns a `Vec<Command>` describing the side-effects the host runtime
should perform (reset timer, broadcast RPC, persist
`(current_term, voted_for)`, etc.). The SM itself never touches the
network, disk, or clock — making it fully deterministic and trivially
testable.

## Safety invariants enforced

- **Election safety** — `voted_for` guarantees at most one leader per
  term.
- **Higher-term step-down** — any RPC carrying `term > current_term`
  from a known voter forces a transition to `Follower` and clears
  `voted_for`.
- **Membership guards** — votes and `AppendEntries` from non-voters
  are rejected without mutating term/leader state. A demoted node or
  observer cannot disrupt the cluster's term progression.
- **Up-to-date check** — votes are granted only when the candidate's
  `(last_log_term, last_log_index)` is at least as up-to-date as ours.
- **Pre-vote does not mutate persistent state** — protects against
  partitioned-node disruption (Raft dissertation §9.6 / KRaft
  KIP-650).
- **Same-term `AppendEntries` always demotes a Candidate** — even
  when the log check fails (preserves at-most-one-leader-per-term).
- **Voter-set changes invalidate in-flight candidacy** — `Candidate`
  / `PreCandidate` steps down on `PromoteToVoter` / `DemoteToObserver`
  so stale vote tallies cannot win against the new voter set.
- **Observers never run elections** — even on election timeout.

## Out of scope (future stages)

- Persistent log storage and entry append/truncate semantics.
- Network transport, RPC framing, wire format.
- Snapshotting (`FetchSnapshot` / `SnapshotId`).
- Joint-consensus commit of `VotersRecord` for membership changes
  (this stage only exposes `PromoteToVoter` / `DemoteToObserver`
  hooks).

## Layout

```
src/
  types.rs                — NodeId, Term, LogIndex, LogMetadata
  config.rs               — NodeConfig (voters, observers, pre-vote)
  error.rs                — RaftError / RaftResult
  node/
    role.rs               — Role + RoleKind
    state.rs              — PersistentState + VolatileState + LogMetadataCache
    event.rs              — Event (inputs)
    command.rs            — Command (outputs)
    state_machine.rs      — RaftNode (the state machine)
tests/
  state_machine_tests.rs  — in-memory cluster scenarios
```

## References

- [Deep dive into Apache Kafka's KRaft protocol (Red Hat)](https://developers.redhat.com/articles/2025/09/17/deep-dive-apache-kafkas-kraft-protocol)
- [Confluent — Kafka Raft (KRaft)](https://developer.confluent.io/learn/kraft/)
- Ongaro & Ousterhout, *In Search of an Understandable Consensus
  Algorithm (Extended Version)*, 2014.
