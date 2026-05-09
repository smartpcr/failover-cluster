# XRAFT Technical Specification

> **Story:** `failover-cluster:XRAFT` · **Points:** 13
> **One-liner:** Implement Raft consensus in Rust, modelled after Apache Kafka's KRaft protocol.

---

## 1  Problem Statement

The `failover-cluster` repository needs a production-grade consensus layer that
lets a cluster of nodes agree on a single leader, replicate a totally-ordered
metadata log, and survive minority failures — all without an external
coordination service (no ZooKeeper equivalent).

The story asks for a **Rust implementation of the Raft consensus protocol**,
using Kafka's KRaft variant as the primary design reference.  KRaft is a
pull-based, event-sourced Raft implementation that manages cluster metadata
through a replicated log with periodic snapshots.  The third reference link
(dragotin/kraft) turned out to be an unrelated KDE invoicing application and
is **not** a technical input for this work.

### Why Raft?

Raft was chosen for its understandability and proven safety properties:

| Property | Guarantee |
|---|---|
| Leader Election Safety | At most one leader per term |
| Append-Only Leader | Leaders never overwrite or delete log entries |
| Leader Completeness | Elected leader contains all committed entries |
| Log Matching | Same (index, term) ⇒ identical prefix |
| State Machine Safety | Applied entry at index N is the same everywhere |

### Why KRaft-style?

KRaft adds pragmatic refinements on top of vanilla Raft that are worth
adopting:

* **Pull-based replication** — followers fetch from the leader rather than the
  leader pushing to all followers.  This scales better and lets followers
  control their own fetch rate.
* **Event-sourced metadata** — the log is the source of truth; in-memory state
  machines are derived views that can be rebuilt from the log at any time.
* **Single-threaded event loop** — ordered processing eliminates concurrency
  bugs in the consensus hot path.
* **Pre-Vote protocol** — prevents disruptive elections by isolated nodes.
* **Check Quorum** — leader periodically verifies majority connectivity and
  steps down otherwise.

---

## 2  In-Scope

The following capabilities are in scope for the XRAFT story:

### 2.1  Core Raft Protocol (Rust library crate)

| Capability | Detail |
|---|---|
| **Leader election** | Term-based voting with randomised election timeouts, `RequestVote` RPC, majority quorum |
| **Log replication** | Append-only log, `AppendEntries`/`Fetch`-style RPC, high-watermark tracking, log consistency checks (`prevLogIndex` / `prevLogTerm`) |
| **Safety invariants** | All five Raft safety properties (§1 table) enforced and tested |
| **Persistent state** | `currentTerm`, `votedFor`, and log durably persisted (`fsync`) before RPC responses |
| **Heartbeats** | Leader sends periodic heartbeats; followers reset election timer on receipt |
| **Commit protocol** | Leader commits when majority acknowledges; followers apply on HW advance |
| **No-op on election** | New leader appends a blank entry to commit pending entries from prior term |

### 2.2  KRaft-Inspired Extensions

| Capability | Detail |
|---|---|
| **Pull-based replication** | Followers and observers initiate `Fetch` RPCs instead of leader-push `AppendEntries` |
| **Pre-Vote** | Candidate checks quorum reachability before incrementing term |
| **Check Quorum** | Leader periodically verifies majority contact; steps down on quorum loss |
| **Snapshot support** | Periodic snapshots of applied state; `FetchSnapshot` RPC for slow/new followers |
| **Observer role** | Non-voting nodes that replicate the log for read scaling or standby purposes |
| **Leader epoch / fencing** | Epoch-based fencing to detect stale leaders during network partitions |

### 2.3  Transport & Networking

| Capability | Detail |
|---|---|
| **Async I/O** | Built on `tokio` async runtime |
| **RPC layer** | gRPC (via `tonic`) or custom TCP framing — architecture doc decides |
| **Multiplexed connections** | Connection pooling between peers |

### 2.4  Observability

| Capability | Detail |
|---|---|
| **Structured logging** | `tracing` crate with span-per-RPC |
| **Metrics** | `metrics` or `prometheus` crate — current leader, term, commit latency, append rate, election latency |
| **Health endpoint** | Liveness + readiness probes for orchestrators |

### 2.5  Testing

| Capability | Detail |
|---|---|
| **Unit tests** | Per-module coverage of election, replication, snapshotting |
| **Deterministic simulation** | In-process multi-node harness with injectable faults (message drops, delays, partitions) |
| **Integration tests** | Real-network 3-node and 5-node cluster scenarios |
| **Linearisability checking** | Jepsen-style validation via `stateright` or equivalent model checker |

---

## 3  Out of Scope

| Item | Rationale |
|---|---|
| **Application-level state machine** | XRAFT provides the replicated log; what the consumer does with committed entries is outside this story |
| **Multi-Raft / sharding** | Single Raft group only; partitioning across multiple groups is a future story |
| **Dynamic quorum changes at runtime** | KRaft supports `AddRaftVoter`/`RemoveRaftVoter` but this adds significant complexity; static cluster membership for v1 |
| **Client SDK / external API** | No gRPC service definition for end-user clients; only inter-node RPCs |
| **Kafka wire protocol compatibility** | We borrow KRaft *design*, not its binary protocol |
| **Disk-based log storage engine** | v1 uses a simple file-per-segment approach; a production WAL engine (e.g., `sled`, `rocksdb`) is a future optimisation |
| **TLS / mTLS between nodes** | Security hardening is a separate story |
| **Benchmarking / performance tuning** | Functional correctness first; optimisation follows |

---

## 4  Non-Goals

These are **explicitly not objectives** for XRAFT, even though related work may
touch them:

1. **Drop-in replacement for etcd / ZooKeeper** — XRAFT is a consensus
   library, not a key-value store.
2. **Kafka compatibility** — we are inspired by KRaft's architecture, not
   implementing the Kafka metadata protocol.
3. **Formal TLA⁺ specification** — desirable for future verification, but not
   a deliverable of this 13-point story.
4. **Cross-language bindings** — Rust-only; FFI or WASM wrappers are future
   work.
5. **GUI / dashboard** — observability is via metrics and logs only.

---

## 5  Hard Constraints

### 5.1  Language & Toolchain

* **Rust stable** (≥ 1.78).  No nightly-only features.
* **Edition 2021** minimum.
* `#![forbid(unsafe_code)]` in the consensus crate; `unsafe` allowed only in
  the storage layer with documented safety invariants.
* Must compile on Linux x86_64 and macOS aarch64.  Windows is best-effort.

### 5.2  Concurrency Model

* Single-threaded event loop for the consensus state machine (matches KRaft's
  `KafkaRaftClient` design).  All state transitions happen on one
  `tokio::task`; RPCs are dispatched to/from that task via channels.
* No `Arc<Mutex<_>>` around consensus state.  The single-owner model
  eliminates data races by construction.

### 5.3  Persistence

* **Durable state must be `fsync`'d before responding to any RPC** — this is a
  Raft safety requirement, not optional.
* Voting state (`currentTerm`, `votedFor`) stored in a separate file from the
  log, consistent with KRaft's `quorum-state` file pattern.
* Log segments stored as append-only files with an index for offset lookup.

### 5.4  Timing

Per the Raft timing requirement:

```
broadcastTime  ≪  electionTimeout  ≪  MTBF

broadcastTime  :   0.5 – 20 ms   (RPC round-trip)
electionTimeout:   150 – 300 ms  (configurable)
MTBF           :   months
```

Election timeouts must be randomised within the configured range to prevent
split-vote livelocks.

### 5.5  Quorum Arithmetic

* Cluster sizes: 3, 5, or 7 voting members (odd only).
* Quorum = ⌊N/2⌋ + 1 (strict majority).
* Observers do not count toward quorum.

### 5.6  Crate Boundaries

The implementation should be split into workspace crates:

| Crate | Responsibility |
|---|---|
| `xraft-core` | Protocol state machine, log, elections — no I/O |
| `xraft-storage` | Durable log segments, snapshots, voting state |
| `xraft-transport` | Async RPC client/server (gRPC or TCP) |
| `xraft-server` | Binary that wires core + storage + transport |
| `xraft-test` | Simulation harness and integration tests |

`xraft-core` must be deterministic and I/O-free so it can be driven by both
real networking and deterministic simulation.

### 5.7  Dependencies (Key)

| Purpose | Crate | Rationale |
|---|---|---|
| Async runtime | `tokio` | Industry standard for Rust async |
| Serialisation | `serde` + `bincode` or `prost` | Compact binary encoding for log entries and RPCs |
| Logging | `tracing` | Structured, async-aware |
| Metrics | `metrics` | Façade pattern; pluggable exporters |
| RPC (if gRPC) | `tonic` + `prost` | Mature, HTTP/2-based |
| CLI | `clap` | Argument parsing for `xraft-server` |
| Testing | `tokio::test`, `proptest` | Async + property-based |

---

## 6  Identified Risks

### 6.1  Correctness Risk — Raft is Subtle

| Risk | Impact | Mitigation |
|---|---|---|
| Incorrect election or commit logic causes split-brain or data loss | **Critical** — violates safety | Deterministic simulation with fault injection; property-based tests asserting linearisability; review against the Raft paper §5 invariants |
| `fsync` not called on all code paths before RPC response | **Critical** — violates durability | Enforce via type system: RPC reply type is only constructible after a `Persisted` token is obtained |
| Snapshot / log compaction leaves gap in replayable history | **High** — stuck followers | Include `lastIncludedIndex` + `lastIncludedTerm` in snapshot metadata; test follower catch-up from snapshot |

### 6.2  Performance Risk

| Risk | Impact | Mitigation |
|---|---|---|
| `fsync` latency dominates commit path | **Medium** — high commit latency | Batch log appends (KRaft's `BatchAccumulator` pattern); concurrent `fsync` with RPC transmission |
| Pull-based fetch adds one extra round-trip vs. push | **Low** — slightly higher replication latency | Acceptable trade-off for simpler leader logic; tuneable fetch interval |
| Single-threaded event loop becomes bottleneck under high throughput | **Medium** | Batch processing within the loop; offload serialisation to I/O tasks |

### 6.3  Scope / Schedule Risk

| Risk | Impact | Mitigation |
|---|---|---|
| 13 story points may be tight for full snapshot + observer support | **Medium** — incomplete delivery | Prioritise core election + replication first; snapshot and observer are lower-priority extensions that can be deferred |
| Pull-based model is architecturally different from textbook Raft | **Medium** — design confusion | Document the mapping between KRaft concepts and standard Raft explicitly in the architecture doc |
| No existing Rust code in repo to build on | **Low** — cold start | Scaffold workspace with `cargo init` early; unblocks parallel work |

### 6.4  Operational Risk

| Risk | Impact | Mitigation |
|---|---|---|
| No TLS in v1 leaves cluster traffic unencrypted | **Medium** — not production-safe without network-level isolation | Document as known limitation; plan TLS story |
| Static membership means replacing a failed node requires cluster restart | **Medium** — availability impact during maintenance | Document operational procedure; dynamic membership is planned for a future story |

---

## 7  Key Decisions Pending

These decisions affect multiple sibling documents and should be resolved
consistently across `architecture.md`, `implementation-plan.md`, and this spec:

1. **RPC transport**: gRPC (`tonic`) vs. custom TCP framing.  gRPC is heavier
   but gives streaming, backpressure, and codegen for free.  Custom TCP is
   lighter but requires hand-rolled framing and connection management.
2. **Log encoding format**: `bincode` (compact, Rust-native) vs. `protobuf`
   (cross-language, self-describing).  If non-goals include cross-language
   bindings, `bincode` is simpler.
3. **Pull vs. push replication**: The story references KRaft (pull-based), but
   the Raft paper describes push-based `AppendEntries`.  This spec assumes
   pull-based per the KRaft design reference.  Architecture doc should confirm.

---

## 8  Glossary

| Term | Definition |
|---|---|
| **Term** | Monotonically increasing logical clock; incremented on each election |
| **Epoch** | KRaft synonym for term |
| **High Watermark (HW)** | Highest log offset replicated to a majority; entries below HW are committed |
| **Observer** | Non-voting node that replicates the log (KRaft's broker role) |
| **Voter** | Quorum member that participates in elections and commits |
| **Pre-Vote** | Speculative election round that does not increment term |
| **Check Quorum** | Leader-side liveness check verifying majority connectivity |
| **Snapshot** | Point-in-time serialisation of the applied state machine; enables log truncation |
| **LSO (Log Start Offset)** | Earliest available log offset after compaction |

---

## 9  References

1. Ongaro, D. & Ousterhout, J. — *In Search of an Understandable Consensus
   Algorithm (Raft)*, 2014.
2. Red Hat — [Deep dive into Apache Kafka's KRaft protocol](https://developers.redhat.com/articles/2025/09/17/deep-dive-apache-kafkas-kraft-protocol)
3. Confluent — [Apache Kafka Raft (KRaft)](https://developer.confluent.io/learn/kraft/)
4. KIP-500 — Replace ZooKeeper with a Self-Managed Metadata Quorum
5. KIP-595 — Raft Protocol for the Metadata Quorum
6. KIP-853 — KRaft Controller Quorum Reconfiguration

---

*Document: `docs/stories/failover-cluster-XRAFT/tech-spec.md`*
*Story: failover-cluster:XRAFT · Status: Draft · Iteration 1*
