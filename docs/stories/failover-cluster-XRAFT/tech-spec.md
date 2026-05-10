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
| **Log replication** | Append-only log with high-watermark tracking and log consistency checks (`prevLogIndex` / `prevLogTerm`).  In our KRaft-inspired model, followers pull entries via `Fetch` RPCs rather than receiving leader-pushed `AppendEntries` (see §2.2); the consistency guarantees are identical to textbook Raft — only the initiator of the RPC changes. |
| **Safety invariants** | All five Raft safety properties (§1 table) enforced and tested |
| **Persistent state** | `currentTerm`, `votedFor`, and log durably persisted (`fsync`) before RPC responses |
| **Heartbeats** | In the pull-based model, the leader does **not** push standalone heartbeat messages.  Instead, followers send periodic `Fetch` RPCs; when no new entries exist, the leader returns an empty `Fetch` response carrying the current term and high watermark.  Followers treat any valid `Fetch` response (empty or not) as proof of leader liveness and reset their election timer accordingly.  This is functionally equivalent to textbook Raft heartbeats but initiated by the follower, consistent with the KRaft design.  **Terminology note:** `e2e-scenarios.md` Features 1, 10, 14, and 16 use the word "heartbeat" as a shorthand for this follower-initiated Fetch cycle.  When those scenarios say "heartbeat interval" or "heartbeat timeout," they mean the Fetch polling interval and the election timer, respectively — there is no separate leader-pushed heartbeat RPC. |
| **Commit protocol** | Leader commits when majority acknowledges; followers apply on HW advance |
| **No-op on election** | New leader appends a blank entry to commit pending entries from prior term |

### 2.2  KRaft-Inspired Extensions

| Capability | Detail |
|---|---|
| **Pull-based replication** | Followers and observers initiate `Fetch` RPCs to the leader instead of receiving leader-pushed RPCs.  This replaces the textbook `AppendEntries` push model: the leader responds to each `Fetch` with new log entries, consistency metadata (`prevLogIndex` / `prevLogTerm`), and the current high watermark.  All Raft safety invariants are preserved — only the direction of initiation changes.  **Proto alignment note:** `implementation-plan.md` §1.3 defines the `proto/raft.proto` file with messages: `VoteRequest`, `VoteResponse`, `PreVoteRequest`, `PreVoteResponse`, `FetchRequest`, `FetchResponse`, `AppendEntriesRequest`, `AppendEntriesResponse`, `FetchSnapshotRequest`, and `FetchSnapshotChunk`.  The `AppendEntries*` types are **internal-only proto types** used for the `Action::AppendEntries` side-effect within `xraft-core` (leader writing to its own log) — they are **not** exposed as network RPCs.  The gRPC `RaftService` (Stage 3.1) exposes only `Vote`, `PreVote`, `Fetch`, and `FetchSnapshot` as wire RPCs. |
| **Pre-Vote** | Candidate checks quorum reachability before incrementing term |
| **Check Quorum** | Leader periodically verifies majority contact; steps down on quorum loss |
| **Snapshot support** | Periodic snapshots of applied state; `FetchSnapshot` RPC (streamed chunks) for slow/new followers.  Both `architecture.md` and `implementation-plan.md` use `FetchSnapshot` as the wire RPC name; `implementation-plan.md` uses `install_snapshot()` only as the internal handler function name on the follower side that processes received snapshot chunks. |
| **Observer role** | Non-voting nodes that replicate the log for read scaling or standby purposes |
| **Leader epoch / fencing** | Epoch-based fencing to detect stale leaders during network partitions |

### 2.3  Transport & Networking

| Capability | Detail |
|---|---|
| **Async I/O** | Built on `tokio` async runtime |
| **RPC layer** | gRPC via `tonic` + `prost`.  gRPC provides HTTP/2 streaming (useful for `Fetch` long-poll), backpressure, and codegen.  The added binary size is acceptable given the observability and interop benefits.  This is a **firm decision**, not deferred. |
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

### 2.6  Internal Peer & Admin Client (`xraft-client`)

| Capability | Detail |
|---|---|
| **Peer RPC client** | `PeerClient` wraps a `tonic` gRPC channel to a specific peer for `Vote`, `PreVote`, `Fetch`, and `FetchSnapshot` RPCs with connection lifecycle management; used internally by `xraft-server` for inter-node communication |
| **Leader discovery** | Tracks last-known leader via hints in `FetchResponse` / `VoteResponse`; transparently retries against the hinted leader on redirect |
| **Connection pool** | `ConnectionPool` maintains lazy-initialised `PeerClient` instances keyed by `NodeId` |
| **Admin client** | `AdminClient` connects to a node's admin HTTP endpoint for operational queries (leader status, metrics, trigger snapshot) |

Per `architecture.md` §2.5 and `e2e-scenarios.md` Feature 11, `xraft-client` is
an **internal infrastructure crate** providing a peer RPC client (used by
`xraft-server` for inter-node gRPC) and an admin client (for operational
queries).  It is **not** an external consumer SDK — no external `propose`/`read`
API for outside callers is in scope for v1.  `e2e-scenarios.md` Feature 11 tests
the internal inter-node routing and leader-discovery behaviour.

### 2.7  Administrative Operations

| Capability | Detail |
|---|---|
| **AdminApi** | HTTP API for cluster status and triggering snapshots |

> **Dynamic membership (`AddVoter`/`RemoveVoter`) is out of scope for v1**
> and deferred to a future story entirely — not a stretch goal within XRAFT.
> Per `architecture.md` §5.5, `e2e-scenarios.md` Feature 12, and
> `implementation-plan.md` Stage 7.2, the v1 deliverable uses **static
> membership** (voter set fixed at bootstrap) and observer support only.  Any
> `AddVoter`/`RemoveVoter` command is rejected with an `UNSUPPORTED` error.
> The `AdminApi` supports status queries and snapshot triggers; membership
> mutation endpoints are not implemented in v1.

| **Optional TLS** | TLS configuration (`tls.cert_path` / `tls.key_path`) is supported as an optional transport setting per `architecture.md` §2.3.  Not mandatory for v1 functional correctness, but the configuration surface exists. |

---

## 3  Out of Scope

| Item | Rationale |
|---|---|
| **Application-level state machine** | XRAFT provides the replicated log; what the consumer does with committed entries is outside this story |
| **Multi-Raft / sharding** | Single Raft group only; partitioning across multiple groups is a future story |
| **Dynamic quorum changes** | `AddVoter`/`RemoveVoter` RPCs are **out of scope for v1** and deferred to a future story entirely — not a stretch goal within XRAFT (per `architecture.md` §5.5, `e2e-scenarios.md` Feature 12, and `implementation-plan.md` Stage 7.2).  The v1 deliverable uses static membership (voter set fixed at bootstrap) and observer support only; any `AddVoter`/`RemoveVoter` command is rejected with `UNSUPPORTED`. |
| **Kafka wire protocol compatibility** | We borrow KRaft *design*, not its binary protocol |
| **Disk-based log storage engine** | v1 uses a simple file-per-segment approach; a production WAL engine (e.g., `sled`, `rocksdb`) is a future optimisation |
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

* **Rust stable** (≥ 1.85).  No nightly-only features.
* **Edition 2024**, per `implementation-plan.md` Stage 1.1 (`rust-toolchain.toml`
  pinning stable Rust edition 2024).  Edition 2024 requires Rust ≥ 1.85.
* `#![forbid(unsafe_code)]` in the consensus crate; `unsafe` allowed only in
  the storage layer with documented safety invariants.
* Must compile on Linux x86_64, macOS aarch64, and Windows x86_64.  Linux and
  macOS are the primary CI targets; Windows must remain usable (the repository
  itself uses a Windows worktree) but platform-specific integration tests (e.g.,
  network fault injection) may be Linux-only.

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

* **Recommended** cluster sizes: 3, 5, or 7 voting members (odd numbers maximise
  fault tolerance per node).
* Per `implementation-plan.md` Stage 7.2, the implementation **accepts** any
  voter count ≥ 1: a single-voter cluster self-elects immediately; even-numbered
  voter sets (e.g., 2 or 4) are allowed but produce a startup **warning** about
  reduced fault tolerance (an even cluster of N tolerates ⌊(N-1)/2⌋ failures —
  the same as a cluster of N-1).  `e2e-scenarios.md` Feature 9 explicitly tests
  a 4-node symmetric partition to verify correct no-progress behaviour.
* Quorum = ⌊N/2⌋ + 1 (strict majority).
* Observers do not count toward quorum.

### 5.6  Crate Boundaries

The implementation is split into six workspace crates, aligned with the layout
defined in `architecture.md` §2:

| Crate | Responsibility |
|---|---|
| `xraft-core` | Protocol state machine, elections, log abstraction — no I/O |
| `xraft-storage` | Durable segmented log, snapshots, hard-state persistence |
| `xraft-transport` | gRPC service definitions and network transport (`tonic` + `prost`) |
| `xraft-server` | Binary that wires core + storage + transport; event loop, config, metrics, `AdminApi` |
| `xraft-client` | Internal peer RPC + admin client only — no external consumer SDK in v1 (see §2.6) |
| `xraft-test` | Deterministic simulation harness and integration test utilities |

These crate names are consistent across `architecture.md` §2 and
`implementation-plan.md` Stage 1.1.

`xraft-core` must be deterministic and I/O-free so it can be driven by both
real networking and deterministic simulation.

### 5.7  Dependencies (Key)

| Purpose | Crate | Rationale |
|---|---|---|
| Async runtime | `tokio` | Industry standard for Rust async |
| Serialisation (wire) | `prost` | Protobuf encoding for gRPC RPCs |
| Serialisation (disk) | Custom binary | On-disk log entries use `[length: u32][term: u64][index: u64][entry_type: u8][data: bytes][crc32: u32]` per `implementation-plan.md` Stage 2.1.  Protobuf is used on the wire; disk uses a compact binary format with CRC integrity checks for performance and simplicity. |
| Logging | `tracing` | Structured, async-aware |
| Metrics | `metrics` | Façade pattern; pluggable exporters |
| RPC | `tonic` + `prost` | Mature, HTTP/2-based gRPC framework (firm decision — see §2.3) |
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
| `fsync` latency dominates commit path | **Medium** — high commit latency | Batch log appends (KRaft's `BatchAccumulator` pattern) to amortise `fsync` cost across multiple entries; pipeline the *next* batch's accumulation while the current batch's `fsync` is in flight.  Note: `fsync` always completes before the corresponding RPC response is sent (per §5.3); the parallelism is between `fsync` and receiving/preparing the next batch, never between `fsync` and responding. |
| Pull-based fetch adds one extra round-trip vs. push | **Low** — slightly higher replication latency | Acceptable trade-off for simpler leader logic; tuneable fetch interval |
| Single-threaded event loop becomes bottleneck under high throughput | **Medium** | Batch processing within the loop; offload serialisation to I/O tasks |

### 6.3  Scope / Schedule Risk

| Risk | Impact | Mitigation |
|---|---|---|
| 13 story points may be tight for full snapshot + observer support | **Medium** — incomplete delivery | Prioritise core election + replication first; snapshot and observer are lower-priority extensions that can be deferred |
| Pull-based model is architecturally different from textbook Raft | **Medium** — design confusion | Document the mapping between KRaft concepts and standard Raft explicitly (see §2.1 and §2.2 for the reconciliation) |
| No existing Rust code in repo to build on | **Low** — cold start | Scaffold workspace with `cargo init` early; unblocks parallel work |

### 6.4  Operational Risk

| Risk | Impact | Mitigation |
|---|---|---|
| TLS is supported but optional — clusters deployed without TLS have unencrypted traffic | **Medium** — not production-safe without TLS enabled or network-level isolation | TLS configuration surface exists (§2.7); documentation should emphasise enabling TLS for production deployments |
| Static membership means replacing a failed node requires cluster restart | **Medium** — availability impact during maintenance | Document operational procedure; dynamic membership is out of scope for v1 and deferred to a future story (§2.7) |

---

## 7  Key Decisions — Resolved

The following decisions were pending in iteration 1 and are now resolved within
this spec to avoid cross-document dependency on files that may not yet exist:

1. **RPC transport → gRPC (`tonic`).**  gRPC provides HTTP/2 streaming (needed
   for `Fetch` long-poll), built-in backpressure, and proto codegen.  The heavier
   binary is an acceptable trade-off.  See §2.3.
2. **Log encoding format → dual-format.**  Wire RPCs use `protobuf` (`prost`)
   via `tonic` codegen.  On-disk log entries use a **custom binary format**:
   `[length: u32][term: u64][index: u64][entry_type: u8][data: bytes][crc32: u32]`,
   as specified in `implementation-plan.md` Stage 2.1.  The binary format avoids
   protobuf overhead on the hot append path and provides CRC32 integrity checking.
   Protobuf is reserved for the RPC layer where codegen and interop matter more.
3. **Pull-based replication → confirmed.**  Followers initiate `Fetch` RPCs to
   the leader.  This is a firm design decision, not pending.  The mapping from
   textbook Raft's push-based `AppendEntries` to KRaft's pull-based `Fetch` is
   documented in §2.1 and §2.2.
4. **Crate naming → aligned across sibling docs.**  `xraft-storage` (not
   `xraft-log`), `xraft-transport` (not `xraft-rpc`), `xraft-test` (not
   `xraft-testkit`), plus `xraft-client`.  See §5.6.  All three sibling
   documents (`architecture.md`, `implementation-plan.md`, `e2e-scenarios.md`)
   use the same names.
5. **Snapshot RPC naming → `FetchSnapshot`.**  All sibling docs define
   `FetchSnapshot` as the gRPC wire RPC.  `implementation-plan.md` uses
   `install_snapshot()` only as the internal handler function name on the
   follower side; it is not a wire RPC name.
6. **Dynamic membership scope → out of scope for v1.**  `AddVoter`/`RemoveVoter`
   RPCs are **out of scope for v1** and deferred to a future story entirely —
   not a stretch goal within XRAFT.  Per `architecture.md` §5.5,
   `e2e-scenarios.md` Feature 12, and `implementation-plan.md` Stage 7.2, the
   v1 deliverable uses static membership (voter set fixed at bootstrap) and
   observer support only.  Any `AddVoter`/`RemoveVoter` command is rejected
   with an `UNSUPPORTED` error.
7. **TLS → optional configuration surface.**  TLS is not mandatory for v1
   functional correctness but the configuration knobs exist per `architecture.md`.
   It is not "out of scope" but is not a gating requirement.

> **Cross-doc alignment (iteration 8):** This spec now uses the same crate
> names as all sibling docs (`xraft-storage`, `xraft-transport`, `xraft-test`).
> The `xraft-client` crate is correctly described as an **internal** peer RPC
> and admin client only — no external consumer SDK (`propose`/`read`) is in
> scope for v1 (per `architecture.md` §2.5 and `e2e-scenarios.md` Feature 11).
> Dynamic membership (`AddVoter`/`RemoveVoter`) is **out of scope for v1** and
> deferred to a future story entirely — not a stretch goal (per
> `architecture.md` §5.5, `e2e-scenarios.md` Feature 12, and
> `implementation-plan.md` Stage 7.2); any such command is rejected with
> `UNSUPPORTED`.  `implementation-plan.md` Stage 1.3 defines `AppendEntriesRequest`
> and `AppendEntriesResponse` as **internal-only proto types** (for the
> `Action::AppendEntries` side-effect); they are not exposed as network RPCs.
> The gRPC `RaftService` exposes only `Vote`, `PreVote`, `Fetch`, and
> `FetchSnapshot` as wire RPCs.  Quorum arithmetic allows any voter count ≥ 1,
> with a warning for even-numbered sets (per `implementation-plan.md` Stage
> 7.2).  Windows is a supported compilation target (the repository uses a
> Windows worktree).  TLS configuration surface exists and is optional (§2.7);
> it is implemented but not mandatory for v1 functional correctness.

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
*Story: failover-cluster:XRAFT · Status: Draft · Iteration 8*
