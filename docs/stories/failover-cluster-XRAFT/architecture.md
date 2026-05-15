# XRAFT Architecture

> Raft consensus protocol implemented in Rust, modelled after Apache Kafka's KRaft protocol.

---

## 1. System Overview

XRAFT is a Rust implementation of the Raft consensus protocol, drawing structural
inspiration from Apache Kafka's KRaft (KIP-500). Where KRaft manages Kafka cluster
metadata through a pull-based Raft variant, XRAFT generalises the approach into a
reusable consensus library and standalone cluster binary. The system provides:

- **Leader election** with Pre-Vote and Check-Quorum extensions.
- **Log replication** using a pull-based (Fetch) model rather than push-based AppendEntries.
- **Snapshot-based log compaction** for bounded storage and fast follower catch-up.
- **Static quorum membership** with a fixed voter set defined at cluster bootstrap.
- **An event-sourced metadata state machine** that replays a deterministic log.

The implementation targets clusters of 3, 5, or 7 voter nodes, with additional
non-voting observer nodes for read scale-out.

---

## 2. Component Architecture

The system is organised as six crates within a Cargo workspace at the
repository root. Crate names are aligned across all sibling documents
(`tech-spec.md` §5.6, `implementation-plan.md` Stage 1.1, and
`e2e-scenarios.md`):

```
Cargo.toml                       # workspace root
xraft-core/                      # Raft algorithm, pure logic, no I/O — defines all traits
xraft-storage/                   # Durable segmented log, snapshots, hard-state persistence
xraft-transport/                 # Network transport (gRPC via tonic)
xraft-server/                    # Node binary, wiring, configuration
xraft-client/                    # Internal peer RPC client and admin client (no external consumer SDK in v1)
xraft-test/                      # Deterministic test harness
```

### 2.1 `xraft-core` — Consensus Engine

**Responsibility:** Encapsulates all Raft state-machine logic with zero I/O
dependencies. Every decision (vote grant, log append, commit advance, leader
step-down) is a pure function of inputs and current state.

| Struct / Trait | Role |
|---|---|
| `RaftNode` | Top-level state machine. Accepts `Input` messages, emits `Vec<Action>` side-effects. |
| `NodeRole` | Enum: `Follower`, `Candidate`, `PreCandidate`, `Leader`, `Observer`. |
| `Term` | Newtype `u64`. Monotonically increasing logical clock. |
| `LogIndex` | Newtype `u64`. 1-based position in the replicated log. |
| `Entry` | `(LogIndex, Term, EntryPayload)` — a single log entry. |
| `EntryPayload` | Enum: `Command(Bytes)`, `NoOp`, `Snapshot(SnapshotMeta)`. A `ConfigChange(VoterSet)` variant is **reserved for future use** if dynamic membership is added in a future story (see §5.5); it is not emitted in the v1 static-membership baseline. |
| `HardState` | Persisted before any RPC reply: `current_term`, `voted_for`. (`commit_index` and `last_applied` are volatile, rebuilt from the log on recovery — see §3.3.) |
| `VoterSet` | Set of `(NodeId, NodeDirectoryId, Vec<Endpoint>)` tuples — the current quorum configuration. |
| `ElectionTimer` | Randomised election timeout (150–300 ms default). Reset on valid leader contact. |
| `Input` | Enum of all inputs: `Tick`, `VoteRequest`, `VoteResponse`, `PreVoteRequest`, `PreVoteResponse`, `FetchRequest`, `FetchResponse`, `ClientPropose`, `FetchRequestAcked` (driver feedback after a successful Stage 3.3 fetch read). |
| `Action` | Enum of all side-effects: `PersistHardState`, `AppendEntries`, `SendMessage`, `ApplyToStateMachine`, `TakeSnapshot`, `InstallSnapshot`, `BecomeLeader`, `StepDown`, `ServeFetch` (driver materialises a `FetchResponse` from the durable log), `TruncateLog` (follower divergence resolution), `RejectUnsupportedInput` (Stage 3.1/3.2 visible rejection of inputs whose handlers are deferred to Stage 3.3 — `ClientPropose` / `FetchRequest` / `FetchResponse` / `FetchRequestAcked`). |

**Key design choice — pull-based replication (KRaft-style):**
Unlike canonical Raft where the leader pushes `AppendEntries`, XRAFT followers
and observers periodically send `FetchRequest` RPCs to the leader. The leader
responds with new entries and the current high-water mark. This mirrors KRaft's
approach and offers better scalability: the leader does not manage per-follower
outbound connections.

Two fetch rounds are required for a follower to learn that an entry is committed:
1. Fetch round 1: follower receives new entries.
2. Fetch round 2: follower receives the advanced high-water mark (HW) reflecting
   majority replication from round 1.

**Pre-Vote protocol:**
Before incrementing its term and starting a real election, a node sends
`PreVoteRequest` RPCs. Followers that have heard from the leader within the
election timeout reject the pre-vote, preventing a partitioned node from
disrupting a healthy cluster.

**Check-Quorum:**
The leader periodically verifies it can communicate with a majority of voters.
If it cannot reach a quorum within `check_quorum_interval` (typically 2×
election timeout), it steps down to follower to avoid split-brain.

### 2.2 `xraft-storage` — Storage Engine

**Responsibility:** Durable, append-only log with segment files, plus snapshot
creation and loading. Provides file-backed implementations of the `LogStore`,
`SnapshotStore`, and `HardStateStore` traits defined in `xraft-core`.

| Trait / Struct | Role |
|---|---|
| `LogStore` (trait) | `append(entries)`, `truncate_after(index)`, `entry_at(index)`, `last_index_term()`, `flush()`. |
| `SegmentedLog` | Implements `LogStore`. Splits the log into fixed-size segment files (`00000000000000000000.log`). Supports `fsync`-on-write for durability. |
| `SnapshotStore` (trait) | `save_snapshot(meta, data)`, `load_latest_snapshot()`, `list_snapshots()`. |
| `FileSnapshotStore` | Implements `SnapshotStore`. Writes snapshots to `<data_dir>/snapshots/snapshot-<term>-<index>.bin` (naming convention follows `implementation-plan.md` Stage 2.3). |
| `HardStateStore` (trait) | `persist(HardState)`, `load() -> Option<HardState>`. |
| `FileHardStateStore` | Implements `HardStateStore`. Atomic write to `quorum-state` file (write-tmp + rename). |
| `LogSegment` | One segment file. Tracks base offset, byte size, entry count. |
| `SegmentIndex` | Memory-mapped sparse index for O(1) offset-to-position lookups within a segment. |

**Snapshot format:**
A snapshot captures the full state machine at a given `(last_included_index,
last_included_term)` plus the `VoterSet` at that point. Snapshots are
transferred in chunks via `FetchSnapshot` RPCs for large state machines.

**Log compaction policy:**
Segments whose entries are fully covered by the latest snapshot are eligible for
deletion. A configurable `log.retention.min_segments` (default 2) keeps recent
segments for slow followers.

### 2.3 `xraft-transport` — Network Transport

**Responsibility:** Defines the gRPC service and message types for all Raft RPCs.
Uses `tonic` (Rust gRPC) with `prost` for Protobuf serialisation. Implements the
`Transport` trait defined in `xraft-core`.

**Service definition (`raft.proto`):**

```protobuf
service RaftService {
  rpc Vote(VoteRequest)             returns (VoteResponse);
  rpc PreVote(PreVoteRequest)       returns (PreVoteResponse);
  rpc Fetch(FetchRequest)           returns (FetchResponse);
  rpc FetchSnapshot(FetchSnapshotRequest) returns (stream FetchSnapshotChunk);
}
```

| RPC | Direction | Purpose |
|---|---|---|
| `Vote` | Candidate → all voters | Leader election. Carries `candidate_id`, `term`, `last_log_index`, `last_log_term`. |
| `PreVote` | PreCandidate → all voters | Check electability without incrementing term. Same payload as `Vote`. |
| `Fetch` | Follower/Observer → Leader | Pull-based log replication. Carries `fetch_offset`, `last_fetched_epoch`, `replica_id`. |
| `FetchSnapshot` | Follower → Leader | Chunked snapshot transfer when follower is too far behind. |

**Identity and fencing fields (every RPC):**
- `cluster_id: String` — rejects cross-cluster messages.
- `leader_epoch: u64` — fences stale leaders; followers reject requests from old epochs.

**Transport configuration:**
- Listener address per node: `controller.listener.address`.
- TLS optional, configured via `tls.cert_path` / `tls.key_path`.
- Connection backoff: exponential with jitter, max 5 s.

### 2.4 `xraft-server` — Node Runtime

**Responsibility:** Wires together core, log, and RPC into a running node.
Owns the event loop, tick scheduling, and configuration loading.

| Struct | Role |
|---|---|
| `XRaftServer` | Top-level server. Initialises `RaftNode`, `SegmentedLog`, `GrpcTransport`, starts the event loop. |
| `EventLoop` | Single-threaded async loop (Tokio). Processes `Input` from RPC handlers + timer ticks, feeds them to `RaftNode`, dispatches resulting `Action`s. |
| `TickDriver` | Fires `Input::Tick` at `tick_interval` (default 50 ms). The core engine counts ticks to derive election and heartbeat timeouts. |
| `NodeConfig` | TOML configuration: `node_id`, `data_dir`, `cluster_peers`, `election_timeout_ms`, `tick_interval_ms`, `snapshot_interval_entries`, `log.segment_max_bytes`. |
| `MetricsRegistry` | Prometheus-compatible metrics: `current_leader`, `current_term`, `commit_latency_avg`, `append_records_rate`, `election_latency_avg`, `log_end_offset`, `replication_lag`. |
| `AdminApi` | HTTP API for operational commands: cluster status, trigger snapshot, node health. |

**Event loop architecture (KRaft-inspired):**
Like KRaft's `KafkaRaftClient`, the event loop is single-threaded to eliminate
concurrency hazards in consensus logic. All RPC handlers enqueue `Input`
messages onto an async channel; the loop drains the channel, feeds inputs to
`RaftNode`, and dispatches the resulting `Action` vector:

```
                  ┌─────────────┐
  gRPC handlers ──►  InputQueue  ├──► EventLoop ──► RaftNode.step(input)
  TickDriver   ──►  (mpsc)      │         │              │
                  └─────────────┘         │         Vec<Action>
                                          │              │
                                          ▼              ▼
                                   ┌──────────────────────────┐
                                   │   Action Dispatcher      │
                                   │  ┌─ PersistHardState ──► FileHardStateStore
                                   │  ├─ AppendEntries ─────► SegmentedLog
                                   │  ├─ SendMessage ───────► GrpcTransport
                                   │  ├─ ApplyToStateMachine ► StateMachineCallback
                                   │  ├─ TakeSnapshot ──────► FileSnapshotStore
                                   │  └─ InstallSnapshot ───► SegmentedLog + FSS
                                   └──────────────────────────┘
```

**Persistence ordering guarantee:**
`PersistHardState` and `AppendEntries` actions are executed and fsynced BEFORE
any `SendMessage` actions in the same batch. This mirrors Raft's safety
requirement that durable state is written before network acknowledgements.

### 2.5 `xraft-client` — Peer & Admin Client

**Responsibility:** An **internal** client crate used by `xraft-server` for
inter-node Raft RPCs and by admin tooling for cluster-management commands.
Per `tech-spec.md` §5.6 and §7, no external consumer SDK (`propose`/`read`)
is in scope for v1 — `xraft-client` serves only the internal peer and admin
roles.

`e2e-scenarios.md` Feature 11 tests the internal peer-client path (inter-node
routing and leader discovery).

| Struct / Trait | Role |
|---|---|
| `PeerClient` | Wraps `tonic` gRPC channel to a specific peer. Sends `Vote`, `PreVote`, `Fetch`, and `FetchSnapshot` RPCs. Handles connection lifecycle and reconnection. |
| `ConnectionPool` | Maintains a pool of `PeerClient` instances keyed by `NodeId`. Lazy-initialises connections on first use. |
| `AdminClient` | Connects to a node's admin HTTP endpoint for operational queries (leader status, metrics, trigger snapshot). |
| `ClientConfig` | Peer endpoint list, retry policy, connect/request timeouts. |

**Leader discovery:**
`PeerClient` tracks the last-known leader via hints
returned in `FetchResponse` and `VoteResponse` messages. When a connection fails
or returns a redirect, the client transparently retries against the hinted leader
endpoint.

### 2.6 `xraft-test` — Deterministic Testing

**Responsibility:** A simulated network and clock for testing the consensus
engine without real I/O. Inspired by deterministic simulation testing (Jepsen-style).

| Struct | Role |
|---|---|
| `SimulatedCluster` | Hosts N `RaftNode` instances with in-memory storage implementations. |
| `SimulatedNetwork` | Programmable message delivery: drop, delay, partition, duplicate. |
| `SimulatedClock` | Manual tick advancement for deterministic, reproducible tests. |
| `Invariant` | Pluggable assertion: `AtMostOneLeader`, `LogsConverge`, `CommittedEntriesNeverLost`. |

---

## 3. Data Model

### 3.1 Core Entities

```
┌──────────────────────────────────────────────────────┐
│  HardState (persisted atomically, quorum-state file) │
│  ─────────────────────────────────────────────────── │
│  current_term  : Term                                │
│  voted_for     : Option<NodeId>                      │
└──────────────────────────────────────────────────────┘
│  (commit_index and last_applied are volatile,        │
│   rebuilt from the log on recovery — see §3.3)       │

┌──────────────────────────────────────────────────────┐
│  LogEntry (persisted in segment files)               │
│  ─────────────────────────────────────────────────── │
│  index         : u64       (1-based, monotonic)      │
│  term          : u64                                 │
│  payload       : EntryPayload                        │
│  timestamp     : i64       (milliseconds)            │
│  crc32         : u32       (integrity check)         │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│  SnapshotMeta                                        │
│  ─────────────────────────────────────────────────── │
│  last_included_index : u64                           │
│  last_included_term  : u64                           │
│  voter_set           : VoterSet                      │
│  size_bytes          : u64                           │
│  checksum            : u64                           │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│  VoterRecord                                         │
│  ─────────────────────────────────────────────────── │
│  node_id       : u32                                 │
│  directory_id  : Uuid                                │
│  endpoints     : Vec<Endpoint>                       │
│  min_supported_version : u16                         │
│  max_supported_version : u16                         │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│  Endpoint                                            │
│  ─────────────────────────────────────────────────── │
│  name : String   (e.g. "CONTROLLER")                 │
│  host : String                                       │
│  port : u16                                          │
└──────────────────────────────────────────────────────┘
```

### 3.2 Leader Volatile State

The leader maintains per-follower replication tracking (in-memory only):

```
┌──────────────────────────────────────────────────────┐
│  ReplicaState (per follower, leader-only, volatile)  │
│  ─────────────────────────────────────────────────── │
│  node_id             : u32                           │
│  last_fetch_offset   : u64                           │
│  last_fetch_time     : Instant                       │
│  last_caught_up_time : Instant                       │
│  is_voter            : bool                          │
└──────────────────────────────────────────────────────┘
```

The leader uses `last_fetch_offset` across all voters to compute the **high-water
mark (HW)**: the highest log index replicated to a majority. Only entries at or
below HW are considered committed.

### 3.3 On-Disk Layout

```
<data_dir>/
├── quorum-state                              # HardState (JSON, atomic write)
├── metadata/
│   └── __cluster_metadata-0/                 # log partition
│       ├── 00000000000000000000.log           # segment 0
│       ├── 00000000000000000000.index         # sparse index for segment 0
│       ├── 00000000000000001024.log           # segment 1
│       ├── 00000000000000001024.index
│       └── leader-epoch-checkpoint            # (epoch, start_offset) pairs
├── snapshots/
│   ├── snapshot-0000000003-00000000000000000512.bin  # snapshot at term 3, index 512
│   └── snapshot-0000000005-00000000000000001024.bin
└── node.toml                                 # node configuration
```

---

## 4. Interfaces Between Components

### 4.1 Trait Boundaries

The core engine depends only on traits, never on concrete implementations.
This is the primary seam for testing and future storage backends.

> **Trait locations — aligned across docs:** All trait definitions (`LogStore`,
> `SnapshotStore`, `HardStateStore`, `Transport`, `StateMachine`) live in
> `xraft-core` so that `xraft-core` has zero dependencies on other xraft crates.
> Implementation crates (`xraft-storage`, `xraft-transport`) import the trait
> definitions from `xraft-core` and provide concrete implementations.
> `implementation-plan.md` Stage 2.1 confirms this placement: "`LogStore` trait
> in `xraft-core/src/storage.rs`" with `xraft-storage` providing `FileLogStore`.
> Stage 2.3 likewise places `SnapshotStore` in `xraft-core`.

```rust
// All trait definitions live in xraft-core:

trait LogStore: Send + Sync {
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    fn entry_at(&self, index: LogIndex) -> Result<Option<Entry>>;
    fn entries_from(&self, start: LogIndex, max_bytes: u64) -> Result<Vec<Entry>>;
    fn last_index_term(&self) -> (LogIndex, Term);
    fn truncate_after(&mut self, index: LogIndex) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn log_start_offset(&self) -> LogIndex;
}

trait SnapshotStore: Send + Sync {
    fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<()>;
    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>>;
    fn snapshot_exists(&self, index: LogIndex, term: Term) -> bool;
}

trait HardStateStore: Send + Sync {
    fn persist(&mut self, state: &HardState) -> Result<()>;
    fn load(&self) -> Result<Option<HardState>>;
}

// xraft-core also defines the network trait
// (xraft-transport provides the gRPC implementation):

trait Transport: Send + Sync {
    async fn send(&self, target: NodeId, message: RaftMessage) -> Result<()>;
    async fn broadcast(&self, targets: &[NodeId], message: RaftMessage) -> Result<()>;
}

// xraft-core also defines the state machine callback trait
// (user-provided, injected into the server at startup):

trait StateMachine: Send + Sync {
    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<Vec<u8>>;
    fn snapshot(&self) -> Result<Vec<u8>>;
    fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}
```

### 4.2 Inter-Crate Dependencies

```
xraft-server ──► xraft-core
             ──► xraft-storage
             ──► xraft-transport
             ──► xraft-client

xraft-client ──► xraft-core
             ──► xraft-transport

xraft-test   ──► xraft-core  (in-memory implementations of all traits)

xraft-storage ──► xraft-core  (imports core types: Entry, LogIndex, Term, HardState;
                               implements LogStore, SnapshotStore, HardStateStore)

xraft-transport ──► xraft-core  (imports core types and message definitions;
                                 implements Transport)
```

`xraft-core` has **zero** dependencies on other xraft crates. It defines all
traits and core types. Implementation crates depend on `xraft-core` to import
those definitions, never the reverse. This ensures `xraft-core` can be tested
in isolation with `xraft-test`.

### 4.3 Event Loop Integration Points

The `EventLoop` in `xraft-server` connects the components:

```
                         ┌───────────────┐
     ┌──── gRPC ────────►│               │
     │                   │  EventLoop    │──── LogStore (xraft-storage)
     ├──── TickDriver ──►│  (single      │──── HardStateStore (xraft-storage)
     │                   │   thread)     │──── SnapshotStore (xraft-storage)
     └──── AdminApi ────►│               │──── Transport (xraft-transport)
                         │  RaftNode     │──── StateMachine (user-provided)
                         │  (xraft-core) │──── MetricsRegistry
                         └───────────────┘
```

---

## 5. End-to-End Sequence Flows

> **Fetch offset convention (all flows below):** `fetch_offset` is the **next
> log index the follower wants to receive**. A `FetchReq(fetch_offset=N)`
> means "send me entries starting at index N". The leader responds with entries
> `[N, N+1, ...]` if available, along with the current high-water mark.

### 5.1 Leader Election (with Pre-Vote)

```
  Node A (Follower)         Node B (Follower)         Node C (Follower)
       │                         │                         │
       │  [election timeout]     │                         │
       │  become PreCandidate    │                         │
       │                         │                         │
       ├── PreVoteReq(term+1) ──►│                         │
       ├── PreVoteReq(term+1) ────────────────────────────►│
       │                         │                         │
       │◄── PreVoteResp(ok) ─────┤                         │
       │◄── PreVoteResp(ok) ──────────────────────────────┤│
       │                         │                         │
       │  [majority pre-votes]   │                         │
       │  increment term         │                         │
       │  become Candidate       │                         │
       │  vote for self          │                         │
       │                         │                         │
       ├── VoteReq(term=2) ─────►│                         │
       ├── VoteReq(term=2) ───────────────────────────────►│
       │                         │                         │
       │◄── VoteResp(granted) ───┤                         │
       │◄── VoteResp(granted) ───────────────────────────┤│
       │                         │                         │
       │  [majority votes]       │                         │
       │  become Leader(term=2)  │                         │
       │  append NoOp entry      │                         │
       │  persist HardState      │                         │
       │                         │                         │
       │  [await Fetch from      │                         │
       │   followers to          │                         │
       │   replicate NoOp]       │                         │
```

**Key safety properties enforced:**
- A node votes for at most one candidate per term.
- Pre-vote prevents term inflation from partitioned nodes.
- The leader appends a NoOp entry on election (KRaft's `LeaderChangeMessage`
  equivalent) to commit any uncommitted entries from prior terms.

### 5.2 Log Replication (Pull-Based Fetch)

```
  Client              Leader (A)          Follower (B)         Follower (C)
    │                    │                     │                     │
    ├── Propose(cmd) ──►│                     │                     │
    │                    │                     │                     │
    │                    │ append entry        │                     │
    │                    │ index=5, term=2     │                     │
    │                    │ persist + fsync     │                     │
    │                    │                     │                     │
    │                    │    ┌── Fetch round 1 ──┐                  │
    │                    │◄───┤ FetchReq(off=5)   │                  │
    │                    │    └────────────────────┘                  │
    │                    │                     │                     │
    │                    │── FetchResp ────────►│                     │
    │                    │   entries=[5]        │                     │
    │                    │   hw=4 (not yet)     │ append entry 5     │
    │                    │                     │ persist + fsync     │
    │                    │                     │                     │
    │                    │    ┌── Fetch from C ──────────────────────┐
    │                    │◄───────────────────────┤ FetchReq(off=5) │
    │                    │    └──────────────────────────────────────┘
    │                    │                     │                     │
    │                    │── FetchResp ─────────────────────────────►│
    │                    │   entries=[5]        │                     │
    │                    │   hw=4               │   append entry 5   │
    │                    │                     │                     │
    │                    │ [B,C replicated 5]   │                     │
    │                    │ advance hw to 5      │                     │
    │                    │ apply entry 5        │                     │
    │                    │ to state machine     │                     │
    │                    │                     │                     │
    │                    │    ┌── Fetch round 2 ──┐                  │
    │                    │◄───┤ FetchReq(off=6)   │                  │
    │                    │    └────────────────────┘                  │
    │                    │                     │                     │
    │                    │── FetchResp ────────►│                     │
    │                    │   entries=[]         │                     │
    │                    │   hw=5               │ apply entry 5      │
    │                    │                     │ to state machine    │
    │                    │                     │                     │
    │◄── Response(ok) ──┤                     │                     │
```

**Two-round commit visibility:** Followers learn the updated high-water mark on
their *next* fetch after the leader advances it. This is inherent to pull-based
replication and is consistent with KRaft's design.

### 5.3 Snapshot Transfer (Slow Follower Catch-Up)

```
  Leader (A)                               Slow Follower (D)
    │                                           │
    │◄────────── FetchReq(fetch_offset=101) ───┤
    │                                           │
    │  [offset 101 < log_start_offset(501)]     │
    │                                           │
    │── FetchResp(snapshot_hint) ──────────────►│
    │   snapshot_id=(index=500, term=4)         │
    │                                           │
    │◄── FetchSnapshotReq(offset=0, max=1MB) ──┤
    │── FetchSnapshotChunk(data, offset=0) ────►│
    │                                           │
    │◄── FetchSnapshotReq(offset=1MB) ─────────┤
    │── FetchSnapshotChunk(data, done=true) ───►│
    │                                           │
    │                               install snapshot │
    │                               restore state    │
    │                               machine          │
    │                               set log_start=500│
    │                                           │
    │◄────────── FetchReq(fetch_offset=501) ───┤
    │── FetchResp(entries=[501,502,...]) ───────►│
    │                                           │
```

### 5.4 Log Divergence Resolution

When a follower has divergent entries (e.g., from a stale leader's uncommitted
writes), the leader detects the mismatch and responds with a `DivergingEpoch`:

```
  Leader (A, term=3)                  Follower (B, has stale entries from term=2)
    │                                      │
    │◄── FetchReq(fetch_offset=11,     ───┤
    │             epoch=2)                 │
    │                                      │
    │  [leader checks leader-epoch-        │
    │   checkpoint: epoch 2 ended          │
    │   at offset 8]                       │
    │                                      │
    │── FetchResp ────────────────────────►│
    │   diverging_epoch=(epoch=2, end=8)   │
    │                                      │
    │                    truncate_after(8)  │
    │                    persist            │
    │                                      │
    │◄── FetchReq(fetch_offset=9,      ───┤
    │             epoch=3)                 │
    │── FetchResp(entries=[9,10,...]) ─────►│
    │                                      │
```

### 5.5 Check-Quorum Leader Step-Down

Dynamic quorum changes (`AddVoter` / `RemoveVoter`) are **out of scope for v1**
and deferred entirely to a future story — they are not a stretch goal within
XRAFT (see `tech-spec.md` §3 and §7, and `e2e-scenarios.md` Feature 12).
The core v1 deliverable uses **static membership** (voter set fixed at cluster
bootstrap). `implementation-plan.md` Stage 7.2 covers static voter set bootstrap
and observer support only; any `AddVoter`/`RemoveVoter` command is rejected with
an `UNSUPPORTED` error.

This section documents the Check-Quorum protocol that prevents stale leadership:

```
  Leader (A, term=3)            Follower (B)             Follower (C)
    │                              │                         │
    │  [check_quorum_interval      │                         │
    │   ticks elapsed]             │                         │
    │                              │                         │
    │  scan ReplicaState:          │                         │
    │   B.last_fetch_time = 200ms  │                         │
    │   C.last_fetch_time = 8000ms │                         │
    │   (C appears unreachable)    │                         │
    │                              │                         │
    │  [count reachable voters:    │                         │
    │   self + B = 2 / 3 = ok]     │                         │
    │  → remain Leader             │                         │
    │                              │                         │
    │  ─── next interval ──────    │                         │
    │                              │                         │
    │  scan ReplicaState:          │                         │
    │   B.last_fetch_time = 9000ms │                         │
    │   C.last_fetch_time = 9500ms │                         │
    │   (B AND C unreachable)      │                         │
    │                              │                         │
    │  [count reachable voters:    │                         │
    │   self only = 1 / 3 = fail]  │                         │
    │  → step down to Follower     │                         │
    │  → clear leader state        │                         │
    │  → emit StepDown action      │                         │
```

**Purpose:** Check-Quorum prevents a leader partitioned from the majority from
continuing to accept proposals that it can never commit. By stepping down, it
allows the reachable majority to elect a new leader.

---

## 6. Safety Invariants

The following invariants are enforced by `xraft-core` and verified by
`xraft-test` in every test run:

| # | Invariant | Enforcement |
|---|---|---|
| S1 | **Election safety:** At most one leader per term. | `voted_for` persisted before granting vote; majority required. |
| S2 | **Leader append-only:** A leader never overwrites or deletes its own log entries. | `RaftNode` only appends when role is `Leader`. |
| S3 | **Log matching:** If two entries share the same index and term, all preceding entries are identical. | Fetch response includes `prev_log_index` / `prev_log_term`; mismatch triggers truncation. |
| S4 | **Leader completeness:** A candidate cannot win if its log is behind the majority. | Vote is rejected if candidate's `(last_log_term, last_log_index)` is behind the voter's. |
| S5 | **State machine safety:** No two nodes apply different entries at the same index. | Follows from S1–S4; entries below HW are immutable. |
| S6 | **Persistence before acknowledgement:** `HardState` and log entries are fsynced before any RPC response. | `Action` dispatch order in `EventLoop`. |

---

## 7. Metrics and Observability

Exposed via Prometheus endpoint (`/metrics`) on the admin HTTP port:

| Metric | Type | Description |
|---|---|---|
| `xraft_current_leader` | Gauge | Node ID of current leader; `-1` if unknown. |
| `xraft_current_term` | Gauge | Current Raft term. |
| `xraft_commit_index` | Gauge | Highest committed log index. |
| `xraft_log_end_offset` | Gauge | Highest log index (may be ahead of commit). |
| `xraft_election_latency_seconds` | Histogram | Time from candidacy to leader election. |
| `xraft_commit_latency_seconds` | Histogram | Time from proposal to commit (leader only). |
| `xraft_append_records_total` | Counter | Total entries appended. |
| `xraft_fetch_requests_total` | Counter | Total Fetch RPCs received (leader) or sent (follower). |
| `xraft_snapshot_installs_total` | Counter | Snapshots installed by this node. |
| `xraft_replication_lag` | Gauge (per replica) | Entries behind leader for each follower. |

---

## 8. Configuration Reference

```toml
# node.toml — per-node configuration (flat layout matching ClusterConfig struct)

node_id = 1
cluster_id = "xraft-cluster-001"
listen_addr = "0.0.0.0:6001"
peers = ["node0.example.com:6000", "node2.example.com:6002"]

# Timing (all have defaults if omitted)
election_timeout_min_ms = 150
election_timeout_max_ms = 300
fetch_interval_ms = 50
tick_interval_ms = 10

# Snapshots
snapshot_interval = 10000
max_log_entries_before_compaction = 100000

# Storage
data_dir = "/var/lib/xraft"
```

Environment variable overrides (applied after TOML parsing, before validation):

| Variable | Overrides |
|---|---|
| `XRAFT_NODE_ID` | `node_id` |
| `XRAFT_CLUSTER_ID` | `cluster_id` |
| `XRAFT_LISTEN_ADDR` | `listen_addr` |
| `XRAFT_PEERS` | `peers` (comma-separated, empty entries filtered) |
| `XRAFT_ELECTION_TIMEOUT_MIN_MS` | `election_timeout_min_ms` |
| `XRAFT_ELECTION_TIMEOUT_MAX_MS` | `election_timeout_max_ms` |
| `XRAFT_FETCH_INTERVAL_MS` | `fetch_interval_ms` |
| `XRAFT_TICK_INTERVAL_MS` | `tick_interval_ms` |
| `XRAFT_SNAPSHOT_INTERVAL` | `snapshot_interval` |
| `XRAFT_MAX_LOG_ENTRIES` | `max_log_entries_before_compaction` |
| `XRAFT_DATA_DIR` | `data_dir` |

---

## 9. Design Decisions and Trade-Offs

| Decision | Rationale | Trade-off |
|---|---|---|
| **Pull-based (Fetch) replication** over push-based AppendEntries | Matches KRaft's approach; leader doesn't manage per-follower outbound connections; scales better with many observers. | Two fetch rounds needed for commit visibility; slightly higher commit latency compared to push-based. |
| **Single-threaded event loop** for consensus logic | Eliminates lock contention and data races in the consensus hot path (KRaft does the same). | All consensus work is serialised; throughput bounded by single core. Mitigated by batching. |
| **Separate `xraft-core` crate with no I/O** | Enables deterministic testing with `xraft-test`; makes the algorithm auditable independent of runtime concerns. | Requires trait-based indirection for storage and transport. |
| **gRPC (tonic) for transport** | Mature Rust ecosystem; streaming support for snapshot transfer; schema evolution via protobuf. | Heavier than a custom TCP protocol; acceptable for controller-plane traffic. |
| **Segment-file log with memory-mapped index** | Proven pattern (Kafka, etcd); efficient sequential writes; O(1) lookups via sparse index. | Requires periodic compaction; index rebuild on unclean shutdown. |
| **Pre-Vote + Check-Quorum** by default | Prevents unnecessary elections from partitioned nodes and split-brain. | Adds one extra RPC round before election; negligible cost. |

---

## 10. Relationship to Sibling Documents

- **`tech-spec.md`**: Defines the problem statement, scope boundaries (in-scope vs. out-of-scope), hard constraints (language, concurrency model, persistence, timing), identified risks, and key resolved decisions (gRPC transport, protobuf encoding, pull-based replication). This document describes *what* the components are and how they interact; tech-spec establishes *why* those choices were made and *what limits* they operate under. **Crate naming:** all sibling documents now use the same names — `xraft-storage`, `xraft-transport`, `xraft-test` — per `tech-spec.md` §5.6 and §7 decision 4. **`xraft-client` scope:** `tech-spec.md` §5.6 and §7 confirm `xraft-client` is an **internal** peer RPC and admin client only — no external consumer SDK (`propose`/`read`) is in scope for v1 (see §2.5). **Dynamic membership:** `AddVoter`/`RemoveVoter` is **out of scope for v1** and deferred to a future story entirely — not a stretch goal (per `tech-spec.md` §3 and §7 decision 6, and `e2e-scenarios.md` Feature 12). `implementation-plan.md` Stage 7.2 covers static voter set bootstrap and observer support only.
- **`implementation-plan.md`**: Breaks this architecture into ordered implementation phases with crate-level milestones. References component names from this document. Where `implementation-plan.md` uses the name `InstallSnapshot`, it refers to the same operation as `FetchSnapshot` defined here. **Trait locations:** `implementation-plan.md` Stage 2.1 and Stage 2.3 confirm that all trait definitions (`LogStore`, `SnapshotStore`) live in `xraft-core`; implementation crates import and implement them (see §4.1). **Snapshot filename convention:** this document adopts the `snapshot-{term}-{index}.bin` naming from `implementation-plan.md` Stage 2.3.
- **`e2e-scenarios.md`**: Defines integration test scenarios (election under partition, snapshot catch-up, check-quorum step-down) against the sequence flows in Section 5.
