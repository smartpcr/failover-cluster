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
  Dynamic membership (`AddVoter`/`RemoveVoter`) is **out of scope for v1** and
  deferred to a future story entirely (per `tech-spec.md` §2.7).
- **A consumer-provided `StateMachine` trait** with `apply`, `snapshot`, and
  `restore` methods (per `implementation-plan.md` Stage 5.1). XRAFT does not
  prescribe application semantics — consumers implement their own state machine
  (e.g., a key-value store) and inject it at server startup. Library consumers
  submit proposals through `xraft-server`'s embedded `propose` API (see §2.4).

The implementation accepts any voter count ≥ 1. Odd-numbered clusters (3, 5, 7)
are recommended because they maximise fault tolerance per node; even-numbered
voter sets are allowed but produce a startup warning about reduced fault
tolerance (per `tech-spec.md` §5.5 and `implementation-plan.md` Stage 7.2).
A single-voter cluster self-elects immediately. Non-voting observer nodes
provide read scale-out and standby capacity.

---

## 2. Component Architecture

The system is planned as six crates within a Cargo workspace at the
repository root. (The worktree currently contains only `README.md` and `docs/`;
these crates will be created during implementation per `implementation-plan.md`
Stage 1.1.) Crate names are aligned across all sibling documents
(`tech-spec.md` §5.6, `implementation-plan.md` Stage 1.1, and
`e2e-scenarios.md`):

```
Cargo.toml                       # workspace root (planned)
xraft-core/                      # Raft algorithm, pure logic, no I/O — defines all traits
xraft-storage/                   # Durable segmented log, snapshots, hard-state persistence
xraft-transport/                 # Network transport (gRPC via tonic)
xraft-server/                    # Node binary, wiring, configuration
xraft-client/                    # Internal peer RPC + admin client (not an external SDK — see §2.5)
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
| `EntryPayload` | Enum: `Command(Bytes)`, `NoOp`, `Snapshot(SnapshotMeta)`, `ConfigChange(VoterSet)`. Dynamic membership (`AddVoter`/`RemoveVoter`) is **out of scope for v1** and deferred to a future story entirely (per `tech-spec.md` §2.7/§3/§7). The `ConfigChange` variant is reserved in the enum so the log format is forward-compatible, but the `RaftNode` rejects any `ConfigChange` proposal with `UNSUPPORTED` in this story. |
| `HardState` | Persisted atomically before any RPC reply. Contains three fields: `current_term: Term`, `voted_for: Option<NodeId>`, `commit_index: LogIndex` (per `implementation-plan.md` Stage 2.2 and Stage 7.2). All three fields are written to the `quorum-state` file in a single atomic operation alongside `VoterSet`. `last_applied` is volatile, rebuilt from the log on recovery by replaying committed entries from `commit_index` (see §3.3). |
| `VoterSet` | Set of `(NodeId, NodeDirectoryId, Vec<Endpoint>)` tuples — the current quorum configuration. |
| `ElectionTimer` | Randomised election timeout (150–300 ms default). Reset on valid leader contact. |
| `Input` | Enum of all inputs: `Tick`, `VoteRequest`, `VoteResponse`, `FetchRequest`, `FetchResponse`, `ClientPropose`. |
| `Action` | Enum of all side-effects: `PersistHardState`, `AppendEntries`, `SendMessage`, `ApplyToStateMachine`, `TakeSnapshot`, `InstallSnapshot`, `BecomeLeader`, `StepDown`. |

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
| `LogStore` (trait) | `append(entries)`, `get(index)`, `get_range(start, end)`, `last_index()`, `last_term()`, `truncate_from(index)`, `term_at(index)`. |
| `FileLogStore` | Implements `LogStore`. Splits the log into fixed-size segment files (`00000000000000000000.log`). Supports `fsync`-on-write for durability. Named `FileLogStore` per `implementation-plan.md` Stage 2.1. |
| `SnapshotStore` (trait) | `save_snapshot(metadata, data)`, `load_latest_snapshot()`, `list_snapshots()`, `delete_snapshot(id)`. |
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

**Responsibility:** Provides the runtime gRPC server and client implementations
for all Raft RPCs, implementing the `Transport` trait defined in `xraft-core`.
Uses `tonic` (Rust gRPC) with `prost` for Protobuf serialisation.

**Proto and message ownership:** The `proto/raft.proto` file, the `build.rs`
that compiles it, and the generated Protobuf types all live in **`xraft-core`**
(per `implementation-plan.md` Stage 1.3). `xraft-core/src/message.rs`
re-exports the generated types and provides `From`/`Into` conversions between
proto types and core Rust types. `xraft-transport` **imports** these proto
types from `xraft-core` and uses them to implement the gRPC service handler
and the outbound `Transport` client.

**Service definition (`proto/raft.proto` — located in `xraft-core`):**

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
Owns the event loop, tick scheduling, and configuration loading. Also exposes
the **embedded propose/read API** for library consumers (since `xraft-client`
is internal-only; see §2.5).

| Struct | Role |
|---|---|
| `XRaftServer` | Top-level server. Initialises `RaftNode`, `FileLogStore`, `GrpcTransport`, starts the event loop. Exposes `propose(command: Bytes) -> Result<LogIndex>` for library consumers to submit commands to the leader. The consumer-provided `StateMachine` (see §4.1) receives applied entries via its `apply` callback; reads against committed state are the consumer's responsibility (XRAFT does not prescribe application-level read semantics — `tech-spec.md` §3 and `implementation-plan.md` Stage 5.1). |
| `EventLoop` | Single-threaded async loop (Tokio). Processes `Input` from RPC handlers + timer ticks, feeds them to `RaftNode`, dispatches resulting `Action`s. |
| `TickDriver` | Fires `Input::Tick` at `tick_interval` (default 50 ms). The core engine counts ticks to derive election and heartbeat timeouts. |
| `NodeConfig` | TOML configuration with a flat field layout matching `ClusterConfig` from `implementation-plan.md` Stage 1.2: `node_id`, `cluster_id`, `data_dir`, `listen_addr`, `peers`, `bootstrap_voters`, `bootstrap_observers`, `election_timeout_min_ms`, `election_timeout_max_ms`, `tick_interval_ms`, `fetch_interval_ms`, `snapshot_interval`, `max_log_entries_before_compaction`, `enable_check_quorum`, `enable_leader_lease`, `segment_max_bytes` (see §8 for the full reference and mapping). |
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
                                   │  ├─ AppendEntries ─────► FileLogStore
                                   │  ├─ SendMessage ───────► GrpcTransport
                                   │  ├─ ApplyToStateMachine ► StateMachineCallback
                                   │  ├─ TakeSnapshot ──────► FileSnapshotStore
                                   │  └─ InstallSnapshot ───► FileLogStore + FSS
                                   └──────────────────────────┘
```

**Persistence ordering guarantee:**
`PersistHardState` and `AppendEntries` actions are executed and fsynced BEFORE
any `SendMessage` actions in the same batch. This mirrors Raft's safety
requirement that durable state is written before network acknowledgements.

### 2.5 `xraft-client` — Internal Peer & Admin Client

**Responsibility:** Provides the **internal** peer-to-peer RPC layer used
by `xraft-server` for inter-node Raft RPCs (Fetch, Vote, FetchSnapshot) and
an `AdminClient` for operational queries. `xraft-client` is **not** an
external consumer SDK — no `propose`/`read` API for outside callers is in
scope for v1 (per `tech-spec.md` §2.6 and `e2e-scenarios.md` Feature 11).
Library consumers submit proposals and reads through `xraft-server`'s
embedded server API instead.

| Struct / Trait | Role |
|---|---|
| `PeerClient` | Wraps `tonic` gRPC channel to a specific peer. Sends `Vote`, `PreVote`, `Fetch`, and `FetchSnapshot` RPCs. Handles connection lifecycle and reconnection. |
| `ConnectionPool` | Maintains a pool of `PeerClient` instances keyed by `NodeId`. Lazy-initialises connections on first use. |
| `AdminClient` | Connects to a node's admin HTTP endpoint for operational queries (leader status, metrics, trigger snapshot). |
| `ClientConfig` | Peer endpoint list, retry policy, connect/request timeouts. |

**Leader discovery:**
`PeerClient` tracks the last-known leader via hints returned in `FetchResponse`
and `VoteResponse` messages. When a connection fails or returns a redirect
(`NOT_LEADER` with `leader_hint`), the client transparently retries against the
hinted leader endpoint.

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
│  current_term  : u64                                 │
│  voted_for     : Option<NodeId>                      │
│  commit_index  : u64                                 │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│  quorum-state file (top-level fields)                │
│  ─────────────────────────────────────────────────── │
│  hard_state    : HardState (current_term, voted_for, │
│                              commit_index)           │
│  voter_set     : VoterSet                            │
└──────────────────────────────────────────────────────┘

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
│  node_id       : u64                                 │
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
│  node_id             : u64                           │
│  last_fetch_offset   : u64                           │
│  last_fetch_time     : Instant                       │
│  last_caught_up_time : Instant                       │
│  is_voter            : bool                          │
└──────────────────────────────────────────────────────┘
```

The leader updates `last_fetch_offset` to the value carried in each incoming
`FetchReq(fetch_offset=N)`. Because `fetch_offset` is the **next** index the
follower wants, the highest index the follower has **already replicated** is
`last_fetch_offset - 1`. The leader computes the **high-water mark (HW)** as
the highest index `I` such that a strict majority of voters satisfy
`last_fetch_offset - 1 >= I` (i.e., they have replicated index `I`).
Only entries at or below HW are considered committed.

### 3.3 On-Disk Layout

```
<data_dir>/
├── quorum-state                              # HardState (term + voted_for + commit_index) + VoterSet (JSON, atomic write)
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
    fn get(&self, index: LogIndex) -> Result<Option<Entry>>;
    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>>;
    fn last_index(&self) -> LogIndex;
    fn last_term(&self) -> Term;
    fn truncate_from(&mut self, index: LogIndex) -> Result<()>;
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>>;
}

trait SnapshotStore: Send + Sync {
    fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> Result<()>;
    fn load_latest_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>>;
    fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>>;
    fn delete_snapshot(&mut self, id: &str) -> Result<()>;
}

trait HardStateStore: Send + Sync {
    fn persist(&mut self, state: &HardState) -> Result<()>;
    fn load(&self) -> Result<Option<HardState>>;
}

// xraft-core also defines the network trait
// (xraft-transport provides the gRPC implementation):

trait Transport: Send + Sync {
    async fn send_vote(&self, target: NodeId, req: VoteRequest) -> Result<VoteResponse>;
    async fn send_pre_vote(&self, target: NodeId, req: PreVoteRequest) -> Result<PreVoteResponse>;
    async fn send_fetch(&self, target: NodeId, req: FetchRequest) -> Result<FetchResponse>;
    async fn send_fetch_snapshot(&self, target: NodeId, req: FetchSnapshotRequest)
        -> Result<impl Stream<Item = Result<FetchSnapshotChunk>>>;
    async fn start_server(&self, addr: SocketAddr) -> Result<()>;
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
  Server-local          Leader (A)          Follower (B)         Follower (C)
  StateMachine caller
    │                    │                     │                     │
    ├── Propose(cmd) ──►│                     │                     │
    │                    │                     │                     │
    │                    │ append entry        │                     │
    │                    │ index=5, term=2     │                     │
    │                    │ persist + fsync     │                     │
    │                    │                     │                     │
    │                    │    ┌── Fetch round 1 (B) ──┐              │
    │                    │◄───┤ FetchReq(off=5)       │              │
    │                    │    └────────────────────────┘              │
    │                    │                     │                     │
    │                    │  update B.last_fetch_offset=5             │
    │                    │  (B replicated up to 4)                   │
    │                    │  HW check: majority at ≥5? no            │
    │                    │                     │                     │
    │                    │── FetchResp ────────►│                     │
    │                    │   entries=[5]        │                     │
    │                    │   hw=4               │ append entry 5     │
    │                    │                     │ persist + fsync     │
    │                    │                     │                     │
    │                    │    ┌── Fetch round 1 (C) ──────────────────┐
    │                    │◄───────────────────────┤ FetchReq(off=5)  │
    │                    │    └───────────────────────────────────────┘
    │                    │                     │                     │
    │                    │  update C.last_fetch_offset=5             │
    │                    │  (C replicated up to 4)                   │
    │                    │  HW check: majority at ≥5? no            │
    │                    │                     │                     │
    │                    │── FetchResp ─────────────────────────────►│
    │                    │   entries=[5]        │                     │
    │                    │   hw=4               │   append entry 5   │
    │                    │                     │   persist + fsync   │
    │                    │                     │                     │
    │                    │    ┌── Fetch round 2 (B) ──┐              │
    │                    │◄───┤ FetchReq(off=6)       │              │
    │                    │    └────────────────────────┘              │
    │                    │                     │                     │
    │                    │  update B.last_fetch_offset=6             │
    │                    │  (B replicated up to 5)                   │
    │                    │  HW check: A(leader)=5, B=5              │
    │                    │   → majority (2/3) at ≥5                 │
    │                    │   → advance HW to 5                      │
    │                    │   → apply entry 5 to state machine       │
    │                    │                     │                     │
    │                    │── FetchResp ────────►│                     │
    │                    │   entries=[]         │                     │
    │                    │   hw=5               │ apply entry 5      │
    │                    │                     │ to state machine    │
    │                    │                     │                     │
    │◄── Response(ok) ──┤                     │                     │
```

**When does HW advance?** The leader can only learn that a follower has
replicated entry `I` when it receives a `FetchReq(fetch_offset=I+1)` — the
follower asking for the *next* entry proves it has persisted all entries up to
`I`. In the flow above, the leader advances HW to 5 only when B's second fetch
(`off=6`) proves B has replicated 5, giving a majority (A + B = 2 of 3 voters).

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
    │                               set log_start=501│
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

The leader periodically verifies that a majority of voters are still reachable
by checking `last_fetch_time` in its `ReplicaState` table. If fewer than a
quorum of voters have fetched within the check-quorum interval, the leader
steps down to prevent a stale leader from accepting proposals it can never commit.

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
| S3 | **Log matching:** If two entries share the same index and term, all preceding entries are identical. | Enforced via KRaft-style `DivergingEpoch`: the follower sends `last_fetched_epoch` in each Fetch request; the leader returns a `DivergingEpoch(epoch, end_offset)` when the follower's epoch does not match, triggering truncation to the divergence point (see §5.4 and `tech-spec.md` §2.2). |
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
# node.toml — per-node configuration
#
# The on-disk config surface maps directly to ClusterConfig fields
# defined in implementation-plan.md Stage 1.2. The table below shows
# the correspondence:
#
#   TOML field                  → ClusterConfig field
#   node_id                     → node_id
#   cluster_id                  → cluster_id
#   data_dir                    → data_dir
#   listen_addr                 → listen_addr
#   peers                       → peers (Vec of peer addresses)
#   bootstrap_voters            → (parsed into VoterSet at startup)
#   bootstrap_observers         → (parsed into observer set at startup)
#   election_timeout_min_ms     → election_timeout_min_ms
#   election_timeout_max_ms     → election_timeout_max_ms
#   tick_interval_ms            → tick_interval_ms
#   fetch_interval_ms           → fetch_interval_ms
#   snapshot_interval           → snapshot_interval
#   max_log_entries_before_compaction → max_log_entries_before_compaction
#   enable_check_quorum         → enable_check_quorum
#   enable_leader_lease         → enable_leader_lease

node_id = 1
cluster_id = "xraft-cluster-001"
data_dir = "/var/lib/xraft"
listen_addr = "0.0.0.0:6001"

# Peer addresses for all other nodes in the cluster
peers = [
  "node0.example.com:6000",
  "node2.example.com:6002",
]

# Voters participate in elections and quorum; observers replicate but do not vote.
bootstrap_voters = [
  { node_id = 0, host = "node0.example.com", port = 6000 },
  { node_id = 1, host = "node1.example.com", port = 6001 },
  { node_id = 2, host = "node2.example.com", port = 6002 },
]
bootstrap_observers = [
  { node_id = 3, host = "observer0.example.com", port = 6003 },
]

# Timing — all fields directly map to ClusterConfig
tick_interval_ms = 50
election_timeout_min_ms = 150
election_timeout_max_ms = 300
fetch_interval_ms = 50
enable_check_quorum = true
enable_leader_lease = false

# Storage
segment_max_bytes = 67_108_864       # 64 MiB per segment (per implementation-plan.md Stage 2.1)
max_log_entries_before_compaction = 50_000
snapshot_interval = 10_000           # snapshot every N committed entries

# Snapshot transfer
snapshot_max_chunk_bytes = 1_048_576 # 1 MiB per FetchSnapshot chunk

# Network
connect_timeout_ms = 500
request_timeout_ms = 2000

# TLS (optional)
tls_enabled = false
tls_cert_path = ""
tls_key_path = ""

# Admin HTTP
admin_listen_addr = "0.0.0.0:9090"
```

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

- **`tech-spec.md`**: Defines the problem statement, scope boundaries (in-scope vs. out-of-scope), hard constraints (language, concurrency model, persistence, timing), identified risks, and key resolved decisions (gRPC transport, protobuf encoding, pull-based replication). This document describes *what* the components are and how they interact; tech-spec establishes *why* those choices were made and *what limits* they operate under. **Crate naming:** all sibling documents now use the same names — `xraft-storage`, `xraft-transport`, `xraft-test` — per `tech-spec.md` §5.6 and §7 decision 4. **`xraft-client` scope:** `xraft-client` is an **internal** crate providing (a) peer-to-peer RPC (Fetch, Vote, FetchSnapshot) for inter-node communication and (b) admin/operational queries via `AdminClient`. It is **not** an external consumer SDK — no `propose`/`read` API for outside callers is in scope for v1 (per `tech-spec.md` §2.6 and `e2e-scenarios.md` Feature 11). Library consumers submit proposals and reads through `xraft-server`'s embedded API (see §2.4). **Dynamic membership:** `AddVoter`/`RemoveVoter` is **out of scope for v1** and deferred to a future story entirely — it is not a stretch goal within XRAFT (per `tech-spec.md` §2.7/§3/§7 and `e2e-scenarios.md` Feature 12). The `ConfigChange(VoterSet)` variant is reserved in `EntryPayload` so the log format is forward-compatible, but the `RaftNode` rejects any `ConfigChange` proposal with `UNSUPPORTED`. The v1 baseline uses static membership (voter set fixed at bootstrap) with observer support.
- **`implementation-plan.md`**: Breaks this architecture into ordered implementation phases with crate-level milestones. References component names from this document. Where `implementation-plan.md` uses the name `InstallSnapshot`, it refers to the same operation as `FetchSnapshot` defined here. **Proto ownership:** `proto/raft.proto`, `build.rs`, and the generated Protobuf types all live in `xraft-core` (per `implementation-plan.md` Stage 1.3); `xraft-core/src/message.rs` re-exports the generated types. `xraft-transport` imports these proto types from `xraft-core` and provides the runtime gRPC server/client implementation (see §2.3). **Trait locations:** `implementation-plan.md` Stage 2.1 and Stage 2.3 confirm that all trait definitions (`LogStore`, `SnapshotStore`) live in `xraft-core`; implementation crates import and implement them (see §4.1). **Trait signatures:** §4.1 method names and shapes are aligned with `implementation-plan.md` — `LogStore` uses `append`, `get`, `get_range`, `last_index`, `last_term`, `truncate_from`, `term_at` (Stage 2.1); `SnapshotStore` uses `save_snapshot`, `load_latest_snapshot`, `list_snapshots`, `delete_snapshot(id)` (Stage 2.3); `Transport` uses `send_vote`, `send_pre_vote`, `send_fetch`, `send_fetch_snapshot`, `start_server` (Stage 4.1). **HardState fields:** `HardState` contains three fields: `current_term: Term`, `voted_for: Option<NodeId>`, and `commit_index: LogIndex` (per `implementation-plan.md` Stage 2.2 and Stage 7.2). All three are persisted atomically to the `quorum-state` file alongside `VoterSet`. `last_applied` is volatile, rebuilt from the log on recovery by replaying committed entries from `commit_index`. **Log implementation naming:** the concrete log implementation is named `FileLogStore` (per `implementation-plan.md` Stage 2.1), backed by append-only segment files. **Segment default size:** 64 MiB (per `implementation-plan.md` Stage 2.1). **NodeId type:** `NodeId` is `u64` everywhere (per `implementation-plan.md` Stage 1.2). **Snapshot filename convention:** this document adopts the `snapshot-{term}-{index}.bin` naming from `implementation-plan.md` Stage 2.3.
- **`e2e-scenarios.md`**: Defines integration test scenarios (election under partition, snapshot catch-up, check-quorum step-down) against the sequence flows in Section 5.
