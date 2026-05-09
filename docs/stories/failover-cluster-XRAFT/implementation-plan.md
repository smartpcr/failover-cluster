---
title: "xraft"
storyId: "failover-cluster:XRAFT"
---

# Phase 1: Project Scaffolding

## Dependencies
- _none — start phase_

## Stage 1.1: Cargo Workspace and Crate Layout

### Implementation Steps
- [ ] Initialize Cargo workspace at repo root with `Cargo.toml` defining members: `xraft-core`, `xraft-storage`, `xraft-transport`, `xraft-server`, `xraft-client`
- [ ] Create `xraft-core` crate with `lib.rs` exporting top-level modules: `types`, `config`, `error`, `message`
- [ ] Create `xraft-storage` crate with `lib.rs` stub and dependency on `xraft-core`
- [ ] Create `xraft-transport` crate with `lib.rs` stub and dependency on `xraft-core`
- [ ] Create `xraft-server` crate (binary) with `main.rs` stub and dependencies on all library crates
- [ ] Create `xraft-client` crate (library) with `lib.rs` stub and dependency on `xraft-core` and `xraft-transport`
- [ ] Add shared workspace dependencies in root `Cargo.toml`: `tokio`, `serde`, `serde_json`, `tracing`, `tracing-subscriber`, `thiserror`, `bytes`, `prost`, `tonic`
- [ ] Add `rust-toolchain.toml` pinning stable Rust edition 2024
- [ ] Add `.gitignore` for Rust (`/target`, `Cargo.lock` for libraries)
- [ ] Verify `cargo check --workspace` succeeds with no errors

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: workspace-compiles — Given the newly created workspace, When `cargo check --workspace` is run, Then it exits with code 0 and no errors
- [ ] Scenario: crate-dependency-graph — Given the workspace, When `cargo metadata` is inspected, Then `xraft-server` depends on all four library crates and `xraft-storage` depends on `xraft-core`

## Stage 1.2: Core Types and Configuration

### Implementation Steps
- [ ] Define `NodeId` (u64), `Term` (u64), `LogIndex` (u64), `DirectoryId` (Uuid) types in `xraft-core/src/types.rs` with `Serialize`, `Deserialize`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Debug` derives
- [ ] Define `NodeRole` enum (`Leader`, `Follower`, `Candidate`, `Observer`) in `xraft-core/src/types.rs`
- [ ] Define `ClusterConfig` struct in `xraft-core/src/config.rs` with fields: `node_id`, `cluster_id`, `listen_addr`, `peers` (Vec of peer addresses), `election_timeout_min_ms`, `election_timeout_max_ms`, `heartbeat_interval_ms`, `snapshot_interval`, `max_log_entries_before_compaction`, `data_dir`
- [ ] Implement config loading from TOML file and environment variable overrides using `serde` and `toml` crate
- [ ] Define `XRaftError` enum in `xraft-core/src/error.rs` using `thiserror` with variants: `Storage`, `Transport`, `NotLeader`, `ElectionTimeout`, `InvalidTerm`, `LogInconsistency`, `Shutdown`, `Config`
- [ ] Define `Result<T>` type alias as `std::result::Result<T, XRaftError>`
- [ ] Add unit tests for config deserialization and default values

### Dependencies
- phase-project-scaffolding/stage-cargo-workspace-and-crate-layout

### Test Scenarios
- [ ] Scenario: config-from-toml — Given a valid TOML config string, When deserialized into `ClusterConfig`, Then all fields match expected values including peer addresses
- [ ] Scenario: config-defaults — Given a minimal TOML with only required fields, When deserialized, Then optional fields use sensible defaults (election_timeout_min_ms=150, heartbeat_interval_ms=50)
- [ ] Scenario: error-display — Given each `XRaftError` variant, When formatted with Display, Then a human-readable message is produced

## Stage 1.3: RPC Message Definitions

### Implementation Steps
- [ ] Create `proto/raft.proto` defining protobuf messages: `VoteRequest`, `VoteResponse`, `FetchRequest`, `FetchResponse`, `AppendEntriesRequest`, `AppendEntriesResponse`
- [ ] Define `LogEntry` protobuf message with fields: `index`, `term`, `entry_type` (enum: Command, NoOp, Config), `data` (bytes)
- [ ] Define `SnapshotMetadata` protobuf message with fields: `last_included_index`, `last_included_term`, `voter_set`
- [ ] Define `InstallSnapshotRequest` and `InstallSnapshotResponse` protobuf messages for snapshot transfer in chunks
- [ ] Add `build.rs` in `xraft-core` using `tonic-build` to compile proto files
- [ ] Create `xraft-core/src/message.rs` re-exporting generated protobuf types and adding conversion traits (`From`/`Into`) between proto types and core Rust types
- [ ] Verify `cargo build --workspace` compiles protobuf definitions without errors

### Dependencies
- phase-project-scaffolding/stage-core-types-and-configuration

### Test Scenarios
- [ ] Scenario: proto-roundtrip — Given a `VoteRequest` with term=5, candidate_id=1, last_log_index=10, last_log_term=4, When serialized and deserialized via prost, Then all fields match
- [ ] Scenario: log-entry-types — Given LogEntry messages of each entry_type variant, When serialized, Then the enum discriminant roundtrips correctly

# Phase 2: Persistent Storage

## Dependencies
- phase-project-scaffolding

## Stage 2.1: Write-Ahead Log

### Implementation Steps
- [ ] Define `LogStore` trait in `xraft-storage/src/log.rs` with methods: `append`, `get(index)`, `get_range(start, end)`, `last_index`, `last_term`, `truncate_from(index)`, `term_at(index)`
- [ ] Implement `FileLogStore` backed by append-only segment files in configurable data directory, each segment containing a header (magic bytes, version) and sequential log entries
- [ ] Implement segment rotation when a segment exceeds configurable size threshold (default 64 MB)
- [ ] Implement binary encoding for log entries: `[length: u32][term: u64][index: u64][entry_type: u8][data: bytes][crc32: u32]`
- [ ] Implement `fsync` after each append batch for durability guarantees
- [ ] Implement `truncate_from(index)` to remove entries from a given index onward (needed for conflict resolution)
- [ ] Add an in-memory index mapping `LogIndex -> (segment_file, byte_offset)` for O(1) lookups, rebuilt on startup by scanning segments

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: append-and-read — Given an empty FileLogStore, When 100 entries are appended, Then `get(i)` returns the correct entry for each index and `last_index` returns 100
- [ ] Scenario: truncate-divergent — Given a log with 50 entries, When `truncate_from(30)` is called, Then `last_index` returns 29 and `get(30)` returns None
- [ ] Scenario: segment-rotation — Given a segment size threshold of 1 KB, When enough entries are appended to exceed the threshold, Then a new segment file is created and reads across segments succeed
- [ ] Scenario: crash-recovery — Given a FileLogStore with written entries, When the process restarts and reopens the store, Then all previously appended entries are readable and the in-memory index is correct

## Stage 2.2: Persistent Raft State

### Implementation Steps
- [ ] Define `StateStore` trait in `xraft-storage/src/state.rs` with methods: `load() -> RaftState`, `save(state: &RaftState)`, `save_voted_for(term, candidate_id)`, `save_current_term(term)`
- [ ] Define `RaftState` struct with fields: `current_term: Term`, `voted_for: Option<NodeId>`, `commit_index: LogIndex`, `last_applied: LogIndex`
- [ ] Implement `FileStateStore` that persists `RaftState` as JSON to a `raft-state.json` file with atomic write (write to temp file then rename) for crash safety
- [ ] Implement `load()` that reads state on startup with fallback to default initial state (term=0, voted_for=None)
- [ ] Add validation in `save()` that term never decreases and voted_for is only set once per term

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: state-persistence — Given a saved RaftState with term=5 and voted_for=Some(3), When the store is reloaded, Then the loaded state matches exactly
- [ ] Scenario: atomic-write-safety — Given a state save in progress, When the process crashes mid-write (simulated by checking temp file), Then the previous valid state is still loadable
- [ ] Scenario: term-monotonicity — Given a state with term=5, When save() is called with term=3, Then an error is returned

## Stage 2.3: Snapshot Store

### Implementation Steps
- [ ] Define `SnapshotStore` trait in `xraft-storage/src/snapshot.rs` with methods: `save_snapshot(metadata, data)`, `load_latest_snapshot()`, `list_snapshots()`, `delete_snapshot(id)`
- [ ] Implement `FileSnapshotStore` that writes snapshots to `snapshots/` directory with filename pattern `snapshot-{term}-{index}.bin`
- [ ] Implement snapshot metadata header format: `[magic: u32][version: u16][last_included_index: u64][last_included_term: u64][voter_set_len: u32][voter_set: bytes][data_len: u64]`
- [ ] Implement snapshot cleanup: retain only the N most recent snapshots (configurable, default 3)
- [ ] Implement chunked snapshot reading for `InstallSnapshot` RPC support, returning iterators over fixed-size chunks (default 1 MB)

### Dependencies
- phase-persistent-storage/stage-write-ahead-log

### Test Scenarios
- [ ] Scenario: snapshot-save-load — Given state machine data serialized to bytes, When saved as a snapshot and loaded back, Then metadata and data match the original
- [ ] Scenario: snapshot-cleanup — Given 5 snapshots saved with retention=3, When cleanup runs, Then only the 3 most recent snapshots remain on disk
- [ ] Scenario: chunked-read — Given a 5 MB snapshot, When read in 1 MB chunks, Then 5 chunks are returned and concatenation equals the original data

# Phase 3: Raft Consensus Engine

## Dependencies
- phase-persistent-storage

## Stage 3.1: Raft Node State Machine

### Implementation Steps
- [ ] Create `xraft-core/src/node.rs` defining `RaftNode` struct holding: `id: NodeId`, `role: NodeRole`, `state: RaftState`, `log: Box<dyn LogStore>`, `state_store: Box<dyn StateStore>`, `config: ClusterConfig`, `election_timer: ElectionTimer`, `peers: HashMap<NodeId, PeerState>`
- [ ] Define `PeerState` struct tracking per-peer replication state: `next_index`, `match_index`, `last_fetch_time`, `is_voter: bool`
- [ ] Implement `ElectionTimer` with randomized timeout in range `[election_timeout_min, election_timeout_max]` using `rand` crate, with `reset()`, `is_expired()`, `remaining()` methods
- [ ] Implement role transition methods: `become_follower(term, leader_id)`, `become_candidate()`, `become_leader()` with appropriate state updates (reset timers, initialize peer state, append no-op entry)
- [ ] Implement `tick()` method that advances time by one logical tick: checks election timeout for followers/candidates, sends heartbeats for leaders
- [ ] Add `tracing` instrumentation to all state transitions and critical paths

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: initial-state — Given a new RaftNode, When created, Then role is Follower, term is 0, and election timer is running
- [ ] Scenario: election-timeout-triggers-candidacy — Given a Follower node, When election timer expires via repeated tick() calls, Then the node transitions to Candidate and increments term
- [ ] Scenario: become-leader-initializes-peers — Given a node becoming leader, When `become_leader()` is called, Then `next_index` for each peer is set to `last_log_index + 1` and a no-op entry is appended

## Stage 3.2: Leader Election

### Implementation Steps
- [ ] Implement `handle_vote_request(req: VoteRequest) -> VoteResponse` in `RaftNode`: validate term, check if vote already granted, verify candidate's log is at least as up-to-date (compare last_log_term then last_log_index)
- [ ] Implement `handle_vote_response(from: NodeId, resp: VoteResponse)` in `RaftNode`: track votes received, check quorum, transition to Leader if majority achieved
- [ ] Implement `start_election()` in `RaftNode`: increment term, vote for self, reset election timer with new random timeout, return list of `VoteRequest` messages to send to all peers
- [ ] Implement Pre-Vote mechanism: `handle_pre_vote_request()` and `handle_pre_vote_response()` that check electability without incrementing term — followers reject pre-votes if they have heard from a leader within the election timeout
- [ ] Add `VoteGrantedSet` to track votes per election, preventing double-counting

### Dependencies
- phase-raft-consensus-engine/stage-raft-node-state-machine

### Test Scenarios
- [ ] Scenario: vote-granted-up-to-date — Given a Follower at term=3 with log up to index=10, When it receives VoteRequest from candidate at term=4 with last_log_index=10 and last_log_term=3, Then it grants the vote and updates voted_for
- [ ] Scenario: vote-rejected-stale-term — Given a Follower at term=5, When it receives VoteRequest with term=3, Then it rejects the vote
- [ ] Scenario: vote-rejected-stale-log — Given a Follower at term=3 with last_log_term=3 and last_log_index=15, When it receives VoteRequest from candidate with last_log_term=2, Then it rejects the vote regardless of candidate's index
- [ ] Scenario: election-wins-majority — Given a 5-node cluster where node 1 starts an election at term=2, When nodes 2 and 3 grant votes, Then node 1 becomes Leader
- [ ] Scenario: pre-vote-prevents-disruption — Given a partitioned Follower, When it attempts Pre-Vote, Then other nodes with an active leader reject the pre-vote

## Stage 3.3: Log Replication

### Implementation Steps
- [ ] Implement `handle_fetch_request(req: FetchRequest) -> FetchResponse` in leader: validate term, return entries from requested offset, include current commit index as high watermark, detect diverging epochs and return `DivergingEpoch` info
- [ ] Implement `handle_fetch_response(from: NodeId, resp: FetchResponse)` in follower: append received entries, update commit index to min(leader_commit, last_new_entry_index), handle log truncation on diverging epoch
- [ ] Implement `replicate_entries()` in leader: for each peer, build FetchResponse with entries from `peer.next_index`, update `peer.match_index` on acknowledgment
- [ ] Implement commit advancement in leader: find the highest index N where a majority of `match_index[i] >= N` and `log[N].term == current_term`, then advance `commit_index` to N
- [ ] Implement `apply_committed()`: apply all log entries between `last_applied` and `commit_index` to the state machine interface, advancing `last_applied`
- [ ] Implement heartbeat mechanism: leader sends empty FetchResponse (no new entries) at `heartbeat_interval_ms` to prevent election timeouts
- [ ] Implement follower log conflict resolution: on receiving entries with conflicting term at same index, truncate local log from conflict point and append leader's entries

### Dependencies
- phase-raft-consensus-engine/stage-leader-election

### Test Scenarios
- [ ] Scenario: basic-replication — Given a 3-node cluster with node 1 as leader, When a log entry is appended, Then after fetch rounds all followers have the entry and commit_index advances
- [ ] Scenario: commit-requires-majority — Given a 5-node cluster leader with 2 slow followers, When 2 of 4 followers acknowledge, Then commit_index advances (majority of 5 is 3 including leader)
- [ ] Scenario: follower-conflict-resolution — Given a follower with entries [1:t1, 2:t1, 3:t2] and leader has [1:t1, 2:t1, 3:t3], When fetch response arrives, Then follower truncates entry 3 and appends leader's version
- [ ] Scenario: heartbeat-resets-timer — Given a follower, When it receives a heartbeat (empty fetch response) from the leader, Then its election timer resets and it remains a follower
- [ ] Scenario: stale-leader-steps-down — Given a leader at term=3, When it receives a FetchRequest or VoteRequest with term=5, Then it steps down to Follower at term=5

# Phase 4: Network Transport

## Dependencies
- phase-raft-consensus-engine

## Stage 4.1: gRPC Transport Layer

### Implementation Steps
- [ ] Define `RaftService` gRPC service in `proto/raft.proto` with RPCs: `Vote`, `PreVote`, `Fetch`, `InstallSnapshot`
- [ ] Implement `RaftGrpcServer` in `xraft-transport/src/grpc_server.rs` using `tonic` that accepts incoming RPCs and dispatches to `RaftNode` message handlers
- [ ] Implement `RaftGrpcClient` in `xraft-transport/src/grpc_client.rs` using `tonic` for sending RPCs to peers with configurable connection timeout and retry logic
- [ ] Implement connection pooling in `RaftGrpcClient`: maintain one persistent channel per peer, reconnect on failure with exponential backoff
- [ ] Define `Transport` trait in `xraft-transport/src/lib.rs` abstracting over network implementation with methods: `send_vote`, `send_pre_vote`, `send_fetch`, `send_install_snapshot`, `start_server`
- [ ] Implement the `Transport` trait for the gRPC implementation

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: grpc-vote-roundtrip — Given a running RaftGrpcServer, When a VoteRequest is sent via RaftGrpcClient, Then a VoteResponse is received with correct fields
- [ ] Scenario: connection-retry — Given a peer that is temporarily unreachable, When send_vote is called, Then the client retries with backoff and eventually succeeds when the peer comes back
- [ ] Scenario: concurrent-rpcs — Given a running server, When 50 concurrent Fetch RPCs arrive, Then all are processed without deadlock or data corruption

## Stage 4.2: Message Router and Driver Loop

### Implementation Steps
- [ ] Create `xraft-server/src/driver.rs` implementing the main async event loop using `tokio::select!` over: incoming RPC messages, outgoing RPC results, tick timer, client command channel, shutdown signal
- [ ] Implement `MessageRouter` that receives outbound messages from `RaftNode` and dispatches them via the `Transport` trait to appropriate peers
- [ ] Implement inbound message dispatching: deserialize incoming RPCs, route to `RaftNode` handler methods, send responses back through gRPC
- [ ] Implement tick scheduling using `tokio::time::interval` at 10ms granularity for election and heartbeat timers
- [ ] Implement graceful shutdown: drain in-flight RPCs, persist final state, close transport connections
- [ ] Implement client command channel (`tokio::sync::mpsc`) for submitting new log entries and waiting for commit confirmation

### Dependencies
- phase-network-transport/stage-grpc-transport-layer

### Test Scenarios
- [ ] Scenario: driver-processes-tick — Given a running driver loop, When the tick interval fires, Then `RaftNode.tick()` is called and any resulting messages are dispatched
- [ ] Scenario: driver-handles-shutdown — Given a running driver loop with in-flight operations, When shutdown signal is received, Then state is persisted and all connections close cleanly within 5 seconds
- [ ] Scenario: client-command-flow — Given a leader node's driver loop, When a client command is submitted via the command channel, Then it is appended to the log and the future resolves after commit

# Phase 5: State Machine Interface

## Dependencies
- phase-raft-consensus-engine

## Stage 5.1: Application State Machine Trait

### Implementation Steps
- [ ] Define `StateMachine` trait in `xraft-core/src/state_machine.rs` with methods: `apply(entry: &LogEntry) -> Result<Vec<u8>>`, `snapshot() -> Result<Vec<u8>>`, `restore(data: &[u8]) -> Result<()>`
- [ ] Implement `KeyValueStateMachine` as a reference implementation in `xraft-server/src/kv.rs` using `BTreeMap<String, Vec<u8>>` with operations: `Get`, `Put`, `Delete`, `CAS` (compare-and-swap)
- [ ] Define command serialization format for KV operations using serde and bincode
- [ ] Implement `snapshot()` for KV state machine: serialize entire BTreeMap to bytes
- [ ] Implement `restore()` for KV state machine: deserialize bytes back to BTreeMap, replacing current state
- [ ] Add linearizable read support: reads are served only after confirming leadership via a heartbeat round

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: kv-put-get — Given an empty KV state machine, When Put("key1", "value1") is applied, Then Get("key1") returns "value1"
- [ ] Scenario: kv-snapshot-restore — Given a KV state machine with 100 entries, When snapshot is taken and restored into a fresh instance, Then all 100 entries are present with correct values
- [ ] Scenario: kv-cas-success — Given a KV with key="k" value="old", When CAS("k", expected="old", new="new") is applied, Then the value updates to "new"
- [ ] Scenario: kv-cas-failure — Given a KV with key="k" value="current", When CAS("k", expected="wrong", new="new") is applied, Then the value remains "current" and the operation returns a conflict error

## Stage 5.2: Snapshot Coordination

### Implementation Steps
- [ ] Implement snapshot trigger logic in `RaftNode`: initiate snapshot when `commit_index - last_snapshot_index > max_log_entries_before_compaction`
- [ ] Implement `take_snapshot()` in `RaftNode`: call `state_machine.snapshot()`, save via `SnapshotStore`, record snapshot metadata, truncate log entries before snapshot index
- [ ] Implement `install_snapshot()` handler for followers: receive snapshot chunks via `InstallSnapshot` RPC, assemble complete snapshot, call `state_machine.restore()`, update `last_applied` and `commit_index`
- [ ] Implement leader-side snapshot sending: when a follower's `next_index` is before the log start (entries were compacted), send snapshot via chunked `InstallSnapshot` RPCs instead of log entries
- [ ] Add snapshot progress tracking: log percentage complete for large snapshot transfers

### Dependencies
- phase-state-machine-interface/stage-application-state-machine-trait

### Test Scenarios
- [ ] Scenario: auto-snapshot-trigger — Given max_log_entries_before_compaction=100, When 150 entries are committed, Then a snapshot is automatically taken at index >= 100 and log entries before the snapshot are truncated
- [ ] Scenario: install-snapshot-on-slow-follower — Given a leader that has compacted entries 1-50, When a follower with next_index=10 fetches, Then the leader sends a snapshot and the follower restores from it
- [ ] Scenario: snapshot-chunks-reassembly — Given a 3 MB snapshot sent in 1 MB chunks, When all 3 InstallSnapshot RPCs complete, Then the follower's state machine matches the leader's

# Phase 6: Server Assembly and Client SDK

## Dependencies
- phase-network-transport
- phase-state-machine-interface

## Stage 6.1: Server Bootstrap and Lifecycle

### Implementation Steps
- [ ] Implement `main()` in `xraft-server` that: parses CLI args (config file path, node ID), loads config, initializes storage (LogStore, StateStore, SnapshotStore), initializes state machine, creates RaftNode, starts transport, runs driver loop
- [ ] Implement cluster bootstrap: if no persisted state exists and node is in the initial voter set, initialize with term=0 and empty log; first node to start election becomes initial leader
- [ ] Implement signal handling: `SIGTERM`/`SIGINT` trigger graceful shutdown, `SIGHUP` reloads configuration
- [ ] Implement structured logging with `tracing`: JSON output format, configurable log level via `RUST_LOG` env var, span context for request tracing
- [ ] Implement health check endpoint: simple HTTP endpoint at `/health` returning node role, term, commit_index, and leader_id using `axum`
- [ ] Add Prometheus metrics endpoint at `/metrics` exposing: `xraft_current_term`, `xraft_commit_index`, `xraft_role`, `xraft_election_count`, `xraft_append_latency_seconds`, `xraft_log_entries_total`

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: server-startup — Given a valid config file, When the server binary is started, Then it initializes all components and begins as a Follower within 1 second
- [ ] Scenario: graceful-shutdown — Given a running server, When SIGTERM is received, Then state is persisted, connections are drained, and the process exits with code 0
- [ ] Scenario: health-endpoint — Given a running server, When GET /health is called, Then it returns JSON with node_id, role, term, and leader_id fields

## Stage 6.2: Client SDK

### Implementation Steps
- [ ] Implement `XRaftClient` in `xraft-client/src/lib.rs` with methods: `connect(addrs: Vec<String>)`, `propose(data: Vec<u8>) -> Result<Vec<u8>>`, `read(key: &[u8]) -> Result<Vec<u8>>`
- [ ] Implement leader discovery: client tries each known address, on `NotLeader` error with leader hint, redirect to the indicated leader
- [ ] Implement automatic retry with exponential backoff for transient failures (connection refused, timeout)
- [ ] Implement request deduplication via client-assigned serial numbers on proposals (idempotency)
- [ ] Add timeout configuration for client operations with sensible defaults (connect: 5s, request: 30s)

### Dependencies
- phase-server-assembly-and-client-sdk/stage-server-bootstrap-and-lifecycle

### Test Scenarios
- [ ] Scenario: client-leader-redirect — Given a 3-node cluster, When client connects to a follower and proposes a command, Then the client automatically redirects to the leader and the command succeeds
- [ ] Scenario: client-retry-on-failure — Given a leader that temporarily fails, When the client's proposal times out, Then it retries and succeeds after a new leader is elected
- [ ] Scenario: client-idempotency — Given a client that sends the same serial number twice (network retry), When both reach the leader, Then the command is applied only once

# Phase 7: Advanced Raft Features

## Dependencies
- phase-server-assembly-and-client-sdk

## Stage 7.1: Check Quorum and Leader Lease

### Implementation Steps
- [ ] Implement Check Quorum mechanism in leader: track last successful communication time per follower, step down if a majority of followers have not responded within the election timeout
- [ ] Implement leader lease optimization: if the leader has heard from a majority within the last election timeout period, serve reads locally without an extra heartbeat round
- [ ] Add configuration option `enable_check_quorum` (default true) and `enable_leader_lease` (default false) in `ClusterConfig`
- [ ] Implement leader step-down on receiving a higher term from any RPC, ensuring no stale leader continues to serve

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: check-quorum-steps-down — Given a 3-node cluster where the leader is partitioned from both followers, When the election timeout elapses without majority contact, Then the leader steps down to Follower
- [ ] Scenario: check-quorum-healthy — Given a 3-node cluster with normal communication, When the leader runs Check Quorum, Then it remains leader because a majority is reachable
- [ ] Scenario: leader-lease-read — Given a leader with an active lease (majority heard from recently), When a read request arrives, Then it is served without an extra heartbeat round

## Stage 7.2: Dynamic Cluster Membership

### Implementation Steps
- [ ] Define `ConfigChange` enum with variants: `AddVoter(NodeId, addr)`, `RemoveVoter(NodeId)`, `AddObserver(NodeId, addr)`
- [ ] Implement `propose_config_change(change: ConfigChange)` in `RaftNode`: append config change entry to log, enforce single-change-at-a-time invariant (reject if a config change is already pending)
- [ ] Implement config change application: when a config change entry is committed, update the active voter set and peer connections
- [ ] Implement new node catch-up: added nodes start as non-voting observers, promoted to voter only after their log is within a configurable lag threshold of the leader
- [ ] Implement leader transfer when the leader itself is being removed: leader sends a `TimeoutNow` message to the most up-to-date follower, triggering immediate election
- [ ] Persist voter set changes as special log entries and in snapshots via `VotersRecord`

### Dependencies
- phase-advanced-raft-features/stage-check-quorum-and-leader-lease

### Test Scenarios
- [ ] Scenario: add-voter — Given a 3-node cluster, When AddVoter(node4) is proposed, Then after commit the cluster operates as a 4-node quorum
- [ ] Scenario: remove-voter — Given a 5-node cluster, When RemoveVoter(node5) is proposed and committed, Then node5 is excluded from future elections and quorum calculations
- [ ] Scenario: single-change-at-a-time — Given a pending AddVoter config change, When another config change is proposed, Then it is rejected with an error
- [ ] Scenario: leader-removal-transfer — Given a 3-node cluster where the leader is being removed, When the config change commits, Then leadership transfers to another node before the old leader steps down

## Stage 7.3: Log Compaction Pipeline

### Implementation Steps
- [ ] Implement background snapshot task using `tokio::task::spawn_blocking` to avoid blocking the event loop during snapshot serialization
- [ ] Implement log truncation prefix: after a snapshot is saved, remove all log entries with index <= snapshot's last_included_index
- [ ] Implement the `leader-epoch-checkpoint` file for fast diverging epoch detection during Fetch RPCs, mapping epochs to their start offsets
- [ ] Implement log segment garbage collection: delete segment files that are entirely before the log start offset
- [ ] Add metrics for snapshot duration, snapshot size, and log compaction events

### Dependencies
- phase-advanced-raft-features/stage-check-quorum-and-leader-lease

### Test Scenarios
- [ ] Scenario: background-snapshot-nonblocking — Given a leader processing client requests, When a background snapshot is taken, Then client request latency does not spike above 2x baseline
- [ ] Scenario: log-segment-gc — Given a log with 10 segment files after snapshot at index 5000, When segment GC runs, Then segments entirely before index 5000 are deleted
- [ ] Scenario: epoch-checkpoint-divergence — Given a follower with a diverging log at epoch 3, When it fetches from the leader, Then the leader uses the epoch checkpoint to identify the exact divergence point

# Phase 8: Integration Testing and Hardening

## Dependencies
- phase-advanced-raft-features

## Stage 8.1: Multi-Node Integration Tests

### Implementation Steps
- [ ] Create `tests/integration/` directory with test harness that spins up 3-node and 5-node in-process clusters using an in-memory transport (no real network)
- [ ] Implement `InMemoryTransport` in test harness that simulates message passing with configurable latency and packet loss
- [ ] Write integration test: cluster elects a leader within 2 election timeout periods after startup
- [ ] Write integration test: client can propose and read back 1000 key-value entries through the leader
- [ ] Write integration test: killing the leader triggers re-election and the new leader has all committed entries
- [ ] Write integration test: a partitioned follower rejoins and catches up via log replication or snapshot install

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: three-node-election — Given a 3-node cluster started simultaneously, When election timeouts elapse, Then exactly one leader is elected and all nodes agree on the term
- [ ] Scenario: data-consistency-after-failover — Given a 3-node cluster with 500 committed entries, When the leader is killed and a new leader elected, Then a client reading from the new leader sees all 500 entries
- [ ] Scenario: network-partition-recovery — Given a 5-node cluster split into groups of 3 and 2, When the partition heals, Then the minority group's nodes catch up and the cluster converges on one leader

## Stage 8.2: Chaos and Stress Testing

### Implementation Steps
- [ ] Implement chaos scenarios in test harness: random node kill/restart, random network partition, random message delay (50-500ms), random message drop (5-20%)
- [ ] Write stress test: sustained 1000 proposals/second for 60 seconds with random single-node failures, verify no data loss and all committed entries are consistent
- [ ] Write test: rapid leader churn (kill leader every 2 seconds for 30 seconds), verify cluster recovers each time and no committed entries are lost
- [ ] Write test: simultaneous election (3 candidates start elections at same term), verify exactly one wins or a new election resolves the tie
- [ ] Implement deterministic simulation mode: replace real timers and network with controllable fakes for reproducible test runs using seed-based random

### Dependencies
- phase-integration-testing-and-hardening/stage-multi-node-integration-tests

### Test Scenarios
- [ ] Scenario: chaos-no-data-loss — Given a 5-node cluster under chaos (random kills, partitions, delays) for 60 seconds, When chaos stops and the cluster stabilizes, Then all committed entries are present on a majority of nodes
- [ ] Scenario: rapid-leader-churn-recovery — Given leader killed every 2 seconds for 30 seconds, When the cluster stabilizes, Then a single leader is elected and all committed entries are intact
- [ ] Scenario: deterministic-replay — Given a chaos test run with seed=42, When replayed with the same seed, Then the exact same sequence of events and outcomes occurs

## Stage 8.3: Performance Benchmarks

### Implementation Steps
- [ ] Create `benches/` directory using `criterion` crate for micro-benchmarks
- [ ] Implement benchmark: log append throughput (entries/second) for FileLogStore with varying entry sizes (64B, 1KB, 64KB)
- [ ] Implement benchmark: end-to-end proposal latency (client propose to commit acknowledgment) for 3-node and 5-node clusters
- [ ] Implement benchmark: leader election time (time from leader death to new leader elected) over 100 trials
- [ ] Implement benchmark: snapshot creation and restore time for state machines of 1K, 10K, and 100K entries
- [ ] Document baseline performance numbers in `docs/benchmarks.md` with hardware specs and methodology

### Dependencies
- phase-integration-testing-and-hardening/stage-multi-node-integration-tests

### Test Scenarios
- [ ] Scenario: log-append-throughput — Given FileLogStore with 1KB entries, When benchmarked with criterion, Then throughput exceeds 10,000 entries/second on a single core
- [ ] Scenario: election-latency — Given a 3-node cluster, When the leader is killed 100 times, Then average election time is under 500ms
- [ ] Scenario: proposal-latency-p99 — Given a 3-node cluster under moderate load (100 proposals/s), When p99 latency is measured, Then it is under 50ms
