---
title: "xraft"
storyId: "failover-cluster:XRAFT"
---

# Phase 1: Project Scaffolding

## Dependencies
- _none — start phase_

## Stage 1.1: Cargo Workspace and Crate Layout

### Implementation Steps
- [ ] Initialize Cargo workspace at repo root with `Cargo.toml` defining members: `xraft-core`, `xraft-storage`, `xraft-transport`, `xraft-server`, `xraft-client`, `xraft-test`
- [ ] Create `xraft-core` crate with `lib.rs` exporting top-level modules: `types`, `config`, `error`, `message`; add `#![forbid(unsafe_code)]` at the crate root per `tech-spec.md` §5.1 (consensus crate must forbid unsafe; `unsafe` allowed only in storage layer with documented safety invariants)
- [ ] Create `xraft-storage` crate with `lib.rs` stub and dependency on `xraft-core`
- [ ] Create `xraft-transport` crate with `lib.rs` stub and dependency on `xraft-core`
- [ ] Create `xraft-server` crate (binary) with `main.rs` stub and dependencies on all library crates
- [ ] Create `xraft-client` crate (library) with `lib.rs` stub and dependency on `xraft-core` and `xraft-transport`
- [ ] Create `xraft-test` crate (library, dev-dependency only) with `lib.rs` stub for deterministic simulation harness and integration test utilities, depending on `xraft-core` and `xraft-storage`
- [ ] Add shared workspace dependencies in root `Cargo.toml`: `tokio`, `serde`, `serde_json`, `tracing`, `tracing-subscriber`, `thiserror`, `bytes`, `prost`, `tonic`, `uuid`, `toml`, `rand`, `crc32fast`, `axum`, `prometheus-client`
- [ ] Add `rust-toolchain.toml` pinning stable channel (e.g. `channel = "stable"`); set `edition = "2024"` in each crate's `Cargo.toml` manifest
- [ ] Add `.gitignore` for Rust (`/target`, `Cargo.lock` for libraries)
- [ ] Verify `cargo check --workspace` succeeds with no errors

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: workspace-compiles — Given the newly created workspace, When `cargo check --workspace` is run, Then it exits with code 0 and no errors
- [ ] Scenario: crate-dependency-graph — Given the workspace, When `cargo metadata` is inspected, Then `xraft-server` depends on all library crates, `xraft-storage` depends on `xraft-core`, and `xraft-test` depends on `xraft-core` and `xraft-storage`

## Stage 1.2: Core Types and Configuration

### Implementation Steps
- [ ] Define `NodeId` (u64), `Term` (u64), `LogIndex` (u64), `DirectoryId` (Uuid) types in `xraft-core/src/types.rs` with `Serialize`, `Deserialize`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Debug` derives
- [ ] Define `NodeRole` enum (`Leader`, `Follower`, `PreCandidate`, `Candidate`, `Observer`) in `xraft-core/src/types.rs` — `PreCandidate` is the state during the Pre-Vote phase before term is incremented (per `architecture.md` §2.1)
- [ ] Define `ClusterConfig` struct in `xraft-core/src/config.rs` with fields: `node_id`, `cluster_id`, `listen_addr`, `peers` (Vec of peer addresses), `election_timeout_min_ms`, `election_timeout_max_ms`, `fetch_interval_ms`, `tick_interval_ms`, `snapshot_interval`, `max_log_entries_before_compaction`, `data_dir`, `tls` (optional `TlsConfig` with `cert_path` and `key_path` fields per `tech-spec.md` §2.7 / `architecture.md` §2.3 — not mandatory for v1 functional correctness but the configuration surface must exist)
- [ ] Implement config loading from TOML file and environment variable overrides using `serde` and `toml` crate
- [ ] Define `XRaftError` enum in `xraft-core/src/error.rs` using `thiserror` with variants: `Storage`, `Transport`, `NotLeader`, `ElectionTimeout`, `InvalidTerm`, `LogInconsistency`, `Shutdown`, `Config`
- [ ] Define `Result<T>` type alias as `std::result::Result<T, XRaftError>`
- [ ] Add unit tests for config deserialization and default values

### Dependencies
- phase-project-scaffolding/stage-cargo-workspace-and-crate-layout

### Test Scenarios
- [ ] Scenario: config-from-toml — Given a valid TOML config string, When deserialized into `ClusterConfig`, Then all fields match expected values including peer addresses
- [ ] Scenario: config-defaults — Given a minimal TOML with only required fields, When deserialized, Then optional fields use sensible defaults (election_timeout_min_ms=150, fetch_interval_ms=50)
- [ ] Scenario: error-display — Given each `XRaftError` variant, When formatted with Display, Then a human-readable message is produced

## Stage 1.3: RPC Message Definitions

### Implementation Steps
- [ ] Create `proto/raft.proto` defining protobuf messages for wire RPCs only: `VoteRequest`, `VoteResponse`, `PreVoteRequest`, `PreVoteResponse`, `FetchRequest`, `FetchResponse` — these are the complete set of wire RPC messages; `Action::AppendEntries` is a pure Rust enum variant in `xraft-core` representing the internal side-effect of the leader appending to its own log and has no protobuf or gRPC representation (per `tech-spec.md` §2.2 and `architecture.md` §2.1)
- [ ] Define `LogEntry` protobuf message with fields: `index`, `term`, `entry_type` (enum: Command, NoOp, Config), `data` (bytes)
- [ ] Define `SnapshotMetadata` protobuf message with fields: `last_included_index`, `last_included_term`, `voter_set`
- [ ] Define `FetchSnapshotRequest` and `FetchSnapshotChunk` protobuf messages for streamed snapshot transfer from leader to follower
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
- [ ] Define `LogStore` trait in `xraft-core/src/storage.rs` with methods: `append`, `get(index)`, `get_range(start, end)`, `last_index`, `last_term`, `truncate_from(index)`, `term_at(index)` — all trait definitions live in `xraft-core` to avoid circular dependencies (per `architecture.md` §4.1); `xraft-storage` imports and implements them
- [ ] Implement `FileLogStore` in `xraft-storage/src/log.rs` backed by append-only segment files in configurable data directory, each segment containing a header (magic bytes, version) and sequential log entries
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
- [ ] Define `HardStateStore` trait in `xraft-core/src/storage.rs` with methods: `persist(state: &HardState) -> Result<()>`, `load() -> Result<Option<HardState>>` — trait lives in `xraft-core` per `architecture.md` §4.1
- [ ] Define `HardState` struct in `xraft-core/src/types.rs` with fields: `current_term: Term`, `voted_for: Option<NodeId>`, `commit_index: LogIndex` — all three fields are persisted atomically to the `quorum-state` file (per `architecture.md` §10 cross-ref and §3.1 which list `commit_index` as part of the durable quorum state); `last_applied` is volatile, rebuilt from the log on recovery by replaying committed entries from `commit_index`
- [ ] Implement `FileHardStateStore` in `xraft-storage/src/state.rs` that persists `HardState` as JSON to a `quorum-state` file with atomic write (write to temp file then rename) for crash safety, consistent with KRaft's `quorum-state` pattern (per `tech-spec.md` §5.3)
- [ ] Implement `load()` that reads state on startup with fallback to default initial state (term=0, voted_for=None)
- [ ] Add validation in `persist()` that term never decreases and voted_for is only set once per term

### Dependencies
- phase-persistent-storage/stage-write-ahead-log

### Test Scenarios
- [ ] Scenario: state-persistence — Given a saved HardState with term=5 and voted_for=Some(3), When the FileHardStateStore is reloaded from the `quorum-state` file, Then the loaded state matches exactly
- [ ] Scenario: atomic-write-safety — Given a state persist in progress, When the process crashes mid-write (simulated by checking temp file), Then the previous valid `quorum-state` is still loadable
- [ ] Scenario: term-monotonicity — Given a HardState with term=5, When persist() is called with term=3, Then an error is returned

## Stage 2.3: Snapshot Store

### Implementation Steps
- [ ] Define `SnapshotStore` trait in `xraft-core/src/storage.rs` with methods: `save_snapshot(metadata, data)`, `load_latest_snapshot()`, `list_snapshots()`, `delete_snapshot(id)` — trait lives in `xraft-core` per `architecture.md` §4.1
- [ ] Implement `FileSnapshotStore` in `xraft-storage/src/snapshot.rs` that writes snapshots to `snapshots/` directory with filename pattern `snapshot-{term}-{index}.bin`
- [ ] Implement snapshot metadata header format: `[magic: u32][version: u16][last_included_index: u64][last_included_term: u64][voter_set_len: u32][voter_set: bytes][data_len: u64]`
- [ ] Implement snapshot cleanup: retain only the N most recent snapshots (configurable, default 3)
- [ ] Implement chunked snapshot reading for `FetchSnapshot` RPC support, returning iterators over fixed-size chunks (default 1 MB)

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
- [ ] Create `xraft-core/src/node.rs` defining `RaftNode` struct holding: `id: NodeId`, `role: NodeRole`, `current_term: Term`, `voted_for: Option<NodeId>`, `log: Vec<Entry>`, `commit_index: LogIndex`, `last_applied: LogIndex`, `config: ClusterConfig`, `election_timer: ElectionTimer`, `peers: HashMap<NodeId, PeerState>` — the node is I/O-free and accepts `Input` enums, returning `Vec<Action>` side-effects
- [ ] Define `PeerState` struct tracking per-peer replication state: `last_fetch_offset`, `last_fetch_time`, `last_caught_up_time`, `is_voter: bool`
- [ ] Implement `ElectionTimer` with randomized timeout in range `[election_timeout_min, election_timeout_max]` using `rand` crate, with `reset()`, `is_expired()`, `remaining()` methods
- [ ] Implement role transition methods: `become_follower(term, leader_id)`, `become_pre_candidate()`, `become_candidate()`, `become_leader()` with appropriate state updates (reset timers, initialize peer state, emit `Action::AppendEntries` for no-op entry) — the `PreCandidate` role is used during the Pre-Vote phase before term is incremented
- [ ] Implement `step(input: Input) -> Vec<Action>` method that processes a single input (Tick, VoteRequest, FetchRequest, ClientPropose, etc.) and returns side-effect actions; for `Input::Tick`, check election timeout for followers/candidates and trigger candidacy if expired — leaders do not push heartbeats (followers pull via Fetch in the KRaft model)
- [ ] Add `tracing` instrumentation to all state transitions and critical paths

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: initial-state — Given a new RaftNode, When created, Then role is Follower, term is 0, and election timer is running
- [ ] Scenario: election-timeout-triggers-pre-candidacy — Given a Follower node, When election timer expires via repeated Tick inputs, Then the node transitions to PreCandidate and sends PreVoteRequest RPCs without incrementing its term (per Pre-Vote protocol design)
- [ ] Scenario: become-leader-initializes-peers — Given a node becoming leader, When `become_leader()` is called, Then `last_fetch_offset` for each peer is initialized and a no-op `Action::AppendEntries` is emitted

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
- [ ] Implement `handle_fetch_request(req: FetchRequest) -> Vec<Action>` in leader: validate term, return entries from requested `fetch_offset`, include current high watermark (HW), detect diverging epochs and return `DivergingEpoch` info in response — *consistency note:* `architecture.md` §6 S3 describes the Log Matching invariant using classical Raft terminology (`prev_log_index`/`prev_log_term`); this implementation enforces the same safety property via KRaft-style `DivergingEpoch` (the follower sends `last_fetched_epoch` and the leader returns a `DivergingEpoch` on mismatch, which triggers truncation — functionally equivalent to `prev_log_index`/`prev_log_term` mismatch detection, per `tech-spec.md` §2.2)
- [ ] Implement follower-side Fetch processing: on receiving `FetchResponse`, append new entries to local log, update commit index to the leader's high watermark if entries are replicated, handle log truncation on diverging epoch
- [ ] Implement follower-initiated fetch scheduling: followers send `FetchRequest` to the leader at a configurable `fetch_interval_ms` (default 50ms); any valid `FetchResponse` (empty or with entries) resets the election timer as proof of leader liveness
- [ ] Implement commit advancement in leader: after each `FetchRequest` arrives, update `peer.last_fetch_offset`; find the highest index N where a majority of peers' `last_fetch_offset >= N` and `log[N].term == current_term`, then advance high watermark to N
- [ ] Implement `apply_committed()`: emit `Action::ApplyToStateMachine` for all log entries between `last_applied` and `commit_index`, advancing `last_applied`
- [ ] Implement follower log conflict resolution: when a `FetchResponse` contains a `DivergingEpoch`, follower truncates local log to the divergence point and re-fetches from there

### Dependencies
- phase-raft-consensus-engine/stage-leader-election

### Test Scenarios
- [ ] Scenario: basic-replication — Given a 3-node cluster with node 1 as leader, When followers send Fetch RPCs, Then the leader responds with new entries and after two fetch rounds all followers have the entry and high watermark advances
- [ ] Scenario: commit-requires-majority — Given a 5-node cluster leader with 2 slow followers, When 2 of 4 followers fetch and acknowledge entries, Then high watermark advances (majority of 5 is 3 including leader)
- [ ] Scenario: follower-conflict-resolution — Given a follower with entries [1:t1, 2:t1, 3:t2] and leader has [1:t1, 2:t1, 3:t3], When follower fetches and receives DivergingEpoch, Then follower truncates entry 3 and re-fetches leader's version
- [ ] Scenario: fetch-resets-election-timer — Given a follower, When it receives any valid FetchResponse (empty or with entries) from the leader, Then its election timer resets and it remains a follower
- [ ] Scenario: stale-leader-steps-down — Given a leader at term=3, When it receives a FetchRequest or VoteRequest with term=5, Then it steps down to Follower at term=5

# Phase 4: Network Transport

## Dependencies
- phase-raft-consensus-engine

## Stage 4.1: gRPC Transport Layer

### Implementation Steps
- [ ] Define `RaftService` gRPC service in `proto/raft.proto` with RPCs: `Vote`, `PreVote`, `Fetch`, `FetchSnapshot` (streamed response via `stream FetchSnapshotChunk`)
- [ ] Implement `RaftGrpcServer` in `xraft-transport/src/grpc_server.rs` using `tonic` that accepts incoming RPCs and dispatches to `RaftNode` message handlers
- [ ] Implement `RaftGrpcClient` in `xraft-transport/src/grpc_client.rs` using `tonic` for sending RPCs to peers with configurable connection timeout and retry logic
- [ ] Implement connection pooling in `RaftGrpcClient`: maintain one persistent channel per peer, reconnect on failure with exponential backoff
- [ ] Define `Transport` trait in `xraft-core/src/transport.rs` abstracting over network implementation with methods: `send_vote`, `send_pre_vote`, `send_fetch`, `send_fetch_snapshot`, `start_server` — trait lives in `xraft-core` per `architecture.md` §4.1; `xraft-transport` imports and implements it
- [ ] Implement the `Transport` trait for the gRPC implementation in `xraft-transport/src/grpc.rs`
- [ ] Implement optional TLS support in `RaftGrpcServer` and `RaftGrpcClient`: if `ClusterConfig.tls` is `Some`, configure `tonic` with `ServerTlsConfig` / `ClientTlsConfig` using the provided cert and key paths; if `None`, use plaintext — per `tech-spec.md` §2.7 the configuration surface must exist even though TLS is not mandatory for v1 functional correctness

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: grpc-vote-roundtrip — Given a running RaftGrpcServer, When a VoteRequest is sent via RaftGrpcClient, Then a VoteResponse is received with correct fields
- [ ] Scenario: connection-retry — Given a peer that is temporarily unreachable, When send_vote is called, Then the client retries with backoff and eventually succeeds when the peer comes back
- [ ] Scenario: concurrent-rpcs — Given a running server, When 50 concurrent Fetch RPCs arrive, Then all are processed without deadlock or data corruption
- [ ] Scenario: tls-transport — Given a cluster configured with TLS cert and key paths, When a VoteRequest is sent over the TLS-enabled gRPC channel, Then the connection succeeds and the VoteResponse is received correctly

## Stage 4.2: Message Router and Driver Loop

### Implementation Steps
- [ ] Create `xraft-server/src/driver.rs` implementing the main async event loop using `tokio::select!` over: incoming RPC messages, outgoing RPC results, tick timer, client command channel, shutdown signal
- [ ] Implement `MessageRouter` that receives outbound messages from `RaftNode` and dispatches them via the `Transport` trait to appropriate peers
- [ ] Implement inbound message dispatching: deserialize incoming RPCs, route to `RaftNode` handler methods, send responses back through gRPC
- [ ] Implement tick scheduling using `tokio::time::interval` at 10ms granularity for election and fetch timers
- [ ] Implement graceful shutdown: drain in-flight RPCs, persist final state, close transport connections
- [ ] Implement client command channel (`tokio::sync::mpsc`) for submitting new log entries and waiting for commit confirmation

### Dependencies
- phase-network-transport/stage-grpc-transport-layer

### Test Scenarios
- [ ] Scenario: driver-processes-tick — Given a running driver loop, When the tick interval fires, Then `RaftNode.step(Input::Tick)` is called and any resulting `Action`s are dispatched
- [ ] Scenario: driver-handles-shutdown — Given a running driver loop with in-flight operations, When shutdown signal is received, Then state is persisted and all connections close cleanly within 5 seconds
- [ ] Scenario: client-command-flow — Given a leader node's driver loop, When a client command is submitted via the command channel, Then it is appended to the log and the future resolves after commit

# Phase 5: State Machine Interface

## Dependencies
- phase-raft-consensus-engine

## Stage 5.1: State Machine Callback Trait

### Implementation Steps
- [ ] Define `StateMachineCallback` trait in `xraft-core/src/state_machine.rs` with methods: `apply(index: LogIndex, entry: &[u8]) -> Result<()>`, `snapshot() -> Result<Vec<u8>>`, `restore(data: &[u8]) -> Result<()>` — this is the extension point for consumers; XRAFT provides the replicated log, not application logic
- [ ] Implement `NoOpStateMachine` as a minimal default in `xraft-core/src/state_machine.rs` that logs applied entries via `tracing` but discards the data — used for testing and as a baseline
- [ ] Wire `StateMachineCallback` into the `Action::ApplyToStateMachine` dispatch path in the event loop, so committed entries are forwarded to the callback
- [ ] Implement `snapshot()` and `restore()` integration: the event loop calls `snapshot()` when a snapshot is triggered and `restore()` when a snapshot is installed from a leader

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: noop-apply — Given a `NoOpStateMachine`, When `apply()` is called with 10 entries, Then no error is returned and the callback completes without side effects
- [ ] Scenario: snapshot-restore-roundtrip — Given a `StateMachineCallback` implementation, When `snapshot()` is called and the result passed to `restore()` on a fresh instance, Then the restored state is equivalent to the original

## Stage 5.2: Snapshot Coordination

### Implementation Steps
- [ ] Implement snapshot trigger logic in `RaftNode`: initiate snapshot when `commit_index - last_snapshot_index > max_log_entries_before_compaction`
- [ ] Implement `take_snapshot()` in `RaftNode` as I/O-free: when the trigger fires, return `Action::TakeSnapshot { through_index }` so the external driver calls `state_machine.snapshot()` and `SnapshotStore::save_snapshot()` outside the core; on completion the driver feeds `Input::SnapshotComplete { metadata }` back into the node, which records metadata and returns `Action::TruncateLog { before_index }`
- [ ] Implement `install_snapshot()` as I/O-free: receiving a `FetchSnapshot` response produces `Action::InstallSnapshot { metadata, data }` so the driver calls `state_machine.restore()` and `SnapshotStore::save_snapshot()` externally; on completion the driver feeds `Input::SnapshotInstalled { metadata }` and the node updates `last_applied` and `commit_index`
- [ ] Implement leader-side snapshot sending: when a follower's `last_fetch_offset` is before the log start (entries were compacted), respond to the follower's Fetch with a redirect to `FetchSnapshot`, then stream snapshot chunks
- [ ] Add snapshot progress tracking: log percentage complete for large snapshot transfers

### Dependencies
- phase-state-machine-interface/stage-state-machine-callback-trait

### Test Scenarios
- [ ] Scenario: auto-snapshot-trigger — Given max_log_entries_before_compaction=100, When 150 entries are committed, Then a snapshot is automatically taken at index >= 100 and log entries before the snapshot are truncated
- [ ] Scenario: install-snapshot-on-slow-follower — Given a leader that has compacted entries 1-50, When a follower with last_fetch_offset=10 sends a Fetch, Then the leader redirects to FetchSnapshot and the follower restores from the snapshot
- [ ] Scenario: snapshot-chunks-reassembly — Given a 3 MB snapshot sent in 1 MB chunks, When all 3 FetchSnapshot stream chunks complete, Then the follower's state machine matches the leader's

# Phase 6: Server Assembly and Internal Client

## Dependencies
- phase-network-transport
- phase-state-machine-interface

## Stage 6.1: Server Bootstrap and Lifecycle

### Implementation Steps
- [ ] Implement `main()` in `xraft-server` that: parses CLI args (config file path, node ID), loads config, initializes storage (LogStore, HardStateStore, SnapshotStore), initializes state machine, creates RaftNode, starts transport, runs driver loop
- [ ] Implement cluster bootstrap: if no persisted state exists and node is in the initial voter set, initialize with term=0 and empty log; first node to start election becomes initial leader
- [ ] Implement signal handling: `SIGTERM`/`SIGINT` trigger graceful shutdown, `SIGHUP` reloads configuration
- [ ] Implement structured logging with `tracing`: JSON output format, configurable log level via `RUST_LOG` env var, span context for request tracing
- [ ] Implement health check endpoint: simple HTTP endpoint at `/health` returning node role, term, commit_index, and leader_id using `axum`
- [ ] Add Prometheus metrics endpoint at `/metrics` using `axum` and `prometheus-client`, exposing an MVP metrics subset using the canonical names from `architecture.md` §7 (per `e2e-scenarios.md` Feature 15 phased delivery): `xraft_current_term` (gauge), `xraft_commit_index` (gauge), `xraft_role` (gauge, encoded as 0=Follower/1=PreCandidate/2=Candidate/3=Leader/4=Observer), `xraft_current_leader` (gauge — node ID of current leader, -1 if unknown), `xraft_election_latency_seconds` (histogram — time from candidacy to leader election), `xraft_append_records_total` (counter — total entries appended) — the remaining canonical metrics from `architecture.md` §7 are added in Stage 7.1 (`xraft_replication_lag`, `xraft_commit_latency_seconds`, `xraft_fetch_requests_total`) and Stage 7.3 (`xraft_snapshot_installs_total`, `xraft_log_end_offset`)

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: server-startup — Given a valid config file, When the server binary is started, Then it initializes all components and begins as a Follower within 1 second
- [ ] Scenario: graceful-shutdown — Given a running server, When SIGTERM is received, Then state is persisted, connections are drained, and the process exits with code 0
- [ ] Scenario: health-endpoint — Given a running server, When GET /health is called, Then it returns JSON with node_id, role, term, and leader_id fields

## Stage 6.2: Internal Peer RPC and Admin Client

### Implementation Steps
- [ ] Implement `PeerClient` in `xraft-client/src/peer.rs` wrapping a `tonic` gRPC channel to a specific peer, providing typed methods for `Vote`, `PreVote`, `Fetch`, and `FetchSnapshot` RPCs with connection lifecycle management
- [ ] Implement `ConnectionPool` in `xraft-client/src/pool.rs` maintaining lazy-initialized `PeerClient` instances keyed by `NodeId`, with automatic reconnection on channel failure
- [ ] Implement leader discovery: `PeerClient` tracks last-known leader via hints returned in `FetchResponse` and `VoteResponse` messages; followers cache the leader hint for internal routing decisions
- [ ] Implement `AdminClient` in `xraft-client/src/admin.rs` connecting to a node's HTTP admin endpoint for operational queries (cluster status, trigger snapshot, node health) — `xraft-client` is an **internal** crate providing peer-to-peer RPC and admin/operational queries only; no external consumer SDK (`propose`/`read`) is in scope for v1 (per `tech-spec.md` §2.6, `architecture.md` §2.5, and `e2e-scenarios.md` Alignment Note 4 — all sibling docs agree on this scope; *note:* `e2e-scenarios.md` Feature 11's preamble still references a "dual-role" client per an earlier draft but its actual scenarios test only internal capabilities)
- [ ] Add timeout and retry configuration with sensible defaults (connect: 5s, request: 30s, backoff with jitter)

### Dependencies
- phase-server-assembly-and-internal-client/stage-server-bootstrap-and-lifecycle

### Test Scenarios
- [ ] Scenario: peer-client-reconnect — Given a PeerClient connected to a peer that restarts, When the next RPC is sent, Then the client reconnects automatically and the RPC succeeds
- [ ] Scenario: connection-pool-lazy-init — Given a ConnectionPool for a 5-node cluster, When a PeerClient for node 3 is requested twice, Then the same channel is reused without creating a new connection
- [ ] Scenario: admin-client-status — Given a running node with admin HTTP endpoint, When AdminClient queries cluster status, Then it returns the current leader, term, and voter set
- [ ] Scenario: leader-hint-tracking — Given a follower that receives a FetchResponse with leader_id=2, When subsequent RPCs need leader routing, Then the cached leader hint is used without additional discovery

# Phase 7: Advanced Raft Features

## Dependencies
- phase-server-assembly-and-internal-client

## Stage 7.1: Check Quorum and Leader Lease

### Implementation Steps
- [ ] Implement Check Quorum mechanism in leader: track last successful communication time per voter peer; the leader counts itself as one voter and steps down if a majority of the full voter set (including self) has not responded within the election timeout — e.g. in a 5-node cluster, the leader needs at least 2 of the 4 other voters to have responded recently (since leader + 2 = 3 = majority of 5)
- [ ] Implement leader lease optimization: if the leader has heard from a majority within the last election timeout period (via incoming Fetch RPCs), it may skip the extra commit-index confirmation round-trip when answering internal read queries (e.g. admin status queries or `StateMachineCallback`-based lookups); this is a leader-internal optimization and does not expose an external client read API
- [ ] Add configuration option `enable_check_quorum` (default true) and `enable_leader_lease` (default false) in `ClusterConfig`
- [ ] Implement leader step-down on receiving a higher term from any RPC, ensuring no stale leader continues to serve
- [ ] Register remaining canonical metrics from `architecture.md` §7 for leader and replication observability: `xraft_replication_lag` (gauge per replica — entries behind leader for each follower, computed from `leader_log_end - follower_last_fetch_offset`), `xraft_commit_latency_seconds` (histogram — time from proposal to commit, leader only), `xraft_fetch_requests_total` (counter — total Fetch RPCs received by leader / sent by follower) — these extend the MVP subset registered in Stage 6.1 toward the complete `architecture.md` §7 / `e2e-scenarios.md` Feature 15 canonical set

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: check-quorum-steps-down — Given a 3-node cluster where the leader is partitioned from both followers, When the election timeout elapses without majority contact, Then the leader steps down to Follower
- [ ] Scenario: check-quorum-healthy — Given a 3-node cluster with normal communication, When the leader runs Check Quorum, Then it remains leader because a majority is reachable
- [ ] Scenario: leader-lease-read — Given a leader with an active lease (majority heard from via recent Fetch RPCs), When an internal read query arrives (e.g. admin status), Then it is answered without an extra commit-index confirmation round-trip

## Stage 7.2: Static Voter Set Bootstrap and Observer Support

### Implementation Steps
- [ ] Implement cluster bootstrap from a fixed voter set defined in configuration: on first start with no persisted state, initialize `VoterSet` from config and persist it alongside `HardState` in the `quorum-state` file (as a separate top-level field — `HardState` itself contains `current_term`, `voted_for`, and `commit_index` per Stage 2.2)
- [ ] Implement `VoterSet` validation at startup: require at least 1 voter; warn (but allow) even-numbered voter sets since they have lower fault tolerance per node; verify the local node is either in the voter set or registered as an observer
- [ ] Implement `Observer` role: non-voting nodes that replicate the log via Fetch RPCs for read scaling or standby purposes, without participating in elections or quorum calculations
- [ ] Implement observer registration: observers connect to the leader and send Fetch RPCs like voters, but the leader excludes them from high-watermark quorum computation
- [ ] Persist the `VoterSet` in snapshots so that nodes restoring from a snapshot know the cluster membership without re-reading configuration
- [ ] Implement `AddVoter`/`RemoveVoter` command rejection: any attempt to issue an `AddVoter` or `RemoveVoter` command must return an `UNSUPPORTED` error with a message indicating dynamic membership is **out of scope for v1** and deferred to a future story entirely — all three authoritative sibling docs agree: `tech-spec.md` §2.7 says "out of scope for v1 and deferred to a future story entirely — it is not a stretch goal within XRAFT", `architecture.md` §2.1/§10 uses the same language, and this plan enforces it via rejection; the voter set remains static for v1 (*cross-doc note:* `e2e-scenarios.md` Feature 12's preamble still labels dynamic membership a "stretch goal within this story" rather than fully deferred — that preamble should be updated to match `tech-spec.md` and `architecture.md`)

### Dependencies
- phase-advanced-raft-features/stage-check-quorum-and-leader-lease

### Test Scenarios
- [ ] Scenario: bootstrap-voter-set — Given a 3-node cluster configuration, When all nodes start for the first time, Then each node initializes with the same VoterSet and an election produces a leader
- [ ] Scenario: observer-replicates-without-voting — Given a 3-node cluster with 1 observer, When the observer sends Fetch RPCs, Then it receives log entries but does not count toward quorum and cannot become a candidate
- [ ] Scenario: single-node-cluster — Given a configuration with 1 voter, When the server starts, Then it elects itself leader immediately and can commit entries without waiting for peers
- [ ] Scenario: even-voter-warning — Given a configuration with 2 voters, When the server starts, Then it logs a warning about reduced fault tolerance but proceeds to form a cluster
- [ ] Scenario: add-remove-voter-rejected — Given a running 3-node cluster, When an operator issues an `AddVoter` or `RemoveVoter` command, Then the node rejects it with an `UNSUPPORTED` error and the voter set remains unchanged

## Stage 7.3: Log Compaction Pipeline

### Implementation Steps
- [ ] Implement background snapshot task using `tokio::task::spawn_blocking` to avoid blocking the event loop during snapshot serialization
- [ ] Implement log truncation prefix: after a snapshot is saved, remove all log entries with index <= snapshot's last_included_index
- [ ] Implement the `leader-epoch-checkpoint` file for fast diverging epoch detection during Fetch RPCs, mapping epochs to their start offsets
- [ ] Implement log segment garbage collection: delete segment files that are entirely before the log start offset
- [ ] Add metrics for snapshot duration, snapshot size, and log compaction events
- [ ] Register remaining canonical metrics from `architecture.md` §7 for snapshot and log observability: `xraft_snapshot_installs_total` (counter — snapshots installed by this node), `xraft_log_end_offset` (gauge — highest log index, may be ahead of commit) — these complete the full `architecture.md` §7 / `e2e-scenarios.md` Feature 15 canonical metric set alongside the metrics registered in Stage 6.1 (MVP subset) and Stage 7.1 (leader/replication metrics)

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
- [ ] Create integration test infrastructure in the `xraft-test` crate with a `SimulatedCluster` harness that spins up 3-node and 5-node in-process clusters using `SimulatedNetwork` (no real network) for deterministic, fast-running tests
- [ ] Implement `SimulatedNetwork` in `xraft-test` that simulates message passing with configurable latency, packet loss, and network partitions, plus `SimulatedClock` for deterministic tick advancement
- [ ] Create real-network integration test infrastructure in `xraft-test` with a `RealCluster` harness that starts 3-node and 5-node clusters using actual gRPC transport (per `tech-spec.md` §2.5 which requires real-network 3-node and 5-node integration tests); each node runs as a separate Tokio task with real `RaftGrpcServer`/`RaftGrpcClient` binding to localhost ports
- [ ] Write integration test (simulated): cluster elects a leader within 2 election timeout periods after startup
- [ ] Write integration test (simulated): test harness submits 1000 opaque log entries as proposals through the leader's internal command channel; a test `StateMachineCallback` records applied entries; after commit, the callback's state contains all 1000 entries in order
- [ ] Write integration test (simulated): killing the leader triggers re-election and the new leader has all committed entries
- [ ] Write integration test (simulated): a partitioned follower rejoins and catches up via log replication or snapshot install
- [ ] Write integration test (real-network): 3-node cluster over real gRPC transport elects a leader and replicates 100 opaque log entries; a test `StateMachineCallback` on each node records applied entries; all nodes' callbacks converge on the same ordered log
- [ ] Write integration test (real-network): 5-node cluster over real gRPC transport survives leader crash (process kill) and re-elects; new leader's `StateMachineCallback` contains all previously committed entries

### Dependencies
- _none — start stage_

### Test Scenarios
- [ ] Scenario: three-node-election — Given a 3-node cluster started simultaneously, When election timeouts elapse, Then exactly one leader is elected and all nodes agree on the term
- [ ] Scenario: data-consistency-after-failover — Given a 3-node cluster with 500 committed opaque log entries applied to a test `StateMachineCallback`, When the leader is killed and a new leader elected, Then the new leader's `StateMachineCallback` contains all 500 entries in order
- [ ] Scenario: network-partition-recovery — Given a 5-node cluster split into groups of 3 and 2, When the partition heals, Then the minority group's nodes catch up and the cluster converges on one leader
- [ ] Scenario: real-network-3-node-replication — Given a 3-node cluster using real gRPC transport on localhost, When 100 opaque log entries are proposed and committed, Then all 3 nodes' test `StateMachineCallback` instances contain the same 100 entries in order
- [ ] Scenario: real-network-5-node-leader-failover — Given a 5-node cluster using real gRPC transport with 50 committed entries, When the leader process is killed, Then a new leader is elected and all previously committed entries are present in the new leader's log

## Stage 8.2: Chaos and Stress Testing

### Implementation Steps
- [ ] Implement chaos scenarios in test harness: random node kill/restart, random network partition, random message delay (50-500ms), random message drop (5-20%)
- [ ] Write stress test: sustained 1000 proposals/second for 60 seconds with random single-node failures, verify no data loss and all committed entries are consistent
- [ ] Write test: rapid leader churn (kill leader every 2 seconds for 30 seconds), verify cluster recovers each time and no committed entries are lost
- [ ] Write test: simultaneous election (3 candidates start elections at same term), verify exactly one wins or a new election resolves the tie
- [ ] Implement deterministic simulation mode in `xraft-test`: use `SimulatedClock` and `SimulatedNetwork` to replace real timers and network with controllable fakes for reproducible test runs using seed-based random
- [ ] Implement linearisability validation using `stateright` or equivalent model checker per `tech-spec.md` §2.5: record a history of operations (proposals and their commit confirmations) during simulation runs and verify the history is linearisable; integrate as a post-hoc check in chaos test scenarios

### Dependencies
- phase-integration-testing-and-hardening/stage-multi-node-integration-tests

### Test Scenarios
- [ ] Scenario: chaos-no-data-loss — Given a 5-node cluster under chaos (random kills, partitions, delays) for 60 seconds, When chaos stops and the cluster stabilizes, Then all committed entries are present on a majority of nodes
- [ ] Scenario: rapid-leader-churn-recovery — Given leader killed every 2 seconds for 30 seconds, When the cluster stabilizes, Then a single leader is elected and all committed entries are intact
- [ ] Scenario: deterministic-replay — Given a chaos test run with seed=42, When replayed with the same seed, Then the exact same sequence of events and outcomes occurs
- [ ] Scenario: linearisability-check — Given a 5-node cluster under chaos for 30 seconds with concurrent proposals, When the operation history is validated by the linearisability checker, Then all committed operations form a linearisable history

