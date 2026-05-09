# End-to-End Scenarios — XRAFT (Raft Consensus in Rust)

> **Story:** `failover-cluster:XRAFT` — Implement Raft consensus protocol in Rust,
> modelled after Apache Kafka's KRaft protocol.
>
> **Companion docs:** `tech-spec.md` · `architecture.md` · `implementation-plan.md`

---

## Notation

All scenarios follow Gherkin syntax.
Cluster sizes use the variable **N** (default 3 unless stated otherwise).
**Quorum** = ⌊N/2⌋ + 1. **F** (max tolerable failures) = (N − 1) / 2.
Terms are called **epochs** when referencing KRaft-style naming; they are
interchangeable with the Raft concept of *term*.

---

## Feature 1: Leader Election — Happy Path

```gherkin
Feature: Leader election under normal conditions
  A cluster of N nodes must elect exactly one leader per epoch and
  all followers must converge on that leader within the election timeout.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And all nodes start in the "Follower" state
    And each node has a randomised election timeout between 150 ms and 300 ms

  Scenario: Initial cold-start election
    When all nodes are started simultaneously
    And one node's election timeout fires first
    Then that node transitions to "Candidate" state
    And it increments its epoch to 1
    And it votes for itself
    And it sends RequestVote RPCs to the other 2 nodes
    When a majority (2 of 3) of nodes grant their votes
    Then the candidate transitions to "Leader" state
    And the other nodes record the leader id for epoch 1
    And the leader begins sending periodic heartbeat AppendEntries RPCs

  Scenario: Follower receives heartbeat before its election timeout
    Given node-1 is the leader in epoch 1
    When node-0 receives a heartbeat from node-1 before its election timeout
    Then node-0 resets its election timeout
    And node-0 remains in "Follower" state

  Scenario: Leader sends no-op entry on election
    Given node-2 wins the election for epoch 2
    When node-2 becomes the leader
    Then node-2 appends a no-op log entry at the start of epoch 2
    And the no-op entry is replicated to a majority before serving reads
    And all previously uncommitted entries from epoch 1 are implicitly committed
```

---

## Feature 2: Leader Election — Edge Cases

```gherkin
Feature: Leader election under adverse conditions
  Elections must converge even when split votes or stale candidates appear.

  Background:
    Given a cluster of 5 nodes [node-0 .. node-4]

  Scenario: Split vote causes election retry
    When node-1 and node-3 both time out and become candidates in epoch 3
    And node-1 receives votes from {node-0, node-1}
    And node-3 receives votes from {node-3, node-4}
    And node-2 has already voted for node-1
    Then neither candidate reaches quorum (3 of 5)
    And both candidates' election timeouts expire (with randomised jitter)
    And a new election begins in epoch 4
    And exactly one leader is elected in epoch 4

  Scenario: Candidate with stale log is rejected
    Given node-0 has log entries up to index 10 at epoch 2
    And node-1 has log entries up to index 8 at epoch 2
    When node-1 becomes a candidate for epoch 3
    And node-1 sends RequestVote to node-0
    Then node-0 rejects the vote because node-1's log is less up-to-date
    And node-0's votedFor remains unset for epoch 3

  Scenario: Candidate discovers higher epoch and steps down
    Given node-2 is a candidate in epoch 5
    When node-2 receives a RequestVote response with epoch 6
    Then node-2 transitions to "Follower" state
    And node-2 updates its current epoch to 6

  Scenario: Only one leader per epoch (leader election safety)
    Given any sequence of elections across N nodes
    Then for every epoch, at most one node is in "Leader" state
    And no two nodes simultaneously believe they are leader for the same epoch
```

---

## Feature 3: Pre-Vote Protocol

```gherkin
Feature: Pre-Vote prevents disruptive elections
  A partitioned node must not force a valid leader to step down when it
  rejoins. The Pre-Vote phase tests viability before incrementing the epoch.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And node-1 is the leader in epoch 4

  Scenario: Partitioned node uses Pre-Vote before real election
    Given node-2 is network-partitioned from node-0 and node-1
    When node-2's election timeout fires
    Then node-2 sends PreVote RPCs (without incrementing its epoch)
    And node-2 does not increment its epoch to 5 yet

  Scenario: Pre-Vote rejected when leader is healthy
    Given node-2 sends PreVote RPCs to node-0 and node-1
    And both node-0 and node-1 have heard from the leader within the election timeout
    Then node-0 rejects the PreVote
    And node-1 rejects the PreVote
    And node-2 does not transition to "Candidate" state
    And the cluster continues with node-1 as leader in epoch 4

  Scenario: Pre-Vote succeeds when leader is actually down
    Given node-1 crashes
    And node-2's election timeout fires
    When node-2 sends PreVote RPCs to node-0
    And node-0 has not heard from the leader within the election timeout
    Then node-0 grants the PreVote
    And node-2 transitions to "Candidate" state
    And node-2 increments its epoch to 5
    And proceeds with a real RequestVote election
```

---

## Feature 4: Log Replication — Happy Path

```gherkin
Feature: Log replication under normal conditions
  The leader must replicate client commands to a majority of followers
  before committing, ensuring linearisable reads and writes.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And node-0 is the leader in epoch 1

  Scenario: Client write is replicated and committed
    When a client sends command "SET x = 42" to node-0
    Then node-0 appends the entry at the next log index with epoch 1
    And node-0 sends AppendEntries RPCs to node-1 and node-2
    When node-1 acknowledges the entry
    Then the entry is committed (majority = 2 of 3 including leader)
    And node-0 applies the entry to its state machine
    And node-0 returns success to the client
    And node-2 eventually applies the committed entry to its state machine

  Scenario: Multiple entries are batched in a single AppendEntries
    When the client sends 5 commands in rapid succession
    Then node-0 batches them into a single AppendEntries RPC
    And followers append all 5 entries atomically
    And all 5 entries are committed once a majority acknowledges

  Scenario: Follower consistency check passes
    Given node-1's log matches node-0's log up to index 10
    When node-0 sends AppendEntries with prevLogIndex=10 and prevLogTerm=1
    And the new entry is at index 11
    Then node-1 verifies its log entry at index 10 has term 1
    And node-1 appends the new entry at index 11
    And node-1 returns success

  Scenario: Heartbeats carry commit index
    Given entries up to index 15 are committed
    When node-0 sends a heartbeat (empty AppendEntries) to node-1
    Then the heartbeat includes leaderCommitIndex = 15
    And node-1 advances its commit index to min(15, last log index)
    And node-1 applies any newly committed entries to its state machine
```

---

## Feature 5: Log Replication — Conflict Resolution

```gherkin
Feature: Log conflict detection and resolution
  When a follower's log diverges from the leader, the leader must
  repair the follower's log to re-establish the Log Matching invariant.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And node-0 is the leader in epoch 3

  Scenario: Follower with conflicting entry truncates and re-appends
    Given node-1 has entry (index=8, epoch=2, cmd="SET y=1")
    And node-0 has entry (index=8, epoch=3, cmd="SET y=2")
    When node-0 sends AppendEntries with prevLogIndex=7, prevLogTerm=2, entry at index 8
    And node-1's entry at index 7 matches prevLogTerm=2
    Then node-1 deletes its entry at index 8 (conflicting epoch)
    And node-1 appends the leader's entry (index=8, epoch=3, cmd="SET y=2")

  Scenario: Follower with missing entries receives backfill
    Given node-2 has entries only up to index 5
    And node-0 has entries up to index 10
    When node-0 sends AppendEntries with prevLogIndex=5
    And node-2 confirms its log matches at index 5
    Then node-0 sends entries 6 through 10 in subsequent RPCs
    And node-2's log converges with the leader's log

  Scenario: Follower rejects AppendEntries with mismatched prevLogTerm
    Given node-1 has entry (index=7, epoch=1)
    And node-0 sends AppendEntries with prevLogIndex=7, prevLogTerm=2
    Then node-1 rejects the AppendEntries
    And node-0 decrements nextIndex for node-1
    And node-0 retries with a lower prevLogIndex until consistency is found
```

---

## Feature 6: Pull-Based Replication (KRaft-Style Fetch)

```gherkin
Feature: Pull-based metadata replication via Fetch RPCs
  Following KRaft's design, followers and observers pull log entries
  from the leader via Fetch RPCs instead of receiving push-based AppendEntries.

  Background:
    Given a cluster of 3 voter nodes and 2 observer nodes
    And node-0 is the active controller (leader) in epoch 5

  Scenario: Follower fetches new entries from leader
    When node-1 sends a Fetch RPC with fetch_offset=20 and last_fetched_epoch=5
    Then node-0 validates the offset against its log
    And responds with entries from offset 20 onwards
    And includes the current high watermark in the response
    When node-1 sends a subsequent Fetch RPC
    Then node-1 receives the updated high watermark
    And commits entries up to the new high watermark

  Scenario: Two fetch rounds needed to commit
    Given node-0 appends entry at offset 25
    When node-1 fetches and receives entries up to offset 25
    Then node-1 does not yet commit offset 25 (high watermark not advanced)
    When node-1 sends a second Fetch RPC after majority replication
    Then the response includes high watermark >= 25
    And node-1 commits entries up to offset 25

  Scenario: Observer fetches metadata without voting rights
    When observer-0 sends a Fetch RPC to node-0
    Then node-0 responds with log entries
    But observer-0's acknowledgement does NOT count toward quorum
    And observer-0 cannot participate in elections

  Scenario: Fetch detects log divergence
    Given node-1 has a diverging entry at offset 18 with epoch 4
    And node-0's log at offset 18 has epoch 5
    When node-1 sends a Fetch RPC with fetch_offset=19 and last_fetched_epoch=4
    Then node-0 responds with a DivergingEpoch field indicating truncation point
    And node-1 truncates its log to the divergence point
    And node-1 re-fetches from the corrected offset
```

---

## Feature 7: Persistence and Crash Recovery

```gherkin
Feature: Durable state survives crashes and restarts
  The three pieces of durable state — currentEpoch, votedFor, and log —
  must be persisted synchronously before responding to any RPC.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]

  Scenario: Node restarts and recovers persisted state
    Given node-1 is a follower in epoch 3 and has voted for node-0
    And node-1's log contains entries up to index 20
    When node-1 crashes and restarts
    Then node-1 reads currentEpoch=3 from stable storage
    And node-1 reads votedFor=node-0 from stable storage
    And node-1 replays its log from index 0 to 20 to rebuild state machine
    And node-1 resumes as a follower without triggering a new election

  Scenario: Node recovers from snapshot plus log tail
    Given node-1 has a snapshot at index 50 and log entries from 51 to 75
    When node-1 restarts
    Then node-1 loads the snapshot to restore state machine at index 50
    And node-1 replays log entries 51 through 75
    And node-1's state machine is identical to a full log replay

  Scenario: Crash before persisting vote does not double-vote
    Given node-2 receives RequestVote from node-0 for epoch 4
    And node-2 crashes before persisting votedFor=node-0
    When node-2 restarts
    Then node-2's votedFor is empty for epoch 4
    And node-2 is free to vote for a different candidate in epoch 4
    And safety is preserved because the grant was never acknowledged

  Scenario: Crash after persisting vote but before sending response
    Given node-2 persists votedFor=node-0 for epoch 4
    And node-2 crashes before sending the VoteResponse
    When node-2 restarts
    Then node-2's votedFor=node-0 for epoch 4 is retained
    And node-2 cannot vote for a different candidate in epoch 4
    And node-0 may or may not have received the vote (handled by election retry)
```

---

## Feature 8: Log Compaction and Snapshots

```gherkin
Feature: Log compaction via periodic snapshotting
  Nodes independently take snapshots to bound log size and enable
  fast catch-up for slow or new nodes.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And node-0 is the leader

  Scenario: Node takes a snapshot at a configured threshold
    Given node-0's log has grown to 10,000 entries
    And the snapshot threshold is configured at 8,000 entries
    When the snapshot trigger fires
    Then node-0 writes a snapshot containing the state machine at index 8,000
    And the snapshot includes the epoch of the last included entry
    And the snapshot includes the current voter set
    And node-0 discards log entries up to index 8,000

  Scenario: Leader sends snapshot to slow follower
    Given node-0 has discarded log entries before index 8,000 (snapshot taken)
    And node-2's nextIndex is 5,000 (it fell behind)
    When node-0 attempts to send AppendEntries for index 5,000
    Then node-0 detects the entry has been discarded
    And node-0 sends an InstallSnapshot RPC to node-2
    And the snapshot is transferred in chunks
    When node-2 finishes receiving the snapshot
    Then node-2 loads the snapshot state machine
    And node-2 sets its log start offset to 8,001
    And subsequent Fetch/AppendEntries RPCs resume from index 8,001

  Scenario: Snapshot consistency across nodes
    Given all 3 nodes independently take snapshots
    Then each snapshot at the same log index produces an identical state machine
    And the snapshot's last_included_epoch matches across nodes
```

---

## Feature 9: Network Partition Handling

```gherkin
Feature: Cluster behaviour during and after network partitions
  The system must remain available when a majority partition exists,
  and must heal correctly when the partition is resolved.

  Background:
    Given a cluster of 5 nodes [node-0 .. node-4]
    And node-0 is the leader in epoch 5

  Scenario: Minority partition — leader retains quorum
    When node-3 and node-4 are partitioned from the rest
    Then node-0 still has a quorum of {node-0, node-1, node-2} (3 of 5)
    And client writes continue to be accepted and committed
    And node-3 and node-4 eventually time out and start elections
    But their elections fail (cannot reach quorum)

  Scenario: Majority partition — leader loses quorum and steps down
    When node-0 is isolated from all other nodes
    And node-0's Check Quorum timer fires
    Then node-0 cannot confirm communication with a majority
    And node-0 steps down to "Follower" state
    And nodes {node-1, node-2, node-3, node-4} elect a new leader in epoch 6
    And client writes sent to old leader node-0 are rejected

  Scenario: Partition heals and stale leader reconciles
    Given node-0 was isolated and stepped down
    And node-1 became leader in epoch 6 and committed new entries
    When the partition heals and node-0 reconnects
    Then node-0 discovers epoch 6 from heartbeats/Fetch responses
    And node-0 updates its epoch to 6
    And node-0 truncates any uncommitted entries from epoch 5
    And node-0 replicates entries from node-1 to catch up

  Scenario: Symmetric partition with even split (no progress)
    Given a cluster of 4 nodes (ill-advised but tested)
    When the network splits into {node-0, node-1} and {node-2, node-3}
    Then neither partition can form a quorum (need 3 of 4)
    And no leader is elected in either partition
    And client writes are unavailable until the partition heals
```

---

## Feature 10: Check Quorum Mechanism

```gherkin
Feature: Check Quorum prevents split-brain
  The leader must periodically verify it can communicate with a
  majority of followers; otherwise it must step down.

  Background:
    Given a cluster of 5 nodes
    And node-0 is the leader
    And the check quorum interval is 2× the election timeout

  Scenario: Leader passes quorum check
    Given node-0 has received Fetch/heartbeat responses from node-1 and node-2
      within the check quorum interval
    When the check quorum timer fires
    Then node-0 confirms communication with a majority (3 of 5 including self)
    And node-0 remains in "Leader" state

  Scenario: Leader fails quorum check and steps down
    Given node-0 has only received responses from node-1 in the last interval
    When the check quorum timer fires
    Then node-0 cannot confirm majority (only 2 of 5)
    And node-0 transitions to "Follower" state
    And a new election is triggered among reachable nodes
```

---

## Feature 11: Client Interaction and Request Routing

```gherkin
Feature: Client request handling and routing
  Clients must be able to discover the leader and have their
  requests handled idempotently.

  Background:
    Given a cluster of 3 nodes
    And node-0 is the leader

  Scenario: Client sends write to the leader
    When a client sends "SET key=value" to node-0
    Then node-0 accepts the request and begins replication
    And responds with success after the entry is committed

  Scenario: Client sends write to a follower
    When a client sends "SET key=value" to node-1 (a follower)
    Then node-1 rejects the request
    And node-1 returns a redirect response containing the leader id (node-0)
    And the client retries the request against node-0

  Scenario: Idempotent client requests with serial numbers
    Given the client sends "SET key=value" with serial_number=42
    And the leader commits the entry
    When the client retries "SET key=value" with serial_number=42
      (e.g., due to network timeout before receiving the response)
    Then the leader detects the duplicate serial number
    And the leader returns the cached response without re-applying the command

  Scenario: Client discovers new leader after election
    Given node-0 was the leader and has stepped down
    And node-2 is the new leader in epoch 6
    When the client sends a request to node-0
    Then node-0 responds with leader_hint=node-2 and epoch=6
    And the client updates its leader reference to node-2
```

---

## Feature 12: Dynamic Cluster Membership

```gherkin
Feature: Dynamic quorum changes (add/remove voter)
  Voter set changes are applied one at a time through the replicated log
  to prevent disjoint majorities.

  Background:
    Given a cluster of 3 voters [node-0, node-1, node-2]
    And node-0 is the leader in epoch 3

  Scenario: Add a new voter to the cluster
    Given node-3 joins as a non-voting observer
    And node-3 catches up with the leader's log (lag < threshold)
    When the operator sends AddVoter(node-3) to the leader
    Then node-0 appends a VotersRecord to the metadata log:
      voters = [node-0, node-1, node-2, node-3]
    And the VotersRecord is replicated to a majority of the OLD config (2 of 3)
    And once committed, node-3 becomes a full voting member
    And the quorum size increases to 3 of 4

  Scenario: Remove a voter from the cluster
    Given a cluster of 4 voters [node-0, node-1, node-2, node-3]
    When the operator sends RemoveVoter(node-3) to the leader
    Then node-0 appends a VotersRecord with voters = [node-0, node-1, node-2]
    And the record is committed by the current (4-node) quorum
    And after commit, node-3 stops participating in elections
    And the quorum size returns to 2 of 3

  Scenario: Cannot perform concurrent membership changes
    Given an AddVoter(node-3) is in-flight (not yet committed)
    When the operator sends AddVoter(node-4)
    Then the leader rejects the second change with an error
    And responds "membership change already in progress"

  Scenario: Leader being removed continues until commit
    Given a RemoveVoter(node-0) request is submitted
    And node-0 is the current leader
    Then node-0 appends the VotersRecord removing itself
    And node-0 continues serving as leader until the record is committed
    And after commit, node-0 steps down
    And the remaining voters elect a new leader

  Scenario: New voter joins with empty log
    Given node-4 joins the cluster with an empty log
    Then node-4 starts as a non-voting observer
    And node-4 fetches the latest snapshot from the leader (if available)
    And node-4 replays log entries after the snapshot
    And node-4 remains non-voting until promoted via AddVoter
```

---

## Feature 13: Safety Invariants (Cross-Cutting)

```gherkin
Feature: Raft safety invariants hold under all conditions
  These properties must be verified continuously, including during
  fault injection, network partitions, and membership changes.

  Scenario: Leader Append-Only
    Given any node is in "Leader" state
    Then the leader never overwrites or deletes entries in its own log
    And the leader only appends new entries

  Scenario: Leader Completeness
    Given a leader is elected in epoch E
    Then the leader's log contains all entries committed in epochs < E
    And no committed entry is ever lost due to leader election

  Scenario: Log Matching across nodes
    Given two nodes have a log entry with the same index and epoch
    Then their logs are identical from index 0 up to that entry

  Scenario: State Machine Safety
    Given node A applies entry at index I to its state machine
    Then no other node applies a different entry at index I
    And the state machine output is deterministic for the same input sequence

  Scenario: Election Safety under crash-recovery
    Given nodes may crash and restart at any point during an election
    Then at most one leader is elected per epoch
    And persisted votedFor prevents double-voting within the same epoch
```

---

## Feature 14: Timing and Performance

```gherkin
Feature: Timing-sensitive behaviour and performance boundaries
  The system must honour the Raft timing constraint:
  broadcastTime << electionTimeout << avgTimeBetweenFailures

  Background:
    Given broadcastTime = 1–20 ms (configurable per test environment)
    And electionTimeout = 150–300 ms (randomised per node)

  Scenario: Leader election completes within expected latency
    When a leader election is triggered
    Then a new leader is elected within 2× electionTimeout (worst case: one retry)
    And election latency is recorded as a metric

  Scenario: Heartbeat frequency prevents unnecessary elections
    Given the heartbeat interval is less than electionTimeout / 3
    When the leader is healthy
    Then no follower triggers an election due to heartbeat timeout
    And the heartbeat interval is recorded as a metric

  Scenario: Write commit latency under normal conditions
    When a client submits a write to the leader
    Then the entry is committed within broadcastTime + fsync latency
    And the commit latency is recorded as a metric
    And p99 commit latency is below 50 ms for a 3-node local cluster

  Scenario: Leader fsync runs concurrently with replication
    When the leader appends an entry to its local log
    Then the leader initiates fsync and sends AppendEntries RPCs concurrently
    And commits as soon as a majority acknowledges (including itself after fsync)
```

---

## Feature 15: Observability and Metrics

```gherkin
Feature: Cluster metrics and health observability
  Operators must be able to monitor cluster health, current leadership,
  replication lag, and election history.

  Background:
    Given a running cluster of 3 nodes

  Scenario: Query current leader and epoch
    When an operator queries the cluster metadata endpoint
    Then the response includes the current leader node id
    And the current epoch number
    And the high watermark offset

  Scenario: Monitor replication lag per follower
    When an operator queries per-node metrics
    Then each follower's lag (leader log end − follower log end) is reported
    And lag is expressed in number of entries and optionally in bytes
    And an alert fires if lag exceeds a configured threshold

  Scenario: Election event is logged and metered
    When a leader election occurs
    Then an event is emitted with: new_leader_id, epoch, election_latency_ms
    And the "elections_total" counter is incremented
    And the "election_latency_avg" gauge is updated

  Scenario: Describe quorum state
    When an operator queries the quorum describe endpoint
    Then the response includes each voter's:
      | field                 | description                          |
      | node_id               | Unique node identifier               |
      | log_end_offset        | Last offset in the node's log        |
      | lag                   | Entries behind the leader            |
      | last_fetch_timestamp  | Time since last successful fetch     |
      | last_caught_up_time   | Time since fully caught up           |
      | status                | Leader / Follower / Observer         |
```

---

## Feature 16: Graceful Shutdown and Controlled Leadership Transfer

```gherkin
Feature: Graceful node shutdown with optional leadership transfer
  A node being shut down should drain cleanly and, if it is the leader,
  optionally transfer leadership before stopping.

  Background:
    Given a cluster of 3 nodes
    And node-0 is the leader

  Scenario: Leader initiates graceful shutdown with transfer
    When node-0 receives a shutdown signal
    Then node-0 selects the most caught-up follower (e.g., node-1)
    And node-0 sends a TimeoutNow RPC to node-1
    And node-1 immediately starts an election (skipping its election timeout)
    And node-1 becomes the new leader in epoch+1
    And node-0 stops accepting new requests and shuts down

  Scenario: Follower graceful shutdown
    When node-2 receives a shutdown signal
    Then node-2 stops sending Fetch RPCs
    And node-2 persists its current state to stable storage
    And node-2 shuts down without triggering an election
    And the leader detects node-2's absence via missed heartbeat/fetch responses

  Scenario: Abrupt crash (no graceful shutdown)
    When node-1 crashes without sending any shutdown signal
    Then the leader detects node-1's absence after heartbeat timeout
    And the cluster continues operating with 2 of 3 nodes
    And client writes are still committed (quorum = 2)
```

---

## Open Questions for QA and Architecture Alignment

1. **Pull vs Push replication:** The story references KRaft's pull-based (Fetch)
   model. Scenarios cover both push (AppendEntries) and pull (Fetch) styles.
   The tech-spec should clarify which model the Rust implementation will use —
   some scenarios may be removed once decided.

2. **Observer role:** KRaft distinguishes voters from observers (brokers).
   Should the Rust implementation include an observer role, or only voters?

3. **Snapshot transfer mechanism:** KRaft uses FetchSnapshot with chunked
   transfer. The implementation-plan should specify the chunk size and
   transport (TCP stream vs RPC-per-chunk).

4. **Metrics exposure format:** Scenarios reference metrics endpoints.
   Architecture should specify whether Prometheus, OpenTelemetry, or a
   custom format is used.
