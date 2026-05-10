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
    And followers begin sending periodic Fetch RPCs to the leader

  Scenario: Follower resets election timer via Fetch response
    Given node-1 is the leader in epoch 1
    When node-0 sends a Fetch RPC to node-1 before its election timeout
    Then node-1 responds with an empty Fetch response (no new entries) carrying the current epoch and high watermark
    And node-0 treats the valid Fetch response as proof of leader liveness
    And node-0 resets its election timeout
    And node-0 remains in "Follower" state

  Scenario: Leader sends no-op entry on election
    Given node-2 wins the election for epoch 2
    When node-2 becomes the leader
    Then node-2 appends a no-op log entry at the start of epoch 2
    And the no-op entry is replicated to a majority before serving reads
    And any epoch-1 entries present on node-2's log that are now covered by the committed no-op are committed (entries from prior terms not on the new leader remain uncommitted)
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
    And node-1 receives votes from {node-0, node-1} (2 of 5)
    And node-3 receives votes from {node-3, node-4} (2 of 5)
    And node-2 is temporarily unreachable and does not respond to either candidate
    Then neither candidate reaches quorum (need 3 of 5)
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
  Following KRaft's pull-based model, followers initiate Fetch RPCs
  to the leader to retrieve new log entries. The leader commits
  once a majority of voters have fetched and acknowledged entries.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And node-0 is the leader in epoch 1

  Scenario: Client write is replicated via follower Fetch and committed
    When a client sends command "SET x = 42" to node-0
    Then node-0 appends the entry at the next log index with epoch 1
    When node-1 sends a Fetch RPC with its current fetch_offset
    Then node-0 responds with the new entry and the current high watermark
    When node-2 also fetches and acknowledges the entry
    Then node-0 advances the high watermark (majority = 2 of 3 including leader)
    And on the next Fetch response, followers learn the new high watermark
    And followers apply the committed entry to their state machines
    And node-0 returns success to the client

  Scenario: Multiple entries are batched in a single Fetch response
    When the client sends 5 commands in rapid succession
    And node-0 appends all 5 entries to its local log
    When node-1 sends a Fetch RPC
    Then node-0 returns all 5 entries in a single Fetch response
    And node-1 appends all 5 entries atomically
    And all 5 entries are committed once a majority has fetched them

  Scenario: Follower consistency check passes during Fetch
    Given node-1's log matches node-0's log up to index 10
    When node-1 sends a Fetch RPC with fetch_offset=11 and last_fetched_epoch=1
    Then node-0 verifies the consistency of node-1's log position
    And node-0 responds with the entry at index 11
    And node-1 appends the new entry at index 11

  Scenario: Empty Fetch response carries high watermark (heartbeat equivalent)
    Given entries up to index 15 are committed
    When node-1 sends a Fetch RPC to node-0 and no new entries exist
    Then node-0 responds with an empty Fetch response including high watermark = 15
    And node-1 advances its commit index to min(15, last log index)
    And node-1 resets its election timer (proof of leader liveness)
    And node-1 applies any newly committed entries to its state machine
```

---

## Feature 5: Log Replication — Conflict Resolution

```gherkin
Feature: Log conflict detection and resolution via Fetch
  When a follower's log diverges from the leader, the Fetch response
  signals the divergence and the follower truncates and re-fetches
  to re-establish the Log Matching invariant.

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]
    And node-0 is the leader in epoch 3

  Scenario: Follower with conflicting entry truncates and re-fetches
    Given node-1 has entry (index=8, epoch=2, cmd="SET y=1")
    And node-0 has entry (index=8, epoch=3, cmd="SET y=2")
    When node-1 sends a Fetch RPC with fetch_offset=9 and last_fetched_epoch=2
    Then node-0 detects the divergence at offset 8
    And node-0 responds with a DivergingEpoch field indicating truncation to index 7
    And node-1 truncates its log from index 8 onwards
    And node-1 re-fetches from index 8 and receives the leader's entry

  Scenario: Follower with missing entries receives backfill via Fetch
    Given node-2 has entries only up to index 5
    And node-0 has entries up to index 10
    When node-2 sends a Fetch RPC with fetch_offset=6
    Then node-0 responds with entries 6 through 10
    And node-2 appends all entries and its log converges with the leader's log

  Scenario: Fetch detects epoch mismatch and triggers truncation
    Given node-1 has entry (index=7, epoch=1)
    And node-0's log at index 7 has epoch=2
    When node-1 sends a Fetch RPC with fetch_offset=8 and last_fetched_epoch=1
    Then node-0 responds with DivergingEpoch indicating the divergence point
    And node-1 truncates its log back to the point of agreement
    And node-1 re-fetches from the corrected offset
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
  The `quorum-state` file (per architecture.md §3.1) persists: `HardState`
  (currentEpoch, votedFor), `commit_index`, and `VoterSet` — all written
  atomically (write-tmp + rename + fsync) before responding to any RPC.
  The log itself is persisted via append-only segment files (per tech-spec
  §5.3).  On recovery, `last_applied` is volatile and rebuilt by replaying
  only *committed* log entries (those at or below the persisted
  `commit_index`).

  Background:
    Given a cluster of 3 nodes [node-0, node-1, node-2]

  Scenario: Node restarts and recovers persisted state
    Given node-1 is a follower in epoch 3 and has voted for node-0
    And node-1's log contains entries up to index 20
    And node-1's persisted commit_index is 18
    When node-1 crashes and restarts
    Then node-1 reads currentEpoch=3 from the quorum-state file
    And node-1 reads votedFor=node-0 from the quorum-state file
    And node-1 reads commit_index=18 from the quorum-state file
    And node-1 replays only committed log entries (index 1 through 18) to rebuild its state machine
    And entries 19–20 remain in the log but are NOT applied until they are committed
    And node-1 resumes as a follower without triggering a new election

  Scenario: Node recovers from snapshot plus log tail
    Given node-1 has a snapshot at index 50 and log entries from 51 to 75
    And node-1's persisted commit_index is 72
    When node-1 restarts
    Then node-1 loads the snapshot to restore state machine at index 50
    And node-1 replays only committed log entries 51 through 72
    And entries 73–75 remain in the log but are NOT applied until committed
    And node-1's state machine is identical to a full committed-entry replay

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

  Scenario: Leader sends snapshot to slow follower via FetchSnapshot
    Given node-0 has discarded log entries before index 8,000 (snapshot taken)
    And node-2's fetch_offset is 5,000 (it fell behind)
    When node-2 sends a Fetch RPC with fetch_offset=5,000
    Then node-0 detects the requested offset has been compacted
    And node-0 responds indicating that a snapshot is required
    And node-2 initiates a FetchSnapshot RPC to node-0
    And the snapshot is transferred in chunks
    When node-2 finishes receiving the snapshot
    Then node-2 loads the snapshot state machine
    And node-2 sets its log start offset to 8,001
    And subsequent Fetch RPCs resume from index 8,001

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
    Then node-0 discovers epoch 6 from Fetch responses
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
    Given node-0 has received Fetch responses from node-1 and node-2
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

## Feature 11: Inter-Node Request Routing and Leader Discovery

```gherkin
Feature: Inter-node request routing and leader discovery
  Per `tech-spec.md` §2.6, `xraft-client` is an **internal** crate
  providing peer-to-peer RPC (`PeerClient` for Fetch, Vote,
  FetchSnapshot) with `ConnectionPool`, and an `AdminClient` for
  operational queries.  It is **not** an external consumer SDK — no
  `propose`/`read` API for outside callers is in scope for v1.
  (Note: `architecture.md` §2.5 describes `xraft-client` as a dual-role
  library with external `XRaftClient.propose`/`read`; these scenarios
  follow the tech-spec's internal-only scoping as the authoritative v1
  scope boundary.)
  Leader discovery occurs through Fetch RPC responses that carry
  leader_id and epoch metadata.

  Background:
    Given a cluster of 3 nodes
    And node-0 is the leader

  Scenario: Follower discovers leader via Fetch response
    When node-1 sends a Fetch RPC to node-0
    Then the Fetch response includes the current leader_id (node-0) and epoch
    And node-1 caches the leader identity for subsequent operations

  Scenario: Node sends Fetch to a non-leader
    When node-2 sends a Fetch RPC to node-1 (a follower)
    Then node-1 responds with NOT_LEADER and includes leader_hint=node-0
    And node-2 redirects its Fetch RPCs to node-0

  Scenario: Node detects leader change via epoch bump
    Given node-0 was the leader in epoch 5
    And node-1 becomes the new leader in epoch 6
    When node-2 sends a Fetch RPC to node-0
    Then node-0 responds indicating it is no longer the leader
    And node-2 discovers node-1 as the new leader in epoch 6
    And node-2 redirects its Fetch RPCs to node-1

  Scenario: All nodes converge on leader identity after election
    When a new election completes and node-2 wins epoch 7
    Then within one Fetch cycle, all followers learn node-2 is the leader
    And all nodes' internal leader tracking agrees on node-2 for epoch 7

  Scenario: Internal peer RPC routes through xraft-client
    When node-1 needs to send a Vote RPC to node-2 during an election
    Then `xraft-client` handles the gRPC connection to node-2
    And if node-2 is unreachable, the RPC fails with a timeout error
    And the caller retries or proceeds based on election logic

  Scenario: AdminClient queries cluster status
    When an operator uses the AdminClient to query cluster status
    Then the AdminClient returns leader identity, current epoch, and node roles
    And the AdminClient can query `/health` and `/metrics` endpoints
    And the AdminClient can trigger a snapshot
```

---

## Feature 12: Static Cluster Membership and Observer Join

```gherkin
Feature: Static voter membership with observer join
  The v1 baseline uses static membership — the voter set is fixed at cluster
  bootstrap.  Per `tech-spec.md` §2.7/§3 and `implementation-plan.md`
  Stage 7.2, dynamic membership (`AddVoter`/`RemoveVoter`) is **out of scope
  for v1** and deferred to a future story entirely.  Any
  `AddVoter`/`RemoveVoter` command is rejected with `UNSUPPORTED`.
  (`architecture.md` §2.1/§10 uses the phrase "stretch goal within this
  story"; these scenarios adopt the stricter `tech-spec.md` scoping because
  the tech-spec is the authoritative scope document.)
  Observers (non-voting nodes) may join to replicate the log for read scaling.

  Background:
    Given a cluster of 3 voters [node-0, node-1, node-2] configured at startup
    And node-0 is the leader in epoch 3

  Scenario: Voter set is fixed at bootstrap
    Given the cluster configuration file lists voters = [node-0, node-1, node-2]
    When the cluster starts
    Then all 3 nodes load the static voter configuration
    And quorum size is fixed at 2 of 3
    And the voter set remains unchanged throughout cluster lifetime in v1

  Scenario: Observer joins and replicates the log
    Given observer-0 is configured as a non-voting observer
    When observer-0 starts and connects to the cluster
    Then observer-0 sends Fetch RPCs to the leader (node-0)
    And observer-0 receives log entries and the current high watermark
    But observer-0 does NOT count toward quorum for commits
    And observer-0 does NOT participate in elections

  Scenario: Observer catches up from snapshot when far behind
    Given the leader has taken a snapshot at index 8,000
    And observer-0 joins with an empty log
    When observer-0 sends a Fetch RPC with fetch_offset=0
    Then the leader indicates a snapshot is required
    And observer-0 initiates a FetchSnapshot RPC
    And observer-0 loads the snapshot and resumes Fetch from index 8,001

  Scenario: AddVoter/RemoveVoter is rejected in v1 baseline
    When an operator attempts to issue an AddVoter or RemoveVoter command
    Then the node rejects the request with an UNSUPPORTED error
    And the error message indicates dynamic membership is not yet implemented
    And the voter set remains unchanged
    # Dynamic membership (AddVoter/RemoveVoter) is out of scope for v1
    # and deferred to a future story (tech-spec §2.7/§3)
```

---

## Feature 13: Safety Invariants (Cross-Cutting)

```gherkin
Feature: Raft safety invariants hold under all conditions
  These properties must be verified continuously, including during
  fault injection, network partitions, crash-recovery, and observer join.

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
    Then their logs are identical from index 1 up to that entry

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

  Scenario: Fetch frequency prevents unnecessary elections
    Given the Fetch interval is less than electionTimeout / 3
    When the leader is healthy
    Then no follower triggers an election due to missing Fetch responses
    And the Fetch interval is recorded as a metric

  Scenario: Write commit latency under normal conditions
    When the leader appends a new entry to the log
    Then the entry is committed after the leader fsync, at least one follower Fetch
      cycle retrieving the entry, follower fsync, and a second Fetch cycle advancing
      the high watermark
    And commit latency is recorded as a histogram metric for operational monitoring
    # Note: in pull-based replication, commit latency is not bounded by a simple
    # formula because it depends on follower Fetch scheduling intervals, fsync
    # durations on leader and followers, and the two-Fetch-round commit cycle.
    # Benchmarking/performance tuning is out of scope for v1 (tech-spec §3);
    # no specific p99 threshold is mandated.  This scenario validates that the
    # commit sequence completes correctly, not that it meets a latency target.

  Scenario: Leader fsync completes before serving entry via Fetch
    # Per tech-spec §5.3 and §6.2, fsync MUST complete before the
    # corresponding RPC response is sent.  The leader does NOT serve
    # an entry to followers until its own fsync for that entry finishes.
    When the leader appends an entry to its local log
    Then the leader fsyncs the entry to durable storage
    And only after fsync completes does the leader make the entry available to Fetch responses
    And followers retrieve the entry on their next Fetch RPC (after leader fsync)
    And the leader commits once a majority (including itself) has durably stored the entry
    # Parallelism is between fsync and *preparing* the next batch, not
    # between fsync and responding (per tech-spec §6.2).
```

---

## Feature 15: Observability and Metrics

```gherkin
Feature: Cluster metrics and health observability
  Operators must be able to monitor cluster health, current leadership,
  replication lag, and election history via the `/health` and `/metrics`
  endpoints defined in tech-spec §2.4.

  Background:
    Given a running cluster of 3 nodes

  Scenario: Health endpoint reports node status
    When an operator queries the `/health` endpoint on any node
    Then the response includes the node's current role (Leader / Follower / Observer)
    And the current epoch number
    And the current leader node id (if known)
    And liveness and readiness status for orchestrator probes

  Scenario: Metrics endpoint exposes replication lag
    When an operator queries the `/metrics` endpoint on the leader
    Then the response includes per-follower replication lag (leader_log_end − follower_fetch_offset)
    And lag is expressed in number of entries
    And a `xraft_replication_lag` gauge is exposed per follower (per architecture.md §7)

  Scenario: Election event is logged and metered
    When a leader election occurs
    Then an event is emitted via `tracing` with: new_leader_id, epoch, election_latency_ms
    And the `xraft_append_records_total` counter tracks total entries appended
    And the `xraft_election_latency_seconds` histogram is updated

  Scenario: Metrics endpoint exposes canonical cluster gauges
    # The canonical metric set is defined in architecture.md §7.
    # implementation-plan.md Stage 6.1 lists a subset for the initial /metrics
    # endpoint (xraft_current_term, xraft_commit_index, xraft_role,
    # xraft_election_count, xraft_append_latency_seconds, xraft_log_entries_total).
    # The full set below from architecture.md §7 is the target; Stage 6.1's
    # subset is the MVP that ships first and is extended in later stages.
    When an operator queries the `/metrics` endpoint on any node
    Then the response includes gauges matching architecture.md §7:
      | metric                              | type      | description                                    |
      | xraft_current_leader                | Gauge     | Node ID of current leader; -1 if unknown        |
      | xraft_current_term                  | Gauge     | Current Raft term / epoch                        |
      | xraft_commit_index                  | Gauge     | Highest committed log index                      |
      | xraft_log_end_offset                | Gauge     | Highest log index (may be ahead of commit)       |
      | xraft_replication_lag               | Gauge     | Entries behind leader (per replica)              |
      | xraft_election_latency_seconds      | Histogram | Time from candidacy to leader election           |
      | xraft_commit_latency_seconds        | Histogram | Time from proposal to commit (leader only)       |
      | xraft_append_records_total          | Counter   | Total entries appended                           |
      | xraft_fetch_requests_total          | Counter   | Total Fetch RPCs received (leader) / sent (follower) |
      | xraft_snapshot_installs_total       | Counter   | Snapshots installed by this node                 |
```

---

## Feature 16: Graceful Shutdown and Leader Step-Down

```gherkin
Feature: Graceful node shutdown with leader step-down
  A node being shut down should drain cleanly.  If it is the leader,
  it steps down and allows a natural election (via the standard Vote RPC)
  rather than using out-of-scope RPCs like TimeoutNow.

  Background:
    Given a cluster of 3 nodes
    And node-0 is the leader

  Scenario: Leader initiates graceful shutdown with transfer
    When node-0 receives a shutdown signal
    Then node-0 stops accepting new proposals
    And node-0 waits for in-flight entries to commit (or a short timeout)
    And node-0 steps down from leadership
    And a follower detects the absence of Fetch responses and triggers an election
    And a new leader is elected in epoch+1
    And node-0 shuts down cleanly

  Scenario: Follower graceful shutdown
    When node-2 receives a shutdown signal
    Then node-2 stops sending Fetch RPCs
    And node-2 persists its current state to stable storage
    And node-2 shuts down without triggering an election
    And the leader detects node-2's absence via missed Fetch requests

  Scenario: Abrupt crash (no graceful shutdown)
    When node-1 crashes without sending any shutdown signal
    Then the leader detects node-1's absence after Fetch timeout
    And the cluster continues operating with 2 of 3 nodes
    And client writes are still committed (quorum = 2)
```

---

## Alignment Notes

These scenarios adopt the following resolved positions from sibling documents.
Where sibling documents disagree, the position taken here and the rationale
are stated explicitly:

1. **Pull-based Fetch replication** — confirmed in `tech-spec.md` §2.2 and `architecture.md` §2.1.
   All replication scenarios use follower-initiated Fetch RPCs (no push-based AppendEntries).

2. **Observer role** — in scope per `tech-spec.md` §2.2. Observers replicate via Fetch but
   do not vote or count toward quorum.

3. **Static voter membership; dynamic membership deferred** — per
   `tech-spec.md` §2.7/§3 and `implementation-plan.md` Stage 7.2, the v1
   baseline uses static membership (voter set fixed at bootstrap).  Dynamic
   membership (`AddVoter`/`RemoveVoter`) is **out of scope for v1** and
   deferred to a future story entirely.  Any `AddVoter`/`RemoveVoter` command
   is rejected with `UNSUPPORTED`.  (`architecture.md` §2.1/§10 uses the
   phrase "stretch goal within this story"; these scenarios adopt the stricter
   `tech-spec.md` scoping because the tech-spec is the authoritative scope
   document.)
   Feature 12 tests static membership, observer join, and the rejection of
   dynamic membership commands in the v1 baseline.

4. **Internal-only client (`xraft-client`)** — per `tech-spec.md` §2.6,
   `xraft-client` is an **internal** crate providing peer-to-peer RPC
   (`PeerClient` for Fetch, Vote, FetchSnapshot; `ConnectionPool`) and
   admin/operational queries (`AdminClient`).  It is **not** an external
   consumer SDK — no `propose`/`read` API for outside callers is in scope
   for v1.  (`architecture.md` §2.5 describes `xraft-client` as a "Dual-Role
   Client Library" with an external `XRaftClient.propose`/`read` API; these
   scenarios adopt `tech-spec.md` §2.6's internal-only scoping because the
   tech-spec is the authoritative scope document for v1 boundaries.)
   Feature 11 tests internal routing, leader discovery via Fetch responses, and
   AdminClient operational queries.

5. **Observability endpoints** — `/health` (liveness/readiness) and `/metrics`
   per `tech-spec.md` §2.4.  **Metrics format:** per `tech-spec.md` §5.6,
   `implementation-plan.md` Stage 1.1, and `architecture.md` §7, cluster metrics
   are exposed via the `prometheus-client` crate in Prometheus exposition format
   on a `/metrics` HTTP endpoint.  These e2e scenarios test that a `/metrics`
   endpoint exists and exposes the canonical metric set from `architecture.md` §7
   in Prometheus format.
   Feature 15's canonical metric set is drawn from `architecture.md` §7.
   `implementation-plan.md` Stage 6.1 defines a smaller initial subset
   (`xraft_current_term`, `xraft_commit_index`, `xraft_role`, `xraft_election_count`,
   `xraft_append_latency_seconds`, `xraft_log_entries_total`); the full `architecture.md` §7
   set is the target and may be delivered incrementally across stages.  Feature 15 tests the
   full target set with a note about this phased delivery.

6. **Snapshot transfer** — uses `FetchSnapshot` RPC with chunked streaming per `tech-spec.md`
   §2.2. Chunk size and transport details are implementation-level decisions deferred to
   `implementation-plan.md`.
