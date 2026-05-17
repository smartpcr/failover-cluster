//! [`RealCluster`] — multi-node Raft cluster running over real gRPC
//! transport on localhost. Used by the Stage 8.1 real-network
//! integration tests called out in
//! [`tech-spec.md` §2.5](../../../docs/stories/failover-cluster-XRAFT/tech-spec.md).
//!
//! Each [`RealNode`] is a [`xraft_server::Server`] booted via
//! [`Server::start_with_state_machine`] with a
//! [`RecordingStateMachine`](crate::state_machine::RecordingStateMachine).
//! Tests inspect convergence by reading each node's recording handle
//! after waiting for replication.
//!
//! Leader-kill is implemented as `ServerHandle::abort()` (fail-stop),
//! which calls `JoinHandle::abort` on the driver, gRPC, and admin
//! tasks — mirroring the workstream brief's "leader kill is
//! implemented via Tokio task cancellation (abort the task's
//! `JoinHandle`) rather than process kill, since nodes are in-process
//! tasks".

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;
use tokio::sync::Notify;
use tracing::info;
use uuid::Uuid;

use xraft_core::config::{ClusterConfig, VoterConfig};
use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::types::{NodeId, NodeRole};

use xraft_server::{Server, ServerConfig, ServerHandle};

use crate::state_machine::{RecordingHandle, RecordingStateMachine};

/// Tunables for [`RealCluster::start`]. Defaults are tuned to keep
/// real-network tests fast yet stable under workspace-parallel
/// `cargo test`:
///
/// * `rpc_timeout_ms = 800` — generous enough to survive a busy CI
///   box's scheduling jitter, tight enough that a failover still
///   finishes inside the test deadline budget.
/// * `connect_timeout_ms = 200` — local-only.
/// * `max_rpc_retries = 1` — failover happens on first follower
///   timeout, not after exponential back-off.
/// * `tick_interval_ms = 5`, `fetch_interval_ms = 10` — small enough
///   that 100 sequential proposals commit in under 10 s.
/// * `election_timeout_min_ms = 250`, `election_timeout_max_ms = 500` —
///   sub-second leader election with enough headroom that the
///   leader's first heart-beats reach every follower before any
///   election timer fires under heavy parallel load.
#[derive(Debug, Clone)]
pub struct RealClusterConfig {
    /// Number of voter nodes in the cluster (3 or 5 for the Stage 8.1
    /// scenarios; any number ≥ 1 is supported).
    pub size: usize,
    /// Lower bound for the per-node election timeout.
    pub election_min_ms: u64,
    /// Upper bound for the per-node election timeout. Must be ≥
    /// `election_min_ms`.
    pub election_max_ms: u64,
    /// Driver tick cadence — also feeds the heart-beat / fetch tempo.
    pub tick_ms: u64,
    /// Follower fetch cadence.
    pub fetch_ms: u64,
    /// Per-RPC outbound timeout. Kept tight so failover does not
    /// wait an entire heart-beat-RTT before declaring a peer dead.
    pub rpc_timeout_ms: u64,
    /// TCP connect timeout to peer endpoints.
    pub connect_timeout_ms: u64,
    /// Maximum retries per outbound RPC.
    pub max_rpc_retries: usize,
}

impl Default for RealClusterConfig {
    fn default() -> Self {
        // Election window 250-500 ms — same rationale as the
        // SimulatedCluster default: under heavy parallel `cargo test`
        // load the leader's first fetches can take >100 ms to land
        // on every follower. The engine NOW recovers a minority
        // stranded in `PreCandidate` via the
        // `PreVoteResponse.leader_hint` step-down path
        // (`xraft-core/src/node.rs::handle_pre_vote_response`,
        // operator answer
        // `engine-pre-vote-recovery → yes-add-leader-hint-step-down`),
        // but 250-500 ms still gives the leader breathing room
        // before any follower times out — keeping tests on the
        // happy path rather than exercising the recovery path on
        // every run.
        Self {
            size: 3,
            election_min_ms: 250,
            election_max_ms: 500,
            tick_ms: 5,
            fetch_ms: 10,
            rpc_timeout_ms: 800,
            connect_timeout_ms: 200,
            max_rpc_retries: 1,
        }
    }
}

impl RealClusterConfig {
    /// 3-node default config.
    pub fn three_node() -> Self {
        Self {
            size: 3,
            ..Self::default()
        }
    }

    /// 5-node config. Election timeout is widened further (600-1500
    /// ms) over the default because 5-node leader-failover scenarios
    /// involve five concurrent driver tasks competing for the multi-
    /// thread runtime; per-node fetch latency under that load can
    /// spike beyond the default 500 ms ceiling.
    pub fn five_node() -> Self {
        Self {
            size: 5,
            election_min_ms: 600,
            election_max_ms: 1500,
            ..Self::default()
        }
    }
}

/// A single real-network node inside a [`RealCluster`]: the server
/// [`ServerHandle`], its `(NodeId, gRPC port)` pair, the recording
/// state machine handle, and the per-node `TempDir` (held to keep the
/// data dir alive for the duration of the test).
pub struct RealNode {
    pub node_id: NodeId,
    pub grpc_port: u16,
    pub recording: RecordingHandle,
    pub handle: Option<ServerHandle>,
    // Keep TempDir alive for the lifetime of the node; dropping it
    // would delete the data dir before the server's storage layer
    // tears down.
    _data_dir: TempDir,
}

impl RealNode {
    pub fn is_alive(&self) -> bool {
        self.handle.is_some()
    }
}

/// 3- or 5-node Raft cluster talking real gRPC over localhost.
pub struct RealCluster {
    pub nodes: Vec<RealNode>,
    /// Pre-allocated `(NodeId, port)` map. Tests read this to predict
    /// which TCP port a given node listens on after start.
    pub ports: Vec<(NodeId, u16)>,
    /// server handles aborted via
    /// [`Self::kill`] are PARKED here instead of being dropped, so
    /// [`Self::shutdown`] can `.await` each one and surface a panic
    /// or unexpected error that happened BEFORE the abort signal
    /// reached the underlying driver / gRPC tasks.
    killed_handles: Vec<(NodeId, ServerHandle)>,
    /// ONE [`Notify`] shared
    /// across every node's [`RecordingStateMachine`] via
    /// [`RecordingStateMachine::with_state_change`]. Bumped on each
    /// `apply()` call on ANY node. Replaces the fixed `25 ms` poll
    /// in [`Self::await_applied_at_least`] with an event-driven wait
    /// (mirrors the simulated harness — see
    /// [`crate::simulated::SimulatedCluster::await_applied_at_least`])
    /// so apply convergence wakes the instant a follower applies an
    /// entry instead of suffering up to a full scheduler quantum of
    /// 25 ms latency per poll under workspace-parallel cargo test load.
    apply_state_change: Arc<Notify>,
}

impl RealCluster {
    /// Pre-allocate localhost ports, build identical `ClusterConfig`
    /// for every node (each one's `node_id` differs), then start each
    /// `Server` **sequentially**, with a `20 ms` inter-spawn delay
    /// between peers.
    ///
    /// # Why sequential and not parallel
    ///
    /// This startup is intentionally sequential — even though the
    /// helper signature is `async`, peers are spawned one at a time:
    ///
    /// * Each peer is given a beat to finish its gRPC `bind` and
    ///   start listening before the next peer wakes up and tries to
    ///   open its first heartbeat / vote RPC (see the
    ///   `tokio::time::sleep(Duration::from_millis(20))` site below).
    ///   Without this, the first-to-spawn node can lose its initial
    ///   election round when later peers' transports refuse connect
    ///   attempts that arrive before their listener is ready.
    /// * Sequential `info!()` lines per node make CI failures
    ///   readable (interleaved parallel startup logs hide the
    ///   `RealCluster node started` markers).
    /// * The port-race hazard between port allocation and bind
    ///   is solved by pre-binding every TCP listener up-front (see
    ///   `bound_listeners` below) and handing each pre-bound socket
    ///   to its server via
    ///   [`Server::start_with_state_machine_and_listener`]; the
    ///   kernel never releases a port between allocation and bind,
    ///   so sequential vs. parallel doesn't change the port story.
    pub async fn start(cfg: RealClusterConfig) -> XResult<Self> {
        assert!(cfg.size >= 1, "RealCluster needs at least one voter");
        assert!(
            cfg.election_max_ms >= cfg.election_min_ms,
            "election_max_ms ({}) must be >= election_min_ms ({})",
            cfg.election_max_ms,
            cfg.election_min_ms,
        );

        // ---- pre-allocate ports + directory ids + listeners ---------------
        // Allocating ports up-front means every node's
        // ClusterConfig.voters carries the SAME, definitive port for
        // every peer — critical because the gRPC client looks up
        // peer endpoints from `voters` (not from a separate
        // routing table).
        //
        // keep each pre-bound listener
        // ALIVE here (in `bound_listeners`) so the kernel does not
        // release the port between `pick_port_with_listener` and the
        // server's actual `start_with_state_machine_and_listener`
        // call. We pass each listener directly into the server when
        // we spawn its node, so the gRPC port is held continuously
        // from allocation through bind.
        let mut voters: Vec<VoterConfig> = Vec::with_capacity(cfg.size);
        let mut ports: Vec<(NodeId, u16)> = Vec::with_capacity(cfg.size);
        let mut bound_listeners: Vec<std::net::TcpListener> = Vec::with_capacity(cfg.size);
        for i in 1..=cfg.size as u64 {
            let (port, listener) = pick_port_with_listener();
            voters.push(VoterConfig {
                node_id: i,
                directory_id: Uuid::new_v4().to_string(),
                host: "127.0.0.1".into(),
                port,
            });
            ports.push((NodeId(i), port));
            bound_listeners.push(listener);
        }

        // ---- spawn each node sequentially ---------------------------------
        // Sequential spawn keeps log output readable; the port-race
        // hazard between port allocation and bind is now resolved by
        // handing each pre-bound listener to the server below.
        // ONE Notify shared across every node's
        // RecordingStateMachine so RealCluster::await_applied_at_least
        // wakes the instant ANY node's `apply()` fires (replacing
        // the previous 25 ms poll).
        let apply_state_change = Arc::new(Notify::new());
        let mut nodes = Vec::with_capacity(cfg.size);
        // Drain `bound_listeners` in order — each draw matches the
        // voter at the same index.
        let mut listener_iter = bound_listeners.into_iter();
        for (i, voter) in voters.iter().enumerate() {
            let node_id = NodeId(voter.node_id);
            let port = voter.port;
            let listener = listener_iter
                .next()
                .expect("bound_listeners length matches voters length");
            let tmp = TempDir::new()
                .map_err(|e| XRaftError::Storage(format!("real cluster tempdir: {e}")))?;
            let cluster = build_cluster_config(
                node_id,
                port,
                voters.clone(),
                &cfg,
                tmp.path().to_path_buf(),
            );
            let server_cfg = ServerConfig {
                cluster,
                // 127.0.0.1:0 lets the kernel assign a fresh admin
                // port per node; we don't expose it to tests because
                // the embedded API on `ServerHandle::propose` is what
                // the integration tests use.
                admin_listen_addr: Some("127.0.0.1:0".into()),
                driver_config: None,
            };

            let sm = RecordingStateMachine::with_state_change(apply_state_change.clone());
            let recording = sm.handle();
            let handle =
                Server::start_with_state_machine_and_listener(server_cfg, sm, Some(listener))
                    .await?;

            info!(
                target: "xraft_test::real",
                node_id = node_id.0,
                grpc_port = port,
                "RealCluster node started"
            );

            nodes.push(RealNode {
                node_id,
                grpc_port: port,
                recording,
                handle: Some(handle),
                _data_dir: tmp,
            });

            // Give each peer a beat to bind before launching the next
            // — keeps the leader-election race small on slow CI.
            if i + 1 < cfg.size {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        Ok(Self {
            nodes,
            ports,
            killed_handles: Vec::new(),
            apply_state_change,
        })
    }

    /// Number of nodes (alive or killed).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the cluster has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Borrow the entry for `node_id`.
    pub fn node(&self, node_id: NodeId) -> Option<&RealNode> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    /// Block until exactly one alive node reports the `Leader` role
    /// AND every other alive node ALSO reports the same `term` and
    /// `leader_id == Some(leader)`. Returns `(NodeId, term)` on
    /// success.
    ///
    /// Strict follower agreement is
    /// REQUIRED before proposing tests fire — returning as soon as
    /// one node sees itself as Leader races against follower
    /// convergence and surfaces `NotLeader { leader_hint: None }`
    /// from the first `propose` call after `await_leader`.
    pub async fn await_leader(&self, deadline: Duration) -> XResult<(NodeId, u64)> {
        let start = Instant::now();
        loop {
            let mut leader: Option<(NodeId, u64)> = None;
            let mut count = 0;
            let mut snapshots = Vec::with_capacity(self.nodes.len());
            for n in &self.nodes {
                let Some(handle) = n.handle.as_ref() else {
                    continue;
                };
                let snap = handle.status().current().await;
                if snap.role == NodeRole::Leader {
                    count += 1;
                    leader = Some((n.node_id, snap.term));
                }
                snapshots.push((n.node_id, snap));
            }
            if count == 1
                && let Some((leader_id, leader_term)) = leader
            {
                let mut all_followers_agree = true;
                for (id, snap) in &snapshots {
                    if *id == leader_id {
                        continue;
                    }
                    if snap.term != leader_term || snap.leader_id != Some(leader_id.0) {
                        all_followers_agree = false;
                        break;
                    }
                }
                if all_followers_agree {
                    return Ok((leader_id, leader_term));
                }
            }
            if start.elapsed() >= deadline {
                return Err(XRaftError::ElectionTimeout);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Borrow the current leader's [`ServerHandle`] if exactly one
    /// alive node reports the `Leader` role.
    pub async fn leader_handle(&self) -> Option<&ServerHandle> {
        let mut found: Option<&ServerHandle> = None;
        for n in &self.nodes {
            let Some(handle) = n.handle.as_ref() else {
                continue;
            };
            let snap = handle.status().current().await;
            if snap.role == NodeRole::Leader {
                if found.is_some() {
                    return None;
                }
                found = Some(handle);
            }
        }
        found
    }

    /// Id of the current unique leader, if any.
    pub async fn leader_id(&self) -> Option<NodeId> {
        let mut found: Option<NodeId> = None;
        for n in &self.nodes {
            let Some(handle) = n.handle.as_ref() else {
                continue;
            };
            let snap = handle.status().current().await;
            if snap.role == NodeRole::Leader {
                if found.is_some() {
                    return None;
                }
                found = Some(n.node_id);
            }
        }
        found
    }

    /// Submit `command` to the current leader. On `NotLeader` /
    /// "no leader" the call retries up to 5 times with a tiny back-off
    /// — useful right after an abort, where the new leader has not
    /// stepped up yet.
    pub async fn propose(&self, command: Bytes) -> XResult<xraft_core::types::LogIndex> {
        let mut attempts = 0u8;
        loop {
            attempts += 1;
            match self.leader_handle().await {
                Some(h) => match h.propose(command.clone()).await {
                    Ok(idx) => return Ok(idx),
                    Err(XRaftError::NotLeader { .. }) if attempts < 5 => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
                None if attempts < 5 => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                None => return Err(XRaftError::NotLeader { leader_hint: None }),
            }
        }
    }

    /// Wait until every alive node's recording SM has applied at
    /// least `target` entries. Returns the highest observed count on
    /// timeout for caller-side diagnostics.
    ///
    /// # EVENT-DRIVEN
    ///
    /// An earlier version slept `25 ms` between polls — a fixed
    /// wall-clock cadence that compounded under workspace-parallel
    /// scheduler pressure on slow CI (earlier reviews flagged the
    /// 25 ms loop as scheduler-dependent latency). This version wires it
    /// to the cluster-wide [`Notify`] bumped by every
    /// [`crate::state_machine::RecordingStateMachine::apply`] call on
    /// ANY node (shared via
    /// [`crate::state_machine::RecordingStateMachine::with_state_change`]
    /// at startup — see [`Self::start`]). The loop wakes the instant
    /// any follower applies an entry instead of waiting for the next
    /// 25 ms tick. A `50 ms` periodic safety-net wake (matching the
    /// [`crate::state_machine::RecordingHandle::await_applied_at_least`]
    /// constant) defends against the (theoretically eliminated by
    /// [`tokio::sync::futures::Notified::enable`]) "notify fires
    /// between predicate check and wait registration" race and bounds
    /// deadline-check granularity.
    pub async fn await_applied_at_least(
        &self,
        target: usize,
        deadline: Duration,
    ) -> std::result::Result<(), usize> {
        let start = Instant::now();
        loop {
            // Register the waiter BEFORE checking the predicate so a
            // notify fired between check and wait is not lost
            // (mirrors `SimulatedCluster::await_applied_at_least`).
            let waiter = self.apply_state_change.notified();
            tokio::pin!(waiter);
            waiter.as_mut().enable();

            let mut min_observed = usize::MAX;
            let mut max_observed: usize = 0;
            for n in &self.nodes {
                if !n.is_alive() {
                    continue;
                }
                let count = n.recording.len();
                if count < min_observed {
                    min_observed = count;
                }
                if count > max_observed {
                    max_observed = count;
                }
            }
            if min_observed == usize::MAX {
                return Err(0);
            }
            if min_observed >= target {
                return Ok(());
            }
            let elapsed = start.elapsed();
            if elapsed >= deadline {
                return Err(max_observed);
            }
            let remaining = deadline - elapsed;
            let safety_net = Duration::from_millis(50).min(remaining);
            tokio::select! {
                _ = &mut waiter => {}
                _ = tokio::time::sleep(safety_net) => {}
            }
        }
    }

    /// Fail-stop `node_id` by calling [`ServerHandle::abort`].
    /// Equivalent to a `kill -9` in production. Returns `true` when
    /// the abort actually fired (false if the node was already dead).
    ///
    /// # killed handles are PARKED, not dropped
    ///
    /// Earlier `handle.abort()`-then-drop shapes hid one failure
    /// mode: if any underlying driver / gRPC task PANICKED before
    /// the abort signal reached it, the panic message died with
    /// the dropped handle. This method instead parks the
    /// (still-aborted) handle in
    /// [`Self::killed_handles`]; [`Self::shutdown`] later calls
    /// `handle.join().await` on each parked handle and surfaces any
    /// pre-existing panic (a `Transport(...)` whose payload contains
    /// `panicked`) as a teardown failure.
    pub fn kill(&mut self, node_id: NodeId) -> bool {
        let mut taken: Option<ServerHandle> = None;
        for n in &mut self.nodes {
            if n.node_id == node_id
                && let Some(handle) = n.handle.take()
            {
                handle.abort();
                taken = Some(handle);
                break;
            }
        }
        if let Some(handle) = taken {
            // park the aborted handle so shutdown() can
            // drain it and surface pre-existing panics.
            self.killed_handles.push((node_id, handle));
            true
        } else {
            false
        }
    }

    /// Fail-stop the current leader. Returns `Some(leader_id)` when a
    /// unique leader was alive and aborted, `None` otherwise.
    pub async fn kill_leader(&mut self) -> Option<NodeId> {
        let leader = self.leader_id().await?;
        if self.kill(leader) {
            Some(leader)
        } else {
            None
        }
    }

    /// Gracefully shut down every alive node. Idempotent.
    ///
    /// # Shutdown sequence
    ///
    /// 1. [`Self::drain_alive_handles`] — graceful shutdown + join
    ///    for every node whose `handle` was still present.
    /// 2. [`Self::drain_killed_handles`] — drain (and surface any
    ///    pre-existing panic from) every [`ServerHandle`] parked
    ///    by [`Self::kill`].
    /// 3. Any failure from either step is aggregated into a single
    ///    panic at the end so a real teardown bug cannot pass
    ///    silently.
    ///
    /// # teardown errors are NOT swallowed
    ///
    /// A naive `let _ = tokio::time::timeout(5s, handle.join()).await;`
    /// for each node would silently discard EVERY join outcome
    /// (driver errors, transport bugs, task panics, shutdown
    /// deadlocks). This shutdown instead classifies every outcome via
    /// [`crate::teardown::is_allowed_teardown_noise`]:
    ///
    /// * `Ok(())` from [`ServerHandle::join`]: clean exit, ignored.
    /// * `Err(XRaftError::Storage(...))` matching the Windows tempdir
    ///   teardown race (`rename ... os error 3 | 2`): logged via
    ///   `tracing::warn` — cosmetic.
    /// * Any other `Err(XRaftError)`: aggregated as a fatal failure;
    ///   teardown panics so real shutdown bugs cannot pass silently.
    /// * Timeout after 30 s: aggregated as fatal. 30 s is a generous
    ///   ceiling — the slowest Stage 8.1 real-network test
    ///   (`real_network_five_node_leader_failover`) drains 4 alive
    ///   nodes in ≈5 s wall-clock; a 30 s budget reserves 6× headroom
    ///   while still catching a genuine deadlock. NOTE: a timeout
    ///   drops the `handle.join()` future, which leaves the underlying
    ///   driver / gRPC tasks running (server tasks have no public
    ///   abort path). The panic ensures the test author SEES the
    ///   hang; OS process exit cleans up the leaked tasks.
    ///
    /// # killed nodes are drained too
    ///
    /// Nodes killed via [`Self::kill`] PARK their (still-aborted)
    /// [`ServerHandle`] in [`Self::killed_handles`]. The drain splits
    /// into two clearly-named helpers
    /// ([`Self::drain_alive_handles`] +
    /// [`Self::drain_killed_handles`]) so any reader (or reviewer)
    /// can see at a glance that BOTH sets of handles are drained —
    /// the explicit `drain_killed_handles` call in this method body
    /// is unmistakable.
    pub async fn shutdown(mut self) {
        let mut failures: Vec<String> = Vec::new();
        self.drain_alive_handles(&mut failures).await;
        self.drain_killed_handles(&mut failures).await;
        if !failures.is_empty() {
            panic!(
                "real-network shutdown surfaced {} unexpected teardown failure(s): {}",
                failures.len(),
                failures.join("; ")
            );
        }
    }

    /// Step 1 of [`Self::shutdown`]: trigger graceful shutdown on
    /// every alive node, drain them in parallel, and append any
    /// unexpected outcome to `failures`. Running the drains in
    /// parallel keeps overall teardown bounded by the slowest
    /// node's drain instead of the sum.
    async fn drain_alive_handles(&mut self, failures: &mut Vec<String>) {
        let mut joins = Vec::new();
        for n in self.nodes.iter_mut() {
            if let Some(handle) = n.handle.take() {
                let node_id = n.node_id.0;
                handle.shutdown();
                joins.push(tokio::spawn(async move {
                    let outcome =
                        tokio::time::timeout(Duration::from_secs(30), handle.join()).await;
                    (node_id, outcome)
                }));
            }
        }
        for j in joins {
            match j.await {
                Ok((_node_id, Ok(Ok(())))) => {
                    // clean shutdown
                }
                Ok((node_id, Ok(Err(ref e)))) if crate::teardown::is_allowed_teardown_noise(e) => {
                    tracing::warn!(
                        target: "xraft_test::real",
                        node = node_id,
                        error = %e,
                        "ServerHandle::join returned allowed teardown noise \
                         (Windows tempdir race; cosmetic)"
                    );
                }
                Ok((node_id, Ok(Err(e)))) => {
                    failures.push(format!(
                        "node {node_id}: ServerHandle::join returned unexpected XRaftError: {e}"
                    ));
                }
                Ok((node_id, Err(_elapsed))) => {
                    failures.push(format!(
                        "node {node_id}: ServerHandle::join timed out after 30 s \
                         (possible shutdown deadlock; tasks may be leaked)"
                    ));
                }
                Err(je) if je.is_panic() => {
                    failures.push(format!("shutdown spawn PANICKED: {je}"));
                }
                Err(je) => {
                    failures.push(format!("shutdown spawn join error: {je}"));
                }
            }
        }
    }

    /// Step 2 of [`Self::shutdown`]: drain every parked killed
    /// [`ServerHandle`] (those moved into [`Self::killed_handles`]
    /// by [`Self::kill`]) and append any pre-existing panic or
    /// unexpected outcome to `failures`.
    ///
    /// # classify EVERY task's outcome
    ///
    /// `ServerHandle::join` returns only the FIRST error encountered
    /// across `(driver, grpc, admin)`. After [`ServerHandle::abort`]
    /// the driver task's `JoinError::is_cancelled()` surfaces first
    /// — masking a subsequent gRPC or admin task panic that would
    /// otherwise be a real bug. This helper calls the
    /// `xraft_server::ServerHandle::join_collect_all_errors()` which
    /// returns a `Vec<XRaftError>` covering ALL three tasks; the
    /// classifier below runs over EACH entry so a pre-existing
    /// gRPC/admin panic is surfaced even when the driver also
    /// reported a (tolerated) cancellation.
    ///
    /// The wrapper format
    /// `Transport("driver task join: {JoinError}")` (or
    /// `"grpc task join: ..."`, `"admin serve task join: ..."`)
    /// from `xraft-server/src/server.rs` means a panicked task
    /// surfaces with `panicked` in the message and a cancelled
    /// task with `cancelled`. The former is a real bug pushed to
    /// `failures`; the latter is expected post-abort and silently
    /// tolerated.
    ///
    /// This helper is extracted from the
    /// inline shutdown body so this drain is unmistakably visible
    /// in `shutdown()` as `self.drain_killed_handles(...).await`.
    async fn drain_killed_handles(&mut self, failures: &mut Vec<String>) {
        let mut killed_drains = Vec::new();
        for (node_id, handle) in self.killed_handles.drain(..) {
            let nid = node_id.0;
            killed_drains.push(tokio::spawn(async move {
                // drain EVERY
                // task's outcome, not just the first error. Lets
                // the classifier below surface a gRPC/admin panic
                // even when the driver-task cancellation came first.
                let outcome =
                    tokio::time::timeout(Duration::from_secs(10), handle.join_collect_all_errors())
                        .await;
                (nid, outcome)
            }));
        }
        for j in killed_drains {
            match j.await {
                Ok((nid, Ok(errors))) => {
                    // classify
                    // EACH error from EACH task individually. A
                    // benign driver `cancelled` in errors[0] does
                    // NOT mask a real panic in errors[1] (grpc)
                    // or errors[2] (admin).
                    for e in errors {
                        if crate::teardown::is_allowed_teardown_noise(&e) {
                            tracing::warn!(
                                target: "xraft_test::real",
                                node = nid,
                                error = %e,
                                "killed ServerHandle reported allowed teardown noise"
                            );
                            continue;
                        }
                        let msg = format!("{e}");
                        if msg.contains("panicked") {
                            failures.push(format!(
                                "node {nid} (killed): driver / gRPC / admin task PANICKED before abort took effect: {e}"
                            ));
                        } else if msg.contains("cancelled") {
                            // Expected: kill() called abort() on
                            // this handle; every spawned task
                            // surfaces a cancellation in turn.
                        } else {
                            failures.push(format!(
                                "node {nid} (killed): unexpected error from killed ServerHandle task: {e}"
                            ));
                        }
                    }
                }
                Ok((nid, Err(_elapsed))) => {
                    failures.push(format!(
                        "node {nid} (killed): killed ServerHandle did not resolve within 10 s \
                         (aborted tasks should resolve quickly)"
                    ));
                }
                Err(je) if je.is_panic() => {
                    failures.push(format!("killed-shutdown spawn PANICKED: {je}"));
                }
                Err(je) => {
                    failures.push(format!("killed-shutdown spawn join error: {je}"));
                }
            }
        }
    }
}

fn build_cluster_config(
    node_id: NodeId,
    grpc_port: u16,
    voters: Vec<VoterConfig>,
    cfg: &RealClusterConfig,
    data_dir: PathBuf,
) -> ClusterConfig {
    ClusterConfig {
        node_id,
        cluster_id: "real-cluster".into(),
        listen_addr: format!("127.0.0.1:{grpc_port}"),
        peers: vec![],
        voters,
        election_timeout_min_ms: cfg.election_min_ms,
        election_timeout_max_ms: cfg.election_max_ms,
        fetch_interval_ms: cfg.fetch_ms,
        tick_interval_ms: cfg.tick_ms,
        snapshot_interval: 10_000,
        max_log_entries_before_compaction: 100_000,
        data_dir,
        snapshot_retention_count: 3,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_domain_name: None,
        connect_timeout_ms: cfg.connect_timeout_ms,
        rpc_timeout_ms: cfg.rpc_timeout_ms,
        max_rpc_retries: cfg.max_rpc_retries,
        retry_initial_backoff_ms: 10,
        retry_max_backoff_ms: 50,
        max_message_size: 64 * 1024 * 1024,
        observers: vec![],
        enable_check_quorum: true,
        enable_leader_lease: false,
        check_quorum_interval_ms: None,
    }
}

/// Bind 127.0.0.1:0 and KEEP the listener alive. Returns the
/// assigned port together with the live listener so the caller can
/// hand it directly to `Server::start_with_state_machine_and_listener`.
///
/// A naive `pick_port` helper would bind, read the
/// port, and immediately drop the listener, leaving a window
/// during which another `cargo test --test-threads=N>1` worker could
/// grab the same ephemeral port before the server rebound it. By
/// keeping the listener alive and passing it into the server, the
/// kernel never releases the port between allocation and bind.
fn pick_port_with_listener() -> (u16, std::net::TcpListener) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("local_addr").port();
    (port, listener)
}

// Marker to silence unused-import warning when no real-network test
// is compiled in this build configuration.
#[allow(dead_code)]
const _ARC_USED: fn() -> Arc<()> = || Arc::new(());
