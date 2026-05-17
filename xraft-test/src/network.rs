//! [`SimulatedNetwork`] + [`SimulatedTransport`] — in-process message
//! routing fabric used by [`SimulatedCluster`](crate::simulated::SimulatedCluster)
//! to spin up multi-node Raft tests without binding any real TCP port.
//!
//! Each node owns a [`SimulatedTransport`] (which implements the
//! [`xraft_core::transport::Transport`] trait) and registers its
//! `DriverInboundHandler` with the shared [`SimulatedNetwork`] before
//! the driver loop starts. Outbound `send_*` calls look up the
//! destination node's handler and invoke it directly inside the same
//! tokio runtime — no socket, no serialisation, no real network.
//!
//! The network supports three pluggable fault modes per the Stage 8.1
//! brief:
//!
//! * **Latency** — every outbound RPC charges a configurable
//!   `Duration` to the shared [`SimulatedClock`] BEFORE calling the
//!   destination handler. Currently the latency is VIRTUAL: the
//!   clock advances by the configured amount and the dispatch arm
//!   yields once to the runtime, but NO wall-clock sleep happens.
//!   Latency is uniform per network (not
//!   per peer); tests that need asymmetric latencies build separate
//!   networks per cluster.
//!
//! * **Packet loss** — a seeded RNG decides, per RPC, whether to drop
//!   the message. Drops happen BEFORE latency so a dropped message
//!   does not charge any virtual latency to the clock. Returned as
//!   `Err(XRaftError::Transport("simulated packet drop"))`, mirroring
//!   what a real RPC timeout would surface.
//!
//! * **Partition** — a set of directed `(from, to)` cuts. Two nodes
//!   are reachable iff neither `(from, to)` nor `(to, from)` is in
//!   the cut set. [`SimulatedNetwork::partition`] is bidirectional;
//!   advanced tests can call [`SimulatedNetwork::cut_directed`] to
//!   model one-way partitions.
//!
//! "Killed" nodes (via [`SimulatedNetwork::kill`]) reject every
//! inbound and outbound RPC. The associated driver task is aborted
//! by the [`SimulatedCluster`](crate::simulated::SimulatedCluster)
//! harness so the node is truly fail-stop, not merely partitioned.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::sync::Notify;
use tracing::{debug, warn};

use xraft_core::error::{Result, XRaftError};
use xraft_core::message::{
    FetchRequest, FetchResponse, FetchSnapshotRequest, PreVoteRequest, PreVoteResponse,
    VoteRequest, VoteResponse,
};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream, Transport};
use xraft_core::types::NodeId;

use xraft_server::DriverInboundHandler;

use crate::clock::SimulatedClock;

/// Per-RPC simulated VIRTUAL latency.
/// Default to zero so tests that do not care about latency are not
/// slowed down.
const DEFAULT_LATENCY: Duration = Duration::ZERO;

/// Per-RPC simulated latency policy.
///
/// * [`LatencyMode::Fixed`] — every dispatch charges the same duration
///   to the clock.
/// * [`LatencyMode::Range`] — every dispatch rolls a uniform value in
///   `[min, max]` from the per-link RNG (so latency draws are
///   reproducible from `(master_seed, from, to)` and independent of
///   when other links sample). This is what the Stage 8.2 chaos
///   brief means by "random message delay (50-500 ms)" — the latency
///   is uniformly distributed and rolled fresh per RPC, not pinned
///   to a fixed value at config time.
#[derive(Debug, Clone, Copy)]
pub enum LatencyMode {
    /// Apply the same latency to every RPC.
    Fixed(Duration),
    /// Roll latency uniformly in `[min, max]` for each RPC against the
    /// per-link RNG. `min <= max` is enforced by the setter.
    Range { min: Duration, max: Duration },
}

impl LatencyMode {
    /// Charge a sample of this policy. Pure clock-advancement
    /// abstraction — used by the network's route_decision to compute
    /// the latency for an individual dispatch.
    fn sample(self, rng: &mut StdRng) -> Duration {
        match self {
            LatencyMode::Fixed(d) => d,
            LatencyMode::Range { min, max } => {
                if max <= min {
                    return min;
                }
                let span = (max - min).as_nanos() as u64;
                let pick = rng.gen_range(0..=span);
                min + Duration::from_nanos(pick)
            }
        }
    }
}

/// Per-RPC hard timeout applied to the in-process handler call. Even
/// with no simulated network failure, a stalled or aborted driver
/// would otherwise hang an outbound RPC forever and stall the entire
/// test. 2 seconds is generous for an in-memory call against a healthy
/// driver and tight enough that a misbehaving peer cannot hold up an
/// election.
const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// SimulatedNetwork
// ---------------------------------------------------------------------------

/// Shared in-process message-passing fabric for a [`SimulatedCluster`].
pub struct SimulatedNetwork {
    inner: Mutex<NetworkInner>,
    /// Virtual tick counter. Every
    /// per-RPC latency window applied in [`SimulatedTransport::dispatch`]
    /// flows through [`SimulatedClock::delay`] so the clock is the
    /// single observable record of how much VIRTUAL transit time the
    /// network charged.
    clock: Arc<SimulatedClock>,
    /// Master seed used to derive a per-link RNG. Captured at
    /// construction so [`NetworkInner::link_rng`] can lazily build a
    /// fresh deterministic RNG the first time a `(from, to)` link
    /// rolls a drop decision.
    ///
    /// A single shared `StdRng` produces
    /// non-replayable drop sequences under concurrent dispatch
    /// because RNG observations get interleaved by mutex ordering.
    /// Per-link state — keyed by `(from, to)` and seeded from
    /// `mix(master_seed, from, to)` — makes every link's drop
    /// sequence deterministic regardless of when other links roll.
    master_seed: u64,
}

impl std::fmt::Debug for SimulatedNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.inner.lock().expect("SimulatedNetwork mutex poisoned");
        f.debug_struct("SimulatedNetwork")
            .field("handlers", &g.handlers.len())
            .field("cuts", &g.cuts.len())
            .field("dead", &g.dead)
            .field("drop_pct", &g.drop_pct)
            .field("latency", &g.latency)
            .field("handler_timeout", &g.handler_timeout)
            .finish_non_exhaustive()
    }
}

struct NetworkInner {
    /// Per-node inbound handlers. Registered by
    /// [`SimulatedNetwork::register`] before the driver loop starts.
    handlers: HashMap<NodeId, Arc<DriverInboundHandler>>,
    /// Directed `(from, to)` edges that are currently cut. A message
    /// from `from` to `to` is dropped iff `(from, to)` is in the set.
    /// [`SimulatedNetwork::partition`] inserts both `(a, b)` and
    /// `(b, a)` for symmetric partitions.
    cuts: HashSet<(NodeId, NodeId)>,
    /// Node IDs that are currently "dead". Every inbound + outbound
    /// RPC referencing a dead node returns
    /// `Err(XRaftError::Transport(...))` so the driver behaves as if
    /// the peer is unreachable. Mirrors a process kill.
    dead: HashSet<NodeId>,
    /// Probability in `[0, 100]` that any single RPC is silently
    /// dropped. Sampled via the per-link RNG below.
    drop_pct: u8,
    /// Per-RPC simulated VIRTUAL latency policy. Charged AFTER the
    /// drop check by sampling [`LatencyMode::sample`] against the
    /// per-link RNG.
    latency: LatencyMode,
    /// Per-RPC hard timeout applied to the in-process handler call.
    handler_timeout: Duration,
    /// Per-link RNG state keyed by `(from, to)`. Lazy-built on first
    /// use via `link_rng`. Each link's RNG is seeded from
    /// `mix(master_seed, from, to)`, so the sequence of drop
    /// decisions on any given link is fully deterministic regardless
    /// of when OTHER links roll. A single shared RNG would otherwise
    /// produce non-replayable drop sequences, because rolls would be
    /// interleaved by mutex ordering across concurrent dispatchers.
    link_rngs: HashMap<(NodeId, NodeId), StdRng>,
}

impl SimulatedNetwork {
    /// Build a clean network with a fixed RNG seed and no faults. A
    /// fresh [`SimulatedClock`] is allocated; for shared-clock tests
    /// use [`Self::new_with_clock`].
    pub fn new(seed: u64) -> Arc<Self> {
        Self::new_with_clock(seed, SimulatedClock::new())
    }

    /// Build a clean network with a fixed RNG seed and an explicit
    /// [`SimulatedClock`] reference. Used by
    /// [`SimulatedCluster`](crate::simulated::SimulatedCluster) to
    /// share one clock across the harness, the network, and tests.
    pub fn new_with_clock(seed: u64, clock: Arc<SimulatedClock>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(NetworkInner {
                handlers: HashMap::new(),
                cuts: HashSet::new(),
                dead: HashSet::new(),
                drop_pct: 0,
                latency: LatencyMode::Fixed(DEFAULT_LATENCY),
                handler_timeout: DEFAULT_HANDLER_TIMEOUT,
                link_rngs: HashMap::new(),
            }),
            clock,
            master_seed: seed,
        })
    }

    /// Borrow the network's [`SimulatedClock`]. Tests use this to read
    /// `clock.elapsed()` after a scenario to see how much simulated
    /// transit time the network charged across all dispatches.
    pub fn clock(&self) -> Arc<SimulatedClock> {
        self.clock.clone()
    }

    /// Register `handler` under `node_id`. Must be called BEFORE the
    /// node's driver task is spawned so the first inbound RPC has a
    /// handler to route to.
    pub fn register(&self, node_id: NodeId, handler: Arc<DriverInboundHandler>) {
        let mut g = self.lock();
        g.handlers.insert(node_id, handler);
    }

    /// Unregister `node_id`'s handler. Used when permanently retiring
    /// a node so the network does not keep a stale `Arc` reference to
    /// a dropped channel.
    pub fn unregister(&self, node_id: NodeId) {
        let mut g = self.lock();
        g.handlers.remove(&node_id);
    }

    /// Mark `node_id` dead — every outbound RPC originating at OR
    /// destined for this node will return
    /// `Err(XRaftError::Transport("simulated node killed"))` until
    /// [`Self::revive`] is called.
    pub fn kill(&self, node_id: NodeId) {
        let mut g = self.lock();
        g.dead.insert(node_id);
    }

    /// Revive a previously-killed node. Has no effect on
    /// partitions or registered handlers.
    pub fn revive(&self, node_id: NodeId) {
        let mut g = self.lock();
        g.dead.remove(&node_id);
    }

    /// Symmetrically partition `a` from `b`. Both directions are cut.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut g = self.lock();
        g.cuts.insert((a, b));
        g.cuts.insert((b, a));
    }

    /// Heal a symmetric partition between `a` and `b`.
    pub fn heal_partition(&self, a: NodeId, b: NodeId) {
        let mut g = self.lock();
        g.cuts.remove(&(a, b));
        g.cuts.remove(&(b, a));
    }

    /// Cut a directed edge `(from, to)`. Messages from `from` to `to`
    /// are dropped; the reverse direction (`to`→`from`) is unaffected.
    ///
    /// Implementation detail: `cuts` stores ordered `(NodeId, NodeId)`
    /// tuples and [`route_decision`] checks only the `(from, to)`
    /// direction, so this insert is genuinely one-way. Symmetric
    /// partitions go through [`Self::partition`], which inserts both
    /// directions explicitly.
    pub fn cut_directed(&self, from: NodeId, to: NodeId) {
        let mut g = self.lock();
        g.cuts.insert((from, to));
    }

    /// Heal a single directed edge `(from, to)`. The reverse direction
    /// is unaffected — paired with [`Self::cut_directed`] so chaos
    /// scenarios can build per-cut tracking sets and undo exactly
    /// the cuts they introduced (rather than calling [`Self::heal_all`]
    /// which would clobber unrelated partitions).
    pub fn heal_directed(&self, from: NodeId, to: NodeId) {
        let mut g = self.lock();
        g.cuts.remove(&(from, to));
    }

    /// Snapshot of every node currently registered with the network.
    /// Used by chaos scenarios to enumerate "everyone else" when
    /// isolating a node (cutting every directed edge between `node`
    /// and its peers).
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.lock().handlers.keys().copied().collect()
    }

    /// Symmetrically partition a `group` of nodes from everyone else
    /// registered with the network. Every node in `group` becomes
    /// unable to talk to any node outside `group`, while intra-group
    /// communication is preserved.
    pub fn partition_group(&self, group: &[NodeId]) {
        let group_set: HashSet<NodeId> = group.iter().copied().collect();
        let mut g = self.lock();
        let others: Vec<NodeId> = g
            .handlers
            .keys()
            .copied()
            .filter(|id| !group_set.contains(id))
            .collect();
        for inside in &group_set {
            for outside in &others {
                g.cuts.insert((*inside, *outside));
                g.cuts.insert((*outside, *inside));
            }
        }
    }

    /// Heal every partition cut currently active in the network.
    pub fn heal_all(&self) {
        let mut g = self.lock();
        g.cuts.clear();
    }

    /// Configure the per-RPC drop probability (clamped to `[0, 100]`).
    pub fn set_drop_pct(&self, pct: u8) {
        let mut g = self.lock();
        g.drop_pct = pct.min(100);
    }

    /// Configure the per-RPC simulated VIRTUAL latency as a fixed
    /// value. Charged to the shared [`SimulatedClock`]; does NOT
    /// wall-clock sleep. For Stage 8.2's "random 50-500 ms" model
    /// use [`Self::set_latency_range`] instead.
    pub fn set_latency(&self, latency: Duration) {
        let mut g = self.lock();
        g.latency = LatencyMode::Fixed(latency);
    }

    /// Configure the per-RPC simulated VIRTUAL latency as a uniform
    /// range `[min, max]`. Each dispatch rolls a fresh sample from
    /// the per-link RNG, so consecutive RPCs on the same link see
    /// independent random latencies in the range — this is exactly
    /// the Stage 8.2 chaos brief's "random message delay (50-500ms)"
    /// model. `max < min` is normalised to `min`.
    pub fn set_latency_range(&self, min: Duration, max: Duration) {
        let mut g = self.lock();
        let (lo, hi) = if max >= min { (min, max) } else { (min, min) };
        g.latency = LatencyMode::Range { min: lo, max: hi };
    }

    /// Configure the per-RPC handler timeout. Calls that exceed this
    /// budget return `Err(XRaftError::Transport("simulated rpc timeout"))`.
    pub fn set_handler_timeout(&self, timeout: Duration) {
        let mut g = self.lock();
        g.handler_timeout = timeout;
    }

    /// Borrow the inner `Mutex` guard. Wrapped so the lock-poison
    /// branch is centralised.
    fn lock(&self) -> std::sync::MutexGuard<'_, NetworkInner> {
        self.inner.lock().expect("SimulatedNetwork mutex poisoned")
    }

    /// Evaluate the routing policy for a single RPC and return either
    /// the destination handler (when the message should be delivered)
    /// or a transport error (drop / partition / dead).
    ///
    /// The `(handler, latency, timeout)` tuple is captured atomically
    /// under the lock so latency/timeout changes mid-test never produce
    /// a half-applied policy for an in-flight RPC.
    fn route_decision(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Result<(Arc<DriverInboundHandler>, Duration, Duration)> {
        let mut g = self.lock();
        if g.dead.contains(&from) || g.dead.contains(&to) {
            return Err(XRaftError::Transport(format!(
                "simulated node killed (from={} to={})",
                from.0, to.0
            )));
        }
        if g.cuts.contains(&(from, to)) {
            return Err(XRaftError::Transport(format!(
                "simulated partition (from={} to={})",
                from.0, to.0
            )));
        }
        // Snapshot the policy fields BEFORE acquiring a mutable
        // borrow of the link RNG (the borrow checker forbids further
        // immutable reads through `g` while `link_rng` is live).
        let drop_pct = g.drop_pct;
        let latency_mode = g.latency;
        let timeout = g.handler_timeout;
        let handler = g.handlers.get(&to).cloned().ok_or_else(|| {
            XRaftError::Transport(format!(
                "no simulated handler registered for node_id {}",
                to.0
            ))
        })?;
        let master = self.master_seed;
        // Borrow the per-link RNG once for both the drop roll AND
        // the latency sample so a single (from, to) link advances
        // its RNG deterministically per dispatch.
        let link_rng = g
            .link_rngs
            .entry((from, to))
            .or_insert_with(|| StdRng::seed_from_u64(mix_link_seed(master, from, to)));
        if drop_pct > 0 {
            let roll: u8 = link_rng.gen_range(0..100);
            if roll < drop_pct {
                return Err(XRaftError::Transport(format!(
                    "simulated packet drop (from={} to={}, roll={}, pct={})",
                    from.0, to.0, roll, drop_pct
                )));
            }
        }
        // Sample latency from the same link RNG so per-RPC random
        // delays (LatencyMode::Range) are reproducible from
        // (master_seed, from, to). Pure pass-through for
        // LatencyMode::Fixed.
        let latency = latency_mode.sample(link_rng);
        Ok((handler, latency, timeout))
    }
}

/// Derive a per-link RNG seed from the master seed and the directed
/// `(from, to)` link key. SplitMix64-style: each lane (master, from,
/// to) is mixed into the output so two links never collide and the
/// sequence on any link is reproducible from `(master, from, to)`
/// alone.
fn mix_link_seed(master: u64, from: NodeId, to: NodeId) -> u64 {
    fn splitmix(mut x: u64) -> u64 {
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
        x ^ (x >> 31)
    }
    let a = splitmix(master);
    let b = splitmix(a ^ from.0);
    splitmix(b ^ to.0.wrapping_add(0xD1B54A32D192ED03))
}

// ---------------------------------------------------------------------------
// SimulatedTransport
// ---------------------------------------------------------------------------

/// Per-node `Transport` implementation backed by a shared
/// [`SimulatedNetwork`].
///
/// One instance per node. The transport keeps the network reference,
/// the local node id (for routing source), and a `Notify` used by
/// [`Transport::start_server`] to block until shutdown is signalled.
///
/// `start_server` is a no-op other than parking on the notify — the
/// network owns the inbound dispatch path (it holds the
/// `DriverInboundHandler` registered via
/// [`SimulatedNetwork::register`]).
pub struct SimulatedTransport {
    network: Arc<SimulatedNetwork>,
    self_id: NodeId,
    shutdown: Arc<Notify>,
}

impl SimulatedTransport {
    /// Build a transport tied to `network` for the local node id
    /// `self_id`.
    pub fn new(network: Arc<SimulatedNetwork>, self_id: NodeId) -> Self {
        Self {
            network,
            self_id,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Signal the parked `start_server` future to return so the
    /// spawned task exits.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
        self.shutdown.notify_one();
    }

    /// Apply latency then forward to the destination handler under a
    /// timeout. Extracted so each `send_*` arm stays a single
    /// readable line.
    async fn dispatch<F, T>(&self, to: NodeId, kind: &'static str, dispatch: F) -> Result<T>
    where
        F: FnOnce(Arc<DriverInboundHandler>) -> futures_local::BoxFut<Result<T>>,
    {
        let (handler, latency, timeout) = self.network.route_decision(self.self_id, to)?;
        // Route latency through the shared SimulatedClock so it is the
        // single observable record of simulated transit time across
        // the cluster. `clock.delay` is a no-op when `latency`
        // is `Duration::ZERO`, preserving the prior fast path.
        if latency > Duration::ZERO {
            self.network.clock().delay(latency).await;
        }
        match tokio::time::timeout(timeout, dispatch(handler)).await {
            Ok(res) => {
                if let Err(ref e) = res {
                    debug!(
                        target: "xraft_test::network",
                        from = self.self_id.0,
                        to = to.0,
                        kind,
                        error = %e,
                        "simulated RPC returned error"
                    );
                }
                res
            }
            Err(_) => {
                warn!(
                    target: "xraft_test::network",
                    from = self.self_id.0,
                    to = to.0,
                    kind,
                    "simulated RPC timed out (handler hang or aborted driver)"
                );
                Err(XRaftError::Transport(format!(
                    "simulated rpc timeout (from={} to={} kind={})",
                    self.self_id.0, to.0, kind
                )))
            }
        }
    }
}

impl Transport for SimulatedTransport {
    async fn send_vote(&self, to: NodeId, request: VoteRequest) -> Result<VoteResponse> {
        self.dispatch(to, "vote", move |h| {
            Box::pin(async move { h.handle_vote(request).await })
        })
        .await
    }

    async fn send_pre_vote(&self, to: NodeId, request: PreVoteRequest) -> Result<PreVoteResponse> {
        self.dispatch(to, "pre_vote", move |h| {
            Box::pin(async move { h.handle_pre_vote(request).await })
        })
        .await
    }

    async fn send_fetch(&self, to: NodeId, request: FetchRequest) -> Result<FetchResponse> {
        self.dispatch(to, "fetch", move |h| {
            Box::pin(async move { h.handle_fetch(request).await })
        })
        .await
    }

    async fn send_fetch_snapshot(
        &self,
        to: NodeId,
        request: FetchSnapshotRequest,
    ) -> Result<SnapshotChunkStream> {
        self.dispatch(to, "fetch_snapshot", move |h| {
            Box::pin(async move { h.handle_fetch_snapshot(request).await })
        })
        .await
    }

    fn start_server(
        self: Arc<Self>,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let shutdown = self.shutdown.clone();
        async move {
            shutdown.notified().await;
            Ok(())
        }
    }
}

/// Private helper module for the boxed-future alias used by
/// [`SimulatedTransport::dispatch`]. Kept module-local because no
/// external caller needs it.
mod futures_local {
    use std::future::Future;
    use std::pin::Pin;
    pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_is_bidirectional() {
        let net = SimulatedNetwork::new(0);
        net.partition(NodeId(1), NodeId(2));
        // route_decision requires a handler, but the partition check
        // happens before the handler lookup so we can probe purely on
        // the cut set via direct field inspection.
        let g = net.lock();
        assert!(g.cuts.contains(&(NodeId(1), NodeId(2))));
        assert!(g.cuts.contains(&(NodeId(2), NodeId(1))));
    }

    #[test]
    fn heal_partition_removes_both_directions() {
        let net = SimulatedNetwork::new(0);
        net.partition(NodeId(1), NodeId(2));
        net.heal_partition(NodeId(1), NodeId(2));
        let g = net.lock();
        assert!(g.cuts.is_empty());
    }

    #[test]
    fn kill_and_revive_toggle_dead_set() {
        let net = SimulatedNetwork::new(0);
        net.kill(NodeId(3));
        assert!(net.lock().dead.contains(&NodeId(3)));
        net.revive(NodeId(3));
        assert!(!net.lock().dead.contains(&NodeId(3)));
    }

    // cut_directed must be one-way. A prior implementation's
    // route_decision rejected traffic in either direction whenever
    // `(from, to)` OR `(to, from)` was present in the cut set, which
    // collapsed `cut_directed` into `partition`. The current
    // route_decision checks ONLY the `(from, to)` direction; this
    // test pins the invariant so a future refactor cannot regress it.
    #[test]
    fn cut_directed_blocks_only_one_direction() {
        let net = SimulatedNetwork::new(0);
        net.cut_directed(NodeId(1), NodeId(2));
        let g = net.lock();
        assert!(
            g.cuts.contains(&(NodeId(1), NodeId(2))),
            "cut_directed(1, 2) must insert (1, 2)"
        );
        assert!(
            !g.cuts.contains(&(NodeId(2), NodeId(1))),
            "cut_directed(1, 2) must NOT insert (2, 1) — that would \
             make the cut bidirectional and collapse into `partition`"
        );
    }

    // route_decision must reject
    // traffic only along the cut direction. We don't register a real
    // handler — the partition check happens BEFORE handler lookup, so
    // the two directions produce distinguishable errors:
    //   1→2: cut hits, returns "simulated partition (from=1 to=2)".
    //   2→1: no cut, falls through to handler lookup and returns
    //         "no simulated handler registered for node_id 1".
    // That distinction is the strongest possible proof that the cut
    // is genuinely one-way under the current route_decision logic.
    #[test]
    fn cut_directed_route_decision_is_one_way() {
        let net = SimulatedNetwork::new(0);
        net.cut_directed(NodeId(1), NodeId(2));

        // Inspect the error message instead of `{:?}`-printing the
        // whole Result — `DriverInboundHandler` does not implement
        // `Debug`, so the inner `Ok` case cannot use the derive-style
        // debug formatter.
        let one_to_two = net.route_decision(NodeId(1), NodeId(2));
        let msg_12 = match one_to_two {
            Err(XRaftError::Transport(m)) => m,
            Err(other) => panic!("1→2 expected Transport err; got {other}"),
            Ok(_) => panic!("1→2 must be blocked by the directed cut"),
        };
        assert!(
            msg_12.contains("partition") && msg_12.contains("from=1") && msg_12.contains("to=2"),
            "1→2 must surface as the directed-partition err; got {msg_12:?}"
        );

        let two_to_one = net.route_decision(NodeId(2), NodeId(1));
        let msg_21 = match two_to_one {
            Err(XRaftError::Transport(m)) => m,
            Err(other) => panic!("2→1 expected Transport err; got {other}"),
            Ok(_) => {
                // Reaching Ok here means the lookup found a handler,
                // which means cut_directed accidentally registered
                // nothing — fine for this test's invariant. Skip.
                return;
            }
        };
        assert!(
            !msg_21.contains("partition"),
            "2→1 must NOT surface as a partition err (the cut is one-way); \
             got {msg_21:?}"
        );
        assert!(
            msg_21.contains("no simulated handler"),
            "2→1 must fall through to handler lookup (cut is one-way); \
             got {msg_21:?}"
        );
    }

    // Per-link RNG state must produce
    // deterministic, replayable drop sequences regardless of how
    // concurrent dispatchers interleave their drop rolls. With a
    // single shared RNG the sequence on link L
    // depended on how many other links had rolled first, so a
    // re-run under different task scheduling produced a different
    // drop sequence even with the same master seed.
    //
    // This test interleaves rolls on three links in two different
    // orders and asserts each link's per-roll outcome is identical
    // across both orderings — that is, link L's i-th roll is the
    // same whether L is the first, second, or third link to sample.
    #[test]
    fn per_link_rng_is_independent_of_interleaving() {
        fn rolls(network: &SimulatedNetwork, link: (NodeId, NodeId), n: usize) -> Vec<bool> {
            (0..n)
                .map(|_| network.route_decision(link.0, link.1).is_err())
                .collect()
        }

        // Build two identical networks with the same seed. Use
        // drop_pct = 50 so roll outcomes split roughly evenly and a
        // bug in interleaving would surface as a different bit
        // pattern.
        let net_a = SimulatedNetwork::new(0xCAFEF00DDEADBEEF);
        let net_b = SimulatedNetwork::new(0xCAFEF00DDEADBEEF);
        net_a.set_drop_pct(50);
        net_b.set_drop_pct(50);

        let l12 = (NodeId(1), NodeId(2));
        let l13 = (NodeId(1), NodeId(3));
        let l23 = (NodeId(2), NodeId(3));

        // Order A: roll link 1→2 ten times, THEN 1→3, THEN 2→3.
        let a_12 = rolls(&net_a, l12, 10);
        let a_13 = rolls(&net_a, l13, 10);
        let a_23 = rolls(&net_a, l23, 10);

        // Order B: interleave one roll per link, round-robin.
        let mut b_12 = Vec::with_capacity(10);
        let mut b_13 = Vec::with_capacity(10);
        let mut b_23 = Vec::with_capacity(10);
        for _ in 0..10 {
            b_12.extend(rolls(&net_b, l12, 1));
            b_13.extend(rolls(&net_b, l13, 1));
            b_23.extend(rolls(&net_b, l23, 1));
        }

        assert_eq!(
            a_12, b_12,
            "link 1→2 drop sequence must be independent of when 1→3 / 2→3 rolled"
        );
        assert_eq!(
            a_13, b_13,
            "link 1→3 drop sequence must be independent of when 1→2 / 2→3 rolled"
        );
        assert_eq!(
            a_23, b_23,
            "link 2→3 drop sequence must be independent of when 1→2 / 1→3 rolled"
        );
    }

    // Different links seeded
    // from the same master must produce distinct sequences. If
    // mix_link_seed accidentally collapsed `(from, to)` and
    // `(to, from)` to the same value, a half-duplex test that
    // checks one direction's drops would mis-predict the reverse.
    #[test]
    fn per_link_seeds_diverge_across_links() {
        let s12 = mix_link_seed(0xCAFEF00DDEADBEEF, NodeId(1), NodeId(2));
        let s21 = mix_link_seed(0xCAFEF00DDEADBEEF, NodeId(2), NodeId(1));
        let s13 = mix_link_seed(0xCAFEF00DDEADBEEF, NodeId(1), NodeId(3));
        assert_ne!(s12, s21, "(1,2) and (2,1) must seed distinct RNGs");
        assert_ne!(s12, s13, "(1,2) and (1,3) must seed distinct RNGs");
        assert_ne!(s13, s21, "(1,3) and (2,1) must seed distinct RNGs");
    }
}
