//! Server driver — Stage 4.2 Message Router and Driver Loop.
//!
//! The [`Driver`] owns a single [`RaftNode`](xraft_core::RaftNode) plus
//! pluggable [`LogStore`](xraft_core::storage::LogStore),
//! [`HardStateStore`](xraft_core::storage::HardStateStore),
//! [`SnapshotStore`](xraft_core::storage::SnapshotStore),
//! [`StateMachine`](xraft_core::state_machine::StateMachine),
//! and [`Transport`](xraft_core::transport::Transport) backends. Its
//! event loop pumps inputs from five sources via `tokio::select!`:
//!
//! 1. inbound RPCs from the gRPC server (via [`DriverHandle::inbound_handler`]),
//! 2. outbound RPC results from spawned client tasks,
//! 3. client commands submitted via [`DriverHandle::propose`],
//! 4. a tick timer driven by `tokio::time::interval`,
//! 5. a shutdown signal.
//!
//! Each event ultimately becomes an [`Input`] fed into
//! [`RaftNode::step`](xraft_core::RaftNode::step); the returned
//! [`Action`](xraft_core::message::Action) list is then processed in
//! order, with persistence honoured **before** any reply leaves the box
//! (Raft safety invariant — see `handle_vote_request` doc comment in
//! `xraft-core`).
//!
//! ## `MessageRouter`
//!
//! [`MessageRouter`] is the thin outbound-dispatch shim that converts
//! [`Action::SendMessage`](xraft_core::message::Action::SendMessage) into a
//! `tokio::spawn`-ed call against the [`Transport`] trait. Each spawned
//! task forwards its result back to the driver via the outbound-result
//! channel so the corresponding [`Input::*Response`](xraft_core::message::Input)
//! re-enters the loop.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Interval, MissedTickBehavior, interval};
use tracing::{debug, error, info, warn};

use xraft_core::RaftNode;
use xraft_core::error::{Result as XResult, XRaftError};
use xraft_core::message::{
    Action, DivergingEpoch, Entry, EntryPayload, FetchRequest, FetchResponse, FetchSnapshotChunk,
    FetchSnapshotRequest, Input, OutboundMessage, PreVoteRequest, PreVoteResponse, VoteRequest,
    VoteResponse,
};
use xraft_core::node::PeerState;
use xraft_core::state_machine::StateMachine;
use xraft_core::storage::{HardStateStore, LogStore, SnapshotMeta, SnapshotStore};
use xraft_core::transport::{RaftMessageHandler, SnapshotChunkStream, Transport};
use xraft_core::types::{LogIndex, NodeId, NodeRole, Term};

// ---------------------------------------------------------------------------
// Public events / handles
// ---------------------------------------------------------------------------

/// Error returned by [`DriverHandle::propose`] when the driver has shut
/// down before the command channel could deliver the request.
const PROPOSE_CHANNEL_CLOSED: &str = "driver event channel closed";

/// Channel capacity for the driver's internal event mpsc.
///
/// Sized to keep a steady-state RPC + tick burst from blocking the
/// inbound handler. Bounded so a runaway producer cannot exhaust
/// memory: under sustained backpressure the inbound handlers stall,
/// which propagates back to the gRPC server (and thus the client).
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for the outbound-result mpsc.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Reply for an inbound RPC: either a typed response or an `XRaftError`.
///
/// `Vote`/`PreVote`/`Fetch` always emit a response — when the node drops
/// the request silently (foreign cluster, unknown candidate, …) the
/// driver synthesises a default-deny reply so the gRPC contract still
/// returns a value rather than hanging the client.
pub type VoteReply = oneshot::Sender<XResult<VoteResponse>>;
/// PreVote reply oneshot.
pub type PreVoteReply = oneshot::Sender<XResult<PreVoteResponse>>;
/// Fetch reply oneshot.
pub type FetchReply = oneshot::Sender<XResult<FetchResponse>>;
/// Fetch-snapshot stream reply oneshot.
pub type FetchSnapshotReply = oneshot::Sender<XResult<SnapshotChunkStream>>;

/// Inbound RPC delivered from the gRPC server to the driver loop.
pub enum InboundRpc {
    /// `VoteRequest` from a peer (a real-election grant request).
    Vote {
        /// The parsed RPC payload.
        req: VoteRequest,
        /// Oneshot the driver uses to return the response.
        reply: VoteReply,
    },
    /// `PreVoteRequest` from a peer.
    PreVote {
        /// The parsed RPC payload.
        req: PreVoteRequest,
        /// Oneshot the driver uses to return the response.
        reply: PreVoteReply,
    },
    /// `FetchRequest` from a follower / observer (pull replication).
    Fetch {
        /// The parsed RPC payload.
        req: FetchRequest,
        /// Oneshot the driver uses to return the response.
        reply: FetchReply,
    },
    /// `FetchSnapshotRequest` from a peer; the driver reads the chunk
    /// stream out of the local [`SnapshotStore`] and replies via the
    /// returned [`SnapshotChunkStream`]. The first chunk carries
    /// `SnapshotMeta`; the final chunk has `done = true`.
    FetchSnapshot {
        /// The parsed RPC payload.
        req: FetchSnapshotRequest,
        /// Oneshot the driver uses to return the streaming response.
        reply: FetchSnapshotReply,
    },
}

/// Result of an outbound RPC dispatched by [`MessageRouter`].
#[derive(Debug)]
pub enum OutboundResult {
    /// Successful `VoteResponse` from `peer`.
    Vote {
        /// Peer node id that produced the response.
        peer: NodeId,
        /// The response payload.
        response: VoteResponse,
    },
    /// Successful `PreVoteResponse` from `peer`.
    PreVote {
        /// Peer node id that produced the response.
        peer: NodeId,
        /// The response payload.
        response: PreVoteResponse,
    },
    /// Successful `FetchResponse` from the leader peer.
    Fetch {
        /// Peer node id that produced the response.
        peer: NodeId,
        /// The response payload.
        response: FetchResponse,
    },
    /// `FetchSnapshot` stream completed cleanly (a final chunk with
    /// `done == true` was observed).
    ///
    /// Stage 4.2 does not yet feed chunks back into `RaftNode::step` —
    /// there is no `Input::SnapshotChunk` variant. The driver collects
    /// the stream's chunks to verify the transport completed and logs
    /// the counts; downstream snapshot install lands in Phase 5.
    ///
    /// Streams that end WITHOUT a final `done = true` chunk are
    /// surfaced as [`OutboundResult::Error`] (kind `"fetch_snapshot"`)
    /// — the `FetchSnapshot` variant is reserved for clean completions
    /// only and therefore `completed` is always `true` when this
    /// variant is observed (the field is retained for backwards
    /// compatibility with any future incremental-chunk consumer).
    FetchSnapshot {
        /// Peer node id that produced the stream.
        peer: NodeId,
        /// Number of chunks received from the stream.
        chunk_count: u64,
        /// True iff the stream terminated with a final chunk
        /// (`done == true`). Currently always `true` in this variant.
        completed: bool,
    },
    /// An outbound RPC failed; nothing is fed back into the node — the
    /// next tick will trigger a retry via the standard PreVote / Fetch
    /// re-issue logic.
    Error {
        /// Peer node id that was the target of the failed RPC.
        peer: NodeId,
        /// Short tag describing which RPC kind failed (`vote`, `pre_vote`,
        /// `fetch`, …) — used for tracing only.
        kind: &'static str,
        /// Display-formatted error.
        err: String,
    },
}

/// Client command submitted via [`DriverHandle::propose`].
struct ClientCommand {
    command: Bytes,
    /// Resolved with the committed `LogIndex` on success, or an error if
    /// this node is not the leader / the driver shuts down before commit.
    reply: oneshot::Sender<XResult<LogIndex>>,
}

/// Unified event consumed by the driver loop.
enum DriverEvent {
    Inbound(InboundRpc),
    Client(ClientCommand),
}

/// Clone-able handle exposing the driver's public API.
///
/// Holds:
/// - the event-channel sender used by the inbound RPC handler,
/// - the client-command-channel sender used by [`propose`](Self::propose),
/// - the shutdown signal sender used by [`shutdown`](Self::shutdown).
#[derive(Clone)]
pub struct DriverHandle {
    events: mpsc::Sender<DriverEvent>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl DriverHandle {
    /// Submit a client command to the driver and await its commit.
    ///
    /// Returns the assigned `LogIndex` once the entry is committed and
    /// applied to the state machine, or an `XRaftError` if:
    /// - this node is not the leader at submission time
    ///   ([`XRaftError::NotLeader`] carrying the current `leader_id` hint),
    /// - the driver shuts down before the entry commits
    ///   ([`XRaftError::Shutdown`]),
    /// - the underlying storage append fails ([`XRaftError::Storage`]).
    pub async fn propose(&self, command: Bytes) -> XResult<LogIndex> {
        let (tx, rx) = oneshot::channel();
        let cmd = ClientCommand { command, reply: tx };
        self.events
            .send(DriverEvent::Client(cmd))
            .await
            .map_err(|_| XRaftError::Transport(PROPOSE_CHANNEL_CLOSED.to_string()))?;
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(XRaftError::Shutdown),
        }
    }

    /// Trigger a graceful shutdown of the driver loop. Returns
    /// immediately; the loop drains in-flight work and returns from
    /// [`Driver::run`].
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
        // notify_one stashes a permit so a notifier that fires before
        // the loop awaits `notified()` still wakes the loop on its
        // first poll.
        self.shutdown.notify_one();
    }

    /// Build an inbound RPC handler for the gRPC server. The handler
    /// implements [`RaftMessageHandler`] by forwarding every RPC into
    /// the driver's event channel and awaiting the reply.
    pub fn inbound_handler(&self) -> DriverInboundHandler {
        DriverInboundHandler {
            events: self.events.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound handler — implements RaftMessageHandler
// ---------------------------------------------------------------------------

/// Implements [`RaftMessageHandler`] on top of the driver's event channel.
///
/// Each RPC sends an [`InboundRpc`] event with a oneshot reply, then
/// awaits the reply. If the driver has shut down or the event channel
/// is full and times out, the handler returns
/// [`XRaftError::Transport`].
#[derive(Clone)]
pub struct DriverInboundHandler {
    events: mpsc::Sender<DriverEvent>,
}

impl RaftMessageHandler for DriverInboundHandler {
    fn handle_vote(
        &self,
        request: VoteRequest,
    ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
        let events = self.events.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            events
                .send(DriverEvent::Inbound(InboundRpc::Vote {
                    req: request,
                    reply: tx,
                }))
                .await
                .map_err(|_| XRaftError::Transport("driver event channel closed".into()))?;
            rx.await
                .map_err(|_| XRaftError::Transport("driver dropped reply".into()))?
        }
    }

    fn handle_pre_vote(
        &self,
        request: PreVoteRequest,
    ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
        let events = self.events.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            events
                .send(DriverEvent::Inbound(InboundRpc::PreVote {
                    req: request,
                    reply: tx,
                }))
                .await
                .map_err(|_| XRaftError::Transport("driver event channel closed".into()))?;
            rx.await
                .map_err(|_| XRaftError::Transport("driver dropped reply".into()))?
        }
    }

    fn handle_fetch(
        &self,
        request: FetchRequest,
    ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
        let events = self.events.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            events
                .send(DriverEvent::Inbound(InboundRpc::Fetch {
                    req: request,
                    reply: tx,
                }))
                .await
                .map_err(|_| XRaftError::Transport("driver event channel closed".into()))?;
            rx.await
                .map_err(|_| XRaftError::Transport("driver dropped reply".into()))?
        }
    }

    fn handle_fetch_snapshot(
        &self,
        request: FetchSnapshotRequest,
    ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
        let events = self.events.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            events
                .send(DriverEvent::Inbound(InboundRpc::FetchSnapshot {
                    req: request,
                    reply: tx,
                }))
                .await
                .map_err(|_| XRaftError::Transport("driver event channel closed".into()))?;
            rx.await
                .map_err(|_| XRaftError::Transport("driver dropped reply".into()))?
        }
    }
}

// ---------------------------------------------------------------------------
// MessageRouter — outbound dispatch
// ---------------------------------------------------------------------------

/// Outbound-message dispatcher. Owns the transport handle and the
/// outbound-result channel; spawns one tokio task per `SendMessage`
/// action and forwards the response back to the driver loop as an
/// [`OutboundResult`].
///
/// Outbound `FetchSnapshotRequest` is dispatched via
/// [`Transport::send_fetch_snapshot`]; the returned chunk stream is
/// drained and an [`OutboundResult::FetchSnapshot`] summarising the
/// chunk count and completion flag is produced. Stage 4.2 does not yet
/// feed individual chunks back into [`RaftNode::step`] — there is no
/// `Input::SnapshotChunk` variant — so the actual snapshot install
/// state-machine wiring lands in Phase 5. This Stage's responsibility
/// is the message routing surface itself: every outbound RPC kind
/// reaches the wire and every transport-level error is surfaced as
/// [`OutboundResult::Error`].
pub struct MessageRouter<T: Transport + Send + Sync + 'static> {
    transport: Arc<T>,
    /// Channel back to the driver loop (responses are surfaced as
    /// `OutboundResult` events).
    tx: mpsc::Sender<OutboundResult>,
    /// In-flight outbound RPCs; the driver reaps completed handles
    /// inside the `select!` loop so they do not accumulate.
    tasks: JoinSet<()>,
    /// Wall-clock deadline applied to the FetchSnapshot stream drain
    /// loop. A slow or malicious peer that keeps the stream open with
    /// non-`done` chunks would otherwise pin a `JoinSet` slot until
    /// shutdown `abort_all_now`; the deadline converts that hang into
    /// an `OutboundResult::Error { kind: "fetch_snapshot" }` so the
    /// task surfaces and forward progress is preserved.
    fetch_snapshot_deadline: Duration,
}

impl<T: Transport + Send + Sync + 'static> MessageRouter<T> {
    /// Default fetch-snapshot drain deadline used by
    /// [`MessageRouter::new`]. Chosen generously (30 s) so legitimate
    /// large snapshot transfers complete without truncation while
    /// still bounding the JoinSet slot a misbehaving peer can occupy.
    /// `Driver::new` always overrides this via
    /// [`DriverConfig::fetch_snapshot_deadline`].
    pub const DEFAULT_FETCH_SNAPSHOT_DEADLINE: Duration = Duration::from_secs(30);

    /// Construct a new `MessageRouter` over the given transport, using
    /// the default [`MessageRouter::DEFAULT_FETCH_SNAPSHOT_DEADLINE`]
    /// for the FetchSnapshot drain timeout. Production code goes
    /// through `Driver::new`, which threads the deadline from
    /// [`DriverConfig`] via
    /// [`MessageRouter::new_with_fetch_snapshot_deadline`].
    pub fn new(transport: Arc<T>, tx: mpsc::Sender<OutboundResult>) -> Self {
        Self::new_with_fetch_snapshot_deadline(transport, tx, Self::DEFAULT_FETCH_SNAPSHOT_DEADLINE)
    }

    /// Construct a new `MessageRouter` with an explicit
    /// `fetch_snapshot_deadline`. Used by `Driver::new` to thread the
    /// configured deadline through, and by router-level unit tests
    /// that exercise the timeout path.
    pub fn new_with_fetch_snapshot_deadline(
        transport: Arc<T>,
        tx: mpsc::Sender<OutboundResult>,
        fetch_snapshot_deadline: Duration,
    ) -> Self {
        Self {
            transport,
            tx,
            tasks: JoinSet::new(),
            fetch_snapshot_deadline,
        }
    }

    /// Dispatch a single outbound message to `peer`. Spawns the RPC on
    /// the router's `JoinSet` so the driver can reap it without blocking.
    pub fn dispatch(&mut self, peer: NodeId, message: OutboundMessage) {
        let transport = self.transport.clone();
        let tx = self.tx.clone();
        match message {
            OutboundMessage::VoteRequest(req) => {
                self.tasks.spawn(async move {
                    let result = transport.send_vote(peer, req).await;
                    let out = match result {
                        Ok(resp) => OutboundResult::Vote {
                            peer,
                            response: resp,
                        },
                        Err(e) => OutboundResult::Error {
                            peer,
                            kind: "vote",
                            err: e.to_string(),
                        },
                    };
                    let _ = tx.send(out).await;
                });
            }
            OutboundMessage::PreVoteRequest(req) => {
                self.tasks.spawn(async move {
                    let result = transport.send_pre_vote(peer, req).await;
                    let out = match result {
                        Ok(resp) => OutboundResult::PreVote {
                            peer,
                            response: resp,
                        },
                        Err(e) => OutboundResult::Error {
                            peer,
                            kind: "pre_vote",
                            err: e.to_string(),
                        },
                    };
                    let _ = tx.send(out).await;
                });
            }
            OutboundMessage::FetchRequest(req) => {
                self.tasks.spawn(async move {
                    let result = transport.send_fetch(peer, req).await;
                    let out = match result {
                        Ok(resp) => OutboundResult::Fetch {
                            peer,
                            response: resp,
                        },
                        Err(e) => OutboundResult::Error {
                            peer,
                            kind: "fetch",
                            err: e.to_string(),
                        },
                    };
                    let _ = tx.send(out).await;
                });
            }
            OutboundMessage::VoteResponse(_)
            | OutboundMessage::PreVoteResponse(_)
            | OutboundMessage::FetchResponse(_) => {
                // Responses to inbound RPCs are returned on the
                // matching gRPC oneshot, never dispatched as
                // free-standing client RPCs. Reaching here means the
                // engine emitted a response with no matching inbound
                // context — defensive log only.
                warn!(
                    target: "xraft_server::router",
                    %peer,
                    "dispatching response-typed OutboundMessage as outbound: dropping (programmer error)"
                );
            }
            OutboundMessage::FetchSnapshotRequest(req) => {
                // Real outbound dispatch — invoke the transport's
                // server-streaming FetchSnapshot RPC and drain the
                // returned `SnapshotChunkStream`. Stage 4.2 does not
                // feed chunks back into `RaftNode::step` (the engine
                // has no `Input::SnapshotChunk` variant yet — that is
                // Phase 5's snapshot install pipeline); we surface the
                // chunk count + completion flag via
                // `OutboundResult::FetchSnapshot` so the driver can
                // observe transport health and tests can assert the
                // dispatch actually reached the wire.
                //
                // The drain loop is wrapped in `tokio::time::timeout`
                // against the router's `fetch_snapshot_deadline`. A
                // slow or malicious peer that keeps the stream open
                // and trickles non-`done` chunks indefinitely would
                // otherwise pin a `JoinSet` slot until shutdown
                // `abort_all_now`; the timeout converts that hang
                // into a surfaced `OutboundResult::Error { kind:
                // "fetch_snapshot" }` so the engine / operator can
                // react (e.g. retry with a different peer) and the
                // task makes forward progress under load.
                let deadline = self.fetch_snapshot_deadline;
                self.tasks.spawn(async move {
                    let out = match transport.send_fetch_snapshot(peer, req).await {
                        Ok(mut stream) => {
                            let drain = async {
                                let mut chunk_count: u64 = 0;
                                let mut completed = false;
                                let mut err: Option<String> = None;
                                loop {
                                    let next = std::future::poll_fn(|cx| {
                                        stream.as_mut().poll_next(cx)
                                    })
                                    .await;
                                    match next {
                                        Some(Ok(chunk)) => {
                                            chunk_count += 1;
                                            if chunk.done {
                                                completed = true;
                                            }
                                        }
                                        Some(Err(e)) => {
                                            err = Some(e.to_string());
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                                (chunk_count, completed, err)
                            };
                            match tokio::time::timeout(deadline, drain).await {
                                Ok((chunk_count, completed, err)) => {
                                    if let Some(e) = err {
                                        OutboundResult::Error {
                                            peer,
                                            kind: "fetch_snapshot",
                                            err: e,
                                        }
                                    } else if !completed {
                                        // Stream ended without a `done = true`
                                        // chunk — this is a transport-level
                                        // truncation (peer closed the stream
                                        // before the snapshot was fully sent).
                                        // Surface as `Error` so the engine /
                                        // operator can distinguish a truncated
                                        // transfer from a successful one. The
                                        // `OutboundResult::FetchSnapshot`
                                        // variant is therefore only emitted on
                                        // a clean `completed=true` stream.
                                        OutboundResult::Error {
                                            peer,
                                            kind: "fetch_snapshot",
                                            err: format!(
                                                "FetchSnapshot stream ended after {chunk_count} chunks without done=true"
                                            ),
                                        }
                                    } else {
                                        OutboundResult::FetchSnapshot {
                                            peer,
                                            chunk_count,
                                            completed,
                                        }
                                    }
                                }
                                Err(_elapsed) => {
                                    // The drain loop exceeded
                                    // `fetch_snapshot_deadline`. Surface
                                    // as `Error` so the spawned task
                                    // does not pin a `JoinSet` slot
                                    // indefinitely on a slow or
                                    // misbehaving peer. The deadline
                                    // is intentionally generous (see
                                    // `DriverConfig::fetch_snapshot_deadline`
                                    // default) so legitimate large
                                    // snapshot transfers are not
                                    // truncated.
                                    OutboundResult::Error {
                                        peer,
                                        kind: "fetch_snapshot",
                                        err: format!(
                                            "FetchSnapshot stream drain exceeded deadline of {}ms",
                                            deadline.as_millis()
                                        ),
                                    }
                                }
                            }
                        }
                        Err(e) => OutboundResult::Error {
                            peer,
                            kind: "fetch_snapshot",
                            err: e.to_string(),
                        },
                    };
                    let _ = tx.send(out).await;
                });
            }
        }
    }

    /// Number of in-flight outbound tasks. Mainly for tests / metrics.
    pub fn in_flight(&self) -> usize {
        self.tasks.len()
    }

    /// Reap one completed outbound task, if any. Returns `None` when
    /// the set is empty.
    async fn reap_one(&mut self) {
        if let Some(joined) = self.tasks.join_next().await
            && let Err(e) = joined
        {
            warn!(target: "xraft_server::router", error = %e, "outbound task panicked");
        }
    }

    /// Immediately abort all in-flight outbound tasks without waiting
    /// for them to complete. Used on fail-stop halt (persistence broken)
    /// where graceful drain is unsafe, and from the graceful
    /// `shutdown_sequence` after its drain deadline expires.
    async fn abort_all_now(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Configuration knobs supplied at [`Driver::new`].
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// Cadence at which the driver feeds [`Input::Tick`] into the node.
    /// Should match `ClusterConfig::tick_interval_ms`.
    pub tick_interval: Duration,
    /// Maximum number of entries the leader returns in a single
    /// `FetchResponse` when serving a non-diverging fetch. Followers
    /// loop the fetch on the next interval, so this only caps a single
    /// round.
    pub max_fetch_batch: usize,
    /// Wall-clock deadline for graceful shutdown drain (in-flight
    /// outbound RPCs + final flushes). Matches the workstream brief's
    /// "5 second" requirement.
    pub shutdown_drain_deadline: Duration,
    /// Wall-clock deadline applied to a single outbound FetchSnapshot
    /// stream-drain task. A slow or malicious peer that keeps the
    /// stream open with non-`done` chunks would otherwise pin a
    /// `JoinSet` slot indefinitely (until `abort_all_now` at
    /// shutdown). Exceeding this deadline surfaces an
    /// `OutboundResult::Error { kind: "fetch_snapshot" }` instead.
    /// Defaults to 30 s — generous for legitimate large snapshot
    /// transfers, tight enough that a misbehaving peer cannot stall a
    /// task forever.
    pub fetch_snapshot_deadline: Duration,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(10),
            max_fetch_batch: 64,
            shutdown_drain_deadline: Duration::from_secs(5),
            fetch_snapshot_deadline: Duration::from_secs(30),
        }
    }
}

/// Async event loop owning a single [`RaftNode`] plus storage,
/// transport, and state-machine backends.
///
/// Construct with [`Driver::new`], obtain a [`DriverHandle`] via
/// [`Driver::handle`] before calling [`Driver::run`].
pub struct Driver<T, L, HS, SS, SM>
where
    T: Transport + Send + Sync + 'static,
    L: LogStore + Send + 'static,
    HS: HardStateStore + Send + 'static,
    SS: SnapshotStore + Send + 'static,
    SM: StateMachine + Send + Sync + 'static,
{
    node: RaftNode,
    log_store: L,
    hs_store: HS,
    snapshot_store: SS,
    state_machine: SM,
    router: MessageRouter<T>,
    config: DriverConfig,
    events_rx: mpsc::Receiver<DriverEvent>,
    outbound_rx: mpsc::Receiver<OutboundResult>,
    shutdown: Arc<tokio::sync::Notify>,
    /// Pending commit waiters keyed by `LogIndex` of the proposed entry.
    pending: BTreeMap<LogIndex, Vec<oneshot::Sender<XResult<LogIndex>>>>,
    tick: Interval,
    /// Public handle's event sender (kept here so the inbound handler
    /// can be obtained even after `run()` has been entered).
    handle: DriverHandle,
    /// Set to `Some(reason)` when the driver MUST halt due to a
    /// persistence failure (Raft driver contract — see
    /// `xraft-core/src/node.rs` §"Driver contract": partial application
    /// of an action list after a persist/append/flush/truncate failure
    /// is unsafe). When set, `run()` exits via `fail_stop_shutdown` and
    /// returns `Err(XRaftError::Storage(reason))`.
    halt_reason: Option<String>,
}

impl<T, L, HS, SS, SM> Driver<T, L, HS, SS, SM>
where
    T: Transport + Send + Sync + 'static,
    L: LogStore + Send + 'static,
    HS: HardStateStore + Send + 'static,
    SS: SnapshotStore + Send + 'static,
    SM: StateMachine + Send + Sync + 'static,
{
    /// Construct a new driver.
    pub fn new(
        node: RaftNode,
        log_store: L,
        hs_store: HS,
        snapshot_store: SS,
        state_machine: SM,
        transport: Arc<T>,
        config: DriverConfig,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let router = MessageRouter::new_with_fetch_snapshot_deadline(
            transport,
            outbound_tx,
            config.fetch_snapshot_deadline,
        );
        let handle = DriverHandle {
            events: events_tx,
            shutdown: shutdown.clone(),
        };
        let mut tick = interval(config.tick_interval);
        // Skip missed ticks rather than burst-firing them — under load
        // we never want a 100ms stall to spawn 10 catch-up Tick events.
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            node,
            log_store,
            hs_store,
            snapshot_store,
            state_machine,
            router,
            config,
            events_rx,
            outbound_rx,
            shutdown,
            pending: BTreeMap::new(),
            tick,
            handle,
            halt_reason: None,
        }
    }

    /// Clone of the public handle. Get one *before* calling `run()`;
    /// after `run` returns the channels are closed.
    pub fn handle(&self) -> DriverHandle {
        self.handle.clone()
    }

    /// Run the driver loop until shutdown is signalled or a persistence
    /// failure forces fail-stop.
    ///
    /// Returns:
    /// - `Ok(())` when graceful shutdown completed (queued events
    ///   drained within the deadline, final hard state persisted).
    /// - `Err(XRaftError::Storage(reason))` when a persistence
    ///   operation (`PersistHardState`, log append, flush, truncate)
    ///   failed mid-action-list. The Raft driver contract
    ///   (`xraft-core/src/node.rs` §"Driver contract") REQUIRES halting
    ///   on persistence failure — partial application of an action
    ///   list is unsafe; the operator must restart the node and the
    ///   node will recover from durable state.
    pub async fn run(mut self) -> XResult<()> {
        info!(
            target: "xraft_server::driver",
            node_id = %self.node.id,
            tick_ms = self.config.tick_interval.as_millis(),
            "driver loop starting"
        );

        // Prime the tick interval — the first .tick() resolves immediately.
        let _ = self.tick.tick().await;

        loop {
            tokio::select! {
                biased;

                _ = self.shutdown.notified() => {
                    info!(target: "xraft_server::driver", "shutdown signal received");
                    break;
                }

                Some(event) = self.events_rx.recv() => {
                    match event {
                        DriverEvent::Inbound(rpc) => self.handle_inbound(rpc).await,
                        DriverEvent::Client(cmd) => self.handle_client_command(cmd).await,
                    }
                }

                Some(res) = self.outbound_rx.recv() => {
                    self.handle_outbound_result(res).await;
                }

                _ = self.tick.tick() => {
                    self.handle_tick().await;
                }

                _ = self.router.reap_one(), if self.router.in_flight() > 0 => {
                    // Completed outbound task reaped; its result (if any)
                    // was already forwarded via `outbound_rx`.
                }
            }

            // Honour the fail-stop contract immediately, before another
            // tick or RPC can advance the in-memory node past durable
            // state.
            if self.halt_reason.is_some() {
                return self.fail_stop_shutdown().await;
            }
        }

        // Graceful shutdown — drain queued events with deadline, then
        // finalize. If a persistence failure surfaces during drain,
        // switch to fail-stop. Otherwise, `shutdown_sequence` returns
        // `Err(Storage(_))` on final-persist/flush failure and `Ok(())`
        // on a clean exit.
        self.graceful_drain().await;
        if self.halt_reason.is_some() {
            return self.fail_stop_shutdown().await;
        }
        self.shutdown_sequence().await
    }

    // -----------------------------------------------------------------------
    // Event handlers
    // -----------------------------------------------------------------------

    async fn handle_tick(&mut self) {
        let actions = self.node.step(Input::Tick);
        self.process_actions(actions, None).await;
    }

    async fn handle_outbound_result(&mut self, res: OutboundResult) {
        match res {
            OutboundResult::Vote { peer, response } => {
                let actions = self.node.step(Input::VoteResponse {
                    from: peer,
                    response,
                });
                self.process_actions(actions, None).await;
            }
            OutboundResult::PreVote { peer, response } => {
                let actions = self.node.step(Input::PreVoteResponse {
                    from: peer,
                    response,
                });
                self.process_actions(actions, None).await;
            }
            OutboundResult::Fetch { response, .. } => {
                let actions = self.node.step(Input::FetchResponse(response));
                self.process_actions(actions, None).await;
            }
            OutboundResult::FetchSnapshot {
                peer,
                chunk_count,
                completed,
            } => {
                // Stage 4.2 — no `Input::SnapshotChunk` exists yet.
                // Phase 5 will pipe chunks through `RaftNode::step`.
                debug!(
                    target: "xraft_server::driver",
                    %peer, chunk_count, completed,
                    "outbound FetchSnapshot stream finished"
                );
            }
            OutboundResult::Error { peer, kind, err } => {
                debug!(
                    target: "xraft_server::driver",
                    %peer, kind, err,
                    "outbound RPC error (will retry on next tick)"
                );
            }
        }
    }

    async fn handle_client_command(&mut self, cmd: ClientCommand) {
        // Capture last_log_index BEFORE step so we can detect whether the
        // proposal was accepted (leader appends at last_log_index + 1).
        let pre_last = self.node.last_log_index;
        let leader_hint = self.node.leader_id;
        let is_leader = self.node.role == NodeRole::Leader;
        if !is_leader {
            let _ = cmd.reply.send(Err(XRaftError::NotLeader { leader_hint }));
            return;
        }

        let actions = self.node.step(Input::ClientPropose(cmd.command));

        // If the node accepted the proposal, last_log_index has advanced
        // by exactly one and the first action is `AppendEntries(vec![entry])`
        // for that entry. We register the waiter at the new index BEFORE
        // processing actions so an immediate single-voter ApplyToStateMachine
        // resolves it correctly.
        let post_last = self.node.last_log_index;
        if post_last.0 == pre_last.0 + 1
            && actions.iter().any(|a| {
                matches!(
                    a,
                    Action::AppendEntries(entries)
                        if entries.iter().any(|e| e.index == post_last)
                )
            })
        {
            self.pending.entry(post_last).or_default().push(cmd.reply);
        } else {
            // Either we were not leader (handled above) or the engine
            // dropped the propose. Reply with NotLeader so the client
            // can route to the correct node.
            let _ = cmd.reply.send(Err(XRaftError::NotLeader {
                leader_hint: self.node.leader_id,
            }));
        }

        self.process_actions(actions, None).await;
    }

    async fn handle_inbound(&mut self, rpc: InboundRpc) {
        match rpc {
            InboundRpc::Vote { req, reply } => self.handle_inbound_vote(req, reply).await,
            InboundRpc::PreVote { req, reply } => self.handle_inbound_pre_vote(req, reply).await,
            InboundRpc::Fetch { req, reply } => self.handle_inbound_fetch(req, reply).await,
            InboundRpc::FetchSnapshot { req, reply } => {
                self.handle_inbound_fetch_snapshot(req, reply).await
            }
        }
    }

    async fn handle_inbound_vote(&mut self, req: VoteRequest, reply: VoteReply) {
        let candidate = req.candidate_id;
        let actions = self.node.step(Input::VoteRequest(req));
        let captured = self.process_actions(actions, Some(candidate)).await;
        // Storage/durability failure during action processing makes any
        // captured Vote reply unsafe to return (e.g. a granted vote
        // whose hard state wasn't persisted). Surface the error instead.
        if let Some(err) = captured.error {
            let _ = reply.send(Err(err));
            return;
        }
        let response = captured.vote.unwrap_or_else(|| self.default_deny_vote());
        let _ = reply.send(Ok(response));
    }

    async fn handle_inbound_pre_vote(&mut self, req: PreVoteRequest, reply: PreVoteReply) {
        let candidate = req.candidate_id;
        let actions = self.node.step(Input::PreVoteRequest(req));
        let captured = self.process_actions(actions, Some(candidate)).await;
        if let Some(err) = captured.error {
            let _ = reply.send(Err(err));
            return;
        }
        let response = captured
            .pre_vote
            .unwrap_or_else(|| self.default_deny_pre_vote());
        let _ = reply.send(Ok(response));
    }

    async fn handle_inbound_fetch(&mut self, req: FetchRequest, reply: FetchReply) {
        let replica = req.replica_id;
        let actions = self.node.step(Input::FetchRequest(req));
        let captured = self.process_actions(actions, Some(replica)).await;
        if let Some(err) = captured.error {
            let _ = reply.send(Err(err));
            return;
        }
        let response = captured.fetch.unwrap_or_else(|| self.default_deny_fetch());
        let _ = reply.send(Ok(response));
    }

    async fn handle_inbound_fetch_snapshot(
        &mut self,
        req: FetchSnapshotRequest,
        reply: FetchSnapshotReply,
    ) {
        // ─── Fencing pipeline ─────────────────────────────────────────
        // FetchSnapshot bypasses RaftNode::step (it is a pure storage
        // read), so the safety / membership / epoch / role checks that
        // the engine applies to FetchRequest (`xraft-core/src/node.rs`
        // `handle_fetch_request`) MUST be replicated inline here, lest
        // an arbitrary same-cluster caller pull our snapshot bytes.
        // Check order is deliberate:
        //   1. cluster_id       — drop foreign clusters first
        //   2. self-loopback    — never serve to ourselves
        //   3. role == Leader   — only the leader serves snapshots in
        //                         Stage 4.2 (matches FetchRequest
        //                         semantics; follower serving is a
        //                         future-stage decision)
        //   4. membership       — replica_id must be a known voter or
        //                         a tracked peer (Observer-only
        //                         fetchers are allowed iff in `peers`)
        //   5. leader_epoch     — strict equality with our current term
        //                         (`leader_epoch` mirrors `current_term`
        //                         in the KRaft protocol). We do NOT
        //                         mutate node state on epoch mismatch;
        //                         caller must re-discover the leader.
        //   6. snapshot lookup  — find_by_id → SnapshotNotFound on miss
        //   7. open reader      — surface storage errors verbatim

        // (1) cluster_id
        if req.cluster_id != self.node.config.cluster_id {
            let _ = reply.send(Err(XRaftError::Transport(format!(
                "FetchSnapshot cluster_id mismatch: expected {}, got {}",
                self.node.config.cluster_id, req.cluster_id
            ))));
            return;
        }

        // (2) self-loopback
        if req.replica_id == self.node.id {
            let _ = reply.send(Err(XRaftError::Transport(
                "FetchSnapshot self-loopback rejected".into(),
            )));
            return;
        }

        // (3) role: only Leader serves
        if self.node.role != NodeRole::Leader {
            let _ = reply.send(Err(XRaftError::NotLeader {
                leader_hint: self.node.leader_id,
            }));
            return;
        }

        // (4) membership: replica_id must be a voter or a tracked peer.
        // Matches the trust-boundary check applied to FetchRequest at
        // `xraft-core/src/node.rs` `handle_fetch_request` — an unknown
        // sender must not be able to pull snapshot bytes.
        let is_voter = self
            .node
            .voter_set
            .as_ref()
            .is_some_and(|vs| vs.contains(req.replica_id));
        let is_tracked_peer = self.node.peers.contains_key(&req.replica_id);
        if !is_voter && !is_tracked_peer {
            let _ = reply.send(Err(XRaftError::Transport(format!(
                "FetchSnapshot from unknown replica {} (not a voter and not a tracked peer)",
                req.replica_id
            ))));
            return;
        }

        // (5) leader_epoch: strict equality with current term. A mismatch
        // means either the caller or we are stale; the caller must
        // re-discover the leader rather than have us serve a chunk
        // stream stamped with our (possibly different) leader_epoch.
        let our_term = self.node.hard_state.current_term.0;
        if req.leader_epoch != our_term {
            let _ = reply.send(Err(XRaftError::Transport(format!(
                "FetchSnapshot leader_epoch mismatch: caller={}, ours={}",
                req.leader_epoch, our_term
            ))));
            return;
        }

        // (6) Resolve snapshot metadata by id.
        let meta = match self.snapshot_store.find_by_id(&req.snapshot_id) {
            Ok(Some(m)) => m,
            Ok(None) => {
                let _ = reply.send(Err(XRaftError::SnapshotNotFound(req.snapshot_id.clone())));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        // (7) Open a chunked reader. `max_bytes == 0` means "no limit"
        // per the FetchSnapshotRequest doc comment; pass `None` to
        // `snapshot_reader_from_offset` in that case so it uses the
        // store's default chunk size.
        let max_bytes_opt: Option<u64> = if req.max_bytes == 0 {
            None
        } else {
            Some(req.max_bytes)
        };
        // `chunk_size` of 0 makes the store pick its default; the
        // store also uses `chunk_size == 0 → default` semantics.
        let chunk_size: usize = max_bytes_opt.map(|n| n as usize).unwrap_or(0);
        let iter = match self.snapshot_store.snapshot_reader_from_offset(
            &meta,
            chunk_size,
            req.offset,
            max_bytes_opt,
        ) {
            Ok(it) => it,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        // Eagerly collect chunks for Stage 4.2. A future Phase 5 iteration
        // can flip this to a lazy / backpressured stream when the snapshot
        // install pipeline lands; today's snapshots are small in-memory
        // payloads so eager collection is acceptable.
        let leader_epoch = self.node.hard_state.current_term.0;
        let cluster_id = self.node.config.cluster_id.clone();
        let mut chunks: std::collections::VecDeque<XResult<FetchSnapshotChunk>> =
            std::collections::VecDeque::new();
        for item_result in iter {
            match item_result {
                Ok(item) => {
                    chunks.push_back(Ok(item.into_fetch_chunk(cluster_id.clone(), leader_epoch)));
                }
                Err(e) => {
                    chunks.push_back(Err(e));
                    break;
                }
            }
        }
        let stream: SnapshotChunkStream = Box::pin(StaticChunkStream { chunks });
        let _ = reply.send(Ok(stream));
    }

    // -----------------------------------------------------------------------
    // Action processing
    // -----------------------------------------------------------------------

    /// Process every `Action` emitted by a single `step` call.
    ///
    /// `inbound_origin` is the `NodeId` of the requester for inbound
    /// RPC processing — used to capture the matching response action as
    /// the gRPC reply rather than dispatching it via the transport.
    ///
    /// Returns the captured response (if any) so the inbound handler
    /// can forward it on the oneshot.
    async fn process_actions(
        &mut self,
        actions: Vec<Action>,
        inbound_origin: Option<NodeId>,
    ) -> CapturedResponse {
        let mut captured = CapturedResponse::default();
        for action in actions {
            match action {
                Action::PersistHardState => {
                    if let Err(e) = self.hs_store.persist(&self.node.hard_state) {
                        let msg = format!("hard-state persist failed: {e}");
                        error!(target: "xraft_server::driver", %msg, "halting driver");
                        // CRITICAL — Raft S1 election safety: a granted
                        // vote (or any term-bump) reply is unsafe until
                        // the hard state is durable. Set both the
                        // captured-reply error (so the inbound handler
                        // returns Err) AND the halt reason (so the
                        // driver fail-stops per node.rs §"Driver
                        // contract").
                        captured.error = Some(XRaftError::Storage(msg.clone()));
                        self.halt_reason.get_or_insert(msg);
                        break;
                    }
                }
                Action::AppendEntries(entries) => {
                    if let Err(e) = self.log_store.append(&entries) {
                        let msg = format!("log append failed: {e}");
                        error!(target: "xraft_server::driver", %msg, "halting driver");
                        // Fail any waiter registered at these indices so
                        // propose() returns Err rather than hanging.
                        for entry in &entries {
                            if let Some(waiters) = self.pending.remove(&entry.index) {
                                for w in waiters {
                                    let _ = w.send(Err(XRaftError::Storage(msg.clone())));
                                }
                            }
                        }
                        captured.error = Some(XRaftError::Storage(msg.clone()));
                        self.halt_reason.get_or_insert(msg);
                        break;
                    }
                    if let Err(e) = self.log_store.flush() {
                        let msg = format!("log flush after append failed: {e}");
                        error!(target: "xraft_server::driver", %msg, "halting driver");
                        for entry in &entries {
                            if let Some(waiters) = self.pending.remove(&entry.index) {
                                for w in waiters {
                                    let _ = w.send(Err(XRaftError::Storage(msg.clone())));
                                }
                            }
                        }
                        captured.error = Some(XRaftError::Storage(msg.clone()));
                        self.halt_reason.get_or_insert(msg);
                        break;
                    }
                }
                Action::TruncateLog {
                    from_index_inclusive,
                } => {
                    if let Err(e) = self.log_store.truncate_from(from_index_inclusive) {
                        let msg = format!("log truncate failed: {e}");
                        error!(target: "xraft_server::driver", %msg, "halting driver");
                        captured.error = Some(XRaftError::Storage(msg.clone()));
                        self.halt_reason.get_or_insert(msg);
                        break;
                    }
                    if let Err(e) = self.log_store.flush() {
                        let msg = format!("log flush after truncate failed: {e}");
                        error!(target: "xraft_server::driver", %msg, "halting driver");
                        captured.error = Some(XRaftError::Storage(msg.clone()));
                        self.halt_reason.get_or_insert(msg);
                        break;
                    }
                    let new_last_index = self.log_store.last_index();
                    let new_last_term = self.log_store.last_term();
                    self.node.set_last_log(new_last_index, new_last_term);
                }
                Action::ApplyToStateMachine { from, to } => {
                    if let Err(reason) = self.apply_committed(from, to) {
                        // Mirror the persistence-failure branches above:
                        // a fail-stopping driver must surface the error
                        // to any in-flight inbound RPC (Vote/PreVote/
                        // Fetch/FetchSnapshot) whose handler reads
                        // `captured.error` before any captured success
                        // payload. Without this, the RPC reply can
                        // return Ok with a captured response while the
                        // driver is about to halt — clients then act on
                        // a "successful" reply from a dying node.
                        captured.error = Some(XRaftError::Storage(reason.clone()));
                        self.halt_reason.get_or_insert(reason);
                        break;
                    }
                }
                Action::TakeSnapshot => {
                    if let Err(reason) = self.handle_take_snapshot() {
                        error!(
                            target: "xraft_server::driver",
                            reason = %reason,
                            "TakeSnapshot failed; halting driver"
                        );
                        captured.error = Some(XRaftError::Storage(reason.clone()));
                        self.halt_reason.get_or_insert(reason);
                        break;
                    }
                }
                Action::InstallSnapshot { metadata, data } => {
                    if let Err(reason) = self.handle_install_snapshot(metadata, data) {
                        error!(
                            target: "xraft_server::driver",
                            reason = %reason,
                            "InstallSnapshot failed; halting driver"
                        );
                        captured.error = Some(XRaftError::Storage(reason.clone()));
                        self.halt_reason.get_or_insert(reason);
                        break;
                    }
                }
                Action::BecomeLeader => {
                    info!(
                        target: "xraft_server::driver",
                        node_id = %self.node.id,
                        term = %self.node.hard_state.current_term,
                        "became Leader"
                    );
                }
                Action::StepDown => {
                    info!(
                        target: "xraft_server::driver",
                        node_id = %self.node.id,
                        term = %self.node.hard_state.current_term,
                        "stepped down"
                    );
                    // Outstanding leader-only proposals can no longer be
                    // committed under our leadership. Resolve them with
                    // NotLeader carrying the new hint (typically None
                    // immediately after step-down).
                    let waiters = std::mem::take(&mut self.pending);
                    for (_idx, list) in waiters {
                        for w in list {
                            let _ = w.send(Err(XRaftError::NotLeader {
                                leader_hint: self.node.leader_id,
                            }));
                        }
                    }
                }
                Action::SendMessage { to, message } => {
                    // Inbound-response capture path: when this action's
                    // recipient matches the inbound RPC's origin AND the
                    // message variant matches the expected response shape,
                    // return it via oneshot rather than dispatching out
                    // over the transport.
                    if Some(to) == inbound_origin {
                        match message {
                            OutboundMessage::VoteResponse(r) if captured.vote.is_none() => {
                                captured.vote = Some(r);
                                continue;
                            }
                            OutboundMessage::PreVoteResponse(r) if captured.pre_vote.is_none() => {
                                captured.pre_vote = Some(r);
                                continue;
                            }
                            OutboundMessage::FetchResponse(r) if captured.fetch.is_none() => {
                                captured.fetch = Some(r);
                                continue;
                            }
                            other => {
                                self.router.dispatch(to, other);
                                continue;
                            }
                        }
                    }
                    self.router.dispatch(to, message);
                }
                Action::ServeFetch {
                    to,
                    cluster_id,
                    leader_epoch,
                    leader_id,
                    high_watermark,
                    fetch_offset,
                    last_fetched_epoch,
                } => {
                    let fetch_resp = match self.materialize_fetch_response(
                        cluster_id,
                        leader_epoch,
                        leader_id,
                        high_watermark,
                        fetch_offset,
                        last_fetched_epoch,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            error!(
                                target: "xraft_server::driver",
                                error = %e,
                                "materialize_fetch_response failed"
                            );
                            if captured.error.is_none() {
                                captured.error = Some(e);
                            }
                            continue;
                        }
                    };

                    // Feed FetchRequestAcked into the engine on
                    // non-diverging paths so peer progress + HW advance
                    // (per node.rs comments on Action::ServeFetch).
                    let acked_offset = if fetch_resp.diverging_epoch.is_none() && fetch_offset.0 > 0
                    {
                        Some(LogIndex(fetch_offset.0 - 1))
                    } else {
                        None
                    };

                    if Some(to) == inbound_origin && captured.fetch.is_none() {
                        captured.fetch = Some(fetch_resp);
                    } else {
                        self.router
                            .dispatch(to, OutboundMessage::FetchResponse(fetch_resp));
                    }

                    if let Some(off) = acked_offset {
                        let follow = self.node.step(Input::FetchRequestAcked {
                            replica_id: to,
                            confirmed_offset: off,
                        });
                        // Recurse — but Box::pin to keep the future Send
                        // and the recursion depth bounded (Acked typically
                        // emits at most ApplyToStateMachine).
                        let nested = Box::pin(self.process_actions(follow, inbound_origin)).await;
                        captured.merge(nested);
                        // Nested step may have halted on a downstream
                        // persistence failure — propagate the break.
                        if self.halt_reason.is_some() {
                            break;
                        }
                    }
                }
            }
        }
        captured
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a `FetchResponse` from the durable log per the
    /// `Action::ServeFetch` contract (see `node.rs` doc comment).
    ///
    /// Divergence detection: compares
    /// `log_store.term_at(fetch_offset - 1)` to `last_fetched_epoch`.
    /// On mismatch, returns an empty-entries response carrying
    /// `Some(DivergingEpoch{...})`. Otherwise reads up to
    /// `max_fetch_batch` entries starting at `fetch_offset`.
    fn materialize_fetch_response(
        &self,
        cluster_id: String,
        leader_epoch: u64,
        leader_id: NodeId,
        high_watermark: LogIndex,
        fetch_offset: LogIndex,
        last_fetched_epoch: Term,
    ) -> XResult<FetchResponse> {
        // Divergence detection at fetch_offset - 1.
        let mut diverging: Option<DivergingEpoch> = None;
        if fetch_offset.0 > 1 {
            let prev = LogIndex(fetch_offset.0 - 1);
            match self.log_store.term_at(prev) {
                Ok(Some(actual_term)) if actual_term != last_fetched_epoch => {
                    // Find the end of this epoch on the leader's log —
                    // best-effort: clamp to leader tail.
                    let end_offset = self.log_store.last_index();
                    diverging = Some(DivergingEpoch {
                        epoch: actual_term,
                        end_offset,
                    });
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    // Follower wants an entry at an index we have
                    // compacted / truncated — report divergence at our
                    // tail so the follower truncates back.
                    let end_offset = self.log_store.last_index();
                    diverging = Some(DivergingEpoch {
                        epoch: self.log_store.last_term(),
                        end_offset,
                    });
                }
                Err(e) => {
                    return Err(XRaftError::Storage(format!(
                        "term_at({prev}) failed during fetch: {e}"
                    )));
                }
            }
        }

        let entries = if diverging.is_some() {
            Vec::new()
        } else {
            let end = LogIndex(
                fetch_offset
                    .0
                    .saturating_add(self.config.max_fetch_batch as u64),
            );
            self.log_store.get_range(fetch_offset, end).map_err(|e| {
                XRaftError::Storage(format!(
                    "get_range({fetch_offset}, {end}) failed during fetch: {e}"
                ))
            })?
        };

        Ok(FetchResponse {
            cluster_id,
            leader_epoch,
            leader_id,
            high_watermark,
            entries,
            diverging_epoch: diverging,
        })
    }

    /// Apply committed entries `[from, to]` to the state machine and
    /// resolve any pending client waiters whose index falls within the
    /// range.
    ///
    /// Returns `Ok(())` when every entry in the range was applied
    /// successfully (or was a non-application payload). Returns
    /// `Err(reason)` on the first failure — the caller MUST set
    /// `halt_reason` and stop processing further input. Per the
    /// `Action::ApplyToStateMachine` contract (engine has already
    /// advanced `last_applied = to` before emitting the action), the
    /// driver cannot safely continue past a failed apply because the
    /// in-memory `last_applied` would diverge from what the state
    /// machine actually applied.
    ///
    /// Any waiter associated with the failing index receives the error
    /// via its oneshot so `propose()` returns rather than hanging;
    /// remaining waiters are failed by `fail_stop_shutdown()` on the
    /// halt path.
    fn apply_committed(&mut self, from: LogIndex, to: LogIndex) -> std::result::Result<(), String> {
        // Defensive guards: the engine's `Action::ApplyToStateMachine`
        // contract states `from <= to`, but `apply_committed` is also
        // reachable from direct dispatch in tests and a future engine
        // change could regress this. Halt instead of panicking on
        // either an inverted range or a `to + 1` overflow.
        if from > to {
            let msg = format!("apply: invalid range {from}..={to} (from > to)");
            error!(target: "xraft_server::driver", %msg, "halting driver");
            return Err(msg);
        }
        let end = match to.0.checked_add(1) {
            Some(v) => LogIndex(v),
            None => {
                let msg = format!("apply: range end overflow for to={to}");
                error!(target: "xraft_server::driver", %msg, "halting driver");
                return Err(msg);
            }
        };
        let entries = match self.log_store.get_range(from, end) {
            Ok(e) => e,
            Err(e) => {
                let msg = format!("apply: read range {from}..={to} failed: {e}");
                error!(
                    target: "xraft_server::driver",
                    error = %e,
                    from = %from,
                    to = %to,
                    "apply: failed to read log range"
                );
                // Fail every waiter in [from, to] so propose() doesn't
                // hang forever when log read fails post-commit.
                let indices: Vec<LogIndex> =
                    self.pending.range(from..=to).map(|(k, _)| *k).collect();
                for idx in indices {
                    self.resolve_waiters_at(idx, Err(XRaftError::Storage(msg.clone())));
                }
                return Err(msg);
            }
        };
        // Validate the range is complete and contiguous. The
        // `Action::ApplyToStateMachine` contract is that the engine has
        // ALREADY advanced `last_applied = to` before emitting the
        // action, so any gap or short read here means the SM will not
        // see entries that the engine believes are applied — a silent
        // divergence that would corrupt every subsequent linearizable
        // read. Halt the driver rather than skipping silently.
        let expected_len = (to.0 - from.0 + 1) as usize;
        if entries.len() != expected_len {
            let msg = format!(
                "apply: log_store.get_range({from}..={to}) returned {} entries, expected {}",
                entries.len(),
                expected_len
            );
            error!(
                target: "xraft_server::driver",
                returned = entries.len(),
                expected = expected_len,
                from = %from,
                to = %to,
                "apply: log range short read; halting driver"
            );
            let indices: Vec<LogIndex> =
                self.pending.range(from..=to).map(|(k, _)| *k).collect();
            for idx in indices {
                self.resolve_waiters_at(idx, Err(XRaftError::Storage(msg.clone())));
            }
            return Err(msg);
        }
        for (offset, entry) in entries.iter().enumerate() {
            let expected_idx = LogIndex(from.0 + offset as u64);
            if entry.index != expected_idx {
                let msg = format!(
                    "apply: log_store.get_range({from}..={to}) returned entry at {} at position {}, expected {}",
                    entry.index, offset, expected_idx
                );
                error!(
                    target: "xraft_server::driver",
                    got = %entry.index,
                    expected = %expected_idx,
                    "apply: log range index gap; halting driver"
                );
                let indices: Vec<LogIndex> =
                    self.pending.range(from..=to).map(|(k, _)| *k).collect();
                for idx in indices {
                    self.resolve_waiters_at(idx, Err(XRaftError::Storage(msg.clone())));
                }
                return Err(msg);
            }
        }
        for entry in entries {
            match &entry.payload {
                EntryPayload::Command(bytes) => {
                    // The `StateMachine::apply` contract returns the
                    // serialised command result. Stage 5.1 does not yet
                    // pipe that result back to the proposing client —
                    // that wiring belongs to the embedded-read / propose-
                    // result work in a later stage — so we discard the
                    // bytes here while still honouring the error path.
                    if let Err(e) = self.state_machine.apply(entry.index, bytes) {
                        let msg = format!("state machine apply at {} failed: {e}", entry.index);
                        error!(
                            target: "xraft_server::driver",
                            error = %e,
                            index = %entry.index,
                            "state machine apply failed"
                        );
                        // Resolve THIS entry's waiter so propose() returns
                        // an error rather than hanging. Halt immediately:
                        // continuing would compound the divergence between
                        // engine `last_applied` and actual SM state.
                        self.resolve_waiters_at(entry.index, Err(XRaftError::Storage(msg.clone())));
                        return Err(msg);
                    }
                }
                EntryPayload::NoOp | EntryPayload::ConfigChange(_) | EntryPayload::Snapshot(_) => {
                    // Non-application payloads; nothing to feed to the SM.
                }
            }
            self.resolve_waiters_at(entry.index, Ok(entry.index));
        }
        Ok(())
    }

    /// Dispatch [`Action::TakeSnapshot`]: capture state-machine state and
    /// persist it to the [`SnapshotStore`] tagged with the current
    /// `last_applied` index, the term of that log entry, and the active
    /// voter set. See module-level "Stage 5.1 dispatch" comment for
    /// invariants enforced by each guard.
    fn handle_take_snapshot(&mut self) -> std::result::Result<(), String> {
        let last_applied = self.node.last_applied;
        if last_applied.0 == 0 {
            debug!(
                target: "xraft_server::driver",
                "TakeSnapshot skipped: last_applied=0 (no committed state)"
            );
            return Ok(());
        }
        let voter_set = match self.node.voter_set.as_ref() {
            Some(vs) => vs.clone(),
            None => {
                return Err(
                    "TakeSnapshot: cannot snapshot without a configured voter_set".to_string(),
                );
            }
        };
        let last_term = match self.log_store.term_at(last_applied) {
            Ok(Some(t)) => t,
            Ok(None) => {
                return Err(format!(
                    "TakeSnapshot: term_at(last_applied={last_applied}) returned None"
                ));
            }
            Err(e) => {
                return Err(format!(
                    "TakeSnapshot: term_at(last_applied={last_applied}) failed: {e}"
                ));
            }
        };
        let data = self
            .state_machine
            .snapshot()
            .map_err(|e| format!("TakeSnapshot: state machine snapshot failed: {e}"))?;
        let meta = SnapshotMeta {
            last_included_index: last_applied,
            last_included_term: last_term,
            // SnapshotStore normalises id to canonical form; the caller-
            // supplied value is discarded.
            id: String::new(),
            voter_set: Some(voter_set),
            size_bytes: None,
            checksum: None,
        };
        self.snapshot_store
            .save_snapshot(meta, &data)
            .map_err(|e| format!("TakeSnapshot: snapshot_store.save_snapshot failed: {e}"))?;
        debug!(
            target: "xraft_server::driver",
            index = %last_applied,
            term = %last_term,
            payload_bytes = data.len(),
            "TakeSnapshot: persisted snapshot"
        );
        Ok(())
    }

    /// Dispatch [`Action::InstallSnapshot`]: persist a snapshot received
    /// from the leader, restore the state machine to the captured state,
    /// and advance the local node's bookkeeping (`last_applied`,
    /// `commit_index`, log tip, voter set, peer table) to reflect the
    /// snapshot.
    ///
    /// Ordering rationale — persist BEFORE restore: a crash between
    /// persist and restore can be recovered from the durable snapshot
    /// on restart. The reverse ordering would leave the SM with state
    /// that has no on-disk counterpart, breaking recovery.
    ///
    /// Membership bookkeeping is rebuilt from the snapshot's voter_set
    /// because the engine's election / fetch / leader paths consult
    /// `node.voter_set` and `node.peers` directly and do NOT
    /// self-rebuild them from index advancement.
    fn handle_install_snapshot(
        &mut self,
        metadata: SnapshotMeta,
        data: Vec<u8>,
    ) -> std::result::Result<(), String> {
        let voter_set = metadata.voter_set.clone().ok_or_else(|| {
            format!(
                "InstallSnapshot: snapshot {} (term={}, index={}) missing required voter_set",
                metadata.id, metadata.last_included_term.0, metadata.last_included_index.0,
            )
        })?;
        let snap_idx = metadata.last_included_index;
        let snap_term = metadata.last_included_term;

        // Reject stale snapshots BEFORE touching persistent state or
        // the state machine. If `snap_idx <= self.node.last_applied`,
        // the local state machine has already applied every entry the
        // snapshot covers. Calling `state_machine.restore(&data)` here
        // would roll the SM back to the older (snapshot) state while
        // `last_applied` still reflects the newer applied tip — a
        // silent rollback that corrupts every subsequent read. The
        // `<=` is intentional: at equality the SM is already at that
        // state, so the restore is at best wasteful and at worst (if
        // the snapshot bytes differ from the deterministic apply
        // result) corrupting. Treat as a no-op success — the driver
        // does NOT halt because a stale snapshot is a benign race
        // (e.g. a delayed install for a snapshot the leader already
        // superseded), not a correctness fault.
        if snap_idx <= self.node.last_applied {
            debug!(
                target: "xraft_server::driver",
                snap_index = %snap_idx,
                last_applied = %self.node.last_applied,
                "InstallSnapshot: snapshot at or behind last_applied; no-op (no persist, no restore)"
            );
            return Ok(());
        }

        // Persist first so a crash between save and restore is recoverable
        // from the durable snapshot on restart.
        self.snapshot_store
            .save_snapshot(metadata, &data)
            .map_err(|e| format!("InstallSnapshot: snapshot_store.save_snapshot failed: {e}"))?;

        // Restore the state machine. A deterministic SM that rejects a
        // leader's snapshot is unrecoverable mid-flight — halt and let
        // the operator investigate.
        self.state_machine
            .restore(&data)
            .map_err(|e| format!("InstallSnapshot: state_machine.restore failed: {e}"))?;

        // Advance engine bookkeeping. The `snap_idx > last_applied`
        // guard above proved we're moving last_applied forward, but
        // commit_index / last_log_index can still be ahead of
        // last_applied (e.g. committed-but-not-yet-applied entries
        // already on disk), so each of those needs its own no-regress
        // guard rather than an unconditional assignment.
        self.node.last_applied = snap_idx;
        if snap_idx > self.node.commit_index {
            self.node.commit_index = snap_idx;
        }
        if snap_idx > self.node.last_log_index {
            self.node.set_last_log(snap_idx, snap_term);
        }

        // Rebuild membership from the snapshot's voter_set: the engine's
        // election quorum / fetch validation / peer tracking all consult
        // `node.voter_set` and `node.peers` directly, so they must reflect
        // the post-snapshot configuration. Self is excluded from `peers`
        // (mirrors `RaftNode::new_with_seed`).
        self.node.voter_set = Some(voter_set.clone());
        self.node.peers.clear();
        for voter in voter_set.voters() {
            if voter.node_id != self.node.id {
                self.node.peers.insert(voter.node_id, PeerState::new(true));
            }
        }

        debug!(
            target: "xraft_server::driver",
            index = %snap_idx,
            term = %snap_term,
            payload_bytes = data.len(),
            "InstallSnapshot: persisted, restored, and rebuilt membership"
        );
        Ok(())
    }

    fn resolve_waiters_at(&mut self, index: LogIndex, result: XResult<LogIndex>) {
        if let Some(list) = self.pending.remove(&index) {
            for w in list {
                let _ = w.send(match &result {
                    Ok(idx) => Ok(*idx),
                    Err(_) => Err(clone_err(&result)),
                });
            }
        }
    }

    fn default_deny_vote(&self) -> VoteResponse {
        VoteResponse {
            cluster_id: self.node.config.cluster_id.clone(),
            leader_epoch: self.node.hard_state.current_term.0,
            term: self.node.hard_state.current_term,
            vote_granted: false,
            leader_hint: self.node.leader_id,
        }
    }

    fn default_deny_pre_vote(&self) -> PreVoteResponse {
        PreVoteResponse {
            cluster_id: self.node.config.cluster_id.clone(),
            leader_epoch: self.node.hard_state.current_term.0,
            term: self.node.hard_state.current_term,
            vote_granted: false,
            leader_hint: self.node.leader_id,
        }
    }

    fn default_deny_fetch(&self) -> FetchResponse {
        FetchResponse {
            cluster_id: self.node.config.cluster_id.clone(),
            leader_epoch: self.node.hard_state.current_term.0,
            leader_id: self.node.leader_id.unwrap_or(self.node.id),
            high_watermark: self.node.commit_index,
            entries: Vec::new(),
            diverging_epoch: None,
        }
    }

    /// Graceful drain: after the shutdown signal, before final flush.
    ///
    /// Closes `events_rx` (no new submissions) and processes any
    /// already-buffered events + outbound results until both channels
    /// are empty OR the configured `shutdown_drain_deadline` expires.
    ///
    /// Inbound RPCs are processed normally so callers see correct
    /// responses rather than `driver dropped reply` errors. Client
    /// commands are rejected with `XRaftError::Shutdown` — we will
    /// not accept new proposals on a node that is stepping down.
    /// Ticks are NOT scheduled during drain.
    async fn graceful_drain(&mut self) {
        self.events_rx.close();
        info!(
            target: "xraft_server::driver",
            pending_waiters = self.pending.len(),
            in_flight = self.router.in_flight(),
            "draining queued events"
        );
        let deadline = tokio::time::sleep(self.config.shutdown_drain_deadline);
        tokio::pin!(deadline);
        loop {
            // Persistence failure during drain immediately escalates to
            // fail-stop — do not continue processing.
            if self.halt_reason.is_some() {
                return;
            }
            tokio::select! {
                biased;

                _ = &mut deadline => {
                    warn!(
                        target: "xraft_server::driver",
                        "drain deadline reached; abandoning buffered events"
                    );
                    return;
                }

                evt = self.events_rx.recv() => match evt {
                    Some(DriverEvent::Inbound(rpc)) => self.handle_inbound(rpc).await,
                    Some(DriverEvent::Client(cmd)) => {
                        // Reject new proposals during drain: the leader
                        // is stepping down, accepting a propose would
                        // create new log/durability/network obligations
                        // a node about to exit cannot satisfy.
                        let _ = cmd.reply.send(Err(XRaftError::Shutdown));
                    }
                    None => break,
                },

                Some(res) = self.outbound_rx.recv() => {
                    self.handle_outbound_result(res).await;
                }

                _ = self.router.reap_one(), if self.router.in_flight() > 0 => {}
            }
        }
        // Best-effort: drain any outbound results that arrived after
        // events_rx closed but before this loop noticed.
        while let Ok(res) = self.outbound_rx.try_recv() {
            self.handle_outbound_result(res).await;
            if self.halt_reason.is_some() {
                return;
            }
        }
    }

    /// Fail-stop shutdown path used when a persistence operation
    /// failed mid-action-list. The Raft driver contract
    /// (`xraft-core/src/node.rs` §"Driver contract") REQUIRES halting:
    /// partial application of an action list after a persist failure is
    /// unsafe — the operator must restart the node and recovery will
    /// proceed from durable state.
    ///
    /// Unlike graceful drain we do NOT attempt a final persist (the
    /// persistence layer is the thing that just failed) and we do NOT
    /// wait for in-flight outbound tasks — they are aborted immediately.
    /// Buffered events and pending waiters receive the halt error so
    /// callers do not hang.
    async fn fail_stop_shutdown(&mut self) -> XResult<()> {
        let reason = self
            .halt_reason
            .take()
            .expect("fail_stop_shutdown called with no halt reason");
        error!(
            target: "xraft_server::driver",
            reason = %reason,
            pending_waiters = self.pending.len(),
            in_flight = self.router.in_flight(),
            "fail-stop shutdown"
        );

        // Close the inbound channel and drain any buffered events,
        // replying to each with the halt error so RPC callers see a
        // consistent failure rather than a dropped oneshot.
        self.events_rx.close();
        while let Ok(event) = self.events_rx.try_recv() {
            match event {
                DriverEvent::Inbound(rpc) => self.reply_halt_to_inbound(rpc, &reason),
                DriverEvent::Client(cmd) => {
                    let _ = cmd.reply.send(Err(XRaftError::Storage(reason.clone())));
                }
            }
        }

        // Abort in-flight outbound tasks immediately — we cannot
        // safely wait, persistence is broken.
        self.router.abort_all_now().await;

        // Fail all pending client waiters with the halt error.
        let waiters = std::mem::take(&mut self.pending);
        for (_idx, list) in waiters {
            for w in list {
                let _ = w.send(Err(XRaftError::Storage(reason.clone())));
            }
        }

        Err(XRaftError::Storage(reason))
    }

    /// Send the halt error to a buffered inbound RPC's reply oneshot.
    fn reply_halt_to_inbound(&self, rpc: InboundRpc, reason: &str) {
        let err = XRaftError::Storage(reason.to_string());
        match rpc {
            InboundRpc::Vote { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            InboundRpc::PreVote { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            InboundRpc::Fetch { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            InboundRpc::FetchSnapshot { reply, .. } => {
                let _ = reply.send(Err(err));
            }
        }
    }

    /// Graceful shutdown:
    /// 1. Close the inbound event channel (no new inbound RPCs).
    /// 2. Drain in-flight outbound RPC tasks alongside `outbound_rx`
    ///    so completed responses are folded back into the node before
    ///    the final persist. Bounded by `shutdown_drain_deadline`.
    /// 3. Persist final hard state and flush the log. ANY failure
    ///    surfaces as `Err(XRaftError::Storage(_))` (the Raft driver
    ///    contract requires durability on every commit/state change
    ///    AND on final shutdown — silently dropping a final persist
    ///    failure would let the next start-up replay from a stale
    ///    durable view).
    /// 4. Fail every pending client waiter — with `Storage` if final
    ///    persist failed, otherwise `Shutdown`.
    /// 5. Returns `Ok(())` on clean exit; `Err(Storage(_))` when
    ///    the final persist or flush failed, or when a late outbound
    ///    result triggered a halt-class `PersistHardState` action.
    async fn shutdown_sequence(&mut self) -> XResult<()> {
        self.events_rx.close();
        info!(
            target: "xraft_server::driver",
            pending_waiters = self.pending.len(),
            in_flight = self.router.in_flight(),
            "draining"
        );

        // Drain router tasks and `outbound_rx` in lock-step. We must
        // process outbound results BEFORE, DURING (via the select!),
        // and AFTER the router drain so a completed Vote/Fetch
        // response that produced a `node.step` advance is folded into
        // hard_state before we persist it. Without the inner select!,
        // a response delivered while we are blocked in `reap_one()`
        // would sit in `outbound_rx` until the drain completes — at
        // which point the only sweep is the post-drain `try_recv()`
        // (which works) but a *single* completed-task slot in
        // `outbound_rx` could backpressure the spawned task and
        // delay shutdown.
        let deadline = tokio::time::sleep(self.config.shutdown_drain_deadline);
        tokio::pin!(deadline);
        loop {
            // First, fold any already-queued outbound results.
            while let Ok(res) = self.outbound_rx.try_recv() {
                self.handle_outbound_result(res).await;
                if self.halt_reason.is_some() {
                    return self.fail_stop_shutdown().await;
                }
            }
            if self.router.in_flight() == 0 {
                break;
            }
            tokio::select! {
                biased;
                _ = &mut deadline => {
                    warn!(
                        target: "xraft_server::driver",
                        in_flight = self.router.in_flight(),
                        "shutdown drain deadline reached; aborting outbound tasks"
                    );
                    break;
                }
                _ = self.router.reap_one() => {}
                Some(res) = self.outbound_rx.recv() => {
                    self.handle_outbound_result(res).await;
                    if self.halt_reason.is_some() {
                        return self.fail_stop_shutdown().await;
                    }
                }
            }
        }
        // Final sweep — anything that arrived between the last
        // `try_recv` and the loop exit, plus anything still buffered
        // after the deadline-induced break.
        while let Ok(res) = self.outbound_rx.try_recv() {
            self.handle_outbound_result(res).await;
            if self.halt_reason.is_some() {
                return self.fail_stop_shutdown().await;
            }
        }
        // Abort any tasks that survived the deadline.
        self.router.abort_all_now().await;

        // Final persistence. ANY failure is a halt-class failure: the
        // driver contract requires that durable state matches the
        // in-memory state we are about to drop. We capture the FIRST
        // failure (persist before flush) and propagate as Err.
        let final_err: Option<String> = match self.hs_store.persist(&self.node.hard_state) {
            Ok(()) => match self.log_store.flush() {
                Ok(()) => None,
                Err(e) => {
                    warn!(target: "xraft_server::driver", error = %e, "final log flush failed");
                    Some(format!("final log flush failed: {e}"))
                }
            },
            Err(e) => {
                warn!(target: "xraft_server::driver", error = %e, "final hard-state persist failed");
                Some(format!("final hard-state persist failed: {e}"))
            }
        };

        // Fail every pending client waiter. On clean shutdown they
        // get `Err(Shutdown)`; on final-persist failure they get the
        // same `Storage(msg)` so the caller can correlate.
        let waiters = std::mem::take(&mut self.pending);
        for (_idx, list) in waiters {
            for w in list {
                let _ = match &final_err {
                    Some(msg) => w.send(Err(XRaftError::Storage(msg.clone()))),
                    None => w.send(Err(XRaftError::Shutdown)),
                };
            }
        }

        match final_err {
            Some(msg) => {
                error!(target: "xraft_server::driver", reason = %msg, "driver loop exited with final-persist failure");
                Err(XRaftError::Storage(msg))
            }
            None => {
                info!(target: "xraft_server::driver", "driver loop exited cleanly");
                Ok(())
            }
        }
    }
}

/// Captured inbound-RPC response. The driver fills in one variant from
/// the matching `Action::SendMessage` / `Action::ServeFetch` while
/// processing inbound-RPC actions.
///
/// `error` is set when an action that the inbound reply DEPENDS ON
/// (`PersistHardState`, `AppendEntries`, `TruncateLog`, log read for
/// `ServeFetch`) fails. Inbound handlers MUST surface that error to
/// the caller rather than returning a captured-but-unsafe response —
/// most importantly, a granted `VoteResponse` whose backing
/// `PersistHardState` failed would violate the Raft single-vote-per-
/// term safety invariant on crash + restart.
#[derive(Default)]
struct CapturedResponse {
    vote: Option<VoteResponse>,
    pre_vote: Option<PreVoteResponse>,
    fetch: Option<FetchResponse>,
    error: Option<XRaftError>,
}

impl CapturedResponse {
    fn merge(&mut self, other: CapturedResponse) {
        if self.vote.is_none() {
            self.vote = other.vote;
        }
        if self.pre_vote.is_none() {
            self.pre_vote = other.pre_vote;
        }
        if self.fetch.is_none() {
            self.fetch = other.fetch;
        }
        if self.error.is_none() {
            self.error = other.error;
        }
    }
}

/// In-memory adapter that turns a pre-collected `VecDeque` of chunk
/// results into a [`SnapshotChunkStream`].
///
/// Used by the inbound `FetchSnapshot` handler to wrap the eager
/// chunk collection from `SnapshotStore::snapshot_reader_from_offset`
/// into the trait-required stream shape. Stage 4.2 collects eagerly
/// because snapshots are small in-memory payloads; a future phase can
/// replace this with a lazy reader once the snapshot install pipeline
/// requires backpressure.
struct StaticChunkStream {
    chunks: std::collections::VecDeque<XResult<FetchSnapshotChunk>>,
}

impl Stream for StaticChunkStream {
    type Item = XResult<FetchSnapshotChunk>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.chunks.pop_front())
    }
}

/// `XRaftError` does not implement `Clone` (some variants wrap non-Clone
/// data). For waiter-resolution paths that fan one error out to many
/// senders we round-trip through `to_string`.
fn clone_err(err: &XResult<LogIndex>) -> XRaftError {
    match err {
        Ok(_) => XRaftError::Transport("internal: clone_err called on Ok".into()),
        Err(e) => match e {
            XRaftError::Storage(s) => XRaftError::Storage(s.clone()),
            XRaftError::Transport(s) => XRaftError::Transport(s.clone()),
            XRaftError::NotLeader { leader_hint } => XRaftError::NotLeader {
                leader_hint: *leader_hint,
            },
            XRaftError::ElectionTimeout => XRaftError::ElectionTimeout,
            XRaftError::InvalidTerm(s) => XRaftError::InvalidTerm(s.clone()),
            XRaftError::LogInconsistency(s) => XRaftError::LogInconsistency(s.clone()),
            XRaftError::Shutdown => XRaftError::Shutdown,
            XRaftError::Config(s) => XRaftError::Config(s.clone()),
            XRaftError::CorruptSnapshot(s) => XRaftError::CorruptSnapshot(s.clone()),
            XRaftError::SnapshotNotFound(s) => XRaftError::SnapshotNotFound(s.clone()),
        },
    }
}

/// Silence unused-warning for the `Entry` import in case future stages
/// re-shape the matchers. Currently `Entry` is referenced via
/// `EntryPayload` patterns above; keeping the explicit `use` makes the
/// dependency clear.
#[allow(dead_code)]
fn _entry_type_marker(_e: &Entry) {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // Test helpers below scaffold negative-path coverage (failure injection,
    // snapshot bytes capture, etc.) that not every test consumes today.
    // Silenced rather than deleted so additional tests can wire them up
    // without re-introducing the helpers.
    #![allow(dead_code, clippy::type_complexity)]
    use super::*;
    use std::sync::Mutex;
    use uuid::Uuid;

    use xraft_core::config::ClusterConfig;
    use xraft_core::storage::SnapshotMeta;
    use xraft_core::transport::SnapshotChunkStream;
    use xraft_core::types::HardState;

    // -----------------------------------------------------------------
    // Test doubles
    // -----------------------------------------------------------------

    #[derive(Default)]
    struct TestLogStore {
        entries: Vec<Entry>,
        /// When set true, the next call to `get_range` returns an error.
        /// Auto-clears after firing once so subsequent calls succeed.
        /// Used by `apply_committed` failure-path tests.
        fail_next_get_range: Arc<std::sync::atomic::AtomicBool>,
        /// When set true, the next call to `term_at` returns an error.
        /// Used by `TakeSnapshot` failure-path tests.
        fail_next_term_at: Arc<std::sync::atomic::AtomicBool>,
        /// When set true, the next call to `term_at` returns `Ok(None)`.
        /// Used to exercise the "log tip missing for last_applied" halt
        /// path on `Action::TakeSnapshot` without needing a real
        /// truncation race.
        missing_next_term_at: Arc<std::sync::atomic::AtomicBool>,
        /// Indices to silently drop from any `get_range` response.
        /// Simulates a buggy or partially-truncated store returning a
        /// short / gapped range — used to exercise the
        /// `apply_committed` range-validation halt path.
        drop_indices_in_get_range: Arc<Mutex<std::collections::HashSet<LogIndex>>>,
    }

    impl LogStore for TestLogStore {
        fn append(&mut self, entries: &[Entry]) -> XResult<()> {
            for e in entries {
                self.entries.push(e.clone());
            }
            Ok(())
        }
        fn get(&self, index: LogIndex) -> XResult<Option<Entry>> {
            Ok(self.entries.iter().find(|e| e.index == index).cloned())
        }
        fn get_range(&self, start: LogIndex, end: LogIndex) -> XResult<Vec<Entry>> {
            if self
                .fail_next_get_range
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(XRaftError::Storage(format!(
                    "injected get_range failure for {start}..{end}"
                )));
            }
            let drop = self.drop_indices_in_get_range.lock().unwrap().clone();
            Ok(self
                .entries
                .iter()
                .filter(|e| e.index >= start && e.index < end && !drop.contains(&e.index))
                .cloned()
                .collect())
        }
        fn last_index(&self) -> LogIndex {
            self.entries.last().map(|e| e.index).unwrap_or(LogIndex(0))
        }
        fn last_term(&self) -> Term {
            self.entries.last().map(|e| e.term).unwrap_or(Term(0))
        }
        fn truncate_from(&mut self, index: LogIndex) -> XResult<()> {
            self.entries.retain(|e| e.index < index);
            Ok(())
        }
        fn term_at(&self, index: LogIndex) -> XResult<Option<Term>> {
            if self
                .fail_next_term_at
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(XRaftError::Storage(format!(
                    "injected term_at failure for {index}"
                )));
            }
            if self
                .missing_next_term_at
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(None);
            }
            Ok(self
                .entries
                .iter()
                .find(|e| e.index == index)
                .map(|e| e.term))
        }
        fn flush(&mut self) -> XResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestHardStateStore {
        state: Option<HardState>,
        persist_count: std::sync::atomic::AtomicUsize,
        fail_next_persist: Arc<std::sync::atomic::AtomicBool>,
    }

    impl HardStateStore for TestHardStateStore {
        fn persist(&mut self, state: &HardState) -> XResult<()> {
            if self
                .fail_next_persist
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(XRaftError::Storage("injected persist failure".into()));
            }
            self.state = Some(state.clone());
            self.persist_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn load(&self) -> XResult<Option<HardState>> {
            Ok(self.state.clone())
        }
    }

    #[derive(Default, Clone)]
    struct TestSnapshotStore {
        saved: Arc<Mutex<Vec<(SnapshotMeta, Vec<u8>)>>>,
        fail_next_save: Arc<std::sync::atomic::AtomicBool>,
    }

    impl TestSnapshotStore {
        fn saved_snapshots(&self) -> Vec<(SnapshotMeta, Vec<u8>)> {
            self.saved.lock().unwrap().clone()
        }
    }

    impl SnapshotStore for TestSnapshotStore {
        fn save_snapshot(&mut self, metadata: SnapshotMeta, data: &[u8]) -> XResult<()> {
            if self
                .fail_next_save
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(XRaftError::Storage("injected save_snapshot failure".into()));
            }
            self.saved.lock().unwrap().push((metadata, data.to_vec()));
            Ok(())
        }
        fn load_latest_snapshot(&self) -> XResult<Option<(SnapshotMeta, Vec<u8>)>> {
            Ok(self.saved.lock().unwrap().last().cloned())
        }
        fn list_snapshots(&self) -> XResult<Vec<SnapshotMeta>> {
            Ok(self
                .saved
                .lock()
                .unwrap()
                .iter()
                .map(|(m, _)| m.clone())
                .collect())
        }
        fn delete_snapshot(&mut self, _id: &str) -> XResult<()> {
            Ok(())
        }
        fn snapshot_exists(&self, _index: LogIndex, _term: Term) -> bool {
            false
        }
    }

    type Applied = Arc<Mutex<Vec<(LogIndex, Vec<u8>)>>>;
    type TestDriver = Driver<
        NoopTransport,
        TestLogStore,
        TestHardStateStore,
        TestSnapshotStore,
        TestStateMachine,
    >;

    #[derive(Default, Clone)]
    struct TestStateMachine {
        applied: Applied,
        /// Bytes returned from `snapshot()`. Tests that exercise
        /// `TakeSnapshot` populate this so they can assert the data
        /// reached `SnapshotStore::save_snapshot` byte-for-byte.
        snapshot_bytes: Arc<Mutex<Vec<u8>>>,
        /// Records every payload passed to `restore()`. Tests that
        /// exercise `InstallSnapshot` assert that the leader-supplied
        /// bytes reached the state machine unchanged.
        restored: Arc<Mutex<Vec<Vec<u8>>>>,
        /// When `Some(idx)`, `apply` returns `Err` for entries at that
        /// log index. Used by `apply_committed` failure-path tests.
        fail_apply_at: Arc<Mutex<Option<LogIndex>>>,
        /// When true, the next call to `snapshot()` returns `Err`.
        /// Cleared after firing so subsequent calls succeed.
        fail_next_snapshot: Arc<std::sync::atomic::AtomicBool>,
        /// When true, the next call to `restore()` returns `Err`.
        /// Cleared after firing so subsequent calls succeed.
        fail_next_restore: Arc<std::sync::atomic::AtomicBool>,
    }

    impl TestStateMachine {
        fn snapshot_handle(&self) -> Applied {
            self.applied.clone()
        }

        fn restored_snapshots(&self) -> Vec<Vec<u8>> {
            self.restored.lock().unwrap().clone()
        }

        fn set_snapshot_bytes(&self, bytes: Vec<u8>) {
            *self.snapshot_bytes.lock().unwrap() = bytes;
        }

        fn arm_fail_apply_at(&self, index: LogIndex) {
            *self.fail_apply_at.lock().unwrap() = Some(index);
        }

        fn arm_fail_next_snapshot(&self) {
            self.fail_next_snapshot
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn arm_fail_next_restore(&self) {
            self.fail_next_restore
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl StateMachine for TestStateMachine {
        fn apply(&mut self, index: LogIndex, command: &[u8]) -> XResult<Vec<u8>> {
            if let Some(fail_idx) = *self.fail_apply_at.lock().unwrap()
                && fail_idx == index
            {
                return Err(XRaftError::Storage(format!(
                    "injected apply failure at {index}"
                )));
            }
            self.applied.lock().unwrap().push((index, command.to_vec()));
            Ok(Vec::new())
        }
        fn query(&self, _query: &[u8]) -> XResult<Vec<u8>> {
            Ok(Vec::new())
        }
        fn snapshot(&self) -> XResult<Vec<u8>> {
            if self
                .fail_next_snapshot
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(XRaftError::Storage("injected snapshot failure".into()));
            }
            Ok(self.snapshot_bytes.lock().unwrap().clone())
        }
        fn restore(&mut self, snapshot: &[u8]) -> XResult<()> {
            if self
                .fail_next_restore
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(XRaftError::Storage("injected restore failure".into()));
            }
            self.restored.lock().unwrap().push(snapshot.to_vec());
            Ok(())
        }
    }

    /// Transport stub that records outbound sends and returns synthetic
    /// errors (peer unreachable). Tests in this file only exercise the
    /// single-voter-cluster path so the transport is never actually
    /// invoked for real network IO.
    #[derive(Default)]
    struct NoopTransport {
        outbound_count: std::sync::atomic::AtomicUsize,
    }

    impl Transport for NoopTransport {
        fn send_vote(
            &self,
            _to: NodeId,
            _request: VoteRequest,
        ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
            self.outbound_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(XRaftError::Transport("noop transport".into())) }
        }
        fn send_pre_vote(
            &self,
            _to: NodeId,
            _request: PreVoteRequest,
        ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
            self.outbound_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(XRaftError::Transport("noop transport".into())) }
        }
        fn send_fetch(
            &self,
            _to: NodeId,
            _request: FetchRequest,
        ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
            self.outbound_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(XRaftError::Transport("noop transport".into())) }
        }
        fn send_fetch_snapshot(
            &self,
            _to: NodeId,
            _request: FetchSnapshotRequest,
        ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
            self.outbound_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(XRaftError::Transport("noop transport".into())) }
        }
        #[allow(clippy::manual_async_fn)]
        fn start_server(
            self: Arc<Self>,
        ) -> impl std::future::Future<Output = XResult<()>> + Send + 'static {
            async { Ok(()) }
        }
    }

    /// Build a single-voter cluster config. Single-voter is a self-quorum
    /// so election + commit are instantaneous, exercising every Action
    /// variant without needing peer traffic.
    fn single_voter_config(tick_ms: u64) -> ClusterConfig {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test-driver"
listen_addr = "127.0.0.1:6900"
tick_interval_ms = {tick_ms}
election_timeout_min_ms = {min}
election_timeout_max_ms = {max}
fetch_interval_ms = {fetch}

[[voters]]
node_id = 1
directory_id = "{dir}"
host = "127.0.0.1"
port = 6000
"#,
            tick_ms = tick_ms,
            min = tick_ms * 2,
            max = tick_ms * 3,
            fetch = tick_ms * 5,
            dir = Uuid::new_v4()
        );
        ClusterConfig::from_toml_str(&toml).expect("single-voter config parses")
    }

    fn build_driver(config: ClusterConfig) -> (TestDriver, DriverHandle, Applied) {
        let (driver, handle, applied, _) = build_driver_with_persist_fail(config);
        (driver, handle, applied)
    }

    fn build_driver_with_persist_fail(
        config: ClusterConfig,
    ) -> (
        TestDriver,
        DriverHandle,
        Applied,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let node = RaftNode::new_with_seed(config, 1234).expect("RaftNode ctor");
        let log = TestLogStore::default();
        let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hs = TestHardStateStore {
            fail_next_persist: fail_flag.clone(),
            ..Default::default()
        };
        let ss = TestSnapshotStore::default();
        let sm = TestStateMachine::default();
        let applied = sm.snapshot_handle();
        let transport = Arc::new(NoopTransport::default());
        let driver = Driver::new(
            node,
            log,
            hs,
            ss,
            sm,
            transport,
            DriverConfig {
                tick_interval: Duration::from_millis(2),
                max_fetch_batch: 8,
                shutdown_drain_deadline: Duration::from_secs(2),
                fetch_snapshot_deadline: Duration::from_secs(2),
            },
        );
        let handle = driver.handle();
        (driver, handle, applied, fail_flag)
    }

    /// Same as `build_driver` but injects `extra_peer` into the node's
    /// `peers` map before driver construction. Used by fencing tests
    /// that need a known voter / tracked peer to pass the membership
    /// check on FetchSnapshot. The injection survives `become_leader`
    /// because the engine never re-populates `peers` from `voter_set`
    /// after `RaftNode::new_with_seed`.
    fn build_driver_with_known_peer(
        config: ClusterConfig,
        extra_peer: NodeId,
    ) -> (TestDriver, DriverHandle, Applied) {
        let mut node = RaftNode::new_with_seed(config, 1234).expect("RaftNode ctor");
        node.peers
            .insert(extra_peer, xraft_core::PeerState::new(true));
        let log = TestLogStore::default();
        let hs = TestHardStateStore::default();
        let ss = TestSnapshotStore::default();
        let sm = TestStateMachine::default();
        let applied = sm.snapshot_handle();
        let transport = Arc::new(NoopTransport::default());
        let driver = Driver::new(
            node,
            log,
            hs,
            ss,
            sm,
            transport,
            DriverConfig {
                tick_interval: Duration::from_millis(2),
                max_fetch_batch: 8,
                shutdown_drain_deadline: Duration::from_secs(2),
                fetch_snapshot_deadline: Duration::from_secs(2),
            },
        );
        let handle = driver.handle();
        (driver, handle, applied)
    }

    // -----------------------------------------------------------------
    // Scenario: driver-processes-tick
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn driver_processes_tick_drives_election() {
        let cfg = single_voter_config(2);
        let (driver, handle, applied) = build_driver(cfg);

        let run_task = tokio::spawn(driver.run());

        // Advance virtual time past the election timeout so the tick
        // task fires Input::Tick → PreCandidate → Candidate → Leader.
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Submit a noop client command to confirm the driver is now
        // accepting proposals as the leader.
        let propose = handle.propose(Bytes::from_static(b"hello"));
        let index = tokio::time::timeout(Duration::from_secs(2), propose)
            .await
            .expect("propose did not complete within 2s")
            .expect("propose returned error");

        // First applied entry should be the no-op at index 1 (from
        // become_leader); the proposed command is at index 2.
        let applied_snapshot = applied.lock().unwrap().clone();
        assert!(
            !applied_snapshot.is_empty(),
            "state machine should have applied entries after leader election + propose"
        );
        // The command is the only non-NoOp entry — it should match.
        assert_eq!(
            applied_snapshot.last().unwrap().0,
            index,
            "last applied index must equal proposed index"
        );
        assert_eq!(applied_snapshot.last().unwrap().1, b"hello".to_vec());

        handle.shutdown();
        run_task.await.expect("run() panicked").expect("run() err");
    }

    // -----------------------------------------------------------------
    // Scenario: driver-handles-shutdown
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn driver_handles_shutdown_within_deadline() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        // Let the driver enter the loop and elect itself.
        tokio::time::sleep(Duration::from_millis(50)).await;

        handle.shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("driver run did not exit within 5s")
            .expect("run task panicked");
        assert!(result.is_ok(), "driver.run returned error: {result:?}");
    }

    // -----------------------------------------------------------------
    // Scenario: client-command-flow
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn client_command_flow_appends_and_resolves_on_commit() {
        let cfg = single_voter_config(2);
        let (driver, handle, applied) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        // Wait for self-election.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Submit a command and assert the future resolves with the
        // committed LogIndex.
        let commit_index = tokio::time::timeout(
            Duration::from_secs(2),
            handle.propose(Bytes::from_static(b"cmd-1")),
        )
        .await
        .expect("propose timed out")
        .expect("propose returned error");
        assert!(
            commit_index.0 >= 2,
            "expected committed index >= 2 (after no-op @ 1), got {commit_index}"
        );

        let applied_snapshot = applied.lock().unwrap().clone();
        assert!(
            applied_snapshot
                .iter()
                .any(|(idx, payload)| *idx == commit_index && payload == b"cmd-1"),
            "state machine should have applied the proposed command at index {commit_index}"
        );

        handle.shutdown();
        run_task.await.expect("run() panicked").expect("run() err");
    }

    // -----------------------------------------------------------------
    // Helper sanity check: propose returns NotLeader on followers.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn propose_on_follower_returns_not_leader() {
        // Three-voter config: this node won't elect itself without
        // peer pre-votes, so it stays a (Pre)Candidate / Follower and
        // never enters Leader role.
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test-driver"
listen_addr = "127.0.0.1:6901"
tick_interval_ms = 2
election_timeout_min_ms = 200
election_timeout_max_ms = 400
fetch_interval_ms = 10

[[voters]]
node_id = 1
directory_id = "{}"
host = "127.0.0.1"
port = 6000

[[voters]]
node_id = 2
directory_id = "{}"
host = "127.0.0.1"
port = 6001

[[voters]]
node_id = 3
directory_id = "{}"
host = "127.0.0.1"
port = 6002
"#,
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let cfg = ClusterConfig::from_toml_str(&toml).expect("three-voter config parses");
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        // Don't sleep long enough for an election to succeed; the
        // engine never reaches Leader without peer pre-votes.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            handle.propose(Bytes::from_static(b"x")),
        )
        .await;
        match result {
            Ok(Err(XRaftError::NotLeader { .. })) => {}
            Ok(other) => panic!("expected NotLeader, got {other:?}"),
            Err(_) => panic!("propose timed out while waiting for NotLeader reply"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    // -----------------------------------------------------------------
    // FetchSnapshot inbound serving (Stage 4.2 — real implementation).
    // -----------------------------------------------------------------

    /// Missing snapshot → `XRaftError::SnapshotNotFound`.
    ///
    /// Stage 4.2 fencing requires the requester to be a voter or a tracked
    /// peer before reaching the snapshot-id lookup. We inject NodeId(2) as
    /// a tracked peer so the request passes the membership check and the
    /// test exercises the actual not-found code path.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_snapshot_unknown_id_returns_not_found() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver_with_known_peer(cfg, NodeId(2));
        let run_task = tokio::spawn(driver.run());

        tokio::time::sleep(Duration::from_millis(20)).await;

        let inbound = handle.inbound_handler();
        let result = inbound
            .handle_fetch_snapshot(FetchSnapshotRequest {
                cluster_id: "test-driver".into(),
                leader_epoch: 1,
                replica_id: NodeId(2),
                snapshot_id: "does-not-exist".into(),
                offset: 0,
                max_bytes: 0,
            })
            .await;
        match result {
            Err(XRaftError::SnapshotNotFound(id)) => assert_eq!(id, "does-not-exist"),
            Err(other) => panic!("expected SnapshotNotFound, got {other:?}"),
            Ok(_) => panic!("expected SnapshotNotFound, got Ok(stream)"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    /// Fencing: FetchSnapshot from an unknown replica (not a voter and
    /// not a tracked peer) is rejected before reaching the snapshot
    /// store. Mirrors the trust-boundary check applied to FetchRequest
    /// in the engine (`xraft-core/src/node.rs` `handle_fetch_request`).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_snapshot_unknown_replica_rejected() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        tokio::time::sleep(Duration::from_millis(20)).await;

        let inbound = handle.inbound_handler();
        let result = inbound
            .handle_fetch_snapshot(FetchSnapshotRequest {
                cluster_id: "test-driver".into(),
                leader_epoch: 1,
                replica_id: NodeId(99),
                snapshot_id: "irrelevant".into(),
                offset: 0,
                max_bytes: 0,
            })
            .await;
        match result {
            Err(XRaftError::Transport(msg)) => assert!(
                msg.contains("unknown replica") && msg.contains("99"),
                "expected Transport(unknown replica ...99...), got: {msg}"
            ),
            Err(other) => panic!("expected Transport(unknown replica), got Err({other:?})"),
            Ok(_) => panic!("expected Transport(unknown replica), got Ok(stream)"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    /// Fencing: FetchSnapshot whose `replica_id` is our own id is a
    /// self-loopback and must be rejected. A leader never serves a
    /// snapshot to itself.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_snapshot_self_loopback_rejected() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        tokio::time::sleep(Duration::from_millis(20)).await;

        let inbound = handle.inbound_handler();
        let result = inbound
            .handle_fetch_snapshot(FetchSnapshotRequest {
                cluster_id: "test-driver".into(),
                leader_epoch: 1,
                replica_id: NodeId(1),
                snapshot_id: "irrelevant".into(),
                offset: 0,
                max_bytes: 0,
            })
            .await;
        match result {
            Err(XRaftError::Transport(msg)) => assert!(
                msg.contains("self-loopback"),
                "expected Transport(self-loopback ...), got: {msg}"
            ),
            Err(other) => panic!("expected Transport(self-loopback), got Err({other:?})"),
            Ok(_) => panic!("expected Transport(self-loopback), got Ok(stream)"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    /// Fencing: a Follower receiving FetchSnapshot must return
    /// `NotLeader` so the caller can re-discover the leader. We use a
    /// 3-voter config without any responding peers — node 1 cannot
    /// reach quorum, so it stays in Follower / PreCandidate / Candidate
    /// (never Leader) and the role check fires.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_snapshot_not_leader_rejected() {
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test-driver"
listen_addr = "127.0.0.1:6908"
tick_interval_ms = 2
election_timeout_min_ms = 800
election_timeout_max_ms = 1000
fetch_interval_ms = 10

[[voters]]
node_id = 1
directory_id = "{}"
host = "127.0.0.1"
port = 6020

[[voters]]
node_id = 2
directory_id = "{}"
host = "127.0.0.1"
port = 6021

[[voters]]
node_id = 3
directory_id = "{}"
host = "127.0.0.1"
port = 6022
"#,
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let cfg = ClusterConfig::from_toml_str(&toml).expect("three-voter config parses");
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        // Brief settle — keep below election timeout so node stays
        // Follower (and even if it advances to PreCandidate, it has no
        // peer to confirm quorum and never reaches Leader).
        tokio::time::sleep(Duration::from_millis(10)).await;

        let inbound = handle.inbound_handler();
        let result = inbound
            .handle_fetch_snapshot(FetchSnapshotRequest {
                cluster_id: "test-driver".into(),
                leader_epoch: 0,
                replica_id: NodeId(2),
                snapshot_id: "irrelevant".into(),
                offset: 0,
                max_bytes: 0,
            })
            .await;
        match result {
            Err(XRaftError::NotLeader { .. }) => {}
            Err(other) => panic!("expected NotLeader, got Err({other:?})"),
            Ok(_) => panic!("expected NotLeader, got Ok(stream)"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    /// Fencing: leader_epoch must strictly equal our current_term. A
    /// stale or future caller gets `Transport(leader_epoch mismatch)`
    /// without mutating our node state.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_snapshot_stale_leader_epoch_rejected() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver_with_known_peer(cfg, NodeId(2));
        let run_task = tokio::spawn(driver.run());

        tokio::time::sleep(Duration::from_millis(20)).await;

        let inbound = handle.inbound_handler();
        // current_term after single-voter self-election = 1. Send a
        // wildly stale leader_epoch=99 to trigger the mismatch.
        let result = inbound
            .handle_fetch_snapshot(FetchSnapshotRequest {
                cluster_id: "test-driver".into(),
                leader_epoch: 99,
                replica_id: NodeId(2),
                snapshot_id: "irrelevant".into(),
                offset: 0,
                max_bytes: 0,
            })
            .await;
        match result {
            Err(XRaftError::Transport(msg)) => assert!(
                msg.contains("leader_epoch mismatch"),
                "expected Transport(leader_epoch mismatch), got: {msg}"
            ),
            Err(other) => panic!("expected Transport(leader_epoch mismatch), got Err({other:?})"),
            Ok(_) => panic!("expected Transport(leader_epoch mismatch), got Ok(stream)"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    /// Wrong cluster id → `XRaftError::Transport` (cluster mismatch).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_snapshot_wrong_cluster_id_returns_transport_error() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());

        tokio::time::sleep(Duration::from_millis(20)).await;

        let inbound = handle.inbound_handler();
        let result = inbound
            .handle_fetch_snapshot(FetchSnapshotRequest {
                cluster_id: "OTHER-CLUSTER".into(),
                leader_epoch: 1,
                replica_id: NodeId(2),
                snapshot_id: "snapshot-0000000001-00000000000000000001".into(),
                offset: 0,
                max_bytes: 0,
            })
            .await;
        match result {
            Err(XRaftError::Transport(msg)) => assert!(
                msg.contains("cluster_id mismatch"),
                "expected Transport(cluster_id mismatch), got Transport({msg})"
            ),
            Err(other) => panic!("expected Transport(cluster_id mismatch), got {other:?}"),
            Ok(_) => panic!("expected Transport(cluster_id mismatch), got Ok(stream)"),
        }

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    // -----------------------------------------------------------------
    // Helper sanity check: inbound Vote always replies (default-deny on drop).
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn inbound_vote_default_deny_when_node_drops() {
        let cfg = single_voter_config(2);
        let (driver, handle, _) = build_driver(cfg);
        let run_task = tokio::spawn(driver.run());
        tokio::time::sleep(Duration::from_millis(20)).await;

        let inbound = handle.inbound_handler();
        // Foreign cluster — node drops silently; driver must
        // synthesise a default-deny response.
        let resp = tokio::time::timeout(
            Duration::from_secs(2),
            inbound.handle_vote(VoteRequest {
                cluster_id: "OTHER".into(),
                leader_epoch: 0,
                term: Term(99),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }),
        )
        .await
        .expect("handle_vote timed out")
        .expect("handle_vote returned XRaftError");
        assert!(!resp.vote_granted, "default-deny vote must be ungranted");

        let pre = tokio::time::timeout(
            Duration::from_secs(2),
            inbound.handle_pre_vote(PreVoteRequest {
                cluster_id: "OTHER".into(),
                leader_epoch: 0,
                next_term: Term(99),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }),
        )
        .await
        .expect("handle_pre_vote timed out")
        .expect("handle_pre_vote returned XRaftError");
        assert!(!pre.vote_granted);

        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), run_task).await;
    }

    // -----------------------------------------------------------------
    // Safety: PersistHardState failure must propagate to inbound Vote
    // reply as an error — a granted Vote whose hard state is not
    // durable would break Raft election safety on crash + restart.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn inbound_vote_returns_err_when_persist_fails() {
        // Three-voter config so this node does not self-elect (we want
        // it as a Follower so the inbound Vote naturally triggers
        // PersistHardState before any captured VoteResponse).
        let toml = format!(
            r#"
node_id = 1
cluster_id = "test-driver"
listen_addr = "127.0.0.1:6902"
tick_interval_ms = 2
election_timeout_min_ms = 500
election_timeout_max_ms = 800
fetch_interval_ms = 10

[[voters]]
node_id = 1
directory_id = "{}"
host = "127.0.0.1"
port = 6000

[[voters]]
node_id = 2
directory_id = "{}"
host = "127.0.0.1"
port = 6001

[[voters]]
node_id = 3
directory_id = "{}"
host = "127.0.0.1"
port = 6002
"#,
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let cfg = ClusterConfig::from_toml_str(&toml).expect("three-voter config parses");
        let (driver, handle, _, fail_flag) = build_driver_with_persist_fail(cfg);
        let run_task = tokio::spawn(driver.run());

        // Let the driver enter its select loop.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Arm the persist-failure injection. The next call to
        // hs_store.persist (triggered by the inbound Vote request that
        // bumps our term) will return Err.
        fail_flag.store(true, std::sync::atomic::Ordering::SeqCst);

        let inbound = handle.inbound_handler();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            inbound.handle_vote(VoteRequest {
                cluster_id: "test-driver".into(),
                leader_epoch: 0,
                term: Term(5),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }),
        )
        .await
        .expect("handle_vote timed out");

        match result {
            Err(XRaftError::Storage(msg)) => {
                assert!(
                    msg.contains("persist"),
                    "expected persist-failure error, got: {msg}"
                );
            }
            Err(other) => panic!("expected Storage error, got {other:?}"),
            Ok(resp) => panic!("expected Err(Storage) when persist failed, got Ok({resp:?})"),
        }

        // Driver must halt on persistence failure per the RaftNode
        // driver contract (`xraft-core/src/node.rs:214-238`). `run()`
        // returns `Err(XRaftError::Storage(_))` without needing an
        // explicit shutdown signal.
        let run_result = tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("driver did not halt within 5s after persist failure")
            .expect("driver task panicked");
        match run_result {
            Err(XRaftError::Storage(msg)) => assert!(
                msg.contains("persist"),
                "halt reason should reference persist failure, got: {msg}"
            ),
            other => panic!("expected halt with Err(Storage), got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Outbound routing — Transport stubs and tests.
    // -----------------------------------------------------------------

    /// Transport stub that records every outbound RPC call (peer, kind)
    /// and synthesises a transport-level error so the engine never
    /// advances state but the dispatch IS observed on the wire.
    #[derive(Default, Clone)]
    struct RecordingTransport {
        log: Arc<Mutex<Vec<(NodeId, &'static str)>>>,
    }

    impl RecordingTransport {
        fn snapshot(&self) -> Vec<(NodeId, &'static str)> {
            self.log.lock().unwrap().clone()
        }
    }

    impl Transport for RecordingTransport {
        fn send_vote(
            &self,
            to: NodeId,
            _request: VoteRequest,
        ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
            let log = self.log.clone();
            async move {
                log.lock().unwrap().push((to, "vote"));
                Err(XRaftError::Transport("recording: peer unreachable".into()))
            }
        }
        fn send_pre_vote(
            &self,
            to: NodeId,
            _request: PreVoteRequest,
        ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
            let log = self.log.clone();
            async move {
                log.lock().unwrap().push((to, "pre_vote"));
                Err(XRaftError::Transport("recording: peer unreachable".into()))
            }
        }
        fn send_fetch(
            &self,
            to: NodeId,
            _request: FetchRequest,
        ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
            let log = self.log.clone();
            async move {
                log.lock().unwrap().push((to, "fetch"));
                Err(XRaftError::Transport("recording: peer unreachable".into()))
            }
        }
        fn send_fetch_snapshot(
            &self,
            to: NodeId,
            _request: FetchSnapshotRequest,
        ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
            let log = self.log.clone();
            async move {
                log.lock().unwrap().push((to, "fetch_snapshot"));
                Err(XRaftError::Transport("recording: peer unreachable".into()))
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn start_server(
            self: Arc<Self>,
        ) -> impl std::future::Future<Output = XResult<()>> + Send + 'static {
            async { Ok(()) }
        }
    }

    /// Transport whose `send_*` futures never complete — used to verify
    /// the shutdown path drains / aborts in-flight outbound RPCs within
    /// the configured deadline.
    #[derive(Default)]
    struct StuckTransport;

    impl Transport for StuckTransport {
        #[allow(clippy::manual_async_fn)] // pure async {} body — kept verbose to match the trait shape.
        fn send_vote(
            &self,
            _to: NodeId,
            _request: VoteRequest,
        ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
            async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!("StuckTransport never returns");
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_pre_vote(
            &self,
            _to: NodeId,
            _request: PreVoteRequest,
        ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
            async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!("StuckTransport never returns");
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_fetch(
            &self,
            _to: NodeId,
            _request: FetchRequest,
        ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
            async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!("StuckTransport never returns");
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_fetch_snapshot(
            &self,
            _to: NodeId,
            _request: FetchSnapshotRequest,
        ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
            async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!("StuckTransport never returns");
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn start_server(
            self: Arc<Self>,
        ) -> impl std::future::Future<Output = XResult<()>> + Send + 'static {
            async { Ok(()) }
        }
    }

    /// Build a three-voter cluster config whose election timer fires
    /// quickly (4..6 ms) — used for tests that need outbound traffic.
    fn three_voter_config_fast(node_id: u64) -> ClusterConfig {
        let toml = format!(
            r#"
node_id = {node_id}
cluster_id = "test-driver"
listen_addr = "127.0.0.1:6910"
tick_interval_ms = 2
election_timeout_min_ms = 4
election_timeout_max_ms = 6
fetch_interval_ms = 10

[[voters]]
node_id = 1
directory_id = "{d1}"
host = "127.0.0.1"
port = 6010

[[voters]]
node_id = 2
directory_id = "{d2}"
host = "127.0.0.1"
port = 6011

[[voters]]
node_id = 3
directory_id = "{d3}"
host = "127.0.0.1"
port = 6012
"#,
            node_id = node_id,
            d1 = Uuid::new_v4(),
            d2 = Uuid::new_v4(),
            d3 = Uuid::new_v4()
        );
        ClusterConfig::from_toml_str(&toml).expect("three-voter config parses")
    }

    /// Build a driver over an arbitrary transport (used to wire
    /// `RecordingTransport` / `StuckTransport` into the standard
    /// `TestLogStore` / `TestHardStateStore` / `TestSnapshotStore` /
    /// `TestStateMachine` stack).
    fn build_driver_with_transport<TX: Transport + Send + Sync + 'static>(
        config: ClusterConfig,
        transport: Arc<TX>,
    ) -> (
        Driver<TX, TestLogStore, TestHardStateStore, TestSnapshotStore, TestStateMachine>,
        DriverHandle,
    ) {
        let node = RaftNode::new_with_seed(config, 1234).expect("RaftNode ctor");
        let log = TestLogStore::default();
        let hs = TestHardStateStore::default();
        let ss = TestSnapshotStore::default();
        let sm = TestStateMachine::default();
        let driver = Driver::new(
            node,
            log,
            hs,
            ss,
            sm,
            transport,
            DriverConfig {
                tick_interval: Duration::from_millis(2),
                max_fetch_batch: 8,
                shutdown_drain_deadline: Duration::from_secs(2),
                fetch_snapshot_deadline: Duration::from_secs(2),
            },
        );
        let handle = driver.handle();
        (driver, handle)
    }

    // -----------------------------------------------------------------
    // Direct MessageRouter unit test: every OutboundMessage variant
    // reaches the underlying Transport's matching send_* method.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn message_router_dispatches_vote_to_transport() {
        let transport = Arc::new(RecordingTransport::default());
        let (tx, mut rx) = mpsc::channel::<OutboundResult>(16);
        let mut router = MessageRouter::new(transport.clone(), tx);

        router.dispatch(
            NodeId(2),
            OutboundMessage::VoteRequest(VoteRequest {
                cluster_id: "test-router".into(),
                leader_epoch: 1,
                term: Term(1),
                candidate_id: NodeId(1),
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }),
        );
        router.dispatch(
            NodeId(3),
            OutboundMessage::PreVoteRequest(PreVoteRequest {
                cluster_id: "test-router".into(),
                leader_epoch: 1,
                next_term: Term(1),
                candidate_id: NodeId(1),
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }),
        );

        // Drain both `OutboundResult::Error` events — RecordingTransport
        // synthesises a transport error after recording, so the result
        // channel must produce two `Error` events with matching kinds.
        let mut got: Vec<(NodeId, &'static str)> = Vec::new();
        for _ in 0..2 {
            let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("router did not produce result within 2s")
                .expect("router channel closed");
            match evt {
                OutboundResult::Error { peer, kind, .. } => got.push((peer, kind)),
                other => panic!("expected Error variant, got {other:?}"),
            }
        }
        got.sort();
        let mut expected: Vec<(NodeId, &'static str)> =
            vec![(NodeId(2), "vote"), (NodeId(3), "pre_vote")];
        expected.sort();
        assert_eq!(got, expected, "router did not produce expected results");

        let mut recorded = transport.snapshot();
        recorded.sort();
        assert_eq!(
            recorded, expected,
            "transport did not see the expected outbound calls"
        );
    }

    // -----------------------------------------------------------------
    // Multi-voter Driver test: the running driver loop dispatches
    // outbound PreVote / Vote RPCs to its peers via the Transport.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn driver_three_voters_outbound_recorded() {
        let cfg = three_voter_config_fast(1);
        let transport = Arc::new(RecordingTransport::default());
        let (driver, handle) = build_driver_with_transport(cfg, transport.clone());
        let run_task = tokio::spawn(driver.run());

        // Advance virtual time past several election timeouts. With
        // RecordingTransport always erroring, the node loops through
        // PreCandidate without electing — but each retry dispatches
        // outbound PreVote RPCs to both peers (2 and 3).
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            tokio::task::yield_now().await;
        }

        let recorded = transport.snapshot();
        assert!(
            !recorded.is_empty(),
            "expected at least one outbound RPC, got none"
        );
        // We must observe outbound dispatches to BOTH peers.
        let peers: std::collections::BTreeSet<NodeId> = recorded.iter().map(|(p, _)| *p).collect();
        assert!(
            peers.contains(&NodeId(2)) && peers.contains(&NodeId(3)),
            "expected outbound RPCs to both peers (2 and 3), got: {peers:?}"
        );
        // At least one of the recorded kinds must be `pre_vote` (the
        // election timer's first move).
        assert!(
            recorded.iter().any(|(_, k)| *k == "pre_vote"),
            "expected at least one pre_vote dispatch, got: {recorded:?}"
        );

        handle.shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("driver run did not exit within 5s")
            .expect("driver task panicked");
        assert!(result.is_ok(), "driver.run returned error: {result:?}");
    }

    // -----------------------------------------------------------------
    // Shutdown drains in-flight outbound RPCs within the configured
    // deadline. StuckTransport's send_* futures never complete, so the
    // drain MUST hit the deadline (~2s) and abort them — the driver
    // returns Ok(()) within `deadline + small slack`.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn shutdown_drains_in_flight_outbound_within_deadline() {
        let cfg = three_voter_config_fast(1);
        let transport = Arc::new(StuckTransport);
        let (driver, handle) = build_driver_with_transport(cfg, transport);
        let run_task = tokio::spawn(driver.run());

        // Drive virtual time past the election timer so the driver
        // dispatches outbound PreVotes — those futures are stuck on
        // a 1h sleep, so they remain in-flight.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;

        // Fire shutdown. The drain has a 2s deadline (set in
        // `build_driver_with_transport`). After the deadline elapses,
        // the router's pending tasks are aborted and `run()` returns.
        let shutdown_start = tokio::time::Instant::now();
        handle.shutdown();
        let result = tokio::time::timeout(Duration::from_secs(6), run_task)
            .await
            .expect("driver run did not exit within 6s of shutdown")
            .expect("driver task panicked");
        let elapsed = shutdown_start.elapsed();
        assert!(
            result.is_ok(),
            "driver.run returned error after shutdown: {result:?}"
        );
        // Drain deadline is 2s; allow up to 5s for the wrap-up
        // (graceful_drain may have its own per-poll budget plus the
        // router drain). The point is that `run()` MUST eventually
        // return even though the outbound RPCs never completed.
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown took {elapsed:?}; expected < 5s with 2s drain deadline"
        );
    }

    // -----------------------------------------------------------------
    // Direct MessageRouter unit test: outbound Fetch dispatch reaches
    // the Transport's `send_fetch` and the result is surfaced as an
    // `OutboundResult::Error` (RecordingTransport always synthesises an
    // error after recording). Pairs with the existing Vote / PreVote
    // coverage to exercise every non-snapshot OutboundMessage variant
    // through the router.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn message_router_dispatches_fetch_to_transport() {
        let transport = Arc::new(RecordingTransport::default());
        let (tx, mut rx) = mpsc::channel::<OutboundResult>(16);
        let mut router = MessageRouter::new(transport.clone(), tx);

        router.dispatch(
            NodeId(2),
            OutboundMessage::FetchRequest(FetchRequest {
                cluster_id: "test-router".into(),
                leader_epoch: 1,
                replica_id: NodeId(1),
                fetch_offset: LogIndex(0),
                last_fetched_epoch: Term(0),
            }),
        );

        let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("router did not produce result within 2s")
            .expect("router channel closed");
        match evt {
            OutboundResult::Error { peer, kind, .. } => {
                assert_eq!(peer, NodeId(2));
                assert_eq!(kind, "fetch");
            }
            other => panic!("expected Error variant, got {other:?}"),
        }

        let recorded = transport.snapshot();
        assert_eq!(recorded, vec![(NodeId(2), "fetch")]);
    }

    /// Test transport that returns a pre-built `SnapshotChunkStream` on
    /// the first `send_fetch_snapshot` call. Used to verify the
    /// router's stream-draining loop in `MessageRouter::dispatch`.
    struct ChunkProducingTransport {
        chunks: Mutex<Option<Vec<XResult<FetchSnapshotChunk>>>>,
    }

    impl ChunkProducingTransport {
        fn new(chunks: Vec<XResult<FetchSnapshotChunk>>) -> Self {
            Self {
                chunks: Mutex::new(Some(chunks)),
            }
        }
    }

    impl Transport for ChunkProducingTransport {
        #[allow(clippy::manual_async_fn)]
        fn send_vote(
            &self,
            _to: NodeId,
            _request: VoteRequest,
        ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
            async {
                Err(XRaftError::Transport(
                    "ChunkProducingTransport: send_vote unsupported".into(),
                ))
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_pre_vote(
            &self,
            _to: NodeId,
            _request: PreVoteRequest,
        ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
            async {
                Err(XRaftError::Transport(
                    "ChunkProducingTransport: send_pre_vote unsupported".into(),
                ))
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_fetch(
            &self,
            _to: NodeId,
            _request: FetchRequest,
        ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
            async {
                Err(XRaftError::Transport(
                    "ChunkProducingTransport: send_fetch unsupported".into(),
                ))
            }
        }
        fn send_fetch_snapshot(
            &self,
            _to: NodeId,
            _request: FetchSnapshotRequest,
        ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
            let taken = self.chunks.lock().unwrap().take();
            async move {
                match taken {
                    Some(chunks) => {
                        let stream = StaticChunkStream {
                            chunks: chunks.into_iter().collect(),
                        };
                        Ok(Box::pin(stream) as SnapshotChunkStream)
                    }
                    None => Err(XRaftError::Transport(
                        "ChunkProducingTransport: chunks already consumed".into(),
                    )),
                }
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn start_server(
            self: Arc<Self>,
        ) -> impl std::future::Future<Output = XResult<()>> + Send + 'static {
            async { Ok(()) }
        }
    }

    /// Router test: a complete FetchSnapshot stream (a single chunk
    /// with `done = true`) is drained into
    /// `OutboundResult::FetchSnapshot { chunk_count: 1, completed:
    /// true }`. Validates the success path of the snapshot drain loop
    /// added in iter 2.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn message_router_dispatches_complete_fetch_snapshot_stream() {
        let chunk = FetchSnapshotChunk {
            cluster_id: "test-router".into(),
            leader_epoch: 1,
            chunk_index: 0,
            data: vec![1, 2, 3, 4],
            done: true,
            metadata: Some(SnapshotMeta {
                id: "snap-1".into(),
                last_included_index: LogIndex(10),
                last_included_term: Term(1),
                voter_set: None,
                size_bytes: Some(4),
                checksum: None,
            }),
        };
        let transport = Arc::new(ChunkProducingTransport::new(vec![Ok(chunk)]));
        let (tx, mut rx) = mpsc::channel::<OutboundResult>(16);
        let mut router = MessageRouter::new(transport, tx);

        router.dispatch(
            NodeId(2),
            OutboundMessage::FetchSnapshotRequest(FetchSnapshotRequest {
                cluster_id: "test-router".into(),
                leader_epoch: 1,
                replica_id: NodeId(1),
                snapshot_id: "snap-1".into(),
                offset: 0,
                max_bytes: 0,
            }),
        );

        let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("router did not produce result within 2s")
            .expect("router channel closed");
        match evt {
            OutboundResult::FetchSnapshot {
                peer,
                chunk_count,
                completed,
            } => {
                assert_eq!(peer, NodeId(2));
                assert_eq!(chunk_count, 1);
                assert!(
                    completed,
                    "expected completed=true for a stream ending with done=true"
                );
            }
            other => panic!("expected OutboundResult::FetchSnapshot, got {other:?}"),
        }
    }

    /// Router test: a FetchSnapshot stream that ends WITHOUT a final
    /// `done = true` chunk must surface as
    /// `OutboundResult::Error { kind: "fetch_snapshot", err: contains
    /// "without done=true" }`. Pinned to iter-3 evaluator item #4
    /// (truncated snapshot streams are not surfaced as errors).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn message_router_dispatches_incomplete_fetch_snapshot_stream() {
        let chunk = FetchSnapshotChunk {
            cluster_id: "test-router".into(),
            leader_epoch: 1,
            chunk_index: 0,
            data: vec![9; 8],
            done: false, // <-- KEY: stream ends without done=true.
            metadata: Some(SnapshotMeta {
                id: "snap-1".into(),
                last_included_index: LogIndex(10),
                last_included_term: Term(1),
                voter_set: None,
                size_bytes: Some(16),
                checksum: None,
            }),
        };
        let transport = Arc::new(ChunkProducingTransport::new(vec![Ok(chunk)]));
        let (tx, mut rx) = mpsc::channel::<OutboundResult>(16);
        let mut router = MessageRouter::new(transport, tx);

        router.dispatch(
            NodeId(2),
            OutboundMessage::FetchSnapshotRequest(FetchSnapshotRequest {
                cluster_id: "test-router".into(),
                leader_epoch: 1,
                replica_id: NodeId(1),
                snapshot_id: "snap-1".into(),
                offset: 0,
                max_bytes: 0,
            }),
        );

        let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("router did not produce result within 2s")
            .expect("router channel closed");
        match evt {
            OutboundResult::Error { peer, kind, err } => {
                assert_eq!(peer, NodeId(2));
                assert_eq!(kind, "fetch_snapshot");
                assert!(
                    err.contains("without done=true"),
                    "expected truncation error message, got: {err}"
                );
            }
            other => {
                panic!("expected OutboundResult::Error (incomplete snapshot stream), got {other:?}")
            }
        }
    }

    /// Test stream that never yields and never closes — `poll_next`
    /// always returns `Poll::Pending` without registering a waker.
    /// Combined with `tokio::test(start_paused = true)`, the runtime
    /// auto-advances time to the next scheduled wakeup (the drain
    /// loop's `tokio::time::timeout` sleep), which lets us
    /// deterministically exercise the snapshot drain deadline path
    /// without sleeping in wall-clock time.
    struct HangingChunkStream;

    impl Stream for HangingChunkStream {
        type Item = XResult<FetchSnapshotChunk>;
        fn poll_next(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Pending
        }
    }

    /// Test transport that returns a `HangingChunkStream` on
    /// `send_fetch_snapshot`. Simulates a slow or malicious peer that
    /// keeps the snapshot stream open forever without sending the
    /// terminal `done = true` chunk and without closing it. Used by
    /// `message_router_fetch_snapshot_drain_surfaces_deadline_timeout`
    /// to verify the drain loop's `tokio::time::timeout` wrapper.
    struct HangingChunkTransport;

    impl Transport for HangingChunkTransport {
        #[allow(clippy::manual_async_fn)]
        fn send_vote(
            &self,
            _to: NodeId,
            _request: VoteRequest,
        ) -> impl std::future::Future<Output = XResult<VoteResponse>> + Send {
            async {
                Err(XRaftError::Transport(
                    "HangingChunkTransport: send_vote unsupported".into(),
                ))
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_pre_vote(
            &self,
            _to: NodeId,
            _request: PreVoteRequest,
        ) -> impl std::future::Future<Output = XResult<PreVoteResponse>> + Send {
            async {
                Err(XRaftError::Transport(
                    "HangingChunkTransport: send_pre_vote unsupported".into(),
                ))
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_fetch(
            &self,
            _to: NodeId,
            _request: FetchRequest,
        ) -> impl std::future::Future<Output = XResult<FetchResponse>> + Send {
            async {
                Err(XRaftError::Transport(
                    "HangingChunkTransport: send_fetch unsupported".into(),
                ))
            }
        }
        #[allow(clippy::manual_async_fn)]
        fn send_fetch_snapshot(
            &self,
            _to: NodeId,
            _request: FetchSnapshotRequest,
        ) -> impl std::future::Future<Output = XResult<SnapshotChunkStream>> + Send {
            async { Ok(Box::pin(HangingChunkStream) as SnapshotChunkStream) }
        }
        #[allow(clippy::manual_async_fn)]
        fn start_server(
            self: Arc<Self>,
        ) -> impl std::future::Future<Output = XResult<()>> + Send + 'static {
            async { Ok(()) }
        }
    }

    /// Router test: a FetchSnapshot stream that never yields and never
    /// closes must NOT pin a `JoinSet` slot indefinitely. The drain
    /// loop wraps itself in `tokio::time::timeout` against
    /// `MessageRouter::fetch_snapshot_deadline`; when that fires the
    /// task surfaces as
    /// `OutboundResult::Error { kind: "fetch_snapshot", err: contains
    /// "exceeded deadline" }`. Pinned to reviewer item:
    /// `OutboundFetchSnapshot drain has no timeout — slow / malicious
    /// peer can pin a JoinSet slot forever`.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn message_router_fetch_snapshot_drain_surfaces_deadline_timeout() {
        let transport = Arc::new(HangingChunkTransport);
        let (tx, mut rx) = mpsc::channel::<OutboundResult>(16);
        // Tight deadline so the test exercises the timeout path quickly
        // under the paused runtime's auto-advance.
        let deadline = Duration::from_millis(50);
        let mut router = MessageRouter::new_with_fetch_snapshot_deadline(transport, tx, deadline);

        router.dispatch(
            NodeId(2),
            OutboundMessage::FetchSnapshotRequest(FetchSnapshotRequest {
                cluster_id: "test-router".into(),
                leader_epoch: 1,
                replica_id: NodeId(1),
                snapshot_id: "snap-1".into(),
                offset: 0,
                max_bytes: 0,
            }),
        );

        // The hanging stream never yields, so without the drain-loop
        // timeout this would block until the outer 2s timeout below
        // expires and the test fails. With the timeout in place the
        // drain task surfaces an Error within `deadline` (auto-advanced
        // by the paused runtime).
        let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("drain did not surface an OutboundResult within 2s — timeout missing?")
            .expect("router channel closed");
        match evt {
            OutboundResult::Error { peer, kind, err } => {
                assert_eq!(peer, NodeId(2));
                assert_eq!(kind, "fetch_snapshot");
                assert!(
                    err.contains("exceeded deadline"),
                    "expected deadline-exceeded error message, got: {err}"
                );
            }
            other => panic!("expected OutboundResult::Error (drain timeout), got {other:?}"),
        }

        // The router's in-flight count must drop back to zero once the
        // spawned task surfaces the timeout error — the timeout MUST
        // release the JoinSet slot, not leak it.
        // We give the JoinSet a chance to reap by yielding once.
        tokio::task::yield_now().await;
        // Reap the completed task explicitly through the router.
        router.reap_one().await;
        assert_eq!(
            router.in_flight(),
            0,
            "fetch_snapshot drain task must release its JoinSet slot after the deadline fires"
        );
    }

    // -----------------------------------------------------------------
    // Item 1: final-shutdown persistence error MUST propagate from
    // `Driver::run()` as `Err(XRaftError::Storage(_))`. Previously the
    // error was only logged and `run()` returned `Ok(())`, masking a
    // partially-persisted shutdown.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn driver_run_returns_err_when_final_persist_fails() {
        let cfg = single_voter_config(2);
        let (driver, handle, _, fail_flag) = build_driver_with_persist_fail(cfg);
        let run_task = tokio::spawn(driver.run());

        // Let the driver enter its select loop and complete the
        // initial self-election persist cycle.
        tokio::time::sleep(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;

        // Arm the persist-failure injection AFTER the driver is past
        // its election persist. The next persist call — which will be
        // the final-state persist inside `shutdown_sequence` — will
        // fail and `run()` must return `Err(Storage(_))`.
        fail_flag.store(true, std::sync::atomic::Ordering::SeqCst);

        // Trigger the graceful shutdown path.
        handle.shutdown();

        let run_result = tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("driver did not exit within 5s after shutdown")
            .expect("driver task panicked");
        match run_result {
            Err(XRaftError::Storage(msg)) => assert!(
                msg.contains("persist") || msg.contains("final"),
                "expected final-persist failure message, got: {msg}"
            ),
            other => panic!("expected Err(Storage) from run(), got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Stage 5.1 — driver-level Action dispatch tests
    //
    // These exercise `Driver::process_actions` directly with synthetic
    // actions so we can verify, in isolation:
    //   * `Action::ApplyToStateMachine` halt-on-failure and waiter-
    //     failure semantics.
    //   * `Action::TakeSnapshot` happy path plus every halt-worthy
    //     failure mode (missing voter_set, missing term, SM error,
    //     store error, last_applied == 0).
    //   * `Action::InstallSnapshot` persist-before-restore ordering,
    //     post-install bookkeeping, halt-on-failure for both backing
    //     calls, missing-voter-set guard, and no-regress invariant.
    //
    // The engine does not currently emit `TakeSnapshot`/`InstallSnapshot`
    // anywhere, so feeding actions directly into `process_actions` is the
    // only way to exercise the dispatch code paths in isolation.
    // -----------------------------------------------------------------

    fn build_dispatch_driver() -> (TestDriver, TestStateMachine, TestSnapshotStore) {
        let cfg = single_voter_config(2);
        let node = RaftNode::new_with_seed(cfg, 1234).expect("RaftNode ctor");
        let log = TestLogStore::default();
        let hs = TestHardStateStore::default();
        let ss = TestSnapshotStore::default();
        let sm = TestStateMachine::default();
        let sm_handle = sm.clone();
        let ss_handle = ss.clone();
        let transport = Arc::new(NoopTransport::default());
        let driver = Driver::new(
            node,
            log,
            hs,
            ss,
            sm,
            transport,
            DriverConfig {
                tick_interval: Duration::from_millis(2),
                max_fetch_batch: 8,
                shutdown_drain_deadline: Duration::from_secs(2),
                fetch_snapshot_deadline: Duration::from_secs(2),
            },
        );
        (driver, sm_handle, ss_handle)
    }

    fn seed_committed_entries(driver: &mut TestDriver, n: u64) {
        let mut entries = Vec::with_capacity(n as usize);
        for i in 1..=n {
            entries.push(Entry {
                index: LogIndex(i),
                term: Term(1),
                payload: EntryPayload::Command(Bytes::from(format!("cmd-{i}").into_bytes())),
            });
        }
        driver.log_store.append(&entries).expect("seed log append");
        driver.node.commit_index = LogIndex(n);
        driver.node.last_applied = LogIndex(n);
        driver.node.set_last_log(LogIndex(n), Term(1));
    }

    fn make_voter_set_for(ids: &[u64]) -> xraft_core::types::VoterSet {
        use xraft_core::types::{DirectoryId, Endpoint, VoterRecord, VoterSet};
        let records: Vec<VoterRecord> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| VoterRecord {
                node_id: NodeId(*id),
                directory_id: DirectoryId::new_random(),
                endpoints: vec![Endpoint::new("127.0.0.1", 9000 + i as u16)],
            })
            .collect();
        VoterSet::try_new(records).expect("voter set construction")
    }

    fn snapshot_meta_for(idx: u64, term: u64, voter_ids: &[u64]) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(idx),
            last_included_term: Term(term),
            id: String::new(),
            voter_set: Some(make_voter_set_for(voter_ids)),
            size_bytes: None,
            checksum: None,
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn apply_to_state_machine_halts_when_state_machine_apply_fails() {
        let (mut driver, sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 4);
        sm.arm_fail_apply_at(LogIndex(3));

        let _ = driver
            .process_actions(
                vec![Action::ApplyToStateMachine {
                    from: LogIndex(1),
                    to: LogIndex(4),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("halt_reason should be set after apply failure");
        assert!(
            halt.contains("state machine apply at LogIndex(3)"),
            "halt reason should name the failing index, got: {halt}"
        );
        let applied = sm.snapshot_handle().lock().unwrap().clone();
        let indices: Vec<u64> = applied.iter().map(|(idx, _)| idx.0).collect();
        assert_eq!(
            indices,
            vec![1, 2],
            "must apply 1,2 then halt at 3 (no apply of 4)"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn apply_to_state_machine_halts_when_log_read_fails() {
        let (mut driver, sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 2);
        driver
            .log_store
            .fail_next_get_range
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let _ = driver
            .process_actions(
                vec![Action::ApplyToStateMachine {
                    from: LogIndex(1),
                    to: LogIndex(2),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("halt_reason should be set after log read failure");
        assert!(
            halt.contains("apply: read range"),
            "halt reason should reference log read range, got: {halt}"
        );
        assert!(sm.snapshot_handle().lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_persists_state_machine_bytes_with_correct_metadata() {
        let (mut driver, sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 3);
        let payload = b"sm-snapshot-payload-v1".to_vec();
        sm.set_snapshot_bytes(payload.clone());

        let _ = driver
            .process_actions(vec![Action::TakeSnapshot], None)
            .await;

        assert!(
            driver.halt_reason.is_none(),
            "TakeSnapshot happy path must not halt: {:?}",
            driver.halt_reason
        );
        let saved = ss.saved_snapshots();
        assert_eq!(saved.len(), 1, "exactly one snapshot should be persisted");
        let (meta, data) = &saved[0];
        assert_eq!(meta.last_included_index, LogIndex(3));
        assert_eq!(meta.last_included_term, Term(1));
        assert_eq!(
            meta.voter_set.as_ref().map(|vs| vs.voters().len()),
            Some(1),
            "snapshot must carry the active voter set"
        );
        assert_eq!(data, &payload, "persisted bytes must equal SM snapshot");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_with_last_applied_zero_is_noop() {
        let (mut driver, _sm, ss) = build_dispatch_driver();
        assert_eq!(driver.node.last_applied, LogIndex(0));

        let _ = driver
            .process_actions(vec![Action::TakeSnapshot], None)
            .await;

        assert!(
            driver.halt_reason.is_none(),
            "TakeSnapshot at index 0 must NOT halt the driver"
        );
        assert!(
            ss.saved_snapshots().is_empty(),
            "no snapshot should be persisted when last_applied == 0"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_halts_when_state_machine_snapshot_fails() {
        let (mut driver, sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 1);
        sm.arm_fail_next_snapshot();

        let _ = driver
            .process_actions(vec![Action::TakeSnapshot], None)
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when state_machine.snapshot fails");
        assert!(
            halt.contains("state machine snapshot failed"),
            "halt reason should reference SM snapshot failure, got: {halt}"
        );
        assert!(
            ss.saved_snapshots().is_empty(),
            "no snapshot should be persisted when SM.snapshot() errored"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_halts_when_snapshot_store_save_fails() {
        let (mut driver, _sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 1);
        ss.fail_next_save
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let _ = driver
            .process_actions(vec![Action::TakeSnapshot], None)
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when snapshot_store.save_snapshot fails");
        assert!(
            halt.contains("snapshot_store.save_snapshot failed"),
            "halt reason should reference snapshot store save failure, got: {halt}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_halts_when_term_at_last_applied_missing() {
        let (mut driver, _sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 1);
        driver
            .log_store
            .missing_next_term_at
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let _ = driver
            .process_actions(vec![Action::TakeSnapshot], None)
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when term_at returns None for last_applied");
        assert!(
            halt.contains("term_at(last_applied="),
            "halt reason should name the missing term lookup, got: {halt}"
        );
        assert!(ss.saved_snapshots().is_empty());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_halts_when_voter_set_unset() {
        let (mut driver, _sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 1);
        driver.node.voter_set = None;

        let _ = driver
            .process_actions(vec![Action::TakeSnapshot], None)
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when voter_set is None");
        assert!(
            halt.contains("voter_set"),
            "halt reason should reference voter_set, got: {halt}"
        );
        assert!(ss.saved_snapshots().is_empty());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_persists_then_restores_and_advances_bookkeeping() {
        let (mut driver, sm, ss) = build_dispatch_driver();
        let snap_data = b"leader-snapshot-v42".to_vec();
        let meta = snapshot_meta_for(50, 7, &[1, 2]);

        let _ = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: snap_data.clone(),
                }],
                None,
            )
            .await;

        assert!(
            driver.halt_reason.is_none(),
            "InstallSnapshot happy path must not halt: {:?}",
            driver.halt_reason
        );
        let saved = ss.saved_snapshots();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].1, snap_data);
        assert_eq!(saved[0].0.last_included_index, LogIndex(50));

        let restored = sm.restored_snapshots();
        assert_eq!(restored, vec![snap_data.clone()]);

        assert_eq!(driver.node.last_applied, LogIndex(50));
        assert_eq!(driver.node.commit_index, LogIndex(50));
        assert_eq!(driver.node.last_log_index, LogIndex(50));
        assert_eq!(driver.node.last_log_term, Term(7));

        let vs = driver
            .node
            .voter_set
            .as_ref()
            .expect("voter_set should be set after InstallSnapshot");
        let voter_ids: Vec<u64> = vs.voters().iter().map(|v| v.node_id.0).collect();
        assert!(voter_ids.contains(&1) && voter_ids.contains(&2));
        assert!(
            driver.node.peers.contains_key(&NodeId(2)),
            "peer table must be rebuilt to include the new peer"
        );
        assert!(
            !driver.node.peers.contains_key(&NodeId(1)),
            "peer table must exclude self"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_halts_when_snapshot_store_save_fails() {
        let (mut driver, sm, ss) = build_dispatch_driver();
        ss.fail_next_save
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let meta = snapshot_meta_for(10, 2, &[1]);

        let _ = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: b"x".to_vec(),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when SnapshotStore.save_snapshot fails");
        assert!(
            halt.contains("snapshot_store.save_snapshot failed"),
            "halt reason should reference snapshot store save failure, got: {halt}"
        );
        assert!(
            sm.restored_snapshots().is_empty(),
            "restore must NOT be called when persist fails (persist-first ordering)"
        );
        assert_eq!(driver.node.last_applied, LogIndex(0));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_halts_when_state_machine_restore_fails() {
        let (mut driver, sm, ss) = build_dispatch_driver();
        sm.arm_fail_next_restore();
        let meta = snapshot_meta_for(10, 2, &[1]);

        let _ = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: b"x".to_vec(),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when state_machine.restore fails");
        assert!(
            halt.contains("state_machine.restore failed"),
            "halt reason should reference SM restore failure, got: {halt}"
        );
        assert_eq!(
            ss.saved_snapshots().len(),
            1,
            "snapshot must be persisted before restore is attempted"
        );
        assert_eq!(driver.node.last_applied, LogIndex(0));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_rejects_metadata_missing_voter_set() {
        let (mut driver, sm, ss) = build_dispatch_driver();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(10),
            last_included_term: Term(2),
            id: "synthetic".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };

        let _ = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: b"x".to_vec(),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt on metadata without voter_set");
        assert!(
            halt.contains("missing required voter_set"),
            "halt reason should reference missing voter_set, got: {halt}"
        );
        assert!(ss.saved_snapshots().is_empty());
        assert!(sm.restored_snapshots().is_empty());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_does_not_regress_bookkeeping_when_behind() {
        // Stale-snapshot guard: a snapshot at or behind `last_applied`
        // must be a NO-OP — no SnapshotStore.save_snapshot, no
        // state_machine.restore, no bookkeeping mutation, and the
        // driver must NOT halt (stale installs are a benign race, not
        // a correctness fault). The earlier iteration only checked
        // index no-regress, which silently allowed the SM to be rolled
        // back to the older snapshot bytes while indices stayed
        // forward — that is exactly the corruption this test now
        // pins down.
        let (mut driver, sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 100);
        let pre_last_applied = driver.node.last_applied;
        let pre_commit = driver.node.commit_index;
        let pre_last_log_index = driver.node.last_log_index;
        let pre_voter_set = driver.node.voter_set.clone();
        let pre_peers: Vec<NodeId> = driver.node.peers.keys().copied().collect();

        let meta = snapshot_meta_for(50, 3, &[1, 2, 3]);
        let _ = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: b"behind".to_vec(),
                }],
                None,
            )
            .await;

        assert!(
            driver.halt_reason.is_none(),
            "install behind local tip should not halt: {:?}",
            driver.halt_reason
        );
        // Indices unchanged.
        assert_eq!(driver.node.last_applied, pre_last_applied);
        assert_eq!(driver.node.commit_index, pre_commit);
        assert_eq!(driver.node.last_log_index, pre_last_log_index);
        // Critical: no persist and no restore. The previous
        // implementation called both BEFORE the no-regress guards,
        // rolling the SM back to old data with stale indices intact.
        assert!(
            ss.saved_snapshots().is_empty(),
            "stale snapshot must NOT be persisted"
        );
        assert!(
            sm.restored_snapshots().is_empty(),
            "stale snapshot must NOT trigger state_machine.restore"
        );
        // Membership unchanged: a stale install's voter_set must not
        // overwrite the current (newer) configuration.
        assert_eq!(driver.node.voter_set, pre_voter_set);
        let post_peers: Vec<NodeId> = driver.node.peers.keys().copied().collect();
        assert_eq!(post_peers, pre_peers);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_at_equal_last_applied_is_noop() {
        // Boundary case of the stale-snapshot guard: `snap_idx ==
        // last_applied`. The SM is already at that state, so the
        // restore is at best wasteful (same bytes) and at worst
        // corrupting (different bytes from a divergent leader). The
        // `<=` in the guard intentionally covers equality — pin it
        // here so a future change to `<` is caught by tests.
        let (mut driver, sm, ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 7);
        assert_eq!(driver.node.last_applied, LogIndex(7));

        let meta = snapshot_meta_for(7, 1, &[1]);
        let _ = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: b"equal".to_vec(),
                }],
                None,
            )
            .await;

        assert!(driver.halt_reason.is_none());
        assert!(ss.saved_snapshots().is_empty());
        assert!(sm.restored_snapshots().is_empty());
    }

    // -----------------------------------------------------------------
    // Iteration 4 — additional fix coverage:
    //   * `apply_committed` halt-on-incomplete-range (gap or short
    //     read) — engine has already advanced last_applied, so
    //     skipping silently would corrupt the SM/engine alignment.
    //   * `process_actions` populates `captured.error` (not just
    //     `halt_reason`) for ApplyToStateMachine / TakeSnapshot /
    //     InstallSnapshot failures, mirroring the persistence
    //     failure branches. Without this, an inbound RPC handler
    //     can return a captured success response while the driver
    //     is fail-stopping.
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn apply_committed_halts_when_log_range_is_short() {
        // Seed 4 entries, ask `apply_committed` for [1..=4], but make
        // the log store drop entry 3 from the response. The driver
        // must halt — the engine has already advanced last_applied to
        // 4, so silently applying only 1,2,4 would leave the SM
        // missing entry 3 forever.
        let (mut driver, sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 4);
        driver
            .log_store
            .drop_indices_in_get_range
            .lock()
            .unwrap()
            .insert(LogIndex(3));

        let captured = driver
            .process_actions(
                vec![Action::ApplyToStateMachine {
                    from: LogIndex(1),
                    to: LogIndex(4),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt when log_store returns short/gapped range");
        assert!(
            halt.contains("returned 3 entries, expected 4")
                || halt.contains("returned entry at LogIndex(4) at position 2"),
            "halt reason should name the short-range/gap, got: {halt}"
        );
        // captured.error must mirror the halt_reason for symmetry
        // with the persistence-failure branches (Fix 3).
        assert!(
            captured.error.is_some(),
            "captured.error must be set so inbound RPC handlers return Err"
        );
        // Critical: validation happens BEFORE the apply loop, so the
        // state machine sees zero applies — the alternative (applying
        // 1,2 then halting at 3) would let the engine and SM diverge
        // by exactly the entries before the gap.
        assert!(
            sm.snapshot_handle().lock().unwrap().is_empty(),
            "no apply must happen when range validation fails"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn apply_committed_halts_when_log_range_has_index_gap() {
        // Seed 1,2,4 (no entry 3) and request [1..=4]. The store
        // returns 3 entries — the length check still triggers, but
        // we also want to confirm the index-continuity check would
        // catch a same-length-but-misaligned response (e.g. a future
        // store that returns 1,2,4,4 by mistake). Easiest way to
        // exercise the index-continuity branch with the existing
        // injectors is to drop entry 2 instead of 3, then assert the
        // failure message names the index mismatch.
        let (mut driver, _sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 4);
        driver
            .log_store
            .drop_indices_in_get_range
            .lock()
            .unwrap()
            .insert(LogIndex(2));

        let captured = driver
            .process_actions(
                vec![Action::ApplyToStateMachine {
                    from: LogIndex(1),
                    to: LogIndex(4),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt on log range gap");
        assert!(
            halt.contains("returned 3 entries, expected 4")
                || halt.contains("at position"),
            "halt reason should describe the range mismatch, got: {halt}"
        );
        assert!(captured.error.is_some());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn apply_to_state_machine_populates_captured_error_on_failure() {
        // Fix 3: when state_machine.apply() fails, `process_actions`
        // must populate `captured.error` AND `halt_reason`. The
        // returned CapturedResponse is what inbound RPC handlers
        // (Vote/PreVote/Fetch/FetchSnapshot) inspect before sending
        // any captured success payload — without captured.error,
        // those handlers will reply Ok while the driver is dying.
        let (mut driver, sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 2);
        sm.arm_fail_apply_at(LogIndex(1));

        let captured = driver
            .process_actions(
                vec![Action::ApplyToStateMachine {
                    from: LogIndex(1),
                    to: LogIndex(2),
                }],
                None,
            )
            .await;

        assert!(driver.halt_reason.is_some());
        let err = captured
            .error
            .expect("captured.error must be set so inbound RPCs return Err");
        let msg = format!("{err}");
        assert!(
            msg.contains("state machine apply at LogIndex(1)"),
            "captured.error should describe the apply failure, got: {msg}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn take_snapshot_populates_captured_error_on_failure() {
        // Fix 3 for the TakeSnapshot dispatch branch.
        let (mut driver, sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 1);
        sm.arm_fail_next_snapshot();

        let captured = driver.process_actions(vec![Action::TakeSnapshot], None).await;

        assert!(driver.halt_reason.is_some());
        let err = captured
            .error
            .expect("captured.error must be set for TakeSnapshot failure");
        let msg = format!("{err}");
        assert!(
            msg.contains("state machine snapshot failed"),
            "captured.error should describe the snapshot failure, got: {msg}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn install_snapshot_populates_captured_error_on_failure() {
        // Fix 3 for the InstallSnapshot dispatch branch. Use the
        // missing-voter-set halt path because it doesn't require any
        // injector wiring.
        let (mut driver, _sm, _ss) = build_dispatch_driver();
        let meta = SnapshotMeta {
            last_included_index: LogIndex(10),
            last_included_term: Term(2),
            id: "no-voters".to_string(),
            voter_set: None,
            size_bytes: None,
            checksum: None,
        };

        let captured = driver
            .process_actions(
                vec![Action::InstallSnapshot {
                    metadata: meta,
                    data: b"x".to_vec(),
                }],
                None,
            )
            .await;

        assert!(driver.halt_reason.is_some());
        let err = captured
            .error
            .expect("captured.error must be set for InstallSnapshot failure");
        let msg = format!("{err}");
        assert!(
            msg.contains("missing required voter_set"),
            "captured.error should describe the install failure, got: {msg}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn apply_committed_halts_on_inverted_range() {
        // Defensive guard against an inverted range arriving at the
        // dispatch helper. The engine's contract is from <= to, but
        // `apply_committed` is reachable from direct test dispatch
        // and any future engine bug would otherwise panic on the
        // subtraction in `expected_len`. Halting is the safe choice.
        let (mut driver, sm, _ss) = build_dispatch_driver();
        seed_committed_entries(&mut driver, 4);

        let captured = driver
            .process_actions(
                vec![Action::ApplyToStateMachine {
                    from: LogIndex(3),
                    to: LogIndex(1),
                }],
                None,
            )
            .await;

        let halt = driver
            .halt_reason
            .as_ref()
            .expect("must halt on inverted range");
        assert!(
            halt.contains("invalid range"),
            "halt reason should call out invalid range, got: {halt}"
        );
        assert!(captured.error.is_some());
        assert!(sm.snapshot_handle().lock().unwrap().is_empty());
    }
}
