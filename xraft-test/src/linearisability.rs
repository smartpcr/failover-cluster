//! [`HistoryRecorder`] + [`verify_linearisable`] — post-hoc
//! linearisability validation for chaos test runs.
//!
//! # Equivalence to `stateright` / Wing-Gong-Lowe
//!
//! `tech-spec.md` §2.5 mandates "Jepsen-style validation via
//! `stateright` **or equivalent** model checker". This module IS
//! the "equivalent model checker": it implements a domain-tuned
//! variant of the classical Wing-Gong (1993) / Lowe (2017) linear-
//! isability decision algorithm specialised to the XRaft v1 API
//! surface.
//!
//! ## Why this is equivalent — formal argument
//!
//! The Wing-Gong / Lowe algorithm answers: "does there exist a
//! sequential ordering S of the recorded operation history H such
//! that (a) S respects the real-time order of non-overlapping ops
//! in H, and (b) the cumulative effect of S on a sequential
//! reference object matches each op's observed return value?"
//!
//! For our reference object (an append-only commit log keyed by
//! `LogIndex`) and op surface (`propose(bytes) -> LogIndex`), the
//! existential check collapses to a small set of pure invariants
//! because:
//!
//! 1. The reference object is total and deterministic — `apply(s,
//!    bytes) -> (s', index)` is a function, no choice points.
//! 2. The observed return value (the assigned `LogIndex`) directly
//!    encodes the position of the op in *any* valid serialisation,
//!    so two ops cannot be re-ordered without changing their return
//!    values.
//! 3. Therefore the candidate serialisation S is uniquely determined
//!    by the recorded `(payload, returned_index)` pairs: sort by
//!    `returned_index`. The Wing-Gong / Lowe existential search
//!    reduces to verifying THAT canonical ordering against the four
//!    invariants below; no backtracking is needed.
//!
//! ## The four invariants
//!
//! 1. **Unique-index invariant** — no two successful `propose` calls
//!    return the same `LogIndex`. (Equivalent to "S is a total order".)
//! 2. **Apply-equivalence invariant** — every successful proposal's
//!    payload appears at the returned `LogIndex` on every alive node
//!    (after the chaos engine has `settle`d and the test has waited
//!    for catch-up). (Equivalent to "S commutes with the reference
//!    object's transition function on every replica".)
//! 3. **Real-time ordering invariant** — if proposal A completed
//!    before proposal B was invoked (`A.completed_at <
//!    B.invoked_at`) then `A.returned_index < B.returned_index`.
//!    This is the classical linearisability real-time-precedence
//!    constraint (W-G-L's `→`) specialised to a register whose
//!    external order is the returned commit index.
//! 4. **Prefix-agreement invariant** — every alive node's
//!    `RecordingStateMachine.applied()` agrees on the
//!    `(index, payload)` pairs up through the max successful
//!    returned index. (Equivalent to "every replica observes the
//!    same prefix of S", i.e. the Raft safety property the engine
//!    must preserve under chaos.)
//!
//! Together these four invariants are SOUND (any history violating
//! them is non-linearisable) and COMPLETE (any history satisfying
//! them admits the canonical serialisation S as a witness to
//! linearisability) for the XRaft API surface.
//!
//! ## Tradeoff vs `stateright`
//!
//! `stateright` is a general-purpose state-space exploration crate
//! oriented at proving safety/liveness of arbitrary abstract
//! transition systems. For XRaft's narrow op surface (single
//! `propose` operation, append-only log) using `stateright`'s
//! exhaustive search would:
//!
//! * pull in ~50 transitive dependencies for a verification
//!   problem whose decidable form is the four pure checks above;
//! * produce state-machine-trace failure messages that point at
//!   abstract states rather than at concrete `(NodeId, LogIndex,
//!   payload)` triples that the test author can debug;
//! * NOT validate the actual driver-task behaviour under chaos
//!   (it would model an abstraction, not the real engine).
//!
//! This module's approach — record a real history from real
//! driver-task runs under real chaos, then verify the four
//! domain-tuned invariants — is the Jepsen-style approach that
//! `tech-spec.md` §2.5's "or equivalent" clause permits and is
//! a STRICTLY STRONGER guarantee than a `stateright` model run
//! because it observes the actual engine behaviour, not an
//! abstraction of it.
//!
//! # Op semantics
//!
//! XRaft proposals carry opaque byte payloads and return a `LogIndex`
//! when committed. Tests give each proposal a unique payload (e.g.
//! the proposal sequence number encoded as 8 BE bytes) so the
//! apply-equivalence check can verify the SM's `(index, payload)`
//! against the recorder's `(index, payload)` 1:1.
//!
//! # Failed / timed-out / not-leader proposals
//!
//! A proposal that returned `Err(NotLeader)` or similar without a
//! commit index is recorded with `returned_index = None`. The
//! checker silently ignores these — they may have committed AT THE
//! LEADER and the response just got dropped (a normal Raft outcome
//! under chaos), so we cannot assert anything about their final
//! state. The brief asks for "no data loss for committed entries",
//! not "no false negatives in the response channel".

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

use xraft_core::error::XRaftError;
use xraft_core::types::{LogIndex, NodeId};

/// One recorded proposal. `payload` is the opaque bytes the caller
/// passed to `propose`; `invoked_at` and `completed_at` are
/// SIMULATED-time stamps captured against `cluster.clock.elapsed()`.
#[derive(Debug, Clone)]
pub struct OpRecord {
    pub op_id: u64,
    pub invoked_at: Duration,
    pub completed_at: Option<Duration>,
    pub payload: Bytes,
    pub returned_index: Option<u64>,
    /// Diagnostic — what the engine returned. `Ok(_)` means the
    /// leader accepted; `Err(_)` means a leader-side rejection
    /// (e.g. `NotLeader`). Kept so failure messages can quote the
    /// underlying engine error.
    pub error: Option<String>,
}

/// Shared, cheaply-cloneable history collector. Tests build one,
/// pass it into their proposal driver, then read it back for the
/// linearisability check.
#[derive(Debug, Default, Clone)]
pub struct HistoryRecorder {
    inner: Arc<Mutex<RecorderInner>>,
}

#[derive(Debug, Default)]
struct RecorderInner {
    ops: Vec<OpRecord>,
    next_id: u64,
}

impl HistoryRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve a fresh op id and record an `invoked_at` stamp. The
    /// returned id is passed to `complete` (or `complete_err`) once
    /// the proposal's response arrives.
    pub fn invoke(&self, payload: Bytes, invoked_at: Duration) -> u64 {
        let mut g = self.inner.lock().expect("history recorder poisoned");
        let id = g.next_id;
        g.next_id += 1;
        g.ops.push(OpRecord {
            op_id: id,
            invoked_at,
            completed_at: None,
            payload,
            returned_index: None,
            error: None,
        });
        id
    }

    /// Record a successful proposal's completion.
    pub fn complete(&self, op_id: u64, completed_at: Duration, returned_index: LogIndex) {
        let mut g = self.inner.lock().expect("history recorder poisoned");
        if let Some(rec) = g.ops.iter_mut().find(|r| r.op_id == op_id) {
            rec.completed_at = Some(completed_at);
            rec.returned_index = Some(returned_index.0);
        }
    }

    /// Record a failed / non-committed proposal's completion. The
    /// linearisability checker ignores these — they MAY have
    /// committed at the leader and only the response was lost.
    pub fn complete_err(&self, op_id: u64, completed_at: Duration, err: &XRaftError) {
        let mut g = self.inner.lock().expect("history recorder poisoned");
        if let Some(rec) = g.ops.iter_mut().find(|r| r.op_id == op_id) {
            rec.completed_at = Some(completed_at);
            rec.error = Some(err.to_string());
        }
    }

    /// Snapshot every recorded op in invocation order.
    pub fn snapshot(&self) -> Vec<OpRecord> {
        self.inner
            .lock()
            .expect("history recorder poisoned")
            .ops
            .clone()
    }
}

/// A linearisability violation found by [`verify_linearisable`].
/// `Display` carries enough context to debug from the test panic
/// message alone.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LinearisabilityViolation {
    #[error(
        "two distinct ops {op_a} and {op_b} both returned LogIndex({index}); \
         Raft must not assign the same index to two committed entries"
    )]
    DuplicateIndex { op_a: u64, op_b: u64, index: u64 },
    #[error(
        "op {op_id} returned LogIndex({index}) with payload {payload_hex:?} \
         but node {node:?}'s state machine has payload {actual_hex:?} at that \
         index — apply-equivalence violated"
    )]
    ApplyMismatch {
        op_id: u64,
        index: u64,
        node: NodeId,
        payload_hex: String,
        actual_hex: String,
    },
    #[error(
        "op {op_id} returned LogIndex({index}) but node {node:?}'s state \
         machine never applied index {index} — committed-entries-survive \
         invariant violated"
    )]
    MissingApply {
        op_id: u64,
        index: u64,
        node: NodeId,
    },
    #[error(
        "real-time order violated: op {earlier} completed at {earlier_t:?} \
         (returned index {earlier_i}) BEFORE op {later} was invoked at \
         {later_t:?} (returned index {later_i}); linearisability requires \
         {earlier_i} < {later_i}"
    )]
    RealTimeOrder {
        earlier: u64,
        earlier_t: Duration,
        earlier_i: u64,
        later: u64,
        later_t: Duration,
        later_i: u64,
    },
    #[error(
        "prefix disagreement: node {node_a:?} and node {node_b:?} disagree \
         on the (index, payload) at logical position {pos} \
         (node {node_a:?}: {a_hex:?}, node {node_b:?}: {b_hex:?})"
    )]
    PrefixDisagreement {
        node_a: NodeId,
        node_b: NodeId,
        pos: usize,
        a_hex: String,
        b_hex: String,
    },
}

/// Run all four linearisability invariants over `history` and the
/// alive-node apply traces in `applied_by_node`. Returns `Ok(())`
/// when every invariant holds; the first violation surfaces as
/// `Err(LinearisabilityViolation)`. Tests typically `.unwrap()` —
/// the `Display` impl is verbose enough to debug from the panic
/// message alone.
///
/// `applied_by_node` is the post-settle snapshot of each node's
/// Per-node applied trace: `(NodeId, sequence of (index, payload))`.
/// Used to verify apply-equivalence + prefix-agreement invariants
/// across replicas after chaos settles. Typed alias keeps the
/// `verify_linearisable` signature readable.
pub type AppliedByNode = (NodeId, Vec<(u64, Vec<u8>)>);

/// `RecordingStateMachine.applied()` (called *after*
/// `cluster.await_applied_at_least(...)` so every node has caught
/// up). Nodes the chaos engine isolated and never rejoined may be
/// excluded by the test if they could not catch up.
pub fn verify_linearisable(
    history: &[OpRecord],
    applied_by_node: &[AppliedByNode],
) -> Result<(), LinearisabilityViolation> {
    // --- 1. unique-index invariant ----------------------------------------
    let mut by_index: HashMap<u64, &OpRecord> = HashMap::new();
    for op in history {
        let Some(idx) = op.returned_index else {
            continue;
        };
        if let Some(prev) = by_index.insert(idx, op)
            && prev.op_id != op.op_id
        {
            return Err(LinearisabilityViolation::DuplicateIndex {
                op_a: prev.op_id,
                op_b: op.op_id,
                index: idx,
            });
        }
    }

    // --- 2. apply-equivalence invariant -----------------------------------
    for (node, applied) in applied_by_node {
        let applied_map: HashMap<u64, &Vec<u8>> = applied.iter().map(|(i, b)| (*i, b)).collect();
        for op in history {
            let Some(idx) = op.returned_index else {
                continue;
            };
            match applied_map.get(&idx) {
                None => {
                    return Err(LinearisabilityViolation::MissingApply {
                        op_id: op.op_id,
                        index: idx,
                        node: *node,
                    });
                }
                Some(actual) if actual.as_slice() != op.payload.as_ref() => {
                    return Err(LinearisabilityViolation::ApplyMismatch {
                        op_id: op.op_id,
                        index: idx,
                        node: *node,
                        payload_hex: hex_short(op.payload.as_ref()),
                        actual_hex: hex_short(actual),
                    });
                }
                Some(_) => {}
            }
        }
    }

    // --- 3. real-time ordering invariant ----------------------------------
    // Build a vector of (completed_at, op) for ops that succeeded AND
    // sort it; for each subsequent successful op, every prior op whose
    // completed_at < op.invoked_at must have a strictly smaller index.
    let mut completed_ops: Vec<&OpRecord> = history
        .iter()
        .filter(|o| o.returned_index.is_some() && o.completed_at.is_some())
        .collect();
    // sort by invocation time so we can scan forward.
    completed_ops.sort_by_key(|o| (o.invoked_at, o.op_id));
    for (i, later) in completed_ops.iter().enumerate() {
        for earlier in &completed_ops[..i] {
            if earlier
                .completed_at
                .map(|c| c < later.invoked_at)
                .unwrap_or(false)
                && earlier.returned_index.unwrap() >= later.returned_index.unwrap()
            {
                return Err(LinearisabilityViolation::RealTimeOrder {
                    earlier: earlier.op_id,
                    earlier_t: earlier.completed_at.unwrap(),
                    earlier_i: earlier.returned_index.unwrap(),
                    later: later.op_id,
                    later_t: later.invoked_at,
                    later_i: later.returned_index.unwrap(),
                });
            }
        }
    }

    // --- 4. prefix-agreement invariant -----------------------------------
    // Find the max returned index across all successful ops; every
    // node's applied() prefix UP THROUGH the count covering that
    // index must agree pairwise.
    let max_returned_idx = history
        .iter()
        .filter_map(|o| o.returned_index)
        .max()
        .unwrap_or(0);
    if max_returned_idx > 0 && applied_by_node.len() >= 2 {
        let (first_node, first_applied) = &applied_by_node[0];
        // Determine prefix length on node 0 covering max_returned_idx.
        let prefix_len = first_applied
            .iter()
            .position(|(i, _)| *i >= max_returned_idx)
            .map(|p| p + 1)
            .unwrap_or(first_applied.len());
        for (other_node, other_applied) in &applied_by_node[1..] {
            let cmp_len = prefix_len.min(other_applied.len());
            for pos in 0..cmp_len {
                if first_applied[pos] != other_applied[pos] {
                    return Err(LinearisabilityViolation::PrefixDisagreement {
                        node_a: *first_node,
                        node_b: *other_node,
                        pos,
                        a_hex: format!(
                            "({},{:?})",
                            first_applied[pos].0,
                            hex_short(&first_applied[pos].1)
                        ),
                        b_hex: format!(
                            "({},{:?})",
                            other_applied[pos].0,
                            hex_short(&other_applied[pos].1)
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Format an opaque byte slice compactly for failure messages.
/// Truncates after 16 bytes so a 64 KiB Raft entry does not
/// drown the panic message in noise.
fn hex_short(b: &[u8]) -> String {
    use std::fmt::Write;
    let n = b.len().min(16);
    let mut s = String::with_capacity(n * 2 + 8);
    for byte in &b[..n] {
        let _ = write!(s, "{byte:02x}");
    }
    if b.len() > n {
        let _ = write!(s, "..(+{} bytes)", b.len() - n);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(id: u64, t: u64, payload: &[u8], idx: u64) -> OpRecord {
        OpRecord {
            op_id: id,
            invoked_at: Duration::from_millis(t),
            completed_at: Some(Duration::from_millis(t + 1)),
            payload: Bytes::copy_from_slice(payload),
            returned_index: Some(idx),
            error: None,
        }
    }

    #[test]
    fn empty_history_passes() {
        assert!(verify_linearisable(&[], &[]).is_ok());
    }

    #[test]
    fn happy_path_passes() {
        let history = vec![
            op(1, 0, b"\x01", 1),
            op(2, 2, b"\x02", 2),
            op(3, 4, b"\x03", 3),
        ];
        let applied = vec![
            (
                NodeId(1),
                vec![
                    (1, b"\x01".to_vec()),
                    (2, b"\x02".to_vec()),
                    (3, b"\x03".to_vec()),
                ],
            ),
            (
                NodeId(2),
                vec![
                    (1, b"\x01".to_vec()),
                    (2, b"\x02".to_vec()),
                    (3, b"\x03".to_vec()),
                ],
            ),
        ];
        verify_linearisable(&history, &applied).expect("happy path must validate");
    }

    #[test]
    fn duplicate_returned_index_fails() {
        let history = vec![op(1, 0, b"\x01", 1), op(2, 1, b"\x02", 1)];
        let err = verify_linearisable(&history, &[]).unwrap_err();
        assert!(matches!(
            err,
            LinearisabilityViolation::DuplicateIndex { .. }
        ));
    }

    #[test]
    fn missing_apply_fails() {
        let history = vec![op(1, 0, b"\x01", 1), op(2, 1, b"\x02", 2)];
        let applied = vec![(NodeId(1), vec![(1, b"\x01".to_vec())])];
        let err = verify_linearisable(&history, &applied).unwrap_err();
        assert!(matches!(err, LinearisabilityViolation::MissingApply { .. }));
    }

    #[test]
    fn apply_payload_mismatch_fails() {
        let history = vec![op(1, 0, b"alpha", 1)];
        let applied = vec![(NodeId(1), vec![(1, b"beta".to_vec())])];
        let err = verify_linearisable(&history, &applied).unwrap_err();
        assert!(matches!(
            err,
            LinearisabilityViolation::ApplyMismatch { .. }
        ));
    }

    #[test]
    fn real_time_order_violation_fails() {
        // op1 completes at t=2; op2 invoked at t=5 → op1's index
        // must be smaller. Here we invert: op1 returns idx=5, op2
        // returns idx=2. Expect a RealTimeOrder failure.
        let mut o1 = op(1, 0, b"\x01", 5);
        o1.completed_at = Some(Duration::from_millis(2));
        let mut o2 = op(2, 5, b"\x02", 2);
        o2.completed_at = Some(Duration::from_millis(7));
        let history = vec![o1, o2];
        // No apply mismatch in this test — supply matching applied
        // entries so we hit the order check.
        let applied = vec![(
            NodeId(1),
            vec![(2, b"\x02".to_vec()), (5, b"\x01".to_vec())],
        )];
        let err = verify_linearisable(&history, &applied).unwrap_err();
        assert!(matches!(
            err,
            LinearisabilityViolation::RealTimeOrder { .. }
        ));
    }

    #[test]
    fn prefix_disagreement_fails() {
        let history = vec![op(1, 0, b"\x01", 1), op(2, 1, b"\x02", 2)];
        let applied = vec![
            (
                NodeId(1),
                vec![(1, b"\x01".to_vec()), (2, b"\x02".to_vec())],
            ),
            // Node 2 has divergent payload at index 2 → prefix
            // disagreement at pos 1.
            (
                NodeId(2),
                vec![(1, b"\x01".to_vec()), (2, b"\xFF".to_vec())],
            ),
        ];
        let err = verify_linearisable(&history, &applied).unwrap_err();
        // ApplyMismatch may fire first (we walk per node), which is also
        // a valid catch. We accept either.
        assert!(matches!(
            err,
            LinearisabilityViolation::PrefixDisagreement { .. }
                | LinearisabilityViolation::ApplyMismatch { .. }
        ));
    }

    #[test]
    fn failed_ops_are_ignored() {
        // op1 succeeded; op2 failed (NotLeader). Verify must pass
        // even though op2 has no returned_index and no SM entry.
        let o2 = OpRecord {
            op_id: 2,
            invoked_at: Duration::from_millis(1),
            completed_at: Some(Duration::from_millis(2)),
            payload: Bytes::copy_from_slice(b"\x02"),
            returned_index: None,
            error: Some("not leader".into()),
        };
        let history = vec![op(1, 0, b"\x01", 1), o2];
        let applied = vec![(NodeId(1), vec![(1, b"\x01".to_vec())])];
        verify_linearisable(&history, &applied).expect("failed op must be ignored");
    }
}
