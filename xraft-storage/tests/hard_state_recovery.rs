// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// Licensed under the MIT License.
// -----------------------------------------------------------------------

//! Stage 2.2 acceptance scenarios at the **integration boundary**:
//! `FileHardStateStore` (xraft-storage) <-> `HardState` (xraft-core)
//! <-> `RaftNode::new_with_initial_hard_state` (xraft-core).
//!
//! The unit tests inside `xraft-storage/src/state.rs` already cover the
//! storage-side invariants in isolation (term monotonicity, single vote
//! per term, atomic-replace + .bak recovery, schema versioning, orphan
//! .tmp cleanup). The acceptance plan (`implementation-plan.md` Stage
//! 2.2) and the architecture doc, however, describe a driver-level
//! handshake between the store and the consensus engine:
//!
//!   1. Driver opens `FileHardStateStore::open(dir)`.
//!   2. Driver calls `store.load()?` to retrieve the most recently
//!      persisted state, mapping `Ok(None)` to `HardState::default()`
//!      so the first-boot path needs no special-case.
//!   3. Driver constructs the `RaftNode` via
//!      `RaftNode::new_with_initial_hard_state(config, recovered)` so
//!      the engine comes up at the exact (term, voted_for) the disk
//!      committed -- never voting twice in the same term across
//!      restarts.
//!
//! These are integration tests because they exercise both crates end
//! to end; placing them in `xraft-storage/tests/` (a separate test
//! crate) means they consume only the public APIs of both crates,
//! mirroring how a real driver in `xraft-server` will wire them.

use std::fs;

use tempfile::TempDir;
use uuid::Uuid;
use xraft_core::config::ClusterConfig;
use xraft_core::error::XRaftError;
use xraft_core::node::RaftNode;
use xraft_core::storage::HardStateStore;
use xraft_core::types::{HardState, LogIndex, NodeId, NodeRole, Term};
use xraft_storage::FileHardStateStore;

// ---------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------

/// Build a deterministic 3-voter `ClusterConfig` rooted at this node
/// (node_id = 1) with fresh UUIDs per call so each test gets an
/// independent voter directory. Mirrors the helper in
/// `xraft-core/src/node.rs` test module so behaviour matches the
/// in-crate scenarios. UUIDs are required by `from_toml_str` voter
/// parsing -- supplying random values is sufficient for these tests
/// (none of which exercise directory-id validation specifically).
fn three_voter_config() -> ClusterConfig {
    let toml = format!(
        r#"
node_id = 1
cluster_id = "stage-2-2-recovery"
listen_addr = "0.0.0.0:6000"
tick_interval_ms = 10
election_timeout_min_ms = 100
election_timeout_max_ms = 200

[[voters]]
node_id = 1
directory_id = "{}"
host = "node1"
port = 6000

[[voters]]
node_id = 2
directory_id = "{}"
host = "node2"
port = 6001

[[voters]]
node_id = 3
directory_id = "{}"
host = "node3"
port = 6002
"#,
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    ClusterConfig::from_toml_str(&toml).expect("test fixture must parse")
}

// ---------------------------------------------------------------------
// Stage 2.2 acceptance scenarios
// ---------------------------------------------------------------------

/// First-boot driver pattern: open an empty store, `load()` returns
/// `Ok(None)`, mapped to `HardState::default()`, and the resulting
/// `RaftNode` comes up at term 0 with no vote -- observationally
/// equivalent to a node constructed with `RaftNode::new_with_seed`.
///
/// This is the path a brand-new cluster member takes on its very first
/// start. The test ensures the driver does not need a special-case for
/// "no prior state" beyond `unwrap_or_default()`.
#[test]
fn integration_first_boot_load_then_construct_node() {
    let tmp = TempDir::new().unwrap();
    let store = FileHardStateStore::open(tmp.path()).expect("open empty");
    let recovered = store
        .load()
        .expect("load on empty must succeed")
        .unwrap_or_default();
    assert_eq!(
        recovered,
        HardState::default(),
        "empty store must surface HardState::default on first boot",
    );

    let node = RaftNode::new_with_seed_and_initial_hard_state(
        three_voter_config(),
        /*seed=*/ 1,
        recovered,
    )
    .expect("constructing a node from default HardState must succeed");

    assert_eq!(node.current_term(), Term(0));
    assert_eq!(node.hard_state.voted_for, None);
    assert_eq!(node.role, NodeRole::Follower);
    assert!(!node.is_leader());
    assert!(node.leader_id.is_none());
    // Volatile state stays at zero per architecture.md §3.3.
    assert_eq!(node.commit_index, LogIndex(0));
    assert_eq!(node.last_applied, LogIndex(0));
}

/// `state-persistence` (implementation-plan Stage 2.2 Test Scenarios):
/// persist a non-trivial HardState, drop the store (clean shutdown),
/// reopen, load, and construct a `RaftNode`. The node must come up at
/// the exact (term, voted_for) the disk committed.
#[test]
fn integration_recover_persisted_state_after_clean_shutdown() {
    let tmp = TempDir::new().unwrap();
    let target = HardState {
        current_term: Term(7),
        voted_for: Some(NodeId(2)),
    };

    // Phase 1: open, persist, drop (clean shutdown).
    {
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        store
            .persist(&target)
            .expect("persist of a fresh HardState must succeed");
    }

    // Phase 2: reopen, load, construct node.
    let store = FileHardStateStore::open(tmp.path()).expect("reopen after drop");
    let loaded = store
        .load()
        .expect("load after reopen")
        .expect("must recover the previously persisted state");
    assert_eq!(loaded, target);

    let node = RaftNode::new_with_seed_and_initial_hard_state(
        three_voter_config(),
        /*seed=*/ 42,
        loaded,
    )
    .unwrap();

    assert_eq!(node.current_term(), Term(7));
    assert_eq!(node.hard_state.voted_for, Some(NodeId(2)));
    // Recovery still produces a Follower; promotion only happens after
    // a real election. This is a Stage 2.2 invariant: we never auto-
    // promote on restart, even if the recovered state implies the node
    // was leader before the crash.
    assert_eq!(node.role, NodeRole::Follower);
    assert!(!node.is_leader());
}

/// `atomic-write-safety` (implementation-plan Stage 2.2 Test Scenarios):
/// stage the worktree as if a crash occurred between step 2 (rename
/// canonical -> .bak) and step 3 (rename tmp -> canonical) of the
/// atomic-replace sequence. On reopen, the store must promote the
/// `.bak` back to canonical and the node must come up at the
/// previously-committed (term, voted_for) -- not at default.
#[test]
fn integration_recover_after_simulated_crash_between_rename_steps() {
    let tmp = TempDir::new().unwrap();
    let committed = HardState {
        current_term: Term(3),
        voted_for: Some(NodeId(1)),
    };

    // Persist a valid state, then hand-stage the post-crash layout.
    {
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        store.persist(&committed).unwrap();
    }
    let canonical = tmp.path().join("quorum-state");
    let bak = tmp.path().join("quorum-state.bak");
    fs::rename(&canonical, &bak).expect("simulate crash window");
    assert!(!canonical.exists(), "precondition: canonical removed");
    assert!(bak.exists(), "precondition: .bak in place");

    // Reopen: store must promote .bak -> canonical, load must return the
    // committed state, and the node must come up at the committed term.
    let store = FileHardStateStore::open(tmp.path()).expect("reopen with .bak only");
    let loaded = store
        .load()
        .unwrap()
        .expect("crash recovery must surface the .bak state");
    assert_eq!(loaded, committed);
    assert!(
        canonical.exists(),
        ".bak must be promoted to canonical on open"
    );
    assert!(!bak.exists(), ".bak must be consumed by recovery");

    let node = RaftNode::new_with_seed_and_initial_hard_state(
        three_voter_config(),
        /*seed=*/ 99,
        loaded,
    )
    .unwrap();
    assert_eq!(node.current_term(), Term(3));
    assert_eq!(node.hard_state.voted_for, Some(NodeId(1)));
}

/// `term-monotonicity` (implementation-plan Stage 2.2 Test Scenarios):
/// once a higher term has been persisted, attempting to persist a
/// lower term must be rejected by the store with
/// `XRaftError::Storage`. The on-disk state must remain at the
/// higher term so a subsequent reopen-and-recover round-trip
/// continues to drive the node into the correct term.
#[test]
fn integration_term_regression_rejected_and_state_preserved() {
    let tmp = TempDir::new().unwrap();
    let high = HardState {
        current_term: Term(10),
        voted_for: Some(NodeId(2)),
    };
    let regression = HardState {
        current_term: Term(5),
        voted_for: None,
    };

    let mut store = FileHardStateStore::open(tmp.path()).unwrap();
    store.persist(&high).expect("first persist");

    let err = store
        .persist(&regression)
        .expect_err("term regression must be rejected");
    assert!(
        matches!(err, XRaftError::Storage(_)),
        "expected XRaftError::Storage variant, got {err:?}",
    );

    // Drop store (simulates clean shutdown after the rejected attempt).
    drop(store);

    // Reopen and verify the on-disk state is still the high term --
    // proving validation ran BEFORE atomic_write touched the disk.
    let store = FileHardStateStore::open(tmp.path()).unwrap();
    let recovered = store.load().unwrap().expect("must recover prior state");
    assert_eq!(
        recovered, high,
        "rejected persist must not corrupt prior on-disk state",
    );

    // And the engine constructed from that state must agree.
    let node = RaftNode::new_with_seed_and_initial_hard_state(
        three_voter_config(),
        /*seed=*/ 7,
        recovered,
    )
    .unwrap();
    assert_eq!(node.current_term(), Term(10));
    assert_eq!(node.hard_state.voted_for, Some(NodeId(2)));
}

/// Multi-step driver workflow demonstrating the full Stage 2.2 cycle:
/// boot fresh -> grant a vote -> bump the term -> grant a new vote ->
/// crash -> reopen -> reconstruct node. After each persist the engine
/// constructed from the latest disk state matches the latest persisted
/// `HardState` exactly, validating that the store + node combination
/// is a faithful end-to-end implementation of Raft's persistent state
/// requirement.
#[test]
fn integration_multi_step_persist_then_recover_end_to_end() {
    let tmp = TempDir::new().unwrap();

    // Boot 1: empty store, term 0, no vote.
    {
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        let s = store.load().unwrap().unwrap_or_default();
        assert_eq!(s, HardState::default());
        let n = RaftNode::new_with_seed_and_initial_hard_state(three_voter_config(), 1, s).unwrap();
        assert_eq!(n.current_term(), Term(0));
    }

    // Boot 2: persist (term=1, vote=Some(3)).
    let after_first = HardState {
        current_term: Term(1),
        voted_for: Some(NodeId(3)),
    };
    {
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        store.persist(&after_first).unwrap();
    }

    // Boot 3: reopen, verify, persist (term=2, vote=Some(2)).
    let after_second = HardState {
        current_term: Term(2),
        voted_for: Some(NodeId(2)),
    };
    {
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, after_first);
        store.persist(&after_second).unwrap();
    }

    // Boot 4 (after a simulated crash): reopen, verify the latest state
    // wins, construct the node and confirm it comes up at term 2 with
    // the term-2 vote -- not the stale term-1 vote.
    {
        let store = FileHardStateStore::open(tmp.path()).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, after_second);
        let n = RaftNode::new_with_seed_and_initial_hard_state(three_voter_config(), 2, loaded)
            .unwrap();
        assert_eq!(n.current_term(), Term(2));
        assert_eq!(n.hard_state.voted_for, Some(NodeId(2)));
        assert_eq!(n.role, NodeRole::Follower);
    }
}

/// Mid-term restart with NO vote yet granted: the recovered node must
/// still come up at the bumped term (so it doesn't vote for a stale
/// candidate) but with `voted_for = None` (so a same-term legitimate
/// candidate can still win this node's vote post-restart).
#[test]
fn integration_recover_term_without_vote_keeps_voting_eligibility() {
    let tmp = TempDir::new().unwrap();
    let recovered = HardState {
        current_term: Term(12),
        voted_for: None,
    };

    {
        let mut store = FileHardStateStore::open(tmp.path()).unwrap();
        store.persist(&recovered).unwrap();
    }

    let store = FileHardStateStore::open(tmp.path()).unwrap();
    let loaded = store.load().unwrap().unwrap();
    assert_eq!(loaded, recovered);

    let node =
        RaftNode::new_with_seed_and_initial_hard_state(three_voter_config(), 7, loaded).unwrap();
    assert_eq!(node.current_term(), Term(12));
    assert!(
        node.hard_state.voted_for.is_none(),
        "voted_for must remain None so the node can grant a fresh vote post-restart",
    );
}
