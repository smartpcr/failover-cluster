//! Core type definitions shared across the xraft consensus implementation.
//!
//! This module is the **single canonical home** for [`Term`] and
//! [`ClusterId`]. All other definitions of these types elsewhere in
//! the crate must be removed in favour of importing from here.
//!
//! Two related types are intentionally **not** defined here:
//!
//! * `Role` — owned by `consensus_state`. The previous version of this
//!   file also defined it, which caused a duplicate-definition compile
//!   error. The cross-module duplication between `consensus_state.rs`
//!   and `node_state.rs` is a separate consolidation, tracked outside
//!   the scope of this fix.
//! * `VoterInfo` — owned by `voter`. Same reasoning as `Role`.
//!
//! We deliberately do **not** re-export them from this module either:
//! adding a re-export now could mask, or be invalidated by, the
//! follow-up cross-module consolidation.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Monotonically increasing Raft term number.
///
/// A new term is started every time a node becomes a candidate. Higher
/// terms always supersede lower ones, so the comparison operators
/// derived below are the primary way the consensus layer reasons about
/// leadership generations.
///
/// `Term` is a transparent newtype around `u64`. The `#[serde(transparent)]`
/// attribute guarantees the on-the-wire and on-disk representation is
/// indistinguishable from a bare `u64`, so this type is safe to use in
/// log entries, snapshots, and RPCs without a format migration.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct Term(pub u64);

impl Term {
    /// The term used before any election has succeeded.
    pub const ZERO: Term = Term(0);

    /// Returns the next term, used when starting an election.
    #[must_use]
    pub fn next(self) -> Term {
        Term(self.0 + 1)
    }

    /// Returns the raw `u64` value of this term.
    #[must_use]
    pub fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.0)
    }
}

impl From<u64> for Term {
    fn from(value: u64) -> Self {
        Term(value)
    }
}

impl From<Term> for u64 {
    fn from(term: Term) -> Self {
        term.0
    }
}

/// Globally-unique cluster identifier.
///
/// A `ClusterId` wraps a [`Uuid`] so that cluster identity is
/// collision-free across deployments and survives node restarts. The
/// failover-cluster surface carries GUIDs end-to-end (matching Windows
/// Failover Cluster semantics), so a UUID-backed identifier is the
/// canonical representation; the previous `u64`-backed variant has been
/// removed.
///
/// `#[serde(transparent)]` makes this serialize as a plain UUID, so the
/// wire/disk format matches `Uuid` directly.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    /// Wraps an existing UUID as a `ClusterId`.
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        ClusterId(uuid)
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Hyphenated lower-case representation, matching the rest of
        // the failover-cluster tooling and Windows GUID rendering.
        write!(f, "{}", self.0.hyphenated())
    }
}

impl From<Uuid> for ClusterId {
    fn from(uuid: Uuid) -> Self {
        ClusterId(uuid)
    }
}

impl From<ClusterId> for Uuid {
    fn from(id: ClusterId) -> Self {
        id.0
    }
}
