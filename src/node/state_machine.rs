//! The Raft node state machine.
//!
//! This module hosts [`RaftNode`], the deterministic Mealy machine
//! that implements role transitions and safety invariants. It is
//! purely functional: every observable side-effect is returned as a
//! [`Command`] for the host runtime to execute.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::NodeConfig;
use crate::error::{RaftError, RaftResult};
use crate::node::command::Command;
use crate::node::event::Event;
use crate::node::role::{Role, RoleKind};
use crate::node::state::{LogMetadataCache, PersistentState, VolatileState};
use crate::types::{LogIndex, LogMetadata, NodeId, Term};

/// A single Raft node, modelled as a deterministic state machine.
#[derive(Debug, Clone)]
pub struct RaftNode {
    config: NodeConfig,
    persistent: PersistentState,
    volatile: VolatileState,
    log_cache: LogMetadataCache,
    role: Role,
    shut_down: bool,
}

impl RaftNode {
    /// Construct a Raft node from previously-persisted consensus
    /// state and the current log tail metadata.
    ///
    /// On startup, a node always begins in either `Follower` (if it
    /// is a voter) or `Observer` (otherwise). Recovery of a leader
    /// from disk is impossible: the term it owned has either lapsed
    /// or the other voters will accept its leadership again through
    /// a normal election.
    pub fn new(
        config: NodeConfig,
        persistent: PersistentState,
        log_metadata: LogMetadata,
    ) -> RaftResult<Self> {
        let role = if config.is_voter() {
            Role::fresh_follower()
        } else {
            Role::Observer
        };
        let mut log_cache = LogMetadataCache::EMPTY;
        log_cache.update(log_metadata);
        Ok(Self {
            config,
            persistent,
            volatile: VolatileState::INITIAL,
            log_cache,
            role,
            shut_down: false,
        })
    }

    // ---------- accessors ----------

    /// Borrow the configuration this node was constructed with.
    #[must_use]
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Current role (Follower / Candidate / etc.).
    #[must_use]
    pub fn role(&self) -> &Role {
        &self.role
    }

    /// Convenience helper for tests / telemetry.
    #[must_use]
    pub fn role_kind(&self) -> RoleKind {
        self.role.kind()
    }

    /// Latest term the node has observed.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.persistent.current_term
    }

    /// Whom we voted for in `current_term`, if anyone.
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.persistent.voted_for
    }

    /// Identifier of the currently-recognised leader, if any.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.volatile.leader_id
    }

    /// Highest log index known to be committed.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.volatile.commit_index
    }

    /// Cached metadata for the local log's tail.
    #[must_use]
    pub fn log_metadata(&self) -> LogMetadata {
        self.log_cache.get()
    }

    /// Whether the node has been shut down.
    #[must_use]
    pub fn is_shut_down(&self) -> bool {
        self.shut_down
    }

    // ---------- main entry point ----------

    /// Deliver a single [`Event`] and return the resulting batch of
    /// [`Command`]s for the host to execute.
    #[allow(clippy::needless_pass_by_value)]
    pub fn handle(&mut self, event: Event) -> RaftResult<Vec<Command>> {
        if self.shut_down {
            return Err(RaftError::AlreadyShutDown);
        }

        let mut out = Vec::new();
        match event {
            Event::ElectionTimeout => self.on_election_timeout(&mut out),
            Event::HeartbeatTick => self.on_heartbeat_tick(&mut out),
            Event::RequestVoteRequest {
                pre_vote,
                candidate_id,
                candidate_term,
                candidate_log,
                local_election_timeout_elapsed,
            } => self.on_request_vote(
                pre_vote,
                candidate_id,
                candidate_term,
                candidate_log,
                local_election_timeout_elapsed,
                &mut out,
            ),
            Event::RequestVoteResponse {
                pre_vote,
                from,
                term,
                vote_granted,
            } => {
                self.on_request_vote_response(pre_vote, from, term, vote_granted, &mut out);
            }
            Event::AppendEntriesRequest {
                leader_id,
                leader_term,
                prev_log_index,
                prev_log_term,
                leader_commit,
                log_ok,
                entry_count,
            } => self.on_append_entries(
                leader_id,
                leader_term,
                prev_log_index,
                prev_log_term,
                leader_commit,
                log_ok,
                entry_count,
                &mut out,
            ),
            Event::AppendEntriesResponse {
                from,
                term,
                success,
                match_index,
            } => {
                self.on_append_entries_response(from, term, success, match_index, &mut out);
            }
            Event::LogTailUpdated { metadata } => {
                self.log_cache.update(metadata);
            }
            Event::PromoteToVoter { node } => self.on_promote(node, &mut out)?,
            Event::DemoteToObserver { node } => self.on_demote(node, &mut out)?,
            Event::Shutdown => {
                self.shut_down = true;
                out.push(Command::StopElectionTimer);
                out.push(Command::StopHeartbeatTimer);
            }
        }
        Ok(out)
    }

    // ============================================================
    // Event handlers
    // ============================================================

    fn on_election_timeout(&mut self, out: &mut Vec<Command>) {
        match self.role.kind() {
            RoleKind::Observer | RoleKind::Leader => {
                // Observers never run elections. Leaders shouldn't be
                // running an election timer at all, but defensively
                // no-op rather than panic.
            }
            RoleKind::Follower => {
                if self.config.pre_vote_enabled {
                    self.become_pre_candidate(out);
                } else {
                    self.become_candidate(out);
                }
            }
            RoleKind::PreCandidate => {
                // Pre-vote did not gather a majority before the timer
                // fired — fall back to Follower and let the host
                // randomise the next timeout.
                self.transition_to_follower(self.persistent.current_term, None, out, false);
            }
            RoleKind::Candidate => {
                // Split vote / no majority — start a fresh election in
                // a higher term, per Raft §5.2.
                self.become_candidate(out);
            }
        }
    }

    fn on_heartbeat_tick(&mut self, out: &mut Vec<Command>) {
        if let RoleKind::Leader = self.role.kind() {
            out.push(Command::BroadcastHeartbeat {
                term: self.persistent.current_term,
                leader_commit: self.volatile.commit_index,
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_request_vote(
        &mut self,
        pre_vote: bool,
        candidate_id: NodeId,
        candidate_term: Term,
        candidate_log: LogMetadata,
        local_election_timeout_elapsed: bool,
        out: &mut Vec<Command>,
    ) {
        // Membership guard: votes are only ever granted to voters.
        // Equally important — do not adopt a higher term reported by
        // a non-voter, since that would let an observer or stale
        // demoted node disrupt the cluster's term progression.
        if !self.config.voters.contains(&candidate_id) {
            out.push(Command::SendVoteResponse {
                pre_vote,
                to: candidate_id,
                term: self.persistent.current_term,
                vote_granted: false,
            });
            return;
        }

        // Higher term on a *real* vote forces step-down even when we
        // ultimately deny the vote (e.g. log not up-to-date). Pre-vote
        // probes must NOT mutate `current_term` or `voted_for`.
        if !pre_vote && candidate_term > self.persistent.current_term {
            self.transition_to_follower(candidate_term, None, out, true);
        }

        let current_term = self.persistent.current_term;

        // Reject stale-term requests immediately.
        if candidate_term < current_term {
            out.push(Command::SendVoteResponse {
                pre_vote,
                to: candidate_id,
                term: current_term,
                vote_granted: false,
            });
            return;
        }

        // Pre-Vote / Check-Quorum guard: a follower that has recently
        // heard from its leader (election timer NOT elapsed) MUST
        // reject pre-votes, otherwise a partitioned node could
        // repeatedly disrupt an otherwise healthy cluster.
        if pre_vote && !local_election_timeout_elapsed {
            out.push(Command::SendVoteResponse {
                pre_vote: true,
                to: candidate_id,
                term: current_term,
                vote_granted: false,
            });
            return;
        }

        // Observers never grant votes (they may still reply so the
        // candidate doesn't time out waiting).
        if matches!(self.role, Role::Observer) {
            out.push(Command::SendVoteResponse {
                pre_vote,
                to: candidate_id,
                term: current_term,
                vote_granted: false,
            });
            return;
        }

        // For real votes: if we have already voted this term for a
        // different candidate, reject.
        if !pre_vote {
            match self.persistent.voted_for {
                Some(prior) if prior != candidate_id => {
                    out.push(Command::SendVoteResponse {
                        pre_vote: false,
                        to: candidate_id,
                        term: current_term,
                        vote_granted: false,
                    });
                    return;
                }
                _ => {}
            }
        }

        // Up-to-date check (Raft §5.4.1).
        let log_up_to_date = self
            .log_cache
            .get()
            .is_at_least_as_up_to_date_as(candidate_log);
        if !log_up_to_date {
            out.push(Command::SendVoteResponse {
                pre_vote,
                to: candidate_id,
                term: current_term,
                vote_granted: false,
            });
            return;
        }

        if !pre_vote {
            self.persistent.voted_for = Some(candidate_id);
            out.push(Command::PersistState);
            // Granting a real vote resets the election timer (§5.2):
            // we treat it as recent contact from a legitimate
            // candidate.
            out.push(Command::ResetElectionTimer);
        }
        out.push(Command::SendVoteResponse {
            pre_vote,
            to: candidate_id,
            term: current_term,
            vote_granted: true,
        });
    }

    fn on_request_vote_response(
        &mut self,
        pre_vote: bool,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        out: &mut Vec<Command>,
    ) {
        // Higher term seen forces step-down regardless of pre/real
        // vote and regardless of whether the grant was positive.
        if term > self.persistent.current_term {
            self.transition_to_follower(term, None, out, true);
            return;
        }
        // Ignore stale responses.
        if term < self.persistent.current_term {
            return;
        }
        // Only tally if we're still in the matching role.
        let expected_kind = if pre_vote {
            RoleKind::PreCandidate
        } else {
            RoleKind::Candidate
        };
        if self.role.kind() != expected_kind {
            return;
        }
        if !vote_granted {
            return;
        }
        // Don't count votes from non-voters (e.g. observer that was
        // demoted after issuing its grant).
        if !self.config.voters.contains(&from) {
            return;
        }

        let won = match &mut self.role {
            Role::PreCandidate { votes_received } | Role::Candidate { votes_received } => {
                votes_received.insert(from);
                // Re-tally only votes from current voters — a node
                // demoted while we were candidating must not count.
                let live = votes_received
                    .iter()
                    .filter(|v| self.config.voters.contains(v))
                    .count();
                live >= self.config.quorum_size()
            }
            _ => unreachable!("role_kind guard above"),
        };

        if !won {
            return;
        }
        if pre_vote {
            self.become_candidate(out);
        } else {
            self.become_leader(out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        leader_id: NodeId,
        leader_term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        leader_commit: LogIndex,
        log_ok: bool,
        entry_count: u64,
        out: &mut Vec<Command>,
    ) {
        let _ = prev_log_term;
        let current_term = self.persistent.current_term;

        // Membership guard: a node not in the voter set cannot be
        // leader. Reject the append without adopting its term — this
        // prevents a removed-but-still-running former leader from
        // bumping our term and triggering an unnecessary election.
        if !self.config.voters.contains(&leader_id) {
            out.push(Command::SendAppendEntriesResponse {
                to: leader_id,
                term: current_term,
                success: false,
                match_index: LogIndex::ZERO,
            });
            return;
        }

        // Stale leader — reject without changing state.
        if leader_term < current_term {
            out.push(Command::SendAppendEntriesResponse {
                to: leader_id,
                term: current_term,
                success: false,
                match_index: LogIndex::ZERO,
            });
            return;
        }

        // Same- or higher-term legitimate leader: we MUST recognise
        // it. Step down regardless of `log_ok` — the log layer
        // separately decides whether the append is consistent, but
        // candidacy must end the moment we see another voter as
        // leader in our term.
        let term_changed = leader_term > current_term;
        if term_changed
            || matches!(
                self.role.kind(),
                RoleKind::PreCandidate | RoleKind::Candidate | RoleKind::Leader
            )
        {
            if matches!(self.role, Role::Observer) {
                self.volatile.leader_id = Some(leader_id);
                if term_changed && self.persistent.observe_term(leader_term) {
                    out.push(Command::PersistState);
                }
            } else {
                self.transition_to_follower(leader_term, Some(leader_id), out, term_changed);
            }
        } else {
            // Already a Follower in the same term — refresh leader
            // and timer.
            if let Role::Follower { leader_hint } = &mut self.role {
                *leader_hint = Some(leader_id);
            }
            self.volatile.leader_id = Some(leader_id);
            out.push(Command::ResetElectionTimer);
        }

        let (success, match_index) = if log_ok {
            (true, prev_log_index.0.saturating_add(entry_count).into())
        } else {
            (false, LogIndex::ZERO)
        };
        if success {
            let new_commit = std::cmp::min(leader_commit, match_index);
            self.volatile.advance_commit(new_commit);
        }

        out.push(Command::SendAppendEntriesResponse {
            to: leader_id,
            term: self.persistent.current_term,
            success,
            match_index,
        });
    }

    fn on_append_entries_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        out: &mut Vec<Command>,
    ) {
        if term > self.persistent.current_term {
            self.transition_to_follower(term, None, out, true);
            return;
        }
        if term < self.persistent.current_term {
            return; // stale
        }
        if !matches!(self.role.kind(), RoleKind::Leader) {
            return;
        }
        if let Role::Leader {
            next_index,
            match_index: mi,
        } = &mut self.role
        {
            if success {
                let entry = mi.entry(from).or_insert(LogIndex::ZERO);
                if match_index > *entry {
                    *entry = match_index;
                }
                next_index.insert(from, match_index.next());
            } else {
                let cur = *next_index.get(&from).unwrap_or(&LogIndex::new(1));
                let backed = LogIndex::new(cur.0.saturating_sub(1).max(1));
                next_index.insert(from, backed);
            }
        }
    }

    fn on_promote(&mut self, node: NodeId, out: &mut Vec<Command>) -> RaftResult<()> {
        if self.config.voters.contains(&node) {
            return Err(RaftError::InvalidMembershipChange {
                node,
                reason: "node is already a voter",
            });
        }
        if !self.config.observers.contains(&node) {
            return Err(RaftError::InvalidMembershipChange {
                node,
                reason: "unknown node — neither voter nor observer",
            });
        }
        self.config.observers.remove(&node);
        self.config.voters.insert(node);

        // Invalidate in-flight candidacy if the voter set changed
        // while we were running an election — otherwise stale
        // (or newly-illegitimate) tallied votes could let us win
        // without a true quorum of the new voter set.
        if matches!(
            self.role.kind(),
            RoleKind::PreCandidate | RoleKind::Candidate
        ) {
            self.transition_to_follower(self.persistent.current_term, None, out, false);
        }

        if node == self.config.id {
            let prev = self.role.kind();
            self.role = Role::fresh_follower();
            out.push(Command::ResetElectionTimer);
            out.push(Command::RoleChanged {
                previous: prev,
                current: RoleKind::Follower,
                term: self.persistent.current_term,
                leader: self.volatile.leader_id,
            });
        } else if let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        {
            next_index.insert(node, self.log_cache.get().last_index.next());
            match_index.insert(node, LogIndex::ZERO);
        }
        Ok(())
    }

    fn on_demote(&mut self, node: NodeId, out: &mut Vec<Command>) -> RaftResult<()> {
        if self.config.observers.contains(&node) {
            return Err(RaftError::InvalidMembershipChange {
                node,
                reason: "node is already an observer",
            });
        }
        if !self.config.voters.contains(&node) {
            return Err(RaftError::InvalidMembershipChange {
                node,
                reason: "unknown node — neither voter nor observer",
            });
        }
        self.config.voters.remove(&node);
        self.config.observers.insert(node);

        // Same rationale as `on_promote` — voter-set changed, so any
        // in-flight candidacy must be invalidated.
        if node != self.config.id
            && matches!(
                self.role.kind(),
                RoleKind::PreCandidate | RoleKind::Candidate
            )
        {
            self.transition_to_follower(self.persistent.current_term, None, out, false);
        }

        if node == self.config.id {
            let prev = self.role.kind();
            if matches!(prev, RoleKind::Leader) {
                out.push(Command::StopHeartbeatTimer);
            }
            out.push(Command::StopElectionTimer);
            self.role = Role::Observer;
            self.volatile.leader_id = None;
            out.push(Command::RoleChanged {
                previous: prev,
                current: RoleKind::Observer,
                term: self.persistent.current_term,
                leader: None,
            });
        } else if let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        {
            next_index.remove(&node);
            match_index.remove(&node);
        }
        Ok(())
    }

    // ============================================================
    // Transition helpers
    // ============================================================

    fn transition_to_follower(
        &mut self,
        new_term: Term,
        leader: Option<NodeId>,
        out: &mut Vec<Command>,
        force_persist: bool,
    ) {
        let prev = self.role.kind();
        let persisted = self.persistent.observe_term(new_term) || force_persist;
        if matches!(prev, RoleKind::Leader) {
            out.push(Command::StopHeartbeatTimer);
        }
        self.volatile.leader_id = leader;
        self.role = match leader {
            Some(id) => Role::follower_with_leader(id),
            None => Role::fresh_follower(),
        };
        if persisted {
            out.push(Command::PersistState);
        }
        out.push(Command::ResetElectionTimer);
        out.push(Command::RoleChanged {
            previous: prev,
            current: RoleKind::Follower,
            term: self.persistent.current_term,
            leader,
        });
    }

    fn become_pre_candidate(&mut self, out: &mut Vec<Command>) {
        let prev = self.role.kind();
        let mut votes = BTreeSet::new();
        votes.insert(self.config.id);
        self.role = Role::PreCandidate {
            votes_received: votes,
        };
        out.push(Command::ResetElectionTimer);
        out.push(Command::BroadcastRequestVote {
            pre_vote: true,
            term: self.persistent.current_term.next(),
            last_log: self.log_cache.get(),
        });
        out.push(Command::RoleChanged {
            previous: prev,
            current: RoleKind::PreCandidate,
            term: self.persistent.current_term,
            leader: self.volatile.leader_id,
        });
        if self.config.quorum_size() <= 1 {
            self.become_candidate(out);
        }
    }

    fn become_candidate(&mut self, out: &mut Vec<Command>) {
        let prev = self.role.kind();
        let new_term = self.persistent.current_term.next();
        let _ = self.persistent.observe_term(new_term);
        self.persistent.voted_for = Some(self.config.id);
        self.volatile.leader_id = None;
        let mut votes = BTreeSet::new();
        votes.insert(self.config.id);
        self.role = Role::Candidate {
            votes_received: votes,
        };
        out.push(Command::PersistState);
        out.push(Command::ResetElectionTimer);
        out.push(Command::BroadcastRequestVote {
            pre_vote: false,
            term: self.persistent.current_term,
            last_log: self.log_cache.get(),
        });
        out.push(Command::RoleChanged {
            previous: prev,
            current: RoleKind::Candidate,
            term: self.persistent.current_term,
            leader: None,
        });
        if self.config.quorum_size() <= 1 {
            self.become_leader(out);
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Command>) {
        let prev = self.role.kind();
        let mut next_index = BTreeMap::new();
        let mut match_index = BTreeMap::new();
        let initial_next = self.log_cache.get().last_index.next();
        for peer in &self.config.voters {
            if *peer != self.config.id {
                next_index.insert(*peer, initial_next);
                match_index.insert(*peer, LogIndex::ZERO);
            }
        }
        for peer in &self.config.observers {
            next_index.insert(*peer, initial_next);
            match_index.insert(*peer, LogIndex::ZERO);
        }
        self.role = Role::Leader {
            next_index,
            match_index,
        };
        self.volatile.leader_id = Some(self.config.id);
        let term = self.persistent.current_term;
        out.push(Command::StopElectionTimer);
        out.push(Command::StartHeartbeatTimer);
        // §8: append a blank no-op so the new leader can commit
        // entries from prior terms.
        out.push(Command::AppendLeaderNoop { term });
        // Initial heartbeat so followers learn of the new leader and
        // reset their election timers immediately.
        out.push(Command::BroadcastHeartbeat {
            term,
            leader_commit: self.volatile.commit_index,
        });
        out.push(Command::RoleChanged {
            previous: prev,
            current: RoleKind::Leader,
            term,
            leader: Some(self.config.id),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(id: u64, voters: &[u64], observers: &[u64], pre_vote: bool) -> NodeConfig {
        let voters: BTreeSet<_> = voters.iter().copied().map(NodeId::new).collect();
        let observers: BTreeSet<_> = observers.iter().copied().map(NodeId::new).collect();
        NodeConfig::new(NodeId::new(id), voters, observers, pre_vote).unwrap()
    }

    fn node(id: u64, voters: &[u64], observers: &[u64], pre_vote: bool) -> RaftNode {
        RaftNode::new(
            cfg(id, voters, observers, pre_vote),
            PersistentState::INITIAL,
            LogMetadata::EMPTY,
        )
        .unwrap()
    }

    fn role_kind_changed_to(commands: &[Command], expected: RoleKind) -> bool {
        commands
            .iter()
            .any(|c| matches!(c, Command::RoleChanged { current, .. } if *current == expected))
    }

    #[test]
    fn fresh_voter_is_follower() {
        let n = node(1, &[1, 2, 3], &[], true);
        assert_eq!(n.role_kind(), RoleKind::Follower);
    }

    #[test]
    fn fresh_non_voter_is_observer() {
        let n = node(7, &[1, 2, 3], &[7], true);
        assert_eq!(n.role_kind(), RoleKind::Observer);
    }

    #[test]
    fn follower_timeout_with_pre_vote_enters_pre_candidate() {
        let mut n = node(1, &[1, 2, 3], &[], true);
        let cmds = n.handle(Event::ElectionTimeout).unwrap();
        assert_eq!(n.role_kind(), RoleKind::PreCandidate);
        assert_eq!(n.current_term(), Term::ZERO, "pre-vote must not bump term");
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::BroadcastRequestVote { pre_vote: true, .. })));
        assert!(role_kind_changed_to(&cmds, RoleKind::PreCandidate));
    }

    #[test]
    fn follower_timeout_without_pre_vote_enters_candidate() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let cmds = n.handle(Event::ElectionTimeout).unwrap();
        assert_eq!(n.role_kind(), RoleKind::Candidate);
        assert_eq!(n.current_term(), Term::new(1));
        assert_eq!(n.voted_for(), Some(NodeId::new(1)));
        assert!(cmds.iter().any(|c| matches!(
            c,
            Command::BroadcastRequestVote {
                pre_vote: false,
                ..
            }
        )));
        assert!(cmds.iter().any(|c| matches!(c, Command::PersistState)));
    }

    #[test]
    fn observer_timeout_is_noop() {
        let mut n = node(7, &[1, 2, 3], &[7], true);
        let cmds = n.handle(Event::ElectionTimeout).unwrap();
        assert!(cmds.is_empty());
        assert_eq!(n.role_kind(), RoleKind::Observer);
    }

    #[test]
    fn candidate_majority_real_votes_becomes_leader() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        assert_eq!(n.role_kind(), RoleKind::Candidate);
        let cmds = n
            .handle(Event::RequestVoteResponse {
                pre_vote: false,
                from: NodeId::new(2),
                term: Term::new(1),
                vote_granted: true,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Leader);
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::AppendLeaderNoop { .. })));
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::StartHeartbeatTimer)));
        assert!(cmds.iter().any(|c| matches!(c, Command::StopElectionTimer)));
    }

    #[test]
    fn pre_vote_majority_promotes_to_candidate_then_real_vote() {
        let mut n = node(1, &[1, 2, 3], &[], true);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        assert_eq!(n.role_kind(), RoleKind::PreCandidate);
        // Pre-vote response carries the responder's current_term (0)
        // — NOT the prospective term.
        let cmds = n
            .handle(Event::RequestVoteResponse {
                pre_vote: true,
                from: NodeId::new(2),
                term: Term::ZERO,
                vote_granted: true,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Candidate);
        assert_eq!(n.current_term(), Term::new(1));
        assert!(cmds.iter().any(|c| matches!(
            c,
            Command::BroadcastRequestVote {
                pre_vote: false,
                ..
            }
        )));
    }

    #[test]
    fn higher_term_steps_leader_down() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        let _ = n
            .handle(Event::RequestVoteResponse {
                pre_vote: false,
                from: NodeId::new(2),
                term: Term::new(1),
                vote_granted: true,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Leader);
        let cmds = n
            .handle(Event::AppendEntriesRequest {
                leader_id: NodeId::new(3),
                leader_term: Term::new(5),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                leader_commit: LogIndex::ZERO,
                log_ok: true,
                entry_count: 0,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Follower);
        assert_eq!(n.current_term(), Term::new(5));
        assert_eq!(n.leader_id(), Some(NodeId::new(3)));
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::StopHeartbeatTimer)));
        assert!(cmds.iter().any(|c| matches!(c, Command::PersistState)));
    }

    #[test]
    fn candidate_steps_down_on_same_term_append_even_if_log_mismatches() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        assert_eq!(n.role_kind(), RoleKind::Candidate);
        let _ = n
            .handle(Event::AppendEntriesRequest {
                leader_id: NodeId::new(2),
                leader_term: Term::new(1),
                prev_log_index: LogIndex::new(5),
                prev_log_term: Term::new(1),
                leader_commit: LogIndex::ZERO,
                log_ok: false,
                entry_count: 0,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Follower);
        assert_eq!(n.leader_id(), Some(NodeId::new(2)));
    }

    #[test]
    fn stale_term_request_vote_is_rejected() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        for _ in 0..5 {
            let _ = n.handle(Event::ElectionTimeout).unwrap();
        }
        assert_eq!(n.current_term(), Term::new(5));
        let cmds = n
            .handle(Event::RequestVoteRequest {
                pre_vote: false,
                candidate_id: NodeId::new(2),
                candidate_term: Term::new(3),
                candidate_log: LogMetadata::EMPTY,
                local_election_timeout_elapsed: true,
            })
            .unwrap();
        assert!(cmds.iter().any(|c| matches!(
            c,
            Command::SendVoteResponse {
                vote_granted: false,
                ..
            }
        )));
    }

    #[test]
    fn pre_vote_rejected_when_election_timer_fresh() {
        let mut n = node(1, &[1, 2, 3], &[], true);
        let cmds = n
            .handle(Event::RequestVoteRequest {
                pre_vote: true,
                candidate_id: NodeId::new(2),
                candidate_term: Term::new(1),
                candidate_log: LogMetadata::EMPTY,
                local_election_timeout_elapsed: false,
            })
            .unwrap();
        let granted = cmds.iter().any(|c| {
            matches!(
                c,
                Command::SendVoteResponse {
                    pre_vote: true,
                    vote_granted: true,
                    ..
                }
            )
        });
        assert!(!granted);
        // Critically — pre-vote did NOT bump our term or set voted_for.
        assert_eq!(n.current_term(), Term::ZERO);
        assert_eq!(n.voted_for(), None);
    }

    #[test]
    fn vote_only_once_per_term() {
        let mut n = node(1, &[1, 2, 3, 4, 5], &[], false);
        let _ = n
            .handle(Event::RequestVoteRequest {
                pre_vote: false,
                candidate_id: NodeId::new(2),
                candidate_term: Term::new(1),
                candidate_log: LogMetadata::EMPTY,
                local_election_timeout_elapsed: true,
            })
            .unwrap();
        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
        let cmds = n
            .handle(Event::RequestVoteRequest {
                pre_vote: false,
                candidate_id: NodeId::new(3),
                candidate_term: Term::new(1),
                candidate_log: LogMetadata::EMPTY,
                local_election_timeout_elapsed: true,
            })
            .unwrap();
        assert!(cmds.iter().any(|c| matches!(
            c,
            Command::SendVoteResponse {
                vote_granted: false,
                ..
            }
        )));
        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
    }

    #[test]
    fn vote_denied_when_log_not_up_to_date() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        n.handle(Event::LogTailUpdated {
            metadata: LogMetadata {
                last_index: LogIndex::new(5),
                last_term: Term::new(2),
            },
        })
        .unwrap();
        let cmds = n
            .handle(Event::RequestVoteRequest {
                pre_vote: false,
                candidate_id: NodeId::new(2),
                candidate_term: Term::new(3),
                candidate_log: LogMetadata {
                    last_index: LogIndex::new(10),
                    last_term: Term::new(1),
                },
                local_election_timeout_elapsed: true,
            })
            .unwrap();
        assert!(cmds.iter().any(|c| matches!(
            c,
            Command::SendVoteResponse {
                vote_granted: false,
                ..
            }
        )));
        assert_eq!(n.current_term(), Term::new(3));
    }

    #[test]
    fn single_voter_cluster_self_elects() {
        let mut n = node(1, &[1], &[], false);
        let cmds = n.handle(Event::ElectionTimeout).unwrap();
        assert_eq!(n.role_kind(), RoleKind::Leader);
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::AppendLeaderNoop { .. })));
    }

    #[test]
    fn promotion_of_self_observer_to_voter_starts_election_timer() {
        let mut n = node(7, &[1, 2, 3], &[7], true);
        let cmds = n
            .handle(Event::PromoteToVoter {
                node: NodeId::new(7),
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Follower);
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::ResetElectionTimer)));
    }

    #[test]
    fn demotion_of_self_leader_steps_down_to_observer() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        let _ = n
            .handle(Event::RequestVoteResponse {
                pre_vote: false,
                from: NodeId::new(2),
                term: Term::new(1),
                vote_granted: true,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Leader);
        let cmds = n
            .handle(Event::DemoteToObserver {
                node: NodeId::new(1),
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Observer);
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::StopHeartbeatTimer)));
        assert!(cmds.iter().any(|c| matches!(c, Command::StopElectionTimer)));
    }

    #[test]
    fn shutdown_blocks_further_events() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let _ = n.handle(Event::Shutdown).unwrap();
        let err = n.handle(Event::ElectionTimeout).unwrap_err();
        assert!(matches!(err, RaftError::AlreadyShutDown));
    }

    #[test]
    fn stale_vote_response_after_step_down_is_ignored() {
        let mut n = node(1, &[1, 2, 3], &[], false);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        let _ = n
            .handle(Event::AppendEntriesRequest {
                leader_id: NodeId::new(2),
                leader_term: Term::new(5),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                leader_commit: LogIndex::ZERO,
                log_ok: true,
                entry_count: 0,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Follower);
        let cmds = n
            .handle(Event::RequestVoteResponse {
                pre_vote: false,
                from: NodeId::new(3),
                term: Term::new(1),
                vote_granted: true,
            })
            .unwrap();
        assert!(cmds.is_empty());
        assert_eq!(n.role_kind(), RoleKind::Follower);
    }

    // ----- Membership-guard tests (rubber-duck blocking issue #1) -----

    #[test]
    fn vote_from_non_voter_is_rejected_without_term_bump() {
        let mut n = node(1, &[1, 2, 3], &[4], false);
        // Observer 4 attempts to bump our term and steal a vote.
        let cmds = n
            .handle(Event::RequestVoteRequest {
                pre_vote: false,
                candidate_id: NodeId::new(4),
                candidate_term: Term::new(99),
                candidate_log: LogMetadata::EMPTY,
                local_election_timeout_elapsed: true,
            })
            .unwrap();
        assert!(cmds.iter().any(|c| matches!(
            c,
            Command::SendVoteResponse {
                vote_granted: false,
                ..
            }
        )));
        // Critically — observer cannot bump our term.
        assert_eq!(n.current_term(), Term::ZERO);
        assert_eq!(n.voted_for(), None);
    }

    #[test]
    fn append_from_non_voter_is_rejected_without_term_bump() {
        let mut n = node(1, &[1, 2, 3], &[4], false);
        let cmds = n
            .handle(Event::AppendEntriesRequest {
                leader_id: NodeId::new(4),
                leader_term: Term::new(99),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                leader_commit: LogIndex::ZERO,
                log_ok: true,
                entry_count: 0,
            })
            .unwrap();
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::SendAppendEntriesResponse { success: false, .. })));
        assert_eq!(n.current_term(), Term::ZERO);
        assert_eq!(n.leader_id(), None);
    }

    // ----- Membership-change vote invalidation (rubber-duck #3) -----

    #[test]
    fn candidacy_invalidated_on_voter_set_change() {
        // 5-voter cluster, candidate has 2 votes (self + node 2).
        let mut n = node(1, &[1, 2, 3, 4, 5], &[6], false);
        let _ = n.handle(Event::ElectionTimeout).unwrap();
        let _ = n
            .handle(Event::RequestVoteResponse {
                pre_vote: false,
                from: NodeId::new(2),
                term: Term::new(1),
                vote_granted: true,
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Candidate);
        // Voter-set change while candidate: must step down.
        let _ = n
            .handle(Event::PromoteToVoter {
                node: NodeId::new(6),
            })
            .unwrap();
        assert_eq!(n.role_kind(), RoleKind::Follower);
    }
}
