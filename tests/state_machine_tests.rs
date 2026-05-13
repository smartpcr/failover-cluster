//! Integration tests that wire several `RaftNode` instances together
//! via an in-memory transport and drive them through full election
//! scenarios.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use failover_cluster_raft::{
    Command, Event, LogIndex, LogMetadata, NodeConfig, NodeId, PersistentState, RaftNode, RoleKind,
    Term,
};

// ------------------------------------------------------------------
// Test harness
// ------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Envelope {
    from: NodeId,
    to: NodeId,
    event: Event,
}

struct Cluster {
    nodes: BTreeMap<NodeId, RaftNode>,
    inbox: VecDeque<Envelope>,
    election_timer_elapsed: BTreeMap<NodeId, bool>,
}

impl Cluster {
    fn new(voters: &[u64], observers: &[u64], pre_vote: bool) -> Self {
        let voter_ids: BTreeSet<NodeId> = voters.iter().copied().map(NodeId::new).collect();
        let observer_ids: BTreeSet<NodeId> = observers.iter().copied().map(NodeId::new).collect();
        let mut nodes = BTreeMap::new();
        let mut timers = BTreeMap::new();
        for &v in voters {
            let id = NodeId::new(v);
            let cfg =
                NodeConfig::new(id, voter_ids.clone(), observer_ids.clone(), pre_vote).unwrap();
            nodes.insert(
                id,
                RaftNode::new(cfg, PersistentState::INITIAL, LogMetadata::EMPTY).unwrap(),
            );
            timers.insert(id, true);
        }
        for &o in observers {
            let id = NodeId::new(o);
            let cfg =
                NodeConfig::new(id, voter_ids.clone(), observer_ids.clone(), pre_vote).unwrap();
            nodes.insert(
                id,
                RaftNode::new(cfg, PersistentState::INITIAL, LogMetadata::EMPTY).unwrap(),
            );
            timers.insert(id, true);
        }
        Self {
            nodes,
            inbox: VecDeque::new(),
            election_timer_elapsed: timers,
        }
    }

    fn node(&self, id: u64) -> &RaftNode {
        &self.nodes[&NodeId::new(id)]
    }

    fn deliver(&mut self, target: NodeId, event: Event) {
        let cmds = self
            .nodes
            .get_mut(&target)
            .expect("unknown node")
            .handle(event)
            .expect("event rejected");
        self.dispatch(target, cmds);
    }

    fn dispatch(&mut self, source: NodeId, cmds: Vec<Command>) {
        let source_cfg = self.nodes[&source].config().clone();
        for cmd in cmds {
            match cmd {
                Command::BroadcastRequestVote {
                    pre_vote,
                    term,
                    last_log,
                } => {
                    for &peer in &source_cfg.voters {
                        if peer == source {
                            continue;
                        }
                        let local_timer = *self.election_timer_elapsed.get(&peer).unwrap_or(&true);
                        self.inbox.push_back(Envelope {
                            from: source,
                            to: peer,
                            event: Event::RequestVoteRequest {
                                pre_vote,
                                candidate_id: source,
                                candidate_term: term,
                                candidate_log: last_log,
                                local_election_timeout_elapsed: local_timer,
                            },
                        });
                    }
                }
                Command::BroadcastHeartbeat {
                    term,
                    leader_commit,
                } => {
                    for &peer in self.nodes.keys() {
                        if peer == source {
                            continue;
                        }
                        self.inbox.push_back(Envelope {
                            from: source,
                            to: peer,
                            event: Event::AppendEntriesRequest {
                                leader_id: source,
                                leader_term: term,
                                prev_log_index: LogIndex::ZERO,
                                prev_log_term: Term::ZERO,
                                leader_commit,
                                log_ok: true,
                                entry_count: 0,
                            },
                        });
                    }
                }
                Command::SendVoteResponse {
                    pre_vote,
                    to,
                    term,
                    vote_granted,
                } => {
                    self.inbox.push_back(Envelope {
                        from: source,
                        to,
                        event: Event::RequestVoteResponse {
                            pre_vote,
                            from: source,
                            term,
                            vote_granted,
                        },
                    });
                }
                Command::SendAppendEntriesResponse {
                    to,
                    term,
                    success,
                    match_index,
                } => {
                    self.inbox.push_back(Envelope {
                        from: source,
                        to,
                        event: Event::AppendEntriesResponse {
                            from: source,
                            term,
                            success,
                            match_index,
                        },
                    });
                }
                Command::ResetElectionTimer | Command::StopElectionTimer => {
                    self.election_timer_elapsed.insert(source, false);
                }
                _ => {
                    // Other commands have no transport-level effect.
                }
            }
        }
    }

    fn pump(&mut self) {
        let mut step = 0;
        while let Some(env) = self.inbox.pop_front() {
            step += 1;
            assert!(step < 10_000, "cluster did not quiesce");
            let cmds = self
                .nodes
                .get_mut(&env.to)
                .expect("unknown destination")
                .handle(env.event)
                .expect("event rejected");
            self.dispatch(env.to, cmds);
        }
    }

    fn trigger_election_timer(&mut self, id: u64) {
        let id = NodeId::new(id);
        self.election_timer_elapsed.insert(id, true);
        self.deliver(id, Event::ElectionTimeout);
        self.pump();
    }

    fn tick_heartbeat(&mut self, id: u64) {
        let id = NodeId::new(id);
        self.deliver(id, Event::HeartbeatTick);
        self.pump();
    }

    fn role(&self, id: u64) -> RoleKind {
        self.node(id).role_kind()
    }

    fn term(&self, id: u64) -> Term {
        self.node(id).current_term()
    }

    fn leader(&self, id: u64) -> Option<NodeId> {
        self.node(id).leader_id()
    }

    fn leaders(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(_, n)| matches!(n.role_kind(), RoleKind::Leader))
            .map(|(id, _)| id.get())
            .collect()
    }
}

// ------------------------------------------------------------------
// Scenarios
// ------------------------------------------------------------------

#[test]
fn three_voter_cluster_elects_single_leader() {
    let mut c = Cluster::new(&[1, 2, 3], &[], false);
    c.trigger_election_timer(1);
    assert_eq!(c.leaders(), vec![1]);
    assert_eq!(c.role(1), RoleKind::Leader);
    assert_eq!(c.role(2), RoleKind::Follower);
    assert_eq!(c.role(3), RoleKind::Follower);
    let term = c.term(1);
    assert_eq!(c.term(2), term);
    assert_eq!(c.term(3), term);
    assert_eq!(c.leader(2), Some(NodeId::new(1)));
    assert_eq!(c.leader(3), Some(NodeId::new(1)));
}

#[test]
fn pre_vote_cluster_elects_single_leader() {
    let mut c = Cluster::new(&[1, 2, 3], &[], true);
    c.trigger_election_timer(1);
    assert_eq!(c.leaders(), vec![1]);
    assert_eq!(c.role(1), RoleKind::Leader);
    assert_eq!(c.term(1), Term::new(1));
}

#[test]
fn observers_do_not_vote_or_lead() {
    let mut c = Cluster::new(&[1, 2, 3], &[4, 5], true);
    c.trigger_election_timer(1);
    assert_eq!(c.role(4), RoleKind::Observer);
    assert_eq!(c.role(5), RoleKind::Observer);
    assert_eq!(c.leader(4), Some(NodeId::new(1)));
    assert_eq!(c.leader(5), Some(NodeId::new(1)));
    c.deliver(NodeId::new(4), Event::ElectionTimeout);
    c.pump();
    assert_eq!(c.role(4), RoleKind::Observer);
    assert_eq!(c.leaders(), vec![1]);
}

#[test]
fn five_voter_cluster_at_most_one_leader_per_term() {
    let mut c = Cluster::new(&[1, 2, 3, 4, 5], &[], false);
    c.election_timer_elapsed.insert(NodeId::new(1), true);
    c.election_timer_elapsed.insert(NodeId::new(2), true);
    c.deliver(NodeId::new(1), Event::ElectionTimeout);
    c.deliver(NodeId::new(2), Event::ElectionTimeout);
    c.pump();
    let leaders = c.leaders();
    assert!(
        leaders.len() <= 1,
        "at most one leader per term, got {leaders:?}"
    );
}

#[test]
fn higher_term_voter_steps_leader_down_on_rejoin() {
    let mut c = Cluster::new(&[1, 2, 3], &[], false);
    c.trigger_election_timer(1);
    assert_eq!(c.role(1), RoleKind::Leader);
    let t0 = c.term(1);

    let n3 = NodeId::new(3);
    // Node 3 partitions and drives its term forward locally.
    for _ in 0..3 {
        c.election_timer_elapsed.insert(n3, true);
        let _ = c
            .nodes
            .get_mut(&n3)
            .unwrap()
            .handle(Event::ElectionTimeout)
            .unwrap();
        c.inbox.clear();
    }
    assert!(c.term(3) > t0);
    let high_term = c.term(3);
    // Rejoin: node 3 (a voter!) sends a real-vote RPC. Leader must
    // step down because the candidate is in the voter set.
    c.deliver(
        NodeId::new(1),
        Event::RequestVoteRequest {
            pre_vote: false,
            candidate_id: n3,
            candidate_term: high_term,
            candidate_log: LogMetadata::EMPTY,
            local_election_timeout_elapsed: true,
        },
    );
    c.pump();
    assert_ne!(c.role(1), RoleKind::Leader);
    assert!(c.term(1) >= high_term);
}

#[test]
fn observer_cannot_bump_leader_term() {
    // The membership-guard fix: an observer trying to disrupt the
    // cluster must not be able to bump the leader's term.
    let mut c = Cluster::new(&[1, 2, 3], &[4], false);
    c.trigger_election_timer(1);
    let leader_term = c.term(1);
    assert_eq!(c.role(1), RoleKind::Leader);

    // Observer 4 sends a high-term real-vote request directly.
    c.deliver(
        NodeId::new(1),
        Event::RequestVoteRequest {
            pre_vote: false,
            candidate_id: NodeId::new(4),
            candidate_term: Term::new(99),
            candidate_log: LogMetadata::EMPTY,
            local_election_timeout_elapsed: true,
        },
    );
    c.pump();
    assert_eq!(
        c.role(1),
        RoleKind::Leader,
        "observer cannot dethrone leader"
    );
    assert_eq!(c.term(1), leader_term, "observer cannot bump term");
}

#[test]
fn pre_vote_blocks_isolated_disruptor() {
    let mut c = Cluster::new(&[1, 2, 3], &[], true);
    c.trigger_election_timer(1);
    assert_eq!(c.role(1), RoleKind::Leader);
    c.election_timer_elapsed.insert(NodeId::new(2), false);
    c.election_timer_elapsed.insert(NodeId::new(3), false);
    let term_before = c.term(3);
    c.election_timer_elapsed.insert(NodeId::new(3), true);
    c.deliver(NodeId::new(3), Event::ElectionTimeout);
    c.pump();
    assert_eq!(c.term(3), term_before, "pre-vote must not bump term");
    assert_ne!(c.role(3), RoleKind::Leader);
    assert_eq!(c.role(1), RoleKind::Leader);
}

#[test]
fn promotion_of_observer_is_accepted_into_quorum() {
    let mut c = Cluster::new(&[1, 2, 3], &[4], true);
    c.trigger_election_timer(1);
    let leader_term = c.term(1);
    assert_eq!(c.role(1), RoleKind::Leader);
    assert_eq!(c.role(4), RoleKind::Observer);
    for id in [1u64, 2, 3, 4] {
        c.deliver(
            NodeId::new(id),
            Event::PromoteToVoter {
                node: NodeId::new(4),
            },
        );
    }
    c.pump();
    assert_eq!(c.role(4), RoleKind::Follower);
    assert_eq!(c.role(1), RoleKind::Leader);
    assert_eq!(c.term(1), leader_term);
    assert_eq!(c.node(1).config().quorum_size(), 3);
    for id in [1u64, 2, 3, 4] {
        assert!(c.node(id).config().voters.contains(&NodeId::new(4)));
    }
}

#[test]
fn demotion_of_leader_steps_down_to_observer() {
    let mut c = Cluster::new(&[1, 2, 3], &[], false);
    c.trigger_election_timer(1);
    assert_eq!(c.role(1), RoleKind::Leader);
    for id in [1u64, 2, 3] {
        c.deliver(
            NodeId::new(id),
            Event::DemoteToObserver {
                node: NodeId::new(1),
            },
        );
    }
    c.pump();
    assert_eq!(c.role(1), RoleKind::Observer);
    c.trigger_election_timer(2);
    let leaders = c.leaders();
    assert_eq!(leaders.len(), 1);
    assert_ne!(leaders[0], 1);
}

#[test]
fn single_voter_cluster_self_elects() {
    let mut c = Cluster::new(&[1], &[], false);
    c.trigger_election_timer(1);
    assert_eq!(c.role(1), RoleKind::Leader);
}

#[test]
fn heartbeat_keeps_followers_in_term() {
    let mut c = Cluster::new(&[1, 2, 3], &[], false);
    c.trigger_election_timer(1);
    let t = c.term(1);
    c.tick_heartbeat(1);
    assert_eq!(c.term(1), t);
    assert_eq!(c.term(2), t);
    assert_eq!(c.term(3), t);
    assert_eq!(c.role(1), RoleKind::Leader);
}

#[test]
fn split_vote_resolves_with_higher_term() {
    let mut c = Cluster::new(&[1, 2, 3, 4], &[], false);
    c.election_timer_elapsed.insert(NodeId::new(1), true);
    c.election_timer_elapsed.insert(NodeId::new(2), true);
    c.deliver(NodeId::new(1), Event::ElectionTimeout);
    c.deliver(NodeId::new(2), Event::ElectionTimeout);
    c.pump();
    if c.leaders().is_empty() {
        let cand = if c.term(1) >= c.term(2) { 1 } else { 2 };
        c.election_timer_elapsed.insert(NodeId::new(cand), true);
        c.deliver(NodeId::new(cand), Event::ElectionTimeout);
        c.pump();
    }
    assert!(c.leaders().len() <= 1);
}
