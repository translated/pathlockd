//! Elastic membership reconciliation.
//!
//! Decentralized operator pattern: **every node reconciles the groups it
//! currently leads.** The desired voter set of a group is a pure function of
//! the stable SWIM member catalog (`select_voters` over HRW, `rf_effective`
//! over the configured replication factor), so all nodes independently agree
//! on the target without coordination, and the group's leader — the only
//! node that can safely change its membership — drives convergence:
//!
//! 1. every desired voter missing from the membership is added as a learner
//!    (openraft replicates / snapshots state into it);
//! 2. once all desired voters are present (voter or caught-up learner) and a
//!    quorum of them is alive, joint consensus moves the voter set;
//! 3. leadership is periodically transferred toward the group's HRW-first
//!    live voter, spreading write leaders across the cluster;
//! 4. the group's directory record (sys group) is refreshed for routing.
//!
//! The sys group gets the same treatment plus one extra duty performed by its
//! leader: every stable node that is not a sys voter is kept as a **sys
//! learner**, so all nodes hold a local replica of the directory, the
//! wait-graph, and the fencing counter for stale-tolerable local reads.
//!
//! Safety rails: nodes only count as *stable* after `stability_window_secs`
//! continuously up; a voter is only dropped when it has been gone for
//! `eviction_window_secs` (or is draining) **and** the change keeps a live
//! majority; membership changes go one group at a time per tick, rate-limited
//! by `max_concurrent_reconciles`.
//!
//! Known limitation (documented Raft hazard, same as etcd/Consul): a voter
//! restarting with a **wiped disk** keeps its identity but lost its vote; in
//! pathological timing it could double-vote within one term. Recommended
//! operating procedure for disk loss is to rejoin with a fresh node id
//! (StatefulSet replica with a new ordinal) and let reconciliation migrate
//! state; the eviction window automates the cleanup of the old identity.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::cluster::directory;
use crate::cluster::gossip::{ClusterMembers, MemberMap};
use crate::cluster::placement::{rf_effective, select_voters, GroupId, SYS_GROUP};
use crate::cluster::router::Router;
use crate::raft::manager::RaftGroups;
use crate::raft::types::NodeMeta;

#[derive(Debug, Clone)]
pub struct ControllerOptions {
    pub group_count: u32,
    pub replication_factor: u32,
    pub stability_window: Duration,
    pub eviction_window: Duration,
    pub reconcile_interval: Duration,
    pub leader_balance_interval: Duration,
    pub max_concurrent_reconciles: usize,
}

impl Default for ControllerOptions {
    fn default() -> Self {
        Self {
            group_count: 32,
            replication_factor: 3,
            stability_window: Duration::from_secs(30),
            eviction_window: Duration::from_secs(60),
            reconcile_interval: Duration::from_secs(5),
            leader_balance_interval: Duration::from_secs(60),
            max_concurrent_reconciles: 4,
        }
    }
}

/// Tracks when nodes were first/last seen to implement the stability and
/// eviction windows.
struct Presence {
    first_seen: HashMap<u64, Instant>,
    last_seen: HashMap<u64, Instant>,
}

impl Presence {
    fn new() -> Self {
        Self {
            first_seen: HashMap::new(),
            last_seen: HashMap::new(),
        }
    }

    fn observe(&mut self, members: &MemberMap, now: Instant) {
        for id in members.keys() {
            self.first_seen.entry(*id).or_insert(now);
            self.last_seen.insert(*id, now);
        }
        // A node that vanished and returns later must re-earn stability.
        self.first_seen
            .retain(|id, _| members.contains_key(id) || self.last_seen.contains_key(id));
        for (id, _) in self.last_seen.clone() {
            if !members.contains_key(&id) {
                self.first_seen.remove(&id);
            }
        }
    }

    /// Nodes continuously up for at least the stability window.
    fn stable(
        &self,
        members: &MemberMap,
        window: Duration,
        now: Instant,
    ) -> BTreeMap<u64, NodeMeta> {
        members
            .iter()
            .filter(|(id, _)| {
                self.first_seen
                    .get(id)
                    .is_some_and(|t| now.duration_since(*t) >= window)
            })
            .map(|(id, ident)| (*id, ident.meta.clone()))
            .collect()
    }

    /// True when a node has been unseen for at least the eviction window.
    fn evictable(&self, node: u64, window: Duration, now: Instant) -> bool {
        match self.last_seen.get(&node) {
            Some(t) => now.duration_since(*t) >= window,
            None => true,
        }
    }
}

pub fn spawn_controller(
    groups: Arc<RaftGroups>,
    router: Arc<Router>,
    members: ClusterMembers,
    opts: ControllerOptions,
) {
    tokio::spawn(controller_loop(groups, router, members, opts));
}

async fn controller_loop(
    groups: Arc<RaftGroups>,
    router: Arc<Router>,
    members: ClusterMembers,
    opts: ControllerOptions,
) {
    let mut presence = Presence::new();
    let mut tick = tokio::time::interval(opts.reconcile_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_balance = Instant::now();
    let member_rx = members.watch();

    loop {
        tick.tick().await;
        let now = Instant::now();
        let catalog = member_rx.borrow().clone();
        presence.observe(&catalog, now);

        // Bootstrap grace: a single-node cluster must not wait the stability
        // window to make itself a voter — it already is one.
        let mut stable = presence.stable(&catalog, opts.stability_window, now);
        stable.insert(members.local().node_id, members.local().meta.clone());

        // Draining nodes are excluded from every desired set.
        let draining = directory::read_draining(&groups.db_handle()).unwrap_or_default();
        for node in &draining {
            stable.remove(node);
        }

        let balance_due = now.duration_since(last_balance) >= opts.leader_balance_interval;
        if balance_due {
            last_balance = now;
        }

        let mut reconciled = 0usize;
        for group in groups.hosted() {
            if !groups.is_leader(group) {
                continue;
            }
            if reconciled >= opts.max_concurrent_reconciles {
                break;
            }
            match reconcile_group(
                &groups,
                &router,
                group,
                &stable,
                &presence,
                &opts,
                balance_due,
                now,
            )
            .await
            {
                Ok(changed) => {
                    if changed {
                        reconciled += 1;
                    }
                }
                Err(e) => {
                    debug!(group, error = %e, "reconcile failed; retrying next tick");
                }
            }
        }
    }
}

/// Reconcile one group this node leads. Returns whether a membership change
/// was proposed.
#[allow(clippy::too_many_arguments)]
async fn reconcile_group(
    groups: &Arc<RaftGroups>,
    router: &Arc<Router>,
    group: GroupId,
    stable: &BTreeMap<u64, NodeMeta>,
    presence: &Presence,
    opts: &ControllerOptions,
    balance_due: bool,
    now: Instant,
) -> anyhow::Result<bool> {
    let Some(raft) = groups.get(group) else {
        return Ok(false);
    };
    let Some(metrics) = groups.metrics(group) else {
        return Ok(false);
    };

    let membership = metrics.membership_config.membership().clone();
    let current_voters: BTreeSet<u64> = membership.voter_ids().collect();
    let current_all: BTreeSet<u64> = membership.nodes().map(|(id, _)| *id).collect();

    let stable_ids: Vec<u64> = stable.keys().copied().collect();
    let rf = rf_effective(opts.replication_factor, stable_ids.len());
    let desired: BTreeSet<u64> = select_voters(group, &stable_ids, rf).into_iter().collect();

    if desired.is_empty() {
        return Ok(false);
    }

    let mut changed = false;

    // Phase 1: every desired voter joins as a learner first.
    for node in &desired {
        if !current_all.contains(node) {
            if let Some(meta) = stable.get(node) {
                info!(group, node, "adding learner");
                raft.add_learner(*node, meta.clone(), false)
                    .await
                    .map_err(|e| anyhow::anyhow!("add_learner({node}): {e}"))?;
                changed = true;
            }
        }
    }

    // Sys group extra: every stable node holds a learner replica for local
    // directory / wait-graph / fence reads.
    if group == SYS_GROUP {
        for (node, meta) in stable {
            if !current_all.contains(node) && !desired.contains(node) {
                info!(node, "adding sys learner");
                raft.add_learner(*node, meta.clone(), false)
                    .await
                    .map_err(|e| anyhow::anyhow!("add_learner(sys, {node}): {e}"))?;
                changed = true;
            }
        }
    }

    // Phase 2: move the voter set when it differs and the move is safe.
    if desired != current_voters {
        // Departing voters must be genuinely gone (eviction window) or
        // demoted while alive (rebalancing/draining) — both fine; the guard
        // that matters is a live majority of the NEW configuration, or joint
        // consensus cannot commit and the group wedges.
        let live_new = desired.iter().filter(|n| stable.contains_key(n)).count();
        if live_new <= desired.len() / 2 {
            warn!(
                group,
                ?desired,
                live_new,
                "skipping membership change: would lack a live majority"
            );
        } else {
            let departing_dead: Vec<u64> = current_voters
                .difference(&desired)
                .filter(|n| !stable.contains_key(n))
                .copied()
                .collect();
            let all_dead_evictable = departing_dead
                .iter()
                .all(|n| presence.evictable(*n, opts.eviction_window, now));
            if all_dead_evictable {
                info!(group, from = ?current_voters, to = ?desired, "changing membership");
                raft.change_membership(desired.clone(), false)
                    .await
                    .map_err(|e| anyhow::anyhow!("change_membership: {e}"))?;
                changed = true;
            } else {
                debug!(
                    group,
                    ?departing_dead,
                    "waiting out the eviction window before replacing dead voters"
                );
            }
        }
    }

    // Phase 3: leadership drifts toward the HRW-first live voter.
    if balance_due && !changed {
        if let Some(preferred) = select_voters(group, &stable_ids, rf)
            .into_iter()
            .find(|n| desired.contains(n) && current_voters.contains(n))
        {
            if preferred != groups.node_id() {
                info!(group, to = preferred, "transferring leadership (balance)");
                let _ = raft.trigger().transfer_leader(preferred).await;
            }
        }
    }

    // Phase 4: refresh the directory record (best effort).
    let learners: Vec<u64> = current_all.difference(&desired).copied().collect();
    let record_cmd = crate::raft::command::Op::DirectoryUpdate {
        group,
        voters: desired.iter().copied().collect(),
        learners,
        leader: Some(groups.node_id()),
    };
    if let Err(e) = router.propose_sys(record_cmd).await {
        debug!(group, error = %e, "directory update failed");
    }

    Ok(changed)
}
