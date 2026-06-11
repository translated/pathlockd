//! Multi-raft runtime: one openraft core per hosted group over the shared
//! RocksDB, the node-wide fsync batcher, and the shared peer channel pool.
//!
//! Cores exist only for groups this node hosts (voter or learner, per the
//! membership directory). Group `g`'s storage lives under its key prefix in
//! the shared DB; starting a core is cheap (no per-group files), stopping one
//! shuts the apply loop down, and `destroy` additionally range-deletes the
//! group's keyspace when the node stops hosting it for good.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rocksdb::DB;
use tracing::info;

use crate::cluster::placement::GroupId;
use crate::raft::log_store::{FsyncBatcher, GroupLogStore};
use crate::raft::network::{PeerPool, RaftClientFactory};
use crate::raft::state_machine::GroupStateMachine;
use crate::raft::types::{NodeMeta, Raft, RaftMetrics};

pub struct RaftGroups {
    db: Arc<DB>,
    node_id: u64,
    node_meta: NodeMeta,
    raft_config: Arc<openraft::Config>,
    batcher: FsyncBatcher,
    pool: PeerPool,
    groups: RwLock<HashMap<GroupId, Raft>>,
}

impl RaftGroups {
    pub fn new(
        db: Arc<DB>,
        node_id: u64,
        node_meta: NodeMeta,
        raft_config: openraft::Config,
        batcher: FsyncBatcher,
        pool: PeerPool,
    ) -> anyhow::Result<Arc<Self>> {
        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| anyhow::anyhow!("invalid raft config: {e}"))?,
        );
        Ok(Arc::new(Self {
            db,
            node_id,
            node_meta,
            raft_config,
            batcher,
            pool,
            groups: RwLock::new(HashMap::new()),
        }))
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn node_meta(&self) -> &NodeMeta {
        &self.node_meta
    }

    pub fn peer_pool(&self) -> &PeerPool {
        &self.pool
    }

    pub fn db_handle(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub fn fsync_healthy(&self) -> bool {
        self.batcher.healthy()
    }

    /// The core for a hosted group, if any.
    pub fn get(&self, group: GroupId) -> Option<Raft> {
        self.groups
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&group)
            .cloned()
    }

    /// Ids of all locally hosted groups.
    pub fn hosted(&self) -> Vec<GroupId> {
        self.groups
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// Latest metrics of a hosted group's core.
    pub fn metrics(&self, group: GroupId) -> Option<RaftMetrics> {
        use openraft::async_runtime::watch::WatchReceiver;
        self.get(group)
            .map(|raft| raft.metrics().borrow_watched().clone())
    }

    /// True when this node currently leads the group.
    pub fn is_leader(&self, group: GroupId) -> bool {
        self.metrics(group)
            .map(|m| m.current_leader == Some(self.node_id))
            .unwrap_or(false)
    }

    /// Start (or return) the Raft core for a group. Idempotent.
    pub async fn start_group(self: &Arc<Self>, group: GroupId) -> anyhow::Result<Raft> {
        if let Some(existing) = self.get(group) {
            return Ok(existing);
        }
        let log_store = GroupLogStore::new(self.db.clone(), group, self.batcher.clone());
        let state_machine = GroupStateMachine::new(self.db.clone(), group, self.batcher.clone());
        let network = RaftClientFactory::new(group, self.pool.clone());
        let raft = Raft::new(
            self.node_id,
            self.raft_config.clone(),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| anyhow::anyhow!("starting raft core for group {group}: {e}"))?;

        let mut groups = self
            .groups
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Lost the race: keep the winner, shut ours down in the background.
        if let Some(existing) = groups.get(&group) {
            let raft_clone = existing.clone();
            let loser = raft;
            tokio::spawn(async move {
                let _ = loser.shutdown().await;
            });
            return Ok(raft_clone);
        }
        groups.insert(group, raft.clone());
        info!(group, "raft core started");
        Ok(raft)
    }

    /// Initialize a brand-new group with an explicit voter set. Errors from
    /// re-initializing an already-initialized group are ignored (idempotent
    /// bootstrap).
    pub async fn bootstrap_group(
        self: &Arc<Self>,
        group: GroupId,
        voters: std::collections::BTreeMap<u64, NodeMeta>,
    ) -> anyhow::Result<()> {
        let raft = self.start_group(group).await?;
        match raft.initialize(voters).await {
            Ok(()) => Ok(()),
            Err(openraft::errors::RaftError::APIError(
                openraft::errors::InitializeError::NotAllowed(_),
            )) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("initializing group {group}: {e}")),
        }
    }

    /// Stop a hosted group's core (state stays on disk).
    pub async fn stop_group(&self, group: GroupId) -> anyhow::Result<()> {
        let raft = {
            let mut groups = self
                .groups
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            groups.remove(&group)
        };
        if let Some(raft) = raft {
            raft.shutdown()
                .await
                .map_err(|e| anyhow::anyhow!("shutting down group {group}: {e}"))?;
            info!(group, "raft core stopped");
        }
        Ok(())
    }

    /// Stop a group's core and erase its local keyspace — the node no longer
    /// hosts this group.
    pub async fn destroy_group(&self, group: GroupId) -> anyhow::Result<()> {
        self.stop_group(group).await?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || crate::store_rocksdb::destroy_group(&db, group))
            .await
            .map_err(|e| anyhow::anyhow!("destroy task failed: {e}"))??;
        info!(group, "group keyspace destroyed");
        Ok(())
    }

    /// Shut down every hosted core (process exit).
    pub async fn shutdown_all(&self) {
        let groups: Vec<(GroupId, Raft)> = {
            let mut map = self
                .groups
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            map.drain().collect()
        };
        for (group, raft) in groups {
            if let Err(e) = raft.shutdown().await {
                tracing::warn!(group, error = %e, "raft core shutdown failed");
            }
        }
    }
}

/// Build the shared openraft config from daemon settings.
pub fn raft_config(cfg: &crate::config::Config) -> openraft::Config {
    openraft::Config {
        cluster_name: "pathlockd".to_string(),
        election_timeout_min: cfg.raft_election_timeout_min_ms,
        election_timeout_max: cfg.raft_election_timeout_max_ms,
        heartbeat_interval: cfg.raft_heartbeat_interval_ms,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
            cfg.raft_snapshot_interval_entries,
        ),
        max_in_snapshot_log_to_keep: cfg.raft_snapshot_min_log_entries,
        ..Default::default()
    }
}
