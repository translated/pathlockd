//! Cluster membership directory, stored in the system group's state machine.
//!
//! For each Raft group the directory records its voters, learners, and last
//! known leader; per node, a draining flag. Records are written by group
//! leaders after reconciliation (`Op::DirectoryUpdate`) and replicated like
//! any other sys-group state, so every node — all nodes host a sys replica —
//! can answer "who serves group g" locally. The directory is a *routing and
//! observability* layer: openraft's own membership remains the correctness
//! authority.

use std::sync::Arc;

use rocksdb::DB;
use serde::{Deserialize, Serialize};

use crate::cluster::placement::{GroupId, SYS_GROUP};
use crate::store_keys;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub leader: Option<u64>,
}

/// Directory keys live in the sys group's `CF_META` keyspace, prefixed to
/// stay clear of raft bookkeeping keys.
pub fn group_record_key(group: GroupId) -> Vec<u8> {
    let mut key = b"dir:".to_vec();
    key.extend_from_slice(&group.to_be_bytes());
    key
}

pub fn draining_key(node_id: u64) -> Vec<u8> {
    let mut key = b"draining:".to_vec();
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

/// Read one group's directory record from the local sys replica.
pub fn read_group_record(db: &Arc<DB>, group: GroupId) -> anyhow::Result<Option<GroupRecord>> {
    let meta = db
        .cf_handle(store_keys::CF_META)
        .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
    let key = store_keys::group_key(SYS_GROUP, &group_record_key(group));
    match db.get_cf(&meta, &key)? {
        Some(bytes) => {
            let (record, _) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;
            Ok(Some(record))
        }
        None => Ok(None),
    }
}

/// Read the set of draining nodes from the local sys replica.
pub fn read_draining(db: &Arc<DB>) -> anyhow::Result<std::collections::BTreeSet<u64>> {
    let meta = db
        .cf_handle(store_keys::CF_META)
        .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
    let prefix = store_keys::group_key(SYS_GROUP, b"draining:");
    let upper = crate::store_rocksdb::prefix_upper_bound(&prefix);
    let mut read_opts = rocksdb::ReadOptions::default();
    if let Some(u) = upper {
        read_opts.set_iterate_upper_bound(u);
    }
    let mut iter = db.raw_iterator_cf_opt(&meta, read_opts);
    iter.seek(&prefix);
    let mut draining = std::collections::BTreeSet::new();
    while iter.valid() {
        let key = iter.key().expect("valid iterator has a key");
        if !key.starts_with(&prefix) {
            break;
        }
        let id_bytes = &key[prefix.len()..];
        if id_bytes.len() == 8 && iter.value() == Some(b"1".as_slice()) {
            draining.insert(u64::from_be_bytes(id_bytes.try_into().unwrap()));
        }
        iter.next();
    }
    iter.status()?;
    Ok(draining)
}

/// Apply a `DirectoryUpdate` inside the sys group's state machine.
pub(crate) fn apply_directory_update(
    txn: &mut crate::store_rocksdb::WriteTxn,
    group: u32,
    record: &GroupRecord,
) -> anyhow::Result<()> {
    let bytes = bincode::serde::encode_to_vec(record, bincode::config::standard())?;
    txn.put_raw(store_keys::CF_META, &group_record_key(group), bytes)
}

/// Apply a `SetNodeDraining` inside the sys group's state machine.
pub(crate) fn apply_set_draining(
    txn: &mut crate::store_rocksdb::WriteTxn,
    node_id: u64,
    draining: bool,
) -> anyhow::Result<()> {
    if draining {
        txn.put_raw(store_keys::CF_META, &draining_key(node_id), b"1".to_vec())
    } else {
        txn.delete_raw(store_keys::CF_META, &draining_key(node_id))
    }
}
