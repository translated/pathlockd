//! Group snapshot image format: build and install one group's entire state
//! as a stream of bincode frames.
//!
//! Lock state is small and TTL-bounded, so a snapshot is a full scan of the
//! group's key range across every state column family (plus the non-raft
//! group meta: monotone clock, GC cursor, fencing counter). Install replaces
//! the group's keyspace atomically in one WriteBatch — local stale readers
//! never observe a half-installed image.

use std::sync::Arc;

use rocksdb::DB;

use crate::cluster::placement::GroupId;
use crate::store_keys;
use crate::store_rocksdb::prefix_upper_bound;

/// One group-relative record inside a snapshot image.
#[derive(serde::Serialize, serde::Deserialize)]
struct Frame {
    /// Index into [`store_keys::STATE_CFS`], or [`META_CF_MARKER`] for the
    /// group-meta extras.
    cf: u8,
    key: Vec<u8>,
    value: Vec<u8>,
}

const META_CF_MARKER: u8 = u8::MAX;

/// Raft-owned group meta keys that must NOT travel inside a snapshot image
/// (vote safety; applied/membership/snapshot bookkeeping is carried in the
/// snapshot *meta*, not the image).
const RAFT_META_KEYS: &[&[u8]] = &[
    store_keys::META_VOTE_KEY,
    store_keys::META_COMMITTED_KEY,
    store_keys::META_LAST_APPLIED_KEY,
    store_keys::META_MEMBERSHIP_KEY,
    store_keys::META_PURGED_KEY,
    store_keys::META_SNAPSHOT_META_KEY,
    store_keys::META_SNAPSHOT_DATA_KEY,
];

fn scan_group_cf(
    db: &DB,
    cf_name: &str,
    group: GroupId,
    mut visit: impl FnMut(&[u8], &[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let handle = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("missing column family {cf_name}"))?;
    let gp = store_keys::group_prefix(group);
    let mut read_opts = rocksdb::ReadOptions::default();
    if let Some(upper) = prefix_upper_bound(&gp) {
        read_opts.set_iterate_upper_bound(upper);
    }
    let mut iter = db.raw_iterator_cf_opt(&handle, read_opts);
    iter.seek(gp);
    while iter.valid() {
        let key = iter.key().expect("valid iterator has a key");
        if !key.starts_with(&gp) {
            break;
        }
        visit(&key[gp.len()..], iter.value().unwrap_or_default())?;
        iter.next();
    }
    iter.status()?;
    Ok(())
}

/// Serialize one group's full state image.
pub fn build_group_image(db: &Arc<DB>, group: GroupId) -> anyhow::Result<Vec<u8>> {
    let mut frames: Vec<Frame> = Vec::new();
    for (idx, cf_name) in store_keys::STATE_CFS.iter().enumerate() {
        scan_group_cf(db, cf_name, group, |key, value| {
            frames.push(Frame {
                cf: idx as u8,
                key: key.to_vec(),
                value: value.to_vec(),
            });
            Ok(())
        })?;
    }
    scan_group_cf(db, store_keys::CF_META, group, |key, value| {
        if RAFT_META_KEYS.contains(&key) {
            return Ok(());
        }
        frames.push(Frame {
            cf: META_CF_MARKER,
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    })?;
    Ok(bincode::serde::encode_to_vec(
        &frames,
        bincode::config::standard(),
    )?)
}

/// Replace one group's state with a snapshot image, atomically.
///
/// The caller persists last-applied/membership (snapshot meta) and makes the
/// batch durable.
pub fn install_group_image(
    db: &Arc<DB>,
    group: GroupId,
    image: &[u8],
    batch: &mut rocksdb::WriteBatch,
) -> anyhow::Result<()> {
    let (frames, _): (Vec<Frame>, _) =
        bincode::serde::decode_from_slice(image, bincode::config::standard())?;

    let gp = store_keys::group_prefix(group);
    let upper = prefix_upper_bound(&gp).unwrap_or_else(|| vec![0xFF; 16]);

    // Wipe the group's current state...
    for cf_name in store_keys::STATE_CFS {
        let handle = db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf_name}"))?;
        batch.delete_range_cf(&handle, gp.as_slice(), upper.as_slice());
    }
    // ...but only the non-raft keys of the group meta (vote must survive).
    let meta = db
        .cf_handle(store_keys::CF_META)
        .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
    scan_group_cf(db, store_keys::CF_META, group, |key, _| {
        if !RAFT_META_KEYS.contains(&key) {
            batch.delete_cf(&meta, store_keys::group_key(group, key));
        }
        Ok(())
    })?;

    // ...then lay down the image.
    for frame in frames {
        let scoped = store_keys::group_key(group, &frame.key);
        if frame.cf == META_CF_MARKER {
            batch.put_cf(&meta, &scoped, &frame.value);
        } else {
            let cf_name = store_keys::STATE_CFS
                .get(frame.cf as usize)
                .ok_or_else(|| anyhow::anyhow!("snapshot frame names unknown cf {}", frame.cf))?;
            let handle = db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("missing column family {cf_name}"))?;
            batch.put_cf(&handle, &scoped, &frame.value);
        }
    }
    Ok(())
}
