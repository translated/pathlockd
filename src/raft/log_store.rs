//! openraft log/vote storage over the shared RocksDB, plus the cluster-wide
//! fsync batcher.
//!
//! Each Raft group's log lives in `CF_RAFT_LOG` under the group's key prefix
//! (`be32(group) ++ be64(index)`); votes, the committed pointer and the purge
//! marker live in group-scoped `CF_META` keys. All groups share one WAL, so
//! durability is amortized: appends are written unsynced and their
//! `IOFlushed` callbacks are queued to a single [`FsyncBatcher`] thread that
//! issues one `flush_wal(true)` per drained batch — the group-commit design
//! the single-node writer used, now spanning every Raft group on the node.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::RaftLogReader;
use rocksdb::DB;
use tracing::{error, info};

use crate::cluster::placement::GroupId;
use crate::raft::types::{Entry, LogId, TypeConfig, Vote};
use crate::store_keys;

fn io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn encode<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
    bincode::serde::encode_to_vec(v, bincode::config::standard()).map_err(io_err)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(io_err)
}

// ---------------------------------------------------------------------------
// FsyncBatcher
// ---------------------------------------------------------------------------

enum FsyncJob {
    /// An openraft append callback to fire once the WAL is durable.
    Flushed(IOFlushed<TypeConfig>),
    /// A synchronous waiter (vote persistence) to release once durable.
    Barrier(std::sync::mpsc::SyncSender<io::Result<()>>),
}

/// One thread, one WAL, one fsync per drained batch of jobs from any number
/// of Raft groups. A failed fsync poisons the node (fail-stop): durability of
/// already-written entries is unknown, so no further appends are acknowledged
/// and health turns not-ready.
#[derive(Clone)]
pub struct FsyncBatcher {
    tx: std::sync::mpsc::Sender<FsyncJob>,
    healthy: Arc<AtomicBool>,
}

impl FsyncBatcher {
    /// `wal_sync = false` skips the physical fsync (dev mode): callbacks fire
    /// immediately after the OS write, trading power-loss durability for
    /// latency, exactly like the old `rocksdb_wal_sync=false`.
    pub fn start(db: Arc<DB>, wal_sync: bool) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<FsyncJob>();
        let healthy = Arc::new(AtomicBool::new(true));
        {
            let healthy = healthy.clone();
            std::thread::Builder::new()
                .name("pathlockd-fsync".into())
                .spawn(move || fsync_loop(db, rx, healthy, wal_sync))
                .expect("spawning fsync batcher thread");
        }
        Self { tx, healthy }
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Queue an append callback for the next fsync.
    fn submit(&self, callback: IOFlushed<TypeConfig>) {
        if self.tx.send(FsyncJob::Flushed(callback)).is_err() {
            // Batcher exited (poisoned); the callback is dropped, which
            // openraft treats as an IO failure on shutdown paths.
            error!("fsync batcher unavailable; dropping append callback");
        }
    }

    /// Block until everything written so far is durable.
    pub fn barrier(&self) -> io::Result<()> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.tx
            .send(FsyncJob::Barrier(tx))
            .map_err(|_| io_err("fsync batcher unavailable"))?;
        rx.recv().map_err(|_| io_err("fsync batcher exited"))?
    }
}

fn fsync_loop(
    db: Arc<DB>,
    rx: std::sync::mpsc::Receiver<FsyncJob>,
    healthy: Arc<AtomicBool>,
    wal_sync: bool,
) {
    const BATCH_MAX: usize = 4096;
    while let Ok(first) = rx.recv() {
        let mut jobs = vec![first];
        while jobs.len() < BATCH_MAX {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }

        let result = if wal_sync {
            db.flush_wal(true).map_err(|e| io_err(&e))
        } else {
            Ok(())
        };

        match result {
            Ok(()) => {
                for job in jobs {
                    match job {
                        FsyncJob::Flushed(cb) => cb.io_completed(Ok(())),
                        FsyncJob::Barrier(tx) => {
                            let _ = tx.send(Ok(()));
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "WAL fsync failed; poisoning node (fail-stop)");
                healthy.store(false, Ordering::Relaxed);
                for job in jobs {
                    match job {
                        FsyncJob::Flushed(cb) => cb.io_completed(Err(io_err(&e))),
                        FsyncJob::Barrier(tx) => {
                            let _ = tx.send(Err(io_err(&e)));
                        }
                    }
                }
                // Exit: the closed channel fails all future submissions.
                return;
            }
        }
    }
    info!("fsync batcher stopped");
}

// ---------------------------------------------------------------------------
// Group log store
// ---------------------------------------------------------------------------

fn log_key(group: GroupId, index: u64) -> Vec<u8> {
    store_keys::group_key(group, &index.to_be_bytes())
}

/// The half-open key range of one group's log.
fn log_range(group: GroupId) -> (Vec<u8>, Vec<u8>) {
    let (start, end) = store_keys::group_range(group);
    let end = end.unwrap_or_else(|| vec![0xFF; 16]);
    (start, end)
}

#[derive(Clone)]
pub struct GroupLogStore {
    db: Arc<DB>,
    group: GroupId,
    batcher: FsyncBatcher,
}

impl GroupLogStore {
    pub fn new(db: Arc<DB>, group: GroupId, batcher: FsyncBatcher) -> Self {
        Self { db, group, batcher }
    }

    fn log_cf(&self) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(store_keys::CF_RAFT_LOG)
            .ok_or_else(|| io_err("missing raft_log column family"))
    }

    fn meta_cf(&self) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(store_keys::CF_META)
            .ok_or_else(|| io_err("missing meta column family"))
    }

    fn get_meta<T: serde::de::DeserializeOwned>(&self, key: &[u8]) -> io::Result<Option<T>> {
        let cf = self.meta_cf()?;
        let scoped = store_keys::group_key(self.group, key);
        match self.db.get_cf(&cf, &scoped).map_err(io_err)? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    fn put_meta<T: serde::Serialize>(&self, key: &[u8], value: &T) -> io::Result<()> {
        let cf = self.meta_cf()?;
        let scoped = store_keys::group_key(self.group, key);
        self.db.put_cf(&cf, &scoped, encode(value)?).map_err(io_err)
    }

    fn last_log_id_on_disk(&self) -> io::Result<Option<LogId>> {
        let cf = self.log_cf()?;
        let (start, end) = log_range(self.group);
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_iterate_lower_bound(start);
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);
        iter.seek_for_prev(&end);
        if !iter.valid() {
            iter.status().map_err(io_err)?;
            return Ok(None);
        }
        let Some(value) = iter.value() else {
            return Ok(None);
        };
        let entry: Entry = decode(value)?;
        Ok(Some(entry.log_id))
    }
}

impl RaftLogReader<TypeConfig> for GroupLogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, io::Error>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        use std::ops::Bound;
        let start_idx = match range.start_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&i) => i + 1,
            Bound::Unbounded => 0,
        };
        let end_idx = match range.end_bound() {
            Bound::Included(&i) => i.saturating_add(1),
            Bound::Excluded(&i) => i,
            Bound::Unbounded => u64::MAX,
        };
        if end_idx <= start_idx {
            return Ok(Vec::new());
        }

        let cf = self.log_cf()?;
        let lower = log_key(self.group, start_idx);
        let upper = if end_idx == u64::MAX {
            log_range(self.group).1
        } else {
            log_key(self.group, end_idx)
        };
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_iterate_upper_bound(upper);
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);
        iter.seek(&lower);
        let mut entries = Vec::new();
        while iter.valid() {
            if let Some(value) = iter.value() {
                entries.push(decode::<Entry>(value)?);
            }
            iter.next();
        }
        iter.status().map_err(io_err)?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        self.get_meta(store_keys::META_VOTE_KEY)
    }
}

impl RaftLogStorage<TypeConfig> for GroupLogStore {
    type LogReader = GroupLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged: Option<LogId> = self.get_meta(store_keys::META_PURGED_KEY)?;
        let last = self.last_log_id_on_disk()?.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        // A vote must be durable before this returns (Raft safety): persist
        // unsynced, then ride the next batched fsync.
        self.put_meta(store_keys::META_VOTE_KEY, vote)?;
        self.batcher.barrier()
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        match committed {
            Some(c) => self.put_meta(store_keys::META_COMMITTED_KEY, &c),
            None => Ok(()),
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        self.get_meta(store_keys::META_COMMITTED_KEY)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let cf = self.log_cf()?;
        let mut batch = rocksdb::WriteBatch::default();
        for entry in entries {
            let key = log_key(self.group, entry.log_id.index);
            batch.put_cf(&cf, &key, encode(&entry)?);
        }
        self.db
            .write_opt(batch, &rocksdb::WriteOptions::default())
            .map_err(io_err)?;
        self.batcher.submit(callback);
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let from_index = match &last_log_id {
            Some(id) => id.index.saturating_add(1),
            None => 0,
        };
        let cf = self.log_cf()?;
        let from = log_key(self.group, from_index);
        let to = log_range(self.group).1;
        self.db.delete_range_cf(&cf, &from, &to).map_err(io_err)?;
        // Conflicting (truncated) entries must not resurrect after a crash.
        self.batcher.barrier()
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        // Persist the purge marker first: a crash between marker and range
        // delete leaves extra entries, which is harmless; the reverse would
        // lose the purge position.
        self.put_meta(store_keys::META_PURGED_KEY, &log_id)?;
        let cf = self.log_cf()?;
        let from = log_key(self.group, 0);
        let to = log_key(self.group, log_id.index.saturating_add(1));
        self.db.delete_range_cf(&cf, &from, &to).map_err(io_err)?;
        let _ = self
            .db
            .delete_file_in_range_cf(&cf, from.as_slice(), to.as_slice());
        Ok(())
    }
}
