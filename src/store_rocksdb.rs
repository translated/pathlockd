//! StoreTxn trait, tuned RocksDB construction, and the two transaction views.
//!
//! The `StoreTxn` trait abstracts storage operations used by the lock engine.
//! All methods are synchronous because RocksDB operations are inherently sync.
//!
//! Two implementations exist:
//! - [`WriteTxn`]: used by the Raft state machine during apply. Writes go to a
//!   `WriteBatch` *and* an in-memory overlay, so every read inside the same
//!   command observes the command's own earlier writes (read-your-writes).
//!   Without the overlay a command could e.g. prune a dead owner's lock during
//!   validation and then re-read the stale committed record during execution.
//! - [`RocksDbTxn`]: a read-only view over committed state for observability
//!   reads (`inspect`, `detect_cycle`, `is_blocking`, `assert_fencing`).
//!
//! Both transactions are **group-scoped**: one node hosts replicas of many
//! Raft groups in a single shared RocksDB, and every key a transaction touches
//! is transparently prefixed with its group id (`store_keys::group_key`).
//! Engine-level code only ever sees group-relative keys.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use rocksdb::DB;

use crate::cluster::placement::{GroupId, SYS_GROUP};
use crate::store_keys::{self, expired};

// ---------------------------------------------------------------------------
// StoreTxn trait (sync)
// ---------------------------------------------------------------------------

pub trait StoreTxn {
    fn now_ms(&self) -> u64;

    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<String>>;
    fn set_str(
        &mut self,
        cf: &'static str,
        key: &[u8],
        value: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()>;
    fn pexpire_str(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> anyhow::Result<()>;
    fn del(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()>;

    fn sadd(
        &mut self,
        cf: &'static str,
        key: &[u8],
        member: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()>;
    fn srem(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<()>;
    fn smembers_limited(
        &mut self,
        cf: &'static str,
        key: &[u8],
        limit: usize,
    ) -> anyhow::Result<Vec<String>>;
    /// Scan one page of a set, resuming from `cursor` (a raw key returned by a
    /// previous page). `page` bounds the *raw keys scanned* — expired residue
    /// counts toward it — so a page is always bounded work and never errors on
    /// set size. Returns the live members found plus the cursor to resume
    /// from, or `None` when the set is exhausted.
    fn smembers_page(
        &mut self,
        cf: &'static str,
        key: &[u8],
        cursor: Option<Vec<u8>>,
        page: usize,
    ) -> anyhow::Result<(Vec<String>, Option<Vec<u8>>)>;
    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<bool>;
    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<bool>;
}

// ---------------------------------------------------------------------------
// Stored record encoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StoredRecord {
    Str {
        v: String,
        exp: u64,
    },
    Counter {
        v: i64,
    },
    /// Arbitrary binary payload with an expiry (e.g. cached ApplyResponses in
    /// the dedupe CF). Appended last so existing variant encodings are stable.
    Bytes {
        v: Vec<u8>,
        exp: u64,
    },
}

pub(crate) fn encode_record(rec: &StoredRecord) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(
        rec,
        bincode::config::standard(),
    )?)
}

/// Strict decode for point reads (propagates corruption).
pub(crate) fn decode_record(bytes: &[u8]) -> anyhow::Result<StoredRecord> {
    let (rec, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    Ok(rec)
}

/// Lenient decode for scans: undecodable entries are skipped, matching the
/// previous scan behaviour.
fn decode_record_lenient(bytes: &[u8]) -> Option<StoredRecord> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .ok()
        .map(|(rec, _)| rec)
}

fn live_str(rec: StoredRecord, now_ms: u64) -> Option<String> {
    match rec {
        StoredRecord::Str { v, exp } if !expired(exp, now_ms) => Some(v),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
#[error("{operation} on set would enumerate more than {limit} live members")]
pub struct SetScanLimitExceeded {
    pub operation: &'static str,
    pub limit: usize,
}

// ---------------------------------------------------------------------------
// Tuned database construction
// ---------------------------------------------------------------------------

const MIB: u64 = 1024 * 1024;

/// RocksDB tuning knobs surfaced through the daemon config.
#[derive(Debug, Clone)]
pub struct DbTuning {
    pub max_open_files: i32,
    pub max_total_wal_size_mb: u64,
    pub max_background_jobs: i32,
    pub block_cache_mb: u64,
    pub write_buffer_mb: u64,
}

impl Default for DbTuning {
    fn default() -> Self {
        Self {
            max_open_files: 4096,
            max_total_wal_size_mb: 512,
            max_background_jobs: 4,
            block_cache_mb: 128,
            write_buffer_mb: 16,
        }
    }
}

/// Open (or create) a pathlockd RocksDB with all column families and a
/// workload-appropriate configuration.
///
/// Notable choices:
/// - `PointInTimeRecovery` WAL recovery: an unclean shutdown (SIGKILL, power
///   loss, torn final WAL record) recovers to the last consistent point
///   instead of refusing to open. Acknowledged writes are fsynced by the
///   writer before the ack, so nothing acknowledged is ever lost.
///   (`AbsoluteConsistency` turned any torn tail into a permanently
///   unopenable database — the "wipe the volume to recover" failure.)
/// - A bounded total WAL size: with many column families, rarely-written CFs
///   (e.g. `meta`) otherwise pin old WAL files for days and the WAL grows by
///   gigabytes; the bound force-flushes the laggards.
/// - Bloom filters + a shared block cache: the engine does many point gets
///   for keys that are usually absent.
/// - Compact-on-deletion collectors: the expiry index and the churn-heavy set
///   CFs are written and deleted in queue order, which accretes tombstone
///   walls that make forward scans degrade over weeks. Tombstone-dense SST
///   files are scheduled for compaction proactively.
pub fn open_db(path: &Path, tuning: &DbTuning) -> anyhow::Result<Arc<DB>> {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);
    db_opts.set_max_open_files(tuning.max_open_files);
    db_opts.set_max_total_wal_size(tuning.max_total_wal_size_mb.saturating_mul(MIB));
    db_opts.set_max_background_jobs(tuning.max_background_jobs);

    let cache = rocksdb::Cache::new_lru_cache(
        usize::try_from(tuning.block_cache_mb.saturating_mul(MIB)).unwrap_or(usize::MAX),
    );

    let cf_descriptors: Vec<rocksdb::ColumnFamilyDescriptor> = store_keys::ALL_CFS
        .iter()
        .map(|name| rocksdb::ColumnFamilyDescriptor::new(*name, cf_options(name, &cache, tuning)))
        .collect();

    let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
        .map_err(|e| anyhow::anyhow!("opening RocksDB at {}: {e}", path.display()))?;
    Ok(Arc::new(db))
}

fn cf_options(name: &str, cache: &rocksdb::Cache, tuning: &DbTuning) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.set_write_buffer_size(
        usize::try_from(tuning.write_buffer_mb.saturating_mul(MIB)).unwrap_or(usize::MAX),
    );
    opts.set_level_compaction_dynamic_level_bytes(true);
    // Backstop: rewrite any SST untouched for a day so trivial tombstones and
    // expired records do not survive indefinitely in cold levels.
    opts.set_periodic_compaction_seconds(86_400);

    let mut table = rocksdb::BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_bloom_filter(10.0, false);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    opts.set_block_based_table_factory(&table);

    if name == store_keys::CF_EXPIRY || name == store_keys::CF_RAFT_LOG {
        // Queue-shaped workloads: written at the head, range-deleted from the
        // tail (expiry sweep / raft log purge).
        opts.add_compact_on_deletion_collector_factory(10_000, 2_500, 0.25);
    } else if store_keys::STATE_CFS.contains(&name) {
        opts.add_compact_on_deletion_collector_factory(10_000, 5_000, 0.5);
    }
    opts
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

pub(crate) fn member_prefix(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(key.len() + 1);
    buf.extend_from_slice(key);
    buf.push(0);
    buf
}

pub(crate) fn member_key(key: &[u8], member: &str) -> Vec<u8> {
    let mut buf = member_prefix(key);
    buf.extend_from_slice(member.as_bytes());
    buf
}

/// The smallest key strictly greater than every key starting with `prefix`,
/// or `None` if no such key exists (prefix is all `0xFF`).
pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.last_mut() {
        if *last == 0xFF {
            upper.pop();
        } else {
            *last += 1;
            return Some(upper);
        }
    }
    None
}

/// The smallest key strictly greater than `key` (for resuming a scan after it).
pub(crate) fn key_successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

// ---------------------------------------------------------------------------
// Merged range scan
// ---------------------------------------------------------------------------

type Overlay = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// Walk `[start, upper)` of a column family in key order, merging committed
/// state with an optional uncommitted overlay (overlay entries shadow
/// committed ones; `None` overlay entries are deletions and are skipped).
///
/// `upper`, when given, is applied as the iterator's upper bound so the scan
/// never wades through tombstones beyond the range of interest. `visit`
/// returns `false` to stop early.
pub(crate) fn scan_with_overlay<F>(
    db: &DB,
    cf: &'static str,
    overlay: Option<&Overlay>,
    start: Option<&[u8]>,
    upper: Option<&[u8]>,
    mut visit: F,
) -> anyhow::Result<()>
where
    F: FnMut(&[u8], &[u8]) -> anyhow::Result<bool>,
{
    let cf_handle = db
        .cf_handle(cf)
        .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
    let mut read_opts = rocksdb::ReadOptions::default();
    if let Some(u) = upper {
        read_opts.set_iterate_upper_bound(u.to_vec());
    }
    let mut iter = db.raw_iterator_cf_opt(&cf_handle, read_opts);
    match start {
        Some(s) => iter.seek(s),
        None => iter.seek_to_first(),
    }

    use std::ops::Bound;
    let empty = Overlay::new();
    let lower_bound = match start {
        Some(s) => Bound::Included(s.to_vec()),
        None => Bound::Unbounded,
    };
    let upper_bound = match upper {
        Some(u) => Bound::Excluded(u.to_vec()),
        None => Bound::Unbounded,
    };
    let mut ov = overlay
        .unwrap_or(&empty)
        .range((lower_bound, upper_bound))
        .peekable();

    loop {
        let db_key: Option<Vec<u8>> = if iter.valid() {
            iter.key().map(|k| k.to_vec())
        } else {
            iter.status()?;
            None
        };
        let take_overlay = match (&db_key, ov.peek()) {
            (None, None) => break,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(dk), Some((ok, _))) => dk.as_slice() >= ok.as_slice(),
        };

        let keep_going = if take_overlay {
            let (k, v) = ov.next().expect("peeked overlay entry");
            // An overlay entry for a key also present in the DB shadows it.
            if db_key.as_deref() == Some(k.as_slice()) {
                iter.next();
            }
            match v {
                Some(value) => visit(k, value)?,
                None => true, // deleted in this txn
            }
        } else {
            let k = db_key.expect("checked valid");
            let v = iter.value().map(|v| v.to_vec()).unwrap_or_default();
            iter.next();
            visit(&k, &v)?
        };
        if !keep_going {
            break;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared set-scan implementations
// ---------------------------------------------------------------------------

/// Cap on raw keys scanned while enumerating up to `limit` live members.
/// Bounds the work spent skipping expired-but-unswept residue without
/// erroring out the moment a set carries transient garbage.
fn raw_scan_cap(limit: usize) -> usize {
    limit.saturating_mul(4)
}

fn smembers_limited_impl(
    db: &DB,
    cf: &'static str,
    overlay: Option<&Overlay>,
    key: &[u8],
    limit: usize,
    now_ms: u64,
) -> anyhow::Result<Vec<String>> {
    let prefix = member_prefix(key);
    let upper = prefix_upper_bound(&prefix);
    let mut members = Vec::new();
    let mut raw = 0usize;
    let raw_cap = raw_scan_cap(limit);
    let mut exceeded = false;
    scan_with_overlay(db, cf, overlay, Some(&prefix), upper.as_deref(), |k, v| {
        if !k.starts_with(&prefix) {
            return Ok(false);
        }
        raw += 1;
        if let Some(member) = decode_record_lenient(v).and_then(|r| live_str(r, now_ms)) {
            if members.len() >= limit {
                exceeded = true;
                return Ok(false);
            }
            members.push(member);
        }
        if raw >= raw_cap {
            exceeded = true;
            return Ok(false);
        }
        Ok(true)
    })?;
    if exceeded {
        return Err(SetScanLimitExceeded {
            operation: "smembers",
            limit,
        }
        .into());
    }
    Ok(members)
}

fn smembers_page_impl(
    db: &DB,
    cf: &'static str,
    overlay: Option<&Overlay>,
    key: &[u8],
    cursor: Option<Vec<u8>>,
    page: usize,
    now_ms: u64,
) -> anyhow::Result<(Vec<String>, Option<Vec<u8>>)> {
    let prefix = member_prefix(key);
    let upper = prefix_upper_bound(&prefix);
    let start = cursor.unwrap_or_else(|| prefix.clone());
    let mut members = Vec::new();
    let mut raw = 0usize;
    let mut next_cursor = None;
    scan_with_overlay(db, cf, overlay, Some(&start), upper.as_deref(), |k, v| {
        if !k.starts_with(&prefix) {
            return Ok(false);
        }
        if raw >= page {
            next_cursor = Some(k.to_vec());
            return Ok(false);
        }
        raw += 1;
        if let Some(member) = decode_record_lenient(v).and_then(|r| live_str(r, now_ms)) {
            members.push(member);
        }
        Ok(true)
    })?;
    Ok((members, next_cursor))
}

fn has_live_member_impl(
    db: &DB,
    cf: &'static str,
    overlay: Option<&Overlay>,
    key: &[u8],
    now_ms: u64,
) -> anyhow::Result<bool> {
    let prefix = member_prefix(key);
    let upper = prefix_upper_bound(&prefix);
    let mut found = false;
    scan_with_overlay(db, cf, overlay, Some(&prefix), upper.as_deref(), |k, v| {
        if !k.starts_with(&prefix) {
            return Ok(false);
        }
        if decode_record_lenient(v)
            .and_then(|r| live_str(r, now_ms))
            .is_some()
        {
            found = true;
            return Ok(false);
        }
        Ok(true)
    })?;
    Ok(found)
}

// ---------------------------------------------------------------------------
// WriteTxn: WriteBatch + read-your-writes overlay
// ---------------------------------------------------------------------------

/// The state machine's transaction: writes accumulate in a `WriteBatch` (for
/// one atomic commit) and in an overlay (so the command's own reads observe
/// them). Dropping the transaction without calling [`WriteTxn::commit`]
/// discards every write — used to keep failed commands (conflict / lost
/// outcomes) from committing partial state.
pub struct WriteTxn {
    db: Arc<DB>,
    group: GroupId,
    batch: rocksdb::WriteBatch,
    overlay: HashMap<&'static str, Overlay>,
    now_ms: u64,
    dirty: bool,
}

impl WriteTxn {
    pub fn new(db: Arc<DB>, group: GroupId, now_ms: u64) -> Self {
        Self {
            db,
            group,
            batch: rocksdb::WriteBatch::default(),
            overlay: HashMap::new(),
            now_ms,
            dirty: false,
        }
    }

    /// Scope a group-relative key into this transaction's group keyspace.
    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        store_keys::group_key(self.group, key)
    }

    /// Overlay-aware raw point read of a group-relative key.
    pub fn get_raw(&self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.get_full(cf, &self.scoped(key))
    }

    /// Overlay-aware read of an already-scoped (full) key.
    fn get_full(&self, cf: &'static str, full_key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(entry) = self.overlay.get(cf).and_then(|m| m.get(full_key)) {
            return Ok(entry.clone());
        }
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
        Ok(self.db.get_cf(&cf_handle, full_key)?)
    }

    pub fn put_raw(&mut self, cf: &'static str, key: &[u8], value: Vec<u8>) -> anyhow::Result<()> {
        let full_key = self.scoped(key);
        self.put_full(cf, full_key, value)
    }

    fn put_full(
        &mut self,
        cf: &'static str,
        full_key: Vec<u8>,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        let db = self.db.clone();
        let cf_handle = db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
        self.batch.put_cf(&cf_handle, &full_key, &value);
        self.overlay
            .entry(cf)
            .or_default()
            .insert(full_key, Some(value));
        self.dirty = true;
        Ok(())
    }

    pub fn delete_raw(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()> {
        let full_key = self.scoped(key);
        let db = self.db.clone();
        let cf_handle = db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
        self.batch.delete_cf(&cf_handle, &full_key);
        self.overlay.entry(cf).or_default().insert(full_key, None);
        self.dirty = true;
        Ok(())
    }

    /// Overlay-aware ordered scan of `[start, upper)` within this group's
    /// keyspace. Bounds are group-relative; `visit` receives group-relative
    /// keys. `upper = None` scans to the end of the group's keyspace, never
    /// into a neighbouring group.
    pub fn scan_merged<F>(
        &self,
        cf: &'static str,
        start: Option<&[u8]>,
        upper: Option<&[u8]>,
        mut visit: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> anyhow::Result<bool>,
    {
        let gp = store_keys::group_prefix(self.group);
        let scoped_start = match start {
            Some(s) => store_keys::group_key(self.group, s),
            None => gp.to_vec(),
        };
        let scoped_upper = match upper {
            Some(u) => Some(store_keys::group_key(self.group, u)),
            None => prefix_upper_bound(&gp),
        };
        scan_with_overlay(
            &self.db,
            cf,
            self.overlay.get(cf),
            Some(&scoped_start),
            scoped_upper.as_deref(),
            |k, v| visit(&k[gp.len()..], v),
        )
    }

    /// Atomically commit the accumulated writes. Returns `false` when the
    /// transaction had nothing to write.
    ///
    /// The WAL is *not* fsynced here: the serialized writer fsyncs once per
    /// drained group of commands (group commit) before acknowledging any of
    /// them, which preserves the durability contract at a fraction of the
    /// per-command fsync cost.
    pub fn commit(self) -> anyhow::Result<bool> {
        if !self.dirty {
            return Ok(false);
        }
        let opts = rocksdb::WriteOptions::default();
        self.db.write_opt(self.batch, &opts)?;
        Ok(true)
    }

    /// Store an expiring binary record (TTL-indexed like `set_str`).
    pub fn set_bytes(
        &mut self,
        cf: &'static str,
        key: &[u8],
        value: Vec<u8>,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        let exp = store_keys::expiry_at(self.now_ms, ttl_ms);
        let record = encode_record(&StoredRecord::Bytes { v: value, exp })?;
        self.put_raw(cf, key, record)?;
        if ttl_ms > 0 {
            self.write_expiry(exp, cf, key)?;
        }
        Ok(())
    }

    /// Read a live binary record written by [`WriteTxn::set_bytes`].
    pub fn get_bytes(&self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        match self.get_raw(cf, key)? {
            Some(raw) => match decode_record(&raw)? {
                StoredRecord::Bytes { v, exp } if !expired(exp, self.now_ms) => Ok(Some(v)),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn write_expiry(
        &mut self,
        exp: u64,
        cf: &'static str,
        primary_key: &[u8],
    ) -> anyhow::Result<()> {
        // Long leases share one quantized index slot across refreshes; see
        // `quantized_index_expiry`. The GC sweep re-checks the record's real
        // expiry before reclaiming, so a late-firing index entry is harmless.
        let index_exp = store_keys::quantized_index_expiry(self.now_ms, exp);
        let ek = store_keys::expiry_key(index_exp, cf, primary_key);
        let record = encode_record(&StoredRecord::Str {
            v: String::new(),
            exp: index_exp,
        })?;
        self.put_raw(store_keys::CF_EXPIRY, &ek, record)
    }
}

impl StoreTxn for WriteTxn {
    fn now_ms(&self) -> u64 {
        self.now_ms
    }

    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<String>> {
        match self.get_raw(cf, key)? {
            Some(bytes) => Ok(live_str(decode_record(&bytes)?, self.now_ms)),
            None => Ok(None),
        }
    }

    fn set_str(
        &mut self,
        cf: &'static str,
        key: &[u8],
        value: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        let exp = store_keys::expiry_at(self.now_ms, ttl_ms);
        let record = encode_record(&StoredRecord::Str {
            v: value.to_string(),
            exp,
        })?;
        self.put_raw(cf, key, record)?;
        if ttl_ms > 0 {
            self.write_expiry(exp, cf, key)?;
        }
        Ok(())
    }

    fn pexpire_str(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> anyhow::Result<()> {
        if let Some(v) = self.get_str(cf, key)? {
            self.set_str(cf, key, &v, ttl_ms)?;
        }
        Ok(())
    }

    fn del(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()> {
        self.delete_raw(cf, key)
    }

    fn sadd(
        &mut self,
        cf: &'static str,
        key: &[u8],
        member: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        let exp = store_keys::expiry_at(self.now_ms, ttl_ms);
        let record = encode_record(&StoredRecord::Str {
            v: member.to_string(),
            exp,
        })?;
        let mk = member_key(key, member);
        self.put_raw(cf, &mk, record)?;
        if ttl_ms > 0 {
            self.write_expiry(exp, cf, &mk)?;
        }
        Ok(())
    }

    fn srem(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<()> {
        self.delete_raw(cf, &member_key(key, member))
    }

    fn smembers_limited(
        &mut self,
        cf: &'static str,
        key: &[u8],
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let sk = self.scoped(key);
        smembers_limited_impl(&self.db, cf, self.overlay.get(cf), &sk, limit, self.now_ms)
    }

    fn smembers_page(
        &mut self,
        cf: &'static str,
        key: &[u8],
        cursor: Option<Vec<u8>>,
        page: usize,
    ) -> anyhow::Result<(Vec<String>, Option<Vec<u8>>)> {
        // The page cursor is a full member key *relative to the group*; the
        // shared impl works on scoped keys, so translate in and out.
        let sk = self.scoped(key);
        let scoped_cursor = cursor.map(|c| store_keys::group_key(self.group, &c));
        let (members, next) = smembers_page_impl(
            &self.db,
            cf,
            self.overlay.get(cf),
            &sk,
            scoped_cursor,
            page,
            self.now_ms,
        )?;
        Ok((members, next.map(|n| n[4..].to_vec())))
    }

    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<bool> {
        let mk = member_key(key, member);
        match self.get_raw(cf, &mk)? {
            Some(bytes) => Ok(matches!(
                decode_record(&bytes)?,
                StoredRecord::Str { exp, .. } if !expired(exp, self.now_ms)
            )),
            None => Ok(false),
        }
    }

    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<bool> {
        let sk = self.scoped(key);
        has_live_member_impl(&self.db, cf, self.overlay.get(cf), &sk, self.now_ms)
    }
}

// ---------------------------------------------------------------------------
// Read-only RocksDB-backed StoreTxn for observability
// ---------------------------------------------------------------------------

pub struct RocksDbTxn {
    db: Arc<DB>,
    group: GroupId,
    now_ms: u64,
}

impl RocksDbTxn {
    pub fn new(db: Arc<DB>, group: GroupId, now_ms: u64) -> Self {
        Self { db, group, now_ms }
    }

    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        store_keys::group_key(self.group, key)
    }
}

impl StoreTxn for RocksDbTxn {
    fn now_ms(&self) -> u64 {
        self.now_ms
    }

    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<String>> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
        match self.db.get_cf(&cf_handle, self.scoped(key))? {
            Some(v) => Ok(live_str(decode_record(&v)?, self.now_ms)),
            None => Ok(None),
        }
    }

    fn set_str(
        &mut self,
        _cf: &'static str,
        _key: &[u8],
        _value: &str,
        _ttl_ms: u64,
    ) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    fn pexpire_str(&mut self, _cf: &'static str, _key: &[u8], _ttl_ms: u64) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    // `del`/`srem` are best-effort lazy cleanup of already-expired or
    // dead-owner entries. On this read-only view (used by `detect_cycle` and
    // `is_blocking`) they are dropped silently rather than erroring: the query
    // result is computed from liveness checks and stays correct, and the actual
    // pruning is performed by the serialized write path and the GC sweep.
    fn del(&mut self, _cf: &'static str, _key: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }

    fn sadd(
        &mut self,
        _cf: &'static str,
        _key: &[u8],
        _member: &str,
        _ttl_ms: u64,
    ) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    fn srem(&mut self, _cf: &'static str, _key: &[u8], _member: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn smembers_limited(
        &mut self,
        cf: &'static str,
        key: &[u8],
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let sk = self.scoped(key);
        smembers_limited_impl(&self.db, cf, None, &sk, limit, self.now_ms)
    }

    fn smembers_page(
        &mut self,
        cf: &'static str,
        key: &[u8],
        cursor: Option<Vec<u8>>,
        page: usize,
    ) -> anyhow::Result<(Vec<String>, Option<Vec<u8>>)> {
        let sk = self.scoped(key);
        let scoped_cursor = cursor.map(|c| store_keys::group_key(self.group, &c));
        let (members, next) =
            smembers_page_impl(&self.db, cf, None, &sk, scoped_cursor, page, self.now_ms)?;
        Ok((members, next.map(|n| n[4..].to_vec())))
    }

    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<bool> {
        let member_key = self.scoped(&member_key(key, member));
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
        match self.db.get_cf(&cf_handle, &member_key)? {
            Some(v) => Ok(matches!(
                decode_record(&v)?,
                StoredRecord::Str { exp, .. } if !expired(exp, self.now_ms)
            )),
            None => Ok(false),
        }
    }

    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<bool> {
        let sk = self.scoped(key);
        has_live_member_impl(&self.db, cf, None, &sk, self.now_ms)
    }
}

// ---------------------------------------------------------------------------
// Lock dump (paginated full scan over owner_holds)
// ---------------------------------------------------------------------------

/// Raw keys examined per dump page, bounding the work spent skipping
/// expired/dead residue between live entries.
const DUMP_RAW_SCAN_CAP: usize = 65_536;

/// Scan one page of every owner's held locks within one group. `cursor` is
/// the group-relative raw key to resume from (returned by the previous page);
/// `page` caps the entries returned. Dead owners' residue is skipped, not
/// reported.
pub fn dump_owner_holds(
    db: &Arc<DB>,
    group: GroupId,
    now_ms: u64,
    cursor: Option<Vec<u8>>,
    page: usize,
) -> anyhow::Result<crate::engine::LockDumpPage> {
    use crate::engine::{LockDumpPage, LockEntry, Mode};

    let mut entries: Vec<LockEntry> = Vec::new();
    let mut alive_memo: HashMap<String, bool> = HashMap::new();
    let mut raw = 0usize;
    let mut next_cursor: Option<Vec<u8>> = None;

    let mut fence_txn = RocksDbTxn::new(db.clone(), group, now_ms);

    let mut owner_alive = |owner: &str, txn: &mut RocksDbTxn| -> anyhow::Result<bool> {
        if let Some(alive) = alive_memo.get(owner) {
            return Ok(*alive);
        }
        let alive = txn
            .get_str(store_keys::CF_OWNER_ALIVE, &store_keys::alive_key(owner))?
            .is_some();
        alive_memo.insert(owner.to_string(), alive);
        Ok(alive)
    };

    let gp = store_keys::group_prefix(group);
    let start = match &cursor {
        Some(c) => store_keys::group_key(group, c),
        None => gp.to_vec(),
    };
    let upper = prefix_upper_bound(&gp);
    scan_with_overlay(
        db,
        store_keys::CF_OWNER_HOLDS,
        None,
        Some(&start),
        upper.as_deref(),
        |full_key, v| {
            let k = &full_key[gp.len()..];
            if entries.len() >= page || raw >= DUMP_RAW_SCAN_CAP {
                next_cursor = Some(k.to_vec());
                return Ok(false);
            }
            raw += 1;

            // Key layout: owner \0 \0 member — owner is everything before the
            // first double-NUL (see `member_key` over `own_prefix`).
            let Some(sep) = k.windows(2).position(|w| w == [0, 0]) else {
                return Ok(true);
            };
            let Ok(owner) = std::str::from_utf8(&k[..sep]) else {
                return Ok(true);
            };
            let Some(member) = decode_record_lenient(v).and_then(|r| live_str(r, now_ms)) else {
                return Ok(true);
            };
            let Some(mode_sep) = member.find(':') else {
                return Ok(true);
            };
            let mode = match &member[..mode_sep] {
                "write" => Mode::Write,
                "read" => Mode::Read,
                _ => return Ok(true),
            };
            let path = &member[mode_sep + 1..];

            if !owner_alive(owner, &mut fence_txn)? {
                return Ok(true);
            }

            let fence = if mode == Mode::Write {
                fence_txn
                    .get_str(store_keys::CF_FENCES, &store_keys::fence_key(path))?
                    .and_then(|s| s.parse::<i64>().ok())
            } else {
                None
            };

            entries.push(LockEntry {
                owner: owner.to_string(),
                path: path.to_string(),
                mode,
                fence,
            });
            Ok(true)
        },
    )?;

    Ok(LockDumpPage {
        entries,
        next_cursor,
    })
}

// ---------------------------------------------------------------------------
// Group lifecycle utilities
// ---------------------------------------------------------------------------

/// Range-delete a lock group's entire keyspace (state CFs, raft log, meta) —
/// used when this node stops hosting the group. The system group is never
/// destroyed locally (every node keeps at least a learner replica of it).
pub fn destroy_group(db: &DB, group: GroupId) -> anyhow::Result<()> {
    anyhow::ensure!(group != SYS_GROUP, "refusing to destroy the system group");
    let (start, end) = store_keys::group_range(group);
    let end = end.expect("non-sys group has a bounded range");
    for cf in store_keys::STATE_CFS
        .iter()
        .chain([store_keys::CF_RAFT_LOG, store_keys::CF_META].iter())
    {
        let handle = db
            .cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("missing column family {cf}"))?;
        db.delete_range_cf(&handle, &start, &end)?;
        let _ = db.delete_file_in_range_cf(&handle, &start, &end);
    }
    Ok(())
}

/// Physically reclaim the swept region of one group's expiry index:
/// everything below the group's persisted GC cursor is already logically
/// deleted, so whole SST files in that range are dropped and the remainder
/// compacted, keeping the queue-shaped index free of tombstone walls.
pub fn compact_swept_expiry(db: &DB, group: GroupId) -> anyhow::Result<()> {
    let meta = db
        .cf_handle(store_keys::CF_META)
        .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
    let cursor_key = store_keys::group_key(group, store_keys::META_GC_CURSOR_KEY);
    let Some(cursor) = db.get_cf(&meta, &cursor_key)? else {
        return Ok(());
    };
    let expiry = db
        .cf_handle(store_keys::CF_EXPIRY)
        .ok_or_else(|| anyhow::anyhow!("missing expiry column family"))?;
    let from = store_keys::group_prefix(group).to_vec();
    let to = store_keys::group_key(group, &cursor);
    db.delete_file_in_range_cf(&expiry, from.as_slice(), to.as_slice())
        .map_err(|e| anyhow::anyhow!("delete_file_in_range: {e}"))?;
    db.compact_range_cf(&expiry, Some(from.as_slice()), Some(to.as_slice()));
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_keys;
    use std::sync::Arc;

    fn open_test_db() -> (Arc<rocksdb::DB>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let db = open_db(&db_path, &DbTuning::default()).unwrap();
        (db, dir)
    }

    // --- prefix / successor helpers ---

    #[test]
    fn prefix_upper_bound_increments_last_byte() {
        assert_eq!(prefix_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_upper_bound(b"a\x00"), Some(b"a\x01".to_vec()));
        assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
    }

    #[test]
    fn key_successor_is_strictly_greater_and_minimal() {
        let succ = key_successor(b"abc");
        assert!(succ.as_slice() > b"abc".as_slice());
        assert!(succ.as_slice() < b"abc\x01".as_slice());
    }

    // --- WriteTxn: read-your-writes ---

    #[test]
    fn write_txn_reads_observe_pending_writes() {
        let (db, _dir) = open_test_db();
        let mut txn = WriteTxn::new(db, 0, 1_000);
        txn.set_str(store_keys::CF_WRITE_LOCKS, b"h:/a", "alice", 5_000)
            .unwrap();
        assert_eq!(
            txn.get_str(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap(),
            Some("alice".to_string())
        );
        txn.del(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap();
        assert_eq!(
            txn.get_str(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap(),
            None
        );
    }

    #[test]
    fn write_txn_pending_delete_shadows_committed_value() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            txn.set_str(store_keys::CF_WRITE_LOCKS, b"h:/a", "alice", 5_000)
                .unwrap();
            assert!(txn.commit().unwrap());
        }
        let mut txn = WriteTxn::new(db, 0, 1_001);
        assert_eq!(
            txn.get_str(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap(),
            Some("alice".to_string())
        );
        txn.del(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap();
        assert_eq!(
            txn.get_str(store_keys::CF_WRITE_LOCKS, b"h:/a").unwrap(),
            None
        );
    }

    #[test]
    fn write_txn_set_scans_merge_overlay_and_committed() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/a", 5_000)
                .unwrap();
            txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/b", 5_000)
                .unwrap();
            assert!(txn.commit().unwrap());
        }
        let mut txn = WriteTxn::new(db, 0, 1_001);
        txn.srem(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/a")
            .unwrap();
        txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/c", 5_000)
            .unwrap();
        let members = txn
            .smembers_limited(store_keys::CF_OWNER_HOLDS, b"alice", 100)
            .unwrap();
        assert_eq!(
            members,
            vec!["write:h:/b".to_string(), "write:h:/c".to_string()]
        );
        assert!(txn
            .sismember(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/c")
            .unwrap());
        assert!(!txn
            .sismember(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/a")
            .unwrap());
        assert!(txn
            .has_live_member(store_keys::CF_OWNER_HOLDS, b"alice")
            .unwrap());
    }

    #[test]
    fn write_txn_has_live_member_sees_pending_removal_of_last_member() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/a", 5_000)
                .unwrap();
            txn.commit().unwrap();
        }
        let mut txn = WriteTxn::new(db, 0, 1_001);
        txn.srem(store_keys::CF_OWNER_HOLDS, b"alice", "write:h:/a")
            .unwrap();
        assert!(!txn
            .has_live_member(store_keys::CF_OWNER_HOLDS, b"alice")
            .unwrap());
    }

    #[test]
    fn write_txn_commit_reports_empty_batches() {
        let (db, _dir) = open_test_db();
        let txn = WriteTxn::new(db, 0, 1_000);
        assert!(!txn.commit().unwrap());
    }

    // --- pagination ---

    #[test]
    fn smembers_page_walks_set_with_cursor() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            for i in 0..10 {
                txn.sadd(
                    store_keys::CF_OWNER_HOLDS,
                    b"alice",
                    &format!("write:h:/p{i:02}"),
                    5_000,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        let mut txn = RocksDbTxn::new(db, 0, 1_001);
        let mut all = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let (members, next) = txn
                .smembers_page(store_keys::CF_OWNER_HOLDS, b"alice", cursor.take(), 3)
                .unwrap();
            all.extend(members);
            pages += 1;
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(all.len(), 10);
        assert!(pages >= 4, "expected at least 4 pages of 3, got {pages}");
        assert_eq!(all[0], "write:h:/p00");
        assert_eq!(all[9], "write:h:/p09");
    }

    #[test]
    fn smembers_page_counts_expired_residue_without_erroring() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            for i in 0..20 {
                // Half the members expire at 1_500.
                let ttl = if i % 2 == 0 { 500 } else { 60_000 };
                txn.sadd(
                    store_keys::CF_OWNER_HOLDS,
                    b"alice",
                    &format!("write:h:/p{i:02}"),
                    ttl,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        let mut txn = RocksDbTxn::new(db, 0, 2_000);
        let mut live = Vec::new();
        let mut cursor = None;
        loop {
            let (members, next) = txn
                .smembers_page(store_keys::CF_OWNER_HOLDS, b"alice", cursor.take(), 4)
                .unwrap();
            live.extend(members);
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(live.len(), 10);
    }

    // --- limits ---

    #[test]
    fn smembers_limited_errors_when_live_members_exceed_limit() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            for i in 0..8 {
                txn.sadd(
                    store_keys::CF_OWNER_HOLDS,
                    b"alice",
                    &format!("m{i}"),
                    60_000,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        let mut txn = RocksDbTxn::new(db, 0, 1_001);
        let err = txn
            .smembers_limited(store_keys::CF_OWNER_HOLDS, b"alice", 4)
            .unwrap_err();
        assert!(err.downcast_ref::<SetScanLimitExceeded>().is_some());
    }

    #[test]
    fn smembers_limited_tolerates_expired_residue_within_raw_cap() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            for i in 0..6 {
                txn.sadd(
                    store_keys::CF_OWNER_HOLDS,
                    b"alice",
                    &format!("dead{i}"),
                    100,
                )
                .unwrap();
            }
            txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice", "live", 60_000)
                .unwrap();
            txn.commit().unwrap();
        }
        // 7 raw keys, 1 live: limit 4 (raw cap 16) must succeed.
        let mut txn = RocksDbTxn::new(db, 0, 5_000);
        let members = txn
            .smembers_limited(store_keys::CF_OWNER_HOLDS, b"alice", 4)
            .unwrap();
        assert_eq!(members, vec!["live".to_string()]);
    }

    // --- read-only txn ---

    #[test]
    fn read_only_txn_get_str_returns_none_for_missing_key() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 0, 100_000);
        assert!(txn
            .get_str(store_keys::CF_WRITE_LOCKS, b"nonexistent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn read_only_txn_mutations_fail_or_noop() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 0, 100_000);
        assert!(txn
            .set_str(store_keys::CF_WRITE_LOCKS, b"key", "val", 1000)
            .is_err());
        assert!(txn
            .sadd(store_keys::CF_OWNER_HOLDS, b"set", "member", 1000)
            .is_err());
        // Lazy-cleanup ops must succeed as silent no-ops so detect_cycle /
        // is_blocking can run against the committed view.
        assert!(txn.del(store_keys::CF_WRITE_LOCKS, b"key").is_ok());
        assert!(txn
            .srem(store_keys::CF_OWNER_HOLDS, b"set", "member")
            .is_ok());
    }

    #[test]
    fn read_only_txn_empty_set_scans() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 0, 100_000);
        assert!(!txn
            .has_live_member(store_keys::CF_OWNER_HOLDS, b"empty-set")
            .unwrap());
        assert!(txn
            .smembers_limited(store_keys::CF_OWNER_HOLDS, b"empty-set", 100)
            .unwrap()
            .is_empty());
        assert!(!txn
            .sismember(store_keys::CF_OWNER_HOLDS, b"empty-set", "member")
            .unwrap());
        let (members, next) = txn
            .smembers_page(store_keys::CF_OWNER_HOLDS, b"empty-set", None, 16)
            .unwrap();
        assert!(members.is_empty());
        assert!(next.is_none());
    }

    // --- set prefix isolation (upper bound correctness) ---

    #[test]
    fn set_scans_do_not_leak_into_adjacent_sets() {
        let (db, _dir) = open_test_db();
        {
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice", "m1", 60_000)
                .unwrap();
            // "alice0" sorts immediately after the "alice\0" prefix range.
            txn.sadd(store_keys::CF_OWNER_HOLDS, b"alice0", "other", 60_000)
                .unwrap();
            txn.commit().unwrap();
        }
        let mut txn = RocksDbTxn::new(db, 0, 1_001);
        let members = txn
            .smembers_limited(store_keys::CF_OWNER_HOLDS, b"alice", 100)
            .unwrap();
        assert_eq!(members, vec!["m1".to_string()]);
    }

    // --- dump ---

    #[test]
    fn dump_owner_holds_lists_live_locks_with_fences() {
        let (db, _dir) = open_test_db();
        {
            // Mirror the engine's layout: the hold-set key is own_prefix(owner).
            let alice = store_keys::own_prefix("alice");
            let ghost = store_keys::own_prefix("ghost");
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            txn.set_str(
                store_keys::CF_OWNER_ALIVE,
                &store_keys::alive_key("alice"),
                "1",
                60_000,
            )
            .unwrap();
            txn.sadd(store_keys::CF_OWNER_HOLDS, &alice, "write:h:/a", 60_000)
                .unwrap();
            txn.sadd(store_keys::CF_OWNER_HOLDS, &alice, "read:h:/b", 60_000)
                .unwrap();
            txn.set_str(
                store_keys::CF_FENCES,
                &store_keys::fence_key("h:/a"),
                "7",
                60_000,
            )
            .unwrap();
            // A dead owner's residue must not be reported.
            txn.sadd(store_keys::CF_OWNER_HOLDS, &ghost, "write:h:/g", 60_000)
                .unwrap();
            txn.commit().unwrap();
        }
        let page = dump_owner_holds(&db, 0, 2_000, None, 64).unwrap();
        assert!(page.next_cursor.is_none());
        assert_eq!(page.entries.len(), 2);
        let write_entry = page
            .entries
            .iter()
            .find(|e| e.path == "h:/a")
            .expect("write entry");
        assert_eq!(write_entry.owner, "alice");
        assert_eq!(write_entry.fence, Some(7));
    }

    #[test]
    fn dump_owner_holds_paginates() {
        let (db, _dir) = open_test_db();
        {
            let alice = store_keys::own_prefix("alice");
            let mut txn = WriteTxn::new(db.clone(), 0, 1_000);
            txn.set_str(
                store_keys::CF_OWNER_ALIVE,
                &store_keys::alive_key("alice"),
                "1",
                60_000,
            )
            .unwrap();
            for i in 0..5 {
                txn.sadd(
                    store_keys::CF_OWNER_HOLDS,
                    &alice,
                    &format!("read:h:/p{i}"),
                    60_000,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        let first = dump_owner_holds(&db, 0, 2_000, None, 2).unwrap();
        assert_eq!(first.entries.len(), 2);
        let cursor = first.next_cursor.expect("more pages");
        let rest = dump_owner_holds(&db, 0, 2_000, Some(cursor), 64).unwrap();
        assert_eq!(rest.entries.len(), 3);
        assert!(rest.next_cursor.is_none());
    }

    // --- group isolation ---

    #[test]
    fn groups_share_one_db_without_leaking() {
        let (db, _dir) = open_test_db();
        for group in [0u32, 1, 2] {
            let mut txn = WriteTxn::new(db.clone(), group, 1_000);
            txn.set_str(
                store_keys::CF_WRITE_LOCKS,
                b"h:/same-key",
                &format!("owner-{group}"),
                60_000,
            )
            .unwrap();
            txn.sadd(
                store_keys::CF_OWNER_HOLDS,
                b"alice\0",
                "write:h:/same-key",
                60_000,
            )
            .unwrap();
            txn.commit().unwrap();
        }
        for group in [0u32, 1, 2] {
            let mut txn = RocksDbTxn::new(db.clone(), group, 1_001);
            assert_eq!(
                txn.get_str(store_keys::CF_WRITE_LOCKS, b"h:/same-key")
                    .unwrap(),
                Some(format!("owner-{group}")),
                "each group reads its own record"
            );
            let members = txn
                .smembers_limited(store_keys::CF_OWNER_HOLDS, b"alice\0", 16)
                .unwrap();
            assert_eq!(members.len(), 1, "set scans stay within the group");
        }
    }

    #[test]
    fn destroy_group_removes_only_that_group() {
        let (db, _dir) = open_test_db();
        for group in [0u32, 1] {
            let mut txn = WriteTxn::new(db.clone(), group, 1_000);
            txn.set_str(store_keys::CF_WRITE_LOCKS, b"h:/a", "o", 60_000)
                .unwrap();
            txn.commit().unwrap();
        }
        destroy_group(&db, 1).unwrap();
        let mut g0 = RocksDbTxn::new(db.clone(), 0, 1_001);
        assert!(g0
            .get_str(store_keys::CF_WRITE_LOCKS, b"h:/a")
            .unwrap()
            .is_some());
        let mut g1 = RocksDbTxn::new(db.clone(), 1, 1_001);
        assert!(g1
            .get_str(store_keys::CF_WRITE_LOCKS, b"h:/a")
            .unwrap()
            .is_none());
        assert!(destroy_group(&db, SYS_GROUP).is_err());
    }
}
