//! Key encoding, path/domain helpers, and column family names for the
//! RocksDB-backed storage layer.
//!
//! ## Column families
//!
//! Set-valued state (read locks, owner holds, descendant indexes) is stored
//! one member per RocksDB key as `set_key \0 member`, so the per-path/per-owner
//! "set" is the contiguous key range under `set_key \0`.
//!
//! Every key below additionally carries a 4-byte big-endian **group prefix**
//! (see `group_key`): one node hosts replicas of many Raft groups in a single
//! RocksDB, and each group owns a contiguous, range-deletable keyspace. The
//! transaction layer applies the prefix; engine-level code only ever sees
//! group-relative keys.
//!
//! ```text
//! cf:default          catches any key not routed to a specific CF (safety net)
//! cf:meta             fence counter, gc cursor, raft vote/membership/applied
//! cf:raft_log         be_u32(group) ++ be_u64(index) -> log entry
//! cf:write_locks      path -> owner record
//! cf:read_locks       path\0 \0owner -> member record
//! cf:fences           path -> fencing token record
//! cf:claims           path -> claimant record
//! cf:desc_write       ancestor\0 \0path -> member record
//! cf:desc_read        ancestor\0 \0path -> member record
//! cf:desc_claim       ancestor\0 \0path -> member record
//! cf:owner_alive      owner -> liveness record
//! cf:owner_holds      owner\0 \0mode:path -> member record
//! cf:wait_edges       owner -> wait edge record
//! cf:expiry           be_u64(expires_at)\0cf_name\0primary_key -> expiry record
//! cf:request_dedupe   request_id -> cached ApplyResponse record
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use crate::cluster::placement::GroupId;

pub const FENCE_MIN_TTL_MS: u64 = 86_400_000;
pub const MAX_SET_ENUM_MEMBERS: usize = 65_536;

/// Expiry-index timestamps for long leases are rounded up to this quantum so
/// that refreshing the same record (e.g. a fence renewed every heartbeat with
/// a one-day TTL) overwrites a single index key instead of accreting a fresh
/// index row — and its eventual tombstone — per refresh.
pub const EXPIRY_INDEX_QUANTUM_MS: u64 = 3_600_000;

// --- meta column family keys (group-relative; the transaction layer scopes
// --- them to a group, see `group_key`) ---

pub const META_FENCE_COUNTER_KEY: &[u8] = b"fence_counter";
pub const META_GC_CURSOR_KEY: &[u8] = b"gc_cursor";
pub const META_LAST_NOW_KEY: &[u8] = b"last_now_ms";
pub const META_VOTE_KEY: &[u8] = b"raft_vote";
pub const META_COMMITTED_KEY: &[u8] = b"raft_committed";
pub const META_LAST_APPLIED_KEY: &[u8] = b"raft_last_applied";
pub const META_MEMBERSHIP_KEY: &[u8] = b"raft_membership";
pub const META_PURGED_KEY: &[u8] = b"raft_purged";
pub const META_SNAPSHOT_META_KEY: &[u8] = b"raft_snapshot_meta";
pub const META_SNAPSHOT_DATA_KEY: &[u8] = b"raft_snapshot_data";

// --- group scoping ---
//
// Every node hosts replicas of many Raft groups in ONE shared RocksDB. All
// keys in every state CF (and the raft log / per-group meta) carry a fixed
// 4-byte big-endian group prefix so each group owns a contiguous, range-
// deletable keyspace.

/// The 4-byte key prefix of a group's keyspace.
pub fn group_prefix(group: GroupId) -> [u8; 4] {
    group.to_be_bytes()
}

/// Scope a group-relative key into the group's keyspace.
pub fn group_key(group: GroupId, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len());
    buf.extend_from_slice(&group.to_be_bytes());
    buf.extend_from_slice(key);
    buf
}

/// The half-open key range `[start, end)` covering a group's entire keyspace
/// within one column family. `end` is `None` for the last possible group.
pub fn group_range(group: GroupId) -> (Vec<u8>, Option<Vec<u8>>) {
    let start = group.to_be_bytes().to_vec();
    let end = group.checked_add(1).map(|g| g.to_be_bytes().to_vec());
    (start, end)
}

// --- column family names ---

pub const CF_META: &str = "meta";
pub const CF_RAFT_LOG: &str = "raft_log";
pub const CF_WRITE_LOCKS: &str = "write_locks";
pub const CF_READ_LOCKS: &str = "read_locks";
pub const CF_FENCES: &str = "fences";
pub const CF_CLAIMS: &str = "claims";
pub const CF_DESC_WRITE: &str = "desc_write";
pub const CF_DESC_READ: &str = "desc_read";
pub const CF_DESC_CLAIM: &str = "desc_claim";
pub const CF_OWNER_ALIVE: &str = "owner_alive";
pub const CF_OWNER_HOLDS: &str = "owner_holds";
pub const CF_WAIT_EDGES: &str = "wait_edges";
pub const CF_EXPIRY: &str = "expiry";
/// Request-id → cached ApplyResponse, so a command retried after an
/// ambiguous timeout (e.g. re-forwarded after a leader change) applies once.
pub const CF_DEDUPE: &str = "request_dedupe";

/// All state-machine column families (excluding CF_RAFT_LOG and CF_META which
/// are owned by openraft's storage wrappers).
pub const STATE_CFS: &[&str] = &[
    CF_WRITE_LOCKS,
    CF_READ_LOCKS,
    CF_FENCES,
    CF_CLAIMS,
    CF_DESC_WRITE,
    CF_DESC_READ,
    CF_DESC_CLAIM,
    CF_OWNER_ALIVE,
    CF_OWNER_HOLDS,
    CF_WAIT_EDGES,
    CF_EXPIRY,
    CF_DEDUPE,
];

/// All column families including raft internals.
pub const ALL_CFS: &[&str] = &[
    "default",
    CF_META,
    CF_RAFT_LOG,
    CF_WRITE_LOCKS,
    CF_READ_LOCKS,
    CF_FENCES,
    CF_CLAIMS,
    CF_DESC_WRITE,
    CF_DESC_READ,
    CF_DESC_CLAIM,
    CF_OWNER_ALIVE,
    CF_OWNER_HOLDS,
    CF_WAIT_EDGES,
    CF_EXPIRY,
    CF_DEDUPE,
];

// --- key encoding ---

/// Encode a write-lock key: `path`
pub fn wr_key(path: &str) -> Vec<u8> {
    path.as_bytes().to_vec()
}

/// Set key under which a path's read-lock owners are stored.
pub fn rd_prefix(path: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(path.len() + 1);
    buf.extend_from_slice(path.as_bytes());
    buf.push(0);
    buf
}

/// Encode a fence key: `path`
pub fn fence_key(path: &str) -> Vec<u8> {
    path.as_bytes().to_vec()
}

/// Encode an owner alive key: `owner`
pub fn alive_key(owner: &str) -> Vec<u8> {
    owner.as_bytes().to_vec()
}

/// Set key under which an owner's held `mode:path` members are stored.
pub fn own_prefix(owner: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(owner.len() + 1);
    buf.extend_from_slice(owner.as_bytes());
    buf.push(0);
    buf
}

/// Encode a wait edge key: `owner`
pub fn wait_key(owner: &str) -> Vec<u8> {
    owner.as_bytes().to_vec()
}

/// Encode a claim key: `path`
pub fn claim_key(path: &str) -> Vec<u8> {
    path.as_bytes().to_vec()
}

/// Encode a write-descendant index key: `ancestor:NUL:path`
pub fn wrdesc_key(anc: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(anc.len() + 1);
    buf.extend_from_slice(anc.as_bytes());
    buf.push(0);
    buf
}

/// Prefix for all read-descendant entries under an ancestor.
pub fn rddesc_prefix(anc: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(anc.len() + 1);
    buf.extend_from_slice(anc.as_bytes());
    buf.push(0);
    buf
}

/// Encode a claim-descendant index key: `ancestor:NUL:path`
pub fn claimdesc_key(anc: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(anc.len() + 1);
    buf.extend_from_slice(anc.as_bytes());
    buf.push(0);
    buf
}

/// Encode an expiry index key: `be_u64(expires_at):NUL:kind:NUL:primary_key_bytes`
pub fn expiry_key(expires_at: u64, kind: &str, primary_key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 1 + kind.len() + 1 + primary_key.len());
    buf.extend_from_slice(&expires_at.to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(kind.as_bytes());
    buf.push(0);
    buf.extend_from_slice(primary_key);
    buf
}

/// Prefix for scanning all expiry entries up to a given `max_expires_at`.
pub fn expiry_scan_upper(max_expires_at: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 1);
    buf.extend_from_slice(&max_expires_at.to_be_bytes());
    buf.push(0);
    buf.push(0); // one past the NUL separator
    buf
}

/// Decode the expiry timestamp from an expiry index key.
pub fn decode_expiry_key_exp(key: &[u8]) -> Option<u64> {
    if key.len() < 8 {
        return None;
    }
    Some(u64::from_be_bytes([
        key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
    ]))
}

/// Decode an expiry index key into `(expires_at, cf_name, primary_key)`.
///
/// The inverse of [`expiry_key`]. `primary_key` may itself contain NUL bytes
/// (set-member keys do), so only the first NUL after the timestamp is treated
/// as the `kind`/cf separator.
pub fn decode_expiry_entry(key: &[u8]) -> Option<(u64, &str, &[u8])> {
    // 8-byte big-endian timestamp + NUL + kind + NUL + primary_key
    if key.len() < 9 {
        return None;
    }
    let exp = u64::from_be_bytes([
        key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
    ]);
    let rest = &key[9..];
    let nul = rest.iter().position(|&b| b == 0)?;
    let cf = std::str::from_utf8(&rest[..nul]).ok()?;
    let primary_key = &rest[nul + 1..];
    Some((exp, cf, primary_key))
}

/// The handler segment of a path `"<handler>:<path>"` (everything before the
/// first `:`); the whole string if there is no `:`.
pub fn handler_of(path: &str) -> &str {
    match path.find(':') {
        Some(i) => &path[..i],
        None => path,
    }
}

/// The lock domain of a path (the segment before the first `:`), used as the
/// sharding key for Raft group placement.
pub fn lock_domain(path: &str) -> &str {
    handler_of(path)
}

// --- time helpers ---

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[inline]
pub fn expired(exp: u64, now: u64) -> bool {
    exp != 0 && now >= exp
}

#[inline]
pub fn expiry_at(now: u64, ttl_ms: u64) -> u64 {
    if ttl_ms == 0 {
        0
    } else {
        now.saturating_add(ttl_ms)
    }
}

/// Timestamp under which a record's expiry-index entry is filed.
///
/// Short leases index at their exact expiry. Long leases (≥ one quantum) round
/// up to the next quantum boundary so repeated refreshes of the same record
/// reuse one index key. The index may therefore fire *late*, never early; the
/// GC sweep re-checks the record's own expiry before reclaiming it, and
/// logical expiry (`expired`) is enforced on read regardless.
pub fn quantized_index_expiry(now_ms: u64, exp: u64) -> u64 {
    if exp.saturating_sub(now_ms) < EXPIRY_INDEX_QUANTUM_MS {
        exp
    } else {
        exp.div_ceil(EXPIRY_INDEX_QUANTUM_MS)
            .saturating_mul(EXPIRY_INDEX_QUANTUM_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- time helpers ---

    #[test]
    fn expired_handles_zero_and_boundaries() {
        assert!(!expired(0, u64::MAX)); // 0 = never expires
        assert!(!expired(100, 99));
        assert!(expired(100, 100));
        assert!(expired(100, 101));
    }

    #[test]
    fn expiry_at_saturates_and_handles_zero_ttl() {
        assert_eq!(expiry_at(10, 0), 0);
        assert_eq!(expiry_at(10, 5), 15);
        assert_eq!(expiry_at(u64::MAX, 5), u64::MAX);
        assert_eq!(expiry_at(500, 1000), 1500);
    }

    // --- handler / domain extraction ---

    #[test]
    fn handler_of_extracts_prefix() {
        assert_eq!(handler_of("google_drive:/a/b"), "google_drive");
        assert_eq!(handler_of("local:/x"), "local");
        assert_eq!(handler_of("nocolon"), "nocolon");
        assert_eq!(handler_of(":/leading"), "");
        assert_eq!(handler_of("s3:/bucket/object"), "s3");
    }

    #[test]
    fn lock_domain_equals_handler() {
        assert_eq!(lock_domain("vol_9:/a/b"), "vol_9");
        assert_eq!(lock_domain("workspace_123:/jobs/10"), "workspace_123");
    }

    // --- key encoding round-trips ---

    #[test]
    fn wr_key_encodes_path_as_bytes() {
        let key = wr_key("h:/a");
        assert_eq!(key, b"h:/a");
    }

    #[test]
    fn rd_prefix_is_path_plus_nul() {
        let prefix = rd_prefix("h:/a");
        assert_eq!(prefix, b"h:/a\x00");
    }

    #[test]
    fn alive_key_is_owner_bytes() {
        assert_eq!(alive_key("owner-42"), b"owner-42");
    }

    #[test]
    fn wait_key_is_owner_bytes() {
        assert_eq!(wait_key("owner-42"), b"owner-42");
    }

    #[test]
    fn fence_key_is_path_bytes() {
        assert_eq!(fence_key("h:/a"), b"h:/a");
    }

    #[test]
    fn claim_key_is_path_bytes() {
        assert_eq!(claim_key("h:/a"), b"h:/a");
    }

    #[test]
    fn own_prefix_is_owner_plus_nul() {
        assert_eq!(own_prefix("alice"), b"alice\x00");
    }

    #[test]
    fn quantized_index_expiry_rounds_long_leases_up() {
        let q = EXPIRY_INDEX_QUANTUM_MS;
        // Short leases keep their exact expiry.
        assert_eq!(quantized_index_expiry(1_000, 1_000 + q - 1), 1_000 + q - 1);
        // Long leases round up to the next quantum boundary.
        assert_eq!(quantized_index_expiry(1_000, q + 1_000), 2 * q);
        assert_eq!(quantized_index_expiry(0, 3 * q), 3 * q);
        // Never rounds down: quantized >= exp always.
        for exp in [q, q + 1, 2 * q - 1, 2 * q] {
            assert!(quantized_index_expiry(0, exp) >= exp);
        }
    }

    #[test]
    fn wrdesc_key_is_anc_nul_prefix() {
        let key = wrdesc_key("h:/a");
        assert_eq!(key, b"h:/a\x00");
    }

    #[test]
    fn rddesc_prefix_is_anc_plus_nul() {
        let prefix = rddesc_prefix("h:/a");
        assert_eq!(prefix, b"h:/a\x00");
    }

    #[test]
    fn claimdesc_key_is_anc_nul_prefix() {
        let key = claimdesc_key("h:/a");
        assert_eq!(key, b"h:/a\x00");
    }

    // --- expiry key encoding ---

    #[test]
    fn expiry_key_starts_with_be64_timestamp() {
        let key = expiry_key(12345, "write_locks", b"h:/a");
        assert_eq!(&key[0..8], &12345u64.to_be_bytes());
        // Contains kind separator
        assert!(key[8..].starts_with(b"\x00write_locks\x00"));
    }

    #[test]
    fn decode_expiry_key_extracts_timestamp() {
        let key = expiry_key(99999, "claims", b"some-key");
        let ts = decode_expiry_key_exp(&key).unwrap();
        assert_eq!(ts, 99999);
    }

    #[test]
    fn decode_expiry_key_rejects_too_short() {
        assert!(decode_expiry_key_exp(&[1, 2, 3]).is_none());
    }

    #[test]
    fn decode_expiry_entry_round_trips() {
        // primary key with no NUL (plain record key)
        let key = expiry_key(12345, "write_locks", b"h:/a");
        let (exp, cf, pk) = decode_expiry_entry(&key).unwrap();
        assert_eq!(exp, 12345);
        assert_eq!(cf, "write_locks");
        assert_eq!(pk, b"h:/a");

        // primary key containing NUL bytes (set-member key) must survive intact
        let member_key = b"h:/a\x00\x00alice".to_vec();
        let key = expiry_key(777, "read_locks", &member_key);
        let (exp, cf, pk) = decode_expiry_entry(&key).unwrap();
        assert_eq!(exp, 777);
        assert_eq!(cf, "read_locks");
        assert_eq!(pk, &member_key[..]);
    }

    #[test]
    fn decode_expiry_entry_rejects_too_short() {
        assert!(decode_expiry_entry(&[1, 2, 3]).is_none());
    }

    #[test]
    fn expiry_scan_upper_produces_sane_bound() {
        let upper = expiry_scan_upper(50000);
        let ts_part = u64::from_be_bytes([
            upper[0], upper[1], upper[2], upper[3], upper[4], upper[5], upper[6], upper[7],
        ]);
        assert_eq!(ts_part, 50000);
        assert_eq!(&upper[8..10], b"\x00\x00");
    }

    // --- constants ---

    #[test]
    fn all_cfs_contains_expected_names() {
        let names: std::collections::HashSet<&str> = ALL_CFS.iter().copied().collect();
        assert!(names.contains("write_locks"));
        assert!(names.contains("read_locks"));
        assert!(names.contains("fences"));
        assert!(names.contains("claims"));
        assert!(names.contains("owner_alive"));
        assert!(names.contains("owner_holds"));
        assert!(names.contains("wait_edges"));
        assert!(names.contains("expiry"));
        assert!(names.contains("meta"));
        assert!(names.contains("raft_log"));
    }

    #[test]
    fn state_cfs_does_not_contain_raft_internals() {
        let names: std::collections::HashSet<&str> = STATE_CFS.iter().copied().collect();
        assert!(!names.contains("meta"));
        assert!(!names.contains("raft_log"));
        assert!(!names.contains("default"));
    }

    // --- now_ms monotonic-ish ---

    #[test]
    fn now_ms_returns_reasonable_value() {
        let t = now_ms();
        assert!(t > 1_700_000_000_000); // year 2023+
        assert!(t < 10_000_000_000_000); // before year 2286
    }
}
