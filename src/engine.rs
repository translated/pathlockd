//! The lock engine — atomic primitives, implemented generically over [`StoreTxn`].
//!
//! Each engine function is a deterministic inner function that takes a generic
//! `StoreTxn` implementation. The Raft state machine calls these directly during
//! apply. The service layer builds Raft commands; the router sends them to the
//! correct group leader.
//!
//! Conflict precedence, dead-owner pruning, fencing rules and TTL refreshes are
//! all enforced here, inside a single `StoreTxn` call per operation.
//!
//! All engine functions are synchronous because the underlying RocksDB
//! operations are inherently sync. The Raft state machine's apply is also sync.

use std::str::FromStr;

use tracing::warn;

use crate::store_keys::{
    alive_key, fence_key, hold_algorithm_key, namespace_policy_key, own_prefix, rd_prefix,
    rddesc_prefix, revoke_key, sem_prefix, semaphore_permits_key, wait_key, wr_key, wrdesc_key,
    FENCE_MIN_TTL_MS, MAX_SET_ENUM_MEMBERS,
};
use crate::store_rocksdb::StoreTxn;

use crate::store_keys::CF_DESC_READ as RDDESC_CF;
use crate::store_keys::CF_DESC_WRITE as WRDESC_CF;
use crate::store_keys::CF_FENCES as FENCE_CF;
use crate::store_keys::CF_META as META_CF;
use crate::store_keys::CF_NAMESPACE_SETTINGS as NS_SETTINGS_CF;
use crate::store_keys::CF_OWNER_ALIVE as ALIVE_CF;
use crate::store_keys::CF_OWNER_HOLDS as OWN_CF;
use crate::store_keys::CF_READ_LOCKS as RD_CF;
use crate::store_keys::CF_SEMAPHORE as SEM_CF;
use crate::store_keys::CF_WAIT_EDGES as WAIT_CF;
use crate::store_keys::CF_WRITE_LOCKS as WR_CF;

const SCAN_WARN_THRESHOLD: usize = 1024;

/// Page size for owner-wide cleanup scans (`release_all`, `force_release`).
const RELEASE_PAGE: usize = 4096;
/// Absolute safety valve on members processed by one owner-wide cleanup
/// command. Cleanup past this point is left to TTL expiry + GC; the owner's
/// liveness marker is still removed, so the residue stops blocking anyone.
///
/// Kept moderate on purpose: one command's deletions (plus per-member
/// descendant-index removals) accumulate in a single WriteBatch + overlay
/// held in memory and applied synchronously by every replica's apply loop.
const MAX_RELEASE_MEMBERS: usize = 1 << 16;
const SCOPED_PATH_SEP: char = '\x1f';
// ---------------------------------------------------------------------------
// Public value types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    Write,
    Read,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Write => "write",
            Mode::Read => "read",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LockAlgorithm {
    /// Default: multiple point reads on a path; writes exclude the path and
    /// descendants.
    #[default]
    RecursiveRw,
    /// Multiple point reads on a path; writes exclude only the exact path.
    PointRw,
    /// Write locks only; writes exclude the path and descendants.
    RecursiveWrite,
    /// Write locks only; writes exclude only the exact path.
    PointWrite,
    /// Counting semaphore: point-scoped, write-only. Each path has its own
    /// permit capacity; no read mode, no descendant exclusion, no fencing.
    Semaphore,
}

#[derive(Debug, Clone, Copy)]
struct AlgorithmStrategy {
    allows_read: bool,
    recursive: bool,
    semaphore: bool,
}

const ALGORITHM_STRATEGIES: &[(LockAlgorithm, AlgorithmStrategy)] = &[
    (
        LockAlgorithm::RecursiveRw,
        AlgorithmStrategy {
            allows_read: true,
            recursive: true,
            semaphore: false,
        },
    ),
    (
        LockAlgorithm::PointRw,
        AlgorithmStrategy {
            allows_read: true,
            recursive: false,
            semaphore: false,
        },
    ),
    (
        LockAlgorithm::RecursiveWrite,
        AlgorithmStrategy {
            allows_read: false,
            recursive: true,
            semaphore: false,
        },
    ),
    (
        LockAlgorithm::PointWrite,
        AlgorithmStrategy {
            allows_read: false,
            recursive: false,
            semaphore: false,
        },
    ),
    (
        LockAlgorithm::Semaphore,
        AlgorithmStrategy {
            allows_read: false,
            recursive: false,
            semaphore: true,
        },
    ),
];

fn algorithm_strategy(algorithm: LockAlgorithm) -> AlgorithmStrategy {
    ALGORITHM_STRATEGIES
        .iter()
        .find_map(|(candidate, strategy)| (*candidate == algorithm).then_some(*strategy))
        .expect("all lock algorithms have a strategy")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Reason {
    AncestorLocked,
    WriteLocked,
    ReadLocked,
    DescendantWriteLocked,
    DescendantReadLocked,
    ReadLocksDisabled,
    StaleFencingToken,
    InvalidPermits,
    SemaphoreFull,
    MissingSemaphore,
    MissingWrite,
    MissingRead,
    MissingFence,
    MissingAlive,
    MissingOwnerSet,
    EmptyOwnerSet,
    Queued,
    StaleOwner,
}

pub type ConflictReason = Reason;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct OwnerId(String);

impl OwnerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for OwnerId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for OwnerId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for OwnerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Namespace(String);

impl Namespace {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Namespace {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Namespace {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NormalizedPath(String);

impl NormalizedPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<&str> for NormalizedPath {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NormalizedPath {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for NormalizedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct FenceToken(i64);

impl FenceToken {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AncestorLocked => "ancestor_locked",
            Self::WriteLocked => "write_locked",
            Self::ReadLocked => "read_locked",
            Self::DescendantWriteLocked => "descendant_write_locked",
            Self::DescendantReadLocked => "descendant_read_locked",
            Self::ReadLocksDisabled => "read_locks_disabled",
            Self::StaleFencingToken => "stale_fencing_token",
            Self::InvalidPermits => "invalid_permits",
            Self::SemaphoreFull => "semaphore_full",
            Self::MissingSemaphore => "missing_semaphore",
            Self::MissingWrite => "missing_write",
            Self::MissingRead => "missing_read",
            Self::MissingFence => "missing_fence",
            Self::MissingAlive => "missing_alive",
            Self::MissingOwnerSet => "missing_owner_set",
            Self::EmptyOwnerSet => "empty_owner_set",
            Self::Queued => "queued",
            Self::StaleOwner => "stale_owner",
        }
    }

    pub fn is_queueable(self) -> bool {
        matches!(
            self,
            Self::WriteLocked
                | Self::ReadLocked
                | Self::AncestorLocked
                | Self::DescendantWriteLocked
                | Self::DescendantReadLocked
                | Self::SemaphoreFull
        )
    }

    pub fn is_blocking_reason(self) -> bool {
        matches!(
            self,
            Self::AncestorLocked
                | Self::WriteLocked
                | Self::ReadLocked
                | Self::DescendantWriteLocked
                | Self::DescendantReadLocked
                | Self::SemaphoreFull
        )
    }
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Reason {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim() {
            "ancestor_locked" => Ok(Self::AncestorLocked),
            "write_locked" => Ok(Self::WriteLocked),
            "read_locked" => Ok(Self::ReadLocked),
            "descendant_write_locked" => Ok(Self::DescendantWriteLocked),
            "descendant_read_locked" => Ok(Self::DescendantReadLocked),
            "read_locks_disabled" => Ok(Self::ReadLocksDisabled),
            "stale_fencing_token" => Ok(Self::StaleFencingToken),
            "invalid_permits" => Ok(Self::InvalidPermits),
            "semaphore_full" => Ok(Self::SemaphoreFull),
            "missing_semaphore" => Ok(Self::MissingSemaphore),
            "missing_write" => Ok(Self::MissingWrite),
            "missing_read" => Ok(Self::MissingRead),
            "missing_fence" => Ok(Self::MissingFence),
            "missing_alive" => Ok(Self::MissingAlive),
            "missing_owner_set" => Ok(Self::MissingOwnerSet),
            "empty_owner_set" => Ok(Self::EmptyOwnerSet),
            "queued" => Ok(Self::Queued),
            "stale_owner" => Ok(Self::StaleOwner),
            _ => anyhow::bail!("unknown reason {raw:?}"),
        }
    }
}

impl LockAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RecursiveRw => "recursive_rw",
            Self::PointRw => "point_rw",
            Self::RecursiveWrite => "recursive_write",
            Self::PointWrite => "point_write",
            Self::Semaphore => "semaphore",
        }
    }

    pub fn allows_read(self) -> bool {
        algorithm_strategy(self).allows_read
    }

    pub fn recursive(self) -> bool {
        algorithm_strategy(self).recursive
    }

    /// A counting semaphore rather than an exclusive lock.
    pub fn is_semaphore(self) -> bool {
        algorithm_strategy(self).semaphore
    }

    pub fn allows_mode(self, mode: Mode) -> bool {
        mode == Mode::Write || self.allows_read()
    }

    pub fn variants() -> &'static [&'static str] {
        &[
            "recursive_rw",
            "point_rw",
            "recursive_write",
            "point_write",
            "semaphore",
        ]
    }
}

impl std::fmt::Display for LockAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LockAlgorithm {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let normalized = raw.trim().to_ascii_lowercase().replace(['-', ' '], "_");
        match normalized.as_str() {
            "recursive_rw" => Ok(Self::RecursiveRw),
            "point_rw" => Ok(Self::PointRw),
            "recursive_write" => Ok(Self::RecursiveWrite),
            "point_write" => Ok(Self::PointWrite),
            "semaphore" => Ok(Self::Semaphore),
            _ => anyhow::bail!(
                "unknown lock algorithm {raw:?}; expected one of: {}",
                Self::variants().join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum State {
    New,
    Held,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockReq {
    pub path: String,
    pub mode: Mode,
    pub state: State,
    #[serde(default)]
    pub permits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RelReq {
    pub path: String,
    pub mode: Mode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcquireArgs {
    pub owner_id: String,
    pub ttl_ms: u64,
    pub requests: Vec<LockReq>,
    pub fencing_token: i64,
    pub release_requests: Vec<RelReq>,
    /// If this acquire is queued, how long its wait-queue entry lives without
    /// being granted — the client's own acquire deadline. `0` selects a server
    /// default. Bounds an abandoned waiter so it self-evicts at the client's
    /// threshold instead of lingering on a fixed server TTL.
    pub queue_ttl_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AcquireOutcome {
    Ok {
        fencing_token: i64,
    },
    Conflict {
        path: String,
        owner: String,
        reason: Reason,
    },
    Lost {
        path: String,
        reason: Reason,
    },
    /// Like `Conflict`, but the request was *enqueued* in the wait queue rather
    /// than refused: it will be granted in place once the contended path frees.
    /// Carries the same `(path, owner, reason)` as the conflict that parked it,
    /// so the wire response is identical to a conflict for clients that have not
    /// yet adopted grant events. The engine never returns this — it is produced
    /// by the apply-layer queue wiring in `state_machine`.
    Queued {
        path: String,
        owner: String,
        reason: Reason,
        fencing_token: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RenewOutcome {
    /// The lease was extended. `revoke_requested` is true when a cooperative
    /// revoke is pending for this owner — the holder should finish its current
    /// work and release. It rides the existing heartbeat so a poll-only client
    /// (no event stream) still learns it has been asked to yield.
    Ok {
        revoke_requested: bool,
    },
    Lost {
        path: String,
        reason: Reason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AssertOutcome {
    Ok,
    Fail { path: String, reason: Reason },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CycleOutcome {
    None,
    Cycle(Vec<String>),
    Truncated(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WaitEdgeMetadata {
    pub conflict_path: String,
    pub reason: Reason,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WaitEdge {
    pub conflict_owner: String,
    pub metadata: Option<WaitEdgeMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NamespacePolicyEntry {
    pub namespace: String,
    pub algorithm: LockAlgorithm,
    pub epoch: u64,
}

impl NamespacePolicyEntry {
    pub fn policy(&self) -> LockPolicy {
        LockPolicy::new(self.algorithm, self.epoch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockPolicy {
    pub algorithm: LockAlgorithm,
    pub epoch: u64,
}

impl LockPolicy {
    pub fn new(algorithm: LockAlgorithm, epoch: u64) -> Self {
        Self { algorithm, epoch }
    }

    pub fn from_algorithm(algorithm: LockAlgorithm) -> Self {
        Self::new(algorithm, 0)
    }
}

impl Default for LockPolicy {
    fn default() -> Self {
        Self::from_algorithm(LockAlgorithm::default())
    }
}

impl From<LockAlgorithm> for LockPolicy {
    fn from(value: LockAlgorithm) -> Self {
        Self::from_algorithm(value)
    }
}

const WAIT_EDGE_V1_PREFIX: &str = "v1:";

// ---------------------------------------------------------------------------
// Namespace-scoped paths / owner-hold members
// ---------------------------------------------------------------------------

pub fn scoped_path(namespace: &str, path: &str) -> String {
    let relative = relative_path(namespace, path);
    format!("{namespace}{SCOPED_PATH_SEP}{relative}")
}

pub fn public_path(namespace: &str, path: &str) -> String {
    let Some((_ns, rel)) = path.split_once(SCOPED_PATH_SEP) else {
        return path.to_string();
    };
    if rel == "/" {
        return if namespace.contains(':') {
            namespace.to_string()
        } else {
            format!("{namespace}:/")
        };
    }
    if namespace.contains(':') {
        if namespace.ends_with(":/") {
            format!("{}{}", namespace, rel.trim_start_matches('/'))
        } else {
            format!("{namespace}{rel}")
        }
    } else {
        format!("{namespace}:{rel}")
    }
}

fn relative_path(namespace: &str, path: &str) -> String {
    if !namespace.contains(':') {
        let Some(colon) = path.find(':') else {
            return path.to_string();
        };
        return path[colon + 1..].to_string();
    }
    if path == namespace {
        return "/".to_string();
    }
    if namespace.ends_with(":/") {
        let suffix = path.strip_prefix(namespace).unwrap_or_default();
        if suffix.is_empty() {
            "/".to_string()
        } else {
            format!("/{suffix}")
        }
    } else {
        path.strip_prefix(namespace)
            .filter(|rest| rest.starts_with('/'))
            .unwrap_or(path)
            .to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeldLock {
    pub namespace: Namespace,
    pub mode: Mode,
    pub path: NormalizedPath,
}

impl HeldLock {
    pub fn new(
        namespace: impl Into<Namespace>,
        mode: Mode,
        path: impl Into<NormalizedPath>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            mode,
            path: path.into(),
        }
    }

    pub fn member(&self) -> String {
        format!(
            "{}\0{}\0{}",
            self.mode.as_str(),
            self.namespace.as_str(),
            self.path.as_str()
        )
    }

    pub fn key_path(&self) -> String {
        scoped_path(self.namespace.as_str(), self.path.as_str())
    }

    pub fn parse_member(member: &str) -> Option<Self> {
        let mut parts = member.splitn(3, '\0');
        let mode = match parts.next()? {
            "write" => Mode::Write,
            "read" => Mode::Read,
            _ => return None,
        };
        Some(Self::new(parts.next()?, mode, parts.next()?))
    }
}

fn hold_member(namespace: &str, mode: Mode, path: &str) -> String {
    HeldLock::new(namespace, mode, path).member()
}

pub fn parse_hold_member(member: &str) -> Option<HeldLock> {
    HeldLock::parse_member(member)
}

fn held_key_path(held: &HeldLock) -> String {
    held.key_path()
}

// ---------------------------------------------------------------------------
// get_ancestors
// ---------------------------------------------------------------------------

pub fn get_ancestors(full_path: &str) -> Vec<String> {
    if let Some((namespace, relative)) = full_path.split_once(SCOPED_PATH_SEP) {
        let mut ancestors = Vec::new();
        let mut current = relative.to_string();
        while current != "/" && !current.is_empty() {
            match current.rfind('/') {
                None => break,
                Some(idx) => {
                    current = if idx == 0 {
                        "/".to_string()
                    } else {
                        current[..idx].to_string()
                    };
                    ancestors.push(format!("{namespace}{SCOPED_PATH_SEP}{current}"));
                    if current == "/" {
                        break;
                    }
                }
            }
        }
        return ancestors;
    }

    let mut ancestors = Vec::new();
    let col_idx = match full_path.find(':') {
        Some(i) => i,
        None => return ancestors,
    };
    let handler = &full_path[..=col_idx];
    let path = &full_path[col_idx + 1..];

    let mut current = path.to_string();
    while current != "/" && !current.is_empty() {
        match current.rfind('/') {
            None => break,
            Some(idx) => {
                current = if idx == 0 {
                    "/".to_string()
                } else {
                    current[..idx].to_string()
                };
                ancestors.push(format!("{handler}{current}"));
                if current == "/" {
                    break;
                }
            }
        }
    }
    ancestors
}

/// Whether `anc` is a strict ancestor of `desc` in the same namespace.
pub fn is_strict_ancestor(anc: &str, desc: &str) -> bool {
    get_ancestors(desc).iter().any(|a| a == anc)
}

/// Whether two requested locks cannot coexist when each uses its own policy.
pub fn locks_conflict(
    a_algorithm: LockAlgorithm,
    a_path: &str,
    a_mode: Mode,
    b_algorithm: LockAlgorithm,
    b_path: &str,
    b_mode: Mode,
) -> bool {
    if !a_algorithm.allows_mode(a_mode) || !b_algorithm.allows_mode(b_mode) {
        return false;
    }
    if a_path == b_path {
        return a_mode == Mode::Write || b_mode == Mode::Write;
    }
    if is_strict_ancestor(a_path, b_path) {
        return a_mode == Mode::Write && a_algorithm.recursive();
    }
    if is_strict_ancestor(b_path, a_path) {
        return b_mode == Mode::Write && b_algorithm.recursive();
    }
    false
}

// ---------------------------------------------------------------------------
// Shared helpers (all sync)
// ---------------------------------------------------------------------------

fn owner_alive<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<bool> {
    tx.get_str(ALIVE_CF, &alive_key(owner)).map(|v| v.is_some())
}

pub fn set_namespace_policy_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    algorithm: LockAlgorithm,
) -> anyhow::Result<u64> {
    let (current, explicit) =
        get_namespace_policy_record_inner(tx, namespace, LockAlgorithm::default())?;
    let policy = LockPolicy::new(algorithm, current.epoch);
    let next_epoch = if explicit && current.algorithm == policy.algorithm {
        current.epoch
    } else {
        current.epoch.saturating_add(1)
    };
    let next_policy = LockPolicy::new(algorithm, next_epoch);
    tx.set_str(
        NS_SETTINGS_CF,
        &namespace_policy_key(namespace),
        &format!("{next_epoch}:{}", next_policy.algorithm.as_str()),
        0,
    )?;
    Ok(next_epoch)
}

pub fn delete_namespace_policy_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
) -> anyhow::Result<()> {
    tx.del(NS_SETTINGS_CF, &namespace_policy_key(namespace))
}

/// Resolve a namespace's lock algorithm. `default` is the configured fallback
/// (`Config::default_lock_algorithm`) applied when no explicit row exists; it is
/// supplied by the caller rather than hardcoded so the cluster-wide default is
/// overridable. The `bool` is whether an explicit row was found.
pub fn get_namespace_policy_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    default: LockAlgorithm,
) -> anyhow::Result<(LockAlgorithm, bool)> {
    get_namespace_policy_record_inner(tx, namespace, default)
        .map(|(policy, explicit)| (policy.algorithm, explicit))
}

pub fn get_namespace_policy_record_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    default: LockAlgorithm,
) -> anyhow::Result<(LockPolicy, bool)> {
    match tx.get_str(NS_SETTINGS_CF, &namespace_policy_key(namespace))? {
        None => Ok((LockPolicy::from_algorithm(default), false)),
        Some(raw) => Ok((parse_namespace_policy_value(&raw)?, true)),
    }
}

pub fn parse_namespace_policy_value(raw: &str) -> anyhow::Result<LockPolicy> {
    let mut parts = raw.splitn(2, ':');
    let epoch = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("malformed namespace policy record"))?
        .parse::<u64>()?;
    let algorithm = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("malformed namespace policy record"))?
        .parse::<LockAlgorithm>()?;
    Ok(LockPolicy::new(algorithm, epoch))
}

fn hold_algorithm<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    mode: Mode,
    path: &str,
) -> anyhow::Result<LockAlgorithm> {
    match tx.get_str(META_CF, &hold_algorithm_key(owner, mode.as_str(), path))? {
        None => Ok(LockAlgorithm::default()),
        Some(raw) => raw.parse::<LockAlgorithm>(),
    }
}

fn set_hold_algorithm<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    mode: Mode,
    path: &str,
    algorithm: LockAlgorithm,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    tx.set_str(
        META_CF,
        &hold_algorithm_key(owner, mode.as_str(), path),
        algorithm.as_str(),
        ttl_ms,
    )
}

fn del_hold_algorithm<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    mode: Mode,
    path: &str,
) -> anyhow::Result<()> {
    tx.del(META_CF, &hold_algorithm_key(owner, mode.as_str(), path))
}

fn semaphore_capacity<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Option<u32>> {
    tx.get_str(META_CF, &semaphore_permits_key(path))?
        .map(|raw| raw.parse::<u32>())
        .transpose()
        .map_err(Into::into)
}

fn set_semaphore_capacity<T: StoreTxn>(tx: &mut T, path: &str, permits: u32) -> anyhow::Result<()> {
    tx.set_str(
        META_CF,
        &semaphore_permits_key(path),
        &permits.to_string(),
        0,
    )
}

fn prune_dead_read_owners<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Vec<String>> {
    let rd_pfx = rd_prefix(path);
    let owners = tx.smembers_limited(RD_CF, &rd_pfx, MAX_SET_ENUM_MEMBERS)?;
    let mut alive = Vec::new();
    for o in owners {
        if owner_alive(tx, &o)? {
            alive.push(o);
        } else {
            tx.srem(RD_CF, &rd_pfx, &o)?;
            del_hold_algorithm(tx, &o, Mode::Read, path)?;
        }
    }
    Ok(alive)
}

/// Live semaphore holders at `path`, dropping any whose owner is dead (and the
/// dead owner's per-hold algorithm marker). Semaphore holds are stored under
/// the engine's `Mode::Write` string, mirroring their wire mode.
fn prune_dead_semaphore_owners<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Vec<String>> {
    let pfx = sem_prefix(path);
    let owners = tx.smembers_limited(SEM_CF, &pfx, MAX_SET_ENUM_MEMBERS)?;
    let mut alive = Vec::new();
    for o in owners {
        if owner_alive(tx, &o)? {
            alive.push(o);
        } else {
            tx.srem(SEM_CF, &pfx, &o)?;
            del_hold_algorithm(tx, &o, Mode::Write, path)?;
        }
    }
    Ok(alive)
}

fn get_live_write_owner<T: StoreTxn>(tx: &mut T, path: &str) -> anyhow::Result<Option<String>> {
    let Some(owner) = tx.get_str(WR_CF, &wr_key(path))? else {
        return Ok(None);
    };
    if owner_alive(tx, &owner)? {
        return Ok(Some(owner));
    }
    tx.del(WR_CF, &wr_key(path))?;
    del_hold_algorithm(tx, &owner, Mode::Write, path)?;
    remove_descendant_indexes(tx, Mode::Write, path)?;
    Ok(None)
}

fn add_descendant_indexes<T: StoreTxn>(
    tx: &mut T,
    mode: Mode,
    path: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        if mode == Mode::Write {
            tx.sadd(WRDESC_CF, &wrdesc_key(&anc), path, ttl_ms)?;
        } else {
            // Keyed by ancestor (member = descendant path), symmetric with the
            // write index, so `find_descendant_read_conflict` can enumerate
            // descendants by scanning `rddesc_prefix(ancestor)`.
            tx.sadd(RDDESC_CF, &rddesc_prefix(&anc), path, ttl_ms)?;
        }
    }
    Ok(())
}

fn remove_descendant_indexes<T: StoreTxn>(
    tx: &mut T,
    mode: Mode,
    path: &str,
) -> anyhow::Result<()> {
    for anc in get_ancestors(path) {
        if mode == Mode::Write {
            tx.srem(WRDESC_CF, &wrdesc_key(&anc), path)?;
        } else {
            tx.srem(RDDESC_CF, &rddesc_prefix(&anc), path)?;
        }
    }
    Ok(())
}

fn find_descendant_write_conflict<T: StoreTxn>(
    tx: &mut T,
    owner_id: &str,
    path: &str,
    namespace: &str,
) -> anyhow::Result<Option<(String, String, Reason)>> {
    let idx = wrdesc_key(path);
    let candidates = tx.smembers_limited(WRDESC_CF, &idx, MAX_SET_ENUM_MEMBERS)?;
    if candidates.len() > SCAN_WARN_THRESHOLD {
        warn!(key = ?path, count = candidates.len(), "large wrdesc scan");
    }
    for candidate in candidates {
        match get_live_write_owner(tx, &candidate)? {
            None => {
                tx.srem(WRDESC_CF, &idx, &candidate)?;
                remove_descendant_indexes(tx, Mode::Write, &candidate)?;
            }
            Some(owner) if owner != owner_id => {
                return Ok(Some((
                    public_path(namespace, &candidate),
                    owner,
                    Reason::DescendantWriteLocked,
                )));
            }
            Some(_) => {}
        }
    }
    Ok(None)
}

fn find_descendant_read_conflict<T: StoreTxn>(
    tx: &mut T,
    owner_id: &str,
    path: &str,
    namespace: &str,
) -> anyhow::Result<Option<(String, String, Reason)>> {
    let idx_pfx = rddesc_prefix(path);
    let candidates = tx.smembers_limited(RDDESC_CF, &idx_pfx, MAX_SET_ENUM_MEMBERS)?;
    if candidates.len() > SCAN_WARN_THRESHOLD {
        warn!(key = ?path, count = candidates.len(), "large rddesc scan");
    }
    let mut seen = std::collections::HashSet::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        let owners = prune_dead_read_owners(tx, &candidate)?;
        if owners.is_empty() {
            tx.srem(RDDESC_CF, &idx_pfx, &candidate)?;
            remove_descendant_indexes(tx, Mode::Read, &candidate)?;
        } else {
            for owner in owners {
                if owner != owner_id {
                    return Ok(Some((
                        public_path(namespace, &candidate),
                        owner,
                        Reason::DescendantReadLocked,
                    )));
                }
            }
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// ACQUIRE
// ---------------------------------------------------------------------------

pub fn acquire_inner<T: StoreTxn>(
    tx: &mut T,
    args: &AcquireArgs,
) -> anyhow::Result<AcquireOutcome> {
    acquire_inner_with_policy(tx, args, LockAlgorithm::default())
}

pub fn acquire_inner_with_policy<T: StoreTxn>(
    tx: &mut T,
    args: &AcquireArgs,
    request_algorithm: LockAlgorithm,
) -> anyhow::Result<AcquireOutcome> {
    let namespace = args
        .requests
        .iter()
        .map(|r| crate::store_keys::handler_of(&r.path))
        .chain(
            args.release_requests
                .iter()
                .map(|r| crate::store_keys::handler_of(&r.path)),
        )
        .next()
        .unwrap_or_default()
        .to_string();
    acquire_inner_in_namespace(
        tx,
        args,
        LockPolicy::from_algorithm(request_algorithm),
        &namespace,
    )
}

pub fn acquire_inner_in_namespace<T: StoreTxn>(
    tx: &mut T,
    args: &AcquireArgs,
    policy: LockPolicy,
    namespace: &str,
) -> anyhow::Result<AcquireOutcome> {
    let owner = &args.owner_id;
    let ttl = args.ttl_ms;
    let fence_ttl = ttl.max(FENCE_MIN_TTL_MS);
    let token = args.fencing_token;
    let alive_k = alive_key(owner);
    let own_pfx = own_prefix(owner);

    if args.requests.is_empty() && args.release_requests.is_empty() {
        return Ok(AcquireOutcome::Ok { fencing_token: 0 });
    }

    let has_held = args.requests.iter().any(|r| r.state == State::Held);
    if has_held && tx.get_str(ALIVE_CF, &alive_k)?.is_none() {
        return Ok(AcquireOutcome::Lost {
            path: String::new(),
            reason: Reason::MissingAlive,
        });
    }

    if let Some(outcome) = validate_acquire(tx, owner, token, &args.requests, namespace, policy)? {
        return Ok(outcome);
    }

    let apply_ctx = AcquireApplyCtx {
        owner,
        namespace,
        request_algorithm: policy.algorithm,
        token,
        fence_ttl,
        own_pfx: &own_pfx,
    };
    for req in &args.requests {
        if let Some(outcome) = apply_acquire(tx, req, &apply_ctx)? {
            return Ok(outcome);
        }
    }

    refresh_portfolio(tx, &alive_k, ttl)?;

    if !args.release_requests.is_empty() {
        apply_releases(
            tx,
            owner,
            namespace,
            &args.release_requests,
            &own_pfx,
            &alive_k,
        )?;
    }

    Ok(AcquireOutcome::Ok {
        fencing_token: if token > 0 { token } else { 0 },
    })
}

fn validate_acquire<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    token: i64,
    requests: &[LockReq],
    namespace: &str,
    policy: LockPolicy,
) -> anyhow::Result<Option<AcquireOutcome>> {
    for req in requests {
        let outcome = match req.state {
            State::Held => validate_held(tx, owner, token, req, namespace)?,
            State::New => validate_new(tx, owner, token, req, namespace, policy)?,
        };
        if outcome.is_some() {
            return Ok(outcome);
        }
    }
    Ok(None)
}

fn validate_held<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    token: i64,
    req: &LockReq,
    namespace: &str,
) -> anyhow::Result<Option<AcquireOutcome>> {
    let path = &req.path;
    let key_path = scoped_path(namespace, path);
    let held_algorithm = hold_algorithm(tx, owner, req.mode, &key_path)?;
    if held_algorithm.is_semaphore() {
        if !tx.sismember(SEM_CF, &sem_prefix(&key_path), owner)? {
            return Ok(Some(lost(path, Reason::MissingSemaphore)));
        }
    } else if req.mode == Mode::Write {
        if tx.get_str(WR_CF, &wr_key(&key_path))?.as_deref() != Some(owner) {
            return Ok(Some(lost(path, Reason::MissingWrite)));
        }
        match parse_fence(tx.get_str(FENCE_CF, &fence_key(&key_path))?) {
            None => return Ok(Some(lost(path, Reason::MissingFence))),
            Some(cur) if cur > token => {
                return Ok(Some(conflict(
                    path,
                    &cur.to_string(),
                    Reason::StaleFencingToken,
                )));
            }
            Some(_) => {}
        }
    } else if !tx.sismember(RD_CF, &rd_prefix(&key_path), owner)? {
        return Ok(Some(lost(path, Reason::MissingRead)));
    }
    Ok(None)
}

fn validate_new<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    token: i64,
    req: &LockReq,
    namespace: &str,
    policy: LockPolicy,
) -> anyhow::Result<Option<AcquireOutcome>> {
    let path = &req.path;
    let key_path = scoped_path(namespace, path);
    let algorithm = policy.algorithm;
    if !algorithm.allows_mode(req.mode) {
        return Ok(Some(conflict(path, "", Reason::ReadLocksDisabled)));
    }
    if let Some(outcome) = validate_new_ancestors(tx, owner, namespace, &key_path)? {
        return Ok(Some(outcome));
    }
    if algorithm.is_semaphore() {
        if req.permits == 0 {
            return Ok(Some(conflict(path, "", Reason::InvalidPermits)));
        }
        if let Some(capacity) = semaphore_capacity(tx, &key_path)? {
            if capacity != req.permits {
                return Ok(Some(conflict(
                    path,
                    &capacity.to_string(),
                    Reason::InvalidPermits,
                )));
            }
        }
        if !tx.sismember(SEM_CF, &sem_prefix(&key_path), owner)? {
            let holders = prune_dead_semaphore_owners(tx, &key_path)?;
            if holders.len() as u32 >= req.permits {
                let blocker = holders.into_iter().next().unwrap_or_default();
                return Ok(Some(conflict(path, &blocker, Reason::SemaphoreFull)));
            }
        }
        return Ok(None);
    }
    let same_owner_write = match get_live_write_owner(tx, &key_path)? {
        Some(wr_owner) if wr_owner != owner => {
            return Ok(Some(conflict(path, &wr_owner, Reason::WriteLocked)));
        }
        Some(_) => true,
        None => false,
    };
    if req.mode == Mode::Write {
        if let Some(outcome) = validate_new_write(
            tx,
            owner,
            token,
            path,
            namespace,
            &key_path,
            algorithm,
            same_owner_write,
        )? {
            return Ok(Some(outcome));
        }
    }
    Ok(None)
}

fn validate_new_ancestors<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    namespace: &str,
    key_path: &str,
) -> anyhow::Result<Option<AcquireOutcome>> {
    for anc in get_ancestors(key_path) {
        if let Some(anc_owner) = get_live_write_owner(tx, &anc)? {
            if anc_owner != owner && hold_algorithm(tx, &anc_owner, Mode::Write, &anc)?.recursive()
            {
                return Ok(Some(conflict(
                    &public_path(namespace, &anc),
                    &anc_owner,
                    Reason::AncestorLocked,
                )));
            }
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn validate_new_write<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    token: i64,
    path: &str,
    namespace: &str,
    key_path: &str,
    algorithm: LockAlgorithm,
    same_owner_write: bool,
) -> anyhow::Result<Option<AcquireOutcome>> {
    let rd_owners = prune_dead_read_owners(tx, key_path)?;
    if rd_owners.is_empty() {
        remove_descendant_indexes(tx, Mode::Read, key_path)?;
    }
    for o in &rd_owners {
        if o != owner {
            return Ok(Some(conflict(path, o, Reason::ReadLocked)));
        }
    }
    if algorithm.recursive() {
        if let Some((path, owner, reason)) =
            find_descendant_write_conflict(tx, owner, key_path, namespace)?
        {
            return Ok(Some(AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }));
        }
        if let Some((path, owner, reason)) =
            find_descendant_read_conflict(tx, owner, key_path, namespace)?
        {
            return Ok(Some(AcquireOutcome::Conflict {
                path,
                owner,
                reason,
            }));
        }
    }
    if let Some(cur) = parse_fence(tx.get_str(FENCE_CF, &fence_key(key_path))?) {
        if cur > token || (cur == token && !same_owner_write) {
            return Ok(Some(conflict(
                path,
                &cur.to_string(),
                Reason::StaleFencingToken,
            )));
        }
    }
    Ok(None)
}

fn refresh_portfolio<T: StoreTxn>(tx: &mut T, alive_k: &[u8], ttl_ms: u64) -> anyhow::Result<()> {
    tx.set_str(ALIVE_CF, alive_k, "1", ttl_ms)
}

struct AcquireApplyCtx<'a> {
    owner: &'a str,
    namespace: &'a str,
    request_algorithm: LockAlgorithm,
    token: i64,
    fence_ttl: u64,
    own_pfx: &'a [u8],
}

fn apply_acquire<T: StoreTxn>(
    tx: &mut T,
    req: &LockReq,
    ctx: &AcquireApplyCtx<'_>,
) -> anyhow::Result<Option<AcquireOutcome>> {
    let path = &req.path;
    let key_path = scoped_path(ctx.namespace, path);
    let member = hold_member(ctx.namespace, req.mode, path);
    let held_algorithm = if req.state == State::Held {
        hold_algorithm(tx, ctx.owner, req.mode, &key_path)?
    } else {
        ctx.request_algorithm
    };

    let outcome = if held_algorithm.is_semaphore() {
        if req.state == State::New {
            match semaphore_capacity(tx, &key_path)? {
                Some(capacity) if capacity != req.permits => Some(conflict(
                    path,
                    &capacity.to_string(),
                    Reason::InvalidPermits,
                )),
                Some(_) => None,
                None => {
                    set_semaphore_capacity(tx, &key_path, req.permits)?;
                    None
                }
            }
        } else {
            None
        }
    } else if req.mode == Mode::Write {
        apply_write_acquire(
            tx,
            ctx.owner,
            path,
            req.state,
            &key_path,
            ctx.token,
            ctx.fence_ttl,
        )?
    } else {
        let rd_pfx = rd_prefix(&key_path);
        tx.sadd(RD_CF, &rd_pfx, ctx.owner, 0)?;
        add_descendant_indexes(tx, Mode::Read, &key_path, 0)?;
        None
    };
    if outcome.is_some() {
        return Ok(outcome);
    }
    if held_algorithm.is_semaphore() {
        tx.sadd(SEM_CF, &sem_prefix(&key_path), ctx.owner, 0)?;
    }
    tx.sadd(OWN_CF, ctx.own_pfx, &member, 0)?;
    set_hold_algorithm(tx, ctx.owner, req.mode, &key_path, held_algorithm, 0)?;
    Ok(None)
}

fn apply_write_acquire<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    path: &str,
    state: State,
    key_path: &str,
    token: i64,
    fence_ttl: u64,
) -> anyhow::Result<Option<AcquireOutcome>> {
    let wr_k = wr_key(key_path);
    let fence_k = fence_key(key_path);
    match state {
        State::Held => {
            tx.set_str(WR_CF, &wr_k, owner, 0)?;
            tx.set_str(FENCE_CF, &fence_k, &token.to_string(), fence_ttl)?;
            add_descendant_indexes(tx, Mode::Write, key_path, 0)?;
        }
        State::New => match tx.get_str(WR_CF, &wr_k)? {
            None => {
                tx.set_str(WR_CF, &wr_k, owner, 0)?;
                tx.set_str(FENCE_CF, &fence_k, &token.to_string(), fence_ttl)?;
                add_descendant_indexes(tx, Mode::Write, key_path, 0)?;
            }
            Some(current) if current == owner => {
                tx.set_str(WR_CF, &wr_k, owner, 0)?;
                tx.set_str(FENCE_CF, &fence_k, &token.to_string(), fence_ttl)?;
                add_descendant_indexes(tx, Mode::Write, key_path, 0)?;
            }
            Some(current) => return Ok(Some(conflict(path, &current, Reason::WriteLocked))),
        },
    }
    Ok(None)
}

fn apply_releases<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    namespace: &str,
    release_requests: &[RelReq],
    own_pfx: &[u8],
    alive_k: &[u8],
) -> anyhow::Result<()> {
    for req in release_requests {
        apply_release(tx, owner, namespace, req, own_pfx)?;
    }
    if !tx.has_live_member(OWN_CF, own_pfx)? {
        tx.del(ALIVE_CF, alive_k)?;
        tx.del(ALIVE_CF, &revoke_key(owner))?;
    }
    Ok(())
}

fn apply_release<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    namespace: &str,
    req: &RelReq,
    own_pfx: &[u8],
) -> anyhow::Result<()> {
    let path = &req.path;
    let key_path = scoped_path(namespace, path);
    let member = hold_member(namespace, req.mode, path);
    tx.srem(OWN_CF, own_pfx, &member)?;

    if req.mode == Mode::Write && hold_algorithm(tx, owner, Mode::Write, &key_path)?.is_semaphore()
    {
        tx.srem(SEM_CF, &sem_prefix(&key_path), owner)?;
        del_hold_algorithm(tx, owner, Mode::Write, &key_path)?;
    } else if req.mode == Mode::Write {
        let wr_k = wr_key(&key_path);
        if tx.get_str(WR_CF, &wr_k)?.as_deref() == Some(owner) {
            tx.del(WR_CF, &wr_k)?;
            del_hold_algorithm(tx, owner, Mode::Write, &key_path)?;
            remove_descendant_indexes(tx, Mode::Write, &key_path)?;
        }
    } else {
        let rd_pfx = rd_prefix(&key_path);
        tx.srem(RD_CF, &rd_pfx, owner)?;
        del_hold_algorithm(tx, owner, Mode::Read, &key_path)?;
        if !tx.has_live_member(RD_CF, &rd_pfx)? {
            remove_descendant_indexes(tx, Mode::Read, &key_path)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RELEASE
// ---------------------------------------------------------------------------

pub fn release_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    reqs: &[RelReq],
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let namespace = reqs
        .iter()
        .map(|r| crate::store_keys::handler_of(&r.path))
        .next()
        .unwrap_or_default()
        .to_string();
    release_inner_in_namespace(tx, &namespace, owner, reqs, del_wait_key)
}

pub fn release_inner_in_namespace<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    owner: &str,
    reqs: &[RelReq],
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let own_pfx = own_prefix(owner);
    let alive_k = alive_key(owner);

    for req in reqs {
        let path = &req.path;
        let key_path = scoped_path(namespace, path);
        let member = hold_member(namespace, req.mode, path);
        tx.srem(OWN_CF, &own_pfx, &member)?;

        if req.mode == Mode::Write
            && hold_algorithm(tx, owner, Mode::Write, &key_path)?.is_semaphore()
        {
            tx.srem(SEM_CF, &sem_prefix(&key_path), owner)?;
            del_hold_algorithm(tx, owner, Mode::Write, &key_path)?;
        } else if req.mode == Mode::Write {
            let wr_k = wr_key(&key_path);
            if tx.get_str(WR_CF, &wr_k)?.as_deref() == Some(owner) {
                tx.del(WR_CF, &wr_k)?;
                del_hold_algorithm(tx, owner, Mode::Write, &key_path)?;
                remove_descendant_indexes(tx, Mode::Write, &key_path)?;
            }
        } else {
            let rd_pfx = rd_prefix(&key_path);
            tx.srem(RD_CF, &rd_pfx, owner)?;
            del_hold_algorithm(tx, owner, Mode::Read, &key_path)?;
            if !tx.has_live_member(RD_CF, &rd_pfx)? {
                remove_descendant_indexes(tx, Mode::Read, &key_path)?;
            }
        }
    }

    if !tx.has_live_member(OWN_CF, &own_pfx)? {
        tx.del(ALIVE_CF, &alive_k)?;
        tx.del(ALIVE_CF, &revoke_key(owner))?;
    }

    // In the multi-group deployment wait edges live only in the sys group,
    // so this delete is a no-op for lock-group releases (the router clears
    // the sys-group edge separately); it matters for single-store embeddings
    // and the engine test suite, where everything shares one keyspace.
    if del_wait_key {
        tx.del(WAIT_CF, &wait_key(owner))?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// RELEASE_ALL
// ---------------------------------------------------------------------------

/// Release the lock state behind one member of an owner's hold set. The
/// member's own `OWN_CF` entry is removed by the caller.
fn release_held_member<T: StoreTxn>(tx: &mut T, owner: &str, item: &str) -> anyhow::Result<()> {
    let Some(held) = parse_hold_member(item) else {
        return Ok(());
    };
    let key_path = held_key_path(&held);
    match held.mode {
        Mode::Write if hold_algorithm(tx, owner, Mode::Write, &key_path)?.is_semaphore() => {
            tx.srem(SEM_CF, &sem_prefix(&key_path), owner)?;
            del_hold_algorithm(tx, owner, Mode::Write, &key_path)?;
        }
        Mode::Write => {
            let wr_k = wr_key(&key_path);
            if tx.get_str(WR_CF, &wr_k)?.as_deref() == Some(owner) {
                tx.del(WR_CF, &wr_k)?;
                del_hold_algorithm(tx, owner, Mode::Write, &key_path)?;
                remove_descendant_indexes(tx, Mode::Write, &key_path)?;
            }
        }
        Mode::Read => {
            let rd_pfx = rd_prefix(&key_path);
            tx.srem(RD_CF, &rd_pfx, owner)?;
            del_hold_algorithm(tx, owner, Mode::Read, &key_path)?;
            if !tx.has_live_member(RD_CF, &rd_pfx)? {
                remove_descendant_indexes(tx, Mode::Read, &key_path)?;
            }
        }
    }
    Ok(())
}

/// Release every lock an owner holds, paging through the hold set so an
/// oversized owner can always be cleaned up (the one-shot enumeration used by
/// renew/acquire would error out, which previously made `release_all` and
/// `force_release` fail for exactly the owners that most needed them).
///
/// The owner's liveness and wait-edge markers are removed unconditionally at
/// the end: even if physical cleanup is capped, a dead `alive` record means
/// every remaining record is prunable on next touch and reclaimable by GC.
fn release_owner_wide<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    del_wait_key: bool,
) -> anyhow::Result<()> {
    let own_pfx = own_prefix(owner);
    let mut cursor: Option<Vec<u8>> = None;
    let mut processed = 0usize;
    loop {
        let (members, next) = tx.smembers_page(OWN_CF, &own_pfx, cursor.take(), RELEASE_PAGE)?;
        for item in &members {
            release_held_member(tx, owner, item)?;
            tx.srem(OWN_CF, &own_pfx, item)?;
            processed += 1;
        }
        match next {
            None => break,
            Some(c) => {
                if processed >= MAX_RELEASE_MEMBERS {
                    warn!(
                        owner,
                        processed, "owner-wide release hit member cap; residue left to TTL+GC"
                    );
                    break;
                }
                cursor = Some(c);
            }
        }
    }

    tx.del(ALIVE_CF, &alive_key(owner))?;
    tx.del(ALIVE_CF, &revoke_key(owner))?;
    if del_wait_key {
        tx.del(WAIT_CF, &wait_key(owner))?;
    }
    Ok(())
}

pub fn release_all_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    del_wait_key: bool,
) -> anyhow::Result<()> {
    release_owner_wide(tx, owner, del_wait_key)
}

// ---------------------------------------------------------------------------
// RENEW
// ---------------------------------------------------------------------------

pub fn renew_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    ttl_ms: u64,
) -> anyhow::Result<RenewOutcome> {
    let alive_k = alive_key(owner);
    let own_pfx = own_prefix(owner);

    if tx.get_str(ALIVE_CF, &alive_k)?.is_none() {
        return Ok(renew_lost("", Reason::MissingAlive));
    }
    if !tx.has_live_member(OWN_CF, &own_pfx)? {
        return Ok(renew_lost("", Reason::MissingOwnerSet));
    }
    tx.pexpire_str(ALIVE_CF, &alive_k, ttl_ms)?;
    // Piggyback the cooperative-revoke signal on the heartbeat the holder is
    // already sending. Read-only; the marker is cleared on release, not here.
    let revoke_requested = tx.get_str(ALIVE_CF, &revoke_key(owner))?.is_some();
    Ok(RenewOutcome::Ok { revoke_requested })
}

// ---------------------------------------------------------------------------
// REQUEST_REVOKE (cooperative)
// ---------------------------------------------------------------------------

/// Record a pending cooperative-revoke marker for `owner`, but only where the
/// owner actually holds a lease — a marker for an absent owner would be litter
/// that only its TTL reaps. The marker is read back on the owner's next
/// [`renew_inner`] and cleared when its liveness record is deleted.
pub fn request_revoke_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    if tx.get_str(ALIVE_CF, &alive_key(owner))?.is_none() {
        return Ok(());
    }
    tx.set_str(ALIVE_CF, &revoke_key(owner), "1", ttl_ms)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// FORCE_RELEASE
// ---------------------------------------------------------------------------

pub fn force_release_inner<T: StoreTxn>(tx: &mut T, victim: &str) -> anyhow::Result<()> {
    release_owner_wide(tx, victim, true)
}

// ---------------------------------------------------------------------------
// ASSERT_FENCING
// ---------------------------------------------------------------------------

pub fn assert_fencing_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    owner: &str,
    fencing_token: i64,
    paths: &[String],
) -> anyhow::Result<AssertOutcome> {
    if !paths.is_empty() && !owner_alive(tx, owner)? {
        return Ok(AssertOutcome::Fail {
            path: paths[0].clone(),
            reason: Reason::StaleOwner,
        });
    }
    let token_str = fencing_token.to_string();
    for path in paths {
        let key_path = scoped_path(namespace, path);
        if tx.get_str(WR_CF, &wr_key(&key_path))?.as_deref() != Some(owner) {
            return Ok(AssertOutcome::Fail {
                path: path.clone(),
                reason: Reason::StaleOwner,
            });
        }
        if tx.get_str(FENCE_CF, &fence_key(&key_path))?.as_deref() != Some(token_str.as_str()) {
            return Ok(AssertOutcome::Fail {
                path: path.clone(),
                reason: Reason::StaleFencingToken,
            });
        }
    }
    Ok(AssertOutcome::Ok)
}

// ---------------------------------------------------------------------------
// Wait edge encoding
// ---------------------------------------------------------------------------

pub fn encode_wait_edge(conflict_owner: &str, metadata: Option<&WaitEdgeMetadata>) -> String {
    let Some(metadata) = metadata else {
        return conflict_owner.to_string();
    };
    let reason = metadata.reason.as_str();
    format!(
        "{WAIT_EDGE_V1_PREFIX}{}:{}:{}:{}{}{}",
        conflict_owner.len(),
        metadata.conflict_path.len(),
        reason.len(),
        conflict_owner,
        metadata.conflict_path,
        reason
    )
}

pub fn parse_wait_edge(raw: String) -> anyhow::Result<WaitEdge> {
    let Some(rest) = raw.strip_prefix(WAIT_EDGE_V1_PREFIX) else {
        return Ok(WaitEdge {
            conflict_owner: raw,
            metadata: None,
        });
    };
    let (owner_len, rest) = parse_len_field(rest)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: missing owner length"))?;
    let (path_len, rest) = parse_len_field(rest)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: missing path length"))?;
    let (reason_len, payload) = parse_len_field(rest)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: missing reason length"))?;

    let total_len = owner_len
        .checked_add(path_len)
        .and_then(|v| v.checked_add(reason_len));
    if total_len != Some(payload.len()) {
        anyhow::bail!("malformed wait edge: payload length mismatch");
    }

    let owner_end = owner_len;
    let path_end = owner_end + path_len;
    let conflict_owner = payload
        .get(..owner_end)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: owner slice out of bounds"))?;
    let conflict_path = payload
        .get(owner_end..path_end)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: path slice out of bounds"))?;
    let reason = payload
        .get(path_end..)
        .ok_or_else(|| anyhow::anyhow!("malformed wait edge: reason slice out of bounds"))?;
    Ok(WaitEdge {
        conflict_owner: conflict_owner.to_string(),
        metadata: Some(WaitEdgeMetadata {
            conflict_path: conflict_path.to_string(),
            reason: reason.parse::<Reason>()?,
        }),
    })
}

fn parse_len_field(input: &str) -> Option<(usize, &str)> {
    let (len, rest) = input.split_once(':')?;
    if len.is_empty() {
        return None;
    }
    Some((len.parse::<usize>().ok()?, rest))
}

/// Read one owner's wait edge (if any) from the wait-graph keyspace.
pub fn read_wait_edge<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<Option<WaitEdge>> {
    match tx.get_str(WAIT_CF, &wait_key(owner))? {
        None => Ok(None),
        Some(raw) => Ok(Some(parse_wait_edge(raw)?)),
    }
}

// ---------------------------------------------------------------------------
// IS_BLOCKING
// ---------------------------------------------------------------------------
//
// The deadlock walk itself lives in `cluster::router::detect_cycle`: wait
// edges are cluster-global (sys group) while each hop's liveness/blocking
// checks read the blocker's lock groups, so the walk composes `read_wait_edge`
// with `is_blocking_inner` across groups rather than running in one engine
// transaction.

pub fn is_blocking_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    conflict_path: &str,
    conflict_owner: &str,
    reason: Reason,
) -> anyhow::Result<bool> {
    let key_path = scoped_path(namespace, conflict_path);
    let is_read = matches!(reason, Reason::ReadLocked | Reason::DescendantReadLocked);

    if is_read {
        let rd_pfx = rd_prefix(&key_path);
        if !tx.sismember(RD_CF, &rd_pfx, conflict_owner)? {
            return Ok(false);
        }
        if tx.get_str(ALIVE_CF, &alive_key(conflict_owner))?.is_some() {
            return Ok(true);
        }
        tx.srem(RD_CF, &rd_pfx, conflict_owner)?;
        del_hold_algorithm(tx, conflict_owner, Mode::Read, &key_path)?;
        if !tx.has_live_member(RD_CF, &rd_pfx)? {
            remove_descendant_indexes(tx, Mode::Read, &key_path)?;
        }
        return Ok(false);
    }

    if reason == Reason::SemaphoreFull {
        if !tx.sismember(SEM_CF, &sem_prefix(&key_path), conflict_owner)? {
            return Ok(false);
        }
        if tx.get_str(ALIVE_CF, &alive_key(conflict_owner))?.is_some() {
            return Ok(true);
        }
        tx.srem(SEM_CF, &sem_prefix(&key_path), conflict_owner)?;
        del_hold_algorithm(tx, conflict_owner, Mode::Write, &key_path)?;
        return Ok(false);
    }

    Ok(get_live_write_owner(tx, &key_path)?.as_deref() == Some(conflict_owner))
}

// ---------------------------------------------------------------------------
// Single-key ops
// ---------------------------------------------------------------------------

pub fn set_wait_edge_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
    conflict_owner: &str,
    ttl_ms: u64,
    metadata: Option<&WaitEdgeMetadata>,
) -> anyhow::Result<()> {
    let edge = encode_wait_edge(conflict_owner, metadata);
    tx.set_str(WAIT_CF, &wait_key(owner), &edge, ttl_ms)
}

pub fn clear_wait_edge_inner<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<()> {
    tx.del(WAIT_CF, &wait_key(owner))
}

pub fn is_owner_alive_inner<T: StoreTxn>(tx: &mut T, owner: &str) -> anyhow::Result<bool> {
    tx.get_str(ALIVE_CF, &alive_key(owner)).map(|v| v.is_some())
}

// ---------------------------------------------------------------------------
// Inspection (read-only observability)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PathInfo {
    pub write_owner: Option<String>,
    pub read_owners: Vec<String>,
    pub fence: Option<i64>,
    pub claim_owner: Option<String>,
    /// Live semaphore holders of this exact path (empty for non-semaphore paths).
    pub semaphore_owners: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnedLock {
    pub path: String,
    pub mode: Mode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockEntry {
    pub owner: String,
    pub path: String,
    pub mode: Mode,
    pub fence: Option<i64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LockDumpPage {
    pub entries: Vec<LockEntry>,
    pub next_cursor: Option<Vec<u8>>,
}

pub fn inspect_path_inner<T: StoreTxn>(
    tx: &mut T,
    namespace: &str,
    path: &str,
) -> anyhow::Result<PathInfo> {
    let key_path = scoped_path(namespace, path);
    let write_owner = match tx.get_str(WR_CF, &wr_key(&key_path))? {
        Some(owner) if owner_alive(tx, &owner)? => Some(owner),
        _ => None,
    };

    let rd_pfx = rd_prefix(&key_path);
    let mut read_owners = Vec::new();
    for owner in tx.smembers_limited(RD_CF, &rd_pfx, MAX_SET_ENUM_MEMBERS)? {
        if owner_alive(tx, &owner)? {
            read_owners.push(owner);
        }
    }

    let fence = parse_fence(tx.get_str(FENCE_CF, &fence_key(&key_path))?);

    let mut semaphore_owners = Vec::new();
    for owner in tx.smembers_limited(SEM_CF, &sem_prefix(&key_path), MAX_SET_ENUM_MEMBERS)? {
        if owner_alive(tx, &owner)? {
            semaphore_owners.push(owner);
        }
    }

    Ok(PathInfo {
        write_owner,
        read_owners,
        fence,
        // Path claims were removed in favour of the wait queue; retained on the
        // inspection struct (always None) for wire/API stability.
        claim_owner: None,
        semaphore_owners,
    })
}

pub fn list_owner_locks_inner<T: StoreTxn>(
    tx: &mut T,
    owner: &str,
) -> anyhow::Result<(bool, Vec<OwnedLock>)> {
    let alive = tx.get_str(ALIVE_CF, &alive_key(owner))?.is_some();
    if !alive {
        return Ok((false, Vec::new()));
    }
    let own_pfx = own_prefix(owner);
    let members = tx.smembers_limited(OWN_CF, &own_pfx, MAX_SET_ENUM_MEMBERS)?;

    let mut locks = Vec::with_capacity(members.len());
    for member in members {
        let Some(held) = parse_hold_member(&member) else {
            continue;
        };
        locks.push(OwnedLock {
            path: held.path.into_string(),
            mode: held.mode,
        });
    }
    Ok((alive, locks))
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

pub fn parse_fence(v: Option<String>) -> Option<i64> {
    v.and_then(|s| s.parse::<i64>().ok())
}

fn conflict(path: &str, owner: &str, reason: Reason) -> AcquireOutcome {
    AcquireOutcome::Conflict {
        path: path.to_string(),
        owner: owner.to_string(),
        reason,
    }
}

fn lost(path: &str, reason: Reason) -> AcquireOutcome {
    AcquireOutcome::Lost {
        path: path.to_string(),
        reason,
    }
}

fn renew_lost(path: &str, reason: Reason) -> RenewOutcome {
    RenewOutcome::Lost {
        path: path.to_string(),
        reason,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_ancestors_walks_up_to_root() {
        assert_eq!(
            get_ancestors("h:/a/b/c"),
            vec!["h:/a/b".to_string(), "h:/a".to_string(), "h:/".to_string()]
        );
        assert_eq!(
            get_ancestors("h:/a/b"),
            vec!["h:/a".to_string(), "h:/".to_string()]
        );
        assert_eq!(get_ancestors("h:/a"), vec!["h:/".to_string()]);
    }

    #[test]
    fn get_ancestors_root_and_degenerate() {
        assert!(get_ancestors("h:/").is_empty());
        assert!(get_ancestors("h:").is_empty());
        assert!(get_ancestors("nocolon").is_empty());
    }

    #[test]
    fn get_ancestors_share_handler_prefix() {
        for anc in get_ancestors("google_drive:/x/y/z") {
            assert!(anc.starts_with("google_drive:"));
        }
    }

    #[test]
    fn lock_algorithm_parses_canonical_names_round_trip() {
        for name in LockAlgorithm::variants() {
            let parsed: LockAlgorithm = name.parse().unwrap();
            assert_eq!(parsed.as_str(), *name);
        }
        // Case / separator normalization still applies.
        assert_eq!(
            "Recursive-RW".parse::<LockAlgorithm>().unwrap(),
            LockAlgorithm::RecursiveRw
        );
        assert_eq!(
            " semaphore ".parse::<LockAlgorithm>().unwrap(),
            LockAlgorithm::Semaphore
        );
    }

    #[test]
    fn lock_algorithm_rejects_removed_aliases() {
        // The legacy synonym set was trimmed to one canonical name per variant.
        for alias in [
            "rwlock",
            "rwlock_no_recursion",
            "mutex",
            "flat_write",
            "bogus",
        ] {
            assert!(
                alias.parse::<LockAlgorithm>().is_err(),
                "{alias:?} should no longer parse"
            );
        }
    }

    #[test]
    fn semaphore_algorithm_flags() {
        let s = LockAlgorithm::Semaphore;
        assert!(s.is_semaphore());
        assert!(!s.allows_read());
        assert!(!s.recursive());
        assert!(s.allows_mode(Mode::Write));
        assert!(!s.allows_mode(Mode::Read));
    }

    #[test]
    fn parse_fence_only_accepts_integers() {
        assert_eq!(parse_fence(Some("5".into())), Some(5));
        assert_eq!(parse_fence(Some("-3".into())), Some(-3));
        assert_eq!(parse_fence(Some("abc".into())), None);
        assert_eq!(parse_fence(Some(String::new())), None);
        assert_eq!(parse_fence(None), None);
    }

    #[test]
    fn wait_edge_encoding_round_trips_metadata() {
        let edge = parse_wait_edge(encode_wait_edge(
            "owner:with:colons",
            Some(&WaitEdgeMetadata {
                conflict_path: "h:/a/b".into(),
                reason: Reason::DescendantWriteLocked,
            }),
        ))
        .unwrap();
        assert_eq!(edge.conflict_owner, "owner:with:colons");
        assert_eq!(
            edge.metadata,
            Some(WaitEdgeMetadata {
                conflict_path: "h:/a/b".into(),
                reason: Reason::DescendantWriteLocked
            })
        );
    }

    #[test]
    fn wait_edge_parser_keeps_bare_owner_values() {
        let edge = parse_wait_edge("plain-owner".into()).unwrap();
        assert_eq!(edge.conflict_owner, "plain-owner");
        assert_eq!(edge.metadata, None);
    }

    #[test]
    fn wait_edge_parser_rejects_malformed_versioned_values() {
        let err = parse_wait_edge(format!("{WAIT_EDGE_V1_PREFIX}3:1:1:too-short")).unwrap_err();
        assert!(err.to_string().contains("malformed wait edge"));
    }
}
