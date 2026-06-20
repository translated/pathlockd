//! Per-owner retained event log backing the HTTP SSE endpoint.
//!
//! The gRPC `Subscribe` stream is live-only: it delivers events while a
//! subscription is attached and forgets them otherwise. That is fine for a
//! persistent stream, but the SSE facade needs one thing gRPC does not:
//! **resume** — a reconnecting `EventSource` replays from its `Last-Event-ID`,
//! so we keep a short, monotonically-numbered history.
//!
//! It is served by a bounded per-owner ring of `(id, Event)`, fed by a single
//! background drain task that subscribes to the [`Broadcaster`] once per owner
//! and fans out to every attached HTTP client. The ring is reference counted:
//! it stays alive (and keeps draining) while any client is attached, and for a
//! short retention window afterwards so quick reconnects do not lose events.
//! History is still best-effort — an owner with no attached client and an
//! expired window starts fresh, exactly like gRPC.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::futures::Notified;
use tokio::sync::Notify;

use crate::events::Broadcaster;
use crate::proto::Event;

/// An event as retained in the ring: the owner-scoped monotonic id plus the
/// wire event. `id` is what a client echoes back as `Last-Event-ID` / `after`.
#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub id: u64,
    pub event: Event,
}

/// Shared registry of per-owner logs.
pub struct EventLog {
    broadcaster: Broadcaster,
    capacity: usize,
    retention: Duration,
    owners: Mutex<HashMap<String, OwnerEntry>>,
    last_evict: Mutex<Instant>,
}

/// One owner's retained log plus its persistent id counter. The counter is
/// kept across evictions (the log is dropped after the retention window, the
/// counter is not) so a reconnecting client's `Last-Event-ID` from a previous
/// incarnation never exceeds the ids a fresh log issues — which would otherwise
/// suppress new events until the counter caught up.
struct OwnerEntry {
    log: Option<Arc<OwnerLog>>,
    seq: Arc<AtomicU64>,
}

/// Soft cap on remembered per-owner id counters. Each is one `AtomicU64`, so
/// retained counters are cheap; beyond this bound idle entries (no live log)
/// are dropped entirely, which only re-introduces id reset for owners unseen
/// in a long time under extreme churn.
const MAX_REMEMBERED_OWNERS: usize = 65_536;

impl EventLog {
    pub fn new(broadcaster: Broadcaster, capacity: usize, retention: Duration) -> Arc<Self> {
        let log = Arc::new(Self {
            broadcaster,
            capacity: capacity.max(1),
            retention,
            owners: Mutex::new(HashMap::new()),
            last_evict: Mutex::new(Instant::now()),
        });
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(&log);
            handle.spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tick.tick().await;
                    let Some(log) = weak.upgrade() else {
                        return;
                    };
                    log.evict_expired();
                }
            });
        }
        log
    }

    fn owners(&self) -> std::sync::MutexGuard<'_, HashMap<String, OwnerEntry>> {
        self.owners.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Attach an interested HTTP client to `owner`, starting the drain task if
    /// this is the first. The returned guard keeps the log alive; drop it when
    /// the client disconnects.
    pub fn attach(self: &Arc<Self>, owner: &str) -> Attach {
        self.evict_expired();
        let mut owners = self.owners();
        let entry = owners
            .entry(owner.to_string())
            .or_insert_with(|| OwnerEntry {
                log: None,
                seq: Arc::new(AtomicU64::new(0)),
            });
        if entry.log.is_none() {
            entry.log = Some(OwnerLog::spawn(self, owner, entry.seq.clone()));
        }
        let log = entry.log.clone().expect("log just spawned");
        log.refs.fetch_add(1, Ordering::AcqRel);
        Attach { log }
    }

    /// Drop owner logs whose last client detached longer ago than the retention
    /// window. Cheap and lazy: runs on every attach. The per-owner id counter
    /// is retained so reconnects resume correctly.
    fn evict_expired(&self) {
        let mut last = self.last_evict.lock().unwrap_or_else(|p| p.into_inner());
        if last.elapsed() < Duration::from_secs(1) {
            return;
        }
        *last = Instant::now();
        drop(last);
        let mut owners = self.owners();
        owners.retain(|_, entry| {
            if let Some(log) = &entry.log {
                if log.refs.load(Ordering::Acquire) > 0 {
                    return true;
                }
                let keep = log
                    .idle_since
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .map(|t| t.elapsed() < self.retention)
                    .unwrap_or(true);
                if !keep {
                    log.shutdown.notify_waiters();
                    entry.log = None;
                }
                return true;
            }
            true
        });
        if owners.len() > MAX_REMEMBERED_OWNERS {
            // Over budget: drop the cheapest remembered counters (those with
            // no live log) to bound memory under owner churn.
            let excess = owners.len() - MAX_REMEMBERED_OWNERS;
            let mut to_drop: Vec<String> = owners
                .iter()
                .filter(|(_, e)| e.log.is_none())
                .take(excess)
                .map(|(k, _)| k.clone())
                .collect();
            for k in to_drop.drain(..) {
                owners.remove(&k);
            }
        }
    }
}

/// One owner's ring plus the synchronization a reader needs to wait for more.
pub struct OwnerLog {
    capacity: usize,
    /// Per-owner monotonic id source, shared with the registry so it survives
    /// eviction of this log. Ids never restart at zero for an owner the
    /// registry remembers, so a reconnecting `Last-Event-ID` always stays
    /// below the next issued id.
    seq: Arc<AtomicU64>,
    last_id: AtomicU64,
    ring: Mutex<VecDeque<StoredEvent>>,
    /// Woken whenever a new event is appended.
    notify: Notify,
    /// Woken when the log is being torn down so readers/drain can exit.
    shutdown: Notify,
    refs: AtomicUsize,
    idle_since: Mutex<Option<Instant>>,
}

impl OwnerLog {
    fn spawn(log: &Arc<EventLog>, owner: &str, seq: Arc<AtomicU64>) -> Arc<OwnerLog> {
        let owner_log = Arc::new(OwnerLog {
            capacity: log.capacity,
            seq,
            last_id: AtomicU64::new(0),
            ring: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            shutdown: Notify::new(),
            refs: AtomicUsize::new(0),
            idle_since: Mutex::new(None),
        });
        let broadcaster = log.broadcaster.clone();
        let owner = owner.to_string();
        let drain_target = owner_log.clone();
        tokio::spawn(async move {
            drain(broadcaster, owner, drain_target).await;
        });
        owner_log
    }

    fn push(&self, event: Event) {
        let id = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        self.last_id.store(id, Ordering::Release);
        let mut ring = self.ring.lock().unwrap_or_else(|p| p.into_inner());
        ring.push_back(StoredEvent { id, event });
        while ring.len() > self.capacity {
            ring.pop_front();
        }
        drop(ring);
        self.notify.notify_waiters();
    }

    /// The highest id currently issued. A client can pass this as `after` to
    /// stream only future events.
    pub fn last_id(&self) -> u64 {
        self.last_id.load(Ordering::Acquire)
    }

    /// Events with `id > after`, oldest first. If `after` predates the retained
    /// window the caller silently skips the evicted ids (a gap it can detect by
    /// comparing the first returned id to `after + 1`).
    pub fn since(&self, after: u64) -> Vec<StoredEvent> {
        let ring = self.ring.lock().unwrap_or_else(|p| p.into_inner());
        ring.iter().filter(|e| e.id > after).cloned().collect()
    }

    /// Register interest in the next event or teardown, returning a guard the
    /// caller awaits *after* draining via [`since`](Self::since). The waiter must
    /// be enabled *before* the drain: [`push`](Self::push) wakes via
    /// `Notify::notify_waiters`, which stores no permit, so an event appended in
    /// the window between a drain and the wait would otherwise be missed until
    /// the guard's `timeout`. Enabling up front closes that window — such a
    /// wakeup lands on the already-registered guard.
    pub fn prepare_wait(&self) -> WaitGuard<'_> {
        let mut notified = Box::pin(self.notify.notified());
        let mut shutdown = Box::pin(self.shutdown.notified());
        notified.as_mut().enable();
        shutdown.as_mut().enable();
        WaitGuard { notified, shutdown }
    }
}

/// A pre-registered wakeup from [`OwnerLog::prepare_wait`], created before the
/// caller drains so no `notify_waiters` wakeup is lost in the check-then-wait
/// gap; awaited afterwards.
pub struct WaitGuard<'a> {
    notified: Pin<Box<Notified<'a>>>,
    shutdown: Pin<Box<Notified<'a>>>,
}

impl WaitGuard<'_> {
    /// Wait until a new event is appended, the log is torn down, or `timeout`
    /// elapses. Returns `true` if woken by an event/teardown, `false` on
    /// timeout. The caller must re-read via [`OwnerLog::since`] after waking.
    pub async fn wait(self, timeout: Duration) -> bool {
        let Self { notified, shutdown } = self;
        tokio::select! {
            _ = notified => true,
            _ = shutdown => true,
            _ = tokio::time::sleep(timeout) => false,
        }
    }
}

/// RAII attachment guard. Holds the owner log alive; on drop, decrements the
/// refcount and stamps the idle clock so the retention window can start.
pub struct Attach {
    log: Arc<OwnerLog>,
}

impl Attach {
    pub fn log(&self) -> &OwnerLog {
        &self.log
    }
}

impl Drop for Attach {
    fn drop(&mut self) {
        if self.log.refs.fetch_sub(1, Ordering::AcqRel) == 1 {
            *self
                .log
                .idle_since
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(Instant::now());
        }
    }
}

/// Background task: drain the broadcaster subscription for one owner into its
/// ring until the log is torn down.
async fn drain(broadcaster: Broadcaster, owner: String, log: Arc<OwnerLog>) {
    use futures::StreamExt;
    let mut sub = broadcaster.subscribe(&owner);
    let shutdown = log.shutdown.notified();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            next = sub.next() => match next {
                Some(ev) => log.push(ev),
                None => break,
            },
        }
    }
}
