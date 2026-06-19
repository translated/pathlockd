//! Per-owner retained event log backing the HTTP event APIs.
//!
//! The gRPC `Subscribe` stream is live-only: it delivers events while a
//! subscription is attached and forgets them otherwise. That is fine for a
//! persistent stream, but the HTTP facade needs two things gRPC does not:
//!
//!   * **SSE resume** — a reconnecting `EventSource` replays from its
//!     `Last-Event-ID`, so we must keep a short, monotonically-numbered history.
//!   * **Long-poll** — a legacy client disconnects *between* polls, so events
//!     raised in the gap must survive until the next poll.
//!
//! Both are served by a bounded per-owner ring of `(id, Event)`, fed by a
//! single background drain task that subscribes to the [`Broadcaster`] once per
//! owner and fans out to every attached HTTP client. The ring is reference
//! counted: it stays alive (and keeps draining) while any client is attached,
//! and for a short retention window afterwards so poll gaps and quick
//! reconnects do not lose events. History is still best-effort — an owner with
//! no attached client and an expired window starts fresh, exactly like gRPC.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    owners: Mutex<HashMap<String, Arc<OwnerLog>>>,
}

impl EventLog {
    pub fn new(broadcaster: Broadcaster, capacity: usize, retention: Duration) -> Arc<Self> {
        Arc::new(Self {
            broadcaster,
            capacity: capacity.max(1),
            retention,
            owners: Mutex::new(HashMap::new()),
        })
    }

    fn owners(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<OwnerLog>>> {
        self.owners.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Attach an interested HTTP client to `owner`, starting the drain task if
    /// this is the first. The returned guard keeps the log alive; drop it when
    /// the client disconnects.
    pub fn attach(self: &Arc<Self>, owner: &str) -> Attach {
        self.evict_expired();
        let mut owners = self.owners();
        let log = owners
            .entry(owner.to_string())
            .or_insert_with(|| OwnerLog::spawn(self, owner))
            .clone();
        log.refs.fetch_add(1, Ordering::AcqRel);
        Attach { log }
    }

    /// Drop owner logs whose last client detached longer ago than the retention
    /// window. Cheap and lazy: runs on every attach.
    fn evict_expired(&self) {
        let mut owners = self.owners();
        owners.retain(|_, log| {
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
            }
            keep
        });
    }
}

/// One owner's ring plus the synchronization a reader needs to wait for more.
pub struct OwnerLog {
    capacity: usize,
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
    fn spawn(log: &Arc<EventLog>, owner: &str) -> Arc<OwnerLog> {
        let owner_log = Arc::new(OwnerLog {
            capacity: log.capacity,
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
        let id = self.last_id.fetch_add(1, Ordering::AcqRel) + 1;
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

    /// Wait until a new event is appended, the log is torn down, or `timeout`
    /// elapses. Returns `true` if woken by an event/teardown, `false` on
    /// timeout. The caller must re-read via [`since`] after waking.
    pub async fn wait(&self, timeout: Duration) -> bool {
        let notified = self.notify.notified();
        let shutdown = self.shutdown.notified();
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
