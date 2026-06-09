//! Event fan-out for the per-owner lifecycle stream (release / kill / revoke).
//!
//! Within a single instance, each `Subscribe` stream registers a bounded mpsc
//! sender in a per-owner registry keyed by owner id, and an event is routed only
//! to the senders registered for *its* owner. Routing is one map lookup, so an
//! event reaches just the handful of streams that asked for that owner — cost
//! scales with that owner's subscribers, not the instance-wide subscriber count.
//! This replaces a single global broadcast channel, where every subscription
//! woke for every event instance-wide only to discard the ones addressed to
//! other owners (O(subscribers × events) wakeups, and slow subscribers lagging
//! the shared ring).
//!
//! Across instances, an event is best-effort forwarded to every peer's
//! `PublishEvent` RPC so an event raised on instance A reaches the owner's
//! subscription on instance B. The client-side recheck timer is the correctness
//! backstop, so a dropped peer message only costs latency, never safety.
//!
//! Peer fan-out uses one long-lived forwarder task per peer draining a bounded
//! queue (not a task per event), so a slow or dead peer can neither pile up
//! tasks nor stall the request path: a full queue simply drops the event, and
//! each forward RPC carries a timeout.
//!
//! The peer set has two sources, unioned: a fixed list ([`Broadcaster::new`])
//! and a *dynamically discovered* set ([`Broadcaster::reconcile_dynamic_peers`])
//! that the daemon refreshes by resolving a headless-Service DNS name. Because a
//! single gRPC channel pins to one resolved address, broadcasting to N replicas
//! needs N forwarders — exactly one per discovered pod IP — which is why fan-out
//! requires individually-addressable replicas (a Kubernetes StatefulSet behind a
//! headless Service), not a single load-balanced VIP. Reconciliation adds a
//! forwarder for each newly seen endpoint and drops the sender for any that
//! vanished (which ends that forwarder task); a transient resolution failure
//! leaves the current set untouched.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use crate::proto::{path_lock_client::PathLockClient, Event, EventType, PublishEventRequest};

/// Per-peer forwarder queue depth. Events are tiny and infrequent; if a peer is
/// slow enough to fill this, we drop (the client-side recheck is the backstop).
const PEER_QUEUE: usize = 1024;
/// Timeout applied to each peer `PublishEvent` RPC (connect and per-call).
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// HTTP/2 keepalive ping interval for a peer channel. A peer connection is idle
/// whenever no events flow, and a load balancer / conntrack table between
/// replicas can silently reap an idle stream; periodic keepalive pings keep the
/// channel live (or surface a dead peer promptly so the lazy channel reconnects).
const PEER_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
/// How long to wait for a keepalive ping ack before declaring the peer dead.
const PEER_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// Hard cap for each subscriber queue. Tokio's bounded mpsc rejects capacities
/// above its semaphore limit; keep config inside a sane operational range before
/// channel construction can panic.
const MAX_SUBSCRIBER_QUEUE: usize = 1_000_000;

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Arc<Registry>,
    /// Fixed peers from config (`PATHLOCKD_PEERS`); never reconciled away.
    static_peer_txs: Vec<mpsc::Sender<Event>>,
    /// Peers discovered by resolving a headless-Service DNS name, keyed by
    /// endpoint URL so reconciliation can add/remove exactly the ones that
    /// changed. Dropping a sender ends its forwarder task.
    dynamic_peer_txs: Mutex<HashMap<String, mpsc::Sender<Event>>>,
}

/// Live subscriptions keyed by owner id. An event is delivered by looking up its
/// owner and pushing to that owner's senders only, so delivery cost scales with
/// the subscribers for *that* owner, not the instance-wide subscriber count.
struct Registry {
    subs: Mutex<HashMap<String, Vec<SubSender>>>,
    next_id: AtomicU64,
    /// Per-subscriber queue depth. A subscriber only ever queues its own owner's
    /// events, so this fills only if that one client stalls; an overflow drops
    /// (the client recheck is the correctness backstop). tokio's bounded mpsc
    /// allocates on demand, so a large depth costs memory only when backlogged.
    capacity: usize,
}

/// One subscriber's sender plus the id used to remove exactly it on drop (an
/// owner may hold more than one subscription).
struct SubSender {
    id: u64,
    tx: mpsc::Sender<Event>,
}

impl Registry {
    fn new(capacity: usize) -> anyhow::Result<Arc<Self>> {
        if capacity == 0 {
            anyhow::bail!("event_buffer must be > 0");
        }
        if capacity > MAX_SUBSCRIBER_QUEUE {
            anyhow::bail!("event_buffer too large (max {MAX_SUBSCRIBER_QUEUE})");
        }
        Ok(Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            capacity,
        }))
    }

    fn lock_subs(&self) -> MutexGuard<'_, HashMap<String, Vec<SubSender>>> {
        self.subs.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("event registry mutex was poisoned; recovering registry state");
            poisoned.into_inner()
        })
    }

    /// Deliver `ev` to the live subscribers for its owner, if any.
    fn route(&self, ev: &Event) {
        let subs = self.lock_subs();
        if let Some(list) = subs.get(&ev.owner_id) {
            for s in list {
                // Non-blocking: never stall the publish path. A full or closed
                // queue drops the event; closed senders are reaped by the owning
                // Subscription's Drop, not here.
                let _ = s.tx.try_send(ev.clone());
            }
        }
    }

    /// Register a new subscription for `owner` and hand back its stream.
    fn register(self: &Arc<Self>, owner: &str) -> Subscription {
        let (tx, rx) = mpsc::channel(self.capacity);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.lock_subs()
            .entry(owner.to_string())
            .or_default()
            .push(SubSender { id, tx });
        Subscription {
            rx,
            registry: Arc::clone(self),
            owner: owner.to_string(),
            id,
        }
    }

    /// Drop one subscription's sender (from `Subscription::drop`), and the owner
    /// entry entirely once its last subscriber leaves, so the map cannot grow
    /// without bound as clients come and go.
    fn unregister(&self, owner: &str, id: u64) {
        let mut subs = self.lock_subs();
        if let Some(list) = subs.get_mut(owner) {
            list.retain(|s| s.id != id);
            if list.is_empty() {
                subs.remove(owner);
            }
        }
    }
}

/// A live per-owner event subscription. Yields the owner's `Event`s; on drop it
/// unregisters itself so a disconnected client leaves no dangling sender behind.
pub struct Subscription {
    rx: mpsc::Receiver<Event>,
    registry: Arc<Registry>,
    owner: String,
    id: u64,
}

impl Stream for Subscription {
    type Item = Event;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Subscription is Unpin (every field is), so poll the receiver directly.
        self.get_mut().rx.poll_recv(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.registry.unregister(&self.owner, self.id);
    }
}

/// Build a lazily-connecting peer channel and spawn its forwarder task, handing
/// back the queue sender. The channel reconnects under the hood, so a peer that
/// is temporarily down (or not yet scheduled) costs only dropped events until it
/// returns. Keepalive pings keep an otherwise-idle peer connection from being
/// reaped by a load balancer / conntrack table between replicas.
fn spawn_peer(endpoint_url: &str) -> anyhow::Result<mpsc::Sender<Event>> {
    let channel = Endpoint::from_shared(endpoint_url.to_string())
        .map_err(|e| anyhow::anyhow!("invalid peer endpoint {endpoint_url}: {e}"))?
        .timeout(PEER_RPC_TIMEOUT)
        .connect_timeout(PEER_RPC_TIMEOUT)
        .http2_keep_alive_interval(PEER_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(PEER_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect_lazy();
    let (ptx, prx) = mpsc::channel(PEER_QUEUE);
    tokio::spawn(peer_forwarder(channel, prx));
    Ok(ptx)
}

impl Broadcaster {
    pub fn new(capacity: usize, peer_endpoints: &[String]) -> anyhow::Result<Self> {
        let registry = Registry::new(capacity)?;
        let mut static_peer_txs = Vec::new();
        for ep in peer_endpoints {
            static_peer_txs.push(spawn_peer(ep)?);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                registry,
                static_peer_txs,
                dynamic_peer_txs: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Replace the dynamically discovered peer set with `endpoints` (typically
    /// the result of resolving a headless-Service DNS name, with this instance's
    /// own address excluded). Adds a forwarder for each endpoint not already
    /// present and drops the sender for any that disappeared — so the live
    /// forwarder set always matches the current replica membership. Call only
    /// with a successfully resolved set; on a resolution error skip the call to
    /// preserve the current peers rather than tearing them all down.
    pub fn reconcile_dynamic_peers(&self, endpoints: &[String]) {
        let mut peers = self
            .inner
            .dynamic_peer_txs
            .lock()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("dynamic peer map mutex was poisoned; recovering");
                poisoned.into_inner()
            });

        // Drop forwarders for peers no longer present (dropping the sender ends
        // the task).
        peers.retain(|url, _| endpoints.iter().any(|e| e == url));

        // Add forwarders for newly seen peers.
        for ep in endpoints {
            if peers.contains_key(ep) {
                continue;
            }
            match spawn_peer(ep) {
                Ok(tx) => {
                    peers.insert(ep.clone(), tx);
                }
                Err(e) => {
                    // We build these URLs from resolved IPs, so this is unexpected;
                    // log and skip the one bad entry rather than failing the sweep.
                    tracing::warn!(endpoint = %ep, error = %e, "skipping invalid discovered peer");
                }
            }
        }
    }

    /// Register a `Subscribe` stream for `owner`; it receives only that owner's
    /// events.
    pub fn subscribe(&self, owner: &str) -> Subscription {
        self.inner.registry.register(owner)
    }

    /// Publish an event that originated on this instance: deliver locally and
    /// enqueue to each peer's forwarder (best-effort, non-blocking).
    pub fn publish_local(&self, ev: Event) {
        self.inner.registry.route(&ev);
        // try_send: if a peer's queue is full we drop rather than block the
        // request path; the client recheck timer is the correctness backstop.
        for ptx in &self.inner.static_peer_txs {
            let _ = ptx.try_send(ev.clone());
        }
        // Brief lock; try_send is non-blocking so we hold it only across cheap
        // enqueues, never an await. Reconciliation takes the same lock rarely.
        let peers = self
            .inner
            .dynamic_peer_txs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for ptx in peers.values() {
            let _ = ptx.try_send(ev.clone());
        }
    }

    /// Publish an event forwarded from a peer: deliver locally only (do not
    /// re-forward, which would loop).
    pub fn publish_from_peer(&self, ev: Event) {
        self.inner.registry.route(&ev);
    }

    pub fn released(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Released as i32,
            owner_id: owner.to_string(),
        });
    }

    pub fn killed(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Killed as i32,
            owner_id: owner.to_string(),
        });
    }

    pub fn revoke(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Revoke as i32,
            owner_id: owner.to_string(),
        });
    }
}

/// One long-lived task per peer: drains its bounded queue and forwards each
/// event via `PublishEvent`. The lazy channel reconnects under the hood; a
/// failed or timed-out send is dropped (best-effort). The task ends when the
/// `Broadcaster` (and thus the sender) is dropped.
async fn peer_forwarder(channel: Channel, mut rx: mpsc::Receiver<Event>) {
    let mut client = PathLockClient::new(channel);
    while let Some(ev) = rx.recv().await {
        let owner_id = ev.owner_id.clone();
        let event_type = ev.r#type;
        if let Err(e) = client
            .publish_event(PublishEventRequest { event: Some(ev) })
            .await
        {
            tracing::debug!(
                owner_id = %owner_id,
                event_type,
                error = %e,
                "peer event forward failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcaster_rejects_invalid_subscriber_queue_sizes() {
        assert!(Broadcaster::new(0, &[]).is_err());
        assert!(Broadcaster::new(MAX_SUBSCRIBER_QUEUE + 1, &[]).is_err());
        assert!(Broadcaster::new(1, &[]).is_ok());
    }

    #[tokio::test]
    async fn reconcile_dynamic_peers_adds_and_removes() {
        let b = Broadcaster::new(8, &[]).unwrap();
        // Lazy channels: these never actually connect in the test, so the
        // forwarder tasks just park on an empty queue.
        b.reconcile_dynamic_peers(&[
            "http://10.0.0.1:50051".into(),
            "http://10.0.0.2:50051".into(),
        ]);
        assert_eq!(b.inner.dynamic_peer_txs.lock().unwrap().len(), 2);

        // A subsequent sweep is a full replace: drop the vanished peer, keep the
        // surviving one, add the new one.
        b.reconcile_dynamic_peers(&[
            "http://10.0.0.2:50051".into(),
            "http://10.0.0.3:50051".into(),
        ]);
        let peers = b.inner.dynamic_peer_txs.lock().unwrap();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains_key("http://10.0.0.2:50051"));
        assert!(peers.contains_key("http://10.0.0.3:50051"));
        assert!(!peers.contains_key("http://10.0.0.1:50051"));
    }
}
