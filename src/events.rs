//! Event fan-out for the per-owner lifecycle stream (release / kill / revoke).
//!
//! Within a single instance, each `Subscribe` stream registers a bounded mpsc
//! sender in a per-owner registry keyed by owner id. Events are routed only
//! to the senders registered for *that* owner.
//!
//! Across instances, an event is best-effort forwarded to every peer's
//! `PublishEvent` RPC. The client-side recheck timer is the correctness
//! backstop.
//!
//! The peer set has two sources, unioned: a fixed list and a dynamically
//! discovered set via DNS resolution of a headless Service name.

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

const PEER_QUEUE: usize = 1024;
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const PEER_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SUBSCRIBER_QUEUE: usize = 1_000_000;

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Arc<Registry>,
    static_peer_txs: Vec<mpsc::Sender<Event>>,
    dynamic_peer_txs: Mutex<HashMap<String, mpsc::Sender<Event>>>,
}

struct Registry {
    subs: Mutex<HashMap<String, Vec<SubSender>>>,
    next_id: AtomicU64,
    capacity: usize,
}

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
        self.subs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn route(&self, ev: &Event) {
        let subs = self.lock_subs();
        if let Some(list) = subs.get(&ev.owner_id) {
            for s in list {
                let _ = s.tx.try_send(ev.clone());
            }
        }
    }

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

pub struct Subscription {
    rx: mpsc::Receiver<Event>,
    registry: Arc<Registry>,
    owner: String,
    id: u64,
}

impl Stream for Subscription {
    type Item = Event;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.registry.unregister(&self.owner, self.id);
    }
}

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

    pub fn reconcile_dynamic_peers(&self, endpoints: &[String]) {
        let mut peers = self
            .inner
            .dynamic_peer_txs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        peers.retain(|url, _| endpoints.iter().any(|e| e == url));
        for ep in endpoints {
            if peers.contains_key(ep) {
                continue;
            }
            match spawn_peer(ep) {
                Ok(tx) => {
                    peers.insert(ep.clone(), tx);
                }
                Err(e) => {
                    tracing::warn!(endpoint = %ep, error = %e, "skipping invalid discovered peer");
                }
            }
        }
    }

    pub fn subscribe(&self, owner: &str) -> Subscription {
        self.inner.registry.register(owner)
    }

    pub fn publish_local(&self, ev: Event) {
        self.inner.registry.route(&ev);
        for ptx in &self.inner.static_peer_txs {
            let _ = ptx.try_send(ev.clone());
        }
        let peers = self
            .inner
            .dynamic_peer_txs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for ptx in peers.values() {
            let _ = ptx.try_send(ev.clone());
        }
    }

    pub fn publish_from_peer(&self, ev: Event) {
        self.inner.registry.route(&ev);
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

    pub fn grant(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Grant as i32,
            owner_id: owner.to_string(),
        });
    }
}

async fn peer_forwarder(channel: Channel, mut rx: mpsc::Receiver<Event>) {
    let mut client = PathLockClient::new(channel);
    while let Some(ev) = rx.recv().await {
        let owner_id = ev.owner_id.clone();
        let event_type = ev.r#type;
        if let Err(e) = client
            .publish_event(PublishEventRequest { event: Some(ev) })
            .await
        {
            tracing::debug!(owner_id = %owner_id, event_type, error = %e, "peer event forward failed");
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
        b.reconcile_dynamic_peers(&[
            "http://10.0.0.1:50051".into(),
            "http://10.0.0.2:50051".into(),
        ]);
        assert_eq!(b.inner.dynamic_peer_txs.lock().unwrap().len(), 2);
        b.reconcile_dynamic_peers(&[
            "http://10.0.0.2:50051".into(),
            "http://10.0.0.3:50051".into(),
        ]);
        let peers = b.inner.dynamic_peer_txs.lock().unwrap();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains_key("http://10.0.0.2:50051"));
        assert!(peers.contains_key("http://10.0.0.3:50051"));
    }
}
