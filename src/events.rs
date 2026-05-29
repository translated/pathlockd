//! Event fan-out for the per-owner lifecycle stream (release / kill / revoke).
//!
//! Within a single instance, events go onto a tokio broadcast channel; each
//! `Subscribe` stream filters it down to its own owner id. Across instances, an
//! event is best-effort forwarded to configured peers' `PublishEvent` RPC so an
//! event raised on instance A reaches the owner's subscription on instance B.
//! The client-side recheck timer is the correctness backstop, so a dropped peer
//! message only costs latency, never safety.

use std::sync::Arc;

use tokio::sync::broadcast;
use tonic::transport::{Channel, Endpoint};

use crate::proto::{path_lock_client::PathLockClient, Event, EventType, PublishEventRequest};

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<Inner>,
}

struct Inner {
    tx: broadcast::Sender<Event>,
    peers: Vec<Channel>,
}

impl Broadcaster {
    pub fn new(capacity: usize, peer_endpoints: &[String]) -> anyhow::Result<Self> {
        let (tx, _rx) = broadcast::channel(capacity.max(16));
        let mut peers = Vec::new();
        for ep in peer_endpoints {
            let endpoint = Endpoint::from_shared(ep.clone())
                .map_err(|e| anyhow::anyhow!("invalid peer endpoint {ep}: {e}"))?;
            peers.push(endpoint.connect_lazy());
        }
        Ok(Self {
            inner: Arc::new(Inner { tx, peers }),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.inner.tx.subscribe()
    }

    /// Publish an event that originated on this instance: deliver locally and
    /// fan out to peers.
    pub fn publish_local(&self, ev: Event) {
        // Local subscribers (ignore "no receivers").
        let _ = self.inner.tx.send(ev.clone());
        // Best-effort, non-blocking peer fan-out.
        for ch in &self.inner.peers {
            let ch = ch.clone();
            let ev = ev.clone();
            tokio::spawn(async move {
                let mut client = PathLockClient::new(ch);
                let _ = client
                    .publish_event(PublishEventRequest { event: Some(ev) })
                    .await;
            });
        }
    }

    /// Publish an event forwarded from a peer: deliver locally only (do not
    /// re-forward, which would loop).
    pub fn publish_from_peer(&self, ev: Event) {
        let _ = self.inner.tx.send(ev);
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
