//! Event fan-out for the per-owner lifecycle stream (release / kill / revoke).
//!
//! Within a single instance, events go onto a tokio broadcast channel; each
//! `Subscribe` stream filters it down to its own owner id. Across instances, an
//! event is best-effort forwarded to configured peers' `PublishEvent` RPC so an
//! event raised on instance A reaches the owner's subscription on instance B.
//! The client-side recheck timer is the correctness backstop, so a dropped peer
//! message only costs latency, never safety.
//!
//! Peer fan-out uses one long-lived forwarder task per peer draining a bounded
//! queue (not a task per event), so a slow or dead peer can neither pile up
//! tasks nor stall the request path: a full queue simply drops the event, and
//! each forward RPC carries a timeout.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tonic::transport::{Channel, Endpoint};

use crate::proto::{path_lock_client::PathLockClient, Event, EventType, PublishEventRequest};

/// Per-peer forwarder queue depth. Events are tiny and infrequent; if a peer is
/// slow enough to fill this, we drop (the client-side recheck is the backstop).
const PEER_QUEUE: usize = 1024;
/// Timeout applied to each peer `PublishEvent` RPC (connect and per-call).
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<Inner>,
}

struct Inner {
    tx: broadcast::Sender<Event>,
    peer_txs: Vec<mpsc::Sender<Event>>,
}

impl Broadcaster {
    pub fn new(capacity: usize, peer_endpoints: &[String]) -> anyhow::Result<Self> {
        let (tx, _rx) = broadcast::channel(capacity.max(16));
        let mut peer_txs = Vec::new();
        for ep in peer_endpoints {
            let endpoint = Endpoint::from_shared(ep.clone())
                .map_err(|e| anyhow::anyhow!("invalid peer endpoint {ep}: {e}"))?
                .timeout(PEER_RPC_TIMEOUT)
                .connect_timeout(PEER_RPC_TIMEOUT);
            let channel = endpoint.connect_lazy();
            let (ptx, prx) = mpsc::channel(PEER_QUEUE);
            tokio::spawn(peer_forwarder(channel, prx));
            peer_txs.push(ptx);
        }
        Ok(Self {
            inner: Arc::new(Inner { tx, peer_txs }),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.inner.tx.subscribe()
    }

    /// Publish an event that originated on this instance: deliver locally and
    /// enqueue to each peer's forwarder (best-effort, non-blocking).
    pub fn publish_local(&self, ev: Event) {
        // Local subscribers (ignore "no receivers").
        let _ = self.inner.tx.send(ev.clone());
        for ptx in &self.inner.peer_txs {
            // try_send: if a peer's queue is full we drop rather than block the
            // request path; the client recheck timer is the correctness backstop.
            let _ = ptx.try_send(ev.clone());
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

/// One long-lived task per peer: drains its bounded queue and forwards each
/// event via `PublishEvent`. The lazy channel reconnects under the hood; a
/// failed or timed-out send is dropped (best-effort). The task ends when the
/// `Broadcaster` (and thus the sender) is dropped.
async fn peer_forwarder(channel: Channel, mut rx: mpsc::Receiver<Event>) {
    let mut client = PathLockClient::new(channel);
    while let Some(ev) = rx.recv().await {
        let _ = client
            .publish_event(PublishEventRequest { event: Some(ev) })
            .await;
    }
}
