//! SWIM membership via foca: node discovery, failure suspicion, and the live
//! member catalog.
//!
//! SWIM provides *hints only* — which nodes exist, their addresses, and
//! whether they look alive. Correctness (group membership, lock state, owner
//! liveness) is always decided by Raft; the membership controller consumes
//! this catalog merely to decide what to reconcile next.
//!
//! Wiring: one task owns the UDP socket and the sans-io [`foca::Foca`]
//! instance, driving it from three inputs — received datagrams
//! (`handle_data`), scheduled timers (`handle_timer`), and a periodic
//! `gossip()` tick — and draining the accumulated outputs (datagrams to
//! send, timers to schedule, membership notifications) after every step.
//! Members are published to a `watch` channel as a map of numeric node id →
//! identity.

use std::collections::{BTreeMap, BinaryHeap};
use std::net::SocketAddr;
use std::sync::Arc;

use foca::{BincodeCodec, Foca, NoCustomBroadcast, Timer};
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::raft::types::NodeMeta;

/// SWIM identity: who a node is plus everything peers need to reach it.
/// `incarnation` makes re-joins win address conflicts against stale entries.
///
/// The identity's cluster-wide `Addr` is the **advertised** `ip:port` gossip
/// address (`meta.gossip_addr`): joining works by announcing to a seed's
/// resolved address, and foca only accepts announces whose destination `Addr`
/// matches the receiver's own — an address is the one thing both sides can
/// agree on before they have exchanged identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub node_id: u64,
    pub meta: NodeMeta,
    pub incarnation: u64,
}

impl foca::Identity for NodeIdentity {
    type Addr = String;

    fn addr(&self) -> String {
        self.meta.gossip_addr.clone()
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.incarnation > adversary.incarnation
    }

    fn renew(&self) -> Option<Self> {
        Some(Self {
            node_id: self.node_id,
            meta: self.meta.clone(),
            incarnation: self.incarnation + 1,
        })
    }
}

/// Live cluster members as currently believed by SWIM (self included).
pub type MemberMap = BTreeMap<u64, NodeIdentity>;

#[derive(Clone)]
pub struct ClusterMembers {
    rx: watch::Receiver<MemberMap>,
    local: NodeIdentity,
}

impl ClusterMembers {
    /// Subscribe to membership updates.
    pub fn watch(&self) -> watch::Receiver<MemberMap> {
        self.rx.clone()
    }

    /// Current snapshot of live members.
    pub fn snapshot(&self) -> MemberMap {
        self.rx.borrow().clone()
    }

    pub fn local(&self) -> &NodeIdentity {
        &self.local
    }
}

type FocaInstance =
    Foca<NodeIdentity, BincodeCodec<bincode::config::Configuration>, StdRng, NoCustomBroadcast>;

/// How often the node proactively gossips its member view.
const GOSSIP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// How often seed nodes are (re-)announced while the member view is lonely.
const ANNOUNCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_DATAGRAM: usize = 1400;

/// Start the SWIM layer: bind the UDP socket, join via `seed_nodes`
/// (`host:port` gossip addresses, DNS-resolvable), and publish the member
/// catalog.
pub async fn start_gossip(
    local: NodeIdentity,
    bind_addr: SocketAddr,
    seed_nodes: Vec<String>,
) -> anyhow::Result<ClusterMembers> {
    let socket = Arc::new(
        UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| anyhow::anyhow!("binding gossip socket {bind_addr}: {e}"))?,
    );
    info!(%bind_addr, node_id = local.node_id, "gossip listening");

    let mut initial = MemberMap::new();
    initial.insert(local.node_id, local.clone());
    let (tx, rx) = watch::channel(initial);

    let config = foca::Config::new_lan(std::num::NonZeroU32::new(32).expect("nonzero"));
    let foca: FocaInstance = Foca::new(
        local.clone(),
        config,
        StdRng::from_os_rng(),
        BincodeCodec(bincode::config::standard()),
    );

    tokio::spawn(gossip_loop(foca, socket, tx, local.clone(), seed_nodes));

    Ok(ClusterMembers { rx, local })
}

/// Resolve a seed's gossip address. The seed's node id and metadata are
/// unknown until it answers, so the announce target is a placeholder identity
/// whose `Addr` (the resolved `ip:port`) matches the seed's advertised
/// address — that match is what makes the seed accept the announce; the real
/// identity arrives with its feed reply. Placeholder ids sit far outside the
/// ordinal range and are filtered from the published catalog.
async fn resolve_seed(seed: &str) -> Option<(SocketAddr, NodeIdentity)> {
    let mut addrs = tokio::net::lookup_host(seed).await.ok()?;
    let addr = addrs.next()?;
    Some((
        addr,
        NodeIdentity {
            node_id: u64::MAX ^ seed_hash(seed),
            meta: NodeMeta {
                name: format!("seed:{seed}"),
                raft_addr: String::new(),
                public_addr: String::new(),
                gossip_addr: addr.to_string(),
            },
            incarnation: 0,
        },
    ))
}

fn seed_hash(seed: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(seed.as_bytes()) >> 16
}

/// The address this node advertises in its SWIM identity, raft membership,
/// and event fan-out: explicit override > specified bind IP > auto-detected
/// outbound IP (the UDP-connect trick — no packet is actually sent).
pub fn advertised_addr(
    bind: SocketAddr,
    advertise_override: Option<&str>,
    probe_target: Option<&SocketAddr>,
) -> anyhow::Result<SocketAddr> {
    if let Some(advertised) = advertise_override {
        return advertised
            .parse()
            .map_err(|e| anyhow::anyhow!("advertise addr {advertised}: {e}"));
    }
    if !bind.ip().is_unspecified() {
        return Ok(bind);
    }
    let probe = probe_target
        .copied()
        .unwrap_or_else(|| "8.8.8.8:53".parse().expect("static addr"));
    let sock = std::net::UdpSocket::bind(("0.0.0.0", 0))
        .map_err(|e| anyhow::anyhow!("probing local address: {e}"))?;
    sock.connect(probe)
        .map_err(|e| anyhow::anyhow!("probing local address via {probe}: {e}"))?;
    let local = sock
        .local_addr()
        .map_err(|e| anyhow::anyhow!("probing local address: {e}"))?;
    Ok(SocketAddr::new(local.ip(), bind.port()))
}

struct PendingTimer {
    fire_at: tokio::time::Instant,
    timer: Timer<NodeIdentity>,
    seq: u64,
}

impl PartialEq for PendingTimer {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at == other.fire_at && self.seq == other.seq
    }
}
impl Eq for PendingTimer {}
impl PartialOrd for PendingTimer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PendingTimer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap by fire time (BinaryHeap is a max-heap).
        other
            .fire_at
            .cmp(&self.fire_at)
            .then(other.seq.cmp(&self.seq))
    }
}

async fn gossip_loop(
    mut foca: FocaInstance,
    socket: Arc<UdpSocket>,
    tx: watch::Sender<MemberMap>,
    local: NodeIdentity,
    seed_nodes: Vec<String>,
) {
    let mut runtime = foca::AccumulatingRuntime::new();
    let mut timers: BinaryHeap<PendingTimer> = BinaryHeap::new();
    let mut timer_seq = 0u64;
    // Gossip-address book for datagram delivery (node id → socket addr).
    let mut addr_book: BTreeMap<u64, SocketAddr> = BTreeMap::new();

    let mut recv_buf = vec![0u8; MAX_DATAGRAM];
    let mut gossip_tick = tokio::time::interval(GOSSIP_INTERVAL);
    gossip_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut announce_tick = tokio::time::interval(ANNOUNCE_INTERVAL);
    announce_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let next_timer = timers.peek().map(|t| t.fire_at);
        tokio::select! {
            recv = socket.recv_from(&mut recv_buf) => {
                match recv {
                    Ok((len, _from)) => {
                        if let Err(e) = foca.handle_data(&recv_buf[..len], &mut runtime) {
                            debug!(error = %e, "gossip: bad datagram");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "gossip: socket recv failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
            _ = async {
                match next_timer {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(pending) = timers.pop() {
                    if let Err(e) = foca.handle_timer(pending.timer, &mut runtime) {
                        debug!(error = %e, "gossip: timer handling failed");
                    }
                }
            }
            _ = gossip_tick.tick() => {
                if let Err(e) = foca.gossip(&mut runtime) {
                    debug!(error = %e, "gossip: round failed");
                }
            }
            _ = announce_tick.tick() => {
                // Lonely (only self known): (re-)announce to the seeds.
                if foca.num_members() <= 1 {
                    for seed in &seed_nodes {
                        if let Some((addr, target)) = resolve_seed(seed).await {
                            // Never announce to ourselves.
                            if addr.to_string() == local.meta.gossip_addr {
                                continue;
                            }
                            addr_book.insert(target.node_id, addr);
                            if let Err(e) = foca.announce(target, &mut runtime) {
                                debug!(seed = %seed, error = %e, "gossip: announce failed");
                            }
                        }
                    }
                }
            }
        }

        // Drain the runtime: sends, new timers, notifications.
        while let Some((to, data)) = runtime.to_send() {
            let dest = resolve_member_addr(&to, &mut addr_book).await;
            if let Some(dest) = dest {
                if let Err(e) = socket.send_to(&data, dest).await {
                    debug!(to = %dest, error = %e, "gossip: send failed");
                }
            }
        }
        while let Some((after, timer)) = runtime.to_schedule() {
            timer_seq += 1;
            timers.push(PendingTimer {
                fire_at: tokio::time::Instant::now() + after,
                timer,
                seq: timer_seq,
            });
        }
        let mut membership_changed = false;
        while let Some(notification) = runtime.to_notify() {
            use foca::OwnedNotification;
            membership_changed |= matches!(
                notification,
                OwnedNotification::MemberUp(_)
                    | OwnedNotification::MemberDown(_)
                    | OwnedNotification::Rename(_, _)
            );
            match &notification {
                OwnedNotification::MemberUp(id) => {
                    info!(node = id.node_id, name = %id.meta.name, "gossip: member up")
                }
                OwnedNotification::MemberDown(id) => {
                    info!(node = id.node_id, name = %id.meta.name, "gossip: member down")
                }
                OwnedNotification::Rename(old, new) => {
                    debug!(
                        node = new.node_id,
                        old_inc = old.incarnation,
                        new_inc = new.incarnation,
                        "gossip: member renamed"
                    )
                }
                _ => {}
            }
        }
        if membership_changed {
            let mut members: MemberMap = foca
                .iter_members()
                .map(|m| (m.id().node_id, m.id().clone()))
                .filter(|(id, ident)| !is_placeholder(*id, ident))
                .collect();
            members.insert(local.node_id, local.clone());
            for member in members.values() {
                if let Ok(addr) = member.meta.gossip_addr.parse() {
                    addr_book.insert(member.node_id, addr);
                }
            }
            let count = members.len();
            if tx.send(members).is_err() {
                info!("gossip: no subscribers left; stopping");
                return;
            }
            debug!(members = count, "gossip: membership updated");
        }
    }
}

fn is_placeholder(id: u64, ident: &NodeIdentity) -> bool {
    // Seed placeholders carry no raft address and ids far outside the
    // ordinal range.
    id > u32::MAX as u64 && ident.meta.raft_addr.is_empty()
}

async fn resolve_member_addr(
    to: &NodeIdentity,
    addr_book: &mut BTreeMap<u64, SocketAddr>,
) -> Option<SocketAddr> {
    if let Ok(addr) = to.meta.gossip_addr.parse() {
        addr_book.insert(to.node_id, addr);
        return Some(addr);
    }
    // Placeholder identities (seeds) resolve via DNS once and are cached.
    if let Some(addr) = addr_book.get(&to.node_id) {
        return Some(*addr);
    }
    let host = to.meta.gossip_addr.clone();
    if host.is_empty() {
        return None;
    }
    let resolved = tokio::net::lookup_host(host.as_str()).await.ok()?.next()?;
    addr_book.insert(to.node_id, resolved);
    Some(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use foca::Identity;

    fn identity(node_id: u64, incarnation: u64) -> NodeIdentity {
        NodeIdentity {
            node_id,
            meta: NodeMeta {
                name: format!("pathlockd-{node_id}"),
                raft_addr: format!("http://10.0.0.{node_id}:50052"),
                public_addr: format!("http://10.0.0.{node_id}:50051"),
                gossip_addr: format!("10.0.0.{node_id}:7946"),
            },
            incarnation,
        }
    }

    #[test]
    fn identity_renewal_bumps_incarnation_and_wins_conflicts() {
        let old = identity(3, 0);
        let renewed = old.renew().expect("renewable");
        assert_eq!(renewed.node_id, old.node_id);
        assert_eq!(renewed.incarnation, 1);
        assert!(renewed.win_addr_conflict(&old));
        assert!(!old.win_addr_conflict(&renewed));
    }

    #[tokio::test]
    async fn two_nodes_discover_each_other() {
        let addr_a: SocketAddr = "127.0.0.1:17946".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:17947".parse().unwrap();
        let mut id_a = identity(1, 0);
        id_a.meta.gossip_addr = addr_a.to_string();
        let mut id_b = identity(2, 0);
        id_b.meta.gossip_addr = addr_b.to_string();

        let members_a = start_gossip(id_a, addr_a, vec![]).await.unwrap();
        let members_b = start_gossip(id_b, addr_b, vec![addr_a.to_string()])
            .await
            .unwrap();

        let mut rx = members_b.watch();
        let found = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                if rx.borrow().len() >= 2 {
                    return true;
                }
                if rx.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(found, "node B must discover node A via the seed announce");
        // A also learns about B.
        let mut rx_a = members_a.watch();
        let found_a = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                if rx_a.borrow().len() >= 2 {
                    return true;
                }
                if rx_a.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(found_a, "node A must learn about node B");
    }
}
