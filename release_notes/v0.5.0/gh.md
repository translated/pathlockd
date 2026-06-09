Dynamic peer discovery for replicated deployments, Kubernetes manifests, and HTTP/2 keepalive.

## Changes

### Added: DNS-based dynamic peer discovery

Three new config fields enable elastic cross-instance event fan-out on
Kubernetes:

| Field | Env var | Default | Description |
| --- | --- | --- | --- |
| `peer_discovery_dns` | `PATHLOCKD_PEER_DISCOVERY_DNS` | none | `host:port` of a headless Service that resolves to every replica |
| `self_ip` | `PATHLOCKD_SELF_IP` | none | this instance's own IP, to exclude itself from discovered peers |
| `peer_refresh_secs` | `PATHLOCKD_PEER_REFRESH_SECS` | `10` | how often to re-resolve `peer_discovery_dns` |

When `peer_discovery_dns` is set, pathlockd periodically resolves the DNS name,
reconciles its dynamic peer forwarder set (adds forwarders for newly seen
replicas, drops forwarders for vanished ones), and excludes itself. Transient
resolution failures are logged and leave the current peer set in place; the
first refresh fires immediately on startup.

Set `self_ip` to this pod's IP (in Kubernetes, wire from the downward API
`status.podIP`) to avoid forwarding events to yourself. If unset, self-forwarding
is a wasted RPC but harmless.

Static `peers` and dynamic discovery are unioned: you can seed a fixed list and
let discovery handle the rest, or use discovery alone.

### Added: Kubernetes manifests (replicated HA)

A new `deploy/kubernetes/` directory ships ready-to-apply manifests for running
pathlockd as a replicated, HA StatefulSet:

| File | What it is |
| --- | --- |
| `pathlockd-services.yaml` | `pathlockd-headless` (headless, for peer discovery + stable pod DNS) and `pathlockd` (ClusterIP, for client traffic) |
| `pathlockd-statefulset.yaml` | the replicated pathlockd StatefulSet (stateless pods; StatefulSet is used for stable network identity) |
| `pathlockd-pdb.yaml` | a PodDisruptionBudget (`minAvailable: 2`) keeping a quorum during node drains |

Replicas scale elastically (`kubectl scale statefulset/pathlockd --replicas=5`)
— no restart or config change is needed because peer discovery tracks membership
automatically.

### Added: HTTP/2 and TCP keepalive

Server-side keepalive pings (`http2_keepalive_interval = 20 s`,
`http2_keepalive_timeout = 10 s`, `tcp_keepalive = 30 s`) prevent load balancers
and conntrack tables from silently reaping idle long-lived `Subscribe` streams.
Peer-side keepalive does the same for forwarder channels between replicas,
surfacing dead peers promptly so the lazy channel reconnects.

### Changed: peer forwarding now has static and dynamic sets

The broadcaster's peer set is now two parts unioned: a static list from config
(`PATHLOCKD_PEERS`) and a dynamically discovered set managed by
`reconcile_dynamic_peers()`. The forwarded-publish path is unchanged: both sets
are drained non-blockingly, and a full queue drops the event (the client-side
recheck poll is always the correctness backstop).

### Changed: documentation

- **README**: new deployment guidance distinguishing single-instance (any
  container runtime) from replicated (Kubernetes only), plus a new "Why
  replication needs Kubernetes" section explaining the fan-out constraint.
- **docker-compose.yml**: clarified to document that it runs a single instance
  and that scaling via Swarm's VIP degrades event wakeups to poll latency.
- **pathlockd.example.toml**: expanded with peer discovery fields and usage docs.
- Clock note corrected: lease expiry uses PD's timestamp oracle (cluster time),
  not host wall clock — replicas do not need mutually NTP-synced clocks.

## Upgrade note

The `pathlockd.v1.PathLock` API is unchanged. No TiKV keyspace migration is
required.

The three new config fields default off (`peer_discovery_dns` unset, `self_ip`
unset, `peer_refresh_secs = 10`) so a single-instance deployment upgrades with
no config changes. If you were using static `peers`, that path is unchanged.

To adopt replicated mode on Kubernetes, apply the manifests from
`deploy/kubernetes/` and set `PATHLOCKD_PD_ENDPOINTS` to your PD Service.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.5.0-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.5.0-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.5.0   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
