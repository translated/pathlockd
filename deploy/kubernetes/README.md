# Deploying pathlockd replicated on Kubernetes

This directory holds the manifests for running pathlockd as a **replicated, HA**
service. Replicated mode is supported **on Kubernetes** (or any platform that
exposes individually-addressable replicas behind a headless/stable-DNS service).
A single instance runs anywhere; multi-instance event fan-out specifically needs
what a Kubernetes **StatefulSet + headless Service** provides — see
[Why Kubernetes](#why-kubernetes) below.

## Prerequisites

- A **replicated TiKV cluster** (≥3 PD, ≥3 TiKV). The easiest path is
  [`tikv-operator`](https://tikv.org/docs/latest/deploy/install/install-on-kubernetes/);
  it gives you a PD Service to point `PATHLOCKD_PD_ENDPOINTS` at.
- The pathlockd container image (`ghcr.io/alexpacio/pathlockd:<tag>`).

## Apply

```sh
# Edit pathlockd-statefulset.yaml first: set PATHLOCKD_PD_ENDPOINTS to your PD
# Service/endpoints and pin the image tag.
kubectl apply -f pathlockd-services.yaml
kubectl apply -f pathlockd-statefulset.yaml
kubectl apply -f pathlockd-pdb.yaml
```

Clients connect to the `pathlockd` ClusterIP Service on port 50051.

## Files

| File | What it is |
| --- | --- |
| `pathlockd-services.yaml` | `pathlockd-headless` (headless, for peer discovery + stable pod DNS) and `pathlockd` (ClusterIP, for client traffic) |
| `pathlockd-statefulset.yaml` | the replicated pathlockd StatefulSet (stateless pods; StatefulSet is used for stable network identity only) |
| `pathlockd-pdb.yaml` | a PodDisruptionBudget keeping a quorum available during node drains |

## Scaling

```sh
kubectl scale statefulset/pathlockd --replicas=5
```

No restart or config change is needed: each replica periodically re-resolves the
headless Service (`PATHLOCKD_PEER_DISCOVERY_DNS`) and reconciles its peer
fan-out set, so new replicas are picked up and removed ones are dropped
automatically. (You can also drive this with an HPA.)

## Why Kubernetes

pathlockd's lock state is entirely in TiKV, so any replica can serve any
request — that part works behind any load balancer. The one thing that needs
more is the **per-owner event stream** (`Subscribe` → `released`/`killed`/
`revoke`): an event is raised on whichever replica handled the request (often a
*different* replica than the one holding the subscriber, e.g. a deadlock
`RequestRevoke` or an admin `ForceRelease` targeting another owner). To deliver
it, the originating replica must forward to the **specific** replica holding the
subscription — which means every replica must be individually addressable, and
each must know the current set of its peers.

- A **single load-balanced VIP** (a plain Deployment + ClusterIP, or a Docker
  Swarm replicated service reached via its VIP / `tasks.` DNS) cannot do this: a
  forwarded event load-balances to *one* replica, not a fan-out to all, so the
  subscriber usually misses it.
- A **StatefulSet + headless Service** gives each pod a stable identity and a
  DNS name that resolves to *all* pod IPs. pathlockd resolves that name and runs
  one forwarder per peer, so an event reaches every replica.

Cross-instance fan-out is best-effort and a latency optimization only: the
client-side recheck poll is always the correctness backstop, so even if fan-out
is misconfigured the locks stay correct — you just fall back to poll latency for
wakeups. Running replicated on Kubernetes is what makes the low-latency event
path actually work.
