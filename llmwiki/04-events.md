# Events & deadlock resolution

## Per-owner streams (`src/events.rs`, `Subscribe` in `src/service.rs`)

`Subscribe(owner_id)` opens a server stream bound to exactly one owner. The
daemon runs an in-process `broadcast` channel; the stream filters it so the
subscriber receives **only** events whose `owner_id` matches — its own
`grant`, `killed`, or `revoke`. Nothing about any other owner is delivered. A
lock's channel therefore carries only that lock's information.

Events are raised at the point ownership changes:

- `GRANT:<owner>` — the owner's queued acquire became grantable (granted in
  place by a release/force-release/GC sweep, or it must refresh a stale fencing
  token and retry). This is the waiter's wake signal.
- `KILLED:<owner>` — on `force_release`.
- `REVOKE:<owner>` — on `request_revoke` (a *request* to the owner to yield; it
  changes no state itself).

There is no `RELEASED` event: a release is only ever observed by the releasing
owner itself, which never needs it, so it was removed.

## Why scoped streams suffice for waiters

A contended acquire is **enqueued** (the server-side wait queue, see
[engine](03-engine.md)). When the contended path frees, the daemon grants the
queued waiter in place and pushes a `GRANT` to that waiter's own stream — so the
waiter *is* woken by an event for itself, even though it never sees the blocker's
release. A periodic `IsBlocking` recheck remains only as a coarse safety net for
an event lost in transit (failover, a dropped cross-instance forward), not as
the primary wake path.

## Deadlock resolution (client policy)

1. A waiter records a wait edge:
   `SetWaitEdge(self → blocker, conflict_path, reason)`.
2. The "leader" of a conflicting pair (lower owner id, by convention) calls
   `DetectCycle(blocker)`. If the returned chain comes back to the leader, it's
   a cycle. Edges carrying conflict metadata are re-checked while walking, so a
   live owner with a stale wait edge cannot create a false deadlock.
3. The leader resolves it: `RequestRevoke(victim)` and waits a grace period for
   the victim to yield; if the victim is still blocking and still alive, one more
   grace round; then `ForceRelease(victim)` as a last resort.
4. The victim, on receiving `REVOKE:victim` on its own stream, releases its locks
   and cancels itself.

The daemon supplies the *mechanism* (cycle walk, revoke event, force release);
the *policy* (who leads, grace timing) is the client's.

## Cross-instance fan-out (peers)

With multiple pathlockd replicas, an event is raised on the replica that handled
the request. If clients are sticky to one replica per owner (recommended — one
lock keeps one connection), the owner's subscription is on that same replica and
gets the event directly. Otherwise set `PATHLOCKD_PEERS` so each replica forwards
events to its peers' internal `PublishEvent` RPC. Forwarding is done by one
long-lived task per peer draining a bounded queue (with a per-RPC timeout): a
slow or dead peer can neither pile up tasks nor stall the request path — a full
queue just drops the event. The client recheck remains the correctness backstop,
so a missed forward only costs latency.
