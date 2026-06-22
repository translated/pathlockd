# Using pathlockd to build a user-space virtual filesystem

This is a practical, end-to-end guide to coordinating a **user-space virtual
filesystem (VFS)** with `pathlockd`. It assumes you have a daemon reachable at
`localhost:50051` (see the [README quick start](../README.md#quick-start-development--playground))
and the contract in [`proto/pathlockd.proto`](../proto/pathlockd.proto).

pathlockd does **not** store your files. It coordinates *who* may touch which
part of a path tree, and hands you **fencing tokens** so a stale actor can be
detected by your backing store. Your VFS still does the real I/O (object store,
remote drive, DB rows); pathlockd makes that I/O safe under concurrency.

> The examples use [`grpcurl`](https://github.com/fullstorydev/grpcurl). The
> daemon does **not** enable server reflection, so pass the proto explicitly:
>
> ```bash
> export PL='grpcurl -plaintext -import-path ./proto -proto pathlockd.proto'
> $PL -d '{}' localhost:50051 pathlockd.v1.PathLock/Health
> ```
>
> A typed client also exists:
> [`pathlockd-nodejs-client`](https://github.com/alexpacio/pathlockd-nodejs-client).
> Orchestration loops below are written as language-agnostic pseudocode.

---

## 1. Mental model

| Concept | In your VFS |
|---|---|
| **Handler** | A backend prefix, e.g. `google_drive`, `s3`, `local`. The segment before the first `:` in a path. |
| **Routing namespace** | The shard/conflict domain. By default `google_drive:/team/report` routes by `google_drive:/team`; operators can define deeper roots such as `google_drive:/team/archive` with `SetNamespacePolicy`. |
| **Path** | `"<handler>:<normalizedPath>"`, e.g. `google_drive:/team/reports/q3.xlsx`. Must be rooted (`/…`), no trailing slash, no `//`, `.` or `..`. |
| **Owner** | One logical lock session = one owner id = ideally one connection. All paths you take under that owner id share **one lease**. Use a fresh UUID per operation (e.g. `mv-7f3a…`). |
| **Write lock** | Covers the whole **subtree**: `…:/a` conflicts with any lock on `/a`, on an ancestor of `/a`, or anywhere under `/a/…`. "This subtree is mine." |
| **Read lock** | **Point-only**: protects exactly one node. An ancestor read does *not* cover descendants. Many readers share a node; a writer on that exact node conflicts. |
| **Fencing token** | A monotonic integer stamped on each write-locked path. Prove you still hold it (`AssertFencing`) right before you mutate the backing store; a stale token is rejected. |
| **Lease (TTL)** | Every lock expires after `ttl_ms`. You **renew** to stay alive. Die without renewing → the lease lapses and the subtree frees itself. |
| **Events** | A per-owner stream delivering `GRANT` / `REVOKE` / `KILLED` for *that* owner: a waiter learns its queued acquire became grantable, and a holder learns it must yield or has been preempted. |

### The conflict matrix

| You request on `P` | Conflicts with an existing… |
|---|---|
| **write** `P` | write on `P`, write on any ancestor of `P`, any write in `P`'s subtree, read on `P`, any read in `P`'s subtree |
| **read** `P` | write on `P`, write on any ancestor of `P` (only — reads are point-only, so a write *below* `P` does not block a read on `P`, and vice-versa) |

Reasons you may see on a `CONFLICT`: `ancestor_locked`, `write_locked`,
`read_locked`, `descendant_write_locked`, `descendant_read_locked`,
`stale_fencing_token` (precedence is in that order).

---

## 2. The lock-session lifecycle (the core loop)

Every VFS operation that needs coordination follows the same shape:

```text
token   = IncrFencingToken()                         # once, per logical operation
owner   = "op-" + uuid()                             # one session
SUBSCRIBE(owner)              ── background ──▶  on KILLED: abort now (lease gone)
                                                on REVOKE: finish ASAP & release
loop:                                                # acquire-with-contention
    r = Acquire(owner, ttl_ms, requests=[…], fencing_token=token)
    if r.status == OK:        break
    if r.status == LOST:      restart/abort          # see §3
    if r.status == CONFLICT:  wait-or-fail(r)        # see §4

RENEW loop  ── background, every ttl_ms/3 ──▶  on LOST: abort, stop all I/O

# ... do the VFS work, calling AssertFencing before each backing-store mutation (§5) ...

ReleaseAll(owner, del_wait_key=true)                 # or Release(owner, [paths])
```

Key rules:

- **Pick `ttl_ms > 0`** (a `0` TTL is rejected — it would never expire) and
  `≤ 7 days`. Typical leases are seconds to a few minutes. Renew at roughly
  `ttl_ms / 3`.
- **Use a positive fencing token for every write.** Read-only acquires can pass
  `0`; write acquires and non-empty `AssertFencing` calls cannot.
- **Never touch the backing store after a `LOST` renew or a `KILLED` event** —
  your lease is gone and another worker may now own the subtree.
- **Always release** when done (or rely on lease expiry if you crash).

### Minimal happy path with grpcurl

```bash
# 1) mint a fencing token
$PL -d '{}' localhost:50051 pathlockd.v1.PathLock/IncrFencingToken
# → { "token": "42" }

# 2) acquire a write lock on a file (10s lease)
$PL -d '{
  "owner_id": "op-abc",
  "ttl_ms": "10000",
  "fencing_token": "42",
  "requests": [{ "path": "s3:/bucket/reports/q3.xlsx", "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW" }]
}' localhost:50051 pathlockd.v1.PathLock/Acquire
# → { "status": "ACQUIRE_STATUS_OK" }

# 3) renew (heartbeat) — repeat every ~3s
$PL -d '{ "owner_id": "op-abc", "ttl_ms": "10000" }' \
  localhost:50051 pathlockd.v1.PathLock/Renew
# → { "status": "RENEW_STATUS_OK" }

# 4) prove we still hold it at token 42, right before writing bytes to S3
$PL -d '{ "owner_id": "op-abc", "fencing_token": "42",
          "paths": ["s3:/bucket/reports/q3.xlsx"] }' \
  localhost:50051 pathlockd.v1.PathLock/AssertFencing
# → { "status": "ASSERT_STATUS_OK" }   (else ASSERT_STATUS_FAIL → abort)

# 5) release everything this owner holds
$PL -d '{ "owner_id": "op-abc", "del_wait_key": true }' \
  localhost:50051 pathlockd.v1.PathLock/ReleaseAll
```

---

## 3. Reacting to outcomes

`Acquire` returns one of:

| Status | Meaning | What to do |
|---|---|---|
| `OK` | All requested paths are yours. | Proceed. |
| `QUEUED` | A path is contended; your request was enqueued in FIFO order. `path`/`owner`/`reason` say what it is queued behind. | Wait for a `GRANT` event for your owner id, then proceed (or re-issue the acquire, which returns `OK` once granted). See §4. |
| `CONFLICT` | A non-waitable condition — chiefly `stale_fencing_token` (here `owner` holds the *persisted fence value*, not an owner id). | Refresh your fencing token (`IncrFencingToken`) and retry. |
| `LOST` | A `HELD` path you claimed to own is gone (`missing_write`/`missing_read`/`missing_fence`/`missing_alive`). | Your lease/lock lapsed. Stop, drop in-flight work, re-mint a token and re-acquire from scratch. |

`Renew` returns `OK` or `LOST`. `LOST` (`missing_alive`, `missing_owner_set`,
`missing_write`, `missing_fence`, `missing_read`, `empty_owner_set`) means the
lease is broken — **abort and stop all backing-store I/O immediately.**

All malformed inputs (empty `owner_id`, `ttl_ms == 0` or too large, a
non-normalized path) are rejected with gRPC `InvalidArgument`. Transient storage
contention surfaces as `Unavailable` — safe to retry with backoff.

---

## 4. Contention: waiting for a busy path

When you get `CONFLICT`, decide between **fail-fast** and **wait**. To wait
politely and detect deadlocks, register a wait edge and poll:

```text
on CONFLICT{path, owner: blocker, reason}:
    SetWaitEdge(owner=me, conflict_owner=blocker, ttl_ms=lease,
                conflict_path=path, reason=reason)   # "I wait for blocker on this conflict"
    repeat with backoff (e.g. 50ms → 1s):
        if not IsBlocking(conflict_path=path, conflict_owner=blocker, reason=reason):
            break                              # blocker let go → retry Acquire
        cyc = DetectCycle(start_owner_id=me, max_depth=64)
        if cyc.kind == FOUND:                  # deadlock through me → resolve (§ below)
            resolve_deadlock(cyc.chain)
    # retry Acquire; on OK:
    ClearWaitEdge(owner=me)
```

- `IsBlocking` is the cheap, authoritative recheck — it also prunes a dead
  reader/owner it finds, so it self-heals stale state.
- `DetectCycle(start=me)` walks `me → blocker → …`. It reports a cycle only if it
  comes back to **you**, so every participant that probes from its own id will
  find a shared deadlock. A wait edge pointing at a dead owner is GC'd during the
  walk; when the edge includes `conflict_path` + `reason`, a live-but-stale edge
  is also discarded before it can create a false cycle.
- Always `ClearWaitEdge` once you stop waiting (or fold it into the release with
  `del_wait_key: true`).

### Resolving a detected deadlock

You found a cycle `[me, B, C, me]`. Pick a victim (your policy — e.g. the
youngest, or lowest priority), then escalate cooperatively → forcibly:

```text
resolve_deadlock(chain):
    victim = choose_victim(chain)
    RequestRevoke(owner=victim)               # polite: victim gets a REVOKE event
    wait grace period (e.g. 1–2s), keep polling IsBlocking
    if still blocking and IsOwnerAlive(victim):
        ForceRelease(victim_id=victim)        # preempt: victim gets a KILLED event,
                                              # all its locks are dropped
```

The victim, subscribed to its own events (§6), should release on `REVOKE`; if it
is force-released it gets `KILLED` and **must stop touching the backing store** —
its fencing token is now stale and any late write will be rejected by
`AssertFencing` on the new holder's side anyway.

---

## 5. Fencing tokens — making stale writers harmless

The danger in any lease-based lock: a worker pauses (GC, network stall) past its
lease, the lease expires, a new worker takes over, then the old worker wakes up
and writes to the backing store — corrupting newer state. Fencing tokens make
that detectable.

Pattern:

1. `token = IncrFencingToken()` once per logical operation generation.
2. Pass `fencing_token: token` to `Acquire`. pathlockd stamps it on every
   write-locked path and **rejects a lower token** than one already persisted
   (`stale_fencing_token`) — so tokens only ever move forward per path.
3. **Immediately before each side-effecting write** to the real backend, call
   `AssertFencing(owner, token, paths)`. Only proceed on `ASSERT_STATUS_OK`.
   - `FAIL` with `stale_owner` → someone else now holds the path.
   - `FAIL` with `stale_fencing_token` → a newer generation took over.
4. Even better: pass the token *to the backend* if it supports conditional writes
   (e.g. an `If-Match`/version check), so the backend itself rejects a stale
   writer without trusting the clock.

```text
ok = AssertFencing(owner, token, ["s3:/bucket/reports/q3.xlsx"])
if ok.status != OK: abort()           # do NOT write
backend.put("…/q3.xlsx", bytes, fence=token)   # ideally fence-checked at the backend too
```

> Clocks: lease expiry uses each daemon's wall clock, so run replicas with
> synchronized clocks (NTP). Fencing is the backstop that keeps a clock skew or a
> too-eager expiry from causing corruption.

---

## 6. The event stream — yielding and preemption

Subscribe once per owner, on the same instance you hold locks on:

```bash
$PL -d '{ "owner_id": "op-abc" }' localhost:50051 pathlockd.v1.PathLock/Subscribe
# server stream of: { "type": "EVENT_TYPE_GRANT",  "owner_id": "op-abc" }
#                   { "type": "EVENT_TYPE_REVOKE", "owner_id": "op-abc" }
#                   { "type": "EVENT_TYPE_KILLED", "owner_id": "op-abc" }
```

Handle them:

- **`GRANT`** — your queued (`QUEUED`) acquire became grantable. Re-issue the
  acquire (it returns `OK`), or treat the event as your signal to proceed. If the
  re-acquire returns `stale_fencing_token`, refresh the token and retry.
- **`REVOKE`** — another worker asks you to yield (deadlock or priority). Finish
  the smallest safe unit of work, then `Release`/`ReleaseAll`. Cooperative.
- **`KILLED`** — you were force-released. Your locks are **already gone**. Stop
  all backing-store I/O at once; do not release (nothing to release); restart the
  operation from scratch if needed (new token).

> A subscription only ever sees **its own** owner's events — never another
> owner's. Events are best-effort (a `GRANT` may be missed if a peer forward
> drops); the lease and a coarse `IsBlocking` recheck are the correctness
> backstops, so events are the fast path, not a guarantee.

In a multi-replica deployment, keep an owner sticky to one replica (one lock =
one connection). If clients hop replicas, set `PATHLOCKD_PEERS` to the siblings'
internal Raft endpoints so events fan out.

---

## 7. VFS operations cookbook

Each recipe shows the lock(s) to take. Combine multiple paths in **one**
`Acquire` so they are granted atomically (all-or-nothing).

### Read / stat a single file → point read lock

```json
{ "owner_id": "rd-1", "ttl_ms": "5000", "fencing_token": "0",
  "requests": [{ "path": "gd:/team/q3.xlsx", "mode": "MODE_READ", "state": "LOCK_STATE_NEW" }] }
```
Many readers coexist. A writer on that exact file (or on an ancestor folder)
conflicts; a writer deeper in the tree does **not** (reads are point-only).
(Fencing tokens are irrelevant to reads — pass `0`.)

### List a directory → point read lock on the dir node

```json
{ "requests": [{ "path": "gd:/team", "mode": "MODE_READ", "state": "LOCK_STATE_NEW" }], "...": "" }
```
This protects the directory node itself, **not** its children — concurrent
writes to `gd:/team/<child>` are still allowed. If you need a consistent
*recursive* snapshot, take a **write** lock on `gd:/team` instead (heavier; it
locks the whole subtree).

### Create / write / upload a file → write lock on the file path

```json
{ "owner_id": "up-9", "ttl_ms": "30000", "fencing_token": "57",
  "requests": [{ "path": "s3:/bucket/a/b/new.bin", "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW" }] }
```
Conflicts if anyone holds the file, an ancestor folder (someone owns the parent
subtree), or anything under it. Then upload, `AssertFencing`, release.

### Rename / move a subtree → one write lock per side, atomically

Moving `gd:/team/old` to `gd:/team/new` means locking **both** subtrees so
nothing changes underneath you on either side. Request both in one `Acquire`:

```json
{ "owner_id": "mv-3", "ttl_ms": "30000", "fencing_token": "61",
  "requests": [
    { "path": "gd:/team/old", "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW" },
    { "path": "gd:/team/new", "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW" }
  ] }
```
A write lock on `…/old` covers `…/old/…`, and `…/new` covers `…/new/…`. After
`OK`: `AssertFencing` on both paths, perform the backend move, then `ReleaseAll`.
(Cross-handler moves work too — list paths from both handlers; they serialize
independently.)

### Delete a subtree → write lock on the subtree root

```json
{ "requests": [{ "path": "s3:/bucket/tmp/job-42", "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW" }], "...": "" }
```
The single write lock on the root covers everything beneath it.

### Reconcile / sync a subtree → hold a write lock for the duration

Take a write lock on the subtree root, run the reconcile, renew throughout,
release at the end. Use a longer `ttl_ms` (but still renew).

### Shadowing transition — promote children to an ancestor atomically

You already hold child write locks `…/s/a` and `…/s/b` and now want to operate on
the whole `…/s` subtree. Acquire the ancestor and **release the now-redundant
children in the same transaction** so there is never a gap:

```json
{ "owner_id": "op-abc", "ttl_ms": "30000", "fencing_token": "70",
  "requests":        [{ "path": "fs:/s",   "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW" }],
  "release_requests":[{ "path": "fs:/s/a", "mode": "MODE_WRITE" },
                      { "path": "fs:/s/b", "mode": "MODE_WRITE" }] }
```
The acquire and the child releases apply atomically; any waiter freed by the
inline release is granted in place and gets a `GRANT` event.

### Upgrading a read lock to a write lock

There is no in-place upgrade. Release the read and acquire the write (accepting
that another writer may race in between), or just acquire the write directly and
let the conflict machinery sort it out. Design for the re-check.

---

## 8. Leases, renewal, and crash recovery

- **Choose `ttl_ms`** for how long you can tolerate a crashed holder blocking the
  subtree. Shorter = faster recovery, more renew traffic. A few seconds to a
  minute is typical; renew at `ttl_ms/3`.
- **Renew loop** runs for the whole operation. On `RENEW_STATUS_LOST`, abort and
  stop I/O — do not assume you still hold anything.
- **Crash recovery is automatic**: a dead holder stops renewing, the lease
  lapses, and lazy expiry frees the subtree on the next touch (a background GC
  reclaims the bytes). Combined with fencing, a resurrected zombie worker cannot
  corrupt the new holder's state.

### Re-validating held locks with `LOCK_STATE_HELD`

When you re-acquire (e.g. adding a path to an existing session, or revalidating
after a hiccup), mark paths you believe you already hold as
`state: LOCK_STATE_HELD`. They are folded into the same atomic transaction, and
if any has silently vanished you get `LOST(missing_…)` instead of a false `OK` —
so you learn about lock loss atomically with the new acquisition.

```json
{ "owner_id": "op-abc", "ttl_ms": "30000", "fencing_token": "70",
  "requests": [
    { "path": "fs:/s",     "mode": "MODE_WRITE", "state": "LOCK_STATE_HELD" },
    { "path": "fs:/s/new", "mode": "MODE_WRITE", "state": "LOCK_STATE_NEW"  }
  ] }
```

---

## 9. Full worked example: atomic folder rename with deadlock handling

```text
token = IncrFencingToken()                 # → 61
owner = "mv-" + uuid()
start SUBSCRIBE(owner):
    on KILLED: abort(); on REVOKE: finish-and-release()

# acquire both subtrees, with polite waiting + deadlock resolution
loop:
    r = Acquire(owner, 30000, [
            (write, "gd:/team/old", NEW),
            (write, "gd:/team/new", NEW)],
        fencing_token=token)
    if r.status == OK: break
    if r.status == LOST: token = IncrFencingToken(); continue
    # CONFLICT:
    SetWaitEdge(owner, r.owner, 30000, conflict_path=r.path, reason=r.reason)
    until not IsBlocking(r.path, r.owner, r.reason):
        if DetectCycle(owner, 64).kind == FOUND:
            v = pick_victim(...)
            RequestRevoke(v); sleep(1s)
            if IsBlocking(r.path, r.owner, r.reason) and IsOwnerAlive(v):
                ForceRelease(v)
        backoff()
ClearWaitEdge(owner)

start RENEW(owner, 30000) every 10s; on LOST: abort()

# do the move, fenced
for p in ["gd:/team/old", "gd:/team/new"]:
    assert AssertFencing(owner, token, [p]).status == OK
backend.move("gd:/team/old", "gd:/team/new", fence=token)

ReleaseAll(owner, del_wait_key=true)       # frees both subtrees + clears wait edge
stop RENEW; stop SUBSCRIBE
```

---

## 10. RPC quick reference

| RPC | Use |
|---|---|
| `IncrFencingToken` | Mint a fresh monotonic token (once per operation generation). |
| `Acquire` | Take read/write locks (and optionally fold in releases). The workhorse. |
| `Renew` | Heartbeat the whole owner's lease. |
| `AssertFencing` | Prove you still hold paths at a token, just before backing-store writes. |
| `Release` / `ReleaseAll` | Give back specific paths / everything for an owner. |
| `IsBlocking` | Cheap recheck of one conflict (also prunes dead state). |
| `SetWaitEdge` / `ClearWaitEdge` | Record/clear "I wait for X" for deadlock detection. |
| `DetectCycle` | Walk the wait-for graph from your owner id. |
| `RequestRevoke` | Politely ask an owner to yield (sends it a `REVOKE`). |
| `ForceRelease` | Preempt an owner (drops its locks, sends it `KILLED`). |
| `IsOwnerAlive` | Is an owner's lease still live? |
| `Subscribe` | Stream `GRANT`/`REVOKE`/`KILLED` for one owner. |
| `Health` | Readiness (verifies internal state-machine liveness). |

For the exact message fields and enum values, see
[`proto/pathlockd.proto`](../proto/pathlockd.proto).
