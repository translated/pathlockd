Lease-refresh correctness fix, GC memory and startup improvements.

## Changes

- **`acquire` now refreshes every still-held path** — previously, only the
  paths explicitly listed in an `Acquire` request had their TTL extended.
  Paths already held by the same owner but absent from the new request kept
  their original expiry; they could lapse while the owner's liveness key was
  still live, allowing another owner to take the lock with no `LOST` event
  surfaced. Now, after handling the listed paths, `acquire` iterates the
  owner's full own-set and refreshes every key it still owns (write-lock key
  + fence key + descendant indexes for write-held paths; read-set membership
  for read-held paths). A regression test (`acquire_refreshes_unlisted_held_lease`)
  covers this scenario.

- **`gc_once` deletes per page instead of accumulating then deleting** — the
  previous implementation collected expired-key candidates across the entire
  keyspace before issuing any deletes. On large keyspaces this made the sweep
  O(total-expired-keys) in memory. Candidates are now collected and deleted
  within each page, keeping memory use O(`gc_page`) regardless of keyspace
  size. The cursor advance happens after the per-page delete, so deleted keys
  are never re-scanned by subsequent pages.

- **`IsBlocking` rejects unknown `reason` values** — the `is_blocking_inner`
  re-check logic interprets the `reason` field to decide whether to re-check
  a read lock or a write lock. An unrecognized value previously fell through
  silently to the write-lock path. It now returns `INVALID_ARGUMENT` with a
  clear message listing the five accepted reasons (`ancestor_locked`,
  `write_locked`, `read_locked`, `descendant_write_locked`,
  `descendant_read_locked`).

- **`gc_page = 0` with GC enabled now fails at startup** — a zero page size
  makes every GC sweep return nothing, silently disabling active reclamation
  while the GC goroutine keeps running. The config validator now rejects this
  combination with a clear message; disabling GC entirely remains supported
  via `gc_interval_secs = 0`.

- **Distroless container image** — the Docker base switched from Debian to
  `gcr.io/distroless/cc-debian12`, shrinking the image footprint and reducing
  the attack surface. No entrypoint or behavior changes.

- **arm64 Docker build is now opt-in** — `scripts/release.sh --docker` builds
  `linux/amd64` only by default. Pass `--arm64` to also build and push
  `linux/arm64` (requires the local builder to have arm64 emulation).

## Upgrade note

No binary or on-disk format changes; a 0.2.1 keyspace is fully compatible
with 0.2.2. The lease-refresh fix is transparent: existing clients benefit
automatically after the server is updated.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.2-linux-amd64.tar.gz` — optimized, stripped release binary (generic x86-64).
- `pathlockd-0.2.2-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
images:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.2             # amd64 + arm64
docker pull ghcr.io/alexpacio/pathlockd:0.2.2-x86-64-v4   # amd64 / AVX-512 only
```
