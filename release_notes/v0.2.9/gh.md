TiKV GC sweep hardening release.

## Changes

- **Background GC no longer reports retryable TiKV lock-resolution errors as
  sweep failures** - `gc_once` now retries transient TiKV errors during page
  scans and then skips the current best-effort sweep if the retry budget is
  exhausted. This fixes repeated `gc sweep failed` logs caused by retryable
  `TxnNotFound` errors surfaced through `MultipleKeyErrors` while TiKV is
  resolving old transaction locks. Lazy expiry still enforces correctness; the
  active GC sweep remains housekeeping only.

- **Expired-key deletion is best-effort on retryable commit failures** - if a GC
  delete chunk hits a retryable TiKV commit error, the daemon now logs it at
  debug level and leaves those bytes for a later sweep instead of surfacing a
  daemon-level error.

- **Serialization tombstones are flushed at commit time** -
  `Tx::serialize_handler` now records the handler in memory and writes the
  `pathlockd:__serialize__:<handler>` tombstone immediately before commit. This
  preserves the same optimistic write-write conflict used for per-handler
  serialization, while keeping the TiKV transaction primary on real lock
  metadata whenever the transaction has ordinary lock mutations. Diagnostics for
  unresolved transactions are therefore less likely to point at the internal
  serialization key.

- **Regression coverage for GC retry handling** - unit tests now cover the
  retryable `TxnNotFound`/`MultipleKeyErrors` path used by the GC skip logic.

## Upgrade note

No TiKV keyspace migration is required. Existing serialization tombstones and
lock metadata remain valid, and expired lock keys will continue to be reclaimed
by later GC sweeps.

This release does not change the protobuf API or lock semantics. The fix only
changes how the daemon treats retryable TiKV errors in the background active-GC
path and how it orders internal serialization tombstone writes inside a
transaction.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.9-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.2.9-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.9   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
