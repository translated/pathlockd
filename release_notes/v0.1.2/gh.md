Bug-fix release on the 0.1 line.

## Fixes

- **`Renew` rolls back on `RenewOutcome::Lost`** — previously, when a renewal
  detected that the lock had already expired, the transaction could still commit
  partial key refreshes done earlier in the same pass. The transaction is now
  rolled back unconditionally on `Lost`, so lease state is never partially
  extended after the caller has been told the lock is gone.
- **Fencing counter overflow guard** — `IncrFencingToken` now uses
  `checked_add` and returns `Internal` instead of silently wrapping the `i64`
  counter to a negative value on overflow.

## Ops

- `scripts/release.sh` — release automation script: validates preconditions
  (clean tree, Cargo version match, no existing tag/release), builds
  linux/amd64 release + debug tarballs with SHA256SUMS, tags, pushes, and
  creates the GitHub release. Supports `--dry-run`, `--prerelease`, `--draft`.

## Upgrade note

No on-disk format changes; a 0.1.1 keyspace is fully compatible with 0.1.2.

## Artifacts (Linux x86_64 / amd64 only)
- `pathlockd-0.1.2-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.1.2-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Both are **dynamically linked** (built on a Debian/glibc system); they need
`glibc` and `libssl3` (+ `ca-certificates`) at runtime. For a self-contained
deployment, use the container image instead.
