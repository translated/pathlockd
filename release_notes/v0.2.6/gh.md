Preemptive claim release.

## Changes

- **Preemptive claim on cooperative revoke** - `RequestRevokeRequest` now
  accepts three optional fields — `claim_path`, `claimant_owner_id`, and
  `claim_ttl_ms` — that let the deadlock winner atomically plant a short-lived
  reservation on the contested path *before* publishing the REVOKE signal. While
  the claim is live every other owner is blocked from (re-)acquiring that path,
  closing the race window where the revoked victim could re-grab the path before
  the winner had a chance to acquire it. Omitting the new fields preserves the
  existing pure-notification behavior.

- **Claim TTL and self-healing** - when `claim_ttl_ms` is zero the daemon
  defaults to a 3 s TTL. A claim whose claimant's `alive` key has elapsed is
  treated as absent and pruned on the spot, so a winner that crashes before
  converting the claim into a real lock cannot block the path past its liveness
  window.

- **Claim cleared on successful acquire** - when the claimant itself acquires
  (or renews) the claimed path the daemon deletes the claim key immediately,
  so it stops blocking unrelated owners for the remainder of its TTL.

- **`IsBlocking` extended for claims** - the `preempt_claimed` conflict reason
  is now a recognized blocking reason. `IsBlocking` re-checks claim liveness
  directly instead of falling through to the write-owner check, preventing
  hot-spin retries while the claim is still active and the winner has not yet
  acquired.

- **Non-fatal claim planting** - if writing the claim to TiKV fails the daemon
  logs a warning and proceeds with the revoke anyway. The deadlock is still
  resolved; the race protection is simply absent for that one revoke.

## Upgrade note

No TiKV keyspace migration is required. The new `pathlockd:claim:<path>` keys
are written only when the caller populates the new proto fields; existing
deployments that do not set those fields see no change in behavior.

The proto change is fully backward compatible: the new fields are optional and
default to empty/zero.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.6-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.2.6-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.6   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
