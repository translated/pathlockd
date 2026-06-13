Client-streamed acquire for large path sets: `AcquireStream` lets clients
split an acquire request across multiple gRPC chunks while the daemon
processes them as a single atomic transaction.

## Changes

### Added: `AcquireStream` RPC

The new client-streaming RPC extends `Acquire` beyond the unary 1024-path
limit. Clients stream multiple `AcquireRequest` chunks; the daemon merges
them into one logical request and applies a single Acquire transaction.
No lock is granted until all chunks have been received and validated.

- **Merge semantics:** metadata fields (`owner_id`, `ttl_ms`,
  `fencing_token`, `idempotency_key`) set in the first non-zero chunk
  become the authoritative values; later chunks may omit them or must
  supply identical values (mismatches are rejected).
- **Path limits:** each chunk is capped at `MAX_PATHS_PER_REQUEST` (1024);
  the merged total is capped at `MAX_PATHS_PER_STREAMED_ACQUIRE` (65,536).
- **Atomicity:** the entire merged request is validated and applied as
  one Raft write, so partial success or lock leakage cannot occur.
- **E2E test:** `e2e_streamed_acquire_is_one_logical_acquire_past_unary_cap`
  streams 1,100 paths across two chunks and verifies that a conflicting
  write acquire on a covered path is correctly rejected with
  `write_locked`.

### Changed: shared acquire logic

The unary `Acquire` handler's request validation and execution logic has
been extracted into `handle_acquire_request`, now shared by both the
unary and streamed paths. Behaviour is unchanged for unary callers.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.8.2-linux-amd64.tar.gz` - optimized, stripped release binary.
- `pathlockd-0.8.2-linux-amd64-debug.tar.gz` - unoptimized binary with debug info.
- `SHA256SUMS` - checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.8.2   # amd64 + arm64
```
