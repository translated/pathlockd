# Backup and Restore

pathlockd persists all lock state as RocksDB databases under `data_dir/groups/`.
Each Raft group has its own RocksDB instance.

## Backup

### Method 1: Filesystem snapshot (recommended)

Stop the pathlockd process, then copy the data directory:

```bash
# Stop pathlockd
systemctl stop pathlockd

# Create a backup
tar -czf pathlockd-backup-$(date +%Y%m%d-%H%M%S).tar.gz /var/lib/pathlockd

# Start pathlockd
systemctl start pathlockd
```

### Method 2: RocksDB Checkpoint (online)

Use the `rocksdb::checkpoint` API to create a consistent snapshot while the
daemon is running. This requires implementing checkpoint support in pathlockd
(planned for P3).

```rust
// In code (not yet exposed via CLI):
let checkpoint = rocksdb::checkpoint::Checkpoint::new(&db)?;
checkpoint.create_checkpoint("/backup/g000001")?;
```

### Method 3: Raft snapshot streaming (planned for P3)

In a multi-node cluster, a follower catches up from a leader's snapshot.
The snapshot includes all state-machine column families, last_applied index,
and membership metadata. This mechanism doubles as a backup transport.

## Restore

### Single-node restore

1. Stop pathlockd on the target node.
2. Replace the data directory with the backup:

```bash
systemctl stop pathlockd
rm -rf /var/lib/pathlockd
tar -xzf pathlockd-backup-*.tar.gz -C /
systemctl start pathlockd
```

3. Verify readiness:

```bash
pathlockd --health-check
```

### Multi-node restore

For a single node in a multi-node cluster:

1. Stop the affected node.
2. Remove its data directory.
3. Restart it in `join` mode:

```bash
rm -rf /var/lib/pathlockd
pathlockd --config pathlockd.toml  # with join = true
```

The node will rejoin the cluster and catch up via Raft log replication or
snapshot installation from the leader.

### Full cluster restore

If all nodes are lost simultaneously:

1. Restore the backup to every node.
2. Bootstrap the first node normally.
3. Start remaining nodes in `join` mode.

Data restored from a filesystem backup may be behind the last committed state
by the interval since the last backup. Replication from surviving nodes or a
more recent Raft snapshot fills the gap.

## Crash recovery

The invariant `last_applied` is advanced in the **same atomic WriteBatch** as
state mutations. After a crash:

1. RocksDB replays its WAL.
2. The state machine reads `last_applied` from the `meta` column family.
3. Raft replays committed entries after `last_applied`.

This prevents state/log divergence after a process crash.

## Column families included in backup

| CF | Purpose | Critical? |
|---|---|---|
| `meta` | Raft vote, membership, last_applied | Yes — needed for recovery |
| `raft_log` | Raft log entries | Yes — needed for catch-up |
| `write_locks` | Exclusive write lock state | Yes |
| `read_locks` | Shared read lock state | Yes |
| `fences` | Per-path fencing tokens | Yes (durable high-water marks) |
| `desc_write` | Write descendant indexes | Yes |
| `desc_read` | Read descendant indexes | Yes |
| `owner_alive` | Owner liveness leases | Yes |
| `owner_holds` | Owner-to-locks mappings | Yes |
| `wait_edges` | Deadlock-detection wait edges | Advisory but include |
| `namespace_settings` | Namespace lock algorithm policies and explicit route roots | Yes |
| `lock_queue` | FIFO wait queue (queued waiters) | Yes — preserves wait order |
| `expiry` | Expiry index for active GC | No — rebuilt on restart |

## Verification after restore

```bash
# Check health
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health

# Check a known lock path
grpcurl -plaintext -d '{"path":"google_drive:/test"}' \
  localhost:50051 pathlockd.v1.PathLock/InspectPath

# List locks for a known owner
grpcurl -plaintext -d '{"owner_id":"test-owner"}' \
  localhost:50051 pathlockd.v1.PathLock/ListOwnerLocks
```
