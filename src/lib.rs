//! pathlockd — fast, scalable, opinionated path-based distributed locking
//! primitives for developers building user-space virtual filesystems.
//!
//! Durable lock metadata is replicated by embedded Multi-Raft groups and stored
//! locally in RocksDB. Cluster discovery uses SWIM/foca; lock correctness is
//! provided by Raft log order, linearizable reads, TTL leases, and fencing tokens.
//!
//! # Trust boundary
//!
//! Neither gRPC surface authenticates callers and no TLS is built in: the
//! client API can force-release any owner, and the internal raft transport
//! accepts forwarded commands and starts group cores on first contact. Both
//! ports assume a trusted network — deploy them behind network policy or an
//! mTLS-terminating mesh/sidecar, and never expose them publicly.

pub mod proto {
    tonic::include_proto!("pathlockd.v1");
}

/// Internal node-to-node Raft transport (no client API stability guarantees).
pub mod raft_proto {
    tonic::include_proto!("pathlockd.raft.v1");
}

pub mod cluster;
pub mod config;
pub mod engine;
pub mod events;
pub mod otel;
pub mod raft;
pub mod service;
pub mod store_keys;
pub mod store_rocksdb;
