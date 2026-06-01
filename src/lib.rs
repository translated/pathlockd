//! pathlockd — fast, scalable, opinionated path-based distributed locking
//! primitives for developers building user-space virtual filesystems, with lock
//! metadata persisted in TiKV.
//!
//! Capabilities: hierarchical read/write locking with point-only reads, fencing
//! tokens, TTL leases with renewal, owner liveness and dead-owner pruning,
//! descendant indexes for O(subtree) conflict scans, wait-edge deadlock
//! detection, cooperative revoke and forced release, and a per-owner
//! release/kill/revoke event stream. The primitives are exposed over gRPC; all
//! durable state lives in TiKV.

/// Generated protobuf/tonic types.
pub mod proto {
    tonic::include_proto!("pathlockd.v1");
}

#[macro_use]
mod macros;

pub mod config;
pub mod engine;
pub mod events;
pub mod service;
pub mod store;
