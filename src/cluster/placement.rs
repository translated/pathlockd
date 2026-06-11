//! HRW (Rendezvous Hashing) group placement and voter selection.
//!
//! Placement assigns each routing domain to a Raft group using rendezvous
//! hashing, and selects each group's voters across the available nodes the
//! same way. Both mappings are deterministic: every node computes identical
//! placements from identical inputs, with no coordination.

use xxhash_rust::xxh3::xxh3_64;

/// Identifies one Raft group. Groups `0..group_count` hold lock state;
/// [`SYS_GROUP`] holds cluster-global state.
pub type GroupId = u32;

/// The system group: global monotonic fencing counter, deadlock wait-graph,
/// and the cluster membership directory.
pub const SYS_GROUP: GroupId = u32::MAX;

/// Compute the Raft group for a routing domain using HRW.
pub fn place_domain(domain: &str, group_count: u32) -> GroupId {
    let mut best_group: GroupId = 0;
    let mut best_weight = 0u64;

    for g in 0..group_count {
        let seed = (g as u64).to_le_bytes();
        let mut buf = Vec::with_capacity(seed.len() + domain.len());
        buf.extend_from_slice(&seed);
        buf.extend_from_slice(domain.as_bytes());
        let weight = xxh3_64(&buf);
        if weight > best_weight {
            best_weight = weight;
            best_group = g;
        }
    }

    best_group
}

/// The routing domain of a path: the handler plus the first
/// `prefix_segments` path segments.
///
/// With `prefix_segments == 0` (the default) the domain is the handler alone,
/// so a handler's entire tree lives in one group and every operation —
/// including locking the handler root — is single-group.
///
/// With `prefix_segments == K > 0`, paths shard by their first K segments for
/// write parallelism within one handler. Containment-closure then requires
/// that no lock is ever taken at depth < K (the service layer rejects such
/// paths), because an ancestor at depth < K would span groups.
pub fn routing_prefix(path: &str, prefix_segments: u32) -> &str {
    let Some(colon) = path.find(':') else {
        return path;
    };
    if prefix_segments == 0 {
        return &path[..colon];
    }
    let rest = &path[colon + 1..];
    // rest is "/seg1/seg2/...": find the end of segment K.
    let mut segs = 0u32;
    for (i, b) in rest.bytes().enumerate() {
        if b == b'/' && i > 0 {
            segs += 1;
            if segs == prefix_segments {
                return &path[..colon + 1 + i];
            }
        }
    }
    // Fewer than K segments: the whole path is the domain.
    path
}

/// Number of `/`-separated segments in the path part (after the handler).
/// `h:/` has 0 segments, `h:/a` has 1, `h:/a/b` has 2.
pub fn path_depth(path: &str) -> u32 {
    let Some(colon) = path.find(':') else {
        return 0;
    };
    let rest = &path[colon + 1..];
    if rest == "/" || rest.is_empty() {
        return 0;
    }
    rest.bytes().filter(|&b| b == b'/').count() as u32
}

/// Select the voters for a group using HRW across the given nodes.
pub fn select_voters(group_id: GroupId, nodes: &[u64], replication_factor: u32) -> Vec<u64> {
    let mut weights: Vec<(u64, u64)> = nodes
        .iter()
        .map(|&node_id| {
            let seed = (group_id as u64).to_le_bytes();
            let node_bytes = node_id.to_le_bytes();
            let mut buf = Vec::with_capacity(seed.len() + node_bytes.len());
            buf.extend_from_slice(&seed);
            buf.extend_from_slice(&node_bytes);
            let weight = xxh3_64(&buf);
            (node_id, weight)
        })
        .collect();

    weights.sort_by_key(|&(_, weight)| std::cmp::Reverse(weight));
    weights
        .into_iter()
        .take(replication_factor as usize)
        .map(|(id, _)| id)
        .collect()
}

/// The effective replication factor for a cluster of `stable_nodes` nodes:
/// the largest odd number ≤ both the configured factor and the node count.
pub fn rf_effective(configured: u32, stable_nodes: usize) -> u32 {
    let n = configured.min(stable_nodes as u32).max(1);
    if n % 2 == 0 {
        n - 1
    } else {
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn place_domain_is_deterministic_and_in_range() {
        for domain in ["h", "google_drive", "s3", "workspace_123"] {
            let g1 = place_domain(domain, 32);
            let g2 = place_domain(domain, 32);
            assert_eq!(g1, g2);
            assert!(g1 < 32);
        }
    }

    #[test]
    fn routing_prefix_handler_only_by_default() {
        assert_eq!(routing_prefix("h:/a/b/c", 0), "h");
        assert_eq!(routing_prefix("google_drive:/x", 0), "google_drive");
        assert_eq!(routing_prefix("h:/", 0), "h");
    }

    #[test]
    fn routing_prefix_with_segments_shards_deeper() {
        assert_eq!(routing_prefix("h:/a/b/c", 1), "h:/a");
        assert_eq!(routing_prefix("h:/a/b/c", 2), "h:/a/b");
        // Fewer segments than K: whole path is the domain.
        assert_eq!(routing_prefix("h:/a", 2), "h:/a");
        assert_eq!(routing_prefix("h:/", 1), "h:/");
    }

    #[test]
    fn path_depth_counts_segments() {
        assert_eq!(path_depth("h:/"), 0);
        assert_eq!(path_depth("h:/a"), 1);
        assert_eq!(path_depth("h:/a/b"), 2);
        assert_eq!(path_depth("h:/a/b/c"), 3);
    }

    #[test]
    fn select_voters_picks_rf_nodes_deterministically() {
        let nodes = vec![1u64, 2, 3, 4, 5];
        let v1 = select_voters(7, &nodes, 3);
        let v2 = select_voters(7, &nodes, 3);
        assert_eq!(v1, v2);
        assert_eq!(v1.len(), 3);
        // All selected voters are real nodes, no duplicates.
        let set: std::collections::HashSet<_> = v1.iter().collect();
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn rf_effective_degrades_to_largest_odd() {
        assert_eq!(rf_effective(3, 1), 1);
        assert_eq!(rf_effective(3, 2), 1);
        assert_eq!(rf_effective(3, 3), 3);
        assert_eq!(rf_effective(3, 4), 3);
        assert_eq!(rf_effective(3, 5), 3);
        assert_eq!(rf_effective(5, 4), 3);
        assert_eq!(rf_effective(5, 6), 5);
        assert_eq!(rf_effective(1, 5), 1);
    }
}
