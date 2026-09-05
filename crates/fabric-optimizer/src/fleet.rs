//! What the optimizer is allowed to know about where a decision could land.
//!
//! README §3 says *ML predicts, the optimizer decides, the controller executes
//! validated decisions.* Deciding to replicate or move something **is** naming
//! a destination — that is the decision. An optimizer that names a node it has
//! no evidence exists is not deciding, it is guessing, and every guess is
//! refused downstream with `UnknownDestination`.
//!
//! So the fleet is an input, exactly like the workload profile is. This module
//! is the shape of that input. It deliberately mirrors the controller's own
//! `FleetView` field for field — condition, headroom, utilization — rather
//! than inventing a second vocabulary, because the controller re-checks every
//! one of them at validation: an optimizer reasoning about different
//! quantities than the validator would propose things that are always refused,
//! and would never learn why.
//!
//! It cannot *depend* on `fabric-controller` to say so — the controller
//! depends on this crate — so the runtime, which depends on both, projects one
//! into the other. That projection is the only place the two are joined.

use std::collections::BTreeMap;

use fabric_core::DbmsId;
use fabric_topology::TopologyRegistry;
use serde::{Deserialize, Serialize};

/// One node the optimizer may name as a destination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetNode {
    pub id: DbmsId,
    pub region: String,

    /// Whether the control plane will let this node receive data it does not
    /// already hold. A node that will not is not a candidate at all — it is
    /// unreachable, degraded or draining, and proposing it produces a
    /// `DestinationUnhealthy` refusal every time.
    pub accepts_placement: bool,

    /// Placements it holds, and how many it is provisioned for. A capacity of
    /// `0` means "not reported": headroom is then unknown and is not filtered
    /// on, rather than being treated as zero headroom.
    pub hosted_placements: usize,
    pub placement_capacity: usize,

    /// The node's worst resource axis, `0.0` when nothing has been reported.
    pub utilization: f64,
}

impl FleetNode {
    /// A node about which only its existence and region are known.
    ///
    /// Used for a fleet derived from the placement map alone: the node is
    /// certainly real — it is holding data — but nothing about its health or
    /// headroom has been observed, so nothing is filtered on either.
    pub fn unreported(id: DbmsId, region: impl Into<String>) -> Self {
        Self {
            id,
            region: region.into(),
            accepts_placement: true,
            hosted_placements: 0,
            placement_capacity: 0,
            utilization: 0.0,
        }
    }

    /// Whether this node could take a copy it does not already hold.
    pub fn has_headroom(&self) -> bool {
        self.placement_capacity == 0
            || self.hosted_placements < self.placement_capacity
    }
}

/// The fleet a decision is being made against.
///
/// Carries the placement map (to find where the cell being decided about
/// actually lives) and the nodes the decision may name.
///
/// Locating the cell itself is not this type's job. It used to be: a decision
/// used to arrive as a bare coordinate, several shards could share one, and
/// resolving that ambiguity was pushed down into a `shard_id` this type
/// optionally carried. Now `WorkloadProfile` names a cell as `(shard_id,
/// coordinate)` directly, so the caller locates it with
/// `fleet.topology().locate(profile.shard_id, profile.coordinate)` and this
/// type stays what its doc always said it was: the nodes a decision may name.
#[derive(Debug, Clone)]
pub struct Fleet<'a> {
    topology: &'a TopologyRegistry,
    nodes: Vec<FleetNode>,
}

impl<'a> Fleet<'a> {
    /// The fleet as the control plane reported it.
    pub fn new(topology: &'a TopologyRegistry, nodes: Vec<FleetNode>) -> Self {
        Self { topology, nodes }
    }

    /// The fleet as the placement map alone describes it.
    ///
    /// Every node holding data is real and is a candidate; nothing is known
    /// about health or headroom, so nothing is filtered on it. This is what a
    /// caller that has a topology and no node inventory can honestly say, and
    /// it is still a world away from inventing an id: every node named here is
    /// one the control plane has a placement record for.
    pub fn from_placements(topology: &'a TopologyRegistry) -> Self {
        let mut seen: BTreeMap<String, FleetNode> = BTreeMap::new();

        for placement in topology.placements() {
            let node = seen
                .entry(placement.dbms_id.0.clone())
                .or_insert_with(|| {
                    FleetNode::unreported(
                        placement.dbms_id.clone(),
                        placement.region.clone(),
                    )
                });

            node.hosted_placements += 1;
        }

        Self {
            topology,
            nodes: seen.into_values().collect(),
        }
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub fn nodes(&self) -> &[FleetNode] {
        &self.nodes
    }

    /// Nodes that could take a copy of the cell `source` holds, best first.
    ///
    /// The ordering is least-loaded first, then fewest placements, then id —
    /// the last purely so that two runs against the same fleet propose the same
    /// node. `away_from` puts nodes outside that region ahead of nodes inside
    /// it without excluding either, which is what a replica wants: a second
    /// copy in the same rack as the first is a copy of the same failure.
    ///
    /// Nothing here applies the controller's thresholds. The optimizer
    /// proposes its best candidate and the controller decides whether it is
    /// allowed — README §4, and the reason this returns a ranked list rather
    /// than a verdict.
    pub fn destinations(
        &self,
        source: &DbmsId,
        away_from: Option<&str>,
    ) -> Vec<&FleetNode> {
        let mut candidates: Vec<&FleetNode> = self
            .nodes
            .iter()
            .filter(|node| &node.id != source)
            .filter(|node| node.accepts_placement)
            .filter(|node| node.has_headroom())
            .collect();

        candidates.sort_by(|left, right| {
            let region = |node: &FleetNode| match away_from {
                Some(region) => u8::from(node.region == region),
                None => 0,
            };

            region(left)
                .cmp(&region(right))
                .then(
                    left.utilization
                        .partial_cmp(&right.utilization)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
                .then(left.hosted_placements.cmp(&right.hosted_placements))
                .then(left.id.0.cmp(&right.id.0))
        });

        candidates
    }

    /// The single best destination, or `None` when the fleet offers none.
    pub fn best_destination(
        &self,
        source: &DbmsId,
        away_from: Option<&str>,
    ) -> Option<&FleetNode> {
        self.destinations(source, away_from).into_iter().next()
    }
}

impl<'a> From<&'a TopologyRegistry> for Fleet<'a> {
    fn from(topology: &'a TopologyRegistry) -> Self {
        Self::from_placements(topology)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fabric_core::{Coordinate, Shard};

    fn node(id: &str, region: &str, utilization: f64, hosted: usize) -> FleetNode {
        FleetNode {
            id: DbmsId::new(id),
            region: region.to_string(),
            accepts_placement: true,
            hosted_placements: hosted,
            placement_capacity: 4,
            utilization,
        }
    }

    fn registry() -> TopologyRegistry {
        let mut registry = TopologyRegistry::new();

        registry.place(
            DbmsId::new("db-a"),
            &Shard::new(1, "social"),
            Coordinate::new(0, 0),
            "us-east",
        );

        registry.place(
            DbmsId::new("db-b"),
            &Shard::new(2, "social"),
            Coordinate::new(0, 0),
            "us-west",
        );

        registry
    }

    #[test]
    fn a_destination_is_always_a_node_the_fleet_named() {
        let registry = registry();

        let fleet = Fleet::new(
            &registry,
            vec![
                node("db-a", "us-east", 0.90, 1),
                node("db-b", "us-west", 0.10, 1),
                node("db-c", "us-east", 0.20, 0),
            ],
        );

        let chosen = fleet
            .best_destination(&DbmsId::new("db-a"), None)
            .expect("a candidate");

        assert_eq!(chosen.id.0, "db-b");
        assert!(fleet.nodes().iter().any(|n| n.id == chosen.id));

        // Nothing outside the supplied fleet is ever reachable.
        assert!(
            fleet
                .destinations(&DbmsId::new("db-a"), None)
                .iter()
                .all(|candidate| ["db-b", "db-c"].contains(&candidate.id.0.as_str()))
        );
    }

    #[test]
    fn a_node_that_cannot_take_the_copy_is_not_a_candidate() {
        let registry = registry();

        let mut full = node("db-b", "us-west", 0.10, 4);
        full.placement_capacity = 4;

        let mut draining = node("db-c", "us-east", 0.05, 0);
        draining.accepts_placement = false;

        let fleet = Fleet::new(
            &registry,
            vec![node("db-a", "us-east", 0.90, 1), full, draining],
        );

        assert!(fleet.best_destination(&DbmsId::new("db-a"), None).is_none());
    }

    #[test]
    fn a_replica_is_ranked_out_of_the_source_region_first() {
        let registry = registry();

        let fleet = Fleet::new(
            &registry,
            vec![
                node("db-a", "us-east", 0.90, 1),
                // Cheaper, but next door to the copy we already have.
                node("db-b", "us-east", 0.01, 0),
                node("db-c", "us-west", 0.50, 0),
            ],
        );

        assert_eq!(
            fleet
                .best_destination(&DbmsId::new("db-a"), Some("us-east"))
                .unwrap()
                .id
                .0,
            "db-c"
        );

        // Without the locality requirement the cheapest node wins.
        assert_eq!(
            fleet.best_destination(&DbmsId::new("db-a"), None).unwrap().id.0,
            "db-b"
        );
    }

    /// A cell is `(shard, coordinate)`. `Fleet` no longer resolves it itself --
    /// that is `WorkloadProfile::shard_id`'s job now -- but the topology it
    /// exposes still answers unambiguously for either of two shards that
    /// share a coordinate, given the shard.
    #[test]
    fn the_topology_a_fleet_exposes_locates_by_shard_not_by_guessing() {
        let registry = registry();
        let cell = Coordinate::new(0, 0);

        let fleet = Fleet::from_placements(&registry);

        assert_eq!(fleet.topology().locate(1, cell).unwrap().dbms_id.0, "db-a");
        assert_eq!(fleet.topology().locate(2, cell).unwrap().dbms_id.0, "db-b");
    }

    #[test]
    fn a_fleet_derived_from_the_map_names_only_nodes_that_hold_data() {
        let registry = registry();
        let fleet = Fleet::from_placements(&registry);

        let ids: Vec<&str> =
            fleet.nodes().iter().map(|node| node.id.0.as_str()).collect();

        assert_eq!(ids, vec!["db-a", "db-b"]);
        assert!(fleet.nodes().iter().all(|node| node.has_headroom()));
    }
}
