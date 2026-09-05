use fabric_ml::WorkloadPredictor;
use fabric_workload::WorkloadProfile;

use crate::decision::{OptimizationAction, OptimizationDecision};
use crate::fleet::Fleet;

#[derive(Debug, Clone)]
pub struct WorkloadOptimizer {
    predictor: WorkloadPredictor,
}

impl Default for WorkloadOptimizer {
    fn default() -> Self {
        Self {
            predictor: WorkloadPredictor::default(),
        }
    }
}

impl WorkloadOptimizer {
    pub fn new(predictor: WorkloadPredictor) -> Self {
        Self { predictor }
    }

    /// Decide what, if anything, should happen to this cell.
    ///
    /// `fleet` is where the destination comes from. It used to be just the
    /// placement map, and the destination used to be `format!("{id}-alternate")`
    /// — a node id no fleet has ever contained, so every `Replicate` and every
    /// `Move` this optimizer produced was refused by the controller with
    /// `UnknownDestination` and the only action that could execute was
    /// `Isolate`. A `&TopologyRegistry` still converts into a [`Fleet`]
    /// (through [`Fleet::from_placements`]) so that a caller holding only a
    /// map keeps working, but what it gets back is a list of nodes that
    /// demonstrably hold data — never an invented one.
    ///
    /// Why the optimizer names the node at all, rather than deferring to the
    /// mechanism the way [`OptimizationAction::Isolate`] does: isolation is
    /// defined by the *source* — get this workload off the node it is
    /// contending on — and any separate node satisfies it, so the choice is
    /// genuinely a placement detail. "Replicate for read capacity" and "move
    /// this mixed hotspot somewhere cooler" are not: which node it lands on is
    /// the decision, it is made from workload knowledge only this layer has,
    /// and the controller's `DestinationSaturated` / `DestinationAtCapacity` /
    /// `DestinationUnhealthy` rules exist precisely to check a *proposed*
    /// destination. Deferring here would leave that whole apparatus with
    /// nothing to check.
    pub fn optimize<'a>(
        &self,
        profile: &WorkloadProfile,
        fleet: impl Into<Fleet<'a>>,
    ) -> OptimizationDecision {
        let fleet = fleet.into();
        let prediction = self.predictor.predict_hotspot(profile);

        if !prediction.is_likely_hot() {
            return OptimizationDecision {
                shard_id: profile.shard_id,
                coordinate: profile.coordinate,
                action: OptimizationAction::NoAction,
                expected_gain: 0.0,
                estimated_cost: 0.0,
                confidence: 1.0 - prediction.probability,
            };
        }

        /*
         * `profile` names a cell, not a bare coordinate -- `shard_id` and
         * `coordinate` together -- so this is a direct lookup, never a guess
         * among several shards that happen to share the coordinate. `None`
         * here means only one honest thing: nothing holds this cell.
         */
        let Some(placement) = fleet.topology().locate(profile.shard_id, profile.coordinate)
        else {
            return Self::cannot_act(profile);
        };

        /*
         * Initial optimization policy:
         *
         * A heavily read-oriented workload is a strong candidate for
         * replication because reads can be distributed without moving
         * the logical coordinate.
         */
        if profile.read_ratio >= 0.70 {
            let Some(destination) = fleet
                .best_destination(&placement.dbms_id, Some(&placement.region))
            else {
                return Self::cannot_act(profile);
            };

            return OptimizationDecision {
                shard_id: profile.shard_id,
                coordinate: profile.coordinate,

                action: OptimizationAction::Replicate {
                    target: destination.id.clone(),
                },

                expected_gain: prediction.probability * 1.20,
                estimated_cost: 0.35,
                confidence: prediction.probability,
            };
        }

        /*
         * Write-heavy hotspots are initially isolated rather than
         * replicated blindly. Replication of write-heavy workloads
         * introduces additional coordination cost.
         *
         * Isolate names no destination on purpose: what makes it isolation is
         * leaving the contended node, not which node it lands on, so the
         * mechanism picks with the same spread rules every other copy is
         * placed by.
         */
        if profile.write_ratio >= 0.70 {
            return OptimizationDecision {
                shard_id: profile.shard_id,
                coordinate: profile.coordinate,
                action: OptimizationAction::Isolate,
                expected_gain: prediction.probability,
                estimated_cost: 0.45,
                confidence: prediction.probability,
            };
        }

        /*
         * Mixed workloads are candidates for moving the logical
         * workload to another physical DBMS node. Locality is not a
         * consideration here the way it is for a replica -- there is still
         * exactly one copy afterwards -- so the least-loaded node wins.
         */
        let Some(destination) = fleet.best_destination(&placement.dbms_id, None)
        else {
            return Self::cannot_act(profile);
        };

        OptimizationDecision {
            shard_id: profile.shard_id,
            coordinate: profile.coordinate,

            action: OptimizationAction::Move {
                target: destination.id.clone(),
            },

            expected_gain: prediction.probability * 0.90,
            estimated_cost: 0.50,
            confidence: prediction.probability,
        }
    }

    /// A hot cell the optimizer cannot honestly propose anything for.
    ///
    /// Deliberately not a low-confidence `Replicate` onto some node: a
    /// decision the control plane would refuse teaches the feedback loop
    /// nothing, and one it would *accept* on bad grounds is worse. Zero
    /// confidence with a non-zero cost is how this crate has always said "I
    /// know something is wrong here and I have no move".
    fn cannot_act(profile: &WorkloadProfile) -> OptimizationDecision {
        OptimizationDecision {
            shard_id: profile.shard_id,
            coordinate: profile.coordinate,
            action: OptimizationAction::NoAction,
            expected_gain: 0.0,
            estimated_cost: 1.0,
            confidence: 0.0,
        }
    }

    pub fn predictor(&self) -> &WorkloadPredictor {
        &self.predictor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fabric_core::{Coordinate, DbmsId, Shard};
    use fabric_telemetry::WorkloadMetrics;
    use fabric_topology::TopologyRegistry;

    use crate::fleet::FleetNode;

    const CELL: Coordinate = Coordinate::new(3, 4);
    const SHARD: u64 = 1;

    fn hot(reads: f64, writes: f64) -> WorkloadProfile {
        WorkloadProfile::from_metrics(
            SHARD,
            CELL,
            WorkloadMetrics {
                operations_per_second: reads + writes,
                reads_per_second: reads,
                writes_per_second: writes,
                read_latency_us: 60_000.0,
                write_latency_us: 60_000.0,
                cpu_utilization: 0.99,
                memory_utilization: 0.95,
                storage_bytes_per_second: 0,
                network_in_bytes_per_second: 0,
                network_out_bytes_per_second: 0,
                queue_depth: 30_000,
                cell_breakdown: Vec::new(),
                cell_breakdown_partial: false,
            },
        )
    }

    fn registry() -> TopologyRegistry {
        let mut registry = TopologyRegistry::new();

        registry.place(
            DbmsId::new("db-a"),
            &Shard::new(SHARD, "social"),
            CELL,
            "us-east",
        );

        registry
    }

    fn node(id: &str, region: &str, utilization: f64) -> FleetNode {
        FleetNode {
            id: DbmsId::new(id),
            region: region.to_string(),
            accepts_placement: true,
            hosted_placements: 1,
            placement_capacity: 8,
            utilization,
        }
    }

    fn fleet(registry: &TopologyRegistry) -> Fleet<'_> {
        Fleet::new(
            registry,
            vec![
                node("db-a", "us-east", 0.99),
                node("db-b", "us-east", 0.40),
                node("db-c", "us-west", 0.50),
            ],
        )
    }

    /// The bug this closes: the destination must be a node the fleet named.
    #[test]
    fn a_named_destination_is_always_a_real_node() {
        let registry = registry();
        let optimizer = WorkloadOptimizer::default();

        let replicate = optimizer.optimize(&hot(11_000.0, 1_000.0), fleet(&registry));
        let relocate = optimizer.optimize(&hot(6_000.0, 6_000.0), fleet(&registry));

        let named: Vec<DbmsId> = [replicate.action, relocate.action]
            .into_iter()
            .map(|action| match action {
                OptimizationAction::Replicate { target }
                | OptimizationAction::Move { target } => target,
                other => panic!("expected a destination-bearing action, got {other:?}"),
            })
            .collect();

        for destination in named {
            assert!(
                fleet(&registry)
                    .nodes()
                    .iter()
                    .any(|candidate| candidate.id == destination),
                "'{}' is not in the fleet",
                destination.0
            );

            assert_ne!(destination.0, "db-a", "the source is not a destination");
            assert!(!destination.0.ends_with("-alternate"));
        }
    }

    #[test]
    fn a_read_heavy_hotspot_replicates_out_of_its_own_region() {
        let registry = registry();

        let decision = WorkloadOptimizer::default()
            .optimize(&hot(11_000.0, 1_000.0), fleet(&registry));

        // db-b is less loaded, but it shares a region with the only copy.
        assert_eq!(
            decision.action,
            OptimizationAction::Replicate {
                target: DbmsId::new("db-c")
            }
        );
        assert!(decision.should_execute());
    }

    #[test]
    fn a_mixed_hotspot_moves_to_the_least_loaded_node() {
        let registry = registry();

        let decision = WorkloadOptimizer::default()
            .optimize(&hot(6_000.0, 6_000.0), fleet(&registry));

        assert_eq!(
            decision.action,
            OptimizationAction::Move {
                target: DbmsId::new("db-b")
            }
        );
        assert!(decision.should_execute());
    }

    /// A write-heavy hotspot still names nothing: the mechanism chooses, which
    /// is the one action whose destination genuinely is a placement detail.
    #[test]
    fn a_write_heavy_hotspot_still_isolates_without_naming_a_node() {
        let registry = registry();

        let decision = WorkloadOptimizer::default()
            .optimize(&hot(1_000.0, 11_000.0), fleet(&registry));

        assert_eq!(decision.action, OptimizationAction::Isolate);
    }

    /// With nowhere to put a copy, the answer is "nothing" -- never a made-up
    /// node, and never a proposal the controller is guaranteed to refuse.
    #[test]
    fn a_fleet_with_nowhere_to_go_produces_no_action() {
        let registry = registry();

        let alone = Fleet::new(&registry, vec![node("db-a", "us-east", 0.99)]);

        let decision =
            WorkloadOptimizer::default().optimize(&hot(11_000.0, 1_000.0), alone);

        assert_eq!(decision.action, OptimizationAction::NoAction);
        assert!(!decision.should_execute());
    }

    /// A caller holding only a placement map still gets real node ids.
    #[test]
    fn a_bare_topology_still_names_a_node_that_holds_data() {
        let mut registry = registry();

        registry.place(
            DbmsId::new("db-z"),
            &Shard::new(1, "social"),
            Coordinate::new(9, 9),
            "us-west",
        );

        let decision =
            WorkloadOptimizer::default().optimize(&hot(11_000.0, 1_000.0), &registry);

        assert_eq!(
            decision.action,
            OptimizationAction::Replicate {
                target: DbmsId::new("db-z")
            }
        );
    }

    /// The ambiguity `WorkloadProfile::shard_id` closes: the same coordinate
    /// placed on two different shards used to be unlocatable from the
    /// coordinate alone, forcing a decline. With the shard on the profile,
    /// each cell resolves to its own placement and its own decision -- never
    /// the other shard's.
    #[test]
    fn a_coordinate_two_shards_share_is_resolved_by_its_own_shard_id() {
        let mut registry = TopologyRegistry::new();

        registry.place(DbmsId::new("db-a"), &Shard::new(1, "social"), CELL, "us-east");
        registry.place(DbmsId::new("db-b"), &Shard::new(2, "social"), CELL, "us-west");

        let fleet = Fleet::new(
            &registry,
            vec![
                node("db-a", "us-east", 0.10),
                node("db-b", "us-west", 0.10),
                node("db-c", "us-east", 0.10),
            ],
        );

        let optimizer = WorkloadOptimizer::default();

        let mut on_shard_one = hot(11_000.0, 1_000.0);
        on_shard_one.shard_id = 1;

        let mut on_shard_two = hot(11_000.0, 1_000.0);
        on_shard_two.shard_id = 2;

        let decision_one = optimizer.optimize(&on_shard_one, fleet.clone());
        let decision_two = optimizer.optimize(&on_shard_two, fleet.clone());

        // Neither declines: both cells were located, each against its own
        // shard's placement.
        assert!(decision_one.should_execute());
        assert!(decision_two.should_execute());

        assert_eq!(decision_one.shard_id, 1);
        assert_eq!(decision_two.shard_id, 2);

        // Shard 1's copy is on db-a, so a replica must land elsewhere; shard
        // 2's copy is on db-b, a different node -- proof neither decision
        // reasoned about the other shard's cell.
        assert_eq!(
            decision_one.action,
            OptimizationAction::Replicate {
                target: DbmsId::new("db-b")
            }
        );
        assert_eq!(
            decision_two.action,
            OptimizationAction::Replicate {
                target: DbmsId::new("db-a")
            }
        );
    }
}
