use fabric_core::DbmsId;
use fabric_ml::WorkloadPredictor;
use fabric_topology::TopologyRegistry;
use fabric_workload::WorkloadProfile;

use crate::decision::{OptimizationAction, OptimizationDecision};

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

    pub fn optimize(
        &self,
        profile: &WorkloadProfile,
        topology: &TopologyRegistry,
    ) -> OptimizationDecision {
        let prediction = self.predictor.predict_hotspot(profile);

        if !prediction.is_likely_hot() {
            return OptimizationDecision {
                coordinate: profile.coordinate,
                action: OptimizationAction::NoAction,
                expected_gain: 0.0,
                estimated_cost: 0.0,
                confidence: 1.0 - prediction.probability,
            };
        }

        let placement = topology
            .placements()
            .find(|p| p.coordinate == profile.coordinate);

        let Some(placement) = placement else {
            return OptimizationDecision {
                coordinate: profile.coordinate,
                action: OptimizationAction::NoAction,
                expected_gain: 0.0,
                estimated_cost: 1.0,
                confidence: 0.0,
            };
        };

        /*
         * Initial optimization policy:
         *
         * A heavily read-oriented workload is a strong candidate for
         * replication because reads can be distributed without moving
         * the logical coordinate.
         */
        if profile.read_ratio >= 0.70 {
            return OptimizationDecision {
                coordinate: profile.coordinate,

                action: OptimizationAction::Replicate {
                    target: alternate_node(&placement.dbms_id),
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
         */
        if profile.write_ratio >= 0.70 {
            return OptimizationDecision {
                coordinate: profile.coordinate,
                action: OptimizationAction::Isolate,
                expected_gain: prediction.probability,
                estimated_cost: 0.45,
                confidence: prediction.probability,
            };
        }

        /*
         * Mixed workloads are candidates for moving the logical
         * workload to another physical DBMS node.
         */
        OptimizationDecision {
            coordinate: profile.coordinate,

            action: OptimizationAction::Move {
                target: alternate_node(&placement.dbms_id),
            },

            expected_gain: prediction.probability * 0.90,
            estimated_cost: 0.50,
            confidence: prediction.probability,
        }
    }

    pub fn predictor(&self) -> &WorkloadPredictor {
        &self.predictor
    }
}

fn alternate_node(current: &DbmsId) -> DbmsId {
    DbmsId::new(format!("{}-alternate", current.0))
}