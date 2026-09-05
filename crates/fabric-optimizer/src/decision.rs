use fabric_core::{Coordinate, DbmsId};
use serde::{Deserialize, Serialize};

/// What the optimizer decided should happen to a cell.
///
/// The variants divide on one line: whether choosing the node is part of the
/// decision or a placement detail left to the mechanism.
///
/// [`Replicate`](Self::Replicate) and [`Move`](Self::Move) name a node,
/// because which node they land on *is* the decision — a replica belongs
/// somewhere the first copy is not, a move belongs somewhere cooler than where
/// it is now, and those are judgements made from workload knowledge. The node
/// named must be one the [`Fleet`](crate::Fleet) the decision was made against
/// contains: the controller refuses an unknown id with `UnknownDestination`,
/// and it is right to.
///
/// [`Split`](Self::Split) and [`Isolate`](Self::Isolate) name none. Isolation
/// is defined by the source — get this workload off the node it is contending
/// on — so any separate node satisfies it and the mechanism picks with the
/// same spread rules every other copy is placed by.
/// [`Colocate`](Self::Colocate) names a *coordinate*, not a node: "put this
/// where that is", and where that is is a question only the topology can
/// answer, so it is resolved during validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptimizationAction {
    NoAction,

    Replicate {
        target: DbmsId,
    },

    Move {
        target: DbmsId,
    },

    Split,

    Isolate,

    Colocate {
        target: Coordinate,
    },
}

/// A decision about one cell, named the same way [`WorkloadProfile`] names it.
///
/// `shard_id` was added alongside `WorkloadProfile::shard_id`: a decision is
/// computed from a profile that now names a cell unambiguously, and carrying
/// the same identity forward is what lets `fabric-controller`'s
/// `DecisionEnvelope` build its `ActionTarget` from the decision itself rather
/// than taking a second, possibly-disagreeing `shard_id` from the caller.
///
/// [`WorkloadProfile`]: fabric_workload::WorkloadProfile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationDecision {
    pub shard_id: u64,
    pub coordinate: Coordinate,
    pub action: OptimizationAction,

    /// Expected improvement produced by the action.
    pub expected_gain: f64,

    /// Estimated operational cost of the action.
    pub estimated_cost: f64,

    /// Confidence in the decision.
    pub confidence: f64,
}

impl OptimizationDecision {
    pub fn score(&self) -> f64 {
        self.expected_gain - self.estimated_cost
    }

    pub fn should_execute(&self) -> bool {
        !matches!(self.action, OptimizationAction::NoAction)
            && self.score() > 0.0
            && self.confidence >= 0.80
    }
}