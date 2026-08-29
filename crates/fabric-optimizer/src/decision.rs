use fabric_core::{Coordinate, DbmsId};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationDecision {
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