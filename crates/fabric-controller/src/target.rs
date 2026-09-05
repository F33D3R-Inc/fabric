//! What a control action points at, and how one is named.

use std::cmp::Ordering;

use fabric_core::Coordinate;
use serde::{Deserialize, Serialize};

/// The logical location a control action operates on.
///
/// A coordinate alone is not addressable: the same `(x, y)` cell exists in
/// every shard, so `TopologyRegistry` is keyed by `(shard_id, coordinate)`.
/// [`OptimizationDecision`](fabric_optimizer::OptimizationDecision) itself
/// names both -- it is computed from a `WorkloadProfile` that does the same --
/// so `ActionTarget` is built straight from the decision rather than from a
/// separately-supplied shard id (see
/// [`DecisionEnvelope`](crate::DecisionEnvelope)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActionTarget {
    pub shard_id: u64,
    pub coordinate: Coordinate,
}

impl ActionTarget {
    pub const fn new(shard_id: u64, coordinate: Coordinate) -> Self {
        Self {
            shard_id,
            coordinate,
        }
    }

    /// Whether this target names a cell that can exist at all.
    pub fn is_valid(&self) -> bool {
        self.coordinate.is_valid()
    }
}

/*
 * `Coordinate` is deliberately not `Ord` in fabric-core, but the controller
 * keys ordered maps by target so that every listing -- in-flight actions,
 * history, feedback reports -- comes out in a stable order without a sort at
 * the call site. Grid index is the natural total order over a 12x13 grid.
 */
impl Ord for ActionTarget {
    fn cmp(&self, other: &Self) -> Ordering {
        self.shard_id
            .cmp(&other.shard_id)
            .then_with(|| {
                self.coordinate
                    .index()
                    .cmp(&other.coordinate.index())
            })
    }
}

impl PartialOrd for ActionTarget {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for ActionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard {} ({},{})",
            self.shard_id, self.coordinate.x, self.coordinate.y
        )
    }
}

/// Identity of one admitted control action.
///
/// Monotonic within a controller instance. It is the handle used to drive,
/// abort, measure and later explain an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActionId(pub u64);

impl std::fmt::Display for ActionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "action-{}", self.0)
    }
}
