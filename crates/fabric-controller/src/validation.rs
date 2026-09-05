//! Every reason the controller will refuse to act.
//!
//! Rejections are typed and carry their evidence. A `bool` or a bare string
//! would make the control plane unexplainable: an operator asking "why did
//! nothing happen?" needs the rule, the measured value and the limit, and the
//! ML feedback loop needs to distinguish "we declined because the model was
//! not confident" from "we declined because the destination was on fire".

use fabric_core::{Coordinate, DbmsId};
use serde::{Deserialize, Serialize};

use crate::fleet::NodeCondition;
use crate::plan::{PlanError, PlanPhase};
use crate::target::{ActionId, ActionTarget};

/// Why a decision was not executed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValidationError {
    /// The optimizer proposed doing nothing. Not an error in the system, but
    /// not something to execute either.
    NoActionRequested,

    /// The decision does not clear the controller's own confidence/benefit
    /// floor. The optimizer's `should_execute` is its opinion of its own
    /// output; this is the control plane's.
    BelowExecutionThreshold {
        score: f64,
        confidence: f64,
        min_confidence: f64,
    },

    /// The coordinate is not a cell of a 12x13 grid.
    InvalidCoordinate { coordinate: Coordinate },

    /// The observation behind the decision is older than the policy allows.
    /// The workload that justified the action is no longer evidence.
    StaleDecision {
        observed_at_ms: u64,
        now_ms: u64,
        age_ms: u64,
        max_age_ms: u64,
    },

    /// The decision claims to be based on an observation the control plane has
    /// not seen yet. Accepting it would let a clock-skewed or malicious
    /// producer keep a decision permanently fresh.
    DecisionFromTheFuture { observed_at_ms: u64, now_ms: u64 },

    /// The placement map changed after the decision was computed, so the
    /// decision was reasoning about an arrangement that no longer exists.
    TopologySuperseded {
        decision_generation: u64,
        current_generation: u64,
    },

    /// Nothing in the topology holds this target, so there is nothing to move,
    /// replicate or isolate.
    UnknownPlacement { target: ActionTarget },

    /// The destination is not a node the fleet knows about. Executing onto an
    /// unregistered id would place data somewhere the control plane cannot
    /// subsequently observe or recover.
    UnknownDestination { node: DbmsId },

    DestinationUnhealthy {
        node: DbmsId,
        condition: NodeCondition,
    },

    /// The destination has no placement headroom left.
    DestinationAtCapacity {
        node: DbmsId,
        hosted: usize,
        capacity: usize,
    },

    /// The destination has headroom but is already resource-saturated. Moving
    /// a hotspot onto a hot node relocates the problem.
    DestinationSaturated {
        node: DbmsId,
        utilization: f64,
        limit: f64,
    },

    /// The destination already holds the target: a move to where it already
    /// is, or a second copy on the same node, which is not a replica.
    DestinationIsSource { node: DbmsId },

    /// A `Colocate` named a coordinate that is not placed anywhere.
    ColocationTargetMissing {
        shard_id: u64,
        coordinate: Coordinate,
    },

    /// Another action is already executing against this exact target.
    ConflictingAction {
        target: ActionTarget,
        existing: ActionId,
        existing_action: String,
    },

    /// A shard-wide action is running on this shard, or this action is
    /// shard-wide and the shard is already busy.
    ShardBusy { shard_id: u64, existing: ActionId },

    /// A node the plan touches is already at its concurrent-action limit.
    NodeBusy {
        node: DbmsId,
        in_flight: usize,
        limit: usize,
    },

    /// The controller is already running as many actions as policy allows.
    TooManyInFlight { in_flight: usize, limit: usize },

    /// The plan would leave the target with fewer copies than the replica
    /// floor -- at the default floor of one, it would leave the data nowhere.
    /// This is checked against the plan's declared effect, not against the
    /// action's name, so a mechanism cannot smuggle a deletion past it.
    WouldDropLastReplica {
        target: ActionTarget,
        remaining: usize,
        minimum: usize,
    },

    /// The plan claims to remove a copy from a node that does not hold one.
    /// Accepting it would corrupt the controller's replica ledger and make
    /// every later replica-floor check wrong.
    RemovesUnheldCopy { target: ActionTarget, node: DbmsId },

    /// No registered mechanism implements this action. This is the honest
    /// answer while `fabric-routing` / `fabric-replication` /
    /// `fabric-migration` are unwired: the controller declines rather than
    /// pretending to have executed something.
    NoMechanism { action: String },

    /// A mechanism was asked to plan and refused.
    PlanRejected { error: PlanError },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoActionRequested => {
                write!(f, "decision requests no action")
            }

            Self::BelowExecutionThreshold {
                score,
                confidence,
                min_confidence,
            } => write!(
                f,
                "decision below execution threshold: score {score:.3}, \
                 confidence {confidence:.3} (minimum {min_confidence:.3})"
            ),

            Self::InvalidCoordinate { coordinate } => write!(
                f,
                "coordinate ({},{}) is outside the 12x13 grid",
                coordinate.x, coordinate.y
            ),

            Self::StaleDecision {
                observed_at_ms,
                now_ms,
                age_ms,
                max_age_ms,
            } => write!(
                f,
                "decision is stale: observed at {observed_at_ms}, now {now_ms} \
                 ({age_ms}ms old, limit {max_age_ms}ms)"
            ),

            Self::DecisionFromTheFuture {
                observed_at_ms,
                now_ms,
            } => write!(
                f,
                "decision claims observation at {observed_at_ms}, ahead of the \
                 control plane clock at {now_ms}"
            ),

            Self::TopologySuperseded {
                decision_generation,
                current_generation,
            } => write!(
                f,
                "decision was computed against topology generation \
                 {decision_generation}; the fleet is now at {current_generation}"
            ),

            Self::UnknownPlacement { target } => {
                write!(f, "no placement is recorded for {target}")
            }

            Self::UnknownDestination { node } => write!(
                f,
                "destination '{}' is not a registered node",
                node.0
            ),

            Self::DestinationUnhealthy { node, condition } => write!(
                f,
                "destination '{}' is {} and may not receive placements",
                node.0,
                condition.label()
            ),

            Self::DestinationAtCapacity {
                node,
                hosted,
                capacity,
            } => write!(
                f,
                "destination '{}' is at capacity: {hosted}/{capacity} placements",
                node.0
            ),

            Self::DestinationSaturated {
                node,
                utilization,
                limit,
            } => write!(
                f,
                "destination '{}' is saturated at {utilization:.3} utilization \
                 (limit {limit:.3})",
                node.0
            ),

            Self::DestinationIsSource { node } => write!(
                f,
                "destination '{}' already holds the target",
                node.0
            ),

            Self::ColocationTargetMissing {
                shard_id,
                coordinate,
            } => write!(
                f,
                "colocation target shard {shard_id} ({},{}) is not placed",
                coordinate.x, coordinate.y
            ),

            Self::ConflictingAction {
                target,
                existing,
                existing_action,
            } => write!(
                f,
                "{existing} ({existing_action}) is already executing on {target}"
            ),

            Self::ShardBusy {
                shard_id,
                existing,
            } => write!(
                f,
                "shard {shard_id} is held by a shard-wide {existing}"
            ),

            Self::NodeBusy {
                node,
                in_flight,
                limit,
            } => write!(
                f,
                "node '{}' already has {in_flight} action(s) in flight (limit {limit})",
                node.0
            ),

            Self::TooManyInFlight { in_flight, limit } => write!(
                f,
                "{in_flight} actions already in flight (limit {limit})"
            ),

            Self::WouldDropLastReplica {
                target,
                remaining,
                minimum,
            } => write!(
                f,
                "plan would leave {target} with {remaining} copies (minimum {minimum})"
            ),

            Self::RemovesUnheldCopy { target, node } => write!(
                f,
                "plan removes a copy of {target} from '{}', which holds none",
                node.0
            ),

            Self::NoMechanism { action } => write!(
                f,
                "no registered mechanism implements '{action}'"
            ),

            Self::PlanRejected { error } => {
                write!(f, "{error}")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Why the controller refused to record an after-measurement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MeasurementError {
    UnknownAction { id: ActionId },

    /// The action is not in a state where a measurement means anything --
    /// still executing, or already concluded.
    NotAwaitingMeasurement { id: ActionId, state: String },

    /// Measuring before the system has settled would attribute transfer noise
    /// to the decision. README §7 asks whether the decision *improved* things,
    /// which a measurement taken mid-rebalance cannot answer.
    TooSoon {
        id: ActionId,
        elapsed_ms: u64,
        settle_ms: u64,
    },
}

impl std::fmt::Display for MeasurementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownAction { id } => {
                write!(f, "{id} is not known to this controller")
            }

            Self::NotAwaitingMeasurement { id, state } => {
                write!(f, "{id} is {state}, not awaiting measurement")
            }

            Self::TooSoon {
                id,
                elapsed_ms,
                settle_ms,
            } => write!(
                f,
                "{id} committed {elapsed_ms}ms ago; the system settles for \
                 {settle_ms}ms before a measurement counts"
            ),
        }
    }
}

impl std::error::Error for MeasurementError {}

/// Why an in-flight action was torn down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortReason {
    /// Someone asked.
    Requested,

    /// The mechanism reported a fault.
    Fault { fault: crate::plan::ExecutionFault },

    /// A phase ran past the policy limit without completing.
    PhaseTimeout {
        phase: PlanPhase,
        elapsed_ms: u64,
        limit_ms: u64,
    },

    /// A node the plan depends on stopped being usable while the plan ran.
    /// Detected by the controller from the fleet view, independently of
    /// whether the mechanism noticed.
    NodeLost {
        node: DbmsId,
        condition: NodeCondition,
    },
}

impl std::fmt::Display for AbortReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Requested => write!(f, "abort requested"),

            Self::Fault { fault } => write!(f, "{fault}"),

            Self::PhaseTimeout {
                phase,
                elapsed_ms,
                limit_ms,
            } => write!(
                f,
                "phase '{}' ran {elapsed_ms}ms without completing (limit {limit_ms}ms)",
                phase.label()
            ),

            Self::NodeLost { node, condition } => write!(
                f,
                "node '{}' became {} mid-flight",
                node.0,
                condition.label()
            ),
        }
    }
}
