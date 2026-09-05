//! The boundary between "the controller decided" and "something moved bytes".
//!
//! The controller is a decision-and-state crate: it validates, admits,
//! sequences, aborts and measures. It never opens a socket or touches a disk.
//! The mechanisms that do -- routing table updates, replica creation, shard
//! migration -- sit behind [`PlacementExecutor`].
//!
//! This is the shape README §3 asks for ("ML predicts, the optimizer decides,
//! a controller executes validated decisions"): one controller that knows how
//! to run *a plan*, and a small number of mechanisms that know how to produce
//! and perform one. A controller with a `match` arm per mechanism would have
//! to be reopened for every new one, and could not be exercised without them.

use fabric_core::DbmsId;
use fabric_optimizer::OptimizationAction;
use fabric_topology::Placement;
use serde::{Deserialize, Serialize};

use crate::target::ActionTarget;

/// A stable name for an optimizer action, for logs, errors and feedback.
pub fn action_label(action: &OptimizationAction) -> &'static str {
    match action {
        OptimizationAction::NoAction => "no-action",
        OptimizationAction::Replicate { .. } => "replicate",
        OptimizationAction::Move { .. } => "move",
        OptimizationAction::Split => "split",
        OptimizationAction::Isolate => "isolate",
        OptimizationAction::Colocate { .. } => "colocate",
    }
}

/// One stage of a placement change.
///
/// The split exists so that a plan has a well-defined point of no return.
/// Everything before [`Cutover`](PlanPhase::Cutover) is invisible to readers
/// and writers and can be thrown away; cutover is the instant authority moves;
/// cleanup is the only part that destroys anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlanPhase {
    /// Reserve capacity and stand up the destination. Nothing is observable
    /// yet and nothing has been destroyed.
    Prepare,

    /// Copy data to the destination while the source keeps serving.
    Transfer,

    /// Switch authority for the target to its new holder.
    Cutover,

    /// Release what the old holder no longer needs.
    Cleanup,
}

impl PlanPhase {
    /// The phases in the only order they may occur.
    pub const ORDER: [PlanPhase; 4] = [
        PlanPhase::Prepare,
        PlanPhase::Transfer,
        PlanPhase::Cutover,
        PlanPhase::Cleanup,
    ];

    pub fn rank(self) -> usize {
        match self {
            Self::Prepare => 0,
            Self::Transfer => 1,
            Self::Cutover => 2,
            Self::Cleanup => 3,
        }
    }

    /// Whether completing this phase can still be undone by discarding work,
    /// rather than by performing a compensating placement change.
    pub fn is_reversible(self) -> bool {
        self.rank() < Self::Cutover.rank()
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Transfer => "transfer",
            Self::Cutover => "cutover",
            Self::Cleanup => "cleanup",
        }
    }
}

/// How much of the data space a plan disturbs.
///
/// Used for conflict detection: two coordinate-scoped plans on different cells
/// of one shard can run together, but nothing may run alongside a shard-scoped
/// plan such as a split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanScope {
    Coordinate,
    Shard,
}

/// What the controller hands a mechanism when asking it to plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRequest {
    pub target: ActionTarget,
    pub action: OptimizationAction,

    /// Where the target lives right now, from the topology registry.
    pub source: Placement,

    /// The destination the decision named, when it named one. `Split` and
    /// `Isolate` carry none: choosing the node is the mechanism's job, and
    /// whatever it chooses is checked against the fleet before admission.
    pub destination: Option<DbmsId>,

    /// Every node currently believed to hold a copy of the target, in
    /// `DbmsId` order. A mechanism must not assume the source is the only one.
    pub replicas: Vec<DbmsId>,

    pub requested_at_ms: u64,
}

/// A validated, executable sequence of phases.
///
/// `adds` and `removes` are the plan's declared effect on the replica set.
/// They are the reason the controller can enforce a replica floor against a
/// mechanism it knows nothing else about: a plan that would leave a target
/// with no holder is refused before its first phase starts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementPlan {
    pub target: ActionTarget,
    pub action: OptimizationAction,
    pub scope: PlanScope,

    phases: Vec<PlanPhase>,

    /// Nodes that will hold a copy afterwards but do not now.
    pub adds: Vec<DbmsId>,

    /// Nodes that hold a copy now but will not afterwards.
    pub removes: Vec<DbmsId>,

    /// Data the plan expects to move. Zero for a plan that only changes
    /// authority.
    pub estimated_bytes: u64,

    /// How long the mechanism expects the whole plan to take.
    pub estimated_duration_ms: u64,
}

impl PlacementPlan {
    /// Build a plan, rejecting one that could not be executed coherently.
    ///
    /// The ordering check is not pedantry: the controller drives phases in the
    /// order the plan lists them, so an out-of-order plan would, for instance,
    /// delete the source before the copy existed.
    pub fn new(
        target: ActionTarget,
        action: OptimizationAction,
        scope: PlanScope,
        phases: Vec<PlanPhase>,
        adds: Vec<DbmsId>,
        removes: Vec<DbmsId>,
    ) -> Result<Self, PlanError> {
        if phases.is_empty() {
            return Err(PlanError::EmptyPlan);
        }

        let ordered = phases
            .windows(2)
            .all(|pair| pair[0].rank() < pair[1].rank());

        if !ordered {
            return Err(PlanError::PhasesOutOfOrder {
                phases: phases
                    .iter()
                    .map(|phase| phase.label().to_string())
                    .collect(),
            });
        }

        /*
         * Destroying a copy without a cutover means destroying a copy that is
         * still authoritative for somebody.
         */
        if !removes.is_empty()
            && !phases.contains(&PlanPhase::Cutover)
        {
            return Err(PlanError::Infeasible {
                reason: "a plan that removes a copy must cut over first"
                    .to_string(),
            });
        }

        Ok(Self {
            target,
            action,
            scope,
            phases,
            adds,
            removes,
            estimated_bytes: 0,
            estimated_duration_ms: 0,
        })
    }

    pub fn with_estimate(
        mut self,
        bytes: u64,
        duration_ms: u64,
    ) -> Self {
        self.estimated_bytes = bytes;
        self.estimated_duration_ms = duration_ms;
        self
    }

    pub fn phases(&self) -> &[PlanPhase] {
        &self.phases
    }

    pub fn first_phase(&self) -> PlanPhase {
        self.phases[0]
    }

    /// The phase after `phase`, or `None` if `phase` is the last one.
    pub fn phase_after(&self, phase: PlanPhase) -> Option<PlanPhase> {
        let position = self
            .phases
            .iter()
            .position(|candidate| *candidate == phase)?;

        self.phases.get(position + 1).copied()
    }

    /// Whether the plan moves authority for the target at some point.
    pub fn changes_authority(&self) -> bool {
        self.phases.contains(&PlanPhase::Cutover)
    }

    /// Nodes the plan touches, for per-node concurrency limits.
    pub fn touched_nodes(&self) -> Vec<DbmsId> {
        let mut nodes: Vec<DbmsId> = self
            .adds
            .iter()
            .chain(self.removes.iter())
            .cloned()
            .collect();

        nodes.sort_by(|a, b| a.0.cmp(&b.0));
        nodes.dedup_by(|a, b| a.0 == b.0);
        nodes
    }
}

/// Why a mechanism could not produce a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanError {
    /// This mechanism does not implement the requested action.
    UnsupportedAction { action: String },

    EmptyPlan,

    PhasesOutOfOrder { phases: Vec<String> },

    /// The mechanism understood the action but cannot carry it out in the
    /// current arrangement -- no candidate destination, nothing to split, and
    /// so on.
    Infeasible { reason: String },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedAction { action } => {
                write!(f, "mechanism does not implement action '{action}'")
            }

            Self::EmptyPlan => {
                write!(f, "plan contains no phases")
            }

            Self::PhasesOutOfOrder { phases } => {
                write!(
                    f,
                    "plan phases are out of order: [{}]",
                    phases.join(", ")
                )
            }

            Self::Infeasible { reason } => {
                write!(f, "plan is infeasible: {reason}")
            }
        }
    }
}

impl std::error::Error for PlanError {}

/// How far a running phase has got.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PhaseProgress {
    /// Still working. `fraction` is in `0.0..=1.0` and is advisory only --
    /// the controller advances on [`Complete`](PhaseProgress::Complete), never
    /// on a fraction reaching 1.0, so a mechanism cannot be misread as done.
    Running { fraction: f64 },

    Complete,
}

/// A mechanism's report that execution went wrong.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionFault {
    PhaseFailed { phase: PlanPhase, reason: String },

    /// The destination stopped being usable partway through. Distinguished
    /// from a generic failure because it is the fault most likely to be worth
    /// retrying elsewhere.
    DestinationLost { node: DbmsId },

    /// The controller was asked to poll a phase the mechanism never began.
    NotStarted { phase: PlanPhase },

    Internal { reason: String },
}

impl std::fmt::Display for ExecutionFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PhaseFailed { phase, reason } => {
                write!(f, "phase '{}' failed: {reason}", phase.label())
            }

            Self::DestinationLost { node } => {
                write!(f, "destination '{}' was lost mid-flight", node.0)
            }

            Self::NotStarted { phase } => {
                write!(f, "phase '{}' was polled before it began", phase.label())
            }

            Self::Internal { reason } => {
                write!(f, "mechanism error: {reason}")
            }
        }
    }
}

impl std::error::Error for ExecutionFault {}

/// What an abort actually achieved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollbackOutcome {
    /// Staged work was discarded and the arrangement is as it was.
    Restored,

    /// The plan had already passed its point of no return. The system is in a
    /// valid state but not the original one, and returning to the original
    /// requires a new, compensating decision -- the controller will not
    /// synthesise one, because inventing an unvalidated action is exactly what
    /// this crate exists to prevent.
    Irreversible { reason: String },
}

/// The seam the routing, replication and migration crates implement.
///
/// The controller owns *when* and *whether*; an implementation owns *how*.
/// Implementations are pure with respect to this crate: the controller only
/// ever calls these methods with a clock value it was given, so a mechanism
/// backed by a simulator is as valid as one backed by a real fleet.
pub trait PlacementExecutor {
    /// Stable identifier, recorded on every execution so that history says
    /// which mechanism ran it.
    fn name(&self) -> &str;

    /// Whether this mechanism implements the action. Consulted before
    /// [`plan`](PlacementExecutor::plan); an action no registered mechanism
    /// supports is refused at validation rather than half-started.
    fn supports(&self, action: &OptimizationAction) -> bool;

    /// Produce a plan. Must have no side effects: the controller validates the
    /// returned plan and may refuse it, and a refused plan must leave nothing
    /// behind.
    fn plan(&self, request: &PlanRequest) -> Result<PlacementPlan, PlanError>;

    /// Begin one phase. Called at most once per phase per plan.
    fn begin_phase(
        &mut self,
        plan: &PlacementPlan,
        phase: PlanPhase,
        at_ms: u64,
    ) -> Result<(), ExecutionFault>;

    /// Report progress on the phase currently begun.
    fn poll_phase(
        &mut self,
        plan: &PlacementPlan,
        phase: PlanPhase,
        at_ms: u64,
    ) -> Result<PhaseProgress, ExecutionFault>;

    /// Undo as much of `completed` (plus any phase in flight) as can be
    /// undone. Called on operator abort, on fault, and on phase timeout.
    fn abort(
        &mut self,
        plan: &PlacementPlan,
        completed: &[PlanPhase],
        at_ms: u64,
    ) -> Result<RollbackOutcome, ExecutionFault>;
}
