//! # fabric-controller
//!
//! The execution arm of the Fabric control loop.
//!
//! ```text
//! Telemetry -> Workload -> ML -> Optimizer -> [ CONTROLLER ] -> FacetQL
//!                            ^                      |
//!                            +------ Measure -------+
//! ```
//!
//! `fabric-optimizer` produces an [`OptimizationDecision`]. This crate decides
//! whether that decision may be executed, sequences the execution, tears it
//! down when it goes wrong, and -- the part that makes it a *control* loop
//! rather than a pipeline -- measures whether it actually helped and hands
//! that back as a training signal.
//!
//! ## Three rules the design exists to enforce
//!
//! **A recommendation is not an authorization.** README §4: *"Machine learning
//! may recommend an action. The database system determines whether that action
//! is valid."* Nothing here executes an `OptimizationDecision`; execution
//! takes a [`ValidatedDecision`], which only [`FabricController::validate`]
//! can produce, and which every rule in [`ValidationError`] can refuse.
//!
//! **Two actions may not fight over the same data.** [`ActionRegistry`] takes
//! an interlock on the target, the shard and the touched nodes at admission,
//! before the first phase starts.
//!
//! **Executed is not successful.** README §7: *"a topology decision is not
//! considered successful merely because it was executed -- the Fabric must
//! measure whether the decision actually improved the system."* A finished
//! plan lands in [`ExecutionState::AwaitingMeasurement`], not `Completed`, and
//! produces an [`OutcomeReport`] carrying a verdict -- including
//! [`OutcomeVerdict::Unchanged`] for an action that cost something and bought
//! nothing.
//!
//! ## Where the mechanisms go
//!
//! This crate performs no IO and holds no topology. The things that move bytes
//! -- routing table updates, replica creation, shard migration -- implement
//! [`PlacementExecutor`], and the controller drives them through phases
//! (`Prepare -> Transfer -> Cutover -> Cleanup`) it understands generically.
//! `fabric-routing`, `fabric-replication` and `fabric-migration` are the
//! intended implementors; none is wired in yet, and until one is, an action no
//! registered mechanism claims is refused with
//! [`ValidationError::NoMechanism`] rather than silently treated as done.
//! `fabric-simulator` implements the same trait against a synthetic cluster,
//! which is how the loop is exercised end to end today.

pub mod controller;
pub mod decision;
pub mod execution;
pub mod fleet;
pub mod outcome;
pub mod plan;
pub mod policy;
pub mod replica;
pub mod target;
pub mod validation;

pub use controller::FabricController;

pub use decision::{
    DecisionEnvelope,
    ValidatedDecision,
};

pub use execution::{
    ActionRegistry,
    ExecutionRecord,
    ExecutionState,
    PlacementChange,
};

pub use fleet::{
    ControlPlaneView,
    FleetView,
    NodeCondition,
    NodeStatus,
};

pub use outcome::{
    ExecutionOutcome,
    MeasurementSnapshot,
    OutcomeReport,
    OutcomeVerdict,
};

pub use plan::{
    ExecutionFault,
    PhaseProgress,
    PlacementExecutor,
    PlacementPlan,
    PlanError,
    PlanPhase,
    PlanRequest,
    PlanScope,
    RollbackOutcome,
    action_label,
};

pub use policy::ControllerPolicy;

pub use replica::ReplicaLedger;

pub use target::{
    ActionId,
    ActionTarget,
};

pub use validation::{
    AbortReason,
    MeasurementError,
    ValidationError,
};

#[doc(no_inline)]
pub use fabric_optimizer::{
    OptimizationAction,
    OptimizationDecision,
};
