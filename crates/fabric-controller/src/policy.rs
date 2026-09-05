//! The thresholds the controller enforces.
//!
//! Every rule in [`crate::validation`] reads its limit from here rather than
//! from a literal buried in a branch, so that "what would this controller
//! refuse?" is one struct rather than an archaeology exercise.

use serde::{Deserialize, Serialize};

/// Execution policy: the bounds inside which the controller is willing to act.
///
/// The defaults are deliberately conservative. A control plane that will
/// happily run eight simultaneous migrations onto a node at 90% CPU is not a
/// control plane, it is an outage generator.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ControllerPolicy {
    /// How old a decision's underlying observation may be and still be acted
    /// on. Past this, the workload that justified the action is no longer
    /// evidence that the action is still right.
    pub max_decision_age_ms: u64,

    /// Confidence floor. Mirrors the optimizer's own `should_execute` gate but
    /// is re-derived here: the optimizer's opinion of its own output is not
    /// authorization.
    pub min_confidence: f64,

    /// Copies of a target that must survive any plan. One means "never drop
    /// the last replica".
    pub min_replicas: usize,

    /// Utilization above which a node is considered over capacity and may not
    /// receive new work, regardless of its placement headroom.
    pub max_node_utilization: f64,

    /// Global cap on simultaneously executing actions.
    pub max_concurrent_actions: usize,

    /// Cap on simultaneously executing actions touching any one node, source
    /// or destination.
    pub max_actions_per_node: usize,

    /// How long a single phase may run before it is treated as hung and the
    /// action is rolled back.
    pub phase_timeout_ms: u64,

    /// How long after execution the system must settle before an
    /// after-measurement means anything.
    pub measurement_settle_ms: u64,

    /// How long after execution the controller will wait for an
    /// after-measurement before giving up and reporting
    /// [`OutcomeVerdict::NotMeasured`](crate::OutcomeVerdict::NotMeasured).
    /// Without this an unmeasured action would hold its target forever.
    pub measurement_deadline_ms: u64,

    /// Pressure change smaller than this counts as no change. Without a noise
    /// floor every action "improves" or "regresses" something and the
    /// feedback signal is worthless.
    pub outcome_noise_floor: f64,
}

impl Default for ControllerPolicy {
    fn default() -> Self {
        Self {
            max_decision_age_ms: 15_000,
            min_confidence: 0.80,
            min_replicas: 1,
            max_node_utilization: 0.85,
            max_concurrent_actions: 8,
            max_actions_per_node: 2,
            phase_timeout_ms: 60_000,
            measurement_settle_ms: 30_000,
            measurement_deadline_ms: 300_000,
            outcome_noise_floor: 0.02,
        }
    }
}
