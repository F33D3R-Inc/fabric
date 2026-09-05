//! Whether the decision actually helped.
//!
//! README §7: *"a topology decision is not considered successful merely
//! because it was executed -- the Fabric must measure whether the decision
//! actually improved the system."* Execution success is a fact about the
//! mechanism; it says nothing about the workload. This module is the part of
//! the controller that closes the loop back to telemetry and ML.

use fabric_optimizer::{OptimizationAction, OptimizationDecision};
use fabric_workload::{PressureLevel, WorkloadProfile};
use serde::{Deserialize, Serialize};

use crate::target::{ActionId, ActionTarget};

/// The state of a target at one instant, taken from its workload profile.
///
/// A copy rather than a reference to the profile: the whole point is to hold
/// on to what the world looked like *before*, which a live profile stops being
/// the moment the next observation lands.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MeasurementSnapshot {
    pub taken_at_ms: u64,

    pub operations_per_second: f64,
    pub read_ratio: f64,
    pub write_ratio: f64,

    pub read_latency_us: f64,
    pub write_latency_us: f64,

    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub queue_depth: u64,

    pub pressure_score: f64,
    pub pressure: PressureLevel,
}

impl MeasurementSnapshot {
    pub fn from_profile(profile: &WorkloadProfile, taken_at_ms: u64) -> Self {
        Self {
            taken_at_ms,
            operations_per_second: profile.operations_per_second,
            read_ratio: profile.read_ratio,
            write_ratio: profile.write_ratio,
            read_latency_us: profile.read_latency_us,
            write_latency_us: profile.write_latency_us,
            cpu_utilization: profile.cpu_utilization,
            memory_utilization: profile.memory_utilization,
            queue_depth: profile.queue_depth,
            pressure_score: profile.pressure_score,
            pressure: profile.pressure,
        }
    }

    /// The worse of the two latencies, which is the one a client notices.
    pub fn worst_latency_us(&self) -> f64 {
        self.read_latency_us.max(self.write_latency_us)
    }
}

/// The verdict on an action, and the value fed back to the learning loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeVerdict {
    /// Pressure fell by more than the noise floor.
    Improved,

    /// Pressure moved by less than the noise floor. The action cost something
    /// and bought nothing measurable -- a negative signal, not a neutral one.
    Unchanged,

    /// Pressure rose by more than the noise floor. The optimizer's model was
    /// wrong about this situation.
    Regressed,

    /// The action was torn down before it finished.
    RolledBack,

    /// The action failed, or its rollback could not restore the original
    /// arrangement.
    Failed,

    /// Executed, but nobody ever supplied an after-measurement inside the
    /// policy deadline. Reported honestly rather than scored as a success.
    NotMeasured,
}

impl OutcomeVerdict {
    /// Whether the decision is evidence *for* the policy that produced it.
    pub fn is_success(self) -> bool {
        matches!(self, Self::Improved)
    }

    /// Whether the decision reached a measurable conclusion at all.
    pub fn is_measured(self) -> bool {
        matches!(self, Self::Improved | Self::Unchanged | Self::Regressed)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Improved => "improved",
            Self::Unchanged => "unchanged",
            Self::Regressed => "regressed",
            Self::RolledBack => "rolled-back",
            Self::Failed => "failed",
            Self::NotMeasured => "not-measured",
        }
    }
}

/// A before/after pair and what it says about the decision.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ExecutionOutcome {
    pub before: MeasurementSnapshot,
    pub after: MeasurementSnapshot,

    /// What the optimizer said the action would be worth.
    pub expected_gain: f64,

    /// What it was actually worth: the drop in pressure. Positive is better.
    pub realized_gain: f64,

    /// `realized_gain / expected_gain`, or `0.0` when nothing was promised.
    /// This is the number a future model is trained to make approach 1.0.
    pub gain_ratio: f64,

    /// Change in worst-case latency. Negative is better.
    pub latency_delta_us: f64,

    /// Change in throughput. Positive is more work served.
    pub throughput_delta: f64,

    pub verdict: OutcomeVerdict,
}

impl ExecutionOutcome {
    /// Judge an executed action.
    ///
    /// Deliberately scored on *pressure*, the same quantity the optimizer and
    /// the hotspot model reason about (`WorkloadProfile::pressure_score`).
    /// Scoring on a different quantity than the one that triggered the action
    /// would produce feedback the model cannot learn from.
    pub fn evaluate(
        before: MeasurementSnapshot,
        after: MeasurementSnapshot,
        decision: &OptimizationDecision,
        noise_floor: f64,
    ) -> Self {
        let realized_gain = before.pressure_score - after.pressure_score;

        let gain_ratio = if decision.expected_gain > 0.0 {
            realized_gain / decision.expected_gain
        } else {
            0.0
        };

        let verdict = if realized_gain > noise_floor {
            OutcomeVerdict::Improved
        } else if realized_gain < -noise_floor {
            OutcomeVerdict::Regressed
        } else {
            OutcomeVerdict::Unchanged
        };

        Self {
            before,
            after,
            expected_gain: decision.expected_gain,
            realized_gain,
            gain_ratio,
            latency_delta_us: after.worst_latency_us()
                - before.worst_latency_us(),
            throughput_delta: after.operations_per_second
                - before.operations_per_second,
            verdict,
        }
    }
}

/// One row of the learning loop's training signal.
///
/// Serialisable because this is the thing that leaves the controller: it is
/// what "Act -> Measure -> Learn" hands back to telemetry and, later, to the
/// historical dataset the ML layer trains on. Every concluded action produces
/// one, including the failures -- a control plane that only reports its
/// successes teaches a model to be overconfident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutcomeReport {
    pub action_id: ActionId,
    pub target: ActionTarget,
    pub action: OptimizationAction,
    pub action_label: String,
    pub mechanism: String,

    /// What the model believed when it proposed this.
    pub confidence: f64,
    pub expected_gain: f64,
    pub estimated_cost: f64,

    pub realized_gain: f64,
    pub gain_ratio: f64,
    pub verdict: OutcomeVerdict,

    pub admitted_at_ms: u64,
    pub concluded_at_ms: u64,

    /// Present only when a before/after pair was actually taken.
    pub measurement: Option<ExecutionOutcome>,

    /// Why the action ended badly, when it did.
    pub failure: Option<String>,
}

impl OutcomeReport {
    /// How wrong the optimizer was, as a signed error on the promised gain.
    /// Zero means the action delivered exactly what was predicted.
    pub fn prediction_error(&self) -> f64 {
        self.realized_gain - self.expected_gain
    }

    /// Whether this row is evidence the decision policy is working.
    pub fn is_success(&self) -> bool {
        self.verdict.is_success()
    }
}
