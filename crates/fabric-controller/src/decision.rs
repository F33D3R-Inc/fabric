//! A decision on its way in, and the proof that it survived validation.

use fabric_core::DbmsId;
use fabric_optimizer::{OptimizationAction, OptimizationDecision};
use fabric_topology::Placement;
use serde::{Deserialize, Serialize};

use crate::plan::action_label;
use crate::target::ActionTarget;

/// An optimizer decision, bound to everything needed to judge it.
///
/// `OptimizationDecision` names a cell -- `shard_id` and `coordinate` -- but it
/// carries no notion of *when* it was true. That gap is safety-critical: it is
/// impossible to tell a fresh decision from one computed against a fleet that
/// has since changed without it. The envelope closes it at the point the
/// decision enters the control path, and builds `target` from the decision's
/// own `shard_id`/`coordinate` rather than taking a second copy from the
/// caller -- there is exactly one place that says which cell a decision is
/// about.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionEnvelope {
    pub decision: OptimizationDecision,
    pub target: ActionTarget,

    /// Timestamp of the observation the decision was computed from.
    pub observed_at_ms: u64,

    /// [`FleetView::generation`](crate::FleetView::generation) at the moment
    /// the decision was computed.
    pub topology_generation: u64,
}

impl DecisionEnvelope {
    pub fn new(
        decision: OptimizationDecision,
        observed_at_ms: u64,
        topology_generation: u64,
    ) -> Self {
        let target = ActionTarget::new(decision.shard_id, decision.coordinate);

        Self {
            decision,
            target,
            observed_at_ms,
            topology_generation,
        }
    }

    pub fn action(&self) -> &OptimizationAction {
        &self.decision.action
    }

    pub fn action_label(&self) -> &'static str {
        action_label(&self.decision.action)
    }

    /// The node the decision explicitly names, if any.
    ///
    /// `Split` and `Isolate` name none by construction: they say *what* should
    /// change, not where it should land. `Colocate` names a coordinate, not a
    /// node, so its destination is resolved from the topology during
    /// validation rather than read off the decision.
    pub fn named_destination(&self) -> Option<&DbmsId> {
        match &self.decision.action {
            OptimizationAction::Replicate { target }
            | OptimizationAction::Move { target } => Some(target),

            OptimizationAction::NoAction
            | OptimizationAction::Split
            | OptimizationAction::Isolate
            | OptimizationAction::Colocate { .. } => None,
        }
    }
}

/// A decision that passed every rule in [`crate::validation`].
///
/// It exists as a distinct type so that "validated" is a thing the compiler
/// can see. Execution takes one of these; there is no path that executes a
/// bare `OptimizationDecision`, which is README §4 ("machine learning may
/// recommend an action; the database system determines whether that action is
/// valid") expressed in the type system rather than in a comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatedDecision {
    envelope: DecisionEnvelope,
    source: Placement,
    destination: Option<DbmsId>,
    destination_region: Option<String>,
    validated_at_ms: u64,
}

impl ValidatedDecision {
    pub(crate) fn new(
        envelope: DecisionEnvelope,
        source: Placement,
        destination: Option<DbmsId>,
        destination_region: Option<String>,
        validated_at_ms: u64,
    ) -> Self {
        Self {
            envelope,
            source,
            destination,
            destination_region,
            validated_at_ms,
        }
    }

    pub fn envelope(&self) -> &DecisionEnvelope {
        &self.envelope
    }

    pub fn decision(&self) -> &OptimizationDecision {
        &self.envelope.decision
    }

    pub fn target(&self) -> ActionTarget {
        self.envelope.target
    }

    /// Where the target lives now.
    pub fn source(&self) -> &Placement {
        &self.source
    }

    /// The resolved destination node, where the action has one.
    pub fn destination(&self) -> Option<&DbmsId> {
        self.destination.as_ref()
    }

    pub fn destination_region(&self) -> Option<&str> {
        self.destination_region.as_deref()
    }

    pub fn validated_at_ms(&self) -> u64 {
        self.validated_at_ms
    }
}
