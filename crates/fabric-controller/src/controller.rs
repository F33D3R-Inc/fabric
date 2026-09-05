//! The control loop's execution arm.
//!
//! The order is fixed and is the whole point of the crate:
//!
//! ```text
//! validate  ->  plan  ->  validate the plan  ->  admit  ->  execute  ->  measure
//! ```
//!
//! Nothing skips a step. A decision arrives as a proposal, is checked against
//! the fleet as it is *now*, is turned into a plan by a mechanism, and the plan
//! is checked again -- because the mechanism, not the optimizer, is what
//! decides which nodes actually gain and lose copies. Only then does anything
//! run, and running is not the end: an action that was executed but never
//! measured is reported as `NotMeasured`, not as a success.

use fabric_core::DbmsId;
use fabric_optimizer::OptimizationAction;
use fabric_topology::TopologyRegistry;
use fabric_workload::WorkloadProfile;

use crate::decision::{DecisionEnvelope, ValidatedDecision};
use crate::execution::{ActionRegistry, ExecutionRecord, ExecutionState};
use crate::fleet::ControlPlaneView;
use crate::outcome::{ExecutionOutcome, MeasurementSnapshot, OutcomeReport};
use crate::plan::{
    ExecutionFault, PhaseProgress, PlacementExecutor, PlacementPlan, PlanError, PlanRequest,
    PlanScope, RollbackOutcome, action_label,
};
use crate::policy::ControllerPolicy;
use crate::replica::ReplicaLedger;
use crate::target::ActionId;
use crate::validation::{AbortReason, MeasurementError, ValidationError};

/// The Fabric's execution arm.
///
/// Holds no topology of its own and performs no IO. Everything it knows about
/// the world arrives as a [`ControlPlaneView`]; everything it does to the world
/// goes through a [`PlacementExecutor`].
pub struct FabricController {
    policy: ControllerPolicy,
    executors: Vec<Box<dyn PlacementExecutor>>,
    actions: ActionRegistry,
    ledger: ReplicaLedger,
}

impl Default for FabricController {
    fn default() -> Self {
        Self::new(ControllerPolicy::default())
    }
}

impl std::fmt::Debug for FabricController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FabricController")
            .field("policy", &self.policy)
            .field("mechanisms", &self.mechanisms())
            .field("in_flight", &self.actions.active_len())
            .field("records", &self.actions.len())
            .finish()
    }
}

impl FabricController {
    pub fn new(policy: ControllerPolicy) -> Self {
        Self {
            policy,
            executors: Vec::new(),
            actions: ActionRegistry::new(),
            ledger: ReplicaLedger::new(),
        }
    }

    /// Register a mechanism.
    ///
    /// Order matters only in that the first mechanism claiming to support an
    /// action gets it; mechanisms are expected to claim disjoint actions.
    pub fn register_executor(
        &mut self,
        executor: Box<dyn PlacementExecutor>,
    ) {
        self.executors.push(executor);
    }

    pub fn mechanisms(&self) -> Vec<&str> {
        self.executors
            .iter()
            .map(|executor| executor.name())
            .collect()
    }

    pub fn policy(&self) -> &ControllerPolicy {
        &self.policy
    }

    pub fn actions(&self) -> &ActionRegistry {
        &self.actions
    }

    pub fn ledger(&self) -> &ReplicaLedger {
        &self.ledger
    }

    /// Fold a topology snapshot into the replica ledger.
    pub fn seed_replicas(&mut self, topology: &TopologyRegistry) {
        self.ledger.seed_from_topology(topology);
    }

    pub fn record(&self, id: ActionId) -> Option<&ExecutionRecord> {
        self.actions.get(id)
    }

    pub fn in_flight(&self) -> impl Iterator<Item = &ExecutionRecord> {
        self.actions.active()
    }

    /// The feedback rows for every concluded action, oldest first. This is
    /// what the learning loop consumes.
    pub fn reports(&self) -> Vec<OutcomeReport> {
        self.actions
            .records()
            .filter(|record| record.state().is_terminal())
            .map(|record| record.report())
            .collect()
    }

    /// Placement-map edits implied by concluded actions, for the owner of the
    /// topology registry to apply.
    pub fn placement_changes(
        &self,
    ) -> Vec<crate::execution::PlacementChange> {
        self.actions
            .records()
            .filter_map(|record| record.placement_change())
            .collect()
    }

    // ---------------------------------------------------------------- validate

    /// Check a decision against the world without changing anything.
    ///
    /// Public because a dry run is genuinely useful -- an operator, or a CLI,
    /// wants to ask "would you do this, and if not why not" without admitting
    /// anything. [`submit`](Self::submit) calls exactly this.
    pub fn validate(
        &self,
        envelope: &DecisionEnvelope,
        view: &ControlPlaneView<'_>,
    ) -> Result<ValidatedDecision, ValidationError> {
        let decision = &envelope.decision;
        let target = envelope.target;
        let now_ms = view.now_ms;

        if matches!(decision.action, OptimizationAction::NoAction) {
            return Err(ValidationError::NoActionRequested);
        }

        if !target.is_valid() {
            return Err(ValidationError::InvalidCoordinate {
                coordinate: target.coordinate,
            });
        }

        /*
         * The optimizer's own `should_execute` is re-derived here rather than
         * called: the controller's floor is policy, and a future optimizer
         * with a different opinion of its own output must not be able to raise
         * its own execution privileges by changing that method.
         */
        if decision.confidence < self.policy.min_confidence
            || decision.score() <= 0.0
        {
            return Err(ValidationError::BelowExecutionThreshold {
                score: decision.score(),
                confidence: decision.confidence,
                min_confidence: self.policy.min_confidence,
            });
        }

        if envelope.observed_at_ms > now_ms {
            return Err(ValidationError::DecisionFromTheFuture {
                observed_at_ms: envelope.observed_at_ms,
                now_ms,
            });
        }

        let age_ms = now_ms.saturating_sub(envelope.observed_at_ms);

        if age_ms > self.policy.max_decision_age_ms {
            return Err(ValidationError::StaleDecision {
                observed_at_ms: envelope.observed_at_ms,
                now_ms,
                age_ms,
                max_age_ms: self.policy.max_decision_age_ms,
            });
        }

        if envelope.topology_generation != view.fleet.generation() {
            return Err(ValidationError::TopologySuperseded {
                decision_generation: envelope.topology_generation,
                current_generation: view.fleet.generation(),
            });
        }

        let in_flight = self.actions.active_len();

        if in_flight >= self.policy.max_concurrent_actions {
            return Err(ValidationError::TooManyInFlight {
                in_flight,
                limit: self.policy.max_concurrent_actions,
            });
        }

        if let Some(existing) = self.actions.holder_of(target) {
            return Err(ValidationError::ConflictingAction {
                target,
                existing: existing.id(),
                existing_action: action_label(
                    &existing.envelope().decision.action,
                )
                .to_string(),
            });
        }

        if let Some(existing) = self.actions.shard_holder(target.shard_id) {
            return Err(ValidationError::ShardBusy {
                shard_id: target.shard_id,
                existing,
            });
        }

        let source = view
            .topology
            .locate(target.shard_id, target.coordinate)
            .ok_or(ValidationError::UnknownPlacement { target })?
            .clone();

        self.check_node_load(&source.dbms_id)?;

        let destination = self.resolve_destination(envelope, view)?;

        let destination_region = match &destination {
            Some(node) => {
                let status = view.fleet.get(node).ok_or_else(|| {
                    ValidationError::UnknownDestination {
                        node: node.clone(),
                    }
                })?;

                if node.0 == source.dbms_id.0
                    || self.ledger.holds(target, node)
                {
                    return Err(ValidationError::DestinationIsSource {
                        node: node.clone(),
                    });
                }

                if !status.condition.accepts_placement() {
                    return Err(ValidationError::DestinationUnhealthy {
                        node: node.clone(),
                        condition: status.condition,
                    });
                }

                if status.is_full() {
                    return Err(ValidationError::DestinationAtCapacity {
                        node: node.clone(),
                        hosted: status.hosted_placements,
                        capacity: status.placement_capacity,
                    });
                }

                if status.utilization() > self.policy.max_node_utilization {
                    return Err(ValidationError::DestinationSaturated {
                        node: node.clone(),
                        utilization: status.utilization(),
                        limit: self.policy.max_node_utilization,
                    });
                }

                self.check_node_load(node)?;

                Some(status.region.clone())
            }

            None => None,
        };

        Ok(ValidatedDecision::new(
            envelope.clone(),
            source,
            destination,
            destination_region,
            now_ms,
        ))
    }

    fn check_node_load(&self, node: &DbmsId) -> Result<(), ValidationError> {
        let in_flight = self.actions.node_load(node);

        if in_flight >= self.policy.max_actions_per_node {
            return Err(ValidationError::NodeBusy {
                node: node.clone(),
                in_flight,
                limit: self.policy.max_actions_per_node,
            });
        }

        Ok(())
    }

    /// Turn the action's own vocabulary into a concrete node, where it names
    /// one at all.
    fn resolve_destination(
        &self,
        envelope: &DecisionEnvelope,
        view: &ControlPlaneView<'_>,
    ) -> Result<Option<DbmsId>, ValidationError> {
        match &envelope.decision.action {
            OptimizationAction::Replicate { target }
            | OptimizationAction::Move { target } => Ok(Some(target.clone())),

            /*
             * Colocate names a coordinate, not a node: "put this next to
             * that". The node is wherever `that` currently lives, which is a
             * question only the topology can answer.
             */
            OptimizationAction::Colocate { target: coordinate } => {
                let shard_id = envelope.target.shard_id;

                let placement = view
                    .topology
                    .locate(shard_id, *coordinate)
                    .ok_or(ValidationError::ColocationTargetMissing {
                        shard_id,
                        coordinate: *coordinate,
                    })?;

                Ok(Some(placement.dbms_id.clone()))
            }

            /*
             * Split and Isolate say what should change, not where it lands.
             * The mechanism chooses, and whatever it chooses is checked in
             * `validate_plan` under exactly the same rules.
             */
            OptimizationAction::Split | OptimizationAction::Isolate => Ok(None),

            OptimizationAction::NoAction => {
                Err(ValidationError::NoActionRequested)
            }
        }
    }

    /// Check the mechanism's plan, which is where the destinations the
    /// decision did not name finally become visible.
    fn validate_plan(
        &self,
        plan: &PlacementPlan,
        validated: &ValidatedDecision,
        view: &ControlPlaneView<'_>,
    ) -> Result<(), ValidationError> {
        let target = validated.target();

        if plan.target != target {
            return Err(ValidationError::PlanRejected {
                error: PlanError::Infeasible {
                    reason: format!(
                        "plan targets {} but the decision targets {target}",
                        plan.target
                    ),
                },
            });
        }

        if plan.scope == PlanScope::Shard
            && let Some(existing) = self.actions.shard_is_busy(target.shard_id)
        {
            return Err(ValidationError::ShardBusy {
                shard_id: target.shard_id,
                existing,
            });
        }

        for node in &plan.adds {
            let status = view.fleet.get(node).ok_or_else(|| {
                ValidationError::UnknownDestination { node: node.clone() }
            })?;

            if !status.condition.accepts_placement() {
                return Err(ValidationError::DestinationUnhealthy {
                    node: node.clone(),
                    condition: status.condition,
                });
            }

            // A node that already holds a copy is not being asked for new
            // capacity, so headroom is not its business.
            if self.ledger.holds(target, node) {
                continue;
            }

            if status.is_full() {
                return Err(ValidationError::DestinationAtCapacity {
                    node: node.clone(),
                    hosted: status.hosted_placements,
                    capacity: status.placement_capacity,
                });
            }

            if status.utilization() > self.policy.max_node_utilization {
                return Err(ValidationError::DestinationSaturated {
                    node: node.clone(),
                    utilization: status.utilization(),
                    limit: self.policy.max_node_utilization,
                });
            }
        }

        for node in &plan.removes {
            if !self.ledger.holds(target, node) {
                return Err(ValidationError::RemovesUnheldCopy {
                    target,
                    node: node.clone(),
                });
            }
        }

        let remaining = self.ledger.projected(plan).len();

        if remaining < self.policy.min_replicas {
            return Err(ValidationError::WouldDropLastReplica {
                target,
                remaining,
                minimum: self.policy.min_replicas,
            });
        }

        for node in plan.touched_nodes() {
            self.check_node_load(&node)?;
        }

        Ok(())
    }

    // ------------------------------------------------------------------ submit

    /// Validate a decision, have it planned, validate the plan, and admit it.
    ///
    /// `baseline` is the workload profile that justified the decision. It is
    /// captured now rather than looked up later because "before" stops being
    /// available the instant the next observation lands, and without it there
    /// is nothing to measure the result against.
    pub fn submit(
        &mut self,
        envelope: DecisionEnvelope,
        baseline: &WorkloadProfile,
        view: &ControlPlaneView<'_>,
    ) -> Result<ActionId, ValidationError> {
        let validated = self.validate(&envelope, view)?;
        let target = validated.target();

        let index = self
            .executors
            .iter()
            .position(|executor| executor.supports(envelope.action()))
            .ok_or_else(|| ValidationError::NoMechanism {
                action: envelope.action_label().to_string(),
            })?;

        // The recorded placement is a copy; without this the replica floor
        // would read every freshly seen target as holding nothing.
        self.ledger
            .ensure_seeded(target, &validated.source().dbms_id);

        let request = PlanRequest {
            target,
            action: envelope.decision.action.clone(),
            source: validated.source().clone(),
            destination: validated.destination().cloned(),
            replicas: self.ledger.replicas(target),
            requested_at_ms: view.now_ms,
        };

        let plan = self.executors[index]
            .plan(&request)
            .map_err(|error| ValidationError::PlanRejected { error })?;

        self.validate_plan(&plan, &validated, view)?;

        let id = self.actions.next_id();
        let mechanism = self.executors[index].name().to_string();

        let record = ExecutionRecord::new(
            id,
            validated,
            plan,
            mechanism,
            MeasurementSnapshot::from_profile(
                baseline,
                envelope.observed_at_ms,
            ),
            view.now_ms,
        );

        Ok(self.actions.admit(record))
    }

    // ----------------------------------------------------------------- execute

    /// Drive one action by one step.
    ///
    /// Returns the state it is in afterwards, or `None` if the id is unknown.
    /// Faults are absorbed into a rollback rather than returned: a caller that
    /// has to remember to clean up after a failed step is a caller that will
    /// one day forget.
    pub fn advance(
        &mut self,
        id: ActionId,
        view: &ControlPlaneView<'_>,
    ) -> Option<ExecutionState> {
        let now_ms = view.now_ms;
        let state = self.actions.get(id)?.state().clone();

        if state.is_terminal() {
            return Some(state);
        }

        if matches!(state, ExecutionState::AwaitingMeasurement) {
            return Some(self.check_measurement_deadline(id, now_ms));
        }

        if let Some(reason) = self.node_loss(id, view) {
            return Some(self.abort_with(id, reason, now_ms));
        }

        if let ExecutionState::Running { phase, .. } = state {
            let elapsed = self.actions.get(id)?.phase_elapsed_ms(now_ms);

            if elapsed > self.policy.phase_timeout_ms {
                return Some(self.abort_with(
                    id,
                    AbortReason::PhaseTimeout {
                        phase,
                        elapsed_ms: elapsed,
                        limit_ms: self.policy.phase_timeout_ms,
                    },
                    now_ms,
                ));
            }
        }

        let mut fault: Option<ExecutionFault> = None;

        {
            let Self {
                executors,
                actions,
                ledger,
                ..
            } = self;

            let record = actions.get_mut(id)?;

            let Some(index) = executors
                .iter()
                .position(|executor| executor.name() == record.mechanism())
            else {
                fault = Some(ExecutionFault::Internal {
                    reason: format!(
                        "mechanism '{}' is no longer registered",
                        record.mechanism()
                    ),
                });

                return Some(self.abort_with(
                    id,
                    AbortReason::Fault {
                        fault: fault.expect("just set"),
                    },
                    now_ms,
                ));
            };

            let executor = &mut executors[index];

            match record.state().clone() {
                ExecutionState::Admitted => {
                    let phase = record.plan().first_phase();

                    match executor.begin_phase(record.plan(), phase, now_ms) {
                        Ok(()) => record.begin_phase(phase, now_ms),
                        Err(error) => fault = Some(error),
                    }
                }

                ExecutionState::Running { phase, .. } => {
                    let progress =
                        executor.poll_phase(record.plan(), phase, now_ms);

                    match progress {
                        Ok(PhaseProgress::Running { fraction }) => {
                            record.set_fraction(phase, fraction);
                        }

                        Ok(PhaseProgress::Complete) => {
                            record.complete_phase(phase);

                            let next = record.plan().phase_after(phase);

                            match next {
                                Some(next) => {
                                    match executor.begin_phase(
                                        record.plan(),
                                        next,
                                        now_ms,
                                    ) {
                                        Ok(()) => {
                                            record.begin_phase(next, now_ms)
                                        }
                                        Err(error) => fault = Some(error),
                                    }
                                }

                                None => {
                                    // Every phase done: the plan's declared
                                    // effect on the replica set is now fact.
                                    ledger.apply(record.plan());
                                    record.commit(now_ms);
                                }
                            }
                        }

                        Err(error) => fault = Some(error),
                    }
                }

                _ => {}
            }
        }

        if let Some(fault) = fault {
            return Some(self.abort_with(
                id,
                AbortReason::Fault { fault },
                now_ms,
            ));
        }

        Some(self.actions.get(id)?.state().clone())
    }

    /// Drive every in-flight action by one step.
    pub fn tick(
        &mut self,
        view: &ControlPlaneView<'_>,
    ) -> Vec<(ActionId, ExecutionState)> {
        let ids = self.actions.active_ids();
        let mut states = Vec::with_capacity(ids.len());

        for id in ids {
            if let Some(state) = self.advance(id, view) {
                states.push((id, state));
            }
        }

        states
    }

    /// Tear down an in-flight action on request.
    pub fn abort(
        &mut self,
        id: ActionId,
        view: &ControlPlaneView<'_>,
    ) -> Option<ExecutionState> {
        let state = self.actions.get(id)?.state().clone();

        if state.is_terminal() {
            return Some(state);
        }

        Some(self.abort_with(id, AbortReason::Requested, view.now_ms))
    }

    /// The controller's own liveness check on a running plan.
    ///
    /// Deliberately independent of the mechanism: a mechanism that has lost
    /// its destination may be sitting in a retry loop reporting steady
    /// progress. The fleet view is the authority on whether a node is still
    /// usable.
    fn node_loss(
        &self,
        id: ActionId,
        view: &ControlPlaneView<'_>,
    ) -> Option<AbortReason> {
        let record = self.actions.get(id)?;

        if !matches!(record.state(), ExecutionState::Running { .. }) {
            return None;
        }

        for node in record.plan().adds.iter() {
            match view.fleet.get(node) {
                Some(status) if status.condition.accepts_placement() => {}

                Some(status) => {
                    return Some(AbortReason::NodeLost {
                        node: node.clone(),
                        condition: status.condition,
                    });
                }

                None => {
                    return Some(AbortReason::NodeLost {
                        node: node.clone(),
                        condition: crate::fleet::NodeCondition::Unreachable,
                    });
                }
            }
        }

        None
    }

    fn abort_with(
        &mut self,
        id: ActionId,
        reason: AbortReason,
        now_ms: u64,
    ) -> ExecutionState {
        let rollback = {
            let Self {
                executors,
                actions,
                ledger,
                ..
            } = self;

            let Some(record) = actions.get_mut(id) else {
                return ExecutionState::Failed;
            };

            let index = executors
                .iter()
                .position(|executor| executor.name() == record.mechanism());

            let rollback = match index {
                Some(index) => match executors[index].abort(
                    record.plan(),
                    record.completed_phases(),
                    now_ms,
                ) {
                    Ok(outcome) => outcome,

                    Err(error) => RollbackOutcome::Irreversible {
                        reason: error.to_string(),
                    },
                },

                None => RollbackOutcome::Irreversible {
                    reason: "mechanism is no longer registered".to_string(),
                },
            };

            /*
             * A rollback that could not restore the arrangement leaves the
             * plan's effect standing. The ledger has to agree with reality
             * even when reality is the bad outcome, or the next replica-floor
             * check is computed against a fiction.
             */
            if record.has_cut_over()
                && matches!(rollback, RollbackOutcome::Irreversible { .. })
            {
                ledger.apply(record.plan());
            }

            record.conclude_aborted(reason, rollback.clone(), now_ms);
            rollback
        };

        let _ = rollback;
        self.actions.retire(id);

        self.actions
            .get(id)
            .map(|record| record.state().clone())
            .unwrap_or(ExecutionState::Failed)
    }

    // ----------------------------------------------------------------- measure

    /// Record the after-measurement and produce the verdict.
    ///
    /// This is the step README §7 insists on: until it happens the action is
    /// `AwaitingMeasurement`, not `Completed`, and it still holds its target
    /// so that a second action cannot land on top of the measurement.
    pub fn measure(
        &mut self,
        id: ActionId,
        after: &WorkloadProfile,
        view: &ControlPlaneView<'_>,
    ) -> Result<ExecutionOutcome, MeasurementError> {
        let now_ms = view.now_ms;

        let record = self
            .actions
            .get(id)
            .ok_or(MeasurementError::UnknownAction { id })?;

        if !matches!(record.state(), ExecutionState::AwaitingMeasurement) {
            return Err(MeasurementError::NotAwaitingMeasurement {
                id,
                state: record.state().label().to_string(),
            });
        }

        let committed_at_ms = record.committed_at_ms().unwrap_or(now_ms);
        let elapsed_ms = now_ms.saturating_sub(committed_at_ms);

        if elapsed_ms < self.policy.measurement_settle_ms {
            return Err(MeasurementError::TooSoon {
                id,
                elapsed_ms,
                settle_ms: self.policy.measurement_settle_ms,
            });
        }

        let outcome = ExecutionOutcome::evaluate(
            *record.baseline(),
            MeasurementSnapshot::from_profile(after, now_ms),
            &record.envelope().decision,
            self.policy.outcome_noise_floor,
        );

        if let Some(record) = self.actions.get_mut(id) {
            record.conclude_measured(outcome, now_ms);
        }

        self.actions.retire(id);

        Ok(outcome)
    }

    /// Actions that have executed and are waiting for somebody to say whether
    /// they helped.
    pub fn awaiting_measurement(
        &self,
    ) -> impl Iterator<Item = &ExecutionRecord> {
        self.actions.records().filter(|record| {
            matches!(record.state(), ExecutionState::AwaitingMeasurement)
        })
    }

    fn check_measurement_deadline(
        &mut self,
        id: ActionId,
        now_ms: u64,
    ) -> ExecutionState {
        let committed_at_ms = self
            .actions
            .get(id)
            .and_then(|record| record.committed_at_ms())
            .unwrap_or(now_ms);

        if now_ms.saturating_sub(committed_at_ms)
            <= self.policy.measurement_deadline_ms
        {
            return ExecutionState::AwaitingMeasurement;
        }

        if let Some(record) = self.actions.get_mut(id) {
            record.conclude_unmeasured(now_ms);
        }

        self.actions.retire(id);
        ExecutionState::Completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fabric_core::{Coordinate, DbmsId, Shard};
    use fabric_optimizer::OptimizationDecision;
    use fabric_telemetry::WorkloadMetrics;
    use fabric_topology::TopologyRegistry;

    use crate::execution::ExecutionState;
    use crate::fleet::{FleetView, NodeCondition, NodeStatus};
    use crate::target::ActionTarget;
    use crate::plan::{PlacementPlan, PlanPhase, PlanScope};

    /// A mechanism that completes each phase on the tick after it begins.
    /// Enough to exercise the controller's sequencing without pretending to
    /// move any bytes.
    struct StubExecutor {
        started: std::collections::BTreeMap<PlanPhase, u64>,
        removes: Vec<DbmsId>,
    }

    impl StubExecutor {
        fn new() -> Self {
            Self {
                started: std::collections::BTreeMap::new(),
                removes: Vec::new(),
            }
        }

        fn dropping(removes: Vec<DbmsId>) -> Self {
            Self {
                started: std::collections::BTreeMap::new(),
                removes,
            }
        }
    }

    impl PlacementExecutor for StubExecutor {
        fn name(&self) -> &str {
            "stub"
        }

        fn supports(&self, action: &OptimizationAction) -> bool {
            !matches!(action, OptimizationAction::NoAction)
        }

        fn plan(
            &self,
            request: &PlanRequest,
        ) -> Result<PlacementPlan, PlanError> {
            let destination = request.destination.clone().ok_or(
                PlanError::Infeasible {
                    reason: "stub needs a named destination".to_string(),
                },
            )?;

            let removes = if self.removes.is_empty() {
                vec![request.source.dbms_id.clone()]
            } else {
                self.removes.clone()
            };

            PlacementPlan::new(
                request.target,
                request.action.clone(),
                PlanScope::Coordinate,
                vec![
                    PlanPhase::Prepare,
                    PlanPhase::Transfer,
                    PlanPhase::Cutover,
                    PlanPhase::Cleanup,
                ],
                vec![destination],
                removes,
            )
        }

        fn begin_phase(
            &mut self,
            _plan: &PlacementPlan,
            phase: PlanPhase,
            at_ms: u64,
        ) -> Result<(), ExecutionFault> {
            self.started.insert(phase, at_ms);
            Ok(())
        }

        fn poll_phase(
            &mut self,
            _plan: &PlacementPlan,
            phase: PlanPhase,
            at_ms: u64,
        ) -> Result<PhaseProgress, ExecutionFault> {
            let started = self
                .started
                .get(&phase)
                .copied()
                .ok_or(ExecutionFault::NotStarted { phase })?;

            if at_ms > started {
                Ok(PhaseProgress::Complete)
            } else {
                Ok(PhaseProgress::Running { fraction: 0.5 })
            }
        }

        fn abort(
            &mut self,
            _plan: &PlacementPlan,
            completed: &[PlanPhase],
            _at_ms: u64,
        ) -> Result<RollbackOutcome, ExecutionFault> {
            if completed.contains(&PlanPhase::Cutover) {
                Ok(RollbackOutcome::Irreversible {
                    reason: "authority already moved".to_string(),
                })
            } else {
                Ok(RollbackOutcome::Restored)
            }
        }
    }

    fn world() -> (TopologyRegistry, FleetView) {
        let mut topology = TopologyRegistry::new();
        let shard = Shard::new(7, "social");

        topology.place(
            DbmsId::new("db-a"),
            &shard,
            Coordinate::new(3, 4),
            "us-east",
        );

        let mut fleet = FleetView::new(1);

        fleet.insert(
            NodeStatus::new(
                DbmsId::new("db-a"),
                "us-east",
                NodeCondition::Healthy,
                10,
            )
            .with_load(4, 0.5, 0.4),
        );

        fleet.insert(
            NodeStatus::new(
                DbmsId::new("db-b"),
                "us-west",
                NodeCondition::Healthy,
                10,
            )
            .with_load(1, 0.2, 0.2),
        );

        (topology, fleet)
    }

    fn profile(pressure_cpu: f64) -> WorkloadProfile {
        WorkloadProfile::from_metrics(
            7,
            Coordinate::new(3, 4),
            WorkloadMetrics {
                operations_per_second: 1_000.0,
                reads_per_second: 900.0,
                writes_per_second: 100.0,
                read_latency_us: 1_000.0,
                write_latency_us: 1_000.0,
                cpu_utilization: pressure_cpu,
                memory_utilization: pressure_cpu,
                storage_bytes_per_second: 0,
                network_in_bytes_per_second: 0,
                network_out_bytes_per_second: 0,
                queue_depth: 0,
                cell_breakdown: Vec::new(),
                cell_breakdown_partial: false,
            },
        )
    }

    fn envelope(action: OptimizationAction, observed_at_ms: u64) -> DecisionEnvelope {
        DecisionEnvelope::new(
            OptimizationDecision {
                shard_id: 7,
                coordinate: Coordinate::new(3, 4),
                action,
                expected_gain: 1.0,
                estimated_cost: 0.3,
                confidence: 0.9,
            },
            observed_at_ms,
            1,
        )
    }

    #[test]
    fn a_stale_decision_is_refused_with_its_evidence() {
        let (topology, fleet) = world();
        let controller = FabricController::default();
        let view = ControlPlaneView::new(&topology, &fleet, 100_000);

        let error = controller
            .validate(
                &envelope(
                    OptimizationAction::Move {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &view,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ValidationError::StaleDecision { age_ms: 99_000, .. }
        ));
        assert!(error.to_string().contains("stale"));
    }

    #[test]
    fn a_decision_computed_against_an_older_fleet_is_refused() {
        let (topology, fleet) = world();
        let controller = FabricController::default();
        let view = ControlPlaneView::new(&topology, &fleet, 1_000);

        let mut stale = envelope(
            OptimizationAction::Move {
                target: DbmsId::new("db-b"),
            },
            1_000,
        );
        stale.topology_generation = 0;

        assert!(matches!(
            controller.validate(&stale, &view).unwrap_err(),
            ValidationError::TopologySuperseded {
                decision_generation: 0,
                current_generation: 1,
            }
        ));
    }

    #[test]
    fn an_unhealthy_destination_is_refused() {
        let (topology, mut fleet) = world();

        fleet.insert(
            NodeStatus::new(
                DbmsId::new("db-b"),
                "us-west",
                NodeCondition::Degraded,
                10,
            )
            .with_load(1, 0.2, 0.2),
        );

        let controller = FabricController::default();
        let view = ControlPlaneView::new(&topology, &fleet, 1_000);

        assert!(matches!(
            controller
                .validate(
                    &envelope(
                        OptimizationAction::Move {
                            target: DbmsId::new("db-b"),
                        },
                        1_000,
                    ),
                    &view,
                )
                .unwrap_err(),
            ValidationError::DestinationUnhealthy { .. }
        ));
    }

    #[test]
    fn a_saturated_destination_is_refused_even_with_headroom() {
        let (topology, mut fleet) = world();

        fleet.insert(
            NodeStatus::new(
                DbmsId::new("db-b"),
                "us-west",
                NodeCondition::Healthy,
                10,
            )
            .with_load(1, 0.95, 0.3),
        );

        let controller = FabricController::default();
        let view = ControlPlaneView::new(&topology, &fleet, 1_000);

        assert!(matches!(
            controller
                .validate(
                    &envelope(
                        OptimizationAction::Move {
                            target: DbmsId::new("db-b"),
                        },
                        1_000,
                    ),
                    &view,
                )
                .unwrap_err(),
            ValidationError::DestinationSaturated { .. }
        ));
    }

    #[test]
    fn two_actions_cannot_run_on_one_target() {
        let (topology, fleet) = world();
        let mut controller = FabricController::default();
        controller.register_executor(Box::new(StubExecutor::new()));
        controller.seed_replicas(&topology);

        let view = ControlPlaneView::new(&topology, &fleet, 1_000);
        let baseline = profile(0.9);

        controller
            .submit(
                envelope(
                    OptimizationAction::Move {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &baseline,
                &view,
            )
            .expect("first action is admitted");

        let error = controller
            .submit(
                envelope(
                    OptimizationAction::Replicate {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &baseline,
                &view,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ValidationError::ConflictingAction { .. }
        ));
    }

    #[test]
    fn a_plan_that_would_drop_the_last_copy_is_refused() {
        let (topology, fleet) = world();
        let mut controller = FabricController::default();

        // This mechanism drops the source *and* the destination it just made,
        // which leaves the target held by nobody.
        controller.register_executor(Box::new(StubExecutor::dropping(vec![
            DbmsId::new("db-a"),
            DbmsId::new("db-b"),
        ])));
        controller.seed_replicas(&topology);

        let view = ControlPlaneView::new(&topology, &fleet, 1_000);

        let error = controller
            .submit(
                envelope(
                    OptimizationAction::Move {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &profile(0.9),
                &view,
            )
            .unwrap_err();

        // db-b holds no copy yet, so the plan is caught even before the
        // replica floor: it claims to remove something that is not there.
        assert!(matches!(
            error,
            ValidationError::RemovesUnheldCopy { .. }
        ));
    }

    #[test]
    fn an_executed_action_is_not_complete_until_it_is_measured() {
        let (topology, fleet) = world();
        let mut controller = FabricController::default();
        controller.register_executor(Box::new(StubExecutor::new()));
        controller.seed_replicas(&topology);

        let mut now = 1_000;
        let view = ControlPlaneView::new(&topology, &fleet, now);

        let id = controller
            .submit(
                envelope(
                    OptimizationAction::Move {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &profile(0.95),
                &view,
            )
            .expect("admitted");

        // Drive to the end of the plan.
        for _ in 0..16 {
            now += 1_000;
            let view = ControlPlaneView::new(&topology, &fleet, now);
            controller.advance(id, &view);
        }

        let record = controller.record(id).expect("record");
        assert_eq!(record.state(), &ExecutionState::AwaitingMeasurement);
        assert_eq!(record.completed_phases().len(), 4);
        assert!(record.placement_change().is_some());

        // The ledger followed the plan: db-b holds it, db-a does not.
        let target = ActionTarget::new(7, Coordinate::new(3, 4));
        assert_eq!(controller.ledger().count(target), 1);
        assert!(controller.ledger().holds(target, &DbmsId::new("db-b")));

        // Measuring too early is refused rather than producing a verdict from
        // mid-rebalance noise.
        let early = ControlPlaneView::new(&topology, &fleet, now);
        assert!(matches!(
            controller.measure(id, &profile(0.2), &early).unwrap_err(),
            MeasurementError::TooSoon { .. }
        ));

        now += 60_000;
        let settled = ControlPlaneView::new(&topology, &fleet, now);
        let outcome = controller
            .measure(id, &profile(0.2), &settled)
            .expect("measured");

        assert!(outcome.realized_gain > 0.0);
        assert_eq!(outcome.verdict, crate::outcome::OutcomeVerdict::Improved);

        let reports = controller.reports();
        assert_eq!(reports.len(), 1);
        assert!(reports[0].is_success());
    }

    #[test]
    fn an_action_that_helped_nothing_is_reported_as_such() {
        let (topology, fleet) = world();
        let mut controller = FabricController::default();
        controller.register_executor(Box::new(StubExecutor::new()));
        controller.seed_replicas(&topology);

        let mut now = 1_000;
        let view = ControlPlaneView::new(&topology, &fleet, now);

        let id = controller
            .submit(
                envelope(
                    OptimizationAction::Move {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &profile(0.9),
                &view,
            )
            .expect("admitted");

        for _ in 0..16 {
            now += 1_000;
            let view = ControlPlaneView::new(&topology, &fleet, now);
            controller.advance(id, &view);
        }

        now += 60_000;
        let settled = ControlPlaneView::new(&topology, &fleet, now);

        // Same pressure afterwards: executed, but it bought nothing.
        let outcome = controller
            .measure(id, &profile(0.9), &settled)
            .expect("measured");

        assert_eq!(outcome.verdict, crate::outcome::OutcomeVerdict::Unchanged);
        assert!(!controller.reports()[0].is_success());
    }

    #[test]
    fn losing_the_destination_mid_flight_tears_the_action_down() {
        let (topology, fleet) = world();
        let mut controller = FabricController::default();
        controller.register_executor(Box::new(StubExecutor::new()));
        controller.seed_replicas(&topology);

        let view = ControlPlaneView::new(&topology, &fleet, 1_000);

        let id = controller
            .submit(
                envelope(
                    OptimizationAction::Move {
                        target: DbmsId::new("db-b"),
                    },
                    1_000,
                ),
                &profile(0.9),
                &view,
            )
            .expect("admitted");

        // Begin the first phase, then lose the destination.
        let view = ControlPlaneView::new(&topology, &fleet, 2_000);
        controller.advance(id, &view);

        let mut broken = fleet.clone();
        broken.insert(
            NodeStatus::new(
                DbmsId::new("db-b"),
                "us-west",
                NodeCondition::Unreachable,
                10,
            )
            .with_load(1, 0.2, 0.2),
        );

        let view = ControlPlaneView::new(&topology, &broken, 3_000);
        let state = controller.advance(id, &view).expect("known action");

        assert_eq!(state, ExecutionState::RolledBack);

        let record = controller.record(id).expect("record");
        assert!(record.placement_change().is_none());
        assert!(matches!(
            record.abort_reason(),
            Some(AbortReason::NodeLost { .. })
        ));

        // The interlock is released, so the target can be tried again.
        assert_eq!(controller.actions().active_len(), 0);
    }
}
