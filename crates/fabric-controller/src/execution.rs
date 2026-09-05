//! The execution record, and the registry that stops two actions from fighting
//! over the same data.
//!
//! An in-flight registry is not bookkeeping. Two concurrent plans on one
//! target -- say a replicate and a move -- interleave their cutovers, and the
//! loser's cleanup deletes the winner's data. The registry is the interlock
//! that makes that unrepresentable, and it is the reason a record exists
//! before the first phase begins rather than after the last one ends.

use std::collections::{BTreeMap, BTreeSet};

use fabric_core::DbmsId;
use fabric_topology::TopologyRegistry;
use serde::{Deserialize, Serialize};

use crate::decision::{DecisionEnvelope, ValidatedDecision};
use crate::outcome::{ExecutionOutcome, MeasurementSnapshot, OutcomeReport, OutcomeVerdict};
use crate::plan::{PlacementPlan, PlanPhase, PlanScope, RollbackOutcome, action_label};
use crate::target::{ActionId, ActionTarget};
use crate::validation::AbortReason;

/// Where an action is in its life.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionState {
    /// Validated and admitted; no phase has begun.
    Admitted,

    /// A phase is in progress. `fraction` is the mechanism's advisory
    /// progress report and is never used to decide that a phase is done.
    Running { phase: PlanPhase, fraction: f64 },

    /// Every phase completed. The action is not finished: until it has been
    /// measured, nobody knows whether it was worth doing.
    AwaitingMeasurement,

    /// Executed and judged.
    Completed,

    /// Torn down; the arrangement was restored.
    RolledBack,

    /// Ended badly -- a fault whose rollback could not restore the original
    /// arrangement, or an abort after the point of no return.
    Failed,
}

impl ExecutionState {
    /// Whether the action still holds its target against other actions.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Admitted | Self::Running { .. } | Self::AwaitingMeasurement
        )
    }

    pub fn is_terminal(&self) -> bool {
        !self.is_active()
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Running { .. } => "running",
            Self::AwaitingMeasurement => "awaiting-measurement",
            Self::Completed => "completed",
            Self::RolledBack => "rolled-back",
            Self::Failed => "failed",
        }
    }
}

/// The placement-map edit an executed action implies.
///
/// The controller does not mutate `TopologyRegistry`: it is handed the map
/// immutably, and the owner of that map (the runtime) applies the change. This
/// keeps the controller a decision-and-state crate and keeps one writer on the
/// fleet's map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementChange {
    pub target: ActionTarget,
    pub from: DbmsId,
    pub to: DbmsId,
    pub to_region: String,
}

impl PlacementChange {
    /// Apply to a registry the caller owns. `false` means the registry had no
    /// placement for the target, i.e. the map moved on without us.
    pub fn apply(&self, topology: &mut TopologyRegistry) -> bool {
        topology.move_coordinate(
            self.target.shard_id,
            self.target.coordinate,
            self.to.clone(),
            self.to_region.clone(),
        )
    }
}

/// Everything known about one admitted action, from validation to verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    id: ActionId,
    envelope: DecisionEnvelope,
    plan: PlacementPlan,
    mechanism: String,

    source: DbmsId,
    destination: Option<DbmsId>,
    destination_region: Option<String>,

    admitted_at_ms: u64,
    phase_started_at_ms: u64,
    committed_at_ms: Option<u64>,
    concluded_at_ms: Option<u64>,

    completed_phases: Vec<PlanPhase>,
    state: ExecutionState,

    baseline: MeasurementSnapshot,
    outcome: Option<ExecutionOutcome>,
    verdict: Option<OutcomeVerdict>,

    abort: Option<AbortReason>,
    rollback: Option<RollbackOutcome>,
}

impl ExecutionRecord {
    pub(crate) fn new(
        id: ActionId,
        validated: ValidatedDecision,
        plan: PlacementPlan,
        mechanism: String,
        baseline: MeasurementSnapshot,
        admitted_at_ms: u64,
    ) -> Self {
        let source = validated.source().dbms_id.clone();
        let destination = validated.destination().cloned();

        let destination_region = validated
            .destination_region()
            .map(|region| region.to_string());

        Self {
            id,
            envelope: validated.envelope().clone(),
            plan,
            mechanism,
            source,
            destination,
            destination_region,
            admitted_at_ms,
            phase_started_at_ms: admitted_at_ms,
            committed_at_ms: None,
            concluded_at_ms: None,
            completed_phases: Vec::new(),
            state: ExecutionState::Admitted,
            baseline,
            outcome: None,
            verdict: None,
            abort: None,
            rollback: None,
        }
    }

    pub fn id(&self) -> ActionId {
        self.id
    }

    pub fn target(&self) -> ActionTarget {
        self.envelope.target
    }

    pub fn envelope(&self) -> &DecisionEnvelope {
        &self.envelope
    }

    pub fn plan(&self) -> &PlacementPlan {
        &self.plan
    }

    pub fn mechanism(&self) -> &str {
        &self.mechanism
    }

    pub fn state(&self) -> &ExecutionState {
        &self.state
    }

    pub fn source(&self) -> &DbmsId {
        &self.source
    }

    pub fn destination(&self) -> Option<&DbmsId> {
        self.destination.as_ref()
    }

    pub fn admitted_at_ms(&self) -> u64 {
        self.admitted_at_ms
    }

    pub fn committed_at_ms(&self) -> Option<u64> {
        self.committed_at_ms
    }

    pub fn completed_phases(&self) -> &[PlanPhase] {
        &self.completed_phases
    }

    pub fn baseline(&self) -> &MeasurementSnapshot {
        &self.baseline
    }

    pub fn outcome(&self) -> Option<&ExecutionOutcome> {
        self.outcome.as_ref()
    }

    pub fn verdict(&self) -> Option<OutcomeVerdict> {
        self.verdict
    }

    pub fn abort_reason(&self) -> Option<&AbortReason> {
        self.abort.as_ref()
    }

    pub fn rollback(&self) -> Option<&RollbackOutcome> {
        self.rollback.as_ref()
    }

    /// Whether authority for the target has already moved. Past this point an
    /// abort cannot restore the original arrangement by discarding work.
    pub fn has_cut_over(&self) -> bool {
        self.completed_phases.contains(&PlanPhase::Cutover)
    }

    /// The placement-map edit this action implies, if it moved authority to a
    /// new node and actually got there.
    pub fn placement_change(&self) -> Option<PlacementChange> {
        if !self.has_cut_over() {
            return None;
        }

        let to = self.destination.as_ref()?;
        let to_region = self.destination_region.as_ref()?;

        Some(PlacementChange {
            target: self.target(),
            from: self.source.clone(),
            to: to.clone(),
            to_region: to_region.clone(),
        })
    }

    /// The feedback row for this action.
    pub fn report(&self) -> OutcomeReport {
        let decision = &self.envelope.decision;

        let failure = self
            .abort
            .as_ref()
            .map(|reason| reason.to_string())
            .or_else(|| match &self.rollback {
                Some(RollbackOutcome::Irreversible { reason }) => {
                    Some(reason.clone())
                }
                _ => None,
            });

        OutcomeReport {
            action_id: self.id,
            target: self.target(),
            action: decision.action.clone(),
            action_label: action_label(&decision.action).to_string(),
            mechanism: self.mechanism.clone(),
            confidence: decision.confidence,
            expected_gain: decision.expected_gain,
            estimated_cost: decision.estimated_cost,
            realized_gain: self
                .outcome
                .map_or(0.0, |outcome| outcome.realized_gain),
            gain_ratio: self
                .outcome
                .map_or(0.0, |outcome| outcome.gain_ratio),
            verdict: self.verdict.unwrap_or(OutcomeVerdict::NotMeasured),
            admitted_at_ms: self.admitted_at_ms,
            concluded_at_ms: self
                .concluded_at_ms
                .unwrap_or(self.admitted_at_ms),
            measurement: self.outcome,
            failure,
        }
    }

    pub(crate) fn begin_phase(&mut self, phase: PlanPhase, at_ms: u64) {
        self.state = ExecutionState::Running {
            phase,
            fraction: 0.0,
        };
        self.phase_started_at_ms = at_ms;
    }

    pub(crate) fn set_fraction(&mut self, phase: PlanPhase, fraction: f64) {
        self.state = ExecutionState::Running {
            phase,
            fraction: fraction.clamp(0.0, 1.0),
        };
    }

    pub(crate) fn complete_phase(&mut self, phase: PlanPhase) {
        if !self.completed_phases.contains(&phase) {
            self.completed_phases.push(phase);
        }
    }

    pub(crate) fn phase_elapsed_ms(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.phase_started_at_ms)
    }

    pub(crate) fn commit(&mut self, at_ms: u64) {
        self.committed_at_ms = Some(at_ms);
        self.state = ExecutionState::AwaitingMeasurement;
    }

    pub(crate) fn conclude_measured(
        &mut self,
        outcome: ExecutionOutcome,
        at_ms: u64,
    ) {
        self.verdict = Some(outcome.verdict);
        self.outcome = Some(outcome);
        self.state = ExecutionState::Completed;
        self.concluded_at_ms = Some(at_ms);
    }

    pub(crate) fn conclude_unmeasured(&mut self, at_ms: u64) {
        self.verdict = Some(OutcomeVerdict::NotMeasured);
        self.state = ExecutionState::Completed;
        self.concluded_at_ms = Some(at_ms);
    }

    pub(crate) fn conclude_aborted(
        &mut self,
        reason: AbortReason,
        rollback: RollbackOutcome,
        at_ms: u64,
    ) {
        let restored = matches!(rollback, RollbackOutcome::Restored);

        self.abort = Some(reason);
        self.rollback = Some(rollback);
        self.concluded_at_ms = Some(at_ms);

        if restored {
            self.state = ExecutionState::RolledBack;
            self.verdict = Some(OutcomeVerdict::RolledBack);
        } else {
            self.state = ExecutionState::Failed;
            self.verdict = Some(OutcomeVerdict::Failed);
        }
    }
}

/// The in-flight interlock plus the history behind it.
///
/// Indexes are maintained eagerly rather than derived by scanning, because
/// they are consulted on the validation path of every decision and a control
/// plane that gets slower as it gets busier is a control plane that stops
/// controlling exactly when it is needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionRegistry {
    records: BTreeMap<u64, ExecutionRecord>,

    active_targets: BTreeMap<ActionTarget, u64>,
    active_shards: BTreeMap<u64, BTreeSet<u64>>,
    active_nodes: BTreeMap<String, BTreeSet<u64>>,
    shard_wide: BTreeMap<u64, u64>,

    next_id: u64,
}

impl ActionRegistry {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            active_targets: BTreeMap::new(),
            active_shards: BTreeMap::new(),
            active_nodes: BTreeMap::new(),
            shard_wide: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub(crate) fn next_id(&self) -> ActionId {
        ActionId(self.next_id)
    }

    /// File an admitted action and take its interlocks.
    pub(crate) fn admit(&mut self, record: ExecutionRecord) -> ActionId {
        let id = record.id();
        self.next_id = self.next_id.max(id.0 + 1);

        let target = record.target();
        self.active_targets.insert(target, id.0);

        self.active_shards
            .entry(target.shard_id)
            .or_default()
            .insert(id.0);

        if record.plan().scope == PlanScope::Shard {
            self.shard_wide.insert(target.shard_id, id.0);
        }

        for node in record.plan().touched_nodes() {
            self.active_nodes
                .entry(node.0)
                .or_default()
                .insert(id.0);
        }

        self.active_nodes
            .entry(record.source().0.clone())
            .or_default()
            .insert(id.0);

        self.records.insert(id.0, record);
        id
    }

    /// Release the interlocks of an action that has reached a terminal state.
    /// The record itself is kept: history is the input to the learning loop.
    pub(crate) fn retire(&mut self, id: ActionId) {
        let Some(record) = self.records.get(&id.0) else {
            return;
        };

        let target = record.target();

        if self.active_targets.get(&target) == Some(&id.0) {
            self.active_targets.remove(&target);
        }

        if let Some(set) = self.active_shards.get_mut(&target.shard_id) {
            set.remove(&id.0);

            if set.is_empty() {
                self.active_shards.remove(&target.shard_id);
            }
        }

        if self.shard_wide.get(&target.shard_id) == Some(&id.0) {
            self.shard_wide.remove(&target.shard_id);
        }

        let mut empty: Vec<String> = Vec::new();

        for (node, set) in self.active_nodes.iter_mut() {
            set.remove(&id.0);

            if set.is_empty() {
                empty.push(node.clone());
            }
        }

        for node in empty {
            self.active_nodes.remove(&node);
        }
    }

    pub fn get(&self, id: ActionId) -> Option<&ExecutionRecord> {
        self.records.get(&id.0)
    }

    pub(crate) fn get_mut(
        &mut self,
        id: ActionId,
    ) -> Option<&mut ExecutionRecord> {
        self.records.get_mut(&id.0)
    }

    /// Every record ever admitted, oldest first.
    pub fn records(&self) -> impl Iterator<Item = &ExecutionRecord> {
        self.records.values()
    }

    /// Records still holding interlocks, oldest first.
    pub fn active(&self) -> impl Iterator<Item = &ExecutionRecord> {
        self.records
            .values()
            .filter(|record| record.state().is_active())
    }

    pub fn active_ids(&self) -> Vec<ActionId> {
        self.active().map(|record| record.id()).collect()
    }

    pub fn active_len(&self) -> usize {
        self.active_targets.len()
    }

    /// The action holding `target`, if any.
    pub fn holder_of(&self, target: ActionTarget) -> Option<&ExecutionRecord> {
        self.active_targets
            .get(&target)
            .and_then(|id| self.records.get(id))
    }

    /// The shard-wide action holding `shard_id`, if any.
    pub fn shard_holder(&self, shard_id: u64) -> Option<ActionId> {
        self.shard_wide.get(&shard_id).copied().map(ActionId)
    }

    /// Whether any action at all is running against `shard_id`.
    pub fn shard_is_busy(&self, shard_id: u64) -> Option<ActionId> {
        self.active_shards
            .get(&shard_id)
            .and_then(|set| set.iter().next().copied())
            .map(ActionId)
    }

    pub fn node_load(&self, node: &DbmsId) -> usize {
        self.active_nodes
            .get(&node.0)
            .map_or(0, |set| set.len())
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
