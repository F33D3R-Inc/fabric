//! A [`PlacementExecutor`] backed by the simulated cluster.
//!
//! This is the crate's second purpose. The controller defines execution as a
//! trait precisely so that the mechanism can be swapped, and a simulator that
//! could not stand in for `fabric-routing` / `fabric-replication` /
//! `fabric-migration` would not actually be exercising the control loop -- it
//! would be exercising a mock of it. The same `FabricController`, unmodified,
//! drives this and will drive the real mechanisms.
//!
//! What it deliberately does *not* implement is
//! [`OptimizationAction::Split`]: nothing here models splitting a shard's grid,
//! so it says so through `supports` and lets the controller refuse the action
//! with `NoMechanism`, rather than accepting it and pretending.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use fabric_controller::{
    ActionTarget, ExecutionFault, OptimizationAction, PhaseProgress, PlacementExecutor,
    PlacementPlan, PlanError, PlanPhase, PlanRequest, PlanScope, RollbackOutcome,
};

use crate::cluster::ClusterState;

/// How long the non-transfer phases take, in simulated milliseconds.
const PREPARE_MS: u64 = 2_000;
const CUTOVER_MS: u64 = 0;
const CLEANUP_MS: u64 = 1_000;

/// One in-flight placement change, as the mechanism sees it.
#[derive(Debug, Clone, PartialEq)]
struct SimJob {
    destination: Option<String>,
    phase: Option<PlanPhase>,
    phase_started_at_ms: u64,
    cut_over: bool,
}

impl SimJob {
    fn new() -> Self {
        Self {
            destination: None,
            phase: None,
            phase_started_at_ms: 0,
            cut_over: false,
        }
    }
}

/// Executes placement changes against a [`ClusterState`].
///
/// Shares the cluster with the [`Simulation`](crate::Simulation) through
/// `Rc<RefCell<_>>`: the simulator is single-threaded pure logic, and the
/// alternative -- handing the executor a copy -- would mean control actions
/// had no effect on the world the observations come from, which is the one
/// thing this has to get right.
pub struct SimExecutor {
    state: Rc<RefCell<ClusterState>>,
    jobs: BTreeMap<ActionTarget, SimJob>,
}

impl SimExecutor {
    pub fn new(state: Rc<RefCell<ClusterState>>) -> Self {
        Self {
            state,
            jobs: BTreeMap::new(),
        }
    }

    /// How long `phase` takes for `plan`.
    fn phase_duration_ms(&self, plan: &PlacementPlan, phase: PlanPhase) -> u64 {
        match phase {
            PlanPhase::Prepare => PREPARE_MS,

            PlanPhase::Transfer => {
                let rate = self.state.borrow().transfer_bytes_per_ms();
                (plan.estimated_bytes / rate.max(1)).max(1)
            }

            PlanPhase::Cutover => CUTOVER_MS,
            PlanPhase::Cleanup => CLEANUP_MS,
        }
    }

    /// A destination that has stopped being usable is reported as a fault
    /// rather than waited on: a mechanism that keeps reporting progress
    /// against a dead node is how a control plane hangs.
    fn check_destination(
        &self,
        destination: Option<&String>,
    ) -> Result<(), ExecutionFault> {
        let Some(destination) = destination else {
            return Ok(());
        };

        let state = self.state.borrow();

        match state.node(destination) {
            Some(node) if node.online => Ok(()),

            _ => Err(ExecutionFault::DestinationLost {
                node: fabric_core::DbmsId::new(destination.clone()),
            }),
        }
    }

    fn apply_completion(&mut self, plan: &PlacementPlan, phase: PlanPhase) {
        let Some(job) = self.jobs.get_mut(&plan.target) else {
            return;
        };

        let destination = job.destination.clone();
        let mut state = self.state.borrow_mut();

        match phase {
            PlanPhase::Prepare | PlanPhase::Transfer => {}

            PlanPhase::Cutover => {
                if let Some(destination) = destination {
                    let owner_is_leaving = state
                        .cell(plan.target)
                        .map(|cell| {
                            plan.removes
                                .iter()
                                .any(|node| node.0 == cell.owner)
                        })
                        .unwrap_or(false);

                    if owner_is_leaving {
                        state.set_owner(plan.target, &destination);
                    } else {
                        state.promote(plan.target, &destination);
                    }
                }

                job.cut_over = true;
            }

            PlanPhase::Cleanup => {
                for node in &plan.removes {
                    state.drop_copy(plan.target, &node.0);
                }
            }
        }
    }
}

impl PlacementExecutor for SimExecutor {
    fn name(&self) -> &str {
        "simulated-placement"
    }

    fn supports(&self, action: &OptimizationAction) -> bool {
        matches!(
            action,
            OptimizationAction::Replicate { .. }
                | OptimizationAction::Move { .. }
                | OptimizationAction::Isolate
                | OptimizationAction::Colocate { .. }
        )
    }

    fn plan(&self, request: &PlanRequest) -> Result<PlacementPlan, PlanError> {
        let state = self.state.borrow();

        let cell = state.cell(request.target).ok_or_else(|| {
            PlanError::Infeasible {
                reason: format!("{} does not exist", request.target),
            }
        })?;

        let destination = match &request.action {
            OptimizationAction::Replicate { .. }
            | OptimizationAction::Move { .. }
            | OptimizationAction::Colocate { .. } => request
                .destination
                .clone()
                .ok_or_else(|| PlanError::Infeasible {
                    reason: "action names no destination".to_string(),
                })?,

            /*
             * Isolate is the action that leaves the choice to the mechanism:
             * put this workload somewhere it is not competing with anything.
             * Every holder is excluded, so "isolate" cannot resolve to a node
             * that already has a copy.
             */
            OptimizationAction::Isolate => {
                let mut exclude = cell.holders();
                exclude.extend(cell.staging.iter().cloned());

                state.least_loaded_node(&exclude).ok_or_else(|| {
                    PlanError::Infeasible {
                        reason: "no healthy node with headroom to isolate onto"
                            .to_string(),
                    }
                })?
            }

            other => {
                return Err(PlanError::UnsupportedAction {
                    action: fabric_controller::action_label(other).to_string(),
                });
            }
        };

        let replicating =
            matches!(request.action, OptimizationAction::Replicate { .. });

        let (phases, removes) = if replicating {
            (
                vec![PlanPhase::Prepare, PlanPhase::Transfer, PlanPhase::Cutover],
                Vec::new(),
            )
        } else {
            (
                vec![
                    PlanPhase::Prepare,
                    PlanPhase::Transfer,
                    PlanPhase::Cutover,
                    PlanPhase::Cleanup,
                ],
                vec![request.source.dbms_id.clone()],
            )
        };

        let bytes = cell.bytes;

        let transfer_ms =
            (bytes / state.transfer_bytes_per_ms().max(1)).max(1);

        let duration_ms = PREPARE_MS + transfer_ms + CUTOVER_MS
            + if replicating { 0 } else { CLEANUP_MS };

        Ok(PlacementPlan::new(
            request.target,
            request.action.clone(),
            PlanScope::Coordinate,
            phases,
            vec![destination],
            removes,
        )?
        .with_estimate(bytes, duration_ms))
    }

    fn begin_phase(
        &mut self,
        plan: &PlacementPlan,
        phase: PlanPhase,
        at_ms: u64,
    ) -> Result<(), ExecutionFault> {
        let destination = plan.adds.first().map(|node| node.0.clone());

        self.check_destination(destination.as_ref())?;

        let job = self
            .jobs
            .entry(plan.target)
            .or_insert_with(SimJob::new);

        job.destination = destination.clone();
        job.phase = Some(phase);
        job.phase_started_at_ms = at_ms;

        if phase == PlanPhase::Prepare
            && let Some(destination) = destination
        {
            // Staged data occupies the destination before it serves anything.
            self.state
                .borrow_mut()
                .stage(plan.target, &destination);
        }

        Ok(())
    }

    fn poll_phase(
        &mut self,
        plan: &PlacementPlan,
        phase: PlanPhase,
        at_ms: u64,
    ) -> Result<PhaseProgress, ExecutionFault> {
        let (started_at_ms, destination) = {
            let job = self
                .jobs
                .get(&plan.target)
                .ok_or(ExecutionFault::NotStarted { phase })?;

            if job.phase != Some(phase) {
                return Err(ExecutionFault::NotStarted { phase });
            }

            (job.phase_started_at_ms, job.destination.clone())
        };

        self.check_destination(destination.as_ref())?;

        let duration_ms = self.phase_duration_ms(plan, phase);
        let elapsed_ms = at_ms.saturating_sub(started_at_ms);

        if elapsed_ms < duration_ms {
            return Ok(PhaseProgress::Running {
                fraction: elapsed_ms as f64 / duration_ms as f64,
            });
        }

        self.apply_completion(plan, phase);

        Ok(PhaseProgress::Complete)
    }

    fn abort(
        &mut self,
        plan: &PlacementPlan,
        completed: &[PlanPhase],
        _at_ms: u64,
    ) -> Result<RollbackOutcome, ExecutionFault> {
        let job = self.jobs.remove(&plan.target);

        let cut_over = job.as_ref().is_some_and(|job| job.cut_over)
            || completed.contains(&PlanPhase::Cutover);

        if let Some(destination) = job.and_then(|job| job.destination) {
            if cut_over {
                /*
                 * Authority already moved. The staged copy is now real data
                 * and discarding it would be data loss, so the honest answer
                 * is that this cannot be undone by rollback -- it needs a new,
                 * validated decision in the opposite direction.
                 */
                return Ok(RollbackOutcome::Irreversible {
                    reason: format!(
                        "authority for {} already moved to '{destination}'",
                        plan.target
                    ),
                });
            }

            self.state
                .borrow_mut()
                .unstage(plan.target, &destination);
        } else if cut_over {
            return Ok(RollbackOutcome::Irreversible {
                reason: format!("{} already cut over", plan.target),
            });
        }

        Ok(RollbackOutcome::Restored)
    }
}
