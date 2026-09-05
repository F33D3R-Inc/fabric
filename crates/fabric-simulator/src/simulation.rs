//! The simulation loop.
//!
//! One [`step`](Simulation::step) advances the clock by exactly one tick,
//! applies whatever faults were scheduled for that instant, recomputes what
//! every node is serving, and emits the observations a real fleet would have
//! reported. Nothing in that sequence reads a wall clock or draws from an
//! unseeded source, which is what makes a run reproducible.
//!
//! The output is deliberately in `fabric-telemetry` and `fabric-protocol`
//! types rather than simulator ones, so that
//! `fabric_runtime::FabricRuntime::handle`, `WorkloadAnalyzer::observe` and
//! `WorkloadOptimizer::optimize` can be driven against a synthetic cluster
//! without being told.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use fabric_controller::{ActionTarget, FleetView};
use fabric_core::DbmsId;
use fabric_protocol::{
    FabricMessage, NodeHeartbeat, NodeRegistration, TelemetryBatch, TelemetrySample,
};
use fabric_telemetry::{Observation, WorkloadMetrics};
use fabric_topology::TopologyRegistry;

use crate::clock::SimClock;
use crate::cluster::{ClusterSpec, ClusterState};
use crate::executor::SimExecutor;
use crate::fault::Fault;
use crate::rng::Rng;
use crate::workload::diurnal;

/// Everything one step produced.
#[derive(Debug, Clone)]
pub struct Tick {
    pub at_ms: u64,

    /// Faults that fired at this instant.
    pub faults: Vec<Fault>,

    /// What the Fabric would have observed. Cells on an unreachable node
    /// produce nothing -- silence is the observation.
    pub observations: Vec<Observation>,

    /// The same information as it would have arrived over `fabric-protocol`.
    pub messages: Vec<FabricMessage>,
}

/// Per-cell demand for one tick, before it becomes an observation.
#[derive(Debug, Clone, Copy)]
struct CellLoad {
    target: ActionTarget,
    reads_on_owner: f64,
    writes: f64,
}

/// A synthetic FacetQL fleet under a synthetic workload.
pub struct Simulation {
    spec: ClusterSpec,
    state: Rc<RefCell<ClusterState>>,
    clock: SimClock,

    /// The workload's own random stream, forked from the seed so that adding a
    /// draw elsewhere cannot shift it.
    rng: Rng,

    schedule: BTreeMap<u64, Vec<Fault>>,
    registered: bool,
}

impl Simulation {
    pub fn new(spec: ClusterSpec) -> Self {
        let state = ClusterState::build(&spec);

        let mut seed_stream = Rng::new(spec.seed);
        let _construction = seed_stream.fork();
        let rng = seed_stream.fork();

        let clock = SimClock::new(spec.start_ms, spec.tick_ms);

        Self {
            spec,
            state: Rc::new(RefCell::new(state)),
            clock,
            rng,
            schedule: BTreeMap::new(),
            registered: false,
        }
    }

    pub fn spec(&self) -> &ClusterSpec {
        &self.spec
    }

    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    pub fn clock(&self) -> SimClock {
        self.clock
    }

    /// The shared cluster. Borrowing it is how a caller inspects the world;
    /// the [`SimExecutor`] mutates the same one.
    pub fn state(&self) -> &Rc<RefCell<ClusterState>> {
        &self.state
    }

    /// A mechanism the controller can be given.
    pub fn executor(&self) -> SimExecutor {
        SimExecutor::new(Rc::clone(&self.state))
    }

    pub fn topology(&self) -> TopologyRegistry {
        self.state.borrow().topology()
    }

    pub fn fleet_view(&self) -> FleetView {
        self.state.borrow().fleet_view()
    }

    pub fn generation(&self) -> u64 {
        self.state.borrow().generation()
    }

    /// Every placed cell, in target order.
    pub fn targets(&self) -> Vec<ActionTarget> {
        self.state
            .borrow()
            .cells()
            .map(|cell| cell.target)
            .collect()
    }

    /// Schedule a fault for the first tick at or after `at_ms`.
    pub fn schedule_fault(&mut self, at_ms: u64, fault: Fault) {
        self.schedule.entry(at_ms).or_default().push(fault);
    }

    /// Apply a fault immediately.
    pub fn inject(&mut self, fault: Fault) {
        let now_ms = self.now_ms();
        self.apply_fault(&fault, now_ms);
    }

    /// Advance one tick.
    pub fn step(&mut self) -> Tick {
        let at_ms = self.clock.advance();

        let faults = self.due_faults(at_ms);

        for fault in &faults {
            self.apply_fault(fault, at_ms);
        }

        self.expire_hotspots(at_ms);

        let loads = self.recompute_load(at_ms);
        let observations = self.build_observations(at_ms, &loads);
        let messages = self.build_messages(at_ms, &observations);

        Tick {
            at_ms,
            faults,
            observations,
            messages,
        }
    }

    /// Advance `ticks` steps.
    pub fn run(&mut self, ticks: usize) -> Vec<Tick> {
        (0..ticks).map(|_| self.step()).collect()
    }

    // ------------------------------------------------------------------ faults

    fn due_faults(&mut self, at_ms: u64) -> Vec<Fault> {
        let due: Vec<u64> = self
            .schedule
            .range(..=at_ms)
            .map(|(at, _)| *at)
            .collect();

        let mut faults = Vec::new();

        for at in due {
            if let Some(scheduled) = self.schedule.remove(&at) {
                faults.extend(scheduled);
            }
        }

        faults
    }

    fn apply_fault(&mut self, fault: &Fault, at_ms: u64) {
        let mut state = self.state.borrow_mut();

        match fault {
            Fault::NodeLoss { node } => {
                if let Some(node) = state.node_mut(&node.0) {
                    node.online = false;
                    node.served_ops = 0.0;
                    node.cpu_utilization = 0.0;
                    node.memory_utilization = 0.0;
                    node.queue_depth = 0;
                }
            }

            Fault::NodeRecovery { node } => {
                if let Some(node) = state.node_mut(&node.0) {
                    node.online = true;
                }
            }

            Fault::PartitionRegion { region } => {
                for node in state.nodes_mut() {
                    if &node.region == region {
                        node.reachable = false;
                    }
                }
            }

            Fault::HealRegion { region } => {
                for node in state.nodes_mut() {
                    if &node.region == region {
                        node.reachable = true;
                    }
                }
            }

            Fault::HotShard {
                target,
                multiplier,
                duration_ms,
            } => {
                if let Some(cell) = state.cell_mut(*target) {
                    cell.hot_multiplier = *multiplier;
                    cell.hot_until_ms = at_ms.saturating_add(*duration_ms);
                }
            }
        }
    }

    fn expire_hotspots(&mut self, at_ms: u64) {
        let expired: Vec<ActionTarget> = self
            .state
            .borrow()
            .cells()
            .filter(|cell| {
                cell.hot_multiplier != 1.0 && cell.hot_until_ms <= at_ms
            })
            .map(|cell| cell.target)
            .collect();

        let mut state = self.state.borrow_mut();

        for target in expired {
            if let Some(cell) = state.cell_mut(target) {
                cell.hot_multiplier = 1.0;
                cell.hot_until_ms = 0;
            }
        }
    }

    // ------------------------------------------------------------------- load

    /// Recompute what every node is serving this tick.
    ///
    /// Reads are shared across a cell's holders and writes go to all of them.
    /// That asymmetry is the reason replicating a read-heavy hotspot helps and
    /// replicating a write-heavy one does not -- if the simulator flattened it,
    /// every optimizer policy would score the same here and the simulation
    /// would be worthless as an evaluation.
    fn recompute_load(&mut self, at_ms: u64) -> Vec<CellLoad> {
        let mut state = self.state.borrow_mut();

        for node in state.nodes_mut() {
            node.served_ops = 0.0;
        }

        let targets: Vec<ActionTarget> =
            state.cells().map(|cell| cell.target).collect();

        let mut loads = Vec::with_capacity(targets.len());
        let mut demand: BTreeMap<String, f64> = BTreeMap::new();

        for target in targets {
            let Some(cell) = state.cell(target) else {
                continue;
            };

            let profile = cell.profile();

            let hot = if at_ms < cell.hot_until_ms {
                cell.hot_multiplier
            } else {
                1.0
            };

            let wave = diurnal(
                at_ms,
                self.spec.diurnal_period_ms,
                cell.phase_offset_ms,
            );

            let jitter = self.rng.range(0.95, 1.05);

            let ops = profile.base_ops * cell.scale * hot * wave * jitter;
            let reads = ops * profile.read_ratio;
            let writes = ops - reads;

            let online_holders: Vec<String> = cell
                .holders()
                .into_iter()
                .filter(|id| {
                    state.node(id).is_some_and(|node| node.online)
                })
                .collect();

            let share = if online_holders.is_empty() {
                0.0
            } else {
                reads / online_holders.len() as f64
            };

            for holder in &online_holders {
                *demand.entry(holder.clone()).or_insert(0.0) +=
                    share + writes;
            }

            /*
             * A staged copy is being written to but is serving nothing. This
             * is the cost of a migration in flight, and making it visible is
             * why the controller's before/after measurement can catch a plan
             * that helped the source by wrecking the destination.
             */
            for staged in &cell.staging {
                if state.node(staged).is_some_and(|node| node.online) {
                    *demand.entry(staged.clone()).or_insert(0.0) += writes;
                }
            }

            loads.push(CellLoad {
                target,
                reads_on_owner: share,
                writes,
            });
        }

        for (id, served) in demand {
            if let Some(node) = state.node_mut(&id) {
                node.served_ops = served;
            }
        }

        let hosted: BTreeMap<String, usize> = state
            .nodes()
            .map(|node| {
                (node.id.0.clone(), state.hosted_placements(&node.id.0))
            })
            .collect();

        for node in state.nodes_mut() {
            if !node.online {
                node.served_ops = 0.0;
                node.cpu_utilization = 0.0;
                node.memory_utilization = 0.0;
                node.queue_depth = 0;
                continue;
            }

            let cpu =
                (node.served_ops / node.capacity_ops.max(1.0)).clamp(0.0, 1.0);

            let placements = *hosted.get(&node.id.0).unwrap_or(&0) as f64;
            let capacity = node.placement_capacity.max(1) as f64;

            node.cpu_utilization = cpu;

            node.memory_utilization =
                (0.10 + (placements / capacity) * 0.35 + cpu * 0.45)
                    .clamp(0.0, 1.0);

            node.queue_depth = if cpu > 0.75 {
                (((cpu - 0.75) / 0.25) * 25_000.0) as u64
            } else {
                (cpu * 400.0) as u64
            };
        }

        loads
    }

    fn build_observations(
        &self,
        at_ms: u64,
        loads: &[CellLoad],
    ) -> Vec<Observation> {
        let state = self.state.borrow();
        let mut observations = Vec::with_capacity(loads.len());

        for load in loads {
            let Some(cell) = state.cell(load.target) else {
                continue;
            };

            let Some(owner) = state.node(&cell.owner) else {
                continue;
            };

            // An unreachable node reports nothing. The control plane's job is
            // to cope with the gap, not to be handed a value it never received.
            if !owner.online || !owner.reachable {
                continue;
            }

            let Some(shard) = state.shard(load.target.shard_id) else {
                continue;
            };

            let profile = cell.profile();

            let stress =
                1.0 + 8.0 * (owner.cpu_utilization - 0.60).max(0.0);

            let bytes_per_op = profile.bytes_per_op as f64;

            let metrics = WorkloadMetrics {
                operations_per_second: load.reads_on_owner + load.writes,
                reads_per_second: load.reads_on_owner,
                writes_per_second: load.writes,
                read_latency_us: profile.read_latency_us * stress,
                write_latency_us: profile.write_latency_us * stress,
                cpu_utilization: owner.cpu_utilization,
                memory_utilization: owner.memory_utilization,
                storage_bytes_per_second: (bytes_per_op * load.writes) as u64,
                network_in_bytes_per_second: (bytes_per_op * load.writes)
                    as u64,
                network_out_bytes_per_second: (bytes_per_op
                    * load.reads_on_owner)
                    as u64,
                queue_depth: owner.queue_depth,
                // The simulator has no per-coordinate FacetQL attribution to
                // echo; a simulated cell is exactly the whole story.
                cell_breakdown: Vec::new(),
                cell_breakdown_partial: false,
            };

            observations.push(Observation::new(
                at_ms,
                shard.clone(),
                load.target.coordinate,
                metrics,
            ));
        }

        observations
    }

    /// The same tick, rendered as `fabric-protocol` traffic.
    fn build_messages(
        &mut self,
        at_ms: u64,
        observations: &[Observation],
    ) -> Vec<FabricMessage> {
        let mut messages = Vec::new();

        {
            let state = self.state.borrow();

            if !self.registered {
                for node in state.nodes() {
                    messages.push(FabricMessage::RegisterNode(
                        NodeRegistration::new(
                            node.id.clone(),
                            "0.13.0",
                            node.region.clone(),
                        ),
                    ));
                }
            }

            for node in state.nodes() {
                if !node.online || !node.reachable {
                    continue;
                }

                messages.push(FabricMessage::Heartbeat(NodeHeartbeat {
                    node_id: node.id.clone(),
                    timestamp_ms: at_ms,
                    healthy: node.condition()
                        == fabric_controller::NodeCondition::Healthy,
                }));
            }

            // Batches are keyed by (node, shard) because a `TelemetryBatch`
            // carries exactly one shard, and ordered so that the message
            // stream is reproducible.
            let mut batches: BTreeMap<(String, u64), Vec<TelemetrySample>> =
                BTreeMap::new();

            for observation in observations {
                let target = ActionTarget::new(
                    observation.shard.id,
                    observation.coordinate,
                );

                let Some(cell) = state.cell(target) else {
                    continue;
                };

                let metrics = &observation.metrics;
                let total = metrics.total_operations();

                let (read_ratio, write_ratio) = if total > 0.0 {
                    (
                        metrics.reads_per_second / total,
                        metrics.writes_per_second / total,
                    )
                } else {
                    (0.0, 0.0)
                };

                batches
                    .entry((cell.owner.clone(), observation.shard.id))
                    .or_default()
                    .push(TelemetrySample {
                        coordinate: observation.coordinate,
                        operations_per_second: total,
                        read_ratio,
                        write_ratio,
                        read_latency_us: metrics.read_latency_us,
                        write_latency_us: metrics.write_latency_us,
                        cpu_utilization: metrics.cpu_utilization,
                        memory_utilization: metrics.memory_utilization,
                        queue_depth: metrics.queue_depth,
                        cell_breakdown: metrics.cell_breakdown.clone(),
                        cell_breakdown_partial: metrics.cell_breakdown_partial,
                    });
            }

            for ((node, shard_id), samples) in batches {
                let Some(shard) = state.shard(shard_id) else {
                    continue;
                };

                messages.push(FabricMessage::Telemetry(TelemetryBatch {
                    timestamp_ms: at_ms,
                    node_id: DbmsId::new(node),
                    shard: shard.clone(),
                    samples,
                }));
            }
        }

        self.registered = true;
        messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fabric_controller::{
        ControlPlaneView, DecisionEnvelope, ExecutionState, FabricController, NodeCondition,
        OptimizationAction, OptimizationDecision, OutcomeVerdict,
    };
    use fabric_core::WorkloadClass;
    use fabric_workload::{WorkloadAnalyzer, WorkloadProfile};

    fn spec() -> ClusterSpec {
        ClusterSpec {
            seed: 20260904,
            cells_per_shard: 4,
            node_capacity_ops: 20_000.0,
            classes: vec![WorkloadClass::ReadHeavy, WorkloadClass::Mixed],
            ..ClusterSpec::default()
        }
    }

    /// A digest of everything a run produced, in order.
    fn digest(ticks: &[Tick]) -> String {
        let mut out = String::new();

        for tick in ticks {
            out.push_str(&format!("{}|", tick.at_ms));

            for observation in &tick.observations {
                out.push_str(&format!(
                    "{}:{}:{}:{:e}:{:e}:{:e};",
                    observation.shard.id,
                    observation.coordinate.x,
                    observation.coordinate.y,
                    observation.metrics.reads_per_second,
                    observation.metrics.writes_per_second,
                    observation.metrics.read_latency_us,
                ));
            }

            out.push('\n');
        }

        out
    }

    #[test]
    fn the_same_seed_reproduces_the_run_exactly() {
        let build = || {
            let mut simulation = Simulation::new(spec());
            let target = simulation.targets()[1];

            simulation.schedule_fault(
                simulation.spec().start_ms + 5_000,
                Fault::HotShard {
                    target,
                    multiplier: 6.0,
                    duration_ms: 20_000,
                },
            );

            simulation.schedule_fault(
                simulation.spec().start_ms + 12_000,
                Fault::NodeLoss {
                    node: fabric_core::DbmsId::new("us-west-db-0"),
                },
            );

            simulation.run(40)
        };

        assert_eq!(digest(&build()), digest(&build()));
    }

    #[test]
    fn a_different_seed_produces_a_different_run() {
        let mut a = Simulation::new(spec());
        let mut b = Simulation::new(spec().with_seed(7));

        assert_ne!(digest(&a.run(20)), digest(&b.run(20)));
    }

    #[test]
    fn a_hot_shard_shows_up_as_pressure_on_its_cell() {
        let mut simulation = Simulation::new(spec());
        let target = simulation.targets()[0];

        let before = simulation.step();

        let quiet = before
            .observations
            .iter()
            .find(|observation| {
                observation.shard.id == target.shard_id
                    && observation.coordinate == target.coordinate
            })
            .map(|observation| observation.metrics.total_operations())
            .expect("the cell reports while its node is healthy");

        simulation.inject(Fault::HotShard {
            target,
            multiplier: 8.0,
            duration_ms: 30_000,
        });

        let after = simulation.step();

        let loud = after
            .observations
            .iter()
            .find(|observation| {
                observation.shard.id == target.shard_id
                    && observation.coordinate == target.coordinate
            })
            .map(|observation| observation.metrics.total_operations())
            .expect("still reporting");

        assert!(
            loud > quiet * 4.0,
            "hot shard produced {loud} ops against a baseline of {quiet}"
        );
    }

    #[test]
    fn a_lost_node_goes_silent_rather_than_reporting_zeroes() {
        let mut simulation = Simulation::new(spec());
        let lost = fabric_core::DbmsId::new("us-east-db-0");

        let before = simulation.step();
        let owned = simulation
            .state()
            .borrow()
            .cells()
            .filter(|cell| cell.owner == lost.0)
            .count();

        assert!(owned > 0);

        simulation.inject(Fault::NodeLoss { node: lost.clone() });
        let after = simulation.step();

        assert_eq!(
            after.observations.len(),
            before.observations.len() - owned
        );

        assert_eq!(
            simulation
                .fleet_view()
                .get(&lost)
                .expect("still in the fleet")
                .condition,
            NodeCondition::Unreachable
        );
    }

    #[test]
    fn a_partitioned_region_keeps_serving_but_stops_reporting() {
        let mut simulation = Simulation::new(spec());

        let before = simulation.step();

        simulation.inject(Fault::PartitionRegion {
            region: "us-west".to_string(),
        });

        let after = simulation.step();

        assert!(after.observations.len() < before.observations.len());

        // The data is still there: nothing was unplaced by the partition.
        assert_eq!(
            simulation.state().borrow().cells().count(),
            before.observations.len()
        );

        simulation.inject(Fault::HealRegion {
            region: "us-west".to_string(),
        });

        let healed = simulation.step();
        assert_eq!(healed.observations.len(), before.observations.len());
    }

    #[test]
    fn the_optimizer_runs_against_the_simulator_unchanged() {
        use fabric_optimizer::WorkloadOptimizer;

        let mut simulation = Simulation::new(spec());
        let target = simulation.targets()[0];

        simulation.inject(Fault::HotShard {
            target,
            multiplier: 40.0,
            duration_ms: 60_000,
        });

        let mut analyzer = WorkloadAnalyzer::new();

        for tick in simulation.run(10) {
            for observation in &tick.observations {
                analyzer.observe(observation);
            }
        }

        let topology = simulation.topology();
        let optimizer = WorkloadOptimizer::default();

        let hot = analyzer
            .profiles()
            .find(|profile| profile.coordinate == target.coordinate)
            .expect("the hot cell was profiled");

        // The whole point: nothing in the analyzer or optimizer knows it is
        // looking at a simulation.
        let decision = optimizer.optimize(hot, &topology);
        assert_eq!(decision.coordinate, target.coordinate);
    }

    /// The full loop: observe, decide, validate, execute through the trait,
    /// and measure the result.
    #[test]
    fn the_controller_executes_and_measures_against_the_simulation() {
        let mut simulation = Simulation::new(spec());
        let target = simulation.targets()[0];

        simulation.inject(Fault::HotShard {
            target,
            multiplier: 30.0,
            duration_ms: 400_000,
        });

        let mut baseline: Option<WorkloadProfile> = None;

        for tick in simulation.run(5) {
            for observation in &tick.observations {
                if observation.shard.id == target.shard_id
                    && observation.coordinate == target.coordinate
                {
                    baseline = Some(WorkloadProfile::from_metrics(
                        observation.shard.id,
                        observation.coordinate,
                        observation.metrics.clone(),
                    ));
                }
            }
        }

        let baseline = baseline.expect("the hot cell reported");

        let owner = simulation
            .state()
            .borrow()
            .cell(target)
            .expect("cell")
            .owner
            .clone();

        let destination = simulation
            .state()
            .borrow()
            .least_loaded_node(&[owner.clone()])
            .expect("somewhere to move to");

        let mut controller = FabricController::default();
        controller.register_executor(Box::new(simulation.executor()));

        let topology = simulation.topology();
        controller.seed_replicas(&topology);
        let fleet = simulation.fleet_view();

        let envelope = DecisionEnvelope::new(
            OptimizationDecision {
                shard_id: target.shard_id,
                coordinate: target.coordinate,
                action: OptimizationAction::Move {
                    target: destination.clone(),
                },
                expected_gain: 0.30,
                estimated_cost: 0.05,
                confidence: 0.95,
            },
            simulation.now_ms(),
            fleet.generation(),
        );

        let id = {
            let view = ControlPlaneView::new(
                &topology,
                &fleet,
                simulation.now_ms(),
            );

            controller
                .submit(envelope, &baseline, &view)
                .expect("a fresh, confident, well-targeted decision is admitted")
        };

        // Drive both clocks together until the plan finishes.
        for _ in 0..20 {
            simulation.step();

            let topology = simulation.topology();
            let fleet = simulation.fleet_view();

            let view = ControlPlaneView::new(
                &topology,
                &fleet,
                simulation.now_ms(),
            );

            controller.advance(id, &view);
        }

        let record = controller.record(id).expect("record");

        assert_eq!(record.state(), &ExecutionState::AwaitingMeasurement);

        // The simulated world actually moved, through the trait boundary.
        assert_eq!(
            simulation.state().borrow().cell(target).unwrap().owner,
            destination.0
        );

        assert!(record.placement_change().is_some());

        // Let the hotspot pass and the system settle, then judge the decision.
        simulation.inject(Fault::HotShard {
            target,
            multiplier: 1.0,
            duration_ms: 0,
        });

        for _ in 0..40 {
            simulation.step();
        }

        let mut after: Option<WorkloadProfile> = None;

        for tick in simulation.run(2) {
            for observation in &tick.observations {
                if observation.shard.id == target.shard_id
                    && observation.coordinate == target.coordinate
                {
                    after = Some(WorkloadProfile::from_metrics(
                        observation.shard.id,
                        observation.coordinate,
                        observation.metrics.clone(),
                    ));
                }
            }
        }

        let after = after.expect("the cell reports from its new home");
        let topology = simulation.topology();
        let fleet = simulation.fleet_view();

        let view =
            ControlPlaneView::new(&topology, &fleet, simulation.now_ms());

        let outcome = controller
            .measure(id, &after, &view)
            .expect("settled long enough to be measured");

        assert!(outcome.verdict.is_measured());
        assert_eq!(outcome.verdict, OutcomeVerdict::Improved);

        let reports = controller.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].mechanism, "simulated-placement");
    }

    #[test]
    fn a_split_is_declined_rather_than_faked() {
        let mut simulation = Simulation::new(spec());
        let target = simulation.targets()[0];

        simulation.step();

        let mut controller = FabricController::default();
        controller.register_executor(Box::new(simulation.executor()));

        let topology = simulation.topology();
        controller.seed_replicas(&topology);
        let fleet = simulation.fleet_view();

        let view =
            ControlPlaneView::new(&topology, &fleet, simulation.now_ms());

        let envelope = DecisionEnvelope::new(
            OptimizationDecision {
                shard_id: target.shard_id,
                coordinate: target.coordinate,
                action: OptimizationAction::Split,
                expected_gain: 1.0,
                estimated_cost: 0.1,
                confidence: 0.99,
            },
            simulation.now_ms(),
            fleet.generation(),
        );

        let profile = WorkloadProfile::from_metrics(
            target.shard_id,
            target.coordinate,
            fabric_telemetry::WorkloadMetrics::default(),
        );

        let error = controller
            .submit(envelope, &profile, &view)
            .unwrap_err();

        assert!(
            error.to_string().contains("no registered mechanism"),
            "unexpected rejection: {error}"
        );
    }
}
