//! # fabric-runtime
//!
//! The assembler. Everything else in the workspace is a layer that knows one
//! thing well and depends on as little as it can; this crate is where those
//! layers are joined into a control loop that actually runs:
//!
//! ```text
//! protocol -> telemetry -> workload -> ml -> optimizer
//!                                              |
//!                                          decision
//!                                              v
//!                       measure  <-  controller  ->  PlacementMechanism
//!                                                      |    |    |
//!                                            routing <-+    |    +-> migration
//!                                                     replication
//! ```
//!
//! The controller defines execution as a trait so that it never has to know
//! which mechanism is running; [`PlacementMechanism`] is the implementation of
//! that trait on top of the real `fabric-routing`, `fabric-replication` and
//! `fabric-migration` crates, and [`PlacementFabric`] is the state they share.
//!
//! ## Why the adapter lives here and not in a crate of its own
//!
//! The three mechanism crates were deliberately built as pure logic with no
//! dependency on the controller that drives them, and that direction must not
//! be reversed -- a mechanism that depends on its driver cannot be tested,
//! replaced or reasoned about on its own. Something has to depend on both, and
//! that something is the assembler. Putting the adapter in its own crate would
//! have produced a crate with exactly one consumer (this one) that would then
//! have needed this crate's [`NodeRegistry`] and [`FabricState`] to build the
//! placement candidates and node conditions the mechanisms are fed -- so it
//! would either duplicate them or depend back on `fabric-runtime`. The
//! dependency it saves is not worth either.

pub mod mechanism;
pub mod placement;
pub mod registry;
pub mod state;

pub use mechanism::{
    PlacementMechanism,
    MECHANISM_NAME,
};

// Re-exported so a caller feeding the placement fabric does not need a direct
// dependency on `fabric-migration` to name the sequence space it feeds.
pub use fabric_migration::{MigrationPhase, WriteSeq};

pub use placement::{
    CopyVerdict,
    FabricError,
    PlacementFabric,
    TransferProgress,
};

pub use registry::{
    HeartbeatError,
    NodeHealth,
    NodeRegistry,
    RegisteredNode,
    DEFAULT_HEARTBEAT_DEADLINE_MS,
};

pub use state::{
    FabricState,
    LocationKey,
};

use std::cell::RefCell;
use std::rc::Rc;

use fabric_controller::{
    ActionId,
    ControllerPolicy,
    ControlPlaneView,
    DecisionEnvelope,
    ExecutionOutcome,
    ExecutionState,
    FabricController,
    FleetView,
    MeasurementError,
    NodeCondition,
    NodeStatus,
    ValidationError,
};

use fabric_core::{Coordinate, Shard};

use fabric_optimizer::{
    Fleet,
    FleetNode,
    OptimizationDecision,
    WorkloadOptimizer,
};

use fabric_protocol::{
    FabricMessage,
    FabricResponse,
};

use fabric_telemetry::{
    Observation,
    WorkloadMetrics,
};

use fabric_topology::TopologyRegistry;

use fabric_workload::{
    WorkloadAnalyzer,
    WorkloadProfile,
};

/// Placements a node is assumed to be provisioned for.
///
/// No protocol message reports a node's placement capacity, so this is an
/// operator input with a documented default rather than a measurement --
/// see [`FabricRuntime::with_placement_capacity`]. It is deliberately not
/// inferred from how much a node currently holds: "as full as it happens to
/// be" would make every node permanently at capacity.
pub const DEFAULT_PLACEMENT_CAPACITY: usize = 64;

/// How many recently ingested observations [`FabricRuntime::observations`]
/// keeps.
///
/// The window exists to be looked at -- "what has just arrived" -- and nothing
/// downstream reads it: the analyzer holds one profile per cell and
/// [`FabricState`] holds the latest observation per location, both bounded by
/// the fleet's size. An unbounded buffer of every sample ever ingested is a
/// leak in any process that runs longer than a replayed session, and a daemon
/// polling a fleet every few seconds is exactly that process.
pub const RECENT_OBSERVATIONS: usize = 1_024;

pub struct FabricRuntime {
    topology: TopologyRegistry,
    analyzer: WorkloadAnalyzer,
    optimizer: WorkloadOptimizer,
    /// The most recent observations, newest last. Bounded: see
    /// [`RECENT_OBSERVATIONS`].
    observations: Vec<Observation>,
    state: FabricState,
    nodes: NodeRegistry,
    clock_ms: u64,

    /// The state the real mechanisms share. Held behind `Rc<RefCell<_>>`
    /// because the controller owns the executor that mutates it and the
    /// runtime has to be able to read the routing table beside it.
    placement: Rc<RefCell<PlacementFabric>>,

    controller: FabricController,

    /// The mechanism's placement generation as of the last reconciliation.
    placement_generation: u64,

    placement_capacity: usize,
    heartbeat_deadline_ms: u64,
}

impl Default for FabricRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl FabricRuntime {
    pub fn new() -> Self {
        Self::with_policy(ControllerPolicy::default())
    }

    /// Build a runtime whose controller enforces an operator-configured
    /// policy.
    ///
    /// The thresholds in [`ControllerPolicy`] -- how stale a decision may be,
    /// how long a phase may hang, how long the system must settle before an
    /// after-measurement means anything -- are deployment properties, not
    /// library constants. A daemon reads them from its configuration file, so
    /// there has to be a way in other than the default.
    pub fn with_policy(policy: ControllerPolicy) -> Self {
        let placement = Rc::new(RefCell::new(PlacementFabric::new()));

        let placement_generation = placement.borrow().generation();

        let mut controller = FabricController::new(policy);

        controller.register_executor(Box::new(PlacementMechanism::new(
            Rc::clone(&placement),
        )));

        Self {
            topology: TopologyRegistry::default(),
            analyzer: WorkloadAnalyzer::default(),
            optimizer: WorkloadOptimizer::default(),
            observations: Vec::new(),
            state: FabricState::new(),
            nodes: NodeRegistry::new(),
            clock_ms: 0,
            placement_generation,
            placement,
            controller,
            placement_capacity: DEFAULT_PLACEMENT_CAPACITY,
            heartbeat_deadline_ms: DEFAULT_HEARTBEAT_DEADLINE_MS,
        }
    }

    /// Set how many placements a node is provisioned for.
    pub fn with_placement_capacity(mut self, capacity: usize) -> Self {
        self.placement_capacity = capacity.max(1);
        self
    }

    /// Set the silence budget used for liveness.
    pub fn with_heartbeat_deadline_ms(mut self, deadline_ms: u64) -> Self {
        self.heartbeat_deadline_ms = deadline_ms;
        self
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub fn analyzer(&self) -> &WorkloadAnalyzer {
        &self.analyzer
    }

    pub fn optimizer(&self) -> &WorkloadOptimizer {
        &self.optimizer
    }

    /// The most recent observations the runtime ingested, newest last.
    ///
    /// A *window*, not a history: at most [`RECENT_OBSERVATIONS`] of them. The
    /// authoritative per-location state is [`Self::state`], which keeps the
    /// latest observation of every cell and is bounded by the fleet's size;
    /// this is the arrival-ordered tail, for looking at what just came in.
    ///
    /// It used to be unbounded. Nothing read it and every ingested sample was
    /// pushed onto it forever, which a replayed session never noticed and a
    /// daemon polling a fleet for a month certainly would.
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }

    /// Push an observation into the bounded window.
    ///
    /// Half the window is dropped when it fills, rather than one entry per
    /// push: shifting the whole buffer on every sample would make ingestion
    /// cost grow with the window's size for no benefit.
    fn remember(&mut self, observation: Observation) {
        if self.observations.len() >= RECENT_OBSERVATIONS {
            self.observations.drain(..RECENT_OBSERVATIONS / 2);
        }

        self.observations.push(observation);
    }

    pub fn state(&self) -> &FabricState {
        &self.state
    }

    /// The fleet inventory built from `RegisterNode` / `Heartbeat`.
    pub fn nodes(&self) -> &NodeRegistry {
        &self.nodes
    }

    /// The routing / replication / migration state the mechanisms share.
    ///
    /// Handed out as the shared cell rather than a borrow because the
    /// controller's executor holds the same cell: a caller that wants to
    /// resolve a route, report copy progress or route a write during a move
    /// borrows it for exactly as long as that takes.
    pub fn placement(&self) -> &Rc<RefCell<PlacementFabric>> {
        &self.placement
    }

    /// The execution arm, with this runtime's mechanism already registered.
    pub fn controller(&self) -> &FabricController {
        &self.controller
    }

    /// The fleet as the controller is allowed to see it.
    ///
    /// Condition comes from the heartbeat registry, load from the newest
    /// telemetry each node reported, and the generation from the placement
    /// map -- not from the routing table, whose generation also moves on every
    /// health report and would supersede every decision in flight.
    pub fn fleet_view(&self) -> FleetView {
        let mut fleet = FleetView::new(self.placement.borrow().generation());

        for node in self.nodes.nodes() {
            let condition =
                match node.health(self.clock_ms, self.heartbeat_deadline_ms) {
                    NodeHealth::Healthy => NodeCondition::Healthy,
                    NodeHealth::Degraded => NodeCondition::Degraded,
                    NodeHealth::Unreachable => NodeCondition::Unreachable,
                };

            let hosted = self
                .topology
                .placements()
                .filter(|placement| placement.dbms_id == node.id)
                .count();

            let mut cpu = 0.0_f64;
            let mut memory = 0.0_f64;

            for observation in self.state.for_node(&node.id) {
                cpu = cpu.max(observation.metrics.cpu_utilization);
                memory = memory.max(observation.metrics.memory_utilization);
            }

            fleet.insert(
                NodeStatus::new(
                    node.id.clone(),
                    node.region.clone(),
                    condition,
                    self.placement_capacity,
                )
                .with_load(hosted, cpu, memory),
            );
        }

        fleet
    }

    /// Adopt a placement map wholesale.
    ///
    /// The protocol's `TopologyReport` cannot express one: it carries no shard
    /// id, so `ingest_topology` has to derive one from the coordinate. A
    /// bootstrap source that knows the real shard ids -- `fabric-facetql`'s
    /// durable placement store, an operator, a simulation -- hands the map
    /// over here instead.
    pub fn adopt_topology(&mut self, topology: TopologyRegistry) {
        self.topology = topology;
        self.sync_placement();
    }

    /// Submit a decision for validation and, if it survives, execution.
    pub fn submit(
        &mut self,
        envelope: DecisionEnvelope,
        baseline: &WorkloadProfile,
    ) -> Result<ActionId, ValidationError> {
        let fleet = self.fleet_view();

        let Self {
            topology,
            controller,
            clock_ms,
            ..
        } = self;

        let view = ControlPlaneView::new(topology, &fleet, *clock_ms);

        controller.submit(envelope, baseline, &view)
    }

    /// Drive every in-flight action by one step, then fold the placement edits
    /// the concluded ones imply back into the map.
    pub fn advance_actions(&mut self) -> Vec<(ActionId, ExecutionState)> {
        let fleet = self.fleet_view();

        let states = {
            let Self {
                topology,
                controller,
                clock_ms,
                ..
            } = self;

            let view = ControlPlaneView::new(topology, &fleet, *clock_ms);

            controller.tick(&view)
        };

        self.reconcile_placement();

        states
    }

    /// Drive one action by one step.
    pub fn advance(&mut self, id: ActionId) -> Option<ExecutionState> {
        let fleet = self.fleet_view();

        let state = {
            let Self {
                topology,
                controller,
                clock_ms,
                ..
            } = self;

            let view = ControlPlaneView::new(topology, &fleet, *clock_ms);

            controller.advance(id, &view)
        };

        self.reconcile_placement();

        state
    }

    /// Record the after-measurement and get the verdict.
    pub fn measure(
        &mut self,
        id: ActionId,
        after: &WorkloadProfile,
    ) -> Result<ExecutionOutcome, MeasurementError> {
        let fleet = self.fleet_view();

        let Self {
            topology,
            controller,
            clock_ms,
            ..
        } = self;

        let view = ControlPlaneView::new(topology, &fleet, *clock_ms);

        controller.measure(id, after, &view)
    }

    /// Tear an in-flight action down on request.
    pub fn abort(&mut self, id: ActionId) -> Option<ExecutionState> {
        let fleet = self.fleet_view();

        let Self {
            topology,
            controller,
            clock_ms,
            ..
        } = self;

        let view = ControlPlaneView::new(topology, &fleet, *clock_ms);

        controller.abort(id, &view)
    }

    /// Push the map and the fleet's liveness into the mechanisms.
    fn sync_placement(&mut self) {
        self.placement.borrow_mut().adopt_topology(&self.topology);
        self.placement_generation = self.placement.borrow().generation();
        self.controller.seed_replicas(&self.topology);
        self.sync_nodes();
    }

    fn sync_nodes(&mut self) {
        self.placement.borrow_mut().observe_nodes(
            &self.nodes,
            self.clock_ms,
            self.heartbeat_deadline_ms,
        );
    }

    /// Bring the runtime's placement map back in line after a tick.
    ///
    /// Two sources, in this order and for different reasons. The mechanism
    /// moves a coordinate at the instant authority moves, so its map is the
    /// one that is right -- and it is the *only* source for an action whose
    /// destination the mechanism chose, because the controller never learned
    /// which node that was and reports no `PlacementChange` for it. The
    /// controller's own changes are then applied on top: they are its record
    /// of what it saw cut over, and they are the only source for a mechanism
    /// that keeps no map. Where both speak they agree, because both are
    /// describing the same cutover.
    fn reconcile_placement(&mut self) {
        let pulled = {
            let fabric = self.placement.borrow();

            (fabric.generation() != self.placement_generation)
                .then(|| (fabric.topology().clone(), fabric.generation()))
        };

        let moved = pulled.is_some();

        if let Some((topology, generation)) = pulled {
            self.topology = topology;
            self.placement_generation = generation;
        }

        let mut changed = moved;

        for change in self.controller.placement_changes() {
            changed |= change.apply(&mut self.topology);
        }

        if changed {
            self.controller.seed_replicas(&self.topology);
        }
    }

    /// The runtime's clock: the newest timestamp it has ingested, in epoch
    /// milliseconds.
    ///
    /// Liveness needs a "now", and the honest one here is message time, not
    /// wall time. Live, the newest heartbeat/telemetry timestamp *is* now.
    /// Replaying a captured session, wall time would declare every node in the
    /// capture dead, which says nothing about the capture. Advancing only
    /// forwards keeps an out-of-order message from rewinding the clock and
    /// resurrecting a node that had already missed its deadline.
    pub fn clock_ms(&self) -> u64 {
        self.clock_ms
    }

    fn advance_clock(&mut self, timestamp_ms: u64) {
        self.clock_ms = self.clock_ms.max(timestamp_ms);
    }

    /// Move the clock to a live "now" and re-judge the fleet's liveness at it.
    ///
    /// [`Self::clock_ms`] is message time, which is the only clock that means
    /// anything when the runtime is fed a captured session -- and it is not
    /// enough for a process that runs continuously. Liveness is a silence
    /// budget measured against the clock, so a fleet whose every node has
    /// stopped reporting also stops advancing the clock, and the silence never
    /// grows: every node stays as healthy as its last heartbeat left it,
    /// forever, which is precisely the state in which routing keeps sending
    /// traffic to a dead instance.
    ///
    /// A daemon knows a real now. Feeding it here advances the clock (never
    /// backwards, exactly like an ingested timestamp) and pushes the resulting
    /// verdicts into the mechanisms, so a node that has gone quiet becomes
    /// [`NodeAvailability::Unreachable`](fabric_routing::NodeAvailability)
    /// in the routing table on the next tick rather than on the next heartbeat
    /// that is never coming.
    pub fn tick_clock(&mut self, now_ms: u64) {
        self.advance_clock(now_ms);
        self.sync_nodes();
    }

    pub fn handle(
        &mut self,
        message: FabricMessage,
    ) -> FabricResponse {
        match message {
            FabricMessage::RegisterNode(registration) => {
                let node_id = registration.node_id.0.clone();

                self.nodes.register(&registration, self.clock_ms);
                self.sync_nodes();

                FabricResponse::Registered { node_id }
            }

            /*
             * A heartbeat is refused rather than filed when the node never
             * registered. Accepting it would let anything that can reach the
             * protocol invent a database instance for the optimizer to place
             * work on -- the inventory has exactly one entrance.
             */
            FabricMessage::Heartbeat(beat) => {
                self.advance_clock(beat.timestamp_ms);

                match self.nodes.heartbeat(&beat) {
                    Ok(()) => {
                        self.sync_nodes();

                        FabricResponse::Acknowledged
                    }

                    Err(error) => FabricResponse::Rejected {
                        reason: error.to_string(),
                    },
                }
            }

            FabricMessage::Topology(report) => {
                self.advance_clock(report.timestamp_ms);

                self.ingest_topology(report);

                FabricResponse::Acknowledged
            }

            FabricMessage::Telemetry(batch) => {
                self.advance_clock(batch.timestamp_ms);

                self.ingest_telemetry(batch);

                FabricResponse::Acknowledged
            }

            FabricMessage::Workload(observation) => {
                self.advance_clock(observation.timestamp_ms);

                self.ingest_workload(observation);

                FabricResponse::Acknowledged
            }
        }
    }

    fn ingest_topology(
        &mut self,
        report: fabric_protocol::TopologyReport,
    ) {
        for placement in report.placements {
            let shard = Shard::new(
                placement.coordinate.index() as u64,
                "unknown",
            );

            self.topology.place(
                placement.dbms_id,
                &shard,
                placement.coordinate,
                placement.region,
            );
        }

        self.sync_placement();
    }

    fn ingest_telemetry(
        &mut self,
        batch: fabric_protocol::TelemetryBatch,
    ) {
        for sample in batch.samples {
            let observation = Observation::new(
                batch.timestamp_ms,
                batch.shard.clone(),
                sample.coordinate,
                sample.metrics(),
            );

            self.analyzer.observe(&observation);

            self.state.record(
                batch.node_id.clone(),
                observation.clone(),
            );

            self.remember(observation);
        }
    }

    fn ingest_workload(
        &mut self,
        observation: fabric_protocol::WorkloadObservation,
    ) {
        let shard = Shard::new(
            0,
            observation.node_id.clone(),
        );

        let total = observation.operations_per_second;

        let metrics = WorkloadMetrics {
            operations_per_second: total,

            reads_per_second:
            total * observation.read_ratio,

            writes_per_second:
            total * observation.write_ratio,

            read_latency_us: 0.0,
            write_latency_us: 0.0,

            cpu_utilization: 0.0,
            memory_utilization: 0.0,

            storage_bytes_per_second: 0,
            network_in_bytes_per_second: 0,
            network_out_bytes_per_second: 0,

            queue_depth: 0,

            cell_breakdown: Vec::new(),
            cell_breakdown_partial: false,
        };

        let observation = Observation::new(
            observation.timestamp_ms,
            shard,
            observation.coordinate,
            metrics,
        );

        let dbms_id =
            fabric_core::DbmsId::new(
                observation.shard.workload_domain.clone(),
            );

        self.analyzer.observe(&observation);

        self.state.record(
            dbms_id,
            observation.clone(),
        );

        self.remember(observation);
    }

    pub fn profile(
        &self,
        shard: &Shard,
        coordinate: Coordinate,
    ) -> Option<&WorkloadProfile> {
        self.analyzer.profile(
            shard,
            coordinate,
        )
    }

    /// The fleet the optimizer is allowed to choose a destination from.
    ///
    /// Projected from [`Self::fleet_view`] -- the *same* view the controller
    /// validates the resulting decision against -- rather than assembled a
    /// second time. That is what stops the optimizer proposing destinations
    /// the controller is bound to refuse: both are reading one answer to "is
    /// this node healthy, does it have headroom, how loaded is it".
    ///
    /// Carries no shard: candidate destinations are a property of the fleet,
    /// not of the one cell a particular decision is about. The cell being
    /// decided on is named by `WorkloadProfile::shard_id` and looked up
    /// directly against [`Self::topology`] -- see [`Self::optimize`].
    pub fn fleet(&self) -> Fleet<'_> {
        let view = self.fleet_view();

        let nodes: Vec<FleetNode> = view
            .nodes()
            .map(|status| FleetNode {
                id: status.id.clone(),
                region: status.region.clone(),
                accepts_placement: status.condition.accepts_placement(),
                hosted_placements: status.hosted_placements,
                placement_capacity: status.placement_capacity,
                utilization: status.utilization(),
            })
            .collect();

        Fleet::new(&self.topology, nodes)
    }

    /// Decide what, if anything, should happen to the cell `profile` names.
    ///
    /// `WorkloadProfile::shard_id` is what makes the cell locatable: a
    /// coordinate alone is not one, since several shards can use the same
    /// one. Every profile the analyzer produces carries its shard already
    /// (see `WorkloadAnalyzer::observe`), so this never has to decline merely
    /// because it could not tell which cell it was looking at -- only because
    /// nothing currently holds it, or the model does not think it is hot.
    ///
    /// This used to be two methods: `optimize_cell(shard_id, profile)` for a
    /// caller holding the shard separately, and `optimize(profile)` for one
    /// that did not and had to accept an ambiguous answer. Now that the shard
    /// travels with the profile, a second, shard-carrying entry point would
    /// only invite a caller to pass a `shard_id` that disagrees with
    /// `profile.shard_id` -- a second source of truth for the same fact -- so
    /// there is one method, and it always uses the profile's own shard.
    pub fn optimize(
        &self,
        profile: &WorkloadProfile,
    ) -> OptimizationDecision {
        self.optimizer.optimize(profile, self.fleet())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use fabric_core::{Coordinate, DbmsId};
    use fabric_protocol::{NodeHeartbeat, NodeRegistration};

    fn register(id: &str) -> FabricMessage {
        FabricMessage::RegisterNode(NodeRegistration::new(
            DbmsId::new(id),
            "0.13.0",
            "us-east",
        ))
    }

    fn heartbeat(id: &str, timestamp_ms: u64, healthy: bool) -> FabricMessage {
        FabricMessage::Heartbeat(NodeHeartbeat {
            node_id: DbmsId::new(id),
            timestamp_ms,
            healthy,
        })
    }

    #[test]
    fn registration_is_retained_not_just_acknowledged() {
        let mut runtime = FabricRuntime::new();

        let response = runtime.handle(register("db-a"));

        assert!(matches!(
            response,
            FabricResponse::Registered { ref node_id } if node_id == "db-a"
        ));
        assert_eq!(runtime.nodes().len(), 1);
    }

    #[test]
    fn a_heartbeat_updates_the_registry_and_the_clock() {
        let mut runtime = FabricRuntime::new();
        runtime.handle(register("db-a"));

        let response = runtime.handle(heartbeat("db-a", 12_000, true));

        assert!(matches!(response, FabricResponse::Acknowledged));
        assert_eq!(runtime.clock_ms(), 12_000);

        let node = runtime.nodes().get(&DbmsId::new("db-a")).expect("registered");
        assert_eq!(node.health(runtime.clock_ms(), 30_000), NodeHealth::Healthy);
    }

    #[test]
    fn a_heartbeat_from_an_unregistered_node_is_rejected() {
        let mut runtime = FabricRuntime::new();

        let response = runtime.handle(heartbeat("ghost", 12_000, true));

        match response {
            FabricResponse::Rejected { reason } => {
                assert!(reason.contains("ghost"), "reason was: {reason}");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        assert!(runtime.nodes().is_empty());
    }

    #[test]
    fn a_node_that_stops_beating_goes_unreachable() {
        let mut runtime = FabricRuntime::new();
        runtime.handle(register("db-a"));
        runtime.handle(register("db-b"));
        runtime.handle(heartbeat("db-a", 10_000, true));
        runtime.handle(heartbeat("db-b", 10_000, true));

        // Only db-b keeps reporting; the clock moves with it.
        runtime.handle(heartbeat("db-b", 100_000, true));
        assert_eq!(runtime.clock_ms(), 100_000);

        let now = runtime.clock_ms();
        let stale: Vec<String> = runtime
            .nodes()
            .unhealthy(now, DEFAULT_HEARTBEAT_DEADLINE_MS)
            .map(|node| node.id.0.clone())
            .collect();
        assert_eq!(stale, vec!["db-a".to_string()]);
    }

    /// The daemon's clock tick is what makes a silence budget mean anything:
    /// without it a fleet that has entirely stopped reporting also stops the
    /// clock, so no node ever exceeds its deadline and routing keeps every
    /// dead instance serviceable.
    #[test]
    fn a_clock_tick_alone_takes_a_silent_node_out_of_routing() {
        let mut runtime = FabricRuntime::new().with_heartbeat_deadline_ms(5_000);
        runtime.handle(register("db-a"));
        runtime.handle(heartbeat("db-a", 10_000, true));

        let id = DbmsId::new("db-a");

        assert_eq!(
            runtime.placement().borrow().routing().node(&id).unwrap().availability,
            fabric_routing::NodeAvailability::Serviceable
        );

        // Nothing reports again; only time passes.
        runtime.tick_clock(20_000);

        assert_eq!(runtime.clock_ms(), 20_000);
        assert_eq!(
            runtime.placement().borrow().routing().node(&id).unwrap().availability,
            fabric_routing::NodeAvailability::Unreachable
        );

        // The clock never runs backwards, so a late tick cannot resurrect it.
        runtime.tick_clock(1_000);
        assert_eq!(runtime.clock_ms(), 20_000);
    }

    /// The observation window is a window. It used to be every sample ever
    /// ingested, which a long-running daemon turns into an unbounded leak.
    #[test]
    fn the_observation_window_is_bounded() {
        let mut runtime = FabricRuntime::new();

        for tick in 0..(RECENT_OBSERVATIONS as u64 * 2) {
            runtime.handle(FabricMessage::Telemetry(fabric_protocol::TelemetryBatch {
                timestamp_ms: 1_000 + tick,
                node_id: DbmsId::new("db-a"),
                shard: Shard::new(1, "us-east"),
                samples: vec![fabric_protocol::TelemetrySample {
                    coordinate: Coordinate::new(0, 0),
                    operations_per_second: 1.0,
                    read_ratio: 1.0,
                    write_ratio: 0.0,
                    read_latency_us: 0.0,
                    write_latency_us: 0.0,
                    cpu_utilization: 0.0,
                    memory_utilization: 0.0,
                    queue_depth: 0,
                    cell_breakdown: Vec::new(),
                    cell_breakdown_partial: false,
                }],
            }));
        }

        assert!(
            runtime.observations().len() <= RECENT_OBSERVATIONS,
            "the window grew to {}",
            runtime.observations().len()
        );

        // The newest sample is still the newest: the window drops from the
        // front, which is the end that has stopped being interesting.
        assert_eq!(
            runtime.observations().last().expect("a window").timestamp_ms,
            1_000 + (RECENT_OBSERVATIONS as u64 * 2) - 1
        );

        // One profile per cell, and one latest observation per location,
        // whatever the window did.
        assert_eq!(runtime.analyzer().len(), 1);
        assert_eq!(runtime.state().len(), 1);
    }

    #[test]
    fn an_operator_policy_reaches_the_controller() {
        let policy = fabric_controller::ControllerPolicy {
            phase_timeout_ms: 1_234,
            ..fabric_controller::ControllerPolicy::default()
        };

        let runtime = FabricRuntime::with_policy(policy);

        assert_eq!(runtime.controller().policy().phase_timeout_ms, 1_234);
    }

    #[test]
    fn telemetry_also_advances_the_clock() {
        let mut runtime = FabricRuntime::new();

        runtime.handle(FabricMessage::Telemetry(fabric_protocol::TelemetryBatch {
            timestamp_ms: 7_000,
            node_id: DbmsId::new("db-a"),
            shard: Shard::new(1, "us-east"),
            samples: vec![fabric_protocol::TelemetrySample {
                coordinate: Coordinate::new(0, 0),
                operations_per_second: 10.0,
                read_ratio: 0.9,
                write_ratio: 0.1,
                read_latency_us: 0.0,
                write_latency_us: 0.0,
                cpu_utilization: 0.0,
                memory_utilization: 0.0,
                queue_depth: 0,
                cell_breakdown: Vec::new(),
                cell_breakdown_partial: false,
            }],
        }));

        assert_eq!(runtime.clock_ms(), 7_000);
        assert_eq!(runtime.analyzer().len(), 1);
    }
}
