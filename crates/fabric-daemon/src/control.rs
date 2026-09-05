//! The loop, and the only thread allowed to touch the runtime.
//!
//! ```text
//!  probe GET /  ─┐
//!                ├─► FabricRuntime ─► analyzer ─► optimizer ─► controller
//!  poll /stats  ─┘        │                                        │
//!                         │                                PlacementMechanism
//!                         │                                        │
//!                         └──────── routing table ◄────────────────┘
//!                                        │
//!                                        ▼  publish_routing
//!                                   FrontDoor  (the data path, other threads)
//! ```
//!
//! # Why this is a thread and not a task
//!
//! [`FabricRuntime`] owns its mechanisms through `Rc<RefCell<_>>`, deliberately:
//! routing, replication and migration are single-threaded pure logic, and the
//! controller's executor has to mutate the very state the runtime reads. That
//! makes the runtime `!Send`, so it cannot be moved between worker threads by
//! a multi-threaded scheduler. It gets its own OS thread with a current-thread
//! runtime, and everything it needs from the outside world — probes, `/stats`
//! samples — is awaited *on that thread*.
//!
//! This is not a workaround; it is the shape that makes the lock discipline
//! below possible. The control plane's state is not shared, so it needs no
//! lock at all, and the data path can never block on it.
//!
//! # Lock discipline
//!
//! Exactly two things cross the boundary between this loop and the data path,
//! and neither is ever held across an `.await`:
//!
//! * **The front door's fleet** (`RwLock<Fleet>` inside [`FrontDoor`]). This
//!   loop takes the *write* side only through `publish_routing`, whose whole
//!   critical section is one move-assignment of a table cloned beforehand. The
//!   clone happens under the `RefCell` borrow, the borrow is dropped, and only
//!   then is the lock taken — so a `RefCell` borrow and the fleet lock are
//!   never held at the same time, in either order. Requests take the read side
//!   and resolve a whole batch under one guard with no await, which is what
//!   makes a batch's keys answer to one snapshot.
//! * **The status snapshot** (`RwLock<Arc<Status>>`). Built whole, swapped in
//!   one assignment, cloned out by admin handlers in one `Arc::clone`.
//!
//! There is nothing else. The control loop cannot starve the data path,
//! because the longest it ever holds the only lock they share is one pointer
//! write; and the data path cannot stall the control loop, because a reader's
//! guard is dropped before it forwards anything.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use fabric_controller::{
    ActionId, DecisionEnvelope, ExecutionState, OutcomeReport, PlanPhase,
};
use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_facetql::frontdoor::FrontDoor;
use fabric_facetql::mover::{CellScope, MoverReport};
use fabric_facetql::{FacetqlClient, FacetqlEndpoint, PlacementStore, StoredPlacement};
use fabric_protocol::{FabricMessage, NodeHeartbeat, NodeRegistration};
use fabric_routing::NodeAvailability;
use fabric_routing::RoutingKey;
use fabric_runtime::{FabricRuntime, NodeHealth, WriteSeq};
use fabric_topology::TopologyRegistry;
use fabric_workload::WorkloadProfile;

use crate::config::Settings;
use crate::liveness::{LivenessProber, ProbeTarget};
use crate::mover::MoverSupervisor;
use crate::status::{
    ActionStatus, BackendStatus, CopyingStatus, DecisionCounters, KeyspaceStatus,
    MigrationStatus, MoverStatus, PlacementStatus, RefusedCopy, Status, StatusHandle,
    StoreStatus,
};
use crate::telemetry::TelemetrySource;
use crate::{now_ms, DaemonError};

use std::sync::Arc;

/// `GET /stats` reports no server build, and parsing it out of the
/// human-readable `GET /` banner would be exactly the contract drift the
/// FacetQL client exists to avoid. Registered honestly as unknown.
const UNKNOWN_VERSION: &str = "unknown";

/// How many concluded actions the admin surface reports, newest last.
const PUBLISHED_HISTORY: usize = 50;

/// Something the operator port asks the loop to do. Every variant carries its
/// own reply channel, because the answer is only knowable on this thread.
#[derive(Debug)]
pub enum ControlRequest {
    /// The thing that is moving the bytes reports how far it has got.
    ///
    /// Fabric copies no data — `PlacementFabric::record_transfer` is
    /// documented as fed from outside for exactly that reason — and FacetQL
    /// exposes no bulk export/import, so nothing in this workspace *can* copy
    /// a cell between instances. This is the door that fact leaves open: the
    /// mover reports, and a transfer nobody reports on never completes and is
    /// rolled back by the controller's phase timeout.
    Transfer {
        id: ActionId,
        atoms_copied: usize,
        bytes_copied: u64,
        resident_bytes: Option<u64>,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },

    /// This daemon's own mover reporting on a copy it is making.
    ///
    /// The same queue `Transfer` arrives on, deliberately: the internal mover
    /// and an out-of-band one report through one seam, so neither can grow a
    /// privilege the other lacks. It carries more than a transfer report
    /// because it knows more — how far behind the destination is, and whether
    /// the copy checks out against its source — and those two facts are what
    /// make `pending_writes` real and cutover conditional on evidence.
    ///
    /// `reply` is optional: a progress ping fired from inside the bulk copy
    /// has nothing to wait for, while a catch-up pass uses the answer as its
    /// stop signal.
    Copy {
        id: ActionId,
        report: MoverReport,
        reply: Option<tokio::sync::oneshot::Sender<Result<String, String>>>,
    },

    /// Tear an action down on request.
    Abort {
        id: ActionId,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
}

/// One action the daemon could not finish, and why that matters.
#[derive(Debug, Clone)]
pub struct Abandoned {
    pub id: u64,
    pub target: String,
    pub source: String,
    pub destination: Option<String>,
    pub state: String,
    pub migration_phase: Option<String>,
}

/// What draining achieved.
#[derive(Debug, Clone, Default)]
pub struct ShutdownReport {
    /// Actions torn down before their cutover: the arrangement was restored.
    pub rolled_back: Vec<u64>,

    /// Actions that executed but were never measured. No data is at risk; the
    /// verdict is.
    pub unmeasured: Vec<u64>,

    /// Actions past the point of no return that the drain budget ran out on.
    /// **Never aborted**: an abort after cutover is `Irreversible`, and
    /// recording one to make shutdown look tidy would be a lie about where the
    /// data is.
    pub abandoned: Vec<Abandoned>,

    /// Whatever could not be written back to the durable placement store.
    pub unpersisted: Option<String>,
}

impl ShutdownReport {
    pub fn is_clean(&self) -> bool {
        self.abandoned.is_empty() && self.unpersisted.is_none()
    }
}

/// The control plane: the runtime, the things that feed it, and the front door
/// it publishes to.
pub struct ControlPlane {
    settings: Settings,
    runtime: FabricRuntime,
    door: FrontDoor,
    prober: LivenessProber,
    telemetry: Box<dyn TelemetrySource>,

    store: Option<PlacementStore>,
    versions: HashMap<String, StoredPlacement>,
    store_state: String,
    store_error: Option<String>,
    store_writes: u64,

    status: Arc<StatusHandle>,

    /// This daemon's own data movers, one per relocation in flight.
    movers: MoverSupervisor,

    /// A sender into the same queue the admin port writes to, so a mover
    /// reports through the seam an out-of-band mover reports through.
    requests: tokio::sync::mpsc::UnboundedSender<ControlRequest>,

    /// An HTTP client with no request timeout, for `GET /events` alone. A
    /// subscription is the one request that is supposed never to finish, and
    /// the shared client's timeout would sever it on a schedule.
    streaming: reqwest::Client,

    /// How many of a mover's observed writes have been accepted into each
    /// migration's sequence space. Kept here rather than derived, because the
    /// migration exposes its high-water mark only as a gap, and a mover's
    /// count is cumulative from the moment its feed opened.
    accepted_writes: BTreeMap<u64, u64>,

    /// The routing generation the front door has been given. Published when it
    /// moves, which is at every phase transition and every liveness change.
    published_generation: u64,

    started_at_ms: u64,
    cycles: u64,
    draining: bool,

    next_probe_ms: u64,
    next_poll_ms: u64,

    decisions: DecisionCounters,
    probes: BTreeMap<String, (String, u64)>,
    samples: BTreeMap<String, String>,
}

impl ControlPlane {
    /// Build the control plane and the front door it will publish to.
    ///
    /// Runs one full cycle before returning, so the fleet's liveness has been
    /// established once by the time the data port accepts its first
    /// connection. Without it every request between binding and the first
    /// probe would be answered `503`: a registered node that has never proven
    /// liveness is unreachable, and silence is never read as health.
    pub async fn boot(
        settings: Settings,
        telemetry: Box<dyn TelemetrySource>,
        status: Arc<StatusHandle>,
        requests: tokio::sync::mpsc::UnboundedSender<ControlRequest>,
    ) -> Result<(Self, FrontDoor), DaemonError> {
        let started_at_ms = now_ms();

        let mut runtime = FabricRuntime::with_policy(settings.policy)
            .with_placement_capacity(settings.placement_capacity)
            .with_heartbeat_deadline_ms(settings.silence_budget_ms);

        runtime.tick_clock(started_at_ms);

        for backend in &settings.backends {
            runtime.handle(FabricMessage::RegisterNode(NodeRegistration::new(
                backend.id.clone(),
                UNKNOWN_VERSION,
                backend.region.clone(),
            )));
        }

        let (store, versions, topology, store_state) =
            Self::bootstrap_placement(&settings).await?;

        runtime.adopt_topology(topology);

        let prober = LivenessProber::new(
            settings
                .backends
                .iter()
                .map(|backend| ProbeTarget {
                    id: backend.id.clone(),
                    url: backend.url.clone(),
                })
                .collect(),
            Duration::from_millis(settings.probe_timeout_ms),
        )
        .map_err(DaemonError::Startup)?;

        let routing = runtime.placement().borrow().routing().clone();

        let door = FrontDoor::with_config(
            settings.keyspace.clone(),
            settings.front_door_backends().map_err(DaemonError::Config)?,
            routing,
            settings.front_door.clone(),
        )
        .map_err(|error| DaemonError::Startup(error.to_string()))?;

        let streaming = reqwest::Client::builder().build().map_err(|error| {
            DaemonError::Startup(format!(
                "could not build the mover's streaming HTTP client: {error}"
            ))
        })?;

        let mut plane = Self {
            settings,
            runtime,
            door: door.clone(),
            prober,
            telemetry,
            store,
            versions,
            store_state,
            store_error: None,
            store_writes: 0,
            status,
            movers: MoverSupervisor::new(),
            requests,
            streaming,
            accepted_writes: BTreeMap::new(),
            published_generation: 0,
            started_at_ms,
            cycles: 0,
            draining: false,
            next_probe_ms: 0,
            next_poll_ms: 0,
            decisions: DecisionCounters::default(),
            probes: BTreeMap::new(),
            samples: BTreeMap::new(),
        };

        plane.cycle().await;

        Ok((plane, door))
    }

    /// Read the placement map from wherever it is authoritative.
    ///
    /// With a store configured, the store is the authority and the declared
    /// placements only seed it the first time: a cell this daemon moved before
    /// its last restart lives on the instance the store names, and starting
    /// from the file would route to the instance that used to hold it. Without
    /// one, the file is all there is — and the daemon says so on the admin
    /// port rather than implying durability it does not have.
    ///
    /// A configured store that cannot be read is a startup failure. Carrying
    /// on with the declared map would be choosing the one map that is known to
    /// be possibly wrong.
    async fn bootstrap_placement(
        settings: &Settings,
    ) -> Result<
        (
            Option<PlacementStore>,
            HashMap<String, StoredPlacement>,
            TopologyRegistry,
            String,
        ),
        DaemonError,
    > {
        let Some(id) = settings.placement_store.clone() else {
            return Ok((
                None,
                HashMap::new(),
                settings.topology.clone(),
                "not configured: the placement map is declared, not durable"
                    .to_string(),
            ));
        };

        let backend = settings
            .backend(&id)
            .expect("the configuration resolved this backend");

        let token = backend
            .token
            .clone()
            .expect("the configuration required a token for the placement store");

        let endpoint = FacetqlEndpoint::new(id.clone(), backend.url.clone(), token)
            .map_err(|error| DaemonError::Startup(error.to_string()))?;

        let store = PlacementStore::new(FacetqlClient::new(endpoint));

        let (registry, versions) = store.load_registry().await.map_err(|error| {
            DaemonError::Startup(format!(
                "could not read the durable placement store on '{}': {error}. \
                 Starting from the declared map instead would route cells that \
                 have since moved to the instance that used to hold them",
                id.0
            ))
        })?;

        if !versions.is_empty() {
            let state = format!("loaded {} placement(s) from '{}'", versions.len(), id.0);

            return Ok((Some(store), versions, registry, state));
        }

        // Empty store: the declaration seeds it once, and is the authority
        // from here on.
        let mut seeded = HashMap::new();

        for placement in settings.topology.placements() {
            let stored = store.create(placement).await.map_err(|error| {
                DaemonError::Startup(format!(
                    "could not seed the placement store on '{}': {error}",
                    id.0
                ))
            })?;

            seeded.insert(stored.address(), stored);
        }

        let state = format!(
            "seeded {} placement(s) into '{}' from the declaration",
            seeded.len(),
            id.0
        );

        Ok((Some(store), seeded, settings.topology.clone(), state))
    }

    // ───────────────────────────────────────────────────────────── the loop

    /// Run until told to drain, then drain, and report what draining achieved.
    pub async fn run(
        mut self,
        mut stop: tokio::sync::watch::Receiver<bool>,
        mut requests: tokio::sync::mpsc::UnboundedReceiver<ControlRequest>,
    ) -> ShutdownReport {
        let cadence = Duration::from_millis(self.settings.cadence.control_cycle_ms);
        let mut ticker = tokio::time::interval(cadence);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                /*
                 * A report from whatever is moving the bytes is applied the
                 * moment it arrives rather than at the next cycle: it is the
                 * only thing that advances a transfer, and making a mover wait
                 * a whole cadence for its own progress to count would put the
                 * cadence inside the phase timeout.
                 */
                Some(request) = requests.recv() => {
                    self.serve(request).await;
                }

                _ = ticker.tick() => {
                    self.cycle().await;
                }

                result = stop.changed() => {
                    if result.is_err() || *stop.borrow() {
                        break;
                    }
                }
            }
        }

        self.drain().await
    }

    /// One full pass: observe, decide, execute, measure, publish.
    async fn cycle(&mut self) {
        let now = now_ms();

        // Time passing is itself evidence: it is what turns silence into
        // unreachability, and without it a fleet that has entirely stopped
        // reporting would also stop the clock that judges it.
        self.runtime.tick_clock(now);

        if now >= self.next_probe_ms {
            self.probe(now).await;
            self.next_probe_ms = now + self.settings.cadence.liveness_probe_ms;
        }

        if now >= self.next_poll_ms {
            self.poll().await;
            self.next_poll_ms = now + self.settings.cadence.telemetry_poll_ms;
        }

        if !self.draining {
            self.decide();
        }

        self.runtime.advance_actions();

        /*
         * After the actions advance, so a plan that reached its copying phase
         * this cycle gets its mover this cycle: a whole cadence of latency at
         * the front of every migration would come straight off the phase
         * timeout budget.
         */
        self.drive_movers();

        self.measure();

        self.publish_routing();
        self.persist().await;

        self.cycles += 1;
        self.publish_status(now);
    }

    /// Ask every instance whether it is there, and file the answers.
    async fn probe(&mut self, now_ms: u64) {
        for (id, probe) in self.prober.sweep().await {
            self.probes.insert(id.0.clone(), (probe.describe(), now_ms));

            /*
             * An unreachable instance files NO heartbeat. Filing an unhealthy
             * one would reset its silence clock on every probe, so it would
             * sit at `Degraded` -- which is routable, deliberately, because a
             * degraded instance may hold the only copy -- forever, and traffic
             * would keep going to a process that is not running.
             */
            if !probe.reached() {
                continue;
            }

            self.runtime.handle(FabricMessage::Heartbeat(NodeHeartbeat {
                node_id: id,
                timestamp_ms: now_ms,
                healthy: probe.healthy(),
            }));
        }
    }

    async fn poll(&mut self) {
        let sample = self.telemetry.sample().await;

        for (id, note) in sample.notes {
            self.samples.insert(id.0, note);
        }

        for message in sample.messages {
            self.runtime.handle(message);
        }
    }

    /// Optimize every hot cell, and submit what survives the optimizer's own
    /// gate to the controller's.
    ///
    /// The optimizer's `should_execute` is its opinion of its own output; the
    /// controller re-derives the same question and every other rule besides. A
    /// refusal here is the control plane working, and it is counted and
    /// reported rather than swallowed.
    fn decide(&mut self) {
        let generation = self.runtime.fleet_view().generation();

        let hot: Vec<WorkloadProfile> = self
            .runtime
            .analyzer()
            .profiles()
            .filter(|profile| profile.is_hot())
            .cloned()
            .collect();

        let now = now_ms();

        for profile in hot {
            let target = fabric_controller::ActionTarget::new(
                profile.shard_id,
                profile.coordinate,
            );

            if self.cooling(target, now) {
                continue;
            }

            let decision = self.runtime.optimize(&profile);

            if !decision.should_execute() {
                continue;
            }

            /*
             * The observation's own timestamp, not "now". `max_decision_age_ms`
             * exists to stop the control plane acting on workload that is no
             * longer evidence, and stamping the decision with the current
             * instant would defeat it precisely for a cell whose instance has
             * gone quiet -- whose last profile stays hot forever.
             */
            let Some(observed_at_ms) =
                self.observed_at_ms(profile.shard_id, profile.coordinate)
            else {
                continue;
            };

            self.decisions.proposed += 1;

            let envelope = DecisionEnvelope::new(decision, observed_at_ms, generation);

            match self.runtime.submit(envelope, &profile) {
                Ok(_) => self.decisions.admitted += 1,

                Err(error) => {
                    self.decisions.refused += 1;
                    self.decisions.last_refusal = Some(error.to_string());
                }
            }
        }
    }

    /// Whether this cell acted recently enough that asking again would be
    /// oscillation rather than control.
    ///
    /// Measured from when the last action on it *concluded* -- the verdict, not
    /// the cutover -- because that is the instant the loop last learned
    /// anything about this cell, and the observations either side of it
    /// describe a system that was still settling.
    fn cooling(&self, target: fabric_controller::ActionTarget, now_ms: u64) -> bool {
        self.runtime
            .controller()
            .actions()
            .records()
            .filter(|record| record.target() == target && record.state().is_terminal())
            .map(|record| record.report().concluded_at_ms)
            .max()
            .is_some_and(|concluded_at_ms| {
                now_ms.saturating_sub(concluded_at_ms) < self.settings.decision_cooldown_ms
            })
    }

    /// Hand the controller the after-measurement for anything that has settled.
    ///
    /// An action stays `AwaitingMeasurement` until this happens: executed is
    /// not successful, and the verdict — including `unchanged` for an action
    /// that cost something and bought nothing — is the loop's training signal.
    fn measure(&mut self) {
        let waiting: Vec<(ActionId, u64, Coordinate)> = self
            .runtime
            .controller()
            .awaiting_measurement()
            .map(|record| {
                let target = record.target();
                (record.id(), target.shard_id, target.coordinate)
            })
            .collect();

        for (id, shard_id, coordinate) in waiting {
            let Some(after) = self
                .runtime
                .analyzer()
                .profile(&Shard::new(shard_id, ""), coordinate)
                .cloned()
            else {
                // Nothing has reported from the cell's new home yet. The
                // controller's own measurement deadline is what ends the wait.
                continue;
            };

            // `TooSoon` is the normal answer before the settle window closes.
            let _ = self.runtime.measure(id, &after);
        }
    }

    /// Give the front door the routing table, if it has changed.
    ///
    /// Every phase transition passes through
    /// `PlacementFabric::step_migration`, which re-snapshots the migration
    /// into the routing table on the way out — so the generation moving is
    /// exactly "something a request would be routed by has changed", and this
    /// is what carries a cutover, a failover or a liveness change to live
    /// traffic without a restart.
    fn publish_routing(&mut self) {
        let routing = {
            let fabric = self.runtime.placement().borrow();

            if fabric.routing().generation() == self.published_generation {
                return;
            }

            // Cloned under the borrow; the borrow ends here, and only then is
            // the front door's lock taken.
            fabric.routing().clone()
        };

        self.published_generation = routing.generation();
        self.door.publish_routing(routing);
    }

    /// Write completed moves back to the durable placement store.
    async fn persist(&mut self) {
        let Some(store) = &self.store else {
            return;
        };

        let current: Vec<fabric_topology::Placement> =
            self.runtime.topology().placements().cloned().collect();

        for placement in current {
            let address = fabric_facetql::address_of(placement.shard_id, placement.coordinate);

            match self.versions.get(&address) {
                Some(stored) if stored.placement == placement => continue,

                Some(stored) => match store.update(stored, &placement).await {
                    Ok(next) => {
                        self.versions.insert(address, next);
                        self.store_writes += 1;
                        self.store_error = None;
                    }

                    Err(error) => {
                        /*
                         * A failed compare-and-set means another controller
                         * wrote this cell first and nothing was applied. That
                         * is not a retryable blip: two control planes are
                         * moving the same data, and the honest response is to
                         * say so loudly rather than to invent a merge.
                         */
                        self.store_error = Some(format!(
                            "could not persist {}: {error}",
                            placement_label(&placement)
                        ));
                    }
                },

                None => match store.create(&placement).await {
                    Ok(stored) => {
                        self.versions.insert(address, stored);
                        self.store_writes += 1;
                        self.store_error = None;
                    }

                    Err(error) => {
                        self.store_error = Some(format!(
                            "could not record {}: {error}",
                            placement_label(&placement)
                        ));
                    }
                },
            }
        }
    }

    // ────────────────────────────────────────────────── operator requests

    async fn serve(&mut self, request: ControlRequest) {
        match request {
            ControlRequest::Transfer {
                id,
                atoms_copied,
                bytes_copied,
                resident_bytes,
                reply,
            } => {
                let answer = self.record_transfer(id, atoms_copied, bytes_copied, resident_bytes);

                if answer.is_ok() {
                    // Apply it now: this is what advances the phase.
                    self.runtime.advance(id);
                    self.publish_routing();
                    self.persist().await;
                    self.publish_status(now_ms());
                }

                let _ = reply.send(answer);
            }

            ControlRequest::Copy { id, report, reply } => {
                let answer = self.record_copy(id, &report);

                if answer.is_ok() {
                    // Same as a transfer report: applied at once, because it
                    // is the only thing that advances the phase and making a
                    // mover wait a whole cadence for its own progress to count
                    // would put the cadence inside the phase timeout.
                    self.runtime.advance(id);
                    self.publish_routing();
                    self.persist().await;
                    self.publish_status(now_ms());
                }

                if let Some(reply) = reply {
                    let _ = reply.send(answer);
                }
            }

            ControlRequest::Abort { id, reply } => {
                let answer = self.abort(id);

                self.publish_routing();
                self.publish_status(now_ms());

                let _ = reply.send(answer);
            }
        }
    }

    fn record_transfer(
        &mut self,
        id: ActionId,
        atoms_copied: usize,
        bytes_copied: u64,
        resident_bytes: Option<u64>,
    ) -> Result<String, String> {
        let record = self
            .runtime
            .controller()
            .record(id)
            .ok_or_else(|| format!("no action {id}"))?;

        if record.state().is_terminal() {
            return Err(format!(
                "{id} has already concluded ({})",
                record.state().label()
            ));
        }

        let target = record.target();

        let destination = destination_of(record)
            .ok_or_else(|| format!("{id} copies nothing: its plan adds no copy"))?;

        let at_ms = now_ms();

        {
            let placement = self.runtime.placement();
            let mut fabric = placement.borrow_mut();

            if let Some(bytes) = resident_bytes {
                fabric.record_size(target, bytes);
            }

            fabric.record_transfer(target, &destination, atoms_copied, bytes_copied, at_ms);
        }

        Ok(format!(
            "{id}: {bytes_copied} byte(s), {atoms_copied} atom(s) recorded against '{}'",
            destination.0
        ))
    }

    /// Fold one mover report into the runtime.
    ///
    /// Three separate facts land here, and the order matters:
    ///
    /// 1. **The copy's progress**, as bytes and rows that actually committed —
    ///    the same `record_transfer` seam `POST /actions/{id}/transfer` uses,
    ///    so `poll_phase`'s fraction is measured rather than simulated. The
    ///    atom count goes to 1 only when the bulk copy has walked the whole
    ///    cell *and* a check has passed on it: a coordinate is one atom, and
    ///    claiming it copied is claiming the cell is there.
    /// 2. **The write gap.** Each change the mover saw on the source is one
    ///    position in the migration's write-sequence space, and each one it
    ///    reconciled onto the destination is one position drained. That is
    ///    what makes `pending_writes` a measurement instead of a zero, and it
    ///    is what `complete_cutover` refuses to move authority past.
    /// 3. **The verdict**, which is what `PlacementMechanism::poll_cutover`
    ///    consults before the irreversible step.
    fn record_copy(
        &mut self,
        id: ActionId,
        report: &MoverReport,
    ) -> Result<String, String> {
        let record = self
            .runtime
            .controller()
            .record(id)
            .ok_or_else(|| format!("no action {id}"))?;

        if record.state().is_terminal() {
            return Err(format!(
                "{id} has already concluded ({})",
                record.state().label()
            ));
        }

        let target = record.target();

        let destination = destination_of(record)
            .ok_or_else(|| format!("{id} copies nothing: its plan adds no copy"))?;

        let at_ms = now_ms();

        /*
         * A coordinate is exactly one atom of its shard's grid. The copy is
         * only "the atom" once the whole cell has been walked and checked --
         * reporting 1 for a snapshot that had merely finished would let the
         * migration leave `Copying` on the strength of a walk nobody had
         * compared against its source.
         */
        let atoms = usize::from(report.snapshot_complete && report.verdict.is_verified());

        let mut accepted = self.accepted_writes.get(&id.0).copied().unwrap_or(0);

        {
            let placement = self.runtime.placement();
            let mut fabric = placement.borrow_mut();

            // Arms the cutover gate. Idempotent, and it never overwrites a
            // verdict already reported.
            fabric.demand_verification(target, &destination);

            if let Some(bytes) = report.resident_bytes {
                fabric.record_size(target, bytes);
            }

            fabric.record_transfer(
                target,
                &destination,
                atoms,
                report.bytes_copied,
                at_ms,
            );

            /*
             * `accept_write` refuses while the fence is up, and that refusal
             * is not an error here. A change whose event arrives after the
             * fence went up is either a straggler from before it or a write
             * that reached the source another way; either is reconciled by the
             * mover and certified by the check, and neither is something this
             * loop can tell apart. So counting stops at the fence and the
             * check -- which compares the two instances in full -- is what
             * says the destination is complete.
             */
            while accepted < report.observed_writes {
                if fabric
                    .accept_write(target, WriteSeq(accepted))
                    .is_err()
                {
                    break;
                }

                accepted += 1;
            }

            /*
             * Clamped to what was accepted: the migration refuses a sequence
             * the source never took (`UnknownWrite`), and reporting a drain
             * past the fence would be claiming to have applied a write the
             * machine has no record of.
             */
            let applied = report.applied_writes.min(accepted);

            if applied > 0 {
                let _ = fabric.record_applied(target, WriteSeq(applied - 1), at_ms);
            }

            fabric.record_verification(target, &destination, report.verdict.clone());
        }

        self.accepted_writes.insert(id.0, accepted);

        Ok(format!(
            "{id}: {} row(s), {} byte(s) onto '{}'; {} change(s) seen, {} applied; copy {}",
            report.rows_copied,
            report.bytes_copied,
            destination.0,
            report.observed_writes,
            report.applied_writes,
            report.verdict.label()
        ))
    }

    /// Make sure every relocation in flight has a mover, and no concluded one
    /// still has.
    ///
    /// A `Replicate` gets none: it moves no authority, so it has no cutover to
    /// gate and no write gap to close, and the copy it makes is a replica the
    /// replication crate seeds. Only a plan that *removes* the source is a
    /// move, and only a move needs the bytes to be somewhere else before
    /// authority follows them.
    fn drive_movers(&mut self) {
        let relocating: Vec<(ActionId, DbmsId, DbmsId, Coordinate, u64)> = self
            .runtime
            .controller()
            .in_flight()
            .filter(|record| !record.plan().removes.is_empty())
            /*
             * A copy stops the instant authority moves. Past the cutover the
             * destination is the one taking writes, so the source is no longer
             * the truth to reconcile against and a mover still comparing the
             * two would report the destination's own new writes as a
             * difference -- a false verdict on a question nobody is asking any
             * more.
             */
            .filter(|record| !record.has_cut_over())
            .filter_map(|record| {
                let target = record.target();

                destination_of(record).map(|destination| {
                    (
                        record.id(),
                        record.source().clone(),
                        destination,
                        target.coordinate,
                        target.shard_id,
                    )
                })
            })
            .collect();

        for (id, source, destination, coordinate, shard_id) in relocating {
            if self.draining {
                continue;
            }

            let scope = match RoutingKey::new(shard_id, coordinate) {
                Ok(key) => CellScope::for_key(&self.settings.keyspace, key),

                Err(error) => {
                    self.movers.refuse(id, error.to_string());
                    continue;
                }
            };

            let endpoints = (
                self.mover_endpoint(&source),
                self.mover_endpoint(&destination),
            );

            match endpoints {
                (Ok(source), Ok(destination)) => {
                    self.movers.ensure(
                        id,
                        &source,
                        &destination,
                        scope,
                        self.streaming.clone(),
                        self.requests.clone(),
                    );
                }

                (Err(reason), _) | (_, Err(reason)) => {
                    self.movers.refuse(id, reason);
                }
            }
        }

        let live: std::collections::BTreeSet<u64> = self
            .runtime
            .controller()
            .in_flight()
            .filter(|record| !record.has_cut_over())
            .map(|record| record.id().0)
            .collect();

        self.movers.retain(|id| live.contains(&id.0));
        self.accepted_writes.retain(|id, _| live.contains(id));
    }

    /// The credential the mover copies with, for one instance.
    ///
    /// The same per-backend token the telemetry poller and the placement store
    /// use. That is a real constraint and it is stated rather than worked
    /// around: `POST /transaction`'s `insert_node` stamps the *writing*
    /// identity as the copied node's owner, so a cell can only be moved
    /// faithfully by a credential whose owner already owns its nodes. The
    /// mover does not paper over a mismatch — its check compares `owner` like
    /// every other field, and a re-owned copy fails verification instead of
    /// cutting over.
    fn mover_endpoint(&self, node: &DbmsId) -> Result<FacetqlEndpoint, String> {
        let backend = self
            .settings
            .backend(node)
            .ok_or_else(|| format!("'{}' is not a declared backend", node.0))?;

        let token = backend.token.clone().ok_or_else(|| {
            format!(
                "'{}' has no configured credential, so this daemon cannot copy \
                 to or from it; declare `token_env` for it, or move the data \
                 with an out-of-band mover reporting through \
                 POST /actions/{{id}}/transfer",
                node.0
            )
        })?;

        FacetqlEndpoint::new(node.clone(), backend.url.clone(), token)
            .map_err(|error| error.to_string())
    }

    fn abort(&mut self, id: ActionId) -> Result<String, String> {
        let record = self
            .runtime
            .controller()
            .record(id)
            .ok_or_else(|| format!("no action {id}"))?;

        if record.state().is_terminal() {
            return Err(format!(
                "{id} has already concluded ({})",
                record.state().label()
            ));
        }

        /*
         * The line the migration crate draws, restated where an operator can
         * hit it: up to and including an uncompleted cutover the source still
         * holds every write, so rolling back restores the arrangement. Once
         * authority has moved, discarding the destination's copy is data loss,
         * and going back is a new decision in the other direction -- which
         * this daemon will not invent on an operator's behalf.
         */
        if record.has_cut_over() {
            return Err(format!(
                "{id} has already cut over: authority for {} is on '{}', so \
                 aborting cannot restore the previous arrangement. Moving it \
                 back is a new decision in the other direction.",
                record.target(),
                destination_of(record)
                    .map(|node| node.0)
                    .unwrap_or_else(|| "the destination".to_string())
            ));
        }

        let state = self
            .runtime
            .abort(id)
            .ok_or_else(|| format!("no action {id}"))?;

        Ok(format!("{id}: {}", state.label()))
    }

    // ─────────────────────────────────────────────────────────── shutdown

    /// Stop deciding, roll back what can be rolled back, and refuse to walk
    /// away from what cannot.
    ///
    /// The bar for a clean exit is not "no actions": it is that no action is
    /// in a state where leaving it loses data. Concretely:
    ///
    /// * **Before cutover** the source still holds every write, so the action
    ///   is aborted and the arrangement is restored.
    /// * **After cutover** the destination is authoritative. Aborting there
    ///   returns `Irreversible` and closes the migration, which would record a
    ///   rollback that did not happen; so the action is instead driven to the
    ///   end of its plan, and if the drain budget runs out first it is
    ///   reported, by name, as abandoned — and the process says so with a
    ///   non-zero exit rather than exiting 0 on a fleet mid-move.
    async fn drain(mut self) -> ShutdownReport {
        self.draining = true;

        /*
         * The copies stop before anything is rolled back. A mover writing into
         * a destination whose migration is being aborted is writing to a
         * arrangement nobody is going to use, and stopping it is free: it
         * keeps no checkpoint, writes only upserts at the source's own
         * addresses, and never touches the source at all.
         */
        self.movers.stop_all();

        let deadline = now_ms() + self.settings.drain_ms;
        let mut report = ShutdownReport::default();

        let in_flight: Vec<(ActionId, bool)> = self
            .runtime
            .controller()
            .in_flight()
            .map(|record| (record.id(), record.has_cut_over()))
            .collect();

        for (id, cut_over) in in_flight {
            if cut_over {
                continue;
            }

            let Some(state) = self.runtime.abort(id) else {
                continue;
            };

            /*
             * `RolledBack` is the mechanism saying `RollbackOutcome::Restored`:
             * the staged copy is gone, the primary is back where it was, and
             * the migration is aborted. Anything else means the rollback could
             * not restore the arrangement, and reporting it as a rollback would
             * be the exact lie this whole sequence exists to avoid -- so it is
             * carried out with the abandoned actions, and the process exits
             * non-zero because of it.
             */
            if matches!(state, ExecutionState::RolledBack) {
                report.rolled_back.push(id.0);
                continue;
            }

            if let Some(record) = self.runtime.controller().record(id) {
                report.abandoned.push(abandoned(record));
            }
        }

        // Publish the rollbacks: an in-flight request must be routed by the
        // arrangement that actually exists now.
        self.publish_routing();

        // A tighter cadence than the running loop's: draining is the one time
        // the interval between polls is pure latency on the way out.
        let step =
            Duration::from_millis(self.settings.cadence.control_cycle_ms.clamp(10, 250));

        loop {
            let unfinished: Vec<ActionId> = self
                .runtime
                .controller()
                .in_flight()
                .filter(|record| {
                    matches!(
                        record.state(),
                        ExecutionState::Admitted | ExecutionState::Running { .. }
                    )
                })
                .map(|record| record.id())
                .collect();

            if unfinished.is_empty() || now_ms() >= deadline {
                break;
            }

            tokio::time::sleep(step).await;
            self.cycle().await;
        }

        for record in self.runtime.controller().actions().records() {
            match record.state() {
                ExecutionState::AwaitingMeasurement => {
                    report.unmeasured.push(record.id().0);
                }

                ExecutionState::Admitted | ExecutionState::Running { .. } => {
                    report.abandoned.push(abandoned(record));
                }

                _ => {}
            }
        }

        // Name the migration phase each abandoned action is stuck in: "cutover"
        // and "cleanup" are very different things to hand an operator.
        let phases: BTreeMap<u64, String> = {
            let fabric = self.runtime.placement().borrow();

            self.runtime
                .controller()
                .actions()
                .records()
                .filter_map(|record| {
                    fabric
                        .migration(record.target())
                        .map(|migration| (record.id().0, migration.phase().label().to_string()))
                })
                .collect()
        };

        for abandoned in &mut report.abandoned {
            abandoned.migration_phase = phases.get(&abandoned.id).cloned();
        }

        self.persist().await;
        report.unpersisted = self.store_error.clone();

        self.publish_routing();
        self.publish_status(now_ms());

        report
    }

    // ───────────────────────────────────────────────────────────── reading

    /// When the newest observation of this cell was taken.
    fn observed_at_ms(&self, shard_id: u64, coordinate: Coordinate) -> Option<u64> {
        self.runtime
            .state()
            .observations()
            .filter(|observation| {
                observation.shard.id == shard_id && observation.coordinate == coordinate
            })
            .map(|observation| observation.timestamp_ms)
            .max()
    }

    fn publish_status(&self, at_ms: u64) {
        let fleet = self.runtime.fleet_view();
        let clock_ms = self.runtime.clock_ms();

        let backends = self
            .settings
            .backends
            .iter()
            .map(|backend| {
                let node = self.runtime.nodes().get(&backend.id);

                let health = node
                    .map(|node| node.health(clock_ms, self.settings.silence_budget_ms))
                    .unwrap_or(NodeHealth::Unreachable);

                let availability = self
                    .runtime
                    .placement()
                    .borrow()
                    .routing()
                    .node(&backend.id)
                    .map(|entry| availability_label(entry.availability))
                    .unwrap_or("unknown");

                let (last_probe, last_probe_at_ms) = self
                    .probes
                    .get(&backend.id.0)
                    .map(|(verdict, at)| (verdict.clone(), Some(*at)))
                    .unwrap_or_else(|| ("not probed yet".to_string(), None));

                BackendStatus {
                    id: backend.id.0.clone(),
                    url: backend.url.clone(),
                    region: backend.region.clone(),
                    availability: availability.to_string(),
                    health: health.label().to_string(),
                    last_probe,
                    last_probe_at_ms,
                    silence_ms: node.and_then(|node| node.silence_ms(clock_ms)),
                    heartbeats: node.map(|node| node.heartbeats).unwrap_or(0),
                    telemetry: backend.token.is_some(),
                    last_sample: self.samples.get(&backend.id.0).cloned(),
                }
            })
            .collect();

        let placements = {
            let fabric = self.runtime.placement().borrow();

            let mut placements: Vec<PlacementStatus> = self
                .runtime
                .topology()
                .placements()
                .map(|placement| {
                    let target = fabric_controller::ActionTarget::new(
                        placement.shard_id,
                        placement.coordinate,
                    );

                    PlacementStatus {
                        shard: placement.shard_id,
                        x: placement.coordinate.x,
                        y: placement.coordinate.y,
                        holder: placement.dbms_id.0.clone(),
                        region: placement.region.clone(),
                        migration: fabric.migration(target).map(|migration| MigrationStatus {
                            phase: migration.phase().label().to_string(),
                            source: migration.plan().source.0.clone(),
                            destination: migration.plan().destination.0.clone(),
                            read_owner: migration.read_owner().0.clone(),
                            write_fenced: migration.is_write_fenced(),
                            has_cut_over: migration.has_cut_over(),
                        }),
                    }
                })
                .collect();

            placements.sort_by_key(|placement| {
                (placement.shard, placement.y, placement.x)
            });

            placements
        };

        let in_flight = {
            let fabric = self.runtime.placement().borrow();

            self.runtime
                .controller()
                .in_flight()
                .map(|record| {
                    let target = record.target();

                    let (phase, fraction) = match record.state() {
                        ExecutionState::Running { phase, fraction } => {
                            (Some(phase_label(*phase).to_string()), Some(*fraction))
                        }
                        _ => (None, None),
                    };

                    let destination = destination_of(record);

                    let transfer = destination
                        .as_ref()
                        .and_then(|node| fabric.transfer(target, node));

                    ActionStatus {
                        id: record.id().0,
                        shard: target.shard_id,
                        x: target.coordinate.x,
                        y: target.coordinate.y,
                        action: record.envelope().action_label().to_string(),
                        mechanism: record.mechanism().to_string(),
                        state: record.state().label().to_string(),
                        phase,
                        fraction,
                        source: record.source().0.clone(),
                        destination: destination_of(record).map(|node| node.0),
                        admitted_at_ms: record.admitted_at_ms(),
                        has_cut_over: record.has_cut_over(),
                        bytes_copied: transfer.map(|progress| progress.bytes_copied).unwrap_or(0),
                        resident_bytes: fabric.size(target),
                    }
                })
                .collect()
            };

        /*
         * The tail, not the archive. The controller keeps every record it ever
         * admitted, because history is the input to the learning loop; this is
         * a status page, rebuilt every cycle, and carrying the whole of that
         * history through it would make the snapshot grow with the daemon's
         * uptime for the benefit of nobody reading it.
         */
        let mut history: Vec<OutcomeReport> = self.runtime.controller().reports();

        if history.len() > PUBLISHED_HISTORY {
            history.drain(..history.len() - PUBLISHED_HISTORY);
        }

        let keyspace = KeyspaceStatus {
            rules: self
                .settings
                .keyspace
                .rules()
                .iter()
                .map(|rule| {
                    format!(
                        "{} / {}* -> {}",
                        rule.kind(),
                        rule.address_prefix(),
                        rule.key()
                    )
                })
                .collect(),
            fallback: self
                .settings
                .keyspace
                .fallback()
                .map(|key| key.to_string()),
            spans_one_place: self.settings.keyspace.spanning_key().is_some(),
        };

        self.status.publish(Status {
            version: env!("CARGO_PKG_VERSION"),
            started_at_ms: self.started_at_ms,
            snapshot_at_ms: at_ms,
            clock_ms,
            cycles: self.cycles,
            draining: self.draining,
            data_listen: self.settings.data_listen.to_string(),
            admin_listen: self.settings.admin_listen.to_string(),
            routing_generation: self.door.generation(),
            placement_generation: fleet.generation(),
            telemetry_source: self.telemetry.describe(),
            observations: self.runtime.state().len(),
            profiles: self.runtime.analyzer().len(),
            hot_cells: self.runtime.analyzer().hot_coordinates().len(),
            keyspace,
            backends,
            placements,
            in_flight,
            decisions: self.decisions.clone(),
            movers: MoverStatus {
                copying: self
                    .movers
                    .active()
                    .into_iter()
                    .map(|(id, destination)| CopyingStatus { id, destination })
                    .collect(),
                refused: self
                    .movers
                    .refusals()
                    .into_iter()
                    .map(|refusal| RefusedCopy {
                        id: refusal.id,
                        reason: refusal.reason,
                    })
                    .collect(),
            },
            placement_store: StoreStatus {
                configured: self
                    .settings
                    .placement_store
                    .as_ref()
                    .map(|id| id.0.clone()),
                state: self.store_state.clone(),
                last_error: self.store_error.clone(),
                writes: self.store_writes,
            },
            history,
        });
    }
}

/// One action the daemon could not leave in a state it is willing to vouch for.
fn abandoned(record: &fabric_controller::ExecutionRecord) -> Abandoned {
    Abandoned {
        id: record.id().0,
        target: record.target().to_string(),
        source: record.source().0.clone(),
        destination: destination_of(record).map(|node| node.0),
        state: record.state().label().to_string(),
        migration_phase: None,
    }
}

/// The node a plan is copying to.
///
/// Read off the *plan*, not the envelope. `Isolate` names no destination — it
/// says "get this workload off the node it is contending on" and leaves the
/// choice to the mechanism, which makes it with the same spread rules every
/// other copy is placed by — so the controller never learned which node that
/// was and `ExecutionRecord::destination` is `None` for it. The plan's `adds`
/// is where the answer actually lives, for every action alike.
fn destination_of(record: &fabric_controller::ExecutionRecord) -> Option<DbmsId> {
    record
        .plan()
        .adds
        .first()
        .cloned()
        .or_else(|| record.destination().cloned())
}

fn availability_label(availability: NodeAvailability) -> &'static str {
    match availability {
        NodeAvailability::Serviceable => "serviceable",
        NodeAvailability::Unreachable => "unreachable",
        NodeAvailability::Unknown => "unknown",
    }
}

fn phase_label(phase: PlanPhase) -> &'static str {
    phase.label()
}

fn placement_label(placement: &fabric_topology::Placement) -> String {
    format!(
        "shard {} ({},{}) on '{}'",
        placement.shard_id,
        placement.coordinate.x,
        placement.coordinate.y,
        placement.dbms_id.0
    )
}
