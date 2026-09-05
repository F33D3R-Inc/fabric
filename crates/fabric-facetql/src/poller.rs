//! The live telemetry source: poll `GET /stats` on an interval, difference
//! consecutive samples, feed the existing pipeline.
//!
//! This replaces `fabric-cli`'s file replay as the way observations enter the
//! runtime. It deliberately produces the **same** [`FabricMessage`] values the
//! replay path produces, so there is one ingestion path and one scoring path:
//! `FabricRuntime::handle` → `WorkloadAnalyzer` → `WorkloadProfile` →
//! `WorkloadOptimizer` → `fabric_ml::WorkloadPredictor`. Nothing here scores,
//! ranks or decides anything; a second scoring path would be a second set of
//! numbers to reconcile.
//!
//! # Why Fabric polls rather than FacetQL pushing
//!
//! Polling keeps FacetQL completely unaware of Fabric: no fabric client, no
//! fabric address, no coupling in the repo that is the current priority. The
//! dependency arrow points from the lowest-priority layer to the highest, and
//! push can be added later without changing the FacetQL primitive.
//!
//! # What a poll target is
//!
//! One FacetQL instance is one [`fabric_core::DbmsNode`], and — even now that
//! FacetQL attributes its traffic to its own per-node coordinates (see
//! `cells` in `GET /stats`) — the whole *instance* remains the placeable unit
//! (FABRIC_INTEGRATION_PLAN Finding A). FacetQL's per-cell breakdown is an
//! orthogonal, FacetQL-native axis system (see `fabric_facetql::wire`'s
//! module docs), not a Fabric grid cell, and this crate never treats it as
//! one: it is surfaced as intra-instance detail on the [`WorkloadProfile`] the
//! instance already produces, not as additional locations the optimizer could
//! place work on. A [`PollTarget`] therefore still pairs the instance's
//! credentials with the single *placement* it occupies in Fabric's grid: the
//! operator declares that, or it is read back from the durable placement
//! store.
//!
//! [`WorkloadProfile`]: fabric_workload::WorkloadProfile
//!
//! # Failing closed
//!
//! An instance that cannot be reached, or cannot be authenticated to, is
//! reported as an **unhealthy heartbeat** — never skipped, and never left
//! looking alive because the last successful poll is still the newest thing
//! the registry saw. It also emits no telemetry: no data is the truth, and
//! zeros would read to the optimizer as a quiet, healthy node.

use std::time::Duration;

use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_protocol::{
    FabricMessage, NodeHeartbeat, NodeRegistration, TelemetryBatch, TelemetrySample,
};
use fabric_runtime::FabricRuntime;
use fabric_topology::Placement;

use crate::client::FacetqlClient;
use crate::endpoint::FacetqlEndpoint;
use crate::error::FacetqlError;
use crate::sample::{now_ms, ratios, StatsSample};

/// Software version announced for an instance that has not answered a
/// `GET /stats` yet.
///
/// FacetQL now reports its own build as `stats.version` (an addition to the
/// wire contract — see `crate::wire::EngineStats::version`), so a target's
/// *real* version is known as of its first successful poll and is
/// re-announced to the registry then (see [`TelemetryPoller::poll_into`]).
/// This placeholder covers only the window before that: an instance this
/// process has registered but never yet successfully asked, or one running
/// an older FacetQL that does not report a version at all.
const UNKNOWN_VERSION: &str = "unknown";

/// One FacetQL instance and the placement it occupies.
#[derive(Debug, Clone)]
pub struct PollTarget {
    pub endpoint: FacetqlEndpoint,
    pub shard_id: u64,
    /// Fabric's placement-grid cell — [`fabric_core::Coordinate`], **not**
    /// FacetQL's 4-axis node coordinate.
    pub coordinate: Coordinate,
    pub region: String,
}

impl PollTarget {
    pub fn new(
        endpoint: FacetqlEndpoint,
        shard_id: u64,
        coordinate: Coordinate,
        region: impl Into<String>,
    ) -> Self {
        Self {
            endpoint,
            shard_id,
            coordinate,
            region: region.into(),
        }
    }

    /// Build a target from a durable placement plus that instance's
    /// credentials, so the poller samples exactly what the topology says is
    /// out there.
    ///
    /// Refuses a credential that belongs to a different instance than the
    /// placement names — pointing a placement at another node's token is a
    /// configuration error that would otherwise attribute one instance's
    /// traffic to another's grid cell.
    pub fn from_placement(
        placement: &Placement,
        endpoint: FacetqlEndpoint,
    ) -> Result<Self, FacetqlError> {
        if endpoint.dbms_id() != &placement.dbms_id {
            return Err(FacetqlError::Configuration(format!(
                "placement names DBMS '{}' but the endpoint is for '{}'",
                placement.dbms_id.0,
                endpoint.dbms_id().0
            )));
        }

        Ok(Self::new(
            endpoint,
            placement.shard_id,
            placement.coordinate,
            placement.region.clone(),
        ))
    }

    pub fn dbms_id(&self) -> &DbmsId {
        self.endpoint.dbms_id()
    }
}

/// What one poll of one instance produced.
#[derive(Debug)]
pub enum PollOutcome {
    /// Two samples were differenced into an interval of real traffic.
    Sampled(TelemetryBatch),

    /// The first successful sample of this instance. There is nothing to
    /// difference it against yet, so it becomes the baseline and no telemetry
    /// is emitted. The instance is alive.
    Baseline,

    /// A successful sample that spanned a restart or a zero-length interval.
    /// The instance is alive; the interval says nothing, so nothing is
    /// reported for it.
    Skipped,

    /// The instance did not answer, or refused the token. It is marked
    /// unhealthy and emits no telemetry.
    Failed(FacetqlError),
}

impl PollOutcome {
    /// Whether this poll is evidence the instance is serviceable.
    pub fn is_healthy(&self) -> bool {
        !matches!(self, Self::Failed(_))
    }
}

/// One instance's polling state: the client and the previous sample.
struct TargetState {
    target: PollTarget,
    client: FacetqlClient,
    previous: Option<StatsSample>,

    /// The instance's own build, as of the most recent successful poll.
    /// `None` until the first one succeeds, or against an older FacetQL that
    /// does not report it at all.
    version: Option<String>,
    /// The version most recently announced to the runtime's registry, so a
    /// newly-learned or changed version (a rolling upgrade mid-fleet) is
    /// re-announced exactly once rather than on every poll.
    announced_version: Option<String>,
}

/// Samples a set of FacetQL instances and turns each interval into protocol
/// messages the runtime already knows how to ingest.
pub struct TelemetryPoller {
    targets: Vec<TargetState>,
}

impl TelemetryPoller {
    /// Build a poller with a default timeout on every request.
    ///
    /// A timeout is not optional here: a sweep of the fleet is sequential, so
    /// a single instance that accepts a connection and then never answers
    /// would hold up the observation of every other instance — and a control
    /// plane blocked on a wedged node is a control plane that cannot react to
    /// the wedged node.
    pub fn new(targets: Vec<PollTarget>) -> Result<Self, FacetqlError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| {
                FacetqlError::Configuration(format!("could not build HTTP client: {error}"))
            })?;

        Ok(Self::with_http(targets, http))
    }

    /// Build a poller over a caller-supplied reqwest client.
    pub fn with_http(targets: Vec<PollTarget>, http: reqwest::Client) -> Self {
        let targets = targets
            .into_iter()
            .map(|target| TargetState {
                client: FacetqlClient::with_http(target.endpoint.clone(), http.clone()),
                target,
                previous: None,
                version: None,
                announced_version: None,
            })
            .collect();

        Self { targets }
    }

    pub fn targets(&self) -> impl Iterator<Item = &PollTarget> {
        self.targets.iter().map(|state| &state.target)
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Announce every target to the runtime's node registry.
    ///
    /// An operator-configured poll target *is* an inventory entry — it is the
    /// list of instances this control plane is responsible for. Registration
    /// alone does not make any of them healthy: the registry holds a
    /// registered node as unreachable until a heartbeat proves otherwise, and
    /// the first heartbeat comes from the first successful poll.
    pub fn register_targets(&self, runtime: &mut FabricRuntime) {
        for state in &self.targets {
            runtime.handle(FabricMessage::RegisterNode(NodeRegistration::new(
                state.target.dbms_id().clone(),
                UNKNOWN_VERSION,
                state.target.region.clone(),
            )));
        }
    }

    /// Poll every target once.
    ///
    /// Returns one outcome per target, in configuration order. Sampling is
    /// sequential and bounded by the client's timeout: a fleet is a handful of
    /// instances, and a sequential sweep keeps the samples' spacing even,
    /// which is what a rate derived from them depends on.
    pub async fn poll_once(&mut self) -> Vec<(DbmsId, PollOutcome)> {
        let mut outcomes = Vec::with_capacity(self.targets.len());

        for state in &mut self.targets {
            let outcome = Self::poll_target(state).await;
            outcomes.push((state.target.dbms_id().clone(), outcome));
        }

        outcomes
    }

    /// Poll every target once and feed the result into the runtime.
    ///
    /// Each target produces a heartbeat — healthy when it answered, unhealthy
    /// when it did not — and, when an interval was measurable, a telemetry
    /// batch. Both are ordinary [`FabricMessage`] values, so this is the same
    /// door the replayed session comes through.
    pub async fn poll_into(&mut self, runtime: &mut FabricRuntime) -> Vec<(DbmsId, PollOutcome)> {
        let outcomes = self.poll_once().await;
        let timestamp_ms = now_ms();

        for (state, (dbms_id, outcome)) in self.targets.iter_mut().zip(&outcomes) {
            // A version learned for the first time, or changed since the
            // last announcement (a rolling upgrade mid-fleet), is
            // re-registered before this round's heartbeat — so the registry
            // is never staler than the instance's own last successful
            // answer. `register` (see `fabric_runtime::registry`) already
            // treats a re-registration as an ordinary update, exactly like a
            // restarted instance re-announcing itself.
            if state.version.is_some() && state.version != state.announced_version {
                runtime.handle(FabricMessage::RegisterNode(NodeRegistration::new(
                    state.target.dbms_id().clone(),
                    state
                        .version
                        .clone()
                        .unwrap_or_else(|| UNKNOWN_VERSION.to_string()),
                    state.target.region.clone(),
                )));
                state.announced_version = state.version.clone();
            }

            runtime.handle(FabricMessage::Heartbeat(NodeHeartbeat {
                node_id: dbms_id.clone(),
                timestamp_ms,
                healthy: outcome.is_healthy(),
            }));

            if let PollOutcome::Sampled(batch) = outcome {
                runtime.handle(FabricMessage::Telemetry(batch.clone()));
            }
        }

        outcomes
    }

    /// Poll on `interval` until `rounds` have completed, feeding the runtime.
    ///
    /// The first round can only ever establish baselines, so `rounds` of 1
    /// yields no telemetry by construction — that is the shape of a
    /// difference, not a bug.
    pub async fn run(
        &mut self,
        runtime: &mut FabricRuntime,
        interval: Duration,
        rounds: usize,
    ) -> Vec<(DbmsId, PollOutcome)> {
        let mut last = Vec::new();

        for round in 0..rounds {
            if round > 0 {
                tokio::time::sleep(interval).await;
            }

            last = self.poll_into(runtime).await;
        }

        last
    }

    async fn poll_target(state: &mut TargetState) -> PollOutcome {
        let stats = match state.client.stats().await {
            Ok(stats) => stats,
            Err(error) => return PollOutcome::Failed(error),
        };

        // Recorded on every successful read, independent of baseline/
        // sampled/skipped: the version is a fact about the instance, not
        // about whether an interval could be derived from it.
        state.version = stats.version.clone();

        let sample = StatsSample::take(&stats);
        let previous = state.previous.replace(sample.clone());

        let Some(previous) = previous else {
            return PollOutcome::Baseline;
        };

        let Some(metrics) = previous.difference(&sample) else {
            return PollOutcome::Skipped;
        };

        let (read_ratio, write_ratio) = ratios(&metrics);

        PollOutcome::Sampled(TelemetryBatch {
            timestamp_ms: sample.taken_at_ms,
            node_id: state.target.dbms_id().clone(),
            shard: Shard::new(state.target.shard_id, state.target.region.clone()),
            samples: vec![TelemetrySample {
                coordinate: state.target.coordinate,
                operations_per_second: metrics.operations_per_second,
                read_ratio,
                write_ratio,

                // Unmeasured by `/stats`; carried through as absent rather
                // than as invented values. See `crate::sample`.
                read_latency_us: metrics.read_latency_us,
                write_latency_us: metrics.write_latency_us,
                cpu_utilization: metrics.cpu_utilization,
                memory_utilization: metrics.memory_utilization,
                queue_depth: metrics.queue_depth,

                cell_breakdown: metrics.cell_breakdown,
                cell_breakdown_partial: metrics.cell_breakdown_partial,
            }],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sample::StatsSample;
    use std::time::Instant;

    fn endpoint(id: &str) -> FacetqlEndpoint {
        FacetqlEndpoint::new(DbmsId::new(id), "http://127.0.0.1:8892", "token").unwrap()
    }

    fn placement(id: &str) -> Placement {
        Placement {
            dbms_id: DbmsId::new(id),
            shard_id: 7,
            coordinate: Coordinate::new(1, 2),
            region: "us-east".to_string(),
        }
    }

    #[test]
    fn a_target_built_from_a_placement_inherits_its_cell() {
        let target = PollTarget::from_placement(&placement("db-a"), endpoint("db-a")).unwrap();
        assert_eq!(target.shard_id, 7);
        assert_eq!(target.coordinate, Coordinate::new(1, 2));
        assert_eq!(target.region, "us-east");
    }

    #[test]
    fn a_credential_for_another_instance_is_refused() {
        let err = PollTarget::from_placement(&placement("db-a"), endpoint("db-b")).unwrap_err();
        assert!(matches!(err, FacetqlError::Configuration(_)));
        assert!(err.to_string().contains("db-b"));
    }

    /// The differenced metrics and the protocol sample the poller emits must
    /// describe the same traffic: `TelemetrySample::metrics()` is what the
    /// runtime actually ingests, so if it did not round-trip, the numbers the
    /// optimizer sees would not be the numbers that were measured.
    #[test]
    fn the_emitted_sample_round_trips_back_to_the_measured_metrics() {
        let start = Instant::now();
        let first = StatsSample {
            taken_at_ms: 0,
            taken_at: start,
            reads_total: 100,
            writes_total: 20,
            node_count: 0,
            edge_count: 0,
            cpu_seconds_total: None,
            cpu_cores: None,
            memory_utilization: None,
            read_latency_p99_us: None,
            write_latency_p99_us: None,
            in_flight: 0,
            max_concurrent: 0,
            cells: Vec::new(),
            overflow_reads: 0,
            overflow_writes: 0,
        };
        let second = StatsSample {
            taken_at_ms: 1_000,
            taken_at: start + Duration::from_secs(1),
            reads_total: 190,
            writes_total: 30,
            node_count: 0,
            edge_count: 0,
            cpu_seconds_total: None,
            cpu_cores: None,
            memory_utilization: None,
            read_latency_p99_us: None,
            write_latency_p99_us: None,
            in_flight: 0,
            max_concurrent: 0,
            cells: Vec::new(),
            overflow_reads: 0,
            overflow_writes: 0,
        };

        let metrics = first.difference(&second).unwrap();
        let (read_ratio, write_ratio) = ratios(&metrics);

        let sample = TelemetrySample {
            coordinate: Coordinate::new(0, 0),
            operations_per_second: metrics.operations_per_second,
            read_ratio,
            write_ratio,
            read_latency_us: metrics.read_latency_us,
            write_latency_us: metrics.write_latency_us,
            cpu_utilization: metrics.cpu_utilization,
            memory_utilization: metrics.memory_utilization,
            queue_depth: metrics.queue_depth,
            cell_breakdown: metrics.cell_breakdown.clone(),
            cell_breakdown_partial: metrics.cell_breakdown_partial,
        };

        let round_tripped = sample.metrics();
        assert!((round_tripped.reads_per_second - 90.0).abs() < 1e-9);
        assert!((round_tripped.writes_per_second - 10.0).abs() < 1e-9);
        assert!(
            (round_tripped.operations_per_second - metrics.operations_per_second).abs() < 1e-9
        );
    }

    #[test]
    fn a_failed_poll_is_not_healthy() {
        let failed = PollOutcome::Failed(FacetqlError::Transport("refused".into()));
        assert!(!failed.is_healthy());
        assert!(PollOutcome::Baseline.is_healthy());
        assert!(PollOutcome::Skipped.is_healthy());
    }

    /// An unreachable instance must reach the registry as an unhealthy
    /// heartbeat, not as silence that the last good poll keeps papering over.
    #[tokio::test]
    async fn an_unreachable_instance_is_marked_unhealthy_not_left_alone() {
        // Port 1 on loopback: nothing listens there, so the connection is
        // refused immediately without a timeout wait.
        let target = PollTarget::new(
            FacetqlEndpoint::new(DbmsId::new("db-dead"), "http://127.0.0.1:1", "token")
                .unwrap(),
            0,
            Coordinate::new(0, 0),
            "us-east",
        );

        let mut poller = TelemetryPoller::new(vec![target]).unwrap();
        let mut runtime = FabricRuntime::new();
        poller.register_targets(&mut runtime);

        let outcomes = poller.poll_into(&mut runtime).await;
        assert_eq!(outcomes.len(), 1);
        assert!(!outcomes[0].1.is_healthy());

        let node = runtime
            .nodes()
            .get(&DbmsId::new("db-dead"))
            .expect("the target was registered");
        assert!(!node.reported_healthy);
        assert_eq!(
            node.health(runtime.clock_ms(), fabric_runtime::DEFAULT_HEARTBEAT_DEADLINE_MS),
            fabric_runtime::NodeHealth::Degraded
        );

        // No telemetry was invented for an instance that never answered.
        assert_eq!(runtime.analyzer().len(), 0);
        assert_eq!(runtime.observations().len(), 0);
    }
}
