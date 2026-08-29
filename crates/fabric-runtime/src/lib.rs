pub mod state;

pub use state::{
    FabricState,
    LocationKey,
};

use fabric_core::{Coordinate, Shard};

use fabric_optimizer::{
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

pub struct FabricRuntime {
    topology: TopologyRegistry,
    analyzer: WorkloadAnalyzer,
    optimizer: WorkloadOptimizer,
    observations: Vec<Observation>,
    state: FabricState,
}

impl Default for FabricRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl FabricRuntime {
    pub fn new() -> Self {
        Self {
            topology: TopologyRegistry::default(),
            analyzer: WorkloadAnalyzer::default(),
            optimizer: WorkloadOptimizer::default(),
            observations: Vec::new(),
            state: FabricState::new(),
        }
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub fn analyzer(&self) -> &WorkloadAnalyzer {
        &self.analyzer
    }

    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }

    pub fn state(&self) -> &FabricState {
        &self.state
    }

    pub fn handle(
        &mut self,
        message: FabricMessage,
    ) -> FabricResponse {
        match message {
            FabricMessage::RegisterNode(registration) => {
                FabricResponse::Registered {
                    node_id: registration.node_id.0,
                }
            }

            FabricMessage::Heartbeat(_) => {
                FabricResponse::Acknowledged
            }

            FabricMessage::Topology(report) => {
                self.ingest_topology(report);

                FabricResponse::Acknowledged
            }

            FabricMessage::Telemetry(batch) => {
                self.ingest_telemetry(batch);

                FabricResponse::Acknowledged
            }

            FabricMessage::Workload(observation) => {
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

            self.observations.push(observation);
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

        self.observations.push(observation);
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

    pub fn optimize(
        &self,
        profile: &WorkloadProfile,
    ) -> OptimizationDecision {
        self.optimizer.optimize(
            profile,
            &self.topology,
        )
    }
}