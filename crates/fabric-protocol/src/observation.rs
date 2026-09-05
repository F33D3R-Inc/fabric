use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_telemetry::{CellWorkloadMetrics, WorkloadMetrics};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryBatch {
    pub timestamp_ms: u64,
    pub node_id: DbmsId,
    pub shard: Shard,
    pub samples: Vec<TelemetrySample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySample {
    pub coordinate: Coordinate,
    pub operations_per_second: f64,
    pub read_ratio: f64,
    pub write_ratio: f64,
    pub read_latency_us: f64,
    pub write_latency_us: f64,
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub queue_depth: u64,

    /// The source's own per-coordinate attribution of the traffic above.
    /// Empty when the source reports none — an older sender, or a sender
    /// with no such concept. `#[serde(default)]` so an older sender's
    /// wire message (which predates this field) still deserializes.
    #[serde(default)]
    pub cell_breakdown: Vec<CellWorkloadMetrics>,

    /// Whether `cell_breakdown` is known to be a partial account. See
    /// [`fabric_telemetry::WorkloadMetrics::cell_breakdown_partial`].
    #[serde(default)]
    pub cell_breakdown_partial: bool,
}

impl TelemetrySample {
    pub fn metrics(&self) -> WorkloadMetrics {
        let total = self.operations_per_second;

        WorkloadMetrics {
            operations_per_second: total,
            reads_per_second: total * self.read_ratio,
            writes_per_second: total * self.write_ratio,
            read_latency_us: self.read_latency_us,
            write_latency_us: self.write_latency_us,
            cpu_utilization: self.cpu_utilization,
            memory_utilization: self.memory_utilization,
            storage_bytes_per_second: 0,
            network_in_bytes_per_second: 0,
            network_out_bytes_per_second: 0,
            queue_depth: self.queue_depth,
            cell_breakdown: self.cell_breakdown.clone(),
            cell_breakdown_partial: self.cell_breakdown_partial,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadObservation {
    pub node_id: String,
    pub coordinate: Coordinate,
    pub timestamp_ms: u64,
    pub operations_per_second: f64,
    pub read_ratio: f64,
    pub write_ratio: f64,
}