use fabric_core::Coordinate;
use fabric_telemetry::WorkloadMetrics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PressureLevel {
    Normal,
    Elevated,
    High,
    Critical,
}

impl PressureLevel {
    pub fn from_score(score: f64) -> Self {
        match score {
            s if s >= 0.90 => Self::Critical,
            s if s >= 0.75 => Self::High,
            s if s >= 0.50 => Self::Elevated,
            _ => Self::Normal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadProfile {
    pub coordinate: Coordinate,

    pub operations_per_second: f64,
    pub read_ratio: f64,
    pub write_ratio: f64,

    pub read_latency_us: f64,
    pub write_latency_us: f64,

    pub cpu_utilization: f64,
    pub memory_utilization: f64,

    pub storage_bytes_per_second: u64,
    pub network_in_bytes_per_second: u64,
    pub network_out_bytes_per_second: u64,

    pub queue_depth: u64,

    pub pressure_score: f64,
    pub pressure: PressureLevel,
}

impl WorkloadProfile {
    pub fn from_metrics(
        coordinate: Coordinate,
        metrics: WorkloadMetrics,
    ) -> Self {
        let total = metrics.total_operations();

        let read_ratio = if total > 0.0 {
            metrics.reads_per_second / total
        } else {
            0.0
        };

        let write_ratio = if total > 0.0 {
            metrics.writes_per_second / total
        } else {
            0.0
        };

        let pressure_score = calculate_pressure(&metrics);

        Self {
            coordinate,

            operations_per_second: total,
            read_ratio,
            write_ratio,

            read_latency_us: metrics.read_latency_us,
            write_latency_us: metrics.write_latency_us,

            cpu_utilization: metrics.cpu_utilization,
            memory_utilization: metrics.memory_utilization,

            storage_bytes_per_second: metrics.storage_bytes_per_second,
            network_in_bytes_per_second: metrics.network_in_bytes_per_second,
            network_out_bytes_per_second: metrics.network_out_bytes_per_second,

            queue_depth: metrics.queue_depth,

            pressure_score,
            pressure: PressureLevel::from_score(pressure_score),
        }
    }

    pub fn is_hot(&self) -> bool {
        matches!(
            self.pressure,
            PressureLevel::High | PressureLevel::Critical
        )
    }
}

fn calculate_pressure(metrics: &WorkloadMetrics) -> f64 {
    let cpu = metrics.cpu_utilization.clamp(0.0, 1.0);
    let memory = metrics.memory_utilization.clamp(0.0, 1.0);

    let queue = (metrics.queue_depth as f64 / 10_000.0)
        .clamp(0.0, 1.0);

    let latency = (
        metrics.read_latency_us.max(metrics.write_latency_us)
            / 100_000.0
    )
        .clamp(0.0, 1.0);

    (cpu * 0.35)
        + (memory * 0.20)
        + (queue * 0.25)
        + (latency * 0.20)
}