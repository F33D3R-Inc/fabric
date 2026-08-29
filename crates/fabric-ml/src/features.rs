use fabric_core::Coordinate;
use fabric_workload::WorkloadProfile;
use serde::{Deserialize, Serialize};

/// Numerical feature vector presented to Fabric ML models.
///
/// This structure intentionally contains no natural-language data.
/// It is the numerical representation of workload behavior.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkloadFeatures {
    pub coordinate: Coordinate,

    pub operations_per_second: f64,
    pub read_ratio: f64,
    pub write_ratio: f64,

    pub read_latency_us: f64,
    pub write_latency_us: f64,

    pub cpu_utilization: f64,
    pub memory_utilization: f64,

    pub storage_bytes_per_second: f64,
    pub network_in_bytes_per_second: f64,
    pub network_out_bytes_per_second: f64,

    pub queue_depth: f64,
    pub pressure_score: f64,
}

impl From<&WorkloadProfile> for WorkloadFeatures {
    fn from(profile: &WorkloadProfile) -> Self {
        Self {
            coordinate: profile.coordinate,

            operations_per_second: profile.operations_per_second,
            read_ratio: profile.read_ratio,
            write_ratio: profile.write_ratio,

            read_latency_us: profile.read_latency_us,
            write_latency_us: profile.write_latency_us,

            cpu_utilization: profile.cpu_utilization,
            memory_utilization: profile.memory_utilization,

            storage_bytes_per_second:
            profile.storage_bytes_per_second as f64,

            network_in_bytes_per_second:
            profile.network_in_bytes_per_second as f64,

            network_out_bytes_per_second:
            profile.network_out_bytes_per_second as f64,

            queue_depth: profile.queue_depth as f64,
            pressure_score: profile.pressure_score,
        }
    }
}