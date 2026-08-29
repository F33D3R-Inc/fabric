use serde::{Deserialize, Serialize};

/// Raw measurements describing the pressure placed on a workload.
///
/// These values are intentionally numerical. They are the raw material
/// that the workload analyzer and ML layer will consume.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct WorkloadMetrics {
    /// Operations per second.
    pub operations_per_second: f64,

    /// Read operations per second.
    pub reads_per_second: f64,

    /// Write operations per second.
    pub writes_per_second: f64,

    /// Average read latency in microseconds.
    pub read_latency_us: f64,

    /// Average write latency in microseconds.
    pub write_latency_us: f64,

    /// CPU utilization from 0.0 to 1.0.
    pub cpu_utilization: f64,

    /// Memory utilization from 0.0 to 1.0.
    pub memory_utilization: f64,

    /// Storage I/O in bytes per second.
    pub storage_bytes_per_second: u64,

    /// Network ingress in bytes per second.
    pub network_in_bytes_per_second: u64,

    /// Network egress in bytes per second.
    pub network_out_bytes_per_second: u64,

    /// Number of queued operations.
    pub queue_depth: u64,
}

impl WorkloadMetrics {
    pub fn total_operations(&self) -> f64 {
        self.reads_per_second + self.writes_per_second
    }

    pub fn is_cpu_hot(&self) -> bool {
        self.cpu_utilization >= 0.80
    }

    pub fn is_memory_hot(&self) -> bool {
        self.memory_utilization >= 0.80
    }

    pub fn is_queue_hot(&self) -> bool {
        self.queue_depth >= 10_000
    }

    pub fn is_hot(&self) -> bool {
        self.is_cpu_hot() || self.is_memory_hot() || self.is_queue_hot()
    }
}