use serde::{Deserialize, Serialize};

/// One coordinate's share of a workload's traffic, as differenced rates.
///
/// `x`/`y`/`z`/`q` are the **source's own** coordinate space — FacetQL's
/// 4-axis per-node tag, at present — and are unrelated to
/// `fabric_core::Coordinate`, Fabric's placement grid. FacetQL's per-cell
/// attribution exists so a workload profile can say how one instance's
/// traffic is distributed *inside itself*; it does not name a Fabric grid
/// cell, and this type deliberately does not pretend otherwise. See
/// `fabric_facetql::wire`'s module docs for the two `Coordinate` types this
/// echoes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct CellWorkloadMetrics {
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,

    pub reads_per_second: f64,
    pub writes_per_second: f64,
    pub bytes_read_per_second: f64,
    pub bytes_written_per_second: f64,
}

/// Raw measurements describing the pressure placed on a workload.
///
/// These values are intentionally numerical. They are the raw material
/// that the workload analyzer and ML layer will consume.
///
/// Not `Copy`: `cell_breakdown` is a `Vec`, so a caller that needs a second
/// copy now clones explicitly rather than getting one implicitly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

    /// The traffic above, broken down by the source's own per-coordinate
    /// attribution — empty when the source cannot or does not report one
    /// (an older FacetQL, or a source with no such concept at all).
    pub cell_breakdown: Vec<CellWorkloadMetrics>,

    /// Whether `cell_breakdown` is known to be **incomplete**: the source's
    /// own attribution table overflowed, so some of the reads/writes above
    /// are real but are not represented in any entry of `cell_breakdown`.
    ///
    /// `false` does not prove completeness — a source that reports no
    /// breakdown at all also reports `false` here, since there is nothing to
    /// call incomplete. `true` is the hard guarantee: comparing entries of
    /// `cell_breakdown` against each other is comparing partial data, and a
    /// consumer must not silently trust it as the whole picture.
    pub cell_breakdown_partial: bool,
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