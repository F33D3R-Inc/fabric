use fabric_core::{Coordinate, Shard};
use serde::{Deserialize, Serialize};

use crate::metrics::WorkloadMetrics;

/// A timestamped observation of a particular location in the
/// FacetQL data space.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub timestamp_ms: u64,
    pub shard: Shard,
    pub coordinate: Coordinate,
    pub metrics: WorkloadMetrics,
}

impl Observation {
    pub fn new(
        timestamp_ms: u64,
        shard: Shard,
        coordinate: Coordinate,
        metrics: WorkloadMetrics,
    ) -> Self {
        Self {
            timestamp_ms,
            shard,
            coordinate,
            metrics,
        }
    }
}