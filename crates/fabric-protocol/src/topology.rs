use serde::{Deserialize, Serialize};

use fabric_core::{Coordinate, DbmsId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyReport {
    pub node_id: DbmsId,
    pub timestamp_ms: u64,
    pub placements: Vec<Placement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Placement {
    pub coordinate: Coordinate,
    pub dbms_id: DbmsId,
    pub region: String,
}