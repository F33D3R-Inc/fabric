use serde::{Deserialize, Serialize};

use fabric_core::DbmsId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub protocol: String,
    pub node_id: DbmsId,
    pub software_version: String,
    pub region: String,
}

impl NodeRegistration {
    pub fn new(
        node_id: DbmsId,
        software_version: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            protocol: "facet/1".to_string(),
            node_id,
            software_version: software_version.into(),
            region: region.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node_id: DbmsId,
    pub timestamp_ms: u64,
    pub healthy: bool,
}