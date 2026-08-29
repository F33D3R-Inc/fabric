use serde::{Deserialize, Serialize};

use crate::{
    NodeHeartbeat,
    NodeRegistration,
    TelemetryBatch,
    TopologyReport,
    WorkloadObservation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const V1: Self = Self {
        major: 1,
        minor: 0,
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FabricMessage {
    RegisterNode(NodeRegistration),
    Heartbeat(NodeHeartbeat),
    Topology(TopologyReport),
    Telemetry(TelemetryBatch),
    Workload(WorkloadObservation),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FabricResponse {
    Acknowledged,

    Registered {
        node_id: String,
    },

    OptimizationProposal {
        coordinate: String,
        action: String,
        expected_gain: f64,
        estimated_cost: f64,
        confidence: f64,
    },

    Rejected {
        reason: String,
    },
}