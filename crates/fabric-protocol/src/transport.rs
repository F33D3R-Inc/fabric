use std::net::SocketAddr;

use crate::{
    FabricMessage,
    FabricResponse,
};

#[derive(Debug)]
pub enum ProtocolError {
    InvalidMessage(String),
    Serialization(String),
    Io(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::InvalidMessage(message) => {
                write!(f, "invalid message: {message}")
            }

            Self::Serialization(message) => {
                write!(f, "serialization error: {message}")
            }

            Self::Io(message) => {
                write!(f, "I/O error: {message}")
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone)]
pub struct ProtocolServer {
    address: SocketAddr,
}

impl ProtocolServer {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn decode(
        payload: &[u8],
    ) -> Result<FabricMessage, ProtocolError> {
        serde_json::from_slice(payload)
            .map_err(|error| {
                ProtocolError::Serialization(error.to_string())
            })
    }

    pub fn encode(
        response: &FabricResponse,
    ) -> Result<Vec<u8>, ProtocolError> {
        serde_json::to_vec(response)
            .map_err(|error| {
                ProtocolError::Serialization(error.to_string())
            })
    }

    pub fn handle(
        &self,
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

            FabricMessage::Topology(_) => {
                FabricResponse::Acknowledged
            }

            FabricMessage::Telemetry(_) => {
                FabricResponse::Acknowledged
            }

            FabricMessage::Workload(_) => {
                FabricResponse::Acknowledged
            }
        }
    }
}