//! The controller's read-only picture of the fleet at the instant it decides.
//!
//! The controller does not observe heartbeats and does not own the node
//! inventory -- `fabric-runtime` does, and it assembles this view from its
//! `NodeRegistry` plus the latest telemetry. Keeping the view an input rather
//! than internal state is what lets the same controller be driven against a
//! live fleet, a replayed session, or `fabric-simulator` without changing a
//! line of it.

use std::collections::BTreeMap;

use fabric_core::DbmsId;
use fabric_topology::TopologyRegistry;
use serde::{Deserialize, Serialize};

/// A node's fitness to receive work, as of the view's instant.
///
/// This is the projection of `fabric_runtime::NodeHealth` the controller acts
/// on, plus [`Draining`](NodeCondition::Draining), which liveness alone cannot
/// express: a node can be perfectly healthy and still be one an operator is
/// emptying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCondition {
    /// Reachable and reporting itself healthy.
    Healthy,

    /// Reachable, but reporting a problem with itself.
    Degraded,

    /// Not heard from inside the liveness deadline.
    Unreachable,

    /// Healthy, but being emptied. It may still be a source; it may never be
    /// a destination.
    Draining,
}

impl NodeCondition {
    /// Whether this node may be given data it does not already hold.
    pub fn accepts_placement(self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unreachable => "unreachable",
            Self::Draining => "draining",
        }
    }
}

/// One node as the controller sees it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: DbmsId,
    pub region: String,
    pub condition: NodeCondition,

    /// Placements the node currently holds.
    pub hosted_placements: usize,

    /// Placements the node is provisioned to hold.
    pub placement_capacity: usize,

    pub cpu_utilization: f64,
    pub memory_utilization: f64,
}

impl NodeStatus {
    /// Every field is required: there is no safe default for "how loaded is
    /// this machine", and a default would be a guess the controller would
    /// then act on.
    pub fn new(
        id: DbmsId,
        region: impl Into<String>,
        condition: NodeCondition,
        placement_capacity: usize,
    ) -> Self {
        Self {
            id,
            region: region.into(),
            condition,
            hosted_placements: 0,
            placement_capacity,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
        }
    }

    pub fn with_load(
        mut self,
        hosted_placements: usize,
        cpu_utilization: f64,
        memory_utilization: f64,
    ) -> Self {
        self.hosted_placements = hosted_placements;
        self.cpu_utilization = cpu_utilization;
        self.memory_utilization = memory_utilization;
        self
    }

    /// The binding resource pressure: a node is as loaded as its worst axis.
    pub fn utilization(&self) -> f64 {
        self.cpu_utilization.max(self.memory_utilization)
    }

    pub fn headroom(&self) -> usize {
        self.placement_capacity
            .saturating_sub(self.hosted_placements)
    }

    pub fn is_full(&self) -> bool {
        self.hosted_placements >= self.placement_capacity
    }
}

/// The fleet at one generation.
///
/// `generation` increments whenever the placement map changes. A decision
/// carries the generation it was computed against, which is how the controller
/// detects that the world moved underneath a proposal (see
/// [`ValidationError::TopologySuperseded`](crate::ValidationError::TopologySuperseded)).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FleetView {
    generation: u64,
    nodes: BTreeMap<String, NodeStatus>,
}

impl FleetView {
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            nodes: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, status: NodeStatus) {
        self.nodes.insert(status.id.0.clone(), status);
    }

    pub fn get(&self, id: &DbmsId) -> Option<&NodeStatus> {
        self.nodes.get(&id.0)
    }

    /// Every node, in `DbmsId` order.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeStatus> {
        self.nodes.values()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// Everything the controller is allowed to consult while deciding, bundled so
/// that a validation and the execution step that follows it are answering to
/// the *same* instant. Passing topology and fleet separately invites a caller
/// to mix a fresh fleet with a stale map.
#[derive(Debug, Clone, Copy)]
pub struct ControlPlaneView<'a> {
    pub topology: &'a TopologyRegistry,
    pub fleet: &'a FleetView,

    /// The control plane's clock, in epoch milliseconds. Message time, not
    /// wall time -- the same convention `fabric_runtime::FabricRuntime` uses,
    /// so that a replayed or simulated run is judged against its own clock.
    pub now_ms: u64,
}

impl<'a> ControlPlaneView<'a> {
    pub fn new(
        topology: &'a TopologyRegistry,
        fleet: &'a FleetView,
        now_ms: u64,
    ) -> Self {
        Self {
            topology,
            fleet,
            now_ms,
        }
    }
}
