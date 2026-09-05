//! The synthetic cluster: nodes, cells, and the state a control action mutates.
//!
//! Everything here is expressed in the vocabulary that already exists --
//! `DbmsId`, `Shard`, `Coordinate`, `Workload`, `WorkloadClass` -- so that the
//! optimizer and the controller cannot tell they are being driven by a
//! simulation. That is the requirement: if the simulator needed its own
//! parallel types, anything proven against it would prove nothing about the
//! real control path.

use std::collections::{BTreeMap, BTreeSet};

use fabric_controller::{ActionTarget, FleetView, NodeCondition, NodeStatus};
use fabric_core::{Coordinate, DbmsId, Shard, Workload, WorkloadClass};
use fabric_topology::TopologyRegistry;
use serde::{Deserialize, Serialize};

use crate::rng::Rng;
use crate::workload::{ClassProfile, class_profile};

/// How to build a cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterSpec {
    /// The one input that decides the whole run.
    pub seed: u64,

    /// Simulated milliseconds per step.
    pub tick_ms: u64,

    /// The instant the run starts. Not "now" -- there is no now.
    pub start_ms: u64,

    pub regions: Vec<String>,
    pub nodes_per_region: usize,
    pub shards_per_node: usize,

    /// Cells populated per shard, taken from the grid in index order. Capped
    /// at the 156 a FacetQL grid actually has.
    pub cells_per_shard: usize,

    /// Placements a node is provisioned for.
    pub placement_capacity: usize,

    /// Operations per second a node can serve before it saturates.
    pub node_capacity_ops: f64,

    /// Classes to draw cell workloads from.
    pub classes: Vec<WorkloadClass>,

    /// Data-movement rate used to estimate how long a transfer takes.
    pub transfer_bytes_per_ms: u64,

    /// Period of the workload's daily shape.
    pub diurnal_period_ms: u64,
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            seed: 1,
            tick_ms: 1_000,
            start_ms: 1_700_000_000_000,
            regions: vec!["us-east".to_string(), "us-west".to_string()],
            nodes_per_region: 2,
            shards_per_node: 1,
            cells_per_shard: 6,
            placement_capacity: 12,
            node_capacity_ops: 4_000.0,
            classes: vec![
                WorkloadClass::ReadHeavy,
                WorkloadClass::WriteHeavy,
                WorkloadClass::Mixed,
                WorkloadClass::EventHeavy,
                WorkloadClass::Realtime,
            ],
            transfer_bytes_per_ms: 100_000,
            diurnal_period_ms: 600_000,
        }
    }
}

impl ClusterSpec {
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn node_count(&self) -> usize {
        self.regions.len() * self.nodes_per_region
    }
}

/// One simulated FacetQL instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimNode {
    pub id: DbmsId,
    pub region: String,

    /// Whether the process is running at all.
    pub online: bool,

    /// Whether the Fabric can reach it. A partitioned node keeps serving its
    /// own traffic -- README §5, "local autonomy, global awareness" -- but the
    /// control plane stops hearing from it, which is exactly what makes a
    /// partition different from a crash.
    pub reachable: bool,

    pub placement_capacity: usize,
    pub capacity_ops: f64,

    pub served_ops: f64,
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub queue_depth: u64,
}

impl SimNode {
    fn new(
        id: DbmsId,
        region: String,
        placement_capacity: usize,
        capacity_ops: f64,
    ) -> Self {
        Self {
            id,
            region,
            online: true,
            reachable: true,
            placement_capacity,
            capacity_ops,
            served_ops: 0.0,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            queue_depth: 0,
        }
    }

    /// How the control plane would see this node.
    pub fn condition(&self) -> NodeCondition {
        if !self.online || !self.reachable {
            // Indistinguishable from the outside, and correctly so: silence is
            // silence, and the control plane must not guess which it is.
            return NodeCondition::Unreachable;
        }

        if self.cpu_utilization >= 0.95 {
            return NodeCondition::Degraded;
        }

        NodeCondition::Healthy
    }
}

/// One placed logical cell and the workload running on it.
///
/// Not `PartialEq`: `fabric_core::Workload` is not, and inventing a comparison
/// for it here would be a parallel definition of when two workloads are the
/// same thing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimCell {
    pub target: ActionTarget,
    pub workload: Workload,

    /// The node that currently answers for this cell.
    pub owner: String,

    /// Additional holders. Reads are shared across owner and replicas; writes
    /// go to all of them, which is why replicating a write-heavy cell makes
    /// things worse and the optimizer is right not to.
    pub replicas: BTreeSet<String>,

    /// Holders that have been staged but not yet cut over to. They consume
    /// capacity without serving traffic, which is what makes a half-finished
    /// migration visible as pressure rather than as nothing.
    pub staging: BTreeSet<String>,

    /// Per-cell multiplier on the class base rate.
    pub scale: f64,

    /// Resident bytes -- what a migration has to move.
    pub bytes: u64,

    pub hot_multiplier: f64,
    pub hot_until_ms: u64,

    /// Phase offset so that cells do not all peak at the same instant.
    pub phase_offset_ms: u64,
}

impl SimCell {
    pub fn class(&self) -> WorkloadClass {
        self.workload.class
    }

    pub fn profile(&self) -> ClassProfile {
        class_profile(self.workload.class)
    }

    /// Every node holding live data for this cell, in id order.
    pub fn holders(&self) -> Vec<String> {
        let mut holders = vec![self.owner.clone()];

        for replica in &self.replicas {
            if replica != &self.owner {
                holders.push(replica.clone());
            }
        }

        holders
    }

    /// Capacity this cell consumes on `node`, counting staged copies.
    pub fn occupies(&self, node: &str) -> bool {
        self.owner == node
            || self.replicas.contains(node)
            || self.staging.contains(node)
    }
}

/// The mutable world the simulation advances and control actions edit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    nodes: BTreeMap<String, SimNode>,
    cells: BTreeMap<ActionTarget, SimCell>,
    shards: BTreeMap<u64, Shard>,
    generation: u64,
    transfer_bytes_per_ms: u64,
}

impl ClusterState {
    /// Build a cluster from a spec.
    ///
    /// Every draw comes from a stream forked off the spec's seed, and every
    /// loop runs over an ordered collection, so construction is reproducible
    /// down to which class landed on which cell.
    pub fn build(spec: &ClusterSpec) -> Self {
        let mut rng = Rng::new(spec.seed);

        let mut nodes = BTreeMap::new();
        let mut cells = BTreeMap::new();
        let mut shards = BTreeMap::new();

        let cells_per_shard = spec.cells_per_shard.min(fabric_core::GRID_ATOMS);

        let mut shard_id: u64 = 1;
        let mut workload_id: u64 = 1;

        for region in &spec.regions {
            for index in 0..spec.nodes_per_region {
                let id = DbmsId::new(format!("{region}-db-{index}"));

                nodes.insert(
                    id.0.clone(),
                    SimNode::new(
                        id.clone(),
                        region.clone(),
                        spec.placement_capacity,
                        spec.node_capacity_ops,
                    ),
                );

                for _ in 0..spec.shards_per_node {
                    let shard = Shard::new(shard_id, region.clone());

                    for cell_index in 0..cells_per_shard {
                        let coordinate = Coordinate::new(
                            (cell_index % fabric_core::GRID_WIDTH as usize) as u8,
                            (cell_index / fabric_core::GRID_WIDTH as usize) as u8,
                        );

                        let class = spec.classes
                            [rng.below(spec.classes.len().max(1))
                                .min(spec.classes.len().saturating_sub(1))];

                        let scale = rng.range(0.5, 1.5);
                        let profile = class_profile(class);

                        let bytes = (profile.bytes_per_op as f64
                            * profile.base_ops
                            * scale
                            * 4.0) as u64;

                        let phase_offset_ms =
                            rng.below(spec.diurnal_period_ms.max(1) as usize)
                                as u64;

                        let target =
                            ActionTarget::new(shard_id, coordinate);

                        cells.insert(
                            target,
                            SimCell {
                                target,
                                workload: Workload::new(workload_id, class),
                                owner: id.0.clone(),
                                replicas: BTreeSet::new(),
                                staging: BTreeSet::new(),
                                scale,
                                bytes,
                                hot_multiplier: 1.0,
                                hot_until_ms: 0,
                                phase_offset_ms,
                            },
                        );

                        workload_id += 1;
                    }

                    shards.insert(shard_id, shard);
                    shard_id += 1;
                }
            }
        }

        Self {
            nodes,
            cells,
            shards,
            generation: 1,
            transfer_bytes_per_ms: spec.transfer_bytes_per_ms.max(1),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn transfer_bytes_per_ms(&self) -> u64 {
        self.transfer_bytes_per_ms
    }

    pub fn node(&self, id: &str) -> Option<&SimNode> {
        self.nodes.get(id)
    }

    pub fn node_mut(&mut self, id: &str) -> Option<&mut SimNode> {
        self.nodes.get_mut(id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &SimNode> {
        self.nodes.values()
    }

    pub fn nodes_mut(&mut self) -> impl Iterator<Item = &mut SimNode> {
        self.nodes.values_mut()
    }

    pub fn cell(&self, target: ActionTarget) -> Option<&SimCell> {
        self.cells.get(&target)
    }

    pub fn cell_mut(&mut self, target: ActionTarget) -> Option<&mut SimCell> {
        self.cells.get_mut(&target)
    }

    pub fn cells(&self) -> impl Iterator<Item = &SimCell> {
        self.cells.values()
    }

    pub fn shard(&self, shard_id: u64) -> Option<&Shard> {
        self.shards.get(&shard_id)
    }

    /// Placements a node holds, staged copies included.
    pub fn hosted_placements(&self, node: &str) -> usize {
        self.cells
            .values()
            .filter(|cell| cell.occupies(node))
            .count()
    }

    /// Stage a copy on `node`: capacity is consumed, traffic is not served.
    pub fn stage(&mut self, target: ActionTarget, node: &str) {
        if let Some(cell) = self.cells.get_mut(&target) {
            cell.staging.insert(node.to_string());
        }
    }

    pub fn unstage(&mut self, target: ActionTarget, node: &str) {
        if let Some(cell) = self.cells.get_mut(&target) {
            cell.staging.remove(node);
        }
    }

    /// Promote a staged copy to a live replica.
    pub fn promote(&mut self, target: ActionTarget, node: &str) {
        if let Some(cell) = self.cells.get_mut(&target) {
            cell.staging.remove(node);
            cell.replicas.insert(node.to_string());
        }
    }

    /// Move authority for a cell. This is the only operation that changes the
    /// placement map, so it is the only one that bumps the generation the
    /// controller uses to detect a superseded decision.
    pub fn set_owner(&mut self, target: ActionTarget, node: &str) -> bool {
        let Some(cell) = self.cells.get_mut(&target) else {
            return false;
        };

        if cell.owner == node {
            return false;
        }

        let previous = cell.owner.clone();
        cell.owner = node.to_string();
        cell.staging.remove(node);
        cell.replicas.remove(node);
        cell.replicas.insert(previous);

        self.generation += 1;
        true
    }

    /// Drop a copy. The owner is never dropped this way: authority moves with
    /// [`set_owner`](Self::set_owner) first, which is what keeps the simulated
    /// world from reaching a state the controller's replica floor forbids.
    pub fn drop_copy(&mut self, target: ActionTarget, node: &str) {
        if let Some(cell) = self.cells.get_mut(&target) {
            cell.staging.remove(node);

            if cell.owner != node {
                cell.replicas.remove(node);
            }
        }
    }

    /// The placement map as the Fabric would hold it.
    pub fn topology(&self) -> TopologyRegistry {
        let mut topology = TopologyRegistry::new();

        for cell in self.cells.values() {
            let Some(shard) = self.shards.get(&cell.target.shard_id) else {
                continue;
            };

            let Some(node) = self.nodes.get(&cell.owner) else {
                continue;
            };

            topology.place(
                node.id.clone(),
                shard,
                cell.target.coordinate,
                node.region.clone(),
            );
        }

        topology
    }

    /// The fleet as the controller would see it.
    pub fn fleet_view(&self) -> FleetView {
        let mut fleet = FleetView::new(self.generation);

        for node in self.nodes.values() {
            fleet.insert(
                NodeStatus::new(
                    node.id.clone(),
                    node.region.clone(),
                    node.condition(),
                    node.placement_capacity,
                )
                .with_load(
                    self.hosted_placements(&node.id.0),
                    node.cpu_utilization,
                    node.memory_utilization,
                ),
            );
        }

        fleet
    }

    /// The least-loaded healthy node other than those in `exclude`, by served
    /// load then by id so the choice never depends on map iteration order.
    pub fn least_loaded_node(&self, exclude: &[String]) -> Option<DbmsId> {
        let mut best: Option<(&SimNode, f64)> = None;

        for node in self.nodes.values() {
            if exclude.iter().any(|id| id == &node.id.0) {
                continue;
            }

            if node.condition() != NodeCondition::Healthy {
                continue;
            }

            if self.hosted_placements(&node.id.0) >= node.placement_capacity {
                continue;
            }

            let load = node.cpu_utilization;

            match best {
                Some((_, best_load)) if best_load <= load => {}
                _ => best = Some((node, load)),
            }
        }

        best.map(|(node, _)| node.id.clone())
    }
}
