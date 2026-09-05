//! Who holds a copy of what.
//!
//! `TopologyRegistry` maps a target to *one* placement -- it answers "where do
//! I read this from", not "how many copies survive if that node dies". The
//! replica floor rule needs the second question answered, so the controller
//! keeps the ledger: seeded from the topology (the recorded placement is a
//! copy) and then maintained by the plans it commits, which are the only
//! things in the system that create or destroy copies.
//!
//! It is deliberately *this* crate's state rather than a new field on
//! `TopologyRegistry`: replication is a control-plane concern, and the
//! registry is shared with readers that must not be taught to care.

use std::collections::{BTreeMap, BTreeSet};

use fabric_core::DbmsId;
use fabric_topology::TopologyRegistry;
use serde::{Deserialize, Serialize};

use crate::plan::PlacementPlan;
use crate::target::ActionTarget;

/// Copies of each target, by node id.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicaLedger {
    replicas: BTreeMap<ActionTarget, BTreeSet<String>>,
}

impl ReplicaLedger {
    pub fn new() -> Self {
        Self {
            replicas: BTreeMap::new(),
        }
    }

    /// Fold a topology snapshot in: every recorded placement is a copy.
    ///
    /// Additive rather than replacing, so that copies the controller created
    /// and the registry does not model are not silently forgotten -- and
    /// forgetting one is exactly what would let the replica floor be breached.
    pub fn seed_from_topology(&mut self, topology: &TopologyRegistry) {
        for placement in topology.placements() {
            let target = ActionTarget::new(
                placement.shard_id,
                placement.coordinate,
            );

            self.replicas
                .entry(target)
                .or_default()
                .insert(placement.dbms_id.0.clone());
        }
    }

    /// Record `node` as a holder of `target` only if nothing is known about
    /// the target yet.
    ///
    /// The replica floor is checked against this ledger, so an unseeded target
    /// would read as zero copies and every plan that removes one would be
    /// refused as dropping the last replica. Seeding from the placement that
    /// justified the action keeps the floor honest without asking the caller
    /// to remember.
    pub fn ensure_seeded(&mut self, target: ActionTarget, node: &DbmsId) {
        if self.replicas.contains_key(&target) {
            return;
        }

        self.replicas
            .entry(target)
            .or_default()
            .insert(node.0.clone());
    }

    pub fn add(&mut self, target: ActionTarget, node: &DbmsId) {
        self.replicas
            .entry(target)
            .or_default()
            .insert(node.0.clone());
    }

    pub fn remove(&mut self, target: ActionTarget, node: &DbmsId) {
        if let Some(set) = self.replicas.get_mut(&target) {
            set.remove(&node.0);

            if set.is_empty() {
                self.replicas.remove(&target);
            }
        }
    }

    pub fn holds(&self, target: ActionTarget, node: &DbmsId) -> bool {
        self.replicas
            .get(&target)
            .is_some_and(|set| set.contains(&node.0))
    }

    pub fn count(&self, target: ActionTarget) -> usize {
        self.replicas
            .get(&target)
            .map_or(0, |set| set.len())
    }

    /// Holders of `target`, in node-id order.
    pub fn replicas(&self, target: ActionTarget) -> Vec<DbmsId> {
        self.replicas
            .get(&target)
            .map(|set| set.iter().map(|id| DbmsId::new(id.clone())).collect())
            .unwrap_or_default()
    }

    /// The holder set `plan` would leave behind, without applying it.
    ///
    /// Computed as a set rather than by counting adds and removes: a plan may
    /// name the same node in both lists, and arithmetic would then invent or
    /// destroy a copy that does not exist.
    pub fn projected(&self, plan: &PlacementPlan) -> BTreeSet<String> {
        let mut projected = self
            .replicas
            .get(&plan.target)
            .cloned()
            .unwrap_or_default();

        for node in &plan.adds {
            projected.insert(node.0.clone());
        }

        for node in &plan.removes {
            projected.remove(&node.0);
        }

        projected
    }

    /// Apply a plan's declared effect. Called only once a plan has run to
    /// completion, so the ledger records what happened rather than what was
    /// hoped for.
    pub fn apply(&mut self, plan: &PlacementPlan) {
        let projected = self.projected(plan);

        if projected.is_empty() {
            self.replicas.remove(&plan.target);
        } else {
            self.replicas.insert(plan.target, projected);
        }
    }

    pub fn targets(&self) -> impl Iterator<Item = &ActionTarget> {
        self.replicas.keys()
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }
}
