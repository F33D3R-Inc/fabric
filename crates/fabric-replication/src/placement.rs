//! Where the copies of a shard should live.
//!
//! The planner is a pure function of (rule, current set, candidate nodes). It
//! produces a [`PlacementPlan`] — a list of additions and removals — and never
//! applies it. Applying is [`ReplicaSet::apply`](crate::ReplicaSet::apply),
//! driven by the controller.

use std::collections::BTreeSet;

use fabric_core::DbmsId;
use serde::{Deserialize, Serialize};

use crate::{
    replica::{ReplicaRole, ReplicaState},
    set::ReplicaSet,
};

/// Three copies: one may fail and one may be mid-seed while the shard is still
/// durable and servable.
pub const DEFAULT_REPLICATION_FACTOR: u8 = 3;

/// How many copies of a shard should exist.
///
/// A validated newtype rather than a bare `u8` because zero copies is not a
/// replication factor, it is deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplicationFactor(u8);

impl ReplicationFactor {
    pub fn new(copies: u8) -> Result<Self, PlacementError> {
        if copies == 0 {
            return Err(PlacementError::ZeroReplicationFactor);
        }

        Ok(Self(copies))
    }

    pub fn get(self) -> u8 {
        self.0
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self(DEFAULT_REPLICATION_FACTOR)
    }
}

/// How far apart the copies must be spread.
///
/// Distinct nodes is not a variant: it is an invariant of every rule. Two
/// copies of a shard on one node are one copy with extra steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpreadPolicy {
    /// Distinct nodes only.
    DistinctNodes,

    /// Distinct nodes; prefer an unused region, but accept a repeat region
    /// rather than leave the shard under-replicated.
    PreferDistinctRegions,

    /// Distinct nodes and distinct regions. A shortfall is reported rather
    /// than resolved by doubling up in a region.
    RequireDistinctRegions,
}

/// The placement rule for one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRule {
    pub factor: ReplicationFactor,
    pub spread: SpreadPolicy,
}

impl Default for PlacementRule {
    fn default() -> Self {
        Self {
            factor: ReplicationFactor::default(),
            spread: SpreadPolicy::PreferDistinctRegions,
        }
    }
}

/// A node the planner may place a copy on.
///
/// Supplied by the caller from its own fleet inventory (the controller builds
/// these from its node registry and topology); the planner does no discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementCandidate {
    pub node: DbmsId,
    pub region: String,

    /// Whether the node is currently fit to be given work. An unserviceable
    /// node is never planned onto.
    pub serviceable: bool,

    /// How many shard copies the node already hosts, across all shards. Used
    /// only to break ties towards the emptier node.
    pub hosted_replicas: u32,
}

impl PlacementCandidate {
    pub fn new(
        node: DbmsId,
        region: impl Into<String>,
        serviceable: bool,
        hosted_replicas: u32,
    ) -> Self {
        Self {
            node,
            region: region.into(),
            serviceable,
            hosted_replicas,
        }
    }
}

/// One copy the plan wants created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaPlacement {
    pub node: DbmsId,
    pub region: String,
}

/// What the set should become, expressed as operations the controller can
/// apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPlan {
    pub shard_id: u64,
    pub factor: ReplicationFactor,

    /// Copies to create, best candidate first.
    pub additions: Vec<ReplicaPlacement>,

    /// Surplus copies to drop. Never contains the primary and never takes the
    /// set below the replication factor.
    pub removals: Vec<DbmsId>,

    /// Copies still missing after every usable candidate was consumed.
    pub shortfall: u8,

    /// Copies that are present but failed, and so are not counted towards the
    /// factor. Re-seeding or replacing them is the controller's call.
    pub failed: Vec<DbmsId>,
}

impl PlacementPlan {
    /// Whether applying this plan reaches the replication factor.
    pub fn is_satisfiable(&self) -> bool {
        self.shortfall == 0
    }

    /// Whether the plan asks for any change at all.
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.removals.is_empty()
    }
}

/// Why a plan could not be produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementError {
    ZeroReplicationFactor,

    /// A candidate list containing the same node twice would let the planner
    /// place two copies of one shard on one node.
    DuplicateCandidate { node: DbmsId },
}

impl std::fmt::Display for PlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroReplicationFactor => {
                write!(f, "replication factor must be at least 1")
            }

            Self::DuplicateCandidate { node } => {
                write!(f, "node '{}' appears twice in the candidate list", node.0)
            }
        }
    }
}

impl std::error::Error for PlacementError {}

/// Plans replica placement. Stateless.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicaPlacementPlanner;

impl ReplicaPlacementPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Plan the set towards `rule`.
    ///
    /// Deterministic: candidates are ranked by (region already used, replicas
    /// already hosted, node id), so the same inputs always produce the same
    /// plan — a property the controller relies on when it re-plans after a
    /// crash and must not thrash placements.
    pub fn plan(
        &self,
        rule: &PlacementRule,
        set: &ReplicaSet,
        candidates: &[PlacementCandidate],
    ) -> Result<PlacementPlan, PlacementError> {
        let mut seen = BTreeSet::new();

        for candidate in candidates {
            if !seen.insert(candidate.node.0.as_str()) {
                return Err(PlacementError::DuplicateCandidate {
                    node: candidate.node.clone(),
                });
            }
        }

        let wanted = rule.factor.as_usize();

        let failed: Vec<DbmsId> = set
            .replicas()
            .filter(|replica| replica.state == ReplicaState::Failed)
            .map(|replica| replica.node.clone())
            .collect();

        /*
         * A failed copy occupies a node without protecting anything, so it does
         * not count towards the factor. It is still listed so the controller
         * can decide between re-seeding it and replacing it -- that choice
         * needs cost information this crate does not have.
         */
        let counted: Vec<&crate::replica::Replica> = set
            .replicas()
            .filter(|replica| replica.state != ReplicaState::Failed)
            .collect();

        let mut used_nodes: BTreeSet<String> = set
            .replicas()
            .map(|replica| replica.node.0.clone())
            .collect();

        let mut used_regions: BTreeSet<String> = counted
            .iter()
            .map(|replica| replica.region.clone())
            .collect();

        let mut additions = Vec::new();
        let mut shortfall = 0_u8;

        let missing = wanted.saturating_sub(counted.len());

        for _ in 0..missing {
            let choice = self.best_candidate(
                rule.spread,
                candidates,
                &used_nodes,
                &used_regions,
            );

            match choice {
                Some(candidate) => {
                    used_nodes.insert(candidate.node.0.clone());
                    used_regions.insert(candidate.region.clone());

                    additions.push(ReplicaPlacement {
                        node: candidate.node.clone(),
                        region: candidate.region.clone(),
                    });
                }

                None => {
                    shortfall = shortfall.saturating_add(1);
                }
            }
        }

        let removals = self.surplus(set, wanted);

        Ok(PlacementPlan {
            shard_id: set.shard_id,
            factor: rule.factor,
            additions,
            removals,
            shortfall,
            failed,
        })
    }

    fn best_candidate<'a>(
        &self,
        spread: SpreadPolicy,
        candidates: &'a [PlacementCandidate],
        used_nodes: &BTreeSet<String>,
        used_regions: &BTreeSet<String>,
    ) -> Option<&'a PlacementCandidate> {
        candidates
            .iter()
            .filter(|candidate| candidate.serviceable)
            // The hard invariant: never a second copy of this shard on a node
            // that already holds one.
            .filter(|candidate| !used_nodes.contains(&candidate.node.0))
            .filter(|candidate| match spread {
                SpreadPolicy::RequireDistinctRegions => {
                    !used_regions.contains(&candidate.region)
                }
                _ => true,
            })
            .min_by(|left, right| {
                let region_rank = |candidate: &PlacementCandidate| {
                    match spread {
                        SpreadPolicy::DistinctNodes => 0_u8,
                        _ => u8::from(used_regions.contains(&candidate.region)),
                    }
                };

                region_rank(left)
                    .cmp(&region_rank(right))
                    .then(left.hosted_replicas.cmp(&right.hosted_replicas))
                    .then(left.node.0.cmp(&right.node.0))
            })
    }

    /// Copies beyond the factor, worst first, excluding the primary.
    ///
    /// Only healthy surplus is proposed for removal: dropping a lagging copy
    /// that is catching up would turn a temporary staleness into a re-seed.
    fn surplus(&self, set: &ReplicaSet, wanted: usize) -> Vec<DbmsId> {
        let healthy: Vec<&crate::replica::Replica> = set
            .replicas()
            .filter(|replica| replica.state == ReplicaState::InSync)
            .collect();

        if healthy.len() <= wanted {
            return Vec::new();
        }

        let mut droppable: Vec<&crate::replica::Replica> = healthy
            .into_iter()
            .filter(|replica| replica.role != ReplicaRole::Primary)
            .collect();

        droppable.sort_by(|left, right| left.node.0.cmp(&right.node.0));

        let excess = set
            .replicas()
            .filter(|replica| replica.state == ReplicaState::InSync)
            .count()
            - wanted;

        droppable
            .into_iter()
            .rev()
            .take(excess)
            .map(|replica| replica.node.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(node: &str, region: &str, hosted: u32) -> PlacementCandidate {
        PlacementCandidate::new(DbmsId::new(node), region, true, hosted)
    }

    #[test]
    fn a_plan_never_puts_two_copies_on_one_node() {
        let set = ReplicaSet::bootstrap(
            7,
            ReplicationFactor::default(),
            DbmsId::new("db-a"),
            "phoenix",
        );

        let plan = ReplicaPlacementPlanner::new()
            .plan(
                &PlacementRule::default(),
                &set,
                &[
                    candidate("db-a", "phoenix", 0),
                    candidate("db-b", "phoenix", 4),
                    candidate("db-c", "dallas", 9),
                ],
            )
            .expect("plannable");

        let nodes: Vec<&str> = plan
            .additions
            .iter()
            .map(|placement| placement.node.0.as_str())
            .collect();

        assert!(!nodes.contains(&"db-a"), "the existing host was reused");
        // dallas is unused, so it outranks the emptier phoenix node.
        assert_eq!(nodes, vec!["db-c", "db-b"]);
        assert!(plan.is_satisfiable());
    }

    #[test]
    fn a_strict_region_rule_reports_a_shortfall_instead_of_doubling_up() {
        let set = ReplicaSet::bootstrap(
            7,
            ReplicationFactor::default(),
            DbmsId::new("db-a"),
            "phoenix",
        );

        let rule = PlacementRule {
            factor: ReplicationFactor::default(),
            spread: SpreadPolicy::RequireDistinctRegions,
        };

        let plan = ReplicaPlacementPlanner::new()
            .plan(
                &rule,
                &set,
                &[candidate("db-b", "dallas", 0), candidate("db-c", "dallas", 0)],
            )
            .expect("plannable");

        assert_eq!(plan.additions.len(), 1);
        assert_eq!(plan.shortfall, 1);
        assert!(!plan.is_satisfiable());
    }

    #[test]
    fn an_unserviceable_node_is_never_planned_onto() {
        let set = ReplicaSet::new(7, ReplicationFactor::new(1).unwrap());

        let plan = ReplicaPlacementPlanner::new()
            .plan(
                &PlacementRule {
                    factor: ReplicationFactor::new(1).unwrap(),
                    spread: SpreadPolicy::DistinctNodes,
                },
                &set,
                &[PlacementCandidate::new(
                    DbmsId::new("db-down"),
                    "phoenix",
                    false,
                    0,
                )],
            )
            .expect("plannable");

        assert!(plan.additions.is_empty());
        assert_eq!(plan.shortfall, 1);
    }
}
