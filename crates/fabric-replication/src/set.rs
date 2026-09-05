//! The replica set of one shard: the copies, their roles, and the only legal
//! ways to change them.
//!
//! # The invariant
//!
//! **A non-empty set has exactly one primary, and a copy only becomes primary
//! from [`ReplicaState::InSync`].**
//!
//! Every operation is checked against it before it is applied, and an operation
//! that would break it is rejected rather than partially applied. Promotion is
//! a swap — the outgoing primary is demoted in the same operation — so there is
//! never an instant with two primaries or none.
//!
//! A second, quieter invariant: **a node hosts at most one copy of a shard.**
//! Two copies on one machine are one copy that costs twice as much.

use fabric_core::DbmsId;
use serde::{Deserialize, Serialize};

use crate::{
    placement::ReplicationFactor,
    replica::{
        LagThresholds,
        Replica,
        ReplicaReport,
        ReplicaRole,
        ReplicaState,
        ReplicaTransition,
    },
};

/// A change to a replica set. The controller applies these; nothing in this
/// crate applies one on its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaOperation {
    /// Declare a copy that does not exist yet. It starts
    /// [`ReplicaState::Absent`]: declaring is not seeding.
    Add { node: DbmsId, region: String },

    /// Begin copying data onto a secondary.
    BeginSeed { node: DbmsId },

    /// The runtime reports the copy is complete and caught up.
    MarkInSync { node: DbmsId },

    /// Mark a copy unusable. Always legal from any state holding data.
    MarkFailed { node: DbmsId },

    /// Make `node` the primary, demoting the current one in the same step.
    Promote { node: DbmsId },

    /// Drop a copy from the set. Refused for the current primary: promote
    /// first, so the shard is never left without a write target.
    Remove { node: DbmsId },
}

/// Why an operation or report was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationError {
    DuplicateReplica { shard_id: u64, node: DbmsId },
    UnknownReplica { shard_id: u64, node: DbmsId },

    IllegalTransition {
        node: DbmsId,
        from: ReplicaState,
        to: ReplicaState,
    },

    /// A primary cannot re-seed: while it seeds it has no complete copy, and
    /// it is the shard's write target.
    PrimaryCannotSeed { node: DbmsId },

    /// Promotion of a copy that is not in sync would silently discard the
    /// writes it has not applied.
    NotPromotable { node: DbmsId, state: ReplicaState },

    PrimaryNotRemovable { node: DbmsId },

    /// A report addressed to a different shard than this set.
    WrongShard { expected: u64, actual: u64 },
}

impl std::fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateReplica { shard_id, node } => write!(
                f,
                "shard {shard_id} already has a replica on node '{}'",
                node.0
            ),

            Self::UnknownReplica { shard_id, node } => write!(
                f,
                "shard {shard_id} has no replica on node '{}'",
                node.0
            ),

            Self::IllegalTransition { node, from, to } => write!(
                f,
                "replica on '{}' cannot go from {} to {}",
                node.0,
                from.label(),
                to.label()
            ),

            Self::PrimaryCannotSeed { node } => write!(
                f,
                "replica on '{}' is the primary and cannot seed; promote another copy first",
                node.0
            ),

            Self::NotPromotable { node, state } => write!(
                f,
                "replica on '{}' is {} and only an in-sync copy may be promoted",
                node.0,
                state.label()
            ),

            Self::PrimaryNotRemovable { node } => write!(
                f,
                "replica on '{}' is the primary; promote another copy before removing it",
                node.0
            ),

            Self::WrongShard { expected, actual } => write!(
                f,
                "report is for shard {actual}, this set is shard {expected}"
            ),
        }
    }
}

impl std::error::Error for ReplicationError {}

/// Every copy of one shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaSet {
    pub shard_id: u64,

    /// How many copies the set is meant to have. Held here so a plan can be
    /// recomputed from the set alone.
    pub desired_factor: ReplicationFactor,

    replicas: Vec<Replica>,
}

impl ReplicaSet {
    /// An empty set. It has no primary and can serve nothing until a copy is
    /// added and seeded, or until [`ReplicaSet::bootstrap`] is used.
    pub fn new(shard_id: u64, desired_factor: ReplicationFactor) -> Self {
        Self {
            shard_id,
            desired_factor,
            replicas: Vec::new(),
        }
    }

    /// A set whose first copy is the primary, already in sync.
    ///
    /// This is the one place a copy reaches [`ReplicaState::InSync`] without
    /// seeding, and it is sound for exactly one reason: the first copy of a
    /// shard has no source to seed *from*, so it is authoritative by
    /// definition. Every later copy must seed.
    pub fn bootstrap(
        shard_id: u64,
        desired_factor: ReplicationFactor,
        node: DbmsId,
        region: impl Into<String>,
    ) -> Self {
        Self {
            shard_id,
            desired_factor,
            replicas: vec![Replica::new(
                shard_id,
                node,
                region,
                ReplicaRole::Primary,
                ReplicaState::InSync,
            )],
        }
    }

    pub fn replicas(&self) -> impl Iterator<Item = &Replica> {
        self.replicas.iter()
    }

    pub fn get(&self, node: &DbmsId) -> Option<&Replica> {
        self.replicas
            .iter()
            .find(|replica| &replica.node == node)
    }

    pub fn primary(&self) -> Option<&Replica> {
        self.replicas
            .iter()
            .find(|replica| replica.is_primary())
    }

    pub fn secondaries(&self) -> impl Iterator<Item = &Replica> {
        self.replicas
            .iter()
            .filter(|replica| !replica.is_primary())
    }

    /// The copy writes must go to, or `None` when there is no usable primary —
    /// which is the condition a failover assessment exists to describe.
    pub fn write_target(&self) -> Option<&Replica> {
        self.primary()
            .filter(|replica| replica.can_serve_writes())
    }

    /// Copies that may serve a read, freshest first (primary, then in-sync
    /// secondaries in node order). Selecting among them — locality, load — is
    /// the caller's decision, so all of them are returned.
    pub fn read_targets(&self, allow_stale: bool) -> Vec<&Replica> {
        let mut targets: Vec<&Replica> = self
            .replicas
            .iter()
            .filter(|replica| {
                if allow_stale {
                    replica.can_serve_stale_reads()
                } else {
                    replica.can_serve_fresh_reads()
                }
            })
            .collect();

        targets.sort_by(|left, right| {
            right
                .is_primary()
                .cmp(&left.is_primary())
                .then(left.state.is_fresh().cmp(&right.state.is_fresh()).reverse())
                .then(left.node.0.cmp(&right.node.0))
        });

        targets
    }

    /// Copies that count towards the replication factor: everything that is not
    /// failed.
    pub fn effective_size(&self) -> usize {
        self.replicas
            .iter()
            .filter(|replica| replica.state != ReplicaState::Failed)
            .count()
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    pub fn is_under_replicated(&self) -> bool {
        self.effective_size() < self.desired_factor.as_usize()
    }

    /// Whether losing one more copy would lose the shard.
    pub fn is_at_risk(&self) -> bool {
        self.replicas
            .iter()
            .filter(|replica| replica.state.holds_full_copy())
            .count()
            <= 1
    }

    /// Apply one operation, returning the state change it caused if any.
    ///
    /// Checked before applied: a rejected operation leaves the set untouched.
    pub fn apply(
        &mut self,
        operation: ReplicaOperation,
    ) -> Result<Option<ReplicaTransition>, ReplicationError> {
        match operation {
            ReplicaOperation::Add { node, region } => {
                if self.get(&node).is_some() {
                    return Err(ReplicationError::DuplicateReplica {
                        shard_id: self.shard_id,
                        node,
                    });
                }

                self.replicas.push(Replica::new(
                    self.shard_id,
                    node,
                    region,
                    ReplicaRole::Secondary,
                    ReplicaState::Absent,
                ));

                self.replicas
                    .sort_by(|left, right| left.node.0.cmp(&right.node.0));

                Ok(None)
            }

            ReplicaOperation::BeginSeed { node } => {
                if self.replica(&node)?.is_primary() {
                    return Err(ReplicationError::PrimaryCannotSeed { node });
                }

                self.transition(&node, ReplicaState::Seeding, None)
            }

            ReplicaOperation::MarkInSync { node } => {
                self.transition(&node, ReplicaState::InSync, None)
            }

            ReplicaOperation::MarkFailed { node } => {
                self.transition(&node, ReplicaState::Failed, None)
            }

            ReplicaOperation::Promote { node } => {
                self.promote(&node)?;

                Ok(None)
            }

            ReplicaOperation::Remove { node } => {
                let replica = self.replica(&node)?;

                if replica.is_primary() {
                    return Err(ReplicationError::PrimaryNotRemovable { node });
                }

                self.replicas
                    .retain(|replica| replica.node != node);

                Ok(None)
            }
        }
    }

    /// Fold a health report into the set.
    ///
    /// This is the only mechanical state movement in the crate, and it is
    /// strictly bounded: freshness (in-sync ⇄ lagging) and death (unreachable
    /// past the grace period → failed). Seeding completion is reported by the
    /// runtime, never inferred from lag, and **a report never changes a role**
    /// — no report can promote anything.
    ///
    /// Reports older than the newest one already folded in are ignored, so an
    /// out-of-order delivery cannot resurrect a stale verdict.
    pub fn observe(
        &mut self,
        report: &ReplicaReport,
        thresholds: &LagThresholds,
    ) -> Result<Option<ReplicaTransition>, ReplicationError> {
        if report.shard_id != self.shard_id {
            return Err(ReplicationError::WrongShard {
                expected: self.shard_id,
                actual: report.shard_id,
            });
        }

        let shard_id = self.shard_id;

        let index = self
            .replicas
            .iter()
            .position(|replica| replica.node == report.node)
            .ok_or_else(|| ReplicationError::UnknownReplica {
                shard_id,
                node: report.node.clone(),
            })?;

        if let Some(last) = self.replicas[index].last_report_ms
            && report.timestamp_ms < last
        {
            return Ok(None);
        }

        let next = {
            let replica = &mut self.replicas[index];

            replica.last_report_ms = Some(report.timestamp_ms);

            if report.reachable {
                replica.unreachable_since_ms = None;
                replica.lag = report.lag;
            } else if replica.unreachable_since_ms.is_none() {
                replica.unreachable_since_ms = Some(report.timestamp_ms);
            }

            Self::verdict(replica, report, thresholds)
        };

        match next {
            Some(state) => self.transition(
                &report.node.clone(),
                state,
                Some(report.timestamp_ms),
            ),

            None => Ok(None),
        }
    }

    /// The state a report implies, or `None` to leave the state alone.
    fn verdict(
        replica: &Replica,
        report: &ReplicaReport,
        thresholds: &LagThresholds,
    ) -> Option<ReplicaState> {
        if !report.reachable {
            let since = replica
                .unreachable_since_ms
                .unwrap_or(report.timestamp_ms);

            let silent_for = report.timestamp_ms.saturating_sub(since);

            if silent_for >= thresholds.unreachable_grace_ms
                && replica.state != ReplicaState::Absent
            {
                return Some(ReplicaState::Failed);
            }

            return None;
        }

        /*
         * Lag is measured against the primary, so the primary's own lag is
         * definitionally zero and says nothing. Freshness transitions apply to
         * secondaries only; a primary leaves in-sync by failing, or by being
         * demoted when another copy is promoted.
         */
        if replica.is_primary() {
            return None;
        }

        match replica.state {
            ReplicaState::InSync if report.lag.exceeds(thresholds) => {
                Some(ReplicaState::Lagging)
            }

            ReplicaState::Lagging if !report.lag.exceeds(thresholds) => {
                Some(ReplicaState::InSync)
            }

            // Seeding progress and failure are reported as operations, not
            // inferred: a seeding copy with low lag is still incomplete.
            _ => None,
        }
    }

    fn replica(&self, node: &DbmsId) -> Result<&Replica, ReplicationError> {
        self.get(node)
            .ok_or_else(|| ReplicationError::UnknownReplica {
                shard_id: self.shard_id,
                node: node.clone(),
            })
    }

    fn transition(
        &mut self,
        node: &DbmsId,
        to: ReplicaState,
        at_ms: Option<u64>,
    ) -> Result<Option<ReplicaTransition>, ReplicationError> {
        let shard_id = self.shard_id;

        let replica = self
            .replicas
            .iter_mut()
            .find(|replica| &replica.node == node)
            .ok_or_else(|| ReplicationError::UnknownReplica {
                shard_id,
                node: node.clone(),
            })?;

        let from = replica.state;

        if !from.can_transition_to(to) {
            return Err(ReplicationError::IllegalTransition {
                node: node.clone(),
                from,
                to,
            });
        }

        if from == to {
            return Ok(None);
        }

        replica.state = to;

        if to == ReplicaState::Seeding || to == ReplicaState::Failed {
            replica.lag = crate::replica::ReplicaLag::NONE;
        }

        Ok(Some(ReplicaTransition {
            shard_id,
            node: node.clone(),
            from,
            to,
            at_ms,
        }))
    }

    /// Promotion as a single swap, so the "exactly one primary" invariant holds
    /// at every observable instant.
    fn promote(&mut self, node: &DbmsId) -> Result<(), ReplicationError> {
        let replica = self.replica(node)?;

        if replica.is_primary() {
            return Ok(());
        }

        if replica.state != ReplicaState::InSync {
            return Err(ReplicationError::NotPromotable {
                node: node.clone(),
                state: replica.state,
            });
        }

        for replica in &mut self.replicas {
            replica.role = if &replica.node == node {
                ReplicaRole::Primary
            } else {
                ReplicaRole::Secondary
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set() -> ReplicaSet {
        let mut set = ReplicaSet::bootstrap(
            9,
            ReplicationFactor::default(),
            DbmsId::new("db-a"),
            "phoenix",
        );

        set.apply(ReplicaOperation::Add {
            node: DbmsId::new("db-b"),
            region: "dallas".to_string(),
        })
        .unwrap();

        set
    }

    #[test]
    fn a_new_copy_must_seed_before_it_can_serve() {
        let mut set = set();

        let b = DbmsId::new("db-b");
        assert_eq!(set.get(&b).unwrap().state, ReplicaState::Absent);
        assert!(set.read_targets(false).iter().all(|r| r.node != b));

        let error = set
            .apply(ReplicaOperation::MarkInSync { node: b.clone() })
            .unwrap_err();

        assert!(matches!(
            error,
            ReplicationError::IllegalTransition {
                from: ReplicaState::Absent,
                to: ReplicaState::InSync,
                ..
            }
        ));

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b.clone() }).unwrap();

        assert!(set.read_targets(false).iter().any(|r| r.node == b));
    }

    #[test]
    fn promotion_is_a_swap_so_there_is_always_exactly_one_primary() {
        let mut set = set();
        let b = DbmsId::new("db-b");

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::Promote { node: b.clone() }).unwrap();

        let primaries: Vec<&DbmsId> = set
            .replicas()
            .filter(|replica| replica.is_primary())
            .map(|replica| &replica.node)
            .collect();

        assert_eq!(primaries, vec![&b]);
        assert_eq!(set.write_target().unwrap().node, b);
    }

    #[test]
    fn a_copy_that_is_not_in_sync_cannot_be_promoted() {
        let mut set = set();
        let b = DbmsId::new("db-b");

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();

        let error = set
            .apply(ReplicaOperation::Promote { node: b.clone() })
            .unwrap_err();

        assert!(matches!(
            error,
            ReplicationError::NotPromotable {
                state: ReplicaState::Seeding,
                ..
            }
        ));
        assert_eq!(set.primary().unwrap().node, DbmsId::new("db-a"));
    }

    #[test]
    fn the_primary_cannot_be_removed_out_from_under_the_shard() {
        let mut set = set();

        let error = set
            .apply(ReplicaOperation::Remove {
                node: DbmsId::new("db-a"),
            })
            .unwrap_err();

        assert!(matches!(error, ReplicationError::PrimaryNotRemovable { .. }));
        assert!(set.write_target().is_some());
    }

    #[test]
    fn lag_reports_move_a_secondary_between_in_sync_and_lagging() {
        let mut set = set();
        let b = DbmsId::new("db-b");
        let thresholds = LagThresholds::default();

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b.clone() }).unwrap();

        let behind = ReplicaReport::new(
            9,
            b.clone(),
            1_000,
            crate::replica::ReplicaLag::new(50_000, 60_000),
        );

        let transition = set.observe(&behind, &thresholds).unwrap().unwrap();
        assert_eq!(transition.to, ReplicaState::Lagging);
        assert!(!set.read_targets(false).iter().any(|r| r.node == b));
        // Stale reads may still use it; fresh reads may not.
        assert!(set.read_targets(true).iter().any(|r| r.node == b));

        let caught_up = ReplicaReport::new(9, b.clone(), 2_000, crate::replica::ReplicaLag::NONE);
        let transition = set.observe(&caught_up, &thresholds).unwrap().unwrap();
        assert_eq!(transition.to, ReplicaState::InSync);
    }

    #[test]
    fn an_out_of_order_report_is_ignored() {
        let mut set = set();
        let b = DbmsId::new("db-b");
        let thresholds = LagThresholds::default();

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b.clone() }).unwrap();

        set.observe(
            &ReplicaReport::new(9, b.clone(), 5_000, crate::replica::ReplicaLag::NONE),
            &thresholds,
        )
        .unwrap();

        let stale = ReplicaReport::new(
            9,
            b.clone(),
            1_000,
            crate::replica::ReplicaLag::new(99_999, 99_999),
        );

        assert_eq!(set.observe(&stale, &thresholds).unwrap(), None);
        assert_eq!(set.get(&b).unwrap().state, ReplicaState::InSync);
    }

    #[test]
    fn unreachability_only_fails_a_copy_after_the_grace_period() {
        let mut set = set();
        let b = DbmsId::new("db-b");
        let thresholds = LagThresholds::default();

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b.clone() }).unwrap();

        assert_eq!(
            set.observe(&ReplicaReport::unreachable(9, b.clone(), 1_000), &thresholds)
                .unwrap(),
            None
        );
        assert_eq!(set.get(&b).unwrap().state, ReplicaState::InSync);

        let transition = set
            .observe(
                &ReplicaReport::unreachable(9, b.clone(), 1_000 + thresholds.unreachable_grace_ms),
                &thresholds,
            )
            .unwrap()
            .unwrap();

        assert_eq!(transition.to, ReplicaState::Failed);
        assert!(set.is_under_replicated());
    }

    #[test]
    fn a_report_for_another_shard_is_refused() {
        let mut set = set();

        let error = set
            .observe(
                &ReplicaReport::new(
                    404,
                    DbmsId::new("db-b"),
                    1,
                    crate::replica::ReplicaLag::NONE,
                ),
                &LagThresholds::default(),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ReplicationError::WrongShard {
                expected: 9,
                actual: 404
            }
        ));
    }
}
