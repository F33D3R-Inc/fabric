//! One copy of one shard, and the evidence about it.
//!
//! A replica's *role* (primary or secondary) and its *state* (how usable the
//! copy is) are deliberately separate axes. Role is changed only by an explicit
//! operation from the controller; state moves along a fixed lifecycle graph and
//! may also be advanced mechanically by health reports. Conflating them is how
//! systems end up promoting a replica that has no data.

use fabric_core::DbmsId;
use fabric_telemetry::Observation;
use serde::{Deserialize, Serialize};

/// Which part a replica plays in its set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// The copy that accepts writes. Exactly one per non-empty set.
    Primary,

    /// A copy that follows the primary. May serve reads once in sync.
    Secondary,
}

impl ReplicaRole {
    pub fn label(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        }
    }
}

/// How usable a copy is, independent of the part it plays.
///
/// The lifecycle graph is:
///
/// ```text
/// absent ──▶ seeding ──▶ in-sync ⇄ lagging
///    ▲          │           │         │
///    └──────────┴───────────┴────▶ failed ──▶ seeding
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Declared but holding no data yet.
    Absent,

    /// Receiving the initial copy. Holds partial data, so it is not readable.
    Seeding,

    /// Holding a complete copy within the freshness thresholds.
    InSync,

    /// Holding a complete but stale copy: readable only if the caller has
    /// explicitly accepted staleness, never promotable.
    Lagging,

    /// Unusable. Needs a re-seed before it counts towards anything.
    Failed,
}

impl ReplicaState {
    /// Whether the lifecycle graph permits `self -> next`.
    ///
    /// A transition to the same state is permitted (health reports are
    /// idempotent), except into [`ReplicaState::Absent`], which is a data-loss
    /// declaration and must be deliberate.
    pub fn can_transition_to(self, next: Self) -> bool {
        use ReplicaState::*;

        if self == next {
            return next != Absent;
        }

        match (self, next) {
            (Absent, Seeding) => true,
            (Seeding, InSync | Failed | Absent) => true,
            (InSync, Lagging | Failed) => true,
            (Lagging, InSync | Seeding | Failed) => true,
            (Failed, Seeding | Absent) => true,
            _ => false,
        }
    }

    /// Whether the copy is complete enough that losing every other copy would
    /// not lose data.
    pub fn holds_full_copy(self) -> bool {
        matches!(self, Self::InSync | Self::Lagging)
    }

    /// Whether a read served from this copy is guaranteed fresh.
    pub fn is_fresh(self) -> bool {
        matches!(self, Self::InSync)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Seeding => "seeding",
            Self::InSync => "in-sync",
            Self::Lagging => "lagging",
            Self::Failed => "failed",
        }
    }
}

/// How far behind the primary a copy is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaLag {
    /// Writes accepted by the primary that this copy has not applied.
    pub pending_writes: u64,

    /// Age of the newest write applied here, relative to the primary's newest
    /// write, in milliseconds.
    pub apply_lag_ms: u64,
}

impl ReplicaLag {
    pub const NONE: Self = Self {
        pending_writes: 0,
        apply_lag_ms: 0,
    };

    pub fn new(pending_writes: u64, apply_lag_ms: u64) -> Self {
        Self {
            pending_writes,
            apply_lag_ms,
        }
    }

    /// Whether this lag exceeds either freshness threshold.
    pub fn exceeds(&self, thresholds: &LagThresholds) -> bool {
        self.pending_writes > thresholds.lagging_pending_writes
            || self.apply_lag_ms > thresholds.lagging_apply_lag_ms
    }
}

/// What the fleet reports about one copy at one instant.
///
/// This is the only input that moves a replica's state without an explicit
/// operation, and it carries its own timestamp: the crate never reads a clock.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaReport {
    pub shard_id: u64,
    pub node: DbmsId,
    pub timestamp_ms: u64,
    pub lag: ReplicaLag,

    /// Whether the reporter could reach the node at all. A single unreachable
    /// report is not a failure; see [`LagThresholds::unreachable_grace_ms`].
    pub reachable: bool,
}

impl ReplicaReport {
    pub fn new(
        shard_id: u64,
        node: DbmsId,
        timestamp_ms: u64,
        lag: ReplicaLag,
    ) -> Self {
        Self {
            shard_id,
            node,
            timestamp_ms,
            lag,
            reachable: true,
        }
    }

    /// Build a report from a telemetry [`Observation`] plus the lag measured
    /// alongside it.
    ///
    /// The observation supplies *when* and *which shard*, and its existence is
    /// itself the reachability evidence. Lag is a replication-specific quantity
    /// that telemetry's [`WorkloadMetrics`](fabric_telemetry::WorkloadMetrics)
    /// does not carry, so it is passed in rather than guessed at.
    pub fn from_observation(
        node: DbmsId,
        observation: &Observation,
        lag: ReplicaLag,
    ) -> Self {
        Self {
            shard_id: observation.shard.id,
            node,
            timestamp_ms: observation.timestamp_ms,
            lag,
            reachable: true,
        }
    }

    /// A report that the node could not be reached. Lag is unknown, so the
    /// last known lag is retained rather than being reset to zero.
    pub fn unreachable(
        shard_id: u64,
        node: DbmsId,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            shard_id,
            node,
            timestamp_ms,
            lag: ReplicaLag::NONE,
            reachable: false,
        }
    }
}

/// The freshness and patience budgets applied to health reports.
///
/// These are inputs, not policy invented here: a caller that wants different
/// budgets passes different thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LagThresholds {
    /// Unapplied writes above which an in-sync copy becomes lagging.
    pub lagging_pending_writes: u64,

    /// Apply lag above which an in-sync copy becomes lagging.
    pub lagging_apply_lag_ms: u64,

    /// How long a copy may stay unreachable before it counts as failed.
    pub unreachable_grace_ms: u64,
}

impl Default for LagThresholds {
    fn default() -> Self {
        Self {
            lagging_pending_writes: 1_000,
            lagging_apply_lag_ms: 5_000,
            unreachable_grace_ms: 30_000,
        }
    }
}

/// One copy of one shard on one node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replica {
    pub shard_id: u64,
    pub node: DbmsId,
    pub region: String,
    pub role: ReplicaRole,
    pub state: ReplicaState,
    pub lag: ReplicaLag,

    /// Timestamp of the newest report applied to this replica.
    pub last_report_ms: Option<u64>,

    /// When the current run of unreachable reports began. Cleared by any
    /// reachable report.
    pub unreachable_since_ms: Option<u64>,
}

impl Replica {
    pub(crate) fn new(
        shard_id: u64,
        node: DbmsId,
        region: impl Into<String>,
        role: ReplicaRole,
        state: ReplicaState,
    ) -> Self {
        Self {
            shard_id,
            node,
            region: region.into(),
            role,
            state,
            lag: ReplicaLag::NONE,
            last_report_ms: None,
            unreachable_since_ms: None,
        }
    }

    pub fn is_primary(&self) -> bool {
        self.role == ReplicaRole::Primary
    }

    /// Whether this copy may serve a read that requires current data.
    pub fn can_serve_fresh_reads(&self) -> bool {
        self.state.is_fresh()
    }

    /// Whether this copy may serve a read that tolerates staleness.
    pub fn can_serve_stale_reads(&self) -> bool {
        self.state.holds_full_copy()
    }

    /// Whether writes may be routed here. Only an in-sync primary qualifies:
    /// a lagging primary is one that has stopped keeping up with itself, which
    /// means its own apply pipeline is behind, and a failed one has no copy.
    pub fn can_serve_writes(&self) -> bool {
        self.is_primary() && self.state.is_fresh()
    }

    /// Milliseconds since the last report at `now_ms`, or `None` if nothing has
    /// ever been reported about this copy.
    pub fn silence_ms(&self, now_ms: u64) -> Option<u64> {
        self.last_report_ms
            .map(|last| now_ms.saturating_sub(last))
    }
}

/// A state change that actually happened, for the caller to log or ship.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaTransition {
    pub shard_id: u64,
    pub node: DbmsId,
    pub from: ReplicaState,
    pub to: ReplicaState,

    /// Report timestamp for a health-driven change; `None` for an operation
    /// applied by the controller, which carries no time of its own.
    pub at_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lifecycle_graph_has_no_shortcut_into_service() {
        // The bug this forbids: a copy that never seeded being treated as a
        // complete copy.
        assert!(!ReplicaState::Absent.can_transition_to(ReplicaState::InSync));
        assert!(!ReplicaState::Failed.can_transition_to(ReplicaState::InSync));
        assert!(ReplicaState::Absent.can_transition_to(ReplicaState::Seeding));
        assert!(ReplicaState::Seeding.can_transition_to(ReplicaState::InSync));
    }

    #[test]
    fn a_repeated_state_is_idempotent_but_absent_is_not() {
        assert!(ReplicaState::InSync.can_transition_to(ReplicaState::InSync));
        assert!(!ReplicaState::Absent.can_transition_to(ReplicaState::Absent));
    }
}
