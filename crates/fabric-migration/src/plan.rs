//! What is being moved, from where, to where, and why.

use fabric_core::{DbmsId, Shard};
use serde::{Deserialize, Serialize};

/// How many unapplied writes a cutover may begin with.
///
/// Not zero: on a live shard the write stream never stops, so insisting on an
/// exactly-empty gap before fencing would mean never cutting over. The gap is
/// instead made small enough that draining it is a short pause, and the fence
/// then makes it exactly zero before authority moves.
pub const DEFAULT_MAX_CUTOVER_PENDING_WRITES: u64 = 64;

/// Identifies one migration attempt.
///
/// A retried migration is a new id: the audit trail of the failed attempt is
/// not overwritten by the attempt that replaced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationId(pub u64);

impl std::fmt::Display for MigrationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "migration-{}", self.0)
    }
}

/// Why the move was ordered.
///
/// Recorded, not acted on. The controller translates an optimizer decision
/// (`fabric_optimizer::OptimizationAction::Move`) or an operator instruction
/// into one of these; this crate keeps it so the migration can explain itself
/// later, when someone asks why a shard moved at 04:00.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MigrationReason {
    /// The shard is a hotspot on its current node.
    Hotspot { pressure_score: f64 },

    /// Evening out shard counts or load across the fleet.
    Rebalance,

    /// The source node is being emptied — decommission, maintenance, or a
    /// failure prediction.
    DrainNode,

    /// Moving the data closer to where it is used.
    RegionLocality { target_region: String },

    /// The source node is running out of capacity.
    CapacityPressure,

    /// A human said so.
    Operator { note: String },
}

impl MigrationReason {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Hotspot { .. } => "hotspot",
            Self::Rebalance => "rebalance",
            Self::DrainNode => "drain-node",
            Self::RegionLocality { .. } => "region-locality",
            Self::CapacityPressure => "capacity-pressure",
            Self::Operator { .. } => "operator",
        }
    }
}

/// An immutable description of one intended move.
///
/// The plan never changes once created; everything that changes lives in
/// [`Migration`](crate::Migration). That split is what lets a controller
/// persist the plan, crash, and reconstruct exactly what it had intended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub id: MigrationId,
    pub shard_id: u64,
    pub source: DbmsId,
    pub destination: DbmsId,
    pub reason: MigrationReason,
    pub created_at_ms: u64,

    /// Atoms to copy — a shard's full 12x13 grid.
    pub atoms: usize,

    /// The gap allowed when the fence goes up.
    pub max_cutover_pending_writes: u64,
}

impl MigrationPlan {
    /// Plan a move of `shard` from `source` to `destination`.
    ///
    /// Takes the shard itself rather than a bare id so the atom count comes
    /// from the core model instead of being restated here.
    pub fn new(
        id: MigrationId,
        shard: &Shard,
        source: DbmsId,
        destination: DbmsId,
        reason: MigrationReason,
        created_at_ms: u64,
    ) -> Result<Self, crate::MigrationError> {
        if source == destination {
            return Err(crate::MigrationError::SameSourceAndDestination { node: source });
        }

        Ok(Self {
            id,
            shard_id: shard.id,
            source,
            destination,
            reason,
            created_at_ms,
            atoms: shard.atom_count(),
            max_cutover_pending_writes: DEFAULT_MAX_CUTOVER_PENDING_WRITES,
        })
    }

    /// Override the gap allowed at fencing time.
    pub fn with_max_cutover_pending_writes(mut self, writes: u64) -> Self {
        self.max_cutover_pending_writes = writes;
        self
    }
}
