//! The migration state machine.
//!
//! See the crate docs for the two invariants. This module is where they are
//! enforced: every method that could break one returns [`MigrationError`]
//! instead of doing it.

use fabric_core::DbmsId;
use serde::{Deserialize, Serialize};

use crate::{
    phase::{MigrationPhase, PhaseTransition},
    plan::MigrationPlan,
    progress::MigrationProgress,
};

/// Position in a shard's write stream.
///
/// Assigned by whichever node currently accepts writes for the shard, densely
/// and in increasing order. Density is what makes "how many writes is the
/// destination behind" answerable by subtraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WriteSeq(pub u64);

impl WriteSeq {
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl std::fmt::Display for WriteSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Where a write must go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteRoute {
    Source,
    Destination,
}

/// The moment the source stopped accepting writes, and the sequence it stopped
/// at.
///
/// The fence is the whole write-safety argument in one struct: everything up to
/// `last_write` is the source's and must reach the destination before authority
/// moves; everything after is the destination's and must never be replayed from
/// the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutoverFence {
    pub at_ms: u64,

    /// The last sequence the source accepted. `None` when the shard had taken
    /// no writes at all during the migration.
    pub last_write: Option<WriteSeq>,
}

/// How a migration ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationOutcome {
    Completed { at_ms: u64 },
    Aborted { at_ms: u64, reason: String },
    Failed { at_ms: u64, error: String },
}

/// Why an operation on a migration was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationError {
    /// Moving a shard to the node it is already on is not a migration.
    SameSourceAndDestination { node: DbmsId },

    /// The phase machine has no such edge.
    InvalidTransition {
        from: MigrationPhase,
        to: MigrationPhase,
    },

    /// The operation is only meaningful in another phase.
    WrongPhase {
        expected: MigrationPhase,
        actual: MigrationPhase,
    },

    /// Catch-up cannot begin while atoms are still missing.
    CopyIncomplete { copied: usize, total: usize },

    ProgressExceedsTotal { copied: usize, total: usize },
    ProgressWentBackwards { reported: usize, recorded: usize },

    /// The gap is too wide to fence: fencing here would pause writes for as
    /// long as it takes to drain it.
    NotCaughtUp { pending: u64, allowed: u64 },

    /// Writes are refused while the fence is up. The caller should retry: this
    /// is a short, bounded window, not a failure.
    WriteFenced {
        shard_id: u64,
        source: DbmsId,
        destination: DbmsId,
    },

    /// A write sequence that is not past the accepting node's high-water mark.
    /// Accepting it would let a replay overwrite a newer value.
    NonMonotonicWrite {
        seq: WriteSeq,
        high_water: WriteSeq,
    },

    /// The destination has already applied this sequence. Applying it again is
    /// the double-apply this crate exists to prevent.
    WriteAlreadyApplied { seq: WriteSeq, applied: WriteSeq },

    /// The destination reported applying a write the source never accepted.
    UnknownWrite {
        seq: WriteSeq,
        high_water: Option<WriteSeq>,
    },

    /// Authority cannot move while the destination is still behind the fence.
    CutoverNotDrained { pending: u64 },

    /// Rollback was requested after authority had already moved.
    AbortAfterCutover { phase: MigrationPhase },

    /// The migration has already ended.
    AlreadyTerminal { phase: MigrationPhase },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SameSourceAndDestination { node } => write!(
                f,
                "source and destination are both '{}'",
                node.0
            ),

            Self::InvalidTransition { from, to } => write!(
                f,
                "a migration cannot go from {} to {}",
                from.label(),
                to.label()
            ),

            Self::WrongPhase { expected, actual } => write!(
                f,
                "operation requires phase {}, migration is {}",
                expected.label(),
                actual.label()
            ),

            Self::CopyIncomplete { copied, total } => write!(
                f,
                "only {copied} of {total} atoms have been copied"
            ),

            Self::ProgressExceedsTotal { copied, total } => write!(
                f,
                "reported {copied} atoms copied, the shard has {total}"
            ),

            Self::ProgressWentBackwards { reported, recorded } => write!(
                f,
                "reported {reported} atoms copied, {recorded} were already recorded"
            ),

            Self::NotCaughtUp { pending, allowed } => write!(
                f,
                "{pending} writes still unapplied, cutover allows at most {allowed}"
            ),

            Self::WriteFenced {
                shard_id,
                source,
                destination,
            } => write!(
                f,
                "shard {shard_id} is fenced for cutover from '{}' to '{}'; retry",
                source.0, destination.0
            ),

            Self::NonMonotonicWrite { seq, high_water } => write!(
                f,
                "write {seq} is not past the high-water mark {high_water}"
            ),

            Self::WriteAlreadyApplied { seq, applied } => write!(
                f,
                "write {seq} was already applied (destination is at {applied})"
            ),

            Self::UnknownWrite { seq, high_water } => match high_water {
                Some(mark) => write!(
                    f,
                    "write {seq} was never accepted by the source (high-water {mark})"
                ),
                None => write!(f, "write {seq} was never accepted by the source"),
            },

            Self::CutoverNotDrained { pending } => write!(
                f,
                "cutover cannot complete with {pending} writes still unapplied"
            ),

            Self::AbortAfterCutover { phase } => write!(
                f,
                "cannot abort in phase {}: authority has already moved",
                phase.label()
            ),

            Self::AlreadyTerminal { phase } => {
                write!(f, "migration has already {}", phase.label())
            }
        }
    }
}

impl std::error::Error for MigrationError {}

/// A migration in flight.
///
/// Nothing advances on its own. Every method is an operation a controller
/// performs after the runtime has told it that the corresponding real-world
/// work happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Migration {
    plan: MigrationPlan,
    phase: MigrationPhase,
    progress: MigrationProgress,

    /// Highest sequence the source has accepted during this migration.
    source_high_water: Option<WriteSeq>,

    /// Highest sequence the destination has applied.
    destination_applied: Option<WriteSeq>,

    fence: Option<CutoverFence>,

    /// Set exactly once, when authority moves. This — not the phase — is what
    /// decides who owns the shard, so a migration that fails during cleanup
    /// still reports the destination as the owner.
    cutover_completed_at_ms: Option<u64>,

    outcome: Option<MigrationOutcome>,
    history: Vec<PhaseTransition>,
}

impl Migration {
    pub fn new(plan: MigrationPlan) -> Self {
        let progress = MigrationProgress::new(plan.atoms, plan.created_at_ms);

        Self {
            plan,
            phase: MigrationPhase::Planned,
            progress,
            source_high_water: None,
            destination_applied: None,
            fence: None,
            cutover_completed_at_ms: None,
            outcome: None,
            history: Vec::new(),
        }
    }

    pub fn plan(&self) -> &MigrationPlan {
        &self.plan
    }

    pub fn phase(&self) -> MigrationPhase {
        self.phase
    }

    pub fn progress(&self) -> &MigrationProgress {
        &self.progress
    }

    pub fn fence(&self) -> Option<CutoverFence> {
        self.fence
    }

    pub fn outcome(&self) -> Option<&MigrationOutcome> {
        self.outcome.as_ref()
    }

    pub fn history(&self) -> &[PhaseTransition] {
        &self.history
    }

    pub fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }

    /// Sequence slots the source has accepted that the destination has not
    /// applied.
    pub fn pending_writes(&self) -> u64 {
        let high = self.source_high_water.map(|seq| seq.0);
        let applied = self.destination_applied.map(|seq| seq.0);

        match (high, applied) {
            (None, _) => 0,
            (Some(high), None) => high + 1,
            (Some(high), Some(applied)) => high.saturating_sub(applied),
        }
    }

    /// Whether authority has moved to the destination.
    pub fn has_cut_over(&self) -> bool {
        self.cutover_completed_at_ms.is_some()
    }

    /// The node that serves reads for this shard, right now.
    ///
    /// Never `None`: this is invariant 1. Before cutover completes the source
    /// holds every write and answers every read; afterwards the destination
    /// does. The destination is not a read target while it is being seeded,
    /// because a partial copy answering reads is a wrong answer.
    pub fn read_owner(&self) -> &DbmsId {
        if self.has_cut_over() {
            &self.plan.destination
        } else {
            &self.plan.source
        }
    }

    /// The node that accepts writes, or `None` while the fence is up.
    ///
    /// `None` means "pause and retry", not "unavailable forever": the fence is
    /// released by [`Migration::complete_cutover`] or [`Migration::abort`].
    pub fn write_owner(&self) -> Option<&DbmsId> {
        if self.is_write_fenced() {
            return None;
        }

        Some(self.read_owner())
    }

    pub fn is_write_fenced(&self) -> bool {
        self.phase.is_fenced()
    }

    /// Route one write, recording it against the accepting node.
    ///
    /// The monotonicity checks here are what stop a retried or replayed write
    /// from being applied twice across the handover.
    pub fn accept_write(&mut self, seq: WriteSeq) -> Result<WriteRoute, MigrationError> {
        if self.is_write_fenced() {
            return Err(MigrationError::WriteFenced {
                shard_id: self.plan.shard_id,
                source: self.plan.source.clone(),
                destination: self.plan.destination.clone(),
            });
        }

        if self.has_cut_over() {
            /*
             * Past the fence the destination owns the sequence space. A write
             * at or below the fence is one the source already accepted and the
             * destination already applied during the drain -- replaying it
             * here is the double-apply.
             */
            if let Some(fence) = self.fence
                && let Some(last) = fence.last_write
                && seq <= last
            {
                return Err(MigrationError::WriteAlreadyApplied {
                    seq,
                    applied: last,
                });
            }

            if let Some(applied) = self.destination_applied
                && seq <= applied
            {
                return Err(MigrationError::WriteAlreadyApplied { seq, applied });
            }

            self.destination_applied = Some(seq);

            return Ok(WriteRoute::Destination);
        }

        if let Some(high_water) = self.source_high_water
            && seq <= high_water
        {
            return Err(MigrationError::NonMonotonicWrite { seq, high_water });
        }

        self.source_high_water = Some(seq);
        self.progress.pending_writes = self.pending_writes();

        Ok(WriteRoute::Source)
    }

    /// Record that the destination has applied the write stream up to `seq`.
    ///
    /// Refuses a sequence the source never accepted, and refuses to go
    /// backwards — either would make the drain check meaningless.
    pub fn record_applied(
        &mut self,
        seq: WriteSeq,
        at_ms: u64,
    ) -> Result<(), MigrationError> {
        match self.source_high_water {
            Some(high_water) if seq <= high_water => {}

            high_water => {
                return Err(MigrationError::UnknownWrite { seq, high_water });
            }
        }

        if let Some(applied) = self.destination_applied
            && seq <= applied
        {
            return Err(MigrationError::WriteAlreadyApplied { seq, applied });
        }

        self.destination_applied = Some(seq);
        self.progress.pending_writes = self.pending_writes();
        self.progress.updated_at_ms = self.progress.updated_at_ms.max(at_ms);

        Ok(())
    }

    /// Start the bulk copy.
    pub fn begin_copy(&mut self, at_ms: u64) -> Result<(), MigrationError> {
        self.advance(MigrationPhase::Copying, at_ms, None)
    }

    /// Record bulk-copy progress. Cumulative, never decreasing.
    pub fn record_copy(
        &mut self,
        atoms_copied: usize,
        bytes_copied: u64,
        at_ms: u64,
    ) -> Result<(), MigrationError> {
        self.require_phase(MigrationPhase::Copying)?;

        if atoms_copied > self.progress.atoms_total {
            return Err(MigrationError::ProgressExceedsTotal {
                copied: atoms_copied,
                total: self.progress.atoms_total,
            });
        }

        if atoms_copied < self.progress.atoms_copied {
            return Err(MigrationError::ProgressWentBackwards {
                reported: atoms_copied,
                recorded: self.progress.atoms_copied,
            });
        }

        self.progress.atoms_copied = atoms_copied;
        self.progress.bytes_copied = self.progress.bytes_copied.max(bytes_copied);
        self.progress.updated_at_ms = self.progress.updated_at_ms.max(at_ms);

        Ok(())
    }

    /// Move to catching-up. Refused while any atom is still uncopied.
    pub fn begin_catch_up(&mut self, at_ms: u64) -> Result<(), MigrationError> {
        self.require_phase(MigrationPhase::Copying)?;

        if !self.progress.is_copy_complete() {
            return Err(MigrationError::CopyIncomplete {
                copied: self.progress.atoms_copied,
                total: self.progress.atoms_total,
            });
        }

        self.advance(MigrationPhase::CatchingUp, at_ms, None)
    }

    /// Throw the destination copy away and start the bulk copy again.
    ///
    /// The escape hatch for a destination that turns out to be bad after the
    /// copy finished. Safe at this point precisely because the source is still
    /// authoritative.
    pub fn restart_copy(
        &mut self,
        at_ms: u64,
        note: impl Into<String>,
    ) -> Result<(), MigrationError> {
        self.require_phase(MigrationPhase::CatchingUp)?;

        self.advance(MigrationPhase::Copying, at_ms, Some(note.into()))?;

        self.progress.atoms_copied = 0;
        self.progress.bytes_copied = 0;
        self.destination_applied = None;
        self.progress.pending_writes = self.pending_writes();

        Ok(())
    }

    /// Fence the source and enter cutover.
    ///
    /// Allowed only from catching-up, only with the copy complete, and only
    /// with the remaining gap inside the plan's budget — so the write pause
    /// that starts here is short and bounded.
    pub fn begin_cutover(&mut self, at_ms: u64) -> Result<(), MigrationError> {
        self.require_phase(MigrationPhase::CatchingUp)?;

        if !self.progress.is_copy_complete() {
            return Err(MigrationError::CopyIncomplete {
                copied: self.progress.atoms_copied,
                total: self.progress.atoms_total,
            });
        }

        let pending = self.pending_writes();

        if pending > self.plan.max_cutover_pending_writes {
            return Err(MigrationError::NotCaughtUp {
                pending,
                allowed: self.plan.max_cutover_pending_writes,
            });
        }

        self.advance(MigrationPhase::Cutover, at_ms, None)?;

        self.fence = Some(CutoverFence {
            at_ms,
            last_write: self.source_high_water,
        });

        Ok(())
    }

    /// Move authority to the destination.
    ///
    /// Refused unless the destination has applied every write up to the fence.
    /// This single check is invariant 2's "no write is lost".
    pub fn complete_cutover(&mut self, at_ms: u64) -> Result<(), MigrationError> {
        self.require_phase(MigrationPhase::Cutover)?;

        let pending = self.pending_writes();

        if pending > 0 {
            return Err(MigrationError::CutoverNotDrained { pending });
        }

        self.advance(MigrationPhase::Cleanup, at_ms, None)?;
        self.cutover_completed_at_ms = Some(at_ms);

        Ok(())
    }

    /// The source copy is released; the migration is done.
    pub fn complete(&mut self, at_ms: u64) -> Result<(), MigrationError> {
        self.require_phase(MigrationPhase::Cleanup)?;

        self.advance(MigrationPhase::Completed, at_ms, None)?;
        self.outcome = Some(MigrationOutcome::Completed { at_ms });

        Ok(())
    }

    /// Roll back. Legal in every phase up to and including an uncompleted
    /// cutover; the source keeps ownership and the destination copy is
    /// discarded.
    pub fn abort(
        &mut self,
        at_ms: u64,
        reason: impl Into<String>,
    ) -> Result<(), MigrationError> {
        if self.phase.is_terminal() {
            return Err(MigrationError::AlreadyTerminal { phase: self.phase });
        }

        if !self.phase.allows_abort() {
            return Err(MigrationError::AbortAfterCutover { phase: self.phase });
        }

        let reason = reason.into();

        self.advance(MigrationPhase::Aborted, at_ms, Some(reason.clone()))?;

        // The fence, if one was raised, comes down with the abort: the source
        // resumes accepting writes at its own high-water mark.
        self.fence = None;
        self.destination_applied = None;
        self.progress.pending_writes = 0;
        self.outcome = Some(MigrationOutcome::Aborted { at_ms, reason });

        Ok(())
    }

    /// Stop on an error. Ownership stays wherever the cutover left it, which is
    /// why [`Migration::read_owner`] still answers afterwards.
    pub fn fail(
        &mut self,
        at_ms: u64,
        error: impl Into<String>,
    ) -> Result<(), MigrationError> {
        if self.phase.is_terminal() {
            return Err(MigrationError::AlreadyTerminal { phase: self.phase });
        }

        let error = error.into();

        self.advance(MigrationPhase::Failed, at_ms, Some(error.clone()))?;
        self.outcome = Some(MigrationOutcome::Failed { at_ms, error });

        Ok(())
    }

    /// A flat snapshot for reporting or shipping over the protocol.
    pub fn status(&self, now_ms: u64) -> MigrationStatus {
        MigrationStatus {
            id: self.plan.id,
            shard_id: self.plan.shard_id,
            source: self.plan.source.clone(),
            destination: self.plan.destination.clone(),
            phase: self.phase,
            atoms_copied: self.progress.atoms_copied,
            atoms_total: self.progress.atoms_total,
            bytes_copied: self.progress.bytes_copied,
            pending_writes: self.pending_writes(),
            fraction: self.progress.overall_fraction(self.phase),
            phase_age_ms: self.progress.phase_age_ms(now_ms),
            read_owner: self.read_owner().clone(),
            write_owner: self.write_owner().cloned(),
            outcome: self.outcome.clone(),
        }
    }

    fn require_phase(&self, expected: MigrationPhase) -> Result<(), MigrationError> {
        if self.phase != expected {
            return Err(MigrationError::WrongPhase {
                expected,
                actual: self.phase,
            });
        }

        Ok(())
    }

    fn advance(
        &mut self,
        to: MigrationPhase,
        at_ms: u64,
        note: Option<String>,
    ) -> Result<(), MigrationError> {
        if self.phase.is_terminal() {
            return Err(MigrationError::AlreadyTerminal { phase: self.phase });
        }

        if !self.phase.can_advance_to(to) {
            return Err(MigrationError::InvalidTransition {
                from: self.phase,
                to,
            });
        }

        self.history.push(PhaseTransition {
            from: self.phase,
            to,
            at_ms,
            note,
        });

        self.phase = to;
        self.progress.phase_started_at_ms = at_ms;
        self.progress.updated_at_ms = self.progress.updated_at_ms.max(at_ms);

        Ok(())
    }
}

/// A flattened view of a migration, for progress reporting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationStatus {
    pub id: crate::plan::MigrationId,
    pub shard_id: u64,
    pub source: DbmsId,
    pub destination: DbmsId,
    pub phase: MigrationPhase,

    pub atoms_copied: usize,
    pub atoms_total: usize,
    pub bytes_copied: u64,
    pub pending_writes: u64,

    /// Coarse estimate; see [`MigrationProgress::overall_fraction`].
    pub fraction: f64,
    pub phase_age_ms: u64,

    pub read_owner: DbmsId,

    /// `None` while the fence is up.
    pub write_owner: Option<DbmsId>,

    pub outcome: Option<MigrationOutcome>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use fabric_core::Shard;

    use crate::plan::{MigrationId, MigrationReason};

    fn migration() -> Migration {
        let shard = Shard::new(42, "social");

        let plan = MigrationPlan::new(
            MigrationId(1),
            &shard,
            DbmsId::new("db-a"),
            DbmsId::new("db-b"),
            MigrationReason::Rebalance,
            1_000,
        )
        .expect("valid plan");

        Migration::new(plan)
    }

    /// Drive a migration to the point where the fence can go up.
    fn caught_up() -> Migration {
        let mut migration = migration();

        migration.begin_copy(2_000).unwrap();
        migration.accept_write(WriteSeq(0)).unwrap();
        migration.accept_write(WriteSeq(1)).unwrap();
        migration.record_copy(156, 4_096, 3_000).unwrap();
        migration.begin_catch_up(4_000).unwrap();
        migration.record_applied(WriteSeq(1), 4_100).unwrap();

        migration
    }

    #[test]
    fn a_plan_to_the_same_node_is_refused() {
        let shard = Shard::new(1, "social");

        let error = MigrationPlan::new(
            MigrationId(1),
            &shard,
            DbmsId::new("db-a"),
            DbmsId::new("db-a"),
            MigrationReason::Rebalance,
            0,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MigrationError::SameSourceAndDestination { .. }
        ));
    }

    #[test]
    fn reads_are_servable_in_every_phase() {
        let mut migration = migration();
        let source = DbmsId::new("db-a");
        let destination = DbmsId::new("db-b");

        assert_eq!(migration.read_owner(), &source);

        migration.begin_copy(2_000).unwrap();
        assert_eq!(migration.read_owner(), &source);

        migration.record_copy(156, 1, 3_000).unwrap();
        migration.begin_catch_up(4_000).unwrap();
        assert_eq!(migration.read_owner(), &source);

        // Even fenced, reads have an owner -- only writes pause.
        migration.begin_cutover(5_000).unwrap();
        assert_eq!(migration.read_owner(), &source);
        assert_eq!(migration.write_owner(), None);

        migration.complete_cutover(5_100).unwrap();
        assert_eq!(migration.read_owner(), &destination);
        assert_eq!(migration.write_owner(), Some(&destination));

        migration.complete(6_000).unwrap();
        assert_eq!(migration.read_owner(), &destination);
    }

    #[test]
    fn cutover_cannot_complete_while_a_write_is_undrained() {
        let mut migration = caught_up();

        // A write lands between catch-up and the fence.
        migration.accept_write(WriteSeq(2)).unwrap();
        migration.begin_cutover(5_000).unwrap();

        assert_eq!(migration.fence().unwrap().last_write, Some(WriteSeq(2)));
        assert_eq!(migration.pending_writes(), 1);

        let error = migration.complete_cutover(5_100).unwrap_err();
        assert_eq!(error, MigrationError::CutoverNotDrained { pending: 1 });
        assert!(!migration.has_cut_over());
        assert_eq!(migration.read_owner(), &DbmsId::new("db-a"));

        // Once the destination drains to the fence, authority may move.
        migration.record_applied(WriteSeq(2), 5_150).unwrap();
        migration.complete_cutover(5_200).unwrap();
        assert!(migration.has_cut_over());
    }

    #[test]
    fn a_write_is_never_applied_twice_across_the_handover() {
        let mut migration = caught_up();

        migration.begin_cutover(5_000).unwrap();
        migration.complete_cutover(5_100).unwrap();

        // A retry of a write the source already took, replayed at the
        // destination, is refused rather than applied a second time.
        let error = migration.accept_write(WriteSeq(1)).unwrap_err();
        assert_eq!(
            error,
            MigrationError::WriteAlreadyApplied {
                seq: WriteSeq(1),
                applied: WriteSeq(1),
            }
        );

        // New writes past the fence are accepted, by the destination.
        assert_eq!(
            migration.accept_write(WriteSeq(2)).unwrap(),
            WriteRoute::Destination
        );
    }

    #[test]
    fn writes_are_refused_while_the_fence_is_up() {
        let mut migration = caught_up();
        migration.begin_cutover(5_000).unwrap();

        let error = migration.accept_write(WriteSeq(2)).unwrap_err();
        assert!(matches!(error, MigrationError::WriteFenced { shard_id: 42, .. }));
    }

    #[test]
    fn a_replayed_write_to_the_source_is_refused() {
        let mut migration = migration();
        migration.begin_copy(2_000).unwrap();
        migration.accept_write(WriteSeq(7)).unwrap();

        let error = migration.accept_write(WriteSeq(7)).unwrap_err();
        assert_eq!(
            error,
            MigrationError::NonMonotonicWrite {
                seq: WriteSeq(7),
                high_water: WriteSeq(7),
            }
        );
    }

    #[test]
    fn the_destination_cannot_claim_a_write_the_source_never_took() {
        let mut migration = migration();
        migration.begin_copy(2_000).unwrap();
        migration.accept_write(WriteSeq(3)).unwrap();

        let error = migration.record_applied(WriteSeq(9), 2_500).unwrap_err();
        assert_eq!(
            error,
            MigrationError::UnknownWrite {
                seq: WriteSeq(9),
                high_water: Some(WriteSeq(3)),
            }
        );
    }

    #[test]
    fn catch_up_cannot_begin_with_atoms_missing() {
        let mut migration = migration();
        migration.begin_copy(2_000).unwrap();
        migration.record_copy(100, 1, 2_500).unwrap();

        let error = migration.begin_catch_up(3_000).unwrap_err();
        assert_eq!(
            error,
            MigrationError::CopyIncomplete {
                copied: 100,
                total: 156,
            }
        );
    }

    #[test]
    fn abort_is_available_up_to_cutover_and_never_after() {
        let mut migration = caught_up();
        migration.begin_cutover(5_000).unwrap();

        // Fenced, but authority has not moved: rollback is still free.
        migration.abort(5_050, "destination disk failed").unwrap();
        assert_eq!(migration.phase(), MigrationPhase::Aborted);
        assert_eq!(migration.read_owner(), &DbmsId::new("db-a"));
        // The fence came down with the abort.
        assert_eq!(migration.write_owner(), Some(&DbmsId::new("db-a")));

        let mut migration = caught_up();
        migration.begin_cutover(5_000).unwrap();
        migration.complete_cutover(5_100).unwrap();

        let error = migration.abort(5_200, "changed my mind").unwrap_err();
        assert_eq!(
            error,
            MigrationError::AbortAfterCutover {
                phase: MigrationPhase::Cleanup
            }
        );
    }

    #[test]
    fn a_failure_during_cleanup_leaves_the_destination_owning_the_shard() {
        let mut migration = caught_up();
        migration.begin_cutover(5_000).unwrap();
        migration.complete_cutover(5_100).unwrap();
        migration.fail(5_300, "source cleanup timed out").unwrap();

        assert_eq!(migration.phase(), MigrationPhase::Failed);
        assert_eq!(migration.read_owner(), &DbmsId::new("db-b"));
        // planned→copying→catching-up→cutover→cleanup→failed
        assert_eq!(migration.history().len(), 5);
    }

    #[test]
    fn status_reports_progress_without_advancing_anything() {
        let mut migration = migration();
        migration.begin_copy(2_000).unwrap();
        migration.record_copy(78, 2_048, 2_500).unwrap();

        let status = migration.status(3_000);

        assert_eq!(status.phase, MigrationPhase::Copying);
        assert_eq!(status.atoms_copied, 78);
        assert_eq!(status.atoms_total, 156);
        assert_eq!(status.phase_age_ms, 1_000);
        assert_eq!(status.read_owner, DbmsId::new("db-a"));
        assert!((status.fraction - 0.40).abs() < 1e-9);
        assert_eq!(migration.phase(), MigrationPhase::Copying);
    }
}
