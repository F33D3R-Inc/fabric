//! The phases of a migration and the edges between them.

use serde::{Deserialize, Serialize};

/// Where a migration has got to.
///
/// ```text
/// planned ──▶ copying ──▶ catching-up ──▶ cutover ──▶ cleanup ──▶ completed
///    │           │        │      ▲                       │
///    │           │        └──────┘ (restart the copy)    │
///    ▼           ▼        ▼        ▼                     ▼
///  aborted    aborted   aborted  aborted               failed
/// ```
///
/// Abort is available up to and including cutover, because until cutover
/// *completes* the source still holds every write and reverting costs nothing
/// but the copy. Once cutover completes, authority has moved: going back is a
/// new migration in the other direction, not a rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Decided, not started. Nothing has been touched.
    Planned,

    /// Bulk-copying the shard's atoms to the destination.
    Copying,

    /// Bulk copy done; the destination is applying the tail of the write
    /// stream to close the gap.
    CatchingUp,

    /// The source is fenced. No writes are accepted anywhere until the
    /// destination has drained to the fence.
    Cutover,

    /// The destination is authoritative. The source copy is being released.
    Cleanup,

    /// Done. The destination owns the shard.
    Completed,

    /// Rolled back before authority moved. The source still owns the shard.
    Aborted,

    /// Stopped by an error. Ownership is wherever the cutover left it.
    Failed,
}

impl MigrationPhase {
    /// Whether the machine permits `self -> next`.
    pub fn can_advance_to(self, next: Self) -> bool {
        use MigrationPhase::*;

        match (self, next) {
            (Planned, Copying) => true,
            (Copying, CatchingUp) => true,
            // A destination copy found to be bad is re-copied rather than
            // cut over to.
            (CatchingUp, Cutover | Copying) => true,
            (Cutover, Cleanup) => true,
            (Cleanup, Completed) => true,

            (_, Aborted) => self.allows_abort(),
            (_, Failed) => !self.is_terminal(),

            _ => false,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted | Self::Failed)
    }

    /// Whether authority still rests with the source in this phase.
    pub fn is_pre_cutover(self) -> bool {
        matches!(
            self,
            Self::Planned | Self::Copying | Self::CatchingUp | Self::Cutover
        )
    }

    /// Whether the migration can still be rolled back from here.
    pub fn allows_abort(self) -> bool {
        self.is_pre_cutover()
    }

    /// Whether writes are refused everywhere in this phase.
    pub fn is_fenced(self) -> bool {
        matches!(self, Self::Cutover)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Copying => "copying",
            Self::CatchingUp => "catching-up",
            Self::Cutover => "cutover",
            Self::Cleanup => "cleanup",
            Self::Completed => "completed",
            Self::Aborted => "aborted",
            Self::Failed => "failed",
        }
    }
}

/// One phase change that happened, with the caller's timestamp.
///
/// The sequence of these is the migration's audit trail: what happened, when,
/// and why it stopped if it stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseTransition {
    pub from: MigrationPhase,
    pub to: MigrationPhase,
    pub at_ms: u64,
    pub note: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutover_cannot_be_skipped() {
        assert!(!MigrationPhase::Copying.can_advance_to(MigrationPhase::Cleanup));
        assert!(!MigrationPhase::CatchingUp.can_advance_to(MigrationPhase::Cleanup));
        assert!(!MigrationPhase::Planned.can_advance_to(MigrationPhase::Completed));
    }

    #[test]
    fn abort_stops_being_available_once_authority_has_moved() {
        assert!(MigrationPhase::Cutover.can_advance_to(MigrationPhase::Aborted));
        assert!(!MigrationPhase::Cleanup.can_advance_to(MigrationPhase::Aborted));
        assert!(!MigrationPhase::Completed.can_advance_to(MigrationPhase::Aborted));
    }

    #[test]
    fn a_terminal_phase_goes_nowhere() {
        for phase in [
            MigrationPhase::Completed,
            MigrationPhase::Aborted,
            MigrationPhase::Failed,
        ] {
            assert!(phase.is_terminal());
            assert!(!phase.can_advance_to(MigrationPhase::Failed));
            assert!(!phase.can_advance_to(MigrationPhase::Copying));
        }
    }
}
