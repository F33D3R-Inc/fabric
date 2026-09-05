//! How far along a migration is, for reporting.

use serde::{Deserialize, Serialize};

use crate::phase::MigrationPhase;

/// Counters describing work done. Advanced only by the runtime reporting
/// actual progress; never estimated forward by this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub atoms_total: usize,
    pub atoms_copied: usize,
    pub bytes_copied: u64,

    /// Writes the source has accepted that the destination has not applied.
    pub pending_writes: u64,

    /// When the current phase began.
    pub phase_started_at_ms: u64,

    /// Timestamp of the newest progress report folded in.
    pub updated_at_ms: u64,
}

impl MigrationProgress {
    pub fn new(atoms_total: usize, started_at_ms: u64) -> Self {
        Self {
            atoms_total,
            atoms_copied: 0,
            bytes_copied: 0,
            pending_writes: 0,
            phase_started_at_ms: started_at_ms,
            updated_at_ms: started_at_ms,
        }
    }

    pub fn copy_fraction(&self) -> f64 {
        if self.atoms_total == 0 {
            return 1.0;
        }

        self.atoms_copied as f64 / self.atoms_total as f64
    }

    pub fn is_copy_complete(&self) -> bool {
        self.atoms_copied >= self.atoms_total
    }

    /// A coarse overall fraction for progress bars.
    ///
    /// Explicitly an estimate: the phases after the bulk copy are short but
    /// not instant, so they get a fixed share rather than a measured one. Never
    /// use this to decide anything — the phase and the counters are the truth.
    pub fn overall_fraction(&self, phase: MigrationPhase) -> f64 {
        match phase {
            MigrationPhase::Planned => 0.0,
            MigrationPhase::Copying => self.copy_fraction() * 0.80,
            MigrationPhase::CatchingUp => 0.85,
            MigrationPhase::Cutover => 0.95,
            MigrationPhase::Cleanup => 0.98,
            MigrationPhase::Completed => 1.0,
            MigrationPhase::Aborted | MigrationPhase::Failed => {
                self.copy_fraction() * 0.80
            }
        }
    }

    /// How long the current phase has been running at `now_ms`.
    pub fn phase_age_ms(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.phase_started_at_ms)
    }
}
