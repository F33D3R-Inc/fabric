//! Moving a shard from one DBMS node to another without losing a write or
//! breaking a read.
//!
//! A migration is a plan plus a state machine. The machine holds two
//! invariants, and every operation on it is checked against them:
//!
//! 1. **A read is always servable.** At every phase, including the moment of
//!    cutover, exactly one node is the authoritative holder of a complete copy
//!    — the source until cutover completes, the destination afterwards. There
//!    is no phase in which the answer is "nobody".
//! 2. **A write is neither lost nor applied twice.** Writes carry a monotonic
//!    [`WriteSeq`]. Before cutover they go to the source; cutover fences the
//!    source at a known sequence and refuses new writes until the destination
//!    has applied every sequence up to the fence; afterwards the destination
//!    accepts only sequences past the fence. A replayed write is rejected, not
//!    re-applied.
//!
//! Everything here is pure logic. Copying bytes, streaming the tail of the
//! write stream, and deleting the source copy are the runtime's work; this
//! crate says what is allowed to happen next and refuses what is not.
//!
//! The crate decides no policy: it never picks a destination, never starts
//! itself, and never advances a phase on its own. The controller drives it,
//! usually from an optimizer decision (`OptimizationAction::Move`), which it
//! records here as a [`MigrationReason`].

pub mod migration;
pub mod phase;
pub mod plan;
pub mod progress;

pub use migration::{
    CutoverFence,
    Migration,
    MigrationError,
    MigrationOutcome,
    MigrationStatus,
    WriteRoute,
    WriteSeq,
};

pub use phase::{
    MigrationPhase,
    PhaseTransition,
};

pub use plan::{
    MigrationId,
    MigrationPlan,
    MigrationReason,
    DEFAULT_MAX_CUTOVER_PENDING_WRITES,
};

pub use progress::MigrationProgress;
