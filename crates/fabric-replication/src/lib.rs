//! Replica placement and the lifecycle of a single shard replica.
//!
//! This crate answers two questions and refuses to answer a third:
//!
//! * *Where should the copies of a shard live?* — [`placement`].
//! * *What state is each copy in, and is it safe to read or write?* —
//!   [`replica`] and [`set`].
//! * *Should we fail over right now?* — **not answered here.** [`failover`]
//!   assembles the inputs and names the operation; the controller decides
//!   whether to apply it. There is no loop in this crate, no clock of its own,
//!   and no autonomous mutation: every state change is either an explicit
//!   [`ReplicaOperation`] applied by a caller, or the mechanical consequence of
//!   a health report the caller fed in.
//!
//! The crate is pure logic — no I/O. Copying bytes between nodes belongs to
//! the runtime; this is the bookkeeping that decides whether that copy may be
//! used.

pub mod failover;
pub mod placement;
pub mod replica;
pub mod set;

pub use failover::{
    FailoverAssessment,
    FailoverBlocker,
    FailoverPolicy,
    Ineligibility,
    PromotionCandidate,
};

pub use placement::{
    PlacementCandidate,
    PlacementError,
    PlacementPlan,
    PlacementRule,
    ReplicaPlacement,
    ReplicaPlacementPlanner,
    ReplicationFactor,
    SpreadPolicy,
    DEFAULT_REPLICATION_FACTOR,
};

pub use replica::{
    LagThresholds,
    Replica,
    ReplicaLag,
    ReplicaReport,
    ReplicaRole,
    ReplicaState,
    ReplicaTransition,
};

pub use set::{
    ReplicaOperation,
    ReplicaSet,
    ReplicationError,
};
