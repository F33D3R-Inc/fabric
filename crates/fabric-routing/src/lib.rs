//! Where a logical identity physically lives *right now*.
//!
//! Routing is a **lookup against topology state**, never a computation over the
//! identity. There is no hash of a coordinate to a node anywhere in this crate,
//! and that is deliberate: README §2 says logical identity is independent of
//! physical placement, and a hash-derived placement would weld them together —
//! moving a shard would mean changing its address. Here, moving a shard means
//! updating one entry in a [`RoutingTable`], and every identity keeps its name.
//!
//! What the crate answers:
//!
//! * one key → the node that should serve it, for a **read** or for a **write**
//!   (they differ as soon as replicas exist);
//! * a coordinate **range or scan** → the segments of it that each node serves;
//! * a shard **mid-migration** → the node that is still authoritative, and an
//!   explicit "fenced, retry" while a cutover is in flight, rather than a wrong
//!   answer.
//!
//! All of it is answered **per cell**. Replica sets and migrations are
//! published at a scope — the whole shard, or one coordinate of it — because
//! the rest of the Fabric places, replicates and moves one cell at a time, and
//! an answer given for a whole shard when only one of its cells moved sends
//! every sibling's reads to a node that never held their data. A shard-wide
//! publication still means every cell; a cell-scoped one overrides it for that
//! cell and touches no other.
//!
//! What it does not do: pick placements, balance load across the candidates it
//! returns, or change any state of its own. It is fed — topology from
//! [`fabric_topology`], replica sets from [`fabric_replication`], migrations
//! from [`fabric_migration`], node health from the controller — and it answers.

pub mod error;
pub mod key;
pub mod range;
pub mod route;
pub mod table;

pub use error::RoutingError;

pub use key::RoutingKey;

pub use range::{
    CoordinateRange,
    RouteSegment,
};

pub use route::{
    ReadPreference,
    Route,
    RouteIntent,
    RouteKind,
    RouteRequest,
    ServedBy,
};

pub use table::{
    NodeAvailability,
    NodeEntry,
    RoutingTable,
    ShardEntry,
    ShardMigration,
};
