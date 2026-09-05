//! What a lookup asks for, and what it gets back.

use fabric_core::{Coordinate, DbmsId};
use serde::{Deserialize, Serialize};

use crate::key::RoutingKey;

/// Which copy a read may be served from.
///
/// This is the caller's requirement, not a policy this crate invents: a
/// read-your-writes request asks for [`ReadPreference::Primary`], a dashboard
/// query can accept [`ReadPreference::AnyCopy`], and the routing table simply
/// honours what it is asked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadPreference {
    /// The write-side copy only. The only preference that guarantees a read
    /// sees the caller's own most recent write.
    Primary,

    /// Any copy that is in sync.
    AnyFresh,

    /// Any in-sync copy, ranking copies in `region` first.
    PreferRegion { region: String },

    /// Any copy holding a complete copy, including one known to be lagging.
    /// The caller is accepting stale data.
    AnyCopy,
}

impl ReadPreference {
    /// Whether a lagging copy satisfies this preference.
    pub fn allows_stale(&self) -> bool {
        matches!(self, Self::AnyCopy)
    }
}

/// Read or write, with the read's freshness requirement attached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouteIntent {
    Read(ReadPreference),
    Write,
}

impl RouteIntent {
    /// A read from the primary — the safe default when a caller has no
    /// staleness opinion.
    pub fn read() -> Self {
        Self::Read(ReadPreference::Primary)
    }

    pub fn kind(&self) -> RouteKind {
        match self {
            Self::Read(_) => RouteKind::Read,
            Self::Write => RouteKind::Write,
        }
    }
}

/// The operation a route was resolved for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouteKind {
    Read,
    Write,
}

/// Why this node is the answer.
///
/// Kept on the route because the caller frequently needs to know: a read served
/// by [`ServedBy::MigrationSource`] is about to move, and a client that caches
/// routes should treat it as short-lived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServedBy {
    /// The primary of the shard's replica set.
    Primary,

    /// An in-sync (or, for a stale read, complete) secondary.
    Secondary,

    /// The shard's placement in topology. No replica set is registered, so
    /// there is exactly one copy and it serves everything.
    ShardOwner,

    /// The node a shard is migrating away from — still authoritative.
    MigrationSource,

    /// The node a shard has migrated to, after cutover completed.
    MigrationDestination,
}

/// One resolved answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    pub shard_id: u64,

    /// The cell this route was resolved for; `None` for a whole-shard route.
    pub coordinate: Option<Coordinate>,

    pub node: DbmsId,
    pub region: String,
    pub served_by: ServedBy,
    pub kind: RouteKind,

    /// The routing table's generation when this answer was produced.
    ///
    /// A client may cache a route, but it must re-resolve when the table's
    /// generation moves past the one it holds: placement changed underneath it.
    pub generation: u64,
}

/// One lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteRequest {
    pub key: RoutingKey,
    pub intent: RouteIntent,
}

impl RouteRequest {
    pub fn read(key: RoutingKey, preference: ReadPreference) -> Self {
        Self {
            key,
            intent: RouteIntent::Read(preference),
        }
    }

    pub fn write(key: RoutingKey) -> Self {
        Self {
            key,
            intent: RouteIntent::Write,
        }
    }
}
