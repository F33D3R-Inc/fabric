//! Why a lookup could not produce a route.
//!
//! Every variant names what was tried, because "unroutable" on its own is
//! useless to the caller that has to decide between retrying, failing the
//! request, and paging someone.

use fabric_core::{Coordinate, DbmsId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingError {
    /// The table has never been told about this shard.
    UnknownShard { shard_id: u64 },

    /// Outside the 12x13 grid.
    InvalidCoordinate { x: u8, y: u8 },

    /// An address string that does not parse. See [`RoutingKey`](crate::RoutingKey).
    InvalidAddress { address: String, reason: String },

    /// A start after its end, in grid order.
    InvalidRange { start: Coordinate, end: Coordinate },

    /// The placement names a node the table has never seen. The topology and
    /// the node inventory disagree — a control-plane bug, not a data problem.
    UnknownNode { node: DbmsId },

    /// Owners exist on paper, but not one of them is fit to serve. This is the
    /// "no live owner" case: the shard is not lost, it is unreachable.
    NoLiveOwner {
        shard_id: u64,
        considered: Vec<DbmsId>,
    },

    /// The replica set has no in-sync primary, so there is nowhere a write can
    /// safely go. A failover decision is owed here.
    NoWritableReplica { shard_id: u64 },

    /// No copy is fresh enough for the requested read preference.
    NoReadableReplica { shard_id: u64 },

    /// A cutover is in flight: writes are paused for a bounded window.
    /// **Retry** — this is not a failure.
    WriteFenced {
        shard_id: u64,
        source: DbmsId,
        destination: DbmsId,
    },
}

impl RoutingError {
    /// Whether the caller should retry shortly rather than fail the request.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::WriteFenced { .. })
    }
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownShard { shard_id } => {
                write!(f, "shard {shard_id} is not in the routing table")
            }

            Self::InvalidCoordinate { x, y } => write!(
                f,
                "coordinate ({x},{y}) is outside the 12x13 grid"
            ),

            Self::InvalidAddress { address, reason } => {
                write!(f, "cannot route address '{address}': {reason}")
            }

            Self::InvalidRange { start, end } => write!(
                f,
                "range ({},{}) to ({},{}) runs backwards",
                start.x, start.y, end.x, end.y
            ),

            Self::UnknownNode { node } => write!(
                f,
                "node '{}' holds a placement but is not in the routing table",
                node.0
            ),

            Self::NoLiveOwner {
                shard_id,
                considered,
            } => {
                let names: Vec<&str> = considered
                    .iter()
                    .map(|node| node.0.as_str())
                    .collect();

                write!(
                    f,
                    "shard {shard_id} has no live owner (considered: {})",
                    names.join(", ")
                )
            }

            Self::NoWritableReplica { shard_id } => write!(
                f,
                "shard {shard_id} has no in-sync primary to accept writes"
            ),

            Self::NoReadableReplica { shard_id } => write!(
                f,
                "shard {shard_id} has no copy fresh enough for this read"
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
        }
    }
}

impl std::error::Error for RoutingError {}
