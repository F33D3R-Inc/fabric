//! The logical identity being routed.
//!
//! A key is a shard id plus a grid coordinate. It carries no placement
//! information whatsoever — that is the point. The same key routes to a
//! different node tomorrow without changing a character.

use std::str::FromStr;

use fabric_core::Coordinate;
use fabric_topology::Placement;
use serde::{Deserialize, Serialize};

use crate::error::RoutingError;

/// A logical address: shard, and a cell in its 12x13 grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoutingKey {
    pub shard_id: u64,
    pub coordinate: Coordinate,
}

impl RoutingKey {
    /// Build a key, rejecting a coordinate outside the grid.
    pub fn new(shard_id: u64, coordinate: Coordinate) -> Result<Self, RoutingError> {
        if !coordinate.is_valid() {
            return Err(RoutingError::InvalidCoordinate {
                x: coordinate.x,
                y: coordinate.y,
            });
        }

        Ok(Self {
            shard_id,
            coordinate,
        })
    }

    /// The key a topology placement is about.
    ///
    /// Note the direction: a [`Placement`] says *where* a key currently lives,
    /// so it contains a key — a key never contains a placement.
    pub fn from_placement(placement: &Placement) -> Result<Self, RoutingError> {
        Self::new(placement.shard_id, placement.coordinate)
    }

    /// Position of the cell within the shard's grid, 0..156.
    pub fn index(&self) -> usize {
        self.coordinate.index()
    }
}

impl std::fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{},{}",
            self.shard_id, self.coordinate.x, self.coordinate.y
        )
    }
}

/// Parses the textual address form `<shard>/<x>,<y>` — for example `42/7,3`.
impl FromStr for RoutingKey {
    type Err = RoutingError;

    fn from_str(address: &str) -> Result<Self, Self::Err> {
        let invalid = |reason: &str| RoutingError::InvalidAddress {
            address: address.to_string(),
            reason: reason.to_string(),
        };

        let (shard, cell) = address
            .split_once('/')
            .ok_or_else(|| invalid("expected '<shard>/<x>,<y>'"))?;

        let (x, y) = cell
            .split_once(',')
            .ok_or_else(|| invalid("expected '<x>,<y>' after the shard"))?;

        let shard_id: u64 = shard
            .trim()
            .parse()
            .map_err(|_| invalid("shard id is not a number"))?;

        let x: u8 = x
            .trim()
            .parse()
            .map_err(|_| invalid("x is not a number"))?;

        let y: u8 = y
            .trim()
            .parse()
            .map_err(|_| invalid("y is not a number"))?;

        Self::new(shard_id, Coordinate::new(x, y))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_round_trips() {
        let key: RoutingKey = "42/7,3".parse().unwrap();

        assert_eq!(key.shard_id, 42);
        assert_eq!(key.coordinate, Coordinate::new(7, 3));
        assert_eq!(key.to_string(), "42/7,3");
    }

    #[test]
    fn an_off_grid_coordinate_is_refused() {
        let error = "42/12,0".parse::<RoutingKey>().unwrap_err();

        assert_eq!(error, RoutingError::InvalidCoordinate { x: 12, y: 0 });
    }

    #[test]
    fn a_malformed_address_says_what_was_expected() {
        let error = "42".parse::<RoutingKey>().unwrap_err();

        assert!(matches!(error, RoutingError::InvalidAddress { .. }));
    }
}
