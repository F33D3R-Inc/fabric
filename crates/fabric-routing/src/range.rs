//! Ranges and scans over a shard's grid.
//!
//! A range query does not have one answer: coordinate-level placement means
//! different parts of the same grid can live on different nodes. Resolution
//! therefore returns *segments* — contiguous runs of the grid that share a
//! node — so the caller issues one request per node rather than 156 lookups.

use fabric_core::{Coordinate, GRID_ATOMS, GRID_WIDTH};
use serde::{Deserialize, Serialize};

use crate::{error::RoutingError, route::Route};

/// An inclusive range of cells in grid order (row-major, the same order as
/// [`Coordinate::index`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinateRange {
    start: Coordinate,
    end: Coordinate,
}

impl CoordinateRange {
    pub fn new(start: Coordinate, end: Coordinate) -> Result<Self, RoutingError> {
        for coordinate in [start, end] {
            if !coordinate.is_valid() {
                return Err(RoutingError::InvalidCoordinate {
                    x: coordinate.x,
                    y: coordinate.y,
                });
            }
        }

        if start.index() > end.index() {
            return Err(RoutingError::InvalidRange { start, end });
        }

        Ok(Self { start, end })
    }

    /// Every cell of a shard: a full scan.
    pub fn full() -> Self {
        Self {
            start: Coordinate::new(0, 0),
            end: coordinate_at(GRID_ATOMS - 1),
        }
    }

    pub fn start(&self) -> Coordinate {
        self.start
    }

    pub fn end(&self) -> Coordinate {
        self.end
    }

    pub fn len(&self) -> usize {
        self.end.index() - self.start.index() + 1
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn contains(&self, coordinate: Coordinate) -> bool {
        coordinate.is_valid()
            && coordinate.index() >= self.start.index()
            && coordinate.index() <= self.end.index()
    }

    pub fn coordinates(&self) -> impl Iterator<Item = Coordinate> {
        (self.start.index()..=self.end.index()).map(coordinate_at)
    }
}

/// The coordinate at a grid index. The inverse of [`Coordinate::index`].
pub fn coordinate_at(index: usize) -> Coordinate {
    let width = GRID_WIDTH as usize;

    Coordinate::new((index % width) as u8, (index / width) as u8)
}

/// A run of consecutive cells served by one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteSegment {
    pub range: CoordinateRange,
    pub route: Route,
}

impl RouteSegment {
    /// How many cells this segment covers.
    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_range_covers_every_atom_of_the_grid() {
        let range = CoordinateRange::full();

        assert_eq!(range.len(), GRID_ATOMS);
        assert_eq!(range.coordinates().count(), GRID_ATOMS);
        assert_eq!(range.end(), Coordinate::new(11, 12));
    }

    #[test]
    fn indexing_round_trips_through_the_grid() {
        for index in 0..GRID_ATOMS {
            assert_eq!(coordinate_at(index).index(), index);
        }
    }

    #[test]
    fn a_backwards_range_is_refused() {
        let error = CoordinateRange::new(Coordinate::new(5, 5), Coordinate::new(0, 0))
            .unwrap_err();

        assert!(matches!(error, RoutingError::InvalidRange { .. }));
    }
}
