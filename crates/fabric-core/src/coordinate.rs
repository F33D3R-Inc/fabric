use serde::{Deserialize, Serialize};

/// A logical location inside a FacetQL grid.
///
/// Coordinates are logical identifiers. The physical machine holding
/// the data may change without changing the coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Coordinate {
    pub x: u8,
    pub y: u8,
}

impl Coordinate {
    pub const fn new(x: u8, y: u8) -> Self {
        Self { x, y }
    }

    pub fn is_valid(&self) -> bool {
        self.x < 12 && self.y < 13
    }

    pub fn index(&self) -> usize {
        (self.y as usize * 12) + self.x as usize
    }
}