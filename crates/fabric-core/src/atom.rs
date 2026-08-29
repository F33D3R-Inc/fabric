use serde::{Deserialize, Serialize};

use crate::coordinate::Coordinate;

/// The smallest addressable unit in FacetQL.
///
/// An atom has a stable logical identity and a coordinate inside
/// its parent shard's 12x13 grid.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Atom {
    pub id: String,
    pub coordinate: Coordinate,
}

impl Atom {
    pub fn new(
        id: impl Into<String>,
        coordinate: Coordinate,
    ) -> Self {
        assert!(
            coordinate.is_valid(),
            "invalid FacetQL atom coordinate"
        );

        Self {
            id: id.into(),
            coordinate,
        }
    }

    pub fn index(&self) -> usize {
        self.coordinate.index()
    }
}