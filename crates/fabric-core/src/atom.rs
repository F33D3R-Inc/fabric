use serde::{Deserialize, Serialize};

use crate::coordinate::Coordinate;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Atom {
    pub id: String,
    pub coordinate: Coordinate,
}

impl Atom {
    pub fn new(id: impl Into<String>, coordinate: Coordinate) -> Self {
        Self {
            id: id.into(),
            coordinate,
        }
    }
}