use serde::{Deserialize, Serialize};

use crate::coordinate::Coordinate;

pub const GRID_WIDTH: u8 = 12;
pub const GRID_HEIGHT: u8 = 13;
pub const GRID_CELLS: usize = 156;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    pub coordinate: Coordinate,
}

impl Cell {
    pub fn new(coordinate: Coordinate) -> Self {
        assert!(coordinate.is_valid(), "invalid FacetQL coordinate");
        Self { coordinate }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grid {
    cells: Vec<Cell>,
}

impl Grid {
    pub fn new() -> Self {
        let mut cells = Vec::with_capacity(GRID_CELLS);

        for y in 0..GRID_HEIGHT {
            for x in 0..GRID_WIDTH {
                cells.push(Cell::new(Coordinate::new(x, y)));
            }
        }

        Self { cells }
    }

    pub fn cell(&self, coordinate: Coordinate) -> Option<&Cell> {
        if !coordinate.is_valid() {
            return None;
        }

        self.cells.get(coordinate.index())
    }

    pub fn cells(&self) -> &[Cell] {
        &self.cells
    }

    pub const fn len(&self) -> usize {
        GRID_CELLS
    }

    pub const fn is_empty(&self) -> bool {
        false
    }
}

impl Default for Grid {
    fn default() -> Self {
        Self::new()
    }
}