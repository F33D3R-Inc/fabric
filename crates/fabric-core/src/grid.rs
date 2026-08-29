use serde::{Deserialize, Serialize};

use crate::{
    atom::Atom,
    coordinate::Coordinate,
};

pub const GRID_WIDTH: u8 = 12;
pub const GRID_HEIGHT: u8 = 13;
pub const GRID_ATOMS: usize = 156;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grid {
    atoms: Vec<Atom>,
}

impl Grid {
    pub fn new() -> Self {
        let mut atoms = Vec::with_capacity(GRID_ATOMS);

        for y in 0..GRID_HEIGHT {
            for x in 0..GRID_WIDTH {
                let coordinate = Coordinate::new(x, y);

                atoms.push(
                    Atom::new(
                        format!("atom-{}", coordinate.index()),
                        coordinate,
                    )
                );
            }
        }

        Self { atoms }
    }

    pub fn atom(
        &self,
        coordinate: Coordinate,
    ) -> Option<&Atom> {
        if !coordinate.is_valid() {
            return None;
        }

        self.atoms.get(
            coordinate.index()
        )
    }

    pub fn atoms(&self) -> &[Atom] {
        &self.atoms
    }

    pub const fn len(&self) -> usize {
        GRID_ATOMS
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