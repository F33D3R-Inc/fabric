use serde::{Deserialize, Serialize};

use crate::{
    atom::Atom,
    coordinate::Coordinate,
    grid::Grid,
    storage::ShardStorage,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Shard {
    pub id: u64,
    pub workload_domain: String,

    #[serde(skip)]
    grid: Grid,

    #[serde(skip)]
    storage: ShardStorage,
}

impl Clone for Shard {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            workload_domain: self.workload_domain.clone(),
            grid: Grid::new(),
            storage: ShardStorage::new(),
        }
    }
}

impl PartialEq for Shard {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.workload_domain == other.workload_domain
    }
}

impl Eq for Shard {}

impl std::hash::Hash for Shard {
    fn hash<H: std::hash::Hasher>(
        &self,
        state: &mut H,
    ) {
        self.id.hash(state);
        self.workload_domain.hash(state);
    }
}

impl Shard {
    pub fn new(
        id: u64,
        workload_domain: impl Into<String>,
    ) -> Self {
        Self {
            id,
            workload_domain: workload_domain.into(),
            grid: Grid::new(),
            storage: ShardStorage::new(),
        }
    }

    pub fn grid(&self) -> &Grid {
        &self.grid
    }

    pub fn storage(&self) -> &ShardStorage {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut ShardStorage {
        &mut self.storage
    }

    pub fn contains(
        &self,
        coordinate: Coordinate,
    ) -> bool {
        coordinate.is_valid()
    }

    pub fn atom(
        &self,
        coordinate: Coordinate,
    ) -> Option<&Atom> {
        self.grid.atom(coordinate)
    }

    pub const fn atom_count(&self) -> usize {
        156
    }
}