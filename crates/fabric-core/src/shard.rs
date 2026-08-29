use serde::{Deserialize, Serialize};

use crate::grid::Grid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Shard {
    pub id: u64,
    pub workload_domain: String,
}

impl Shard {
    pub fn new(id: u64, workload_domain: impl Into<String>) -> Self {
        Self {
            id,
            workload_domain: workload_domain.into(),
        }
    }

    pub fn grid(&self) -> Grid {
        Grid::new()
    }
}