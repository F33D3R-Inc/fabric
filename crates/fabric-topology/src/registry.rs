use std::collections::HashMap;

use fabric_core::{Coordinate, DbmsId, Shard};
use serde::{Deserialize, Serialize};

use crate::placement::Placement;

/// Registry of the Fabric's current physical topology.
///
/// This is the Fabric's map of the distributed FacetQL world.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologyRegistry {
    placements: HashMap<(u64, Coordinate), Placement>,
}

impl TopologyRegistry {
    pub fn new() -> Self {
        Self {
            placements: HashMap::new(),
        }
    }

    pub fn place(
        &mut self,
        dbms_id: DbmsId,
        shard: &Shard,
        coordinate: Coordinate,
        region: impl Into<String>,
    ) {
        let placement =
            Placement::new(dbms_id, shard, coordinate, region);

        self.placements
            .insert((shard.id, coordinate), placement);
    }

    pub fn locate(
        &self,
        shard_id: u64,
        coordinate: Coordinate,
    ) -> Option<&Placement> {
        self.placements.get(&(shard_id, coordinate))
    }

    pub fn move_coordinate(
        &mut self,
        shard_id: u64,
        coordinate: Coordinate,
        dbms_id: DbmsId,
        region: impl Into<String>,
    ) -> bool {
        if let Some(existing) = self.placements.get_mut(&(shard_id, coordinate)) {
            existing.dbms_id = dbms_id;
            existing.region = region.into();
            true
        } else {
            false
        }
    }

    pub fn remove(
        &mut self,
        shard_id: u64,
        coordinate: Coordinate,
    ) -> Option<Placement> {
        self.placements.remove(&(shard_id, coordinate))
    }

    pub fn len(&self) -> usize {
        self.placements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.placements.is_empty()
    }

    pub fn placements(&self) -> impl Iterator<Item = &Placement> {
        self.placements.values()
    }
}