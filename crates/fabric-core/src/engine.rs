use crate::{
    AtomRecord,
    Coordinate,
    Shard,
    Value,
};

/// Native FacetQL storage engine for a shard.
///
/// Coordinates are the logical addresses used for all data access.
#[derive(Debug)]
pub struct FacetEngine {
    shard: Shard,
}

impl FacetEngine {
    pub fn new(shard: Shard) -> Self {
        Self { shard }
    }

    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    pub fn shard_mut(&mut self) -> &mut Shard {
        &mut self.shard
    }

    pub fn write(
        &mut self,
        coordinate: Coordinate,
        value: Value,
    ) -> Result<(), String> {
        if !coordinate.is_valid() {
            return Err(
                "invalid FacetQL coordinate".to_string()
            );
        }

        let atom = self
            .shard
            .atom(coordinate)
            .ok_or_else(|| {
                "atom does not exist".to_string()
            })?
            .clone();

        let record = AtomRecord::new(
            atom,
            value,
        );

        self.shard
            .storage_mut()
            .insert(record);

        Ok(())
    }

    pub fn read(
        &self,
        coordinate: Coordinate,
    ) -> Option<&Value> {
        self.shard
            .storage()
            .get(coordinate)
            .map(|record| record.value())
    }

    pub fn delete(
        &mut self,
        coordinate: Coordinate,
    ) -> bool {
        self.shard
            .storage_mut()
            .remove(coordinate)
            .is_some()
    }

    pub fn contains(
        &self,
        coordinate: Coordinate,
    ) -> bool {
        self.shard
            .storage()
            .contains(coordinate)
    }

    pub fn len(&self) -> usize {
        self.shard
            .storage()
            .len()
    }
}