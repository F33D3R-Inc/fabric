use std::collections::HashMap;

use crate::{
    AtomRecord,
    Coordinate,
};

/// Native storage for a FacetQL shard.
///
/// Coordinates are the primary logical addresses.
/// Records are stored independently of physical DBMS placement.
#[derive(Debug, Default)]
pub struct ShardStorage {
    records: HashMap<Coordinate, AtomRecord>,
}

impl ShardStorage {
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        record: AtomRecord,
    ) -> Option<AtomRecord> {
        let coordinate = record.coordinate();

        self.records.insert(
            coordinate,
            record,
        )
    }

    pub fn get(
        &self,
        coordinate: Coordinate,
    ) -> Option<&AtomRecord> {
        self.records.get(&coordinate)
    }

    pub fn get_mut(
        &mut self,
        coordinate: Coordinate,
    ) -> Option<&mut AtomRecord> {
        self.records.get_mut(&coordinate)
    }

    pub fn remove(
        &mut self,
        coordinate: Coordinate,
    ) -> Option<AtomRecord> {
        self.records.remove(&coordinate)
    }

    pub fn contains(
        &self,
        coordinate: Coordinate,
    ) -> bool {
        self.records.contains_key(&coordinate)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn records(
        &self,
    ) -> impl Iterator<Item = &AtomRecord> {
        self.records.values()
    }
}