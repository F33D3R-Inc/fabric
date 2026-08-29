use std::collections::HashMap;

use fabric_core::{Coordinate, DbmsId};
use fabric_telemetry::Observation;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocationKey {
    pub dbms_id: DbmsId,
    pub shard_id: u64,
    pub coordinate: Coordinate,
}

impl LocationKey {
    pub fn new(
        dbms_id: DbmsId,
        shard_id: u64,
        coordinate: Coordinate,
    ) -> Self {
        Self {
            dbms_id,
            shard_id,
            coordinate,
        }
    }
}

#[derive(Debug, Default)]
pub struct FabricState {
    latest: HashMap<LocationKey, Observation>,
}

impl FabricState {
    pub fn new() -> Self {
        Self {
            latest: HashMap::new(),
        }
    }

    pub fn record(
        &mut self,
        dbms_id: DbmsId,
        observation: Observation,
    ) {
        let key = LocationKey::new(
            dbms_id,
            observation.shard.id,
            observation.coordinate,
        );

        self.latest.insert(key, observation);
    }

    pub fn latest(
        &self,
        dbms_id: &DbmsId,
        shard_id: u64,
        coordinate: Coordinate,
    ) -> Option<&Observation> {
        self.latest.get(
            &LocationKey::new(
                dbms_id.clone(),
                shard_id,
                coordinate,
            )
        )
    }

    pub fn len(&self) -> usize {
        self.latest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.latest.is_empty()
    }

    pub fn observations(
        &self,
    ) -> impl Iterator<Item = &Observation> {
        self.latest.values()
    }
}