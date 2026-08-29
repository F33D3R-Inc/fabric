use std::collections::HashMap;

use fabric_core::{Coordinate, Shard};
use fabric_telemetry::Observation;

use crate::profile::WorkloadProfile;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkloadKey {
    pub shard_id: u64,
    pub coordinate: Coordinate,
}

impl WorkloadKey {
    pub fn new(shard_id: u64, coordinate: Coordinate) -> Self {
        Self {
            shard_id,
            coordinate,
        }
    }
}

#[derive(Debug, Default)]
pub struct WorkloadAnalyzer {
    profiles: HashMap<WorkloadKey, WorkloadProfile>,
}

impl WorkloadAnalyzer {
    pub fn new() -> Self {
        Self {
            profiles: HashMap::new(),
        }
    }

    pub fn observe(&mut self, observation: &Observation) {
        let key = WorkloadKey::new(
            observation.shard.id,
            observation.coordinate,
        );

        let profile = WorkloadProfile::from_metrics(
            observation.coordinate,
            observation.metrics,
        );

        self.profiles.insert(key, profile);
    }

    pub fn profile(
        &self,
        shard: &Shard,
        coordinate: Coordinate,
    ) -> Option<&WorkloadProfile> {
        self.profiles.get(
            &WorkloadKey::new(shard.id, coordinate)
        )
    }

    pub fn hot_coordinates(&self) -> Vec<Coordinate> {
        self.profiles
            .values()
            .filter(|profile| profile.is_hot())
            .map(|profile| profile.coordinate)
            .collect()
    }

    pub fn profiles(
        &self,
    ) -> impl Iterator<Item = &WorkloadProfile> {
        self.profiles.values()
    }

    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
}