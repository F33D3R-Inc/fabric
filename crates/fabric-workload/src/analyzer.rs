use std::collections::HashMap;

use fabric_core::Coordinate;
use fabric_telemetry::Observation;

use crate::profile::WorkloadProfile;

#[derive(Debug, Default)]
pub struct WorkloadAnalyzer {
    profiles: HashMap<Coordinate, WorkloadProfile>,
}

impl WorkloadAnalyzer {
    pub fn new() -> Self {
        Self {
            profiles: HashMap::new(),
        }
    }

    pub fn observe(&mut self, observation: &Observation) {
        let profile = WorkloadProfile::from_metrics(
            observation.coordinate,
            observation.metrics,
        );

        self.profiles
            .insert(observation.coordinate, profile);
    }

    pub fn profile(
        &self,
        coordinate: Coordinate,
    ) -> Option<&WorkloadProfile> {
        self.profiles.get(&coordinate)
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