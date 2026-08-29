use fabric_core::Coordinate;
use fabric_workload::WorkloadProfile;
use serde::{Deserialize, Serialize};

use crate::features::WorkloadFeatures;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HotspotPrediction {
    pub coordinate: Coordinate,
    pub probability: f64,
}

impl HotspotPrediction {
    pub fn is_likely_hot(&self) -> bool {
        self.probability >= 0.80
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AnomalyScore {
    pub coordinate: Coordinate,
    pub score: f64,
}

impl AnomalyScore {
    pub fn is_anomalous(&self) -> bool {
        self.score >= 0.80
    }
}

/// Initial Fabric workload predictor.
///
/// This is intentionally a baseline model. It establishes the inference
/// interface that future trained models will implement.
#[derive(Debug, Clone)]
pub struct WorkloadPredictor {
    hotspot_threshold: f64,
}

impl Default for WorkloadPredictor {
    fn default() -> Self {
        Self {
            hotspot_threshold: 0.75,
        }
    }
}

impl WorkloadPredictor {
    pub fn new(hotspot_threshold: f64) -> Self {
        Self {
            hotspot_threshold: hotspot_threshold.clamp(0.0, 1.0),
        }
    }

    pub fn predict_hotspot(
        &self,
        profile: &WorkloadProfile,
    ) -> HotspotPrediction {
        let features = WorkloadFeatures::from(profile);

        let probability = self.hotspot_probability(&features);

        HotspotPrediction {
            coordinate: features.coordinate,
            probability,
        }
    }

    pub fn detect_anomaly(
        &self,
        profile: &WorkloadProfile,
    ) -> AnomalyScore {
        let features = WorkloadFeatures::from(profile);

        let score = self.anomaly_score(&features);

        AnomalyScore {
            coordinate: features.coordinate,
            score,
        }
    }

    fn hotspot_probability(
        &self,
        features: &WorkloadFeatures,
    ) -> f64 {
        let pressure = features.pressure_score;

        let latency_pressure = (
            features.read_latency_us
                .max(features.write_latency_us)
                / 100_000.0
        )
            .clamp(0.0, 1.0);

        let queue_pressure =
            (features.queue_depth / 10_000.0).clamp(0.0, 1.0);

        let resource_pressure =
            (features.cpu_utilization * 0.6)
                + (features.memory_utilization * 0.4);

        let probability =
            (pressure * 0.45)
                + (latency_pressure * 0.15)
                + (queue_pressure * 0.20)
                + (resource_pressure * 0.20);

        probability.clamp(0.0, 1.0)
    }

    fn anomaly_score(
        &self,
        features: &WorkloadFeatures,
    ) -> f64 {
        let cpu_anomaly =
            ((features.cpu_utilization - 0.80) / 0.20)
                .max(0.0);

        let memory_anomaly =
            ((features.memory_utilization - 0.80) / 0.20)
                .max(0.0);

        let queue_anomaly =
            ((features.queue_depth - 10_000.0) / 10_000.0)
                .max(0.0);

        let latency =
            features.read_latency_us
                .max(features.write_latency_us);

        let latency_anomaly =
            ((latency - 50_000.0) / 50_000.0)
                .max(0.0);

        (
            cpu_anomaly * 0.30
                + memory_anomaly * 0.20
                + queue_anomaly * 0.25
                + latency_anomaly * 0.25
        )
            .clamp(0.0, 1.0)
    }

    pub fn threshold(&self) -> f64 {
        self.hotspot_threshold
    }
}