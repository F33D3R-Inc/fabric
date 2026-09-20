use fabric_core::Coordinate;
use fabric_workload::WorkloadProfile;
use serde::{Deserialize, Serialize};

use crate::features::WorkloadFeatures;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HotspotPrediction {
    pub coordinate: Coordinate,
    pub probability: f64,

    /// The threshold `probability` was judged against — the same
    /// `WorkloadPredictor::hotspot_threshold` (customizable via
    /// [`WorkloadPredictor::new`]) that predicted this value, carried along
    /// so [`is_likely_hot`](Self::is_likely_hot) can compare against the
    /// threshold that's actually configured rather than a second, separate
    /// constant of its own. Two independent cutoffs for the same question
    /// ("is this hot") is exactly how this drifted before: `hotspot_threshold`
    /// defaults to 0.75 — deliberately the same cutoff
    /// `fabric_workload::PressureLevel::High` uses — but `is_likely_hot`
    /// used to hardcode `0.80` and never read the threshold at all, so
    /// configuring a custom `WorkloadPredictor` had no effect on the one
    /// decision it exists to make.
    pub threshold: f64,
}

impl HotspotPrediction {
    pub fn is_likely_hot(&self) -> bool {
        self.probability >= self.threshold
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
            threshold: self.hotspot_threshold,
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

#[cfg(test)]
mod tests {
    use super::*;
    use fabric_core::Coordinate;
    use fabric_telemetry::WorkloadMetrics;
    use fabric_workload::WorkloadProfile;

    fn loaded_profile() -> WorkloadProfile {
        // Enough real load to land somewhere in the middle of the range,
        // not pinned to either end — a profile with cpu/queue/latency all
        // maxed would be "hot" against nearly any threshold and would
        // never have caught the bug this module's tests exist to catch.
        let metrics = WorkloadMetrics {
            cpu_utilization: 0.6,
            memory_utilization: 0.3,
            queue_depth: 4_000,
            write_latency_us: 40_000.0,
            reads_per_second: 10.0,
            writes_per_second: 90.0,
            ..Default::default()
        };
        WorkloadProfile::from_metrics(1, Coordinate::new(0, 0), metrics)
    }

    /// `HotspotPrediction::is_likely_hot` used to hardcode `>= 0.80`,
    /// ignoring `WorkloadPredictor::hotspot_threshold` entirely — so
    /// `WorkloadPredictor::new(threshold)` had zero effect on the one
    /// decision it exists to make. This is the regression test: the same
    /// probability must answer differently depending on which predictor
    /// produced it.
    #[test]
    fn is_likely_hot_respects_the_predictors_own_configured_threshold() {
        let profile = loaded_profile();
        let probability = WorkloadPredictor::default()
            .predict_hotspot(&profile)
            .probability;
        assert!(
            probability > 0.0 && probability < 1.0,
            "fixture should land strictly between the two thresholds this test compares, got {probability}"
        );

        let lenient = WorkloadPredictor::new(probability - 0.05);
        assert!(
            lenient.predict_hotspot(&profile).is_likely_hot(),
            "a threshold set below the observed probability must call it hot"
        );

        let strict = WorkloadPredictor::new(probability + 0.05);
        assert!(
            !strict.predict_hotspot(&profile).is_likely_hot(),
            "a threshold set above the observed probability must NOT call it hot"
        );
    }

    /// `is_likely_hot` reads the threshold off the `HotspotPrediction`
    /// value itself, not off whatever `WorkloadPredictor` happens to be in
    /// scope — so a prediction, once produced, keeps answering against the
    /// threshold it was actually judged by even if compared later.
    #[test]
    fn is_likely_hot_is_a_pure_function_of_its_own_two_fields() {
        assert!(HotspotPrediction {
            coordinate: Coordinate::new(0, 0),
            probability: 0.78,
            threshold: 0.75,
        }
        .is_likely_hot());

        assert!(!HotspotPrediction {
            coordinate: Coordinate::new(0, 0),
            probability: 0.78,
            threshold: 0.80,
        }
        .is_likely_hot());
    }

    /// The default threshold is deliberately the same cutoff
    /// `fabric_workload::PressureLevel::High` uses (0.75) — the two
    /// independent "is this hot" gates this crate and `fabric-workload`
    /// each compute are meant to agree at the default configuration, even
    /// though they're unrelated formulas over the same raw metrics.
    #[test]
    fn the_default_threshold_matches_pressure_level_highs_cutoff() {
        assert_eq!(WorkloadPredictor::default().threshold(), 0.75);
    }
}