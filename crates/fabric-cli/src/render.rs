//! Pure output formatting for each command.
//!
//! Every function is deterministic and free of I/O so it can be unit-tested.
//! JSON output serializes the underlying Fabric domain types directly (they all
//! derive `serde::Serialize`); human output is a compact aligned table.

use fabric_core::Coordinate;
use fabric_optimizer::{OptimizationAction, OptimizationDecision};
use fabric_telemetry::Observation;
use fabric_topology::Placement;
use fabric_workload::{PressureLevel, WorkloadProfile};

/// Stable snapshot counts for `fabric status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StatusSummary {
    pub placements: usize,
    pub profiles: usize,
    pub observations: usize,
    pub hot_coordinates: usize,
    pub grid_width: u8,
    pub grid_height: u8,
    pub grid_atoms: usize,
    pub protocol_major: u16,
    pub protocol_minor: u16,
}

/// One row of `fabric predict`: the ML model's hotspot and anomaly verdicts for
/// a single workload profile. Both signals come from `fabric-ml` via the
/// optimizer's own predictor, so they match what the optimizer acts on.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct PredictionRow {
    pub coordinate: Coordinate,
    pub hotspot_probability: f64,
    pub likely_hot: bool,
    pub anomaly_score: f64,
    pub anomalous: bool,
}

/// One finding of `fabric validate`: a coordinate that fell outside the grid.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ValidationIssue {
    pub coordinate: Coordinate,
    pub reason: String,
}

fn coord(c: Coordinate) -> String {
    format!("({},{})", c.x, c.y)
}

pub fn pressure_label(level: PressureLevel) -> &'static str {
    match level {
        PressureLevel::Normal => "normal",
        PressureLevel::Elevated => "elevated",
        PressureLevel::High => "high",
        PressureLevel::Critical => "critical",
    }
}

pub fn action_label(action: &OptimizationAction) -> String {
    match action {
        OptimizationAction::NoAction => "no-action".to_string(),
        OptimizationAction::Replicate { target } => format!("replicate -> {}", target.0),
        OptimizationAction::Move { target } => format!("move -> {}", target.0),
        OptimizationAction::Split => "split".to_string(),
        OptimizationAction::Isolate => "isolate".to_string(),
        OptimizationAction::Colocate { target } => {
            format!("colocate -> {}", coord(*target))
        }
    }
}

pub fn render_status(summary: &StatusSummary, json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(summary)
            .unwrap_or_else(|_| "{}".to_string());
    }

    let mut out = String::new();
    out.push_str("Facet Fabric runtime status\n");
    out.push_str(&format!(
        "  protocol version   {}.{}\n",
        summary.protocol_major, summary.protocol_minor
    ));
    out.push_str(&format!(
        "  grid geometry      {}x{} ({} atoms)\n",
        summary.grid_width, summary.grid_height, summary.grid_atoms
    ));
    out.push_str(&format!("  placements         {}\n", summary.placements));
    out.push_str(&format!("  workload profiles  {}\n", summary.profiles));
    out.push_str(&format!("  observations       {}\n", summary.observations));
    out.push_str(&format!("  hot coordinates    {}\n", summary.hot_coordinates));
    out
}

pub fn render_topology(placements: &[Placement], json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(placements)
            .unwrap_or_else(|_| "[]".to_string());
    }

    if placements.is_empty() {
        return "No placements. Topology registry is empty.".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("Topology: {} placement(s)\n", placements.len()));
    out.push_str("  COORD    SHARD  REGION            DBMS\n");
    for p in placements {
        out.push_str(&format!(
            "  {:<7}  {:<5}  {:<16}  {}\n",
            coord(p.coordinate),
            p.shard_id,
            p.region,
            p.dbms_id.0
        ));
    }
    out
}

pub fn render_workload(profiles: &[WorkloadProfile], json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(profiles)
            .unwrap_or_else(|_| "[]".to_string());
    }

    if profiles.is_empty() {
        return "No workload profiles. No telemetry has been ingested.".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("Workload: {} profile(s)\n", profiles.len()));
    out.push_str("  COORD    OPS/S       READ%  WRITE%  PRESSURE   HOT\n");
    for p in profiles {
        out.push_str(&format!(
            "  {:<7}  {:>10.1}  {:>5.0}  {:>6.0}  {:<9}  {}\n",
            coord(p.coordinate),
            p.operations_per_second,
            p.read_ratio * 100.0,
            p.write_ratio * 100.0,
            pressure_label(p.pressure),
            if p.is_hot() { "yes" } else { "no" }
        ));
    }
    out
}

pub fn render_metrics(observations: &[Observation], json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(observations)
            .unwrap_or_else(|_| "[]".to_string());
    }

    if observations.is_empty() {
        return "No metrics. No observations recorded.".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("Metrics: {} observation(s)\n", observations.len()));
    out.push_str("  COORD    SHARD  OPS/S       CPU    MEM    QUEUE\n");
    for o in observations {
        let m = &o.metrics;
        out.push_str(&format!(
            "  {:<7}  {:<5}  {:>10.1}  {:>4.0}%  {:>4.0}%  {:>6}\n",
            coord(o.coordinate),
            o.shard.id,
            m.total_operations(),
            m.cpu_utilization * 100.0,
            m.memory_utilization * 100.0,
            m.queue_depth
        ));
    }
    out
}

pub fn render_placement(decisions: &[OptimizationDecision], json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(decisions)
            .unwrap_or_else(|_| "[]".to_string());
    }

    if decisions.is_empty() {
        return "No placement decisions. No workload profiles to optimize.".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("Placement: {} decision(s)\n", decisions.len()));
    out.push_str("  COORD    ACTION                 GAIN   COST   SCORE   CONF   EXECUTE\n");
    for d in decisions {
        out.push_str(&format!(
            "  {:<7}  {:<21}  {:>4.2}   {:>4.2}   {:>5.2}   {:>4.2}   {}\n",
            coord(d.coordinate),
            action_label(&d.action),
            d.expected_gain,
            d.estimated_cost,
            d.score(),
            d.confidence,
            if d.should_execute() { "yes" } else { "no" }
        ));
    }
    out
}

pub fn render_predict(rows: &[PredictionRow], json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(rows)
            .unwrap_or_else(|_| "[]".to_string());
    }

    if rows.is_empty() {
        return "No predictions. No workload profiles to score.".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("Predict: {} profile(s)\n", rows.len()));
    out.push_str("  COORD    HOTSPOT  LIKELY-HOT  ANOMALY  ANOMALOUS\n");
    for r in rows {
        out.push_str(&format!(
            "  {:<7}  {:>7.2}  {:<10}  {:>7.2}  {}\n",
            coord(r.coordinate),
            r.hotspot_probability,
            if r.likely_hot { "yes" } else { "no" },
            r.anomaly_score,
            if r.anomalous { "yes" } else { "no" }
        ));
    }
    out
}

pub fn render_validation(issues: &[ValidationIssue], json: bool) -> String {
    if json {
        return serde_json::to_string_pretty(issues)
            .unwrap_or_else(|_| "[]".to_string());
    }

    if issues.is_empty() {
        return "Valid. Every ingested coordinate is within the grid.".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!(
        "Invalid: {} out-of-grid coordinate(s)\n",
        issues.len()
    ));
    for issue in issues {
        out.push_str(&format!("  {}\n", issue.reason));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabric_core::{DbmsId, Shard};

    #[test]
    fn pressure_labels() {
        assert_eq!(pressure_label(PressureLevel::Normal), "normal");
        assert_eq!(pressure_label(PressureLevel::Critical), "critical");
    }

    #[test]
    fn action_labels() {
        assert_eq!(action_label(&OptimizationAction::NoAction), "no-action");
        assert_eq!(action_label(&OptimizationAction::Isolate), "isolate");
        assert_eq!(
            action_label(&OptimizationAction::Move {
                target: DbmsId::new("node-b")
            }),
            "move -> node-b"
        );
        assert_eq!(
            action_label(&OptimizationAction::Colocate {
                target: Coordinate::new(3, 4)
            }),
            "colocate -> (3,4)"
        );
    }

    #[test]
    fn empty_renderers_are_explicit() {
        assert!(render_topology(&[], false).contains("empty"));
        assert!(render_workload(&[], false).contains("No workload"));
        assert!(render_metrics(&[], false).contains("No metrics"));
        assert!(render_placement(&[], false).contains("No placement"));
    }

    #[test]
    fn topology_human_and_json() {
        let placement = Placement::new(
            DbmsId::new("node-a"),
            &Shard::new(7, "domain"),
            Coordinate::new(1, 2),
            "us-east",
        );
        let placements = vec![placement];

        let human = render_topology(&placements, false);
        assert!(human.contains("(1,2)"));
        assert!(human.contains("us-east"));
        assert!(human.contains("node-a"));

        let json = render_topology(&placements, true);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["region"], "us-east");
        assert_eq!(parsed[0]["shard_id"], 7);
    }

    #[test]
    fn predict_empty_and_populated() {
        assert!(render_predict(&[], false).contains("No predictions"));

        let rows = vec![PredictionRow {
            coordinate: Coordinate::new(2, 3),
            hotspot_probability: 0.91,
            likely_hot: true,
            anomaly_score: 0.42,
            anomalous: false,
        }];

        let human = render_predict(&rows, false);
        assert!(human.contains("(2,3)"));
        assert!(human.contains("0.91"));
        assert!(human.contains("yes"));

        let json = render_predict(&rows, true);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["likely_hot"], true);
        assert_eq!(parsed[0]["coordinate"]["x"], 2);
    }

    #[test]
    fn validation_valid_and_invalid() {
        assert!(render_validation(&[], false).contains("Valid"));

        let issues = vec![ValidationIssue {
            coordinate: Coordinate::new(20, 20),
            reason: "coordinate (20,20) is outside the 12x13 grid".to_string(),
        }];

        let human = render_validation(&issues, false);
        assert!(human.contains("Invalid"));
        assert!(human.contains("(20,20)"));

        let json = render_validation(&issues, true);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["coordinate"]["y"], 20);
    }

    #[test]
    fn status_json_roundtrips() {
        let summary = StatusSummary {
            placements: 2,
            profiles: 3,
            observations: 4,
            hot_coordinates: 1,
            grid_width: 12,
            grid_height: 13,
            grid_atoms: 156,
            protocol_major: 1,
            protocol_minor: 0,
        };
        let json = render_status(&summary, true);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["placements"], 2);
        assert_eq!(parsed["grid_atoms"], 156);
    }
}
