//! `fabric` -- native operator CLI for Facet Fabric.
//!
//! Fabric is early: there is no persistent daemon or live-state service yet, so
//! this CLI does not pretend to manage or query a running cluster. Instead it
//! drives the *real* in-process analysis pipeline (`fabric-runtime`) over a
//! session of protocol messages and reports what the actual Fabric crates
//! compute. Every command is backed by a concrete crate capability; commands
//! that would require capability Fabric does not have yet are deliberately
//! absent (see the crate README / handoff notes).

mod args;
mod render;
mod session;

use std::process::ExitCode;

use args::{Command, RunOpts, Subcommand};
use fabric_core::{GRID_ATOMS, GRID_HEIGHT, GRID_WIDTH};
use fabric_protocol::ProtocolVersion;
use fabric_runtime::FabricRuntime;
use render::StatusSummary;

const EXIT_OK: u8 = 0;
const EXIT_ERROR: u8 = 1;
const EXIT_USAGE: u8 = 2;
/// `validate` ran successfully but found out-of-grid coordinates. Distinct from
/// EXIT_ERROR so scripts can tell "the tool failed" from "the input is invalid".
const EXIT_INVALID: u8 = 3;

const TOP_HELP: &str = "\
fabric -- Facet Fabric operator CLI

USAGE:
    fabric <command> [--input <file>] [--json]

COMMANDS:
    status      Runtime summary (grid geometry, protocol version, counts)
    topology    Physical placements known to the topology registry
    workload    Workload profiles derived from ingested telemetry
    metrics     Latest raw workload metrics per location
    placement   Optimizer placement/optimization decisions
    predict     ML hotspot probability and anomaly score per profile
    validate    Check that ingested coordinates fall within the grid

OPTIONS:
    -i, --input <file>   JSON array of FabricMessage values to replay ('-' = stdin)
        --json           Emit JSON instead of human-readable text
    -h, --help           Show help
    -V, --version        Show version

NOTES:
    Fabric has no live daemon yet, so this tool runs the real analysis pipeline
    over a captured/authored protocol-message session. With no --input the
    runtime is empty and each command reports the empty state.";

fn subcommand_help(sub: Subcommand) -> String {
    let purpose = match sub {
        Subcommand::Status => "Print a runtime summary: protocol version, grid geometry, and counts of placements, profiles, observations and hot coordinates.",
        Subcommand::Topology => "List physical placements (coordinate, shard, region, DBMS) held by the topology registry.",
        Subcommand::Workload => "Summarize workload profiles: ops/sec, read/write ratio, pressure level and whether the location is hot.",
        Subcommand::Metrics => "Show the latest raw metrics per observed location (ops/sec, CPU, memory, queue depth).",
        Subcommand::Placement => "Run the optimizer over each workload profile and print the resulting placement decision.",
        Subcommand::Predict => "Score each workload profile with the optimizer's ML predictor: hotspot probability and anomaly score.",
        Subcommand::Validate => "Check every ingested coordinate against the grid bounds; exit 3 if any fall outside.",
    };
    format!(
        "fabric {name} -- {purpose}\n\nUSAGE:\n    fabric {name} [--input <file>] [--json]\n",
        name = sub.name(),
    )
}

fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();

    match args::parse(raw) {
        Ok(Command::Help) => {
            println!("{TOP_HELP}");
            ExitCode::from(EXIT_OK)
        }
        Ok(Command::Version) => {
            println!("fabric {}", env!("CARGO_PKG_VERSION"));
            ExitCode::from(EXIT_OK)
        }
        Ok(Command::Run { subcommand, opts }) => run(subcommand, opts),
        Err(err) => {
            eprintln!("error: {err}");
            eprintln!("try 'fabric --help'");
            ExitCode::from(EXIT_USAGE)
        }
    }
}

fn run(subcommand: Subcommand, opts: RunOpts) -> ExitCode {
    if opts.help {
        print!("{}", subcommand_help(subcommand));
        return ExitCode::from(EXIT_OK);
    }

    let runtime = match session::load(opts.input.as_deref()) {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::from(EXIT_ERROR);
        }
    };

    let output = match subcommand {
        Subcommand::Status => render::render_status(&status_summary(&runtime), opts.json),
        Subcommand::Topology => render::render_topology(&topology(&runtime), opts.json),
        Subcommand::Workload => render::render_workload(&workload(&runtime), opts.json),
        Subcommand::Metrics => render::render_metrics(&metrics(&runtime), opts.json),
        Subcommand::Placement => render::render_placement(&placement(&runtime), opts.json),
        Subcommand::Predict => render::render_predict(&predictions(&runtime), opts.json),
        Subcommand::Validate => {
            // A linter-style command: a clean run still exits 0, but any
            // out-of-grid coordinate is a finding that scripts can gate on.
            let issues = validation(&runtime);
            println!("{}", render::render_validation(&issues, opts.json));
            let code = if issues.is_empty() { EXIT_OK } else { EXIT_INVALID };
            return ExitCode::from(code);
        }
    };

    println!("{output}");
    ExitCode::from(EXIT_OK)
}

fn status_summary(runtime: &FabricRuntime) -> StatusSummary {
    StatusSummary {
        placements: runtime.topology().len(),
        profiles: runtime.analyzer().len(),
        observations: runtime.state().len(),
        hot_coordinates: runtime.analyzer().hot_coordinates().len(),
        grid_width: GRID_WIDTH,
        grid_height: GRID_HEIGHT,
        grid_atoms: GRID_ATOMS,
        protocol_major: ProtocolVersion::V1.major,
        protocol_minor: ProtocolVersion::V1.minor,
    }
}

fn topology(runtime: &FabricRuntime) -> Vec<fabric_topology::Placement> {
    let mut placements: Vec<_> = runtime.topology().placements().cloned().collect();
    placements.sort_by_key(|p| (p.coordinate.index(), p.shard_id));
    placements
}

fn workload(runtime: &FabricRuntime) -> Vec<fabric_workload::WorkloadProfile> {
    let mut profiles: Vec<_> = runtime.analyzer().profiles().cloned().collect();
    profiles.sort_by_key(|p| p.coordinate.index());
    profiles
}

fn metrics(runtime: &FabricRuntime) -> Vec<fabric_telemetry::Observation> {
    let mut observations: Vec<_> = runtime.state().observations().cloned().collect();
    observations.sort_by_key(|o| (o.coordinate.index(), o.shard.id));
    observations
}

fn placement(runtime: &FabricRuntime) -> Vec<fabric_optimizer::OptimizationDecision> {
    let mut decisions: Vec<_> = runtime
        .analyzer()
        .profiles()
        .map(|profile| runtime.optimize(profile))
        .collect();
    decisions.sort_by_key(|d| d.coordinate.index());
    decisions
}

fn predictions(runtime: &FabricRuntime) -> Vec<render::PredictionRow> {
    // Use the optimizer's own predictor so these numbers are exactly the ones
    // the placement policy reacts to -- not a separately-configured model.
    let predictor = runtime.optimizer().predictor();
    let mut rows: Vec<_> = runtime
        .analyzer()
        .profiles()
        .map(|profile| {
            let hotspot = predictor.predict_hotspot(profile);
            let anomaly = predictor.detect_anomaly(profile);
            render::PredictionRow {
                coordinate: profile.coordinate,
                hotspot_probability: hotspot.probability,
                likely_hot: hotspot.is_likely_hot(),
                anomaly_score: anomaly.score,
                anomalous: anomaly.is_anomalous(),
            }
        })
        .collect();
    rows.sort_by_key(|r| r.coordinate.index());
    rows
}

fn validation(runtime: &FabricRuntime) -> Vec<render::ValidationIssue> {
    // Coordinates enter the runtime from two sources -- topology placements and
    // telemetry observations. `Coordinate::is_valid` is the real capability;
    // this command just surfaces which ingested coordinates fail it.
    let mut seen = std::collections::HashSet::new();
    let mut invalid: Vec<_> = runtime
        .topology()
        .placements()
        .map(|p| p.coordinate)
        .chain(runtime.state().observations().map(|o| o.coordinate))
        .filter(|c| seen.insert(*c))
        .filter(|c| !c.is_valid())
        .collect();
    invalid.sort_by_key(|c| c.index());
    invalid
        .into_iter()
        .map(|c| render::ValidationIssue {
            reason: format!(
                "coordinate ({},{}) is outside the {}x{} grid",
                c.x, c.y, GRID_WIDTH, GRID_HEIGHT
            ),
            coordinate: c,
        })
        .collect()
}
