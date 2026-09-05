//! Hand-rolled argument parsing for the `fabric` operator CLI.
//!
//! `clap` is intentionally NOT used: it is not vendored in the workspace
//! `Cargo.lock`, and the command surface here is small enough that a
//! dependency-free parser is the simpler, auditable choice. This module is
//! pure (no I/O) so it can be unit-tested directly.

use std::path::PathBuf;

/// Fully parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Top-level `--help` / no arguments.
    Help,
    /// Top-level `--version`.
    Version,
    /// Per-subcommand invocation.
    Run {
        subcommand: Subcommand,
        opts: RunOpts,
    },
}

/// Analysis subcommands, each backed by a real Fabric crate capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subcommand {
    /// Runtime summary (`fabric-runtime`, `fabric-core` grid geometry).
    Status,
    /// Physical placements (`fabric-topology::TopologyRegistry`).
    Topology,
    /// Workload profiles (`fabric-workload::WorkloadAnalyzer`).
    Workload,
    /// Latest raw metrics (`fabric-telemetry`, `fabric-runtime::FabricState`).
    Metrics,
    /// Optimizer placement decisions (`fabric-optimizer::WorkloadOptimizer`).
    Placement,
    /// ML hotspot/anomaly predictions (`fabric-ml::WorkloadPredictor`).
    Predict,
    /// Grid-bounds validation of ingested coordinates (`fabric-core::Coordinate`).
    Validate,
    /// Fleet inventory and liveness (`fabric-runtime::NodeRegistry`).
    Nodes,
}

impl Subcommand {
    pub fn name(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Topology => "topology",
            Self::Workload => "workload",
            Self::Metrics => "metrics",
            Self::Placement => "placement",
            Self::Predict => "predict",
            Self::Validate => "validate",
            Self::Nodes => "nodes",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "status" => Some(Self::Status),
            "topology" => Some(Self::Topology),
            "workload" => Some(Self::Workload),
            "metrics" => Some(Self::Metrics),
            "placement" => Some(Self::Placement),
            "predict" => Some(Self::Predict),
            "validate" => Some(Self::Validate),
            "nodes" => Some(Self::Nodes),
            _ => None,
        }
    }
}

/// Options common to every subcommand.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOpts {
    /// Optional session file: a JSON array of `FabricMessage` values (the same
    /// messages Fabric nodes emit over the protocol). When absent the runtime
    /// is empty and commands report the empty state honestly.
    pub input: Option<PathBuf>,
    /// Emit machine-readable JSON instead of human-readable text.
    pub json: bool,
    /// Show help for this subcommand and exit.
    pub help: bool,
    /// `nodes`: the instant to judge liveness at, in epoch milliseconds.
    /// Defaults to the runtime's own clock (the newest timestamp it ingested),
    /// which is the only "now" that means anything when replaying a session.
    pub now: Option<u64>,
    /// `nodes`: how long a node may stay silent before it counts as
    /// unreachable, in milliseconds. Defaults to
    /// `fabric_runtime::DEFAULT_HEARTBEAT_DEADLINE_MS`.
    pub deadline: Option<u64>,
}

/// Argument parsing failure. Maps to exit code 2 (usage error) at the top level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnknownCommand(String),
    UnknownFlag(String),
    MissingValue(String),
    UnexpectedArgument(String),
    InvalidValue { flag: String, value: String },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCommand(c) => write!(f, "unknown command '{c}'"),
            Self::UnknownFlag(flag) => write!(f, "unknown flag '{flag}'"),
            Self::MissingValue(flag) => write!(f, "flag '{flag}' expects a value"),
            Self::UnexpectedArgument(a) => write!(f, "unexpected argument '{a}'"),
            Self::InvalidValue { flag, value } => {
                write!(f, "flag '{flag}' expects a number, got '{value}'")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse process arguments (excluding argv[0]).
pub fn parse<I, S>(args: I) -> Result<Command, ParseError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args: Vec<String> = args.into_iter().map(Into::into).collect();
    let mut iter = args.iter();

    let Some(first) = iter.next() else {
        return Ok(Command::Help);
    };

    match first.as_str() {
        "-h" | "--help" | "help" => return Ok(Command::Help),
        "-V" | "--version" | "version" => return Ok(Command::Version),
        _ => {}
    }

    let subcommand = Subcommand::parse(first)
        .ok_or_else(|| ParseError::UnknownCommand(first.clone()))?;

    let mut opts = RunOpts::default();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => opts.help = true,
            "--json" => opts.json = true,
            "-i" | "--input" => {
                let value = iter
                    .next()
                    .ok_or_else(|| ParseError::MissingValue(arg.clone()))?;
                opts.input = Some(PathBuf::from(value));
            }
            other if other.starts_with("--input=") => {
                let value = &other["--input=".len()..];
                opts.input = Some(PathBuf::from(value));
            }
            "--now" => opts.now = Some(number(arg, iter.next())?),
            other if other.starts_with("--now=") => {
                opts.now = Some(parse_number(arg, &other["--now=".len()..])?);
            }
            "--deadline" => opts.deadline = Some(number(arg, iter.next())?),
            other if other.starts_with("--deadline=") => {
                opts.deadline = Some(parse_number(arg, &other["--deadline=".len()..])?);
            }
            other if other.starts_with('-') => {
                return Err(ParseError::UnknownFlag(other.to_string()));
            }
            other => {
                return Err(ParseError::UnexpectedArgument(other.to_string()));
            }
        }
    }

    Ok(Command::Run { subcommand, opts })
}

/// A numeric flag value taken from the next argument.
fn number(flag: &str, value: Option<&String>) -> Result<u64, ParseError> {
    let value = value.ok_or_else(|| ParseError::MissingValue(flag.to_string()))?;
    parse_number(flag, value)
}

fn parse_number(flag: &str, value: &str) -> Result<u64, ParseError> {
    value.parse().map_err(|_| ParseError::InvalidValue {
        flag: flag.to_string(),
        value: value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(args: &[&str]) -> Command {
        parse(args.iter().map(|s| s.to_string())).expect("parse should succeed")
    }

    #[test]
    fn no_args_is_help() {
        assert_eq!(parse_ok(&[]), Command::Help);
    }

    #[test]
    fn help_and_version_flags() {
        assert_eq!(parse_ok(&["--help"]), Command::Help);
        assert_eq!(parse_ok(&["-h"]), Command::Help);
        assert_eq!(parse_ok(&["help"]), Command::Help);
        assert_eq!(parse_ok(&["--version"]), Command::Version);
        assert_eq!(parse_ok(&["-V"]), Command::Version);
    }

    #[test]
    fn bare_subcommand_defaults() {
        let cmd = parse_ok(&["topology"]);
        assert_eq!(
            cmd,
            Command::Run {
                subcommand: Subcommand::Topology,
                opts: RunOpts::default(),
            }
        );
    }

    #[test]
    fn every_subcommand_parses() {
        for name in [
            "status",
            "topology",
            "workload",
            "metrics",
            "placement",
            "predict",
            "validate",
            "nodes",
        ] {
            match parse_ok(&[name]) {
                Command::Run { subcommand, .. } => assert_eq!(subcommand.name(), name),
                other => panic!("expected Run for {name}, got {other:?}"),
            }
        }
    }

    #[test]
    fn input_and_json_flags() {
        let cmd = parse_ok(&["metrics", "--input", "session.json", "--json"]);
        let Command::Run { subcommand, opts } = cmd else {
            panic!("expected Run");
        };
        assert_eq!(subcommand, Subcommand::Metrics);
        assert_eq!(opts.input, Some(PathBuf::from("session.json")));
        assert!(opts.json);
    }

    #[test]
    fn input_short_flag_and_equals_form() {
        let a = parse_ok(&["workload", "-i", "s.json"]);
        let b = parse_ok(&["workload", "--input=s.json"]);
        let expected = Some(PathBuf::from("s.json"));
        assert!(matches!(a, Command::Run { opts, .. } if opts.input == expected));
        assert!(matches!(b, Command::Run { opts, .. } if opts.input == expected));
    }

    #[test]
    fn stdin_dash_is_accepted_as_input() {
        // The `-` convention selects stdin; it must parse as an ordinary
        // `--input` value, not be misread as an unknown flag.
        let a = parse_ok(&["status", "--input", "-"]);
        let b = parse_ok(&["status", "-i", "-"]);
        let expected = Some(PathBuf::from("-"));
        assert!(matches!(a, Command::Run { opts, .. } if opts.input == expected));
        assert!(matches!(b, Command::Run { opts, .. } if opts.input == expected));
    }

    #[test]
    fn subcommand_help() {
        let cmd = parse_ok(&["placement", "--help"]);
        assert!(matches!(cmd, Command::Run { opts, .. } if opts.help));
    }

    #[test]
    fn liveness_flags_parse_in_both_forms() {
        let a = parse_ok(&["nodes", "--now", "5000", "--deadline", "1000"]);
        let b = parse_ok(&["nodes", "--now=5000", "--deadline=1000"]);
        for cmd in [a, b] {
            let Command::Run { subcommand, opts } = cmd else {
                panic!("expected Run");
            };
            assert_eq!(subcommand, Subcommand::Nodes);
            assert_eq!(opts.now, Some(5_000));
            assert_eq!(opts.deadline, Some(1_000));
        }
    }

    #[test]
    fn a_non_numeric_liveness_flag_errors() {
        let err = parse(["nodes".to_string(), "--now".to_string(), "soon".to_string()])
            .unwrap_err();
        assert_eq!(
            err,
            ParseError::InvalidValue {
                flag: "--now".into(),
                value: "soon".into()
            }
        );
        assert!(err.to_string().contains("expects a number"));
    }

    #[test]
    fn unknown_command_errors() {
        let err = parse(["frobnicate".to_string()]).unwrap_err();
        assert_eq!(err, ParseError::UnknownCommand("frobnicate".into()));
    }

    #[test]
    fn unknown_flag_errors() {
        let err = parse(["status".to_string(), "--wat".to_string()]).unwrap_err();
        assert_eq!(err, ParseError::UnknownFlag("--wat".into()));
    }

    #[test]
    fn missing_input_value_errors() {
        let err = parse(["status".to_string(), "--input".to_string()]).unwrap_err();
        assert_eq!(err, ParseError::MissingValue("--input".into()));
    }

    #[test]
    fn unexpected_positional_errors() {
        let err = parse(["status".to_string(), "extra".to_string()]).unwrap_err();
        assert_eq!(err, ParseError::UnexpectedArgument("extra".into()));
    }
}
