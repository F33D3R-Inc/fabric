//! `fabricd` — the Fabric control plane, running.
//!
//! ```text
//! fabricd --config /etc/fabric/fabric.json
//! ```
//!
//! Everything it needs is in that file except the secrets, which the file
//! names by environment variable and which are read from the environment. See
//! [`fabric_daemon::config`].
//!
//! # Exit status
//!
//! * **0** — a clean shutdown: nothing was left mid-move.
//! * **75** (`EX_TEMPFAIL`) — the drain budget ran out with an action past its
//!   cutover. The data is on the destination and the move did not finish; the
//!   report on stderr names the action, the cell and both instances. This is
//!   deliberately not 0: whatever restarts this process must be able to tell
//!   the difference.
//! * **78** (`EX_CONFIG`) — the configuration was refused, or a socket could
//!   not be bound.

use std::path::PathBuf;
use std::process::ExitCode;

use fabric_daemon::{facetql_telemetry, Daemon, DaemonError, Settings};

const EXIT_OK: u8 = 0;
const EXIT_UNFINISHED: u8 = 75;
const EXIT_CONFIG: u8 = 78;

const USAGE: &str = "\
fabricd -- the Fabric control plane, with FacetQL's front door on its data port

USAGE:
    fabricd --config <file>

OPTIONS:
    -c, --config <file>  Configuration (JSON). Also FABRIC_CONFIG.
    -h, --help           Show help
    -V, --version        Show version

The configuration names every secret by environment variable and holds none of
them. The admin token (FABRIC_ADMIN_TOKEN unless the file says otherwise) is
required: the operator port is stateful and is never served unauthenticated.
";

#[tokio::main]
async fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();

    let path = match parse(&arguments) {
        Ok(Some(path)) => path,
        Ok(None) => return ExitCode::from(EXIT_OK),

        Err(message) => {
            eprintln!("fabricd: {message}");
            eprint!("{USAGE}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };

    let settings = match Settings::load(&path) {
        Ok(settings) => settings,

        Err(error) => {
            eprintln!("fabricd: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };

    let telemetry = facetql_telemetry(&settings);

    let daemon = match Daemon::start(settings, telemetry).await {
        Ok(daemon) => daemon,

        Err(error) => {
            eprintln!("fabricd: {error}");

            return ExitCode::from(match error {
                DaemonError::Config(_) | DaemonError::Bind { .. } => EXIT_CONFIG,
                DaemonError::Startup(_) => EXIT_CONFIG,
            });
        }
    };

    eprintln!(
        "fabricd: serving FacetQL's wire on {} and the operator surface on {}",
        daemon.data_addr(),
        daemon.admin_addr()
    );

    wait_for_signal().await;

    eprintln!("fabricd: draining");

    let report = daemon.shutdown().await;

    for id in &report.rolled_back {
        eprintln!("fabricd: action-{id} was rolled back; the arrangement is restored");
    }

    for id in &report.unmeasured {
        eprintln!(
            "fabricd: action-{id} executed and was never measured; its verdict is lost, \
             its data is not"
        );
    }

    if let Some(unpersisted) = &report.unpersisted {
        eprintln!("fabricd: {unpersisted}");
    }

    for abandoned in &report.abandoned {
        eprintln!(
            "fabricd: action-{} is past its cutover and did not finish: {} moved from '{}' \
             to '{}', migration phase '{}', execution state '{}'. It was NOT aborted -- an \
             abort after cutover cannot restore the previous arrangement, and recording one \
             would claim a rollback that did not happen. The data is on the destination.",
            abandoned.id,
            abandoned.target,
            abandoned.source,
            abandoned.destination.as_deref().unwrap_or("(none)"),
            abandoned.migration_phase.as_deref().unwrap_or("(none)"),
            abandoned.state,
        );
    }

    if report.is_clean() {
        eprintln!("fabricd: stopped cleanly");
        return ExitCode::from(EXIT_OK);
    }

    ExitCode::from(EXIT_UNFINISHED)
}

/// `Ok(None)` means the argument was answered here (help, version) and the
/// process is done.
fn parse(arguments: &[String]) -> Result<Option<PathBuf>, String> {
    let mut path: Option<PathBuf> = std::env::var("FABRIC_CONFIG").ok().map(PathBuf::from);
    let mut index = 0;

    while index < arguments.len() {
        match arguments[index].as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(None);
            }

            "-V" | "--version" => {
                println!("fabricd {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }

            "-c" | "--config" => {
                index += 1;

                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--config expects a file".to_string())?;

                path = Some(PathBuf::from(value));
            }

            other => return Err(format!("unexpected argument '{other}'")),
        }

        index += 1;
    }

    path.ok_or_else(|| {
        "no configuration: pass --config <file> or set FABRIC_CONFIG".to_string()
    })
    .map(Some)
}

/// Wait for the signals a supervisor actually sends.
///
/// `SIGTERM` is what a container runtime, systemd and Kubernetes send first,
/// and a daemon that only handled `SIGINT` would be killed by the follow-up
/// `SIGKILL` mid-migration having drained nothing.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,

            Err(error) => {
                eprintln!("fabricd: could not listen for SIGTERM: {error}");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
