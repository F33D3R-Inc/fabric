//! # fabric-daemon
//!
//! Fabric as a process.
//!
//! Everything else in the workspace is a layer: routing answers where a key
//! lives, replication keeps copies honest, migration moves a cell without
//! losing a write, the controller decides whether an optimizer's
//! recommendation may be executed at all, and `fabric-facetql`'s front door
//! speaks FacetQL's wire protocol so that `fqStore` cannot tell the difference.
//! All of it was a library. Nothing owned a socket, a clock or a fleet, and
//! `FrontDoor::serve` was a seam with nothing on the other side of it.
//!
//! `fabricd` is the other side.
//!
//! ```text
//!            clients                         operators
//!               │                                │
//!        :7710  ▼  data                   :7711  ▼  admin (x-api-key)
//!        ┌──────────────┐                 ┌──────────────┐
//!        │  FrontDoor   │◄── publish ─────│   status     │
//!        └──────┬───────┘   routing       └──────┬───────┘
//!               │                                │ transfer / abort
//!               ▼                                ▼
//!         FacetQL fleet ◄── GET / probes ── control loop (one thread)
//!                       ◄── GET /stats ───      │
//!                                          runtime → optimizer → controller
//!                                                            → mechanisms
//! ```
//!
//! ## The two planes, and the one thing they share
//!
//! The data plane is ordinary async work on the process's worker threads: a
//! request arrives, resolves a route under a read guard, and is forwarded. The
//! control plane is one OS thread, because [`FabricRuntime`] owns its
//! mechanisms through `Rc<RefCell<_>>` and is therefore `!Send` — deliberately,
//! since routing, replication and migration are single-threaded pure logic
//! that the controller's executor mutates in place.
//!
//! They share the front door's fleet snapshot and a status snapshot, and
//! nothing else. Neither is ever held across an await; see
//! [`control`] for the discipline in full.
//!
//! ## What this daemon cannot do yet, and whose gap that is
//!
//! The loop runs, executes and measures. Against a **real** FacetQL fleet it
//! will nonetheless propose nothing, and that is not a bug here:
//!
//! * `WorkloadProfile`'s pressure score is computed from CPU utilization,
//!   memory utilization, queue depth and read/write latency
//!   (`fabric_workload::profile::calculate_pressure`). **`GET /stats` reports
//!   none of the four.** Operations per second, which is all it does report,
//!   contributes nothing to pressure by design. So every cell observed only
//!   through `/stats` scores exactly 0.0, is never `is_hot`, and is never
//!   optimized. One of those four fields on the existing `/stats` response
//!   would close it. Owner: **Persistence → FacetQL**.
//! * Nothing in this workspace, and no FacetQL endpoint, **copies a cell's
//!   data from one instance to another**. There is no bulk export/import, so
//!   the migration machinery's `Transfer` phase is fed by whatever does move
//!   the bytes, through the admin port's transfer report. A transfer nobody
//!   reports on never completes and the controller's phase timeout rolls it
//!   back — which is the honest failure, not a phase that reports itself done
//!   because time passed.
//!
//! Neither is worked around here. `fabric-simulator` exists precisely so the
//! loop can be driven end to end without a live fleet, and that is what the
//! integration test does.

pub mod admin;
pub mod config;
pub mod control;
pub mod daemon;
pub mod liveness;
pub mod mover;
pub mod status;
pub mod telemetry;

pub use config::{ConfigError, ConfigFile, Settings};
pub use control::{ControlPlane, ControlRequest, ShutdownReport};
pub use daemon::Daemon;
pub use liveness::{LivenessProber, Probe, ProbeTarget};
pub use mover::{MoverRefusal, MoverSupervisor};
pub use status::{Status, StatusHandle};
pub use telemetry::{FacetqlTelemetry, Sample, TelemetryFactory, TelemetrySource};

use std::net::SocketAddr;

/// Why the daemon is not running.
#[derive(Debug)]
pub enum DaemonError {
    Config(ConfigError),
    Bind { address: SocketAddr, error: String },
    Startup(String),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),

            Self::Bind { address, error } => {
                write!(f, "could not bind {address}: {error}")
            }

            Self::Startup(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for DaemonError {}

impl From<ConfigError> for DaemonError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

/// Wall-clock epoch milliseconds.
///
/// The daemon's "now". The runtime's clock is otherwise message time, which is
/// the only clock that means anything when replaying a captured session and
/// not enough for a process that has to notice a fleet going quiet.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

/// Build the live telemetry source from a resolved configuration.
///
/// Every backend that declares a credential and holds at least one cell
/// becomes a poll target. One that declares no credential is not a failure and
/// is not silently dropped: it simply produces no telemetry, and the admin
/// surface reports `telemetry: false` for it rather than showing a quiet
/// instance as a healthy one.
pub fn facetql_telemetry(settings: &Settings) -> TelemetryFactory {
    let mut targets = Vec::new();

    for backend in &settings.backends {
        let Some(token) = backend.token.clone() else {
            continue;
        };

        let endpoint = match fabric_facetql::FacetqlEndpoint::new(
            backend.id.clone(),
            backend.url.clone(),
            token,
        ) {
            Ok(endpoint) => endpoint,
            Err(_) => continue,
        };

        /*
         * One FacetQL instance is one placeable unit until FacetQL grows a
         * native shard/cell concept, so an instance's counters are attributed
         * to the first cell it holds rather than split across several. Guessing
         * that split is the adapter-side invention the integration plan
         * forbids (Finding A).
         */
        let Some(cell) = backend.placements.first() else {
            continue;
        };

        targets.push(fabric_facetql::poller::PollTarget::new(
            endpoint,
            cell.shard,
            fabric_core::Coordinate::new(cell.x, cell.y),
            backend.region.clone(),
        ));
    }

    Box::new(move || {
        FacetqlTelemetry::new(targets)
            .map(|source| Box::new(source) as Box<dyn TelemetrySource>)
    })
}
