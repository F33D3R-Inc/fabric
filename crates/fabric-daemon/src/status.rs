//! What the operator port reports, and how it gets there without touching the
//! control loop.
//!
//! The control loop owns the [`FabricRuntime`](fabric_runtime::FabricRuntime),
//! which is `Rc`-based and lives on one thread for exactly that reason. An
//! admin request therefore cannot borrow it, and making it possible would put
//! the data plane's neighbours behind a lock an HTTP handler holds.
//!
//! So the loop *publishes* instead: at the end of every cycle it builds one
//! immutable [`Status`] and swaps it in. Admin handlers clone an `Arc` under a
//! read lock they hold for the length of that clone and nothing else — no
//! await, no I/O, no chance of an operator's `curl` slowing a cutover down.
//!
//! The consequence is honest and stated in the payload: every reading is
//! as-of `snapshot_at_ms`, the end of the last cycle. A status surface that
//! blocked the loop to be a few hundred milliseconds fresher would be a worse
//! trade in exactly the moment it matters.

use std::sync::{Arc, RwLock};

use fabric_controller::OutcomeReport;
use serde::Serialize;

/// One FacetQL instance, as the control plane currently sees it.
#[derive(Debug, Clone, Serialize)]
pub struct BackendStatus {
    pub id: String,
    pub url: String,
    pub region: String,

    /// What routing will do with it: `serviceable`, `unreachable`, `unknown`.
    pub availability: String,

    /// What the fleet inventory makes of it: `healthy`, `degraded`,
    /// `unreachable`.
    pub health: String,

    /// The last liveness probe's verdict, in its own words.
    pub last_probe: String,
    pub last_probe_at_ms: Option<u64>,

    /// Milliseconds since the last accepted heartbeat, or `null` if the
    /// instance has never proven liveness.
    pub silence_ms: Option<u64>,
    pub heartbeats: u64,

    /// Whether a control-plane credential was configured for this instance.
    /// Without one it produces no telemetry, and its workload is invisible to
    /// the optimizer.
    pub telemetry: bool,

    /// The last `/stats` sample's verdict, where telemetry is configured.
    pub last_sample: Option<String>,
}

/// One placed cell.
#[derive(Debug, Clone, Serialize)]
pub struct PlacementStatus {
    pub shard: u64,
    pub x: u8,
    pub y: u8,
    pub holder: String,
    pub region: String,

    /// The migration routing currently holds for this cell, if any.
    pub migration: Option<MigrationStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationStatus {
    pub phase: String,
    pub source: String,
    pub destination: String,
    pub read_owner: String,
    pub write_fenced: bool,
    pub has_cut_over: bool,
}

/// One action the controller is executing.
#[derive(Debug, Clone, Serialize)]
pub struct ActionStatus {
    pub id: u64,
    pub shard: u64,
    pub x: u8,
    pub y: u8,
    pub action: String,
    pub mechanism: String,
    pub state: String,
    pub phase: Option<String>,
    pub fraction: Option<f64>,
    pub source: String,
    pub destination: Option<String>,
    pub admitted_at_ms: u64,

    /// Past this, an abort cannot restore the original arrangement: the
    /// destination is authoritative and discarding its copy would be data
    /// loss. It is what shutdown refuses to walk away from.
    pub has_cut_over: bool,

    /// Bytes the fleet has reported copied for this action, and the cell's
    /// resident size if anything has reported one. Fabric copies no bytes; a
    /// transfer nobody reports on never completes.
    pub bytes_copied: u64,
    pub resident_bytes: Option<u64>,
}

/// What the loop did with the decisions it produced.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DecisionCounters {
    /// Decisions the optimizer produced that were worth executing.
    pub proposed: u64,

    /// Decisions the controller admitted.
    pub admitted: u64,

    /// Decisions the controller refused. A refusal is the control plane
    /// working, not failing.
    pub refused: u64,

    pub last_refusal: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyspaceStatus {
    pub rules: Vec<String>,
    pub fallback: Option<String>,

    /// Whether the whole namespace lives in one place, which is what makes
    /// `GET /stats`, kind-less queries and `POST /admin/users` answerable
    /// through the front door at all.
    pub spans_one_place: bool,
}

/// Whether Fabric's own placement state is durable, and whether it agrees with
/// the map the daemon is running.
#[derive(Debug, Clone, Serialize)]
pub struct StoreStatus {
    pub configured: Option<String>,
    pub state: String,
    pub last_error: Option<String>,
    pub writes: u64,
}

/// Everything the operator port answers with, as of one instant.
#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub version: &'static str,
    pub started_at_ms: u64,
    pub snapshot_at_ms: u64,

    /// The runtime's own clock: the newest timestamp it has ingested.
    pub clock_ms: u64,

    pub cycles: u64,
    pub draining: bool,

    pub data_listen: String,
    pub admin_listen: String,

    /// The generation of the routing table the front door is serving from.
    /// This is the number that moves when a control-loop decision reaches live
    /// traffic.
    pub routing_generation: u64,

    /// The placement generation, which moves only when a cell actually changes
    /// holder. A decision is superseded by this and by nothing else.
    pub placement_generation: u64,

    pub telemetry_source: String,
    pub observations: usize,
    pub profiles: usize,
    pub hot_cells: usize,

    pub keyspace: KeyspaceStatus,
    pub backends: Vec<BackendStatus>,
    pub placements: Vec<PlacementStatus>,
    pub in_flight: Vec<ActionStatus>,
    pub decisions: DecisionCounters,
    pub placement_store: StoreStatus,

    /// What this daemon's own data mover is doing, and what it refused to do.
    ///
    /// Reported because a migration whose mover never started looks, from
    /// every other row on this page, exactly like one whose copy is slow: the
    /// transfer sits at zero and the phase eventually times out. The reason —
    /// almost always a backend with no configured credential — belongs where
    /// an operator is already looking.
    pub movers: MoverStatus,

    /// The most recently concluded actions' feedback rows, oldest last: what
    /// each cost, what it bought, and the verdict — including `unchanged` for
    /// an action that cost something and bought nothing.
    ///
    /// A tail, not the archive: the controller keeps every record, and this
    /// snapshot is rebuilt every cycle.
    pub history: Vec<OutcomeReport>,
}

/// The daemon's own copies, in flight and refused.
#[derive(Debug, Clone, Default, Serialize)]
pub struct MoverStatus {
    /// `(action id, destination)` for every copy this daemon is making itself.
    pub copying: Vec<CopyingStatus>,

    /// Copies this daemon declined to start, newest state last. A non-empty
    /// list means those migrations depend entirely on an out-of-band mover
    /// reporting through `POST /actions/{id}/transfer`.
    pub refused: Vec<RefusedCopy>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CopyingStatus {
    pub id: u64,
    pub destination: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RefusedCopy {
    pub id: u64,
    pub reason: String,
}

/// The published snapshot, and the only thing the two planes share besides the
/// front door's own fleet.
#[derive(Debug)]
pub struct StatusHandle {
    current: RwLock<Arc<Status>>,
}

impl StatusHandle {
    pub fn new(initial: Status) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
        }
    }

    /// Read the newest snapshot. The lock is held for one `Arc` clone.
    pub fn get(&self) -> Arc<Status> {
        Arc::clone(
            &self
                .current
                .read()
                .expect("the status lock is poisoned"),
        )
    }

    /// Swap in a new snapshot. The lock is held for one assignment, and the
    /// snapshot is fully built before it is taken.
    pub fn publish(&self, status: Status) {
        *self
            .current
            .write()
            .expect("the status lock is poisoned") = Arc::new(status);
    }
}
