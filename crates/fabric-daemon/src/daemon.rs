//! Assembly: two sockets, one control thread, and a shutdown that tells the
//! truth.
//!
//! The order things happen in is load-bearing:
//!
//! 1. **Both listeners are bound first.** A port conflict is a configuration
//!    fault, and finding it after the control plane has read the durable
//!    placement store and started probing the fleet means finding it late.
//! 2. **The control plane boots and runs one full cycle** — probe, publish —
//!    before anything is served. It returns the [`FrontDoor`] it will publish
//!    to, so the door the data port serves is the one the loop already
//!    populated. Serving before the first probe would answer `503` for
//!    everything: a registered instance that has never proven liveness is
//!    unreachable, and silence is never read as health.
//! 3. **Only then does the data port accept.**
//!
//! Shutdown runs it backwards, and is described on [`Daemon::shutdown`].

use std::net::SocketAddr;
use std::sync::Arc;

use fabric_facetql::frontdoor::FrontDoor;

use crate::admin::{self, AdminState};
use crate::config::Settings;
use crate::control::{ControlPlane, ControlRequest, ShutdownReport};
use crate::status::{
    DecisionCounters, KeyspaceStatus, MoverStatus, Status, StatusHandle, StoreStatus,
};
use crate::telemetry::TelemetryFactory;
use crate::{now_ms, DaemonError};

/// A running daemon.
pub struct Daemon {
    data_addr: SocketAddr,
    admin_addr: SocketAddr,
    status: Arc<StatusHandle>,

    stop: tokio::sync::watch::Sender<bool>,
    stop_data: Option<tokio::sync::oneshot::Sender<()>>,
    stop_admin: Option<tokio::sync::oneshot::Sender<()>>,

    data: tokio::task::JoinHandle<()>,
    admin: tokio::task::JoinHandle<()>,
    control: std::thread::JoinHandle<ShutdownReport>,
}

impl Daemon {
    /// Bind, boot the control plane, and start serving.
    pub async fn start(
        settings: Settings,
        telemetry: TelemetryFactory,
    ) -> Result<Self, DaemonError> {
        let data_listener = bind(settings.data_listen).await?;
        let admin_listener = bind(settings.admin_listen).await?;

        let data_addr = data_listener.local_addr().map_err(|error| DaemonError::Bind {
            address: settings.data_listen,
            error: error.to_string(),
        })?;

        let admin_addr = admin_listener
            .local_addr()
            .map_err(|error| DaemonError::Bind {
                address: settings.admin_listen,
                error: error.to_string(),
            })?;

        let status = Arc::new(StatusHandle::new(starting(&settings, data_addr, admin_addr)));

        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        let (requests, requests_rx) = tokio::sync::mpsc::unbounded_channel::<ControlRequest>();
        let (booted, boot) = tokio::sync::oneshot::channel();

        let admin_token = settings.admin_token.clone();
        let control_status = Arc::clone(&status);

        /*
         * The control plane gets a sender of its own into the same queue the
         * admin port writes to. That is deliberate: its mover reports through
         * exactly the seam an out-of-band mover reports through, so there is
         * one path into the runtime rather than a privileged internal one and
         * a public one that could drift apart.
         */
        let control_requests = requests.clone();

        /*
         * The control plane gets an OS thread of its own with a current-thread
         * runtime. `FabricRuntime` owns its mechanisms through `Rc<RefCell<_>>`
         * -- single-threaded pure logic, mutated by the controller's executor
         * and read by the runtime beside it -- so it is `!Send` and cannot live
         * on a work-stealing scheduler. Everything it awaits (probes, `/stats`)
         * is awaited here.
         */
        let control = std::thread::Builder::new()
            .name("fabric-control".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,

                    Err(error) => {
                        let _ = booted.send(Err(DaemonError::Startup(format!(
                            "could not start the control-plane runtime: {error}"
                        ))));

                        return ShutdownReport::default();
                    }
                };

                runtime.block_on(async move {
                    let source = match telemetry() {
                        Ok(source) => source,

                        Err(error) => {
                            let _ = booted.send(Err(DaemonError::Startup(error)));
                            return ShutdownReport::default();
                        }
                    };

                    match ControlPlane::boot(settings, source, control_status, control_requests)
                        .await
                    {
                        Ok((plane, door)) => {
                            if booted.send(Ok(door)).is_err() {
                                return ShutdownReport::default();
                            }

                            plane.run(stop_rx, requests_rx).await
                        }

                        Err(error) => {
                            let _ = booted.send(Err(error));
                            ShutdownReport::default()
                        }
                    }
                })
            })
            .map_err(|error| {
                DaemonError::Startup(format!("could not start the control thread: {error}"))
            })?;

        let door: FrontDoor = match boot.await {
            Ok(Ok(door)) => door,

            Ok(Err(error)) => {
                let _ = control.join();
                return Err(error);
            }

            Err(_) => {
                let _ = control.join();
                return Err(DaemonError::Startup(
                    "the control plane stopped before it finished booting".to_string(),
                ));
            }
        };

        let (stop_data, data_stopped) = tokio::sync::oneshot::channel();
        let (stop_admin, admin_stopped) = tokio::sync::oneshot::channel();

        let data = tokio::spawn(async move {
            let _ = axum::serve(data_listener, door.router())
                .with_graceful_shutdown(async move {
                    let _ = data_stopped.await;
                })
                .await;
        });

        let admin_router = admin::router(AdminState::new(
            Arc::clone(&status),
            admin_token,
            requests,
        ));

        let admin = tokio::spawn(async move {
            let _ = axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(async move {
                    let _ = admin_stopped.await;
                })
                .await;
        });

        Ok(Self {
            data_addr,
            admin_addr,
            status,
            stop,
            stop_data: Some(stop_data),
            stop_admin: Some(stop_admin),
            data,
            admin,
            control,
        })
    }

    /// The client-facing address. `FACET_DATABASE_URL` points here.
    pub fn data_addr(&self) -> SocketAddr {
        self.data_addr
    }

    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    pub fn status(&self) -> Arc<StatusHandle> {
        Arc::clone(&self.status)
    }

    /// Stop accepting, drain, and say what could not be finished.
    ///
    /// The sequence, and why each step is where it is:
    ///
    /// 1. **The data port stops accepting**, and the control loop is told to
    ///    stop deciding. New connections are refused immediately; requests
    ///    already in flight keep their answer, which is the whole point of a
    ///    graceful shutdown on a proxy.
    /// 2. **In-flight requests drain.** They were routed by a table they
    ///    already resolved against, and the loop keeps publishing while they
    ///    finish, so a request that has not yet resolved still sees the
    ///    arrangement that actually exists.
    /// 3. **The control loop drains its actions.** Anything before its cutover
    ///    is rolled back, and the arrangement is restored. Anything past its
    ///    cutover is driven to the end of its plan — never aborted, because an
    ///    abort there is `Irreversible` and recording one would claim a
    ///    rollback that did not happen.
    /// 4. **The admin port goes last**, so an operator watching a drain can
    ///    watch all of it.
    ///
    /// The returned report is empty of abandoned actions exactly when the exit
    /// is honest. `fabricd` turns a non-empty one into a non-zero exit status:
    /// a fleet mid-move is not a clean shutdown, and a process that exits 0 on
    /// one has lied to whatever is about to restart it.
    pub async fn shutdown(mut self) -> ShutdownReport {
        let _ = self.stop.send(true);

        if let Some(stop_data) = self.stop_data.take() {
            let _ = stop_data.send(());
        }

        let _ = self.data.await;

        let report = tokio::task::spawn_blocking(move || self.control.join())
            .await
            .ok()
            .and_then(|joined| joined.ok())
            .unwrap_or_default();

        if let Some(stop_admin) = self.stop_admin.take() {
            let _ = stop_admin.send(());
        }

        let _ = self.admin.await;

        report
    }
}

async fn bind(address: SocketAddr) -> Result<tokio::net::TcpListener, DaemonError> {
    tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| DaemonError::Bind {
            address,
            error: error.to_string(),
        })
}

/// The snapshot the admin port answers with between binding and the first
/// cycle. It claims nothing: no backend is healthy, no route is published.
fn starting(settings: &Settings, data: SocketAddr, admin: SocketAddr) -> Status {
    Status {
        version: env!("CARGO_PKG_VERSION"),
        started_at_ms: now_ms(),
        snapshot_at_ms: now_ms(),
        clock_ms: 0,
        cycles: 0,
        draining: false,
        data_listen: data.to_string(),
        admin_listen: admin.to_string(),
        routing_generation: 0,
        placement_generation: 0,
        telemetry_source: "starting".to_string(),
        observations: 0,
        profiles: 0,
        hot_cells: 0,
        keyspace: KeyspaceStatus {
            rules: Vec::new(),
            fallback: None,
            spans_one_place: false,
        },
        backends: Vec::new(),
        placements: Vec::new(),
        in_flight: Vec::new(),
        decisions: DecisionCounters::default(),
        movers: MoverStatus::default(),
        placement_store: StoreStatus {
            configured: settings
                .placement_store
                .as_ref()
                .map(|id| id.0.clone()),
            state: "starting".to_string(),
            last_error: None,
            writes: 0,
        },
        history: Vec::new(),
    }
}
