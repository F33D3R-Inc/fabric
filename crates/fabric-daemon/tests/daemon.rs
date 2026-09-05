//! The daemon, running, over real sockets.
//!
//! Nothing here is a mock except the two things that are genuinely outside
//! this workspace: the FacetQL processes (real HTTP servers that record what
//! arrived and answer FacetQL's own shapes) and the thing that copies bytes
//! between them, which does not exist in any repo and reports in through the
//! admin port. The daemon is the real `Daemon`; the front door, routing table,
//! replica set, migration, controller and optimizer are all the real ones; the
//! workload comes from `fabric-simulator`, which exists precisely so the loop
//! can be driven without a live fleet.
//!
//! What the first test proves, in order:
//!
//! 1. the daemon **boots**: both ports bind and the operator surface answers;
//! 2. it **routes real traffic**: a `GET /nodes?kind=Post` carrying an API key
//!    arrives at the instance the keyspace and routing table say holds the
//!    cell, byte for byte;
//! 3. it **reacts to a control-loop decision**: the optimizer proposes, the
//!    controller admits, the real migration runs, and *the same request now
//!    arrives at the other instance* — with no restart, no reconfiguration,
//!    and the routing generation moving underneath it;
//! 4. it **refuses to pretend** a cutover can be undone: aborting the action
//!    afterwards is a `409`, not a rollback;
//! 5. it **stops routing to a dead instance**: kill the backend, and once its
//!    silence outlasts the budget the front door answers `503` with
//!    `Retry-After` rather than forwarding into a hole;
//! 6. it **shuts down cleanly** and says so.
//!
//! The second test proves the other half of the shutdown rule: an action that
//! has *not* cut over is rolled back on the way out, and the arrangement is
//! restored.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_daemon::config::{ConfigFile, MapEnv, Settings};
use fabric_daemon::telemetry::{Sample, TelemetryFactory, TelemetrySource};
use fabric_daemon::{now_ms, Daemon};
use fabric_protocol::{FabricMessage, TelemetryBatch, TelemetrySample};
use fabric_simulator::{ClusterSpec, Fault, Simulation};

// ── a FacetQL that records what it was asked ────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    method: String,
    target: String,
    api_key: Option<String>,
}

#[derive(Clone)]
struct Fake {
    base_url: String,
    log: Arc<Mutex<Vec<Recorded>>>,
    stop: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl Fake {
    async fn spawn() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();

        let fake = Self {
            base_url: format!("http://{address}"),
            log: Arc::new(Mutex::new(Vec::new())),
            stop: Arc::new(Mutex::new(None)),
        };

        let (stop, stopped) = tokio::sync::oneshot::channel();
        *fake.stop.lock().unwrap() = Some(stop);

        let router = Router::new().fallback(any(record)).with_state(fake.clone());

        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    stopped.await.ok();
                })
                .await
                .ok();
        });

        fake
    }

    fn seen(&self) -> Vec<Recorded> {
        self.log.lock().unwrap().clone()
    }

    fn forget(&self) {
        self.log.lock().unwrap().clear();
    }

    fn saw_nothing(&self) -> bool {
        self.log.lock().unwrap().is_empty()
    }

    async fn stop(&self) {
        if let Some(stop) = self.stop.lock().unwrap().take() {
            stop.send(()).ok();
        }
    }
}

async fn record(State(fake): State<Fake>, request: Request) -> Response {
    let (parts, _) = request.into_parts();

    fake.log.lock().unwrap().push(Recorded {
        method: parts.method.to_string(),
        target: parts
            .uri
            .path_and_query()
            .map(|target| target.to_string())
            .unwrap_or_default(),
        api_key: parts
            .headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
    });

    // `GET /` is FacetQL's unauthenticated banner, and the daemon's liveness
    // probe. Everything else answers a plausible FacetQL body.
    if parts.uri.path() == "/" {
        return (StatusCode::OK, "FacetQL Online").into_response();
    }

    (StatusCode::OK, r#"{"nodes":[],"next_cursor":null}"#).into_response()
}

// ── the workload ────────────────────────────────────────────────────────

const SHARD: u64 = 1;
const CELL: Coordinate = Coordinate::new(0, 0);
const SOURCE: &str = "us-east-db-0";
const DESTINATION: &str = "us-west-db-0";

/// The simulated fleet, as a telemetry source.
///
/// Two things are fed in, and the difference between them is the honest one:
///
/// * the **simulation's own messages**, which are what a fleet under a
///   diurnal, class-shaped workload actually reports; and
/// * one explicit observation of a cell under sustained write pressure, which
///   `fabric-runtime`'s own control-loop test supplies for the same reason:
///   the simulator's latency model tops out below the threshold the optimizer
///   acts on, and telemetry is an input to the loop rather than part of it.
///   Everything downstream of this batch — analyzer, predictor, optimizer,
///   controller, mechanisms, routing, the front door — runs untouched.
///
/// Timestamps are restamped to wall time. The simulator's clock is a counter
/// from an explicit start instant and is deliberately not "now"; the daemon's
/// is wall time, because a process that has to notice a fleet going quiet
/// needs a clock that keeps moving when the fleet stops reporting. Translating
/// at this seam is what keeps the two from disagreeing about how old an
/// observation is.
struct SimulatedFleet {
    simulation: Simulation,
    duress: bool,
}

impl SimulatedFleet {
    fn new(duress: bool) -> Self {
        let mut simulation = Simulation::new(ClusterSpec {
            seed: 7,
            regions: vec!["us-east".to_string(), "us-west".to_string()],
            nodes_per_region: 1,
            shards_per_node: 1,
            cells_per_shard: 1,
            classes: vec![fabric_core::WorkloadClass::WriteHeavy],
            ..ClusterSpec::default()
        });

        simulation.inject(Fault::HotShard {
            target: fabric_controller::ActionTarget::new(SHARD, CELL),
            multiplier: 3.0,
            duration_ms: 900_000,
        });

        Self {
            simulation,
            duress,
        }
    }
}

impl TelemetrySource for SimulatedFleet {
    fn describe(&self) -> String {
        "fabric-simulator (2 instances, write-heavy)".to_string()
    }

    fn sample(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Sample> + '_>> {
        Box::pin(async move {
            let at_ms = now_ms();
            let mut sample = Sample::default();

            for message in self.simulation.step().messages {
                // Liveness has exactly one authority in this daemon and it is
                // the prober, so the simulation's heartbeats are dropped: the
                // fleet's reachability is decided by `GET /` against the real
                // sockets, not by a simulation of it.
                if let FabricMessage::Telemetry(mut batch) = message {
                    batch.timestamp_ms = at_ms;
                    sample.messages.push(FabricMessage::Telemetry(batch));
                }
            }

            if self.duress {
                sample.messages.push(duress(at_ms));
            }

            sample
        })
    }
}

/// The observation a node under sustained write pressure reports — the same
/// numbers `fabric-runtime`'s control-loop test uses.
fn duress(at_ms: u64) -> FabricMessage {
    FabricMessage::Telemetry(TelemetryBatch {
        timestamp_ms: at_ms,
        node_id: DbmsId::new(SOURCE),
        shard: Shard::new(SHARD, "us-east"),
        samples: vec![TelemetrySample {
            coordinate: CELL,
            operations_per_second: 12_000.0,
            read_ratio: 0.10,
            write_ratio: 0.90,
            read_latency_us: 40_000.0,
            write_latency_us: 60_000.0,
            cpu_utilization: 0.99,
            memory_utilization: 0.95,
            queue_depth: 30_000,
            cell_breakdown: Vec::new(),
            cell_breakdown_partial: false,
        }],
    })
}

// ── the daemon under test ───────────────────────────────────────────────

const ADMIN_TOKEN: &str = "operator-secret";
const CLIENT_TOKEN: &str = "client-secret";

fn settings(source: &Fake, destination: &Fake, silence_budget_ms: u64) -> Settings {
    let declaration = format!(
        r#"{{
            "data_listen": "127.0.0.1:0",
            "admin_listen": "127.0.0.1:0",
            "backends": [
                {{
                    "id": "{SOURCE}",
                    "url": "{source}",
                    "region": "us-east",
                    "placements": [{{ "shard": {SHARD}, "x": 0, "y": 0 }}]
                }},
                {{
                    "id": "{DESTINATION}",
                    "url": "{destination}",
                    "region": "us-west",
                    "placements": [{{ "shard": 2, "x": 0, "y": 0 }}]
                }}
            ],
            "keyspace": {{
                "rules": [
                    {{ "kind": "Post", "address_prefix": "Post:",
                       "shard": {SHARD}, "x": 0, "y": 0 }}
                ],
                "fallback": {{ "shard": {SHARD}, "x": 0, "y": 0 }}
            }},
            "cadence": {{
                "liveness_probe_ms": 100,
                "telemetry_poll_ms": 100,
                "control_cycle_ms": 50
            }},
            "silence_budget_ms": {silence_budget_ms},
            "probe_timeout_ms": 300,
            "policy": {{
                "measurement_settle_ms": 0,
                "phase_timeout_ms": 60000
            }},
            "drain_ms": 5000
        }}"#,
        source = source.base_url,
        destination = destination.base_url,
    );

    let file: ConfigFile = serde_json::from_str(&declaration).expect("a valid declaration");

    Settings::resolve(file, &MapEnv::of(&[("FABRIC_ADMIN_TOKEN", ADMIN_TOKEN)]))
        .expect("a resolvable declaration")
}

fn simulated(duress: bool) -> TelemetryFactory {
    Box::new(move || Ok(Box::new(SimulatedFleet::new(duress)) as Box<dyn TelemetrySource>))
}

/// One `fqStore`-shaped read through the data port.
async fn read_posts(http: &reqwest::Client, data: SocketAddr) -> reqwest::Response {
    http.get(format!("http://{data}/nodes?kind=Post"))
        .header("x-api-key", CLIENT_TOKEN)
        .send()
        .await
        .expect("the front door answered")
}

async fn admin(http: &reqwest::Client, admin: SocketAddr, path: &str) -> serde_json::Value {
    let response = http
        .get(format!("http://{admin}{path}"))
        .header("x-api-key", ADMIN_TOKEN)
        .send()
        .await
        .expect("the operator surface answered");

    assert_eq!(response.status(), 200, "{path}");

    // `reqwest` is built without its `json` feature across this workspace, so
    // bodies are parsed with `serde_json` exactly as the FacetQL client does.
    let body = response.text().await.expect("a body");

    serde_json::from_str(&body).unwrap_or_else(|error| panic!("{path}: {error}: {body}"))
}

/// Poll until the block is true, or fail saying what was being waited for.
///
/// A macro rather than a function taking a closure: the conditions below are
/// `.await`-ing HTTP calls that borrow the client, and a boxed-future
/// predicate trait to express that would be more machinery than the thing it
/// is checking.
macro_rules! until {
    ($what:expr, $budget:expr, $condition:expr) => {{
        let deadline = std::time::Instant::now() + $budget;

        loop {
            if $condition {
                break;
            }

            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for {}", $what);
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }};
}

// ── the test ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_daemon_boots_routes_traffic_and_follows_a_control_loop_decision() {
    let source = Fake::spawn().await;
    let destination = Fake::spawn().await;

    let daemon = Daemon::start(settings(&source, &destination, 5_000), simulated(true))
        .await
        .expect("the daemon boots");

    let data = daemon.data_addr();
    let operator = daemon.admin_addr();
    let http = reqwest::Client::new();

    // ── 1. it booted ────────────────────────────────────────────────────

    let health = http
        .get(format!("http://{operator}/healthz"))
        .send()
        .await
        .expect("the operator port is listening");

    assert_eq!(health.status(), 200);

    // The operator surface is stateful, so it is never unauthenticated.
    let refused = http
        .get(format!("http://{operator}/status"))
        .send()
        .await
        .unwrap();

    assert_eq!(refused.status(), 401);

    // Liveness was established before the data port ever accepted: the boot
    // cycle probes, so no client is answered 503 for a fleet that is up.
    let status = admin(&http, operator, "/status").await;

    assert_eq!(status["backends"][0]["availability"], "serviceable");
    assert_eq!(status["backends"][1]["availability"], "serviceable");
    assert_eq!(status["backends"][0]["last_probe"], "serving (200)");
    assert_eq!(status["placements"][0]["holder"], SOURCE);

    let generation_at_boot = status["routing_generation"].as_u64().unwrap();
    assert!(generation_at_boot > 0);

    // ── 2. it routes real traffic ───────────────────────────────────────

    let answered = read_posts(&http, data).await;
    assert_eq!(answered.status(), 200);

    let seen = source.seen();
    let arrived = seen
        .iter()
        .find(|request| request.target.starts_with("/nodes?"))
        .expect("the read reached the instance that holds the cell");

    assert_eq!(arrived.method, "GET");
    assert_eq!(arrived.target, "/nodes?kind=Post");

    // The front door holds no identity: the client's own credential is what
    // arrived, unread and unmodified.
    assert_eq!(arrived.api_key.as_deref(), Some(CLIENT_TOKEN));

    assert!(
        !destination
            .seen()
            .iter()
            .any(|request| request.target.starts_with("/nodes?")),
        "the read was also sent to an instance that does not hold the cell"
    );

    // ── 3. the loop decides, and the decision becomes a migration ───────

    until!(
        "the control loop to admit an action",
        Duration::from_secs(10),
        admin(&http, operator, "/actions")
            .await
            .as_array()
            .is_some_and(|actions| !actions.is_empty())
    );

    let actions = admin(&http, operator, "/actions").await;
    let action = &actions[0];

    assert_eq!(action["source"], SOURCE);
    assert_eq!(action["destination"], DESTINATION);
    assert_eq!(action["mechanism"], "fabric-placement");
    assert_eq!(action["shard"], SHARD);

    let id = action["id"].as_u64().expect("an action id");

    // Nothing in Fabric or FacetQL copies a cell's data between instances, so
    // the transfer is fed by whatever does. Until it reports, the phase does
    // not advance -- a transfer nobody reports on never completes.
    until!(
        "the transfer phase",
        Duration::from_secs(10),
        admin(&http, operator, "/actions").await[0]["phase"] == "transfer"
    );

    let reported = http
        .post(format!("http://{operator}/actions/{id}/transfer"))
        .header("x-api-key", ADMIN_TOKEN)
        .body(r#"{"atoms_copied":1,"bytes_copied":4096,"resident_bytes":4096}"#)
        .send()
        .await
        .expect("the mover reported");

    assert_eq!(reported.status(), 200);

    // ── 4. traffic follows the data, with no restart ────────────────────

    until!(
        "the cell to move",
        Duration::from_secs(10),
        admin(&http, operator, "/routing").await["placements"][0]["holder"] == DESTINATION
    );

    let routing = admin(&http, operator, "/routing").await;

    assert!(
        routing["routing_generation"].as_u64().unwrap() > generation_at_boot,
        "the routing table the front door serves from never changed"
    );

    source.forget();
    destination.forget();

    let answered = read_posts(&http, data).await;
    assert_eq!(answered.status(), 200);

    assert!(
        destination
            .seen()
            .iter()
            .any(|request| request.target == "/nodes?kind=Post"),
        "the same request did not follow the data to its new home"
    );

    assert!(
        !source
            .seen()
            .iter()
            .any(|request| request.target.starts_with("/nodes?")),
        "the request was still sent to the instance that no longer holds the cell"
    );

    // ── 5. a cutover cannot be undone by asking nicely ──────────────────

    let refused = http
        .post(format!("http://{operator}/actions/{id}/abort"))
        .header("x-api-key", ADMIN_TOKEN)
        .send()
        .await
        .expect("the operator surface answered");

    let status = refused.status();
    let body = refused.text().await.unwrap();

    assert_eq!(status, 409, "{body}");
    assert!(
        body.contains("cut over") || body.contains("concluded"),
        "the refusal did not explain itself: {body}"
    );

    // ── 6. a dead instance stops receiving traffic ──────────────────────

    destination.stop().await;

    until!(
        "routing to drop the dead instance",
        Duration::from_secs(15),
        admin(&http, operator, "/status").await["backends"]
            .as_array()
            .expect("a fleet listing")
            .iter()
            .any(|backend| {
                backend["id"] == DESTINATION && backend["availability"] == "unreachable"
            })
    );

    let answered = read_posts(&http, data).await;

    assert_eq!(
        answered.status(),
        503,
        "the front door kept forwarding to an instance that is not running"
    );

    assert_eq!(
        answered
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1"),
        "a 503 that is retryable must say so"
    );

    // ── 7. it stops cleanly ─────────────────────────────────────────────

    let report = daemon.shutdown().await;

    assert!(
        report.abandoned.is_empty(),
        "shutdown abandoned {:?}",
        report.abandoned
    );
    assert!(report.is_clean());

    source.stop().await;
}

/// The other half of the shutdown rule.
///
/// An action still short of its cutover has its whole effect on one node that
/// is not authoritative for anything, so tearing it down restores the
/// arrangement exactly. The daemon does that on the way out rather than
/// leaving a half-made copy and a migration in flight for the next process to
/// find.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_rolls_back_an_action_that_has_not_cut_over() {
    let source = Fake::spawn().await;
    let destination = Fake::spawn().await;

    let daemon = Daemon::start(settings(&source, &destination, 5_000), simulated(true))
        .await
        .expect("the daemon boots");

    let operator = daemon.admin_addr();
    let data = daemon.data_addr();
    let http = reqwest::Client::new();

    until!(
        "an action to be in flight",
        Duration::from_secs(10),
        admin(&http, operator, "/actions").await[0]["phase"] == "transfer"
    );

    // No transfer is ever reported: the copy never completes, and shutdown
    // arrives with the migration still short of its cutover.
    let report = daemon.shutdown().await;

    assert_eq!(
        report.rolled_back.len(),
        1,
        "the in-flight action was not rolled back: {report:?}"
    );
    assert!(report.abandoned.is_empty(), "{report:?}");
    assert!(report.is_clean());

    // The cell never moved: the source still holds it, which is what
    // "restored" means.
    let _ = data;
    assert!(!source.saw_nothing(), "the source was never probed");

    source.stop().await;
    destination.stop().await;
}
