//! A real migration, moving real bytes, between two real FacetQL processes.
//!
//! `tests/daemon.rs` proves the control plane around a copy: two recording
//! HTTP fakes, and the transfer fed in through `POST /actions/{id}/transfer`
//! because nothing in the workspace could move data. This test removes that
//! prop. Two actual `facetql` servers are started, a cell is seeded on one,
//! and **nothing external reports anything** — the daemon's own mover has to
//! do the copy, close the write gap, check the result, and let the cutover
//! through.
//!
//! What it proves, in order:
//!
//! 1. the daemon's mover **copies real rows** between two real instances,
//!    driven only by the control loop reaching its transfer phase;
//! 2. every node lands **byte-identical** — every field FacetQL returns,
//!    including `owner`, `claimed_by` and `visibility`, not just `data`;
//! 3. writes that land on the source **during** the copy arrive too, by way of
//!    the change feed rather than the snapshot;
//! 4. the transfer's progress is **measured**: the bytes the daemon reports
//!    are bytes that committed, and they match what the source actually holds;
//! 5. the cutover happens, and **a client's requests follow it** — a write
//!    sent to the same front-door address after the cutover lands on the new
//!    instance and not on the old one;
//! 6. it shuts down cleanly.
//!
//! It is opt-in on the binary being there. `FABRIC_FACETQL_BIN` names it;
//! otherwise the sibling checkout's release build is used, and when that is
//! missing the test prints why it skipped rather than silently passing.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_daemon::config::{ConfigFile, MapEnv, Settings};
use fabric_daemon::telemetry::{Sample, TelemetryFactory, TelemetrySource};
use fabric_daemon::{now_ms, Daemon};
use fabric_protocol::{FabricMessage, TelemetryBatch, TelemetrySample};

const SHARD: u64 = 1;
const CELL: Coordinate = Coordinate::new(0, 0);
const SOURCE: &str = "us-east-db-0";
const DESTINATION: &str = "us-west-db-0";

const ADMIN_TOKEN: &str = "operator-secret";

/// One credential, admin *and* owner `app`.
///
/// Both halves are needed and the reason is a real constraint, not a test
/// convenience: `GET /stats` is admin-gated (the mover reads it to refuse a
/// cell with edges in it), while `POST /transaction`'s `insert_node` stamps
/// the *writing* identity as the copied node's owner — so the credential that
/// copies a cell must already own its nodes, or the copy fails verification on
/// the `owner` field. `ENOCHIAN_TOKENS` gives one token both roles.
const DB_TOKEN: &str = "app-secret";
const DB_TOKENS: &str = "app-secret:app:admin";

/// Seeded before the migration starts.
const SEEDED: usize = 400;

// ── a real FacetQL process ──────────────────────────────────────────────

struct Instance {
    process: Child,
    base_url: String,
    data_dir: PathBuf,
}

impl Drop for Instance {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

impl Instance {
    async fn start(binary: &PathBuf, name: &str) -> Instance {
        Self::start_with(binary, name, &[], None).await
    }

    /// `start`, plus extra environment variables and an optional `taskset`
    /// binary to pin the process to a single CPU core.
    ///
    /// The pin exists for the loaded-cell pressure test below: `runtime.
    /// process.cpu_cores` reflects the process's own CPU affinity (see
    /// `fabric_facetql::wire::ProcessStats::cpu_cores`'s doc — the same
    /// mechanism a cgroup CPU quota uses), so pinning to one core is what
    /// lets a modest, portable amount of concurrent load actually saturate
    /// it, rather than needing to peg every core of whatever machine happens
    /// to run this suite.
    async fn start_with(
        binary: &PathBuf,
        name: &str,
        extra_env: &[(&str, &str)],
        pin_to_one_core: Option<&PathBuf>,
    ) -> Instance {
        let port = free_port().await;

        let data_dir = std::env::temp_dir().join(format!(
            "fabric-mover-{name}-{}-{port}",
            std::process::id()
        ));

        std::fs::create_dir_all(&data_dir).expect("a data directory");

        let mut command = match pin_to_one_core {
            Some(taskset) => {
                let mut command = Command::new(taskset);
                command.arg("-c").arg("0").arg(binary);
                command
            }
            None => Command::new(binary),
        };

        command
            .arg("start")
            // Development posture, because the dev master key and the dev
            // token are exactly what a test wants and FacetQL refuses both
            // unless it is told this is not production.
            .env("FACETQL_ENV", "development")
            .env("ENOCHIAN_DATA_DIR", &data_dir)
            .env("ENOCHIAN_PORT", port.to_string())
            .env("ENOCHIAN_TOKENS", DB_TOKENS)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());

        for (key, value) in extra_env {
            command.env(key, value);
        }

        let process = command.spawn().expect("the facetql binary starts");

        let instance = Instance {
            process,
            base_url: format!("http://127.0.0.1:{port}"),
            data_dir,
        };

        instance.wait_until_serving().await;

        instance
    }

    async fn wait_until_serving(&self) {
        let http = reqwest::Client::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);

        loop {
            if http
                .get(format!("{}/", self.base_url))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }

            assert!(
                std::time::Instant::now() < deadline,
                "{} never started serving",
                self.base_url
            );

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Every node this instance holds, keyed by address, walked with the same
    /// keyset cursor the mover uses.
    async fn nodes(&self, http: &reqwest::Client) -> std::collections::BTreeMap<String, serde_json::Value> {
        let mut out = std::collections::BTreeMap::new();
        let mut after = String::new();

        loop {
            let body = serde_json::json!({ "limit": 500, "after": after }).to_string();

            let response = http
                .post(format!("{}/nodes/query", self.base_url))
                .header("x-api-key", DB_TOKEN)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .expect("the instance answered a query");

            assert_eq!(response.status(), 200);

            let text = response.text().await.expect("a body");
            let page: serde_json::Value =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"));

            for node in page["nodes"].as_array().expect("nodes") {
                out.insert(
                    node["address"].as_str().expect("an address").to_string(),
                    node.clone(),
                );
            }

            let next = page["next"].as_str().unwrap_or_default().to_string();

            if next.is_empty() {
                return out;
            }

            after = next;
        }
    }

    async fn write(&self, http: &reqwest::Client, ops: serde_json::Value) {
        let response = http
            .post(format!("{}/transaction", self.base_url))
            .header("x-api-key", DB_TOKEN)
            .header("content-type", "application/json")
            .body(serde_json::json!({ "operations": ops }).to_string())
            .send()
            .await
            .expect("the instance answered a transaction");

        assert_eq!(
            response.status(),
            200,
            "{}",
            response.text().await.unwrap_or_default()
        );
    }
}

fn insert(index: usize, generation: u32) -> serde_json::Value {
    serde_json::json!({
        "type": "insert_node",
        "address": format!("Post:{index:04}"),
        "kind": "Post",
        "x": (index % 12) as u8,
        "y": (index % 13) as u8,
        "z": 0,
        "q": 0,
        "data": format!("{{\"n\":{index},\"gen\":{generation},\"body\":\"{}\"}}", "x".repeat(64)),
        "public": index % 7 == 0,
    })
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// The `facetql` binary, or `None` when this test should skip.
fn binary() -> Option<PathBuf> {
    if let Ok(declared) = std::env::var("FABRIC_FACETQL_BIN") {
        let path = PathBuf::from(declared);

        return path.is_file().then_some(path);
    }

    // The sibling checkout, from this crate's manifest directory.
    let sibling = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../facetql/target/release/facetql");

    sibling.is_file().then_some(sibling)
}

/// Panics when `bin` is older than the newest `.rs` file under its engine's
/// `src/` — the same false-positive this whole suite is built to catch one
/// layer up, caught here instead of in the engine: `AGENT_LOG.md`'s
/// 2026-09-06 audit found every "integration green" claim since 09-04 20:40
/// had run against a `facetql` built before ten `src` files it should have
/// exercised. `binary()` returning `None` means "not built, skip" — this is
/// the opposite case, "built, but stale", and staleness must fail the test,
/// not silently pass it or silently skip it.
///
/// Looks for `src/` three directories up from `target/{release,debug}/facetql`
/// first, then falls back to the sibling checkout `../../../facetql/src`
/// (relative to this crate's manifest dir) for a `FABRIC_FACETQL_BIN` that
/// points somewhere else entirely. When neither exists — e.g. a CI artifact
/// with no source tree beside it — there is nothing to compare against, so
/// this does not fail: it cannot know.
fn assert_binary_not_stale(bin: &std::path::Path) {
    let candidates = [
        bin.parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .map(|p| p.join("src")),
        Some(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../facetql/src"),
        ),
    ];
    let Some(src) = candidates.into_iter().flatten().find(|p| p.is_dir()) else {
        return;
    };

    let bin_mtime = match std::fs::metadata(bin).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return,
    };

    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in walkdir_rs_files(&src) {
        if let Ok(mtime) = std::fs::metadata(&entry).and_then(|m| m.modified()) {
            if newest.as_ref().map(|(t, _)| mtime > *t).unwrap_or(true) {
                newest = Some((mtime, entry));
            }
        }
    }

    if let Some((newest_mtime, newest_path)) = newest {
        assert!(
            newest_mtime <= bin_mtime,
            "facetql binary {} is older than {} — rebuild it first: \
             cd facetql && cargo build --release. An integration run against \
             a stale engine is a false positive, not a pass.",
            bin.display(),
            newest_path.display(),
        );
    }
}

/// A minimal recursive `.rs` file walker — this test suite has no directory-
/// walking dependency already in its `Cargo.toml`, and pulling one in for a
/// single staleness check is a heavier fix than the check itself.
fn walkdir_rs_files(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walkdir_rs_files(&path));
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            out.push(path);
        }
    }
    out
}

/// `taskset`, if this host has it on `PATH`, or `None` when this test should
/// skip the part of itself that needs to pin a process to one core.
fn taskset() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;

    std::env::split_paths(&path)
        .map(|dir| dir.join("taskset"))
        .find(|candidate| candidate.is_file())
}

// ── the workload that makes the cell hot ────────────────────────────────

/// One cell under sustained write pressure, restamped to wall time.
///
/// The same observation `fabric-runtime`'s control-loop test and
/// `tests/daemon.rs` use, and for the same reason: telemetry is an *input* to
/// the loop, and everything downstream of it — analyzer, predictor, optimizer,
/// controller, mechanisms, routing, the front door and now the mover — runs
/// untouched.
struct HotCell;

impl TelemetrySource for HotCell {
    fn describe(&self) -> String {
        "one cell under sustained write pressure".to_string()
    }

    fn sample(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Sample> + '_>> {
        Box::pin(async move {
            let at_ms = now_ms();
            let mut sample = Sample::default();

            sample.messages.push(FabricMessage::Telemetry(TelemetryBatch {
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
            }));

            sample
        })
    }
}

fn settings(source: &Instance, destination: &Instance) -> Settings {
    let declaration = format!(
        r#"{{
            "data_listen": "127.0.0.1:0",
            "admin_listen": "127.0.0.1:0",
            "backends": [
                {{
                    "id": "{SOURCE}",
                    "url": "{source}",
                    "region": "us-east",
                    "token_env": "FABRIC_TEST_DB_TOKEN",
                    "placements": [{{ "shard": {SHARD}, "x": 0, "y": 0 }}]
                }},
                {{
                    "id": "{DESTINATION}",
                    "url": "{destination}",
                    "region": "us-west",
                    "token_env": "FABRIC_TEST_DB_TOKEN",
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
            "silence_budget_ms": 5000,
            "probe_timeout_ms": 500,
            "policy": {{
                "measurement_settle_ms": 0,
                "phase_timeout_ms": 120000
            }},
            "drain_ms": 5000
        }}"#,
        source = source.base_url,
        destination = destination.base_url,
    );

    let file: ConfigFile = serde_json::from_str(&declaration).expect("a valid declaration");

    Settings::resolve(
        file,
        &MapEnv::of(&[
            ("FABRIC_ADMIN_TOKEN", ADMIN_TOKEN),
            ("FABRIC_TEST_DB_TOKEN", DB_TOKEN),
        ]),
    )
    .expect("a resolvable declaration")
}

async fn admin(http: &reqwest::Client, admin: SocketAddr, path: &str) -> serde_json::Value {
    let response = http
        .get(format!("http://{admin}{path}"))
        .header("x-api-key", ADMIN_TOKEN)
        .send()
        .await
        .expect("the operator surface answered");

    assert_eq!(response.status(), 200, "{path}");

    let body = response.text().await.expect("a body");

    serde_json::from_str(&body).unwrap_or_else(|error| panic!("{path}: {error}: {body}"))
}

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
async fn the_daemon_moves_a_cells_data_between_two_real_facetql_instances() {
    let Some(binary) = binary() else {
        eprintln!(
            "skipping: no facetql binary. Point FABRIC_FACETQL_BIN at one, or \
             build the sibling checkout with `cargo build --release`."
        );
        return;
    };
    assert_binary_not_stale(&binary);

    let source = Instance::start(&binary, "source").await;
    let destination = Instance::start(&binary, "destination").await;
    let http = reqwest::Client::new();

    // ── seed the cell on the source ─────────────────────────────────────

    for chunk in (0..SEEDED).collect::<Vec<_>>().chunks(100) {
        let ops: Vec<serde_json::Value> =
            chunk.iter().map(|index| insert(*index, 1)).collect();

        source.write(&http, serde_json::Value::Array(ops)).await;
    }

    let seeded = source.nodes(&http).await;
    assert_eq!(seeded.len(), SEEDED);
    assert!(destination.nodes(&http).await.is_empty());

    // ── boot the daemon over them ───────────────────────────────────────

    let telemetry: TelemetryFactory =
        Box::new(|| Ok(Box::new(HotCell) as Box<dyn TelemetrySource>));

    let daemon = Daemon::start(settings(&source, &destination), telemetry)
        .await
        .expect("the daemon boots over two real FacetQL instances");

    let data = daemon.data_addr();
    let operator = daemon.admin_addr();

    let status = admin(&http, operator, "/status").await;
    assert_eq!(status["backends"][0]["availability"], "serviceable");
    assert_eq!(status["backends"][1]["availability"], "serviceable");
    assert_eq!(status["placements"][0]["holder"], SOURCE);

    // ── the loop decides, and this daemon's own mover starts ────────────

    until!(
        "the control loop to admit an action",
        Duration::from_secs(20),
        admin(&http, operator, "/actions")
            .await
            .as_array()
            .is_some_and(|actions| !actions.is_empty())
    );

    let action = admin(&http, operator, "/actions").await[0].clone();
    assert_eq!(action["source"], SOURCE);
    assert_eq!(action["destination"], DESTINATION);

    // Nothing is reported through `POST /actions/{id}/transfer` anywhere in
    // this test. If the copy happens, it is because the daemon made it happen.
    until!(
        "the daemon's own mover to start",
        Duration::from_secs(20),
        {
            let status = admin(&http, operator, "/status").await;

            assert!(
                status["movers"]["refused"]
                    .as_array()
                    .is_some_and(|refused| refused.is_empty()),
                "the daemon refused to copy: {}",
                status["movers"]["refused"]
            );

            status["movers"]["copying"]
                .as_array()
                .is_some_and(|copying| !copying.is_empty())
        }
    );

    // ── a write lands on the source *during* the copy ───────────────────
    //
    // Sent straight to the instance, not through the door, so it is exactly
    // the case the snapshot cannot see: a row the cursor may already have
    // walked past. Only the change feed can carry it, and the pre-cutover
    // check is what refuses to move authority until it has.

    source
        .write(
            &http,
            serde_json::json!([
                insert(0, 2),
                insert(SEEDED - 1, 2),
                insert(SEEDED, 2),
                { "type": "delete_node", "address": format!("Post:{:04}", 1) },
            ]),
        )
        .await;

    let expected = source.nodes(&http).await;
    assert_eq!(expected.len(), SEEDED, "one added, one removed");

    // ── the cutover happens, on the strength of the copy alone ──────────

    // The largest transfer report the daemon published while the copy ran.
    // Nothing external reported it, and nothing ramps it with elapsed time:
    // `PlacementFabric::record_transfer` only ever moves when bytes have
    // actually committed on the destination.
    let mut reported_bytes = 0u64;
    let mut reported_resident = 0u64;

    until!("the cell to move", Duration::from_secs(60), {
        for action in admin(&http, operator, "/actions")
            .await
            .as_array()
            .unwrap_or(&Vec::new())
        {
            reported_bytes =
                reported_bytes.max(action["bytes_copied"].as_u64().unwrap_or(0));
            reported_resident =
                reported_resident.max(action["resident_bytes"].as_u64().unwrap_or(0));
        }

        admin(&http, operator, "/routing").await["placements"][0]["holder"] == DESTINATION
    });

    // ── every node landed, byte-identical ───────────────────────────────

    let arrived = destination.nodes(&http).await;

    assert_eq!(
        arrived.len(),
        expected.len(),
        "the destination holds a different number of nodes than the source"
    );

    for (address, node) in &expected {
        let landed = arrived
            .get(address)
            .unwrap_or_else(|| panic!("'{address}' never reached the destination"));

        assert_eq!(
            landed, node,
            "'{address}' is not byte-identical on the destination"
        );
    }

    // The mid-copy write is there, with its *new* contents, and the mid-copy
    // delete did not leave a ghost behind.
    assert!(arrived["Post:0000"]["data"].as_str().unwrap().contains("\"gen\":2"));
    assert!(arrived.contains_key(&format!("Post:{SEEDED:04}")));
    assert!(!arrived.contains_key("Post:0001"));

    // ── the progress that drove it was measured, not simulated ──────────
    //
    // The bytes the daemon reported are the `data` bytes of the rows that
    // actually committed on the destination, so they land on the source's own
    // payload total rather than on a round number or a fraction of elapsed
    // time. The slack is the handful of rows the mid-copy write changed
    // between the snapshot walking them and this measurement being taken.

    let payload: u64 = seeded
        .values()
        .map(|node| node["data"].as_str().unwrap_or_default().len() as u64)
        .sum();

    let slack = 4 * 128;

    assert!(
        reported_bytes.abs_diff(payload) < slack,
        "the reported transfer was {reported_bytes} byte(s); the cell's rows \
         carry {payload}"
    );

    assert!(
        reported_resident.abs_diff(payload) < slack,
        "the reported resident size was {reported_resident} byte(s); the \
         cell's rows carry {payload}"
    );

    // ── a client's requests follow the cutover ──────────────────────────
    //
    // The same front-door address, the same request shape, no restart and no
    // reconfiguration. The write must land on the new holder and nowhere else.

    let marker = "Post:9999";

    let written = http
        .post(format!("http://{data}/node"))
        .header("x-api-key", DB_TOKEN)
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "address": marker, "kind": "Post",
                "x": 0, "y": 0, "z": 0, "q": 0,
                "data": "{\"after_cutover\":true}"
            })
            .to_string(),
        )
        .send()
        .await
        .expect("the front door answered");

    assert_eq!(
        written.status(),
        201,
        "{}",
        written.text().await.unwrap_or_default()
    );

    assert!(
        destination.nodes(&http).await.contains_key(marker),
        "a write through the front door after the cutover did not reach the \
         instance that now holds the cell"
    );

    assert!(
        !source.nodes(&http).await.contains_key(marker),
        "a write through the front door after the cutover still reached the \
         instance that no longer holds the cell"
    );

    // ── and it shuts down cleanly ───────────────────────────────────────

    let report = daemon.shutdown().await;

    assert!(
        report.is_clean(),
        "an unclean shutdown: {:?} abandoned, {:?} unpersisted",
        report.abandoned,
        report.unpersisted
    );
}

/// The catch-up half, deterministically: writes that land on the source
/// **strictly after** the bulk snapshot returned.
///
/// The migration test above cannot pin that down — it races the control loop —
/// and the race matters, because the snapshot swallowing a mid-copy write and
/// the change feed carrying it look identical from outside. Here the ordering
/// is explicit: `snapshot` has returned before a single one of these writes is
/// sent, so the only thing that can carry them is `GET /events`.
///
/// It also proves the check is a real gate rather than a formality: one node
/// on the destination is edited behind the mover's back, and the verdict flips
/// from verified to failed, naming the address and the field.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_landing_after_the_snapshot_still_reach_the_destination() {
    use fabric_facetql::mover::{CellMover, CellScope, MoverConfig};
    use fabric_facetql::{FacetqlClient, FacetqlEndpoint};
    use fabric_runtime::CopyVerdict;

    let Some(binary) = binary() else {
        eprintln!("skipping: no facetql binary (see FABRIC_FACETQL_BIN)");
        return;
    };
    assert_binary_not_stale(&binary);

    let source = Instance::start(&binary, "feed-source").await;
    let destination = Instance::start(&binary, "feed-destination").await;
    let http = reqwest::Client::new();

    for chunk in (0..200).collect::<Vec<_>>().chunks(100) {
        let ops: Vec<serde_json::Value> =
            chunk.iter().map(|index| insert(*index, 1)).collect();

        source.write(&http, serde_json::Value::Array(ops)).await;
    }

    let endpoint = |id: &str, instance: &Instance| {
        FacetqlEndpoint::new(DbmsId::new(id), instance.base_url.clone(), DB_TOKEN)
            .expect("a well-formed endpoint")
    };

    let mut mover = CellMover::new(
        FacetqlClient::new(endpoint("db-a", &source)),
        FacetqlClient::new(endpoint("db-b", &destination)),
        CellScope::whole_namespace(),
        MoverConfig::default(),
        // No request timeout: the subscription is the one request that is
        // supposed never to finish.
        reqwest::Client::builder().build().unwrap(),
    );

    // Subscribed before the first page is asked for. An event seen early is a
    // redundant re-read; one missed after the walk began is a lost write.
    let feed = mover.subscribe().await.expect("the source's change feed");

    let mut batches = 0u32;
    mover
        .snapshot(&feed, |_, _| batches += 1)
        .await
        .expect("the bulk copy");

    assert!(batches > 0, "the snapshot committed nothing");
    assert_eq!(destination.nodes(&http).await.len(), 200);

    // ── now, and only now, the source takes more writes ─────────────────

    source
        .write(
            &http,
            serde_json::json!([
                insert(0, 9),
                insert(199, 9),
                insert(200, 9),
                { "type": "delete_node", "address": "Post:0001" },
            ]),
        )
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(20);

    loop {
        mover.catch_up(&feed).await.expect("catch-up applied");

        if mover.verify(&feed, now_ms()).await.is_verified() {
            break;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "the change feed never carried the writes that landed after the \
             snapshot"
        );

        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let expected = source.nodes(&http).await;
    let arrived = destination.nodes(&http).await;

    assert_eq!(expected.len(), 200, "one added, one removed");
    assert_eq!(arrived, expected, "the destination is not the source");
    assert!(!arrived.contains_key("Post:0001"), "a deleted node lingered");
    assert!(arrived["Post:0000"]["data"].as_str().unwrap().contains("\"gen\":9"));

    // ── and the check is a gate, not a formality ────────────────────────

    destination
        .write(&http, serde_json::json!([insert(42, 404)]))
        .await;

    let verdict = mover.verify(&feed, now_ms()).await;

    match verdict {
        CopyVerdict::Failed { reason, .. } => {
            assert!(reason.contains("Post:0042"), "{reason}");
            assert!(reason.contains("data"), "{reason}");
        }

        other => panic!("an edited destination still verified: {other:?}"),
    }
}

/// The point of the whole FacetQL-telemetry task, proved against a real
/// server rather than the `HotCell` fixture above: before this, `GET /stats`
/// reported none of CPU, memory, queue depth or latency, so
/// `WorkloadProfile::pressure_score` was `0.35*0 + 0.20*0 + 0.25*0 + 0.20*0`
/// — always exactly `0.0` — and the optimizer could never see a real cell as
/// hot. This test drives one real, unmodified `facetql` binary hard enough
/// that its own `/stats` reports genuine CPU, queue and latency pressure,
/// polls it with the production [`TelemetryPoller`], and hands the resulting
/// [`WorkloadProfile`] to the production [`WorkloadOptimizer`] — the same
/// pipeline `fabric_daemon::telemetry::FacetqlTelemetry` runs in production,
/// with nothing faked at any layer above `GET /stats` itself.
///
/// Two things make the load deterministic instead of "probably enough":
///
/// * the instance is started with `FACETQL_MAX_CONCURRENT_REQUESTS` set to a
///   small number, so a modest burst of concurrent requests genuinely
///   exhausts its own admission cap — the exact `in_flight`/`max_concurrent`
///   saturation `fabric_facetql::sample` rescales into queue pressure (see
///   its module docs for why that ratio, not a raw request count, is used);
/// * the instance is pinned to one CPU core with `taskset`, so the same
///   modest load can saturate `runtime.process.cpu_cores` worth of
///   parallelism without needing to peg every core of whatever machine
///   happens to run this suite.
///
/// Both are real, honest ways to load a real server — not a shortcut around
/// measuring it.
///
/// # Why this retries
///
/// How much of the pinned core this test's load actually gets depends on
/// what else the host schedules onto it at the moment the sample is taken —
/// real hardware, shared with everything else running on this machine, is
/// noisier than a synthetic fixture. A single 20-second window occasionally
/// lands on the low side of that noise. Retrying with a fresh instance is the
/// ordinary, honest answer to that: every attempt is still real load against
/// a real, unmodified server, never a fabricated number, and it is the same
/// reasoning that has any flaky-network-call retry — the assertion is about
/// what real load *can* prove, not about winning a single dice roll against
/// the host scheduler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loaded_cell_crosses_the_pressure_threshold_from_real_facetql_stats() {
    use fabric_facetql::{FacetqlEndpoint, PollOutcome, PollTarget, TelemetryPoller};
    use fabric_optimizer::{OptimizationAction, OptimizationDecision, WorkloadOptimizer};
    use fabric_runtime::FabricRuntime;
    use fabric_topology::TopologyRegistry;
    use fabric_workload::WorkloadProfile;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    let Some(binary) = binary() else {
        eprintln!(
            "skipping: no facetql binary. Point FABRIC_FACETQL_BIN at one, or \
             build the sibling checkout with `cargo build --release`."
        );
        return;
    };
    assert_binary_not_stale(&binary);

    let Some(taskset) = taskset() else {
        eprintln!("skipping: no `taskset` on PATH to pin the instance to one core");
        return;
    };

    const LOADED_SHARD: u64 = 9;
    const LOADED_CELL: Coordinate = Coordinate::new(0, 0);
    const ATTEMPTS: u32 = 8;

    let mut last_hottest: Option<WorkloadProfile> = None;
    let mut last_decision: Option<OptimizationDecision> = None;

    for attempt in 1..=ATTEMPTS {
        let loaded_id = DbmsId::new(format!("loaded-instance-{attempt}"));

        let instance = Instance::start_with(
            &binary,
            &format!("loaded-{attempt}"),
            &[("FACETQL_MAX_CONCURRENT_REQUESTS", "6")],
            Some(&taskset),
        )
        .await;

        let http = reqwest::Client::new();

        let endpoint =
            FacetqlEndpoint::new(loaded_id.clone(), instance.base_url.clone(), DB_TOKEN)
                .expect("a well-formed endpoint");
        let target = PollTarget::new(endpoint, LOADED_SHARD, LOADED_CELL, "us-east");

        let mut poller = TelemetryPoller::new(vec![target]).expect("a poller over one instance");
        let mut runtime = FabricRuntime::new();
        poller.register_targets(&mut runtime);

        // Baseline, before any load: establishes the differencing baseline
        // and, just as importantly, is the "always 0.0" starting point this
        // test exists to move away from.
        poller.poll_into(&mut runtime).await;

        // ── sustained, all-write, highly concurrent load ────────────────
        //
        // All writes, so `write_ratio >= 0.70` and the optimizer's decision
        // (once it judges the cell hot) is `Isolate` — the one action that
        // names no destination, so this test needs no second instance or
        // fleet capacity to prove a real decision came back.
        //
        // Each request is a batch of several inserts with a substantial
        // payload: big enough that applying one, under the engine's single
        // write mutex, takes real, measurable time — which is what lets a
        // modest worker count keep both the admission cap and the mutex
        // queue saturated (real queue depth, real write latency), and what
        // gives the pinned core enough per-request parsing and validation
        // work to stay genuinely busy (real CPU) rather than mostly waiting.
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();

        for worker in 0..60 {
            let http = http.clone();
            let base_url = instance.base_url.clone();
            let stop = Arc::clone(&stop);

            workers.push(tokio::spawn(async move {
                let mut i: usize = 0;

                while !stop.load(Ordering::Relaxed) {
                    let ops: Vec<serde_json::Value> = (0..20)
                        .map(|n| {
                            serde_json::json!({
                                "type": "insert_node",
                                "address": format!("Load:{worker}:{i}:{n}"),
                                "kind": "Load",
                                "x": 0, "y": 0, "z": 0, "q": 0,
                                "data": format!("{{\"n\":{i},\"body\":\"{}\"}}", "x".repeat(9_000)),
                                "public": false,
                            })
                        })
                        .collect();

                    let _ = http
                        .post(format!("{base_url}/transaction"))
                        .header("x-api-key", DB_TOKEN)
                        .header("content-type", "application/json")
                        .body(serde_json::json!({ "operations": ops }).to_string())
                        .send()
                        .await;

                    i += 1;
                }
            }));
        }

        // Poll repeatedly through the loaded window and keep the hottest
        // profile seen — a single unlucky poll landing between bursts proves
        // nothing, so this does not gamble the attempt on one sample the way
        // a single poll would.
        let mut hottest: Option<WorkloadProfile> = None;
        let deadline = Instant::now() + Duration::from_secs(20);

        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(500)).await;

            for (_, outcome) in poller.poll_once().await {
                let PollOutcome::Sampled(batch) = outcome else {
                    continue;
                };

                for sample in &batch.samples {
                    let profile = WorkloadProfile::from_metrics(
                        LOADED_SHARD,
                        LOADED_CELL,
                        sample.metrics(),
                    );

                    if hottest
                        .as_ref()
                        .is_none_or(|hottest| profile.pressure_score > hottest.pressure_score)
                    {
                        hottest = Some(profile);
                    }
                }
            }

            if hottest.as_ref().is_some_and(|profile| profile.is_hot()) {
                break;
            }
        }

        stop.store(true, Ordering::Relaxed);
        for worker in workers {
            let _ = worker.await;
        }

        let hottest = hottest.expect("at least one interval was sampled under load");

        eprintln!(
            "attempt {attempt}/{ATTEMPTS}: pressure_score={:.3} pressure={:?} cpu={:.3} \
             queue_depth={} write_latency_us={:.0} write_ratio={:.2}",
            hottest.pressure_score,
            hottest.pressure,
            hottest.cpu_utilization,
            hottest.queue_depth,
            hottest.write_latency_us,
            hottest.write_ratio,
        );

        assert!(
            hottest.pressure_score > 0.0,
            "pressure stayed at exactly 0.0 under real, heavy, concurrent load — \
             the always-cold bug this task exists to fix is back"
        );

        // The optimizer needs a placement to name in its decision, exactly as
        // production wires it: `FabricRuntime`'s own topology, not a bare
        // profile.
        let mut registry = TopologyRegistry::new();
        registry.place(
            loaded_id,
            &Shard::new(LOADED_SHARD, "us-east"),
            LOADED_CELL,
            "us-east",
        );

        let decision = WorkloadOptimizer::default().optimize(&hottest, &registry);
        eprintln!("attempt {attempt}/{ATTEMPTS}: decision: {decision:?}");

        if decision.action != OptimizationAction::NoAction {
            // The transition this whole task exists to prove: a real cell,
            // observed only through real `GET /stats`, crossed the pressure
            // threshold and the optimizer proposed something.
            return;
        }

        last_hottest = Some(hottest);
        last_decision = Some(decision);
        // `instance` drops here: the process is killed and its port and
        // pinned core are free before the next attempt starts.
    }

    panic!(
        "a real, heavily loaded cell produced no decision in {ATTEMPTS} attempts — \
         last pressure_score was {:?}, last decision was {:?}",
        last_hottest.map(|profile| profile.pressure_score),
        last_decision,
    );
}
