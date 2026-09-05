//! End-to-end tests against a **real, running FacetQL**.
//!
//! Unit tests can prove the bytes on the wire match the contract; only a real
//! server can prove the contract was read correctly. These tests therefore do
//! the actual thing: read `/stats`, write and read back a placement, and make
//! the engine's native compare-and-set refuse a stale update.
//!
//! They are opt-in. Without `FABRIC_FACETQL_URL` and `FABRIC_FACETQL_TOKEN`
//! each test returns immediately, so `cargo test` on a machine with no server
//! passes — an integration test that silently becomes a no-op is worse than
//! one that says it skipped, so each prints why.
//!
//! ```text
//! rm -rf /tmp/fab && mkdir -p /tmp/fab
//! ENOCHIAN_DATA_DIR=/tmp/fab ENOCHIAN_PORT=8892 ENOCHIAN_TOKENS="fabtok:fabric:admin" \
//!   ./facetql/target/release/facetql start &
//! FABRIC_FACETQL_URL=http://127.0.0.1:8892 FABRIC_FACETQL_TOKEN=fabtok \
//!   cargo test -p fabric-facetql --test live -- --test-threads=1
//! ```
//!
//! The token is admin because `GET /stats` is admin-gated.

use std::time::Duration;

use fabric_core::{Coordinate, DbmsId};
use fabric_facetql::{
    FacetqlClient, FacetqlEndpoint, FacetqlError, PlacementStore, PollTarget, TelemetryPoller,
};
use fabric_runtime::{FabricRuntime, NodeHealth, DEFAULT_HEARTBEAT_DEADLINE_MS};
use fabric_topology::Placement;

/// Build a client against the configured server, or `None` when the test
/// should skip.
fn client(id: &str) -> Option<FacetqlClient> {
    let url = std::env::var("FABRIC_FACETQL_URL").ok()?;
    let token = std::env::var("FABRIC_FACETQL_TOKEN").ok()?;

    let endpoint = FacetqlEndpoint::new(DbmsId::new(id), url, token)
        .expect("a configured endpoint should be well-formed");

    Some(FacetqlClient::new(endpoint))
}

macro_rules! server_or_skip {
    ($id:expr) => {
        match client($id) {
            Some(client) => client,
            None => {
                eprintln!(
                    "skipping: set FABRIC_FACETQL_URL and FABRIC_FACETQL_TOKEN to run \
                     this against a live FacetQL"
                );
                return;
            }
        }
    };
}

fn placement(shard_id: u64, dbms: &str, region: &str) -> Placement {
    Placement {
        dbms_id: DbmsId::new(dbms),
        shard_id,
        coordinate: Coordinate::new(1, 2),
        region: region.to_string(),
    }
}

#[tokio::test]
async fn stats_reads_the_engines_own_counters() {
    let client = server_or_skip!("db-live");

    let first = client.stats().await.expect("GET /stats");
    let second = client.stats().await.expect("GET /stats");

    // Reading /stats is itself not a node read, but the counters are
    // monotonic and the storage block is always present.
    assert!(second.reads_total >= first.reads_total);
    assert!(second.writes_total >= first.writes_total);
    assert!(second.storage.page_size > 0);

    println!(
        "stats: nodes={} edges={} kinds={} reads_total={} writes_total={} pages={}",
        second.node_count,
        second.edge_count,
        second.kinds.len(),
        second.reads_total,
        second.writes_total,
        second.storage.pages,
    );
}

#[tokio::test]
async fn a_placement_is_written_and_read_back() {
    let client = server_or_skip!("db-live");
    let store = PlacementStore::new(client);
    let shard = 9_001;

    let stored = store
        .create(&placement(shard, "db-a", "us-east"))
        .await
        .expect("create a fresh placement");
    assert_eq!(stored.version, 1);

    let read_back = store
        .get(shard, Coordinate::new(1, 2))
        .await
        .expect("query the placement kind")
        .expect("the placement we just wrote");
    assert_eq!(read_back, stored);
    assert_eq!(read_back.placement.dbms_id, DbmsId::new("db-a"));
    assert_eq!(read_back.address(), format!("__fabric_placement:{shard}:1:2"));

    // Loading straight into a TopologyRegistry is what a controller does on
    // start-up: the durable map becomes the live one.
    let (registry, versions) = store.load_registry().await.expect("load the registry");
    let located = registry
        .locate(shard, Coordinate::new(1, 2))
        .expect("the placement is in the registry");
    assert_eq!(located.dbms_id, DbmsId::new("db-a"));
    assert_eq!(versions[&read_back.address()].version, 1);

    // A second create of the same cell loses the race rather than silently
    // overwriting the first.
    let conflict = store
        .create(&placement(shard, "db-b", "eu-west"))
        .await
        .unwrap_err();
    assert!(matches!(conflict, FacetqlError::Conflict(_)), "{conflict}");

    store.remove(&read_back).await.expect("clean up");
}

#[tokio::test]
async fn a_stale_update_is_refused_by_the_engines_compare_and_set() {
    let client = server_or_skip!("db-live");
    let store = PlacementStore::new(client);
    let shard = 9_002;

    let v1 = store
        .create(&placement(shard, "db-a", "us-east"))
        .await
        .expect("create");

    // Controller A moves the cell. It presents version 1 and wins.
    let v2 = store
        .update(&v1, &placement(shard, "db-b", "eu-west"))
        .await
        .expect("the first update presents the current version and wins");
    assert_eq!(v2.version, 2);

    // Controller B still holds the version-1 read and tries the same move.
    // FacetQL's set_if refuses it: 412, and nothing in the batch applied.
    let err = store
        .update(&v1, &placement(shard, "db-c", "ap-south"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, FacetqlError::PreconditionFailed(_)),
        "a stale update must be refused, got: {err}"
    );
    println!("stale update refused: {err}");

    // ...and the refusal changed nothing: the winner's move stands.
    let current = store
        .get(shard, Coordinate::new(1, 2))
        .await
        .expect("query")
        .expect("still there");
    assert_eq!(current.version, 2);
    assert_eq!(current.placement.dbms_id, DbmsId::new("db-b"));
    assert_eq!(current.placement.region, "eu-west");

    // A CAS-guarded delete is refused on a stale version too, and the node
    // survives it — the set_if and the delete_node are one all-or-nothing
    // batch, so a failed precondition cannot leave the node half-removed.
    let stale_remove = store.remove(&v1).await.unwrap_err();
    assert!(
        matches!(stale_remove, FacetqlError::PreconditionFailed(_)),
        "a stale remove must be refused, got: {stale_remove}"
    );
    println!("stale remove refused: {stale_remove}");
    assert!(store
        .get(shard, Coordinate::new(1, 2))
        .await
        .expect("query")
        .is_some());

    // Presenting the current version removes it.
    store.remove(&current).await.expect("remove at version 2");
    assert!(store
        .get(shard, Coordinate::new(1, 2))
        .await
        .expect("query")
        .is_none());
}

#[tokio::test]
async fn polling_stats_drives_the_existing_optimizer_path() {
    let client = server_or_skip!("db-live");
    let endpoint = client.endpoint().clone();

    let target = PollTarget::new(endpoint, 9_003, Coordinate::new(4, 5), "us-east");
    let mut poller = TelemetryPoller::new(vec![target]).expect("build poller");

    let mut runtime = FabricRuntime::new();
    poller.register_targets(&mut runtime);
    assert_eq!(runtime.nodes().len(), 1);

    // Registration alone is not proof of life.
    let node = runtime.nodes().get(&DbmsId::new("db-live")).unwrap();
    assert_eq!(
        node.health(runtime.clock_ms(), DEFAULT_HEARTBEAT_DEADLINE_MS),
        NodeHealth::Unreachable
    );

    // First round establishes the baseline; second differences against it.
    poller.poll_into(&mut runtime).await;

    // Generate some traffic so the interval is not empty.
    for _ in 0..25 {
        let _ = client.list_nodes(Some("__fabric_placement"), None, Some(1), None).await;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let outcomes = poller.poll_into(&mut runtime).await;
    assert!(outcomes[0].1.is_healthy());

    let node = runtime.nodes().get(&DbmsId::new("db-live")).unwrap();
    assert_eq!(
        node.health(runtime.clock_ms(), DEFAULT_HEARTBEAT_DEADLINE_MS),
        NodeHealth::Healthy
    );

    // The observation reached the analyzer, and the same optimizer/predictor
    // the CLI's `predict` command uses scores it. One pipeline, not a second.
    let profiles: Vec<_> = runtime.analyzer().profiles().cloned().collect();
    assert_eq!(profiles.len(), 1, "one target, one profile");

    let profile = &profiles[0];
    assert!(
        profile.operations_per_second > 0.0,
        "the interval carried real traffic"
    );
    assert_eq!(profile.coordinate, Coordinate::new(4, 5));

    let prediction = runtime.optimizer().predictor().predict_hotspot(profile);
    let decision = runtime.optimize(profile);

    println!(
        "polled: ops/s={:.2} read_ratio={:.2} pressure={:?} hotspot_p={:.3} action={:?}",
        profile.operations_per_second,
        profile.read_ratio,
        profile.pressure,
        prediction.probability,
        decision.action,
    );
}

#[tokio::test]
async fn a_bad_token_fails_closed_rather_than_reading_as_healthy() {
    let url = match std::env::var("FABRIC_FACETQL_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping: FABRIC_FACETQL_URL not set");
            return;
        }
    };

    let endpoint = FacetqlEndpoint::new(DbmsId::new("db-badtoken"), url, "not-a-real-token")
        .expect("well-formed endpoint");

    let err = FacetqlClient::new(endpoint.clone())
        .stats()
        .await
        .unwrap_err();
    assert!(
        matches!(err, FacetqlError::Unauthorized { .. }),
        "expected an auth refusal, got: {err}"
    );
    assert!(err.implies_unhealthy());
    assert!(
        !err.to_string().contains("not-a-real-token"),
        "the token must not appear in an error: {err}"
    );
    println!("unauthenticated instance failed closed: {err}");

    let target = PollTarget::new(endpoint, 9_004, Coordinate::new(0, 0), "us-east");
    let mut poller = TelemetryPoller::new(vec![target]).expect("build poller");
    let mut runtime = FabricRuntime::new();
    poller.register_targets(&mut runtime);

    let outcomes = poller.poll_into(&mut runtime).await;
    assert!(!outcomes[0].1.is_healthy());

    let node = runtime.nodes().get(&DbmsId::new("db-badtoken")).unwrap();
    assert!(!node
        .health(runtime.clock_ms(), DEFAULT_HEARTBEAT_DEADLINE_MS)
        .is_serviceable());
    assert_eq!(runtime.analyzer().len(), 0, "no telemetry was invented");
}

/// Walking a kind larger than one page must follow the cursor. FacetQL caps a
/// deep `offset` at 10,000 and an offset walk is unstable under concurrent
/// writes, so this proves the client actually paginates the way the contract
/// says to — a single 500-row page would pass a smaller test by accident.
#[tokio::test]
async fn a_kind_larger_than_one_page_is_walked_by_cursor() {
    let client = server_or_skip!("db-live");
    let store = PlacementStore::new(client.clone());
    let shard = 9_005;

    // 600 placements: more than the 500-row page limit, so the walk must
    // follow `next` at least once.
    // One placement per shard: the address is `(shard, x, y)`, and the fabric
    // grid has only 156 cells, so varying the shard is what makes 600 distinct
    // rows without leaving the grid.
    let mut created = Vec::with_capacity(600);
    for row in 0..600u64 {
        created.push(
            store
                .create(&Placement {
                    dbms_id: DbmsId::new(format!("db-{row}")),
                    shard_id: shard + row,
                    coordinate: Coordinate::new(1, 2),
                    region: "us-east".to_string(),
                })
                .await
                .expect("create"),
        );
    }

    let loaded = store.load().await.expect("cursor-paged load");
    let ours: Vec<_> = loaded
        .iter()
        .filter(|entry| entry.placement.shard_id >= shard && entry.placement.shard_id < shard + 600)
        .collect();
    assert_eq!(ours.len(), 600, "every row came back across page boundaries");

    // No duplicates and no skips: an offset walk under concurrent writes is
    // exactly what produces those.
    let mut addresses: Vec<String> = ours.iter().map(|entry| entry.address()).collect();
    addresses.sort();
    addresses.dedup();
    assert_eq!(addresses.len(), 600);

    for entry in created {
        store.remove(&entry).await.expect("clean up");
    }
}

/// `POST /node/:address/claim` is the atomic claim primitive. Two claims of
/// the same address must produce exactly one winner.
#[tokio::test]
async fn claim_has_exactly_one_winner() {
    let client = server_or_skip!("db-live");
    let store = PlacementStore::new(client.clone());
    let shard = 9_900;

    let stored = store
        .create(&placement(shard, "db-a", "us-east"))
        .await
        .expect("create");
    let address = stored.address();

    assert!(client.claim(&address).await.expect("first claim"));
    assert!(
        !client.claim(&address).await.expect("second claim"),
        "a second claim of a held address must lose, not error"
    );

    // A claim on an address that does not exist is a 404, not a silent win.
    let missing = client.claim("__fabric_placement:0:99:99").await.unwrap_err();
    assert!(matches!(missing, FacetqlError::NotFound(_)), "{missing}");

    // GET /nodes is the offset API; it is part of the contract and still works.
    let listed = client
        .list_nodes(Some("__fabric_placement"), None, Some(500), Some(0))
        .await
        .expect("GET /nodes");
    assert!(listed.iter().any(|node| node.address == address));

    store.remove(&stored).await.expect("clean up");
}
