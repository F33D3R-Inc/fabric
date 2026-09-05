//! The front door, end to end, over real sockets.
//!
//! Every request in this file is one `fct/runtime/fqclient.go` actually emits,
//! byte for byte: the same paths, the same bodies, the same `x-api-key`
//! header, the same idea of what a response looks like. The backends are real
//! HTTP servers that record what arrived and answer FacetQL's own shapes, so
//! what is proved here is the property the whole design rests on — **`fqStore`
//! cannot tell the front door from a FacetQL** — plus the four ways the front
//! door is allowed to say no.
//!
//! What is deliberately NOT faked: the routing. The routing table, the
//! migration in cutover and the replica set with no primary are the real
//! `fabric-routing`, `fabric-migration` and `fabric-replication` types, driven
//! through their real state machines. The refusals below are the ones the
//! control loop actually produces.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_facetql::frontdoor::{Backend, FrontDoor, FrontDoorConfig, Keyspace, KeyspaceRule};
use fabric_migration::{Migration, MigrationId, MigrationPlan, MigrationReason};
use fabric_replication::{ReplicaSet, ReplicationFactor};
use fabric_routing::{RoutingKey, RoutingTable};
use fabric_topology::TopologyRegistry;

// ── a FacetQL that records what it was asked ────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    method: String,
    /// The raw request target: path and query exactly as they arrived.
    target: String,
    api_key: Option<String>,
    body: String,
}

#[derive(Clone)]
struct Fake {
    base_url: String,
    log: Arc<Mutex<Vec<Recorded>>>,
    reply: Arc<Mutex<(u16, String)>>,
    /// SSE frames this instance emits on `GET /events`, when it serves that
    /// path at all.
    events: Arc<Mutex<Option<Vec<String>>>>,
    stop: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl Fake {
    async fn spawn() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();

        let fake = Self {
            base_url: format!("http://{address}"),
            log: Arc::new(Mutex::new(Vec::new())),
            reply: Arc::new(Mutex::new((200, "FacetQL Online".to_string()))),
            events: Arc::new(Mutex::new(None)),
            stop: Arc::new(Mutex::new(None)),
        };

        let (stop, stopped) = tokio::sync::oneshot::channel();
        *fake.stop.lock().unwrap() = Some(stop);

        let router = Router::new()
            .fallback(any(record))
            .with_state(fake.clone());

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

    fn answers(&self, status: u16, body: &str) {
        *self.reply.lock().unwrap() = (status, body.to_string());
    }

    fn emits(&self, frames: &[&str]) {
        *self.events.lock().unwrap() =
            Some(frames.iter().map(|frame| frame.to_string()).collect());
    }

    fn seen(&self) -> Vec<Recorded> {
        self.log.lock().unwrap().clone()
    }

    fn saw_nothing(&self) -> bool {
        self.log.lock().unwrap().is_empty()
    }

    fn forget(&self) {
        self.log.lock().unwrap().clear();
    }

    async fn shut_down(&self) {
        if let Some(stop) = self.stop.lock().unwrap().take() {
            stop.send(()).ok();
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn record(State(fake): State<Fake>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 4 * 1024 * 1024)
        .await
        .unwrap_or_default();

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
        body: String::from_utf8_lossy(&body).into_owned(),
    });

    if parts.uri.path() == "/events" {
        let frames = fake.events.lock().unwrap().clone();

        if let Some(frames) = frames {
            /*
             * A real subscription stays open after its frames, which is the
             * condition the merge has to work under: the client must see one
             * instance's event without waiting for another instance to say
             * anything.
             */
            use futures_util::StreamExt;

            let stream = futures_util::stream::iter(
                frames
                    .into_iter()
                    .map(|frame| Ok::<_, std::io::Error>(axum::body::Bytes::from(frame))),
            )
            .chain(futures_util::stream::pending());

            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(axum::body::Body::from_stream(stream))
                .unwrap();
        }
    }

    let (status, body) = fake.reply.lock().unwrap().clone();

    (
        StatusCode::from_u16(status).unwrap(),
        [("content-type", "application/json")],
        body,
    )
        .into_response()
}

// ── the fleet under test ────────────────────────────────────────────────

const TOKEN: &str = "an-identity-token";

fn key(shard_id: u64) -> RoutingKey {
    RoutingKey::new(shard_id, Coordinate::new(0, 0)).unwrap()
}

/// Two kinds on two shards on two instances. Nothing here is a hash of an
/// address: the operator declared where `Post` and `User` live.
fn keyspace() -> Keyspace {
    Keyspace::new()
        .with_rule(KeyspaceRule::new("Post", "Post:", key(1)).unwrap())
        .unwrap()
        .with_rule(KeyspaceRule::new("User", "User:", key(2)).unwrap())
        .unwrap()
}

fn routing() -> RoutingTable {
    let mut registry = TopologyRegistry::new();

    registry.place(
        DbmsId::new("db-a"),
        &Shard::new(1, "app"),
        Coordinate::new(0, 0),
        "eu",
    );
    registry.place(
        DbmsId::new("db-b"),
        &Shard::new(2, "app"),
        Coordinate::new(0, 0),
        "us",
    );

    let mut routing = RoutingTable::new();
    routing.apply_placements(&registry);
    routing
}

struct Fleet {
    a: Fake,
    b: Fake,
    door: FrontDoor,
    url: String,
    client: reqwest::Client,
}

impl Fleet {
    async fn spawn() -> Self {
        let a = Fake::spawn().await;
        let b = Fake::spawn().await;

        let door = FrontDoor::with_config(
            keyspace(),
            vec![
                Backend::new(DbmsId::new("db-a"), &a.base_url).unwrap(),
                Backend::new(DbmsId::new("db-b"), &b.base_url).unwrap(),
            ],
            routing(),
            FrontDoorConfig {
                // Short enough to finish a test, long enough to absorb a
                // cutover: the property is the shape, not the number.
                fence_retry_budget: Duration::from_millis(400),
                fence_retry_interval: Duration::from_millis(20),
                upstream_timeout: Duration::from_secs(5),
                ..FrontDoorConfig::default()
            },
        )
        .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();
        let url = format!("http://{address}");

        let serving = door.clone();
        tokio::spawn(async move { serving.serve(listener).await });

        Self {
            a,
            b,
            door,
            url,
            client: reqwest::Client::new(),
        }
    }

    /// Exactly what `fqClient.do` sends: method, path, JSON body when there is
    /// one, and the identity's token in `x-api-key`.
    async fn call(&self, method: reqwest::Method, path: &str, body: Option<&str>) -> Answer {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.url))
            .header("x-api-key", TOKEN);

        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body.to_string());
        }

        let response = request.send().await.expect("the front door answered");

        Answer {
            status: response.status().as_u16(),
            retry_after: response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            body: response.text().await.unwrap_or_default(),
        }
    }

    async fn get(&self, path: &str) -> Answer {
        self.call(reqwest::Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: &str) -> Answer {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }
}

#[derive(Debug)]
struct Answer {
    status: u16,
    retry_after: Option<String>,
    body: String,
}

/// A migration of shard 1's only cell, driven through its real state machine
/// to the phase where the source is fenced and writes are refused everywhere.
fn fenced_cutover() -> Migration {
    let shard = Shard::new(1, "app");

    let plan = MigrationPlan::new(
        MigrationId(1),
        &shard,
        DbmsId::new("db-a"),
        DbmsId::new("db-b"),
        MigrationReason::Rebalance,
        0,
    )
    .unwrap();

    let mut migration = Migration::new(plan);

    migration.begin_copy(1).unwrap();
    migration.record_copy(shard.atom_count(), 4096, 2).unwrap();
    migration.begin_catch_up(3).unwrap();
    migration.begin_cutover(4).unwrap();

    assert!(migration.is_write_fenced(), "the fence is the point");

    migration
}

// ── what fqStore does every day ─────────────────────────────────────────

/// The whole point, in one test: the requests `fqStore` actually emits reach
/// the instance that holds their data, carrying the caller's own credential,
/// with their bytes untouched — and the answers come back untouched too.
#[tokio::test]
async fn a_facetql_client_cannot_tell_the_front_door_from_a_facetql() {
    let fleet = Fleet::spawn().await;

    // 1. `fqClient.upsert` — POST /node.
    let node = r#"{"address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{\"title\":\"hi\"}","public":false}"#;
    fleet.a.answers(200, r#"{"address":"Post:1","edges_created":[]}"#);

    let answer = fleet.post("/node", node).await;
    assert_eq!(answer.status, 200);
    assert_eq!(answer.body, r#"{"address":"Post:1","edges_created":[]}"#);

    let seen = fleet.a.seen();
    assert_eq!(seen.len(), 1, "the write went to db-a exactly once");
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].target, "/node");
    assert_eq!(seen[0].body, node, "the body was forwarded byte for byte");
    assert_eq!(
        seen[0].api_key.as_deref(),
        Some(TOKEN),
        "the caller's own token reached the engine; the door invented nothing"
    );
    assert!(fleet.b.saw_nothing(), "db-b holds no Posts");

    // 2. `fqClient.getNode` — GET /node/:address, percent-escaped by Go's
    //    url.PathEscape exactly as it is here.
    fleet.a.forget();
    fleet
        .a
        .answers(200, r#"{"address":"Post:1","kind":"Post","data":"{}"}"#);

    let answer = fleet.get("/node/Post%3A1").await;
    assert_eq!(answer.status, 200);
    assert_eq!(
        fleet.a.seen()[0].target,
        "/node/Post%3A1",
        "the escaped address was forwarded exactly as it arrived"
    );

    // 3. `fqClient.query` — POST /nodes/query on the *other* kind, which lives
    //    on the other instance. Same door, different engine.
    fleet.a.forget();
    fleet.b.answers(200, r#"{"nodes":[],"next":""}"#);

    let query = r#"{"kind":"User","item_var":"item","order":"","desc":false,"limit":500}"#;
    let answer = fleet.post("/nodes/query", query).await;

    assert_eq!(answer.status, 200);
    assert_eq!(answer.body, r#"{"nodes":[],"next":""}"#);
    assert_eq!(fleet.b.seen()[0].body, query);
    assert!(fleet.a.saw_nothing(), "a User query never touched db-a");

    // 4. `fqClient.transaction` — a batch entirely on one kind, forwarded
    //    whole, and its 412 relayed as a 412 so `errFQPrecondition` still
    //    fires on the client.
    fleet.b.forget();
    fleet.a.answers(412, "precondition failed");

    let batch = r#"{"operations":[{"type":"set_if","address":"Post:1","field":"next_run","expect_le":1000,"set":{"next_run":2000}},{"type":"delete_node","address":"Post:2"}]}"#;
    let answer = fleet.post("/transaction", batch).await;

    assert_eq!(answer.status, 412, "a lost compare-and-set is still a 412");
    assert_eq!(answer.body, "precondition failed");
    assert_eq!(fleet.a.seen()[0].body, batch);
}

// ── the refusals ────────────────────────────────────────────────────────

/// The single worst outcome available to this design would be a transaction
/// split across two engines: half applied, reported as success, with
/// `POST /transaction`'s all-or-nothing guarantee silently gone. It is refused
/// instead — and *nothing is forwarded*, which is the half of the claim that
/// is easy to get wrong.
#[tokio::test]
async fn a_transaction_spanning_two_backends_is_refused_and_neither_engine_is_touched() {
    let fleet = Fleet::spawn().await;

    let batch = r#"{"operations":[
        {"type":"insert_node","address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false},
        {"type":"delete_node","address":"User:9"}
    ]}"#;

    let answer = fleet.post("/transaction", batch).await;

    assert_eq!(answer.status, 421, "misdirected: no server here can do this");
    assert!(
        answer.body.contains("nothing was forwarded"),
        "the refusal has to say so: {}",
        answer.body
    );
    assert!(answer.body.contains("operations[0]"), "{}", answer.body);
    assert!(answer.body.contains("operations[1]"), "{}", answer.body);

    assert!(fleet.a.saw_nothing(), "db-a must not have half the batch");
    assert!(fleet.b.saw_nothing(), "db-b must not have half the batch");
}

/// A read whose answer would be assembled from two engines is refused whole,
/// for the same reason `RoutingTable::resolve_range` fails whole: an address
/// missing because a backend failed is indistinguishable, in FacetQL's
/// multiget contract, from an address that does not exist.
#[tokio::test]
async fn a_multiget_spanning_two_backends_is_refused_rather_than_answered_in_part() {
    let fleet = Fleet::spawn().await;

    let answer = fleet
        .post(
            "/nodes/multiget",
            r#"{"addresses":["Post:1","User:2"]}"#,
        )
        .await;

    assert_eq!(answer.status, 421);
    assert!(fleet.a.saw_nothing());
    assert!(fleet.b.saw_nothing());
}

/// A cutover fence is a bounded pause, not a failure, and the front door
/// absorbs it: the write waits, the migration finishes, and the write lands.
/// Nothing was forwarded while the fence was up, so waiting cannot double-apply
/// anything.
#[tokio::test]
async fn a_write_fenced_by_a_cutover_waits_for_it_and_then_lands() {
    let fleet = Fleet::spawn().await;

    let mut fenced = routing();
    assert!(fenced.observe_migration(&fenced_cutover(), Some(Coordinate::new(0, 0))));
    fleet.door.publish_routing(fenced);

    // The cutover completes 80ms in, well inside the 400ms budget.
    let door = fleet.door.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        door.publish_routing(routing());
    });

    fleet.a.answers(200, r#"{"address":"Post:1","edges_created":[]}"#);

    let answer = fleet
        .post(
            "/node",
            r#"{"address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false}"#,
        )
        .await;

    assert_eq!(answer.status, 200, "the fence was absorbed, not surfaced");
    assert_eq!(
        fleet.a.seen().len(),
        1,
        "waiting on a fence sends the write exactly once"
    );
}

/// A fence that outlasts the budget stops being a routine cutover and becomes
/// something the client is told to retry — with `Retry-After`, because
/// retrying really is the right next move. Reads are unaffected throughout:
/// only the write path is fenced.
#[tokio::test]
async fn a_fence_that_outlasts_the_budget_becomes_a_retryable_503_and_reads_keep_working() {
    let fleet = Fleet::spawn().await;

    let mut fenced = routing();
    assert!(fenced.observe_migration(&fenced_cutover(), Some(Coordinate::new(0, 0))));
    fleet.door.publish_routing(fenced);

    let answer = fleet
        .post(
            "/node",
            r#"{"address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false}"#,
        )
        .await;

    assert_eq!(answer.status, 503, "not a 500: nothing failed");
    assert_eq!(answer.retry_after.as_deref(), Some("1"));
    assert!(answer.body.contains("fenced"), "{}", answer.body);
    assert!(
        fleet.a.saw_nothing(),
        "a fenced write must never reach an engine"
    );

    // The same cell, read: still served by the migration source.
    fleet.a.answers(200, r#"{"address":"Post:1"}"#);
    let answer = fleet.get("/node/Post:1").await;

    assert_eq!(answer.status, 200, "a cutover fences writes, not reads");
    assert_eq!(fleet.a.seen().len(), 1);
}

/// A replica set with no in-sync primary owes a failover decision. It is not a
/// server fault and it is not a lost shard, so it is neither a 500 nor a 404.
#[tokio::test]
async fn no_writable_replica_is_a_retryable_503_not_a_500() {
    let fleet = Fleet::spawn().await;

    let mut broken = routing();
    assert!(broken.set_replica_set(
        ReplicaSet::new(1, ReplicationFactor::new(3).unwrap()),
        None
    ));
    fleet.door.publish_routing(broken);

    let answer = fleet
        .post(
            "/node",
            r#"{"address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false}"#,
        )
        .await;

    assert_eq!(answer.status, 503);
    assert_eq!(answer.retry_after.as_deref(), Some("1"));
    assert!(
        answer.body.contains("no in-sync primary"),
        "{}",
        answer.body
    );

    // And the read half of the same condition is its own status, also a 503.
    let answer = fleet.get("/node/Post:1").await;
    assert_eq!(answer.status, 503);
    assert!(answer.body.contains("fresh enough"), "{}", answer.body);

    assert!(fleet.a.saw_nothing());
}

/// The front door holds no identity. A request with no credential is turned
/// away in FacetQL's own words, before any route is resolved, so it cannot be
/// used to reach an engine as nobody.
#[tokio::test]
async fn a_request_without_a_credential_is_refused_in_facetqls_own_words() {
    let fleet = Fleet::spawn().await;

    let response = fleet
        .client
        .get(format!("{}/node/Post:1", fleet.url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 401);
    assert_eq!(
        response.text().await.unwrap(),
        "missing x-api-key header (or ?key= for SSE)"
    );
    assert!(fleet.a.saw_nothing());

    // `GET /` is FacetQL's one anonymous route and stays anonymous here.
    let response = fleet
        .client
        .get(format!("{}/", fleet.url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 200);
}

/// `fqClient.ping` asks "is the database up". Behind the front door the
/// database is a fleet, so the probe answers for the fleet: every instance, or
/// a failure naming the one that is not there.
#[tokio::test]
async fn liveness_answers_for_the_whole_fleet() {
    let fleet = Fleet::spawn().await;

    let answer = fleet.get("/").await;
    assert_eq!(answer.status, 200);
    assert_eq!(answer.body, "FacetQL Online");
    assert_eq!(fleet.a.seen().len(), 1);
    assert_eq!(fleet.b.seen().len(), 1);

    fleet.b.shut_down().await;

    let answer = fleet.get("/").await;
    assert_eq!(
        answer.status, 502,
        "a fleet with an instance down is not 'online'"
    );
    assert!(answer.body.contains("db-b"), "{}", answer.body);
}

/// Declarations are per-instance schema that every instance must carry, so
/// they go to all of them — which is what keeps `fqStore.Migrate`'s boot-time
/// reconcile meaningful behind the door. A listing that disagrees across the
/// fleet is reported as the divergence it is, rather than answered from
/// whichever instance replied first.
#[tokio::test]
async fn declarations_reach_every_instance_and_a_divergent_listing_is_reported() {
    let fleet = Fleet::spawn().await;

    let index = r#"{"name":"post_by_author","kind":"Post","field":"author"}"#;
    fleet.a.answers(201, "");
    fleet.b.answers(201, "");

    let answer = fleet.post("/admin/indexes", index).await;
    assert_eq!(answer.status, 201);
    assert_eq!(fleet.a.seen()[0].body, index);
    assert_eq!(fleet.b.seen()[0].body, index, "db-b must not be skipped");

    // Same content, different order: not a divergence.
    fleet
        .a
        .answers(200, r#"[{"name":"x"},{"name":"post_by_author"}]"#);
    fleet
        .b
        .answers(200, r#"[{"name":"post_by_author"},{"name":"x"}]"#);
    assert_eq!(fleet.get("/admin/indexes").await.status, 200);

    // An instance actually missing one: a real fact, reported.
    fleet.b.answers(200, r#"[{"name":"x"}]"#);
    let answer = fleet.get("/admin/indexes").await;

    assert_eq!(answer.status, 409);
    assert!(answer.body.contains("does not agree"), "{}", answer.body);
}

/// The two namespace-wide questions that have no true fleet answer. Both name
/// what is wrong rather than inventing a number or picking an instance.
#[tokio::test]
async fn requests_with_no_true_fleet_answer_say_so_instead_of_faking_one() {
    let fleet = Fleet::spawn().await;

    let answer = fleet.get("/stats").await;
    assert_eq!(answer.status, 421);
    assert!(answer.body.contains("one engine"), "{}", answer.body);

    let answer = fleet.post("/admin/users", r#"{"owner":"alice"}"#).await;
    assert_eq!(answer.status, 421);
    assert!(answer.body.contains("mints a secret"), "{}", answer.body);

    // Unknown paths are 404, exactly as FacetQL answers them.
    assert_eq!(fleet.get("/not-a-facetql-route").await.status, 404);

    assert!(fleet.a.saw_nothing());
    assert!(fleet.b.saw_nothing());
}

/// In front of one FacetQL — the deployment everybody starts from — the front
/// door refuses nothing. That is what makes it adoptable before anything has
/// been split, and it is the honest test of "transparent".
#[tokio::test]
async fn in_front_of_a_single_facetql_the_door_is_fully_transparent() {
    let only = Fake::spawn().await;

    let mut registry = TopologyRegistry::new();
    registry.place(
        DbmsId::new("db-a"),
        &Shard::new(1, "app"),
        Coordinate::new(0, 0),
        "eu",
    );

    let mut table = RoutingTable::new();
    table.apply_placements(&registry);

    let door = FrontDoor::new(
        Keyspace::single(key(1)),
        vec![Backend::new(DbmsId::new("db-a"), &only.base_url).unwrap()],
        table,
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { door.serve(listener).await });

    let client = reqwest::Client::new();

    for (method, path, body) in [
        (reqwest::Method::GET, "/stats", None),
        (reqwest::Method::GET, "/nodes", None),
        (
            reqwest::Method::POST,
            "/nodes/query",
            Some(r#"{"limit":10}"#),
        ),
        (
            reqwest::Method::POST,
            "/admin/users",
            Some(r#"{"owner":"alice"}"#),
        ),
        (
            reqwest::Method::POST,
            "/transaction",
            Some(
                r#"{"operations":[{"type":"delete_node","address":"Anything:1"},{"type":"clear_kind","kind":"Whatever"}]}"#,
            ),
        ),
    ] {
        only.answers(200, "{}");

        let mut request = client
            .request(method.clone(), format!("{url}{path}"))
            .header("x-api-key", TOKEN);

        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body);
        }

        let response = request.send().await.unwrap();

        assert_eq!(
            response.status().as_u16(),
            200,
            "{method} {path} was refused in front of a single FacetQL"
        );
    }
}

/// `GET /events` is a fan-*in*. FacetQL's event bus is per instance, so the
/// only faithful presentation of "the fleet's events" is every instance's
/// stream merged — and a merged stream that quietly loses one instance would
/// be a partial stream that looks complete, which is why an upstream that ends
/// ends the client's stream too.
#[tokio::test]
async fn a_subscription_merges_every_instances_stream_and_never_goes_quietly_partial() {
    use futures_util::StreamExt;

    let fleet = Fleet::spawn().await;

    fleet.a.emits(&["event: node\ndata: {\"from\":\"a\"}\n\n"]);
    fleet.b.emits(&["event: node\ndata: {\"from\":\"b\"}\n\n"]);

    let response = fleet
        .client
        .get(format!("{}/events", fleet.url))
        .header("x-api-key", TOKEN)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );

    let mut seen = String::new();
    let mut stream = response.bytes_stream();

    // Both instances stay subscribed, so the read ends when both frames have
    // arrived — not when one of them happens to close.
    while !(seen.contains(r#"{"from":"a"}"#) && seen.contains(r#"{"from":"b"}"#)) {
        let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("the merged stream produced something in time")
            .expect("the merged stream did not end")
            .expect("the merged stream did not fail");

        seen.push_str(&String::from_utf8_lossy(&chunk));
    }

    assert!(
        seen.contains("event: node"),
        "whole SSE frames, not spliced ones: {seen}"
    );

    assert_eq!(
        fleet.a.seen()[0].target,
        "/events",
        "both instances were subscribed to"
    );
    assert_eq!(fleet.b.seen()[0].target, "/events");
}

/// One instance refusing the subscription fails the whole subscription. A
/// stream missing one instance's events for its whole (possibly days-long)
/// life is far worse than a failure the client reconnects from.
#[tokio::test]
async fn a_subscription_one_instance_refuses_is_not_served_in_part() {
    let fleet = Fleet::spawn().await;

    fleet.a.emits(&["event: node\ndata: {}\n\n"]);
    fleet.b.answers(403, "not your events");

    let response = fleet
        .client
        .get(format!("{}/events", fleet.url))
        .header("x-api-key", TOKEN)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 502);

    let body = response.text().await.unwrap();
    assert!(body.contains("db-b"), "{body}");
    assert!(body.contains("403"), "{body}");
}
