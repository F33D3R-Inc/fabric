//! The transparent front door: Fabric speaking FacetQL's wire protocol.
//!
//! Fabric had a working control loop — routing, replication, migration, a
//! validating controller — and no hands on the request path. `fqStore` spoke
//! HTTP to one FacetQL and nothing routed real traffic through any of it. This
//! module is the seam that closes that, and its shape is the one
//! `FABRIC_INTEGRATION_PLAN.md` §3 names:
//!
//! > The root-correct way to introduce Fabric routing later is the
//! > **transparent front door**: when Fabric does routing/replication, it
//! > presents a **FacetQL-wire-compatible** endpoint, so `fqStore` never knows.
//!
//! So `FACET_DATABASE_URL` points here instead of at one FacetQL, and nothing
//! above changes: not fct, not the §4b contract, not FacetQL. Every request
//! this server accepts is a request `facetql/src/api/routes.rs` accepts, and
//! every byte it returns is a byte a FacetQL returned.
//!
//! # The three layers
//!
//! * [`keyspace`] — the operator's declared map from FacetQL's namespace
//!   (kinds and address prefixes) to Fabric's routing keys. A lookup, never a
//!   hash: `fabric_routing` exists precisely so that moving data does not
//!   change its name.
//! * [`plan`] — a pure function from a request to what the front door will do
//!   with it. Every route in FacetQL's router is classified there, and the
//!   refusals are argued there.
//! * [`proxy`] — the bytes. Fidelity only; it decides nothing.
//!
//! This module joins them: it resolves a plan's keys through the live
//! [`RoutingTable`], turns every way that can fail into an HTTP status a
//! client can act on, and forwards.
//!
//! # Authentication: the front door has no identity
//!
//! FacetQL authenticates a per-identity token in the `x-api-key` header (and,
//! for browser `EventSource` only, a `?key=` query parameter). The front door
//! holds **no token of its own on the data path**. It does exactly two things
//! with a credential:
//!
//! 1. refuses a request that carries none, in FacetQL's own words, so that no
//!    route is ever resolved for an unauthenticated caller; and
//! 2. forwards whatever arrived, unread and unmodified, to the backend.
//!
//! It cannot validate a token — it has no user store — and it must not try:
//! every authorization decision (`can_read`, `can_write`, the admin role, the
//! per-owner event audience) stays inside the engine that owns the data. The
//! consequence is the important one: **the front door cannot be used to act as
//! an identity FacetQL did not authenticate**, because it never holds one.
//!
//! Note that the control plane's *own* credentials live elsewhere entirely —
//! [`crate::FacetqlEndpoint`] holds one per instance for telemetry and the
//! placement store — and none of them is reachable from this path.
//!
//! # What a client sees when routing cannot answer
//!
//! | condition | status | why |
//! |---|---|---|
//! | `WriteFenced` (cutover in flight) | retried in place; `503` + `Retry-After` if the window outlasts the budget | see [`FrontDoorConfig::fence_retry_budget`] |
//! | `NoWritableReplica` | `503` + `Retry-After: 1` | a failover is owed; the shard is not lost |
//! | `NoLiveOwner` | `503` + `Retry-After: 1` | the shard is unreachable, not gone |
//! | `NoReadableReplica` | `503` + `Retry-After: 1` | no copy is fresh enough *yet* |
//! | `UnknownShard` / `InvalidCoordinate` | `421` | the keyspace names a place routing has never heard of: configuration, and no amount of retrying fixes it |
//! | `InvalidAddress` / `InvalidRange` | `400` | the request named something unroutable |
//! | `UnknownNode` | `500` | routing and the node inventory disagree — a control-plane fault, and the only 500 this door produces |
//! | a resolved node with no registered URL | `503` | the fleet knows who serves it and not how to reach it; registering the backend fixes it without a restart |
//!
//! None of these is a 500 except the one that genuinely is one. A `503`
//! carries `Retry-After` exactly when retrying is the right next move.

pub mod keyspace;
pub mod plan;
pub mod proxy;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;

use fabric_core::DbmsId;
use fabric_routing::{ReadPreference, RouteRequest, RoutingError, RoutingTable};

use crate::error::FacetqlError;

pub use keyspace::{Keyspace, KeyspaceError, KeyspaceMiss, KeyspaceRule};
pub use plan::{Agreement, KeyIntent, Plan, Refusal};
pub use proxy::{Unreachable, Upstream};

/// One FacetQL instance, as the front door reaches it.
///
/// Deliberately *not* a [`crate::FacetqlEndpoint`]: that type carries the
/// control plane's own credential, and the data path must not have one. A
/// backend here is an identity and an address, and the token that opens it is
/// whatever the client sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backend {
    id: DbmsId,
    base_url: String,
}

impl Backend {
    pub fn new(id: DbmsId, base_url: impl Into<String>) -> Result<Self, FacetqlError> {
        let base_url = base_url.into();
        let base_url = base_url.trim().trim_end_matches('/').to_string();

        if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
            return Err(FacetqlError::Configuration(format!(
                "FacetQL backend '{}' has a base URL that is not http(s): {base_url}",
                id.0
            )));
        }

        Ok(Self { id, base_url })
    }

    pub fn id(&self) -> &DbmsId {
        &self.id
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

/// The knobs, with the reasoning for each default.
#[derive(Debug, Clone)]
pub struct FrontDoorConfig {
    /// Which copy a read may be served from.
    ///
    /// [`ReadPreference::Primary`] by default, and that default is not
    /// timidity: `fqStore` reads its own writes constantly (it writes a
    /// session and reads it back, it inserts a row and queries the kind), and
    /// a front door that silently served those from a lagging replica would
    /// turn a correct application into an intermittently wrong one. An
    /// operator who knows a workload tolerates staleness opts into it here.
    pub read_preference: ReadPreference,

    /// How long a write may wait for a migration cutover fence to lift before
    /// the client is told to retry instead.
    ///
    /// The front door retries a fenced write **in place**, and it is allowed
    /// to because of where the fence happens: `RoutingError::WriteFenced` is
    /// produced while *resolving* a route, before any byte has been forwarded.
    /// A retry is therefore a first send, not a re-send — it cannot apply a
    /// non-idempotent write twice, which is the usual reason a proxy must not
    /// retry writes. A cutover fence is a bounded pause measured in
    /// milliseconds, so absorbing it here keeps ordinary application writes
    /// working through a routine, planned migration, which is the entire point
    /// of doing migrations this way.
    ///
    /// It is bounded rather than unbounded for the equally important converse:
    /// `fqStore`'s HTTP client gives up at 15 seconds, so a front door that
    /// waited longer would burn the client's whole budget and return nothing.
    /// Two seconds leaves the client eleven times that to retry with, and a
    /// fence that outlasts it is no longer a routine cutover — it is something
    /// an operator needs to see, which is what the `503` says.
    pub fence_retry_budget: Duration,

    /// How often the fence is re-checked. Each attempt re-reads the routing
    /// table, so a cutover that completes is noticed on the next tick.
    pub fence_retry_interval: Duration,

    /// How long to wait on a backend.
    ///
    /// Longer than FacetQL's own 30-second request deadline
    /// (`FACETQL_REQUEST_TIMEOUT_SECS`) on purpose: when a request is slow, the
    /// truthful answer is the backend's own `408`, and a front door that timed
    /// out first would replace it with a fabricated one.
    pub upstream_timeout: Duration,

    /// Largest request body accepted, matching FacetQL's own 4 MiB
    /// (`FACETQL_MAX_BODY_BYTES`) so the two agree on what is too large and a
    /// client gets the same `413` either way.
    pub max_body_bytes: usize,
}

impl Default for FrontDoorConfig {
    fn default() -> Self {
        Self {
            read_preference: ReadPreference::Primary,
            fence_retry_budget: Duration::from_secs(2),
            fence_retry_interval: Duration::from_millis(50),
            upstream_timeout: Duration::from_secs(35),
            max_body_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Everything the door needs to answer, swappable while it is serving.
#[derive(Debug)]
struct Fleet {
    keyspace: Keyspace,
    routing: RoutingTable,
    backends: BTreeMap<String, Backend>,
}

/// The server.
///
/// Cheap to clone: every clone shares one fleet snapshot and one connection
/// pool, so the control loop can hold one and the server another.
#[derive(Debug, Clone)]
pub struct FrontDoor {
    fleet: Arc<RwLock<Fleet>>,
    http: reqwest::Client,
    streaming: reqwest::Client,
    config: FrontDoorConfig,
}

impl FrontDoor {
    /// Build a front door over a keyspace, a set of backends and a routing
    /// table.
    ///
    /// The three are checked against each other here rather than per request:
    /// a keyspace naming a shard the routing table has never heard of, or a
    /// routing table naming a node with no registered URL, is a configuration
    /// fault, and a control plane that discovers it one failed request at a
    /// time is one that has already failed those requests.
    pub fn new(
        keyspace: Keyspace,
        backends: Vec<Backend>,
        routing: RoutingTable,
    ) -> Result<Self, FacetqlError> {
        Self::with_config(keyspace, backends, routing, FrontDoorConfig::default())
    }

    pub fn with_config(
        keyspace: Keyspace,
        backends: Vec<Backend>,
        routing: RoutingTable,
        config: FrontDoorConfig,
    ) -> Result<Self, FacetqlError> {
        if backends.is_empty() {
            return Err(FacetqlError::Configuration(
                "a front door with no backends can serve nothing".to_string(),
            ));
        }

        let http = reqwest::Client::builder()
            .timeout(config.upstream_timeout)
            .build()
            .map_err(|error| {
                FacetqlError::Configuration(format!("could not build the HTTP client: {error}"))
            })?;

        /*
         * A second client with no timeout, for `GET /events` alone. An SSE
         * stream that is severed every thirty-five seconds is not a guarded
         * stream, it is a broken one -- the same reason FacetQL keeps
         * `/events` outside its own request-timeout layer.
         */
        let streaming = reqwest::Client::builder().build().map_err(|error| {
            FacetqlError::Configuration(format!(
                "could not build the streaming HTTP client: {error}"
            ))
        })?;

        let backends = backends
            .into_iter()
            .map(|backend| (backend.id.0.clone(), backend))
            .collect();

        Ok(Self {
            fleet: Arc::new(RwLock::new(Fleet {
                keyspace,
                routing,
                backends,
            })),
            http,
            streaming,
            config,
        })
    }

    pub fn config(&self) -> &FrontDoorConfig {
        &self.config
    }

    /// Adopt a new routing table.
    ///
    /// This is how the control loop's decisions reach real traffic: the
    /// controller executes a migration or a failover, re-derives the routing
    /// table, and publishes it here. In-flight requests keep the answer they
    /// already resolved; the next one is routed by the new table.
    pub fn publish_routing(&self, routing: RoutingTable) {
        self.fleet
            .write()
            .expect("the front door's fleet lock is poisoned")
            .routing = routing;
    }

    /// Adopt a new keyspace — a kind moving to a different shard.
    pub fn publish_keyspace(&self, keyspace: Keyspace) {
        self.fleet
            .write()
            .expect("the front door's fleet lock is poisoned")
            .keyspace = keyspace;
    }

    /// Add or replace one backend's address, without a restart.
    pub fn publish_backend(&self, backend: Backend) {
        self.fleet
            .write()
            .expect("the front door's fleet lock is poisoned")
            .backends
            .insert(backend.id.0.clone(), backend);
    }

    /// The routing table's generation, as the door currently sees it.
    pub fn generation(&self) -> u64 {
        self.fleet
            .read()
            .expect("the front door's fleet lock is poisoned")
            .routing
            .generation()
    }

    /// The axum router. One fallback handler: FacetQL's surface is classified
    /// in [`plan`], not in a route table that could disagree with it.
    pub fn router(self) -> Router {
        Router::new()
            .fallback(any(dispatch))
            .with_state(Arc::new(self))
    }

    /// Serve until the process ends.
    pub async fn serve(self, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        axum::serve(listener, self.router()).await
    }
}

/// The one handler.
async fn dispatch(State(door): State<Arc<FrontDoor>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();

    // The raw request target, forwarded byte-for-byte: re-encoding a path that
    // a client percent-escaped is a way to address a different node.
    let target = parts
        .uri
        .path_and_query()
        .map(|target| target.as_str().to_string())
        .unwrap_or_else(|| parts.uri.path().to_string());

    let path = parts.uri.path().to_string();
    let query = parts.uri.query().map(str::to_string);

    let body = match axum::body::to_bytes(body, door.config.max_body_bytes).await {
        Ok(body) => body,

        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "request body exceeds the {} byte limit",
                    door.config.max_body_bytes
                ),
            )
                .into_response();
        }
    };

    /*
     * `GET /` is FacetQL's only unauthenticated route. Everything else is
     * refused here without a credential, so the front door never resolves a
     * route -- never spends a lookup, never reveals by its timing or its status
     * which shards exist -- for a caller FacetQL would have turned away.
     */
    if path != "/" && !carries_credential(&parts.headers, query.as_deref()) {
        return refuse(&Refusal::MissingCredential);
    }

    let plan = {
        let fleet = door
            .fleet
            .read()
            .expect("the front door's fleet lock is poisoned");

        plan::plan(
            &parts.method,
            &path,
            query.as_deref(),
            &body,
            &fleet.keyspace,
            &door.config.read_preference,
        )
    };

    match plan {
        Plan::Refuse(refusal) => refuse(&refusal),

        Plan::Colocated { keys } => {
            match resolve_colocated(&door, &keys).await {
                Ok(backend) => {
                    match proxy::forward(
                        &door.http,
                        &backend.id.0,
                        &backend.base_url,
                        &parts.method,
                        &target,
                        &parts.headers,
                        body,
                    )
                    .await
                    {
                        Ok(upstream) => upstream.into_response(),
                        Err(unreachable) => bad_gateway(&[unreachable]),
                    }
                }

                Err(error) => error.into_response(),
            }
        }

        Plan::Broadcast { agreement, what } => {
            broadcast(&door, agreement, what, &parts.method, &target, &parts.headers, body).await
        }

        Plan::Subscribe => subscribe(&door, &parts.method, &target, &parts.headers).await,
    }
}

/// Resolve every key a request touches to the one backend that serves them
/// all, absorbing a cutover fence within the configured budget.
async fn resolve_colocated(door: &FrontDoor, keys: &[KeyIntent]) -> Result<Backend, DoorError> {
    let deadline = std::time::Instant::now() + door.config.fence_retry_budget;

    loop {
        let attempt = {
            /*
             * The whole resolution happens under one read guard and contains no
             * await, so every key of a batch is answered by ONE snapshot of the
             * routing table. Resolving them across a table swap could conclude
             * that a batch is colocated when neither the old table nor the new
             * one says so.
             */
            let fleet = door
                .fleet
                .read()
                .expect("the front door's fleet lock is poisoned");

            resolve_once(&fleet, keys)
        };

        match attempt {
            Err(DoorError::Routing(RoutingError::WriteFenced { .. }))
                if std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(door.config.fence_retry_interval).await;
            }

            other => return other,
        }
    }
}

/// One pass over the keys against one snapshot of the fleet.
fn resolve_once(fleet: &Fleet, keys: &[KeyIntent]) -> Result<Backend, DoorError> {
    let mut chosen: Option<(String, &KeyIntent)> = None;
    let mut nodes: Vec<String> = Vec::new();

    for key in keys {
        let route = fleet
            .routing
            .resolve(&RouteRequest {
                key: key.key,
                intent: key.intent.clone(),
            })
            .map_err(DoorError::Routing)?;

        if !nodes.contains(&route.node.0) {
            nodes.push(route.node.0.clone());
        }

        match &chosen {
            None => chosen = Some((route.node.0.clone(), key)),

            Some((node, first)) if node != &route.node.0 => {
                /*
                 * The refusal every other invariant rests on. A batch, an
                 * edge, or a multiget that does not fit on one engine is
                 * refused having forwarded nothing -- splitting it would
                 * trade the guarantee it was made for (atomicity, or a whole
                 * answer) for the appearance of success.
                 */
                return Err(DoorError::Refuse(Refusal::SpansBackends {
                    what: format!("{} and {}", first.origin, key.origin),
                    nodes,
                }));
            }

            Some(_) => {}
        }
    }

    let (node, _) = chosen.expect("a colocated plan always carries at least one key");

    fleet
        .backends
        .get(&node)
        .cloned()
        .ok_or(DoorError::NoBackendUrl { node })
}

/// Send to every backend in the fleet.
///
/// The set is the *registered* backends, not the ones routing currently
/// resolves to. That is deliberate for the two things that broadcast: a
/// declaration (an index, a reference) is per-instance schema that every
/// instance must carry, and a liveness probe answers for the fleet. Excluding
/// an instance because routing thinks it is unavailable would let a
/// declaration report success while one instance quietly missed it, which is
/// exactly the drift `fqStore`'s boot-time reconcile exists to prevent.
async fn broadcast(
    door: &FrontDoor,
    agreement: Agreement,
    what: &'static str,
    method: &axum::http::Method,
    target: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let backends: Vec<Backend> = {
        let fleet = door
            .fleet
            .read()
            .expect("the front door's fleet lock is poisoned");

        fleet.backends.values().cloned().collect()
    };

    let mut answers = Vec::with_capacity(backends.len());
    let mut unreachable = Vec::new();

    for backend in &backends {
        match proxy::forward(
            &door.http,
            &backend.id.0,
            &backend.base_url,
            method,
            target,
            headers,
            body.clone(),
        )
        .await
        {
            Ok(upstream) => answers.push(upstream),
            Err(failure) => unreachable.push(failure),
        }
    }

    if !unreachable.is_empty() {
        return bad_gateway(&unreachable);
    }

    /*
     * A refusal from any single instance is the fleet's answer, relayed
     * verbatim: a 403 on `POST /admin/indexes` means this identity may not
     * declare indexes anywhere, and reporting it as the backend worded it is
     * what lets `fqStore.Migrate` tell that apart from a reconcile that broke.
     */
    if let Some(refused) = answers.iter().find(|answer| !answer.is_success()) {
        return refused.clone().into_response();
    }

    if agreement == Agreement::Required {
        if let Some(disagreement) = first_disagreement(&answers) {
            return (
                StatusCode::CONFLICT,
                format!(
                    "fabric front door: {what} does not agree across the fleet: \
                     '{}' and '{}' returned different answers. This is a real \
                     divergence between instances, not a tie to break — \
                     reconcile them rather than reading one of them as the \
                     fleet's answer.",
                    disagreement.0, disagreement.1
                ),
            )
                .into_response();
        }
    }

    answers
        .into_iter()
        .next()
        .expect("a front door always has at least one backend")
        .into_response()
}

fn first_disagreement(answers: &[Upstream]) -> Option<(String, String)> {
    let first = answers.first()?;

    answers
        .iter()
        .skip(1)
        .find(|answer| !proxy::answers_agree(first, answer))
        .map(|answer| (first.node.clone(), answer.node.clone()))
}

/// `GET /events`: every backend's stream, merged into one.
async fn subscribe(
    door: &FrontDoor,
    method: &axum::http::Method,
    target: &str,
    headers: &HeaderMap,
) -> Response {
    let backends: Vec<Backend> = {
        let fleet = door
            .fleet
            .read()
            .expect("the front door's fleet lock is poisoned");

        fleet.backends.values().cloned().collect()
    };

    let mut streams = Vec::with_capacity(backends.len());

    for backend in &backends {
        match proxy::open_stream(
            &door.streaming,
            &backend.id.0,
            &backend.base_url,
            method,
            target,
            headers,
        )
        .await
        {
            Ok(response) => streams.push((backend.id.0.clone(), response)),

            Err(failure) => {
                /*
                 * One backend refused or is down. Serving the rest would hand
                 * the client a stream that looks like the fleet's and silently
                 * omits one instance's events -- for a subscription, which may
                 * run for days, that is a far worse outcome than a failure the
                 * client reconnects from. Every stream opened so far is
                 * dropped with this response.
                 */
                return bad_gateway(&[failure]);
            }
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(proxy::merge_events(streams))
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "could not open the merged event stream")
                .into_response()
        })
}

/// Whether a credential is present at all. Its *validity* is FacetQL's to
/// judge; the front door has no user store and must not pretend to.
fn carries_credential(headers: &HeaderMap, query: Option<&str>) -> bool {
    if headers.contains_key("x-api-key") {
        return true;
    }

    // The `?key=` fallback exists for browser `EventSource`, which cannot set
    // headers. Recognised here for the same reason FacetQL recognises it.
    query.is_some_and(|query| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .any(|(name, value)| name == "key" && !value.is_empty())
    })
}

/// Everything that can stop a request between the plan and the wire.
#[derive(Debug)]
enum DoorError {
    Refuse(Refusal),
    Routing(RoutingError),

    /// Routing named a node the backend registry has no address for.
    NoBackendUrl { node: String },
}

impl DoorError {
    fn into_response(self) -> Response {
        match self {
            Self::Refuse(refusal) => refuse(&refusal),

            Self::Routing(error) => {
                let (status, retryable) = routing_status(&error);

                let mut response =
                    (status, format!("fabric front door: {error}")).into_response();

                if retryable {
                    response
                        .headers_mut()
                        .insert("retry-after", axum::http::HeaderValue::from_static("1"));
                }

                response
            }

            Self::NoBackendUrl { node } => {
                let mut response = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "fabric front door: node '{node}' serves this request but has \
                         no registered address; the fleet knows who should answer and \
                         not how to reach them"
                    ),
                )
                    .into_response();

                response
                    .headers_mut()
                    .insert("retry-after", axum::http::HeaderValue::from_static("1"));

                response
            }
        }
    }
}

/// The whole routing-failure mapping, in one place. See the module table.
fn routing_status(error: &RoutingError) -> (StatusCode, bool) {
    match error {
        /*
         * Reached only when a cutover outlasts the retry budget: the fence is
         * absorbed in `resolve_colocated` first. Still `Retry-After`, because
         * it is still a bounded window -- it has just stopped being a routine
         * one.
         */
        RoutingError::WriteFenced { .. }
        | RoutingError::NoWritableReplica { .. }
        | RoutingError::NoLiveOwner { .. }
        | RoutingError::NoReadableReplica { .. } => (StatusCode::SERVICE_UNAVAILABLE, true),

        // The keyspace points at a shard or a cell routing does not have.
        // Configuration, and retrying will not fix it.
        RoutingError::UnknownShard { .. } | RoutingError::InvalidCoordinate { .. } => {
            (StatusCode::MISDIRECTED_REQUEST, false)
        }

        RoutingError::InvalidAddress { .. } | RoutingError::InvalidRange { .. } => {
            (StatusCode::BAD_REQUEST, false)
        }

        // Routing holds a placement on a node its own inventory has never
        // seen. Nothing the client did, nothing a retry mends: Fabric
        // disagrees with itself, and a 500 is the truthful way to say so.
        RoutingError::UnknownNode { .. } => (StatusCode::INTERNAL_SERVER_ERROR, false),
    }
}

fn refuse(refusal: &Refusal) -> Response {
    (refusal.status(), refusal.to_string()).into_response()
}

fn bad_gateway(failures: &[Unreachable]) -> Response {
    let detail = failures
        .iter()
        .map(|failure| failure.message.clone())
        .collect::<Vec<String>>()
        .join("; ");

    let mut response = (
        StatusCode::BAD_GATEWAY,
        format!("fabric front door: {detail}"),
    )
        .into_response();

    response
        .headers_mut()
        .insert("retry-after", axum::http::HeaderValue::from_static("1"));

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabric_core::Coordinate;
    use fabric_routing::{NodeAvailability, RoutingKey};

    fn key(shard_id: u64) -> RoutingKey {
        RoutingKey::new(shard_id, Coordinate::new(0, 0)).unwrap()
    }

    #[test]
    fn every_routing_failure_has_a_status_a_client_can_act_on() {
        let cases = [
            (
                RoutingError::WriteFenced {
                    shard_id: 1,
                    source: DbmsId::new("a"),
                    destination: DbmsId::new("b"),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                true,
            ),
            (
                RoutingError::NoWritableReplica { shard_id: 1 },
                StatusCode::SERVICE_UNAVAILABLE,
                true,
            ),
            (
                RoutingError::NoLiveOwner {
                    shard_id: 1,
                    considered: vec![DbmsId::new("a")],
                },
                StatusCode::SERVICE_UNAVAILABLE,
                true,
            ),
            (
                RoutingError::NoReadableReplica { shard_id: 1 },
                StatusCode::SERVICE_UNAVAILABLE,
                true,
            ),
            (
                RoutingError::UnknownShard { shard_id: 1 },
                StatusCode::MISDIRECTED_REQUEST,
                false,
            ),
            (
                RoutingError::InvalidCoordinate { x: 99, y: 0 },
                StatusCode::MISDIRECTED_REQUEST,
                false,
            ),
            (
                RoutingError::InvalidAddress {
                    address: "x".into(),
                    reason: "nope".into(),
                },
                StatusCode::BAD_REQUEST,
                false,
            ),
            (
                RoutingError::UnknownNode {
                    node: DbmsId::new("ghost"),
                },
                StatusCode::INTERNAL_SERVER_ERROR,
                false,
            ),
        ];

        for (error, status, retryable) in cases {
            assert_eq!(routing_status(&error), (status, retryable), "{error}");
        }

        // Exactly one routing failure is ever a 500, and it is the one that
        // means Fabric disagrees with itself.
        let five_hundreds = cases_5xx();
        assert_eq!(five_hundreds, 1);
    }

    fn cases_5xx() -> usize {
        [
            RoutingError::WriteFenced {
                shard_id: 1,
                source: DbmsId::new("a"),
                destination: DbmsId::new("b"),
            },
            RoutingError::NoWritableReplica { shard_id: 1 },
            RoutingError::NoLiveOwner {
                shard_id: 1,
                considered: vec![],
            },
            RoutingError::NoReadableReplica { shard_id: 1 },
            RoutingError::UnknownShard { shard_id: 1 },
            RoutingError::InvalidCoordinate { x: 0, y: 99 },
            RoutingError::InvalidAddress {
                address: "x".into(),
                reason: "r".into(),
            },
            RoutingError::InvalidRange {
                start: Coordinate::new(1, 1),
                end: Coordinate::new(0, 0),
            },
            RoutingError::UnknownNode {
                node: DbmsId::new("ghost"),
            },
        ]
        .iter()
        .filter(|error| routing_status(error).0 == StatusCode::INTERNAL_SERVER_ERROR)
        .count()
    }

    #[test]
    fn a_credential_is_recognised_in_the_header_and_in_the_sse_fallback() {
        let mut headers = HeaderMap::new();
        assert!(!carries_credential(&headers, None));
        assert!(!carries_credential(&headers, Some("key=")));
        assert!(carries_credential(&headers, Some("kind=Post&key=abc")));

        headers.insert("x-api-key", axum::http::HeaderValue::from_static("t"));
        assert!(carries_credential(&headers, None));
    }

    #[test]
    fn a_backend_url_must_be_http() {
        let error = Backend::new(DbmsId::new("a"), "127.0.0.1:8892").unwrap_err();
        assert!(error.to_string().contains("not http(s)"));

        let backend = Backend::new(DbmsId::new("a"), "http://127.0.0.1:8892/").unwrap();
        assert_eq!(backend.base_url(), "http://127.0.0.1:8892");
    }

    #[test]
    fn a_front_door_with_no_backends_is_refused() {
        let error =
            FrontDoor::new(Keyspace::single(key(1)), Vec::new(), RoutingTable::new())
                .unwrap_err();

        assert!(error.to_string().contains("no backends"));
    }

    /// Two keys that resolve to two nodes are refused, and the refusal names
    /// both origins so whoever has to fix it knows which two ops collided.
    #[test]
    fn a_request_spanning_two_backends_is_refused_naming_both_halves() {
        let mut routing = RoutingTable::new();
        routing.register_node(DbmsId::new("db-a"), "eu");
        routing.register_node(DbmsId::new("db-b"), "us");
        routing.set_availability(&DbmsId::new("db-a"), NodeAvailability::Serviceable);
        routing.set_availability(&DbmsId::new("db-b"), NodeAvailability::Serviceable);

        let mut registry = fabric_topology::TopologyRegistry::new();
        registry.place(
            DbmsId::new("db-a"),
            &fabric_core::Shard::new(1, "app"),
            Coordinate::new(0, 0),
            "eu",
        );
        registry.place(
            DbmsId::new("db-b"),
            &fabric_core::Shard::new(2, "app"),
            Coordinate::new(0, 0),
            "us",
        );
        routing.apply_placements(&registry);

        let fleet = Fleet {
            keyspace: Keyspace::single(key(1)),
            routing,
            backends: BTreeMap::from([
                (
                    "db-a".to_string(),
                    Backend::new(DbmsId::new("db-a"), "http://a").unwrap(),
                ),
                (
                    "db-b".to_string(),
                    Backend::new(DbmsId::new("db-b"), "http://b").unwrap(),
                ),
            ]),
        };

        let keys = vec![
            KeyIntent {
                key: key(1),
                intent: fabric_routing::RouteIntent::Write,
                origin: "operations[0] 'Post:1'".to_string(),
            },
            KeyIntent {
                key: key(2),
                intent: fabric_routing::RouteIntent::Write,
                origin: "operations[1] 'User:2'".to_string(),
            },
        ];

        match resolve_once(&fleet, &keys) {
            Err(DoorError::Refuse(refusal)) => {
                assert_eq!(refusal.status(), StatusCode::MISDIRECTED_REQUEST);
                let message = refusal.to_string();
                assert!(message.contains("operations[0] 'Post:1'"), "{message}");
                assert!(message.contains("operations[1] 'User:2'"), "{message}");
                assert!(message.contains("nothing was forwarded"), "{message}");
            }

            other => panic!("expected a refusal, got {other:?}"),
        }

        // One key alone resolves cleanly.
        match resolve_once(&fleet, &keys[..1]) {
            Ok(backend) => assert_eq!(backend.id().0, "db-a"),
            other => panic!("{other:?}"),
        }
    }
}
