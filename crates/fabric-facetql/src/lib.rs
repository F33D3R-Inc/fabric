//! Fabric's wire to FacetQL.
//!
//! Fabric is the distribution/control plane over a fleet of FacetQL
//! instances: each instance is one [`fabric_core::DbmsNode`], placed and
//! moved through `fabric_topology::TopologyRegistry`. Until this crate
//! existed there was **no wire at all** — `fabric` appeared zero times in
//! fct/facets/facetql, Fabric had no FacetQL client, and the analysis
//! pipeline was fed by replaying a JSON file from disk. This crate is that
//! seam, and it holds three things:
//!
//! * [`client`] / [`wire`] — the canonical FacetQL contract (AGENT_LOG §4b),
//!   transcribed exactly: `x-api-key` auth, `GET /stats`, `GET /nodes`,
//!   `POST /nodes/query` with an opaque keyset cursor, `POST /node`,
//!   `POST /transaction` with the tagged op set, and
//!   `POST /node/:address/claim`.
//! * [`frontdoor`] — the **transparent front door**: an HTTP server that
//!   speaks FacetQL's own wire protocol, resolves each request through
//!   `fabric-routing`, and forwards it to the FacetQL instance that should
//!   serve it. This is the seam `FABRIC_INTEGRATION_PLAN.md` §3 names — point
//!   `FACET_DATABASE_URL` at it and `fqStore` cannot tell the difference —
//!   and it is what gives Fabric's control loop hands on the request path.
//! * [`sample`] / [`poller`] — the telemetry seam. FacetQL emits monotonic
//!   counters; Fabric differences two samples into `WorkloadMetrics` and
//!   feeds the *existing* runtime → analyzer → optimizer → predictor path.
//! * [`placement`] — Fabric's own control state, persisted **in FacetQL**
//!   under the reserved kind `__fabric_placement` with a native
//!   compare-and-set on a version field. Not in `fabric-core`'s duplicate
//!   storage engine, which must not grow: Persistence is FacetQL's domain
//!   (§29, FABRIC_INTEGRATION_PLAN Finding B).
//!
//! # Why this crate and not an existing one
//!
//! Every crate below `fabric-cli` is dependency-light and synchronous;
//! `fabric-topology` and `fabric-telemetry` in particular are depended on by
//! nearly everything. Putting an HTTP client in one of them would drag
//! reqwest, tokio and a TLS stack into every crate in the workspace. The
//! transport is an adapter concern, so it lives in an adapter crate that
//! depends on the domain crates rather than the other way round.
//!
//! # Security posture
//!
//! The control plane holds a credential for every database instance in the
//! fleet, so three rules are structural rather than advisory:
//!
//! * **the token is only ever an `x-api-key` header** — never a URL, where it
//!   would land in access logs; FacetQL's `?key=` is an SSE-only fallback and
//!   is not used here;
//! * **the token cannot be logged** — [`FacetqlEndpoint`]'s `Debug` is
//!   hand-written to redact it, and no error variant carries it; and
//! * **failure is closed** — an instance that is unreachable, unauthenticated
//!   or answering nonsense is reported as unhealthy and produces no
//!   telemetry. It is never treated as a quiet, healthy node.
//!
//! # Interfaces wanted from FacetQL (§28 — documented here, not implemented there)
//!
//! * `GET /stats` reports no **server software version**. Fabric registers
//!   every instance as version `"unknown"` rather than scraping the
//!   human-readable `GET /` banner, which would be string-matching drift. A
//!   `version` field on the existing `/stats` response would be additive and
//!   would close it. Owner: **Persistence → FacetQL**.
//! * `GET /stats` reports no **latency, CPU, memory or queue depth**, so
//!   Fabric's pressure model runs on operation rates alone. That is truthful
//!   and by design for v1 (the plan omits them deliberately rather than
//!   emitting fake zeros), and is noted here only so the gap is not
//!   rediscovered as a Fabric bug. Owner: **Persistence → FacetQL**.
//! * There is no **per-cell attribution**: FacetQL's counters are per
//!   instance, and its 4-axis coordinate carries no grid geometry. Until
//!   FacetQL owns a native shard/cell concept, the placeable unit is the
//!   whole instance and Fabric must not invent the mapping. Owner:
//!   **Persistence → FacetQL** (FABRIC_INTEGRATION_PLAN Finding A).

pub mod client;
pub mod endpoint;
pub mod error;
pub mod frontdoor;
pub mod mover;
pub mod placement;
pub mod poller;
pub mod sample;
pub mod wire;

pub use client::FacetqlClient;
pub use endpoint::FacetqlEndpoint;
pub use error::FacetqlError;
pub use frontdoor::{
    Backend, FrontDoor, FrontDoorConfig, Keyspace, KeyspaceRule,
};
pub use mover::{
    CellMover, CellScope, ChangeFeed, FeedHealth, MoverConfig, MoverError, MoverReport,
};
pub use placement::{address_of, PlacementStore, StoredPlacement, PLACEMENT_KIND};
pub use poller::{PollOutcome, PollTarget, TelemetryPoller};
pub use sample::StatsSample;
pub use wire::{
    CreateNodeRequest, EngineStats, Expect, KindCount, Node, QueryPage, QueryRequest,
    StorageStats, TransactionRequest, TxOperation, Visibility,
};
