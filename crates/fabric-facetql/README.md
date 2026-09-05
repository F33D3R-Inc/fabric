# fabric-facetql — Fabric's wire to FacetQL

Fabric is the distribution/control plane over a fleet of FacetQL instances: each
instance is one `fabric_core::DbmsNode`, placed and moved through
`fabric_topology::TopologyRegistry`. Until this crate existed there was **no wire
at all** — `fabric` appeared zero times in `fct`, `facets` or `facetql`, Fabric
had no FacetQL client, and the analysis pipeline was fed by replaying a JSON file
from disk (`fabric-cli/src/session.rs`).

This crate is that seam. It holds three things and nothing else.

## 1. The contract (`wire`, `client`)

FacetQL's canonical wire contract (AGENT_LOG §4b), transcribed exactly. Auth is
the header `x-api-key` on every request — never `?key=`, which is an SSE-only
fallback and would put a credential in access logs.

| Call | Method | Notes |
|------|--------|-------|
| `stats()` | `GET /stats` | Admin-gated. Monotonic `reads_total`/`writes_total`, per-kind counts, physical `storage` block. |
| `list_nodes()` | `GET /nodes?kind=&owner=&limit=&offset=` | Returns a bare JSON array. The offset API — FacetQL caps a deep offset at 10,000. |
| `query()` / `query_all()` | `POST /nodes/query` | Request field `after`, response `{nodes, next}`. `query_all` follows the opaque keyset cursor to the end. **Never** an offset walk. |
| `create_node()` | `POST /node` | With `if_absent: true` this is create-once: 409 means somebody created it first. |
| `transaction()` | `POST /transaction` | Serde-tagged ops: `insert_node`, `delete_node`, `insert_edge`, `delete_edge`, `clear_kind`, `delete_where`, `set_if`. All-or-nothing. |
| `claim()` | `POST /node/:address/claim` | The atomic claim primitive. `Ok(false)` = somebody else holds it. |

`wire.rs`'s unit tests assert the **exact JSON** of every transaction op. A field
rename on either side fails there rather than in production: inventing a field
name is contract drift, and contract drift is rework.

## 2. Telemetry (`sample`, `poller`)

FacetQL exposes counters, not rates — only the engine can count its own
operations, and a counter is the honest primitive. The rate is derived here by
differencing two `GET /stats` samples over the elapsed time between them, into
the existing `fabric_telemetry::WorkloadMetrics`.

The poller then emits ordinary `FabricMessage` values into
`FabricRuntime::handle`, so live telemetry comes through the same door the
replayed session does: **one ingestion path, one scoring path**
(runtime → `WorkloadAnalyzer` → `WorkloadProfile` → `WorkloadOptimizer` →
`fabric_ml::WorkloadPredictor`, the same numbers `fabric predict` prints).

Three deliberate refusals:

- **Nothing FacetQL does not measure is invented.** CPU, memory, queue depth and
  latency stay at zero because `/stats` reports none of them. Fabric's model
  treats absent resource pressure as low pressure, which is truthful.
- **No reporting across a restart.** The counters are process-lifetime; a counter
  that went backwards means the interval spans a restart, so the interval is
  dropped rather than reported as a fabricated spike.
- **Elapsed time comes from `Instant`,** not the wall clock, so an NTP step
  cannot manufacture a rate.

Attribution is per instance: until FacetQL owns a native shard/cell concept, the
placeable unit is the whole instance. Guessing which grid cell served which
operation is the adapter-side invention the integration plan forbids.

## 3. Durable control state (`placement`)

`TopologyRegistry` is an in-memory map; restart the control plane and it is gone.
It is persisted **in FacetQL** under the reserved kind `__fabric_placement`, one
node per `(shard_id, coordinate)` at address
`__fabric_placement:<shard>:<x>:<y>` — not in `fabric-core`'s duplicate storage
engine, which must not grow (Persistence is FacetQL's domain, §29).

Every placement carries a `version`, and every update is one `set_if` op with
`expect_eq: <version>` — FacetQL's **native compare-and-set**, evaluated in the
engine under the write lock. Read-then-write cannot make this safe: between the
read and the write another controller's move lands and is silently overwritten,
and the topology then records a placement nobody made.

| Operation | How | Losing looks like |
|-----------|-----|-------------------|
| `create` | `POST /node` with `if_absent` | 409 `Conflict` |
| `update` | tx: `set_if{expect_eq: version}` | 412 `PreconditionFailed`, nothing applied |
| `remove` | tx: `set_if{expect_eq: version}` + `delete_node`, one batch | 412 `PreconditionFailed`, the node survives intact |
| `load` | `POST /nodes/query`, cursor-paged | — |

`remove` is a genuine CAS delete built from the primitives that exist: because a
transaction is all-or-nothing, the version check and the delete either both apply
or neither does. There is no window between them.

## Security

The control plane holds a credential for every database instance in the fleet, so
these are structural, not advisory:

- the token is only ever an `x-api-key` **header** — never a URL;
- `FacetqlEndpoint`'s `Debug` is hand-written to redact the token, and no error
  variant carries one (a derived `Debug` would print it into every log line that
  ever touched a struct holding an endpoint);
- an empty token or a non-`http(s)` base URL is refused at construction, not
  discovered one request at a time; and
- **failure is closed.** `FacetqlError::implies_unhealthy()` splits "you lost a
  race" (412/409/404 — the instance is fine) from "we have no working
  relationship with this instance" (unreachable, unauthenticated, 5xx,
  undecodable). The latter marks the node unhealthy and emits **no** telemetry.
  Zeros would read to the optimizer as a quiet, healthy node.

## Running the live tests

Unit tests prove the bytes match the contract; only a real server proves the
contract was read correctly. `tests/live.rs` is opt-in and skips (loudly) when
the environment is not set.

```sh
rm -rf /tmp/fab && mkdir -p /tmp/fab
ENOCHIAN_DATA_DIR=/tmp/fab ENOCHIAN_PORT=8892 ENOCHIAN_TOKENS="fabtok:fabric:admin" \
  ./facetql/target/release/facetql start >/tmp/fab/fq.log 2>&1 &

FABRIC_FACETQL_URL=http://127.0.0.1:8892 FABRIC_FACETQL_TOKEN=fabtok \
  cargo test -p fabric-facetql --test live -- --test-threads=1 --nocapture
```

The token must be admin because `GET /stats` is admin-gated.

## Interfaces wanted from FacetQL (§28 — documented, not implemented there)

- **`GET /stats` reports no server software version.** Fabric registers every
  instance as `"unknown"` rather than scraping the human-readable `GET /` banner,
  which would be string-matching drift. A `version` field on the existing
  response would be additive. Owner: **Persistence → FacetQL**.
- **`GET /stats` reports no latency, CPU, memory or queue depth,** so the
  pressure model runs on operation rates alone. Deliberate for v1 (better than
  fake zeros); noted so it is not rediscovered as a Fabric bug. Owner:
  **Persistence → FacetQL**.
- **No per-cell attribution.** Counters are per instance and FacetQL's 4-axis
  coordinate carries no grid geometry, so the placeable unit is the whole
  instance. Owner: **Persistence → FacetQL** (integration plan Finding A).
