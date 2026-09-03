# Fabric Integration Plan — the correct seams, and the one primitive to build first

Status: **design / research deliverable** (no code changed by this doc).
Author: planning agent, 2026-09-03. Owner domain: **Distribution → Fabric** (§29).
Priority context: Fabric is §33 #12 — **lowest priority, Phase‑6+, NOT a launch
blocker.** This plan exists so that the *design* is correct and ready, so that
"once FacetQL is operational, we can connect everything" is a small, honest step
and not a rebuild. Nothing here proposes touching higher‑priority work.

Read alongside: `AGENT_LOG.md` (§4b canonical wire contract, §29 owners, §33
priority), `NOTES.md` (EPIC 08 observability, EPIC 15–21 Fabric), `fabric/README.md`.

Hard rule honored throughout: **root solutions only, never patch, no rework.**
Every recommendation places the primitive in its owning layer and never proposes
an adapter‑side hack where a real primitive belongs.

---

## 0. Evidence base (what was actually read)

- **fabric** — every `crates/*/src/*.rs` (2,771 LOC total). Key files cited inline.
- **facetql** — `src/api/routes.rs` (full), `src/storage/engine.rs` (surface),
  `src/core/coordinate.rs`, CLI stats path.
- **fct** — `runtime/store.go` (the `Store` seam), `runtime/fqclient.go`,
  `runtime/fqstore.go` surface.
- **Cross‑repo grep**: `fabric` appears **zero** times in `fct`, `facets`, or
  `facetql/src`. `facetql`/`reqwest`/`8080`/`/node` appear **zero** times in
  `fabric/crates` (the only `http` hit is axum in fabric's own protocol server).
  **There is no wire between fabric and any other repo today.** That is the
  correct starting point: we are designing the seam, not repairing one.

Two findings shape everything below and are called out up front:

> **FINDING A — the 12×13/156 grid currently lives in the wrong repo, and two
> different `Coordinate` types share one name.**
> `fabric-core::Coordinate { x: u8, y: u8 }` is a cell in a fixed **12×13 = 156**
> grid (`coordinate.rs`: `is_valid` = `x<12 && y<13`; `grid.rs`: `GRID_ATOMS=156`).
> `facetql::core::coordinate::Coordinate { x, y, z, q }` is a **4‑axis** per‑node
> tag with **no grid geometry** enforced in the engine. They are *not* the same
> concept today and must never be silently marshalled into each other.
> Per the authoritative spec (§5, §15), the **12×13/156 grid is FacetQL's native
> addressing/layout foundation** — it belongs to the Persistence owner (FacetQL),
> as an "internal addressing/layout/organization mechanism," *not* to Fabric. The
> spec is explicit that the grid is not a flat 156‑record model and must not be
> casually replaced by a relational one. **Root‑solution implication:** the grid
> addressing model must eventually become **native to FacetQL** (a FacetQL‑owned
> primitive), and Fabric's `Placement` must operate over *that* FacetQL‑defined
> grid rather than fabric‑core's private copy. Fabric consumes the addressing
> model; it does not define it. Until FacetQL grows a native grid/shard concept,
> the placeable unit stays the whole instance (§1) — Fabric must not invent grid
> semantics adapter‑side to fill the gap.

> **FINDING B — fabric‑core contains a second, parallel storage engine.**
> `fabric-core` ships `FacetEngine`, `ShardStorage`, `AtomRecord`, `Value`, `Grid`,
> `Atom` — an in‑memory key→value store (`engine.rs`, `storage.rs`, `record.rs`,
> `value.rs`). Per §29 **Persistence is owned by FacetQL.** This engine is
> scaffold; it must **not** be grown into a real second database. The root‑correct
> move is to let fabric‑core keep only the *distribution* primitives (`DbmsId`,
> `Coordinate`, `Shard` as a placement unit, `Topology`) and treat the storage
> half as inert placeholder — and, where fabric needs to persist its own control
> state, persist it **in FacetQL** (see §5), not in a fabric‑owned engine.

---

## 1. What Fabric actually is, relative to the stack

**Fabric is the distribution / control plane that sits _around and beneath_
FacetQL.** It does not store application data and does not replace FacetQL. Its
job (per `fabric/README.md` and the crate code) is the loop:

```
observe → analyze → predict → optimize → act → measure → learn
```

over a **fleet of FacetQL instances**. The README states the rule plainly:
"**FacetQL is the DBMS. Facet Fabric is the distributed system that observes,
understands, predicts, and optimizes how FacetQL database computers operate.**"

### The concrete type‑level relationship

- **A FacetQL instance == one `fabric_core::DbmsNode`** (`topology.rs`):
  `DbmsNode { id: DbmsId, region: String, shards: Vec<Shard> }`. `DbmsId(String)`
  is that instance's stable identity. So yes — **FacetQL becomes the "DBMS node"
  Fabric places and moves.**
- **Fabric orchestrates _multiple_ FacetQL instances.** `fabric_core::Topology`
  is `{ nodes: Vec<DbmsNode> }` — the fleet. The live map Fabric actually uses is
  `fabric_topology::TopologyRegistry` (`registry.rs`): a
  `HashMap<(shard_id: u64, Coordinate), Placement>` where
  `Placement { dbms_id, shard_id, coordinate, region }` (`placement.rs`) answers
  "which physical FacetQL instance currently holds this logical (shard,
  coordinate)?" `place` / `locate` / `move_coordinate` / `remove` are the
  placement operations. The doc comment is exactly right: *"The logical
  coordinate never changes because of physical movement. The Fabric is allowed to
  change this placement."*
- **The optimizer emits placement decisions, not data mutations.**
  `fabric_optimizer::OptimizationDecision { coordinate, action, expected_gain,
  estimated_cost, confidence }` with `OptimizationAction ∈ {NoAction, Replicate{target:DbmsId},
  Move{target:DbmsId}, Split, Isolate, Colocate{target:Coordinate}}` (`decision.rs`).
  These act on *placement*, i.e. which DbmsId serves a coordinate — never on the
  contents of a node.

### The seam(s), named precisely

There are **two** logical seams, and one of them does not exist yet in either
direction:

1. **Telemetry / observation seam (FacetQL → Fabric).** Fabric's whole pipeline
   is data‑starved. `WorkloadAnalyzer::observe` (`analyzer.rs`) consumes
   `fabric_telemetry::Observation`, which carries `WorkloadMetrics`
   (`metrics.rs`: ops/sec, reads/sec, writes/sec, latencies, cpu/mem, queue
   depth). Today the **only** source of these is a replayed JSON file:
   `fabric-cli/src/session.rs` reads `Vec<FabricMessage>` off disk and feeds
   `FabricRuntime::handle`. Its own doc comment is honest: *"Fabric today has no
   persistent daemon or live‑state service … With no input file, the runtime is
   empty."* **FacetQL currently emits nothing Fabric can observe** (confirmed:
   no `/stats`, `/metrics`, or `/health` route in `routes.rs`; the CLI `stats`
   command counts kinds *client‑side* by enumerating `/nodes`). This is the seam
   to build first — see §2 and §6.

2. **Control seam (Fabric → FacetQL).** When Fabric decides `Replicate`/`Move`,
   something must (a) durably record the new placement and (b) actually stand up /
   redirect a FacetQL instance. `fabric-protocol` already models the *inbound*
   direction — `facet-protocol` binary is an axum server on `127.0.0.1:7700`
   exposing `POST /v1/message` taking `FabricMessage ∈ {RegisterNode, Heartbeat,
   Topology, Telemetry, Workload}` and returning `FabricResponse` (incl.
   `OptimizationProposal`). But `ProtocolServer::handle` and
   `FabricRuntime::handle` only *acknowledge* — registration returns the id back,
   no registry is persisted (matches NOTES FAB‑004). The outbound control action
   (execute a placement) is entirely unbuilt (NOTES EPIC 21). **This seam is
   Phase‑6+ and explicitly out of scope to implement now; the design is in §5.**

### Where the grid/shard model actually stands (Finding A, expanded)

Fabric's 12×13/156 grid and `Shard` come from `fabric-core`, and FacetQL **has no
counterpart**: FacetQL's engine is a flat `HashMap<address, Node>` with no shard
and no grid constraint on its 4‑axis coordinate. Therefore, for any realistic
first integration, **the placeable unit is the whole FacetQL instance (one
`DbmsNode`), not a sub‑instance grid cell.** Fabric's finer‑grained
(shard, coordinate) placement is a *future* capability that presupposes FacetQL
gaining a native shard/partition concept (a FacetQL‑owned primitive, not a fabric
one). The plan below does not require it and does not invent it adapter‑side.

---

## 2. Fabric ↔ FacetQL — the concrete integration seam

### What Fabric needs from FacetQL

Exactly one thing to become useful with real (not replayed) data: **FacetQL must
emit observations.** Fabric already has the ingestion and analysis side
(`FabricRuntime::ingest_telemetry` → `analyzer.observe` → `WorkloadProfile` →
`optimizer`). What is missing is a real producer of `WorkloadMetrics`.

Two shapes are possible; the **owning‑layer** analysis decides which primitive to
build and where:

- **The measurement itself (counts, operation counters, later latency) is a
  FacetQL‑owned primitive.** Only the engine can count its own reads/writes and
  nodes. Per §29 (Persistence→FacetQL) and NOTES EPIC 08 (observability is a
  FacetQL epic), **this is built in FacetQL**, not scraped or guessed by a fabric
  adapter. This is the root solution and it is independently valuable (health,
  readiness, capacity) regardless of Fabric.
- **The transport (poll vs push) is a Fabric‑owned adapter concern.** Whether
  Fabric polls `GET /stats` on an interval or FacetQL pushes
  `FabricMessage::Telemetry` to `POST /v1/message` is a fabric decision. **v1
  recommendation: Fabric polls.** Rationale: polling keeps FacetQL completely
  unaware of Fabric (no fabric client, no fabric address, no coupling in the
  primary repo — critical while FacetQL is the priority and Fabric is #12).
  Push can come later without changing the FacetQL primitive.

### The primitive to build in FacetQL (maps to §6 handoff)

A native **`GET /stats`** endpoint. Mapping to the canonical contract: this is a
**new endpoint**, additive to §4 / §4b, same auth (`x-api-key`), admin‑gated (it
exposes fleet‑wide counts). It is NOT a change to any existing op — no contract
drift. Precise shape in §6.

Why counters and not just counts: Fabric's `WorkloadProfile`/pressure model
(`profile.rs::calculate_pressure`, `predictor.rs::hotspot_probability`) is driven
by **rates and ratios** (ops/sec, read_ratio, write_ratio) plus resource/latency
pressure. Structural counts alone (node_count per kind) can't produce a rate. The
minimal honest primitive therefore includes **monotonic `reads_total` /
`writes_total` counters**, so a poller differencing two samples yields ops/sec and
the read/write split. CPU/memory/queue/latency are **out of v1 scope** and the
endpoint should simply omit them rather than emit fake zeros — Fabric's model
already treats absent resource pressure as low pressure, which is truthful.

### Coordinate reconciliation at this seam (Finding A)

The telemetry seam does **not** require reconciling the two `Coordinate` types:
instance‑level `/stats` is keyed by `DbmsId` (the instance), not by a grid cell.
This is another reason to start instance‑level. Grid‑cell telemetry (per‑156‑cell
samples) is deferred until FacetQL has a native shard/cell concept to attribute
operations to; when that exists, the FacetQL side owns the mapping from its 4‑axis
coordinate to a placement cell — Fabric must not guess it.

---

## 3. Fabric ↔ FCT — does FCT need to know about Fabric?

**Conclusion: No. FCT must NOT become fabric‑aware. This is a firm recommendation,
with reasoning — not a deferral.**

Evidence: FCT talks to persistence through exactly one Go interface, `Store`
(`fct/runtime/store.go`), whose FacetQL implementation `fqStore`/`fqClient`
(`fqstore.go`, `fqclient.go`) speaks the §4b HTTP contract to a **single** base
URL parsed from `FACET_DATABASE_URL` (`parseFacetQLURL`). Fabric operates at the
*distribution* layer, one level **below** that single endpoint.

The root‑correct way to introduce Fabric routing later is the **transparent front
door**: when Fabric does routing/replication, it presents a **FacetQL‑wire‑
compatible** endpoint (the exact §4b contract) and `FACET_DATABASE_URL` points at
*that* instead of at one FacetQL. `fqStore` is then **unchanged** — it still POSTs
`/transaction`, GETs `/nodes`, etc.; the fabric router decides which physical
FacetQL instance serves the request. This is the standard "the proxy speaks the
backend's protocol" pattern and it preserves the **no‑rework** rule absolutely:
zero change to fct, zero change to the contract, zero change to facetql.

Explicitly rejected (would be a patch): teaching `fqStore` about placements,
DbmsIds, or a fabric control API. That would leak distribution concerns into the
language layer, violate §29 ownership, and create rework the moment placement
logic changes. Do not do it.

The one legitimate, *optional*, far‑future touch point: FCT's compiler already
computes placement calculus (server/client) — NOTES EPIC 12. That is *code*
placement, unrelated to Fabric's *data* placement; they must not be conflated. If
anything ever crosses here it is a new, separate design, not a `Store` change.

---

## 4. Fabric ↔ Facets — any direct relationship?

**None. Refuted with evidence.** `grep -rni fabric` over the entire `facets`
repo returns zero hits. Facets is the rendering / app‑definition layer (§29
Rendering→Facets), three layers above distribution (Facets→FCT→FacetQL→Fabric).
A component library has no business knowing how its data is physically placed
across a FacetQL fleet; that is precisely the separation the stack exists to
enforce. There is no seam here and none should be created.

---

## 5. Sequencing (dependency‑ordered, with domain owners)

Legend for owner: **[FQL]** FacetQL/Persistence · **[FAB]** Fabric/Distribution ·
**[FCT]** Language · **[—]** no work / explicitly none.

### Tier 0 — prerequisites that already exist (do not redo)
- FacetQL wire contract §4b, `POST /transaction`, `/nodes`, `/nodes/query`,
  atomic `claim`, `/events`+`/publish` — **done** (§6 task table).
- Fabric ingestion+analysis+optimizer pipeline — **exists** (scaffold‑complete,
  session‑replay driven).

### Tier 1 — make FacetQL "fabric‑ready" (the only thing needed to start; §6)
1. **[FQL] Native `GET /stats` observability endpoint** — engine counters +
   handler. **This is the single unblocking primitive.** Fully specified in §6.
   Independent value (health/readiness), so worth doing even before Fabric is
   prioritized. *Belongs to the FacetQL coding agent spawned after this plan.*

### Tier 2 — Fabric consumes real data (Fabric‑only, no other repo touched)
2. **[FAB] Telemetry poller adapter** — a fabric component that polls one or more
   FacetQL `/stats` endpoints on an interval, differences successive samples into
   `WorkloadMetrics`, wraps them as `fabric_protocol::TelemetryBatch` /
   `Observation`, and feeds `FabricRuntime::ingest_telemetry`. Replaces the
   `session.rs` file‑replay as the live source. Parallelizable with anything;
   depends only on Tier 1. No FacetQL or fct change.
3. **[FAB] Persistent node registry** (NOTES FAB‑004) — make `RegisterNode` /
   `Heartbeat` actually retain fleet state instead of acking. Depends on nothing;
   can run parallel to #2.

### Tier 3 — Fabric persists its own control state (root‑solution for Finding B)
4. **[FAB, using FQL primitives] Store the `TopologyRegistry` in FacetQL** as
   nodes of a reserved kind (e.g. `__fabric_placement`, one node per
   `(shard_id, coordinate)` with the `Placement` as `data` JSON). This is how
   Fabric gets durability **without growing its own engine** (Finding B): it
   dogfoods FacetQL as its own control store, exactly as fct's `fqStore` uses
   reserved kinds `__session`/`__job`/`__cron`. Needs no new FacetQL primitive
   beyond what exists — upsert + `/nodes/query` suffice for read/write; the atomic
   `claim` covers "one Fabric controller owns a placement mutation."
5. **[FQL, conditional] CAS / `set_if` tx op** — the §28 request already logged
   for fct's `ReserveCron`. Fabric's control plane (Tier 4) wants the *same*
   primitive for "advance topology version only if unchanged" (optimistic
   concurrency on placement). **Recommendation: Fabric does not need a NEW,
   fabric‑specific primitive — it needs the identical `set_if` already queued for
   FacetQL.** So this stays one FacetQL‑owned item serving both callers; do not
   duplicate it. Not a launch blocker for either.

### Tier 4 — Fabric acts (Phase‑6+, design only; NOTES EPIC 21/22/23)
6. **[FAB] Control‑plane executor** — proposal → validate → authorize → dry‑run →
   execute → verify → commit, with signed/versioned decisions (NOTES FAB‑SEC‑001).
   Depends on #4 (durable topology) and #5 (CAS). **Out of scope now.**
7. **[FAB] Transparent FacetQL‑wire router** (the §3 front door) — enables real
   `Move`/`Replicate` without touching fct. Depends on real multi‑node FacetQL.
   **Out of scope now.**

### Parallelism summary
- Tier 1 (#1) is the gate; everything else waits on it.
- After #1: #2, #3 run in parallel (both Fabric‑only).
- #4 waits on #2/#3; #5 is FacetQL‑owned and can be scheduled with the existing
  §28 cron CAS work. #6/#7 are Phase‑6+.

### Note on the §28 CAS/`set_if` request
Already in `AGENT_LOG.md` for `ReserveCron`:
`{type:"set_if", address, field, expect_le:<now>, set:{…}}` → applied?. **Fabric
needs nothing beyond this.** Its topology‑version optimistic update is the same
compare‑and‑swap shape (`expect_eq:<version>`), so if `set_if` is generalized to
support an equality predicate as well as `expect_le`, it serves both. Flag for the
FacetQL owner: design `set_if` with a small predicate (`expect_le` **and**
`expect_eq`) rather than hard‑coding the cron case — one primitive, two callers,
no rework later.

---

## 6. Build FIRST — `GET /stats` in FacetQL (the handoff spec)

This section is the direct, implementable handoff for the **FacetQL coding agent**
(Persistence owner). It is minimal, additive, contract‑safe, and independently
useful.

### Goal
A native endpoint that reports FacetQL's own storage/operation statistics, so a
Fabric poller can derive real `WorkloadMetrics` (and so FacetQL gains a real
health/capacity surface). No existing endpoint or op changes.

### Files
- `facetql/src/storage/engine.rs` — add operation counters + a `stats()` method.
- `facetql/src/api/routes.rs` — add response structs, a `stats` handler, and one
  route line.

### Engine changes (`src/storage/engine.rs`)
1. Add two monotonic counters to `StorageEngine`. They must be mutable under the
   existing `RwLock` **read** guard (reads happen while holding a read lock), so
   use atomics:
   ```rust
   use std::sync::atomic::{AtomicU64, Ordering};
   // in struct StorageEngine:
   reads_total: AtomicU64,   // init AtomicU64::new(0) in new()
   writes_total: AtomicU64,  // init AtomicU64::new(0) in new()
   ```
   (Both are process‑lifetime counters; they are NOT persisted and reset to 0 on
   restart — that is correct and expected for a rate source. Document it.)
2. Increment `writes_total` by 1 on each applied write in `insert`, `delete`,
   `insert_edge`, and once per successful `execute_transaction` op applied
   (increment inside the apply pass, not per validation). Increment `reads_total`
   by 1 at the top of `get`, `query`, and `query_where`. Use
   `Ordering::Relaxed` — these are statistics, not synchronization.
3. Add:
   ```rust
   pub fn stats(&self) -> EngineStats {
       use std::collections::BTreeMap;
       let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();
       for node in self.nodes.values() {
           *by_kind.entry(node.kind.clone()).or_default() += 1;
       }
       let edge_count = self.edges_out.values().map(|v| v.len() as u64).sum();
       EngineStats {
           node_count: self.nodes.len() as u64,
           edge_count,
           user_count: self.users.len() as u64,
           history_entries: self.history.values().map(|v| v.len() as u64).sum(),
           kinds: by_kind.into_iter()
               .map(|(kind, count)| KindCount { kind, count })
               .collect(),
           reads_total: self.reads_total.load(Ordering::Relaxed),
           writes_total: self.writes_total.load(Ordering::Relaxed),
       }
   }
   ```
   with plain serializable structs `EngineStats` / `KindCount` (define in engine
   or routes — either is fine; keep them `#[derive(Serialize)]`). `kinds` is
   sorted (BTreeMap) so output is deterministic and testable.

### Route changes (`src/api/routes.rs`)
```rust
#[derive(Serialize)]
struct KindCount { kind: String, count: u64 }

#[derive(Serialize)]
struct StatsResponse {
    node_count: u64,
    edge_count: u64,
    user_count: u64,
    history_entries: u64,
    kinds: Vec<KindCount>,
    reads_total: u64,
    writes_total: u64,
}

async fn stats(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }
    let engine = db.engine.read().expect("storage engine lock poisoned");
    let s = engine.stats();
    (StatusCode::OK, Json(/* map s -> StatsResponse */)).into_response()
}
```
Add to the protected router (so it inherits `x-api-key` auth):
```rust
.route("/stats", get(stats))
```
Admin‑gated because it exposes fleet‑wide counts; matches the existing
`list_users`/`admin` gating pattern already in this file.

### Wire contract (new; additive to §4/§4b)
```
GET /stats           Header: x-api-key: <admin token>
200 OK  application/json:
{
  "node_count": 1234,
  "edge_count": 56,
  "user_count": 3,
  "history_entries": 42,
  "kinds": [ { "kind": "Post", "count": 900 }, { "kind": "User", "count": 334 } ],
  "reads_total": 98765,
  "writes_total": 4321
}
403 FORBIDDEN  if the caller is not admin.
```
Fabric derives, between two polls Δt apart:
`ops_per_second = (Δreads_total + Δwrites_total)/Δt`,
`read_ratio = Δreads_total / (Δreads_total + Δwrites_total)` (guard divide‑by‑zero),
feeding `fabric_telemetry::WorkloadMetrics` / `fabric_protocol::TelemetrySample`.
Resource/latency fields are intentionally omitted in v1 (see §2) — Fabric treats
their absence as low pressure, which is truthful, not faked.

### Tests to add
- **Engine unit test** (`src/storage/engine.rs` tests): insert N nodes across 2
  kinds + M `get`s; assert `stats().node_count == N`, `kinds` grouping exact and
  sorted, `writes_total >= N`, `reads_total >= M`. Use the test‑time data‑dir
  discipline already noted in the log.
- **Route test**: `GET /stats` with an admin token → 200 with expected counts;
  with a non‑admin token → 403. (Mirror the existing admin‑route tests.)

### Why this is the right first move
- **Owning layer**: only the engine can count its own ops → FacetQL owns it (§29).
- **Root, not patch**: it is a real primitive (also EPIC 08), not a fabric‑side
  scrape of `/nodes`.
- **Zero coupling / zero rework**: additive endpoint; no existing contract
  changes; FacetQL stays unaware of Fabric; fct untouched; poll transport keeps
  the dependency arrow pointing the right way.
- **Unblocks Fabric entirely for Tier 2**: with `/stats` live, the fabric poller
  (a fabric‑only task) turns the currently file‑replayed pipeline into a real one.

---

## 7. Alignment with the authoritative architecture spec

This plan is checked against the F33D3R / Facet Authoritative Engineering
Specification. Every recommendation here is consistent with it; the points below
make the mapping explicit so no downstream agent has to re‑derive it.

- **Fabric is the distributed systems/scaling layer, not a database (spec §6, §15).**
  This plan treats Fabric strictly as topology/placement/workload/optimizer over a
  fleet of FacetQL nodes and never as a store of application data. Directly
  reinforced by **Finding B**: `fabric-core`'s `FacetEngine`/`ShardStorage` must not
  be grown into a second database, because **Persistence is owned by FacetQL**
  (spec §4, §15). Where Fabric needs durability it persists **in FacetQL** (§5,
  Tier 3), never in a fabric‑owned engine.

- **No hidden SQL, no external DB, no swap‑it‑out‑later (spec §1, §10, §14).**
  The one primitive this plan asks for — `GET /stats` (§6) — is a **native FacetQL
  engine primitive** (in‑engine read/write counters + per‑kind counts), not a
  scrape, not a translator, not a dependency on anything external. It answers the
  spec's mandated question "what native primitive does FacetQL need here?" rather
  than reaching for an outside system.

- **Finish single‑node FacetQL first; Fabric is later (spec §8 Phases, §11).**
  This plan keeps Fabric at §33 #12 / Phase‑6+ and does not propose building the
  distributed fabric ahead of the correct core. `GET /stats` is deliberately
  scoped so it is *also* useful to the single‑node core (health/readiness/capacity,
  spec §8 Phase‑2 + §9 auditability), i.e. it advances the current priority instead
  of diverting to distribution.

- **The 12×13/156 grid is FacetQL's foundation, owned by FacetQL (spec §5, §15).**
  Reflected in the revised **Finding A**: the grid addressing model must become
  native to FacetQL and Fabric's `Placement` must operate over *that* model. This
  plan explicitly forbids Fabric inventing grid/shard semantics adapter‑side to
  paper over FacetQL not yet having them — that would be the reinvention the spec
  warns against (§11) and a patch (§29 ownership). Until FacetQL owns a native
  shard/cell concept, the placeable unit is the whole instance.

- **Separation of responsibilities is preserved (spec §15).** FCT stays
  language‑only and fabric‑agnostic (this plan §3 — future routing hides behind a
  FacetQL‑wire‑compatible front door, so `fqStore` never changes). Facets stays
  model‑only with no Fabric relationship (§4). FacetQL owns storage/execution/
  telemetry (so `/stats` lives there, §6). Fabric owns topology/placement/
  optimization only (§1). No layer absorbs another.

- **Security designed in, not bolted on (spec §9).** The `/stats` endpoint is
  admin‑gated behind the existing `x-api-key` auth (§6) rather than open; the
  Phase‑6+ control plane is specified with signed/versioned decisions and
  authorized transport (§5 Tier 4, referencing FAB‑SEC‑001) — designed up front
  even though built later.

- **Rust / Axum / Tokio core (spec §12).** All proposed FacetQL work is native Rust
  in the existing axum/tokio surface (`src/api/routes.rs`, `src/storage/engine.rs`);
  no new runtime or language is introduced into the data engine.

- **Respect existing code; don't rewrite (spec §13, and §31 startup procedure).**
  This plan was produced by auditing the actual repositories (evidence in §0) and
  extends what exists — additive endpoint, existing engine structures, existing
  fabric ingestion pipeline — rather than proposing a from‑scratch redesign of any
  repo.

## 8. One‑paragraph conclusion

Fabric is the distribution/control plane that treats each **FacetQL instance as a
`DbmsNode`** it places and moves via `TopologyRegistry`/`Placement`; it never
stores app data and never sits in fct's write path. The stack has **no fabric
wire today**, which is fine — Fabric is §33 #12 and not a launch blocker. The one
genuine seam that matters now is **telemetry: FacetQL must emit observations**, and
the correct, minimal, root‑layer primitive is a native **`GET /stats`** endpoint in
FacetQL (engine read/write counters + per‑kind counts), from which a fabric‑only
poller derives real `WorkloadMetrics`. FCT stays fabric‑agnostic by design (future
routing hides behind a FacetQL‑wire‑compatible front door, so `fqStore` never
changes); Facets has no relationship at all. Fabric must not grow its own
duplicate storage engine (`fabric-core` Finding B) — when it needs durability it
should persist its topology **in FacetQL** under a reserved kind, reusing existing
primitives plus the already‑queued §28 `set_if` CAS. Build `GET /stats` first;
everything else in Fabric is Phase‑6+ design that this document has now made ready.
