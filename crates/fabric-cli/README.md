# fabric — Facet Fabric operator CLI

`fabric` is the native operator command-line tool for **Facet Fabric**, the
distribution/scaling layer of the F33D3R stack. It is dependency-free (no `clap`,
no async runtime) and links only the real Fabric crates.

## The "no live daemon yet" reality

Fabric is early. There is **no persistent daemon and no live-state service**: the
protocol server (`fabric-protocol`) acknowledges messages but does not retain
queryable state. So this CLI deliberately does **not** pretend to connect to,
manage, or query a running cluster.

Instead it drives the *real* in-process analysis pipeline (`fabric-runtime`) over
a **session** — a captured or authored file of protocol messages — and reports
exactly what the actual Fabric crates compute from them. Every command is backed
by a concrete, already-implemented crate capability. Capabilities Fabric does not
have yet (node registry, heartbeat liveness, historical time-series) have **no
command** — they are honestly absent, not faked.

With no session provided, the runtime is empty and every command reports the
empty state truthfully.

## Usage

```
fabric <command> [--input <file>] [--json]
```

| Option | Meaning |
|--------|---------|
| `-i`, `--input <file>` | JSON array of `FabricMessage` values to replay. `-` reads from **stdin**. |
| `--json` | Emit machine-readable JSON instead of the human-readable table. |
| `-h`, `--help` | Show help (top-level, or per-command after a command name). |
| `-V`, `--version` | Show version. |

## Commands

Each command replays the session through `FabricRuntime`, then renders one facet
of the resulting state:

| Command | Backing capability | Reports |
|---------|--------------------|---------|
| `status` | `fabric-runtime` + `fabric-core` grid geometry | Protocol version, grid geometry, and counts of placements, profiles, observations and hot coordinates. |
| `topology` | `fabric-topology::TopologyRegistry` | Physical placements: coordinate, shard, region, DBMS id. |
| `workload` | `fabric-workload::WorkloadAnalyzer` | Workload profiles: ops/sec, read/write ratio, pressure level, hot flag. |
| `metrics` | `fabric-telemetry` + `fabric-runtime::FabricState` | Latest raw metrics per observed location: ops/sec, CPU, memory, queue depth. |
| `placement` | `fabric-optimizer::WorkloadOptimizer` | The optimizer's placement decision per profile: action, gain, cost, score, confidence, execute flag. |
| `predict` | `fabric-ml::WorkloadPredictor` (via the optimizer's own predictor) | Per profile: hotspot probability + likely-hot verdict, and anomaly score + anomalous verdict. |
| `validate` | `fabric-core::Coordinate::is_valid` | Every ingested coordinate checked against the grid bounds; lists any that fall outside. |

`predict` uses the *optimizer's own* predictor instance, so its numbers are
exactly the ones the placement policy reacts to — not a separately-configured
model.

## Session file format

A session is a **JSON array of `FabricMessage` values** — the same messages a
Fabric node emits over the protocol. `FabricMessage` is an externally-tagged
enum, so each element is a single-key object whose key is the variant name:

| Variant | Effect on the runtime |
|---------|-----------------------|
| `RegisterNode` | Acknowledged only (no node registry exists yet — no queryable effect). |
| `Heartbeat` | Acknowledged only (no liveness tracking yet). |
| `Topology` | Ingested into the topology registry (drives `topology`). |
| `Telemetry` | Ingested into analyzer + state (drives `workload`, `metrics`, `placement`, `predict`). |
| `Workload` | A lighter single-sample observation; ingested like telemetry. |

`RegisterNode` and `Heartbeat` parse and are accepted, but have no queryable
effect because Fabric has no node registry or liveness service yet.

### Example

A runnable example ships at [`examples/session.json`](examples/session.json):

```json
[
  {
    "Topology": {
      "node_id": "node-a",
      "timestamp_ms": 1000,
      "placements": [
        { "coordinate": { "x": 2, "y": 3 }, "dbms_id": "node-a", "region": "us-east" },
        { "coordinate": { "x": 5, "y": 5 }, "dbms_id": "node-b", "region": "us-west" }
      ]
    }
  },
  {
    "Telemetry": {
      "timestamp_ms": 2000,
      "node_id": "node-a",
      "shard": { "id": 7, "workload_domain": "orders" },
      "samples": [
        {
          "coordinate": { "x": 2, "y": 3 },
          "operations_per_second": 50000.0,
          "read_ratio": 0.9, "write_ratio": 0.1,
          "read_latency_us": 100000.0, "write_latency_us": 5000.0,
          "cpu_utilization": 0.95, "memory_utilization": 0.95,
          "queue_depth": 10000
        }
      ]
    }
  }
]
```

Run it:

```sh
fabric status   --input examples/session.json
fabric predict  --input examples/session.json --json
cat examples/session.json | fabric placement --input -
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success. For `validate`, also means every coordinate is within the grid. |
| `1` | Runtime error: the session file could not be read or parsed (missing file, malformed JSON, unknown message variant). |
| `2` | Usage error: unknown command, unknown flag, missing flag value, or unexpected argument. |
| `3` | `validate` only — the command ran successfully but found out-of-grid coordinates. Distinct from `1` so scripts can tell "the tool failed" from "the input is invalid". |

## Capabilities deliberately absent

These would require Fabric capabilities that **do not exist yet**, so no command
pretends to offer them:

- **Live cluster / daemon queries** — there is no running daemon to talk to.
- **Node inventory / heartbeat liveness** — `RegisterNode` and `Heartbeat` are
  acknowledged but not retained; there is no registry to list.
- **Historical / time-series views** — `FabricState` keeps only the *latest*
  observation per location; there is no time-series store.
- **Mutating operations** (apply a placement, move a coordinate) — the topology
  registry can mutate in-process, but there is no durable cluster to apply
  changes to, so the CLI stays read-only/analytical.
