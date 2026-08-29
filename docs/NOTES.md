Yes. **Those six are the correct starting boundary.** Don't create routing, replication, migration, simulator, or controller yet—we can add them once the core model is proven.

From inside your `fabric` repo:

```bash
mkdir -p crates/{fabric-core,fabric-telemetry,fabric-topology,fabric-workload,fabric-ml,fabric-optimizer}
```

Then make it a Rust workspace:

```bash
cat > Cargo.toml <<'EOF'
[workspace]
resolver = "2"
members = [
    "crates/fabric-core",
    "crates/fabric-telemetry",
    "crates/fabric-topology",
    "crates/fabric-workload",
    "crates/fabric-ml",
    "crates/fabric-optimizer",
]
EOF
```

Create the six crates:

```bash
cargo new crates/fabric-core --lib
cargo new crates/fabric-telemetry --lib
cargo new crates/fabric-topology --lib
cargo new crates/fabric-workload --lib
cargo new crates/fabric-ml --lib
cargo new crates/fabric-optimizer --lib
```

Then:

```bash
cargo check --workspace
```

You should get all six compiling.

### What each one means

```text
fabric-core
    ↓
The vocabulary and contracts of the Fabric.
Coordinates, identities, workload IDs, topology IDs,
decisions, policies, etc.

fabric-telemetry
    ↓
Observes FacetQL.
Reads, writes, latency, IO, CPU, memory, network,
hot coordinates, event rates, replication state, etc.

fabric-topology
    ↓
Maintains the Fabric's map of the distributed world.
DBMS → shard → grid → coordinate → placement.

fabric-workload
    ↓
Turns raw telemetry into workload behavior.
Hotspots, locality, temporal patterns, read/write
pressure, workload classes, etc.

fabric-ml
    ↓
Prediction.
Forecasting, anomaly detection, hotspot prediction,
capacity prediction, workload classification.

fabric-optimizer
    ↓
Decides what topology would be better.
Split, merge, move, replicate, colocate, separate,
tier, etc.
```

And the dependency direction should initially be:

```text
                 fabric-core
                /     |      \
               /      |       \
              ▼       ▼        ▼
       telemetry   topology  workload
                           \     /
                            ▼   ▼
                          fabric-ml
                              │
                              ▼
                       fabric-optimizer
```

**The most important rule:** `fabric-ml` should depend on the data produced by the other components, but the ML model should **not own the topology or execute database mutations**.

The optimizer produces a **proposal**. FacetQL ultimately decides whether/how that proposal can be executed.

Once `cargo check --workspace` is green, **the first real thing we should design is `fabric-core`**, because every other crate needs to agree on what a coordinate, shard, workload, observation, topology, and Fabric decision actually are.
