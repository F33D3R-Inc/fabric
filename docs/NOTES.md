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


Yes. **The ML is one part of the Fabric, not the Fabric itself.**

Think of **Facet Fabric** as the control/intelligence layer that sits around the FacetQL DBMS network.

```text
                         FACET FABRIC
                    ┌─────────────────────┐
                    │                     │
                    │  Observe            │
                    │  Understand         │
                    │  Predict            │
                    │  Decide             │
                    │  Coordinate         │
                    │  Move/reshape       │
                    │                     │
                    └──────────┬──────────┘
                               │
                 ┌─────────────┼─────────────┐
                 │             │             │
                ML        Optimizer      Topology
                 │             │             │
                 └─────────────┼─────────────┘
                               │
                        FACET PROTOCOL
                               │
             ┌─────────────────┼─────────────────┐
             ▼                 ▼                 ▼
         FacetQL #1        FacetQL #2        FacetQL #N
         DBMS node         DBMS node         DBMS node
```

### What each part actually does

**FacetQL = the database engine.**

It is where the data actually lives and where the database operations happen.

Your fundamental address is:

```text
(x, y)
```

inside the 12×13 logical grid.

And beneath that you eventually have:

```text
coordinate
   ↓
atom
   ↓
cell
   ↓
156-cell grid
   ↓
shard
   ↓
FacetQL DBMS
```

---

**Facet Protocol = how the DBMS nodes communicate.**

It allows separate FacetQL instances to communicate across machines/networks.

```text
FacetQL A ←→ Facet Protocol ←→ FacetQL B
                     ↕
                 Facet Fabric
```

---

**Facet Fabric = the distributed intelligence/control plane.**

This is the piece we're building now.

It watches the entire system.

For example:

```text
coordinate (7,3)

2,000 ops/sec
CPU 91%
queue 18,400
latency rising
```

Fabric sees that.

Then:

### Telemetry

Collects what's happening.

```text
"What's happening?"
```

### Workload

Understands the behavior.

```text
"This coordinate is becoming extremely hot."
```

### ML

Predicts what is likely to happen.

```text
"92% probability this workload will become
a hotspot within the next interval."
```

### Optimizer

Determines what should happen.

```text
"Replicate this workload to DBMS-07."
```

### Topology

Knows where everything physically exists.

```text
logical coordinate: (7,3)

currently:
DBMS-02 / Phoenix

possible:
DBMS-07 / Dallas
```

### Fabric executes the decision

And this is the crucial part:

**ML doesn't directly control the database.**

It provides intelligence to the Fabric.

```text
Telemetry
    ↓
Workload Analysis
    ↓
ML prediction
    ↓
Optimizer
    ↓
Fabric decision
    ↓
Facet Protocol
    ↓
FacetQL nodes
```

---

## And this is where your bigger idea becomes interesting

You don't want six unrelated services.

You are building a **self-observing distributed database fabric**.

Eventually Fabric should be capable of seeing something like:

```text
             FACET FABRIC
                  │
       ┌──────────┼──────────┐
       │          │          │
     DBMS 01    DBMS 02    DBMS 03
       │          │          │
     shards     shards     shards
       │          │          │
    156 cells  156 cells  156 cells
       │          │          │
      atoms      atoms      atoms
```

Then the Fabric continuously asks:

```text
Where is the workload?

Where is the data?

Where is pressure building?

Which workloads belong together?

Which workloads should separate?

Where should replicas exist?

Where should data move?

Where should capacity be added?

Which DBMS should communicate with which?

What is the cheapest/fastest topology?

What is likely to become a problem before it happens?
```

**That last question is where your ML becomes extremely important.**

And because you specifically decided against external LLMs, the ML layer we're building is intended to be **F33D3R-controlled numerical/system ML**, not "send the database state to ChatGPT and ask what to do."

So the mental model I want you to keep is:

> **FacetQL is the computer. Facet Protocol is its nervous system. Facet Fabric is the control/intelligence system. ML is one of the brains inside the Fabric.**

And **fct is the language used to build applications and workloads on top of that computer.**

That's the architecture we're carving toward.
