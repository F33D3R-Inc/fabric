# Facet Fabric

## The Distributed Data Fabric for FacetQL

Facet Fabric is the intelligent distributed control and optimization layer for **FacetQL**.

**FacetQL is the DBMS.**

**Facet Fabric is the distributed system that observes, understands, predicts, and optimizes how FacetQL database computers operate across machines, regions, networks, and workloads.**

The long-term objective is to create a new generation of database infrastructure in which storage, workload placement, topology, and optimization are designed around the FacetQL coordinate model rather than traditional relational database architecture.

---

# Architecture

```text
                         APPLICATION
                              │
                              ▼
                             FCT
                              │
                              ▼
                           FACETQL
                         ┌──────────┐
                         │   DBMS   │
                         │          │
                         │  Shards  │
                         │    ↓     │
                         │ 12 × 13  │
                         │   Grid   │
                         └────┬─────┘
                              │
                       Facet Protocol
                              │
                              ▼
                    ┌──────────────────┐
                    │   FACET FABRIC   │
                    │                  │
                    │ Core             │
                    │ Telemetry        │
                    │ Topology         │
                    │ Workload         │
                    │ ML               │
                    │ Optimizer        │
                    └────────┬─────────┘
                             │
                             ▼
                    Optimization Decision
                             │
                             ▼
                          FACETQL
```

Facet Fabric does not replace FacetQL.

Facet Fabric operates and optimizes a distributed fleet of FacetQL database instances.

---

# Core Principle

Traditional database infrastructure generally relies on static configuration, database administrators, external orchestration, and manually defined scaling policies to determine where workloads should run and how resources should be allocated.

Facet Fabric is designed around a different model:

```text
Observe
   ↓
Analyze
   ↓
Predict
   ↓
Optimize
   ↓
Act
   ↓
Measure
   ↓
Learn
   ↺
```

The Fabric continuously builds a model of the distributed database environment and uses that model to make workload-placement and topology decisions.

The ultimate objective is a **self-optimizing distributed data fabric**.

---

# FacetQL

FacetQL is the underlying DBMS.

Its storage model is based on a coordinate-oriented data space.

A fundamental FacetQL unit is:

```text
1 Shard
   ↓
12 × 13 Grid
   ↓
156 Cells
```

The 156 cells constitute one shard.

Coordinates provide logical locations within that grid.

The physical machine hosting a coordinate is a separate concern.

For example:

```text
LOGICAL

Shard 42
└── Coordinate (7,3)


PHYSICAL

Shard 42
└── Coordinate (7,3)
       └── DBMS-17
           └── Region: Phoenix
```

Facet Fabric can change the physical placement without changing the logical coordinate.

This separation is fundamental to the architecture.

---

# The Coordinate Model

FacetQL is designed around the idea that data can be addressed through logical coordinates rather than requiring the application to understand physical storage placement.

The conceptual hierarchy is:

```text
DBMS
 │
 └── Shard
      │
      └── 12 × 13 Grid
           │
           ├── Cell
           │
           ├── Cell
           │
           ├── ...
           │
           └── Cell
```

One shard contains exactly:

```text
12 × 13 = 156 cells
```

A coordinate identifies a logical location inside that grid.

The physical infrastructure beneath that coordinate is controlled independently.

This allows the Fabric to reason about the database as a **logical data space** while continuously optimizing the physical implementation.

---

# Facet Fabric Responsibilities

Facet Fabric is responsible for:

* distributed topology awareness
* workload observation
* workload classification
* hotspot detection
* workload prediction
* anomaly detection
* capacity prediction
* topology optimization
* workload placement
* replication decisions
* migration decisions
* workload isolation
* workload locality
* capacity planning
* future autonomous database optimization

The Fabric is not the database itself.

FacetQL owns database state.

Facet Fabric owns the distributed intelligence and coordination required to optimize that state.

---

# Repository Structure

```text
facet-fabric/
│
├── Cargo.toml
├── README.md
│
└── crates/
    │
    ├── fabric-core/
    │   └── Fundamental Fabric types and contracts
    │
    ├── fabric-telemetry/
    │   └── FacetQL workload observations and measurements
    │
    ├── fabric-topology/
    │   └── Logical and physical database topology
    │
    ├── fabric-workload/
    │   └── Workload analysis and feature generation
    │
    ├── fabric-ml/
    │   └── Machine-learning and prediction infrastructure
    │
    ├── fabric-optimizer/
    │   └── Workload and topology optimization
    │
    └── fabric-protocol/
        └── FacetQL ↔ Facet Fabric communication contract
```

---

# Components

## fabric-core

The foundation of Facet Fabric.

Defines the shared vocabulary used by every Fabric subsystem.

Current concepts include:

* coordinates
* grids
* cells
* shards
* atoms
* workloads
* DBMS identities
* topology primitives

The core model remains independent of any particular ML implementation or physical infrastructure.

---

## fabric-telemetry

Collects and represents numerical observations from FacetQL.

Examples include:

```text
operations / second
reads / second
writes / second
read latency
write latency
CPU utilization
memory utilization
storage I/O
network ingress
network egress
queue depth
```

Telemetry is intentionally numerical.

It provides the raw observations from which higher-level workload intelligence can be constructed.

---

## fabric-topology

Maintains the Fabric's representation of the distributed FacetQL environment.

The topology separates:

### Logical location

```text
Shard
Coordinate
```

from:

### Physical location

```text
DBMS
Machine
Region
```

This allows Facet Fabric to reason about moving workloads without changing their logical identity.

---

## fabric-workload

Transforms raw telemetry into workload profiles.

A workload profile describes characteristics such as:

```text
read-heavy
write-heavy
mixed
high pressure
low pressure
high latency
high queue depth
resource intensive
```

This layer produces features consumed by the ML subsystem and optimizer.

---

## fabric-ml

Provides the machine-learning layer.

Facet Fabric does not require a language model to operate.

The fundamental problem is not language generation.

It is:

```text
measurement
+
prediction
+
optimization
+
control
```

The ML system therefore focuses on numerical problems such as:

* hotspot probability
* anomaly detection
* workload growth
* capacity demand
* failure probability
* traffic prediction
* locality prediction

The current implementation establishes the inference interfaces and baseline models.

Future versions will introduce trained models using historical FacetQL telemetry.

No external LLM is required by the architecture.

---

## fabric-optimizer

The optimizer converts workload intelligence into candidate topology decisions.

Examples include:

```text
NO ACTION

REPLICATE

MOVE

SPLIT

ISOLATE

COLOCATE
```

An optimization decision contains:

```text
coordinate
action
expected gain
estimated cost
confidence
```

The optimizer does not directly mutate the database.

It produces a decision or proposal that can later be validated and executed through the FacetQL/Fabric control path.

---

## fabric-protocol

`fabric-protocol` defines the communication boundary between FacetQL and Facet Fabric.

The protocol provides versioned messages for:

### FacetQL → Facet Fabric

```text
node registration
heartbeat
topology reports
workload observations
telemetry batches
```

### Facet Fabric → FacetQL

```text
acknowledgements
optimization proposals
topology instructions
```

The protocol separates the communication contract from internal implementation details.

---

# Distributed Architecture

Facet Fabric is designed to operate across many FacetQL database computers.

```text
                    FACET FABRIC
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
       FacetQL          FacetQL          FacetQL
       DBMS A           DBMS B           DBMS C
          │               │               │
        Shards           Shards           Shards
          │               │               │
        Grids             Grids            Grids
          │               │               │
     Coordinates      Coordinates     Coordinates
```

Individual FacetQL instances execute database operations while Facet Fabric maintains awareness of the larger distributed environment.

---

# The Moving Data Space

The coordinate model can be viewed as a logical map rather than a fixed physical disk layout.

```text
                 LOGICAL SPACE

        ┌──────────────────────────────┐
        │ 12 × 13 FacetQL Grid         │
        │                              │
        │ (0,0) (1,0) ... (11,0)       │
        │ (0,1) (1,1) ... (11,1)       │
        │       ...                    │
        │ (0,12) ...       (11,12)     │
        └──────────────────────────────┘
                       │
                       │ Facet Fabric
                       ▼
                PHYSICAL PLACEMENT

        DBMS A       DBMS B       DBMS C
        Region 1     Region 2     Region 3
```

A coordinate's logical identity remains stable while its physical placement can change.

This enables the Fabric to optimize the physical arrangement of workloads independently from application-level addressing.

---

# Machine Learning Architecture

Facet Fabric uses machine learning as one component of a larger deterministic control system.

```text
Telemetry
    ↓
Feature Extraction
    ↓
ML Prediction
    ↓
Optimization
    ↓
Decision
    ↓
Validation
    ↓
Execution
```

ML does not directly control the database.

Instead:

```text
ML
 ↓
Prediction

Optimizer
 ↓
Decision

Controller
 ↓
Execution
```

This separation allows machine-learning models to evolve independently from database execution semantics.

---

# Learning Loop

The long-term ML architecture is:

```text
FacetQL
   ↓
Telemetry
   ↓
Historical Dataset
   ↓
Training
   ↓
Model
   ↓
Inference
   ↓
Prediction
   ↓
Optimization
   ↓
FacetQL
   ↓
New Telemetry
   ↓
Improved Dataset
   ↓
Improved Model
```

This creates a continuous feedback loop.

The system becomes increasingly capable of recognizing workload behavior as more operational data is accumulated.

---

# Autonomous Optimization

The long-term objective is to move beyond reactive infrastructure.

Traditional systems often respond after a workload becomes problematic:

```text
Workload spike
      ↓
Resource exhaustion
      ↓
Alert
      ↓
Operator / orchestrator
      ↓
Remediation
```

Facet Fabric is designed to eventually operate proactively:

```text
Workload pattern
      ↓
Detection
      ↓
Prediction
      ↓
Expected future hotspot
      ↓
Optimization
      ↓
Topology adjustment
      ↓
Workload arrives
      ↓
System already prepared
```

The objective is to make the distributed database continuously adapt to changing workloads.

---

# Logical Identity vs Physical Placement

A central design principle is that logical identity must not depend on physical infrastructure.

For example:

```text
Logical:

Shard 42
Coordinate (7,3)
```

may physically exist at:

```text
DBMS-01
Region A
```

and later:

```text
DBMS-42
Region B
```

without changing the logical identity:

```text
Shard 42
Coordinate (7,3)
```

This abstraction allows the Fabric to move, replicate, isolate, or otherwise reorganize workloads while maintaining a stable logical data space.

---

# Design Principles

## 1. Native FacetQL Storage

FacetQL is the native DBMS.

The architecture is not intended to permanently depend on PostgreSQL as its storage engine.

The target architecture is:

```text
FCT
 ↓
FacetQL
 ↕
Facet Fabric
```

rather than:

```text
FCT
 ↓
PostgreSQL
 ↓
Facet Fabric
```

Facet Fabric is designed specifically around the FacetQL data model.

---

## 2. Logical Identity Is Independent of Physical Placement

Coordinates, shards, and logical data identities must not depend on the physical machine currently holding them.

---

## 3. ML Predicts; Optimization Decides

Machine learning produces predictions.

The optimizer evaluates possible actions.

A future controller executes validated decisions.

```text
ML
 ↓
Prediction

Optimizer
 ↓
Decision

Controller
 ↓
Execution
```

---

## 4. Deterministic Execution

Database mutations must have explicit semantics and validation.

Machine learning may recommend an action.

The database system determines whether that action is valid.

---

## 5. Local Autonomy, Global Awareness

Individual FacetQL nodes should remain capable of serving workloads independently while Facet Fabric maintains awareness of the larger distributed environment.

---

## 6. Workload-Aware Infrastructure

Physical resources should be arranged according to actual workload behavior rather than assuming every workload behaves the same way.

---

## 7. Continuous Optimization

The system should continuously measure the results of its decisions.

A topology decision is not considered successful merely because it was executed.

The Fabric must measure whether the decision actually improved the system.

---

# Current Status

## Foundation

* [x] Rust workspace
* [x] `fabric-core`
* [x] `fabric-telemetry`
* [x] `fabric-topology`
* [x] `fabric-workload`
* [x] `fabric-ml`
* [x] `fabric-optimizer`

## In Progress

* [ ] `fabric-protocol`
* [ ] FacetQL telemetry integration
* [ ] FacetQL node registration
* [ ] live topology synchronization
* [ ] real workload stream
* [ ] Facet Fabric runtime

## Future

* [ ] historical telemetry system
* [ ] production ML training pipeline
* [ ] trained hotspot models
* [ ] workload forecasting
* [ ] distributed optimization
* [ ] topology migration
* [ ] replication controller
* [ ] workload movement
* [ ] autonomous control loop
* [ ] large-scale simulation
* [ ] multi-region Fabric
* [ ] failure recovery
* [ ] adaptive optimization

---

# Relationship to the Facet Stack

The broader architecture is:

```text
                         FACET
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
             FCT        FacetQL       Facets
                          │
                          ▼
                    Facet Fabric
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
          Topology     Workload         ML
                                       │
                                       ▼
                                   Optimizer
```

### FCT

The application and data-definition language.

### FacetQL

The native DBMS and data computer.

### Facets

The application and interface component system.

### Facet Fabric

The distributed intelligence, topology, workload, machine-learning, and optimization layer operating the FacetQL environment.

---

# Long-Term Objective

Facet Fabric is being built as the distributed intelligence layer for a new database architecture.

The objective is not simply to build another database cluster.

The objective is to create a system in which:

```text
data
+
coordinates
+
workloads
+
topology
+
machine learning
+
optimization
+
control
```

form one continuously adapting distributed computer.

FacetQL provides the underlying database computer.

Facet Fabric provides the intelligence and coordination that allows a large population of FacetQL database computers to operate as a coordinated system.

The architecture is being developed from first principles in Rust, with FacetQL as the native database substrate and Facet Fabric as its distributed optimization and control layer.
