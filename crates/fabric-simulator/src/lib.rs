//! # fabric-simulator
//!
//! A synthetic FacetQL fleet the control loop can be run against without a
//! live cluster.
//!
//! README's Future list asks for "large-scale simulation". The reason it
//! matters is upstream of scale: a control plane's decisions can only be
//! judged against outcomes, and outcomes on a real fleet arrive slowly, once,
//! and never twice the same way. Here a decision, its execution, its failure
//! modes and its measured effect all happen in microseconds and happen
//! identically every time.
//!
//! ## Determinism is the feature
//!
//! Same seed, same run -- exactly. That holds because:
//!
//! * time is [`SimClock`], a counter of ticks from an explicit start instant.
//!   Nothing in this crate calls a wall clock.
//! * randomness is [`Rng`], a seeded SplitMix64 written out in full rather
//!   than pulled from a dependency whose algorithm could change under a
//!   `cargo update`. There is no way to seed it from entropy.
//! * every iteration that can affect output runs over an ordered collection
//!   (`BTreeMap` / `BTreeSet` / `Vec`), never a `HashMap`.
//! * the workload's daily shape is an exact triangle wave over integer time,
//!   not a call into the platform's `sin`.
//!
//! ## It speaks the real vocabulary
//!
//! The cluster is built from `DbmsNode`-shaped nodes, `Shard`s, `Coordinate`s
//! and `Workload`/`WorkloadClass` streams, and it emits
//! [`fabric_telemetry::Observation`] plus the `fabric-protocol` messages a
//! real instance would send. `WorkloadAnalyzer`, `WorkloadOptimizer`,
//! `FabricController` and `FabricRuntime` therefore run against it unmodified
//! -- which is the only way a result here says anything about production.
//!
//! [`SimExecutor`] implements `fabric_controller::PlacementExecutor`, so the
//! controller executes real, phased placement changes against the simulated
//! world and then measures them. It is a stand-in for `fabric-routing`,
//! `fabric-replication` and `fabric-migration`, and it is honest about what it
//! does not model: it declines `OptimizationAction::Split` rather than
//! pretending to perform one.
//!
//! ```text
//! Simulation::step  ->  Observation  ->  WorkloadAnalyzer  ->  Optimizer
//!        ^                                                        |
//!        |                                                   Decision
//!   SimExecutor  <-  FabricController  <-  validate  <------------+
//!        |                   |
//!        +----- effect ------+-----> measure -> OutcomeReport
//! ```

pub mod clock;
pub mod cluster;
pub mod executor;
pub mod fault;
pub mod rng;
pub mod simulation;
pub mod workload;

pub use clock::SimClock;

pub use cluster::{
    ClusterSpec,
    ClusterState,
    SimCell,
    SimNode,
};

pub use executor::SimExecutor;

pub use fault::Fault;

pub use rng::Rng;

pub use simulation::{
    Simulation,
    Tick,
};

pub use workload::{
    ClassProfile,
    class_profile,
    diurnal,
};
