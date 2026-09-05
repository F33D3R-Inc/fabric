//! Simulated time.
//!
//! The simulator never reads a wall clock. Every timestamp in a run -- in
//! observations, heartbeats, execution records and outcome measurements --
//! descends from [`SimClock::start_ms`] plus a whole number of ticks, so two
//! runs of the same spec produce byte-identical timelines.

use serde::{Deserialize, Serialize};

/// A monotonic, explicitly advanced clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimClock {
    start_ms: u64,
    tick_ms: u64,
    ticks: u64,
}

impl SimClock {
    /// `tick_ms` is clamped to at least one: a zero-length tick would make
    /// every timestamp identical and every duration zero, which quietly
    /// disables every timeout in the controller.
    pub fn new(start_ms: u64, tick_ms: u64) -> Self {
        Self {
            start_ms,
            tick_ms: tick_ms.max(1),
            ticks: 0,
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.start_ms + self.ticks * self.tick_ms
    }

    pub fn start_ms(&self) -> u64 {
        self.start_ms
    }

    pub fn tick_ms(&self) -> u64 {
        self.tick_ms
    }

    pub fn ticks(&self) -> u64 {
        self.ticks
    }

    /// Move one tick forward and return the new instant.
    pub fn advance(&mut self) -> u64 {
        self.ticks += 1;
        self.now_ms()
    }
}
