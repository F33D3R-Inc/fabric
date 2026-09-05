//! Synthetic workload streams, built from the classes `fabric-core` already
//! defines.
//!
//! The numbers here are the simulator's only genuinely invented content, so
//! they are kept in one table rather than scattered through the tick loop. The
//! point is not that they are the true behaviour of a Media workload -- it is
//! that the classes differ from each other in the ways the optimizer reasons
//! about (read/write mix, latency, data volume), so that a policy which only
//! works for read-heavy traffic visibly fails here.

use fabric_core::WorkloadClass;
use serde::{Deserialize, Serialize};

/// The behavioural signature of one workload class.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ClassProfile {
    /// Operations per second a single cell of this class generates at rest.
    pub base_ops: f64,

    /// Share of those operations that are reads.
    pub read_ratio: f64,

    /// Service latency with an idle host, in microseconds.
    pub read_latency_us: f64,
    pub write_latency_us: f64,

    /// Bytes of resident data per operation per second -- what determines how
    /// long this cell takes to move.
    pub bytes_per_op: u64,
}

impl ClassProfile {
    pub fn write_ratio(&self) -> f64 {
        1.0 - self.read_ratio
    }
}

/// The signature of a class.
pub const fn class_profile(class: WorkloadClass) -> ClassProfile {
    match class {
        WorkloadClass::ReadHeavy => ClassProfile {
            base_ops: 800.0,
            read_ratio: 0.92,
            read_latency_us: 400.0,
            write_latency_us: 1_200.0,
            bytes_per_op: 512,
        },

        WorkloadClass::WriteHeavy => ClassProfile {
            base_ops: 400.0,
            read_ratio: 0.15,
            read_latency_us: 900.0,
            write_latency_us: 2_500.0,
            bytes_per_op: 1_024,
        },

        WorkloadClass::Mixed => ClassProfile {
            base_ops: 500.0,
            read_ratio: 0.55,
            read_latency_us: 600.0,
            write_latency_us: 1_500.0,
            bytes_per_op: 768,
        },

        WorkloadClass::EventHeavy => ClassProfile {
            base_ops: 1_200.0,
            read_ratio: 0.35,
            read_latency_us: 300.0,
            write_latency_us: 800.0,
            bytes_per_op: 256,
        },

        WorkloadClass::Media => ClassProfile {
            base_ops: 150.0,
            read_ratio: 0.85,
            read_latency_us: 2_500.0,
            write_latency_us: 9_000.0,
            bytes_per_op: 65_536,
        },

        WorkloadClass::Realtime => ClassProfile {
            base_ops: 2_000.0,
            read_ratio: 0.60,
            read_latency_us: 150.0,
            write_latency_us: 400.0,
            bytes_per_op: 128,
        },

        WorkloadClass::Unknown => ClassProfile {
            base_ops: 100.0,
            read_ratio: 0.50,
            read_latency_us: 1_000.0,
            write_latency_us: 1_000.0,
            bytes_per_op: 512,
        },
    }
}

/// A daily-shaped multiplier in `[0.6, 1.4]`.
///
/// Computed as an exact triangle wave over integer time rather than with a
/// trigonometric function: `sin` is not guaranteed bit-identical across
/// platforms or libm versions, and a simulator whose "same seed, same run"
/// promise quietly depends on the host's math library has no promise at all.
pub fn diurnal(now_ms: u64, period_ms: u64, offset_ms: u64) -> f64 {
    let period = period_ms.max(1);
    let position = now_ms.wrapping_add(offset_ms) % period;

    let phase = position as f64 / period as f64;
    let triangle = 1.0 - (2.0 * phase - 1.0).abs();

    0.6 + 0.8 * triangle
}
