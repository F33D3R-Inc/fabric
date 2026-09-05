//! A seeded random number generator with no entropy source.
//!
//! Deliberately hand-rolled rather than pulled from a crate: the simulator's
//! whole value is that a seed reproduces a run exactly, and that guarantee
//! must not depend on a dependency's version, its default features, or its
//! choice of algorithm changing under a `cargo update`. SplitMix64 is small
//! enough to read in one sitting and is fully specified by these twenty lines.
//!
//! There is no `Rng::from_entropy` and there never should be. If a
//! non-deterministic seed is wanted, the caller passes one in and owns the
//! consequences.

use serde::{Deserialize, Serialize};

/// SplitMix64.
///
/// Serialisable so that a run can be checkpointed and resumed on exactly the
/// same sequence -- a simulator whose state can be saved but whose randomness
/// cannot is not reproducible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rng {
    state: u64,
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

impl Rng {
    pub const fn new(seed: u64) -> Self {
        Self {
            state: seed ^ GOLDEN,
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN);

        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `[0.0, 1.0)`, using the 53 bits an `f64` can hold exactly,
    /// so the mapping is a pure integer shift and a multiply -- bit-identical
    /// on every platform.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
    }

    /// A value in `[low, high)`.
    pub fn range(&mut self, low: f64, high: f64) -> f64 {
        low + (high - low) * self.next_f64()
    }

    /// An index in `0..n`, or `0` when `n` is zero.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }

        (self.next_u64() % n as u64) as usize
    }

    pub fn chance(&mut self, probability: f64) -> bool {
        self.next_f64() < probability
    }

    /// An independent stream derived from this one.
    ///
    /// Used to give each part of the simulation its own sequence, so that
    /// adding a draw in one place does not shift every later draw everywhere
    /// else and silently change an unrelated run.
    pub fn fork(&mut self) -> Self {
        Self::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_gives_the_same_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);

        for _ in 0..1_000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge_immediately() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);

        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn floats_stay_in_the_unit_interval() {
        let mut rng = Rng::new(7);

        for _ in 0..10_000 {
            let value = rng.next_f64();
            assert!((0.0..1.0).contains(&value));
        }
    }
}
