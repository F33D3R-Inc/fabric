//! Things going wrong, on schedule.
//!
//! Faults are scheduled at a simulated instant rather than injected with a
//! probability per tick. Both are useful, but only the scheduled form is
//! reproducible enough to be worth arguing about: "the controller mishandles a
//! destination lost 4 seconds into a transfer" is a claim someone else can
//! check. Probabilistic injection is still deterministic here -- it draws from
//! the seeded stream -- but a schedule is what makes a failure a test case.

use fabric_controller::ActionTarget;
use fabric_core::DbmsId;
use serde::{Deserialize, Serialize};

/// Something that happens to the cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Fault {
    /// The process stops. It serves nothing and reports nothing.
    NodeLoss { node: DbmsId },

    NodeRecovery { node: DbmsId },

    /// A region keeps serving its own traffic but the Fabric stops hearing
    /// from it. The distinction matters: a partitioned node's data is still
    /// there, so a control plane that treats the partition as data loss and
    /// re-replicates everything has just doubled the damage.
    PartitionRegion { region: String },

    HealRegion { region: String },

    /// A cell's traffic multiplies for a while -- the hotspot the whole
    /// optimizer exists to notice.
    HotShard {
        target: ActionTarget,
        multiplier: f64,
        duration_ms: u64,
    },
}

impl Fault {
    pub fn label(&self) -> &'static str {
        match self {
            Self::NodeLoss { .. } => "node-loss",
            Self::NodeRecovery { .. } => "node-recovery",
            Self::PartitionRegion { .. } => "partition",
            Self::HealRegion { .. } => "heal",
            Self::HotShard { .. } => "hot-shard",
        }
    }
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeLoss { node } => {
                write!(f, "node '{}' lost", node.0)
            }

            Self::NodeRecovery { node } => {
                write!(f, "node '{}' recovered", node.0)
            }

            Self::PartitionRegion { region } => {
                write!(f, "region '{region}' partitioned from the Fabric")
            }

            Self::HealRegion { region } => {
                write!(f, "region '{region}' rejoined the Fabric")
            }

            Self::HotShard {
                target,
                multiplier,
                duration_ms,
            } => write!(
                f,
                "{target} running {multiplier:.1}x for {duration_ms}ms"
            ),
        }
    }
}
