//! The inputs to a failover decision — and nothing else.
//!
//! This module deliberately stops one step short of acting. It reports whether
//! the primary still looks like a primary, which copies could take over, what
//! each promotion would cost, and what is in the way. [`FailoverAssessment::promotion`]
//! *names* the operation that would be applied. It does not apply it, there is
//! no timer here, and calling `evaluate` twice changes nothing.
//!
//! That line is the README's layering: ML predicts, the optimizer decides, the
//! **controller executes**. A replica set that promoted itself would be a
//! second controller, racing the real one over the same shard.

use fabric_core::DbmsId;
use serde::{Deserialize, Serialize};

use crate::{
    replica::{ReplicaLag, ReplicaState},
    set::{ReplicaOperation, ReplicaSet},
};

/// The budgets a caller applies when judging a failover situation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverPolicy {
    /// How long the primary may go unheard-from before it is considered to
    /// need replacing.
    pub primary_silence_ms: u64,

    /// Unapplied writes above which a candidate is not worth promoting: those
    /// writes are lost if it takes over.
    pub max_promotion_pending_writes: u64,

    /// Apply lag above which a candidate is not worth promoting.
    pub max_promotion_apply_lag_ms: u64,

    /// How long a candidate may go unheard-from before its own health is too
    /// stale to promote on.
    pub max_candidate_silence_ms: u64,
}

impl Default for FailoverPolicy {
    fn default() -> Self {
        Self {
            primary_silence_ms: 30_000,
            max_promotion_pending_writes: 0,
            max_promotion_apply_lag_ms: 0,
            max_candidate_silence_ms: 30_000,
        }
    }
}

/// Why a copy cannot be promoted right now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Ineligibility {
    /// Not a complete, current copy.
    NotInSync { state: ReplicaState },

    /// Promoting it would drop writes it never applied.
    WouldLoseWrites { pending_writes: u64 },

    /// Too far behind in time.
    TooFarBehind { apply_lag_ms: u64 },

    /// Its health information is too old to promote on.
    HealthTooStale { silence_ms: u64 },

    /// Never reported at all, so nothing is known about it.
    NeverReported,
}

impl std::fmt::Display for Ineligibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInSync { state } => write!(f, "replica is {}", state.label()),

            Self::WouldLoseWrites { pending_writes } => {
                write!(f, "{pending_writes} unapplied writes would be lost")
            }

            Self::TooFarBehind { apply_lag_ms } => {
                write!(f, "{apply_lag_ms}ms behind the primary")
            }

            Self::HealthTooStale { silence_ms } => {
                write!(f, "no health report for {silence_ms}ms")
            }

            Self::NeverReported => write!(f, "no health report has ever arrived"),
        }
    }
}

/// A copy considered for promotion, with the evidence behind the verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionCandidate {
    pub node: DbmsId,
    pub region: String,
    pub state: ReplicaState,
    pub lag: ReplicaLag,
    pub silence_ms: Option<u64>,

    /// `None` when the copy is promotable; otherwise the first reason it is
    /// not.
    pub ineligibility: Option<Ineligibility>,
}

impl PromotionCandidate {
    pub fn is_eligible(&self) -> bool {
        self.ineligibility.is_none()
    }
}

/// What stands between the shard and a promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailoverBlocker {
    /// The primary is still serving. Nothing to fail over from.
    PrimaryStillServing,

    /// There is no primary and no copy at all: the shard is gone, not failed
    /// over. This needs a restore, not a promotion.
    SetIsEmpty,

    /// Copies exist, but none of them is a complete, current copy.
    NoInSyncSecondary,

    /// In-sync copies exist but every one of them fails the policy.
    EveryCandidateIneligible,
}

impl std::fmt::Display for FailoverBlocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrimaryStillServing => write!(f, "the primary is still serving writes"),
            Self::SetIsEmpty => write!(f, "the shard has no replicas to promote"),
            Self::NoInSyncSecondary => write!(f, "no secondary holds a current copy"),
            Self::EveryCandidateIneligible => {
                write!(f, "every in-sync secondary fails the failover policy")
            }
        }
    }
}

/// A read-only description of a shard's failover situation at `now_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverAssessment {
    pub shard_id: u64,
    pub assessed_at_ms: u64,

    pub primary: Option<DbmsId>,
    pub primary_state: Option<ReplicaState>,
    pub primary_silence_ms: Option<u64>,

    /// Whether the evidence says the primary can no longer serve writes.
    pub primary_needs_replacement: bool,

    /// Every secondary, best first. Ineligible ones are kept, with their
    /// reason, because "why can't we fail over" is the question that actually
    /// gets asked at 3am.
    pub candidates: Vec<PromotionCandidate>,

    pub blockers: Vec<FailoverBlocker>,
}

impl FailoverAssessment {
    /// Assemble the evidence. Pure: reads the set, writes nothing.
    pub fn evaluate(set: &ReplicaSet, now_ms: u64, policy: &FailoverPolicy) -> Self {
        let primary = set.primary();

        let primary_silence_ms = primary.and_then(|replica| replica.silence_ms(now_ms));

        /*
         * Three separate ways a primary stops being one, and all three are
         * evidence-based: it is gone, it is marked failed, or it has gone
         * quiet past the budget. Note that "never reported" is not treated as
         * failure here -- a freshly bootstrapped primary has no reports yet,
         * and declaring it dead would fail over a healthy new shard.
         */
        let primary_needs_replacement = match primary {
            None => true,

            Some(replica) => {
                !replica.state.is_fresh()
                    || primary_silence_ms
                        .is_some_and(|silence| silence > policy.primary_silence_ms)
            }
        };

        let mut candidates: Vec<PromotionCandidate> = set
            .secondaries()
            .map(|replica| {
                let silence_ms = replica.silence_ms(now_ms);

                PromotionCandidate {
                    node: replica.node.clone(),
                    region: replica.region.clone(),
                    state: replica.state,
                    lag: replica.lag,
                    silence_ms,
                    ineligibility: ineligibility(
                        replica.state,
                        replica.lag,
                        silence_ms,
                        policy,
                    ),
                }
            })
            .collect();

        candidates.sort_by(|left, right| {
            right
                .is_eligible()
                .cmp(&left.is_eligible())
                .then(left.lag.pending_writes.cmp(&right.lag.pending_writes))
                .then(left.lag.apply_lag_ms.cmp(&right.lag.apply_lag_ms))
                .then(left.node.0.cmp(&right.node.0))
        });

        let mut blockers = Vec::new();

        if !primary_needs_replacement {
            blockers.push(FailoverBlocker::PrimaryStillServing);
        } else if set.is_empty() {
            blockers.push(FailoverBlocker::SetIsEmpty);
        } else if !candidates.iter().any(|candidate| candidate.is_eligible()) {
            if candidates
                .iter()
                .any(|candidate| candidate.state == ReplicaState::InSync)
            {
                blockers.push(FailoverBlocker::EveryCandidateIneligible);
            } else {
                blockers.push(FailoverBlocker::NoInSyncSecondary);
            }
        }

        Self {
            shard_id: set.shard_id,
            assessed_at_ms: now_ms,
            primary: primary.map(|replica| replica.node.clone()),
            primary_state: primary.map(|replica| replica.state),
            primary_silence_ms,
            primary_needs_replacement,
            candidates,
            blockers,
        }
    }

    /// The best eligible candidate, or `None`.
    pub fn best_candidate(&self) -> Option<&PromotionCandidate> {
        self.candidates
            .iter()
            .find(|candidate| candidate.is_eligible())
    }

    /// The operation a controller would apply to resolve this situation, or
    /// `None` when there is nothing to do or nothing safe to do.
    ///
    /// This is a *suggestion object*. Nothing here applies it: the caller
    /// passes it to [`ReplicaSet::apply`](crate::ReplicaSet::apply) if, and
    /// only if, it decides to.
    pub fn promotion(&self) -> Option<ReplicaOperation> {
        if !self.primary_needs_replacement {
            return None;
        }

        self.best_candidate()
            .map(|candidate| ReplicaOperation::Promote {
                node: candidate.node.clone(),
            })
    }

    /// Whether the shard currently has no write target and no safe way to get
    /// one. The most urgent thing a controller can be told.
    pub fn is_stuck(&self) -> bool {
        self.primary_needs_replacement && self.promotion().is_none()
    }
}

fn ineligibility(
    state: ReplicaState,
    lag: ReplicaLag,
    silence_ms: Option<u64>,
    policy: &FailoverPolicy,
) -> Option<Ineligibility> {
    if state != ReplicaState::InSync {
        return Some(Ineligibility::NotInSync { state });
    }

    match silence_ms {
        None => return Some(Ineligibility::NeverReported),

        Some(silence) if silence > policy.max_candidate_silence_ms => {
            return Some(Ineligibility::HealthTooStale {
                silence_ms: silence,
            });
        }

        Some(_) => {}
    }

    if lag.pending_writes > policy.max_promotion_pending_writes {
        return Some(Ineligibility::WouldLoseWrites {
            pending_writes: lag.pending_writes,
        });
    }

    if lag.apply_lag_ms > policy.max_promotion_apply_lag_ms {
        return Some(Ineligibility::TooFarBehind {
            apply_lag_ms: lag.apply_lag_ms,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        placement::ReplicationFactor,
        replica::{LagThresholds, ReplicaReport},
    };

    fn healthy_pair() -> ReplicaSet {
        let mut set = ReplicaSet::bootstrap(
            3,
            ReplicationFactor::default(),
            DbmsId::new("db-a"),
            "phoenix",
        );

        let b = DbmsId::new("db-b");

        set.apply(ReplicaOperation::Add {
            node: b.clone(),
            region: "dallas".to_string(),
        })
        .unwrap();

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b.clone() }).unwrap();

        set.observe(
            &ReplicaReport::new(3, b, 1_000, ReplicaLag::NONE),
            &LagThresholds::default(),
        )
        .unwrap();

        set
    }

    #[test]
    fn a_serving_primary_produces_no_promotion() {
        let set = healthy_pair();

        let assessment = FailoverAssessment::evaluate(&set, 1_000, &FailoverPolicy::default());

        assert!(!assessment.primary_needs_replacement);
        assert_eq!(assessment.promotion(), None);
        assert_eq!(assessment.blockers, vec![FailoverBlocker::PrimaryStillServing]);
    }

    #[test]
    fn a_failed_primary_names_the_promotion_but_does_not_perform_it() {
        let mut set = healthy_pair();

        set.apply(ReplicaOperation::MarkFailed {
            node: DbmsId::new("db-a"),
        })
        .unwrap();

        let assessment = FailoverAssessment::evaluate(&set, 1_000, &FailoverPolicy::default());

        assert!(assessment.primary_needs_replacement);
        assert_eq!(
            assessment.promotion(),
            Some(ReplicaOperation::Promote {
                node: DbmsId::new("db-b")
            })
        );

        // The assessment changed nothing: db-a is still the (failed) primary
        // until a controller applies the operation.
        assert_eq!(set.primary().unwrap().node, DbmsId::new("db-a"));
        assert!(set.write_target().is_none());
    }

    #[test]
    fn a_lagging_candidate_is_refused_with_its_reason() {
        let mut set = healthy_pair();
        let b = DbmsId::new("db-b");

        set.observe(
            &ReplicaReport::new(3, b.clone(), 2_000, ReplicaLag::new(12, 400)),
            &LagThresholds::default(),
        )
        .unwrap();

        set.apply(ReplicaOperation::MarkFailed {
            node: DbmsId::new("db-a"),
        })
        .unwrap();

        // db-b is still "in-sync" by the lag thresholds, but the failover
        // policy refuses to lose 12 writes.
        let assessment = FailoverAssessment::evaluate(&set, 2_000, &FailoverPolicy::default());

        assert_eq!(assessment.promotion(), None);
        assert!(assessment.is_stuck());
        assert_eq!(
            assessment.candidates[0].ineligibility,
            Some(Ineligibility::WouldLoseWrites { pending_writes: 12 })
        );
        assert_eq!(
            assessment.blockers,
            vec![FailoverBlocker::EveryCandidateIneligible]
        );

        // A caller willing to lose those writes is free to say so; the crate
        // does not make that choice for it.
        let lenient = FailoverPolicy {
            max_promotion_pending_writes: 100,
            max_promotion_apply_lag_ms: 1_000,
            ..FailoverPolicy::default()
        };

        let assessment = FailoverAssessment::evaluate(&set, 2_000, &lenient);
        assert_eq!(
            assessment.promotion(),
            Some(ReplicaOperation::Promote { node: b })
        );
    }

    #[test]
    fn a_silent_primary_needs_replacement_even_while_marked_in_sync() {
        let mut set = healthy_pair();

        set.observe(
            &ReplicaReport::new(3, DbmsId::new("db-a"), 1_000, ReplicaLag::NONE),
            &LagThresholds::default(),
        )
        .unwrap();

        let policy = FailoverPolicy::default();
        let now = 1_000 + policy.primary_silence_ms + 1;

        let assessment = FailoverAssessment::evaluate(&set, now, &policy);

        assert!(assessment.primary_needs_replacement);
        assert_eq!(assessment.primary_state, Some(ReplicaState::InSync));
        // db-b has also gone quiet by then, so promoting on its stale health
        // is refused rather than guessed at.
        assert_eq!(
            assessment.candidates[0].ineligibility,
            Some(Ineligibility::HealthTooStale {
                silence_ms: now - 1_000
            })
        );
    }
}
