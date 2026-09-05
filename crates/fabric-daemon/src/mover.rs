//! The daemon's own data mover, and the supervisor that runs one per move.
//!
//! Until this existed, `fabricd` drove a migration's phases correctly around a
//! copy that nothing performed: `POST /actions/{id}/transfer` was the only way
//! progress could ever be reported, so a fleet with no external mover watched
//! every transfer time out and roll back. The state machine was right and the
//! hands were missing.
//!
//! [`fabric_facetql::mover::CellMover`] is the hands. This module is what puts
//! them on the control loop:
//!
//! * [`MoverSupervisor::ensure`] starts one task per relocating action, the
//!   first time the control loop sees that action reach a copying phase, and
//!   never starts a second for the same action.
//! * the task reports through **the same channel `POST /actions/{id}/transfer`
//!   uses** — [`ControlRequest::Copy`] alongside `ControlRequest::Transfer` —
//!   so there is one seam into the runtime, not two, and the admin route keeps
//!   working unchanged for a mover run out of band.
//! * nothing here touches the runtime. The control thread is still the only
//!   thing that may, which is what makes `FabricRuntime`'s `Rc<RefCell<_>>`
//!   safe.
//!
//! # Why the mover reports rather than acts
//!
//! The mover knows whether bytes landed; it does not know whether a phase may
//! advance, and it must not. Every decision — whether the gap is small enough
//! to fence, whether authority may move, whether to roll back — stays in the
//! mechanisms that own those invariants. The mover contributes exactly three
//! facts: how much it has copied, how far behind the destination is, and
//! whether the copy checks out against its source.

use std::collections::BTreeMap;
use std::time::Duration;

use fabric_controller::ActionId;
use fabric_core::DbmsId;
use fabric_facetql::mover::{CellMover, CellScope, MoverConfig, MoverReport};
use fabric_facetql::{FacetqlClient, FacetqlEndpoint};
use fabric_runtime::CopyVerdict;

use crate::control::ControlRequest;
use crate::now_ms;

/// How long the mover waits between catch-up passes once the bulk copy is
/// done.
///
/// Short, because this interval is pure latency inside the cutover: the fence
/// is up while the last passes run, so every millisecond here is a millisecond
/// the cell refuses writes. Not zero, because a mover that spun would hammer
/// both instances with `multiget`s for a dirty set that is usually empty.
const CATCH_UP_INTERVAL: Duration = Duration::from_millis(50);

/// One running copy.
#[derive(Debug)]
struct Running {
    destination: DbmsId,
    task: tokio::task::JoinHandle<()>,
}

/// Why a copy could not even be started.
///
/// Reported, never swallowed: a migration whose mover never started is one
/// whose transfer will time out, and an operator reading `last_error` deserves
/// to know it was a missing credential rather than a slow copy.
#[derive(Debug, Clone)]
pub struct MoverRefusal {
    pub id: u64,
    pub reason: String,
}

/// Keeps exactly one mover per in-flight relocation.
#[derive(Debug, Default)]
pub struct MoverSupervisor {
    running: BTreeMap<ActionId, Running>,
    refusals: BTreeMap<ActionId, MoverRefusal>,
    config: MoverConfig,
}

impl MoverSupervisor {
    pub fn new() -> Self {
        Self::default()
    }

    /// What this supervisor is currently copying, for the status page.
    pub fn active(&self) -> Vec<(u64, String)> {
        self.running
            .iter()
            .map(|(id, running)| (id.0, running.destination.0.clone()))
            .collect()
    }

    /// Copies this daemon declined to start, and why.
    pub fn refusals(&self) -> Vec<MoverRefusal> {
        self.refusals.values().cloned().collect()
    }

    /// Start the copy for `id` if it is not already running.
    ///
    /// Idempotent by design: the control loop calls this every cycle for every
    /// relocating action, and the answer for one already being copied is to do
    /// nothing at all. A second mover on one target would have two change
    /// feeds, two sequence spaces and two opinions about how far behind the
    /// destination is.
    #[allow(clippy::too_many_arguments)]
    pub fn ensure(
        &mut self,
        id: ActionId,
        source: &FacetqlEndpoint,
        destination: &FacetqlEndpoint,
        scope: CellScope,
        streaming: reqwest::Client,
        requests: tokio::sync::mpsc::UnboundedSender<ControlRequest>,
    ) {
        if self.running.contains_key(&id) {
            return;
        }

        let destination_id = destination.dbms_id().clone();

        let mover = CellMover::new(
            FacetqlClient::new(source.clone()),
            FacetqlClient::new(destination.clone()),
            scope,
            self.config,
            streaming,
        );

        let task = tokio::spawn(copy(id, mover, requests));

        self.running.insert(
            id,
            Running {
                destination: destination_id,
                task,
            },
        );

        self.refusals.remove(&id);
    }

    /// Record that no copy could be started for `id`.
    pub fn refuse(&mut self, id: ActionId, reason: impl Into<String>) {
        if self.running.contains_key(&id) {
            return;
        }

        self.refusals.insert(
            id,
            MoverRefusal {
                id: id.0,
                reason: reason.into(),
            },
        );
    }

    /// Stop the copy for `id` and forget it.
    ///
    /// Aborting the task is safe at any instant precisely because the mover
    /// writes nothing but upserts at the source's own addresses and keeps no
    /// checkpoint: a copy stopped halfway leaves complete rows on the
    /// destination and no state a later attempt could mistake for progress.
    pub fn stop(&mut self, id: ActionId) {
        if let Some(running) = self.running.remove(&id) {
            running.task.abort();
        }
    }

    /// Stop every copy. Called when the control loop is shutting down.
    pub fn stop_all(&mut self) {
        for (_, running) in std::mem::take(&mut self.running) {
            running.task.abort();
        }
    }

    /// Drop the bookkeeping for actions that have concluded.
    pub fn retain(&mut self, live: impl Fn(ActionId) -> bool) {
        let finished: Vec<ActionId> = self
            .running
            .keys()
            .copied()
            .filter(|id| !live(*id))
            .collect();

        for id in finished {
            self.stop(id);
        }

        self.refusals.retain(|id, _| live(*id));
    }
}

/// One copy, from subscription to cutover.
///
/// The sequence is the whole correctness argument and its order is
/// load-bearing:
///
/// 1. **subscribe first.** An event seen before the snapshot began costs one
///    redundant re-read; an event missed after it began is a lost write, and
///    FacetQL offers no way to discover one after the fact.
/// 2. **snapshot**, reporting real bytes after every committed batch.
/// 3. **catch up and check, repeatedly**, until the control loop says the
///    action is over. The verdict is re-reported every pass rather than
///    latched, because the question cutover asks is whether the destination is
///    good *now*: a copy that passed a check and then took a write nobody
///    reconciled must stop being verified.
///
/// Every failure ends the same way — a `Failed` verdict on the wire — because
/// the mechanism turns that into a phase failure and the controller rolls the
/// migration back while the source is still authoritative. A mover that
/// vanished silently would leave the phase timeout to do it, minutes later,
/// with no reason attached.
async fn copy(
    id: ActionId,
    mut mover: CellMover,
    requests: tokio::sync::mpsc::UnboundedSender<ControlRequest>,
) {
    let feed = match mover.subscribe().await {
        Ok(feed) => feed,

        Err(error) => {
            let _ = requests.send(ControlRequest::Copy {
                id,
                report: failed(format!(
                    "could not subscribe to the source's change feed: {error}"
                )),
                reply: None,
            });

            return;
        }
    };

    let snapshot = {
        let outbox = requests.clone();
        let seen = &feed;

        mover
            .snapshot(seen, |rows, bytes| {
                let (observed, applied) = seen.counts();

                let _ = outbox.send(ControlRequest::Copy {
                    id,
                    report: MoverReport {
                        rows_copied: rows,
                        bytes_copied: bytes,
                        resident_bytes: None,
                        snapshot_complete: false,
                        observed_writes: observed,
                        applied_writes: applied,
                        verdict: CopyVerdict::Pending,
                    },
                    reply: None,
                });
            })
            .await
    };

    if let Err(error) = snapshot {
        let _ = requests.send(ControlRequest::Copy {
            id,
            report: failed(format!("the bulk copy did not finish: {error}")),
            reply: None,
        });

        return;
    }

    loop {
        let verdict = match mover.catch_up(&feed).await {
            Ok(_) => mover.verify(&feed, now_ms()).await,

            Err(error) => CopyVerdict::Failed {
                reason: format!("catch-up could not be applied: {error}"),
                at_ms: now_ms(),
            },
        };

        let fatal = matches!(verdict, CopyVerdict::Failed { .. });
        let report = mover.report(&feed, verdict);

        let (reply, answer) = tokio::sync::oneshot::channel();

        if requests
            .send(ControlRequest::Copy {
                id,
                report,
                reply: Some(reply),
            })
            .is_err()
        {
            return;
        }

        /*
         * The control loop's answer is also the mover's stop signal: it is the
         * only thing that knows whether the action still exists, and a mover
         * that kept copying into a migration the controller had already rolled
         * back would be writing to a destination nobody is going to use.
         */
        match answer.await {
            Ok(Ok(_)) => {}
            _ => return,
        }

        if fatal {
            return;
        }

        tokio::time::sleep(CATCH_UP_INTERVAL).await;
    }
}

fn failed(reason: String) -> MoverReport {
    MoverReport {
        rows_copied: 0,
        bytes_copied: 0,
        resident_bytes: None,
        snapshot_complete: false,
        observed_writes: 0,
        applied_writes: 0,
        verdict: CopyVerdict::Failed {
            reason,
            at_ms: now_ms(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_names_the_action_it_is_about() {
        let mut supervisor = MoverSupervisor::new();
        supervisor.refuse(ActionId(7), "no credential for 'db-b'");

        let refusals = supervisor.refusals();

        assert_eq!(refusals.len(), 1);
        assert_eq!(refusals[0].id, 7);
        assert!(refusals[0].reason.contains("db-b"));
    }

    /// A concluded action's bookkeeping must not outlive it, or a retry of the
    /// same target would read a stale refusal as the current state.
    #[test]
    fn concluded_actions_are_forgotten() {
        let mut supervisor = MoverSupervisor::new();
        supervisor.refuse(ActionId(1), "gone");
        supervisor.refuse(ActionId(2), "still here");

        supervisor.retain(|id| id == ActionId(2));

        let refusals = supervisor.refusals();

        assert_eq!(refusals.len(), 1);
        assert_eq!(refusals[0].id, 2);
    }
}
