//! Where the loop's evidence comes from.
//!
//! In production it is [`FacetqlTelemetry`]: `GET /stats` on every instance
//! that has a control-plane credential, differenced into rates by
//! [`fabric_facetql::TelemetryPoller`] and delivered as the ordinary
//! `fabric-protocol` messages the runtime already ingests. There is one
//! ingestion path and one scoring path, which is the whole reason the poller
//! emits protocol messages rather than a private shape.
//!
//! It is a trait for one honest reason, even now that it is no longer the
//! reason it was written for: **a source and a runtime are two different
//! things to be honest about.** `WorkloadProfile`'s pressure score is
//! computed from CPU, memory, queue depth and latency — see
//! `fabric_workload::profile` — and for a long time `GET /stats` reported
//! none of the four, so a fleet observed only through it had a pressure
//! score of exactly 0.0 and the optimizer never proposed anything; only a
//! source that invented those numbers (`fabric-simulator`) could drive the
//! loop end to end. FacetQL's `/stats` now reports all four (`runtime.
//! process`, `runtime.window`, `runtime.requests`; see
//! `fabric_facetql::wire::EngineStats` and `fabric_facetql::sample` for how
//! this crate turns them into honest, non-invented pressure — including the
//! rescaled queue term and the CPU counter differenced across this daemon's
//! own poll interval), so [`FacetqlTelemetry`] can make a real cell hot on
//! its own.
//!
//! The trait stays because the simulator is still a legitimate, separate
//! source — reproducible, seeded, driven without a real fleet — and because
//! an older FacetQL a target has not upgraded to yet still reports none of
//! the four. Making the source a parameter rather than a compile-time fork
//! keeps the daemon honest about which one is live: it is reported by name
//! on the admin port, and nothing anywhere invents a metric its source did
//! not measure.

use std::future::Future;
use std::pin::Pin;

use fabric_core::DbmsId;
use fabric_protocol::FabricMessage;

use fabric_facetql::poller::{PollOutcome, PollTarget, TelemetryPoller};

/// One sweep's worth of evidence, plus what went wrong getting it.
#[derive(Debug, Default)]
pub struct Sample {
    /// Protocol messages to feed the runtime. Telemetry only: liveness has
    /// exactly one authority in this daemon, and it is the prober.
    pub messages: Vec<FabricMessage>,

    /// Per-instance notes for the admin surface, in the source's own words.
    pub notes: Vec<(DbmsId, String)>,
}

/// A source of workload evidence.
///
/// Not `Send`: it is owned by the control-plane thread, alongside the runtime
/// it feeds, and a simulated fleet is `Rc`-based for the same determinism
/// reasons the runtime is. Sources are therefore *built* on that thread, from
/// a [`TelemetryFactory`].
pub trait TelemetrySource {
    /// What this source is, for the admin surface. An operator reading
    /// `fleet is quiet` needs to know whether the loop is watching a fleet or
    /// a simulation.
    fn describe(&self) -> String;

    fn sample(&mut self) -> Pin<Box<dyn Future<Output = Sample> + '_>>;
}

/// Builds a source on the control-plane thread.
pub type TelemetryFactory =
    Box<dyn FnOnce() -> Result<Box<dyn TelemetrySource>, String> + Send>;

/// The live source: FacetQL's own counters.
pub struct FacetqlTelemetry {
    poller: TelemetryPoller,
}

impl FacetqlTelemetry {
    pub fn new(targets: Vec<PollTarget>) -> Result<Self, String> {
        TelemetryPoller::new(targets)
            .map(|poller| Self { poller })
            .map_err(|error| error.to_string())
    }
}

impl TelemetrySource for FacetqlTelemetry {
    fn describe(&self) -> String {
        format!(
            "facetql /stats over {} instance(s)",
            self.poller.len()
        )
    }

    fn sample(&mut self) -> Pin<Box<dyn Future<Output = Sample> + '_>> {
        Box::pin(async move {
            let mut sample = Sample::default();

            for (id, outcome) in self.poller.poll_once().await {
                let note = match outcome {
                    PollOutcome::Sampled(batch) => {
                        sample.messages.push(FabricMessage::Telemetry(batch));
                        "sampled".to_string()
                    }

                    PollOutcome::Baseline => "baseline taken".to_string(),

                    PollOutcome::Skipped => {
                        "interval spanned a restart or no time".to_string()
                    }

                    /*
                     * A failed `/stats` read is a real fact and is reported
                     * here, but it is deliberately not liveness: the token
                     * could be wrong while the instance serves clients
                     * perfectly. Whether an instance is reachable is the
                     * prober's answer, and it has only one.
                     */
                    PollOutcome::Failed(error) => format!("failed: {error}"),
                };

                sample.notes.push((id, note));
            }

            sample
        })
    }
}
