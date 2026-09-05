//! Whether an instance is actually there, asked directly rather than inferred.
//!
//! `GET /` is FacetQL's only unauthenticated route — the front door classifies
//! it as a fleet-wide probe for exactly that reason — so the control plane can
//! ask every backend whether it is serving without holding a credential for
//! it. The prober asks on an interval and hands the answers to the fleet
//! inventory, which is the one thing in the runtime allowed to judge liveness;
//! that judgement reaches `RoutingTable::set_availability` through
//! `PlacementFabric::observe_nodes`, and from there the front door stops
//! resolving routes onto a dead instance.
//!
//! # Three answers, not two
//!
//! * **Serving** — a 2xx. A heartbeat, healthy.
//! * **Answering badly** — any other status. The instance is reachable and is
//!   telling us something is wrong, which is a heartbeat that reports itself
//!   unhealthy: [`NodeHealth::Degraded`](fabric_runtime::NodeHealth), and
//!   still routable. It may hold the only copy of the data, and refusing to
//!   route to it would turn a degraded shard into a lost one.
//! * **Unreachable** — the connection failed or timed out. **No heartbeat is
//!   sent at all.** This is the important one: silence is what the inventory
//!   measures unreachability with, and an "unhealthy" heartbeat would reset
//!   the silence clock on every probe, leaving a permanently dead instance
//!   permanently `Degraded` and permanently routable. The daemon's own clock
//!   tick is what then carries it past the silence budget.

use std::time::Duration;

use fabric_core::DbmsId;

/// One instance's probe address.
#[derive(Debug, Clone)]
pub struct ProbeTarget {
    pub id: DbmsId,
    pub url: String,
}

/// What one probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    /// Answered `GET /` with a 2xx.
    Serving { status: u16 },

    /// Answered, but not with success.
    Answering { status: u16 },

    /// Did not answer.
    Unreachable { error: String },
}

impl Probe {
    /// Whether this probe is evidence the instance is there at all.
    ///
    /// `false` means no heartbeat is filed — see the module docs.
    pub fn reached(&self) -> bool {
        !matches!(self, Self::Unreachable { .. })
    }

    /// Whether the instance reported itself well. Only meaningful when it was
    /// reached.
    pub fn healthy(&self) -> bool {
        matches!(self, Self::Serving { .. })
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Serving { status } => format!("serving ({status})"),
            Self::Answering { status } => format!("answering {status}"),
            Self::Unreachable { error } => format!("unreachable: {error}"),
        }
    }
}

/// Probes every backend on an interval.
pub struct LivenessProber {
    http: reqwest::Client,
    targets: Vec<ProbeTarget>,
}

impl LivenessProber {
    pub fn new(targets: Vec<ProbeTarget>, timeout: Duration) -> Result<Self, String> {
        /*
         * A timeout is not optional. A sweep that hangs on one wedged instance
         * is a control plane that cannot react to the wedged instance -- and
         * the timeout must be shorter than the silence budget, or an instance
         * could never be probed often enough to stay alive. The configuration
         * enforces the second half; this enforces the first.
         */
        let http = reqwest::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|error| format!("could not build the probe client: {error}"))?;

        Ok(Self { http, targets })
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Probe every instance once, concurrently.
    ///
    /// Concurrently rather than in sequence because this is liveness, not a
    /// rate: a sample's spacing does not have to be even, and a fleet of ten
    /// instances behind one timed-out probe would otherwise take ten timeouts
    /// to sweep.
    pub async fn sweep(&self) -> Vec<(DbmsId, Probe)> {
        let probes = self.targets.iter().map(|target| async move {
            (target.id.clone(), self.probe(target).await)
        });

        futures_util::future::join_all(probes).await
    }

    async fn probe(&self, target: &ProbeTarget) -> Probe {
        match self.http.get(format!("{}/", target.url)).send().await {
            Ok(response) => {
                let status = response.status().as_u16();

                if response.status().is_success() {
                    Probe::Serving { status }
                } else {
                    Probe::Answering { status }
                }
            }

            Err(error) => Probe::Unreachable {
                error: transport_reason(&error),
            },
        }
    }
}

/// The reason a probe failed, without the URL.
///
/// `reqwest`'s own `Display` embeds the request URL, and a probe URL is fleet
/// topology: it belongs in the admin surface's `url` field, which is
/// authenticated, and not repeated into every error string that might reach a
/// log.
fn transport_reason(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        return "timed out".to_string();
    }

    if error.is_connect() {
        return "connection refused or unresolvable".to_string();
    }

    if error.is_request() {
        return "the request could not be sent".to_string();
    }

    "the probe failed".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The distinction the whole liveness feed rests on: a reachable instance
    /// answering badly is still routable, and an unreachable one files no
    /// heartbeat at all so that its silence can grow.
    #[test]
    fn only_a_reachable_instance_files_a_heartbeat() {
        let serving = Probe::Serving { status: 200 };
        let answering = Probe::Answering { status: 503 };
        let gone = Probe::Unreachable {
            error: "timed out".to_string(),
        };

        assert!(serving.reached() && serving.healthy());
        assert!(answering.reached() && !answering.healthy());
        assert!(!gone.reached() && !gone.healthy());
    }

    #[tokio::test]
    async fn nothing_listening_is_unreachable_rather_than_unhealthy() {
        // Port 1 on loopback: refused immediately, without a timeout wait.
        let prober = LivenessProber::new(
            vec![ProbeTarget {
                id: DbmsId::new("db-dead"),
                url: "http://127.0.0.1:1".to_string(),
            }],
            Duration::from_millis(500),
        )
        .unwrap();

        let swept = prober.sweep().await;

        assert_eq!(swept.len(), 1);
        assert!(!swept[0].1.reached(), "{:?}", swept[0].1);
        assert!(swept[0].1.describe().starts_with("unreachable"));
    }
}
