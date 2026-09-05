//! Turning two `GET /stats` reads into the rates Fabric's model runs on.
//!
//! FacetQL reports **monotonic totals** (`reads_total`, `writes_total`,
//! `runtime.process.cpu_seconds_total`, per-cell `reads`/`writes`/bytes),
//! not rates — the engine is the only thing that can count its own
//! operations, and a counter is the honest primitive to expose. The rate is
//! derived here, by the consumer, from the difference between two samples
//! over the elapsed time between them.
//!
//! Four things this deliberately does not do:
//!
//! * **It does not fake what FacetQL cannot measure.** Every figure below is
//!   either derived from a real counter/gauge or left at the documented
//!   "absent reads as low pressure" default — never invented. See each
//!   helper's doc comment for exactly which case it is.
//! * **It does not treat a closed-but-empty window the same as one that
//!   never closed**, except in the one place that is actually
//!   indistinguishable from the outside: both report `None` on the wire
//!   (`duration_ms == 0` for the former, an empty bucket count for the
//!   latter), and both honestly mean "no measurement" to a consumer — so
//!   both are mapped to the same "absent" fallback, never to a fabricated
//!   zero utilization.
//! * **It does not report across a restart.** `reads_total`/`writes_total`
//!   are process-lifetime counters that return to zero when the server
//!   restarts. A counter that went backwards means the interval spans a
//!   restart, so the interval is dropped and the new sample becomes the
//!   baseline — reporting `(0 - 5_000_000) / dt` as a rate would be a
//!   fabricated spike. The per-cell counters are equally process-lifetime,
//!   and a whole-process restart is already caught by the top-level check,
//!   so no separate restart detection is needed per cell.
//! * **It does not trust the wall clock for elapsed time.** Elapsed is
//!   measured with [`Instant`], which cannot jump when NTP steps the clock;
//!   the epoch-millisecond timestamp is carried separately, for labelling.
//!
//! # CPU: the monotonic counter, not the server's window
//!
//! `runtime.process.cpu_seconds_total` (monotonic, differenced here) and
//! `runtime.window.cpu_utilization` (a rate FacetQL already computed, over a
//! window FacetQL chose) both describe the same fact. This module uses the
//! former and ignores the latter, because **the daemon knows its own poll
//! cadence and the server does not**: FacetQL's window closes on whatever
//! interval a `GET /stats` happens to arrive on, which is shorter than the
//! daemon's own poll interval whenever more than one consumer polls the same
//! instance (see FacetQL's own `Window` docs), and longer than it whenever a
//! poll is missed. Differencing the monotonic counter across exactly the
//! interval the daemon itself measured gives a CPU utilization scoped to the
//! same window as every other rate in this module, rather than one scoped to
//! a window nobody here chose.
//!
//! # Queue depth: rescaled to the units the pressure model already uses
//!
//! `fabric_workload::profile::calculate_pressure` divides `queue_depth` by
//! `10_000` to fold it into a 0..1 pressure term — a scale this codebase
//! already treats as the abstract unit of "queue pressure" (every synthetic
//! test fixture in this workspace that wants to simulate a saturated cell
//! sets `queue_depth: 30_000`, not a literal request count). FacetQL's own
//! `runtime.requests.write_queue_depth` cannot possibly reach that scale: it
//! serializes mutations on one mutex and caps in-flight requests at
//! `max_concurrent` (512 in the reference configuration), so a raw pass-through
//! would repeat exactly the bug this module exists to fix, just with a
//! smaller ceiling.
//!
//! This module instead projects `in_flight / max_concurrent` — the server's
//! own admission-control saturation, already bounded to `0..1` with no
//! invented denominator — onto the `0..10_000` scale `calculate_pressure`
//! expects. Three reasons this ratio over the alternatives FacetQL also
//! reports:
//!
//! * it needs no differencing and no baseline: it is an instantaneous
//!   fact of the current sample, so it is available on the very first poll;
//! * it is systemic (every request counts against `max_concurrent`, not only
//!   writes), whereas `write_queue_depth` is undercounting by construction —
//!   an instant gauge of writers alone, which the server's own docs note
//!   "can miss a burst between samples";
//! * it needs no invented threshold at all: `1.0` means "literally at the
//!   concurrency cap", a physically meaningful saturation point, whereas
//!   `write_queue_contended_total` (a monotonic contention counter) would
//!   need its own guessed contended-events-per-second ceiling to turn into a
//!   0..1 figure — the exact kind of arbitrary constant this rescale is
//!   meant to retire.
//!
//! A fully saturated engine (`in_flight == max_concurrent`) therefore
//! produces `queue_depth == 10_000`, which is exactly the value
//! `calculate_pressure` and [`fabric_telemetry::WorkloadMetrics::is_queue_hot`]
//! already treat as maximum queue pressure.

use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fabric_telemetry::{CellWorkloadMetrics, WorkloadMetrics};

use crate::wire::{CellStats, EngineStats};

/// One `GET /stats` read, with the clock readings needed to difference it
/// against the next one.
#[derive(Debug, Clone)]
pub struct StatsSample {
    /// When the sample was taken, in epoch milliseconds — for labelling the
    /// observation, not for measuring elapsed time.
    pub taken_at_ms: u64,
    /// Monotonic reading, for measuring elapsed time.
    pub taken_at: Instant,
    pub reads_total: u64,
    pub writes_total: u64,
    pub node_count: u64,
    pub edge_count: u64,

    /// `runtime.process.cpu_seconds_total` — monotonic; `None` when the
    /// platform would not report it.
    pub cpu_seconds_total: Option<f64>,
    /// `runtime.process.cpu_cores` — the divisor CPU seconds are turned into
    /// a `0..1` utilization by.
    pub cpu_cores: Option<u64>,
    /// `runtime.process.memory_utilization` — already a `0..1` ratio, so
    /// this is read from the newer of two samples rather than differenced.
    pub memory_utilization: Option<f64>,

    /// `runtime.window.read_latency.p99_us` / `write_latency.p99_us` — the
    /// tail of the most recent closed window. `None` when no window has
    /// closed, or the window closed with zero requests of that class.
    pub read_latency_p99_us: Option<u64>,
    pub write_latency_p99_us: Option<u64>,

    /// `runtime.requests.in_flight` / `max_concurrent` — instantaneous
    /// admission-control gauges, read from the newer of two samples.
    pub in_flight: u64,
    pub max_concurrent: u64,

    /// `cells.cells` — the per-coordinate attribution, monotonic per
    /// coordinate for the life of the process, exactly like `reads_total`.
    pub cells: Vec<CellStats>,
    /// `cells.overflow_reads` / `overflow_writes` — non-zero means `cells`
    /// above was, at the time of this sample, missing some traffic.
    pub overflow_reads: u64,
    pub overflow_writes: u64,
}

impl StatsSample {
    /// Take a sample from a `/stats` response, stamping it with both clocks.
    pub fn take(stats: &EngineStats) -> Self {
        Self {
            taken_at_ms: now_ms(),
            taken_at: Instant::now(),
            reads_total: stats.reads_total,
            writes_total: stats.writes_total,
            node_count: stats.node_count,
            edge_count: stats.edge_count,

            cpu_seconds_total: stats.runtime.process.cpu_seconds_total,
            cpu_cores: stats.runtime.process.cpu_cores,
            memory_utilization: stats.runtime.process.memory_utilization,

            read_latency_p99_us: stats.runtime.window.read_latency.p99_us,
            write_latency_p99_us: stats.runtime.window.write_latency.p99_us,

            in_flight: stats.runtime.requests.in_flight,
            max_concurrent: stats.runtime.requests.max_concurrent,

            cells: stats.cells.cells.clone(),
            overflow_reads: stats.cells.overflow_reads,
            overflow_writes: stats.cells.overflow_writes,
        }
    }

    /// The metrics implied by the interval from `self` to `next`, or `None`
    /// when the interval says nothing.
    ///
    /// `None` means one of:
    ///
    /// * no measurable time passed (two samples inside the same instant —
    ///   dividing by that produces an infinity, not a rate); or
    /// * a counter went backwards, i.e. the server restarted between the two
    ///   samples, so there is no interval to speak of.
    ///
    /// Either way the caller should adopt `next` as the new baseline and
    /// report nothing for this round, rather than emit a number it made up.
    pub fn difference(&self, next: &StatsSample) -> Option<WorkloadMetrics> {
        if next.reads_total < self.reads_total || next.writes_total < self.writes_total {
            return None;
        }

        let elapsed = next.taken_at.saturating_duration_since(self.taken_at);
        let seconds = elapsed.as_secs_f64();

        // `Instant` differences are non-negative and finite, so this is
        // exactly the "two samples landed in the same instant" case — and
        // dividing by it would produce an infinity, not a rate.
        if seconds <= 0.0 {
            return None;
        }

        let reads_per_second = (next.reads_total - self.reads_total) as f64 / seconds;
        let writes_per_second = (next.writes_total - self.writes_total) as f64 / seconds;

        let (cell_breakdown, cell_breakdown_partial) = cell_breakdown(self, next, seconds);

        Some(WorkloadMetrics {
            operations_per_second: reads_per_second + writes_per_second,
            reads_per_second,
            writes_per_second,

            // p99 over the mean: pressure scoring cares about the tail a
            // control plane needs to react to, not the common case. Absent
            // (no window closed, or the window held no requests of this
            // class) reads as 0.0 — see the module docs' "absent reads as
            // low pressure" convention.
            read_latency_us: next.read_latency_p99_us.map(|us| us as f64).unwrap_or(0.0),
            write_latency_us: next.write_latency_p99_us.map(|us| us as f64).unwrap_or(0.0),

            cpu_utilization: cpu_utilization(self, next, seconds),

            // Already a 0..1 ratio on the current sample; absent (platform
            // would not report it) reads as 0.0, same convention.
            memory_utilization: next.memory_utilization.unwrap_or(0.0),

            storage_bytes_per_second: 0,
            network_in_bytes_per_second: 0,
            network_out_bytes_per_second: 0,

            queue_depth: queue_pressure_units(next.in_flight, next.max_concurrent),

            cell_breakdown,
            cell_breakdown_partial,
        })
    }
}

/// CPU utilization from the daemon's own poll interval — see the module
/// docs for why this, and not `runtime.window.cpu_utilization`, is used.
///
/// `0.0` (the "absent reads as low pressure" default) when either sample
/// lacks `cpu_seconds_total`/`cpu_cores`, or when the counter went backwards
/// (a restart the top-level `reads_total`/`writes_total` check did not
/// happen to catch — defensive, since CPU seconds reset with the process
/// exactly like every other counter here).
fn cpu_utilization(before: &StatsSample, after: &StatsSample, seconds: f64) -> f64 {
    let (Some(cpu_before), Some(cpu_after), Some(cores)) =
        (before.cpu_seconds_total, after.cpu_seconds_total, after.cpu_cores)
    else {
        return 0.0;
    };

    if cores == 0 {
        return 0.0;
    }

    let cpu_seconds = cpu_after - cpu_before;

    if cpu_seconds < 0.0 {
        return 0.0;
    }

    (cpu_seconds / (seconds * cores as f64)).clamp(0.0, 1.0)
}

/// Project `in_flight / max_concurrent` onto the `0..10_000` scale
/// `fabric_workload::profile::calculate_pressure` already treats as the
/// abstract unit of queue pressure. See the module docs for why this ratio,
/// and why this scale.
fn queue_pressure_units(in_flight: u64, max_concurrent: u64) -> u64 {
    if max_concurrent == 0 {
        return 0;
    }

    let saturation = (in_flight as f64 / max_concurrent as f64).clamp(0.0, 1.0);

    (saturation * 10_000.0).round() as u64
}

/// Difference the per-coordinate attribution between two samples into
/// per-cell rates, and say whether that attribution is known-partial.
///
/// A coordinate present in `next` but not `self` is a coordinate first
/// tracked during this interval; its whole count is treated as the delta,
/// since the table starts every coordinate at zero. A coordinate's counters
/// are monotonic for the life of the process (FacetQL never evicts an
/// occupied slot), so — other than the whole-process restart the caller
/// already checks for — a delta here cannot go backwards; `saturating_sub`
/// is still used, defensively, rather than assuming that invariant holds.
fn cell_breakdown(
    before: &StatsSample,
    after: &StatsSample,
    seconds: f64,
) -> (Vec<CellWorkloadMetrics>, bool) {
    let previous: HashMap<(u8, u8, u8, u8), &CellStats> = before
        .cells
        .iter()
        .map(|cell| ((cell.x, cell.y, cell.z, cell.q), cell))
        .collect();

    let mut cells: Vec<CellWorkloadMetrics> = after
        .cells
        .iter()
        .map(|cell| {
            let key = (cell.x, cell.y, cell.z, cell.q);
            let before = previous.get(&key);

            let reads = cell.reads.saturating_sub(before.map_or(0, |b| b.reads));
            let writes = cell.writes.saturating_sub(before.map_or(0, |b| b.writes));
            let bytes_read = cell
                .bytes_read
                .saturating_sub(before.map_or(0, |b| b.bytes_read));
            let bytes_written = cell
                .bytes_written
                .saturating_sub(before.map_or(0, |b| b.bytes_written));

            CellWorkloadMetrics {
                x: cell.x,
                y: cell.y,
                z: cell.z,
                q: cell.q,
                reads_per_second: reads as f64 / seconds,
                writes_per_second: writes as f64 / seconds,
                bytes_read_per_second: bytes_read as f64 / seconds,
                bytes_written_per_second: bytes_written as f64 / seconds,
            }
        })
        .collect();

    // Busiest first, mirroring the ordering FacetQL's own `CellTable`
    // reports its snapshot in.
    cells.sort_by(|a, b| {
        let busy_a = a.reads_per_second + a.writes_per_second;
        let busy_b = b.reads_per_second + b.writes_per_second;
        busy_b
            .partial_cmp(&busy_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // The overflow counters are cumulative for the life of the process, so
    // "non-zero right now" is exactly "this attribution has ever missed
    // something" — the current state of completeness, per the wire type's
    // own docs.
    let partial = after.overflow_reads > 0 || after.overflow_writes > 0;

    (cells, partial)
}

/// Current time in epoch milliseconds. A clock set before 1970 is not worth a
/// panic in a telemetry path, so it reads as 0.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The read share of a metric's traffic, and the write share.
///
/// Split out because both the protocol's [`TelemetrySample`] and the workload
/// [`WorkloadProfile`] want the same two ratios computed the same way,
/// including the same divide-by-zero guard: an idle interval is 0/0 traffic,
/// which is not "all reads".
///
/// [`TelemetrySample`]: fabric_protocol::TelemetrySample
/// [`WorkloadProfile`]: fabric_workload::WorkloadProfile
pub(crate) fn ratios(metrics: &WorkloadMetrics) -> (f64, f64) {
    let total = metrics.total_operations();

    if total > 0.0 {
        (
            metrics.reads_per_second / total,
            metrics.writes_per_second / total,
        )
    } else {
        (0.0, 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample(taken_at: Instant, reads: u64, writes: u64) -> StatsSample {
        StatsSample {
            taken_at_ms: 0,
            taken_at,
            reads_total: reads,
            writes_total: writes,
            node_count: 0,
            edge_count: 0,
            cpu_seconds_total: None,
            cpu_cores: None,
            memory_utilization: None,
            read_latency_p99_us: None,
            write_latency_p99_us: None,
            in_flight: 0,
            max_concurrent: 0,
            cells: Vec::new(),
            overflow_reads: 0,
            overflow_writes: 0,
        }
    }

    fn cell(x: u8, reads: u64, writes: u64, bytes_read: u64, bytes_written: u64) -> CellStats {
        CellStats {
            x,
            y: 0,
            z: 0,
            q: 0,
            reads,
            writes,
            bytes_read,
            bytes_written,
        }
    }

    #[test]
    fn two_samples_a_second_apart_are_a_rate() {
        let start = Instant::now();
        let first = sample(start, 100, 10);
        let second = sample(start + Duration::from_secs(2), 300, 30);

        let metrics = first.difference(&second).expect("a real interval");
        assert_eq!(metrics.reads_per_second, 100.0);
        assert_eq!(metrics.writes_per_second, 10.0);
        assert_eq!(metrics.operations_per_second, 110.0);

        let (read_ratio, write_ratio) = ratios(&metrics);
        assert!((read_ratio - 100.0 / 110.0).abs() < 1e-12);
        assert!((write_ratio - 10.0 / 110.0).abs() < 1e-12);
    }

    #[test]
    fn nothing_facetql_does_not_measure_is_invented() {
        let start = Instant::now();
        let metrics = sample(start, 0, 0)
            .difference(&sample(start + Duration::from_secs(1), 10, 0))
            .unwrap();

        assert_eq!(metrics.cpu_utilization, 0.0);
        assert_eq!(metrics.memory_utilization, 0.0);
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(metrics.read_latency_us, 0.0);
        assert!(metrics.cell_breakdown.is_empty());
        assert!(!metrics.cell_breakdown_partial);
        // ...and absent resource pressure reads as low pressure, not as hot.
        assert!(!metrics.is_hot());
    }

    #[test]
    fn a_restart_drops_the_interval_instead_of_reporting_a_spike() {
        let start = Instant::now();
        let before = sample(start, 5_000_000, 1_000);
        let after_restart = sample(start + Duration::from_secs(1), 3, 0);

        assert!(before.difference(&after_restart).is_none());
    }

    #[test]
    fn a_zero_length_interval_is_not_a_rate() {
        let start = Instant::now();
        assert!(sample(start, 0, 0).difference(&sample(start, 10, 10)).is_none());
    }

    #[test]
    fn an_idle_interval_is_zero_traffic_not_all_reads() {
        let start = Instant::now();
        let metrics = sample(start, 7, 7)
            .difference(&sample(start + Duration::from_secs(1), 7, 7))
            .unwrap();

        assert_eq!(metrics.operations_per_second, 0.0);
        assert_eq!(ratios(&metrics), (0.0, 0.0));
    }

    /// CPU is differenced from the monotonic counter across the daemon's own
    /// interval — not read from FacetQL's own window.
    #[test]
    fn cpu_utilization_comes_from_the_monotonic_counter() {
        let start = Instant::now();
        let mut before = sample(start, 0, 0);
        before.cpu_seconds_total = Some(1.0);
        before.cpu_cores = Some(2);

        let mut after = sample(start + Duration::from_secs(1), 0, 0);
        after.cpu_seconds_total = Some(2.0); // +1 CPU-second over 1 wall second, 2 cores.
        after.cpu_cores = Some(2);

        let metrics = before.difference(&after).unwrap();
        assert!((metrics.cpu_utilization - 0.5).abs() < 1e-9);
    }

    /// A window that never closed (`duration_ms == 0` on the wire — modeled
    /// here as the server simply not reporting `cpu_seconds_total`/latency
    /// yet) must not be read as an idle process: it is absent, and reads as
    /// the same "low pressure" default a genuinely idle-but-measured window
    /// would also produce. The two are indistinguishable from here, which is
    /// exactly the point: neither is a confident wrong answer.
    #[test]
    fn an_unclosed_window_and_a_measured_idle_window_agree_on_absence() {
        let start = Instant::now();

        // No `cpu_seconds_total` at all on either sample: as if the process
        // has been up for under FacetQL's minimum window and nothing has
        // closed yet.
        let never_closed = sample(start, 0, 0)
            .difference(&sample(start + Duration::from_secs(1), 0, 0))
            .unwrap();
        assert_eq!(never_closed.cpu_utilization, 0.0);

        // A window that genuinely closed with zero CPU consumed reports the
        // same number, for a different, equally honest reason.
        let mut before = sample(start, 0, 0);
        before.cpu_seconds_total = Some(5.0);
        before.cpu_cores = Some(4);
        let mut after = sample(start + Duration::from_secs(1), 0, 0);
        after.cpu_seconds_total = Some(5.0);
        after.cpu_cores = Some(4);

        let idle = before.difference(&after).unwrap();
        assert_eq!(idle.cpu_utilization, 0.0);
    }

    /// A saturated engine — every in-flight slot taken — must score at the
    /// top of the pressure model's queue term, not at ~1% of it.
    #[test]
    fn a_saturated_engine_reports_maximum_queue_pressure() {
        let start = Instant::now();
        let mut before = sample(start, 0, 0);
        before.max_concurrent = 512;
        before.in_flight = 0;

        let mut after = sample(start + Duration::from_secs(1), 0, 0);
        after.max_concurrent = 512;
        after.in_flight = 512;

        let metrics = before.difference(&after).unwrap();
        assert_eq!(metrics.queue_depth, 10_000);
        assert!(metrics.is_queue_hot());
    }

    /// A quiet engine — most slots free — must not read as saturated.
    #[test]
    fn a_quiet_engine_reports_low_queue_pressure() {
        let start = Instant::now();
        let mut before = sample(start, 0, 0);
        before.max_concurrent = 512;
        before.in_flight = 5;

        let mut after = sample(start + Duration::from_secs(1), 0, 0);
        after.max_concurrent = 512;
        after.in_flight = 5;

        let metrics = before.difference(&after).unwrap();
        assert!(metrics.queue_depth < 200, "got {}", metrics.queue_depth);
        assert!(!metrics.is_queue_hot());
    }

    /// Per-cell reads/writes/bytes are differenced exactly like the
    /// whole-instance totals, keyed by FacetQL's own coordinate.
    #[test]
    fn per_cell_traffic_is_differenced_into_rates() {
        let start = Instant::now();
        let mut before = sample(start, 0, 0);
        before.cells = vec![cell(1, 100, 10, 1_000, 100), cell(2, 50, 5, 500, 50)];

        let mut after = sample(start + Duration::from_secs(1), 0, 0);
        after.cells = vec![
            cell(1, 300, 10, 3_000, 100), // +200 reads, no new writes.
            cell(2, 50, 25, 500, 250),    // +20 writes only.
            cell(3, 40, 0, 400, 0),       // newly tracked: whole count is the delta.
        ];

        let metrics = before.difference(&after).unwrap();
        assert_eq!(metrics.cell_breakdown.len(), 3);

        let cell1 = metrics
            .cell_breakdown
            .iter()
            .find(|c| c.x == 1)
            .expect("cell 1");
        assert_eq!(cell1.reads_per_second, 200.0);
        assert_eq!(cell1.writes_per_second, 0.0);
        assert_eq!(cell1.bytes_read_per_second, 2_000.0);

        let cell3 = metrics
            .cell_breakdown
            .iter()
            .find(|c| c.x == 3)
            .expect("newly tracked cell 3");
        assert_eq!(cell3.reads_per_second, 40.0);

        // Busiest first: cell 1 did 200 reads/sec, more than any other.
        assert_eq!(metrics.cell_breakdown[0].x, 1);

        assert!(!metrics.cell_breakdown_partial);
    }

    /// Non-zero overflow means the attribution is a known-partial account,
    /// and the daemon must be told so rather than trusting `cell_breakdown`
    /// as the whole picture.
    #[test]
    fn overflow_marks_the_breakdown_as_partial() {
        let start = Instant::now();
        let before = sample(start, 0, 0);

        let mut after = sample(start + Duration::from_secs(1), 0, 0);
        after.cells = vec![cell(1, 10, 0, 100, 0)];
        after.overflow_reads = 3;

        let metrics = before.difference(&after).unwrap();
        assert!(metrics.cell_breakdown_partial);
    }
}
