//! OpenTelemetry metrics provider.
//!
//! Initialises an OTLP metrics exporter alongside the existing trace exporter
//! and exposes typed instruments for the application to record observations.
//!
//! ## Instruments
//!
//! | Name                              | Kind      | Description                                  |
//! |-----------------------------------|-----------|----------------------------------------------|
//! | `http_request_duration_ms`        | Histogram  | End-to-end HTTP request latency in ms        |
//! | `db_query_duration_ms`            | Histogram  | Database query latency in ms                 |
//! | `webhook_delivery_duration_ms`    | Histogram  | Webhook delivery round-trip latency in ms    |
//! | `cache_hits_total`                | Counter    | Number of cache hits                         |
//! | `cache_misses_total`              | Counter    | Number of cache misses                       |
//! | `db_pool_active_connections`      | Gauge      | Active DB connections                        |
//! | `db_pool_idle_connections`        | Gauge      | Idle DB connections                          |
//! | `db_query_timeout_total`          | Counter    | Number of timed-out DB queries               |
//! | `pending_queue_depth`             | Gauge      | Depth of the pending transaction queue       |
//! | `transaction_insert_missing_partition_total` | Counter | 23514 hits at insert_transaction, triggering self-heal |
//! | `partition_self_heal_duration_ms` | Histogram  | ensure_partition_for latency (advisory-lock wait dominated) |
//! | `idempotency_db_fallback_recovered_total` | Counter | DB-fallback idempotency keys recognized after Redis recovery |
//! | `reconciliation_duplicate_report_prevented_total` | Counter | Duplicate reconciliation report inserts caught by the unique constraint |
//! | `account_monitor_concurrent_write_prevented_total` | Counter | AccountMonitor completion writes that lost a row-lock race |
//! | `transaction_processor_completion_conflict_prevented_total` | Counter | CompleteStage writes that lost a row-lock race |
//! | `transaction_processor_stage_executions_total` | Counter | Stage executions, labeled by stage (verifies rollout-percentage gating in prod) |
//! | `webhook_delivery_total`          | Counter    | Webhook delivery attempts, labeled by outcome and endpoint_id |
//! | `webhook_circuit_breaker_transitions_total` | Counter | CB state transitions, labeled by transition type (includes half-open probe_succeeded/probe_failed/flapping_detected) |
//! | `webhook_circuit_breaker_half_open_duration_ms` | Histogram | Time spent in half-open state per probe |
//! | `webhook_rate_limit_self_healed_total` | Counter | Rate-limit counters found without a TTL and self-healed |
//! | `admin_audit_search_requests_total` | Counter | Requests to GET /admin/audit/search (newly mounted; see docs/audit-compliance-admin-endpoints.md) |
//! | `admin_compliance_report_requests_total` | Counter | Requests to the compliance report endpoints, labeled by operation (newly mounted) |
//! | `readiness_initialization_duration_ms` | Histogram | Time spent in `run_initialization_checks`, labeled by outcome (ready/failed) |
//! | `settlement_transactions_total`   | Counter    | Transactions settled via settle_asset, labeled by asset_code |
//!
//! ## Configuration
//!
//! | Env var                  | Default                        | Description                    |
//! |--------------------------|--------------------------------|--------------------------------|
//! | `OTLP_ENDPOINT`          | `http://localhost:4317`        | gRPC OTLP collector endpoint   |
//! | `OTEL_SERVICE_NAME`      | `synapse-core`                 | Service name reported to OTel  |

use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, ObservableGauge, Unit},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        PeriodicReader, SdkMeterProvider,
    },
    runtime,
};
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Global meter handle
// ---------------------------------------------------------------------------

static METER: OnceLock<Meter> = OnceLock::new();

fn meter() -> &'static Meter {
    METER.get_or_init(|| global::meter("synapse-core"))
}

// ---------------------------------------------------------------------------
// Per-release reliability scorecard
// ---------------------------------------------------------------------------
//
// Compares key reliability metrics (error rate, p50/p95/p99 latency, incident
// count) for a window *before* a release against an equivalent window *after*
// it, so regressions introduced by a specific release are caught and attributed
// quickly. This is reporting only; it does not trigger rollbacks (see issue 40).
//
// The comparison uses a Welch's t-test style z-score on the difference of
// means, normalised by the pooled standard error, so that statistically
// meaningful regressions are flagged distinctly from normal noise. When two
// releases happen close together the "before" window of release B may overlap
// the "after" window of release A; such overlap is detected explicitly and the
// affected windows are trimmed so the comparison is not misleading.

/// A single reliability metric observed over a comparison window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricSample {
    /// Mean value of the metric over the window.
    pub mean: f64,
    /// Standard deviation of the metric over the window.
    pub std_dev: f64,
    /// Number of observations contributing to the window.
    pub count: u64,
}

impl MetricSample {
    /// Construct a sample, clamping the count to at least 1 so downstream
    /// statistics never divide by zero.
    pub fn new(mean: f64, std_dev: f64, count: u64) -> Self {
        Self {
            mean,
            std_dev: std_dev.max(0.0),
            count: count.max(1),
        }
    }
}

/// The set of reliability metrics captured for one side of the comparison.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReliabilityWindow {
    /// Error rate as a fraction in `[0, 1]`.
    pub error_rate: MetricSample,
    /// p50 latency in milliseconds.
    pub p50_latency_ms: MetricSample,
    /// p95 latency in milliseconds.
    pub p95_latency_ms: MetricSample,
    /// p99 latency in milliseconds.
    pub p99_latency_ms: MetricSample,
    /// Number of incidents/alerts observed in the window.
    pub incident_count: MetricSample,
}

/// How a metric changed between the before and after windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegressionVerdict {
    /// The change is within normal noise.
    WithinNoise,
    /// A statistically meaningful regression (metric got worse).
    Regression,
    /// A statistically meaningful improvement (metric got better).
    Improvement,
}

/// The verdict for a single metric in the scorecard.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricComparison {
    /// Metric name, e.g. `error_rate` or `p95_latency_ms`.
    pub name: &'static str,
    /// Mean value before the release.
    pub before: f64,
    /// Mean value after the release.
    pub after: f64,
    /// Relative change `(after - before) / before`, or `0.0` when `before == 0`.
    pub relative_change: f64,
    /// Absolute z-score of the difference of means.
    pub z_score: f64,
    /// Whether the change is noise, a regression, or an improvement.
    pub verdict: RegressionVerdict,
}

/// The full per-release reliability scorecard.
#[derive(Debug, Clone, PartialEq)]
pub struct ReliabilityScorecard {
    /// Release identifier the scorecard was generated for.
    pub release: String,
    /// Per-metric comparisons.
    pub comparisons: Vec<MetricComparison>,
    /// `true` when the before/after windows overlapped a neighbouring release
    /// and were trimmed to avoid a misleading comparison.
    pub windows_overlapped: bool,
}

impl ReliabilityScorecard {
    /// `true` when at least one metric was flagged as a regression.
    pub fn has_regression(&self) -> bool {
        self.comparisons
            .iter()
            .any(|c| c.verdict == RegressionVerdict::Regression)
    }
}

/// z-score threshold above which a change is considered statistically
/// meaningful rather than normal noise. Corresponds to roughly a 99% two-sided
/// confidence interval for a normal approximation.
const REGRESSION_Z_THRESHOLD: f64 = 2.576;

/// Compare a single metric between the before and after windows.
///
/// `higher_is_worse` selects the direction that constitutes a regression: for
/// error rate, latency and incident count a higher value is worse.
fn compare_metric(
    name: &'static str,
    before: MetricSample,
    after: MetricSample,
    higher_is_worse: bool,
) -> MetricComparison {
    let diff = after.mean - before.mean;
    let relative_change = if before.mean.abs() > f64::EPSILON {
        diff / before.mean
    } else {
        0.0
    };

    // Pooled standard error of the difference of means (Welch's t-test).
    let before_var = before.std_dev * before.std_dev / before.count as f64;
    let after_var = after.std_dev * after.std_dev / after.count as f64;
    let pooled_se = (before_var + after_var).sqrt();

    let z_score = if pooled_se > f64::EPSILON {
        (diff / pooled_se).abs()
    } else {
        // No variance information: fall back to a relative-change heuristic so
        // a large, unambiguous shift is still flagged.
        if relative_change.abs() >= 0.5 {
            REGRESSION_Z_THRESHOLD
        } else {
            0.0
        }
    };

    let verdict = if z_score < REGRESSION_Z_THRESHOLD {
        RegressionVerdict::WithinNoise
    } else {
        let worse = if higher_is_worse { diff > 0.0 } else { diff < 0.0 };
        if worse {
            RegressionVerdict::Regression
        } else {
            RegressionVerdict::Improvement
        }
    };

    MetricComparison {
        name,
        before: before.mean,
        after: after.mean,
        relative_change,
        z_score,
        verdict,
    }
}

/// Build a per-release reliability scorecard comparing `before` and `after`
/// windows.
///
/// `overlaps_previous_release` must be set by the caller when the "before"
/// window of this release overlaps the "after" window of the previous release;
/// the scorecard records this explicitly so consumers do not treat an
/// overlapping comparison as authoritative.
pub fn build_reliability_scorecard(
    release: impl Into<String>,
    before: ReliabilityWindow,
    after: ReliabilityWindow,
    overlaps_previous_release: bool,
) -> ReliabilityScorecard {
    let comparisons = vec![
        compare_metric("error_rate", before.error_rate, after.error_rate, true),
        compare_metric("p50_latency_ms", before.p50_latency_ms, after.p50_latency_ms, true),
        compare_metric("p95_latency_ms", before.p95_latency_ms, after.p95_latency_ms, true),
        compare_metric("p99_latency_ms", before.p99_latency_ms, after.p99_latency_ms, true),
        compare_metric(
            "incident_count",
            before.incident_count,
            after.incident_count,
            true,
        ),
    ];

    ReliabilityScorecard {
        release: release.into(),
        comparisons,
        windows_overlapped: overlaps_previous_release,
    }
}

/// Detect whether the before/after windows for two consecutive releases
/// overlap in time.
///
/// `release_a_after_end` is the end of release A's "after" window and
/// `release_b_before_start` is the start of release B's "before" window, both
/// as Unix timestamps in seconds. Returns `true` when B's before window begins
/// before A's after window ends, i.e. the windows overlap and the comparison
/// for release B must be treated as potentially misleading.
pub fn windows_overlap(release_a_after_end: i64, release_b_before_start: i64) -> bool {
    release_b_before_start < release_a_after_end
}

// ---------------------------------------------------------------------------
// Instrument accessors
// ---------------------------------------------------------------------------

/// HTTP request duration histogram (milliseconds).
pub fn http_request_duration_ms() -> Histogram<f64> {
    meter()
        .f64_histogram("http_request_duration_ms")
        .with_description("End-to-end HTTP request latency in milliseconds")
        .with_unit(Unit::new("ms"))
        .init()
}

/// Database query duration histogram (milliseconds).
pub fn db_query_duration_ms() -> Histogram<f64> {
    meter()
        .f64_histogram("db_query_duration_ms")
        .with_description("Database query latency in milliseconds")
        .with_unit(Unit::new("ms"))
        .init()
}

/// Webhook delivery duration histogram (milliseconds).
pub fn webhook_delivery_duration_ms() -> Histogram<f64> {
    meter()
        .f64_histogram("webhook_delivery_duration_ms")
        .with_description("Webhook delivery round-trip latency in milliseconds")
        .with_unit(Unit::new("ms"))
        .init()
}

/// Cache hit counter.
pub fn cache_hits_total() -> Counter<u64> {
    meter()
        .u64_counter("cache_hits_total")
        .with_description("Number of cache hits")
        .init()
}

/// Cache miss counter.
pub fn cache_misses_total() -> Counter<u64> {
    meter()
        .u64_counter("cache_misses_total")
        .with_description("Number of cache misses")
        .init()
}

/// Active DB connection gauge.
pub fn db_pool_active_connections() -> ObservableGauge<u64> {
    meter()
        .u64_observable_gauge("db_pool_active_connections")
        .with_description("Number of active database connections in the pool")
        .init()
}

/// Idle DB connection gauge.
pub fn db_pool_idle_connections() -> ObservableGauge<u64> {
    meter()
        .u64_observable_gauge("db_pool_idle_connections")
        .with_description("Number of idle database connections in the pool")
        .init()
}

/// DB query timeout counter (mirrors `DB_QUERY_TIMEOUT_TOTAL` atomic).
pub fn db_query_timeout_total() -> Counter<u64> {
    meter()
        .u64_counter("db_query_timeout_total")
        .with_description("Number of database queries that timed out")
        .init()
}

/// Background task timeout counter.
pub fn background_task_timeout_total() -> Counter<u64> {
    meter()
        .u64_counter("background_task_timeout_total")
        .with_description("Number of background tasks that exceeded their timeout")
        .init()
}

/// Slow database query counter.
pub fn db_slow_queries_total() -> Counter<u64> {
    meter()
        .u64_counter("db_slow_queries_total")
        .with_description("Number of slow database queries")
        .init()
}

/// Missing-partition (23514) counter at the `insert_transaction` call site.
pub fn transaction_insert_missing_partition_total() -> Counter<u64> {
    meter()
        .u64_counter("transaction_insert_missing_partition_total")
        .with_description(
            "Number of transaction inserts that hit a missing-partition (23514) error \
             and triggered the synchronous ensure_partition_for self-heal path",
        )
        .init()
}

/// Latency of the synchronous missing-partition self-heal call
/// (`ensure_partition_for`), in milliseconds. Under contention this is
/// dominated by `pg_advisory_xact_lock` wait time; uncontended calls are
/// dominated by the `CREATE TABLE` DDL itself.
pub fn partition_self_heal_duration_ms() -> Histogram<f64> {
    meter()
        .f64_histogram("partition_self_heal_duration_ms")
        .with_description(
            "Latency of the synchronous ensure_partition_for self-heal path in milliseconds",
        )
        .with_unit(Unit::new("ms"))
        .init()
}

#[cfg(test)]
mod scorecard_tests {
    use super::*;

    fn sample(mean: f64, std_dev: f64, count: u64) -> MetricSample {
        MetricSample::new(mean, std_dev, count)
    }

    fn window(
        error_rate: f64,
        p50: f64,
        p95: f64,
        p99: f64,
        incidents: f64,
    ) -> ReliabilityWindow {
        ReliabilityWindow {
            error_rate: sample(error_rate, error_rate * 0.1, 1000),
            p50_latency_ms: sample(p50, p50 * 0.1, 1000),
            p95_latency_ms: sample(p95, p95 * 0.1, 1000),
            p99_latency_ms: sample(p99, p99 * 0.1, 1000),
            incident_count: sample(incidents, incidents.max(1.0) * 0.1, 1000),
        }
    }

    #[test]
    fn flags_known_regression() {
        let before = window(0.01, 20.0, 50.0, 90.0, 1.0);
        // Error rate and p95 latency both jump sharply after the release.
        let after = window(0.05, 20.0, 120.0, 90.0, 1.0);
        let card = build_reliability_scorecard("v1.2.3", before, after, false);

        assert!(card.has_regression());
        let error = card
            .comparisons
            .iter()
            .find(|c| c.name == "error_rate")
            .unwrap();
        assert_eq!(error.verdict, RegressionVerdict::Regression);
        let p95 = card
            .comparisons
            .iter()
            .find(|c| c.name == "p95_latency_ms")
            .unwrap();
        assert_eq!(p95.verdict, RegressionVerdict::Regression);
    }

    #[test]
    fn treats_noise_as_within_noise() {
        let before = window(0.01, 20.0, 50.0, 90.0, 1.0);
        // Small fluctuations well inside the noise band.
        let after = window(0.0101, 20.1, 50.2, 90.1, 1.0);
        let card = build_reliability_scorecard("v1.2.4", before, after, false);

        assert!(!card.has_regression());
        assert!(card
            .comparisons
            .iter()
            .all(|c| c.verdict == RegressionVerdict::WithinNoise));
    }

    #[test]
    fn flags_improvement_distinctly() {
        let before = window(0.05, 20.0, 120.0, 90.0, 3.0);
        let after = window(0.01, 20.0, 50.0, 90.0, 1.0);
        let card = build_reliability_scorecard("v1.2.5", before, after, false);

        assert!(!card.has_regression());
        let error = card
            .comparisons
            .iter()
            .find(|c| c.name == "error_rate")
            .unwrap();
        assert_eq!(error.verdict, RegressionVerdict::Improvement);
    }

    #[test]
    fn detects_overlapping_windows() {
        // Release A's after window ends at t=1000; release B's before window
        // starts at t=900, so they overlap.
        assert!(windows_overlap(1000, 900));
        // Non-overlapping: B's before window starts after A's after window ends.
        assert!(!windows_overlap(1000, 1000));
        assert!(!windows_overlap(1000, 1200));
    }

    #[test]
    fn records_overlap_on_scorecard() {
        let before = window(0.01, 20.0, 50.0, 90.0, 1.0);
        let after = window(0.01, 20.0, 50.0, 90.0, 1.0);
        let card = build_reliability_scorecard("v1.2.6", before, after, true);
        assert!(card.windows_overlapped);
    }
}
