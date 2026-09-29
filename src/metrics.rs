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
//! | `tokio_spawned_tasks`             | Gauge      | Currently-live spawned tasks, labeled by category (see `TaskCategory`) |
//! | `tokio_task_load_reference`       | Gauge      | Load reference per category (active connections / in-flight jobs) used to correlate task count |
//! | `tokio_task_leak_suspected_total` | Counter    | Times a category's task count grew without bound relative to load |
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
// Tokio task leak detection
// ---------------------------------------------------------------------------
//
// Long-running services spawn many background tasks (scheduler jobs, webhook
// dispatch workers, WebSocket connection handlers). A task that never
// terminates (e.g. an unbounded channel receiver that is never dropped) leaks
// gradually and is easy to miss until it causes resource exhaustion.
//
// To make this class of bug observable we tag every spawned task with a
// [`TaskCategory`] at spawn time, track the live count per category, and
// correlate that count against a per-category *load reference* (active
// connections, in-flight jobs). A healthy task pool scales with load; a leak
// grows without bound while load stays flat, which is what we alert on.

/// Origin/category of a spawned tokio task.
///
/// Tagging at spawn time lets the resulting metric distinguish, e.g.,
/// WebSocket-connection tasks from scheduler-job tasks instead of reporting
/// one opaque total count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskCategory {
    /// One task per live WebSocket connection.
    WebSocketConnection,
    /// One task per scheduler job execution.
    SchedulerJob,
    /// One task per in-flight webhook dispatch.
    WebhookDispatch,
}

impl TaskCategory {
    /// Stable label value used on the exported metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            TaskCategory::WebSocketConnection => "websocket-connection",
            TaskCategory::SchedulerJob => "scheduler-job",
            TaskCategory::WebhookDispatch => "webhook-dispatch",
        }
    }

    /// All categories, used when registering observable gauges.
    pub const ALL: [TaskCategory; 3] = [
        TaskCategory::WebSocketConnection,
        TaskCategory::SchedulerJob,
        TaskCategory::WebhookDispatch,
    ];
}

/// Per-category live task count and load reference.
#[derive(Debug, Default, Clone, Copy)]
struct TaskCategoryState {
    live: u64,
    load: u64,
}

static TASK_STATE: OnceLock<[std::sync::Mutex<TaskCategoryState>; 3]> = OnceLock::new();

fn task_state() -> &'static [std::sync::Mutex<TaskCategoryState>; 3] {
    TASK_STATE.get_or_init(|| {
        [
            std::sync::Mutex::new(TaskCategoryState::default()),
            std::sync::Mutex::new(TaskCategoryState::default()),
            std::sync::Mutex::new(TaskCategoryState::default()),
        ]
    })
}

fn category_index(category: TaskCategory) -> usize {
    match category {
        TaskCategory::WebSocketConnection => 0,
        TaskCategory::SchedulerJob => 1,
        TaskCategory::WebhookDispatch => 2,
    }
}

/// Record that a task of `category` has been spawned.
///
/// Call this immediately before `tokio::spawn`; pair it with
/// [`task_finished`] in the task body (or via [`TaskLeakGuard`]) so the live
/// count is decremented when the task terminates.
pub fn task_spawned(category: TaskCategory) {
    let mut state = task_state()[category_index(category)].lock().unwrap();
    state.live = state.live.saturating_add(1);
}

/// Record that a task of `category` has terminated.
pub fn task_finished(category: TaskCategory) {
    let mut state = task_state()[category_index(category)].lock().unwrap();
    state.live = state.live.saturating_sub(1);
}

/// Update the load reference for `category` (active connections, in-flight
/// jobs, ...). Used to correlate task count against legitimate traffic.
pub fn task_load_reference(category: TaskCategory, load: u64) {
    let mut state = task_state()[category_index(category)].lock().unwrap();
    state.load = load;
}

/// RAII guard that decrements the live task count when dropped.
///
/// Wrap the body of a spawned task so the count is decremented even if the
/// task panics or returns early:
///
/// ```ignore
/// metrics::task_spawned(TaskCategory::SchedulerJob);
/// tokio::spawn(async move {
///     let _guard = metrics::TaskLeakGuard::new(TaskCategory::SchedulerJob);
///     // ... job body ...
/// });
/// ```
pub struct TaskLeakGuard {
    category: TaskCategory,
}

impl TaskLeakGuard {
    /// Create a guard for `category`.
    pub fn new(category: TaskCategory) -> Self {
        Self { category }
    }
}

impl Drop for TaskLeakGuard {
    fn drop(&mut self) {
        task_finished(self.category);
    }
}

/// Snapshot of `(live, load)` for a category, for tests and alerting.
pub fn task_snapshot(category: TaskCategory) -> (u64, u64) {
    let state = task_state()[category_index(category)].lock().unwrap();
    (state.live, state.load)
}

/// A category's task count is considered leaked when it exceeds the load
/// reference by more than this many tasks *and* by more than this ratio.
const LEAK_ABSOLUTE_SLACK: u64 = 32;
const LEAK_RATIO_SLACK: f64 = 2.0;

/// Evaluate whether `category`'s live task count has grown without bound
/// relative to its load reference.
///
/// Raw count alone is not a leak signal (it scales with legitimate traffic),
/// so we only flag growth that is uncorrelated with load: the live count must
/// exceed both an absolute slack and a multiple of the load reference.
///
pub fn task_leak_suspected(category: TaskCategory) -> bool {
    let (live, load) = task_snapshot(category);
    let threshold = (load as f64 * LEAK_RATIO_SLACK) as u64 + LEAK_ABSOLUTE_SLACK;
    live > threshold
}

/// Register the tokio task-leak observable gauges on `meter`.
///
/// Exposes `tokio_spawned_tasks` (live count per category),
/// `tokio_task_load_reference` (load per category) and
/// `tokio_task_leak_suspected_total` (alert counter).
fn register_task_leak_gauges(meter: &Meter) {
    let spawned = meter
        .u64_observable_gauge("tokio_spawned_tasks")
        .with_description("Currently-live spawned tokio tasks, by category")
        .with_unit(Unit::new("{task}"))
        .with_callback(|observer| {
            for category in TaskCategory::ALL {
                let (live, _) = task_snapshot(category);
                observer.observe(live, &[KeyValue::new("category", category.as_str())]);
            }
        })
        .build();

    let load = meter
        .u64_observable_gauge("tokio_task_load_reference")
        .with_description("Load reference per task category (active connections / in-flight jobs)")
        .with_unit(Unit::new("{unit}"))
        .with_callback(|observer| {
            for category in TaskCategory::ALL {
                let (_, load) = task_snapshot(category);
                observer.observe(load, &[KeyValue::new("category", category.as_str())]);
            }
        })
        .build();

    let leak_counter = meter
        .u64_counter("tokio_task_leak_suspected_total")
        .with_description("Times a category's task count grew without bound relative to load")
        .build();

    // Keep the instruments alive for the lifetime of the process; the SDK
    // holds the callbacks, but we retain the handles so they are not dropped.
    let _ = (spawned, load, leak_counter);
}

/// Evaluate every category and increment `tokio_task_leak_suspected_total`
/// for any that look leaked. Intended to be called periodically (e.g. from a
/// background watchdog task).
///
/// Returns the categories flagged as suspected leaks.
pub fn check_task_leaks() -> Vec<TaskCategory> {
    let mut flagged = Vec::new();
    for category in TaskCategory::ALL {
        if task_leak_suspected(category) {
            flagged.push(category);
        }
    }
    if !flagged.is_empty() {
        let counter = meter().u64_counter("tokio_task_leak_suspected_total").build();
        for category in &flagged {
            counter.add(1, &[KeyValue::new("category", category.as_str())]);
        }
    }
    flagged
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
    /// `true` when the before/after windows overlapped a neighbouring rel

/* … truncated 11040 chars — edit only what you need near the top … */
