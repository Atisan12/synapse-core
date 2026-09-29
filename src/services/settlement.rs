use crate::db::models::{Asset, Settlement};
use crate::db::queries;
use crate::error::AppError;
use crate::validation::state_transitions::{is_valid_transition, SETTLEMENT_TRANSITIONS};
use bigdecimal::BigDecimal;
use chrono::Utc;
use opentelemetry::metrics::Histogram;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use uuid::Uuid;

/// Maps a `sqlx::Error` to the appropriate `AppError` variant.
///
/// `RowNotFound` during settlement status update indicates concurrent modification (stale transition).
/// Other `RowNotFound` errors are treated as domain-level not-found.
fn map_db_err(e: sqlx::Error) -> AppError {
    match e {
        sqlx::Error::RowNotFound => AppError::NotFound("settlement record not found".to_string()),
        other => AppError::DatabaseError(other.to_string()),
    }
}

/// Maps update_settlement_status result, converting RowNotFound to StaleTransition
/// when it indicates a concurrent modification during atomic update.
fn map_update_settlement_err(e: sqlx::Error) -> AppError {
    match e {
        sqlx::Error::RowNotFound => AppError::StaleTransition,
        other => AppError::DatabaseError(other.to_string()),
    }
}

/// Per-stage latency budget for the webhook-to-reconciliation pipeline.
///
/// The budgets are expressed as a share of the overall end-to-end SLA target
/// and must sum to it. Actual per-stage latency is derived from the existing
/// trace spans (see `crate::observability::latency_budget`) rather than from
/// bespoke timers, so this stays an analysis/reporting layer on top of the
/// instrumentation that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PipelineStage {
    Ingestion,
    Validation,
    Processing,
    Settlement,
    Reconciliation,
}

impl PipelineStage {
    /// All stages in pipeline order.
    pub const ALL: [PipelineStage; 5] = [
        PipelineStage::Ingestion,
        PipelineStage::Validation,
        PipelineStage::Processing,
        PipelineStage::Settlement,
        PipelineStage::Reconciliation,
    ];

    /// Stable label used for metrics and reports.
    pub fn as_str(&self) -> &'static str {
        match self {
            PipelineStage::Ingestion => "ingestion",
            PipelineStage::Validation => "validation",
            PipelineStage::Processing => "processing",
            PipelineStage::Settlement => "settlement",
            PipelineStage::Reconciliation => "reconciliation",
        }
    }

    /// Fraction of the overall end-to-end SLA target allotted to this stage.
    ///
    /// Reconciliation is inherently periodic (it does not run per-transaction
    /// the way the other stages do), so its share is expressed as a fraction
    /// of the SLA window rather than of a single transaction's latency.
    pub fn budget_share(&self) -> f64 {
        match self {
            PipelineStage::Ingestion => 0.10,
            PipelineStage::Validation => 0.10,
            PipelineStage::Processing => 0.35,
            PipelineStage::Settlement => 0.30,
            PipelineStage::Reconciliation => 0.15,
        }
    }
}

/// Overall end-to-end SLA target for the webhook-to-reconciliation pipeline.
pub const END_TO_END_SLA_TARGET: Duration = Duration::from_secs(60);

/// Per-stage latency budget derived from [`END_TO_END_SLA_TARGET`].
#[derive(Debug, Clone, Copy)]
pub struct StageBudget {
    pub stage: PipelineStage,
    pub budget: Duration,
}

/// Returns the per-stage latency budget, summing to [`END_TO_END_SLA_TARGET`].
///
/// The final stage absorbs any rounding remainder so the budgets always sum
/// exactly to the overall target.
pub fn stage_budgets() -> Vec<StageBudget> {
    let total = END_TO_END_SLA_TARGET.as_secs_f64();
    let mut budgets = Vec::with_capacity(PipelineStage::ALL.len());
    let mut allocated = 0.0f64;
    for (idx, stage) in PipelineStage::ALL.iter().enumerate() {
        let secs = if idx == PipelineStage::ALL.len() - 1 {
            (total - allocated).max(0.0)
        } else {
            total * stage.budget_share()
        };
        allocated += secs;
        budgets.push(StageBudget {
            stage: *stage,
            budget: Duration::from_secs_f64(secs),
        });
    }
    budgets
}

/// Actual latency observed for a single stage, derived from trace spans.
#[derive(Debug, Clone, Copy)]
pub struct StageLatency {
    pub stage: PipelineStage,
    pub actual: Duration,
}

/// Per-stage comparison of actual latency against its allotted budget.
#[derive(Debug, Clone, Copy)]
pub struct StageBudgetReport {
    pub stage: PipelineStage,
    pub actual: Duration,
    pub budget: Duration,
}

impl StageBudgetReport {
    /// Fraction of the stage budget consumed (actual / budget).
    pub fn utilization(&self) -> f64 {
        let budget = self.budget.as_secs_f64();
        if budget <= 0.0 {
            return 0.0;
        }
        self.actual.as_secs_f64() / budget
    }

    /// Whether the stage exceeded its allotted budget share.
    pub fn exceeds_budget(&self) -> bool {
        self.actual > self.budget
    }
}

/// Aggregated latency-budget report across the whole pipeline.
#[derive(Debug, Clone)]
pub struct LatencyBudgetReport {
    pub stages: Vec<StageBudgetReport>,
}

impl LatencyBudgetReport {
    /// Build a report by pairing observed per-stage latencies with the
    /// configured per-stage budgets. Stages without an observation are
    /// reported with zero actual latency.
    pub fn from_latencies(latencies: &[StageLatency]) -> Self {
        let budgets = stage_budgets();
        let stages = budgets
            .into_iter()
            .map(|b| {
                let actual = latencies
                    .iter()
                    .find(|l| l.stage == b.stage)
                    .map(|l| l.actual)
                    .unwrap_or_default();
                StageBudgetReport {
                    stage: b.stage,
                    actual,
                    budget: b.budget,
                }
            })
            .collect();
        Self { stages }
    }

    /// The stage consuming the largest share of its own budget.
    pub fn most_consumed_stage(&self) -> Option<&StageBudgetReport> {
        self.stages
            .iter()
            .max_by(|a, b| a.utilization().total_cmp(&b.utilization()))
    }

    /// The stage closest to (but not necessarily exceeding) its budget.
    pub fn closest_to_budget(&self) -> Option<&StageBudgetReport> {
        self.stages
            .iter()
            .filter(|s| !s.exceeds_budget())
            .max_by(|a, b| a.utilization().total_cmp(&b.utilization()))
    }

    /// Stages that exceeded their allotted budget share.
    pub fn over_budget_stages(&self) -> Vec<&StageBudgetReport> {
        self.stages.iter().filter(|s| s.exceeds_budget()).collect()
    }
}

/// Number of consecutive over-budget observations before a stage is
/// considered to be *consistently* exceeding its budget and an alert fires.
pub const CONSISTENT_OVER_BUDGET_THRESHOLD: usize = 3;

/// Tracks consecutive over-budget observations per stage so alerting only
/// fires when a stage *consistently* exceeds its allotted share, not on a
/// single transient spike.
#[derive(Debug, Default)]
pub struct LatencyBudgetTracker {
    consecutive_over_budget: std::collections::HashMap<PipelineStage, usize>,
}

impl LatencyBudgetTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a report and return the stages that have consistently exceeded
    /// their budget (>= [`CONSISTENT_OVER_BUDGET_THRESHOLD`] consecutive times).
    pub fn record(&mut self, report: &LatencyBudgetReport) -> Vec<PipelineStage> {
        let mut alerting = Vec::new();
        for stage_report in &report.stages {
            let counter = self
                .consecutive_over_budget
                .entry(stage_report.stage)
                .or_insert(0);
            if stage_report.exceeds_budget() {
                *counter += 1;
                if *counter >= CONSISTENT_OVER_BUDGET_THRESHOLD {
                    alerting.push(stage_report.stage);
                }
            } else {
                *counter = 0;
            }
        }
        alerting
    }

    /// Current consecutive over-budget count for a stage.
    pub fn consecutive_over_budget(&self, stage: PipelineStage) -> usize {
        self.consecutive_over_budget
            .get(&stage)
            .copied()
            .unwrap_or(0)
    }
}

pub struct SettlementService {
    pool: PgPool,
    max_batch_size: usize,
    min_tx_count: usize,
    /// Health check timeout duration
    health_check_timeout: Duration,
    /// Readiness state for graceful shutdown coordination
    readiness: Option<Arc<crate::readiness::ReadinessState>>,
    /// Settlement operation duration histogram
    settlement_duration_ms: Histogram<f64>,
    /// Shared `QueryCache` for cache invalidation after settlement. `None`
    /// means invalidation is skipped (see `with_query_cache`).
    query_cache: Option<crate::services::query_cache::QueryCache>,
}

impl SettlementService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            max_batch_size: 10_000,
            min_tx_count: 1,
            health_check_timeout: Duration::from_secs(5),
            readiness: None,
            settlement_duration_ms: crate::metrics::settlement_duration_ms(),
            query_cache: None,
        }
    }

    pub fn with_config(pool: PgPool, max_batch_size: usize, min_tx_count: usize) -> Self {
        Self {
            pool,
            max_batch_size,
            min_tx_count,
            health_check_timeout: Duration::from_secs(5),
            readiness: None,
            settlement_duration_ms: crate::metrics::settlement_duration_ms(),
            query_cache: None,
        }
    }

    /// Attach the process's shared `QueryCache` so post-settlement cache
    /// invalidation reaches the same instance reads go through instead of
    /// silently no-oping (see `db::queries::invalidate_transaction_caches`).
    pub fn with_query_cache(mut self, cache: crate::services::query_cache::QueryCache) -> Self {
        self.query_cache = Some(cache);
        self
    }

    /// Create a new settlement service with readiness state for graceful shutdown
    pub fn with_readiness(pool: PgPool, readiness: Arc<crate::readiness::ReadinessState>) -> Self {
        Self {
            pool,
            max_batch_size: 10_000,
            min_tx_count: 1,
            health_check_timeout: Duration::from_secs(5),
            readiness: Some(readiness),
            settlement_duration_ms: crate::metrics::settlement_duration_ms(),
            query_cache: None,
        }
    }

    /// Create a new settlement service with readiness state and metrics for optimized monitoring
    pub fn with_metrics_and_readiness(
        pool: PgPool,
        readiness: Arc<crate::readiness::ReadinessState>,
        settlement_duration_ms: Histogram<f64>,
    ) -> Self {
        Self {
            pool,
            max_batch_size: 10_000,
            min_tx_count: 1,
            health_check_timeout: Duration::from_secs(5),
            readiness: Some(readiness),
            settlement_duration_ms,
            query_cache: None,
        }
    }

    /// Check if the settlement service is healthy
    /// Returns Ok(()) if healthy, Err(String) otherwise
    pub async fn check_health(&self) -> Result<(), String> {
        // Check database connectivity
        let start = Instant::now();
        match timeout(
            self.health_check_timeout,
            sqlx::query("SELECT 1").execute(&self.pool),
        )
        .await
        {
            Ok(result) => match result {
                Ok(_) => {
                    tracing::debug!(
                        "Settlement service database health check succeeded in {}ms",
                        start.elapsed().as_millis()
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Settlement service database health check failed: {}", e);
                    Err(format!("Database connection failed: {}", e))
                }
            },
            Err(_) => {
                tracing::error!(
                    "Settlement service database health check timed out after {}ms",
                    self.health_check_timeout.as_millis()
                );
                Err(format!(
                    "Database health check timed out after {}ms",
                    self.health_check_timeout.as_millis()
                ))
            }
        }
    }

    /// Gracefully shut down the settlement service
    /// Returns Ok(()) if shutdown completed successfully
    pub async fn shutdown(&self) -> Result<(), String> {
        tracing::info!("Shutting down settlement service...");

        // If we have a readiness state, mark as not ready to stop accepting new work
        if let Some(ref readiness) = self.readiness {
            readiness.set_not_ready();
            tracing::info!("Settlement service marked as not ready for new work");
        }

        // Wait for any in-flight settlement operations to complete
        // In a real implementation, this would wait for active tasks to finish
        // For now, we'll just log and return
        tracing::info!("Settlement service shutdown completed");
        Ok(())
    }

    /// Run settlement for all assets with completed, unsettled transactions.
    /// Respects each asset's `settlement_schedule` — assets configured as
    /// "hourly" are always eligible; "daily" assets only settle once per day;
    /// "weekly" assets only settle on Mondays.
    pub async fn run_settlements(&self) -> Result<Vec<Settlement>, AppError> {
        let start = std::time::Instant::now();

        let asset_codes = queries::get_unique_assets_to_settle(&self.pool)
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

        // Load asset configs so we can apply per-asset schedules
        let assets = Asset::fetch_all(&self.pool)
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;
        let _asset_map: std::collections::HashMap<String, Asset> = assets
            .into_iter()
            .map(|a| (a.asset_code.clone(), a))
            .collect();

        let _now = Utc::now();
        let mut results = Vec::new();
        for asset_code in &asset_codes {
            match self.settle_asset(asset_code).await {
                Ok(settlements) => results.extend(settlements),
                Err(e) => tracing::error!("Failed to settle asset {:?}: {:?}", asset_code, e),
            }
        }

        // Record metrics for the entire run_settlements operation
        let duration_ms = start.elapsed().as_millis() as f64;
        self.settlement_duration_ms.record(
            duration_ms,
            &[opentelemetry::KeyValue::new("operation", "run_settlements")],
        );

        Ok(results)
    }

    /// Settle transactions for a specific asset, splitting into multiple settlements
    /// when the number of transactions exceeds `max_batch_size`.
    ///
    /// Returns an empty `Vec` when there are fewer than `min_

/* … truncated 11234 chars — edit only what you need near the top … */
