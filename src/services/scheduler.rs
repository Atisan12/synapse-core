use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use cron::Schedule;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

/// Represents a scheduled job that can be executed at specific intervals
#[async_trait]
pub trait Job: Send + Sync {
    /// Unique name of the job
    fn name(&self) -> &str;

    /// Cron expression defining when the job should run
    fn schedule(&self) -> &str;

    /// Execute the job's business logic
    async fn execute(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Last-run outcome recorded for a job, used to distinguish "did not run at
/// all" from "ran but failed" when checking job health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LastRunOutcome {
    Success,
    Failure,
}

#[derive(Debug, Clone)]
struct JobRunRecord {
    at: DateTime<Utc>,
    outcome: LastRunOutcome,
}

/// One alertable condition surfaced by [`JobScheduler::check_job_health`].
/// `MissedRun` and `Failed` are deliberately distinct so an operator (or
/// alerting rule) can tell "this job silently stopped running" apart from
/// "this job is running but erroring every time" — the former usually means
/// a crash loop or a stuck lock, the latter a logic/dependency bug.
#[derive(Debug, Clone, PartialEq)]
pub enum JobHealthAlert {
    /// The job has not completed successfully within its expected interval
    /// plus grace period. `last_success` is `None` if it has never run.
    MissedRun {
        job_name: String,
        last_success: Option<DateTime<Utc>>,
        expected_by: DateTime<Utc>,
    },
    /// The job's most recent run failed (regardless of whether earlier runs
    /// succeeded within the window).
    Failed {
        job_name: String,
        failed_at: DateTime<Utc>,
    },
}

/// A job scheduler that manages cron-based recurring tasks
pub struct JobScheduler {
    jobs: Arc<Mutex<HashMap<String, Arc<dyn Job>>>>,
    active_handles: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    last_success: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    last_run: Arc<Mutex<HashMap<String, JobRunRecord>>>,
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl JobScheduler {
    /// Create a new job scheduler instance
    pub fn new() -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            active_handles: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx,
            last_success: Arc::new(Mutex::new(HashMap::new())),
            last_run: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a new job with the scheduler
    pub async fn register_job(
        &self,
        job: Box<dyn Job>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let name = job.name().to_string();

        // Validate the cron expression
        Schedule::from_str(job.schedule())
            .map_err(|e| format!("Invalid cron expression '{}': {}", job.schedule(), e))?;

        let mut jobs = self.jobs.lock().await;
        jobs.insert(name, Arc::from(job));
        Ok(())
    }

    /// Start the scheduler and all registered jobs
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let jobs = self.jobs.lock().await;
        let active_handles = self.active_handles.clone();

        for (name, job) in jobs.iter() {
            let job_clone = Arc::clone(job);
            let name_clone = name.clone();
            let shutdown_rx = self.shutdown_tx.subscribe();
            let active_handles_clone = Arc::clone(&active_handles);

            // Tag this spawned task by category so task-leak detection can
            // attribute it to `scheduler-job` rather than an opaque total.
            let handle = crate::metrics::spawn_tracked(
                crate::metrics::TaskCategory::SchedulerJob,
                Self::run_job_loop(
                    name_clone,
                    job_clone,
                    self.shutdown_tx.clone(),
                    shutdown_rx,
                    active_handles_clone,
                    self.last_success.clone(),
                    self.last_run.clone(),
                ),
            );

            active_handles.lock().await.insert(name.clone(), handle);
        }

        info!("Job scheduler started with {} jobs", jobs.len());
        Ok(())
    }

    /// Stop the scheduler and all running jobs gracefully
    pub async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Sync>> {
        info!("Stopping job scheduler...");

        // Signal all jobs to shut down
        let _ = self.shutdown_tx.send(());

        // Wait for all active handles to finish
        let handles: Vec<_> = {
            let mut active_handles = self.active_handles.lock().await;
            active_handles.drain().map(|(_, handle)| handle).collect()
        };

        // Wait for all tasks to complete
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Error waiting for job task to finish: {}", e);
            }
        }

        info!("Job scheduler stopped");
        Ok(())
    }

    /// Get status information about all registered jobs
    pub async fn get_job_status(&self) -> HashMap<String, JobStatus> {
        let jobs = self.jobs.lock().await;
        let active_handles = self.active_handles.lock().await;
        let mut status = HashMap::new();

        for (name, job) in jobs.iter() {
            // Parse the schedule to get the next run time
            let next_run = Self::get_next_run_time(job.schedule());

            status.insert(
                name.clone(),
                JobStatus {
                    name: name.clone(),
                    schedule: job.schedule().to_string(),
                    next_run,
                    is_active: active_handles.contains_key(name),
                },
            );
        }

        status
    }

    /// Checks every registered job for missed or failed runs.
    ///
    /// A job alerts as [`JobHealthAlert::MissedRun`] if it has never
    /// completed successfully, or if its last successful run is older than
    /// its cron schedule's expected interval (computed from that job's own
    /// cron expression, so irregular/non-fixed-interval schedules are
    /// handled correctly) plus `grace_period`. It separately alerts as
    /// [`JobHealthAlert::Failed`] if its most recent run attempt errored,
    /// regardless of whether an earlier run succeeded within the window —
    /// so a job can surface both alerts at once (ran too long ago, and the
    /// last attempt also failed).
    pub async fn check_job_health(&self, grace_period: Duration) -> Vec<JobHealthAlert> {
        let jobs = self.jobs.lock().await;
        let last_success = self.last_success.lock().await;
        let last_run = self.last_run.lock().await;
        let now = Utc::now();
        let mut alerts = Vec::new();

        for (name, job) in jobs.iter() {
            let success_at = last_success.get(name).copied();

            let expected_interval = match Schedule::from_str(job.schedule()) {
                Ok(schedule) => {
                    let anchor = success_at.unwrap_or(now);
                    schedule
                        .after(&anchor)
                        .next()
                        .map(|next| next - anchor)
                        .unwrap_or_else(|| Duration::zero())
                }
                Err(_) => Duration::zero(),
            };

            let expected_by = match success_at {
                Some(at) => at + expected_interval + grace_period,
                None => now, // never succeeded: overdue immediately
            };

            if success_at.is_none() || expected_by < now {
                alerts.push(JobHealthAlert::MissedRun {
                    job_name: name.clone(),
                    last_success: success_at,
                    expected_by,
                });
            }

            if let Some(record) = last_run.get(name) {
                if record.outcome == LastRunOutcome::Failure {
                    alerts.push(JobHealthAlert::Failed {
                        job_name: name.clone(),
                        failed_at: record.at,
                    });
                }
            }
        }

        alerts
    }

    /// Compute the next run time for a cron expression, if parseable.
    fn get_next_run_time(schedule: &str) -> Option<DateTime<Utc>> {
        Schedule::from_str(schedule)
            .ok()
            .and_then(|s| s.upcoming(Utc).next())
    }

    /// The per-job execution loop. Runs the job on its cron schedule until
    /// the shutdown signal is received.
    async fn run_job_loop(
        name: String,
        job: Arc<dyn Job>,
        _shutdown_tx: tokio::sync::broadcast::Sender<()>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        _active_handles: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
        last_success: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
        last_run: Arc<Mutex<HashMap<String, JobRunRecord>>>,
    ) {
        let schedule = match Schedule::from_str(job.schedule()) {
            Ok(s) => s,
            Err(e) => {
                error!("Job '{}' has invalid schedule: {}", name, e);
                return;
            }
        };

        loop {
            let next = match schedule.upcoming(Utc).next() {
                Some(next) => next,
                None => break,
            };

            let now = Utc::now();
            let sleep_for = (next - now).to_std().unwrap_or_default();

            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {
                    let outcome = match job.execute().await {
                        Ok(()) => {
                            last_success.lock().await.insert(name.clone(), Utc::now());
                            LastRunOutcome::Success
                        }
                        Err(e) => {
                            error!("Job '{}' failed: {}", name, e);
                            LastRunOutcome::Failure
                        }
                    };
                    last_run.lock().await.insert(
                        name.clone(),
                        JobRunRecord {
                            at: Utc::now(),
                            outcome,
                        },
                    );
                }
                _ = shutdown_rx.recv() => {
                    info!("Job '{}' received shutdown signal", name);
                    break;
                }
            }
        }
    }
}

/// Status information for a registered job
#[derive(Debug, Clone)]
pub struct JobStatus {
    pub name: String,
    pub schedule: String,
    pub next_run: Option<DateTime<Utc>>,
    pub is_active: bool,
}
