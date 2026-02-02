//! Worker for load tests - claims and processes tasks.

use crate::metrics::LoadMetrics;
use buquet::models::TaskStatus;
use buquet::queue::Queue;
use buquet::worker::{claim_task, complete_task, ClaimResult};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// A worker that claims and processes tasks for load testing.
pub struct LoadWorker {
    queue: Queue,
    worker_id: String,
    task_type: String,
    metrics: Arc<LoadMetrics>,
    min_process_ms: u64,
    max_process_ms: u64,
}

impl LoadWorker {
    /// Creates a new load test worker.
    pub const fn new(
        queue: Queue,
        worker_id: String,
        task_type: String,
        metrics: Arc<LoadMetrics>,
        min_process_ms: u64,
        max_process_ms: u64,
    ) -> Self {
        Self {
            queue,
            worker_id,
            task_type,
            metrics,
            min_process_ms,
            max_process_ms,
        }
    }

    /// Runs the worker until shutdown signal.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let mut rng = StdRng::from_entropy();

        loop {
            // Check for shutdown
            if *shutdown.borrow() {
                break;
            }

            // Poll for ready tasks
            self.metrics.record_list();

            // Get all shards this worker should poll
            let shards = self.queue.all_shards();

            let mut found_task = false;

            for shard in &shards {
                let task_ids = match self.queue.list_ready(shard, 10).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        eprintln!("Failed to list ready tasks for shard {shard}: {e:?}");
                        continue;
                    }
                };

                for task_id in task_ids {
                    // Try to claim the task
                    let claim_start = Instant::now();
                    match claim_task(&self.queue, task_id, &self.worker_id).await {
                        Ok(ClaimResult::Claimed { task, etag: _ }) => {
                            let claim_latency = claim_start.elapsed();
                            self.metrics.record_claim_success(claim_latency);

                            // Verify task type matches
                            if task.task_type != self.task_type {
                                continue;
                            }

                            found_task = true;

                            // Simulate processing with random delay
                            let process_time = if self.min_process_ms < self.max_process_ms {
                                rng.gen_range(self.min_process_ms..=self.max_process_ms)
                            } else {
                                self.min_process_ms
                            };
                            let process_start = Instant::now();
                            tokio::time::sleep(Duration::from_millis(process_time)).await;

                            // Complete the task
                            let lease_id =
                                task.lease_id.expect("Claimed task should have lease_id");
                            let output = json!({
                                "worker_id": self.worker_id,
                                "processed_at": chrono::Utc::now().to_rfc3339(),
                                "process_time_ms": process_time,
                            });

                            match complete_task(&self.queue, task.id, lease_id, output).await {
                                Ok(completed) => {
                                    assert_eq!(completed.status, TaskStatus::Completed);
                                    self.metrics.record_complete(process_start.elapsed());
                                }
                                Err(e) => {
                                    eprintln!("Failed to complete task {}: {:?}", task.id, e);
                                    self.metrics.record_failed();
                                }
                            }
                        }
                        Ok(ClaimResult::AlreadyClaimed) => {
                            self.metrics.record_claim_conflict();
                        }
                        Ok(
                            ClaimResult::NotAvailable
                            | ClaimResult::NotFound
                            | ClaimResult::Expired,
                        ) => {
                            // Task not available, skip
                        }
                        Err(e) => {
                            eprintln!("Claim error: {e:?}");
                        }
                    }
                }
            }

            // If no task found, wait a bit before polling again
            if !found_task {
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(50)) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        }
    }
}
