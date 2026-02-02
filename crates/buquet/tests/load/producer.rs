//! Producer for load tests - submits tasks at configurable rate.

use crate::metrics::LoadMetrics;
use buquet::queue::{Queue, SubmitOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A producer that submits tasks to the queue.
pub struct Producer {
    queue: Queue,
    task_type: String,
    metrics: Arc<LoadMetrics>,
    max_delay_ms: u64,
}

impl Producer {
    /// Creates a new producer.
    pub fn new(
        queue: Queue,
        task_type: String,
        metrics: Arc<LoadMetrics>,
        max_delay_ms: u64,
    ) -> Self {
        Self {
            queue,
            task_type,
            metrics,
            max_delay_ms,
        }
    }

    /// Runs the producer, submitting the specified number of tasks.
    pub async fn run(&self, task_count: usize) {
        let mut rng = StdRng::from_entropy();

        for i in 0..task_count {
            // Random delay between submissions
            if self.max_delay_ms > 0 {
                let delay = rng.gen_range(0..=self.max_delay_ms);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            // Submit task with timing
            let start = Instant::now();
            let input = json!({
                "task_index": i,
                "producer_time": chrono::Utc::now().to_rfc3339(),
                "random_data": rng.gen::<u64>(),
            });

            match self
                .queue
                .submit(&self.task_type, input, SubmitOptions::default())
                .await
            {
                Ok(_task) => {
                    let latency = start.elapsed();
                    self.metrics.record_submit(latency);
                }
                Err(e) => {
                    eprintln!("Failed to submit task {}: {:?}", i, e);
                }
            }
        }
    }
}
